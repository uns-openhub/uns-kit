from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional
from urllib.parse import urljoin

from ..core.client import UnsMqttClient
from ..core.async_worker import AsyncCapacityError, AsyncClientClosedError, BoundedAsyncWorker
from ..core.packet import UnsPacket, isoformat
from ..core.proxy import UnsProxy
from ..core.topic_builder import TopicBuilder
from ..core.topic_matcher import matches_topic_filter
from ..core.uns_path import build_uns_route_path
from ..version import __package_name__, __version__
from .service_authority import ServiceApiAuthority, ServiceAuthorizationError, has_service_authorization_hint
from .request_lifecycle import RequestLifecycle, RequestOwnershipMiddleware


logger = logging.getLogger(__name__)


def _normalize_base_prefix(value: Optional[str], default: str) -> str:
    if not value:
        return default
    trimmed = value.strip()
    if not trimmed:
        return default
    with_leading = trimmed if trimmed.startswith("/") else f"/{trimmed}"
    return with_leading.rstrip("/") or "/"


def _build_swagger_path(base: str, process_name: str, instance_name: str) -> str:
    process_segment = f"/{process_name}"
    base_with_process = base or "/"
    if not base_with_process.endswith(process_segment):
        base_with_process = f"{base_with_process}{process_segment}"
    return f"{base_with_process}/{instance_name}/swagger.json".replace("//", "/")


def _external_ipv4() -> str:
    try:
        host_name = socket.gethostname()
        addresses = socket.getaddrinfo(host_name, None, family=socket.AF_INET)
        for result in addresses:
            address = result[4][0]
            if not address.startswith("127."):
                return address
    except Exception:
        pass
    return "127.0.0.1"


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@dataclass
class QueryParamDef:
    name: str
    type: str
    required: bool = False
    description: Optional[str] = None
    chat_canonical: Optional[str] = None
    default_value: Optional[str | int | float | bool] = None

    @staticmethod
    def from_value(value: "QueryParamDef | Mapping[str, Any]") -> "QueryParamDef":
        if isinstance(value, QueryParamDef):
            return value
        return QueryParamDef(
            name=str(value["name"]),
            type=str(value["type"]),
            required=bool(value.get("required", False)),
            description=value.get("description"),
            chat_canonical=value.get("chatCanonical") or value.get("chat_canonical"),
            default_value=value.get("defaultValue") if "defaultValue" in value else value.get("default_value"),
        )


@dataclass
class GetEndpointOptions:
    api_description: Optional[str] = None
    tags: list[str] = field(default_factory=list)
    query_params: list[QueryParamDef] = field(default_factory=list)
    chat_defaults: dict[str, str | int | float | bool] = field(default_factory=dict)
    route_only: bool = False
    registry_topic: str = "api-endpoints"
    service_api: Optional[dict[str, Any]] = None

    @staticmethod
    def from_value(value: Optional["GetEndpointOptions | Mapping[str, Any]"]) -> "GetEndpointOptions":
        if value is None:
            return GetEndpointOptions()
        if isinstance(value, GetEndpointOptions):
            return value
        return GetEndpointOptions(
            api_description=value.get("apiDescription") or value.get("api_description"),
            tags=list(value.get("tags") or []),
            query_params=[QueryParamDef.from_value(item) for item in value.get("queryParams") or value.get("query_params") or []],
            chat_defaults=dict(value.get("chatDefaults") or value.get("chat_defaults") or {}),
            route_only=bool(value.get("routeOnly") or value.get("route_only", False)),
            registry_topic=str(value.get("registryTopic") or value.get("registry_topic") or "api-endpoints"),
            service_api=dict(value.get("serviceApi") or value.get("service_api") or {}) or None,
        )


@dataclass
class PostEndpointOptions:
    api_description: Optional[str] = None
    tags: list[str] = field(default_factory=list)
    request_body: Optional[dict[str, Any]] = None
    route_only: bool = False
    registry_topic: str = "api-endpoints"
    service_api: Optional[dict[str, Any]] = None

    @staticmethod
    def from_value(value: Optional["PostEndpointOptions | Mapping[str, Any]"]) -> "PostEndpointOptions":
        if value is None:
            return PostEndpointOptions()
        if isinstance(value, PostEndpointOptions):
            return value
        return PostEndpointOptions(
            api_description=value.get("apiDescription") or value.get("api_description"),
            tags=list(value.get("tags") or []),
            request_body=value.get("requestBody") or value.get("request_body"),
            route_only=bool(value.get("routeOnly") or value.get("route_only", False)),
            registry_topic=str(value.get("registryTopic") or value.get("registry_topic") or "api-endpoints"),
            service_api=dict(value.get("serviceApi") or value.get("service_api") or {}) or None,
        )


@dataclass
class ApiProxyOptions:
    jwt_secret: Optional[str] = None
    jwks: Optional[dict[str, Any]] = None
    api_base_path: Optional[str] = None
    swagger_base_path: Optional[str] = None
    disable_default_api_mount: bool = False
    service_authorization_status_url: Optional[str] = None
    max_pending_requests: int = 256

    @staticmethod
    def from_value(value: Optional["ApiProxyOptions | Mapping[str, Any]"]) -> "ApiProxyOptions":
        if value is None:
            return ApiProxyOptions()
        if isinstance(value, ApiProxyOptions):
            return value
        return ApiProxyOptions(
            jwt_secret=value.get("jwtSecret") or value.get("jwt_secret"),
            jwks=value.get("jwks"),
            api_base_path=value.get("apiBasePath") or value.get("api_base_path"),
            swagger_base_path=value.get("swaggerBasePath") or value.get("swagger_base_path"),
            disable_default_api_mount=bool(value.get("disableDefaultApiMount") or value.get("disable_default_api_mount", False)),
            service_authorization_status_url=value.get("serviceAuthorizationStatusUrl") or value.get("service_authorization_status_url") or value.get("operatorAuthorizationStatusUrl"),
            max_pending_requests=value.get("maxPendingRequests", value.get("max_pending_requests", 256)),
        )


class ApiEventContext:
    def __init__(self, request: Any) -> None:
        self.req = request
        self.request = request
        self._response: Any = None
        self._status_code = 200

    def json(self, payload: Any, status_code: int = 200) -> None:
        from fastapi.responses import JSONResponse

        resolved_status = self._status_code if status_code == 200 and self._status_code != 200 else status_code
        self._response = JSONResponse(payload, status_code=resolved_status)
        self._status_code = 200

    def text(self, payload: str, status_code: int = 200) -> None:
        from fastapi.responses import PlainTextResponse

        resolved_status = self._status_code if status_code == 200 and self._status_code != 200 else status_code
        self._response = PlainTextResponse(payload, status_code=resolved_status)
        self._status_code = 200

    def response(self, response: Any) -> None:
        self._response = response

    def status(self, status_code: int) -> "ApiEventContext":
        self._status_code = status_code
        return self

    @property
    def res(self) -> "ApiEventContext":
        return self

    @property
    def response_object(self) -> Any:
        return self._response


class UnsApiProxy(UnsProxy):
    def __init__(
        self,
        client: UnsMqttClient,
        *,
        process_name: str,
        instance_name: str,
        topic_builder: TopicBuilder,
        options: Optional[ApiProxyOptions | Mapping[str, Any]] = None,
    ) -> None:
        self.options = ApiProxyOptions.from_value(options)
        self._requests = RequestLifecycle(max_pending=self.options.max_pending_requests)
        self._stop_task: asyncio.Task | None = None
        controller_base = os.environ.get("UNS_CONTROLLER_PUBLIC_BASE")
        status_url = self.options.service_authorization_status_url
        if status_url is None and controller_base:
            status_url = urljoin(controller_base, "/api/auth/token-status")
        self._service_authority = ServiceApiAuthority(status_url)
        self._jwt_worker = BoundedAsyncWorker(max_pending=16)
        self.process_name = process_name
        self.instance_name = instance_name
        self.topic_builder = topic_builder
        self.instance_status_topic = topic_builder.instance_status_topic(instance_name)
        super().__init__(client, self.instance_status_topic, instance_name)
        self.api_base_prefix = _normalize_base_prefix(self.options.api_base_path, "/api")
        raw_swagger_base = _normalize_base_prefix(self.options.swagger_base_path, self.api_base_prefix)
        self.swagger_base_prefix = raw_swagger_base[:-4] if raw_swagger_base.endswith("/api") else raw_swagger_base
        self.swagger_json_path = _build_swagger_path(self.swagger_base_prefix, self.process_name, self.instance_name)
        self.started_at = datetime.now(timezone.utc)
        self._host = "0.0.0.0"
        self._port = _pick_free_port()
        self._public_ip = _external_ipv4()
        self._server_thread: Optional[threading.Thread] = None
        self._server: Any = None
        self._ready = threading.Event()
        self._status_task: Optional[asyncio.Task] = None
        self._app = self._create_application()
        self.swagger_spec: dict[str, Any] = {
            "openapi": "3.0.0",
            "info": {
                "title": "UNS API",
                "version": "1.0.0",
            },
            "paths": {},
            "servers": [{"url": self.swagger_base_prefix or "/"}],
        }
        self._data_catalog_offers: dict[str, dict[str, Any]] = {}
        self._register_health_endpoint()

    async def start(self) -> None:
        await super().start()
        self._start_server()
        await self._wait_until_ready()
        logger.info("API listening on http://%s:%s%s", self._public_ip, self._port, self.api_base_prefix)
        logger.info("Swagger openAPI on http://%s:%s%s", self._public_ip, self._port, self.swagger_json_path)
        self._status_task = asyncio.create_task(self._status_loop())

    def get_process_name(self) -> str:
        return self.process_name

    def set_ownership_gate(self, gate: Callable[[], bool]) -> None:
        self._requests.set_gate(gate)

    def setOwnershipGate(self, gate: Callable[[], bool]) -> None:
        self.set_ownership_gate(gate)

    def begin_release(self) -> None:
        self._requests.begin_release()

    async def drain_requests(self, timeout: float | None = 30.0) -> None:
        await self._requests.drain(timeout)

    async def run_read_only_request(self, context: Any, operation: Callable[[], Any]) -> Any:
        return await self._requests.run_read_only(context, operation)

    async def stop(self, *, timeout: float | None = 30.0) -> None:
        self.begin_release()
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._perform_stop(timeout))
            self._stop_task.add_done_callback(lambda task: None if task.cancelled() else task.exception())
        task = self._stop_task
        try:
            await asyncio.shield(task)
        except Exception:
            if self._stop_task is task:
                self._stop_task = None
            raise

    async def _perform_stop(self, timeout: float | None) -> None:
        await self.drain_requests(timeout)
        if self._status_task and not self._status_task.done():
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass
        if self._server is not None:
            self._server.should_exit = True
        if self._server_thread is not None:
            await asyncio.to_thread(self._server_thread.join, timeout)
            if self._server_thread.is_alive():
                raise TimeoutError("HTTP server exit remains unconfirmed")
        await self._service_authority.close()
        await self._jwt_worker.close()
        await super().stop()

    async def get(self, topic: str, asset: str, object_type: str, object_id: str, attribute: str, options: Optional[GetEndpointOptions | Mapping[str, Any]] = None) -> None:
        from fastapi.responses import JSONResponse

        await self._wait_until_ready()
        endpoint_options = GetEndpointOptions.from_value(options)
        full_path = build_uns_route_path(topic, asset, object_type, object_id, attribute)
        api_path = f"{self.api_base_prefix}{full_path}".replace("//", "/")
        api_host = f"http://{self._public_ip}:{self._port}"

        await self.register_api_endpoint(
            {
                "timestamp": isoformat(datetime.now(timezone.utc)),
                "topic": topic,
                "asset": asset,
                "objectType": object_type,
                "objectId": object_id,
                "attribute": attribute,
                "routeOnly": endpoint_options.route_only,
                "registryTopic": endpoint_options.registry_topic,
                "attributeType": "Api",
                "apiHost": api_host,
                "apiEndpoint": api_path,
                "apiSwaggerEndpoint": self.swagger_json_path,
                "apiMethod": "GET",
                "apiQueryParams": [self._query_param_to_registry_item(param) for param in endpoint_options.query_params],
                "apiDescription": endpoint_options.api_description,
                "serviceApi": endpoint_options.service_api,
            }
        )

        async def handler(request) -> Any:
            unauthorized = await self._authorize_request(request, full_path)
            if unauthorized is not None:
                return unauthorized
            validation_error = self._validate_query_params(request, endpoint_options.query_params)
            if validation_error is not None:
                return validation_error
            context = ApiEventContext(request)
            await self.event.emit("apiGetEvent", context)
            return context.response_object or JSONResponse({"error": "No handler response produced"}, status_code=500)

        self._add_route(api_path, handler, ["GET"])
        self._ensure_swagger_path(api_path)
        self.swagger_spec["paths"][api_path]["get"] = self._build_get_swagger_operation(endpoint_options)

    async def post(self, topic: str, asset: str, object_type: str, object_id: str, attribute: str, options: Optional[PostEndpointOptions | Mapping[str, Any]] = None) -> None:
        await self._register_mutation_endpoint("POST", topic, asset, object_type, object_id, attribute, options)

    async def put(self, topic: str, asset: str, object_type: str, object_id: str, attribute: str, options: Optional[PostEndpointOptions | Mapping[str, Any]] = None) -> None:
        await self._register_mutation_endpoint("PUT", topic, asset, object_type, object_id, attribute, options)

    async def patch(self, topic: str, asset: str, object_type: str, object_id: str, attribute: str, options: Optional[PostEndpointOptions | Mapping[str, Any]] = None) -> None:
        await self._register_mutation_endpoint("PATCH", topic, asset, object_type, object_id, attribute, options)

    async def delete(self, topic: str, asset: str, object_type: str, object_id: str, attribute: str, options: Optional[PostEndpointOptions | Mapping[str, Any]] = None) -> None:
        await self._register_mutation_endpoint("DELETE", topic, asset, object_type, object_id, attribute, options)

    async def _register_mutation_endpoint(self, method: str, topic: str, asset: str, object_type: str, object_id: str, attribute: str, options: Optional[PostEndpointOptions | Mapping[str, Any]] = None) -> None:
        from fastapi.responses import JSONResponse

        await self._wait_until_ready()
        endpoint_options = PostEndpointOptions.from_value(options)
        full_path = build_uns_route_path(topic, asset, object_type, object_id, attribute)
        api_path = f"{self.api_base_prefix}{full_path}".replace("//", "/")
        api_host = f"http://{self._public_ip}:{self._port}"

        await self.register_api_endpoint(
            {
                "timestamp": isoformat(datetime.now(timezone.utc)),
                "topic": topic,
                "asset": asset,
                "objectType": object_type,
                "objectId": object_id,
                "attribute": attribute,
                "routeOnly": endpoint_options.route_only,
                "registryTopic": endpoint_options.registry_topic,
                "attributeType": "Api",
                "apiHost": api_host,
                "apiEndpoint": api_path,
                "apiSwaggerEndpoint": self.swagger_json_path,
                "apiMethod": method,
                "apiQueryParams": [],
                "apiDescription": endpoint_options.api_description,
                "serviceApi": endpoint_options.service_api,
            }
        )

        async def handler(request) -> Any:
            unauthorized = await self._authorize_request(request, full_path)
            if unauthorized is not None:
                return unauthorized
            context = ApiEventContext(request)
            await self.event.emit(self._event_name_for_method(method), context)
            return context.response_object or JSONResponse({"error": "No handler response produced"}, status_code=500)

        self._add_route(api_path, handler, [method])
        self._ensure_swagger_path(api_path)
        self.swagger_spec["paths"][api_path][method.lower()] = self._build_post_swagger_operation(endpoint_options)

    async def register_catch_all(self, topic_prefix: str, options: Optional[Mapping[str, Any]] = None) -> None:
        normalized = topic_prefix if topic_prefix.endswith("/") else f"{topic_prefix}/"
        api_base = (options or {}).get("apiBase") or f"http://{self._public_ip}:{self._port}"
        api_base_path = (options or {}).get("apiBasePath") or self.api_base_prefix
        swagger_path = (options or {}).get("swaggerPath") or self.swagger_json_path
        final_options = dict(options or {})
        query_params = [
            QueryParamDef.from_value(item)
            for item in final_options.get("queryParams", [])
        ]
        swagger_doc = final_options.get("swaggerDoc")
        catchall_route = f"{api_base_path}/{{topicPath:path}}".replace("//", "/")

        if swagger_doc is None:
            swagger_doc = {
                "openapi": "3.0.0",
                "info": {"title": "Catch-all API", "version": "1.0.0"},
                "paths": {
                    "/api/{topicPath}": {
                        "get": {
                            "summary": final_options.get("apiDescription") or "Catch-all handler",
                            "tags": final_options.get("tags") or [],
                            "parameters": [
                                {
                                    "name": "topicPath",
                                    "in": "path",
                                    "required": True,
                                    "schema": {"type": "string"},
                                    "description": "Resolved UNS topic path",
                                },
                                *[
                                    {
                                        "name": param.name,
                                        "in": "query",
                                        "required": param.required,
                                        "schema": {"type": param.type},
                                        "description": param.description,
                                    }
                                    for param in query_params
                                ],
                            ],
                            "responses": {
                                "200": {"description": "OK"},
                                "400": {"description": "Bad Request"},
                                "401": {"description": "Unauthorized"},
                                "403": {"description": "Forbidden"},
                            },
                        }
                    }
                },
            }

        async def handler(request) -> Any:
            full_path = request.url.path.replace(api_base_path, "", 1) or "/"
            unauthorized = await self._authorize_request(request, full_path)
            if unauthorized is not None:
                return unauthorized
            context = ApiEventContext(request)
            await self.event.emit("apiGetEvent", context)
            return context.response_object

        async def swagger_handler(_request) -> Any:
            return self._json_response(swagger_doc)

        self._add_route(catchall_route, handler, ["GET"])
        normalized_swagger_path = swagger_path if str(swagger_path).startswith("/") else f"/{swagger_path}"
        self._app.router.routes = [route for route in self._app.router.routes if getattr(route, "path", None) != normalized_swagger_path]
        self._add_route(normalized_swagger_path, swagger_handler, ["GET"])
        logger.info(
            "%s-%s - Catch-all Swagger available at %s (target %s%s)",
            self.process_name,
            self.instance_name,
            normalized_swagger_path,
            str(api_base).rstrip("/"),
            normalized_swagger_path,
        )
        await self.register_api_catchall(
            {
                "topic": normalized,
                "apiBase": api_base,
                "apiBasePath": api_base_path,
                "swaggerPath": swagger_path,
            }
        )

    async def registerCatchAll(self, topicPrefix: str, options: Optional[Mapping[str, Any]] = None) -> None:
        await self.register_catch_all(topicPrefix, options)

    def register_data_offer(self, input_value: Mapping[str, Any]) -> None:
        offer_id = str(input_value.get("offerId") or input_value.get("offer_id") or "").strip()
        display_name = str(input_value.get("displayName") or input_value.get("display_name") or "").strip()
        if not offer_id or not display_name:
            raise ValueError("Data catalog offer requires non-empty offerId and displayName.")

        operations = []
        for index, operation in enumerate(input_value.get("operations") or []):
            if not isinstance(operation, Mapping):
                continue
            method = str(operation.get("method") or "").upper().strip()
            path = str(operation.get("path") or "").strip()
            if not method or not path:
                continue
            operations.append(
                {
                    "id": str(operation.get("id") or f"{offer_id}-{method.lower()}-{index + 1}"),
                    "method": method,
                    "path": path if path.startswith("/") else f"/{path}",
                    "summary": operation.get("summary"),
                    "description": operation.get("description"),
                    "tags": list(operation.get("tags") or []),
                    "deprecated": bool(operation.get("deprecated", False)),
                    "parameters": list(operation.get("parameters") or []),
                    "headers": list(operation.get("headers") or []),
                    "requestBody": operation.get("requestBody") or operation.get("request_body"),
                    "responses": list(operation.get("responses") or []),
                }
            )
        if not operations:
            raise ValueError(f"Data catalog offer {offer_id} requires at least one operation.")

        base_paths = list(input_value.get("basePaths") or input_value.get("base_paths") or [])
        if not base_paths:
            base_paths = sorted({"/" + "/".join(operation["path"].strip("/").split("/")[:-1]) for operation in operations})

        offer = {
            "offerId": offer_id,
            "displayName": display_name,
            "description": input_value.get("description"),
            "owner": input_value.get("owner"),
            "status": input_value.get("status") or "available",
            "tags": list(input_value.get("tags") or []),
            "categories": list(input_value.get("categories") or []),
            "microserviceName": input_value.get("microserviceName") or input_value.get("microservice_name") or self.process_name,
            "version": input_value.get("version") or __version__,
            "basePaths": [path if str(path).startswith("/") else f"/{path}" for path in base_paths],
            "operations": operations,
            "schemas": list(input_value.get("schemas") or []),
            "swaggerPath": input_value.get("swaggerPath") or input_value.get("swagger_path"),
            "metadata": input_value.get("metadata"),
            "packageName": __package_name__,
            "processName": self.process_name,
            "processVersion": __version__,
            "instanceName": self.instance_name,
        }
        self._apply_controller_metadata(offer)
        self._data_catalog_offers[offer_id] = offer
        asyncio.create_task(self._emit_data_catalog_offers())

    def registerDataOffer(self, inputValue: Mapping[str, Any]) -> None:
        self.register_data_offer(inputValue)

    async def unregister(self, topic: str, asset: str, object_type: str, object_id: str, attribute: str, method: str) -> None:
        full_path = build_uns_route_path(topic, asset, object_type, object_id, attribute)
        api_path = f"{self.api_base_prefix}{full_path}".replace("//", "/")
        lower_method = method.lower()
        self._app.router.routes = [
            route
            for route in self._app.router.routes
            if not (
                getattr(route, "path", None) == api_path
                and lower_method in {m.lower() for m in getattr(route, "methods", set())}
            )
        ]
        if api_path in self.swagger_spec["paths"]:
            self.swagger_spec["paths"][api_path].pop(lower_method, None)
            if not self.swagger_spec["paths"][api_path]:
                del self.swagger_spec["paths"][api_path]
        await self.unregister_api_endpoint(topic, asset, object_type, object_id, attribute, method)

    async def create_openapi_doc(self) -> dict[str, Any]:
        return json.loads(json.dumps(self.swagger_spec))

    def _create_application(self) -> Any:
        try:
            from fastapi import FastAPI
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                'API support requires the "api" extra. Install with `pip install "uns-kit[api]"`.'
            ) from exc

        app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
        app.add_middleware(RequestOwnershipMiddleware, lifecycle=self._requests)
        return app

    def _start_server(self) -> None:
        if self._server_thread is not None:
            return

        def run() -> None:
            import uvicorn

            config = uvicorn.Config(self._app, host=self._host, port=self._port, log_level="warning")
            self._server = uvicorn.Server(config)
            self._ready.set()
            self._server.run()

        self._server_thread = threading.Thread(target=run, daemon=True)
        self._server_thread.start()

    async def _wait_until_ready(self) -> None:
        while not self._ready.is_set():
            await asyncio.sleep(0.05)
        while self._server is None or not getattr(self._server, "started", False):
            await asyncio.sleep(0.05)

    def _register_health_endpoint(self) -> None:
        from fastapi.responses import JSONResponse

        route_path = f"{self.api_base_prefix}/status".replace("//", "/")

        async def handler(_request) -> Any:
            return JSONResponse(
                {
                    "alive": True,
                    "processName": self.process_name,
                    "instanceName": self.instance_name,
                    "package": __package_name__,
                    "version": __version__,
                    "startedAt": self.started_at.isoformat(),
                    "uptimeMs": int((datetime.now(timezone.utc) - self.started_at).total_seconds() * 1000),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )

        async def swagger_handler(_request) -> Any:
            from fastapi.responses import JSONResponse

            return JSONResponse(await self.create_openapi_doc())

        self._add_route(route_path, handler, ["GET"])
        self._add_route(self.swagger_json_path, swagger_handler, ["GET"])
        self.swagger_spec["paths"][route_path] = {
            "get": {
                "summary": "Health status",
                "responses": {"200": {"description": "OK"}},
            }
        }

    def _add_route(self, path: str, endpoint: Any, methods: list[str]) -> None:
        from starlette.routing import Route

        self._app.router.routes.append(Route(path, endpoint=endpoint, methods=methods))

    def _json_response(self, payload: Any) -> Any:
        from fastapi.responses import JSONResponse

        return JSONResponse(payload)

    async def _status_loop(self) -> None:
        while True:
            uptime_minutes = round((datetime.now(timezone.utc) - self.started_at).total_seconds() / 60)
            for suffix, value, uom in (
                ("uptime", uptime_minutes, "minute"),
                ("alive", 1, "bit"),
            ):
                packet = UnsPacket.data(value=value, uom=uom)
                await self.event.emit(
                    "mqttProxyStatus",
                    {
                        "event": suffix,
                        "value": value,
                        "uom": uom,
                        "statusTopic": f"{self.topic_builder.process_status_topic}{suffix}",
                    },
                )
                await self._client.publish_raw(f"{self.topic_builder.process_status_topic}{suffix}", UnsPacket.to_json(packet))
                await self.event.emit(
                    "mqttProxyStatus",
                    {
                        "event": suffix,
                        "value": value,
                        "uom": uom,
                        "statusTopic": f"{self.instance_status_topic}{suffix}",
                    },
                )
                await self._client.publish_raw(f"{self.instance_status_topic}{suffix}", UnsPacket.to_json(packet))
            await self._emit_data_catalog_offers()
            await asyncio.sleep(10)

    async def _emit_data_catalog_offers(self) -> None:
        if not self._data_catalog_offers:
            return
        payload = list(self._data_catalog_offers.values())
        await self.event.emit(
            "unsProxyProducedDataCatalogOffers",
            {
                "producedDataCatalogOffers": payload,
                "statusTopic": f"{self.instance_status_topic}data-catalog-offers",
            },
        )
        await self._client.publish_raw(
            f"{self.instance_status_topic}data-catalog-offers",
            json.dumps(payload, separators=(",", ":")),
            retain=True,
        )

    async def _authorize_request(self, request: Any, full_path: str) -> Any:
        from fastapi.responses import JSONResponse

        if not self.options.jwks and not self.options.jwt_secret:
            return None

        auth_header = request.headers.get("authorization")
        endpoint = f"{request.method} {request.url.path}"
        if not auth_header or not auth_header.startswith("Bearer "):
            logger.warning(
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "user": "Anonymous",
                    "endpoint": endpoint,
                    "message": "Access attempted without token",
                }
            )
            return JSONResponse({"error": "Missing or invalid Authorization header"}, status_code=401)

        token = auth_header[7:]
        logger.debug("Bearer token extracted successfully.")
        try:
            payload = await self._decode_token(token)
        except (AsyncCapacityError, AsyncClientClosedError, TimeoutError):
            return JSONResponse({"error": "token_verification_unavailable"}, status_code=503)
        except Exception as exc:
            logger.error(
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "user": "Unknown",
                    "endpoint": endpoint,
                    "message": "JWT verification failed",
                    "error": str(exc),
                }
            )
            return JSONResponse({"error": "Invalid token"}, status_code=401)

        access_rules = payload.get("accessRules")
        if not isinstance(access_rules, list):
            path_filter = payload.get("pathFilter")
            access_rules = [path_filter] if isinstance(path_filter, str) and path_filter else []

        service = has_service_authorization_hint(payload)
        # An empty/non-string machine rule set must never become unrestricted.
        def valid_rule(rule: Any) -> bool:
            if not isinstance(rule, str) or not rule:
                return False
            parts = rule.split("/")
            return all(part and (part == "+" or part == "#" and index == len(parts) - 1 or "+" not in part and "#" not in part) for index, part in enumerate(parts))

        if service and (not access_rules or any(not valid_rule(rule) for rule in access_rules)):
            return JSONResponse({"error": "Path not allowed by token access rules"}, status_code=403)
        if access_rules and not any(matches_topic_filter(str(rule), full_path) for rule in access_rules):
            return JSONResponse({"error": "Path not allowed by token access rules"}, status_code=403)
        try:
            await self._service_authority.authorize(payload, auth_header)
        except ServiceAuthorizationError as error:
            return JSONResponse({"error": error.code}, status_code=error.status)
        logger.info("Authenticated API request authorized")
        return None

    async def _decode_token(self, token: str) -> dict[str, Any]:
        return await asyncio.wait_for(self._jwt_worker.run(lambda: self._decode_token_sync(token)), timeout=5.0)

    def _decode_token_sync(self, token: str) -> dict[str, Any]:
        import jwt

        if self.options.jwks:
            jwks_client = jwt.PyJWKClient(self.options.jwks["wellKnownJwksUrl"], timeout=5.0)
            signing_key = jwks_client.get_signing_key_from_jwt(token)
            algorithms = self.options.jwks.get("algorithms") or ["RS256"]
            return dict(
                jwt.decode(
                    token,
                    signing_key.key,
                    algorithms=algorithms,
                    options={"verify_aud": False},
                )
            )

        header = jwt.get_unverified_header(token)
        algorithm = header.get("alg")
        if not algorithm:
            raise ValueError("JWT header is missing alg.")
        if algorithm.startswith(("RS", "ES", "PS")) and not self._looks_like_asymmetric_key(self.options.jwt_secret):
            raise ValueError(
                f"JWT algorithm {algorithm} requires JWKS or a public key. "
                "Set uns.jwksWellKnownUrl in config.json or pass a PEM public key as jwtSecret."
            )

        return dict(
            jwt.decode(
                token,
                self.options.jwt_secret,
                algorithms=[algorithm],
                options={"verify_aud": False},
            )
        )

    def _looks_like_asymmetric_key(self, value: Optional[str]) -> bool:
        if not value:
            return False
        return "-----BEGIN PUBLIC KEY-----" in value or "-----BEGIN CERTIFICATE-----" in value

    def _validate_query_params(self, request: Any, query_params: list[QueryParamDef]) -> Any:
        from fastapi.responses import JSONResponse

        missing_params: list[str] = []
        for param in query_params:
            value = request.query_params.get(param.name)
            if value is None:
                if param.required:
                    missing_params.append(param.name)
                continue
            if param.type == "number":
                try:
                    float(value)
                except ValueError:
                    return JSONResponse({"error": f"Query param {param.name} must be a number"}, status_code=400)
            if param.type == "boolean" and value not in {"true", "false", "1", "0"}:
                return JSONResponse({"error": f"Query param {param.name} must be boolean"}, status_code=400)
        if missing_params:
            return JSONResponse({"error": f"Missing query params: {', '.join(missing_params)}"}, status_code=400)
        return None

    def _ensure_swagger_path(self, api_path: str) -> None:
        self.swagger_spec["paths"].setdefault(api_path, {})

    def _query_param_to_registry_item(self, param: QueryParamDef) -> dict[str, Any]:
        result: dict[str, Any] = {
            "name": param.name,
            "type": param.type,
            "required": param.required,
            "description": param.description,
        }
        if param.chat_canonical is not None:
            result["chatCanonical"] = param.chat_canonical
        if param.default_value is not None:
            result["defaultValue"] = param.default_value
        return result

    def _build_get_swagger_operation(self, options: GetEndpointOptions) -> dict[str, Any]:
        canonical_params = {
            param.chat_canonical: param.name
            for param in options.query_params
            if param.chat_canonical
        }
        defaults = {
            param.name: param.default_value
            for param in options.query_params
            if param.default_value is not None
        }
        defaults.update(options.chat_defaults)
        operation: dict[str, Any] = {
            "summary": options.api_description or "No description",
            "tags": options.tags,
            "parameters": [
                {
                    "name": param.name,
                    "in": "query",
                    "required": param.required,
                    "schema": {
                        "type": param.type,
                        **({"default": param.default_value} if param.default_value is not None else {}),
                    },
                    "description": param.description,
                    **({"x-uns-chat-canonical": param.chat_canonical} if param.chat_canonical else {}),
                }
                for param in options.query_params
            ],
            "responses": {
                "200": {"description": "OK"},
                "400": {"description": "Bad Request"},
                "401": {"description": "Unauthorized"},
                "403": {"description": "Forbidden"},
            },
        }
        if canonical_params or defaults:
            operation["x-uns-chat"] = {
                "canonicalParams": canonical_params,
                "defaults": defaults,
            }
        return operation

    def _build_post_swagger_operation(self, options: PostEndpointOptions) -> dict[str, Any]:
        operation: dict[str, Any] = {
            "summary": options.api_description or "No description",
            "tags": options.tags,
            "responses": {
                "200": {"description": "OK"},
                "400": {"description": "Bad Request"},
                "401": {"description": "Unauthorized"},
                "403": {"description": "Forbidden"},
            },
        }
        if options.request_body:
            operation["requestBody"] = {
                "description": options.request_body.get("description"),
                "required": options.request_body.get("required", True),
                "content": {
                    "application/json": {
                        "schema": options.request_body.get("schema") or {"type": "object"},
                    }
                },
            }
        return operation

    def _event_name_for_method(self, method: str) -> str:
        upper = method.upper()
        if upper == "POST":
            return "apiPostEvent"
        if upper == "PUT":
            return "apiPutEvent"
        if upper == "PATCH":
            return "apiPatchEvent"
        if upper == "DELETE":
            return "apiDeleteEvent"
        return "apiPostEvent"
