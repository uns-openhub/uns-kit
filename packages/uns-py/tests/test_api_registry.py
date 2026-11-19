from __future__ import annotations

import asyncio
import io
import json

import pytest

from uns_kit.api import (
    UnsApiProxy,
    build_api_interaction_topic_path,
    build_api_interactions_from_data_offer_sources,
    build_service_api_interactions,
    define_data_catalog_field,
    define_data_catalog_offer_source,
    define_data_catalog_query_param,
    define_data_catalog_schema,
    define_service_api,
    project_rows_for_data_catalog_schema,
    resolve_api_handler_path,
    write_schema_rows_to_parquet,
)
from uns_kit.core import TopicBuilder
from uns_kit.core.proxy import UnsProxy


class _FakeClient:
    def __init__(self) -> None:
        self.published: list[tuple[str, str | bytes, bool]] = []

    async def publish_raw(self, topic: str, payload: str | bytes, retain: bool = False) -> None:
        self.published.append((topic, payload, retain))


@pytest.mark.asyncio
async def test_api_proxy_publishes_service_endpoints_and_data_catalog_offers(monkeypatch) -> None:
    monkeypatch.setenv("UNS_CONTROLLER_NAME", "controller-local")
    monkeypatch.setenv("UNS_CONTROLLER_PUBLIC_BASE", "http://localhost:3200")
    client = _FakeClient()
    proxy = UnsApiProxy(
        client,
        process_name="test-process",
        instance_name="test-api",
        topic_builder=TopicBuilder("uns-kit", "0.0.1", "test-process"),
    )
    assert proxy.get_process_name() == "test-process"
    await proxy.start()

    await proxy.get(
        "enterprise/site/",
        "line-1",
        "motor",
        "main",
        "status",
        {
            "apiDescription": "Service API status",
            "registryTopic": "service-endpoints",
            "serviceApi": {"kind": "service-api"},
        },
    )
    proxy.register_data_offer(
        {
            "offerId": "offer-1",
            "displayName": "Offer 1",
            "operations": [
                {
                    "method": "GET",
                    "path": "/api/enterprise/site/line-1/motor/main/status",
                    "summary": "Status",
                }
            ],
        }
    )
    await asyncio.sleep(0.2)

    published_topics = [topic for topic, _payload, _retain in client.published]
    assert any(topic.endswith("service-endpoints") for topic in published_topics)
    assert any(topic.endswith("data-catalog-offers") for topic in published_topics)
    service_payload = next(
        payload
        for topic, payload, _retain in client.published
        if topic.endswith("service-endpoints")
    )
    assert '"controllerName":"controller-local"' in service_payload
    assert '"controllerPublicBase":"http://localhost:3200"' in service_payload
    offer_payload = next(
        payload
        for topic, payload, _retain in client.published
        if topic.endswith("data-catalog-offers")
    )
    assert '"controllerName":"controller-local"' in offer_payload
    assert '"controllerPublicBase":"http://localhost:3200"' in offer_payload

    await proxy.stop()


def test_ts_like_api_catalog_definitions_are_coerced_into_runtime_interactions() -> None:
    async def status_handler(event, _context) -> None:
        event.res.json({"ok": True})

    async def orders_handler(event, _context) -> None:
        event.res.json({"count": 0, "data": []})

    orders_schema = define_data_catalog_schema(
        {
            "id": "orders",
            "title": "Orders",
            "contentType": "application/json",
            "fields": [
                define_data_catalog_field("count", "number", "Count"),
                define_data_catalog_field("data", "array", "Rows"),
                define_data_catalog_field("orderId", "string", "Order id", path="data[].orderId"),
            ],
        }
    )

    service_apis = {
        "status": define_service_api(
            {
                "attribute": "status",
                "method": "GET",
                "description": "Service status endpoint",
                "handler": status_handler,
            }
        )
    }
    data_offer_sources = {
        "orders": define_data_catalog_offer_source(
            {
                "offer_id": "orders",
                "topic": "factory/demo/app/",
                "asset": "orders",
                "object_type": "dataset",
                "object_id": "production",
                "attribute": "list",
                "display_name": "Orders",
                "description": "Browse orders.",
                "query_params": [define_data_catalog_query_param("status", "Filter status")],
                "schema": orders_schema,
                "handler": orders_handler,
            }
        )
    }

    service_routes = build_service_api_interactions("test-process", service_apis)
    offer_routes = build_api_interactions_from_data_offer_sources(data_offer_sources)

    assert build_api_interaction_topic_path(service_routes["status"]) == "system/service/runtime/test-process/status"
    assert build_api_interaction_topic_path(offer_routes["orders"]) == "factory/demo/app/orders/dataset/production/list"
    assert offer_routes["orders"].options["registryTopic"] == "data-offer-endpoints"
    assert service_routes["status"].options["serviceApi"] == {
        "id": "status",
        "summary": "Service status endpoint",
        "description": "Service status endpoint",
        "tags": [],
        "parameters": [],
        "headers": [],
        "requestBody": None,
        "responses": [
            {
                "statusCode": "200",
                "description": "Service status endpoint",
                "contentType": None,
                "schemas": [],
                "schemaIds": [],
                "examplePayloads": [],
                "headers": [],
            }
        ],
        "schemas": [],
    }


def test_project_rows_and_parquet_helpers_match_ts_style_usage() -> None:
    schema = define_data_catalog_schema(
        {
            "id": "demo-export-columns",
            "title": "Sample Coil Export Columns",
            "contentType": "application/octet-stream",
            "fields": [
                define_data_catalog_field("orderId", "string", "Order identifier"),
                define_data_catalog_field("coilId", "string", "Coil identifier"),
                define_data_catalog_field("length", "number", "Measured strip length"),
            ],
        }
    )
    source_rows = [
        {"ORDERID": "ORD-1001", "COILID": "COIL-10024", "LENGTH": 1280},
        {"ORDERID": "ORD-1002", "COILID": "COIL-10025", "LENGTH": 1325},
    ]

    projected_rows = project_rows_for_data_catalog_schema(source_rows, schema)

    assert projected_rows == [
        {"orderId": "ORD-1001", "coilId": "COIL-10024", "length": 1280},
        {"orderId": "ORD-1002", "coilId": "COIL-10025", "length": 1325},
    ]

    payload = write_schema_rows_to_parquet(rows=projected_rows, schema=schema)

    import pandas as pd

    frame = pd.read_parquet(io.BytesIO(payload))
    assert frame.to_dict(orient="records") == projected_rows


def test_resolve_api_handler_path_strips_api_prefix() -> None:
    class _ApiInput:
        api_base_prefix = "/api"

    assert resolve_api_handler_path(_ApiInput(), "/api/projects/demo/path") == "projects/demo/path"
    assert resolve_api_handler_path(_ApiInput(), "/projects/demo/path") == "projects/demo/path"


@pytest.mark.asyncio
@pytest.mark.parametrize("package,process,topic,asset", [
    ("test-app", "test-process", "enterprise/site/", "line-1"),
    ("weather-reader", "weather-local", "campus/north/", "weather-station"),
])
async def test_default_api_registry_uses_controller_topic_on_register_and_remove(
    package, process, topic, asset,
) -> None:
    client = _FakeClient()
    status_topic = f"uns-infra/{package}/1-0-0/{process}/api/"
    proxy = UnsProxy(client, status_topic, "api")
    events = []

    async def observe(event):
        events.append(event)

    proxy.event.on("unsProxyProducedApiEndpoints", observe)
    endpoint = {
        "topic": topic, "asset": asset,
        "objectType": "motor", "objectId": "main", "attribute": "status",
        "apiMethod": "GET", "apiHost": "http://127.0.0.1:9000",
        "apiEndpoint": f"/api/{topic}{asset}/motor/main/status",
    }
    await proxy.register_api_endpoint(endpoint)
    registry_topic, payload, retained = client.published[-1]
    assert registry_topic == status_topic + "api-endpoints"
    assert retained is True
    assert json.loads(payload) == [endpoint]
    assert events[-1]["statusTopic"] == registry_topic

    await proxy.unregister_api_endpoint(
        topic, asset, "motor", "main", "status", "GET"
    )
    assert client.published[-1] == (registry_topic, "[]", True)
    assert events[-1]["producedApiEndpoints"] == []
    assert events[-1]["statusTopic"] == registry_topic
