from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Protocol, Sequence


@dataclass(frozen=True)
class RuntimeServiceDescriptor:
    """Non-secret description sent by a controller-managed RTT service."""

    id: str
    version: Optional[str] = None
    capabilities: Optional[Sequence[str]] = None
    health_contract: Optional[str] = None
    process_name: Optional[str] = None


@dataclass(frozen=True)
class RuntimeServiceRegistration:
    registered: bool
    rtt_node: str
    instance_id: str
    rest_path: str
    graphql_path: str
    service_version: Optional[str] = None


class RuntimeServiceRegistrationClient(Protocol):
    def post(self, endpoint: str, data: Optional[dict[str, Any]] = None) -> dict[str, Any]: ...


def register_service(
    client: RuntimeServiceRegistrationClient,
    service: RuntimeServiceDescriptor,
    *,
    environment: Optional[Mapping[str, str]] = None,
) -> Optional[RuntimeServiceRegistration]:
    """Register only controller-launched RTT services.

    Direct development intentionally has no controller-injected RTT identity,
    so the function returns ``None`` and never attempts registration.
    """

    env = environment if environment is not None else os.environ
    rtt_node = _optional_string(env.get("RTT_NODE"))
    instance_id = _optional_string(env.get("RTT_INSTANCE_ID"))
    if not rtt_node or not instance_id:
        return None

    service_id = _optional_string(service.id)
    if not service_id:
        raise ValueError("Runtime service registration requires service.id.")
    if service_id != rtt_node:
        raise ValueError("Runtime service id does not match the controller-injected RTT_NODE.")

    capabilities = _normalize_string_list(service.capabilities)
    payload: dict[str, Any] = {"rttNode": rtt_node, "instanceId": instance_id}
    if service_version := _optional_string(service.version):
        payload["serviceVersion"] = service_version
    if capabilities:
        payload["capabilities"] = capabilities
    if health_contract := _optional_string(service.health_contract):
        payload["healthContract"] = health_contract
    if process_name := _optional_string(service.process_name):
        payload["processName"] = process_name

    return _parse_registration(client.post("runtime-services/register", payload))


def _parse_registration(payload: Mapping[str, Any]) -> RuntimeServiceRegistration:
    service = payload.get("service")
    controller = payload.get("controller")
    service_values = service if isinstance(service, Mapping) else {}
    controller_values = controller if isinstance(controller, Mapping) else {}
    rtt_node = _optional_string(service_values.get("rttNode"))
    instance_id = _optional_string(service_values.get("instanceId"))
    rest_path = _optional_string(controller_values.get("restPath"))
    graphql_path = _optional_string(controller_values.get("graphqlPath"))
    if payload.get("registered") is not True or not rtt_node or not instance_id or not rest_path or not graphql_path:
        raise RuntimeError("Controller returned an invalid runtime service registration response.")
    return RuntimeServiceRegistration(
        registered=True,
        rtt_node=rtt_node,
        instance_id=instance_id,
        rest_path=rest_path,
        graphql_path=graphql_path,
        service_version=_optional_string(service_values.get("serviceVersion")),
    )


def _optional_string(value: object) -> Optional[str]:
    normalized = value.strip() if isinstance(value, str) else ""
    return normalized or None


def _normalize_string_list(values: Optional[Sequence[str]]) -> list[str]:
    normalized: list[str] = []
    for value in values or ():
        candidate = _optional_string(value)
        if candidate and candidate not in normalized:
            normalized.append(candidate)
    return normalized
