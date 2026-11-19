"""Controller service discovery using the TypeScript schemaVersion=1 wire format."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal, Mapping, TypedDict

from .runtime_metadata import RUNTIME_METADATA

UnsServiceKind = Literal["core", "addon", "capability", "service"]


class _RequiredMetadata(TypedDict):
    serviceId: str
    kind: UnsServiceKind


class UnsServiceMetadataInput(_RequiredMetadata, total=False):
    addonId: str
    label: str
    description: str
    capabilities: list[str]
    apiRoutes: list[dict[str, Any]]
    healthPath: str
    controller: dict[str, str]
    instanceId: str
    packageName: str
    packageVersion: str
    extra: dict[str, Any]


def _optional(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("Service metadata string field must be a string")
    return value.strip() or None


def build_uns_service_metadata(
    metadata: UnsServiceMetadataInput,
    *,
    process_name: str,
    process_id: str,
    package_name: str | None = None,
    package_version: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    service_id = _optional(metadata.get("serviceId"))
    if not service_id:
        raise ValueError("UNS service metadata requires a serviceId")
    kind = metadata.get("kind")
    if kind not in ("core", "addon", "capability", "service"):
        raise ValueError("Invalid UNS service kind")
    instant = now if now is not None else datetime.now(timezone.utc)
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("Service metadata time must include a timezone")
    routes = []
    for route in metadata.get("apiRoutes", []):
        path = _optional(route.get("path"))
        if not path:
            continue
        normalized = {**route, "path": path}
        for key in ("healthPath", "swaggerPath"):
            value = _optional(route.get(key))
            normalized.pop(key, None)
            if value:
                normalized[key] = value
        routes.append(normalized)
    result: dict[str, Any] = {
        "schemaVersion": 1,
        "serviceId": service_id,
        "kind": kind,
        "packageName": _optional(metadata.get("packageName")) or package_name or RUNTIME_METADATA.package_name,
        "packageVersion": _optional(metadata.get("packageVersion")) or package_version or RUNTIME_METADATA.package_version,
        "processName": process_name,
        "processId": process_id,
        "capabilities": list(dict.fromkeys(value for raw in metadata.get("capabilities", []) if (value := _optional(raw)))),
        "apiRoutes": routes,
        "publishedAt": instant.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    }
    for key in ("addonId", "label", "description", "healthPath", "instanceId"):
        value = _optional(metadata.get(key))
        if value:
            result[key] = value
    controller = metadata.get("controller")
    if controller is not None:
        if not isinstance(controller, Mapping):
            raise TypeError("Service controller metadata must be an object")
        clean_controller = {key: value for key in ("name", "publicBase") if (value := _optional(controller.get(key)))}
        if clean_controller:
            result["controller"] = clean_controller
    if "extra" in metadata:
        result["extra"] = metadata["extra"]
    return result
