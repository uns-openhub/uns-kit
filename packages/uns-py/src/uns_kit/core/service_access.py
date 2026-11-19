"""Protocol-1 background capability requests, never permission grants."""

from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path
from typing import Any, Literal, TypedDict

SERVICE_RESOURCE_SCOPES = (
    "read:uns", "identity:asset:publish", "runtime:captures:read",
    "runtime:captures:write", "runtime:triggers:read",
)


class ServiceAccessPermissions(TypedDict):
    scopes: list[str]
    accessRules: list[str]


class BackgroundServiceAccessDeclaration(TypedDict):
    protocol: Literal[1]
    permissions: ServiceAccessPermissions


def parse_background_service_access_declaration(value: Any) -> BackgroundServiceAccessDeclaration:
    """The background-only subset. Catalog/configured protocols fail closed."""
    if not isinstance(value, dict) or set(value) != {"protocol", "permissions"}:
        raise ValueError("background_service_access_fields_invalid")
    if type(value["protocol"]) is not int or value["protocol"] != 1:
        raise ValueError("background_service_access_protocol_unsupported")
    permissions = value["permissions"]
    if not isinstance(permissions, dict) or set(permissions) != {"scopes", "accessRules"}:
        raise ValueError("service_access_permissions_invalid")
    scopes, rules = permissions["scopes"], permissions["accessRules"]
    if not isinstance(scopes, list) or not 1 <= len(scopes) <= 5 or any(not isinstance(scope, str) or scope not in SERVICE_RESOURCE_SCOPES for scope in scopes):
        raise ValueError("service_access_scopes_invalid")
    if not isinstance(rules, list) or not 1 <= len(rules) <= 64:
        raise ValueError("service_access_rules_invalid")
    for rule in rules:
        if not isinstance(rule, str) or not 1 <= len(rule) <= 512:
            raise ValueError("service_access_rules_invalid")
        segments = rule.split("/")
        if any(not (part == "+" or part == "#" and index == len(segments) - 1 or re.fullmatch(r"[A-Za-z0-9-]+", part)) for index, part in enumerate(segments)):
            raise ValueError("service_access_rules_invalid")
    return {"protocol": 1, "permissions": {"scopes": sorted(set(scopes)), "accessRules": sorted(set(rules))}}


def _read_json(root: Path, segments: list[str]) -> Any:
    if not hasattr(os, "O_NOFOLLOW") or os.open not in os.supports_dir_fd:
        raise RuntimeError("Safe service declaration loading requires no-follow directory support")
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for segment in segments[:-1]:
            child = os.open(segment, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        file = os.open(segments[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor)
        try:
            info = os.fstat(file)
            if not stat.S_ISREG(info.st_mode) or info.st_size > 65536:
                raise ValueError("service_access_file_invalid")
            data = bytearray()
            while len(data) <= 65536:
                part = os.read(file, 65537 - len(data))
                if not part:
                    break
                data.extend(part)
            if len(data) > 65536:
                raise ValueError("service_access_file_invalid")
            return json.loads(data)
        finally:
            os.close(file)
    finally:
        os.close(descriptor)


def load_background_service_access_declaration(package_file: str | Path) -> BackgroundServiceAccessDeclaration | None:
    """Read bounded package JSON and a safe relative reference; execute no code.

    Use from application setup, or offload this synchronous file helper from an
    async hot path. Supporting Controller review is still required independently.
    """
    file = Path(package_file).absolute()
    root = file.parent.resolve()
    package = _read_json(root, [file.name])
    if not isinstance(package, dict):
        raise ValueError("service_access_package_invalid")
    metadata = package.get("unsDatahub", {})
    if not isinstance(metadata, dict):
        raise ValueError("service_access_package_invalid")
    if "serviceAccess" not in metadata:
        return None
    if "catalogAccess" in metadata:
        raise ValueError("service_access_declaration_conflict")
    declaration = metadata["serviceAccess"]
    if isinstance(declaration, str):
        parts = declaration.split("/")
        if not 1 <= len(declaration) <= 512 or "\\" in declaration or re.match(r"^[A-Za-z]:", declaration) or any(part in ("", ".", "..") for part in parts):
            raise ValueError("service_access_reference_invalid")
        declaration = _read_json(root, parts)
    return parse_background_service_access_declaration(declaration)
