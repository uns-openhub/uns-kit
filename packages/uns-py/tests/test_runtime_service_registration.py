from __future__ import annotations

import pytest

from uns_kit.core import RuntimeServiceDescriptor, register_service


class _Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def post(self, endpoint: str, data: dict[str, object] | None = None) -> dict[str, object]:
        self.calls.append((endpoint, data or {}))
        return {
            "registered": True,
            "service": {"rttNode": "uns-example", "instanceId": "instance-1", "serviceVersion": "1.2.3"},
            "controller": {"restPath": "/api", "graphqlPath": "/graphql"},
        }


def test_direct_service_does_not_register() -> None:
    client = _Client()

    registration = register_service(
        client,
        RuntimeServiceDescriptor(id="uns-example"),
        environment={},
    )

    assert registration is None
    assert client.calls == []


def test_controller_managed_service_registers_non_secret_descriptor() -> None:
    client = _Client()

    registration = register_service(
        client,
        RuntimeServiceDescriptor(
            id="uns-example",
            version="1.2.3",
            capabilities=["history", "history", ""],
            health_contract="service-metadata-v1",
            process_name="uns-example-instance-1",
        ),
        environment={"RTT_NODE": "uns-example", "RTT_INSTANCE_ID": "instance-1"},
    )

    assert registration is not None
    assert registration.rest_path == "/api"
    assert client.calls == [
        (
            "runtime-services/register",
            {
                "rttNode": "uns-example",
                "instanceId": "instance-1",
                "serviceVersion": "1.2.3",
                "capabilities": ["history"],
                "healthContract": "service-metadata-v1",
                "processName": "uns-example-instance-1",
            },
        )
    ]


def test_controller_managed_service_rejects_rtt_identity_mismatch() -> None:
    with pytest.raises(ValueError, match="does not match"):
        register_service(
            _Client(),
            RuntimeServiceDescriptor(id="other-service"),
            environment={"RTT_NODE": "uns-example", "RTT_INSTANCE_ID": "instance-1"},
        )
