from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from uns_kit import UnsProxyProcess, build_uns_service_metadata


FIXTURES = json.loads((Path(__file__).resolve().parents[3] / "tests/fixtures/service-metadata-v1.json").read_text())


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda item: item["processName"])
def test_service_metadata_matches_shared_typescript_wire_fixture(fixture) -> None:
    metadata = fixture["metadata"]
    original = json.dumps(metadata)
    actual = build_uns_service_metadata(
        metadata,
        process_name=fixture["processName"],
        process_id=fixture["processId"],
        now=datetime.fromisoformat(fixture["now"].replace("Z", "+00:00")),
    )
    assert actual == fixture["expected"]
    assert json.dumps(metadata) == original


@pytest.mark.parametrize("service_id", [None, "", " "])
def test_service_metadata_requires_a_nonempty_service_id(service_id) -> None:
    with pytest.raises(ValueError, match="serviceId"):
        build_uns_service_metadata({"serviceId": service_id, "kind": "service"}, process_name="p", process_id="id")


@pytest.mark.asyncio
async def test_process_publishes_retained_metadata_on_its_own_topic() -> None:
    process = UnsProxyProcess("localhost", {"processName": "fixture", "packageName": "example", "packageVersion": "1.2.3"})
    publish = AsyncMock()
    process._client.publish_raw = publish
    result = await process.publish_service_metadata({"serviceId": "example", "kind": "service"})
    topic, payload = publish.await_args.args
    assert topic == process.topic_builder.process_status_topic + "service-metadata"
    assert json.loads(payload) == result
    assert publish.await_args.kwargs == {"retain": True}
    assert result["packageName"] == "example"
    assert result["packageVersion"] == "1.2.3"
    assert result["processId"] == process.process_id
    assert "handoverProtocol" not in result


@pytest.mark.asyncio
async def test_invalid_metadata_never_reaches_transport() -> None:
    process = UnsProxyProcess("localhost", {"processName": "fixture"})
    publish = AsyncMock()
    process._client.publish_raw = publish
    for metadata in (
        {"serviceId": " ", "kind": "service"},
        {"serviceId": "example", "kind": "invalid"},
        {"serviceId": "example", "kind": "service", "extra": {"value": float("nan")}},
    ):
        with pytest.raises(ValueError):
            await process.publish_service_metadata(metadata)
    publish.assert_not_called()
