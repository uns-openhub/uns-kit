from __future__ import annotations

import asyncio
import json
import threading
import time
import runpy
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from uns_kit import AsyncCapacityError, AsyncUnsClient, ClientError, ServiceTokenProvider, UnsClient


@pytest.fixture
def endpoint(monkeypatch):
    for key in ("UNS_SERVICE_TOKEN", "UNS_SERVICE_TOKEN_FILE"):
        monkeypatch.delenv(key, raising=False)
    requests = []
    entered, release = threading.Event(), threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.respond()

        def do_POST(self):
            self.respond()

        def respond(self):
            raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            requests.append((self.path, self.headers.get("Authorization"), json.loads(raw) if raw else None))
            if self.path == "/api/blocked":
                entered.set()
                release.wait(2)
            if self.path == "/api/redirect":
                self.send_response(307)
                self.send_header("Location", "/api/redirect-target")
                self.end_headers()
                return
            if self.path == "/api/denied":
                self.send_response(403)
                self.end_headers()
                self.wfile.write(b"private upstream payload")
                return
            if self.path == "/api/trickle":
                self.send_response(200)
                self.send_header("Content-Length", "100")
                self.end_headers()
                try:
                    for _ in range(100):
                        self.wfile.write(b"x")
                        self.wfile.flush()
                        time.sleep(0.01)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return
            body = json.dumps({"results": [], "ok": True, "padding": "x" * 100}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests, entered, release
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_async_history_rereads_rotated_file_without_human_credentials(endpoint, tmp_path) -> None:
    base, requests, _, _ = endpoint
    token = tmp_path / "token"
    token.write_text("first-fixture-token")
    async with AsyncUnsClient(base, token_provider=ServiceTokenProvider(token_file=str(token))) as client:
        assert (await client.history("example/line/temperature", from_="2026-01-01", limit=10)).results == []
        token.write_text("rotated-fixture-token")
        await client.last_value("warehouse/pump/pressure")
        token.unlink()
        with pytest.raises(RuntimeError, match="Controller-managed"):
            await client.history("warehouse/pump/pressure")
    assert [item[1] for item in requests] == ["Bearer first-fixture-token", "Bearer rotated-fixture-token"]
    assert requests[0][2]["from"] == "2026-01-01"


@pytest.mark.asyncio
async def test_missing_service_credential_never_attempts_implicit_login(endpoint, monkeypatch) -> None:
    from uns_kit.core.auth_client import AuthClient
    monkeypatch.setattr(AuthClient, "create", lambda: pytest.fail("implicit human login"))
    async with AsyncUnsClient(endpoint[0]) as client:
        with pytest.raises(RuntimeError, match="Service credential unavailable"):
            await client.history("example/source")
    assert endpoint[1] == []


@pytest.mark.asyncio
async def test_denial_redirect_foreign_origin_and_response_limit_fail_without_replay(endpoint) -> None:
    base, requests, _, _ = endpoint
    async with AsyncUnsClient(base, token="fixture-token") as client:
        with pytest.raises(ClientError) as caught:
            await client.post("denied")
        assert caught.value.status_code == 403
        assert "private upstream" not in str(caught.value)
        with pytest.raises(ClientError) as caught:
            await client.post("redirect")
        assert caught.value.status_code == 307
        with pytest.raises(ClientError, match="configured origin"):
            await client.get("https://example.invalid/foreign")
    assert [path for path, _, _ in requests] == ["/api/denied", "/api/redirect"]
    async with AsyncUnsClient(base, token="fixture-token", max_response_bytes=20) as client:
        with pytest.raises(ClientError, match="size limit"):
            await client.get("large")


@pytest.mark.asyncio
async def test_synchronous_service_client_uses_the_same_safe_transport(endpoint) -> None:
    client = UnsClient(endpoint[0], token_provider=ServiceTokenProvider(environment={"UNS_SERVICE_TOKEN": "fixture"}))
    with pytest.raises(ClientError) as caught:
        await asyncio.to_thread(client.post, "redirect")
    assert caught.value.status_code == 307
    with pytest.raises(ClientError, match="configured origin"):
        await asyncio.to_thread(client.get, "https://example.invalid/foreign")
    assert [item[0] for item in endpoint[1]] == ["/api/redirect"]


@pytest.mark.asyncio
@pytest.mark.parametrize("credential", ["fixture-secret\ninjected", "fixture-secret\rinjected", "fixture-secret\u2603"])
async def test_malformed_credentials_are_rejected_without_disclosure_or_transport(endpoint, credential) -> None:
    async with AsyncUnsClient(endpoint[0], token=credential) as client:
        with pytest.raises(ClientError, match="invalid HTTP header") as caught:
            await client.get("never-sent")
    assert credential not in str(caught.value)
    assert "fixture-secret" not in str(caught.value)
    assert endpoint[1] == []


@pytest.mark.asyncio
async def test_body_trickles_do_not_extend_http_budget(endpoint) -> None:
    async with AsyncUnsClient(endpoint[0], token="fixture", timeout=0.05) as client:
        with pytest.raises(ClientError, match="timed out"):
            await client.get("trickle")


@pytest.mark.asyncio
async def test_queued_payload_is_bound_before_caller_mutation(endpoint) -> None:
    base, requests, entered, release = endpoint
    client = AsyncUnsClient(base, token="fixture", max_pending=2)
    blocked = asyncio.create_task(client.post("blocked"))
    queued = None
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        payload = {"operation": {"id": "original"}}
        queued = asyncio.create_task(client.post("next", payload))
        while client._worker.pending < 2:
            await asyncio.sleep(0)
        payload["operation"]["id"] = "changed"
    finally:
        release.set()
        await blocked
        if queued is not None:
            await queued
        await client.close()
    assert requests[1][2] == {"operation": {"id": "original"}}


@pytest.mark.asyncio
async def test_cancelled_post_remains_owned_until_close_without_replaying_effect(endpoint) -> None:
    base, requests, entered, release = endpoint
    client = AsyncUnsClient(base, token="fixture-token", max_pending=1)
    post = asyncio.create_task(client.post("blocked", {"requestId": "fixture-operation"}))
    close = None
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        post.cancel()
        with pytest.raises(asyncio.CancelledError):
            await post
        with pytest.raises(AsyncCapacityError):
            await client.get("unexpected")
        close = asyncio.create_task(client.close())
        await asyncio.sleep(0)
        assert not close.done()
    finally:
        release.set()
        if close is not None:
            await close
        await client.close()
    assert len(requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("name,source", [
    ("temperature-reader", "example/production/line-1/equipment/main/temperature"),
    ("warehouse-reader", "warehouse/cold-store/pump-7/equipment/main/pressure"),
])
async def test_two_real_example_entry_functions_use_their_declared_source(endpoint, monkeypatch, name, source) -> None:
    monkeypatch.setenv("UNS_SERVICE_TOKEN", "synthetic-example-token")
    script = Path(__file__).resolve().parents[3] / "examples/python-service-access" / name / "main.py"
    entry = runpy.run_path(str(script))["read_history"]
    assert (await entry(endpoint[0], "2026-01-01", "2026-01-02")).results == []
    assert endpoint[1][0][1] == "Bearer synthetic-example-token"
    assert endpoint[1][0][2]["topics"] == [source]
    assert endpoint[1][0][2]["limit"] == 100
