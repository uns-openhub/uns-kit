from __future__ import annotations

import asyncio
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import jwt
import pytest

from uns_kit import AsyncUnsClient, ClientError, TopicBuilder
from uns_kit.api import ServiceApiAuthority, ServiceAuthorizationError, UnsApiProxy

CASES = json.loads((Path(__file__).resolve().parents[3] / "tests/fixtures/service-authority-v1.json").read_text())


@pytest.fixture
def authority_endpoint():
    state = {"body": {"active": True, "protocolVersion": 1, "principalType": "machine", "configuredAccessAuthority": 2}, "status": 200, "requests": []}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            state["requests"].append((self.path, self.headers.get("Authorization")))
            payload = state["body"]
            data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
            self.send_response(state["status"])
            if state.get("redirect"):
                self.send_header("Location", state["redirect"])
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/api/auth/token-status", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
async def test_current_authority_matches_typescript_wire_cases(authority_endpoint, case) -> None:
    url, state = authority_endpoint
    state["body"] = case["body"]
    state["status"] = case.get("httpStatus", 200)
    authority = ServiceApiAuthority(None if case.get("noStatusUrl") else url)
    status = 200
    try:
        try:
            await authority.authorize(case["claims"], "Bearer fixture")
        except ServiceAuthorizationError as error:
            status = error.status
        assert status == case["expectedStatus"]
        assert len(state["requests"]) == case.get("expectedProbes", 1)
    finally:
        await authority.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [b"not-json", b"[]", b"x" * 4097])
async def test_invalid_or_oversized_authority_never_grants_access(authority_endpoint, body) -> None:
    url, state = authority_endpoint
    state["body"] = body
    authority = ServiceApiAuthority(url)
    try:
        with pytest.raises(ServiceAuthorizationError) as caught:
            await authority.authorize({"serviceToken": True}, "Bearer fixture")
        assert caught.value.status == 503
    finally:
        await authority.close()


@pytest.mark.asyncio
async def test_authority_redirect_never_forwards_bearer_to_another_path(authority_endpoint) -> None:
    url, state = authority_endpoint
    state["status"] = 307
    state["redirect"] = url.replace("token-status", "must-not-receive-token")
    authority = ServiceApiAuthority(url)
    try:
        with pytest.raises(ServiceAuthorizationError) as caught:
            await authority.authorize({"serviceToken": True}, "Bearer synthetic-fixture")
        assert caught.value.status == 503
        assert state["requests"] == [("/api/auth/token-status", "Bearer synthetic-fixture")]
    finally:
        await authority.close()


@pytest.mark.asyncio
async def test_authority_timeout_keeps_underlying_capacity_bounded(monkeypatch) -> None:
    authority = ServiceApiAuthority("http://localhost/unused")
    release = threading.Event()
    monkeypatch.setattr(authority, "_probe", lambda authorization: release.wait(3))
    try:
        results = await asyncio.gather(*(authority.authorize({"serviceToken": True}, "Bearer fixture") for _ in range(16)), return_exceptions=True)
        assert all(isinstance(result, ServiceAuthorizationError) and result.status == 503 for result in results)
        assert authority._worker.pending == 16
        with pytest.raises(ServiceAuthorizationError, match="capacity"):
            await authority.authorize({"serviceToken": True}, "Bearer fixture")
    finally:
        release.set()
        await authority.close()


@pytest.mark.asyncio
async def test_real_signed_service_api_rechecks_revocation_before_each_effect(authority_endpoint) -> None:
    url, state = authority_endpoint

    class Broker:
        async def publish_raw(self, *args, **kwargs):
            pass

    key = "synthetic-local-service-api-key-" * 3
    proxy = UnsApiProxy(Broker(), process_name="fixture", instance_name="api", topic_builder=TopicBuilder("fixture", "1.0.0", "fixture"), options={"jwtSecret": key, "serviceAuthorizationStatusUrl": url})
    effects = []

    async def handle(event):
        effects.append("applied")
        event.res.json({"ok": True})

    proxy.event.on("apiPostEvent", handle)
    await proxy.start()
    await proxy.post("example/site", "asset", "equipment", "main", "run")
    path = "example/site/asset/equipment/main/run"
    claims = {"sub": "fixture-machine", "serviceToken": True, "identityType": "machine", "accessRules": [path], "exp": int(time.time()) + 60}
    try:
        async with AsyncUnsClient(f"http://127.0.0.1:{proxy._port}", token=jwt.encode(claims, key, algorithm="HS256")) as client:
            assert await client.post(path) == {"ok": True}
            assert await client.post(path) == {"ok": True}
            assert len(state["requests"]) == 2
            state["body"] = {"active": False, "protocolVersion": 1}
            with pytest.raises(ClientError) as caught:
                await client.post(path)
            assert caught.value.status_code == 403
        for changes in ({"configuredService": {"protocol": 2}}, {"unsOperatorDelegation": {}}, {"accessRules": []}, {"accessRules": ["other/#"]}, {"accessRules": ["example/#/invalid"]}):
            async with AsyncUnsClient(f"http://127.0.0.1:{proxy._port}", token=jwt.encode({**claims, **changes}, key, algorithm="HS256")) as client:
                with pytest.raises(ClientError) as caught:
                    await client.post(path)
                assert caught.value.status_code == 403
        async with AsyncUnsClient(f"http://127.0.0.1:{proxy._port}", token=jwt.encode(claims, "wrong-synthetic-key-" * 4, algorithm="HS256")) as client:
            with pytest.raises(ClientError) as caught:
                await client.post(path)
            assert caught.value.status_code == 401
        assert effects == ["applied", "applied"]
        assert len(state["requests"]) == 3
    finally:
        await proxy.stop()
