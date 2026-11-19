"""Credential-bearing HTTP transport: fixed origin, no redirects or replay."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from .datahub_client import ClientError, RawResponse


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def http_origin(url: str) -> tuple[str, str, int]:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username is not None or parsed.password is not None:
        raise ValueError("Expected an absolute HTTP(S) URL without credentials")
    if parsed.fragment:
        raise ValueError("HTTP URL must not have a fragment")
    port = parsed.port if parsed.port is not None else (443 if parsed.scheme == "https" else 80)
    return parsed.scheme, parsed.hostname.lower(), port


def bounded_http_request(
    method: str,
    url: str,
    *,
    token: str | None,
    timeout: float,
    max_bytes: int,
    body: dict[str, Any] | None = None,
) -> RawResponse:
    if token is not None and (not isinstance(token, str) or any(not 33 <= ord(char) <= 126 for char in token)):
        # urllib/http.client otherwise include a malformed Authorization value
        # in ValueError/encoding diagnostics, exposing the service credential.
        raise ClientError("UNS credential contains invalid HTTP header characters")
    data = None if body is None else json.dumps(body, allow_nan=False).encode("utf-8")
    request = urllib.request.Request(url, data=data, method=method, headers={
        "Accept": "application/json",
        **({"Content-Type": "application/json"} if data is not None else {}),
        **({"Authorization": f"Bearer {token}"} if token else {}),
    })
    deadline = time.monotonic() + timeout
    try:
        with urllib.request.build_opener(_NoRedirect()).open(request, timeout=timeout) as response:
            length = response.headers.get("Content-Length")
            if length is not None and (not length.isdecimal() or int(length) > max_bytes):
                raise ClientError("UNS response exceeds the configured size limit")
            content = bytearray()
            while True:
                if time.monotonic() >= deadline:
                    raise ClientError("UNS HTTP response timed out")
                # read1 returns available bytes rather than waiting for the
                # whole body, so slow trickles cannot renew the total budget.
                part = response.read1(min(65536, max_bytes + 1 - len(content)))
                if time.monotonic() >= deadline:
                    raise ClientError("UNS HTTP response timed out")
                if not part:
                    break
                content.extend(part)
                if len(content) > max_bytes:
                    raise ClientError("UNS response exceeds the configured size limit")
            return RawResponse(status_code=response.status, headers=dict(response.headers.items()), content=bytes(content))
    except urllib.error.HTTPError as error:
        status = error.code
        error.close()
        # Do not expose upstream response bodies, query strings or credentials.
        raise ClientError(f"UNS request failed with HTTP {status}", status_code=status) from None
    except (urllib.error.URLError, TimeoutError, OSError):
        raise ClientError("UNS HTTP transport unavailable or timed out") from None
