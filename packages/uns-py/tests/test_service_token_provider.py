from __future__ import annotations

import pytest

from uns_kit.core import ServiceTokenProvider, UnsClient


def test_controller_managed_token_file_is_re_read_for_each_request() -> None:
    values = iter(["first-token\n", "second-token\n"])
    provider = ServiceTokenProvider(
        environment={"UNS_SERVICE_TOKEN_FILE": "/run/uns/service-token"},
        read_text=lambda _path: next(values),
    )
    client = UnsClient("https://datahub.example.com", token_provider=provider)

    assert client.ensure_token() == "first-token"
    assert client.ensure_token() == "second-token"


def test_controller_managed_token_file_is_authoritative() -> None:
    class _Fallback:
        calls = 0

        def get_access_token(self) -> str:
            self.calls += 1
            return "legacy-token"

    fallback = _Fallback()
    provider = ServiceTokenProvider(
        environment={"UNS_SERVICE_TOKEN_FILE": "/missing"},
        fallback=fallback,
        read_text=lambda _path: (_ for _ in ()).throw(FileNotFoundError()),
    )

    with pytest.raises(RuntimeError, match="Controller-managed service token file is unavailable"):
        provider.get_access_token()
    assert fallback.calls == 0


def test_service_token_provider_uses_direct_development_sources_in_order() -> None:
    class _Fallback:
        def get_access_token(self) -> str:
            return "legacy-token"

    assert ServiceTokenProvider(
        environment={"UNS_SERVICE_TOKEN": " environment-token "},
        config_token="config-token",
        fallback=_Fallback(),
    ).get_access_token() == "environment-token"
    assert ServiceTokenProvider(
        environment={},
        config_token="config-token",
        fallback=_Fallback(),
    ).get_access_token() == "config-token"
    assert ServiceTokenProvider(environment={}, fallback=_Fallback()).get_access_token() == "legacy-token"
