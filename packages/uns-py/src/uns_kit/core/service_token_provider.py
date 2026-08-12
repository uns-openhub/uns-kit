from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, Mapping, Optional, Protocol


class AccessTokenProvider(Protocol):
    """Minimal bearer-token source shared by service clients."""

    def get_access_token(self) -> Optional[str]: ...


class ServiceTokenProvider:
    """Resolve controller-managed service credentials without storing a user token.

    A controller-mounted token file is deliberately authoritative and is read on
    every request, so a controller can rotate the service token atomically
    without restarting its RTT process. Direct local development can instead
    supply ``UNS_SERVICE_TOKEN``, a resolved ``uns.token``, or a legacy login
    provider.
    """

    def __init__(
        self,
        *,
        token_file: Optional[str] = None,
        environment: Optional[Mapping[str, str]] = None,
        config_token: Optional[str] = None,
        fallback: Optional[AccessTokenProvider] = None,
        read_text: Optional[Callable[[str], str]] = None,
    ) -> None:
        self._token_file = _normalize_token(token_file)
        self._environment = environment if environment is not None else os.environ
        self._config_token = _normalize_token(config_token)
        self._fallback = fallback
        self._read_text = read_text or _read_token_file

    def get_access_token(self) -> Optional[str]:
        token_file = self._token_file or _normalize_token(self._environment.get("UNS_SERVICE_TOKEN_FILE"))
        if token_file:
            return self._read_controller_managed_token(token_file)

        environment_token = _normalize_token(self._environment.get("UNS_SERVICE_TOKEN"))
        if environment_token:
            return environment_token

        if self._config_token:
            return self._config_token

        return self._fallback.get_access_token() if self._fallback else None

    def _read_controller_managed_token(self, path: str) -> str:
        try:
            token = _normalize_token(self._read_text(path))
            if not token:
                raise ValueError("empty token file")
            return token
        except Exception as exc:
            raise RuntimeError("Controller-managed service token file is unavailable.") from exc


def _read_token_file(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _normalize_token(value: Optional[str]) -> Optional[str]:
    token = value.strip() if isinstance(value, str) else ""
    return token or None
