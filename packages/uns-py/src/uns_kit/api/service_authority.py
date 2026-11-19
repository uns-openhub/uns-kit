"""Current authority for already cryptographically verified service claims."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Mapping

from ..core.async_worker import AsyncCapacityError, BoundedAsyncWorker
from ..core.datahub_client import ClientError
from ..core.http_transport import bounded_http_request, http_origin

SERVICE_AUTHORIZATION_HINTS = (
    "serviceToken", "machineIdentityId", "machineIdentityName", "workloadIdentityType",
    "configuredService", "configuredServiceLaunch", "configuredAccess", "catalogPublication",
    "catalogLaunch", "runtimeRouteDeployment",
)


def has_service_authorization_hint(claims: Mapping[str, Any]) -> bool:
    return any(key in claims for key in SERVICE_AUTHORIZATION_HINTS) or claims.get("identityType") == "machine"


class ServiceAuthorizationError(RuntimeError):
    def __init__(self, code: str, status: int) -> None:
        self.code = code
        self.status = status
        super().__init__(code)


class ServiceApiAuthority:
    """No positive cache: check current Controller authority on each request.

    The trusted URL comes from process options/environment, never JWT claims or
    request parameters. Declaration, UNS access and service API permission are
    different authorities. Operator/catalog delegation is not implemented here.
    """

    def __init__(self, status_url: str | None = None) -> None:
        if status_url is not None:
            http_origin(status_url)
        self.status_url = status_url
        self._worker = BoundedAsyncWorker(max_pending=16, workers=4)

    async def authorize(self, claims: Mapping[str, Any], authorization: str) -> None:
        # Ordinary Python endpoints cannot validate purpose-bound delegation.
        if "unsOperatorDelegation" in claims:
            raise ServiceAuthorizationError("operator_endpoint_not_declared", 403)
        if "configuredService" in claims:
            raise ServiceAuthorizationError("service_api_permission_required", 403)
        if not has_service_authorization_hint(claims):
            return
        if self.status_url is None:
            raise ServiceAuthorizationError("service_authorization_status_required", 503)
        try:
            await asyncio.wait_for(self._worker.run(lambda: self._probe(authorization)), timeout=1.0)
        except AsyncCapacityError:
            raise ServiceAuthorizationError("service_authorization_capacity", 503) from None
        except ServiceAuthorizationError:
            raise
        except Exception:
            raise ServiceAuthorizationError("service_authorization_unavailable", 503) from None

    def _probe(self, authorization: str) -> None:
        assert self.status_url is not None
        try:
            response = bounded_http_request(
                "POST", self.status_url, token=authorization.removeprefix("Bearer "),
                timeout=1.0, max_bytes=4096,
            )
        except ClientError as error:
            if error.status_code in (401, 403):
                raise ServiceAuthorizationError("operator_authorization_inactive", 403) from None
            raise ServiceAuthorizationError("service_authorization_unavailable", 503) from None
        try:
            status = json.loads(response.content)
        except (ValueError, UnicodeError):
            raise ServiceAuthorizationError("service_authorization_unavailable", 503) from None
        if not isinstance(status, dict):
            raise ServiceAuthorizationError("service_authorization_unavailable", 503)
        if status.get("active") is not True or type(status.get("protocolVersion")) is not int or status["protocolVersion"] != 1:
            raise ServiceAuthorizationError("operator_authorization_inactive", 403)
        if status.get("principalType") != "machine" or type(status.get("configuredAccessAuthority")) is not int or status["configuredAccessAuthority"] != 2:
            raise ServiceAuthorizationError("service_authorization_unavailable", 503)

    async def close(self) -> None:
        await self._worker.close()
