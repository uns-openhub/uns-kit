from __future__ import annotations

from uns_kit.api import UnsApiProxy
from uns_kit.core import (
    AuthClient,
    ConfigFile,
    RuntimeServiceDescriptor,
    ServiceTokenProvider,
    UnsClient,
    UnsProxyProcess,
    UnsProxyProcessSync,
    register_service,
)
from uns_kit.cron import UnsCronProxy
from uns_kit.database import DatabaseManager, compile_named_params


def test_namespaced_exports_exist() -> None:
    assert ConfigFile is not None
    assert UnsProxyProcess is not None
    assert UnsProxyProcessSync is not None
    assert UnsApiProxy is not None
    assert UnsCronProxy is not None
    assert UnsClient is not None
    assert AuthClient is not None
    assert ServiceTokenProvider is not None
    assert RuntimeServiceDescriptor is not None
    assert register_service is not None
    assert DatabaseManager is not None
    statement = compile_named_params("sqlite", "select * from table where id = :id", {"id": 1})
    assert statement.text == "select * from table where id = ?"
