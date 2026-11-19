"""Opt-in application drain hooks; these do not declare hot handover support."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable


@dataclass(frozen=True)
class ProcessLifecycleHooks:
    """Stop new admission synchronously, then finish accepted work asynchronously.

    The application owns its input admission, writers and resource cleanup.
    A drain timeout does not cancel accepted work or prove that effects stopped.
    """

    drain: Callable[[], Awaitable[None]]
    on_release: Callable[[], None] | None = None

    def __post_init__(self) -> None:
        if not callable(self.drain) or (self.on_release is not None and not callable(self.on_release)):
            raise TypeError("Invalid process lifecycle hooks")


class ProcessShutdownError(RuntimeError):
    """Python 3.10-compatible aggregation of independent cleanup failures."""

    def __init__(self, errors: list[BaseException]) -> None:
        self.errors = tuple(errors)
        super().__init__(f"Process shutdown has {len(errors)} unconfirmed cleanup operation(s)")
