from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Protocol, TypeVar


class ExclusiveRuntimeLock(Protocol):
    def acquire(self) -> None: ...

    def release(self) -> None: ...


class RuntimeSession(Protocol):
    def close(self) -> None: ...


Token = TypeVar("Token")
Session = TypeVar("Session", bound=RuntimeSession)


@dataclass(frozen=True)
class BootstrappedRuntime:
    """Opaque initialized resources; this object grants no trading permission."""

    token_health: object
    stream_session: RuntimeSession


@contextmanager
def bootstrap_runtime(
    *,
    runtime_lock: ExclusiveRuntimeLock,
    verify_ledger: Callable[[], bool],
    initialize_token_health: Callable[[], Token],
    connect_stream: Callable[[Token], Session],
    reconcile: Callable[[Token, Session], bool],
) -> Iterator[BootstrappedRuntime]:
    """Initialize external resources only while the account lock is held.

    Successful startup stops after reconciliation. Arming remains an explicit,
    separate operator action.
    """
    runtime_lock.acquire()
    session: RuntimeSession | None = None
    try:
        if verify_ledger() is not True:
            raise RuntimeError("ledger verification did not pass")
        token_health = initialize_token_health()
        if token_health is None:
            raise RuntimeError("token health initialization returned no evidence")
        session = connect_stream(token_health)
        if session is None or not callable(getattr(session, "close", None)):
            raise RuntimeError("stream initialization returned no closeable session")
        if reconcile(token_health, session) is not True:
            raise RuntimeError("startup reconciliation did not pass")
        yield BootstrappedRuntime(token_health, session)
    finally:
        try:
            if session is not None:
                session.close()
        finally:
            runtime_lock.release()
