"""Offline-testable Kiwoom WebSocket session supervisor."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from math import isfinite
from threading import Lock
from typing import Any, Protocol

from trader.domain.models import TradingEnvironment, require_id


class StreamState(Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    READY = "READY"
    RECONNECTING = "RECONNECTING"
    CLOSED = "CLOSED"


class StreamEventKind(Enum):
    DATA = "DATA"
    HEARTBEAT = "HEARTBEAT"


@dataclass(frozen=True)
class StreamEvent:
    event_id: str
    sequence: int
    environment: TradingEnvironment
    kind: StreamEventKind
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        require_id(self.event_id, "event_id")
        if type(self.sequence) is not int or self.sequence < 0:
            raise ValueError("sequence must be a non-negative integer")
        if type(self.environment) is not TradingEnvironment:
            raise ValueError("environment must be TradingEnvironment")
        if type(self.kind) is not StreamEventKind:
            raise ValueError("kind must be StreamEventKind")
        if not isinstance(self.payload, Mapping):
            raise ValueError("payload must be a mapping")


@dataclass(frozen=True)
class StreamEvidence:
    index: int
    code: str
    state: StreamState
    monotonic_time: float | None
    details: tuple[tuple[str, str], ...] = ()


class WebSocketTransport(Protocol):
    def connect(
        self, account_id: str, token: str, environment: TradingEnvironment
    ) -> None: ...

    def subscribe(self, symbols: tuple[str, ...]) -> None: ...

    def close(self) -> None: ...


class SessionOwnership:
    """Process-local ownership registry, injectable to keep tests isolated."""

    def __init__(self) -> None:
        self._owners: dict[tuple[str, str], object] = {}
        self._lock = Lock()

    def acquire(self, key: tuple[str, str], owner: object) -> None:
        with self._lock:
            current = self._owners.get(key)
            if current is not None and current is not owner:
                raise RuntimeError("account and token already have an active stream session")
            self._owners[key] = owner

    def release(self, key: tuple[str, str], owner: object) -> None:
        with self._lock:
            if self._owners.get(key) is owner:
                del self._owners[key]


_PROCESS_OWNERSHIP = SessionOwnership()


class KiwoomWebSocketSupervisor:
    MAX_SYMBOLS = 200

    def __init__(
        self,
        *,
        account_id: str,
        token: str,
        environment: TradingEnvironment,
        transport: WebSocketTransport,
        monotonic_clock: Callable[[], float],
        blocker: Callable[[str], None],
        reconcile: Callable[[str, TradingEnvironment], bool],
        heartbeat_timeout_seconds: float,
        ownership: SessionOwnership = _PROCESS_OWNERSHIP,
    ) -> None:
        require_id(account_id, "account_id")
        require_id(token, "token")
        if type(environment) is not TradingEnvironment:
            raise ValueError("environment must be TradingEnvironment")
        if not callable(monotonic_clock) or not callable(blocker) or not callable(reconcile):
            raise ValueError("clock, blocker, and reconcile must be callable")
        if (
            type(heartbeat_timeout_seconds) not in (int, float)
            or not isfinite(heartbeat_timeout_seconds)
            or heartbeat_timeout_seconds <= 0
        ):
            raise ValueError("heartbeat timeout must be finite and positive")
        self.account_id = account_id
        self.environment = environment
        self.state = StreamState.DISCONNECTED
        self._token = token
        self._transport = transport
        self._clock = monotonic_clock
        self._blocker = blocker
        self._reconcile = reconcile
        self._heartbeat_timeout = float(heartbeat_timeout_seconds)
        self._ownership = ownership
        self._ownership_key = (account_id, sha256(token.encode()).hexdigest())
        self._owned = False
        self._subscriptions: set[str] = set()
        self._consumers: list[Callable[[StreamEvent], None]] = []
        self._seen_ids: set[str] = set()
        self._last_sequence: int | None = None
        self._last_activity: float | None = None
        self._last_clock: float | None = None
        self._evidence: list[StreamEvidence] = []

    @property
    def subscriptions(self) -> tuple[str, ...]:
        return tuple(sorted(self._subscriptions))

    @property
    def evidence(self) -> tuple[StreamEvidence, ...]:
        return tuple(self._evidence)

    def add_consumer(self, consumer: Callable[[StreamEvent], None]) -> None:
        if not callable(consumer):
            raise ValueError("consumer must be callable")
        if consumer not in self._consumers:
            self._consumers.append(consumer)

    def subscribe(self, *symbols: str) -> None:
        requested = set(symbols)
        for symbol in requested:
            require_id(symbol, "symbol")
        additions = requested - self._subscriptions
        if len(self._subscriptions) + len(additions) > self.MAX_SYMBOLS:
            raise ValueError("a stream session supports at most 200 symbols")
        self._subscriptions.update(additions)
        if additions and self.state is StreamState.READY:
            try:
                self._transport.subscribe(tuple(sorted(additions)))
            except Exception:
                self._fail_closed("SUBSCRIPTION_FAILURE")
                raise
        if additions:
            self._record("SUBSCRIPTIONS_ADDED", count=len(additions))

    def connect(self) -> None:
        if self.state is not StreamState.DISCONNECTED:
            raise RuntimeError("connect requires DISCONNECTED state")
        self._ownership.acquire(self._ownership_key, self)
        self._owned = True
        self._transition(StreamState.CONNECTING, "CONNECT_STARTED")
        try:
            self._transport.connect(self.account_id, self._token, self.environment)
            self._restore_subscriptions()
            now = self._now()
        except Exception:
            if self.state is not StreamState.RECONNECTING:
                self._fail_closed("STREAM_CONNECT_FAILURE")
            raise
        self._last_activity = now
        self._transition(StreamState.READY, "CONNECT_READY")

    def accept(self, event: StreamEvent) -> None:
        if type(event) is not StreamEvent:
            self._fail_closed("STREAM_EVENT_SCHEMA")
            return
        if self.state is StreamState.RECONNECTING:
            self._record("BROKER_EVENT_DISTRUSTED", event_id=event.event_id)
            return
        if self.state is not StreamState.READY:
            self._record("BROKER_EVENT_IGNORED", event_id=event.event_id)
            return
        try:
            now = self._now()
        except RuntimeError:
            return
        if event.environment is not self.environment:
            self._fail_closed("STREAM_PROVENANCE_MISMATCH")
            return
        if event.event_id in self._seen_ids:
            self._record("DUPLICATE_EVENT", event_id=event.event_id)
            return
        if self._last_sequence is not None:
            if event.sequence == self._last_sequence:
                self._record("DUPLICATE_SEQUENCE", sequence=event.sequence)
                return
            if event.sequence < self._last_sequence:
                self._record("REORDERED_EVENT", sequence=event.sequence)
                return
            if event.sequence > self._last_sequence + 1:
                self._fail_closed(
                    "STREAM_GAP",
                    expected=self._last_sequence + 1,
                    received=event.sequence,
                )
                return
        self._seen_ids.add(event.event_id)
        self._last_sequence = event.sequence
        self._last_activity = now
        self._record("HEARTBEAT" if event.kind is StreamEventKind.HEARTBEAT else "EVENT_ACCEPTED")
        consumer_failed = False
        for index, consumer in enumerate(tuple(self._consumers)):
            try:
                consumer(event)
            except Exception as error:
                consumer_failed = True
                self._record(
                    "CONSUMER_ERROR",
                    consumer=index,
                    error_type=type(error).__name__,
                )
        if consumer_failed:
            self._fail_closed("CONSUMER_FAILURE", event_id=event.event_id)

    def check_heartbeat(self) -> None:
        if self.state is not StreamState.READY:
            return
        try:
            now = self._now()
        except RuntimeError:
            return
        if self._last_activity is None or now - self._last_activity >= self._heartbeat_timeout:
            self._fail_closed("HEARTBEAT_TIMEOUT")

    def disconnected(self, reason: str = "STREAM_DISCONNECTED") -> None:
        require_id(reason, "reason")
        if self.state in {StreamState.CLOSED, StreamState.RECONNECTING}:
            return
        self._fail_closed(reason)

    def reconnect(self) -> bool:
        if self.state is not StreamState.RECONNECTING:
            raise RuntimeError("reconnect requires RECONNECTING state")
        try:
            self._transport.connect(self.account_id, self._token, self.environment)
            self._restore_subscriptions()
            reconciled = self._reconcile(self.account_id, self.environment)
            if type(reconciled) is not bool or not reconciled:
                self._record("RECONCILIATION_FAILED")
                return False
            now = self._now()
        except Exception as error:
            self._record("RECONNECT_FAILED", error_type=type(error).__name__)
            return False
        self._seen_ids.clear()
        self._last_sequence = None
        self._last_activity = now
        self._transition(StreamState.READY, "RECONCILIATION_READY")
        return True

    def close(self) -> None:
        if self.state is StreamState.CLOSED:
            return
        try:
            self._transport.close()
        finally:
            if self._owned:
                self._ownership.release(self._ownership_key, self)
                self._owned = False
            self._transition(StreamState.CLOSED, "STREAM_CLOSED")

    def _restore_subscriptions(self) -> None:
        if self._subscriptions:
            self._transport.subscribe(tuple(sorted(self._subscriptions)))
        self._record("SUBSCRIPTIONS_RESTORED", count=len(self._subscriptions))

    def _fail_closed(self, blocker: str, **details: object) -> None:
        try:
            self._blocker(blocker)
        finally:
            if self.state not in {StreamState.CLOSED, StreamState.RECONNECTING}:
                self._transition(StreamState.RECONNECTING, blocker, **details)

    def _now(self) -> float:
        try:
            value = self._clock()
            if type(value) not in (int, float) or not isfinite(value) or value < 0:
                raise ValueError("invalid monotonic clock")
            now = float(value)
            if self._last_clock is not None and now < self._last_clock:
                raise ValueError("monotonic clock moved backwards")
            self._last_clock = now
            return now
        except Exception as error:
            self._fail_closed("CLOCK_FAILURE")
            raise RuntimeError("monotonic clock failure") from error

    def _transition(self, target: StreamState, code: str, **details: object) -> None:
        previous = self.state
        self.state = target
        self._record(code, previous=previous.value, target=target.value, **details)

    def _record(self, code: str, **details: object) -> None:
        self._evidence.append(
            StreamEvidence(
                len(self._evidence),
                code,
                self.state,
                self._last_clock,
                tuple(sorted((key, str(value)) for key, value in details.items())),
            )
        )
