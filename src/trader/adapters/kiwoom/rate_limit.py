from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import StrEnum
from hashlib import sha256
from math import isfinite
from threading import Lock

from trader.ports.account import AccountEnvironment
from trader.ports.clock import MonotonicClock


_KST = timezone(timedelta(hours=9))


class RateLimitKind(StrEnum):
    QUERY = "QUERY"
    ORDER = "ORDER"
    FX = "FX"
    CHART = "CHART"
    SPECIAL = "SPECIAL"


class OrderPriority(StrEnum):
    CANCEL = "CANCEL"
    REDUCE_ONLY = "REDUCE_ONLY"
    NEW_ORDER = "NEW_ORDER"


class QueryPriority(StrEnum):
    STANDARD = "STANDARD"
    RESEARCH = "RESEARCH"
    BULK = "BULK"
    RECONCILIATION = "RECONCILIATION"
    UNKNOWN_RECONCILIATION = "UNKNOWN_RECONCILIATION"
    ACCOUNT_RECONCILIATION = "ACCOUNT_RECONCILIATION"


class RateLimitReason(StrEnum):
    GLOBAL = "GLOBAL"
    ACCOUNT_QUERY = "ACCOUNT_QUERY"
    RECONCILIATION_RESERVED = "RECONCILIATION_RESERVED"
    ACCOUNT_ORDER = "ACCOUNT_ORDER"
    CANCEL_RESERVED = "CANCEL_RESERVED"
    FX = "FX"
    CHART = "CHART"
    SPECIAL = "SPECIAL"
    MOCK_SAME_TR = "MOCK_SAME_TR"
    CLOCK_FAILURE = "CLOCK_FAILURE"
    INVALID_REQUEST = "INVALID_REQUEST"


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    reason: RateLimitReason | None = None
    retry_after: float | None = None


@dataclass(frozen=True)
class KiwoomRateLimitPolicy:
    snapshot_version: str
    effective_date: date
    window_seconds: float
    peak_start_kst: time
    peak_end_kst: time
    us_account_queries: int
    us_peak_account_queries: int
    us_global_queries: int
    mock_same_tr_queries: int
    reconciliation_reserve: int
    us_account_orders: int = 10
    us_peak_account_orders: int = 3
    cancel_reserve: int = 1
    us_account_fx: int = 1
    us_chart_queries: int = 20
    special_queries: int = 5
    special_window_seconds: float = 60.0

    def __post_init__(self) -> None:
        if not self.snapshot_version:
            raise ValueError("snapshot_version is required")
        if type(self.effective_date) is not date:
            raise ValueError("effective_date must be a date")
        windows = (self.window_seconds, self.special_window_seconds)
        if any(
            type(value) not in (int, float) or not isfinite(value) or value <= 0
            for value in windows
        ):
            raise ValueError("windows must be positive and finite")
        if (
            type(self.peak_start_kst) is not time
            or type(self.peak_end_kst) is not time
            or self.peak_start_kst.tzinfo is not None
            or self.peak_end_kst.tzinfo is not None
            or self.peak_start_kst >= self.peak_end_kst
        ):
            raise ValueError("peak KST window must be ordered naive times")
        limits = (
            self.us_account_queries,
            self.us_peak_account_queries,
            self.us_global_queries,
            self.mock_same_tr_queries,
            self.us_account_orders,
            self.us_peak_account_orders,
            self.us_account_fx,
            self.us_chart_queries,
            self.special_queries,
        )
        if any(type(value) is not int or value <= 0 for value in limits):
            raise ValueError("rate limits must be positive integers")
        if (
            type(self.reconciliation_reserve) is not int
            or self.reconciliation_reserve < 1
            or self.reconciliation_reserve
            >= min(self.us_account_queries, self.us_peak_account_queries)
        ):
            raise ValueError("reconciliation_reserve must leave standard capacity")
        if (
            type(self.cancel_reserve) is not int
            or self.cancel_reserve < 1
            or self.cancel_reserve
            >= min(self.us_account_orders, self.us_peak_account_orders)
        ):
            raise ValueError("cancel_reserve must leave non-cancel capacity")


def current_official_policy() -> KiwoomRateLimitPolicy:
    """Return the caller-visible official-policy snapshot used by this build."""
    return KiwoomRateLimitPolicy(
        snapshot_version="kiwoom-official-2026-08-27",
        effective_date=date(2026, 8, 27),
        window_seconds=1.0,
        peak_start_kst=time(9),
        peak_end_kst=time(10),
        us_account_queries=5,
        us_peak_account_queries=3,
        us_global_queries=50,
        mock_same_tr_queries=1,
        reconciliation_reserve=1,
        us_account_orders=10,
        us_peak_account_orders=3,
        cancel_reserve=1,
        us_account_fx=1,
        us_chart_queries=20,
        special_queries=5,
        special_window_seconds=60.0,
    )


kiwoom_official_policy_2026_08_27 = current_official_policy


class KiwoomReadonlyRateLimiter:
    def __init__(
        self,
        policy: KiwoomRateLimitPolicy,
        monotonic_clock: MonotonicClock,
        wall_clock: Callable[[], datetime],
    ) -> None:
        if type(policy) is not KiwoomRateLimitPolicy:
            raise ValueError("policy must be KiwoomRateLimitPolicy")
        self._policy = policy
        self._monotonic_clock = monotonic_clock
        self._wall_clock = wall_clock
        self._lock = Lock()
        self._events: dict[tuple[str, ...], deque[float]] = defaultdict(deque)
        self._last_monotonic: float | None = None

    @property
    def policy(self) -> KiwoomRateLimitPolicy:
        return self._policy

    def acquire(
        self,
        *,
        environment: AccountEnvironment,
        token: str,
        api_id: str,
        priority: QueryPriority = QueryPriority.STANDARD,
        kind: RateLimitKind = RateLimitKind.QUERY,
        order_priority: OrderPriority | None = None,
    ) -> RateLimitDecision:
        if (
            type(environment) is not AccountEnvironment
            or not isinstance(token, str)
            or not token
            or not isinstance(api_id, str)
            or not api_id
            or type(priority) is not QueryPriority
            or type(kind) is not RateLimitKind
            or (kind is RateLimitKind.ORDER) != (type(order_priority) is OrderPriority)
            or (api_id == "usa10099") != (kind is RateLimitKind.SPECIAL)
        ):
            return RateLimitDecision(False, RateLimitReason.INVALID_REQUEST)
        try:
            token_key = sha256(token.encode()).hexdigest()
        except UnicodeEncodeError:
            return RateLimitDecision(False, RateLimitReason.INVALID_REQUEST)

        with self._lock:
            clock = self._read_clocks()
            if clock is None:
                return RateLimitDecision(False, RateLimitReason.CLOCK_FAILURE)
            now, in_peak = clock
            buckets = self._buckets(
                token_key, api_id, environment, priority, kind, order_priority, in_peak
            )

            blocked: list[tuple[float, RateLimitReason]] = []
            for key, capacity, window, reason in buckets:
                events = self._events[key]
                boundary = now - window
                while events and events[0] <= boundary:
                    events.popleft()
                if len(events) >= capacity:
                    blocked.append((events[0] + window - now, reason))
            if blocked:
                retry_after, reason = max(blocked, key=lambda item: item[0])
                return RateLimitDecision(False, reason, max(0.0, retry_after))
            for key, _, _, _ in buckets:
                self._events[key].append(now)
            return RateLimitDecision(True)

    def _read_clocks(self) -> tuple[float, bool] | None:
        try:
            now = self._monotonic_clock()
            wall_now = self._wall_clock()
            if (
                type(now) not in (int, float)
                or not isfinite(now)
                or now < 0
                or (self._last_monotonic is not None and now < self._last_monotonic)
                or not isinstance(wall_now, datetime)
                or wall_now.tzinfo is None
                or wall_now.utcoffset() is None
            ):
                return None
            now = float(now)
            kst_time = wall_now.astimezone(_KST).timetz().replace(tzinfo=None)
        except Exception:
            return None
        self._last_monotonic = now
        return now, self._policy.peak_start_kst <= kst_time < self._policy.peak_end_kst

    def _buckets(
        self,
        token_key: str,
        api_id: str,
        environment: AccountEnvironment,
        priority: QueryPriority,
        kind: RateLimitKind,
        order_priority: OrderPriority | None,
        in_peak: bool,
    ) -> list[tuple[tuple[str, ...], int, float, RateLimitReason]]:
        second = self._policy.window_seconds
        buckets = [(("global",), self._policy.us_global_queries, second, RateLimitReason.GLOBAL)]
        if kind in (RateLimitKind.QUERY, RateLimitKind.SPECIAL):
            limit = (
                self._policy.us_peak_account_queries
                if in_peak
                else self._policy.us_account_queries
            )
            buckets.append((("query", token_key), limit, second, RateLimitReason.ACCOUNT_QUERY))
            if priority in (QueryPriority.STANDARD, QueryPriority.RESEARCH, QueryPriority.BULK):
                buckets.append(
                    (
                        ("query-standard", token_key),
                        limit - self._policy.reconciliation_reserve,
                        second,
                        RateLimitReason.RECONCILIATION_RESERVED,
                    )
                )
            if kind is RateLimitKind.SPECIAL:
                buckets.append(
                    (
                        ("special", token_key, api_id),
                        self._policy.special_queries,
                        self._policy.special_window_seconds,
                        RateLimitReason.SPECIAL,
                    )
                )
        elif kind is RateLimitKind.ORDER:
            limit = (
                self._policy.us_peak_account_orders
                if in_peak
                else self._policy.us_account_orders
            )
            buckets.append((("order", token_key), limit, second, RateLimitReason.ACCOUNT_ORDER))
            if order_priority is not OrderPriority.CANCEL:
                buckets.append(
                    (
                        ("order-noncancel", token_key),
                        limit - self._policy.cancel_reserve,
                        second,
                        RateLimitReason.CANCEL_RESERVED,
                    )
                )
        elif kind is RateLimitKind.FX:
            buckets.append(
                (("fx", token_key), self._policy.us_account_fx, second, RateLimitReason.FX)
            )
        else:
            buckets.append((("chart",), self._policy.us_chart_queries, second, RateLimitReason.CHART))
        if environment is AccountEnvironment.MOCK:
            buckets.append(
                (
                    ("mock-tr", token_key, api_id),
                    self._policy.mock_same_tr_queries,
                    second,
                    RateLimitReason.MOCK_SAME_TR,
                )
            )
        return buckets


ReadonlyRateLimiter = KiwoomReadonlyRateLimiter
