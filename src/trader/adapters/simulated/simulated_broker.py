from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256

from trader.domain.models import (
    BrokerExecutionState,
    BrokerOrder,
    OrderRequest,
    Side,
    TradingEnvironment,
    require_utc,
)
from trader.domain.cancellation import CancelOrderCommand
from trader.ports.broker import (
    BrokerCancelOutcome,
    BrokerCancelResult,
    BrokerSubmitOutcome,
    BrokerSubmitResult,
)


class SimulationReason(StrEnum):
    FILLED = "FILLED"
    NOT_MARKETABLE = "NOT_MARKETABLE"
    LATENCY = "LATENCY"
    STALE_QUOTE = "STALE_QUOTE"
    HALTED = "HALTED"
    UNKNOWN_SYMBOL = "UNKNOWN_SYMBOL"
    DUPLICATE_QUOTE = "DUPLICATE_QUOTE"
    OUT_OF_ORDER_QUOTE = "OUT_OF_ORDER_QUOTE"
    NO_ACTIVE_ORDER = "NO_ACTIVE_ORDER"
    CANCELED = "CANCELED"
    DAY_EXPIRED = "DAY_EXPIRED"
    ORDER_TERMINAL = "ORDER_TERMINAL"
    UNSUPPORTED_CORPORATE_ACTION = "UNSUPPORTED_CORPORATE_ACTION"


def _positive_decimal(value: Decimal, name: str, *, allow_zero: bool = False) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")
    if value < 0 or (not allow_zero and value == 0):
        raise ValueError(f"{name} must be {'non-negative' if allow_zero else 'positive'}")


def _sequence(value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValueError("sequence must be a non-negative integer")


@dataclass(frozen=True)
class QuoteEvent:
    symbol: str
    bid: Decimal
    ask: Decimal
    available_quantity: int
    occurred_at: datetime
    sequence: int
    halted: bool = False

    def __post_init__(self) -> None:
        if type(self.symbol) is not str or not self.symbol.strip():
            raise ValueError("symbol must be a non-empty string")
        _positive_decimal(self.bid, "bid")
        _positive_decimal(self.ask, "ask")
        if self.ask < self.bid:
            raise ValueError("ask cannot be below bid")
        if type(self.available_quantity) is not int or self.available_quantity < 0:
            raise ValueError("available_quantity must be a non-negative integer")
        require_utc(self.occurred_at, "occurred_at")
        _sequence(self.sequence)
        if type(self.halted) is not bool:
            raise ValueError("halted must be a bool")


@dataclass(frozen=True)
class Fill:
    broker_order_id: str
    quantity: Decimal
    price: Decimal
    fee: Decimal
    occurred_at: datetime
    quote_sequence: int


@dataclass(frozen=True)
class SimulationResult:
    reason: SimulationReason
    broker_order_id: str | None = None
    fills: tuple[Fill, ...] = ()
    affected_quantity: Decimal = Decimal(0)
    detail: str = ""


@dataclass
class _Order:
    request: OrderRequest
    broker_order_id: str
    filled: Decimal = Decimal(0)
    canceled: Decimal = Decimal(0)
    expired: Decimal = Decimal(0)
    last_event: tuple[datetime, int] | None = None

    @property
    def remaining(self) -> Decimal:
        return self.request.quantity - self.filled - self.canceled - self.expired


class SimulatedBroker:
    """Small deterministic LIMIT DAY quote matcher; it performs no I/O."""

    environment = TradingEnvironment.SIMULATED

    def __init__(
        self,
        *,
        clock: Callable[[], datetime],
        known_symbols: Iterable[str],
        latency: timedelta = timedelta(0),
        partial_fill_cap: int | None = None,
        slippage_bps: Decimal = Decimal(0),
        fee_bps: Decimal = Decimal(0),
        max_quote_age: timedelta = timedelta(seconds=5),
    ) -> None:
        if not callable(clock):
            raise ValueError("clock must be callable")
        if type(latency) is not timedelta or latency < timedelta(0):
            raise ValueError("latency must be a non-negative timedelta")
        if type(max_quote_age) is not timedelta or max_quote_age < timedelta(0):
            raise ValueError("max_quote_age must be a non-negative timedelta")
        if partial_fill_cap is not None and (
            type(partial_fill_cap) is not int or partial_fill_cap <= 0
        ):
            raise ValueError("partial_fill_cap must be a positive integer or None")
        _positive_decimal(slippage_bps, "slippage_bps", allow_zero=True)
        _positive_decimal(fee_bps, "fee_bps", allow_zero=True)
        symbols = frozenset(known_symbols)
        if any(type(symbol) is not str or not symbol.strip() for symbol in symbols):
            raise ValueError("known_symbols must contain non-empty strings")
        self._clock = clock
        self._known_symbols = symbols
        self._latency = latency
        self._partial_fill_cap = partial_fill_cap
        self._slippage_bps = slippage_bps
        self._fee_bps = fee_bps
        self._max_quote_age = max_quote_age
        self._orders: dict[str, _Order] = {}
        self._client_ids: set[str] = set()
        self._last_quotes: dict[str, tuple[datetime, int]] = {}
        self._corporate_action_halts: set[str] = set()
        self._last_clock: datetime | None = None

    def _now(self) -> datetime:
        now = self._clock()
        require_utc(now, "clock value")
        if self._last_clock is not None and now < self._last_clock:
            raise ValueError("clock moved backwards")
        self._last_clock = now
        return now

    def submit(self, request: OrderRequest) -> BrokerSubmitResult:
        now = self._now()
        if request.client_order_id in self._client_ids:
            return BrokerSubmitResult(BrokerSubmitOutcome.REJECTED, detail_code="DUPLICATE_CLIENT_ORDER_ID")
        if request.instrument.symbol in self._corporate_action_halts:
            return BrokerSubmitResult(
                BrokerSubmitOutcome.REJECTED,
                detail_code="CORPORATE_ACTION_UNRESOLVED",
            )
        if request.created_at > now:
            return BrokerSubmitResult(BrokerSubmitOutcome.REJECTED, detail_code="FUTURE_ORDER_TIMESTAMP")
        self._client_ids.add(request.client_order_id)
        digest = sha256(request.client_order_id.encode("utf-8")).hexdigest()[:20]
        broker_order_id = f"sim-{digest}"
        self._orders[broker_order_id] = _Order(request, broker_order_id)
        return BrokerSubmitResult(BrokerSubmitOutcome.ACKNOWLEDGED, broker_order_id)

    def on_quote(self, quote: QuoteEvent) -> SimulationResult:
        now = self._now()
        if quote.symbol not in self._known_symbols:
            return SimulationResult(SimulationReason.UNKNOWN_SYMBOL, detail=quote.symbol)
        if quote.symbol in self._corporate_action_halts:
            return SimulationResult(
                SimulationReason.UNSUPPORTED_CORPORATE_ACTION,
                detail="CORPORATE_ACTION_POLICY_REQUIRED",
            )
        prior = self._last_quotes.get(quote.symbol)
        key = (quote.occurred_at, quote.sequence)
        if prior == key:
            return SimulationResult(SimulationReason.DUPLICATE_QUOTE, detail=quote.symbol)
        if prior is not None and key <= prior:
            return SimulationResult(SimulationReason.OUT_OF_ORDER_QUOTE, detail=quote.symbol)
        self._last_quotes[quote.symbol] = key
        age = now - quote.occurred_at
        if age < timedelta(0) or age > self._max_quote_age:
            return SimulationResult(SimulationReason.STALE_QUOTE, detail=quote.symbol)
        if quote.halted:
            return SimulationResult(SimulationReason.HALTED, detail=quote.symbol)

        candidates = [
            order
            for order in self._orders.values()
            if order.request.instrument.symbol == quote.symbol and order.remaining > 0
        ]
        if not candidates:
            return SimulationResult(SimulationReason.NO_ACTIVE_ORDER, detail=quote.symbol)
        fills: list[Fill] = []
        last_reason = SimulationReason.NOT_MARKETABLE
        quote_left = Decimal(quote.available_quantity)
        for order in candidates:
            if quote_left == 0:
                break
            if order.last_event is not None and key <= order.last_event:
                continue
            if quote.occurred_at.date() > order.request.created_at.date():
                self._expire(order, key)
                last_reason = SimulationReason.DAY_EXPIRED
                continue
            if quote.occurred_at < order.request.created_at + self._latency:
                last_reason = SimulationReason.LATENCY
                continue
            request = order.request
            touch = quote.ask if request.side is Side.BUY else quote.bid
            marketable = touch <= request.limit_price if request.side is Side.BUY else touch >= request.limit_price
            if not marketable:
                continue
            cap = Decimal(self._partial_fill_cap) if self._partial_fill_cap is not None else order.remaining
            quantity = min(order.remaining, quote_left, cap)
            if quantity == 0:
                continue
            slip = touch * self._slippage_bps / Decimal(10_000)
            raw_price = touch + slip if request.side is Side.BUY else touch - slip
            price = min(raw_price, request.limit_price) if request.side is Side.BUY else max(raw_price, request.limit_price)
            fee = price * quantity * self._fee_bps / Decimal(10_000)
            fill = Fill(order.broker_order_id, quantity, price, fee, quote.occurred_at, quote.sequence)
            order.filled += quantity
            order.last_event = key
            quote_left -= quantity
            fills.append(fill)
        if fills:
            return SimulationResult(
                SimulationReason.FILLED,
                fills[0].broker_order_id if len(fills) == 1 else None,
                tuple(fills),
                sum((fill.quantity for fill in fills), Decimal(0)),
            )
        return SimulationResult(last_reason, detail=quote.symbol)

    def cancel(
        self,
        command: CancelOrderCommand | str,
        *,
        occurred_at: datetime | None = None,
        sequence: int | None = None,
    ) -> BrokerCancelResult | SimulationResult:
        if type(command) is CancelOrderCommand:
            return self._cancel_command(command)
        if occurred_at is None or sequence is None:
            raise TypeError("simulation event cancellation requires occurred_at and sequence")
        return self._cancel_event(command, occurred_at=occurred_at, sequence=sequence)

    def _cancel_command(self, command: CancelOrderCommand) -> BrokerCancelResult:
        target = command.target
        order = self._orders.get(target.broker_order_id)
        if (
            target.environment is not self.environment
            or order is None
            or target.account_id != order.request.account_id
            or target.business_date != order.request.created_at.date()
            or command.instrument != order.request.instrument
        ):
            return BrokerCancelResult(BrokerCancelOutcome.DEFINITE_REJECTED, "TARGET_MISMATCH")
        if order.remaining <= 0 or command.remaining_quantity != order.remaining:
            return BrokerCancelResult(BrokerCancelOutcome.DEFINITE_REJECTED, "REMAINING_MISMATCH")
        now = self._now()
        sequence = 0 if order.last_event is None or order.last_event[0] < now else order.last_event[1] + 1
        result = self._cancel_event(target.broker_order_id, occurred_at=now, sequence=sequence)
        if result.reason is SimulationReason.CANCELED:
            return BrokerCancelResult(BrokerCancelOutcome.ACK)
        return BrokerCancelResult(BrokerCancelOutcome.DEFINITE_REJECTED, result.detail or "ORDER_TERMINAL")

    def _cancel_event(
        self, broker_order_id: str, *, occurred_at: datetime, sequence: int
    ) -> SimulationResult:
        require_utc(occurred_at, "occurred_at")
        _sequence(sequence)
        now = self._now()
        if occurred_at > now:
            raise ValueError("cancel timestamp cannot be in the future")
        order = self._orders.get(broker_order_id)
        if order is None:
            return SimulationResult(SimulationReason.ORDER_TERMINAL, detail="UNKNOWN_ORDER")
        key = (occurred_at, sequence)
        if order.last_event is not None and key <= order.last_event:
            return SimulationResult(SimulationReason.ORDER_TERMINAL, broker_order_id, detail="OUT_OF_ORDER_EVENT")
        remaining = order.remaining
        if remaining == 0:
            return SimulationResult(SimulationReason.ORDER_TERMINAL, broker_order_id)
        order.canceled += remaining
        order.last_event = key
        return SimulationResult(SimulationReason.CANCELED, broker_order_id, affected_quantity=remaining)

    def expire_day(self, *, occurred_at: datetime, sequence: int) -> tuple[SimulationResult, ...]:
        require_utc(occurred_at, "occurred_at")
        _sequence(sequence)
        if occurred_at > self._now():
            raise ValueError("expiry timestamp cannot be in the future")
        key = (occurred_at, sequence)
        results = []
        for order in self._orders.values():
            if order.remaining > 0 and order.request.created_at.date() < occurred_at.date():
                quantity = order.remaining
                self._expire(order, key)
                results.append(SimulationResult(SimulationReason.DAY_EXPIRED, order.broker_order_id, affected_quantity=quantity))
        return tuple(results)

    def _expire(self, order: _Order, key: tuple[datetime, int]) -> None:
        if order.last_event is not None and key <= order.last_event:
            return
        order.expired += order.remaining
        order.last_event = key

    def on_corporate_action(
        self, symbol: str, action: str, *, occurred_at: datetime, sequence: int
    ) -> SimulationResult:
        if (
            type(symbol) is not str
            or not symbol.strip()
            or type(action) is not str
            or not action.strip()
        ):
            raise ValueError("symbol and action must be non-empty")
        require_utc(occurred_at, "occurred_at")
        _sequence(sequence)
        if occurred_at > self._now():
            raise ValueError("corporate action timestamp cannot be in the future")
        if symbol not in self._known_symbols:
            return SimulationResult(SimulationReason.UNKNOWN_SYMBOL, detail=symbol)
        active = any(
            order.request.instrument.symbol == symbol and order.remaining > 0
            for order in self._orders.values()
        )
        self._corporate_action_halts.add(symbol)
        detail = f"{action.upper()}_{'ACTIVE_ORDER_HALT' if active else 'UNSUPPORTED'}"
        return SimulationResult(SimulationReason.UNSUPPORTED_CORPORATE_ACTION, detail=detail)

    def order(self, broker_order_id: str) -> BrokerOrder:
        order = self._orders[broker_order_id]
        remaining = order.remaining
        if remaining > 0:
            state = BrokerExecutionState.PARTIALLY_FILLED if order.filled else BrokerExecutionState.OPEN
        elif order.canceled:
            state = BrokerExecutionState.CANCELED
        elif order.expired:
            state = BrokerExecutionState.EXPIRED
        else:
            state = BrokerExecutionState.FILLED
        return BrokerOrder(
            broker_order_id,
            order.request.account_id,
            order.request.quantity,
            order.filled,
            remaining,
            order.canceled,
            Decimal(0),
            order.expired,
            state,
        )
