from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
from math import isfinite
import re


def require_id(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def require_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{name} must be timezone-aware UTC")


def require_decimal(value: Decimal, name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")


def require_enum(value: object, enum_type: type[StrEnum], name: str) -> None:
    if not isinstance(value, enum_type):
        raise ValueError(f"{name} must be a {enum_type.__name__}")


SIGNED_64_MAX = (1 << 63) - 1


def require_minor_integer(value: int, name: str) -> None:
    if type(value) is not int or not 0 <= value <= SIGNED_64_MAX:
        raise ValueError(f"{name} must be a non-negative signed 64-bit integer")


class RiskStage(StrEnum):
    ELIGIBILITY = "ELIGIBILITY"
    PRE_TRADE = "PRE_TRADE"
    CONTINUOUS = "CONTINUOUS"


class RiskOutcome(StrEnum):
    APPROVED = "APPROVED"
    ADJUSTED = "ADJUSTED"
    REJECTED = "REJECTED"


class SubmissionState(StrEnum):
    PREPARED = "PREPARED"
    SUBMISSION_STARTED = "SUBMISSION_STARTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    SUBMISSION_REJECTED = "SUBMISSION_REJECTED"
    SUBMITTED_UNKNOWN = "SUBMITTED_UNKNOWN"


class BrokerExecutionState(StrEnum):
    NOT_OBSERVED = "NOT_OBSERVED"
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class PendingAction(StrEnum):
    NONE = "NONE"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    REPLACE_REQUESTED = "REPLACE_REQUESTED"


class SnapshotQuality(StrEnum):
    CONSISTENT = "CONSISTENT"
    STAGGERED = "STAGGERED"
    INCOMPLETE = "INCOMPLETE"
    STALE = "STALE"


class PermitScope(StrEnum):
    NEW_ORDER = "NEW_ORDER"
    CANCEL = "CANCEL"
    REDUCE_ONLY = "REDUCE_ONLY"
    EMERGENCY_FLATTEN = "EMERGENCY_FLATTEN"


class TradingEnvironment(StrEnum):
    SIMULATED = "SIMULATED"
    PAPER = "PAPER"
    LIVE = "LIVE"


class SafetyState(StrEnum):
    BOOTSTRAPPING = "BOOTSTRAPPING"
    RECONCILING = "RECONCILING"
    READY = "READY"
    TRADING = "TRADING"
    HALTED = "HALTED"


class OperatorAction(StrEnum):
    ACKNOWLEDGE_STARTUP_RECOVERY = "ACKNOWLEDGE_STARTUP_RECOVERY"
    BEGIN_RECONCILIATION = "BEGIN_RECONCILIATION"
    ARM = "ARM"
    HALT = "HALT"
    ISSUE_CANCEL = "ISSUE_CANCEL"
    ISSUE_REDUCE_ONLY = "ISSUE_REDUCE_ONLY"
    ISSUE_EMERGENCY_FLATTEN = "ISSUE_EMERGENCY_FLATTEN"
    RESOLVE_SUBMITTED_UNKNOWN = "RESOLVE_SUBMITTED_UNKNOWN"


class OperatorCommandOutcome(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class UnknownResolutionResult(StrEnum):
    BROKER_ORDER_LINKED = "BROKER_ORDER_LINKED"
    CONFIRMED_ABSENT = "CONFIRMED_ABSENT"
    MANUAL_ACTIVITY_LINKED = "MANUAL_ACTIVITY_LINKED"


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class TargetUnit(StrEnum):
    SHARES = "SHARES"


class OrderType(StrEnum):
    LIMIT = "LIMIT"


class TimeInForce(StrEnum):
    DAY = "DAY"


@dataclass(frozen=True)
class InstrumentId:
    market: str
    symbol: str
    currency: str

    def __post_init__(self) -> None:
        for name in ("market", "symbol", "currency"):
            require_id(getattr(self, name), name)


@dataclass(frozen=True)
class ReservationPosition:
    instrument: InstrumentId
    quantity: int

    def __post_init__(self) -> None:
        if type(self.instrument) is not InstrumentId:
            raise ValueError("instrument must be exact InstrumentId")
        require_minor_integer(self.quantity, "quantity")


@dataclass(frozen=True)
class ReservationAccountState:
    account_id: str
    account_snapshot_id: str
    account_currency: str
    available_cash_minor: int
    current_exposure_minor: int
    positions: tuple[ReservationPosition, ...]

    def __post_init__(self) -> None:
        for name in ("account_id", "account_snapshot_id", "account_currency"):
            require_id(getattr(self, name), name)
        for name in ("available_cash_minor", "current_exposure_minor"):
            require_minor_integer(getattr(self, name), name)
        if type(self.positions) is not tuple or any(
            type(position) is not ReservationPosition for position in self.positions
        ):
            raise ValueError("positions must be an exact tuple of ReservationPosition")
        for position in self.positions:
            ReservationPosition.__post_init__(position)
        instruments = tuple(position.instrument for position in self.positions)
        if len(set(instruments)) != len(instruments):
            raise ValueError("reservation positions must have unique instruments")
        if self.account_currency != "USD" or any(
            position.instrument.currency != self.account_currency
            for position in self.positions
        ):
            raise ValueError("Phase 1B reservation account state supports USD only")


@dataclass(frozen=True)
class RiskReservationPolicy:
    policy_version: str
    cash_cap_minor: int
    exposure_cap_minor: int
    fee_buffer_minor: int

    def __post_init__(self) -> None:
        require_id(self.policy_version, "policy_version")
        for name in ("cash_cap_minor", "exposure_cap_minor", "fee_buffer_minor"):
            require_minor_integer(getattr(self, name), name)


@dataclass(frozen=True)
class ReservationTerms:
    """Immutable, same-currency capacity facts for one order submission."""

    account_id: str
    account_snapshot_id: str
    environment: TradingEnvironment
    policy_version: str
    instrument: InstrumentId
    side: Side
    quantity: int
    account_currency: str
    instrument_currency: str
    available_cash_minor: int
    current_exposure_minor: int
    current_position_quantity: int
    cash_cap_minor: int
    exposure_cap_minor: int
    fee_buffer_minor: int
    reserved_cash_minor: int
    reserved_exposure_minor: int
    reserved_sell_quantity: int

    def __post_init__(self) -> None:
        for name in (
            "account_id", "account_snapshot_id", "policy_version",
            "account_currency", "instrument_currency",
        ):
            require_id(getattr(self, name), name)
        require_enum(self.environment, TradingEnvironment, "environment")
        require_enum(self.side, Side, "side")
        if type(self.instrument) is not InstrumentId:
            raise ValueError("instrument must be exact InstrumentId")
        for name in (
            "quantity", "available_cash_minor", "current_exposure_minor",
            "current_position_quantity", "cash_cap_minor", "exposure_cap_minor",
            "fee_buffer_minor", "reserved_cash_minor", "reserved_exposure_minor",
            "reserved_sell_quantity",
        ):
            require_minor_integer(getattr(self, name), name)
        if self.quantity == 0:
            raise ValueError("reservation quantity must be positive")
        if not (
            self.account_currency == self.instrument_currency
            == self.instrument.currency == "USD"
        ):
            raise ValueError("Phase 1B reservations support same-currency USD only")
        if self.side is Side.BUY:
            if (
                self.reserved_exposure_minor == 0
                or self.reserved_cash_minor
                != self.reserved_exposure_minor + self.fee_buffer_minor
                or self.reserved_sell_quantity != 0
            ):
                raise ValueError("BUY reservation amounts are inconsistent")
        elif (
            self.reserved_cash_minor != 0
            or self.reserved_exposure_minor != 0
            or self.reserved_sell_quantity != self.quantity
            or self.fee_buffer_minor != 0
        ):
            raise ValueError("SELL reservation amounts are inconsistent")


@dataclass(frozen=True)
class StrategyDecision:
    decision_id: str
    strategy_version: str
    input_snapshot_id: str
    signal: str
    rationale: str
    decided_at: datetime

    def __post_init__(self) -> None:
        for name in ("decision_id", "strategy_version", "input_snapshot_id", "signal"):
            require_id(getattr(self, name), name)
        require_utc(self.decided_at, "decided_at")


@dataclass(frozen=True)
class PositionTarget:
    target_id: str
    strategy_id: str
    instrument: InstrumentId
    quantity: Decimal
    unit: TargetUnit
    target_at: datetime

    def __post_init__(self) -> None:
        require_id(self.target_id, "target_id")
        require_id(self.strategy_id, "strategy_id")
        require_decimal(self.quantity, "quantity")
        if self.quantity < 0:
            raise ValueError("long-only PositionTarget cannot be negative")
        require_enum(self.unit, TargetUnit, "unit")
        if self.unit is not TargetUnit.SHARES:
            raise ValueError("Phase 1A PositionTarget supports SHARES only")
        require_utc(self.target_at, "target_at")


@dataclass(frozen=True)
class TradeIntent:
    intent_id: str
    target_id: str
    strategy_id: str
    account_id: str
    account_snapshot_id: str
    instrument: InstrumentId
    target_quantity: Decimal
    current_quantity: Decimal
    open_quantity: Decimal
    original_quantity: Decimal
    created_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "intent_id", "target_id", "strategy_id", "account_id", "account_snapshot_id"
        ):
            require_id(getattr(self, name), name)
        for name in (
            "target_quantity", "current_quantity", "open_quantity", "original_quantity"
        ):
            require_decimal(getattr(self, name), name)
        if self.target_quantity < 0 or self.current_quantity < 0:
            raise ValueError("long-only target/current quantities cannot be negative")
        if self.original_quantity == 0:
            raise ValueError("original_quantity must be non-zero")
        if self.original_quantity != (
            self.target_quantity - self.current_quantity - self.open_quantity
        ):
            raise ValueError("original_quantity must equal target - current - open")
        require_utc(self.created_at, "created_at")


@dataclass(frozen=True)
class RiskDecision:
    decision_id: str
    risk_stage: RiskStage
    policy_version: str
    input_snapshot_id: str
    trade_intent_id: str | None
    original_quantity: Decimal | None
    approved_quantity: Decimal | None
    outcome: RiskOutcome
    reason_codes: tuple[str, ...]
    evaluated_at: datetime

    def __post_init__(self) -> None:
        for name in ("decision_id", "policy_version", "input_snapshot_id"):
            require_id(getattr(self, name), name)
        require_enum(self.risk_stage, RiskStage, "risk_stage")
        require_enum(self.outcome, RiskOutcome, "outcome")
        if type(self.reason_codes) is not tuple or any(
            not isinstance(code, str)
            or re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", code) is None
            for code in self.reason_codes
        ):
            raise ValueError("risk reason codes must be machine-readable uppercase codes")
        if len(set(self.reason_codes)) != len(self.reason_codes):
            raise ValueError("risk reason codes cannot contain duplicates")
        if self.outcome in {RiskOutcome.ADJUSTED, RiskOutcome.REJECTED} and not self.reason_codes:
            raise ValueError("adjusted/rejected risk requires a reason code")
        for name in ("original_quantity", "approved_quantity"):
            value = getattr(self, name)
            if value is not None:
                require_decimal(value, name)
        if self.risk_stage is not RiskStage.PRE_TRADE:
            if self.trade_intent_id is not None:
                raise ValueError("only pre-trade risk can reference a trade intent")
            if self.original_quantity is not None or self.approved_quantity is not None:
                raise ValueError("eligibility/continuous risk quantities must be None")
            if self.outcome is RiskOutcome.ADJUSTED:
                raise ValueError("only pre-trade risk can adjust quantity")
        else:
            if self.trade_intent_id is None:
                raise ValueError("pre-trade risk requires trade_intent_id")
            require_id(self.trade_intent_id, "trade_intent_id")
            if self.original_quantity is None or self.original_quantity == 0:
                raise ValueError("pre-trade risk requires nonzero original quantity")
            approved = self.approved_quantity
            if self.outcome is RiskOutcome.REJECTED:
                if approved not in (None, Decimal(0)):
                    raise ValueError("rejected decision cannot approve quantity")
            elif approved is None or approved == 0:
                raise ValueError("approved/adjusted decision requires nonzero quantity")
            elif (approved > 0) != (self.original_quantity > 0):
                raise ValueError("approved quantity cannot change side")
            elif self.outcome is RiskOutcome.APPROVED and approved != self.original_quantity:
                raise ValueError("approved decision must preserve quantity")
            elif self.outcome is RiskOutcome.ADJUSTED and abs(approved) >= abs(self.original_quantity):
                raise ValueError("adjusted quantity must reduce absolute quantity")
        require_utc(self.evaluated_at, "evaluated_at")


@dataclass(frozen=True)
class ExecutionPlan:
    plan_id: str
    intent_id: str
    risk_decision_id: str
    side: Side
    order_type: OrderType
    time_in_force: TimeInForce
    quantity: Decimal
    limit_price: Decimal
    market_evidence: MarketEvidence
    pricing_policy_version: str
    created_at: datetime
    expires_at: datetime
    minimum_limit_price: Decimal
    maximum_limit_price: Decimal
    clock_session_id: str
    created_monotonic: float
    expires_monotonic: float

    def __post_init__(self) -> None:
        for name in (
            "plan_id", "intent_id", "risk_decision_id", "pricing_policy_version",
            "clock_session_id",
        ):
            require_id(getattr(self, name), name)
        require_enum(self.side, Side, "side")
        require_enum(self.order_type, OrderType, "order_type")
        require_enum(self.time_in_force, TimeInForce, "time_in_force")
        for name in (
            "quantity", "limit_price", "minimum_limit_price", "maximum_limit_price",
        ):
            require_decimal(getattr(self, name), name)
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if type(self.market_evidence) is not MarketEvidence:
            raise ValueError("market_evidence must be exact MarketEvidence")
        if (
            self.pricing_policy_version != self.market_evidence.pricing_policy_version
            or self.minimum_limit_price != self.market_evidence.minimum_limit_price
            or self.maximum_limit_price != self.market_evidence.maximum_limit_price
        ):
            raise ValueError("execution plan must preserve authoritative market policy band")
        if not self.minimum_limit_price <= self.limit_price <= self.maximum_limit_price:
            raise ValueError("limit_price must be inside the allowed band")
        if self.order_type is not OrderType.LIMIT or self.time_in_force is not TimeInForce.DAY:
            raise ValueError("Phase 1A execution supports LIMIT DAY only")
        require_utc(self.created_at, "created_at")
        require_utc(self.expires_at, "expires_at")
        if self.expires_at <= self.created_at:
            raise ValueError("execution plan must expire after creation")
        for name in ("created_monotonic", "expires_monotonic"):
            value = getattr(self, name)
            if type(value) not in (int, float) or not isfinite(value) or value < 0:
                raise ValueError(f"{name} must be a non-negative monotonic reading")
        if self.expires_monotonic <= self.created_monotonic:
            raise ValueError("execution plan monotonic expiry must follow creation")


@dataclass(frozen=True)
class OrderRequest:
    """Immutable order using an internal, non-secret ``account_id`` alias.

    The broker account number from ``.env`` must never enter this domain contract.
    """

    client_order_id: str
    execution_plan_id: str
    account_id: str
    instrument: InstrumentId
    side: Side
    order_type: OrderType
    time_in_force: TimeInForce
    quantity: Decimal
    limit_price: Decimal
    created_at: datetime

    def __post_init__(self) -> None:
        require_id(self.client_order_id, "client_order_id")
        require_id(self.execution_plan_id, "execution_plan_id")
        require_id(self.account_id, "account_id")
        require_enum(self.side, Side, "side")
        require_enum(self.order_type, OrderType, "order_type")
        require_enum(self.time_in_force, TimeInForce, "time_in_force")
        for name in ("quantity", "limit_price"):
            require_decimal(getattr(self, name), name)
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.quantity != self.quantity.to_integral_value():
            raise ValueError("Phase 1A OrderRequest quantity must be integral shares")
        if self.order_type is not OrderType.LIMIT or self.time_in_force is not TimeInForce.DAY:
            raise ValueError("Phase 1A orders support LIMIT DAY only")
        require_utc(self.created_at, "created_at")


@dataclass(frozen=True)
class OrderSubmission:
    client_order_id: str
    state: SubmissionState
    occurred_at: datetime
    detail_code: str = ""

    def __post_init__(self) -> None:
        require_id(self.client_order_id, "client_order_id")
        require_enum(self.state, SubmissionState, "state")
        require_utc(self.occurred_at, "occurred_at")


@dataclass(frozen=True)
class BrokerOrder:
    broker_order_id: str
    account_id: str
    requested: Decimal
    filled: Decimal
    open: Decimal
    canceled: Decimal
    rejected: Decimal
    expired: Decimal
    execution_state: BrokerExecutionState
    pending_action: PendingAction = PendingAction.NONE

    def __post_init__(self) -> None:
        require_id(self.broker_order_id, "broker_order_id")
        require_id(self.account_id, "account_id")
        require_enum(self.execution_state, BrokerExecutionState, "execution_state")
        require_enum(self.pending_action, PendingAction, "pending_action")
        parts = (self.filled, self.open, self.canceled, self.rejected, self.expired)
        for value in (self.requested, *parts):
            require_decimal(value, "broker order quantity")
            if value < 0:
                raise ValueError("broker order quantities cannot be negative")
        if self.requested == 0:
            raise ValueError("requested quantity must be positive")
        if self.requested != sum(parts, Decimal(0)):
            raise ValueError("requested quantity must equal observed quantity partition")
        state = self.execution_state
        if state is BrokerExecutionState.NOT_OBSERVED:
            raise ValueError("BrokerOrder requires an observed execution state")
        if state is BrokerExecutionState.OPEN and not (
            self.open == self.requested and self.filled == 0
        ):
            raise ValueError("OPEN order must be wholly open")
        if state is BrokerExecutionState.PARTIALLY_FILLED and not (
            self.filled > 0
            and self.open > 0
            and self.canceled == self.rejected == self.expired == 0
        ):
            raise ValueError("PARTIALLY_FILLED requires filled and open quantities")
        terminal_quantity = {
            BrokerExecutionState.FILLED: self.filled,
            BrokerExecutionState.REJECTED: self.rejected,
        }.get(state)
        if terminal_quantity is not None and terminal_quantity != self.requested:
            raise ValueError(f"{state} quantity must equal requested quantity")
        if state is BrokerExecutionState.CANCELED and not (
            self.canceled > 0
            and self.open == self.rejected == self.expired == 0
        ):
            raise ValueError("CANCELED requires canceled quantity")
        if state is BrokerExecutionState.EXPIRED and not (
            self.expired > 0
            and self.open == self.canceled == self.rejected == 0
        ):
            raise ValueError("EXPIRED requires expired quantity")
        if self.pending_action is not PendingAction.NONE and state not in {
            BrokerExecutionState.OPEN, BrokerExecutionState.PARTIALLY_FILLED,
        }:
            raise ValueError("pending cancel/replace requires an open execution state")


@dataclass(frozen=True)
class ObservedAmount:
    value: Decimal
    source_observation_id: str
    observed_at: datetime

    def __post_init__(self) -> None:
        require_decimal(self.value, "value")
        require_id(self.source_observation_id, "source_observation_id")
        require_utc(self.observed_at, "observed_at")


@dataclass(frozen=True)
class AccountSnapshot:
    snapshot_id: str
    account_id: str
    environment: TradingEnvironment
    quality: SnapshotQuality
    cash: ObservedAmount
    buying_power: ObservedAmount
    positions: ObservedAmount
    open_orders: ObservedAmount
    fees: ObservedAmount
    fx_rate: ObservedAmount
    captured_at: datetime

    def __post_init__(self) -> None:
        require_id(self.snapshot_id, "snapshot_id")
        require_id(self.account_id, "account_id")
        require_enum(self.environment, TradingEnvironment, "environment")
        require_enum(self.quality, SnapshotQuality, "quality")
        require_utc(self.captured_at, "captured_at")

    def is_fresh_consistent(self, now: datetime, max_age_seconds: int) -> bool:
        require_utc(now, "now")
        observations = (
            self.cash, self.buying_power, self.positions, self.open_orders, self.fees, self.fx_rate
        )
        return (
            self.quality is SnapshotQuality.CONSISTENT
            and all(0 <= (now - item.observed_at).total_seconds() <= max_age_seconds for item in observations)
        )

    def valid_until(self, max_age_seconds: int) -> datetime:
        observations = (
            self.cash, self.buying_power, self.positions, self.open_orders, self.fees, self.fx_rate
        )
        return min(item.observed_at for item in observations) + timedelta(seconds=max_age_seconds)


@dataclass(frozen=True)
class MarketEvidence:
    snapshot_id: str
    environment: TradingEnvironment
    quality: SnapshotQuality
    observed_at: datetime
    pricing_policy_version: str
    minimum_limit_price: Decimal
    maximum_limit_price: Decimal

    def __post_init__(self) -> None:
        require_id(self.snapshot_id, "snapshot_id")
        require_enum(self.environment, TradingEnvironment, "environment")
        require_enum(self.quality, SnapshotQuality, "quality")
        require_utc(self.observed_at, "observed_at")
        require_id(self.pricing_policy_version, "pricing_policy_version")
        for name in ("minimum_limit_price", "maximum_limit_price"):
            require_decimal(getattr(self, name), name)
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.maximum_limit_price < self.minimum_limit_price:
            raise ValueError("market evidence price band is inverted")

    def is_fresh_consistent(self, now: datetime, max_age_seconds: int) -> bool:
        require_utc(now, "now")
        age = (now - self.observed_at).total_seconds()
        return self.quality is SnapshotQuality.CONSISTENT and 0 <= age <= max_age_seconds

    def valid_until(self, max_age_seconds: int) -> datetime:
        return self.observed_at + timedelta(seconds=max_age_seconds)


@dataclass(frozen=True)
class TradingPermit:
    permit_id: str
    account_id: str
    environment: TradingEnvironment
    scope: PermitScope
    safety_epoch: int
    client_order_id: str | None
    risk_decision_id: str | None
    execution_plan_id: str | None
    account_snapshot_id: str | None
    market_snapshot_id: str | None
    policy_version: str | None
    deployment_version: str | None
    issued_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        for name in ("permit_id", "account_id"):
            require_id(getattr(self, name), name)
        if self.safety_epoch < 0:
            raise ValueError("safety_epoch cannot be negative")
        require_enum(self.scope, PermitScope, "scope")
        require_enum(self.environment, TradingEnvironment, "environment")
        binding_claims = (
            self.client_order_id,
            self.risk_decision_id,
            self.execution_plan_id,
        )
        if self.scope is PermitScope.NEW_ORDER:
            for name in (
                "client_order_id", "risk_decision_id", "execution_plan_id",
            ):
                require_id(getattr(self, name), name)
        elif any(value is not None for value in binding_claims):
            raise ValueError("only NEW_ORDER permits can carry order binding claims")
        evidence_claims = (
            self.account_snapshot_id,
            self.market_snapshot_id,
            self.policy_version,
            self.deployment_version,
        )
        if self.scope is PermitScope.CANCEL:
            if any(value is not None for value in evidence_claims):
                raise ValueError("CANCEL permit evidence claims must be None")
        else:
            for name in (
                "account_snapshot_id", "market_snapshot_id",
                "policy_version", "deployment_version",
            ):
                require_id(getattr(self, name), name)
        require_utc(self.issued_at, "issued_at")
        require_utc(self.expires_at, "expires_at")
        if self.expires_at <= self.issued_at:
            raise ValueError("permit must expire after issuance")


@dataclass(frozen=True)
class OperatorCommand:
    """Authenticated operator intent containing only an internal account alias."""

    command_id: str
    actor: str
    reason: str
    deployment_version: str
    expected_safety_epoch: int
    requested_at: datetime
    expires_at: datetime
    action: OperatorAction
    account_id: str
    environment: TradingEnvironment
    client_order_id: str | None = None
    risk_decision_id: str | None = None
    execution_plan_id: str | None = None

    def __post_init__(self) -> None:
        for name in ("command_id", "actor", "reason", "deployment_version"):
            require_id(getattr(self, name), name)
        if self.expected_safety_epoch < 0:
            raise ValueError("expected_safety_epoch cannot be negative")
        require_enum(self.action, OperatorAction, "action")
        require_enum(self.environment, TradingEnvironment, "environment")
        require_utc(self.requested_at, "requested_at")
        require_utc(self.expires_at, "expires_at")
        if self.expires_at <= self.requested_at:
            raise ValueError("operator command must expire after it is requested")
        require_id(self.account_id, "account_id")
        for name in (
            "client_order_id", "risk_decision_id", "execution_plan_id",
        ):
            value = getattr(self, name)
            if value is not None:
                require_id(value, name)
        if self.action is OperatorAction.RESOLVE_SUBMITTED_UNKNOWN:
            require_id(self.client_order_id, "client_order_id")
            if self.risk_decision_id is not None or self.execution_plan_id is not None:
                raise ValueError(
                    "unknown resolution cannot carry risk or plan binding claims"
                )
