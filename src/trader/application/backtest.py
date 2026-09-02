from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
import json
import re

from trader.domain.accounting import AccountingPosition, AccountingProjection
from trader.domain.models import (
    BrokerExecutionState,
    InstrumentId,
    PositionTarget,
    Side,
    StrategyDecision,
    TargetUnit,
    TradingEnvironment,
    canonical_share_quantity,
    require_decimal,
    require_enum,
    require_id,
    require_utc,
)


_SHA256 = re.compile(r"[0-9a-f]{64}")


def _canonical_decimal(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _instrument_payload(instrument: InstrumentId) -> dict[str, str]:
    return {
        "market": instrument.market,
        "symbol": instrument.symbol,
        "currency": instrument.currency,
    }


def _instrument_key(instrument: InstrumentId) -> tuple[str, str, str]:
    return instrument.market, instrument.symbol, instrument.currency


def _require_share_quantity(value: Decimal, name: str, *, allow_zero: bool) -> None:
    canonical_share_quantity(value, name)
    if not allow_zero and value <= 0:
        raise ValueError(f"{name} must be positive integral shares")


def _require_sha256(value: str, name: str) -> None:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA256 digest")


@dataclass(frozen=True)
class PreviousCloseThresholdConfig:
    version: str
    threshold: Decimal
    target_quantity: Decimal

    def __post_init__(self) -> None:
        require_id(self.version, "version")
        require_decimal(self.threshold, "threshold")
        if self.threshold <= 0:
            raise ValueError("threshold must be positive")
        _require_share_quantity(self.target_quantity, "target_quantity", allow_zero=True)


@dataclass(frozen=True)
class BacktestDecisionRecord:
    decision: StrategyDecision
    target: PositionTarget
    previous_close_mid: Decimal

    @property
    def strategy_decision(self) -> StrategyDecision:
        return self.decision

    @property
    def position_target(self) -> PositionTarget:
        return self.target

    def __post_init__(self) -> None:
        if type(self.decision) is not StrategyDecision:
            raise ValueError("decision must be exact StrategyDecision")
        if type(self.target) is not PositionTarget:
            raise ValueError("target must be exact PositionTarget")
        self.decision.__post_init__()
        self.target.__post_init__()
        require_decimal(self.previous_close_mid, "previous_close_mid")
        if self.previous_close_mid <= 0:
            raise ValueError("previous_close_mid must be positive")
        if self.target.source_decision_id != self.decision.decision_id:
            raise ValueError("target must reference its decision")
        if self.target.strategy_version != self.decision.strategy_version:
            raise ValueError("target and decision strategy versions must match")
        if self.target.input_snapshot_id != self.decision.input_snapshot_id:
            raise ValueError("target and decision input snapshots must match")
        if self.target.target_at != self.decision.decided_at:
            raise ValueError("target timestamp must match decision timestamp")
        if self.target.unit is not TargetUnit.SHARES:
            raise ValueError("backtest targets must use SHARES")
        if type(self.target.instrument) is not InstrumentId:
            raise ValueError("target instrument must be exact InstrumentId")


@dataclass(frozen=True)
class BacktestOrderRecord:
    client_order_id: str
    broker_order_id: str | None
    side: Side
    requested_quantity: Decimal
    filled_quantity: Decimal
    limit_price: Decimal
    execution_state: BrokerExecutionState

    def __post_init__(self) -> None:
        require_id(self.client_order_id, "client_order_id")
        if self.broker_order_id is not None:
            require_id(self.broker_order_id, "broker_order_id")
        require_enum(self.side, Side, "side")
        require_enum(self.execution_state, BrokerExecutionState, "execution_state")
        _require_share_quantity(self.requested_quantity, "requested_quantity", allow_zero=False)
        _require_share_quantity(self.filled_quantity, "filled_quantity", allow_zero=True)
        require_decimal(self.limit_price, "limit_price")
        if self.limit_price <= 0:
            raise ValueError("limit_price must be positive")
        if self.filled_quantity > self.requested_quantity:
            raise ValueError("filled_quantity cannot exceed requested_quantity")
        if self.execution_state is BrokerExecutionState.NOT_OBSERVED:
            raise ValueError("backtest order requires an observed execution state")
        state = self.execution_state
        if state is BrokerExecutionState.OPEN and self.filled_quantity != 0:
            raise ValueError("OPEN order must be wholly unfilled")
        if state is BrokerExecutionState.PARTIALLY_FILLED and not (
            0 < self.filled_quantity < self.requested_quantity
        ):
            raise ValueError("PARTIALLY_FILLED requires a partial fill")
        if state is BrokerExecutionState.FILLED and self.filled_quantity != self.requested_quantity:
            raise ValueError("FILLED quantity must equal requested quantity")
        if state is BrokerExecutionState.REJECTED and self.filled_quantity != 0:
            raise ValueError("REJECTED order cannot have fills")
        if state in {BrokerExecutionState.CANCELED, BrokerExecutionState.EXPIRED} and (
            self.filled_quantity >= self.requested_quantity
        ):
            raise ValueError(f"{state} order must retain unresolved quantity")


@dataclass(frozen=True)
class BacktestFillRecord:
    client_order_id: str
    broker_execution_id: str
    side: Side
    quantity: Decimal
    price: Decimal
    fee: Decimal
    occurred_at: datetime

    def __post_init__(self) -> None:
        require_id(self.client_order_id, "client_order_id")
        require_id(self.broker_execution_id, "broker_execution_id")
        require_enum(self.side, Side, "side")
        _require_share_quantity(self.quantity, "quantity", allow_zero=False)
        require_decimal(self.price, "price")
        require_decimal(self.fee, "fee")
        if self.price <= 0:
            raise ValueError("price must be positive")
        if self.fee < 0:
            raise ValueError("fee cannot be negative")
        require_utc(self.occurred_at, "occurred_at")


@dataclass(frozen=True)
class BacktestOutput:
    run_spec_fingerprint: str
    quote_count: int
    decisions: tuple[BacktestDecisionRecord, ...]
    orders: tuple[BacktestOrderRecord, ...]
    fills: tuple[BacktestFillRecord, ...]
    accounting: AccountingProjection

    def __post_init__(self) -> None:
        _require_sha256(self.run_spec_fingerprint, "run_spec_fingerprint")
        if type(self.quote_count) is not int or self.quote_count < 0:
            raise ValueError("quote_count must be a non-negative integer")
        if type(self.decisions) is not tuple or any(
            type(item) is not BacktestDecisionRecord for item in self.decisions
        ):
            raise ValueError("decisions must be an exact tuple of BacktestDecisionRecord")
        if type(self.orders) is not tuple or any(
            type(item) is not BacktestOrderRecord for item in self.orders
        ):
            raise ValueError("orders must be an exact tuple of BacktestOrderRecord")
        if type(self.fills) is not tuple or any(
            type(item) is not BacktestFillRecord for item in self.fills
        ):
            raise ValueError("fills must be an exact tuple of BacktestFillRecord")
        for decision in self.decisions:
            decision.__post_init__()
        for order in self.orders:
            order.__post_init__()
        for fill in self.fills:
            fill.__post_init__()
        if any(
            current.decision.decided_at < prior.decision.decided_at
            for prior, current in zip(self.decisions, self.decisions[1:])
        ):
            raise ValueError("backtest decisions must preserve replay order")
        if any(
            current.occurred_at < prior.occurred_at
            for prior, current in zip(self.fills, self.fills[1:])
        ):
            raise ValueError("backtest fills must preserve replay order")
        self._validate_unique_ids()
        self._validate_fill_bindings()
        self._validate_accounting()

    def _validate_unique_ids(self) -> None:
        decision_ids = tuple(item.decision.decision_id for item in self.decisions)
        target_ids = tuple(item.target.target_id for item in self.decisions)
        client_order_ids = tuple(item.client_order_id for item in self.orders)
        broker_order_ids = tuple(
            item.broker_order_id for item in self.orders if item.broker_order_id is not None
        )
        execution_ids = tuple(item.broker_execution_id for item in self.fills)
        for name, values in (
            ("decision_id", decision_ids),
            ("target_id", target_ids),
            ("client_order_id", client_order_ids),
            ("broker_order_id", broker_order_ids),
            ("broker_execution_id", execution_ids),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"{name} must be unique in backtest output")

    def _validate_fill_bindings(self) -> None:
        orders_by_client = {item.client_order_id: item for item in self.orders}
        filled_by_client: dict[str, Decimal] = {}
        for fill in self.fills:
            order = orders_by_client.get(fill.client_order_id)
            if order is None:
                raise ValueError("fill must reference an output order")
            if order.side is not fill.side:
                raise ValueError("fill side must match its order")
            filled_by_client[fill.client_order_id] = (
                filled_by_client.get(fill.client_order_id, Decimal(0)) + fill.quantity
            )
        for client_order_id, order in orders_by_client.items():
            total = filled_by_client.get(client_order_id, Decimal(0))
            if total != order.filled_quantity:
                raise ValueError("fills must equal each order's filled quantity")

    def _validate_accounting(self) -> None:
        if type(self.accounting) is not AccountingProjection:
            raise ValueError("accounting must be exact AccountingProjection")
        require_id(self.accounting.account_id, "accounting.account_id")
        if type(self.accounting.environment) is not TradingEnvironment:
            raise ValueError("accounting.environment must be TradingEnvironment")
        if self.accounting.environment is not TradingEnvironment.SIMULATED:
            raise ValueError("backtest accounting must be SIMULATED")
        require_id(self.accounting.currency, "accounting.currency")
        require_id(self.accounting.policy_version, "accounting.policy_version")
        require_decimal(self.accounting.cash, "accounting.cash")
        if self.accounting.cash < 0:
            raise ValueError("accounting cash cannot be negative")
        require_decimal(self.accounting.gross_traded_value, "accounting.gross_traded_value")
        require_decimal(self.accounting.total_fees, "accounting.total_fees")
        if self.accounting.gross_traded_value < 0 or self.accounting.total_fees < 0:
            raise ValueError("accounting totals cannot be negative")
        if type(self.accounting.positions) is not tuple or any(
            type(position) is not AccountingPosition for position in self.accounting.positions
        ):
            raise ValueError("accounting.positions must be an exact tuple of AccountingPosition")
        for position in self.accounting.positions:
            position.__post_init__()
        instruments = tuple(position.instrument for position in self.accounting.positions)
        if len(instruments) != len(set(instruments)):
            raise ValueError("accounting positions must have unique instruments")
        if self.accounting.currency != "USD" or any(
            position.instrument.currency != self.accounting.currency
            for position in self.accounting.positions
        ):
            raise ValueError("backtest accounting supports USD instruments only")

    def canonical_json(self) -> str:
        order_ordinals = {
            order.client_order_id: ordinal
            for ordinal, order in enumerate(self.orders, start=1)
        }
        payload: dict[str, object] = {
            "run_spec_fingerprint": self.run_spec_fingerprint,
            "quote_count": self.quote_count,
            "decisions": [_canonical_decision(item) for item in self.decisions],
            "orders": [_canonical_order(item) for item in self.orders],
            "fills": [
                _canonical_fill(item, order_ordinals[item.client_order_id])
                for item in self.fills
            ],
            "accounting": _canonical_accounting(self.accounting),
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def fingerprint(self) -> str:
        return sha256(self.canonical_json().encode("utf-8")).hexdigest()


def evaluate_previous_close_threshold(
    config: PreviousCloseThresholdConfig,
    strategy_id: str,
    decision_id: str,
    target_id: str,
    input_snapshot_id: str,
    instrument: InstrumentId,
    previous_close_mid: Decimal,
    decided_at: datetime,
) -> BacktestDecisionRecord:
    if type(config) is not PreviousCloseThresholdConfig:
        raise TypeError("exact PreviousCloseThresholdConfig required")
    config.__post_init__()
    require_id(strategy_id, "strategy_id")
    require_id(decision_id, "decision_id")
    require_id(target_id, "target_id")
    require_id(input_snapshot_id, "input_snapshot_id")
    if type(instrument) is not InstrumentId:
        raise ValueError("instrument must be exact InstrumentId")
    require_decimal(previous_close_mid, "previous_close_mid")
    if previous_close_mid <= 0:
        raise ValueError("previous_close_mid must be positive")
    require_utc(decided_at, "decided_at")
    signal = "LONG" if previous_close_mid > config.threshold else "FLAT"
    rationale = (
        "previous_close_mid_above_threshold"
        if signal == "LONG"
        else "previous_close_mid_at_or_below_threshold"
    )
    decision = StrategyDecision(
        decision_id,
        config.version,
        input_snapshot_id,
        signal,
        rationale,
        decided_at,
    )
    target = PositionTarget(
        target_id,
        strategy_id,
        decision_id,
        config.version,
        input_snapshot_id,
        instrument,
        Decimal(config.target_quantity) if signal == "LONG" else Decimal(0),
        TargetUnit.SHARES,
        decided_at,
    )
    return BacktestDecisionRecord(decision, target, previous_close_mid)


def _canonical_decision(item: BacktestDecisionRecord) -> dict[str, object]:
    decision = item.decision
    target = item.target
    return {
        "strategy_id": target.strategy_id,
        "strategy_version": decision.strategy_version,
        "input_snapshot_id": decision.input_snapshot_id,
        "signal": decision.signal,
        "rationale": decision.rationale,
        "decided_at": decision.decided_at.isoformat(),
        "previous_close_mid": _canonical_decimal(item.previous_close_mid),
        "instrument": _instrument_payload(target.instrument),
        "quantity": _canonical_decimal(target.quantity),
        "unit": target.unit.value,
        "target_at": target.target_at.isoformat(),
    }


def _canonical_order(item: BacktestOrderRecord) -> dict[str, object]:
    return {
        "side": item.side.value,
        "requested_quantity": _canonical_decimal(item.requested_quantity),
        "filled_quantity": _canonical_decimal(item.filled_quantity),
        "limit_price": _canonical_decimal(item.limit_price),
        "execution_state": item.execution_state.value,
    }


def _canonical_fill(item: BacktestFillRecord, order_ordinal: int) -> dict[str, object]:
    return {
        "order_ordinal": order_ordinal,
        "side": item.side.value,
        "quantity": _canonical_decimal(item.quantity),
        "price": _canonical_decimal(item.price),
        "fee": _canonical_decimal(item.fee),
        "occurred_at": item.occurred_at.isoformat(),
    }


def _canonical_accounting(accounting: AccountingProjection) -> dict[str, object]:
    positions = sorted(accounting.positions, key=lambda item: _instrument_key(item.instrument))
    return {
        "environment": accounting.environment.value,
        "currency": accounting.currency,
        "policy_version": accounting.policy_version,
        "cash": _canonical_decimal(accounting.cash),
        "positions": [
            {
                "instrument": _instrument_payload(position.instrument),
                "quantity": _canonical_decimal(position.quantity),
            }
            for position in positions
        ],
        "gross_traded_value": _canonical_decimal(accounting.gross_traded_value),
        "total_fees": _canonical_decimal(accounting.total_fees),
    }
