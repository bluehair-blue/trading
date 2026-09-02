from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Context, Decimal, ROUND_HALF_EVEN, localcontext
from enum import StrEnum
import json

from trader.domain.accounting import AccountingSeed, fold_accounting
from trader.domain.broker_lifecycle import (
    BROKER_LIFECYCLE_FACT_TYPES,
    BrokerFillObserved,
    BrokerLifecycleFact,
    BrokerOrderOpened,
)
from trader.domain.models import InstrumentId, Side, require_decimal, require_utc


ZERO = Decimal(0)
REFERENCE_ONLY = "REFERENCE_ONLY"
_PERFORMANCE_CONTEXT = Context(
    prec=34,
    rounding=ROUND_HALF_EVEN,
    Emin=-999_999,
    Emax=999_999,
    capitals=1,
    clamp=0,
)


class EvaluationStatus(StrEnum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"


class IncompleteReasonCode(StrEnum):
    MISSING_MARK = "MISSING_MARK"
    STALE_MARK = "STALE_MARK"
    ACCOUNTING_INVARIANT = "ACCOUNTING_INVARIANT"


@dataclass(frozen=True)
class PerformanceMark:
    instrument: InstrumentId
    price: Decimal

    def __post_init__(self) -> None:
        if type(self.instrument) is not InstrumentId:
            raise ValueError("instrument must be exact InstrumentId")
        require_decimal(self.price, "mark price")
        if self.price <= 0:
            raise ValueError("mark price must be positive")


@dataclass(frozen=True)
class MarkUnavailable:
    instrument: InstrumentId
    code: IncompleteReasonCode

    def __post_init__(self) -> None:
        if type(self.instrument) is not InstrumentId:
            raise ValueError("instrument must be exact InstrumentId")
        if self.code not in {
            IncompleteReasonCode.MISSING_MARK,
            IncompleteReasonCode.STALE_MARK,
        }:
            raise ValueError("mark unavailability must be MISSING_MARK or STALE_MARK")


@dataclass(frozen=True)
class ValuationCheckpoint:
    checkpoint_at: datetime
    marks: tuple[PerformanceMark, ...] = ()
    unavailable_marks: tuple[MarkUnavailable, ...] = ()
    is_session_close: bool = True
    is_sample_end: bool = False

    def __post_init__(self) -> None:
        require_utc(self.checkpoint_at, "checkpoint_at")
        if type(self.marks) is not tuple or any(
            type(mark) is not PerformanceMark for mark in self.marks
        ):
            raise ValueError("marks must be an exact tuple of PerformanceMark")
        if type(self.unavailable_marks) is not tuple or any(
            type(reason) is not MarkUnavailable for reason in self.unavailable_marks
        ):
            raise ValueError("unavailable_marks must be an exact tuple of MarkUnavailable")
        if type(self.is_session_close) is not bool:
            raise ValueError("is_session_close must be a bool")
        if type(self.is_sample_end) is not bool:
            raise ValueError("is_sample_end must be a bool")
        for mark in self.marks:
            mark.__post_init__()
        for reason in self.unavailable_marks:
            reason.__post_init__()
        mark_instruments = tuple(mark.instrument for mark in self.marks)
        unavailable_instruments = tuple(reason.instrument for reason in self.unavailable_marks)
        if len(mark_instruments) != len(set(mark_instruments)):
            raise ValueError("checkpoint marks must be unique by instrument")
        if len(unavailable_instruments) != len(set(unavailable_instruments)):
            raise ValueError("unavailable marks must be unique by instrument")
        if set(mark_instruments) & set(unavailable_instruments):
            raise ValueError("an instrument cannot have both a mark and an unavailable reason")


@dataclass(frozen=True)
class IncompleteReason:
    code: IncompleteReasonCode
    checkpoint_at: datetime
    instrument: InstrumentId | None = None


@dataclass(frozen=True)
class PositionValuation:
    instrument: InstrumentId
    quantity: Decimal
    average_cost: Decimal
    cost_basis: Decimal
    mark_price: Decimal | None
    market_value: Decimal | None
    unrealized_pnl: Decimal | None


@dataclass(frozen=True)
class ValuationSnapshot:
    checkpoint_at: datetime
    is_session_close: bool
    is_sample_end: bool
    evaluation_status: EvaluationStatus
    incomplete_reasons: tuple[IncompleteReason, ...]
    cash: Decimal
    positions: tuple[PositionValuation, ...]
    equity: Decimal | None
    realized_pnl: Decimal
    unrealized_pnl: Decimal | None
    net_pnl: Decimal | None


@dataclass(frozen=True)
class PerformanceProjection:
    evaluation_status: EvaluationStatus
    incomplete_reasons: tuple[IncompleteReason, ...]
    starting_equity: Decimal
    ending_equity: Decimal | None
    realized_pnl: Decimal
    unrealized_pnl: Decimal | None
    net_pnl: Decimal | None
    cumulative_return: Decimal | None
    maximum_session_close_drawdown: Decimal | None
    gross_traded_value: Decimal
    total_fees: Decimal
    fills: int
    gross_turnover: Decimal | None
    snapshots: tuple[ValuationSnapshot, ...]

    @property
    def evidence_use(self) -> str:
        return REFERENCE_ONLY

    def canonical_payload(self) -> dict[str, object]:
        return _projection_payload(self)

    def canonical_json(self) -> str:
        return json.dumps(
            self.canonical_payload(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )


@dataclass
class _BasisState:
    quantity: Decimal = ZERO
    cost_basis: Decimal = ZERO
    realized_pnl: Decimal = ZERO


def _canonical_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
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


def _reason_payload(reason: IncompleteReason) -> dict[str, object]:
    return {
        "code": reason.code.value,
        "checkpoint_at": reason.checkpoint_at.isoformat(),
        "instrument": (
            None if reason.instrument is None else _instrument_payload(reason.instrument)
        ),
    }


def _position_payload(position: PositionValuation) -> dict[str, object]:
    return {
        "instrument": _instrument_payload(position.instrument),
        "quantity": str(int(position.quantity)),
        "average_cost": _canonical_decimal(position.average_cost),
        "cost_basis": _canonical_decimal(position.cost_basis),
        "mark_price": _canonical_decimal(position.mark_price),
        "market_value": _canonical_decimal(position.market_value),
        "unrealized_pnl": _canonical_decimal(position.unrealized_pnl),
    }


def _snapshot_payload(snapshot: ValuationSnapshot) -> dict[str, object]:
    return {
        "checkpoint_at": snapshot.checkpoint_at.isoformat(),
        "is_session_close": snapshot.is_session_close,
        "is_sample_end": snapshot.is_sample_end,
        "evaluation_status": snapshot.evaluation_status.value,
        "incomplete_reasons": [
            _reason_payload(reason) for reason in snapshot.incomplete_reasons
        ],
        "cash": _canonical_decimal(snapshot.cash),
        "positions": [_position_payload(position) for position in snapshot.positions],
        "equity": _canonical_decimal(snapshot.equity),
        "realized_pnl": _canonical_decimal(snapshot.realized_pnl),
        "unrealized_pnl": _canonical_decimal(snapshot.unrealized_pnl),
        "net_pnl": _canonical_decimal(snapshot.net_pnl),
    }


def _projection_payload(projection: PerformanceProjection) -> dict[str, object]:
    return {
        "evidence_use": projection.evidence_use,
        "evaluation_status": projection.evaluation_status.value,
        "incomplete_reasons": [
            _reason_payload(reason) for reason in projection.incomplete_reasons
        ],
        "starting_equity": _canonical_decimal(projection.starting_equity),
        "ending_equity": _canonical_decimal(projection.ending_equity),
        "realized_pnl": _canonical_decimal(projection.realized_pnl),
        "unrealized_pnl": _canonical_decimal(projection.unrealized_pnl),
        "net_pnl": _canonical_decimal(projection.net_pnl),
        "cumulative_return": _canonical_decimal(projection.cumulative_return),
        "maximum_session_close_drawdown": _canonical_decimal(
            projection.maximum_session_close_drawdown
        ),
        "gross_traded_value": _canonical_decimal(projection.gross_traded_value),
        "total_fees": _canonical_decimal(projection.total_fees),
        "fills": projection.fills,
        "gross_turnover": _canonical_decimal(projection.gross_turnover),
        "session_close_equity": [
            {
                "checkpoint_at": snapshot.checkpoint_at.isoformat(),
                "equity": _canonical_decimal(snapshot.equity),
            }
            for snapshot in projection.snapshots
            if snapshot.is_session_close
        ],
        "snapshots": [_snapshot_payload(snapshot) for snapshot in projection.snapshots],
    }


def _fills_with_orders(
    facts: tuple[BrokerLifecycleFact, ...],
) -> tuple[tuple[BrokerFillObserved, BrokerOrderOpened], ...]:
    opened_by_order: dict[str, BrokerOrderOpened] = {}
    fills: list[tuple[BrokerFillObserved, BrokerOrderOpened]] = []
    for fact in facts:
        if type(fact) is BrokerOrderOpened:
            opened_by_order[fact.client_order_id] = fact
        elif type(fact) is BrokerFillObserved:
            opened = opened_by_order.get(fact.client_order_id)
            if opened is None:
                raise ValueError("broker fill must follow its order-opened fact")
            fills.append((fact, opened))
    return tuple(fills)


def _basis_at(
    fills: tuple[tuple[BrokerFillObserved, BrokerOrderOpened], ...],
    checkpoint_at: datetime,
) -> dict[InstrumentId, _BasisState]:
    states: dict[InstrumentId, _BasisState] = {}
    for fill, opened in fills:
        if fill.occurred_at > checkpoint_at:
            continue
        state = states.setdefault(opened.instrument, _BasisState())
        notional = fill.price * fill.quantity
        if opened.side is Side.BUY:
            state.quantity += fill.quantity
            state.cost_basis += notional + fill.fee
            continue
        if fill.quantity > state.quantity:
            raise ValueError("performance fill would create a short position")
        average_cost = state.cost_basis / state.quantity
        allocated_basis = average_cost * fill.quantity
        state.realized_pnl += notional - fill.fee - allocated_basis
        state.quantity -= fill.quantity
        state.cost_basis -= allocated_basis
        if state.quantity == 0:
            state.cost_basis = ZERO
    return states


def _maximum_drawdown(equities: tuple[Decimal, ...]) -> Decimal | None:
    if not equities:
        return ZERO
    peak = equities[0]
    maximum = ZERO
    for equity in equities[1:]:
        if equity > peak:
            peak = equity
        elif peak > 0:
            maximum = max(maximum, (peak - equity) / peak)
    return maximum


def _project_performance(
    seed: AccountingSeed,
    facts: tuple[BrokerLifecycleFact, ...],
    checkpoints: tuple[ValuationCheckpoint, ...],
) -> PerformanceProjection:
    """Project Phase A performance without changing execution or ledger state."""
    if type(seed) is not AccountingSeed:
        raise TypeError("exact AccountingSeed required")
    seed.__post_init__()
    if seed.positions:
        raise ValueError("Phase A performance requires zero starting positions")
    if type(facts) is not tuple or any(
        type(fact) not in BROKER_LIFECYCLE_FACT_TYPES for fact in facts
    ):
        raise TypeError("facts must be an exact tuple of broker lifecycle facts")
    if type(checkpoints) is not tuple or any(
        type(checkpoint) is not ValuationCheckpoint for checkpoint in checkpoints
    ):
        raise TypeError("checkpoints must be an exact tuple of ValuationCheckpoint")
    if not checkpoints:
        raise ValueError("at least one valuation checkpoint is required")
    for checkpoint in checkpoints:
        checkpoint.__post_init__()
    if any(
        current.checkpoint_at >= following.checkpoint_at
        for current, following in zip(checkpoints, checkpoints[1:], strict=False)
    ):
        raise ValueError("valuation checkpoints must be strictly chronological")

    fold_accounting(seed, facts)
    fills_with_orders = _fills_with_orders(facts)
    snapshots: list[ValuationSnapshot] = []
    for checkpoint in checkpoints:
        scoped_facts = tuple(
            fact for fact in facts if fact.occurred_at <= checkpoint.checkpoint_at
        )
        accounting = fold_accounting(seed, scoped_facts)
        basis = _basis_at(fills_with_orders, checkpoint.checkpoint_at)
        mark_by_instrument = {mark.instrument: mark.price for mark in checkpoint.marks}
        unavailable_by_instrument = {
            reason.instrument: reason.code for reason in checkpoint.unavailable_marks
        }
        reasons: list[IncompleteReason] = []
        positions: list[PositionValuation] = []
        unrealized = ZERO
        realized = sum((state.realized_pnl for state in basis.values()), ZERO)
        accounting_positions = {
            position.instrument: position.quantity for position in accounting.positions
        }
        if accounting_positions != {
            instrument: state.quantity
            for instrument, state in basis.items()
            if state.quantity != 0
        }:
            reasons.append(
                IncompleteReason(
                    IncompleteReasonCode.ACCOUNTING_INVARIANT,
                    checkpoint.checkpoint_at,
                )
            )

        for instrument, quantity in sorted(
            accounting_positions.items(),
            key=lambda item: (
                item[0].market,
                item[0].symbol,
                item[0].currency,
            ),
        ):
            state = basis[instrument]
            average_cost = state.cost_basis / quantity
            mark_price = mark_by_instrument.get(instrument)
            if mark_price is None:
                reasons.append(
                    IncompleteReason(
                        unavailable_by_instrument.get(
                            instrument, IncompleteReasonCode.MISSING_MARK
                        ),
                        checkpoint.checkpoint_at,
                        instrument,
                    )
                )
                positions.append(
                    PositionValuation(
                        instrument,
                        quantity,
                        average_cost,
                        state.cost_basis,
                        None,
                        None,
                        None,
                    )
                )
                continue
            market_value = quantity * mark_price
            position_unrealized = market_value - state.cost_basis
            unrealized += position_unrealized
            positions.append(
                PositionValuation(
                    instrument,
                    quantity,
                    average_cost,
                    state.cost_basis,
                    mark_price,
                    market_value,
                    position_unrealized,
                )
            )

        equity: Decimal | None = None
        unrealized_output: Decimal | None = None
        net_pnl: Decimal | None = None
        if not reasons:
            market_value = sum(
                (position.market_value or ZERO for position in positions), ZERO
            )
            equity = accounting.cash + market_value
            unrealized_output = unrealized
            net_pnl = equity - seed.cash
            if net_pnl != realized + unrealized:
                reasons.append(
                    IncompleteReason(
                        IncompleteReasonCode.ACCOUNTING_INVARIANT,
                        checkpoint.checkpoint_at,
                    )
                )
                equity = None
                unrealized_output = None
                net_pnl = None

        snapshots.append(
            ValuationSnapshot(
                checkpoint.checkpoint_at,
                checkpoint.is_session_close,
                checkpoint.is_sample_end,
                (
                    EvaluationStatus.COMPLETE
                    if not reasons
                    else EvaluationStatus.INCOMPLETE
                ),
                tuple(reasons),
                accounting.cash,
                tuple(positions),
                equity,
                realized,
                unrealized_output,
                net_pnl,
            )
        )

    all_reasons = tuple(
        reason for snapshot in snapshots for reason in snapshot.incomplete_reasons
    )
    status = (
        EvaluationStatus.COMPLETE if not all_reasons else EvaluationStatus.INCOMPLETE
    )
    sample_ends = tuple(snapshot for snapshot in snapshots if snapshot.is_sample_end)
    if len(sample_ends) > 1:
        raise ValueError("only one sample-end valuation checkpoint is allowed")
    ending = sample_ends[0] if sample_ends else snapshots[-1]
    final_accounting = fold_accounting(
        seed,
        tuple(fact for fact in facts if fact.occurred_at <= ending.checkpoint_at),
    )
    cumulative_return = None
    maximum_drawdown = None
    gross_turnover = None
    if status is EvaluationStatus.COMPLETE:
        if seed.cash != 0 and ending.net_pnl is not None:
            cumulative_return = ending.net_pnl / seed.cash
        session_equities = tuple(
            snapshot.equity
            for snapshot in snapshots
            if snapshot.is_session_close and snapshot.equity is not None
        )
        maximum_drawdown = _maximum_drawdown((seed.cash,) + session_equities)
        complete_equities = (seed.cash,) + tuple(
            snapshot.equity
            for snapshot in snapshots
            if snapshot.equity is not None
        )
        mean_equity = sum(complete_equities, ZERO) / Decimal(len(complete_equities))
        if mean_equity != 0:
            gross_turnover = final_accounting.gross_traded_value / mean_equity

    return PerformanceProjection(
        status,
        all_reasons,
        seed.cash,
        ending.equity,
        ending.realized_pnl,
        ending.unrealized_pnl,
        ending.net_pnl,
        cumulative_return,
        maximum_drawdown,
        final_accounting.gross_traded_value,
        final_accounting.total_fees,
        sum(
            1
            for fill, _opened in fills_with_orders
            if fill.occurred_at <= ending.checkpoint_at
        ),
        gross_turnover,
        tuple(snapshots),
    )


def project_performance(
    seed: AccountingSeed,
    facts: tuple[BrokerLifecycleFact, ...],
    checkpoints: tuple[ValuationCheckpoint, ...],
) -> PerformanceProjection:
    """Project Phase A performance under a fixed decimal128-style context."""
    with localcontext(_PERFORMANCE_CONTEXT):
        return _project_performance(seed, facts, checkpoints)
