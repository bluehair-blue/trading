from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from itertools import groupby

from trader.domain.models import InstrumentId, PositionTarget, TargetUnit, require_decimal, require_id, require_utc


@dataclass(frozen=True)
class StrategyQuantityBudget:
    strategy_id: str
    instrument: InstrumentId
    maximum_quantity: Decimal

    def __post_init__(self) -> None:
        require_id(self.strategy_id, "strategy_id")
        if type(self.instrument) is not InstrumentId:
            raise ValueError("instrument must be exact InstrumentId")
        require_decimal(self.maximum_quantity, "maximum_quantity")
        if self.maximum_quantity < 0:
            raise ValueError("maximum_quantity cannot be negative")


@dataclass(frozen=True)
class InstrumentQuantityLimit:
    instrument: InstrumentId
    maximum_quantity: Decimal

    def __post_init__(self) -> None:
        if type(self.instrument) is not InstrumentId:
            raise ValueError("instrument must be exact InstrumentId")
        require_decimal(self.maximum_quantity, "maximum_quantity")
        if self.maximum_quantity < 0:
            raise ValueError("maximum_quantity cannot be negative")


@dataclass(frozen=True)
class VirtualPositionTarget:
    strategy_id: str
    target_id: str
    source_decision_id: str
    strategy_version: str
    input_snapshot_id: str
    instrument: InstrumentId
    quantity: Decimal

    def __post_init__(self) -> None:
        for name in (
            "strategy_id",
            "target_id",
            "source_decision_id",
            "strategy_version",
            "input_snapshot_id",
        ):
            require_id(getattr(self, name), name)
        if type(self.instrument) is not InstrumentId:
            raise ValueError("instrument must be exact InstrumentId")
        require_decimal(self.quantity, "quantity")
        if self.quantity < 0:
            raise ValueError("virtual long-only quantity cannot be negative")


@dataclass(frozen=True)
class AccountPositionTarget:
    account_id: str
    allocation_id: str
    policy_version: str
    input_snapshot_id: str
    instrument: InstrumentId
    quantity: Decimal
    component_target_ids: tuple[str, ...]
    allocated_at: datetime

    def __post_init__(self) -> None:
        for name in ("account_id", "allocation_id", "policy_version", "input_snapshot_id"):
            require_id(getattr(self, name), name)
        if type(self.instrument) is not InstrumentId:
            raise ValueError("instrument must be exact InstrumentId")
        require_decimal(self.quantity, "quantity")
        if self.quantity < 0:
            raise ValueError("account long-only quantity cannot be negative")
        if not self.component_target_ids or len(set(self.component_target_ids)) != len(
            self.component_target_ids
        ):
            raise ValueError("component target IDs must be non-empty and unique")
        for target_id in self.component_target_ids:
            require_id(target_id, "component_target_id")
        require_utc(self.allocated_at, "allocated_at")


@dataclass(frozen=True)
class AllocationResult:
    account_targets: tuple[AccountPositionTarget, ...]
    virtual_targets: tuple[VirtualPositionTarget, ...]


def allocate_targets(
    *,
    account_id: str,
    allocation_id: str,
    policy_version: str,
    input_snapshot_id: str,
    targets: tuple[PositionTarget, ...],
    strategy_budgets: tuple[StrategyQuantityBudget, ...],
    instrument_limits: tuple[InstrumentQuantityLimit, ...],
    allocated_at: datetime,
) -> AllocationResult:
    """Aggregate explicit strategy targets without losing virtual ownership."""
    for name, value in (
        ("account_id", account_id),
        ("allocation_id", allocation_id),
        ("policy_version", policy_version),
        ("input_snapshot_id", input_snapshot_id),
    ):
        require_id(value, name)
    require_utc(allocated_at, "allocated_at")
    if not targets:
        raise ValueError("allocation requires at least one strategy target")
    if any(type(target) is not PositionTarget for target in targets):
        raise ValueError("targets must be exact PositionTarget values")

    budget_by_key = _unique_by_key(
        strategy_budgets, lambda item: (item.strategy_id, item.instrument), "strategy budget"
    )
    limit_by_instrument = _unique_by_key(
        instrument_limits, lambda item: item.instrument, "instrument limit"
    )
    target_by_key = _unique_by_key(
        targets, lambda item: (item.strategy_id, item.instrument), "strategy target"
    )
    if len({target.target_id for target in targets}) != len(targets):
        raise ValueError("target_id must be unique within an allocation")

    virtual: list[VirtualPositionTarget] = []
    for key, target in target_by_key.items():
        if target.unit is not TargetUnit.SHARES:
            raise ValueError("allocator supports share targets only")
        if target.target_at > allocated_at:
            raise ValueError("future strategy targets cannot be allocated")
        budget = budget_by_key.get(key)
        if budget is None:
            raise ValueError("every strategy target requires an explicit quantity budget")
        if target.quantity > budget.maximum_quantity:
            raise ValueError("strategy target exceeds its quantity budget")
        virtual.append(
            VirtualPositionTarget(
                target.strategy_id,
                target.target_id,
                target.source_decision_id,
                target.strategy_version,
                target.input_snapshot_id,
                target.instrument,
                target.quantity,
            )
        )

    virtual.sort(key=lambda item: (*_instrument_key(item.instrument), item.strategy_id))
    account_targets: list[AccountPositionTarget] = []
    for instrument, grouped in groupby(virtual, key=lambda item: item.instrument):
        components = tuple(grouped)
        limit = limit_by_instrument.get(instrument)
        if limit is None:
            raise ValueError("every allocated instrument requires an explicit account limit")
        quantity = sum((item.quantity for item in components), Decimal(0))
        if quantity > limit.maximum_quantity:
            raise ValueError("combined strategy targets exceed the account instrument limit")
        account_targets.append(
            AccountPositionTarget(
                account_id,
                allocation_id,
                policy_version,
                input_snapshot_id,
                instrument,
                quantity,
                tuple(item.target_id for item in components),
                allocated_at,
            )
        )
    return AllocationResult(tuple(account_targets), tuple(virtual))


def _unique_by_key(items: tuple, key, label: str) -> dict:
    result = {}
    for item in items:
        item_key = key(item)
        if item_key in result:
            raise ValueError(f"duplicate {label}")
        result[item_key] = item
    return result


def _instrument_key(instrument: InstrumentId) -> tuple[str, str, str]:
    return instrument.market, instrument.symbol, instrument.currency
