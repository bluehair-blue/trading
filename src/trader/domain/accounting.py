from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
import json

from trader.domain.broker_lifecycle import (
    BROKER_LIFECYCLE_FACT_TYPES,
    BrokerFillObserved,
    BrokerLifecycleFact,
    BrokerOrderOpened,
    fold_broker_order,
)
from trader.domain.models import (
    InstrumentId,
    Side,
    TradingEnvironment,
    canonical_share_quantity,
    require_decimal,
    require_id,
)


@dataclass(frozen=True)
class AccountingPosition:
    instrument: InstrumentId
    quantity: Decimal

    def __post_init__(self) -> None:
        if type(self.instrument) is not InstrumentId:
            raise ValueError("instrument must be exact InstrumentId")
        canonical_share_quantity(self.quantity, "quantity")
        if self.quantity < 0:
            raise ValueError("accounting position must be non-negative integral shares")


@dataclass(frozen=True)
class AccountingSeed:
    account_id: str
    environment: TradingEnvironment
    currency: str
    policy_version: str
    cash: Decimal
    positions: tuple[AccountingPosition, ...] = ()

    def __post_init__(self) -> None:
        for name in ("account_id", "currency", "policy_version"):
            require_id(getattr(self, name), name)
        if type(self.environment) is not TradingEnvironment:
            raise ValueError("environment must be TradingEnvironment")
        require_decimal(self.cash, "cash")
        if self.cash < 0:
            raise ValueError("accounting seed cash cannot be negative")
        if type(self.positions) is not tuple or any(
            type(position) is not AccountingPosition for position in self.positions
        ):
            raise ValueError("positions must be an exact tuple of AccountingPosition")
        for position in self.positions:
            position.__post_init__()
        instruments = tuple(position.instrument for position in self.positions)
        if len(instruments) != len(set(instruments)):
            raise ValueError("accounting seed positions must be unique")
        if self.currency != "USD" or any(
            position.instrument.currency != self.currency for position in self.positions
        ):
            raise ValueError("Dry accounting currently supports USD instruments only")

    def canonical_json(self) -> str:
        positions = sorted(
            self.positions,
            key=lambda item: (
                item.instrument.market,
                item.instrument.symbol,
                item.instrument.currency,
            ),
        )
        payload = {
            "account_id": self.account_id,
            "environment": self.environment.value,
            "currency": self.currency,
            "policy_version": self.policy_version,
            "cash": _canonical_decimal(self.cash),
            "positions": [
                {
                    "instrument": {
                        "market": position.instrument.market,
                        "symbol": position.instrument.symbol,
                        "currency": position.instrument.currency,
                    },
                    "quantity": str(int(position.quantity)),
                }
                for position in positions
            ],
        }
        return json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

    def fingerprint(self) -> str:
        return sha256(self.canonical_json().encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class AccountingProjection:
    account_id: str
    environment: TradingEnvironment
    currency: str
    policy_version: str
    cash: Decimal
    positions: tuple[AccountingPosition, ...]
    gross_traded_value: Decimal
    total_fees: Decimal


def _canonical_decimal(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def fold_accounting(
    seed: AccountingSeed,
    facts: tuple[BrokerLifecycleFact, ...],
) -> AccountingProjection:
    """Replay immutable broker fills into a long-only, same-currency Dry account."""
    if type(seed) is not AccountingSeed:
        raise TypeError("exact AccountingSeed required")
    seed.__post_init__()
    if type(facts) is not tuple or any(
        type(fact) not in BROKER_LIFECYCLE_FACT_TYPES for fact in facts
    ):
        raise TypeError("facts must be an exact tuple of broker lifecycle facts")

    grouped: dict[str, list[BrokerLifecycleFact]] = {}
    fact_ids: set[str] = set()
    execution_ids: set[str] = set()
    for fact in facts:
        if fact.fact_id in fact_ids:
            raise ValueError("broker lifecycle fact ID is duplicated")
        fact_ids.add(fact.fact_id)
        if type(fact) is BrokerFillObserved:
            if fact.broker_execution_id in execution_ids:
                raise ValueError("broker execution ID is duplicated")
            execution_ids.add(fact.broker_execution_id)
        grouped.setdefault(fact.client_order_id, []).append(fact)
    opened_by_order: dict[str, BrokerOrderOpened] = {}
    for client_order_id, order_facts in grouped.items():
        projection = fold_broker_order(tuple(order_facts))
        if (
            projection.broker_order_ref.account_id != seed.account_id
            or projection.broker_order_ref.environment is not seed.environment
        ):
            raise ValueError("broker lifecycle does not belong to the accounting seed")
        opened = order_facts[0]
        assert type(opened) is BrokerOrderOpened
        if opened.instrument.currency != seed.currency:
            raise ValueError("broker lifecycle currency does not match accounting seed")
        opened_by_order[client_order_id] = opened

    cash = seed.cash
    positions = {position.instrument: position.quantity for position in seed.positions}
    gross = Decimal(0)
    fees = Decimal(0)
    for fact in facts:
        if type(fact) is not BrokerFillObserved:
            continue
        opened = opened_by_order[fact.client_order_id]
        notional = fact.price * fact.quantity
        current = positions.get(opened.instrument, Decimal(0))
        if opened.side is Side.BUY:
            debit = notional + fact.fee
            if debit > cash:
                raise ValueError("broker fill would overdraw Dry account cash")
            cash -= debit
            positions[opened.instrument] = current + fact.quantity
        else:
            if fact.quantity > current:
                raise ValueError("broker fill would create a short Dry position")
            credit = notional - fact.fee
            if credit < 0:
                raise ValueError("sell fee cannot exceed proceeds")
            cash += credit
            positions[opened.instrument] = current - fact.quantity
        gross += notional
        fees += fact.fee

    projected_positions = tuple(
        AccountingPosition(instrument, quantity)
        for instrument, quantity in sorted(
            positions.items(),
            key=lambda item: (
                item[0].market,
                item[0].symbol,
                item[0].currency,
            ),
        )
        if quantity != 0
    )
    return AccountingProjection(
        seed.account_id,
        seed.environment,
        seed.currency,
        seed.policy_version,
        cash,
        projected_positions,
        gross,
        fees,
    )
