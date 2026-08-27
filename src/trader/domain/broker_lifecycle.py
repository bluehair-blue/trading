from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import re

from trader.domain.broker_observations import BrokerOrderRef
from trader.domain.models import (
    BrokerExecutionState,
    BrokerOrder,
    InstrumentId,
    Side,
    TradingEnvironment,
    canonical_share_quantity,
    require_decimal,
    require_id,
    require_utc,
)


_REASON_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,63}")


def _require_sequence(value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValueError("source_sequence must be a non-negative integer")


def _require_share_quantity(value: Decimal, name: str) -> None:
    canonical_share_quantity(value, name)
    if value <= 0:
        raise ValueError(f"{name} must be positive integral shares")


@dataclass(frozen=True, kw_only=True)
class _BrokerFact:
    fact_id: str
    client_order_id: str
    broker_order_ref: BrokerOrderRef
    source_api_id: str
    source_sequence: int
    occurred_at: datetime
    observed_at: datetime

    def __post_init__(self) -> None:
        for name in ("fact_id", "client_order_id", "source_api_id"):
            require_id(getattr(self, name), name)
        if type(self.broker_order_ref) is not BrokerOrderRef:
            raise ValueError("broker_order_ref must be exact BrokerOrderRef")
        _require_sequence(self.source_sequence)
        require_utc(self.occurred_at, "occurred_at")
        require_utc(self.observed_at, "observed_at")
        if self.observed_at < self.occurred_at:
            raise ValueError("broker fact cannot be observed before it occurs")


@dataclass(frozen=True, kw_only=True)
class BrokerOrderOpened(_BrokerFact):
    instrument: InstrumentId
    side: Side
    requested_quantity: Decimal

    def __post_init__(self) -> None:
        super().__post_init__()
        if type(self.instrument) is not InstrumentId:
            raise ValueError("instrument must be exact InstrumentId")
        if type(self.side) is not Side:
            raise ValueError("side must be Side")
        _require_share_quantity(self.requested_quantity, "requested_quantity")


@dataclass(frozen=True, kw_only=True)
class BrokerFillObserved(_BrokerFact):
    broker_execution_id: str
    quantity: Decimal
    price: Decimal
    fee: Decimal
    currency: str

    def __post_init__(self) -> None:
        super().__post_init__()
        require_id(self.broker_execution_id, "broker_execution_id")
        require_id(self.currency, "currency")
        _require_share_quantity(self.quantity, "quantity")
        require_decimal(self.price, "price")
        require_decimal(self.fee, "fee")
        if self.price <= 0 or self.fee < 0:
            raise ValueError("fill price must be positive and fee non-negative")


@dataclass(frozen=True, kw_only=True)
class BrokerOrderCanceled(_BrokerFact):
    quantity: Decimal

    def __post_init__(self) -> None:
        super().__post_init__()
        _require_share_quantity(self.quantity, "quantity")


@dataclass(frozen=True, kw_only=True)
class BrokerOrderExpired(_BrokerFact):
    quantity: Decimal

    def __post_init__(self) -> None:
        super().__post_init__()
        _require_share_quantity(self.quantity, "quantity")


@dataclass(frozen=True, kw_only=True)
class BrokerOrderRejected(_BrokerFact):
    quantity: Decimal
    reason_code: str

    def __post_init__(self) -> None:
        super().__post_init__()
        _require_share_quantity(self.quantity, "quantity")
        if type(self.reason_code) is not str or _REASON_CODE.fullmatch(self.reason_code) is None:
            raise ValueError("reason_code must be a bounded uppercase safe code")


BrokerLifecycleFact = (
    BrokerOrderOpened
    | BrokerFillObserved
    | BrokerOrderCanceled
    | BrokerOrderExpired
    | BrokerOrderRejected
)
BROKER_LIFECYCLE_FACT_TYPES = (
    BrokerOrderOpened,
    BrokerFillObserved,
    BrokerOrderCanceled,
    BrokerOrderExpired,
    BrokerOrderRejected,
)

_FACT_KIND = {
    BrokerOrderOpened: "ORDER_OPENED",
    BrokerFillObserved: "FILL_OBSERVED",
    BrokerOrderCanceled: "ORDER_CANCELED",
    BrokerOrderExpired: "ORDER_EXPIRED",
    BrokerOrderRejected: "ORDER_REJECTED",
}


def _canonical_decimal(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def canonical_broker_fact_payload(fact: BrokerLifecycleFact) -> dict[str, object]:
    if type(fact) not in BROKER_LIFECYCLE_FACT_TYPES:
        raise TypeError("exact typed broker lifecycle fact required")
    fact.__post_init__()
    ref = fact.broker_order_ref
    payload: dict[str, object] = {
        "kind": _FACT_KIND[type(fact)],
        "fact_id": fact.fact_id,
        "client_order_id": fact.client_order_id,
        "broker_order_ref": {
            "environment": ref.environment.value,
            "account_id": ref.account_id,
            "business_date": ref.business_date.isoformat(),
            "broker_order_id": ref.broker_order_id,
        },
        "source_api_id": fact.source_api_id,
        "source_sequence": fact.source_sequence,
        "occurred_at": fact.occurred_at.isoformat(),
        "observed_at": fact.observed_at.isoformat(),
    }
    if type(fact) is BrokerOrderOpened:
        payload.update(
            {
                "instrument": {
                    "market": fact.instrument.market,
                    "symbol": fact.instrument.symbol,
                    "currency": fact.instrument.currency,
                },
                "side": fact.side.value,
                "requested_quantity": str(int(fact.requested_quantity)),
            }
        )
    elif type(fact) is BrokerFillObserved:
        payload.update(
            {
                "broker_execution_id": fact.broker_execution_id,
                "quantity": str(int(fact.quantity)),
                "price": _canonical_decimal(fact.price),
                "fee": _canonical_decimal(fact.fee),
                "currency": fact.currency,
            }
        )
    elif type(fact) in (BrokerOrderCanceled, BrokerOrderExpired):
        payload["quantity"] = str(int(fact.quantity))
    else:
        assert type(fact) is BrokerOrderRejected
        payload.update(
            {"quantity": str(int(fact.quantity)), "reason_code": fact.reason_code}
        )
    return payload


def broker_fact_from_payload(payload: object) -> BrokerLifecycleFact:
    if type(payload) is not dict:
        raise ValueError("broker fact payload must be an exact object")
    common = {
        "kind",
        "fact_id",
        "client_order_id",
        "broker_order_ref",
        "source_api_id",
        "source_sequence",
        "occurred_at",
        "observed_at",
    }
    variants = {
        "ORDER_OPENED": common | {"instrument", "side", "requested_quantity"},
        "FILL_OBSERVED": common
        | {"broker_execution_id", "quantity", "price", "fee", "currency"},
        "ORDER_CANCELED": common | {"quantity"},
        "ORDER_EXPIRED": common | {"quantity"},
        "ORDER_REJECTED": common | {"quantity", "reason_code"},
    }
    kind = payload.get("kind")
    if kind not in variants or set(payload) != variants[kind]:
        raise ValueError("broker fact payload keys do not match its variant")
    ref_payload = payload["broker_order_ref"]
    if type(ref_payload) is not dict or set(ref_payload) != {
        "environment",
        "account_id",
        "business_date",
        "broker_order_id",
    }:
        raise ValueError("broker order reference payload is malformed")
    try:
        common_values = {
            "fact_id": payload["fact_id"],
            "client_order_id": payload["client_order_id"],
            "broker_order_ref": BrokerOrderRef(
                TradingEnvironment(ref_payload["environment"]),
                ref_payload["account_id"],
                date.fromisoformat(ref_payload["business_date"]),
                ref_payload["broker_order_id"],
            ),
            "source_api_id": payload["source_api_id"],
            "source_sequence": payload["source_sequence"],
            "occurred_at": datetime.fromisoformat(payload["occurred_at"]),
            "observed_at": datetime.fromisoformat(payload["observed_at"]),
        }
        if kind == "ORDER_OPENED":
            instrument = payload["instrument"]
            if type(instrument) is not dict or set(instrument) != {
                "market",
                "symbol",
                "currency",
            }:
                raise ValueError("broker fact instrument payload is malformed")
            fact: BrokerLifecycleFact = BrokerOrderOpened(
                **common_values,
                instrument=InstrumentId(
                    instrument["market"], instrument["symbol"], instrument["currency"]
                ),
                side=Side(payload["side"]),
                requested_quantity=Decimal(payload["requested_quantity"]),
            )
        elif kind == "FILL_OBSERVED":
            fact = BrokerFillObserved(
                **common_values,
                broker_execution_id=payload["broker_execution_id"],
                quantity=Decimal(payload["quantity"]),
                price=Decimal(payload["price"]),
                fee=Decimal(payload["fee"]),
                currency=payload["currency"],
            )
        elif kind == "ORDER_CANCELED":
            fact = BrokerOrderCanceled(
                **common_values, quantity=Decimal(payload["quantity"])
            )
        elif kind == "ORDER_EXPIRED":
            fact = BrokerOrderExpired(
                **common_values, quantity=Decimal(payload["quantity"])
            )
        else:
            fact = BrokerOrderRejected(
                **common_values,
                quantity=Decimal(payload["quantity"]),
                reason_code=payload["reason_code"],
            )
    except (
        KeyError,
        TypeError,
        ValueError,
        InvalidOperation,
    ) as error:
        raise ValueError("broker fact payload is malformed") from error
    if canonical_broker_fact_payload(fact) != payload:
        raise ValueError("broker fact payload is not canonical")
    return fact


@dataclass(frozen=True)
class BrokerLifecycleProjection:
    client_order_id: str
    broker_order_ref: BrokerOrderRef
    instrument: InstrumentId
    side: Side
    order: BrokerOrder
    fact_ids: tuple[str, ...]
    broker_execution_ids: tuple[str, ...]


def fold_broker_order(facts: tuple[BrokerLifecycleFact, ...]) -> BrokerLifecycleProjection:
    if not facts or type(facts[0]) is not BrokerOrderOpened:
        raise ValueError("broker lifecycle must start with exactly one OPEN fact")
    opened = facts[0]
    assert type(opened) is BrokerOrderOpened
    opened.__post_init__()
    requested = opened.requested_quantity
    filled = Decimal(0)
    open_quantity = requested
    canceled = Decimal(0)
    rejected = Decimal(0)
    expired = Decimal(0)
    state = BrokerExecutionState.OPEN
    terminal = False
    fact_ids: set[str] = set()
    execution_ids: set[str] = set()
    source_sequences: dict[str, int] = {}
    prior_occurred_at: datetime | None = None
    prior_observed_at: datetime | None = None

    for index, fact in enumerate(facts):
        if type(fact) not in BROKER_LIFECYCLE_FACT_TYPES:
            raise TypeError("exact typed broker lifecycle fact required")
        fact.__post_init__()
        if (
            fact.client_order_id != opened.client_order_id
            or fact.broker_order_ref != opened.broker_order_ref
        ):
            raise ValueError("broker lifecycle fact scope changed")
        if fact.fact_id in fact_ids:
            raise ValueError("broker lifecycle fact ID is duplicated")
        fact_ids.add(fact.fact_id)
        prior_sequence = source_sequences.get(fact.source_api_id)
        if prior_sequence is not None and fact.source_sequence <= prior_sequence:
            raise ValueError("broker source sequence did not increase")
        source_sequences[fact.source_api_id] = fact.source_sequence
        if prior_occurred_at is not None and fact.occurred_at < prior_occurred_at:
            raise ValueError("broker lifecycle occurrence time moved backwards")
        if prior_observed_at is not None and fact.observed_at < prior_observed_at:
            raise ValueError("broker lifecycle observation time moved backwards")
        prior_occurred_at = fact.occurred_at
        prior_observed_at = fact.observed_at
        if index == 0:
            continue
        if type(fact) is BrokerOrderOpened or terminal:
            raise ValueError("broker lifecycle cannot reopen or continue after terminal")
        if type(fact) is BrokerFillObserved:
            if fact.broker_execution_id in execution_ids:
                raise ValueError("broker execution ID is duplicated")
            execution_ids.add(fact.broker_execution_id)
            if fact.currency != opened.instrument.currency or fact.quantity > open_quantity:
                raise ValueError("fill does not match the open broker order")
            filled += fact.quantity
            open_quantity -= fact.quantity
            terminal = open_quantity == 0
            state = (
                BrokerExecutionState.FILLED
                if terminal
                else BrokerExecutionState.PARTIALLY_FILLED
            )
        elif type(fact) is BrokerOrderCanceled:
            if fact.quantity != open_quantity:
                raise ValueError("cancel must resolve the entire remaining quantity")
            canceled = fact.quantity
            open_quantity = Decimal(0)
            state = BrokerExecutionState.CANCELED
            terminal = True
        elif type(fact) is BrokerOrderExpired:
            if fact.quantity != open_quantity:
                raise ValueError("expiry must resolve the entire remaining quantity")
            expired = fact.quantity
            open_quantity = Decimal(0)
            state = BrokerExecutionState.EXPIRED
            terminal = True
        else:
            assert type(fact) is BrokerOrderRejected
            if filled != 0 or fact.quantity != open_quantity:
                raise ValueError("broker rejection is allowed only before any fill")
            rejected = fact.quantity
            open_quantity = Decimal(0)
            state = BrokerExecutionState.REJECTED
            terminal = True

    order = BrokerOrder(
        opened.broker_order_ref.broker_order_id,
        opened.broker_order_ref.account_id,
        requested,
        filled,
        open_quantity,
        canceled,
        rejected,
        expired,
        state,
    )
    return BrokerLifecycleProjection(
        opened.client_order_id,
        opened.broker_order_ref,
        opened.instrument,
        opened.side,
        order,
        tuple(fact.fact_id for fact in facts),
        tuple(
            fact.broker_execution_id
            for fact in facts
            if type(fact) is BrokerFillObserved
        ),
    )
