from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
import re

from trader.domain.models import TradingEnvironment


_SHA256 = re.compile(r"[0-9a-f]{64}")


def _require_text(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _require_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{name} must be timezone-aware UTC")


def _require_decimal(value: Decimal, name: str, *, nonnegative: bool = False) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")
    if nonnegative and value < 0:
        raise ValueError(f"{name} must be non-negative")


class ObservationQuality(StrEnum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"


@dataclass(frozen=True)
class ResponseEvidence:
    raw_sha256: str
    cursor_sha256: str
    request_started_at: datetime
    response_completed_at: datetime

    def __post_init__(self) -> None:
        if _SHA256.fullmatch(self.raw_sha256) is None:
            raise ValueError("raw_sha256 must be a lowercase SHA256 digest")
        if _SHA256.fullmatch(self.cursor_sha256) is None:
            raise ValueError("cursor_sha256 must be a lowercase SHA256 digest")
        _require_utc(self.request_started_at, "request_started_at")
        _require_utc(self.response_completed_at, "response_completed_at")
        if self.response_completed_at < self.request_started_at:
            raise ValueError("response cannot complete before its request starts")


@dataclass(frozen=True)
class Position:
    symbol: str
    currency: str
    quantity: Decimal
    available_quantity: Decimal
    sell_allowed_quantity: Decimal

    def __post_init__(self) -> None:
        _require_text(self.symbol, "symbol")
        _require_text(self.currency, "currency")
        for name in ("quantity", "available_quantity", "sell_allowed_quantity"):
            value = getattr(self, name)
            _require_decimal(value, name, nonnegative=True)
            if value != value.to_integral_value():
                raise ValueError(f"{name} must be integral shares")


@dataclass(frozen=True)
class CashBalance:
    currency: str
    cash: Decimal
    payment_available: Decimal
    order_available: Decimal

    def __post_init__(self) -> None:
        _require_text(self.currency, "currency")
        for name in ("cash", "payment_available", "order_available"):
            _require_decimal(getattr(self, name), name)


@dataclass(frozen=True)
class DailyOrder:
    broker_order_id: str
    symbol: str
    currency: str
    side_name: str
    ordered_quantity: Decimal
    filled_quantity: Decimal
    modified_quantity: Decimal
    canceled_quantity: Decimal
    remaining_quantity: Decimal
    order_price: Decimal
    fill_price: Decimal
    status_name: str

    def __post_init__(self) -> None:
        for name in ("broker_order_id", "symbol", "currency", "side_name", "status_name"):
            _require_text(getattr(self, name), name)
        for name in (
            "ordered_quantity",
            "filled_quantity",
            "modified_quantity",
            "canceled_quantity",
            "remaining_quantity",
            "order_price",
            "fill_price",
        ):
            value = getattr(self, name)
            _require_decimal(value, name, nonnegative=True)
            if name.endswith("quantity") and value != value.to_integral_value():
                raise ValueError(f"{name} must be integral shares")


@dataclass(frozen=True)
class ComponentObservation:
    quality: ObservationQuality
    started_at: datetime
    completed_at: datetime
    evidence: tuple[ResponseEvidence, ...]
    error_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if type(self.quality) is not ObservationQuality:
            raise ValueError("quality must be ObservationQuality")
        _require_window(self.started_at, self.completed_at)
        _require_evidence_window(self.started_at, self.completed_at, self.evidence)
        if self.quality is ObservationQuality.COMPLETE and not self.evidence:
            raise ValueError("complete component requires response evidence")
        if self.quality is ObservationQuality.COMPLETE and self.error_codes:
            raise ValueError("complete component cannot contain error codes")
        if self.quality is ObservationQuality.INCOMPLETE and not self.error_codes:
            raise ValueError("incomplete component requires an error code")
        for code in self.error_codes:
            if re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", code) is None:
                raise ValueError("error codes must be bounded uppercase safe codes")

    @property
    def observed_at(self) -> datetime:
        return self.completed_at

    @property
    def is_reconciliation_safe(self) -> bool:
        return self.quality is ObservationQuality.COMPLETE


@dataclass(frozen=True)
class PositionsObservation(ComponentObservation):
    positions: tuple[Position, ...] = ()

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.quality is ObservationQuality.INCOMPLETE and self.positions:
            raise ValueError("incomplete positions cannot expose partial records")


@dataclass(frozen=True)
class CashObservation(ComponentObservation):
    krw_cash: Decimal | None = None
    unsettled: Decimal | None = None
    other_loans: Decimal | None = None
    balances: tuple[CashBalance, ...] = ()

    def __post_init__(self) -> None:
        super().__post_init__()
        for name in ("krw_cash", "unsettled", "other_loans"):
            value = getattr(self, name)
            if value is not None:
                _require_decimal(value, name)
        if self.quality is ObservationQuality.INCOMPLETE and (
            any(value is not None for value in (self.krw_cash, self.unsettled, self.other_loans))
            or self.balances
        ):
            raise ValueError("incomplete cash cannot expose partial records")


@dataclass(frozen=True)
class DailyOrdersObservation(ComponentObservation):
    orders: tuple[DailyOrder, ...] = ()

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.quality is ObservationQuality.INCOMPLETE and self.orders:
            raise ValueError("incomplete orders cannot expose partial records")


@dataclass(frozen=True)
class AuthenticationObservation:
    quality: ObservationQuality
    started_at: datetime
    completed_at: datetime
    evidence: tuple[ResponseEvidence, ...]
    expires_dt: str | None
    error_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        ComponentObservation(
            self.quality,
            self.started_at,
            self.completed_at,
            self.evidence,
            self.error_codes,
        )
        if self.quality is ObservationQuality.COMPLETE:
            _require_text(self.expires_dt, "expires_dt")
        elif self.expires_dt is not None:
            raise ValueError("incomplete authentication cannot carry expires_dt")

    @property
    def observed_at(self) -> datetime:
        return self.completed_at

    @property
    def is_reconciliation_safe(self) -> bool:
        return self.quality is ObservationQuality.COMPLETE


@dataclass(frozen=True)
class AccountObservation:
    account_id: str
    environment: TradingEnvironment
    quality: ObservationQuality
    authentication: AuthenticationObservation
    positions: PositionsObservation
    cash: CashObservation
    daily_orders: DailyOrdersObservation
    started_at: datetime
    completed_at: datetime
    error_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.account_id, "account_id")
        if type(self.environment) is not TradingEnvironment:
            raise ValueError("environment must be TradingEnvironment")
        _require_window(self.started_at, self.completed_at)
        if type(self.quality) is not ObservationQuality:
            raise ValueError("quality must be ObservationQuality")
        complete = all(
            item.quality is ObservationQuality.COMPLETE
            for item in (self.authentication, self.positions, self.cash, self.daily_orders)
        )
        if self.quality is ObservationQuality.COMPLETE and not complete:
            raise ValueError("complete account requires every component to be complete")
        if self.quality is ObservationQuality.COMPLETE and self.error_codes:
            raise ValueError("complete account cannot contain error codes")
        if self.quality is ObservationQuality.INCOMPLETE and not self.error_codes:
            raise ValueError("incomplete account requires an error code")
        windows = (self.authentication, self.positions, self.cash, self.daily_orders)
        if self.started_at != min(item.started_at for item in windows):
            raise ValueError("account started_at must be the earliest component start")
        if self.completed_at != max(item.completed_at for item in windows):
            raise ValueError("account completed_at must be the latest component completion")

    @property
    def captured_at(self) -> datetime:
        return self.completed_at

    @property
    def is_reconciliation_safe(self) -> bool:
        return self.quality is ObservationQuality.COMPLETE and all(
            item.is_reconciliation_safe
            for item in (self.authentication, self.positions, self.cash, self.daily_orders)
        )


def _require_window(started_at: datetime, completed_at: datetime) -> None:
    _require_utc(started_at, "started_at")
    _require_utc(completed_at, "completed_at")
    if completed_at < started_at:
        raise ValueError("observation cannot complete before it starts")


def _require_evidence_window(
    started_at: datetime,
    completed_at: datetime,
    evidence: tuple[ResponseEvidence, ...],
) -> None:
    if not evidence:
        return
    if evidence[0].request_started_at != started_at:
        raise ValueError("component start must match its first request")
    if evidence[-1].response_completed_at != completed_at:
        raise ValueError("component completion must match its final response")
    previous_completion = started_at
    for item in evidence:
        if item.request_started_at < previous_completion:
            raise ValueError("component evidence windows must be ordered")
        previous_completion = item.response_completed_at
