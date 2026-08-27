from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from trader.domain.models import TradingEnvironment
from trader.domain.observations import AccountObservation


class ReconciliationStatus(StrEnum):
    MATCHED = "MATCHED"
    MISMATCH = "MISMATCH"
    INCOMPLETE = "INCOMPLETE"
    AMBIGUOUS = "AMBIGUOUS"


@dataclass(frozen=True)
class ExpectedPosition:
    symbol: str
    currency: str
    quantity: Decimal

    def __post_init__(self) -> None:
        if (
            not isinstance(self.symbol, str)
            or not self.symbol.strip()
            or not isinstance(self.currency, str)
            or not self.currency.strip()
        ):
            raise ValueError("expected position identity must be non-empty")
        if not isinstance(self.quantity, Decimal) or not self.quantity.is_finite():
            raise ValueError("expected quantity must be a finite Decimal")
        if self.quantity < 0:
            raise ValueError("expected quantity must be non-negative")


@dataclass(frozen=True)
class ExpectedCashBalance:
    currency: str
    balance: Decimal
    buying_power: Decimal

    def __post_init__(self) -> None:
        _require_text(self.currency, "currency")
        _require_finite(self.balance, "balance")
        _require_finite(self.buying_power, "buying_power")


@dataclass(frozen=True)
class ExpectedDailyOrder:
    broker_order_id: str
    symbol: str
    currency: str
    requested_quantity: Decimal
    filled_quantity: Decimal
    open_quantity: Decimal

    def __post_init__(self) -> None:
        for name in ("broker_order_id", "symbol", "currency"):
            _require_text(getattr(self, name), name)
        for name in ("requested_quantity", "filled_quantity", "open_quantity"):
            value = getattr(self, name)
            _require_finite(value, name)
            if value < 0 or value != value.to_integral_value():
                raise ValueError(f"{name} must be non-negative integral shares")
        if self.filled_quantity + self.open_quantity > self.requested_quantity:
            raise ValueError("filled plus open quantity cannot exceed requested quantity")


@dataclass(frozen=True)
class ExpectedAccountState:
    account_id: str
    environment: TradingEnvironment
    positions: tuple[ExpectedPosition, ...]
    cash_balances: tuple[ExpectedCashBalance, ...]
    daily_orders: tuple[ExpectedDailyOrder, ...]
    unresolved_client_order_ids: frozenset[str] = frozenset()
    manual_activity_present: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.account_id, str) or not self.account_id.strip():
            raise ValueError("account_id must be non-empty")
        if type(self.environment) is not TradingEnvironment:
            raise ValueError("environment must be TradingEnvironment")
        if any(
            not isinstance(value, str) or not value.strip()
            for value in self.unresolved_client_order_ids
        ):
            raise ValueError("order identifiers must be non-empty strings")


@dataclass(frozen=True)
class PositionDifference:
    symbol: str
    currency: str
    expected: Decimal
    observed: Decimal


@dataclass(frozen=True)
class CashDifference:
    currency: str
    expected_balance: Decimal
    observed_balance: Decimal
    expected_buying_power: Decimal
    observed_buying_power: Decimal


@dataclass(frozen=True)
class ReconciliationReport:
    status: ReconciliationStatus
    position_differences: tuple[PositionDifference, ...] = ()
    missing_broker_order_ids: frozenset[str] = frozenset()
    unexpected_broker_order_ids: frozenset[str] = frozenset()
    cash_differences: tuple[CashDifference, ...] = ()
    order_field_mismatch_ids: frozenset[str] = frozenset()
    reason_codes: tuple[str, ...] = ()


def reconcile(
    expected: ExpectedAccountState, observed: AccountObservation
) -> ReconciliationReport:
    if expected.account_id != observed.account_id:
        return ReconciliationReport(
            ReconciliationStatus.INCOMPLETE, reason_codes=("ACCOUNT_ID_MISMATCH",)
        )
    if expected.environment is not observed.environment:
        return ReconciliationReport(
            ReconciliationStatus.INCOMPLETE,
            reason_codes=("ENVIRONMENT_MISMATCH",),
        )
    if not observed.is_reconciliation_safe:
        return ReconciliationReport(
            ReconciliationStatus.INCOMPLETE, reason_codes=("OBSERVATION_INCOMPLETE",)
        )
    if expected.unresolved_client_order_ids:
        return ReconciliationReport(
            ReconciliationStatus.AMBIGUOUS, reason_codes=("SUBMITTED_UNKNOWN",)
        )
    if expected.manual_activity_present:
        return ReconciliationReport(
            ReconciliationStatus.AMBIGUOUS, reason_codes=("MANUAL_ACTIVITY",)
        )

    expected_positions = _unique_positions(
        (item.symbol, item.currency, item.quantity) for item in expected.positions
    )
    observed_positions = _unique_positions(
        (item.symbol, item.currency, item.quantity) for item in observed.positions.positions
    )
    expected_cash = _unique_cash(
        (item.currency, item.balance, item.buying_power)
        for item in expected.cash_balances
    )
    observed_cash = _unique_cash(
        (item.currency, item.cash, item.order_available)
        for item in observed.cash.balances
    )
    expected_orders = _unique_orders(
        (
            item.broker_order_id,
            item.symbol,
            item.currency,
            item.requested_quantity,
            item.filled_quantity,
            item.open_quantity,
        )
        for item in expected.daily_orders
    )
    observed_orders = _unique_orders(
        (
            item.broker_order_id,
            item.symbol,
            item.currency,
            item.ordered_quantity,
            item.filled_quantity,
            item.remaining_quantity,
        )
        for item in observed.daily_orders.orders
    )
    if expected_positions is None or observed_positions is None:
        return ReconciliationReport(
            ReconciliationStatus.AMBIGUOUS, reason_codes=("DUPLICATE_POSITION",)
        )
    if expected_cash is None or observed_cash is None:
        return ReconciliationReport(
            ReconciliationStatus.AMBIGUOUS, reason_codes=("DUPLICATE_CURRENCY",)
        )
    if expected_orders is None or observed_orders is None:
        return ReconciliationReport(
            ReconciliationStatus.AMBIGUOUS, reason_codes=("DUPLICATE_BROKER_ORDER",)
        )

    differences = _position_differences(expected_positions, observed_positions)
    cash_differences = _cash_differences(expected_cash, observed_cash)
    expected_order_ids = frozenset(expected_orders)
    actual_orders = frozenset(observed_orders)
    missing = expected_order_ids - actual_orders
    unexpected = actual_orders - expected_order_ids
    order_field_mismatches = frozenset(
        order_id
        for order_id in expected_order_ids & actual_orders
        if expected_orders[order_id] != observed_orders[order_id]
    )
    status = (
        ReconciliationStatus.MATCHED
        if not differences
        and not cash_differences
        and not missing
        and not unexpected
        and not order_field_mismatches
        else ReconciliationStatus.MISMATCH
    )
    return ReconciliationReport(
        status,
        differences,
        missing,
        unexpected,
        cash_differences,
        order_field_mismatches,
    )


def _unique_positions(
    positions: Iterable[tuple[str, str, Decimal]],
) -> dict[tuple[str, str], Decimal] | None:
    result: dict[tuple[str, str], Decimal] = {}
    for symbol, currency, quantity in positions:
        key = (symbol, currency)
        if key in result:
            return None
        result[key] = quantity
    return result


def _unique_cash(
    balances: Iterable[tuple[str, Decimal, Decimal]],
) -> dict[str, tuple[Decimal, Decimal]] | None:
    result: dict[str, tuple[Decimal, Decimal]] = {}
    for currency, balance, buying_power in balances:
        if currency in result:
            return None
        result[currency] = (balance, buying_power)
    return result


def _unique_orders(
    orders: Iterable[tuple[str, str, str, Decimal, Decimal, Decimal]],
) -> dict[str, tuple[str, str, Decimal, Decimal, Decimal]] | None:
    result: dict[str, tuple[str, str, Decimal, Decimal, Decimal]] = {}
    for order_id, symbol, currency, requested, filled, open_quantity in orders:
        if order_id in result:
            return None
        result[order_id] = (symbol, currency, requested, filled, open_quantity)
    return result


def _position_differences(
    expected: dict[tuple[str, str], Decimal],
    observed: dict[tuple[str, str], Decimal],
) -> tuple[PositionDifference, ...]:
    differences = []
    for symbol, currency in sorted(expected.keys() | observed.keys()):
        expected_quantity = expected.get((symbol, currency), Decimal(0))
        observed_quantity = observed.get((symbol, currency), Decimal(0))
        if (
            (symbol, currency) not in expected
            or (symbol, currency) not in observed
            or expected_quantity != observed_quantity
        ):
            differences.append(
                PositionDifference(
                    symbol, currency, expected_quantity, observed_quantity
                )
            )
    return tuple(differences)


def _cash_differences(
    expected: dict[str, tuple[Decimal, Decimal]],
    observed: dict[str, tuple[Decimal, Decimal]],
) -> tuple[CashDifference, ...]:
    differences = []
    for currency in sorted(expected.keys() | observed.keys()):
        expected_values = expected.get(currency, (Decimal(0), Decimal(0)))
        observed_values = observed.get(currency, (Decimal(0), Decimal(0)))
        if (
            currency not in expected
            or currency not in observed
            or expected_values != observed_values
        ):
            differences.append(
                CashDifference(
                    currency,
                    expected_values[0],
                    observed_values[0],
                    expected_values[1],
                    observed_values[1],
                )
            )
    return tuple(differences)


def _require_text(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty")


def _require_finite(value: Decimal, name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")
