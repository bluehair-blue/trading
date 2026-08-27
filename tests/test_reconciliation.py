from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
import unittest

from trader.application.reconciliation import (
    ExpectedAccountState,
    ExpectedCashBalance,
    ExpectedDailyOrder,
    ExpectedPosition,
    ReconciliationStatus,
    reconcile,
)
from trader.domain.models import TradingEnvironment
from trader.domain.observations import (
    AccountObservation,
    AuthenticationObservation,
    CashBalance,
    CashObservation,
    DailyOrder,
    DailyOrdersObservation,
    ObservationQuality,
    Position,
    PositionsObservation,
    ResponseEvidence,
)


NOW = datetime(2026, 8, 27, tzinfo=timezone.utc)
EVIDENCE = (
    ResponseEvidence(
        sha256(b"raw").hexdigest(), sha256(b"").hexdigest(), NOW, NOW
    ),
)


def observation() -> AccountObservation:
    quality = ObservationQuality.COMPLETE
    authentication = AuthenticationObservation(
        quality, NOW, NOW, EVIDENCE, "20261130235959"
    )
    positions = PositionsObservation(
        quality,
        NOW,
        NOW,
        EVIDENCE,
        (),
        (Position("AAPL", "USD", Decimal(2), Decimal(2), Decimal(2)),),
    )
    cash = CashObservation(
        quality,
        NOW,
        NOW,
        EVIDENCE,
        (),
        Decimal(1000),
        Decimal(0),
        Decimal(0),
        (CashBalance("USD", Decimal(10), Decimal(10), Decimal(10)),),
    )
    orders = DailyOrdersObservation(
        quality,
        NOW,
        NOW,
        EVIDENCE,
        (),
        (
            DailyOrder(
                "broker-1",
                "AAPL",
                "USD",
                "매수",
                Decimal(2),
                Decimal(2),
                Decimal(0),
                Decimal(0),
                Decimal(0),
                Decimal(200),
                Decimal(199),
                "체결완료",
            ),
        ),
    )
    return AccountObservation(
        "account",
        TradingEnvironment.PAPER,
        quality,
        authentication,
        positions,
        cash,
        orders,
        NOW,
        NOW,
    )


def expected() -> ExpectedAccountState:
    return ExpectedAccountState(
        "account",
        TradingEnvironment.PAPER,
        (ExpectedPosition("AAPL", "USD", Decimal(2)),),
        (ExpectedCashBalance("USD", Decimal(10), Decimal(10)),),
        (
            ExpectedDailyOrder(
                "broker-1",
                "AAPL",
                "USD",
                Decimal(2),
                Decimal(2),
                Decimal(0),
            ),
        ),
    )


class ReconciliationTests(unittest.TestCase):
    def test_exact_ids_and_positions_match(self) -> None:
        self.assertEqual(reconcile(expected(), observation()).status, ReconciliationStatus.MATCHED)

    def test_reports_exact_position_and_order_differences(self) -> None:
        state = ExpectedAccountState(
            "account",
            TradingEnvironment.PAPER,
            (ExpectedPosition("AAPL", "USD", Decimal(3)),),
            (ExpectedCashBalance("USD", Decimal(10), Decimal(10)),),
            (
                ExpectedDailyOrder(
                    "broker-missing",
                    "AAPL",
                    "USD",
                    Decimal(2),
                    Decimal(2),
                    Decimal(0),
                ),
            ),
        )

        report = reconcile(state, observation())

        self.assertEqual(report.status, ReconciliationStatus.MISMATCH)
        self.assertEqual(report.position_differences[0].observed, Decimal(2))
        self.assertEqual(report.missing_broker_order_ids, frozenset({"broker-missing"}))
        self.assertEqual(report.unexpected_broker_order_ids, frozenset({"broker-1"}))

    def test_incomplete_observation_never_reconciles(self) -> None:
        observed = observation()
        incomplete_positions = PositionsObservation(
            ObservationQuality.INCOMPLETE, NOW, NOW, EVIDENCE, ("SCHEMA",)
        )
        incomplete = replace(
            observed,
            quality=ObservationQuality.INCOMPLETE,
            positions=incomplete_positions,
            error_codes=("SCHEMA",),
        )
        report = reconcile(expected(), incomplete)
        self.assertEqual(report.status, ReconciliationStatus.INCOMPLETE)

    def test_account_window_defect_blocks_reconciliation_with_complete_components(self) -> None:
        observed = replace(
            observation(),
            quality=ObservationQuality.INCOMPLETE,
            error_codes=("COMPONENT_WINDOW_EXCEEDED",),
        )
        self.assertFalse(observed.is_reconciliation_safe)
        self.assertEqual(
            reconcile(expected(), observed).status,
            ReconciliationStatus.INCOMPLETE,
        )

    def test_unknown_or_manual_activity_is_never_automatically_linked(self) -> None:
        unknown = replace(expected(), unresolved_client_order_ids=frozenset({"client-unknown"}))
        manual = replace(expected(), manual_activity_present=True)

        self.assertEqual(reconcile(unknown, observation()).status, ReconciliationStatus.AMBIGUOUS)
        self.assertEqual(reconcile(manual, observation()).status, ReconciliationStatus.AMBIGUOUS)

    def test_environment_mismatch_is_incomplete(self) -> None:
        report = reconcile(
            replace(expected(), environment=TradingEnvironment.LIVE), observation()
        )
        self.assertEqual(report.status, ReconciliationStatus.INCOMPLETE)
        self.assertEqual(report.reason_codes, ("ENVIRONMENT_MISMATCH",))

    def test_cash_balance_or_buying_power_change_cannot_match(self) -> None:
        for field_name in ("balance", "buying_power"):
            changed_cash = replace(
                expected().cash_balances[0], **{field_name: Decimal(9)}
            )
            report = reconcile(
                replace(expected(), cash_balances=(changed_cash,)), observation()
            )
            with self.subTest(field_name):
                self.assertEqual(report.status, ReconciliationStatus.MISMATCH)
                self.assertEqual(report.cash_differences[0].currency, "USD")

    def test_extra_zero_position_or_cash_currency_is_still_a_mismatch(self) -> None:
        observed = observation()
        extra_position = Position("ZERO", "USD", Decimal(0), Decimal(0), Decimal(0))
        with_position = replace(
            observed,
            positions=replace(
                observed.positions,
                positions=observed.positions.positions + (extra_position,),
            ),
        )
        self.assertEqual(
            reconcile(expected(), with_position).status,
            ReconciliationStatus.MISMATCH,
        )

        extra_cash = CashBalance("EUR", Decimal(0), Decimal(0), Decimal(0))
        with_cash = replace(
            observed,
            cash=replace(
                observed.cash,
                balances=observed.cash.balances + (extra_cash,),
            ),
        )
        self.assertEqual(
            reconcile(expected(), with_cash).status,
            ReconciliationStatus.MISMATCH,
        )

    def test_requested_filled_and_open_order_fields_are_exact(self) -> None:
        for field_name, changes in (
            ("requested_quantity", {"requested_quantity": Decimal(3)}),
            ("filled_quantity", {"filled_quantity": Decimal(1)}),
            (
                "open_quantity",
                {"requested_quantity": Decimal(3), "open_quantity": Decimal(1)},
            ),
        ):
            changed_order = replace(expected().daily_orders[0], **changes)
            report = reconcile(
                replace(expected(), daily_orders=(changed_order,)), observation()
            )
            with self.subTest(field_name):
                self.assertEqual(report.status, ReconciliationStatus.MISMATCH)
                self.assertEqual(report.order_field_mismatch_ids, frozenset({"broker-1"}))

    def test_duplicate_observations_are_ambiguous(self) -> None:
        observed = observation()
        duplicate_positions = replace(
            observed.positions,
            positions=observed.positions.positions + observed.positions.positions,
        )
        report = reconcile(expected(), replace(observed, positions=duplicate_positions))
        self.assertEqual(report.status, ReconciliationStatus.AMBIGUOUS)


if __name__ == "__main__":
    unittest.main()
