from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
import json
import unittest

from trader.application.backtest import (
    BacktestFillRecord,
    BacktestOrderRecord,
    BacktestOutput,
)
from trader.domain.accounting import AccountingPosition, AccountingSeed, fold_accounting
from trader.domain.broker_lifecycle import BrokerFillObserved, BrokerOrderOpened
from trader.domain.broker_observations import BrokerOrderRef
from trader.domain.models import (
    BrokerExecutionState,
    InstrumentId,
    Side,
    TradingEnvironment,
)
from trader.domain.performance import (
    EvaluationStatus,
    IncompleteReasonCode,
    MarkUnavailable,
    PerformanceMark,
    ValuationCheckpoint,
    project_performance,
)


NOW = datetime(2026, 8, 27, 14, tzinfo=timezone.utc)
APPLE = InstrumentId("NASDAQ", "AAPL", "USD")
ENV = TradingEnvironment.SIMULATED


def lifecycle(
    client_id: str,
    side: Side,
    quantity: int,
    price: str,
    fee: str,
    second: int,
):
    occurred_at = NOW + timedelta(seconds=second)
    ref = BrokerOrderRef(ENV, "account-1", NOW.date(), f"broker-{client_id}")
    common = {
        "client_order_id": client_id,
        "broker_order_ref": ref,
        "source_api_id": "simulated",
    }
    return (
        BrokerOrderOpened(
            **common,
            fact_id=f"open-{client_id}",
            source_sequence=0,
            occurred_at=occurred_at,
            observed_at=occurred_at,
            instrument=APPLE,
            side=side,
            requested_quantity=Decimal(quantity),
        ),
        BrokerFillObserved(
            **common,
            fact_id=f"fill-{client_id}",
            source_sequence=1,
            occurred_at=occurred_at + timedelta(microseconds=1),
            observed_at=occurred_at + timedelta(microseconds=1),
            broker_execution_id=f"execution-{client_id}",
            quantity=Decimal(quantity),
            price=Decimal(price),
            fee=Decimal(fee),
            currency="USD",
        ),
    )


def seed(cash: str = "1000") -> AccountingSeed:
    return AccountingSeed("account-1", ENV, "USD", "accounting-v1", Decimal(cash))


def checkpoint(price: str | None = None, seconds: int = 60) -> ValuationCheckpoint:
    marks = () if price is None else (PerformanceMark(APPLE, Decimal(price)),)
    return ValuationCheckpoint(NOW + timedelta(seconds=seconds), marks)


class PerformanceTests(unittest.TestCase):
    def test_buy_only_includes_buy_fee_in_basis(self) -> None:
        projection = project_performance(
            seed(), lifecycle("buy", Side.BUY, 2, "100", "2", 1), (checkpoint("110"),)
        )
        position = projection.snapshots[-1].positions[0]
        self.assertEqual(projection.evaluation_status, EvaluationStatus.COMPLETE)
        self.assertEqual(position.average_cost, Decimal("101"))
        self.assertEqual(position.cost_basis, Decimal("202"))
        self.assertEqual(projection.ending_equity, Decimal("1018"))
        self.assertEqual(projection.realized_pnl, Decimal(0))
        self.assertEqual(projection.unrealized_pnl, Decimal("18"))
        self.assertEqual(projection.net_pnl, Decimal("18"))
        self.assertEqual(projection.cumulative_return, Decimal("0.018"))
        self.assertEqual(projection.evidence_use, "REFERENCE_ONLY")
        self.assertEqual(json.loads(projection.canonical_json())["evidence_use"], "REFERENCE_ONLY")

    def test_multiple_buys_use_weighted_average_and_canonical_json(self) -> None:
        facts = lifecycle("first", Side.BUY, 2, "100", "2", 1) + lifecycle(
            "second", Side.BUY, 1, "130", "1", 2
        )
        projection = project_performance(seed(), facts, (checkpoint("120"),))
        position = projection.snapshots[0].positions[0]
        self.assertEqual(position.cost_basis, Decimal("333"))
        self.assertEqual(position.average_cost, Decimal("111"))
        self.assertEqual(projection.unrealized_pnl, Decimal("27"))
        payload = json.loads(projection.canonical_json())
        self.assertEqual(payload["ending_equity"], "1027")
        self.assertEqual(payload["snapshots"][0]["positions"][0]["quantity"], "3")
        self.assertEqual(projection.canonical_json(), projection.canonical_json())

    def test_canonical_output_ignores_ambient_decimal_precision(self) -> None:
        facts = lifecycle("buy", Side.BUY, 3, "100", "1", 1) + lifecycle(
            "sell", Side.SELL, 1, "110", "0", 2
        )
        outputs = []
        for precision in (10, 28, 50):
            with localcontext() as context:
                context.prec = precision
                outputs.append(
                    project_performance(
                        seed(), facts, (checkpoint("105"),)
                    ).canonical_json()
                )
        self.assertEqual(outputs, [outputs[0]] * 3)

    def test_partial_sell_reduces_proceeds_by_fee(self) -> None:
        facts = lifecycle("buy", Side.BUY, 4, "100", "4", 1) + lifecycle(
            "sell", Side.SELL, 1, "120", "2", 2
        )
        projection = project_performance(seed(), facts, (checkpoint("110"),))
        position = projection.snapshots[0].positions[0]
        self.assertEqual(position.average_cost, Decimal("101"))
        self.assertEqual(position.cost_basis, Decimal("303"))
        self.assertEqual(projection.realized_pnl, Decimal("17"))
        self.assertEqual(projection.unrealized_pnl, Decimal("27"))
        self.assertEqual(projection.net_pnl, Decimal("44"))
        self.assertEqual(projection.gross_traded_value, Decimal("520"))
        self.assertEqual(projection.total_fees, Decimal("6"))
        self.assertEqual(projection.fills, 2)

    def test_full_exit_needs_no_mark(self) -> None:
        facts = lifecycle("buy", Side.BUY, 2, "100", "2", 1) + lifecycle(
            "sell", Side.SELL, 2, "120", "2", 2
        )
        projection = project_performance(seed(), facts, (checkpoint(),))
        self.assertEqual(projection.evaluation_status, EvaluationStatus.COMPLETE)
        self.assertEqual(projection.ending_equity, Decimal("1036"))
        self.assertEqual(projection.realized_pnl, Decimal("36"))
        self.assertEqual(projection.unrealized_pnl, Decimal(0))
        self.assertEqual(projection.net_pnl, Decimal("36"))
        self.assertEqual(projection.snapshots[0].positions, ())

    def test_no_trade_has_zero_return_drawdown_and_turnover(self) -> None:
        projection = project_performance(seed(), (), (checkpoint(),))
        self.assertEqual(projection.ending_equity, Decimal("1000"))
        self.assertEqual(projection.cumulative_return, Decimal(0))
        self.assertEqual(projection.maximum_session_close_drawdown, Decimal(0))
        self.assertEqual(projection.gross_turnover, Decimal(0))
        self.assertEqual(projection.fills, 0)

    def test_session_drawdown_and_turnover_use_complete_equity_observations(self) -> None:
        facts = lifecycle("buy", Side.BUY, 1, "100", "0", 1)
        projection = project_performance(
            seed(),
            facts,
            (
                checkpoint("120", 10),
                checkpoint("90", 20),
                replace(checkpoint("110", 30), is_session_close=False),
            ),
        )
        self.assertEqual(
            projection.maximum_session_close_drawdown,
            Decimal("0.02941176470588235294117647058823529"),
        )
        self.assertEqual(
            projection.gross_turnover,
            Decimal("0.09950248756218905472636815920398010"),
        )

    def test_missing_or_stale_mark_is_incomplete_and_aggregates_are_null(self) -> None:
        facts = lifecycle("buy", Side.BUY, 1, "100", "0", 1)
        missing = project_performance(seed(), facts, (checkpoint(),))
        stale = project_performance(
            seed(),
            facts,
            (
                ValuationCheckpoint(
                    NOW + timedelta(seconds=60),
                    unavailable_marks=(
                        MarkUnavailable(APPLE, IncompleteReasonCode.STALE_MARK),
                    ),
                ),
            ),
        )
        for projection, reason in (
            (missing, IncompleteReasonCode.MISSING_MARK),
            (stale, IncompleteReasonCode.STALE_MARK),
        ):
            self.assertEqual(projection.evaluation_status, EvaluationStatus.INCOMPLETE)
            self.assertEqual(projection.incomplete_reasons[0].code, reason)
            self.assertIsNone(projection.ending_equity)
            self.assertIsNone(projection.cumulative_return)
            self.assertIsNone(projection.maximum_session_close_drawdown)
            self.assertIsNone(projection.gross_turnover)

    def test_backtest_output_serializes_complete_and_incomplete_evaluation(self) -> None:
        complete = project_performance(seed(), (), (checkpoint(),))
        complete_output = BacktestOutput(
            "a" * 64, 0, (), (), (), fold_accounting(seed(), ()), complete
        )
        complete_payload = json.loads(complete_output.canonical_json())
        self.assertEqual(complete_payload["performance"]["evaluation_status"], "COMPLETE")

        facts = lifecycle("buy", Side.BUY, 1, "100", "0", 1)
        incomplete = project_performance(
            seed(),
            facts,
            (checkpoint(),),
        )
        fill = facts[1]
        assert type(fill) is BrokerFillObserved
        incomplete_output = BacktestOutput(
            "a" * 64,
            0,
            (),
            (
                BacktestOrderRecord(
                    "buy",
                    "broker-buy",
                    Side.BUY,
                    Decimal(1),
                    Decimal(1),
                    Decimal(100),
                    BrokerExecutionState.FILLED,
                ),
            ),
            (
                BacktestFillRecord(
                    "buy",
                    fill.broker_execution_id,
                    Side.BUY,
                    fill.quantity,
                    fill.price,
                    fill.fee,
                    fill.occurred_at,
                ),
            ),
            fold_accounting(seed(), facts),
            incomplete,
        )
        incomplete_payload = json.loads(incomplete_output.canonical_json())
        self.assertEqual(
            incomplete_payload["performance"]["evaluation_status"], "INCOMPLETE"
        )
        self.assertEqual(
            incomplete_payload["performance"]["evidence_use"], "REFERENCE_ONLY"
        )

    def test_invalid_marks_and_nonzero_seed_fail_closed(self) -> None:
        for value in (Decimal(0), Decimal("-1"), Decimal("NaN")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                PerformanceMark(APPLE, value)
        with self.assertRaisesRegex(ValueError, "both a mark"):
            ValuationCheckpoint(
                NOW,
                (PerformanceMark(APPLE, Decimal(1)),),
                (MarkUnavailable(APPLE, IncompleteReasonCode.MISSING_MARK),),
            )
        with self.assertRaisesRegex(ValueError, "zero starting positions"):
            project_performance(
                replace(seed(), positions=(AccountingPosition(APPLE, Decimal(1)),)),
                (),
                (checkpoint("1"),),
            )


if __name__ == "__main__":
    unittest.main()
