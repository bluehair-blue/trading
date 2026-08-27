from datetime import datetime, timedelta, timezone
from decimal import Decimal
import unittest

from trader.adapters.simulated.fake_broker import FakeBroker
from trader.adapters.simulated.simulated_broker import (
    QuoteEvent,
    SimulatedBroker,
    SimulationReason,
)
from trader.adapters.simulated.stub_broker import StubBroker
from trader.domain.models import (
    BrokerExecutionState,
    InstrumentId,
    OrderRequest,
    OrderType,
    Side,
    TimeInForce,
)
from trader.ports.broker import BrokerSubmitOutcome


UTC = timezone.utc


class MutableClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def request(now: datetime, *, client_id: str = "c-1", side: Side = Side.BUY) -> OrderRequest:
    return OrderRequest(
        client_id,
        "plan-1",
        "account-1",
        InstrumentId("NASDAQ", "AAPL", "USD"),
        side,
        OrderType.LIMIT,
        TimeInForce.DAY,
        Decimal(10),
        Decimal("101") if side is Side.BUY else Decimal("99"),
        now,
    )


class SimulatedBrokerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.start = datetime(2026, 8, 27, 9, tzinfo=UTC)
        self.clock = MutableClock(self.start)
        self.broker = SimulatedBroker(
            clock=self.clock,
            known_symbols={"AAPL"},
            latency=timedelta(seconds=1),
            partial_fill_cap=3,
            slippage_bps=Decimal(10),
            fee_bps=Decimal(5),
            max_quote_age=timedelta(seconds=2),
        )

    def submit(self, **kwargs: object) -> str:
        result = self.broker.submit(request(self.start, **kwargs))
        self.assertEqual(result.outcome, BrokerSubmitOutcome.ACKNOWLEDGED)
        assert result.broker_order_id is not None
        return result.broker_order_id

    def quote(self, sequence: int, **kwargs: object) -> QuoteEvent:
        values = {
            "symbol": "AAPL",
            "bid": Decimal(99),
            "ask": Decimal(100),
            "available_quantity": 10,
            "occurred_at": self.clock.now,
            "sequence": sequence,
        }
        values.update(kwargs)
        return QuoteEvent(**values)  # type: ignore[arg-type]

    def test_fake_broker_is_compatibility_alias(self) -> None:
        self.assertIs(FakeBroker, StubBroker)

    def test_submit_is_deterministic_and_duplicate_is_rejected(self) -> None:
        first = self.broker.submit(request(self.start))
        duplicate = self.broker.submit(request(self.start))
        self.assertEqual(first.broker_order_id, "sim-a6f7ef47ee8dc84af905")
        self.assertEqual(duplicate.detail_code, "DUPLICATE_CLIENT_ORDER_ID")

    def test_latency_partial_fills_slippage_fee_and_partition(self) -> None:
        order_id = self.submit()
        self.assertEqual(self.broker.on_quote(self.quote(1)).reason, SimulationReason.LATENCY)
        self.clock.now += timedelta(seconds=1)
        result = self.broker.on_quote(self.quote(2))
        fill = result.fills[0]
        self.assertEqual(fill.quantity, Decimal(3))
        self.assertEqual(fill.price, Decimal("100.1"))
        self.assertEqual(fill.fee, Decimal("0.15015"))
        observed = self.broker.order(order_id)
        self.assertEqual(observed.execution_state, BrokerExecutionState.PARTIALLY_FILLED)
        self.assertEqual(observed.requested, observed.filled + observed.open)

    def test_limit_price_is_never_violated_by_slippage(self) -> None:
        self.broker = SimulatedBroker(
            clock=self.clock,
            known_symbols={"AAPL"},
            slippage_bps=Decimal(1000),
        )
        self.submit()
        fill = self.broker.on_quote(self.quote(1, ask=Decimal("100.9"))).fills[0]
        self.assertEqual(fill.price, Decimal(101))

    def test_stale_halted_unknown_and_out_of_order_quotes_do_not_fill(self) -> None:
        order_id = self.submit()
        self.clock.now += timedelta(seconds=3)
        stale = self.broker.on_quote(self.quote(1, occurred_at=self.start))
        halted = self.broker.on_quote(self.quote(2, halted=True))
        unknown = self.broker.on_quote(self.quote(1, symbol="MSFT"))
        duplicate = self.broker.on_quote(self.quote(2, halted=True))
        old = self.broker.on_quote(self.quote(1))
        self.assertEqual(
            [stale.reason, halted.reason, unknown.reason, duplicate.reason, old.reason],
            [
                SimulationReason.STALE_QUOTE,
                SimulationReason.HALTED,
                SimulationReason.UNKNOWN_SYMBOL,
                SimulationReason.DUPLICATE_QUOTE,
                SimulationReason.OUT_OF_ORDER_QUOTE,
            ],
        )
        self.assertEqual(self.broker.order(order_id).filled, Decimal(0))

    def test_cancel_after_fill_cancels_only_remaining_and_race_is_ordered(self) -> None:
        order_id = self.submit()
        self.clock.now += timedelta(seconds=1)
        self.broker.on_quote(self.quote(1))
        canceled = self.broker.cancel(order_id, occurred_at=self.clock.now, sequence=2)
        losing_race = self.broker.on_quote(self.quote(1))
        observed = self.broker.order(order_id)
        self.assertEqual(canceled.affected_quantity, Decimal(7))
        self.assertEqual(losing_race.reason, SimulationReason.DUPLICATE_QUOTE)
        self.assertEqual(observed.filled + observed.canceled, observed.requested)

    def test_day_expiry_and_corporate_action_halt(self) -> None:
        order_id = self.submit()
        self.clock.now += timedelta(days=1)
        expired = self.broker.expire_day(occurred_at=self.clock.now, sequence=1)
        action = self.broker.on_corporate_action(
            "AAPL", "split", occurred_at=self.clock.now, sequence=2
        )
        self.assertEqual(expired[0].affected_quantity, Decimal(10))
        self.assertEqual(self.broker.order(order_id).execution_state, BrokerExecutionState.EXPIRED)
        self.assertEqual(action.reason, SimulationReason.UNSUPPORTED_CORPORATE_ACTION)

    def test_corporate_action_halts_active_and_future_orders_until_policy_exists(self) -> None:
        order_id = self.submit()
        action = self.broker.on_corporate_action(
            "AAPL", "split", occurred_at=self.clock.now, sequence=1
        )
        self.clock.now += timedelta(seconds=1)
        quote_result = self.broker.on_quote(self.quote(2))
        rejected = self.broker.submit(request(self.clock.now, client_id="after-action"))

        self.assertEqual(action.detail, "SPLIT_ACTIVE_ORDER_HALT")
        self.assertEqual(
            quote_result.reason, SimulationReason.UNSUPPORTED_CORPORATE_ACTION
        )
        self.assertEqual(self.broker.order(order_id).filled, Decimal(0))
        self.assertEqual(rejected.outcome, BrokerSubmitOutcome.REJECTED)
        self.assertEqual(rejected.detail_code, "CORPORATE_ACTION_UNRESOLVED")

    def test_rejects_hostile_numeric_and_time_inputs(self) -> None:
        bad_values = [1.0, True, Decimal("NaN"), Decimal("Infinity"), Decimal(-1)]
        for value in bad_values:
            with self.subTest(value=value), self.assertRaises(ValueError):
                SimulatedBroker(
                    clock=self.clock,
                    known_symbols={"AAPL"},
                    fee_bps=value,  # type: ignore[arg-type]
                )
        with self.assertRaises(ValueError):
            self.quote(1, available_quantity=True)
        self.broker.submit(request(self.start))
        self.clock.now -= timedelta(seconds=1)
        with self.assertRaises(ValueError):
            self.broker.on_quote(self.quote(1))


if __name__ == "__main__":
    unittest.main()
