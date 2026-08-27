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
from trader.domain.broker_lifecycle import (
    BrokerFillObserved,
    BrokerOrderExpired,
    BrokerOrderOpened,
    BrokerOrderRejected,
)
from trader.domain.broker_observations import BrokerOrderRef
from trader.domain.cancellation import CancelOrderCommand
from trader.ports.broker import (
    BrokerCancelOutcome,
    BrokerEnvironment,
    BrokerSubmitOutcome,
)


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
            business_date=lambda value: value.date(),
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
        self.assertTrue(fill.broker_execution_id.startswith("sim-execution-"))
        self.assertIs(type(result.facts[0]), BrokerFillObserved)
        observed = self.broker.order(order_id)
        self.assertEqual(observed.execution_state, BrokerExecutionState.PARTIALLY_FILLED)
        self.assertEqual(observed.requested, observed.filled + observed.open)

    def test_limit_price_is_never_violated_by_slippage(self) -> None:
        self.broker = SimulatedBroker(
            clock=self.clock,
            business_date=lambda value: value.date(),
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

    def test_business_date_policy_controls_cancel_and_day_expiry(self) -> None:
        shifted_start = datetime(2026, 8, 27, 6, tzinfo=UTC)
        shifted_clock = MutableClock(shifted_start)
        shifted = SimulatedBroker(
            clock=shifted_clock,
            business_date=lambda value: (value - timedelta(hours=5)).date(),
            known_symbols={"AAPL"},
        )
        cancel_request = request(
            shifted_start - timedelta(hours=2),
            client_id="cancel-local-date",
        )
        cancel_id = shifted.submit(cancel_request)
        assert cancel_id.broker_order_id is not None
        opened = shifted.opened_fact(cancel_id.broker_order_id)
        self.assertEqual(opened.broker_order_ref.business_date, shifted_start.date())
        canceled = shifted.cancel(CancelOrderCommand(
            "cancel-local-date-command",
            opened.broker_order_ref,
            cancel_request.instrument,
            Decimal(10),
            "snapshot-1",
        ))
        self.assertEqual(canceled.outcome, BrokerCancelOutcome.ACK)

        near_midnight = datetime(2026, 8, 27, 23, 30, tzinfo=UTC)
        shifted_clock.now = near_midnight
        expiry = shifted.submit(request(near_midnight, client_id="expiry-local-date"))
        assert expiry.broker_order_id is not None
        shifted_clock.now += timedelta(hours=1)
        self.assertEqual(
            shifted.expire_day(occurred_at=shifted_clock.now, sequence=1),
            (),
        )
        quote_result = shifted.on_quote(QuoteEvent(
            "AAPL",
            Decimal(99),
            Decimal(100),
            0,
            shifted_clock.now,
            2,
        ))
        self.assertNotEqual(quote_result.reason, SimulationReason.DAY_EXPIRED)
        shifted_clock.now += timedelta(days=1)
        expired = shifted.on_quote(QuoteEvent(
            "AAPL",
            Decimal(99),
            Decimal(100),
            0,
            shifted_clock.now,
            3,
        ))
        self.assertEqual(expired.reason, SimulationReason.DAY_EXPIRED)
        self.assertIs(type(expired.facts[0]), BrokerOrderExpired)
        self.assertEqual(
            expired.facts[0].broker_order_ref,
            BrokerOrderRef(
                BrokerEnvironment.SIMULATED,
                "account-1",
                near_midnight.date(),
                expiry.broker_order_id,
            ),
        )

    def test_open_expire_and_reject_emit_deterministic_lifecycle_facts(self) -> None:
        order_id = self.submit()
        first_open = self.broker.opened_fact(order_id)
        second_open = self.broker.opened_fact(order_id)
        self.assertIs(type(first_open), BrokerOrderOpened)
        self.assertEqual(first_open, second_open)

        rejected = self.broker.reject_order(
            order_id,
            occurred_at=self.clock.now,
            sequence=1,
            reason_code="SIMULATED_REJECT",
        )
        self.assertEqual(rejected.reason, SimulationReason.BROKER_REJECTED)
        self.assertIs(type(rejected.facts[0]), BrokerOrderRejected)
        self.assertEqual(
            self.broker.order(order_id).execution_state,
            BrokerExecutionState.REJECTED,
        )

        expiring_id = self.broker.submit(
            request(self.clock.now, client_id="expires")
        ).broker_order_id
        assert expiring_id is not None
        self.clock.now += timedelta(days=1)
        expired = self.broker.expire_day(occurred_at=self.clock.now, sequence=2)
        self.assertIs(type(expired[0].facts[0]), BrokerOrderExpired)

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
                    business_date=lambda value: value.date(),
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
