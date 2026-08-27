import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from trader.adapters.simulated.simulated_broker import (
    QuoteEvent,
    SimulatedBroker,
    SimulationReason,
)
from trader.application.cancellation import OfflineCancellationService
from trader.application.safety import InvalidPermit, SafetyController
from trader.domain.broker_observations import BrokerOrderRef
from trader.domain.cancellation import CancelOrderCommand, CancelPermit
from trader.domain.models import (
    InstrumentId,
    OrderRequest,
    OrderType,
    SafetyState,
    Side,
    TimeInForce,
    TradingEnvironment,
)
from trader.ports.broker import (
    BrokerCancelOutcome,
    BrokerSubmitOutcome,
)


NOW = datetime(2026, 8, 27, 1, tzinfo=timezone.utc)
INSTRUMENT = InstrumentId("NASDAQ", "AAPL", "USD")


class MutableClock:
    def __init__(self, value):
        self.value = value

    def __call__(self):
        return self.value


def cancel_command(**changes: object) -> CancelOrderCommand:
    values = {
        "command_id": "cancel-1",
        "target": BrokerOrderRef(
            TradingEnvironment.SIMULATED, "paper-main", date(2026, 8, 27), "order-1"
        ),
        "instrument": INSTRUMENT,
        "remaining_quantity": Decimal(3),
        "account_snapshot_id": "snapshot-1",
    }
    values.update(changes)
    return CancelOrderCommand(**values)


class RaisingCancelBroker:
    environment = TradingEnvironment.SIMULATED

    def __init__(self) -> None:
        self.calls = 0

    def cancel(self, command: CancelOrderCommand):
        self.calls += 1
        raise TimeoutError("outcome is unknowable")


class CancellationPermitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.wall = MutableClock(NOW)
        self.monotonic = MutableClock(100.0)
        self.safety = SafetyController(
            TradingEnvironment.SIMULATED, monotonic_clock=self.monotonic
        )
        self.safety.state = SafetyState.HALTED

    def issue(self, command: CancelOrderCommand | None = None) -> tuple[CancelOrderCommand, CancelPermit]:
        command = command or cancel_command()
        return command, self.safety._issue_cancel_permit(command, self.wall())

    def test_forged_wrong_target_and_snapshot_permits_fail_closed(self) -> None:
        command, permit = self.issue()
        forged = replace(permit, expires_at=permit.expires_at + timedelta(seconds=1))
        with self.assertRaises(InvalidPermit):
            self.safety.consume_cancel_permit(forged, command, self.wall())

        command, permit = self.issue()
        wrong_target = replace(
            command,
            target=replace(command.target, broker_order_id="other-order"),
        )
        with self.assertRaises(InvalidPermit):
            self.safety.consume_cancel_permit(permit, wrong_target, self.wall())

        command, permit = self.issue()
        with self.assertRaises(InvalidPermit):
            self.safety.consume_cancel_permit(
                permit, replace(command, account_snapshot_id="snapshot-2"), self.wall()
            )

    def test_expiry_epoch_clock_and_reuse_fail_closed(self) -> None:
        command, permit = self.issue()
        self.wall.value = permit.expires_at
        with self.assertRaises(InvalidPermit):
            self.safety.consume_cancel_permit(permit, command, self.wall())

        self.wall.value = NOW
        command, permit = self.issue()
        self.safety.halt("NEW_BLOCKER")
        with self.assertRaises(InvalidPermit):
            self.safety.consume_cancel_permit(permit, command, self.wall())

        self.safety._blockers.clear()
        command, permit = self.issue()
        self.wall.value = NOW - timedelta(seconds=1)
        with self.assertRaises(InvalidPermit):
            self.safety.consume_cancel_permit(permit, command, self.wall())
        self.assertIn("CLOCK_ROLLBACK", self.safety.blockers)

        self.safety._blockers.clear()
        self.wall.value = NOW
        command, permit = self.issue()
        self.safety.consume_cancel_permit(permit, command, self.wall())
        with self.assertRaises(InvalidPermit):
            self.safety.consume_cancel_permit(permit, command, self.wall())

    def test_timeout_is_unknown_and_broker_is_called_once(self) -> None:
        broker = RaisingCancelBroker()
        service = OfflineCancellationService(
            broker, self.safety, account_id="paper-main", clock=self.wall
        )
        command, permit = self.issue()
        self.assertIs(service.cancel(command, permit), BrokerCancelOutcome.UNKNOWN)
        with self.assertRaises(InvalidPermit):
            service.cancel(command, permit)
        self.assertEqual(broker.calls, 1)


class SimulatedCancellationTests(unittest.TestCase):
    def test_partial_fill_then_typed_cancel_preserves_event_order(self) -> None:
        wall = MutableClock(NOW)
        broker = SimulatedBroker(
            clock=wall,
            known_symbols={"AAPL"},
            partial_fill_cap=2,
        )
        request = OrderRequest(
            "client-1",
            "plan-1",
            "paper-main",
            INSTRUMENT,
            Side.BUY,
            OrderType.LIMIT,
            TimeInForce.DAY,
            Decimal(5),
            Decimal("200"),
            NOW,
        )
        submitted = broker.submit(request)
        self.assertIs(submitted.outcome, BrokerSubmitOutcome.ACKNOWLEDGED)
        order_id = submitted.broker_order_id
        assert order_id is not None
        fill = broker.on_quote(
            QuoteEvent("AAPL", Decimal("199"), Decimal("200"), 5, NOW, 1)
        )
        self.assertIs(fill.reason, SimulationReason.FILLED)
        command = cancel_command(
            target=BrokerOrderRef(
                TradingEnvironment.SIMULATED,
                "paper-main",
                NOW.date(),
                order_id,
            )
        )
        result = broker.cancel(command)
        self.assertIs(result.outcome, BrokerCancelOutcome.ACK)
        self.assertEqual(broker.order(order_id).canceled, Decimal(3))
        wall.value = NOW + timedelta(seconds=1)
        later = broker.on_quote(
            QuoteEvent(
                "AAPL", Decimal("199"), Decimal("200"), 5, NOW + timedelta(seconds=1), 2
            )
        )
        self.assertIs(later.reason, SimulationReason.NO_ACTIVE_ORDER)


if __name__ == "__main__":
    unittest.main()
