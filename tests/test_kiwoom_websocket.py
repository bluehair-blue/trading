import math
import threading
import unittest

from trader.adapters.kiwoom.websocket import (
    KiwoomWebSocketSupervisor,
    SessionOwnership,
    StreamEvent,
    StreamEventKind,
    StreamState,
)
from trader.application.safety import SafetyController
from trader.domain.models import SafetyState, TradingEnvironment


class Transport:
    def __init__(self):
        self.calls = []

    def connect(self, account_id, token, environment):
        self.calls.append(("connect", account_id, token, environment))

    def subscribe(self, symbols):
        self.calls.append(("subscribe", symbols))

    def close(self):
        self.calls.append(("close",))


class SupervisorTests(unittest.TestCase):
    def setUp(self):
        self.now = [1.0]
        self.transport = Transport()
        self.blockers = []
        self.reconciliations = []
        self.ownership = SessionOwnership()

    def supervisor(self, **changes):
        values = {
            "account_id": "acct",
            "token": "secret",
            "environment": TradingEnvironment.PAPER,
            "transport": self.transport,
            "monotonic_clock": lambda: self.now[0],
            "blocker": self.blockers.append,
            "reconcile": lambda account, environment: self.reconciliations.append(
                (account, environment)
            )
            is None,
            "heartbeat_timeout_seconds": 5,
            "ownership": self.ownership,
        }
        values.update(changes)
        return KiwoomWebSocketSupervisor(**values)

    def event(self, sequence, event_id=None, environment=TradingEnvironment.PAPER):
        return StreamEvent(
            event_id or f"event-{sequence}",
            sequence,
            environment,
            StreamEventKind.DATA,
            {"price": "1"},
        )

    def test_ownership_subscription_limit_and_deduplication(self):
        first = self.supervisor()
        first.subscribe("B", "A", "A")
        first.connect()
        self.assertEqual(first.subscriptions, ("A", "B"))
        self.assertEqual(self.transport.calls[-1], ("subscribe", ("A", "B")))
        first.subscribe("A")
        self.assertEqual(len(self.transport.calls), 2)
        with self.assertRaises(RuntimeError):
            self.supervisor().connect()
        with self.assertRaises(ValueError):
            first.subscribe(*(f"S{i}" for i in range(199)))
        first.close()
        replacement = self.supervisor()
        replacement.connect()
        self.assertEqual(replacement.state, StreamState.READY)

    def test_consumer_failure_reaches_other_consumers_then_fails_closed(self):
        received = []
        supervisor = self.supervisor()
        supervisor.add_consumer(lambda _: (_ for _ in ()).throw(ValueError("bad")))
        supervisor.add_consumer(received.append)
        supervisor.connect()
        event = self.event(1)
        supervisor.accept(event)
        self.assertEqual(received, [event])
        self.assertIn("CONSUMER_ERROR", [item.code for item in supervisor.evidence])
        self.assertEqual(supervisor.state, StreamState.RECONNECTING)
        self.assertIn("CONSUMER_FAILURE", self.blockers)

    def test_session_ownership_acquisition_is_atomic_across_threads(self):
        ownership = SessionOwnership()
        key = ("account", "token-hash")
        barrier = threading.Barrier(24)
        successes = []
        failures = []

        def acquire(owner):
            barrier.wait()
            try:
                ownership.acquire(key, owner)
                successes.append(owner)
            except RuntimeError:
                failures.append(owner)

        owners = [object() for _ in range(24)]
        workers = [threading.Thread(target=acquire, args=(owner,)) for owner in owners]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(2)
        self.assertTrue(all(not worker.is_alive() for worker in workers))
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(failures), 23)

    def test_gap_disarms_ignores_events_then_reconciles_without_arming(self):
        received = []
        supervisor = self.supervisor()
        supervisor.add_consumer(received.append)
        supervisor.subscribe("005930")
        supervisor.connect()
        supervisor.accept(self.event(10))
        supervisor.accept(self.event(12))
        self.assertEqual(supervisor.state, StreamState.RECONNECTING)
        self.assertEqual(self.blockers, ["STREAM_GAP"])
        supervisor.accept(self.event(11))
        self.assertEqual(received, [self.event(10)])
        self.assertTrue(supervisor.reconnect())
        self.assertEqual(supervisor.state, StreamState.READY)
        self.assertEqual(
            self.reconciliations, [("acct", TradingEnvironment.PAPER)]
        )
        self.assertEqual(
            [call for call in self.transport.calls if call[0] == "subscribe"],
            [("subscribe", ("005930",)), ("subscribe", ("005930",))],
        )
        self.assertNotIn("ARM", [item.code for item in supervisor.evidence])

    def test_failed_reconciliation_stays_reconnecting(self):
        supervisor = self.supervisor(reconcile=lambda _account, _environment: False)
        supervisor.connect()
        supervisor.disconnected()
        self.assertFalse(supervisor.reconnect())
        self.assertEqual(supervisor.state, StreamState.RECONNECTING)

    def test_safety_controller_halt_callback_invalidates_safety_state(self):
        safety = SafetyController(TradingEnvironment.PAPER)
        supervisor = self.supervisor(blocker=safety.halt)
        supervisor.connect()
        supervisor.accept(self.event(1))
        supervisor.accept(self.event(3))
        self.assertEqual(safety.state, SafetyState.HALTED)
        self.assertIn("STREAM_GAP", safety.blockers)

    def test_duplicate_reorder_and_heartbeat_timeout_are_deterministic(self):
        supervisor = self.supervisor()
        supervisor.connect()
        supervisor.accept(self.event(2, "a"))
        supervisor.accept(self.event(2, "b"))
        supervisor.accept(self.event(1, "c"))
        supervisor.accept(self.event(3, "a"))
        self.now[0] = 7.0
        supervisor.check_heartbeat()
        self.assertEqual(
            [item.code for item in supervisor.evidence][-4:],
            [
                "DUPLICATE_SEQUENCE",
                "REORDERED_EVENT",
                "DUPLICATE_EVENT",
                "HEARTBEAT_TIMEOUT",
            ],
        )
        transition = supervisor.evidence[-1]
        self.assertEqual(transition.state, StreamState.RECONNECTING)
        self.assertEqual(
            transition.details,
            (("previous", "READY"), ("target", "RECONNECTING")),
        )

    def test_provenance_clock_rollback_and_nan_fail_closed(self):
        supervisor = self.supervisor()
        supervisor.connect()
        supervisor.accept(self.event(1, environment=TradingEnvironment.LIVE))
        self.assertEqual(self.blockers, ["STREAM_PROVENANCE_MISMATCH"])

        rollback = self.supervisor(token="other")
        self.now[0] = 10.0
        rollback.connect()
        self.now[0] = 9.0
        rollback.check_heartbeat()
        self.assertEqual(self.blockers[-1], "CLOCK_FAILURE")
        self.assertEqual(rollback.state, StreamState.RECONNECTING)

        nan_clock = self.supervisor(
            token="third", monotonic_clock=lambda: math.nan
        )
        with self.assertRaises(RuntimeError):
            nan_clock.connect()
        self.assertEqual(self.blockers[-1], "CLOCK_FAILURE")
        self.assertEqual(nan_clock.state, StreamState.RECONNECTING)


if __name__ == "__main__":
    unittest.main()
