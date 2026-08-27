import unittest

from trader.entrypoints.runtime import bootstrap_runtime


class Lock:
    def __init__(self, events, *, busy=False):
        self.events = events
        self.busy = busy
        self.held = False

    def acquire(self):
        self.events.append("lock")
        if self.busy:
            raise RuntimeError("busy")
        self.held = True

    def release(self):
        self.events.append("unlock")
        self.held = False


class Session:
    def __init__(self, events):
        self.events = events

    def close(self):
        self.events.append("close-stream")


class RuntimeBootstrapTests(unittest.TestCase):
    def test_lock_precedes_every_external_step_and_no_automatic_arm_occurs(self):
        events = []
        lock = Lock(events)

        def verify():
            self.assertTrue(lock.held)
            events.append("verify-ledger")
            return True

        def token():
            self.assertTrue(lock.held)
            events.append("token-health")
            return object()

        def stream(unused_token):
            self.assertTrue(lock.held)
            events.append("connect-stream")
            return Session(events)

        def reconcile(unused_token, unused_stream):
            self.assertTrue(lock.held)
            events.append("reconcile")
            return True

        with bootstrap_runtime(
            runtime_lock=lock,
            verify_ledger=verify,
            initialize_token_health=token,
            connect_stream=stream,
            reconcile=reconcile,
        ) as runtime:
            self.assertTrue(lock.held)
            self.assertIsNotNone(runtime.token_health)
            self.assertNotIn("arm", events)

        self.assertEqual(
            events,
            [
                "lock",
                "verify-ledger",
                "token-health",
                "connect-stream",
                "reconcile",
                "close-stream",
                "unlock",
            ],
        )

    def test_busy_lock_prevents_ledger_and_broker_initialization(self):
        events = []
        with self.assertRaisesRegex(RuntimeError, "busy"):
            with bootstrap_runtime(
                runtime_lock=Lock(events, busy=True),
                verify_ledger=lambda: events.append("verify") or True,
                initialize_token_health=lambda: events.append("token") or object(),
                connect_stream=lambda token: events.append("stream") or Session(events),
                reconcile=lambda token, stream: events.append("reconcile") or True,
            ):
                self.fail("busy runtime cannot start")
        self.assertEqual(events, ["lock"])

    def test_each_failure_closes_owned_resources_and_releases_lock(self):
        for failure in ("ledger", "token", "stream", "reconcile"):
            with self.subTest(failure=failure):
                events = []
                lock = Lock(events)

                def verify():
                    events.append("verify")
                    return failure != "ledger"

                def token():
                    events.append("token")
                    return None if failure == "token" else object()

                def stream(unused_token):
                    events.append("stream")
                    return None if failure == "stream" else Session(events)

                def reconcile(unused_token, unused_stream):
                    events.append("reconcile")
                    return failure != "reconcile"

                with self.assertRaises(RuntimeError):
                    with bootstrap_runtime(
                        runtime_lock=lock,
                        verify_ledger=verify,
                        initialize_token_health=token,
                        connect_stream=stream,
                        reconcile=reconcile,
                    ):
                        self.fail("failed startup cannot yield")
                self.assertFalse(lock.held)
                self.assertEqual(events[-1], "unlock")
                if failure == "reconcile":
                    self.assertEqual(events[-2], "close-stream")

    def test_non_boolean_truthy_verdicts_do_not_open_the_runtime(self):
        events = []
        with self.assertRaisesRegex(RuntimeError, "ledger"):
            with bootstrap_runtime(
                runtime_lock=Lock(events),
                verify_ledger=lambda: 1,
                initialize_token_health=object,
                connect_stream=lambda token: Session(events),
                reconcile=lambda token, stream: True,
            ):
                self.fail("truthy is not verified")


if __name__ == "__main__":
    unittest.main()
