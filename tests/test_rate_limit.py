from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from threading import Barrier, Thread
import unittest

from trader.adapters.kiwoom.rate_limit import (
    KiwoomReadonlyRateLimiter,
    OrderPriority,
    QueryPriority,
    RateLimitKind,
    RateLimitReason,
    current_official_policy,
)
from trader.ports.account import AccountEnvironment


class ManualClock:
    def __init__(self, value: float = 0.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class WallClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class RateLimitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.monotonic = ManualClock()
        self.wall = WallClock(datetime(2026, 8, 27, 2, 0, tzinfo=timezone.utc))
        self.limiter = KiwoomReadonlyRateLimiter(
            current_official_policy(), self.monotonic, self.wall
        )

    def acquire(
        self,
        *,
        token: str = "secret-token",
        api_id: str = "ust21070",
        environment: AccountEnvironment = AccountEnvironment.LIVE,
        priority: QueryPriority = QueryPriority.RECONCILIATION,
        kind: RateLimitKind = RateLimitKind.QUERY,
        order_priority: OrderPriority | None = None,
    ):
        return self.limiter.acquire(
            environment=environment,
            token=token,
            api_id=api_id,
            priority=priority,
            kind=kind,
            order_priority=order_priority,
        )

    def test_rolling_window_releases_at_exact_boundary(self) -> None:
        self.assertTrue(all(self.acquire().allowed for _ in range(5)))
        denied = self.acquire()
        self.assertEqual(denied.reason, RateLimitReason.ACCOUNT_QUERY)
        self.assertEqual(denied.retry_after, 1.0)

        self.monotonic.value = 1.0
        self.assertTrue(self.acquire().allowed)

    def test_kst_peak_boundaries_use_three_per_second(self) -> None:
        self.wall.value = datetime(
            2026, 8, 26, 23, 59, 59, 999_999, tzinfo=timezone.utc
        )
        self.assertTrue(all(self.acquire().allowed for _ in range(5)))

        self.monotonic.value = 1.0
        self.wall.value = datetime(2026, 8, 27, 0, 0, tzinfo=timezone.utc)
        self.assertTrue(all(self.acquire().allowed for _ in range(3)))
        self.assertEqual(self.acquire().reason, RateLimitReason.ACCOUNT_QUERY)

        self.monotonic.value = 2.0
        self.wall.value = datetime(2026, 8, 27, 1, 0, tzinfo=timezone.utc)
        self.assertTrue(all(self.acquire().allowed for _ in range(5)))

    def test_mock_same_tr_is_one_per_second(self) -> None:
        self.assertTrue(self.acquire(environment=AccountEnvironment.MOCK).allowed)
        denied = self.acquire(environment=AccountEnvironment.MOCK)
        self.assertEqual(denied.reason, RateLimitReason.MOCK_SAME_TR)
        self.assertTrue(
            self.acquire(environment=AccountEnvironment.MOCK, api_id="ust21110").allowed
        )

    def test_global_limit_is_shared_across_tokens(self) -> None:
        for number in range(50):
            self.assertTrue(self.acquire(token=f"token-{number}").allowed)
        self.assertEqual(self.acquire(token="token-50").reason, RateLimitReason.GLOBAL)

    def test_standard_queries_preserve_reconciliation_capacity(self) -> None:
        for _ in range(4):
            self.assertTrue(self.acquire(priority=QueryPriority.STANDARD).allowed)
        self.assertEqual(
            self.acquire(priority=QueryPriority.STANDARD).reason,
            RateLimitReason.RECONCILIATION_RESERVED,
        )
        self.assertTrue(self.acquire(priority=QueryPriority.RECONCILIATION).allowed)
        self.assertEqual(
            self.acquire(priority=QueryPriority.RECONCILIATION).reason,
            RateLimitReason.ACCOUNT_QUERY,
        )

    def test_query_reconciliation_priorities_share_reserved_capacity(self) -> None:
        for priority in (
            QueryPriority.RESEARCH,
            QueryPriority.BULK,
            QueryPriority.RESEARCH,
            QueryPriority.BULK,
        ):
            self.assertTrue(self.acquire(priority=priority).allowed)
        self.assertEqual(
            self.acquire(priority=QueryPriority.STANDARD).reason,
            RateLimitReason.RECONCILIATION_RESERVED,
        )
        self.assertTrue(
            self.acquire(priority=QueryPriority.UNKNOWN_RECONCILIATION).allowed
        )
        self.assertEqual(
            self.acquire(priority=QueryPriority.ACCOUNT_RECONCILIATION).reason,
            RateLimitReason.ACCOUNT_QUERY,
        )

    def test_cancel_capacity_survives_new_and_reduce_order_saturation(self) -> None:
        for number in range(9):
            priority = (
                OrderPriority.NEW_ORDER
                if number % 2 == 0
                else OrderPriority.REDUCE_ONLY
            )
            self.assertTrue(
                self.acquire(kind=RateLimitKind.ORDER, order_priority=priority).allowed
            )
        self.assertEqual(
            self.acquire(
                kind=RateLimitKind.ORDER,
                order_priority=OrderPriority.NEW_ORDER,
            ).reason,
            RateLimitReason.CANCEL_RESERVED,
        )
        self.assertTrue(
            self.acquire(
                kind=RateLimitKind.ORDER,
                order_priority=OrderPriority.CANCEL,
            ).allowed
        )
        self.assertEqual(
            self.acquire(
                kind=RateLimitKind.ORDER,
                order_priority=OrderPriority.CANCEL,
            ).reason,
            RateLimitReason.ACCOUNT_ORDER,
        )

    def test_peak_transition_applies_order_limit_immediately(self) -> None:
        self.wall.value = datetime(2026, 8, 26, 23, 59, 59, tzinfo=timezone.utc)
        for _ in range(2):
            self.assertTrue(
                self.acquire(
                    kind=RateLimitKind.ORDER,
                    order_priority=OrderPriority.NEW_ORDER,
                ).allowed
            )
        self.wall.value = datetime(2026, 8, 27, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(
            self.acquire(
                kind=RateLimitKind.ORDER,
                order_priority=OrderPriority.NEW_ORDER,
            ).reason,
            RateLimitReason.CANCEL_RESERVED,
        )
        self.assertTrue(
            self.acquire(
                kind=RateLimitKind.ORDER,
                order_priority=OrderPriority.CANCEL,
            ).allowed
        )

    def test_special_tr_has_five_per_minute_limit(self) -> None:
        for second in range(5):
            self.monotonic.value = float(second)
            self.assertTrue(
                self.acquire(api_id="usa10099", kind=RateLimitKind.SPECIAL).allowed
            )
        self.monotonic.value = 5.0
        denied = self.acquire(api_id="usa10099", kind=RateLimitKind.SPECIAL)
        self.assertEqual(denied.reason, RateLimitReason.SPECIAL)
        self.assertEqual(denied.retry_after, 55.0)
        self.monotonic.value = 60.0
        self.assertTrue(
            self.acquire(api_id="usa10099", kind=RateLimitKind.SPECIAL).allowed
        )

    def test_global_limit_is_shared_across_request_kinds(self) -> None:
        for number in range(50):
            kwargs = (
                {
                    "kind": RateLimitKind.ORDER,
                    "order_priority": OrderPriority.CANCEL,
                }
                if number % 2
                else {
                    "kind": RateLimitKind.QUERY,
                    "priority": QueryPriority.RECONCILIATION,
                }
            )
            self.assertTrue(self.acquire(token=f"shared-{number}", **kwargs).allowed)
        self.assertEqual(
            self.acquire(token="shared-50", kind=RateLimitKind.FX).reason,
            RateLimitReason.GLOBAL,
        )

    def test_kind_specific_limits_and_order_validation(self) -> None:
        policy = current_official_policy()
        self.assertEqual(
            (
                policy.us_global_queries,
                policy.us_account_orders,
                policy.us_peak_account_orders,
                policy.us_account_fx,
                policy.us_chart_queries,
            ),
            (50, 10, 3, 1, 20),
        )
        self.assertEqual(
            self.acquire(kind=RateLimitKind.ORDER).reason,
            RateLimitReason.INVALID_REQUEST,
        )
        self.assertEqual(
            self.acquire(
                kind=RateLimitKind.QUERY,
                order_priority=OrderPriority.CANCEL,
            ).reason,
            RateLimitReason.INVALID_REQUEST,
        )
        self.assertEqual(
            self.acquire(api_id="usa10099").reason,
            RateLimitReason.INVALID_REQUEST,
        )
        self.assertEqual(
            self.acquire(kind=RateLimitKind.SPECIAL).reason,
            RateLimitReason.INVALID_REQUEST,
        )
        self.assertTrue(self.acquire(kind=RateLimitKind.FX).allowed)
        self.assertEqual(
            self.acquire(kind=RateLimitKind.FX).reason,
            RateLimitReason.FX,
        )

    def test_lock_admits_exactly_the_capacity_under_concurrency(self) -> None:
        barrier = Barrier(11)
        decisions = []

        def run() -> None:
            barrier.wait()
            decisions.append(self.acquire())

        threads = [Thread(target=run) for _ in range(10)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()
        self.assertEqual(sum(decision.allowed for decision in decisions), 5)

    def test_clock_faults_and_regression_fail_closed_without_consuming_quota(self) -> None:
        self.assertTrue(self.acquire().allowed)
        self.monotonic.value = -1.0
        self.assertEqual(self.acquire().reason, RateLimitReason.CLOCK_FAILURE)
        self.monotonic.value = float("nan")
        self.assertEqual(self.acquire().reason, RateLimitReason.CLOCK_FAILURE)
        self.monotonic.value = 1.0
        self.assertTrue(self.acquire().allowed)

        def broken_clock() -> float:
            raise RuntimeError("clock unavailable")

        broken = KiwoomReadonlyRateLimiter(
            current_official_policy(), broken_clock, self.wall
        )
        self.assertEqual(
            broken.acquire(
                environment=AccountEnvironment.LIVE,
                token="token",
                api_id="ust21070",
                priority=QueryPriority.RECONCILIATION,
            ).reason,
            RateLimitReason.CLOCK_FAILURE,
        )

    def test_policy_is_versioned_injected_and_token_key_is_fingerprinted(self) -> None:
        policy = replace(current_official_policy(), us_account_queries=6)
        limiter = KiwoomReadonlyRateLimiter(policy, self.monotonic, self.wall)
        limiter.acquire(
            environment=AccountEnvironment.LIVE,
            token="raw-secret-token",
            api_id="ust21070",
            priority=QueryPriority.RECONCILIATION,
        )
        self.assertEqual(limiter.policy.snapshot_version, "kiwoom-official-2026-08-27")
        self.assertNotIn("raw-secret-token", repr(limiter._events))


if __name__ == "__main__":
    unittest.main()
