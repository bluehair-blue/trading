from datetime import datetime, timezone
from decimal import Decimal
import unittest

from trader.application.portfolio import (
    InstrumentQuantityLimit,
    StrategyQuantityBudget,
    allocate_targets,
)
from trader.domain.models import InstrumentId, PositionTarget, TargetUnit


NOW = datetime(2026, 8, 27, tzinfo=timezone.utc)
APPLE = InstrumentId("NASDAQ", "AAPL", "USD")


def target(target_id: str, strategy_id: str, quantity: str) -> PositionTarget:
    return PositionTarget(
        target_id,
        strategy_id,
        f"decision-{target_id}",
        f"{strategy_id}-v1",
        "strategy-input-1",
        APPLE,
        Decimal(quantity),
        TargetUnit.SHARES,
        NOW,
    )


class PortfolioAllocatorTests(unittest.TestCase):
    def allocate(self, targets, strategy_caps=("6", "7"), account_cap="10"):
        return allocate_targets(
            account_id="paper-main",
            allocation_id="allocation-1",
            policy_version="allocation-v1",
            input_snapshot_id="account-snapshot-1",
            targets=targets,
            strategy_budgets=(
                StrategyQuantityBudget("alpha", APPLE, Decimal(strategy_caps[0])),
                StrategyQuantityBudget("beta", APPLE, Decimal(strategy_caps[1])),
            ),
            instrument_limits=(InstrumentQuantityLimit(APPLE, Decimal(account_cap)),),
            allocated_at=NOW,
        )

    def test_targets_are_summed_but_strategy_ownership_is_preserved(self):
        result = self.allocate((target("b", "beta", "4"), target("a", "alpha", "6")))
        self.assertEqual(result.account_targets[0].quantity, Decimal("10"))
        self.assertEqual(result.account_targets[0].component_target_ids, ("a", "b"))
        self.assertEqual(
            [(item.strategy_id, item.quantity) for item in result.virtual_targets],
            [("alpha", Decimal("6")), ("beta", Decimal("4"))],
        )
        self.assertEqual(
            [item.source_decision_id for item in result.virtual_targets],
            ["decision-a", "decision-b"],
        )

    def test_strategy_and_account_caps_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "strategy target exceeds"):
            self.allocate((target("a", "alpha", "7"),), account_cap="20")
        with self.assertRaisesRegex(ValueError, "combined strategy targets"):
            self.allocate((target("a", "alpha", "6"), target("b", "beta", "7")))

    def test_missing_or_ambiguous_policy_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "explicit quantity budget"):
            allocate_targets(
                account_id="paper-main",
                allocation_id="allocation-1",
                policy_version="allocation-v1",
                input_snapshot_id="snapshot-1",
                targets=(target("a", "alpha", "1"),),
                strategy_budgets=(),
                instrument_limits=(InstrumentQuantityLimit(APPLE, Decimal("2")),),
                allocated_at=NOW,
            )
        with self.assertRaisesRegex(ValueError, "duplicate strategy target"):
            self.allocate((target("a", "alpha", "1"), target("b", "alpha", "2")))

    def test_future_target_and_bool_like_policy_values_are_rejected(self):
        future = PositionTarget(
            "a", "alpha", "decision-a", "alpha-v1", "strategy-input-1",
            APPLE,
            Decimal("1"),
            TargetUnit.SHARES,
            datetime(2026, 8, 28, tzinfo=timezone.utc),
        )
        with self.assertRaisesRegex(ValueError, "future"):
            self.allocate((future,))
        with self.assertRaises(ValueError):
            StrategyQuantityBudget("alpha", APPLE, True)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
