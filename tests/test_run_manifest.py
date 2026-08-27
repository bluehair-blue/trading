from datetime import datetime, timedelta, timezone
import json
import unittest

from trader.research.manifest import RunResult, RunSpec, RunStatus


NOW = datetime(2026, 8, 27, tzinfo=timezone.utc)


def spec(**changes: object) -> RunSpec:
    values: dict[str, object] = {
        "code_commit": "e2bb483",
        "strategy_version": "strategy-v1",
        "config_sha256": "a" * 64,
        "account_seed_sha256": "d" * 64,
        "data_snapshot_id": "bars-20260826",
        "universe_snapshot_id": "universe-20260826",
        "calendar_version": "calendar-v1",
        "corporate_action_version": "actions-v1",
        "fee_model_version": "fees-v1",
        "slippage_model_version": "slippage-v1",
        "fx_model_version": "fx-v1",
        "accounting_model_version": "accounting-v1",
        "random_seed": 7,
        "decision_cutoff_policy": "previous-close-v1",
        "sample_started_at": NOW,
        "sample_completed_at": NOW + timedelta(days=30),
    }
    values.update(changes)
    return RunSpec(**values)  # type: ignore[arg-type]


class RunSpecTests(unittest.TestCase):
    def test_run_spec_is_stable_and_excludes_run_identity_and_wall_time(self) -> None:
        first = spec()
        second = spec()
        self.assertEqual(first.canonical_json(), second.canonical_json())
        self.assertEqual(first.fingerprint(), second.fingerprint())
        self.assertEqual(json.loads(first.canonical_json())["random_seed"], 7)
        self.assertEqual(len(first.fingerprint()), 64)

    def test_any_reproducibility_input_changes_the_fingerprint(self) -> None:
        self.assertNotEqual(spec().fingerprint(), spec(random_seed=8).fingerprint())
        self.assertNotEqual(
            spec().fingerprint(), spec(data_snapshot_id="bars-corrected").fingerprint()
        )
        self.assertNotEqual(
            spec().fingerprint(), spec(account_seed_sha256="e" * 64).fingerprint()
        )

    def test_result_links_success_or_failure_to_the_exact_spec(self) -> None:
        fingerprint = spec().fingerprint()
        succeeded = RunResult(
            "run-1",
            fingerprint,
            RunStatus.SUCCEEDED,
            NOW,
            NOW,
            "b" * 64,
            "c" * 64,
        )
        failed = RunResult(
            "run-2", fingerprint, RunStatus.FAILED, NOW, NOW, failure_code="DATA_GAP"
        )
        self.assertEqual(json.loads(succeeded.canonical_json())["status"], "SUCCEEDED")
        self.assertEqual(failed.failure_code, "DATA_GAP")

    def test_invalid_spec_and_result_contracts_are_rejected(self) -> None:
        for changes in (
            {"config_sha256": "A" * 64},
            {"code_commit": "not-a-revision"},
            {"random_seed": True},
            {"sample_completed_at": NOW - timedelta(seconds=1)},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                spec(**changes)
        fingerprint = spec().fingerprint()
        invalid_results = (
            (RunStatus.SUCCEEDED, None, None, None),
            (RunStatus.SUCCEEDED, "b" * 64, "c" * 64, "FAILED_ANYWAY"),
            (RunStatus.FAILED, None, None, None),
        )
        for status, ledger, output, failure in invalid_results:
            with self.subTest(status=status, ledger=ledger, output=output, failure=failure):
                with self.assertRaises(ValueError):
                    RunResult(
                        "run-1", fingerprint, status, NOW, NOW,
                        ledger_sha256=ledger,
                        output_sha256=output,
                        failure_code=failure,
                    )


if __name__ == "__main__":
    unittest.main()
