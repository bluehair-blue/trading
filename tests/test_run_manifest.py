from datetime import datetime, timezone
import json
import unittest

from trader.research.manifest import RunManifest


NOW = datetime(2026, 8, 27, tzinfo=timezone.utc)


def manifest(**changes) -> RunManifest:
    values = {
        "run_id": "run-1",
        "code_commit": "e2bb483",
        "strategy_version": "strategy-v1",
        "config_sha256": "a" * 64,
        "data_snapshot_id": "bars-20260826",
        "universe_snapshot_id": "universe-20260826",
        "calendar_version": "calendar-v1",
        "corporate_action_version": "actions-v1",
        "fee_model_version": "fees-v1",
        "slippage_model_version": "slippage-v1",
        "fx_model_version": "fx-v1",
        "random_seed": 7,
        "decision_cutoff_policy": "previous-close-v1",
        "started_at": NOW,
        "completed_at": NOW,
    }
    values.update(changes)
    return RunManifest(**values)


class RunManifestTests(unittest.TestCase):
    def test_canonical_manifest_is_stable_and_fingerprinted(self):
        first = manifest()
        second = manifest()
        self.assertEqual(first.canonical_json(), second.canonical_json())
        self.assertEqual(first.fingerprint(), second.fingerprint())
        self.assertEqual(json.loads(first.canonical_json())["random_seed"], 7)
        self.assertEqual(len(first.fingerprint()), 64)

    def test_any_reproducibility_input_changes_the_fingerprint(self):
        self.assertNotEqual(manifest().fingerprint(), manifest(random_seed=8).fingerprint())
        self.assertNotEqual(
            manifest().fingerprint(), manifest(data_snapshot_id="bars-corrected").fingerprint()
        )

    def test_invalid_digest_revision_time_and_bool_seed_are_rejected(self):
        for changes in (
            {"config_sha256": "A" * 64},
            {"code_commit": "not-a-revision"},
            {"random_seed": True},
            {"completed_at": datetime(2026, 8, 26, tzinfo=timezone.utc)},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                manifest(**changes)


if __name__ == "__main__":
    unittest.main()
