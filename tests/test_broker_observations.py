from datetime import date, datetime, timezone
import unittest

from trader.domain.broker_observations import (
    BrokerOrderLinked,
    BrokerOrderRef,
    ConfirmedAbsent,
    ManualActivityLinked,
    ResolutionQueryEvidence,
    canonical_resolution_payload,
    resolution_from_payload,
)
from trader.domain.models import TradingEnvironment, UnknownResolutionResult


NOW = datetime(2026, 8, 27, tzinfo=timezone.utc)


def query(*, candidates=1, complete=True, environment=TradingEnvironment.PAPER):
    refs = () if candidates == 0 else tuple(
        BrokerOrderRef(environment, "paper-main", date(2026, 8, 27), f"{index:06d}")
        for index in range(1, candidates + 1)
    )
    return ResolutionQueryEvidence(
        environment, "paper-main", date(2026, 8, 27), NOW, NOW,
        "unknown-resolution-v1", ("broker.orders.read",),
        ("broker.orders.read",), complete,
        ("observation-1",), "a" * 64, refs, NOW,
    )


def reference(environment=TradingEnvironment.PAPER):
    return BrokerOrderRef(environment, "paper-main", date(2026, 8, 27), "000001")


class BrokerObservationTests(unittest.TestCase):
    def test_each_resolution_variant_is_typed_and_decisive(self):
        linked = BrokerOrderLinked(reference(), "broker.orders.read", query())
        absent = ConfirmedAbsent(query(candidates=0))
        manual = ManualActivityLinked(reference(), "manual-1", "operator", NOW, query())
        self.assertIs(linked.result, UnknownResolutionResult.BROKER_ORDER_LINKED)
        self.assertIs(absent.result, UnknownResolutionResult.CONFIRMED_ABSENT)
        self.assertIs(manual.result, UnknownResolutionResult.MANUAL_ACTIVITY_LINKED)

    def test_zero_multiple_and_incomplete_queries_cannot_be_misclassified(self):
        for evidence in (query(candidates=0), query(candidates=2), query(complete=False)):
            with self.subTest(evidence=evidence), self.assertRaises(ValueError):
                BrokerOrderLinked(reference(), "broker.orders.read", evidence)
        for evidence in (query(candidates=1), query(candidates=2), query(complete=False)):
            with self.subTest(evidence=evidence), self.assertRaises(ValueError):
                ConfirmedAbsent(evidence)

    def test_environment_and_account_provenance_must_match(self):
        with self.assertRaisesRegex(ValueError, "canonical query candidate"):
            BrokerOrderLinked(
                reference(TradingEnvironment.LIVE), "broker.orders.read", query()
            )
        with self.assertRaises(ValueError):
            BrokerOrderRef(TradingEnvironment.PAPER, "paper-main", NOW, "1")  # type: ignore[arg-type]

    def test_non_tuple_candidates_and_bad_hash_are_rejected(self):
        values = vars(query()).copy()
        values["candidates"] = [reference()]
        with self.assertRaises(ValueError):
            ResolutionQueryEvidence(**values)
        values = vars(query()).copy()
        values["response_sha256"] = "A" * 64
        with self.assertRaises(ValueError):
            ResolutionQueryEvidence(**values)

    def test_canonical_payload_round_trips_each_exact_variant(self):
        variants = (
            ConfirmedAbsent(query(candidates=0)),
            BrokerOrderLinked(reference(), "broker.orders.read", query()),
            ManualActivityLinked(reference(), "manual-1", "operator", NOW, query()),
        )
        for resolution in variants:
            with self.subTest(result=resolution.result):
                payload = canonical_resolution_payload("command-1", resolution)
                self.assertEqual(resolution_from_payload(payload), resolution)
                expected_count = 0 if type(resolution) is ConfirmedAbsent else 1
                self.assertEqual(payload["query"]["candidate_count"], expected_count)

    def test_broker_account_number_is_not_an_internal_alias(self):
        with self.assertRaisesRegex(ValueError, "internal alias"):
            BrokerOrderRef(
                TradingEnvironment.PAPER, "1234567890", date(2026, 8, 27), "1"
            )
        values = vars(query()).copy()
        values["account_id"] = "1234567890"
        with self.assertRaisesRegex(ValueError, "internal alias"):
            ResolutionQueryEvidence(**values)

    def test_policy_window_candidate_membership_and_derived_hash_are_bound(self):
        values = vars(query()).copy()
        values["query_policy_version"] = "caller-invented-v1"
        with self.assertRaisesRegex(ValueError, "policy"):
            ResolutionQueryEvidence(**values)

        values = vars(query()).copy()
        values["required_source_capabilities"] = ("broker.fills.read",)
        values["queried_api_ids"] = ("broker.fills.read",)
        with self.assertRaisesRegex(ValueError, "policy"):
            ResolutionQueryEvidence(**values)

        with self.assertRaisesRegex(ValueError, "canonical query candidate"):
            BrokerOrderLinked(
                BrokerOrderRef(
                    TradingEnvironment.PAPER, "paper-main", date(2026, 8, 27), "other"
                ),
                "broker.orders.read",
                query(),
            )

        values = vars(query()).copy()
        values["business_date"] = date(2026, 8, 26)
        with self.assertRaisesRegex(ValueError, "outside"):
            ResolutionQueryEvidence(**values)

        payload = canonical_resolution_payload(
            "command-1", BrokerOrderLinked(
                reference(), "broker.orders.read", query(),
            )
        )
        payload["query"]["candidate_set_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "candidate summary"):
            resolution_from_payload(payload)


if __name__ == "__main__":
    unittest.main()
