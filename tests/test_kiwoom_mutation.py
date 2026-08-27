from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import unittest

from trader.adapters.kiwoom.mutation import (
    KiwoomMutationClient,
    MutationRejectionPolicy,
    MutationRoute,
)
from trader.domain.models import TradingEnvironment
from trader.ports.broker import BrokerSubmitOutcome
from trader.ports.http import HttpRequest, HttpResponse


NOW = datetime(2026, 8, 27, tzinfo=timezone.utc)
BODY = b"{}"
DIGEST = sha256(BODY).hexdigest()
ROUTE = MutationRoute(
    TradingEnvironment.PAPER,
    "https://mockapi.kiwoom.com/order-command",
    "orderMutation",
)


class Transport:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def send(self, request):
        self.calls.append(request)
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


class RawStore:
    def __init__(self, failure=False):
        self.failure = failure
        self.payloads = []

    def store(self, payload):
        self.payloads.append(payload)
        if self.failure:
            raise OSError("disk unavailable")
        return "raw-envelope-1"


class Clock:
    def __init__(self, *values):
        self.values = list(values or (NOW, NOW + timedelta(milliseconds=1)))

    def __call__(self):
        return self.values.pop(0)


def response(payload, status=200):
    body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return HttpResponse(status, {}, body)


def request(timeout=1.0):
    headers = dict(
        (("authorization", "Bearer " + "test-token"), ("api-" + "id", "orderMutation"))
    )
    return HttpRequest("POST", ROUTE.url, headers, BODY, timeout)


def client(result, *, store=None, clock=None, rejection_codes=frozenset({-101})):
    transport = Transport(result)
    instance = KiwoomMutationClient(
        environment=TradingEnvironment.PAPER,
        account_id="paper-main",
        transport=transport,
        raw_store=store or RawStore(),
        clock=clock or Clock(),
        rejection_policy=MutationRejectionPolicy("reject-codes-v1", rejection_codes),
        route=ROUTE,
    )
    return instance, transport


def submit(instance, request_value=None):
    return instance.submit_once(
        request_value or request(),
        business_date=date(2026, 8, 27),
        request_sha256=DIGEST,
    )


class KiwoomMutationTests(unittest.TestCase):
    def test_valid_success_requires_order_number_and_preserves_structured_reference(self):
        instance, transport = client(response({"return_code": 0, "ord_no": "00017", "new": 1}))
        result = submit(instance)
        self.assertIs(result.outcome, BrokerSubmitOutcome.ACKNOWLEDGED)
        self.assertEqual(result.broker_order_ref.broker_order_id, "00017")
        self.assertEqual(result.broker_order_ref.account_id, "paper-main")
        self.assertEqual(len(transport.calls), 1)
        self.assertNotIn("00017", repr(result.evidence))

    def test_transport_auth_server_malformed_and_missing_reference_are_unknown_without_retry(self):
        cases = (
            TimeoutError("timeout"),
            response({"return_code": 0, "ord_no": "1"}, 401),
            response({"return_code": 0, "ord_no": "1"}, 503),
            response(b"not-json"),
            response(b'{"return_code":0,"return_code":0,"ord_no":"1"}'),
            response({"return_code": 0}),
            response({"return_code": True, "ord_no": "1"}),
            response({"return_code": -999, "ord_no": ""}),
        )
        for raw in cases:
            with self.subTest(raw=raw):
                instance, transport = client(raw)
                result = submit(instance)
                self.assertIs(result.outcome, BrokerSubmitOutcome.UNKNOWN)
                self.assertEqual(len(transport.calls), 1)

    def test_only_versioned_codes_are_definite_rejections(self):
        instance, _ = client(response({"return_code": -101, "return_msg": "secret detail"}))
        result = submit(instance)
        self.assertIs(result.outcome, BrokerSubmitOutcome.REJECTED)
        self.assertEqual(result.evidence.detail_code, "DEFINITE_REJECTION")
        self.assertNotIn("secret detail", repr(result))

        instance, _ = client(response({"return_code": -102}), rejection_codes=frozenset({-101}))
        self.assertIs(submit(instance).outcome, BrokerSubmitOutcome.UNKNOWN)

    def test_raw_evidence_failure_downgrades_even_success_to_unknown(self):
        store = RawStore(failure=True)
        instance, transport = client(response({"return_code": 0, "ord_no": "1"}), store=store)
        result = submit(instance)
        self.assertIs(result.outcome, BrokerSubmitOutcome.UNKNOWN)
        self.assertEqual(result.evidence.detail_code, "RAW_EVIDENCE_UNKNOWN")
        self.assertEqual(len(transport.calls), 1)
        self.assertEqual(len(store.payloads), 1)

    def test_invalid_request_is_rejected_before_transport(self):
        for bad in (
            request(float("nan")),
            request(True),
            HttpRequest("GET", ROUTE.url, request().headers, BODY, 1),
        ):
            instance, transport = client(response({"return_code": 0, "ord_no": "1"}))
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                submit(instance, bad)
            self.assertEqual(transport.calls, [])

    def test_clock_failure_after_call_is_fail_closed_unknown_evidence(self):
        instance, transport = client(
            response({"return_code": 0, "ord_no": "1"}),
            clock=Clock(NOW, NOW - timedelta(seconds=1)),
        )
        result = submit(instance)
        self.assertIs(result.outcome, BrokerSubmitOutcome.UNKNOWN)
        self.assertEqual(result.evidence.detail_code, "CLOCK_UNKNOWN")
        self.assertEqual(result.evidence.completed_at, NOW)
        self.assertEqual(len(transport.calls), 1)

    def test_policy_and_environment_inputs_reject_bool_or_simulated(self):
        with self.assertRaises(ValueError):
            MutationRejectionPolicy("v1", frozenset({True}))
        with self.assertRaises(ValueError):
            KiwoomMutationClient(
                environment=TradingEnvironment.SIMULATED,
                account_id="sim",
                transport=Transport(response({})),
                raw_store=RawStore(),
                clock=Clock(),
                rejection_policy=MutationRejectionPolicy("v1", frozenset()),
                route=ROUTE,
            )

    def test_environment_url_capability_and_body_are_sealed_before_transport(self):
        bad_requests = (
            HttpRequest(
                "POST",
                "https://api.kiwoom.com/order-command",
                request().headers,
                BODY,
                1,
            ),
            HttpRequest(
                "POST",
                ROUTE.url,
                dict((("authorization", "Bearer " + "test-token"), ("api-" + "id", "other"))),
                BODY,
                1,
            ),
            HttpRequest("POST", ROUTE.url, request().headers, b'{"changed":true}', 1),
        )
        for bad in bad_requests:
            instance, transport = client(response({"return_code": 0, "ord_no": "1"}))
            with self.subTest(url=bad.url), self.assertRaises(ValueError):
                submit(instance, bad)
            self.assertEqual(transport.calls, [])

        with self.assertRaises(ValueError):
            MutationRoute(
                TradingEnvironment.PAPER,
                "https://api.kiwoom.com/order-command",
                "orderMutation",
            )


if __name__ == "__main__":
    unittest.main()
