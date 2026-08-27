from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import json
import unittest

from trader.adapters.kiwoom.account import KiwoomReadonlyAccount
from trader.adapters.kiwoom.rate_limit import (
    KiwoomReadonlyRateLimiter,
    RateLimitDecision,
    RateLimitReason,
    current_official_policy,
)
from trader.domain.observations import ObservationQuality, Position, PositionsObservation
from trader.domain.models import TradingEnvironment
from trader.ports.account import AccountEnvironment, AccountProfile
from trader.ports.http import HttpRequest, HttpResponse


def response(payload: object, headers: dict[str, str] | None = None) -> HttpResponse:
    return HttpResponse(
        200,
        headers or {},
        json.dumps(payload, separators=(",", ":")).encode(),
    )


def token_response(return_code: object = 0) -> HttpResponse:
    return response(
        {
            "expires_dt": "20261130235959",
            "token_type": "bearer",
            "token": "offline-token",
            "return_code": return_code,
        }
    )


def position_row(symbol: str, quantity: str = "1") -> dict[str, str]:
    return {
        "stk_cd": symbol,
        "crnc_code": "USD",
        "qty": quantity,
        "poss_qty": quantity,
        "sell_alowq": quantity,
    }


def cash_payload() -> dict[str, object]:
    return {
        "krw_entra": "1000",
        "ch_uncla": "0",
        "etc_loana": "0",
        "result_list": [
            {
                "crnc_code": "USD",
                "fc_entra": "12.34",
                "fc_pymn_alowa": "10.00",
                "fc_ord_alowa": "9.50",
            }
        ],
        "return_code": 0,
    }


def order_payload() -> dict[str, object]:
    return {
        "result_list": [
            {
                "ord_no": "000000252",
                "crnc_code": "USD",
                "stk_cd": "AAPL",
                "slby_tp_nm": "매수",
                "ord_qty": "1",
                "cntr_qty": "1",
                "mdfy_qty": "0",
                "cncl_qty": "0",
                "ord_remnq": "0",
                "ord_uv": "200.0000",
                "cntr_uv": "199.5000",
                "ord_stat_nm": "체결완료",
            }
        ],
        "return_code": 0,
    }


class QueueTransport:
    def __init__(self, responses: list[HttpResponse]) -> None:
        self.responses = responses
        self.requests: list[HttpRequest] = []

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        return self.responses.pop(0)


class TickClock:
    def __init__(self) -> None:
        self.current = datetime(2026, 8, 27, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        value = self.current
        self.current += timedelta(milliseconds=10)
        return value


class ScriptedClock:
    def __init__(self, offsets: list[int]) -> None:
        base = datetime(2026, 8, 27, tzinfo=timezone.utc)
        self.values = [base + timedelta(seconds=value) for value in offsets]

    def __call__(self) -> datetime:
        return self.values.pop(0)


class StepMonotonic:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        value = self.value
        self.value += 1.0
        return value


def rate_limiter() -> KiwoomReadonlyRateLimiter:
    return KiwoomReadonlyRateLimiter(
        current_official_policy(),
        StepMonotonic(),
        lambda: datetime(2026, 8, 27, tzinfo=timezone.utc),
    )


class RecordingLimiter:
    def __init__(self, allowed: bool = True) -> None:
        self.allowed = allowed
        self.api_ids: list[str] = []

    def acquire(self, **request: object) -> RateLimitDecision:
        self.api_ids.append(str(request["api_id"]))
        if self.allowed:
            return RateLimitDecision(True)
        return RateLimitDecision(False, RateLimitReason.ACCOUNT_QUERY, 1.0)


class KiwoomReadonlyTests(unittest.TestCase):
    def client(self, responses: list[HttpResponse]) -> tuple[KiwoomReadonlyAccount, QueueTransport]:
        transport = QueueTransport(responses)
        profile = AccountProfile(
            "internal-us-account", AccountEnvironment.MOCK, "test-app-key", "test-secret"
        )
        return KiwoomReadonlyAccount(
            profile, transport, TickClock(), rate_limiter()
        ), transport

    def test_observes_three_structured_components_with_pagination(self) -> None:
        first_positions = response(
            {"result_list": [position_row("AAPL")], "return_code": 0},
            {"cont-yn": "Y", "next-key": "cursor-2"},
        )
        second_positions = response(
            {"result_list": [position_row("NVDA", "000000000002")], "return_code": 0},
            {"cont-yn": "N"},
        )
        client, transport = self.client(
            [
                token_response(),
                first_positions,
                second_positions,
                response(cash_payload()),
                response(order_payload()),
            ]
        )

        observed = client.observe_account(
            timeout_seconds=2.5, page_cap=3, max_component_skew_seconds=1
        )

        self.assertEqual(observed.quality, ObservationQuality.COMPLETE)
        self.assertEqual(observed.environment, TradingEnvironment.PAPER)
        self.assertEqual(observed.authentication.expires_dt, "20261130235959")
        self.assertEqual([item.symbol for item in observed.positions.positions], ["AAPL", "NVDA"])
        self.assertEqual(observed.cash.balances[0].cash, Decimal("12.34"))
        self.assertEqual(observed.daily_orders.orders[0].broker_order_id, "000000252")
        self.assertEqual(len(transport.requests), 5)
        self.assertEqual(transport.requests[0].url, "https://mockapi.kiwoom.com/oauth2/token")
        self.assertEqual(transport.requests[0].headers["api-id"], "au10001")
        self.assertEqual(
            json.loads(transport.requests[0].body),
            {
                "grant_type": "client_credentials",
                "appkey": "test-app-key",
                "secretkey": "test-secret",
            },
        )
        self.assertEqual(transport.requests[2].headers["cont-yn"], "Y")
        self.assertEqual(transport.requests[2].headers["next-key"], "cursor-2")
        self.assertEqual(transport.requests[3].headers["api-id"], "ust21110")
        self.assertEqual(transport.requests[4].headers["api-id"], "ust21150")
        self.assertEqual(
            json.loads(transport.requests[4].body), {"query_tp": "1", "slby_tp": "0"}
        )
        self.assertEqual(
            observed.positions.evidence[0].raw_sha256,
            sha256(first_positions.body).hexdigest(),
        )
        self.assertEqual(
            observed.positions.evidence[1].cursor_sha256,
            sha256(b"cursor-2").hexdigest(),
        )
        self.assertEqual(
            observed.positions.started_at,
            observed.positions.evidence[0].request_started_at,
        )
        self.assertEqual(
            observed.positions.completed_at,
            observed.positions.evidence[-1].response_completed_at,
        )
        self.assertEqual(observed.started_at, observed.authentication.started_at)
        self.assertEqual(observed.completed_at, observed.daily_orders.completed_at)
        self.assertLess(
            observed.positions.evidence[0].response_completed_at,
            observed.positions.completed_at,
        )
        self.assertNotIn("test-secret", repr(client._profile))
        self.assertNotIn("test-app-key", repr(client._profile))

    def test_application_code_must_be_integer_zero_and_is_not_retried(self) -> None:
        client, transport = self.client([token_response("0")])

        observed = client.observe_account(
            timeout_seconds=1, page_cap=1, max_component_skew_seconds=1
        )

        self.assertEqual(observed.quality, ObservationQuality.INCOMPLETE)
        self.assertEqual(observed.authentication.error_codes, ("APP_CODE_SCHEMA",))
        self.assertEqual(len(transport.requests), 1)

    def test_duplicate_json_keys_fail_closed_without_retry(self) -> None:
        duplicate = HttpResponse(
            200,
            {},
            b'{"token":"offline-token","token_type":"bearer",'
            b'"expires_dt":"20261130235959","return_code":0,"return_code":0}',
        )
        client, transport = self.client([duplicate])

        observed = client.observe_account(
            timeout_seconds=1, page_cap=1, max_component_skew_seconds=1
        )

        self.assertEqual(observed.authentication.error_codes, ("JSON_SCHEMA",))
        self.assertEqual(len(transport.requests), 1)

    def test_live_profile_maps_to_live_trading_environment(self) -> None:
        transport = QueueTransport(
            [
                token_response(),
                response({"result_list": [], "return_code": 0}),
                response(cash_payload()),
                response(order_payload()),
            ]
        )
        client = KiwoomReadonlyAccount(
            AccountProfile("live-alias", AccountEnvironment.LIVE, "key", "secret"),
            transport,
            TickClock(),
            rate_limiter(),
        )
        observed = client.observe_account(
            timeout_seconds=1, page_cap=1, max_component_skew_seconds=1
        )
        self.assertEqual(observed.environment, TradingEnvironment.LIVE)
        self.assertEqual(transport.requests[0].url, "https://api.kiwoom.com/oauth2/token")

    def test_http_failure_is_hashed_and_component_queries_continue_without_retry(self) -> None:
        failed = HttpResponse(503, {}, b"temporarily unavailable")
        client, transport = self.client(
            [token_response(), failed, response(cash_payload()), response(order_payload())]
        )

        observed = client.observe_account(
            timeout_seconds=1, page_cap=1, max_component_skew_seconds=1
        )

        self.assertEqual(observed.positions.error_codes, ("HTTP_STATUS",))
        self.assertEqual(
            observed.positions.evidence[0].raw_sha256, sha256(failed.body).hexdigest()
        )
        self.assertEqual(len(transport.requests), 4)

    def test_misspelled_collection_and_bad_numeric_fail_closed(self) -> None:
        bad_positions = {"result_list": [position_row("AAPL", "NaN")], "return_code": 0}
        bad_orders = order_payload()
        bad_orders["result_lsit"] = bad_orders.pop("result_list")
        client, transport = self.client(
            [
                token_response(),
                response(bad_positions),
                response(cash_payload()),
                response(bad_orders),
            ]
        )

        observed = client.observe_account(
            timeout_seconds=1, page_cap=2, max_component_skew_seconds=1
        )

        self.assertEqual(observed.positions.error_codes, ("NUMERIC_SCHEMA",))
        self.assertEqual(observed.positions.positions, ())
        self.assertEqual(observed.daily_orders.error_codes, ("SCHEMA",))
        self.assertEqual(observed.daily_orders.orders, ())
        self.assertEqual(len(transport.requests), 4)

    def test_pagination_repeat_missing_unknown_and_cap_fail_closed(self) -> None:
        cases = (
            ({"cont-yn": "Y", "next-key": ""}, 2, "PAGINATION_CURSOR_MISSING"),
            ({"cont-yn": "M", "next-key": "x"}, 2, "PAGINATION_UNKNOWN"),
            ({"cont-yn": "Y", "next-key": "x"}, 1, "PAGINATION_CAP"),
        )
        for headers, cap, expected_code in cases:
            with self.subTest(expected_code):
                client, _ = self.client(
                    [
                        token_response(),
                        response({"result_list": [], "return_code": 0}, headers),
                        response(cash_payload()),
                        response(order_payload()),
                    ]
                )
                observed = client.observe_account(
                    timeout_seconds=1, page_cap=cap, max_component_skew_seconds=1
                )
                self.assertEqual(observed.positions.error_codes, (expected_code,))

        client, transport = self.client(
            [
                token_response(),
                response(
                    {"result_list": [], "return_code": 0},
                    {"cont-yn": "Y", "next-key": "x"},
                ),
                response(
                    {"result_list": [], "return_code": 0},
                    {"cont-yn": "Y", "next-key": "x"},
                ),
                response(cash_payload()),
                response(order_payload()),
            ]
        )
        observed = client.observe_account(
            timeout_seconds=1, page_cap=3, max_component_skew_seconds=1
        )
        self.assertEqual(observed.positions.error_codes, ("PAGINATION_CURSOR_REPEAT",))
        self.assertEqual(len(transport.requests), 5)

    def test_required_call_limits_are_validated_before_transport(self) -> None:
        client, transport = self.client([])
        for kwargs in (
            {"timeout_seconds": 0, "page_cap": 1, "max_component_skew_seconds": 1},
            {"timeout_seconds": False, "page_cap": 1, "max_component_skew_seconds": 1},
            {"timeout_seconds": float("nan"), "page_cap": 1, "max_component_skew_seconds": 1},
            {"timeout_seconds": float("inf"), "page_cap": 1, "max_component_skew_seconds": 1},
            {"timeout_seconds": 1, "page_cap": 0, "max_component_skew_seconds": 1},
            {"timeout_seconds": 1, "page_cap": False, "max_component_skew_seconds": 1},
            {"timeout_seconds": 1, "page_cap": -1, "max_component_skew_seconds": 1},
            {"timeout_seconds": 1, "page_cap": 1, "max_component_skew_seconds": -1},
            {"timeout_seconds": 1, "page_cap": 1, "max_component_skew_seconds": False},
            {"timeout_seconds": 1, "page_cap": 1, "max_component_skew_seconds": float("nan")},
            {"timeout_seconds": 1, "page_cap": 1, "max_component_skew_seconds": float("inf")},
        ):
            with self.subTest(kwargs), self.assertRaises(ValueError):
                client.observe_account(**kwargs)
        self.assertEqual(transport.requests, [])

    def test_integer_fields_reject_noncanonical_or_overwidth_values(self) -> None:
        for value in ("1.0", "+1", "-1", "1234567890123", " 1", "1e1", 1):
            with self.subTest(value=value):
                row = position_row("AAPL")
                row["qty"] = value  # type: ignore[assignment]
                client, _ = self.client(
                    [
                        token_response(),
                        response({"result_list": [row], "return_code": 0}),
                        response(cash_payload()),
                        response(order_payload()),
                    ]
                )
                observed = client.observe_account(
                    timeout_seconds=1, page_cap=1, max_component_skew_seconds=1
                )
                self.assertEqual(observed.positions.error_codes, ("NUMERIC_SCHEMA",))

    def test_decimal_fields_reject_non_ascii_or_noncanonical_values(self) -> None:
        bad_values: tuple[object, ...] = (
            " 1",
            "1 ",
            "1,000",
            "1e2",
            "NaN",
            "Infinity",
            "",
            "1" * 65,
            1.5,
        )
        for value in bad_values:
            with self.subTest(value=value):
                payload = cash_payload()
                payload["result_list"][0]["fc_entra"] = value  # type: ignore[index]
                client, _ = self.client(
                    [
                        token_response(),
                        response({"result_list": [position_row("AAPL")], "return_code": 0}),
                        response(payload),
                        response(order_payload()),
                    ]
                )
                observed = client.observe_account(
                    timeout_seconds=1, page_cap=1, max_component_skew_seconds=1
                )
                self.assertEqual(observed.cash.error_codes, ("NUMERIC_SCHEMA",))

    def test_component_skew_makes_only_the_composite_observation_incomplete(self) -> None:
        client, _ = self.client(
            [
                token_response(),
                response({"result_list": [], "return_code": 0}),
                response(cash_payload()),
                response(order_payload()),
            ]
        )
        observed = client.observe_account(
            timeout_seconds=1, page_cap=1, max_component_skew_seconds=0
        )
        self.assertEqual(observed.quality, ObservationQuality.INCOMPLETE)
        self.assertIn("COMPONENT_WINDOW_EXCEEDED", observed.error_codes)
        self.assertEqual(observed.daily_orders.quality, ObservationQuality.COMPLETE)
        self.assertFalse(observed.is_reconciliation_safe)

    def test_incomplete_component_cannot_expose_partial_records(self) -> None:
        with self.assertRaises(ValueError):
            PositionsObservation(
                ObservationQuality.INCOMPLETE,
                datetime(2026, 8, 27, tzinfo=timezone.utc),
                datetime(2026, 8, 27, tzinfo=timezone.utc),
                (),
                ("SCHEMA",),
                (Position("AAPL", "USD", Decimal(1), Decimal(1), Decimal(1)),),
            )

    def test_http_request_repr_never_contains_credentials_or_body(self) -> None:
        request = HttpRequest(
            "POST",
            "https://mockapi.kiwoom.com/oauth2/token",
            {"authorization": "Bearer test-token"},
            b'{"secretkey":"app-secret"}',
            1,
        )
        rendered = repr(request)
        self.assertNotIn("test-token", rendered)
        self.assertNotIn("app-secret", rendered)
        self.assertNotIn("authorization", rendered)
        self.assertNotIn("body", rendered)
        with self.assertRaises(RuntimeError) as raised:
            raise RuntimeError(f"transport failed for {request!r}")
        self.assertNotIn("test-token", str(raised.exception))
        self.assertNotIn("app-secret", str(raised.exception))

    def test_slow_multi_page_window_blocks_reconciliation(self) -> None:
        transport = QueueTransport(
            [
                token_response(),
                response(
                    {"result_list": [position_row("AAPL")], "return_code": 0},
                    {"cont-yn": "Y", "next-key": "next"},
                ),
                response(
                    {"result_list": [position_row("NVDA")], "return_code": 0}
                ),
                response(cash_payload()),
                response(order_payload()),
            ]
        )
        profile = AccountProfile(
            "internal-us-account", AccountEnvironment.MOCK, "key", "secret"
        )
        clock = ScriptedClock([0, 1, 2, 3, 4, 20, 21, 22, 23, 24, 25, 26, 27])
        client = KiwoomReadonlyAccount(profile, transport, clock, rate_limiter())

        observed = client.observe_account(
            timeout_seconds=1, page_cap=2, max_component_skew_seconds=5
        )

        self.assertEqual(observed.positions.started_at.second, 3)
        self.assertEqual(observed.positions.completed_at.second, 21)
        self.assertEqual(observed.positions.evidence[0].response_completed_at.second, 4)
        self.assertEqual(observed.positions.evidence[1].request_started_at.second, 20)
        self.assertEqual(observed.quality, ObservationQuality.INCOMPLETE)
        self.assertIn("COMPONENT_WINDOW_EXCEEDED", observed.error_codes)
        self.assertFalse(observed.is_reconciliation_safe)

    def test_rate_limit_denial_never_calls_account_transport(self) -> None:
        transport = QueueTransport([token_response()])
        limiter = RecordingLimiter(allowed=False)
        client = KiwoomReadonlyAccount(
            AccountProfile("account", AccountEnvironment.LIVE, "key", "secret"),
            transport,
            TickClock(),
            limiter,  # type: ignore[arg-type]
        )

        observed = client.observe_account(
            timeout_seconds=1, page_cap=2, max_component_skew_seconds=1
        )

        self.assertEqual(len(transport.requests), 1)
        self.assertEqual(observed.positions.error_codes, ("RATE_LIMITED",))
        self.assertEqual(observed.cash.error_codes, ("RATE_LIMITED",))
        self.assertEqual(observed.daily_orders.error_codes, ("RATE_LIMITED",))
        self.assertEqual(limiter.api_ids, ["ust21070", "ust21110", "ust21150"])

    def test_every_account_page_consumes_quota_but_token_issue_does_not(self) -> None:
        transport = QueueTransport(
            [
                token_response(),
                response(
                    {"result_list": [], "return_code": 0},
                    {"cont-yn": "Y", "next-key": "next"},
                ),
                response({"result_list": [], "return_code": 0}),
                response(cash_payload()),
                response(order_payload()),
            ]
        )
        limiter = RecordingLimiter()
        client = KiwoomReadonlyAccount(
            AccountProfile("account", AccountEnvironment.LIVE, "key", "secret"),
            transport,
            TickClock(),
            limiter,  # type: ignore[arg-type]
        )

        observed = client.observe_account(
            timeout_seconds=1, page_cap=2, max_component_skew_seconds=1
        )

        self.assertEqual(observed.quality, ObservationQuality.COMPLETE)
        self.assertEqual(
            limiter.api_ids, ["ust21070", "ust21070", "ust21110", "ust21150"]
        )


if __name__ == "__main__":
    unittest.main()
