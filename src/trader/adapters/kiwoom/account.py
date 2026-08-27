from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import math
import re
from typing import Any

from trader.domain.observations import (
    AccountObservation,
    AuthenticationObservation,
    CashBalance,
    CashObservation,
    DailyOrder,
    DailyOrdersObservation,
    ObservationQuality,
    Position,
    PositionsObservation,
    ResponseEvidence,
)
from trader.domain.models import TradingEnvironment
from trader.adapters.kiwoom.rate_limit import KiwoomReadonlyRateLimiter, QueryPriority
from trader.ports.account import AccountEnvironment, AccountProfile
from trader.ports.http import HttpRequest, HttpResponse, HttpTransport


_BASE_URLS = {
    AccountEnvironment.LIVE: "https://api.kiwoom.com",
    AccountEnvironment.MOCK: "https://mockapi.kiwoom.com",
}
_ACCOUNT_PATH = "/api/us/acnt"
_TRADING_ENVIRONMENTS = {
    AccountEnvironment.LIVE: TradingEnvironment.LIVE,
    AccountEnvironment.MOCK: TradingEnvironment.PAPER,
}
_DECIMAL_PATTERN = re.compile(r"[+-]?[0-9]+(?:\.[0-9]+)?", re.ASCII)
_INTEGER_PATTERN = re.compile(r"[0-9]{1,12}", re.ASCII)
_MAX_DECIMAL_TEXT_LENGTH = 64


@dataclass(frozen=True)
class KiwoomAccessToken:
    token: str = field(repr=False)
    token_type: str
    expires_dt: str
    started_at: datetime
    completed_at: datetime
    evidence: ResponseEvidence


class _ObservationFailure(Exception):
    def __init__(
        self,
        code: str,
        observed_at: datetime,
        evidence: tuple[ResponseEvidence, ...] = (),
        started_at: datetime | None = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.completed_at = observed_at
        self.evidence = evidence
        self.started_at = (
            started_at
            if started_at is not None
            else evidence[0].request_started_at
            if evidence
            else observed_at
        )

    @property
    def observed_at(self) -> datetime:
        return self.completed_at


class KiwoomReadonlyAccount:
    def __init__(
        self,
        profile: AccountProfile,
        transport: HttpTransport,
        clock: Callable[[], datetime],
        rate_limiter: KiwoomReadonlyRateLimiter,
    ) -> None:
        self._profile = profile
        self._transport = transport
        self._clock = clock
        self._rate_limiter = rate_limiter

    def issue_token(self, *, timeout_seconds: float) -> KiwoomAccessToken:
        _require_positive(timeout_seconds, "timeout_seconds")
        body = _json_bytes(
            {
                "grant_type": "client_credentials",
                "appkey": self._profile.app_key,
                "secretkey": self._profile.secret_key,
            }
        )
        response, started_at, observed_at = self._send(
            "/oauth2/token",
            {"api-id": "au10001", "content-type": "application/json;charset=UTF-8"},
            body,
            timeout_seconds,
        )
        evidence = _evidence(response.body, "", started_at, observed_at)
        payload = _validated_payload(response, observed_at, (evidence,))
        token = _text(payload, "token", observed_at, (evidence,))
        token_type = _text(payload, "token_type", observed_at, (evidence,))
        expires_dt = _text(payload, "expires_dt", observed_at, (evidence,))
        if token_type.casefold() != "bearer":
            raise _ObservationFailure("TOKEN_TYPE", observed_at, (evidence,))
        return KiwoomAccessToken(
            token, token_type, expires_dt, started_at, observed_at, evidence
        )

    def observe_account(
        self,
        *,
        timeout_seconds: float,
        page_cap: int,
        max_component_skew_seconds: float,
    ) -> AccountObservation:
        _require_positive(timeout_seconds, "timeout_seconds")
        if type(page_cap) is not int or page_cap <= 0:
            raise ValueError("page_cap must be a positive integer")
        _require_nonnegative(max_component_skew_seconds, "max_component_skew_seconds")

        try:
            token = self.issue_token(timeout_seconds=timeout_seconds)
        except _ObservationFailure as failure:
            authentication = AuthenticationObservation(
                ObservationQuality.INCOMPLETE,
                failure.started_at,
                failure.completed_at,
                failure.evidence,
                None,
                (failure.code,),
            )
            return self._authentication_failure(authentication)

        authentication = AuthenticationObservation(
            ObservationQuality.COMPLETE,
            token.started_at,
            token.completed_at,
            (token.evidence,),
            token.expires_dt,
        )
        headers = {
            "api-id": "",
            "authorization": f"Bearer {token.token}",
            "content-type": "application/json;charset=UTF-8",
        }
        positions = self._positions(headers, token.token, timeout_seconds, page_cap)
        cash = self._cash(headers, token.token, timeout_seconds, page_cap)
        orders = self._orders(headers, token.token, timeout_seconds, page_cap)

        error_codes = tuple(
            code
            for component in (positions, cash, orders)
            for code in component.error_codes
        )
        components = (positions, cash, orders)
        window_seconds = (
            max(item.completed_at for item in components)
            - min(item.started_at for item in components)
        ).total_seconds()
        if window_seconds > max_component_skew_seconds:
            error_codes += ("COMPONENT_WINDOW_EXCEEDED",)
        quality = (
            ObservationQuality.COMPLETE
            if not error_codes
            else ObservationQuality.INCOMPLETE
        )
        return AccountObservation(
            self._profile.account_id,
            _TRADING_ENVIRONMENTS[self._profile.environment],
            quality,
            authentication,
            positions,
            cash,
            orders,
            min(
                authentication.started_at,
                positions.started_at,
                cash.started_at,
                orders.started_at,
            ),
            max(
                authentication.completed_at,
                positions.completed_at,
                cash.completed_at,
                orders.completed_at,
            ),
            error_codes,
        )

    def _authentication_failure(
        self, authentication: AuthenticationObservation
    ) -> AccountObservation:
        started_at = authentication.started_at
        completed_at = authentication.completed_at
        positions = PositionsObservation(
            ObservationQuality.INCOMPLETE,
            started_at,
            completed_at,
            (),
            ("AUTHENTICATION",),
        )
        cash = CashObservation(
            ObservationQuality.INCOMPLETE,
            started_at,
            completed_at,
            (),
            ("AUTHENTICATION",),
        )
        orders = DailyOrdersObservation(
            ObservationQuality.INCOMPLETE,
            started_at,
            completed_at,
            (),
            ("AUTHENTICATION",),
        )
        return AccountObservation(
            self._profile.account_id,
            _TRADING_ENVIRONMENTS[self._profile.environment],
            ObservationQuality.INCOMPLETE,
            authentication,
            positions,
            cash,
            orders,
            started_at,
            completed_at,
            (authentication.error_codes[0],),
        )

    def _positions(
        self, headers: dict[str, str], token: str, timeout: float, page_cap: int
    ) -> PositionsObservation:
        result = self._pages("ust21070", {}, headers, token, timeout, page_cap)
        if isinstance(result, _ObservationFailure):
            return PositionsObservation(
                ObservationQuality.INCOMPLETE,
                result.started_at,
                result.completed_at,
                result.evidence,
                (result.code,),
            )
        pages, started_at, observed_at, evidence = result
        try:
            positions = tuple(
                Position(
                    _text(row, "stk_cd", observed_at, evidence),
                    _text(row, "crnc_code", observed_at, evidence),
                    _integer(row, "qty", observed_at, evidence),
                    _integer(row, "poss_qty", observed_at, evidence),
                    _integer(row, "sell_alowq", observed_at, evidence),
                )
                for page in pages
                for row in _rows(page, observed_at, evidence)
            )
        except (_ObservationFailure, ValueError) as failure:
            code = failure.code if isinstance(failure, _ObservationFailure) else "SCHEMA"
            return PositionsObservation(
                ObservationQuality.INCOMPLETE,
                started_at,
                observed_at,
                evidence,
                (code,),
            )
        return PositionsObservation(
            ObservationQuality.COMPLETE,
            started_at,
            observed_at,
            evidence,
            (),
            positions,
        )

    def _cash(
        self, headers: dict[str, str], token: str, timeout: float, page_cap: int
    ) -> CashObservation:
        result = self._pages("ust21110", {}, headers, token, timeout, page_cap)
        if isinstance(result, _ObservationFailure):
            return CashObservation(
                ObservationQuality.INCOMPLETE,
                result.started_at,
                result.completed_at,
                result.evidence,
                (result.code,),
            )
        pages, started_at, observed_at, evidence = result
        try:
            first = pages[0]
            balances = tuple(
                CashBalance(
                    _text(row, "crnc_code", observed_at, evidence),
                    _decimal(row, "fc_entra", observed_at, evidence),
                    _decimal(row, "fc_pymn_alowa", observed_at, evidence),
                    _decimal(row, "fc_ord_alowa", observed_at, evidence),
                )
                for page in pages
                for row in _rows(page, observed_at, evidence)
            )
            krw_cash = _decimal(first, "krw_entra", observed_at, evidence)
            unsettled = _decimal(first, "ch_uncla", observed_at, evidence)
            other_loans = _decimal(first, "etc_loana", observed_at, evidence)
        except (_ObservationFailure, ValueError) as failure:
            code = failure.code if isinstance(failure, _ObservationFailure) else "SCHEMA"
            return CashObservation(
                ObservationQuality.INCOMPLETE,
                started_at,
                observed_at,
                evidence,
                (code,),
            )
        return CashObservation(
            ObservationQuality.COMPLETE,
            started_at,
            observed_at,
            evidence,
            (),
            krw_cash,
            unsettled,
            other_loans,
            balances,
        )

    def _orders(
        self, headers: dict[str, str], token: str, timeout: float, page_cap: int
    ) -> DailyOrdersObservation:
        result = self._pages(
            "ust21150",
            {"query_tp": "1", "slby_tp": "0"},
            headers,
            token,
            timeout,
            page_cap,
        )
        if isinstance(result, _ObservationFailure):
            return DailyOrdersObservation(
                ObservationQuality.INCOMPLETE,
                result.started_at,
                result.completed_at,
                result.evidence,
                (result.code,),
            )
        pages, started_at, observed_at, evidence = result
        try:
            orders = tuple(
                DailyOrder(
                    _text(row, "ord_no", observed_at, evidence),
                    _text(row, "stk_cd", observed_at, evidence),
                    _text(row, "crnc_code", observed_at, evidence),
                    _text(row, "slby_tp_nm", observed_at, evidence),
                    _integer(row, "ord_qty", observed_at, evidence),
                    _integer(row, "cntr_qty", observed_at, evidence),
                    _integer(row, "mdfy_qty", observed_at, evidence),
                    _integer(row, "cncl_qty", observed_at, evidence),
                    _integer(row, "ord_remnq", observed_at, evidence),
                    _decimal(row, "ord_uv", observed_at, evidence),
                    _decimal(row, "cntr_uv", observed_at, evidence),
                    _text(row, "ord_stat_nm", observed_at, evidence),
                )
                for page in pages
                for row in _rows(page, observed_at, evidence)
            )
        except (_ObservationFailure, ValueError) as failure:
            code = failure.code if isinstance(failure, _ObservationFailure) else "SCHEMA"
            return DailyOrdersObservation(
                ObservationQuality.INCOMPLETE,
                started_at,
                observed_at,
                evidence,
                (code,),
            )
        return DailyOrdersObservation(
            ObservationQuality.COMPLETE,
            started_at,
            observed_at,
            evidence,
            (),
            orders,
        )

    def _pages(
        self,
        api_id: str,
        body: dict[str, str],
        base_headers: dict[str, str],
        token: str,
        timeout: float,
        page_cap: int,
    ) -> (
        tuple[
            list[dict[str, Any]],
            datetime,
            datetime,
            tuple[ResponseEvidence, ...],
        ]
        | _ObservationFailure
    ):
        pages: list[dict[str, Any]] = []
        evidence: list[ResponseEvidence] = []
        cursor = ""
        seen_cursors: set[str] = set()
        component_started_at: datetime | None = None
        observed_at = self._clock()
        for page_number in range(page_cap):
            headers = dict(base_headers)
            headers["api-id"] = api_id
            if cursor:
                headers["cont-yn"] = "Y"
                headers["next-key"] = cursor
            decision = self._rate_limiter.acquire(
                environment=self._profile.environment,
                token=token,
                api_id=api_id,
                priority=QueryPriority.RECONCILIATION,
            )
            if not decision.allowed:
                return _ObservationFailure(
                    "RATE_LIMITED",
                    self._clock(),
                    tuple(evidence),
                    component_started_at,
                )
            try:
                response, request_started_at, observed_at = self._send(
                    _ACCOUNT_PATH, headers, _json_bytes(body), timeout
                )
            except _ObservationFailure as failure:
                if component_started_at is not None and not failure.evidence:
                    failure = _ObservationFailure(
                        failure.code,
                        failure.completed_at,
                        failure.evidence,
                        component_started_at,
                    )
                return failure
            if component_started_at is None:
                component_started_at = request_started_at
            page_evidence = _evidence(
                response.body, cursor, request_started_at, observed_at
            )
            evidence.append(page_evidence)
            evidence_tuple = tuple(evidence)
            try:
                payload = _validated_payload(response, observed_at, evidence_tuple)
            except _ObservationFailure as failure:
                return failure
            pages.append(payload)
            if not isinstance(response.headers, Mapping) or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in response.headers.items()
            ):
                return _ObservationFailure("HTTP_SCHEMA", observed_at, evidence_tuple)
            response_headers = {key.casefold(): value for key, value in response.headers.items()}
            cont_yn = response_headers.get("cont-yn", "")
            next_key = response_headers.get("next-key", "")
            if not isinstance(cont_yn, str) or not isinstance(next_key, str):
                return _ObservationFailure("PAGINATION_SCHEMA", observed_at, evidence_tuple)
            if cont_yn in ("", "N"):
                if next_key:
                    return _ObservationFailure("PAGINATION_UNKNOWN", observed_at, evidence_tuple)
                return pages, component_started_at, observed_at, evidence_tuple
            if cont_yn != "Y":
                return _ObservationFailure("PAGINATION_UNKNOWN", observed_at, evidence_tuple)
            if not next_key:
                return _ObservationFailure("PAGINATION_CURSOR_MISSING", observed_at, evidence_tuple)
            if next_key in seen_cursors or next_key == cursor:
                return _ObservationFailure("PAGINATION_CURSOR_REPEAT", observed_at, evidence_tuple)
            if page_number + 1 == page_cap:
                return _ObservationFailure("PAGINATION_CAP", observed_at, evidence_tuple)
            seen_cursors.add(next_key)
            cursor = next_key
        raise AssertionError("positive page_cap loop must return")

    def _send(
        self,
        path: str,
        headers: Mapping[str, str],
        body: bytes,
        timeout: float,
    ) -> tuple[HttpResponse, datetime, datetime]:
        started_at = self._clock()
        try:
            response = self._transport.send(
                HttpRequest(
                    "POST",
                    _BASE_URLS[self._profile.environment] + path,
                    headers,
                    body,
                    timeout,
                )
            )
        except Exception as exc:
            failure = _ObservationFailure(
                "HTTP_TRANSPORT", self._clock(), started_at=started_at
            )
            raise failure from exc
        observed_at = self._clock()
        if type(response) is not HttpResponse:
            raise _ObservationFailure(
                "HTTP_SCHEMA", observed_at, started_at=started_at
            )
        if not isinstance(response.body, bytes):
            raise _ObservationFailure(
                "HTTP_SCHEMA", observed_at, started_at=started_at
            )
        return response, started_at, observed_at


def _validated_payload(
    response: HttpResponse,
    observed_at: datetime,
    evidence: tuple[ResponseEvidence, ...],
) -> dict[str, Any]:
    if type(response.status_code) is not int or not 200 <= response.status_code < 300:
        raise _ObservationFailure("HTTP_STATUS", observed_at, evidence)
    try:
        payload = json.loads(response.body, object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise _ObservationFailure("JSON_SCHEMA", observed_at, evidence) from exc
    if not isinstance(payload, dict):
        raise _ObservationFailure("JSON_SCHEMA", observed_at, evidence)
    return_code = payload.get("return_code")
    if type(return_code) is not int:
        raise _ObservationFailure("APP_CODE_SCHEMA", observed_at, evidence)
    if return_code != 0:
        raise _ObservationFailure("APP_FAILURE", observed_at, evidence)
    return payload


def _rows(
    payload: dict[str, Any],
    observed_at: datetime,
    evidence: tuple[ResponseEvidence, ...],
) -> list[dict[str, Any]]:
    rows = payload.get("result_list")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise _ObservationFailure("SCHEMA", observed_at, evidence)
    return rows


def _text(
    payload: Mapping[str, Any],
    key: str,
    observed_at: datetime,
    evidence: tuple[ResponseEvidence, ...],
) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise _ObservationFailure("SCHEMA", observed_at, evidence)
    return value


def _decimal(
    payload: Mapping[str, Any],
    key: str,
    observed_at: datetime,
    evidence: tuple[ResponseEvidence, ...],
) -> Decimal:
    value = payload.get(key)
    if (
        not isinstance(value, str)
        or len(value) > _MAX_DECIMAL_TEXT_LENGTH
        or _DECIMAL_PATTERN.fullmatch(value) is None
    ):
        raise _ObservationFailure("NUMERIC_SCHEMA", observed_at, evidence)
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise _ObservationFailure("NUMERIC_SCHEMA", observed_at, evidence) from exc
    if not result.is_finite():
        raise _ObservationFailure("NUMERIC_SCHEMA", observed_at, evidence)
    return result


def _integer(
    payload: Mapping[str, Any],
    key: str,
    observed_at: datetime,
    evidence: tuple[ResponseEvidence, ...],
) -> Decimal:
    value = payload.get(key)
    if not isinstance(value, str) or _INTEGER_PATTERN.fullmatch(value) is None:
        raise _ObservationFailure("NUMERIC_SCHEMA", observed_at, evidence)
    return Decimal(value)


def _json_bytes(payload: dict[str, str]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _evidence(
    raw: bytes,
    cursor: str,
    request_started_at: datetime,
    response_completed_at: datetime,
) -> ResponseEvidence:
    return ResponseEvidence(
        sha256(raw).hexdigest(),
        sha256(cursor.encode()).hexdigest(),
        request_started_at,
        response_completed_at,
    )


def _require_positive(value: float, name: str) -> None:
    if (
        type(value) not in (int, float)
        or (type(value) is float and not math.isfinite(value))
        or value <= 0
    ):
        raise ValueError(f"{name} must be positive")


def _require_nonnegative(value: float, name: str) -> None:
    if (
        type(value) not in (int, float)
        or (type(value) is float and not math.isfinite(value))
        or value < 0
    ):
        raise ValueError(f"{name} must be non-negative")
