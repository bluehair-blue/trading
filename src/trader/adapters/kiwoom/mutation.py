from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
import json
import re
from collections.abc import Mapping
from typing import Protocol
from urllib.parse import urlsplit

from trader.domain.broker_observations import BrokerOrderRef
from trader.domain.models import TradingEnvironment, require_enum, require_id, require_utc
from trader.ports.broker import BrokerSubmitOutcome
from trader.ports.http import HttpRequest, HttpResponse, HttpTransport


_SHA256 = re.compile(r"[0-9a-f]{64}")
_SAFE_CAPABILITY = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,63}")
_ENVIRONMENT_HOSTS = {
    TradingEnvironment.PAPER: "mockapi.kiwoom.com",
    TradingEnvironment.LIVE: "api.kiwoom.com",
}


class RawEnvelopeStore(Protocol):
    def store(self, payload: bytes) -> str: ...


@dataclass(frozen=True)
class MutationRoute:
    environment: TradingEnvironment
    url: str
    api_id: str

    def __post_init__(self) -> None:
        require_enum(self.environment, TradingEnvironment, "environment")
        if self.environment is TradingEnvironment.SIMULATED:
            raise ValueError("Kiwoom mutation route must be PAPER or LIVE")
        if not isinstance(self.url, str):
            raise ValueError("mutation route URL must be text")
        parsed = urlsplit(self.url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != _ENVIRONMENT_HOSTS[self.environment]
            or parsed.port is not None
            or parsed.username is not None
            or parsed.password is not None
            or not parsed.path.startswith("/")
            or parsed.path == "/"
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("mutation route does not match its broker environment")
        if _SAFE_CAPABILITY.fullmatch(self.api_id) is None:
            raise ValueError("mutation route api_id is invalid")


@dataclass(frozen=True)
class MutationRejectionPolicy:
    version: str
    definite_return_codes: frozenset[int]

    def __post_init__(self) -> None:
        require_id(self.version, "version")
        if type(self.definite_return_codes) is not frozenset or any(
            type(code) is not int for code in self.definite_return_codes
        ):
            raise ValueError("definite_return_codes must be a frozenset of exact integers")
        if 0 in self.definite_return_codes:
            raise ValueError("success cannot be configured as a rejection")


@dataclass(frozen=True)
class MutationAttemptEvidence:
    api_id: str
    policy_version: str
    request_sha256: str
    response_sha256: str | None
    raw_response_reference: str | None
    http_status: int | None
    return_code: int | None
    started_at: datetime
    completed_at: datetime
    detail_code: str

    def __post_init__(self) -> None:
        if _SAFE_CAPABILITY.fullmatch(self.api_id) is None:
            raise ValueError("api_id is invalid")
        require_id(self.policy_version, "policy_version")
        for name in ("request_sha256", "response_sha256"):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, str) or _SHA256.fullmatch(value) is None
            ):
                raise ValueError(f"{name} must be a lowercase SHA256 digest")
        if self.raw_response_reference is not None:
            require_id(self.raw_response_reference, "raw_response_reference")
        if self.http_status is not None and type(self.http_status) is not int:
            raise ValueError("http_status must be an exact integer")
        if self.return_code is not None and type(self.return_code) is not int:
            raise ValueError("return_code must be an exact integer")
        require_utc(self.started_at, "started_at")
        require_utc(self.completed_at, "completed_at")
        if self.completed_at < self.started_at:
            raise ValueError("mutation evidence clock moved backwards")
        if re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", self.detail_code) is None:
            raise ValueError("detail_code must be a bounded safe code")


@dataclass(frozen=True)
class MutationSubmissionResult:
    outcome: BrokerSubmitOutcome
    evidence: MutationAttemptEvidence
    broker_order_ref: BrokerOrderRef | None = None

    def __post_init__(self) -> None:
        require_enum(self.outcome, BrokerSubmitOutcome, "outcome")
        if type(self.evidence) is not MutationAttemptEvidence:
            raise ValueError("evidence must be exact MutationAttemptEvidence")
        acknowledged = self.outcome is BrokerSubmitOutcome.ACKNOWLEDGED
        if acknowledged != (self.broker_order_ref is not None):
            raise ValueError("only acknowledged mutation can carry a broker order reference")


class KiwoomMutationClient:
    """Classify one injected mutation call. This client never retries or refreshes auth."""

    def __init__(
        self,
        *,
        environment: TradingEnvironment,
        account_id: str,
        transport: HttpTransport,
        raw_store: RawEnvelopeStore,
        clock,
        rejection_policy: MutationRejectionPolicy,
        route: MutationRoute,
    ) -> None:
        require_enum(environment, TradingEnvironment, "environment")
        if environment is TradingEnvironment.SIMULATED:
            raise ValueError("Kiwoom mutation environment must be PAPER or LIVE")
        require_id(account_id, "account_id")
        if type(rejection_policy) is not MutationRejectionPolicy:
            raise ValueError("rejection_policy must be exact MutationRejectionPolicy")
        if type(route) is not MutationRoute or route.environment is not environment:
            raise ValueError("mutation route environment does not match client")
        self.environment = environment
        self.account_id = account_id
        self._transport = transport
        self._raw_store = raw_store
        self._clock = clock
        self._rejection_policy = rejection_policy
        self._route = route

    def submit_once(
        self,
        request: HttpRequest,
        *,
        business_date: date,
        request_sha256: str,
    ) -> MutationSubmissionResult:
        _validate_request(request, self._route, business_date, request_sha256)
        api_id = self._route.api_id
        started_at = self._clock()
        require_utc(started_at, "started_at")
        try:
            response = self._transport.send(request)
        except BaseException:
            completed_at, _ = _safe_completion(self._clock, started_at)
            return self._result(
                BrokerSubmitOutcome.UNKNOWN,
                api_id,
                request_sha256,
                started_at,
                completed_at,
                "TRANSPORT_UNKNOWN",
            )

        completed_at, clock_healthy = _safe_completion(self._clock, started_at)
        if type(response) is not HttpResponse or not isinstance(response.body, bytes):
            return self._result(
                BrokerSubmitOutcome.UNKNOWN,
                api_id,
                request_sha256,
                started_at,
                completed_at,
                "RESPONSE_SCHEMA_UNKNOWN",
            )
        response_sha256 = sha256(response.body).hexdigest()
        try:
            raw_reference = self._raw_store.store(response.body)
            require_id(raw_reference, "raw_response_reference")
        except BaseException:
            return self._result(
                BrokerSubmitOutcome.UNKNOWN,
                api_id,
                request_sha256,
                started_at,
                completed_at,
                "RAW_EVIDENCE_UNKNOWN",
                response_sha256=response_sha256,
                http_status=_safe_status(response.status_code),
            )
        status = _safe_status(response.status_code)
        if not clock_healthy:
            return self._result(
                BrokerSubmitOutcome.UNKNOWN,
                api_id,
                request_sha256,
                started_at,
                completed_at,
                "CLOCK_UNKNOWN",
                response_sha256,
                raw_reference,
                status,
            )
        if status is None or not 200 <= status < 300:
            return self._result(
                BrokerSubmitOutcome.UNKNOWN,
                api_id,
                request_sha256,
                started_at,
                completed_at,
                "HTTP_STATUS_UNKNOWN",
                response_sha256,
                raw_reference,
                status,
            )
        try:
            payload = json.loads(response.body, object_pairs_hook=_unique_object)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            return self._result(
                BrokerSubmitOutcome.UNKNOWN,
                api_id,
                request_sha256,
                started_at,
                completed_at,
                "MALFORMED_RESPONSE_UNKNOWN",
                response_sha256,
                raw_reference,
                status,
            )
        if not isinstance(payload, dict) or type(payload.get("return_code")) is not int:
            return self._result(
                BrokerSubmitOutcome.UNKNOWN,
                api_id,
                request_sha256,
                started_at,
                completed_at,
                "RETURN_CODE_UNKNOWN",
                response_sha256,
                raw_reference,
                status,
            )
        return_code = payload["return_code"]
        if return_code == 0:
            order_id = payload.get("ord_no")
            if not _valid_order_id(order_id):
                return self._result(
                    BrokerSubmitOutcome.UNKNOWN,
                    api_id,
                    request_sha256,
                    started_at,
                    completed_at,
                    "ORDER_REFERENCE_UNKNOWN",
                    response_sha256,
                    raw_reference,
                    status,
                    return_code,
                )
            evidence = self._evidence(
                api_id,
                request_sha256,
                started_at,
                completed_at,
                "ACKNOWLEDGED",
                response_sha256,
                raw_reference,
                status,
                return_code,
            )
            return MutationSubmissionResult(
                BrokerSubmitOutcome.ACKNOWLEDGED,
                evidence,
                BrokerOrderRef(
                    self.environment, self.account_id, business_date, order_id
                ),
            )
        if return_code in self._rejection_policy.definite_return_codes:
            return self._result(
                BrokerSubmitOutcome.REJECTED,
                api_id,
                request_sha256,
                started_at,
                completed_at,
                "DEFINITE_REJECTION",
                response_sha256,
                raw_reference,
                status,
                return_code,
            )
        return self._result(
            BrokerSubmitOutcome.UNKNOWN,
            api_id,
            request_sha256,
            started_at,
            completed_at,
            "UNCLASSIFIED_RETURN_CODE",
            response_sha256,
            raw_reference,
            status,
            return_code,
        )

    def _result(
        self,
        outcome: BrokerSubmitOutcome,
        api_id: str,
        request_sha256: str,
        started_at: datetime,
        completed_at: datetime,
        detail_code: str,
        response_sha256: str | None = None,
        raw_reference: str | None = None,
        http_status: int | None = None,
        return_code: int | None = None,
    ) -> MutationSubmissionResult:
        return MutationSubmissionResult(
            outcome,
            self._evidence(
                api_id,
                request_sha256,
                started_at,
                completed_at,
                detail_code,
                response_sha256,
                raw_reference,
                http_status,
                return_code,
            ),
        )

    def _evidence(
        self,
        api_id: str,
        request_sha256: str,
        started_at: datetime,
        completed_at: datetime,
        detail_code: str,
        response_sha256: str | None,
        raw_reference: str | None,
        http_status: int | None,
        return_code: int | None,
    ) -> MutationAttemptEvidence:
        return MutationAttemptEvidence(
            api_id,
            self._rejection_policy.version,
            request_sha256,
            response_sha256,
            raw_reference,
            http_status,
            return_code,
            started_at,
            completed_at,
            detail_code,
        )


def _validate_request(
    request: HttpRequest,
    route: MutationRoute,
    business_date: date,
    request_sha256: str,
) -> None:
    if type(request) is not HttpRequest:
        raise ValueError("request must be exact HttpRequest")
    if request.method != "POST" or request.url != route.url:
        raise ValueError("mutation request does not match its sealed route")
    if not isinstance(request.headers, Mapping) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in request.headers.items()
    ):
        raise ValueError("request headers must be a string mapping")
    normalized_headers: dict[str, str] = {}
    for key, value in request.headers.items():
        normalized_key = key.casefold()
        if normalized_key in normalized_headers:
            raise ValueError("request contains duplicate case-insensitive headers")
        normalized_headers[normalized_key] = value
    authorization = normalized_headers.get("authorization", "")
    if (
        normalized_headers.get("api-id") != route.api_id
        or not authorization.startswith("Bearer ")
        or not authorization.removeprefix("Bearer ").strip()
    ):
        raise ValueError("mutation request capability headers do not match its route")
    if not isinstance(request.body, bytes) or not request.body:
        raise ValueError("request body must be non-empty bytes")
    if (
        type(request.timeout_seconds) not in (int, float)
        or isinstance(request.timeout_seconds, bool)
        or not 0 < request.timeout_seconds < float("inf")
    ):
        raise ValueError("timeout_seconds must be finite and positive")
    if type(business_date) is not date:
        raise ValueError("business_date must be an exact date")
    if not isinstance(request_sha256, str) or _SHA256.fullmatch(request_sha256) is None:
        raise ValueError("request_sha256 must be a lowercase SHA256 digest")
    if request_sha256 != sha256(request.body).hexdigest():
        raise ValueError("request_sha256 does not match the exact request body")


def _safe_completion(clock, started_at: datetime) -> tuple[datetime, bool]:
    try:
        completed_at = clock()
        require_utc(completed_at, "completed_at")
    except BaseException:
        return started_at, False
    if completed_at < started_at:
        return started_at, False
    return completed_at, True


def _safe_status(status: object) -> int | None:
    return status if type(status) is int else None


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _valid_order_id(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value.strip())
        and len(value) <= 128
        and not any(ord(character) < 32 for character in value)
    )
