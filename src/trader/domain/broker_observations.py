from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import re

from trader.domain.models import (
    TradingEnvironment,
    UnknownResolutionResult,
    require_enum,
    require_id,
    require_utc,
)


_SHA256 = re.compile(r"[0-9a-f]{64}")
_SAFE_SOURCE_ID = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,63}")
_BROKER_ACCOUNT_NUMBER = re.compile(r"[0-9-]{8,14}")
RESOLUTION_QUERY_POLICIES = {
    "unknown-resolution-v1": ("broker.orders.read",),
}


def _require_internal_account_alias(value: str) -> None:
    require_id(value, "account_id")
    if _BROKER_ACCOUNT_NUMBER.fullmatch(value) and len(value.replace("-", "")) >= 8:
        raise ValueError("account_id must be an internal alias, not a broker account number")


@dataclass(frozen=True)
class BrokerOrderRef:
    environment: TradingEnvironment
    account_id: str
    business_date: date
    broker_order_id: str

    def __post_init__(self) -> None:
        require_enum(self.environment, TradingEnvironment, "environment")
        _require_internal_account_alias(self.account_id)
        if type(self.business_date) is not date:
            raise ValueError("business_date must be an exact date")
        require_id(self.broker_order_id, "broker_order_id")
        if len(self.broker_order_id) > 128 or any(
            ord(character) < 32 for character in self.broker_order_id
        ):
            raise ValueError("broker_order_id must be bounded printable text")


@dataclass(frozen=True)
class ResolutionQueryEvidence:
    environment: TradingEnvironment
    account_id: str
    business_date: date
    window_started_at: datetime
    window_completed_at: datetime
    query_policy_version: str
    required_source_capabilities: tuple[str, ...]
    queried_api_ids: tuple[str, ...]
    pagination_complete: bool
    observation_ids: tuple[str, ...]
    response_sha256: str
    candidates: tuple[BrokerOrderRef, ...]
    fetched_at: datetime

    def __post_init__(self) -> None:
        require_enum(self.environment, TradingEnvironment, "environment")
        _require_internal_account_alias(self.account_id)
        if type(self.business_date) is not date:
            raise ValueError("business_date must be an exact date")
        require_utc(self.window_started_at, "window_started_at")
        require_utc(self.window_completed_at, "window_completed_at")
        require_utc(self.fetched_at, "fetched_at")
        if not self.window_started_at <= self.window_completed_at <= self.fetched_at:
            raise ValueError("resolution evidence times are not ordered")
        if not (
            self.window_started_at.date()
            <= self.business_date
            <= self.window_completed_at.date()
        ):
            raise ValueError("query business_date is outside the query window")
        if type(self.pagination_complete) is not bool:
            raise ValueError("pagination_complete must be bool")
        require_id(self.query_policy_version, "query_policy_version")
        if self.query_policy_version not in RESOLUTION_QUERY_POLICIES:
            raise ValueError("unknown resolution query policy version")
        if (
            type(self.required_source_capabilities) is not tuple
            or not self.required_source_capabilities
            or tuple(sorted(self.required_source_capabilities))
            != self.required_source_capabilities
            or len(set(self.required_source_capabilities))
            != len(self.required_source_capabilities)
        ):
            raise ValueError("required source capabilities must be canonical and unique")
        if type(self.queried_api_ids) is not tuple or not self.queried_api_ids or len(set(self.queried_api_ids)) != len(
            self.queried_api_ids
        ):
            raise ValueError("queried_api_ids must be non-empty and unique")
        if any(_SAFE_SOURCE_ID.fullmatch(value) is None for value in self.queried_api_ids):
            raise ValueError("queried_api_ids contain an invalid identifier")
        if self.queried_api_ids != self.required_source_capabilities:
            raise ValueError("query must cover the exact required source capability set")
        if self.required_source_capabilities != RESOLUTION_QUERY_POLICIES[
            self.query_policy_version
        ]:
            raise ValueError("source capabilities do not match the query policy")
        if type(self.observation_ids) is not tuple or not self.observation_ids or len(set(self.observation_ids)) != len(
            self.observation_ids
        ):
            raise ValueError("observation_ids must be non-empty and unique")
        for observation_id in self.observation_ids:
            require_id(observation_id, "observation_id")
        if not isinstance(self.response_sha256, str) or _SHA256.fullmatch(
            self.response_sha256
        ) is None:
            raise ValueError("response_sha256 must be a lowercase SHA256 digest")
        if type(self.candidates) is not tuple or any(
            type(candidate) is not BrokerOrderRef for candidate in self.candidates
        ):
            raise ValueError("candidates must be exact BrokerOrderRef values")
        if len(set(self.candidates)) != len(self.candidates):
            raise ValueError("candidate references must be unique")
        if tuple(sorted(self.candidates, key=_broker_ref_sort_key)) != self.candidates:
            raise ValueError("candidate references must be canonically ordered")
        if any(
            candidate.environment is not self.environment
            or candidate.account_id != self.account_id
            or candidate.business_date != self.business_date
            for candidate in self.candidates
        ):
            raise ValueError("candidate provenance does not match the query")

    @property
    def candidate_count(self) -> int:
        return len(self.candidates)

    @property
    def candidate_set_sha256(self) -> str:
        encoded = json.dumps(
            [_broker_ref_payload(candidate) for candidate in self.candidates],
            sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class BrokerOrderLinked:
    broker_order_ref: BrokerOrderRef
    source_api_id: str
    evidence: ResolutionQueryEvidence

    def __post_init__(self) -> None:
        if type(self.broker_order_ref) is not BrokerOrderRef:
            raise ValueError("broker_order_ref must be exact BrokerOrderRef")
        if type(self.evidence) is not ResolutionQueryEvidence:
            raise ValueError("evidence must be exact ResolutionQueryEvidence")
        if _SAFE_SOURCE_ID.fullmatch(self.source_api_id) is None:
            raise ValueError("source_api_id is invalid")
        if self.source_api_id not in self.evidence.queried_api_ids:
            raise ValueError("source_api_id was not part of the query evidence")
        _require_resolution_match(self.broker_order_ref, self.evidence, 1)

    @property
    def result(self) -> UnknownResolutionResult:
        return UnknownResolutionResult.BROKER_ORDER_LINKED


@dataclass(frozen=True)
class ConfirmedAbsent:
    evidence: ResolutionQueryEvidence

    def __post_init__(self) -> None:
        if type(self.evidence) is not ResolutionQueryEvidence:
            raise ValueError("evidence must be exact ResolutionQueryEvidence")
        _require_complete_candidates(self.evidence, 0)

    @property
    def result(self) -> UnknownResolutionResult:
        return UnknownResolutionResult.CONFIRMED_ABSENT


@dataclass(frozen=True)
class ManualActivityLinked:
    broker_order_ref: BrokerOrderRef
    manual_activity_reference: str
    actor: str
    observed_at: datetime
    evidence: ResolutionQueryEvidence

    def __post_init__(self) -> None:
        if type(self.broker_order_ref) is not BrokerOrderRef:
            raise ValueError("broker_order_ref must be exact BrokerOrderRef")
        if type(self.evidence) is not ResolutionQueryEvidence:
            raise ValueError("evidence must be exact ResolutionQueryEvidence")
        for name in ("manual_activity_reference", "actor"):
            require_id(getattr(self, name), name)
        require_utc(self.observed_at, "observed_at")
        if self.observed_at > self.evidence.fetched_at:
            raise ValueError("manual activity cannot be observed after evidence was fetched")
        _require_resolution_match(self.broker_order_ref, self.evidence, 1)

    @property
    def result(self) -> UnknownResolutionResult:
        return UnknownResolutionResult.MANUAL_ACTIVITY_LINKED


TypedUnknownResolutionEvidence = BrokerOrderLinked | ConfirmedAbsent | ManualActivityLinked
TYPED_UNKNOWN_RESOLUTION_TYPES = (
    BrokerOrderLinked, ConfirmedAbsent, ManualActivityLinked,
)


def canonical_resolution_payload(
    operator_command_id: str, resolution: TypedUnknownResolutionEvidence,
) -> dict[str, object]:
    """Return the one durable, variant-tagged representation."""
    require_id(operator_command_id, "operator_command_id")
    if type(resolution) not in TYPED_UNKNOWN_RESOLUTION_TYPES:
        raise TypeError("exact typed unknown-resolution evidence is required")
    query = resolution.evidence
    payload: dict[str, object] = {
        "operator_command_id": operator_command_id,
        "result": resolution.result.value,
        "query": {
            "environment": query.environment.value,
            "account_id": query.account_id,
            "business_date": query.business_date.isoformat(),
            "window_started_at": query.window_started_at.isoformat(),
            "window_completed_at": query.window_completed_at.isoformat(),
            "query_policy_version": query.query_policy_version,
            "required_source_capabilities": list(query.required_source_capabilities),
            "queried_api_ids": list(query.queried_api_ids),
            "pagination_complete": query.pagination_complete,
            "observation_ids": list(query.observation_ids),
            "response_sha256": query.response_sha256,
            "candidate_set_sha256": query.candidate_set_sha256,
            "candidate_count": query.candidate_count,
            "candidates": [_broker_ref_payload(candidate) for candidate in query.candidates],
            "fetched_at": query.fetched_at.isoformat(),
        },
    }
    if type(resolution) in (BrokerOrderLinked, ManualActivityLinked):
        payload["broker_order_ref"] = _broker_ref_payload(resolution.broker_order_ref)
    if type(resolution) is BrokerOrderLinked:
        payload["source_api_id"] = resolution.source_api_id
    elif type(resolution) is ManualActivityLinked:
        payload["manual_activity"] = {
            "reference": resolution.manual_activity_reference,
            "actor": resolution.actor,
            "observed_at": resolution.observed_at.isoformat(),
        }
    return payload


def resolution_from_payload(payload: object) -> TypedUnknownResolutionEvidence:
    """Strictly rebuild typed evidence; extra or missing keys are rejected."""
    if type(payload) is not dict:
        raise ValueError("resolution payload must be an exact object")
    result = UnknownResolutionResult(payload.get("result"))
    expected = {
        UnknownResolutionResult.CONFIRMED_ABSENT: {
            "operator_command_id", "result", "query",
        },
        UnknownResolutionResult.BROKER_ORDER_LINKED: {
            "operator_command_id", "result", "query", "broker_order_ref", "source_api_id",
        },
        UnknownResolutionResult.MANUAL_ACTIVITY_LINKED: {
            "operator_command_id", "result", "query", "broker_order_ref", "manual_activity",
        },
    }[result]
    if set(payload) != expected:
        raise ValueError("resolution payload keys do not match its variant")
    require_id(payload["operator_command_id"], "operator_command_id")
    query_payload = payload["query"]
    query_keys = {
        "environment", "account_id", "business_date", "window_started_at",
        "window_completed_at", "query_policy_version", "required_source_capabilities",
        "queried_api_ids", "pagination_complete", "observation_ids", "response_sha256",
        "candidate_set_sha256", "candidate_count", "candidates", "fetched_at",
    }
    if type(query_payload) is not dict or set(query_payload) != query_keys:
        raise ValueError("resolution query keys are malformed")
    query = ResolutionQueryEvidence(
        environment=TradingEnvironment(query_payload["environment"]),
        account_id=query_payload["account_id"],
        business_date=date.fromisoformat(query_payload["business_date"]),
        window_started_at=datetime.fromisoformat(query_payload["window_started_at"]),
        window_completed_at=datetime.fromisoformat(query_payload["window_completed_at"]),
        query_policy_version=query_payload["query_policy_version"],
        required_source_capabilities=_exact_string_tuple(
            query_payload["required_source_capabilities"]
        ),
        queried_api_ids=_exact_string_tuple(query_payload["queried_api_ids"]),
        pagination_complete=query_payload["pagination_complete"],
        observation_ids=_exact_string_tuple(query_payload["observation_ids"]),
        response_sha256=query_payload["response_sha256"],
        candidates=_broker_refs_from_payload(query_payload["candidates"]),
        fetched_at=datetime.fromisoformat(query_payload["fetched_at"]),
    )
    if (
        type(query_payload["candidate_count"]) is not int
        or type(query_payload["candidate_set_sha256"]) is not str
        or query_payload["candidate_count"] != query.candidate_count
        or query_payload["candidate_set_sha256"] != query.candidate_set_sha256
    ):
        raise ValueError("candidate summary does not match canonical candidates")
    if result is UnknownResolutionResult.CONFIRMED_ABSENT:
        return ConfirmedAbsent(query)
    ref_payload = payload["broker_order_ref"]
    if type(ref_payload) is not dict or set(ref_payload) != {
        "environment", "account_id", "business_date", "broker_order_id",
    }:
        raise ValueError("broker order reference keys are malformed")
    ref = BrokerOrderRef(
        TradingEnvironment(ref_payload["environment"]),
        ref_payload["account_id"],
        date.fromisoformat(ref_payload["business_date"]),
        ref_payload["broker_order_id"],
    )
    if result is UnknownResolutionResult.BROKER_ORDER_LINKED:
        return BrokerOrderLinked(ref, payload["source_api_id"], query)
    manual = payload["manual_activity"]
    if type(manual) is not dict or set(manual) != {"reference", "actor", "observed_at"}:
        raise ValueError("manual activity keys are malformed")
    return ManualActivityLinked(
        ref, manual["reference"], manual["actor"],
        datetime.fromisoformat(manual["observed_at"]), query,
    )


def _exact_string_tuple(value: object) -> tuple[str, ...]:
    if type(value) is not list or any(type(item) is not str for item in value):
        raise ValueError("resolution evidence identifiers must be an array of strings")
    return tuple(value)


def _broker_ref_payload(ref: BrokerOrderRef) -> dict[str, str]:
    return {
        "environment": ref.environment.value,
        "account_id": ref.account_id,
        "business_date": ref.business_date.isoformat(),
        "broker_order_id": ref.broker_order_id,
    }


def _broker_ref_sort_key(ref: BrokerOrderRef) -> tuple[str, str, str, str]:
    return (
        ref.environment.value, ref.account_id, ref.business_date.isoformat(),
        ref.broker_order_id,
    )


def _broker_refs_from_payload(value: object) -> tuple[BrokerOrderRef, ...]:
    if type(value) is not list:
        raise ValueError("candidates must be an array")
    refs = []
    for item in value:
        if type(item) is not dict or set(item) != {
            "environment", "account_id", "business_date", "broker_order_id",
        }:
            raise ValueError("candidate reference keys are malformed")
        refs.append(BrokerOrderRef(
            TradingEnvironment(item["environment"]), item["account_id"],
            date.fromisoformat(item["business_date"]), item["broker_order_id"],
        ))
    return tuple(refs)


def _require_complete_candidates(
    evidence: ResolutionQueryEvidence, expected_count: int
) -> None:
    if not evidence.pagination_complete:
        raise ValueError("unknown resolution requires complete pagination")
    if evidence.candidate_count != expected_count:
        raise ValueError("unknown resolution candidate count is not decisive")


def _require_resolution_match(
    broker_order_ref: BrokerOrderRef,
    evidence: ResolutionQueryEvidence,
    expected_count: int,
) -> None:
    _require_complete_candidates(evidence, expected_count)
    if (
        broker_order_ref.environment is not evidence.environment
        or broker_order_ref.account_id != evidence.account_id
        or broker_order_ref.business_date != evidence.business_date
        or not (
            evidence.window_started_at.date()
            <= broker_order_ref.business_date
            <= evidence.window_completed_at.date()
        )
        or evidence.candidates != (broker_order_ref,)
    ):
        raise ValueError("broker reference is not the exact canonical query candidate")
