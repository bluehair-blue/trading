from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from hashlib import sha256
import json
import re

from trader.domain.models import require_id, require_utc


_SHA256 = re.compile(r"[0-9a-f]{64}")
_GIT_REVISION = re.compile(r"[0-9a-f]{7,64}")
_FAILURE_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,63}")


def _canonical_json(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class RunSpec:
    """Immutable backtest inputs persisted before execution starts."""

    code_commit: str
    strategy_version: str
    config_sha256: str
    account_seed_sha256: str
    data_snapshot_id: str
    universe_snapshot_id: str
    calendar_version: str
    corporate_action_version: str
    fee_model_version: str
    slippage_model_version: str
    fx_model_version: str
    accounting_model_version: str
    random_seed: int
    decision_cutoff_policy: str
    sample_started_at: datetime
    sample_completed_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "strategy_version",
            "data_snapshot_id",
            "universe_snapshot_id",
            "calendar_version",
            "corporate_action_version",
            "fee_model_version",
            "slippage_model_version",
            "fx_model_version",
            "accounting_model_version",
            "decision_cutoff_policy",
        ):
            require_id(getattr(self, name), name)
        if type(self.code_commit) is not str or _GIT_REVISION.fullmatch(self.code_commit) is None:
            raise ValueError("code_commit must be a lowercase hexadecimal git revision")
        for name in ("config_sha256", "account_seed_sha256"):
            value = getattr(self, name)
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise ValueError(f"{name} must be a lowercase SHA256 digest")
        if type(self.random_seed) is not int or not -(2**63) <= self.random_seed < 2**63:
            raise ValueError("random_seed must be a signed 64-bit integer")
        require_utc(self.sample_started_at, "sample_started_at")
        require_utc(self.sample_completed_at, "sample_completed_at")
        if self.sample_completed_at < self.sample_started_at:
            raise ValueError("backtest sample cannot end before it starts")

    def canonical_json(self) -> str:
        payload = asdict(self)
        payload["sample_started_at"] = self.sample_started_at.isoformat()
        payload["sample_completed_at"] = self.sample_completed_at.isoformat()
        return _canonical_json(payload)

    def fingerprint(self) -> str:
        return sha256(self.canonical_json().encode("utf-8")).hexdigest()


class RunStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class RunResult:
    """Terminal result linked to the exact pre-run specification."""

    run_id: str
    run_spec_fingerprint: str
    status: RunStatus
    started_at: datetime
    completed_at: datetime
    ledger_sha256: str | None = None
    output_sha256: str | None = None
    failure_code: str | None = None

    def __post_init__(self) -> None:
        require_id(self.run_id, "run_id")
        if (
            type(self.run_spec_fingerprint) is not str
            or _SHA256.fullmatch(self.run_spec_fingerprint) is None
        ):
            raise ValueError("run_spec_fingerprint must be a lowercase SHA256 digest")
        if type(self.status) is not RunStatus:
            raise ValueError("status must be RunStatus")
        require_utc(self.started_at, "started_at")
        require_utc(self.completed_at, "completed_at")
        if self.completed_at < self.started_at:
            raise ValueError("research run cannot complete before it starts")
        for name in ("ledger_sha256", "output_sha256"):
            value = getattr(self, name)
            if value is not None and (
                type(value) is not str or _SHA256.fullmatch(value) is None
            ):
                raise ValueError(f"{name} must be a lowercase SHA256 digest")
        if self.status is RunStatus.SUCCEEDED:
            if (
                self.ledger_sha256 is None
                or self.output_sha256 is None
                or self.failure_code is not None
            ):
                raise ValueError(
                    "successful run requires ledger/output hashes and no failure code"
                )
        elif (
            self.failure_code is None
            or type(self.failure_code) is not str
            or _FAILURE_CODE.fullmatch(self.failure_code) is None
        ):
            raise ValueError("failed run requires a bounded uppercase failure code")

    def canonical_json(self) -> str:
        payload = asdict(self)
        payload["status"] = self.status.value
        payload["started_at"] = self.started_at.isoformat()
        payload["completed_at"] = self.completed_at.isoformat()
        return _canonical_json(payload)
