from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from hashlib import sha256
import json
import re

from trader.domain.models import require_id, require_utc


_SHA256 = re.compile(r"[0-9a-f]{64}")
_GIT_REVISION = re.compile(r"[0-9a-f]{7,64}")


@dataclass(frozen=True)
class RunManifest:
    run_id: str
    code_commit: str
    strategy_version: str
    config_sha256: str
    data_snapshot_id: str
    universe_snapshot_id: str
    calendar_version: str
    corporate_action_version: str
    fee_model_version: str
    slippage_model_version: str
    fx_model_version: str
    random_seed: int
    decision_cutoff_policy: str
    started_at: datetime
    completed_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "run_id",
            "strategy_version",
            "data_snapshot_id",
            "universe_snapshot_id",
            "calendar_version",
            "corporate_action_version",
            "fee_model_version",
            "slippage_model_version",
            "fx_model_version",
            "decision_cutoff_policy",
        ):
            require_id(getattr(self, name), name)
        if not isinstance(self.code_commit, str) or _GIT_REVISION.fullmatch(
            self.code_commit
        ) is None:
            raise ValueError("code_commit must be a lowercase hexadecimal git revision")
        if not isinstance(self.config_sha256, str) or _SHA256.fullmatch(
            self.config_sha256
        ) is None:
            raise ValueError("config_sha256 must be a lowercase SHA256 digest")
        if type(self.random_seed) is not int or not -(2**63) <= self.random_seed < 2**63:
            raise ValueError("random_seed must be a signed 64-bit integer")
        require_utc(self.started_at, "started_at")
        require_utc(self.completed_at, "completed_at")
        if self.completed_at < self.started_at:
            raise ValueError("research run cannot complete before it starts")

    def canonical_json(self) -> str:
        payload = asdict(self)
        payload["started_at"] = self.started_at.isoformat()
        payload["completed_at"] = self.completed_at.isoformat()
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def fingerprint(self) -> str:
        return sha256(self.canonical_json().encode("utf-8")).hexdigest()
