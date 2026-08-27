from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

from trader.domain.models import (
    AccountSnapshot,
    MarketEvidence,
    PermitScope,
    SafetyState,
    TradingPermit,
    require_id,
    require_utc,
)


class InvalidPermit(RuntimeError):
    pass


class SafetyGuardError(RuntimeError):
    pass


@dataclass(frozen=True)
class ReconciliationEvidence:
    account_snapshot: AccountSnapshot
    market: MarketEvidence
    policy_version: str
    deployment_version: str


class SafetyController:
    def __init__(self) -> None:
        self.state = SafetyState.BOOTSTRAPPING
        self.epoch = 0
        self.recovery_checked = False
        self.evidence: ReconciliationEvidence | None = None
        self._issued_permits: dict[str, tuple[TradingPermit, int]] = {}
        self._blockers: set[str] = set()

    @property
    def blockers(self) -> frozenset[str]:
        return frozenset(self._blockers)

    def acknowledge_startup_recovery(self, *_: object) -> None:
        raise TypeError("use OperatorCommandService for operator actions")

    def begin_reconciliation(self, *_: object) -> None:
        raise TypeError("use OperatorCommandService for operator actions")

    def arm(self, *_: object, **__: object) -> None:
        raise TypeError("use OperatorCommandService for operator actions")

    def _acknowledge_startup_recovery(self) -> None:
        if self.recovery_checked or self.state not in {
            SafetyState.BOOTSTRAPPING,
            SafetyState.HALTED,
        }:
            raise SafetyGuardError("startup recovery can only be acknowledged once")
        self._require_no_blockers()
        self.recovery_checked = True
        self.evidence = None
        self._change_state(SafetyState.RECONCILING)

    def _begin_reconciliation(self) -> None:
        if not self.recovery_checked or self.state is not SafetyState.HALTED:
            raise SafetyGuardError("reconciliation can begin only after recovery from HALTED")
        self._require_no_blockers()
        self.evidence = None
        self._change_state(SafetyState.RECONCILING)

    def complete_reconciliation(
        self,
        account_snapshot: AccountSnapshot,
        market: MarketEvidence,
        policy_version: str,
        deployment_version: str,
        now: datetime,
        *,
        max_snapshot_age_seconds: int = 30,
    ) -> None:
        if self.state is not SafetyState.RECONCILING or not self.recovery_checked:
            raise SafetyGuardError("reconciliation is not active")
        self._require_no_blockers()
        for name, value in (
            ("policy_version", policy_version),
            ("deployment_version", deployment_version),
        ):
            require_id(value, name)
        if not account_snapshot.is_fresh_consistent(now, max_snapshot_age_seconds):
            raise SafetyGuardError("fresh CONSISTENT account evidence required")
        if not market.is_fresh_consistent(now, max_snapshot_age_seconds):
            raise SafetyGuardError("fresh CONSISTENT market evidence required")
        self.evidence = ReconciliationEvidence(
            account_snapshot, market, policy_version, deployment_version
        )
        self._change_state(SafetyState.READY)

    def _arm(
        self,
        now: datetime,
        *,
        max_snapshot_age_seconds: int = 30,
    ) -> None:
        require_utc(now, "now")
        if self.state is not SafetyState.READY or self.evidence is None:
            raise SafetyGuardError("arm requires completed current reconciliation")
        self._require_no_blockers()
        if not self.evidence.account_snapshot.is_fresh_consistent(
            now, max_snapshot_age_seconds
        ):
            raise SafetyGuardError("arm requires fresh CONSISTENT account evidence")
        if not self.evidence.market.is_fresh_consistent(now, max_snapshot_age_seconds):
            raise SafetyGuardError("arm requires fresh CONSISTENT market evidence")
        self._change_state(SafetyState.TRADING)

    def halt(self, blocker: str | None = None) -> None:
        blocker_added = False
        if blocker is not None:
            require_id(blocker, "blocker")
            blocker_added = blocker not in self._blockers
            self._blockers.add(blocker)
        if self.state is not SafetyState.HALTED:
            self._change_state(SafetyState.HALTED)
        elif blocker_added:
            self._invalidate_permits()

    def block_unknown_submission(self, client_order_id: str) -> None:
        require_id(client_order_id, "client_order_id")
        self.halt(f"SUBMITTED_UNKNOWN:{client_order_id}")

    def _resolve_persisted_unknown_submission(self, client_order_id: str) -> None:
        require_id(client_order_id, "client_order_id")
        blocker = f"SUBMITTED_UNKNOWN:{client_order_id}"
        if blocker not in self._blockers:
            raise SafetyGuardError("safety blocker does not exist")
        self._blockers.remove(blocker)
        self._invalidate_permits()

    def issue_permit(
        self,
        account_id: str,
        scope: PermitScope,
        now: datetime,
        *,
        client_order_id: str | None = None,
        risk_decision_id: str | None = None,
        execution_plan_id: str | None = None,
        ttl_seconds: int = 30,
        max_snapshot_age_seconds: int = 30,
    ) -> TradingPermit:
        require_utc(now, "now")
        if scope is not PermitScope.NEW_ORDER or not self._scope_allowed(scope, False):
            raise InvalidPermit(f"scope {scope} is not allowed in {self.state}")
        try:
            for name, value in (
                ("client_order_id", client_order_id),
                ("risk_decision_id", risk_decision_id),
                ("execution_plan_id", execution_plan_id),
            ):
                require_id(value, name)
        except ValueError as error:
            raise InvalidPermit("NEW_ORDER permit requires exact order binding") from error
        if ttl_seconds <= 0:
            raise InvalidPermit("permit TTL must be positive")
        evidence = self.evidence
        if evidence is None or evidence.account_snapshot.account_id != account_id:
            raise InvalidPermit("current reconciliation evidence is required")
        if not evidence.account_snapshot.is_fresh_consistent(now, max_snapshot_age_seconds):
            raise InvalidPermit("fresh CONSISTENT account evidence required")
        if not evidence.market.is_fresh_consistent(now, max_snapshot_age_seconds):
            raise InvalidPermit("fresh CONSISTENT market evidence required")
        evidence_expires_at = min(
            evidence.account_snapshot.valid_until(max_snapshot_age_seconds),
            evidence.market.valid_until(max_snapshot_age_seconds),
        )
        expires_at = min(now + timedelta(seconds=ttl_seconds), evidence_expires_at)
        if expires_at <= now:
            raise InvalidPermit("evidence validity has expired")
        permit = TradingPermit(
            str(uuid4()), account_id, scope, self.epoch,
            client_order_id, risk_decision_id, execution_plan_id,
            evidence.account_snapshot.snapshot_id, evidence.market.snapshot_id,
            evidence.policy_version, evidence.deployment_version, now,
            expires_at,
        )
        self._issued_permits[permit.permit_id] = (permit, max_snapshot_age_seconds)
        return permit

    def _issue_high_risk_permit(
        self,
        account_id: str,
        scope: PermitScope,
        now: datetime,
        *,
        ttl_seconds: int = 30,
        max_snapshot_age_seconds: int = 30,
    ) -> TradingPermit:
        require_utc(now, "now")
        if scope not in {
            PermitScope.CANCEL,
            PermitScope.REDUCE_ONLY,
            PermitScope.EMERGENCY_FLATTEN,
        } or not self._scope_allowed(scope, True):
            raise InvalidPermit(f"scope {scope} is not allowed in {self.state}")
        if ttl_seconds <= 0:
            raise InvalidPermit("permit TTL must be positive")
        if scope is PermitScope.CANCEL:
            expires_at = now + timedelta(seconds=min(ttl_seconds, 30))
            permit = TradingPermit(
                str(uuid4()), account_id, scope, self.epoch,
                None, None, None,
                None, None, None, None, now, expires_at,
            )
            self._issued_permits[permit.permit_id] = (permit, 0)
            return permit
        evidence = self.evidence
        if evidence is None or evidence.account_snapshot.account_id != account_id:
            raise InvalidPermit("current reconciliation evidence is required")
        if not evidence.account_snapshot.is_fresh_consistent(now, max_snapshot_age_seconds):
            raise InvalidPermit("fresh CONSISTENT account evidence required")
        if not evidence.market.is_fresh_consistent(now, max_snapshot_age_seconds):
            raise InvalidPermit("fresh CONSISTENT market evidence required")
        expires_at = min(
            now + timedelta(seconds=ttl_seconds),
            evidence.account_snapshot.valid_until(max_snapshot_age_seconds),
            evidence.market.valid_until(max_snapshot_age_seconds),
        )
        if expires_at <= now:
            raise InvalidPermit("evidence validity has expired")
        permit = TradingPermit(
            str(uuid4()), account_id, scope, self.epoch,
            None, None, None,
            evidence.account_snapshot.snapshot_id, evidence.market.snapshot_id,
            evidence.policy_version, evidence.deployment_version, now, expires_at,
        )
        self._issued_permits[permit.permit_id] = (permit, max_snapshot_age_seconds)
        return permit

    def validate(
        self,
        permit: TradingPermit,
        account_id: str,
        scope: PermitScope,
        now: datetime,
        *,
        client_order_id: str | None = None,
        risk_decision_id: str | None = None,
        execution_plan_id: str | None = None,
    ) -> None:
        require_utc(now, "now")
        evidence = self.evidence
        issued = self._issued_permits.get(permit.permit_id)
        max_age_seconds = issued[1] if issued is not None else 0
        common_invalid = (
            issued is None
            or issued[0] != permit
            or permit.account_id != account_id
            or permit.scope is not scope
            or permit.client_order_id != client_order_id
            or permit.risk_decision_id != risk_decision_id
            or permit.execution_plan_id != execution_plan_id
            or permit.safety_epoch != self.epoch
            or now < permit.issued_at
            or now >= permit.expires_at
            or not self._scope_allowed(scope, operator_approved=True)
        )
        if common_invalid:
            raise InvalidPermit("permit is forged, stale, expired, or invalidated")
        if scope is PermitScope.CANCEL:
            return
        if (
            evidence is None
            or permit.account_snapshot_id != evidence.account_snapshot.snapshot_id
            or permit.market_snapshot_id != evidence.market.snapshot_id
            or permit.policy_version != evidence.policy_version
            or permit.deployment_version != evidence.deployment_version
            or not evidence.account_snapshot.is_fresh_consistent(now, max_age_seconds)
            or not evidence.market.is_fresh_consistent(now, max_age_seconds)
        ):
            raise InvalidPermit("permit is forged, stale, expired, or invalidated")

    def _scope_allowed(self, scope: PermitScope, operator_approved: bool) -> bool:
        if not isinstance(scope, PermitScope):
            return False
        if "PERSISTENCE_FAILURE" in self._blockers:
            return False
        if scope is PermitScope.NEW_ORDER:
            return self.state is SafetyState.TRADING and not self._blockers
        if scope is PermitScope.CANCEL:
            return operator_approved and self.state in {
                SafetyState.READY, SafetyState.TRADING, SafetyState.HALTED,
            }
        if scope is PermitScope.REDUCE_ONLY:
            return operator_approved and self.state in {
                SafetyState.READY, SafetyState.TRADING, SafetyState.HALTED,
            }
        return self.state is SafetyState.HALTED and operator_approved

    def _require_no_blockers(self) -> None:
        if self._blockers:
            raise SafetyGuardError(f"safety blockers remain: {sorted(self._blockers)}")

    def _change_state(self, target: SafetyState) -> None:
        self.state = target
        self._invalidate_permits()

    def _invalidate_permits(self) -> None:
        self.epoch += 1
        self._issued_permits.clear()
