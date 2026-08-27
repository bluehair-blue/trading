from dataclasses import dataclass
from datetime import datetime, timedelta
from math import isfinite
from uuid import uuid4

from trader.domain.cancellation import CancelOrderCommand, CancelPermit
from trader.domain.models import (
    AccountSnapshot,
    MarketEvidence,
    PermitScope,
    ReservationAccountState,
    RiskReservationPolicy,
    SafetyState,
    TradingPermit,
    TradingEnvironment,
    require_id,
    require_utc,
)
from trader.ports.clock import MonotonicClock, system_monotonic


class InvalidPermit(RuntimeError):
    pass


class SafetyGuardError(RuntimeError):
    pass


@dataclass(frozen=True)
class ReconciliationEvidence:
    account_snapshot: AccountSnapshot
    market: MarketEvidence
    reservation_account_state: ReservationAccountState
    risk_reservation_policy: RiskReservationPolicy
    policy_version: str
    deployment_version: str


class SafetyController:
    def __init__(
        self,
        environment: TradingEnvironment,
        monotonic_clock: MonotonicClock = system_monotonic,
    ) -> None:
        if type(environment) is not TradingEnvironment:
            raise ValueError("environment must be TradingEnvironment")
        self.environment = environment
        self.state = SafetyState.BOOTSTRAPPING
        self.epoch = 0
        self.recovery_checked = False
        self.evidence: ReconciliationEvidence | None = None
        self._issued_permits: dict[str, tuple[TradingPermit, int, float]] = {}
        self._issued_cancel_permits: dict[str, tuple[CancelPermit, float]] = {}
        self._blockers: set[str] = set()
        self._monotonic_clock = monotonic_clock
        self._last_monotonic: float | None = None
        self._evidence_deadline: float | None = None

    def _monotonic_now(self) -> float:
        try:
            now = self._monotonic_clock()
            if type(now) not in (int, float) or not isfinite(now) or now < 0:
                raise ValueError("invalid monotonic reading")
            now = float(now)
            if self._last_monotonic is not None and now < self._last_monotonic:
                raise ValueError("monotonic clock moved backwards")
            self._last_monotonic = now
            return now
        except Exception as error:
            self.halt("CLOCK_FAILURE")
            raise SafetyGuardError("monotonic clock failure") from error

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
        reservation_account_state: ReservationAccountState,
        risk_reservation_policy: RiskReservationPolicy,
        policy_version: str,
        deployment_version: str,
        now: datetime,
        *,
        max_snapshot_age_seconds: int = 30,
    ) -> None:
        monotonic_now = self._monotonic_now()
        if self.state is not SafetyState.RECONCILING or not self.recovery_checked:
            raise SafetyGuardError("reconciliation is not active")
        self._require_no_blockers()
        for name, value in (
            ("policy_version", policy_version),
            ("deployment_version", deployment_version),
        ):
            require_id(value, name)
        if type(reservation_account_state) is not ReservationAccountState:
            raise SafetyGuardError("exact reservation account state is required")
        if type(risk_reservation_policy) is not RiskReservationPolicy:
            raise SafetyGuardError("exact risk reservation policy is required")
        try:
            ReservationAccountState.__post_init__(reservation_account_state)
            RiskReservationPolicy.__post_init__(risk_reservation_policy)
        except ValueError as error:
            raise SafetyGuardError("invalid reservation evidence") from error
        if (
            account_snapshot.environment is not self.environment
            or market.environment is not self.environment
        ):
            raise SafetyGuardError("reconciliation evidence environment does not match safety")
        if (
            reservation_account_state.account_id != account_snapshot.account_id
            or reservation_account_state.account_snapshot_id != account_snapshot.snapshot_id
            or risk_reservation_policy.policy_version != policy_version
        ):
            raise SafetyGuardError("reservation evidence does not match reconciliation")
        if market.pricing_policy_version != policy_version:
            raise SafetyGuardError("market evidence pricing policy does not match reconciliation")
        if not account_snapshot.is_fresh_consistent(now, max_snapshot_age_seconds):
            raise SafetyGuardError("fresh CONSISTENT account evidence required")
        if not market.is_fresh_consistent(now, max_snapshot_age_seconds):
            raise SafetyGuardError("fresh CONSISTENT market evidence required")
        self.evidence = ReconciliationEvidence(
            account_snapshot, market, reservation_account_state,
            risk_reservation_policy, policy_version, deployment_version,
        )
        remaining = min(
            (account_snapshot.valid_until(max_snapshot_age_seconds) - now).total_seconds(),
            (market.valid_until(max_snapshot_age_seconds) - now).total_seconds(),
        )
        self._evidence_deadline = monotonic_now + remaining
        self._change_state(SafetyState.READY)

    def _arm(
        self,
        now: datetime,
        *,
        max_snapshot_age_seconds: int = 30,
    ) -> None:
        require_utc(now, "now")
        monotonic_now = self._monotonic_now()
        if self.state is not SafetyState.READY or self.evidence is None:
            raise SafetyGuardError("arm requires completed current reconciliation")
        self._require_no_blockers()
        if (
            self.evidence.account_snapshot.environment is not self.environment
            or self.evidence.market.environment is not self.environment
        ):
            raise SafetyGuardError("arm evidence environment does not match safety")
        if not self.evidence.account_snapshot.is_fresh_consistent(
            now, max_snapshot_age_seconds
        ):
            raise SafetyGuardError("arm requires fresh CONSISTENT account evidence")
        if not self.evidence.market.is_fresh_consistent(now, max_snapshot_age_seconds):
            raise SafetyGuardError("arm requires fresh CONSISTENT market evidence")
        if self._evidence_deadline is None or monotonic_now >= self._evidence_deadline:
            raise SafetyGuardError("arm requires unexpired monotonic evidence")
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
        try:
            monotonic_now = self._monotonic_now()
        except SafetyGuardError as error:
            raise InvalidPermit("monotonic clock failure") from error
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
        if (
            evidence.account_snapshot.environment is not self.environment
            or evidence.market.environment is not self.environment
            or evidence.market.pricing_policy_version != evidence.policy_version
        ):
            raise InvalidPermit("reconciliation evidence does not match safety")
        if not evidence.account_snapshot.is_fresh_consistent(now, max_snapshot_age_seconds):
            raise InvalidPermit("fresh CONSISTENT account evidence required")
        if not evidence.market.is_fresh_consistent(now, max_snapshot_age_seconds):
            raise InvalidPermit("fresh CONSISTENT market evidence required")
        if self._evidence_deadline is None or monotonic_now >= self._evidence_deadline:
            raise InvalidPermit("reconciliation evidence has expired")
        evidence_expires_at = min(
            evidence.account_snapshot.valid_until(max_snapshot_age_seconds),
            evidence.market.valid_until(max_snapshot_age_seconds),
        )
        expires_at = min(now + timedelta(seconds=ttl_seconds), evidence_expires_at)
        if expires_at <= now:
            raise InvalidPermit("evidence validity has expired")
        permit = TradingPermit(
            str(uuid4()), account_id, self.environment, scope, self.epoch,
            client_order_id, risk_decision_id, execution_plan_id,
            evidence.account_snapshot.snapshot_id, evidence.market.snapshot_id,
            evidence.policy_version, evidence.deployment_version, now,
            expires_at,
        )
        remaining = (expires_at - now).total_seconds()
        self._issued_permits[permit.permit_id] = (
            permit, max_snapshot_age_seconds,
            min(monotonic_now + remaining, self._evidence_deadline),
        )
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
        try:
            monotonic_now = self._monotonic_now()
        except SafetyGuardError as error:
            raise InvalidPermit("monotonic clock failure") from error
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
                str(uuid4()), account_id, self.environment, scope, self.epoch,
                None, None, None,
                None, None, None, None, now, expires_at,
            )
            self._issued_permits[permit.permit_id] = (
                permit, 0, monotonic_now + (expires_at - now).total_seconds()
            )
            return permit
        evidence = self.evidence
        if evidence is None or evidence.account_snapshot.account_id != account_id:
            raise InvalidPermit("current reconciliation evidence is required")
        if (
            evidence.account_snapshot.environment is not self.environment
            or evidence.market.environment is not self.environment
            or evidence.market.pricing_policy_version != evidence.policy_version
        ):
            raise InvalidPermit("reconciliation evidence does not match safety")
        if not evidence.account_snapshot.is_fresh_consistent(now, max_snapshot_age_seconds):
            raise InvalidPermit("fresh CONSISTENT account evidence required")
        if not evidence.market.is_fresh_consistent(now, max_snapshot_age_seconds):
            raise InvalidPermit("fresh CONSISTENT market evidence required")
        if self._evidence_deadline is None or monotonic_now >= self._evidence_deadline:
            raise InvalidPermit("reconciliation evidence has expired")
        expires_at = min(
            now + timedelta(seconds=ttl_seconds),
            evidence.account_snapshot.valid_until(max_snapshot_age_seconds),
            evidence.market.valid_until(max_snapshot_age_seconds),
        )
        if expires_at <= now:
            raise InvalidPermit("evidence validity has expired")
        permit = TradingPermit(
            str(uuid4()), account_id, self.environment, scope, self.epoch,
            None, None, None,
            evidence.account_snapshot.snapshot_id, evidence.market.snapshot_id,
            evidence.policy_version, evidence.deployment_version, now, expires_at,
        )
        self._issued_permits[permit.permit_id] = (
            permit, max_snapshot_age_seconds,
            min(monotonic_now + (expires_at - now).total_seconds(), self._evidence_deadline),
        )
        return permit

    def _issue_cancel_permit(
        self,
        command: CancelOrderCommand,
        now: datetime,
        *,
        ttl_seconds: int = 15,
    ) -> CancelPermit:
        """Issue process-local authority; this is intentionally not crash durable."""
        require_utc(now, "now")
        if type(command) is not CancelOrderCommand:
            raise InvalidPermit("exact CancelOrderCommand is required")
        try:
            monotonic_now = self._monotonic_now()
        except SafetyGuardError as error:
            raise InvalidPermit("monotonic clock failure") from error
        if not self._scope_allowed(PermitScope.CANCEL, True):
            raise InvalidPermit(f"scope {PermitScope.CANCEL} is not allowed in {self.state}")
        if ttl_seconds <= 0:
            raise InvalidPermit("permit TTL must be positive")
        if (
            command.target.environment is not self.environment
            or command.target.account_id == ""
        ):
            raise InvalidPermit("cancellation target does not match safety")
        expires_at = now + timedelta(seconds=min(ttl_seconds, 30))
        permit = CancelPermit(str(uuid4()), command, self.epoch, now, expires_at)
        self._issued_cancel_permits[permit.permit_id] = (
            permit,
            monotonic_now + (expires_at - now).total_seconds(),
        )
        return permit

    def consume_cancel_permit(
        self,
        permit: CancelPermit,
        command: CancelOrderCommand,
        now: datetime,
    ) -> None:
        """Validate and consume before broker I/O so reuse can never retry an unknown."""
        require_utc(now, "now")
        try:
            monotonic_now = self._monotonic_now()
        except SafetyGuardError as error:
            raise InvalidPermit("monotonic clock failure") from error
        if type(permit) is not CancelPermit or type(command) is not CancelOrderCommand:
            raise InvalidPermit("exact cancellation permit and command are required")
        if now < permit.issued_at:
            self.halt("CLOCK_ROLLBACK")
            raise InvalidPermit("wall clock rollback invalidated permit")
        issued = self._issued_cancel_permits.pop(permit.permit_id, None)
        if (
            issued is None
            or issued[0] != permit
            or permit.command != command
            or command.target.environment is not self.environment
            or permit.safety_epoch != self.epoch
            or now >= permit.expires_at
            or monotonic_now >= issued[1]
            or not self._scope_allowed(PermitScope.CANCEL, True)
        ):
            raise InvalidPermit("cancel permit is forged, stale, expired, reused, or mismatched")

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
        try:
            monotonic_now = self._monotonic_now()
        except SafetyGuardError as error:
            raise InvalidPermit("monotonic clock failure") from error
        if now < permit.issued_at:
            self.halt("CLOCK_ROLLBACK")
            raise InvalidPermit("wall clock rollback invalidated permit")
        evidence = self.evidence
        issued = self._issued_permits.get(permit.permit_id)
        max_age_seconds = issued[1] if issued is not None else 0
        common_invalid = (
            issued is None
            or issued[0] != permit
            or permit.account_id != account_id
            or permit.environment is not self.environment
            or permit.scope is not scope
            or permit.client_order_id != client_order_id
            or permit.risk_decision_id != risk_decision_id
            or permit.execution_plan_id != execution_plan_id
            or permit.safety_epoch != self.epoch
            or now >= permit.expires_at
            or monotonic_now >= issued[2]
            or not self._scope_allowed(scope, operator_approved=True)
        )
        if common_invalid:
            raise InvalidPermit("permit is forged, stale, expired, or invalidated")
        if scope is PermitScope.CANCEL:
            return
        if (
            evidence is None
            or evidence.account_snapshot.environment is not self.environment
            or evidence.market.environment is not self.environment
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
        self._issued_cancel_permits.clear()
