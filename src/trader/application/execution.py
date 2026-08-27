from collections.abc import Callable
from contextlib import AbstractContextManager, ExitStack
from datetime import datetime, timezone
from math import isfinite
import re
from typing import Protocol
from uuid import uuid4

from trader.application.safety import SafetyController
from trader.application.operator import OperatorCommandService
from trader.domain.broker_observations import (
    TYPED_UNKNOWN_RESOLUTION_TYPES,
    TypedUnknownResolutionEvidence,
)
from trader.domain.models import (
    ExecutionPlan,
    OperatorCommand,
    OrderRequest,
    PermitScope,
    RiskDecision,
    ReservationAccountState,
    ReservationTerms,
    RiskReservationPolicy,
    RiskOutcome,
    RiskStage,
    Side,
    SnapshotQuality,
    SubmissionState,
    TradeIntent,
    TradingPermit,
    canonical_share_quantity,
    require_utc,
)
from trader.ports.clock import (
    MonotonicClock,
    PROCESS_CLOCK_SESSION_ID,
    system_monotonic,
)
from trader.ports.broker import (
    Broker,
    BrokerEnvironment,
    BrokerSubmitOutcome,
    BrokerSubmitResult,
)
from trader.ports.ledger import (
    Ledger,
    LedgerEvent,
    OrderReservationConflict,
    PermitAlreadyConsumed,
)


class AlreadySubmitted(RuntimeError):
    pass


class PersistenceFailure(RuntimeError):
    pass


class SubmissionValidationError(ValueError):
    pass


class ProcessLock(Protocol):
    @property
    def acquired(self) -> bool: ...

    def protects(self, account_alias: str) -> bool: ...

    def protects_runtime(self, runtime_identity: str) -> bool: ...

    def hold(self, account_alias: str) -> AbstractContextManager[None]: ...


class OrderCoordinator:
    def __init__(
        self,
        broker: Broker,
        ledger: Ledger,
        safety: SafetyController,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        process_lock: ProcessLock | None = None,
        *,
        account_id: str,
        monotonic_clock: MonotonicClock = system_monotonic,
        clock_session_id: str = PROCESS_CLOCK_SESSION_ID,
    ) -> None:
        if type(broker.environment) is not BrokerEnvironment:
            raise ValueError("broker environment must be BrokerEnvironment")
        if safety.environment is not broker.environment:
            raise SubmissionValidationError("safety and broker environments must match")
        self.broker = broker
        self.ledger = ledger
        self.process_lock = process_lock
        self.account_id = account_id
        if not isinstance(account_id, str) or not account_id.strip():
            raise SubmissionValidationError("service requires an internal account alias")
        self._require_live_lock(self.account_id)
        self.safety = safety
        self.clock = clock
        self.monotonic_clock = monotonic_clock
        self.clock_session_id = clock_session_id
        self._last_monotonic: float | None = None
        self._do_not_retry: set[str] = set()
        self._invalid_plans: set[str] = set()
        if self.broker.environment is BrokerEnvironment.LIVE:
            with self._held_live_lock():
                self._recover_incomplete_submissions(
                    self.account_id, self.broker.environment
                )
        else:
            self._recover_incomplete_submissions(
                self.account_id, self.broker.environment
            )

    def submit(
        self,
        request: OrderRequest,
        risk_decision: RiskDecision,
        plan: ExecutionPlan,
        intent: TradeIntent,
        permit: TradingPermit | None = None,
    ) -> SubmissionState:
        if request.account_id != self.account_id:
            raise SubmissionValidationError("request account does not match the service account")
        if self.broker.environment is not BrokerEnvironment.LIVE:
            return self._submit(
                request, risk_decision, plan, intent, permit
            )
        with self._held_live_lock():
            return self._submit(
                request, risk_decision, plan, intent, permit
            )

    def _submit(
        self,
        request: OrderRequest,
        risk_decision: RiskDecision,
        plan: ExecutionPlan,
        intent: TradeIntent,
        permit: TradingPermit | None,
    ) -> SubmissionState:
        if request.client_order_id in self._do_not_retry:
            raise AlreadySubmitted("submission is never retried automatically")
        try:
            now = self.clock()
            require_utc(now, "pre_submit_time")
            monotonic_now = self.monotonic_clock()
            if (
                type(monotonic_now) not in (int, float)
                or not isfinite(monotonic_now)
                or monotonic_now < 0
                or (
                    self._last_monotonic is not None
                    and monotonic_now < self._last_monotonic
                )
            ):
                raise ValueError("invalid monotonic reading")
            self._last_monotonic = float(monotonic_now)
        except Exception as error:
            self.safety.halt("CLOCK_FAILURE")
            raise SubmissionValidationError("clock failure halted submission") from error
        self._validate_evidence(
            request, risk_decision, plan, intent, permit, now, self._last_monotonic
        )
        reservation_terms = self._derive_reservation_terms(
            request, risk_decision, plan, intent
        )
        prepared = LedgerEvent(
            str(uuid4()), SubmissionState.PREPARED, request.client_order_id, now,
            {
                "execution_plan_id": plan.plan_id,
                "risk_decision_id": risk_decision.decision_id,
            },
        )
        submission_started = LedgerEvent(
            str(uuid4()), SubmissionState.SUBMISSION_STARTED,
            request.client_order_id, now, {"permit_id": None if permit is None else permit.permit_id},
        )
        try:
            reserved = self.ledger.reserve_submission(
                request.client_order_id,
                self._canonical_payload(request, risk_decision, plan, intent, permit),
                prepared,
                submission_started,
                permit.permit_id,
                reservation_terms,
            )
        except OrderReservationConflict:
            raise
        except PermitAlreadyConsumed as error:
            raise AlreadySubmitted("permit was already consumed") from error
        except Exception as error:
            self._persistence_failed(request.client_order_id, error)
        if not reserved:
            raise AlreadySubmitted("client_order_id was already reserved")

        self._do_not_retry.add(request.client_order_id)
        try:
            result = self.broker.submit(request)  # Deliberately outside a database transaction.
            if type(result) is not BrokerSubmitResult:
                raise ValueError("malformed broker result")
            BrokerSubmitResult.__post_init__(result)
            state = {
                BrokerSubmitOutcome.ACKNOWLEDGED: SubmissionState.ACKNOWLEDGED,
                BrokerSubmitOutcome.REJECTED: SubmissionState.SUBMISSION_REJECTED,
                BrokerSubmitOutcome.UNKNOWN: SubmissionState.SUBMITTED_UNKNOWN,
            }[result.outcome]
            occurred_at = self.clock()
            require_utc(occurred_at, "post_submit_time")
        except Exception as error:
            self.safety.block_unknown_submission(request.client_order_id)
            self._complete_submission(
                LedgerEvent(
                    str(uuid4()), SubmissionState.SUBMITTED_UNKNOWN.value,
                    request.client_order_id, now,
                    {"broker_order_id": None, "detail_code": self._exception_code(error)},
                )
            )
            return SubmissionState.SUBMITTED_UNKNOWN

        if state is SubmissionState.SUBMITTED_UNKNOWN:
            self.safety.block_unknown_submission(request.client_order_id)
        self._complete_submission(
            LedgerEvent(
                str(uuid4()), state.value, request.client_order_id, occurred_at,
                {
                    "broker_order_id": result.broker_order_id,
                    "detail_code": result.detail_code,
                },
            )
        )
        return state

    def _derive_reservation_terms(
        self,
        request: OrderRequest,
        risk: RiskDecision,
        plan: ExecutionPlan,
        intent: TradeIntent,
    ) -> ReservationTerms:
        evidence = self.safety.evidence
        if evidence is None:
            raise SubmissionValidationError("current reservation evidence is required")
        state = evidence.reservation_account_state
        policy = evidence.risk_reservation_policy
        if (
            type(state) is not ReservationAccountState
            or type(policy) is not RiskReservationPolicy
        ):
            raise SubmissionValidationError("exact reservation evidence is required")
        try:
            ReservationAccountState.__post_init__(state)
            RiskReservationPolicy.__post_init__(policy)
            minor_notional = request.limit_price * request.quantity * 100
            if minor_notional != minor_notional.to_integral_value():
                raise ValueError("limit notional is not an exact minor-unit amount")
            notional = int(minor_notional)
        except (ArithmeticError, ValueError) as error:
            raise SubmissionValidationError("invalid reservation terms") from error
        if not 0 <= notional <= (1 << 63) - 1:
            raise SubmissionValidationError("limit notional exceeds signed 64-bit minor units")
        if (
            state.account_id != request.account_id
            or state.account_id != evidence.account_snapshot.account_id
            or state.account_snapshot_id != risk.input_snapshot_id
            or state.account_snapshot_id != intent.account_snapshot_id
            or state.account_snapshot_id != evidence.account_snapshot.snapshot_id
            or policy.policy_version != risk.policy_version
            or policy.policy_version != evidence.policy_version
            or plan.market_evidence.environment is not self.broker.environment
            or state.account_currency != request.instrument.currency
        ):
            raise SubmissionValidationError("reservation evidence does not match submission")
        current_position = next(
            (
                position.quantity
                for position in state.positions
                if position.instrument == request.instrument
            ),
            0,
        )
        fee_buffer = policy.fee_buffer_minor if request.side is Side.BUY else 0
        reserved_cash = notional + fee_buffer if request.side is Side.BUY else 0
        if reserved_cash > (1 << 63) - 1:
            raise SubmissionValidationError("reserved cash exceeds signed 64-bit minor units")
        try:
            return ReservationTerms(
                state.account_id, state.account_snapshot_id, self.broker.environment,
                policy.policy_version, request.instrument, request.side,
                int(request.quantity), state.account_currency, request.instrument.currency,
                state.available_cash_minor, state.current_exposure_minor,
                current_position, policy.cash_cap_minor, policy.exposure_cap_minor,
                fee_buffer, reserved_cash,
                notional if request.side is Side.BUY else 0,
                int(request.quantity) if request.side is Side.SELL else 0,
            )
        except ValueError as error:
            raise SubmissionValidationError("invalid derived reservation terms") from error

    def _complete_submission(self, event: LedgerEvent) -> None:
        try:
            self.ledger.complete_submission(event)
        except Exception as error:
            self._persistence_failed(event.aggregate_id, error)

    def _require_live_lock(self, account_id: str | None = None) -> None:
        if self.broker.environment is not BrokerEnvironment.LIVE:
            return
        if not isinstance(account_id, str) or not account_id.strip():
            raise SubmissionValidationError("LIVE service requires an internal account alias")
        lock = self.process_lock
        try:
            valid = (
                lock is not None
                and lock.acquired
                and lock.protects(account_id)
                and lock.protects_runtime(self.ledger.runtime_identity)
            )
        except Exception as error:
            raise SubmissionValidationError("LIVE process lock validation failed") from error
        if not valid:
            raise SubmissionValidationError(
                "LIVE broker requires an acquired lock for the request account"
            )

    def _held_live_lock(self) -> AbstractContextManager[None]:
        self._require_live_lock(self.account_id)
        lock = self.process_lock
        account_id = self.account_id
        if lock is None or account_id is None:
            raise SubmissionValidationError("LIVE process lock is unavailable")
        stack = ExitStack()
        try:
            stack.enter_context(lock.hold(account_id))
        except Exception as error:
            stack.close()
            raise SubmissionValidationError("LIVE process lock validation failed") from error
        return stack

    def _validate_evidence(
        self,
        request: OrderRequest,
        risk: RiskDecision,
        plan: ExecutionPlan,
        intent: TradeIntent,
        permit: TradingPermit | None,
        now: datetime,
        monotonic_now: float,
    ) -> None:
        try:
            for name, value in (
                ("request.quantity", request.quantity),
                ("risk.original_quantity", risk.original_quantity),
                ("risk.approved_quantity", risk.approved_quantity),
                ("plan.quantity", plan.quantity),
                ("intent.target_quantity", intent.target_quantity),
                ("intent.current_quantity", intent.current_quantity),
                ("intent.open_quantity", intent.open_quantity),
                ("intent.original_quantity", intent.original_quantity),
            ):
                canonical_share_quantity(value, name)
        except ValueError as error:
            raise SubmissionValidationError("share quantity is outside canonical range") from error
        if risk.risk_stage is not RiskStage.PRE_TRADE:
            raise SubmissionValidationError("submission requires PRE_TRADE risk")
        if risk.outcome not in {RiskOutcome.APPROVED, RiskOutcome.ADJUSTED}:
            raise SubmissionValidationError("submission requires approved risk")
        approved = risk.approved_quantity
        if approved is None or approved == 0:
            raise SubmissionValidationError("submission requires nonzero approved quantity")
        expected_side = Side.BUY if approved > 0 else Side.SELL
        if (
            risk.trade_intent_id != intent.intent_id
            or risk.original_quantity != intent.original_quantity
            or risk.input_snapshot_id != intent.account_snapshot_id
            or request.account_id != intent.account_id
            or request.execution_plan_id != plan.plan_id
            or plan.intent_id != intent.intent_id
            or plan.risk_decision_id != risk.decision_id
            or request.instrument != intent.instrument
            or plan.side is not expected_side
            or plan.quantity != abs(approved)
            or request.side is not plan.side
            or request.quantity != plan.quantity
            or request.limit_price != plan.limit_price
            or request.order_type is not plan.order_type
            or request.time_in_force is not plan.time_in_force
            or plan.pricing_policy_version != risk.policy_version
            or plan.market_evidence.environment is not self.broker.environment
            or plan.market_evidence.pricing_policy_version != plan.pricing_policy_version
            or plan.market_evidence.minimum_limit_price != plan.minimum_limit_price
            or plan.market_evidence.maximum_limit_price != plan.maximum_limit_price
            or not plan.minimum_limit_price <= request.limit_price <= plan.maximum_limit_price
            or not plan.created_at <= request.created_at < plan.expires_at
        ):
            raise SubmissionValidationError("request, risk, and execution plan do not match")
        if approved < 0 and abs(approved) > intent.current_quantity:
            raise SubmissionValidationError("long-only SELL exceeds current long position")
        if (
            plan.market_evidence.quality is not SnapshotQuality.CONSISTENT
            or plan.market_evidence.observed_at > plan.created_at
        ):
            raise SubmissionValidationError("execution plan market evidence is invalid")
        if (
            plan.plan_id in self._invalid_plans
            or plan.clock_session_id != self.clock_session_id
            or monotonic_now < plan.created_monotonic
            or monotonic_now >= plan.expires_monotonic
            or now >= plan.expires_at
        ):
            raise SubmissionValidationError("execution plan has expired")
        if now < plan.created_at:
            self._invalid_plans.add(plan.plan_id)
            self.safety.halt("CLOCK_ROLLBACK")
            raise SubmissionValidationError("wall clock rollback invalidated execution plan")
        if permit is None:
            raise SubmissionValidationError("broker submission requires issued permit")
        if permit.environment is not self.broker.environment:
            raise SubmissionValidationError("permit environment does not match broker")
        self.safety.validate(
            permit,
            request.account_id,
            PermitScope.NEW_ORDER,
            now,
            client_order_id=request.client_order_id,
            risk_decision_id=risk.decision_id,
            execution_plan_id=plan.plan_id,
        )
        if (
            permit.account_snapshot_id != risk.input_snapshot_id
            or permit.policy_version != risk.policy_version
            or permit.market_snapshot_id != plan.market_evidence.snapshot_id
        ):
            raise SubmissionValidationError("permit and risk evidence do not match")
        evidence = self.safety.evidence
        if evidence is None or (
            evidence.account_snapshot.environment is not self.broker.environment
            or evidence.market.environment is not self.broker.environment
            or plan.market_evidence.snapshot_id != evidence.market.snapshot_id
            or plan.market_evidence.observed_at != evidence.market.observed_at
            or plan.market_evidence.quality is not evidence.market.quality
            or plan.market_evidence.pricing_policy_version
            != evidence.market.pricing_policy_version
            or plan.market_evidence.minimum_limit_price
            != evidence.market.minimum_limit_price
            or plan.market_evidence.maximum_limit_price
            != evidence.market.maximum_limit_price
        ):
            raise SubmissionValidationError("execution plan market evidence is stale")

    def _canonical_payload(
        self,
        request: OrderRequest,
        risk: RiskDecision,
        plan: ExecutionPlan,
        intent: TradeIntent,
        permit: TradingPermit | None,
    ) -> dict[str, object]:
        return {
            "environment": self.broker.environment.value,
            "request": {
                "client_order_id": request.client_order_id,
                "execution_plan_id": request.execution_plan_id,
                "account_id": request.account_id,
                "instrument": {
                    "market": request.instrument.market,
                    "symbol": request.instrument.symbol,
                    "currency": request.instrument.currency,
                },
                "side": request.side.value,
                "order_type": request.order_type.value,
                "time_in_force": request.time_in_force.value,
                "quantity": canonical_share_quantity(request.quantity),
                "limit_price": str(request.limit_price),
                "created_at": request.created_at.isoformat(),
            },
            "risk": {
                "decision_id": risk.decision_id,
                "stage": risk.risk_stage.value,
                "policy_version": risk.policy_version,
                "input_snapshot_id": risk.input_snapshot_id,
                "input_snapshot_environment": (
                    self.safety.evidence.account_snapshot.environment.value
                ),
                "trade_intent_id": risk.trade_intent_id,
                "original_quantity": canonical_share_quantity(risk.original_quantity),
                "approved_quantity": canonical_share_quantity(risk.approved_quantity),
                "outcome": risk.outcome.value,
                "reason_codes": list(risk.reason_codes),
                "evaluated_at": risk.evaluated_at.isoformat(),
            },
            "intent": {
                "intent_id": intent.intent_id,
                "target_id": intent.target_id,
                "strategy_id": intent.strategy_id,
                "source_decision_id": intent.source_decision_id,
                "strategy_version": intent.strategy_version,
                "strategy_input_snapshot_id": intent.strategy_input_snapshot_id,
                "account_id": intent.account_id,
                "account_snapshot_id": intent.account_snapshot_id,
                "instrument": {
                    "market": intent.instrument.market,
                    "symbol": intent.instrument.symbol,
                    "currency": intent.instrument.currency,
                },
                "target_quantity": canonical_share_quantity(intent.target_quantity),
                "current_quantity": canonical_share_quantity(intent.current_quantity),
                "open_quantity": canonical_share_quantity(intent.open_quantity),
                "original_quantity": canonical_share_quantity(intent.original_quantity),
                "created_at": intent.created_at.isoformat(),
            },
            "plan": {
                "plan_id": plan.plan_id,
                "intent_id": plan.intent_id,
                "risk_decision_id": plan.risk_decision_id,
                "side": plan.side.value,
                "order_type": plan.order_type.value,
                "time_in_force": plan.time_in_force.value,
                "quantity": canonical_share_quantity(plan.quantity),
                "limit_price": str(plan.limit_price),
                "market_evidence": {
                    "snapshot_id": plan.market_evidence.snapshot_id,
                    "environment": plan.market_evidence.environment.value,
                    "observed_at": plan.market_evidence.observed_at.isoformat(),
                    "quality": plan.market_evidence.quality.value,
                    "pricing_policy_version": (
                        plan.market_evidence.pricing_policy_version
                    ),
                    "minimum_limit_price": str(
                        plan.market_evidence.minimum_limit_price
                    ),
                    "maximum_limit_price": str(
                        plan.market_evidence.maximum_limit_price
                    ),
                },
                "pricing_policy_version": plan.pricing_policy_version,
                "created_at": plan.created_at.isoformat(),
                "expires_at": plan.expires_at.isoformat(),
                "minimum_limit_price": str(plan.minimum_limit_price),
                "maximum_limit_price": str(plan.maximum_limit_price),
                "clock_session_id": plan.clock_session_id,
                "created_monotonic": plan.created_monotonic,
                "expires_monotonic": plan.expires_monotonic,
            },
            "permit": None if permit is None else {
                "permit_id": permit.permit_id,
                "environment": permit.environment.value,
                "account_id": permit.account_id,
                "scope": permit.scope.value,
                "client_order_id": permit.client_order_id,
                "risk_decision_id": permit.risk_decision_id,
                "execution_plan_id": permit.execution_plan_id,
                "safety_epoch": permit.safety_epoch,
                "account_snapshot_id": permit.account_snapshot_id,
                "market_snapshot_id": permit.market_snapshot_id,
                "policy_version": permit.policy_version,
                "deployment_version": permit.deployment_version,
                "issued_at": permit.issued_at.isoformat(),
                "expires_at": permit.expires_at.isoformat(),
            },
        }

    def _recover_incomplete_submissions(
        self,
        account_id: str | None = None,
        environment: BrokerEnvironment | None = None,
    ) -> None:
        try:
            incomplete = self.ledger.incomplete_submissions(account_id, environment)
            for client_order_id in incomplete:
                self._complete_submission(
                    LedgerEvent(
                        str(uuid4()), SubmissionState.SUBMITTED_UNKNOWN.value,
                        client_order_id, self.clock(),
                        {"broker_order_id": None, "detail_code": "RESTART_RECOVERY"},
                    )
                )
            for client_order_id in self.ledger.unresolved_unknown_submissions(
                account_id, environment
            ):
                self._do_not_retry.add(client_order_id)
                self.safety.block_unknown_submission(client_order_id)
        except PersistenceFailure:
            raise
        except Exception as error:
            self._persistence_failed("STARTUP_RECOVERY", error)

    def resolve_unknown_submission(
        self,
        client_order_id: str,
        command: OperatorCommand,
        evidence: TypedUnknownResolutionEvidence,
        operator_commands: OperatorCommandService,
    ) -> None:
        if type(command) is not OperatorCommand or type(evidence) not in TYPED_UNKNOWN_RESOLUTION_TYPES:
            raise TypeError("typed operator command and resolution evidence are required")
        if operator_commands.ledger is not self.ledger or operator_commands.safety is not self.safety:
            raise ValueError("operator and execution services must share ledger and safety")
        operator_commands.resolve_unknown_submission(command, client_order_id, evidence)

    def _persistence_failed(self, client_order_id: str, error: Exception) -> None:
        self._do_not_retry.add(client_order_id)
        self.safety.halt("PERSISTENCE_FAILURE")
        raise PersistenceFailure("ledger failure halted submission") from error

    @staticmethod
    def _exception_code(error: Exception) -> str:
        name = re.sub(r"[^A-Z0-9]+", "_", type(error).__name__.upper()).strip("_")
        return f"BROKER_{name}"[:64] or "BROKER_EXCEPTION"


# Compatibility for the Phase 1A application name.
ExecutionService = OrderCoordinator
