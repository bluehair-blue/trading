from collections.abc import Callable
from contextlib import AbstractContextManager, ExitStack
from datetime import datetime, timezone
import re
from typing import Protocol
from uuid import uuid4

from trader.application.safety import SafetyController
from trader.application.operator import OperatorCommandService
from trader.domain.models import (
    ExecutionPlan,
    OperatorCommand,
    OrderRequest,
    PermitScope,
    RiskDecision,
    RiskOutcome,
    RiskStage,
    Side,
    SubmissionState,
    TradeIntent,
    TradingPermit,
    UnknownResolutionEvidence,
    require_utc,
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


class ExecutionService:
    def __init__(
        self,
        broker: Broker,
        ledger: Ledger,
        safety: SafetyController,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        process_lock: ProcessLock | None = None,
        *,
        account_id: str,
    ) -> None:
        if type(broker.environment) is not BrokerEnvironment:
            raise ValueError("broker environment must be BrokerEnvironment")
        self.broker = broker
        self.ledger = ledger
        self.process_lock = process_lock
        self.account_id = account_id
        if not isinstance(account_id, str) or not account_id.strip():
            raise SubmissionValidationError("service requires an internal account alias")
        self._require_live_lock(self.account_id)
        self.safety = safety
        self.clock = clock
        self._do_not_retry: set[str] = set()
        if self.broker.environment is BrokerEnvironment.LIVE:
            with self._held_live_lock():
                self._recover_incomplete_submissions(self.account_id)
        else:
            self._recover_incomplete_submissions(self.account_id)

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
            return self._submit(request, risk_decision, plan, intent, permit)
        with self._held_live_lock():
            return self._submit(request, risk_decision, plan, intent, permit)

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
        except Exception as error:
            self.safety.halt("CLOCK_FAILURE")
            raise SubmissionValidationError("clock failure halted submission") from error
        self._validate_evidence(request, risk_decision, plan, intent, permit, now)
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
                None if permit is None else permit.permit_id,
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
            self._record(
                request.client_order_id,
                SubmissionState.SUBMITTED_UNKNOWN,
                now,
                detail_code=self._exception_code(error),
            )
            return SubmissionState.SUBMITTED_UNKNOWN

        if state is SubmissionState.SUBMITTED_UNKNOWN:
            self.safety.block_unknown_submission(request.client_order_id)
        self._record(
            request.client_order_id,
            state,
            occurred_at,
            result.broker_order_id,
            result.detail_code,
        )
        return state

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
    ) -> None:
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
        ):
            raise SubmissionValidationError("request, risk, and execution plan do not match")
        if approved < 0 and abs(approved) > intent.current_quantity:
            raise SubmissionValidationError("long-only SELL exceeds current long position")
        if now >= plan.expires_at:
            raise SubmissionValidationError("execution plan has expired")
        if self.broker.environment is not BrokerEnvironment.LIVE and permit is not None:
            raise SubmissionValidationError("non-LIVE broker does not accept a permit")
        if self.broker.environment is BrokerEnvironment.LIVE:
            if permit is None:
                raise SubmissionValidationError("LIVE broker requires issued permit")
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
            ):
                raise SubmissionValidationError("permit and risk evidence do not match")

    def _canonical_payload(
        self,
        request: OrderRequest,
        risk: RiskDecision,
        plan: ExecutionPlan,
        intent: TradeIntent,
        permit: TradingPermit | None,
    ) -> dict[str, object]:
        return {
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
                "quantity": str(request.quantity),
                "limit_price": str(request.limit_price),
                "created_at": request.created_at.isoformat(),
            },
            "risk": {
                "decision_id": risk.decision_id,
                "stage": risk.risk_stage.value,
                "policy_version": risk.policy_version,
                "input_snapshot_id": risk.input_snapshot_id,
                "trade_intent_id": risk.trade_intent_id,
                "original_quantity": str(risk.original_quantity),
                "approved_quantity": str(risk.approved_quantity),
                "outcome": risk.outcome.value,
                "reason_codes": list(risk.reason_codes),
                "evaluated_at": risk.evaluated_at.isoformat(),
            },
            "intent": {
                "intent_id": intent.intent_id,
                "target_id": intent.target_id,
                "strategy_id": intent.strategy_id,
                "account_id": intent.account_id,
                "account_snapshot_id": intent.account_snapshot_id,
                "instrument": {
                    "market": intent.instrument.market,
                    "symbol": intent.instrument.symbol,
                    "currency": intent.instrument.currency,
                },
                "target_quantity": str(intent.target_quantity),
                "current_quantity": str(intent.current_quantity),
                "open_quantity": str(intent.open_quantity),
                "original_quantity": str(intent.original_quantity),
                "created_at": intent.created_at.isoformat(),
            },
            "plan": {
                "plan_id": plan.plan_id,
                "intent_id": plan.intent_id,
                "risk_decision_id": plan.risk_decision_id,
                "side": plan.side.value,
                "order_type": plan.order_type.value,
                "time_in_force": plan.time_in_force.value,
                "quantity": str(plan.quantity),
                "limit_price": str(plan.limit_price),
                "expires_at": plan.expires_at.isoformat(),
            },
            "permit": None if permit is None else {
                "permit_id": permit.permit_id,
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

    def _recover_incomplete_submissions(self, account_id: str | None = None) -> None:
        try:
            incomplete = self.ledger.incomplete_submissions(account_id)
            for client_order_id in incomplete:
                self._record(
                    client_order_id,
                    SubmissionState.SUBMITTED_UNKNOWN,
                    self.clock(),
                    detail_code="RESTART_RECOVERY",
                )
            for client_order_id in self.ledger.unresolved_unknown_submissions(account_id):
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
        evidence: UnknownResolutionEvidence,
        operator_commands: OperatorCommandService,
    ) -> None:
        if type(command) is not OperatorCommand or type(evidence) is not UnknownResolutionEvidence:
            raise TypeError("typed operator command and resolution evidence are required")
        if operator_commands.ledger is not self.ledger or operator_commands.safety is not self.safety:
            raise ValueError("operator and execution services must share ledger and safety")
        operator_commands.resolve_unknown_submission(command, client_order_id, evidence)

    def _record(
        self,
        client_order_id: str,
        state: SubmissionState,
        occurred_at: datetime,
        broker_order_id: str | None = None,
        detail_code: str = "",
    ) -> None:
        event = LedgerEvent(
            str(uuid4()), state.value, client_order_id, occurred_at,
            {
                "broker_order_id": broker_order_id,
                "detail_code": detail_code,
            },
        )
        try:
            self.ledger.append(event)
        except Exception as error:
            self._persistence_failed(client_order_id, error)

    def _persistence_failed(self, client_order_id: str, error: Exception) -> None:
        self._do_not_retry.add(client_order_id)
        self.safety.halt("PERSISTENCE_FAILURE")
        raise PersistenceFailure("ledger failure halted submission") from error

    @staticmethod
    def _exception_code(error: Exception) -> str:
        name = re.sub(r"[^A-Z0-9]+", "_", type(error).__name__.upper()).strip("_")
        return f"BROKER_{name}"[:64] or "BROKER_EXCEPTION"
