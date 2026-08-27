from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import TypeVar
from uuid import uuid4

from trader.application.safety import SafetyController
from trader.domain.models import (
    OperatorAction,
    OperatorCommand,
    OperatorCommandOutcome,
    PermitScope,
    TradingPermit,
    UnknownResolutionEvidence,
    require_id,
    require_utc,
)
from trader.ports.ledger import (
    Ledger,
    LedgerEvent,
    LedgerPersistenceError,
    OperatorCommandConflict,
)

T = TypeVar("T")


class OperatorCommandRejected(RuntimeError):
    pass


class OperatorPersistenceFailure(RuntimeError):
    pass


class OperatorCommandService:
    """The only application boundary allowed to apply authenticated operator effects."""

    def __init__(
        self,
        ledger: Ledger,
        safety: SafetyController,
        deployment_version: str,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        *,
        account_id: str,
    ) -> None:
        if not deployment_version.strip():
            raise ValueError("deployment_version must be non-empty")
        require_id(account_id, "account_id")
        self.ledger = ledger
        self.safety = safety
        self.deployment_version = deployment_version
        self.clock = clock
        self.account_id = account_id
        try:
            for command_id in ledger.pending_operator_commands(account_id):
                safety.halt(f"PENDING_OPERATOR_COMMAND:{command_id}")
        except Exception as error:
            safety.halt("PERSISTENCE_FAILURE")
            raise OperatorPersistenceFailure("operator recovery failed") from error

    def _execute(
        self,
        command: OperatorCommand,
        expected_action: OperatorAction,
        effect: Callable[[datetime], T],
        *,
        related_order_id: str | None = None,
    ) -> T:
        if type(command) is not OperatorCommand:
            raise TypeError("operator action requires an OperatorCommand")
        if command.account_id != self.account_id:
            raise OperatorCommandRejected("operator command belongs to another account")
        requested = LedgerEvent(
            str(uuid4()),
            "OPERATOR_COMMAND_REQUESTED",
            command.command_id,
            command.requested_at,
            {
                "command_id": command.command_id,
                "actor": command.actor,
                "reason": command.reason,
                "deployment_version": command.deployment_version,
                "expected_safety_epoch": command.expected_safety_epoch,
                "requested_at": command.requested_at.isoformat(),
                "expires_at": command.expires_at.isoformat(),
                "action": command.action.value,
                "account_id": command.account_id,
                "client_order_id": command.client_order_id,
                "risk_decision_id": command.risk_decision_id,
                "execution_plan_id": command.execution_plan_id,
                "previous_state": self.safety.state.value,
            },
        )
        try:
            self.ledger.reserve_operator_command(command, requested)
        except OperatorCommandConflict:
            raise
        except Exception as error:
            self.safety.halt("PERSISTENCE_FAILURE")
            raise OperatorPersistenceFailure("operator REQUESTED could not be persisted") from error
        try:
            now = self.clock()
            require_utc(now, "operator_effect_time")
        except Exception as error:
            self.safety.halt("CLOCK_FAILURE")
            self._terminal(
                command,
                OperatorCommandOutcome.FAILED,
                error,
                occurred_at=command.requested_at,
                related_order_id=related_order_id,
            )
            raise OperatorCommandRejected("operator clock failed before effect") from error
        try:
            self._validate(command, expected_action, now)
            result = effect(now)
        except Exception as error:
            self._terminal(
                command,
                OperatorCommandOutcome.FAILED,
                error,
                related_order_id=related_order_id,
            )
            raise
        self._terminal(
            command,
            OperatorCommandOutcome.SUCCEEDED,
            None,
            related_permit_id=result.permit_id if isinstance(result, TradingPermit) else None,
            related_order_id=related_order_id,
        )
        return result

    def _validate(
        self, command: OperatorCommand, expected_action: OperatorAction, now: datetime,
    ) -> None:
        require_utc(now, "operator_effect_time")
        if command.action is not expected_action:
            raise OperatorCommandRejected("operator action does not match the requested operation")
        if command.deployment_version != self.deployment_version:
            raise OperatorCommandRejected("operator command deployment version is stale")
        if command.expected_safety_epoch != self.safety.epoch:
            raise OperatorCommandRejected("operator command safety epoch is stale")
        if now < command.requested_at or now >= command.expires_at:
            raise OperatorCommandRejected("operator command is not currently valid")
        account_actions = {
            OperatorAction.ISSUE_CANCEL,
            OperatorAction.ISSUE_REDUCE_ONLY,
            OperatorAction.ISSUE_EMERGENCY_FLATTEN,
            OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
        }
        if command.action in account_actions and command.account_id is None:
            raise OperatorCommandRejected("operator command requires an internal account alias")
        if command.action is OperatorAction.RESOLVE_SUBMITTED_UNKNOWN:
            if (
                command.client_order_id is None
                or command.risk_decision_id is not None
                or command.execution_plan_id is not None
            ):
                raise OperatorCommandRejected(
                    "unknown resolution requires only a client_order_id claim"
                )
        elif any((
                command.client_order_id,
                command.risk_decision_id,
                command.execution_plan_id,
            )):
            raise OperatorCommandRejected(
                "current operator permit actions cannot carry order binding claims"
            )

    def _terminal(
        self,
        command: OperatorCommand,
        outcome: OperatorCommandOutcome,
        error: Exception | None,
        *,
        occurred_at: datetime | None = None,
        related_permit_id: str | None = None,
        related_order_id: str | None = None,
    ) -> None:
        try:
            if occurred_at is None:
                try:
                    occurred_at = self.clock()
                    require_utc(occurred_at, "operator_terminal_time")
                except Exception:
                    self.safety.halt("CLOCK_FAILURE")
                    occurred_at = command.requested_at
            event = LedgerEvent(
                str(uuid4()),
                f"OPERATOR_COMMAND_{outcome.value}",
                command.command_id,
                occurred_at,
                {
                    "result_state": self.safety.state.value,
                    "error": None if error is None else type(error).__name__,
                    "related_permit_id": related_permit_id,
                    "related_order_id": related_order_id,
                },
            )
            self.ledger.complete_operator_command(command.command_id, outcome, event)
        except Exception as persistence_error:
            self.safety.halt("PERSISTENCE_FAILURE")
            raise OperatorPersistenceFailure(
                "operator terminal result could not be persisted"
            ) from persistence_error

    def acknowledge_startup_recovery(self, command: OperatorCommand) -> None:
        return self._execute(
            command,
            OperatorAction.ACKNOWLEDGE_STARTUP_RECOVERY,
            lambda _: self.safety._acknowledge_startup_recovery(),
        )

    def begin_reconciliation(self, command: OperatorCommand) -> None:
        return self._execute(
            command,
            OperatorAction.BEGIN_RECONCILIATION,
            lambda _: self.safety._begin_reconciliation(),
        )

    def arm(self, command: OperatorCommand) -> None:
        return self._execute(command, OperatorAction.ARM, self.safety._arm)

    def halt(self, command: OperatorCommand) -> None:
        return self._execute(command, OperatorAction.HALT, lambda _: self.safety.halt())

    def issue_permit(self, command: OperatorCommand) -> TradingPermit:
        scopes = {
            OperatorAction.ISSUE_CANCEL: PermitScope.CANCEL,
            OperatorAction.ISSUE_REDUCE_ONLY: PermitScope.REDUCE_ONLY,
            OperatorAction.ISSUE_EMERGENCY_FLATTEN: PermitScope.EMERGENCY_FLATTEN,
        }
        try:
            scope = scopes[command.action]
        except (AttributeError, KeyError) as error:
            raise OperatorCommandRejected("command does not request a high-risk permit") from error
        return self._execute(
            command,
            command.action,
            lambda now: self.safety._issue_high_risk_permit(command.account_id, scope, now),
        )

    def resolve_unknown_submission(
        self,
        command: OperatorCommand,
        client_order_id: str,
        evidence: UnknownResolutionEvidence,
    ) -> None:
        if type(command) is not OperatorCommand:
            raise TypeError("operator action requires an OperatorCommand")
        if type(evidence) is not UnknownResolutionEvidence:
            raise TypeError("typed unknown-resolution evidence is required")
        if command.client_order_id != client_order_id:
            raise OperatorCommandRejected(
                "operator command target does not match the unknown submission"
            )

        def persist_evidence_then_clear(now: datetime) -> None:
            blocker = f"SUBMITTED_UNKNOWN:{client_order_id}"
            if blocker not in self.safety.blockers:
                raise OperatorCommandRejected("unknown submission is not blocked")
            if evidence.observed_at > now:
                raise OperatorCommandRejected(
                    "unknown-resolution evidence cannot be observed in the future"
                )
            event = LedgerEvent(
                str(uuid4()),
                "SUBMITTED_UNKNOWN_RESOLVED",
                client_order_id,
                now,
                {
                    "operator_command_id": command.command_id,
                    "result": evidence.result.value,
                    "observation": evidence.observation,
                    "reference": evidence.reference,
                    "observed_at": evidence.observed_at.isoformat(),
                },
            )
            try:
                self.ledger.record_unknown_resolution(
                    client_order_id, command, evidence, event
                )
            except LedgerPersistenceError as error:
                self.safety.halt("PERSISTENCE_FAILURE")
                raise OperatorPersistenceFailure(
                    "unknown-resolution evidence could not be persisted"
                ) from error
            self.safety._resolve_persisted_unknown_submission(client_order_id)

        self._execute(
            command,
            OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            persist_evidence_then_clear,
            related_order_id=client_order_id,
        )
