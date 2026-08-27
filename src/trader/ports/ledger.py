from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from typing import Mapping, Protocol

from trader.domain.broker_observations import TypedUnknownResolutionEvidence
from trader.domain.cancellation import CancelOrderCommand
from trader.domain.broker_lifecycle import (
    BrokerLifecycleFact,
    BrokerLifecycleProjection,
)
from trader.domain.models import (
    OperatorCommand,
    OperatorCommandOutcome,
    ReservationTerms,
    TradingEnvironment,
    canonical_share_quantity,
)


@dataclass(frozen=True)
class LedgerEvent:
    event_id: str
    event_type: str
    aggregate_id: str
    occurred_at: datetime
    payload: Mapping[str, object]


class OrderReservationConflict(RuntimeError):
    pass


class ReservationCapacityExceeded(OrderReservationConflict):
    """The account's durable active reservations exhaust a stated capacity."""


class PermitAlreadyConsumed(RuntimeError):
    """The immutable LIVE permit is already bound to another order request."""


class OperatorCommandConflict(RuntimeError):
    """The immutable command ID has already been reserved."""


class LedgerPersistenceError(RuntimeError):
    """The persistence adapter could not durably complete a requested write."""


def canonical_cancel_order(command: CancelOrderCommand) -> dict[str, object]:
    if type(command) is not CancelOrderCommand:
        raise TypeError("exact CancelOrderCommand is required")
    CancelOrderCommand.__post_init__(command)
    return {
        "command_id": command.command_id,
        "target": {
            "environment": command.target.environment.value,
            "account_id": command.target.account_id,
            "business_date": command.target.business_date.isoformat(),
            "broker_order_id": command.target.broker_order_id,
        },
        "instrument": {
            "market": command.instrument.market,
            "symbol": command.instrument.symbol,
            "currency": command.instrument.currency,
        },
        "remaining_quantity": canonical_share_quantity(
            command.remaining_quantity, "remaining_quantity"
        ),
        "account_snapshot_id": command.account_snapshot_id,
    }


def canonical_operator_command(
    command: OperatorCommand,
    cancellation: CancelOrderCommand | None = None,
) -> dict[str, object]:
    cancel_payload = None if cancellation is None else canonical_cancel_order(cancellation)
    cancel_sha256 = None
    if cancel_payload is not None:
        encoded = json.dumps(
            cancel_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        cancel_sha256 = hashlib.sha256(encoded).hexdigest()
    return {
        "command_id": command.command_id,
        "actor": command.actor,
        "reason": command.reason,
        "deployment_version": command.deployment_version,
        "expected_safety_epoch": command.expected_safety_epoch,
        "requested_at": command.requested_at.isoformat(),
        "expires_at": command.expires_at.isoformat(),
        "action": command.action.value,
        "account_id": command.account_id,
        "environment": command.environment.value,
        "client_order_id": command.client_order_id,
        "risk_decision_id": command.risk_decision_id,
        "execution_plan_id": command.execution_plan_id,
        "cancel_order": cancel_payload,
        "cancel_order_sha256": cancel_sha256,
    }


class Ledger(Protocol):
    @property
    def runtime_identity(self) -> str: ...
    def reserve_submission(
        self,
        client_order_id: str,
        canonical_payload: Mapping[str, object],
        prepared_event: LedgerEvent,
        started_event: LedgerEvent,
        permit_id: str,
        reservation_terms: ReservationTerms,
    ) -> bool: ...
    def complete_submission(self, terminal_event: LedgerEvent) -> None: ...
    def record_broker_execution(self, fact: BrokerLifecycleFact) -> bool: ...
    def broker_order_projection(
        self, client_order_id: str
    ) -> BrokerLifecycleProjection | None: ...
    def broker_lifecycle_facts(
        self, account_id: str, environment: TradingEnvironment
    ) -> tuple[BrokerLifecycleFact, ...]: ...
    def content_digest(self) -> str: ...
    def append(self, event: LedgerEvent) -> None: ...
    def reserve_operator_command(
        self,
        command: OperatorCommand,
        requested_event: LedgerEvent,
        cancellation: CancelOrderCommand | None = None,
    ) -> None: ...
    def complete_operator_command(
        self,
        command_id: str,
        outcome: OperatorCommandOutcome,
        terminal_event: LedgerEvent,
    ) -> None: ...
    def pending_operator_commands(self, account_id: str | None = None) -> tuple[str, ...]: ...
    def record_unknown_resolution(
        self,
        client_order_id: str,
        command: OperatorCommand,
        evidence: TypedUnknownResolutionEvidence,
        event: LedgerEvent,
    ) -> None: ...
    def events_for(self, aggregate_id: str) -> tuple[LedgerEvent, ...]: ...
    def incomplete_submissions(
        self,
        account_id: str | None = None,
        environment: TradingEnvironment | None = None,
    ) -> tuple[str, ...]: ...
    def unresolved_unknown_submissions(
        self,
        account_id: str | None = None,
        environment: TradingEnvironment | None = None,
    ) -> tuple[str, ...]: ...
    def physical_integrity_check(self) -> bool: ...
    def foreign_key_check(self) -> bool: ...
    def schema_contract_check(self) -> bool: ...
    def audit_semantic_check(self) -> bool: ...
    def submission_state_check(self) -> bool: ...
    def full_ledger_verify(self) -> bool: ...
    def integrity_check(self) -> bool:
        """Backward-compatible physical SQLite check; use full_ledger_verify for all checks."""
        ...
