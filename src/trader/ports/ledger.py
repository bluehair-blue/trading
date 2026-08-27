from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Protocol

from trader.domain.broker_observations import TypedUnknownResolutionEvidence
from trader.domain.models import (
    OperatorCommand,
    OperatorCommandOutcome,
    ReservationTerms,
    TradingEnvironment,
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
    def append(self, event: LedgerEvent) -> None: ...
    def reserve_operator_command(
        self, command: OperatorCommand, requested_event: LedgerEvent
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
