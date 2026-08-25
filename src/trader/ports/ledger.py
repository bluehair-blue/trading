from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Protocol

from trader.domain.models import (
    OperatorCommand,
    OperatorCommandOutcome,
    UnknownResolutionEvidence,
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


class OperatorCommandConflict(RuntimeError):
    """The immutable command ID has already been reserved."""


class LedgerPersistenceError(RuntimeError):
    """The persistence adapter could not durably complete a requested write."""


class Ledger(Protocol):
    def reserve_order(
        self,
        client_order_id: str,
        canonical_payload: Mapping[str, object],
        prepared_event: LedgerEvent,
    ) -> bool: ...
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
    def pending_operator_commands(self) -> tuple[str, ...]: ...
    def record_unknown_resolution(
        self,
        client_order_id: str,
        command: OperatorCommand,
        evidence: UnknownResolutionEvidence,
        event: LedgerEvent,
    ) -> None: ...
    def events_for(self, aggregate_id: str) -> tuple[LedgerEvent, ...]: ...
    def incomplete_submissions(self) -> tuple[str, ...]: ...
    def unresolved_unknown_submissions(self) -> tuple[str, ...]: ...
    def integrity_check(self) -> bool: ...
