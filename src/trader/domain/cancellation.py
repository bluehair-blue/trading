from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from trader.domain.broker_observations import BrokerOrderRef
from trader.domain.models import InstrumentId, require_decimal, require_id, require_utc


@dataclass(frozen=True)
class CancelOrderCommand:
    """One offline cancellation attempt against one observed broker order."""

    command_id: str
    target: BrokerOrderRef
    instrument: InstrumentId
    remaining_quantity: Decimal
    account_snapshot_id: str

    def __post_init__(self) -> None:
        require_id(self.command_id, "command_id")
        if type(self.target) is not BrokerOrderRef:
            raise ValueError("target must be exact BrokerOrderRef")
        if type(self.instrument) is not InstrumentId:
            raise ValueError("instrument must be exact InstrumentId")
        require_decimal(self.remaining_quantity, "remaining_quantity")
        if self.remaining_quantity <= 0:
            raise ValueError("remaining_quantity must be positive")
        if self.remaining_quantity != self.remaining_quantity.to_integral_value():
            raise ValueError("remaining_quantity must be integral shares")
        require_id(self.account_snapshot_id, "account_snapshot_id")


@dataclass(frozen=True)
class CancelPermit:
    """Short-lived, process-local authority for exactly one cancellation command."""

    permit_id: str
    command: CancelOrderCommand
    safety_epoch: int
    issued_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        require_id(self.permit_id, "permit_id")
        if type(self.command) is not CancelOrderCommand:
            raise ValueError("command must be exact CancelOrderCommand")
        if type(self.safety_epoch) is not int or self.safety_epoch < 0:
            raise ValueError("safety_epoch must be a non-negative integer")
        require_utc(self.issued_at, "issued_at")
        require_utc(self.expires_at, "expires_at")
        if self.expires_at <= self.issued_at:
            raise ValueError("permit must expire after issuance")
