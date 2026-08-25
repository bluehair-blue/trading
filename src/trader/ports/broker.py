from dataclasses import dataclass
from enum import StrEnum
import re
from typing import Protocol

from trader.domain.models import OrderRequest


class BrokerSubmitOutcome(StrEnum):
    ACKNOWLEDGED = "ACKNOWLEDGED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


class BrokerEnvironment(StrEnum):
    SIMULATED = "SIMULATED"
    PAPER = "PAPER"
    LIVE = "LIVE"


@dataclass(frozen=True)
class BrokerSubmitResult:
    outcome: BrokerSubmitOutcome
    broker_order_id: str | None = None
    detail_code: str = ""

    def __post_init__(self) -> None:
        if type(self.outcome) is not BrokerSubmitOutcome:
            raise ValueError("outcome must be BrokerSubmitOutcome")
        acknowledged = self.outcome is BrokerSubmitOutcome.ACKNOWLEDGED
        if acknowledged and (not self.broker_order_id or not self.broker_order_id.strip()):
            raise ValueError("acknowledged submission requires broker_order_id")
        if not acknowledged and self.broker_order_id is not None:
            raise ValueError("rejected or unknown submission cannot have broker_order_id")
        if self.detail_code and re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", self.detail_code) is None:
            raise ValueError("detail_code must be a bounded uppercase safe code")


class Broker(Protocol):
    environment: BrokerEnvironment

    def submit(self, request: OrderRequest) -> BrokerSubmitResult: ...
