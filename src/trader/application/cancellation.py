from collections.abc import Callable
from datetime import datetime, timezone

from trader.application.safety import SafetyController
from trader.domain.cancellation import CancelOrderCommand, CancelPermit
from trader.domain.models import require_id, require_utc
from trader.ports.broker import (
    BrokerCancelOutcome,
    BrokerCancelResult,
    BrokerEnvironment,
    OrderCancellation,
)


class CancellationValidationError(ValueError):
    pass


class OfflineCancellationService:
    """Process-local, at-most-once cancellation; no durable crash recovery."""

    def __init__(
        self,
        broker: OrderCancellation,
        safety: SafetyController,
        *,
        account_id: str,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        require_id(account_id, "account_id")
        if type(broker.environment) is not BrokerEnvironment:
            raise CancellationValidationError("broker environment must be BrokerEnvironment")
        if broker.environment is not safety.environment:
            raise CancellationValidationError("safety and broker environments must match")
        self.broker = broker
        self.safety = safety
        self.account_id = account_id
        self.clock = clock

    def cancel(
        self,
        command: CancelOrderCommand,
        permit: CancelPermit,
    ) -> BrokerCancelOutcome:
        if type(command) is not CancelOrderCommand or type(permit) is not CancelPermit:
            raise CancellationValidationError("exact cancellation command and permit required")
        if (
            command.target.account_id != self.account_id
            or command.target.environment is not self.broker.environment
        ):
            raise CancellationValidationError("cancellation target does not match service")
        try:
            now = self.clock()
            require_utc(now, "cancel_time")
        except Exception as error:
            self.safety.halt("CLOCK_FAILURE")
            raise CancellationValidationError("clock failure halted cancellation") from error
        self.safety.consume_cancel_permit(permit, command, now)
        try:
            result = self.broker.cancel(command)
            if type(result) is not BrokerCancelResult:
                raise ValueError("malformed broker cancel result")
            BrokerCancelResult.__post_init__(result)
            return result.outcome
        except Exception:
            return BrokerCancelOutcome.UNKNOWN
