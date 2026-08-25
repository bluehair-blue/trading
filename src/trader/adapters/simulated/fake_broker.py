from trader.domain.models import OrderRequest
from trader.ports.broker import BrokerEnvironment, BrokerSubmitOutcome, BrokerSubmitResult


class FakeBroker:
    def __init__(
        self,
        outcome: BrokerSubmitOutcome,
        environment: BrokerEnvironment = BrokerEnvironment.SIMULATED,
    ) -> None:
        self.outcome = outcome
        self.environment = environment
        self.calls: list[OrderRequest] = []

    def submit(self, request: OrderRequest) -> BrokerSubmitResult:
        self.calls.append(request)
        if self.outcome is BrokerSubmitOutcome.ACKNOWLEDGED:
            return BrokerSubmitResult(self.outcome, f"fake-{request.client_order_id}")
        if self.outcome is BrokerSubmitOutcome.REJECTED:
            return BrokerSubmitResult(self.outcome, detail_code="FAKE_REJECTED")
        return BrokerSubmitResult(self.outcome, detail_code="FAKE_UNKNOWN")
