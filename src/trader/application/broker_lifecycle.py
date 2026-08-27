from trader.domain.broker_lifecycle import (
    BROKER_LIFECYCLE_FACT_TYPES,
    BrokerLifecycleFact,
    BrokerLifecycleProjection,
)
from trader.domain.models import TradingEnvironment
from trader.ports.ledger import Ledger


class BrokerLifecycleService:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger

    def record(
        self,
        facts: tuple[BrokerLifecycleFact, ...],
    ) -> BrokerLifecycleProjection | None:
        if type(facts) is not tuple:
            raise TypeError("broker lifecycle facts must be an exact tuple")
        client_order_id: str | None = None
        for fact in facts:
            if type(fact) not in BROKER_LIFECYCLE_FACT_TYPES:
                raise TypeError("exact typed broker lifecycle fact required")
            if client_order_id is None:
                client_order_id = fact.client_order_id
            elif fact.client_order_id != client_order_id:
                raise ValueError("broker lifecycle batch must target one client order")
            self.ledger.record_broker_execution(fact)
        return (
            None
            if client_order_id is None
            else self.ledger.broker_order_projection(client_order_id)
        )

    def facts_for(
        self,
        account_id: str,
        environment: TradingEnvironment,
    ) -> tuple[BrokerLifecycleFact, ...]:
        return self.ledger.broker_lifecycle_facts(account_id, environment)
