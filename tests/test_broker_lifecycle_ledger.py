from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest

from trader.adapters.persistence.sqlite_ledger import SQLiteLedger, SchemaError
from trader.application.broker_lifecycle import BrokerLifecycleService
from trader.domain.broker_lifecycle import (
    BrokerFillObserved,
    BrokerOrderCanceled,
    BrokerOrderExpired,
    BrokerOrderOpened,
    BrokerOrderRejected,
)
from trader.domain.broker_observations import BrokerOrderRef
from trader.domain.models import (
    InstrumentId,
    ReservationTerms,
    Side,
    TradingEnvironment,
)
from trader.ports.ledger import LedgerEvent


NOW = datetime(2026, 8, 27, tzinfo=timezone.utc)
APPLE = InstrumentId("NASDAQ", "AAPL", "USD")


def canonical_order(
    order_id: str,
    permit_id: str,
    environment: TradingEnvironment,
    side: Side,
) -> dict[str, object]:
    return {
        "environment": environment.value,
        "request": {
            "account_id": "acct",
            "instrument": {"market": "NASDAQ", "symbol": "AAPL", "currency": "USD"},
            "side": side.value,
            "quantity": "10",
        },
        "risk": {
            "input_snapshot_environment": environment.value,
            "input_snapshot_id": "snap",
            "policy_version": "policy-v1",
        },
        "plan": {
            "pricing_policy_version": "policy-v1",
            "minimum_limit_price": "99",
            "maximum_limit_price": "101",
            "market_evidence": {
                "snapshot_id": "market",
                "environment": environment.value,
                "observed_at": NOW.isoformat(),
                "quality": "CONSISTENT",
                "pricing_policy_version": "policy-v1",
                "minimum_limit_price": "99",
                "maximum_limit_price": "101",
            },
        },
        "permit": {
            "permit_id": permit_id,
            "environment": environment.value,
            "market_snapshot_id": "market",
            "policy_version": "policy-v1",
        },
        "test_order_id": order_id,
    }


def terms(environment: TradingEnvironment, side: Side) -> ReservationTerms:
    buy = side is Side.BUY
    return ReservationTerms(
        "acct", "snap", environment, "policy-v1", APPLE, side, 10,
        "USD", "USD", 10_000, 0, 10, 10_000, 10_000,
        100 if buy else 0,
        1_100 if buy else 0,
        1_000 if buy else 0,
        0 if buy else 10,
    )


def common(
    order_id: str, broker_id: str, environment: TradingEnvironment,
    sequence: int, fact_id: str,
) -> dict[str, object]:
    at = NOW + timedelta(seconds=sequence)
    return {
        "fact_id": fact_id,
        "client_order_id": order_id,
        "broker_order_ref": BrokerOrderRef(
            environment, "acct", NOW.date(), broker_id
        ),
        "source_api_id": "simulator",
        "source_sequence": sequence,
        "occurred_at": at,
        "observed_at": at,
    }


def opened(order_id="order", broker_id="broker-1", environment=TradingEnvironment.SIMULATED):
    return BrokerOrderOpened(
        **common(order_id, broker_id, environment, 0, f"{order_id}-open"),
        instrument=APPLE,
        side=Side.BUY,
        requested_quantity=Decimal(10),
    )


def fill(order_id: str, broker_id: str, sequence: int, quantity: int):
    return BrokerFillObserved(
        **common(
            order_id, broker_id, TradingEnvironment.SIMULATED,
            sequence, f"{order_id}-fill-{sequence}",
        ),
        broker_execution_id=f"execution-{order_id}-{sequence}",
        quantity=Decimal(quantity),
        price=Decimal("100"),
        fee=Decimal("0.01"),
        currency="USD",
    )


class BrokerLifecycleLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "ledger.db"
        self.ledger = SQLiteLedger(self.path)

    def tearDown(self) -> None:
        self.ledger.close()
        self.temp.cleanup()

    def acknowledge(
        self,
        order_id="order",
        broker_id="broker-1",
        environment=TradingEnvironment.SIMULATED,
        side=Side.BUY,
    ) -> None:
        permit_id = f"{order_id}-permit"
        self.ledger.reserve_submission(
            order_id,
            canonical_order(order_id, permit_id, environment, side),
            LedgerEvent(f"{order_id}-prepared", "PREPARED", order_id, NOW, {}),
            LedgerEvent(
                f"{order_id}-started", "SUBMISSION_STARTED", order_id, NOW, {}
            ),
            permit_id,
            terms(environment, side),
        )
        self.ledger.complete_submission(LedgerEvent(
            f"{order_id}-ack", "ACKNOWLEDGED", order_id, NOW,
            {"broker_order_id": broker_id, "detail_code": "ACK"},
        ))

    def releases(self, order_id="order") -> list[dict[str, int]]:
        return [
            dict(event.payload)
            for event in self.ledger.events_for(order_id)
            if event.event_type == "RISK_RELEASED"
        ]

    def test_buy_partial_full_release_order_projection_and_digest(self) -> None:
        self.acknowledge()
        facts = (opened(), fill("order", "broker-1", 1, 3), fill("order", "broker-1", 2, 7))
        projection = BrokerLifecycleService(self.ledger).record(facts)
        self.assertEqual(projection.order.filled, Decimal(10))
        self.assertFalse(self.ledger.record_broker_execution(facts[-1]))
        self.assertEqual(self.releases(), [
            {
                "reserved_cash_minor": 300,
                "reserved_exposure_minor": 300,
                "reserved_sell_quantity": 0,
            },
            {
                "reserved_cash_minor": 800,
                "reserved_exposure_minor": 700,
                "reserved_sell_quantity": 0,
            },
        ])
        events = self.ledger.events_for("order")
        for fact_id in ("order-fill-1", "order-fill-2"):
            index = next(i for i, event in enumerate(events) if event.event_id == fact_id)
            self.assertEqual(events[index + 1].event_id, f"risk-released:{fact_id}")
        digest = self.ledger.content_digest()
        self.ledger.close()
        self.ledger = SQLiteLedger(self.path)
        self.assertEqual(self.ledger.content_digest(), digest)
        self.assertEqual(
            self.ledger.broker_order_projection("order"), projection
        )

    def test_partial_cancel_expire_reject_sell_and_environment_isolation(self) -> None:
        self.acknowledge("cancel", "broker-cancel")
        service = BrokerLifecycleService(self.ledger)
        service.record((
            opened("cancel", "broker-cancel"),
            fill("cancel", "broker-cancel", 1, 3),
            BrokerOrderCanceled(
                **common(
                    "cancel", "broker-cancel", TradingEnvironment.SIMULATED,
                    2, "cancel-terminal",
                ),
                quantity=Decimal(7),
            ),
        ))
        self.assertEqual(sum(r["reserved_cash_minor"] for r in self.releases("cancel")), 1_100)

        for order_id, fact_type in (("expire", BrokerOrderExpired), ("reject", BrokerOrderRejected)):
            self.acknowledge(order_id, f"broker-{order_id}")
            values = common(
                order_id, f"broker-{order_id}", TradingEnvironment.SIMULATED,
                1, f"{order_id}-terminal",
            )
            terminal = (
                fact_type(**values, quantity=Decimal(10))
                if fact_type is BrokerOrderExpired
                else fact_type(**values, quantity=Decimal(10), reason_code="BROKER_REJECTED")
            )
            service.record((opened(order_id, f"broker-{order_id}"), terminal))
            self.assertEqual(self.releases(order_id)[0]["reserved_cash_minor"], 1_100)

        self.acknowledge("sell", "broker-sell", side=Side.SELL)
        sell_open = replace(opened("sell", "broker-sell"), side=Side.SELL)
        sell_fill = fill("sell", "broker-sell", 1, 4)
        service.record((sell_open, sell_fill))
        self.assertEqual(self.releases("sell")[0]["reserved_sell_quantity"], 4)

        self.acknowledge(
            "paper", "broker-1", environment=TradingEnvironment.PAPER
        )
        self.assertTrue(self.ledger.record_broker_execution(opened(
            "paper", "broker-1", TradingEnvironment.PAPER
        )))
        simulated_facts = self.ledger.broker_lifecycle_facts(
            "acct", TradingEnvironment.SIMULATED
        )
        paper_facts = self.ledger.broker_lifecycle_facts(
            "acct", TradingEnvironment.PAPER
        )
        self.assertNotIn("paper", {fact.client_order_id for fact in simulated_facts})
        self.assertEqual(
            {fact.client_order_id for fact in paper_facts},
            {"paper"},
        )

    def test_duplicate_out_of_order_terminal_and_generic_append_fail_closed(self) -> None:
        self.acknowledge()
        open_fact = opened()
        self.assertTrue(self.ledger.record_broker_execution(open_fact))
        self.assertFalse(self.ledger.record_broker_execution(open_fact))
        with self.assertRaises(ValueError):
            self.ledger.record_broker_execution(
                replace(open_fact, requested_quantity=Decimal(9))
            )
        with self.assertRaises(ValueError):
            self.ledger.record_broker_execution(fill("order", "broker-1", 0, 1))
        terminal = BrokerOrderExpired(
            **common(
                "order", "broker-1", TradingEnvironment.SIMULATED,
                1, "order-expired",
            ),
            quantity=Decimal(10),
        )
        self.assertTrue(self.ledger.record_broker_execution(terminal))
        with self.assertRaises(ValueError):
            self.ledger.record_broker_execution(fill("order", "broker-1", 2, 1))
        with self.assertRaises(ValueError):
            self.ledger.append(LedgerEvent(
                "forged-open", "BROKER_ORDER_OPENED", "order", NOW, {}
            ))

    def test_release_failure_rolls_back_fact_and_release(self) -> None:
        self.acknowledge()
        self.ledger.record_broker_execution(opened())
        original = self.ledger._insert_event

        def fail_release(event, recorded_at):
            if event.event_type == "RISK_RELEASED":
                raise RuntimeError("fault injection")
            return original(event, recorded_at)

        self.ledger._insert_event = fail_release
        try:
            with self.assertRaises(RuntimeError):
                self.ledger.record_broker_execution(fill("order", "broker-1", 1, 3))
        finally:
            self.ledger._insert_event = original
        self.assertNotIn(
            "order-fill-1", {event.event_id for event in self.ledger.events_for("order")}
        )

    def test_reopen_rejects_missing_or_tampered_lifecycle_release(self) -> None:
        self.acknowledge()
        self.ledger.record_broker_execution(opened())
        self.ledger.record_broker_execution(fill("order", "broker-1", 1, 3))
        no_delete_sql = self.ledger.connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='ledger_events_no_delete'"
        ).fetchone()[0]
        self.ledger.connection.execute("DROP TRIGGER ledger_events_no_delete")
        self.ledger.connection.execute(
            "DELETE FROM ledger_events WHERE event_id='risk-released:order-fill-1'"
        )
        self.ledger.connection.execute(no_delete_sql)
        self.ledger.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(self.path)

        second = Path(self.temp.name) / "tampered.db"
        self.ledger = SQLiteLedger(second)
        self.acknowledge()
        self.ledger.record_broker_execution(opened())
        self.ledger.record_broker_execution(fill("order", "broker-1", 1, 3))
        no_update_sql = self.ledger.connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='ledger_events_no_update'"
        ).fetchone()[0]
        self.ledger.connection.execute("DROP TRIGGER ledger_events_no_update")
        self.ledger.connection.execute(
            """UPDATE ledger_events SET payload_json=?
               WHERE event_id='risk-released:order-fill-1'""",
            (json.dumps({
                "reserved_cash_minor": 299,
                "reserved_exposure_minor": 299,
                "reserved_sell_quantity": 0,
            }),),
        )
        self.ledger.connection.execute(no_update_sql)
        self.ledger.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(second)


if __name__ == "__main__":
    unittest.main()
