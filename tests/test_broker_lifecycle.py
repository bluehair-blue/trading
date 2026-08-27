from datetime import datetime, timedelta, timezone
from decimal import Decimal
import unittest

from trader.domain.broker_lifecycle import (
    BrokerFillObserved,
    BrokerOrderCanceled,
    BrokerOrderExpired,
    BrokerOrderOpened,
    BrokerOrderRejected,
    broker_fact_from_payload,
    canonical_broker_fact_payload,
    fold_broker_order,
)
from trader.domain.broker_observations import BrokerOrderRef
from trader.domain.models import BrokerExecutionState, InstrumentId, Side, TradingEnvironment


NOW = datetime(2026, 8, 27, tzinfo=timezone.utc)
REF = BrokerOrderRef(TradingEnvironment.SIMULATED, "account-1", NOW.date(), "sim-1")
APPLE = InstrumentId("NASDAQ", "AAPL", "USD")


def common(sequence: int, fact_id: str, **changes: object) -> dict[str, object]:
    values: dict[str, object] = {
        "fact_id": fact_id,
        "client_order_id": "client-1",
        "broker_order_ref": REF,
        "source_api_id": "simulated",
        "source_sequence": sequence,
        "occurred_at": NOW + timedelta(seconds=sequence),
        "observed_at": NOW + timedelta(seconds=sequence),
    }
    values.update(changes)
    return values


def opened() -> BrokerOrderOpened:
    return BrokerOrderOpened(
        **common(0, "open"),
        instrument=APPLE,
        side=Side.BUY,
        requested_quantity=Decimal(10),
    )


def fill(sequence: int, quantity: int) -> BrokerFillObserved:
    return BrokerFillObserved(
        **common(sequence, f"fill-{sequence}"),
        broker_execution_id=f"execution-{sequence}",
        quantity=Decimal(quantity),
        price=Decimal("100.25"),
        fee=Decimal("0.01"),
        currency="USD",
    )


class BrokerLifecycleTests(unittest.TestCase):
    def test_open_partial_full_fill_partition(self) -> None:
        partial = fold_broker_order((opened(), fill(1, 3)))
        full = fold_broker_order((opened(), fill(1, 3), fill(2, 7)))
        self.assertEqual(partial.order.execution_state, BrokerExecutionState.PARTIALLY_FILLED)
        self.assertEqual(partial.order.open, Decimal(7))
        self.assertEqual(full.order.execution_state, BrokerExecutionState.FILLED)
        self.assertEqual(full.order.filled, Decimal(10))

    def test_partial_cancel_and_open_expire_or_reject(self) -> None:
        canceled = fold_broker_order(
            (
                opened(),
                fill(1, 3),
                BrokerOrderCanceled(**common(2, "cancel"), quantity=Decimal(7)),
            )
        )
        expired = fold_broker_order(
            (opened(), BrokerOrderExpired(**common(1, "expire"), quantity=Decimal(10)))
        )
        rejected = fold_broker_order(
            (
                opened(),
                BrokerOrderRejected(
                    **common(1, "reject"), quantity=Decimal(10), reason_code="BROKER_REJECTED"
                ),
            )
        )
        self.assertEqual(canceled.order.execution_state, BrokerExecutionState.CANCELED)
        self.assertEqual(expired.order.execution_state, BrokerExecutionState.EXPIRED)
        self.assertEqual(rejected.order.execution_state, BrokerExecutionState.REJECTED)

    def test_duplicate_out_of_order_and_terminal_continuation_are_rejected(self) -> None:
        cases = (
            (opened(), fill(1, 3), fill(1, 3)),
            (opened(), fill(2, 3), fill(1, 7)),
            (
                opened(),
                BrokerOrderExpired(**common(1, "expire"), quantity=Decimal(10)),
                fill(2, 1),
            ),
        )
        for facts in cases:
            with self.subTest(facts=facts), self.assertRaises(ValueError):
                fold_broker_order(facts)

    def test_invalid_quantity_partition_and_scope_are_rejected(self) -> None:
        other_ref = BrokerOrderRef(
            TradingEnvironment.SIMULATED, "account-1", NOW.date(), "sim-2"
        )
        with self.assertRaises(ValueError):
            fold_broker_order(
                (opened(), BrokerOrderCanceled(**common(1, "cancel"), quantity=Decimal(9)))
            )
        with self.assertRaises(ValueError):
            fold_broker_order(
                (
                    opened(),
                    BrokerOrderExpired(
                        **common(1, "expire", broker_order_ref=other_ref),
                        quantity=Decimal(10),
                    ),
                )
            )
        with self.assertRaises(ValueError):
            BrokerOrderOpened(
                **common(0, "too-large-open"),
                instrument=APPLE,
                side=Side.BUY,
                requested_quantity=Decimal(1 << 63),
            )

    def test_canonical_fact_payload_round_trips_and_normalizes_decimals(self) -> None:
        observed = BrokerFillObserved(
            **common(1, "fill-1"),
            broker_execution_id="execution-1",
            quantity=Decimal("2.00"),
            price=Decimal("100.2500"),
            fee=Decimal("0.0100"),
            currency="USD",
        )
        payload = canonical_broker_fact_payload(observed)
        self.assertEqual(payload["quantity"], "2")
        self.assertEqual(payload["price"], "100.25")
        self.assertEqual(broker_fact_from_payload(payload), observed)
        payload["price"] = "100.2500"
        with self.assertRaisesRegex(ValueError, "not canonical"):
            broker_fact_from_payload(payload)


if __name__ == "__main__":
    unittest.main()
