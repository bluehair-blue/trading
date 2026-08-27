from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import unittest

from trader.domain.accounting import (
    AccountingPosition,
    AccountingSeed,
    fold_accounting,
)
from trader.domain.broker_lifecycle import BrokerFillObserved, BrokerOrderOpened
from trader.domain.broker_observations import BrokerOrderRef
from trader.domain.models import InstrumentId, Side, TradingEnvironment


NOW = datetime(2026, 8, 27, tzinfo=timezone.utc)
APPLE = InstrumentId("NASDAQ", "AAPL", "USD")
ENV = TradingEnvironment.SIMULATED


def lifecycle(client_id: str, side: Side, quantity: int, price: str, fee: str):
    ref = BrokerOrderRef(ENV, "account-1", NOW.date(), f"broker-{client_id}")
    common = {
        "client_order_id": client_id,
        "broker_order_ref": ref,
        "source_api_id": "simulated",
    }
    return (
        BrokerOrderOpened(
            **common,
            fact_id=f"open-{client_id}",
            source_sequence=0,
            occurred_at=NOW,
            observed_at=NOW,
            instrument=APPLE,
            side=side,
            requested_quantity=Decimal(quantity),
        ),
        BrokerFillObserved(
            **common,
            fact_id=f"fill-{client_id}",
            source_sequence=1,
            occurred_at=NOW + timedelta(seconds=1),
            observed_at=NOW + timedelta(seconds=1),
            broker_execution_id=f"execution-{client_id}",
            quantity=Decimal(quantity),
            price=Decimal(price),
            fee=Decimal(fee),
            currency="USD",
        ),
    )


class AccountingTests(unittest.TestCase):
    def test_seed_fingerprint_is_canonical_and_position_order_independent(self) -> None:
        microsoft = InstrumentId("NASDAQ", "MSFT", "USD")
        first = AccountingSeed(
            "account-1",
            ENV,
            "USD",
            "accounting-v1",
            Decimal("1000.00"),
            (
                AccountingPosition(microsoft, Decimal("2.0")),
                AccountingPosition(APPLE, Decimal("1.00")),
            ),
        )
        second = AccountingSeed(
            "account-1",
            ENV,
            "USD",
            "accounting-v1",
            Decimal(1000),
            tuple(reversed(first.positions)),
        )
        self.assertEqual(first.canonical_json(), second.canonical_json())
        self.assertEqual(first.fingerprint(), second.fingerprint())

    def test_buy_then_sell_replays_cash_position_notional_and_fees(self) -> None:
        buy = lifecycle("buy", Side.BUY, 3, "100", "1")
        sell = lifecycle("sell", Side.SELL, 1, "110", "0.5")
        projection = fold_accounting(
            AccountingSeed("account-1", ENV, "USD", "accounting-v1", Decimal(1000)),
            buy + sell,
        )
        self.assertEqual(projection.cash, Decimal("808.5"))
        self.assertEqual(projection.positions[0].quantity, Decimal(2))
        self.assertEqual(projection.gross_traded_value, Decimal(410))
        self.assertEqual(projection.total_fees, Decimal("1.5"))

    def test_overdraw_short_and_scope_mismatch_fail_closed(self) -> None:
        buy = lifecycle("buy", Side.BUY, 3, "100", "1")
        with self.assertRaisesRegex(ValueError, "overdraw"):
            fold_accounting(
                AccountingSeed("account-1", ENV, "USD", "v1", Decimal(100)),
                buy,
            )
        sell = lifecycle("sell", Side.SELL, 2, "100", "0")
        with self.assertRaisesRegex(ValueError, "short"):
            fold_accounting(
                AccountingSeed(
                    "account-1",
                    ENV,
                    "USD",
                    "v1",
                    Decimal(0),
                    (AccountingPosition(APPLE, Decimal(1)),),
                ),
                sell,
            )
        with self.assertRaisesRegex(ValueError, "does not belong"):
            fold_accounting(
                AccountingSeed("other", ENV, "USD", "v1", Decimal(1000)),
                buy,
            )
        with self.assertRaises(ValueError):
            AccountingPosition(APPLE, Decimal(1 << 63))

    def test_global_fact_and_execution_ids_cannot_be_double_counted(self) -> None:
        first = lifecycle("first", Side.BUY, 1, "10", "0")
        second = lifecycle("second", Side.BUY, 1, "10", "0")
        seed = AccountingSeed("account-1", ENV, "USD", "v1", Decimal(100))
        with self.assertRaisesRegex(ValueError, "execution ID is duplicated"):
            fold_accounting(
                seed,
                first + (second[0], replace(
                    second[1],
                    broker_execution_id=first[1].broker_execution_id,
                )),
            )
        with self.assertRaisesRegex(ValueError, "fact ID is duplicated"):
            fold_accounting(
                seed,
                first + (replace(second[0], fact_id=first[0].fact_id), second[1]),
            )


if __name__ == "__main__":
    unittest.main()
