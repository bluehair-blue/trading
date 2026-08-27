import unittest
from datetime import datetime, timezone

from trader.adapters.persistence.sqlite_ledger import SQLiteLedger
from trader.ports.ledger import LedgerEvent


NOW = datetime(2026, 8, 27, 4, tzinfo=timezone.utc)


class LedgerVerificationTests(unittest.TestCase):
    def test_clean_ledger_passes_each_read_only_check(self):
        ledger = SQLiteLedger(":memory:")
        try:
            ledger.append(LedgerEvent("event-1", "AUDIT", "aggregate-1", NOW, {}))
            changes = ledger.connection.total_changes

            self.assertTrue(ledger.physical_integrity_check())
            self.assertTrue(ledger.foreign_key_check())
            self.assertTrue(ledger.schema_contract_check())
            self.assertTrue(ledger.audit_semantic_check())
            self.assertTrue(ledger.submission_state_check())
            self.assertTrue(ledger.full_ledger_verify())
            self.assertTrue(ledger.integrity_check())
            self.assertEqual(ledger.connection.total_changes, changes)
        finally:
            ledger.close()

    def test_integrity_check_remains_physical_only_when_schema_is_tampered(self):
        ledger = SQLiteLedger(":memory:")
        try:
            ledger.connection.execute("DROP TRIGGER submission_state_contract")
            ledger.connection.execute("DROP TRIGGER order_environment_contract")

            self.assertTrue(ledger.physical_integrity_check())
            self.assertTrue(ledger.integrity_check())
            self.assertFalse(ledger.schema_contract_check())
            self.assertFalse(ledger.full_ledger_verify())
        finally:
            ledger.close()

    def test_submission_state_check_rejects_tampered_history(self):
        ledger = SQLiteLedger(":memory:")
        try:
            ledger.connection.execute("DROP TRIGGER submission_state_contract")
            ledger.connection.execute("DROP TRIGGER order_environment_contract")
            recorded_at = NOW.isoformat()
            ledger.connection.execute(
                "INSERT INTO order_requests VALUES ('order-1', '{}', ?)",
                (recorded_at,),
            )
            for event_id, event_type in (
                ("prepared", "PREPARED"),
                ("started", "SUBMISSION_STARTED"),
            ):
                ledger.connection.execute(
                    """INSERT INTO ledger_events
                       (event_id, event_type, aggregate_id, occurred_at, recorded_at, payload_json)
                       VALUES (?, ?, 'order-1', ?, ?, '{}')""",
                    (event_id, event_type, recorded_at, recorded_at),
                )
            self.assertTrue(ledger.submission_state_check())

            ledger.connection.execute("DROP TRIGGER ledger_events_no_update")
            ledger.connection.execute(
                "UPDATE ledger_events SET event_type='ACKNOWLEDGED' WHERE event_id='prepared'"
            )

            self.assertFalse(ledger.submission_state_check())
            self.assertFalse(ledger.full_ledger_verify())
        finally:
            ledger.close()


if __name__ == "__main__":
    unittest.main()
