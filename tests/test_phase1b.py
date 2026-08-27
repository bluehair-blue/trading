import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4
from unittest.mock import patch

from trader.adapters.persistence.sqlite_ledger import (
    LEGACY_SCHEMA,
    LEGACY_STATEMENTS,
    SCHEMA_VERSION,
    V2_STATEMENTS,
    V3_STATEMENTS,
    V4_STATEMENTS,
    V5_STATEMENTS,
    V6_STATEMENTS,
    BackupError,
    SQLiteLedger,
    SchemaError,
)
from trader.application.operator import (
    OperatorCommandRejected,
    OperatorCommandService,
    OperatorPersistenceFailure,
)
from trader.application.safety import SafetyController
from trader.domain.models import (
    OperatorAction,
    OperatorCommand,
    OperatorCommandOutcome,
    PermitScope,
    SafetyState,
    UnknownResolutionEvidence,
    UnknownResolutionResult,
)
from trader.ports.ledger import (
    LedgerEvent,
    LedgerPersistenceError,
    OperatorCommandConflict,
    PermitAlreadyConsumed,
)

NOW = datetime(2026, 8, 25, 4, tzinfo=timezone.utc)


def command(safety, action=OperatorAction.HALT, command_id="command-1", **changes):
    values = {
        "command_id": command_id,
        "actor": "authenticated-operator",
        "reason": "phase 1B acceptance test",
        "deployment_version": "deploy-v1",
        "expected_safety_epoch": safety.epoch,
        "requested_at": NOW,
        "expires_at": NOW + timedelta(minutes=1),
        "action": action,
        "account_id": "acct",
    }
    values.update(changes)
    return OperatorCommand(**values)


def reserve_started(ledger, client_order_id, canonical_payload, prefix="submission"):
    permit = canonical_payload.get("permit")
    permit_id = None if permit is None else permit["permit_id"]
    return ledger.reserve_submission(
        client_order_id,
        canonical_payload,
        LedgerEvent(f"{prefix}-prepared", "PREPARED", client_order_id, NOW, {}),
        LedgerEvent(f"{prefix}-started", "SUBMISSION_STARTED", client_order_id, NOW, {}),
        permit_id,
    )


def downgrade_v7_objects(connection):
    connection.execute("DROP TRIGGER submission_state_contract")
    connection.execute("DROP INDEX order_request_permit_once")
    connection.execute("DROP TRIGGER operator_terminal_contract")
    connection.execute(V5_STATEMENTS[4])
    connection.execute("DROP TRIGGER unknown_resolution_contract")
    connection.execute(V6_STATEMENTS[1])
    connection.execute("DROP TRIGGER operator_commands_no_update")
    connection.execute(
        """UPDATE operator_commands SET canonical_json = json_remove(canonical_json,
           '$.client_order_id', '$.risk_decision_id', '$.execution_plan_id')"""
    )
    connection.execute("DROP TRIGGER ledger_events_no_update")
    connection.execute(
        """UPDATE ledger_events SET payload_json = json_remove(payload_json,
           '$.client_order_id', '$.risk_decision_id', '$.execution_plan_id')
           WHERE event_type = 'OPERATOR_COMMAND_REQUESTED'"""
    )
    connection.execute(LEGACY_STATEMENTS[2])
    connection.execute(
        """CREATE TRIGGER operator_commands_no_update BEFORE UPDATE ON operator_commands BEGIN
           SELECT RAISE(ABORT, 'operator commands are immutable'); END"""
    )
    connection.execute("DROP TRIGGER schema_metadata_no_delete")
    connection.execute("DELETE FROM schema_metadata WHERE key = 'operator_binding_v7_cutoff'")
    connection.execute(
        """CREATE TRIGGER schema_metadata_no_delete BEFORE DELETE ON schema_metadata BEGIN
           SELECT RAISE(ABORT, 'schema metadata is immutable'); END"""
    )


def downgrade_current_to_v2(path):
    connection = sqlite3.connect(path)
    try:
        downgrade_v7_objects(connection)
        connection.execute("DROP TRIGGER operator_terminal_contract")
        connection.execute("DROP TRIGGER unknown_resolution_contract")
        connection.execute("DROP TRIGGER schema_metadata_no_update")
        connection.execute("DROP TRIGGER schema_metadata_no_delete")
        connection.execute("DROP TABLE schema_metadata")
        connection.execute("DROP TRIGGER ledger_events_no_update")
        connection.execute(
            """UPDATE ledger_events SET payload_json =
               json_remove(payload_json, '$.related_permit_id', '$.related_order_id')
               WHERE event_type IN ('OPERATOR_COMMAND_SUCCEEDED','OPERATOR_COMMAND_FAILED')"""
        )
        connection.execute(LEGACY_STATEMENTS[2])
        connection.execute(V2_STATEMENTS[0])
        connection.execute("PRAGMA user_version = 2")
        connection.commit()
    finally:
        connection.close()


def downgrade_current_to_v3(path):
    connection = sqlite3.connect(path)
    try:
        downgrade_v7_objects(connection)
        connection.execute("DROP TRIGGER operator_terminal_contract")
        connection.execute("DROP TRIGGER unknown_resolution_contract")
        connection.execute("DROP TRIGGER schema_metadata_no_update")
        connection.execute("DROP TRIGGER schema_metadata_no_delete")
        connection.execute("DROP TABLE schema_metadata")
        connection.execute("DROP TRIGGER ledger_events_no_update")
        connection.execute(
            """UPDATE ledger_events SET payload_json =
               json_remove(payload_json, '$.related_permit_id', '$.related_order_id')
               WHERE event_type IN ('OPERATOR_COMMAND_SUCCEEDED','OPERATOR_COMMAND_FAILED')"""
        )
        connection.execute(LEGACY_STATEMENTS[2])
        connection.execute(V3_STATEMENTS[1])
        connection.execute(V3_STATEMENTS[2])
        connection.execute("PRAGMA user_version = 3")
        connection.commit()
    finally:
        connection.close()


def downgrade_current_to_v4(path):
    connection = sqlite3.connect(path)
    try:
        downgrade_v7_objects(connection)
        connection.execute("DROP TRIGGER operator_terminal_contract")
        connection.execute(V4_STATEMENTS[2])
        connection.execute("DROP TRIGGER unknown_resolution_contract")
        connection.execute(V4_STATEMENTS[3])
        connection.execute("DROP TRIGGER schema_metadata_no_update")
        connection.execute("DROP TRIGGER schema_metadata_no_delete")
        connection.execute("DROP TABLE schema_metadata")
        connection.execute("DROP TRIGGER ledger_events_no_update")
        connection.execute(
            """UPDATE ledger_events SET payload_json =
               json_remove(payload_json, '$.related_permit_id', '$.related_order_id')
               WHERE event_type IN ('OPERATOR_COMMAND_SUCCEEDED','OPERATOR_COMMAND_FAILED')"""
        )
        connection.execute(LEGACY_STATEMENTS[2])
        connection.execute("PRAGMA user_version = 4")
        connection.commit()
    finally:
        connection.close()


def downgrade_current_to_v5(path):
    connection = sqlite3.connect(path)
    try:
        downgrade_v7_objects(connection)
        connection.execute("DROP TRIGGER unknown_resolution_contract")
        connection.execute(V4_STATEMENTS[3])
        connection.execute("PRAGMA user_version = 5")
        connection.commit()
    finally:
        connection.close()


def downgrade_current_to_v6(path):
    connection = sqlite3.connect(path)
    try:
        downgrade_v7_objects(connection)
        connection.execute("PRAGMA user_version = 6")
        connection.commit()
    finally:
        connection.close()


class ProxyLedger:
    def __init__(self, real, *, fail_reserve=False, fail_terminal=False):
        self.real = real
        self.fail_reserve = fail_reserve
        self.fail_terminal = fail_terminal

    def __getattr__(self, name):
        return getattr(self.real, name)

    def reserve_operator_command(self, *args):
        if self.fail_reserve:
            raise sqlite3.OperationalError("requested write failed")
        return self.real.reserve_operator_command(*args)

    def complete_operator_command(self, *args):
        if self.fail_terminal:
            raise sqlite3.OperationalError("terminal write failed")
        return self.real.complete_operator_command(*args)


class SchemaMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "ledger.db"

    def tearDown(self):
        self.temp.cleanup()

    def test_fresh_current_and_reopen_are_idempotent(self):
        ledger = SQLiteLedger(self.path)
        self.assertEqual(ledger.schema_version, SCHEMA_VERSION)
        self.assertEqual(
            ledger.runtime_identity, os.path.normcase(str(self.path.resolve()))
        )
        with self.assertRaises(AttributeError):
            ledger.runtime_identity = "different"
        self.assertEqual(ledger.connection.execute("PRAGMA journal_mode").fetchone(), ("wal",))
        self.assertEqual(ledger.connection.execute("PRAGMA synchronous").fetchone(), (2,))
        self.assertEqual(ledger.connection.execute("PRAGMA foreign_keys").fetchone(), (1,))
        ledger.append(LedgerEvent("e1", "AUDIT", "a", NOW, {}))
        ledger.close()
        reopened = SQLiteLedger(self.path)
        self.assertEqual(reopened.highest_sequence(), 1)
        reopened.close()
        memory = SQLiteLedger(":memory:")
        self.assertEqual(memory.connection.execute("PRAGMA journal_mode").fetchone(), ("memory",))
        self.assertEqual(memory.connection.execute("PRAGMA synchronous").fetchone(), (2,))
        self.assertEqual(memory.connection.execute("PRAGMA foreign_keys").fetchone(), (1,))
        memory.close()

    def test_hard_linked_database_is_rejected_without_mutating_the_ledger(self):
        ledger = SQLiteLedger(self.path)
        ledger.append(LedgerEvent("hardlink-event", "AUDIT", "hardlink", NOW, {}))
        ledger.close()
        before = self.path.read_bytes()
        alias = Path(self.temp.name) / "hardlink-alias.db"
        try:
            os.link(self.path, alias)
        except OSError as error:
            self.skipTest(f"hard links are unavailable: {error}")
        self.assertGreater(self.path.stat().st_nlink, 1)

        for candidate in (self.path, alias):
            with self.subTest(candidate=candidate):
                with self.assertRaises(SchemaError):
                    SQLiteLedger(candidate)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(alias.read_bytes(), before)
        self.assertFalse(Path(f"{self.path}-wal").exists())
        self.assertFalse(Path(f"{alias}-wal").exists())

        connection = sqlite3.connect(
            f"{self.path.resolve().as_uri()}?immutable=1", uri=True
        )
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT event_id FROM ledger_events WHERE aggregate_id = 'hardlink'"
                ).fetchone(),
                ("hardlink-event",),
            )
        finally:
            connection.close()

    def test_reserve_submission_atomically_starts_and_consumes_permit_once(self):
        ledger = SQLiteLedger(self.path)
        payload = {
            "request": {"account_id": "acct"},
            "permit": {"permit_id": "permit-1"},
        }
        self.assertTrue(reserve_started(ledger, "order-1", payload, "order-1"))
        self.assertEqual(
            [event.event_type for event in ledger.events_for("order-1")],
            ["PREPARED", "SUBMISSION_STARTED"],
        )
        self.assertFalse(reserve_started(ledger, "order-1", payload, "duplicate"))
        with self.assertRaises(PermitAlreadyConsumed):
            reserve_started(ledger, "order-2", payload, "order-2")
        self.assertEqual(ledger.events_for("order-2"), ())
        self.assertIsNone(ledger.connection.execute(
            "SELECT 1 FROM order_requests WHERE client_order_id = 'order-2'"
        ).fetchone())
        ledger.close()

    def test_reserve_submission_rolls_back_order_permit_and_prepared_on_started_failure(self):
        ledger = SQLiteLedger(self.path)
        payload = {
            "request": {"account_id": "acct"},
            "permit": {"permit_id": "rollback-permit"},
        }
        duplicate_id = "same-event-id"
        with self.assertRaises(sqlite3.IntegrityError):
            ledger.reserve_submission(
                "rollback-order",
                payload,
                LedgerEvent(duplicate_id, "PREPARED", "rollback-order", NOW, {}),
                LedgerEvent(
                    duplicate_id, "SUBMISSION_STARTED", "rollback-order", NOW, {}
                ),
                "rollback-permit",
            )
        self.assertEqual(ledger.events_for("rollback-order"), ())
        self.assertIsNone(ledger.connection.execute(
            "SELECT 1 FROM order_requests WHERE client_order_id = 'rollback-order'"
        ).fetchone())
        self.assertTrue(reserve_started(ledger, "replacement", payload, "replacement"))
        ledger.close()

    def test_database_trigger_rejects_direct_sql_state_machine_bypasses(self):
        ledger = SQLiteLedger(self.path)

        def raw_event(event_id, event_type, aggregate_id):
            ledger.connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES (?,?,?,?,?,'{}')""",
                (event_id, event_type, aggregate_id, NOW.isoformat(), NOW.isoformat()),
            )

        with self.assertRaises(sqlite3.IntegrityError):
            raw_event("orphan-prepared", "PREPARED", "orphan")
        with self.assertRaises(sqlite3.IntegrityError):
            raw_event("orphan-started", "SUBMISSION_STARTED", "orphan")

        ledger.connection.execute(
            "INSERT INTO order_requests VALUES ('skipped-start','{}',?)", (NOW.isoformat(),)
        )
        raw_event("skipped-prepared", "PREPARED", "skipped-start")
        with self.assertRaises(sqlite3.IntegrityError):
            raw_event("skipped-ack", "ACKNOWLEDGED", "skipped-start")

        reserve_started(
            ledger, "terminal-order", {"request": {"account_id": "acct"}}, "terminal"
        )
        raw_event("terminal-audit", "AUDIT_NOTE", "terminal-order")
        raw_event("terminal-ack", "ACKNOWLEDGED", "terminal-order")
        with self.assertRaises(sqlite3.IntegrityError):
            raw_event("duplicate-terminal", "SUBMITTED_UNKNOWN", "terminal-order")
        with self.assertRaises(sqlite3.IntegrityError):
            raw_event("reverse-start", "SUBMISSION_STARTED", "terminal-order")
        ledger.close()

    def test_permit_expression_index_rejects_direct_sql_reuse(self):
        ledger = SQLiteLedger(self.path)
        canonical = json.dumps({"permit": {"permit_id": "raw-permit"}})
        ledger.connection.execute(
            "INSERT INTO order_requests VALUES ('raw-order-1',?,?)",
            (canonical, NOW.isoformat()),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            ledger.connection.execute(
                "INSERT INTO order_requests VALUES ('raw-order-2',?,?)",
                (canonical, NOW.isoformat()),
            )
        ledger.close()

    def test_v7_rejects_new_operator_audit_rows_using_grandfathered_shape(self):
        ledger = SQLiteLedger(self.path)
        legacy = {
            "command_id": "legacy-shape",
            "actor": "operator",
            "reason": "tamper check",
            "deployment_version": "deploy-v1",
            "expected_safety_epoch": 0,
            "requested_at": NOW.isoformat(),
            "expires_at": (NOW + timedelta(minutes=1)).isoformat(),
            "action": "HALT",
            "account_id": None,
        }
        ledger.connection.execute(
            "INSERT INTO operator_commands VALUES (?,?,?)",
            ("legacy-shape", json.dumps(legacy), NOW.isoformat()),
        )
        ledger.connection.execute(
            """INSERT INTO ledger_events
               (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
               VALUES ('legacy-requested','OPERATOR_COMMAND_REQUESTED','legacy-shape',?,?,?)""",
            (
                NOW.isoformat(),
                NOW.isoformat(),
                json.dumps({**legacy, "previous_state": "BOOTSTRAPPING"}),
            ),
        )
        ledger.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(self.path)

    def test_v6_valid_history_migrates_and_enforces_v7_contracts(self):
        ledger = SQLiteLedger(self.path)
        reserve_started(ledger, "legacy-valid", {"request": {"account_id": "acct"}})
        ledger.close()
        downgrade_current_to_v6(self.path)
        migrated = SQLiteLedger(self.path)
        self.assertEqual(migrated.schema_version, 7)
        with self.assertRaises(sqlite3.IntegrityError):
            migrated.append(
                LedgerEvent("legacy-second-start", "SUBMISSION_STARTED", "legacy-valid", NOW, {})
            )
        migrated.close()

    def test_v6_invalid_history_and_duplicate_permit_fail_closed(self):
        invalid_history = Path(self.temp.name) / "invalid-history.db"
        ledger = SQLiteLedger(invalid_history)
        ledger.close()
        downgrade_current_to_v6(invalid_history)
        connection = sqlite3.connect(invalid_history)
        connection.execute(
            "INSERT INTO order_requests VALUES ('legacy-order','{}',?)", (NOW.isoformat(),)
        )
        for event_id, event_type in (("legacy-prepared", "PREPARED"), ("legacy-ack", "ACKNOWLEDGED")):
            connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES (?,?, 'legacy-order', ?, ?, '{}')""",
                (event_id, event_type, NOW.isoformat(), NOW.isoformat()),
            )
        connection.commit()
        connection.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(invalid_history)

        duplicate_permit = Path(self.temp.name) / "duplicate-permit.db"
        ledger = SQLiteLedger(duplicate_permit)
        ledger.close()
        downgrade_current_to_v6(duplicate_permit)
        connection = sqlite3.connect(duplicate_permit)
        canonical = json.dumps({"permit": {"permit_id": "legacy-duplicate"}})
        connection.executemany(
            "INSERT INTO order_requests VALUES (?,?,?)",
            (("legacy-1", canonical, NOW.isoformat()), ("legacy-2", canonical, NOW.isoformat())),
        )
        connection.commit()
        connection.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(duplicate_permit)

    def test_exact_legacy_migrates_data_and_preserves_immutability(self):
        connection = sqlite3.connect(self.path)
        connection.executescript(LEGACY_SCHEMA)
        connection.execute(
            "INSERT INTO ledger_events VALUES (1,'e1','AUDIT','a',?,?,?)",
            (NOW.isoformat(), NOW.isoformat(), "{}"),
        )
        connection.commit()
        connection.close()
        ledger = SQLiteLedger(self.path)
        self.assertEqual(ledger.highest_sequence(), 1)
        self.assertEqual(ledger.schema_version, SCHEMA_VERSION)
        with self.assertRaises(sqlite3.IntegrityError):
            ledger.connection.execute("DELETE FROM ledger_events")
        ledger.close()

    def test_partial_tampered_and_newer_schemas_fail_closed(self):
        connection = sqlite3.connect(self.path)
        connection.execute("CREATE TABLE ledger_events(sequence INTEGER PRIMARY KEY)")
        connection.commit()
        connection.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(self.path)

        newer = Path(self.temp.name) / "newer.db"
        connection = sqlite3.connect(newer)
        connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
        connection.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(newer)

        tampered = Path(self.temp.name) / "tampered.db"
        connection = sqlite3.connect(tampered)
        connection.executescript(
            LEGACY_SCHEMA.replace("event_type TEXT NOT NULL", "event_type TEXT")
        )
        connection.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(tampered)

    def test_interleaved_non_submission_event_cannot_hide_incomplete_submit(self):
        ledger = SQLiteLedger(self.path)
        reserve_started(ledger, "order", {"request": {"account_id": "acct"}}, "order")
        ledger.append(LedgerEvent("audit", "AUDIT_NOTE", "order", NOW, {}))
        self.assertEqual(ledger.incomplete_submissions(), ("order",))
        ledger.append(LedgerEvent("ack", "ACKNOWLEDGED", "order", NOW, {}))
        self.assertEqual(ledger.incomplete_submissions(), ())
        ledger.close()

    def test_incomplete_submissions_can_be_filtered_by_immutable_account_alias(self):
        ledger = SQLiteLedger(self.path)
        reserve_started(
            ledger, "acct-a-pending", {"request": {"account_id": "acct-a"}}, "a-pending"
        )
        reserve_started(
            ledger, "acct-a-done", {"request": {"account_id": "acct-a"}}, "a-done"
        )
        ledger.append(LedgerEvent("a-done-ack", "ACKNOWLEDGED", "acct-a-done", NOW, {}))
        reserve_started(
            ledger, "acct-b-pending", {"request": {"account_id": "acct-b"}}, "b-pending"
        )
        self.assertEqual(
            ledger.incomplete_submissions(), ("acct-a-pending", "acct-b-pending")
        )
        self.assertEqual(ledger.incomplete_submissions("acct-a"), ("acct-a-pending",))
        self.assertEqual(ledger.incomplete_submissions("acct-b"), ("acct-b-pending",))
        self.assertEqual(ledger.incomplete_submissions("acct-c"), ())
        with self.assertRaises(ValueError):
            ledger.incomplete_submissions(" ")
        ledger.close()

    def test_unresolved_unknowns_can_be_filtered_by_immutable_account_alias(self):
        ledger = SQLiteLedger(self.path)
        for client_order_id, account_id in (
            ("unknown-a-1", "acct-a"),
            ("unknown-b-1", "acct-b"),
            ("unknown-a-2", "acct-a"),
        ):
            reserve_started(
                ledger,
                client_order_id,
                {"request": {"account_id": account_id}},
                client_order_id,
            )
            ledger.append(
                LedgerEvent(
                    f"{client_order_id}-unknown",
                    "SUBMITTED_UNKNOWN",
                    client_order_id,
                    NOW,
                    {},
                )
            )
        self.assertEqual(
            ledger.unresolved_unknown_submissions(),
            ("unknown-a-1", "unknown-b-1", "unknown-a-2"),
        )
        self.assertEqual(
            ledger.unresolved_unknown_submissions("acct-a"),
            ("unknown-a-1", "unknown-a-2"),
        )
        self.assertEqual(
            ledger.unresolved_unknown_submissions("acct-b"), ("unknown-b-1",)
        )
        self.assertEqual(ledger.unresolved_unknown_submissions("acct-c"), ())
        with self.assertRaises(ValueError):
            ledger.unresolved_unknown_submissions("")
        ledger.close()

    def test_known_v1_migrates_forward_to_current(self):
        ledger = SQLiteLedger(self.path)
        ledger.append(LedgerEvent("v1-data", "AUDIT", "a", NOW, {}))
        ledger.close()
        connection = sqlite3.connect(self.path)
        try:
            downgrade_v7_objects(connection)
            connection.execute("DROP TRIGGER operator_terminal_contract")
            connection.execute("DROP TRIGGER unknown_resolution_contract")
            connection.execute("DROP TRIGGER schema_metadata_no_update")
            connection.execute("DROP TRIGGER schema_metadata_no_delete")
            connection.execute("DROP TABLE schema_metadata")
            connection.execute("PRAGMA user_version = 1")
            connection.commit()
        finally:
            connection.close()
        migrated = SQLiteLedger(self.path)
        self.assertEqual(migrated.schema_version, SCHEMA_VERSION)
        self.assertEqual(migrated.events_for("a")[0].event_id, "v1-data")
        migrated.close()

    def test_v1_with_orphan_operator_terminal_fails_closed(self):
        ledger = SQLiteLedger(self.path)
        ledger.close()
        connection = sqlite3.connect(self.path)
        try:
            downgrade_v7_objects(connection)
            connection.execute("DROP TRIGGER operator_terminal_contract")
            connection.execute("DROP TRIGGER unknown_resolution_contract")
            connection.execute("DROP TRIGGER schema_metadata_no_update")
            connection.execute("DROP TRIGGER schema_metadata_no_delete")
            connection.execute("DROP TABLE schema_metadata")
            connection.execute("PRAGMA user_version = 1")
            connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('orphan-v1','OPERATOR_COMMAND_FAILED','missing',?,?,?)""",
                (NOW.isoformat(), NOW.isoformat(), '{"result_state":"HALTED","error":null}'),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(self.path)

    def test_known_v2_valid_audit_chain_migrates_to_v3(self):
        ledger = SQLiteLedger(self.path)
        safety = SafetyController()
        service = OperatorCommandService(
            ledger, safety, "deploy-v1", lambda: NOW, account_id="acct"
        )
        service.halt(command(safety, command_id="valid-v2-command"))
        ledger.close()
        downgrade_current_to_v2(self.path)
        migrated = SQLiteLedger(self.path)
        self.assertEqual(migrated.schema_version, SCHEMA_VERSION)
        self.assertEqual(
            [event.event_type for event in migrated.events_for("valid-v2-command")],
            ["OPERATOR_COMMAND_REQUESTED", "OPERATOR_COMMAND_SUCCEEDED"],
        )
        migrated.close()

    def test_v2_malformed_terminal_payload_fails_migration(self):
        ledger = SQLiteLedger(self.path)
        safety = SafetyController()
        pending = command(safety, command_id="bad-v2-terminal")
        ledger.reserve_operator_command(
            pending,
            LedgerEvent(
                "bad-v2-requested",
                "OPERATOR_COMMAND_REQUESTED",
                pending.command_id,
                NOW,
                {
                    **ledger.canonical_command(pending),
                    "previous_state": safety.state.value,
                },
            ),
        )
        ledger.close()
        downgrade_current_to_v2(self.path)
        connection = sqlite3.connect(self.path)
        try:
            connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('bad-v2-terminal-event','OPERATOR_COMMAND_FAILED',?,?,?,?)""",
                (pending.command_id, NOW.isoformat(), NOW.isoformat(), '{"unexpected":true}'),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(self.path)

    def test_v2_terminal_before_requested_fails_migration(self):
        ledger = SQLiteLedger(self.path)
        safety = SafetyController()
        pending = command(safety, command_id="reordered-v2")
        ledger.close()
        downgrade_current_to_v2(self.path)
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("DROP TRIGGER operator_terminal_requires_chain")
            canonical = json.dumps(SQLiteLedger.canonical_command(pending), sort_keys=True)
            connection.execute(
                "INSERT INTO operator_commands VALUES (?,?,?)",
                (pending.command_id, canonical, NOW.isoformat()),
            )
            connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('early-terminal','OPERATOR_COMMAND_FAILED',?,?,?,?)""",
                (
                    pending.command_id,
                    NOW.isoformat(),
                    NOW.isoformat(),
                    '{"result_state":"HALTED","error":"Early"}',
                ),
            )
            requested_payload = {
                **SQLiteLedger.canonical_command(pending),
                "previous_state": safety.state.value,
            }
            connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('late-requested','OPERATOR_COMMAND_REQUESTED',?,?,?,?)""",
                (
                    pending.command_id,
                    NOW.isoformat(),
                    NOW.isoformat(),
                    json.dumps(requested_payload),
                ),
            )
            connection.execute(V2_STATEMENTS[0])
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(self.path)

    def test_v2_malformed_unknown_resolution_fails_migration(self):
        ledger = SQLiteLedger(self.path)
        safety = SafetyController()
        ledger.reserve_submission(
            "unknown-order",
            {"request": {"account_id": "acct"}},
            LedgerEvent("prepared", "PREPARED", "unknown-order", NOW, {}),
            LedgerEvent("started", "SUBMISSION_STARTED", "unknown-order", NOW, {}),
            None,
        )
        ledger.append(
            LedgerEvent("unknown", "SUBMITTED_UNKNOWN", "unknown-order", NOW, {})
        )
        resolution = command(
            safety,
            OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            "bad-resolution-v2",
            account_id="acct",
            client_order_id="unknown-order",
        )
        ledger.reserve_operator_command(
            resolution,
            LedgerEvent(
                "resolution-requested",
                "OPERATOR_COMMAND_REQUESTED",
                resolution.command_id,
                NOW,
                {
                    **ledger.canonical_command(resolution),
                    "previous_state": safety.state.value,
                },
            ),
        )
        ledger.close()
        downgrade_current_to_v2(self.path)
        connection = sqlite3.connect(self.path)
        try:
            connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('bad-resolution','SUBMITTED_UNKNOWN_RESOLVED',?,?,?,?)""",
                (
                    "unknown-order",
                    NOW.isoformat(),
                    NOW.isoformat(),
                    json.dumps(
                        {
                            "operator_command_id": resolution.command_id,
                            "result": "CONFIRMED_ABSENT",
                            "observation": "",
                            "reference": "case",
                            "observed_at": NOW.isoformat(),
                        }
                    ),
                ),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(self.path)

    def test_valid_v2_unknown_resolution_chain_migrates_and_reopens(self):
        ledger = SQLiteLedger(self.path)
        safety = SafetyController()
        ledger.reserve_submission(
            "valid-unknown-order",
            {"request": {"account_id": "acct"}},
            LedgerEvent("valid-prepared", "PREPARED", "valid-unknown-order", NOW, {}),
            LedgerEvent("valid-started", "SUBMISSION_STARTED", "valid-unknown-order", NOW, {}),
            None,
        )
        ledger.append(
            LedgerEvent(
                "valid-unknown", "SUBMITTED_UNKNOWN", "valid-unknown-order", NOW, {}
            )
        )
        safety.block_unknown_submission("valid-unknown-order")
        service = OperatorCommandService(
            ledger, safety, "deploy-v1", lambda: NOW, account_id="acct"
        )
        service.resolve_unknown_submission(
            command(
                safety,
                OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
                "valid-resolution-v2",
                account_id="acct",
                client_order_id="valid-unknown-order",
            ),
            "valid-unknown-order",
            UnknownResolutionEvidence(
                UnknownResolutionResult.CONFIRMED_ABSENT,
                "broker inquiry",
                "case-valid",
                NOW,
            ),
        )
        ledger.close()
        downgrade_current_to_v2(self.path)
        migrated = SQLiteLedger(self.path)
        self.assertEqual(migrated.unresolved_unknown_submissions(), ())
        migrated.close()
        reopened = SQLiteLedger(self.path)
        self.assertEqual(reopened.schema_version, SCHEMA_VERSION)
        reopened.close()

    def test_known_v3_valid_chain_migrates_to_v4(self):
        ledger = SQLiteLedger(self.path)
        safety = SafetyController()
        service = OperatorCommandService(
            ledger, safety, "deploy-v1", lambda: NOW, account_id="acct"
        )
        service.halt(command(safety, command_id="valid-v3-command"))
        ledger.close()
        downgrade_current_to_v3(self.path)
        migrated = SQLiteLedger(self.path)
        self.assertEqual(migrated.schema_version, SCHEMA_VERSION)
        migrated.close()

    def test_known_v4_terminal_is_grandfathered_without_ledger_update(self):
        ledger = SQLiteLedger(self.path)
        safety = SafetyController()
        service = OperatorCommandService(
            ledger, safety, "deploy-v1", lambda: NOW, account_id="acct"
        )
        service.halt(command(safety, command_id="valid-v4-command"))
        ledger.close()
        downgrade_current_to_v4(self.path)
        migrated = SQLiteLedger(self.path)
        terminal = migrated.events_for("valid-v4-command")[-1]
        self.assertEqual(set(terminal.payload), {"result_state", "error"})
        cutoff = int(migrated.connection.execute(
            "SELECT value FROM schema_metadata WHERE key='terminal_payload_v5_cutoff'"
        ).fetchone()[0])
        sequence = migrated.connection.execute(
            "SELECT sequence FROM ledger_events WHERE event_id = ?", (terminal.event_id,)
        ).fetchone()[0]
        self.assertLessEqual(sequence, cutoff)
        migrated.close()

    def test_known_v5_migrates_forward_to_current(self):
        ledger = SQLiteLedger(self.path)
        ledger.close()
        downgrade_current_to_v5(self.path)
        migrated = SQLiteLedger(self.path)
        self.assertEqual(migrated.schema_version, SCHEMA_VERSION)
        migrated.close()

    def test_v3_duplicate_terminal_keys_fail_migration(self):
        ledger = SQLiteLedger(self.path)
        safety = SafetyController()
        pending = command(safety, command_id="duplicate-v3-terminal")
        ledger.reserve_operator_command(
            pending,
            LedgerEvent(
                "duplicate-v3-requested",
                "OPERATOR_COMMAND_REQUESTED",
                pending.command_id,
                NOW,
                {
                    **ledger.canonical_command(pending),
                    "previous_state": safety.state.value,
                },
            ),
        )
        ledger.close()
        downgrade_current_to_v3(self.path)
        connection = sqlite3.connect(self.path)
        try:
            connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('duplicate-v3-terminal-event','OPERATOR_COMMAND_FAILED',?,?,?,?)""",
                (
                    pending.command_id,
                    NOW.isoformat(),
                    NOW.isoformat(),
                    '{"result_state":"HALTED","result_state":"READY"}',
                ),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(self.path)

    def test_v3_duplicate_resolution_keys_fail_migration(self):
        ledger = SQLiteLedger(self.path)
        safety = SafetyController()
        ledger.reserve_submission(
            "duplicate-v3-order",
            {"request": {"account_id": "acct"}},
            LedgerEvent("duplicate-prepared", "PREPARED", "duplicate-v3-order", NOW, {}),
            LedgerEvent("duplicate-started", "SUBMISSION_STARTED", "duplicate-v3-order", NOW, {}),
            None,
        )
        ledger.append(
            LedgerEvent(
                "duplicate-unknown", "SUBMITTED_UNKNOWN", "duplicate-v3-order", NOW, {}
            )
        )
        resolution = command(
            safety,
            OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            "duplicate-v3-resolution",
            account_id="acct",
            client_order_id="duplicate-v3-order",
        )
        ledger.reserve_operator_command(
            resolution,
            LedgerEvent(
                "duplicate-resolution-requested",
                "OPERATOR_COMMAND_REQUESTED",
                resolution.command_id,
                NOW,
                {
                    **ledger.canonical_command(resolution),
                    "previous_state": safety.state.value,
                },
            ),
        )
        ledger.close()
        downgrade_current_to_v3(self.path)
        connection = sqlite3.connect(self.path)
        duplicate_payload = (
            '{"operator_command_id":"duplicate-v3-resolution",'
            '"observation":"one","observation":"two",'
            '"reference":"case","observed_at":"2026-08-25T04:00:00+00:00"}'
        )
        try:
            connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('duplicate-v3-resolution-event','SUBMITTED_UNKNOWN_RESOLVED',?,?,?,?)""",
                (
                    "duplicate-v3-order",
                    NOW.isoformat(),
                    NOW.isoformat(),
                    duplicate_payload,
                ),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(self.path)

    def test_v3_duplicate_command_canonical_key_fails_migration(self):
        ledger = SQLiteLedger(self.path)
        ledger.close()
        downgrade_current_to_v3(self.path)
        duplicate_canonical = (
            '{"command_id":"duplicate-canonical","actor":"one","actor":"two",'
            '"reason":"reason","deployment_version":"deploy-v1",'
            '"expected_safety_epoch":0,"requested_at":"2026-08-25T04:00:00+00:00",'
            '"expires_at":"2026-08-25T04:01:00+00:00","action":"HALT",'
            '"account_id":null}'
        )
        connection = sqlite3.connect(self.path)
        try:
            connection.execute(
                "INSERT INTO operator_commands VALUES ('duplicate-canonical',?,?)",
                (duplicate_canonical, NOW.isoformat()),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(self.path)

    def test_v5_future_unknown_evidence_fails_closed_during_migration(self):
        safety = SafetyController()
        ledger = SQLiteLedger(self.path)
        ledger.reserve_submission(
            "future-v5-order",
            {"request": {"account_id": "acct"}},
            LedgerEvent("future-v5-prepared", "PREPARED", "future-v5-order", NOW, {}),
            LedgerEvent("future-v5-started", "SUBMISSION_STARTED", "future-v5-order", NOW, {}),
            None,
        )
        ledger.append(
            LedgerEvent("future-v5-unknown", "SUBMITTED_UNKNOWN", "future-v5-order", NOW, {})
        )
        resolution = command(
            safety,
            OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            "future-v5-command",
            account_id="acct",
            client_order_id="future-v5-order",
        )
        ledger.reserve_operator_command(
            resolution,
            LedgerEvent(
                "future-v5-requested",
                "OPERATOR_COMMAND_REQUESTED",
                resolution.command_id,
                NOW,
                {**ledger.canonical_command(resolution), "previous_state": safety.state.value},
            ),
        )
        ledger.close()
        downgrade_current_to_v5(self.path)
        connection = sqlite3.connect(self.path)
        try:
            connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('future-v5-resolution','SUBMITTED_UNKNOWN_RESOLVED',?,?,?,?)""",
                (
                    "future-v5-order",
                    NOW.isoformat(),
                    NOW.isoformat(),
                    json.dumps(
                        {
                            "operator_command_id": resolution.command_id,
                            "result": "CONFIRMED_ABSENT",
                            "observation": "future broker observation",
                            "reference": "case-future-v5",
                            "observed_at": (NOW + timedelta(seconds=1)).isoformat(),
                        }
                    ),
                ),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(self.path)


class OperatorBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "ledger.db"
        self.ledger = SQLiteLedger(self.path)
        self.safety = SafetyController()

    def tearDown(self):
        self.ledger.close()
        self.temp.cleanup()

    def service(self, ledger=None, safety=None):
        return OperatorCommandService(
            ledger or self.ledger,
            safety or self.safety,
            "deploy-v1",
            lambda: NOW,
            account_id="acct",
        )

    def test_requested_is_committed_before_effect_and_terminal_after(self):
        service = self.service()
        original_halt = self.safety.halt

        def observed_halt(blocker=None):
            self.assertEqual(
                [event.event_type for event in self.ledger.events_for("command-1")],
                ["OPERATOR_COMMAND_REQUESTED"],
            )
            original_halt(blocker)

        self.safety.halt = observed_halt
        service.halt(command(self.safety))
        self.assertEqual(
            [event.event_type for event in self.ledger.events_for("command-1")],
            ["OPERATOR_COMMAND_REQUESTED", "OPERATOR_COMMAND_SUCCEEDED"],
        )
        self.assertEqual(
            dict(self.ledger.events_for("command-1")[-1].payload),
            {
                "result_state": "HALTED",
                "error": None,
                "related_permit_id": None,
                "related_order_id": None,
            },
        )

    def test_high_risk_permit_terminal_records_returned_permit_id(self):
        self.safety.halt()
        service = self.service()
        permit = service.issue_permit(
            command(
                self.safety,
                OperatorAction.ISSUE_CANCEL,
                "permit-correlation",
                account_id="acct",
            )
        )
        self.assertEqual(permit.scope, PermitScope.CANCEL)
        terminal = self.ledger.events_for("permit-correlation")[-1]
        self.assertEqual(terminal.payload["related_permit_id"], permit.permit_id)
        self.assertIsNone(terminal.payload["related_order_id"])

    def test_duplicate_same_or_different_payload_cannot_apply_second_effect(self):
        service = self.service()
        used = command(self.safety)
        service.halt(used)
        epoch = self.safety.epoch
        with self.assertRaises(OperatorCommandConflict):
            service.halt(used)
        with self.assertRaises(OperatorCommandConflict):
            service.halt(command(self.safety, reason="different immutable payload"))
        self.assertEqual(self.safety.epoch, epoch)

    def test_requested_failure_has_zero_effect(self):
        self.safety.state = SafetyState.TRADING
        effect_called = False

        def forbidden_arm(_):
            nonlocal effect_called
            effect_called = True

        self.safety._arm = forbidden_arm
        service = self.service(ProxyLedger(self.ledger, fail_reserve=True))
        with self.assertRaises(OperatorPersistenceFailure):
            service.arm(command(self.safety, OperatorAction.ARM))
        self.assertFalse(effect_called)
        self.assertEqual(self.safety.state, SafetyState.HALTED)
        self.assertIn("PERSISTENCE_FAILURE", self.safety.blockers)

    def test_terminal_failure_halts_with_persistence_blocker(self):
        service = self.service(ProxyLedger(self.ledger, fail_terminal=True))
        with self.assertRaises(OperatorPersistenceFailure):
            service.halt(command(self.safety))
        self.assertEqual(self.safety.state, SafetyState.HALTED)
        self.assertIn("PERSISTENCE_FAILURE", self.safety.blockers)

    def test_pending_commands_are_filtered_to_the_matching_account(self):
        for command_id, account_id in (
            ("acct-pending", "acct"),
            ("other-pending", "other"),
        ):
            pending = command(
                self.safety,
                OperatorAction.HALT,
                command_id,
                account_id=account_id,
            )
            self.ledger.reserve_operator_command(
                pending,
                LedgerEvent(
                    f"{command_id}-requested",
                    "OPERATOR_COMMAND_REQUESTED",
                    command_id,
                    NOW,
                    {
                        **self.ledger.canonical_command(pending),
                        "previous_state": self.safety.state.value,
                    },
                ),
            )
        self.assertEqual(
            self.ledger.pending_operator_commands(),
            ("acct-pending", "other-pending"),
        )
        self.assertEqual(
            self.ledger.pending_operator_commands("acct"),
            ("acct-pending",),
        )
        self.assertEqual(
            self.ledger.pending_operator_commands("other"),
            ("other-pending",),
        )
        with self.assertRaises(ValueError):
            self.ledger.pending_operator_commands(" ")

    def test_pending_requested_command_blocks_restart_without_replay(self):
        pending = command(self.safety)
        requested_payload = {
            **self.ledger.canonical_command(pending),
            "previous_state": self.safety.state.value,
        }
        self.ledger.reserve_operator_command(
            pending,
            LedgerEvent(
                str(uuid4()),
                "OPERATOR_COMMAND_REQUESTED",
                pending.command_id,
                NOW,
                requested_payload,
            ),
        )
        restarted = SafetyController()
        self.service(safety=restarted)
        self.assertEqual(restarted.state, SafetyState.HALTED)
        self.assertIn("PENDING_OPERATOR_COMMAND:command-1", restarted.blockers)
        self.assertEqual(
            [event.event_type for event in self.ledger.events_for("command-1")],
            ["OPERATOR_COMMAND_REQUESTED"],
        )

    def test_raw_operator_ids_are_rejected(self):
        with self.assertRaises(TypeError):
            self.safety.arm("raw-id", NOW)
        with self.assertRaises(TypeError):
            self.safety.issue_permit(
                "acct", "CANCEL", NOW, operator_command_id="raw-id"
            )

    def test_pre_effect_clock_failure_halts_trading_and_records_failed(self):
        self.safety.state = SafetyState.TRADING
        effect_called = False

        def forbidden_arm(_):
            nonlocal effect_called
            effect_called = True

        self.safety._arm = forbidden_arm

        def failed_clock():
            raise RuntimeError("clock unavailable")

        service = OperatorCommandService(
            self.ledger, self.safety, "deploy-v1", failed_clock, account_id="acct"
        )
        arm = command(self.safety, OperatorAction.ARM, "clock-command")
        with self.assertRaises(OperatorCommandRejected):
            service.arm(arm)
        self.assertFalse(effect_called)
        self.assertEqual(self.safety.state, SafetyState.HALTED)
        self.assertIn("CLOCK_FAILURE", self.safety.blockers)
        self.assertEqual(
            [event.event_type for event in self.ledger.events_for("clock-command")],
            ["OPERATOR_COMMAND_REQUESTED", "OPERATOR_COMMAND_FAILED"],
        )

    def test_operator_terminal_boundary_and_database_reject_orphans(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('orphan','OPERATOR_COMMAND_FAILED','missing',?,?,?)""",
                (NOW.isoformat(), NOW.isoformat(), '{"result_state":"HALTED","error":null}'),
            )
        no_request = command(self.safety, command_id="no-request-command")
        self.ledger.connection.execute(
            "INSERT INTO operator_commands VALUES (?,?,?)",
            (
                no_request.command_id,
                json.dumps(self.ledger.canonical_command(no_request)),
                NOW.isoformat(),
            ),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('missing-request','OPERATOR_COMMAND_FAILED',?,?,?,?)""",
                (
                    no_request.command_id,
                    NOW.isoformat(),
                    NOW.isoformat(),
                    '{"result_state":"HALTED","error":"MissingRequested"}',
                ),
            )
        pending = command(self.safety, command_id="terminal-command")
        requested_payload = {
            **self.ledger.canonical_command(pending),
            "previous_state": self.safety.state.value,
        }
        self.ledger.reserve_operator_command(
            pending,
            LedgerEvent(
                "terminal-requested",
                "OPERATOR_COMMAND_REQUESTED",
                pending.command_id,
                NOW,
                requested_payload,
            ),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('raw-bad-terminal','OPERATOR_COMMAND_FAILED',?,?,?,?)""",
                (
                    pending.command_id,
                    NOW.isoformat(),
                    NOW.isoformat(),
                    '{"unexpected":true}',
                ),
            )
        duplicate_terminal = '{"result_state":"HALTED","result_state":"READY"}'
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('duplicate-terminal','OPERATOR_COMMAND_FAILED',?,?,?,?)""",
                (
                    pending.command_id,
                    NOW.isoformat(),
                    NOW.isoformat(),
                    duplicate_terminal,
                ),
            )
        valid_terminal_payload = json.dumps(
            {"result_state": "HALTED", "error": "InvalidRawTerminal"}
        )
        for event_id, occurred_at in (("", NOW.isoformat()), ("bad-utc", "2026-08-25")):
            with self.subTest(event_id=event_id, occurred_at=occurred_at):
                with self.assertRaises(sqlite3.IntegrityError):
                    self.ledger.connection.execute(
                        """INSERT INTO ledger_events
                           (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                           VALUES (?,'OPERATOR_COMMAND_FAILED',?,?,?,?)""",
                        (
                            event_id,
                            pending.command_id,
                            occurred_at,
                            NOW.isoformat(),
                            valid_terminal_payload,
                        ),
                    )
        with self.assertRaises(ValueError):
            self.ledger.complete_operator_command(
                pending.command_id,
                OperatorCommandOutcome.FAILED,
                LedgerEvent(
                    "bad-terminal",
                    "OPERATOR_COMMAND_FAILED",
                    pending.command_id,
                    NOW,
                    {"unexpected": True},
                ),
            )
        self.ledger.complete_operator_command(
            pending.command_id,
            OperatorCommandOutcome.FAILED,
            LedgerEvent(
                "good-terminal",
                "OPERATOR_COMMAND_FAILED",
                pending.command_id,
                NOW,
                {
                    "result_state": "HALTED",
                    "error": "ManualInvestigation",
                    "related_permit_id": None,
                    "related_order_id": None,
                },
            ),
        )
        with self.assertRaises(LedgerPersistenceError):
            self.ledger.complete_operator_command(
                pending.command_id,
                OperatorCommandOutcome.FAILED,
                LedgerEvent(
                    "second-terminal",
                    "OPERATOR_COMMAND_FAILED",
                    pending.command_id,
                    NOW,
                    {
                        "result_state": "HALTED",
                        "error": "Duplicate",
                        "related_permit_id": None,
                        "related_order_id": None,
                    },
                ),
            )

    def test_generic_paths_cannot_append_resolution_or_run_arbitrary_callback(self):
        self.assertFalse(hasattr(self.service(), "resolve_submitted_unknown"))
        with self.assertRaises(ValueError):
            self.ledger.append(
                LedgerEvent(
                    "forged-resolution",
                    "SUBMITTED_UNKNOWN_RESOLVED",
                    "order",
                    NOW,
                    {},
                )
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('raw-resolution','SUBMITTED_UNKNOWN_RESOLVED','order',?,?,?)""",
                (
                    NOW.isoformat(),
                    NOW.isoformat(),
                    json.dumps(
                        {
                            "operator_command_id": "missing",
                            "result": "CONFIRMED_ABSENT",
                            "observation": "broker check",
                            "reference": "case",
                            "observed_at": NOW.isoformat(),
                        }
                    ),
                ),
            )
        duplicate_resolution = (
            '{"operator_command_id":"raw-resolution-command",'
            '"observation":"one","observation":"two",'
            '"reference":"case","observed_at":"2026-08-25T04:00:00+00:00"}'
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('duplicate-resolution','SUBMITTED_UNKNOWN_RESOLVED',?,?,?,?)""",
                (
                    "raw-unknown-order",
                    NOW.isoformat(),
                    NOW.isoformat(),
                    duplicate_resolution,
                ),
            )

    def test_raw_resolution_with_valid_chain_but_empty_evidence_is_rejected(self):
        self.ledger.reserve_submission(
            "raw-unknown-order",
            {"request": {"account_id": "acct"}},
            LedgerEvent("raw-prepared", "PREPARED", "raw-unknown-order", NOW, {}),
            LedgerEvent("raw-started", "SUBMISSION_STARTED", "raw-unknown-order", NOW, {}),
            None,
        )
        self.ledger.append(
            LedgerEvent("raw-unknown", "SUBMITTED_UNKNOWN", "raw-unknown-order", NOW, {})
        )
        resolution = command(
            self.safety,
            OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            "raw-resolution-command",
            account_id="acct",
            client_order_id="raw-unknown-order",
        )
        self.ledger.reserve_operator_command(
            resolution,
            LedgerEvent(
                "raw-resolution-requested",
                "OPERATOR_COMMAND_REQUESTED",
                resolution.command_id,
                NOW,
                {
                    **self.ledger.canonical_command(resolution),
                    "previous_state": self.safety.state.value,
                },
            ),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('empty-evidence','SUBMITTED_UNKNOWN_RESOLVED',?,?,?,?)""",
                (
                    "raw-unknown-order",
                    NOW.isoformat(),
                    NOW.isoformat(),
                    json.dumps(
                        {
                            "operator_command_id": resolution.command_id,
                            "result": "CONFIRMED_ABSENT",
                            "observation": "",
                            "reference": "case",
                            "observed_at": NOW.isoformat(),
                        }
                    ),
                ),
            )

    def test_mismatched_unknown_resolution_target_is_rejected_durably(self):
        client_order_id = "target-order"
        self.ledger.reserve_submission(
            client_order_id,
            {"request": {"account_id": "acct"}},
            LedgerEvent("target-prepared", "PREPARED", client_order_id, NOW, {}),
            LedgerEvent("target-started", "SUBMISSION_STARTED", client_order_id, NOW, {}),
            None,
        )
        self.ledger.append(
            LedgerEvent("target-unknown", "SUBMITTED_UNKNOWN", client_order_id, NOW, {})
        )
        resolution = command(
            self.safety,
            OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            "wrong-target-command",
            account_id="acct",
            client_order_id="different-order",
        )
        self.ledger.reserve_operator_command(
            resolution,
            LedgerEvent(
                "wrong-target-requested",
                "OPERATOR_COMMAND_REQUESTED",
                resolution.command_id,
                NOW,
                {
                    **self.ledger.canonical_command(resolution),
                    "previous_state": self.safety.state.value,
                },
            ),
        )
        evidence = UnknownResolutionEvidence(
            UnknownResolutionResult.CONFIRMED_ABSENT,
            "broker inquiry",
            "wrong-target-case",
            NOW,
        )
        payload = {
            "operator_command_id": resolution.command_id,
            "result": evidence.result.value,
            "observation": evidence.observation,
            "reference": evidence.reference,
            "observed_at": evidence.observed_at.isoformat(),
        }
        event = LedgerEvent(
            "wrong-target-resolution",
            "SUBMITTED_UNKNOWN_RESOLVED",
            client_order_id,
            NOW,
            payload,
        )
        with self.assertRaises(ValueError):
            self.ledger.record_unknown_resolution(
                client_order_id, resolution, evidence, event
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES (?,?,?,?,?,?)""",
                (
                    event.event_id,
                    event.event_type,
                    event.aggregate_id,
                    event.occurred_at.isoformat(),
                    NOW.isoformat(),
                    json.dumps(payload),
                ),
            )
        self.assertFalse(self.ledger.connection.in_transaction)
        self.assertNotIn(
            "SUBMITTED_UNKNOWN_RESOLVED",
            [event.event_type for event in self.ledger.events_for(client_order_id)],
        )
        self.ledger.close()
        self.ledger = SQLiteLedger(self.path)
        self.assertNotIn(
            "SUBMITTED_UNKNOWN_RESOLVED",
            [event.event_type for event in self.ledger.events_for(client_order_id)],
        )

    def test_unknown_resolution_terminal_requires_matching_target_and_resolution(self):
        client_order_id = "terminal-target"
        self.ledger.reserve_submission(
            client_order_id,
            {"request": {"account_id": "acct"}},
            LedgerEvent("terminal-prepared", "PREPARED", client_order_id, NOW, {}),
            LedgerEvent("terminal-started", "SUBMISSION_STARTED", client_order_id, NOW, {}),
            None,
        )
        self.ledger.append(
            LedgerEvent("terminal-unknown", "SUBMITTED_UNKNOWN", client_order_id, NOW, {})
        )

        def reserve_resolution(command_id, target):
            resolution = command(
                self.safety,
                OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
                command_id,
                account_id="acct",
                client_order_id=target,
            )
            self.ledger.reserve_operator_command(
                resolution,
                LedgerEvent(
                    f"{command_id}-requested",
                    "OPERATOR_COMMAND_REQUESTED",
                    command_id,
                    NOW,
                    {
                        **self.ledger.canonical_command(resolution),
                        "previous_state": self.safety.state.value,
                    },
                ),
            )
            return resolution

        def terminal(command_id, outcome, related_order_id, event_id):
            return LedgerEvent(
                event_id,
                f"OPERATOR_COMMAND_{outcome.value}",
                command_id,
                NOW,
                {
                    "result_state": self.safety.state.value,
                    "error": None if outcome is OperatorCommandOutcome.SUCCEEDED else "failed",
                    "related_permit_id": None,
                    "related_order_id": related_order_id,
                },
            )

        missing_resolution = reserve_resolution("missing-resolution", client_order_id)
        missing_terminal = terminal(
            missing_resolution.command_id,
            OperatorCommandOutcome.SUCCEEDED,
            client_order_id,
            "missing-resolution-terminal",
        )
        with self.assertRaises(ValueError):
            self.ledger.complete_operator_command(
                missing_resolution.command_id,
                OperatorCommandOutcome.SUCCEEDED,
                missing_terminal,
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES (?,?,?,?,?,?)""",
                (
                    "missing-resolution-raw",
                    missing_terminal.event_type,
                    missing_terminal.aggregate_id,
                    NOW.isoformat(),
                    NOW.isoformat(),
                    json.dumps(missing_terminal.payload),
                ),
            )

        wrong_target = reserve_resolution("wrong-terminal-target", "different-order")
        wrong_terminal = terminal(
            wrong_target.command_id,
            OperatorCommandOutcome.FAILED,
            client_order_id,
            "wrong-target-terminal",
        )
        with self.assertRaises(ValueError):
            self.ledger.complete_operator_command(
                wrong_target.command_id,
                OperatorCommandOutcome.FAILED,
                wrong_terminal,
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES (?,?,?,?,?,?)""",
                (
                    "wrong-target-raw",
                    wrong_terminal.event_type,
                    wrong_terminal.aggregate_id,
                    NOW.isoformat(),
                    NOW.isoformat(),
                    json.dumps(wrong_terminal.payload),
                ),
            )

        correct = reserve_resolution("correct-terminal", client_order_id)
        evidence = UnknownResolutionEvidence(
            UnknownResolutionResult.CONFIRMED_ABSENT,
            "broker inquiry",
            "correct-terminal-case",
            NOW,
        )
        resolution_event = LedgerEvent(
            "correct-resolution",
            "SUBMITTED_UNKNOWN_RESOLVED",
            client_order_id,
            NOW,
            {
                "operator_command_id": correct.command_id,
                "result": evidence.result.value,
                "observation": evidence.observation,
                "reference": evidence.reference,
                "observed_at": evidence.observed_at.isoformat(),
            },
        )
        self.ledger.record_unknown_resolution(
            client_order_id, correct, evidence, resolution_event
        )
        correct_terminal = terminal(
            correct.command_id,
            OperatorCommandOutcome.SUCCEEDED,
            client_order_id,
            "correct-terminal-succeeded",
        )
        self.ledger.complete_operator_command(
            correct.command_id,
            OperatorCommandOutcome.SUCCEEDED,
            correct_terminal,
        )
        self.assertFalse(self.ledger.connection.in_transaction)
        self.ledger.close()
        self.ledger = SQLiteLedger(self.path)
        self.assertEqual(
            self.ledger.events_for(correct.command_id)[-1].event_type,
            "OPERATOR_COMMAND_SUCCEEDED",
        )

    def test_future_evidence_is_rejected_by_storage_boundary_and_database(self):
        client_order_id = "future-evidence-order"
        self.ledger.reserve_submission(
            client_order_id,
            {"request": {"account_id": "acct"}},
            LedgerEvent("future-prepared", "PREPARED", client_order_id, NOW, {}),
            LedgerEvent("future-started", "SUBMISSION_STARTED", client_order_id, NOW, {}),
            None,
        )
        self.ledger.append(
            LedgerEvent("future-unknown", "SUBMITTED_UNKNOWN", client_order_id, NOW, {})
        )
        resolution = command(
            self.safety,
            OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            "future-resolution-command",
            account_id="acct",
            client_order_id=client_order_id,
        )
        self.ledger.reserve_operator_command(
            resolution,
            LedgerEvent(
                "future-resolution-requested",
                "OPERATOR_COMMAND_REQUESTED",
                resolution.command_id,
                NOW,
                {
                    **self.ledger.canonical_command(resolution),
                    "previous_state": self.safety.state.value,
                },
            ),
        )
        evidence = UnknownResolutionEvidence(
            UnknownResolutionResult.CONFIRMED_ABSENT,
            "future broker observation",
            "case-future",
            NOW + timedelta(seconds=1),
        )
        payload = {
            "operator_command_id": resolution.command_id,
            "result": evidence.result.value,
            "observation": evidence.observation,
            "reference": evidence.reference,
            "observed_at": evidence.observed_at.isoformat(),
        }
        event = LedgerEvent(
            "future-resolution", "SUBMITTED_UNKNOWN_RESOLVED", client_order_id, NOW, payload
        )
        with self.assertRaises(ValueError):
            self.ledger.record_unknown_resolution(client_order_id, resolution, evidence, event)
        self.assertEqual(self.ledger.unresolved_unknown_submissions(), (client_order_id,))
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES (?,?,?,?,?,?)""",
                (
                    event.event_id,
                    event.event_type,
                    event.aggregate_id,
                    event.occurred_at.isoformat(),
                    NOW.isoformat(),
                    json.dumps(payload),
                ),
            )
        self.assertEqual(self.ledger.unresolved_unknown_submissions(), (client_order_id,))


class BackupRestoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.ledger = SQLiteLedger(self.root / "live.db")

    def tearDown(self):
        self.ledger.close()
        self.temp.cleanup()

    def test_wal_backup_manifest_and_isolated_restore(self):
        self.ledger.append(LedgerEvent("wal-event", "AUDIT", "a", NOW, {"value": 1}))
        self.assertTrue((self.root / "live.db-wal").exists())
        backup = self.root / "backup.db"
        manifest = self.ledger.backup(backup, app_version="0.1.0")
        data = json.loads(manifest.read_text(encoding="utf-8"))
        self.assertEqual(data["schema_version"], SCHEMA_VERSION)
        self.assertEqual(data["highest_ledger_sequence"], 1)
        self.assertNotIn("payload", data)
        restored = SQLiteLedger.restore(backup, self.root / "restored.db")
        self.assertEqual(restored.events_for("a")[0].event_id, "wal-event")
        self.assertTrue(restored.integrity_check())
        self.assertEqual(SafetyController().state, SafetyState.BOOTSTRAPPING)
        restored.close()

    def test_v6_backup_is_verified_migrated_and_restored_as_current(self):
        self.ledger.append(LedgerEvent("v6-backup-event", "AUDIT", "a", NOW, {}))
        backup = self.root / "v6-backup.db"
        manifest_path = self.ledger.backup(backup, app_version="0.1.0")
        downgrade_current_to_v6(backup)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["schema_version"] = 6
        manifest["sha256"] = hashlib.sha256(backup.read_bytes()).hexdigest()
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        destination = self.root / "v6-restored.db"
        restored = SQLiteLedger.restore(backup, destination)
        self.assertEqual(restored.schema_version, SCHEMA_VERSION)
        self.assertEqual(restored.events_for("a")[0].event_id, "v6-backup-event")
        self.assertTrue(restored.integrity_check())
        restored.close()

    def test_tampered_or_unsupported_legacy_backup_leaves_no_destination(self):
        self.ledger.append(LedgerEvent("legacy-backup-event", "AUDIT", "a", NOW, {}))
        unsupported = self.root / "unsupported.db"
        unsupported_manifest = self.ledger.backup(unsupported, app_version="0.1.0")
        manifest = json.loads(unsupported_manifest.read_text(encoding="utf-8"))
        manifest["schema_version"] = 5
        unsupported_manifest.write_text(json.dumps(manifest), encoding="utf-8")
        unsupported_destination = self.root / "unsupported-restored.db"
        with self.assertRaises(BackupError):
            SQLiteLedger.restore(unsupported, unsupported_destination)
        self.assertFalse(unsupported_destination.exists())

        tampered = self.root / "tampered-v6.db"
        tampered_manifest = self.ledger.backup(tampered, app_version="0.1.0")
        downgrade_current_to_v6(tampered)
        connection = sqlite3.connect(tampered)
        connection.execute("DROP TRIGGER ledger_events_no_delete")
        connection.commit()
        connection.close()
        manifest = json.loads(tampered_manifest.read_text(encoding="utf-8"))
        manifest["schema_version"] = 6
        manifest["sha256"] = hashlib.sha256(tampered.read_bytes()).hexdigest()
        tampered_manifest.write_text(json.dumps(manifest), encoding="utf-8")
        tampered_destination = self.root / "tampered-restored.db"
        with self.assertRaises(BackupError):
            SQLiteLedger.restore(tampered, tampered_destination)
        self.assertFalse(tampered_destination.exists())

    def test_corrupt_or_wrong_manifest_refused_without_destination(self):
        backup = self.root / "backup.db"
        manifest_path = self.ledger.backup(backup, app_version="0.1.0")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["highest_ledger_sequence"] = 99
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        destination = self.root / "refused.db"
        with self.assertRaises(BackupError):
            SQLiteLedger.restore(backup, destination)
        self.assertFalse(destination.exists())

    def test_restore_ignores_unhashed_sibling_wal(self):
        backup = self.root / "backup.db"
        self.ledger.backup(backup, app_version="0.1.0")
        original_hash = json.loads(
            SQLiteLedger._manifest_path(backup).read_text(encoding="utf-8")
        )["sha256"]
        attacker = sqlite3.connect(backup)
        attacker.execute("PRAGMA journal_mode = WAL")
        attacker.execute(
            """INSERT INTO ledger_events
               (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
               VALUES ('unhashed','AUDIT','a',?,?, '{}')""",
            (NOW.isoformat(), NOW.isoformat()),
        )
        attacker.commit()
        self.assertEqual(
            original_hash,
            hashlib.sha256(backup.read_bytes()).hexdigest(),
        )
        restored = SQLiteLedger.restore(backup, self.root / "restored.db")
        self.assertEqual(restored.highest_sequence(), 0)
        restored.close()
        attacker.close()

    def test_backup_overwrite_and_existing_restore_destination_are_refused(self):
        backup = self.root / "backup.db"
        self.ledger.backup(backup, app_version="0.1.0")
        with self.assertRaises(FileExistsError):
            self.ledger.backup(backup, app_version="0.1.0")
        destination = self.root / "existing.db"
        destination.touch()
        with self.assertRaises(FileExistsError):
            SQLiteLedger.restore(backup, destination)

    def test_backup_manifest_sequence_comes_from_snapshot_not_live_connection(self):
        self.ledger.append(LedgerEvent("snapshot-event", "AUDIT", "a", NOW, {}))
        with patch.object(
            self.ledger,
            "highest_sequence",
            side_effect=AssertionError("live sequence must not be sampled"),
        ):
            manifest_path = self.ledger.backup(
                self.root / "snapshot.db", app_version="0.1.0"
            )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["highest_ledger_sequence"], 1)

    def test_backup_publish_race_never_overwrites_racer(self):
        backup = self.root / "raced-backup.db"
        original_link = os.link

        def race_database(source, destination):
            destination = Path(destination)
            if destination == backup and not destination.exists():
                destination.write_bytes(b"racer-database")
            return original_link(source, destination)

        with patch(
            "trader.adapters.persistence.sqlite_ledger.os.link",
            side_effect=race_database,
        ):
            with self.assertRaises(FileExistsError):
                self.ledger.backup(backup, app_version="0.1.0")
        self.assertEqual(backup.read_bytes(), b"racer-database")

    def test_manifest_publish_race_retains_orphan_database_for_investigation(self):
        backup = self.root / "manifest-race.db"
        manifest = SQLiteLedger._manifest_path(backup)
        original_link = os.link

        def race_manifest(source, destination):
            destination = Path(destination)
            if destination == manifest and not destination.exists():
                destination.write_bytes(b"racer-manifest")
            return original_link(source, destination)

        with patch(
            "trader.adapters.persistence.sqlite_ledger.os.link",
            side_effect=race_manifest,
        ):
            with self.assertRaises(FileExistsError):
                self.ledger.backup(backup, app_version="0.1.0")
        self.assertTrue(backup.exists())
        self.assertEqual(manifest.read_bytes(), b"racer-manifest")

    def test_restore_publish_race_preserves_racer_destination(self):
        backup = self.root / "restore-source.db"
        self.ledger.backup(backup, app_version="0.1.0")
        destination = self.root / "restore-race.db"
        original_link = os.link

        def race_restore(source, target):
            target = Path(target)
            if target == destination and not target.exists():
                target.write_bytes(b"racer-restore")
            return original_link(source, target)

        with patch(
            "trader.adapters.persistence.sqlite_ledger.os.link",
            side_effect=race_restore,
        ):
            with self.assertRaises(FileExistsError):
                SQLiteLedger.restore(backup, destination)
        self.assertEqual(destination.read_bytes(), b"racer-restore")

    def test_restore_rejects_compatible_post_publish_wal_and_retains_artifacts(self):
        self.ledger.append(LedgerEvent("manifest-seq-1", "AUDIT", "a", NOW, {}))
        backup = self.root / "wal-race-source.db"
        self.ledger.backup(backup, app_version="0.1.0")
        destination = self.root / "wal-race-restored.db"
        original_link = os.link
        racer_connections = []

        def publish_then_add_wal(source, target):
            result = original_link(source, target)
            if Path(target) == destination:
                racer = sqlite3.connect(destination)
                racer.execute("PRAGMA journal_mode = WAL")
                racer.execute(
                    """INSERT INTO ledger_events
                       (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                       VALUES ('unmanifested-seq-2','AUDIT','a',?,?, '{}')""",
                    (NOW.isoformat(), NOW.isoformat()),
                )
                racer.commit()
                racer_connections.append(racer)
            return result

        try:
            with patch(
                "trader.adapters.persistence.sqlite_ledger.os.link",
                side_effect=publish_then_add_wal,
            ):
                with self.assertRaises(BackupError):
                    SQLiteLedger.restore(backup, destination)
            self.assertTrue(destination.exists())
            self.assertTrue(Path(f"{destination}-wal").exists())
            self.assertEqual(
                racer_connections[0].execute(
                    "SELECT MAX(sequence) FROM ledger_events"
                ).fetchone()[0],
                2,
            )
            self.assertFalse(hasattr(SQLiteLedger, "_unlink_if_same"))
        finally:
            for connection in racer_connections:
                connection.close()
        destination.unlink()
        Path(f"{destination}-wal").touch()
        with self.assertRaises(FileExistsError):
            SQLiteLedger.restore(backup, destination)


if __name__ == "__main__":
    unittest.main()
