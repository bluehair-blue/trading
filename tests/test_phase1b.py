import hashlib
import json
import os
import sqlite3
import tempfile
import threading
import unittest
from datetime import date, datetime, timedelta, timezone
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
    V7_STATEMENTS,
    V10_RESOLUTION_SQL,
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
from trader.domain.broker_observations import (
    BrokerOrderLinked,
    BrokerOrderRef,
    ConfirmedAbsent,
    ManualActivityLinked,
    ResolutionQueryEvidence,
    canonical_resolution_payload,
)
from trader.domain.models import (
    OperatorAction,
    OperatorCommand,
    OperatorCommandOutcome,
    InstrumentId,
    PermitScope,
    SafetyState,
    ReservationTerms,
    Side,
)
from trader.ports.ledger import (
    LedgerEvent,
    LedgerPersistenceError,
    OperatorCommandConflict,
    PermitAlreadyConsumed,
    ReservationCapacityExceeded,
)
from trader.ports.broker import BrokerEnvironment

NOW = datetime(2026, 8, 25, 4, tzinfo=timezone.utc)


def confirmed_absent(
    observed_at=NOW,
    account_id="acct",
    environment=BrokerEnvironment.SIMULATED,
):
    return ConfirmedAbsent(ResolutionQueryEvidence(
        environment, account_id, date(2026, 8, 25),
        observed_at, observed_at, "unknown-resolution-v1", ("broker.orders.read",),
        ("broker.orders.read",), True, ("observation-1",), "a" * 64, (), observed_at,
    ))


def current_order_payload(
    permit_id="test-permit",
    environment=BrokerEnvironment.SIMULATED,
):
    return {
        "environment": environment.value,
        "request": {
            "account_id": "acct",
            "instrument": {"market": "NASDAQ", "symbol": "AAPL", "currency": "USD"},
            "side": "BUY", "quantity": "2",
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
    }


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
        "environment": safety.environment,
    }
    values.update(changes)
    return OperatorCommand(**values)


def reserve_started(ledger, client_order_id, canonical_payload, prefix="submission"):
    supplied = dict(canonical_payload)
    supplied_permit = supplied.get("permit")
    default_permit_id = (
        supplied_permit.get("permit_id")
        if isinstance(supplied_permit, dict)
        else f"{prefix}-permit"
    )
    canonical_payload = current_order_payload(default_permit_id)
    canonical_payload.update(supplied)
    request_payload = dict(current_order_payload(default_permit_id)["request"])
    request_payload.update(canonical_payload.get("request", {}))
    canonical_payload["request"] = request_payload
    risk_payload = dict(current_order_payload(default_permit_id)["risk"])
    risk_payload.update(canonical_payload.get("risk", {}))
    canonical_payload["risk"] = risk_payload
    canonical_payload.setdefault("environment", BrokerEnvironment.SIMULATED.value)
    canonical_payload.setdefault("permit", {
        "permit_id": f"{prefix}-permit",
        "environment": canonical_payload["environment"],
    })
    permit = canonical_payload.get("permit")
    if isinstance(permit, dict):
        permit = dict(permit)
        permit.setdefault("environment", canonical_payload["environment"])
        canonical_payload["permit"] = permit
    permit_id = None if permit is None else permit["permit_id"]
    return ledger.reserve_submission(
        client_order_id,
        canonical_payload,
        LedgerEvent(f"{prefix}-prepared", "PREPARED", client_order_id, NOW, {}),
        LedgerEvent(f"{prefix}-started", "SUBMISSION_STARTED", client_order_id, NOW, {}),
        permit_id,
        reservation_terms(
            account_id=canonical_payload["request"]["account_id"],
            environment=BrokerEnvironment(canonical_payload["environment"]),
        ),
    )


def reservation_terms(
    *,
    account_id="acct",
    capacity_minor=10_000_000,
    environment=BrokerEnvironment.SIMULATED,
):
    return ReservationTerms(
        account_id, "snap", environment, "policy-v1",
        InstrumentId("NASDAQ", "AAPL", "USD"), Side.BUY, 2, "USD", "USD",
        capacity_minor, 0, 1, capacity_minor, capacity_minor, 0,
        20_000, 20_000, 0,
    )


def downgrade_v7_objects(connection):
    connection.execute("DROP TRIGGER risk_event_contract")
    connection.execute("DROP INDEX risk_reserved_once")
    connection.execute("DROP TRIGGER risk_reservations_no_update")
    connection.execute("DROP TRIGGER risk_reservations_no_delete")
    connection.execute("DROP TABLE risk_reservations")
    connection.execute("DROP TRIGGER ledger_events_no_delete")
    connection.execute(
        "DELETE FROM ledger_events WHERE event_type IN ('RISK_RESERVED','RISK_RELEASED')"
    )
    connection.execute(LEGACY_STATEMENTS[3])
    connection.execute("DROP TRIGGER order_environment_contract")
    connection.execute("DROP TRIGGER operator_environment_contract")
    connection.execute("DROP TRIGGER submission_state_contract")
    connection.execute("DROP INDEX order_request_permit_once")
    connection.execute("DROP TRIGGER operator_terminal_contract")
    connection.execute(V5_STATEMENTS[4])
    connection.execute("DROP TRIGGER unknown_resolution_contract")
    connection.execute(V6_STATEMENTS[1])
    connection.execute("DROP TRIGGER operator_commands_no_update")
    connection.execute(
        """UPDATE operator_commands SET canonical_json = json_remove(canonical_json,
           '$.client_order_id', '$.risk_decision_id', '$.execution_plan_id',
           '$.environment')"""
    )
    connection.execute("DROP TRIGGER ledger_events_no_update")
    connection.execute(
        """UPDATE ledger_events SET payload_json = json_remove(payload_json,
           '$.client_order_id', '$.risk_decision_id', '$.execution_plan_id',
           '$.environment')
           WHERE event_type = 'OPERATOR_COMMAND_REQUESTED'"""
    )
    connection.execute(LEGACY_STATEMENTS[2])
    connection.execute(
        """CREATE TRIGGER operator_commands_no_update BEFORE UPDATE ON operator_commands BEGIN
           SELECT RAISE(ABORT, 'operator commands are immutable'); END"""
    )
    connection.execute("DROP TRIGGER order_requests_no_update")
    connection.execute(
        """UPDATE order_requests SET canonical_json =
           json_remove(canonical_json,
             '$.environment', '$.permit.environment',
             '$.risk.input_snapshot_environment',
             '$.plan.market_evidence.environment',
             '$.plan.market_evidence.pricing_policy_version',
             '$.plan.market_evidence.minimum_limit_price',
             '$.plan.market_evidence.maximum_limit_price')"""
    )
    connection.execute(LEGACY_STATEMENTS[4])
    connection.execute("DROP TRIGGER schema_metadata_no_delete")
    connection.execute(
        """DELETE FROM schema_metadata WHERE key IN (
           'operator_binding_v7_cutoff', 'order_environment_v8_cutoff',
           'operator_environment_v8_cutoff', 'risk_reservation_v9_order_cutoff',
           'typed_resolution_v10_cutoff')"""
    )
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
            """UPDATE ledger_events SET payload_json=json_object(
                 'operator_command_id',json_extract(payload_json,'$.operator_command_id'),
                 'result',json_extract(payload_json,'$.result'),
                 'observation','grandfathered broker query',
                 'reference','grandfathered-case',
                 'observed_at',json_extract(payload_json,'$.query.fetched_at'))
               WHERE event_type='SUBMITTED_UNKNOWN_RESOLVED'"""
        )
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


def downgrade_current_to_v9(path):
    connection = sqlite3.connect(path)
    try:
        connection.execute("DROP TRIGGER unknown_resolution_contract")
        connection.execute("DROP TRIGGER ledger_events_no_update")
        connection.execute(
            """UPDATE ledger_events SET payload_json=json_object(
                 'operator_command_id',json_extract(payload_json,'$.operator_command_id'),
                 'result',json_extract(payload_json,'$.result'),
                 'observation','grandfathered broker query',
                 'reference','grandfathered-case',
                 'observed_at',json_extract(payload_json,'$.query.fetched_at'))
               WHERE event_type='SUBMITTED_UNKNOWN_RESOLVED'"""
        )
        connection.execute(LEGACY_STATEMENTS[2])
        connection.execute(V7_STATEMENTS[5])
        connection.execute("DROP TRIGGER schema_metadata_no_delete")
        connection.execute(
            "DELETE FROM schema_metadata WHERE key='typed_resolution_v10_cutoff'"
        )
        connection.execute(
            """CREATE TRIGGER schema_metadata_no_delete BEFORE DELETE ON schema_metadata BEGIN
               SELECT RAISE(ABORT, 'schema metadata is immutable'); END"""
        )
        connection.execute("PRAGMA user_version = 9")
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
        payload = current_order_payload("permit-1")
        self.assertTrue(reserve_started(ledger, "order-1", payload, "order-1"))
        self.assertEqual(
            [event.event_type for event in ledger.events_for("order-1")],
            ["RISK_RESERVED", "PREPARED", "SUBMISSION_STARTED"],
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
        payload = current_order_payload("rollback-permit")
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
                reservation_terms(),
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
            "INSERT INTO order_requests VALUES ('skipped-start',?,?)",
            (json.dumps(current_order_payload("skipped-permit")), NOW.isoformat()),
        )
        raw_event("skipped-prepared", "PREPARED", "skipped-start")
        with self.assertRaises(sqlite3.IntegrityError):
            raw_event("skipped-ack", "ACKNOWLEDGED", "skipped-start")

        reserve_started(
            ledger, "terminal-order", current_order_payload(), "terminal"
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
        canonical = json.dumps(current_order_payload("raw-permit"))
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

    def test_v8_rejects_missing_or_mismatched_evidence_contracts(self):
        ledger = SQLiteLedger(self.path)
        cases = []
        missing_account_environment = current_order_payload("evidence-a")
        del missing_account_environment["risk"]["input_snapshot_environment"]
        cases.append(missing_account_environment)
        wrong_market_environment = current_order_payload("evidence-b")
        wrong_market_environment["plan"]["market_evidence"]["environment"] = "PAPER"
        cases.append(wrong_market_environment)
        wrong_policy = current_order_payload("evidence-c")
        wrong_policy["plan"]["market_evidence"]["pricing_policy_version"] = "other"
        cases.append(wrong_policy)
        widened_band = current_order_payload("evidence-d")
        widened_band["plan"]["market_evidence"]["maximum_limit_price"] = "102"
        cases.append(widened_band)
        for index, canonical in enumerate(cases):
            with self.subTest(index=index), self.assertRaises(sqlite3.IntegrityError):
                ledger.connection.execute(
                    "INSERT INTO order_requests VALUES (?,?,?)",
                    (f"evidence-{index}", json.dumps(canonical), NOW.isoformat()),
                )
        ledger.close()

    def test_v8_reopen_rejects_tampered_authoritative_market_band(self):
        ledger = SQLiteLedger(self.path)
        reserve_started(
            ledger, "tampered-band", current_order_payload("tampered-permit")
        )
        ledger.connection.execute("DROP TRIGGER order_requests_no_update")
        ledger.connection.execute(
            """UPDATE order_requests SET canonical_json = json_set(
               canonical_json, '$.plan.market_evidence.maximum_limit_price', '102')
               WHERE client_order_id = 'tampered-band'"""
        )
        ledger.connection.execute(LEGACY_STATEMENTS[4])
        ledger.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(self.path)

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
        with self.assertRaises(sqlite3.IntegrityError):
            ledger.connection.execute(
                "INSERT INTO operator_commands VALUES (?,?,?)",
                ("legacy-shape", json.dumps(legacy), NOW.isoformat()),
            )
        ledger.close()

    def test_v6_valid_history_migrates_and_enforces_v7_contracts(self):
        ledger = SQLiteLedger(self.path)
        reserve_started(ledger, "legacy-valid", current_order_payload())
        ledger.close()
        downgrade_current_to_v6(self.path)
        migrated = SQLiteLedger(self.path)
        self.assertEqual(migrated.schema_version, SCHEMA_VERSION)
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
        reserve_started(ledger, "order", current_order_payload(), "order")
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

    def test_recovery_queries_isolate_same_alias_by_environment(self):
        ledger = SQLiteLedger(self.path)
        for environment in (BrokerEnvironment.PAPER, BrokerEnvironment.LIVE):
            prefix = environment.value.lower()
            for state in ("pending", "unknown"):
                order_id = f"{prefix}-{state}"
                reserve_started(
                    ledger,
                    order_id,
                    current_order_payload(
                        f"{order_id}-permit", environment=environment
                    ),
                    order_id,
                )
                if state == "unknown":
                    ledger.append(LedgerEvent(
                        f"{order_id}-unknown",
                        "SUBMITTED_UNKNOWN",
                        order_id,
                        NOW,
                        {},
                    ))

        self.assertEqual(
            ledger.incomplete_submissions("acct", BrokerEnvironment.PAPER),
            ("paper-pending",),
        )
        self.assertEqual(
            ledger.incomplete_submissions("acct", BrokerEnvironment.LIVE),
            ("live-pending",),
        )
        self.assertEqual(
            ledger.unresolved_unknown_submissions(
                "acct", BrokerEnvironment.PAPER
            ),
            ("paper-unknown",),
        )
        self.assertEqual(
            ledger.unresolved_unknown_submissions(
                "acct", BrokerEnvironment.LIVE
            ),
            ("live-unknown",),
        )
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
        safety = SafetyController(BrokerEnvironment.SIMULATED)
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
        safety = SafetyController(BrokerEnvironment.SIMULATED)
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
        safety = SafetyController(BrokerEnvironment.SIMULATED)
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
        safety = SafetyController(BrokerEnvironment.SIMULATED)
        ledger.reserve_submission(
            "unknown-order",
            current_order_payload(),
            LedgerEvent("prepared", "PREPARED", "unknown-order", NOW, {}),
            LedgerEvent("started", "SUBMISSION_STARTED", "unknown-order", NOW, {}),
            "test-permit",
            reservation_terms(),
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
        safety = SafetyController(BrokerEnvironment.SIMULATED)
        ledger.reserve_submission(
            "valid-unknown-order",
            current_order_payload(),
            LedgerEvent("valid-prepared", "PREPARED", "valid-unknown-order", NOW, {}),
            LedgerEvent("valid-started", "SUBMISSION_STARTED", "valid-unknown-order", NOW, {}),
            "test-permit",
            reservation_terms(),
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
            confirmed_absent(),
        )
        ledger.close()
        downgrade_current_to_v2(self.path)
        migrated = SQLiteLedger(self.path)
        self.assertEqual(migrated.unresolved_unknown_submissions(), ())
        migrated.close()
        reopened = SQLiteLedger(self.path)
        self.assertEqual(reopened.schema_version, SCHEMA_VERSION)
        reopened.close()

    def test_v9_legacy_free_text_resolution_is_grandfathered_on_v10_migration(self):
        ledger = SQLiteLedger(self.path)
        safety = SafetyController(BrokerEnvironment.SIMULATED)
        order_id = "v9-legacy-resolution"
        ledger.reserve_submission(
            order_id, current_order_payload(),
            LedgerEvent("v9-prepared", "PREPARED", order_id, NOW, {}),
            LedgerEvent("v9-started", "SUBMISSION_STARTED", order_id, NOW, {}),
            "test-permit", reservation_terms(),
        )
        ledger.append(LedgerEvent("v9-unknown", "SUBMITTED_UNKNOWN", order_id, NOW, {}))
        safety.block_unknown_submission(order_id)
        OperatorCommandService(
            ledger, safety, "deploy-v1", lambda: NOW, account_id="acct",
        ).resolve_unknown_submission(
            command(
                safety, OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
                "v9-resolution-command", account_id="acct", client_order_id=order_id,
            ),
            order_id,
            confirmed_absent(),
        )
        ledger.close()
        downgrade_current_to_v9(self.path)

        migrated = SQLiteLedger(self.path)
        self.assertEqual(migrated.schema_version, SCHEMA_VERSION)
        self.assertEqual(migrated.unresolved_unknown_submissions(), ())
        resolution = next(
            event for event in migrated.events_for(order_id)
            if event.event_type == "SUBMITTED_UNKNOWN_RESOLVED"
        )
        self.assertEqual(
            set(resolution.payload),
            {"operator_command_id", "result", "observation", "reference", "observed_at"},
        )
        migrated.close()

    def test_known_v3_valid_chain_migrates_to_v4(self):
        ledger = SQLiteLedger(self.path)
        safety = SafetyController(BrokerEnvironment.SIMULATED)
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
        safety = SafetyController(BrokerEnvironment.SIMULATED)
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
        safety = SafetyController(BrokerEnvironment.SIMULATED)
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
        safety = SafetyController(BrokerEnvironment.SIMULATED)
        ledger.reserve_submission(
            "duplicate-v3-order",
            current_order_payload(),
            LedgerEvent("duplicate-prepared", "PREPARED", "duplicate-v3-order", NOW, {}),
            LedgerEvent("duplicate-started", "SUBMISSION_STARTED", "duplicate-v3-order", NOW, {}),
            "test-permit",
            reservation_terms(),
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
        safety = SafetyController(BrokerEnvironment.SIMULATED)
        ledger = SQLiteLedger(self.path)
        ledger.reserve_submission(
            "future-v5-order",
            current_order_payload(),
            LedgerEvent("future-v5-prepared", "PREPARED", "future-v5-order", NOW, {}),
            LedgerEvent("future-v5-started", "SUBMISSION_STARTED", "future-v5-order", NOW, {}),
            "test-permit",
            reservation_terms(),
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
        self.safety = SafetyController(BrokerEnvironment.SIMULATED)

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

    def _unknown(self, order_id):
        self.ledger.reserve_submission(
            order_id, current_order_payload(f"{order_id}-permit"),
            LedgerEvent(f"{order_id}-prepared", "PREPARED", order_id, NOW, {}),
            LedgerEvent(f"{order_id}-started", "SUBMISSION_STARTED", order_id, NOW, {}),
            f"{order_id}-permit", reservation_terms(),
        )
        self.ledger.append(LedgerEvent(
            f"{order_id}-unknown", "SUBMITTED_UNKNOWN", order_id, NOW, {},
        ))
        self.safety.block_unknown_submission(order_id)

    def test_typed_resolution_variants_persist_exact_payload_and_release_only_absent(self):
        ref = BrokerOrderRef(
            BrokerEnvironment.SIMULATED, "acct", date(2026, 8, 25), "broker-1",
        )
        query_one = ResolutionQueryEvidence(
            BrokerEnvironment.SIMULATED, "acct", date(2026, 8, 25), NOW, NOW,
            "unknown-resolution-v1", ("broker.orders.read",),
            ("broker.orders.read",), True,
            ("observation-1",), "a" * 64, (ref,), NOW,
        )
        variants = {
            "absent": confirmed_absent(),
            "linked": BrokerOrderLinked(ref, "broker.orders.read", query_one),
            "manual": ManualActivityLinked(ref, "manual-1", "operator", NOW, query_one),
        }
        for name, evidence in variants.items():
            with self.subTest(name=name):
                order_id = f"typed-{name}"
                self._unknown(order_id)
                resolution = command(
                    self.safety, OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
                    f"resolve-{name}", account_id="acct", client_order_id=order_id,
                )
                self.service().resolve_unknown_submission(resolution, order_id, evidence)
                events = self.ledger.events_for(order_id)
                stored = next(
                    event for event in events
                    if event.event_type == "SUBMITTED_UNKNOWN_RESOLVED"
                )
                self.assertEqual(
                    dict(stored.payload),
                    canonical_resolution_payload(resolution.command_id, evidence),
                )
                self.assertEqual(
                    "RISK_RELEASED" in [event.event_type for event in events],
                    name == "absent",
                )
                self.assertNotIn(f"SUBMITTED_UNKNOWN:{order_id}", self.safety.blockers)

    def test_unknown_resolution_requires_order_command_and_evidence_environment(self):
        order_id = "paper-order-live-resolution"
        self.ledger.reserve_submission(
            order_id,
            current_order_payload(
                f"{order_id}-permit", environment=BrokerEnvironment.PAPER
            ),
            LedgerEvent(f"{order_id}-prepared", "PREPARED", order_id, NOW, {}),
            LedgerEvent(
                f"{order_id}-started", "SUBMISSION_STARTED", order_id, NOW, {}
            ),
            f"{order_id}-permit",
            reservation_terms(environment=BrokerEnvironment.PAPER),
        )
        self.ledger.append(LedgerEvent(
            f"{order_id}-unknown", "SUBMITTED_UNKNOWN", order_id, NOW, {}
        ))
        live_safety = SafetyController(BrokerEnvironment.LIVE)
        resolution = command(
            live_safety,
            OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            "live-resolution-command",
            client_order_id=order_id,
        )
        self.ledger.reserve_operator_command(
            resolution,
            LedgerEvent(
                "live-resolution-requested",
                "OPERATOR_COMMAND_REQUESTED",
                resolution.command_id,
                NOW,
                {
                    **self.ledger.canonical_command(resolution),
                    "previous_state": live_safety.state.value,
                },
            ),
        )
        evidence = confirmed_absent(environment=BrokerEnvironment.LIVE)
        event = LedgerEvent(
            "live-resolution",
            "SUBMITTED_UNKNOWN_RESOLVED",
            order_id,
            NOW,
            canonical_resolution_payload(resolution.command_id, evidence),
        )

        with self.assertRaises(ValueError):
            self.ledger.record_unknown_resolution(
                order_id, resolution, evidence, event
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
                    json.dumps(event.payload),
                ),
            )

        self.ledger.connection.execute("DROP TRIGGER unknown_resolution_contract")
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
                json.dumps(event.payload),
            ),
        )
        self.ledger.connection.execute(V10_RESOLUTION_SQL)
        self.ledger.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(self.path)

    def test_resolution_release_fault_rolls_back_resolution_and_release(self):
        order_id = "resolution-rollback"
        self._unknown(order_id)
        resolution = command(
            self.safety, OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            "resolution-rollback-command", account_id="acct", client_order_id=order_id,
        )
        service = self.service()
        original = self.ledger._insert_full_release

        def fail_release(*unused):
            raise sqlite3.OperationalError("fault injection")

        self.ledger._insert_full_release = fail_release
        try:
            with self.assertRaises(OperatorPersistenceFailure):
                service.resolve_unknown_submission(
                    resolution, order_id, confirmed_absent(),
                )
        finally:
            self.ledger._insert_full_release = original
        self.assertEqual(self.ledger.unresolved_unknown_submissions(), (order_id,))
        self.assertNotIn(
            "SUBMITTED_UNKNOWN_RESOLVED",
            [event.event_type for event in self.ledger.events_for(order_id)],
        )
        self.assertIn(f"SUBMITTED_UNKNOWN:{order_id}", self.safety.blockers)

    def test_direct_sql_cannot_misclassify_one_candidate_as_absent(self):
        order_id = "direct-sql-candidate"
        self._unknown(order_id)
        resolution = command(
            self.safety, OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            "direct-sql-command", account_id="acct", client_order_id=order_id,
        )
        self.ledger.reserve_operator_command(
            resolution,
            LedgerEvent(
                "direct-sql-requested", "OPERATOR_COMMAND_REQUESTED",
                resolution.command_id, NOW,
                {
                    **self.ledger.canonical_command(resolution),
                    "previous_state": self.safety.state.value,
                },
            ),
        )
        payload = canonical_resolution_payload(
            resolution.command_id, confirmed_absent(),
        )
        payload["query"]["candidate_count"] = 1
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('direct-sql-resolution','SUBMITTED_UNKNOWN_RESOLVED',?,?,?,?)""",
                (order_id, NOW.isoformat(), NOW.isoformat(), json.dumps(payload)),
            )
        ref = BrokerOrderRef(
            BrokerEnvironment.SIMULATED, "acct", date(2026, 8, 25), "candidate-1",
        )
        query = ResolutionQueryEvidence(
            BrokerEnvironment.SIMULATED, "acct", date(2026, 8, 25), NOW, NOW,
            "unknown-resolution-v1", ("broker.orders.read",),
            ("broker.orders.read",), True, ("observation-1",), "a" * 64, (ref,), NOW,
        )
        payload = canonical_resolution_payload(
            resolution.command_id,
            BrokerOrderLinked(ref, "broker.orders.read", query),
        )
        payload["broker_order_ref"]["broker_order_id"] = "not-the-candidate"
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute(
                """INSERT INTO ledger_events
                   (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                   VALUES ('direct-sql-membership','SUBMITTED_UNKNOWN_RESOLVED',?,?,?,?)""",
                (order_id, NOW.isoformat(), NOW.isoformat(), json.dumps(payload)),
            )
        payload = canonical_resolution_payload(
            resolution.command_id, confirmed_absent(),
        )
        payload["query"]["candidate_set_sha256"] = "0" * 64
        self.ledger.connection.execute(
            """INSERT INTO ledger_events
               (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
               VALUES ('direct-sql-hash','SUBMITTED_UNKNOWN_RESOLVED',?,?,?,?)""",
            (order_id, NOW.isoformat(), NOW.isoformat(), json.dumps(payload)),
        )
        self.ledger.close()
        with self.assertRaises(SchemaError):
            SQLiteLedger(self.path)

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
        restarted = SafetyController(BrokerEnvironment.SIMULATED)
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
            current_order_payload(),
            LedgerEvent("raw-prepared", "PREPARED", "raw-unknown-order", NOW, {}),
            LedgerEvent("raw-started", "SUBMISSION_STARTED", "raw-unknown-order", NOW, {}),
            "test-permit",
            reservation_terms(),
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
            current_order_payload(),
            LedgerEvent("target-prepared", "PREPARED", client_order_id, NOW, {}),
            LedgerEvent("target-started", "SUBMISSION_STARTED", client_order_id, NOW, {}),
            "test-permit",
            reservation_terms(),
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
        evidence = confirmed_absent()
        payload = canonical_resolution_payload(resolution.command_id, evidence)
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
            current_order_payload(),
            LedgerEvent("terminal-prepared", "PREPARED", client_order_id, NOW, {}),
            LedgerEvent("terminal-started", "SUBMISSION_STARTED", client_order_id, NOW, {}),
            "test-permit",
            reservation_terms(),
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
        evidence = confirmed_absent()
        resolution_event = LedgerEvent(
            "correct-resolution",
            "SUBMITTED_UNKNOWN_RESOLVED",
            client_order_id,
            NOW,
            canonical_resolution_payload(correct.command_id, evidence),
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
            current_order_payload(),
            LedgerEvent("future-prepared", "PREPARED", client_order_id, NOW, {}),
            LedgerEvent("future-started", "SUBMISSION_STARTED", client_order_id, NOW, {}),
            "test-permit",
            reservation_terms(),
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
        evidence = confirmed_absent(NOW + timedelta(seconds=1))
        payload = canonical_resolution_payload(resolution.command_id, evidence)
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
        self.assertEqual(
            SafetyController(BrokerEnvironment.SIMULATED).state,
            SafetyState.BOOTSTRAPPING,
        )
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

    def test_v9_backup_is_verified_migrated_and_restored_as_current(self):
        self.ledger.append(LedgerEvent("v9-backup-event", "AUDIT", "a", NOW, {}))
        backup = self.root / "v9-backup.db"
        manifest_path = self.ledger.backup(backup, app_version="0.1.0")
        downgrade_current_to_v9(backup)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["schema_version"] = 9
        manifest["sha256"] = hashlib.sha256(backup.read_bytes()).hexdigest()
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        restored = SQLiteLedger.restore(backup, self.root / "v9-restored.db")
        self.assertEqual(restored.schema_version, SCHEMA_VERSION)
        self.assertEqual(restored.events_for("a")[0].event_id, "v9-backup-event")
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


class RiskReservationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "risk.db"
        ledger = SQLiteLedger(self.path)
        ledger.close()

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def _reserve(ledger, order_id, terms=None):
        payload = current_order_payload(f"{order_id}-permit")
        return ledger.reserve_submission(
            order_id, payload,
            LedgerEvent(f"{order_id}-prepared", "PREPARED", order_id, NOW, {}),
            LedgerEvent(f"{order_id}-started", "SUBMISSION_STARTED", order_id, NOW, {}),
            f"{order_id}-permit", terms or reservation_terms(),
        )

    def test_concurrent_account_capacity_admits_exactly_one(self):
        barrier = threading.Barrier(2)
        results = []

        def compete(order_id):
            ledger = SQLiteLedger(self.path)
            try:
                barrier.wait()
                self._reserve(ledger, order_id, reservation_terms(capacity_minor=20_000))
                results.append("reserved")
            except ReservationCapacityExceeded:
                results.append("blocked")
            finally:
                ledger.close()

        threads = [threading.Thread(target=compete, args=(f"order-{index}",)) for index in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertCountEqual(results, ["reserved", "blocked"])

    def test_rejection_releases_atomically_and_direct_tampering_fails(self):
        ledger = SQLiteLedger(self.path)
        try:
            valid_release = json.dumps({
                "reserved_cash_minor": 20_000,
                "reserved_exposure_minor": 20_000,
                "reserved_sell_quantity": 0,
            })
            with self.assertRaises(sqlite3.IntegrityError):
                ledger.connection.execute(
                    """INSERT INTO ledger_events
                       (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                       VALUES ('before-reserve','RISK_RELEASED','missing',?,?,?)""",
                    (NOW.isoformat(), NOW.isoformat(), valid_release),
                )
            self._reserve(ledger, "first")
            with self.assertRaises(sqlite3.IntegrityError):
                ledger.connection.execute(
                    "UPDATE risk_reservations SET reserved_cash_minor=0 WHERE client_order_id='first'"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                ledger.connection.execute(
                    """INSERT INTO ledger_events
                       (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                       VALUES ('negative','RISK_RELEASED','first',?,?,?)""",
                    (NOW.isoformat(), NOW.isoformat(), json.dumps({
                        "reserved_cash_minor": -1,
                        "reserved_exposure_minor": 0,
                        "reserved_sell_quantity": 0,
                    })),
                )
            with self.assertRaises(sqlite3.IntegrityError):
                ledger.connection.execute(
                    """INSERT INTO ledger_events
                       (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                       VALUES ('before-terminal','RISK_RELEASED','first',?,?,?)""",
                    (NOW.isoformat(), NOW.isoformat(), valid_release),
                )
            ledger.complete_submission(LedgerEvent(
                "first-rejected", "SUBMISSION_REJECTED", "first", NOW,
                {"broker_order_id": None, "detail_code": "REJECTED"},
            ))
            self.assertEqual(
                [event.event_type for event in ledger.events_for("first")][-2:],
                ["SUBMISSION_REJECTED", "RISK_RELEASED"],
            )
            self.assertTrue(self._reserve(ledger, "second"))
            with self.assertRaises(sqlite3.IntegrityError):
                ledger.connection.execute(
                    """INSERT INTO ledger_events
                       (event_id,event_type,aggregate_id,occurred_at,recorded_at,payload_json)
                       VALUES ('duplicate-release','RISK_RELEASED','first',?,?,?)""",
                    (NOW.isoformat(), NOW.isoformat(), valid_release),
                )
        finally:
            ledger.close()


if __name__ == "__main__":
    unittest.main()
