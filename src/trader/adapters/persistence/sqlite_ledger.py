from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path

from trader.domain.models import (
    OperatorAction,
    OperatorCommand,
    OperatorCommandOutcome,
    SafetyState,
    UnknownResolutionEvidence,
    UnknownResolutionResult,
    require_id,
    require_utc,
)
from trader.ports.ledger import (
    LedgerEvent,
    LedgerPersistenceError,
    OperatorCommandConflict,
    OrderReservationConflict,
    PermitAlreadyConsumed,
)

SCHEMA_VERSION = 7
TERMINAL_EVENTS = {
    OperatorCommandOutcome.SUCCEEDED: "OPERATOR_COMMAND_SUCCEEDED",
    OperatorCommandOutcome.FAILED: "OPERATOR_COMMAND_FAILED",
}


class SchemaError(RuntimeError):
    pass


class BackupError(RuntimeError):
    pass


LEGACY_SCHEMA = """
CREATE TABLE IF NOT EXISTS ledger_events (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
 event_type TEXT NOT NULL, aggregate_id TEXT NOT NULL, occurred_at TEXT NOT NULL,
 recorded_at TEXT NOT NULL, payload_json TEXT NOT NULL CHECK(json_valid(payload_json))
);
CREATE TABLE IF NOT EXISTS order_requests (
 client_order_id TEXT PRIMARY KEY,
 canonical_json TEXT NOT NULL CHECK(json_valid(canonical_json)), reserved_at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS ledger_events_no_update BEFORE UPDATE ON ledger_events BEGIN
 SELECT RAISE(ABORT, 'ledger events are append-only'); END;
CREATE TRIGGER IF NOT EXISTS ledger_events_no_delete BEFORE DELETE ON ledger_events BEGIN
 SELECT RAISE(ABORT, 'ledger events are append-only'); END;
CREATE TRIGGER IF NOT EXISTS order_requests_no_update BEFORE UPDATE ON order_requests BEGIN
 SELECT RAISE(ABORT, 'order requests are immutable'); END;
CREATE TRIGGER IF NOT EXISTS order_requests_no_delete BEFORE DELETE ON order_requests BEGIN
 SELECT RAISE(ABORT, 'order requests are immutable'); END;
"""
CURRENT_ADDITIONS = """
CREATE TABLE operator_commands (
 command_id TEXT PRIMARY KEY,
 canonical_json TEXT NOT NULL CHECK(json_valid(canonical_json)), reserved_at TEXT NOT NULL
);
CREATE TRIGGER operator_commands_no_update BEFORE UPDATE ON operator_commands BEGIN
 SELECT RAISE(ABORT, 'operator commands are immutable'); END;
CREATE TRIGGER operator_commands_no_delete BEFORE DELETE ON operator_commands BEGIN
 SELECT RAISE(ABORT, 'operator commands are immutable'); END;
CREATE UNIQUE INDEX operator_command_terminal_once ON ledger_events(aggregate_id)
 WHERE event_type IN ('OPERATOR_COMMAND_SUCCEEDED', 'OPERATOR_COMMAND_FAILED');
"""
LEGACY_OBJECTS = {
    "ledger_events", "order_requests", "ledger_events_no_update", "ledger_events_no_delete",
    "order_requests_no_update", "order_requests_no_delete",
}
V1_OBJECTS = LEGACY_OBJECTS | {
    "operator_commands", "operator_commands_no_update", "operator_commands_no_delete",
    "operator_command_terminal_once",
}
V2_OBJECTS = V1_OBJECTS | {"operator_terminal_requires_chain"}
V3_OBJECTS = V1_OBJECTS | {
    "operator_terminal_contract",
    "unknown_resolution_contract",
}
V6_OBJECTS = V3_OBJECTS | {
    "schema_metadata",
    "schema_metadata_no_update",
    "schema_metadata_no_delete",
}
CURRENT_OBJECTS = V6_OBJECTS | {
    "order_request_permit_once",
    "submission_state_contract",
}
TABLE_COLUMNS = {
    "ledger_events": (
        "sequence", "event_id", "event_type", "aggregate_id", "occurred_at", "recorded_at",
        "payload_json",
    ),
    "order_requests": ("client_order_id", "canonical_json", "reserved_at"),
    "operator_commands": ("command_id", "canonical_json", "reserved_at"),
    "schema_metadata": ("key", "value"),
}
TABLE_INFO = {
    "ledger_events": (
        ("sequence", "INTEGER", 0, None, 1),
        ("event_id", "TEXT", 1, None, 0),
        ("event_type", "TEXT", 1, None, 0),
        ("aggregate_id", "TEXT", 1, None, 0),
        ("occurred_at", "TEXT", 1, None, 0),
        ("recorded_at", "TEXT", 1, None, 0),
        ("payload_json", "TEXT", 1, None, 0),
    ),
    "order_requests": (
        ("client_order_id", "TEXT", 0, None, 1),
        ("canonical_json", "TEXT", 1, None, 0),
        ("reserved_at", "TEXT", 1, None, 0),
    ),
    "operator_commands": (
        ("command_id", "TEXT", 0, None, 1),
        ("canonical_json", "TEXT", 1, None, 0),
        ("reserved_at", "TEXT", 1, None, 0),
    ),
    "schema_metadata": (
        ("key", "TEXT", 0, None, 1),
        ("value", "TEXT", 1, None, 0),
    ),
}
LEGACY_STATEMENTS = (
    """CREATE TABLE ledger_events (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        event_type TEXT NOT NULL, aggregate_id TEXT NOT NULL, occurred_at TEXT NOT NULL,
        recorded_at TEXT NOT NULL,
        payload_json TEXT NOT NULL CHECK(json_valid(payload_json)))""",
    """CREATE TABLE order_requests (
        client_order_id TEXT PRIMARY KEY,
        canonical_json TEXT NOT NULL CHECK(json_valid(canonical_json)),
        reserved_at TEXT NOT NULL)""",
    """CREATE TRIGGER ledger_events_no_update BEFORE UPDATE ON ledger_events BEGIN
        SELECT RAISE(ABORT, 'ledger events are append-only'); END""",
    """CREATE TRIGGER ledger_events_no_delete BEFORE DELETE ON ledger_events BEGIN
        SELECT RAISE(ABORT, 'ledger events are append-only'); END""",
    """CREATE TRIGGER order_requests_no_update BEFORE UPDATE ON order_requests BEGIN
        SELECT RAISE(ABORT, 'order requests are immutable'); END""",
    """CREATE TRIGGER order_requests_no_delete BEFORE DELETE ON order_requests BEGIN
        SELECT RAISE(ABORT, 'order requests are immutable'); END""",
)
CURRENT_STATEMENTS = (
    """CREATE TABLE operator_commands (
        command_id TEXT PRIMARY KEY,
        canonical_json TEXT NOT NULL CHECK(json_valid(canonical_json)),
        reserved_at TEXT NOT NULL)""",
    """CREATE TRIGGER operator_commands_no_update BEFORE UPDATE ON operator_commands BEGIN
        SELECT RAISE(ABORT, 'operator commands are immutable'); END""",
    """CREATE TRIGGER operator_commands_no_delete BEFORE DELETE ON operator_commands BEGIN
        SELECT RAISE(ABORT, 'operator commands are immutable'); END""",
    """CREATE UNIQUE INDEX operator_command_terminal_once ON ledger_events(aggregate_id)
        WHERE event_type IN ('OPERATOR_COMMAND_SUCCEEDED', 'OPERATOR_COMMAND_FAILED')""",
)
V2_STATEMENTS = (
    """CREATE TRIGGER operator_terminal_requires_chain
        BEFORE INSERT ON ledger_events
        WHEN NEW.event_type IN ('OPERATOR_COMMAND_SUCCEEDED', 'OPERATOR_COMMAND_FAILED')
        BEGIN
          SELECT CASE WHEN NOT EXISTS (
            SELECT 1 FROM operator_commands WHERE command_id = NEW.aggregate_id
          ) THEN RAISE(ABORT, 'operator terminal requires command') END;
          SELECT CASE WHEN NOT EXISTS (
            SELECT 1 FROM ledger_events
            WHERE aggregate_id = NEW.aggregate_id
              AND event_type = 'OPERATOR_COMMAND_REQUESTED'
          ) THEN RAISE(ABORT, 'operator terminal requires requested event') END;
        END""",
)
V3_STATEMENTS = (
    "DROP TRIGGER operator_terminal_requires_chain",
    """CREATE TRIGGER operator_terminal_contract AFTER INSERT ON ledger_events
        WHEN NEW.event_type IN ('OPERATOR_COMMAND_SUCCEEDED', 'OPERATOR_COMMAND_FAILED')
        BEGIN
          SELECT CASE WHEN trim(NEW.event_id) = ''
            OR substr(NEW.occurred_at, -6) != '+00:00'
            OR julianday(NEW.occurred_at) IS NULL
            OR (SELECT COUNT(*) FROM json_each(NEW.payload_json)) != 2
            OR EXISTS (SELECT 1 FROM json_each(NEW.payload_json)
              WHERE key NOT IN ('result_state', 'error'))
            OR json_type(NEW.payload_json, '$.result_state') != 'text'
            OR json_extract(NEW.payload_json, '$.result_state') NOT IN
              ('BOOTSTRAPPING','RECONCILING','READY','TRADING','HALTED')
            OR (NEW.event_type = 'OPERATOR_COMMAND_SUCCEEDED'
              AND json_type(NEW.payload_json, '$.error') != 'null')
            OR (NEW.event_type = 'OPERATOR_COMMAND_FAILED' AND (
              json_type(NEW.payload_json, '$.error') != 'text'
              OR trim(json_extract(NEW.payload_json, '$.error')) = ''))
            OR NOT EXISTS (SELECT 1 FROM operator_commands
              WHERE command_id = NEW.aggregate_id)
            OR NOT EXISTS (SELECT 1 FROM ledger_events
              WHERE aggregate_id = NEW.aggregate_id
                AND event_type = 'OPERATOR_COMMAND_REQUESTED'
                AND sequence < NEW.sequence)
          THEN RAISE(ABORT, 'invalid operator terminal audit contract') END;
        END""",
    """CREATE TRIGGER unknown_resolution_contract AFTER INSERT ON ledger_events
        WHEN NEW.event_type = 'SUBMITTED_UNKNOWN_RESOLVED'
        BEGIN
          SELECT CASE WHEN trim(NEW.event_id) = ''
            OR substr(NEW.occurred_at, -6) != '+00:00'
            OR julianday(NEW.occurred_at) IS NULL
            OR (SELECT COUNT(*) FROM json_each(NEW.payload_json)) != 5
            OR EXISTS (SELECT 1 FROM json_each(NEW.payload_json) WHERE key NOT IN
              ('operator_command_id','result','observation','reference','observed_at'))
            OR json_type(NEW.payload_json, '$.operator_command_id') != 'text'
            OR trim(json_extract(NEW.payload_json, '$.operator_command_id')) = ''
            OR json_extract(NEW.payload_json, '$.result') NOT IN
              ('BROKER_ORDER_LINKED','CONFIRMED_ABSENT','MANUAL_ACTIVITY_LINKED')
            OR json_type(NEW.payload_json, '$.observation') != 'text'
            OR trim(json_extract(NEW.payload_json, '$.observation')) = ''
            OR json_type(NEW.payload_json, '$.reference') != 'text'
            OR trim(json_extract(NEW.payload_json, '$.reference')) = ''
            OR json_type(NEW.payload_json, '$.observed_at') != 'text'
            OR substr(json_extract(NEW.payload_json, '$.observed_at'), -6) != '+00:00'
            OR julianday(json_extract(NEW.payload_json, '$.observed_at')) IS NULL
            OR NOT EXISTS (SELECT 1 FROM order_requests AS reserved
              JOIN operator_commands AS command
                ON command.command_id = json_extract(
                  NEW.payload_json, '$.operator_command_id')
              WHERE reserved.client_order_id = NEW.aggregate_id
                AND json_extract(command.canonical_json, '$.action') =
                  'RESOLVE_SUBMITTED_UNKNOWN'
                AND trim(json_extract(command.canonical_json, '$.account_id')) != ''
                AND json_extract(command.canonical_json, '$.account_id') =
                  json_extract(reserved.canonical_json, '$.request.account_id'))
            OR NOT EXISTS (SELECT 1 FROM ledger_events AS requested
              WHERE requested.aggregate_id = json_extract(
                  NEW.payload_json, '$.operator_command_id')
                AND requested.event_type = 'OPERATOR_COMMAND_REQUESTED'
                AND requested.sequence < NEW.sequence)
            OR EXISTS (SELECT 1 FROM ledger_events AS terminal
              WHERE terminal.aggregate_id = json_extract(
                  NEW.payload_json, '$.operator_command_id')
                AND terminal.event_type IN
                  ('OPERATOR_COMMAND_SUCCEEDED','OPERATOR_COMMAND_FAILED')
                AND terminal.sequence < NEW.sequence)
            OR NOT EXISTS (SELECT 1 FROM ledger_events AS unknown_event
              WHERE unknown_event.aggregate_id = NEW.aggregate_id
                AND unknown_event.event_type = 'SUBMITTED_UNKNOWN'
                AND unknown_event.sequence < NEW.sequence
                AND unknown_event.sequence > COALESCE((SELECT MAX(prior.sequence)
                  FROM ledger_events AS prior
                  WHERE prior.aggregate_id = NEW.aggregate_id
                    AND prior.event_type = 'SUBMITTED_UNKNOWN_RESOLVED'
                    AND prior.sequence < NEW.sequence), 0))
          THEN RAISE(ABORT, 'invalid unknown resolution audit contract') END;
        END""",
)

V4_TERMINAL_SQL = (
    V3_STATEMENTS[1]
    .replace(
        "(SELECT COUNT(*) FROM json_each(NEW.payload_json)) != 2",
        "(SELECT COUNT(*) FROM json_each(NEW.payload_json)) != 2\n"
        "            OR (SELECT COUNT(DISTINCT key) FROM json_each(NEW.payload_json)) != 2",
    )
    .replace(
        "json_type(NEW.payload_json, '$.result_state') != 'text'",
        "json_type(NEW.payload_json, '$.result_state') IS NOT 'text'",
    )
    .replace(
        "json_type(NEW.payload_json, '$.error') != 'null'",
        "json_type(NEW.payload_json, '$.error') IS NOT 'null'",
    )
    .replace(
        "json_type(NEW.payload_json, '$.error') != 'text'",
        "json_type(NEW.payload_json, '$.error') IS NOT 'text'",
    )
)
V4_RESOLUTION_SQL = (
    V3_STATEMENTS[2]
    .replace(
        "(SELECT COUNT(*) FROM json_each(NEW.payload_json)) != 5",
        "(SELECT COUNT(*) FROM json_each(NEW.payload_json)) != 5\n"
        "            OR (SELECT COUNT(DISTINCT key) FROM json_each(NEW.payload_json)) != 5",
    )
    .replace(
        "json_type(NEW.payload_json, '$.operator_command_id') != 'text'",
        "json_type(NEW.payload_json, '$.operator_command_id') IS NOT 'text'",
    )
    .replace(
        "OR json_extract(NEW.payload_json, '$.result') NOT IN",
        "OR json_type(NEW.payload_json, '$.result') IS NOT 'text'\n"
        "            OR json_extract(NEW.payload_json, '$.result') NOT IN",
    )
    .replace(
        "json_type(NEW.payload_json, '$.observation') != 'text'",
        "json_type(NEW.payload_json, '$.observation') IS NOT 'text'",
    )
    .replace(
        "json_type(NEW.payload_json, '$.reference') != 'text'",
        "json_type(NEW.payload_json, '$.reference') IS NOT 'text'",
    )
    .replace(
        "json_type(NEW.payload_json, '$.observed_at') != 'text'",
        "json_type(NEW.payload_json, '$.observed_at') IS NOT 'text'",
    )
)
V4_STATEMENTS = (
    "DROP TRIGGER operator_terminal_contract",
    "DROP TRIGGER unknown_resolution_contract",
    V4_TERMINAL_SQL,
    V4_RESOLUTION_SQL,
)
V5_TERMINAL_SQL = (
    V4_TERMINAL_SQL
    .replace(
        "(SELECT COUNT(*) FROM json_each(NEW.payload_json)) != 2",
        "(SELECT COUNT(*) FROM json_each(NEW.payload_json)) != 4",
    )
    .replace(
        "(SELECT COUNT(DISTINCT key) FROM json_each(NEW.payload_json)) != 2",
        "(SELECT COUNT(DISTINCT key) FROM json_each(NEW.payload_json)) != 4",
    )
    .replace(
        "WHERE key NOT IN ('result_state', 'error')",
        "WHERE key NOT IN ('result_state', 'error', 'related_permit_id', 'related_order_id')",
    )
    .replace(
        "OR json_type(NEW.payload_json, '$.result_state') IS NOT 'text'",
        "OR json_type(NEW.payload_json, '$.result_state') IS NOT 'text'\n"
        "            OR (json_type(NEW.payload_json, '$.related_permit_id') IS NOT 'text'\n"
        "              AND json_type(NEW.payload_json, '$.related_permit_id') IS NOT 'null')\n"
        "            OR (json_type(NEW.payload_json, '$.related_permit_id') = 'text'\n"
        "              AND trim(json_extract(NEW.payload_json, '$.related_permit_id')) = '')\n"
        "            OR (json_type(NEW.payload_json, '$.related_order_id') IS NOT 'text'\n"
        "              AND json_type(NEW.payload_json, '$.related_order_id') IS NOT 'null')\n"
        "            OR (json_type(NEW.payload_json, '$.related_order_id') = 'text'\n"
        "              AND trim(json_extract(NEW.payload_json, '$.related_order_id')) = '')",
    )
)
V5_STATEMENTS = (
    "DROP TRIGGER operator_terminal_contract",
    """CREATE TABLE schema_metadata (
        key TEXT PRIMARY KEY, value TEXT NOT NULL)""",
    """CREATE TRIGGER schema_metadata_no_update BEFORE UPDATE ON schema_metadata BEGIN
        SELECT RAISE(ABORT, 'schema metadata is immutable'); END""",
    """CREATE TRIGGER schema_metadata_no_delete BEFORE DELETE ON schema_metadata BEGIN
        SELECT RAISE(ABORT, 'schema metadata is immutable'); END""",
    V5_TERMINAL_SQL,
)
V6_RESOLUTION_SQL = V4_RESOLUTION_SQL.replace(
    "OR julianday(json_extract(NEW.payload_json, '$.observed_at')) IS NULL",
    "OR julianday(json_extract(NEW.payload_json, '$.observed_at')) IS NULL\n"
    "            OR julianday(json_extract(NEW.payload_json, '$.observed_at')) >\n"
    "              julianday(NEW.occurred_at)",
)
V6_STATEMENTS = (
    "DROP TRIGGER unknown_resolution_contract",
    V6_RESOLUTION_SQL,
)
V7_RESOLUTION_SQL = V6_RESOLUTION_SQL.replace(
    "AND json_extract(command.canonical_json, '$.action') =\n"
    "                  'RESOLVE_SUBMITTED_UNKNOWN'",
    "AND json_extract(command.canonical_json, '$.action') =\n"
    "                  'RESOLVE_SUBMITTED_UNKNOWN'\n"
    "                AND json_type(command.canonical_json, '$.client_order_id') = 'text'\n"
    "                AND json_extract(command.canonical_json, '$.client_order_id') =\n"
    "                  NEW.aggregate_id",
)
V7_TERMINAL_SQL = V5_TERMINAL_SQL.replace(
    "OR NOT EXISTS (SELECT 1 FROM operator_commands\n"
    "              WHERE command_id = NEW.aggregate_id)",
    "OR NOT EXISTS (SELECT 1 FROM operator_commands\n"
    "              WHERE command_id = NEW.aggregate_id)\n"
    "            OR EXISTS (SELECT 1 FROM operator_commands AS command\n"
    "              WHERE command.command_id = NEW.aggregate_id\n"
    "                AND json_extract(command.canonical_json, '$.action') =\n"
    "                  'RESOLVE_SUBMITTED_UNKNOWN'\n"
    "                AND (\n"
    "                  json_type(command.canonical_json, '$.client_order_id') IS NOT 'text'\n"
    "                  OR json_type(NEW.payload_json, '$.related_order_id') IS NOT 'text'\n"
    "                  OR json_extract(command.canonical_json, '$.client_order_id') IS NOT\n"
    "                    json_extract(NEW.payload_json, '$.related_order_id')\n"
    "                  OR (NEW.event_type = 'OPERATOR_COMMAND_SUCCEEDED'\n"
    "                    AND NOT EXISTS (SELECT 1 FROM ledger_events AS resolution\n"
    "                      WHERE resolution.aggregate_id =\n"
    "                        json_extract(command.canonical_json, '$.client_order_id')\n"
    "                        AND resolution.event_type = 'SUBMITTED_UNKNOWN_RESOLVED'\n"
    "                        AND json_extract(resolution.payload_json,\n"
    "                          '$.operator_command_id') = NEW.aggregate_id\n"
    "                        AND resolution.sequence < NEW.sequence))))",
)
V7_STATEMENTS = (
    """CREATE UNIQUE INDEX order_request_permit_once
        ON order_requests(json_extract(canonical_json, '$.permit.permit_id'))
        WHERE json_type(canonical_json, '$.permit.permit_id') = 'text'""",
    """CREATE TRIGGER submission_state_contract BEFORE INSERT ON ledger_events
        WHEN NEW.event_type IN ('PREPARED','SUBMISSION_STARTED','ACKNOWLEDGED',
          'SUBMISSION_REJECTED','SUBMITTED_UNKNOWN')
        BEGIN
          SELECT CASE
            WHEN NEW.event_type = 'PREPARED' AND (
              NOT EXISTS (SELECT 1 FROM order_requests
                WHERE client_order_id = NEW.aggregate_id)
              OR (SELECT event_type FROM ledger_events
                WHERE aggregate_id = NEW.aggregate_id AND event_type IN
                  ('PREPARED','SUBMISSION_STARTED','ACKNOWLEDGED',
                   'SUBMISSION_REJECTED','SUBMITTED_UNKNOWN')
                ORDER BY sequence DESC LIMIT 1) IS NOT NULL)
            THEN RAISE(ABORT, 'invalid PREPARED transition')
            WHEN NEW.event_type = 'SUBMISSION_STARTED' AND
              (SELECT event_type FROM ledger_events
                WHERE aggregate_id = NEW.aggregate_id AND event_type IN
                  ('PREPARED','SUBMISSION_STARTED','ACKNOWLEDGED',
                   'SUBMISSION_REJECTED','SUBMITTED_UNKNOWN')
                ORDER BY sequence DESC LIMIT 1) IS NOT 'PREPARED'
            THEN RAISE(ABORT, 'invalid SUBMISSION_STARTED transition')
            WHEN NEW.event_type IN
              ('ACKNOWLEDGED','SUBMISSION_REJECTED','SUBMITTED_UNKNOWN') AND
              (SELECT event_type FROM ledger_events
                WHERE aggregate_id = NEW.aggregate_id AND event_type IN
                  ('PREPARED','SUBMISSION_STARTED','ACKNOWLEDGED',
                   'SUBMISSION_REJECTED','SUBMITTED_UNKNOWN')
                ORDER BY sequence DESC LIMIT 1) IS NOT 'SUBMISSION_STARTED'
            THEN RAISE(ABORT, 'invalid submission terminal transition')
          END;
        END""",
    "DROP TRIGGER operator_terminal_contract",
    V7_TERMINAL_SQL,
    "DROP TRIGGER unknown_resolution_contract",
    V7_RESOLUTION_SQL,
)


class SQLiteLedger:
    def __init__(self, path: str | Path) -> None:
        self.path = None if str(path) == ":memory:" else Path(path)
        self.connection = sqlite3.connect(path, isolation_level=None)
        try:
            if self.path is not None:
                try:
                    link_count = self.path.stat().st_nlink
                except OSError as error:
                    raise SchemaError("SQLite database identity could not be verified") from error
                if link_count != 1:
                    raise SchemaError("hard-linked SQLite databases are unsupported")
            self.connection.execute("PRAGMA foreign_keys = ON")
            self._initialize()
            journal_mode = str(
                self.connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            ).lower()
            self.connection.execute("PRAGMA synchronous = FULL")
            if self.connection.execute("PRAGMA foreign_keys").fetchone() != (1,):
                raise SchemaError("SQLite foreign_keys could not be enabled")
            if self.connection.execute("PRAGMA synchronous").fetchone() != (2,):
                raise SchemaError("SQLite synchronous mode is not FULL")
            if self.path is not None and journal_mode != "wal":
                raise SchemaError("file-backed SQLite journal mode is not WAL")
            if self.path is None and journal_mode not in {"memory", "wal"}:
                raise SchemaError("in-memory SQLite journal mode is unsupported")
        except BaseException:
            self.connection.close()
            raise

    @property
    def schema_version(self) -> int:
        return int(self.connection.execute("PRAGMA user_version").fetchone()[0])

    @property
    def runtime_identity(self) -> str:
        if self.path is None:
            return ":memory:"
        return os.path.normcase(str(self.path.resolve()))

    @staticmethod
    def _objects(connection: sqlite3.Connection) -> set[str]:
        return {
            row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            )
        }

    @classmethod
    def _validate_schema(cls, connection: sqlite3.Connection, version: int) -> None:
        expected_objects = {
            0: LEGACY_OBJECTS,
            1: V1_OBJECTS,
            2: V2_OBJECTS,
            3: V3_OBJECTS,
            4: V3_OBJECTS,
            5: V6_OBJECTS,
            6: V6_OBJECTS,
            SCHEMA_VERSION: CURRENT_OBJECTS,
        }[version]
        if cls._objects(connection) != expected_objects:
            raise SchemaError("database objects do not match the known schema")
        if version == 0:
            tables = ("ledger_events", "order_requests")
        elif version < 5:
            tables = ("ledger_events", "order_requests", "operator_commands")
        else:
            tables = tuple(TABLE_COLUMNS)
        for table in tables:
            info = tuple(
                (row[1], row[2], row[3], row[4], row[5])
                for row in connection.execute(f"PRAGMA table_info({table})")
            )
            if info != TABLE_INFO[table]:
                raise SchemaError(f"table {table} does not match the known schema")
        expected_sql = {
            "ledger_events": LEGACY_STATEMENTS[0],
            "order_requests": LEGACY_STATEMENTS[1],
            "ledger_events_no_update": LEGACY_STATEMENTS[2],
            "ledger_events_no_delete": LEGACY_STATEMENTS[3],
            "order_requests_no_update": LEGACY_STATEMENTS[4],
            "order_requests_no_delete": LEGACY_STATEMENTS[5],
        }
        if version >= 1:
            expected_sql.update(
                {
                    "operator_commands": CURRENT_STATEMENTS[0],
                    "operator_commands_no_update": CURRENT_STATEMENTS[1],
                    "operator_commands_no_delete": CURRENT_STATEMENTS[2],
                    "operator_command_terminal_once": CURRENT_STATEMENTS[3],
                }
            )
        if version == 2:
            expected_sql["operator_terminal_requires_chain"] = V2_STATEMENTS[0]
        if version == 3:
            expected_sql.update(
                {
                    "operator_terminal_contract": V3_STATEMENTS[1],
                    "unknown_resolution_contract": V3_STATEMENTS[2],
                }
            )
        if version == 4:
            expected_sql.update(
                {
                    "operator_terminal_contract": V4_STATEMENTS[2],
                    "unknown_resolution_contract": V4_STATEMENTS[3],
                }
            )
        if version >= 5:
            expected_sql.update(
                {
                    "operator_terminal_contract": V5_STATEMENTS[4],
                    "unknown_resolution_contract": (
                        V7_STATEMENTS[5]
                        if version >= 7
                        else V6_STATEMENTS[1]
                        if version >= 6
                        else V4_STATEMENTS[3]
                    ),
                    "schema_metadata": V5_STATEMENTS[1],
                    "schema_metadata_no_update": V5_STATEMENTS[2],
                    "schema_metadata_no_delete": V5_STATEMENTS[3],
                }
            )
        if version >= 7:
            expected_sql.update(
                {
                    "order_request_permit_once": V7_STATEMENTS[0],
                    "submission_state_contract": V7_STATEMENTS[1],
                    "operator_terminal_contract": V7_STATEMENTS[3],
                }
            )
        actual_sql = {
            row[0]: cls._normalize_sql(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            )
        }
        for name, sql in expected_sql.items():
            if actual_sql.get(name) != cls._normalize_sql(sql):
                raise SchemaError(f"database object {name} does not match the known schema")

    @staticmethod
    def _normalize_sql(sql: str) -> str:
        normalized = " ".join(sql.lower().replace(" if not exists", "").split())
        return normalized.replace("( ", "(").replace(" )", ")")

    @staticmethod
    def _strict_json(value: str) -> object:
        def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, item in pairs:
                if key in result:
                    raise ValueError(f"duplicate JSON key: {key}")
                result[key] = item
            return result

        return json.loads(value, object_pairs_hook=reject_duplicates)

    @staticmethod
    def _validate_audit_semantics(connection: sqlite3.Connection, *, version: int) -> None:
        try:
            if version >= 5:
                metadata = connection.execute(
                    "SELECT key, value FROM schema_metadata"
                ).fetchall()
                expected_metadata = {"terminal_payload_v5_cutoff"}
                if version >= 7:
                    expected_metadata.add("operator_binding_v7_cutoff")
                metadata_map = dict(metadata)
                if len(metadata) != len(expected_metadata) or set(metadata_map) != expected_metadata:
                    raise SchemaError("schema metadata contract is malformed")
                terminal_v5_cutoff = int(metadata_map["terminal_payload_v5_cutoff"])
                operator_v7_cutoff = int(metadata_map.get("operator_binding_v7_cutoff", "0"))
                if terminal_v5_cutoff < 0:
                    raise SchemaError("terminal payload cutoff cannot be negative")
                highest_sequence = int(connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) FROM ledger_events"
                ).fetchone()[0])
                if terminal_v5_cutoff > highest_sequence:
                    raise SchemaError("terminal payload cutoff exceeds ledger history")
                if operator_v7_cutoff < 0 or operator_v7_cutoff > highest_sequence:
                    raise SchemaError("operator binding cutoff is outside ledger history")
            else:
                terminal_v5_cutoff = int(connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) FROM ledger_events"
                ).fetchone()[0])
                operator_v7_cutoff = 0
            command_rows = connection.execute(
                "SELECT command_id, canonical_json FROM operator_commands"
            ).fetchall()
            commands: dict[str, tuple[OperatorCommand, dict[str, object]]] = {}
            for command_id, canonical_json in command_rows:
                canonical = SQLiteLedger._strict_json(canonical_json)
                legacy_command_keys = {
                    "command_id", "actor", "reason", "deployment_version",
                    "expected_safety_epoch", "requested_at", "expires_at", "action",
                    "account_id",
                }
                current_command_keys = legacy_command_keys | {
                    "client_order_id", "risk_decision_id", "execution_plan_id",
                }
                legacy_command = frozenset(canonical) == frozenset(legacy_command_keys)
                if (
                    frozenset(canonical) not in {
                        frozenset(legacy_command_keys), frozenset(current_command_keys),
                    }
                    or canonical["command_id"] != command_id
                ):
                    raise SchemaError("operator command canonical payload is malformed")
                parsed = OperatorCommand(
                    command_id=canonical["command_id"],
                    actor=canonical["actor"],
                    reason=canonical["reason"],
                    deployment_version=canonical["deployment_version"],
                    expected_safety_epoch=canonical["expected_safety_epoch"],
                    requested_at=datetime.fromisoformat(canonical["requested_at"]),
                    expires_at=datetime.fromisoformat(canonical["expires_at"]),
                    action=OperatorAction(canonical["action"]),
                    account_id=canonical["account_id"] or "LEGACY_GLOBAL",
                    client_order_id=(
                        "LEGACY_UNBOUND"
                        if legacy_command
                        and canonical["action"] == "RESOLVE_SUBMITTED_UNKNOWN"
                        else canonical.get("client_order_id")
                    ),
                    risk_decision_id=canonical.get("risk_decision_id"),
                    execution_plan_id=canonical.get("execution_plan_id"),
                )
                commands[command_id] = (parsed, canonical)

            rows = connection.execute(
                """SELECT sequence,event_id,event_type,aggregate_id,occurred_at,payload_json
                   FROM ledger_events ORDER BY sequence"""
            ).fetchall()
            requested: dict[str, list[int]] = {}
            terminals: dict[str, list[int]] = {}
            for sequence, event_id, event_type, aggregate_id, occurred_at, payload_json in rows:
                payload = SQLiteLedger._strict_json(payload_json)
                if event_type == "OPERATOR_COMMAND_REQUESTED":
                    require_id(event_id, "operator requested event_id")
                    require_utc(datetime.fromisoformat(occurred_at), "operator requested time")
                    command_entry = commands.get(aggregate_id)
                    if command_entry is None:
                        raise SchemaError("operator REQUESTED event has no immutable command")
                    expected = {**command_entry[1], "previous_state": payload.get("previous_state")}
                    if (
                        payload != expected
                        or payload.get("previous_state") not in {
                            state.value for state in SafetyState
                        }
                    ):
                        raise SchemaError("operator REQUESTED payload is malformed")
                    requested.setdefault(aggregate_id, []).append(sequence)
                elif event_type in TERMINAL_EVENTS.values():
                    terminals.setdefault(aggregate_id, []).append(sequence)

            if any(len(requested.get(command_id, ())) != 1 for command_id in commands):
                raise SchemaError("operator command must have exactly one REQUESTED event")
            if version >= 7:
                for command_id, (_, canonical) in commands.items():
                    is_legacy = "client_order_id" not in canonical
                    requested_sequence = requested[command_id][0]
                    if is_legacy != (requested_sequence <= operator_v7_cutoff):
                        raise SchemaError("operator command binding payload crosses v7 cutoff")
                    if not is_legacy and canonical["account_id"] is None:
                        raise SchemaError("current operator command requires an account alias")

            order_rows = {
                row[0]: SQLiteLedger._strict_json(row[1])
                for row in connection.execute(
                    "SELECT client_order_id, canonical_json FROM order_requests"
                )
            }
            duplicate_permit = connection.execute(
                """SELECT json_extract(canonical_json, '$.permit.permit_id')
                   FROM order_requests
                   WHERE json_type(canonical_json, '$.permit.permit_id') = 'text'
                   GROUP BY json_extract(canonical_json, '$.permit.permit_id')
                   HAVING COUNT(*) > 1 LIMIT 1"""
            ).fetchone()
            if duplicate_permit is not None:
                raise SchemaError("permit is consumed by multiple order requests")
            submission_states: dict[str, str] = {}
            last_unknown: dict[str, int] = {}
            last_resolution: dict[str, int] = {}
            resolutions_by_command: dict[str, str] = {}
            for sequence, event_id, event_type, aggregate_id, occurred_at, payload_json in rows:
                previous_submission_state = submission_states.get(aggregate_id)
                if event_type == "PREPARED":
                    if aggregate_id not in order_rows or previous_submission_state is not None:
                        raise SchemaError("invalid historical PREPARED transition")
                    submission_states[aggregate_id] = event_type
                elif event_type == "SUBMISSION_STARTED":
                    if previous_submission_state != "PREPARED":
                        raise SchemaError("invalid historical SUBMISSION_STARTED transition")
                    submission_states[aggregate_id] = event_type
                elif event_type in {
                    "ACKNOWLEDGED", "SUBMISSION_REJECTED", "SUBMITTED_UNKNOWN",
                }:
                    if previous_submission_state != "SUBMISSION_STARTED":
                        raise SchemaError("invalid historical submission terminal transition")
                    submission_states[aggregate_id] = event_type
                if event_type == "SUBMITTED_UNKNOWN":
                    last_unknown[aggregate_id] = sequence
                    continue
                if event_type in TERMINAL_EVENTS.values():
                    require_id(event_id, "operator terminal event_id")
                    require_utc(datetime.fromisoformat(occurred_at), "operator terminal time")
                    payload = SQLiteLedger._strict_json(payload_json)
                    outcome = (
                        OperatorCommandOutcome.SUCCEEDED
                        if event_type == TERMINAL_EVENTS[OperatorCommandOutcome.SUCCEEDED]
                        else OperatorCommandOutcome.FAILED
                    )
                    if aggregate_id not in commands or not any(
                        prior < sequence for prior in requested.get(aggregate_id, ())
                    ):
                        raise SchemaError("operator terminal audit chain is orphaned")
                    if sequence <= terminal_v5_cutoff:
                        payload_invalid = set(payload) != {"result_state", "error"}
                    else:
                        payload_invalid = set(payload) != {
                            "result_state", "error", "related_permit_id", "related_order_id",
                        }
                        if not payload_invalid:
                            permit_id = payload["related_permit_id"]
                            order_id = payload["related_order_id"]
                            for related in (permit_id, order_id):
                                if related is not None and (
                                    not isinstance(related, str) or not related.strip()
                                ):
                                    payload_invalid = True
                            action = commands[aggregate_id][0].action
                            permit_action = action in {
                                OperatorAction.ISSUE_CANCEL,
                                OperatorAction.ISSUE_REDUCE_ONLY,
                                OperatorAction.ISSUE_EMERGENCY_FLATTEN,
                            }
                            payload_invalid = payload_invalid or (
                                action is OperatorAction.RESOLVE_SUBMITTED_UNKNOWN
                                and (order_id is None or permit_id is not None)
                            ) or (
                                permit_action
                                and outcome is OperatorCommandOutcome.SUCCEEDED
                                and (permit_id is None or order_id is not None)
                            ) or (
                                (not permit_action or outcome is OperatorCommandOutcome.FAILED)
                                and action is not OperatorAction.RESOLVE_SUBMITTED_UNKNOWN
                                and (permit_id is not None or order_id is not None)
                            )
                    if payload_invalid:
                        raise SchemaError("operator terminal payload is malformed")
                    common_invalid = (
                        payload["result_state"] not in {
                            state.value for state in SafetyState
                        }
                        or (
                            outcome is OperatorCommandOutcome.SUCCEEDED
                            and payload["error"] is not None
                        )
                        or (
                            outcome is OperatorCommandOutcome.FAILED
                            and (
                                not isinstance(payload["error"], str)
                                or not payload["error"].strip()
                            )
                        )
                    )
                    if common_invalid:
                        raise SchemaError("operator terminal payload is malformed")
                    command = commands[aggregate_id][0]
                    if (
                        version >= 7
                        and sequence > operator_v7_cutoff
                        and command.action is OperatorAction.RESOLVE_SUBMITTED_UNKNOWN
                        and (
                            payload["related_order_id"] != command.client_order_id
                            or (
                                outcome is OperatorCommandOutcome.SUCCEEDED
                                and resolutions_by_command.get(aggregate_id)
                                != command.client_order_id
                            )
                        )
                    ):
                        raise SchemaError("unknown-resolution terminal contract is invalid")
                    continue
                if event_type != "SUBMITTED_UNKNOWN_RESOLVED":
                    continue
                require_id(event_id, "unknown resolution event_id")
                require_utc(datetime.fromisoformat(occurred_at), "unknown resolution time")
                payload = SQLiteLedger._strict_json(payload_json)
                if set(payload) != {
                    "operator_command_id", "result", "observation", "reference", "observed_at",
                }:
                    raise SchemaError("unknown resolution payload keys are malformed")
                evidence = UnknownResolutionEvidence(
                    result=UnknownResolutionResult(payload["result"]),
                    observation=payload["observation"],
                    reference=payload["reference"],
                    observed_at=datetime.fromisoformat(payload["observed_at"]),
                )
                resolution_time = datetime.fromisoformat(occurred_at)
                if evidence.observed_at > resolution_time:
                    raise SchemaError("unknown resolution evidence is from the future")
                if evidence.result.value != payload["result"]:
                    raise SchemaError("unknown resolution result is malformed")
                command_id = payload["operator_command_id"]
                command_entry = commands.get(command_id)
                order = order_rows.get(aggregate_id)
                try:
                    order_account = order["request"]["account_id"]
                except (KeyError, TypeError):
                    raise SchemaError("resolved order has no internal account alias") from None
                if (
                    command_entry is None
                    or command_entry[0].action is not OperatorAction.RESOLVE_SUBMITTED_UNKNOWN
                    or command_entry[0].account_id != order_account
                    or (
                        "client_order_id" in command_entry[1]
                        and command_entry[0].client_order_id != aggregate_id
                    )
                    or not any(prior < sequence for prior in requested.get(command_id, ()))
                    or any(prior < sequence for prior in terminals.get(command_id, ()))
                    or last_unknown.get(aggregate_id, 0)
                    <= last_resolution.get(aggregate_id, 0)
                ):
                    raise SchemaError("unknown resolution audit chain is invalid")
                last_resolution[aggregate_id] = sequence
                resolutions_by_command[command_id] = aggregate_id
        except SchemaError:
            raise
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise SchemaError("audit semantic validation failed") from error

    def _initialize(self) -> None:
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            version = self.schema_version
            objects = self._objects(self.connection)
            if version > SCHEMA_VERSION or version not in {0, 1, 2, 3, 4, 5, 6, 7}:
                raise SchemaError(f"unsupported schema version {version}")
            if version == SCHEMA_VERSION:
                self._validate_schema(self.connection, SCHEMA_VERSION)
                self._validate_audit_semantics(
                    self.connection, version=SCHEMA_VERSION
                )
                self.connection.execute("COMMIT")
                return
            if version == 6:
                self._validate_schema(self.connection, 6)
                self._validate_audit_semantics(self.connection, version=6)
                statements = V7_STATEMENTS
            elif version == 5:
                self._validate_schema(self.connection, 5)
                self._validate_audit_semantics(self.connection, version=5)
                statements = V6_STATEMENTS + V7_STATEMENTS
            elif version == 4:
                self._validate_schema(self.connection, 4)
                self._validate_audit_semantics(self.connection, version=4)
                statements = V5_STATEMENTS + V6_STATEMENTS + V7_STATEMENTS
            elif version == 3:
                self._validate_schema(self.connection, 3)
                self._validate_audit_semantics(self.connection, version=3)
                statements = V4_STATEMENTS + V5_STATEMENTS + V6_STATEMENTS + V7_STATEMENTS
            elif version == 2:
                self._validate_schema(self.connection, 2)
                self._validate_audit_semantics(self.connection, version=2)
                statements = (
                    V3_STATEMENTS + V4_STATEMENTS + V5_STATEMENTS + V6_STATEMENTS
                    + V7_STATEMENTS
                )
            elif version == 1:
                self._validate_schema(self.connection, 1)
                self._validate_audit_semantics(self.connection, version=1)
                statements = (
                    V2_STATEMENTS + V3_STATEMENTS + V4_STATEMENTS + V5_STATEMENTS
                    + V6_STATEMENTS + V7_STATEMENTS
                )
            elif objects:
                self._validate_schema(self.connection, 0)
                statements = (
                    CURRENT_STATEMENTS
                    + V2_STATEMENTS
                    + V3_STATEMENTS
                    + V4_STATEMENTS
                    + V5_STATEMENTS
                    + V6_STATEMENTS + V7_STATEMENTS
                )
            else:
                statements = (
                    LEGACY_STATEMENTS
                    + CURRENT_STATEMENTS
                    + V2_STATEMENTS
                    + V3_STATEMENTS
                    + V4_STATEMENTS
                    + V5_STATEMENTS
                    + V6_STATEMENTS + V7_STATEMENTS
                )
            for statement in statements:
                self.connection.execute(statement)
            if version < 5:
                cutoff = int(self.connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) FROM ledger_events"
                ).fetchone()[0])
                self.connection.execute(
                    "INSERT INTO schema_metadata VALUES ('terminal_payload_v5_cutoff', ?)",
                    (str(cutoff),),
                )
            if version < 7:
                cutoff = int(self.connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) FROM ledger_events"
                ).fetchone()[0])
                self.connection.execute(
                    "INSERT INTO schema_metadata VALUES ('operator_binding_v7_cutoff', ?)",
                    (str(cutoff),),
                )
            self._validate_schema(self.connection, SCHEMA_VERSION)
            self._validate_audit_semantics(self.connection, version=SCHEMA_VERSION)
            self.connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            self.connection.execute("COMMIT")
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    @staticmethod
    def _json(value: object, *, canonical: bool = False) -> str:
        return json.dumps(
            value, ensure_ascii=False, allow_nan=False, sort_keys=canonical, separators=(",", ":")
        )

    def _insert_event(self, event: LedgerEvent, recorded_at: str) -> None:
        self.connection.execute(
            """INSERT INTO ledger_events
               (event_id, event_type, aggregate_id, occurred_at, recorded_at, payload_json)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                event.event_id, event.event_type, event.aggregate_id, event.occurred_at.isoformat(),
                recorded_at, self._json(event.payload),
            ),
        )

    def reserve_submission(
        self,
        client_order_id: str,
        canonical_payload: Mapping[str, object],
        prepared_event: LedgerEvent,
        started_event: LedgerEvent,
        permit_id: str | None,
    ) -> bool:
        require_id(client_order_id, "client_order_id")
        require_id(prepared_event.event_id, "prepared_event.event_id")
        require_id(started_event.event_id, "started_event.event_id")
        if (
            prepared_event.aggregate_id != client_order_id
            or prepared_event.event_type != "PREPARED"
            or started_event.aggregate_id != client_order_id
            or started_event.event_type != "SUBMISSION_STARTED"
        ):
            raise ValueError("reservation requires matching PREPARED and SUBMISSION_STARTED events")
        require_utc(prepared_event.occurred_at, "occurred_at")
        require_utc(started_event.occurred_at, "occurred_at")
        permit = canonical_payload.get("permit")
        if permit is None:
            canonical_permit_id = None
        elif isinstance(permit, Mapping):
            canonical_permit_id = permit.get("permit_id")
        else:
            raise ValueError("canonical permit must be an object or null")
        if permit_id is not None:
            require_id(permit_id, "permit_id")
        if canonical_permit_id != permit_id:
            raise ValueError("permit_id does not match the immutable order payload")
        canonical = self._json(canonical_payload, canonical=True)
        recorded_at = datetime.now(timezone.utc).isoformat()
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            existing = self.connection.execute(
                "SELECT canonical_json FROM order_requests WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
            if existing is not None:
                if existing[0] != canonical:
                    raise OrderReservationConflict("client_order_id has different immutable payload")
                self.connection.execute("COMMIT")
                return False
            try:
                self.connection.execute(
                    "INSERT INTO order_requests VALUES (?, ?, ?)",
                    (client_order_id, canonical, recorded_at),
                )
            except sqlite3.IntegrityError as error:
                consumed = permit_id is not None and self.connection.execute(
                    """SELECT 1 FROM order_requests
                       WHERE json_type(canonical_json, '$.permit.permit_id') = 'text'
                         AND json_extract(canonical_json, '$.permit.permit_id') = ?""",
                    (permit_id,),
                ).fetchone()
                if consumed:
                    raise PermitAlreadyConsumed("permit has already been consumed") from error
                raise
            self._insert_event(prepared_event, recorded_at)
            self._insert_event(started_event, recorded_at)
            self.connection.execute("COMMIT")
            return True
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    def append(self, event: LedgerEvent) -> None:
        require_id(event.event_id, "event_id")
        require_id(event.event_type, "event_type")
        require_id(event.aggregate_id, "aggregate_id")
        require_utc(event.occurred_at, "occurred_at")
        if (
            event.event_type.startswith("OPERATOR_COMMAND_")
            or event.event_type == "SUBMITTED_UNKNOWN_RESOLVED"
        ):
            raise ValueError("audited events require their dedicated ledger operation")
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            self._insert_event(event, datetime.now(timezone.utc).isoformat())
            self.connection.execute("COMMIT")
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    @staticmethod
    def canonical_command(command: OperatorCommand) -> dict[str, object]:
        return {
            "command_id": command.command_id, "actor": command.actor, "reason": command.reason,
            "deployment_version": command.deployment_version,
            "expected_safety_epoch": command.expected_safety_epoch,
            "requested_at": command.requested_at.isoformat(),
            "expires_at": command.expires_at.isoformat(), "action": command.action.value,
            "account_id": command.account_id,
            "client_order_id": command.client_order_id,
            "risk_decision_id": command.risk_decision_id,
            "execution_plan_id": command.execution_plan_id,
        }

    def reserve_operator_command(self, command: OperatorCommand, event: LedgerEvent) -> None:
        require_id(event.event_id, "event_id")
        require_utc(event.occurred_at, "occurred_at")
        expected_payload = {**self.canonical_command(command), "previous_state": event.payload.get("previous_state")}
        if (
            event.event_type != "OPERATOR_COMMAND_REQUESTED"
            or event.aggregate_id != command.command_id
            or event.payload.get("previous_state") not in {state.value for state in SafetyState}
            or dict(event.payload) != expected_payload
        ):
            raise ValueError("command reservation requires matching REQUESTED event")
        recorded_at = datetime.now(timezone.utc).isoformat()
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            if self.connection.execute(
                "SELECT 1 FROM operator_commands WHERE command_id = ?", (command.command_id,)
            ).fetchone():
                raise OperatorCommandConflict("operator command ID has already been used")
            self.connection.execute(
                "INSERT INTO operator_commands VALUES (?, ?, ?)",
                (command.command_id, self._json(self.canonical_command(command), canonical=True), recorded_at),
            )
            self._insert_event(event, recorded_at)
            self.connection.execute("COMMIT")
        except OperatorCommandConflict:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise
        except sqlite3.Error as error:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise LedgerPersistenceError("operator command reservation failed") from error
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    def complete_operator_command(
        self, command_id: str, outcome: OperatorCommandOutcome, event: LedgerEvent,
    ) -> None:
        require_id(command_id, "command_id")
        require_id(event.event_id, "event_id")
        require_utc(event.occurred_at, "occurred_at")
        if not isinstance(outcome, OperatorCommandOutcome):
            raise ValueError("outcome must be OperatorCommandOutcome")
        payload = dict(event.payload)
        if (
            event.aggregate_id != command_id
            or event.event_type != TERMINAL_EVENTS[outcome]
            or set(payload) != {
                "result_state", "error", "related_permit_id", "related_order_id",
            }
            or payload["result_state"] not in {state.value for state in SafetyState}
            or (
                outcome is OperatorCommandOutcome.SUCCEEDED and payload["error"] is not None
            )
            or (
                outcome is OperatorCommandOutcome.FAILED
                and (not isinstance(payload["error"], str) or not payload["error"].strip())
            )
            or any(
                related is not None
                and (not isinstance(related, str) or not related.strip())
                for related in (
                    payload["related_permit_id"], payload["related_order_id"]
                )
            )
        ):
            raise ValueError("invalid operator command terminal event")
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            command_row = self.connection.execute(
                "SELECT canonical_json FROM operator_commands WHERE command_id = ?", (command_id,)
            ).fetchone()
            if command_row is None:
                raise ValueError("operator command was not reserved")
            command_payload = json.loads(command_row[0])
            action = OperatorAction(command_payload["action"])
            permit_action = action in {
                OperatorAction.ISSUE_CANCEL,
                OperatorAction.ISSUE_REDUCE_ONLY,
                OperatorAction.ISSUE_EMERGENCY_FLATTEN,
            }
            permit_id, order_id = payload["related_permit_id"], payload["related_order_id"]
            if (
                action is OperatorAction.RESOLVE_SUBMITTED_UNKNOWN
                and (
                    order_id != command_payload.get("client_order_id")
                    or permit_id is not None
                )
            ) or (
                permit_action
                and outcome is OperatorCommandOutcome.SUCCEEDED
                and (permit_id is None or order_id is not None)
            ) or (
                (not permit_action or outcome is OperatorCommandOutcome.FAILED)
                and action is not OperatorAction.RESOLVE_SUBMITTED_UNKNOWN
                and (permit_id is not None or order_id is not None)
            ):
                raise ValueError("operator terminal correlation is invalid")
            if (
                action is OperatorAction.RESOLVE_SUBMITTED_UNKNOWN
                and outcome is OperatorCommandOutcome.SUCCEEDED
                and not self.connection.execute(
                    """SELECT 1 FROM ledger_events
                       WHERE aggregate_id = ?
                         AND event_type = 'SUBMITTED_UNKNOWN_RESOLVED'
                         AND json_extract(payload_json, '$.operator_command_id') = ?""",
                    (order_id, command_id),
                ).fetchone()
            ):
                raise ValueError("successful unknown resolution requires persisted evidence")
            if not self.connection.execute(
                """SELECT 1 FROM ledger_events
                   WHERE aggregate_id = ? AND event_type = 'OPERATOR_COMMAND_REQUESTED'""",
                (command_id,),
            ).fetchone():
                raise ValueError("operator command REQUESTED event is missing")
            self._insert_event(event, datetime.now(timezone.utc).isoformat())
            self.connection.execute("COMMIT")
        except sqlite3.Error as error:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise LedgerPersistenceError("operator command completion failed") from error
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    def pending_operator_commands(self, account_id: str | None = None) -> tuple[str, ...]:
        if account_id is not None:
            require_id(account_id, "account_id")
        account_filter = ""
        parameters: tuple[str, ...] = ()
        if account_id is not None:
            account_filter = (
                " AND (json_type(command.canonical_json, '$.account_id') = 'null'"
                " OR (json_type(command.canonical_json, '$.account_id') = 'text'"
                " AND json_extract(command.canonical_json, '$.account_id') = ?))"
            )
            parameters = (account_id,)
        rows = self.connection.execute(
            f"""SELECT command.command_id FROM operator_commands AS command
               WHERE NOT EXISTS (SELECT 1 FROM ledger_events AS terminal
                 WHERE terminal.aggregate_id = command.command_id
                   AND terminal.event_type IN
                       ('OPERATOR_COMMAND_SUCCEEDED','OPERATOR_COMMAND_FAILED'))
               {account_filter}
               ORDER BY command.rowid""",
            parameters,
        ).fetchall()
        return tuple(row[0] for row in rows)

    def events_for(self, aggregate_id: str) -> tuple[LedgerEvent, ...]:
        rows = self.connection.execute(
            """SELECT event_id, event_type, aggregate_id, occurred_at, payload_json
               FROM ledger_events WHERE aggregate_id = ? ORDER BY sequence""",
            (aggregate_id,),
        ).fetchall()
        return tuple(
            LedgerEvent(row[0], row[1], row[2], datetime.fromisoformat(row[3]), json.loads(row[4]))
            for row in rows
        )

    def record_unknown_resolution(
        self,
        client_order_id: str,
        command: OperatorCommand,
        evidence: UnknownResolutionEvidence,
        event: LedgerEvent,
    ) -> None:
        require_id(client_order_id, "client_order_id")
        require_id(event.event_id, "event_id")
        require_utc(event.occurred_at, "occurred_at")
        require_utc(evidence.observed_at, "evidence.observed_at")
        if evidence.observed_at > event.occurred_at:
            raise ValueError("unknown-resolution evidence cannot postdate its event")
        expected_payload = {
            "operator_command_id": command.command_id,
            "result": evidence.result.value,
            "observation": evidence.observation,
            "reference": evidence.reference,
            "observed_at": evidence.observed_at.isoformat(),
        }
        if (
            event.aggregate_id != client_order_id
            or event.event_type != "SUBMITTED_UNKNOWN_RESOLVED"
            or dict(event.payload) != expected_payload
            or command.action is not OperatorAction.RESOLVE_SUBMITTED_UNKNOWN
            or command.client_order_id != client_order_id
            or command.account_id is None
        ):
            raise ValueError("invalid unknown-resolution event")
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            order_row = self.connection.execute(
                "SELECT canonical_json FROM order_requests WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
            command_row = self.connection.execute(
                "SELECT canonical_json FROM operator_commands WHERE command_id = ?",
                (command.command_id,),
            ).fetchone()
            if order_row is None or command_row is None:
                raise ValueError("resolution requires reserved order and operator command")
            try:
                order_account = json.loads(order_row[0])["request"]["account_id"]
            except (KeyError, TypeError, json.JSONDecodeError) as error:
                raise ValueError("order reservation has no internal account alias") from error
            if (
                order_account != command.account_id
                or command_row[0] != self._json(self.canonical_command(command), canonical=True)
            ):
                raise ValueError("operator command account or immutable payload does not match")
            chain = self.connection.execute(
                """SELECT
                     MAX(CASE WHEN event_type='SUBMITTED_UNKNOWN' THEN sequence END),
                     MAX(CASE WHEN event_type='SUBMITTED_UNKNOWN_RESOLVED' THEN sequence END)
                   FROM ledger_events WHERE aggregate_id = ?""",
                (client_order_id,),
            ).fetchone()
            requested = self.connection.execute(
                """SELECT 1 FROM ledger_events WHERE aggregate_id = ?
                   AND event_type = 'OPERATOR_COMMAND_REQUESTED'""",
                (command.command_id,),
            ).fetchone()
            terminal = self.connection.execute(
                """SELECT 1 FROM ledger_events WHERE aggregate_id = ? AND event_type IN
                   ('OPERATOR_COMMAND_SUCCEEDED','OPERATOR_COMMAND_FAILED')""",
                (command.command_id,),
            ).fetchone()
            if chain[0] is None or (chain[1] is not None and chain[1] >= chain[0]):
                raise ValueError("submission is not unresolved UNKNOWN")
            if requested is None or terminal is not None:
                raise ValueError("operator command audit chain is not pending")
            self._insert_event(event, datetime.now(timezone.utc).isoformat())
            self.connection.execute("COMMIT")
        except sqlite3.Error as error:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise LedgerPersistenceError("unknown resolution persistence failed") from error
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    def incomplete_submissions(self, account_id: str | None = None) -> tuple[str, ...]:
        if account_id is not None:
            require_id(account_id, "account_id")
        account_join = ""
        account_filter = ""
        parameters: tuple[str, ...] = ()
        if account_id is not None:
            account_join = (
                " JOIN order_requests AS reserved"
                " ON reserved.client_order_id = event.aggregate_id"
            )
            account_filter = (
                " AND json_type(reserved.canonical_json, '$.request.account_id') = 'text'"
                " AND json_extract(reserved.canonical_json, '$.request.account_id') = ?"
            )
            parameters = (account_id,)
        rows = self.connection.execute(
            f"""SELECT event.aggregate_id FROM ledger_events AS event{account_join}
               WHERE event.event_type = 'SUBMISSION_STARTED' AND event.sequence = (
                 SELECT MAX(latest.sequence) FROM ledger_events AS latest
                 WHERE latest.aggregate_id = event.aggregate_id AND latest.event_type IN
                   ('PREPARED','SUBMISSION_STARTED','ACKNOWLEDGED',
                    'SUBMISSION_REJECTED','SUBMITTED_UNKNOWN'))
               {account_filter}
               ORDER BY event.sequence""",
            parameters,
        ).fetchall()
        return tuple(row[0] for row in rows)

    def unresolved_unknown_submissions(
        self, account_id: str | None = None,
    ) -> tuple[str, ...]:
        if account_id is not None:
            require_id(account_id, "account_id")
        account_join = ""
        account_filter = ""
        parameters: tuple[str, ...] = ()
        if account_id is not None:
            account_join = (
                " JOIN order_requests AS reserved"
                " ON reserved.client_order_id = unknown_event.aggregate_id"
            )
            account_filter = (
                " AND json_type(reserved.canonical_json, '$.request.account_id') = 'text'"
                " AND json_extract(reserved.canonical_json, '$.request.account_id') = ?"
            )
            parameters = (account_id,)
        rows = self.connection.execute(
            f"""SELECT unknown_event.aggregate_id
               FROM ledger_events AS unknown_event{account_join}
               WHERE unknown_event.event_type = 'SUBMITTED_UNKNOWN'
               {account_filter}
               GROUP BY unknown_event.aggregate_id
               HAVING MAX(unknown_event.sequence) > COALESCE((
                 SELECT MAX(resolved.sequence) FROM ledger_events AS resolved
                 WHERE resolved.aggregate_id = unknown_event.aggregate_id
                   AND resolved.event_type = 'SUBMITTED_UNKNOWN_RESOLVED'), 0)
               ORDER BY MAX(unknown_event.sequence)""",
            parameters,
        ).fetchall()
        return tuple(row[0] for row in rows)

    def highest_sequence(self) -> int:
        return int(self.connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) FROM ledger_events"
        ).fetchone()[0])

    def integrity_check(self) -> bool:
        return self.connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)

    @classmethod
    def _verify_version(
        cls, connection: sqlite3.Connection, sequence: int, version: int,
    ) -> None:
        if connection.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            raise BackupError("database integrity check failed")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise BackupError("database foreign-key check failed")
        if int(connection.execute("PRAGMA user_version").fetchone()[0]) != version:
            raise BackupError("backup schema version does not match")
        try:
            cls._validate_schema(connection, version)
            cls._validate_audit_semantics(connection, version=version)
        except SchemaError as error:
            raise BackupError("backup schema validation failed") from error
        actual = int(connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) FROM ledger_events"
        ).fetchone()[0])
        if actual != sequence:
            raise BackupError("backup ledger sequence does not match")

    @classmethod
    def _verify_current(cls, connection: sqlite3.Connection, sequence: int) -> None:
        cls._verify_version(connection, sequence, SCHEMA_VERSION)

    @staticmethod
    def _manifest_path(path: Path) -> Path:
        return path.with_name(path.name + ".manifest.json")

    @staticmethod
    def _publish_exclusive(source: Path, destination: Path) -> tuple[int, int]:
        source_stat = source.stat()
        identity = (source_stat.st_dev, source_stat.st_ino)
        os.link(source, destination)
        try:
            source.unlink()
        except OSError:
            pass
        return identity

    @staticmethod
    def _content_digest(connection: sqlite3.Connection) -> str:
        digest = hashlib.sha256()
        for statement in connection.iterdump():
            digest.update(statement.encode("utf-8"))
            digest.update(b"\n")
        return digest.hexdigest()

    def backup(self, destination: str | Path, *, app_version: str) -> Path:
        require_id(app_version, "app_version")
        destination = Path(destination)
        manifest_path = self._manifest_path(destination)
        if destination.exists() or manifest_path.exists():
            raise FileExistsError("backup and manifest destinations must not exist")
        destination.parent.mkdir(parents=True, exist_ok=True)
        handle, temp_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
        os.close(handle)
        handle, manifest_name = tempfile.mkstemp(prefix=f".{manifest_path.name}.", dir=destination.parent)
        os.close(handle)
        temp_db, temp_manifest = Path(temp_name), Path(manifest_name)
        try:
            backup_connection = sqlite3.connect(temp_db)
            try:
                self.connection.backup(backup_connection)
                backup_connection.commit()
                sequence = int(backup_connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) FROM ledger_events"
                ).fetchone()[0])
                self._verify_current(backup_connection, sequence)
            finally:
                backup_connection.close()
            manifest = {
                "sha256": hashlib.sha256(temp_db.read_bytes()).hexdigest(),
                "schema_version": SCHEMA_VERSION, "highest_ledger_sequence": sequence,
                "created_at": datetime.now(timezone.utc).isoformat(), "app_version": app_version,
            }
            temp_manifest.write_text(
                json.dumps(manifest, sort_keys=True, separators=(",", ":")), encoding="utf-8"
            )
            self._publish_exclusive(temp_db, destination)
            self._publish_exclusive(temp_manifest, manifest_path)
            return manifest_path
        finally:
            temp_db.unlink(missing_ok=True)
            temp_manifest.unlink(missing_ok=True)

    @classmethod
    def restore(
        cls, backup_path: str | Path, destination: str | Path,
        *, manifest_path: str | Path | None = None,
    ) -> SQLiteLedger:
        backup_path, destination = Path(backup_path), Path(destination)
        manifest_path = Path(manifest_path) if manifest_path else cls._manifest_path(backup_path)
        destination_sidecars = (Path(f"{destination}-wal"), Path(f"{destination}-shm"))
        if destination.exists() or any(path.exists() for path in destination_sidecars):
            raise FileExistsError("restore destination and its SQLite sidecars must not exist")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            required = {
                "sha256", "schema_version", "highest_ledger_sequence", "created_at", "app_version",
            }
            if set(manifest) != required:
                raise BackupError("manifest fields do not match")
            backup_bytes = backup_path.read_bytes()
            if hashlib.sha256(backup_bytes).hexdigest() != manifest["sha256"]:
                raise BackupError("backup hash does not match manifest")
            declared_version = manifest["schema_version"]
            if type(declared_version) is not int or declared_version not in {6, SCHEMA_VERSION}:
                raise BackupError("manifest schema version is unsupported")
            require_utc(datetime.fromisoformat(manifest["created_at"]), "created_at")
            require_id(manifest["app_version"], "app_version")
            sequence = int(manifest["highest_ledger_sequence"])
        except BackupError:
            raise
        except Exception as error:
            raise BackupError("manifest is invalid") from error
        destination.parent.mkdir(parents=True, exist_ok=True)
        handle, temp_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
        os.close(handle)
        handle, source_name = tempfile.mkstemp(
            prefix=f".{destination.name}.verified.", dir=destination.parent
        )
        os.close(handle)
        temp_db = Path(temp_name)
        verified_source = Path(source_name)
        try:
            verified_source.write_bytes(backup_bytes)
            source = sqlite3.connect(
                f"{verified_source.resolve().as_uri()}?immutable=1", uri=True
            )
            restored = sqlite3.connect(temp_db)
            try:
                cls._verify_version(source, sequence, declared_version)
                source_digest = cls._content_digest(source)
                source.backup(restored)
                restored.commit()
                cls._verify_version(restored, sequence, declared_version)
                if cls._content_digest(restored) != source_digest:
                    raise BackupError("restored logical content does not match verified backup")
            finally:
                restored.close()
                source.close()
            try:
                staged_ledger = cls(temp_db)
            except BaseException as error:
                raise BackupError("staged backup migration failed") from error
            try:
                cls._verify_current(staged_ledger.connection, sequence)
                staged_ledger.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                verified_digest = cls._content_digest(staged_ledger.connection)
            finally:
                staged_ledger.close()
            temp_sidecars = (Path(f"{temp_db}-wal"), Path(f"{temp_db}-shm"))
            if any(path.exists() for path in temp_sidecars):
                raise BackupError("staged restore retained SQLite sidecars")
            staged = sqlite3.connect(
                f"{temp_db.resolve().as_uri()}?immutable=1", uri=True
            )
            try:
                cls._verify_current(staged, sequence)
                if cls._content_digest(staged) != verified_digest:
                    raise BackupError("staged migrated content changed before publication")
            finally:
                staged.close()
            if destination.exists() or any(path.exists() for path in destination_sidecars):
                raise FileExistsError("restore destination or SQLite sidecar appeared")
            cls._publish_exclusive(temp_db, destination)
            if any(path.exists() for path in destination_sidecars):
                raise BackupError("SQLite sidecar appeared after restore publication")
            try:
                final_ledger = cls(destination)
            except BaseException as error:
                raise BackupError("published restore failed final open validation") from error
            try:
                cls._verify_current(final_ledger.connection, sequence)
                if cls._content_digest(final_ledger.connection) != verified_digest:
                    raise BackupError("published restore content changed after publication")
            except BaseException:
                final_ledger.close()
                raise
            return final_ledger
        finally:
            temp_db.unlink(missing_ok=True)
            Path(f"{temp_db}-wal").unlink(missing_ok=True)
            Path(f"{temp_db}-shm").unlink(missing_ok=True)
            verified_source.unlink(missing_ok=True)

    def close(self) -> None:
        self.connection.close()
