from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from trader.domain.broker_observations import (
    ConfirmedAbsent,
    TYPED_UNKNOWN_RESOLUTION_TYPES,
    TypedUnknownResolutionEvidence,
    canonical_resolution_payload,
    resolution_from_payload,
)
from trader.domain.models import (
    BrokerExecutionState,
    OperatorAction,
    OperatorCommand,
    OperatorCommandOutcome,
    SafetyState,
    ReservationTerms,
    Side,
    TradingEnvironment,
    canonical_share_quantity,
    require_id,
    require_utc,
)
from trader.ports.ledger import (
    LedgerEvent,
    LedgerPersistenceError,
    OperatorCommandConflict,
    OrderReservationConflict,
    PermitAlreadyConsumed,
    ReservationCapacityExceeded,
    canonical_operator_command,
)
from trader.domain.cancellation import CancelOrderCommand
from trader.domain.broker_lifecycle import (
    BROKER_LIFECYCLE_FACT_TYPES,
    BrokerFillObserved,
    BrokerLifecycleFact,
    BrokerLifecycleProjection,
    BrokerOrderOpened,
    broker_fact_from_payload,
    canonical_broker_fact_payload,
    fold_broker_order,
)

SCHEMA_VERSION = 10
TERMINAL_EVENTS = {
    OperatorCommandOutcome.SUCCEEDED: "OPERATOR_COMMAND_SUCCEEDED",
    OperatorCommandOutcome.FAILED: "OPERATOR_COMMAND_FAILED",
}
BROKER_FACT_EVENTS = {
    "ORDER_OPENED": "BROKER_ORDER_OPENED",
    "FILL_OBSERVED": "BROKER_FILL_OBSERVED",
    "ORDER_CANCELED": "BROKER_ORDER_CANCELED",
    "ORDER_EXPIRED": "BROKER_ORDER_EXPIRED",
    "ORDER_REJECTED": "BROKER_ORDER_REJECTED",
}
BROKER_EVENT_TYPES = frozenset(BROKER_FACT_EVENTS.values())


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
V7_OBJECTS = V6_OBJECTS | {
    "order_request_permit_once",
    "submission_state_contract",
}
V8_OBJECTS = V7_OBJECTS | {
    "order_environment_contract",
    "operator_environment_contract",
}
V9_OBJECTS = V8_OBJECTS | {
    "risk_reservations", "risk_reservations_no_update", "risk_reservations_no_delete",
    "risk_reserved_once", "risk_event_contract",
}
CURRENT_OBJECTS = V9_OBJECTS
TABLE_COLUMNS = {
    "ledger_events": (
        "sequence", "event_id", "event_type", "aggregate_id", "occurred_at", "recorded_at",
        "payload_json",
    ),
    "order_requests": ("client_order_id", "canonical_json", "reserved_at"),
    "operator_commands": ("command_id", "canonical_json", "reserved_at"),
    "schema_metadata": ("key", "value"),
    "risk_reservations": (
        "client_order_id", "account_id", "account_snapshot_id", "environment",
        "policy_version", "market", "symbol", "account_currency",
        "instrument_currency", "side", "quantity", "available_cash_minor",
        "current_exposure_minor", "current_position_quantity", "cash_cap_minor",
        "exposure_cap_minor", "fee_buffer_minor", "reserved_cash_minor",
        "reserved_exposure_minor", "reserved_sell_quantity", "reserved_at",
    ),
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
    "risk_reservations": (
        ("client_order_id", "TEXT", 0, None, 1),
        ("account_id", "TEXT", 1, None, 0),
        ("account_snapshot_id", "TEXT", 1, None, 0),
        ("environment", "TEXT", 1, None, 0),
        ("policy_version", "TEXT", 1, None, 0),
        ("market", "TEXT", 1, None, 0),
        ("symbol", "TEXT", 1, None, 0),
        ("account_currency", "TEXT", 1, None, 0),
        ("instrument_currency", "TEXT", 1, None, 0),
        ("side", "TEXT", 1, None, 0),
        ("quantity", "INTEGER", 1, None, 0),
        ("available_cash_minor", "INTEGER", 1, None, 0),
        ("current_exposure_minor", "INTEGER", 1, None, 0),
        ("current_position_quantity", "INTEGER", 1, None, 0),
        ("cash_cap_minor", "INTEGER", 1, None, 0),
        ("exposure_cap_minor", "INTEGER", 1, None, 0),
        ("fee_buffer_minor", "INTEGER", 1, None, 0),
        ("reserved_cash_minor", "INTEGER", 1, None, 0),
        ("reserved_exposure_minor", "INTEGER", 1, None, 0),
        ("reserved_sell_quantity", "INTEGER", 1, None, 0),
        ("reserved_at", "TEXT", 1, None, 0),
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
V8_STATEMENTS = (
    """CREATE TRIGGER order_environment_contract BEFORE INSERT ON order_requests
        BEGIN
          SELECT CASE WHEN
            json_type(NEW.canonical_json, '$.environment') IS NOT 'text'
            OR json_extract(NEW.canonical_json, '$.environment') NOT IN
              ('SIMULATED','PAPER','LIVE')
            OR json_type(NEW.canonical_json, '$.permit') IS NOT 'object'
            OR json_type(NEW.canonical_json, '$.permit.permit_id') IS NOT 'text'
            OR trim(json_extract(NEW.canonical_json, '$.permit.permit_id')) = ''
            OR json_type(NEW.canonical_json, '$.permit.environment') IS NOT 'text'
            OR json_extract(NEW.canonical_json, '$.permit.environment') IS NOT
              json_extract(NEW.canonical_json, '$.environment')
            OR json_type(NEW.canonical_json,
              '$.risk.input_snapshot_environment') IS NOT 'text'
            OR json_extract(NEW.canonical_json,
              '$.risk.input_snapshot_environment') IS NOT
              json_extract(NEW.canonical_json, '$.environment')
            OR json_type(NEW.canonical_json,
              '$.plan.market_evidence.environment') IS NOT 'text'
            OR json_extract(NEW.canonical_json,
              '$.plan.market_evidence.environment') IS NOT
              json_extract(NEW.canonical_json, '$.environment')
            OR json_type(NEW.canonical_json,
              '$.plan.market_evidence.pricing_policy_version') IS NOT 'text'
            OR json_extract(NEW.canonical_json,
              '$.plan.market_evidence.pricing_policy_version') IS NOT
              json_extract(NEW.canonical_json, '$.plan.pricing_policy_version')
            OR json_extract(NEW.canonical_json, '$.plan.pricing_policy_version') IS NOT
              json_extract(NEW.canonical_json, '$.risk.policy_version')
            OR json_extract(NEW.canonical_json, '$.risk.policy_version') IS NOT
              json_extract(NEW.canonical_json, '$.permit.policy_version')
            OR json_type(NEW.canonical_json,
              '$.plan.market_evidence.snapshot_id') IS NOT 'text'
            OR trim(json_extract(NEW.canonical_json,
              '$.plan.market_evidence.snapshot_id')) = ''
            OR json_extract(NEW.canonical_json,
              '$.plan.market_evidence.snapshot_id') IS NOT
              json_extract(NEW.canonical_json, '$.permit.market_snapshot_id')
            OR json_extract(NEW.canonical_json,
              '$.plan.market_evidence.quality') IS NOT 'CONSISTENT'
            OR json_type(NEW.canonical_json,
              '$.plan.market_evidence.observed_at') IS NOT 'text'
            OR substr(json_extract(NEW.canonical_json,
              '$.plan.market_evidence.observed_at'), -6) != '+00:00'
            OR julianday(json_extract(NEW.canonical_json,
              '$.plan.market_evidence.observed_at')) IS NULL
            OR json_type(NEW.canonical_json,
              '$.plan.market_evidence.minimum_limit_price') IS NOT 'text'
            OR json_extract(NEW.canonical_json,
              '$.plan.market_evidence.minimum_limit_price') IS NOT
              json_extract(NEW.canonical_json, '$.plan.minimum_limit_price')
            OR json_type(NEW.canonical_json,
              '$.plan.market_evidence.maximum_limit_price') IS NOT 'text'
            OR json_extract(NEW.canonical_json,
              '$.plan.market_evidence.maximum_limit_price') IS NOT
              json_extract(NEW.canonical_json, '$.plan.maximum_limit_price')
            OR CAST(json_extract(NEW.canonical_json,
              '$.plan.market_evidence.minimum_limit_price') AS REAL) <= 0
            OR CAST(json_extract(NEW.canonical_json,
              '$.plan.market_evidence.maximum_limit_price') AS REAL) <
              CAST(json_extract(NEW.canonical_json,
                '$.plan.market_evidence.minimum_limit_price') AS REAL)
          THEN RAISE(ABORT, 'invalid order environment contract') END;
        END""",
    """CREATE TRIGGER operator_environment_contract BEFORE INSERT ON operator_commands
        BEGIN
          SELECT CASE WHEN
            json_type(NEW.canonical_json, '$.environment') IS NOT 'text'
            OR json_extract(NEW.canonical_json, '$.environment') NOT IN
              ('SIMULATED','PAPER','LIVE')
          THEN RAISE(ABORT, 'invalid operator environment contract') END;
        END""",
)
V9_STATEMENTS = (
    """CREATE TABLE risk_reservations (
        client_order_id TEXT PRIMARY KEY,
        account_id TEXT NOT NULL, account_snapshot_id TEXT NOT NULL,
        environment TEXT NOT NULL CHECK(environment IN ('SIMULATED','PAPER','LIVE')),
        policy_version TEXT NOT NULL, market TEXT NOT NULL, symbol TEXT NOT NULL,
        account_currency TEXT NOT NULL CHECK(account_currency = 'USD'),
        instrument_currency TEXT NOT NULL CHECK(instrument_currency = account_currency),
        side TEXT NOT NULL CHECK(side IN ('BUY','SELL')),
        quantity INTEGER NOT NULL CHECK(typeof(quantity)='integer' AND quantity BETWEEN 1 AND 9223372036854775807),
        available_cash_minor INTEGER NOT NULL CHECK(typeof(available_cash_minor)='integer' AND available_cash_minor BETWEEN 0 AND 9223372036854775807),
        current_exposure_minor INTEGER NOT NULL CHECK(typeof(current_exposure_minor)='integer' AND current_exposure_minor BETWEEN 0 AND 9223372036854775807),
        current_position_quantity INTEGER NOT NULL CHECK(typeof(current_position_quantity)='integer' AND current_position_quantity BETWEEN 0 AND 9223372036854775807),
        cash_cap_minor INTEGER NOT NULL CHECK(typeof(cash_cap_minor)='integer' AND cash_cap_minor BETWEEN 0 AND 9223372036854775807),
        exposure_cap_minor INTEGER NOT NULL CHECK(typeof(exposure_cap_minor)='integer' AND exposure_cap_minor BETWEEN 0 AND 9223372036854775807),
        fee_buffer_minor INTEGER NOT NULL CHECK(typeof(fee_buffer_minor)='integer' AND fee_buffer_minor BETWEEN 0 AND 9223372036854775807),
        reserved_cash_minor INTEGER NOT NULL CHECK(typeof(reserved_cash_minor)='integer' AND reserved_cash_minor BETWEEN 0 AND 9223372036854775807),
        reserved_exposure_minor INTEGER NOT NULL CHECK(typeof(reserved_exposure_minor)='integer' AND reserved_exposure_minor BETWEEN 0 AND 9223372036854775807),
        reserved_sell_quantity INTEGER NOT NULL CHECK(typeof(reserved_sell_quantity)='integer' AND reserved_sell_quantity BETWEEN 0 AND 9223372036854775807),
        reserved_at TEXT NOT NULL,
        CHECK(length(trim(account_id)) > 0 AND length(trim(account_snapshot_id)) > 0
          AND length(trim(policy_version)) > 0 AND length(trim(market)) > 0
          AND length(trim(symbol)) > 0 AND substr(reserved_at, -6) = '+00:00'
          AND julianday(reserved_at) IS NOT NULL),
        CHECK((side='BUY' AND reserved_exposure_minor > 0
          AND reserved_cash_minor = reserved_exposure_minor + fee_buffer_minor
          AND reserved_sell_quantity = 0)
          OR (side='SELL' AND reserved_cash_minor=0 AND reserved_exposure_minor=0
          AND fee_buffer_minor=0 AND reserved_sell_quantity=quantity))
        )""",
    """CREATE TRIGGER risk_reservations_no_update BEFORE UPDATE ON risk_reservations
        BEGIN SELECT RAISE(ABORT, 'risk reservations are immutable'); END""",
    """CREATE TRIGGER risk_reservations_no_delete BEFORE DELETE ON risk_reservations
        BEGIN SELECT RAISE(ABORT, 'risk reservations are immutable'); END""",
    """CREATE UNIQUE INDEX risk_reserved_once ON ledger_events(aggregate_id)
        WHERE event_type = 'RISK_RESERVED'""",
    """CREATE TRIGGER risk_event_contract BEFORE INSERT ON ledger_events
        WHEN NEW.event_type IN ('RISK_RESERVED','RISK_RELEASED')
        BEGIN
          SELECT CASE WHEN trim(NEW.event_id) = '' OR substr(NEW.occurred_at, -6) != '+00:00'
            OR julianday(NEW.occurred_at) IS NULL
            OR (SELECT COUNT(*) FROM json_each(NEW.payload_json)) != 3
            OR (SELECT COUNT(DISTINCT key) FROM json_each(NEW.payload_json)) != 3
            OR EXISTS (SELECT 1 FROM json_each(NEW.payload_json) WHERE key NOT IN
              ('reserved_cash_minor','reserved_exposure_minor','reserved_sell_quantity'))
            OR json_type(NEW.payload_json, '$.reserved_cash_minor') IS NOT 'integer'
            OR json_type(NEW.payload_json, '$.reserved_exposure_minor') IS NOT 'integer'
            OR json_type(NEW.payload_json, '$.reserved_sell_quantity') IS NOT 'integer'
            OR json_extract(NEW.payload_json, '$.reserved_cash_minor') < 0
            OR json_extract(NEW.payload_json, '$.reserved_exposure_minor') < 0
            OR json_extract(NEW.payload_json, '$.reserved_sell_quantity') < 0
            OR NOT EXISTS (SELECT 1 FROM risk_reservations r
              WHERE r.client_order_id = NEW.aggregate_id)
          THEN RAISE(ABORT, 'invalid risk event') END;
          SELECT CASE WHEN NEW.event_type='RISK_RESERVED' AND NOT EXISTS (
            SELECT 1 FROM risk_reservations r WHERE r.client_order_id=NEW.aggregate_id
              AND r.reserved_cash_minor=json_extract(NEW.payload_json,'$.reserved_cash_minor')
              AND r.reserved_exposure_minor=json_extract(NEW.payload_json,'$.reserved_exposure_minor')
              AND r.reserved_sell_quantity=json_extract(NEW.payload_json,'$.reserved_sell_quantity'))
          THEN RAISE(ABORT, 'risk reserve payload mismatch') END;
          SELECT CASE WHEN NEW.event_type='RISK_RELEASED' AND (
            NOT EXISTS (SELECT 1 FROM ledger_events e WHERE e.aggregate_id=NEW.aggregate_id
              AND e.event_type='RISK_RESERVED')
            OR json_extract(NEW.payload_json,'$.reserved_cash_minor')
              + COALESCE((SELECT SUM(json_extract(e.payload_json,'$.reserved_cash_minor'))
                FROM ledger_events e WHERE e.aggregate_id=NEW.aggregate_id
                AND e.event_type='RISK_RELEASED'),0) >
                (SELECT reserved_cash_minor FROM risk_reservations WHERE client_order_id=NEW.aggregate_id)
            OR json_extract(NEW.payload_json,'$.reserved_exposure_minor')
              + COALESCE((SELECT SUM(json_extract(e.payload_json,'$.reserved_exposure_minor'))
                FROM ledger_events e WHERE e.aggregate_id=NEW.aggregate_id
                AND e.event_type='RISK_RELEASED'),0) >
                (SELECT reserved_exposure_minor FROM risk_reservations WHERE client_order_id=NEW.aggregate_id)
            OR json_extract(NEW.payload_json,'$.reserved_sell_quantity')
              + COALESCE((SELECT SUM(json_extract(e.payload_json,'$.reserved_sell_quantity'))
                FROM ledger_events e WHERE e.aggregate_id=NEW.aggregate_id
                AND e.event_type='RISK_RELEASED'),0) >
                (SELECT reserved_sell_quantity FROM risk_reservations WHERE client_order_id=NEW.aggregate_id)
            OR NOT (
              EXISTS (SELECT 1 FROM ledger_events cause
                WHERE cause.aggregate_id=NEW.aggregate_id AND cause.sequence=(
                  SELECT MAX(prior.sequence) FROM ledger_events prior
                  WHERE prior.aggregate_id=NEW.aggregate_id)
                AND cause.event_type IN
                  ('BROKER_FILL_OBSERVED','BROKER_ORDER_CANCELED',
                   'BROKER_ORDER_EXPIRED','BROKER_ORDER_REJECTED')
                AND NEW.event_id='risk-released:' || cause.event_id)
              OR (
                EXISTS (SELECT 1 FROM risk_reservations r
                  WHERE r.client_order_id=NEW.aggregate_id
                    AND r.reserved_cash_minor=
                      json_extract(NEW.payload_json,'$.reserved_cash_minor')
                    AND r.reserved_exposure_minor=
                      json_extract(NEW.payload_json,'$.reserved_exposure_minor')
                    AND r.reserved_sell_quantity=
                      json_extract(NEW.payload_json,'$.reserved_sell_quantity'))
                AND EXISTS (SELECT 1 FROM ledger_events terminal
                  WHERE terminal.aggregate_id=NEW.aggregate_id AND terminal.sequence=(
                    SELECT MAX(prior.sequence) FROM ledger_events prior
                    WHERE prior.aggregate_id=NEW.aggregate_id)
                  AND (terminal.event_type='SUBMISSION_REJECTED'
                    OR (terminal.event_type='SUBMITTED_UNKNOWN_RESOLVED'
                      AND json_extract(terminal.payload_json,'$.result')=
                        'CONFIRMED_ABSENT'))))))
          THEN RAISE(ABORT, 'risk release exceeds reservation') END;
        END""",
)

V10_RESOLUTION_SQL = """CREATE TRIGGER unknown_resolution_contract
    AFTER INSERT ON ledger_events
    WHEN NEW.event_type = 'SUBMITTED_UNKNOWN_RESOLVED'
    BEGIN
      SELECT CASE WHEN trim(NEW.event_id) = ''
        OR substr(NEW.occurred_at, -6) != '+00:00'
        OR julianday(NEW.occurred_at) IS NULL
        OR json_type(NEW.payload_json, '$.operator_command_id') IS NOT 'text'
        OR trim(json_extract(NEW.payload_json, '$.operator_command_id')) = ''
        OR json_type(NEW.payload_json, '$.result') IS NOT 'text'
        OR json_extract(NEW.payload_json, '$.result') NOT IN
          ('BROKER_ORDER_LINKED','CONFIRMED_ABSENT','MANUAL_ACTIVITY_LINKED')
        OR (SELECT COUNT(*) FROM json_each(NEW.payload_json)) != CASE
          WHEN json_extract(NEW.payload_json, '$.result')='CONFIRMED_ABSENT' THEN 3
          ELSE 5 END
        OR (SELECT COUNT(DISTINCT key) FROM json_each(NEW.payload_json)) != CASE
          WHEN json_extract(NEW.payload_json, '$.result')='CONFIRMED_ABSENT' THEN 3
          ELSE 5 END
        OR EXISTS (SELECT 1 FROM json_each(NEW.payload_json) WHERE key NOT IN
          ('operator_command_id','result','query','broker_order_ref',
           'source_api_id','manual_activity'))
        OR json_type(NEW.payload_json, '$.query') IS NOT 'object'
        OR (SELECT COUNT(*) FROM json_each(NEW.payload_json, '$.query')) != 15
        OR (SELECT COUNT(DISTINCT key) FROM json_each(NEW.payload_json, '$.query')) != 15
        OR EXISTS (SELECT 1 FROM json_each(NEW.payload_json, '$.query') WHERE key NOT IN
          ('environment','account_id','business_date','window_started_at','window_completed_at',
           'query_policy_version','required_source_capabilities','queried_api_ids',
           'pagination_complete','observation_ids','response_sha256','candidate_set_sha256',
           'candidate_count','candidates','fetched_at'))
        OR json_extract(NEW.payload_json, '$.query.environment') NOT IN
          ('SIMULATED','PAPER','LIVE')
        OR json_type(NEW.payload_json, '$.query.account_id') IS NOT 'text'
        OR trim(json_extract(NEW.payload_json, '$.query.account_id')) = ''
        OR (length(json_extract(NEW.payload_json, '$.query.account_id')) BETWEEN 8 AND 14
          AND replace(json_extract(NEW.payload_json, '$.query.account_id'),'-','')
            NOT GLOB '*[^0-9]*'
          AND length(replace(json_extract(NEW.payload_json, '$.query.account_id'),'-','')) >= 8)
        OR json_type(NEW.payload_json, '$.query.business_date') IS NOT 'text'
        OR length(json_extract(NEW.payload_json, '$.query.business_date')) != 10
        OR date(json_extract(NEW.payload_json, '$.query.business_date')) IS NULL
        OR json_type(NEW.payload_json, '$.query.query_policy_version') IS NOT 'text'
        OR json_extract(NEW.payload_json, '$.query.query_policy_version') IS NOT
          'unknown-resolution-v1'
        OR json_type(NEW.payload_json, '$.query.window_started_at') IS NOT 'text'
        OR json_type(NEW.payload_json, '$.query.window_completed_at') IS NOT 'text'
        OR json_type(NEW.payload_json, '$.query.fetched_at') IS NOT 'text'
        OR substr(json_extract(NEW.payload_json, '$.query.window_started_at'),-6)!='+00:00'
        OR substr(json_extract(NEW.payload_json, '$.query.window_completed_at'),-6)!='+00:00'
        OR substr(json_extract(NEW.payload_json, '$.query.fetched_at'),-6)!='+00:00'
        OR julianday(json_extract(NEW.payload_json, '$.query.window_started_at')) IS NULL
        OR julianday(json_extract(NEW.payload_json, '$.query.window_completed_at')) IS NULL
        OR julianday(json_extract(NEW.payload_json, '$.query.fetched_at')) IS NULL
        OR julianday(json_extract(NEW.payload_json, '$.query.window_started_at')) >
          julianday(json_extract(NEW.payload_json, '$.query.window_completed_at'))
        OR julianday(json_extract(NEW.payload_json, '$.query.window_completed_at')) >
          julianday(json_extract(NEW.payload_json, '$.query.fetched_at'))
        OR julianday(json_extract(NEW.payload_json, '$.query.fetched_at')) > julianday(NEW.occurred_at)
        OR json_type(NEW.payload_json, '$.query.pagination_complete') IS NOT 'true'
        OR json_type(NEW.payload_json, '$.query.candidate_count') IS NOT 'integer'
        OR json_extract(NEW.payload_json, '$.query.candidate_count') != CASE
          WHEN json_extract(NEW.payload_json, '$.result')='CONFIRMED_ABSENT' THEN 0 ELSE 1 END
        OR json_type(NEW.payload_json, '$.query.queried_api_ids') IS NOT 'array'
        OR json_type(NEW.payload_json, '$.query.required_source_capabilities') IS NOT 'array'
        OR json_extract(NEW.payload_json, '$.query.required_source_capabilities') IS NOT
          json_array('broker.orders.read')
        OR json_extract(NEW.payload_json, '$.query.required_source_capabilities') IS NOT
          json_extract(NEW.payload_json, '$.query.queried_api_ids')
        OR json_array_length(NEW.payload_json, '$.query.queried_api_ids') < 1
        OR EXISTS (SELECT 1 FROM json_each(NEW.payload_json, '$.query.queried_api_ids')
          WHERE type IS NOT 'text' OR trim(value)='')
        OR (SELECT COUNT(*) FROM json_each(NEW.payload_json, '$.query.queried_api_ids')) !=
          (SELECT COUNT(DISTINCT value) FROM json_each(NEW.payload_json, '$.query.queried_api_ids'))
        OR json_type(NEW.payload_json, '$.query.observation_ids') IS NOT 'array'
        OR json_array_length(NEW.payload_json, '$.query.observation_ids') < 1
        OR EXISTS (SELECT 1 FROM json_each(NEW.payload_json, '$.query.observation_ids')
          WHERE type IS NOT 'text' OR trim(value)='')
        OR (SELECT COUNT(*) FROM json_each(NEW.payload_json, '$.query.observation_ids')) !=
          (SELECT COUNT(DISTINCT value) FROM json_each(NEW.payload_json, '$.query.observation_ids'))
        OR json_type(NEW.payload_json, '$.query.response_sha256') IS NOT 'text'
        OR length(json_extract(NEW.payload_json, '$.query.response_sha256')) != 64
        OR json_extract(NEW.payload_json, '$.query.response_sha256') GLOB '*[^0-9a-f]*'
        OR json_type(NEW.payload_json, '$.query.candidate_set_sha256') IS NOT 'text'
        OR length(json_extract(NEW.payload_json, '$.query.candidate_set_sha256')) != 64
        OR json_extract(NEW.payload_json, '$.query.candidate_set_sha256') GLOB '*[^0-9a-f]*'
        OR json_type(NEW.payload_json, '$.query.candidates') IS NOT 'array'
        OR json_array_length(NEW.payload_json, '$.query.candidates') IS NOT
          json_extract(NEW.payload_json, '$.query.candidate_count')
        OR EXISTS (SELECT 1 FROM json_each(NEW.payload_json, '$.query.candidates') candidate
          WHERE candidate.type IS NOT 'object'
            OR (SELECT COUNT(*) FROM json_each(candidate.value)) != 4
            OR (SELECT COUNT(DISTINCT key) FROM json_each(candidate.value)) != 4
            OR EXISTS (SELECT 1 FROM json_each(candidate.value) WHERE key NOT IN
              ('environment','account_id','business_date','broker_order_id'))
            OR json_extract(candidate.value, '$.environment') IS NOT
              json_extract(NEW.payload_json, '$.query.environment')
            OR json_extract(candidate.value, '$.account_id') IS NOT
              json_extract(NEW.payload_json, '$.query.account_id')
            OR json_extract(candidate.value, '$.business_date') IS NOT
              json_extract(NEW.payload_json, '$.query.business_date')
            OR json_type(candidate.value, '$.broker_order_id') IS NOT 'text'
            OR trim(json_extract(candidate.value, '$.broker_order_id'))='')
      THEN RAISE(ABORT, 'invalid typed unknown resolution evidence') END;
      SELECT CASE WHEN json_extract(NEW.payload_json, '$.result')='BROKER_ORDER_LINKED' AND (
          json_type(NEW.payload_json, '$.source_api_id') IS NOT 'text'
          OR NOT EXISTS (SELECT 1 FROM json_each(NEW.payload_json, '$.query.queried_api_ids')
            WHERE value=json_extract(NEW.payload_json, '$.source_api_id'))
          OR json_type(NEW.payload_json, '$.manual_activity') IS NOT NULL)
        OR json_extract(NEW.payload_json, '$.result')='CONFIRMED_ABSENT' AND (
          json_type(NEW.payload_json, '$.broker_order_ref') IS NOT NULL
          OR json_type(NEW.payload_json, '$.source_api_id') IS NOT NULL
          OR json_type(NEW.payload_json, '$.manual_activity') IS NOT NULL)
        OR json_extract(NEW.payload_json, '$.result')='MANUAL_ACTIVITY_LINKED' AND (
          json_type(NEW.payload_json, '$.source_api_id') IS NOT NULL
          OR json_type(NEW.payload_json, '$.manual_activity') IS NOT 'object'
          OR (SELECT COUNT(*) FROM json_each(NEW.payload_json, '$.manual_activity')) != 3
          OR (SELECT COUNT(DISTINCT key) FROM json_each(
               NEW.payload_json, '$.manual_activity')) != 3
          OR EXISTS (SELECT 1 FROM json_each(NEW.payload_json, '$.manual_activity')
            WHERE key NOT IN ('reference','actor','observed_at'))
          OR json_type(NEW.payload_json, '$.manual_activity.reference') IS NOT 'text'
          OR trim(json_extract(NEW.payload_json, '$.manual_activity.reference'))=''
          OR json_type(NEW.payload_json, '$.manual_activity.actor') IS NOT 'text'
          OR trim(json_extract(NEW.payload_json, '$.manual_activity.actor'))=''
          OR json_type(NEW.payload_json, '$.manual_activity.observed_at') IS NOT 'text'
          OR substr(json_extract(NEW.payload_json, '$.manual_activity.observed_at'),-6)!='+00:00'
          OR julianday(json_extract(NEW.payload_json, '$.manual_activity.observed_at')) IS NULL
          OR julianday(json_extract(NEW.payload_json, '$.manual_activity.observed_at')) >
             julianday(json_extract(NEW.payload_json, '$.query.fetched_at')))
      THEN RAISE(ABORT, 'invalid unknown resolution variant') END;
      SELECT CASE WHEN json_extract(NEW.payload_json, '$.result')!='CONFIRMED_ABSENT' AND (
          json_type(NEW.payload_json, '$.broker_order_ref') IS NOT 'object'
          OR (SELECT COUNT(*) FROM json_each(NEW.payload_json, '$.broker_order_ref')) != 4
          OR (SELECT COUNT(DISTINCT key) FROM json_each(
               NEW.payload_json, '$.broker_order_ref')) != 4
          OR EXISTS (SELECT 1 FROM json_each(NEW.payload_json, '$.broker_order_ref')
            WHERE key NOT IN ('environment','account_id','business_date','broker_order_id'))
          OR json_extract(NEW.payload_json, '$.broker_order_ref.environment') IS NOT
             json_extract(NEW.payload_json, '$.query.environment')
          OR json_extract(NEW.payload_json, '$.broker_order_ref.account_id') IS NOT
             json_extract(NEW.payload_json, '$.query.account_id')
          OR json_extract(NEW.payload_json, '$.broker_order_ref.business_date') IS NOT
             json_extract(NEW.payload_json, '$.query.business_date')
          OR date(json_extract(NEW.payload_json, '$.broker_order_ref.business_date')) <
             date(json_extract(NEW.payload_json, '$.query.window_started_at'))
          OR date(json_extract(NEW.payload_json, '$.broker_order_ref.business_date')) >
             date(json_extract(NEW.payload_json, '$.query.window_completed_at'))
          OR json_type(NEW.payload_json, '$.broker_order_ref.business_date') IS NOT 'text'
          OR length(json_extract(NEW.payload_json, '$.broker_order_ref.business_date')) != 10
          OR date(json_extract(NEW.payload_json, '$.broker_order_ref.business_date')) IS NULL
          OR json_type(NEW.payload_json, '$.broker_order_ref.broker_order_id') IS NOT 'text'
          OR trim(json_extract(NEW.payload_json, '$.broker_order_ref.broker_order_id'))=''
          OR NOT EXISTS (SELECT 1 FROM json_each(NEW.payload_json, '$.query.candidates') candidate
            WHERE json_extract(candidate.value, '$.environment') IS
                json_extract(NEW.payload_json, '$.broker_order_ref.environment')
              AND json_extract(candidate.value, '$.account_id') IS
                json_extract(NEW.payload_json, '$.broker_order_ref.account_id')
              AND json_extract(candidate.value, '$.business_date') IS
                json_extract(NEW.payload_json, '$.broker_order_ref.business_date')
              AND json_extract(candidate.value, '$.broker_order_id') IS
                json_extract(NEW.payload_json, '$.broker_order_ref.broker_order_id')))
      THEN RAISE(ABORT, 'invalid broker order reference') END;
      SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM order_requests AS reserved
        JOIN operator_commands AS command
          ON command.command_id=json_extract(NEW.payload_json, '$.operator_command_id')
        WHERE reserved.client_order_id=NEW.aggregate_id
          AND json_extract(command.canonical_json, '$.action')='RESOLVE_SUBMITTED_UNKNOWN'
          AND json_extract(command.canonical_json, '$.client_order_id')=NEW.aggregate_id
          AND json_extract(command.canonical_json, '$.account_id')=
              json_extract(reserved.canonical_json, '$.request.account_id')
          AND json_extract(command.canonical_json, '$.account_id')=
              json_extract(NEW.payload_json, '$.query.account_id')
          AND json_extract(command.canonical_json, '$.environment')=
              json_extract(reserved.canonical_json, '$.environment')
          AND json_extract(command.canonical_json, '$.environment')=
              json_extract(NEW.payload_json, '$.query.environment'))
        OR NOT EXISTS (SELECT 1 FROM ledger_events requested
          WHERE requested.aggregate_id=json_extract(NEW.payload_json, '$.operator_command_id')
            AND requested.event_type='OPERATOR_COMMAND_REQUESTED' AND requested.sequence<NEW.sequence)
        OR EXISTS (SELECT 1 FROM ledger_events terminal
          WHERE terminal.aggregate_id=json_extract(NEW.payload_json, '$.operator_command_id')
            AND terminal.event_type IN ('OPERATOR_COMMAND_SUCCEEDED','OPERATOR_COMMAND_FAILED')
            AND terminal.sequence<NEW.sequence)
        OR NOT EXISTS (SELECT 1 FROM ledger_events unknown_event
          WHERE unknown_event.aggregate_id=NEW.aggregate_id
            AND unknown_event.event_type='SUBMITTED_UNKNOWN' AND unknown_event.sequence<NEW.sequence
            AND unknown_event.sequence>COALESCE((SELECT MAX(prior.sequence)
              FROM ledger_events prior WHERE prior.aggregate_id=NEW.aggregate_id
                AND prior.event_type='SUBMITTED_UNKNOWN_RESOLVED'
                AND prior.sequence<NEW.sequence),0))
      THEN RAISE(ABORT, 'invalid unknown resolution audit chain') END;
    END"""
V10_STATEMENTS = ("DROP TRIGGER unknown_resolution_contract", V10_RESOLUTION_SQL)


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
            7: V7_OBJECTS,
            8: V8_OBJECTS,
            9: V9_OBJECTS,
            SCHEMA_VERSION: CURRENT_OBJECTS,
        }[version]
        if cls._objects(connection) != expected_objects:
            raise SchemaError("database objects do not match the known schema")
        tables: tuple[str, ...]
        if version == 0:
            tables = ("ledger_events", "order_requests")
        elif version < 5:
            tables = ("ledger_events", "order_requests", "operator_commands")
        elif version < 9:
            tables = tuple(name for name in TABLE_COLUMNS if name != "risk_reservations")
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
        if version >= 8:
            expected_sql.update(
                {
                    "order_environment_contract": V8_STATEMENTS[0],
                    "operator_environment_contract": V8_STATEMENTS[1],
                }
            )
        if version >= 9:
            expected_sql.update({
                "risk_reservations": V9_STATEMENTS[0],
                "risk_reservations_no_update": V9_STATEMENTS[1],
                "risk_reservations_no_delete": V9_STATEMENTS[2],
                "risk_reserved_once": V9_STATEMENTS[3],
                "risk_event_contract": V9_STATEMENTS[4],
            })
        if version >= 10:
            expected_sql["unknown_resolution_contract"] = V10_RESOLUTION_SQL
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
    def _strict_json(value: str) -> dict[str, object]:
        def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, item in pairs:
                if key in result:
                    raise ValueError(f"duplicate JSON key: {key}")
                result[key] = item
            return result

        parsed = json.loads(value, object_pairs_hook=reject_duplicates)
        if not isinstance(parsed, dict):
            raise ValueError("canonical JSON must be an object")
        return parsed

    @staticmethod
    def _cancel_binding_is_valid(canonical: dict[str, object]) -> bool:
        payload = canonical.get("cancel_order")
        digest = canonical.get("cancel_order_sha256")
        if canonical.get("action") != OperatorAction.ISSUE_CANCEL.value:
            return payload is None and digest is None
        if not isinstance(payload, dict) or not isinstance(digest, str):
            return False
        target = payload.get("target")
        instrument = payload.get("instrument")
        if not isinstance(target, dict) or not isinstance(instrument, dict):
            return False
        try:
            quantity = payload["remaining_quantity"]
            valid = (
                set(payload) == {
                    "command_id", "target", "instrument", "remaining_quantity",
                    "account_snapshot_id",
                }
                and set(target) == {
                    "environment", "account_id", "business_date", "broker_order_id",
                }
                and target["environment"] == canonical["environment"]
                and target["account_id"] == canonical["account_id"]
                and isinstance(target["business_date"], str)
                and datetime.fromisoformat(target["business_date"]).date().isoformat()
                == target["business_date"]
                and set(instrument) == {"market", "symbol", "currency"}
                and isinstance(quantity, str)
                and canonical_share_quantity(
                    Decimal(quantity), "remaining_quantity"
                ) == quantity
                and Decimal(quantity) > 0
            )
            for value in (
                target["broker_order_id"], payload["account_snapshot_id"],
                instrument["market"], instrument["symbol"], instrument["currency"],
            ):
                require_id(value, "cancel binding")
        except (KeyError, TypeError, ValueError, InvalidOperation):
            return False
        expected = hashlib.sha256(
            json.dumps(
                payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest()
        return valid and digest == expected

    @staticmethod
    def _validate_submission_states(connection: sqlite3.Connection) -> None:
        order_ids = {
            row[0] for row in connection.execute(
                "SELECT client_order_id FROM order_requests"
            )
        }
        submission_states: dict[str, str] = {}
        for event_type, aggregate_id in connection.execute(
            """SELECT event_type, aggregate_id FROM ledger_events ORDER BY sequence"""
        ):
            previous_submission_state = submission_states.get(aggregate_id)
            if event_type == "PREPARED":
                if aggregate_id not in order_ids or previous_submission_state is not None:
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
                if version >= 8:
                    expected_metadata.update({
                        "order_environment_v8_cutoff",
                        "operator_environment_v8_cutoff",
                    })
                if version >= 9:
                    expected_metadata.add("risk_reservation_v9_order_cutoff")
                if version >= 10:
                    expected_metadata.add("typed_resolution_v10_cutoff")
                metadata_map = dict(metadata)
                if len(metadata) != len(expected_metadata) or set(metadata_map) != expected_metadata:
                    raise SchemaError("schema metadata contract is malformed")
                terminal_v5_cutoff = int(metadata_map["terminal_payload_v5_cutoff"])
                operator_v7_cutoff = int(metadata_map.get("operator_binding_v7_cutoff", "0"))
                order_v8_cutoff = int(metadata_map.get("order_environment_v8_cutoff", "0"))
                command_v8_cutoff = int(
                    metadata_map.get("operator_environment_v8_cutoff", "0")
                )
                reservation_v9_cutoff = int(
                    metadata_map.get("risk_reservation_v9_order_cutoff", "0")
                )
                typed_resolution_v10_cutoff = int(
                    metadata_map.get("typed_resolution_v10_cutoff", "0")
                )
                if terminal_v5_cutoff < 0:
                    raise SchemaError("terminal payload cutoff cannot be negative")
                highest_sequence = int(connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) FROM ledger_events"
                ).fetchone()[0])
                if terminal_v5_cutoff > highest_sequence:
                    raise SchemaError("terminal payload cutoff exceeds ledger history")
                if operator_v7_cutoff < 0 or operator_v7_cutoff > highest_sequence:
                    raise SchemaError("operator binding cutoff is outside ledger history")
                highest_order_rowid = int(connection.execute(
                    "SELECT COALESCE(MAX(rowid), 0) FROM order_requests"
                ).fetchone()[0])
                highest_command_rowid = int(connection.execute(
                    "SELECT COALESCE(MAX(rowid), 0) FROM operator_commands"
                ).fetchone()[0])
                if not 0 <= order_v8_cutoff <= highest_order_rowid:
                    raise SchemaError("order environment cutoff is outside history")
                if not 0 <= command_v8_cutoff <= highest_command_rowid:
                    raise SchemaError("operator environment cutoff is outside history")
                if not 0 <= reservation_v9_cutoff <= highest_order_rowid:
                    raise SchemaError("risk reservation cutoff is outside history")
                if not 0 <= typed_resolution_v10_cutoff <= highest_sequence:
                    raise SchemaError("typed resolution cutoff is outside history")
            else:
                terminal_v5_cutoff = int(connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) FROM ledger_events"
                ).fetchone()[0])
                operator_v7_cutoff = 0
                order_v8_cutoff = 0
                command_v8_cutoff = 0
                reservation_v9_cutoff = 0
                typed_resolution_v10_cutoff = 0
            if version < 10:
                typed_resolution_v10_cutoff = int(connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) FROM ledger_events"
                ).fetchone()[0])
            command_rows = connection.execute(
                "SELECT rowid, command_id, canonical_json FROM operator_commands"
            ).fetchall()
            commands: dict[str, tuple[OperatorCommand, dict[str, object]]] = {}
            for rowid, command_id, canonical_json in command_rows:
                canonical = SQLiteLedger._strict_json(canonical_json)
                legacy_command_keys = {
                    "command_id", "actor", "reason", "deployment_version",
                    "expected_safety_epoch", "requested_at", "expires_at", "action",
                    "account_id",
                }
                current_command_keys = legacy_command_keys | {
                    "client_order_id", "risk_decision_id", "execution_plan_id",
                }
                environment_command_keys = current_command_keys | {"environment"}
                cancel_keys = {"cancel_order", "cancel_order_sha256"}
                bound_legacy_keys = legacy_command_keys | cancel_keys
                bound_current_keys = current_command_keys | cancel_keys
                bound_command_keys = environment_command_keys | cancel_keys
                legacy_command = frozenset(canonical) in {
                    frozenset(legacy_command_keys), frozenset(bound_legacy_keys),
                }
                if (
                    frozenset(canonical) not in {
                        frozenset(legacy_command_keys), frozenset(current_command_keys),
                        frozenset(environment_command_keys), frozenset(bound_legacy_keys),
                        frozenset(bound_current_keys), frozenset(bound_command_keys),
                    }
                    or canonical["command_id"] != command_id
                    or (
                        version >= 8
                        and ((rowid > command_v8_cutoff) != ("environment" in canonical))
                    )
                ):
                    raise SchemaError("operator command canonical payload is malformed")
                command_id_value = canonical["command_id"]
                actor = canonical["actor"]
                reason = canonical["reason"]
                deployment_version = canonical["deployment_version"]
                expected_safety_epoch = canonical["expected_safety_epoch"]
                requested_at = canonical["requested_at"]
                expires_at = canonical["expires_at"]
                action = canonical["action"]
                account_id = canonical["account_id"]
                environment = canonical.get(
                    "environment", TradingEnvironment.SIMULATED.value
                )
                client_order_id = canonical.get("client_order_id")
                risk_decision_id = canonical.get("risk_decision_id")
                execution_plan_id = canonical.get("execution_plan_id")
                if (
                    not all(isinstance(value, str) for value in (
                        command_id_value, actor, reason, deployment_version,
                        requested_at, expires_at, action, environment,
                    ))
                    or type(expected_safety_epoch) is not int
                    or (account_id is not None and not isinstance(account_id, str))
                    or any(value is not None and not isinstance(value, str) for value in (
                        client_order_id, risk_decision_id, execution_plan_id,
                    ))
                ):
                    raise SchemaError("operator command canonical payload is malformed")
                assert isinstance(command_id_value, str)
                assert isinstance(actor, str)
                assert isinstance(reason, str)
                assert isinstance(deployment_version, str)
                assert type(expected_safety_epoch) is int
                assert isinstance(requested_at, str)
                assert isinstance(expires_at, str)
                assert isinstance(action, str)
                assert isinstance(environment, str)
                assert account_id is None or isinstance(account_id, str)
                assert client_order_id is None or isinstance(client_order_id, str)
                assert risk_decision_id is None or isinstance(risk_decision_id, str)
                assert execution_plan_id is None or isinstance(execution_plan_id, str)
                parsed = OperatorCommand(
                    command_id=command_id_value,
                    actor=actor,
                    reason=reason,
                    deployment_version=deployment_version,
                    expected_safety_epoch=expected_safety_epoch,
                    requested_at=datetime.fromisoformat(requested_at),
                    expires_at=datetime.fromisoformat(expires_at),
                    action=OperatorAction(action),
                    account_id=account_id or "LEGACY_GLOBAL",
                    environment=TradingEnvironment(environment),
                    client_order_id=(
                        "LEGACY_UNBOUND"
                        if legacy_command
                        and action == "RESOLVE_SUBMITTED_UNKNOWN"
                        else client_order_id
                    ),
                    risk_decision_id=risk_decision_id,
                    execution_plan_id=execution_plan_id,
                )
                if (
                    cancel_keys.issubset(canonical)
                    and not SQLiteLedger._cancel_binding_is_valid(canonical)
                ):
                    raise SchemaError("operator cancellation binding is malformed")
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
                row[1]: SQLiteLedger._strict_json(row[2])
                for row in connection.execute(
                    "SELECT rowid, client_order_id, canonical_json FROM order_requests"
                )
            }
            if version >= 8:
                for rowid, client_order_id, canonical_json in connection.execute(
                    "SELECT rowid, client_order_id, canonical_json FROM order_requests"
                ):
                    canonical = SQLiteLedger._strict_json(canonical_json)
                    if rowid <= order_v8_cutoff:
                        if "environment" in canonical:
                            raise SchemaError("order environment payload crosses v8 cutoff")
                        continue
                    environment = canonical.get("environment")
                    permit = canonical.get("permit")
                    risk = canonical.get("risk")
                    plan = canonical.get("plan")
                    if not all(isinstance(value, dict) for value in (permit, risk, plan)):
                        raise SchemaError("order environment payload is malformed")
                    assert isinstance(permit, dict)
                    assert isinstance(risk, dict)
                    assert isinstance(plan, dict)
                    market = plan.get("market_evidence")
                    if (
                        environment not in {item.value for item in TradingEnvironment}
                        or permit.get("environment") != environment
                        or not isinstance(permit.get("permit_id"), str)
                        or not permit["permit_id"].strip()
                        or risk.get("input_snapshot_environment") != environment
                        or not isinstance(market, dict)
                        or market.get("environment") != environment
                        or market.get("pricing_policy_version")
                        != plan.get("pricing_policy_version")
                        or plan.get("pricing_policy_version") != risk.get("policy_version")
                        or risk.get("policy_version") != permit.get("policy_version")
                        or not isinstance(market.get("snapshot_id"), str)
                        or not market["snapshot_id"].strip()
                        or market.get("snapshot_id") != permit.get("market_snapshot_id")
                        or market.get("quality") != "CONSISTENT"
                        or market.get("minimum_limit_price")
                        != plan.get("minimum_limit_price")
                        or market.get("maximum_limit_price")
                        != plan.get("maximum_limit_price")
                    ):
                        raise SchemaError("order environment payload is malformed")
                    minimum = Decimal(market["minimum_limit_price"])
                    maximum = Decimal(market["maximum_limit_price"])
                    observed_at = datetime.fromisoformat(market["observed_at"])
                    require_utc(observed_at, "order market evidence observed_at")
                    if (
                        not minimum.is_finite()
                        or not maximum.is_finite()
                        or minimum <= 0
                        or maximum < minimum
                    ):
                        raise SchemaError("order market evidence band is malformed")
            duplicate_permit = connection.execute(
                """SELECT json_extract(canonical_json, '$.permit.permit_id')
                   FROM order_requests
                   WHERE json_type(canonical_json, '$.permit.permit_id') = 'text'
                   GROUP BY json_extract(canonical_json, '$.permit.permit_id')
                   HAVING COUNT(*) > 1 LIMIT 1"""
            ).fetchone()
            if duplicate_permit is not None:
                raise SchemaError("permit is consumed by multiple order requests")
            SQLiteLedger._validate_submission_states(connection)
            last_unknown: dict[str, int] = {}
            last_resolution: dict[str, int] = {}
            resolutions_by_command: dict[str, str] = {}
            for sequence, event_id, event_type, aggregate_id, occurred_at, payload_json in rows:
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
                            cancel_order = commands[aggregate_id][1].get("cancel_order")
                            cancel_target = (
                                cancel_order.get("target")
                                if isinstance(cancel_order, dict)
                                else None
                            )
                            cancel_broker_order_id = (
                                cancel_target.get("broker_order_id")
                                if isinstance(cancel_target, dict)
                                else None
                            )
                            other_permit_action = action in {
                                OperatorAction.ISSUE_REDUCE_ONLY,
                                OperatorAction.ISSUE_EMERGENCY_FLATTEN,
                            }
                            payload_invalid = payload_invalid or (
                                action is OperatorAction.RESOLVE_SUBMITTED_UNKNOWN
                                and (order_id is None or permit_id is not None)
                            ) or (
                                action is OperatorAction.ISSUE_CANCEL
                                and outcome is OperatorCommandOutcome.SUCCEEDED
                                and (
                                    permit_id is None
                                    or order_id is None
                                    or not SQLiteLedger._cancel_binding_is_valid(
                                        commands[aggregate_id][1]
                                    )
                                    or order_id != cancel_broker_order_id
                                )
                            ) or (
                                other_permit_action
                                and outcome is OperatorCommandOutcome.SUCCEEDED
                                and (permit_id is None or order_id is not None)
                            ) or (
                                (
                                    action not in {
                                        OperatorAction.ISSUE_CANCEL,
                                        OperatorAction.ISSUE_REDUCE_ONLY,
                                        OperatorAction.ISSUE_EMERGENCY_FLATTEN,
                                    }
                                    or outcome is OperatorCommandOutcome.FAILED
                                )
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
                resolution_time = datetime.fromisoformat(occurred_at)
                if sequence <= typed_resolution_v10_cutoff:
                    if set(payload) != {
                        "operator_command_id", "result", "observation", "reference",
                        "observed_at",
                    } or payload["result"] not in {
                        "BROKER_ORDER_LINKED", "CONFIRMED_ABSENT", "MANUAL_ACTIVITY_LINKED",
                    }:
                        raise SchemaError("legacy unknown resolution payload is malformed")
                    for key in ("operator_command_id", "observation", "reference"):
                        value = payload[key]
                        if not isinstance(value, str):
                            raise SchemaError(
                                "legacy unknown resolution payload is malformed"
                            )
                        require_id(value, key)
                    resolution_observed_at = payload["observed_at"]
                    if not isinstance(resolution_observed_at, str):
                        raise SchemaError(
                            "legacy unknown resolution payload is malformed"
                        )
                    evidence_time = datetime.fromisoformat(resolution_observed_at)
                    require_utc(evidence_time, "legacy unknown resolution observed_at")
                    if evidence_time > resolution_time:
                        raise SchemaError("unknown resolution evidence is from the future")
                else:
                    evidence = resolution_from_payload(payload)
                    if evidence.evidence.fetched_at > resolution_time:
                        raise SchemaError("unknown resolution evidence is from the future")
                command_id = payload["operator_command_id"]
                if not isinstance(command_id, str):
                    raise SchemaError("unknown resolution command ID is malformed")
                command_entry = commands.get(command_id)
                order = order_rows.get(aggregate_id)
                if not isinstance(order, dict):
                    raise SchemaError("resolved order has no internal account alias")
                request = order.get("request")
                if not isinstance(request, dict):
                    raise SchemaError("resolved order has no internal account alias")
                order_account = request.get("account_id")
                if not isinstance(order_account, str):
                    raise SchemaError("resolved order has no internal account alias")
                order_environment = order.get("environment")
                if (
                    command_entry is None
                    or command_entry[0].action is not OperatorAction.RESOLVE_SUBMITTED_UNKNOWN
                    or command_entry[0].account_id != order_account
                    or (
                        sequence > typed_resolution_v10_cutoff
                        and (
                            evidence.evidence.account_id != order_account
                            or evidence.evidence.environment is not command_entry[0].environment
                            or evidence.evidence.environment.value != order_environment
                        )
                    )
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
            if version >= 9:
                malformed = connection.execute(
                    """SELECT 1 FROM risk_reservations r
                       LEFT JOIN order_requests o ON o.client_order_id=r.client_order_id
                       LEFT JOIN ledger_events e ON e.aggregate_id=r.client_order_id
                         AND e.event_type='RISK_RESERVED'
                       GROUP BY r.client_order_id
                       HAVING o.client_order_id IS NULL OR COUNT(e.sequence) != 1
                         OR MAX(json_extract(e.payload_json,'$.reserved_cash_minor'))
                           != r.reserved_cash_minor
                         OR MAX(json_extract(e.payload_json,'$.reserved_exposure_minor'))
                           != r.reserved_exposure_minor
                         OR MAX(json_extract(e.payload_json,'$.reserved_sell_quantity'))
                           != r.reserved_sell_quantity
                       LIMIT 1"""
                ).fetchone()
                orphan = connection.execute(
                    """SELECT 1 FROM ledger_events e LEFT JOIN risk_reservations r
                         ON r.client_order_id=e.aggregate_id
                       WHERE e.event_type IN ('RISK_RESERVED','RISK_RELEASED')
                         AND r.client_order_id IS NULL LIMIT 1"""
                ).fetchone()
                if malformed is not None or orphan is not None:
                    raise SchemaError("risk reservation projection is malformed")
                lifecycle: dict[
                    str, list[tuple[int, str, BrokerLifecycleFact]]
                ] = {}
                broker_refs: dict[object, str] = {}
                execution_ids: set[str] = set()
                for (
                    sequence, event_id, event_type, aggregate_id, occurred_at,
                    payload_json,
                ) in rows:
                    if event_type not in BROKER_EVENT_TYPES:
                        continue
                    payload = SQLiteLedger._strict_json(payload_json)
                    fact = broker_fact_from_payload(payload)
                    kind = payload.get("kind")
                    if (
                        not isinstance(kind, str)
                        or kind not in BROKER_FACT_EVENTS
                        or BROKER_FACT_EVENTS[kind] != event_type
                        or fact.fact_id != event_id
                        or fact.client_order_id != aggregate_id
                        or fact.occurred_at != datetime.fromisoformat(occurred_at)
                    ):
                        raise SchemaError("broker lifecycle event is not canonical")
                    owner = broker_refs.setdefault(fact.broker_order_ref, aggregate_id)
                    if owner != aggregate_id:
                        raise SchemaError("broker order reference is bound to multiple clients")
                    if type(fact) is BrokerFillObserved:
                        if fact.broker_execution_id in execution_ids:
                            raise SchemaError("broker execution ID is duplicated")
                        execution_ids.add(fact.broker_execution_id)
                    lifecycle.setdefault(aggregate_id, []).append(
                        (sequence, event_type, fact)
                    )
                reservation_ids = {
                    row[0] for row in connection.execute(
                        "SELECT client_order_id FROM risk_reservations"
                    )
                }
                if not set(lifecycle).issubset(reservation_ids):
                    raise SchemaError("broker lifecycle has no risk reservation")
                for order_id, cash, exposure, sell, side in connection.execute(
                    """SELECT client_order_id,reserved_cash_minor,
                              reserved_exposure_minor,reserved_sell_quantity,side
                       FROM risk_reservations"""
                ):
                    requires_full_release = connection.execute(
                        """SELECT EXISTS(SELECT 1 FROM ledger_events
                               WHERE aggregate_id=? AND event_type='SUBMISSION_REJECTED')
                                  OR EXISTS(SELECT 1 FROM ledger_events
                               WHERE aggregate_id=?
                                 AND event_type='SUBMITTED_UNKNOWN_RESOLVED'
                                 AND json_extract(payload_json,'$.result')='CONFIRMED_ABSENT')""",
                        (order_id, order_id),
                    ).fetchone()[0]
                    releases = [
                        (row[0], row[1], SQLiteLedger._strict_json(row[2]))
                        for row in connection.execute(
                            """SELECT sequence,event_id,payload_json FROM ledger_events
                               WHERE aggregate_id=? AND event_type='RISK_RELEASED'
                               ORDER BY sequence""",
                            (order_id,),
                        )
                    ]
                    lifecycle_rows = lifecycle.get(order_id, [])
                    if lifecycle_rows:
                        facts = tuple(row[2] for row in lifecycle_rows)
                        projection = fold_broker_order(facts)
                        opened = facts[0]
                        assert type(opened) is BrokerOrderOpened
                        ack = [
                            (sequence, SQLiteLedger._strict_json(payload_json))
                            for sequence, _, event_type, aggregate_id, _, payload_json in rows
                            if aggregate_id == order_id and event_type == "ACKNOWLEDGED"
                        ]
                        order = order_rows.get(order_id)
                        if len(ack) != 1 or ack[0][0] >= lifecycle_rows[0][0]:
                            raise SchemaError("broker OPEN must follow exactly one ACK")
                        if not isinstance(order, dict) or not isinstance(ack[0][1], dict):
                            raise SchemaError("acknowledged broker order is malformed")
                        broker_order_id = ack[0][1].get("broker_order_id")
                        if not isinstance(broker_order_id, str):
                            raise SchemaError("acknowledged broker order is malformed")
                        SQLiteLedger._validate_open_against_submission(
                            opened, order, broker_order_id
                        )
                        expected_plan = SQLiteLedger._lifecycle_release_plan(
                            (cash, exposure, sell, side), facts
                        )
                        expected_releases = []
                        for fact_id, release in expected_plan:
                            fact_sequence = next(
                                row[0] for row in lifecycle_rows
                                if row[2].fact_id == fact_id
                            )
                            expected_releases.append(
                                (
                                    fact_sequence + 1,
                                    f"risk-released:{fact_id}",
                                    release,
                                )
                            )
                        if releases != expected_releases:
                            raise SchemaError(
                                "broker lifecycle release order or amount is malformed"
                            )
                        release_cash = [
                            item[2].get("reserved_cash_minor") for item in releases
                        ]
                        release_exposure = [
                            item[2].get("reserved_exposure_minor") for item in releases
                        ]
                        release_sell = [
                            item[2].get("reserved_sell_quantity") for item in releases
                        ]
                        if not all(
                            type(value) is int
                            for values in (release_cash, release_exposure, release_sell)
                            for value in values
                        ):
                            raise SchemaError("broker lifecycle release amount is malformed")
                        if projection.order.execution_state not in {
                            BrokerExecutionState.OPEN,
                            BrokerExecutionState.PARTIALLY_FILLED,
                        } and sum(value for value in release_cash if isinstance(value, int)) != cash:
                            raise SchemaError("terminal broker lifecycle did not fully release")
                        if projection.order.execution_state not in {
                            BrokerExecutionState.OPEN,
                            BrokerExecutionState.PARTIALLY_FILLED,
                        } and (
                            sum(
                                value for value in release_exposure
                                if isinstance(value, int)
                            )
                            != exposure
                            or sum(
                                value for value in release_sell
                                if isinstance(value, int)
                            ) != sell
                        ):
                            raise SchemaError("terminal broker lifecycle did not fully release")
                        continue
                    expected_release = {
                        "reserved_cash_minor": cash,
                        "reserved_exposure_minor": exposure,
                        "reserved_sell_quantity": sell,
                    }
                    if (
                        requires_full_release
                        and [row[2] for row in releases] != [expected_release]
                    ) or (not requires_full_release and releases):
                        raise SchemaError(
                            "terminal reservation state requires exactly one full release"
                        )
        except SchemaError:
            raise
        except (
            KeyError, TypeError, ValueError, InvalidOperation, json.JSONDecodeError,
        ) as error:
            raise SchemaError("audit semantic validation failed") from error

    def _initialize(self) -> None:
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            version = self.schema_version
            objects = self._objects(self.connection)
            if version > SCHEMA_VERSION or version not in {
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, SCHEMA_VERSION,
            }:
                raise SchemaError(f"unsupported schema version {version}")
            if version == SCHEMA_VERSION:
                self._validate_schema(self.connection, SCHEMA_VERSION)
                self._validate_audit_semantics(
                    self.connection, version=SCHEMA_VERSION
                )
                self.connection.execute("COMMIT")
                return
            statements: tuple[str, ...]
            if version == 9:
                self._validate_schema(self.connection, 9)
                self._validate_audit_semantics(self.connection, version=9)
                statements = V10_STATEMENTS
            elif version == 8:
                self._validate_schema(self.connection, 8)
                self._validate_audit_semantics(self.connection, version=8)
                statements = ()
            elif version == 7:
                self._validate_schema(self.connection, 7)
                self._validate_audit_semantics(self.connection, version=7)
                statements = V8_STATEMENTS
            elif version == 6:
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
            if version < 7:
                statements += V8_STATEMENTS
            if version < 9:
                statements += V9_STATEMENTS
            if version < 10 and version != 9:
                statements += V10_STATEMENTS
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
            if version < 8:
                order_cutoff = int(self.connection.execute(
                    "SELECT COALESCE(MAX(rowid), 0) FROM order_requests"
                ).fetchone()[0])
                command_cutoff = int(self.connection.execute(
                    "SELECT COALESCE(MAX(rowid), 0) FROM operator_commands"
                ).fetchone()[0])
                self.connection.execute(
                    "INSERT INTO schema_metadata VALUES ('order_environment_v8_cutoff', ?)",
                    (str(order_cutoff),),
                )
                self.connection.execute(
                    "INSERT INTO schema_metadata VALUES ('operator_environment_v8_cutoff', ?)",
                    (str(command_cutoff),),
                )
            if version < 9:
                reservation_cutoff = int(self.connection.execute(
                    "SELECT COALESCE(MAX(rowid), 0) FROM order_requests"
                ).fetchone()[0])
                self.connection.execute(
                    "INSERT INTO schema_metadata VALUES ('risk_reservation_v9_order_cutoff', ?)",
                    (str(reservation_cutoff),),
                )
            if version < 10:
                resolution_cutoff = int(self.connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) FROM ledger_events"
                ).fetchone()[0])
                self.connection.execute(
                    "INSERT INTO schema_metadata VALUES ('typed_resolution_v10_cutoff', ?)",
                    (str(resolution_cutoff),),
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
                recorded_at, self._json(
                    event.payload,
                    canonical=event.event_type == "SUBMITTED_UNKNOWN_RESOLVED",
                ),
            ),
        )

    @staticmethod
    def _risk_payload(terms: ReservationTerms) -> dict[str, int]:
        return {
            "reserved_cash_minor": terms.reserved_cash_minor,
            "reserved_exposure_minor": terms.reserved_exposure_minor,
            "reserved_sell_quantity": terms.reserved_sell_quantity,
        }

    def _insert_full_release(
        self, client_order_id: str, event_id: str, occurred_at: datetime, recorded_at: str,
    ) -> None:
        row = self.connection.execute(
            """SELECT reserved_cash_minor, reserved_exposure_minor,
                      reserved_sell_quantity FROM risk_reservations
               WHERE client_order_id=?""",
            (client_order_id,),
        ).fetchone()
        if row is None:
            raise ValueError("submission has no risk reservation")
        self._insert_event(
            LedgerEvent(
                event_id, "RISK_RELEASED", client_order_id, occurred_at,
                {
                    "reserved_cash_minor": row[0],
                    "reserved_exposure_minor": row[1],
                    "reserved_sell_quantity": row[2],
                },
            ),
            recorded_at,
        )

    @staticmethod
    def _lifecycle_release_plan(
        reservation: tuple[int, int, int, str],
        facts: tuple[BrokerLifecycleFact, ...],
    ) -> tuple[tuple[str, dict[str, int]], ...]:
        reserved_cash, reserved_exposure, reserved_sell, side = reservation
        released_cash = released_exposure = released_sell = 0
        plan: list[tuple[str, dict[str, int]]] = []
        for index, fact in enumerate(facts[1:], start=1):
            projection = fold_broker_order(facts[: index + 1])
            terminal = projection.order.execution_state not in {
                BrokerExecutionState.OPEN,
                BrokerExecutionState.PARTIALLY_FILLED,
            }
            if terminal:
                target_cash = reserved_cash
                target_exposure = reserved_exposure
                target_sell = reserved_sell
            elif side == Side.BUY.value:
                resolved = int(projection.order.filled)
                requested = int(projection.order.requested)
                target_exposure = reserved_exposure * resolved // requested
                target_cash = target_exposure
                target_sell = 0
            else:
                target_cash = target_exposure = 0
                target_sell = int(projection.order.filled)
            payload = {
                "reserved_cash_minor": target_cash - released_cash,
                "reserved_exposure_minor": target_exposure - released_exposure,
                "reserved_sell_quantity": target_sell - released_sell,
            }
            if min(payload.values()) < 0:
                raise ValueError("broker lifecycle release moved backwards")
            plan.append((fact.fact_id, payload))
            released_cash, released_exposure, released_sell = (
                target_cash, target_exposure, target_sell
            )
        return tuple(plan)

    @staticmethod
    def _facts_for(
        connection: sqlite3.Connection, client_order_id: str
    ) -> tuple[BrokerLifecycleFact, ...]:
        return tuple(
            broker_fact_from_payload(SQLiteLedger._strict_json(payload))
            for (payload,) in connection.execute(
                f"""SELECT payload_json FROM ledger_events
                    WHERE aggregate_id=? AND event_type IN
                      ({','.join('?' for _ in BROKER_EVENT_TYPES)})
                    ORDER BY sequence""",
                (client_order_id, *sorted(BROKER_EVENT_TYPES)),
            )
        )

    @staticmethod
    def _validate_open_against_submission(
        opened: BrokerOrderOpened,
        order_payload: dict[str, object],
        broker_order_id: str,
    ) -> None:
        request = order_payload.get("request")
        if not isinstance(request, dict):
            raise ValueError("reserved order request is malformed")
        instrument = request.get("instrument")
        expected_instrument = {
            "market": opened.instrument.market,
            "symbol": opened.instrument.symbol,
            "currency": opened.instrument.currency,
        }
        try:
            matches = (
                opened.broker_order_ref.broker_order_id == broker_order_id
                and opened.broker_order_ref.account_id == request["account_id"]
                and opened.broker_order_ref.environment.value == order_payload["environment"]
                and instrument == expected_instrument
                and opened.side.value == request["side"]
                and opened.requested_quantity == Decimal(request["quantity"])
            )
        except (KeyError, TypeError, InvalidOperation) as error:
            raise ValueError("reserved order request is malformed") from error
        if not matches:
            raise ValueError("broker OPEN fact does not match acknowledged submission")

    def record_broker_execution(self, fact: BrokerLifecycleFact) -> bool:
        if type(fact) not in BROKER_LIFECYCLE_FACT_TYPES:
            raise TypeError("exact typed broker lifecycle fact required")
        payload = canonical_broker_fact_payload(fact)
        kind = payload.get("kind")
        if not isinstance(kind, str) or kind not in BROKER_FACT_EVENTS:
            raise ValueError("broker lifecycle fact kind is invalid")
        event_type = BROKER_FACT_EVENTS[kind]
        recorded_at = datetime.now(timezone.utc).isoformat()
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            duplicate = self.connection.execute(
                """SELECT event_type,aggregate_id,payload_json FROM ledger_events
                   WHERE event_id=?""",
                (fact.fact_id,),
            ).fetchone()
            if duplicate is not None:
                if (
                    duplicate[0] != event_type
                    or duplicate[1] != fact.client_order_id
                    or SQLiteLedger._strict_json(duplicate[2]) != payload
                ):
                    raise ValueError("broker fact ID has a different immutable payload")
                if type(fact) is not BrokerOrderOpened:
                    paired = self.connection.execute(
                        """SELECT 1 FROM ledger_events cause
                           JOIN ledger_events release ON release.sequence=cause.sequence+1
                           WHERE cause.event_id=? AND release.event_type='RISK_RELEASED'
                             AND release.event_id=?""",
                        (fact.fact_id, f"risk-released:{fact.fact_id}"),
                    ).fetchone()
                    if paired is None:
                        raise ValueError("broker fact is missing its atomic risk release")
                self.connection.execute("COMMIT")
                return False
            order_row = self.connection.execute(
                "SELECT canonical_json FROM order_requests WHERE client_order_id=?",
                (fact.client_order_id,),
            ).fetchone()
            ack_rows = self.connection.execute(
                """SELECT sequence,payload_json FROM ledger_events
                   WHERE aggregate_id=? AND event_type='ACKNOWLEDGED'
                   ORDER BY sequence""",
                (fact.client_order_id,),
            ).fetchall()
            reservation = self.connection.execute(
                """SELECT reserved_cash_minor,reserved_exposure_minor,
                          reserved_sell_quantity,side
                   FROM risk_reservations WHERE client_order_id=?""",
                (fact.client_order_id,),
            ).fetchone()
            if order_row is None or len(ack_rows) != 1 or reservation is None:
                raise ValueError("broker lifecycle requires one acknowledged reserved order")
            order_payload = SQLiteLedger._strict_json(order_row[0])
            ack_payload = SQLiteLedger._strict_json(ack_rows[0][1])
            if not isinstance(order_payload, dict) or not isinstance(ack_payload, dict):
                raise ValueError("acknowledged order payload is malformed")
            broker_order_id = ack_payload.get("broker_order_id")
            if not isinstance(broker_order_id, str):
                raise ValueError("acknowledged broker_order_id is malformed")
            require_id(broker_order_id, "acknowledged broker_order_id")
            prior_facts = self._facts_for(self.connection, fact.client_order_id)
            if type(fact) is BrokerOrderOpened:
                if prior_facts:
                    raise ValueError("broker order OPEN is recorded exactly once")
                self._validate_open_against_submission(fact, order_payload, broker_order_id)
                for (existing_payload,) in self.connection.execute(
                    """SELECT payload_json FROM ledger_events
                       WHERE event_type='BROKER_ORDER_OPENED'"""
                ):
                    existing = broker_fact_from_payload(
                        SQLiteLedger._strict_json(existing_payload)
                    )
                    assert type(existing) is BrokerOrderOpened
                    if existing.broker_order_ref == fact.broker_order_ref:
                        raise ValueError("broker order reference is already bound")
            else:
                if not prior_facts:
                    raise ValueError("broker lifecycle must start with OPEN")
                opened = prior_facts[0]
                assert type(opened) is BrokerOrderOpened
                self._validate_open_against_submission(
                    opened, order_payload, broker_order_id
                )
                if type(fact) is BrokerFillObserved and self.connection.execute(
                    """SELECT 1 FROM ledger_events
                       WHERE event_type='BROKER_FILL_OBSERVED'
                         AND json_extract(payload_json,'$.broker_execution_id')=?""",
                    (fact.broker_execution_id,),
                ).fetchone():
                    raise ValueError("broker execution ID is already recorded")
            candidate = (*prior_facts, fact)
            fold_broker_order(candidate)
            expected_plan = self._lifecycle_release_plan(reservation, candidate)
            prior_releases = self.connection.execute(
                """SELECT event_id,payload_json FROM ledger_events
                   WHERE aggregate_id=? AND event_type='RISK_RELEASED'
                   ORDER BY sequence""",
                (fact.client_order_id,),
            ).fetchall()
            expected_prior = expected_plan[:-1] if type(fact) is not BrokerOrderOpened else ()
            if tuple(
                (row[0], SQLiteLedger._strict_json(row[1])) for row in prior_releases
            ) != tuple(
                (f"risk-released:{fact_id}", release)
                for fact_id, release in expected_prior
            ):
                raise ValueError("existing lifecycle releases do not match broker facts")
            self._insert_event(
                LedgerEvent(
                    fact.fact_id, event_type, fact.client_order_id,
                    fact.occurred_at, payload,
                ),
                recorded_at,
            )
            if type(fact) is not BrokerOrderOpened:
                _, release = expected_plan[-1]
                self._insert_event(
                    LedgerEvent(
                        f"risk-released:{fact.fact_id}", "RISK_RELEASED",
                        fact.client_order_id, fact.occurred_at, release,
                    ),
                    recorded_at,
                )
            self.connection.execute("COMMIT")
            return True
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    def broker_order_projection(
        self, client_order_id: str
    ) -> BrokerLifecycleProjection | None:
        require_id(client_order_id, "client_order_id")
        facts = self._facts_for(self.connection, client_order_id)
        return None if not facts else fold_broker_order(facts)

    def broker_lifecycle_facts(
        self,
        account_id: str,
        environment: TradingEnvironment,
    ) -> tuple[BrokerLifecycleFact, ...]:
        require_id(account_id, "account_id")
        if type(environment) is not TradingEnvironment:
            raise ValueError("environment must be TradingEnvironment")
        return tuple(
            broker_fact_from_payload(self._strict_json(payload))
            for (payload,) in self.connection.execute(
                f"""SELECT event.payload_json
                      FROM ledger_events event
                      JOIN risk_reservations reservation
                        ON reservation.client_order_id=event.aggregate_id
                     WHERE reservation.account_id=?
                       AND reservation.environment=?
                       AND event.event_type IN
                         ({','.join('?' for _ in BROKER_EVENT_TYPES)})
                     ORDER BY event.sequence""",
                (account_id, environment.value, *sorted(BROKER_EVENT_TYPES)),
            )
        )

    def reserve_submission(
        self,
        client_order_id: str,
        canonical_payload: Mapping[str, object],
        prepared_event: LedgerEvent,
        started_event: LedgerEvent,
        permit_id: str,
        reservation_terms: ReservationTerms,
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
        try:
            normalized_payload = self._strict_json(
                self._json(canonical_payload, canonical=True)
            )
            if not isinstance(normalized_payload, dict):
                raise ValueError("canonical order payload must be an object")
            for section, fields in (
                ("request", ("quantity",)),
                ("risk", ("original_quantity", "approved_quantity")),
                (
                    "intent",
                    (
                        "target_quantity", "current_quantity", "open_quantity",
                        "original_quantity",
                    ),
                ),
                ("plan", ("quantity",)),
            ):
                values = normalized_payload.get(section)
                if not isinstance(values, dict):
                    continue
                for field in fields:
                    value = values.get(field)
                    if value is None:
                        continue
                    if isinstance(value, bool):
                        raise ValueError("share quantity cannot be bool")
                    values[field] = canonical_share_quantity(
                        Decimal(str(value)), f"{section}.{field}"
                    )
            canonical_payload = normalized_payload
        except (InvalidOperation, TypeError, ValueError) as error:
            raise ValueError("canonical order share quantity is invalid") from error
        permit = canonical_payload.get("permit")
        if isinstance(permit, Mapping):
            canonical_permit_id = permit.get("permit_id")
        else:
            raise ValueError("canonical permit must be an object")
        require_id(permit_id, "permit_id")
        if canonical_permit_id != permit_id:
            raise ValueError("permit_id does not match the immutable order payload")
        if type(reservation_terms) is not ReservationTerms:
            raise ValueError("exact ReservationTerms are required")
        ReservationTerms.__post_init__(reservation_terms)
        try:
            request = canonical_payload["request"]
            risk = canonical_payload["risk"]
            if not isinstance(request, dict) or not isinstance(risk, dict):
                raise ValueError("immutable order payload sections must be objects")
            if (
                request["account_id"] != reservation_terms.account_id
                or request["instrument"] != {
                    "market": reservation_terms.instrument.market,
                    "symbol": reservation_terms.instrument.symbol,
                    "currency": reservation_terms.instrument.currency,
                }
                or request["side"] != reservation_terms.side.value
                or Decimal(request["quantity"]) != reservation_terms.quantity
                or request["quantity"] != str(reservation_terms.quantity)
                or risk["input_snapshot_id"] != reservation_terms.account_snapshot_id
                or risk["policy_version"] != reservation_terms.policy_version
                or canonical_payload["environment"] != reservation_terms.environment.value
            ):
                raise ValueError("reservation terms do not match immutable order payload")
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("reservation terms do not match immutable order payload") from error
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
                existing_terms = self.connection.execute(
                    """SELECT account_id, account_snapshot_id, environment, policy_version,
                              market, symbol, account_currency, instrument_currency, side,
                              quantity, available_cash_minor, current_exposure_minor,
                              current_position_quantity, cash_cap_minor, exposure_cap_minor,
                              fee_buffer_minor, reserved_cash_minor, reserved_exposure_minor,
                              reserved_sell_quantity
                       FROM risk_reservations WHERE client_order_id=?""",
                    (client_order_id,),
                ).fetchone()
                supplied_terms = (
                    reservation_terms.account_id, reservation_terms.account_snapshot_id,
                    reservation_terms.environment.value, reservation_terms.policy_version,
                    reservation_terms.instrument.market, reservation_terms.instrument.symbol,
                    reservation_terms.account_currency, reservation_terms.instrument_currency,
                    reservation_terms.side.value, reservation_terms.quantity,
                    reservation_terms.available_cash_minor,
                    reservation_terms.current_exposure_minor,
                    reservation_terms.current_position_quantity,
                    reservation_terms.cash_cap_minor, reservation_terms.exposure_cap_minor,
                    reservation_terms.fee_buffer_minor,
                    reservation_terms.reserved_cash_minor,
                    reservation_terms.reserved_exposure_minor,
                    reservation_terms.reserved_sell_quantity,
                )
                if existing_terms != supplied_terms:
                    raise OrderReservationConflict(
                        "client_order_id has different immutable reservation terms"
                    )
                self.connection.execute("COMMIT")
                return False
            cutoff = int(self.connection.execute(
                "SELECT value FROM schema_metadata WHERE key='risk_reservation_v9_order_cutoff'"
            ).fetchone()[0])
            legacy_blocker = self.connection.execute(
                """SELECT 1 FROM order_requests o
                   WHERE o.rowid <= ?
                     AND json_extract(o.canonical_json,'$.request.account_id') = ?
                     AND (json_type(o.canonical_json,'$.environment') IS NULL
                       OR json_extract(o.canonical_json,'$.environment') = ?)
                     AND NOT EXISTS (SELECT 1 FROM risk_reservations r
                       WHERE r.client_order_id=o.client_order_id)
                     AND COALESCE((SELECT e.event_type FROM ledger_events e
                       WHERE e.aggregate_id=o.client_order_id AND e.event_type IN
                         ('PREPARED','SUBMISSION_STARTED','ACKNOWLEDGED',
                          'SUBMISSION_REJECTED','SUBMITTED_UNKNOWN')
                       ORDER BY e.sequence DESC LIMIT 1),'PREPARED') != 'SUBMISSION_REJECTED'
                     AND NOT EXISTS (SELECT 1 FROM ledger_events resolved
                       WHERE resolved.aggregate_id=o.client_order_id
                         AND resolved.event_type='SUBMITTED_UNKNOWN_RESOLVED'
                         AND json_extract(resolved.payload_json,'$.result')='CONFIRMED_ABSENT')
                   LIMIT 1""",
                (
                    cutoff, reservation_terms.account_id,
                    reservation_terms.environment.value,
                ),
            ).fetchone()
            if legacy_blocker is not None:
                raise ReservationCapacityExceeded(
                    "legacy active order has no deterministic risk projection"
                )
            active = self.connection.execute(
                """SELECT
                     COALESCE(SUM(r.reserved_cash_minor - COALESCE(rel.cash,0)),0),
                     COALESCE(SUM(r.reserved_exposure_minor - COALESCE(rel.exposure,0)),0)
                   FROM risk_reservations r
                   LEFT JOIN (SELECT aggregate_id,
                     SUM(json_extract(payload_json,'$.reserved_cash_minor')) cash,
                     SUM(json_extract(payload_json,'$.reserved_exposure_minor')) exposure
                     FROM ledger_events WHERE event_type='RISK_RELEASED'
                     GROUP BY aggregate_id) rel ON rel.aggregate_id=r.client_order_id
                   WHERE r.account_id=? AND r.environment=?""",
                (
                    reservation_terms.account_id,
                    reservation_terms.environment.value,
                ),
            ).fetchone()
            active_sell = int(self.connection.execute(
                """SELECT COALESCE(SUM(r.reserved_sell_quantity - COALESCE(rel.qty,0)),0)
                   FROM risk_reservations r
                   LEFT JOIN (SELECT aggregate_id,
                     SUM(json_extract(payload_json,'$.reserved_sell_quantity')) qty
                     FROM ledger_events WHERE event_type='RISK_RELEASED'
                     GROUP BY aggregate_id) rel ON rel.aggregate_id=r.client_order_id
                   WHERE r.account_id=? AND r.environment=?
                     AND r.market=? AND r.symbol=?""",
                (
                    reservation_terms.account_id, reservation_terms.environment.value,
                    reservation_terms.instrument.market,
                    reservation_terms.instrument.symbol,
                ),
            ).fetchone()[0])
            cash_total = int(active[0]) + reservation_terms.reserved_cash_minor
            exposure_total = (
                reservation_terms.current_exposure_minor + int(active[1])
                + reservation_terms.reserved_exposure_minor
            )
            if (
                cash_total > reservation_terms.available_cash_minor
                or cash_total > reservation_terms.cash_cap_minor
                or exposure_total > reservation_terms.exposure_cap_minor
                or active_sell + reservation_terms.reserved_sell_quantity
                > reservation_terms.current_position_quantity
            ):
                raise ReservationCapacityExceeded("account risk reservation capacity exceeded")
            self.connection.execute(
                """INSERT INTO risk_reservations VALUES
                   (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    client_order_id, reservation_terms.account_id,
                    reservation_terms.account_snapshot_id,
                    reservation_terms.environment.value, reservation_terms.policy_version,
                    reservation_terms.instrument.market, reservation_terms.instrument.symbol,
                    reservation_terms.account_currency, reservation_terms.instrument_currency,
                    reservation_terms.side.value, reservation_terms.quantity,
                    reservation_terms.available_cash_minor,
                    reservation_terms.current_exposure_minor,
                    reservation_terms.current_position_quantity,
                    reservation_terms.cash_cap_minor, reservation_terms.exposure_cap_minor,
                    reservation_terms.fee_buffer_minor,
                    reservation_terms.reserved_cash_minor,
                    reservation_terms.reserved_exposure_minor,
                    reservation_terms.reserved_sell_quantity, recorded_at,
                ),
            )
            self._insert_event(
                LedgerEvent(
                    f"risk-reserved:{prepared_event.event_id}", "RISK_RESERVED",
                    client_order_id, prepared_event.occurred_at,
                    self._risk_payload(reservation_terms),
                ),
                recorded_at,
            )
            try:
                self.connection.execute(
                    "INSERT INTO order_requests VALUES (?, ?, ?)",
                    (client_order_id, canonical, recorded_at),
                )
            except sqlite3.IntegrityError as error:
                consumed = self.connection.execute(
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

    def complete_submission(self, terminal_event: LedgerEvent) -> None:
        require_id(terminal_event.event_id, "event_id")
        require_id(terminal_event.aggregate_id, "aggregate_id")
        require_utc(terminal_event.occurred_at, "occurred_at")
        if terminal_event.event_type not in {
            "ACKNOWLEDGED", "SUBMISSION_REJECTED", "SUBMITTED_UNKNOWN",
        }:
            raise ValueError("submission completion requires a terminal submission event")
        recorded_at = datetime.now(timezone.utc).isoformat()
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            self._insert_event(terminal_event, recorded_at)
            if terminal_event.event_type == "SUBMISSION_REJECTED":
                self._insert_full_release(
                    terminal_event.aggregate_id,
                    f"risk-released:{terminal_event.event_id}",
                    terminal_event.occurred_at,
                    recorded_at,
                )
            self.connection.execute("COMMIT")
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
            or event.event_type in {
                "PREPARED", "SUBMISSION_STARTED", "ACKNOWLEDGED",
                "SUBMISSION_REJECTED", "SUBMITTED_UNKNOWN",
                "RISK_RESERVED", "RISK_RELEASED",
            }
            or event.event_type in BROKER_EVENT_TYPES
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
    def canonical_command(
        command: OperatorCommand,
        cancellation: CancelOrderCommand | None = None,
    ) -> dict[str, object]:
        return canonical_operator_command(command, cancellation)

    def reserve_operator_command(
        self,
        command: OperatorCommand,
        event: LedgerEvent,
        cancellation: CancelOrderCommand | None = None,
    ) -> None:
        require_id(event.event_id, "event_id")
        require_utc(event.occurred_at, "occurred_at")
        canonical = self.canonical_command(command, cancellation)
        if (command.action is OperatorAction.ISSUE_CANCEL) != (cancellation is not None):
            raise ValueError("ISSUE_CANCEL requires an exact cancellation command")
        expected_payload = {**canonical, "previous_state": event.payload.get("previous_state")}
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
                (command.command_id, self._json(canonical, canonical=True), recorded_at),
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
            other_permit_action = action in {
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
                action is OperatorAction.ISSUE_CANCEL
                and outcome is OperatorCommandOutcome.SUCCEEDED
                and (
                    permit_id is None
                    or order_id is None
                    or not self._cancel_binding_is_valid(command_payload)
                    or order_id
                    != command_payload["cancel_order"]["target"]["broker_order_id"]
                )
            ) or (
                other_permit_action
                and outcome is OperatorCommandOutcome.SUCCEEDED
                and (permit_id is None or order_id is not None)
            ) or (
                (
                    action not in {
                        OperatorAction.ISSUE_CANCEL,
                        OperatorAction.ISSUE_REDUCE_ONLY,
                        OperatorAction.ISSUE_EMERGENCY_FLATTEN,
                    }
                    or outcome is OperatorCommandOutcome.FAILED
                )
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
        evidence: TypedUnknownResolutionEvidence,
        event: LedgerEvent,
    ) -> None:
        require_id(client_order_id, "client_order_id")
        require_id(event.event_id, "event_id")
        require_utc(event.occurred_at, "occurred_at")
        if type(evidence) not in TYPED_UNKNOWN_RESOLUTION_TYPES:
            raise TypeError("exact typed unknown-resolution evidence is required")
        if evidence.evidence.fetched_at > event.occurred_at:
            raise ValueError("unknown-resolution evidence cannot postdate its event")
        expected_payload = canonical_resolution_payload(command.command_id, evidence)
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
                order_payload = json.loads(order_row[0])
                order_account = order_payload["request"]["account_id"]
                order_environment = TradingEnvironment(order_payload["environment"])
            except (KeyError, TypeError, json.JSONDecodeError) as error:
                raise ValueError("order reservation has no account/environment scope") from error
            if (
                order_account != command.account_id
                or order_environment is not command.environment
                or order_environment is not evidence.evidence.environment
                or command_row[0] != self._json(self.canonical_command(command), canonical=True)
            ):
                raise ValueError(
                    "reserved order, operator command, and evidence scope do not match"
                )
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
            recorded_at = datetime.now(timezone.utc).isoformat()
            self._insert_event(event, recorded_at)
            if (
                type(evidence) is ConfirmedAbsent
                and self.connection.execute(
                    "SELECT 1 FROM risk_reservations WHERE client_order_id=?",
                    (client_order_id,),
                ).fetchone() is not None
            ):
                self._insert_full_release(
                    client_order_id, f"risk-released:{event.event_id}",
                    event.occurred_at, recorded_at,
                )
            self.connection.execute("COMMIT")
        except sqlite3.Error as error:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise LedgerPersistenceError("unknown resolution persistence failed") from error
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    def incomplete_submissions(
        self,
        account_id: str | None = None,
        environment: TradingEnvironment | None = None,
    ) -> tuple[str, ...]:
        if account_id is not None:
            require_id(account_id, "account_id")
        if environment is not None and type(environment) is not TradingEnvironment:
            raise ValueError("environment must be TradingEnvironment")
        account_join = ""
        account_filter = ""
        parameters: tuple[str, ...] = ()
        if account_id is not None or environment is not None:
            account_join = (
                " JOIN order_requests AS reserved"
                " ON reserved.client_order_id = event.aggregate_id"
            )
        if account_id is not None:
            account_filter = (
                " AND json_type(reserved.canonical_json, '$.request.account_id') = 'text'"
                " AND json_extract(reserved.canonical_json, '$.request.account_id') = ?"
            )
            parameters = (account_id,)
        if environment is not None:
            account_filter += (
                " AND json_type(reserved.canonical_json, '$.environment') = 'text'"
                " AND json_extract(reserved.canonical_json, '$.environment') = ?"
            )
            parameters += (environment.value,)
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
        self,
        account_id: str | None = None,
        environment: TradingEnvironment | None = None,
    ) -> tuple[str, ...]:
        if account_id is not None:
            require_id(account_id, "account_id")
        if environment is not None and type(environment) is not TradingEnvironment:
            raise ValueError("environment must be TradingEnvironment")
        account_join = ""
        account_filter = ""
        parameters: tuple[str, ...] = ()
        if account_id is not None or environment is not None:
            account_join = (
                " JOIN order_requests AS reserved"
                " ON reserved.client_order_id = unknown_event.aggregate_id"
            )
        if account_id is not None:
            account_filter = (
                " AND json_type(reserved.canonical_json, '$.request.account_id') = 'text'"
                " AND json_extract(reserved.canonical_json, '$.request.account_id') = ?"
            )
            parameters = (account_id,)
        if environment is not None:
            account_filter += (
                " AND json_type(reserved.canonical_json, '$.environment') = 'text'"
                " AND json_extract(reserved.canonical_json, '$.environment') = ?"
            )
            parameters += (environment.value,)
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

    def physical_integrity_check(self) -> bool:
        """Return whether SQLite's physical integrity check reports ``ok``."""
        try:
            return self.connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        except Exception:
            return False

    def foreign_key_check(self) -> bool:
        """Return whether SQLite reports no foreign-key violations."""
        try:
            return self.connection.execute("PRAGMA foreign_key_check").fetchone() is None
        except Exception:
            return False

    def schema_contract_check(self) -> bool:
        """Return whether the current database matches its declared schema contract."""
        try:
            self._validate_schema(self.connection, self.schema_version)
            return True
        except Exception:
            return False

    def audit_semantic_check(self) -> bool:
        """Return whether the current ledger history satisfies audit semantics."""
        try:
            version = self.schema_version
            if not 0 <= version <= SCHEMA_VERSION:
                return False
            self._validate_audit_semantics(self.connection, version=version)
            return True
        except Exception:
            return False

    def submission_state_check(self) -> bool:
        """Return whether submission events form a valid historical state machine."""
        try:
            self._validate_submission_states(self.connection)
            return True
        except Exception:
            return False

    def full_ledger_verify(self) -> bool:
        """Run every read-only ledger verification and require all to pass."""
        checks = (
            self.physical_integrity_check(),
            self.foreign_key_check(),
            self.schema_contract_check(),
            self.audit_semantic_check(),
            self.submission_state_check(),
        )
        return all(checks)

    def integrity_check(self) -> bool:
        """Backward-compatible alias for the physical SQLite check only."""
        return self.physical_integrity_check()

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

    def content_digest(self) -> str:
        return self._content_digest(self.connection)

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
            if type(declared_version) is not int or declared_version not in {
                6, 7, 8, 9, SCHEMA_VERSION,
            }:
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
