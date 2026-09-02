from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from trader.adapters.persistence.sqlite_ledger import SQLiteLedger
from trader.domain.models import BrokerExecutionState, TradingEnvironment
from trader.domain.performance import EvaluationStatus, IncompleteReasonCode
from trader.entrypoints.backtest import (
    BacktestInputError,
    CompletedBacktest,
    main,
    run_dry_backtest,
    validate_backtest_inputs,
)
from trader.ports.ledger import ReservationCapacityExceeded


UTC = timezone.utc
CODE_COMMIT = "e2bb483"


def _events(*, future_ask: str = "102") -> list[dict[str, object]]:
    return [
        {
            "event_id": "s1-e1",
            "session_id": "session-1",
            "occurred_at": "2026-08-31T13:31:00Z",
            "available_at": "2026-08-31T13:31:00Z",
            "source_sequence": 1,
            "ingest_sequence": 1,
            "bid": "99",
            "ask": "100",
            "available_quantity": 10,
            "halted": False,
        },
        {
            "event_id": "s1-e2",
            "session_id": "session-1",
            "occurred_at": "2026-08-31T13:35:00Z",
            "available_at": "2026-08-31T13:35:00Z",
            "source_sequence": 2,
            "ingest_sequence": 2,
            "bid": "100",
            "ask": "102",
            "available_quantity": 10,
            "halted": False,
        },
        {
            "event_id": "s2-e1",
            "session_id": "session-2",
            "occurred_at": "2026-09-01T13:31:00Z",
            "available_at": "2026-09-01T13:31:00Z",
            "source_sequence": 3,
            "ingest_sequence": 3,
            "bid": "101",
            "ask": "102",
            "available_quantity": 10,
            "halted": False,
        },
        {
            "event_id": "s2-e2",
            "session_id": "session-2",
            "occurred_at": "2026-09-01T13:32:00Z",
            "available_at": "2026-09-01T13:32:00Z",
            "source_sequence": 4,
            "ingest_sequence": 4,
            "bid": str(Decimal(future_ask) - Decimal(1)),
            "ask": future_ask,
            "available_quantity": 10,
            "halted": False,
        },
    ]


def _sessions() -> list[dict[str, object]]:
    return [
        {
            "session_id": "session-1",
            "business_date": "2026-08-31",
            "open_at": "2026-08-31T13:30:00Z",
            "close_at": "2026-08-31T14:00:00Z",
        },
        {
            "session_id": "session-2",
            "business_date": "2026-09-01",
            "open_at": "2026-09-01T13:30:00Z",
            "close_at": "2026-09-01T14:00:00Z",
        },
    ]


def _data_payload(
    *,
    events: list[dict[str, object]] | None = None,
    sessions: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "calendar_version": "calendar-v1",
        "provenance": {"source": "synthetic", "license": "test-only"},
        "sessions": _sessions() if sessions is None else sessions,
        "events": _events() if events is None else events,
    }


def _write_json(path: Path, payload: dict[str, object]) -> bytes:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    path.write_text(text, encoding="utf-8")
    return text.encode("utf-8")


def _write_inputs(
    directory: Path,
    *,
    events: list[dict[str, object]] | None = None,
    sessions: list[dict[str, object]] | None = None,
    cash: str = "1000",
    expected_data_sha256: str | None = None,
    fee_buffer_minor: int = 100,
    fee_bps: str = "10",
    partial_fill_cap: int | None = None,
    max_quote_age_seconds: int = 300,
    valuation_policy_version: str = "session-close-last-non-halted-bid-v1",
    valuation_max_mark_age_seconds: int = 3600,
    positions: list[dict[str, object]] | None = None,
) -> tuple[Path, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    data_path = directory / "data.json"
    data_raw = _write_json(data_path, _data_payload(events=events, sessions=sessions))
    data_digest = sha256(data_raw).hexdigest()
    config = {
        "schema_version": 2,
        "mode": "DRY",
        "data_sha256": data_digest if expected_data_sha256 is None else expected_data_sha256,
        "instrument": {"market": "NASDAQ", "symbol": "AAPL", "currency": "USD"},
        "account": {
            "account_id": "sim-account",
            "cash": cash,
            "positions": [] if positions is None else positions,
        },
        "strategy": {"version": "strategy-v1", "threshold": "100", "target_quantity": "2"},
        "risk": {
            "policy_version": "risk-v1",
            "max_order_quantity": "2",
            "cash_cap_minor": 100000,
            "exposure_cap_minor": 100000,
            "fee_buffer_minor": fee_buffer_minor,
        },
        "simulation": {
            "partial_fill_cap": partial_fill_cap,
            "slippage_bps": "0",
            "fee_bps": fee_bps,
            "max_quote_age_seconds": max_quote_age_seconds,
        },
        "valuation": {
            "policy_version": valuation_policy_version,
            "max_mark_age_seconds": valuation_max_mark_age_seconds,
        },
        "random_seed": 7,
    }
    config_path = directory / "config.json"
    _write_json(config_path, config)
    return config_path, data_path


class BacktestTests(unittest.TestCase):
    def _run(
        self,
        directory: Path,
        *,
        events: list[dict[str, object]] | None = None,
        valuation_max_mark_age_seconds: int = 3600,
    ) -> CompletedBacktest:
        config_path, data_path = _write_inputs(
            directory,
            events=events,
            valuation_max_mark_age_seconds=valuation_max_mark_age_seconds,
        )
        return run_dry_backtest(
            config_path, data_path, directory / "ledger.sqlite", code_commit=CODE_COMMIT
        )

    def test_validate_is_read_only_and_builds_simulated_run_spec(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            config_path, data_path = _write_inputs(directory)
            before = set(directory.iterdir())
            validated = validate_backtest_inputs(config_path, data_path, code_commit=CODE_COMMIT)
            self.assertEqual(set(directory.iterdir()), before)
            self.assertEqual(validated.config.accounting_seed.environment, TradingEnvironment.SIMULATED)
            self.assertEqual(validated.data.sha256, sha256(data_path.read_bytes()).hexdigest())
            self.assertEqual(validated.run_spec.data_snapshot_id, f"sha256-{validated.data.sha256}")
            self.assertEqual(len(validated.run_spec.source_sha256), 64)
            self.assertEqual(
                validated.run_spec.valuation_policy_version,
                "session-close-last-non-halted-bid-v1",
            )
            self.assertEqual(validated.run_spec.valuation_max_mark_age_seconds, 3600)
            self.assertEqual(len(validated.run_spec.fingerprint()), 64)

    def test_checked_in_synthetic_fixture_runs_complete(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            completed = run_dry_backtest(
                repository / "examples" / "dry" / "backtest.json",
                repository / "examples" / "dry" / "quotes.json",
                Path(temporary) / "ledger.sqlite",
                code_commit=CODE_COMMIT,
            )
        self.assertEqual(completed.result.status.value, "SUCCEEDED")
        self.assertEqual(
            completed.output.performance.evaluation_status,
            EvaluationStatus.COMPLETE,
        )
        self.assertEqual(completed.output.performance.evidence_use, "REFERENCE_ONLY")

    def test_run_fills_acknowledged_order_on_next_event_and_replays_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            completed = self._run(directory)
            output = completed.output
            self.assertEqual(len(output.decisions), 1)
            decision = output.decisions[0]
            self.assertEqual(decision.decision.signal, "LONG")
            self.assertEqual(decision.decision.input_snapshot_id, "s1-e2")
            self.assertEqual(decision.previous_close_mid, Decimal("101"))
            self.assertEqual(len(output.orders), 1)
            order = output.orders[0]
            self.assertEqual(order.execution_state, BrokerExecutionState.FILLED)
            self.assertEqual(order.filled_quantity, Decimal("2"))
            self.assertEqual(len(output.fills), 1)
            self.assertEqual(output.fills[0].occurred_at, datetime(2026, 9, 1, 13, 32, tzinfo=UTC))
            self.assertEqual(output.fills[0].price, Decimal("102"))
            self.assertEqual(output.fills[0].fee, Decimal("0.204"))
            self.assertEqual(output.accounting.cash, Decimal("795.796"))
            self.assertEqual(output.accounting.positions[0].quantity, Decimal("2"))
            self.assertEqual(output.accounting.gross_traded_value, Decimal("204"))
            self.assertEqual(output.accounting.total_fees, Decimal("0.204"))
            self.assertEqual(output.performance.evaluation_status, EvaluationStatus.COMPLETE)
            self.assertEqual(output.performance.evidence_use, "REFERENCE_ONLY")
            self.assertEqual(output.performance.ending_equity, Decimal("997.796"))
            self.assertEqual(output.performance.realized_pnl, Decimal(0))
            self.assertEqual(output.performance.unrealized_pnl, Decimal("-2.204"))
            self.assertEqual(output.performance.net_pnl, Decimal("-2.204"))
            sample_end = next(
                snapshot for snapshot in output.performance.snapshots if snapshot.is_sample_end
            )
            self.assertEqual(sample_end.positions[0].mark_price, Decimal("101"))
            self.assertEqual(
                tuple(snapshot.is_sample_end for snapshot in output.performance.snapshots),
                (False, True, False),
            )
            self.assertLess(
                output.performance.snapshots[1].checkpoint_at,
                output.performance.snapshots[2].checkpoint_at,
            )
            self.assertEqual(completed.result.output_sha256, output.fingerprint())
            ledger_sha256 = completed.result.ledger_sha256
            self.assertIsNotNone(ledger_sha256)
            ledger_path = directory / "ledger.sqlite"
            self.assertTrue(ledger_path.is_file())
            ledger = SQLiteLedger(ledger_path)
            try:
                self.assertTrue(ledger.full_ledger_verify())
                self.assertEqual(ledger.content_digest(), ledger_sha256)
                order_events = ledger.events_for(order.client_order_id)
            finally:
                ledger.close()
            event_types = tuple(event.event_type for event in order_events)
            self.assertIn("ACKNOWLEDGED", event_types)
            self.assertIn("BROKER_FILL_OBSERVED", event_types)
            acknowledged = next(event for event in order_events if event.event_type == "ACKNOWLEDGED")
            observed = next(
                event for event in order_events if event.event_type == "BROKER_FILL_OBSERVED"
            )
            self.assertEqual(acknowledged.occurred_at, datetime(2026, 9, 1, 13, 31, tzinfo=UTC))
            self.assertEqual(observed.occurred_at, datetime(2026, 9, 1, 13, 32, tzinfo=UTC))

    def test_fresh_runs_repeat_spec_output_and_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            first = self._run(directory / "first")
            second = self._run(directory / "second")
            self.assertEqual(first.run_spec.fingerprint(), second.run_spec.fingerprint())
            self.assertEqual(first.output.canonical_json(), second.output.canonical_json())
            self.assertEqual(first.output.fingerprint(), second.output.fingerprint())
            self.assertEqual(first.result.output_sha256, second.result.output_sha256)
            self.assertEqual(first.output.accounting, second.output.accounting)
            self.assertEqual(
                first.output.performance.canonical_json(),
                second.output.performance.canonical_json(),
            )
            self.assertNotEqual(first.result.run_id, second.result.run_id)

    def test_future_price_change_cannot_change_prior_decision(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            baseline = self._run(directory / "baseline")
            changed = self._run(directory / "changed", events=_events(future_ask="101"))
            baseline_decisions = json.loads(baseline.output.canonical_json())["decisions"]
            changed_decisions = json.loads(changed.output.canonical_json())["decisions"]
            baseline_payload = json.loads(baseline.output.canonical_json())
            changed_payload = json.loads(changed.output.canonical_json())
            self.assertEqual(baseline_decisions, changed_decisions)
            self.assertEqual(baseline_payload["orders"], changed_payload["orders"])
            self.assertEqual(
                baseline_payload["performance"]["snapshots"][0],
                changed_payload["performance"]["snapshots"][0],
            )
            self.assertEqual(baseline.output.orders[0].limit_price, Decimal("102"))
            self.assertEqual(changed.output.orders[0].limit_price, Decimal("102"))
            self.assertNotEqual(baseline.output.fills[0].price, changed.output.fills[0].price)
            self.assertNotEqual(baseline.output.canonical_json(), changed.output.canonical_json())

    def test_stale_mark_keeps_execution_success_and_open_position(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            completed = self._run(
                Path(temporary), valuation_max_mark_age_seconds=1
            )
            self.assertEqual(completed.result.status.value, "SUCCEEDED")
            self.assertEqual(
                completed.output.performance.evaluation_status,
                EvaluationStatus.INCOMPLETE,
            )
            self.assertIn(
                IncompleteReasonCode.STALE_MARK,
                {reason.code for reason in completed.output.performance.incomplete_reasons},
            )
            self.assertIsNone(completed.output.performance.cumulative_return)
            self.assertIsNone(
                completed.output.performance.maximum_session_close_drawdown
            )
            self.assertIsNone(completed.output.performance.gross_turnover)
            self.assertEqual(completed.output.accounting.positions[0].quantity, Decimal(2))
            self.assertEqual(len(completed.output.fills), 1)

    def test_missing_mark_keeps_execution_success_without_forced_liquidation(self) -> None:
        sessions = [
            *_sessions(),
            {
                "session_id": "session-3",
                "business_date": "2026-09-02",
                "open_at": "2026-09-02T13:30:00Z",
                "close_at": "2026-09-02T14:00:00Z",
            },
        ]
        events = [
            *_events(),
            {
                "event_id": "s3-halted",
                "session_id": "session-3",
                "occurred_at": "2026-09-02T13:31:00Z",
                "available_at": "2026-09-02T13:31:00Z",
                "source_sequence": 5,
                "ingest_sequence": 5,
                "bid": "100",
                "ask": "101",
                "available_quantity": 0,
                "halted": True,
            },
        ]
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            config_path, data_path = _write_inputs(
                directory, sessions=sessions, events=events
            )
            completed = run_dry_backtest(
                config_path,
                data_path,
                directory / "ledger.sqlite",
                code_commit=CODE_COMMIT,
            )
            self.assertEqual(completed.result.status.value, "SUCCEEDED")
            self.assertEqual(
                completed.output.performance.evaluation_status,
                EvaluationStatus.INCOMPLETE,
            )
            self.assertIn(
                IncompleteReasonCode.MISSING_MARK,
                {reason.code for reason in completed.output.performance.incomplete_reasons},
            )
            self.assertEqual(completed.output.accounting.positions[0].quantity, Decimal(2))
            self.assertEqual(len(completed.output.fills), 1)

    def test_nonmarketable_order_is_canceled_at_sample_end(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            completed = self._run(directory, events=_events(future_ask="103"))
            self.assertEqual(len(completed.output.orders), 1)
            order = completed.output.orders[0]
            self.assertEqual(order.execution_state, BrokerExecutionState.CANCELED)
            self.assertEqual(order.filled_quantity, Decimal(0))
            self.assertEqual(completed.output.fills, ())
            self.assertEqual(completed.output.accounting.cash, Decimal("1000"))
            self.assertEqual(completed.output.accounting.positions, ())
            ledger = SQLiteLedger(directory / "ledger.sqlite")
            try:
                event_types = tuple(
                    event.event_type for event in ledger.events_for(order.client_order_id)
                )
            finally:
                ledger.close()
            self.assertIn("BROKER_ORDER_CANCELED", event_types)

    def test_partial_fill_then_sample_end_cancel_replays_terminal_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            config_path, data_path = _write_inputs(directory, partial_fill_cap=1)
            completed = run_dry_backtest(
                config_path,
                data_path,
                directory / "ledger.sqlite",
                code_commit=CODE_COMMIT,
            )
            order = completed.output.orders[0]
            self.assertEqual(order.execution_state, BrokerExecutionState.CANCELED)
            self.assertEqual(order.filled_quantity, Decimal(1))
            self.assertEqual(len(completed.output.fills), 1)
            self.assertEqual(completed.output.accounting.cash, Decimal("897.898"))
            self.assertEqual(completed.output.accounting.positions[0].quantity, Decimal(1))
            self.assertEqual(completed.output.accounting.gross_traded_value, Decimal(102))
            self.assertEqual(completed.output.accounting.total_fees, Decimal("0.102"))
            ledger = SQLiteLedger(directory / "ledger.sqlite")
            try:
                self.assertTrue(ledger.full_ledger_verify())
                event_types = tuple(
                    event.event_type for event in ledger.events_for(order.client_order_id)
                )
            finally:
                ledger.close()
            self.assertLess(
                event_types.index("BROKER_FILL_OBSERVED"),
                event_types.index("BROKER_ORDER_CANCELED"),
            )

    def test_day_order_expires_on_next_business_session(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            sessions = [
                *_sessions(),
                {
                    "session_id": "session-3",
                    "business_date": "2026-09-02",
                    "open_at": "2026-09-02T13:30:00Z",
                    "close_at": "2026-09-02T14:00:00Z",
                },
            ]
            events = [
                *_events()[:3],
                {
                    "event_id": "s3-e1",
                    "session_id": "session-3",
                    "occurred_at": "2026-09-02T13:31:00Z",
                    "available_at": "2026-09-02T13:31:00Z",
                    "source_sequence": 4,
                    "ingest_sequence": 4,
                    "bid": "102",
                    "ask": "103",
                    "available_quantity": 10,
                    "halted": False,
                },
            ]
            config_path, data_path = _write_inputs(
                directory, events=events, sessions=sessions
            )
            completed = run_dry_backtest(
                config_path,
                data_path,
                directory / "ledger.sqlite",
                code_commit=CODE_COMMIT,
            )
            self.assertEqual(
                tuple(order.execution_state for order in completed.output.orders),
                (BrokerExecutionState.EXPIRED, BrokerExecutionState.CANCELED),
            )
            self.assertEqual(completed.output.fills, ())
            self.assertEqual(completed.output.accounting.cash, Decimal(1000))
            ledger = SQLiteLedger(directory / "ledger.sqlite")
            try:
                self.assertTrue(ledger.full_ledger_verify())
                first_order_events = tuple(
                    event.event_type
                    for event in ledger.events_for(
                        completed.output.orders[0].client_order_id
                    )
                )
            finally:
                ledger.close()
            self.assertIn("BROKER_ORDER_EXPIRED", first_order_events)

    def test_invalid_inputs_and_insufficient_cash_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            config_path, data_path = _write_inputs(directory, expected_data_sha256="0" * 64)
            with self.assertRaises(BacktestInputError):
                validate_backtest_inputs(config_path, data_path, code_commit=CODE_COMMIT)

            policy_directory = directory / "valuation-policy"
            policy_config, policy_data = _write_inputs(
                policy_directory, valuation_policy_version="unsupported-v1"
            )
            with self.assertRaisesRegex(BacktestInputError, "policy_version is unsupported"):
                validate_backtest_inputs(policy_config, policy_data, code_commit=CODE_COMMIT)

            position_directory = directory / "starting-position"
            position_config, position_data = _write_inputs(
                position_directory,
                positions=[
                    {
                        "instrument": {
                            "market": "NASDAQ",
                            "symbol": "AAPL",
                            "currency": "USD",
                        },
                        "quantity": "1",
                    }
                ],
            )
            with self.assertRaisesRegex(BacktestInputError, "zero starting positions"):
                validate_backtest_inputs(position_config, position_data, code_commit=CODE_COMMIT)

            duplicate_config = directory / "duplicate-config.json"
            duplicate_config.write_text(
                '{"schema_version":2,"mode":"DRY","data_sha256":"'
                + sha256(data_path.read_bytes()).hexdigest()
                + '","instrument":{"market":"NASDAQ","symbol":"AAPL","currency":"USD"},'
                '"account":{"account_id":"sim-account","cash":"1000","positions":[]},'
                '"strategy":{"version":"strategy-v1","threshold":"100","threshold":"101",'
                '"target_quantity":"2"},"risk":{"policy_version":"risk-v1",'
                '"max_order_quantity":"2","cash_cap_minor":100000,"exposure_cap_minor":100000,'
                '"fee_buffer_minor":100},"simulation":{"partial_fill_cap":null,"slippage_bps":"0",'
                '"fee_bps":"10","max_quote_age_seconds":300},"valuation":{'
                '"policy_version":"session-close-last-non-halted-bid-v1",'
                '"max_mark_age_seconds":3600},"random_seed":7}',
                encoding="utf-8",
            )
            with self.assertRaises(BacktestInputError):
                validate_backtest_inputs(duplicate_config, data_path, code_commit=CODE_COMMIT)

            fee_directory = directory / "fee-buffer"
            fee_config, fee_data = _write_inputs(fee_directory, fee_buffer_minor=0)
            with self.assertRaisesRegex(BacktestInputError, "fee_buffer_minor"):
                validate_backtest_inputs(fee_config, fee_data, code_commit=CODE_COMMIT)
            self.assertFalse((fee_directory / "ledger.sqlite").exists())

            age_directory = directory / "quote-age"
            age_config, age_data = _write_inputs(
                age_directory, max_quote_age_seconds=10**30
            )
            with self.assertRaisesRegex(BacktestInputError, "supported duration"):
                validate_backtest_inputs(age_config, age_data, code_commit=CODE_COMMIT)
            self.assertFalse((age_directory / "ledger.sqlite").exists())

            for name, mutate in (
                ("reversed", lambda items: list(reversed(items))),
                ("duplicate-sequence", lambda items: [items[0], {**items[1], "source_sequence": 1, "ingest_sequence": 1}, *items[2:]]),
                ("outside-session", lambda items: [*items[:3], {**items[3], "occurred_at": "2026-09-01T14:00:00Z", "available_at": "2026-09-01T14:00:00Z"}]),
            ):
                with self.subTest(name=name):
                    invalid_directory = directory / name
                    invalid_directory.mkdir()
                    invalid_config, invalid_data = _write_inputs(
                        invalid_directory, events=mutate(_events())
                    )
                    with self.assertRaises(BacktestInputError):
                        validate_backtest_inputs(invalid_config, invalid_data, code_commit=CODE_COMMIT)

            poor_directory = directory / "poor"
            poor_directory.mkdir()
            poor_config, poor_data = _write_inputs(poor_directory, cash="100")
            with self.assertRaises(ReservationCapacityExceeded):
                run_dry_backtest(poor_config, poor_data, poor_directory / "ledger.sqlite", code_commit=CODE_COMMIT)

    def test_cli_validates_runs_writes_result_exclusively_and_refuses_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            config_path, data_path = _write_inputs(directory)
            ledger_path = directory / "cli-ledger.sqlite"
            result_path = directory / "result.json"
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                self.assertEqual(
                    main(
                        [
                            "validate",
                            "--config",
                            str(config_path),
                            "--data",
                            str(data_path),
                            "--code-commit",
                            CODE_COMMIT,
                        ]
                    ),
                    0,
                )
            self.assertEqual(json.loads(stdout.getvalue())["quote_count"], 4)
            with redirect_stdout(io.StringIO()):
                self.assertEqual(
                    main(
                        [
                            "run",
                            "--config",
                            str(config_path),
                            "--data",
                            str(data_path),
                            "--ledger",
                            str(ledger_path),
                            "--result",
                            str(result_path),
                            "--code-commit",
                            CODE_COMMIT,
                        ]
                    ),
                    0,
                )
            original = result_path.read_text(encoding="utf-8")
            self.assertEqual(json.loads(original)["schema_version"], 1)
            second_ledger = directory / "cli-ledger-2.sqlite"
            stderr = io.StringIO()
            with redirect_stderr(stderr), redirect_stdout(io.StringIO()):
                status = main(
                    [
                        "run",
                        "--config",
                        str(config_path),
                        "--data",
                        str(data_path),
                        "--ledger",
                        str(second_ledger),
                        "--result",
                        str(result_path),
                        "--code-commit",
                        CODE_COMMIT,
                    ]
                )
            self.assertEqual(status, 2)
            self.assertIn("result path already exists", stderr.getvalue())
            self.assertEqual(result_path.read_text(encoding="utf-8"), original)
            self.assertFalse(second_ledger.exists())

            alias_ledger = directory / "alias-ledger.sqlite"
            alias_result = Path(f"{alias_ledger}-wal")
            stderr = io.StringIO()
            with redirect_stderr(stderr), redirect_stdout(io.StringIO()):
                status = main(
                    [
                        "run",
                        "--config",
                        str(config_path),
                        "--data",
                        str(data_path),
                        "--ledger",
                        str(alias_ledger),
                        "--result",
                        str(alias_result),
                        "--code-commit",
                        CODE_COMMIT,
                    ]
                )
            self.assertEqual(status, 2)
            self.assertIn("cannot alias", stderr.getvalue())
            self.assertFalse(alias_ledger.exists())
            self.assertFalse(alias_result.exists())

    def test_cli_execution_failure_writes_terminal_failed_result(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            config_path, data_path = _write_inputs(directory, cash="100")
            ledger_path = directory / "failed-ledger.sqlite"
            result_path = directory / "failed-result.json"
            stderr = io.StringIO()
            with redirect_stderr(stderr), redirect_stdout(io.StringIO()):
                status = main(
                    [
                        "run",
                        "--config",
                        str(config_path),
                        "--data",
                        str(data_path),
                        "--ledger",
                        str(ledger_path),
                        "--result",
                        str(result_path),
                        "--code-commit",
                        CODE_COMMIT,
                    ]
                )
            self.assertEqual(status, 2)
            self.assertIn("ReservationCapacityExceeded", stderr.getvalue())
            payload = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], 1)
            self.assertEqual(payload["result"]["status"], "FAILED")
            self.assertEqual(
                payload["result"]["failure_code"], "BACKTEST_EXECUTION_FAILED"
            )
            self.assertIsNone(payload["result"]["output_sha256"])
            self.assertIsNone(payload["output"])
            self.assertEqual(
                payload["result"]["run_spec_fingerprint"],
                sha256(
                    json.dumps(
                        payload["run_spec"],
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest(),
            )
            ledger = SQLiteLedger(ledger_path)
            try:
                self.assertTrue(ledger.full_ledger_verify())
                self.assertEqual(
                    payload["result"]["ledger_sha256"], ledger.content_digest()
                )
            finally:
                ledger.close()

    def test_existing_ledger_is_not_attributed_to_failed_run(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            config_path, data_path = _write_inputs(directory)
            ledger_path = directory / "existing-ledger.sqlite"
            existing = SQLiteLedger(ledger_path)
            try:
                existing_digest = existing.content_digest()
            finally:
                existing.close()
            result_path = directory / "failed-result.json"
            stderr = io.StringIO()
            with redirect_stderr(stderr), redirect_stdout(io.StringIO()):
                status = main(
                    [
                        "run",
                        "--config",
                        str(config_path),
                        "--data",
                        str(data_path),
                        "--ledger",
                        str(ledger_path),
                        "--result",
                        str(result_path),
                        "--code-commit",
                        CODE_COMMIT,
                    ]
                )
            self.assertEqual(status, 2)
            self.assertIn("ledger path already exists", stderr.getvalue())
            payload = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["result"]["status"], "FAILED")
            self.assertIsNone(payload["result"]["ledger_sha256"])
            reopened = SQLiteLedger(ledger_path)
            try:
                self.assertEqual(reopened.content_digest(), existing_digest)
            finally:
                reopened.close()

    def test_result_publish_failure_leaves_no_partial_json(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            config_path, data_path = _write_inputs(directory)
            ledger_path = directory / "ledger.sqlite"
            result_path = directory / "result.json"
            stderr = io.StringIO()
            with patch(
                "trader.entrypoints.backtest.os.link",
                side_effect=OSError("injected publish failure"),
            ), redirect_stderr(stderr), redirect_stdout(io.StringIO()):
                status = main(
                    [
                        "run",
                        "--config",
                        str(config_path),
                        "--data",
                        str(data_path),
                        "--ledger",
                        str(ledger_path),
                        "--result",
                        str(result_path),
                        "--code-commit",
                        CODE_COMMIT,
                    ]
                )
            self.assertEqual(status, 2)
            self.assertIn("result could not be written exclusively", stderr.getvalue())
            self.assertIn("failure result could not be written", stderr.getvalue())
            self.assertFalse(result_path.exists())
            self.assertEqual(tuple(directory.glob(f".{result_path.name}.*.tmp")), ())

    def test_repository_launcher_cannot_import_live_paths_or_open_network(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            config_path, data_path = _write_inputs(directory)
            guard = (
                "import runpy\n"
                "import socket\n"
                "import sys\n"
                "class _NoLiveImports:\n"
                "    def find_spec(self, fullname, path=None, target=None):\n"
                "        blocked = ('trader.adapters.kiwoom', "
                "'trader.entrypoints.runtime', 'trader.ports.http')\n"
                "        if fullname.startswith(blocked):\n"
                "            raise RuntimeError('live module import blocked')\n"
                "        return None\n"
                "def _no_network(*args, **kwargs):\n"
                "    raise RuntimeError('network access blocked')\n"
                "sys.meta_path.insert(0, _NoLiveImports())\n"
                "socket.socket = _no_network\n"
                "socket.create_connection = _no_network\n"
                "script = sys.argv[1]\n"
                "sys.argv = [script, *sys.argv[2:]]\n"
                "runpy.run_path(script, run_name='__main__')\n"
            )
            repository = Path(__file__).resolve().parents[1]
            ledger_path = directory / "launcher-ledger.sqlite"
            result_path = directory / "launcher-result.json"
            process = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    guard,
                    str(repository / "scripts" / "backtest.py"),
                    "run",
                    "--config",
                    str(config_path),
                    "--data",
                    str(data_path),
                    "--ledger",
                    str(ledger_path),
                    "--result",
                    str(result_path),
                    "--code-commit",
                    CODE_COMMIT,
                ],
                cwd=repository,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            self.assertEqual(process.returncode, 0, process.stderr)
            payload = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["result"]["status"], "SUCCEEDED")
            self.assertEqual(payload["run_spec"]["code_commit"], CODE_COMMIT)


if __name__ == "__main__":
    unittest.main()
