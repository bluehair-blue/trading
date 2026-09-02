from __future__ import annotations

import argparse
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import json
import os
from pathlib import Path
import sys
import tempfile
from uuid import uuid4

from trader.adapters.persistence.sqlite_ledger import SQLiteLedger
from trader.adapters.simulated.clock import VirtualClock
from trader.adapters.simulated.simulated_broker import (
    QuoteEvent,
    SimulatedBroker,
    SimulationReason,
    SimulationResult,
)
from trader.application.backtest import (
    BacktestDecisionRecord,
    BacktestFillRecord,
    BacktestOrderRecord,
    BacktestOutput,
    evaluate_previous_close_threshold,
)
from trader.application.broker_lifecycle import BrokerLifecycleService
from trader.application.execution import OrderCoordinator
from trader.application.operator import OperatorCommandService
from trader.application.safety import SafetyController
from trader.domain.accounting import (
    AccountingProjection,
    AccountingSeed,
    fold_accounting,
)
from trader.domain.broker_lifecycle import (
    BrokerFillObserved,
    BrokerLifecycleFact,
    BrokerOrderOpened,
)
from trader.domain.models import (
    AccountSnapshot,
    BrokerExecutionState,
    ExecutionPlan,
    InstrumentId,
    MarketEvidence,
    ObservedAmount,
    OperatorAction,
    OperatorCommand,
    OrderRequest,
    OrderType,
    PermitScope,
    ReservationAccountState,
    ReservationPosition,
    RiskOutcome,
    RiskReservationPolicy,
    SafetyState,
    Side,
    SnapshotQuality,
    SubmissionState,
    TimeInForce,
    TradeIntent,
    TradingEnvironment,
)
from trader.domain.risk import pre_trade_quantity_cap
from trader.research.backtest_input import (
    BacktestConfiguration as BacktestConfiguration,
    BacktestData as BacktestData,
    BacktestInputError as BacktestInputError,
    HistoricalQuote as HistoricalQuote,
    TradingSession as TradingSession,
    ValidatedBacktest as ValidatedBacktest,
    validate_backtest_inputs as validate_backtest_inputs,
)
from trader.research.manifest import RunResult, RunSpec, RunStatus


_DEPLOYMENT_VERSION = "dry-backtest-v1"
_STRATEGY_ID = "reference-previous-close-threshold"
_CLOCK_SESSION_PREFIX = "dry-clock"


class BacktestExecutionError(RuntimeError):
    """The simulated execution could not produce a complete auditable result."""


@dataclass(frozen=True)
class CompletedBacktest:
    run_spec: RunSpec
    result: RunResult
    output: BacktestOutput

    def canonical_json(self) -> str:
        return _run_envelope_json(self.run_spec, self.result, self.output)


def _run_envelope_json(
    run_spec: RunSpec,
    result: RunResult,
    output: BacktestOutput | None,
) -> str:
    payload = {
        "schema_version": 1,
        "run_spec": json.loads(run_spec.canonical_json()),
        "result": json.loads(result.canonical_json()),
        "output": None if output is None else json.loads(output.canonical_json()),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _minor_floor(value: Decimal, name: str) -> int:
    if not value.is_finite() or value < 0:
        raise BacktestExecutionError(f"{name} is not non-negative finite money")
    result = int((value * 100).to_integral_value(rounding=ROUND_FLOOR))
    if result > (1 << 63) - 1:
        raise BacktestExecutionError(f"{name} exceeds signed 64-bit minor units")
    return result


def _minor_ceiling(value: Decimal, name: str) -> int:
    if not value.is_finite() or value < 0:
        raise BacktestExecutionError(f"{name} is not non-negative finite money")
    result = int((value * 100).to_integral_value(rounding=ROUND_CEILING))
    if result > (1 << 63) - 1:
        raise BacktestExecutionError(f"{name} exceeds signed 64-bit minor units")
    return result


def _current_quantity(accounting: AccountingProjection, instrument: InstrumentId) -> Decimal:
    return next(
        (
            position.quantity
            for position in accounting.positions
            if position.instrument == instrument
        ),
        Decimal(0),
    )


def _operator_command(
    *,
    safety: SafetyController,
    action: OperatorAction,
    command_id: str,
    account_id: str,
    now: datetime,
) -> OperatorCommand:
    return OperatorCommand(
        command_id,
        "dry-runner",
        "offline deterministic backtest lifecycle",
        _DEPLOYMENT_VERSION,
        safety.epoch,
        now,
        now + timedelta(seconds=20),
        action,
        account_id,
        TradingEnvironment.SIMULATED,
    )


def _refresh_safety(
    *,
    safety: SafetyController,
    operator: OperatorCommandService,
    account: AccountingProjection,
    config: BacktestConfiguration,
    event: HistoricalQuote,
    command_prefix: str,
) -> tuple[AccountSnapshot, MarketEvidence]:
    now = event.available_at
    account_id = config.accounting_seed.account_id
    if safety.state is SafetyState.BOOTSTRAPPING:
        operator.acknowledge_startup_recovery(
            _operator_command(
                safety=safety,
                action=OperatorAction.ACKNOWLEDGE_STARTUP_RECOVERY,
                command_id=f"{command_prefix}-recover",
                account_id=account_id,
                now=now,
            )
        )
    elif safety.state in {SafetyState.READY, SafetyState.TRADING}:
        operator.halt(
            _operator_command(
                safety=safety,
                action=OperatorAction.HALT,
                command_id=f"{command_prefix}-halt",
                account_id=account_id,
                now=now,
            )
        )
        operator.begin_reconciliation(
            _operator_command(
                safety=safety,
                action=OperatorAction.BEGIN_RECONCILIATION,
                command_id=f"{command_prefix}-reconcile",
                account_id=account_id,
                now=now,
            )
        )
    elif safety.state is SafetyState.HALTED:
        operator.begin_reconciliation(
            _operator_command(
                safety=safety,
                action=OperatorAction.BEGIN_RECONCILIATION,
                command_id=f"{command_prefix}-reconcile",
                account_id=account_id,
                now=now,
            )
        )
    if safety.state is not SafetyState.RECONCILING:
        raise BacktestExecutionError("safety could not enter reconciliation")

    quantity = _current_quantity(account, config.instrument)
    observation_id = f"{command_prefix}-account-observation"
    observations = (
        ObservedAmount(account.cash, observation_id, now),
        ObservedAmount(account.cash, observation_id, now),
        ObservedAmount(quantity, observation_id, now),
        ObservedAmount(Decimal(0), observation_id, now),
        ObservedAmount(account.total_fees, observation_id, now),
        ObservedAmount(Decimal(1), observation_id, now),
    )
    snapshot = AccountSnapshot(
        f"{command_prefix}-account-snapshot",
        account_id,
        TradingEnvironment.SIMULATED,
        SnapshotQuality.CONSISTENT,
        *observations,
        now,
    )
    market = MarketEvidence(
        f"{command_prefix}-market-snapshot",
        TradingEnvironment.SIMULATED,
        SnapshotQuality.CONSISTENT,
        now,
        config.risk_policy_version,
        event.bid,
        event.ask,
    )
    positions = (
        ()
        if quantity == 0
        else (ReservationPosition(config.instrument, int(quantity)),)
    )
    reservation_state = ReservationAccountState(
        account_id,
        snapshot.snapshot_id,
        config.instrument.currency,
        _minor_floor(account.cash, "available cash"),
        _minor_ceiling(quantity * event.ask, "current exposure"),
        positions,
    )
    reservation_policy = RiskReservationPolicy(
        config.risk_policy_version,
        config.cash_cap_minor,
        config.exposure_cap_minor,
        config.fee_buffer_minor,
    )
    safety.complete_reconciliation(
        snapshot,
        market,
        reservation_state,
        reservation_policy,
        config.risk_policy_version,
        _DEPLOYMENT_VERSION,
        now,
    )
    operator.arm(
        _operator_command(
            safety=safety,
            action=OperatorAction.ARM,
            command_id=f"{command_prefix}-arm",
            account_id=account_id,
            now=now,
        )
    )
    return snapshot, market


def _record_facts(
    service: BrokerLifecycleService,
    facts: Iterable[BrokerLifecycleFact],
) -> None:
    grouped: dict[str, list[BrokerLifecycleFact]] = {}
    for fact in facts:
        grouped.setdefault(fact.client_order_id, []).append(fact)
    for grouped_facts in grouped.values():
        service.record(tuple(grouped_facts))


def _broker_order_id(ledger: SQLiteLedger, client_order_id: str) -> str:
    for event in ledger.events_for(client_order_id):
        if event.event_type == SubmissionState.ACKNOWLEDGED.value:
            value = event.payload.get("broker_order_id")
            if type(value) is str and value:
                return value
    raise BacktestExecutionError("acknowledged order has no persisted broker order ID")


def _has_active_order(broker: SimulatedBroker, broker_order_ids: Sequence[str]) -> bool:
    return any(
        broker.order(order_id).execution_state
        in {BrokerExecutionState.OPEN, BrokerExecutionState.PARTIALLY_FILLED}
        for order_id in broker_order_ids
    )


def _require_quote_result(result: SimulationResult) -> None:
    allowed = {
        SimulationReason.FILLED,
        SimulationReason.NOT_MARKETABLE,
        SimulationReason.LATENCY,
        SimulationReason.HALTED,
        SimulationReason.NO_ACTIVE_ORDER,
        SimulationReason.DAY_EXPIRED,
    }
    if result.reason not in allowed:
        raise BacktestExecutionError(f"simulator rejected historical event: {result.reason.value}")


def _projection_from_ledger(
    ledger: SQLiteLedger,
    seed: AccountingSeed,
) -> tuple[tuple[BrokerLifecycleFact, ...], AccountingProjection]:
    facts = BrokerLifecycleService(ledger).facts_for(
        seed.account_id, TradingEnvironment.SIMULATED
    )
    return facts, fold_accounting(seed, facts)


def run_dry_backtest(
    config_path: str | Path,
    data_path: str | Path,
    ledger_path: str | Path,
    *,
    code_commit: str,
) -> CompletedBacktest:
    """Run one offline SIMULATED replay; no credential or network adapter is reachable."""
    validated = validate_backtest_inputs(config_path, data_path, code_commit=code_commit)
    return _run_validated_backtest(validated, ledger_path)


def _run_validated_backtest(
    validated: ValidatedBacktest,
    ledger_path: str | Path,
) -> CompletedBacktest:
    config = validated.config
    data = validated.data
    run_spec = validated.run_spec
    target_path = Path(ledger_path)
    ledger_artifacts = (
        target_path,
        Path(f"{target_path}-wal"),
        Path(f"{target_path}-shm"),
        Path(f"{target_path}-journal"),
    )
    if any(path.exists() or path.is_symlink() for path in ledger_artifacts):
        raise BacktestInputError("ledger path already exists; a Dry run requires a fresh database")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(target_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as error:
        raise BacktestInputError(
            "ledger path already exists; a Dry run requires a fresh database"
        ) from error
    except OSError as error:
        raise BacktestExecutionError("ledger path could not be claimed exclusively") from error
    os.close(descriptor)
    started_at = datetime.now(timezone.utc)

    clock = VirtualClock(data.events[0].available_at)
    broker = SimulatedBroker(
        clock=clock.wall,
        business_date=data.business_date,
        known_symbols={config.instrument.symbol},
        partial_fill_cap=config.partial_fill_cap,
        slippage_bps=config.slippage_bps,
        fee_bps=config.fee_bps,
        max_quote_age=timedelta(seconds=config.max_quote_age_seconds),
    )
    ledger = SQLiteLedger(target_path)
    try:
        safety = SafetyController(
            TradingEnvironment.SIMULATED, monotonic_clock=clock.monotonic
        )
        operator = OperatorCommandService(
            ledger,
            safety,
            _DEPLOYMENT_VERSION,
            clock.wall,
            account_id=config.accounting_seed.account_id,
        )
        clock_session_id = f"{_CLOCK_SESSION_PREFIX}-{run_spec.fingerprint()[:20]}"
        coordinator = OrderCoordinator(
            broker,
            ledger,
            safety,
            clock.wall,
            account_id=config.accounting_seed.account_id,
            monotonic_clock=clock.monotonic,
            clock_session_id=clock_session_id,
        )
        lifecycle = BrokerLifecycleService(ledger)
        accounting = fold_accounting(config.accounting_seed, ())
    except BaseException:
        ledger.close()
        raise
    decisions: list[BacktestDecisionRecord] = []
    broker_order_ids: list[str] = []
    broker_to_client: dict[str, str] = {}
    client_to_side: dict[str, Side] = {}
    client_to_limit: dict[str, Decimal] = {}
    previous_session_mid: Decimal | None = None
    previous_session_event_id: str | None = None
    active_session_id: str | None = None
    current_session_mid: Decimal | None = None
    current_session_event_id: str | None = None
    decision_made_for_session = False

    try:
        for event in data.events:
            if event.session_id != active_session_id:
                previous_session_mid = current_session_mid
                previous_session_event_id = current_session_event_id
                active_session_id = event.session_id
                current_session_mid = None
                current_session_event_id = None
                decision_made_for_session = False

            clock.advance_to(event.available_at)
            quote = QuoteEvent(
                config.instrument.symbol,
                event.bid,
                event.ask,
                event.available_quantity,
                event.occurred_at,
                event.source_sequence,
                event.halted,
            )
            simulation = broker.on_quote(quote)
            _require_quote_result(simulation)
            _record_facts(lifecycle, simulation.facts)
            _, accounting = _projection_from_ledger(ledger, config.accounting_seed)

            if event.halted:
                continue
            current_session_mid = event.mid
            current_session_event_id = event.event_id
            if (
                decision_made_for_session
                or previous_session_mid is None
                or previous_session_event_id is None
            ):
                continue
            decision_made_for_session = True
            prefix = f"dry-{run_spec.fingerprint()[:12]}-{event.ingest_sequence}"
            decision = evaluate_previous_close_threshold(
                config.strategy,
                strategy_id=_STRATEGY_ID,
                decision_id=f"{prefix}-decision",
                target_id=f"{prefix}-target",
                input_snapshot_id=previous_session_event_id,
                instrument=config.instrument,
                previous_close_mid=previous_session_mid,
                decided_at=event.available_at,
            )
            decisions.append(decision)
            current_quantity = _current_quantity(accounting, config.instrument)
            target_quantity = decision.target.quantity
            if target_quantity == current_quantity or _has_active_order(
                broker, broker_order_ids
            ):
                continue

            snapshot, market = _refresh_safety(
                safety=safety,
                operator=operator,
                account=accounting,
                config=config,
                event=event,
                command_prefix=prefix,
            )
            delta = target_quantity - current_quantity
            intent = TradeIntent(
                f"{prefix}-intent",
                decision.target.target_id,
                decision.target.strategy_id,
                decision.decision.decision_id,
                decision.decision.strategy_version,
                decision.decision.input_snapshot_id,
                config.accounting_seed.account_id,
                snapshot.snapshot_id,
                config.instrument,
                target_quantity,
                current_quantity,
                Decimal(0),
                delta,
                event.available_at,
            )
            risk = pre_trade_quantity_cap(
                f"{prefix}-risk",
                config.risk_policy_version,
                snapshot.snapshot_id,
                intent,
                config.max_order_quantity,
                event.available_at,
            )
            if risk.outcome is RiskOutcome.REJECTED or risk.approved_quantity is None:
                continue
            approved = risk.approved_quantity
            side = Side.BUY if approved > 0 else Side.SELL
            limit_price = event.ask if side is Side.BUY else event.bid
            plan = ExecutionPlan(
                f"{prefix}-plan",
                intent.intent_id,
                risk.decision_id,
                side,
                OrderType.LIMIT,
                TimeInForce.DAY,
                abs(approved),
                limit_price,
                market,
                config.risk_policy_version,
                event.available_at,
                event.available_at + timedelta(seconds=20),
                event.bid,
                event.ask,
                clock_session_id,
                clock.monotonic(),
                clock.monotonic() + 20,
            )
            client_order_id = f"{prefix}-order"
            request = OrderRequest(
                client_order_id,
                plan.plan_id,
                config.accounting_seed.account_id,
                config.instrument,
                side,
                OrderType.LIMIT,
                TimeInForce.DAY,
                abs(approved),
                limit_price,
                event.available_at,
            )
            permit = safety.issue_permit(
                config.accounting_seed.account_id,
                PermitScope.NEW_ORDER,
                event.available_at,
                client_order_id=client_order_id,
                risk_decision_id=risk.decision_id,
                execution_plan_id=plan.plan_id,
            )
            state = coordinator.submit(request, risk, plan, intent, permit)
            if state is not SubmissionState.ACKNOWLEDGED:
                raise BacktestExecutionError(
                    f"simulated broker submission was not acknowledged: {state.value}"
                )
            broker_order_id = _broker_order_id(ledger, client_order_id)
            lifecycle.record((broker.opened_fact(broker_order_id),))
            broker_order_ids.append(broker_order_id)
            broker_to_client[broker_order_id] = client_order_id
            client_to_side[client_order_id] = side
            client_to_limit[client_order_id] = limit_price

        cancel_sequence = data.events[-1].source_sequence + 1
        for broker_order_id in broker_order_ids:
            order = broker.order(broker_order_id)
            if order.execution_state not in {
                BrokerExecutionState.OPEN,
                BrokerExecutionState.PARTIALLY_FILLED,
            }:
                continue
            cancellation = broker.cancel(
                broker_order_id,
                occurred_at=clock.wall(),
                sequence=cancel_sequence,
            )
            cancel_sequence += 1
            if type(cancellation) is not SimulationResult:
                raise BacktestExecutionError("simulator returned a malformed terminal result")
            if cancellation.reason is not SimulationReason.CANCELED:
                raise BacktestExecutionError("sample-end cancellation did not terminate the order")
            _record_facts(lifecycle, cancellation.facts)

        facts, accounting = _projection_from_ledger(ledger, config.accounting_seed)
        opened_sides = {
            fact.client_order_id: fact.side
            for fact in facts
            if type(fact) is BrokerOrderOpened
        }
        fills = [
            BacktestFillRecord(
                fact.client_order_id,
                fact.broker_execution_id,
                opened_sides[fact.client_order_id],
                fact.quantity,
                fact.price,
                fact.fee,
                fact.occurred_at,
            )
            for fact in facts
            if type(fact) is BrokerFillObserved
        ]
        orders: list[BacktestOrderRecord] = []
        for broker_order_id in broker_order_ids:
            client_order_id = broker_to_client[broker_order_id]
            projection = ledger.broker_order_projection(client_order_id)
            if projection is None:
                raise BacktestExecutionError("persisted broker projection is missing")
            order = projection.order
            if order.execution_state in {
                BrokerExecutionState.OPEN,
                BrokerExecutionState.PARTIALLY_FILLED,
            }:
                raise BacktestExecutionError("Dry run ended with an active order")
            side = client_to_side[client_order_id]
            orders.append(
                BacktestOrderRecord(
                    client_order_id,
                    broker_order_id,
                    side,
                    order.requested,
                    order.filled,
                    client_to_limit[client_order_id],
                    order.execution_state,
                )
            )
        output = BacktestOutput(
            run_spec.fingerprint(),
            len(data.events),
            tuple(decisions),
            tuple(orders),
            tuple(fills),
            accounting,
        )
        if not ledger.full_ledger_verify():
            raise BacktestExecutionError("ledger verification failed before close")
        ledger_digest = ledger.content_digest()
    finally:
        ledger.close()

    reopened = SQLiteLedger(target_path)
    try:
        if not reopened.full_ledger_verify() or reopened.content_digest() != ledger_digest:
            raise BacktestExecutionError("ledger changed or failed verification after reopen")
        reopened_facts, reopened_accounting = _projection_from_ledger(
            reopened, config.accounting_seed
        )
        if reopened_facts != facts or reopened_accounting != accounting:
            raise BacktestExecutionError("ledger reopen changed lifecycle or accounting replay")
    finally:
        reopened.close()

    result = RunResult(
        f"dry-{uuid4()}",
        run_spec.fingerprint(),
        RunStatus.SUCCEEDED,
        started_at,
        datetime.now(timezone.utc),
        ledger_digest,
        output.fingerprint(),
    )
    return CompletedBacktest(run_spec, result, output)


def _write_exclusive(path: Path, content: str) -> None:
    if path.exists() or path.is_symlink():
        raise BacktestInputError("result path already exists")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor: int | None = None
    temporary_path: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
        )
        temporary_path = Path(temporary_name)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            descriptor = None
            handle.write(content)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary_path, path)
        except FileExistsError as error:
            raise BacktestInputError("result path already exists") from error
    except OSError as error:
        raise BacktestExecutionError("result could not be written exclusively") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass


def _validate_cli_output_paths(ledger_path: Path, result_path: Path) -> None:
    if ledger_path.is_symlink():
        raise BacktestInputError("ledger path cannot be a symlink")
    if result_path.is_symlink():
        raise BacktestInputError("result path cannot be a symlink")
    try:
        ledger = ledger_path.resolve(strict=False)
        result = result_path.resolve(strict=False)
    except OSError as error:
        raise BacktestInputError("ledger/result paths could not be resolved") from error
    reserved = {
        ledger,
        Path(f"{ledger}-wal"),
        Path(f"{ledger}-shm"),
        Path(f"{ledger}-journal"),
    }
    if result in reserved:
        raise BacktestInputError("result path cannot alias the ledger or a SQLite sidecar")


def _verified_ledger_digest(path: Path) -> str | None:
    if path.is_symlink() or not path.is_file():
        return None
    try:
        ledger = SQLiteLedger(path)
        try:
            if not ledger.full_ledger_verify():
                return None
            return ledger.content_digest()
        finally:
            ledger.close()
    except Exception:
        return None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate or run an offline, credential-free Dry backtest."
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    for name in ("validate", "run"):
        command = subcommands.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--data", type=Path, required=True)
        command.add_argument("--code-commit", required=True)
        if name == "run":
            command.add_argument("--ledger", type=Path, required=True)
            command.add_argument("--result", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "validate":
            validated = validate_backtest_inputs(
                arguments.config,
                arguments.data,
                code_commit=arguments.code_commit,
            )
            print(
                json.dumps(
                    {
                        "run_spec_fingerprint": validated.run_spec.fingerprint(),
                        "data_sha256": validated.data.sha256,
                        "quote_count": len(validated.data.events),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
            return 0
        if arguments.result.exists():
            raise BacktestInputError("result path already exists")
        _validate_cli_output_paths(arguments.ledger, arguments.result)
        validated = validate_backtest_inputs(
            arguments.config,
            arguments.data,
            code_commit=arguments.code_commit,
        )
        started_at = datetime.now(timezone.utc)
        try:
            completed = _run_validated_backtest(validated, arguments.ledger)
            _write_exclusive(arguments.result, completed.canonical_json())
        except Exception as error:
            failed = RunResult(
                f"dry-{uuid4()}",
                validated.run_spec.fingerprint(),
                RunStatus.FAILED,
                started_at,
                datetime.now(timezone.utc),
                ledger_sha256=(
                    None
                    if isinstance(error, BacktestInputError)
                    else _verified_ledger_digest(arguments.ledger)
                ),
                failure_code="BACKTEST_EXECUTION_FAILED",
            )
            try:
                _write_exclusive(
                    arguments.result,
                    _run_envelope_json(validated.run_spec, failed, None),
                )
            except Exception as result_error:
                print(
                    "backtest failure result could not be written: "
                    f"{type(result_error).__name__}: {result_error}",
                    file=sys.stderr,
                )
            print(f"backtest failed: {type(error).__name__}: {error}", file=sys.stderr)
            return 2
        print(completed.result.canonical_json())
        return 0
    except Exception as error:
        print(f"backtest failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
