import ast
from dataclasses import replace
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from importlib.util import resolve_name
from pathlib import Path

from trader.adapters.persistence.sqlite_ledger import SQLiteLedger
from trader.adapters.process_lock import AccountProcessLock, ProcessLockBusy
from trader.adapters.simulated.fake_broker import FakeBroker
from trader.application.execution import (
    AlreadySubmitted,
    ExecutionService,
    PersistenceFailure,
    SubmissionValidationError,
)
from trader.application.operator import (
    OperatorCommandRejected,
    OperatorCommandService,
    OperatorPersistenceFailure,
)
from trader.application.safety import InvalidPermit, SafetyController, SafetyGuardError
from trader.domain.models import (
    AccountSnapshot,
    BrokerExecutionState,
    BrokerOrder,
    ExecutionPlan,
    InstrumentId,
    MarketEvidence,
    ObservedAmount,
    OperatorAction,
    OperatorCommand,
    OrderRequest,
    OrderType,
    PendingAction,
    PermitScope,
    PositionTarget,
    RiskDecision,
    RiskOutcome,
    RiskStage,
    SafetyState,
    Side,
    SnapshotQuality,
    SubmissionState,
    TargetUnit,
    TimeInForce,
    TradeIntent,
    UnknownResolutionEvidence,
    UnknownResolutionResult,
)
from trader.domain.risk import continuous_disarm, eligibility, pre_trade_quantity_cap
from trader.ports.broker import (
    BrokerEnvironment,
    BrokerSubmitOutcome,
    BrokerSubmitResult,
)
from trader.ports.ledger import (
    LedgerEvent,
    LedgerPersistenceError,
    OrderReservationConflict,
    PermitAlreadyConsumed,
)


NOW = datetime(2026, 8, 25, 3, tzinfo=timezone.utc)
_TEST_LEDGERS = []


def tearDownModule():
    for ledger in _TEST_LEDGERS:
        ledger.close()


def snapshot(quality=SnapshotQuality.CONSISTENT, observed_at=NOW):
    observed = ObservedAmount(Decimal("1"), "obs", observed_at)
    return AccountSnapshot("snap", "acct", quality, *(observed,) * 6, observed_at)


def market(quality=SnapshotQuality.CONSISTENT, observed_at=NOW):
    return MarketEvidence("market", quality, observed_at)


def intent(quantity=Decimal("2"), current=None):
    if current is None:
        current = Decimal("1") if quantity > 0 else Decimal("5")
    target = current + quantity
    return TradeIntent(
        "intent", "target", "strategy", "acct", "snap",
        InstrumentId("NASDAQ", "AAPL", "USD"),
        target, current, Decimal(0), quantity, NOW,
    )


def risk(quantity=Decimal("2"), outcome=RiskOutcome.APPROVED, snapshot_id="snap"):
    approved = Decimal(0) if outcome is RiskOutcome.REJECTED else quantity
    return RiskDecision(
        "risk", RiskStage.PRE_TRADE, "policy-v1", snapshot_id, "intent", quantity, approved,
        outcome, (), NOW,
    )


def plan(quantity=Decimal("2"), expires_at=NOW + timedelta(minutes=1), price=Decimal("100.25")):
    side = Side.BUY if quantity > 0 else Side.SELL
    return ExecutionPlan(
        "plan", "intent", "risk", side, OrderType.LIMIT, TimeInForce.DAY,
        abs(quantity), price, expires_at,
    )


def request(order_id="order-1", quantity=Decimal("2"), price=Decimal("100.25")):
    side = Side.BUY if quantity > 0 else Side.SELL
    return OrderRequest(
        order_id, "plan", "acct", InstrumentId("NASDAQ", "AAPL", "USD"), side,
        OrderType.LIMIT, TimeInForce.DAY, abs(quantity), price, NOW,
    )


def new_order_permit(
    safety,
    *,
    order_id="order-1",
    risk_id="risk",
    plan_id="plan",
    **kwargs,
):
    return safety.issue_permit(
        "acct",
        PermitScope.NEW_ORDER,
        NOW,
        client_order_id=order_id,
        risk_decision_id=risk_id,
        execution_plan_id=plan_id,
        **kwargs,
    )


def command(
    safety, action, command_id, account_id="acct", at=NOW, *, client_order_id=None,
):
    return OperatorCommand(
        command_id, "operator", "phase 1A test", "deploy-v1", safety.epoch,
        at, at + timedelta(minutes=1), action, account_id, client_order_id,
    )


def operator(safety, ledger=None, at=NOW):
    if ledger is None:
        ledger = SQLiteLedger(":memory:")
        _TEST_LEDGERS.append(ledger)
    service = OperatorCommandService(
        ledger, safety, "deploy-v1", lambda: at, account_id="acct"
    )
    safety._test_operator_service = service
    return service


def reconciled_safety(trading=True):
    safety = SafetyController()
    ops = operator(safety)
    ops.acknowledge_startup_recovery(
        command(safety, OperatorAction.ACKNOWLEDGE_STARTUP_RECOVERY, "op-recovery")
    )
    safety.complete_reconciliation(snapshot(), market(), "policy-v1", "deploy-v1", NOW)
    if trading:
        ops.arm(command(safety, OperatorAction.ARM, "op-arm"))
    return safety


class DomainAndRiskTests(unittest.TestCase):
    def test_decimal_utc_long_only_and_enum_invariants(self):
        with self.assertRaises(ValueError):
            request(quantity=Decimal("1.5"))
        with self.assertRaises(ValueError):
            replace(request(), created_at=NOW.replace(tzinfo=None))
        with self.assertRaises(ValueError):
            replace(request(), side="BUY")
        with self.assertRaises(ValueError):
            PositionTarget(
                "t", "s", InstrumentId("M", "S", "USD"), Decimal("-1"),
                TargetUnit.SHARES, NOW,
            )
        with self.assertRaises(ValueError):
            PositionTarget(
                "t", "s", InstrumentId("M", "S", "USD"), Decimal(1), "NOTIONAL", NOW,
            )

    def test_risk_quantity_semantics(self):
        with self.assertRaises(ValueError):
            RiskDecision(
                "x", RiskStage.ELIGIBILITY, "p", "s", None, Decimal(1), None,
                RiskOutcome.APPROVED, (), NOW,
            )
        with self.assertRaises(ValueError):
            RiskDecision(
                "x", RiskStage.PRE_TRADE, "p", "s", "intent", Decimal(2), Decimal(-1),
                RiskOutcome.ADJUSTED, (), NOW,
            )
        with self.assertRaises(ValueError):
            replace(risk(), outcome="APPROVED")

    def test_unknown_resolution_command_requires_only_order_target(self):
        safety = SafetyController()
        with self.assertRaises(ValueError):
            command(
                safety,
                OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
                "missing-target",
                "acct",
            )
        resolution = command(
            safety,
            OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            "valid-target",
            "acct",
            client_order_id="order-1",
        )
        with self.assertRaises(ValueError):
            replace(resolution, risk_decision_id="risk")
        with self.assertRaises(ValueError):
            replace(resolution, execution_plan_id="plan")

    def test_broker_order_state_and_pending_action_invariants(self):
        with self.assertRaises(ValueError):
            BrokerOrder(
                "b", "a", Decimal(2), Decimal(0), Decimal(2), Decimal(0),
                Decimal(0), Decimal(0), BrokerExecutionState.FILLED,
            )
        with self.assertRaises(ValueError):
            BrokerOrder(
                "b", "a", Decimal(2), Decimal(2), Decimal(0), Decimal(0),
                Decimal(0), Decimal(0), BrokerExecutionState.FILLED,
                PendingAction.CANCEL_REQUESTED,
            )
        observed = BrokerOrder(
            "b", "a", Decimal(2), Decimal(1), Decimal(1), Decimal(0), Decimal(0),
            Decimal(0), BrokerExecutionState.PARTIALLY_FILLED,
            PendingAction.CANCEL_REQUESTED,
        )
        self.assertEqual(observed.pending_action, PendingAction.CANCEL_REQUESTED)

    def test_three_risk_stages_are_deterministic(self):
        denied = eligibility(
            "e", "v1", "s", NOW,
            instrument_allowed=True, session_open=False, data_fresh=True,
        )
        self.assertEqual(denied.reason_codes, ("SESSION_CLOSED",))
        intent = TradeIntent(
            "i", "t", "strategy", "acct", "s", InstrumentId("M", "S", "USD"),
            Decimal("2"), Decimal("6"), Decimal("1"), Decimal("-5"), NOW,
        )
        adjusted = pre_trade_quantity_cap("p", "v1", "s", intent, Decimal("3"), NOW)
        self.assertEqual(adjusted.approved_quantity, Decimal("-3"))
        trigger = continuous_disarm("c", "v1", "s", NOW, submitted_unknown=True)
        self.assertEqual(trigger.reason_codes, ("SUBMITTED_UNKNOWN",))


class SafetyTests(unittest.TestCase):
    def test_global_operator_command_fails_closed_at_domain_and_boundary(self):
        safety_a = SafetyController()
        with self.assertRaises(ValueError):
            OperatorCommand(
                "global-command",
                "operator",
                "unsafe broadcast",
                "deploy-v1",
                safety_a.epoch,
                NOW,
                NOW + timedelta(minutes=1),
                OperatorAction.HALT,
                None,
            )

        ledger = SQLiteLedger(":memory:")
        try:
            safety_b = SafetyController()
            service_a = OperatorCommandService(
                ledger, safety_a, "deploy-v1", lambda: NOW, account_id="acct"
            )
            service_b = OperatorCommandService(
                ledger, safety_b, "deploy-v1", lambda: NOW, account_id="other"
            )
            forged_global = command(
                safety_a, OperatorAction.HALT, "global-command"
            )
            object.__setattr__(forged_global, "account_id", None)
            for service in (service_a, service_b):
                with self.assertRaises(OperatorCommandRejected):
                    service.halt(forged_global)
            self.assertEqual(ledger.events_for("global-command"), ())
            self.assertEqual(safety_a.state, SafetyState.BOOTSTRAPPING)
            self.assertEqual(safety_b.state, SafetyState.BOOTSTRAPPING)
        finally:
            ledger.close()

    def test_operator_recovery_and_effects_are_account_scoped(self):
        ledger = SQLiteLedger(":memory:")
        safety = SafetyController()
        try:
            for command_id, account_id in (("pending-a", "acct"), ("pending-b", "other")):
                pending = command(
                    safety, OperatorAction.HALT, command_id, account_id
                )
                ledger.reserve_operator_command(
                    pending,
                    LedgerEvent(
                        f"{command_id}-requested",
                        "OPERATOR_COMMAND_REQUESTED",
                        command_id,
                        NOW,
                        {
                            **ledger.canonical_command(pending),
                            "previous_state": SafetyState.BOOTSTRAPPING.value,
                        },
                    ),
                )
            OperatorCommandService(
                ledger,
                safety,
                "deploy-v1",
                lambda: NOW,
                account_id="acct",
            )
            self.assertIn("PENDING_OPERATOR_COMMAND:pending-a", safety.blockers)
            self.assertNotIn("PENDING_OPERATOR_COMMAND:pending-b", safety.blockers)

            clean_safety = SafetyController()
            scoped = OperatorCommandService(
                ledger,
                clean_safety,
                "deploy-v1",
                lambda: NOW,
                account_id="acct",
            )
            foreign = command(
                clean_safety, OperatorAction.HALT, "foreign-effect", "other"
            )
            with self.assertRaises(OperatorCommandRejected):
                scoped.halt(foreign)
            self.assertEqual(ledger.events_for(foreign.command_id), ())
        finally:
            ledger.close()

    def test_direct_arm_and_forged_permit_fail(self):
        safety = SafetyController()
        with self.assertRaises(TypeError):
            safety.arm("op", NOW)
        safety = reconciled_safety()
        with self.assertRaises(InvalidPermit):
            safety.issue_permit("acct", PermitScope.NEW_ORDER, NOW)
        permit = new_order_permit(safety)
        forged = replace(permit, permit_id="forged")
        with self.assertRaises(InvalidPermit):
            safety.validate(
                forged,
                "acct",
                PermitScope.NEW_ORDER,
                NOW,
                client_order_id="order-1",
                risk_decision_id="risk",
                execution_plan_id="plan",
            )
        safety.halt()
        with self.assertRaises(InvalidPermit):
            safety.validate(
                permit,
                "acct",
                PermitScope.NEW_ORDER,
                NOW,
                client_order_id="order-1",
                risk_decision_id="risk",
                execution_plan_id="plan",
            )

    def test_permit_cannot_outlive_account_or_market_evidence(self):
        observed_at = NOW - timedelta(seconds=29)
        safety = SafetyController()
        ops = operator(safety)
        ops.acknowledge_startup_recovery(
            command(safety, OperatorAction.ACKNOWLEDGE_STARTUP_RECOVERY, "op-recovery")
        )
        safety.complete_reconciliation(
            snapshot(observed_at=observed_at),
            market(observed_at=observed_at),
            "policy-v1",
            "deploy-v1",
            NOW,
        )
        ops.arm(command(safety, OperatorAction.ARM, "op-arm"))
        permit = new_order_permit(safety, ttl_seconds=30)
        self.assertEqual(permit.expires_at, NOW + timedelta(seconds=1))
        with self.assertRaises(InvalidPermit):
            safety.validate(
                permit,
                "acct",
                PermitScope.NEW_ORDER,
                NOW + timedelta(seconds=5),
                client_order_id="order-1",
                risk_decision_id="risk",
                execution_plan_id="plan",
            )

    def test_unknown_blocker_requires_audited_resolution(self):
        safety = SafetyController()
        safety.block_unknown_submission("order")
        with self.assertRaises(TypeError):
            safety.acknowledge_startup_recovery("op")
        self.assertEqual(safety.state, SafetyState.HALTED)

    def test_cancel_is_operator_approved_short_lived_and_evidence_independent(self):
        safety = SafetyController()
        safety.halt()
        with self.assertRaises(InvalidPermit):
            safety.issue_permit("acct", PermitScope.CANCEL, NOW)
        ops = operator(safety)
        permit = ops.issue_permit(
            command(safety, OperatorAction.ISSUE_CANCEL, "op-cancel", "acct")
        )
        self.assertIsNone(permit.account_snapshot_id)
        self.assertIsNone(permit.market_snapshot_id)
        self.assertEqual(permit.expires_at, NOW + timedelta(seconds=30))
        safety.validate(permit, "acct", PermitScope.CANCEL, NOW)
        with self.assertRaises(OperatorCommandRejected):
            ops.issue_permit(replace(
                command(
                    safety,
                    OperatorAction.ISSUE_CANCEL,
                    "op-bound-cancel",
                    "acct",
                ),
                client_order_id="order-1",
            ))

        stale = reconciled_safety()
        stale.halt()
        stale_at = NOW + timedelta(minutes=10)
        stale_ops = operator(stale, at=stale_at)
        stale_cancel = stale_ops.issue_permit(
            command(
                stale, OperatorAction.ISSUE_CANCEL, "op-stale-cancel", "acct", stale_at
            )
        )
        stale.validate(
            stale_cancel, "acct", PermitScope.CANCEL, NOW + timedelta(minutes=10)
        )

    def test_reduce_only_always_requires_operator_approval(self):
        safety = reconciled_safety()
        with self.assertRaises(InvalidPermit):
            safety.issue_permit("acct", PermitScope.REDUCE_ONLY, NOW)
        permit = safety._test_operator_service.issue_permit(
            command(safety, OperatorAction.ISSUE_REDUCE_ONLY, "op-reduce", "acct")
        )
        safety.validate(permit, "acct", PermitScope.REDUCE_ONLY, NOW)

    def test_new_halted_blocker_invalidates_emergency_permits_without_epoch_churn(self):
        safety = reconciled_safety()
        safety.halt()
        ops = safety._test_operator_service
        permits = (
            ops.issue_permit(
                command(safety, OperatorAction.ISSUE_CANCEL, "op-cancel", "acct")
            ),
            ops.issue_permit(
                command(safety, OperatorAction.ISSUE_REDUCE_ONLY, "op-reduce", "acct")
            ),
            ops.issue_permit(
                command(safety, OperatorAction.ISSUE_EMERGENCY_FLATTEN, "op-flatten", "acct")
            ),
        )
        prior_epoch = safety.epoch
        safety.halt("MARKET_FAILURE")
        self.assertEqual(safety.epoch, prior_epoch + 1)
        for permit in permits:
            with self.assertRaises(InvalidPermit):
                safety.validate(permit, "acct", permit.scope, NOW)
        stable_epoch = safety.epoch
        safety.halt("MARKET_FAILURE")
        self.assertEqual(safety.epoch, stable_epoch)

    def test_persistence_failure_blocks_every_system_permit(self):
        safety = reconciled_safety()
        permits = [new_order_permit(safety)]
        safety.halt()
        ops = safety._test_operator_service
        permits.extend((
            ops.issue_permit(
                command(safety, OperatorAction.ISSUE_CANCEL, "op-cancel", "acct")
            ),
            ops.issue_permit(
                command(safety, OperatorAction.ISSUE_REDUCE_ONLY, "op-reduce", "acct")
            ),
            ops.issue_permit(
                command(safety, OperatorAction.ISSUE_EMERGENCY_FLATTEN, "op-flatten", "acct")
            ),
        ))
        safety.halt("PERSISTENCE_FAILURE")
        for permit in permits:
            with self.assertRaises(InvalidPermit):
                safety.validate(permit, "acct", permit.scope, NOW)
        for scope in PermitScope:
            with self.assertRaises(InvalidPermit):
                if scope is PermitScope.NEW_ORDER:
                    new_order_permit(safety)
                else:
                    action = {
                        PermitScope.CANCEL: OperatorAction.ISSUE_CANCEL,
                        PermitScope.REDUCE_ONLY: OperatorAction.ISSUE_REDUCE_ONLY,
                        PermitScope.EMERGENCY_FLATTEN: OperatorAction.ISSUE_EMERGENCY_FLATTEN,
                    }[scope]
                    ops.issue_permit(command(safety, action, f"op-{scope.value}", "acct"))


class LedgerAndExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "ledger.db"
        self.ledger = SQLiteLedger(self.path)
        self.account_lock = AccountProcessLock(
            self.path, "acct", "deploy-v1"
        )
        self.account_lock.acquire()

    def tearDown(self):
        self.account_lock.release()
        self.ledger.close()
        self.temp.cleanup()

    def service(self, outcome=BrokerSubmitOutcome.ACKNOWLEDGED, safety=None, environment=None):
        broker = FakeBroker(outcome, environment or BrokerEnvironment.SIMULATED)
        process_lock = self.account_lock if broker.environment is BrokerEnvironment.LIVE else None
        return ExecutionService(
            broker,
            self.ledger,
            safety or SafetyController(),
            lambda: NOW,
            process_lock,
            account_id="acct",
        )

    def test_live_constructor_requires_acquired_lock_before_recovery(self):
        class UntouchedLedger:
            touched = False

            def incomplete_submissions(self, account_id=None):
                self.touched = True
                raise AssertionError("ledger must not be touched")

        ledger = UntouchedLedger()
        broker = FakeBroker(BrokerSubmitOutcome.ACKNOWLEDGED, BrokerEnvironment.LIVE)
        with self.assertRaises(SubmissionValidationError):
            ExecutionService(
                broker, ledger, SafetyController(), lambda: NOW, account_id="acct"
            )
        self.assertFalse(ledger.touched)
        self.assertEqual(broker.calls, [])

    def test_live_constructor_requires_lock_for_matching_account(self):
        safety = reconciled_safety()
        wrong_lock = AccountProcessLock(
            self.path, "other-account", "deploy-v1"
        )
        wrong_lock.acquire()
        try:
            broker = FakeBroker(BrokerSubmitOutcome.ACKNOWLEDGED, BrokerEnvironment.LIVE)
            with self.assertRaises(SubmissionValidationError):
                ExecutionService(
                    broker,
                    self.ledger,
                    safety,
                    lambda: NOW,
                    wrong_lock,
                    account_id="acct",
                )
            self.assertEqual(self.ledger.events_for("order-1"), ())
            self.assertEqual(broker.calls, [])
        finally:
            wrong_lock.release()

    def test_live_constructor_rejects_same_account_lock_for_another_runtime(self):
        safety = reconciled_safety()
        wrong_runtime_lock = AccountProcessLock(
            Path(self.temp.name) / "other.db", "acct", "deploy-v1"
        )
        wrong_runtime_lock.acquire()
        try:
            broker = FakeBroker(BrokerSubmitOutcome.ACKNOWLEDGED, BrokerEnvironment.LIVE)
            with self.assertRaises(SubmissionValidationError):
                ExecutionService(
                    broker,
                    self.ledger,
                    safety,
                    lambda: NOW,
                    wrong_runtime_lock,
                    account_id="acct",
                )
            self.assertEqual(broker.calls, [])
        finally:
            wrong_runtime_lock.release()

    def test_live_submit_fails_closed_after_outer_lock_release(self):
        safety = reconciled_safety()
        broker = FakeBroker(BrokerSubmitOutcome.ACKNOWLEDGED, BrokerEnvironment.LIVE)
        service = ExecutionService(
            broker,
            self.ledger,
            safety,
            lambda: NOW,
            self.account_lock,
            account_id="acct",
        )
        self.account_lock.release()
        with self.assertRaises(SubmissionValidationError):
            service.submit(request(), risk(), plan(), intent(), new_order_permit(safety))
        self.assertEqual(self.ledger.events_for("order-1"), ())
        self.assertEqual(broker.calls, [])

    def test_live_submit_rejects_request_for_another_account(self):
        safety = reconciled_safety()
        service = self.service(safety=safety, environment=BrokerEnvironment.LIVE)
        with self.assertRaises(SubmissionValidationError):
            service.submit(
                replace(request(), account_id="other-account"),
                risk(),
                plan(),
                intent(),
                new_order_permit(safety),
            )
        self.assertEqual(self.ledger.events_for("order-1"), ())
        self.assertEqual(service.broker.calls, [])

    def test_live_submit_succeeds_with_matching_outer_lock(self):
        safety = reconciled_safety()
        service = self.service(safety=safety, environment=BrokerEnvironment.LIVE)
        self.assertEqual(
            service.submit(request(), risk(), plan(), intent(), new_order_permit(safety)),
            SubmissionState.ACKNOWLEDGED,
        )

    def test_simulated_constructor_does_not_require_process_lock(self):
        service = self.service()
        self.assertEqual(
            service.submit(request(), risk(), plan(), intent()),
            SubmissionState.ACKNOWLEDGED,
        )

    def test_non_live_startup_does_not_recover_other_accounts_live_inflight(self):
        self.ledger.reserve_submission(
            "live-inflight",
            {"request": {"account_id": "acct"}, "permit": None},
            LedgerEvent("live-prepared", "PREPARED", "live-inflight", NOW, {}),
            LedgerEvent("live-started", "SUBMISSION_STARTED", "live-inflight", NOW, {}),
            None,
        )
        ExecutionService(
            FakeBroker(BrokerSubmitOutcome.ACKNOWLEDGED, BrokerEnvironment.SIMULATED),
            self.ledger,
            SafetyController(),
            lambda: NOW,
            account_id="other-account",
        )
        self.assertEqual(
            self.ledger.events_for("live-inflight")[-1].event_type,
            "SUBMISSION_STARTED",
        )

    def test_live_startup_recovers_only_its_account(self):
        for order_id, account_id in (("owned", "acct"), ("foreign", "other-account")):
            self.ledger.reserve_submission(
                order_id,
                {"request": {"account_id": account_id}, "permit": None},
                LedgerEvent(f"{order_id}-prepared", "PREPARED", order_id, NOW, {}),
                LedgerEvent(f"{order_id}-started", "SUBMISSION_STARTED", order_id, NOW, {}),
                None,
            )
        ExecutionService(
            FakeBroker(BrokerSubmitOutcome.ACKNOWLEDGED, BrokerEnvironment.LIVE),
            self.ledger,
            SafetyController(),
            lambda: NOW,
            self.account_lock,
            account_id="acct",
        )
        self.assertEqual(self.ledger.events_for("owned")[-1].event_type, "SUBMITTED_UNKNOWN")
        self.assertEqual(self.ledger.events_for("foreign")[-1].event_type, "SUBMISSION_STARTED")

    def test_live_startup_blocks_only_its_accounts_unresolved_unknown(self):
        for order_id, account_id in (("owned", "acct"), ("foreign", "other-account")):
            self.ledger.reserve_submission(
                order_id,
                {"request": {"account_id": account_id}, "permit": None},
                LedgerEvent(f"{order_id}-prepared", "PREPARED", order_id, NOW, {}),
                LedgerEvent(f"{order_id}-started", "SUBMISSION_STARTED", order_id, NOW, {}),
                None,
            )
            self.ledger.append(
                LedgerEvent(f"{order_id}-unknown", "SUBMITTED_UNKNOWN", order_id, NOW, {})
            )
        safety = SafetyController()
        ExecutionService(
            FakeBroker(BrokerSubmitOutcome.ACKNOWLEDGED, BrokerEnvironment.LIVE),
            self.ledger,
            safety,
            lambda: NOW,
            self.account_lock,
            account_id="acct",
        )
        self.assertIn("SUBMITTED_UNKNOWN:owned", safety.blockers)
        self.assertNotIn("SUBMITTED_UNKNOWN:foreign", safety.blockers)

    def test_live_hold_covers_reservation_broker_and_terminal_record(self):
        reserved = threading.Event()
        release_attempted = threading.Event()
        released = threading.Event()
        contender = AccountProcessLock(
            self.path, "acct", "deploy-second"
        )

        class SignalingLedger:
            def __init__(self, real):
                self.real = real

            def __getattr__(self, name):
                return getattr(self.real, name)

            def reserve_submission(self, *args):
                result = self.real.reserve_submission(*args)
                reserved.set()
                return result

        class RacingBroker:
            environment = BrokerEnvironment.LIVE

            def __init__(self):
                self.calls = []
                self.release_was_blocked = False
                self.second_owner_was_blocked = False

            def submit(self, order):
                self.calls.append(order)
                self.release_was_blocked = (
                    release_attempted.wait(2) and not released.wait(0.05)
                )
                try:
                    contender.acquire()
                except ProcessLockBusy:
                    self.second_owner_was_blocked = True
                else:
                    contender.release()
                return BrokerSubmitResult(BrokerSubmitOutcome.ACKNOWLEDGED, "broker-order")

        def release_after_reservation():
            reserved.wait(2)
            release_attempted.set()
            self.account_lock.release()
            released.set()

        safety = reconciled_safety()
        broker = RacingBroker()
        service = ExecutionService(
            broker,
            SignalingLedger(self.ledger),
            safety,
            lambda: NOW,
            self.account_lock,
            account_id="acct",
        )
        releaser = threading.Thread(target=release_after_reservation)
        releaser.start()
        self.assertEqual(
            service.submit(request(), risk(), plan(), intent(), new_order_permit(safety)),
            SubmissionState.ACKNOWLEDGED,
        )
        releaser.join(2)
        self.assertTrue(released.is_set())
        self.assertTrue(broker.release_was_blocked)
        self.assertTrue(broker.second_owner_was_blocked)
        self.assertEqual(self.ledger.events_for("order-1")[-1].event_type, "ACKNOWLEDGED")
        contender.acquire()
        contender.release()

    def test_append_only_duplicate_and_integrity(self):
        event = LedgerEvent("event", "TYPE", "agg", NOW, {"safe": "json"})
        self.ledger.append(event)
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.append(event)
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute("UPDATE ledger_events SET event_type='X'")
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute("DELETE FROM ledger_events")
        self.assertTrue(self.ledger.integrity_check())

    def test_atomic_order_reservation_conflict_and_immutability(self):
        prepared = LedgerEvent("prepare", "PREPARED", "same", NOW, {})
        started = LedgerEvent("started", "SUBMISSION_STARTED", "same", NOW, {})
        self.assertTrue(self.ledger.reserve_submission(
            "same", {"quantity": "1", "permit": None}, prepared, started, None,
        ))
        self.assertFalse(self.ledger.reserve_submission(
            "same", {"quantity": "1", "permit": None}, prepared, started, None,
        ))
        with self.assertRaises(OrderReservationConflict):
            self.ledger.reserve_submission(
                "same", {"quantity": "2", "permit": None}, prepared, started, None,
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute(
                "INSERT INTO order_requests VALUES ('same', '{}', 'x')"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute("UPDATE order_requests SET canonical_json='{}'")
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.connection.execute("DELETE FROM order_requests")

    def test_all_broker_outcomes_and_unknown_is_not_retried(self):
        for index, (outcome, expected) in enumerate((
            (BrokerSubmitOutcome.ACKNOWLEDGED, SubmissionState.ACKNOWLEDGED),
            (BrokerSubmitOutcome.REJECTED, SubmissionState.SUBMISSION_REJECTED),
            (BrokerSubmitOutcome.UNKNOWN, SubmissionState.SUBMITTED_UNKNOWN),
        )):
            order = request(f"order-{index}")
            service = self.service(outcome)
            self.assertEqual(service.submit(order, risk(), plan(), intent()), expected)
            self.assertEqual(
                [event.event_type for event in self.ledger.events_for(order.client_order_id)],
                ["PREPARED", "SUBMISSION_STARTED", expected.value],
            )
            if outcome is BrokerSubmitOutcome.UNKNOWN:
                self.assertEqual(service.safety.state, SafetyState.HALTED)
                with self.assertRaises(AlreadySubmitted):
                    service.submit(order, risk(), plan(), intent())

    def test_broker_environment_controls_live_permit(self):
        live_safety = reconciled_safety()
        service = self.service(
            safety=live_safety,
            environment=BrokerEnvironment.LIVE,
        )
        with self.assertRaises(SubmissionValidationError):
            service.submit(request(), risk(), plan(), intent())
        permit = new_order_permit(live_safety)
        self.assertEqual(
            service.submit(request(), risk(), plan(), intent(), permit),
            SubmissionState.ACKNOWLEDGED,
        )
        with self.assertRaises(AlreadySubmitted):
            service.submit(request(), risk(), plan(), intent(), permit)
        self.assertEqual(len(service.broker.calls), 1)

    def test_live_risk_must_match_permit_evidence(self):
        live_safety = reconciled_safety()
        service = self.service(safety=live_safety, environment=BrokerEnvironment.LIVE)
        permit = new_order_permit(live_safety)
        with self.assertRaises(SubmissionValidationError):
            service.submit(
                request(), risk(snapshot_id="different"), plan(), intent(), permit
            )
        self.assertEqual(len(service.broker.calls), 0)

    def test_live_permit_binding_mismatches_have_no_effect(self):
        for field, value in (
            ("order_id", "other-order"),
            ("risk_id", "other-risk"),
            ("plan_id", "other-plan"),
        ):
            with self.subTest(field=field):
                live_safety = reconciled_safety()
                service = self.service(
                    safety=live_safety, environment=BrokerEnvironment.LIVE
                )
                permit = new_order_permit(live_safety, **{field: value})
                with self.assertRaises(InvalidPermit):
                    service.submit(request(), risk(), plan(), intent(), permit)
                self.assertEqual(service.broker.calls, [])
                self.assertEqual(self.ledger.events_for("order-1"), ())

    def test_simulated_submission_rejects_live_permit_without_effect(self):
        live_safety = reconciled_safety()
        service = self.service(safety=live_safety)
        with self.assertRaises(SubmissionValidationError):
            service.submit(
                request(), risk(), plan(), intent(), new_order_permit(live_safety)
            )
        self.assertEqual(service.broker.calls, [])
        self.assertEqual(self.ledger.events_for("order-1"), ())

    def test_permit_consumption_is_unique_and_atomic(self):
        def events(order_id):
            return (
                LedgerEvent(f"{order_id}-prepared", "PREPARED", order_id, NOW, {}),
                LedgerEvent(f"{order_id}-started", "SUBMISSION_STARTED", order_id, NOW, {}),
            )

        first_prepared, first_started = events("first")
        payload = {"permit": {"permit_id": "single-use"}}
        self.assertTrue(self.ledger.reserve_submission(
            "first", payload, first_prepared, first_started, "single-use",
        ))
        second_prepared, second_started = events("second")
        with self.assertRaises(PermitAlreadyConsumed):
            self.ledger.reserve_submission(
                "second", payload, second_prepared, second_started, "single-use",
            )
        self.assertEqual(self.ledger.events_for("second"), ())

        self.ledger.connection.execute(
            """CREATE TRIGGER fail_submission_started BEFORE INSERT ON ledger_events
               WHEN NEW.event_type = 'SUBMISSION_STARTED' BEGIN
                 SELECT RAISE(ABORT, 'injected started failure'); END"""
        )
        rollback_prepared, rollback_started = events("rollback")
        rollback_payload = {"permit": {"permit_id": "rollback-permit"}}
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.reserve_submission(
                "rollback", rollback_payload, rollback_prepared, rollback_started,
                "rollback-permit",
            )
        self.assertIsNone(self.ledger.connection.execute(
            "SELECT 1 FROM order_requests WHERE client_order_id = 'rollback'"
        ).fetchone())
        self.assertEqual(self.ledger.events_for("rollback"), ())
        self.ledger.connection.execute("DROP TRIGGER fail_submission_started")
        self.assertTrue(self.ledger.reserve_submission(
            "rollback", rollback_payload, rollback_prepared, rollback_started,
            "rollback-permit",
        ))

    def test_consumed_permit_is_not_a_persistence_failure(self):
        class ConsumedPermitLedger:
            def __init__(self, real):
                self.real = real

            def __getattr__(self, name):
                return getattr(self.real, name)

            def reserve_submission(self, *unused_args):
                raise PermitAlreadyConsumed("already consumed")

        live_safety = reconciled_safety()
        broker = FakeBroker(BrokerSubmitOutcome.ACKNOWLEDGED, BrokerEnvironment.LIVE)
        service = ExecutionService(
            broker,
            ConsumedPermitLedger(self.ledger),
            live_safety,
            lambda: NOW,
            self.account_lock,
            account_id="acct",
        )
        with self.assertRaises(AlreadySubmitted):
            service.submit(
                request(), risk(), plan(), intent(), new_order_permit(live_safety)
            )
        self.assertEqual(live_safety.state, SafetyState.TRADING)
        self.assertEqual(broker.calls, [])

    def test_absent_rejected_stale_and_mismatched_evidence_fail(self):
        service = self.service()
        with self.assertRaises(TypeError):
            service.submit(request())
        with self.assertRaises(SubmissionValidationError):
            service.submit(
                request(), risk(outcome=RiskOutcome.REJECTED), plan(), intent()
            )
        with self.assertRaises(SubmissionValidationError):
            service.submit(request(), risk(), plan(expires_at=NOW), intent())
        with self.assertRaises(SubmissionValidationError):
            service.submit(request(), risk(), plan(price=Decimal("99")), intent())
        self.assertEqual(len(service.broker.calls), 0)

    def test_malformed_broker_result_is_unknown_and_halts(self):
        class MalformedBroker:
            environment = BrokerEnvironment.SIMULATED

            def submit(self, unused_request):
                return {"outcome": "ACKNOWLEDGED", "broker_order_id": "fake"}

        safety = SafetyController()
        service = ExecutionService(
            MalformedBroker(), self.ledger, safety, lambda: NOW, account_id="acct"
        )
        self.assertEqual(
            service.submit(request(), risk(), plan(), intent()),
            SubmissionState.SUBMITTED_UNKNOWN,
        )
        self.assertEqual(safety.state, SafetyState.HALTED)

    def test_broker_result_subclass_cannot_bypass_validation(self):
        class ForgedResult(BrokerSubmitResult):
            def __post_init__(self):
                pass

        class ForgedBroker:
            environment = BrokerEnvironment.SIMULATED

            def submit(self, unused_request):
                return ForgedResult("ACKNOWLEDGED", "fake-id", "free text")

        safety = SafetyController()
        service = ExecutionService(
            ForgedBroker(), self.ledger, safety, lambda: NOW, account_id="acct"
        )
        self.assertEqual(
            service.submit(request(), risk(), plan(), intent()),
            SubmissionState.SUBMITTED_UNKNOWN,
        )
        self.assertEqual(safety.state, SafetyState.HALTED)

    def test_post_submit_clock_failure_is_unknown_and_halts(self):
        calls = 0

        def failing_clock():
            nonlocal calls
            calls += 1
            if calls > 1:
                raise RuntimeError("clock failed after broker side effect")
            return NOW

        safety = SafetyController()
        broker = FakeBroker(BrokerSubmitOutcome.ACKNOWLEDGED)
        service = ExecutionService(
            broker, self.ledger, safety, failing_clock, account_id="acct"
        )
        self.assertEqual(
            service.submit(request("clock-failure"), risk(), plan(), intent()),
            SubmissionState.SUBMITTED_UNKNOWN,
        )
        self.assertEqual(len(broker.calls), 1)
        self.assertEqual(safety.state, SafetyState.HALTED)
        self.assertEqual(
            self.ledger.events_for("clock-failure")[-1].event_type,
            "SUBMITTED_UNKNOWN",
        )

    def test_pre_submit_clock_failure_halts_without_reservation_or_broker_effect(self):
        safety = reconciled_safety()
        broker = FakeBroker(BrokerSubmitOutcome.ACKNOWLEDGED)

        def failing_clock():
            raise RuntimeError("clock failed before submission")

        service = ExecutionService(
            broker, self.ledger, safety, failing_clock, account_id="acct"
        )
        with self.assertRaises(SubmissionValidationError):
            service.submit(request("pre-clock-failure"), risk(), plan(), intent())
        self.assertEqual(safety.state, SafetyState.HALTED)
        self.assertIn("CLOCK_FAILURE", safety.blockers)
        self.assertEqual(broker.calls, [])
        self.assertEqual(self.ledger.events_for("pre-clock-failure"), ())

    def test_long_only_intent_allows_buy_and_reduction_but_blocks_naked_sell(self):
        buy_service = self.service()
        self.assertEqual(
            buy_service.submit(request("buy"), risk(), plan(), intent()),
            SubmissionState.ACKNOWLEDGED,
        )
        sell_service = self.service()
        self.assertEqual(
            sell_service.submit(
                request("sell", Decimal("-2")),
                risk(Decimal("-2")),
                plan(Decimal("-2")),
                intent(Decimal("-2")),
            ),
            SubmissionState.ACKNOWLEDGED,
        )

        live_safety = reconciled_safety()
        live_service = self.service(
            safety=live_safety, environment=BrokerEnvironment.LIVE
        )
        permit = new_order_permit(live_safety, order_id="naked")
        naked = TradeIntent(
            "intent", "target", "strategy", "acct", "snap",
            InstrumentId("NASDAQ", "AAPL", "USD"),
            Decimal(0), Decimal(0), Decimal(2), Decimal("-2"), NOW,
        )
        with self.assertRaises(SubmissionValidationError):
            live_service.submit(
                request("naked", Decimal("-2")),
                risk(Decimal("-2")),
                plan(Decimal("-2")),
                naked,
                permit,
            )
        self.assertEqual(len(live_service.broker.calls), 0)

    def test_terminal_ledger_failure_halts_and_never_retries_broker(self):
        class FailingLedger:
            def __init__(self, real):
                self.real = real
                self.append_count = 0

            def reserve_submission(self, *args):
                return self.real.reserve_submission(*args)

            def append(self, event):
                self.append_count += 1
                if self.append_count == 1:
                    raise sqlite3.OperationalError("deterministic write failure")
                self.real.append(event)

            def events_for(self, aggregate_id):
                return self.real.events_for(aggregate_id)

            def incomplete_submissions(self, account_id=None):
                return self.real.incomplete_submissions(account_id)

            def unresolved_unknown_submissions(self, account_id=None):
                return self.real.unresolved_unknown_submissions(account_id)

            def record_unknown_resolution(self, *args):
                return self.real.record_unknown_resolution(*args)

            def integrity_check(self):
                return self.real.integrity_check()

        safety = SafetyController()
        broker = FakeBroker(BrokerSubmitOutcome.ACKNOWLEDGED)
        service = ExecutionService(
            broker,
            FailingLedger(self.ledger),
            safety,
            lambda: NOW,
            account_id="acct",
        )
        order = request("persistence-failure")
        with self.assertRaises(PersistenceFailure):
            service.submit(order, risk(), plan(), intent())
        self.assertEqual(safety.state, SafetyState.HALTED)
        self.assertEqual(len(broker.calls), 1)
        with self.assertRaises(AlreadySubmitted):
            service.submit(order, risk(), plan(), intent())
        self.assertEqual(len(broker.calls), 1)

    def test_restart_recovers_started_as_unknown_and_blocks_arm(self):
        order = request("restart-order")
        prepared = LedgerEvent("restart-prepared", "PREPARED", order.client_order_id, NOW, {})
        self.ledger.reserve_submission(
            order.client_order_id,
            {"request": {"account_id": "acct"}, "permit": None},
            prepared,
            LedgerEvent("restart-started", "SUBMISSION_STARTED", order.client_order_id, NOW, {}),
            None,
        )
        self.ledger.close()
        self.ledger = SQLiteLedger(self.path)
        first_safety = SafetyController()
        ExecutionService(
            FakeBroker(BrokerSubmitOutcome.ACKNOWLEDGED),
            self.ledger,
            first_safety,
            lambda: NOW,
            account_id="acct",
        )
        self.assertEqual(
            self.ledger.events_for(order.client_order_id)[-1].event_type,
            "SUBMITTED_UNKNOWN",
        )
        self.assertIn(
            f"SUBMITTED_UNKNOWN:{order.client_order_id}", first_safety.blockers
        )

        self.ledger.close()
        self.ledger = SQLiteLedger(self.path)
        safety = SafetyController()
        service = ExecutionService(
            FakeBroker(BrokerSubmitOutcome.ACKNOWLEDGED),
            self.ledger,
            safety,
            lambda: NOW,
            account_id="acct",
        )
        with self.assertRaises(TypeError):
            safety.acknowledge_startup_recovery("op")
        ops = operator(safety, self.ledger)
        resolve_command = command(
            safety,
            OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            "op-resolve",
            "acct",
            client_order_id=order.client_order_id,
        )
        evidence = UnknownResolutionEvidence(
            UnknownResolutionResult.CONFIRMED_ABSENT, "broker inquiry", "case-1", NOW
        )
        with self.assertRaises(TypeError):
            service.resolve_unknown_submission(order.client_order_id, "raw", "raw", ops)
        mismatched_service = operator(safety)
        with self.assertRaises(ValueError):
            service.resolve_unknown_submission(
                order.client_order_id, resolve_command, evidence, mismatched_service
            )
        wrong_account = command(
            safety,
            OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            "op-wrong-account",
            "other-account",
            client_order_id=order.client_order_id,
        )
        with self.assertRaises(OperatorCommandRejected):
            service.resolve_unknown_submission(
                order.client_order_id, wrong_account, evidence, ops
            )
        self.assertIn(f"SUBMITTED_UNKNOWN:{order.client_order_id}", safety.blockers)
        wrong_target = command(
            safety,
            OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            "op-wrong-target",
            "acct",
            client_order_id="other-order",
        )
        with self.assertRaises(OperatorCommandRejected):
            service.resolve_unknown_submission(
                order.client_order_id, wrong_target, evidence, ops
            )
        self.assertEqual(self.ledger.events_for(wrong_target.command_id), ())
        self.assertEqual(
            self.ledger.events_for(order.client_order_id)[-1].event_type,
            "SUBMITTED_UNKNOWN",
        )
        service.resolve_unknown_submission(order.client_order_id, resolve_command, evidence, ops)
        command_terminal = self.ledger.events_for(resolve_command.command_id)[-1]
        self.assertEqual(
            command_terminal.payload["related_order_id"], order.client_order_id
        )
        self.assertIsNone(command_terminal.payload["related_permit_id"])
        resolution = self.ledger.events_for(order.client_order_id)[-1]
        self.assertEqual(resolution.event_type, "SUBMITTED_UNKNOWN_RESOLVED")
        self.assertEqual(resolution.payload["operator_command_id"], "op-resolve")
        with self.assertRaises(TypeError):
            safety.arm("op-arm", NOW)
        with self.assertRaises(SafetyGuardError):
            safety.complete_reconciliation(
                snapshot(), market(), "policy-v1", "deploy-v1", NOW
            )
        with self.assertRaises(InvalidPermit):
            new_order_permit(safety)

        self.ledger.close()
        self.ledger = SQLiteLedger(self.path)
        restarted_safety = SafetyController()
        ExecutionService(
            FakeBroker(BrokerSubmitOutcome.ACKNOWLEDGED),
            self.ledger,
            restarted_safety,
            lambda: NOW,
            account_id="acct",
        )
        self.assertFalse(restarted_safety.blockers)
        with self.assertRaises(TypeError):
            restarted_safety.arm("op-arm", NOW)
        with self.assertRaises(InvalidPermit):
            new_order_permit(restarted_safety)
        restarted_ops = operator(restarted_safety, self.ledger)
        restarted_ops.acknowledge_startup_recovery(
            command(
                restarted_safety,
                OperatorAction.ACKNOWLEDGE_STARTUP_RECOVERY,
                "op-recovery",
            )
        )
        restarted_safety.complete_reconciliation(
            snapshot(), market(), "policy-v1", "deploy-v1", NOW
        )
        restarted_ops.arm(command(restarted_safety, OperatorAction.ARM, "op-arm"))
        self.assertEqual(restarted_safety.state, SafetyState.TRADING)

    def test_unknown_resolution_write_failure_keeps_blocker(self):
        safety = SafetyController()
        service = self.service(BrokerSubmitOutcome.UNKNOWN, safety=safety)
        order = request("resolution-failure")
        service.submit(order, risk(), plan(), intent())

        def fail_resolution(*unused_args):
            raise LedgerPersistenceError("resolution write failed")

        self.ledger.record_unknown_resolution = fail_resolution
        ops = operator(safety, self.ledger)
        resolve_command = command(
            safety,
            OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            "op-resolve",
            "acct",
            client_order_id=order.client_order_id,
        )
        evidence = UnknownResolutionEvidence(
            UnknownResolutionResult.CONFIRMED_ABSENT, "broker inquiry", "case-2", NOW
        )
        with self.assertRaises(OperatorPersistenceFailure):
            service.resolve_unknown_submission(
                order.client_order_id, resolve_command, evidence, ops
            )
        self.assertIn(
            f"SUBMITTED_UNKNOWN:{order.client_order_id}", safety.blockers
        )
        self.assertNotIn(
            "SUBMITTED_UNKNOWN_RESOLVED",
            [event.event_type for event in self.ledger.events_for(order.client_order_id)],
        )

    def test_future_unknown_evidence_records_failed_and_keeps_blocker(self):
        safety = SafetyController()
        service = self.service(BrokerSubmitOutcome.UNKNOWN, safety=safety)
        order = request("future-evidence")
        service.submit(order, risk(), plan(), intent())
        ops = operator(safety, self.ledger)
        resolution = command(
            safety,
            OperatorAction.RESOLVE_SUBMITTED_UNKNOWN,
            "future-resolution",
            "acct",
            client_order_id=order.client_order_id,
        )
        evidence = UnknownResolutionEvidence(
            UnknownResolutionResult.CONFIRMED_ABSENT,
            "future broker inquiry",
            "future-case",
            NOW + timedelta(seconds=1),
        )
        with self.assertRaises(OperatorCommandRejected):
            service.resolve_unknown_submission(order.client_order_id, resolution, evidence, ops)
        self.assertIn(f"SUBMITTED_UNKNOWN:{order.client_order_id}", safety.blockers)
        self.assertEqual(
            [event.event_type for event in self.ledger.events_for(resolution.command_id)],
            ["OPERATOR_COMMAND_REQUESTED", "OPERATOR_COMMAND_FAILED"],
        )
        self.assertNotIn(
            "SUBMITTED_UNKNOWN_RESOLVED",
            [event.event_type for event in self.ledger.events_for(order.client_order_id)],
        )

    def test_broker_submit_result_invariants_and_sanitized_code(self):
        with self.assertRaises(ValueError):
            BrokerSubmitResult("ACKNOWLEDGED", "fake-id")
        with self.assertRaises(ValueError):
            BrokerSubmitResult(BrokerSubmitOutcome.ACKNOWLEDGED)
        with self.assertRaises(ValueError):
            BrokerSubmitResult(BrokerSubmitOutcome.REJECTED, "fake-id")
        with self.assertRaises(ValueError):
            BrokerSubmitResult(BrokerSubmitOutcome.REJECTED, detail_code="free text")


class ArchitectureTests(unittest.TestCase):
    @staticmethod
    def package_name(root: Path, path: Path) -> str:
        relative = path.relative_to(root).with_suffix("")
        return ".".join(("trader", *relative.parts[:-1]))

    @staticmethod
    def imported_modules(source: str, package: str) -> set[str]:
        tree = ast.parse(source)
        modules: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                module = f"{'.' * node.level}{node.module or ''}"
                if module:
                    modules.add(resolve_name(module, package) if node.level else module)
        return modules

    def test_domain_package_init_relative_cross_layer_import_is_normalized(self):
        root = Path("src/trader")
        package = self.package_name(root, root / "domain" / "__init__.py")
        self.assertEqual(package, "trader.domain")
        self.assertEqual(
            self.imported_modules("from ..adapters import persistence", package),
            {"trader.adapters"},
        )

    def test_dependency_boundaries(self):
        root = Path(__file__).parents[1] / "src" / "trader"
        forbidden_domain = {"sqlite3", "urllib", "http", "websocket", "os", "openai"}

        forbidden_layers = {
            "domain": {"trader.application", "trader.ports", "trader.adapters"},
            "application": {"trader.adapters"},
            "ports": {"trader.application", "trader.adapters"},
            "adapters": {"trader.application"},
        }
        for layer, forbidden in forbidden_layers.items():
            for path in (root / layer).rglob("*.py"):
                modules = self.imported_modules(
                    path.read_text(encoding="utf-8"), self.package_name(root, path)
                )
                violations = {
                    module
                    for module in modules
                    if any(module == prefix or module.startswith(f"{prefix}.") for prefix in forbidden)
                }
                self.assertFalse(violations, f"{path}: {violations}")
                if layer == "domain":
                    top_level = {module.split(".")[0] for module in modules}
                    self.assertFalse(
                        top_level & forbidden_domain,
                        f"{path}: {top_level & forbidden_domain}",
                    )
                elif layer == "application":
                    self.assertNotIn("sqlite3", modules, f"{path}: sqlite3")


if __name__ == "__main__":
    unittest.main()
