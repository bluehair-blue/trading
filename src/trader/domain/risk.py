from datetime import datetime
from decimal import Decimal

from .models import RiskDecision, RiskOutcome, RiskStage, TradeIntent


def eligibility(
    decision_id: str,
    policy_version: str,
    snapshot_id: str,
    evaluated_at: datetime,
    *,
    instrument_allowed: bool,
    session_open: bool,
    data_fresh: bool,
) -> RiskDecision:
    reasons = tuple(
        code for ok, code in (
            (instrument_allowed, "INSTRUMENT_NOT_ALLOWED"),
            (session_open, "SESSION_CLOSED"),
            (data_fresh, "STALE_DATA"),
        ) if not ok
    )
    return RiskDecision(
        decision_id, RiskStage.ELIGIBILITY, policy_version, snapshot_id, None, None, None,
        RiskOutcome.REJECTED if reasons else RiskOutcome.APPROVED, reasons, evaluated_at,
    )


def pre_trade_quantity_cap(
    decision_id: str,
    policy_version: str,
    snapshot_id: str,
    intent: TradeIntent,
    max_absolute_quantity: Decimal,
    evaluated_at: datetime,
) -> RiskDecision:
    if not isinstance(max_absolute_quantity, Decimal) or max_absolute_quantity < 0:
        raise ValueError("max_absolute_quantity must be a non-negative Decimal")
    original = intent.original_quantity
    approved = min(abs(original), max_absolute_quantity).copy_sign(original)
    outcome = (
        RiskOutcome.REJECTED if approved == 0
        else RiskOutcome.ADJUSTED if approved != original
        else RiskOutcome.APPROVED
    )
    reasons = () if outcome is RiskOutcome.APPROVED else ("QUANTITY_CAP",)
    return RiskDecision(
        decision_id, RiskStage.PRE_TRADE, policy_version, snapshot_id, intent.intent_id,
        original, approved,
        outcome, reasons, evaluated_at,
    )


def continuous_disarm(
    decision_id: str,
    policy_version: str,
    snapshot_id: str,
    evaluated_at: datetime,
    *,
    stale_market_data: bool = False,
    account_mismatch: bool = False,
    loss_limit_breached: bool = False,
    submitted_unknown: bool = False,
) -> RiskDecision:
    reasons = tuple(
        code for hit, code in (
            (stale_market_data, "STALE_MARKET_DATA"),
            (account_mismatch, "ACCOUNT_MISMATCH"),
            (loss_limit_breached, "LOSS_LIMIT_BREACHED"),
            (submitted_unknown, "SUBMITTED_UNKNOWN"),
        ) if hit
    )
    return RiskDecision(
        decision_id, RiskStage.CONTINUOUS, policy_version, snapshot_id, None, None, None,
        RiskOutcome.REJECTED if reasons else RiskOutcome.APPROVED, reasons, evaluated_at,
    )
