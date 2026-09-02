from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import NoReturn

from trader.application.backtest import PreviousCloseThresholdConfig
from trader.domain.accounting import AccountingPosition, AccountingSeed
from trader.domain.models import (
    InstrumentId,
    TradingEnvironment,
    require_id,
    require_utc,
)
from trader.research.manifest import RunSpec


__all__ = [
    "BacktestConfiguration",
    "BacktestData",
    "BacktestInputError",
    "HistoricalQuote",
    "TradingSession",
    "VALUATION_POLICY_VERSION",
    "ValidatedBacktest",
    "validate_backtest_inputs",
]


_SHA256 = re.compile(r"[0-9a-f]{64}")
_MAX_CONFIG_BYTES = 1_000_000
_MAX_DATA_BYTES = 100_000_000
VALUATION_POLICY_VERSION = "session-close-last-non-halted-bid-v1"


class BacktestInputError(ValueError):
    """The local immutable input does not satisfy the Dry replay contract."""


@dataclass(frozen=True)
class TradingSession:
    session_id: str
    business_date: date
    open_at: datetime
    close_at: datetime

    def __post_init__(self) -> None:
        require_id(self.session_id, "session_id")
        if type(self.business_date) is not date:
            raise BacktestInputError("business_date must be an exact date")
        require_utc(self.open_at, "session open_at")
        require_utc(self.close_at, "session close_at")
        if self.close_at <= self.open_at:
            raise BacktestInputError("session close_at must follow open_at")

    def contains(self, value: datetime) -> bool:
        require_utc(value, "calendar timestamp")
        return self.open_at <= value < self.close_at


@dataclass(frozen=True)
class HistoricalQuote:
    event_id: str
    session_id: str
    occurred_at: datetime
    available_at: datetime
    source_sequence: int
    ingest_sequence: int
    bid: Decimal
    ask: Decimal
    available_quantity: int
    halted: bool

    def __post_init__(self) -> None:
        for name in ("event_id", "session_id"):
            require_id(getattr(self, name), name)
        require_utc(self.occurred_at, "event occurred_at")
        require_utc(self.available_at, "event available_at")
        if self.available_at < self.occurred_at:
            raise BacktestInputError("event cannot be available before it occurs")
        for name in ("source_sequence", "ingest_sequence", "available_quantity"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise BacktestInputError(f"{name} must be a non-negative integer")
        if self.source_sequence == 0 or self.ingest_sequence == 0:
            raise BacktestInputError("event sequences must start at one")
        for name in ("bid", "ask"):
            value = getattr(self, name)
            if type(value) is not Decimal or not value.is_finite() or value <= 0:
                raise BacktestInputError(f"{name} must be a positive finite decimal")
            exponent = value.as_tuple().exponent
            if type(exponent) is int and exponent < -2:
                raise BacktestInputError(f"{name} supports at most two decimal places")
        if self.ask < self.bid:
            raise BacktestInputError("ask cannot be below bid")
        if type(self.halted) is not bool:
            raise BacktestInputError("halted must be a bool")

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal(2)


@dataclass(frozen=True)
class BacktestConfiguration:
    expected_data_sha256: str
    instrument: InstrumentId
    accounting_seed: AccountingSeed
    strategy: PreviousCloseThresholdConfig
    risk_policy_version: str
    max_order_quantity: Decimal
    cash_cap_minor: int
    exposure_cap_minor: int
    fee_buffer_minor: int
    partial_fill_cap: int | None
    slippage_bps: Decimal
    fee_bps: Decimal
    max_quote_age_seconds: int
    valuation_policy_version: str
    valuation_max_mark_age_seconds: int
    random_seed: int

    def __post_init__(self) -> None:
        if _SHA256.fullmatch(self.expected_data_sha256) is None:
            raise BacktestInputError("data_sha256 must be a lowercase SHA256 digest")
        if type(self.instrument) is not InstrumentId:
            raise BacktestInputError("instrument must be exact InstrumentId")
        if self.instrument.currency != "USD":
            raise BacktestInputError("the Dry runner currently supports USD only")
        if type(self.accounting_seed) is not AccountingSeed:
            raise BacktestInputError("account seed must be exact AccountingSeed")
        if type(self.strategy) is not PreviousCloseThresholdConfig:
            raise BacktestInputError("strategy config is malformed")
        require_id(self.risk_policy_version, "risk policy_version")
        if (
            type(self.max_order_quantity) is not Decimal
            or not self.max_order_quantity.is_finite()
            or self.max_order_quantity <= 0
            or self.max_order_quantity != self.max_order_quantity.to_integral_value()
        ):
            raise BacktestInputError("max_order_quantity must be positive integral shares")
        for name in ("cash_cap_minor", "exposure_cap_minor", "fee_buffer_minor"):
            value = getattr(self, name)
            if type(value) is not int or not 0 <= value <= (1 << 63) - 1:
                raise BacktestInputError(
                    f"{name} must be a non-negative signed 64-bit integer"
                )
        if self.partial_fill_cap is not None and (
            type(self.partial_fill_cap) is not int or self.partial_fill_cap <= 0
        ):
            raise BacktestInputError("partial_fill_cap must be positive or null")
        for name in ("slippage_bps", "fee_bps"):
            value = getattr(self, name)
            if (
                type(value) is not Decimal
                or not value.is_finite()
                or not Decimal(0) <= value <= Decimal(10_000)
            ):
                raise BacktestInputError(
                    f"{name} must be a finite decimal between 0 and 10000"
                )
        if type(self.max_quote_age_seconds) is not int or self.max_quote_age_seconds < 0:
            raise BacktestInputError("max_quote_age_seconds must be non-negative")
        try:
            timedelta(seconds=self.max_quote_age_seconds)
        except OverflowError as error:
            raise BacktestInputError(
                "max_quote_age_seconds exceeds the supported duration"
            ) from error
        if self.valuation_policy_version != VALUATION_POLICY_VERSION:
            raise BacktestInputError("valuation policy_version is unsupported")
        if (
            type(self.valuation_max_mark_age_seconds) is not int
            or self.valuation_max_mark_age_seconds < 0
        ):
            raise BacktestInputError("valuation max_mark_age_seconds must be non-negative")
        try:
            timedelta(seconds=self.valuation_max_mark_age_seconds)
        except OverflowError as error:
            raise BacktestInputError(
                "valuation max_mark_age_seconds exceeds the supported duration"
            ) from error
        if type(self.random_seed) is not int or not -(2**63) <= self.random_seed < 2**63:
            raise BacktestInputError("random_seed must be a signed 64-bit integer")


@dataclass(frozen=True)
class BacktestData:
    calendar_version: str
    sessions: tuple[TradingSession, ...]
    events: tuple[HistoricalQuote, ...]
    sha256: str

    def __post_init__(self) -> None:
        require_id(self.calendar_version, "calendar_version")
        if not self.sessions or any(type(item) is not TradingSession for item in self.sessions):
            raise BacktestInputError("at least one exact session is required")
        if not self.events or any(type(item) is not HistoricalQuote for item in self.events):
            raise BacktestInputError("at least one exact event is required")
        if _SHA256.fullmatch(self.sha256) is None:
            raise BacktestInputError("data digest is malformed")

    def business_date(self, value: datetime) -> date:
        matches = tuple(session for session in self.sessions if session.contains(value))
        if len(matches) != 1:
            raise BacktestInputError("timestamp is outside the explicit trading calendar")
        return matches[0].business_date


@dataclass(frozen=True)
class ValidatedBacktest:
    """Fully checked local inputs; creating this object performs no broker or ledger I/O."""

    run_spec: RunSpec
    config: BacktestConfiguration
    data: BacktestData


def _invalid_constant(value: str) -> NoReturn:
    raise BacktestInputError(f"non-finite JSON number is forbidden: {value}")


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise BacktestInputError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _read_json(path: Path, *, maximum_bytes: int) -> tuple[dict[str, object], bytes]:
    if path.is_symlink() or not path.is_file():
        raise BacktestInputError(f"input must be a regular non-symlink file: {path}")
    try:
        size = path.stat().st_size
    except OSError as error:
        raise BacktestInputError(f"input metadata could not be read: {path}") from error
    if size > maximum_bytes:
        raise BacktestInputError(f"input exceeds the size limit: {path}")
    try:
        raw = path.read_bytes()
        value: object = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_strict_object,
            parse_constant=_invalid_constant,
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BacktestInputError(f"input is not strict UTF-8 JSON: {path}") from error
    if type(value) is not dict:
        raise BacktestInputError(f"JSON root must be an object: {path}")
    return value, raw


def _source_sha256() -> str:
    trader_root = Path(__file__).resolve().parents[1]
    digest = sha256()
    paths = sorted(
        trader_root.rglob("*.py"),
        key=lambda item: item.relative_to(trader_root).as_posix(),
    )
    if not paths:
        raise BacktestInputError("trader source snapshot is empty")
    try:
        for path in paths:
            if path.is_symlink() or not path.is_file():
                raise BacktestInputError("trader source snapshot contains a non-regular file")
            relative = path.relative_to(trader_root).as_posix().encode("utf-8")
            digest.update(relative)
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
    except OSError as error:
        raise BacktestInputError("trader source snapshot could not be read") from error
    return digest.hexdigest()


def _object(value: object, name: str) -> dict[str, object]:
    if type(value) is not dict:
        raise BacktestInputError(f"{name} must be an object")
    return value


def _array(value: object, name: str) -> list[object]:
    if type(value) is not list:
        raise BacktestInputError(f"{name} must be an array")
    return value


def _exact_keys(value: Mapping[str, object], expected: set[str], name: str) -> None:
    if set(value) != expected:
        missing = sorted(expected - set(value))
        extra = sorted(set(value) - expected)
        raise BacktestInputError(f"{name} keys mismatch; missing={missing}, extra={extra}")


def _string(value: object, name: str) -> str:
    if type(value) is not str or not value.strip():
        raise BacktestInputError(f"{name} must be a non-empty string")
    return value


def _integer(value: object, name: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise BacktestInputError(f"{name} must be an integer >= {minimum}")
    return value


def _signed_integer(value: object, name: str) -> int:
    if type(value) is not int or not -(2**63) <= value < 2**63:
        raise BacktestInputError(f"{name} must be a signed 64-bit integer")
    return value


def _decimal(value: object, name: str, *, minimum: Decimal | None = None) -> Decimal:
    if type(value) is not str:
        raise BacktestInputError(f"{name} must be a decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise BacktestInputError(f"{name} is not a decimal") from error
    if not parsed.is_finite() or (minimum is not None and parsed < minimum):
        raise BacktestInputError(f"{name} is outside its allowed range")
    return parsed


def _utc(value: object, name: str) -> datetime:
    text = _string(value, name)
    normalized = f"{text[:-1]}+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise BacktestInputError(f"{name} must be ISO-8601 UTC") from error
    try:
        require_utc(parsed, name)
    except ValueError as error:
        raise BacktestInputError(f"{name} must be timezone-aware UTC") from error
    return parsed


def _instrument(value: object, name: str) -> InstrumentId:
    payload = _object(value, name)
    _exact_keys(payload, {"market", "symbol", "currency"}, name)
    return InstrumentId(
        _string(payload["market"], f"{name}.market"),
        _string(payload["symbol"], f"{name}.symbol"),
        _string(payload["currency"], f"{name}.currency"),
    )


def _parse_config(payload: dict[str, object]) -> BacktestConfiguration:
    _exact_keys(
        payload,
        {
            "schema_version",
            "mode",
            "data_sha256",
            "instrument",
            "account",
            "strategy",
            "risk",
            "simulation",
            "valuation",
            "random_seed",
        },
        "config",
    )
    if payload["schema_version"] != 2 or payload["mode"] != "DRY":
        raise BacktestInputError("config requires schema_version 2 and mode DRY")
    instrument = _instrument(payload["instrument"], "instrument")

    account = _object(payload["account"], "account")
    _exact_keys(account, {"account_id", "cash", "positions"}, "account")
    positions: list[AccountingPosition] = []
    for index, value in enumerate(_array(account["positions"], "account.positions")):
        position = _object(value, f"account.positions[{index}]")
        _exact_keys(position, {"instrument", "quantity"}, f"account.positions[{index}]")
        position_instrument = _instrument(
            position["instrument"], f"account.positions[{index}].instrument"
        )
        if position_instrument != instrument:
            raise BacktestInputError("seed positions must belong to the configured instrument")
        positions.append(
            AccountingPosition(
                position_instrument,
                _decimal(
                    position["quantity"],
                    f"account.positions[{index}].quantity",
                    minimum=Decimal(0),
                ),
            )
        )
    if positions:
        raise BacktestInputError("Phase A requires zero starting positions")
    seed = AccountingSeed(
        _string(account["account_id"], "account.account_id"),
        TradingEnvironment.SIMULATED,
        instrument.currency,
        "dry-accounting-long-only-usd-v1",
        _decimal(account["cash"], "account.cash", minimum=Decimal(0)),
        tuple(positions),
    )

    strategy = _object(payload["strategy"], "strategy")
    _exact_keys(strategy, {"version", "threshold", "target_quantity"}, "strategy")
    strategy_config = PreviousCloseThresholdConfig(
        _string(strategy["version"], "strategy.version"),
        _decimal(strategy["threshold"], "strategy.threshold", minimum=Decimal(0)),
        _decimal(
            strategy["target_quantity"],
            "strategy.target_quantity",
            minimum=Decimal(0),
        ),
    )

    risk = _object(payload["risk"], "risk")
    _exact_keys(
        risk,
        {
            "policy_version",
            "max_order_quantity",
            "cash_cap_minor",
            "exposure_cap_minor",
            "fee_buffer_minor",
        },
        "risk",
    )
    simulation = _object(payload["simulation"], "simulation")
    _exact_keys(
        simulation,
        {
            "partial_fill_cap",
            "slippage_bps",
            "fee_bps",
            "max_quote_age_seconds",
        },
        "simulation",
    )
    partial = simulation["partial_fill_cap"]
    if partial is not None:
        partial = _integer(partial, "simulation.partial_fill_cap", minimum=1)
    valuation = _object(payload["valuation"], "valuation")
    _exact_keys(
        valuation,
        {"policy_version", "max_mark_age_seconds"},
        "valuation",
    )
    return BacktestConfiguration(
        _string(payload["data_sha256"], "data_sha256"),
        instrument,
        seed,
        strategy_config,
        _string(risk["policy_version"], "risk.policy_version"),
        _decimal(
            risk["max_order_quantity"],
            "risk.max_order_quantity",
            minimum=Decimal(0),
        ),
        _integer(risk["cash_cap_minor"], "risk.cash_cap_minor"),
        _integer(risk["exposure_cap_minor"], "risk.exposure_cap_minor"),
        _integer(risk["fee_buffer_minor"], "risk.fee_buffer_minor"),
        partial,
        _decimal(
            simulation["slippage_bps"],
            "simulation.slippage_bps",
            minimum=Decimal(0),
        ),
        _decimal(
            simulation["fee_bps"],
            "simulation.fee_bps",
            minimum=Decimal(0),
        ),
        _integer(
            simulation["max_quote_age_seconds"],
            "simulation.max_quote_age_seconds",
        ),
        _string(valuation["policy_version"], "valuation.policy_version"),
        _integer(
            valuation["max_mark_age_seconds"],
            "valuation.max_mark_age_seconds",
        ),
        _signed_integer(payload["random_seed"], "random_seed"),
    )


def _parse_data(payload: dict[str, object], raw: bytes) -> BacktestData:
    _exact_keys(
        payload,
        {"schema_version", "calendar_version", "provenance", "sessions", "events"},
        "data",
    )
    if payload["schema_version"] != 1:
        raise BacktestInputError("data schema_version must be 1")
    provenance = _object(payload["provenance"], "provenance")
    _exact_keys(provenance, {"source", "license"}, "provenance")
    _string(provenance["source"], "provenance.source")
    _string(provenance["license"], "provenance.license")

    sessions: list[TradingSession] = []
    for index, value in enumerate(_array(payload["sessions"], "sessions")):
        item = _object(value, f"sessions[{index}]")
        _exact_keys(
            item,
            {"session_id", "business_date", "open_at", "close_at"},
            f"sessions[{index}]",
        )
        try:
            business_date = date.fromisoformat(
                _string(item["business_date"], f"sessions[{index}].business_date")
            )
        except ValueError as error:
            raise BacktestInputError("session business_date must be ISO-8601") from error
        sessions.append(
            TradingSession(
                _string(item["session_id"], f"sessions[{index}].session_id"),
                business_date,
                _utc(item["open_at"], f"sessions[{index}].open_at"),
                _utc(item["close_at"], f"sessions[{index}].close_at"),
            )
        )
    if len(sessions) < 2:
        raise BacktestInputError("previous-close strategy requires at least two sessions")
    if len({item.session_id for item in sessions}) != len(sessions):
        raise BacktestInputError("session_id must be unique")
    if len({item.business_date for item in sessions}) != len(sessions):
        raise BacktestInputError("session business_date must be unique")
    for prior_session, current_session in zip(sessions, sessions[1:]):
        if (
            current_session.open_at < prior_session.close_at
            or current_session.business_date <= prior_session.business_date
        ):
            raise BacktestInputError("sessions must be sorted, non-overlapping, and increasing")

    by_session = {item.session_id: item for item in sessions}
    events: list[HistoricalQuote] = []
    for index, value in enumerate(_array(payload["events"], "events")):
        item = _object(value, f"events[{index}]")
        _exact_keys(
            item,
            {
                "event_id",
                "session_id",
                "occurred_at",
                "available_at",
                "source_sequence",
                "ingest_sequence",
                "bid",
                "ask",
                "available_quantity",
                "halted",
            },
            f"events[{index}]",
        )
        halted = item["halted"]
        if type(halted) is not bool:
            raise BacktestInputError(f"events[{index}].halted must be a bool")
        event = HistoricalQuote(
            _string(item["event_id"], f"events[{index}].event_id"),
            _string(item["session_id"], f"events[{index}].session_id"),
            _utc(item["occurred_at"], f"events[{index}].occurred_at"),
            _utc(item["available_at"], f"events[{index}].available_at"),
            _integer(item["source_sequence"], f"events[{index}].source_sequence", minimum=1),
            _integer(item["ingest_sequence"], f"events[{index}].ingest_sequence", minimum=1),
            _decimal(item["bid"], f"events[{index}].bid", minimum=Decimal(0)),
            _decimal(item["ask"], f"events[{index}].ask", minimum=Decimal(0)),
            _integer(item["available_quantity"], f"events[{index}].available_quantity"),
            halted,
        )
        session = by_session.get(event.session_id)
        if session is None:
            raise BacktestInputError("event references an unknown session")
        if not session.contains(event.occurred_at) or not session.contains(event.available_at):
            raise BacktestInputError("event is outside its declared session")
        events.append(event)
    if not events:
        raise BacktestInputError("data requires at least one event")
    if len({item.event_id for item in events}) != len(events):
        raise BacktestInputError("event_id must be unique")
    for expected, event in enumerate(events, start=1):
        if event.ingest_sequence != expected or event.source_sequence != expected:
            raise BacktestInputError("event sequences must be globally contiguous from one")
    for prior_event, current_event in zip(events, events[1:]):
        if (current_event.available_at, current_event.ingest_sequence) <= (
            prior_event.available_at,
            prior_event.ingest_sequence,
        ):
            raise BacktestInputError(
                "events must be sorted by availability and ingest sequence"
            )
        if (current_event.occurred_at, current_event.source_sequence) <= (
            prior_event.occurred_at,
            prior_event.source_sequence,
        ):
            raise BacktestInputError("source events must be strictly increasing")
    session_ids_with_events = {item.session_id for item in events}
    if session_ids_with_events != set(by_session):
        raise BacktestInputError("every declared session requires at least one event")
    return BacktestData(
        _string(payload["calendar_version"], "calendar_version"),
        tuple(sessions),
        tuple(events),
        sha256(raw).hexdigest(),
    )


def validate_backtest_inputs(
    config_path: str | Path,
    data_path: str | Path,
    *,
    code_commit: str,
) -> ValidatedBacktest:
    """Validate all local inputs without creating a database or simulated order."""
    config_payload, config_raw = _read_json(
        Path(config_path), maximum_bytes=_MAX_CONFIG_BYTES
    )
    data_payload, data_raw = _read_json(Path(data_path), maximum_bytes=_MAX_DATA_BYTES)
    config = _parse_config(config_payload)
    data = _parse_data(data_payload, data_raw)
    if data.sha256 != config.expected_data_sha256:
        raise BacktestInputError("historical data SHA256 does not match the config")
    maximum_buy_quantity = min(
        config.strategy.target_quantity, config.max_order_quantity
    )
    maximum_buy_fee = (
        max(event.ask for event in data.events)
        * maximum_buy_quantity
        * config.fee_bps
        / Decimal(10_000)
    )
    required_fee_buffer_minor = int(
        (maximum_buy_fee * 100).to_integral_value(rounding=ROUND_CEILING)
    )
    if config.fee_buffer_minor < required_fee_buffer_minor:
        raise BacktestInputError(
            "risk fee_buffer_minor cannot cover the configured maximum simulated buy fee"
        )
    instrument_payload = json.dumps(
        {
            "currency": config.instrument.currency,
            "market": config.instrument.market,
            "symbol": config.instrument.symbol,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    run_spec = RunSpec(
        code_commit=code_commit,
        source_sha256=_source_sha256(),
        strategy_version=config.strategy.version,
        config_sha256=sha256(config_raw).hexdigest(),
        account_seed_sha256=config.accounting_seed.fingerprint(),
        data_snapshot_id=f"sha256-{data.sha256}",
        universe_snapshot_id=(
            f"sha256-{sha256(instrument_payload.encode('utf-8')).hexdigest()}"
        ),
        calendar_version=data.calendar_version,
        corporate_action_version="fail-closed-no-corporate-actions-v1",
        fee_model_version="simulated-fee-bps-v1",
        slippage_model_version="simulated-slippage-bps-v1",
        fx_model_version="usd-only-v1",
        accounting_model_version=config.accounting_seed.policy_version,
        valuation_policy_version=config.valuation_policy_version,
        valuation_max_mark_age_seconds=config.valuation_max_mark_age_seconds,
        random_seed=config.random_seed,
        decision_cutoff_policy=(
            "previous-close-current-session-first-quote-next-event-fill-v1"
        ),
        sample_started_at=data.events[0].available_at,
        sample_completed_at=data.events[-1].available_at,
    )
    return ValidatedBacktest(run_spec, config, data)
