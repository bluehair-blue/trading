# Phase A performance and valuation plan

> Status: PROPOSED. Do not implement until the decision set below is approved.

## Outcome

Extend the existing Offline Dry runner with deterministic performance accounting and session-close
valuation. Completion proves that synthetic fills and marks produce internally consistent numbers;
it does not prove profitability or authorize Paper, Shadow, or Live operation.

## Minimal context

This file is the implementation brief. Read only the source file owned by the current work item and
its tests. Consult these documents only if a contract conflict is found:

- `README.md`: architecture and mode boundaries
- `docs/DRY_BACKTEST_READY.md`: current Dry guarantees and exclusions
- `docs/PHASE1B_DECISIONS.md`: accounting and Paper/Live blocking boundaries

Do not load the external strategy-review report, `OPERATIONS.md`, `TERMS_REVIEW.md`, or
`credentials.md` for Phase A.

## Proposed decision set

Approve or replace this set as one unit before implementation:

1. Scope is USD, long-only, integer shares, no external cash flows, and zero starting positions.
2. Cost basis is weighted average. Buy fees enter basis; sell fees reduce proceeds.
3. Each declared session `close_at` is a valuation checkpoint. This is not an attested official
   exchange calendar in Phase A.
4. A long position uses the last non-halted bid whose `occurred_at` and `available_at` are no later
   than the checkpoint and whose `checkpoint - occurred_at` age is within required
   `valuation.max_mark_age_seconds`.
5. A flat position needs no mark. A missing or stale required mark makes evaluation incomplete;
   prices are never carried forward.
6. Open positions are marked at sample end. No forced liquidation, hidden order, or synthetic fill
   is created.
7. Cash uses trade-date accounting. Interest, tax, settlement, FX, and corporate actions are out of
   scope.
8. Phase A outputs starting/ending equity, realized/unrealized/net PnL, cumulative return,
   session-close equity, maximum session-close drawdown, gross traded value, fees, fills, and gross
   turnover.
9. Gross turnover is gross traded value divided by the arithmetic mean of complete equity
   observations, including starting equity. It is null when the denominator is unavailable or zero.
10. CAGR, annualized volatility, Sharpe, alpha, and benchmark-relative metrics are excluded.

## Status semantics

- Existing `RunResult.status` remains execution-only: `SUCCEEDED` or `FAILED`.
- Performance output adds `evaluation_status`: `COMPLETE` or `INCOMPLETE`.
- Incomplete reasons are structured and initially limited to `MISSING_MARK`, `STALE_MARK`, and
  `ACCOUNTING_INVARIANT`.
- Synthetic/reference runs may be mathematically `COMPLETE`, but their evidence use is fixed to
  `REFERENCE_ONLY`. Code must not create an approved or promotion-eligible state.
- An incomplete run may retain auditable snapshots, but aggregate return, drawdown, and turnover
  are null.

## Required invariants

For the Phase A scope:

```text
ending_quantity = buys - sells
ending_cash = starting_cash - buy_notional - buy_fees + sell_notional - sell_fees
ending_equity = ending_cash + ending_quantity * eligible_mark
net_pnl = ending_equity - starting_equity
net_pnl = realized_pnl + unrealized_pnl
```

All calculations use finite `Decimal` values. The performance projection is pure and derives from
the immutable accounting seed, persisted broker lifecycle facts, and validated valuation marks. It
must not change order, risk, broker, or ledger semantics.

## Work order

Complete the work sequentially. Do not begin the next item while the current gate is red.

### A1. Pure performance projection

Ownership:

- `src/trader/domain/performance.py`
- `tests/test_performance.py`

Deliver:

- weighted-average basis and fee treatment
- realized/unrealized/net PnL
- immutable valuation snapshot and performance projection
- cumulative return, session-close drawdown, and gross turnover
- invariant validation and canonical serialization

Gate: hand-calculated buy-only, multiple-buy, partial-sell, full-exit, no-trade, and invalid-mark
tests pass without importing adapters or entrypoints.

### A2. Input and output contracts

Ownership:

- `src/trader/research/backtest_input.py`
- `src/trader/research/manifest.py` only if a new policy version must enter `RunSpec`
- `src/trader/application/backtest.py`
- related tests

Deliver:

- reject non-zero starting positions for Phase A
- require and hash the valuation policy and maximum mark age
- add performance output, evaluation status, reasons, and `REFERENCE_ONLY` evidence use
- keep execution status independent from evaluation status

Gate: strict JSON, canonical hash, invalid policy, non-zero seed, and complete/incomplete output
tests pass.

### A3. Runner integration and replay

Ownership:

- `src/trader/entrypoints/backtest.py`
- `tests/test_backtest.py`

Deliver:

- select marks only from information available by each declared session close
- create no forced liquidation or synthetic fill
- fold performance from persisted fills and validated marks
- repeat the projection after ledger reopen and require equality
- preserve the existing order path and all network/live-import guards

Gate: future quotes cannot change prior decisions, orders, or equity snapshots; missing/stale marks
produce `SUCCEEDED + INCOMPLETE`; two fresh runs have equal canonical performance output.

### A4. Truthful documentation and full QA

Ownership:

- `docs/DRY_BACKTEST_READY.md`
- `docs/PHASE1B_DECISIONS.md`
- `README.md` only when its checkpoint statement changes

Deliver:

- state that the current reference value is the previous session's last non-halted midpoint, not an
  official close
- remove the stale statement that the Dry runner is absent
- document the Phase A metric definitions, incomplete behavior, and excluded metrics
- retain the explicit no-profitability and no-Paper/Live claims

Gate:

```powershell
uv sync --locked --dev
uv lock --check
uv run python scripts/verify.py
git diff --check
```

Required result: all tests pass, branch coverage remains at least 70%, Ruff and secret scan pass,
and mypy reports zero source errors. No database, result, coverage, or cache artifact is committed.

## Explicit deferrals

Do not add these in Phase A:

- official exchange-calendar attestation or close-driven DAY expiry
- licensed market-data ingestion or provider abstractions
- corporate actions, security identity, or point-in-time universe
- holdout, walk-forward, trial registry, or parameter optimization
- cost/latency/liquidity scenario orchestration
- Paper, Shadow, Live, credentials, network access, or promotion logic
- decision-reason audit expansion unrelated to performance correctness
- launcher, lockfile, runtime, or Git commit attestation expansion
- dashboards, generic event buses, data lakes, or plugin frameworks

Add a deferred item only when its own phase has approved inputs and an acceptance plan.

## Completion handoff

Report only:

1. adopted policy values and any deviation from this plan;
2. changed files by work item;
3. exact QA results;
4. incomplete or deferred items;
5. confirmation that no Paper/Live authority or profitability claim was introduced.
