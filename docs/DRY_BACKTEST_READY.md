# Offline Dry backtest

> 2026-09-02 checkpoint. The local, credential-free Dry backtest entrypoint and deterministic
> Phase A performance valuation are implemented. This is an execution/accounting validation
> boundary, not evidence of profitability, strategy quality, or broker connectivity.

## Implemented boundary

`scripts/backtest.py` exposes `validate` and `run` commands that compose the existing typed
strategy, risk, simulated broker, ledger, and accounting contracts:

```text
raw config + raw quotes
  -> strict validation + data SHA-256 binding
  -> RunSpec fingerprint
  -> VirtualClock + previous-close reference strategy
  -> TradeIntent -> RiskDecision -> ExecutionPlan -> OrderRequest
  -> SimulatedBroker lifecycle facts
  -> SQLite append-only ledger + SIMULATED accounting fold
  -> session-close and sample-end valuation -> Phase A performance projection
  -> RunResult (ledger_sha256, output_sha256)
  -> ledger verification and lifecycle/accounting/performance reopen/replay check
```

- The config's `data_sha256` must equal the SHA-256 of the quotes file's exact raw bytes. The
  `RunSpec` also binds the declared code commit, an exact executed `src/trader` source-tree digest,
  raw config, account seed, data snapshot, calendar, model versions, and random seed. A dirty tree
  therefore cannot reuse a clean tree's reproducibility fingerprint merely by declaring its commit.
- Config schema version 2 requires `valuation.policy_version` and
  `valuation.max_mark_age_seconds`; both enter the canonical `RunSpec` and therefore its SHA-256
  fingerprint. Phase A accepts only `session-close-last-non-halted-bid-v1`.
- Inputs are strict UTF-8 JSON regular files. Duplicate keys, missing keys, unknown keys, invalid
  types, non-finite numbers, duplicate IDs, unknown sessions, out-of-session events, reordered or
  non-contiguous source/ingest sequences, and overlapping or unsorted sessions are rejected.
- Phase A requires USD, long-only integral shares, zero starting positions, and no external cash
  flows. A non-empty starting-position list is rejected before ledger creation.
- The reference strategy is `previous-close-threshold-v1`: on the first non-halted quote of a
  session, compare the previous session's last non-halted midpoint with the configured threshold
  and emit a share target. That value is a fixture reference, not an attested official exchange
  close or a selected investment strategy.
- A decision's current quote is used only to choose the LIMIT price (ask for BUY, bid for SELL).
  The order is submitted after that quote has been processed, so a fill can only be caused by a
  subsequent quote event. Fees, slippage, partial fills, DAY expiry, and sample-end cancellation
  follow the configured simulator policy.
- Fee and slippage rates are bounded to 0--10000 basis points. Validation also requires the risk
  fee buffer to cover the maximum configured simulated BUY fee before any ledger is created.
- The whole path is `TradingEnvironment.SIMULATED`; no credentials, Kiwoom endpoint, or network
  adapter is reachable.
- Typed order/risk/lifecycle facts and accounting invariants are enforced. The ledger is verified
  before close, reopened, verified again, and replayed to prove the same lifecycle facts and
  accounting and performance projections.

## Phase A valuation and metrics

Every declared session `close_at` is a valuation checkpoint, not an attested exchange-calendar
close. Open positions are also marked at sample end without forced liquidation, hidden orders, or
synthetic fills. A checkpoint uses only the same session's last non-halted bid whose `occurred_at`
and `available_at` are no later than the checkpoint and whose age does not exceed
`valuation.max_mark_age_seconds`; quotes are not carried across sessions. Flat positions need no
mark.

Buy fees enter weighted-average cost basis and sell fees reduce proceeds. Cash uses trade-date
accounting. Projection arithmetic uses a fixed 34-significant-digit, half-even Decimal context so
ambient process settings cannot change canonical output. The canonical performance output contains starting and sample-end equity,
session-close equity snapshots, realized/unrealized/net PnL, cumulative return, maximum
session-close drawdown, gross traded value, total fees, fill count, and gross turnover. Cumulative
return is `net_pnl / starting_equity`. Maximum drawdown is the largest positive peak-to-trough
fraction across starting equity and complete session-close observations. Gross turnover is gross
traded value divided by the arithmetic mean of complete equity observations, including starting
equity; it is null when that mean is unavailable or zero.

`RunResult.status` remains execution-only (`SUCCEEDED` or `FAILED`). Performance independently uses
`evaluation_status` (`COMPLETE` or `INCOMPLETE`) and structured `MISSING_MARK`, `STALE_MARK`, or
`ACCOUNTING_INVARIANT` reasons. An incomplete evaluation retains auditable snapshots where
possible, but cumulative return, maximum drawdown, and gross turnover are null. Every Phase A
performance result is fixed to `evidence_use: REFERENCE_ONLY`; no approved or promotion-eligible
state exists.

## Validate and run the synthetic fixture

Run these commands from the repository root. `validate` creates no database or simulated order.
`run` requires fresh ledger and result paths; the temporary directory below supplies unique paths.

```powershell
$rev = git rev-parse HEAD
uv run python scripts/backtest.py validate `
  --config examples/dry/backtest.json `
  --data examples/dry/quotes.json `
  --code-commit $rev

$dry = Join-Path $env:TEMP ("trading-dry-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $dry | Out-Null
uv run python scripts/backtest.py run `
  --config examples/dry/backtest.json `
  --data examples/dry/quotes.json `
  --ledger (Join-Path $dry "ledger.sqlite") `
  --result (Join-Path $dry "result.json") `
  --code-commit $rev
```

The checked-in `examples/dry/backtest.json` and `examples/dry/quotes.json` are synthetic project
fixtures. They must not be used to claim profitability, production readiness, or any other
performance result.

`RunResult.ledger_sha256` is the canonical content digest of that particular SQLite ledger. It
can differ between fresh runs because runtime UUIDs and `recorded_at` timestamps are written to the
ledger. `RunResult.output_sha256` hashes canonical, runtime-independent output and is the
repeatability key; compare it across fresh runs with the same inputs and code commit.

If validated execution fails, the CLI exits with status 2 and writes a terminal `FAILED` result
envelope with `output: null`. When the retained forensic ledger passes full verification, that
result also binds its digest. Input validation failure creates neither a ledger nor a result.

## Outside this boundary

- Selecting and approving a real strategy, licensed historical data, production calendar, or
  production accounting/tax/FX/corporate-action policy remains outstanding.
- Holdout, walk-forward, trial/parameter governance, cost/latency/liquidity scenarios, bias
  analysis, and any performance conclusion remain outstanding; this fixture provides no
  performance claim.
- CAGR, annualized volatility, Sharpe, alpha, and benchmark-relative metrics are intentionally
  excluded.
- Paper/live credentials, Kiwoom REST/WebSocket contracts, order/cancel/replace/reconcile
  endpoints, authenticated operator control, durable production projections, off-host backup, and
  deployment/rollback operations remain outside scope.
- A successful offline Dry run does not authorize paper or live operation and does not create a
  live `TradingPermit`.
