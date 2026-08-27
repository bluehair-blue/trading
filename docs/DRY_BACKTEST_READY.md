# Dry backtest ready boundary

> 2026-08-27 checkpoint. This is the boundary immediately before selecting concrete experiment
> inputs and adding a backtest entrypoint. It is not evidence of strategy performance or broker
> connectivity.

## Implemented foundation

The reusable deterministic path is now present:

```text
StrategyDecision lineage
  -> PositionTarget / virtual allocation
  -> TradeIntent / risk / immutable OrderRequest
  -> OrderCoordinator / SQLite reservation and submission FSM
  -> SimulatedBroker / typed broker lifecycle facts
  -> atomic fact + incremental risk release
  -> replayed BrokerOrder and Dry cash/position accounting
  -> ledger digest + RunResult
```

- `SimulatedBroker` emits deterministic opened, fill, cancel, expiry, and rejection facts and uses
  an injected business-date policy and virtual time.
- SQLite derives reservation releases from typed facts. Callers cannot choose release amounts.
  Duplicate identical facts are no-ops; conflicts, out-of-order facts, or terminal continuation
  fail closed. Reopen repeats the semantic fold rather than trusting process memory, and an
  account/environment-scoped reader supplies the persisted facts to Dry accounting replay.
- `AccountingSeed` and lifecycle facts deterministically replay long-only USD cash and positions.
  Overdrafts, shorts, scope changes, and currency changes fail closed.
- `RunSpec` fingerprints immutable experiment inputs before execution. `RunResult` records status
  and content digests separately, so runtime identity and timestamps do not change the input key.

## Four inputs required before the first Dry run

1. **Strategy** — strategy/version, evaluation and rebalance cadence, signal cutoff, target rule,
   and portfolio/risk budgets.
2. **Immutable data** — market-data snapshot and universe IDs, interval, correction policy,
   corporate-action treatment, missing/stale/reordered-data behavior, and license provenance.
3. **Trading calendar** — source/version, holidays, early closes, DST/session rules, business-date
   mapping, and DAY expiry policy.
4. **Accounting start and policy** — initial cash/positions, fee and tax model, FX and settlement
   rules, corporate-action corrections, and mark-to-market policy/version.

After these four choices are fixed, the next smallest change is one composition function that reads
a `RunSpec`, replays the immutable input stream under `VirtualClock`, and emits `RunResult`. A generic
event bus, plugin strategy hierarchy, database projection framework, or live endpoint is not needed.

## Deliberately outside this checkpoint

- No historical dataset, strategy, or performance claim has been selected or executed.
- No Kiwoom credential, production/paper order URL, request builder, cancel endpoint, or network
  mutation is connected.
- Durable authoritative paper/live cash, position, tax, FX, settlement, and corporate-action
  projections still require broker reconciliation policy.
- Paper readiness separately requires official-contract refresh, authenticated operator control,
  long-running read-only verification, WebSocket recovery proof, shared rate-limit ownership,
  durable cancel recovery, and off-host backup operations.
