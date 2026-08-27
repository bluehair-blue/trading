# Phase 1B decisions

> 2026-08-27 implementation checkpoint. These are code-enforced contracts, not an approval to
> connect paper or live order endpoints.

## B1 — common execution safety

- `AccountProcessLock` is a non-blocking, cross-platform, account-scoped lock bound to the resolved
  ledger runtime identity. The internal alias is used only to derive a SHA-256 filename. Runtime
  composition acquires the lock before ledger verification, token health, WebSocket, or broker
  queries. A second process performs no external initialization.
- `SIMULATED`, `PAPER`, and `LIVE` share the same safety evidence, market price-band validation,
  monotonic clock checks, short-lived `TradingPermit`, `OrderCoordinator`, and ledger reservation
  path. A permit is bound to one environment and cannot confer LIVE authority from another mode.
- A `NEW_ORDER` permit is bound to one `client_order_id`, `risk_decision_id`, and
  `execution_plan_id`. The permit, immutable request, `PREPARED`, `SUBMISSION_STARTED`, and exact
  signed-int64 risk reservation are committed atomically. A permit cannot be consumed twice.
- Market evidence is part of the execution plan and must match the permit, current reconciliation
  evidence, pricing policy version, freshness window, and approved price band.
- Reconciliation fixes an immutable account reservation state and versioned risk-reservation
  policy. Callers cannot supply capacity terms to `submit`; the trusted coordinator derives cash,
  exposure, fee buffer, and per-instrument sell quantity from that evidence. ACK and UNKNOWN retain
  the reservation. Only a definitive rejection or broker-proven `CONFIRMED_ABSENT` releases it.
- SQLite schema version is `10`. The database and reopen validation enforce
  `PREPARED -> SUBMISSION_STARTED -> ACKNOWLEDGED | SUBMISSION_REJECTED | SUBMITTED_UNKNOWN`, a
  single terminal state, reservation relationships, immutable audit events, and known migrations
  from the exact supported legacy schemas.

## B2 — Kiwoom read-only contracts

- Credential profiles explicitly separate mock and live. Secrets and real account numbers exist
  only in adapter runtime objects with redacted representations. The loader accepts one explicit
  JSON path, rejects duplicate keys, symlinks, non-regular or oversized files, and insecure POSIX
  permissions. It never falls back to environment variables, keyrings, or network discovery.
- Token health is fail-closed on profile/environment mismatch, clock errors, future issuance,
  expiry, or insufficient remaining lifetime. Health evidence stores provenance and a token hash,
  not token material.
- The read-only mapper independently implements official `ust21070`, `ust21110`, and `ust21150`
  required fields. It requires integer `return_code == 0`, rejects duplicate JSON keys and missing
  or malformed required values, accepts additive fields, follows every page, detects repeated or
  missing cursors, enforces a page cap, and preserves response hashes and request windows.
- `AccountObservation` contains currency cash/buying power, instrument positions, daily broker
  orders, authentication evidence, component windows, pagination evidence, and quality. Only a
  complete, bounded observation can be used for reconciliation.
- Reconciliation compares internal expected cash, positions, daily orders, unresolved UNKNOWN, and
  manual activity. Any mismatch, ambiguity, or incomplete observation blocks readiness; it is not
  auto-corrected.
- One `KiwoomWebSocketSupervisor` owns each account/token session and enforces the 200-symbol cap.
  Gap, heartbeat failure, disconnect, required-consumer failure, duplicate/reordered anomalies, or
  reconnect requires REST reconciliation and returns only to `READY`; it never auto-arms.
- The versioned hierarchical limiter uses rolling monotonic windows and a lock. It enforces US
  global, order, query, FX, chart, special-TR, peak-time, and mock same-TR limits. New/reduce orders
  cannot consume the reserved cancel capacity, and research/bulk queries cannot consume reserved
  account/UNKNOWN reconciliation capacity. It does not sleep, retry, queue, or claim global
  fairness.

## B3 — mutation and UNKNOWN safety

- `KiwoomMutationClient` classifies one injected transport call and never refreshes authentication
  or retries. Environment, HTTPS host, URL, API capability header, authorization scheme, and exact body
  digest are validated before the call.
- ACK requires a successful HTTP status, unique-key JSON, exact integer `return_code == 0`, a
  non-empty order number, raw response storage, and a healthy clock. Only versioned explicit codes
  are definite rejections. Timeout, 401/5xx, malformed or ambiguous responses, unknown codes,
  missing order numbers, evidence storage failure, and clock failure are UNKNOWN.
- `BrokerOrderRef` is scoped by environment, internal account alias, business date, and broker order
  ID. Broker IDs are not treated as globally permanent identifiers.
- UNKNOWN resolution accepts only `BrokerOrderLinked`, `ConfirmedAbsent`, or
  `ManualActivityLinked`. Policy `unknown-resolution-v1` requires the exact
  `broker.orders.read` capability set, business date and query window, complete pagination, and a
  canonical candidate tuple. Count and hash are derived from the tuple. A linked reference must be
  the sole exact candidate; absence requires an empty tuple. Schema triggers and reopen validation
  recompute and enforce these facts against direct-SQL tampering.
- `CancelOrderCommand` binds one `BrokerOrderRef`, instrument, positive integral remaining quantity,
  and account snapshot. An audited operator command can issue one short-lived `CancelPermit` for
  that exact command. `OfflineCancellationService` consumes it before broker I/O, invokes the
  injected cancellation capability at most once, and classifies any exception or malformed result
  as UNKNOWN. This authority is process-local and has no durable crash recovery.

The mutation route remains deliberately unwired: there is no production Kiwoom order URL or
request builder in composition, and no credential-backed paper/live call has been made.

## Phase 1C foundation

- `StubBroker` returns fixed unit-test outcomes. `SimulatedBroker` separately models latency,
  partial fills, quote depth, spread/slippage, fees, cancel/fill ordering, DAY expiry, stale or
  reordered input, deterministic execution IDs, broker rejection, and unresolved corporate-action
  halts. A caller-supplied business-date policy prevents UTC dates from silently defining sessions.
- `PortfolioAllocator` preserves per-strategy virtual target ownership while deriving deterministic
  account targets under explicit strategy and instrument quantity caps. Decision, strategy, and
  input-snapshot lineage survives target allocation and order audit serialization.
- Typed broker lifecycle facts fold `OPEN -> PARTIAL/FILLED/CANCELED/EXPIRED/REJECTED` without a
  generic event bus or projection table. The ledger stores each quantity-resolving fact with its
  derived incremental risk release in one transaction, rejects caller-supplied releases, and
  revalidates exact fact, execution-ID, quantity-partition, release-order, and
  terminal-full-release semantics on reopen.
- `AccountingSeed` and broker fills form a deterministic, long-only, same-currency Dry cash and
  position fold. It is not the durable authoritative accounting projection for paper/live.
- `RunSpec` serializes immutable code, strategy, config, data, universe, calendar,
  corporate-action, fee, slippage, FX, accounting, seed, cutoff-policy, and sample-window inputs to
  canonical JSON with a SHA-256 fingerprint. `RunResult` separately binds execution identity,
  status, timing, ledger digest, output digest, or a canonical failure code to that fingerprint.
- `VirtualClock` advances only under the Dry runner. The actual runner remains intentionally absent
  until the strategy, immutable data, calendar, and accounting inputs in
  [`DRY_BACKTEST_READY.md`](DRY_BACKTEST_READY.md) are selected.

## Ledger verification and backup

- Verification is split into physical integrity, foreign keys, exact schema contract, audit
  semantics, submission state, and `full_ledger_verify()`. The legacy `integrity_check()` name is a
  physical-only compatibility alias.
- Local backup uses SQLite's online backup API, a SHA-256 manifest, exclusive no-overwrite
  publication, isolated restore, supported-schema migration, and full reopen validation. A failed
  artifact is retained for investigation rather than silently overwritten or deleted.

## Still blocking paper/live mutation

1. Select and version the trading calendar/session source, including holidays, early closes, DST,
   missing-data behavior, and update ownership.
2. Define authoritative paper/live cash/position accounting, fee/tax/FX/settlement/corporate-action
   correction rules, then add durable projections and broker reconciliation. The Dry fold is not a
   substitute for this contract.
3. Implement authenticated OS-user operator CLI; the current `actor` value is an authenticated
   identity claim, not authentication.
4. Set maximum order notional, daily loss, exposure, turnover, liquidity, and strategy budget values.
5. Choose off-host backup destination, encryption/key ownership, retention, monitoring, and restore
   drill evidence.
6. Reconfirm the current official API contract, connect paper credentials and endpoints, and prove
   complete pagination, disconnect recovery, timeout/401/malformed response, cancel priority, and
   crash/restart reconciliation with actual broker observations.
7. Extend the offline cancel contract with durable attempt evidence and crash/restart recovery,
   then connect and verify the real paper cancel endpoint before any live order path is enabled.

No source inspection, simulated response, or CI result is evidence that real broker connectivity or
operational readiness has been achieved.
