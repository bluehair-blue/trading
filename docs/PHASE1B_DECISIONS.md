# Phase 1B decisions

## Phase 1B-A — completed local safety boundary

- SQLite schema version is `6`, stored in `PRAGMA user_version`. Exact unversioned Phase 1A
  and known versions `1` through `5` migrate forward transactionally. Version 5 also stores an
  immutable migration cutoff so pre-version-5 two-field terminal payloads remain auditable without
  rewriting append-only ledger history; every new terminal payload has nullable permit/order
  correlation fields. Version 6 rejects unknown-resolution evidence dated after its resolution
  event at both the storage boundary and the database trigger.
- A fresh database is created at the current version in one transaction. Only the exact
  unversioned Phase 1A schema is adopted and migrated; partial, modified, unknown, and newer
  schemas fail closed.
- Operator commands are immutable records keyed by a database-unique `command_id`. A committed
  `OPERATOR_COMMAND_REQUESTED` event precedes every operator effect, and at most one terminal
  `SUCCEEDED` or `FAILED` event may follow it. A terminal-write failure intentionally leaves a
  pending request; startup halts for manual investigation and never replays its effect automatically.
- Startup recovery acknowledgement, reconciliation start, arm, explicit halt, high-risk permit
  issuance, and unknown-submission resolution are available only through
  `OperatorCommandService`. The normal `NEW_ORDER` permit remains the automated `TRADING` path.
- Unknown-submission resolution records a typed outcome, observation, reference, and UTC evidence
  time before its blocker is cleared. Clearing it does not make the controller trade-ready; fresh
  reconciliation and a separately audited arm are still required.
- Local backup uses SQLite's online backup API. A backup is published only after schema and
  integrity checks, with a SHA-256 manifest. The database and manifest are each published
  atomically and exclusively, but the two-file pair is not claimed to be one atomic operation; a
  usable backup requires both files. A destination that appears during either publication is never
  overwritten. Failed publication artifacts are retained for manual verification and cleanup.
  Restore verifies one immutable byte snapshot into a new isolated destination, rejects existing
  SQLite sidecars, and uses the same exclusive no-overwrite rule. After publication it opens and
  rechecks the final database's schema, integrity, sequence, and logical content before returning
  success. A verified v6 backup is migrated in a staged isolated file to the current schema and
  re-verified before publication. A failed restore artifact is likewise retained for investigation
  and never auto-deleted.

## Phase 1B-B1 — process lock and submission safety increment

- A cross-platform, non-blocking exclusive process lock is scoped by the ledger runtime identity and
  internal account alias, not an arbitrary directory. The alias is used only as the input to a
  SHA-256-derived lock filename; it is not written as raw alias data to the path or lock metadata.
  The future composition root must acquire this lock before initializing tokens, WebSocket, or
  broker clients. `ExecutionService` and `OperatorCommandService` are account-scoped. Every
  `OperatorCommand` requires `account_id`; no global broadcast coordinator exists, so global
  commands are not allowed. `ExecutionService` additionally requires a non-empty
  `account_id` and an already acquired lock matching that account; account-scoped startup recovery
  and the complete submit mutation (reservation, broker call, and terminal record) hold that lock.
  The composition root and Kiwoom integrations are not implemented by B1.
- A file-backed WAL SQLite ledger rejects a hard-linked database at open and fails closed without
  mutating the ledger.
- SQLite schema version is now `7`, stored in `PRAGMA user_version`. The database enforces the
  submission state machine `PREPARED -> SUBMISSION_STARTED -> ACKNOWLEDGED | SUBMISSION_REJECTED |
  SUBMITTED_UNKNOWN`, including the single terminal transition, at the storage boundary and during
  historical validation.
- A live `NEW_ORDER` `TradingPermit` is bound exactly to `client_order_id`, `risk_decision_id`, and
  `execution_plan_id`. The binding is persisted in the immutable order payload and validated before
  submission. A permit ID has a durable unique constraint across order reservations, so one permit
  cannot be consumed by a second order.
- Order reservation, the immutable order request, `PREPARED`, and `SUBMISSION_STARTED` are committed
  atomically. If any part fails, the order reservation and both submission events roll back together.
- Fake/Simulated Broker paths remain permitless. Their successful submissions do not issue or imply a live permit.
- Kiwoom read-only/live adapters, the common-mode safety path, a monotonic clock, coordinator, and risk reservation remain unimplemented follow-up work. This increment does not represent live-trading readiness or approval.

## Blocking decisions for Phase 1B-B

The following are deliberately not implied by Phase 1B-A:

1. Select and version the official TradingCalendar/session source, including holidays, early
   closes, DST, missing-data behavior, and update ownership.
2. Define per-instrument broker facts and reconciliation fields for positions, open orders, fills,
   cash, buying power, fees, FX, settlements, and manual activity.
3. Define cash/position projection accounting, replay/correction rules, and currency, fee, tax,
   settlement, and corporate-action treatment.
4. Define authenticated operator identity and role authorization for the eventual CLI. Actor text
   in the current internal command is an already-authenticated identity claim, not authentication.
5. Add real broker observation that can independently verify unknown-resolution evidence. Phase
   1B-A records operator-supplied evidence without claiming broker verification.
6. Define backup retention, encryption/key ownership, off-host transport, restore drills, and
   downgrade/rollback compatibility. Phase 1B-A is verified local backup/restore only.
7. Extend the broker port before implementing real cancel, reduce-only, or emergency-flatten
   effects. Phase 1B-A issues audited short-lived permits; it does not invent broker operations.

No Kiwoom adapter, network access, `.env` access, CLI, migration framework, or empty future
scaffolding is part of Phase 1B-A.
