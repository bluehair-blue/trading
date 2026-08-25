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
  success. A failed restore artifact is likewise retained for investigation and never auto-deleted.

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
