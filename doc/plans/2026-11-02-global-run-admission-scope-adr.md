# ADR: Scope of the global heartbeat run admission cap

- Status: Accepted
- Date: 2026-11-02
- Decision owner: Ron Weasley (CTO)
- Related: security review MAR-256 / F1, task MAR-258 / C1, commit `0454df6d`

## Context

Commit `0454df6d` added `PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS` and `admitGlobalRunSlot()`. The cap is enforced when a queued run is claimed (queue claims and native-review claims) while holding a transaction-scoped advisory lock and counting `heartbeat_runs.status = 'running'` in the same transaction.

Six recovery paths can reset an existing run from a terminal state back to `running` without that lock:

- `server/src/services/native-runtime/native-workspace-export-retry.ts:137`
- `server/src/services/native-runtime/native-workspace-export-recovery.ts:91`
- `server/src/services/native-runtime/native-restart-recovery.ts:1035`
- `server/src/services/native-runtime/native-finalization-reconciler.ts:573`
- `server/src/services/native-runtime/status-decision-committer.ts:976`
- `server/src/services/native-runtime/native-session-executor.ts:7355` (`accept_replacement_turn`)

These paths do not create runs. They re-enter an existing run after finalization, workspace export, restart, or replacement-turn handling.

## Decision

The global cap is a **claim-path ceiling**, not an invariant over every `status = 'running'` transition. The recovery transitions above are explicitly out of scope for `admitGlobalRunSlot`.

## Rationale

1. They re-admit already-admitted work; they do not admit new work.
2. Each transition is already serialized per run by durable coordinator state (`native_run_finalizations` attempt/phase/lease owner, `ownership.assertHeld`, and the replacement-evidence checks). Two controllers cannot both recover the same run.
3. Taking the global admission lock after the coordinator/row locks would invert the claim-path lock order (admission lock, then row locks). The admission lock is `pg_try_advisory_xact_lock`, so a recovery would abort rather than wait, potentially stranding a run that is otherwise recoverable.
4. The paths are reached through reconciliation of existing rows, not from untrusted input.

## Consequences

- `heartbeat_runs` can transiently contain more `running` rows than the configured limit during recovery, by at most the number of recovery transitions in flight. New claims continue to wait until the count drops below the limit.
- A controller restart can process several restart-recovery candidates in one pass (sequentially, one transaction each). This is the main path that can exceed the limit. Those runs were running before the restart (reattach/remote) or are continuations of admitted runs.
- The cap remains a resource guard for new concurrent work, not a hard memory or process ceiling.
- Revisit this ADR if the cap must become a hard invariant. The change would be to call `admitGlobalRunSlot` inside each recovery transaction, accept that a busy claim path can defer recovery, and add per-path retry/backoff. That is explicitly out of scope for MAR-258.

## Alternatives considered

- **Route all six paths through admission.** Rejected for the lock-order and recovery-liveness reasons above.
- **A separate recovery admission budget.** Possible, but adds state and complexity that is not justified while the cap is a resource guard rather than a hard invariant.
