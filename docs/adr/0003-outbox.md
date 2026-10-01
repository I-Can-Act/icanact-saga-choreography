# ADR-0003 — Durable outbound obligations embedded in the transition row; terminal-reply ordering

- **Status:** approved with owner decisions Q7, Q8 and the global no-silent-failure rule (`adr/OWNER-DECISIONS.md`, 2026-10-01).
- **Findings:** R09 (BLOCKER), R21 (HIGH)
- **Depends on:** ADR-0001 (`RunKey`, required run-scoped journal methods, `finalize_run`, replay horizon), ADR-0002 (`commit_transition`, `IngressReport`)
- **Snapshot verified:** `main@89955fc`; W0 `T28B` (`qa-w0-b`) adds `SagaBusPublishError::AdmissionRejected` in `bus.rs` — compatible (it is a publish failure like any other).

## 1. Context (verified in code)

| Anchor | Today |
|---|---|
| `src/durability.rs:1595-1622, 1711-1737, 2451-2483` | Ingress collects emitted events, `publish_strict`s each, only `error!`s a failure. Nothing durable records that an emission is owed. |
| `src/helpers.rs:567-581` | `StepCompleted` emitted after an unrelated best-effort result append. |
| `src/durability.rs:337-432` `complete_accepted_workflow_step` | Strict append, then returns the `StepCompleted` to publish; a crash in between loses it. |
| `src/durability.rs:2937-2957` | Panic-recovery `SagaQuarantined` suppressed by a dedupe pre-mark (`PANIC_QUARANTINE_PUBLISH_KEY`) **before** delivery. |
| `src/bus.rs:353-367` `publish_terminal_events` | Caller's reply resolved **before** `publish_strict`; publish errors logged. |
| `src/bus.rs:379-401, 408-412` | Resolver subscription journals every event of its type before `resolver.ingest`; `ActivateRecovery` drains `recovery_events` with `mem::take`, failed publications are lost. |
| `src/resolver.rs:239-255` `restore_from_events` | Resolver outputs are a pure function of journaled inputs. |
| `src/journal.rs:27-29` | Contract: a single `append` is atomic for every implementation. |
| `src/events.rs:328` `ParticipantEvent` | `Clone, Debug, rkyv` derives; no `PartialEq`. rkyv `0.8.16` with `bytecheck` (`Cargo.toml`, `Cargo.lock`). |
| bus transport | In-process `FirehosePubSub`; "delivered" = enqueued in a mailbox that does not survive a crash. |

## 2. Decision

### 2.1 The outbox is part of the transition row (Q8)

A transition that owes outbound events commits them **inside the same journal row**: `ParticipantEvent::TransitionCommitted { transition: Box<ParticipantEvent>, outbox: Vec<SagaChoreographyEvent> }`. Because one `append` is atomic under the existing journal contract, every journal — LMDB, in-memory, custom — persists the transition and its obligations together. There is no weaker capability and no separate outbox store, DB or schema bump.

Feasibility (checked): rkyv `0.8.16` supports a self-recursive `Box` field with `#[rkyv(omit_bounds)]` plus explicit `serialize_bounds` / `deserialize_bounds` / `bytecheck(bounds(..))` on the enum (rkyv's own `examples/json_like_schema.rs` and the `Node::Cons(Box<Node>)` test in `src/impls/mod.rs`). The new variant is far smaller than `AcceptedStepRecorded`, so the archived size of `JournalEntry` is unchanged (TSK pins it). The variant shape is fixed; if the compiler demands different *bounds*, TSK adjusts only the bounds attribute. Only if no bounds compile does TSK stop with `BLOCKED: recursive rkyv derive` — then the fallback is Q8's alternative: `commit_with_outbox` and `outbox_for_replay` become **required** methods (never a dropping default).

Reading rows: `ParticipantEvent::transition(&self) -> &ParticipantEvent` returns the wrapped transition (or `self`), and `ParticipantEvent::outbox(&self) -> &[SagaChoreographyEvent]` the embedded obligations (empty otherwise). **Every reader of `JournalEntry::event` in `src/` matches on `entry.event.transition()`** (switched by TSK in W1, behaviour-neutral because nothing wraps yet), so recovery reducers handle wrapped and bare rows identically with no per-site changes later. `tests/support/faults.rs::event_kind` uses `transition()` as well, so fault triggers keyed on `StepExecutionCompleted` still fire.

`ParticipantJournal` provided methods (correct for every implementation, derived from the required run-scoped methods of ADR-0001):

- `commit_with_outbox(run, event, outbox) -> Result<u64, JournalError>`: rejects (typed `Err`) a nested `TransitionCommitted` and any outbox event whose context is not `run`; empty outbox → `append_run(run, event)` (bare row, identical to today); otherwise `append_run(run, TransitionCommitted { .. })`.
- `outbox_for_replay(cutoff) -> Result<Vec<OutboxRecord>, JournalError>`: for each `list_runs()` run with `incarnation ≥ cutoff`, every `read_run` row's `outbox()` in `(run, sequence, index)` order. `OutboxId = (run, row sequence, index)` is stable across restarts.
- `has_durable_outbox` is **removed** from the design (always true).

Callers: `SagaStateExt::commit_transition_with_outbox` — helpers result transitions (`T01P`, W3: passes the `StepCompleted`/`StepFailed` it will emit), workflow result transitions and accepted-step completion in `durability.rs` (`T01D`/`T09D`).

### 2.2 No delivered marks; replay non-finalized obligations at startup (Q7)

- An obligation lives exactly as long as its row: `finalize_run` (ADR-0001 §2.5) deletes the run's rows, and with them its outbox, in the tombstone txn. Quarantined runs are never finalized, so their obligations remain as evidence.
- On startup, recovery prepends `outbox_for_replay(cutoff)` to `startup_recovery_events` ahead of re-derived recovery events (same bytes, same `event_identity`). Receivers are idempotent per `RunKey` (run-scoped dedupe, ADR-0005 inbox, resolver latch/state), so a replayed already-processed obligation is a no-op.
- Non-finalized runs older than the cutoff (stuck or quarantined beyond the horizon) are **not replayed** (receivers would reject them as expired) and recovery logs one `warn!(saga_outbox_retained_not_replayed, run)` per run so they are visible for reconciliation.
- In-process publish failures are returned in `IngressReport.publish_failures`; the event stays in its row, so the next startup redelivers it. No in-process retry loop.

Cost: startup replay volume = obligations of active + in-horizon quarantined runs. No extra write per publish.

### 2.3 Recovery emissions

Panic-recovery and stale-recovery emissions are re-derived from the journal on every startup; the `PANIC_QUARANTINE_PUBLISH_KEY` pre-mark is removed (`T09D`). `ActivateRecovery` keeps undelivered events: failed `publish_strict` results are put back, not dropped (`T09B`/`T16B`).

### 2.4 Resolver: reply follows the durable decision (T09B)

`publish_terminal_events` stops resolving replies; the `Ingest` arm resolves the pending reply when it receives a resolver-originated terminal event (`context.step_name == TERMINAL_RESOLVER_STEP`) **after** a successful resolver-journal append (or immediately when no journal is attached). Publish errors of resolver outputs are logged with the `RunKey` and retained for `ActivateRecovery`, not swallowed.

### 2.5 `CompletedWithEffect` (R21)

Fail closed (ADR-0002 §2.2): `ReconciliationNeeded { cause: UnsupportedEffect }` + `SagaQuarantined`; the effect string is not interpreted. A future typed effect becomes an extra outbox entry of the result row (no new mechanism).

### 2.6 Failure handling (no silent failures)

| Path | Typed outcome | Log | Event |
|---|---|---|---|
| `commit_with_outbox` fails (post-effect) | ADR-0002 `Result` row: `ReconciliationNeeded { ResultCommitFailed }` | `error!` | `SagaQuarantined` (published directly) |
| nested row / foreign-run outbox event | `Err(JournalError::Storage(..))` from `commit_with_outbox` → same as above | `error!` | same as above |
| `outbox_for_replay` fails at open | open / recovery returns `Err` (the participant does not start with unknown obligations) | `error!` | none — no bus is attached yet |
| replayed or live publish fails | `IngressReport.publish_failures` / recovery report; event retained (row or `ActivateRecovery` buffer) | `error!` per event | the event itself is retried at next activation/startup |
| obligations of a non-finalized run older than the cutoff | not replayed; listed in the recovery report | `warn!` per run | none — receivers would reject them as expired |
| resolver output publish fails | logged, retained for `ActivateRecovery` | `error!` | retried |

## 3. Exact Rust signatures (W1 `TSK`)

`src/outbox.rs` (new; not persisted, no rkyv derives):

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OutboxId { run: RunKey, sequence: u64, index: u32 }
impl OutboxId {
    pub fn new(run: RunKey, sequence: u64, index: u32) -> Self;
    pub fn run(&self) -> &RunKey;
    pub fn sequence(&self) -> u64;
    pub fn index(&self) -> u32;
}
impl std::fmt::Display for OutboxId; // "{run}#{sequence}.{index}"

#[derive(Clone, Debug)]
pub struct OutboxRecord { pub id: OutboxId, pub event: SagaChoreographyEvent }
```

`src/events.rs`: `ParticipantEvent::TransitionCommitted { #[rkyv(omit_bounds)] transition: Box<ParticipantEvent>, outbox: Vec<SagaChoreographyEvent> }` appended **after** `InboxCommitted` (ADR-0005); enum-level rkyv bounds attributes; `ParticipantEvent::transition`, `ParticipantEvent::outbox`.

`src/journal.rs` provided: `commit_with_outbox`, `outbox_for_replay(cutoff: RunIncarnation)` as in §2.1.

`src/state_ext.rs` provided: `commit_transition_with_outbox(&self, run, event, outbox) -> Result<(), SagaStateStoreError>`.

## 4. Mapping to today's behaviour at existing match sites

`TransitionCommitted` is never produced in W1. Every read site uses `entry.event.transition()`, which never yields `TransitionCommitted`; exhaustive matches still need an arm, so TSK adds `| ParticipantEvent::TransitionCommitted { .. }` to the no-op list of every exhaustive `ParticipantEvent` match and to `recorded_saga_type`'s `=> None` list (unreachable through `transition()`, harmless).

## 5. Migration / legacy

No new DB and no schema bump. Rows written before T01P/T09D carry no obligations; their lost emissions are not recoverable (same as today). Pre-W1 binaries cannot decode `TransitionCommitted` rows (no downgrade; ADR-0001 bumps the LMDB journal to `"3"` in W2 anyway).

## 6. Consequences

- **`T09D` shrinks:** no store work. It removes the panic pre-mark, adds startup replay from `outbox_for_replay`, routes workflow results and accepted-step completion through `commit_transition_with_outbox`, and returns publish errors.
- `T01P` (W3) already makes helper obligations durable (embedded rows); replay arrives with `T09D` (W4).
- Tests that pattern-match raw journal rows must read `entry.event.transition()`; TSK converts existing read-side matches in `tests/` (behaviour-neutral).
- `bus.rs` `ActivateRecovery` retains failed publications (`T09B`/`T16B`).

## 7. Open questions

Q7 and Q8 are answered. No new questions.
