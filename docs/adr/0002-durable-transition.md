# ADR-0002 — Durable transition boundary and ingress outcomes

- **Status:** approved with owner decisions Q5, Q6 and the global no-silent-failure rule (`adr/OWNER-DECISIONS.md`, 2026-10-01).
- **Findings:** R01 (BLOCKER), R10, R19, R21; R11 consumes `IngressOutcome::Applied`
- **Depends on:** ADR-0001 (`RunKey`, admission), ADR-0003 (single-row outbox), ADR-0004 (`CompensationFailedRetryable`)
- **Snapshot verified:** `main@89955fc`; W0 does not touch `helpers.rs`/`state_ext.rs`; `durability.rs` changes only in the LMDB allocator (R29).

## 1. Context (verified in code)

| Anchor | Today |
|---|---|
| `src/state_ext.rs:16-20` | `SagaStateStoreError { Dedupe, Journal }`, `Debug` only, not re-exported. |
| `src/state_ext.rs:247-267` | `record_event_strict` returns `Result`; `record_event` logs and returns `()`. |
| `src/state_ext.rs:215-235` | `check_dedupe` maps a storage error to `false` (= "duplicate"). |
| `src/helpers.rs:419-431, 471-483` | Execution intent (`StepExecutionStarted`) via best-effort `record_event`; callback runs even if the append failed. |
| `src/helpers.rs:558-581` (sync), `:646-668` (async) | State `Executing → Completed` **before** the best-effort result append; `StepCompleted` emitted regardless. |
| `src/helpers.rs:538-552` | `StepOutput::CompletedWithEffect` treated as plain completion; effect dropped (R21). |
| `src/helpers.rs:920-926, 1046-1052, 1108-1113, 1141-1146, 780-787, 821-828` | Compensation start/completion and step failure via best-effort `record_event`. |
| `src/helpers.rs:672-703` `quarantine_accepted_step_persistence_failure` | Precedent: failed strict commit after an external acceptance → `Quarantined` state, best-effort `Quarantined` row, `SagaQuarantined` emitted. |
| `src/state.rs` typestate | `Failed` is reachable only via `Executing::fail`; `Quarantined` via `Executing::quarantine` / `Compensating::quarantine`. |
| `src/helpers.rs:10-138`, `durability.rs:1563-1646, 1648-1737, 2415-2483` | Public ingress returns `()`; publish errors only logged. |
| `src/durability.rs:99-135` `is_valid_emitted_transition` | Emitted `SagaQuarantined` requires a `Quarantined` local state; `_ => true`. |
| `src/durability.rs:1771-1787` | Workflow ingress: strict dedupe, quarantine on error, but the validator filters that quarantine when no state exists (R10). |
| `src/durability.rs:738-771, 809-835` | Heartbeat mutates the deadline before the append; timeout polling drops errors with `.ok()` (R19). |

## 2. Decision

### 2.1 Commit-before-mutate

Every correctness-relevant journal write goes through `SagaStateExt::commit_transition`, `commit_transition_with_outbox` (ADR-0003) or `commit_inbox` (ADR-0005), each returning `Result<(), SagaStateStoreError>`. In-memory state, emitted events and hooks change **only after `Ok`**. `record_event` (best-effort) is allowed only for the evidence row written while already handling a commit failure (the `Quarantined` row); it is renamed in W6 (`T25`). No success path may use it.

### 2.2 Failure matrix (normative for T01P, T01D, T10P, T10D, T19D, T21P, T02D)

Every row satisfies the global rule: typed outcome + log with `RunKey` + event where safe.

| Stage (`CommitStage`) | Effect happened? | Participant state after | Emitted | Dedupe / inbox | Outcome | Log |
|---|---|---|---|---|---|---|
| `Admission` (run-status / tombstone lookup, ADR-0001) | no | unchanged → `Quarantined` (memory) | `SagaQuarantined { reason: "reconciliation_needed: admission_lookup_failed: …" }` | — | `Failed { stage: Admission }` | `error!` |
| `Dedupe` (strict check errors) — **both** generic and workflow adapters | no | `Quarantined` (memory) + best-effort `Quarantined` row | `SagaQuarantined { reason: "reconciliation_needed: dedupe_check_failed: …" }` (safe: no effect ran, quarantine is non-contradictory; `StepFailed` is unsafe because the input may duplicate an already-completed step) | — | `Failed { stage: Dedupe }` | `error!` |
| `Intent` / `Inbox` (`StepExecutionStarted` or `InboxCommitted` with intent) — **Q5** | no — callback **not** invoked | `Failed` in memory, built by the typestate chain `Idle::new → trigger → start_execution → fail` (no callback runs; memory-only) | `StepFailed { requires_compensation: true, error_code: Some("intent_commit_failed"), error: <store error> }` (safe: nothing ran; lets the resolver roll back other completed steps) | pre-D5: the dedupe mark is **kept** so an in-process redelivery is `Duplicate`; D5: nothing persisted, the memory `Failed` state guards in-process redelivery | `Failed { stage: Intent }` / `Failed { stage: Inbox }` | `error!` |
| `Result` (`StepExecutionCompleted` / `StepExecutionFailed`, with outbox) — **Q6** | yes / maybe | `Executing → Quarantined` (typestate `quarantine`); never pruned (R07) | `SagaQuarantined { reason: "reconciliation_needed: result_commit_failed: …" }`; **no** `StepCompleted`/`StepFailed` | — | `ReconciliationNeeded { cause: ResultCommitFailed }` carrying the step's `compensation_data` (evidence) | `error!` |
| `CompensationRequest` (`CompensationRequestRecorded`) | no | existing `quarantine_compensation_request_persistence_failure` | existing `SagaQuarantined` | — | `Failed { stage: CompensationRequest }` | `error!` |
| `CompensationStart` (`CompensationStarted`) | no — undo not invoked | `Completed` kept with undo data | `CompensationFailedRetryable` (ADR-0004) — accurate, nothing undone | — | `Failed { stage: CompensationStart }` | `error!` |
| `CompensationResult` (`CompensationCompleted` / `CompensationFailed`) — **Q6** | yes / maybe | `Compensating → Quarantined` | `SagaQuarantined { reason: "reconciliation_needed: compensation_result_commit_failed: …" }`; no `CompensationCompleted` | — | `ReconciliationNeeded { cause: CompensationResultCommitFailed }` | `error!` |
| `Finalize` (ADR-0001 §2.5) | n/a | memory cleared only after journal `Ok` | none — run already terminal; any event would contradict it | sweep retries | `Failed { stage: Finalize }` | `error!` |

"Pre-transition state kept" (plan) means *never claim the post-effect success state*. For post-effect failures the honest state is `Quarantined`, which also keeps `is_valid_emitted_transition` consistent with the emitted `SagaQuarantined`. "Emit where the emit path is available" (Q6): the event is published directly (not via the outbox — the journal is failing); a publish failure goes to `IngressReport.publish_failures` and is logged, never dropped.

**R21 (`CompletedWithEffect`)**: commit `StepExecutionCompleted` as evidence (undo data survives restart) → best-effort `Quarantined` row → `Executing → Quarantined` → emit `SagaQuarantined { reason: "reconciliation_needed: unsupported_effect: <effect>" }` → `ReconciliationNeeded { cause: UnsupportedEffect { effect } }`, `error!`. If the evidence commit fails, the `Result` row applies.

**R10**: T10D lets `is_valid_emitted_transition` accept a participant-originated `SagaQuarantined` when there is no local state; T10P adds the same quarantine to the generic adapters (conformance with the workflow adapter, checked by `T23T`).

**R19 (heartbeat / timeout polling)**:

| Path | Outcome | Log | Event |
|---|---|---|---|
| heartbeat metadata commit fails | `Err(SagaStateStoreError)` to the heartbeat caller; deadline **unchanged** | `warn!` | none — the step is still running; a failure event would be false. Liveness backstop: the existing idle deadline |
| timeout resolution commit fails | `PollOutcome.errors` (T19D, local to `durability.rs`); pending accepted step retained | `error!` | none — emitting the timeout outcome without its commit would publish an uncommitted decision; the next poll retries |

**R11**: mutating terminal hooks (`apply_terminal_side_effects`, `on_saga_*`) run only when the outcome is `Applied`.

**Publish failures (all adapters)**: returned in `IngressReport.publish_failures`, `error!` per failure; the event stays in the journal-embedded outbox for startup replay (ADR-0003).

### 2.3 Outcome types

One enum, `IngressOutcome`, for participant ingress (the plan's `ParticipantOutcome::ReconciliationNeeded` is `IngressOutcome::ReconciliationNeeded`). Public ingress functions change `()` → `IngressOutcome` (helpers: `T10P`/`T01P`) or `IngressReport` (durability adapters: `T01D`/`T10D`; replay-backed by `T09D`). `IngressOutcome` is **not** `#[must_use]` (statement calls such as `durability.rs:1597` must stay clean under `-D warnings`). New outcome/error enums are `#[non_exhaustive]` so later variants are not breaking.

## 3. Exact Rust signatures (W1 `TSK`)

`src/state_ext.rs` — displayable, public, same variants:

```rust
#[derive(Debug, thiserror::Error)]
pub enum SagaStateStoreError {
    #[error("saga dedupe store: {0}")]
    Dedupe(DedupeError),
    #[error("saga journal: {0}")]
    Journal(JournalError),
}

/// Correctness-relevant journal write; mutate memory only after `Ok` (ADR-0002).
fn commit_transition(&self, run: &RunKey, event: ParticipantEvent) -> Result<(), SagaStateStoreError> {
    self.saga_journal().append_run(run, event).map(|_| ()).map_err(SagaStateStoreError::Journal)
}
```

`src/errors.rs`:

```rust
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitStage { Admission, Dedupe, Inbox, Intent, Result, CompensationRequest, CompensationStart, CompensationResult, Finalize }

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngressRejection {
    /// Saga type / workflow not handled by this participant (not a failure).
    NotParticipant,
    /// Non-start event for a run that is already terminal (ADR-0001 `RunAdmission::TerminalRun`).
    TerminalRun,
    /// Run-identity fence: stale or expired incarnation (ADR-0001).
    RunIdentity(RunIdentityError),
}

#[derive(Debug)]
pub struct IngressFailure { pub run: RunKey, pub stage: CommitStage, pub source: SagaStateStoreError }

#[non_exhaustive]
#[derive(Debug)]
pub enum ReconciliationCause {
    ResultCommitFailed(SagaStateStoreError),
    CompensationResultCommitFailed(SagaStateStoreError),
    /// R21.
    UnsupportedEffect { effect: Box<str> },
    /// R02: a required compensation request found no completed state.
    MissingCompletedState,
    /// Q14: an active pre-W5 run whose join progress was never journaled (ADR-0005).
    LegacyJoinStateMissing,
}

#[derive(Debug)]
pub struct ReconciliationNeeded { pub run: RunKey, pub step: Box<str>, pub cause: ReconciliationCause, pub compensation_data: Vec<u8> }

#[non_exhaustive]
#[derive(Debug)]
pub enum IngressOutcome {
    Applied,
    Duplicate,
    Rejected(IngressRejection),
    Failed(IngressFailure),
    ReconciliationNeeded(ReconciliationNeeded),
}

#[derive(Debug)]
pub struct IngressReport { pub outcome: IngressOutcome, pub publish_failures: Vec<SagaBusPublishError> }
```

`SagaBusPublishError` already carries W0's `AdmissionRejected` (R28); no change here.

## 4. Mapping to today's behaviour at existing match sites

None of these types is matched or produced in W1. Mapping of today's early returns for W3 implementers:

| Today (`helpers.rs` / `durability.rs`) | `IngressOutcome` |
|---|---|
| saga type not handled, `workflow_for_event → Ok(None)` | `Rejected(NotParticipant)` |
| `is_terminal_saga_start_replay` / `is_terminal_saga_latched` return | `Duplicate` for start replays and terminal events of the latched run; `Rejected(TerminalRun)` otherwise |
| `check_dedupe → false` | `Duplicate` |
| `check_dedupe → Err` (today silently `false`) | `Failed { stage: Dedupe }` + quarantine (§2.2) |
| match arm runs, including `_ => {}` | `Applied` |
| `workflow_for_event → Err` (ambiguous registration; today publishes a framework `SagaFailed`) | keep the publish; `Applied`; a failed publish goes to `publish_failures` |

## 5. Migration / legacy

No persisted format change in this ADR. Public ingress signatures change `()` → `IngressOutcome`/`IngressReport` in W3 (source-compatible for statement calls).

## 6. Consequences

- `T01P` is the only writer of the helpers' commit sites; `T01D` of the durability workflow commit sites. Both use the W1 API, so W3 lanes stay parallel.
- `T01P` routes result transitions through `commit_transition_with_outbox(run, row, vec![<StepCompleted|StepFailed>])`; because the outbox is embedded in the row (ADR-0003), the obligation is durable from W3 on in every journal; `T09D` adds replay.
- No retry scheduler is added: "retryable" means a redelivery (recovery or outbox replay) will be processed. The resolver stall timeout (ADR-0004 → `Aborting`) is the liveness backstop.
- Existing tests that assert best-effort semantics on a failing journal encode the defect; the owning card updates them and lists them in its handoff.

## 7. Open questions

Q5 and Q6 are answered. No new questions.
