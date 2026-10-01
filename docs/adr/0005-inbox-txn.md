# ADR-0005 — Inbox transaction: input mark + join progress + execution intent as one journal row

- **Status:** approved with owner decisions Q13, Q14 and the global no-silent-failure rule (`adr/OWNER-DECISIONS.md`, 2026-10-01).
- **Findings:** R06 (HIGH), R11 (MEDIUM, hooks only on `Applied`)
- **Depends on:** ADR-0001 (`RunKey`, `event_identity`, required run-scoped journal/dedupe methods incl. `list_runs`, `keys_run`), ADR-0002 (`IngressOutcome`, failure matrix), ADR-0003 (`ParticipantEvent::transition()` read convention)
- **Snapshot verified:** `main@89955fc`; W0 does not touch the inbox paths.

## 1. Context (verified in code)

| Anchor | Today |
|---|---|
| `src/helpers.rs:41-49, 168-172` | Input marked in the dedupe store **before** any journal write; a comment admits the crash gap. |
| `src/durability.rs:1766-1787` | Workflow ingress: same order. |
| `src/helpers.rs:266-348`, `durability.rs:1883-1920` | AllOf join bits (`dependency_completions`) and firing latch (`dependency_fired`) live only in memory (`support.rs:52-53`). |
| `src/state_ext.rs:86-99`, `support.rs:72-89` | Nothing rebuilds join state on restart. |
| `src/durability.rs:2787-2791` | Recovery does not resume a marked-but-unjournaled input (silently lost). |
| `src/durability.rs:3530-3531` | Journal and dedupe are separate LMDB envs — no shared txn. |
| `src/journal.rs:27-29` | One `append` is atomic for every journal. |
| `src/durability.rs:1582-1598, 1684-1715` | `apply_terminal_side_effects` runs before admission (R11). |

## 2. Decision

### 2.1 The inbox transaction is one journal row (Q13: accepted)

`ParticipantJournal::commit_inbox` (provided) writes the whole transaction as **one** `ParticipantEvent::InboxCommitted` row. A single append is atomic under the existing contract, so mark + join bit + execution intent commit together in every journal (LMDB, in-memory, custom) without a third store generic on `SagaParticipantSupport<J, D>` and without merging the dedupe env.

For inputs that can advance this participant's dependency join or trigger its execution, the **journal-derived `InboxState` replaces the dedupe store** as the admission authority — these inputs no longer write dedupe marks:

| Input (for this participant's `depends_on`) | `InboxTxn` |
|---|---|
| `SagaStarted`, participant `OnSagaStart` | `dependency_step: None`, `execution_intent: Some(..)` |
| `StepCompleted` of a step in `After`/`AnyOf`, latch not fired | `dependency_step: Some(step)`, `execution_intent: Some(..)` |
| `StepCompleted` of a step in `AllOf`, join not complete after adding it | `dependency_step: Some(step)`, `execution_intent: None` |
| `StepCompleted` completing the `AllOf` join | `dependency_step: Some(step)`, `execution_intent: Some(..)` (input = original `saga_input`, as `prefers_original_saga_input`) |
| `StepCompleted` of a dependency after the latch fired | `dependency_step: Some(step)`, `execution_intent: None` (dedupe record only) |

`input_key = event_identity(&event)` (ADR-0001). Admission: `InboxState::is_admitted(input_key)` → `Duplicate`; else `commit_inbox` → `Ok` → apply the same row to the in-memory `InboxState`, then execute if an intent was committed. The `InboxCommitted` row with an intent **is** the durable execution intent (no separate `StepExecutionStarted` for inbox-triggered executions). On `Err` see §2.6.

Other inputs (compensation requests, terminal events, acks) keep the run-scoped dedupe store (ADR-0001) with ADR-0002's rules. Folding them into the inbox is W6 (`T23`).

### 2.2 Rebuild

`InboxState::from_entries(&read_run(run)?)` rebuilds state on open/recovery (`T06D` for the workflow adapter and recovery, `T06P` for the generic helpers). Reducer (reads `entry.event.transition()`, ADR-0003):
- `InboxCommitted` → insert `input_key`; insert `dependency_step` if `Some`; set `execution_fired` if `execution_intent` is `Some`.
- `StepExecutionStarted` (legacy/explicit intent) → set `execution_fired` (never re-execute a run with execution evidence).
- every other variant → no change.

Recovery resumes a run whose `InboxCommitted` intent has no result row exactly like a `StepExecutionStarted` without result today (stale → `SagaAbortRequested { StaleRecovery }` per ADR-0004, otherwise continue).

### 2.3 Hooks (R11)

`apply_terminal_side_effects` and `on_saga_*` run only after admission with `IngressOutcome::Applied`. Doc comments state observation callbacks may repeat across restarts.

### 2.4 Memory layout

`SagaParticipantSupport` gains `pub inbox_states: HashMap<RunKey, InboxState>` (TSK, unused until W5) so `T06P` (helpers) and `T06D` (durability) share one field through `SagaStateExt::inbox_state{,_mut}` without touching each other's files. `dependency_completions`/`dependency_fired` are removed in W6 once both adapters use `inbox_states`.

### 2.5 Upgrade: drain first, enforced fail-closed (Q14)

Upgrade notes (`docs/upgrade.md`, `T06D`): **drain in-flight AllOf sagas before deploying W5.** Enforcement, on every open/recovery (all stores, no schema epoch needed):

A pre-W5 run is recognisable because pre-W5 binaries wrote **dedupe marks for inbox-kind inputs**, which W5 never writes (§2.1). For each run in `dedupe.list_runs()` that has no tombstone, is not already `Quarantined` and has no execution evidence (`InboxState::from_entries(read_run(run)).execution_fired() == false`): if any `dedupe.keys_run(run)` key is an inbox-kind input **for this participant** (`event_identity` of `SagaStarted` when it depends `OnSagaStart`; of `StepCompleted` for a step in its `depends_on`) and that key is not `is_admitted` in the rebuilt `InboxState`, the run's join progress was never journaled. It is **not resumed**:

- journal a `Quarantined { reason: "reconciliation_needed: legacy_join_state_missing" }` row (strict; on failure `error!` and continue — the marks remain as evidence),
- in-memory state `Quarantined` (never pruned, R07),
- `SagaQuarantined { reason: "reconciliation_needed: legacy_join_state_missing" }` queued in `startup_recovery_events`,
- recovery report entry `ReconciliationNeeded { cause: LegacyJoinStateMissing }` + `error!` with the `RunKey`.

This also closes today's silent loss at `durability.rs:2787-2791` (marked-but-unjournaled start input).

### 2.6 Failure handling (no silent failures)

| Path | Typed outcome | State / persistence | Log | Event |
|---|---|---|---|---|
| `commit_inbox` fails, row has an execution intent or would complete/advance a join whose latch has not fired | `Failed { stage: Inbox }` | nothing persisted; memory `Failed` (typestate chain, ADR-0002 Q5) so in-process redelivery is `Duplicate` | `error!` | `StepFailed { requires_compensation: true, error_code: Some("inbox_commit_failed") }` — safe: the step did not run and now will not |
| `commit_inbox` fails for a dependency input after the latch fired (dedupe record only) | `Failed { stage: Inbox }` | nothing persisted; input stays retryable | `error!` | none — unsafe: the step already fired; a `StepFailed` would contradict its result |
| `read_run` fails while rebuilding `InboxState` on open | open / recovery `Err` | — | `error!` | none — no bus yet; the participant does not start with unknown join state |
| `read_run` fails for a cold run during live ingress | `Failed { stage: Admission }` | — | `error!` | `SagaQuarantined` (ADR-0002 `Admission` row) |
| pre-W5 run detected (§2.5) | recovery report `ReconciliationNeeded { LegacyJoinStateMissing }` | `Quarantined` | `error!` | `SagaQuarantined` at recovery activation |
| `dedupe.list_runs` / `keys_run` fails during the §2.5 scan | open / recovery `Err` | — | `error!` | none — fail closed: no run is resumed with unverified join state |

## 3. Exact Rust signatures (W1 `TSK`)

`src/inbox.rs` (new): `StepExecutionIntent` (rkyv), `InboxTxn` (+ `into_event`), `InboxState` (+ `from_entries`, `apply`, `is_admitted`, `completed_dependencies`, `execution_fired`).

`src/events.rs`: `ParticipantEvent::InboxCommitted { input_key: Box<str>, dependency_step: Option<Box<str>>, execution_intent: Option<StepExecutionIntent>, admitted_at_millis: u64 }`, appended **after** `AcceptedCompensationRecorded` and **before** `TransitionCommitted` (ADR-0003).

`src/journal.rs` provided: `fn commit_inbox(&self, run: &RunKey, txn: InboxTxn) -> Result<u64, JournalError> { self.append_run(run, txn.into_event()) }`.

`src/dedupe.rs` required (ADR-0001 list): includes `fn keys_run(&self, run: &RunKey) -> Result<Vec<Box<str>>, DedupeError>`.

`src/state_ext.rs` provided: `commit_inbox`, `inbox_state`, `inbox_state_mut`. `src/support.rs`: `inbox_states` field.

## 4. Mapping to today's behaviour at existing match sites (W1)

`InboxCommitted` is never produced in W1. Every exhaustive `ParticipantEvent` match in `durability.rs` (read through `entry.event.transition()`):

| Site (symbol) | Mapping |
|---|---|
| every no-op or-list containing both `StepTriggered { .. }` and `StepExecutionStarted { .. }` (5 sites at 89955fc) | add `\| ParticipantEvent::InboxCommitted { .. } \| ParticipantEvent::TransitionCommitted { .. }` |
| `recover_completed_step_effect_for_unstarted_compensation` arm `StepExecutionStarted { .. } => completed = None,` | becomes `StepExecutionStarted { .. } \| InboxCommitted { execution_intent: Some(_), .. } => completed = None,`; append `\| InboxCommitted { execution_intent: None, .. } \| TransitionCommitted { .. }` to its no-op list |
| `recorded_saga_type` | add both to the `=> None` list |
| `classify_recovery`, property-test generators, `matches!` sites | no change (non-exhaustive) |
| `tests/*.rs` exhaustive matches | arm of `StepExecutionStarted` |

## 5. Migration / legacy

`InboxCommitted` is appended last-but-one and is smaller than `AcceptedStepRecorded`; archived size unchanged (TSK pins). No downgrade. Pre-W5 runs: execution evidence prevents re-execution; lost join progress is detected and quarantined (§2.5), never silently resumed.

## 6. Consequences

- `T06D`: workflow ingress → `commit_inbox`; rebuild `inbox_states` on open/recovery; §2.5 detection; `docs/upgrade.md` AllOf drain note. No new LMDB DB.
- `T06P`: generic helpers → `commit_inbox` + `inbox_state_mut`.
- `T11D`: hooks gated on `Applied`.

## 7. Open questions

Q13 and Q14 are answered. No new questions.
