# ADR-0004 — Resolver phase machine, abort input, serial rollback, retryable compensation, loser branches, `TerminalPolicy` builder

- **Status:** approved with owner decisions Q9, Q10, Q11, Q12 and the global no-silent-failure rule (`adr/OWNER-DECISIONS.md`, 2026-10-01).
- **Findings:** R03 (BLOCKER), R04 (BLOCKER, minimal latch landed by W0 `T04R`), R05 (BLOCKER), R12, R13 (HIGH); owner item Q11 (loser branches)
- **Depends on:** ADR-0001 (`RunKey`, `event_identity` incl. `attempt`, replay horizon), ADR-0002 (`CompensationFailedRetryable` on compensation-start commit failure)
- **Snapshot verified:** `main@89955fc` + W0 lane R (`qa-w0-r`): `T04R` adds `SagaResolutionState::terminal_outcome: Option<&'static str>`, latches incoming `SagaCompleted/SagaFailed/SagaQuarantined` and warns `saga_contradictory_terminal_ignored`; `T22R` adds `TerminalPolicy::validate() -> Result<(), TerminalPolicyError>` (`Copy` enum, 7 variants) and dependency-group checks in `validate_workflow_contract`; `T29R` checks resolver-journal sequences. Workers locate arms by variant name.

## 1. Context (verified in code)

| Anchor | Today |
|---|---|
| `resolver.rs` `SagaResolutionState` | Phase implicit in `compensation_requested`, `pending_compensation_steps`, `compensable_steps: Vec`, `terminal_latched` (+ W0 `terminal_outcome`). |
| `resolver.rs:300-306, 339-367` | Late completions extend `compensable_steps` and success is evaluated **even during rollback** → `SagaCompleted` (R05). |
| `resolver.rs:410-447` | `CompensationCompleted` emits `SagaFailed` when pending empties; `CompensationFailed` → `SagaQuarantined` if ambiguous else `SagaFailed`, latching even with other undo pending (R13). |
| `resolver.rs:840-893` `apply_step_failure` | One `CompensationRequested` with all compensable steps (R12). `requires_compensation == false` → `SagaFailed` directly (explicit `StepError::Terminal`, intentional). |
| `resolver.rs:677-704, 1107-1141` | Overall/stalled timeout → `SagaFailed` directly, ignoring compensable steps (R03). Accepted-step timeouts already route through `apply_step_failure`. |
| `resolver.rs` `SuccessCriteria::{AllOf, AnyOf, Quorum}` | AnyOf/Quorum success latches immediately; completed or in-flight losing-branch effects are never compensated (Q11). |
| `bus.rs:619-645, 684-717` | Required-path delivery shortfall synthesizes `SagaFailed` (R03). |
| `durability.rs:2931-2936` | Stale startup recovery publishes `SagaFailed` with a fabricated context (R03). |
| `helpers.rs:1166-1170, 1236-1240, 1223, 1292` | `SafeToRetry` flattened to terminal failure; `on_quarantined` called for every compensation failure. |
| `helpers.rs:97-115` | Participants act on `CompensationRequested` only for steps in `steps_to_compensate` — per-step requests already honoured (no `T12P` needed). |
| `resolver.rs:93-127` `TerminalPolicy` + `new` (7 args) | Public-field struct. Struct literals: `workflow_contract.rs` macro (`define_saga_workflow_contract!`, **exported**, expands in user crates), `workflow_contract.rs` tests, `bus.rs` tests (5), `resolver.rs` tests (≈9, incl. W0's new test), `tests/saga_testkit_smoke.rs:311,325`, `tests/order_lifecycle_e2e.rs:357`. `TerminalPolicy::new` already used in `bus.rs:1497`, `resolver.rs:2075`, `tests/async_workflow_lifecycle_e2e.rs`. |

## 2. Decision

### 2.1 Phase (private to `resolver.rs`; `T05R` introduces it, `T31R` adds `Settling`)

```rust
enum ResolverPhase {
    Running,
    /// Rollback after a step failure or an internal abort (R03/R05/R12/R13).
    Aborting(RollbackPlan),
    /// Success decided; losing AnyOf/Quorum branches being undone before `SagaCompleted` (Q11).
    Settling(LoserPlan),
    Terminal(ResolvedOutcome),
    Quarantined,
}
enum ResolvedOutcome { Completed, Failed }
enum AbortCause { StepFailure(SagaFailureDetails), Internal { source: AbortSource, reason: Box<str> } }

/// Shared by `RollbackPlan` and `LoserPlan`.
struct UndoLedger {
    owed: Vec<Box<str>>,                  // most recent completion first
    requested: HashSet<Box<str>>,         // CompensationRequested emitted, outcome pending
    compensated: HashSet<Box<str>>,
    retries: HashMap<Box<str>, u32>,      // CompensationFailedRetryable count per step
    unresolved: Vec<Box<str>>,            // effects that cannot be undone
}
struct RollbackPlan { cause: AbortCause, undo: UndoLedger }
struct LoserPlan { winners: HashSet<Box<str>>, losers: HashSet<Box<str>>, awaiting: HashSet<Box<str>>, undo: UndoLedger }
```

`terminal_latched`, `compensation_requested`, `pending_compensation_steps`, `pending_failure` and W0's `terminal_outcome` are replaced by the phase (`Terminal`/`Quarantined` carry the latched outcome; the contradictory-terminal `warn!` keeps working off it).

### 2.2 Transition table

| Phase | Input | Effect |
|---|---|---|
| `Running` | `StepCompleted` | record; if success criteria satisfied → §2.7 (losers) → `Terminal(Completed)` + `SagaCompleted`, or `Settling`. **Success is evaluated only in `Running`.** |
| `Running` | authorized `StepFailed { requires_compensation: true }`, accepted-step `FailStep { requires_compensation: true }` timeout | `Aborting` (`StepFailure`); `owed` = compensable completed + accepted steps with `compensation_available`; emit frontier (§2.4). |
| `Running` | authorized `StepFailed { requires_compensation: false }` | `Terminal(Failed)` + `SagaFailed` (unchanged: explicit `StepError::Terminal`). |
| `Running` | `SagaAbortRequested`, overall timeout, stalled timeout | `Aborting` (`Internal { source, reason }`); same rollback. |
| `Running` | incoming `SagaCompleted`/`SagaFailed`/`SagaQuarantined` (R04) | `Terminal(..)`/`Quarantined`; no output. |
| `Aborting` | late `StepCompleted`/`StepAccepted { compensation_available: true }` | push front of `owed` (a leaf → on the frontier) → emit its request. No success evaluation. |
| `Aborting` | late `StepCompleted { compensation_available: false }` | `unresolved += step`. |
| `Aborting`/`Settling` | `CompensationCompleted(X)` | `requested −= X; compensated += X`; emit newly ready frontier; settle if possible (§2.3). |
| `Aborting`/`Settling` | `CompensationFailedRetryable(X)` | `retries[X] += 1`; if `≤ policy.compensation_retry_limit()` re-request `[X]` with `context.attempt = retries[X]`; else `unresolved += X`, stop releasing new frontier steps. |
| `Aborting`/`Settling` | `CompensationFailed { is_ambiguous: true }` | `Quarantined` + `SagaQuarantined` (unchanged). |
| `Aborting`/`Settling` | `CompensationFailed { is_ambiguous: false }` (terminal undo failure) — **Q9** | `unresolved += X`, stop releasing new frontier steps; when `requested` is empty → `Quarantined` + `SagaQuarantined` (an effect remains in the world; `SagaFailed` means "cleanly rolled back"). |
| `Aborting`/`Settling` | overall / stalled / accepted-compensation timeout | `Quarantined` + `SagaQuarantined` listing `owed − compensated` (+ `awaiting` in `Settling`). |
| `Aborting` | another `SagaAbortRequested`, `StepFailed` | record evidence only. |
| `Settling` | loser `StepCompleted { compensation_available: true }` | `awaiting −= s`; push front of `owed`; emit its request. |
| `Settling` | loser `StepCompleted { compensation_available: false }` | `awaiting −= s`; `unresolved += s`. |
| `Settling` | loser `StepFailed` | `awaiting −= s` (nothing to undo). |
| `Settling` | any other step event, `SagaAbortRequested` | record only + `warn!(saga_event_after_success_decision)`; success stays decided. |
| `Terminal(Completed\|Failed)` | `StepCompleted` of a step not in the run's accounted set (late effect after terminal) | `compensation_available` → emit `CompensationRequested { steps_to_compensate: [s], reason: "late_effect_after_terminal" }`, track it (retry budget as above); not compensable, undo failed or budget exhausted → emit `SagaEffectsRetained { steps: [s], disposition: NotCompensable \| UndoFailed }` + `error!`. Under `LoserPolicy::Keep`, a late **loser** completion → `SagaEffectsRetained { KeptByPolicy }` + `info!`. No second terminal event (it would contradict the published one). |
| `Terminal`/`Quarantined` | duplicate of an accounted step / identical terminal | no output. |
| `Terminal`/`Quarantined` | contradictory terminal | no output + `warn!(saga_contradictory_terminal_ignored)` (W0 `T04R`). |
| `Quarantined` | late `StepCompleted` | no output (evidence retained for the operator) + `warn!(saga_late_effect_after_quarantine)`. |

"Accounted set" of a terminal run = completed + compensated + kept + already-late-handled steps; it is retained with the run's terminal record until the replay horizon expires (`T17R`), so a replayed duplicate never triggers an undo.

### 2.3 Settlement

`Aborting` settles when `requested` is empty and no owed step is ready: `unresolved` empty **and** no unknown in-flight step (started/acked, neither completed, failed nor owed) → `Terminal(Failed)` + `SagaFailed` (step-failure details or `failure: None` + reason for internal aborts); otherwise → `Quarantined` + `SagaQuarantined` listing `unresolved` and in-flight steps.
`Settling` settles when `requested` is empty, no owed step is ready and `awaiting` is empty: `unresolved` empty → `Terminal(Completed)` + `SagaCompleted`; otherwise → `Quarantined` + `SagaQuarantined` (Q11: "quarantine if not compensable").

### 2.4 Rollback frontier (R12, Q10)

A step `S ∈ owed − compensated − requested` is **ready** when no other step `T ∈ owed − compensated` depends on `S` transitively under `policy.workflow_steps` (`After`, `AnyOf`, `AllOf` edges). Each ready step gets its **own** `CompensationRequested { steps_to_compensate: vec![S], .. }`; dependents are released only after the predecessor's `CompensationCompleted` has been ingested (with a journal attached, appended first by the bus `Ingest` path — the durable ack). Steps with no dependency relation are released together.
**Q10:** if `workflow_steps` is empty (no declared graph), undo is **strictly serial in reverse completion order** (newest first): exactly one step is requested at a time, the next only after the previous one's durable ack.

### 2.5 Retryable compensation (R13)

- Wire variant `CompensationFailedRetryable { context, participant_id, error }` emitted by participants for `CompensationError::SafeToRetry` and for a `CompensationStart` commit failure (ADR-0002). Participant state stays `Compensating` (undo data kept); `on_quarantined` only for quarantine (`T13P`). The participant stamps `context.attempt` of its compensation outcome with the request's `attempt`.
- Durability: `SafeToRetry` writes a durable `CompensationRetryable` participant row (last `ParticipantEvent` variant) and each retry commits `CompensationStarted{attempt}` before the undo runs. On open, a `CompensationStarted` with no later completion/failure/retryable marker is a *dangling start* (crash mid-undo, effect unknown) → the run is `Quarantined` and `SagaQuarantined` is re-derived; it is never retried automatically. Retryable rehydration requires the marker. A crash after the marker re-applies the pending request.
- Dangling start at the resolver level → quarantine. An undeliverable `CompensationRequested` (target participant gone) is reported as given up, so settlement quarantines immediately naming the step.
- Budget: `TerminalPolicy::compensation_retry_limit()` (default `DEFAULT_COMPENSATION_RETRY_LIMIT = 3`, `0` = no retry; §2.8). Exhaustion → `unresolved` → `Quarantined`.

### 2.6 Abort input (R03)

Wire variant `SagaAbortRequested { context, reason, source: AbortSource }`.
- bus required-path shortfall / partial delivery (`T03B`): publish `SagaAbortRequested` instead of the synthesized `SagaFailed`. It is excluded from required-path delivery checks (terminal exclusion list in `required_path_expected_min_delivery_for_event`) so an abort cannot trigger another abort; if the abort reaches no resolver at all, `T03B` publishes today's `SagaFailed` and logs `error!` (the only safe event left).
- resolver timeouts (`T03R`): internal (re-derived on replay; no wire event).
- stale startup recovery (`T03D`, new card, W3 D lane): `SagaAbortRequested { source: StaleRecovery }` with a context built from the stored `RunKey` (never a fabricated start time).
Participants ignore it (`_ => {}`).

### 2.7 Losing branches (Q11, `T31R`)

`TerminalPolicy::loser_policy()` → `LoserPolicy::{Compensate (default), Keep}`; applies to `AnyOf`/`Quorum` (AllOf has no losers).
- **Winners** at the moment success is satisfied in `Running`: `AnyOf` → the group step whose completion satisfied it; `Quorum` → the `required_count` group steps completed at that moment.
- **Losers** = group steps − winners, plus (transitively, under `workflow_steps`) every step whose `After`/`AllOf` dependencies include a loser or whose `AnyOf` dependencies are all losers.
- **Compensate:** completed compensable losers → `owed` (serial/frontier as §2.4); completed non-compensable losers → `unresolved`; started/accepted-but-unfinished losers → `awaiting`. Nothing pending → `SagaCompleted` immediately; else `Settling` (§2.2/§2.3).
- **Keep:** no undo. Completed losers are recorded by emitting `SagaEffectsRetained { steps, disposition: KeptByPolicy }` immediately before `SagaCompleted` (journaled by the resolver subscription like every event); late loser completions emit another `SagaEffectsRetained`. Never silent.
- Existing tests that assert immediate `SagaCompleted` while a compensable loser has completed encode the superseded behaviour; `T31R` updates them and lists them in its handoff.

### 2.8 `TerminalPolicy` builder (Q12 — accepted break, made the last one)

`TerminalPolicy` becomes `#[non_exhaustive]`; the seven existing fields stay `pub` (read/assign still work, W0 tests assign `success_criteria`/timeouts); three new **private** fields with builder setters and getters; the existing 7-arg `TerminalPolicy::new` is the only constructor. Every struct literal (macro, `src/` tests, `tests/`) is converted to `TerminalPolicy::new(..)` by TSK; the exported macro expands to `$crate::TerminalPolicy::new(..)`.

```rust
pub const DEFAULT_COMPENSATION_RETRY_LIMIT: u32 = 3;
#[non_exhaustive] #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LoserPolicy { #[default] Compensate, Keep }

impl TerminalPolicy {
    pub fn with_compensation_retry_limit(self, limit: u32) -> Self;
    pub fn with_replay_horizon(self, horizon: ReplayHorizon) -> Self;
    pub fn with_loser_policy(self, policy: LoserPolicy) -> Self;
    pub fn compensation_retry_limit(&self) -> u32;
    /// Explicit horizon, else `ReplayHorizon::for_overall_timeout(self.overall_timeout)` (ADR-0001).
    pub fn replay_horizon(&self) -> ReplayHorizon;
    pub fn loser_policy(&self) -> LoserPolicy;
}
```

`TerminalPolicyError` (W0) becomes `#[non_exhaustive]` and gains `ReplayHorizonShorterThanOverallTimeout`; `validate()` returns it when an explicit horizon is shorter than `overall_timeout`.

### 2.9 Checked resolver ingress

`TerminalResolver::try_ingest_at(&mut self, event, now_millis) -> Result<Vec<SagaChoreographyEvent>, RunIdentityError>` is the checked entry point (W1: `Ok(self.ingest_at(..))`). `T08R` makes it the core (stale fence; `T17R` adds the expiry fence) and turns `ingest_at` into a wrapper that logs `warn!` with the `RunKey` on `Err` and returns no events; `T08B` switches the bus `Ingest` arm to `try_ingest_at`. `ingest_at` is deprecated by `T25`.

### 2.10 Failure handling (no silent failures)

| Path | Typed outcome | Log | Event |
|---|---|---|---|
| terminal undo failure (Q9), retry budget exhausted, ambiguous undo | phase `Quarantined` | `error!` with `RunKey`, step | `SagaQuarantined` listing unresolved steps |
| timeout while `Aborting`/`Settling` | `Quarantined` | `error!` | `SagaQuarantined` with owed/awaiting steps |
| unknown in-flight at settlement | `Quarantined` | `warn!` | `SagaQuarantined` |
| late non-compensable effect during rollback | `unresolved` → `Quarantined` at settlement | `error!` | `SagaQuarantined` |
| late effect after `Terminal(Completed\|Failed)`, not compensable / undo failed | terminal unchanged | `error!` | `SagaEffectsRetained { NotCompensable \| UndoFailed }` — `SagaQuarantined` is unsafe here: it would contradict the already-published terminal |
| late effect after `Quarantined` | none (absorbing) | `warn!` | none — the run already awaits the operator; evidence retained |
| stale / expired incarnation | `try_ingest_at → Err(RunIdentityError)` | `warn!` | none — unsafe (ADR-0001 §2.7) |
| contradictory incoming terminal | `Ok(vec![])` | `warn!` (W0) | none — the first terminal stands |
| abort event delivered to no resolver (`T03B`) | `publish_strict → Err` | `error!` | fallback `SagaFailed` |
| resolver-journal append fails in the bus `Ingest` arm | event not ingested (`T09B`/`T15B`) | `error!` | none — the resolver cannot decide without the input; stall timeout aborts the run |

## 3. Exact Rust signatures (W1 `TSK`)

`src/events.rs`:

```rust
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum AbortSource { DeliveryShortfall, PartialDelivery, OverallTimeout, StalledTimeout, StaleRecovery }

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum RetainedEffectDisposition { KeptByPolicy, NotCompensable, UndoFailed }
```

Appended after `StepAck` (last), in this order:

```rust
SagaAbortRequested { context: SagaContext, reason: Box<str>, source: AbortSource },
CompensationFailedRetryable { context: SagaContext, participant_id: Box<str>, error: Box<str> },
SagaEffectsRetained { context: SagaContext, steps: Vec<Box<str>>, disposition: RetainedEffectDisposition },
```

`event_type()`: `"saga_abort_requested"`, `"compensation_failed_retryable"`, `"saga_effects_retained"`; `context()` returns `context`; `terminal_outcome()` unchanged.

`src/resolver.rs`: `TerminalPolicy` changes of §2.8, `LoserPolicy`, `DEFAULT_COMPENSATION_RETRY_LIMIT`, `TerminalPolicyError` change, `TerminalResolver::try_ingest_at`. `ResolverPhase`, plans and ledgers are **not** added by TSK (private, R lane).

## 4. Mapping to today's behaviour at existing match sites (W1)

| Site | `SagaAbortRequested` | `CompensationFailedRetryable` | `SagaEffectsRetained` |
|---|---|---|---|
| `events.rs` `context()`, `event_type()` | as above | as above | as above |
| `helpers.rs` `dedupe_key_for_event` | 4-part arm | 4-part arm | 4-part arm |
| `resolver.rs` `TerminalResolver::ingest_at` | push `SagaFailed { terminal_context(context), reason, failure: None }`; `state.terminal_latched = true` (W0's post-match block records `terminal_outcome`) | as today's `CompensationFailed { is_ambiguous: false }` branch: remove `accepted_compensations[step]`, push `SagaFailed { .., reason: error, failure: state.pending_failure.clone() }`, latch | no-op (same arm as `CompensationStarted`) |
| `resolver.rs` `is_progress_event` | not added | added | not added |
| `durability.rs` `is_valid_emitted_transition` | `_ => true` | `=> matches!(entry, Some(SagaStateEntry::Compensating(_)))` | `_ => true` |
| all other `_`-armed matches | no change | no change | no change |
| `tests/*.rs` exhaustive matches | body of its `SagaFailed` arm | body of its `CompensationFailed` arm | body of its `CompensationStarted` arm, else `{}` |

Nothing emits these variants and nothing reads the new `TerminalPolicy` fields in W1, so the suite is unchanged.

## 5. Migration / legacy

The three variants are appended after the last existing variant and are smaller than `CompensationRequested`; the archived size is unchanged and old resolver rows decode (TSK pins size + golden bytes). No downgrade after rows with new variants exist. Replaying an old journal: old multi-step `CompensationRequested` rows seed `owed`; old terminal rows latch (R04).

## 6. Consequences

- R lane, serial: `T05R` (phase, late effects in `Aborting`) → `T03R` (timeouts as aborts, settlement) → `T20R` (W3); `T13R` (retry budget, Q9 quarantine) → `T12R` (frontier, Q10 serial) (W4); `T17R` (RunKey tombstones/terminal records, horizon) → `T31R` (Q11 `Settling` + post-terminal late effects) (W5).
- `T03D` (W3 D lane) covers stale recovery; `T12P` is dropped (participants already honour per-step requests).
- `T13P` stamps `context.attempt` and keeps `Compensating` on retryable failure. `T03B` excludes `SagaAbortRequested` from required-path escalation.
- External users constructing `TerminalPolicy` by literal must switch to `TerminalPolicy::new(..)` (release note).

## 7. Open questions

Q9–Q12 are answered. New, non-blocking (default applies unless the owner overrides before W5):

- **N2** A late effect after `SagaCompleted`/`SagaFailed` that cannot be undone is recorded with `SagaEffectsRetained` + `error!` instead of `SagaQuarantined`, because a second terminal would contradict the published one (R04). Default: as stated. **Accepted by owner 2026-10-01.**
