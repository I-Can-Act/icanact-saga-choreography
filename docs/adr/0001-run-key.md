# ADR-0001 — Run identity (`RunKey`), event identity, durable keys, bounded tombstones

- **Status:** approved with owner decisions Q1–Q4 (`adr/OWNER-DECISIONS.md`, 2026-10-01). Binding for W1 `TSK` and every later card.
- **Findings:** R08 (primary), R17 (resolver tombstones), feeds R01/R02/R06/R07/R09/R10/R11
- **Snapshot verified:** `main@89955fc` (== `origin/main` at gate time). W0 lanes (`qa-w0-r`: R04/R22/R29-resolver, `qa-w0-d`: R29-journal, `qa-w0-b`: R28, `qa-w0-c`: clippy baseline, `qa-w0-fixtures`: T00) change `resolver.rs`, `resolver_journal.rs`, `workflow_contract.rs`, `durability.rs` (LMDB allocator), `bus.rs`, `binding.rs`, `tests/support/**`. W1+ workers locate code **by symbol**.
- **Global rule (owner):** no silent failures. Every failed or uncertain path (a) returns a typed error / non-success outcome, (b) logs at `error`/`warn` with the `RunKey`, (c) emits a failure/quarantine event wherever that is safe. Every failure table below has those three columns; "no event" is only allowed with the reason it is unsafe.

## 1. Context (verified in code)

| Anchor | What the code does today |
|---|---|
| `src/context.rs:55-79` `SagaContext` | Public-field struct, rkyv-persisted. Fields: `saga_id`, `saga_type`, `step_name`, `correlation_id`, `causation_id`, `trace_id`, `step_index`, `attempt`, `initiator_peer_id`, `saga_started_at_millis`, `event_timestamp_millis`. No run field. `next_step` resets `attempt` to 0 and mints a new `trace_id`. |
| struct-literal constructions of `SagaContext` | 14 `src/` sites across lanes P/D/R/B (+ `testkit.rs`, `observer.rs`) and 4 test files. `durability.rs:3056` `recovery_context_for_saga_type` **fabricates `saga_started_at_millis: now`**. |
| persisted enums embedding `SagaContext` | `ParticipantEvent::{CompensationRequestRecorded, AcceptedStepRecorded, AcceptedCompensationRecorded}` via `JournalEntry`; every `SagaChoreographyEvent` via `TerminalResolverJournalEntry`. Both rkyv-encoded in LMDB. |
| `src/support.rs:51-59` | All participant maps keyed by bare `SagaId`. |
| `src/state_ext.rs:116-158` | `is_terminal_saga_start_replay(saga_id, started_at)` — `saga_started_at_millis` is already the de-facto run discriminator; terminal latches evicted at 4096. |
| `src/state_ext.rs:278-286` `prune_saga_strict` | Clears memory **before** the journal/dedupe prune (mutate-before-commit). |
| `src/helpers.rs:30-70, 159-196, 350-391` | Dedupe key = `trace_id:saga_started_at_millis:event_type:step_name[..]`; a re-minted `trace_id` defeats dedupe and a replayed `SagaStarted` clears active tracking. |
| `src/helpers.rs:1480-1545` | Two existing tests start a newer incarnation (`+1 ms`) while the old run is **not terminal** and expect it to execute with clean dependency state. |
| `durability.rs:3194-3204, 3427-3429, 3172-3173, 3238-3266, 3530-3531` | LMDB journal keys `{saga_id:020}:{seq:020}`; dedupe keys `{saga_id:020}:{key}`; journal schema `"2"`; unversioned populated stores refused; journal and dedupe are separate envs. R29 (W0, `qa-w0-d`) makes the allocator checked and puts rows with `NO_OVERWRITE`. |
| `resolver.rs` `TerminalResolver` | `states`/`terminal_latched_set` keyed by `SagaId`; latch set never shrinks (R17). W0 `T04R` adds `SagaResolutionState::terminal_outcome: Option<&'static str>` and latches incoming terminals. |
| `bus.rs:419-421, 1222-1238` | Pending replies and terminal cache keyed by `SagaId`. |
| `src/journal.rs`, `src/dedupe.rs` impls | `InMemoryJournal`, `Arc<T>`, `LmdbJournal`; `InMemoryDedupe`, `Arc<T>`, `LmdbDedupe`; test impls `FailOnAppendJournal` (`tests/async_workflow_lifecycle_e2e.rs`), `StaticJournal` (`tests/durability_integration.rs`), `FaultJournal`/`FaultDedupe` (`tests/support/faults.rs`, W0 `T00`). |

## 2. Decision

### 2.1 Identity (Q1: accepted)

`RunKey = (saga_type, saga_id, incarnation)`, **`incarnation := SagaContext::saga_started_at_millis`** (`RunIncarnation(u64)`). `trace_id`, `correlation_id`, `causation_id` and other timestamps are **not** identity. No new `SagaContext` field (it would change the archived layout of both journals and touch 14 struct literals across lanes). Two starts of one `(saga_type, saga_id)` in the same millisecond are the **same run** (the second is a duplicate). Contract (documented on `RunKey`): a caller reusing a `SagaId` must use a strictly greater `saga_started_at_millis`.

### 2.2 Admission (Q4: coexist; Q3: expiry fence)

Runs are **isolated, not exclusive**: a newer incarnation of one `(type, id)` may be admitted while an older one is still active; both keep their own state and resolve/compensate independently (the tests at `helpers.rs:1480-1545` stay valid). Admitting a second active run of the same saga id logs `warn!(event = "saga_concurrent_run_admitted", run = %key, other_active)` and increments `ParticipantStats::concurrent_runs_admitted` (resolver: same `warn!`).

One pure function, used by participants (helpers + workflow adapter) and the resolver:

`admit_run(exact, known, incoming, is_start, cutoff) -> Result<RunAdmission, RunIdentityError>`

| `exact` (status of the incoming `RunKey`) | `is_start = true` | `is_start = false` |
|---|---|---|
| `Active` | `DuplicateStart` (idempotent, **no reset**, regardless of `trace_id`) | `CurrentRun` |
| `Terminal` (in-memory latch or durable tombstone) | `DuplicateStart` | `TerminalRun` |
| `Unknown` and `incoming.incarnation < cutoff` | `Err(ExpiredIncarnation)` | `Err(ExpiredIncarnation)` |
| `Unknown` and `known.newest = Some(n)` with `incoming.incarnation < n` | `Err(StaleIncarnation)` | `Err(StaleIncarnation)` |
| `Unknown` otherwise | `NewRun { concurrent: known.other_active > 0 }` | same (lazy state creation, as today) |

`known.newest` = max incarnation of any active run or unexpired tombstone of `(saga_type, saga_id)`; `known.other_active` = number of other active runs of that pair. `cutoff = ReplayHorizon::cutoff(now_millis)` (§2.5). The expiry row is checked first (it needs no store lookup).

### 2.3 Event identity (replaces trace-based dedupe keys)

`event_identity(&SagaChoreographyEvent) -> String` = `event_type`, `context.step_name`, `context.attempt`, discriminator, joined by `'\u{1f}'`. Scoped to its `RunKey` by the store.

| Variant | discriminator |
|---|---|
| `CompensationRequested` | `failed_step` + `'\u{1e}'` + `steps_to_compensate` joined by `','` |
| `StepAccepted`, `CompensationAccepted` | `execution_id` |
| `StepAck` | `format!("{status:?}")` |
| `SagaAbortRequested` (ADR-0004) | `format!("{source:?}")` |
| `SagaEffectsRetained` (ADR-0004) | `format!("{disposition:?}")` + `'\u{1e}'` + `steps` joined by `','` |
| all others | empty |

`attempt` is identity. `SagaContext::next_step` resets it, so the resolver's compensation re-request (ADR-0004) and the participant's compensation-outcome events stamp `context.attempt` explicitly.

### 2.4 Durable keys and store contract

Canonical encoding (pure functions in `run_identity.rs`, length-prefixed so any `saga_type` is unambiguous):

```
type_prefix  = "{len(saga_type):010}:{saga_type}:"
saga_prefix  = type_prefix + "{saga_id:020}:"
run prefix   = saga_prefix + "{incarnation:020}:"            (RunKey::storage_prefix)
journal row  = run prefix + "{sequence:020}"                   (inbox + outbox are inside rows: ADR-0003/0005)
dedupe row   = run prefix + event_identity / caller key
tombstone    = run prefix (value = rkyv RunTombstone)
```

**Run-scoped store methods are required trait methods (no provided defaults).** Reasons: (1) a default that delegates to the `SagaId` partition is not run-scoped (two saga types with one numeric id collide — the R08 bug), has no durable tombstones and cannot delete one run's rows without deleting a concurrent run's rows — a silent weaker capability the owner rejected (Q8 principle, global rule); (2) test wrappers (`FaultJournal`, `FailOnAppendJournal`) that only override legacy methods would silently route run-scoped calls to the legacy partition of a run-scoped inner store. Required methods make both a compile error. Owner accepted breaking changes for this release (Q12).

- `ParticipantJournal` (required): `append_run`, `read_run`, `list_runs`, `finalize_run(tombstone, cutoff)`, `run_tombstones(saga_type, saga_id)`, `prune_expired_tombstones(cutoff)`. Provided (derived, correct for every implementation because a single `append` is atomic): `commit_inbox` (ADR-0005), `commit_with_outbox`, `outbox_for_replay` (ADR-0003).
- `ParticipantDedupeStore` (required): `check_and_mark_run`, `contains_run`, `mark_processed_run`, `remove_processed_run`, `prune_run`, `list_runs`, `keys_run(run)` (ADR-0005 §2.5 upgrade scan), `prune_expired(cutoff)`.
- `Arc<T>` impls forward every method.
- **W1 bodies** (in-crate stores and test wrappers): legacy delegation that is truthful for today's single-run usage — `append_run → append(saga_id)`, `read_run → read(saga_id)`, `finalize_run → prune(saga_id)`, `run_tombstones → Ok(vec![])` (none exist yet), `prune_expired_* → Ok(0)`, dedupe `*_run → legacy(saga_id, key)`; **`list_runs` (journal and dedupe) and `keys_run` → `Err(Storage("<method> requires run-scoped storage (ADR-0001, T08D)"))`** because no truthful answer exists before W2. Test wrappers forward to the inner store through their fault check. Nothing calls these in W1.
- **W2 (`T08D-stores`)** makes `InMemoryJournal`, `InMemoryDedupe`, `LmdbJournal`, `LmdbDedupe` truly run-scoped. On those stores the legacy `SagaId` methods keep **union semantics** (`read(saga_id)` = legacy-partition rows ∪ all run rows with that id, by global sequence; `list_sagas` = union; `prune(saga_id)` = both partitions; `append(saga_id, ..)` = legacy partition) so the ≥10 `tests/` call sites reading framework rows stay green. LMDB: same journal env, new DBs `journal_run_rows`, `journal_run_index`, `run_tombstones`; dedupe env gets `dedupe_run_entries` + `dedupe_meta` (`dedupe_schema_version = "3"`); participant journal schema `"2"` → `"3"`. New row puts keep R29's checked allocator and `NO_OVERWRITE`.

### 2.5 Tombstones with a bounded replay horizon (Q3)

No permanent tombstones. Cleanup happens on every run; only what correctness needs is kept.

- **`ReplayHorizon`** (newtype over `Duration`, `run_identity.rs`): floor `MIN_REPLAY_HORIZON = 1 h` (enforced by `ReplayHorizon::new` → `Err(ReplayHorizonError)`, never clamped silently). Resolver default = `TerminalPolicy::replay_horizon()` = explicit `with_replay_horizon(..)` or `max(2 × overall_timeout, MIN_REPLAY_HORIZON)` (ADR-0004 §2.8). `TerminalPolicy::validate` rejects an explicit horizon shorter than `overall_timeout` (`TerminalPolicyError::ReplayHorizonShorterThanOverallTimeout`). Participants have no policy (generic `SagaParticipant` never sees one), so `SagaParticipantSupport::replay_horizon` defaults to `ReplayHorizon::PARTICIPANT_DEFAULT = 24 h` and is set with `with_replay_horizon`. Hosts must configure a participant horizon ≥ the policy horizon of every saga it joins (documented on the field; a too-short horizon fails loudly — see table).
- **Cutoff:** `ReplayHorizon::cutoff(now_millis) = RunIncarnation(now − horizon)` (saturating).
- **Finalize (one write txn):** `finalize_run(&tombstone, cutoff)` writes the run's tombstone, deletes all of the run's journal rows (which embed its inbox and outbox — ADR-0003/0005, so nothing else needs deleting), and deletes **every tombstone with `incarnation < cutoff`**. Only `Completed`/`Failed` runs are finalized; **quarantined runs are never finalized** — their rows, outbox and marks stay as evidence (R07).
- **Order at a participant** (`SagaStateExt::finalize_run_strict`, rewritten by `T08PD-rekey`): `journal.finalize_run` → on `Ok` clear in-memory run state → `dedupe.prune_run(run)`. Dedupe marks of a tombstoned run are harmless (the tombstone answers first) and are collected by the startup sweep (`T32D`) if `prune_run` fails.
- **Expiry correctness:** a tombstone is pruned only when `incarnation < cutoff`, which is exactly when §2.2 rejects any event of that run as `ExpiredIncarnation`. So at every instant an event for a finalized run is answered either by its tombstone (`Terminal`) or by the expiry fence — never admitted as a new run. Older incarnations are older still, so the stale fence is not weakened by pruning.
- **Startup sweep (`T32D`):** `journal.prune_expired_tombstones(cutoff)`, `dedupe.prune_expired(cutoff)` (all marks of runs with `incarnation < cutoff` that are not quarantined), and `dedupe.prune_run` for each tombstoned run that still has marks. Exposed as a public function returning a typed report; open calls it and logs failures (it does not block startup — the expiry fence keeps correctness).
- **Resolver (T17R, T17B):** the resolver journal gets the same per-run contract with its own tombstone type `ResolverTombstone { run: RunKey, outcome: RunTerminalOutcome, accounted_steps: Vec<Box<str>> }` (accounted steps let ADR-0004 tell a replayed duplicate from a late effect after restart): required `finalize_run(&ResolverTombstone, cutoff)` (one txn: tombstone + delete the run's rows + delete expired tombstones) and `tombstones()`. In-memory terminal records are evicted by the same cutoff (`policy.replay_horizon()`); quarantined and active runs are never evicted. `T17R` defines the type and the journal methods (R lane); `T17B` (W6) wires the bus.

### 2.6 Legacy rows (Q2: refuse, explicit drain procedure)

- **LMDB participant journal stamped `"2"` with any row** → `open` fails with `JournalError::LegacyRunIdentity { legacy_rows }`, Display (exact): `participant journal holds {legacy_rows} rows written before run identity (schema 2); drain in-flight sagas on the previous binary until the journal is empty, then start this version (docs/upgrade.md#run-identity)`. Logged at `error!`. `"2"` with no rows → upgraded in place to `"3"`. Identity is never guessed; `recovery_context_for_saga_type`'s fabricated start time is never used as an incarnation (run-aware recovery builds contexts from the stored `RunKey` — ADR-0004 §2.6).
- **Unversioned LMDB dedupe env with legacy entries** → kept inert (never consulted by run-scoped lookups), logged once at `warn!` with the entry count. Not a failure: cross-restart redelivery only originates from journals/outboxes, and journals with legacy rows are refused.
- **Resolver LMDB journal** schema `"1"`: rows embed full `SagaContext`, so `T17R` re-keys them exactly (`event.context().run_key()`) to schema `"2"` in one txn on open — no drain needed. New `SagaChoreographyEvent` variants are appended last and do not grow the archived size (TSK pins it). **No downgrade** after a W1+ binary has written new variants.
- `docs/upgrade.md` (new, written by `T08D-stores`) documents the drain procedure; W5 adds the AllOf drain note (ADR-0005 §2.5).

### 2.7 Failure handling (no silent failures)

| Path | Typed outcome to caller | Log (with `RunKey`) | Event |
|---|---|---|---|
| Stale incarnation (older than a known newer run) | `IngressOutcome::Rejected(RunIdentity(StaleIncarnation))`; resolver `try_ingest_at → Err` | `warn!(saga_stale_incarnation_rejected)`; `ParticipantStats::runs_rejected_stale += 1` | none — unsafe: any terminal/failure event for the old run could contradict its recorded outcome or the newer run |
| Expired incarnation (no state, no tombstone, older than cutoff) | `Rejected(RunIdentity(ExpiredIncarnation))`; resolver `Err` | `warn!(saga_expired_incarnation_rejected)`; `runs_rejected_expired += 1` | none — unsafe: the run is finalized or unknown |
| Run-status / tombstone lookup fails during admission | `Failed { stage: Admission }` | `error!` | `SagaQuarantined { reason: "reconciliation_needed: admission_lookup_failed: …" }` — safe: no effect ran, quarantine is non-contradictory |
| Second active run admitted | `Applied` (not a failure) | `warn!(saga_concurrent_run_admitted)`; `concurrent_runs_admitted += 1` | — |
| Legacy journal with rows | `open` → `Err(LegacyRunIdentity)` | `error!` (no run: store-level) | none — no participant runs yet |
| `finalize_run` fails | `Failed { stage: Finalize }`; memory **not** cleared | `error!` | none — the run is already terminal; any event would contradict it. Retried by the next terminal redelivery or the sweep |
| `dedupe.prune_run` after finalize fails | `Failed { stage: Finalize }` | `error!` | none (same reason); sweep collects marks |
| Startup sweep fails | sweep fn → `Err(SagaStateStoreError)`; open continues | `error!`; `ParticipantStats::gc_failures += 1` | none — GC is not a run transition |

## 3. Exact Rust signatures (W1 `TSK`; full text in `TSK-signatures.md`)

`src/run_identity.rs` (new): `RunIncarnation`, `RunKey` (+ `is_run_of`, prefixes, `parse_storage_prefix`, `Display` = `{saga_type}/{saga_id}@{incarnation}` for logs), `RunStatus`, `KnownRuns { newest, other_active }`, `RunAdmission { NewRun { concurrent }, CurrentRun, DuplicateStart, TerminalRun }`, `RunIdentityError { StaleIncarnation, ExpiredIncarnation }` (`#[non_exhaustive]`), `admit_run`, `event_identity`, `RunTerminalOutcome { Completed, Failed }`, `RunTombstone`, `MIN_REPLAY_HORIZON`, `DEFAULT_PARTICIPANT_REPLAY_HORIZON`, `ReplayHorizon`, `ReplayHorizonError`.

`src/context.rs`: `SagaContext::run_incarnation`, `SagaContext::run_key`.

`src/journal.rs` `ParticipantJournal` (required): `append_run`, `read_run`, `list_runs`, `finalize_run(&RunTombstone, RunIncarnation)`, `run_tombstones`, `prune_expired_tombstones(RunIncarnation) -> Result<u64, _>`. `JournalError::LegacyRunIdentity { legacy_rows: u64 }`.

`src/dedupe.rs` `ParticipantDedupeStore` (required): `check_and_mark_run`, `contains_run`, `mark_processed_run`, `remove_processed_run`, `prune_run`, `list_runs`, `keys_run`, `prune_expired(RunIncarnation) -> Result<u64, _>`.

`src/state_ext.rs` `SagaStateExt` (provided): `check_dedupe_run_strict`, `finalize_run_strict(&mut self, &RunTombstone, RunIncarnation)` (W1 body = `prune_saga_strict(saga_id)`), `replay_cutoff(&self) -> RunIncarnation`.

`src/support.rs`: `pub replay_horizon: ReplayHorizon` (init `ReplayHorizon::PARTICIPANT_DEFAULT`), `with_replay_horizon`.

`src/stats.rs`: `ParticipantStats` + snapshot gain `concurrent_runs_admitted`, `runs_rejected_stale`, `runs_rejected_expired`, `gc_failures`.

## 4. Mapping to today's behaviour at existing match sites

No new enum here is matched by existing code. `JournalError::LegacyRunIdentity` is never produced in W1 (no existing exhaustive `match` on `JournalError` in `src/`; any in `tests/` gets an arm with the `Storage` body). Required trait methods get the W1 bodies of §2.4 in every impl; nothing calls them in W1.

## 5. Consequences / dispatch

- **W2 lane merge (accepted):** `T08P` + `T08D` are one P+D lane running two serial cards in one worktree: `T08D-stores` (`journal.rs`, `dedupe.rs`, `durability.rs::lmdb`, `docs/upgrade.md`) then `T08PD-rekey` (`support.rs`, `state_ext.rs`, `state.rs`, `helpers.rs`, `stats.rs`, `durability.rs` non-lmdb). Reason: re-keying `SagaStateExt` accessors changes types used at 83 sites in `durability.rs`. R (`T08R`) and B (`T08B`) stay parallel.
- Public `SagaId`-keyed APIs keep a wrapper resolving the newest incarnation until W6 `T25` deprecates them; no W2 lane changes a public signature used by another lane's file or by `src/testkit.rs`.
- Resolver (`T08R`): state keyed by `RunKey`, `admit_run` fence (cutoff from `policy.replay_horizon()`), `try_ingest_at` is the checked entry point. Bus (`T08B`): replies/terminal cache keyed by `RunKey`; resolver ingress via `try_ingest_at`.
- `docs/architecture.md` "Storage and Idempotency" updated by `T08PD-rekey`.

## 6. Open questions

Q1–Q4 are answered (`OWNER-DECISIONS.md`). New, non-blocking (defaults apply unless the owner overrides before W2):

- **N1** Participant default horizon `24 h` and floor `1 h` are architect choices ("documented floor" left open by Q3). Default: as stated. **Accepted by owner 2026-10-01.**
