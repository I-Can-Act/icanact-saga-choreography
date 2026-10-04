# Migration: production saga safety remediation

Baseline: `583cd830fda8b4518a7aa7a6a6d7e8f654079a1f`. This change set is **not released**.
The crate remains `0.1.0`: manifests/lockfiles were excluded from this task. Before any
publication, choose a semver-appropriate version bump and complete release validation.
Nothing in these notes authorizes publication or certifies production operation.

## Breaking changes

1. **Exhaustive enums grew.** `ParticipantEvent` and `TerminalResolverJournalError` gained
   variants. Update exhaustive downstream matches; recovery evidence is not success.
   `ParticipantRunEvidence` also gained `undo_required` and
   `forward_definitively_rejected` (default false, derived exact-run evidence); update
   explicit struct literals or construct with `Default`. Rejection is not physical
   cancellation and no archive field/tag was added for this derived marker.
2. **Archive tags are appended.** Original tags 0–11 are not renumbered; appended tags
   12–14 retain run, terminal and reconciliation records. Tag 15 is
   `ParticipantForwardOutcomeRecorded { outcome: ParticipantForwardOutcome }`. The proof
   contains original completion context, output, input, compensation, optional declared
   effect/receipt and recording time. `record_forward_outcome_strict` persists it before
   publication; `participant_run_evidence_strict` distinguishes proof from a raw result.
   Recovery resends `completion_event()` without executing the effect. Missing proof is
   uncertainty. Tags are pinned by `tests/qa_remediation_state.rs` and
   `tests/qa_review_state.rs`. Tag 16 is
   `ParticipantDependencyCompletedRecorded { context, recorded_at_millis }`: run-scoped
   AllOf dependency observations written strictly before being marked seen and read back
   by exact type/ID/start (`record_dependency_completion_strict`,
   `completed_dependency_steps_strict`; covered in `tests/qa_rechallenge_state.rs`). Plain
   `Completed`/accepted completion uses a single tag-15 append; declared effects keep
   result-then-proof. Old readers cannot decode new tags. Upgrade every reader
   before enabling writers; do not roll an old binary onto new records.
3. **Declared effects fail closed.** `CompletedWithEffect` invokes `dispatch_effect` on
   the participant/workflow trait. The default is `Unsupported` and quarantines. Return
   `EffectDispatchOutcome::Durable { receipt }` only after durable application handoff.
   The confirmation retains the receipt, but is not an external outbox. Plain `Completed`
   needs no hook. All external effects and hooks still require stable idempotency keys.
4. **Legacy execution needs identity.** Unidentified execution/panic history blocks
   recovery and participant store open until application reconciliation. The SDK never
   invents a timestamp. Registration-only legacy history may admit its first identified
   run. Confirmed results recover; unconfirmed legacy results require reconciliation.
5. **Failure no longer resolves uncertainty.** Partial delivery, failed undo, unresolved
   forward/undo expiry or an unconfirmed result quarantine with retained evidence.
   Recovery does not quarantine idle admission or healthy accepted work merely by age.
   `RecoveryPolicy::stale_after_ms` is retained for source compatibility, not classification.
6. **Low-level accepted APIs are fenced.** Admission checks durable identity and execution
   history; clearing volatile maps cannot reopen execution. Completion may return
   `SagaQuarantined` for late identified evidence; publish every returned event and do not
   assume all successful returns are `StepCompleted`. Result-write failure is an error
   with retained reconciliation evidence where storage permits. Reconciliation is not
   permission to repeat a remote effect. Keep execution IDs stable within, and distinct
   across, runs; ambiguous callback identity requires application reconciliation.
7. **Admission and replies are run-scoped.** Starts are reserved before fanout; durable
   starts are journaled strictly. A refused start must not poison the existing owner.
   Prefer `register_terminal_reply_for_run`, `complete_terminal_reply_for_run`,
   `reject_terminal_reply_for_run`, `take_terminal_reply_for_run` and
   `take_terminal_outcome_for_run`. Preserve type/ID/start in the context. Saga-ID-only
   compatibility lookups follow the newest admitted run, not arbitrary old outcomes.
8. **Pruning is destructive.** Permanent fences and unresolved evidence are not routine
   cleanup targets. Resolver compaction preserves fences and all quarantined/unresolved
   history. `prune_saga*` and backend pruning remove replay protection.

9. **Resolver LMDB schema is now 2.** Opening schema 1 transactionally backfills
   `resolver_saga_index` and commits the index/version together. Older schema-1
   binaries refuse the migrated store; do not roll them back onto it. Budget backup,
   map capacity and startup scan time before opening an existing store.
10. **Admission lookups and capacity are explicit.** `TerminalResolverJournal` has
    default `supports_saga_lookup`, `read_saga` and `read_saga_bounded` methods.
    A custom journal advertising lookup must implement the bounded method; the
    default returns `Unsupported`, not a global scan. Without lookup, fences remain
    resident and new runs are refused at capacity. `SAGA_ADMISSION_CAPACITY` bounds
    resident IDs **and runs**, including multiple runs reusing an ID; oversized
    unresolved recovery or a per-ID miss fails visibly rather than forgetting fences.
11. **Accepted timeout output includes the step disposition.** `FailStep` publishes
    `StepFailed` before ordinary terminal failure or undo requests so the owner can
    durably record its resolution. A step's safe disposition is independent of its
    authority to fail the whole saga. `requires_compensation=true` still retains
    owned undo; `false` never erases a real result or already-requested undo.
12. **Managed start hook timing changed.** Workflow `on_emitted_transition` sees
    `StepStarted` before business execution. With an attached bus, helper emit sinks
    observe an already-published start; do not forward that event to the bus again.
    Managed sync/async wrappers suppress the duplicate bus send, not the observer hook.
13. **Owned no-effect rollback is durably acknowledged.** Definitive accepted rejection
    can remove only an unrequested potential queue entry. Requested undo remains owned;
    exact-run rejection without conflicting effect/undo evidence allows strict
    `CompensationCompleted` before acknowledgement without business undo. Completion
    hooks include this logical no-op. Failed completion persistence quarantines and sends
    no acknowledgement. Duplicate/cold/startup resend needs proof, not missing rows.
14. **Release completion is observable.** `SagaBusReleaseWaiter`, returned by
    `SagaChoreographyBus::release_waiter()`, exposes `wait_timeout(Duration) -> bool`.
    Obtain it before dropping public owners and wait only off-actor/off-callback. True
    observes actual bus-owned resource destruction; it does not shut down work, resolve
    sagas, close application-held journal clones or physically cancel external work.

## Drain before upgrading from baseline

Baseline writers produced archives without run identity. Before replacing a baseline
binary: stop new starts, **explicitly drain every in-flight saga**, and verify each reached
ordinary completion or compensated failure in resolver/application records, with no
unresolved participant execution/undo history. Baseline participants may already have
pruned terminal detail, so an empty store alone is not proof of ordinary resolution.
Only then upgrade. Stores that already contain unidentified execution or unconfirmed
results remain blocked; keep them intact. Do not prune, forge identity or hand-edit them, and do not
expect a built-in safe-clear.

## Behavioral policies

- A declared effect without compensation is irreversible: later failure needs
  reconciliation/quarantine, not ordinary resolution.
- `FailStep` with `requires_compensation=false` is an explicit safe/no-undo remote
  contract, not physical cancellation; genuinely unknown work still quarantines.
- Success keeps its effects: late compensable/accepted evidence after `SagaCompleted`
  does not itself quarantine; after failure it does. A `SagaFailed` reply may be
  superseded by quarantine on later evidence.
- Managed attached-bus `StepStarted` is published strictly before business execution;
  hooks stay application-idempotent.
- ID-scoped compatibility waiters bind to the unique known active run after admission.
- Closed full-run history is classified before reconstruction. Compacted ordinary
  terminals cannot invent success/failure/undo/timeouts; even a later quarantine uses
  actual quarantine rows rather than gapped detail, preserving older-run successor fencing.
- Ephemeral resolvers refuse new admission at capacity; durable caches reclaim reloadable
  ordinary terminals/failed fingerprints while protecting the whole current ID. Temporary
  durable shortage recovers only after real room and a complete bounded history merge.
  Active/quarantined ownership, oversize IDs, non-indexed/ephemeral lifetime fences and
  unrepresented/unpersisted uncertainty still fail closed; permanent storage is not bounded.
- A resolver write failure latches live quarantine before loopback. Once writable,
  a known quarantine fence precedes retained new evidence. Unwritable storage cannot
  produce durable proof: application/external evidence and reconciliation remain necessary.

## Blocked legacy history and quarantine

There is **no built-in safe-clear, replacement-fence or legacy repair API**. Stop starts
for the affected identity. Retain/export its journal, dedupe, receipts and external state;
reconcile every possibly-run forward/undo operation and record an audited application
resolution. Keep unresolved quarantine intact. A deliberately new business operation
should use a **new saga ID**, with application-owned fencing against the old operation.
A later timestamp on the old ID does not clear quarantine.

If unidentified history blocks opening a store, preserve that store and resolve it out of
band. Do not prune, hand-edit rows or fabricate identity to make startup pass. Any
application migration/archival requires replacement replay protection and independent
operational validation; these notes are not an implementation of such tooling.

## Upgrade coordination

- Upgrade archive readers, export/audit tooling and SDK/API callers before new writers.
- Update exhaustive matches, effect hooks, quarantine handling and strict-publication
  error handling. Keep the original run context on redelivery.
- Follow attach -> restore/bind -> activate -> new starts; see [operations.md](operations.md).
- Treat callbacks as idempotent. Managed ingress fences stale/duplicate ordinary
  terminals, but is not a transaction with arbitrary application/external hooks.
- Alert on quarantine growth, storage errors, held recovery output and permanent-fence
  capacity. Accepted deadlines do not physically cancel remote work.

## Limits

No exactly-once guarantee, durable message transport, production-load certification,
current dependency-advisory certification or Linux/Windows runtime verification is
established by this change set. External handoff/local-confirmation crash windows and
operator reconciliation remain application-owned. Versioning and publication are separate,
unauthorized work.
