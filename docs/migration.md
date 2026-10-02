# Migration: production saga safety remediation

Baseline: `583cd830fda8b4518a7aa7a6a6d7e8f654079a1f`. This change set is **not released**.
The crate remains `0.1.0`: manifests/lockfiles were excluded from this task. Before any
publication, choose a semver-appropriate version bump and complete release validation.
Nothing in these notes authorizes publication or certifies production operation.

## Breaking changes

1. **Exhaustive enums grew.** `ParticipantEvent` and `TerminalResolverJournalError` gained
   variants. Update exhaustive downstream matches; recovery evidence is not success.
2. **Archive tags are appended.** Original tags 0–11 are not renumbered; appended tags
   12–14 retain run, terminal and reconciliation records. Tag 15 is
   `ParticipantForwardOutcomeRecorded { outcome: ParticipantForwardOutcome }`. The proof
   contains original completion context, output, input, compensation, optional declared
   effect/receipt and recording time. `record_forward_outcome_strict` persists it before
   publication; `participant_run_evidence_strict` distinguishes proof from a raw result.
   Recovery resends `completion_event()` without executing the effect. Missing proof is
   uncertainty. Tags are pinned by `tests/qa_remediation_state.rs` and
   `tests/qa_review_state.rs`. Old readers cannot decode new tags. Upgrade every reader
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
