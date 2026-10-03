# Operations: Saga Safety Contract

This library fails closed on uncertain effects; it does **not** provide exactly-once
external execution or a durable message transport. Applications must handle delivery
errors, stable idempotency keys and reconciliation. These checks are not deployment,
load, Linux/Windows or external-service certification.

## Startup order

1. Attach a durable resolver with
   `SagaChoreographyBus::attach_durable_terminal_resolver_for_contract` and a persistent
   `TerminalResolverJournal`, one journal per saga type. Attachment holds recovery output.
2. Open participant stores and restore projections before binding ingress.
   `durability::lmdb::open_lmdb_participant_support_for_saga_type` (or `_for_saga_types`)
   collects startup recovery events and invokes
   `durability::recover_accepted_workflow_steps_for_saga_type` to restore accepted
   work, confirmed completed projections, recorded unstarted undo requests and current-run fences. For custom stores,
   use that recovery function plus `collect_startup_recovery_events_for_saga_type`.
   Applications remain responsible for their business projections and reconciliation;
   opening stores is not permission to replay unresolved business effects.
3. Bind every required participant through the strict binding helpers. Deliver the
   collected startup events through normal ingress. Resolver output produced by
   ingestion **and** the watchdog stays held until activation.
4. Call `activate_terminal_resolver_recovery_for_contract` only after binding is live.
   Activation queues retained output; it is not an acknowledgement that remote work
   has finished. Only then admit new `SagaStarted` events.

Unreadable durable history blocks resolver attachment; starts before recovery activation
are rejected before fanout. Admission indexes retained history once, then strictly records
and reserves each durable start before fanout. Ephemeral resolvers serialize live starts
but cannot retain fences across process restart. Required-delivery shortfalls, including partial start fanout, quarantine because
an effect owner may already have received the event. Always handle `publish_strict`
and participant-support publication errors; do not treat best-effort publish statistics
as a delivery guarantee. Shut down application actor handles before releasing stores.
Dropping the last public bus initiates shutdown of its resolver runtimes and releases journal ownership.

## Run identity and retained evidence

- A run is `(saga_type, saga_id, saga_started_at_millis)`. Keep this identity unchanged
  on redelivery. Reusing an ID for a genuinely new run requires a strictly later start
  and ordinary resolution of all previous identified work. An active owner cannot be
  replaced; an unresolved quarantine cannot be automatically reused.
- The participant journal, not a bounded in-memory latch, is authoritative. Terminal
  tombstones, dedupe markers and quarantine/accepted metadata are not automatically
  pruned. Reconstructed participants reject terminal and stale replays.
- The first ordinary terminal outcome is retained. Quarantine never downgrades and all
  quarantine history remains available. `SagaCompleted` and `SagaFailed` are different
  phases: after **success** (for example AnyOf/Quorum siblings or trailing steps) a later
  compensable or accepted effect keeps the successful business effect and does **not**
  by itself manufacture quarantine, even from compacted/reopened success history. After
  rollback or `SagaFailed`, a late effect escalates to quarantine, and unresolved
  older-run uncertainty still fences a successor.
- Managed intent is durable before execution/undo. A changed delivery trace cannot
  reopen a durably started step, even with cleared volatile caches; uncertain replay
  quarantines for reconciliation. Keep the entire original context on redelivery.
  A failed result write cannot publish success.
  `ParticipantReconciliationEvidence` retains typed output and compensation bytes when
  the normal result append fails. Those bytes do not enter public quarantine reasons.
  If the store rejects this evidence too, reconciliation must use application/external
  records; logs are not a transactional substitute. Do not automatically replay it.
- Execution-bearing legacy journals without run identity require reconciliation, not
  guessed identity or automatic resume. Registration-only legacy history may admit
  its first identified run. Previous archive tags are preserved; appended run,
  terminal and reconciliation variants require upgraded readers. Coordinate readers
  and writers; do not roll an old binary back onto new records without a compatibility plan.

## Ordered rollback and timeouts

The resolver emits one compensation owner at a time in reverse effect-completion order.
`CompensationAccepted` does not release the next owner: authoritative completion does.
Duplicates do not advance the queue. Legacy multi-step requests are head-only and may
require reconciliation rather than concurrent execution of their remaining entries.

Rollback absorbs forward success. Late compensable effects join its queue or quarantine;
an effect materialising after its undo/ordinary failure escalates to quarantine. Failed
undo, including definitive final undo failure, keeps the unreversed effect quarantined.
Forward overall/stalled expiry starts rollback when effects exist, with renewed rollback
budgets. Started-unresolved or accepted forward work remains uncertain: known effects are
undone first, then remaining uncertainty quarantines. Expiry with unresolved undo likewise
quarantines. Participant recovery distinguishes idle admission and healthy accepted deadlines
from open execution/undo intent or an unconfirmed result. Uncertainty quarantines at any age,
using its original run identity; idle/confirmed work is not quarantined merely by elapsed
age. `RecoveryPolicy::stale_after_ms` remains an API compatibility field, not a safety proxy.
Unidentified execution blocks recovery rather than inventing an outcome.

An accepted deadline does not physically cancel remote work. `FailStep` with
`requires_compensation=false` (and `fail_accepted_workflow_step(..., false)`) is an
**explicit safe/no-undo remote contract**: the application asserts that the remote side
rejected the work or that any late effect is harmless. It is not physical cancellation.
An authoritative durable `StepExecutionFailed` then closes the accepted forward work and
the saga may fail ordinarily. Potential compensation bytes carried by acceptance are only
metadata, not a materialized effect. A real result/outcome, a requires-compensation
failure or a requested/open undo remains an obligation until resolved. An unrelated
`SagaFailed` never closes unresolved accepted work, and a genuinely late result after an
authoritative failure becomes original-run reconciliation evidence (quarantine). Genuinely
unknown remote work, or a policy that cannot make late effects safe, must use compensation
or `QuarantineSaga` and reconciliation.

A published `SagaFailed` reply is a statement of the resolver's current evidence, not a
promise that no work is running: it may be **superseded** by `SagaQuarantined` when
genuinely later evidence arrives (late effect, uncertain result). Waiters keep the first
ordinary outcome until quarantine dominates it. Managed attached-bus ingress records
durable intent and then publishes `StepStarted` strictly **before** business execution; a
publication failure prevents execution. Observer/hook callback timing may still follow
actor borrowing, and hooks stay application-idempotent. With an attached bus, an
explicit helper emit sink observes an already-published `StepStarted`; it must not
forward that buffered start to the bus again. Managed wrappers skip the duplicate
publication while preserving observer hooks. Raw/custom participants that
publish `StepStarted` only after running do not provide this visibility.

**Irreversible declared effects.** A `CompletedWithEffect` step with no compensation
capability is irreversible once dispatch is declared. Any later saga failure (or an
undo that cannot exist) requires reconciliation/quarantine; it is never an automatic
ordinary resolution and the library does not invent compensation.

## Declared effect hooks

`CompletedWithEffect` uses `dispatch_effect` on `SagaParticipant`, `AsyncSagaParticipant`
or `SagaWorkflowParticipant`. It receives an `EffectDispatchRequest`; success requires
`EffectDispatchOutcome::Durable { receipt }`. Only the async participant hook is a future.
The default returns `EffectDispatchError::Unsupported`; unsupported or failed dispatch
quarantines and never publishes step success. Compensation metadata is persisted before
dispatch. Plain `Completed` does not require a hook; arbitrary effects inside
`execute_step` are still application-owned. Plain `Completed` (and accepted completion)
persists the full business result with a **single strict proof append** (tag 15) before
publication. A declared effect keeps two stages: result/compensation before dispatch, then
the proof only after durable handoff; a crash in that window is genuinely uncertain
and quarantines rather than becoming automatically confirmed. Old raw results are not
silently promoted to confirmed proof.

Implement durable outbox/idempotency semantics in the hook. After successful dispatch,
`ParticipantForwardOutcomeRecorded` retains the returned receipt along with the original
completion context, input, output and compensation data, strictly before publication. It is
a local confirmation record, **not** a library-managed external outbox. Persist application
receipts at handoff and reconcile the crash window before local confirmation. Accepted
execution IDs must remain stable within a run and distinguish different runs; a callback ID
that names multiple retained runs requires explicit application reconciliation. Panic
quarantine does not establish whether an external effect happened.

Pooled actor shutdown initiated inside a scheduler callback can drain asynchronously
in the pinned core runtime. The public lifecycle must disappear without an ownership
cycle, but backend ownership is released as that drain completes, not necessarily at
the instant the last bus clone is dropped. Coordinate explicit store close/reopen with
runtime completion; the journal-release regression checks bounded eventual release.

## Capacity and maintenance

- Participant LMDB journal/dedupe maps use `SAGA_LMDB_MAP_SIZE_BYTES` (default 1 GiB).
  Resolver LMDB uses `LmdbTerminalResolverJournal::open_with_map_size(path, bytes)`;
  `open` defaults to 256 MiB. Choose capacity at startup, monitor retained usage, and
  follow backend/backup procedures for resizing; there is no implied live-resize API.
- `TerminalResolverJournal::compact_terminal_detail()` returns removed-row count.
  LMDB removes obsolete detail of ordinarily resolved runs but keeps their terminal
  fences and monotonic sequence. Every unresolved **or quarantined** run keeps all
  evidence, even with contradictory ordinary terminal rows. Failed runs also keep
  known compensable-completion/acceptance fingerprints, distinguishing harmless
  replay from new uncertainty after compaction/reopen. Corruption aborts maintenance. Journals without maintenance return `Unsupported`, not pretend success.
- In-process admission caches (runs, completions, accepted fingerprints) are bounded and
  miss to bounded durable point/range lookups instead of scanning all history. An
  ephemeral resolver cannot forget replay fences, so at capacity it **refuses new
  admission** explicitly rather than silently evicting; size it or use a durable
  journal. `SAGA_ADMISSION_CAPACITY` defaults to 262,144 resident IDs **and full runs**
  per resolver, with a fingerprint budget of four times that value. Reusing one ID
  does not bypass the run budget. Active fingerprint overflow quarantines visibly;
  unresolved recovery above capacity refuses attach. A bounded per-ID miss that cannot
  fit refuses admission rather than retaining a partial history. Custom lookup journals
  must implement `read_saga_bounded`; no global-read fallback is used. Attach still
  scans retained history once, so startup time/temporary memory require planning.
  Durable starts still fsync under per-type admission serialization; that cost protects
  admission authority and is not removable without replacing the fence.
- Compaction does not bound permanent fence count. Storage has irreducible per-run
  replay-fence cost; capacity exhaustion is an error, never permission to forget IDs.
  Participant journals have no automatic safe-compaction API. Alert on storage errors,
  quarantine growth and held recovery output before capacity is exhausted.
- `prune_saga`/`prune_saga_strict` and backend `prune` are destructive administrative
  primitives: they remove replay protection and are **not** routine terminal cleanup.
  Do not use them to unblock a saga or replace safe resolver compaction.

## Application hooks and restart

The library does not promise that arbitrary application hooks run exactly once across a
restart. Terminal side-effect hooks and effect hooks **must be idempotent**. Managed
ingress fences duplicate/stale ordinary terminal callbacks for its supported saga types;
quarantine notifications and arbitrary non-terminal/foreign application callbacks still
require idempotence. A confirmed forward outcome (`ParticipantForwardOutcomeRecorded`,
tag 15) is persisted by `SagaStateExt::record_forward_outcome_strict` before success.
`participant_run_evidence_strict` separates it from an unconfirmed raw result; the stored
`ParticipantForwardOutcome::completion_event()` is resent without re-execution.
A confirmed AnyOf branch (including a receipted dispatch) is already fired, not uncertain.
For AllOf, tag 16 `ParticipantDependencyCompletedRecorded { context, recorded_at_millis }`
retains run-scoped dependency observations; a relevant dependency completion is
recorded strictly (`record_dependency_completion_strict`) before it is marked seen, and
`completed_dependency_steps_strict` restores them on restart for the exact type/ID/start.
A storage failure is a visible quarantine, never effect execution. Dependency-only history
is idle, not unknown executed work.
A cold undo request hydrates retained compensation; a proved completed undo whose
acknowledgement was lost resends only `CompensationCompleted` at startup (no new request or
physical undo; ambiguous or terminally resolved history is not resent). Missing confirmation is not automatic success.

`complete_accepted_workflow_step` may return `SagaQuarantined` for late identified
completion, including after cold restart. Publish the returned event, not only
`StepCompleted`; typed reconciliation evidence retains the late output/compensation under
its original run without contaminating a successor. Result-write errors retain evidence
and quarantine where storage permits; they do not authorize another external execution.
Use `register_terminal_reply_for_run` and `take_terminal_outcome_for_run` with the full
context. Saga-ID-only compatibility waiters registered after admission bind to the unique known
active run; ambiguous multi-type ownership is not guessed. They are not an unambiguous
way to wait for concurrent saga types sharing an ID.

## Upgrading from the baseline

Baseline archive writers record no run identity. **Drain every in-flight saga** (started,
accepted, compensating or unresolved) and verify ordinary resolution before upgrading;
unconfirmed or unidentified legacy history blocks store open, and there is no repair API.
Never prune or fabricate identity to unblock an old store; preserve it (see below).

## Release and migration

This work contains breaking changes (exhaustive enums, appended archive tags, fail-closed
default effect hook, legacy-identity reconciliation). See [migration.md](migration.md).
No version bump is included; a bump and release work are prerequisites before publication.

## Administrative reconciliation

There is no built-in “clear quarantine and safely retry” API. Stop starts for the affected
identity; retain/export journal, dedupe and external receipts; query every possibly-run
operation; reconcile forward/undo outcomes out of band and record an audited application
resolution. Preserve a durable replacement replay fence before any deliberate archival
or deletion. An operator cannot turn uncertain work into safe replay merely by changing
a timestamp or pruning a journal. Prefer a separately identified, reconciled new business
operation with a **new saga ID** rather than erasing the old quarantine. There is no
built-in replacement-fence or legacy-row repair API: replacement fencing/reconciliation
is application-owned and audited (see [migration.md](migration.md)). Keep blocked legacy
stores/evidence intact; do not hand-edit or prune them to force a participant open.

External idempotency keys should include saga type, **saga ID**, original start time,
participant/step, forward-versus-undo direction, and a stable effect/execution identifier.
Never use delivery timestamp, attempt number or a fresh random value. Compensation must
itself be idempotent and safe when the forward effect never happened. Reconcile an unknown
in-flight result before retrying or compensating.

## Checklist

- [ ] Persistent stores and capacity alerts; upgraded archive readers
- [ ] Attach -> restore/bind -> activate -> new starts
- [ ] Strict publication errors observed; no automatic evidence pruning
- [ ] Declared effect hooks, stable external keys and application receipt storage
- [ ] Quarantine/reconciliation procedure; safe resolver maintenance
