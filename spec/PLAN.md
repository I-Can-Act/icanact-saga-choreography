# Choreography-Based SAGA: Design Specification

This specification describes the `icanact-saga-choreography` crate as implemented on
`main` at `5af675f`. It replaces the pre-implementation design draft previously held
in this file (preserved in git history); that draft's module layout, per-actor
`state_ext.rs` boilerplate, `RetryPolicy`, `handle_saga_event`, and Deribit example
structure do not exist in the crate and must not be used. Operational procedures live
in [`docs/operations.md`](../docs/operations.md), integration steps in
[`docs/integration_guide.md`](../docs/integration_guide.md), and breaking-change notes
in [`docs/migration.md`](../docs/migration.md). This file holds design only: no
execution status or verification claims.

## Purpose

Choreography-based saga coordination native to `icanact-core` actors: actors are the
service boundaries, and saga participation is actor behavior over a pub/sub event bus
rather than a central orchestrator process. A saga progresses by choreography events;
failure is handled by serialized reverse compensation, with quarantine whenever an
outcome is ambiguous. The crate fails closed on uncertain effects and never promises
exactly-once external execution.

## Module Map (as implemented)

```
src/
├── lib.rs                 # crate docs and public re-exports
├── context.rs             # SagaId, SagaContext, PeerId, StepId
├── events.rs              # SagaChoreographyEvent, ParticipantEvent (archived)
├── errors.rs              # StepOutput, StepError, Compensation*, effect dispatch
├── state.rs               # typestate participant states and transitions
├── support.rs             # SagaParticipantSupport<J, D>, HasSagaParticipantSupport
├── state_ext.rs           # SagaStateExt blanket protocol (strict admission/replay)
├── traits.rs              # SagaParticipant, AsyncSagaParticipant,
│                          # SagaWorkflowParticipant, DependencySpec
├── helpers.rs             # sync/async step execution & compensation protocol
├── durability.rs          # managed ingress (apply_*), accepted-step lifecycle,
│                          # recovery, lmdb submodule (LmdbJournal/LmdbDedupe)
├── journal.rs / dedupe.rs # ParticipantJournal / ParticipantDedupeStore + in-memory
├── lmdb_owner.rs          # path-keyed LMDB env ownership (feature `lmdb`)
├── bus.rs                 # SagaChoreographyBus: admission gate, resolver hosting,
│                          # terminal replies, release waiter
├── binding.rs             # participant subscription/binding helpers (strict variants)
├── workflow_contract.rs   # SagaWorkflowContract, define_saga_workflow_contract!
├── resolver.rs            # TerminalResolver state machine and TerminalPolicy
├── resolver_journal.rs    # TerminalResolverJournal (+ LMDB impl, schema 2)
├── reply_registry.rs      # terminal waiter handles
├── observer.rs / stats.rs # observer callback vocabulary; participant counters
└── testkit.rs             # SagaTestWorld and deterministic helpers
                           # (feature `test-harness`, or crate tests)
```

## Core Model

- A **run** is identified by `(saga_type, saga_id, saga_started_at_millis)`. Redelivery
  keeps the original context; a later run reusing an ID needs a strictly later start
  and ordinary resolution of every previous identified run.
- Participants exchange `SagaChoreographyEvent` values on the bus topic named by
  `saga_type`: `SagaStarted`, `StepStarted`, `StepAccepted`, `StepCompleted`,
  `StepFailed`, `CompensationRequested`, `CompensationStarted`,
  `CompensationAccepted`, `CompensationCompleted`, `CompensationFailed`,
  `SagaCompleted`, `SagaFailed`, `SagaQuarantined`.
- Each participant actor embeds one `SagaParticipantSupport<J, D>` owning its volatile
  per-run state (keyed by `SagaId`), journal `J`, dedupe store `D`, accepted-step
  metadata, and an optional attached `SagaChoreographyBus`.
- Participant-local state uses the typestate machine in `state.rs`
  (`Idle → Triggered → Executing → Completed | Failed | Compensating | Quarantined`,
  `Completed → Compensating → Compensated | Failed | Quarantined`).

## Actor Integration Contract

For each saga participant the application provides:

1. A `saga: SagaParticipantSupport<J, D>` field on the actor.
2. `HasSagaParticipantSupport` exposing that field. `SagaStateExt` is then
   blanket-implemented; it is never implemented directly and there are no per-actor
   state/journal/dedupe fields to wire.
3. Business behavior via `SagaParticipant` (sync), `AsyncSagaParticipant` (async), or
   `SagaWorkflowParticipant` objects registered through
   `HasSagaWorkflowParticipants` (one actor, several workflow contracts). Required
   surface: step name, saga types, `execute_step`, `compensate_step`; optional:
   `depends_on` (`DependencySpec`), terminal hooks, identity overrides, and
   `dispatch_effect` for `StepOutput::CompletedWithEffect`.
4. A saga-event route into the actor (a command/tell variant, or preferably a
   dedicated `SagaParticipantChannel`), forwarded to the managed ingress helpers
   `apply_sync_participant_saga_ingress`, `apply_async_participant_saga_ingress`, or
   the workflow variants. The low-level `handle_saga_event_with_emit` /
   `handle_async_saga_event_with_emit` functions are the protocol core beneath those
   helpers, not the runtime entry point.
5. Bus attachment (`SagaParticipantSupport::attach_bus`) before ingress, so the
   managed helpers can validate and republish emitted events. Subscription/binding
   helpers in `binding.rs` (`bind_*_strict` for workflow actors) subscribe the actor
   and register bound steps; they do not attach the bus.

The crate imposes no actor folder layout. Application code owns actor construction,
business state, and message enums.

## Workflow Contracts and the Startup Gate

Each saga type declares a `SagaWorkflowContract` (usually via
`define_saga_workflow_contract!`): `saga_type`, `first_step`, the step graph with
dependencies (`on_start`, `after`, `any_of`, `all_of`), failure authority, required
(terminal) steps, and overall/stalled timeouts. `validate_workflow_contract` rejects
invalid contracts at registration (duplicate steps, undeclared dependencies, cycles,
missing required terminal steps, invalid policy).

A `SagaStarted` is only fanned out when all of the following hold; admission
(durable run reservation) happens before contract diagnostics, and a refused start
never fabricates failure for an existing owner:

1. the run is admitted (durable starts are journaled and reserved before fanout);
2. a terminal policy/resolver is attached (`attach_terminal_resolver*`, durable in
   production);
3. a workflow contract is registered, the start step equals `first_step`, and every
   declared step is bound (`register_bound_workflow_step` or strict binding helpers) —
   violations publish a diagnostic `SagaFailed` and make `publish_strict` return an
   admission error;
4. for durable resolvers, recovery has been activated
   (`activate_terminal_resolver_recovery_for_contract`). Non-durable attachment starts
   activated.

Live fanout below the required minimum (`required_path_steps + terminal_resolver`)
quarantines the run, because an effect owner may already have received the event.

## Terminal Resolution

The `TerminalResolver` owns per-run completion: success criteria, failure authority,
`overall_timeout` (wall clock) and `stalled_timeout` (reset by progress), plus
per-step accepted deadlines (`AcceptedStepPolicy`: idle/hard timeouts and
`FailStep`/`QuarantineSaga` outcome). Invariants:

- Rollback is serialized: one compensation owner at a time, in reverse
  effect-completion order; accepted compensation must reach authoritative completion
  before the queue advances or terminal failure publishes.
- Forward timeout with known effects starts rollback (with renewed budgets); expiry
  or failure with unresolved rollback quarantines with evidence retained.
- Success and failure differ: late effects after `SagaCompleted` keep the successful
  effect, while after failure they escalate to quarantine, potentially superseding an
  already-returned `SagaFailed` or a successor run's ordinary outcome.
- Replay fences are durable: terminal and quarantine outcomes are journaled, and
  managed ingress rejects stale or replayed runs after restart; volatile caches are
  never authority.

## Execution Rules

- Managed ingress records durable intent before business execution and publishes
  `StepStarted` (attached bus) strictly before the step runs; a failed intent write
  prevents the effect, and a failed result write never publishes success.
- Plain `Completed` is committed by one strict journal proof append
  (`ParticipantForwardOutcomeRecorded`) before publication. Declared effects
  (`CompletedWithEffect`) persist result/compensation first, then call
  `dispatch_effect`, which fails closed (`Unsupported`) by default; only a returned
  `EffectDispatchOutcome::Durable { receipt }` allows success publication. A declared
  effect without compensation capability is irreversible.
- Accepted steps (`StepOutput::Accepted`) persist metadata, input, and compensation
  data before `StepAccepted` publishes; they resolve later through the accepted-step
  completion/failure/progress helpers, never hand-built lifecycle events.
  `FailStep(requires_compensation = false)` is an explicit safe/no-undo remote
  contract, not cancellation.
- AllOf dependency observations are journaled before they are marked seen, so a
  restart restores join progress for the exact run.

## Storage

- Traits: `ParticipantJournal` (append/read/list/prune of `ParticipantEvent`) and
  `ParticipantDedupeStore` (atomic check-and-mark). Built in: in-memory
  implementations, and LMDB/Heed implementations (`durability::lmdb::{LmdbJournal,
  LmdbDedupe}`, feature `lmdb`). Applications may implement the traits for other
  durable stores.
- `ParticipantEvent` is an rkyv archive; variants are only appended, never
  renumbered (current appended range includes run identity, terminal tombstones,
  reconciliation evidence, forward-outcome proof, and dependency observations).
  Readers must be upgraded before writers emit newer records.
- Dedupe markers are run-scoped (`(saga_started_at_millis, saga_type)` prefixed to
  the event key) and retained as replay fences; `prune`/`prune_saga*` are
  destructive administrative primitives, never routine cleanup.
- The resolver journal (`TerminalResolverJournal`) retains saga-wide history;
  its LMDB schema is 2 (schema 1 is backfilled transactionally on open), and safe
  compaction (`compact_terminal_detail`) preserves unresolved runs and every fence.

## Failure Philosophy

Any uncertainty — failed durable write, dedupe outage, partial delivery, unconfirmed
result, ambiguous undo, lost acknowledgement without proof — quarantines with
evidence retained instead of reporting success or ordinary failure. There is no
repair or safe-clear API: blocked legacy history and quarantines are reconciled by
the application out of band, and a genuinely new business operation uses a new saga
ID. External effects and application hooks must be idempotent under stable
application-owned keys.

## Testing

`SagaTestWorld` (feature `test-harness`) runs real actors against a real
`SagaChoreographyBus` with transcript capture and terminal waits
(`DeterministicContextBuilder`, `spawn_*_participant`, accepted-step helpers).
Deterministic `drive_scenario`/`drive_workflow_scenario` helpers support narrower
unit tests. `tests/saga_testkit_smoke.rs` is the compiling reference for the testkit
surface.

## Non-Goals

- No exactly-once guarantee, durable message transport, or external-effect atomicity.
- No library-managed external outbox; declared-effect hooks are application handoffs.
- No administrative repair, safe-clear, or automatic quarantine pruning.
- No prescribed application module structure, serialization format for payloads, or
  deployment/load certification.
