# LLM Integration Guide

Use this guide when you want to add a saga workflow to an `icanact-core` application with `icanact-saga-choreography`.

This crate does not require any specific actor folder layout or project structure. It only provides:

- the saga event model
- participant traits
- embedded saga support state
- helper functions for execution, compensation, and recovery
- workflow contract + startup gate enforcement APIs

Your application decides how actors, modules, and messages are organized.

## Minimal Model

To make an actor saga-capable:

1. embed `SagaParticipantSupport<Journal, Dedupe>` in the actor
2. implement `HasSagaParticipantSupport`
3. implement `SagaParticipant`
4. route incoming `SagaChoreographyEvent` messages through `apply_sync_participant_saga_ingress(...)` or `apply_async_participant_saga_ingress(...)`

To make a workflow startup-safe (required for non-test runtime):

1. declare a `SagaWorkflowContract` for each `saga_type`
2. register the contract on startup (`register_workflow_contract_provider`)
3. attach terminal resolver/policy for that contract
4. register bound participant steps before any `SagaStarted`

If any of these are missing, `SagaStarted` is rejected immediately with terminal failure instead of allowing a latent stall.
`register_workflow_contract_provider` does not implicitly attach a resolver; resolver attachment must succeed explicitly.

## Generic Example Workflow

Imagine a workflow with three generic steps:

- `step_a`
- `step_b`
- `step_c`

`step_a` runs when the saga starts.  
`step_b` runs after `step_a` completes.  
`step_c` runs after `step_b` completes.  
If `step_c` fails, earlier steps may compensate.

## Example Participant

```rust
use icanact_saga_choreography::{
    CompensationError, DependencySpec, HasSagaParticipantSupport, InMemoryDedupe,
    InMemoryJournal, SagaContext, SagaParticipant, SagaParticipantSupport, StepError, StepOutput,
};

pub struct StepAActor {
    pub saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
}

impl Default for StepAActor {
    fn default() -> Self {
        Self {
            saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
        }
    }
}

impl HasSagaParticipantSupport for StepAActor {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

impl SagaParticipant for StepAActor {
    type Error = String;

    fn step_name(&self) -> &str {
        "step_a"
    }

    fn saga_types(&self) -> &[&'static str] {
        &["example_workflow"]
    }

    fn depends_on(&self) -> DependencySpec {
        DependencySpec::OnSagaStart
    }

    fn execute_step(
        &mut self,
        _context: &SagaContext,
        input: &[u8],
    ) -> Result<StepOutput, StepError> {
        let output = input.to_vec();
        Ok(StepOutput::Completed {
            output,
            compensation_data: b"undo_step_a".to_vec(),
        })
    }

    fn compensate_step(
        &mut self,
        _context: &SagaContext,
        _compensation_data: &[u8],
    ) -> Result<(), CompensationError> {
        Ok(())
    }
}
```

`step_b` and `step_c` look the same, except:

- they use different `step_name()` values
- they return `DependencySpec::After("step_a")` or `DependencySpec::After("step_b")`
- they implement their own forward and compensation behavior

## Startup Registration Pattern

Use startup wiring like this:

```rust,ignore
use icanact_saga_choreography::{
    define_saga_workflow_contract, SagaChoreographyBus,
};

define_saga_workflow_contract! {
    pub struct ExampleWorkflowContract {
        saga_type: "example_workflow",
        first_step: step_a,
        failure_authority: any (),
        required_steps: [step_c],
        overall_timeout_ms: 30_000,
        stalled_timeout_ms: 10_000,
        steps: {
            step_a => { participant: "step-a", depends_on: on_start () },
            step_b => { participant: "step-b", depends_on: after [step_a] },
            step_c => { participant: "step-c", depends_on: after [step_b] }
        }
    }
}

let bus = SagaChoreographyBus::new();
bus.register_workflow_contract_provider::<ExampleWorkflowContract>()?;
let _resolver =
    bus.attach_terminal_resolver_for_contract::<ExampleWorkflowContract>("terminal-resolver")?;

// If you do not use strict workflow bind helpers, register steps explicitly:
bus.register_bound_workflow_step("example_workflow", "step_a")?;
bus.register_bound_workflow_step("example_workflow", "step_b")?;
bus.register_bound_workflow_step("example_workflow", "step_c")?;
```

For actors implementing `HasSagaWorkflowParticipants`, prefer:

- `bind_sync_workflow_participant_channel_strict(...)`
- `bind_sync_workflow_participant_tell_strict(...)`
- `bind_async_workflow_participant_channel_strict(...)`

These bind the subscriber path and auto-register workflow steps as bound.
Do not register steps as bound unless a real participant is wired for that step; otherwise the saga can still stall after start.

## Event Flow

Typical flow:

1. publish `SagaStarted` for saga type `example_workflow`
2. `step_a` receives it and emits `StepCompleted`
3. `step_b` reacts to `StepCompleted` from `step_a` and emits its own `StepCompleted`
4. `step_c` reacts to `StepCompleted` from `step_b`
5. if a step fails with compensation required, `CompensationRequested` is published and earlier participants compensate

At step 1, the bus validates startup invariants:

- terminal policy present
- workflow contract present
- started step equals contract `first_step`
- all contract steps are bound

For runtime publishing paths, prefer `publish_strict(...)` so partial delivery is surfaced immediately as an error instead of being silently ignored.

## Routing Saga Events

When your actor receives a saga event inside its normal actor command handling, pass it through the ingress helper rather than calling low-level event handlers (for example `handle_saga_event_with_emit(...)`) directly. The ingress helper is the runtime-facing path because it validates emitted transitions and republishes emitted choreography events through the attached bus.

```rust
use icanact_saga_choreography::{
    durability::apply_sync_participant_saga_ingress, SagaChoreographyEvent,
};

fn on_saga_event(
    actor: &mut impl SagaParticipant + icanact_saga_choreography::SagaStateExt,
    event: SagaChoreographyEvent,
) {
    apply_sync_participant_saga_ingress(actor, event, |_actor, _incoming| {}, |_invalid| {});
}
```

For async participants, use `apply_async_participant_saga_ingress(...)` with the same hook shape.

## E2E Test Example

When you want to test the real saga workflow code without rewriting the actor for tests, use `SagaTestWorld` behind the `test-harness` feature.

```rust,ignore
use std::time::Duration;

use icanact_core::local_sync::{self, SyncActor};
use icanact_saga_choreography::{
    durability::apply_sync_participant_saga_ingress, DeterministicContextBuilder,
    SagaChoreographyEvent, SagaTestWorld,
};

enum MyCmd {
    SagaEvent(SagaChoreographyEvent),
    Snapshot,
}

impl SyncActor for MyActor {
    type Contract = local_sync::contract::TellAsk;
    type Tell = MyCmd;
    type Ask = ();
    type Reply = MySnapshot;

    fn handle_tell(&mut self, msg: Self::Tell) {
        match msg {
            MyCmd::SagaEvent(event) => {
                apply_sync_participant_saga_ingress(self, event, |_actor, _incoming| {}, |_invalid| {});
            }
            MyCmd::Snapshot => {}
        }
    }

    fn handle_ask(&mut self, _msg: Self::Ask) -> Self::Reply {
        self.snapshot()
    }
}

let world = SagaTestWorld::new();
let bus = world.bus();
bus.register_workflow_contract_provider::<ExampleWorkflowContract>()?;
let _resolver =
    bus.attach_terminal_resolver_for_contract::<ExampleWorkflowContract>("testkit")?;
bus.register_bound_workflow_step("example_workflow", "step_a")?;
bus.register_bound_workflow_step("example_workflow", "step_b")?;
bus.register_bound_workflow_step("example_workflow", "step_c")?;
let actor = world.spawn_sync_participant(MyActor::default(), MyCmd::SagaEvent);

let ctx = DeterministicContextBuilder::default()
    .with_saga_id(7)
    .with_saga_type("example_workflow")
    .with_step_name("step_a")
    .build();

world.start_saga(ctx.clone(), b"payload".to_vec());
let _first_step = world.wait_for_event(
    |event| {
        matches!(
            event,
            SagaChoreographyEvent::StepCompleted { context, .. }
                if context.saga_id == ctx.saga_id && context.step_name.as_ref() == "step_a"
        )
    },
    Duration::from_secs(1),
);
let transcript = world.transcript_for_saga(ctx.saga_id);

// Assert on:
// - expected step progression
// - transcript sequence
// - normal actor ask/snapshot replies
// - shared journal/dedupe stores if provided to the actor
// If your test wires all participants from the contract, also assert terminal outcome.

actor.shutdown();
```

## Recovery

On startup or restart, enumerate participant journal state through your durability layer and decide how your application should resume or reconcile non-terminal workflows.

## Timeout Model

- `overall_timeout`: hard maximum elapsed wall-clock from saga start.
- `stalled_timeout`: resettable stall watchdog that resets on each participant progress event.

## Notes

- `SagaParticipantSupport` is the canonical integration path.
- The crate stays storage-generic; your app can choose in-memory, LMDB, or another backend.
- Keep your actor business logic generic to your domain; this crate does not impose any actor module structure.
- For e2e tests, prefer `SagaTestWorld` over manually looping on low-level handlers in unit-style tests.
