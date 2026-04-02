# LLM Integration Guide

Use this guide when you want to add a saga workflow to an `icanact-core` application with `icanact-saga-choreography`.

This crate does not require any specific actor folder layout or project structure. It only provides:

- the saga event model
- participant traits
- embedded saga support state
- helper functions for execution, compensation, and recovery

Your application decides how actors, modules, and messages are organized.

## Minimal Model

To make an actor saga-capable:

1. embed `SagaParticipantSupport<Journal, Dedupe>` in the actor
2. implement `HasSagaParticipantSupport`
3. implement `SagaParticipant`
4. route incoming `SagaChoreographyEvent` messages through `apply_sync_participant_saga_ingress(...)` or `apply_async_participant_saga_ingress(...)`

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

## Event Flow

Typical flow:

1. publish `SagaStarted` for saga type `example_workflow`
2. `step_a` receives it and emits `StepCompleted`
3. `step_b` reacts to `StepCompleted` from `step_a` and emits its own `StepCompleted`
4. `step_c` reacts to `StepCompleted` from `step_b`
5. if a step fails with compensation required, `CompensationRequested` is published and earlier participants compensate

## Routing Saga Events

When your actor receives a saga event inside its normal actor command handling, pass it through the ingress helper rather than calling `handle_saga_event(...)` directly. The ingress helper is the runtime-facing path because it validates emitted transitions and republishes emitted choreography events through the attached bus.

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
let _resolver = world.attach_order_lifecycle_terminal_resolver("testkit");
let actor = world.spawn_sync_participant(MyActor::default(), MyCmd::SagaEvent);

let ctx = DeterministicContextBuilder::default()
    .with_saga_id(7)
    .with_saga_type("order_lifecycle")
    .with_step_name("start")
    .build();

world.start_saga(ctx.clone(), b"payload".to_vec());
let terminal = world.wait_for_terminal(ctx.saga_id, Duration::from_secs(1));
let transcript = world.transcript_for_saga(ctx.saga_id);

// Assert on:
// - terminal outcome
// - transcript sequence
// - normal actor ask/snapshot replies
// - shared journal/dedupe stores if provided to the actor

actor.shutdown();
```

## Recovery

On startup or restart, use `recover_sagas(...)` to enumerate non-terminal sagas from the journal and decide how your app should resume or reconcile them.

## Notes

- `SagaParticipantSupport` is the canonical integration path.
- The crate stays storage-generic; your app can choose in-memory, LMDB, or another backend.
- Keep your actor business logic generic to your domain; this crate does not impose any actor module structure.
- For e2e tests, prefer `SagaTestWorld` over calling `handle_saga_event(...)` directly in unit-style loops.
