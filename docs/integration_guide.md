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
4. route incoming `SagaChoreographyEvent` messages to `handle_saga_event(...)`

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

When your actor receives a saga event, pass it to the crate helper:

```rust
use icanact_saga_choreography::{SagaChoreographyEvent, handle_saga_event};

fn on_saga_event(
    actor: &mut impl SagaParticipant + icanact_saga_choreography::SagaStateExt,
    event: SagaChoreographyEvent,
) {
    handle_saga_event(actor, event);
}
```

## Recovery

On startup or restart, use `recover_sagas(...)` to enumerate non-terminal sagas from the journal and decide how your app should resume or reconcile them.

## Notes

- `SagaParticipantSupport` is the canonical integration path.
- The crate stays storage-generic; your app can choose in-memory, LMDB, or another backend.
- Keep your actor business logic generic to your domain; this crate does not impose any actor module structure.
