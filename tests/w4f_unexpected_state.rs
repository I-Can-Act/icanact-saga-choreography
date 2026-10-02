//! W4-review R6: a compensation request that meets a state it cannot act on is never a silent
//! `Applied`: an `Executing` state with no accepted undo data is quarantined with a typed
//! `ReconciliationNeeded`; an already-settled state answers `Duplicate`.
#![cfg(feature = "test-harness")]

use icanact_saga_choreography::{
    CompensationError, CompensationOutput, HasSagaParticipantSupport, InMemoryDedupe,
    InMemoryJournal, IngressOutcome, PeerId, ReconciliationCause, SagaChoreographyEvent,
    SagaContext, SagaId, SagaParticipant, SagaParticipantState, SagaParticipantSupport,
    SagaStateEntry, SagaStateExt, StepError, StepOutput, compensation_requested,
    handle_saga_event_with_emit,
};

const STEP: &str = "reserve";
const TYPE: &str = "w4f_unexpected";

type Support = SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>;

struct Actor {
    saga: Support,
    undo_calls: usize,
}

impl HasSagaParticipantSupport for Actor {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;
    fn saga_support(&self) -> &Support {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut Support {
        &mut self.saga
    }
}

impl SagaParticipant for Actor {
    type Error = String;
    fn step_name(&self) -> &str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[TYPE]
    }
    fn execute_step(&mut self, _: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
        Ok(StepOutput::Completed {
            output: vec![1],
            compensation_data: vec![9],
        })
    }
    fn compensate_step(
        &mut self,
        _: &SagaContext,
        _: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        self.undo_calls += 1;
        Ok(CompensationOutput::Completed)
    }
}

fn ctx(id: u64) -> SagaContext {
    let now = SagaContext::now_millis();
    SagaContext {
        saga_id: SagaId::new(id),
        saga_type: TYPE.into(),
        step_name: STEP.into(),
        correlation_id: id,
        causation_id: id,
        trace_id: id,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: now,
        event_timestamp_millis: now,
    }
}

fn actor() -> Actor {
    Actor {
        saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
        undo_calls: 0,
    }
}

fn base_state(context: &SagaContext) -> SagaParticipantState<icanact_saga_choreography::Triggered> {
    SagaParticipantState::new(
        context.saga_id,
        context.saga_type.clone(),
        STEP.into(),
        context.correlation_id,
        context.trace_id,
        context.initiator_peer_id,
        context.saga_started_at_millis,
    )
    .trigger("test", 1)
}

fn request(context: &SagaContext) -> SagaChoreographyEvent {
    compensation_requested(
        context.clone(),
        "downstream",
        "boom",
        vec![STEP.to_string()],
    )
}

#[test]
fn executing_state_without_undo_data_is_quarantined_not_silently_applied() {
    let mut actor = actor();
    let context = ctx(1);
    let run = context.run_key();
    actor.saga_states().insert(
        run.clone(),
        SagaStateEntry::Executing(base_state(&context).start_execution(2)),
    );
    let mut emitted = Vec::new();
    let outcome = handle_saga_event_with_emit(&mut actor, request(&context), |e| emitted.push(e));
    assert!(
        matches!(
            &outcome,
            IngressOutcome::ReconciliationNeeded(r)
                if r.run == run && matches!(r.cause, ReconciliationCause::MissingCompletedState)
        ),
        "{outcome:?}"
    );
    assert!(
        emitted
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::SagaQuarantined { .. })),
        "{emitted:?}"
    );
    assert!(matches!(
        actor.saga_states_ref().get(&run),
        Some(SagaStateEntry::Quarantined(_))
    ));
    assert_eq!(actor.undo_calls, 0);
}

#[test]
fn already_compensated_state_answers_duplicate() {
    let mut actor = actor();
    let context = ctx(2);
    let run = context.run_key();
    actor.saga_states().insert(
        run.clone(),
        SagaStateEntry::Compensated(
            base_state(&context)
                .start_execution(2)
                .start_compensation(3)
                .complete_compensation(4),
        ),
    );
    let mut emitted = Vec::new();
    let outcome = handle_saga_event_with_emit(&mut actor, request(&context), |e| emitted.push(e));
    assert!(matches!(outcome, IngressOutcome::Duplicate), "{outcome:?}");
    assert!(emitted.is_empty(), "{emitted:?}");
    assert_eq!(actor.undo_calls, 0);
}
