//! R13 / ADR-0004 §2.5: a `SafeToRetry` undo is reported as `CompensationFailedRetryable`
//! (stamped with the request's attempt), the participant stays `Compensating` with its undo
//! data, `on_quarantined` is not called, and a later re-request retries the undo.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

use icanact_saga_choreography::{
    CompensationError, CompensationOutput, HasSagaParticipantSupport, InMemoryDedupe,
    InMemoryJournal, IngressOutcome, PeerId, RunKey, SagaChoreographyEvent, SagaContext, SagaId,
    SagaParticipant, SagaParticipantSupport, SagaStateEntry, SagaStateExt, StepError, StepOutput,
    handle_saga_event_with_emit,
};

const SAGA: &str = "r13_retry_saga";
const STEP: &str = "reserve";

type Support = SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>;

struct Actor {
    saga: Support,
    undo_results: Vec<Result<CompensationOutput, CompensationError>>,
    undo_inputs: Vec<Vec<u8>>,
    quarantined_calls: usize,
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
        &[SAGA]
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
        data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        self.undo_inputs.push(data.to_vec());
        self.undo_results.remove(0)
    }
    fn on_quarantined(&mut self, _: &SagaContext, _: &str) {
        self.quarantined_calls += 1;
    }
}

fn now() -> u64 {
    static NOW: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *NOW.get_or_init(SagaContext::now_millis)
}

fn ctx() -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(13),
        saga_type: SAGA.into(),
        step_name: STEP.into(),
        correlation_id: 13,
        causation_id: 13,
        trace_id: 1,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: now(),
        event_timestamp_millis: now(),
    }
}

fn run() -> RunKey {
    ctx().run_key()
}

fn request(attempt: u32) -> SagaChoreographyEvent {
    let mut context = ctx().next_step("upstream".into());
    context.attempt = attempt;
    SagaChoreographyEvent::CompensationRequested {
        context,
        failed_step: "upstream".into(),
        reason: "boom".into(),
        failure: icanact_saga_choreography::SagaFailureDetails {
            step_name: "upstream".into(),
            participant_id: "upstream".into(),
            error_code: None,
            error_message: "boom".into(),
            at_millis: 1,
        },
        steps_to_compensate: vec![STEP.into()],
    }
}

fn actor(undo_results: Vec<Result<CompensationOutput, CompensationError>>) -> Actor {
    let mut actor = Actor {
        saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
        undo_results,
        undo_inputs: Vec::new(),
        quarantined_calls: 0,
    };
    let started = SagaChoreographyEvent::SagaStarted {
        context: ctx(),
        payload: vec![7],
    };
    let outcome = handle_saga_event_with_emit(&mut actor, started, |_| {});
    assert!(matches!(outcome, IngressOutcome::Applied), "{outcome:?}");
    actor
}

#[test]
fn safe_to_retry_undo_emits_retryable_and_stays_compensating_then_retries() {
    let mut actor = actor(vec![
        Err(CompensationError::SafeToRetry {
            reason: "downstream busy".into(),
        }),
        Ok(CompensationOutput::Completed),
    ]);

    let mut emitted = Vec::new();
    let outcome = handle_saga_event_with_emit(&mut actor, request(2), |e| emitted.push(e));
    assert!(matches!(outcome, IngressOutcome::Applied), "{outcome:?}");

    let retryable: Vec<_> = emitted
        .iter()
        .filter_map(|e| match e {
            SagaChoreographyEvent::CompensationFailedRetryable { context, .. } => {
                Some(context.attempt)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        retryable,
        vec![2],
        "retryable stamped with request attempt: {emitted:?}"
    );
    assert!(
        !emitted
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::CompensationFailed { .. })),
        "a retryable failure must not be reported as terminal: {emitted:?}"
    );
    assert_eq!(
        actor.quarantined_calls, 0,
        "on_quarantined only for quarantine"
    );
    assert!(matches!(
        actor.saga_states_ref().get(&run()),
        Some(SagaStateEntry::Compensating(_))
    ));

    // The resolver re-requests with a bumped attempt: the undo runs again with the same data.
    let mut emitted = Vec::new();
    let outcome = handle_saga_event_with_emit(&mut actor, request(3), |e| emitted.push(e));
    assert!(matches!(outcome, IngressOutcome::Applied), "{outcome:?}");
    assert_eq!(actor.undo_inputs, vec![vec![9u8], vec![9u8]]);
    assert!(
        emitted
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
        "{emitted:?}"
    );
    assert!(matches!(
        actor.saga_states_ref().get(&run()),
        Some(SagaStateEntry::Compensated(_))
    ));
    assert_eq!(actor.quarantined_calls, 0);
}
