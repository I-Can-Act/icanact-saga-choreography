//! W4-review R6: `CompensationFailedRetryable` is a valid emission for an accepted (`Executing`)
//! step whose compensation-start commit failed; dropping it left the resolver to learn only
//! through a timeout.
use icanact_saga_choreography::{
    PeerId, SagaChoreographyEvent, SagaContext, SagaId, SagaParticipantState, SagaStateEntry,
    is_valid_emitted_transition,
};

#[test]
fn retryable_is_valid_for_executing_accepted_step() {
    let context = SagaContext {
        saga_id: SagaId::new(1),
        saga_type: "t".into(),
        step_name: "s".into(),
        correlation_id: 1,
        causation_id: 1,
        trace_id: 1,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: 1,
        event_timestamp_millis: 1,
    };
    let executing = SagaStateEntry::Executing(
        SagaParticipantState::new(
            context.saga_id,
            "t".into(),
            "s".into(),
            1,
            1,
            PeerId::default(),
            1,
        )
        .trigger("e", 1)
        .start_execution(2),
    );
    let retryable = SagaChoreographyEvent::CompensationFailedRetryable {
        context,
        participant_id: "p".into(),
        error: "compensation start commit failed".into(),
    };
    assert!(is_valid_emitted_transition(Some(&executing), &retryable));
}
