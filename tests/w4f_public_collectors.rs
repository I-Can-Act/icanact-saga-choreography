//! W4-review R6: the public `collect_startup_recovery_events*` include the durable outbox replay,
//! so a custom durable journal gets committed obligations back without calling
//! `collect_outbox_replay_events` itself.
use icanact_saga_choreography::{
    InMemoryDedupe, InMemoryJournal, ParticipantEvent, ParticipantJournal, PeerId,
    SagaChoreographyEvent, SagaContext, SagaId, collect_startup_recovery_events_for_saga_type,
};

const SAGA: &str = "w4f_public_collectors";
const STEP: &str = "reserve";

#[test]
fn public_collector_replays_committed_outbox_obligations() {
    let now = SagaContext::now_millis();
    let context = SagaContext {
        saga_id: SagaId::new(7),
        saga_type: SAGA.into(),
        step_name: STEP.into(),
        correlation_id: 7,
        causation_id: 7,
        trace_id: 7,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: now,
        event_timestamp_millis: now,
    };
    let run = context.run_key();
    let journal = InMemoryJournal::new();
    journal
        .commit_with_outbox(
            &run,
            ParticipantEvent::StepExecutionCompleted {
                output: b"ok".to_vec(),
                compensation_data: vec![],
                completed_at_millis: now,
            },
            vec![SagaChoreographyEvent::StepCompleted {
                context: context.clone(),
                output: b"ok".to_vec(),
                saga_input: vec![],
                compensation_available: false,
            }],
        )
        .expect("commit with outbox");

    let events =
        collect_startup_recovery_events_for_saga_type(&journal, &InMemoryDedupe::new(), STEP, SAGA)
            .expect("collect");
    assert!(
        events.iter().any(|e| matches!(
            e,
            SagaChoreographyEvent::StepCompleted { context: c, .. } if run.is_run_of(c)
        )),
        "committed StepCompleted obligation missing from public collector: {events:?}"
    );
}
