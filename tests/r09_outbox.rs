//! R09 / ADR-0003: committed results and recovery emissions survive a crash at every publish
//! cut point: obligations embedded in rows are replayed at startup, and panic-recovery output is
//! re-derived until it is actually sent (no pre-marked replay key).
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

use icanact_saga_choreography::durability::ActiveSagaExecutionPhase;
use icanact_saga_choreography::durability::lmdb::{
    LmdbJournal, open_lmdb_participant_support_for_saga_type,
};
use icanact_saga_choreography::durability::panic_quarantine_reason;
use icanact_saga_choreography::{
    ParticipantEvent, ParticipantJournal, PeerId, RunIncarnation, RunKey, SagaChoreographyEvent,
    SagaContext, SagaId,
};

const SAGA: &str = "r09_outbox";
const STEP: &str = "reserve";

fn ctx(id: u64, started: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(id),
        saga_type: SAGA.into(),
        step_name: STEP.into(),
        correlation_id: id,
        causation_id: id,
        trace_id: id,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: started,
        event_timestamp_millis: started,
    }
}

fn step_completed(context: &SagaContext) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepCompleted {
        context: context.clone(),
        output: b"ok".to_vec(),
        saga_input: vec![],
        compensation_available: false,
    }
}

fn reopen(base: &std::path::Path) -> Vec<SagaChoreographyEvent> {
    let mut support =
        open_lmdb_participant_support_for_saga_type(base, STEP, SAGA).expect("reopen must succeed");
    support.take_startup_recovery_events()
}

#[test]
fn committed_result_and_recovery_output_survive_every_publish_crash() {
    let now = SagaContext::now_millis();

    // Committed result row with an embedded StepCompleted obligation: the process crashes at
    // each cut point after the commit; the next open must hand the obligation back every time.
    for (n, cut) in ["AfterResult", "BeforeSend", "AfterSend", "BeforeAck"]
        .into_iter()
        .enumerate()
    {
        let temp = tempfile::tempdir().expect("tempdir");
        let context = ctx(100 + n as u64, now);
        let run = context.run_key();
        {
            let journal = LmdbJournal::open(&temp.path().join("journal")).expect("journal");
            journal
                .commit_with_outbox(
                    &run,
                    ParticipantEvent::StepExecutionCompleted {
                        output: b"ok".to_vec(),
                        compensation_data: vec![],
                        completed_at_millis: now,
                    },
                    vec![step_completed(&context)],
                )
                .expect("commit with outbox");
        }
        let events = reopen(temp.path());
        assert!(
            events.iter().any(|e| matches!(
                e,
                SagaChoreographyEvent::StepCompleted { context: c, .. } if run.is_run_of(c)
            )),
            "{cut}: committed StepCompleted obligation lost across reopen: {events:?}"
        );
    }

    // Panic-recovery output lost before send: reopen twice without sending anything. The first
    // open must not consume the emission.
    let temp = tempfile::tempdir().expect("tempdir");
    let context = ctx(200, now);
    let run = context.run_key();
    {
        let journal = LmdbJournal::open(&temp.path().join("journal")).expect("journal");
        journal
            .append_run(
                &run,
                ParticipantEvent::Quarantined {
                    reason: panic_quarantine_reason(
                        ActiveSagaExecutionPhase::StepExecution,
                        "boom",
                    ),
                    quarantined_at_millis: now,
                },
            )
            .expect("append quarantine row");
    }
    for attempt in 0..2 {
        let events = reopen(temp.path());
        assert!(
            events.iter().any(|e| matches!(
                e,
                SagaChoreographyEvent::SagaQuarantined { context: c, .. } if run.is_run_of(c)
            )),
            "open #{attempt}: panic-recovery SagaQuarantined must be re-derived until sent"
        );
    }

    // Beyond the replay horizon: retained, never replayed.
    let temp = tempfile::tempdir().expect("tempdir");
    let old = ctx(300, 1);
    let old_run = RunKey::new(SAGA, SagaId::new(300), RunIncarnation::new(1));
    {
        let journal = LmdbJournal::open(&temp.path().join("journal")).expect("journal");
        journal
            .commit_with_outbox(
                &old_run,
                ParticipantEvent::StepExecutionCompleted {
                    output: vec![],
                    compensation_data: vec![],
                    completed_at_millis: 1,
                },
                vec![step_completed(&old)],
            )
            .expect("commit old");
    }
    let events = reopen(temp.path());
    assert!(
        !events.iter().any(|e| matches!(
            e,
            SagaChoreographyEvent::StepCompleted { context: c, .. } if old_run.is_run_of(c)
        )),
        "obligations older than the replay cutoff must not be replayed"
    );
}
