//! W4-review R6 / R2 on the async participant adapter: `SafeToRetry` is durable (marker row),
//! a retried undo commits its own `CompensationStarted`, the retry survives a restart, and a crash
//! mid-undo reopens `Quarantined` without re-running the undo.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

use icanact_saga_choreography::durability::lmdb::{
    LmdbDedupe, LmdbJournal, open_lmdb_participant_support_for_saga_type,
};
use icanact_saga_choreography::{
    AsyncSagaParticipant, CompensationError, CompensationOutput, HasSagaParticipantSupport,
    ParticipantEvent, ParticipantJournal, PeerId, SagaBoxFuture, SagaChoreographyEvent,
    SagaContext, SagaFailureDetails, SagaId, SagaParticipantSupport, SagaStateEntry, SagaStateExt,
    StepError, StepOutput, compensation_requested, handle_async_saga_event_with_emit,
};

const STEP: &str = "reserve";
const TYPE: &str = "w4f_async_retry";

type Support = SagaParticipantSupport<LmdbJournal, LmdbDedupe>;

struct Actor {
    saga: Support,
    failures_left: usize,
    /// `CompensationStarted` rows visible at each undo invocation.
    starts_seen: Vec<usize>,
}

impl HasSagaParticipantSupport for Actor {
    type Journal = LmdbJournal;
    type Dedupe = LmdbDedupe;
    fn saga_support(&self) -> &Support {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut Support {
        &mut self.saga
    }
}

impl AsyncSagaParticipant for Actor {
    type Error = String;
    fn step_name(&self) -> &str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[TYPE]
    }
    fn execute_step<'a>(
        &'a mut self,
        _: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<StepOutput, StepError>> {
        Box::pin(async {
            Ok(StepOutput::Completed {
                output: vec![1],
                compensation_data: vec![9],
            })
        })
    }
    fn compensate_step<'a>(
        &'a mut self,
        context: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<CompensationOutput, CompensationError>> {
        let starts = self
            .saga_journal()
            .read_run(&context.run_key())
            .expect("read")
            .iter()
            .filter(|row| {
                matches!(
                    row.event.transition(),
                    ParticipantEvent::CompensationStarted { .. }
                )
            })
            .count();
        self.starts_seen.push(starts);
        let failing = self.failures_left > 0;
        self.failures_left = self.failures_left.saturating_sub(1);
        Box::pin(async move {
            if failing {
                Err(CompensationError::SafeToRetry {
                    reason: "transient".into(),
                })
            } else {
                Ok(CompensationOutput::Completed)
            }
        })
    }
}

fn open(base: &std::path::Path, failures: usize) -> Actor {
    Actor {
        saga: open_lmdb_participant_support_for_saga_type(base, STEP, TYPE).expect("open"),
        failures_left: failures,
        starts_seen: Vec::new(),
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

fn request(context: &SagaContext, attempt: u32) -> SagaChoreographyEvent {
    let mut context = context.clone();
    context.attempt = attempt;
    compensation_requested(context, "downstream", "boom", vec![STEP.to_string()])
}

async fn feed(actor: &mut Actor, event: SagaChoreographyEvent) -> Vec<SagaChoreographyEvent> {
    let mut emitted = Vec::new();
    let _ = handle_async_saga_event_with_emit(actor, event, |e| emitted.push(e)).await;
    emitted
}

fn started(context: &SagaContext) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: context.clone(),
        payload: vec![7],
    }
}

fn is_retryable(events: &[SagaChoreographyEvent]) -> bool {
    events
        .iter()
        .any(|e| matches!(e, SagaChoreographyEvent::CompensationFailedRetryable { .. }))
}

fn completed(events: &[SagaChoreographyEvent]) -> bool {
    events
        .iter()
        .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. }))
}

#[tokio::test]
async fn async_safe_to_retry_emits_retryable_then_retry_commits_its_own_start() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut actor = open(temp.path(), 1);
    let context = ctx(1);
    feed(&mut actor, started(&context)).await;

    let first = feed(&mut actor, request(&context, 0)).await;
    assert!(is_retryable(&first), "{first:?}");
    assert!(
        matches!(
            actor.saga_states_ref().get(&context.run_key()),
            Some(SagaStateEntry::Compensating(_))
        ),
        "SafeToRetry keeps the run Compensating"
    );
    let second = feed(&mut actor, request(&context, 1)).await;
    assert!(completed(&second), "{second:?}");
    assert_eq!(
        actor.starts_seen,
        vec![1, 2],
        "retry needs its own durable start"
    );
}

#[tokio::test]
async fn async_retry_survives_restart_between_failure_and_rerequest() {
    let temp = tempfile::tempdir().expect("tempdir");
    let context = ctx(2);
    {
        let mut actor = open(temp.path(), 1);
        feed(&mut actor, started(&context)).await;
        let first = feed(&mut actor, request(&context, 0)).await;
        assert!(is_retryable(&first), "{first:?}");
    }
    let mut actor = open(temp.path(), 0);
    assert!(matches!(
        actor.saga_states_ref().get(&context.run_key()),
        Some(SagaStateEntry::Compensating(_))
    ));
    let second = feed(&mut actor, request(&context, 1)).await;
    assert!(completed(&second), "{second:?}");
    assert_eq!(
        actor.starts_seen.len(),
        1,
        "undo re-invoked once after restart"
    );
}

#[tokio::test]
async fn async_crash_mid_retried_undo_reopens_quarantined_and_never_reruns_the_undo() {
    let temp = tempfile::tempdir().expect("tempdir");
    let context = ctx(3);
    let run = context.run_key();
    {
        let actor = open(temp.path(), 0);
        let now = SagaContext::now_millis();
        let recorded = |attempt: u32| {
            let mut c = context.clone();
            c.attempt = attempt;
            ParticipantEvent::CompensationRequestRecorded {
                context: c,
                failed_step: "downstream".into(),
                reason: "boom".into(),
                failure: SagaFailureDetails {
                    step_name: "downstream".into(),
                    participant_id: "downstream".into(),
                    error_code: None,
                    error_message: "boom".into(),
                    at_millis: now,
                },
                steps_to_compensate: vec![STEP.into()],
                requested_at_millis: now,
            }
        };
        for event in [
            ParticipantEvent::StepExecutionCompleted {
                output: vec![1],
                compensation_data: vec![9],
                completed_at_millis: now,
            },
            recorded(0),
            ParticipantEvent::CompensationStarted {
                attempt: 0,
                started_at_millis: now,
            },
            recorded(1),
        ] {
            actor
                .saga_journal()
                .append_run(&run, event)
                .expect("append");
        }
    }
    let mut actor = open(temp.path(), 0);
    let startup = actor.saga.take_startup_recovery_events();
    assert!(
        !startup
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::CompensationRequested { .. })),
        "{startup:?}"
    );
    assert!(
        startup
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::SagaQuarantined { .. })),
        "{startup:?}"
    );
    assert!(matches!(
        actor.saga_states_ref().get(&run),
        Some(SagaStateEntry::Quarantined(_))
    ));
    feed(&mut actor, request(&context, 1)).await;
    assert!(actor.starts_seen.is_empty(), "the undo must not run again");
}
