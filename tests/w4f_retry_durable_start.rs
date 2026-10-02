//! W4-review R2 (Q6): a retried undo is durable. Every undo invocation, including a retry after
//! `SafeToRetry`, commits `CompensationStarted` first; a `SafeToRetry` outcome writes a durable
//! retryable marker; a dangling `CompensationStarted` (crash mid-undo) rehydrates `Quarantined`
//! and is never re-run automatically.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use icanact_saga_choreography::durability::lmdb::{
    LmdbDedupe, LmdbJournal, open_lmdb_participant_support_for_saga_type,
};
use icanact_saga_choreography::{
    CompensationError, CompensationOutput, HasSagaParticipantSupport, HasSagaWorkflowParticipants,
    ParticipantEvent, ParticipantJournal, PeerId, SagaChoreographyEvent, SagaContext,
    SagaFailureDetails, SagaId, SagaParticipant, SagaParticipantSupport, SagaStateEntry,
    SagaStateExt, SagaWorkflowParticipant, StepError, StepOutput,
    apply_sync_workflow_participant_saga_ingress_with_hooks, compensation_requested,
    handle_saga_event_with_emit,
};

const STEP: &str = "reserve";
const TYPE: &str = "w4f_retry";

static SERIAL: Mutex<()> = Mutex::new(());
static UNDO_CALLS: AtomicUsize = AtomicUsize::new(0);
static UNDO_FAILURES_LEFT: AtomicUsize = AtomicUsize::new(0);
/// `CompensationStarted` rows visible in the journal at each undo invocation.
static STARTS_SEEN: Mutex<Vec<usize>> = Mutex::new(Vec::new());

type Support = SagaParticipantSupport<LmdbJournal, LmdbDedupe>;

struct Actor {
    saga: Support,
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

fn starts_in_journal(actor: &Actor, context: &SagaContext) -> usize {
    actor
        .saga_journal()
        .read_run(&context.run_key())
        .expect("read journal")
        .iter()
        .filter(|row| {
            matches!(
                row.event.transition(),
                ParticipantEvent::CompensationStarted { .. }
            )
        })
        .count()
}

fn undo(actor: &Actor, context: &SagaContext) -> Result<CompensationOutput, CompensationError> {
    STARTS_SEEN
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(starts_in_journal(actor, context));
    UNDO_CALLS.fetch_add(1, Ordering::SeqCst);
    let failing = UNDO_FAILURES_LEFT
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok();
    if failing {
        Err(CompensationError::SafeToRetry {
            reason: "transient".into(),
        })
    } else {
        Ok(CompensationOutput::Completed)
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
        context: &SagaContext,
        _: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        undo(self, context)
    }
}

struct Reserve;
static RESERVE: Reserve = Reserve;
static WORKFLOWS: [&'static dyn SagaWorkflowParticipant<Actor>; 1] = [&RESERVE];

impl SagaWorkflowParticipant<Actor> for Reserve {
    fn step_name(&self) -> &'static str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[TYPE]
    }
    fn execute_step(
        &self,
        _: &mut Actor,
        _: &SagaContext,
        _: &[u8],
    ) -> Result<StepOutput, StepError> {
        Ok(StepOutput::Completed {
            output: vec![1],
            compensation_data: vec![9],
        })
    }
    fn compensate_step(
        &self,
        actor: &mut Actor,
        context: &SagaContext,
        _: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        undo(actor, context)
    }
}

impl HasSagaWorkflowParticipants for Actor {
    fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
        &WORKFLOWS
    }
}

fn open(base: &std::path::Path) -> Actor {
    Actor {
        saga: open_lmdb_participant_support_for_saga_type(base, STEP, TYPE).expect("open"),
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

fn reset(failures: usize) -> std::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    UNDO_CALLS.store(0, Ordering::SeqCst);
    UNDO_FAILURES_LEFT.store(failures, Ordering::SeqCst);
    STARTS_SEEN
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clear();
    guard
}

fn starts_seen() -> Vec<usize> {
    STARTS_SEEN
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

fn feed_sync(actor: &mut Actor, event: SagaChoreographyEvent) -> Vec<SagaChoreographyEvent> {
    let mut emitted = Vec::new();
    let _ = handle_saga_event_with_emit(actor, event, |e| emitted.push(e));
    emitted
}

fn feed_workflow(actor: &mut Actor, event: SagaChoreographyEvent) -> Vec<SagaChoreographyEvent> {
    let mut emitted = Vec::new();
    let _ = apply_sync_workflow_participant_saga_ingress_with_hooks(
        actor,
        event,
        |_, _| {},
        |_| {},
        |_, e| emitted.push(e.clone()),
    );
    emitted
}

fn started(context: &SagaContext) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: context.clone(),
        payload: vec![7],
    }
}

#[test]
fn sync_retried_undo_commits_compensation_started_first() {
    let _guard = reset(1);
    let temp = tempfile::tempdir().expect("tempdir");
    let mut actor = open(temp.path());
    let context = ctx(1);
    feed_sync(&mut actor, started(&context));
    feed_sync(&mut actor, request(&context, 0));
    let second = feed_sync(&mut actor, request(&context, 1));
    assert!(
        second
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
        "{second:?}"
    );
    assert_eq!(
        starts_seen(),
        vec![1, 2],
        "each undo invocation (incl. the retry) must see its own durable CompensationStarted"
    );
}

#[test]
fn workflow_retried_undo_commits_compensation_started_first() {
    let _guard = reset(1);
    let temp = tempfile::tempdir().expect("tempdir");
    let mut actor = open(temp.path());
    let context = ctx(2);
    feed_workflow(&mut actor, started(&context));
    feed_workflow(&mut actor, request(&context, 0));
    let second = feed_workflow(&mut actor, request(&context, 1));
    assert!(
        second
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
        "{second:?}"
    );
    assert_eq!(starts_seen(), vec![1, 2]);
}

/// Journal after a crash mid-retried-undo: completed, req0, started, req1.
fn append_crash_mid_retry(actor: &Actor, context: &SagaContext) {
    let run = context.run_key();
    let journal = actor.saga_journal();
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
        journal.append_run(&run, event).expect("append");
    }
}

#[test]
fn crash_mid_retried_undo_reopens_quarantined_without_replaying_the_request() {
    let _guard = reset(0);
    let temp = tempfile::tempdir().expect("tempdir");
    let context = ctx(3);
    let run = context.run_key();
    {
        let actor = open(temp.path());
        append_crash_mid_retry(&actor, &context);
    }
    let mut actor = open(temp.path());
    let startup = actor.saga.take_startup_recovery_events();
    assert!(
        !startup
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::CompensationRequested { .. })),
        "a dangling CompensationStarted must not replay the re-request: {startup:?}"
    );
    assert!(
        startup
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::SagaQuarantined { .. })),
        "SagaQuarantined must be re-derived for startup publication: {startup:?}"
    );
    assert!(
        matches!(
            actor.saga_states_ref().get(&run),
            Some(SagaStateEntry::Quarantined(_))
        ),
        "an uncertain undo rehydrates Quarantined"
    );
    let emitted = feed_sync(&mut actor, request(&context, 1));
    assert_eq!(UNDO_CALLS.load(Ordering::SeqCst), 0, "{emitted:?}");
}

#[test]
fn safe_to_retry_is_durable_and_retry_allowed_after_restart() {
    let _guard = reset(1);
    let temp = tempfile::tempdir().expect("tempdir");
    let context = ctx(4);
    {
        let mut actor = open(temp.path());
        feed_sync(&mut actor, started(&context));
        feed_sync(&mut actor, request(&context, 0));
    }
    let mut actor = open(temp.path());
    assert!(
        matches!(
            actor.saga_states_ref().get(&context.run_key()),
            Some(SagaStateEntry::Compensating(_))
        ),
        "SafeToRetry marker keeps the run Compensating across a restart"
    );
    let second = feed_sync(&mut actor, request(&context, 1));
    assert!(
        second
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
        "{second:?}"
    );
    assert_eq!(UNDO_CALLS.load(Ordering::SeqCst), 2);
}
