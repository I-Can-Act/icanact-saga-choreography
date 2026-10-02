//! R13 / ADR-0004 §2.5 on the workflow adapter: a `SafeToRetry` undo is not a quarantine. The
//! participant emits `CompensationFailedRetryable`, stays `Compensating` with its undo data
//! (also across a restart), and re-invokes the undo when the resolver re-requests it.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use icanact_saga_choreography::durability::lmdb::{
    LmdbDedupe, LmdbJournal, open_lmdb_participant_support_for_saga_type,
};
use icanact_saga_choreography::{
    CompensationError, CompensationOutput, HasSagaParticipantSupport, HasSagaWorkflowParticipants,
    PeerId, SagaChoreographyEvent, SagaContext, SagaId, SagaParticipantSupport,
    SagaWorkflowParticipant, StepError, StepOutput,
    apply_sync_workflow_participant_saga_ingress_with_hooks, compensation_requested,
};

const STEP: &str = "reserve";
const TYPE: &str = "r13_workflow";

static SERIAL: Mutex<()> = Mutex::new(());
static UNDO_CALLS: AtomicUsize = AtomicUsize::new(0);
static UNDO_FAILURES_LEFT: AtomicUsize = AtomicUsize::new(0);
static QUARANTINE_HOOKS: AtomicUsize = AtomicUsize::new(0);

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
        _: &mut Actor,
        _: &SagaContext,
        data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        assert_eq!(
            data,
            [9],
            "undo bytes must be the durable compensation data"
        );
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
    fn on_quarantined(&self, _: &mut Actor, _: &SagaContext, _: &str) {
        QUARANTINE_HOOKS.fetch_add(1, Ordering::SeqCst);
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

fn feed(actor: &mut Actor, event: SagaChoreographyEvent) -> Vec<SagaChoreographyEvent> {
    let mut emitted = Vec::new();
    let _report = apply_sync_workflow_participant_saga_ingress_with_hooks(
        actor,
        event,
        |_, _| {},
        |_| {},
        |_, e| emitted.push(e.clone()),
    );
    emitted
}

fn request(context: &SagaContext, attempt: u32) -> SagaChoreographyEvent {
    let mut context = context.clone();
    context.attempt = attempt;
    compensation_requested(context, "downstream", "boom", vec![STEP.to_string()])
}

fn start(actor: &mut Actor, context: &SagaContext) {
    let emitted = feed(
        actor,
        SagaChoreographyEvent::SagaStarted {
            context: context.clone(),
            payload: vec![7],
        },
    );
    assert!(
        emitted
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::StepCompleted { .. })),
        "{emitted:?}"
    );
}

fn reset(failures: usize) -> std::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    UNDO_CALLS.store(0, Ordering::SeqCst);
    UNDO_FAILURES_LEFT.store(failures, Ordering::SeqCst);
    QUARANTINE_HOOKS.store(0, Ordering::SeqCst);
    guard
}

fn is_retryable(events: &[SagaChoreographyEvent]) -> bool {
    events
        .iter()
        .any(|e| matches!(e, SagaChoreographyEvent::CompensationFailedRetryable { .. }))
}

#[test]
fn workflow_safe_to_retry_emits_retryable_and_reinvokes_undo() {
    let _guard = reset(1);
    let temp = tempfile::tempdir().expect("tempdir");
    let mut actor = open(temp.path());
    let context = ctx(1);
    start(&mut actor, &context);

    let first = feed(&mut actor, request(&context, 0));
    assert!(
        is_retryable(&first),
        "SafeToRetry must emit retryable: {first:?}"
    );
    assert!(
        !first.iter().any(|e| matches!(
            e,
            SagaChoreographyEvent::CompensationFailed { .. }
                | SagaChoreographyEvent::SagaQuarantined { .. }
        )),
        "SafeToRetry must not fail or quarantine: {first:?}"
    );
    assert_eq!(QUARANTINE_HOOKS.load(Ordering::SeqCst), 0);

    let second = feed(&mut actor, request(&context, 1));
    assert_eq!(
        UNDO_CALLS.load(Ordering::SeqCst),
        2,
        "re-request re-invokes the undo"
    );
    assert!(
        second
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
        "{second:?}"
    );
}

#[test]
fn workflow_retry_survives_restart_between_failure_and_rerequest() {
    let _guard = reset(1);
    let temp = tempfile::tempdir().expect("tempdir");
    let context = ctx(2);
    {
        let mut actor = open(temp.path());
        start(&mut actor, &context);
        let first = feed(&mut actor, request(&context, 0));
        assert!(is_retryable(&first), "{first:?}");
    }

    let mut actor = open(temp.path());
    let second = feed(&mut actor, request(&context, 1));
    assert_eq!(
        UNDO_CALLS.load(Ordering::SeqCst),
        2,
        "re-request after restart must re-invoke the undo with the durable data"
    );
    assert!(
        second
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
        "{second:?}"
    );
}
