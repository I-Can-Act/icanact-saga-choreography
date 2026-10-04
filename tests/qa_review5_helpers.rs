//! Review5 N2: a MANAGED (sync/async) accepted forward step whose definitive
//! rejection (`fail_accepted_workflow_step(false)`) is durably appended but whose
//! post-append journal read fails leaves a matching run marker, `Executing` state
//! and accepted cache. A later owned compensation request must trust the strict
//! journal evidence over that stale cache: zero physical undo, strict proof before ack.

// Each test crate uses only a subset of the shared fixture re-exports.
#[allow(unused_imports)]
mod support;

use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::{Context, Waker};
use std::time::Duration;

use icanact_saga_choreography::durability::fail_accepted_workflow_step;
use icanact_saga_choreography::*;
use support::{FaultDedupe, FaultJournal, FaultTrigger, JournalOp};

const TYPE: &str = "qa_r5h";
const SAGA: u64 = 9;

/// Arm the next read only AFTER a successful owned-request append. This keeps
/// admission readable and targets the strict evidence-versus-cache decision.
#[derive(Clone)]
struct Journal {
    inner: FaultJournal<InMemoryJournal>,
    fail_read_after_request: Arc<AtomicBool>,
}

impl Journal {
    fn new(inner: InMemoryJournal) -> Self {
        Self {
            inner: FaultJournal::new(inner),
            fail_read_after_request: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl std::ops::Deref for Journal {
    type Target = FaultJournal<InMemoryJournal>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ParticipantJournal for Journal {
    fn append(&self, saga_id: SagaId, event: ParticipantEvent) -> Result<u64, JournalError> {
        let owned_request = matches!(&event, ParticipantEvent::CompensationRequestRecorded { .. });
        let sequence = self.inner.append(saga_id, event)?;
        if owned_request && self.fail_read_after_request.swap(false, Ordering::SeqCst) {
            self.inner.fail_once(
                JournalOp::Read,
                FaultTrigger::NthCall(self.inner.call_count(JournalOp::Read) + 1),
            );
        }
        Ok(sequence)
    }
    fn read(&self, saga_id: SagaId) -> Result<Vec<JournalEntry>, JournalError> {
        self.inner.read(saga_id)
    }
    fn list_sagas(&self) -> Result<Vec<SagaId>, JournalError> {
        self.inner.list_sagas()
    }
    fn prune(&self, saga_id: SagaId) -> Result<(), JournalError> {
        self.inner.prune(saga_id)
    }
}

type Dedupe = FaultDedupe<InMemoryDedupe>;

#[derive(Clone, Copy, Debug)]
enum Mode {
    Sync,
    Async,
}
const MODES: [Mode; 2] = [Mode::Sync, Mode::Async];

#[derive(Default)]
struct Counters {
    undos: AtomicUsize,
    quarantine_hooks: AtomicUsize,
    completed_hooks: AtomicUsize,
}

struct Actor {
    saga: SagaParticipantSupport<Journal, Dedupe>,
    shared: Arc<Counters>,
}

impl HasSagaParticipantSupport for Actor {
    type Journal = Journal;
    type Dedupe = Dedupe;
    fn saga_support(&self) -> &SagaParticipantSupport<Journal, Dedupe> {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Journal, Dedupe> {
        &mut self.saga
    }
}

fn accepted_output() -> StepOutput {
    StepOutput::Accepted {
        execution_id: StepExecutionId::new("exec-1"),
        policy: AcceptedStepPolicy {
            idle_timeout: Duration::from_secs(60),
            hard_timeout: Duration::from_secs(3600),
            timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: false,
            },
        },
        compensation_data: vec![42],
    }
}

impl SagaParticipant for Actor {
    type Error = String;
    fn step_name(&self) -> &str {
        "a"
    }
    fn saga_types(&self) -> &[&'static str] {
        &[TYPE]
    }
    fn depends_on(&self) -> DependencySpec {
        DependencySpec::OnSagaStart
    }
    fn execute_step(&mut self, _: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
        Ok(accepted_output())
    }
    fn compensate_step(
        &mut self,
        _: &SagaContext,
        _: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        self.shared.undos.fetch_add(1, Ordering::SeqCst);
        Ok(CompensationOutput::Completed)
    }
    fn on_compensation_completed(&mut self, _: &SagaContext) {
        self.shared.completed_hooks.fetch_add(1, Ordering::SeqCst);
    }
    fn on_quarantined(&mut self, _: &SagaContext, _: &str) {
        self.shared.quarantine_hooks.fetch_add(1, Ordering::SeqCst);
    }
}

impl AsyncSagaParticipant for Actor {
    type Error = String;
    fn step_name(&self) -> &str {
        "a"
    }
    fn saga_types(&self) -> &[&'static str] {
        &[TYPE]
    }
    fn depends_on(&self) -> DependencySpec {
        DependencySpec::OnSagaStart
    }
    fn execute_step<'a>(
        &'a mut self,
        _: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<StepOutput, StepError>> {
        Box::pin(async move { Ok(accepted_output()) })
    }
    fn compensate_step<'a>(
        &'a mut self,
        _: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<CompensationOutput, CompensationError>> {
        Box::pin(async move {
            self.shared.undos.fetch_add(1, Ordering::SeqCst);
            Ok(CompensationOutput::Completed)
        })
    }
    fn on_compensation_completed(&mut self, _: &SagaContext) {
        self.shared.completed_hooks.fetch_add(1, Ordering::SeqCst);
    }
    fn on_quarantined(&mut self, _: &SagaContext, _: &str) {
        self.shared.quarantine_hooks.fetch_add(1, Ordering::SeqCst);
    }
}

struct World {
    journal: Journal,
    dedupe: Dedupe,
    shared: Arc<Counters>,
}

impl World {
    fn new() -> Self {
        Self {
            journal: Journal::new(InMemoryJournal::new()),
            dedupe: FaultDedupe::new(InMemoryDedupe::new()),
            shared: Arc::default(),
        }
    }
    fn actor(&self) -> Actor {
        Actor {
            saga: SagaParticipantSupport::new(self.journal.clone(), self.dedupe.clone()),
            shared: self.shared.clone(),
        }
    }
    fn restarted_actor(&self) -> Actor {
        Actor {
            saga: SagaParticipantSupport::new(
                self.journal.clone(),
                FaultDedupe::new(InMemoryDedupe::new()),
            ),
            shared: self.shared.clone(),
        }
    }
    fn undos(&self) -> usize {
        self.shared.undos.load(Ordering::SeqCst)
    }
    fn quarantine_hooks(&self) -> usize {
        self.shared.quarantine_hooks.load(Ordering::SeqCst)
    }
    fn completed_hooks(&self) -> usize {
        self.shared.completed_hooks.load(Ordering::SeqCst)
    }
    fn count(&self, kind: &str) -> usize {
        self.journal
            .read(SagaId::new(SAGA))
            .unwrap()
            .into_iter()
            .filter(|e| format!("{:?}", e.event).starts_with(kind))
            .count()
    }
}

fn run_ctx() -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(SAGA),
        saga_type: TYPE.into(),
        step_name: "start".into(),
        correlation_id: SAGA,
        causation_id: SAGA,
        trace_id: SAGA,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: 100,
        event_timestamp_millis: 100,
    }
}

fn started_event() -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: run_ctx(),
        payload: b"order".to_vec(),
    }
}

fn request_event() -> SagaChoreographyEvent {
    SagaChoreographyEvent::CompensationRequested {
        context: run_ctx(),
        failed_step: "z".into(),
        reason: "downstream failed".into(),
        failure: SagaFailureDetails {
            step_name: "z".into(),
            participant_id: "z".into(),
            error_code: None,
            error_message: "downstream failed".into(),
            at_millis: 100,
        },
        steps_to_compensate: vec!["a".into()],
    }
}

fn drive(
    mode: Mode,
    actor: &mut Actor,
    event: SagaChoreographyEvent,
) -> Vec<SagaChoreographyEvent> {
    let mut out = Vec::new();
    let sink = |e| out.push(e);
    match mode {
        Mode::Sync => {
            let mut sink = sink;
            handle_saga_event_with_emit(actor, event, &mut sink);
        }
        Mode::Async => {
            let mut fut = std::pin::pin!(handle_async_saga_event_with_emit(actor, event, sink));
            let mut cx = Context::from_waker(Waker::noop());
            while fut.as_mut().poll(&mut cx).is_pending() {}
        }
    }
    out
}

fn acks(events: &[SagaChoreographyEvent]) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. }))
        .count()
}

fn rejection(requires_compensation: bool) -> AcceptedStepFailure {
    AcceptedStepFailure {
        reason: "remote rejected".into(),
        requires_compensation,
        failed_at_millis: 110,
    }
}

/// Managed start -> accepted execution, then `fail_accepted_workflow_step(false)`
/// whose POST-append read (the second journal read of the call: the pre-append
/// terminal check is the first) fails. Returns with the rejection durable and the
/// stale Executing/accepted cache + matching run marker still in place.
fn managed_rejection_with_lagging_cache(world: &World, mode: Mode) -> Actor {
    let mut actor = world.actor();
    let out = drive(mode, &mut actor, started_event());
    assert!(
        out.iter()
            .any(|e| matches!(e, SagaChoreographyEvent::StepAccepted { .. })),
        "{mode:?}: managed accept expected: {out:?}"
    );
    assert!(
        actor
            .saga_support()
            .accepted_workflow_steps
            .contains_key(&SagaId::new(SAGA))
    );
    assert_eq!(
        actor
            .saga_support()
            .saga_run_started_at
            .get(&SagaId::new(SAGA)),
        Some(&100),
        "{mode:?}: managed run marker must match"
    );
    let reads = world.journal.call_count(JournalOp::Read);
    world
        .journal
        .fail_once(JournalOp::Read, FaultTrigger::NthCall(reads + 2));
    let err = fail_accepted_workflow_step(
        &mut actor,
        SagaId::new(SAGA),
        StepExecutionId::new("exec-1"),
        rejection(false),
    )
    .expect_err("post-append read fault must surface");
    assert!(
        matches!(err, AcceptedStepError::Durability { .. }),
        "{mode:?}: {err:?}"
    );
    // Fault position: rejection row is durable; cache/state are stale.
    assert_eq!(world.count("StepExecutionFailed"), 1, "{mode:?}");
    assert!(matches!(
        actor.saga_states_ref().get(&SagaId::new(SAGA)),
        Some(SagaStateEntry::Executing(_))
    ));
    assert!(
        actor
            .saga_support()
            .accepted_workflow_steps
            .contains_key(&SagaId::new(SAGA))
    );
    assert!(
        actor
            .participant_run_evidence_strict(&run_ctx())
            .unwrap()
            .forward_definitively_rejected
    );
    actor
}

#[test]
fn managed_post_append_read_lag_never_runs_physical_undo() {
    for mode in MODES {
        let world = World::new();
        let mut actor = managed_rejection_with_lagging_cache(&world, mode);
        let out = drive(mode, &mut actor, request_event());
        assert_eq!(
            world.undos(),
            0,
            "{mode:?}: stale cache cannot authorize physical undo: {out:?}"
        );
        assert_eq!(acks(&out), 1, "{mode:?}: {out:?}");
        assert_eq!(world.count("CompensationRequestRecorded"), 1, "{mode:?}");
        assert_eq!(world.count("CompensationCompleted"), 1, "{mode:?}");
        assert_eq!(world.count("CompensationStarted"), 0, "{mode:?}");
        assert_eq!(world.quarantine_hooks(), 0, "{mode:?}");
        assert_eq!(world.completed_hooks(), 1, "{mode:?}");
        assert!(
            !actor
                .saga_support()
                .accepted_workflow_steps
                .contains_key(&SagaId::new(SAGA)),
            "{mode:?}: stale accepted cache evicted"
        );
        assert!(
            actor
                .saga_support()
                .resolved_workflow_steps
                .contains(&(SagaId::new(SAGA), StepExecutionId::new("exec-1"))),
            "{mode:?}: accepted step marked resolved"
        );
        assert!(matches!(
            actor.saga_states_ref().get(&SagaId::new(SAGA)),
            Some(SagaStateEntry::Compensated(_))
        ));
    }
}

#[test]
fn managed_lag_proof_write_failure_quarantines_without_ack_or_undo() {
    for mode in MODES {
        let world = World::new();
        let mut actor = managed_rejection_with_lagging_cache(&world, mode);
        world.journal.fail_once(
            JournalOp::Append,
            FaultTrigger::EventKind("CompensationCompleted"),
        );
        let out = drive(mode, &mut actor, request_event());
        assert_eq!(acks(&out), 0, "{mode:?}: {out:?}");
        assert_eq!(world.undos(), 0, "{mode:?}");
        assert_eq!(world.count("CompensationCompleted"), 0, "{mode:?}");
        assert_eq!(world.quarantine_hooks(), 1, "{mode:?}");
        assert!(
            out.iter().any(|event| matches!(event,
                SagaChoreographyEvent::SagaQuarantined { context, .. }
                    if context.saga_type.as_ref() == TYPE
                        && context.saga_id == run_ctx().saga_id
                        && context.saga_started_at_millis == 100
            )),
            "{mode:?}: quarantine must be emitted for the original run: {out:?}"
        );
        let evidence = actor.participant_run_evidence_strict(&run_ctx()).unwrap();
        assert!(
            evidence.quarantined && evidence.undo_required,
            "{mode:?}: {evidence:?}"
        );
        assert_eq!(world.count("CompensationRequestRecorded"), 1, "{mode:?}");
        assert!(matches!(
            actor.saga_states_ref().get(&SagaId::new(SAGA)),
            Some(SagaStateEntry::Quarantined(_))
        ));
    }
}

#[test]
fn managed_owned_request_post_append_read_error_never_uses_stale_cache() {
    for mode in MODES {
        let world = World::new();
        let mut actor = managed_rejection_with_lagging_cache(&world, mode);
        world
            .journal
            .fail_read_after_request
            .store(true, Ordering::SeqCst);
        let out = drive(mode, &mut actor, request_event());
        world.journal.disarm();
        assert_eq!(
            world.count("CompensationRequestRecorded"),
            1,
            "{mode:?}: request must be durable"
        );
        assert!(!world.journal.fail_read_after_request.load(Ordering::SeqCst));
        assert_eq!(
            world.undos(),
            0,
            "{mode:?}: post-request evidence failure must not use stale cache: {out:?}"
        );
        assert_eq!(acks(&out), 0, "{mode:?}: {out:?}");
        assert_eq!(world.count("CompensationStarted"), 0, "{mode:?}");
        assert_eq!(world.count("CompensationCompleted"), 0, "{mode:?}");
        assert_eq!(world.quarantine_hooks(), 1, "{mode:?}");
        assert!(
            out.iter().any(|event| matches!(event,
                SagaChoreographyEvent::SagaQuarantined { context, reason, .. }
                    if context.saga_type.as_ref() == TYPE
                        && context.saga_id == run_ctx().saga_id
                        && context.saga_started_at_millis == 100
                        && reason.contains("undo evidence unavailable before local cache use")
            )),
            "{mode:?}: targeted evidence failure must quarantine: {out:?}"
        );
        let evidence = actor.participant_run_evidence_strict(&run_ctx()).unwrap();
        assert!(
            evidence.quarantined && evidence.undo_required,
            "{mode:?}: {evidence:?}"
        );
    }
}

#[test]
fn managed_lag_evidence_read_error_never_falls_back_to_cache() {
    for mode in MODES {
        let world = World::new();
        let mut actor = managed_rejection_with_lagging_cache(&world, mode);
        // Every read from here on fails: evidence is unavailable.
        let base = world.journal.call_count(JournalOp::Read);
        for n in 1..=16 {
            world
                .journal
                .fail_always(JournalOp::Read, FaultTrigger::NthCall(base + n));
        }
        let out = drive(mode, &mut actor, request_event());
        world.journal.disarm();
        assert_eq!(world.undos(), 0, "{mode:?}: no cache fallback: {out:?}");
        assert_eq!(acks(&out), 0, "{mode:?}: {out:?}");
        assert_eq!(world.count("CompensationCompleted"), 0, "{mode:?}");
    }
}

#[test]
fn managed_lag_duplicate_and_cold_restart_resend_without_repeating_work() {
    for mode in MODES {
        let world = World::new();
        let mut actor = managed_rejection_with_lagging_cache(&world, mode);
        let first = drive(mode, &mut actor, request_event());
        assert_eq!(acks(&first), 1, "{mode:?}: {first:?}");
        let dup = drive(mode, &mut actor, request_event());
        // Hot ingress dedupe suppresses the whole duplicate request; a lost
        // acknowledgement is resent from durable proof by cold/startup recovery.
        assert_eq!(acks(&dup), 0, "{mode:?}: hot duplicate: {dup:?}");
        let mut cold = world.restarted_actor();
        let again = drive(mode, &mut cold, request_event());
        assert_eq!(acks(&again), 1, "{mode:?}: {again:?}");
        assert_eq!(world.undos(), 0, "{mode:?}");
        assert_eq!(world.count("CompensationCompleted"), 1, "{mode:?}");
    }
}

#[test]
fn managed_true_failure_keeps_real_undo_ownership() {
    // requires_compensation=true is NOT a no-effect proof: the accepted work may
    // have had an effect, so the owned undo still runs exactly once.
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        drive(mode, &mut actor, started_event());
        fail_accepted_workflow_step(
            &mut actor,
            SagaId::new(SAGA),
            StepExecutionId::new("exec-1"),
            rejection(true),
        )
        .unwrap();
        assert!(
            !actor
                .participant_run_evidence_strict(&run_ctx())
                .unwrap()
                .forward_definitively_rejected,
            "{mode:?}: true failure is not a definitive no-effect rejection"
        );
        let out = drive(mode, &mut actor, request_event());
        assert_eq!(world.undos(), 1, "{mode:?}: real undo ownership: {out:?}");
        assert_eq!(world.count("CompensationStarted"), 1, "{mode:?}");
    }
}
