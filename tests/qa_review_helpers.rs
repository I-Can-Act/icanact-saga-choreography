//! Review remediation for the sync and async participant helpers: ordinary
//! `SagaFailed` must never hide open local intent / unreversed effects, confirmed
//! forward proof is persisted before success is published, a cold actor recovers
//! confirmed effects on demand for a fresh undo request, and a restarted AnyOf
//! participant neither re-executes nor falsely quarantines.

// Each test crate uses only a subset of the shared fixture re-exports.
#[allow(unused_imports)]
mod support;

use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Waker};
use std::time::Duration;

use icanact_saga_choreography::{
    AcceptedStepPolicy, AcceptedStepTimeoutOutcome, AsyncSagaParticipant, CompensationError,
    CompensationOutput, DependencySpec, EffectDispatchError, EffectDispatchOutcome,
    EffectDispatchRequest, HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal,
    ParticipantEvent, ParticipantJournal, ParticipantTerminalKind, PeerId, SagaBoxFuture,
    SagaChoreographyEvent, SagaContext, SagaFailureDetails, SagaId, SagaParticipant,
    SagaParticipantSupport, SagaStateExt, StepError, StepExecutionId, StepOutput,
    handle_async_saga_event_with_emit, handle_saga_event_with_emit,
};
use support::{FaultDedupe, FaultJournal, FaultTrigger, JournalOp};

type Journal = FaultJournal<InMemoryJournal>;
type Dedupe = FaultDedupe<InMemoryDedupe>;

#[derive(Clone, Copy, Debug)]
enum Mode {
    Sync,
    Async,
}
const MODES: [Mode; 2] = [Mode::Sync, Mode::Async];

#[derive(Default)]
struct Counters {
    effects: AtomicUsize,
    undos: AtomicUsize,
    failed_hooks: AtomicUsize,
    quarantine_hooks: AtomicUsize,
}

struct Actor {
    saga: SagaParticipantSupport<Journal, Dedupe>,
    shared: Arc<Counters>,
    with_effect: bool,
    accept: bool,
    fail_terminal: bool,
    deps: DependencySpec,
}

impl Actor {
    fn new(world: &World) -> Self {
        Self {
            saga: SagaParticipantSupport::new(world.journal.clone(), world.dedupe.clone()),
            shared: world.shared.clone(),
            with_effect: false,
            accept: false,
            fail_terminal: false,
            deps: DependencySpec::OnSagaStart,
        }
    }

    fn effect(&self) -> Result<StepOutput, StepError> {
        self.shared.effects.fetch_add(1, Ordering::SeqCst);
        if self.fail_terminal {
            return Err(StepError::Terminal {
                reason: "business refused".into(),
            });
        }
        Ok(if self.accept {
            StepOutput::Accepted {
                execution_id: StepExecutionId::new("exec-1"),
                policy: AcceptedStepPolicy {
                    idle_timeout: Duration::from_secs(60),
                    hard_timeout: Duration::from_secs(60),
                    timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
                },
                compensation_data: vec![42],
            }
        } else if self.with_effect {
            StepOutput::CompletedWithEffect {
                output: vec![1],
                compensation_data: vec![42],
                effect: "notify".into(),
            }
        } else {
            StepOutput::Completed {
                output: vec![1],
                compensation_data: vec![42],
            }
        })
    }

    fn undo(&self) -> Result<CompensationOutput, CompensationError> {
        self.shared.undos.fetch_add(1, Ordering::SeqCst);
        Ok(CompensationOutput::Completed)
    }
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

impl SagaParticipant for Actor {
    type Error = String;
    fn step_name(&self) -> &str {
        "a"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["order"]
    }
    fn depends_on(&self) -> DependencySpec {
        self.deps.clone()
    }
    fn execute_step(&mut self, _: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
        self.effect()
    }
    fn compensate_step(
        &mut self,
        _: &SagaContext,
        _: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        self.undo()
    }
    fn dispatch_effect(
        &mut self,
        _: &EffectDispatchRequest<'_>,
    ) -> Result<EffectDispatchOutcome, EffectDispatchError> {
        Ok(EffectDispatchOutcome::Durable {
            receipt: "r-1".into(),
        })
    }
    fn on_saga_failed(&mut self, _: &SagaContext, _: &str) {
        self.shared.failed_hooks.fetch_add(1, Ordering::SeqCst);
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
        &["order"]
    }
    fn depends_on(&self) -> DependencySpec {
        self.deps.clone()
    }
    fn execute_step<'a>(
        &'a mut self,
        _: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<StepOutput, StepError>> {
        Box::pin(async move { self.effect() })
    }
    fn compensate_step<'a>(
        &'a mut self,
        _: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<CompensationOutput, CompensationError>> {
        Box::pin(async move { self.undo() })
    }
    fn dispatch_effect<'a>(
        &'a mut self,
        _: &'a EffectDispatchRequest<'a>,
    ) -> SagaBoxFuture<'a, Result<EffectDispatchOutcome, EffectDispatchError>> {
        Box::pin(async move {
            Ok(EffectDispatchOutcome::Durable {
                receipt: "r-1".into(),
            })
        })
    }
    fn on_saga_failed(&mut self, _: &SagaContext, _: &str) {
        self.shared.failed_hooks.fetch_add(1, Ordering::SeqCst);
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
            journal: FaultJournal::new(InMemoryJournal::new()),
            dedupe: FaultDedupe::new(InMemoryDedupe::new()),
            shared: Arc::default(),
        }
    }
    /// A cold actor (no volatile state) over the same durable stores.
    fn actor(&self) -> Actor {
        Actor::new(self)
    }
    fn effects(&self) -> usize {
        self.shared.effects.load(Ordering::SeqCst)
    }
    fn undos(&self) -> usize {
        self.shared.undos.load(Ordering::SeqCst)
    }
    fn failed_hooks(&self) -> usize {
        self.shared.failed_hooks.load(Ordering::SeqCst)
    }
    fn quarantine_hooks(&self) -> usize {
        self.shared.quarantine_hooks.load(Ordering::SeqCst)
    }
    fn rows(&self) -> Vec<ParticipantEvent> {
        self.journal
            .read(SagaId::new(7))
            .unwrap()
            .into_iter()
            .map(|e| e.event)
            .collect()
    }
    fn seed(&self, events: Vec<ParticipantEvent>) {
        self.journal
            .append(
                SagaId::new(7),
                ParticipantEvent::ParticipantRunRecorded {
                    saga_type: "order".into(),
                    saga_started_at_millis: 100,
                    recorded_at_millis: 1,
                },
            )
            .unwrap();
        for event in events {
            self.journal.append(SagaId::new(7), event).unwrap();
        }
    }
    fn terminal_kinds(&self) -> Vec<ParticipantTerminalKind> {
        self.rows()
            .into_iter()
            .filter_map(|e| match e {
                ParticipantEvent::ParticipantTerminalRecorded { outcome, .. } => Some(outcome),
                _ => None,
            })
            .collect()
    }
}

fn ctx(step: &str, trace: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(7),
        saga_type: "order".into(),
        step_name: step.into(),
        correlation_id: 7,
        causation_id: 7,
        trace_id: trace,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: 100,
        event_timestamp_millis: 100,
    }
}

fn start() -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: ctx("start", 7),
        payload: vec![9],
    }
}

fn saga_failed() -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaFailed {
        context: ctx("start", 7),
        reason: "downstream failed".into(),
        failure: None,
    }
}

fn step_completed(step: &str, trace: u64) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepCompleted {
        context: ctx(step, trace),
        output: vec![5],
        saga_input: vec![9],
        compensation_available: false,
    }
}

fn comp_request(failed_step: &str) -> SagaChoreographyEvent {
    SagaChoreographyEvent::CompensationRequested {
        context: ctx("start", 7),
        failed_step: failed_step.into(),
        reason: "downstream failed".into(),
        failure: SagaFailureDetails {
            step_name: failed_step.into(),
            participant_id: failed_step.into(),
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
    match mode {
        Mode::Sync => handle_saga_event_with_emit(actor, event, |e| out.push(e)),
        Mode::Async => {
            let mut fut = std::pin::pin!(handle_async_saga_event_with_emit(actor, event, |e| {
                out.push(e)
            }));
            let mut cx = Context::from_waker(Waker::noop());
            while fut.as_mut().poll(&mut cx).is_pending() {}
        }
    }
    out
}

fn has_step_completed(events: &[SagaChoreographyEvent]) -> bool {
    events
        .iter()
        .any(|e| matches!(e, SagaChoreographyEvent::StepCompleted { .. }))
}

fn has_quarantine(events: &[SagaChoreographyEvent]) -> bool {
    events
        .iter()
        .any(|e| matches!(e, SagaChoreographyEvent::SagaQuarantined { .. }))
}

fn has_comp_completed(events: &[SagaChoreographyEvent]) -> bool {
    events
        .iter()
        .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. }))
}

fn started() -> ParticipantEvent {
    ParticipantEvent::StepExecutionStarted {
        attempt: 1,
        started_at_millis: 100,
    }
}

fn raw_result() -> ParticipantEvent {
    ParticipantEvent::StepExecutionCompleted {
        output: vec![1],
        compensation_data: vec![42],
        completed_at_millis: 101,
    }
}

// ---- P1-1: ordinary SagaFailed must not hide local obligations ----

#[test]
fn saga_failed_with_open_forward_intent_escalates_to_quarantine() {
    for mode in MODES {
        let world = World::new();
        world.seed(vec![started()]);
        let mut actor = world.actor();
        let out = drive(mode, &mut actor, saga_failed());
        assert!(has_quarantine(&out), "{mode:?}: must visibly escalate");
        assert_eq!(world.failed_hooks(), 0, "{mode:?}: on_saga_failed cleanup");
        assert_eq!(world.quarantine_hooks(), 1, "{mode:?}");
        assert_eq!(
            world.terminal_kinds(),
            vec![ParticipantTerminalKind::Quarantined],
            "{mode:?}"
        );
        assert_eq!(world.effects(), 0, "{mode:?}");
    }
}

#[test]
fn saga_failed_with_unreversed_compensation_escalates_and_retains_evidence() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        assert!(has_step_completed(&drive(mode, &mut actor, start())));
        let before = world.rows().len();
        let out = drive(mode, &mut actor, saga_failed());
        assert!(has_quarantine(&out), "{mode:?}");
        assert_eq!(world.failed_hooks(), 0, "{mode:?}");
        assert_eq!(world.quarantine_hooks(), 1, "{mode:?}");
        assert!(world.rows().len() > before, "{mode:?}");
        assert_eq!(world.journal.call_count(JournalOp::Prune), 0, "{mode:?}");
        let evidence = actor
            .participant_run_evidence_strict(&ctx("start", 7))
            .unwrap();
        assert_eq!(evidence.compensation_data, vec![42], "{mode:?}");
        assert!(evidence.quarantined, "{mode:?}");
    }
}

#[test]
fn saga_failed_with_accepted_work_escalates_and_preserves_accepted_metadata() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        actor.accept = true;
        drive(mode, &mut actor, start());
        let out = drive(mode, &mut actor, saga_failed());
        assert!(has_quarantine(&out), "{mode:?}");
        assert_eq!(world.failed_hooks(), 0, "{mode:?}");
        let rows = world.rows();
        assert!(
            rows.iter().any(|e| matches!(e,
                ParticipantEvent::AcceptedStepRecorded { compensation_data, .. }
                    if compensation_data.as_slice() == [42])),
            "{mode:?}: accepted metadata must remain: {rows:?}"
        );
    }
}

#[test]
fn saga_failed_stays_ordinary_for_idle_failed_and_fully_reversed_participants() {
    for mode in MODES {
        // Healthy idle participant: only a run record.
        let world = World::new();
        world.seed(vec![]);
        let mut actor = world.actor();
        let out = drive(mode, &mut actor, saga_failed());
        assert!(!has_quarantine(&out), "{mode:?}: idle work quarantined");
        assert_eq!(world.failed_hooks(), 1, "{mode:?}");
        assert_eq!(
            world.terminal_kinds(),
            vec![ParticipantTerminalKind::Failed],
            "{mode:?}"
        );

        // A step whose own business failure left nothing behind.
        let world = World::new();
        let mut actor = world.actor();
        actor.fail_terminal = true;
        drive(mode, &mut actor, start());
        let out = drive(mode, &mut actor, saga_failed());
        assert!(!has_quarantine(&out), "{mode:?}");
        assert_eq!(world.failed_hooks(), 1, "{mode:?}");

        // Completed then fully undone.
        let world = World::new();
        let mut actor = world.actor();
        drive(mode, &mut actor, start());
        assert!(has_comp_completed(&drive(
            mode,
            &mut actor,
            comp_request("b")
        )));
        let out = drive(mode, &mut actor, saga_failed());
        assert!(!has_quarantine(&out), "{mode:?}: reversed work quarantined");
        assert_eq!(world.failed_hooks(), 1, "{mode:?}");
    }
}

#[test]
fn saga_failed_evidence_read_failure_fails_closed_visibly() {
    for mode in MODES {
        let world = World::new();
        world.seed(vec![]);
        // Read 1 is admission; read 2 is the evidence load.
        world
            .journal
            .fail_once(JournalOp::Read, FaultTrigger::NthCall(2));
        let mut actor = world.actor();
        let out = drive(mode, &mut actor, saga_failed());
        assert!(has_quarantine(&out), "{mode:?}: unreadable evidence");
        assert_eq!(world.failed_hooks(), 0, "{mode:?}");
    }
}

#[test]
fn saga_failed_admission_read_failure_fails_closed_visibly() {
    for mode in MODES {
        let world = World::new();
        world.seed(vec![started()]);
        world
            .journal
            .fail_once(JournalOp::Read, FaultTrigger::NthCall(1));
        let mut actor = world.actor();
        let out = drive(mode, &mut actor, saga_failed());
        assert!(has_quarantine(&out), "{mode:?}");
        assert_eq!(world.failed_hooks(), 0, "{mode:?}");
    }
}

// ---- strict forward proof before success ----

#[test]
fn confirmed_forward_outcome_is_durable_before_success_and_matches_it() {
    for mode in MODES {
        for with_effect in [false, true] {
            let world = World::new();
            let mut actor = world.actor();
            actor.with_effect = with_effect;
            let out = drive(mode, &mut actor, start());
            let published = out
                .iter()
                .find(|e| matches!(e, SagaChoreographyEvent::StepCompleted { .. }))
                .unwrap_or_else(|| panic!("{mode:?}: no success published"));
            let outcome = world
                .rows()
                .into_iter()
                .find_map(|e| match e {
                    ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome } => {
                        Some(outcome)
                    }
                    _ => None,
                })
                .unwrap_or_else(|| panic!("{mode:?}: success without durable proof"));
            assert_eq!(
                format!("{:?}", outcome.completion_event()),
                format!("{published:?}"),
                "{mode:?}"
            );
            assert_eq!(outcome.compensation_data, vec![42]);
            assert_eq!(outcome.saga_input, vec![9]);
            assert_eq!(outcome.effect.as_deref(), with_effect.then_some("notify"));
            assert_eq!(outcome.receipt.as_deref(), with_effect.then_some("r-1"));
        }
    }
}

#[test]
fn failed_forward_outcome_append_publishes_no_success_and_quarantines() {
    for mode in MODES {
        for with_effect in [false, true] {
            let world = World::new();
            world.journal.fail_always(
                JournalOp::Append,
                FaultTrigger::EventKind("ParticipantForwardOutcomeRecorded"),
            );
            let mut actor = world.actor();
            actor.with_effect = with_effect;
            let out = drive(mode, &mut actor, start());
            assert_eq!(world.effects(), 1, "{mode:?}");
            assert!(!has_step_completed(&out), "{mode:?}: unproven success");
            assert!(has_quarantine(&out), "{mode:?}");
            assert_eq!(world.quarantine_hooks(), 1, "{mode:?}");
            // Restart never repeats the effect.
            let mut cold = world.actor();
            drive(mode, &mut cold, start());
            assert_eq!(world.effects(), 1, "{mode:?}");
        }
    }
}

// ---- P2-1: on-demand recovery for a fresh undo request ----

#[test]
fn cold_actor_runs_undo_for_confirmed_completed_step_without_reexecuting() {
    for mode in MODES {
        for with_effect in [false, true] {
            let world = World::new();
            let mut first = world.actor();
            first.with_effect = with_effect;
            drive(mode, &mut first, start());
            drop(first);
            let mut cold = world.actor();
            let out = drive(mode, &mut cold, comp_request("b"));
            assert_eq!(world.undos(), 1, "{mode:?}: undo silently skipped");
            assert_eq!(world.effects(), 1, "{mode:?}: forward re-executed");
            assert!(has_comp_completed(&out), "{mode:?}");
            assert!(!has_quarantine(&out), "{mode:?}");
        }
    }
}

#[test]
fn cold_undo_with_unconfirmed_forward_state_quarantines_instead_of_skipping() {
    for mode in MODES {
        for seeded in [
            vec![started()],
            vec![started(), raw_result()],
            vec![ParticipantEvent::CompensationStarted {
                attempt: 1,
                started_at_millis: 102,
            }],
        ] {
            let world = World::new();
            world.seed(seeded);
            let mut cold = world.actor();
            let out = drive(mode, &mut cold, comp_request("b"));
            assert!(has_quarantine(&out), "{mode:?}");
            assert_eq!(world.undos(), 0, "{mode:?}");
            assert!(!has_comp_completed(&out), "{mode:?}");
            assert_eq!(world.effects(), 0, "{mode:?}");
        }
    }
}

#[test]
fn completed_undo_resends_ack_after_restart_without_rerunning() {
    for mode in MODES {
        let world = World::new();
        let mut first = world.actor();
        drive(mode, &mut first, start());
        assert!(has_comp_completed(&drive(
            mode,
            &mut first,
            comp_request("b")
        )));
        let mut cold = world.actor();
        let out = drive(mode, &mut cold, comp_request("c"));
        assert!(has_comp_completed(&out), "{mode:?}: ack not resent");
        assert!(!has_quarantine(&out), "{mode:?}");
        assert_eq!(world.undos(), 1, "{mode:?}: undo repeated");
    }
}

#[test]
fn late_forward_evidence_after_undo_quarantines_a_new_undo_request() {
    for mode in MODES {
        let world = World::new();
        let mut first = world.actor();
        drive(mode, &mut first, start());
        drive(mode, &mut first, comp_request("b"));
        // A late effect landed after the undo completed.
        world.journal.append(SagaId::new(7), raw_result()).unwrap();
        let mut cold = world.actor();
        let out = drive(mode, &mut cold, comp_request("c"));
        assert!(has_quarantine(&out), "{mode:?}");
        assert_eq!(world.undos(), 1, "{mode:?}");
    }
}

#[test]
fn undo_request_recovery_survives_read_failure_by_quarantining() {
    for mode in MODES {
        let world = World::new();
        let mut first = world.actor();
        drive(mode, &mut first, start());
        // Cold actor reads: admission, then the evidence load.
        let base = world.journal.call_count(JournalOp::Read);
        world
            .journal
            .fail_once(JournalOp::Read, FaultTrigger::NthCall(base + 2));
        let mut cold = world.actor();
        let out = drive(mode, &mut cold, comp_request("b"));
        assert_eq!(world.undos(), 0, "{mode:?}");
        assert!(has_quarantine(&out), "{mode:?}");
    }
}

// ---- P2-2: AnyOf after restart ----

static ANY: &[&str] = &["x", "y"];

#[test]
fn restarted_anyof_second_branch_ignores_confirmed_firing() {
    for mode in MODES {
        let world = World::new();
        let mut first = world.actor();
        first.deps = DependencySpec::AnyOf(ANY);
        assert!(has_step_completed(&drive(
            mode,
            &mut first,
            step_completed("x", 11)
        )));
        drop(first);
        let mut cold = world.actor();
        cold.deps = DependencySpec::AnyOf(ANY);
        let out = drive(mode, &mut cold, step_completed("y", 12));
        assert_eq!(world.effects(), 1, "{mode:?}: re-executed");
        assert!(!has_quarantine(&out), "{mode:?}: false quarantine");
        assert!(!has_step_completed(&out), "{mode:?}");
        assert_eq!(world.quarantine_hooks(), 0, "{mode:?}");
        assert!(
            !world
                .rows()
                .iter()
                .any(|e| matches!(e, ParticipantEvent::Quarantined { .. })),
            "{mode:?}"
        );
    }
}

#[test]
fn restarted_anyof_with_unconfirmed_forward_still_quarantines() {
    for mode in MODES {
        for seeded in [vec![started()], vec![started(), raw_result()]] {
            let world = World::new();
            world.seed(seeded);
            let mut cold = world.actor();
            cold.deps = DependencySpec::AnyOf(ANY);
            let out = drive(mode, &mut cold, step_completed("y", 12));
            assert!(has_quarantine(&out), "{mode:?}");
            assert_eq!(world.effects(), 0, "{mode:?}");
        }
    }
}
