//! Re-challenge (round 3) regressions for the sync and async participant helpers:
//!
//! * P2-a: a managed step's `StepStarted` is strictly published on the attached bus
//!   (after durable intent, before the business callback), and a failed publication
//!   prevents the effect and quarantines visibly;
//! * P2-e: a plain `Completed` step is made durable by ONE strict forward-proof
//!   append (no raw result row first); declared effects keep result-before-dispatch
//!   and proof-after-handoff;
//! * AllOf restart liveness: relevant dependency completions are strictly recorded
//!   before they count, hydrated per run after a restart, and never leak across runs.

// Each test crate uses only a subset of the shared fixture re-exports.
#[allow(unused_imports)]
mod support;

use std::future::Future;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::{Context, Waker};
use std::time::{Duration, Instant};

use icanact_saga_choreography::*;
use support::{FaultDedupe, FaultJournal, FaultTrigger, JournalOp};

const TYPE: &str = "qa_rc";

define_saga_workflow_contract! {
    struct RcContract {
        saga_type: "qa_rc",
        first_step: a,
        failure_authority: any (),
        required_steps: [a],
        overall_timeout_ms: 20_000,
        stalled_timeout_ms: 400,
        steps: {
            a => { participant: "a", depends_on: on_start () }
        }
    }
}

type Journal = FaultJournal<InMemoryJournal>;
type Dedupe = FaultDedupe<InMemoryDedupe>;

#[derive(Clone, Copy, Debug)]
enum Mode {
    Sync,
    Async,
}
const MODES: [Mode; 2] = [Mode::Sync, Mode::Async];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Execute,
    Dispatch,
}

type Probe = Arc<dyn Fn(Phase) + Send + Sync>;

#[derive(Default)]
struct Counters {
    effects: AtomicUsize,
    dispatches: AtomicUsize,
    undos: AtomicUsize,
    quarantine_hooks: AtomicUsize,
}

struct Actor {
    saga: SagaParticipantSupport<Journal, Dedupe>,
    shared: Arc<Counters>,
    with_effect: bool,
    deps: DependencySpec,
    probe: Option<Probe>,
}

impl Actor {
    fn new(world: &World) -> Self {
        Self {
            saga: SagaParticipantSupport::new(world.journal.clone(), world.dedupe.clone()),
            shared: world.shared.clone(),
            with_effect: false,
            deps: DependencySpec::OnSagaStart,
            probe: None,
        }
    }

    fn probe(&self, phase: Phase) {
        if let Some(probe) = &self.probe {
            probe(phase);
        }
    }

    fn effect(&self) -> Result<StepOutput, StepError> {
        self.probe(Phase::Execute);
        self.shared.effects.fetch_add(1, Ordering::SeqCst);
        Ok(if self.with_effect {
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

    fn dispatch(&self) -> Result<EffectDispatchOutcome, EffectDispatchError> {
        self.probe(Phase::Dispatch);
        self.shared.dispatches.fetch_add(1, Ordering::SeqCst);
        Ok(EffectDispatchOutcome::Durable {
            receipt: "r-1".into(),
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
        &[TYPE]
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
        self.dispatch()
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
        Box::pin(async move { self.dispatch() })
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
    fn dispatches(&self) -> usize {
        self.shared.dispatches.load(Ordering::SeqCst)
    }
    fn undos(&self) -> usize {
        self.shared.undos.load(Ordering::SeqCst)
    }
    fn rows(&self) -> Vec<ParticipantEvent> {
        rows_of(&self.journal)
    }
}

fn rows_of(journal: &Journal) -> Vec<ParticipantEvent> {
    journal
        .read(SagaId::new(7))
        .unwrap()
        .into_iter()
        .map(|e| e.event)
        .collect()
}

fn kind(event: &ParticipantEvent) -> &'static str {
    match event {
        ParticipantEvent::StepExecutionStarted { .. } => "started",
        ParticipantEvent::StepExecutionCompleted { .. } => "raw_result",
        ParticipantEvent::ParticipantForwardOutcomeRecorded { .. } => "forward_outcome",
        ParticipantEvent::ParticipantDependencyCompletedRecorded { .. } => "dependency",
        ParticipantEvent::ParticipantReconciliationEvidence { .. } => "evidence",
        ParticipantEvent::Quarantined { .. } => "quarantined",
        _ => "other",
    }
}

fn kinds(rows: &[ParticipantEvent]) -> Vec<&'static str> {
    rows.iter()
        .map(kind)
        .filter(|kind| *kind != "other")
        .collect()
}

fn context_at(id: u64, step: &str, started_at: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(id),
        saga_type: TYPE.into(),
        step_name: step.into(),
        correlation_id: id,
        causation_id: id,
        trace_id: id,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: started_at,
        event_timestamp_millis: started_at,
    }
}

fn ctx(step: &str, trace: u64, started_at: u64) -> SagaContext {
    let mut context = context_at(7, step, started_at);
    context.trace_id = trace;
    context
}

fn start_of(context: &SagaContext) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: context.clone(),
        payload: vec![9],
    }
}

fn start() -> SagaChoreographyEvent {
    start_of(&ctx("start", 7, 100))
}

fn dep_completed(step: &str, trace: u64, started_at: u64) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepCompleted {
        context: ctx(step, trace, started_at),
        output: vec![5],
        saga_input: vec![9],
        compensation_available: false,
    }
}

fn comp_request() -> SagaChoreographyEvent {
    SagaChoreographyEvent::CompensationRequested {
        context: ctx("start", 7, 100),
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

fn drive_with(
    mode: Mode,
    actor: &mut Actor,
    event: SagaChoreographyEvent,
    mut sink: impl FnMut(SagaChoreographyEvent),
) {
    match mode {
        Mode::Sync => handle_saga_event_with_emit(actor, event, &mut sink),
        Mode::Async => {
            let mut fut = std::pin::pin!(handle_async_saga_event_with_emit(actor, event, sink));
            let mut cx = Context::from_waker(Waker::noop());
            while fut.as_mut().poll(&mut cx).is_pending() {}
        }
    }
}

fn drive(
    mode: Mode,
    actor: &mut Actor,
    event: SagaChoreographyEvent,
) -> Vec<SagaChoreographyEvent> {
    let mut out = Vec::new();
    drive_with(mode, actor, event, |e| out.push(e));
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

fn wait_until(limit: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if pred() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    pred()
}

type Seen = Arc<Mutex<Vec<SagaChoreographyEvent>>>;

fn count_seen(seen: &Seen, pred: impl Fn(&SagaChoreographyEvent) -> bool) -> usize {
    seen.lock().unwrap().iter().filter(|e| pred(e)).count()
}

fn is_step_started(event: &SagaChoreographyEvent) -> bool {
    matches!(event, SagaChoreographyEvent::StepStarted { .. })
}

/// Real bus with the contract bound, filler subscribers (so the required path is
/// deliverable) and a capturing subscriber. `reject_step_started` makes one
/// subscriber refuse `StepStarted`, i.e. a genuine partial-delivery failure.
fn real_bus(reject_step_started: bool) -> (SagaChoreographyBus, Seen) {
    let bus = SagaChoreographyBus::new();
    bus.register_workflow_contract_provider::<RcContract>()
        .unwrap();
    bus.register_bound_workflow_step(TYPE, "a").unwrap();
    let seen: Seen = Arc::default();
    let sink = Arc::clone(&seen);
    bus.subscribe_saga_type_fn(TYPE, move |event| {
        sink.lock().unwrap().push(event.clone());
        true
    });
    if reject_step_started {
        bus.subscribe_saga_type_fn(TYPE, |event| !is_step_started(event));
    }
    for _ in 0..2 {
        bus.subscribe_saga_type_fn(TYPE, |_| true);
    }
    (bus, seen)
}

// ------------------------------------------------------------------ P2-a

#[test]
fn start_is_visible_on_the_real_bus_while_business_work_outlives_resolver_deadlines() {
    for (mode, id) in [(Mode::Sync, 9_101), (Mode::Async, 9_102)] {
        let (bus, seen) = real_bus(false);
        bus.attach_terminal_resolver_for_contract::<RcContract>("rc-resolver")
            .unwrap();
        let run = context_at(id, "a", SagaContext::now_millis());
        bus.publish_strict(start_of(&run)).unwrap();

        let world = World::new();
        let mut actor = world.actor();
        actor.saga.attach_bus(bus.clone());
        let visible = Arc::new(AtomicBool::new(false));
        let (probe_seen, probe_visible) = (Arc::clone(&seen), Arc::clone(&visible));
        actor.probe = Some(Arc::new(move |phase| {
            if phase == Phase::Execute {
                // The business callback is "slow": it first checks the bus saw the
                // start, then outlives the 400ms stalled budget.
                probe_visible.store(
                    wait_until(Duration::from_secs(2), || {
                        count_seen(&probe_seen, is_step_started) == 1
                    }),
                    Ordering::SeqCst,
                );
                std::thread::sleep(Duration::from_millis(1_500));
            }
        }));
        drive(mode, &mut actor, start_of(&run));

        assert!(
            visible.load(Ordering::SeqCst),
            "{mode:?}: StepStarted was not on the bus while the callback ran"
        );
        assert_eq!(
            count_seen(&seen, is_step_started),
            1,
            "{mode:?}: start published once"
        );
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            count_seen(&seen, |e| matches!(
                e,
                SagaChoreographyEvent::SagaFailed { .. }
            )),
            0,
            "{mode:?}: ordinary Failed escaped while managed work was running"
        );
        assert!(
            matches!(
                bus.take_terminal_outcome_for_run(&run),
                Some(SagaTerminalOutcome::Quarantined { .. })
            ),
            "{mode:?}: the first terminal must be a quarantine for known-running work"
        );
    }
}

#[test]
fn start_is_published_after_durable_intent_and_before_the_effect() {
    for mode in MODES {
        let (bus, seen) = real_bus(false);
        let world = World::new();
        let mut actor = world.actor();
        actor.saga.attach_bus(bus);
        let journal = world.journal.clone();
        let observed = Arc::new(Mutex::new(None));
        let (probe_seen, probe_observed) = (Arc::clone(&seen), Arc::clone(&observed));
        actor.probe = Some(Arc::new(move |phase| {
            if phase == Phase::Execute {
                let published = wait_until(Duration::from_secs(2), || {
                    count_seen(&probe_seen, is_step_started) == 1
                });
                *probe_observed.lock().unwrap() = Some((published, kinds(&rows_of(&journal))));
            }
        }));
        drive(mode, &mut actor, start());
        let (published, rows) = observed.lock().unwrap().clone().expect("effect ran");
        assert!(
            published,
            "{mode:?}: effect ran before the start was on the bus"
        );
        assert_eq!(
            rows,
            vec!["started"],
            "{mode:?}: intent durable, nothing else yet"
        );
    }
}

#[test]
fn failed_start_publication_prevents_the_effect_and_quarantines_visibly() {
    for mode in MODES {
        let (bus, _seen) = real_bus(true);
        let world = World::new();
        let mut actor = world.actor();
        actor.saga.attach_bus(bus);
        let out = drive(mode, &mut actor, start());
        assert_eq!(
            world.effects(),
            0,
            "{mode:?}: effect ran without a published start"
        );
        assert!(!has_step_completed(&out), "{mode:?}");
        assert!(
            has_quarantine(&out),
            "{mode:?}: must be a visible quarantine"
        );
        let rows = kinds(&world.rows());
        assert!(
            rows.contains(&"started") && rows.contains(&"quarantined"),
            "{mode:?}: intent and quarantine evidence retained: {rows:?}"
        );
        let mut cold = world.actor();
        drive(mode, &mut cold, start());
        assert_eq!(world.effects(), 0, "{mode:?}: restart must not execute");
    }
}

#[test]
fn without_an_attached_bus_the_start_is_still_emitted_to_the_sink() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        let out = drive(mode, &mut actor, start());
        assert_eq!(
            out.iter().filter(|e| is_step_started(e)).count(),
            1,
            "{mode:?}: observers still receive the start"
        );
        assert!(has_step_completed(&out), "{mode:?}");
    }
}

// ------------------------------------------------------------------ P2-e

#[test]
fn plain_completion_uses_one_strict_forward_proof_append() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        let journal = world.journal.clone();
        let mut at_publish = Vec::new();
        drive_with(mode, &mut actor, start(), |event| {
            if matches!(event, SagaChoreographyEvent::StepCompleted { .. }) {
                at_publish = kinds(&rows_of(&journal));
            }
        });
        assert_eq!(world.effects(), 1, "{mode:?}");
        assert_eq!(
            at_publish,
            vec!["started", "forward_outcome"],
            "{mode:?}: success must be published only after the single proof append"
        );
        assert_eq!(
            kinds(&world.rows()),
            vec!["started", "forward_outcome"],
            "{mode:?}"
        );
        let outcome = world
            .rows()
            .into_iter()
            .find_map(|e| match e {
                ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome } => Some(outcome),
                _ => None,
            })
            .unwrap();
        assert_eq!(outcome.output, vec![1]);
        assert_eq!(outcome.compensation_data, vec![42]);
        assert_eq!(outcome.saga_input, vec![9]);
        assert!(outcome.effect.is_none() && outcome.receipt.is_none());
    }
}

#[test]
fn failed_plain_proof_append_publishes_no_success_and_retains_the_bytes() {
    for mode in MODES {
        let world = World::new();
        world.journal.fail_always(
            JournalOp::Append,
            FaultTrigger::EventKind("ParticipantForwardOutcomeRecorded"),
        );
        let mut actor = world.actor();
        let out = drive(mode, &mut actor, start());
        assert_eq!(world.effects(), 1, "{mode:?}");
        assert!(!has_step_completed(&out), "{mode:?}: success without proof");
        assert!(has_quarantine(&out), "{mode:?}");
        assert_eq!(
            world.shared.quarantine_hooks.load(Ordering::SeqCst),
            1,
            "{mode:?}"
        );
        let rows = world.rows();
        assert!(
            !kinds(&rows).contains(&"raw_result"),
            "{mode:?}: plain path has no second-stage raw result row: {rows:?}"
        );
        assert!(
            rows.iter().any(|e| matches!(e,
                ParticipantEvent::ParticipantReconciliationEvidence { output, compensation_data, .. }
                    if output.as_slice() == [1] && compensation_data.as_slice() == [42])),
            "{mode:?}: business bytes must survive as typed evidence: {rows:?}"
        );
        let mut cold = world.actor();
        drive(mode, &mut cold, start());
        assert_eq!(
            world.effects(),
            1,
            "{mode:?}: effect repeated after restart"
        );
        // Evidence is not confirmation: an undo request is not run blindly.
        drive(mode, &mut cold, comp_request());
        assert_eq!(
            world.undos(),
            0,
            "{mode:?}: undo ran from unconfirmed evidence"
        );
    }
}

#[test]
fn declared_effect_keeps_result_before_dispatch_and_proof_after_handoff() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        actor.with_effect = true;
        let journal = world.journal.clone();
        let at_dispatch = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&at_dispatch);
        actor.probe = Some(Arc::new(move |phase| {
            if phase == Phase::Dispatch {
                *sink.lock().unwrap() = kinds(&rows_of(&journal));
            }
        }));
        let out = drive(mode, &mut actor, start());
        assert!(has_step_completed(&out), "{mode:?}");
        assert_eq!(
            *at_dispatch.lock().unwrap(),
            vec!["started", "raw_result"],
            "{mode:?}: result/compensation persisted before dispatch, proof not yet"
        );
        assert_eq!(
            kinds(&world.rows()),
            vec!["started", "raw_result", "forward_outcome"],
            "{mode:?}"
        );
        assert_eq!(world.dispatches(), 1, "{mode:?}");
    }
}

#[test]
fn declared_effect_with_failed_proof_is_never_inferred_confirmed_from_the_raw_result() {
    for mode in MODES {
        let world = World::new();
        world.journal.fail_always(
            JournalOp::Append,
            FaultTrigger::EventKind("ParticipantForwardOutcomeRecorded"),
        );
        let mut actor = world.actor();
        actor.with_effect = true;
        let out = drive(mode, &mut actor, start());
        assert_eq!(world.dispatches(), 1, "{mode:?}");
        assert!(!has_step_completed(&out), "{mode:?}");
        assert!(has_quarantine(&out), "{mode:?}");
        let mut cold = world.actor();
        cold.with_effect = true;
        drive(mode, &mut cold, start());
        assert_eq!(world.effects(), 1, "{mode:?}");
        assert_eq!(world.dispatches(), 1, "{mode:?}: dispatch repeated");
        drive(mode, &mut cold, comp_request());
        assert_eq!(
            world.undos(),
            0,
            "{mode:?}: raw result inferred as confirmation"
        );
    }
}

#[test]
fn cold_undo_still_works_from_the_single_plain_proof() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        drive(mode, &mut actor, start());
        let mut cold = world.actor();
        let out = drive(mode, &mut cold, comp_request());
        assert_eq!(world.effects(), 1, "{mode:?}");
        assert_eq!(world.undos(), 1, "{mode:?}");
        assert!(
            out.iter()
                .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
            "{mode:?}"
        );
    }
}

// ------------------------------------------------------------- AllOf restart

static ALL: &[&str] = &["x", "y"];

fn allof_actor(world: &World) -> Actor {
    let mut actor = world.actor();
    actor.deps = DependencySpec::AllOf(ALL);
    actor
}

#[test]
fn allof_restart_between_branches_still_fires_exactly_once() {
    for mode in MODES {
        let world = World::new();
        let mut first = allof_actor(&world);
        let out = drive(mode, &mut first, dep_completed("x", 11, 100));
        assert!(
            out.is_empty(),
            "{mode:?}: fired on a partial dependency set"
        );
        assert_eq!(world.effects(), 0, "{mode:?}");
        assert!(
            kinds(&world.rows()).contains(&"dependency"),
            "{mode:?}: observation durable"
        );
        drop(first);

        let mut cold = allof_actor(&world);
        let out = drive(mode, &mut cold, dep_completed("y", 12, 100));
        assert_eq!(
            world.effects(),
            1,
            "{mode:?}: restart stalled the AllOf step"
        );
        assert!(has_step_completed(&out), "{mode:?}");

        // Redelivery after another restart neither re-executes nor quarantines.
        let mut again = allof_actor(&world);
        for event in [dep_completed("x", 11, 100), dep_completed("y", 12, 100)] {
            let out = drive(mode, &mut again, event);
            assert!(!has_quarantine(&out), "{mode:?}");
        }
        assert_eq!(world.effects(), 1, "{mode:?}: duplicate business effect");
    }
}

#[test]
fn allof_without_the_other_branch_never_fires_even_after_restarts() {
    for mode in MODES {
        let world = World::new();
        let mut first = allof_actor(&world);
        drive(mode, &mut first, dep_completed("x", 11, 100));
        let mut cold = allof_actor(&world);
        // A different irrelevant step and a duplicate x never complete the set.
        drive(mode, &mut cold, dep_completed("irrelevant", 13, 100));
        drive(mode, &mut cold, dep_completed("x", 14, 100));
        assert_eq!(world.effects(), 0, "{mode:?}");
    }
}

#[test]
fn irrelevant_branches_are_not_recorded() {
    for mode in MODES {
        let world = World::new();
        let mut actor = allof_actor(&world);
        drive(mode, &mut actor, dep_completed("irrelevant", 13, 100));
        assert!(
            !kinds(&world.rows()).contains(&"dependency"),
            "{mode:?}: only declared dependencies are durable"
        );
    }
}

#[test]
fn allof_observations_from_another_run_do_not_count() {
    for mode in MODES {
        let world = World::new();
        // An older run of the same saga id durably observed `x`.
        world
            .journal
            .append(
                SagaId::new(7),
                ParticipantEvent::ParticipantDependencyCompletedRecorded {
                    context: ctx("x", 3, 50),
                    recorded_at_millis: 1,
                },
            )
            .unwrap();
        let mut actor = allof_actor(&world);
        drive(mode, &mut actor, dep_completed("y", 12, 100));
        assert_eq!(
            world.effects(),
            0,
            "{mode:?}: an older run's `x` satisfied this run"
        );
        drive(mode, &mut actor, dep_completed("x", 11, 100));
        assert_eq!(
            world.effects(),
            1,
            "{mode:?}: the current run completes normally"
        );
    }
}

#[test]
fn allof_observation_is_durable_before_the_input_dedupe_mark() {
    for mode in MODES {
        let world = World::new();
        world.dedupe.fail_check_and_mark(FaultTrigger::NthCall(1));
        let mut actor = allof_actor(&world);
        let out = drive(mode, &mut actor, dep_completed("x", 11, 100));
        assert!(
            has_quarantine(&out),
            "{mode:?}: failed input mark must stay visible"
        );
        assert_eq!(world.effects(), 0);
        assert!(
            world.rows().iter().any(|event| matches!(event,
            ParticipantEvent::ParticipantDependencyCompletedRecorded { context, .. }
                if context.step_name.as_ref() == "x")),
            "{mode:?}: dependency observation must precede even a failed input mark"
        );
    }
}

#[test]
fn failed_dependency_observation_quarantines_before_any_effect() {
    for mode in MODES {
        let world = World::new();
        world.journal.fail_always(
            JournalOp::Append,
            FaultTrigger::EventKind("ParticipantDependencyCompletedRecorded"),
        );
        let mut actor = allof_actor(&world);
        let mut out = drive(mode, &mut actor, dep_completed("x", 11, 100));
        out.extend(drive(mode, &mut actor, dep_completed("y", 12, 100)));
        assert_eq!(
            world.effects(),
            0,
            "{mode:?}: effect ran without durable observation"
        );
        assert!(!has_step_completed(&out), "{mode:?}");
        assert!(
            has_quarantine(&out),
            "{mode:?}: storage failure must be visible"
        );
        assert!(kinds(&world.rows()).contains(&"quarantined"), "{mode:?}");
    }
}

#[test]
fn any_single_dependency_read_failure_never_loses_the_step_silently() {
    let mut quarantined_somewhere = false;
    for mode in MODES {
        for nth in 1..=40 {
            let world = World::new();
            let mut first = allof_actor(&world);
            drive(mode, &mut first, dep_completed("x", 11, 100));
            drop(first);
            let base = world.journal.call_count(JournalOp::Read);
            world
                .journal
                .fail_once(JournalOp::Read, FaultTrigger::NthCall(base + nth));
            let mut cold = allof_actor(&world);
            let out = drive(mode, &mut cold, dep_completed("y", 12, 100));
            let fired = world.effects() == 1 && has_step_completed(&out);
            let fenced = world.effects() == 0 && has_quarantine(&out);
            // Reads past the last one of the call never fire; those succeed normally.
            let untouched = world.journal.call_count(JournalOp::Read) < base + nth && fired;
            assert!(
                fired || fenced || untouched,
                "{mode:?} nth={nth}: read failure neither ran the step nor quarantined \
                 (effects={}, out={out:?})",
                world.effects()
            );
            quarantined_somewhere |= fenced;
        }
    }
    assert!(
        quarantined_somewhere,
        "read faults never reached the dependency path"
    );
}
