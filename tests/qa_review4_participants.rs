//! Review4 participant consumer: an owned compensation request for a forward step
//! that was durably and definitively rejected (no effect) is answered with a
//! strictly persisted `CompensationCompleted` WITHOUT invoking the business undo.
//! Real evidence (results, compensating failures, unknown history) is never
//! guessed safe. Covers sync/async generic and workflow callers, duplicates,
//! cold restart, startup recovery and strict append failure.

// Each test crate uses only a subset of the shared fixture re-exports.
#[allow(unused_imports)]
mod support;

use std::cell::RefCell;
use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Waker};
use std::time::Duration;

use icanact_saga_choreography::durability::{
    apply_sync_workflow_participant_saga_ingress_with_hooks,
    collect_startup_recovery_events_for_saga_type, fail_accepted_workflow_step,
};
use icanact_saga_choreography::*;
use support::{FaultDedupe, FaultJournal, FaultTrigger, JournalOp};

const TYPE: &str = "qa_r4p";
const SAGA: u64 = 7;

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
        Ok(StepOutput::Completed {
            output: vec![1],
            compensation_data: vec![42],
        })
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
        Box::pin(async move {
            Ok(StepOutput::Completed {
                output: vec![1],
                compensation_data: vec![42],
            })
        })
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
            journal: FaultJournal::new(InMemoryJournal::new()),
            dedupe: FaultDedupe::new(InMemoryDedupe::new()),
            shared: Arc::default(),
        }
    }
    /// A cold actor (no volatile state) over the same durable stores.
    fn actor(&self) -> Actor {
        Actor {
            saga: SagaParticipantSupport::new(self.journal.clone(), self.dedupe.clone()),
            shared: self.shared.clone(),
        }
    }
    /// Cold restart where the (volatile) ingress dedupe marker was lost too.
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
    fn rows(&self) -> Vec<ParticipantEvent> {
        self.journal
            .read(SagaId::new(SAGA))
            .unwrap()
            .into_iter()
            .map(|e| e.event)
            .collect()
    }
    fn count(&self, kind: &str) -> usize {
        self.rows()
            .iter()
            .filter(|e| format!("{e:?}").starts_with(kind))
            .count()
    }
}

fn context_for(ty: &str, step: &str) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(SAGA),
        saga_type: ty.into(),
        step_name: step.into(),
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

fn run_ctx() -> SagaContext {
    context_for(TYPE, "start")
}

fn request_event(c: &SagaContext, step: &str) -> SagaChoreographyEvent {
    SagaChoreographyEvent::CompensationRequested {
        context: c.clone(),
        failed_step: "z".into(),
        reason: "downstream failed".into(),
        failure: SagaFailureDetails {
            step_name: "z".into(),
            participant_id: "z".into(),
            error_code: None,
            error_message: "downstream failed".into(),
            at_millis: 100,
        },
        steps_to_compensate: vec![step.into()],
    }
}

fn append(a: &Actor, event: ParticipantEvent) {
    a.record_event_strict(SagaId::new(SAGA), event).unwrap();
}

fn accepted_row(c: &SagaContext, participant: &str) -> ParticipantEvent {
    ParticipantEvent::AcceptedStepRecorded {
        context: c.clone(),
        participant_id: participant.into(),
        execution_id: StepExecutionId::new("exec-1"),
        idle_timeout_millis: 1_000,
        hard_timeout_millis: 5_000,
        timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        },
        saga_input: vec![1],
        compensation_data: vec![42],
        accepted_at_millis: 101,
        deadline_at_millis: 1_101,
        hard_deadline_at_millis: 5_101,
    }
}

fn failed_row(requires_compensation: bool) -> ParticipantEvent {
    ParticipantEvent::StepExecutionFailed {
        error: "remote rejected".into(),
        requires_compensation,
        failed_at_millis: 110,
    }
}

/// Durable history of a definitively rejected accepted forward step.
fn seed_rejection(a: &Actor, participant: &str) {
    let c = run_ctx();
    a.admit_participant_event_strict(&c).unwrap();
    append(a, accepted_row(&c, participant));
    append(a, failed_row(false));
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

#[test]
fn rejected_request_persists_completion_and_acks_without_physical_undo() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        seed_rejection(&actor, "a");
        let out = drive(mode, &mut actor, request_event(&run_ctx(), "a"));
        assert_eq!(acks(&out), 1, "{mode:?}: {out:?}");
        assert_eq!(world.undos(), 0, "{mode:?}: no business undo");
        assert_eq!(world.count("CompensationCompleted"), 1, "{mode:?}");
        assert_eq!(world.count("CompensationRequestRecorded"), 1, "{mode:?}");
        assert_eq!(world.count("CompensationStarted"), 0, "{mode:?}");
        assert_eq!(world.quarantine_hooks(), 0, "{mode:?}");
        // The proof is durable before it is published: replay cannot lose it.
        let ev = actor.participant_run_evidence_strict(&run_ctx()).unwrap();
        assert!(ev.undo_completed, "{mode:?}");
    }
}

#[test]
fn duplicate_and_cold_restart_resend_the_proof_without_repeating_work() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        seed_rejection(&actor, "a");
        drive(mode, &mut actor, request_event(&run_ctx(), "a"));
        // A duplicate is fenced by the run-scoped ingress dedupe: nothing repeats.
        let dup = drive(mode, &mut actor, request_event(&run_ctx(), "a"));
        assert!(acks(&dup) <= 1, "{mode:?}: {dup:?}");
        let mut cold = world.restarted_actor();
        let again = drive(mode, &mut cold, request_event(&run_ctx(), "a"));
        assert_eq!(acks(&again), 1, "{mode:?}: cold restart resends: {again:?}");
        assert_eq!(world.undos(), 0, "{mode:?}");
        assert_eq!(world.count("CompensationCompleted"), 1, "{mode:?}");
        assert_eq!(world.quarantine_hooks(), 0, "{mode:?}");
    }
}

#[test]
fn failed_completion_append_publishes_no_ack_and_quarantines() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        seed_rejection(&actor, "a");
        world.journal.fail_once(
            JournalOp::Append,
            FaultTrigger::EventKind("CompensationCompleted"),
        );
        let out = drive(mode, &mut actor, request_event(&run_ctx(), "a"));
        assert_eq!(acks(&out), 0, "{mode:?}: {out:?}");
        assert_eq!(world.undos(), 0, "{mode:?}");
        assert_eq!(world.count("CompensationCompleted"), 0, "{mode:?}");
        assert_eq!(world.count("CompensationRequestRecorded"), 1, "{mode:?}");
        assert_eq!(world.quarantine_hooks(), 1, "{mode:?}");
        assert!(
            out.iter()
                .any(|e| matches!(e, SagaChoreographyEvent::SagaQuarantined { .. })),
            "{mode:?}: {out:?}"
        );
    }
}

#[test]
fn real_or_unknown_evidence_is_never_acknowledged_as_no_effect() {
    type Seed = fn(&Actor);
    let cases: [(&str, Seed); 4] = [
        ("unknown: failure without accepted metadata", |a| {
            a.admit_participant_event_strict(&run_ctx()).unwrap();
            append(a, failed_row(false));
        }),
        ("actual result then rejection", |a| {
            let c = run_ctx();
            a.admit_participant_event_strict(&c).unwrap();
            append(a, accepted_row(&c, "a"));
            append(
                a,
                ParticipantEvent::StepExecutionCompleted {
                    output: vec![1],
                    compensation_data: vec![42],
                    completed_at_millis: 105,
                },
            );
            append(a, failed_row(false));
        }),
        ("prior compensating failure", |a| {
            let c = run_ctx();
            a.admit_participant_event_strict(&c).unwrap();
            append(a, accepted_row(&c, "a"));
            append(a, failed_row(true));
            append(a, failed_row(false));
        }),
        ("open undo before rejection", |a| {
            let c = run_ctx();
            a.admit_participant_event_strict(&c).unwrap();
            append(a, accepted_row(&c, "a"));
            append(
                a,
                ParticipantEvent::CompensationStarted {
                    attempt: 1,
                    started_at_millis: 106,
                },
            );
            append(a, failed_row(false));
        }),
    ];
    for mode in MODES {
        for (name, seed) in cases {
            let world = World::new();
            let mut actor = world.actor();
            seed(&actor);
            let out = drive(mode, &mut actor, request_event(&run_ctx(), "a"));
            assert_eq!(acks(&out), 0, "{mode:?} {name}: {out:?}");
            assert_eq!(world.count("CompensationCompleted"), 0, "{mode:?} {name}");
            assert_eq!(world.undos(), 0, "{mode:?} {name}: no guessed undo");
        }
    }
}

#[test]
fn durable_rejection_overrides_still_executing_accepted_cache_without_undo() {
    for mode in MODES {
        let world = World::new();
        let mut actor = world.actor();
        let c = run_ctx().next_step("a".into());
        accept_workflow_step(
            &mut actor,
            c.clone(),
            "a".into(),
            StepExecutionId::new("cached-exec"),
            AcceptedStepPolicy {
                idle_timeout: Duration::from_secs(60),
                hard_timeout: Duration::from_secs(3600),
                timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                    requires_compensation: false,
                },
            },
            vec![1],
            vec![42],
        )
        .unwrap();
        // Model a durable application disposition before volatile cleanup. The
        // journal is authority; an accepted cache entry is not proof of an effect.
        append(&actor, failed_row(false));
        assert!(
            actor
                .participant_run_evidence_strict(&c)
                .unwrap()
                .forward_definitively_rejected
        );
        assert!(matches!(
            actor.saga_states_ref().get(&c.saga_id),
            Some(SagaStateEntry::Executing(_))
        ));
        let out = drive(mode, &mut actor, request_event(&c, "a"));
        assert_eq!(
            world.undos(),
            0,
            "{mode:?}: stale cache cannot authorize physical undo"
        );
        assert_eq!(acks(&out), 1, "{mode:?}: {out:?}");
        assert_eq!(world.count("CompensationCompleted"), 1);
        assert_eq!(world.count("CompensationStarted"), 0);
        assert!(
            !actor
                .saga_support()
                .accepted_workflow_steps
                .contains_key(&c.saga_id)
        );
    }
}

// ------------------------------------------------------------------ workflow

define_saga_workflow_contract! {
    struct AcceptContract {
        saga_type: "qa_r4p_wf", first_step: accf, failure_authority: any (),
        required_steps: [accf], overall_timeout_ms: 30_000, stalled_timeout_ms: 30_000,
        steps: { accf => { participant: "accf", depends_on: on_start () } }
    }
}

struct WfActor {
    inner: Actor,
}

impl HasSagaParticipantSupport for WfActor {
    type Journal = Journal;
    type Dedupe = Dedupe;
    fn saga_support(&self) -> &SagaParticipantSupport<Journal, Dedupe> {
        &self.inner.saga
    }
    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Journal, Dedupe> {
        &mut self.inner.saga
    }
}

struct AcceptFail;
static WORKFLOWS: [&'static dyn SagaWorkflowParticipant<WfActor>; 1] = [&AcceptFail];

impl HasSagaWorkflowParticipants for WfActor {
    fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
        &WORKFLOWS
    }
}

impl SagaWorkflowParticipant<WfActor> for AcceptFail {
    fn step_name(&self) -> &'static str {
        "accf"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["qa_r4p_wf"]
    }
    fn execute_step(
        &self,
        _: &mut WfActor,
        _: &SagaContext,
        _: &[u8],
    ) -> Result<StepOutput, StepError> {
        Ok(StepOutput::Accepted {
            execution_id: StepExecutionId::new("ext-1"),
            policy: AcceptedStepPolicy {
                idle_timeout: Duration::from_secs(60),
                hard_timeout: Duration::from_secs(3600),
                timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                    requires_compensation: false,
                },
            },
            compensation_data: b"release".to_vec(),
        })
    }
    fn compensate_step(
        &self,
        actor: &mut WfActor,
        _: &SagaContext,
        _: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        actor.inner.shared.undos.fetch_add(1, Ordering::SeqCst);
        Ok(CompensationOutput::Completed)
    }
    fn on_compensation_completed(&self, actor: &mut WfActor, _: &SagaContext) {
        actor
            .inner
            .shared
            .completed_hooks
            .fetch_add(1, Ordering::SeqCst);
    }
    fn on_quarantined(&self, actor: &mut WfActor, _: &SagaContext, _: &str) {
        actor
            .inner
            .shared
            .quarantine_hooks
            .fetch_add(1, Ordering::SeqCst);
    }
}

fn wf_actor(world: &World) -> WfActor {
    WfActor {
        inner: world.actor(),
    }
}

fn wf_restarted(world: &World) -> WfActor {
    WfActor {
        inner: world.restarted_actor(),
    }
}

fn wf_ctx() -> SagaContext {
    context_for("qa_r4p_wf", "accf")
}

fn deliver(actor: &mut WfActor, event: SagaChoreographyEvent) -> Vec<SagaChoreographyEvent> {
    let valid = RefCell::new(Vec::new());
    apply_sync_workflow_participant_saga_ingress_with_hooks(
        actor,
        event,
        |_a, _e| {},
        |_e| {},
        |_a, e| valid.borrow_mut().push(e.clone()),
    );
    valid.into_inner()
}

/// Accepts the step then durably rejects it through the public API.
fn wf_rejected(world: &World) -> WfActor {
    let mut actor = wf_actor(world);
    let c = wf_ctx();
    let out = deliver(
        &mut actor,
        SagaChoreographyEvent::SagaStarted {
            context: c.clone(),
            payload: b"input".to_vec(),
        },
    );
    assert!(
        out.iter().any(|e| e.event_type() == "step_accepted"),
        "{out:?}"
    );
    fail_accepted_workflow_step(
        &mut actor,
        c.saga_id,
        StepExecutionId::new("ext-1"),
        AcceptedStepFailure {
            reason: "remote rejected".into(),
            requires_compensation: false,
            failed_at_millis: 150,
        },
    )
    .expect("definitive rejection");
    actor
}

fn count_acks(events: &[SagaChoreographyEvent]) -> usize {
    events
        .iter()
        .filter(|e| e.event_type() == "compensation_completed")
        .count()
}

#[test]
fn workflow_rejected_request_completes_without_undo_then_resends_on_duplicate_and_cold() {
    let world = World::new();
    let mut actor = wf_rejected(&world);
    let request = request_event(&wf_ctx(), "accf");
    let first = deliver(&mut actor, request.clone());
    assert_eq!(count_acks(&first), 1, "{first:?}");
    assert_eq!(world.undos(), 0);
    assert_eq!(world.count("CompensationCompleted"), 1);
    assert_eq!(world.completed_hooks(), 1);
    let dup = deliver(&mut actor, request.clone());
    assert!(count_acks(&dup) <= 1, "{dup:?}");
    let mut cold = wf_restarted(&world);
    let again = deliver(&mut cold, request);
    assert_eq!(count_acks(&again), 1, "{again:?}");
    assert_eq!(world.undos(), 0);
    assert_eq!(world.count("CompensationCompleted"), 1);
    assert_eq!(world.quarantine_hooks(), 0);
}

#[test]
fn workflow_failed_completion_append_publishes_no_ack_and_quarantines() {
    let world = World::new();
    let mut actor = wf_rejected(&world);
    world.journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("CompensationCompleted"),
    );
    let out = deliver(&mut actor, request_event(&wf_ctx(), "accf"));
    assert_eq!(count_acks(&out), 0, "{out:?}");
    assert_eq!(world.count("CompensationCompleted"), 0);
    assert_eq!(world.count("CompensationRequestRecorded"), 1);
    assert_eq!(world.quarantine_hooks(), 1);
    assert_eq!(world.undos(), 0);
}

fn recover(world: &World) -> Vec<SagaChoreographyEvent> {
    collect_startup_recovery_events_for_saga_type(
        &world.journal,
        &world.dedupe,
        "accf",
        "qa_r4p_wf",
    )
    .expect("startup recovery")
}

#[test]
fn startup_replays_an_unanswered_request_and_resends_a_proven_ack() {
    let world = World::new();
    let mut actor = wf_rejected(&world);
    // Owned request recorded, crash before the completion proof.
    let c = wf_ctx();
    append(
        &actor.inner,
        ParticipantEvent::CompensationRequestRecorded {
            context: c.clone(),
            failed_step: "z".into(),
            reason: "downstream failed".into(),
            failure: SagaFailureDetails {
                step_name: "z".into(),
                participant_id: "z".into(),
                error_code: None,
                error_message: "downstream failed".into(),
                at_millis: 100,
            },
            steps_to_compensate: vec!["accf".into()],
            requested_at_millis: 160,
        },
    );
    let events = recover(&world);
    assert!(
        events
            .iter()
            .all(|e| !matches!(e, SagaChoreographyEvent::SagaQuarantined { .. })),
        "{events:?}"
    );
    let replay = events
        .into_iter()
        .find(|e| matches!(e, SagaChoreographyEvent::CompensationRequested { .. }))
        .expect("request replayed");
    let mut cold = wf_restarted(&world);
    let out = deliver(&mut cold, replay);
    assert_eq!(count_acks(&out), 1, "{out:?}");
    assert_eq!(world.undos(), 0);
    // Completion durable but acknowledgement lost: startup resends only the proof.
    let events = recover(&world);
    assert_eq!(count_acks(&events), 1, "{events:?}");
    assert!(
        events
            .iter()
            .all(|e| !matches!(e, SagaChoreographyEvent::CompensationRequested { .. })),
        "{events:?}"
    );
    assert_eq!(world.undos(), 0);
    let _ = &mut actor;
}

#[test]
fn startup_ignores_unproved_no_effect_completion_and_open_undo() {
    for conflict in [
        "real-result",
        "requires-compensation",
        "open-undo",
        "new-forward-intent",
    ] {
        let world = World::new();
        let actor = wf_rejected(&world);
        let c = wf_ctx();
        append(
            &actor.inner,
            ParticipantEvent::CompensationRequestRecorded {
                context: c.clone(),
                failed_step: "z".into(),
                reason: "downstream failed".into(),
                failure: SagaFailureDetails {
                    step_name: "z".into(),
                    participant_id: "z".into(),
                    error_code: None,
                    error_message: "downstream failed".into(),
                    at_millis: 100,
                },
                steps_to_compensate: vec!["accf".into()],
                requested_at_millis: 160,
            },
        );
        let event = match conflict {
            "real-result" => ParticipantEvent::StepExecutionCompleted {
                output: vec![9],
                compensation_data: vec![42],
                completed_at_millis: 170,
            },
            "requires-compensation" => failed_row(true),
            "open-undo" => ParticipantEvent::CompensationStarted {
                attempt: 1,
                started_at_millis: 170,
            },
            _ => ParticipantEvent::StepExecutionStarted {
                attempt: 2,
                started_at_millis: 170,
            },
        };
        append(&actor.inner, event);
        if conflict != "open-undo" {
            append(
                &actor.inner,
                ParticipantEvent::CompensationCompleted {
                    completed_at_millis: 180,
                },
            );
        }
        let events = recover(&world);
        assert_eq!(
            count_acks(&events),
            0,
            "{conflict}: a completion without undo-start is not proven no-effect: {events:?}"
        );
        assert_eq!(world.undos(), 0);
    }
}
