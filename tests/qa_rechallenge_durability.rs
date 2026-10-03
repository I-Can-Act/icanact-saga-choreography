//! Re-challenge remediation (durability seam): managed start visibility before
//! business work, one-append plain/accepted completion proof, durable accepted
//! timeout resolution, startup resend of a proven-but-unacknowledged undo, and
//! durable AllOf dependency observations. Every behavior is driven through the
//! public ingress/recovery surface over real journals and a real bus.

// Each test crate uses only a subset of the shared fixture re-exports.
#[allow(unused_imports)]
mod support;

use std::cell::RefCell;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use icanact_saga_choreography::durability::{
    apply_sync_workflow_participant_saga_ingress_with_hooks,
    collect_startup_recovery_events_for_saga_type, complete_accepted_workflow_step,
    recover_accepted_workflow_steps_for_saga_type,
};
use icanact_saga_choreography::{
    AcceptedStepCompletion, AcceptedStepFailure, AcceptedStepPolicy, AcceptedStepTimeoutOutcome,
    CompensationError, CompensationOutput, DependencySpec, EffectDispatchError,
    EffectDispatchOutcome, EffectDispatchRequest, HasSagaParticipantSupport,
    HasSagaWorkflowParticipants, InMemoryDedupe, InMemoryJournal, JournalEntry, ParticipantEvent,
    ParticipantJournal, ParticipantTerminalKind, PeerId, SagaChoreographyBus,
    SagaChoreographyEvent, SagaContext, SagaId, SagaParticipantSupport, SagaStateExt,
    SagaWorkflowParticipant, StepError, StepExecutionId, StepOutput, fail_accepted_workflow_step,
};
use support::{FaultDedupe, FaultJournal, FaultTrigger, JournalOp};

type Journal = FaultJournal<InMemoryJournal>;
type Dedupe = FaultDedupe<InMemoryDedupe>;

const SAGA: u64 = 77;

type Seen = Arc<Mutex<Vec<SagaChoreographyEvent>>>;

icanact_saga_choreography::define_saga_workflow_contract! {
    struct SlowContract {
        saga_type: "wf_slow", first_step: slow, failure_authority: any (),
        required_steps: [slow], overall_timeout_ms: 30_000, stalled_timeout_ms: 150,
        steps: { slow => { participant: "slow", depends_on: on_start () } }
    }
}

icanact_saga_choreography::define_saga_workflow_contract! {
    struct BareContract {
        saga_type: "wf_accn", first_step: accn, failure_authority: any (),
        required_steps: [accn], overall_timeout_ms: 30_000, stalled_timeout_ms: 30_000,
        steps: { accn => { participant: "accn", depends_on: on_start () } }
    }
}

icanact_saga_choreography::define_saga_workflow_contract! {
    struct AcceptContract {
        saga_type: "wf_accf", first_step: accf, failure_authority: any (),
        required_steps: [accf], overall_timeout_ms: 30_000, stalled_timeout_ms: 30_000,
        steps: { accf => { participant: "accf", depends_on: on_start () } }
    }
}

struct Actor {
    saga: SagaParticipantSupport<Journal, Dedupe>,
    calls: usize,
    comp_calls: usize,
    dispatched: usize,
    failed_hooks: usize,
    quarantine_hooks: usize,
    /// Bus events visible at the instant business work began.
    seen_at_work: Option<Vec<SagaChoreographyEvent>>,
    /// Journal rows visible at the instant the effect was dispatched.
    rows_at_dispatch: Option<Vec<JournalEntry>>,
    probe: Option<Seen>,
    work_millis: u64,
    accepted_idle_timeout: Duration,
}

impl Actor {
    fn new(journal: &Journal, dedupe: &Dedupe) -> Self {
        Self {
            saga: SagaParticipantSupport::new(journal.clone(), dedupe.clone()),
            calls: 0,
            comp_calls: 0,
            dispatched: 0,
            failed_hooks: 0,
            quarantine_hooks: 0,
            seen_at_work: None,
            rows_at_dispatch: None,
            probe: None,
            work_millis: 0,
            accepted_idle_timeout: Duration::from_secs(60),
        }
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

struct Slow;
struct Plain;
struct Effect;
struct AcceptFail;
struct Join;
struct AcceptBare;

static WORKFLOWS: [&'static dyn SagaWorkflowParticipant<Actor>; 6] =
    [&Slow, &Plain, &Effect, &AcceptFail, &Join, &AcceptBare];

impl HasSagaWorkflowParticipants for Actor {
    fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
        &WORKFLOWS
    }
}

fn undo(actor: &mut Actor) -> Result<CompensationOutput, CompensationError> {
    actor.comp_calls += 1;
    Ok(CompensationOutput::Completed)
}

macro_rules! hooks {
    () => {
        fn on_saga_failed(&self, actor: &mut Actor, _c: &SagaContext, _r: &str) {
            actor.failed_hooks += 1;
        }
        fn on_quarantined(&self, actor: &mut Actor, _c: &SagaContext, _r: &str) {
            actor.quarantine_hooks += 1;
        }
    };
}

impl SagaWorkflowParticipant<Actor> for Slow {
    fn step_name(&self) -> &'static str {
        "slow"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_slow"]
    }
    fn execute_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        _i: &[u8],
    ) -> Result<StepOutput, StepError> {
        actor.calls += 1;
        if let Some(probe) = &actor.probe {
            actor.seen_at_work = Some(probe.lock().unwrap().clone());
        }
        std::thread::sleep(Duration::from_millis(actor.work_millis));
        Ok(StepOutput::Completed {
            output: b"out".to_vec(),
            compensation_data: b"undo".to_vec(),
        })
    }
    fn compensate_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        _d: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        undo(actor)
    }
    hooks!();
}

impl SagaWorkflowParticipant<Actor> for Plain {
    fn step_name(&self) -> &'static str {
        "plain"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_plain"]
    }
    fn execute_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        _i: &[u8],
    ) -> Result<StepOutput, StepError> {
        actor.calls += 1;
        Ok(StepOutput::Completed {
            output: b"out".to_vec(),
            compensation_data: b"undo".to_vec(),
        })
    }
    fn compensate_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        _d: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        undo(actor)
    }
    hooks!();
}

impl SagaWorkflowParticipant<Actor> for Effect {
    fn step_name(&self) -> &'static str {
        "fx"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_fx"]
    }
    fn execute_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        _i: &[u8],
    ) -> Result<StepOutput, StepError> {
        actor.calls += 1;
        Ok(StepOutput::CompletedWithEffect {
            output: b"sent".to_vec(),
            compensation_data: b"unsend".to_vec(),
            effect: "email".into(),
        })
    }
    fn compensate_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        _d: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        undo(actor)
    }
    fn dispatch_effect(
        &self,
        actor: &mut Actor,
        request: &EffectDispatchRequest<'_>,
    ) -> Result<EffectDispatchOutcome, EffectDispatchError> {
        actor.dispatched += 1;
        actor.rows_at_dispatch = actor.saga.journal.read(request.context.saga_id).ok();
        Ok(EffectDispatchOutcome::Durable {
            receipt: "outbox-1".into(),
        })
    }
    hooks!();
}

impl SagaWorkflowParticipant<Actor> for AcceptFail {
    fn step_name(&self) -> &'static str {
        "accf"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_accf"]
    }
    fn execute_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        _i: &[u8],
    ) -> Result<StepOutput, StepError> {
        actor.calls += 1;
        Ok(StepOutput::Accepted {
            execution_id: StepExecutionId::new("ext-1"),
            policy: AcceptedStepPolicy {
                idle_timeout: actor.accepted_idle_timeout,
                hard_timeout: Duration::from_secs(3600),
                timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                    requires_compensation: false,
                },
            },
            // Potential compensation bytes: metadata, not a materialised effect.
            compensation_data: b"release".to_vec(),
        })
    }
    fn compensate_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        _d: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        undo(actor)
    }
    hooks!();
}

impl SagaWorkflowParticipant<Actor> for AcceptBare {
    fn step_name(&self) -> &'static str {
        "accn"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_accn"]
    }
    fn execute_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        _i: &[u8],
    ) -> Result<StepOutput, StepError> {
        actor.calls += 1;
        Ok(StepOutput::Accepted {
            execution_id: StepExecutionId::new("ext-9"),
            policy: AcceptedStepPolicy {
                idle_timeout: Duration::from_secs(60),
                hard_timeout: Duration::from_secs(3600),
                timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                    requires_compensation: false,
                },
            },
            compensation_data: Vec::new(),
        })
    }
    fn compensate_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        _d: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        undo(actor)
    }
    hooks!();
}

impl SagaWorkflowParticipant<Actor> for Join {
    fn step_name(&self) -> &'static str {
        "join"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_all"]
    }
    fn depends_on(&self) -> DependencySpec {
        DependencySpec::AllOf(&["a", "b"])
    }
    fn execute_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        _i: &[u8],
    ) -> Result<StepOutput, StepError> {
        actor.calls += 1;
        Ok(StepOutput::Completed {
            output: b"joined".to_vec(),
            compensation_data: Vec::new(),
        })
    }
    fn compensate_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        _d: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        undo(actor)
    }
    hooks!();
}

fn stores() -> (Journal, Dedupe) {
    (
        FaultJournal::new(InMemoryJournal::new()),
        FaultDedupe::new(InMemoryDedupe::new()),
    )
}

fn ctx_for(saga: u64, saga_type: &str, step: &str, started_at: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(saga),
        saga_type: saga_type.into(),
        step_name: step.into(),
        correlation_id: saga,
        causation_id: saga,
        trace_id: saga,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: started_at,
        event_timestamp_millis: started_at,
    }
}

fn ctx(saga_type: &str, step: &str, started_at: u64) -> SagaContext {
    ctx_for(SAGA, saga_type, step, started_at)
}

fn started(c: &SagaContext) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: c.clone(),
        payload: b"input".to_vec(),
    }
}

fn dependency_done(c: &SagaContext, step: &str) -> SagaChoreographyEvent {
    let mut c = c.clone();
    c.step_name = step.into();
    SagaChoreographyEvent::StepCompleted {
        context: c,
        output: b"in".to_vec(),
        saga_input: b"input".to_vec(),
        compensation_available: false,
    }
}

fn saga_failed(c: &SagaContext, reason: &str) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaFailed {
        context: c.next_step("terminal_resolver".into()),
        reason: reason.into(),
        failure: None,
    }
}

fn undo_request(c: &SagaContext, step: &str) -> SagaChoreographyEvent {
    SagaChoreographyEvent::CompensationRequested {
        context: c.next_step("terminal_resolver".into()),
        failed_step: "later".into(),
        reason: "rollback".into(),
        failure: icanact_saga_choreography::SagaFailureDetails {
            step_name: "later".into(),
            participant_id: "p".into(),
            error_code: None,
            error_message: "rollback".into(),
            at_millis: 100,
        },
        steps_to_compensate: vec![step.into()],
    }
}

#[derive(Default, Debug)]
struct Delivery {
    valid: Vec<SagaChoreographyEvent>,
    invalid: Vec<SagaChoreographyEvent>,
}

impl Delivery {
    fn count(&self, kind: &str) -> usize {
        self.valid.iter().filter(|e| e.event_type() == kind).count()
    }
}

fn deliver(actor: &mut Actor, event: SagaChoreographyEvent) -> Delivery {
    let valid = RefCell::new(Vec::new());
    let invalid = RefCell::new(Vec::new());
    apply_sync_workflow_participant_saga_ingress_with_hooks(
        actor,
        event,
        |_a, _e| {},
        |e| invalid.borrow_mut().push(e.clone()),
        |_a, e| valid.borrow_mut().push(e.clone()),
    );
    Delivery {
        valid: valid.into_inner(),
        invalid: invalid.into_inner(),
    }
}

fn recover(
    journal: &Journal,
    dedupe: &Dedupe,
    step: &'static str,
    ty: &'static str,
) -> Vec<SagaChoreographyEvent> {
    collect_startup_recovery_events_for_saga_type(journal, dedupe, step, ty)
        .expect("startup recovery should collect")
}

fn rows(journal: &Journal) -> Vec<JournalEntry> {
    journal.read(SagaId::new(SAGA)).unwrap()
}

fn count_rows(journal: &Journal, kind: &str) -> usize {
    rows(journal)
        .iter()
        .filter(|e| format!("{:?}", e.event).starts_with(kind))
        .count()
}

fn recording_bus(saga_type: &str, refuse_started: bool) -> (SagaChoreographyBus, Seen) {
    let bus = SagaChoreographyBus::new();
    let seen: Seen = Arc::default();
    let sink = Arc::clone(&seen);
    // The subscription lives as long as the process-wide bus clone does.
    let _ = bus.subscribe_saga_type_fn(saga_type, move |event| {
        sink.lock().unwrap().push(event.clone());
        !(refuse_started && matches!(event, SagaChoreographyEvent::StepStarted { .. }))
    });
    (bus, seen)
}

// ---------------------------------------------------------------------------
// P2-a: managed StepStarted is strictly published after intent and before work
// ---------------------------------------------------------------------------

#[test]
fn managed_start_is_visible_on_the_bus_while_business_work_runs() {
    let (journal, dedupe) = stores();
    let (bus, seen) = recording_bus("wf_slow", false);
    let mut actor = Actor::new(&journal, &dedupe);
    actor.saga.attach_bus(bus);
    actor.probe = Some(Arc::clone(&seen));
    let c = ctx("wf_slow", "slow", 100);

    let out = deliver(&mut actor, started(&c));

    let at_work = actor.seen_at_work.as_ref().expect("business work ran");
    assert!(
        at_work.iter().any(|e| matches!(e,
            SagaChoreographyEvent::StepStarted { context } if context.step_name.as_ref() == "slow")),
        "StepStarted must be on the bus before the effect runs: {at_work:?}"
    );
    // Published exactly once overall, hooks preserved, and ordered before completion.
    let bus_events = seen.lock().unwrap().clone();
    let kinds: Vec<_> = bus_events.iter().map(|e| e.event_type()).collect();
    assert_eq!(
        kinds.iter().filter(|k| **k == "step_started").count(),
        1,
        "{kinds:?}"
    );
    let start_at = kinds.iter().position(|k| *k == "step_started").unwrap();
    let done_at = kinds.iter().position(|k| *k == "step_completed").unwrap();
    assert!(start_at < done_at, "{kinds:?}");
    assert_eq!(out.count("step_started"), 1, "{:?}", out.valid);
    assert!(out.invalid.is_empty());
}

#[test]
fn failed_start_publication_prevents_the_effect_and_quarantines() {
    let (journal, dedupe) = stores();
    let (bus, _seen) = recording_bus("wf_slow", true);
    let mut actor = Actor::new(&journal, &dedupe);
    actor.saga.attach_bus(bus);
    let c = ctx("wf_slow", "slow", 100);

    let out = deliver(&mut actor, started(&c));

    assert_eq!(
        actor.calls, 0,
        "no business effect without a published start"
    );
    assert_eq!(out.count("saga_quarantined"), 1, "{:?}", out.valid);
    assert_eq!(out.count("step_completed"), 0);
    assert_eq!(
        actor.terminal_run_outcome_strict(&c).unwrap(),
        Some(ParticipantTerminalKind::Quarantined)
    );
}

#[test]
fn managed_work_longer_than_the_stall_window_never_escapes_as_an_ordinary_failure() {
    let (journal, dedupe) = stores();
    let (bus, seen) = recording_bus("wf_slow", false);
    bus.register_workflow_contract_provider::<SlowContract>()
        .expect("contract");
    bus.register_bound_workflow_step("wf_slow", "slow").unwrap();
    let _resolver = bus
        .attach_terminal_resolver_for_contract::<SlowContract>("rechallenge-resolver")
        .expect("resolver");
    let c = ctx("wf_slow", "slow", SagaContext::now_millis());
    bus.publish_strict(started(&c)).expect("start admitted");

    let mut actor = Actor::new(&journal, &dedupe);
    actor.saga.attach_bus(bus.clone());
    actor.work_millis = 1_200;
    let worker = std::thread::spawn(move || {
        deliver(&mut actor, started(&c));
        actor
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let first_terminal = loop {
        let found = seen
            .lock()
            .unwrap()
            .iter()
            .find(|e| e.terminal_outcome().is_some())
            .cloned();
        if let Some(found) = found {
            break Some(found);
        }
        if std::time::Instant::now() > deadline {
            break None;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let actor = worker.join().expect("worker thread");
    let first = first_terminal.expect("a terminal while work is known running");
    assert!(
        !matches!(first, SagaChoreographyEvent::SagaFailed { .. }),
        "an ordinary Failed escaped while managed work was running: {first:?}"
    );
    assert_eq!(actor.calls, 1);
}

// ---------------------------------------------------------------------------
// P2-e: plain completion is ONE strict proof append; declared effects stay two-stage
// ---------------------------------------------------------------------------

#[test]
fn plain_completion_is_a_single_proof_append_with_no_raw_result() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_plain", "plain", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    let out = deliver(&mut actor, started(&c));

    assert_eq!(count_rows(&journal, "StepExecutionCompleted"), 0);
    assert_eq!(count_rows(&journal, "ParticipantForwardOutcomeRecorded"), 1);
    let proof = actor
        .participant_run_evidence_strict(&c)
        .unwrap()
        .forward_outcome
        .expect("proof");
    assert_eq!(proof.output, b"out");
    assert_eq!(proof.compensation_data, b"undo");
    let emitted = out
        .valid
        .iter()
        .find(|e| matches!(e, SagaChoreographyEvent::StepCompleted { .. }))
        .expect("success published after the proof");
    assert_eq!(
        format!("{emitted:?}"),
        format!("{:?}", proof.completion_event())
    );
    // Restart resends the proven completion; nothing re-runs.
    let events = recover(&journal, &dedupe, "plain", "wf_plain");
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(format!("{:?}", events[0]), format!("{emitted:?}"));
    assert_eq!(actor.calls, 1);
}

#[test]
fn plain_proof_append_failure_keeps_typed_bytes_and_never_reports_success() {
    let (journal, dedupe) = stores();
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("ParticipantForwardOutcomeRecorded"),
    );
    let c = ctx("wf_plain", "plain", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    let out = deliver(&mut actor, started(&c));

    assert_eq!(out.count("step_completed"), 0, "{:?}", out.valid);
    assert_eq!(out.count("saga_quarantined"), 1, "{:?}", out.valid);
    let evidence = rows(&journal)
        .into_iter()
        .find_map(|e| match e.event {
            ParticipantEvent::ParticipantReconciliationEvidence {
                output,
                compensation_data,
                ..
            } => Some((output, compensation_data)),
            _ => None,
        })
        .expect("typed result bytes retained for reconciliation");
    assert_eq!(evidence, (b"out".to_vec(), b"undo".to_vec()));
    let run = actor.participant_run_evidence_strict(&c).unwrap();
    assert!(
        run.forward_outcome.is_none(),
        "no confirmation without the proof"
    );
    assert!(run.failure_requires_quarantine());
    // Restart never resends success or re-executes.
    let events = recover(&journal, &dedupe, "plain", "wf_plain");
    assert!(
        events
            .iter()
            .all(|e| matches!(e, SagaChoreographyEvent::SagaQuarantined { .. })),
        "{events:?}"
    );
    assert_eq!(actor.calls, 1);
}

#[test]
fn declared_effect_keeps_pre_dispatch_result_and_post_handoff_proof() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_fx", "fx", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    deliver(&mut actor, started(&c));
    assert_eq!(actor.dispatched, 1);
    let at_dispatch = actor.rows_at_dispatch.as_ref().expect("dispatch observed");
    let raw = at_dispatch
        .iter()
        .find_map(|e| match &e.event {
            ParticipantEvent::StepExecutionCompleted {
                output,
                compensation_data,
                ..
            } => Some((output.clone(), compensation_data.clone())),
            _ => None,
        })
        .expect("result and compensation durable before dispatch");
    assert_eq!(raw, (b"sent".to_vec(), b"unsend".to_vec()));
    assert!(
        !at_dispatch.iter().any(|e| matches!(
            e.event,
            ParticipantEvent::ParticipantForwardOutcomeRecorded { .. }
        )),
        "proof only after the durable handoff"
    );
    let proof = actor
        .participant_run_evidence_strict(&c)
        .unwrap()
        .forward_outcome
        .expect("proof after handoff");
    assert_eq!(proof.receipt.as_deref(), Some("outbox-1"));
}

fn accepted_actor(journal: &Journal, dedupe: &Dedupe, c: &SagaContext) -> Actor {
    let mut actor = Actor::new(journal, dedupe);
    let out = deliver(&mut actor, started(c));
    assert_eq!(out.count("step_accepted"), 1, "{:?}", out.valid);
    actor
}

fn completion() -> AcceptedStepCompletion {
    AcceptedStepCompletion {
        completed_at_millis: 200,
        output: b"done".to_vec(),
        compensation_data: b"release".to_vec(),
    }
}

#[test]
fn authoritative_accepted_completion_is_a_single_proof_append() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_accf", "accf", 100);
    let mut actor = accepted_actor(&journal, &dedupe, &c);
    let event = complete_accepted_workflow_step(
        &mut actor,
        c.saga_id,
        StepExecutionId::new("ext-1"),
        completion(),
    )
    .expect("completion");
    assert_eq!(count_rows(&journal, "StepExecutionCompleted"), 0);
    assert_eq!(count_rows(&journal, "ParticipantForwardOutcomeRecorded"), 1);
    let proof = actor
        .participant_run_evidence_strict(&c)
        .unwrap()
        .forward_outcome
        .expect("proof");
    assert_eq!(
        format!("{:?}", proof.completion_event()),
        format!("{event:?}")
    );
    // A restart must see a completed step, not an accepted one still pending.
    let events = recover(&journal, &dedupe, "accf", "wf_accf");
    assert!(
        events
            .iter()
            .all(|e| !matches!(e, SagaChoreographyEvent::StepAccepted { .. })),
        "{events:?}"
    );
}

#[test]
fn accepted_completion_append_failure_keeps_typed_bytes_and_quarantines() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_accf", "accf", 100);
    let mut actor = accepted_actor(&journal, &dedupe, &c);
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("ParticipantForwardOutcomeRecorded"),
    );
    let result = complete_accepted_workflow_step(
        &mut actor,
        c.saga_id,
        StepExecutionId::new("ext-1"),
        completion(),
    );
    assert!(result.is_err(), "{result:?}");
    let evidence = rows(&journal).into_iter().find_map(|e| match e.event {
        ParticipantEvent::ParticipantReconciliationEvidence {
            output,
            compensation_data,
            ..
        } => Some((output, compensation_data)),
        _ => None,
    });
    assert_eq!(
        evidence,
        Some((b"done".to_vec(), b"release".to_vec())),
        "typed result bytes must be retained"
    );
    assert_eq!(
        actor.terminal_run_outcome_strict(&c).unwrap(),
        Some(ParticipantTerminalKind::Quarantined)
    );
    let run = actor.participant_run_evidence_strict(&c).unwrap();
    assert!(run.forward_outcome.is_none());
    assert!(run.failure_requires_quarantine());
}

// ---------------------------------------------------------------------------
// P1-B end to end: resolver SagaFailed fed back into the accepted actor
// ---------------------------------------------------------------------------

fn failed_ordinarily(actor: &Actor, c: &SagaContext) {
    assert_eq!(
        actor.terminal_run_outcome_strict(c).unwrap(),
        Some(ParticipantTerminalKind::Failed),
        "definitive rejection must stay an ordinary Failed"
    );
    assert_eq!(actor.quarantine_hooks, 0);
    assert_eq!(actor.failed_hooks, 1);
}

fn assert_rejected_accepted_step_fails_ordinarily<
    C: icanact_saga_choreography::SagaWorkflowContract,
>(
    step: &'static str,
    execution: &str,
) {
    let (journal, dedupe) = stores();
    let (bus, seen) = recording_bus(C::saga_type(), false);
    bus.register_workflow_contract_provider::<C>()
        .expect("contract");
    bus.register_bound_workflow_step(C::saga_type(), step)
        .unwrap();
    let _resolver = bus
        .attach_terminal_resolver_for_contract::<C>("rechallenge-resolver")
        .expect("resolver");
    let c = ctx(C::saga_type(), step, SagaContext::now_millis());
    bus.publish_strict(started(&c)).expect("start admitted");

    let mut actor = Actor::new(&journal, &dedupe);
    actor.saga.attach_bus(bus.clone());
    deliver(&mut actor, started(&c));
    let rejection = fail_accepted_workflow_step(
        &mut actor,
        c.saga_id,
        StepExecutionId::new(execution),
        AcceptedStepFailure {
            failed_at_millis: SagaContext::now_millis(),
            reason: "remote rejected".into(),
            requires_compensation: false,
        },
    )
    .expect("authoritative failure");
    bus.publish_strict(rejection).expect("failure published");

    let (tx, rx) = mpsc::channel();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let terminal = seen
            .lock()
            .unwrap()
            .iter()
            .find(|e| e.terminal_outcome().is_some())
            .cloned();
        if let Some(terminal) = terminal {
            tx.send(terminal).unwrap();
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "resolver never ended the saga: {:?}",
            seen.lock().unwrap()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let terminal = rx.recv().unwrap();
    assert!(
        matches!(terminal, SagaChoreographyEvent::SagaFailed { .. }),
        "{terminal:?}"
    );
    let out = deliver(&mut actor, terminal);
    assert_eq!(out.count("saga_quarantined"), 0, "{:?}", out.valid);
    failed_ordinarily(&actor, &c);
}

#[test]
fn rejected_accepted_step_fails_ordinarily_through_a_real_resolver() {
    assert_rejected_accepted_step_fails_ordinarily::<BareContract>("accn", "ext-9");
}

#[test]
fn rejected_accepted_step_with_potential_undo_bytes_fails_ordinarily_through_a_real_resolver() {
    assert_rejected_accepted_step_fails_ordinarily::<AcceptContract>("accf", "ext-1");
}

#[test]
fn real_accepted_watchdog_resolves_potential_undo_metadata_before_terminal_failure() {
    let (journal, dedupe) = stores();
    let (bus, seen) = recording_bus("wf_accf", false);
    bus.register_workflow_contract_provider::<AcceptContract>()
        .unwrap();
    bus.register_bound_workflow_step("wf_accf", "accf").unwrap();
    bus.attach_terminal_resolver_for_contract::<AcceptContract>("real-timeout")
        .unwrap();
    let c = ctx("wf_accf", "accf", SagaContext::now_millis());
    bus.publish_strict(started(&c)).unwrap();
    let mut actor = Actor::new(&journal, &dedupe);
    actor.accepted_idle_timeout = Duration::from_millis(100);
    actor.saga.attach_bus(bus.clone());
    deliver(&mut actor, started(&c));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if seen
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.terminal_outcome().is_some())
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "watchdog did not resolve: {:?}",
            seen.lock().unwrap()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let events = seen.lock().unwrap().clone();
    for event in events
        .iter()
        .filter(|event| matches!(event, SagaChoreographyEvent::StepFailed { .. }))
    {
        deliver(&mut actor, event.clone());
    }
    assert_eq!(
        count_rows(&journal, "StepExecutionFailed"),
        1,
        "real watchdog must publish the definitive step disposition before its terminal: {events:?}"
    );
    let terminal = events
        .iter()
        .find(|event| event.terminal_outcome().is_some())
        .unwrap();
    assert!(
        matches!(terminal, SagaChoreographyEvent::SagaFailed { .. }),
        "{events:?}"
    );
    deliver(&mut actor, terminal.clone());
    failed_ordinarily(&actor, &c);
}

fn resolver_timeout(c: &SagaContext) -> SagaChoreographyEvent {
    let mut timeout = c.next_step("accf".into());
    timeout.event_timestamp_millis = c.saga_started_at_millis + 10;
    SagaChoreographyEvent::StepFailed {
        context: timeout,
        participant_id: "accf".into(),
        error_code: Some("idle".into()),
        error: "accepted step idle timeout: step=accf execution_id=ext-1".into(),
        requires_compensation: false,
    }
}

#[test]
fn resolver_origin_accepted_timeout_is_durably_resolved_before_the_failure_arrives() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_accf", "accf", 100);
    let mut actor = accepted_actor(&journal, &dedupe, &c);

    deliver(&mut actor, resolver_timeout(&c));
    assert_eq!(
        count_rows(&journal, "StepExecutionFailed"),
        1,
        "volatile metadata may not vanish without durable resolution"
    );
    let run = actor.participant_run_evidence_strict(&c).unwrap();
    assert!(!run.accepted_forward_pending);

    let out = deliver(&mut actor, saga_failed(&c, "accepted step timed out"));
    assert_eq!(out.count("saga_quarantined"), 0, "{:?}", out.valid);
    failed_ordinarily(&actor, &c);
}

#[test]
fn resolver_timeout_with_unwritable_failure_keeps_pending_work_visible() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_accf", "accf", 100);
    let mut actor = accepted_actor(&journal, &dedupe, &c);
    journal.fail_always(
        JournalOp::Append,
        FaultTrigger::EventKind("StepExecutionFailed"),
    );
    deliver(&mut actor, resolver_timeout(&c));
    journal.disarm();
    // The durable resolution failed, so the accepted work is still unresolved
    // and the later ordinary failure escalates rather than hiding it.
    assert!(
        actor
            .participant_run_evidence_strict(&c)
            .unwrap()
            .accepted_forward_pending
    );
    deliver(&mut actor, saga_failed(&c, "accepted step timed out"));
    assert_eq!(
        actor.terminal_run_outcome_strict(&c).unwrap(),
        Some(ParticipantTerminalKind::Quarantined)
    );
}

#[test]
fn old_run_timeout_cannot_resolve_the_successor_run() {
    let (journal, dedupe) = stores();
    let old = ctx("wf_accf", "accf", 100);
    let mut actor = accepted_actor(&journal, &dedupe, &old);
    deliver(&mut actor, resolver_timeout(&old));
    deliver(&mut actor, saga_failed(&old, "timed out"));

    let successor = ctx("wf_accf", "accf", 200);
    let out = deliver(&mut actor, started(&successor));
    assert_eq!(out.count("step_accepted"), 1, "{:?}", out.valid);
    let failures_before = count_rows(&journal, "StepExecutionFailed");

    deliver(&mut actor, resolver_timeout(&old));

    assert_eq!(count_rows(&journal, "StepExecutionFailed"), failures_before);
    assert!(
        actor
            .participant_run_evidence_strict(&successor)
            .unwrap()
            .accepted_forward_pending,
        "the successor's accepted work must stay pending"
    );
    assert!(
        actor
            .saga
            .accepted_workflow_steps
            .contains_key(&successor.saga_id)
    );
}

// ---------------------------------------------------------------------------
// P2-b: a proven undo whose acknowledgement was lost is resent at startup
// ---------------------------------------------------------------------------

#[test]
fn startup_resends_the_proven_undo_acknowledgement_without_a_new_request_or_undo() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_plain", "plain", 100);
    let mut first = Actor::new(&journal, &dedupe);
    deliver(&mut first, started(&c));
    let undone = deliver(&mut first, undo_request(&c, "plain"));
    assert_eq!(first.comp_calls, 1);
    let original_ack = undone
        .valid
        .iter()
        .find(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. }))
        .expect("ack emitted before the crash")
        .clone();
    // The acknowledgement was lost: nothing reached the resolver.

    let events = recover(&journal, &dedupe, "plain", "wf_plain");
    assert_eq!(events.len(), 1, "{events:?}");
    let SagaChoreographyEvent::CompensationCompleted { context } = &events[0] else {
        panic!("expected a resent acknowledgement: {events:?}");
    };
    assert_eq!(context.saga_id, c.saga_id);
    assert_eq!(context.saga_type.as_ref(), "wf_plain");
    assert_eq!(context.saga_started_at_millis, 100);
    assert_eq!(context.step_name.as_ref(), "plain");
    assert_eq!(
        context.step_name.as_ref(),
        original_ack.context().step_name.as_ref()
    );
    let mut restarted = Actor::new(&journal, &dedupe);
    recover_accepted_workflow_steps_for_saga_type(&mut restarted.saga, "plain", "wf_plain")
        .unwrap();
    assert_eq!(restarted.comp_calls, 0, "no physical undo at startup");
    assert_eq!(count_rows(&journal, "CompensationStarted"), 1);
    assert_eq!(count_rows(&journal, "CompensationRequestRecorded"), 1);
}

#[test]
fn startup_does_not_resend_an_undo_acknowledgement_after_terminal_resolution() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_plain", "plain", 100);
    let mut first = Actor::new(&journal, &dedupe);
    deliver(&mut first, started(&c));
    deliver(&mut first, undo_request(&c, "plain"));
    deliver(&mut first, saga_failed(&c, "rolled back"));
    assert!(recover(&journal, &dedupe, "plain", "wf_plain").is_empty());
}

#[test]
fn startup_does_not_resend_for_an_undo_with_unknown_outcome() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_plain", "plain", 100);
    let mut first = Actor::new(&journal, &dedupe);
    deliver(&mut first, started(&c));
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("CompensationCompleted"),
    );
    deliver(&mut first, undo_request(&c, "plain"));
    let events = recover(&journal, &dedupe, "plain", "wf_plain");
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
        "an undo that is not durably proven complete must not be acknowledged: {events:?}"
    );
}

// ---------------------------------------------------------------------------
// AllOf liveness: durable dependency observations
// ---------------------------------------------------------------------------

#[test]
fn allof_dependency_observed_before_a_restart_still_completes_the_join() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_all", "join", 100);
    let mut first = Actor::new(&journal, &dedupe);
    deliver(&mut first, started(&c));
    let out = deliver(&mut first, dependency_done(&c, "a"));
    assert_eq!(first.calls, 0);
    assert_eq!(out.count("step_started"), 0);
    assert_eq!(
        count_rows(&journal, "ParticipantDependencyCompletedRecorded"),
        1
    );

    // Restart between the two dependencies.
    let mut restarted = Actor::new(&journal, &dedupe);
    recover_accepted_workflow_steps_for_saga_type(&mut restarted.saga, "join", "wf_all").unwrap();
    let out = deliver(&mut restarted, dependency_done(&c, "b"));
    assert_eq!(
        restarted.calls, 1,
        "join must fire once both inputs are durable"
    );
    assert_eq!(out.count("step_completed"), 1, "{:?}", out.valid);
}

#[test]
fn allof_hydrates_durably_even_without_the_startup_hook() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_all", "join", 100);
    let mut first = Actor::new(&journal, &dedupe);
    deliver(&mut first, started(&c));
    deliver(&mut first, dependency_done(&c, "a"));
    let mut restarted = Actor::new(&journal, &dedupe);
    deliver(&mut restarted, dependency_done(&c, "b"));
    assert_eq!(restarted.calls, 1);
}

#[test]
fn allof_observations_from_another_run_do_not_count() {
    let (journal, dedupe) = stores();
    let old = ctx("wf_all", "join", 100);
    let mut first = Actor::new(&journal, &dedupe);
    deliver(&mut first, started(&old));
    deliver(&mut first, dependency_done(&old, "a"));
    deliver(&mut first, saga_failed(&old, "abandoned"));

    let fresh = ctx("wf_all", "join", 200);
    let mut next = Actor::new(&journal, &dedupe);
    deliver(&mut next, started(&fresh));
    deliver(&mut next, dependency_done(&fresh, "b"));
    assert_eq!(
        next.calls, 0,
        "the earlier run's input must not satisfy this run"
    );
    deliver(&mut next, dependency_done(&fresh, "a"));
    assert_eq!(next.calls, 1);
}

#[test]
fn allof_observation_write_failure_is_visible_and_never_executes() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_all", "join", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    deliver(&mut actor, started(&c));
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("ParticipantDependencyCompletedRecorded"),
    );
    let out = deliver(&mut actor, dependency_done(&c, "a"));
    assert_eq!(out.count("saga_quarantined"), 1, "{:?}", out.valid);
    deliver(&mut actor, dependency_done(&c, "b"));
    assert_eq!(actor.calls, 0);
}

#[test]
fn dependency_only_history_is_idle_not_an_ambiguity() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_all", "join", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    deliver(&mut actor, started(&c));
    deliver(&mut actor, dependency_done(&c, "a"));
    assert!(recover(&journal, &dedupe, "join", "wf_all").is_empty());
    assert_eq!(
        actor.terminal_run_outcome_strict(&c).unwrap(),
        None,
        "waiting for the second input is healthy"
    );
}
