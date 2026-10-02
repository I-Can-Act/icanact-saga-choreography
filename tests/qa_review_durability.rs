//! Review remediation for durable participant recovery and ingress: no repeated or
//! forgotten effects, no ordinary terminal while local obligations remain, and no
//! quarantine of healthy idle work. Everything here is driven through the public
//! ingress/recovery surface over real journals (fault-injecting in-memory stores).

// Each test crate uses only a subset of the shared fixture re-exports.
#[allow(unused_imports)]
mod support;

use std::cell::{Cell, RefCell};
use std::task::{Context, Poll, Waker};

use icanact_saga_choreography::durability::{
    apply_async_participant_saga_ingress, apply_sync_participant_saga_ingress,
    apply_sync_workflow_participant_saga_ingress_with_hooks, classify_recovery,
    collect_startup_recovery_events_for_saga_type, complete_accepted_workflow_step,
    recover_accepted_workflow_steps_for_saga_type,
};
use icanact_saga_choreography::{
    AcceptedStepCompletion, AcceptedStepError, AcceptedStepPolicy, AcceptedStepTimeoutOutcome,
    AsyncSagaParticipant, CompensationError, CompensationOutput, DependencySpec,
    EffectDispatchError, EffectDispatchOutcome, EffectDispatchRequest, HasSagaParticipantSupport,
    HasSagaWorkflowParticipants, InMemoryDedupe, InMemoryJournal, JournalEntry, JournalError,
    ParticipantDedupeStore, ParticipantEvent, ParticipantJournal, ParticipantTerminalKind, PeerId,
    RecoveryDecision, RecoveryPolicy, SagaBoxFuture, SagaChoreographyEvent, SagaContext, SagaId,
    SagaParticipant, SagaParticipantSupport, SagaStateEntry, SagaStateExt, SagaWorkflowParticipant,
    StepError, StepExecutionId, StepOutput,
};
use std::time::Duration;
use support::{FaultDedupe, FaultJournal, FaultTrigger, JournalOp};

type Journal = FaultJournal<InMemoryJournal>;
type Dedupe = FaultDedupe<InMemoryDedupe>;

const SAGA: u64 = 41;

struct Actor {
    saga: SagaParticipantSupport<Journal, Dedupe>,
    pay_calls: usize,
    comp_calls: usize,
    any_calls: usize,
    dispatched: usize,
    last_comp_data: Vec<u8>,
    failed_hooks: usize,
    quarantine_hooks: usize,
}

impl Actor {
    fn new(journal: &Journal, dedupe: &Dedupe) -> Self {
        Self {
            saga: SagaParticipantSupport::new(journal.clone(), dedupe.clone()),
            pay_calls: 0,
            comp_calls: 0,
            any_calls: 0,
            dispatched: 0,
            last_comp_data: Vec::new(),
            failed_hooks: 0,
            quarantine_hooks: 0,
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

struct Pay;
struct Idle;
struct Accepting;
struct AnyPure;
struct AnyEffect;
struct Notify;

static WORKFLOWS: [&'static dyn SagaWorkflowParticipant<Actor>; 6] =
    [&Pay, &Idle, &Accepting, &AnyPure, &AnyEffect, &Notify];

impl HasSagaWorkflowParticipants for Actor {
    fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
        &WORKFLOWS
    }
}

fn undo(actor: &mut Actor, data: &[u8]) -> Result<CompensationOutput, CompensationError> {
    actor.comp_calls += 1;
    actor.last_comp_data = data.to_vec();
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

impl SagaWorkflowParticipant<Actor> for Pay {
    fn step_name(&self) -> &'static str {
        "pay"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_pay"]
    }
    fn execute_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        _i: &[u8],
    ) -> Result<StepOutput, StepError> {
        actor.pay_calls += 1;
        Ok(StepOutput::Completed {
            output: b"paid".to_vec(),
            compensation_data: b"refund".to_vec(),
        })
    }
    fn compensate_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        undo(actor, data)
    }
    hooks!();
}

impl SagaWorkflowParticipant<Actor> for Idle {
    fn step_name(&self) -> &'static str {
        "idle"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_idle"]
    }
    fn depends_on(&self) -> DependencySpec {
        DependencySpec::After("never")
    }
    fn execute_step(
        &self,
        _a: &mut Actor,
        _c: &SagaContext,
        _i: &[u8],
    ) -> Result<StepOutput, StepError> {
        Err(StepError::Terminal {
            reason: "never runs".into(),
        })
    }
    fn compensate_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        undo(actor, data)
    }
    hooks!();
}

impl SagaWorkflowParticipant<Actor> for Accepting {
    fn step_name(&self) -> &'static str {
        "acc"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_acc"]
    }
    fn execute_step(
        &self,
        _a: &mut Actor,
        _c: &SagaContext,
        _i: &[u8],
    ) -> Result<StepOutput, StepError> {
        Ok(StepOutput::Accepted {
            execution_id: StepExecutionId::new("ext-1"),
            policy: AcceptedStepPolicy {
                idle_timeout: Duration::from_secs(60),
                hard_timeout: Duration::from_secs(3600),
                timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                    requires_compensation: true,
                },
            },
            compensation_data: b"release".to_vec(),
        })
    }
    fn compensate_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        undo(actor, data)
    }
    hooks!();
}

impl SagaWorkflowParticipant<Actor> for AnyPure {
    fn step_name(&self) -> &'static str {
        "merge"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_any"]
    }
    fn depends_on(&self) -> DependencySpec {
        DependencySpec::AnyOf(&["a", "b"])
    }
    fn execute_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        _i: &[u8],
    ) -> Result<StepOutput, StepError> {
        actor.any_calls += 1;
        Ok(StepOutput::Completed {
            output: b"merged".to_vec(),
            compensation_data: Vec::new(),
        })
    }
    fn compensate_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        undo(actor, data)
    }
    hooks!();
}

impl SagaWorkflowParticipant<Actor> for AnyEffect {
    fn step_name(&self) -> &'static str {
        "mergefx"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_anyfx"]
    }
    fn depends_on(&self) -> DependencySpec {
        DependencySpec::AnyOf(&["a", "b"])
    }
    fn execute_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        _i: &[u8],
    ) -> Result<StepOutput, StepError> {
        actor.any_calls += 1;
        Ok(StepOutput::CompletedWithEffect {
            output: b"sent".to_vec(),
            compensation_data: Vec::new(),
            effect: "email".into(),
        })
    }
    fn compensate_step(
        &self,
        actor: &mut Actor,
        _c: &SagaContext,
        data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        undo(actor, data)
    }
    fn dispatch_effect(
        &self,
        actor: &mut Actor,
        _r: &EffectDispatchRequest<'_>,
    ) -> Result<EffectDispatchOutcome, EffectDispatchError> {
        actor.dispatched += 1;
        Ok(EffectDispatchOutcome::Durable {
            receipt: "outbox-9".into(),
        })
    }
    hooks!();
}

impl SagaWorkflowParticipant<Actor> for Notify {
    fn step_name(&self) -> &'static str {
        "notify"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_notify"]
    }
    fn execute_step(
        &self,
        _a: &mut Actor,
        _c: &SagaContext,
        _i: &[u8],
    ) -> Result<StepOutput, StepError> {
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
        data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        undo(actor, data)
    }
    fn dispatch_effect(
        &self,
        actor: &mut Actor,
        _r: &EffectDispatchRequest<'_>,
    ) -> Result<EffectDispatchOutcome, EffectDispatchError> {
        actor.dispatched += 1;
        Ok(EffectDispatchOutcome::Durable {
            receipt: "outbox-1".into(),
        })
    }
    hooks!();
}

fn stores() -> (Journal, Dedupe) {
    (
        FaultJournal::new(InMemoryJournal::new()),
        FaultDedupe::new(InMemoryDedupe::new()),
    )
}

fn ctx(saga_type: &str, step: &str, started_at: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(SAGA),
        saga_type: saga_type.into(),
        step_name: step.into(),
        correlation_id: SAGA,
        causation_id: SAGA,
        trace_id: SAGA,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: started_at,
        event_timestamp_millis: started_at,
    }
}

fn started(c: &SagaContext) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: c.clone(),
        payload: b"input".to_vec(),
    }
}

fn completed_from(c: &SagaContext, step: &str) -> SagaChoreographyEvent {
    let mut c = c.clone();
    c.step_name = step.into();
    SagaChoreographyEvent::StepCompleted {
        context: c,
        output: b"in".to_vec(),
        saga_input: b"input".to_vec(),
        compensation_available: false,
    }
}

fn failed(c: &SagaContext, reason: &str) -> SagaChoreographyEvent {
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
    terminal_callbacks: usize,
}

impl Delivery {
    fn quarantines(&self) -> usize {
        self.valid
            .iter()
            .filter(|e| matches!(e, SagaChoreographyEvent::SagaQuarantined { .. }))
            .count()
    }
    fn step_completed(&self) -> Option<&SagaChoreographyEvent> {
        self.valid
            .iter()
            .find(|e| matches!(e, SagaChoreographyEvent::StepCompleted { .. }))
    }
}

fn deliver(actor: &mut Actor, event: SagaChoreographyEvent) -> Delivery {
    let valid = RefCell::new(Vec::new());
    let invalid = RefCell::new(Vec::new());
    let callbacks = Cell::new(0usize);
    apply_sync_workflow_participant_saga_ingress_with_hooks(
        actor,
        event,
        |_a, e| {
            if matches!(
                e,
                SagaChoreographyEvent::SagaCompleted { .. }
                    | SagaChoreographyEvent::SagaFailed { .. }
                    | SagaChoreographyEvent::SagaQuarantined { .. }
            ) {
                callbacks.set(callbacks.get() + 1)
            }
        },
        |e| invalid.borrow_mut().push(e.clone()),
        |_a, e| valid.borrow_mut().push(e.clone()),
    );
    Delivery {
        valid: valid.into_inner(),
        invalid: invalid.into_inner(),
        terminal_callbacks: callbacks.get(),
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

/// Journal view whose rows are all ancient, to prove age is never a stale-run proxy.
struct Aged(Journal);

impl ParticipantJournal for Aged {
    fn append(&self, saga_id: SagaId, event: ParticipantEvent) -> Result<u64, JournalError> {
        self.0.append(saga_id, event)
    }
    fn read(&self, saga_id: SagaId) -> Result<Vec<JournalEntry>, JournalError> {
        let mut rows = self.0.read(saga_id)?;
        for row in &mut rows {
            row.recorded_at_millis = 1_000;
        }
        Ok(rows)
    }
    fn list_sagas(&self) -> Result<Vec<SagaId>, JournalError> {
        self.0.list_sagas()
    }
    fn prune(&self, saga_id: SagaId) -> Result<(), JournalError> {
        self.0.prune(saga_id)
    }
}

fn entries(journal: &Journal) -> Vec<JournalEntry> {
    journal.read(SagaId::new(SAGA)).unwrap()
}

// ---------------------------------------------------------------------------
// P1-2: idle and healthy work is never quarantined by restart
// ---------------------------------------------------------------------------

#[test]
fn idle_participant_run_record_is_not_stale_execution_however_old() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_idle", "idle", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    let out = deliver(&mut actor, started(&c));
    assert_eq!(out.quarantines(), 0);
    assert!(out.invalid.is_empty(), "{:?}", out.invalid);
    assert!(
        entries(&journal)
            .iter()
            .any(|e| matches!(e.event, ParticipantEvent::ParticipantRunRecorded { .. })),
        "the idle participant journals a run record"
    );

    let aged = Aged(journal.clone());
    let events = collect_startup_recovery_events_for_saga_type(&aged, &dedupe, "idle", "wf_idle")
        .expect("collect");
    assert!(
        events.is_empty(),
        "idle run must not be quarantined: {events:?}"
    );

    let decision = classify_recovery(
        &aged.read(SagaId::new(SAGA)).unwrap(),
        u64::MAX / 2,
        RecoveryPolicy { stale_after_ms: 1 },
    );
    assert_eq!(decision, RecoveryDecision::Continue);
}

#[test]
fn healthy_accepted_deadline_is_replayed_not_quarantined_after_restart() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_acc", "acc", SagaContext::now_millis());
    let mut actor = Actor::new(&journal, &dedupe);
    deliver(&mut actor, started(&c));
    let aged = Aged(journal.clone());
    let events = collect_startup_recovery_events_for_saga_type(&aged, &dedupe, "acc", "wf_acc")
        .expect("collect");
    assert!(
        matches!(
            events.as_slice(),
            [SagaChoreographyEvent::StepAccepted { .. }]
        ),
        "{events:?}"
    );
}

// ---------------------------------------------------------------------------
// P1-1 recovery: unknown intent / unconfirmed result are visible quarantines at any age
// ---------------------------------------------------------------------------

#[test]
fn open_forward_intent_quarantines_at_startup_within_the_age_window() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_pay", "pay", 100);
    let actor = Actor::new(&journal, &dedupe);
    actor.admit_participant_event_strict(&c).unwrap();
    journal
        .append(
            c.saga_id,
            ParticipantEvent::StepExecutionStarted {
                attempt: 1,
                started_at_millis: SagaContext::now_millis(),
            },
        )
        .unwrap();
    let events = recover(&journal, &dedupe, "pay", "wf_pay");
    assert!(
        matches!(events.as_slice(), [SagaChoreographyEvent::SagaQuarantined { context, .. }]
            if context.saga_started_at_millis == 100),
        "{events:?}"
    );
}

#[test]
fn unconfirmed_result_quarantines_visibly_and_is_not_resent() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_pay", "pay", 100);
    let actor = Actor::new(&journal, &dedupe);
    actor.admit_participant_event_strict(&c).unwrap();
    for event in [
        ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: 1,
        },
        ParticipantEvent::StepExecutionCompleted {
            output: b"paid".to_vec(),
            compensation_data: b"refund".to_vec(),
            completed_at_millis: 2,
        },
    ] {
        journal.append(c.saga_id, event).unwrap();
    }
    let events = recover(&journal, &dedupe, "pay", "wf_pay");
    assert!(
        matches!(
            events.as_slice(),
            [SagaChoreographyEvent::SagaQuarantined { .. }]
        ),
        "{events:?}"
    );
}

#[test]
fn confirmed_forward_proof_resends_the_original_completion_without_effects() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_notify", "notify", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    let first = deliver(&mut actor, started(&c));
    let original = first.step_completed().expect("step completed").clone();
    assert_eq!(actor.dispatched, 1);

    let proof = entries(&journal)
        .into_iter()
        .find_map(|e| match e.event {
            ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome } => Some(outcome),
            _ => None,
        })
        .expect("forward proof must be durable before success is published");
    assert_eq!(proof.effect.as_deref(), Some("email"));
    assert_eq!(proof.receipt.as_deref(), Some("outbox-1"));
    assert_eq!(
        format!("{:?}", proof.completion_event()),
        format!("{original:?}")
    );

    // Restart: a fresh actor over the same durable stores.
    let events = recover(&journal, &dedupe, "notify", "wf_notify");
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(format!("{:?}", events[0]), format!("{original:?}"));

    let mut restarted = Actor::new(&journal, &dedupe);
    recover_accepted_workflow_steps_for_saga_type(&mut restarted.saga, "notify", "wf_notify")
        .unwrap();
    assert!(matches!(
        restarted.saga.saga_states.get(&c.saga_id),
        Some(SagaStateEntry::Completed(_))
    ));
    assert!(restarted.saga.dependency_fired.contains(&c.saga_id));
    assert_eq!(restarted.dispatched, 0, "recovery must never re-dispatch");
}

#[test]
fn forward_proof_write_failure_publishes_no_success_and_never_redispatches() {
    let (journal, dedupe) = stores();
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("ParticipantForwardOutcomeRecorded"),
    );
    let c = ctx("wf_notify", "notify", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    let out = deliver(&mut actor, started(&c));
    assert_eq!(actor.dispatched, 1, "effect already dispatched once");
    assert!(out.step_completed().is_none(), "{:?}", out.valid);
    assert_eq!(out.quarantines(), 1, "{:?}", out.valid);

    let events = recover(&journal, &dedupe, "notify", "wf_notify");
    assert!(
        events.is_empty()
            || events
                .iter()
                .all(|e| matches!(e, SagaChoreographyEvent::SagaQuarantined { .. }))
    );
    assert_eq!(actor.dispatched, 1);
}

// ---------------------------------------------------------------------------
// P2-1: undo after a cold restart uses the journaled compensation
// ---------------------------------------------------------------------------

#[test]
fn undo_request_after_cold_restart_runs_with_journaled_compensation() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_pay", "pay", 100);
    let mut first = Actor::new(&journal, &dedupe);
    deliver(&mut first, started(&c));
    assert_eq!(first.pay_calls, 1);

    let mut restarted = Actor::new(&journal, &dedupe);
    let out = deliver(&mut restarted, undo_request(&c, "pay"));
    assert_eq!(restarted.comp_calls, 1, "{:?}", out.valid);
    assert_eq!(restarted.last_comp_data, b"refund");
    assert_eq!(restarted.pay_calls, 0);
    assert!(
        out.valid
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
        "{:?}",
        out.valid
    );
}

#[test]
fn undo_with_an_unknown_open_undo_intent_quarantines_instead_of_repeating() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_pay", "pay", 100);
    let mut first = Actor::new(&journal, &dedupe);
    deliver(&mut first, started(&c));
    journal
        .append(
            c.saga_id,
            ParticipantEvent::CompensationStarted {
                attempt: 1,
                started_at_millis: 5,
            },
        )
        .unwrap();
    let mut restarted = Actor::new(&journal, &dedupe);
    let out = deliver(&mut restarted, undo_request(&c, "pay"));
    assert_eq!(
        restarted.comp_calls, 0,
        "unknown undo state must not be repeated"
    );
    assert_eq!(out.quarantines(), 1, "{:?}", out.valid);
}

// ---------------------------------------------------------------------------
// P2-2: AnyOf
// ---------------------------------------------------------------------------

#[test]
fn confirmed_pure_anyof_branch_is_not_false_quarantined_after_restart() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_any", "x", 100);
    let mut first = Actor::new(&journal, &dedupe);
    let out = deliver(&mut first, completed_from(&c, "a"));
    assert_eq!(first.any_calls, 1);
    assert!(out.step_completed().is_some());

    let mut restarted = Actor::new(&journal, &dedupe);
    let out = deliver(&mut restarted, completed_from(&c, "b"));
    assert_eq!(restarted.any_calls, 0, "no re-execution");
    assert_eq!(out.quarantines(), 0, "{:?}", out.valid);
    assert!(out.valid.is_empty(), "{:?}", out.valid);
    assert_eq!(restarted.terminal_run_outcome_strict(&c).unwrap(), None);
}

#[test]
fn confirmed_anyof_declared_effect_is_not_dispatched_or_quarantined_again_after_restart() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_anyfx", "x", 100);
    let mut first = Actor::new(&journal, &dedupe);
    deliver(&mut first, completed_from(&c, "a"));
    assert_eq!((first.any_calls, first.dispatched), (1, 1));

    let mut restarted = Actor::new(&journal, &dedupe);
    let out = deliver(&mut restarted, completed_from(&c, "b"));
    assert_eq!((restarted.any_calls, restarted.dispatched), (0, 0));
    assert_eq!(
        out.quarantines(),
        0,
        "a stored dispatch receipt confirms the already-fired branch: {:?}",
        out.valid
    );
}

// ---------------------------------------------------------------------------
// P1-1 participant: ordinary failure with unresolved local evidence escalates
// ---------------------------------------------------------------------------

#[test]
fn ordinary_failure_with_accepted_work_escalates_visibly_and_keeps_late_evidence() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_acc", "acc", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    deliver(&mut actor, started(&c));
    assert!(actor.saga.accepted_workflow_steps.contains_key(&c.saga_id));

    let out = deliver(&mut actor, failed(&c, "timeout"));
    assert_eq!(
        out.quarantines(),
        1,
        "escalation must be visible: {:?}",
        out.valid
    );
    assert_eq!(actor.failed_hooks, 0, "never the ordinary failure hook");
    assert_eq!(actor.quarantine_hooks, 1);
    assert_eq!(
        actor.terminal_run_outcome_strict(&c).unwrap(),
        Some(ParticipantTerminalKind::Quarantined)
    );
    assert!(
        actor.saga.accepted_workflow_steps.contains_key(&c.saga_id),
        "accepted work stays retained"
    );

    let late = complete_accepted_workflow_step(
        &mut actor,
        c.saga_id,
        StepExecutionId::new("ext-1"),
        AcceptedStepCompletion {
            completed_at_millis: 500,
            output: b"done".to_vec(),
            compensation_data: b"release".to_vec(),
        },
    );
    assert!(
        matches!(late, Ok(SagaChoreographyEvent::SagaQuarantined { .. })),
        "{late:?}"
    );
    assert!(
        entries(&journal).iter().any(|e| matches!(&e.event,
            ParticipantEvent::ParticipantReconciliationEvidence { output, compensation_data, .. }
                if output == b"done" && compensation_data == b"release")),
        "late completion must retain durable output/compensation evidence"
    );
    let evidence = actor.participant_run_evidence_strict(&c).unwrap();
    assert_eq!(evidence.compensation_data, b"release");
    assert!(evidence.quarantined);
}

#[test]
fn ordinary_failure_with_unreversed_compensable_effect_escalates() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_pay", "pay", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    deliver(&mut actor, started(&c));
    let out = deliver(&mut actor, failed(&c, "later step failed"));
    assert_eq!(out.quarantines(), 1, "{:?}", out.valid);
    assert_eq!((actor.failed_hooks, actor.quarantine_hooks), (0, 1));
}

#[test]
fn ordinary_failure_of_an_idle_participant_stays_ordinary() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_idle", "idle", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    deliver(&mut actor, started(&c));
    let out = deliver(&mut actor, failed(&c, "elsewhere"));
    assert_eq!(out.quarantines(), 0, "{:?}", out.valid);
    assert_eq!((actor.failed_hooks, actor.quarantine_hooks), (1, 0));
    assert_eq!(
        actor.terminal_run_outcome_strict(&c).unwrap(),
        Some(ParticipantTerminalKind::Failed)
    );
}

// ---------------------------------------------------------------------------
// Accepted completion: forward proof before success, durable admission
// ---------------------------------------------------------------------------

fn accept(actor: &mut Actor, c: &SagaContext) {
    deliver(actor, started(c));
    assert!(actor.saga.accepted_workflow_steps.contains_key(&c.saga_id));
}

fn completion() -> AcceptedStepCompletion {
    AcceptedStepCompletion {
        completed_at_millis: 500,
        output: b"done".to_vec(),
        compensation_data: b"release".to_vec(),
    }
}

#[test]
fn accepted_completion_records_forward_proof_matching_the_returned_event() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_acc", "acc", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    accept(&mut actor, &c);
    let event = complete_accepted_workflow_step(
        &mut actor,
        c.saga_id,
        StepExecutionId::new("ext-1"),
        completion(),
    )
    .expect("completion");
    let proof = actor
        .participant_run_evidence_strict(&c)
        .unwrap()
        .forward_outcome
        .expect("proof recorded before success is returned");
    assert_eq!(
        format!("{:?}", proof.completion_event()),
        format!("{event:?}")
    );
    assert_eq!(proof.compensation_data, b"release");
}

#[test]
fn accepted_completion_fails_closed_when_forward_proof_cannot_be_recorded() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_acc", "acc", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    accept(&mut actor, &c);
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
    assert!(
        matches!(result, Err(AcceptedStepError::Durability { .. })),
        "{result:?}"
    );
    // The raw result is retained, and recovery treats it as unconfirmed.
    let aged = actor.participant_run_evidence_strict(&c).unwrap();
    assert!(aged.forward_result_recorded && aged.forward_outcome.is_none());
    assert!(aged.failure_requires_quarantine());
}

#[test]
fn low_level_resolution_consults_the_journal_not_the_bounded_cache() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_acc", "acc", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    accept(&mut actor, &c);
    // Durable terminal exists, but this process's cache never saw it.
    journal
        .append(
            c.saga_id,
            ParticipantEvent::ParticipantTerminalRecorded {
                saga_type: c.saga_type.clone(),
                saga_started_at_millis: c.saga_started_at_millis,
                outcome: ParticipantTerminalKind::Completed,
                reason: "done".into(),
                recorded_at_millis: 1,
            },
        )
        .unwrap();
    assert!(!actor.is_terminal_saga_latched(c.saga_id));
    let result = complete_accepted_workflow_step(
        &mut actor,
        c.saga_id,
        StepExecutionId::new("ext-1"),
        completion(),
    );
    assert!(
        matches!(result, Err(AcceptedStepError::AlreadyTerminal { .. })),
        "{result:?}"
    );
    assert!(
        !entries(&journal)
            .iter()
            .any(|e| matches!(e.event, ParticipantEvent::StepExecutionCompleted { .. })),
        "nothing may be journaled into a terminal run"
    );
}

#[test]
fn low_level_accept_is_rejected_for_a_durably_terminal_run_after_cache_loss() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_acc", "acc", 100);
    let mut first = Actor::new(&journal, &dedupe);
    first.admit_participant_event_strict(&c).unwrap();
    first
        .retain_terminal_saga_strict(&c, ParticipantTerminalKind::Completed, "done")
        .unwrap();
    let mut fresh = Actor::new(&journal, &dedupe);
    let result = icanact_saga_choreography::durability::accept_workflow_step(
        &mut fresh,
        c.clone(),
        "acc".into(),
        StepExecutionId::new("late"),
        AcceptedStepPolicy {
            idle_timeout: Duration::from_secs(1),
            hard_timeout: Duration::from_secs(2),
            timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
        },
        Vec::new(),
        b"undo".to_vec(),
    );
    assert!(
        matches!(result, Err(AcceptedStepError::AlreadyTerminal { .. })),
        "{result:?}"
    );
    assert!(
        !entries(&journal)
            .iter()
            .any(|e| matches!(e.event, ParticipantEvent::StepExecutionStarted { .. })),
        "no new execution may be journaled into a terminal run"
    );
}

#[test]
fn failed_accepted_metadata_write_keeps_available_compensation_evidence() {
    let (journal, dedupe) = stores();
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("AcceptedStepRecorded"),
    );
    let c = ctx("wf_acc", "acc", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    let result = icanact_saga_choreography::durability::accept_workflow_step(
        &mut actor,
        c.clone(),
        "acc".into(),
        StepExecutionId::new("ext-1"),
        AcceptedStepPolicy {
            idle_timeout: Duration::from_secs(1),
            hard_timeout: Duration::from_secs(2),
            timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
        },
        Vec::new(),
        b"undo-me".to_vec(),
    );
    assert!(
        matches!(result, Err(AcceptedStepError::Durability { .. })),
        "{result:?}"
    );
    assert!(
        entries(&journal).iter().any(|e| matches!(&e.event,
            ParticipantEvent::ParticipantReconciliationEvidence { compensation_data, .. }
                if compensation_data == b"undo-me")),
        "the compensation bytes must survive the failed metadata write"
    );
}

// ---------------------------------------------------------------------------
// P2-6: panic recovery identity
// ---------------------------------------------------------------------------

#[test]
fn panic_recovery_uses_the_recorded_run_identity() {
    use icanact_saga_choreography::durability::{
        ActiveSagaExecutionPhase, panic_quarantine_reason,
    };
    let (journal, dedupe) = stores();
    let c = ctx("wf_pay", "pay", 777);
    let actor = Actor::new(&journal, &dedupe);
    actor.admit_participant_event_strict(&c).unwrap();
    journal
        .append(
            c.saga_id,
            ParticipantEvent::Quarantined {
                reason: panic_quarantine_reason(ActiveSagaExecutionPhase::StepExecution, "boom"),
                quarantined_at_millis: 5,
            },
        )
        .unwrap();
    let events = recover(&journal, &dedupe, "pay", "wf_pay");
    assert!(
        matches!(events.as_slice(), [SagaChoreographyEvent::SagaQuarantined { context, .. }]
            if context.saga_started_at_millis == 777 && context.saga_type.as_ref() == "wf_pay"),
        "{events:?}"
    );
}

#[test]
fn unidentified_panic_history_requires_explicit_reconciliation() {
    use icanact_saga_choreography::durability::{
        ActiveSagaExecutionPhase, panic_quarantine_reason,
    };
    let (journal, dedupe) = stores();
    journal
        .append(
            SagaId::new(SAGA),
            ParticipantEvent::Quarantined {
                reason: panic_quarantine_reason(ActiveSagaExecutionPhase::StepExecution, "boom"),
                quarantined_at_millis: 5,
            },
        )
        .unwrap();
    let result = collect_startup_recovery_events_for_saga_type(&journal, &dedupe, "pay", "wf_pay");
    assert!(result.is_err(), "no invented run timestamp: {result:?}");
    assert!(
        !dedupe
            .contains(
                SagaId::new(SAGA),
                icanact_saga_choreography::durability::PANIC_QUARANTINE_PUBLISH_KEY
            )
            .unwrap(),
        "an unreplayable history must not be marked as published"
    );
}

// ---------------------------------------------------------------------------
// P2-7: terminal side-effect callbacks are fenced before invocation
// ---------------------------------------------------------------------------

#[test]
fn workflow_terminal_callback_runs_once_for_duplicate_ordinary_terminals() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_idle", "idle", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    deliver(&mut actor, started(&c));
    assert_eq!(deliver(&mut actor, failed(&c, "x")).terminal_callbacks, 1);
    assert_eq!(
        deliver(&mut actor, failed(&c, "x")).terminal_callbacks,
        0,
        "a duplicate terminal replay must not repeat the callback"
    );
    // Cache loss must not matter: the journal fences it.
    let mut restarted = Actor::new(&journal, &dedupe);
    assert_eq!(
        deliver(&mut restarted, failed(&c, "x")).terminal_callbacks,
        0
    );
}

#[test]
fn workflow_terminal_callback_is_skipped_for_a_stale_run() {
    let (journal, dedupe) = stores();
    let new = ctx("wf_idle", "idle", 200);
    let mut actor = Actor::new(&journal, &dedupe);
    deliver(&mut actor, started(&new));
    let stale = ctx("wf_idle", "idle", 100);
    assert_eq!(
        deliver(&mut actor, failed(&stale, "old")).terminal_callbacks,
        0
    );
}

#[test]
fn workflow_local_quarantine_notification_still_reaches_the_callback() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_pay", "pay", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    // Local quarantine writes its tombstone before its own publication returns.
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("StepExecutionCompleted"),
    );
    let out = deliver(&mut actor, started(&c));
    assert_eq!(out.quarantines(), 1);
    let echoed = SagaChoreographyEvent::SagaQuarantined {
        context: c.next_step("pay".into()),
        reason: "step result persistence failed".into(),
        step: "pay".into(),
        participant_id: "pay".into(),
    };
    assert_eq!(
        deliver(&mut actor, echoed).terminal_callbacks,
        1,
        "valid local quarantine notifications are preserved"
    );
}

struct SyncP {
    saga: SagaParticipantSupport<Journal, Dedupe>,
}
impl HasSagaParticipantSupport for SyncP {
    type Journal = Journal;
    type Dedupe = Dedupe;
    fn saga_support(&self) -> &SagaParticipantSupport<Journal, Dedupe> {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Journal, Dedupe> {
        &mut self.saga
    }
}
impl SagaParticipant for SyncP {
    type Error = ();
    fn step_name(&self) -> &str {
        "sync_step"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_sync"]
    }
    fn depends_on(&self) -> DependencySpec {
        DependencySpec::After("never")
    }
    fn execute_step(&mut self, _c: &SagaContext, _i: &[u8]) -> Result<StepOutput, StepError> {
        Err(StepError::Terminal {
            reason: "no".into(),
        })
    }
    fn compensate_step(
        &mut self,
        _c: &SagaContext,
        _d: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        Ok(CompensationOutput::Completed)
    }
}

#[test]
fn sync_participant_terminal_callback_is_fenced_for_duplicates_and_foreign_types() {
    let (journal, dedupe) = stores();
    let mut p = SyncP {
        saga: SagaParticipantSupport::new(journal.clone(), dedupe.clone()),
    };
    let count = Cell::new(0usize);
    let c = ctx("wf_sync", "sync_step", 100);
    for _ in 0..2 {
        apply_sync_participant_saga_ingress(
            &mut p,
            failed(&c, "x"),
            |_p, e| {
                if matches!(e, SagaChoreographyEvent::SagaFailed { .. }) {
                    count.set(count.get() + 1)
                }
            },
            |_| {},
        );
    }
    assert_eq!(
        count.get(),
        1,
        "duplicate ordinary terminal must not repeat the callback"
    );
}

struct AsyncP {
    saga: SagaParticipantSupport<Journal, Dedupe>,
}
impl HasSagaParticipantSupport for AsyncP {
    type Journal = Journal;
    type Dedupe = Dedupe;
    fn saga_support(&self) -> &SagaParticipantSupport<Journal, Dedupe> {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Journal, Dedupe> {
        &mut self.saga
    }
}
impl AsyncSagaParticipant for AsyncP {
    type Error = ();
    fn step_name(&self) -> &str {
        "async_step"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_async"]
    }
    fn depends_on(&self) -> DependencySpec {
        DependencySpec::After("never")
    }
    fn execute_step<'a>(
        &'a mut self,
        _c: &'a SagaContext,
        _i: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<StepOutput, StepError>> {
        Box::pin(async {
            Err(StepError::Terminal {
                reason: "no".into(),
            })
        })
    }
    fn compensate_step<'a>(
        &'a mut self,
        _c: &'a SagaContext,
        _d: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<CompensationOutput, CompensationError>> {
        Box::pin(async { Ok(CompensationOutput::Completed) })
    }
}

#[test]
fn async_participant_terminal_callback_is_fenced_for_duplicates() {
    let (journal, dedupe) = stores();
    let mut p = AsyncP {
        saga: SagaParticipantSupport::new(journal.clone(), dedupe.clone()),
    };
    let count = Cell::new(0usize);
    let c = ctx("wf_async", "async_step", 100);
    for _ in 0..2 {
        let mut fut = std::pin::pin!(apply_async_participant_saga_ingress(
            &mut p,
            failed(&c, "x"),
            |_p, e| {
                if matches!(e, SagaChoreographyEvent::SagaFailed { .. }) {
                    count.set(count.get() + 1)
                }
            },
            |_| {},
        ));
        let mut cx = Context::from_waker(Waker::noop());
        let Poll::Ready(()) = fut.as_mut().poll(&mut cx) else {
            panic!("ingress should be immediately ready");
        };
    }
    assert_eq!(count.get(), 1);
}

#[test]
fn late_accepted_completion_after_cold_quarantine_retains_typed_evidence_and_returns_quarantine() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_acc", "acc", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    accept(&mut actor, &c);
    deliver(&mut actor, failed(&c, "timeout"));
    let mut cold = Actor::new(&journal, &dedupe);
    recover_accepted_workflow_steps_for_saga_type(&mut cold.saga, "acc", "wf_acc").unwrap();
    let event = complete_accepted_workflow_step(
        &mut cold,
        c.saga_id,
        StepExecutionId::new("ext-1"),
        completion(),
    )
    .expect("late evidence yields quarantine, never success");
    assert!(
        matches!(event, SagaChoreographyEvent::SagaQuarantined { ref context, .. } if context.saga_started_at_millis == 100)
    );
    assert!(entries(&journal).iter().any(|e| matches!(&e.event, ParticipantEvent::ParticipantReconciliationEvidence { context, output, compensation_data, .. }
        if context.saga_started_at_millis == 100 && output == b"done" && compensation_data == b"release")));
    assert_eq!(
        cold.participant_run_evidence_strict(&c)
            .unwrap()
            .compensation_data,
        b"release"
    );
    assert_eq!(cold.pay_calls, 0);
}

#[test]
fn late_old_accepted_result_does_not_become_the_successors_generic_result() {
    let (journal, dedupe) = stores();
    let old = ctx("wf_acc", "acc", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    accept(&mut actor, &old);
    // Simulate a pre-upgrade unsafe ordinary failure, followed by a new admitted run.
    journal
        .append(
            old.saga_id,
            ParticipantEvent::ParticipantTerminalRecorded {
                saga_type: old.saga_type.clone(),
                saga_started_at_millis: 100,
                outcome: ParticipantTerminalKind::Failed,
                reason: "legacy failure".into(),
                recorded_at_millis: 1,
            },
        )
        .unwrap();
    let successor = ctx("wf_acc", "acc", 200);
    let mut cold = Actor::new(&journal, &dedupe);
    icanact_saga_choreography::accept_workflow_step(
        &mut cold,
        successor.clone(),
        "p".into(),
        StepExecutionId::new("ext-2"),
        AcceptedStepPolicy {
            idle_timeout: Duration::from_secs(60),
            hard_timeout: Duration::from_secs(120),
            timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
        },
        vec![],
        vec![],
    )
    .unwrap();
    let event = complete_accepted_workflow_step(
        &mut cold,
        old.saga_id,
        StepExecutionId::new("ext-1"),
        completion(),
    )
    .expect("old identified evidence remains retainable");
    assert!(
        matches!(event, SagaChoreographyEvent::SagaQuarantined { ref context, .. } if context.saga_started_at_millis == 100)
    );
    let current = cold.participant_run_evidence_strict(&successor).unwrap();
    assert!(
        !current.forward_result_recorded,
        "old unscoped result must not be attributed to the active successor"
    );
    assert!(current.accepted_forward_pending);
    assert_eq!(
        cold.saga
            .accepted_workflow_steps
            .get(&old.saga_id)
            .unwrap()
            .execution_id,
        StepExecutionId::new("ext-2")
    );
    assert!(matches!(
        cold.admit_participant_event_strict(&successor).unwrap(),
        icanact_saga_choreography::ParticipantAdmission::QuarantinedReuse {
            quarantined_started_at_millis: 100,
            ..
        }
    ));
}

#[test]
fn failed_accepted_result_write_preserves_available_output_and_compensation() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_acc", "acc", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    accept(&mut actor, &c);
    journal.fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("StepExecutionCompleted"),
    );
    let result = complete_accepted_workflow_step(
        &mut actor,
        c.saga_id,
        StepExecutionId::new("ext-1"),
        completion(),
    );
    assert!(matches!(result, Err(AcceptedStepError::Durability { .. })));
    assert!(entries(&journal).iter().any(|e| matches!(&e.event, ParticipantEvent::ParticipantReconciliationEvidence { output, compensation_data, .. }
        if output == b"done" && compensation_data == b"release")), "available late result bytes must not disappear on a failed append");
    assert_eq!(
        actor.terminal_run_outcome_strict(&c).unwrap(),
        Some(ParticipantTerminalKind::Quarantined)
    );
}

#[test]
fn completed_workflow_undo_after_cold_restart_resends_only_its_acknowledgement() {
    let (journal, dedupe) = stores();
    let c = ctx("wf_pay", "pay", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    deliver(&mut actor, started(&c));
    deliver(&mut actor, undo_request(&c, "pay"));
    assert_eq!(actor.comp_calls, 1);
    let mut cold = Actor::new(&journal, &dedupe);
    let out = deliver(&mut cold, undo_request(&c, "pay"));
    assert_eq!((cold.pay_calls, cold.comp_calls), (0, 0));
    assert!(
        out.valid
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
        "lost acknowledgement must be replayed without repeating undo: {:?}",
        out.valid
    );
    assert_eq!(out.quarantines(), 0);
}

#[test]
fn recognized_foreign_workflow_refusal_does_not_emit_owner_poisoning_quarantine() {
    let (journal, dedupe) = stores();
    let owner = ctx("wf_pay", "pay", 100);
    let foreign = ctx("wf_pay", "pay", 200);
    let mut actor = Actor::new(&journal, &dedupe);
    deliver(&mut actor, started(&owner));
    let out = deliver(&mut actor, started(&foreign));
    assert_eq!(actor.pay_calls, 1);
    assert!(
        out.valid.is_empty(),
        "refusal must not manufacture cross-run quarantine of the legitimate owner: {:?}",
        out.valid
    );
    assert_eq!(actor.terminal_run_outcome_strict(&owner).unwrap(), None);
}
