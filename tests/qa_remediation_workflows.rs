//! U6 workflow ingress safety: strict intent/result persistence, fail-closed storage
//! errors, effect dispatch, durable terminal fencing and retained quarantine evidence.

// Each test crate uses only a subset of the shared fixture re-exports.
#[allow(unused_imports)]
mod support;

use std::cell::RefCell;

use icanact_saga_choreography::durability::{
    apply_sync_workflow_participant_saga_ingress_with_hooks,
    collect_startup_recovery_events_for_saga_type,
};
use icanact_saga_choreography::{
    CompensationError, CompensationOutput, EffectDispatchError, EffectDispatchOutcome,
    EffectDispatchRequest, HasSagaParticipantSupport, HasSagaWorkflowParticipants, InMemoryDedupe,
    InMemoryJournal, ParticipantEvent, ParticipantJournal, ParticipantTerminalKind, PeerId,
    SagaChoreographyEvent, SagaContext, SagaId, SagaParticipantSupport, SagaWorkflowParticipant,
    StepError, StepOutput,
};
use support::{FaultDedupe, FaultJournal, FaultTrigger};

type Journal = FaultJournal<InMemoryJournal>;
type Dedupe = FaultDedupe<InMemoryDedupe>;

#[derive(Clone, Copy)]
enum DispatchMode {
    Durable,
    Ambiguous,
}

struct Actor {
    saga: SagaParticipantSupport<Journal, Dedupe>,
    pay_calls: usize,
    comp_calls: usize,
    dispatched: Vec<(String, Vec<u8>)>,
    dispatch_mode: DispatchMode,
}

impl Actor {
    fn new(journal: &Journal, dedupe: &Dedupe) -> Self {
        Self {
            saga: SagaParticipantSupport::new(journal.clone(), dedupe.clone()),
            pay_calls: 0,
            comp_calls: 0,
            dispatched: Vec::new(),
            dispatch_mode: DispatchMode::Durable,
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

struct PayWorkflow;
struct EffectWorkflow;
struct PlainEffectWorkflow;

static PAY: PayWorkflow = PayWorkflow;
static EFFECT: EffectWorkflow = EffectWorkflow;
static PLAIN: PlainEffectWorkflow = PlainEffectWorkflow;
static WORKFLOWS: [&'static dyn SagaWorkflowParticipant<Actor>; 3] = [&PAY, &EFFECT, &PLAIN];

impl HasSagaWorkflowParticipants for Actor {
    fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
        &WORKFLOWS
    }
}

fn compensate_ok(actor: &mut Actor) -> Result<CompensationOutput, CompensationError> {
    actor.comp_calls += 1;
    Ok(CompensationOutput::Completed)
}

impl SagaWorkflowParticipant<Actor> for PayWorkflow {
    fn step_name(&self) -> &'static str {
        "pay"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_pay"]
    }
    fn execute_step(
        &self,
        actor: &mut Actor,
        _context: &SagaContext,
        _input: &[u8],
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
        _context: &SagaContext,
        _data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        compensate_ok(actor)
    }
}

fn effect_output() -> StepOutput {
    StepOutput::CompletedWithEffect {
        output: b"sent".to_vec(),
        compensation_data: b"unsend".to_vec(),
        effect: "email".into(),
    }
}

impl SagaWorkflowParticipant<Actor> for EffectWorkflow {
    fn step_name(&self) -> &'static str {
        "notify"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_effect"]
    }
    fn execute_step(
        &self,
        _actor: &mut Actor,
        _context: &SagaContext,
        _input: &[u8],
    ) -> Result<StepOutput, StepError> {
        Ok(effect_output())
    }
    fn compensate_step(
        &self,
        actor: &mut Actor,
        _context: &SagaContext,
        _data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        compensate_ok(actor)
    }
    fn dispatch_effect(
        &self,
        actor: &mut Actor,
        request: &EffectDispatchRequest<'_>,
    ) -> Result<EffectDispatchOutcome, EffectDispatchError> {
        actor
            .dispatched
            .push((request.effect.to_owned(), request.output.to_vec()));
        match actor.dispatch_mode {
            DispatchMode::Durable => Ok(EffectDispatchOutcome::Durable {
                receipt: "outbox-1".into(),
            }),
            DispatchMode::Ambiguous => Err(EffectDispatchError::Failed {
                reason: "timeout after send".into(),
                ambiguous: true,
            }),
        }
    }
}

impl SagaWorkflowParticipant<Actor> for PlainEffectWorkflow {
    fn step_name(&self) -> &'static str {
        "plain"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_plain"]
    }
    fn execute_step(
        &self,
        _actor: &mut Actor,
        _context: &SagaContext,
        _input: &[u8],
    ) -> Result<StepOutput, StepError> {
        Ok(effect_output())
    }
    fn compensate_step(
        &self,
        actor: &mut Actor,
        _context: &SagaContext,
        _data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        compensate_ok(actor)
    }
}

fn stores() -> (Journal, Dedupe) {
    (
        FaultJournal::new(InMemoryJournal::new()),
        FaultDedupe::new(InMemoryDedupe::new()),
    )
}

fn ctx(saga_type: &str, step: &str, started_at: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(41),
        saga_type: saga_type.into(),
        step_name: step.into(),
        correlation_id: 41,
        causation_id: 41,
        trace_id: 41,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: started_at,
        event_timestamp_millis: started_at,
    }
}

fn started(context: &SagaContext) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: context.clone(),
        payload: b"input".to_vec(),
    }
}

#[derive(Default)]
struct Delivery {
    valid: Vec<SagaChoreographyEvent>,
    invalid: Vec<SagaChoreographyEvent>,
}

impl Delivery {
    fn has_quarantine(&self) -> bool {
        self.valid
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::SagaQuarantined { .. }))
    }
    fn has_step_completed(&self) -> bool {
        self.valid
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::StepCompleted { .. }))
    }
}

fn deliver(actor: &mut Actor, event: SagaChoreographyEvent) -> Delivery {
    let valid = RefCell::new(Vec::new());
    let invalid = RefCell::new(Vec::new());
    apply_sync_workflow_participant_saga_ingress_with_hooks(
        actor,
        event,
        |_actor, _event| {},
        |event| invalid.borrow_mut().push(event.clone()),
        |_actor, event| valid.borrow_mut().push(event.clone()),
    );
    Delivery {
        valid: valid.into_inner(),
        invalid: invalid.into_inner(),
    }
}

#[test]
fn failed_intent_write_prevents_workflow_effect_and_quarantines_visibly() {
    let (journal, dedupe) = stores();
    journal.fail_once(
        support::JournalOp::Append,
        FaultTrigger::EventKind("StepExecutionStarted"),
    );
    let mut actor = Actor::new(&journal, &dedupe);
    let delivery = deliver(&mut actor, started(&ctx("wf_pay", "pay", 100)));

    assert_eq!(
        actor.pay_calls, 0,
        "no business effect without durable intent"
    );
    assert!(delivery.has_quarantine(), "{:?}", delivery.valid);
    assert!(!delivery.has_step_completed());
    assert!(delivery.invalid.is_empty(), "{:?}", delivery.invalid);
}

#[test]
fn failed_result_write_cannot_publish_workflow_success() {
    let (journal, dedupe) = stores();
    // Plain completion now persists its full result in the single proof append;
    // fault that authoritative write rather than a raw row which no longer exists.
    journal.fail_once(
        support::JournalOp::Append,
        FaultTrigger::EventKind("ParticipantForwardOutcomeRecorded"),
    );
    let mut actor = Actor::new(&journal, &dedupe);
    let delivery = deliver(&mut actor, started(&ctx("wf_pay", "pay", 100)));

    assert_eq!(actor.pay_calls, 1);
    assert!(
        !delivery.has_step_completed(),
        "success without durable evidence: {:?}",
        delivery.valid
    );
    assert!(delivery.has_quarantine(), "{:?}", delivery.valid);
    assert!(delivery.invalid.is_empty(), "{:?}", delivery.invalid);
    assert!(
        journal
            .read(SagaId::new(41))
            .unwrap()
            .iter()
            .any(|entry| matches!(&entry.event,
                ParticipantEvent::ParticipantReconciliationEvidence { compensation_data, .. }
                    if compensation_data == b"refund")),
        "the refund data must survive a failed result write as typed durable evidence"
    );
}

#[test]
fn a_non_owned_undo_request_does_not_consume_the_owned_successor_dedupe_key() {
    let (journal, dedupe) = stores();
    let mut actor = Actor::new(&journal, &dedupe);
    let c = ctx("wf_pay", "pay", 100);
    deliver(&mut actor, started(&c));
    let request_context = c.next_step("terminal_resolver".into());
    let request = |step: &str| SagaChoreographyEvent::CompensationRequested {
        context: request_context.clone(),
        failed_step: "failed".into(),
        reason: "rollback".into(),
        failure: icanact_saga_choreography::SagaFailureDetails {
            step_name: "failed".into(),
            participant_id: "p".into(),
            error_code: None,
            error_message: "rollback".into(),
            at_millis: 100,
        },
        steps_to_compensate: vec![step.into()],
    };
    deliver(&mut actor, request("other"));
    let mut legacy = request("other");
    if let SagaChoreographyEvent::CompensationRequested {
        steps_to_compensate,
        ..
    } = &mut legacy
    {
        steps_to_compensate.push("pay".into());
    }
    deliver(&mut actor, legacy);
    assert_eq!(
        actor.comp_calls, 0,
        "legacy non-head recipients must not undo concurrently"
    );
    let own = deliver(&mut actor, request("pay"));
    assert_eq!(
        actor.comp_calls, 1,
        "every singleton owner must be able to process its request"
    );
    assert!(
        own.valid
            .iter()
            .any(|event| matches!(event, SagaChoreographyEvent::CompensationCompleted { .. }))
    );
}

#[test]
fn dedupe_outage_quarantines_visibly_instead_of_being_dropped_by_the_validator() {
    let (journal, dedupe) = stores();
    dedupe.fail_check_and_mark(FaultTrigger::NthCall(1));
    let mut actor = Actor::new(&journal, &dedupe);
    let delivery = deliver(&mut actor, started(&ctx("wf_pay", "pay", 100)));

    assert_eq!(actor.pay_calls, 0);
    assert!(delivery.has_quarantine(), "{:?}", delivery);
    assert!(delivery.invalid.is_empty(), "{:?}", delivery.invalid);
}

#[test]
fn locally_emitted_quarantine_is_durable_without_bus_loopback() {
    let (journal, dedupe) = stores();
    dedupe.fail_check_and_mark(FaultTrigger::NthCall(1));
    let mut actor = Actor::new(&journal, &dedupe);
    let c = ctx("wf_pay", "pay", 100);
    assert!(deliver(&mut actor, started(&c)).has_quarantine());
    assert_eq!(
        icanact_saga_choreography::SagaStateExt::terminal_run_outcome_strict(&actor, &c).unwrap(),
        Some(ParticipantTerminalKind::Quarantined)
    );
    dedupe.disarm();
    let mut retry = c;
    retry.trace_id = 999;
    deliver(&mut actor, started(&retry));
    assert_eq!(
        actor.pay_calls, 0,
        "quarantine cannot depend on receiving its own publication"
    );
}

#[test]
fn changed_delivery_trace_cannot_reexecute_a_durable_workflow_step() {
    let (journal, dedupe) = stores();
    let mut actor = Actor::new(&journal, &dedupe);
    let c = ctx("wf_pay", "pay", 100);
    deliver(&mut actor, started(&c));
    let mut retry = c;
    retry.trace_id = 999;
    retry.attempt = 1;
    let out = deliver(&mut actor, started(&retry));
    assert_eq!(
        actor.pay_calls, 1,
        "delivery identity cannot reopen durable execution"
    );
    assert!(out.has_quarantine());
}

#[test]
fn journal_read_outage_during_admission_fails_closed_and_visibly() {
    let (journal, dedupe) = stores();
    journal.fail_once(support::JournalOp::Read, FaultTrigger::NthCall(1));
    let mut actor = Actor::new(&journal, &dedupe);
    let delivery = deliver(&mut actor, started(&ctx("wf_pay", "pay", 100)));

    assert_eq!(actor.pay_calls, 0);
    assert!(delivery.has_quarantine(), "{:?}", delivery);
    assert!(delivery.invalid.is_empty());
}

#[test]
fn completed_effect_is_dispatched_with_its_identifier_before_success() {
    let (journal, dedupe) = stores();
    let mut actor = Actor::new(&journal, &dedupe);
    let delivery = deliver(&mut actor, started(&ctx("wf_effect", "notify", 100)));

    assert_eq!(
        actor.dispatched,
        vec![("email".to_owned(), b"sent".to_vec())]
    );
    assert!(delivery.has_step_completed(), "{:?}", delivery.valid);
}

#[test]
fn ambiguous_dispatch_failure_quarantines_and_never_reports_success() {
    let (journal, dedupe) = stores();
    let mut actor = Actor::new(&journal, &dedupe);
    actor.dispatch_mode = DispatchMode::Ambiguous;
    let delivery = deliver(&mut actor, started(&ctx("wf_effect", "notify", 100)));

    assert_eq!(actor.dispatched.len(), 1);
    assert!(!delivery.has_step_completed(), "{:?}", delivery.valid);
    assert!(delivery.has_quarantine(), "{:?}", delivery.valid);
}

#[test]
fn default_effect_dispatch_fails_closed() {
    let (journal, dedupe) = stores();
    let mut actor = Actor::new(&journal, &dedupe);
    let delivery = deliver(&mut actor, started(&ctx("wf_plain", "plain", 100)));

    assert!(!delivery.has_step_completed(), "{:?}", delivery.valid);
    assert!(delivery.has_quarantine(), "{:?}", delivery.valid);
}

#[test]
fn completed_run_does_not_repeat_effect_after_restart_and_later_run_is_admitted() {
    let (journal, dedupe) = stores();
    let run = ctx("wf_pay", "pay", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    assert!(deliver(&mut actor, started(&run)).has_step_completed());
    deliver(
        &mut actor,
        SagaChoreographyEvent::SagaCompleted {
            context: run.clone(),
        },
    );
    assert_eq!(actor.pay_calls, 1);

    // Restart: empty caches over the same durable stores.
    let mut restarted = Actor::new(&journal, &dedupe);
    let replay = deliver(&mut restarted, started(&run));
    assert_eq!(restarted.pay_calls, 0, "terminal run must stay fenced");
    assert!(replay.valid.is_empty() && replay.invalid.is_empty());

    // A later valid run of the same saga id executes; the old run is then stale.
    let later = ctx("wf_pay", "pay", 200);
    assert!(deliver(&mut restarted, started(&later)).has_step_completed());
    assert_eq!(restarted.pay_calls, 1);
    deliver(&mut restarted, started(&run));
    assert_eq!(restarted.pay_calls, 1, "older run must not be re-admitted");
}

#[test]
fn quarantined_run_retains_evidence_and_is_not_auto_resolved_or_reused() {
    let (journal, dedupe) = stores();
    let run = ctx("wf_pay", "pay", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    assert!(deliver(&mut actor, started(&run)).has_step_completed());
    deliver(
        &mut actor,
        SagaChoreographyEvent::SagaQuarantined {
            context: run.clone(),
            reason: "operator review".into(),
            step: "pay".into(),
            participant_id: "pay".into(),
        },
    );

    let entries = journal.read(run.saga_id).unwrap();
    assert!(
        entries.iter().any(|e| matches!(
            &e.event,
            ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome }
                if outcome.compensation_data == b"refund"
        )),
        "the single plain proof must retain compensation evidence: {entries:?}"
    );
    assert!(entries.iter().any(|e| matches!(
        &e.event,
        ParticipantEvent::ParticipantTerminalRecorded {
            outcome: ParticipantTerminalKind::Quarantined,
            ..
        }
    )));

    // Restart does not auto-resolve it and the id cannot be reused by a new run.
    assert!(
        collect_startup_recovery_events_for_saga_type(&journal, &dedupe, "pay", "wf_pay")
            .unwrap()
            .is_empty()
    );
    let mut restarted = Actor::new(&journal, &dedupe);
    let reuse = deliver(&mut restarted, started(&ctx("wf_pay", "pay", 200)));
    assert_eq!(restarted.pay_calls, 0);
    assert!(
        reuse.valid.is_empty() && reuse.invalid.is_empty(),
        "refusal must not manufacture uncertainty in the incoming run: {reuse:?}"
    );
    let after = journal.read(run.saga_id).unwrap();
    assert!(after.len() >= entries.len(), "evidence must never shrink");
    assert!(
        after.iter().any(|entry| matches!(
            &entry.event,
            ParticipantEvent::ParticipantTerminalRecorded {
                outcome: ParticipantTerminalKind::Quarantined,
                saga_started_at_millis: 100,
                ..
            }
        )),
        "original quarantine must remain authoritative"
    );
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "valid={:?} invalid={:?}", self.valid, self.invalid)
    }
}
