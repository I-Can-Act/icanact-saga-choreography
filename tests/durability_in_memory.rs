//! U6 participant recovery over durable journals (in-memory stores): current-run
//! segregation, fence hydration, quarantine preservation, singleton compensation
//! requests and accepted-step ingress/recovery parity.

// Each test crate uses only a subset of the shared fixture re-exports.
#[allow(unused_imports)]
mod support;

use std::cell::RefCell;
use std::time::Duration;

use icanact_saga_choreography::ParticipantDedupeStore;
use icanact_saga_choreography::durability::{
    apply_sync_workflow_participant_saga_ingress_with_hooks,
    collect_startup_recovery_events_for_saga_type, recover_accepted_workflow_steps_for_saga_type,
};
use icanact_saga_choreography::{
    AcceptedStepPolicy, AcceptedStepTimeoutOutcome, CompensationError, CompensationOutput,
    HasSagaParticipantSupport, HasSagaWorkflowParticipants, InMemoryDedupe, InMemoryJournal,
    ParticipantEvent, ParticipantJournal, ParticipantTerminalKind, PeerId, SagaChoreographyEvent,
    SagaContext, SagaFailureDetails, SagaId, SagaParticipantSupport, SagaStateExt,
    SagaWorkflowParticipant, StepError, StepExecutionId, StepOutput,
};
use support::{FaultDedupe, FaultJournal};

type Journal = FaultJournal<InMemoryJournal>;
type Dedupe = FaultDedupe<InMemoryDedupe>;

struct Actor {
    saga: SagaParticipantSupport<Journal, Dedupe>,
    comp_calls: usize,
    pay_calls: usize,
}

impl Actor {
    fn new(journal: &Journal, dedupe: &Dedupe) -> Self {
        Self {
            saga: SagaParticipantSupport::new(journal.clone(), dedupe.clone()),
            comp_calls: 0,
            pay_calls: 0,
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
struct AcceptingWorkflow;
static PAY: PayWorkflow = PayWorkflow;
static ACCEPTING: AcceptingWorkflow = AcceptingWorkflow;
static WORKFLOWS: [&'static dyn SagaWorkflowParticipant<Actor>; 2] = [&PAY, &ACCEPTING];

impl HasSagaWorkflowParticipants for Actor {
    fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
        &WORKFLOWS
    }
}

struct OldJournal(Vec<icanact_saga_choreography::JournalEntry>);
impl ParticipantJournal for OldJournal {
    fn append(
        &self,
        _: SagaId,
        _: ParticipantEvent,
    ) -> Result<u64, icanact_saga_choreography::JournalError> {
        unreachable!("read-only recovery fixture")
    }
    fn read(
        &self,
        _: SagaId,
    ) -> Result<Vec<icanact_saga_choreography::JournalEntry>, icanact_saga_choreography::JournalError>
    {
        Ok(self.0.clone())
    }
    fn list_sagas(&self) -> Result<Vec<SagaId>, icanact_saga_choreography::JournalError> {
        Ok(vec![SagaId::new(909)])
    }
    fn prune(&self, _: SagaId) -> Result<(), icanact_saga_choreography::JournalError> {
        unreachable!("no recovery pruning")
    }
}

#[test]
fn stale_generic_execution_or_undo_recovers_as_quarantine_with_original_identity() {
    for event in [
        ParticipantEvent::StepExecutionStarted {
            attempt: 1,
            started_at_millis: 20,
        },
        ParticipantEvent::CompensationStarted {
            attempt: 1,
            started_at_millis: 20,
        },
    ] {
        let journal = OldJournal(vec![
            icanact_saga_choreography::JournalEntry {
                sequence: 1,
                recorded_at_millis: 10,
                event: ParticipantEvent::ParticipantRunRecorded {
                    saga_type: "wf_pay".into(),
                    saga_started_at_millis: 10,
                    recorded_at_millis: 10,
                },
            },
            icanact_saga_choreography::JournalEntry {
                sequence: 2,
                recorded_at_millis: 20,
                event,
            },
        ]);
        let events = collect_startup_recovery_events_for_saga_type(
            &journal,
            &InMemoryDedupe::new(),
            "pay",
            "wf_pay",
        )
        .unwrap();
        assert!(
            matches!(events.as_slice(), [SagaChoreographyEvent::SagaQuarantined { context, .. }]
            if context.saga_started_at_millis == 10),
            "stale intent is uncertain, not an ordinary failure: {events:?}"
        );
    }
}

#[test]
fn stale_accepted_timeout_cannot_resolve_a_new_run_with_the_same_execution_id() {
    let journal = Journal::new(InMemoryJournal::new());
    let dedupe = Dedupe::new(InMemoryDedupe::new());
    let c = ctx("wf_accept", "reserve", 300);
    append(&journal, c.saga_id, run_recorded("wf_accept", 300));
    let mut actor = Actor::new(&journal, &dedupe);
    icanact_saga_choreography::durability::accept_workflow_step(
        &mut actor,
        c.clone(),
        "reserve".into(),
        StepExecutionId::new("ext-77"),
        policy(),
        vec![],
        b"release".to_vec(),
    )
    .unwrap();
    deliver(
        &mut actor,
        SagaChoreographyEvent::step_failed_for_participant(
            ctx("wf_accept", "reserve", 100),
            "reserve".into(),
            Some("idle".into()),
            "accepted step reserve idle timeout".into(),
            false,
        ),
    );
    assert!(
        actor.saga.accepted_workflow_steps.contains_key(&c.saga_id),
        "stale timeout must not remove the later run's accepted owner"
    );
}

fn comp(actor: &mut Actor) -> Result<CompensationOutput, CompensationError> {
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
        comp(actor)
    }
}

fn policy() -> AcceptedStepPolicy {
    AcceptedStepPolicy {
        idle_timeout: Duration::from_secs(3600),
        hard_timeout: Duration::from_secs(7200),
        timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: true,
        },
    }
}

impl SagaWorkflowParticipant<Actor> for AcceptingWorkflow {
    fn step_name(&self) -> &'static str {
        "reserve"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["wf_accept"]
    }
    fn execute_step(
        &self,
        _actor: &mut Actor,
        _context: &SagaContext,
        _input: &[u8],
    ) -> Result<StepOutput, StepError> {
        Ok(StepOutput::Accepted {
            execution_id: StepExecutionId::new("ext-77"),
            policy: policy(),
            compensation_data: b"release".to_vec(),
        })
    }
    fn compensate_step(
        &self,
        actor: &mut Actor,
        _context: &SagaContext,
        _data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        comp(actor)
    }
}

fn ctx(saga_type: &str, step: &str, started_at: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(52),
        saga_type: saga_type.into(),
        step_name: step.into(),
        correlation_id: 52,
        causation_id: 52,
        trace_id: 52,
        step_index: 1,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: started_at,
        event_timestamp_millis: started_at,
    }
}

fn deliver(actor: &mut Actor, event: SagaChoreographyEvent) -> Vec<SagaChoreographyEvent> {
    let out = RefCell::new(Vec::new());
    apply_sync_workflow_participant_saga_ingress_with_hooks(
        actor,
        event,
        |_actor, _event| {},
        |event| panic!("valid transition rejected: {event:?}"),
        |_actor, event| out.borrow_mut().push(event.clone()),
    );
    out.into_inner()
}

fn append(journal: &Journal, saga_id: SagaId, event: ParticipantEvent) {
    journal.append(saga_id, event).expect("append");
}

fn run_recorded(saga_type: &str, started: u64) -> ParticipantEvent {
    ParticipantEvent::ParticipantRunRecorded {
        saga_type: saga_type.into(),
        saga_started_at_millis: started,
        recorded_at_millis: started,
    }
}

fn terminal(saga_type: &str, started: u64, outcome: ParticipantTerminalKind) -> ParticipantEvent {
    ParticipantEvent::ParticipantTerminalRecorded {
        saga_type: saga_type.into(),
        saga_started_at_millis: started,
        outcome,
        reason: "test".into(),
        recorded_at_millis: started + 1,
    }
}

fn far_future() -> u64 {
    SagaContext::now_millis() + 3_600_000
}

fn accepted_step(context: &SagaContext, expired: bool) -> ParticipantEvent {
    let deadline = if expired { 1 } else { far_future() };
    ParticipantEvent::AcceptedStepRecorded {
        context: context.clone(),
        participant_id: "reserve".into(),
        execution_id: StepExecutionId::new("ext-old"),
        idle_timeout_millis: 1000,
        hard_timeout_millis: 2000,
        timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: true,
        },
        saga_input: b"in".to_vec(),
        compensation_data: b"release".to_vec(),
        accepted_at_millis: 1,
        deadline_at_millis: deadline,
        hard_deadline_at_millis: deadline,
    }
}

fn started_event() -> ParticipantEvent {
    ParticipantEvent::StepExecutionStarted {
        attempt: 1,
        started_at_millis: 1,
    }
}

#[test]
fn recovery_does_not_fold_a_resolved_old_run_into_the_current_run() {
    let journal = FaultJournal::new(InMemoryJournal::new());
    let dedupe = FaultDedupe::new(InMemoryDedupe::new());
    let old = ctx("wf_accept", "reserve", 100);
    let id = old.saga_id;
    append(&journal, id, run_recorded("wf_accept", 100));
    append(&journal, id, started_event());
    append(&journal, id, accepted_step(&old, false));
    append(
        &journal,
        id,
        terminal("wf_accept", 100, ParticipantTerminalKind::Completed),
    );
    append(&journal, id, run_recorded("wf_accept", 200));
    append(&journal, id, started_event());

    // The new run holds an open forward intent with unknown outcome, which is unsafe
    // at any age and must be quarantined visibly. The resolved old run's accepted step
    // must neither be replayed nor attributed to the new run.
    let events =
        collect_startup_recovery_events_for_saga_type(&journal, &dedupe, "reserve", "wf_accept")
            .unwrap();
    assert!(
        matches!(events.as_slice(),
            [SagaChoreographyEvent::SagaQuarantined { context, .. }]
                if context.saga_started_at_millis == 200),
        "old run replayed into new run, or the new run's open intent was not quarantined: {events:?}"
    );

    let mut support = SagaParticipantSupport::new(journal.clone(), dedupe.clone());
    recover_accepted_workflow_steps_for_saga_type(&mut support, "reserve", "wf_accept").unwrap();
    assert!(
        support.accepted_workflow_steps.is_empty(),
        "old accepted metadata hydrated into the current run"
    );
    assert!(support.saga_states.is_empty());
}

#[test]
fn recovery_hydrates_terminal_fence_for_the_current_run() {
    let journal = FaultJournal::new(InMemoryJournal::new());
    let id = ctx("wf_pay", "pay", 100).saga_id;
    append(&journal, id, run_recorded("wf_pay", 100));
    append(&journal, id, started_event());
    append(
        &journal,
        id,
        terminal("wf_pay", 100, ParticipantTerminalKind::Completed),
    );

    let mut support = SagaParticipantSupport::new(journal, FaultDedupe::new(InMemoryDedupe::new()));
    recover_accepted_workflow_steps_for_saga_type(&mut support, "pay", "wf_pay").unwrap();
    assert!(
        support.terminal_sagas.contains(&id),
        "terminal fence not hydrated"
    );
    assert_eq!(support.saga_run_started_at.get(&id), Some(&100));
}

#[test]
fn restart_never_auto_resolves_quarantined_accepted_work() {
    let journal = FaultJournal::new(InMemoryJournal::new());
    let dedupe = FaultDedupe::new(InMemoryDedupe::new());
    let run = ctx("wf_accept", "reserve", 100);
    let id = run.saga_id;
    append(&journal, id, run_recorded("wf_accept", 100));
    append(&journal, id, started_event());
    // Expired accepted step that would otherwise be failed and compensated.
    append(&journal, id, accepted_step(&run, true));
    append(
        &journal,
        id,
        terminal("wf_accept", 100, ParticipantTerminalKind::Quarantined),
    );
    let before = journal.read(id).unwrap().len();

    let events =
        collect_startup_recovery_events_for_saga_type(&journal, &dedupe, "reserve", "wf_accept")
            .unwrap();
    assert!(
        events.is_empty(),
        "quarantined work auto-resolved: {events:?}"
    );
    let mut support = SagaParticipantSupport::new(journal.clone(), dedupe);
    recover_accepted_workflow_steps_for_saga_type(&mut support, "reserve", "wf_accept").unwrap();
    assert!(support.accepted_workflow_steps.is_empty());
    assert_eq!(journal.read(id).unwrap().len(), before, "evidence mutated");
}

#[test]
fn singleton_unstarted_compensation_request_is_replayed_and_executes_once() {
    let journal = FaultJournal::new(InMemoryJournal::new());
    let dedupe = FaultDedupe::new(InMemoryDedupe::new());
    let run = ctx("wf_pay", "pay", 100);
    let id = run.saga_id;
    let request_ctx = ctx("wf_pay", "coordinator", 100);
    append(&journal, id, run_recorded("wf_pay", 100));
    append(&journal, id, started_event());
    append(
        &journal,
        id,
        ParticipantEvent::StepExecutionCompleted {
            output: b"paid".to_vec(),
            compensation_data: b"refund".to_vec(),
            completed_at_millis: 2,
        },
    );
    append(
        &journal,
        id,
        ParticipantEvent::CompensationRequestRecorded {
            context: request_ctx.clone(),
            failed_step: "ship".into(),
            reason: "ship failed".into(),
            failure: SagaFailureDetails {
                step_name: "ship".into(),
                participant_id: "ship".into(),
                error_code: None,
                error_message: "ship failed".into(),
                at_millis: 3,
            },
            steps_to_compensate: vec!["pay".into()],
            requested_at_millis: 3,
        },
    );
    // The request was already dedupe-marked before the crash.
    let probe = Actor::new(&journal, &dedupe);
    let key = probe.run_dedupe_key(
        &request_ctx,
        &format!(
            "{}:{}:compensation_requested:{}:ship",
            request_ctx.trace_id, request_ctx.saga_started_at_millis, request_ctx.step_name
        ),
    );
    dedupe.mark_processed(id, &key).unwrap();

    let events =
        collect_startup_recovery_events_for_saga_type(&journal, &dedupe, "pay", "wf_pay").unwrap();
    let [
        request @ SagaChoreographyEvent::CompensationRequested {
            steps_to_compensate,
            ..
        },
    ] = events.as_slice()
    else {
        panic!("expected exactly one singleton compensation request: {events:?}");
    };
    assert_eq!(steps_to_compensate.len(), 1);

    let mut restarted = Actor::new(&journal, &dedupe);
    recover_accepted_workflow_steps_for_saga_type(&mut restarted.saga, "pay", "wf_pay").unwrap();
    let emitted = deliver(&mut restarted, request.clone());
    assert_eq!(restarted.comp_calls, 1);
    assert!(
        emitted
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::CompensationCompleted { .. })),
        "{emitted:?}"
    );
    // Redelivery after completion does not compensate again.
    deliver(&mut restarted, request.clone());
    assert_eq!(restarted.comp_calls, 1);
}

#[test]
fn accepted_step_recovery_matches_what_ingress_emitted() {
    let journal = FaultJournal::new(InMemoryJournal::new());
    let dedupe = FaultDedupe::new(InMemoryDedupe::new());
    let run = ctx("wf_accept", "reserve", 100);
    let mut actor = Actor::new(&journal, &dedupe);
    let emitted = deliver(
        &mut actor,
        SagaChoreographyEvent::SagaStarted {
            context: run.clone(),
            payload: b"in".to_vec(),
        },
    );
    let Some(SagaChoreographyEvent::StepAccepted {
        execution_id,
        deadline_at_millis,
        hard_deadline_at_millis,
        compensation_available,
        ..
    }) = emitted
        .iter()
        .find(|e| matches!(e, SagaChoreographyEvent::StepAccepted { .. }))
    else {
        panic!("ingress must emit StepAccepted: {emitted:?}");
    };

    let recovered =
        collect_startup_recovery_events_for_saga_type(&journal, &dedupe, "reserve", "wf_accept")
            .unwrap();
    let [
        SagaChoreographyEvent::StepAccepted {
            execution_id: r_exec,
            deadline_at_millis: r_deadline,
            hard_deadline_at_millis: r_hard,
            compensation_available: r_comp,
            ..
        },
    ] = recovered.as_slice()
    else {
        panic!("expected one recovered StepAccepted: {recovered:?}");
    };
    assert_eq!(r_exec, execution_id);
    assert_eq!(r_deadline, deadline_at_millis);
    assert_eq!(r_hard, hard_deadline_at_millis);
    assert_eq!(r_comp, compensation_available);
}
