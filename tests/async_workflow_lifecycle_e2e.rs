use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use icanact_saga_choreography::durability::{
    apply_sync_workflow_participant_saga_ingress_with_hooks,
    collect_startup_recovery_events_for_saga_type, complete_accepted_workflow_compensation,
    fail_accepted_workflow_compensation,
};
use icanact_saga_choreography::{
    AcceptedCompensationCompletion, AcceptedCompensationFailure, AcceptedStepCompletion,
    AcceptedStepError, AcceptedStepFailure, AcceptedStepPolicy, AcceptedStepTimeoutOutcome,
    AcceptedWorkflowCompensation, CompensationError, CompensationOutput, DependencySpec,
    FailureAuthority, HasSagaParticipantSupport, HasSagaWorkflowParticipants, InMemoryDedupe,
    InMemoryJournal, ParticipantDedupeStore, ParticipantEvent, ParticipantJournal,
    SagaChoreographyEvent, SagaContext, SagaId, SagaParticipant, SagaParticipantState,
    SagaParticipantSupport, SagaStateEntry, SagaStateExt, SagaTerminalOutcome, SagaTestWorld,
    SagaWorkflowParticipant, StepError, StepExecutionId, StepOutput, SuccessCriteria,
    TerminalPolicy, TerminalResolver, accept_workflow_step as accept_workflow_step_with_state,
    complete_accepted_workflow_step, fail_accepted_workflow_step, handle_saga_event_with_emit,
    poll_accepted_workflow_step_timeouts, record_accepted_workflow_step_progress,
    recover_accepted_workflow_steps_for_saga_type,
};

struct HarnessActor {
    saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
}

impl Default for HarnessActor {
    fn default() -> Self {
        Self {
            saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
        }
    }
}

impl HasSagaParticipantSupport for HarnessActor {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

struct DeferredWorkflowActor {
    saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
    emitted: Vec<SagaChoreographyEvent>,
    compensation_completed_hooks: usize,
}

impl Default for DeferredWorkflowActor {
    fn default() -> Self {
        Self {
            saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
            emitted: Vec::new(),
            compensation_completed_hooks: 0,
        }
    }
}

impl HasSagaParticipantSupport for DeferredWorkflowActor {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

struct DeferredOrderWorkflow;

static DEFERRED_ORDER_WORKFLOW: DeferredOrderWorkflow = DeferredOrderWorkflow;
static DEFERRED_ORDER_WORKFLOWS: [&'static dyn SagaWorkflowParticipant<DeferredWorkflowActor>; 1] =
    [&DEFERRED_ORDER_WORKFLOW];

impl SagaWorkflowParticipant<DeferredWorkflowActor> for DeferredOrderWorkflow {
    fn step_name(&self) -> &'static str {
        "create_order"
    }

    fn saga_types(&self) -> &[&'static str] {
        &["order_lifecycle"]
    }

    fn depends_on(&self) -> DependencySpec {
        DependencySpec::OnSagaStart
    }

    fn execute_step(
        &self,
        _actor: &mut DeferredWorkflowActor,
        _context: &SagaContext,
        _input: &[u8],
    ) -> Result<StepOutput, StepError> {
        Ok(StepOutput::Accepted {
            execution_id: StepExecutionId::new("order-request-36"),
            policy: AcceptedStepPolicy {
                idle_timeout: Duration::from_millis(1),
                hard_timeout: Duration::from_millis(1),
                timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                    requires_compensation: true,
                },
            },
            compensation_data: b"release-order-request-36".to_vec(),
        })
    }

    fn compensate_step(
        &self,
        _actor: &mut DeferredWorkflowActor,
        _context: &SagaContext,
        compensation_data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        assert_eq!(compensation_data, b"release-order-request-36");
        Ok(CompensationOutput::Accepted {
            execution_id: StepExecutionId::new("cancel-order-request-36"),
            policy: AcceptedStepPolicy {
                idle_timeout: Duration::from_secs(1),
                hard_timeout: Duration::from_secs(1),
                timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
            },
        })
    }

    fn on_compensation_completed(&self, actor: &mut DeferredWorkflowActor, _context: &SagaContext) {
        actor.compensation_completed_hooks += 1;
    }
}

impl HasSagaWorkflowParticipants for DeferredWorkflowActor {
    fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
        &DEFERRED_ORDER_WORKFLOWS
    }
}

impl HasSagaWorkflowParticipants for HarnessActor {
    fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
        &[]
    }
}

struct FailOnAppendJournal {
    inner: InMemoryJournal,
    fail_on_append: usize,
    appends: AtomicUsize,
}

impl FailOnAppendJournal {
    fn new(fail_on_append: usize) -> Self {
        Self {
            inner: InMemoryJournal::new(),
            fail_on_append,
            appends: AtomicUsize::new(0),
        }
    }
}

impl ParticipantJournal for FailOnAppendJournal {
    fn append(
        &self,
        saga_id: SagaId,
        event: ParticipantEvent,
    ) -> Result<u64, icanact_saga_choreography::JournalError> {
        let append_number = self.appends.fetch_add(1, Ordering::SeqCst) + 1;
        if append_number == self.fail_on_append {
            return Err(icanact_saga_choreography::JournalError::Storage(
                format!("forced append failure {append_number}").into(),
            ));
        }
        self.inner.append(saga_id, event)
    }

    fn read(
        &self,
        saga_id: SagaId,
    ) -> Result<Vec<icanact_saga_choreography::JournalEntry>, icanact_saga_choreography::JournalError>
    {
        self.inner.read(saga_id)
    }

    fn list_sagas(&self) -> Result<Vec<SagaId>, icanact_saga_choreography::JournalError> {
        self.inner.list_sagas()
    }

    fn prune(&self, saga_id: SagaId) -> Result<(), icanact_saga_choreography::JournalError> {
        self.inner.prune(saga_id)
    }
}

struct FailingJournalActor {
    saga: SagaParticipantSupport<FailOnAppendJournal, InMemoryDedupe>,
}

impl FailingJournalActor {
    fn fail_on_append(fail_on_append: usize) -> Self {
        Self {
            saga: SagaParticipantSupport::new(
                FailOnAppendJournal::new(fail_on_append),
                InMemoryDedupe::new(),
            ),
        }
    }
}

impl HasSagaParticipantSupport for FailingJournalActor {
    type Journal = FailOnAppendJournal;
    type Dedupe = InMemoryDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

impl HasSagaWorkflowParticipants for FailingJournalActor {
    fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
        &[]
    }
}

impl SagaParticipant for FailingJournalActor {
    type Error = String;

    fn step_name(&self) -> &str {
        "create_order"
    }

    fn saga_types(&self) -> &[&'static str] {
        &["order_lifecycle"]
    }

    fn depends_on(&self) -> DependencySpec {
        DependencySpec::OnSagaStart
    }

    fn execute_step(
        &mut self,
        _context: &SagaContext,
        _input: &[u8],
    ) -> Result<StepOutput, StepError> {
        Ok(StepOutput::Accepted {
            execution_id: StepExecutionId::new("external-create"),
            policy: policy(AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: true,
            }),
            compensation_data: b"cancel-external-create".to_vec(),
        })
    }

    fn compensate_step(
        &mut self,
        _context: &SagaContext,
        _compensation_data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        Ok(CompensationOutput::Completed)
    }
}

struct RestartedHarnessActor {
    saga: SagaParticipantSupport<Arc<InMemoryJournal>, InMemoryDedupe>,
}

struct RestartedHarnessWorkflow;

static RESTARTED_HARNESS_WORKFLOW: RestartedHarnessWorkflow = RestartedHarnessWorkflow;
static RESTARTED_HARNESS_WORKFLOWS: [&'static dyn SagaWorkflowParticipant<RestartedHarnessActor>;
    1] = [&RESTARTED_HARNESS_WORKFLOW];

impl SagaWorkflowParticipant<RestartedHarnessActor> for RestartedHarnessWorkflow {
    fn step_name(&self) -> &'static str {
        "create_order"
    }

    fn saga_types(&self) -> &[&'static str] {
        &["order_lifecycle"]
    }

    fn execute_step(
        &self,
        _actor: &mut RestartedHarnessActor,
        _context: &SagaContext,
        _input: &[u8],
    ) -> Result<StepOutput, StepError> {
        unreachable!("restart harness only resolves persisted work")
    }

    fn compensate_step(
        &self,
        _actor: &mut RestartedHarnessActor,
        _context: &SagaContext,
        _compensation_data: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        unreachable!("restart harness only resolves persisted work")
    }
}

impl HasSagaWorkflowParticipants for RestartedHarnessActor {
    fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
        &RESTARTED_HARNESS_WORKFLOWS
    }
}

impl RestartedHarnessActor {
    fn new(journal: Arc<InMemoryJournal>) -> Self {
        Self {
            saga: SagaParticipantSupport::new(journal, InMemoryDedupe::new()),
        }
    }
}

impl HasSagaParticipantSupport for RestartedHarnessActor {
    type Journal = Arc<InMemoryJournal>;
    type Dedupe = InMemoryDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

fn context(step_name: &str, saga_id: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(saga_id),
        saga_type: "order_lifecycle".into(),
        step_name: step_name.into(),
        correlation_id: saga_id,
        causation_id: saga_id,
        trace_id: saga_id,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: [0; 32],
        saga_started_at_millis: 1_700_000_000_000,
        event_timestamp_millis: 1_700_000_000_000,
    }
}

fn policy(timeout_outcome: AcceptedStepTimeoutOutcome) -> AcceptedStepPolicy {
    AcceptedStepPolicy {
        idle_timeout: Duration::from_millis(100),
        hard_timeout: Duration::from_millis(250),
        timeout_outcome,
    }
}

fn accept_workflow_step<A>(
    actor: &mut A,
    context: SagaContext,
    participant_id: Box<str>,
    execution_id: StepExecutionId,
    policy: AcceptedStepPolicy,
) -> Result<SagaChoreographyEvent, AcceptedStepError>
where
    A: SagaStateExt,
{
    accept_workflow_step_with_state(
        actor,
        context,
        participant_id,
        execution_id,
        policy,
        b"original-saga-input".to_vec(),
        Vec::new(),
    )
}

fn completion(
    completed_at_millis: u64,
    output: impl Into<Vec<u8>>,
    _saga_input: impl Into<Vec<u8>>,
    compensation_data: impl Into<Vec<u8>>,
) -> AcceptedStepCompletion {
    AcceptedStepCompletion {
        completed_at_millis,
        output: output.into(),
        compensation_data: compensation_data.into(),
    }
}

fn failure(
    failed_at_millis: u64,
    reason: impl Into<Box<str>>,
    requires_compensation: bool,
) -> AcceptedStepFailure {
    AcceptedStepFailure {
        failed_at_millis,
        reason: reason.into(),
        requires_compensation,
    }
}

fn terminal_policy() -> TerminalPolicy {
    let mut required = std::collections::HashSet::new();
    required.insert("create_order".into());
    TerminalPolicy::new(
        "order_lifecycle".into(),
        "order_lifecycle/default".into(),
        FailureAuthority::AnyParticipant,
        SuccessCriteria::AllOf(required),
        Duration::from_secs(30),
        Duration::from_secs(10),
        &[],
    )
}

#[test]
fn accepted_step_rejects_idle_timeout_after_hard_timeout() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 0);
    let result = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        StepExecutionId::new("effect-invalid-policy"),
        AcceptedStepPolicy {
            idle_timeout: Duration::from_millis(300),
            hard_timeout: Duration::from_millis(100),
            timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: false,
            },
        },
    );

    assert!(matches!(
        result,
        Err(AcceptedStepError::InvalidPolicy { .. })
    ));
    assert!(
        actor
            .saga_support()
            .journal
            .read(ctx.saga_id)
            .expect("journal read should succeed")
            .is_empty()
    );
}

#[test]
fn accepted_step_rejects_zero_duration_timeouts() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 0);
    let zero_idle = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        StepExecutionId::new("effect-zero-idle"),
        AcceptedStepPolicy {
            idle_timeout: Duration::ZERO,
            hard_timeout: Duration::from_millis(100),
            timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: false,
            },
        },
    );
    assert!(matches!(
        zero_idle,
        Err(AcceptedStepError::InvalidPolicy { .. })
    ));

    let zero_hard = accept_workflow_step(
        &mut actor,
        ctx,
        "order-manager".into(),
        StepExecutionId::new("effect-zero-hard"),
        AcceptedStepPolicy {
            idle_timeout: Duration::from_millis(100),
            hard_timeout: Duration::ZERO,
            timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: false,
            },
        },
    );
    assert!(matches!(
        zero_hard,
        Err(AcceptedStepError::InvalidPolicy { .. })
    ));
}

#[test]
fn accepted_step_does_not_complete_saga_until_late_completion() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 1);
    let mut resolver = TerminalResolver::new(terminal_policy());
    let execution_id = StepExecutionId::new("effect-1");

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");

    assert!(matches!(
        accepted,
        SagaChoreographyEvent::StepAccepted {
            ref context,
            ref execution_id,
            ..
        } if context.step_name.as_ref() == "create_order"
            && execution_id.as_ref() == "effect-1"
    ));
    assert!(
        resolver.ingest(&accepted).is_empty(),
        "accepted step is progress, not completion"
    );

    let completed = complete_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id,
        completion(
            1_700_000_000_050,
            b"created",
            b"ignored-completion-input",
            Vec::new(),
        ),
    )
    .expect("authoritative completion should resolve the accepted step");
    assert!(matches!(
        &completed,
        SagaChoreographyEvent::StepCompleted { saga_input, .. }
            if saga_input.as_slice() == b"original-saga-input"
    ));
    assert!(matches!(
        resolver.ingest(&completed).as_slice(),
        [SagaChoreographyEvent::SagaCompleted { .. }]
    ));
}

#[test]
fn accepted_order_timeout_compensates_the_current_step_before_terminal_failure() {
    let ctx = context("create_order", 35);
    let mut actor = HarnessActor::default();
    let mut resolver = TerminalResolver::new(terminal_policy());
    let execution_id = StepExecutionId::new("order-request-35");
    let accepted = accept_workflow_step_with_state(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        AcceptedStepPolicy {
            idle_timeout: Duration::from_millis(1),
            hard_timeout: Duration::from_millis(1),
            timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: true,
            },
        },
        Vec::new(),
        b"order-request-35".to_vec(),
    )
    .expect("order step should be accepted");

    assert!(resolver.ingest(&accepted).is_empty());
    std::thread::sleep(Duration::from_millis(3));
    let timeout_events = resolver.poll_timeouts();

    assert!(
        matches!(
            timeout_events.as_slice(),
            [SagaChoreographyEvent::CompensationRequested {
                failed_step,
                steps_to_compensate,
                ..
            }] if failed_step.as_ref() == "create_order"
                && steps_to_compensate.as_slice() == [Box::<str>::from("create_order")]
        ),
        "an accepted order step owns possible exchange effects and must compensate itself before the saga can fail: {timeout_events:?}"
    );

    let late_completion = complete_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id,
        completion(
            SagaContext::now_millis(),
            b"late-created-order",
            b"ignored-completion-input",
            b"order-request-35",
        ),
    )
    .expect("authoritative completion can race with the compensation request");
    assert!(
        resolver.ingest(&late_completion).is_empty(),
        "a timed-out forward step must not complete the saga after compensation is pending"
    );
    let compensation_completed = SagaChoreographyEvent::CompensationCompleted {
        context: ctx.next_step("create_order".into()),
    };
    assert!(matches!(
        resolver.ingest(&compensation_completed).as_slice(),
        [SagaChoreographyEvent::SagaFailed { .. }]
    ));
}

#[test]
fn workflow_owned_order_and_release_wait_for_authoritative_completion() {
    let ctx = context("create_order", 36);
    let started = SagaChoreographyEvent::SagaStarted {
        context: ctx.clone(),
        payload: b"create-order-intent".to_vec(),
    };
    let mut actor = DeferredWorkflowActor::default();
    let mut resolver = TerminalResolver::new(terminal_policy());
    assert!(resolver.ingest(&started).is_empty());

    apply_sync_workflow_participant_saga_ingress_with_hooks(
        &mut actor,
        started,
        |_actor, _event| {},
        |_event| panic!("workflow emitted an invalid transition"),
        |actor, event| actor.emitted.push(event.clone()),
    );
    let accepted = actor
        .emitted
        .iter()
        .find(|event| matches!(event, SagaChoreographyEvent::StepAccepted { .. }))
        .cloned()
        .expect("workflow should emit StepAccepted instead of premature StepCompleted");
    assert!(matches!(
        accepted,
        SagaChoreographyEvent::StepAccepted {
            compensation_available: true,
            ..
        }
    ));
    assert!(resolver.ingest(&accepted).is_empty());

    std::thread::sleep(Duration::from_millis(3));
    let timeout_events = resolver.poll_timeouts();
    let [compensation_requested] = timeout_events.as_slice() else {
        panic!("accepted order timeout must request its own compensation: {timeout_events:?}");
    };
    assert!(matches!(
        compensation_requested,
        SagaChoreographyEvent::CompensationRequested {
            steps_to_compensate,
            ..
        } if steps_to_compensate.as_slice() == [Box::<str>::from("create_order")]
    ));

    apply_sync_workflow_participant_saga_ingress_with_hooks(
        &mut actor,
        compensation_requested.clone(),
        |_actor, _event| {},
        |_event| panic!("compensation emitted an invalid transition"),
        |actor, event| actor.emitted.push(event.clone()),
    );
    let compensation_accepted = actor
        .emitted
        .iter()
        .find(|event| matches!(event, SagaChoreographyEvent::CompensationAccepted { .. }))
        .cloned()
        .expect("exchange release should remain pending until authoritative completion");
    assert!(resolver.ingest(&compensation_accepted).is_empty());

    let compensation_completed = complete_accepted_workflow_compensation(
        &mut actor,
        ctx.saga_id,
        StepExecutionId::new("cancel-order-request-36"),
        AcceptedCompensationCompletion {
            completed_at_millis: SagaContext::now_millis(),
        },
    )
    .expect("authoritative release should resolve the accepted compensation");
    assert_eq!(actor.compensation_completed_hooks, 1);
    let terminal = resolver.ingest(&compensation_completed);
    assert!(matches!(
        terminal.as_slice(),
        [SagaChoreographyEvent::SagaFailed { .. }]
    ));
}

#[test]
fn accept_failure_on_started_append_leaves_no_accepted_metadata() {
    let mut actor = FailingJournalActor::fail_on_append(1);
    let ctx = context("create_order", 30);

    assert!(matches!(
        accept_workflow_step(
            &mut actor,
            ctx.clone(),
            "order-manager".into(),
            StepExecutionId::new("effect-start-fail"),
            policy(AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: false,
            }),
        ),
        Err(AcceptedStepError::Durability { .. })
    ));
    assert_eq!(actor.saga.accepted_workflow_step_count(), 0);
    assert!(
        actor
            .saga
            .journal
            .read(ctx.saga_id)
            .expect("journal read should succeed")
            .is_empty()
    );
}

#[test]
fn accept_failure_on_metadata_append_does_not_orphan_accepted_metadata() {
    let mut actor = FailingJournalActor::fail_on_append(2);
    let ctx = context("create_order", 31);

    assert!(matches!(
        accept_workflow_step(
            &mut actor,
            ctx.clone(),
            "order-manager".into(),
            StepExecutionId::new("effect-metadata-fail"),
            policy(AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: false,
            }),
        ),
        Err(AcceptedStepError::Durability { .. })
    ));
    assert_eq!(actor.saga.accepted_workflow_step_count(), 0);
    let entries = actor
        .saga
        .journal
        .read(ctx.saga_id)
        .expect("journal read should succeed");
    assert!(matches!(
        entries.as_slice(),
        [icanact_saga_choreography::JournalEntry {
            event: ParticipantEvent::StepExecutionStarted { .. },
            ..
        }]
    ));
    assert!(
        !entries
            .iter()
            .any(|entry| matches!(entry.event, ParticipantEvent::AcceptedStepRecorded { .. }))
    );
}

#[test]
fn accepted_completion_append_failure_keeps_step_pending_for_retry() {
    let mut actor = FailingJournalActor::fail_on_append(3);
    let ctx = context("create_order", 32);
    let execution_id = StepExecutionId::new("effect-complete-retry");
    accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("accept should journal start and metadata");

    assert!(matches!(
        complete_accepted_workflow_step(
            &mut actor,
            ctx.saga_id,
            execution_id.clone(),
            completion(1_700_000_000_060, b"created", b"input", b"undo"),
        ),
        Err(AcceptedStepError::Durability { .. })
    ));
    assert_eq!(actor.saga.accepted_workflow_step_count(), 1);
    assert_eq!(actor.saga.resolved_workflow_step_count(), 0);

    complete_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id,
        completion(1_700_000_000_070, b"created", b"input", b"undo"),
    )
    .expect("retry should append terminal record and complete");
    assert_eq!(actor.saga.accepted_workflow_step_count(), 0);
    assert_eq!(actor.saga.resolved_workflow_step_count(), 1);
}

#[test]
fn accepted_step_metadata_persistence_failure_quarantines_external_effect() {
    let mut actor = FailingJournalActor::fail_on_append(2);
    let ctx = context("create_order", 42);
    let mut emitted = Vec::new();

    handle_saga_event_with_emit(
        &mut actor,
        SagaChoreographyEvent::SagaStarted {
            context: ctx.clone(),
            payload: b"create-order-input".to_vec(),
        },
        |event| emitted.push(event),
    );

    assert!(matches!(
        emitted.as_slice(),
        [SagaChoreographyEvent::StepStarted { .. }, SagaChoreographyEvent::SagaQuarantined {
            reason,
            step,
            participant_id,
            ..
        }] if reason.contains("accepted step persistence failed")
            && step.as_ref() == "create_order"
            && participant_id.as_ref() == "create_order"
    ));
    assert!(matches!(
        actor.saga.saga_states.get(&ctx.saga_id),
        Some(SagaStateEntry::Quarantined(_))
    ));
    let journal = actor
        .saga
        .journal
        .read(ctx.saga_id)
        .expect("quarantine evidence should remain readable");
    assert!(matches!(
        journal.last().map(|entry| &entry.event),
        Some(ParticipantEvent::Quarantined { reason, .. })
            if reason.contains("accepted step persistence failed")
    ));
}

#[test]
fn accepted_failure_append_failure_keeps_step_pending_for_retry() {
    let mut actor = FailingJournalActor::fail_on_append(3);
    let ctx = context("create_order", 33);
    let execution_id = StepExecutionId::new("effect-fail-retry");
    accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("accept should journal start and metadata");

    assert!(matches!(
        fail_accepted_workflow_step(
            &mut actor,
            ctx.saga_id,
            execution_id.clone(),
            failure(1_700_000_000_060, "dispatch failed", false),
        ),
        Err(AcceptedStepError::Durability { .. })
    ));
    assert_eq!(actor.saga.accepted_workflow_step_count(), 1);
    assert_eq!(actor.saga.resolved_workflow_step_count(), 0);

    fail_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id,
        failure(1_700_000_000_070, "dispatch failed", false),
    )
    .expect("retry should append terminal record and fail");
    assert_eq!(actor.saga.accepted_workflow_step_count(), 0);
    assert_eq!(actor.saga.resolved_workflow_step_count(), 1);
}

#[test]
fn compensating_failure_tombstones_forward_execution_but_keeps_compensation_data() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 38);
    let execution_id = StepExecutionId::new("effect-38");
    accept_workflow_step_with_state(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: true,
        }),
        b"create-order-input".to_vec(),
        b"cancel-order-38".to_vec(),
    )
    .expect("step should be accepted");

    fail_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id.clone(),
        failure(1_700_000_000_080, "authoritative create failure", true),
    )
    .expect("compensating failure should resolve the forward execution");

    assert_eq!(actor.saga.accepted_workflow_step_count(), 1);
    assert_eq!(actor.saga.resolved_workflow_step_count(), 1);
    assert_eq!(
        actor
            .saga
            .accepted_workflow_steps
            .get(&ctx.saga_id)
            .expect("compensation data must remain available")
            .compensation_data,
        b"cancel-order-38"
    );
    assert!(matches!(
        complete_accepted_workflow_step(
            &mut actor,
            ctx.saga_id,
            execution_id,
            completion(
                1_700_000_000_090,
                b"stale-success",
                b"create-order-input",
                b"cancel-order-38",
            ),
        ),
        Err(AcceptedStepError::AlreadyResolved { .. })
    ));
}

#[test]
fn live_accepted_step_replays_its_exact_deadline_policy_after_restart() {
    let journal = Arc::new(InMemoryJournal::new());
    let mut actor = RestartedHarnessActor::new(journal.clone());
    let ctx = context("create_order", 42);
    let accepted = accept_workflow_step_with_state(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        StepExecutionId::new("effect-42"),
        AcceptedStepPolicy {
            idle_timeout: Duration::from_secs(60),
            hard_timeout: Duration::from_secs(120),
            timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: true,
            },
        },
        b"create-order-input".to_vec(),
        b"cancel-order-42".to_vec(),
    )
    .expect("step should be accepted before restart");
    let SagaChoreographyEvent::StepAccepted {
        deadline_at_millis,
        hard_deadline_at_millis,
        ..
    } = accepted
    else {
        panic!("expected accepted step event");
    };

    let recovery_events = collect_startup_recovery_events_for_saga_type(
        journal.as_ref(),
        &InMemoryDedupe::new(),
        "create_order",
        "order_lifecycle",
    )
    .expect("live accepted step should recover");
    assert!(matches!(
        recovery_events.as_slice(),
        [SagaChoreographyEvent::StepAccepted {
            context,
            execution_id,
            deadline_at_millis: replayed_deadline,
            hard_deadline_at_millis: replayed_hard_deadline,
            timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: true,
            },
            compensation_available: true,
            ..
        }] if context.saga_id == ctx.saga_id
            && execution_id.as_ref() == "effect-42"
            && *replayed_deadline == deadline_at_millis
            && *replayed_hard_deadline == hard_deadline_at_millis
    ));
}

#[test]
fn compensating_failure_restart_keeps_forward_execution_tombstoned() {
    let journal = Arc::new(InMemoryJournal::new());
    let mut actor = RestartedHarnessActor::new(journal.clone());
    let ctx = context("create_order", 39);
    let execution_id = StepExecutionId::new("effect-39");
    accept_workflow_step_with_state(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: true,
        }),
        b"create-order-input".to_vec(),
        b"cancel-order-39".to_vec(),
    )
    .expect("step should be accepted before restart");
    fail_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id.clone(),
        failure(1_700_000_000_100, "authoritative create failure", true),
    )
    .expect("compensating failure should be durable");

    let recovery_events = collect_startup_recovery_events_for_saga_type(
        &journal,
        &InMemoryDedupe::new(),
        "create_order",
        "order_lifecycle",
    )
    .expect("durable failure recovery events should be collected");
    assert!(matches!(
        recovery_events.as_slice(),
        [
            SagaChoreographyEvent::StepAccepted {
                compensation_available: true,
                ..
            },
            SagaChoreographyEvent::StepFailed {
                requires_compensation: true,
                ..
            }
        ]
    ));
    let mut restarted_resolver = TerminalResolver::new(terminal_policy());
    let mut resolver_events = Vec::new();
    for event in &recovery_events {
        resolver_events.extend(restarted_resolver.ingest(event));
    }
    assert!(matches!(
        resolver_events.as_slice(),
        [SagaChoreographyEvent::CompensationRequested {
            failed_step,
            steps_to_compensate,
            ..
        }] if failed_step.as_ref() == "create_order"
            && steps_to_compensate.as_slice() == [Box::<str>::from("create_order")]
    ));
    drop(actor);

    let mut reopened = RestartedHarnessActor::new(journal);
    recover_accepted_workflow_steps_for_saga_type(
        reopened.saga_support_mut(),
        "create_order",
        "order_lifecycle",
    )
    .expect("compensating failure should recover");

    assert_eq!(reopened.saga.accepted_workflow_step_count(), 1);
    assert_eq!(reopened.saga.resolved_workflow_step_count(), 1);
    assert_eq!(
        reopened
            .saga
            .accepted_workflow_steps
            .get(&ctx.saga_id)
            .expect("compensation data must recover")
            .compensation_data,
        b"cancel-order-39"
    );
    assert!(matches!(
        complete_accepted_workflow_step(
            &mut reopened,
            ctx.saga_id,
            execution_id,
            completion(
                1_700_000_000_110,
                b"stale-success",
                b"create-order-input",
                b"cancel-order-39",
            ),
        ),
        Err(AcceptedStepError::AlreadyResolved { .. })
    ));
}

#[test]
fn accepted_timeout_append_failure_keeps_step_pending_for_retry() {
    let mut actor = FailingJournalActor::fail_on_append(3);
    let ctx = context("create_order", 34);
    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        StepExecutionId::new("effect-timeout-retry"),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("accept should journal start and metadata");
    let SagaChoreographyEvent::StepAccepted {
        deadline_at_millis, ..
    } = accepted
    else {
        panic!("expected accepted step");
    };

    assert!(
        poll_accepted_workflow_step_timeouts(&mut actor, deadline_at_millis + 1).is_empty(),
        "timeout event must not publish when terminal append fails"
    );
    assert_eq!(actor.saga.accepted_workflow_step_count(), 1);
    assert_eq!(actor.saga.resolved_workflow_step_count(), 0);

    let timed_out = poll_accepted_workflow_step_timeouts(&mut actor, deadline_at_millis + 2);
    assert!(matches!(
        timed_out.as_slice(),
        [SagaChoreographyEvent::StepFailed { error, .. }]
            if error.contains("accepted step idle timeout")
    ));
    assert_eq!(actor.saga.accepted_workflow_step_count(), 0);
    assert_eq!(actor.saga.resolved_workflow_step_count(), 1);
}

#[test]
fn accepted_step_uses_actor_clock_for_context_and_deadlines() {
    let mut actor = HarnessActor::default();
    let mut ctx = context("create_order", 20);
    ctx.event_timestamp_millis = 1;

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        StepExecutionId::new("effect-20"),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");

    let SagaChoreographyEvent::StepAccepted {
        context,
        deadline_at_millis,
        hard_deadline_at_millis,
        ..
    } = accepted
    else {
        panic!("expected step accepted event");
    };

    assert!(
        context.event_timestamp_millis > ctx.event_timestamp_millis,
        "acceptance timestamp must come from participant local time"
    );
    assert_eq!(
        deadline_at_millis,
        context.event_timestamp_millis + Duration::from_millis(100).as_millis() as u64
    );
    assert_eq!(
        hard_deadline_at_millis,
        context.event_timestamp_millis + Duration::from_millis(250).as_millis() as u64
    );
}

#[test]
fn saga_run_tracking_reset_allows_same_saga_id_to_accept_again() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 22);
    let execution_id = StepExecutionId::new("effect-22");

    accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("first run should accept step");

    let failed = fail_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id.clone(),
        failure(1_700_000_000_175, "late participant failure", false),
    )
    .expect("first run should resolve step");
    assert!(matches!(failed, SagaChoreographyEvent::StepFailed { .. }));

    actor.clear_in_memory_saga_run_tracking(ctx.saga_id);

    let accepted_again = accept_workflow_step(
        &mut actor,
        ctx,
        "order-manager".into(),
        execution_id,
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    );
    assert!(
        matches!(
            accepted_again,
            Ok(SagaChoreographyEvent::StepAccepted { .. })
        ),
        "reset/prune must clear accepted and resolved step maps for saga id reuse"
    );
}

#[test]
fn accepted_step_can_complete_after_participant_restart() {
    let journal = Arc::new(InMemoryJournal::new());
    let mut actor = RestartedHarnessActor::new(journal.clone());
    let ctx = context("create_order", 23);
    let execution_id = StepExecutionId::new("effect-23");

    accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted before restart");

    let mut reopened = RestartedHarnessActor::new(journal);
    recover_accepted_workflow_steps_for_saga_type(
        reopened.saga_support_mut(),
        "create_order",
        "order_lifecycle",
    )
    .expect("accepted step metadata should recover");

    let completed = complete_accepted_workflow_step(
        &mut reopened,
        ctx.saga_id,
        execution_id,
        completion(
            1_700_000_000_230,
            b"created-after-restart",
            b"input",
            Vec::new(),
        ),
    )
    .expect("recovered accepted step should complete after restart");
    assert!(matches!(
        completed,
        SagaChoreographyEvent::StepCompleted {
            ref context,
            ref output,
            ..
        } if context.saga_id == ctx.saga_id
            && context.event_timestamp_millis == 1_700_000_000_230
            && output == b"created-after-restart"
    ));
}

#[test]
fn accepted_compensation_can_complete_after_participant_restart() {
    let journal = Arc::new(InMemoryJournal::new());
    let ctx = context("create_order", 37);
    let execution_id = StepExecutionId::new("cancel-order-request-37");
    let accepted_at_millis = SagaContext::now_millis();
    journal
        .append(
            ctx.saga_id,
            ParticipantEvent::CompensationRequestRecorded {
                context: ctx.clone(),
                failed_step: "create_order".into(),
                reason: "authoritative create failure".into(),
                failure: icanact_saga_choreography::SagaFailureDetails {
                    step_name: "create_order".into(),
                    participant_id: "order-manager".into(),
                    error_code: Some("exchange_rejected".into()),
                    error_message: "authoritative create failure".into(),
                    at_millis: accepted_at_millis,
                },
                steps_to_compensate: vec!["create_order".into()],
                requested_at_millis: accepted_at_millis,
            },
        )
        .expect("compensation request should be durable before restart");
    journal
        .append(
            ctx.saga_id,
            ParticipantEvent::AcceptedCompensationRecorded {
                context: ctx.clone(),
                participant_id: "order-manager".into(),
                execution_id: execution_id.clone(),
                idle_timeout_millis: 5_000,
                hard_timeout_millis: 10_000,
                accepted_at_millis,
                deadline_at_millis: accepted_at_millis + 5_000,
                hard_deadline_at_millis: accepted_at_millis + 10_000,
            },
        )
        .expect("accepted compensation should be durable before restart");

    let recovery_events = collect_startup_recovery_events_for_saga_type(
        &journal,
        &InMemoryDedupe::new(),
        "create_order",
        "order_lifecycle",
    )
    .expect("accepted compensation lifecycle should recover");
    assert!(matches!(
        recovery_events.as_slice(),
        [SagaChoreographyEvent::CompensationRequested { steps_to_compensate, .. }, SagaChoreographyEvent::CompensationAccepted { .. }]
            if steps_to_compensate.as_slice() == [Box::<str>::from("create_order")]
    ));
    let mut restarted_resolver = TerminalResolver::new(terminal_policy());
    for event in &recovery_events {
        assert!(restarted_resolver.ingest(event).is_empty());
    }

    let mut reopened = RestartedHarnessActor::new(journal);
    recover_accepted_workflow_steps_for_saga_type(
        reopened.saga_support_mut(),
        "create_order",
        "order_lifecycle",
    )
    .expect("accepted compensation metadata should recover");
    assert_eq!(reopened.saga.accepted_workflow_compensation_count(), 1);

    let completed = complete_accepted_workflow_compensation(
        &mut reopened,
        ctx.saga_id,
        execution_id,
        AcceptedCompensationCompletion {
            completed_at_millis: accepted_at_millis + 100,
        },
    )
    .expect("authoritative release should resolve after restart");
    assert!(matches!(
        &completed,
        SagaChoreographyEvent::CompensationCompleted { context }
            if context.saga_id == ctx.saga_id
    ));
    assert!(matches!(
        restarted_resolver.ingest(&completed).as_slice(),
        [SagaChoreographyEvent::SagaFailed {
            failure: Some(failure),
            ..
        }] if failure.step_name.as_ref() == "create_order"
            && failure.participant_id.as_ref() == "order-manager"
            && failure.error_code.as_deref() == Some("exchange_rejected")
            && failure.error_message.as_ref() == "authoritative create failure"
            && failure.at_millis == accepted_at_millis
    ));
}

#[test]
fn unstarted_compensation_request_replays_and_rearms_its_dedupe_key() {
    let journal = InMemoryJournal::new();
    let dedupe = InMemoryDedupe::new();
    let ctx = context("create_order", 43);
    let request = ParticipantEvent::CompensationRequestRecorded {
        context: ctx.clone(),
        failed_step: "create_order".into(),
        reason: "authoritative create failure".into(),
        failure: icanact_saga_choreography::SagaFailureDetails {
            step_name: "create_order".into(),
            participant_id: "order-manager".into(),
            error_code: Some("exchange_rejected".into()),
            error_message: "authoritative create failure".into(),
            at_millis: 1_700_000_000_100,
        },
        steps_to_compensate: vec!["create_order".into()],
        requested_at_millis: 1_700_000_000_100,
    };
    journal
        .append(ctx.saga_id, request)
        .expect("compensation request should persist");
    let dedupe_key = format!(
        "{}:{}:compensation_requested:{}:{}",
        ctx.trace_id, ctx.saga_started_at_millis, ctx.step_name, "create_order"
    );
    dedupe
        .mark_processed(ctx.saga_id, &dedupe_key)
        .expect("original ingress should be marked");

    let recovery_events = collect_startup_recovery_events_for_saga_type(
        &journal,
        &dedupe,
        "create_order",
        "order_lifecycle",
    )
    .expect("unstarted compensation request should recover");
    assert!(matches!(
        recovery_events.as_slice(),
        [SagaChoreographyEvent::CompensationRequested { context, .. }]
            if context.saga_id == ctx.saga_id
    ));
    assert!(
        !dedupe
            .contains(ctx.saga_id, &dedupe_key)
            .expect("rearmed dedupe key should be readable"),
        "startup replay must be allowed through normal participant ingress exactly once"
    );

    journal
        .append(
            ctx.saga_id,
            ParticipantEvent::CompensationStarted {
                attempt: 1,
                started_at_millis: 1_700_000_000_110,
            },
        )
        .expect("compensation start should persist");
    dedupe
        .mark_processed(ctx.saga_id, &dedupe_key)
        .expect("started request should remain deduped");
    let after_start = collect_startup_recovery_events_for_saga_type(
        &journal,
        &dedupe,
        "create_order",
        "order_lifecycle",
    )
    .expect("started compensation recovery should classify");
    assert!(after_start.is_empty());
    assert!(
        dedupe
            .contains(ctx.saga_id, &dedupe_key)
            .expect("started dedupe key should remain readable")
    );
}

#[test]
fn completed_accepted_compensation_replays_terminal_resolution_after_restart() {
    let journal = InMemoryJournal::new();
    let ctx = context("create_order", 44);
    let accepted_at_millis = 1_700_000_000_200;
    journal
        .append(
            ctx.saga_id,
            ParticipantEvent::CompensationRequestRecorded {
                context: ctx.clone(),
                failed_step: "create_order".into(),
                reason: "authoritative create failure".into(),
                failure: icanact_saga_choreography::SagaFailureDetails {
                    step_name: "create_order".into(),
                    participant_id: "order-manager".into(),
                    error_code: Some("exchange_rejected".into()),
                    error_message: "authoritative create failure".into(),
                    at_millis: accepted_at_millis - 10,
                },
                steps_to_compensate: vec!["create_order".into()],
                requested_at_millis: accepted_at_millis - 10,
            },
        )
        .expect("compensation request should persist");
    journal
        .append(
            ctx.saga_id,
            ParticipantEvent::AcceptedCompensationRecorded {
                context: ctx.clone(),
                participant_id: "order-manager".into(),
                execution_id: StepExecutionId::new("cancel-44"),
                idle_timeout_millis: 5_000,
                hard_timeout_millis: 10_000,
                accepted_at_millis,
                deadline_at_millis: accepted_at_millis + 5_000,
                hard_deadline_at_millis: accepted_at_millis + 10_000,
            },
        )
        .expect("accepted compensation should persist");
    journal
        .append(
            ctx.saga_id,
            ParticipantEvent::CompensationCompleted {
                completed_at_millis: accepted_at_millis + 100,
            },
        )
        .expect("authoritative compensation completion should persist");

    let recovery_events = collect_startup_recovery_events_for_saga_type(
        &journal,
        &InMemoryDedupe::new(),
        "create_order",
        "order_lifecycle",
    )
    .expect("completed accepted compensation should recover");
    assert!(matches!(
        recovery_events.as_slice(),
        [SagaChoreographyEvent::CompensationRequested { .. }, SagaChoreographyEvent::CompensationCompleted { context }]
            if context.saga_id == ctx.saga_id
                && context.event_timestamp_millis == accepted_at_millis + 100
    ));
    let mut resolver = TerminalResolver::new(terminal_policy());
    assert!(resolver.ingest(&recovery_events[0]).is_empty());
    assert!(matches!(
        resolver.ingest(&recovery_events[1]).as_slice(),
        [SagaChoreographyEvent::SagaFailed { .. }]
    ));
}

#[test]
fn accepted_compensation_failure_leaves_no_compensating_state() {
    for is_ambiguous in [false, true] {
        let mut actor = HarnessActor::default();
        let ctx = context("create_order", if is_ambiguous { 41 } else { 40 });
        let execution_id = StepExecutionId::new(if is_ambiguous {
            "cancel-order-request-41"
        } else {
            "cancel-order-request-40"
        });
        let accepted_at_millis = 1_700_000_000_000;
        let state = SagaParticipantState::new(
            ctx.saga_id,
            ctx.saga_type.clone(),
            ctx.step_name.clone(),
            ctx.correlation_id,
            ctx.trace_id,
            ctx.initiator_peer_id,
            ctx.saga_started_at_millis,
        )
        .trigger("compensation_accepted", accepted_at_millis)
        .start_execution(accepted_at_millis)
        .start_compensation(accepted_at_millis);
        actor
            .saga
            .saga_states
            .insert(ctx.saga_id, SagaStateEntry::Compensating(state));
        actor.saga.accepted_workflow_compensations.insert(
            ctx.saga_id,
            AcceptedWorkflowCompensation {
                context: ctx.clone(),
                participant_id: "order-manager".into(),
                execution_id: execution_id.clone(),
                policy: policy(AcceptedStepTimeoutOutcome::QuarantineSaga),
                accepted_at_millis,
                deadline_at_millis: accepted_at_millis + 100,
                hard_deadline_at_millis: accepted_at_millis + 250,
            },
        );

        let failed = fail_accepted_workflow_compensation(
            &mut actor,
            ctx.saga_id,
            execution_id,
            AcceptedCompensationFailure {
                failed_at_millis: accepted_at_millis + 50,
                reason: "authoritative cancel failure".into(),
                is_ambiguous,
            },
        )
        .expect("accepted compensation failure should resolve");

        assert!(matches!(
            failed,
            SagaChoreographyEvent::CompensationFailed {
                is_ambiguous: event_ambiguous,
                ..
            } if event_ambiguous == is_ambiguous
        ));
        if is_ambiguous {
            assert!(matches!(
                actor.saga.saga_states.get(&ctx.saga_id),
                Some(SagaStateEntry::Quarantined(_))
            ));
        } else {
            assert!(matches!(
                actor.saga.saga_states.get(&ctx.saga_id),
                Some(SagaStateEntry::Failed(_))
            ));
        }
        let journal = actor
            .saga
            .journal
            .read(ctx.saga_id)
            .expect("compensation failure should be durable");
        if is_ambiguous {
            assert!(matches!(
                journal.last().map(|entry| &entry.event),
                Some(ParticipantEvent::Quarantined { reason, .. })
                    if reason.as_ref() == "authoritative cancel failure"
            ));
        } else {
            assert!(matches!(
                journal.last().map(|entry| &entry.event),
                Some(ParticipantEvent::CompensationFailed {
                    error,
                    is_ambiguous: false,
                    ..
                }) if error.as_ref() == "authoritative cancel failure"
            ));
        }
        assert_eq!(actor.saga.accepted_workflow_compensation_count(), 0);
    }
}

#[test]
fn accepted_step_restart_recovers_refreshed_progress_deadline() {
    let journal = Arc::new(InMemoryJournal::new());
    let mut actor = RestartedHarnessActor::new(journal.clone());
    let ctx = context("create_order", 24);
    let execution_id = StepExecutionId::new("effect-24");

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted before restart");
    let SagaChoreographyEvent::StepAccepted { context, .. } = accepted else {
        panic!("expected accepted step event");
    };
    let accepted_at_millis = context.event_timestamp_millis;

    record_accepted_workflow_step_progress(
        &mut actor,
        ctx.saga_id,
        execution_id,
        accepted_at_millis + 90,
    )
    .expect("progress should refresh deadline before restart");

    let mut reopened = RestartedHarnessActor::new(journal);
    recover_accepted_workflow_steps_for_saga_type(
        reopened.saga_support_mut(),
        "create_order",
        "order_lifecycle",
    )
    .expect("accepted step metadata should recover");

    assert!(
        poll_accepted_workflow_step_timeouts(&mut reopened, accepted_at_millis + 180).is_empty(),
        "recovered accepted step must use the refreshed progress deadline"
    );
}

#[test]
fn accepted_step_late_completion_completes_saga_once() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 2);
    let execution_id = StepExecutionId::new("effect-2");
    let mut resolver = TerminalResolver::new(terminal_policy());

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");
    assert!(resolver.ingest(&accepted).is_empty());

    let completed = complete_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id.clone(),
        completion(1_700_000_000_050, b"created", b"input", Vec::new()),
    )
    .expect("late completion should resolve accepted step");
    let terminal = resolver.ingest(&completed);

    assert!(matches!(
        completed,
        SagaChoreographyEvent::StepCompleted { .. }
    ));
    assert!(matches!(
        terminal.as_slice(),
        [SagaChoreographyEvent::SagaCompleted { .. }]
    ));
    assert!(matches!(
        complete_accepted_workflow_step(
            &mut actor,
            ctx.saga_id,
            execution_id,
            completion(1_700_000_000_060, b"created-again", Vec::new(), Vec::new()),
        ),
        Err(AcceptedStepError::AlreadyResolved { .. })
    ));
}

#[test]
fn accepted_step_completion_records_actual_completion_time() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 21);
    let execution_id = StepExecutionId::new("effect-21");
    let completed_at_millis = 1_700_000_000_175;

    accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");

    let completed = complete_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id,
        completion(completed_at_millis, b"created", b"input", Vec::new()),
    )
    .expect("late completion should resolve accepted step");
    assert!(matches!(
    completed,
    SagaChoreographyEvent::StepCompleted { ref context, .. }
    if context.event_timestamp_millis == completed_at_millis
    ));

    let entries = actor
        .saga
        .journal
        .read(ctx.saga_id)
        .expect("journal read should succeed");
    assert!(entries.iter().any(|entry| {
        matches!(
            &entry.event,
            ParticipantEvent::StepExecutionCompleted {
                completed_at_millis: observed,
                ..
            } if *observed == completed_at_millis
        )
    }));
}

#[test]
fn prune_clears_accepted_and_resolved_workflow_step_state() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 23);
    let execution_id = StepExecutionId::new("effect-23");

    accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");
    complete_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id.clone(),
        completion(1_700_000_000_050, b"created", b"input", Vec::new()),
    )
    .expect("accepted step should complete");

    actor.prune_saga(ctx.saga_id);

    accept_workflow_step(
        &mut actor,
        ctx,
        "order-manager".into(),
        execution_id,
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("pruned saga id should be reusable with the same execution id");
}

#[test]
fn duplicate_accept_for_same_saga_is_rejected_without_losing_original_step() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 22);
    let original = StepExecutionId::new("effect-22-a");

    accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        original.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("first execution should be accepted");
    assert!(matches!(
        accept_workflow_step(
            &mut actor,
            ctx.clone(),
            "order-manager".into(),
            StepExecutionId::new("effect-22-b"),
            policy(AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: false,
            }),
        ),
        Err(AcceptedStepError::AlreadyAccepted { .. })
    ));

    fail_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        original,
        failure(1_700_000_000_060, "original execution failed later", false),
    )
    .expect("original execution should remain pending after duplicate rejection");
}

#[test]
fn accepted_step_late_failure_fails_saga_with_authority() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 3);
    let execution_id = StepExecutionId::new("effect-3");
    let mut resolver = TerminalResolver::new(terminal_policy());

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");
    assert!(resolver.ingest(&accepted).is_empty());

    let failed = fail_accepted_workflow_step(
        &mut actor,
        ctx.saga_id,
        execution_id,
        failure(1_700_000_000_070, "transport dispatch failed", false),
    )
    .expect("late failure should resolve accepted step");
    let terminal = resolver.ingest(&failed);

    assert!(matches!(
            failed,
            SagaChoreographyEvent::StepFailed {
                ref participant_id,
                ref error,
                ..
    } if participant_id.as_ref() == "order-manager"
    && error.contains("transport dispatch failed")
    ));
    assert!(matches!(
    failed,
    SagaChoreographyEvent::StepFailed { ref context, .. }
    if context.event_timestamp_millis == 1_700_000_000_070
    ));
    assert!(matches!(
    terminal.as_slice(),
    [SagaChoreographyEvent::SagaFailed { failure: Some(details), .. }]
                if details.participant_id.as_ref() == "order-manager"
        ));
}

#[test]
fn accepted_step_timeout_can_quarantine_ambiguous_saga() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 4);
    let mut resolver = TerminalResolver::new(terminal_policy());

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        StepExecutionId::new("effect-4"),
        policy(AcceptedStepTimeoutOutcome::QuarantineSaga),
    )
    .expect("step should be accepted");
    let SagaChoreographyEvent::StepAccepted {
        deadline_at_millis, ..
    } = &accepted
    else {
        panic!("expected accepted step");
    };
    assert!(resolver.ingest(&accepted).is_empty());

    let timed_out_at_millis = deadline_at_millis + 1;
    let timed_out = poll_accepted_workflow_step_timeouts(&mut actor, timed_out_at_millis);

    assert!(matches!(
                timed_out.as_slice(),
        [SagaChoreographyEvent::SagaQuarantined {
        context,
        reason,
        participant_id,
    ..
    }] if context.event_timestamp_millis == timed_out_at_millis
    && reason.contains("accepted step idle timeout")
    && participant_id.as_ref() == "order-manager"
    ));
}

#[test]
fn accepted_step_progress_extends_idle_deadline_but_not_hard_deadline() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 5);
    let execution_id = StepExecutionId::new("effect-5");
    let mut resolver = TerminalResolver::new(TerminalPolicy::new(
        "order_lifecycle".into(),
        "order_lifecycle/stalled-test".into(),
        FailureAuthority::AnyParticipant,
        SuccessCriteria::AllOf(["create_order".into()].into()),
        Duration::from_secs(30),
        Duration::from_millis(100),
        &[],
    ));

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");
    let SagaChoreographyEvent::StepAccepted {
        context,
        hard_deadline_at_millis,
        ..
    } = &accepted
    else {
        panic!("expected accepted step");
    };
    let accepted_at_millis = context.event_timestamp_millis;
    assert!(resolver.ingest(&accepted).is_empty());

    let progress = record_accepted_workflow_step_progress(
        &mut actor,
        ctx.saga_id,
        execution_id.clone(),
        accepted_at_millis + 90,
    )
    .expect("progress should extend idle deadline");
    assert!(matches!(
        progress,
        SagaChoreographyEvent::StepAccepted {
            ref context,
            ref execution_id,
            deadline_at_millis,
            ..
        } if context.event_timestamp_millis == accepted_at_millis + 90
            && execution_id.as_ref() == "effect-5"
            && deadline_at_millis > accepted_at_millis + 180
    ));
    assert!(resolver.ingest(&progress).is_empty());
    assert!(
        poll_accepted_workflow_step_timeouts(&mut actor, accepted_at_millis + 180).is_empty(),
        "idle timeout should be refreshed by progress"
    );

    let hard_timed_out_at_millis = hard_deadline_at_millis + 1;
    let hard_timed_out = poll_accepted_workflow_step_timeouts(&mut actor, hard_timed_out_at_millis);
    assert!(matches!(
    hard_timed_out.as_slice(),
    [SagaChoreographyEvent::StepFailed { context, error, .. }]
    if context.event_timestamp_millis == hard_timed_out_at_millis
    && error.contains("accepted step hard timeout")
    ));
    assert!(matches!(
        complete_accepted_workflow_step(
            &mut actor,
            ctx.saga_id,
            execution_id,
            completion(1_700_000_000_260, b"too-late", Vec::new(), Vec::new()),
        ),
        Err(AcceptedStepError::AlreadyResolved { .. })
    ));
}

#[test]
fn resolver_timeout_step_failure_blocks_late_accepted_completion() {
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 7);
    let execution_id = StepExecutionId::new("effect-7");

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");
    if !matches!(accepted, SagaChoreographyEvent::StepAccepted { .. }) {
        panic!("expected accepted step");
    }
    let mut timeout_context = ctx.next_step("create_order".into());
    timeout_context.event_timestamp_millis = 1_700_000_000_101;
    let timeout_event = SagaChoreographyEvent::StepFailed {
        context: timeout_context,
        participant_id: "order-manager".into(),
        error_code: Some("idle".into()),
        error: "accepted step idle timeout: step=create_order execution_id=effect-7 deadline_at_millis=1700000000100 hard_deadline_at_millis=1700000000250".into(),
        requires_compensation: false,
    };

    icanact_saga_choreography::apply_sync_workflow_participant_saga_ingress(
        &mut actor,
        timeout_event,
        |_actor, _event| {},
        |_| {},
    );

    assert!(matches!(
        complete_accepted_workflow_step(
            &mut actor,
            ctx.saga_id,
            execution_id,
            completion(1_700_000_000_260, b"too-late", Vec::new(), Vec::new()),
        ),
        Err(AcceptedStepError::AlreadyResolved { .. })
    ));
}

#[test]
fn saga_test_world_waits_for_and_resolves_accepted_steps() {
    let world = SagaTestWorld::new();
    let _resolver = world
        .attach_terminal_resolver(terminal_policy(), "testkit-terminal")
        .expect("terminal resolver should attach");
    let mut actor = HarnessActor::default();
    let ctx = context("create_order", 6);
    let execution_id = StepExecutionId::new("effect-6");

    let accepted = accept_workflow_step(
        &mut actor,
        ctx.clone(),
        "order-manager".into(),
        execution_id.clone(),
        policy(AcceptedStepTimeoutOutcome::FailStep {
            requires_compensation: false,
        }),
    )
    .expect("step should be accepted");
    world.publish(accepted);

    let observed =
        world.wait_for_step_accepted(ctx.saga_id, "create_order", Duration::from_secs(1));
    assert!(matches!(
        observed,
        SagaChoreographyEvent::StepAccepted {
            ref execution_id,
            ..
        } if execution_id.as_ref() == "effect-6"
    ));

    world
        .complete_accepted_step(
            &mut actor,
            ctx.saga_id,
            execution_id,
            completion(1_700_000_000_050, b"created", b"input", Vec::new()),
        )
        .expect("testkit helper should publish late completion");

    assert!(matches!(
        world.wait_for_terminal(ctx.saga_id, Duration::from_secs(1)),
        SagaTerminalOutcome::Completed { .. }
    ));
}

#[test]
fn saga_test_world_progress_helper_publishes_resolver_heartbeat() {
    let world = SagaTestWorld::new();
    let _resolver = world
        .attach_terminal_resolver(
            TerminalPolicy::new(
                "order_lifecycle".into(),
                "order_lifecycle/progress-testkit".into(),
                FailureAuthority::AnyParticipant,
                SuccessCriteria::AllOf(["create_order".into()].into()),
                Duration::from_secs(30),
                Duration::from_millis(100),
                &[],
            ),
            "testkit-progress-terminal",
        )
        .expect("terminal resolver should attach");
    let mut actor = HarnessActor::default();
    let now_millis = SagaContext::now_millis();
    let mut ctx = context("create_order", 8);
    ctx.saga_started_at_millis = now_millis;
    ctx.event_timestamp_millis = now_millis;
    let execution_id = StepExecutionId::new("effect-8");

    world
        .accept_step(
            &mut actor,
            ctx.clone(),
            "order-manager".into(),
            execution_id.clone(),
            policy(AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: false,
            }),
            b"testkit-saga-input".to_vec(),
            Vec::new(),
        )
        .expect("testkit helper should publish accepted step");
    let accepted =
        world.wait_for_step_accepted(ctx.saga_id, "create_order", Duration::from_secs(1));
    let SagaChoreographyEvent::StepAccepted {
        context,
        deadline_at_millis,
        ..
    } = accepted
    else {
        panic!("expected accepted step");
    };
    let accepted_at_millis = context.event_timestamp_millis;

    world
        .record_accepted_step_progress(
            &mut actor,
            ctx.saga_id,
            execution_id,
            accepted_at_millis + 90,
        )
        .expect("testkit helper should publish progress heartbeat");
    let progress = world.wait_for_event(
        move |event| {
            matches!(
                event,
                SagaChoreographyEvent::StepAccepted { context, .. }
                    if context.saga_id == SagaId::new(8)
                        && context.step_name.as_ref() == "create_order"
                        && context.event_timestamp_millis == accepted_at_millis + 90
            )
        },
        Duration::from_secs(1),
    );
    assert!(matches!(
        progress,
        SagaChoreographyEvent::StepAccepted {
            context,
            deadline_at_millis: refreshed_deadline_at_millis,
            ..
        } if context.event_timestamp_millis == accepted_at_millis + 90
            && refreshed_deadline_at_millis > deadline_at_millis
    ));

    assert!(
        world
            .transcript_for_saga(ctx.saga_id)
            .into_iter()
            .all(|event| !matches!(event, SagaChoreographyEvent::StepFailed { .. })),
        "published progress should refresh resolver deadline before the old deadline fires"
    );

    world
        .complete_accepted_step(
            &mut actor,
            ctx.saga_id,
            StepExecutionId::new("effect-8"),
            completion(SagaContext::now_millis(), b"created", b"input", Vec::new()),
        )
        .expect("completion after refreshed deadline should publish");
    assert!(matches!(
        world.wait_for_terminal(ctx.saga_id, Duration::from_secs(1)),
        SagaTerminalOutcome::Completed { .. }
    ));
}
