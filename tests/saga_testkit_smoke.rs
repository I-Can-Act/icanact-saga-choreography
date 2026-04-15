#![cfg(feature = "test-harness")]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use icanact_core::local_async::{self, AsyncActor};
use icanact_core::local_sync::{self, SyncActor};
use icanact_saga_choreography::{
    AsyncSagaParticipant, CompensationError, DependencySpec, DeterministicContextBuilder,
    FailureAuthority, HasSagaParticipantSupport, HasSagaWorkflowParticipants, InMemoryDedupe,
    InMemoryJournal, JournalEntry, ParticipantDedupeStore, ParticipantJournal,
    SagaChoreographyEvent, SagaParticipant, SagaParticipantChannel, SagaParticipantSupport,
    SagaStateExt, SagaTerminalOutcome, SagaTestWorld, SagaWorkflowParticipant, StepError,
    StepOutput, SuccessCriteria, TerminalPolicy,
};

#[derive(Clone, Debug)]
enum SyncCmd {
    AddBusinessFlag(&'static str),
}

impl icanact_core::TellAskTell for SyncCmd {}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SyncSnapshot {
    executed_inputs: Vec<Vec<u8>>,
    compensated: usize,
    business_flags: Vec<&'static str>,
    active_sagas: usize,
}

struct SyncParticipant {
    saga: SagaParticipantSupport<Arc<InMemoryJournal>, Arc<InMemoryDedupe>>,
    step_name: &'static str,
    dependency: DependencySpec,
    fail_on_execute: bool,
    executed_inputs: Vec<Vec<u8>>,
    compensation_calls: usize,
    business_flags: Vec<&'static str>,
}

impl SyncParticipant {
    fn new(
        step_name: &'static str,
        dependency: DependencySpec,
        journal: Arc<InMemoryJournal>,
        dedupe: Arc<InMemoryDedupe>,
    ) -> Self {
        Self {
            saga: SagaParticipantSupport::new(journal, dedupe),
            step_name,
            dependency,
            fail_on_execute: false,
            executed_inputs: Vec::new(),
            compensation_calls: 0,
            business_flags: Vec::new(),
        }
    }

    fn snapshot(&self) -> SyncSnapshot {
        SyncSnapshot {
            executed_inputs: self.executed_inputs.clone(),
            compensated: self.compensation_calls,
            business_flags: self.business_flags.clone(),
            active_sagas: self.active_saga_count(),
        }
    }
}

impl HasSagaParticipantSupport for SyncParticipant {
    type Journal = Arc<InMemoryJournal>;
    type Dedupe = Arc<InMemoryDedupe>;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

impl SagaParticipant for SyncParticipant {
    type Error = String;

    fn step_name(&self) -> &str {
        self.step_name
    }

    fn saga_types(&self) -> &[&'static str] {
        &["order_lifecycle"]
    }

    fn depends_on(&self) -> DependencySpec {
        self.dependency.clone()
    }

    fn execute_step(
        &mut self,
        _context: &icanact_saga_choreography::SagaContext,
        input: &[u8],
    ) -> Result<StepOutput, StepError> {
        self.executed_inputs.push(input.to_vec());
        if self.fail_on_execute {
            return Err(StepError::RequireCompensation {
                reason: format!("{} failed", self.step_name).into(),
            });
        }
        Ok(StepOutput::Completed {
            output: self.step_name.as_bytes().to_vec(),
            compensation_data: self.step_name.as_bytes().to_vec(),
        })
    }

    fn compensate_step(
        &mut self,
        _context: &icanact_saga_choreography::SagaContext,
        _compensation_data: &[u8],
    ) -> Result<(), CompensationError> {
        self.compensation_calls += 1;
        Ok(())
    }
}

impl SyncActor for SyncParticipant {
    type Contract = local_sync::contract::TellAsk;
    type Tell = SyncCmd;
    type Ask = ();
    type Reply = SyncSnapshot;
    type Channel = SagaParticipantChannel<()>;
    type PubSub = ();
    type Broadcast = ();

    fn handle_tell(&mut self, msg: Self::Tell) {
        match msg {
            SyncCmd::AddBusinessFlag(flag) => self.business_flags.push(flag),
        }
    }

    fn handle_ask(&mut self, _msg: Self::Ask) -> Self::Reply {
        self.snapshot()
    }

    fn handle_channel(&mut self, _channel_id: local_sync::ChannelId, msg: Self::Channel) {
        match msg {
            SagaParticipantChannel::Saga(event) => {
                icanact_saga_choreography::durability::apply_sync_participant_saga_ingress(
                    self,
                    event,
                    |_actor, _incoming| {},
                    |_invalid| {},
                );
            }
            SagaParticipantChannel::Business(()) => {}
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AsyncSnapshot {
    executed_inputs: Vec<Vec<u8>>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
enum AsyncCtl {
    Noop,
}

impl icanact_core::TellAskTell for AsyncCtl {}

struct AsyncParticipant {
    saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
    executed_inputs: Vec<Vec<u8>>,
}

impl Default for AsyncParticipant {
    fn default() -> Self {
        Self {
            saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
            executed_inputs: Vec::new(),
        }
    }
}

impl HasSagaParticipantSupport for AsyncParticipant {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

impl AsyncSagaParticipant for AsyncParticipant {
    type Error = String;

    fn step_name(&self) -> &str {
        "async_step"
    }

    fn saga_types(&self) -> &[&'static str] {
        &["order_lifecycle"]
    }

    fn depends_on(&self) -> DependencySpec {
        DependencySpec::OnSagaStart
    }

    fn execute_step<'a>(
        &'a mut self,
        _context: &'a icanact_saga_choreography::SagaContext,
        input: &'a [u8],
    ) -> icanact_saga_choreography::SagaBoxFuture<'a, Result<StepOutput, StepError>> {
        self.executed_inputs.push(input.to_vec());
        Box::pin(async move {
            Ok(StepOutput::Completed {
                output: b"async_step".to_vec(),
                compensation_data: b"async_step".to_vec(),
            })
        })
    }

    fn compensate_step<'a>(
        &'a mut self,
        _context: &'a icanact_saga_choreography::SagaContext,
        _compensation_data: &'a [u8],
    ) -> icanact_saga_choreography::SagaBoxFuture<'a, Result<(), CompensationError>> {
        Box::pin(async { Ok(()) })
    }
}

impl AsyncActor for AsyncParticipant {
    type Contract = local_async::contract::TellAsk;
    type Tell = AsyncCtl;
    type Ask = ();
    type Reply = AsyncSnapshot;
    type Channel = SagaParticipantChannel<()>;
    type PubSub = ();
    type Broadcast = ();

    fn supports_channels() -> bool {
        true
    }

    async fn handle_tell(
        &mut self,
        _msg: Self::Tell,
        _ctx: &mut icanact_core::local_async::AsyncContext,
    ) {
    }

    fn handle_ask(
        &mut self,
        _msg: Self::Ask,
    ) -> impl std::future::Future<Output = Self::Reply> + Send {
        let snapshot = AsyncSnapshot {
            executed_inputs: self.executed_inputs.clone(),
        };
        async move { snapshot }
    }

    async fn handle_channel(
        &mut self,
        _channel_id: icanact_core::local_async::ChannelId,
        msg: Self::Channel,
    ) {
        match msg {
            SagaParticipantChannel::Saga(event) => {
                icanact_saga_choreography::durability::apply_async_participant_saga_ingress(
                    self,
                    event,
                    |_actor, _incoming| {},
                    |_invalid| {},
                )
                .await;
            }
            SagaParticipantChannel::Business(()) => {}
        }
    }
}

fn test_terminal_policy() -> TerminalPolicy {
    let mut required = HashSet::new();
    required.insert("step_b".into());
    TerminalPolicy {
        saga_type: "order_lifecycle".into(),
        policy_id: "order_lifecycle/test".into(),
        failure_authority: FailureAuthority::AnyParticipant,
        success_criteria: SuccessCriteria::AllOf(required),
        timeout: None,
    }
}

fn workflow_terminal_policy() -> TerminalPolicy {
    let mut required = HashSet::new();
    required.insert("beta_step".into());
    TerminalPolicy {
        saga_type: "workflow_beta".into(),
        policy_id: "workflow_beta/test".into(),
        failure_authority: FailureAuthority::AnyParticipant,
        success_criteria: SuccessCriteria::AllOf(required),
        timeout: None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkflowSnapshot {
    alpha_inputs: Vec<Vec<u8>>,
    beta_inputs: Vec<Vec<u8>>,
}

struct WorkflowActor {
    saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
    alpha_inputs: Vec<Vec<u8>>,
    beta_inputs: Vec<Vec<u8>>,
}

impl Default for WorkflowActor {
    fn default() -> Self {
        Self {
            saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
            alpha_inputs: Vec::new(),
            beta_inputs: Vec::new(),
        }
    }
}

impl HasSagaParticipantSupport for WorkflowActor {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

struct AlphaWorkflow;
struct BetaWorkflow;

static ALPHA_WORKFLOW: AlphaWorkflow = AlphaWorkflow;
static BETA_WORKFLOW: BetaWorkflow = BetaWorkflow;
static WORKFLOW_SPECS: [&'static dyn SagaWorkflowParticipant<WorkflowActor>; 2] =
    [&ALPHA_WORKFLOW, &BETA_WORKFLOW];

impl SagaWorkflowParticipant<WorkflowActor> for AlphaWorkflow {
    fn step_name(&self) -> &'static str {
        "alpha_step"
    }

    fn saga_types(&self) -> &[&'static str] {
        &["workflow_alpha"]
    }

    fn execute_step(
        &self,
        actor: &mut WorkflowActor,
        _context: &icanact_saga_choreography::SagaContext,
        input: &[u8],
    ) -> Result<StepOutput, StepError> {
        actor.alpha_inputs.push(input.to_vec());
        Ok(StepOutput::Completed {
            output: b"alpha-done".to_vec(),
            compensation_data: b"alpha-comp".to_vec(),
        })
    }

    fn compensate_step(
        &self,
        _actor: &mut WorkflowActor,
        _context: &icanact_saga_choreography::SagaContext,
        _compensation_data: &[u8],
    ) -> Result<(), CompensationError> {
        Ok(())
    }
}

impl SagaWorkflowParticipant<WorkflowActor> for BetaWorkflow {
    fn step_name(&self) -> &'static str {
        "beta_step"
    }

    fn saga_types(&self) -> &[&'static str] {
        &["workflow_beta"]
    }

    fn execute_step(
        &self,
        actor: &mut WorkflowActor,
        _context: &icanact_saga_choreography::SagaContext,
        input: &[u8],
    ) -> Result<StepOutput, StepError> {
        actor.beta_inputs.push(input.to_vec());
        Ok(StepOutput::Completed {
            output: b"beta-done".to_vec(),
            compensation_data: b"beta-comp".to_vec(),
        })
    }

    fn compensate_step(
        &self,
        _actor: &mut WorkflowActor,
        _context: &icanact_saga_choreography::SagaContext,
        _compensation_data: &[u8],
    ) -> Result<(), CompensationError> {
        Ok(())
    }
}

impl HasSagaWorkflowParticipants for WorkflowActor {
    fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
        &WORKFLOW_SPECS
    }
}

impl SyncActor for WorkflowActor {
    type Contract = local_sync::contract::TellAsk;
    type Tell = SyncCmd;
    type Ask = ();
    type Reply = WorkflowSnapshot;
    type Channel = SagaParticipantChannel<()>;
    type PubSub = ();
    type Broadcast = ();

    fn handle_tell(&mut self, _msg: Self::Tell) {}

    fn handle_ask(&mut self, _msg: Self::Ask) -> Self::Reply {
        WorkflowSnapshot {
            alpha_inputs: self.alpha_inputs.clone(),
            beta_inputs: self.beta_inputs.clone(),
        }
    }

    fn handle_channel(&mut self, _channel_id: local_sync::ChannelId, msg: Self::Channel) {
        match msg {
            SagaParticipantChannel::Saga(event) => {
                icanact_saga_choreography::durability::apply_sync_workflow_participant_saga_ingress(
                    self,
                    event,
                    |_actor, _incoming| {},
                    |_invalid| {},
                );
            }
            SagaParticipantChannel::Business(()) => {}
        }
    }
}

struct DuplicateWorkflowActor {
    saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
}

impl Default for DuplicateWorkflowActor {
    fn default() -> Self {
        Self {
            saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
        }
    }
}

impl HasSagaParticipantSupport for DuplicateWorkflowActor {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

struct DuplicateWorkflowOne;
struct DuplicateWorkflowTwo;

static DUPLICATE_WORKFLOW_ONE: DuplicateWorkflowOne = DuplicateWorkflowOne;
static DUPLICATE_WORKFLOW_TWO: DuplicateWorkflowTwo = DuplicateWorkflowTwo;
static DUPLICATE_WORKFLOW_SPECS: [&'static dyn SagaWorkflowParticipant<DuplicateWorkflowActor>; 2] =
    [&DUPLICATE_WORKFLOW_ONE, &DUPLICATE_WORKFLOW_TWO];

impl SagaWorkflowParticipant<DuplicateWorkflowActor> for DuplicateWorkflowOne {
    fn step_name(&self) -> &'static str {
        "dup_one"
    }

    fn saga_types(&self) -> &[&'static str] {
        &["duplicate_workflow"]
    }

    fn execute_step(
        &self,
        _actor: &mut DuplicateWorkflowActor,
        _context: &icanact_saga_choreography::SagaContext,
        _input: &[u8],
    ) -> Result<StepOutput, StepError> {
        Ok(StepOutput::Completed {
            output: Vec::new(),
            compensation_data: Vec::new(),
        })
    }

    fn compensate_step(
        &self,
        _actor: &mut DuplicateWorkflowActor,
        _context: &icanact_saga_choreography::SagaContext,
        _compensation_data: &[u8],
    ) -> Result<(), CompensationError> {
        Ok(())
    }
}

impl SagaWorkflowParticipant<DuplicateWorkflowActor> for DuplicateWorkflowTwo {
    fn step_name(&self) -> &'static str {
        "dup_two"
    }

    fn saga_types(&self) -> &[&'static str] {
        &["duplicate_workflow"]
    }

    fn execute_step(
        &self,
        _actor: &mut DuplicateWorkflowActor,
        _context: &icanact_saga_choreography::SagaContext,
        _input: &[u8],
    ) -> Result<StepOutput, StepError> {
        Ok(StepOutput::Completed {
            output: Vec::new(),
            compensation_data: Vec::new(),
        })
    }

    fn compensate_step(
        &self,
        _actor: &mut DuplicateWorkflowActor,
        _context: &icanact_saga_choreography::SagaContext,
        _compensation_data: &[u8],
    ) -> Result<(), CompensationError> {
        Ok(())
    }
}

impl HasSagaWorkflowParticipants for DuplicateWorkflowActor {
    fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
        &DUPLICATE_WORKFLOW_SPECS
    }
}

impl SyncActor for DuplicateWorkflowActor {
    type Contract = local_sync::contract::TellAsk;
    type Tell = SyncCmd;
    type Ask = ();
    type Reply = ();
    type Channel = SagaParticipantChannel<()>;
    type PubSub = ();
    type Broadcast = ();

    fn handle_tell(&mut self, _msg: Self::Tell) {}

    fn handle_ask(&mut self, _msg: Self::Ask) -> Self::Reply {}

    fn handle_channel(&mut self, _channel_id: local_sync::ChannelId, _msg: Self::Channel) {}
}

fn wait_for_sync_snapshot<A, P>(
    actor_ref: &icanact_core::local_sync::SyncActorRef<A>,
    predicate: P,
    timeout: Duration,
) -> A::Reply
where
    A: SyncActor<Ask = ()>,
    A::Contract: local_sync::contract::SupportsAsk<A>,
    A::Reply: Clone + Send + 'static,
    P: Fn(&A::Reply) -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(reply) = actor_ref.ask(()) {
            if predicate(&reply) {
                return reply;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for sync actor snapshot"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

async fn wait_for_async_snapshot<A, P>(
    actor_ref: &icanact_core::local_async::AsyncActorRef<A>,
    predicate: P,
    timeout: Duration,
) -> A::Reply
where
    A: AsyncActor<Ask = ()>,
    A::Contract: local_async::contract::SupportsAsk<A>,
    A::Reply: Clone + Send + 'static,
    P: Fn(&A::Reply) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(reply) = actor_ref.ask(()).await {
            if predicate(&reply) {
                return reply;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for async actor snapshot"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[test]
fn sync_world_runs_real_saga_workflow_and_exposes_actor_state() {
    SagaTestWorld::init_test_logging();
    let world = SagaTestWorld::new();
    let _resolver = world.attach_terminal_resolver(test_terminal_policy(), "testkit");

    let step_a_journal = Arc::new(InMemoryJournal::new());
    let step_a_dedupe = Arc::new(InMemoryDedupe::new());

    let step_a = world.spawn_sync_channel_participant(
        SyncParticipant::new(
            "step_a",
            DependencySpec::OnSagaStart,
            Arc::clone(&step_a_journal),
            Arc::clone(&step_a_dedupe),
        ),
        "saga",
        1024,
    );
    let step_b = world.spawn_sync_channel_participant(
        SyncParticipant::new(
            "step_b",
            DependencySpec::After("step_a"),
            Arc::new(InMemoryJournal::new()),
            Arc::new(InMemoryDedupe::new()),
        ),
        "saga",
        1024,
    );

    let ctx = DeterministicContextBuilder::default()
        .with_saga_id(42)
        .with_saga_type("order_lifecycle")
        .with_step_name("start")
        .build();
    let saga_id = ctx.saga_id;

    assert!(step_a.actor_ref().tell(SyncCmd::AddBusinessFlag("warm")));
    world.start_saga(ctx, b"payload".to_vec());

    let terminal = world.wait_for_terminal(saga_id, Duration::from_secs(2));
    assert!(matches!(terminal, SagaTerminalOutcome::Completed { .. }));

    let a_state = wait_for_sync_snapshot(
        &step_a.actor_ref(),
        |snapshot: &SyncSnapshot| snapshot.executed_inputs.len() == 1,
        Duration::from_secs(1),
    );
    let b_state = wait_for_sync_snapshot(
        &step_b.actor_ref(),
        |snapshot: &SyncSnapshot| snapshot.executed_inputs.len() == 1,
        Duration::from_secs(1),
    );

    assert_eq!(a_state.executed_inputs, vec![b"payload".to_vec()]);
    assert_eq!(a_state.business_flags, vec!["warm"]);
    assert_eq!(b_state.executed_inputs, vec![b"step_a".to_vec()]);
    assert!(world
        .transcript_for_saga(saga_id)
        .iter()
        .any(|event| matches!(event, SagaChoreographyEvent::StepCompleted { context, .. } if context.step_name.as_ref() == "step_b")));

    let entries: Vec<JournalEntry> = step_a_journal
        .read(saga_id)
        .expect("shared journal should be readable");
    assert!(!entries.is_empty());
    assert!(
        !step_a_dedupe.contains(saga_id, "1:1700000000000:saga_started:start"),
        "terminal processing should prune participant dedupe keys"
    );

    step_a.shutdown();
    step_b.shutdown();
}

#[test]
fn sync_world_runs_real_workflow_participant_path() {
    let world = SagaTestWorld::new();
    let _resolver = world.attach_terminal_resolver(workflow_terminal_policy(), "testkit");

    let actor = world.spawn_sync_workflow_channel_participant::<WorkflowActor, ()>(
        WorkflowActor::default(),
        "workflow-saga",
        1024,
    );

    let ctx = DeterministicContextBuilder::default()
        .with_saga_id(501)
        .with_saga_type("workflow_beta")
        .with_step_name("start")
        .build();

    world.start_saga(ctx.clone(), b"workflow-input".to_vec());

    let terminal = world.wait_for_terminal(ctx.saga_id, Duration::from_secs(2));
    assert!(matches!(terminal, SagaTerminalOutcome::Completed { .. }));

    let snapshot = wait_for_sync_snapshot(
        &actor.actor_ref(),
        |state: &WorkflowSnapshot| state.beta_inputs.len() == 1,
        Duration::from_secs(1),
    );
    assert!(snapshot.alpha_inputs.is_empty());
    assert_eq!(snapshot.beta_inputs, vec![b"workflow-input".to_vec()]);
    assert!(world
        .transcript_for_saga(ctx.saga_id)
        .iter()
        .any(|event| matches!(event, SagaChoreographyEvent::StepCompleted { context, .. } if context.step_name.as_ref() == "beta_step")));

    actor.shutdown();
}

#[test]
#[should_panic(expected = "workflow participant saga type registration should be valid")]
fn sync_workflow_spawn_rejects_duplicate_saga_type_registration() {
    let world = SagaTestWorld::new();

    let _ = world.spawn_sync_workflow_channel_participant::<DuplicateWorkflowActor, ()>(
        DuplicateWorkflowActor::default(),
        "workflow-saga",
        1024,
    );
}

#[test]
fn sync_world_spawn_args_runs_real_saga_workflow() {
    let world = SagaTestWorld::new();
    let _resolver = world.attach_terminal_resolver(test_terminal_policy(), "testkit");

    let step_a = world.spawn_sync_channel_participant(
        SyncParticipant::new(
            "step_a",
            DependencySpec::OnSagaStart,
            Arc::new(InMemoryJournal::new()),
            Arc::new(InMemoryDedupe::new()),
        ),
        "saga",
        1024,
    );
    let step_b = world.spawn_sync_channel_participant(
        SyncParticipant::new(
            "step_b",
            DependencySpec::After("step_a"),
            Arc::new(InMemoryJournal::new()),
            Arc::new(InMemoryDedupe::new()),
        ),
        "saga",
        1024,
    );

    let ctx = DeterministicContextBuilder::default()
        .with_saga_id(142)
        .with_saga_type("order_lifecycle")
        .with_step_name("start")
        .build();
    world.start_saga(ctx.clone(), b"payload".to_vec());

    let terminal = world.wait_for_terminal(ctx.saga_id, Duration::from_secs(2));
    assert!(matches!(terminal, SagaTerminalOutcome::Completed { .. }));

    step_a.shutdown();
    step_b.shutdown();
}

#[test]
fn sync_world_drives_compensation_flow_without_changing_actor_contracts() {
    let world = SagaTestWorld::new();
    let _resolver = world.attach_terminal_resolver(test_terminal_policy(), "testkit");

    let step_a = world.spawn_sync_channel_participant(
        SyncParticipant::new(
            "step_a",
            DependencySpec::OnSagaStart,
            Arc::new(InMemoryJournal::new()),
            Arc::new(InMemoryDedupe::new()),
        ),
        "saga",
        1024,
    );
    let mut failing_step_b = SyncParticipant::new(
        "step_b",
        DependencySpec::After("step_a"),
        Arc::new(InMemoryJournal::new()),
        Arc::new(InMemoryDedupe::new()),
    );
    failing_step_b.fail_on_execute = true;
    let step_b = world.spawn_sync_channel_participant(failing_step_b, "saga", 1024);

    let ctx = DeterministicContextBuilder::default()
        .with_saga_id(77)
        .with_saga_type("order_lifecycle")
        .with_step_name("start")
        .build();

    world.start_saga(ctx.clone(), b"payload".to_vec());

    let terminal = world.wait_for_terminal(ctx.saga_id, Duration::from_secs(2));
    assert!(matches!(terminal, SagaTerminalOutcome::Failed { .. }));

    let a_state = wait_for_sync_snapshot(
        &step_a.actor_ref(),
        |snapshot: &SyncSnapshot| snapshot.compensated == 1,
        Duration::from_secs(1),
    );
    assert_eq!(a_state.compensated, 1);
    assert!(world
        .transcript_for_saga(ctx.saga_id)
        .iter()
        .any(|event| { matches!(event, SagaChoreographyEvent::CompensationRequested { .. }) }));

    step_a.shutdown();
    step_b.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_world_runs_real_async_participant_path() {
    let world = SagaTestWorld::new();
    let actor = world
        .spawn_async_channel_participant(AsyncParticipant::default(), "saga", 1024)
        .await;

    let ctx = DeterministicContextBuilder::default()
        .with_saga_id(99)
        .with_saga_type("order_lifecycle")
        .with_step_name("start")
        .build();

    world.start_saga(ctx.clone(), b"async-input".to_vec());

    let observed = world
        .wait_for_event_async(
            move |event| {
                matches!(
                    event,
                    SagaChoreographyEvent::StepCompleted { context, .. }
                        if context.saga_id == ctx.saga_id && context.step_name.as_ref() == "async_step"
                )
            },
            Duration::from_secs(2),
        )
        .await;
    assert!(matches!(
        observed,
        SagaChoreographyEvent::StepCompleted { .. }
    ));

    let snapshot = wait_for_async_snapshot(
        &actor.actor_ref(),
        |state: &AsyncSnapshot| state.executed_inputs.len() == 1,
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(snapshot.executed_inputs, vec![b"async-input".to_vec()]);

    actor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_world_spawn_args_runs_real_async_participant_path() {
    let world = SagaTestWorld::new();
    let actor = world
        .spawn_async_channel_participant(AsyncParticipant::default(), "saga", 1024)
        .await;

    let ctx = DeterministicContextBuilder::default()
        .with_saga_id(199)
        .with_saga_type("order_lifecycle")
        .with_step_name("start")
        .build();

    world.start_saga(ctx.clone(), b"async-input".to_vec());

    let observed = world
        .wait_for_event_async(
            move |event| {
                matches!(
                    event,
                    SagaChoreographyEvent::StepCompleted { context, .. }
                        if context.saga_id == ctx.saga_id && context.step_name.as_ref() == "async_step"
                )
            },
            Duration::from_secs(2),
        )
        .await;
    assert!(matches!(
        observed,
        SagaChoreographyEvent::StepCompleted { .. }
    ));

    actor.shutdown().await;
}
