#![cfg(feature = "test-harness")]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use icanact_core::local_async::{self, AsyncActor};
use icanact_core::local_sync::{self, SyncActor};
use icanact_saga_choreography::{
    AsyncSagaParticipant, CompensationError, DependencySpec, DeterministicContextBuilder,
    FailureAuthority, HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal, JournalEntry,
    ParticipantDedupeStore, ParticipantJournal, SagaChoreographyEvent, SagaParticipant,
    SagaParticipantSupport, SagaStateExt, SagaTerminalOutcome, SagaTestWorld, StepError,
    StepOutput, SuccessCriteria, TerminalPolicy,
};

#[derive(Clone, Debug)]
enum SyncCmd {
    SagaEvent(SagaChoreographyEvent),
    AddBusinessFlag(&'static str),
}

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

    fn handle_tell(&mut self, msg: Self::Tell) {
        match msg {
            SyncCmd::SagaEvent(event) => {
                icanact_saga_choreography::durability::apply_sync_participant_saga_ingress(
                    self,
                    event,
                    |_actor, _incoming| {},
                    |_invalid| {},
                );
            }
            SyncCmd::AddBusinessFlag(flag) => self.business_flags.push(flag),
        }
    }

    fn handle_ask(&mut self, _msg: Self::Ask) -> Self::Reply {
        self.snapshot()
    }
}

#[derive(Clone, Debug)]
enum AsyncCmd {
    SagaEvent(SagaChoreographyEvent),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AsyncSnapshot {
    executed_inputs: Vec<Vec<u8>>,
}

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
    type Tell = AsyncCmd;
    type Ask = ();
    type Reply = AsyncSnapshot;

    fn handle_tell(&mut self, msg: Self::Tell) -> impl std::future::Future<Output = ()> + Send {
        match msg {
            AsyncCmd::SagaEvent(event) => Box::pin(
                icanact_saga_choreography::durability::apply_async_participant_saga_ingress(
                    self,
                    event,
                    |_actor, _incoming| {},
                    |_invalid| {},
                ),
            ),
        }
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

    let step_a = world.spawn_sync_participant(
        SyncParticipant::new(
            "step_a",
            DependencySpec::OnSagaStart,
            Arc::clone(&step_a_journal),
            Arc::clone(&step_a_dedupe),
        ),
        SyncCmd::SagaEvent,
    );
    let step_b = world.spawn_sync_participant(
        SyncParticipant::new(
            "step_b",
            DependencySpec::After("step_a"),
            Arc::new(InMemoryJournal::new()),
            Arc::new(InMemoryDedupe::new()),
        ),
        SyncCmd::SagaEvent,
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
fn sync_world_drives_compensation_flow_without_changing_actor_contracts() {
    let world = SagaTestWorld::new();
    let _resolver = world.attach_terminal_resolver(test_terminal_policy(), "testkit");

    let step_a = world.spawn_sync_participant(
        SyncParticipant::new(
            "step_a",
            DependencySpec::OnSagaStart,
            Arc::new(InMemoryJournal::new()),
            Arc::new(InMemoryDedupe::new()),
        ),
        SyncCmd::SagaEvent,
    );
    let mut failing_step_b = SyncParticipant::new(
        "step_b",
        DependencySpec::After("step_a"),
        Arc::new(InMemoryJournal::new()),
        Arc::new(InMemoryDedupe::new()),
    );
    failing_step_b.fail_on_execute = true;
    let step_b = world.spawn_sync_participant(failing_step_b, SyncCmd::SagaEvent);

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
        .spawn_async_participant(AsyncParticipant::default(), AsyncCmd::SagaEvent)
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
