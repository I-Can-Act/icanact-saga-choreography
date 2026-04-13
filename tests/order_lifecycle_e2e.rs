//! End-to-end multi-participant saga choreography tests.
//!
//! Tests a 3-participant order lifecycle saga:
//!   PositionActor ("check_position", OnSagaStart)
//!   BalanceActor  ("check_balance",  OnSagaStart)
//!   OrderActor    ("create_order",   AllOf(["check_position", "check_balance"]))
//!
//! Covers: happy path, step failures, compensation chains, compensation failures,
//! panics, idempotency, dependency gating, and terminal latch.

use std::collections::HashSet;
use std::time::Duration;

use icanact_core::local_sync;
use icanact_core::local_sync::contract::TellAsk;
use icanact_core::testkit::TestWorld;

use icanact_saga_choreography::durability::{
    run_participant_phase_with_panic_quarantine, ActiveSagaExecution, ActiveSagaExecutionPhase,
    HasActiveSagaExecution,
};
use icanact_saga_choreography::{
    handle_saga_event_with_emit, CompensationError, DependencySpec, FailureAuthority,
    HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal, SagaChoreographyBus,
    SagaChoreographyEvent, SagaContext, SagaId, SagaParticipant, SagaParticipantSupport, StepError,
    StepOutput, SuccessCriteria, TerminalPolicy,
};

const SAGA_TYPE: &str = "order_lifecycle";
const STEP_POSITION: &str = "check_position";
const STEP_BALANCE: &str = "check_balance";
const STEP_ORDER: &str = "create_order";
const TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Participant actor
// ---------------------------------------------------------------------------

/// Query message to inspect participant state from tests.
#[derive(Debug)]
struct QueryState;

/// Reply with current execution and compensation counts.
#[derive(Debug, PartialEq, Eq)]
struct ParticipantState {
    executed_count: usize,
    compensated_count: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct TerminalCounts {
    completed: usize,
    failed: usize,
    quarantined: usize,
}

#[derive(Debug)]
struct QueryTerminalCounts;

struct TerminalProbe {
    counts: TerminalCounts,
}

impl TerminalProbe {
    fn new() -> Self {
        Self {
            counts: TerminalCounts::default(),
        }
    }
}

/// A configurable saga participant spawned as a real SyncActor.
///
/// Receives `SagaChoreographyEvent` via tell, processes them through
/// `handle_saga_event_with_emit`, and re-publishes emitted events to the bus.
struct ConfigurableParticipant {
    step_name: &'static str,
    dependency_spec: DependencySpec,
    execute_result: Option<Result<StepOutput, StepError>>,
    compensate_result: Result<(), CompensationError>,
    should_panic: bool,
    saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
    active_saga_execution: Option<ActiveSagaExecution>,
    bus: Option<SagaChoreographyBus>,
    executed_count: usize,
    compensated_count: usize,
}

impl ConfigurableParticipant {
    fn new(step_name: &'static str, dependency_spec: DependencySpec) -> Self {
        Self {
            step_name,
            dependency_spec,
            execute_result: Some(Ok(StepOutput::Completed {
                output: vec![],
                compensation_data: vec![1],
            })),
            compensate_result: Ok(()),
            should_panic: false,
            saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
            active_saga_execution: None,
            bus: None,
            executed_count: 0,
            compensated_count: 0,
        }
    }

    fn with_execute_result(mut self, result: Result<StepOutput, StepError>) -> Self {
        self.execute_result = Some(result);
        self
    }

    fn with_compensate_result(mut self, result: Result<(), CompensationError>) -> Self {
        self.compensate_result = result;
        self
    }

    fn with_panic(mut self) -> Self {
        self.should_panic = true;
        self
    }

    fn attach_bus(&mut self, bus: SagaChoreographyBus) {
        self.saga.attach_bus(bus.clone());
        self.bus = Some(bus);
    }
}

impl local_sync::SyncActor for ConfigurableParticipant {
    type Contract = TellAsk;
    type Tell = SagaChoreographyEvent;
    type Ask = QueryState;
    type Reply = ParticipantState;

    fn handle_tell(&mut self, msg: Self::Tell) {
        let bus = self.bus.clone();
        if let Some(bus) = bus {
            handle_saga_event_with_emit(self, msg, |emitted| {
                let _ = bus.publish(emitted);
            });
        }
    }

    fn handle_ask(&mut self, _msg: Self::Ask) -> Self::Reply {
        ParticipantState {
            executed_count: self.executed_count,
            compensated_count: self.compensated_count,
        }
    }
}

impl local_sync::SyncActor for TerminalProbe {
    type Contract = TellAsk;
    type Tell = SagaChoreographyEvent;
    type Ask = QueryTerminalCounts;
    type Reply = TerminalCounts;

    fn handle_tell(&mut self, msg: Self::Tell) {
        match &msg {
            SagaChoreographyEvent::SagaCompleted { .. } => {
                self.counts.completed += 1;
            }
            SagaChoreographyEvent::SagaFailed { .. } => {
                self.counts.failed += 1;
            }
            SagaChoreographyEvent::SagaQuarantined { .. } => {
                self.counts.quarantined += 1;
            }
            _ => {}
        }
    }

    fn handle_ask(&mut self, _msg: Self::Ask) -> Self::Reply {
        self.counts
    }
}

impl HasSagaParticipantSupport for ConfigurableParticipant {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

impl HasActiveSagaExecution for ConfigurableParticipant {
    fn active_saga_execution_slot(&mut self) -> &mut Option<ActiveSagaExecution> {
        &mut self.active_saga_execution
    }
}

impl SagaParticipant for ConfigurableParticipant {
    type Error = String;

    fn step_name(&self) -> &str {
        self.step_name
    }

    fn saga_types(&self) -> &[&'static str] {
        &[SAGA_TYPE]
    }

    fn depends_on(&self) -> DependencySpec {
        self.dependency_spec.clone()
    }

    fn execute_step(
        &mut self,
        _context: &SagaContext,
        _input: &[u8],
    ) -> Result<StepOutput, StepError> {
        self.executed_count += 1;
        if self.should_panic {
            panic!("deliberate panic from {}", self.step_name);
        }
        match &self.execute_result {
            Some(result) => result.clone(),
            None => Ok(StepOutput::Completed {
                output: vec![],
                compensation_data: vec![1],
            }),
        }
    }

    fn compensate_step(
        &mut self,
        _context: &SagaContext,
        _compensation_data: &[u8],
    ) -> Result<(), CompensationError> {
        self.compensated_count += 1;
        self.compensate_result.clone()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn context_for(saga_id: u64) -> SagaContext {
    let now = SagaContext::now_millis();
    SagaContext {
        saga_id: SagaId::new(saga_id),
        saga_type: SAGA_TYPE.into(),
        step_name: "start".into(),
        correlation_id: saga_id,
        causation_id: saga_id,
        trace_id: saga_id,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: [0; 32],
        saga_started_at_millis: now,
        event_timestamp_millis: now,
    }
}

fn terminal_error(reason: &str) -> StepError {
    StepError::Terminal {
        reason: reason.into(),
    }
}

fn require_compensation_error(reason: &str) -> StepError {
    StepError::RequireCompensation {
        reason: reason.into(),
    }
}

fn test_policy() -> TerminalPolicy {
    let mut required: HashSet<Box<str>> = HashSet::new();
    required.insert(STEP_POSITION.into());
    required.insert(STEP_BALANCE.into());
    required.insert(STEP_ORDER.into());
    TerminalPolicy {
        saga_type: SAGA_TYPE.into(),
        policy_id: "order_lifecycle/e2e_test".into(),
        failure_authority: FailureAuthority::AnyParticipant,
        success_criteria: SuccessCriteria::AllOf(required),
        timeout: None,
    }
}

/// Mailbox capacity large enough for synchronous re-entrant bus dispatch cascades.
/// Each bus.publish dispatches to all subscribers, which may trigger nested publishes.
/// With N subscribers, a single publish can generate O(N^2) messages per actor.
const MAILBOX_CAPACITY: usize = 512;

/// Spawn a participant as a real SyncActor, subscribe it to the bus, return the actor ref.
fn spawn_and_subscribe(
    world: &TestWorld,
    bus: &SagaChoreographyBus,
    mut participant: ConfigurableParticipant,
) -> (
    local_sync::SyncActorRef<ConfigurableParticipant>,
    local_sync::ActorHandle,
) {
    participant.attach_bus(bus.clone());
    let opts = local_sync::SpawnOpts {
        mailbox_capacity: MAILBOX_CAPACITY,
        ..Default::default()
    };
    let (actor_ref, handle) = world.spawn_sync_with_opts(participant, opts);
    handle.wait_for_startup();
    let ref_clone = actor_ref.clone();
    let _sub = bus.subscribe_saga_type_fn(SAGA_TYPE, move |event: &SagaChoreographyEvent| {
        ref_clone.try_tell(event.clone()).is_ok()
    });
    (actor_ref, handle)
}

fn spawn_terminal_probe(
    world: &TestWorld,
    bus: &SagaChoreographyBus,
) -> (
    local_sync::SyncActorRef<TerminalProbe>,
    local_sync::ActorHandle,
) {
    let opts = local_sync::SpawnOpts {
        mailbox_capacity: MAILBOX_CAPACITY,
        ..Default::default()
    };
    let (probe_ref, handle) = world.spawn_sync_with_opts(TerminalProbe::new(), opts);
    handle.wait_for_startup();
    let ref_clone = probe_ref.clone();
    let _probe = bus.subscribe_saga_type_fn(SAGA_TYPE, move |event: &SagaChoreographyEvent| {
        let _ = ref_clone.try_tell(event.clone());
        true
    });
    (probe_ref, handle)
}

/// Query a participant's state via ask.
fn query_state(actor_ref: &local_sync::SyncActorRef<ConfigurableParticipant>) -> ParticipantState {
    actor_ref
        .ask(QueryState)
        .expect("participant should respond")
}

fn query_terminal_counts(actor_ref: &local_sync::SyncActorRef<TerminalProbe>) -> TerminalCounts {
    actor_ref
        .ask(QueryTerminalCounts)
        .expect("terminal probe should respond")
}

/// Wait until a predicate is true or timeout.
fn wait_until(timeout: Duration, mut pred: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if pred() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("wait_until timed out after {timeout:?}");
}

fn shutdown(handle: local_sync::ActorHandle) {
    handle.shutdown();
}

// ===========================================================================
// Test 1: Happy Path
// ===========================================================================

#[test]
fn all_succeed_saga_completed() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();

    let _resolver = bus.attach_terminal_resolver(test_policy(), "e2e-resolver");
    let (terminal_ref, terminal_h) = spawn_terminal_probe(&world, &bus);

    let (p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart),
    );
    let (b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart),
    );
    let (o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        ),
    );

    let ctx = context_for(1);
    let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
        context: ctx,
        payload: vec![42],
    });

    wait_until(TIMEOUT, || {
        query_terminal_counts(&terminal_ref).completed >= 1
    });

    assert_eq!(
        query_terminal_counts(&terminal_ref),
        TerminalCounts {
            completed: 1,
            failed: 0,
            quarantined: 0,
        }
    );
    assert_eq!(
        query_state(&p_ref),
        ParticipantState {
            executed_count: 1,
            compensated_count: 0
        }
    );
    assert_eq!(
        query_state(&b_ref),
        ParticipantState {
            executed_count: 1,
            compensated_count: 0
        }
    );
    assert_eq!(
        query_state(&o_ref),
        ParticipantState {
            executed_count: 1,
            compensated_count: 0
        }
    );
    shutdown(p_h);
    shutdown(b_h);
    shutdown(o_h);
    shutdown(terminal_h);
}

// ===========================================================================
// Test 2: Position fails Terminal
// ===========================================================================

#[test]
fn position_fails_terminal_no_compensation() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();
    let _resolver = bus.attach_terminal_resolver(test_policy(), "e2e-resolver");
    let (terminal_ref, terminal_h) = spawn_terminal_probe(&world, &bus);

    let (p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart)
            .with_execute_result(Err(terminal_error("position unavailable"))),
    );
    let (b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart),
    );
    let (o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        ),
    );

    let ctx = context_for(2);
    let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
        context: ctx,
        payload: vec![42],
    });

    wait_until(TIMEOUT, || query_terminal_counts(&terminal_ref).failed >= 1);

    assert_eq!(
        query_terminal_counts(&terminal_ref),
        TerminalCounts {
            completed: 0,
            failed: 1,
            quarantined: 0,
        }
    );
    assert_eq!(
        query_state(&p_ref),
        ParticipantState {
            executed_count: 1,
            compensated_count: 0
        }
    );
    assert_eq!(
        query_state(&b_ref),
        ParticipantState {
            executed_count: 1,
            compensated_count: 0
        }
    ); // OnSagaStart, still executes
    assert_eq!(
        query_state(&o_ref),
        ParticipantState {
            executed_count: 0,
            compensated_count: 0
        }
    ); // Never fires
    shutdown(p_h);
    shutdown(b_h);
    shutdown(o_h);
    shutdown(terminal_h);
}

// ===========================================================================
// Test 3: Balance fails Terminal after Position succeeds
// ===========================================================================

#[test]
fn balance_fails_terminal_after_position_succeeds() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();
    let _resolver = bus.attach_terminal_resolver(test_policy(), "e2e-resolver");
    let (terminal_ref, terminal_h) = spawn_terminal_probe(&world, &bus);

    let (p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart),
    );
    let (b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart)
            .with_execute_result(Err(terminal_error("insufficient balance"))),
    );
    let (o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        ),
    );

    let ctx = context_for(3);
    let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
        context: ctx,
        payload: vec![42],
    });

    wait_until(TIMEOUT, || query_terminal_counts(&terminal_ref).failed >= 1);

    assert_eq!(
        query_state(&p_ref),
        ParticipantState {
            executed_count: 1,
            compensated_count: 0
        }
    );
    assert_eq!(
        query_state(&b_ref),
        ParticipantState {
            executed_count: 1,
            compensated_count: 0
        }
    );
    assert_eq!(
        query_state(&o_ref),
        ParticipantState {
            executed_count: 0,
            compensated_count: 0
        }
    );
    shutdown(p_h);
    shutdown(b_h);
    shutdown(o_h);
    shutdown(terminal_h);
}

// ===========================================================================
// Test 4: Order fails Terminal after both succeed
// ===========================================================================

#[test]
fn order_fails_terminal_after_both_succeed() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();
    let _resolver = bus.attach_terminal_resolver(test_policy(), "e2e-resolver");
    let (terminal_ref, terminal_h) = spawn_terminal_probe(&world, &bus);

    let (p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart),
    );
    let (b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart),
    );
    let (o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        )
        .with_execute_result(Err(terminal_error("order rejected"))),
    );

    let ctx = context_for(4);
    let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
        context: ctx,
        payload: vec![42],
    });

    wait_until(TIMEOUT, || query_terminal_counts(&terminal_ref).failed >= 1);

    assert_eq!(
        query_state(&p_ref),
        ParticipantState {
            executed_count: 1,
            compensated_count: 0
        }
    );
    assert_eq!(
        query_state(&b_ref),
        ParticipantState {
            executed_count: 1,
            compensated_count: 0
        }
    );
    assert_eq!(
        query_state(&o_ref),
        ParticipantState {
            executed_count: 1,
            compensated_count: 0
        }
    );
    shutdown(p_h);
    shutdown(b_h);
    shutdown(o_h);
    shutdown(terminal_h);
}

// ===========================================================================
// Test 5: RequireCompensation triggers full compensation chain
// ===========================================================================

#[test]
fn order_fails_require_compensation_triggers_full_compensation() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();
    let _resolver = bus.attach_terminal_resolver(test_policy(), "e2e-resolver");
    let (terminal_ref, terminal_h) = spawn_terminal_probe(&world, &bus);

    let (p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart),
    );
    let (b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart),
    );
    let (o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        )
        .with_execute_result(Err(require_compensation_error("order partially created"))),
    );

    let ctx = context_for(5);
    let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
        context: ctx,
        payload: vec![42],
    });

    wait_until(TIMEOUT, || {
        let counts = query_terminal_counts(&terminal_ref);
        counts.failed >= 1 || counts.quarantined >= 1
    });

    let p = query_state(&p_ref);
    let b = query_state(&b_ref);
    let o = query_state(&o_ref);

    assert_eq!(p.executed_count, 1);
    assert_eq!(b.executed_count, 1);
    assert_eq!(o.executed_count, 1);
    assert_eq!(p.compensated_count, 1, "position should be compensated");
    assert_eq!(b.compensated_count, 1, "balance should be compensated");
    assert_eq!(o.compensated_count, 0, "order is Failed, not Completed");
    shutdown(p_h);
    shutdown(b_h);
    shutdown(o_h);
    shutdown(terminal_h);
}

// ===========================================================================
// Test 6: Position compensation fails Terminal -> SagaFailed
// ===========================================================================

#[test]
fn position_compensation_fails_terminal_causes_saga_failed() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();
    let _resolver = bus.attach_terminal_resolver(test_policy(), "e2e-resolver");
    let (terminal_ref, terminal_h) = spawn_terminal_probe(&world, &bus);

    let (p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart)
            .with_compensate_result(Err(CompensationError::Terminal {
                reason: "cannot undo position".into(),
            })),
    );
    let (_b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart)
            .with_execute_result(Err(require_compensation_error("balance check fatal"))),
    );
    let (_o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        ),
    );

    let ctx = context_for(6);
    let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
        context: ctx,
        payload: vec![42],
    });

    wait_until(TIMEOUT, || {
        let counts = query_terminal_counts(&terminal_ref);
        counts.failed >= 1 || counts.quarantined >= 1
    });

    wait_until(TIMEOUT, || query_state(&p_ref).compensated_count >= 1);

    assert_eq!(
        query_state(&p_ref).compensated_count,
        1,
        "compensation attempted"
    );
    shutdown(p_h);
    shutdown(b_h);
    shutdown(o_h);
    shutdown(terminal_h);
}

// ===========================================================================
// Test 7: Balance compensation fails Ambiguous -> SagaQuarantined
// ===========================================================================

#[test]
fn balance_compensation_fails_ambiguous_causes_quarantine() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();
    let _resolver = bus.attach_terminal_resolver(test_policy(), "e2e-resolver");
    let (terminal_ref, terminal_h) = spawn_terminal_probe(&world, &bus);

    let (_p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart),
    );
    let (_b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart)
            .with_compensate_result(Err(CompensationError::Ambiguous {
                reason: "partial rollback".into(),
            })),
    );
    let (_o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        )
        .with_execute_result(Err(require_compensation_error("order failed"))),
    );

    let ctx = context_for(7);
    let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
        context: ctx,
        payload: vec![42],
    });

    wait_until(TIMEOUT, || {
        query_terminal_counts(&terminal_ref).quarantined >= 1
    });
    shutdown(p_h);
    shutdown(b_h);
    shutdown(o_h);
    shutdown(terminal_h);
}

// ===========================================================================
// Test 8: Balance compensation fails SafeToRetry -> SagaFailed
// ===========================================================================

#[test]
fn balance_compensation_fails_safe_to_retry_causes_failed() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();
    let _resolver = bus.attach_terminal_resolver(test_policy(), "e2e-resolver");
    let (terminal_ref, terminal_h) = spawn_terminal_probe(&world, &bus);

    let (_p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart),
    );
    let (_b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart)
            .with_compensate_result(Err(CompensationError::SafeToRetry {
                reason: "transient".into(),
            })),
    );
    let (_o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        )
        .with_execute_result(Err(require_compensation_error("order failed"))),
    );

    let ctx = context_for(8);
    let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
        context: ctx,
        payload: vec![42],
    });

    wait_until(TIMEOUT, || query_terminal_counts(&terminal_ref).failed >= 1);
    shutdown(p_h);
    shutdown(b_h);
    shutdown(o_h);
    shutdown(terminal_h);
}

// ===========================================================================
// Test 9: Position panics -> SagaQuarantined
// ===========================================================================

#[test]
fn position_panics_emits_quarantined() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();
    let _resolver = bus.attach_terminal_resolver(test_policy(), "e2e-resolver");
    let (terminal_ref, terminal_h) = spawn_terminal_probe(&world, &bus);

    // Subscribe balance and order as normal actors
    let (_b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart),
    );
    let (_o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        ),
    );

    // Position: NOT subscribed -- driven manually with panic quarantine
    let mut position =
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart).with_panic();
    position.attach_bus(bus.clone());

    let ctx = context_for(9);
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_participant_phase_with_panic_quarantine(
            &mut position,
            &ctx,
            ActiveSagaExecutionPhase::StepExecution,
            |actor| {
                let _ = actor.execute_step(&ctx, &[]);
            },
        );
    }));

    wait_until(TIMEOUT, || {
        query_terminal_counts(&terminal_ref).quarantined >= 1
    });
    assert!(query_terminal_counts(&terminal_ref).quarantined >= 1);
    shutdown(b_h);
    shutdown(o_h);
    shutdown(terminal_h);
}

// ===========================================================================
// Test 10: Balance panics after Position succeeds
// ===========================================================================

#[test]
fn balance_panics_after_position_succeeds() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();
    let _resolver = bus.attach_terminal_resolver(test_policy(), "e2e-resolver");
    let (terminal_ref, terminal_h) = spawn_terminal_probe(&world, &bus);

    let (p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart),
    );
    let (_o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        ),
    );

    // Balance: NOT subscribed -- driven manually
    let mut balance =
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart).with_panic();
    balance.attach_bus(bus.clone());

    let ctx = context_for(10);
    let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
        context: ctx.clone(),
        payload: vec![42],
    });

    // Give position a moment to process
    std::thread::sleep(Duration::from_millis(100));

    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_participant_phase_with_panic_quarantine(
            &mut balance,
            &ctx,
            ActiveSagaExecutionPhase::StepExecution,
            |actor| {
                let _ = actor.execute_step(&ctx, &[]);
            },
        );
    }));

    wait_until(TIMEOUT, || {
        query_terminal_counts(&terminal_ref).quarantined >= 1
    });
    assert_eq!(query_state(&p_ref).executed_count, 1);
    shutdown(p_h);
    shutdown(o_h);
    shutdown(terminal_h);
}

// ===========================================================================
// Test 11: Order panics after both succeed
// ===========================================================================

#[test]
fn order_panics_after_both_succeed() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();
    let _resolver = bus.attach_terminal_resolver(test_policy(), "e2e-resolver");
    let (terminal_ref, terminal_h) = spawn_terminal_probe(&world, &bus);

    let (p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart),
    );
    let (b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart),
    );

    // Order: NOT subscribed -- driven manually
    let mut order = ConfigurableParticipant::new(
        STEP_ORDER,
        DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
    )
    .with_panic();
    order.attach_bus(bus.clone());

    let ctx = context_for(11);
    let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
        context: ctx.clone(),
        payload: vec![42],
    });

    // Give participants a moment to process
    std::thread::sleep(Duration::from_millis(100));

    assert_eq!(query_state(&p_ref).executed_count, 1);
    assert_eq!(query_state(&b_ref).executed_count, 1);

    let order_ctx = context_for(11).next_step(STEP_ORDER.into());
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_participant_phase_with_panic_quarantine(
            &mut order,
            &order_ctx,
            ActiveSagaExecutionPhase::StepExecution,
            |actor| {
                let _ = actor.execute_step(&order_ctx, &[]);
            },
        );
    }));

    wait_until(TIMEOUT, || {
        query_terminal_counts(&terminal_ref).quarantined >= 1
    });
    assert_eq!(
        query_state(&p_ref).compensated_count,
        0,
        "no compensation from panic"
    );
    assert_eq!(
        query_state(&b_ref).compensated_count,
        0,
        "no compensation from panic"
    );
    shutdown(p_h);
    shutdown(b_h);
    shutdown(terminal_h);
}

// ===========================================================================
// Test 12: Idempotency
// ===========================================================================

#[test]
fn duplicate_saga_started_is_deduped() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();
    let _resolver = bus.attach_terminal_resolver(test_policy(), "e2e-resolver");
    let (terminal_ref, terminal_h) = spawn_terminal_probe(&world, &bus);

    let (p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart),
    );
    let (b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart),
    );
    let (o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        ),
    );

    let ctx = context_for(12);
    let event = SagaChoreographyEvent::SagaStarted {
        context: ctx,
        payload: vec![42],
    };
    let _ = bus.publish(event.clone());
    let _ = bus.publish(event);

    wait_until(TIMEOUT, || {
        query_terminal_counts(&terminal_ref).completed >= 1
    });

    assert_eq!(query_state(&p_ref).executed_count, 1);
    assert_eq!(query_state(&b_ref).executed_count, 1);
    assert_eq!(query_state(&o_ref).executed_count, 1);
    shutdown(p_h);
    shutdown(b_h);
    shutdown(o_h);
    shutdown(terminal_h);
}

// ===========================================================================
// Test 13: AllOf partial dependency does not fire
// ===========================================================================

#[test]
fn order_does_not_fire_on_partial_dependency() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();

    let (o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        ),
    );

    let ctx = context_for(13);

    // Only one StepCompleted
    let _ = bus.publish(SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(STEP_POSITION.into()),
        output: vec![],
        saga_input: vec![42],
        compensation_available: false,
    });

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        query_state(&o_ref).executed_count,
        0,
        "should not fire with only one dependency"
    );

    // Second StepCompleted
    let _ = bus.publish(SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(STEP_BALANCE.into()),
        output: vec![],
        saga_input: vec![42],
        compensation_available: false,
    });

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        query_state(&o_ref).executed_count,
        1,
        "should fire once both dependencies met"
    );
    shutdown(o_h);
}

// ===========================================================================
// Test 14: Terminal latch
// ===========================================================================

#[test]
fn terminal_latch_prevents_duplicate_events() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();
    let _resolver = bus.attach_terminal_resolver(test_policy(), "e2e-resolver");
    let (terminal_ref, terminal_h) = spawn_terminal_probe(&world, &bus);

    let (_p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart),
    );
    let (_b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart),
    );
    let (_o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        ),
    );

    let ctx = context_for(14);
    let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
        context: ctx.clone(),
        payload: vec![42],
    });

    wait_until(TIMEOUT, || {
        query_terminal_counts(&terminal_ref).completed >= 1
    });
    assert_eq!(query_terminal_counts(&terminal_ref).completed, 1);

    // Publish extra StepCompleted for same saga
    let _ = bus.publish(SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step("extra_step".into()),
        output: vec![],
        saga_input: vec![],
        compensation_available: false,
    });

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        query_terminal_counts(&terminal_ref).completed,
        1,
        "terminal latch should prevent duplicates"
    );
    shutdown(p_h);
    shutdown(b_h);
    shutdown(o_h);
    shutdown(terminal_h);
}

// ===========================================================================
// Test 15: Duplicate dependency replay after execution does not re-fire order
// ===========================================================================

#[test]
fn duplicate_dependency_completion_after_order_executes_is_deduped() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();
    let _resolver = bus.attach_terminal_resolver(test_policy(), "e2e-resolver");
    let (terminal_ref, terminal_h) = spawn_terminal_probe(&world, &bus);

    let (_p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart),
    );
    let (_b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart),
    );
    let (o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        ),
    );

    let ctx = context_for(15);
    let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
        context: ctx.clone(),
        payload: vec![42],
    });

    wait_until(TIMEOUT, || {
        query_terminal_counts(&terminal_ref).completed >= 1
    });
    assert_eq!(query_state(&o_ref).executed_count, 1);

    let _ = bus.publish(SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(STEP_BALANCE.into()),
        output: vec![],
        saga_input: vec![42],
        compensation_available: true,
    });

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        query_state(&o_ref).executed_count,
        1,
        "duplicate dependency replay must not re-execute the dependent actor"
    );
    assert_eq!(query_terminal_counts(&terminal_ref).completed, 1);
    shutdown(p_h);
    shutdown(b_h);
    shutdown(o_h);
    shutdown(terminal_h);
}

// ===========================================================================
// Test 16: Duplicate compensation request does not re-run compensation
// ===========================================================================

#[test]
fn duplicate_compensation_request_is_deduped() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();

    let (p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart),
    );

    let ctx = context_for(16);
    let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
        context: ctx.clone(),
        payload: vec![42],
    });

    wait_until(TIMEOUT, || query_state(&p_ref).executed_count >= 1);
    assert_eq!(query_state(&p_ref).compensated_count, 0);

    let compensation = SagaChoreographyEvent::CompensationRequested {
        context: ctx.next_step(STEP_ORDER.into()),
        failed_step: STEP_ORDER.into(),
        reason: "order failed after partial side effects".into(),
        steps_to_compensate: vec![STEP_POSITION.into()],
    };
    let _ = bus.publish(compensation.clone());
    let _ = bus.publish(compensation);

    wait_until(TIMEOUT, || query_state(&p_ref).compensated_count >= 1);
    assert_eq!(
        query_state(&p_ref).compensated_count,
        1,
        "duplicate compensation requests must not re-run compensation"
    );
    shutdown(p_h);
}

// ===========================================================================
// Test 17: Duplicate position approval does not double-fire order
// ===========================================================================

#[test]
fn duplicate_position_approval_before_balance_keeps_order_exactly_once() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();

    let (p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart),
    );
    let (b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart),
    );
    let (o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        ),
    );

    let ctx = context_for(17);
    let _ = bus.publish(SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(STEP_POSITION.into()),
        output: vec![],
        saga_input: vec![42],
        compensation_available: true,
    });
    let _ = bus.publish(SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(STEP_POSITION.into()),
        output: vec![],
        saga_input: vec![42],
        compensation_available: true,
    });

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(query_state(&o_ref).executed_count, 0);

    let _ = bus.publish(SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(STEP_BALANCE.into()),
        output: vec![],
        saga_input: vec![42],
        compensation_available: true,
    });

    wait_until(TIMEOUT, || query_state(&o_ref).executed_count >= 1);
    assert_eq!(query_state(&p_ref).executed_count, 0);
    assert_eq!(query_state(&b_ref).executed_count, 0);
    assert_eq!(query_state(&o_ref).executed_count, 1);

    let _ = bus.publish(SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(STEP_POSITION.into()),
        output: vec![],
        saga_input: vec![42],
        compensation_available: true,
    });

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(query_state(&o_ref).executed_count, 1);
    shutdown(p_h);
    shutdown(b_h);
    shutdown(o_h);
}

// ===========================================================================
// Test 18: Duplicate balance approval does not double-fire order
// ===========================================================================

#[test]
fn duplicate_balance_approval_before_position_keeps_order_exactly_once() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();

    let (p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart),
    );
    let (b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart),
    );
    let (o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        ),
    );

    let ctx = context_for(18);
    let _ = bus.publish(SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(STEP_BALANCE.into()),
        output: vec![],
        saga_input: vec![42],
        compensation_available: true,
    });
    let _ = bus.publish(SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(STEP_BALANCE.into()),
        output: vec![],
        saga_input: vec![42],
        compensation_available: true,
    });

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(query_state(&o_ref).executed_count, 0);

    let _ = bus.publish(SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(STEP_POSITION.into()),
        output: vec![],
        saga_input: vec![42],
        compensation_available: true,
    });

    wait_until(TIMEOUT, || query_state(&o_ref).executed_count >= 1);
    assert_eq!(query_state(&p_ref).executed_count, 0);
    assert_eq!(query_state(&b_ref).executed_count, 0);
    assert_eq!(query_state(&o_ref).executed_count, 1);

    let _ = bus.publish(SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(STEP_BALANCE.into()),
        output: vec![],
        saga_input: vec![42],
        compensation_available: true,
    });

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(query_state(&o_ref).executed_count, 1);
    shutdown(p_h);
    shutdown(b_h);
    shutdown(o_h);
}

// ===========================================================================
// Test 19: Duplicate approvals on both sides still produce one order
// ===========================================================================

#[test]
fn duplicate_both_approvals_still_produce_single_order_execution() {
    let world = TestWorld::new();
    let bus = SagaChoreographyBus::new();
    let _resolver = bus.attach_terminal_resolver(test_policy(), "e2e-resolver");
    let (terminal_ref, terminal_h) = spawn_terminal_probe(&world, &bus);

    let (p_ref, p_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_POSITION, DependencySpec::OnSagaStart),
    );
    let (b_ref, b_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(STEP_BALANCE, DependencySpec::OnSagaStart),
    );
    let (o_ref, o_h) = spawn_and_subscribe(
        &world,
        &bus,
        ConfigurableParticipant::new(
            STEP_ORDER,
            DependencySpec::AllOf(&[STEP_POSITION, STEP_BALANCE]),
        ),
    );

    let ctx = context_for(19);
    let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
        context: ctx.clone(),
        payload: vec![42],
    });

    wait_until(TIMEOUT, || {
        query_terminal_counts(&terminal_ref).completed >= 1
    });
    assert_eq!(query_state(&p_ref).executed_count, 1);
    assert_eq!(query_state(&b_ref).executed_count, 1);
    assert_eq!(query_state(&o_ref).executed_count, 1);

    let duplicate_position = SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(STEP_POSITION.into()),
        output: vec![],
        saga_input: vec![42],
        compensation_available: true,
    };
    let duplicate_balance = SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(STEP_BALANCE.into()),
        output: vec![],
        saga_input: vec![42],
        compensation_available: true,
    };
    let _ = bus.publish(duplicate_position.clone());
    let _ = bus.publish(duplicate_balance.clone());
    let _ = bus.publish(duplicate_position);
    let _ = bus.publish(duplicate_balance);

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(query_state(&p_ref).executed_count, 1);
    assert_eq!(query_state(&b_ref).executed_count, 1);
    assert_eq!(query_state(&o_ref).executed_count, 1);
    assert_eq!(query_terminal_counts(&terminal_ref).completed, 1);
    shutdown(p_h);
    shutdown(b_h);
    shutdown(o_h);
    shutdown(terminal_h);
}
