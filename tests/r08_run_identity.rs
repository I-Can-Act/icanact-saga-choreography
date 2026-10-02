//! R08 / ADR-0001 integration gate (T08X): run identity is enforced consistently by the
//! participant adapters (sync, async, workflow), the durable stores, the terminal resolver
//! and the bus reply/terminal cache.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use icanact_core::local_sync;
use icanact_saga_choreography::define_saga_workflow_contract;
use icanact_saga_choreography::durability::apply_async_participant_saga_ingress;
use icanact_saga_choreography::durability::lmdb::{LmdbDedupe, LmdbJournal};
use icanact_saga_choreography::{
    AsyncSagaParticipant, CompensationError, CompensationOutput, FailureAuthority,
    HasSagaParticipantSupport, HasSagaWorkflowParticipants, InMemoryTerminalResolverJournal,
    LmdbTerminalResolverJournal, ParticipantDedupeStore, ParticipantEvent, ParticipantJournal,
    PeerId, RunIdentityError, RunIncarnation, RunKey, SagaBoxFuture, SagaChoreographyBus,
    SagaChoreographyEvent, SagaContext, SagaId, SagaParticipant, SagaParticipantSupport,
    SagaReplyToHandle, SagaReplyToResult, SagaTerminalOutcome, SagaWorkflowParticipant, StepError,
    StepOutput, SuccessCriteria, TERMINAL_RESOLVER_STEP, TerminalPolicy, TerminalResolver,
    TerminalResolverJournal, drive_workflow_scenario, handle_saga_event_with_emit,
};

const ORDER: &str = "order_lifecycle";
const PAYMENT: &str = "payment_lifecycle";
const STEP: &str = "risk_check";

// ---------------------------------------------------------------------------------------
// Event builders
// ---------------------------------------------------------------------------------------

fn ctx(saga_type: &str, id: u64, incarnation: u64, trace: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(id),
        saga_type: saga_type.into(),
        step_name: STEP.into(),
        correlation_id: id,
        causation_id: id,
        trace_id: trace,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: incarnation,
        event_timestamp_millis: incarnation,
    }
}

fn run_of(saga_type: &str, id: u64, incarnation: u64) -> RunKey {
    RunKey::new(saga_type, SagaId::new(id), RunIncarnation::new(incarnation))
}

fn start(saga_type: &str, id: u64, incarnation: u64, trace: u64) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: ctx(saga_type, id, incarnation, trace),
        payload: vec![7],
    }
}

fn terminal_completed(saga_type: &str, id: u64, incarnation: u64) -> SagaChoreographyEvent {
    let mut context = ctx(saga_type, id, incarnation, 900);
    context.step_name = TERMINAL_RESOLVER_STEP.into();
    SagaChoreographyEvent::SagaCompleted { context }
}

fn terminal_failed(saga_type: &str, id: u64, incarnation: u64) -> SagaChoreographyEvent {
    let mut context = ctx(saga_type, id, incarnation, 901);
    context.step_name = TERMINAL_RESOLVER_STEP.into();
    SagaChoreographyEvent::SagaFailed {
        context,
        reason: "late contradictory terminal".into(),
        failure: None,
    }
}

fn compensation_request(saga_type: &str, id: u64, incarnation: u64) -> SagaChoreographyEvent {
    icanact_saga_choreography::compensation_requested(
        ctx(saga_type, id, incarnation, 902),
        "downstream",
        "late rollback of an old run",
        vec![STEP.to_string()],
    )
}

fn step_completed(saga_type: &str, id: u64, incarnation: u64) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepCompleted {
        context: ctx(saga_type, id, incarnation, 903),
        output: vec![1],
        saga_input: vec![7],
        compensation_available: true,
    }
}

// ---------------------------------------------------------------------------------------
// Participant harnesses: one durable (LMDB) actor per adapter kind, all counting effects
// per RunKey in an `EffectLedger`-style vector.
// ---------------------------------------------------------------------------------------

type Support = SagaParticipantSupport<LmdbJournal, LmdbDedupe>;

fn open_support(dir: &Path) -> Support {
    let journal = LmdbJournal::open(&dir.join("journal")).expect("journal opens");
    let dedupe = LmdbDedupe::open(&dir.join("dedupe")).expect("dedupe opens");
    SagaParticipantSupport::new(journal, dedupe)
}

trait Harness: HasSagaParticipantSupport<Journal = LmdbJournal, Dedupe = LmdbDedupe> + Sized {
    fn open(dir: &Path) -> Self;
    fn feed(&mut self, event: SagaChoreographyEvent);
    fn executed(&self) -> &[RunKey];
    fn compensated(&self) -> &[RunKey];
}

#[derive(Default)]
struct Calls {
    executed: Vec<RunKey>,
    compensated: Vec<RunKey>,
}

fn ok_step() -> StepOutput {
    StepOutput::Completed {
        output: vec![1],
        compensation_data: vec![9],
    }
}

struct SyncActor {
    saga: Support,
    calls: Calls,
}

impl HasSagaParticipantSupport for SyncActor {
    type Journal = LmdbJournal;
    type Dedupe = LmdbDedupe;
    fn saga_support(&self) -> &Support {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut Support {
        &mut self.saga
    }
}

impl SagaParticipant for SyncActor {
    type Error = String;
    fn step_name(&self) -> &str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[ORDER, PAYMENT]
    }
    fn execute_step(&mut self, c: &SagaContext, _: &[u8]) -> Result<StepOutput, StepError> {
        self.calls.executed.push(c.run_key());
        Ok(ok_step())
    }
    fn compensate_step(
        &mut self,
        c: &SagaContext,
        _: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        self.calls.compensated.push(c.run_key());
        Ok(CompensationOutput::Completed)
    }
}

impl Harness for SyncActor {
    fn open(dir: &Path) -> Self {
        Self {
            saga: open_support(dir),
            calls: Calls::default(),
        }
    }
    fn feed(&mut self, event: SagaChoreographyEvent) {
        handle_saga_event_with_emit(self, event, |_| {});
    }
    fn executed(&self) -> &[RunKey] {
        &self.calls.executed
    }
    fn compensated(&self) -> &[RunKey] {
        &self.calls.compensated
    }
}

struct AsyncActor {
    saga: Support,
    calls: Calls,
    rt: tokio::runtime::Runtime,
}

impl HasSagaParticipantSupport for AsyncActor {
    type Journal = LmdbJournal;
    type Dedupe = LmdbDedupe;
    fn saga_support(&self) -> &Support {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut Support {
        &mut self.saga
    }
}

impl AsyncSagaParticipant for AsyncActor {
    type Error = String;
    fn step_name(&self) -> &str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[ORDER, PAYMENT]
    }
    fn execute_step<'a>(
        &'a mut self,
        c: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<StepOutput, StepError>> {
        self.calls.executed.push(c.run_key());
        Box::pin(async { Ok(ok_step()) })
    }
    fn compensate_step<'a>(
        &'a mut self,
        c: &'a SagaContext,
        _: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<CompensationOutput, CompensationError>> {
        self.calls.compensated.push(c.run_key());
        Box::pin(async { Ok(CompensationOutput::Completed) })
    }
}

impl Harness for AsyncActor {
    fn open(dir: &Path) -> Self {
        Self {
            saga: open_support(dir),
            calls: Calls::default(),
            rt: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime"),
        }
    }
    fn feed(&mut self, event: SagaChoreographyEvent) {
        // The runtime is moved out for the call so `self` can be borrowed mutably.
        let rt = std::mem::replace(
            &mut self.rt,
            tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("runtime"),
        );
        rt.block_on(apply_async_participant_saga_ingress(
            self,
            event,
            |_, _| {},
            |_| {},
        ));
        self.rt = rt;
    }
    fn executed(&self) -> &[RunKey] {
        &self.calls.executed
    }
    fn compensated(&self) -> &[RunKey] {
        &self.calls.compensated
    }
}

struct WorkflowActor {
    saga: Support,
    calls: Calls,
}

struct WorkflowStep;

impl SagaWorkflowParticipant<WorkflowActor> for WorkflowStep {
    fn step_name(&self) -> &'static str {
        STEP
    }
    fn saga_types(&self) -> &[&'static str] {
        &[ORDER, PAYMENT]
    }
    fn execute_step(
        &self,
        actor: &mut WorkflowActor,
        c: &SagaContext,
        _: &[u8],
    ) -> Result<StepOutput, StepError> {
        actor.calls.executed.push(c.run_key());
        Ok(ok_step())
    }
    fn compensate_step(
        &self,
        actor: &mut WorkflowActor,
        c: &SagaContext,
        _: &[u8],
    ) -> Result<CompensationOutput, CompensationError> {
        actor.calls.compensated.push(c.run_key());
        Ok(CompensationOutput::Completed)
    }
}

static WORKFLOW_STEP: WorkflowStep = WorkflowStep;
static WORKFLOW_STEPS: [&dyn SagaWorkflowParticipant<WorkflowActor>; 1] = [&WORKFLOW_STEP];

impl HasSagaWorkflowParticipants for WorkflowActor {
    fn saga_workflows() -> &'static [&'static dyn SagaWorkflowParticipant<Self>] {
        &WORKFLOW_STEPS
    }
}

impl HasSagaParticipantSupport for WorkflowActor {
    type Journal = LmdbJournal;
    type Dedupe = LmdbDedupe;
    fn saga_support(&self) -> &Support {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut Support {
        &mut self.saga
    }
}

impl Harness for WorkflowActor {
    fn open(dir: &Path) -> Self {
        Self {
            saga: open_support(dir),
            calls: Calls::default(),
        }
    }
    fn feed(&mut self, event: SagaChoreographyEvent) {
        drive_workflow_scenario(self, [event]);
    }
    fn executed(&self) -> &[RunKey] {
        &self.calls.executed
    }
    fn compensated(&self) -> &[RunKey] {
        &self.calls.compensated
    }
}

fn started_rows<H: Harness>(actor: &H, run: &RunKey) -> usize {
    actor
        .saga_support()
        .journal
        .read_run(run)
        .expect("read_run")
        .iter()
        .filter(|row| matches!(row.event, ParticipantEvent::StepExecutionStarted { .. }))
        .count()
}

fn tombstoned_incarnations<H: Harness>(actor: &H, saga_type: &str, id: u64) -> Vec<u64> {
    let mut found: Vec<u64> = actor
        .saga_support()
        .journal
        .run_tombstones(saga_type, SagaId::new(id))
        .expect("run_tombstones")
        .iter()
        .map(|t| t.run().incarnation().get())
        .collect();
    found.sort_unstable();
    found
}

/// The participant-side half of the gate; every adapter must satisfy all of it.
fn participant_scenario<H: Harness>(kind: &str) {
    let temp = tempfile::tempdir().expect("tempdir");
    let dir = temp.path();
    let now = SagaContext::now_millis();
    let base = now.saturating_sub(120_000);
    let (order_1, pay_1) = (run_of(ORDER, 7, base), run_of(PAYMENT, 7, base));
    let order_2 = run_of(ORDER, 7, base + 1_000);

    // -- two saga types share numeric SagaId 7; an active start is replayed with a re-minted trace --
    let mut actor = H::open(dir);
    actor.feed(start(ORDER, 7, base, 1));
    actor.feed(start(PAYMENT, 7, base, 1));
    actor.feed(start(ORDER, 7, base, 777));
    actor.feed(start(PAYMENT, 7, base, 778));
    assert_eq!(
        actor.executed(),
        [order_1.clone(), pay_1.clone()],
        "{kind}: each (type,id,incarnation) executes exactly once; a new-trace replay must not re-execute"
    );
    assert_eq!(
        started_rows(&actor, &order_1),
        1,
        "{kind}: replay appended journal rows"
    );
    assert_eq!(started_rows(&actor, &pay_1), 1, "{kind}");
    assert!(
        actor.saga_support().stats.snapshot().duplicate_events >= 2,
        "{kind}: duplicates must be counted, not silently dropped"
    );
    let mut runs = actor.saga_support().journal.list_runs().expect("list_runs");
    runs.sort();
    let mut expected = vec![order_1.clone(), pay_1.clone()];
    expected.sort();
    assert_eq!(
        runs, expected,
        "{kind}: journal must hold one partition per type"
    );
    let dedupe = &actor.saga_support().dedupe;
    let mut dedupe_runs = dedupe.list_runs().expect("dedupe list_runs");
    dedupe_runs.sort();
    assert_eq!(
        dedupe_runs, expected,
        "{kind}: dedupe must hold one partition per type"
    );
    assert!(!dedupe.keys_run(&order_1).expect("keys").is_empty());
    assert!(!dedupe.keys_run(&pay_1).expect("keys").is_empty());

    // -- restart: durable state must keep both runs idempotent and separate --
    drop(actor);
    let mut actor = H::open(dir);
    actor.feed(start(ORDER, 7, base, 4_001));
    actor.feed(start(PAYMENT, 7, base, 4_002));
    assert!(
        actor.executed().is_empty(),
        "{kind}: after reopen a replayed start must not re-execute (got {:?})",
        actor.executed()
    );
    assert_eq!(
        started_rows(&actor, &order_1),
        1,
        "{kind}: reopen replay grew journal"
    );
    assert_eq!(started_rows(&actor, &pay_1), 1, "{kind}");

    // -- order run 1 terminates; payment run (same numeric id) is untouched --
    actor.feed(terminal_completed(ORDER, 7, base));
    assert_eq!(tombstoned_incarnations(&actor, ORDER, 7), [base], "{kind}");
    assert!(
        tombstoned_incarnations(&actor, PAYMENT, 7).is_empty(),
        "{kind}: order terminal must not tombstone the payment run"
    );
    assert!(
        actor
            .saga_support()
            .journal
            .read_run(&order_1)
            .expect("read")
            .is_empty(),
        "{kind}: finalize must delete the run's rows"
    );
    assert!(
        !actor
            .saga_support()
            .journal
            .read_run(&pay_1)
            .expect("read")
            .is_empty(),
        "{kind}: finalize of one type deleted another type's rows"
    );
    assert!(
        actor
            .saga_support()
            .dedupe
            .keys_run(&pay_1)
            .is_ok_and(|k| !k.is_empty()),
        "{kind}: payment dedupe marks vanished"
    );

    // Late events and replays of the terminal run are inert, live and after restart.
    for round in 0..2 {
        actor.feed(compensation_request(ORDER, 7, base));
        actor.feed(step_completed(ORDER, 7, base));
        actor.feed(start(ORDER, 7, base, 5_000 + round));
        assert!(
            actor.executed().is_empty(),
            "{kind}: terminal run re-executed (round {round})"
        );
        assert!(
            actor.compensated().is_empty(),
            "{kind}: terminal run compensated (round {round})"
        );
        assert_eq!(
            tombstoned_incarnations(&actor, ORDER, 7),
            [base],
            "{kind}: tombstone must survive (round {round})"
        );
        if round == 0 {
            drop(actor);
            actor = H::open(dir);
        }
    }

    // -- SagaId reuse: a strictly newer incarnation after terminal is admitted and isolated --
    actor.feed(start(ORDER, 7, base + 1_000, 11));
    assert_eq!(
        actor.executed(),
        std::slice::from_ref(&order_2),
        "{kind}: reuse must execute once"
    );
    assert_eq!(started_rows(&actor, &order_2), 1, "{kind}");
    let rows_before = actor
        .saga_support()
        .journal
        .read_run(&order_2)
        .expect("read")
        .len();
    // Old run's late events (compensation, completion, contradictory terminal, replay).
    actor.feed(compensation_request(ORDER, 7, base));
    actor.feed(step_completed(ORDER, 7, base));
    actor.feed(terminal_failed(ORDER, 7, base));
    actor.feed(start(ORDER, 7, base, 6_000));
    assert_eq!(
        actor.executed(),
        std::slice::from_ref(&order_2),
        "{kind}: old-run event re-executed"
    );
    assert!(
        actor.compensated().is_empty(),
        "{kind}: old-run compensation reached new run"
    );
    assert_eq!(
        actor
            .saga_support()
            .journal
            .read_run(&order_2)
            .expect("read")
            .len(),
        rows_before,
        "{kind}: old-run events mutated the new run's journal"
    );
    assert_eq!(
        tombstoned_incarnations(&actor, ORDER, 7),
        [base],
        "{kind}: new run was finalized by old terminal"
    );

    // Old run's *new-run* terminal finalizes only the new incarnation.
    actor.feed(terminal_completed(ORDER, 7, base + 1_000));
    assert_eq!(
        tombstoned_incarnations(&actor, ORDER, 7),
        [base, base + 1_000],
        "{kind}"
    );
    assert!(
        !actor
            .saga_support()
            .journal
            .read_run(&pay_1)
            .expect("read")
            .is_empty(),
        "{kind}: payment run lost its rows"
    );

    // -- stale incarnation: loud rejection (counter), no effect --
    let executed_before = actor.executed().len();
    let stale_before = actor.saga_support().stats.snapshot().runs_rejected_stale;
    actor.feed(start(ORDER, 8, base + 2_000, 21));
    actor.feed(start(ORDER, 8, base + 1_000, 22));
    assert_eq!(
        actor.executed().len(),
        executed_before + 1,
        "{kind}: stale start must not execute"
    );
    assert_eq!(
        actor.saga_support().stats.snapshot().runs_rejected_stale,
        stale_before + 1,
        "{kind}: stale incarnation must be counted"
    );
    assert!(
        actor
            .saga_support()
            .journal
            .read_run(&run_of(ORDER, 8, base + 1_000))
            .expect("read")
            .is_empty(),
        "{kind}: stale run left journal rows"
    );
    // The stale fence is per saga type: an older incarnation of the *payment* id 8 is fine.
    actor.feed(start(PAYMENT, 8, base + 500, 23));
    assert_eq!(
        actor.executed().last(),
        Some(&run_of(PAYMENT, 8, base + 500)),
        "{kind}: stale fence leaked across saga types"
    );

    // -- expired incarnation: older than the 24h participant horizon, never admitted --
    let expired_before = actor.saga_support().stats.snapshot().runs_rejected_expired;
    let executed_before = actor.executed().len();
    actor.feed(start(ORDER, 9, now.saturating_sub(2 * 24 * 3_600_000), 31));
    assert_eq!(
        actor.executed().len(),
        executed_before,
        "{kind}: expired start must not execute"
    );
    assert_eq!(
        actor.saga_support().stats.snapshot().runs_rejected_expired,
        expired_before + 1,
        "{kind}: expired incarnation must be counted"
    );
    assert!(
        actor
            .saga_support()
            .journal
            .list_runs()
            .expect("list")
            .iter()
            .all(|r| r.saga_id() != SagaId::new(9)),
        "{kind}: expired run was recorded"
    );
}

// ---------------------------------------------------------------------------------------
// Resolver + bus
// ---------------------------------------------------------------------------------------

fn policy(saga_type: &str) -> TerminalPolicy {
    let mut required = HashSet::new();
    required.insert(STEP.into());
    TerminalPolicy::new(
        saga_type.into(),
        format!("{saga_type}/r08").into(),
        FailureAuthority::AnyParticipant,
        SuccessCriteria::AllOf(required),
        Duration::from_secs(600),
        Duration::from_secs(600),
        &[],
    )
}

type Terminals = Arc<Mutex<Vec<(RunKey, &'static str)>>>;

fn capture_terminals(bus: &SagaChoreographyBus, saga_type: &str) -> Terminals {
    let seen: Terminals = Arc::default();
    let sink = Arc::clone(&seen);
    let sub = bus.subscribe_fn(saga_type, move |event| {
        let kind = match event {
            SagaChoreographyEvent::SagaCompleted { .. } => "completed",
            SagaChoreographyEvent::SagaFailed { .. } => "failed",
            SagaChoreographyEvent::SagaQuarantined { .. } => "quarantined",
            _ => return true,
        };
        if let Ok(mut guard) = sink.lock() {
            guard.push((event.context().run_key(), kind));
        }
        true
    });
    std::mem::forget(sub); // lives as long as the bus
    seen
}

fn terminals_of(seen: &Terminals, run: &RunKey) -> Vec<&'static str> {
    seen.lock()
        .expect("lock")
        .iter()
        .filter(|(key, _)| key == run)
        .map(|(_, kind)| *kind)
        .collect()
}

fn wait_until(what: &str, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if pred() {
            return;
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!("timed out waiting for {what}");
}

/// Lets an in-flight (asynchronous) resolver settle so "nothing happened" can be asserted.
fn settle() {
    thread::sleep(Duration::from_millis(250));
}

enum Probe {
    Register(SagaChoreographyBus, RunKey, SagaReplyToHandle),
}

fn register_waiter(
    bus: &SagaChoreographyBus,
    run: &RunKey,
) -> (
    local_sync::PendingAsk<SagaReplyToResult>,
    local_sync::mpsc::ActorHandle,
) {
    let (addr, handle) = local_sync::mpsc::spawn(8, |Probe::Register(bus, run, reply)| {
        let _ = bus.register_terminal_reply_for_run(&run, reply);
    });
    let pending = addr
        .ask_delegated(|reply| Probe::Register(bus.clone(), run.clone(), reply))
        .expect("waiter registered");
    (pending, handle)
}

fn step_ok(saga_type: &str, id: u64, incarnation: u64) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepCompleted {
        context: ctx(saga_type, id, incarnation, 50).next_step(STEP.into()),
        output: vec![1],
        saga_input: vec![],
        compensation_available: false,
    }
}

fn step_fail(saga_type: &str, id: u64, incarnation: u64) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepFailed {
        context: ctx(saga_type, id, incarnation, 51).next_step(STEP.into()),
        participant_id: "risk".into(),
        error_code: None,
        error: "business failure".into(),
        requires_compensation: false,
    }
}

define_saga_workflow_contract! {
    struct OrderContract {
        saga_type: "order_lifecycle",
        first_step: risk_check,
        failure_authority: any (),
        required_steps: [risk_check],
        overall_timeout_ms: 600_000,
        stalled_timeout_ms: 600_000,
        steps: {
            risk_check => {
                participant: "risk",
                depends_on: on_start ()
            }
        }
    }
}

define_saga_workflow_contract! {
    struct PaymentContract {
        saga_type: "payment_lifecycle",
        first_step: risk_check,
        failure_authority: any (),
        required_steps: [risk_check],
        overall_timeout_ms: 600_000,
        stalled_timeout_ms: 600_000,
        steps: {
            risk_check => {
                participant: "risk",
                depends_on: on_start ()
            }
        }
    }
}

/// Bus + resolver half of the gate, parameterised over the durable resolver journal so the
/// restart half runs against both the in-memory and the LMDB journal.
fn resolver_and_bus_scenario<J: TerminalResolverJournal>(open: impl Fn(&str) -> Arc<J>) {
    let base = SagaContext::now_millis().saturating_sub(120_000);
    let journals: Vec<(&'static str, Arc<J>)> = [ORDER, PAYMENT]
        .into_iter()
        .map(|ty| (ty, open(ty)))
        .collect();
    let attach = |bus: &SagaChoreographyBus| {
        bus.register_workflow_contract_provider::<OrderContract>()
            .expect("order contract registers");
        bus.register_workflow_contract_provider::<PaymentContract>()
            .expect("payment contract registers");
        bus.register_bound_workflow_step(ORDER, STEP)
            .expect("order step binds");
        bus.register_bound_workflow_step(PAYMENT, STEP)
            .expect("payment step binds");
        bus.attach_durable_terminal_resolver_for_contract::<OrderContract, _>(
            "terminal-resolver",
            Arc::clone(&journals[0].1),
        )
        .expect("order resolver attaches");
        bus.attach_durable_terminal_resolver_for_contract::<PaymentContract, _>(
            "terminal-resolver",
            Arc::clone(&journals[1].1),
        )
        .expect("payment resolver attaches");
        // The start step's participants: lanes tagged with it receive SagaStarted.
        for saga_type in [ORDER, PAYMENT] {
            let lane = bus.subscribe_participant_fn(saga_type, &[STEP], |_event| true);
            std::mem::forget(lane); // lives as long as the bus
            // Participants are bound: a durable resolver publishes only once activated.
            bus.activate_terminal_resolver_recovery(saga_type)
                .expect("recovery activates");
        }
    };
    let (order_1, pay_1) = (run_of(ORDER, 7, base), run_of(PAYMENT, 7, base));
    let order_2 = run_of(ORDER, 7, base + 1_000);

    // ---- bus A ----
    let bus = SagaChoreographyBus::new();
    attach(&bus);
    let order_seen = capture_terminals(&bus, ORDER);
    let pay_seen = capture_terminals(&bus, PAYMENT);
    let (order_waiter, probe_a) = register_waiter(&bus, &order_1);
    let (pay_waiter, probe_b) = register_waiter(&bus, &pay_1);
    settle();

    bus.publish_strict(start(ORDER, 7, base, 1))
        .expect("start admitted");
    bus.publish_strict(start(PAYMENT, 7, base, 1))
        .expect("start admitted");
    bus.publish(step_ok(ORDER, 7, base));
    wait_until("order run 1 to complete", || {
        !terminals_of(&order_seen, &order_1).is_empty()
    });
    let reply = order_waiter
        .wait_timeout(Duration::from_secs(5))
        .expect("order waiter resolves")
        .expect("order reply ok");
    assert!(matches!(
        reply.outcome,
        SagaTerminalOutcome::Completed { .. }
    ));
    settle();
    assert_eq!(terminals_of(&order_seen, &order_1), ["completed"]);
    assert!(
        terminals_of(&pay_seen, &pay_1).is_empty(),
        "payment run (same numeric id) must not be resolved by the order run"
    );
    assert!(
        bus.take_terminal_outcome_for_run(&pay_1).is_none(),
        "bus cached the order outcome under the payment run"
    );
    assert!(matches!(
        bus.take_terminal_outcome_for_run(&order_1),
        Some(SagaTerminalOutcome::Completed { .. })
    ));

    // Replay with a re-minted trace, plus late step events of a terminal run: no new outcome.
    bus.publish_strict(start(ORDER, 7, base, 999))
        .expect("start admitted");
    bus.publish(step_fail(ORDER, 7, base));
    settle();
    assert_eq!(
        terminals_of(&order_seen, &order_1),
        ["completed"],
        "terminal run was contradicted"
    );
    assert!(bus.take_terminal_outcome_for_run(&order_1).is_none());

    // Reuse of the SagaId for a new incarnation fails independently of the completed run.
    bus.publish_strict(start(ORDER, 7, base + 1_000, 2))
        .expect("start admitted");
    bus.publish(step_fail(ORDER, 7, base + 1_000));
    wait_until("order run 2 to fail", || {
        !terminals_of(&order_seen, &order_2).is_empty()
    });
    assert_eq!(terminals_of(&order_seen, &order_2), ["failed"]);
    assert_eq!(terminals_of(&order_seen, &order_1), ["completed"]);
    assert!(matches!(
        bus.take_terminal_outcome_for_run(&order_2),
        Some(SagaTerminalOutcome::Failed { .. })
    ));
    assert!(
        bus.take_terminal_outcome_for_run(&order_1).is_none(),
        "old run's cache entry must not be recreated by the new run"
    );

    // A stale (older than newest known) incarnation is ignored loudly by the resolver
    // and never produces a terminal event.
    let stale = run_of(ORDER, 7, base - 500);
    bus.publish_strict(start(ORDER, 7, base - 500, 3))
        .expect("start admitted");
    bus.publish(step_ok(ORDER, 7, base - 500));
    settle();
    assert!(
        terminals_of(&order_seen, &stale).is_empty(),
        "stale run produced a terminal"
    );
    assert!(bus.take_terminal_outcome_for_run(&stale).is_none());
    assert!(terminals_of(&pay_seen, &pay_1).is_empty());
    probe_a.shutdown();
    drop((pay_waiter, bus));

    // ---- bus B: restart over the same durable resolver journals ----
    let bus = SagaChoreographyBus::new();
    attach(&bus);
    let order_seen = capture_terminals(&bus, ORDER);
    let pay_seen = capture_terminals(&bus, PAYMENT);
    let (pay_waiter, probe_c) = register_waiter(&bus, &pay_1);
    settle();
    for ty in [ORDER, PAYMENT] {
        bus.activate_terminal_resolver_recovery(ty)
            .expect("recovery activates");
    }
    settle();
    // Recovery may republish an unpublished outcome but never a contradictory one.
    assert!(
        terminals_of(&order_seen, &order_1)
            .iter()
            .all(|k| *k == "completed")
    );
    assert!(
        terminals_of(&order_seen, &order_2)
            .iter()
            .all(|k| *k == "failed")
    );
    assert!(
        terminals_of(&pay_seen, &pay_1).is_empty(),
        "restart invented a payment outcome"
    );
    let order_1_before = terminals_of(&order_seen, &order_1).len();
    let order_2_before = terminals_of(&order_seen, &order_2).len();

    // Restored terminal runs stay terminal; replays and late events change nothing.
    bus.publish_strict(start(ORDER, 7, base, 4_000))
        .expect("start admitted");
    bus.publish(step_fail(ORDER, 7, base));
    bus.publish_strict(start(ORDER, 7, base + 1_000, 4_001))
        .expect("start admitted");
    bus.publish(step_ok(ORDER, 7, base + 1_000));
    settle();
    assert_eq!(terminals_of(&order_seen, &order_1).len(), order_1_before);
    assert_eq!(terminals_of(&order_seen, &order_2).len(), order_2_before);
    assert!(
        terminals_of(&order_seen, &order_2)
            .iter()
            .all(|k| *k == "failed")
    );

    // The payment run with the same numeric id is still live after restart and completes once.
    bus.publish(step_ok(PAYMENT, 7, base));
    wait_until("payment run to complete", || {
        !terminals_of(&pay_seen, &pay_1).is_empty()
    });
    let reply = pay_waiter
        .wait_timeout(Duration::from_secs(5))
        .expect("payment waiter resolves")
        .expect("payment reply ok");
    assert!(matches!(
        reply.outcome,
        SagaTerminalOutcome::Completed { .. }
    ));
    settle();
    assert_eq!(terminals_of(&pay_seen, &pay_1), ["completed"]);
    assert!(matches!(
        bus.take_terminal_outcome_for_run(&pay_1),
        Some(SagaTerminalOutcome::Completed { .. })
    ));
    probe_b.shutdown();
    probe_c.shutdown();
}

/// Typed, non-silent rejection at the resolver entry point (`try_ingest_at`).
fn resolver_fences_scenario() {
    let now = SagaContext::now_millis();
    let base = now - 120_000;
    let mut resolver = TerminalResolver::new(policy(ORDER));

    resolver
        .try_ingest_at(&start(ORDER, 7, base + 1_000, 1), now)
        .expect("newest run admitted");
    let err = resolver
        .try_ingest_at(&start(ORDER, 7, base, 2), now)
        .expect_err("older incarnation must be rejected loudly");
    assert!(
        matches!(&err, RunIdentityError::StaleIncarnation { incoming, newest_known }
            if *incoming == run_of(ORDER, 7, base) && newest_known.get() == base + 1_000),
        "unexpected error {err:?}"
    );

    let two_hours = 2 * 3_600_000;
    let err = resolver
        .try_ingest_at(&start(ORDER, 9, now - two_hours, 3), now)
        .expect_err("incarnation beyond the replay horizon must be rejected loudly");
    assert!(
        matches!(err, RunIdentityError::ExpiredIncarnation { .. }),
        "unexpected error {err:?}"
    );

    // Same numeric id under another saga type is not a collision (resolver is per type: ignored).
    assert!(
        resolver
            .try_ingest_at(&start(PAYMENT, 7, base, 4), now)
            .expect("other saga type is not an error")
            .is_empty()
    );
    // An exact duplicate start with a new trace is idempotent: no events, no error.
    assert!(
        resolver
            .try_ingest_at(&start(ORDER, 7, base + 1_000, 5), now)
            .expect("duplicate start is not an error")
            .is_empty()
    );
}

#[test]
fn run_identity_fences_replay_reuse_and_cross_workflow_collisions() {
    participant_scenario::<SyncActor>("sync");
    participant_scenario::<AsyncActor>("async");
    participant_scenario::<WorkflowActor>("workflow");
    resolver_fences_scenario();
    resolver_and_bus_scenario(|_| Arc::new(InMemoryTerminalResolverJournal::default()));
    let temp = tempfile::tempdir().expect("tempdir");
    resolver_and_bus_scenario(|ty| {
        Arc::new(
            LmdbTerminalResolverJournal::open(&temp.path().join(ty))
                .expect("resolver journal opens"),
        )
    });

    // Sanity: the harness ids really collide numerically across types.
    let a = run_of(ORDER, 7, 1);
    let b = run_of(PAYMENT, 7, 1);
    assert_eq!(a.saga_id(), b.saga_id());
    assert_ne!(a, b);
}
