//! Safety regressions for bus admission, recovery activation, and delivery
//! failure handling. These assert the *safe* behavior.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use icanact_core::local_sync::{self, SyncActor};
use icanact_saga_choreography::*;

define_saga_workflow_contract! {
    struct BusContract {
        saga_type: "qa",
        first_step: a,
        failure_authority: any (),
        required_steps: [b],
        overall_timeout_ms: 60_000,
        stalled_timeout_ms: 60_000,
        steps: {
            a => { participant: "a", depends_on: on_start () },
            b => { participant: "b", depends_on: after [a] }
        }
    }
}

struct Accepter {
    saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
}

impl HasSagaParticipantSupport for Accepter {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;
    fn saga_support(&self) -> &SagaParticipantSupport<InMemoryJournal, InMemoryDedupe> {
        &self.saga
    }
    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<InMemoryJournal, InMemoryDedupe> {
        &mut self.saga
    }
}

impl SagaParticipant for Accepter {
    type Error = String;
    fn step_name(&self) -> &str {
        "a"
    }
    fn saga_types(&self) -> &[&'static str] {
        &["qa"]
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
        Ok(CompensationOutput::Completed)
    }
}

fn context_at(id: u64, step: &str, started_at: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId(id),
        saga_type: "qa".into(),
        step_name: step.into(),
        correlation_id: id,
        causation_id: 0,
        trace_id: id,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: [0; 32],
        saga_started_at_millis: started_at,
        event_timestamp_millis: started_at,
    }
}

fn context(id: u64, step: &str) -> SagaContext {
    context_at(id, step, SagaContext::now_millis())
}

fn started(ctx: &SagaContext) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: ctx.clone(),
        payload: vec![9],
    }
}

fn completed(ctx: &SagaContext, step: &str) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(step.into()),
        output: vec![1],
        saga_input: vec![9],
        compensation_available: true,
    }
}

fn wait_until(limit: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if pred() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    pred()
}

type Seen = Arc<Mutex<Vec<SagaChoreographyEvent>>>;

/// Bus with contract, bound steps, filler subscribers (required-path delivery)
/// and a capturing subscriber. The resolver is attached by the caller.
fn prepared_bus() -> (SagaChoreographyBus, Seen) {
    let bus = SagaChoreographyBus::new();
    bus.register_workflow_contract_provider::<BusContract>()
        .unwrap();
    bus.register_bound_workflow_step("qa", "a").unwrap();
    bus.register_bound_workflow_step("qa", "b").unwrap();
    let seen: Seen = Arc::default();
    let sink = Arc::clone(&seen);
    bus.subscribe_saga_type_fn("qa", move |event| {
        sink.lock().unwrap().push(event.clone());
        true
    });
    for _ in 0..2 {
        bus.subscribe_saga_type_fn("qa", |_| true);
    }
    (bus, seen)
}

fn count(seen: &Seen, pred: impl Fn(&SagaChoreographyEvent) -> bool) -> usize {
    seen.lock().unwrap().iter().filter(|e| pred(e)).count()
}

fn is_start(e: &SagaChoreographyEvent) -> bool {
    matches!(e, SagaChoreographyEvent::SagaStarted { .. })
}

fn durable_bus(journal: Arc<InMemoryTerminalResolverJournal>) -> (SagaChoreographyBus, Seen) {
    let (bus, seen) = prepared_bus();
    bus.attach_durable_terminal_resolver_for_contract::<BusContract, _>("qa", journal)
        .unwrap();
    (bus, seen)
}

// ---------------------------------------------------------------- B8

#[test]
fn recovered_deadline_is_held_until_activation_then_delivered_once() {
    let mut p = Accepter {
        saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
    };
    let ctx = context(121, "a");
    let accepted = accept_workflow_step(
        &mut p,
        ctx.clone(),
        "a".into(),
        StepExecutionId::new("recovered-external-effect"),
        AcceptedStepPolicy {
            idle_timeout: Duration::from_secs(1),
            hard_timeout: Duration::from_secs(1),
            timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: true,
            },
        },
        vec![9],
        vec![42],
    )
    .unwrap();
    let journal = Arc::new(InMemoryTerminalResolverJournal::default());
    journal.append(started(&ctx)).unwrap();
    journal.append(accepted).unwrap();

    let bus = SagaChoreographyBus::new();
    bus.register_workflow_contract_provider::<BusContract>()
        .unwrap();
    let (tx, rx) = mpsc::channel();
    bus.subscribe_saga_type_fn("qa", move |event| {
        if matches!(event, SagaChoreographyEvent::CompensationRequested { .. }) {
            tx.send(event.clone()).unwrap();
        }
        true
    });
    bus.attach_durable_terminal_resolver_for_contract::<BusContract, _>("qa", journal)
        .unwrap();

    // Deadline (1s) expires well inside this window; nothing may escape.
    assert!(
        rx.recv_timeout(Duration::from_millis(2_500)).is_err(),
        "recovered rollback published before binding/activation"
    );

    bus.activate_terminal_resolver_recovery_for_contract::<BusContract>()
        .unwrap();
    let request = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("retained obligation delivered at activation");
    assert!(matches!(
        request,
        SagaChoreographyEvent::CompensationRequested { steps_to_compensate, .. }
            if steps_to_compensate == vec![Box::<str>::from("a")]
    ));
    assert!(
        rx.recv_timeout(Duration::from_millis(600)).is_err(),
        "obligation must be delivered exactly once"
    );
}

#[test]
fn new_start_is_not_admitted_before_recovery_activation() {
    let journal = Arc::new(InMemoryTerminalResolverJournal::default());
    let (bus, seen) = durable_bus(journal);
    let ctx = context(201, "a");
    let err = bus.publish_strict(started(&ctx)).unwrap_err();
    assert!(matches!(err, SagaBusPublishError::AdmissionRejected { .. }));
    assert_eq!(count(&seen, is_start), 0, "start must not fan out");

    bus.activate_terminal_resolver_recovery_for_contract::<BusContract>()
        .unwrap();
    bus.publish_strict(started(&ctx)).unwrap();
    assert!(wait_until(Duration::from_secs(1), || count(
        &seen, is_start
    ) == 1));
}

// ---------------------------------------------------------------- B3 bus side

#[test]
fn terminal_run_replay_is_rejected_before_fanout_across_bus_restart() {
    let journal = Arc::new(InMemoryTerminalResolverJournal::default());
    let t0 = 1_000_000;
    let ctx = context_at(301, "a", t0);
    journal.append(started(&ctx)).unwrap();
    journal
        .append(SagaChoreographyEvent::SagaCompleted {
            context: ctx.next_step("terminal".into()),
        })
        .unwrap();

    let (bus, seen) = durable_bus(journal);
    bus.activate_terminal_resolver_recovery_for_contract::<BusContract>()
        .unwrap();

    // Same run identity: rejected, no participant ever sees it.
    let err = bus.publish_strict(started(&ctx)).unwrap_err();
    assert!(matches!(err, SagaBusPublishError::AdmissionRejected { .. }));
    assert_eq!(count(&seen, is_start), 0);
    // An older run is equally dead.
    let older = context_at(301, "a", t0 - 1);
    assert!(bus.publish_strict(started(&older)).is_err());
    assert_eq!(count(&seen, is_start), 0);

    // A strictly later run of the ordinarily resolved id is admitted.
    let later = context_at(301, "a", t0 + 5_000);
    bus.publish_strict(started(&later)).unwrap();
    assert!(wait_until(Duration::from_secs(1), || count(
        &seen, is_start
    ) == 1));
}

#[test]
fn unresolved_quarantine_blocks_reuse_of_the_saga_id() {
    let journal = Arc::new(InMemoryTerminalResolverJournal::default());
    let t0 = 1_000_000;
    let ctx = context_at(302, "a", t0);
    journal.append(started(&ctx)).unwrap();
    journal
        .append(SagaChoreographyEvent::SagaQuarantined {
            context: ctx.next_step("terminal".into()),
            reason: "operator review".into(),
            step: "a".into(),
            participant_id: "a".into(),
        })
        .unwrap();
    let (bus, seen) = durable_bus(journal);
    bus.activate_terminal_resolver_recovery_for_contract::<BusContract>()
        .unwrap();
    let later = context_at(302, "a", t0 + 5_000);
    let err = bus.publish_strict(started(&later)).unwrap_err();
    assert!(matches!(err, SagaBusPublishError::AdmissionRejected { .. }));
    assert_eq!(count(&seen, is_start), 0);
    // Unrelated ids are unaffected.
    bus.publish_strict(started(&context(303, "a"))).unwrap();
}

struct ReadFailJournal {
    inner: InMemoryTerminalResolverJournal,
    fail_reads: AtomicBool,
}

impl TerminalResolverJournal for ReadFailJournal {
    fn append(
        &self,
        event: SagaChoreographyEvent,
    ) -> Result<u64, icanact_saga_choreography::TerminalResolverJournalError> {
        self.inner.append(event)
    }
    fn read_all(
        &self,
    ) -> Result<
        Vec<TerminalResolverJournalEntry>,
        icanact_saga_choreography::TerminalResolverJournalError,
    > {
        if self.fail_reads.load(Ordering::SeqCst) {
            return Err(TerminalResolverJournalError::Storage("read down".into()));
        }
        self.inner.read_all()
    }
}

#[test]
fn journal_read_failure_blocks_admission() {
    let journal = Arc::new(ReadFailJournal {
        inner: InMemoryTerminalResolverJournal::default(),
        fail_reads: AtomicBool::new(false),
    });
    let (bus, seen) = prepared_bus();
    bus.attach_durable_terminal_resolver_for_contract::<BusContract, _>("qa", journal.clone())
        .unwrap();
    bus.activate_terminal_resolver_recovery_for_contract::<BusContract>()
        .unwrap();
    journal.fail_reads.store(true, Ordering::SeqCst);
    let err = bus.publish_strict(started(&context(401, "a"))).unwrap_err();
    assert!(matches!(err, SagaBusPublishError::AdmissionRejected { .. }));
    assert_eq!(count(&seen, is_start), 0);
    journal.fail_reads.store(false, Ordering::SeqCst);
    bus.publish_strict(started(&context(401, "a"))).unwrap();
}

// ---------------------------------------------------------------- B6 (real actor)

/// Participant actor whose first saga event parks it, so a capacity-1 bound
/// channel fills and further forwards fail.
struct ParkedActor {
    release: Arc<Mutex<mpsc::Receiver<()>>>,
    handled: Arc<AtomicUsize>,
}

#[derive(Clone, Debug)]
struct NoTell;
impl icanact_core::TellAskTell for NoTell {}

impl SyncActor for ParkedActor {
    type Contract = local_sync::contract::TellOnly;
    type Tell = NoTell;
    type Ask = ();
    type Reply = ();
    type Channel = SagaParticipantChannel<()>;
    type PubSub = ();
    type Broadcast = ();
    fn handle_tell(&mut self, _: NoTell) {}
    fn handle_channel(&mut self, _: local_sync::ChannelId, _: Self::Channel) {
        self.handled.fetch_add(1, Ordering::SeqCst);
        let _ = self.release.lock().unwrap().recv();
    }
}

struct Parked {
    bus: SagaChoreographyBus,
    seen: Seen,
    journal: Arc<InMemoryTerminalResolverJournal>,
    release: mpsc::Sender<()>,
    _handle: local_sync::ActorHandle,
}

fn parked_bus(saga_id: u64) -> (Parked, SagaContext) {
    let journal = Arc::new(InMemoryTerminalResolverJournal::default());
    let (bus, seen) = durable_bus(Arc::clone(&journal));
    bus.activate_terminal_resolver_recovery_for_contract::<BusContract>()
        .unwrap();
    let (release, rx) = mpsc::channel();
    let handled = Arc::new(AtomicUsize::new(0));
    let (actor_ref, handle) = local_sync::spawn(ParkedActor {
        release: Arc::new(Mutex::new(rx)),
        handled: Arc::clone(&handled),
    });
    bind_sync_participant_channel::<ParkedActor, ()>(&bus, &actor_ref, &["qa"], "saga", 1).unwrap();
    let ctx = context(saga_id, "a");
    bus.publish_strict(started(&ctx)).unwrap();
    // Actor parks on the start; the next event fills the single slot.
    assert!(wait_until(Duration::from_secs(1), || handled
        .load(Ordering::SeqCst)
        == 1));
    // Saturate the bounded channel with benign progress events until the
    // actor forward starts failing (non-strict publish never escalates).
    let mut saturated = false;
    for _ in 0..4_096 {
        let stats = bus.publish(SagaChoreographyEvent::StepStarted {
            context: ctx.next_step("a".into()),
        });
        if stats.delivered < stats.attempted {
            saturated = true;
            break;
        }
    }
    assert!(saturated, "actor channel never saturated");
    (
        Parked {
            bus,
            seen,
            journal,
            release,
            _handle: handle,
        },
        ctx,
    )
}

fn no_plain_failure(seen: &Seen) {
    assert_eq!(
        count(seen, |e| matches!(
            e,
            SagaChoreographyEvent::SagaFailed { .. }
        )),
        0,
        "delivery failure must not manufacture ordinary SagaFailed over effects"
    );
}

#[test]
fn actor_forward_failure_after_effect_quarantines_instead_of_failing() {
    let (p, ctx) = parked_bus(501);
    let err = p
        .bus
        .publish_strict(completed(&ctx, "a"))
        .expect_err("caller must not see success");
    assert!(
        matches!(
            err,
            SagaBusPublishError::PartialDelivery { .. }
                | SagaBusPublishError::TerminalEscalationPartialDelivery { .. }
                | SagaBusPublishError::RequiredPathDeliveryShortfall { .. }
        ),
        "{err:?}"
    );
    assert!(wait_until(Duration::from_secs(1), || count(&p.seen, |e| {
        matches!(e, SagaChoreographyEvent::SagaQuarantined { .. })
    }) == 1));
    no_plain_failure(&p.seen);
    let outcome = p.bus.take_terminal_outcome(ctx.saga_id);
    assert!(
        matches!(outcome, Some(SagaTerminalOutcome::Quarantined { .. })),
        "{outcome:?}"
    );
    // Durable evidence retained in the resolver journal.
    assert!(wait_until(Duration::from_secs(1), || {
        p.journal
            .read_all()
            .unwrap()
            .iter()
            .any(|entry| matches!(entry.event, SagaChoreographyEvent::SagaQuarantined { .. }))
    }));
    let _ = p.release.send(());
}

#[test]
fn partially_delivered_compensation_retains_reconciliation_evidence() {
    let (p, ctx) = parked_bus(502);
    let err = p
        .bus
        .publish_strict(SagaChoreographyEvent::CompensationRequested {
            context: ctx.next_step("resolver".into()),
            failed_step: "a".into(),
            reason: "rollback".into(),
            failure: SagaFailureDetails {
                step_name: "a".into(),
                participant_id: "a".into(),
                error_code: None,
                error_message: "forward failed".into(),
                at_millis: ctx.saga_started_at_millis,
            },
            steps_to_compensate: vec!["a".into()],
        })
        .expect_err("partial compensation delivery is not success");
    assert!(
        matches!(
            err,
            SagaBusPublishError::PartialDelivery { .. }
                | SagaBusPublishError::TerminalEscalationPartialDelivery { .. }
        ),
        "{err:?}"
    );
    assert!(wait_until(Duration::from_secs(1), || p
        .journal
        .read_all()
        .unwrap()
        .iter()
        .any(|entry| matches!(
            &entry.event,
            SagaChoreographyEvent::SagaQuarantined { reason, .. }
                if reason.contains("compensation_requested")
        ))));
    no_plain_failure(&p.seen);
    let _ = p.release.send(());
}

#[test]
fn ingress_driven_recovery_output_is_held_until_activation_too() {
    let journal = Arc::new(InMemoryTerminalResolverJournal::default());
    let (bus, seen) = durable_bus(Arc::clone(&journal));
    let ctx = context(601, "a");
    bus.publish_strict(completed(&ctx, "b")).unwrap();
    // A later FIFO marker proves the resolver has finished handling completion.
    bus.publish_strict(SagaChoreographyEvent::StepStarted {
        context: ctx.next_step("b".into()),
    })
    .unwrap();
    assert!(wait_until(Duration::from_secs(1), || journal
        .read_all()
        .unwrap()
        .iter()
        .any(|entry| matches!(
            entry.event,
            SagaChoreographyEvent::StepStarted { .. }
        ))));
    assert_eq!(
        count(&seen, |e| matches!(
            e,
            SagaChoreographyEvent::SagaCompleted { .. }
        )),
        0,
        "ingress-driven recovery output escaped before activation"
    );
    bus.activate_terminal_resolver_recovery("qa").unwrap();
    assert!(wait_until(Duration::from_secs(1), || count(
        &seen,
        |e| matches!(e, SagaChoreographyEvent::SagaCompleted { .. })
    ) == 1));
}
