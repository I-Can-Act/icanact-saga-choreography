//! Review-5 bus regressions: terminal quarantine liveness and the durable
//! quarantine fence lifecycle (waiters for non-resolver quarantines, a late
//! older-run quarantine versus successor admission, restart journal orders,
//! outage recovery without a later evidence event, and no redundant fences).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use icanact_core::local_sync;
use icanact_saga_choreography::*;

define_saga_workflow_contract! {
    struct Review5Contract {
        saga_type: "qa_review5",
        first_step: a,
        failure_authority: any (),
        required_steps: [b],
        overall_timeout_ms: 600_000,
        stalled_timeout_ms: 600_000,
        steps: {
            a => { participant: "a", depends_on: on_start () },
            b => { participant: "b", depends_on: after [a] }
        }
    }
}

const TYPE: &str = "qa_review5";

fn context_at(id: u64, step: &str, started_at: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId(id),
        saga_type: TYPE.into(),
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

fn started(ctx: &SagaContext) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: ctx.clone(),
        payload: vec![9],
    }
}

fn step_completed(ctx: &SagaContext, step: &str, trace: u64) -> SagaChoreographyEvent {
    let mut context = ctx.next_step(step.into());
    context.trace_id = trace;
    SagaChoreographyEvent::StepCompleted {
        context,
        output: vec![1],
        saga_input: vec![9],
        compensation_available: true,
    }
}

fn participant_quarantine(ctx: &SagaContext, reason: &str) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaQuarantined {
        context: ctx.next_step("a".into()),
        reason: reason.into(),
        step: "a".into(),
        participant_id: "a".into(),
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

fn count(seen: &Seen, pred: impl Fn(&SagaChoreographyEvent) -> bool) -> usize {
    seen.lock().unwrap().iter().filter(|e| pred(e)).count()
}

fn is_completed(e: &SagaChoreographyEvent) -> bool {
    matches!(e, SagaChoreographyEvent::SagaCompleted { .. })
}

fn is_quarantined(e: &SagaChoreographyEvent) -> bool {
    matches!(e, SagaChoreographyEvent::SagaQuarantined { .. })
}

fn quarantined_run(started_at: u64) -> impl Fn(&SagaChoreographyEvent) -> bool {
    move |e| is_quarantined(e) && e.context().saga_started_at_millis == started_at
}

fn prepared_bus(extra_subscribers: usize) -> (SagaChoreographyBus, Seen) {
    let bus = SagaChoreographyBus::new();
    bus.register_workflow_contract_provider::<Review5Contract>()
        .unwrap();
    bus.register_bound_workflow_step(TYPE, "a").unwrap();
    bus.register_bound_workflow_step(TYPE, "b").unwrap();
    let seen: Seen = Arc::default();
    let sink = Arc::clone(&seen);
    bus.subscribe_saga_type_fn(TYPE, move |event| {
        sink.lock().unwrap().push(event.clone());
        true
    });
    for _ in 0..extra_subscribers {
        bus.subscribe_saga_type_fn(TYPE, |_| true);
    }
    (bus, seen)
}

fn durable_bus<J: TerminalResolverJournal>(journal: Arc<J>) -> (SagaChoreographyBus, Seen) {
    let (bus, seen) = prepared_bus(2);
    bus.attach_durable_terminal_resolver_for_contract::<Review5Contract, _>("qa", journal)
        .unwrap();
    bus.activate_terminal_resolver_recovery_for_contract::<Review5Contract>()
        .unwrap();
    (bus, seen)
}

/// Journal wrapper that can fail appends and park one chosen quarantine append.
struct FaultJournal {
    inner: Arc<dyn TerminalResolverJournal>,
    fail_append: AtomicBool,
    append_failures: AtomicUsize,
    park_reason: Mutex<Option<&'static str>>,
    entered: (Mutex<bool>, Condvar),
    released: (Mutex<bool>, Condvar),
}

impl FaultJournal {
    fn new(inner: Arc<dyn TerminalResolverJournal>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            fail_append: AtomicBool::new(false),
            append_failures: AtomicUsize::new(0),
            park_reason: Mutex::new(None),
            entered: (Mutex::new(false), Condvar::new()),
            released: (Mutex::new(false), Condvar::new()),
        })
    }

    fn memory() -> Arc<Self> {
        Self::new(Arc::new(InMemoryTerminalResolverJournal::default()))
    }

    fn park_next(&self, reason: &'static str) {
        *self.park_reason.lock().unwrap() = Some(reason);
    }

    fn wait_parked(&self) -> bool {
        let (lock, cvar) = &self.entered;
        let guard = lock.lock().unwrap();
        let (guard, _) = cvar
            .wait_timeout_while(guard, Duration::from_secs(5), |entered| !*entered)
            .unwrap();
        *guard
    }

    fn release(&self) {
        let (lock, cvar) = &self.released;
        *lock.lock().unwrap() = true;
        cvar.notify_all();
    }

    fn rows(&self) -> Vec<SagaChoreographyEvent> {
        self.inner
            .read_all()
            .unwrap()
            .into_iter()
            .map(|entry| entry.event)
            .collect()
    }

    fn rows_matching(&self, pred: impl Fn(&SagaChoreographyEvent) -> bool) -> usize {
        self.rows().iter().filter(|e| pred(e)).count()
    }
}

impl TerminalResolverJournal for FaultJournal {
    fn append(&self, event: SagaChoreographyEvent) -> Result<u64, TerminalResolverJournalError> {
        let parked = match &event {
            SagaChoreographyEvent::SagaQuarantined { reason, .. } => {
                let mut slot = self.park_reason.lock().unwrap();
                if *slot == Some(reason.as_ref()) {
                    slot.take();
                    true
                } else {
                    false
                }
            }
            _ => false,
        };
        if parked {
            {
                let (lock, cvar) = &self.entered;
                *lock.lock().unwrap() = true;
                cvar.notify_all();
            }
            let (lock, cvar) = &self.released;
            let guard = lock.lock().unwrap();
            let _ = cvar
                .wait_timeout_while(guard, Duration::from_secs(10), |released| !*released)
                .unwrap();
        }
        if self.fail_append.load(Ordering::SeqCst) {
            self.append_failures.fetch_add(1, Ordering::SeqCst);
            return Err(TerminalResolverJournalError::Storage("append down".into()));
        }
        self.inner.append(event)
    }
    fn read_all(&self) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
        self.inner.read_all()
    }
    fn supports_saga_lookup(&self) -> bool {
        self.inner.supports_saga_lookup()
    }
    fn read_saga(
        &self,
        saga_id: SagaId,
    ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
        self.inner.read_saga(saga_id)
    }
    fn read_saga_bounded(
        &self,
        saga_id: SagaId,
        max_entries: usize,
    ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
        self.inner.read_saga_bounded(saga_id, max_entries)
    }
    fn compact_terminal_detail(&self) -> Result<u64, TerminalResolverJournalError> {
        self.inner.compact_terminal_detail()
    }
}

enum ProbeMsg {
    Register {
        bus: SagaChoreographyBus,
        context: Option<SagaContext>,
        saga_id: SagaId,
        reply: SagaReplyToHandle,
    },
}

type WaiterOutcome = Result<SagaReplyToResult, String>;

/// Registers a run-scoped (`Some`) or id-scoped (`None`) waiter, off any actor.
fn waiter(
    bus: &SagaChoreographyBus,
    saga_id: SagaId,
    context: Option<&SagaContext>,
) -> mpsc::Receiver<WaiterOutcome> {
    let (probe, handle) = local_sync::mpsc::spawn(8, |msg: ProbeMsg| match msg {
        ProbeMsg::Register {
            bus,
            context,
            saga_id,
            reply,
        } => {
            let _ = match context {
                Some(context) => bus.register_terminal_reply_for_run(&context, reply),
                None => bus.register_terminal_reply(saga_id, reply),
            };
        }
    });
    let pending = probe
        .ask_delegated(|reply| ProbeMsg::Register {
            bus: bus.clone(),
            context: context.cloned(),
            saga_id,
            reply,
        })
        .expect("waiter registered");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _keep = handle;
        let _ = tx.send(pending.wait().map_err(|e| format!("{e:?}")));
    });
    std::thread::sleep(Duration::from_millis(50));
    rx
}

fn expect_quarantine_reply(rx: &mpsc::Receiver<WaiterOutcome>, what: &str) {
    match rx.recv_timeout(Duration::from_secs(3)) {
        Ok(Ok(Ok(reply))) => assert!(
            matches!(reply.outcome, SagaTerminalOutcome::Quarantined { .. }),
            "{what}: expected quarantine, got {reply:?}"
        ),
        other => panic!("{what}: waiter was not resolved by the quarantine: {other:?}"),
    }
}

fn expect_unresolved(rx: &mpsc::Receiver<WaiterOutcome>, what: &str) {
    assert!(
        matches!(
            rx.recv_timeout(Duration::from_millis(400)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ),
        "{what}: an unrelated waiter must not be satisfied"
    );
}

// ------------------------------------------------------------------ N1

#[test]
fn participant_quarantine_resolves_bound_run_and_id_waiters_only() {
    for durable in [false, true] {
        let (bus, _seen) = prepared_bus(2);
        if durable {
            bus.attach_durable_terminal_resolver_for_contract::<Review5Contract, _>(
                "qa",
                FaultJournal::memory(),
            )
            .unwrap();
            bus.activate_terminal_resolver_recovery_for_contract::<Review5Contract>()
                .unwrap();
        } else {
            bus.attach_terminal_resolver_for_contract::<Review5Contract>("qa")
                .unwrap();
        }
        let base = SagaContext::now_millis();
        let ctx = context_at(5_001, "a", base);
        let foreign = context_at(5_002, "a", base);
        let successor = context_at(5_001, "a", base + 7);

        let run_rx = waiter(&bus, ctx.saga_id, Some(&ctx));
        let foreign_rx = waiter(&bus, foreign.saga_id, Some(&foreign));
        let successor_rx = waiter(&bus, successor.saga_id, Some(&successor));
        bus.publish_strict(started(&ctx)).unwrap();
        let id_rx = waiter(&bus, ctx.saga_id, None);

        // Participant-originated (not resolver-originated) quarantine.
        bus.publish(participant_quarantine(&ctx, "result write failed"));

        expect_quarantine_reply(&run_rx, &format!("run waiter durable={durable}"));
        expect_quarantine_reply(&id_rx, &format!("id waiter durable={durable}"));
        expect_unresolved(&foreign_rx, "foreign run");
        expect_unresolved(&successor_rx, "successor run");
    }
}

#[test]
fn delivery_shortfall_quarantine_resolves_the_run_waiter() {
    let (bus, _seen) = prepared_bus(0);
    bus.attach_durable_terminal_resolver_for_contract::<Review5Contract, _>(
        "qa",
        FaultJournal::memory(),
    )
    .unwrap();
    bus.activate_terminal_resolver_recovery_for_contract::<Review5Contract>()
        .unwrap();
    let ctx = context_at(5_010, "a", SagaContext::now_millis());
    let run_rx = waiter(&bus, ctx.saga_id, Some(&ctx));
    // Too few subscribers for the required path: start fanout is quarantined.
    let _ = bus.publish(started(&ctx));
    expect_quarantine_reply(&run_rx, "delivery shortfall");
    assert!(matches!(
        bus.take_terminal_outcome_for_run(&ctx),
        Some(SagaTerminalOutcome::Quarantined { .. })
    ));
}

#[test]
fn parent_review5_recovery_quarantine_resolves_held_waiters_at_activation_only() {
    let journal = FaultJournal::memory();
    let ctx = context_at(5_020, "a", SagaContext::now_millis());
    journal.inner.append(started(&ctx)).unwrap();
    let (bus, seen) = prepared_bus(2);
    bus.attach_durable_terminal_resolver_for_contract::<Review5Contract, _>(
        "qa",
        Arc::clone(&journal),
    )
    .unwrap();
    let run_rx = waiter(&bus, ctx.saga_id, Some(&ctx));
    let id_rx = waiter(&bus, ctx.saga_id, None);
    bus.publish(participant_quarantine(
        &ctx,
        "startup participant uncertainty",
    ));
    assert!(wait_until(Duration::from_secs(3), || journal
        .rows_matching(is_quarantined)
        == 1));
    expect_unresolved(&run_rx, "full-run recovery output before activation");
    expect_unresolved(&id_rx, "bound legacy recovery output before activation");

    bus.activate_terminal_resolver_recovery_for_contract::<Review5Contract>()
        .unwrap();
    expect_quarantine_reply(&run_rx, "held full-run quarantine at activation");
    expect_quarantine_reply(&id_rx, "held legacy quarantine at activation");
    bus.activate_terminal_resolver_recovery_for_contract::<Review5Contract>()
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        journal.rows_matching(is_quarantined),
        1,
        "reply delivery must not append a duplicate quarantine"
    );
    assert_eq!(
        count(&seen, is_quarantined),
        1,
        "activation resolves the held reply without republishing ingress"
    );
}

// ------------------------------------------------------------------ N3

#[test]
fn late_old_quarantine_racing_successor_admission_quarantines_the_successor() {
    let journal = FaultJournal::memory();
    let (bus, seen) = durable_bus(Arc::clone(&journal));
    let base = SagaContext::now_millis();
    let old = context_at(5_100, "a", base);
    let new = context_at(5_100, "a", base + 10);
    bus.publish_strict(started(&old)).unwrap();
    bus.publish_strict(step_completed(&old, "a", 1)).unwrap();
    bus.publish_strict(step_completed(&old, "b", 2)).unwrap();
    assert!(wait_until(Duration::from_secs(3), || count(
        &seen,
        is_completed
    ) == 1));

    // The old quarantine is journaled (parked) but not yet indexed when the
    // successor is admitted.
    journal.park_next("late old uncertainty");
    bus.publish(participant_quarantine(&old, "late old uncertainty"));
    assert!(journal.wait_parked(), "old quarantine reached the journal");
    bus.publish_strict(started(&new)).unwrap();
    journal.release();

    assert!(
        wait_until(Duration::from_secs(3), || count(
            &seen,
            quarantined_run(new.saga_started_at_millis)
        ) >= 1),
        "the successor admitted in the window must be visibly quarantined"
    );
    assert!(wait_until(Duration::from_secs(3), || {
        journal.rows_matching(quarantined_run(new.saga_started_at_millis)) >= 1
    }));
    bus.publish_strict(step_completed(&new, "a", 3)).ok();
    bus.publish_strict(step_completed(&new, "b", 4)).ok();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        count(&seen, |e| is_completed(e)
            && e.context().saga_started_at_millis
                == new.saga_started_at_millis),
        0,
        "the successor cannot complete behind an unresolved quarantine"
    );
}

fn restart_with(journal: &Arc<FaultJournal>) -> (SagaChoreographyBus, Seen) {
    durable_bus(Arc::clone(journal))
}

fn restart_order_case(quarantine_first: bool) {
    let journal = FaultJournal::memory();
    let base = SagaContext::now_millis();
    let id = if quarantine_first { 5_201 } else { 5_202 };
    let old = context_at(id, "a", base);
    let new = context_at(id, "a", base + 10);
    let old_q = SagaChoreographyEvent::SagaQuarantined {
        context: old.next_step(TERMINAL_RESOLVER_STEP.into()),
        reason: "late old uncertainty".into(),
        step: "a".into(),
        participant_id: "a".into(),
    };
    journal.inner.append(started(&old)).unwrap();
    if quarantine_first {
        journal.inner.append(old_q).unwrap();
        journal.inner.append(started(&new)).unwrap();
    } else {
        journal.inner.append(started(&new)).unwrap();
        journal.inner.append(old_q).unwrap();
    }
    let (bus, seen) = restart_with(&journal);
    assert!(
        wait_until(Duration::from_secs(3), || count(
            &seen,
            quarantined_run(new.saga_started_at_millis)
        ) >= 1),
        "restart must quarantine the successor (quarantine_first={quarantine_first})"
    );
    assert!(wait_until(Duration::from_secs(3), || journal
        .rows_matching(quarantined_run(new.saga_started_at_millis))
        >= 1));
    assert_eq!(count(&seen, is_completed), 0);
    drop(bus);
    let before = journal.rows().len();
    // A second restart is quiet: no duplicate quarantine output or rows.
    let (_bus, seen) = restart_with(&journal);
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(count(&seen, is_quarantined), 0, "no repeated output");
    assert_eq!(journal.rows().len(), before, "no repeated journal rows");
    assert_eq!(
        journal.rows_matching(quarantined_run(new.saga_started_at_millis)),
        1,
        "exactly one successor quarantine row"
    );
}

#[test]
fn restart_old_quarantine_then_admitted_successor_start_quarantines_the_successor() {
    restart_order_case(true);
}

#[test]
fn restart_admitted_successor_start_then_old_quarantine_quarantines_the_successor() {
    restart_order_case(false);
}

// ------------------------------------------------------------------ N5/N6

fn outage_quarantine(
    journal: &Arc<FaultJournal>,
    ctx: &SagaContext,
) -> (SagaChoreographyBus, Seen) {
    let (bus, seen) = durable_bus(Arc::clone(journal));
    bus.publish_strict(started(ctx)).unwrap();
    journal.fail_append.store(true, Ordering::SeqCst);
    bus.publish_strict(step_completed(ctx, "a", 1)).unwrap();
    assert!(
        wait_until(Duration::from_secs(3), || {
            count(&seen, is_quarantined) >= 1 && journal.append_failures.load(Ordering::SeqCst) >= 2
        }),
        "the failed append visibly quarantines the live run"
    );
    (bus, seen)
}

#[test]
fn pending_quarantine_fence_is_retried_after_recovery_without_new_evidence() {
    let journal = FaultJournal::memory();
    let ctx = context_at(5_301, "a", SagaContext::now_millis());
    let (bus, _seen) = outage_quarantine(&journal, &ctx);
    // Live fencing is immediate even though nothing is durable.
    assert!(
        bus.publish_strict(started(&context_at(
            5_301,
            "a",
            ctx.saga_started_at_millis + 5
        )))
        .is_err(),
        "successor refused during the outage"
    );
    assert_eq!(journal.rows_matching(is_quarantined), 0);

    journal.fail_append.store(false, Ordering::SeqCst);
    // No further evidence event: the watchdog tick alone must fence storage.
    assert!(
        wait_until(Duration::from_secs(5), || journal
            .rows_matching(quarantined_run(ctx.saga_started_at_millis))
            >= 1),
        "recovered storage receives the pending quarantine fence"
    );
    let durable_rows = journal.rows_matching(is_quarantined);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        journal.rows_matching(is_quarantined),
        durable_rows,
        "a durable fence is not retried again"
    );

    // Restart: the run is reconstructed quarantined, never as an ordinary result.
    let release = bus.release_waiter();
    drop(bus);
    assert!(release.wait_timeout(Duration::from_secs(5)));
    let (bus, seen) = restart_with(&journal);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(count(&seen, is_completed), 0);
    assert!(
        bus.publish_strict(started(&ctx)).is_err(),
        "restart keeps the run quarantined"
    );
}

#[test]
fn already_durable_quarantine_does_not_append_a_fence_before_each_late_event() {
    let journal = FaultJournal::memory();
    let (bus, _seen) = durable_bus(Arc::clone(&journal));
    let ctx = context_at(5_401, "a", SagaContext::now_millis());
    bus.publish_strict(started(&ctx)).unwrap();
    bus.publish(participant_quarantine(&ctx, "durable uncertainty"));
    assert!(wait_until(Duration::from_secs(3), || journal
        .rows_matching(is_quarantined)
        == 1));
    for trace in 10..14 {
        bus.publish_strict(step_completed(&ctx, "b", trace)).ok();
    }
    assert!(
        wait_until(Duration::from_secs(3), || journal.rows_matching(
            |e| matches!(e, SagaChoreographyEvent::StepCompleted { .. })
        ) == 4),
        "late evidence stays retained in order"
    );
    assert_eq!(
        journal.rows_matching(is_quarantined),
        1,
        "the already-durable quarantine is not re-fenced per late event"
    );
}

#[test]
fn restored_quarantine_is_already_durable_before_late_evidence() {
    let journal = FaultJournal::memory();
    let ctx = context_at(5_450, "a", SagaContext::now_millis());
    journal.inner.append(started(&ctx)).unwrap();
    journal
        .inner
        .append(participant_quarantine(
            &ctx,
            "retained exact-run quarantine",
        ))
        .unwrap();
    let (_bus, _seen) = {
        let (bus, seen) = durable_bus(Arc::clone(&journal));
        for trace in 30..34 {
            bus.publish_strict(step_completed(&ctx, "b", trace)).ok();
        }
        assert!(wait_until(Duration::from_secs(3), || journal
            .rows_matching(|event| matches!(
                event,
                SagaChoreographyEvent::StepCompleted { .. }
            ))
            == 4));
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(
            journal.rows_matching(is_quarantined),
            1,
            "validated restored row needs no synthetic fence per late event or tick"
        );
        (bus, seen)
    };
}

#[test]
fn recovered_pending_fence_is_written_once_before_late_evidence() {
    let journal = FaultJournal::memory();
    let ctx = context_at(5_501, "a", SagaContext::now_millis());
    let (bus, _seen) = outage_quarantine(&journal, &ctx);
    journal.fail_append.store(false, Ordering::SeqCst);
    for trace in 20..23 {
        bus.publish_strict(step_completed(&ctx, "b", trace)).ok();
    }
    assert!(wait_until(Duration::from_secs(5), || journal
        .rows_matching(|e| matches!(
            e,
            SagaChoreographyEvent::StepCompleted { .. }
        ))
        == 3));
    let rows = journal.rows();
    let first_fence = rows.iter().position(is_quarantined).expect("fence written");
    let first_late = rows
        .iter()
        .position(|e| matches!(e, SagaChoreographyEvent::StepCompleted { .. }))
        .unwrap();
    assert!(first_fence < first_late, "fence precedes retained evidence");
    assert_eq!(journal.rows_matching(is_quarantined), 1);
}

#[cfg(feature = "lmdb")]
#[test]
fn parent_review5_old_quarantine_fences_an_ordinarily_completed_successor_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let base = SagaContext::now_millis();
    let old = context_at(5_650, "a", base);
    let new = context_at(5_650, "a", base + 10);
    {
        let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
        let (bus, seen) = durable_bus(Arc::clone(&journal));
        for (ctx, trace) in [(&old, 1), (&new, 3)] {
            bus.publish_strict(started(ctx)).unwrap();
            bus.publish_strict(step_completed(ctx, "a", trace)).unwrap();
            bus.publish_strict(step_completed(ctx, "b", trace + 1))
                .unwrap();
            assert!(wait_until(Duration::from_secs(3), || journal
                .read_all()
                .unwrap()
                .iter()
                .any(|row| is_completed(&row.event)
                    && row.event.context().saga_started_at_millis
                        == ctx.saga_started_at_millis)));
        }
        assert_eq!(count(&seen, is_completed), 2);
        let release = bus.release_waiter();
        drop(bus);
        assert!(release.wait_timeout(Duration::from_secs(5)));
        journal.compact_terminal_detail().unwrap();
    }
    let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
    let (bus, seen) = durable_bus(Arc::clone(&journal));
    let successor_rx = waiter(&bus, new.saga_id, Some(&new));
    bus.publish(participant_quarantine(
        &old,
        "late old uncertainty after both ordinary terminals",
    ));
    expect_quarantine_reply(&successor_rx, "ordinarily closed successor after reopen");
    assert!(
        wait_until(Duration::from_secs(3), || journal
            .read_all()
            .unwrap()
            .iter()
            .any(|row| quarantined_run(new.saga_started_at_millis)(&row.event))),
        "successor quarantine must be durable after resolver cache/history detail loss"
    );
    assert_eq!(
        count(&seen, is_completed),
        0,
        "closed history never invents fresh success"
    );
    assert!(matches!(
        bus.take_terminal_outcome_for_run(&new),
        Some(SagaTerminalOutcome::Quarantined { .. })
    ));
    assert!(
        bus.publish_strict(started(&context_at(5_650, "a", base + 20)))
            .is_err()
    );
}

#[cfg(feature = "lmdb")]
#[test]
fn parent_review5_recovery_quarantine_fences_closed_successor_without_new_ingress() {
    let dir = tempfile::tempdir().unwrap();
    let base = SagaContext::now_millis();
    let old = context_at(5_660, "a", base);
    let new = context_at(5_660, "a", base + 10);
    {
        let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
        let (bus, _seen) = durable_bus(Arc::clone(&journal));
        for (ctx, trace) in [(&old, 1), (&new, 3)] {
            bus.publish_strict(started(ctx)).unwrap();
            bus.publish_strict(step_completed(ctx, "a", trace)).unwrap();
            bus.publish_strict(step_completed(ctx, "b", trace + 1))
                .unwrap();
            assert!(wait_until(Duration::from_secs(3), || journal
                .read_all()
                .unwrap()
                .iter()
                .any(|row| is_completed(&row.event)
                    && row.event.context().saga_started_at_millis
                        == ctx.saga_started_at_millis)));
        }
        let release = bus.release_waiter();
        drop(bus);
        assert!(release.wait_timeout(Duration::from_secs(5)));
        journal.compact_terminal_detail().unwrap();
        // Crash boundary: old uncertainty was journaled, but propagation to the
        // already-closed successor was not. Recovery must not need another event.
        journal
            .append(participant_quarantine(
                &old,
                "old uncertainty persisted before propagation",
            ))
            .unwrap();
    }
    let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
    let (bus, seen) = prepared_bus(2);
    bus.attach_durable_terminal_resolver_for_contract::<Review5Contract, _>(
        "qa",
        Arc::clone(&journal),
    )
    .unwrap();
    let successor_rx = waiter(&bus, new.saga_id, Some(&new));
    expect_unresolved(&successor_rx, "successor recovery before activation");
    assert_eq!(count(&seen, is_quarantined), 0);
    bus.activate_terminal_resolver_recovery_for_contract::<Review5Contract>()
        .unwrap();
    expect_quarantine_reply(
        &successor_rx,
        "closed successor behind restored old quarantine",
    );
    assert!(wait_until(Duration::from_secs(3), || journal
        .read_all()
        .unwrap()
        .iter()
        .any(|row| quarantined_run(new.saga_started_at_millis)(
            &row.event
        ))));
    let release = bus.release_waiter();
    drop(bus);
    assert!(release.wait_timeout(Duration::from_secs(5)));
    let (_bus, seen) = durable_bus(Arc::clone(&journal));
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(count(&seen, is_quarantined), 0, "a second restart is quiet");
    assert_eq!(
        journal
            .read_all()
            .unwrap()
            .iter()
            .filter(|row| quarantined_run(new.saga_started_at_millis)(&row.event))
            .count(),
        1,
        "exactly one durable successor quarantine"
    );
}

#[cfg(feature = "lmdb")]
#[test]
fn lmdb_pending_fence_survives_close_and_reopen_after_tick_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = context_at(5_601, "a", SagaContext::now_millis());
    {
        let journal = FaultJournal::new(Arc::new(
            LmdbTerminalResolverJournal::open(dir.path()).unwrap(),
        ));
        let (bus, _seen) = outage_quarantine(&journal, &ctx);
        journal.fail_append.store(false, Ordering::SeqCst);
        assert!(wait_until(Duration::from_secs(5), || journal
            .rows_matching(quarantined_run(ctx.saga_started_at_millis))
            >= 1));
        let release = bus.release_waiter();
        drop(bus);
        drop(journal);
        assert!(release.wait_timeout(Duration::from_secs(5)));
    }
    let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
    let (bus, seen) = durable_bus(journal);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(count(&seen, is_completed), 0);
    assert!(bus.publish_strict(started(&ctx)).is_err());
}
