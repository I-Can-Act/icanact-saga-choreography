//! Review-remediation regressions for bus run admission: serialized/reserved
//! starts, one-time durable history indexing, waiter isolation, durable fencing
//! of non-start ingress after resolver cache loss, and strict start journaling.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use icanact_core::local_sync;
use icanact_saga_choreography::*;

define_saga_workflow_contract! {
    struct ReviewContract {
        saga_type: "qa_review",
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

const TYPE: &str = "qa_review";

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

fn step_completed(ctx: &SagaContext, step: &str, compensable: bool) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(step.into()),
        output: vec![1],
        saga_input: vec![9],
        compensation_available: compensable,
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

fn is_start(e: &SagaChoreographyEvent) -> bool {
    matches!(e, SagaChoreographyEvent::SagaStarted { .. })
}

fn is_failed(e: &SagaChoreographyEvent) -> bool {
    matches!(e, SagaChoreographyEvent::SagaFailed { .. })
}

fn is_quarantined(e: &SagaChoreographyEvent) -> bool {
    matches!(e, SagaChoreographyEvent::SagaQuarantined { .. })
}

fn prepared_bus() -> (SagaChoreographyBus, Seen) {
    let bus = SagaChoreographyBus::new();
    bus.register_workflow_contract_provider::<ReviewContract>()
        .unwrap();
    bus.register_bound_workflow_step(TYPE, "a").unwrap();
    bus.register_bound_workflow_step(TYPE, "b").unwrap();
    let seen: Seen = Arc::default();
    let sink = Arc::clone(&seen);
    bus.subscribe_saga_type_fn(TYPE, move |event| {
        sink.lock().unwrap().push(event.clone());
        true
    });
    // Required-path delivery needs more subscribers than the capture sink.
    for _ in 0..2 {
        bus.subscribe_saga_type_fn(TYPE, |_| true);
    }
    (bus, seen)
}

fn durable_bus<J: TerminalResolverJournal>(journal: Arc<J>) -> (SagaChoreographyBus, Seen) {
    let (bus, seen) = prepared_bus();
    bus.attach_durable_terminal_resolver_for_contract::<ReviewContract, _>("qa", journal)
        .unwrap();
    bus.activate_terminal_resolver_recovery_for_contract::<ReviewContract>()
        .unwrap();
    (bus, seen)
}

/// Journal that counts `read_all` calls and can fail appends on demand.
#[derive(Default)]
struct ProbeJournal {
    inner: InMemoryTerminalResolverJournal,
    reads: AtomicUsize,
    fail_appends: AtomicBool,
}

impl TerminalResolverJournal for ProbeJournal {
    fn append(&self, event: SagaChoreographyEvent) -> Result<u64, TerminalResolverJournalError> {
        if self.fail_appends.load(Ordering::SeqCst) {
            return Err(TerminalResolverJournalError::Storage("append down".into()));
        }
        self.inner.append(event)
    }
    fn read_all(&self) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read_all()
    }
}

fn journal_has(journal: &ProbeJournal, pred: impl Fn(&SagaChoreographyEvent) -> bool) -> bool {
    journal
        .inner
        .read_all()
        .unwrap()
        .iter()
        .any(|entry| pred(&entry.event))
}

fn journal_count(journal: &ProbeJournal, pred: impl Fn(&SagaChoreographyEvent) -> bool) -> usize {
    journal
        .inner
        .read_all()
        .unwrap()
        .iter()
        .filter(|entry| pred(&entry.event))
        .count()
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

/// Registers a terminal waiter (legacy saga-id API when `context` is `None`,
/// run-scoped otherwise) and returns a channel carrying its eventual resolution.
fn waiter(
    bus: &SagaChoreographyBus,
    saga_id: SagaId,
    context: Option<&SagaContext>,
) -> mpsc::Receiver<WaiterOutcome> {
    let (probe, _handle) = local_sync::mpsc::spawn(8, |msg: ProbeMsg| match msg {
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
        let _keep = _handle;
        let _ = tx.send(pending.wait().map_err(|e| format!("{e:?}")));
    });
    // Let the probe actor register before the caller proceeds.
    std::thread::sleep(Duration::from_millis(50));
    rx
}

// ------------------------------------------------------------ P2-4 admission

#[test]
fn concurrent_duplicate_starts_admit_and_fan_out_exactly_once() {
    let journal = Arc::new(ProbeJournal::default());
    let (bus, seen) = durable_bus(Arc::clone(&journal));
    let ctx = context_at(1_001, "a", SagaContext::now_millis());
    let threads = 8;
    let barrier = Arc::new(Barrier::new(threads));
    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let (bus, ctx, barrier) = (bus.clone(), ctx.clone(), Arc::clone(&barrier));
            std::thread::spawn(move || {
                barrier.wait();
                bus.publish_strict(started(&ctx)).is_ok()
            })
        })
        .collect();
    let admitted = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .filter(|ok| *ok)
        .count();
    assert_eq!(admitted, 1, "exactly one concurrent start owns the run");
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(count(&seen, is_start), 1, "start fanned out once");
    assert_eq!(
        journal_count(&journal, is_start),
        1,
        "admission intent journaled exactly once (no double append)"
    );
}

#[test]
fn concurrent_distinct_runs_for_one_id_admit_exactly_one() {
    let journal = Arc::new(ProbeJournal::default());
    let (bus, seen) = durable_bus(Arc::clone(&journal));
    let base = SagaContext::now_millis();
    let threads = 6;
    let barrier = Arc::new(Barrier::new(threads));
    let handles: Vec<_> = (0..threads as u64)
        .map(|i| {
            let (bus, barrier) = (bus.clone(), Arc::clone(&barrier));
            let ctx = context_at(1_002, "a", base + i * 10);
            std::thread::spawn(move || {
                barrier.wait();
                bus.publish_strict(started(&ctx)).is_ok()
            })
        })
        .collect();
    let admitted = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .filter(|ok| *ok)
        .count();
    assert_eq!(admitted, 1);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(count(&seen, is_start), 1);
}

#[test]
fn back_to_back_start_for_active_id_is_refused_without_waiting_for_the_journal() {
    let journal = Arc::new(ProbeJournal::default());
    let (bus, seen) = durable_bus(journal);
    let base = SagaContext::now_millis();
    bus.publish_strict(started(&context_at(1_003, "a", base)))
        .unwrap();
    // No pause: the resolver actor may not yet have journaled the first start.
    let err = bus
        .publish_strict(started(&context_at(1_003, "a", base + 5_000)))
        .unwrap_err();
    assert!(
        matches!(&err, SagaBusPublishError::AdmissionRejected { reason, .. }
            if reason.contains("unresolved")),
        "{err:?}"
    );
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(count(&seen, is_start), 1);
}

#[test]
fn start_admission_reads_history_once_not_per_start() {
    let journal = Arc::new(ProbeJournal::default());
    let (bus, seen) = durable_bus(Arc::clone(&journal));
    let base = SagaContext::now_millis();
    for id in 0..25 {
        bus.publish_strict(started(&context_at(2_000 + id, "a", base)))
            .unwrap();
    }
    // Refusals consult the same index.
    assert!(
        bus.publish_strict(started(&context_at(2_000, "a", base + 1)))
            .is_err()
    );
    assert!(wait_until(Duration::from_secs(1), || count(
        &seen, is_start
    ) == 25));
    assert_eq!(
        journal.reads.load(Ordering::SeqCst),
        1,
        "history is read once at attach, never per start"
    );
}

#[test]
fn append_failure_rejects_start_before_any_effect_and_allows_retry() {
    let journal = Arc::new(ProbeJournal::default());
    let (bus, seen) = durable_bus(Arc::clone(&journal));
    let ctx = context_at(1_004, "a", SagaContext::now_millis());
    journal.fail_appends.store(true, Ordering::SeqCst);
    let err = bus.publish_strict(started(&ctx)).unwrap_err();
    assert!(matches!(err, SagaBusPublishError::AdmissionRejected { .. }));
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        count(&seen, is_start),
        0,
        "no fanout without durable intent"
    );
    assert_eq!(count(&seen, is_failed), 0);
    assert!(!journal_has(&journal, |_| true), "nothing was journaled");

    // A failed reservation leaves no phantom ownership behind.
    journal.fail_appends.store(false, Ordering::SeqCst);
    bus.publish_strict(started(&ctx)).unwrap();
    assert!(wait_until(Duration::from_secs(1), || count(
        &seen, is_start
    ) == 1));
    assert_eq!(journal_count(&journal, is_start), 1);
}

#[test]
fn start_published_from_a_subscriber_during_fanout_does_not_deadlock() {
    let journal = Arc::new(ProbeJournal::default());
    let (bus, seen) = durable_bus(journal);
    let inner_bus = bus.clone();
    let base = SagaContext::now_millis();
    bus.subscribe_saga_type_fn(TYPE, move |event| {
        if let SagaChoreographyEvent::SagaStarted { context, .. } = event
            && context.saga_id == SagaId(3_001)
        {
            // Reentrant admission while the outer fanout is in progress.
            let _ = inner_bus.publish_strict(started(&context_at(3_002, "a", base)));
        }
        true
    });
    let (tx, rx) = mpsc::channel();
    let outer = bus.clone();
    std::thread::spawn(move || {
        let _ = tx.send(outer.publish_strict(started(&context_at(3_001, "a", base))));
    });
    let result = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("reentrant publish must not deadlock admission");
    assert!(result.is_ok());
    assert!(wait_until(Duration::from_secs(1), || count(
        &seen, is_start
    ) == 2));
}

// ------------------------------------------------------------ P2-3 waiters

#[test]
fn refused_start_does_not_resolve_or_poison_the_active_owners_waiter() {
    let journal = Arc::new(ProbeJournal::default());
    let (bus, _seen) = durable_bus(journal);
    let base = SagaContext::now_millis();
    let owner = context_at(1_005, "a", base);
    let rx = waiter(&bus, owner.saga_id, None);
    bus.publish_strict(started(&owner)).unwrap();

    let refused = context_at(1_005, "a", base + 5_000);
    assert!(bus.publish_strict(started(&refused)).is_err());
    assert!(
        rx.recv_timeout(Duration::from_millis(400)).is_err(),
        "owner waiter must stay pending after a refused foreign start"
    );

    // The owner still resolves normally.
    bus.publish_strict(step_completed(&owner, "b", true))
        .unwrap();
    let resolved = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("owner waiter resolves with the owner's terminal")
        .expect("ask completed")
        .expect("terminal reply, not an admission error");
    assert!(matches!(
        resolved.outcome,
        SagaTerminalOutcome::Completed { .. }
    ));
}

// ------------------------------------------------------------ P2-8 fencing

/// Journal with a terminal run, then a fresh bus: the resolver cache starts
/// empty exactly as after eviction, so only the durable fence can protect it.
fn terminal_history(id: u64, t0: u64) -> (Arc<ProbeJournal>, SagaContext) {
    let journal = Arc::new(ProbeJournal::default());
    let ctx = context_at(id, "a", t0);
    journal.inner.append(started(&ctx)).unwrap();
    journal
        .inner
        .append(SagaChoreographyEvent::SagaFailed {
            context: ctx.next_step("resolver".into()),
            reason: "ordinary".into(),
            failure: None,
        })
        .unwrap();
    (journal, ctx)
}

#[test]
fn non_start_replay_for_terminal_run_is_dropped_after_cache_loss() {
    let (journal, ctx) = terminal_history(4_001, 1_000_000);
    let (bus, seen) = durable_bus(Arc::clone(&journal));
    let before = journal.inner.read_all().unwrap().len();
    // Would make a fresh resolver state emit a contradictory terminal failure.
    bus.publish_strict(SagaChoreographyEvent::StepFailed {
        context: ctx.next_step("a".into()),
        participant_id: "a".into(),
        error: "late".into(),
        error_code: None,
        requires_compensation: false,
    })
    .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        count(&seen, is_failed),
        0,
        "no contradictory terminal output"
    );
    assert_eq!(count(&seen, is_quarantined), 0);
    assert_eq!(
        journal.inner.read_all().unwrap().len(),
        before,
        "fenced replay is not journaled"
    );
}

#[test]
fn late_compensable_completion_after_terminal_fence_quarantines_and_retains_evidence() {
    let (journal, ctx) = terminal_history(4_002, 1_000_000);
    let (bus, seen) = durable_bus(Arc::clone(&journal));
    bus.publish_strict(step_completed(&ctx, "a", true)).unwrap();
    assert!(
        wait_until(Duration::from_secs(2), || count(&seen, is_quarantined) == 1),
        "late effect evidence escalates to quarantine"
    );
    assert!(wait_until(Duration::from_secs(1), || journal_has(
        &journal,
        is_quarantined
    )));
    assert!(journal_has(&journal, |e| matches!(
        e,
        SagaChoreographyEvent::StepCompleted {
            compensation_available: true,
            ..
        }
    )));
    // The id is now quarantined: reuse is blocked.
    let err = bus
        .publish_strict(started(&context_at(4_002, "a", 9_000_000)))
        .unwrap_err();
    assert!(matches!(err, SagaBusPublishError::AdmissionRejected { .. }));
    // Further late evidence is retained without re-escalation.
    bus.publish_strict(step_completed(&ctx, "b", true)).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(count(&seen, is_quarantined), 1);
}

// ------------------------------------------------------------ run-scoped API

#[test]
fn old_terminal_does_not_resolve_later_run_waiter_or_outcome() {
    let journal = Arc::new(ProbeJournal::default());
    let (bus, _seen) = durable_bus(journal);
    let base = SagaContext::now_millis();
    let first = context_at(5_001, "a", base);
    bus.publish_strict(started(&first)).unwrap();
    bus.publish_strict(step_completed(&first, "b", true))
        .unwrap();
    assert!(wait_until(Duration::from_secs(2), || {
        matches!(
            bus.take_terminal_outcome_for_run(&first),
            Some(SagaTerminalOutcome::Completed { .. })
        )
    }));
    // Outcome is consumed exactly once.
    assert!(bus.take_terminal_outcome_for_run(&first).is_none());

    let second = context_at(5_001, "a", base + 10_000);
    // Valid later reuse: strictly later run of an ordinarily resolved id.
    let rx = waiter(&bus, second.saga_id, Some(&second));
    bus.publish_strict(started(&second)).unwrap();
    assert!(
        bus.take_terminal_outcome_for_run(&second).is_none(),
        "first run's terminal must not leak into the second run"
    );
    // A late replay of the old run's terminal must not resolve the new waiter.
    let _ = bus.publish(SagaChoreographyEvent::SagaFailed {
        context: first.next_step("resolver".into()),
        reason: "stale".into(),
        failure: None,
    });
    assert!(rx.recv_timeout(Duration::from_millis(400)).is_err());

    bus.publish_strict(step_completed(&second, "b", true))
        .unwrap();
    let reply = rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        reply.outcome,
        SagaTerminalOutcome::Completed { .. }
    ));
}

#[test]
fn refused_start_rejects_only_its_own_run_waiter() {
    let journal = Arc::new(ProbeJournal::default());
    let (bus, _seen) = durable_bus(journal);
    let base = SagaContext::now_millis();
    let owner = context_at(5_002, "a", base);
    let owner_rx = waiter(&bus, owner.saga_id, Some(&owner));
    bus.publish_strict(started(&owner)).unwrap();

    let foreign = context_at(5_002, "a", base + 1);
    let foreign_rx = waiter(&bus, foreign.saga_id, Some(&foreign));
    assert!(bus.publish_strict(started(&foreign)).is_err());
    let rejected = foreign_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("the refused run's own waiter observes the refusal")
        .expect("ask completed");
    assert!(rejected.is_err());
    assert!(
        owner_rx.recv_timeout(Duration::from_millis(300)).is_err(),
        "owner waiter untouched"
    );
}

#[test]
fn unreadable_history_fails_attach_so_no_start_can_be_admitted() {
    struct Unreadable;
    impl TerminalResolverJournal for Unreadable {
        fn append(&self, _: SagaChoreographyEvent) -> Result<u64, TerminalResolverJournalError> {
            Ok(0)
        }
        fn read_all(
            &self,
        ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
            Err(TerminalResolverJournalError::Storage("read down".into()))
        }
    }
    let (bus, seen) = prepared_bus();
    assert!(
        bus.attach_durable_terminal_resolver_for_contract::<ReviewContract, _>(
            "qa",
            Arc::new(Unreadable)
        )
        .is_err()
    );
    // No resolver owns the type, so nothing was indexed or admitted durably.
    assert_eq!(count(&seen, is_start), 0);
}

#[test]
fn a_known_completed_effect_replay_does_not_quarantine_healthy_terminal_history() {
    let journal = Arc::new(ProbeJournal::default());
    let c = context_at(6_001, "a", 1_000_000);
    let completed = step_completed(&c, "b", true);
    journal.inner.append(started(&c)).unwrap();
    journal.inner.append(completed.clone()).unwrap();
    journal
        .inner
        .append(SagaChoreographyEvent::SagaCompleted {
            context: c.next_step("resolver".into()),
        })
        .unwrap();
    let (bus, seen) = durable_bus(Arc::clone(&journal));
    bus.publish_strict(completed).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        count(&seen, is_quarantined),
        0,
        "known result redelivery is not a new materialised effect"
    );
    assert_eq!(journal.inner.read_all().unwrap().len(), 3);
}

#[test]
fn old_quarantine_and_new_late_effect_fence_an_already_active_successor() {
    for explicit_quarantine in [true, false] {
        let (journal, old) = terminal_history(6_002 + u64::from(explicit_quarantine), 1_000_000);
        let successor = context_at(old.saga_id.get(), "a", 2_000_000);
        journal.inner.append(started(&successor)).unwrap();
        let (bus, seen) = durable_bus(journal);
        let event = if explicit_quarantine {
            SagaChoreographyEvent::SagaQuarantined {
                context: old.next_step("a".into()),
                reason: "old uncertainty".into(),
                step: "a".into(),
                participant_id: "a".into(),
            }
        } else {
            step_completed(&old, "a", true)
        };
        bus.publish_strict(event).unwrap();
        assert!(
            wait_until(Duration::from_secs(2), || count(&seen, |e| is_quarantined(
                e
            ) && e
                .context()
                .saga_started_at_millis
                == successor.saga_started_at_millis)
                > 0),
            "old-run uncertainty must fence the active successor, explicit={explicit_quarantine}"
        );
        bus.publish_strict(step_completed(&successor, "b", false))
            .unwrap();
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            count(&seen, |e| matches!(
                e,
                SagaChoreographyEvent::SagaCompleted { .. }
            ) && e.context().saga_started_at_millis
                == successor.saga_started_at_millis),
            0
        );
    }
}

#[test]
fn legacy_waiter_is_bound_by_type_id_and_start_not_just_start() {
    let (bus, _) = durable_bus(Arc::new(ProbeJournal::default()));
    let owner = context_at(6_004, "a", SagaContext::now_millis());
    let rx = waiter(&bus, owner.saga_id, None);
    bus.publish_strict(started(&owner)).unwrap();
    let mut foreign = owner.clone();
    foreign.saga_type = "other-type".into();
    let foreign_terminal = SagaChoreographyEvent::SagaCompleted { context: foreign };
    bus.complete_terminal_reply_for_run(
        foreign_terminal.context(),
        SagaReplyTo {
            responder: "other-resolver".into(),
            outcome: foreign_terminal.terminal_outcome().unwrap(),
        },
    );
    assert!(
        rx.recv_timeout(Duration::from_millis(300)).is_err(),
        "same timestamp in another saga type must not satisfy the owner"
    );
    bus.publish_strict(step_completed(&owner, "b", false))
        .unwrap();
    assert!(
        rx.recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap()
            .is_ok()
    );
}

#[test]
fn legacy_outcome_cannot_return_the_previous_run_while_successor_is_active() {
    let (journal, old) = terminal_history(6_005, 1_000_000);
    let (bus, _) = durable_bus(journal);
    bus.complete_terminal_reply_for_run(
        &old,
        SagaReplyTo {
            responder: "resolver".into(),
            outcome: SagaTerminalOutcome::Failed {
                context: old.clone(),
                reason: "old failure".into(),
                failure: None,
            },
        },
    );
    let successor = context_at(old.saga_id.get(), "a", 2_000_000);
    bus.publish_strict(started(&successor)).unwrap();
    assert!(
        bus.take_terminal_outcome(successor.saga_id).is_none(),
        "the current active run has no outcome yet"
    );
    assert!(
        matches!(
            bus.take_terminal_outcome_for_run(&old),
            Some(SagaTerminalOutcome::Failed { .. })
        ),
        "explicit old-run lookup may still retrieve old history"
    );
}

#[test]
fn cached_terminal_outcomes_preserve_first_ordinary_result_and_quarantine_dominance() {
    let bus = SagaChoreographyBus::new();
    let c = context_at(6_006, "a", 1_000_000);
    bus.publish(SagaChoreographyEvent::SagaCompleted { context: c.clone() });
    bus.publish(SagaChoreographyEvent::SagaFailed {
        context: c.clone(),
        reason: "contradictory replay".into(),
        failure: None,
    });
    assert!(matches!(
        bus.take_terminal_outcome_for_run(&c),
        Some(SagaTerminalOutcome::Completed { .. })
    ));
    bus.publish(SagaChoreographyEvent::SagaQuarantined {
        context: c.clone(),
        reason: "late uncertainty".into(),
        step: "a".into(),
        participant_id: "a".into(),
    });
    bus.publish(SagaChoreographyEvent::SagaFailed {
        context: c.clone(),
        reason: "stale ordinary failure".into(),
        failure: None,
    });
    assert!(matches!(
        bus.take_terminal_outcome_for_run(&c),
        Some(SagaTerminalOutcome::Quarantined { .. })
    ));
}

#[test]
fn ephemeral_resolver_also_serializes_concurrent_live_admission() {
    let (bus, seen) = prepared_bus();
    bus.attach_terminal_resolver_for_contract::<ReviewContract>("ephemeral")
        .unwrap();
    let c = context_at(6_007, "a", SagaContext::now_millis());
    let barrier = Arc::new(Barrier::new(6));
    let threads: Vec<_> = (0..6)
        .map(|_| {
            let (bus, c, barrier) = (bus.clone(), c.clone(), Arc::clone(&barrier));
            std::thread::spawn(move || {
                barrier.wait();
                bus.publish_strict(started(&c)).is_ok()
            })
        })
        .collect();
    let admitted = threads
        .into_iter()
        .map(|h| h.join().unwrap())
        .filter(|ok| *ok)
        .count();
    assert_eq!(admitted, 1);
    assert_eq!(count(&seen, is_start), 1);
}
