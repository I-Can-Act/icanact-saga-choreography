//! Review-4 bus regressions: compacted failed recovery must not invent output,
//! failed-run fingerprint reclamation, and the narrow release signal.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use icanact_saga_choreography::*;

macro_rules! contract {
    ($name:ident, $type:literal, $authority:expr, $criteria:expr, [$(($step:literal, $dep:expr)),+ $(,)?]) => {
        struct $name;
        impl SagaWorkflowContract for $name {
            fn saga_type() -> &'static str {
                $type
            }
            fn first_step() -> &'static str {
                "a"
            }
            fn steps() -> &'static [SagaWorkflowStepContract] {
                &[$(SagaWorkflowStepContract {
                    step_name: $step,
                    participant_id: $step,
                    depends_on: $dep,
                }),+]
            }
            fn terminal_policy() -> TerminalPolicy {
                TerminalPolicy::new(
                    $type.into(),
                    concat!($type, "/policy").into(),
                    $authority,
                    $criteria,
                    Duration::from_secs(60),
                    Duration::from_secs(60),
                    Self::steps(),
                )
            }
        }
    };
}

fn set(steps: &[&str]) -> HashSet<Box<str>> {
    steps.iter().map(|s| Box::<str>::from(*s)).collect()
}

contract!(
    RollbackContract,
    "qa_r4_rollback",
    FailureAuthority::OnlySteps(set(&["b", "x"])),
    SuccessCriteria::AnyOf(set(&["x"])),
    [
        ("a", WorkflowDependencySpec::OnSagaStart),
        ("b", WorkflowDependencySpec::After("a")),
        ("x", WorkflowDependencySpec::After("a"))
    ]
);

contract!(
    OtherContract,
    "qa_r4_other",
    FailureAuthority::AnyParticipant,
    SuccessCriteria::AllOf(set(&["b"])),
    [
        ("a", WorkflowDependencySpec::OnSagaStart),
        ("b", WorkflowDependencySpec::After("a"))
    ]
);

#[cfg(feature = "lmdb")]
mod lmdb {
    use super::*;
    use std::time::Instant;

    fn context_for(saga_type: &str, id: u64, step: &str, started_at: u64) -> SagaContext {
        SagaContext {
            saga_id: SagaId(id),
            saga_type: saga_type.into(),
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

    fn undo_done(ctx: &SagaContext, step: &str) -> SagaChoreographyEvent {
        SagaChoreographyEvent::CompensationCompleted {
            context: ctx.next_step(step.into()),
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
    #[cfg(feature = "lmdb")]
    fn is_failed(e: &SagaChoreographyEvent) -> bool {
        matches!(e, SagaChoreographyEvent::SagaFailed { .. })
    }
    fn is_quarantined(e: &SagaChoreographyEvent) -> bool {
        matches!(e, SagaChoreographyEvent::SagaQuarantined { .. })
    }
    fn is_undo_request(e: &SagaChoreographyEvent) -> bool {
        matches!(e, SagaChoreographyEvent::CompensationRequested { .. })
    }

    fn bus_for<C: SagaWorkflowContract>() -> (SagaChoreographyBus, Seen) {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<C>().unwrap();
        for step in C::steps() {
            bus.register_bound_workflow_step(C::saga_type(), step.step_name)
                .unwrap();
        }
        let seen: Seen = Arc::default();
        let sink = Arc::clone(&seen);
        bus.subscribe_saga_type_fn(C::saga_type(), move |event| {
            sink.lock().unwrap().push(event.clone());
            true
        });
        for _ in 0..3 {
            bus.subscribe_saga_type_fn(C::saga_type(), |_| true);
        }
        (bus, seen)
    }

    fn durable<C: SagaWorkflowContract, J: TerminalResolverJournal>(
        journal: Arc<J>,
    ) -> (SagaChoreographyBus, Seen) {
        let (bus, seen) = bus_for::<C>();
        bus.attach_durable_terminal_resolver_for_contract::<C, _>("qa", journal)
            .unwrap();
        bus.activate_terminal_resolver_recovery_for_contract::<C>()
            .unwrap();
        (bus, seen)
    }

    /// Absorbed sibling during rollback: the live run ends `SagaFailed`.
    fn drive_failed_parallel_rollback(bus: &SagaChoreographyBus, seen: &Seen, ctx: &SagaContext) {
        bus.publish_strict(started(ctx)).unwrap();
        bus.publish_strict(step_completed(ctx, "a", true)).unwrap();
        bus.publish_strict(SagaChoreographyEvent::StepFailed {
            context: ctx.next_step("b".into()),
            participant_id: "b".into(),
            error_code: None,
            error: "boom".into(),
            requires_compensation: true,
        })
        .unwrap();
        assert!(wait_until(Duration::from_secs(2), || {
            count(seen, is_undo_request) == 1
        }));
        // x completes during the rollback and is absorbed into it.
        bus.publish_strict(step_completed(ctx, "x", true)).unwrap();
        bus.publish_strict(undo_done(ctx, "a")).unwrap();
        assert!(wait_until(Duration::from_secs(2), || {
            count(seen, is_undo_request) == 2
        }));
        bus.publish_strict(undo_done(ctx, "x")).unwrap();
        assert!(
            wait_until(Duration::from_secs(2), || count(seen, is_failed) == 1),
            "live run ends with SagaFailed"
        );
        assert_eq!(count(seen, is_completed), 0);
        assert_eq!(count(seen, is_quarantined), 0);
    }

    #[test]
    fn compacted_failed_parallel_rollback_reopens_without_invented_output() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context_for("qa_r4_rollback", 8_001, "a", 1_000_000);
        {
            let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
            let (bus, seen) = durable::<RollbackContract, _>(Arc::clone(&journal));
            drive_failed_parallel_rollback(&bus, &seen, &ctx);
            assert!(journal.compact_terminal_detail().unwrap() > 0);
        }
        // Two restarts: output must not be re-published on every restart.
        for restart in 0..2 {
            let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
            let (bus, seen) = bus_for::<RollbackContract>();
            bus.attach_durable_terminal_resolver_for_contract::<RollbackContract, _>(
                "qa",
                Arc::clone(&journal),
            )
            .unwrap();
            bus.activate_terminal_resolver_recovery_for_contract::<RollbackContract>()
                .unwrap();
            std::thread::sleep(Duration::from_millis(400));
            assert_eq!(count(&seen, is_completed), 0, "restart {restart}");
            assert_eq!(count(&seen, is_quarantined), 0, "restart {restart}");
            assert_eq!(count(&seen, is_undo_request), 0, "restart {restart}");
            assert_eq!(seen.lock().unwrap().len(), 0, "restart {restart}");
            assert!(
                bus.take_terminal_reply_for_run(&ctx).is_none(),
                "no run-scoped outcome is invented (restart {restart})"
            );
            assert!(
                !journal
                    .read_all()
                    .unwrap()
                    .iter()
                    .any(|entry| is_quarantined(&entry.event)),
                "no quarantine journaled (restart {restart})"
            );

            // A failed run's fence stays: a new compensable effect escalates...
            // (checked only on the last restart so the earlier one stays quiet.)
            if restart == 1 {
                // ...but an allowed successor is not wedged by internal state.
                let successor = context_for("qa_r4_rollback", 8_001, "a", 2_000_000);
                bus.publish_strict(started(&successor)).unwrap();
                bus.publish_strict(step_completed(&successor, "a", true))
                    .unwrap();
                bus.publish_strict(step_completed(&successor, "x", true))
                    .unwrap();
                assert!(
                    wait_until(Duration::from_secs(2), || count(&seen, is_completed) == 1),
                    "successor completes normally"
                );
                assert_eq!(count(&seen, is_quarantined), 0);
            }
        }
    }

    #[test]
    fn compacted_failed_run_keeps_its_fingerprints_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context_for("qa_r4_rollback", 8_002, "a", 1_000_000);
        let known_x = {
            let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
            let (bus, seen) = durable::<RollbackContract, _>(Arc::clone(&journal));
            drive_failed_parallel_rollback(&bus, &seen, &ctx);
            journal.compact_terminal_detail().unwrap();
            // The exact journaled event a participant would redeliver.
            journal
                .read_all()
                .unwrap()
                .into_iter()
                .map(|entry| entry.event)
                .find(|event| {
                    matches!(event, SagaChoreographyEvent::StepCompleted { .. })
                        && event.context().step_name.as_ref() == "x"
                })
                .expect("compaction keeps the absorbed sibling's fingerprint")
        };
        let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
        let (bus, seen) = durable::<RollbackContract, _>(journal);
        // Known replay of the absorbed sibling is quiet.
        bus.publish_strict(known_x).unwrap();
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(count(&seen, is_quarantined), 0);
        // A genuinely new effect still escalates.
        bus.publish_strict(step_completed(&ctx, "fresh", true))
            .unwrap();
        assert!(wait_until(Duration::from_secs(2), || count(
            &seen,
            is_quarantined
        ) == 1));
    }

    #[test]
    fn recovered_older_quarantine_still_fences_an_already_admitted_successor() {
        let dir = tempfile::tempdir().unwrap();
        let old = context_for("qa_r4_rollback", 8_005, "a", 1_000_000);
        let successor = context_for("qa_r4_rollback", 8_005, "a", SagaContext::now_millis());
        {
            let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
            let (bus, seen) = durable::<RollbackContract, _>(Arc::clone(&journal));
            drive_failed_parallel_rollback(&bus, &seen, &old);
            journal.compact_terminal_detail().unwrap();
            // WAL reservation, then old-run quarantine persisted before propagation
            // to the already-admitted successor: simulate the crash boundary.
            journal.append(started(&successor)).unwrap();
            journal
                .append(SagaChoreographyEvent::SagaQuarantined {
                    context: old.next_step(TERMINAL_RESOLVER_STEP.into()),
                    reason: "late old-run uncertainty before propagation".into(),
                    step: "fresh".into(),
                    participant_id: "fresh".into(),
                })
                .unwrap();
            let release = bus.release_waiter();
            drop(bus);
            assert!(release.wait_timeout(Duration::from_secs(5)));
        }
        let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
        let (bus, seen) = durable::<RollbackContract, _>(journal);
        assert!(wait_until(Duration::from_secs(2), || {
            seen.lock().unwrap().iter().any(|event| {
                is_quarantined(event)
                    && event.context().saga_started_at_millis == successor.saga_started_at_millis
            })
        }));
        assert_eq!(count(&seen, is_completed), 0);
        assert_eq!(count(&seen, is_undo_request), 0);
        assert!(matches!(
            bus.take_terminal_outcome_for_run(&successor),
            Some(SagaTerminalOutcome::Quarantined { .. })
        ));
    }

    #[test]
    fn failed_then_compacted_then_quarantined_history_never_republishes_success() {
        let dir = tempfile::tempdir().unwrap();
        let old = context_for("qa_r4_rollback", 8_003, "a", 1_000_000);
        {
            let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
            let (bus, seen) = durable::<RollbackContract, _>(Arc::clone(&journal));
            drive_failed_parallel_rollback(&bus, &seen, &old);
            journal.compact_terminal_detail().unwrap();
            bus.publish_strict(step_completed(&old, "fresh", true))
                .unwrap();
            assert!(wait_until(Duration::from_secs(2), || count(
                &seen,
                is_quarantined
            ) == 1));
            let released = bus.release_waiter();
            drop(bus);
            assert!(released.wait_timeout(Duration::from_secs(5)));
        }
        let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
        let (bus, seen) = durable::<RollbackContract, _>(journal);
        // A different live run is an inbox barrier after recovery activation.
        let barrier = context_for("qa_r4_rollback", 8_004, "a", SagaContext::now_millis());
        bus.publish_strict(started(&barrier)).unwrap();
        bus.publish_strict(step_completed(&barrier, "a", true))
            .unwrap();
        bus.publish_strict(step_completed(&barrier, "x", true))
            .unwrap();
        assert!(wait_until(Duration::from_secs(2), || {
            seen.lock()
                .unwrap()
                .iter()
                .any(|event| is_completed(event) && event.context().saga_id == barrier.saga_id)
        }));
        let invented: Vec<_> = seen
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.context().saga_id == old.saga_id)
            .cloned()
            .collect();
        assert!(
            invented.is_empty(),
            "closed quarantine replay invented output: {invented:?}"
        );
        assert!(bus.take_terminal_reply_for_run(&old).is_none());
    }
}

// ------------------------------------------------------ release waiter
// Characterization of the new API (not compiler-error RED).

const SHORT: Duration = Duration::from_millis(100);
const LONG: Duration = Duration::from_secs(5);

#[test]
fn release_waiter_is_false_while_the_bus_lives_and_true_after_last_drop() {
    let bus = SagaChoreographyBus::new();
    let waiter = bus.release_waiter();
    assert!(!waiter.wait_timeout(SHORT), "public owner still alive");
    // The waiter alone is not an owner.
    drop(bus);
    assert!(waiter.wait_timeout(LONG), "bus without resolvers releases");
    assert!(waiter.wait_timeout(SHORT), "release is sticky");
}

#[test]
fn release_waits_for_every_public_clone_and_multiple_contracts() {
    let bus = SagaChoreographyBus::new();
    let first = Arc::new(InMemoryTerminalResolverJournal::default());
    let second = Arc::new(InMemoryTerminalResolverJournal::default());
    bus.attach_durable_terminal_resolver_for_contract::<RollbackContract, _>(
        "qa",
        Arc::clone(&first),
    )
    .unwrap();
    bus.attach_durable_terminal_resolver_for_contract::<OtherContract, _>(
        "qa",
        Arc::clone(&second),
    )
    .unwrap();
    let waiter = bus.release_waiter();
    let clone = bus.clone();
    drop(bus);
    assert!(!waiter.wait_timeout(SHORT), "a public clone remains");
    assert!(Arc::strong_count(&first) > 1);
    drop(clone);
    assert!(waiter.wait_timeout(LONG));
    assert_eq!(Arc::strong_count(&first), 1, "journal released on true");
    assert_eq!(Arc::strong_count(&second), 1, "journal released on true");
}

#[test]
fn release_signal_fires_when_the_last_owner_drops_in_a_native_callback() {
    use icanact_core::local_sync::{self, SyncActor};
    enum DropMessage {
        Bus(SagaChoreographyBus),
    }
    impl icanact_core::TellAskTell for DropMessage {}
    struct DropActor;
    impl SyncActor for DropActor {
        type Contract = local_sync::contract::TellOnly;
        type Tell = DropMessage;
        type Ask = ();
        type Reply = ();
        type Channel = ();
        type PubSub = ();
        type Broadcast = ();
        fn handle_tell(&mut self, message: DropMessage) {
            let DropMessage::Bus(bus) = message;
            drop(bus);
        }
    }
    let bus = SagaChoreographyBus::new();
    let journal = Arc::new(InMemoryTerminalResolverJournal::default());
    bus.attach_durable_terminal_resolver_for_contract::<RollbackContract, _>(
        "qa",
        Arc::clone(&journal),
    )
    .unwrap();
    let waiter = bus.release_waiter();
    let (actor, handle) = local_sync::spawn(DropActor);
    assert!(actor.tell(DropMessage::Bus(bus)));
    let released = waiter.wait_timeout(LONG);
    handle.shutdown();
    assert!(released, "callback-dropped bus must signal actual release");
    assert_eq!(Arc::strong_count(&journal), 1);
}

#[test]
fn release_waiter_waits_for_bus_owned_subscriber_resource_destruction() {
    use icanact_core::local_sync::{self, SyncActor};
    use std::sync::mpsc;

    struct BlockingDrop {
        dropping: mpsc::Sender<()>,
        finish: Mutex<mpsc::Receiver<()>>,
    }
    impl Drop for BlockingDrop {
        fn drop(&mut self) {
            let _ = self.dropping.send(());
            let _ = self
                .finish
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(3));
        }
    }
    enum DropMessage {
        Bus(SagaChoreographyBus),
    }
    impl icanact_core::TellAskTell for DropMessage {}
    struct DropActor;
    impl SyncActor for DropActor {
        type Contract = local_sync::contract::TellOnly;
        type Tell = DropMessage;
        type Ask = ();
        type Reply = ();
        type Channel = ();
        type PubSub = ();
        type Broadcast = ();
        fn handle_tell(&mut self, message: DropMessage) {
            let DropMessage::Bus(bus) = message;
            drop(bus);
        }
    }
    let bus = SagaChoreographyBus::new();
    let journal = Arc::new(InMemoryTerminalResolverJournal::default());
    bus.attach_durable_terminal_resolver_for_contract::<RollbackContract, _>("qa", journal)
        .unwrap();
    let (dropping, entered_drop) = mpsc::channel();
    let (finish, unblock) = mpsc::channel();
    let resource = Arc::new(BlockingDrop {
        dropping,
        finish: Mutex::new(unblock),
    });
    let owned = Arc::clone(&resource);
    bus.subscribe_saga_type_fn("qa_r4_rollback", move |_| {
        let _keep_alive = &owned;
        true
    });
    drop(resource);
    let waiter = bus.release_waiter();
    let (actor, handle) = local_sync::spawn(DropActor);
    assert!(actor.tell(DropMessage::Bus(bus)));
    entered_drop
        .recv_timeout(Duration::from_secs(3))
        .expect("subscriber destructor entered");
    let premature = waiter.wait_timeout(Duration::from_millis(100));
    let _ = finish.send(()); // Always unblock before asserting or shutting down.
    assert!(waiter.wait_timeout(LONG));
    handle.shutdown();
    assert!(
        !premature,
        "release signalled while a bus-owned subscriber destructor was blocked"
    );
}

#[cfg(feature = "lmdb")]
#[test]
fn lmdb_can_be_reopened_after_observed_release() {
    let dir = tempfile::tempdir().unwrap();
    let bus = SagaChoreographyBus::new();
    let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
    bus.attach_durable_terminal_resolver_for_contract::<RollbackContract, _>(
        "qa",
        Arc::clone(&journal),
    )
    .unwrap();
    bus.activate_terminal_resolver_recovery_for_contract::<RollbackContract>()
        .unwrap();
    let waiter = bus.release_waiter();
    drop(bus);
    assert!(waiter.wait_timeout(LONG));
    assert_eq!(
        Arc::strong_count(&journal),
        1,
        "only the application's handle remains"
    );
    drop(journal);
    LmdbTerminalResolverJournal::open(dir.path()).expect("reopen after observed release");
}
