//! Re-challenge regressions for the resolver bus: a successful terminal must not
//! turn healthy trailing work into quarantine, genuinely new uncertainty after a
//! failure must still escalate, and the id-scoped compatibility waiter must bind
//! to its unique active run.

use std::collections::HashSet;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use icanact_core::local_sync;
use icanact_saga_choreography::*;

macro_rules! contract {
    ($name:ident, $type:literal, $criteria:expr, [$(($step:literal, $dep:expr)),+ $(,)?]) => {
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
                    FailureAuthority::AnyParticipant,
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
    AnyOfContract,
    "qa_rc_any",
    SuccessCriteria::AnyOf(set(&["a", "b"])),
    [
        ("a", WorkflowDependencySpec::OnSagaStart),
        ("b", WorkflowDependencySpec::OnSagaStart)
    ]
);
contract!(
    QuorumContract,
    "qa_rc_quorum",
    SuccessCriteria::Quorum {
        group_steps: set(&["a", "b", "c"]),
        required_count: 2
    },
    [
        ("a", WorkflowDependencySpec::OnSagaStart),
        ("b", WorkflowDependencySpec::OnSagaStart),
        ("c", WorkflowDependencySpec::OnSagaStart)
    ]
);
contract!(
    TrailingContract,
    "qa_rc_trailing",
    SuccessCriteria::AllOf(set(&["b"])),
    [
        ("a", WorkflowDependencySpec::OnSagaStart),
        ("b", WorkflowDependencySpec::After("a")),
        ("c", WorkflowDependencySpec::After("b"))
    ]
);
contract!(
    OwnerOneContract,
    "qa_rc_owner_one",
    SuccessCriteria::AllOf(set(&["b"])),
    [
        ("a", WorkflowDependencySpec::OnSagaStart),
        ("b", WorkflowDependencySpec::After("a"))
    ]
);
contract!(
    OwnerTwoContract,
    "qa_rc_owner_two",
    SuccessCriteria::AllOf(set(&["b"])),
    [
        ("a", WorkflowDependencySpec::OnSagaStart),
        ("b", WorkflowDependencySpec::After("a"))
    ]
);

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

fn bus_for<C: SagaWorkflowContract>(
    attach: impl FnOnce(&SagaChoreographyBus),
) -> (SagaChoreographyBus, Seen) {
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
    // Required-path delivery needs more subscribers than the capture sink.
    for _ in 0..3 {
        bus.subscribe_saga_type_fn(C::saga_type(), |_| true);
    }
    attach(&bus);
    (bus, seen)
}

fn ephemeral<C: SagaWorkflowContract>() -> (SagaChoreographyBus, Seen) {
    bus_for::<C>(|bus| {
        bus.attach_terminal_resolver_for_contract::<C>("qa")
            .unwrap();
    })
}

fn durable<C: SagaWorkflowContract, J: TerminalResolverJournal>(
    journal: Arc<J>,
) -> (SagaChoreographyBus, Seen) {
    bus_for::<C>(|bus| {
        bus.attach_durable_terminal_resolver_for_contract::<C, _>("qa", journal)
            .unwrap();
        bus.activate_terminal_resolver_recovery_for_contract::<C>()
            .unwrap();
    })
}

fn settle() {
    std::thread::sleep(Duration::from_millis(300));
}

// ------------------------------------------------- P1-A successful siblings

#[test]
fn anyof_sibling_after_success_does_not_manufacture_quarantine() {
    for durable_resolver in [false, true] {
        let (bus, seen) = if durable_resolver {
            durable::<AnyOfContract, _>(Arc::new(InMemoryTerminalResolverJournal::default()))
        } else {
            ephemeral::<AnyOfContract>()
        };
        let ctx = context_for("qa_rc_any", 7_001, "a", SagaContext::now_millis());
        bus.publish_strict(started(&ctx)).unwrap();
        bus.publish_strict(step_completed(&ctx, "a", true)).unwrap();
        assert!(
            wait_until(Duration::from_secs(2), || count(&seen, is_completed) == 1),
            "AnyOf completes on the first member (durable={durable_resolver})"
        );
        // The other member also started on SagaStarted and finishes later.
        let mut sibling = step_completed(&ctx, "b", true);
        if let SagaChoreographyEvent::StepCompleted { context, .. } = &mut sibling {
            context.trace_id = 99;
        }
        bus.publish_strict(sibling).unwrap();
        settle();
        assert_eq!(
            count(&seen, is_quarantined),
            0,
            "a successful run keeps its business effects (durable={durable_resolver})"
        );
        assert!(matches!(
            bus.take_terminal_outcome_for_run(&ctx),
            Some(SagaTerminalOutcome::Completed { .. })
        ));
    }
}

#[test]
fn quorum_compensable_and_accepted_siblings_after_success_stay_successful() {
    for durable_resolver in [false, true] {
        let (bus, seen) = if durable_resolver {
            durable::<QuorumContract, _>(Arc::new(InMemoryTerminalResolverJournal::default()))
        } else {
            ephemeral::<QuorumContract>()
        };
        let ctx = context_for("qa_rc_quorum", 7_002, "a", SagaContext::now_millis());
        bus.publish_strict(started(&ctx)).unwrap();
        bus.publish_strict(step_completed(&ctx, "a", true)).unwrap();
        bus.publish_strict(step_completed(&ctx, "b", true)).unwrap();
        assert!(wait_until(Duration::from_secs(2), || count(
            &seen,
            is_completed
        ) == 1));
        // The third member completes compensably, and an accepted sibling appears.
        bus.publish_strict(step_completed(&ctx, "c", true)).unwrap();
        settle();
        assert_eq!(
            count(&seen, is_quarantined),
            0,
            "durable={durable_resolver}"
        );
        assert!(matches!(
            bus.take_terminal_outcome_for_run(&ctx),
            Some(SagaTerminalOutcome::Completed { .. })
        ));
    }
}

#[test]
fn trailing_step_after_the_required_step_stays_successful() {
    let (bus, seen) = ephemeral::<TrailingContract>();
    let ctx = context_for("qa_rc_trailing", 7_003, "a", SagaContext::now_millis());
    bus.publish_strict(started(&ctx)).unwrap();
    bus.publish_strict(step_completed(&ctx, "a", true)).unwrap();
    bus.publish_strict(step_completed(&ctx, "b", true)).unwrap();
    assert!(wait_until(Duration::from_secs(2), || count(
        &seen,
        is_completed
    ) == 1));
    bus.publish_strict(step_completed(&ctx, "c", true)).unwrap();
    settle();
    assert_eq!(count(&seen, is_quarantined), 0);
}

#[test]
fn exact_confirmed_completion_after_reopen_of_successful_history_stays_successful() {
    let journal = Arc::new(InMemoryTerminalResolverJournal::default());
    let ctx = context_for("qa_rc_any", 7_004, "a", 1_000_000);
    for event in [
        started(&ctx),
        step_completed(&ctx, "a", true),
        SagaChoreographyEvent::SagaCompleted {
            context: ctx.next_step(TERMINAL_RESOLVER_STEP.into()),
        },
    ] {
        journal.append(event).unwrap();
    }
    let (bus, seen) = durable::<AnyOfContract, _>(journal);
    // Participant crashed before SagaCompleted: it resends its confirmed work,
    // and the sibling is new compensable evidence.
    bus.publish_strict(step_completed(&ctx, "a", true)).unwrap();
    let mut sibling = step_completed(&ctx, "b", true);
    if let SagaChoreographyEvent::StepCompleted { context, .. } = &mut sibling {
        context.trace_id = 99;
    }
    bus.publish_strict(sibling).unwrap();
    settle();
    assert_eq!(count(&seen, is_quarantined), 0);
}

// ----------------------------------------------- P2-c legacy waiter binding

type WaiterOutcome = Result<SagaReplyToResult, String>;

enum ProbeMsg {
    Register {
        bus: SagaChoreographyBus,
        saga_id: SagaId,
        reply: SagaReplyToHandle,
    },
}

fn legacy_waiter(bus: &SagaChoreographyBus, saga_id: SagaId) -> mpsc::Receiver<WaiterOutcome> {
    let (probe, handle) = local_sync::mpsc::spawn(8, |msg: ProbeMsg| match msg {
        ProbeMsg::Register {
            bus,
            saga_id,
            reply,
        } => {
            let _ = bus.register_terminal_reply(saga_id, reply);
        }
    });
    let pending = probe
        .ask_delegated(|reply| ProbeMsg::Register {
            bus: bus.clone(),
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

#[test]
fn legacy_waiter_registered_after_admission_binds_to_the_unique_active_run() {
    let (bus, _seen) = ephemeral::<OwnerOneContract>();
    let ctx = context_for("qa_rc_owner_one", 7_101, "a", SagaContext::now_millis());
    bus.publish_strict(started(&ctx)).unwrap();
    // Registered after the start was admitted (and so after its start time).
    std::thread::sleep(Duration::from_millis(20));
    let rx = legacy_waiter(&bus, ctx.saga_id);
    bus.publish_strict(step_completed(&ctx, "a", true)).unwrap();
    bus.publish_strict(step_completed(&ctx, "b", true)).unwrap();
    let reply = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("late-registered id waiter resolves with the active run's terminal")
        .expect("ask completed")
        .expect("terminal reply");
    assert!(matches!(
        reply.outcome,
        SagaTerminalOutcome::Completed { .. }
    ));
}

#[test]
fn legacy_waiter_does_not_choose_between_multiple_active_runs_or_stale_history() {
    let bus = SagaChoreographyBus::new();
    bus.register_workflow_contract_provider::<OwnerOneContract>()
        .unwrap();
    bus.register_workflow_contract_provider::<OwnerTwoContract>()
        .unwrap();
    for ty in ["qa_rc_owner_one", "qa_rc_owner_two"] {
        for step in ["a", "b"] {
            bus.register_bound_workflow_step(ty, step).unwrap();
        }
        for _ in 0..3 {
            bus.subscribe_saga_type_fn(ty, |_| true);
        }
    }
    bus.attach_terminal_resolver_for_contract::<OwnerOneContract>("qa")
        .unwrap();
    bus.attach_terminal_resolver_for_contract::<OwnerTwoContract>("qa")
        .unwrap();
    let base = SagaContext::now_millis();
    let one = context_for("qa_rc_owner_one", 7_102, "a", base);
    let two = context_for("qa_rc_owner_two", 7_102, "a", base);
    bus.publish_strict(started(&one)).unwrap();
    bus.publish_strict(started(&two)).unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let rx = legacy_waiter(&bus, one.saga_id);
    bus.publish_strict(step_completed(&one, "a", true)).unwrap();
    bus.publish_strict(step_completed(&one, "b", true)).unwrap();
    assert!(
        rx.recv_timeout(Duration::from_millis(500)).is_err(),
        "ownership of the bare id is ambiguous across saga types"
    );
}

#[test]
fn legacy_waiter_ignores_stale_terminal_history() {
    let (bus, _seen) = ephemeral::<OwnerOneContract>();
    let base = SagaContext::now_millis();
    let old = context_for("qa_rc_owner_one", 7_103, "a", base);
    bus.publish_strict(started(&old)).unwrap();
    bus.publish_strict(step_completed(&old, "a", true)).unwrap();
    bus.publish_strict(step_completed(&old, "b", true)).unwrap();
    assert!(wait_until(Duration::from_secs(2), || matches!(
        bus.take_terminal_outcome_for_run(&old),
        Some(SagaTerminalOutcome::Completed { .. })
    )));
    std::thread::sleep(Duration::from_millis(20));
    let rx = legacy_waiter(&bus, old.saga_id);
    // A replay of the old terminal is stale history, not the waiter's run.
    let _ = bus.publish(SagaChoreographyEvent::SagaCompleted {
        context: old.next_step(TERMINAL_RESOLVER_STEP.into()),
    });
    assert!(rx.recv_timeout(Duration::from_millis(400)).is_err());
    // A genuinely later run does resolve it.
    let later = context_for("qa_rc_owner_one", 7_103, "a", base + 60_000);
    bus.publish_strict(started(&later)).unwrap();
    bus.publish_strict(step_completed(&later, "a", true))
        .unwrap();
    bus.publish_strict(step_completed(&later, "b", true))
        .unwrap();
    assert!(
        rx.recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap()
            .is_ok()
    );
}

// --------------------------------------- failed versus successful, compacted

#[cfg(feature = "lmdb")]
mod lmdb {
    use super::*;

    fn saga_failed(ctx: &SagaContext) -> SagaChoreographyEvent {
        SagaChoreographyEvent::SagaFailed {
            context: ctx.next_step(TERMINAL_RESOLVER_STEP.into()),
            reason: "ordinary".into(),
            failure: None,
        }
    }

    fn failed_history(
        journal: &LmdbTerminalResolverJournal,
        ctx: &SagaContext,
        known: &SagaChoreographyEvent,
    ) {
        journal.append(started(ctx)).unwrap();
        journal.append(known.clone()).unwrap();
        journal.append(saga_failed(ctx)).unwrap();
    }

    #[test]
    fn successful_history_compacted_and_reopened_keeps_siblings_successful() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context_for("qa_rc_any", 7_201, "a", 1_000_000);
        {
            let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
            journal.append(started(&ctx)).unwrap();
            journal.append(step_completed(&ctx, "a", true)).unwrap();
            journal
                .append(SagaChoreographyEvent::SagaCompleted {
                    context: ctx.next_step(TERMINAL_RESOLVER_STEP.into()),
                })
                .unwrap();
            assert!(journal.compact_terminal_detail().unwrap() > 0);
        }
        let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
        let (bus, seen) = durable::<AnyOfContract, _>(journal);
        // The exact confirmed completion resent after the participant restart.
        bus.publish_strict(step_completed(&ctx, "a", true)).unwrap();
        let mut sibling = step_completed(&ctx, "b", true);
        if let SagaChoreographyEvent::StepCompleted { context, .. } = &mut sibling {
            context.trace_id = 99;
        }
        bus.publish_strict(sibling).unwrap();
        settle();
        assert_eq!(count(&seen, is_quarantined), 0);
    }

    #[test]
    fn known_failed_replay_after_compaction_is_quiet_but_new_late_effect_escalates() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context_for("qa_rc_owner_one", 7_202, "a", 1_000_000);
        // The very event a participant would redeliver (same trace identity).
        let known = step_completed(&ctx, "a", true);
        {
            let journal = LmdbTerminalResolverJournal::open(dir.path()).unwrap();
            failed_history(&journal, &ctx, &known);
            journal.compact_terminal_detail().unwrap();
        }
        let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
        let (bus, seen) = durable::<OwnerOneContract, _>(Arc::clone(&journal));
        // Exact replay of an effect the failed run already knew about.
        bus.publish_strict(known).unwrap();
        settle();
        assert_eq!(
            count(&seen, is_quarantined),
            0,
            "known replay of a failed run is not new uncertainty"
        );
        // A genuinely new compensable effect still escalates.
        bus.publish_strict(step_completed(&ctx, "x", true)).unwrap();
        assert!(
            wait_until(Duration::from_secs(2), || count(&seen, is_quarantined) == 1),
            "new effect after Failed escalates"
        );
        // And the quarantine evidence is retained through compaction.
        assert!(wait_until(Duration::from_secs(1), || journal
            .read_all()
            .unwrap()
            .iter()
            .any(|entry| is_quarantined(&entry.event))));
        journal.compact_terminal_detail().unwrap();
        assert!(
            journal
                .read_all()
                .unwrap()
                .iter()
                .any(|entry| is_quarantined(&entry.event))
        );
    }
}
