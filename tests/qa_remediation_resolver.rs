//! Safety regressions for rollback completeness, ordering, timeouts and run isolation
//! in the terminal resolver (public API only).

use std::collections::HashSet;
use std::time::Duration;

use icanact_saga_choreography::*;

fn policy(criteria: SuccessCriteria) -> TerminalPolicy {
    TerminalPolicy::new(
        "qa".into(),
        "qa-policy".into(),
        FailureAuthority::AnyParticipant,
        criteria,
        Duration::from_secs(60),
        Duration::from_secs(60),
        &[],
    )
}

fn all_of(steps: &[&str]) -> SuccessCriteria {
    SuccessCriteria::AllOf(
        steps
            .iter()
            .map(|s| Box::<str>::from(*s))
            .collect::<HashSet<_>>(),
    )
}

fn any_of(steps: &[&str]) -> SuccessCriteria {
    SuccessCriteria::AnyOf(
        steps
            .iter()
            .map(|s| Box::<str>::from(*s))
            .collect::<HashSet<_>>(),
    )
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
        event_timestamp_millis: SagaContext::now_millis(),
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

fn completed(ctx: &SagaContext, step: &str, compensable: bool) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepCompleted {
        context: ctx.next_step(step.into()),
        output: vec![1],
        saga_input: vec![9],
        compensation_available: compensable,
    }
}

fn failed(ctx: &SagaContext, step: &str, requires_compensation: bool) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepFailed {
        context: ctx.next_step(step.into()),
        participant_id: step.into(),
        error_code: Some("E".into()),
        error: format!("{step} failed").into(),
        requires_compensation,
    }
}

fn comp_done(ctx: &SagaContext, step: &str) -> SagaChoreographyEvent {
    SagaChoreographyEvent::CompensationCompleted {
        context: ctx.next_step(step.into()),
    }
}

fn requested_steps(out: &[SagaChoreographyEvent]) -> Vec<Vec<String>> {
    out.iter()
        .filter_map(|e| match e {
            SagaChoreographyEvent::CompensationRequested {
                steps_to_compensate,
                ..
            } => Some(steps_to_compensate.iter().map(|s| s.to_string()).collect()),
            _ => None,
        })
        .collect()
}

fn has_terminal(out: &[SagaChoreographyEvent], kind: &str) -> bool {
    out.iter().any(|e| e.event_type() == kind)
}

fn step_names(names: &[&str]) -> Vec<Vec<String>> {
    names.iter().map(|n| vec![n.to_string()]).collect()
}

#[test]
fn anyof_success_cannot_complete_while_rollback_is_pending() {
    let mut resolver = TerminalResolver::new(policy(any_of(&["b", "c"])));
    let ctx = context(105, "a");
    resolver.ingest(&started(&ctx));
    assert!(resolver.ingest(&completed(&ctx, "a", true)).is_empty());
    let rollback = resolver.ingest(&failed(&ctx, "b", true));
    assert_eq!(requested_steps(&rollback), step_names(&["a"]));

    let late = resolver.ingest(&completed(&ctx, "c", false));
    assert!(
        !has_terminal(&late, "saga_completed"),
        "rollback is absorbing for forward success: {late:?}"
    );
    let done = resolver.ingest(&comp_done(&ctx, "a"));
    assert!(matches!(
        done.as_slice(),
        [SagaChoreographyEvent::SagaFailed { failure: Some(f), .. }] if f.step_name.as_ref() == "b"
    ));
}

#[test]
fn late_allof_effect_is_undone_before_failure_is_terminal() {
    let mut resolver = TerminalResolver::new(policy(all_of(&["b", "c"])));
    let ctx = context(122, "a");
    resolver.ingest(&started(&ctx));
    resolver.ingest(&completed(&ctx, "a", true));
    let request = resolver.ingest(&failed(&ctx, "b", true));
    assert_eq!(requested_steps(&request), step_names(&["a"]));

    assert!(resolver.ingest(&completed(&ctx, "c", true)).is_empty());
    let after_a = resolver.ingest(&comp_done(&ctx, "a"));
    assert_eq!(requested_steps(&after_a), step_names(&["c"]));
    assert!(!has_terminal(&after_a, "saga_failed"));
    let after_c = resolver.ingest(&comp_done(&ctx, "c"));
    assert!(has_terminal(&after_c, "saga_failed"));
    assert!(requested_steps(&after_c).is_empty());
}

#[test]
fn undo_is_singleton_reverse_order_and_ignores_duplicates() {
    let mut resolver = TerminalResolver::new(policy(all_of(&["z"])));
    let ctx = context(106, "a");
    resolver.ingest(&started(&ctx));
    for step in ["a", "b", "c"] {
        assert!(resolver.ingest(&completed(&ctx, step, true)).is_empty());
    }
    let first = resolver.ingest(&failed(&ctx, "d", true));
    assert_eq!(requested_steps(&first), step_names(&["c"]));

    // An unrelated second failure must not alter the failure of record or skip ahead.
    assert!(resolver.ingest(&failed(&ctx, "e", true)).is_empty());

    let second = resolver.ingest(&comp_done(&ctx, "c"));
    assert_eq!(requested_steps(&second), step_names(&["b"]));
    // Duplicate completion must not advance the queue.
    assert!(resolver.ingest(&comp_done(&ctx, "c")).is_empty());

    let third = resolver.ingest(&comp_done(&ctx, "b"));
    assert_eq!(requested_steps(&third), step_names(&["a"]));
    let last = resolver.ingest(&comp_done(&ctx, "a"));
    assert!(matches!(
        last.as_slice(),
        [SagaChoreographyEvent::SagaFailed { failure: Some(f), .. }] if f.step_name.as_ref() == "d"
    ));
}

#[test]
fn non_compensating_failure_during_rollback_does_not_bypass_obligations() {
    let mut resolver = TerminalResolver::new(policy(all_of(&["z"])));
    let ctx = context(107, "a");
    resolver.ingest(&started(&ctx));
    resolver.ingest(&completed(&ctx, "a", true));
    resolver.ingest(&failed(&ctx, "b", true));
    let out = resolver.ingest(&failed(&ctx, "c", false));
    assert!(!has_terminal(&out, "saga_failed"), "{out:?}");
    let done = resolver.ingest(&comp_done(&ctx, "a"));
    assert!(matches!(
        done.as_slice(),
        [SagaChoreographyEvent::SagaFailed { failure: Some(f), .. }] if f.step_name.as_ref() == "b"
    ));
}

#[test]
fn forward_overall_and_stalled_timeouts_compensate_or_quarantine() {
    for overall in [true, false] {
        for rollback_pending in [false, true] {
            let mut p = policy(all_of(&["b"]));
            if overall {
                p.overall_timeout = Duration::from_millis(100);
            } else {
                p.stalled_timeout = Duration::from_millis(100);
            }
            let mut resolver = TerminalResolver::new(p);
            let ctx = context(120, "a");
            resolver.ingest(&started(&ctx));
            assert!(resolver.ingest(&completed(&ctx, "a", true)).is_empty());
            if rollback_pending {
                let out = resolver.ingest(&failed(&ctx, "b", true));
                assert_eq!(requested_steps(&out), step_names(&["a"]));
            }
            std::thread::sleep(Duration::from_millis(150));
            let out = resolver.poll_timeouts();
            if rollback_pending {
                assert!(
                    matches!(out.as_slice(), [SagaChoreographyEvent::SagaQuarantined { step, .. }] if step.as_ref() == "a"),
                    "overall={overall}: {out:?}"
                );
            } else {
                assert_eq!(
                    requested_steps(&out),
                    step_names(&["a"]),
                    "overall={overall}: {out:?}"
                );
                assert!(!has_terminal(&out, "saga_failed"));
                // Rollback completion resolves the timeout as a failure.
                let done = resolver.ingest(&comp_done(&ctx, "a"));
                assert!(matches!(
                    done.as_slice(),
                    [SagaChoreographyEvent::SagaFailed { reason, .. }]
                        if reason.contains(if overall { "overall_timeout" } else { "stalled_timeout" })
                ));
            }
        }
    }
}

#[test]
fn forward_timeout_without_effects_still_fails_plainly() {
    let mut p = policy(all_of(&["b"]));
    p.stalled_timeout = Duration::from_millis(50);
    let mut resolver = TerminalResolver::new(p);
    let ctx = context(121, "a");
    resolver.ingest(&started(&ctx));
    std::thread::sleep(Duration::from_millis(100));
    let out = resolver.poll_timeouts();
    assert!(matches!(
        out.as_slice(),
        [SagaChoreographyEvent::SagaFailed { failure: None, .. }]
    ));
}

#[test]
fn later_run_is_isolated_and_stale_run_events_are_ignored() {
    let mut resolver = TerminalResolver::new(policy(all_of(&["a"])));
    let run1 = context_at(200, "a", 1_000);
    resolver.ingest(&started(&run1));
    assert!(has_terminal(
        &resolver.ingest(&completed(&run1, "a", false)),
        "saga_completed"
    ));

    let run2 = context_at(200, "a", 2_000);
    resolver.ingest(&started(&run2));
    assert!(has_terminal(
        &resolver.ingest(&completed(&run2, "a", false)),
        "saga_completed"
    ));

    // Run-1 events after run 2 are stale and must not produce output or state.
    assert!(resolver.ingest(&completed(&run1, "a", false)).is_empty());
    assert!(resolver.ingest(&failed(&run1, "x", true)).is_empty());
}

#[test]
fn stale_run_events_do_not_disturb_an_unresolved_newer_run() {
    let mut resolver = TerminalResolver::new(policy(all_of(&["b"])));
    let run2 = context_at(201, "a", 2_000);
    resolver.ingest(&started(&run2));
    resolver.ingest(&completed(&run2, "a", true));
    let stale = context_at(201, "a", 1_000);
    assert!(resolver.ingest(&failed(&stale, "x", true)).is_empty());
    assert!(has_terminal(
        &resolver.ingest(&completed(&run2, "b", false)),
        "saga_completed"
    ));
}
