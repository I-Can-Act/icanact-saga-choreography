//! Review remediation (P1-1): forward work that is started, accepted or still
//! outstanding is an uncertain obligation. The resolver never resolves such a
//! saga as an ordinary failure and escalates any late compensable effect.
//! Public API only.

use std::collections::HashSet;
use std::time::Duration;

use icanact_saga_choreography::*;

fn policy(stalled_ms: u64) -> TerminalPolicy {
    let mut required = HashSet::new();
    required.insert(Box::<str>::from("z"));
    TerminalPolicy::new(
        "qa".into(),
        "qa-policy".into(),
        FailureAuthority::AnyParticipant,
        SuccessCriteria::AllOf(required),
        Duration::from_secs(60),
        Duration::from_millis(stalled_ms),
        &[],
    )
}

fn context(id: u64, step: &str, started_at: u64) -> SagaContext {
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

fn started(ctx: &SagaContext) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: ctx.clone(),
        payload: vec![9],
    }
}

fn step_started(ctx: &SagaContext, step: &str) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepStarted {
        context: ctx.next_step(step.into()),
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

fn requested(out: &[SagaChoreographyEvent]) -> Vec<String> {
    out.iter()
        .filter_map(|e| match e {
            SagaChoreographyEvent::CompensationRequested {
                steps_to_compensate,
                ..
            } => Some(
                steps_to_compensate
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .flatten()
        .collect()
}

fn has(out: &[SagaChoreographyEvent], kind: &str) -> bool {
    out.iter().any(|e| e.event_type() == kind)
}

fn quarantined_step(out: &[SagaChoreographyEvent]) -> Option<String> {
    out.iter().find_map(|e| match e {
        SagaChoreographyEvent::SagaQuarantined { step, .. } => Some(step.to_string()),
        _ => None,
    })
}

fn expire() {
    std::thread::sleep(Duration::from_millis(120));
}

#[test]
fn long_running_sync_step_is_quarantined_not_failed_at_stalled_expiry() {
    let mut resolver = TerminalResolver::new(policy(50));
    let ctx = context(1, "a", SagaContext::now_millis());
    resolver.ingest(&started(&ctx));
    resolver.ingest(&step_started(&ctx, "charge"));
    expire();
    let out = resolver.poll_timeouts();
    assert!(!has(&out, "saga_failed"), "{out:?}");
    assert_eq!(quarantined_step(&out).as_deref(), Some("charge"), "{out:?}");
    // The effect that materialises afterwards cannot be resolved ordinarily.
    let late = resolver.ingest(&completed(&ctx, "charge", true));
    assert!(
        !has(&late, "saga_failed") && !has(&late, "saga_completed"),
        "{late:?}"
    );
}

#[test]
fn idle_saga_with_no_started_work_still_fails_plainly() {
    let mut resolver = TerminalResolver::new(policy(50));
    let ctx = context(3, "a", SagaContext::now_millis());
    resolver.ingest(&started(&ctx));
    expire();
    let out = resolver.poll_timeouts();
    assert!(
        has(&out, "saga_failed") && !has(&out, "saga_quarantined"),
        "{out:?}"
    );
}

#[test]
fn known_effects_roll_back_in_order_then_unknown_work_quarantines() {
    let mut resolver = TerminalResolver::new(policy(80));
    let ctx = context(4, "a", SagaContext::now_millis());
    resolver.ingest(&started(&ctx));
    resolver.ingest(&completed(&ctx, "a", true));
    resolver.ingest(&completed(&ctx, "b", true));
    resolver.ingest(&step_started(&ctx, "c"));
    std::thread::sleep(Duration::from_millis(150));
    let out = resolver.poll_timeouts();
    assert_eq!(requested(&out), ["b"], "{out:?}");
    // The undo phase has renewed budgets: it is not quarantined immediately.
    assert!(resolver.poll_timeouts().is_empty());
    let out = resolver.ingest(&comp_done(&ctx, "b"));
    assert_eq!(requested(&out), ["a"], "{out:?}");
    let out = resolver.ingest(&comp_done(&ctx, "a"));
    assert!(!has(&out, "saga_failed"), "{out:?}");
    assert_eq!(quarantined_step(&out).as_deref(), Some("c"), "{out:?}");
}

#[test]
fn unknown_work_that_resolves_during_rollback_allows_ordinary_failure() {
    let mut resolver = TerminalResolver::new(policy(80));
    let ctx = context(5, "a", SagaContext::now_millis());
    resolver.ingest(&started(&ctx));
    resolver.ingest(&completed(&ctx, "a", true));
    resolver.ingest(&step_started(&ctx, "c"));
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(requested(&resolver.poll_timeouts()), ["a"]);
    resolver.ingest(&completed(&ctx, "c", false));
    let out = resolver.ingest(&comp_done(&ctx, "a"));
    assert!(
        has(&out, "saga_failed") && !has(&out, "saga_quarantined"),
        "{out:?}"
    );
}

#[test]
fn late_compensable_effect_of_unknown_work_joins_the_rollback() {
    let mut resolver = TerminalResolver::new(policy(80));
    let ctx = context(6, "a", SagaContext::now_millis());
    resolver.ingest(&started(&ctx));
    resolver.ingest(&completed(&ctx, "a", true));
    resolver.ingest(&step_started(&ctx, "c"));
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(requested(&resolver.poll_timeouts()), ["a"]);
    resolver.ingest(&completed(&ctx, "c", true));
    let out = resolver.ingest(&comp_done(&ctx, "a"));
    assert_eq!(requested(&out), ["c"], "{out:?}");
    let out = resolver.ingest(&comp_done(&ctx, "c"));
    assert!(
        has(&out, "saga_failed") && !has(&out, "saga_quarantined"),
        "{out:?}"
    );
}

#[test]
fn non_compensating_failure_does_not_hide_a_known_effect() {
    let mut resolver = TerminalResolver::new(policy(60_000));
    let ctx = context(7, "a", SagaContext::now_millis());
    resolver.ingest(&started(&ctx));
    resolver.ingest(&completed(&ctx, "a", true));
    let out = resolver.ingest(&failed(&ctx, "b", false));
    assert!(!has(&out, "saga_failed"), "{out:?}");
    assert_eq!(requested(&out), ["a"], "{out:?}");
    let out = resolver.ingest(&comp_done(&ctx, "a"));
    assert!(
        matches!(
            out.as_slice(),
            [SagaChoreographyEvent::SagaFailed { failure: Some(f), .. }] if f.step_name.as_ref() == "b"
        ),
        "{out:?}"
    );
}

#[test]
fn non_compensating_failure_does_not_hide_a_running_sibling() {
    let mut resolver = TerminalResolver::new(policy(60_000));
    let ctx = context(8, "a", SagaContext::now_millis());
    resolver.ingest(&started(&ctx));
    resolver.ingest(&step_started(&ctx, "sibling"));
    resolver.ingest(&step_started(&ctx, "b"));
    let out = resolver.ingest(&failed(&ctx, "b", false));
    assert!(!has(&out, "saga_failed"), "{out:?}");
    assert_eq!(
        quarantined_step(&out).as_deref(),
        Some("sibling"),
        "{out:?}"
    );
}

#[test]
fn non_compensating_failure_with_only_its_own_step_fails_plainly() {
    for requires_compensation in [false, true] {
        let mut resolver = TerminalResolver::new(policy(60_000));
        let ctx = context(9, "a", SagaContext::now_millis());
        resolver.ingest(&started(&ctx));
        resolver.ingest(&step_started(&ctx, "b"));
        let out = resolver.ingest(&failed(&ctx, "b", requires_compensation));
        assert!(
            has(&out, "saga_failed") && !has(&out, "saga_quarantined"),
            "{out:?}"
        );
    }
}

#[test]
fn late_compensable_effect_after_ordinary_failure_without_rollback_escalates() {
    let mut resolver = TerminalResolver::new(policy(60_000));
    let ctx = context(10, "a", SagaContext::now_millis());
    resolver.ingest(&started(&ctx));
    let out = resolver.ingest(&failed(&ctx, "b", false));
    assert!(has(&out, "saga_failed"), "{out:?}");
    // Non-compensable late completion is harmless.
    assert!(resolver.ingest(&completed(&ctx, "d", false)).is_empty());
    let late = resolver.ingest(&completed(&ctx, "c", true));
    assert_eq!(quarantined_step(&late).as_deref(), Some("c"), "{late:?}");
    // Escalation happens once.
    assert!(resolver.ingest(&completed(&ctx, "c", true)).is_empty());
}

#[test]
fn compacted_terminal_only_history_escalates_like_full_history() {
    let started_at = SagaContext::now_millis();
    let ctx = context(11, "a", started_at);
    let terminal = SagaChoreographyEvent::SagaFailed {
        context: ctx.next_step("saga_terminal_resolver".into()),
        reason: "boom".into(),
        failure: None,
    };
    // Compaction keeps only the start and the first terminal fence.
    let mut compacted = TerminalResolver::new(policy(60_000));
    compacted.ingest(&started(&ctx));
    compacted.ingest(&terminal);
    let late = compacted.ingest(&completed(&ctx, "c", true));
    assert_eq!(quarantined_step(&late).as_deref(), Some("c"), "{late:?}");

    let mut full = TerminalResolver::new(policy(60_000));
    full.ingest(&started(&ctx));
    full.ingest(&failed(&ctx, "b", false));
    let late = full.ingest(&completed(&ctx, "c", true));
    assert_eq!(quarantined_step(&late).as_deref(), Some("c"), "{late:?}");
}
