//! R30/T30A: the canonical fixture must surface publish failures that the
//! legacy configurable participant (`let _ = bus.publish(..)`) hides.

#[allow(unused_imports)]
mod support;

use icanact_saga_choreography::{
    DependencySpec, SagaChoreographyEvent, handle_saga_event_with_emit,
};
use support::canonical::*;

fn started(saga_id: u64) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: canonical_context(saga_id),
        payload: vec![],
    }
}

fn participant(bus: &icanact_saga_choreography::SagaChoreographyBus) -> CanonicalParticipant {
    let mut p = CanonicalParticipant::new(STEP_A, DependencySpec::OnSagaStart);
    p.attach_bus(bus.clone());
    p
}

#[test]
fn canonical_fixture_surfaces_publish_failure() {
    // A recipient refuses every event: injected publish failure.
    let (bus, _sub) = canonical_bus_with_rejecting_recipient();
    let mut p = participant(&bus);
    let report = p.ingest(started(1));
    assert_eq!(p.executed, 1, "step must have run through ingress");
    assert!(
        !report.publish_errors.is_empty(),
        "injected publish failure must be reported, got {report:?}"
    );
    assert!(report.into_result().is_err());
}

#[test]
fn canonical_fixture_surfaces_commit_failure_outcome() {
    use icanact_saga_choreography::{CommitStage, IngressOutcome};
    use support::{FaultTrigger, JournalOp};
    let bus = canonical_bus(&[]);
    let mut p = participant(&bus);
    p.journal().fail_once(
        JournalOp::Append,
        FaultTrigger::EventKind("StepExecutionStarted"),
    );
    let report = p.ingest(started(4));
    assert_eq!(
        p.executed, 0,
        "callback must not run without a durable intent"
    );
    assert!(
        matches!(&report.outcome, IngressOutcome::Failed(f) if f.stage == CommitStage::Intent),
        "intent commit failure must surface as a typed outcome, got {report:?}"
    );
    assert!(!report.is_clean());
}

#[test]
fn legacy_low_level_path_hides_the_same_failure() {
    let (bus, _sub) = canonical_bus_with_rejecting_recipient();
    let mut p = participant(&bus);
    let mut emitted = 0;
    handle_saga_event_with_emit(&mut p, started(2), |e| {
        emitted += 1;
        let _ = bus.publish(e); // legacy: result discarded, nothing observable
    });
    assert!(emitted > 0, "legacy path emitted but surfaced no error");
}

#[test]
fn canonical_fixture_clean_control_reports_no_fault() {
    let bus = canonical_bus(&[]);
    let mut p = participant(&bus);
    let report = p.ingest(started(3));
    assert!(report.is_clean(), "control must be clean: {report:?}");
    assert!(report.published > 0);
}
