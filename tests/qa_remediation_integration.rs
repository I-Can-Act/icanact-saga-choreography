//! Integration-edge safety assertions added after inspecting the actual lane diffs.
use icanact_saga_choreography::*;
use std::sync::Arc;
use std::time::Duration;

fn ctx(id: u64, step: &str, start: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(id),
        saga_type: "integration-qa".into(),
        step_name: step.into(),
        correlation_id: id,
        causation_id: id,
        trace_id: id,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: start,
        event_timestamp_millis: start,
    }
}

define_saga_workflow_contract! {
    struct IntegrationContract {
        saga_type: "integration-qa",
        first_step: a,
        failure_authority: any (),
        required_steps: [finish],
        overall_timeout_ms: 60_000,
        stalled_timeout_ms: 60_000,
        steps: {
            a => { participant: "a", depends_on: on_start () },
            finish => { participant: "finish", depends_on: after [a] }
        }
    }
}
fn durable_bus(journal: Arc<InMemoryTerminalResolverJournal>, bind: bool) -> SagaChoreographyBus {
    let bus = SagaChoreographyBus::new();
    bus.register_workflow_contract_provider::<IntegrationContract>()
        .unwrap();
    bus.attach_durable_terminal_resolver_for_contract::<IntegrationContract, _>("qa", journal)
        .unwrap();
    if bind {
        for step in ["a", "finish"] {
            bus.register_bound_workflow_step("integration-qa", step)
                .unwrap();
            bus.subscribe_saga_type_fn("integration-qa", |_| true);
        }
    }
    bus.activate_terminal_resolver_recovery("integration-qa")
        .unwrap();
    bus
}

fn policy() -> TerminalPolicy {
    TerminalPolicy::new(
        "integration-qa".into(),
        "integration-qa".into(),
        FailureAuthority::AnyParticipant,
        SuccessCriteria::AnyOf([Box::from("finish")].into_iter().collect()),
        Duration::from_secs(60),
        Duration::from_secs(60),
        &[],
    )
}

fn effect(c: &SagaContext, step: &str) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepCompleted {
        context: c.next_step(step.into()),
        output: vec![1],
        saga_input: vec![],
        compensation_available: true,
    }
}
fn failure(c: &SagaContext) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepFailed {
        context: c.next_step("failed".into()),
        participant_id: "p".into(),
        error_code: None,
        error: "failed".into(),
        requires_compensation: true,
    }
}

struct Support(SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>);
impl HasSagaParticipantSupport for Support {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;
    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.0
    }
    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.0
    }
}

#[test]
fn completion_after_accepted_effect_was_undone_escalates_terminal_failure_to_quarantine() {
    let mut r = TerminalResolver::new(policy());
    let c = ctx(5001, "a", SagaContext::now_millis());
    let mut actor = Support(SagaParticipantSupport::new(
        InMemoryJournal::new(),
        InMemoryDedupe::new(),
    ));
    let accepted = accept_workflow_step(
        &mut actor,
        c.clone(),
        "p".into(),
        StepExecutionId::new("external-a"),
        AcceptedStepPolicy {
            idle_timeout: Duration::from_secs(60),
            hard_timeout: Duration::from_secs(60),
            timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
        },
        vec![],
        vec![1],
    )
    .unwrap();
    r.ingest(&accepted);
    assert!(matches!(
        r.ingest(&failure(&c)).as_slice(),
        [SagaChoreographyEvent::CompensationRequested { .. }]
    ));
    assert!(matches!(
        r.ingest(&SagaChoreographyEvent::CompensationCompleted { context: c.clone() })
            .as_slice(),
        [SagaChoreographyEvent::SagaFailed { .. }]
    ));
    let out = r.ingest(&effect(&c, "a"));
    assert!(
        matches!(
            out.as_slice(),
            [SagaChoreographyEvent::SagaQuarantined { .. }]
        ),
        "effect materialised after undo: terminal absorption must not hide it: {out:?}"
    );
}

#[test]
fn failed_final_undo_keeps_the_unreversed_effect_quarantined() {
    let mut r = TerminalResolver::new(policy());
    let c = ctx(5002, "a", SagaContext::now_millis());
    r.ingest(&effect(&c, "a"));
    r.ingest(&failure(&c));
    let out = r.ingest(&SagaChoreographyEvent::CompensationFailed {
        context: c,
        participant_id: "p".into(),
        error: "undo refused".into(),
        is_ambiguous: false,
    });
    assert!(
        matches!(
            out.as_slice(),
            [SagaChoreographyEvent::SagaQuarantined { .. }]
        ),
        "definitive undo failure is not successful reversal: {out:?}"
    );
}

#[test]
fn cached_quarantine_cannot_be_replaced_by_a_newer_run() {
    let mut r = TerminalResolver::new(policy());
    let now = SagaContext::now_millis();
    let c = ctx(5003, "a", now);
    r.ingest(&SagaChoreographyEvent::SagaQuarantined {
        context: c,
        reason: "unresolved".into(),
        step: "a".into(),
        participant_id: "p".into(),
    });
    let out = r.ingest(&effect(&ctx(5003, "finish", now + 1), "finish"));
    assert!(
        out.is_empty(),
        "quarantined ownership cannot be reset by a timestamp: {out:?}"
    );
}

#[test]
fn late_quarantine_of_an_old_run_fences_an_already_active_successor() {
    let mut r = TerminalResolver::new(policy());
    let now = SagaContext::now_millis();
    let old = ctx(5011, "finish", now);
    assert!(matches!(
        r.ingest(&effect(&old, "finish")).as_slice(),
        [SagaChoreographyEvent::SagaCompleted { .. }]
    ));
    let new = ctx(5011, "a", now + 1);
    r.ingest(&SagaChoreographyEvent::SagaStarted {
        context: new.clone(),
        payload: vec![],
    });
    let out = r.ingest(&SagaChoreographyEvent::SagaQuarantined {
        context: old,
        reason: "late external uncertainty".into(),
        step: "a".into(),
        participant_id: "a".into(),
    });
    assert!(
        matches!(out.as_slice(), [SagaChoreographyEvent::SagaQuarantined { context, .. }]
        if context.saga_started_at_millis == new.saga_started_at_millis),
        "uncertain older ownership must fence the already-active successor: {out:?}"
    );
    assert!(
        r.ingest(&effect(&new, "finish")).is_empty(),
        "success must not escape the family quarantine"
    );
}

#[test]
fn late_quarantine_durably_overrides_an_ordinary_terminal_tombstone() {
    let mut actor = Support(SagaParticipantSupport::new(
        InMemoryJournal::new(),
        InMemoryDedupe::new(),
    ));
    let c = ctx(5010, "a", SagaContext::now_millis());
    actor.admit_participant_event_strict(&c).unwrap();
    actor
        .retain_terminal_saga_strict(&c, ParticipantTerminalKind::Failed, "rolled back")
        .unwrap();
    actor
        .retain_terminal_saga_strict(&c, ParticipantTerminalKind::Quarantined, "late effect")
        .unwrap();
    assert_eq!(
        actor.terminal_run_outcome_strict(&c).unwrap(),
        Some(ParticipantTerminalKind::Quarantined)
    );
    let later = ctx(5010, "a", c.saga_started_at_millis + 1);
    assert!(matches!(
        actor.admit_participant_event_strict(&later).unwrap(),
        ParticipantAdmission::QuarantinedReuse { .. }
    ));
}

#[test]
fn durable_bus_rejects_a_new_run_over_an_unresolved_run() {
    let journal = Arc::new(InMemoryTerminalResolverJournal::default());
    let now = SagaContext::now_millis();
    let first = ctx(5004, "a", now);
    journal
        .append(SagaChoreographyEvent::SagaStarted {
            context: first,
            payload: vec![],
        })
        .unwrap();
    let bus = durable_bus(journal, true);
    let start = SagaChoreographyEvent::SagaStarted {
        context: ctx(5004, "a", now + 1),
        payload: vec![],
    };
    let result = bus.publish_strict(start);
    assert!(
        matches!(&result, Err(SagaBusPublishError::AdmissionRejected { reason, .. })
        if reason.contains("unresolved")),
        "active durable history must fence newer identity before participant fanout: {result:?}"
    );
}

#[test]
fn stale_start_contract_error_does_not_overwrite_durable_quarantine() {
    let journal = Arc::new(InMemoryTerminalResolverJournal::default());
    let c = ctx(5005, "a", SagaContext::now_millis());
    journal
        .append(SagaChoreographyEvent::SagaQuarantined {
            context: c.clone(),
            reason: "unresolved".into(),
            step: "a".into(),
            participant_id: "p".into(),
        })
        .unwrap();
    let bus = durable_bus(journal, false);
    // Missing bindings would normally be rejected by a contract. A retained
    // quarantine must take precedence over any fabricated ordinary failure.
    let start = SagaChoreographyEvent::SagaStarted {
        context: c,
        payload: vec![],
    };
    assert!(bus.publish_strict(start).is_err());
    assert!(!matches!(
        bus.take_terminal_outcome(SagaId::new(5005)),
        Some(SagaTerminalOutcome::Failed { .. })
    ));
}
