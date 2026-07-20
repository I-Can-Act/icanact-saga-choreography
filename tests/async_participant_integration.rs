use std::time::Duration;

use icanact_saga_choreography::durability::apply_async_participant_saga_ingress_with_hooks;
use icanact_saga_choreography::{
    AcceptedStepCompletion, AcceptedStepError, AcceptedStepPolicy, AcceptedStepTimeoutOutcome,
    AsyncSagaParticipant, CompensationError, CompensationOutput, DependencySpec,
    HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal, ParticipantEvent,
    ParticipantJournal, PeerId, SagaChoreographyEvent, SagaContext, SagaId, SagaParticipantSupport,
    SagaStateEntry, SagaStateExt, StepError, StepExecutionId, StepOutput,
    complete_accepted_workflow_step,
};

struct AsyncTestParticipant {
    saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
    dependency_spec: DependencySpec,
    execute_output: Result<StepOutput, StepError>,
    compensation_result: Result<CompensationOutput, CompensationError>,
    executed_inputs: Vec<Vec<u8>>,
    compensation_calls: usize,
}

impl Default for AsyncTestParticipant {
    fn default() -> Self {
        Self {
            saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
            dependency_spec: DependencySpec::OnSagaStart,
            execute_output: Ok(StepOutput::Completed {
                output: b"ok".to_vec(),
                compensation_data: vec![1, 2, 3],
            }),
            compensation_result: Ok(CompensationOutput::Completed),
            executed_inputs: Vec::new(),
            compensation_calls: 0,
        }
    }
}

impl HasSagaParticipantSupport for AsyncTestParticipant {
    type Journal = InMemoryJournal;
    type Dedupe = InMemoryDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}

impl AsyncSagaParticipant for AsyncTestParticipant {
    type Error = String;

    fn step_name(&self) -> &str {
        "async_step"
    }

    fn saga_types(&self) -> &[&'static str] {
        &["order_lifecycle"]
    }

    fn depends_on(&self) -> DependencySpec {
        self.dependency_spec.clone()
    }

    fn execute_step<'a>(
        &'a mut self,
        _context: &'a SagaContext,
        input: &'a [u8],
    ) -> icanact_saga_choreography::SagaBoxFuture<'a, Result<StepOutput, StepError>> {
        self.executed_inputs.push(input.to_vec());
        let output = self.execute_output.clone();
        Box::pin(async move { output })
    }

    fn compensate_step<'a>(
        &'a mut self,
        _context: &'a SagaContext,
        _compensation_data: &'a [u8],
    ) -> icanact_saga_choreography::SagaBoxFuture<'a, Result<CompensationOutput, CompensationError>>
    {
        self.compensation_calls += 1;
        let result = self.compensation_result.clone();
        Box::pin(async move { result })
    }
}

fn started_event() -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: test_context(1),
        payload: vec![7, 8, 9],
    }
}

fn test_context(saga_id: u64) -> SagaContext {
    SagaContext {
        saga_id: SagaId::new(saga_id),
        saga_type: "order_lifecycle".into(),
        step_name: "risk_check".into(),
        correlation_id: saga_id,
        causation_id: saga_id,
        trace_id: saga_id,
        step_index: 0,
        attempt: 0,
        initiator_peer_id: PeerId::default(),
        saga_started_at_millis: 1_700_000_000_000,
        event_timestamp_millis: 1_700_000_000_000,
    }
}

#[tokio::test]
async fn async_ingress_executes_on_saga_start_and_emits_step_completed() {
    let mut participant = AsyncTestParticipant::default();
    let mut emitted = Vec::new();

    apply_async_participant_saga_ingress_with_hooks(
        &mut participant,
        started_event(),
        |_actor, _incoming| {},
        |_invalid| {},
        |_actor, event| emitted.push(event.clone()),
    )
    .await;

    assert_eq!(participant.executed_inputs, vec![vec![7, 8, 9]]);
    assert_eq!(emitted.len(), 2);
    assert!(matches!(
        emitted.first(),
        Some(SagaChoreographyEvent::StepStarted { .. })
    ));
    assert!(matches!(
        emitted.get(1),
        Some(SagaChoreographyEvent::StepCompleted { .. })
    ));
    assert!(matches!(
        participant.saga_states_ref().values().next(),
        Some(SagaStateEntry::Completed(_))
    ));
}

#[tokio::test]
async fn async_ingress_waits_for_all_dependencies_before_execution() {
    let mut participant = AsyncTestParticipant {
        dependency_spec: DependencySpec::AllOf(&["a", "b"]),
        ..AsyncTestParticipant::default()
    };
    let ctx = test_context(1);

    apply_async_participant_saga_ingress_with_hooks(
        &mut participant,
        SagaChoreographyEvent::StepCompleted {
            context: ctx.next_step("a".into()),
            output: b"partial".to_vec(),
            saga_input: b"origin".to_vec(),
            compensation_available: false,
        },
        |_actor, _incoming| {},
        |_invalid| {},
        |_actor, _event| {},
    )
    .await;

    assert!(participant.executed_inputs.is_empty());

    let mut emitted = Vec::new();
    apply_async_participant_saga_ingress_with_hooks(
        &mut participant,
        SagaChoreographyEvent::StepCompleted {
            context: ctx.next_step("b".into()),
            output: b"final".to_vec(),
            saga_input: b"origin".to_vec(),
            compensation_available: false,
        },
        |_actor, _incoming| {},
        |_invalid| {},
        |_actor, event| emitted.push(event.clone()),
    )
    .await;

    assert_eq!(participant.executed_inputs, vec![b"origin".to_vec()]);
    assert_eq!(emitted.len(), 2);
    assert!(matches!(
        emitted.first(),
        Some(SagaChoreographyEvent::StepStarted { .. })
    ));
}

#[tokio::test]
async fn async_ingress_non_ambiguous_compensation_failure_keeps_local_failed_state() {
    let mut participant = AsyncTestParticipant {
        compensation_result: Err(CompensationError::Terminal {
            reason: "undo failed".into(),
        }),
        ..AsyncTestParticipant::default()
    };
    let context = test_context(1);

    apply_async_participant_saga_ingress_with_hooks(
        &mut participant,
        started_event(),
        |_actor, _incoming| {},
        |_invalid| {},
        |_actor, _event| {},
    )
    .await;

    let mut emitted = Vec::new();
    apply_async_participant_saga_ingress_with_hooks(
        &mut participant,
        SagaChoreographyEvent::CompensationRequested {
            context,
            failed_step: "upstream".into(),
            reason: "undo failed".into(),
            failure: icanact_saga_choreography::SagaFailureDetails {
                step_name: "upstream".into(),
                participant_id: "upstream-participant".into(),
                error_code: None,
                error_message: "undo failed".into(),
                at_millis: 1,
            },
            steps_to_compensate: vec!["async_step".into()],
        },
        |_actor, _incoming| {},
        |_invalid| {},
        |_actor, event| emitted.push(event.clone()),
    )
    .await;

    assert_eq!(participant.compensation_calls, 1);
    assert_eq!(emitted.len(), 1);
    assert!(matches!(
        emitted.first(),
        Some(SagaChoreographyEvent::CompensationFailed {
            is_ambiguous: false,
            ..
        })
    ));
    assert!(matches!(
        participant.saga_states_ref().values().next(),
        Some(SagaStateEntry::Failed(_))
    ));
    let entries = participant
        .saga
        .journal
        .read(SagaId::new(1))
        .expect("journal read should succeed");
    assert!(matches!(
        entries.last(),
        Some(icanact_saga_choreography::JournalEntry {
            event: ParticipantEvent::CompensationFailed {
                error,
                is_ambiguous: false,
                ..
            },
            ..
        }) if error.as_ref() == "undo failed"
    ));
}

#[tokio::test]
async fn async_ingress_tombstones_an_accepted_step_after_terminal_timeout() {
    let execution_id = StepExecutionId::new("async-timeout-2");
    let mut participant = AsyncTestParticipant {
        execute_output: Ok(StepOutput::Accepted {
            execution_id: execution_id.clone(),
            policy: AcceptedStepPolicy {
                idle_timeout: Duration::from_secs(5),
                hard_timeout: Duration::from_secs(10),
                timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                    requires_compensation: false,
                },
            },
            compensation_data: Vec::new(),
        }),
        ..AsyncTestParticipant::default()
    };

    apply_async_participant_saga_ingress_with_hooks(
        &mut participant,
        SagaChoreographyEvent::SagaStarted {
            context: test_context(2),
            payload: b"order".to_vec(),
        },
        |_actor, _incoming| {},
        |_invalid| {},
        |_actor, _event| {},
    )
    .await;
    assert_eq!(participant.saga.accepted_workflow_steps.len(), 1);

    apply_async_participant_saga_ingress_with_hooks(
        &mut participant,
        SagaChoreographyEvent::StepFailed {
            context: test_context(2).next_step("async_step".into()),
            participant_id: "async-participant".into(),
            error_code: Some("idle".into()),
            error: "accepted step idle timeout execution_id=async-timeout-2".into(),
            requires_compensation: false,
        },
        |_actor, _incoming| {},
        |_invalid| {},
        |_actor, _event| {},
    )
    .await;

    assert!(participant.saga.accepted_workflow_steps.is_empty());
    let late = complete_accepted_workflow_step(
        &mut participant,
        SagaId::new(2),
        execution_id,
        AcceptedStepCompletion {
            completed_at_millis: SagaContext::now_millis(),
            output: b"late".to_vec(),
            compensation_data: Vec::new(),
        },
    );
    assert!(matches!(
        late,
        Err(AcceptedStepError::AlreadyResolved { .. })
    ));
}
