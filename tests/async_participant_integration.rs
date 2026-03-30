use icanact_saga_choreography::durability::apply_async_participant_saga_ingress_with_hooks;
use icanact_saga_choreography::{
    AsyncSagaParticipant, CompensationError, DependencySpec, DeterministicContextBuilder,
    HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal, SagaChoreographyEvent, SagaContext,
    SagaParticipantSupport, SagaStateEntry, SagaStateExt, StepError, StepOutput,
};

struct AsyncTestParticipant {
    saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
    dependency_spec: DependencySpec,
    execute_output: Result<StepOutput, StepError>,
    compensation_result: Result<(), CompensationError>,
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
            compensation_result: Ok(()),
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
    ) -> icanact_saga_choreography::SagaBoxFuture<'a, Result<(), CompensationError>> {
        self.compensation_calls += 1;
        let result = self.compensation_result.clone();
        Box::pin(async move { result })
    }
}

fn started_event() -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted {
        context: DeterministicContextBuilder::default().build(),
        payload: vec![7, 8, 9],
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
    assert_eq!(emitted.len(), 1);
    assert!(matches!(
        emitted.first(),
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
    let ctx = DeterministicContextBuilder::default().build();

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
    assert_eq!(emitted.len(), 1);
}

#[tokio::test]
async fn async_ingress_compensation_failure_quarantines_saga() {
    let mut participant = AsyncTestParticipant {
        compensation_result: Err(CompensationError::Terminal {
            reason: "undo failed".into(),
        }),
        ..AsyncTestParticipant::default()
    };
    let context = DeterministicContextBuilder::default().build();

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
            steps_to_compensate: vec!["async_step".into()],
        },
        |_actor, _incoming| {},
        |_invalid| {},
        |_actor, event| emitted.push(event.clone()),
    )
    .await;

    assert_eq!(participant.compensation_calls, 1);
    assert!(emitted
        .iter()
        .any(|event| matches!(event, SagaChoreographyEvent::SagaQuarantined { .. })));
    assert!(matches!(
        participant.saga_states_ref().values().next(),
        Some(SagaStateEntry::Quarantined(_))
    ));
}
