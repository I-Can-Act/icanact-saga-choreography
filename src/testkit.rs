//! Deterministic helpers for participant choreography tests.

use crate::{
    handle_saga_event, SagaChoreographyEvent, SagaContext, SagaId, SagaParticipant, SagaStateExt,
};

/// Small deterministic builder for saga test contexts.
#[derive(Debug, Clone)]
pub struct DeterministicContextBuilder {
    saga_id: u64,
    saga_type: String,
    step_name: String,
    correlation_id: u64,
    causation_id: u64,
    trace_id: u64,
    started_at_millis: u64,
    event_at_millis: u64,
}

impl Default for DeterministicContextBuilder {
    fn default() -> Self {
        Self {
            saga_id: 1,
            saga_type: "order_lifecycle".to_string(),
            step_name: "risk_check".to_string(),
            correlation_id: 1,
            causation_id: 1,
            trace_id: 1,
            started_at_millis: 1_700_000_000_000,
            event_at_millis: 1_700_000_000_000,
        }
    }
}

impl DeterministicContextBuilder {
    pub fn with_saga_id(mut self, saga_id: u64) -> Self {
        self.saga_id = saga_id;
        self
    }

    pub fn with_saga_type(mut self, saga_type: impl Into<String>) -> Self {
        self.saga_type = saga_type.into();
        self
    }

    pub fn with_step_name(mut self, step_name: impl Into<String>) -> Self {
        self.step_name = step_name.into();
        self
    }

    pub fn with_trace_id(mut self, trace_id: u64) -> Self {
        self.trace_id = trace_id;
        self
    }

    pub fn build(self) -> SagaContext {
        SagaContext {
            saga_id: SagaId::new(self.saga_id),
            saga_type: self.saga_type.into_boxed_str(),
            step_name: self.step_name.into_boxed_str(),
            correlation_id: self.correlation_id,
            causation_id: self.causation_id,
            trace_id: self.trace_id,
            step_index: 0,
            attempt: 0,
            initiator_peer_id: [0; 32],
            saga_started_at_millis: self.started_at_millis,
            event_timestamp_millis: self.event_at_millis,
        }
    }
}

pub fn saga_started(context: SagaContext, payload: Vec<u8>) -> SagaChoreographyEvent {
    SagaChoreographyEvent::SagaStarted { context, payload }
}

pub fn step_completed(
    context: SagaContext,
    output: Vec<u8>,
    saga_input: Vec<u8>,
    compensation_available: bool,
) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepCompleted {
        context,
        output,
        saga_input,
        compensation_available,
    }
}

pub fn step_failed(
    context: SagaContext,
    error: impl Into<String>,
    will_retry: bool,
    requires_compensation: bool,
) -> SagaChoreographyEvent {
    SagaChoreographyEvent::StepFailed {
        context,
        participant_id: "testkit".into(),
        error_code: None,
        error: error.into().into_boxed_str(),
        will_retry,
        requires_compensation,
    }
}

pub fn compensation_requested(
    context: SagaContext,
    failed_step: impl Into<String>,
    reason: impl Into<String>,
    steps_to_compensate: Vec<String>,
) -> SagaChoreographyEvent {
    SagaChoreographyEvent::CompensationRequested {
        context,
        failed_step: failed_step.into().into_boxed_str(),
        reason: reason.into().into_boxed_str(),
        steps_to_compensate: steps_to_compensate
            .into_iter()
            .map(|step| step.into_boxed_str())
            .collect(),
    }
}

/// Replay a deterministic event sequence against one participant.
pub fn drive_scenario<P>(
    participant: &mut P,
    events: impl IntoIterator<Item = SagaChoreographyEvent>,
) where
    P: SagaParticipant + SagaStateExt,
{
    for event in events {
        handle_saga_event(participant, event);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        CompensationError, DependencySpec, HasSagaParticipantSupport, InMemoryDedupe,
        InMemoryJournal, SagaParticipantSupport, StepError, StepOutput,
    };

    use super::*;

    struct TestParticipant {
        saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
        called: bool,
    }

    impl Default for TestParticipant {
        fn default() -> Self {
            Self {
                saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
                called: false,
            }
        }
    }

    impl HasSagaParticipantSupport for TestParticipant {
        type Journal = InMemoryJournal;
        type Dedupe = InMemoryDedupe;

        fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
            &self.saga
        }

        fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
            &mut self.saga
        }
    }

    impl SagaParticipant for TestParticipant {
        type Error = String;

        fn step_name(&self) -> &str {
            "risk_check"
        }

        fn saga_types(&self) -> &[&'static str] {
            &["order_lifecycle"]
        }

        fn depends_on(&self) -> DependencySpec {
            DependencySpec::OnSagaStart
        }

        fn execute_step(
            &mut self,
            _context: &SagaContext,
            _input: &[u8],
        ) -> Result<StepOutput, StepError> {
            self.called = true;
            Ok(StepOutput::Completed {
                output: vec![],
                compensation_data: vec![],
            })
        }

        fn compensate_step(
            &mut self,
            _context: &SagaContext,
            _compensation_data: &[u8],
        ) -> Result<(), CompensationError> {
            Ok(())
        }
    }

    #[test]
    fn drive_scenario_runs_on_saga_start() {
        let mut participant = TestParticipant::default();
        let ctx = DeterministicContextBuilder::default().build();
        drive_scenario(&mut participant, [saga_started(ctx, vec![1, 2, 3])]);
        assert!(participant.called);
    }
}
