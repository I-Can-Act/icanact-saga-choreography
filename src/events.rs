//! Saga events

use super::{SagaContext, SagaId};
use icanact_core::ActorId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SagaFailureDetails {
    pub step_name: Box<str>,
    pub participant_id: Box<str>,
    pub error_code: Option<Box<str>>,
    pub error_message: Box<str>,
    pub will_retry: bool,
    pub at_millis: u64,
}

/// Events published via the local saga event bus.
#[derive(Clone, Debug)]
pub enum SagaChoreographyEvent {
    /// Emitted when a new SAGA orchestration begins.
    SagaStarted {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
        /// The initial payload for the saga execution.
        payload: Vec<u8>,
    },
    /// Emitted when the entire saga completes successfully.
    SagaCompleted {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
    },
    /// Emitted when the saga fails and cannot proceed.
    SagaFailed {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
        /// The reason for the saga failure.
        reason: Box<str>,
        /// Optional structured failure metadata preserved from the failing step.
        failure: Option<SagaFailureDetails>,
    },

    /// Emitted when a step within the saga begins execution.
    StepStarted {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
    },
    /// Emitted when a step completes successfully.
    StepCompleted {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
        /// The output produced by the completed step.
        output: Vec<u8>,
        /// The original input payload executed by the completed step.
        saga_input: Vec<u8>,
        /// Whether compensation logic is available for this step if rollback is needed.
        compensation_available: bool,
    },
    /// Emitted when a step fails during execution.
    StepFailed {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
        /// The participant that emitted the failure.
        participant_id: Box<str>,
        /// Optional machine-readable failure code.
        error_code: Option<Box<str>>,
        /// The error message describing why the step failed.
        error: Box<str>,
        /// Whether the step will be retried.
        will_retry: bool,
        /// Whether compensation is required due to this failure.
        requires_compensation: bool,
    },

    /// Emitted when compensation is requested for one or more steps.
    CompensationRequested {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
        /// The name of the step that triggered the compensation request.
        failed_step: Box<str>,
        /// The reason compensation was requested.
        reason: Box<str>,
        /// The list of step names that need to be compensated, in reverse execution order.
        steps_to_compensate: Vec<Box<str>>,
    },
    /// Emitted when compensation begins execution.
    CompensationStarted {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
    },
    /// Emitted when compensation completes successfully.
    CompensationCompleted {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
    },
    /// Emitted when compensation fails.
    CompensationFailed {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
        /// The participant that failed compensation.
        participant_id: Box<str>,
        /// The error message describing why compensation failed.
        error: Box<str>,
        /// Whether the system state is ambiguous (partial compensation may have occurred).
        is_ambiguous: bool,
    },
    /// Emitted when a saga is quarantined due to unrecoverable errors.
    SagaQuarantined {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
        /// The reason the saga was quarantined.
        reason: Box<str>,
        /// The step during which the quarantine occurred.
        step: Box<str>,
        /// The participant that caused quarantine.
        participant_id: Box<str>,
    },

    /// Emitted as an acknowledgment from a participant for a step.
    StepAck {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
        /// The identifier of the participant sending the acknowledgment.
        participant_id: super::PeerId,
        /// The status of the acknowledgment.
        status: AckStatus,
    },
}

#[derive(Clone, Debug)]
pub enum SagaTerminalOutcome {
    Completed {
        context: SagaContext,
    },
    Failed {
        context: SagaContext,
        reason: Box<str>,
        failure: Option<SagaFailureDetails>,
    },
    Quarantined {
        context: SagaContext,
        reason: Box<str>,
        step: Box<str>,
        participant_id: Box<str>,
    },
}

#[derive(Clone, Debug)]
pub struct SagaReplyTo {
    pub responder: Box<str>,
    pub outcome: SagaTerminalOutcome,
}

impl SagaChoreographyEvent {
    pub fn saga_failed_default(context: SagaContext, reason: Box<str>) -> Self {
        Self::SagaFailed {
            context,
            reason,
            failure: None,
        }
    }

    pub fn step_failed_default(
        context: SagaContext,
        error: Box<str>,
        will_retry: bool,
        requires_compensation: bool,
    ) -> Self {
        let participant_id = context.step_name.clone();
        Self::step_failed_for_participant(
            context,
            participant_id,
            None,
            error,
            will_retry,
            requires_compensation,
        )
    }

    pub fn step_failed_for_participant(
        context: SagaContext,
        participant_id: Box<str>,
        error_code: Option<Box<str>>,
        error: Box<str>,
        will_retry: bool,
        requires_compensation: bool,
    ) -> Self {
        Self::StepFailed {
            context,
            participant_id,
            error_code,
            error,
            will_retry,
            requires_compensation,
        }
    }

    pub fn step_failed_for_actor_id(
        context: SagaContext,
        participant_id: ActorId,
        error_code: Option<Box<str>>,
        error: Box<str>,
        will_retry: bool,
        requires_compensation: bool,
    ) -> Self {
        Self::step_failed_for_participant(
            context,
            participant_id.as_str().to_string().into_boxed_str(),
            error_code,
            error,
            will_retry,
            requires_compensation,
        )
    }

    /// Returns a reference to the saga context associated with this event.
    ///
    /// @return A reference to the `SagaContext` containing saga identifiers and metadata.
    pub fn context(&self) -> &SagaContext {
        match self {
            Self::SagaStarted { context, .. } => context,
            Self::SagaCompleted { context } => context,
            Self::SagaFailed { context, .. } => context,
            Self::StepStarted { context } => context,
            Self::StepCompleted { context, .. } => context,
            Self::StepFailed { context, .. } => context,
            Self::CompensationRequested { context, .. } => context,
            Self::CompensationStarted { context } => context,
            Self::CompensationCompleted { context } => context,
            Self::CompensationFailed { context, .. } => context,
            Self::SagaQuarantined { context, .. } => context,
            Self::StepAck { context, .. } => context,
        }
    }

    /// Returns a static string identifier for this event type.
    ///
    /// @return A `&'static str` representing the event type name (e.g., "saga_started", "step_completed").
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::SagaStarted { .. } => "saga_started",
            Self::SagaCompleted { .. } => "saga_completed",
            Self::SagaFailed { .. } => "saga_failed",
            Self::StepStarted { .. } => "step_started",
            Self::StepCompleted { .. } => "step_completed",
            Self::StepFailed { .. } => "step_failed",
            Self::CompensationRequested { .. } => "compensation_requested",
            Self::CompensationStarted { .. } => "compensation_started",
            Self::CompensationCompleted { .. } => "compensation_completed",
            Self::CompensationFailed { .. } => "compensation_failed",
            Self::SagaQuarantined { .. } => "saga_quarantined",
            Self::StepAck { .. } => "step_ack",
        }
    }

    pub fn terminal_outcome(&self) -> Option<SagaTerminalOutcome> {
        match self {
            Self::SagaCompleted { context } => Some(SagaTerminalOutcome::Completed {
                context: context.clone(),
            }),
            Self::SagaFailed {
                context,
                reason,
                failure,
            } => Some(SagaTerminalOutcome::Failed {
                context: context.clone(),
                reason: reason.clone(),
                failure: failure.clone(),
            }),
            Self::SagaQuarantined {
                context,
                reason,
                step,
                participant_id,
            } => Some(SagaTerminalOutcome::Quarantined {
                context: context.clone(),
                reason: reason.clone(),
                step: step.clone(),
                participant_id: participant_id.clone(),
            }),
            _ => None,
        }
    }
}

impl icanact_core::local::EventTopic for SagaChoreographyEvent {
    fn event_topic(&self) -> &str {
        self.context().saga_type.as_ref()
    }
}

/// Acknowledgment status for step processing responses.
#[derive(Clone, Debug)]
pub enum AckStatus {
    /// The step has been accepted and queued for processing.
    Accepted,
    /// The step has completed successfully.
    Completed,
    /// The step has failed during processing.
    Failed,
    /// The step is not applicable to this participant.
    NotApplicable,
    /// The step is already being processed by this participant.
    AlreadyProcessing,
}

/// Events stored in participant's local journal for durability and recovery.
#[derive(Clone, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum ParticipantEvent {
    /// Emitted when a participant registers to handle a step in a saga type.
    SagaRegistered {
        /// The type of saga this participant is registered for.
        saga_type: Box<str>,
        /// The name of the step this participant will handle.
        step_name: Box<str>,
        /// The timestamp (in milliseconds since epoch) when registration occurred.
        registered_at_millis: u64,
    },
    /// Emitted when a step is triggered by an incoming choreography event.
    StepTriggered {
        /// The type of event that triggered this step.
        triggering_event: Box<str>,
        /// The timestamp (in milliseconds since epoch) when the step was triggered.
        triggered_at_millis: u64,
    },
    /// Emitted when step execution begins.
    StepExecutionStarted {
        /// The current attempt number (1 for initial attempt, incremented on retries).
        attempt: u32,
        /// The timestamp (in milliseconds since epoch) when execution started.
        started_at_millis: u64,
    },
    /// Emitted when step execution completes successfully.
    StepExecutionCompleted {
        /// The output produced by the step execution.
        output: Vec<u8>,
        /// Data stored for potential compensation if rollback is needed.
        compensation_data: Vec<u8>,
        /// The timestamp (in milliseconds since epoch) when execution completed.
        completed_at_millis: u64,
    },
    /// Emitted when step execution fails.
    StepExecutionFailed {
        /// The error message describing why execution failed.
        error: Box<str>,
        /// Whether compensation is required due to this failure.
        requires_compensation: bool,
        /// The timestamp (in milliseconds since epoch) when execution failed.
        failed_at_millis: u64,
    },
    /// Emitted when compensation execution begins.
    CompensationStarted {
        /// The current attempt number (1 for initial attempt, incremented on retries).
        attempt: u32,
        /// The timestamp (in milliseconds since epoch) when compensation started.
        started_at_millis: u64,
    },
    /// Emitted when compensation completes successfully.
    CompensationCompleted {
        /// The timestamp (in milliseconds since epoch) when compensation completed.
        completed_at_millis: u64,
    },
    /// Emitted when compensation fails.
    CompensationFailed {
        /// The error message describing why compensation failed.
        error: Box<str>,
        /// Whether the system state is ambiguous (partial compensation may have occurred).
        is_ambiguous: bool,
        /// The timestamp (in milliseconds since epoch) when compensation failed.
        failed_at_millis: u64,
    },
    /// Emitted when a participant is quarantined due to unrecoverable errors.
    Quarantined {
        /// The reason the participant was quarantined.
        reason: Box<str>,
        /// The timestamp (in milliseconds since epoch) when quarantine occurred.
        quarantined_at_millis: u64,
    },
}
