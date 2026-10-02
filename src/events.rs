//! Saga events

use super::{AcceptedStepTimeoutOutcome, SagaContext, StepExecutionId};
use crate::StepExecutionIntent;
use icanact_core::ActorId;

#[derive(Clone, Debug, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct SagaFailureDetails {
    pub step_name: Box<str>,
    pub participant_id: Box<str>,
    pub error_code: Option<Box<str>>,
    pub error_message: Box<str>,
    pub at_millis: u64,
}

/// Why infrastructure requested an abort (ADR-0004).
#[non_exhaustive]
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum AbortSource {
    /// Fewer participants received the start than required (ADR-0004).
    DeliveryShortfall,
    /// Only part of the participants received the start (ADR-0004).
    PartialDelivery,
    /// The overall saga timeout elapsed (ADR-0004).
    OverallTimeout,
    /// The saga stalled without progress (ADR-0004).
    StalledTimeout,
    /// Recovery found a stale run (ADR-0004).
    StaleRecovery,
}

/// Why an effect remains in the world after a terminal decision (ADR-0004).
#[non_exhaustive]
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum RetainedEffectDisposition {
    /// Kept deliberately by the loser policy (ADR-0004).
    KeptByPolicy,
    /// The step has no compensation (ADR-0004).
    NotCompensable,
    /// The undo was attempted and failed (ADR-0004).
    UndoFailed,
}

/// Events published via the local saga event bus.
#[derive(Clone, Debug, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
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
    /// Emitted when a participant accepts async responsibility for a step.
    #[non_exhaustive]
    StepAccepted {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
        /// Participant that accepted responsibility for this step.
        participant_id: Box<str>,
        /// External or participant-local execution identifier.
        execution_id: StepExecutionId,
        /// Resettable idle deadline in epoch milliseconds.
        deadline_at_millis: u64,
        /// Non-resettable hard deadline in epoch milliseconds.
        hard_deadline_at_millis: u64,
        /// Whether timeout evaluation is armed for this accepted state.
        /// Recovery hydration disarms it until the immediately following
        /// authoritative failure is ingested.
        timeouts_enabled: bool,
        /// Terminal outcome to apply when accepted step deadline expires.
        timeout_outcome: AcceptedStepTimeoutOutcome,
        /// Whether this accepted step owns an effect that must be compensated on failure.
        compensation_available: bool,
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
        /// Exact failure evidence that triggered compensation.
        failure: SagaFailureDetails,
        /// The list of step names that need to be compensated, in reverse execution order.
        steps_to_compensate: Vec<Box<str>>,
    },
    /// Emitted when compensation begins execution.
    CompensationStarted {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
    },
    /// Emitted when compensation was dispatched and awaits authoritative resolution.
    #[non_exhaustive]
    CompensationAccepted {
        context: SagaContext,
        participant_id: Box<str>,
        execution_id: StepExecutionId,
        deadline_at_millis: u64,
        hard_deadline_at_millis: u64,
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
        participant_id: Box<str>,
        /// The status of the acknowledgment.
        status: AckStatus,
    },
    /// Infrastructure requests rollback of the run; resolver input, participants ignore it (ADR-0004).
    SagaAbortRequested {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
        /// Human-readable abort reason.
        reason: Box<str>,
        /// Why the abort was requested.
        source: AbortSource,
    },
    /// Compensation failed without side effects and may be retried (ADR-0004).
    CompensationFailedRetryable {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
        /// Participant whose compensation failed.
        participant_id: Box<str>,
        /// The error that caused the failure.
        error: Box<str>,
    },
    /// Effects that remain after a terminal decision, recorded instead of a contradictory terminal (ADR-0004).
    SagaEffectsRetained {
        /// The saga context containing identifiers and metadata.
        context: SagaContext,
        /// Steps whose effects remain.
        steps: Vec<Box<str>>,
        /// Why the effects remain.
        disposition: RetainedEffectDisposition,
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
        requires_compensation: bool,
    ) -> Self {
        let participant_id = context.step_name.clone();
        Self::step_failed_for_participant(
            context,
            participant_id,
            None,
            error,
            requires_compensation,
        )
    }

    pub fn step_failed_for_participant(
        context: SagaContext,
        participant_id: Box<str>,
        error_code: Option<Box<str>>,
        error: Box<str>,
        requires_compensation: bool,
    ) -> Self {
        Self::StepFailed {
            context,
            participant_id,
            error_code,
            error,
            requires_compensation,
        }
    }

    pub fn step_failed_for_actor_id(
        context: SagaContext,
        participant_id: ActorId,
        error_code: Option<Box<str>>,
        error: Box<str>,
        requires_compensation: bool,
    ) -> Self {
        Self::step_failed_for_participant(
            context,
            participant_id.as_str().to_string().into_boxed_str(),
            error_code,
            error,
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
            Self::StepAccepted { context, .. } => context,
            Self::StepCompleted { context, .. } => context,
            Self::StepFailed { context, .. } => context,
            Self::CompensationRequested { context, .. } => context,
            Self::CompensationStarted { context } => context,
            Self::CompensationAccepted { context, .. } => context,
            Self::CompensationCompleted { context } => context,
            Self::CompensationFailed { context, .. } => context,
            Self::SagaQuarantined { context, .. } => context,
            Self::StepAck { context, .. } => context,
            Self::SagaAbortRequested { context, .. } => context,
            Self::CompensationFailedRetryable { context, .. } => context,
            Self::SagaEffectsRetained { context, .. } => context,
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
            Self::StepAccepted { .. } => "step_accepted",
            Self::StepCompleted { .. } => "step_completed",
            Self::StepFailed { .. } => "step_failed",
            Self::CompensationRequested { .. } => "compensation_requested",
            Self::CompensationStarted { .. } => "compensation_started",
            Self::CompensationAccepted { .. } => "compensation_accepted",
            Self::CompensationCompleted { .. } => "compensation_completed",
            Self::CompensationFailed { .. } => "compensation_failed",
            Self::SagaQuarantined { .. } => "saga_quarantined",
            Self::StepAck { .. } => "step_ack",
            Self::SagaAbortRequested { .. } => "saga_abort_requested",
            Self::CompensationFailedRetryable { .. } => "compensation_failed_retryable",
            Self::SagaEffectsRetained { .. } => "saga_effects_retained",
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

/// Acknowledgment status for step processing responses.
#[derive(Clone, Debug, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
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
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(bounds(
    __C: rkyv::validation::ArchiveContext,
    __C::Error: rkyv::rancor::Source,
)))]
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
    /// Durable copy of the compensation request that caused external rollback work.
    CompensationRequestRecorded {
        context: SagaContext,
        failed_step: Box<str>,
        reason: Box<str>,
        failure: SagaFailureDetails,
        steps_to_compensate: Vec<Box<str>>,
        requested_at_millis: u64,
    },
    /// Emitted when a participant is quarantined due to unrecoverable errors.
    Quarantined {
        /// The reason the participant was quarantined.
        reason: Box<str>,
        /// The timestamp (in milliseconds since epoch) when quarantine occurred.
        quarantined_at_millis: u64,
    },
    /// Emitted when an async step is accepted and must survive participant restart.
    AcceptedStepRecorded {
        /// Saga context stamped at acceptance time.
        context: SagaContext,
        /// Participant that accepted responsibility for the step.
        participant_id: Box<str>,
        /// External or participant-local execution identifier.
        execution_id: StepExecutionId,
        /// Resettable idle timeout duration in milliseconds.
        idle_timeout_millis: u64,
        /// Non-resettable hard timeout duration in milliseconds.
        hard_timeout_millis: u64,
        /// Terminal outcome used when the accepted step times out.
        timeout_outcome: AcceptedStepTimeoutOutcome,
        /// Original workflow input needed for deterministic recovery.
        saga_input: Vec<u8>,
        /// Compensation data for an accepted step that owns an external effect.
        compensation_data: Vec<u8>,
        /// Timestamp when the participant accepted the step.
        accepted_at_millis: u64,
        /// Current resettable idle deadline in epoch milliseconds.
        deadline_at_millis: u64,
        /// Non-resettable hard deadline in epoch milliseconds.
        hard_deadline_at_millis: u64,
    },
    /// Durable metadata for compensation awaiting authoritative resolution.
    AcceptedCompensationRecorded {
        context: SagaContext,
        participant_id: Box<str>,
        execution_id: StepExecutionId,
        /// Original workflow input needed for deterministic participant recovery.
        saga_input: Vec<u8>,
        /// Exact compensation input accepted by the participant.
        compensation_data: Vec<u8>,
        idle_timeout_millis: u64,
        hard_timeout_millis: u64,
        accepted_at_millis: u64,
        deadline_at_millis: u64,
        hard_deadline_at_millis: u64,
    },
    /// Atomic inbox commit: admitted input, join progress and optional execution intent (ADR-0005).
    InboxCommitted {
        input_key: Box<str>,
        dependency_step: Option<Box<str>>,
        execution_intent: Option<StepExecutionIntent>,
        admitted_at_millis: u64,
    },
    /// A transition committed together with the outbound events it obliges, in one row (ADR-0003).
    TransitionCommitted {
        #[rkyv(omit_bounds)]
        transition: Box<ParticipantEvent>,
        outbox: Vec<SagaChoreographyEvent>,
    },
    /// The undo reported `SafeToRetry` (ADR-0004 §2.5): it did not take effect and the resolver may
    /// re-request it. Closes the preceding `CompensationStarted`; without it a `CompensationStarted`
    /// that never settled means the undo may have run (crash mid-undo) and the run is quarantined.
    CompensationRetryable {
        /// The attempt of the request whose undo reported `SafeToRetry`.
        attempt: u32,
        /// Why the undo is retryable.
        reason: Box<str>,
        /// The timestamp (in milliseconds since epoch) of the retryable outcome.
        retryable_at_millis: u64,
    },
}

impl ParticipantEvent {
    /// The state transition of this row: the wrapped transition of `TransitionCommitted`, else `self` (ADR-0003).
    pub fn transition(&self) -> &ParticipantEvent {
        match self {
            Self::TransitionCommitted { transition, .. } => transition,
            other => other,
        }
    }

    /// Outbound obligations embedded in this row; empty for every other variant (ADR-0003).
    pub fn outbox(&self) -> &[SagaChoreographyEvent] {
        match self {
            Self::TransitionCommitted { outbox, .. } => outbox,
            _ => &[],
        }
    }
}
