//! Error types for saga execution and compensation

use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct StepExecutionId(Box<str>);

impl StepExecutionId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into().into_boxed_str())
    }
}

impl AsRef<str> for StepExecutionId {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl std::fmt::Display for StepExecutionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum AcceptedStepTimeoutOutcome {
    FailStep { requires_compensation: bool },
    QuarantineSaga,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedStepPolicy {
    pub idle_timeout: Duration,
    pub hard_timeout: Duration,
    pub timeout_outcome: AcceptedStepTimeoutOutcome,
}

impl AcceptedStepPolicy {
    pub fn validate(&self) -> Result<(), AcceptedStepPolicyError> {
        if self.idle_timeout.is_zero() {
            return Err(AcceptedStepPolicyError::ZeroIdleTimeout);
        }
        if self.hard_timeout.is_zero() {
            return Err(AcceptedStepPolicyError::ZeroHardTimeout);
        }
        if self.idle_timeout > self.hard_timeout {
            return Err(AcceptedStepPolicyError::IdleTimeoutExceedsHardTimeout {
                idle_timeout: self.idle_timeout,
                hard_timeout: self.hard_timeout,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcceptedStepPolicyError {
    ZeroIdleTimeout,
    ZeroHardTimeout,
    IdleTimeoutExceedsHardTimeout {
        idle_timeout: Duration,
        hard_timeout: Duration,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedStepCompletion {
    pub completed_at_millis: u64,
    pub output: Vec<u8>,
    pub compensation_data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedStepFailure {
    pub failed_at_millis: u64,
    pub reason: Box<str>,
    pub requires_compensation: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcceptedStepError {
    AlreadyAccepted {
        saga_id: super::SagaId,
        execution_id: StepExecutionId,
    },
    NotFound {
        saga_id: super::SagaId,
        execution_id: StepExecutionId,
    },
    ExecutionIdMismatch {
        saga_id: super::SagaId,
        expected: StepExecutionId,
        actual: StepExecutionId,
    },
    WorkflowNotFound {
        saga_id: super::SagaId,
        saga_type: Box<str>,
        step_name: Box<str>,
    },
    AlreadyResolved {
        saga_id: super::SagaId,
        execution_id: StepExecutionId,
    },
    AlreadyTerminal {
        saga_id: super::SagaId,
        execution_id: StepExecutionId,
    },
    InvalidPolicy {
        execution_id: StepExecutionId,
        source: AcceptedStepPolicyError,
    },
    Durability {
        saga_id: super::SagaId,
        execution_id: StepExecutionId,
        error: Box<str>,
    },
}

/// Output from step execution
#[derive(Clone, Debug)]
pub enum StepOutput {
    /// The participant durably accepted responsibility and will resolve later.
    Accepted {
        /// Stable external or participant-local execution identity.
        execution_id: StepExecutionId,
        /// Per-execution timeout policy.
        policy: AcceptedStepPolicy,
        /// Data required to release any side effect if the accepted step fails.
        compensation_data: Vec<u8>,
    },
    /// Step completed successfully
    Completed {
        /// Output data (passed to next step or stored)
        output: Vec<u8>,
        /// Data needed for compensation (stored until saga completes)
        compensation_data: Vec<u8>,
    },
    /// Step completed with an effect to emit
    CompletedWithEffect {
        /// Output data
        output: Vec<u8>,
        /// Compensation data
        compensation_data: Vec<u8>,
        /// Effect identifier (actor message to send)
        effect: Box<str>,
    },
}

/// Output from compensation execution.
#[derive(Clone, Debug)]
pub enum CompensationOutput {
    /// Compensation completed synchronously.
    Completed,
    /// Compensation was dispatched and will resolve from authoritative state later.
    Accepted {
        /// Stable compensation execution identity.
        execution_id: StepExecutionId,
        /// Deadline policy; compensation timeout must quarantine ambiguous state.
        policy: AcceptedStepPolicy,
    },
}

/// Completion data for a previously accepted compensation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedCompensationCompletion {
    pub completed_at_millis: u64,
}

/// Failure data for a previously accepted compensation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedCompensationFailure {
    pub failed_at_millis: u64,
    pub reason: Box<str>,
    pub is_ambiguous: bool,
}

/// Error from step execution
#[derive(Clone, Debug)]
pub enum StepError {
    /// Permanent error - fail saga without compensation
    Terminal {
        /// Error description
        reason: Box<str>,
    },
    /// Error that requires compensation
    RequireCompensation {
        /// Error description
        reason: Box<str>,
    },
}

impl StepError {
    /// Check if this error requires compensation
    pub fn requires_compensation(&self) -> bool {
        matches!(self, Self::RequireCompensation { .. })
    }
}

/// Error from compensation execution
#[derive(Clone, Debug)]
pub enum CompensationError {
    /// Safe to retry - no side effects were applied
    SafeToRetry {
        /// Error description
        reason: Box<str>,
    },
    /// Ambiguous state - compensation may or may not have applied
    Ambiguous {
        /// Error description
        reason: Box<str>,
    },
    /// Terminal failure - cannot compensate
    Terminal {
        /// Error description
        reason: Box<str>,
    },
}

impl CompensationError {
    /// Check if safe to retry
    pub fn is_safe_to_retry(&self) -> bool {
        matches!(self, Self::SafeToRetry { .. })
    }

    /// Check if state is ambiguous
    pub fn is_ambiguous(&self) -> bool {
        matches!(self, Self::Ambiguous { .. })
    }
}
