//! Error types for saga execution and compensation

/// Output from step execution
#[derive(Clone, Debug)]
pub enum StepOutput {
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
