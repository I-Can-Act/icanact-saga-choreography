//! Core traits for saga participants

use std::future::Future;
use std::pin::Pin;

use crate::{CompensationError, SagaContext, StepError, StepOutput};

use icanact_core::{ActorId, ActorIdError};

/// Trait for actors that participate in choreography-based sagas.
///
/// Actors implementing this trait handle saga events alongside their
/// normal business messages. The saga state is isolated from business
/// state using the typestate pattern.
///
/// # Example
///
/// ```rust,ignore
/// impl SagaParticipant for OrderManagerActor {
///     type Error = OrderError;
///     
///     fn step_name(&self) -> &str { "create_order" }
///     fn saga_types(&self) -> &[&'static str] { &["order_workflow"] }
///     
///     fn execute_step(&mut self, ctx: &SagaContext, input: &[u8])
///         -> Result<StepOutput, StepError>
///     {
///         // Use self.deribit_ws.ask() to place order
///     }
///     
///     fn compensate_step(&mut self, ctx: &SagaContext, data: &[u8])
///         -> Result<(), CompensationError>
///     {
///         // Cancel the order
///     }
/// }
/// ```
pub trait SagaParticipant: Send + 'static {
    /// Error type for this participant
    type Error: std::fmt::Debug + Send;

    /// The step name this participant handles
    fn step_name(&self) -> &str;

    /// Stable participant identity used for terminal failure fidelity.
    ///
    /// Defaults to the step name so existing participants remain compatible.
    fn participant_id(&self) -> &str {
        self.step_name()
    }

    /// Parsed `ActorId` view of participant identity when valid.
    fn participant_actor_id(&self) -> Result<ActorId, ActorIdError> {
        ActorId::new(self.participant_id())
    }

    /// Owned participant identity for event payloads.
    ///
    /// Framework internals should prefer this when constructing events that
    /// carry participant identity to avoid repeating ad-hoc conversion logic.
    fn participant_id_owned(&self) -> Box<str> {
        match self.participant_actor_id() {
            Ok(actor_id) => actor_id.as_str().to_string().into_boxed_str(),
            Err(_) => self.participant_id().to_string().into_boxed_str(),
        }
    }

    /// Which saga types this participant joins
    fn saga_types(&self) -> &[&'static str];

    /// Execute the forward step
    ///
    /// Called when a triggering event is received (based on `depends_on`).
    fn execute_step(
        &mut self,
        context: &SagaContext,
        input: &[u8],
    ) -> Result<StepOutput, StepError>;

    /// Execute compensation (undo)
    ///
    /// Called when `CompensationRequested` is received and this step
    /// is in the compensation list.
    fn compensate_step(
        &mut self,
        context: &SagaContext,
        compensation_data: &[u8],
    ) -> Result<(), CompensationError>;

    // === Optional Hooks ===

    /// Called after saga completes successfully
    fn on_saga_completed(&mut self, _context: &SagaContext) {}

    /// Called after saga fails
    fn on_saga_failed(&mut self, _context: &SagaContext, _reason: &str) {}

    /// Called after compensation completes
    fn on_compensation_completed(&mut self, _context: &SagaContext) {}

    /// Called when saga is quarantined
    fn on_quarantined(&mut self, _context: &SagaContext, _reason: &str) {}

    /// When does this participant execute?
    /// Default: execute when saga starts
    fn depends_on(&self) -> DependencySpec {
        DependencySpec::OnSagaStart
    }

    /// Retry policy for this step
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::default()
    }

    /// Timeout for step execution
    fn step_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(30)
    }
}

pub type SagaBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Async variant of [`SagaParticipant`].
///
/// Use this when a participant step needs to await actor asks or other async IO.
/// This keeps sync and async choreography execution models distinct at the type level.
pub trait AsyncSagaParticipant: Send + 'static {
    type Error: std::fmt::Debug + Send;

    fn step_name(&self) -> &str;

    fn participant_id(&self) -> &str {
        self.step_name()
    }

    fn participant_actor_id(&self) -> Result<ActorId, ActorIdError> {
        ActorId::new(self.participant_id())
    }

    fn participant_id_owned(&self) -> Box<str> {
        match self.participant_actor_id() {
            Ok(actor_id) => actor_id.as_str().to_string().into_boxed_str(),
            Err(_) => self.participant_id().to_string().into_boxed_str(),
        }
    }

    fn saga_types(&self) -> &[&'static str];

    fn execute_step<'a>(
        &'a mut self,
        context: &'a SagaContext,
        input: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<StepOutput, StepError>>;

    fn compensate_step<'a>(
        &'a mut self,
        context: &'a SagaContext,
        compensation_data: &'a [u8],
    ) -> SagaBoxFuture<'a, Result<(), CompensationError>>;

    fn on_saga_completed(&mut self, _context: &SagaContext) {}

    fn on_saga_failed(&mut self, _context: &SagaContext, _reason: &str) {}

    fn on_compensation_completed(&mut self, _context: &SagaContext) {}

    fn on_quarantined(&mut self, _context: &SagaContext, _reason: &str) {}

    fn depends_on(&self) -> DependencySpec {
        DependencySpec::OnSagaStart
    }

    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::default()
    }

    fn step_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(30)
    }
}

/// Dependency specification - when does this step execute?
#[derive(Clone, Debug)]
pub enum DependencySpec {
    /// Execute when saga starts (no dependencies)
    OnSagaStart,
    /// Execute after ANY of these steps complete
    AnyOf(&'static [&'static str]),
    /// Execute after ALL of these steps complete
    AllOf(&'static [&'static str]),
    /// Execute after this specific step
    After(&'static str),
}

impl DependencySpec {
    /// Check if a completed step satisfies this dependency
    pub fn is_satisfied_by(&self, completed_step: &str) -> bool {
        match self {
            DependencySpec::OnSagaStart => false,
            DependencySpec::After(step) => completed_step == *step,
            DependencySpec::AnyOf(steps) => steps.contains(&completed_step),
            DependencySpec::AllOf(steps) => steps.contains(&completed_step),
        }
    }

    /// Check if this is OnSagaStart
    pub fn is_on_saga_start(&self) -> bool {
        matches!(self, DependencySpec::OnSagaStart)
    }

    /// Whether dependent execution should use the original saga input
    /// rather than the upstream step output.
    pub fn prefers_original_saga_input(&self) -> bool {
        matches!(self, DependencySpec::AllOf(_))
    }
}

/// Retry policy for step execution
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Initial delay before first retry (milliseconds)
    pub initial_delay_millis: u64,
    /// Maximum delay cap (milliseconds)
    pub max_delay_millis: u64,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay_millis: 1000,
            max_delay_millis: 30000,
            backoff_multiplier: 2.0,
        }
    }
}

impl RetryPolicy {
    /// Calculate delay for a given attempt (1-indexed)
    pub fn delay_for_attempt(&self, attempt: u32) -> std::time::Duration {
        if attempt == 0 {
            return std::time::Duration::from_millis(0);
        }

        let delay = self.initial_delay_millis as f64
            * self
                .backoff_multiplier
                .powi(attempt.saturating_sub(1) as i32);
        let capped = delay.min(self.max_delay_millis as f64);
        std::time::Duration::from_millis(capped as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_delay() {
        let policy = RetryPolicy::default();

        assert_eq!(
            policy.delay_for_attempt(0),
            std::time::Duration::from_millis(0)
        );
        assert_eq!(
            policy.delay_for_attempt(1),
            std::time::Duration::from_millis(1000)
        );
        assert_eq!(
            policy.delay_for_attempt(2),
            std::time::Duration::from_millis(2000)
        );
        assert_eq!(
            policy.delay_for_attempt(3),
            std::time::Duration::from_millis(4000)
        );
        // Would be 8000 but capped at max
        assert!(policy.delay_for_attempt(10) <= std::time::Duration::from_millis(30000));
    }

    #[test]
    fn test_dependency_spec() {
        let spec = DependencySpec::After("reserve_inventory");
        assert!(spec.is_satisfied_by("reserve_inventory"));
        assert!(!spec.is_satisfied_by("other_step"));
        assert!(!spec.is_on_saga_start());
    }
}
