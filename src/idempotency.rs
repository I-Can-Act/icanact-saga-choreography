//! Idempotency key generation for saga steps.
//!
//! This module provides the [`IdempotencyKey`] type for generating unique
//! identifiers that enable safe retry semantics in distributed saga orchestration.
//!
//! # Purpose
//!
//! In distributed systems, network failures and timeouts can cause duplicate
//! message delivery. Idempotency keys allow saga participants to detect and
//! ignore duplicate operations, ensuring that each step executes exactly once
//! even when requests are retried.
//!
//! # Key Format
//!
//! Idempotency keys follow a structured format that includes:
//! - The saga identifier for correlation
//! - The step name for operation identification
//! - An attempt number (for step execution) to distinguish retries

use crate::SagaId;

/// A unique key for deduplicating side effects in saga execution.
///
/// This type wraps a string key that uniquely identifies a particular
/// operation within a saga, enabling idempotent execution. When a participant
/// receives a request with a key it has already processed, it can safely
/// skip the operation and return the cached result.
///
/// # Key Structure
///
/// Keys are formatted as structured strings containing:
/// - `saga:{id}` - The saga identifier
/// - `step:{name}` or `compensate:{name}` - The operation type and name
/// - `attempt:{n}` - The attempt number (for step execution only)
///
/// # Example
///
/// ```
/// # use icanact_saga_choreography::{IdempotencyKey, SagaId};
/// # let saga_id = SagaId::new(1);
/// let key = IdempotencyKey::for_step(saga_id, "reserve_inventory", 1);
/// assert!(key.as_str().contains("reserve_inventory"));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(pub Box<str>);

impl IdempotencyKey {
    /// Creates an idempotency key for a step execution attempt.
    ///
    /// This generates a unique key for a specific execution attempt of a saga
    /// step. Each retry attempt gets a different key, allowing the system to
    /// track individual attempts while still preventing duplicate processing
    /// of the same attempt.
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the saga
    /// * `step_name` - The name of the step being executed
    /// * `attempt` - The attempt number (starting from 1)
    ///
    /// # Returns
    ///
    /// An `IdempotencyKey` in the format `saga:{id}:step:{name}:attempt:{n}`
    ///
    /// # Example
    ///
    /// ```
    /// # use icanact_saga_choreography::{IdempotencyKey, SagaId};
    /// # let saga_id = SagaId::new(1);
    /// let key = IdempotencyKey::for_step(saga_id, "charge_payment", 2);
    /// // Key format: "saga:{uuid}:step:charge_payment:attempt:2"
    /// ```
    pub fn for_step(saga_id: SagaId, step_name: &str, attempt: u32) -> Self {
        Self(format!("saga:{}:step:{}:attempt:{}", saga_id.0, step_name, attempt).into_boxed_str())
    }

    /// Creates an idempotency key for a compensation operation.
    ///
    /// Compensation keys differ from step keys in that they do not include
    /// an attempt number, as compensation should execute exactly once per
    /// step regardless of how many times the original step was attempted.
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the saga
    /// * `step_name` - The name of the step being compensated
    ///
    /// # Returns
    ///
    /// An `IdempotencyKey` in the format `saga:{id}:compensate:{name}`
    ///
    /// # Example
    ///
    /// ```
    /// # use icanact_saga_choreography::{IdempotencyKey, SagaId};
    /// # let saga_id = SagaId::new(1);
    /// let key = IdempotencyKey::for_compensation(saga_id, "reserve_inventory");
    /// // Key format: "saga:{uuid}:compensate:reserve_inventory"
    /// ```
    pub fn for_compensation(saga_id: SagaId, step_name: &str) -> Self {
        Self(format!("saga:{}:compensate:{}", saga_id.0, step_name).into_boxed_str())
    }

    /// Returns the key as a string slice.
    ///
    /// This provides borrowed access to the underlying key string for
    /// use in lookups, comparisons, and storage operations.
    ///
    /// # Returns
    ///
    /// A string slice containing the full idempotency key.
    ///
    /// # Example
    ///
    /// ```
    /// # use icanact_saga_choreography::{IdempotencyKey, SagaId};
    /// # let saga_id = SagaId::new(1);
    /// let key = IdempotencyKey::for_step(saga_id, "process", 1);
    /// assert!(key.as_str().starts_with("saga:"));
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
