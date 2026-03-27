//! Choreography-Based SAGA for icanact-core Actors
//!
//! A choreography-based SAGA pattern that integrates natively with `icanact-core` actors.
//! Actors implement saga business behavior via `SagaParticipant`, and can either:
//!
//! - implement `SagaStateExt` directly, or
//! - embed `SagaParticipantSupport` and implement `HasSagaParticipantSupport`
//!   to get `SagaStateExt` automatically.
//!
//! # Quick Start
//!
//! ```rust,ignore
//! // 1. Add one embedded saga support field to your actor
//! pub struct MyActor {
//!     saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
//! }
//!
//! // 2. Expose the embedded support
//! impl HasSagaParticipantSupport for MyActor {
//!     type Journal = InMemoryJournal;
//!     type Dedupe = InMemoryDedupe;
//!
//!     fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
//!         &self.saga
//!     }
//!     fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
//!         &mut self.saga
//!     }
//! }
//!
//! // 3. Implement SagaParticipant (business logic)
//! impl SagaParticipant for MyActor { /* ... */ }
//!
//! // 4. Handle saga events in Actor::handle
//! MyActorCommand::SagaEvent { event } => handle_saga_event(self, event),
//! ```

#![allow(missing_docs, unused_imports, unused_variables, dead_code)]

// === Core Types ===
mod bus;
mod context;
pub mod durability;
mod errors;
mod events;
mod idempotency;
mod state;
mod support;

// === Traits ===
mod state_ext;
mod traits;

// === Storage ===
mod dedupe;
mod journal;

// === Observability ===
mod observer;
mod stats;

// === Helpers ===
mod helpers;
mod reply_registry;
mod resolver;
mod testkit;

// === Re-exports ===

// Types
pub use bus::SagaChoreographyBus;
pub use context::{PeerId, SagaContext, SagaId, StepId};
pub use durability::*;
pub use idempotency::IdempotencyKey;

// State (typestate)
pub use state::{
    Compensated, Compensating, Completed, Executing, Failed, Idle, Quarantined,
    SagaParticipantState, SagaStateEntry, TimestampedEvent, Triggered,
};
pub use support::{HasSagaParticipantSupport, SagaParticipantSupport, SagaParticipantSupportExt};

// Events
pub use events::{
    AckStatus, ParticipantEvent, SagaChoreographyEvent, SagaDelegatedReply, SagaFailureDetails,
    SagaTerminalOutcome,
};

// Errors
pub use errors::{CompensationError, StepError, StepOutput};

// Traits
pub use state_ext::SagaStateExt;
pub use traits::{AsyncSagaParticipant, DependencySpec, RetryPolicy, SagaBoxFuture, SagaParticipant};

// Storage
pub use dedupe::{DedupeError, InMemoryDedupe, ParticipantDedupeStore};
pub use journal::{InMemoryJournal, JournalEntry, JournalError, ParticipantJournal};

// Observability
pub use observer::{NoOpObserver, SagaObserver, TracingObserver};
pub use stats::{ParticipantStats, ParticipantStatsSnapshot};

// Helpers
pub use helpers::{
    compensate_wrapper, execute_step_wrapper, handle_async_saga_event,
    handle_async_saga_event_with_emit, handle_saga_event, handle_saga_event_with_emit,
    recover_sagas,
};
pub use reply_registry::{SagaDelegatedReplyHandle, SagaDelegatedReplyResult};
pub use resolver::{
    FailureAuthority, SuccessCriteria, TerminalPolicy, TerminalResolver, TERMINAL_RESOLVER_STEP,
};
pub use testkit::{
    compensation_requested, drive_scenario, saga_started, step_completed, step_failed,
    DeterministicContextBuilder,
};
