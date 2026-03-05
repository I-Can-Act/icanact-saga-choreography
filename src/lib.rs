//! Choreography-Based SAGA for icanact-core Actors
//!
//! A choreography-based SAGA pattern that integrates natively with `icanact-core` actors.
//! Actors implement two traits (`SagaParticipant` + `SagaStateExt`) to participate in sagas.
//!
//! # Quick Start
//!
//! ```rust,ignore
//! // 1. Add saga state fields to your actor
//! pub struct MyActor {
//!     saga_states: HashMap<SagaId, SagaStateEntry>,
//!     saga_journal: InMemoryJournal,
//!     saga_dedupe: InMemoryDedupe,
//!     saga_stats: ParticipantStats,
//! }
//!
//! // 2. Implement SagaStateExt (boilerplate)
//! impl SagaStateExt for MyActor { /* ... */ }
//!
//! // 3. Implement SagaParticipant (business logic)
//! impl SagaParticipant for MyActor { /* ... */ }
//!
//! // 4. Handle saga events in Actor::handle
//! MyActorCommand::SagaEvent { event } => handle_saga_event(self, event),
//! ```

#![allow(missing_docs, unused_imports, unused_variables, dead_code)]

// === Core Types ===
mod context;
mod errors;
mod events;
mod idempotency;
mod state;

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
mod testkit;

// === Re-exports ===

// Types
pub use context::{PeerId, SagaContext, SagaId, StepId};
pub use idempotency::IdempotencyKey;

// State (typestate)
pub use state::{
    Compensated, Compensating, Completed, Executing, Failed, Idle, Quarantined,
    SagaParticipantState, SagaStateEntry, TimestampedEvent, Triggered,
};

// Events
pub use events::{AckStatus, ParticipantEvent, SagaChoreographyEvent};

// Errors
pub use errors::{CompensationError, StepError, StepOutput};

// Traits
pub use state_ext::SagaStateExt;
pub use traits::{DependencySpec, RetryPolicy, SagaParticipant};

// Storage
pub use dedupe::{DedupeError, InMemoryDedupe, ParticipantDedupeStore};
pub use journal::{InMemoryJournal, JournalEntry, JournalError, ParticipantJournal};

// Observability
pub use observer::{NoOpObserver, SagaObserver, TracingObserver};
pub use stats::{ParticipantStats, ParticipantStatsSnapshot};

// Helpers
pub use helpers::{
    compensate_wrapper, execute_step_wrapper, handle_saga_event, handle_saga_event_with_emit,
    recover_sagas,
};
pub use testkit::{
    compensation_requested, drive_scenario, saga_started, step_completed, step_failed,
    DeterministicContextBuilder,
};
