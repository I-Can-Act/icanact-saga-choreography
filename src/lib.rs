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
//! MyActorCommand::SagaEvent { event } => handle_saga_event_with_emit(self, event, |_| {}),
//! ```

#![allow(missing_docs)]

// === Core Types ===
mod binding;
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
mod workflow_contract;

// === Re-exports ===

// Types
pub use binding::{
    bind_async_participant_channel, bind_async_participant_channel_lazy,
    bind_async_participant_tell, bind_async_workflow_participant_channel,
    bind_async_workflow_participant_channel_lazy,
    bind_async_workflow_participant_channel_lazy_strict,
    bind_async_workflow_participant_channel_strict, bind_sync_participant_channel,
    bind_sync_participant_channel_lazy, bind_sync_participant_tell,
    bind_sync_workflow_participant_channel, bind_sync_workflow_participant_channel_lazy,
    bind_sync_workflow_participant_channel_lazy_strict,
    bind_sync_workflow_participant_channel_strict, bind_sync_workflow_participant_tell,
    bind_sync_workflow_participant_tell_strict, checked_workflow_saga_types, workflow_saga_types,
    SagaParticipantChannel,
};
pub use bus::{global_saga_choreography_bus, SagaBusPublishError, SagaChoreographyBus};
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
    AckStatus, ParticipantEvent, SagaChoreographyEvent, SagaFailureDetails, SagaReplyTo,
    SagaTerminalOutcome,
};

// Errors
pub use errors::{CompensationError, StepError, StepOutput};

// Traits
pub use state_ext::SagaStateExt;
pub use traits::{
    AllowsSagaTellIngress, AsyncSagaParticipant, DependencySpec, HasSagaWorkflowParticipants,
    SagaBoxFuture, SagaParticipant, SagaWorkflowParticipant,
};

// Storage
pub use dedupe::{DedupeError, InMemoryDedupe, ParticipantDedupeStore};
pub use journal::{InMemoryJournal, JournalEntry, JournalError, ParticipantJournal};

// Observability
pub use observer::{NoOpObserver, SagaObserver, TracingObserver};
pub use stats::{ParticipantStats, ParticipantStatsSnapshot};

// Helpers
pub use helpers::{handle_async_saga_event_with_emit, handle_saga_event_with_emit};
pub use reply_registry::{SagaReplyToHandle, SagaReplyToResult};
pub use resolver::{
    FailureAuthority, SuccessCriteria, TerminalPolicy, TerminalResolver, TERMINAL_RESOLVER_STEP,
};
#[cfg(any(test, feature = "test-harness"))]
pub use testkit::AsyncSagaParticipantHandle;
pub use testkit::{
    compensation_requested, drive_scenario, drive_workflow_scenario, saga_started, step_completed,
    step_failed, DeterministicContextBuilder,
};
#[cfg(any(test, feature = "test-harness"))]
pub use testkit::{SagaTestWorld, SyncSagaParticipantHandle};
pub use workflow_contract::{
    required_steps_from_success_criteria, validate_workflow_contract, SagaWorkflowContract,
    SagaWorkflowStepContract, WorkflowDependencySpec,
};
