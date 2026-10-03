//! Extension trait for saga state management.
//!
//! This module provides the [`SagaStateExt`] trait, which supplies common
//! state-management methods for saga participants.
//!
//! Preferred integration: embed [`crate::SagaParticipantSupport`] in your actor
//! and implement [`crate::HasSagaParticipantSupport`]. This crate will then
//! provide `SagaStateExt` automatically.

use crate::{
    DedupeError, HasSagaParticipantSupport, JournalError, ParticipantDedupeStore, ParticipantEvent,
    ParticipantForwardOutcome, ParticipantJournal, ParticipantTerminalKind, SagaChoreographyEvent,
    SagaContext, SagaId, SagaStateEntry,
};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug)]
pub enum SagaStateStoreError {
    Dedupe(DedupeError),
    Journal(JournalError),
}

impl std::fmt::Display for SagaStateStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dedupe(err) => write!(f, "dedupe store: {err}"),
            Self::Journal(err) => write!(f, "journal store: {err}"),
        }
    }
}

impl std::error::Error for SagaStateStoreError {}

/// Result of shared durable participant admission for one event's run identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantAdmission {
    /// The event's run may be processed; its run record is durable.
    Admitted,
    /// The same run (id, type, start time) already has a durable terminal tombstone.
    TerminalReplay { outcome: ParticipantTerminalKind },
    /// A strictly newer run for this saga id has already been recorded.
    StaleRun { latest_started_at_millis: u64 },
    /// An unresolved run still owns this saga id; a new run cannot replace it.
    ActiveRunReuse { active_started_at_millis: u64 },
    /// Legacy execution evidence has no durable run identity and needs reconciliation.
    LegacyHistory,
    /// The latest run is quarantined; the saga id cannot be reused until an operator
    /// explicitly resolves or archives it.
    QuarantinedReuse {
        quarantined_started_at_millis: u64,
        reason: Box<str>,
    },
}

impl ParticipantAdmission {
    pub fn is_admitted(&self) -> bool {
        matches!(self, Self::Admitted)
    }
}

/// Result of a run-scoped dedupe mark. Storage failures are `Err`, never `Duplicate`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunDedupe {
    First,
    Duplicate,
}

/// Journal-derived current-run evidence. Confirmation is distinct from a raw
/// result written before a declared effect dispatch. No volatile cache is authority.
#[derive(Clone, Debug, Default)]
pub struct ParticipantRunEvidence {
    pub forward_intent_open: bool,
    pub accepted_forward_pending: bool,
    pub forward_result_recorded: bool,
    pub forward_outcome: Option<ParticipantForwardOutcome>,
    pub output: Vec<u8>,
    pub compensation_data: Vec<u8>,
    pub undo_intent_open: bool,
    /// An owned undo request or compensating accepted failure remains unresolved
    /// even before execution of the undo has started.
    pub undo_required: bool,
    pub undo_completed: bool,
    pub quarantined: bool,
    pub needs_reconciliation: bool,
    pub last_updated_at_millis: u64,
}

impl ParticipantRunEvidence {
    /// A failure may be ordinary only when this participant has no unknown or
    /// unreversed work. Successful sagas intentionally keep their business effects.
    pub fn failure_requires_quarantine(&self) -> bool {
        self.forward_intent_open
            || self.accepted_forward_pending
            || self.undo_intent_open
            || (self.undo_required && !self.undo_completed)
            || self.quarantined
            || self.needs_reconciliation
            || (!self.compensation_data.is_empty() && !self.undo_completed)
            || (self
                .forward_outcome
                .as_ref()
                .is_some_and(|o| o.effect.is_some())
                && !self.undo_completed)
            || (self.forward_result_recorded
                && self.forward_outcome.is_none()
                && !self.undo_completed)
    }
}

/// Extension trait providing common saga state management operations.
///
/// This trait defines the core interface for types that manage saga lifecycle
/// state, including access to state storage, journaling, and deduplication.
/// This trait is only available to types that implement
/// [`crate::HasSagaParticipantSupport`]. Manual `SagaStateExt` implementations
/// are intentionally not supported.
///
/// # Implementation Requirements
///
/// Implementors must provide:
/// - Mutable and immutable access to the saga state map
/// - Access to the participant journal for event persistence
/// - Access to the deduplication store for idempotency tracking
/// - A monotonic timestamp source for time-based operations
///
/// # Example
///
/// ```ignore
/// struct MyActor {
///     saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
/// }
///
/// impl HasSagaParticipantSupport for MyActor {
///     type Journal = InMemoryJournal;
///     type Dedupe = InMemoryDedupe;
///
///     fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
///         &self.saga
///     }
///
///     fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
///         &mut self.saga
///     }
/// }
/// ```
pub trait SagaStateExt: HasSagaParticipantSupport {
    /// Returns mutable access to the saga state map.
    ///
    /// This provides direct access to the underlying storage for saga state entries,
    /// allowing modifications such as inserting new sagas or updating existing ones.
    fn saga_states(&mut self) -> &mut HashMap<SagaId, SagaStateEntry> {
        &mut self.saga_support_mut().saga_states
    }

    /// Returns immutable access to the saga state map.
    ///
    /// Use this for read-only operations that need to inspect saga state
    /// without modifying it.
    fn saga_states_ref(&self) -> &HashMap<SagaId, SagaStateEntry> {
        &self.saga_support().saga_states
    }

    /// Returns mutable access to per-saga dependency completion tracking.
    fn dependency_completions(&mut self) -> &mut HashMap<SagaId, HashSet<Box<str>>> {
        &mut self.saga_support_mut().dependency_completions
    }

    /// Returns mutable access to per-saga dependency fire tracking.
    fn dependency_fired(&mut self) -> &mut HashSet<SagaId> {
        &mut self.saga_support_mut().dependency_fired
    }

    /// Clears per-run in-memory tracking for a saga id before a new run or prune.
    fn clear_in_memory_saga_run_tracking(&mut self, saga_id: SagaId) {
        self.saga_states().remove(&saga_id);
        self.dependency_completions().remove(&saga_id);
        self.dependency_fired().remove(&saga_id);
        self.saga_support_mut()
            .accepted_workflow_steps
            .remove(&saga_id);
        self.saga_support_mut()
            .accepted_workflow_compensations
            .remove(&saga_id);
        self.saga_support_mut()
            .resolved_workflow_steps
            .retain(|(resolved_saga_id, _)| *resolved_saga_id != saga_id);
    }

    /// Returns mutable access to terminal saga latches.
    fn terminal_sagas(&mut self) -> &mut HashSet<SagaId> {
        &mut self.saga_support_mut().terminal_sagas
    }

    fn terminal_saga_order(&mut self) -> &mut VecDeque<SagaId> {
        &mut self.saga_support_mut().terminal_saga_order
    }

    /// Returns true when this participant has already observed terminal saga state
    /// for the given saga id and should ignore late replays until a new SagaStarted resets it.
    fn is_terminal_saga_latched(&self, saga_id: SagaId) -> bool {
        self.saga_support().terminal_sagas.contains(&saga_id)
    }

    fn is_terminal_saga_start_replay(&self, saga_id: SagaId, started_at_millis: u64) -> bool {
        self.is_terminal_saga_latched(saga_id)
            && self
                .saga_support()
                .saga_run_started_at
                .get(&saga_id)
                .is_some_and(|started_at| *started_at == started_at_millis)
    }

    fn record_saga_run_start(&mut self, saga_id: SagaId, started_at_millis: u64) {
        self.saga_support_mut()
            .saga_run_started_at
            .insert(saga_id, started_at_millis);
    }

    fn terminal_latch_retention_limit(&self) -> usize {
        static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        *LIMIT.get_or_init(
            || match std::env::var("SAGA_PARTICIPANT_TERMINAL_LATCH_RETENTION") {
                Ok(raw) => match raw.parse::<usize>() {
                    Ok(parsed) if parsed > 0 => parsed,
                    _ => 4096,
                },
                Err(_) => 4096,
            },
        )
    }

    fn latch_terminal_saga(&mut self, saga_id: SagaId) {
        let inserted = self.terminal_sagas().insert(saga_id);
        if !inserted {
            return;
        }
        self.terminal_saga_order().push_back(saga_id);
        let cap = self.terminal_latch_retention_limit();
        while self.terminal_saga_order().len() > cap {
            let Some(evicted) = self.terminal_saga_order().pop_front() else {
                break;
            };
            self.terminal_sagas().remove(&evicted);
            self.saga_support_mut().saga_run_started_at.remove(&evicted);
        }
    }

    fn unlatch_terminal_saga(&mut self, saga_id: SagaId) {
        self.terminal_sagas().remove(&saga_id);
        self.terminal_saga_order().retain(|entry| *entry != saga_id);
    }

    /// Returns the participant journal for event persistence.
    ///
    /// The journal is used to durably record saga events for recovery
    /// and audit purposes.
    fn saga_journal(&self) -> &<Self as HasSagaParticipantSupport>::Journal {
        &self.saga_support().journal
    }

    /// Returns the deduplication store for idempotency tracking.
    ///
    /// The dedupe store tracks which operations have already been processed
    /// to prevent duplicate execution of side effects.
    fn saga_dedupe(&self) -> &<Self as HasSagaParticipantSupport>::Dedupe {
        &self.saga_support().dedupe
    }

    /// Returns the current timestamp in milliseconds.
    ///
    /// This should return a monotonically increasing value suitable for
    /// time-based operations such as timeouts and expiration checks.
    fn now_millis(&self) -> u64 {
        match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(duration) => duration.as_millis() as u64,
            Err(err) => {
                tracing::error!(
                    target: "core::saga",
                    event = "saga_state_now_millis_failed",
                    error = %err
                );
                0
            }
        }
    }

    /// Checks and marks a deduplication key for the given saga.
    ///
    /// Returns `true` if this is the first time the key has been seen for
    /// this saga (indicating the operation should proceed), or `false` if
    /// the key has already been processed (indicating the operation should
    /// be skipped as a duplicate).
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the saga
    /// * `key` - The deduplication key to check
    ///
    /// # Returns
    ///
    /// `true` if the operation is new and should proceed, `false` if it
    /// has already been processed.
    fn check_dedupe_strict(&self, saga_id: SagaId, key: &str) -> Result<bool, SagaStateStoreError> {
        self.saga_dedupe()
            .check_and_mark(saga_id, key)
            .map_err(SagaStateStoreError::Dedupe)
    }

    fn check_dedupe(&self, saga_id: SagaId, key: &str) -> bool {
        match self.saga_dedupe().check_and_mark(saga_id, key) {
            Ok(value) => value,
            Err(err) => {
                tracing::error!(
                    target: "core::saga",
                    event = "saga_state_dedupe_check_failed",
                    saga_id = saga_id.get(),
                    key,
                    error = %err
                );
                false
            }
        }
    }

    /// Records an event to the saga journal.
    ///
    /// Appends the event to the durable journal and propagates storage errors.
    /// Critical intent/result/fence writes must use this strict operation.
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the saga
    /// * `event` - The participant event to record
    fn record_event_strict(
        &self,
        saga_id: SagaId,
        event: ParticipantEvent,
    ) -> Result<(), SagaStateStoreError> {
        self.saga_journal()
            .append(saga_id, event)
            .map(|_| ())
            .map_err(SagaStateStoreError::Journal)
    }

    fn record_event(&self, saga_id: SagaId, event: ParticipantEvent) {
        if let Err(err) = self.record_event_strict(saga_id, event) {
            tracing::error!(
                target: "core::saga",
                event = "saga_state_journal_append_failed",
                saga_id = saga_id.get(),
                error = ?err
            );
        }
    }

    /// Removes all state associated with a saga.
    ///
    /// This destructive administrative primitive removes the durable replay fence
    /// as well as volatile state, journal history and dedupe entries. Never use it
    /// as routine terminal cleanup or to unblock a quarantine. Preserve an audited
    /// replacement fence before deliberate deletion.
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the saga to prune
    fn prune_saga_strict(&mut self, saga_id: SagaId) -> Result<(), SagaStateStoreError> {
        self.clear_in_memory_saga_run_tracking(saga_id);
        self.saga_journal()
            .prune(saga_id)
            .map_err(SagaStateStoreError::Journal)?;
        self.saga_dedupe()
            .prune(saga_id)
            .map_err(SagaStateStoreError::Dedupe)
    }

    fn prune_saga(&mut self, saga_id: SagaId) {
        if let Err(err) = self.prune_saga_strict(saga_id) {
            tracing::error!(
                target: "core::saga",
                event = "saga_state_prune_failed",
                saga_id = saga_id.get(),
                error = ?err
            );
        }
    }

    /// Returns the run-scoped dedupe key: the run identity (start time and saga type)
    /// prefixed to `key`, so a later run of the same saga id never collides with an
    /// earlier run's markers.
    fn run_dedupe_key(&self, context: &SagaContext, key: &str) -> String {
        format!(
            "run\u{1f}{}\u{1f}{}\u{1f}{key}",
            context.saga_started_at_millis, context.saga_type
        )
    }

    /// Run-scoped atomic check-and-mark. A true duplicate is `Ok(Duplicate)`; storage
    /// failure is `Err` and must fail closed (it is never reported as a duplicate).
    fn check_run_dedupe_strict(
        &self,
        context: &SagaContext,
        key: &str,
    ) -> Result<RunDedupe, SagaStateStoreError> {
        let scoped = self.run_dedupe_key(context, key);
        Ok(if self.check_dedupe_strict(context.saga_id, &scoped)? {
            RunDedupe::First
        } else {
            RunDedupe::Duplicate
        })
    }

    /// Returns the durable terminal tombstone for exactly this run, if any.
    fn terminal_run_outcome_strict(
        &self,
        context: &SagaContext,
    ) -> Result<Option<ParticipantTerminalKind>, SagaStateStoreError> {
        let entries = self
            .saga_journal()
            .read(context.saga_id)
            .map_err(SagaStateStoreError::Journal)?;
        Ok(scan_runs(&entries)
            .terminal_of(context)
            .map(|(kind, _)| kind))
    }

    /// Shared durable admission. Consults the journal (never a bounded cache) for run
    /// identity `(saga_id, saga_type, saga_started_at_millis)`, surfaces read and write
    /// errors, and records the run durably before returning `Admitted`.
    ///
    /// Rejects terminal replays, stale runs, unresolved-run replacement, quarantine
    /// reuse, and legacy execution history whose run identity is unknown. A new
    /// ordinary run is admitted only after earlier identified runs have resolved.
    fn admit_participant_event_strict(
        &self,
        context: &SagaContext,
    ) -> Result<ParticipantAdmission, SagaStateStoreError> {
        let entries = self
            .saga_journal()
            .read(context.saga_id)
            .map_err(SagaStateStoreError::Journal)?;
        let runs = scan_runs(&entries);
        if runs.runs.is_empty()
            && entries.iter().any(|entry| {
                !matches!(
                    entry.event,
                    ParticipantEvent::SagaRegistered { .. }
                        | ParticipantEvent::ParticipantDependencyCompletedRecorded { .. }
                )
            })
        {
            return Ok(ParticipantAdmission::LegacyHistory);
        }
        if let Some((outcome, _)) = runs.terminal_of(context) {
            return Ok(ParticipantAdmission::TerminalReplay { outcome });
        }
        if let Some(latest) = runs.latest_started_at()
            && context.saga_started_at_millis < latest
        {
            return Ok(ParticipantAdmission::StaleRun {
                latest_started_at_millis: latest,
            });
        }
        if let Some((started_at, reason)) = runs.runs.iter().find_map(|run| match run.terminal {
            Some((ParticipantTerminalKind::Quarantined, reason)) => Some((run.started_at, reason)),
            _ => None,
        }) {
            return Ok(ParticipantAdmission::QuarantinedReuse {
                quarantined_started_at_millis: started_at,
                reason: reason.into(),
            });
        }
        if !runs.contains(context) {
            if let Some(active) = runs.runs.iter().find(|run| run.terminal.is_none()) {
                return Ok(ParticipantAdmission::ActiveRunReuse {
                    active_started_at_millis: active.started_at,
                });
            }
            self.record_event_strict(
                context.saga_id,
                ParticipantEvent::ParticipantRunRecorded {
                    saga_type: context.saga_type.clone(),
                    saga_started_at_millis: context.saga_started_at_millis,
                    recorded_at_millis: self.now_millis(),
                },
            )?;
        }
        Ok(ParticipantAdmission::Admitted)
    }

    /// Checks the current run's durable forward intent/evidence. A changed
    /// delivery trace or cleared volatile dependency cache must not reopen it.
    fn forward_execution_recorded_strict(
        &self,
        context: &SagaContext,
    ) -> Result<bool, SagaStateStoreError> {
        let entries = self
            .saga_journal()
            .read(context.saga_id)
            .map_err(SagaStateStoreError::Journal)?;
        let start = entries.iter().rposition(|entry| matches!(&entry.event,
            ParticipantEvent::ParticipantRunRecorded { saga_type, saga_started_at_millis, .. }
                if saga_type == &context.saga_type && *saga_started_at_millis == context.saga_started_at_millis
        )).unwrap_or(0);
        Ok(entries[start..].iter().any(|entry| {
            matches!(
                entry.event,
                ParticipantEvent::StepExecutionStarted { .. }
                    | ParticipantEvent::StepExecutionCompleted { .. }
                    | ParticipantEvent::ParticipantForwardOutcomeRecorded { .. }
                    | ParticipantEvent::Quarantined { .. }
                    | ParticipantEvent::ParticipantReconciliationEvidence { .. }
            )
        }))
    }

    /// Loads only this run's evidence, separating implicit rows by run records
    /// and explicit context-bearing rows by identity. Read failures propagate.
    fn participant_run_evidence_strict(
        &self,
        context: &SagaContext,
    ) -> Result<ParticipantRunEvidence, SagaStateStoreError> {
        let entries = self
            .saga_journal()
            .read(context.saga_id)
            .map_err(SagaStateStoreError::Journal)?;
        Ok(run_evidence(&entries, context))
    }

    /// Durable proof after business/effect success and before completion publication.
    /// An append error must quarantine; callers may not publish step success.
    fn record_forward_outcome_strict(
        &self,
        outcome: &ParticipantForwardOutcome,
    ) -> Result<(), SagaStateStoreError> {
        self.record_event_strict(
            outcome.context.saga_id,
            ParticipantEvent::ParticipantForwardOutcomeRecorded {
                outcome: outcome.clone(),
            },
        )
    }

    /// Durably records, before the caller treats it as seen, that the incoming
    /// dependency step named by `context.step_name` completed for this exact run.
    /// Storage failure is `Err`; it never authorizes or implies step execution.
    fn record_dependency_completion_strict(
        &self,
        context: &SagaContext,
    ) -> Result<(), SagaStateStoreError> {
        self.record_event_strict(
            context.saga_id,
            ParticipantEvent::ParticipantDependencyCompletedRecorded {
                context: context.clone(),
                recorded_at_millis: self.now_millis(),
            },
        )
    }

    /// Dependency step names durably recorded for exactly this run
    /// (saga type, id and start time). Other runs' observations are excluded.
    fn completed_dependency_steps_strict(
        &self,
        context: &SagaContext,
    ) -> Result<HashSet<Box<str>>, SagaStateStoreError> {
        let entries = self
            .saga_journal()
            .read(context.saga_id)
            .map_err(SagaStateStoreError::Journal)?;
        Ok(entries
            .iter()
            .filter_map(|entry| match &entry.event {
                ParticipantEvent::ParticipantDependencyCompletedRecorded {
                    context: seen, ..
                } if seen.saga_id == context.saga_id
                    && seen.saga_type == context.saga_type
                    && seen.saga_started_at_millis == context.saga_started_at_millis =>
                {
                    Some(seen.step_name.clone())
                }
                _ => None,
            })
            .collect())
    }

    /// Retains typed reconciliation data after a result-write failure. This does
    /// not claim success or authorize automatic execution/undo on recovery.
    fn retain_reconciliation_evidence_strict(
        &self,
        context: &SagaContext,
        output: &[u8],
        compensation_data: &[u8],
        reason: &str,
    ) -> Result<(), SagaStateStoreError> {
        self.record_event_strict(
            context.saga_id,
            ParticipantEvent::ParticipantReconciliationEvidence {
                context: context.clone(),
                output: output.to_vec(),
                compensation_data: compensation_data.to_vec(),
                reason: reason.into(),
                recorded_at_millis: self.now_millis(),
            },
        )
    }

    /// Durably retains a terminal tombstone for this run (no pruning of journal,
    /// dedupe or accepted metadata), then latches the in-memory cache. The first
    /// ordinary outcome is absorbing, but late effect evidence may escalate it to
    /// quarantine. Quarantine never downgrades. The latch requires a durable write.
    fn retain_terminal_saga_strict(
        &mut self,
        context: &SagaContext,
        outcome: ParticipantTerminalKind,
        reason: &str,
    ) -> Result<(), SagaStateStoreError> {
        let entries = self
            .saga_journal()
            .read(context.saga_id)
            .map_err(SagaStateStoreError::Journal)?;
        let previous = scan_runs(&entries)
            .terminal_of(context)
            .map(|(kind, _)| kind);
        let unsafe_failure = outcome == ParticipantTerminalKind::Failed
            && run_evidence(&entries, context).failure_requires_quarantine();
        let outcome = if unsafe_failure {
            ParticipantTerminalKind::Quarantined
        } else {
            outcome
        };
        let escalated_reason = unsafe_failure.then(|| {
            format!("{reason}; unresolved participant intent/effect/undo requires reconciliation")
        });
        let reason = escalated_reason.as_deref().unwrap_or(reason);
        if previous.is_none()
            || (outcome == ParticipantTerminalKind::Quarantined
                && previous != Some(ParticipantTerminalKind::Quarantined))
        {
            self.record_event_strict(
                context.saga_id,
                ParticipantEvent::ParticipantTerminalRecorded {
                    saga_type: context.saga_type.clone(),
                    saga_started_at_millis: context.saga_started_at_millis,
                    outcome,
                    reason: reason.into(),
                    recorded_at_millis: self.now_millis(),
                },
            )?;
        }
        self.record_saga_run_start(context.saga_id, context.saga_started_at_millis);
        self.latch_terminal_saga(context.saga_id);
        Ok(())
    }

    /// Retains the tombstone for a terminal choreography event. Returns `Ok(false)`
    /// for non-terminal events.
    fn retain_terminal_event_strict(
        &mut self,
        event: &SagaChoreographyEvent,
    ) -> Result<bool, SagaStateStoreError> {
        let (outcome, reason): (_, &str) = match event {
            SagaChoreographyEvent::SagaCompleted { .. } => {
                (ParticipantTerminalKind::Completed, "saga completed")
            }
            SagaChoreographyEvent::SagaFailed { reason, .. } => {
                (ParticipantTerminalKind::Failed, reason)
            }
            SagaChoreographyEvent::SagaQuarantined { reason, .. } => {
                (ParticipantTerminalKind::Quarantined, reason)
            }
            _ => return Ok(false),
        };
        self.retain_terminal_saga_strict(event.context(), outcome, reason)?;
        Ok(true)
    }

    /// Checks whether a saga is still actively running.
    ///
    /// Returns `true` if the saga exists and has not reached a terminal state,
    /// `false` otherwise (including if the saga does not exist).
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the saga to check
    ///
    /// # Returns
    ///
    /// `true` if the saga is active, `false` if completed, failed, or not found.
    fn is_saga_active(&self, saga_id: SagaId) -> bool {
        match self.saga_states_ref().get(&saga_id) {
            Some(entry) => !entry.is_terminal(),
            None => false,
        }
    }

    /// Returns a list of all active saga identifiers.
    ///
    /// Collects and returns the IDs of all sagas that have not yet reached
    /// a terminal state. Useful for monitoring and cleanup operations.
    ///
    /// # Returns
    ///
    /// A vector containing the IDs of all active sagas.
    fn active_saga_ids(&self) -> Vec<SagaId> {
        self.saga_states_ref()
            .iter()
            .filter(|(_, entry)| !entry.is_terminal())
            .map(|(id, _)| *id)
            .collect()
    }

    /// Returns the count of currently active sagas.
    ///
    /// This is a convenience method that counts sagas that have not yet
    /// reached a terminal state.
    ///
    /// # Returns
    ///
    /// The number of active (non-terminal) sagas.
    fn active_saga_count(&self) -> usize {
        self.saga_states_ref()
            .values()
            .filter(|e| !e.is_terminal())
            .count()
    }
}

fn run_evidence(entries: &[crate::JournalEntry], context: &SagaContext) -> ParticipantRunEvidence {
    let mut evidence = ParticipantRunEvidence::default();
    let legacy = !entries
        .iter()
        .any(|entry| matches!(entry.event, ParticipantEvent::ParticipantRunRecorded { .. }));
    let mut active: Option<(&str, u64)> = None;
    let mut undo_seen = false;
    for entry in entries {
        if let ParticipantEvent::ParticipantRunRecorded {
            saga_type,
            saga_started_at_millis,
            ..
        } = &entry.event
        {
            active = Some((saga_type, *saga_started_at_millis));
        }
        let same = |ctx: &SagaContext| {
            ctx.saga_id == context.saga_id
                && ctx.saga_type == context.saga_type
                && ctx.saga_started_at_millis == context.saga_started_at_millis
        };
        let belongs = match &entry.event {
            ParticipantEvent::AcceptedStepRecorded { context, .. }
            | ParticipantEvent::AcceptedCompensationRecorded { context, .. }
            | ParticipantEvent::CompensationRequestRecorded { context, .. }
            | ParticipantEvent::ParticipantReconciliationEvidence { context, .. }
            | ParticipantEvent::ParticipantDependencyCompletedRecorded { context, .. } => {
                same(context)
            }
            ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome } => {
                same(&outcome.context)
            }
            ParticipantEvent::ParticipantTerminalRecorded {
                saga_type,
                saga_started_at_millis,
                ..
            } => {
                saga_type == &context.saga_type
                    && *saga_started_at_millis == context.saga_started_at_millis
            }
            _ => {
                legacy
                    || active == Some((context.saga_type.as_ref(), context.saga_started_at_millis))
            }
        };
        if !belongs {
            continue;
        }
        evidence.last_updated_at_millis = evidence
            .last_updated_at_millis
            .max(entry.recorded_at_millis);
        match &entry.event {
            ParticipantEvent::StepExecutionStarted { .. } => {
                evidence.forward_intent_open = true;
            }
            ParticipantEvent::AcceptedStepRecorded {
                compensation_data, ..
            } => {
                evidence.accepted_forward_pending = true;
                evidence.compensation_data = compensation_data.clone();
            }
            ParticipantEvent::CompensationRequestRecorded { .. } => {
                evidence.undo_required = true;
            }
            ParticipantEvent::StepExecutionCompleted {
                output,
                compensation_data,
                ..
            } => {
                evidence.forward_intent_open = false;
                evidence.accepted_forward_pending = false;
                evidence.forward_result_recorded = true;
                evidence.forward_outcome = None;
                evidence.output = output.clone();
                evidence.compensation_data = compensation_data.clone();
                if undo_seen {
                    evidence.needs_reconciliation = true;
                }
                evidence.undo_completed = false;
            }
            ParticipantEvent::ParticipantForwardOutcomeRecorded { outcome } => {
                evidence.forward_intent_open = false;
                evidence.accepted_forward_pending = false;
                evidence.forward_result_recorded = true;
                evidence.output = outcome.output.clone();
                evidence.compensation_data = outcome.compensation_data.clone();
                evidence.forward_outcome = Some(outcome.clone());
                if undo_seen {
                    evidence.needs_reconciliation = true;
                }
            }
            ParticipantEvent::StepExecutionFailed {
                requires_compensation,
                ..
            } => {
                evidence.forward_intent_open = false;
                // A durable authoritative failure without compensation is the
                // definitive rejection of accepted forward work: no effect
                // materialised. Compensation bytes stored at acceptance are only
                // potential metadata, so they stop being an obligation unless a
                // real result/outcome or reconciliation evidence exists. A failure
                // that requires compensation keeps the obligation until undo.
                if *requires_compensation && evidence.accepted_forward_pending {
                    evidence.undo_required = true;
                }
                if !*requires_compensation {
                    evidence.accepted_forward_pending = false;
                    if !evidence.forward_result_recorded
                        && !evidence.needs_reconciliation
                        && !evidence.undo_required
                    {
                        evidence.compensation_data.clear();
                    }
                }
            }
            ParticipantEvent::CompensationStarted { .. }
            | ParticipantEvent::AcceptedCompensationRecorded { .. } => {
                undo_seen = true;
                evidence.undo_required = true;
                evidence.undo_intent_open = true;
            }
            ParticipantEvent::CompensationCompleted { .. } => {
                undo_seen = true;
                evidence.undo_required = true;
                evidence.undo_intent_open = false;
                evidence.undo_completed = true;
                evidence.accepted_forward_pending = false;
                evidence.forward_intent_open = false;
            }
            ParticipantEvent::CompensationFailed { .. } => {
                undo_seen = true;
                evidence.undo_required = true;
                evidence.undo_intent_open = true;
                evidence.needs_reconciliation = true;
            }
            ParticipantEvent::ParticipantReconciliationEvidence {
                output,
                compensation_data,
                ..
            } => {
                evidence.needs_reconciliation = true;
                evidence.output = output.clone();
                evidence.compensation_data = compensation_data.clone();
            }
            ParticipantEvent::Quarantined { .. }
            | ParticipantEvent::ParticipantTerminalRecorded {
                outcome: ParticipantTerminalKind::Quarantined,
                ..
            } => {
                evidence.quarantined = true;
            }
            _ => {}
        }
    }
    evidence
}

/// Durable run history of one saga id, derived from journal run/terminal records.
struct RunScan<'a> {
    runs: Vec<RunRecord<'a>>,
}

struct RunRecord<'a> {
    saga_type: &'a str,
    started_at: u64,
    terminal: Option<(ParticipantTerminalKind, &'a str)>,
}

fn scan_runs(entries: &[crate::JournalEntry]) -> RunScan<'_> {
    let mut runs: Vec<RunRecord<'_>> = Vec::new();
    for entry in entries {
        match &entry.event {
            ParticipantEvent::ParticipantRunRecorded {
                saga_type,
                saga_started_at_millis,
                ..
            } => {
                if !runs
                    .iter()
                    .any(|r| r.saga_type == &**saga_type && r.started_at == *saga_started_at_millis)
                {
                    runs.push(RunRecord {
                        saga_type,
                        started_at: *saga_started_at_millis,
                        terminal: None,
                    });
                }
            }
            ParticipantEvent::ParticipantTerminalRecorded {
                saga_type,
                saga_started_at_millis,
                outcome,
                reason,
                ..
            } => {
                match runs.iter_mut().find(|r| {
                    r.saga_type == &**saga_type && r.started_at == *saga_started_at_millis
                }) {
                    Some(run) => {
                        if *outcome == ParticipantTerminalKind::Quarantined {
                            run.terminal = Some((*outcome, reason));
                        } else {
                            run.terminal.get_or_insert((*outcome, reason));
                        }
                    }
                    None => runs.push(RunRecord {
                        saga_type,
                        started_at: *saga_started_at_millis,
                        terminal: Some((*outcome, reason)),
                    }),
                }
            }
            _ => {}
        }
    }
    RunScan { runs }
}

impl<'a> RunScan<'a> {
    fn find(&self, context: &SagaContext) -> Option<&RunRecord<'a>> {
        self.runs.iter().find(|r| {
            r.saga_type == &*context.saga_type && r.started_at == context.saga_started_at_millis
        })
    }

    fn contains(&self, context: &SagaContext) -> bool {
        self.find(context).is_some()
    }

    fn terminal_of(&self, context: &SagaContext) -> Option<(ParticipantTerminalKind, &'a str)> {
        self.find(context).and_then(|r| r.terminal)
    }

    fn latest_started_at(&self) -> Option<u64> {
        self.runs.iter().map(|r| r.started_at).max()
    }
}

impl<T> SagaStateExt for T where T: HasSagaParticipantSupport {}

#[cfg(test)]
mod tests {
    use crate::{
        HasSagaParticipantSupport, InMemoryDedupe, InMemoryJournal, ParticipantEvent,
        ParticipantJournal, SagaId, SagaParticipantSupport,
    };

    use super::SagaStateExt;

    struct DummyParticipant {
        saga: SagaParticipantSupport<InMemoryJournal, InMemoryDedupe>,
    }

    impl DummyParticipant {
        fn new() -> Self {
            Self {
                saga: SagaParticipantSupport::new(InMemoryJournal::new(), InMemoryDedupe::new()),
            }
        }
    }

    impl HasSagaParticipantSupport for DummyParticipant {
        type Journal = InMemoryJournal;
        type Dedupe = InMemoryDedupe;

        fn saga_support(&self) -> &crate::SagaParticipantSupport<Self::Journal, Self::Dedupe> {
            &self.saga
        }

        fn saga_support_mut(
            &mut self,
        ) -> &mut crate::SagaParticipantSupport<Self::Journal, Self::Dedupe> {
            &mut self.saga
        }
    }

    #[test]
    fn blanket_impl_routes_state_and_storage_through_embedded_support() {
        let participant = DummyParticipant::new();
        let saga_id = SagaId::new(42);

        participant.record_event(
            saga_id,
            ParticipantEvent::StepTriggered {
                triggering_event: "saga_started".into(),
                triggered_at_millis: 10,
            },
        );
        assert_eq!(
            participant
                .saga_journal()
                .read(saga_id)
                .expect("journal should read")
                .len(),
            1
        );

        assert!(participant.check_dedupe(saga_id, "step_started"));
        assert!(!participant.check_dedupe(saga_id, "step_started"));
        assert_eq!(participant.active_saga_count(), 0);
    }
}
