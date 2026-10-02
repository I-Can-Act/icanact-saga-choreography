//! Extension trait for saga state management.
//!
//! This module provides the [`SagaStateExt`] trait, which supplies common
//! state-management methods for saga participants.
//!
//! Preferred integration: embed [`crate::SagaParticipantSupport`] in your actor
//! and implement [`crate::HasSagaParticipantSupport`]. This crate will then
//! provide `SagaStateExt` automatically.

use crate::{
    CommitStage, IngressFailure, IngressOutcome, IngressRejection, ReconciliationCause,
    ReconciliationNeeded,
};
use crate::{
    DedupeError, HasSagaParticipantSupport, InboxState, InboxTxn, JournalError, KnownRuns,
    ParticipantDedupeStore, ParticipantEvent, ParticipantJournal, RunAdmission, RunIdentityError,
    RunIncarnation, RunKey, RunStatus, RunTerminalOutcome, RunTombstone, SagaChoreographyEvent,
    SagaContext, SagaId, SagaParticipantState, SagaStateEntry, admit_run,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::Ordering;

#[derive(Debug, thiserror::Error)]
pub enum SagaStateStoreError {
    #[error("saga dedupe store: {0}")]
    Dedupe(DedupeError),
    #[error("saga journal: {0}")]
    Journal(JournalError),
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
    /// Returns mutable access to the per-run state map.
    ///
    /// This provides direct access to the underlying storage for run state entries,
    /// allowing modifications such as inserting new runs or updating existing ones.
    fn saga_states(&mut self) -> &mut HashMap<RunKey, SagaStateEntry> {
        &mut self.saga_support_mut().saga_states
    }

    /// Returns immutable access to the per-run state map.
    ///
    /// Use this for read-only operations that need to inspect run state
    /// without modifying it.
    fn saga_states_ref(&self) -> &HashMap<RunKey, SagaStateEntry> {
        &self.saga_support().saga_states
    }

    /// Returns mutable access to per-run dependency completion tracking.
    fn dependency_completions(&mut self) -> &mut HashMap<RunKey, HashSet<Box<str>>> {
        &mut self.saga_support_mut().dependency_completions
    }

    /// Returns mutable access to per-run dependency fire tracking.
    fn dependency_fired(&mut self) -> &mut HashSet<RunKey> {
        &mut self.saga_support_mut().dependency_fired
    }

    /// Clears all in-memory tracking of exactly one run; other runs of the same saga id
    /// are untouched (ADR-0001, Q4).
    fn clear_in_memory_saga_run_tracking(&mut self, run: &RunKey) {
        let support = self.saga_support_mut();
        support.saga_states.remove(run);
        support.dependency_completions.remove(run);
        support.dependency_fired.remove(run);
        support.accepted_workflow_steps.remove(run);
        support.accepted_workflow_compensations.remove(run);
        support
            .resolved_workflow_steps
            .retain(|(resolved_run, _)| resolved_run != run);
        support.inbox_states.remove(run);
        support.admitted_runs.remove(run);
    }

    /// Returns mutable access to terminal run latches.
    fn terminal_sagas(&mut self) -> &mut HashSet<RunKey> {
        &mut self.saga_support_mut().terminal_sagas
    }

    fn terminal_saga_order(&mut self) -> &mut VecDeque<RunKey> {
        &mut self.saga_support_mut().terminal_saga_order
    }

    /// Returns true when this participant has already observed terminal state for the
    /// run and should ignore late replays of it.
    fn is_terminal_saga_latched(&self, run: &RunKey) -> bool {
        self.saga_support().terminal_sagas.contains(run)
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

    fn latch_terminal_saga(&mut self, run: &RunKey) {
        let inserted = self.terminal_sagas().insert(run.clone());
        if !inserted {
            return;
        }
        self.terminal_saga_order().push_back(run.clone());
        let cap = self.terminal_latch_retention_limit();
        while self.terminal_saga_order().len() > cap {
            let Some(evicted) = self.terminal_saga_order().pop_front() else {
                break;
            };
            self.terminal_sagas().remove(&evicted);
        }
    }

    fn unlatch_terminal_saga(&mut self, run: &RunKey) {
        self.terminal_sagas().remove(run);
        self.terminal_saga_order().retain(|entry| entry != run);
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

    /// Maps a dedupe store error to `false` ("duplicate"), which silently drops work.
    #[deprecated(
        note = "maps dedupe errors to 'duplicate'; use `check_dedupe_strict` (or `check_dedupe_run_strict`) and handle the error"
    )]
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
    /// Appends the given event to the durable journal for the specified saga.
    /// Errors during journaling are silently ignored; use this for best-effort
    /// event recording where durability is desired but not strictly required.
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

    /// Correctness-relevant journal write; mutate memory only after `Ok` (ADR-0002).
    fn commit_transition(
        &self,
        run: &RunKey,
        event: ParticipantEvent,
    ) -> Result<(), SagaStateStoreError> {
        self.saga_journal()
            .append_run(run, event)
            .map(|_| ())
            .map_err(SagaStateStoreError::Journal)
    }

    /// `commit_transition` plus the outbound events it obliges, in one row (ADR-0003).
    fn commit_transition_with_outbox(
        &self,
        run: &RunKey,
        event: ParticipantEvent,
        outbox: Vec<SagaChoreographyEvent>,
    ) -> Result<(), SagaStateStoreError> {
        self.saga_journal()
            .commit_with_outbox(run, event, outbox)
            .map(|_| ())
            .map_err(SagaStateStoreError::Journal)
    }

    /// Atomic inbox commit (ADR-0005).
    fn commit_inbox(&self, run: &RunKey, txn: InboxTxn) -> Result<(), SagaStateStoreError> {
        self.saga_journal()
            .commit_inbox(run, txn)
            .map(|_| ())
            .map_err(SagaStateStoreError::Journal)
    }

    /// Run-scoped strict dedupe (ADR-0001).
    fn check_dedupe_run_strict(
        &self,
        run: &RunKey,
        key: &str,
    ) -> Result<bool, SagaStateStoreError> {
        self.saga_dedupe()
            .check_and_mark_run(run, key)
            .map_err(SagaStateStoreError::Dedupe)
    }

    /// Finalize a terminal run (ADR-0001 §2.5): journal `finalize_run` (tombstone + delete the
    /// run's rows + expire old tombstones), then, only on `Ok`, clear the run's memory, then
    /// prune the run's dedupe marks. A journal failure leaves memory untouched.
    fn finalize_run_strict(
        &mut self,
        tombstone: &RunTombstone,
        cutoff: RunIncarnation,
    ) -> Result<(), SagaStateStoreError> {
        let run = tombstone.run();
        self.saga_journal()
            .finalize_run(tombstone, cutoff)
            .map_err(SagaStateStoreError::Journal)?;
        self.clear_in_memory_saga_run_tracking(run);
        self.saga_dedupe()
            .prune_run(run)
            .map_err(SagaStateStoreError::Dedupe)
    }

    /// Run-scoped journal append for events that must not be lost silently (ADR-0001).
    fn record_event_run_strict(
        &self,
        run: &RunKey,
        event: ParticipantEvent,
    ) -> Result<(), SagaStateStoreError> {
        self.saga_journal()
            .append_run(run, event)
            .map(|_| ())
            .map_err(SagaStateStoreError::Journal)
    }

    /// Best-effort run-scoped append; failures are logged with the run key.
    fn record_event_run(&self, run: &RunKey, event: ParticipantEvent) {
        if let Err(err) = self.record_event_run_strict(run, event) {
            tracing::error!(
                target: "core::saga",
                event = "saga_state_journal_append_failed",
                run = %run,
                error = ?err
            );
        }
    }

    /// Replay cutoff for this participant now (ADR-0001).
    fn replay_cutoff(&self) -> RunIncarnation {
        self.saga_support().replay_horizon.cutoff(self.now_millis())
    }

    /// Journal-derived inbox state of a run, if loaded (ADR-0005).
    fn inbox_state(&self, run: &RunKey) -> Option<&InboxState> {
        self.saga_support().inbox_states.get(run)
    }

    /// Mutable inbox state of a run, created empty if absent (ADR-0005).
    fn inbox_state_mut(&mut self, run: &RunKey) -> &mut InboxState {
        self.saga_support_mut()
            .inbox_states
            .entry(run.clone())
            .or_default()
    }

    /// Removes all state of every run of a saga id: journal rows (all runs and the legacy
    /// partition), dedupe marks, then in-memory tracking. Memory is cleared only after the
    /// stores succeed. Terminal run handling uses [`SagaStateExt::finalize_run_strict`] instead.
    ///
    /// # Arguments
    ///
    /// * `saga_id` - The unique identifier of the saga to prune
    fn prune_saga_strict(&mut self, saga_id: SagaId) -> Result<(), SagaStateStoreError> {
        self.saga_journal()
            .prune(saga_id)
            .map_err(SagaStateStoreError::Journal)?;
        self.saga_dedupe()
            .prune(saga_id)
            .map_err(SagaStateStoreError::Dedupe)?;
        let runs: Vec<RunKey> = self
            .saga_support()
            .admitted_runs
            .iter()
            .chain(self.saga_support().saga_states.keys())
            .filter(|run| run.saga_id() == saga_id)
            .cloned()
            .collect();
        for run in runs {
            self.clear_in_memory_saga_run_tracking(&run);
        }
        Ok(())
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

    /// Checks whether any run of a saga id is still actively running.
    ///
    /// Returns `true` if a run exists and has not reached a terminal state,
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
        self.saga_states_ref()
            .iter()
            .any(|(run, entry)| run.saga_id() == saga_id && !entry.is_terminal())
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
        let mut ids: Vec<SagaId> = self
            .saga_states_ref()
            .iter()
            .filter(|(_, entry)| !entry.is_terminal())
            .map(|(run, _)| run.saga_id())
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
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

/// Participant-side admission decision for one inbound event (ADR-0001 §2.2).
pub(crate) enum EventAdmission {
    /// Process the event (new run, or the event belongs to the active run).
    Proceed,
    /// Duplicate start, terminal run, or a rejected incarnation; already logged and counted.
    Skip(IngressOutcome),
    /// The run status or tombstone lookup failed; the caller must quarantine (ADR-0001 §2.7).
    LookupFailed(SagaStateStoreError),
}

/// Status of the exact run plus what is known about other runs of the same saga id.
fn run_status<A: SagaStateExt + ?Sized>(
    actor: &A,
    run: &RunKey,
) -> Result<(RunStatus, KnownRuns), SagaStateStoreError> {
    let support = actor.saga_support();
    if support.admitted_runs.contains(run) {
        return Ok((RunStatus::Active, KnownRuns::default()));
    }
    if support.terminal_sagas.contains(run) {
        return Ok((RunStatus::Terminal, KnownRuns::default()));
    }
    // Q6: a participant-local quarantine fences its run even after the bounded terminal latch
    // evicted it; only an operator release (not yet available) may lift it.
    if matches!(
        support.saga_states.get(run),
        Some(SagaStateEntry::Quarantined(_))
    ) {
        return Ok((RunStatus::Terminal, KnownRuns::default()));
    }
    let tombstones = support
        .journal
        .run_tombstones(run.saga_type(), run.saga_id())
        .map_err(SagaStateStoreError::Journal)?;
    if tombstones.iter().any(|tombstone| tombstone.run() == run) {
        return Ok((RunStatus::Terminal, KnownRuns::default()));
    }
    let lowest = RunKey::new(run.saga_type(), run.saga_id(), RunIncarnation::new(0));
    let highest = RunKey::new(
        run.saga_type(),
        run.saga_id(),
        RunIncarnation::new(u64::MAX),
    );
    let mut newest = tombstones
        .iter()
        .map(|tombstone| tombstone.run().incarnation())
        .max();
    let mut other_active = 0;
    for other in support.admitted_runs.range(lowest..=highest) {
        other_active += 1;
        newest = newest.max(Some(other.incarnation()));
    }
    Ok((
        RunStatus::Unknown,
        KnownRuns {
            newest,
            other_active,
        },
    ))
}

/// Run-identity admission for an inbound event; mutates only the in-memory admission set.
pub(crate) fn admit_event<A: SagaStateExt + ?Sized>(
    actor: &mut A,
    event: &SagaChoreographyEvent,
) -> EventAdmission {
    let run = event.context().run_key();
    let is_start = matches!(event, SagaChoreographyEvent::SagaStarted { .. });
    let (exact, known) = match run_status(actor, &run) {
        Ok(status) => status,
        Err(err) => {
            tracing::error!(
                target: "core::saga",
                event = "saga_admission_lookup_failed",
                run = %run,
                error = ?err
            );
            return EventAdmission::LookupFailed(err);
        }
    };
    match admit_run(exact, known, &run, is_start, actor.replay_cutoff()) {
        Ok(RunAdmission::NewRun { concurrent }) => {
            if concurrent {
                tracing::warn!(
                    target: "core::saga",
                    event = "saga_concurrent_run_admitted",
                    run = %run,
                    other_active = known.other_active
                );
                actor
                    .saga_support()
                    .stats
                    .concurrent_runs_admitted
                    .fetch_add(1, Ordering::Relaxed);
            }
            actor.saga_support_mut().admitted_runs.insert(run);
            EventAdmission::Proceed
        }
        Ok(RunAdmission::CurrentRun) => EventAdmission::Proceed,
        Ok(admission @ (RunAdmission::DuplicateStart | RunAdmission::TerminalRun)) => {
            if matches!(admission, RunAdmission::TerminalRun)
                && matches!(
                    actor.saga_states().get(&run),
                    Some(SagaStateEntry::Quarantined(_))
                )
            {
                // Q6: a quarantined run runs no business effects automatically; the event is
                // rejected loudly and the evidence stays for reconciliation.
                tracing::error!(
                    target: "core::saga",
                    event = "saga_quarantined_run_event_rejected",
                    run = %run,
                    event_type = event.event_type()
                );
            } else {
                tracing::debug!(
                    target: "core::saga",
                    event = "saga_known_run_event_ignored",
                    run = %run,
                    event_type = event.event_type()
                );
            }
            actor
                .saga_support()
                .stats
                .duplicate_events
                .fetch_add(1, Ordering::Relaxed);
            EventAdmission::Skip(match admission {
                RunAdmission::DuplicateStart => IngressOutcome::Duplicate,
                _ => IngressOutcome::Rejected(IngressRejection::TerminalRun),
            })
        }
        Err(err) => {
            match &err {
                RunIdentityError::StaleIncarnation { newest_known, .. } => {
                    tracing::warn!(
                        target: "core::saga",
                        event = "saga_stale_incarnation_rejected",
                        run = %run,
                        newest_known = %newest_known
                    );
                    actor
                        .saga_support()
                        .stats
                        .runs_rejected_stale
                        .fetch_add(1, Ordering::Relaxed);
                }
                RunIdentityError::ExpiredIncarnation { cutoff, .. } => {
                    tracing::warn!(
                        target: "core::saga",
                        event = "saga_expired_incarnation_rejected",
                        run = %run,
                        cutoff = %cutoff
                    );
                    actor
                        .saga_support()
                        .stats
                        .runs_rejected_expired
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            EventAdmission::Skip(IngressOutcome::Rejected(IngressRejection::RunIdentity(err)))
        }
    }
}

/// Quarantines a run in memory after a pre-effect lookup failure without destroying evidence
/// (ADR-0002 §2.2), so memory matches the journal's `Quarantined` row after a restart. A missing
/// run gets a fresh `Quarantined` entry (`prior_state` `"none"`); `Executing`, `Compensating`,
/// `Completed` (keeping its compensation data) and `Failed` move through their typestate
/// transition. An already `Quarantined` entry keeps its original reason and evidence; a
/// `Compensated` run is finished and left as is (the only case returning `false`). `Idle` and
/// `Triggered` entries (never executed) are quarantined like a missing run. Returns whether the run
/// is now `Quarantined`; if so it is also un-admitted and latched terminal (identically for both
/// engines and the workflow adapter).
pub(crate) fn quarantine_preserving_state<A: SagaStateExt + ?Sized>(
    actor: &mut A,
    context: &SagaContext,
    step: &str,
    trigger: &str,
    reason: &str,
    now: u64,
) -> bool {
    let run = context.run_key();
    let reason: Box<str> = reason.into();
    let entry = match actor.saga_states().remove(&run) {
        None => SagaStateEntry::Quarantined({
            let mut fresh = SagaParticipantState::new(
                context.saga_id,
                context.saga_type.clone(),
                step.into(),
                context.correlation_id,
                context.trace_id,
                context.initiator_peer_id,
                context.saga_started_at_millis,
            )
            .trigger(trigger, now)
            .start_execution(now)
            .quarantine(reason, now);
            fresh.state.prior_state = "none";
            fresh
        }),
        Some(SagaStateEntry::Executing(state)) => {
            SagaStateEntry::Quarantined(state.quarantine(reason, now))
        }
        Some(SagaStateEntry::Compensating(state)) => {
            SagaStateEntry::Quarantined(state.quarantine(reason, now))
        }
        Some(SagaStateEntry::Completed(state)) => {
            SagaStateEntry::Quarantined(state.quarantine(reason, now))
        }
        Some(SagaStateEntry::Failed(state)) => {
            SagaStateEntry::Quarantined(state.quarantine(reason, now))
        }
        // Never executed: no effect and no undo data, so the same fresh-style transition applies.
        Some(SagaStateEntry::Idle(state)) => SagaStateEntry::Quarantined(
            state
                .trigger(trigger, now)
                .start_execution(now)
                .quarantine(reason, now),
        ),
        Some(SagaStateEntry::Triggered(state)) => {
            SagaStateEntry::Quarantined(state.start_execution(now).quarantine(reason, now))
        }
        Some(other) => other,
    };
    let quarantined = matches!(entry, SagaStateEntry::Quarantined(_));
    actor.saga_states().insert(run.clone(), entry);
    if quarantined {
        un_admit_and_latch_quarantined(actor, &run);
    }
    quarantined
}

/// Keeps undo data that was handed to an accepted step on the run's in-memory `Quarantined` entry
/// (`Executing::quarantine` has none): the evidence stays available for reconciliation. An empty
/// slice, or a run that is not `Quarantined`, changes nothing.
pub(crate) fn keep_quarantine_compensation_data<A: SagaStateExt + ?Sized>(
    actor: &mut A,
    run: &RunKey,
    compensation_data: &[u8],
) {
    if compensation_data.is_empty() {
        return;
    }
    if let Some(SagaStateEntry::Quarantined(state)) = actor.saga_states().get_mut(run)
        && state.state.compensation_data.is_none()
    {
        state.state.compensation_data = Some(compensation_data.to_vec());
    }
}

/// Terminal latch for a run whose memory state just became `Quarantined` (checked after
/// `admitted_runs`, so un-admit): later events of this run are rejected as `TerminalRun` instead of
/// running effects or finalizing the evidence away (owner decision Q6). Every quarantine that does
/// not go through [`quarantine_preserving_state`] must call this.
pub(crate) fn un_admit_and_latch_quarantined<A: SagaStateExt + ?Sized>(
    actor: &mut A,
    run: &RunKey,
) {
    actor.saga_support_mut().admitted_runs.remove(run);
    actor.latch_terminal_saga(run);
}

/// The single post-effect (or lookup-failure) quarantine path shared by the generic helpers and
/// the workflow adapter (ADR-0002 §2.2, W3 review HIGH 4): moves the state through
/// [`quarantine_preserving_state`] (keeps undo data, un-admits, latches terminal), writes the
/// best-effort `Quarantined` evidence row and emits `SagaQuarantined`.
///
/// A `Compensated` run cannot be quarantined (it is finished): the refusal is logged with the run
/// key and neither the row nor the event is produced, so memory, journal and the bus never disagree.
/// Returns whether the run is now `Quarantined`.
pub(crate) fn quarantine_run_with_evidence<A, F>(
    actor: &mut A,
    context: &SagaContext,
    (step, participant_id): (Box<str>, Box<str>),
    (trigger, reason): (&str, Box<str>),
    now: u64,
    emit: &mut F,
) -> bool
where
    A: SagaStateExt + ?Sized,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    if !quarantine_preserving_state(actor, context, &step, trigger, &reason, now) {
        tracing::error!(
            target: "core::saga",
            event = "saga_quarantine_refused",
            run = %run,
            step = %step,
            reason = %reason
        );
        return false;
    }
    // Best-effort evidence only: the journal may be the thing that just failed.
    actor.record_event_run(
        &run,
        ParticipantEvent::Quarantined {
            reason: reason.clone(),
            quarantined_at_millis: now,
        },
    );
    emit(SagaChoreographyEvent::SagaQuarantined {
        context: context.next_step(step.clone()),
        reason,
        step,
        participant_id,
    });
    true
}

/// R02 (ADR-0002, owner decision NO SILENT FAILURES): a `CompensationRequested` names this
/// participant's step but the run holds no in-memory state, so the undo ownership is unknown.
///
/// Unless the journal shows the step was already settled (its undo completed or its forward
/// execution failed, so there is nothing to undo), the run is quarantined through the single
/// quarantine path and `ReconciliationNeeded { MissingCompletedState }` is returned. A journal read
/// error is logged (`error!` with the RunKey) and counts as "not settled".
///
/// Multi-instance deployments (several instances serving one step name for a saga type) must tag
/// steps per instance: a request addressed to a step this instance never executed is quarantined,
/// not ignored.
pub(crate) fn missing_completed_state_outcome<A, F>(
    actor: &mut A,
    context: &SagaContext,
    (step, participant_id): (Box<str>, Box<str>),
    now: u64,
    emit: &mut F,
) -> IngressOutcome
where
    A: SagaStateExt + ?Sized,
    F: FnMut(SagaChoreographyEvent),
{
    let run = context.run_key();
    let settled = match actor.saga_journal().read_run(&run) {
        Ok(rows) => rows.iter().any(|row| {
            matches!(
                row.event.transition(),
                ParticipantEvent::CompensationCompleted { .. }
                    | ParticipantEvent::StepExecutionFailed { .. }
            )
        }),
        Err(error) => {
            // W4-review R4: fail closed. A journal we cannot read proves nothing was settled.
            tracing::error!(
                target: "core::saga",
                event = "saga_compensation_settled_check_read_failed",
                run = %run,
                step = %step,
                error = %error,
                "cannot read the journal to confirm the step was settled; treating as not settled"
            );
            false
        }
    };
    if settled {
        tracing::debug!(
            target: "core::saga",
            event = "saga_compensation_request_already_settled",
            run = %run,
            step = %step
        );
        return IngressOutcome::Applied;
    }
    tracing::error!(
        target: "core::saga",
        event = "saga_compensation_missing_completed_state",
        run = %run,
        step = %step,
        "compensation required but this participant holds no completed state"
    );
    quarantine_run_with_evidence(
        actor,
        context,
        (step.clone(), participant_id),
        (
            "compensation_missing_completed_state",
            "reconciliation_needed: compensation requested without completed state".into(),
        ),
        now,
        emit,
    );
    IngressOutcome::ReconciliationNeeded(ReconciliationNeeded {
        run,
        step,
        cause: ReconciliationCause::MissingCompletedState,
        compensation_data: Vec::new(),
    })
}

/// Finalizes a `Completed`/`Failed` run after its terminal event (ADR-0001 §2.5, §2.7).
///
/// On a journal failure the run's memory is kept, the failure is logged with the run key, and the
/// terminal event's dedupe mark is removed so a redelivery retries the finalize. A failure after the
/// tombstone was written (dedupe prune) is logged and returned as a typed `Finalize` failure; the
/// tombstone already answers for the run, so the run stays terminal.
pub(crate) fn finalize_terminal_run<A: SagaStateExt + ?Sized>(
    actor: &mut A,
    run: &RunKey,
    outcome: RunTerminalOutcome,
    terminal_identity: &str,
) -> Result<(), IngressFailure> {
    // A quarantined run keeps its journal rows and dedupe marks as evidence (owner decision Q6).
    if matches!(
        actor.saga_states_ref().get(run),
        Some(SagaStateEntry::Quarantined(_))
    ) {
        tracing::error!(
            target: "core::saga",
            event = "saga_finalize_refused_quarantined",
            run = %run,
            outcome = ?outcome
        );
        return Ok(());
    }
    let tombstone = RunTombstone::new(run.clone(), outcome, actor.now_millis());
    let cutoff = actor.replay_cutoff();
    let Err(err) = actor.finalize_run_strict(&tombstone, cutoff) else {
        return Ok(());
    };
    actor
        .saga_support()
        .stats
        .gc_failures
        .fetch_add(1, Ordering::Relaxed);
    tracing::error!(
        target: "core::saga",
        event = "saga_finalize_failed",
        run = %run,
        error = ?err
    );
    if matches!(err, SagaStateStoreError::Journal(_)) {
        actor.unlatch_terminal_saga(run);
        if let Err(mark_err) = actor
            .saga_dedupe()
            .remove_processed_run(run, terminal_identity)
        {
            tracing::error!(
                target: "core::saga",
                event = "saga_finalize_retry_unmark_failed",
                run = %run,
                error = %mark_err
            );
        }
    }
    Err(IngressFailure {
        run: run.clone(),
        stage: CommitStage::Finalize,
        source: err,
    })
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

        assert!(
            participant
                .check_dedupe_strict(saga_id, "step_started")
                .expect("dedupe should check")
        );
        assert!(
            !participant
                .check_dedupe_strict(saga_id, "step_started")
                .expect("dedupe should check")
        );
        assert_eq!(participant.active_saga_count(), 0);
    }
}
