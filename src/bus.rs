use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock, Weak};
use std::thread;
use std::time::Duration;

use icanact_core::CorrelationRegistry;
use icanact_core::local::{FirehosePubSub, FirehoseSubscription, PublishStats};
use icanact_core::local_sync::{self, SyncActor};

use crate::events::{SagaReplyToHandle, SagaReplyToResult};
use crate::workflow_contract::required_path_steps_from_success_criteria;
use crate::{
    SagaChoreographyEvent, SagaContext, SagaId, SagaReplyTo, SagaTerminalOutcome,
    SagaWorkflowContract, SagaWorkflowStepContract, TERMINAL_RESOLVER_STEP, TerminalPolicy,
    TerminalResolver, TerminalResolverJournal, required_steps_from_success_criteria,
    validate_workflow_contract,
};

#[derive(Clone, Debug)]
struct WorkflowContractState {
    first_step: Box<str>,
    declared_steps: HashSet<Box<str>>,
    required_path_steps: HashSet<Box<str>>,
    required_path_description: Box<str>,
    steps: &'static [SagaWorkflowStepContract],
    criteria: crate::SuccessCriteria,
}

/// An id-scoped waiter: bound to the first run admitted after it registered;
/// until then only a run that began after registration may resolve it.
#[derive(Clone, Debug)]
struct LegacyWaiter {
    registered_at_millis: u64,
    bound_run: Option<RunKey>,
}

impl LegacyWaiter {
    fn accepts(&self, context: &SagaContext) -> bool {
        match &self.bound_run {
            Some(bound) => *bound == RunKey::of(context),
            None => context.saga_started_at_millis >= self.registered_at_millis,
        }
    }
}

/// Identity of one saga run. Waiters and outcomes are scoped by it so an
/// earlier run's terminal can never satisfy a later run that reuses the id.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RunKey {
    saga_type: Box<str>,
    saga_id: SagaId,
    started_at_millis: u64,
}

impl RunKey {
    fn of(context: &SagaContext) -> Self {
        Self {
            saga_type: context.saga_type.clone(),
            saga_id: context.saga_id,
            started_at_millis: context.saga_started_at_millis,
        }
    }
}

/// Completion coordination for bus-owned resources. Counts live
/// [`ReleaseGuard`]s; it owns nothing else, so holding it (via a
/// [`SagaBusReleaseWaiter`]) can neither keep the journal alive nor form a cycle.
#[derive(Default)]
struct ReleaseLatch {
    outstanding: Mutex<usize>,
    released: Condvar,
}

impl ReleaseLatch {
    fn guard(self: &Arc<Self>) -> ReleaseGuard {
        *self
            .outstanding
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        ReleaseGuard(Arc::clone(self))
    }
}

/// Dropped together with the resource it is stored beside; the last drop wakes
/// release waiters. Drop-based, so it fires when actor *state* is destroyed,
/// not merely when a shutdown request returns.
struct ReleaseGuard(Arc<ReleaseLatch>);

impl Drop for ReleaseGuard {
    fn drop(&mut self) {
        let mut outstanding = self
            .0
            .outstanding
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *outstanding = outstanding.saturating_sub(1);
        if *outstanding == 0 {
            self.0.released.notify_all();
        }
    }
}

/// Observes actual release of a bus's actors and resources.
///
/// Obtained from [`SagaChoreographyBus::release_waiter`]. It holds only
/// completion coordination: it is not a lifecycle owner and never keeps the
/// bus, a resolver, an actor handle or a journal alive. It neither initiates
/// nor cancels shutdown and resolves no sagas.
#[derive(Clone)]
pub struct SagaBusReleaseWaiter {
    latch: Arc<ReleaseLatch>,
}

impl SagaBusReleaseWaiter {
    /// Waits up to `timeout` for the last public bus owner to be dropped *and*
    /// every bus-owned actor state (including resolver gates and the journal
    /// references they hold) to be destroyed. Returns `false` on timeout, so
    /// also while any public clone or bus-owned actor/resource remains.
    ///
    /// Blocks the calling thread: never call it from a runtime actor callback
    /// (the actors it waits for may need that worker). Journal handles cloned
    /// by the application remain the application's responsibility.
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let guard = self
            .latch
            .outstanding
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (guard, _) = self
            .latch
            .released
            .wait_timeout_while(guard, timeout, |outstanding| *outstanding != 0)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard == 0
    }
}

#[derive(Default)]
struct BusState {
    terminal_replies: HashMap<RunKey, SagaReplyTo>,
    terminal_outcomes: HashMap<RunKey, SagaTerminalOutcome>,
    terminal_order: VecDeque<RunKey>,
    /// Bounded cache tombstones survive consumption: repeated publication cannot
    /// recreate an already-taken ordinary result; quarantine may still escalate it.
    terminal_strengths: HashMap<RunKey, bool>,
    /// Newest admitted/stored run per saga id, for the saga-id compatibility API.
    latest_run_by_id: HashMap<SagaId, RunKey>,
    terminal_policies_by_saga_type: HashMap<Box<str>, Box<str>>,
    workflow_contracts_by_saga_type: HashMap<Box<str>, WorkflowContractState>,
    step_subscriptions: HashMap<FirehoseSubscription, (Box<str>, Box<str>)>,
    runtimes: HashMap<Box<str>, TerminalResolverRuntime>,
    detached: HashSet<Box<str>>,
    pending_replies: CorrelationRegistry<SagaId, SagaReplyToResult>,
    pending_run_replies: CorrelationRegistry<RunKey, SagaReplyToResult>,
    legacy_waiter_floors: HashMap<SagaId, LegacyWaiter>,
}

impl BusState {
    /// Records the newest run per id; an older run never displaces a newer one.
    fn note_latest(&mut self, run: &RunKey) {
        match self.latest_run_by_id.get(&run.saga_id) {
            Some(current)
                if current.started_at_millis > run.started_at_millis
                    || (current.started_at_millis == run.started_at_millis && current != run) => {}
            _ => {
                self.latest_run_by_id.insert(run.saga_id, run.clone());
            }
        }
    }

    fn insert_terminal_outcome(
        &mut self,
        run: RunKey,
        outcome: SagaTerminalOutcome,
        retention_limit: usize,
    ) -> Option<SagaTerminalOutcome> {
        let quarantine = matches!(outcome, SagaTerminalOutcome::Quarantined { .. });
        if let Some(was_quarantined) = self.terminal_strengths.get(&run)
            && (*was_quarantined || !quarantine)
        {
            return self.terminal_outcomes.get(&run).cloned();
        }
        self.note_latest(&run);
        let inserted_new = self
            .terminal_strengths
            .insert(run.clone(), quarantine)
            .is_none();
        self.terminal_outcomes.insert(run.clone(), outcome.clone());
        if inserted_new {
            self.terminal_order.push_back(run.clone());
        }
        while self.terminal_order.len() > retention_limit {
            let Some(candidate) = self.terminal_order.pop_front() else {
                break;
            };
            if candidate != run {
                self.terminal_outcomes.remove(&candidate);
                self.terminal_replies.remove(&candidate);
                self.terminal_strengths.remove(&candidate);
                if self.latest_run_by_id.get(&candidate.saga_id) == Some(&candidate) {
                    self.latest_run_by_id.remove(&candidate.saga_id);
                }
                break;
            }
        }
        Some(outcome)
    }

    fn take_reply(&mut self, run: &RunKey) -> Option<SagaReplyTo> {
        let reply = self.terminal_replies.remove(run);
        if reply.is_some() {
            self.terminal_outcomes.remove(run);
        }
        reply
    }

    fn take_outcome(&mut self, run: &RunKey) -> Option<SagaTerminalOutcome> {
        let reply = self.terminal_replies.remove(run).map(|reply| reply.outcome);
        let direct = self.terminal_outcomes.remove(run);
        reply.or(direct)
    }
}

/// Admission state shared between a resolver runtime and the bus.
///
/// A durable resolver is `activated == false` from attachment until the
/// application explicitly activates recovery after binding participants.
struct ResolverGate {
    journal: Option<Arc<dyn TerminalResolverJournal>>,
    activated: AtomicBool,
    /// Bounded run index. Authoritative when the journal supports per-saga
    /// lookup (misses reload; ordinarily resolved ids are evictable); otherwise
    /// every fence stays resident and admission is refused at capacity.
    /// Serializes start admission.
    admission: Mutex<AdmissionIndex>,
    // Last: journal and all other gate-owned fields are released first.
    _release: ReleaseGuard,
}

/// Bounds of the in-memory admission index.
#[derive(Clone, Copy, Debug)]
struct AdmissionLimits {
    /// Saga ids and full-run fences resident at once (each bounded by this value).
    max_ids: usize,
    /// Effect fingerprints of unresolved or failed runs resident at once.
    max_fingerprints: usize,
}

const DEFAULT_ADMISSION_CAPACITY: usize = 1 << 18;

impl Default for AdmissionLimits {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_ADMISSION_CAPACITY)
    }
}

impl AdmissionLimits {
    fn with_capacity(max_ids: usize) -> Self {
        Self {
            max_ids,
            max_fingerprints: max_ids.saturating_mul(4),
        }
    }

    /// Fingerprint room kept free when admitting a run, so a healthy run can
    /// record its first effects without immediately hitting the hard budget.
    /// Bounded and small: it is practical slack, not a future-run reservation.
    fn fingerprint_headroom(&self) -> usize {
        (self.max_fingerprints / 4).min(64)
    }

    /// `SAGA_ADMISSION_CAPACITY` bounds resident saga ids and runs per resolver.
    fn from_env() -> Self {
        match std::env::var("SAGA_ADMISSION_CAPACITY") {
            Ok(raw) => match raw.parse::<usize>() {
                Ok(value) if value > 0 => Self::with_capacity(value),
                Ok(_) => Self::default(),
                Err(error) => {
                    tracing::error!(
                        target: "core::saga",
                        event = "saga_admission_capacity_parse_failed",
                        env = "SAGA_ADMISSION_CAPACITY",
                        value = %raw,
                        error = %error
                    );
                    Self::default()
                }
            },
            Err(_) => Self::default(),
        }
    }
}

/// Lifecycle phase of one run as known from retained durable history.
///
/// A successful and a failed terminal are different: a successful run keeps
/// its business effects, so later sibling or trailing work is healthy, while
/// new uncertainty after a failure needs reconciliation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RunPhase {
    Active,
    Completed,
    Failed,
    Quarantined,
}

impl RunPhase {
    /// Quarantine dominates; the first ordinary terminal is never replaced.
    fn merge(self, new: Self) -> Self {
        match (self, new) {
            (Self::Quarantined, _) | (_, Self::Quarantined) => Self::Quarantined,
            (Self::Active, new) => new,
            (current, _) => current,
        }
    }

    fn is_ordinary_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

/// What the resolver does with a non-start event, given durable history.
#[derive(Debug, PartialEq, Eq)]
enum Fence {
    /// Unknown or active run: the resolver handles it normally.
    Pass,
    /// Ordinarily resolved run: stale replay or healthy trailing work after
    /// success; no journal, no output.
    Drop,
    /// Quarantined run (or a quarantine of a resolved run): keep the evidence
    /// but never feed a resolver whose cache may have been evicted.
    RetainOnly,
    /// New compensable effect after a *failed* terminal: retain it and escalate.
    Escalate,
}

/// Why a start was refused, and which waiters the refusal may fail.
struct Refusal {
    reason: Box<str>,
    /// The refused run is not an admitted run, so its own waiter may fail.
    reject_run_waiter: bool,
    /// No run of the id is open, so an id-scoped waiter cannot belong to an owner.
    reject_id_waiter: bool,
}

type CompletedFingerprint = (Box<str>, u64);
type AcceptedFingerprint = (Box<str>, crate::StepExecutionId);

struct RunEntry {
    started_at_millis: u64,
    phase: RunPhase,
    /// The start's admission intent is already journaled; the resolver must not
    /// append it a second time when the fanned-out event reaches it.
    prejournaled: bool,
    /// A `SagaQuarantined` row of this exact run is known to be in the journal
    /// (read back from it, or appended successfully). A quarantined run without
    /// it is only a live fence whose durable copy is still pending.
    quarantine_durable: bool,
    /// Compensable effects known while the run was open. Dropped once the run
    /// succeeded, because success never escalates later siblings.
    completed: HashSet<CompletedFingerprint>,
    accepted: HashSet<AcceptedFingerprint>,
}

#[derive(Default)]
struct IdEntry {
    runs: Vec<RunEntry>,
    queued_for_eviction: bool,
    /// A durable run of this id could not be made resident for lack of room.
    /// The journal holds it; it is merged back in place once room returns.
    incomplete: bool,
}

impl IdEntry {
    fn fingerprints(&self) -> usize {
        self.runs
            .iter()
            .map(|run| run.completed.len() + run.accepted.len())
            .sum()
    }

    fn evictable(&self) -> bool {
        self.runs.iter().all(|run| run.phase.is_ordinary_terminal())
    }
}

#[derive(Default)]
struct AdmissionIndex {
    ids: HashMap<SagaId, IdEntry>,
    evictable: VecDeque<SagaId>,
    runs: usize,
    fingerprints: usize,
    /// Unidentified excess evidence cannot be forgotten in an ephemeral index.
    /// Refuse new ownership thereafter rather than manufacture missing fences.
    overflowed: bool,
    limits: AdmissionLimits,
    /// The journal can reload an evicted id, so ordinarily resolved ids may be
    /// dropped from memory. Without it nothing is ever forgotten.
    can_evict: bool,
    /// Rebuilding from history must be complete, so limits are not enforced.
    loading: bool,
    ephemeral: bool,
    ordinary_runs: VecDeque<(SagaId, u64)>,
    pending_durable_quarantine: VecDeque<SagaContext>,
    pending_fence_keys: HashSet<(SagaId, u64)>,
}

impl AdmissionIndex {
    fn new(limits: AdmissionLimits, can_evict: bool) -> Self {
        Self {
            limits,
            can_evict,
            ..Self::default()
        }
    }

    fn from_events<'a>(
        limits: AdmissionLimits,
        can_evict: bool,
        events: impl Iterator<Item = &'a SagaChoreographyEvent>,
    ) -> Self {
        let mut index = Self::new(limits, can_evict);
        index.loading = true;
        for event in events {
            let _ = index.observe_row(event);
        }
        index.loading = false;
        index.make_room();
        index
    }

    fn resident_ids(&self) -> usize {
        self.ids.len()
    }

    fn fingerprint_count(&self) -> usize {
        self.fingerprints
    }

    fn contains(&self, saga_id: SagaId) -> bool {
        self.ids.contains_key(&saga_id)
    }

    fn is_incomplete(&self, saga_id: SagaId) -> bool {
        self.ids.get(&saga_id).is_some_and(|entry| entry.incomplete)
    }

    fn clear_incomplete(&mut self, saga_id: SagaId) {
        if let Some(entry) = self.ids.get_mut(&saga_id) {
            entry.incomplete = false;
        }
    }

    /// Evidence that is neither resident nor persisted can never be recovered:
    /// refuse new ownership permanently rather than forget it.
    fn mark_unpersisted(&mut self, context: &SagaContext) {
        if self.run(context).is_none() {
            self.overflowed = true;
        }
    }

    fn run(&self, context: &SagaContext) -> Option<&RunEntry> {
        self.ids
            .get(&context.saga_id)?
            .runs
            .iter()
            .find(|run| run.started_at_millis == context.saga_started_at_millis)
    }

    fn phase(&self, context: &SagaContext) -> Option<RunPhase> {
        self.run(context).map(|run| run.phase)
    }

    fn run_mut(&mut self, context: &SagaContext) -> Option<&mut RunEntry> {
        self.ids
            .get_mut(&context.saga_id)?
            .runs
            .iter_mut()
            .find(|run| run.started_at_millis == context.saga_started_at_millis)
    }

    /// Only a validated journal row or a successful append counts as durable.
    fn mark_quarantine_durable(&mut self, context: &SagaContext) {
        if let Some(run) = self.run_mut(context) {
            run.quarantine_durable = true;
        }
    }

    fn quarantine_durable(&self, context: &SagaContext) -> bool {
        self.run(context).is_some_and(|run| run.quarantine_durable)
    }

    fn exceeds_capacity(&self) -> bool {
        self.ids.len() > self.limits.max_ids
            || self.runs > self.limits.max_ids
            || self.fingerprints > self.limits.max_fingerprints
    }

    fn remove_id(&mut self, saga_id: SagaId) {
        if let Some(entry) = self.ids.remove(&saga_id) {
            self.runs = self.runs.saturating_sub(entry.runs.len());
            self.fingerprints = self.fingerprints.saturating_sub(entry.fingerprints());
            if entry.queued_for_eviction {
                // Explicit rejection of an oversized reload must not leave stale
                // queue entries that accumulate on every subsequent cache miss.
                self.evictable.retain(|candidate| *candidate != saga_id);
            }
        }
    }

    fn make_room(&mut self) {
        self.make_room_for(None);
    }

    /// Evict only reloadable ordinary ids, never the complete id currently used
    /// by an admission/fence decision. Evicting that id and then reserving only
    /// its new run would silently turn a complete resident history into a partial one.
    fn make_room_for(&mut self, protected: Option<SagaId>) {
        if self.ephemeral {
            while (self.ids.len() >= self.limits.max_ids
                || self.runs >= self.limits.max_ids
                || self
                    .fingerprints
                    .saturating_add(self.limits.fingerprint_headroom())
                    >= self.limits.max_fingerprints)
                && !self.ordinary_runs.is_empty()
            {
                let (id, started) = self.ordinary_runs.pop_front().unwrap();
                if let Some(entry) = self.ids.get_mut(&id)
                    && let Some(position) = entry.runs.iter().position(|r| {
                        r.started_at_millis == started && r.phase.is_ordinary_terminal()
                    })
                {
                    let run = entry.runs.remove(position);
                    self.runs -= 1;
                    self.fingerprints -= run.completed.len() + run.accepted.len();
                    if entry.runs.is_empty() {
                        self.ids.remove(&id);
                    }
                }
            }
            return;
        }
        if !self.can_evict {
            return;
        }
        let candidates = self.evictable.len();
        for _ in 0..candidates {
            if self.ids.len() < self.limits.max_ids
                && self.runs < self.limits.max_ids
                && self
                    .fingerprints
                    .saturating_add(self.limits.fingerprint_headroom())
                    < self.limits.max_fingerprints
            {
                break;
            }
            let Some(candidate) = self.evictable.pop_front() else {
                break;
            };
            if protected == Some(candidate) {
                self.evictable.push_back(candidate);
                continue;
            }
            let Some(entry) = self.ids.get_mut(&candidate) else {
                continue;
            };
            entry.queued_for_eviction = false;
            if entry.evictable() {
                self.remove_id(candidate);
            }
        }
    }

    /// Bound both ids and full-run fences. Reusing one id must not bypass the
    /// capacity of an ephemeral resolver by growing its run vector forever.
    fn has_capacity_for(&mut self, context: &SagaContext) -> bool {
        self.make_room_for(Some(context.saga_id));
        if self.run(context).is_some() {
            return true;
        }
        !self.overflowed
            && (self.ids.contains_key(&context.saga_id) || self.ids.len() < self.limits.max_ids)
            && self.runs < self.limits.max_ids
            && self
                .fingerprints
                .saturating_add(self.limits.fingerprint_headroom())
                < self.limits.max_fingerprints
    }

    fn raise(&mut self, saga_id: SagaId, started: u64, phase: RunPhase) -> bool {
        let known = self.ids.get(&saga_id).is_some_and(|entry| {
            entry
                .runs
                .iter()
                .any(|run| run.started_at_millis == started)
        });
        if !known && !self.loading {
            self.make_room_for(Some(saga_id));
            if self.runs >= self.limits.max_ids
                || (!self.ids.contains_key(&saga_id) && self.ids.len() >= self.limits.max_ids)
            {
                if !self.can_evict {
                    // Nothing can be reloaded: the lost run is permanent.
                    self.overflowed = true;
                } else if let Some(entry) = self.ids.get_mut(&saga_id) {
                    // The durable journal keeps the run; the resident id is just
                    // not complete until room returns. An absent id reloads whole.
                    entry.incomplete = true;
                }
                return false;
            }
        }
        let entry = self.ids.entry(saga_id).or_default();
        let position = match entry
            .runs
            .iter()
            .position(|run| run.started_at_millis == started)
        {
            Some(position) => position,
            None => {
                self.runs += 1;
                entry.runs.push(RunEntry {
                    started_at_millis: started,
                    phase: RunPhase::Active,
                    prejournaled: false,
                    quarantine_durable: false,
                    completed: HashSet::new(),
                    accepted: HashSet::new(),
                });
                entry.runs.len() - 1
            }
        };
        let run = &mut entry.runs[position];
        let was_terminal = run.phase.is_ordinary_terminal();
        run.phase = run.phase.merge(phase);
        if self.ephemeral && !was_terminal && run.phase.is_ordinary_terminal() {
            self.ordinary_runs.push_back((saga_id, started));
        }
        if matches!(run.phase, RunPhase::Completed | RunPhase::Quarantined)
            && !(run.completed.is_empty() && run.accepted.is_empty())
        {
            self.fingerprints = self
                .fingerprints
                .saturating_sub(run.completed.len() + run.accepted.len());
            run.completed.clear();
            run.accepted.clear();
        }
        if self.can_evict && !entry.queued_for_eviction && entry.evictable() {
            entry.queued_for_eviction = true;
            self.evictable.push_back(saga_id);
        }
        true
    }

    /// False means excess evidence could not be represented safely; the caller
    /// must visibly quarantine, not publish an ordinary outcome or silently grow.
    fn observe(&mut self, event: &SagaChoreographyEvent) -> bool {
        self.observe_with(event, false)
    }

    /// Observes a row that was read back from the journal: a quarantine row
    /// also proves that run's quarantine is durable.
    fn observe_row(&mut self, event: &SagaChoreographyEvent) -> bool {
        self.observe_with(event, true)
    }

    fn observe_with(&mut self, event: &SagaChoreographyEvent, journal_row: bool) -> bool {
        let context = event.context();
        let phase = match event {
            SagaChoreographyEvent::SagaCompleted { .. } => RunPhase::Completed,
            SagaChoreographyEvent::SagaFailed { .. } => RunPhase::Failed,
            SagaChoreographyEvent::SagaQuarantined { .. } => RunPhase::Quarantined,
            _ => RunPhase::Active,
        };
        // Phase first: a run that is not (yet) known is created Active.
        if !self.raise(context.saga_id, context.saga_started_at_millis, phase) {
            return false;
        }
        if journal_row && phase == RunPhase::Quarantined {
            self.mark_quarantine_durable(context);
        }
        let Some(entry) = self.ids.get_mut(&context.saga_id) else {
            return false;
        };
        let Some(run) = entry
            .runs
            .iter_mut()
            .find(|run| run.started_at_millis == context.saga_started_at_millis)
        else {
            return false;
        };
        // Fingerprints matter only while the run can still fail: success never
        // escalates later siblings, and quarantine already dominates.
        if run.phase != RunPhase::Active {
            return true;
        }
        let new_fingerprint = match event {
            SagaChoreographyEvent::StepCompleted {
                compensation_available: true,
                ..
            } => !run
                .completed
                .contains(&(context.step_name.clone(), context.trace_id)),
            SagaChoreographyEvent::StepAccepted { execution_id, .. } => !run
                .accepted
                .contains(&(context.step_name.clone(), execution_id.clone())),
            _ => false,
        };
        if new_fingerprint && !self.loading && self.fingerprints >= self.limits.max_fingerprints {
            // Reloadable ordinary terminal ids are cache, not authority: reclaim
            // them (never this id) before declaring the evidence unrepresentable.
            self.make_room_for(Some(context.saga_id));
            if self.fingerprints >= self.limits.max_fingerprints {
                return false;
            }
        }
        let Some(run) = self.ids.get_mut(&context.saga_id).and_then(|entry| {
            entry
                .runs
                .iter_mut()
                .find(|run| run.started_at_millis == context.saga_started_at_millis)
        }) else {
            return false;
        };
        let inserted = match event {
            SagaChoreographyEvent::StepCompleted {
                compensation_available: true,
                ..
            } => run
                .completed
                .insert((context.step_name.clone(), context.trace_id)),
            SagaChoreographyEvent::StepAccepted { execution_id, .. } => run
                .accepted
                .insert((context.step_name.clone(), execution_id.clone())),
            _ => false,
        };
        if inserted {
            self.fingerprints += 1;
        }
        true
    }

    fn mark_prejournaled(&mut self, context: &SagaContext) {
        if let Some(entry) = self.ids.get_mut(&context.saga_id)
            && let Some(run) = entry
                .runs
                .iter_mut()
                .find(|run| run.started_at_millis == context.saga_started_at_millis)
        {
            run.prejournaled = true;
        }
    }

    fn take_prejournaled(&mut self, context: &SagaContext) -> bool {
        self.ids
            .get_mut(&context.saga_id)
            .and_then(|entry| {
                entry
                    .runs
                    .iter_mut()
                    .find(|run| run.started_at_millis == context.saga_started_at_millis)
            })
            .is_some_and(|run| std::mem::take(&mut run.prejournaled))
    }

    fn has_open_run(&self, saga_id: SagaId) -> bool {
        self.ids.get(&saga_id).is_some_and(|entry| {
            entry
                .runs
                .iter()
                .any(|run| matches!(run.phase, RunPhase::Active | RunPhase::Quarantined))
        })
    }

    /// Starts of the id's runs that are still open (not resolved, not quarantined).
    fn active_starts(&self, saga_id: SagaId) -> impl Iterator<Item = u64> + '_ {
        self.ids.get(&saga_id).into_iter().flat_map(|entry| {
            entry
                .runs
                .iter()
                .filter(|run| run.phase == RunPhase::Active)
                .map(|run| run.started_at_millis)
        })
    }

    /// Replay, quarantine and active-ownership rules for a new start.
    fn start_refusal(&self, start: &SagaContext) -> Option<String> {
        let runs = &self.ids.get(&start.saga_id)?.runs;
        if runs.iter().any(|run| run.phase == RunPhase::Quarantined) {
            return Some(format!(
                "saga id is quarantined and unresolved; saga_id={}",
                start.saga_id.get()
            ));
        }
        if runs.iter().any(|run| {
            run.phase.is_ordinary_terminal()
                && run.started_at_millis >= start.saga_started_at_millis
        }) {
            return Some(format!(
                "terminal saga run replay; saga_id={} run_started_at_millis={}",
                start.saga_id.get(),
                start.saga_started_at_millis
            ));
        }
        if runs.iter().any(|run| {
            run.phase == RunPhase::Active && run.started_at_millis != start.saga_started_at_millis
        }) {
            return Some(format!(
                "saga id has an unresolved active run; saga_id={}",
                start.saga_id.get()
            ));
        }
        if self.phase(start).is_some() {
            return Some(format!(
                "saga run was already admitted; saga_id={} run_started_at_millis={}",
                start.saga_id.get(),
                start.saga_started_at_millis
            ));
        }
        None
    }

    fn has_successor(&self, context: &SagaContext) -> bool {
        self.ids.get(&context.saga_id).is_some_and(|entry| {
            entry.runs.iter().any(|run| {
                run.started_at_millis > context.saga_started_at_millis
                    && run.phase != RunPhase::Quarantined
            })
        })
    }

    /// Older uncertainty also supersedes a successor's ordinary terminal, even
    /// when the resolver's volatile state/detail for that successor is gone.
    fn successor_starts(&self, context: &SagaContext) -> Vec<u64> {
        self.ids
            .get(&context.saga_id)
            .map_or_else(Vec::new, |entry| {
                entry
                    .runs
                    .iter()
                    .filter_map(|run| {
                        (run.started_at_millis > context.saga_started_at_millis
                            && run.phase != RunPhase::Quarantined)
                            .then_some(run.started_at_millis)
                    })
                    .collect()
            })
    }

    fn fence(&self, event: &SagaChoreographyEvent) -> Fence {
        let context = event.context();
        let Some(run) = self.run(context) else {
            return if self.overflowed || self.is_incomplete(context.saga_id) {
                Fence::RetainOnly
            } else {
                Fence::Pass
            };
        };
        match run.phase {
            RunPhase::Active => Fence::Pass,
            RunPhase::Quarantined => Fence::RetainOnly,
            RunPhase::Completed | RunPhase::Failed
                if matches!(event, SagaChoreographyEvent::SagaQuarantined { .. }) =>
            {
                Fence::RetainOnly
            }
            // Healthy trailing, sibling or replayed work of a successful run.
            RunPhase::Completed => Fence::Drop,
            RunPhase::Failed => match event {
                SagaChoreographyEvent::StepCompleted {
                    compensation_available: true,
                    ..
                } if !run
                    .completed
                    .contains(&(context.step_name.clone(), context.trace_id)) =>
                {
                    Fence::Escalate
                }
                SagaChoreographyEvent::StepAccepted { execution_id, .. }
                    if !run
                        .accepted
                        .contains(&(context.step_name.clone(), execution_id.clone())) =>
                {
                    Fence::Escalate
                }
                _ => Fence::Drop,
            },
        }
    }
}

impl ResolverGate {
    fn index(&self) -> std::sync::MutexGuard<'_, AdmissionIndex> {
        // Entries only ever merge toward stronger phases, so a poisoned guard
        // is still a safe (conservative) view for the resolver actor.
        self.admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn can_reload(&self) -> bool {
        self.journal
            .as_ref()
            .is_some_and(|journal| journal.supports_saga_lookup())
    }

    /// Makes the id's complete durable history resident. Indexed resident ids
    /// marked incomplete by capacity pressure are merged before callers trust
    /// them; an absent id reloads whole. Lookup/merge errors remain fail-closed.
    fn ensure_resident(&self, index: &mut AdmissionIndex, saga_id: SagaId) -> Result<(), Box<str>> {
        if !self.can_reload() {
            return Ok(());
        }
        let Some(journal) = &self.journal else {
            return Ok(());
        };
        if index.contains(saga_id) {
            if !index.is_incomplete(saga_id) {
                return Ok(());
            }
            return Self::complete_resident(journal.as_ref(), index, saga_id);
        }
        let row_budget = index
            .limits
            .max_ids
            .saturating_add(index.limits.max_fingerprints);
        let entries = journal
            .read_saga_bounded(saga_id, row_budget)
            .map_err(|error| {
                Box::<str>::from(format!(
                    "terminal resolver journal lookup failed; saga_id={}: {error}",
                    saga_id.get()
                ))
            })?;
        if entries.is_empty() {
            return Ok(());
        }
        index.make_room_for(Some(saga_id));
        index.loading = true;
        for entry in &entries {
            let _ = index.observe_row(&entry.event);
        }
        index.loading = false;
        index.make_room_for(Some(saga_id));
        if index.exceeds_capacity() {
            // The requested id alone may exceed the run/fingerprint budget. Do
            // not keep a partial id or forget any fence: refuse the lookup visibly.
            index.remove_id(saga_id);
            return Err(format!(
                "terminal resolver admission history exceeds capacity; saga_id={}",
                saga_id.get()
            )
            .into());
        }
        Ok(())
    }

    /// Merges durable runs that earlier lacked room into a resident id, in
    /// place (so in-flight flags such as prejournaled starts survive). The id
    /// stays incomplete, and the caller is refused, unless every run fits.
    fn complete_resident(
        journal: &dyn TerminalResolverJournal,
        index: &mut AdmissionIndex,
        saga_id: SagaId,
    ) -> Result<(), Box<str>> {
        let row_budget = index
            .limits
            .max_ids
            .saturating_add(index.limits.max_fingerprints);
        let entries = journal
            .read_saga_bounded(saga_id, row_budget)
            .map_err(|error| {
                Box::<str>::from(format!(
                    "terminal resolver journal lookup failed; saga_id={}: {error}",
                    saga_id.get()
                ))
            })?;
        for entry in &entries {
            if !index.observe_row(&entry.event) {
                return Err(format!(
                    "terminal resolver admission history exceeds capacity; saga_id={}",
                    saga_id.get()
                )
                .into());
            }
        }
        index.clear_incomplete(saga_id);
        Ok(())
    }

    /// Fence for a non-start event. A storage failure never feeds a resolver
    /// blindly: the evidence is retained and nothing is answered.
    fn fence(&self, event: &SagaChoreographyEvent) -> Fence {
        let saga_id = event.context().saga_id;
        let mut index = self.index();
        if let Err(reason) = self.ensure_resident(&mut index, saga_id) {
            tracing::error!(target: "core::saga", event = "terminal_resolver_fence_lookup_failed", reason = %reason);
            return Fence::RetainOnly;
        }
        index.fence(event)
    }

    fn observe(&self, event: &SagaChoreographyEvent) -> bool {
        let saga_id = event.context().saga_id;
        let mut index = self.index();
        match self.ensure_resident(&mut index, saga_id) {
            Ok(()) => index.observe(event),
            // The event is already journaled (or the journal is down); a later
            // admission reloads authoritative history from the journal.
            Err(reason) => {
                tracing::error!(target: "core::saga", event = "terminal_resolver_index_reload_failed", reason = %reason);
                false
            }
        }
    }

    fn mark_unpersisted(&self, context: &SagaContext) {
        self.index().mark_unpersisted(context);
    }

    fn mark_quarantine_durable(&self, context: &SagaContext) {
        let mut index = self.index();
        index.mark_quarantine_durable(context);
        index
            .pending_fence_keys
            .remove(&(context.saga_id, context.saga_started_at_millis));
    }

    /// Resident quarantined run whose durable quarantine row is not yet proven.
    fn quarantine_pending(&self, context: &SagaContext) -> bool {
        let index = self.index();
        index.phase(context) == Some(RunPhase::Quarantined) && !index.quarantine_durable(context)
    }

    fn phase(&self, context: &SagaContext) -> Option<RunPhase> {
        self.index().phase(context)
    }

    /// Run authority within this resolver's saga type: preserve a resident hit,
    /// otherwise consult complete bounded durable history before the incoming
    /// event is journaled. Failed/oversize lookup or absent exact-run evidence
    /// cannot prove ownership; the incoming event cannot vouch for itself.
    fn known_run(&self, context: &SagaContext) -> bool {
        let mut index = self.index();
        if index.phase(context).is_some() {
            return true;
        }
        if let Err(reason) = self.ensure_resident(&mut index, context.saga_id) {
            tracing::error!(target: "core::saga", event = "terminal_resolver_known_run_lookup_failed", reason = %reason);
            return false;
        }
        index.phase(context).is_some()
    }

    fn raise(&self, context: &SagaContext, phase: RunPhase) {
        self.index()
            .raise(context.saga_id, context.saga_started_at_millis, phase);
        if phase == RunPhase::Quarantined {
            self.track_pending_fence(context);
        }
    }
    fn track_pending_fence(&self, context: &SagaContext) {
        if self.journal.is_none() {
            return;
        }
        let mut index = self.index();
        if index.phase(context) == Some(RunPhase::Quarantined)
            && !index.quarantine_durable(context)
            && index
                .pending_fence_keys
                .insert((context.saga_id, context.saga_started_at_millis))
        {
            index.pending_durable_quarantine.push_back(context.clone());
        }
    }

    fn has_successor(&self, context: &SagaContext) -> bool {
        self.index().has_successor(context)
    }

    fn successor_starts(&self, context: &SagaContext) -> Vec<u64> {
        self.index().successor_starts(context)
    }

    fn take_prejournaled(&self, context: &SagaContext) -> bool {
        self.index().take_prejournaled(context)
    }

    /// Active runs of the id (all open runs of this resolver's saga type).
    fn active_starts(&self, saga_id: SagaId) -> Vec<u64> {
        self.index().active_starts(saga_id).collect()
    }

    /// Serialized start admission: check, journal the intent strictly, then
    /// reserve the run. Nothing is fanned out unless this returns `Ok`.
    ///
    /// The strict append (an fsync for durable journals) stays inside the
    /// critical section: admission authority requires check-then-append to be
    /// atomic per resolver, so durable starts of one saga type are serialized.
    fn admit_start(
        &self,
        event: &SagaChoreographyEvent,
        context: &SagaContext,
    ) -> Result<(), Refusal> {
        if !self.activated.load(Ordering::Acquire) {
            return Err(Refusal {
                reason: format!(
                    "terminal resolver recovery is not activated; saga_type={} saga_id={}",
                    context.saga_type,
                    context.saga_id.get()
                )
                .into(),
                reject_run_waiter: true,
                reject_id_waiter: true,
            });
        }
        let Ok(mut index) = self.admission.lock() else {
            return Err(Refusal {
                reason: "terminal resolver admission index unavailable".into(),
                reject_run_waiter: true,
                reject_id_waiter: true,
            });
        };
        if let Err(reason) = self.ensure_resident(&mut index, context.saga_id) {
            return Err(Refusal {
                reason,
                reject_run_waiter: true,
                reject_id_waiter: true,
            });
        }
        if let Some(reason) = index.start_refusal(context) {
            return Err(Refusal {
                reason: reason.into(),
                reject_run_waiter: true,
                reject_id_waiter: !index.has_open_run(context.saga_id)
                    || index.phase(context).is_some(),
            });
        }
        if !index.has_capacity_for(context) {
            return Err(Refusal {
                reason: format!(
                    "terminal resolver admission capacity reached; saga_type={} saga_id={} resident_ids={} fingerprints={}",
                    context.saga_type,
                    context.saga_id.get(),
                    index.resident_ids(),
                    index.fingerprint_count()
                )
                .into(),
                reject_run_waiter: true,
                reject_id_waiter: true,
            });
        }
        if let Some(journal) = &self.journal
            && let Err(error) = journal.append(event.clone())
        {
            return Err(Refusal {
                reason: format!(
                    "terminal resolver could not journal start admission; saga_id={}: {error}",
                    context.saga_id.get()
                )
                .into(),
                reject_run_waiter: true,
                reject_id_waiter: true,
            });
        }
        index.raise(
            context.saga_id,
            context.saga_started_at_millis,
            RunPhase::Active,
        );
        if self.journal.is_some() {
            index.mark_prejournaled(context);
        }
        Ok(())
    }
}

struct TerminalResolverRuntime {
    gate: Arc<ResolverGate>,
    subscription: FirehoseSubscription,
    resolver_ref: local_sync::SyncActorRef<TerminalResolverActor>,
    handle: Arc<Mutex<Option<local_sync::ActorHandle>>>,
    policy: TerminalPolicy,
    responder: String,
}

fn shutdown_resolver_handle(handle: &Mutex<Option<local_sync::ActorHandle>>) {
    let handle = handle
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(handle) = handle {
        handle.shutdown();
    }
}

#[derive(Clone, Debug)]
enum TerminalResolverTell {
    Ingest(Box<SagaChoreographyEvent>),
    ActivateRecovery,
    PollTimeouts,
}

impl icanact_core::TellAskTell for TerminalResolverTell {}

struct TerminalResolverActor {
    resolver: TerminalResolver,
    recovery_events: Vec<SagaChoreographyEvent>,
    /// Resolver output computed before recovery activation; delivered once at
    /// activation so watchdog and ingress recovery cannot precede binding.
    held_events: Vec<SagaChoreographyEvent>,
    /// Incoming known-run quarantine replies held until activation. One per
    /// represented quarantined run: bounded by admission capacity, no replay of
    /// the input event or duplicate journal append merely to notify a waiter.
    held_quarantine_replies: HashMap<(SagaId, u64), SagaChoreographyEvent>,
    activated: bool,
    gate: Arc<ResolverGate>,
    bus: Weak<BusInner>,
    responder: Arc<str>,
    saga_type: Box<str>,
    // Last, including the internal bus's subscriber resources, not just its gate.
    _release: ReleaseGuard,
}

/// Pending quarantine fences retried per watchdog tick (bounded, no spinning).
const PENDING_FENCE_RETRIES_PER_TICK: usize = 8;

impl TerminalResolverActor {
    fn publish_terminal_events(&mut self, terminal_events: Vec<SagaChoreographyEvent>) {
        if !self.activated {
            self.held_events.extend(terminal_events);
            return;
        }
        let Some(inner) = self.bus.upgrade() else {
            return;
        };
        let bus = SagaChoreographyBus { inner };
        for terminal_event in terminal_events {
            if !matches!(
                terminal_event,
                SagaChoreographyEvent::SagaQuarantined { .. }
            ) && self.gate.phase(terminal_event.context()) == Some(RunPhase::Quarantined)
            {
                continue;
            }
            let _ =
                bus.complete_terminal_reply_from_event(&terminal_event, self.responder.as_ref());
            if let Err(err) = bus.publish_strict(terminal_event) {
                tracing::error!(
                    target: "core::saga",
                    event = "terminal_resolver_publish_failed",
                    saga_type = self.saga_type.as_ref(),
                    error = ?err
                );
            }
        }
    }
}

impl TerminalResolverActor {
    fn fence_event(&self, context: &SagaContext) -> SagaChoreographyEvent {
        SagaChoreographyEvent::SagaQuarantined {
            context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
            reason: "terminal resolver retained quarantined ownership; reconciliation required"
                .into(),
            step: context.step_name.clone(),
            participant_id: self.responder.as_ref().into(),
        }
    }

    /// Strictly appends the quarantine fence of a run; only success proves it durable.
    fn append_fence(&mut self, context: &SagaContext) -> bool {
        let Some(journal) = &self.gate.journal else {
            return true;
        };
        let fence = self.fence_event(context);
        if let Err(error) = journal.append(fence) {
            tracing::error!(target: "core::saga", event = "terminal_resolver_quarantine_fence_append_failed",
                saga_type = self.saga_type.as_ref(), saga_id = %context.saga_id, error = ?error);
            self.track_pending_fence(context);
            return false;
        }
        self.gate.mark_quarantine_durable(context);

        true
    }

    /// Remembers a live quarantine whose durable row is not yet proven. Runs the
    /// index cannot represent are not tracked (they stay conservatively fenced).
    fn track_pending_fence(&mut self, context: &SagaContext) {
        if self.gate.journal.is_none() || !self.gate.quarantine_pending(context) {
            return;
        }
        self.gate.track_pending_fence(context);
    }

    /// Fences a run live (admission and resolver view) and tracks its durable copy.
    fn fence_live(&mut self, context: &SagaContext) {
        self.gate.raise(context, RunPhase::Quarantined);
        self.track_pending_fence(context);
    }

    /// Raises and keeps resolver outputs; a quarantine for a run that is already
    /// fenced is a duplicate and is dropped.
    fn fence_outputs(&mut self, outputs: Vec<SagaChoreographyEvent>) -> Vec<SagaChoreographyEvent> {
        let mut kept = Vec::with_capacity(outputs.len());
        for output in outputs {
            if matches!(output, SagaChoreographyEvent::SagaQuarantined { .. }) {
                if self.gate.phase(output.context()) == Some(RunPhase::Quarantined) {
                    continue;
                }
                self.fence_live(output.context());
            }
            kept.push(output);
        }
        kept
    }

    /// Bounded retry of pending quarantine fences from the watchdog tick, so a
    /// recovered journal receives them without any later evidence event.
    fn retry_pending_fences(&mut self) {
        if !self.activated {
            return;
        }
        let count =
            PENDING_FENCE_RETRIES_PER_TICK.min(self.gate.index().pending_durable_quarantine.len());
        for _ in 0..count {
            let context = self.gate.index().pending_durable_quarantine.pop_front();
            let Some(context) = context else {
                break;
            };
            let key = (context.saga_id, context.saga_started_at_millis);
            if !self.gate.quarantine_pending(&context) && self.gate.phase(&context).is_some() {
                self.gate.index().pending_fence_keys.remove(&key);
                continue;
            }
            if !self.append_fence(&context) {
                self.gate
                    .index()
                    .pending_durable_quarantine
                    .push_back(context);
                break;
            }
        }
    }

    /// Reconstruct successor fences from actual retained quarantine rows, never
    /// from gapped ordinary detail. The earliest quarantine per ID subsumes the
    /// others, so history is scanned once rather than once per duplicate row.
    /// Outputs remain held until activation; already-derived recovery outputs
    /// are latched first so they are not synthesized twice.
    fn recover_successor_fences(&mut self, history: &[SagaChoreographyEvent]) {
        let derived: Vec<_> = self
            .recovery_events
            .iter()
            .filter(|event| matches!(event, SagaChoreographyEvent::SagaQuarantined { .. }))
            .map(|event| event.context().clone())
            .collect();
        for context in derived {
            self.fence_live(&context);
        }
        let mut oldest: HashMap<SagaId, &SagaChoreographyEvent> = HashMap::new();
        for event in history {
            if !matches!(event, SagaChoreographyEvent::SagaQuarantined { .. })
                || self.gate.phase(event.context()) != Some(RunPhase::Quarantined)
            {
                continue;
            }
            oldest
                .entry(event.context().saga_id)
                .and_modify(|prior| {
                    if event.context().saga_started_at_millis
                        < prior.context().saga_started_at_millis
                    {
                        *prior = event;
                    }
                })
                .or_insert(event);
        }
        for event in oldest.into_values() {
            self.fence_successors(event);
        }
    }

    /// A quarantine row for the run was retained: older-run uncertainty also
    /// fences every admitted successor. Evaluated AFTER the retained quarantine
    /// raised the older run, so admission (which serializes on the index) either
    /// refused the successor or is seen here.
    fn fence_successors(&mut self, event: &SagaChoreographyEvent) {
        let SagaChoreographyEvent::SagaQuarantined {
            context,
            reason,
            step,
            participant_id,
        } = event
        else {
            return;
        };
        if !self.gate.has_successor(context) {
            return;
        }
        let outputs = self.resolver.ingest(event);
        let mut out = self.fence_outputs(outputs);
        for start in self.gate.successor_starts(context) {
            let mut successor = context.next_step(TERMINAL_RESOLVER_STEP.into());
            successor.saga_started_at_millis = start;
            let quarantine = SagaChoreographyEvent::SagaQuarantined {
                context: successor,
                reason: format!(
                    "unresolved quarantine in run {}: {reason}",
                    context.saga_started_at_millis
                )
                .into(),
                step: step.clone(),
                participant_id: participant_id.clone(),
            };
            self.fence_live(quarantine.context());
            // Latch the resolver's own view; its cross-run echo is a duplicate.
            let _ = self.resolver.ingest(&quarantine);
            out.push(quarantine);
        }
        self.publish_terminal_events(out);
    }

    /// Journals evidence for a fenced run without involving the resolver.
    fn retain_evidence(&mut self, event: &SagaChoreographyEvent) {
        // A quarantine may have been latched while the journal was unwritable.
        // Until its durable row is proven, the fence must precede new retained
        // evidence; otherwise a restart could reconstruct an ordinary success
        // from that evidence and forget the already-published quarantine. Once
        // durable it is never re-appended per late event.
        let context = event.context();
        let incoming_quarantine = matches!(event, SagaChoreographyEvent::SagaQuarantined { .. });
        if !incoming_quarantine
            && self.gate.journal.is_some()
            && self.gate.quarantine_pending(context)
            && !self.append_fence(context)
        {
            return;
        }
        let mut persisted = true;
        if let Some(journal) = &self.gate.journal
            && let Err(error) = journal.append(event.clone())
        {
            persisted = false;
            tracing::error!(target: "core::saga", event = "terminal_resolver_evidence_append_failed",
                saga_type = self.saga_type.as_ref(), saga_id = %event.context().saga_id, error = ?error);
        }
        // Storage failure cannot weaken the live fence; restart reconciliation
        // still requires a writable journal/application-owned durable evidence.
        if !self.gate.observe(event) && !persisted {
            // Neither resident nor persisted: never recoverable from the journal.
            self.gate.mark_unpersisted(event.context());
        }
        if incoming_quarantine && self.gate.journal.is_some() {
            if persisted {
                self.gate.mark_quarantine_durable(context);
            } else {
                self.track_pending_fence(context);
            }
        }
    }

    /// A compensable effect materialised after an ordinary terminal. The
    /// evidence is retained and the run escalates to quarantine. The index is
    /// raised first so a second queued late effect cannot escalate twice.
    fn escalate_late_effect(&mut self, event: &SagaChoreographyEvent) {
        let context = event.context();
        self.retain_evidence(event);
        self.fence_live(context);
        self.publish_terminal_events(vec![SagaChoreographyEvent::SagaQuarantined {
            context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
            reason: "a compensable effect materialised after the saga resolved".into(),
            step: context.step_name.clone(),
            participant_id: "unknown".into(),
        }]);
    }
}

impl SyncActor for TerminalResolverActor {
    type Contract = local_sync::contract::TellOnly;
    type Tell = TerminalResolverTell;
    type Ask = ();
    type Reply = ();
    type Channel = ();
    type PubSub = ();
    type Broadcast = ();

    fn handle_tell(&mut self, msg: Self::Tell) {
        let terminal_events = match msg {
            TerminalResolverTell::Ingest(event) => {
                // A quarantine of a run this resolver already owns resolves its
                // waiters whoever raised it (participant, delivery shortfall, ...).
                let known_quarantine =
                    matches!(*event, SagaChoreographyEvent::SagaQuarantined { .. })
                        && self.gate.known_run(event.context());
                self.ingest_event(&event);
                if known_quarantine {
                    if self.activated {
                        if let Some(inner) = self.bus.upgrade() {
                            let _ = SagaChoreographyBus { inner }
                                .complete_quarantine_reply_from_event(
                                    &event,
                                    self.responder.as_ref(),
                                );
                        }
                    } else {
                        let context = event.context();
                        self.held_quarantine_replies
                            .entry((context.saga_id, context.saga_started_at_millis))
                            .or_insert_with(|| (*event).clone());
                    }
                }
                return;
            }
            TerminalResolverTell::ActivateRecovery => {
                self.activated = true;
                let mut pending = std::mem::take(&mut self.recovery_events);
                pending.append(&mut self.held_events);
                self.publish_terminal_events(pending);
                for event in std::mem::take(&mut self.held_quarantine_replies).into_values() {
                    if let Some(inner) = self.bus.upgrade() {
                        let _ = SagaChoreographyBus { inner }
                            .complete_quarantine_reply_from_event(&event, self.responder.as_ref());
                    }
                }
                return;
            }
            TerminalResolverTell::PollTimeouts => {
                self.retry_pending_fences();
                self.resolver.poll_timeouts()
            }
        };
        self.publish_terminal_events(terminal_events);
    }
}

impl TerminalResolverActor {
    fn ingest_event(&mut self, event: &SagaChoreographyEvent) {
        match self.gate.fence(event) {
            Fence::Pass => {}
            Fence::Drop => {
                tracing::warn!(
                    target: "core::saga",
                    event = "terminal_resolver_fenced_stale_event",
                    saga_type = self.saga_type.as_ref(),
                    saga_id = %event.context().saga_id,
                    event_type = event.event_type()
                );
                return;
            }
            Fence::RetainOnly => {
                // Retain (and raise the index) first; only then is the successor
                // check ordered against concurrent start admission.
                self.retain_evidence(event);
                self.fence_successors(event);
                return;
            }
            Fence::Escalate => {
                self.escalate_late_effect(event);
                return;
            }
        }
        let already_journaled = matches!(event, SagaChoreographyEvent::SagaStarted { .. })
            && self.gate.journal.is_some()
            && self.gate.take_prejournaled(event.context());
        if !already_journaled
            && let Some(journal) = &self.gate.journal
            && let Err(error) = journal.append(event.clone())
        {
            tracing::error!(
                target: "core::saga",
                event = "terminal_resolver_journal_append_failed",
                saga_type = self.saga_type.as_ref(),
                saga_id = %event.context().saga_id,
                error = ?error
            );
            self.gate.mark_unpersisted(event.context());
            let incoming_quarantine =
                matches!(event, SagaChoreographyEvent::SagaQuarantined { .. });
            let quarantine = if incoming_quarantine {
                event.clone()
            } else {
                SagaChoreographyEvent::SagaQuarantined {
                    context: event.context().next_step(TERMINAL_RESOLVER_STEP.into()),
                    reason: format!("terminal resolver durability failed: {error}").into(),
                    step: TERMINAL_RESOLVER_STEP.into(),
                    participant_id: self.responder.as_ref().into(),
                }
            };
            // Publication/loopback is not the live authority, especially
            // when its own append also fails. Fence both admission and
            // resolver state before any later queued completion/watchdog.
            self.fence_live(quarantine.context());
            let mut outputs = self.resolver.ingest(&quarantine);
            for output in &outputs {
                if matches!(output, SagaChoreographyEvent::SagaQuarantined { .. }) {
                    self.fence_live(output.context());
                }
            }
            if !incoming_quarantine {
                outputs.insert(0, quarantine);
            }
            self.publish_terminal_events(outputs);
            return;
        }
        if !already_journaled && !self.gate.observe(event) {
            let context = event.context();
            let quarantine = SagaChoreographyEvent::SagaQuarantined {
                context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
                reason: "terminal resolver admission evidence capacity unavailable; reconciliation required".into(),
                step: context.step_name.clone(),
                participant_id: "terminal_resolver".into(),
            };
            // Latch the resolver itself and retain the strong fence before
            // replying. Cross-run outputs (an active older run fenced by this
            // unrepresentable newer run) are raised, retained and published too,
            // so a later watchdog cannot emit an ordinary outcome.
            let cross_run = self.resolver.ingest(&quarantine);
            let mut outputs = vec![quarantine.clone()];
            outputs.extend(self.fence_outputs(cross_run));
            self.retain_evidence(&quarantine);
            self.publish_terminal_events(outputs);
            return;
        }
        if !already_journaled
            && matches!(event, SagaChoreographyEvent::SagaQuarantined { .. })
            && self.gate.journal.is_some()
        {
            // Appended successfully above: this exact run's quarantine is durable.
            self.gate.mark_quarantine_durable(event.context());
        }
        let outputs = self.resolver.ingest(event);
        self.publish_terminal_events(outputs);
    }
}

#[derive(Clone)]
pub struct SagaChoreographyBus {
    inner: Arc<BusInner>,
}

struct Publication {
    event: SagaChoreographyEvent,
    delivered_steps: Mutex<DeliveryReceipt>,
}

#[derive(Default)]
struct DeliveryReceipt {
    steps: HashSet<Box<str>>,
    attempted: u32,
    delivered: u32,
    overflowed: bool,
    resolver_selected: bool,
}
impl DeliveryReceipt {
    fn record(&mut self, step: &str, delivered: bool) {
        match self.attempted.checked_add(1) {
            Some(count) => self.attempted = count,
            None => self.overflowed = true,
        }
        if delivered {
            match self.delivered.checked_add(1) {
                Some(count) => self.delivered = count,
                None => self.overflowed = true,
            }
            self.steps.insert(step.into());
        }
    }
}

#[derive(Default)]
struct WatchdogControl {
    stopped: bool,
    generation: u64,
}

struct BusInner {
    bus: FirehosePubSub<Publication>,
    state: Arc<RwLock<BusState>>,
    release: Arc<ReleaseLatch>,
    watchdog: Arc<(Mutex<WatchdogControl>, Condvar)>,
    watchdog_started: AtomicBool,
    // Last: signal only after transport callbacks have been destroyed.
    _release: ReleaseGuard,
}

impl Drop for BusInner {
    fn drop(&mut self) {
        let (stop, wake) = &*self.watchdog;
        stop.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stopped = true;
        wake.notify_all();
        let (runtimes, replies, run_replies) = {
            let mut state = self
                .state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                std::mem::take(&mut state.runtimes),
                state.pending_replies.drain(),
                state.pending_run_replies.drain(),
            )
        };
        for (_, runtime) in runtimes {
            self.bus.unsubscribe(runtime.subscription);
            shutdown_resolver_handle(&runtime.handle);
        }
        for (_, reply) in replies {
            let _ = reply.reply(Err("saga bus dropped".into()));
        }
        for (_, reply) in run_replies {
            let _ = reply.reply(Err("saga bus dropped".into()));
        }
    }
}

const DEFAULT_TERMINAL_RETENTION_LIMIT: usize = 1024;
const DEFAULT_TERMINAL_WATCHDOG_TICK_MS: u64 = 100;
const RESOLVER_MAILBOX_CAPACITY: usize = 1024;
const DEFAULT_WAITER_CAPACITY: usize = 1 << 18;
const DEFAULT_WAITER_TTL: Duration = Duration::from_secs(300);

fn same_policy(a: &TerminalPolicy, b: &TerminalPolicy) -> bool {
    use crate::{FailureAuthority as F, SuccessCriteria as S};
    let authority = match (&a.failure_authority, &b.failure_authority) {
        (F::AnyParticipant, F::AnyParticipant) => true,
        (F::OnlySteps(a), F::OnlySteps(b)) | (F::DenySteps(a), F::DenySteps(b)) => a == b,
        _ => false,
    };
    let criteria = match (&a.success_criteria, &b.success_criteria) {
        (S::AllOf(a), S::AllOf(b)) | (S::AnyOf(a), S::AnyOf(b)) => a == b,
        (
            S::Quorum {
                group_steps: a,
                required_count: ac,
            },
            S::Quorum {
                group_steps: b,
                required_count: bc,
            },
        ) => a == b && ac == bc,
        _ => false,
    };
    a.saga_type == b.saga_type
        && a.policy_id == b.policy_id
        && authority
        && criteria
        && a.overall_timeout == b.overall_timeout
        && a.stalled_timeout == b.stalled_timeout
        && a.workflow_steps == b.workflow_steps
}

fn required_path_description(
    steps: &[SagaWorkflowStepContract],
    required_path_steps: &HashSet<Box<str>>,
) -> String {
    let mut labels = Vec::new();
    for step in steps {
        if required_path_steps.contains(step.step_name) {
            labels.push(format!("{}({})", step.step_name, step.participant_id));
        }
    }
    labels.sort_unstable();
    labels.join(",")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SagaBusPublishError {
    WaiterRegistryFull,
    DuplicateWaiter,
    AdmissionRejected {
        saga_id: SagaId,
        saga_type: Box<str>,
        reason: Box<str>,
    },
    PartialDelivery {
        saga_id: SagaId,
        saga_type: Box<str>,
        step_name: Box<str>,
        attempted: u32,
        delivered: u32,
    },
    TerminalEscalationPartialDelivery {
        saga_id: SagaId,
        saga_type: Box<str>,
        attempted: u32,
        delivered: u32,
    },
    RequiredPathDeliveryShortfall {
        saga_id: SagaId,
        saga_type: Box<str>,
        step_name: Box<str>,
        event_type: &'static str,
        attempted: u32,
        delivered: u32,
        required_min_delivered: u32,
        required_path: Box<str>,
    },
}

impl SagaChoreographyBus {
    pub fn new() -> Self {
        Self::with_waiter_ttl(DEFAULT_WAITER_TTL)
    }

    /// Creates a bus with a nonzero terminal-waiter TTL (default: 300 seconds).
    /// Expired handles are lazily swept and canceled, not replied to as failures.
    pub fn with_waiter_ttl(ttl: Duration) -> Self {
        assert!(!ttl.is_zero(), "waiter TTL must be nonzero");
        let release = Arc::new(ReleaseLatch::default());
        Self {
            inner: Arc::new(BusInner {
                bus: FirehosePubSub::new(),
                state: Arc::new(RwLock::new(BusState {
                    pending_replies: CorrelationRegistry::with_ttl(DEFAULT_WAITER_CAPACITY, ttl),
                    pending_run_replies: CorrelationRegistry::with_ttl(
                        DEFAULT_WAITER_CAPACITY,
                        ttl,
                    ),
                    ..BusState::default()
                })),
                watchdog: Arc::new((Mutex::new(WatchdogControl::default()), Condvar::new())),
                watchdog_started: AtomicBool::new(false),
                _release: release.guard(),
                release,
            }),
        }
    }

    fn read_state(&self) -> std::sync::RwLockReadGuard<'_, BusState> {
        self.inner
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn write_state(&self) -> std::sync::RwLockWriteGuard<'_, BusState> {
        self.inner
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn notify_watchdog(&self) {
        let (control, wake) = &*self.inner.watchdog;
        let mut control = control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        control.generation = control.generation.wrapping_add(1);
        wake.notify_all();
    }
    fn start_watchdog(&self) -> Result<(), String> {
        self.notify_watchdog();
        if self.inner.watchdog_started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let weak = Arc::downgrade(&self.inner);
        let control = Arc::clone(&self.inner.watchdog);
        if let Err(error) = thread::Builder::new()
            .name("saga-terminal-watchdog".into())
            .spawn(move || {
                let (signal, wake) = &*control;
                loop {
                    let generation = {
                        let signal = signal
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if signal.stopped {
                            break;
                        }
                        signal.generation
                    };
                    let Some(inner) = weak.upgrade() else {
                        break;
                    };
                    let bus = SagaChoreographyBus { inner };
                    bus.sweep_waiters();
                    let (refs, has_waiters) = {
                        let state = bus.read_state();
                        let refs: Vec<_> = state
                            .runtimes
                            .values()
                            .filter(|r| {
                                let index = r.gate.index();
                                !index.pending_durable_quarantine.is_empty()
                                    || index.ids.values().any(|id| {
                                        id.runs.iter().any(|run| run.phase == RunPhase::Active)
                                    })
                            })
                            .map(|r| r.resolver_ref.clone())
                            .collect();
                        (
                            refs,
                            !state.pending_replies.is_empty()
                                || !state.pending_run_replies.is_empty(),
                        )
                    };
                    let has_work = has_waiters || !refs.is_empty();
                    for resolver in refs {
                        let _ = resolver.tell(TerminalResolverTell::PollTimeouts);
                    }
                    // No strong bus owner, resolver or journal survives a wait.
                    drop(bus);
                    let guard = signal
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if guard.stopped {
                        break;
                    }
                    if guard.generation != generation {
                        continue;
                    }
                    if has_work {
                        drop(
                            wake.wait_timeout_while(
                                guard,
                                terminal_watchdog_tick_interval(),
                                |signal| !signal.stopped && signal.generation == generation,
                            )
                            .unwrap_or_else(std::sync::PoisonError::into_inner),
                        );
                    } else {
                        drop(
                            wake.wait_while(guard, |signal| {
                                !signal.stopped && signal.generation == generation
                            })
                            .unwrap_or_else(std::sync::PoisonError::into_inner),
                        );
                    }
                }
            })
        {
            self.inner.watchdog_started.store(false, Ordering::Release);
            return Err(format!("terminal watchdog spawn failed: {error}"));
        }
        Ok(())
    }

    /// Returns a waiter that observes actual release of this bus's actors and
    /// resources after its last public owner is dropped. See
    /// [`SagaBusReleaseWaiter::wait_timeout`]. The global bus never releases.
    pub fn release_waiter(&self) -> SagaBusReleaseWaiter {
        SagaBusReleaseWaiter {
            latch: Arc::clone(&self.inner.release),
        }
    }

    pub fn subscribe_fn<F>(&self, topic: &str, f: F) -> FirehoseSubscription
    where
        F: Fn(&SagaChoreographyEvent) -> bool + Send + Sync + 'static,
    {
        self.inner
            .bus
            .subscribe_checked_fn(topic, move |publication| f(&publication.event))
            .unwrap_or_else(|err| panic!("invalid saga topic '{topic}': {err}"))
    }

    pub fn unsubscribe(&self, sub: FirehoseSubscription) -> bool {
        self.write_state().step_subscriptions.remove(&sub);
        self.inner.bus.unsubscribe(sub)
    }

    fn publish_event(&self, event: SagaChoreographyEvent) -> PublishStats {
        let saga_type = event.context().saga_type.clone();
        self.publish_to_saga_type(saga_type.as_ref(), event)
    }

    pub fn publish(&self, event: SagaChoreographyEvent) -> PublishStats {
        self.publish_with_admission(event).0
    }

    /// Publishes `event`; the second value is the rejection reason when a
    /// `SagaStarted` was refused and replaced by a diagnostic `SagaFailed`.
    fn publish_with_admission(
        &self,
        event: SagaChoreographyEvent,
    ) -> (PublishStats, Option<SagaBusPublishError>) {
        self.sweep_waiters();
        if let SagaChoreographyEvent::SagaStarted { context, .. } = &event {
            let (gate, detached, violation) = self.start_configuration(context);
            let admission = match &gate {
                Some(gate) => gate.admit_start(&event, context),
                None if detached => Err(Refusal {
                    reason: format!(
                        "terminal resolver is detached; saga_type={}",
                        context.saga_type
                    )
                    .into(),
                    reject_run_waiter: true,
                    reject_id_waiter: true,
                }),
                None => Ok(()),
            };
            if let Err(refusal) = admission {
                if refusal.reject_run_waiter {
                    self.reject_terminal_reply_for_run(context, refusal.reason.to_string());
                }
                if refusal.reject_id_waiter {
                    self.resolve_id_waiter(
                        context.saga_id,
                        Some(context),
                        Err(refusal.reason.to_string()),
                    );
                }
                return (
                    PublishStats::default(),
                    Some(SagaBusPublishError::AdmissionRejected {
                        saga_id: context.saga_id,
                        saga_type: context.saga_type.clone(),
                        reason: refusal.reason,
                    }),
                );
            }
            self.notify_watchdog();
            self.bind_id_waiter(context);
            self.write_state().note_latest(&RunKey::of(context));
            if let Some(reason) = violation {
                let terminal = SagaChoreographyEvent::SagaFailed {
                    context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
                    reason: reason.clone().into(),
                    failure: None,
                };
                if let Some(gate) = &gate {
                    gate.raise(context, RunPhase::Failed);
                }
                self.store_terminal_outcome(context, terminal.terminal_outcome().unwrap());
                self.resolve_run_waiters(context, Err(reason.clone()));
                return (
                    self.publish_event(terminal),
                    Some(SagaBusPublishError::AdmissionRejected {
                        saga_id: context.saga_id,
                        saga_type: context.saga_type.clone(),
                        reason: reason.into(),
                    }),
                );
            }
        }
        let topic = event.context().saga_type.clone();
        self.fanout(topic.as_ref(), event)
    }

    pub fn publish_strict(
        &self,
        event: SagaChoreographyEvent,
    ) -> Result<PublishStats, SagaBusPublishError> {
        let (stats, error) = self.publish_with_admission(event);
        match error {
            Some(error) => Err(error),
            None => Ok(stats),
        }
    }

    fn gate(&self, saga_type: &str) -> Option<Arc<ResolverGate>> {
        self.read_state()
            .runtimes
            .get(saga_type)
            .map(|r| Arc::clone(&r.gate))
    }

    fn fanout(
        &self,
        topic: &str,
        event: SagaChoreographyEvent,
    ) -> (PublishStats, Option<SagaBusPublishError>) {
        self.notify_watchdog();
        let context = event.context().clone();
        let (contract, resolver) = {
            let state = self.read_state();
            (
                state
                    .workflow_contracts_by_saga_type
                    .get(context.saga_type.as_ref())
                    .cloned(),
                state
                    .runtimes
                    .get(context.saga_type.as_ref())
                    .map(|r| (r.resolver_ref.clone(), Arc::clone(&r.gate))),
            )
        };
        if let Some(outcome) = event.terminal_outcome() {
            if !matches!(outcome, SagaTerminalOutcome::Quarantined { .. })
                && resolver
                    .as_ref()
                    .is_some_and(|(_, gate)| gate.phase(&context) == Some(RunPhase::Quarantined))
            {
                return (PublishStats::default(), None);
            }
            self.store_terminal_outcome(&context, outcome);
        }
        let publication = Publication {
            event,
            delivered_steps: Mutex::default(),
        };
        let mut stats = self
            .inner
            .bus
            .publish_checked(topic, &publication)
            .unwrap_or_else(|error| panic!("invalid saga topic: {error}"));
        let receipt = publication
            .delivered_steps
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut error = None;
        if publication.event.terminal_outcome().is_none()
            && let Some(contract) = contract
            && (receipt.overflowed
                || !crate::workflow_contract::workflow_satisfiable(
                    contract.steps,
                    &contract.criteria,
                    &receipt.steps,
                ))
        {
            let required_min_delivered = u32::try_from(contract.required_path_steps.len())
                .ok()
                .and_then(|n| n.checked_add(1));
            let required_min_delivered = required_min_delivered.unwrap_or(u32::MAX);
            let reason = format!(
                "required_path_delivery_shortfall: saga_type={} event_type={} step={} delivered={} attempted={} required_min_delivered={} required_path={}",
                context.saga_type,
                publication.event.event_type(),
                context.step_name,
                stats.delivered,
                stats.attempted,
                required_min_delivered,
                contract.required_path_description
            );
            self.publish_delivery_quarantine(&context, reason);
            error = Some(SagaBusPublishError::RequiredPathDeliveryShortfall {
                saga_id: context.saga_id,
                saga_type: context.saga_type.clone(),
                step_name: context.step_name.clone(),
                event_type: publication.event.event_type(),
                attempted: stats.attempted,
                delivered: stats.delivered,
                required_min_delivered,
                required_path: contract.required_path_description,
            });
        }
        if let Some((resolver, _)) = resolver {
            let delivered = receipt.resolver_selected
                && resolver.tell(TerminalResolverTell::Ingest(Box::new(publication.event)));
            if delivered && let Some(count) = stats.delivered.checked_add(1) {
                stats.delivered = count;
            } else {
                // Settle before attempting diagnostic fanout. A full mailbox
                // cannot block safety fencing or the gate-owned durable retry.
                let quarantine = self.fence_delivery(
                    &context,
                    "terminal resolver ingress unavailable or delivery count overflow".into(),
                );
                let diagnostic = Publication {
                    event: quarantine,
                    delivered_steps: Mutex::default(),
                };
                let _ = self
                    .inner
                    .bus
                    .publish_checked(context.saga_type.as_ref(), &diagnostic);
                error = Some(SagaBusPublishError::PartialDelivery {
                    saga_id: context.saga_id,
                    saga_type: context.saga_type,
                    step_name: context.step_name,
                    attempted: stats.attempted,
                    delivered: stats.delivered,
                });
            }
        }
        if let Some(SagaBusPublishError::RequiredPathDeliveryShortfall { delivered, .. }) =
            &mut error
        {
            *delivered = stats.delivered;
        }
        (stats, error)
    }

    pub fn register_workflow_contract_provider<C: SagaWorkflowContract>(
        &self,
    ) -> Result<(), String> {
        let policy = C::terminal_policy();
        validate_workflow_contract(C::saga_type(), C::first_step(), C::steps(), &policy)?;

        let mut declared_steps: HashSet<Box<str>> = HashSet::new();
        for step in C::steps() {
            declared_steps.insert(step.step_name.into());
        }
        let required_path_steps =
            required_path_steps_from_success_criteria(C::steps(), &policy.success_criteria);
        let required_path_description = required_path_description(C::steps(), &required_path_steps);
        let contract_state = WorkflowContractState {
            first_step: C::first_step().into(),
            declared_steps: declared_steps.clone(),
            required_path_steps,
            required_path_description: required_path_description.into(),
            steps: C::steps(),
            criteria: policy.success_criteria.clone(),
        };

        let mut state = self.write_state();
        let mut unknown: Vec<_> = state
            .step_subscriptions
            .values()
            .filter(|(ty, step)| ty.as_ref() == C::saga_type() && !declared_steps.contains(step))
            .map(|(_, step)| step.as_ref())
            .collect();
        unknown.sort_unstable();
        unknown.dedup();
        if !unknown.is_empty() {
            return Err(format!(
                "bound step set contains steps not declared by workflow contract: unknown_steps={}",
                unknown.join(",")
            ));
        }
        state
            .workflow_contracts_by_saga_type
            .insert(C::saga_type().into(), contract_state);

        let required = required_steps_from_success_criteria(&policy.success_criteria);
        for step in required {
            if !declared_steps.contains(step.as_ref()) {
                return Err(format!(
                    "workflow contract required terminal step not declared: saga_type={} step={}",
                    C::saga_type(),
                    step
                ));
            }
        }

        Ok(())
    }

    /// Subscribes a live workflow-step role. Only successful role receipts
    /// satisfy the workflow DAG; ordinary observers never supply coverage.
    pub fn subscribe_workflow_step_fn<F>(
        &self,
        saga_type: &str,
        step_name: &str,
        f: F,
    ) -> Result<FirehoseSubscription, String>
    where
        F: Fn(&SagaChoreographyEvent) -> bool + Send + Sync + 'static,
    {
        if saga_type.is_empty() || step_name.is_empty() {
            return Err("saga_type and step_name must be non-empty".into());
        }
        let mut state = self.write_state();
        if state
            .workflow_contracts_by_saga_type
            .get(saga_type)
            .is_some_and(|c| !c.declared_steps.contains(step_name))
        {
            return Err(format!(
                "bound workflow step is not declared by contract: saga_type={saga_type} step={step_name}"
            ));
        }
        let role_name: Box<str> = step_name.into();
        let sub = self
            .inner
            .bus
            .subscribe_checked_fn(saga_type, move |publication| {
                let delivered = f(&publication.event);
                publication
                    .delivered_steps
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .record(role_name.as_ref(), delivered);
                delivered
            })
            .map_err(|e| e.to_string())?;
        state
            .step_subscriptions
            .insert(sub.clone(), (saga_type.into(), step_name.into()));
        Ok(sub)
    }

    pub fn publish_to_saga_type(
        &self,
        saga_type: &str,
        event: SagaChoreographyEvent,
    ) -> PublishStats {
        self.fanout(saga_type, event).0
    }

    pub fn subscribe_saga_type_fn<F>(&self, saga_type: &str, f: F) -> FirehoseSubscription
    where
        F: Fn(&SagaChoreographyEvent) -> bool + Send + Sync + 'static,
    {
        self.subscribe_fn(saga_type, f)
    }

    /// Registers an id-scoped terminal waiter (compatibility API).
    ///
    /// It binds to the run admitted after it registered, or to the unique open
    /// run (one saga type, id and start) already admitted when it registers;
    /// with several open owners it waits for a run that starts afterwards. Stale
    /// terminal history never resolves it. Prefer
    /// [`Self::register_terminal_reply_for_run`].
    pub fn register_terminal_reply(
        &self,
        saga_id: SagaId,
        reply: SagaReplyToHandle,
    ) -> Result<(), SagaBusPublishError> {
        self.sweep_waiters();
        let result = {
            let mut state = self.write_state();
            let run = Self::unique_active_run(&state, saga_id);
            let result = state.pending_replies.register(saga_id, reply);
            if result.is_ok() {
                state.legacy_waiter_floors.insert(
                    saga_id,
                    LegacyWaiter {
                        registered_at_millis: SagaContext::now_millis(),
                        bound_run: run,
                    },
                );
            }
            result
        };
        self.registration_result(result, "saga id")
    }
    pub fn register_terminal_reply_for_run(
        &self,
        context: &SagaContext,
        reply: SagaReplyToHandle,
    ) -> Result<(), SagaBusPublishError> {
        self.sweep_waiters();
        let state = self.write_state();
        let result = state
            .pending_run_replies
            .register(RunKey::of(context), reply);
        drop(state);
        self.registration_result(result, "saga run")
    }
    fn registration_result(
        &self,
        result: Result<(), icanact_core::RegisterWaiterError<SagaReplyToResult>>,
        scope: &str,
    ) -> Result<(), SagaBusPublishError> {
        match result {
            Ok(()) => {
                if let Err(error) = self.start_watchdog() {
                    tracing::error!(target: "core::saga", event = "waiter_watchdog_unavailable", %error);
                }
                Ok(())
            }
            Err(error) => {
                let reason: Box<str> = match &error {
                    icanact_core::RegisterWaiterError::DuplicateKey(_) => {
                        format!("terminal reply already registered for {scope}").into()
                    }
                    icanact_core::RegisterWaiterError::AtCapacity(_) => {
                        "terminal waiter registry full".into()
                    }
                };
                let rejected = if matches!(error, icanact_core::RegisterWaiterError::AtCapacity(_))
                {
                    SagaBusPublishError::WaiterRegistryFull
                } else {
                    SagaBusPublishError::DuplicateWaiter
                };
                let _ = error.into_reply().reply(Err(reason.to_string()));
                Err(rejected)
            }
        }
    }
    fn sweep_waiters(&self) {
        let mut state = self.write_state();
        state.pending_replies.sweep_expired();
        state.pending_run_replies.sweep_expired();
        let registry = state.pending_replies.clone();
        state
            .legacy_waiter_floors
            .retain(|id, _| registry.contains_key(id));
    }
    /// Cancels an id-scoped waiter and removes its legacy run binding.
    pub fn cancel_terminal_reply(&self, saga_id: SagaId) -> bool {
        let reply = {
            let mut state = self.write_state();
            state.legacy_waiter_floors.remove(&saga_id);
            state.pending_replies.take(&saga_id)
        };
        reply.is_some()
    }
    /// Cancels only the waiter for this exact saga run.
    pub fn cancel_terminal_reply_for_run(&self, context: &SagaContext) -> bool {
        self.write_state()
            .pending_run_replies
            .take(&RunKey::of(context))
            .is_some()
    }

    pub fn complete_terminal_reply(&self, saga_id: SagaId, reply: SagaReplyTo) -> bool {
        // Without run identity the reply is stored as the id's newest outcome.
        let Some(reply) = self.store_terminal_reply(
            RunKey {
                saga_type: "".into(),
                saga_id,
                started_at_millis: SagaContext::now_millis(),
            },
            reply,
        ) else {
            return false;
        };
        self.resolve_id_waiter(saga_id, None, Ok(reply))
    }

    pub fn complete_terminal_reply_for_run(
        &self,
        context: &SagaContext,
        reply: SagaReplyTo,
    ) -> bool {
        let Some(reply) = self.store_terminal_reply(RunKey::of(context), reply) else {
            return false;
        };
        self.resolve_run_waiters(context, Ok(reply))
    }

    pub fn reject_terminal_reply(&self, saga_id: SagaId, reason: impl Into<String>) -> bool {
        self.resolve_id_waiter(saga_id, None, Err(reason.into()))
    }

    pub fn reject_terminal_reply_for_run(
        &self,
        context: &SagaContext,
        reason: impl Into<String>,
    ) -> bool {
        let reply = self
            .write_state()
            .pending_run_replies
            .take(&RunKey::of(context));
        reply.is_some_and(|reply| reply.reply(Err(reason.into())).is_ok())
    }

    /// Binds a not-yet-bound id-scoped waiter to the run just admitted.
    fn bind_id_waiter(&self, context: &SagaContext) {
        self.bind_id_waiter_to(context.saga_id, RunKey::of(context));
    }

    fn bind_id_waiter_to(&self, saga_id: SagaId, run: RunKey) {
        if let Some(waiter) = self.write_state().legacy_waiter_floors.get_mut(&saga_id)
            && waiter.bound_run.is_none()
        {
            waiter.bound_run = Some(run);
        }
    }

    /// The single open run (type, id, start) of `saga_id` across every attached
    /// resolver, or `None` when there is none or ownership is ambiguous.
    /// Reads the gates directly, never asking an actor, so it is safe to call
    /// from inside a sync actor.
    fn unique_active_run(state: &BusState, saga_id: SagaId) -> Option<RunKey> {
        let mut found: Option<RunKey> = None;
        for (saga_type, runtime) in &state.runtimes {
            let gate = &runtime.gate;
            for started_at_millis in gate.active_starts(saga_id) {
                if found.is_some() {
                    return None;
                }
                found = Some(RunKey {
                    saga_type: saga_type.clone(),
                    saga_id,
                    started_at_millis,
                });
            }
        }
        found
    }

    /// Resolves the id-scoped waiter. With `run_started_at`, only the run the
    /// waiter belongs to is allowed to satisfy it.
    fn resolve_id_waiter(
        &self,
        saga_id: SagaId,
        context: Option<&SagaContext>,
        result: SagaReplyToResult,
    ) -> bool {
        let reply = {
            let mut state = self.write_state();
            if let (Some(waiter), Some(context)) =
                (state.legacy_waiter_floors.get(&saga_id), context)
                && !waiter.accepts(context)
            {
                return false;
            }
            state.legacy_waiter_floors.remove(&saga_id);
            state.pending_replies.take(&saga_id)
        };
        reply.is_some_and(|reply| reply.reply(result).is_ok())
    }
    fn resolve_run_waiters(&self, context: &SagaContext, result: SagaReplyToResult) -> bool {
        let reply = self
            .write_state()
            .pending_run_replies
            .take(&RunKey::of(context));
        let run = reply.is_some_and(|reply| reply.reply(result.clone()).is_ok());
        let id = self.resolve_id_waiter(context.saga_id, Some(context), result);
        run || id
    }

    /// Use `detach_terminal_resolver` as the supported removal path.
    pub fn attach_terminal_resolver(
        &self,
        policy: TerminalPolicy,
        responder: &'static str,
    ) -> Result<FirehoseSubscription, String> {
        self.attach_terminal_resolver_inner(policy, responder, None, AdmissionLimits::from_env())
    }

    /// Detaches the terminal resolver for `saga_type`: unsubscribes it and stops
    /// its actor (the shared watchdog idles). Returns `false` if none was attached. In-flight
    /// sagas of this type lose terminal resolution; their waiters are not
    /// resolved by detach. A later attach for the same type starts fresh.
    /// New saga starts for this type fail closed until a replacement resolver
    /// is attached and, for durable resolvers, recovery is activated.
    pub fn detach_terminal_resolver(&self, saga_type: &str) -> bool {
        let runtime = {
            let mut state = self.write_state();
            let runtime = state.runtimes.remove(saga_type);
            if runtime.is_some() {
                state.detached.insert(saga_type.into());
                state.terminal_policies_by_saga_type.remove(saga_type);
            }
            runtime
        };
        if let Some(runtime) = runtime {
            self.unsubscribe(runtime.subscription);
            shutdown_resolver_handle(&runtime.handle);
            true
        } else {
            false
        }
    }

    pub fn attach_durable_terminal_resolver<J: TerminalResolverJournal>(
        &self,
        policy: TerminalPolicy,
        responder: &'static str,
        journal: Arc<J>,
    ) -> Result<FirehoseSubscription, String> {
        self.attach_terminal_resolver_inner(
            policy,
            responder,
            Some(journal),
            AdmissionLimits::from_env(),
        )
    }

    fn attach_terminal_resolver_inner(
        &self,
        policy: TerminalPolicy,
        responder: &'static str,
        journal: Option<Arc<dyn TerminalResolverJournal>>,
        limits: AdmissionLimits,
    ) -> Result<FirehoseSubscription, String> {
        let saga_type_topic = policy.saga_type.clone();
        {
            let state = self.read_state();
            if let Some(runtime) = state.runtimes.get(saga_type_topic.as_ref()) {
                if runtime.responder == responder
                    && same_policy(&runtime.policy, &policy)
                    && match (&runtime.gate.journal, &journal) {
                        (None, None) => true,
                        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                        _ => false,
                    }
                {
                    return Ok(runtime.subscription.clone());
                }
                return Err("conflicting terminal resolver attachment".into());
            }
        }
        let responder: Arc<str> = Arc::from(responder);
        let (resolver, recovery_events, admission, recovery_history) = match &journal {
            Some(journal) => {
                let mut entries = Vec::new();
                match journal.visit_unresolved_runs(&mut |entry| {
                    entries.push(entry.clone());
                    Ok(())
                }) {
                    Ok(()) => {}
                    Err(crate::TerminalResolverJournalError::Unsupported(_)) => {
                        // Compatibility for append/read_all-only custom journals.
                        // Discard any partial visit before taking the legacy path.
                        entries = journal.read_all().map_err(|error| {
                            format!(
                                "terminal resolver recovery read failed saga_type={}: {error}",
                                policy.saga_type
                            )
                        })?;
                    }
                    Err(error) => {
                        return Err(format!(
                            "terminal resolver recovery stream failed saga_type={}: {error}",
                            policy.saga_type
                        ));
                    }
                }
                // Stores may stream in run-index order; cross-run quarantine
                // reconstruction requires the original durable sequence order.
                entries.sort_by_key(|entry| entry.sequence);
                let mut events = Vec::with_capacity(entries.len());
                for entry in entries {
                    if entry.event.context().saga_type.as_ref() != policy.saga_type.as_ref() {
                        return Err(format!(
                            "terminal resolver journal saga_type mismatch: expected={} actual={} sequence={}",
                            policy.saga_type,
                            entry.event.context().saga_type,
                            entry.sequence
                        ));
                    }
                    events.push(entry.event);
                }
                // Only unresolved/quarantined evidence is resident for streaming
                // stores. Closed fences are loaded by bounded per-saga lookup.
                let admission = AdmissionIndex::from_events(
                    limits,
                    journal.supports_saga_lookup(),
                    events.iter(),
                );
                if admission.exceeds_capacity() {
                    return Err("terminal resolver recovery exceeds admission capacity; increase SAGA_ADMISSION_CAPACITY or maintain resolved journal detail without deleting fences".into());
                }
                // Admission used the complete history. Closed detail can already
                // be gapped even if an ordinary failure later became quarantine.
                // Never re-derive work or success from it. Keep actual quarantine
                // rows in order: they seed the original fence and must still
                // quarantine an already-active successor during reconstruction.
                let phases = closed_run_phases(&events);
                events.retain(|event| {
                    let context = event.context();
                    match phases.get(&(context.saga_id, context.saga_started_at_millis)) {
                        Some(RunPhase::Completed | RunPhase::Failed) => false,
                        Some(RunPhase::Quarantined) => {
                            matches!(event, SagaChoreographyEvent::SagaQuarantined { .. })
                        }
                        _ => true,
                    }
                });
                let (resolver, recovery_events) =
                    TerminalResolver::restore_from_events(policy.clone(), &events);
                (resolver, recovery_events, admission, events)
            }
            None => (
                TerminalResolver::new(policy.clone()),
                Vec::new(),
                AdmissionIndex {
                    ephemeral: true,
                    ..AdmissionIndex::new(limits, false)
                },
                Vec::new(),
            ),
        };
        let durable = journal.is_some();
        let gate = Arc::new(ResolverGate {
            journal,
            _release: self.inner.release.guard(),
            activated: AtomicBool::new(!durable),
            admission: Mutex::new(admission),
        });
        let mut resolver_actor = TerminalResolverActor {
            resolver,
            _release: self.inner.release.guard(),
            recovery_events,
            held_events: Vec::new(),
            held_quarantine_replies: HashMap::new(),
            activated: !durable,
            gate: Arc::clone(&gate),
            bus: Arc::downgrade(&self.inner),
            responder: Arc::clone(&responder),
            saga_type: saga_type_topic.clone(),
        };
        resolver_actor.recover_successor_fences(&recovery_history);
        // Finish fallible setup before spawning an actor that needs explicit shutdown.
        self.start_watchdog()?;
        // Resolver ingress is phase two, never an ordinary observer callback.
        let subscription = self
            .inner
            .bus
            .subscribe_checked_fn(saga_type_topic.as_ref(), |publication| {
                publication
                    .delivered_steps
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .resolver_selected = true;
                // Phase one only selects ingress; delivery is counted after enqueue.
                false
            })
            .map_err(|error| error.to_string())?;
        let (resolver_ref, resolver_handle) = local_sync::spawn_with_opts(
            resolver_actor,
            local_sync::SpawnOpts {
                mailbox_capacity: RESOLVER_MAILBOX_CAPACITY,
                ..Default::default()
            },
        );
        let resolver_handle = Arc::new(Mutex::new(Some(resolver_handle)));
        let runtime = TerminalResolverRuntime {
            gate,
            subscription: subscription.clone(),
            resolver_ref,
            handle: resolver_handle,
            policy: policy.clone(),
            responder: responder.to_string(),
        };
        let loser = {
            let mut state = self.write_state();
            if state.runtimes.contains_key(saga_type_topic.as_ref()) {
                Some(runtime)
            } else {
                state
                    .terminal_policies_by_saga_type
                    .insert(saga_type_topic.clone(), policy.policy_id.clone());
                state.detached.remove(saga_type_topic.as_ref());
                state.runtimes.insert(saga_type_topic.clone(), runtime);
                None
            }
        };
        if let Some(loser) = loser {
            self.unsubscribe(loser.subscription);
            shutdown_resolver_handle(&loser.handle);
            let state = self.read_state();
            let winner = state
                .runtimes
                .get(saga_type_topic.as_ref())
                .ok_or("terminal resolver detached during attachment")?;
            if winner.responder == loser.responder
                && same_policy(&winner.policy, &loser.policy)
                && match (&winner.gate.journal, &loser.gate.journal) {
                    (None, None) => true,
                    (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                    _ => false,
                }
            {
                return Ok(winner.subscription.clone());
            }
            return Err("conflicting terminal resolver attachment".into());
        }
        Ok(subscription)
    }

    pub fn attach_terminal_resolver_for_contract<C: SagaWorkflowContract>(
        &self,
        responder: &'static str,
    ) -> Result<FirehoseSubscription, String> {
        self.attach_terminal_resolver(C::terminal_policy(), responder)
    }

    pub fn attach_durable_terminal_resolver_for_contract<
        C: SagaWorkflowContract,
        J: TerminalResolverJournal,
    >(
        &self,
        responder: &'static str,
        journal: Arc<J>,
    ) -> Result<FirehoseSubscription, String> {
        self.attach_durable_terminal_resolver(C::terminal_policy(), responder, journal)
    }

    pub fn activate_terminal_resolver_recovery(&self, saga_type: &str) -> Result<(), String> {
        let state = self.read_state();
        let runtime = state
            .runtimes
            .get(saga_type)
            .ok_or_else(|| format!("terminal resolver not attached: {saga_type}"))?;
        if runtime
            .resolver_ref
            .tell(TerminalResolverTell::ActivateRecovery)
        {
            runtime.gate.activated.store(true, Ordering::Release);
            Ok(())
        } else {
            Err(format!(
                "terminal resolver recovery activation failed saga_type={saga_type}"
            ))
        }
    }

    pub fn activate_terminal_resolver_recovery_for_contract<C: SagaWorkflowContract>(
        &self,
    ) -> Result<(), String> {
        self.activate_terminal_resolver_recovery(C::saga_type())
    }

    /// Newest stored terminal reply for the id (compatibility API).
    pub fn take_terminal_reply(&self, saga_id: SagaId) -> Option<SagaReplyTo> {
        let mut state = self.write_state();
        let run = state.latest_run_by_id.get(&saga_id)?.clone();
        state.take_reply(&run)
    }
    pub fn take_terminal_outcome(&self, saga_id: SagaId) -> Option<SagaTerminalOutcome> {
        let mut state = self.write_state();
        let run = state.latest_run_by_id.get(&saga_id)?.clone();
        state.take_outcome(&run)
    }
    pub fn take_terminal_reply_for_run(&self, context: &SagaContext) -> Option<SagaReplyTo> {
        self.write_state().take_reply(&RunKey::of(context))
    }
    pub fn take_terminal_outcome_for_run(
        &self,
        context: &SagaContext,
    ) -> Option<SagaTerminalOutcome> {
        self.write_state().take_outcome(&RunKey::of(context))
    }

    fn complete_terminal_reply_from_event(
        &self,
        event: &SagaChoreographyEvent,
        responder: impl Into<Box<str>>,
    ) -> bool {
        let is_resolver_terminal = match event {
            SagaChoreographyEvent::SagaCompleted { context }
            | SagaChoreographyEvent::SagaFailed { context, .. }
            | SagaChoreographyEvent::SagaQuarantined { context, .. } => {
                context.step_name.as_ref() == TERMINAL_RESOLVER_STEP
            }
            _ => false,
        };
        if !is_resolver_terminal {
            return false;
        }
        self.resolve_terminal_reply(event, responder)
    }

    /// Quarantine of a run the resolver already owns, raised by anyone (a
    /// participant, the delivery-shortfall path, ...). Only quarantine is
    /// accepted here: an ordinary terminal still needs resolver authority.
    /// Quarantine dominates, and waiters resolve by exact run binding.
    fn complete_quarantine_reply_from_event(
        &self,
        event: &SagaChoreographyEvent,
        responder: impl Into<Box<str>>,
    ) -> bool {
        if !matches!(event, SagaChoreographyEvent::SagaQuarantined { .. }) {
            return false;
        }
        self.resolve_terminal_reply(event, responder)
    }

    fn resolve_terminal_reply(
        &self,
        event: &SagaChoreographyEvent,
        responder: impl Into<Box<str>>,
    ) -> bool {
        let Some(outcome) = event.terminal_outcome() else {
            return false;
        };
        let reply = SagaReplyTo {
            responder: responder.into(),
            outcome,
        };
        // Commit the effective (quarantine-dominant) result before waking anyone.
        // All callbacks run after the state guard has been released.
        let Some(reply) = self.store_terminal_reply(RunKey::of(event.context()), reply) else {
            return false;
        };
        self.resolve_run_waiters(event.context(), Ok(reply))
    }

    fn fence_delivery(&self, context: &SagaContext, reason: String) -> SagaChoreographyEvent {
        if let Some(gate) = self.gate(context.saga_type.as_ref()) {
            gate.raise(context, RunPhase::Quarantined);
        }
        let quarantine = SagaChoreographyEvent::SagaQuarantined {
            context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
            reason: reason.into(),
            step: context.step_name.clone(),
            participant_id: TERMINAL_RESOLVER_STEP.into(),
        };
        self.notify_watchdog();
        self.complete_quarantine_reply_from_event(&quarantine, TERMINAL_RESOLVER_STEP);
        quarantine
    }
    fn publish_delivery_quarantine(&self, context: &SagaContext, reason: String) -> PublishStats {
        let quarantine = self.fence_delivery(context, reason);
        self.publish_event(quarantine)
    }

    fn terminal_retention_limit(&self) -> usize {
        static LIMIT: OnceLock<usize> = OnceLock::new();
        *LIMIT.get_or_init(|| match std::env::var("SAGA_TERMINAL_RETENTION_LIMIT") {
            Ok(raw) => match raw.parse::<usize>() {
                Ok(value) if value > 0 => value,
                Ok(_value) => DEFAULT_TERMINAL_RETENTION_LIMIT,
                Err(err) => {
                    tracing::error!(
                        target: "core::saga",
                        event = "saga_terminal_retention_limit_parse_failed",
                        env = "SAGA_TERMINAL_RETENTION_LIMIT",
                        value = %raw,
                        error = %err
                    );
                    DEFAULT_TERMINAL_RETENTION_LIMIT
                }
            },
            Err(_) => DEFAULT_TERMINAL_RETENTION_LIMIT,
        })
    }

    fn start_configuration(
        &self,
        context: &SagaContext,
    ) -> (Option<Arc<ResolverGate>>, bool, Option<String>) {
        let state = self.read_state();
        let saga_type = context.saga_type.as_ref();
        let gate = state
            .runtimes
            .get(saga_type)
            .map(|runtime| Arc::clone(&runtime.gate));
        let detached = state.detached.contains(saga_type);
        let violation = if !state.terminal_policies_by_saga_type.contains_key(saga_type) {
            Some(format!(
                "terminal policy is required before saga start; saga_type={} saga_id={}",
                context.saga_type,
                context.saga_id.get()
            ))
        } else {
            Self::contract_violation_reason(&state, context)
        };
        (gate, detached, violation)
    }
    fn contract_violation_reason(state: &BusState, context: &SagaContext) -> Option<String> {
        let saga_type = context.saga_type.as_ref();
        let Some(contract) = state.workflow_contracts_by_saga_type.get(saga_type) else {
            return Some(format!(
                "workflow contract is required before saga start; saga_type={saga_type} saga_id={}",
                context.saga_id.get()
            ));
        };
        if context.step_name.as_ref() != contract.first_step.as_ref() {
            return Some(format!(
                "workflow contract first_step mismatch at saga start; saga_type={saga_type} expected_first_step={} received_first_step={}",
                contract.first_step, context.step_name
            ));
        }
        let live = state
            .step_subscriptions
            .values()
            .filter(|(ty, _)| ty.as_ref() == saga_type)
            .map(|(_, step)| step.clone())
            .collect();
        if !crate::workflow_contract::workflow_satisfiable(
            contract.steps,
            &contract.criteria,
            &live,
        ) {
            let mut missing: Vec<_> = contract
                .declared_steps
                .iter()
                .filter(|s| !live.contains(*s))
                .map(|s| s.as_ref())
                .collect();
            missing.sort_unstable();
            return Some(format!(
                "workflow contract violation: unbound participant steps; saga_type={saga_type} missing_steps={}",
                missing.join(",")
            ));
        }
        None
    }
    fn store_terminal_reply(&self, run: RunKey, mut reply: SagaReplyTo) -> Option<SagaReplyTo> {
        let mut state = self.write_state();
        reply.outcome = state.insert_terminal_outcome(
            run.clone(),
            reply.outcome,
            self.terminal_retention_limit(),
        )?;
        if matches!(reply.outcome, SagaTerminalOutcome::Quarantined { .. }) {
            state.terminal_replies.insert(run, reply.clone());
        } else {
            state
                .terminal_replies
                .entry(run)
                .or_insert_with(|| reply.clone());
        }
        Some(reply)
    }
    fn store_terminal_outcome(&self, context: &SagaContext, outcome: SagaTerminalOutcome) {
        self.write_state().insert_terminal_outcome(
            RunKey::of(context),
            outcome,
            self.terminal_retention_limit(),
        );
    }
}

/// Final durable phases of runs in this one saga type's already-filtered history.
/// Quarantine dominates and the first ordinary terminal is never replaced.
fn closed_run_phases(events: &[SagaChoreographyEvent]) -> HashMap<(SagaId, u64), RunPhase> {
    let mut phases: HashMap<(SagaId, u64), RunPhase> = HashMap::new();
    for event in events {
        let phase = match event {
            SagaChoreographyEvent::SagaCompleted { .. } => RunPhase::Completed,
            SagaChoreographyEvent::SagaFailed { .. } => RunPhase::Failed,
            SagaChoreographyEvent::SagaQuarantined { .. } => RunPhase::Quarantined,
            _ => RunPhase::Active,
        };
        let context = event.context();
        let slot = phases
            .entry((context.saga_id, context.saga_started_at_millis))
            .or_insert(RunPhase::Active);
        *slot = slot.merge(phase);
    }
    phases
}

fn terminal_watchdog_tick_interval() -> Duration {
    static INTERVAL: OnceLock<Duration> = OnceLock::new();
    *INTERVAL.get_or_init(|| match std::env::var("SAGA_TERMINAL_WATCHDOG_TICK_MS") {
        Ok(raw) => match raw.parse::<u64>() {
            Ok(value) if value > 0 => Duration::from_millis(value),
            Ok(_value) => Duration::from_millis(DEFAULT_TERMINAL_WATCHDOG_TICK_MS),
            Err(err) => {
                tracing::error!(
                    target: "core::saga",
                    event = "saga_terminal_watchdog_tick_parse_failed",
                    env = "SAGA_TERMINAL_WATCHDOG_TICK_MS",
                    value = %raw,
                    error = %err
                );
                Duration::from_millis(DEFAULT_TERMINAL_WATCHDOG_TICK_MS)
            }
        },
        Err(_) => Duration::from_millis(DEFAULT_TERMINAL_WATCHDOG_TICK_MS),
    })
}

pub fn global_saga_choreography_bus() -> SagaChoreographyBus {
    static BUS: OnceLock<SagaChoreographyBus> = OnceLock::new();
    BUS.get_or_init(SagaChoreographyBus::new).clone()
}

impl Default for SagaChoreographyBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    use icanact_core::local_sync;

    use crate::{
        AcceptedStepTimeoutOutcome, FailureAuthority, InMemoryTerminalResolverJournal,
        SagaChoreographyEvent, SagaContext, SagaId, SagaReplyToResult, SagaTerminalOutcome,
        SagaWorkflowContract, SagaWorkflowStepContract, StepExecutionId, SuccessCriteria,
        TERMINAL_RESOLVER_STEP, TerminalPolicy, TerminalResolverJournal,
        TerminalResolverJournalEntry, TerminalResolverJournalError, WorkflowDependencySpec,
    };

    use super::{DEFAULT_TERMINAL_RETENTION_LIMIT, SagaChoreographyBus};

    fn context_for(saga_type: &str, step_name: &str, saga_id: u64) -> SagaContext {
        let now = SagaContext::now_millis();
        SagaContext {
            saga_id: SagaId::new(saga_id),
            saga_type: saga_type.into(),
            step_name: step_name.into(),
            correlation_id: saga_id,
            causation_id: saga_id,
            trace_id: saga_id,
            step_index: 0,
            attempt: 0,
            initiator_peer_id: [0; 32],
            saga_started_at_millis: now,
            event_timestamp_millis: now,
        }
    }

    fn context(step_name: &str, saga_id: u64) -> SagaContext {
        context_for("order_lifecycle", step_name, saga_id)
    }

    fn wait_until(deadline: Instant, mut pred: impl FnMut() -> bool) {
        while Instant::now() < deadline {
            if pred() {
                return;
            }
            std::hint::spin_loop();
            thread::yield_now();
        }
        assert!(pred(), "condition not met before deadline");
    }

    enum ProbeMsg {
        Register {
            bus: SagaChoreographyBus,
            saga_id: SagaId,
            reply: super::SagaReplyToHandle,
        },
    }

    fn register_pending_reply(
        bus: SagaChoreographyBus,
        saga_id: SagaId,
    ) -> (
        local_sync::PendingAsk<SagaReplyToResult>,
        local_sync::mpsc::ActorHandle,
    ) {
        let (registered_tx, registered_rx) = std::sync::mpsc::channel();
        let (probe_addr, probe_handle) =
            local_sync::mpsc::spawn(8, move |msg: ProbeMsg| match msg {
                ProbeMsg::Register {
                    bus,
                    saga_id,
                    reply,
                } => {
                    let _ = bus.register_terminal_reply(saga_id, reply);
                    registered_tx.send(()).unwrap();
                }
            });
        let pending = probe_addr
            .ask_delegated(|reply| ProbeMsg::Register {
                bus,
                saga_id,
                reply,
            })
            .expect("pending reply should be registered");
        registered_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("registration barrier");
        (pending, probe_handle)
    }

    #[test]
    fn invalid_start_settles_waiters_without_diagnostic_delivery() {
        let bus = SagaChoreographyBus::new();
        let id = SagaId::new(991001);
        let (pending, handle) = register_pending_reply(bus.clone(), id);
        bus.publish(SagaChoreographyEvent::SagaStarted {
            context: context("create_order", id.get()),
            payload: Vec::new(),
        });
        assert!(matches!(
            pending.wait_timeout(Duration::from_millis(200)),
            Ok(Err(_))
        ));
        handle.shutdown();
    }

    #[derive(Default)]
    struct SlowTerminalResolverJournal(InMemoryTerminalResolverJournal);

    impl TerminalResolverJournal for SlowTerminalResolverJournal {
        fn append(
            &self,
            event: SagaChoreographyEvent,
        ) -> Result<u64, TerminalResolverJournalError> {
            thread::sleep(Duration::from_millis(2));
            self.0.append(event)
        }

        fn read_all(
            &self,
        ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
            self.0.read_all()
        }
    }

    #[test]
    fn ingest_burst_never_counts_dropped_events_as_delivered() {
        let bus = SagaChoreographyBus::new();
        bus.attach_durable_terminal_resolver(
            TerminalPolicy::order_lifecycle_default(),
            "burst-test",
            Arc::new(SlowTerminalResolverJournal::default()),
        )
        .unwrap();
        bus.activate_terminal_resolver_recovery("order_lifecycle")
            .unwrap();
        let mut publishes = Vec::new();
        for id in 10000..10300 {
            let stats = bus.publish(SagaChoreographyEvent::StepCompleted {
                context: context("create_order", id),
                output: Vec::new(),
                saga_input: Vec::new(),
                compensation_available: false,
            });
            publishes.push((SagaId::new(id), stats));
        }
        let mut fully_delivered: HashSet<_> = publishes
            .into_iter()
            .filter_map(|(id, stats)| (stats.delivered == stats.attempted).then_some(id))
            .collect();
        assert!(!fully_delivered.is_empty());
        let expected = fully_delivered.len();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !fully_delivered.is_empty() && Instant::now() < deadline {
            fully_delivered.retain(|id| {
                !matches!(
                    bus.take_terminal_outcome(*id),
                    Some(SagaTerminalOutcome::Completed { .. })
                )
            });
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            fully_delivered.is_empty(),
            "{} of {expected} fully delivered sagas never completed",
            fully_delivered.len()
        );
    }

    #[test]
    fn detach_then_reattach_restores_resolver_processing() {
        let bus = SagaChoreographyBus::new();
        bus.attach_terminal_resolver(TerminalPolicy::order_lifecycle_default(), "detach-test")
            .unwrap();
        assert!(bus.detach_terminal_resolver("order_lifecycle"));
        assert!(!bus.detach_terminal_resolver("order_lifecycle"));
        let publish_step = |id| {
            bus.publish(SagaChoreographyEvent::StepCompleted {
                context: context("create_order", id),
                output: Vec::new(),
                saga_input: Vec::new(),
                compensation_available: false,
            })
        };
        publish_step(9001);
        thread::sleep(Duration::from_millis(100));
        assert!(bus.take_terminal_outcome(SagaId::new(9001)).is_none());
        bus.attach_terminal_resolver(TerminalPolicy::order_lifecycle_default(), "reattach-test")
            .unwrap();
        publish_step(9002);
        wait_until(Instant::now() + Duration::from_secs(3), || {
            matches!(
                bus.take_terminal_outcome(SagaId::new(9002)),
                Some(SagaTerminalOutcome::Completed { .. })
            )
        });
    }

    #[test]
    fn detached_durable_resolver_rejects_starts_until_recovery_is_reactivated() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<OrderLifecycleContract>()
            .unwrap();
        bus.subscribe_workflow_step_fn("order_lifecycle", "create_order", |_| true)
            .unwrap();
        let effects = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&effects);
        let _participant = bus.subscribe_saga_type_fn("order_lifecycle", move |event| {
            if matches!(event, SagaChoreographyEvent::SagaStarted { .. }) {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            true
        });
        // Keep enough live routes that delivery checks cannot mask admission.
        let _other_route = bus.subscribe_saga_type_fn("order_lifecycle", |_| true);
        let journal = Arc::new(InMemoryTerminalResolverJournal::default());
        let attach = || {
            bus.attach_durable_terminal_resolver_for_contract::<OrderLifecycleContract, _>(
                "detach-test",
                Arc::clone(&journal),
            )
            .unwrap();
        };
        let start = |id| {
            bus.publish_strict(SagaChoreographyEvent::SagaStarted {
                context: context("create_order", id),
                payload: Vec::new(),
            })
        };
        attach();
        bus.activate_terminal_resolver_recovery("order_lifecycle")
            .unwrap();
        assert!(bus.detach_terminal_resolver("order_lifecycle"));
        assert!(!bus.detach_terminal_resolver("order_lifecycle"));

        let result = start(9011);
        assert!(
            matches!(
                &result,
                Err(super::SagaBusPublishError::AdmissionRejected { reason, .. })
                    if reason.contains("terminal resolver is detached")
                        && reason.contains("order_lifecycle")
            ),
            "detached resolver must reject before fanout: {result:?}, effects={}",
            effects.load(Ordering::SeqCst)
        );
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        assert!(journal.read_all().unwrap().is_empty());

        attach();
        let result = start(9012);
        assert!(
            matches!(
                &result,
                Err(super::SagaBusPublishError::AdmissionRejected { reason, .. })
                    if reason.contains("recovery is not activated")
            ),
            "replacement must remain closed before recovery activation: {result:?}"
        );
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        assert!(journal.read_all().unwrap().is_empty());

        bus.activate_terminal_resolver_recovery("order_lifecycle")
            .unwrap();
        start(9013).expect("replacement must admit starts after recovery activation");
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert!(journal.read_all().unwrap().iter().any(|entry| matches!(
            &entry.event,
            SagaChoreographyEvent::SagaStarted { context, .. }
                if context.saga_id == SagaId::new(9013)
        )));
    }

    struct RejectingTerminalResolverJournal;

    impl TerminalResolverJournal for RejectingTerminalResolverJournal {
        fn append(
            &self,
            _event: SagaChoreographyEvent,
        ) -> Result<u64, TerminalResolverJournalError> {
            Err(TerminalResolverJournalError::Storage(
                "injected resolver journal failure".into(),
            ))
        }

        fn read_all(
            &self,
        ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn dropping_the_last_public_bus_releases_resolver_journal_ownership() {
        let bus = SagaChoreographyBus::new();
        let lifecycle = Arc::downgrade(&bus.inner);
        let journal = Arc::new(InMemoryTerminalResolverJournal::default());
        bus.attach_durable_terminal_resolver(
            TerminalPolicy::order_lifecycle_default(),
            "lifetime-test",
            Arc::clone(&journal),
        )
        .unwrap();
        drop(bus);
        assert!(
            lifecycle.upgrade().is_none(),
            "resolver must not keep its lifecycle owner alive forever"
        );
        // The registry calls pooled shutdown from a scheduler callback; pinned
        // core intentionally initiates stop without waiting there. Keep the
        // immediate no-cycle assertion above, and require bounded actual release
        // rather than assuming the resolver has already drained at reply time.
        wait_until(Instant::now() + Duration::from_secs(3), || {
            Arc::strong_count(&journal) == 1
        });
        assert_eq!(
            Arc::strong_count(&journal),
            1,
            "resolver and gate must release LMDB ownership at shutdown"
        );
    }

    #[test]
    fn last_public_bus_dropped_in_a_sync_callback_also_releases_the_journal() {
        enum DropMessage {
            Bus(SagaChoreographyBus),
        }
        impl icanact_core::TellAskTell for DropMessage {}
        let bus = SagaChoreographyBus::new();
        let lifecycle = Arc::downgrade(&bus.inner);
        let journal = Arc::new(InMemoryTerminalResolverJournal::default());
        bus.attach_durable_terminal_resolver(
            TerminalPolicy::order_lifecycle_default(),
            "callback-drop",
            Arc::clone(&journal),
        )
        .unwrap();
        struct DropActor;
        impl local_sync::SyncActor for DropActor {
            type Contract = local_sync::contract::TellOnly;
            type Tell = DropMessage;
            type Ask = ();
            type Reply = ();
            type Channel = ();
            type PubSub = ();
            type Broadcast = ();
            fn handle_tell(&mut self, message: DropMessage) {
                let DropMessage::Bus(bus) = message;
                drop(bus);
            }
        }
        let (actor, handle) = local_sync::spawn(DropActor);
        assert!(actor.tell(DropMessage::Bus(bus)));
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline
            && (lifecycle.upgrade().is_some() || Arc::strong_count(&journal) != 1)
        {
            thread::yield_now();
        }
        let released = lifecycle.upgrade().is_none() && Arc::strong_count(&journal) == 1;
        handle.shutdown();
        assert!(
            released,
            "scheduler callback shutdown must release the resolver journal, count={}",
            Arc::strong_count(&journal)
        );
    }

    #[test]
    fn attached_resolver_emits_single_terminal_completion() {
        let bus = SagaChoreographyBus::new();
        let _resolver_sub = bus
            .attach_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
            )
            .expect("terminal resolver should attach");

        let delivered = Arc::new(AtomicUsize::new(0));
        let _capture_sub = bus.subscribe_saga_type_fn("order_lifecycle", {
            let delivered = Arc::clone(&delivered);
            move |event: &SagaChoreographyEvent| {
                if matches!(
                    event,
                    SagaChoreographyEvent::SagaCompleted { .. }
                        | SagaChoreographyEvent::SagaFailed { .. }
                        | SagaChoreographyEvent::SagaQuarantined { .. }
                ) {
                    delivered.fetch_add(1, Ordering::Relaxed);
                }
                true
            }
        });

        let step = SagaChoreographyEvent::StepCompleted {
            context: context("create_order", 42),
            output: Vec::new(),
            saga_input: Vec::new(),
            compensation_available: false,
        };
        let _ = bus.publish(step.clone());
        let _ = bus.publish(step);

        wait_until(Instant::now() + Duration::from_secs(1), || {
            delivered.load(Ordering::Relaxed) == 1
        });
    }

    #[test]
    fn durable_journal_failure_resolves_pending_reply_as_quarantined() {
        let bus = SagaChoreographyBus::new();
        let _resolver = bus
            .attach_durable_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
                Arc::new(RejectingTerminalResolverJournal),
            )
            .expect("durable resolver should attach");
        bus.activate_terminal_resolver_recovery("order_lifecycle")
            .unwrap();
        let saga_id = SagaId::new(811);
        let (pending, probe) = register_pending_reply(bus.clone(), saga_id);
        thread::sleep(Duration::from_millis(20));

        bus.publish_strict(SagaChoreographyEvent::StepStarted {
            context: context("create_order", saga_id.get()),
        })
        .expect("step start should reach the resolver");

        let reply = pending
            .wait()
            .expect("journal failure should resolve the pending reply")
            .expect("quarantine is a terminal saga reply");
        assert!(matches!(
            reply.outcome,
            SagaTerminalOutcome::Quarantined { .. }
        ));
        probe.shutdown();
    }

    #[test]
    fn durable_resolver_restart_preserves_complete_compensation_scope() {
        let journal = Arc::new(InMemoryTerminalResolverJournal::default());
        let policy = DurableMultiStepContract::terminal_policy();
        let start = context_for("durable_multi_step", "first_effect", 812);

        {
            let bus = SagaChoreographyBus::new();
            bus.register_workflow_contract_provider::<DurableMultiStepContract>()
                .expect("workflow contract registration should succeed");
            for step in ["first_effect", "second_effect", "finalize"] {
                bus.subscribe_workflow_step_fn("durable_multi_step", step, |_| true)
                    .expect("step binding should succeed");
            }
            // Live participant lanes so the strict start meets its required delivery.
            let _participants: Vec<_> = (0..3)
                .map(|_| bus.subscribe_saga_type_fn("durable_multi_step", |_event| true))
                .collect();
            let _resolver = bus
                .attach_durable_terminal_resolver(
                    policy.clone(),
                    "terminal-resolver",
                    Arc::clone(&journal),
                )
                .expect("durable resolver should attach");
            bus.activate_terminal_resolver_recovery("durable_multi_step")
                .expect("new starts require explicit recovery activation");
            bus.publish_strict(SagaChoreographyEvent::SagaStarted {
                context: start.clone(),
                payload: Vec::new(),
            })
            .expect("saga start should be admitted");
            bus.publish_strict(SagaChoreographyEvent::StepCompleted {
                context: start.next_step("first_effect".into()),
                output: Vec::new(),
                saga_input: Vec::new(),
                compensation_available: true,
            })
            .expect("first effect completion should publish");
            bus.publish_strict(SagaChoreographyEvent::StepAccepted {
                context: start.next_step("second_effect".into()),
                participant_id: "second-participant".into(),
                execution_id: StepExecutionId::new("external-812"),
                deadline_at_millis: SagaContext::now_millis().saturating_add(30_000),
                hard_deadline_at_millis: SagaContext::now_millis().saturating_add(60_000),
                timeouts_enabled: true,
                timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                    requires_compensation: true,
                },
                compensation_available: true,
            })
            .expect("second effect acceptance should publish");
            wait_until(Instant::now() + Duration::from_secs(1), || {
                journal.read_all().is_ok_and(|entries| entries.len() == 3)
            });
        }

        let bus = SagaChoreographyBus::new();
        let _resolver = bus
            .attach_durable_terminal_resolver(policy, "terminal-resolver", Arc::clone(&journal))
            .expect("durable resolver should restore");
        let compensation_scope = Arc::new(std::sync::Mutex::new(None));
        let _capture = bus.subscribe_saga_type_fn("durable_multi_step", {
            let compensation_scope = Arc::clone(&compensation_scope);
            move |event| {
                if let SagaChoreographyEvent::CompensationRequested {
                    steps_to_compensate,
                    ..
                } = event
                {
                    let mut guard = compensation_scope
                        .lock()
                        .expect("capture lock should remain available");
                    *guard = Some(steps_to_compensate.clone());
                }
                true
            }
        });
        bus.activate_terminal_resolver_recovery("durable_multi_step")
            .unwrap();
        bus.publish_strict(SagaChoreographyEvent::StepFailed {
            context: start.next_step("second_effect".into()),
            participant_id: "second-participant".into(),
            error_code: Some("exchange_reject".into()),
            error: "authoritative exchange failure".into(),
            requires_compensation: true,
        })
        .expect("recovered failure should publish");

        wait_until(Instant::now() + Duration::from_secs(1), || {
            compensation_scope
                .lock()
                .expect("capture lock should remain available")
                .is_some()
        });
        assert_eq!(
            compensation_scope
                .lock()
                .expect("capture lock should remain available")
                .as_deref(),
            Some([Box::<str>::from("second_effect")].as_slice())
        );
        bus.publish_strict(SagaChoreographyEvent::CompensationCompleted {
            context: start.next_step("second_effect".into()),
        })
        .unwrap();
        wait_until(Instant::now() + Duration::from_secs(1), || {
            compensation_scope.lock().unwrap().as_deref()
                == Some([Box::<str>::from("first_effect")].as_slice())
        });
        assert!(
            bus.take_terminal_outcome(start.saga_id).is_none(),
            "first undo is still pending"
        );
    }

    #[test]
    fn durable_recovery_output_waits_for_explicit_post_binding_activation() {
        let journal = Arc::new(InMemoryTerminalResolverJournal::default());
        let saga_id = SagaId::new(813);
        let start = context("create_order", saga_id.get());
        journal
            .append(SagaChoreographyEvent::SagaStarted {
                context: start.clone(),
                payload: Vec::new(),
            })
            .expect("saga start should be journaled");
        journal
            .append(SagaChoreographyEvent::StepCompleted {
                context: start.next_step("create_order".into()),
                output: Vec::new(),
                saga_input: Vec::new(),
                compensation_available: false,
            })
            .expect("terminal step should be journaled without its resolver output");

        let bus = SagaChoreographyBus::new();
        let _resolver = bus
            .attach_durable_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
                journal,
            )
            .expect("durable resolver should restore pending output");
        let delivered = Arc::new(AtomicUsize::new(0));
        let _participant_binding = bus.subscribe_saga_type_fn("order_lifecycle", {
            let delivered = Arc::clone(&delivered);
            move |event| {
                if matches!(event, SagaChoreographyEvent::SagaCompleted { .. }) {
                    delivered.fetch_add(1, Ordering::Relaxed);
                }
                true
            }
        });
        thread::sleep(Duration::from_millis(20));
        assert_eq!(delivered.load(Ordering::Relaxed), 0);

        bus.activate_terminal_resolver_recovery("order_lifecycle")
            .expect("post-binding activation should succeed");
        wait_until(Instant::now() + Duration::from_secs(1), || {
            delivered.load(Ordering::Relaxed) == 1
        });
    }

    #[test]
    fn terminal_reply_state_is_bus_scoped() {
        let bus_a = SagaChoreographyBus::new();
        let bus_b = SagaChoreographyBus::new();
        let saga_id = SagaId::new(7);
        let (pending_a, probe_a) = register_pending_reply(bus_a.clone(), saga_id);
        let (pending_b, probe_b) = register_pending_reply(bus_b.clone(), saga_id);
        thread::sleep(Duration::from_millis(20));

        let completed_a = SagaChoreographyEvent::SagaCompleted {
            context: SagaContext {
                step_name: TERMINAL_RESOLVER_STEP.into(),
                ..context("create_order", saga_id.get())
            },
        };
        assert!(bus_a.complete_terminal_reply_from_event(&completed_a, "resolver-a"));
        assert!(bus_b.complete_terminal_reply_from_event(&completed_a, "resolver-b"));

        let got_a = pending_a.wait().expect("bus A reply should arrive");
        let got_b = pending_b.wait().expect("bus B reply should arrive");
        assert!(got_a.is_ok());
        assert!(got_b.is_ok());
        probe_a.shutdown();
        probe_b.shutdown();
    }

    #[test]
    fn take_terminal_outcome_returns_and_consumes_resolved_reply() {
        let bus = SagaChoreographyBus::new();
        let saga_id = SagaId::new(77);
        let (pending, probe) = register_pending_reply(bus.clone(), saga_id);
        thread::sleep(Duration::from_millis(20));

        let completed = SagaChoreographyEvent::SagaCompleted {
            context: SagaContext {
                step_name: TERMINAL_RESOLVER_STEP.into(),
                ..context("create_order", saga_id.get())
            },
        };
        assert!(bus.complete_terminal_reply_from_event(&completed, "resolver"));

        let reply = pending
            .wait()
            .expect("terminal reply should resolve")
            .expect("terminal reply should be successful");
        assert!(matches!(
            reply.outcome,
            SagaTerminalOutcome::Completed { .. }
        ));
        assert!(matches!(
            bus.take_terminal_outcome(saga_id),
            Some(SagaTerminalOutcome::Completed { .. })
        ));
        assert!(
            bus.take_terminal_outcome(saga_id).is_none(),
            "terminal outcome should be consumed after the first read"
        );
        probe.shutdown();
    }

    #[test]
    fn attaching_terminal_resolver_twice_is_idempotent_for_saga_type() {
        let bus = SagaChoreographyBus::new();
        let first = bus
            .attach_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
            )
            .expect("terminal resolver should attach");
        let second = bus
            .attach_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
            )
            .expect("terminal resolver should return existing attachment");
        assert_eq!(first, second);

        let delivered = Arc::new(AtomicUsize::new(0));
        let _capture_sub = bus.subscribe_saga_type_fn("order_lifecycle", {
            let delivered = Arc::clone(&delivered);
            move |event: &SagaChoreographyEvent| {
                if matches!(event, SagaChoreographyEvent::SagaCompleted { .. }) {
                    delivered.fetch_add(1, Ordering::Relaxed);
                }
                true
            }
        });

        let step = SagaChoreographyEvent::StepCompleted {
            context: context_for("order_lifecycle", "create_order", 77),
            output: Vec::new(),
            saga_input: Vec::new(),
            compensation_available: false,
        };
        let stats = bus
            .publish_strict(step)
            .expect("single completed step should publish");
        assert_eq!(stats.delivered, stats.attempted);
        wait_until(Instant::now() + Duration::from_millis(200), || {
            delivered.load(Ordering::Relaxed) == 1
        });
        assert_eq!(delivered.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn terminal_outcome_retention_is_bounded() {
        let bus = SagaChoreographyBus::new();
        let total = DEFAULT_TERMINAL_RETENTION_LIMIT + 8;

        for raw_id in 1..=total as u64 {
            let _ = bus.publish(SagaChoreographyEvent::SagaCompleted {
                context: SagaContext {
                    step_name: TERMINAL_RESOLVER_STEP.into(),
                    ..context("create_order", raw_id)
                },
            });
        }

        assert!(
            bus.take_terminal_outcome(SagaId::new(1)).is_none(),
            "oldest terminal outcome should be evicted once retention limit is exceeded"
        );
        assert!(matches!(
            bus.take_terminal_outcome(SagaId::new(total as u64)),
            Some(SagaTerminalOutcome::Completed { .. })
        ));
    }

    #[test]
    fn unsubscribe_stops_future_resolver_processing() {
        let bus = SagaChoreographyBus::new();
        let resolver_sub = bus
            .attach_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
            )
            .expect("terminal resolver should attach");
        let delivered = Arc::new(AtomicUsize::new(0));
        let _capture_sub = bus.subscribe_saga_type_fn("order_lifecycle", {
            let delivered = Arc::clone(&delivered);
            move |event: &SagaChoreographyEvent| {
                if matches!(event, SagaChoreographyEvent::SagaCompleted { .. }) {
                    delivered.fetch_add(1, Ordering::Relaxed);
                }
                true
            }
        });

        assert!(bus.unsubscribe(resolver_sub));

        let step = SagaChoreographyEvent::StepCompleted {
            context: context("create_order", 99),
            output: Vec::new(),
            saga_input: Vec::new(),
            compensation_available: false,
        };
        let _ = bus.publish(step);

        assert_eq!(delivered.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn duplicate_terminal_reply_registration_is_rejected_on_same_bus() {
        let bus = SagaChoreographyBus::new();
        let saga_id = SagaId::new(123);
        let (_pending, probe_a) = register_pending_reply(bus.clone(), saga_id);
        let (pending_dup, probe_b) = register_pending_reply(bus, saga_id);

        let dup_result = pending_dup
            .wait()
            .expect("duplicate registration reply should arrive");
        assert!(dup_result.is_err());
        probe_a.shutdown();
        probe_b.shutdown();
    }

    #[test]
    fn repeated_unsubscribe_is_safe() {
        let bus = SagaChoreographyBus::new();
        let resolver_sub = bus
            .attach_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
            )
            .expect("terminal resolver should attach");

        assert!(bus.unsubscribe(resolver_sub.clone()));
        assert!(!bus.unsubscribe(resolver_sub));
    }

    #[test]
    fn dropping_owner_bus_rejects_pending_terminal_replies() {
        let bus = SagaChoreographyBus::new();
        let saga_id = SagaId::new(456);
        let (pending, probe) = register_pending_reply(bus.clone(), saga_id);
        thread::sleep(Duration::from_millis(20));

        drop(bus);

        let result = pending
            .wait()
            .expect("pending terminal reply should resolve on bus drop");
        assert!(result.is_err());
        probe.shutdown();
    }

    #[test]
    fn saga_started_without_terminal_policy_is_failed_immediately() {
        let bus = SagaChoreographyBus::new();
        let saga_id = SagaId::new(9001);
        let started = SagaChoreographyEvent::SagaStarted {
            context: context("create_order", saga_id.get()),
            payload: Vec::new(),
        };

        let _ = bus.publish(started);

        let Some(SagaTerminalOutcome::Failed { reason, .. }) = bus.take_terminal_outcome(saga_id)
        else {
            panic!("expected immediate terminal failure for unregistered terminal policy");
        };
        assert!(
            reason.contains("terminal policy is required before saga start"),
            "unexpected reason: {reason}"
        );
    }

    #[test]
    fn terminal_policy_without_contract_fails_saga_started() {
        let bus = SagaChoreographyBus::new();
        bus.attach_terminal_resolver(TerminalPolicy::order_lifecycle_default(), "test")
            .unwrap();
        let saga_id = SagaId::new(9002);
        let started = SagaChoreographyEvent::SagaStarted {
            context: context("create_order", saga_id.get()),
            payload: Vec::new(),
        };

        let _ = bus.publish(started);

        let Some(SagaTerminalOutcome::Failed { reason, .. }) = bus.take_terminal_outcome(saga_id)
        else {
            panic!("expected immediate terminal failure for missing workflow contract");
        };
        assert!(
            reason.contains("workflow contract is required before saga start"),
            "unexpected reason: {reason}"
        );
    }

    struct OrderLifecycleContract;

    impl SagaWorkflowContract for OrderLifecycleContract {
        fn saga_type() -> &'static str {
            "order_lifecycle"
        }

        fn first_step() -> &'static str {
            "create_order"
        }

        fn steps() -> &'static [SagaWorkflowStepContract] {
            &[SagaWorkflowStepContract {
                step_name: "create_order",
                participant_id: "order-manager",
                depends_on: WorkflowDependencySpec::OnSagaStart,
            }]
        }

        fn terminal_policy() -> TerminalPolicy {
            TerminalPolicy::order_lifecycle_default()
        }
    }

    struct DurableMultiStepContract;

    impl SagaWorkflowContract for DurableMultiStepContract {
        fn saga_type() -> &'static str {
            "durable_multi_step"
        }

        fn first_step() -> &'static str {
            "first_effect"
        }

        fn steps() -> &'static [SagaWorkflowStepContract] {
            &[
                SagaWorkflowStepContract {
                    step_name: "first_effect",
                    participant_id: "first-participant",
                    depends_on: WorkflowDependencySpec::OnSagaStart,
                },
                SagaWorkflowStepContract {
                    step_name: "second_effect",
                    participant_id: "second-participant",
                    depends_on: WorkflowDependencySpec::After("first_effect"),
                },
                SagaWorkflowStepContract {
                    step_name: "finalize",
                    participant_id: "finalizer",
                    depends_on: WorkflowDependencySpec::After("second_effect"),
                },
            ]
        }

        fn terminal_policy() -> TerminalPolicy {
            let mut required = HashSet::new();
            required.insert(Box::<str>::from("finalize"));
            TerminalPolicy::new(
                "durable_multi_step".into(),
                "durable_multi_step/test".into(),
                FailureAuthority::AnyParticipant,
                SuccessCriteria::AllOf(required),
                Duration::from_secs(60),
                Duration::from_secs(60),
                &[],
            )
        }
    }

    struct MultiStepOrderLifecycleContract;

    impl SagaWorkflowContract for MultiStepOrderLifecycleContract {
        fn saga_type() -> &'static str {
            "order_lifecycle"
        }

        fn first_step() -> &'static str {
            "risk_check"
        }

        fn steps() -> &'static [SagaWorkflowStepContract] {
            &[
                SagaWorkflowStepContract {
                    step_name: "risk_check",
                    participant_id: "risk-engine",
                    depends_on: WorkflowDependencySpec::OnSagaStart,
                },
                SagaWorkflowStepContract {
                    step_name: "create_order",
                    participant_id: "order-manager",
                    depends_on: WorkflowDependencySpec::After("risk_check"),
                },
            ]
        }

        fn terminal_policy() -> TerminalPolicy {
            TerminalPolicy::order_lifecycle_default()
        }
    }

    struct AnyOfOrderLifecycleContract;

    impl SagaWorkflowContract for AnyOfOrderLifecycleContract {
        fn saga_type() -> &'static str {
            "order_lifecycle"
        }

        fn first_step() -> &'static str {
            "risk_check"
        }

        fn steps() -> &'static [SagaWorkflowStepContract] {
            &[
                SagaWorkflowStepContract {
                    step_name: "risk_check",
                    participant_id: "risk-engine",
                    depends_on: WorkflowDependencySpec::OnSagaStart,
                },
                SagaWorkflowStepContract {
                    step_name: "manual_review",
                    participant_id: "risk-human",
                    depends_on: WorkflowDependencySpec::OnSagaStart,
                },
            ]
        }

        fn terminal_policy() -> TerminalPolicy {
            let mut possible_success_steps = HashSet::new();
            possible_success_steps.insert("risk_check".into());
            possible_success_steps.insert("manual_review".into());
            TerminalPolicy {
                saga_type: "order_lifecycle".into(),
                policy_id: "order_lifecycle/any-of".into(),
                failure_authority: FailureAuthority::AnyParticipant,
                success_criteria: SuccessCriteria::AnyOf(possible_success_steps),
                overall_timeout: Duration::from_secs(30),
                stalled_timeout: Duration::from_secs(5),
                workflow_steps: Self::steps(),
            }
        }
    }

    struct MismatchedPolicySagaTypeContract;

    impl SagaWorkflowContract for MismatchedPolicySagaTypeContract {
        fn saga_type() -> &'static str {
            "order_lifecycle"
        }

        fn first_step() -> &'static str {
            "create_order"
        }

        fn steps() -> &'static [SagaWorkflowStepContract] {
            &[SagaWorkflowStepContract {
                step_name: "create_order",
                participant_id: "order-manager",
                depends_on: WorkflowDependencySpec::OnSagaStart,
            }]
        }

        fn terminal_policy() -> TerminalPolicy {
            let mut required_steps = HashSet::new();
            required_steps.insert("create_order".into());
            TerminalPolicy {
                saga_type: "different_saga_type".into(),
                policy_id: "different_saga_type/default".into(),
                failure_authority: FailureAuthority::AnyParticipant,
                success_criteria: SuccessCriteria::AllOf(required_steps),
                overall_timeout: Duration::from_secs(30),
                stalled_timeout: Duration::from_secs(30),
                workflow_steps: Self::steps(),
            }
        }
    }

    #[test]
    fn workflow_contract_without_resolver_fails_saga_started() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<OrderLifecycleContract>()
            .expect("workflow contract registration should succeed");
        bus.subscribe_workflow_step_fn("order_lifecycle", "create_order", |_| true)
            .expect("bound workflow step registration should succeed");
        let saga_id = SagaId::new(9003);
        let started = SagaChoreographyEvent::SagaStarted {
            context: context("create_order", saga_id.get()),
            payload: Vec::new(),
        };

        let _ = bus.publish(started);

        let Some(SagaTerminalOutcome::Failed { reason, .. }) = bus.take_terminal_outcome(saga_id)
        else {
            panic!("expected immediate terminal failure for missing resolver attachment");
        };
        assert!(
            reason.contains("terminal policy is required before saga start"),
            "unexpected reason: {reason}"
        );
    }

    #[test]
    fn workflow_contract_with_resolver_allows_saga_started() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<OrderLifecycleContract>()
            .expect("workflow contract registration should succeed");
        bus.subscribe_workflow_step_fn("order_lifecycle", "create_order", |_| true)
            .expect("bound workflow step registration should succeed");
        let _resolver = bus
            .attach_terminal_resolver_for_contract::<OrderLifecycleContract>("test-resolver")
            .expect("terminal resolver should attach");
        let _participant_sub = bus.subscribe_saga_type_fn("order_lifecycle", |_event| true);
        let saga_id = SagaId::new(90031);
        let started = SagaChoreographyEvent::SagaStarted {
            context: context("create_order", saga_id.get()),
            payload: Vec::new(),
        };

        let _ = bus.publish(started);

        assert!(
            bus.take_terminal_outcome(saga_id).is_none(),
            "contract + resolver + bound step should allow saga start"
        );
    }

    #[test]
    fn cloned_bus_keeps_shared_actors_alive_after_original_drop() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<OrderLifecycleContract>()
            .expect("workflow contract registration should succeed");
        bus.subscribe_workflow_step_fn("order_lifecycle", "create_order", |_| true)
            .expect("bound workflow step registration should succeed");
        let _resolver = bus
            .attach_terminal_resolver_for_contract::<OrderLifecycleContract>("test-resolver")
            .expect("terminal resolver should attach");
        let _participant_sub = bus.subscribe_saga_type_fn("order_lifecycle", |_event| true);
        let cloned = bus.clone();
        drop(bus);

        let saga_id = SagaId::new(90032);
        let stats = cloned.publish(SagaChoreographyEvent::SagaStarted {
            context: context("create_order", saga_id.get()),
            payload: Vec::new(),
        });

        assert!(stats.delivered >= 1, "clone should still deliver events");
        assert!(
            cloned.take_terminal_outcome(saga_id).is_none(),
            "clone should retain workflow contract, bound steps, and resolver actors"
        );
    }

    #[test]
    fn concurrent_terminal_resolver_attach_is_idempotent() {
        let bus = Arc::new(SagaChoreographyBus::new());
        bus.register_workflow_contract_provider::<OrderLifecycleContract>()
            .expect("workflow contract registration should succeed");
        let mut workers = Vec::new();
        for _ in 0..8 {
            let bus = Arc::clone(&bus);
            workers.push(thread::spawn(move || {
                bus.attach_terminal_resolver_for_contract::<OrderLifecycleContract>("test-resolver")
                    .expect("resolver attach should succeed")
            }));
        }
        let subscriptions = workers
            .into_iter()
            .map(|worker| worker.join().expect("attach worker should not panic"))
            .collect::<Vec<_>>();
        let first = subscriptions
            .first()
            .expect("at least one subscription should be returned")
            .clone();
        assert!(
            subscriptions
                .iter()
                .all(|subscription| *subscription == first),
            "concurrent attaches must all return the same resolver subscription"
        );
    }

    #[test]
    fn saga_started_with_first_step_mismatch_fails_immediately() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<OrderLifecycleContract>()
            .expect("workflow contract registration should succeed");
        bus.subscribe_workflow_step_fn("order_lifecycle", "create_order", |_| true)
            .expect("bound workflow step registration should succeed");
        let _resolver = bus
            .attach_terminal_resolver_for_contract::<OrderLifecycleContract>("test-resolver")
            .expect("terminal resolver should attach");
        let saga_id = SagaId::new(9004);
        let started = SagaChoreographyEvent::SagaStarted {
            context: context("risk_check", saga_id.get()),
            payload: Vec::new(),
        };

        let _ = bus.publish(started);

        let Some(SagaTerminalOutcome::Failed { reason, .. }) = bus.take_terminal_outcome(saga_id)
        else {
            panic!("expected immediate terminal failure for first_step mismatch");
        };
        assert!(
            reason.contains("workflow contract first_step mismatch at saga start"),
            "unexpected reason: {reason}"
        );
    }

    #[test]
    fn saga_started_with_missing_bound_step_fails_immediately() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<MultiStepOrderLifecycleContract>()
            .expect("workflow contract registration should succeed");
        bus.subscribe_workflow_step_fn("order_lifecycle", "risk_check", |_| true)
            .expect("risk_check binding should succeed");
        let _resolver = bus
            .attach_terminal_resolver_for_contract::<MultiStepOrderLifecycleContract>(
                "test-resolver",
            )
            .expect("terminal resolver should attach");
        let saga_id = SagaId::new(9005);
        let started = SagaChoreographyEvent::SagaStarted {
            context: context("risk_check", saga_id.get()),
            payload: Vec::new(),
        };

        let _ = bus.publish(started);

        let Some(SagaTerminalOutcome::Failed { reason, .. }) = bus.take_terminal_outcome(saga_id)
        else {
            panic!("expected immediate terminal failure for missing bound steps");
        };
        assert!(
            reason.contains("workflow contract violation: unbound participant steps"),
            "unexpected reason: {reason}"
        );
        assert!(
            reason.contains("missing_steps=create_order"),
            "expected missing create_order step, got: {reason}"
        );
    }

    #[test]
    fn saga_started_with_live_delivery_shortfall_quarantines_possible_effects() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<MultiStepOrderLifecycleContract>()
            .expect("workflow contract registration should succeed");
        bus.subscribe_workflow_step_fn("order_lifecycle", "risk_check", |_| true)
            .expect("risk_check binding should succeed");
        bus.subscribe_workflow_step_fn("order_lifecycle", "create_order", |_| false)
            .expect("create_order binding should succeed");
        let _resolver = bus
            .attach_terminal_resolver_for_contract::<MultiStepOrderLifecycleContract>(
                "test-resolver",
            )
            .expect("terminal resolver should attach");

        // One subscriber can already execute an effect during partial start fanout.
        let effects = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&effects);
        let _single_participant = bus.subscribe_saga_type_fn("order_lifecycle", move |event| {
            if matches!(event, SagaChoreographyEvent::SagaStarted { .. }) {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            true
        });

        let saga_id = SagaId::new(90051);
        let started = SagaChoreographyEvent::SagaStarted {
            context: context("risk_check", saga_id.get()),
            payload: Vec::new(),
        };

        let _ = bus.publish(started);

        assert_eq!(
            effects.load(Ordering::SeqCst),
            1,
            "start already reached an effect owner"
        );
        let Some(SagaTerminalOutcome::Quarantined { reason, .. }) =
            bus.take_terminal_outcome(saga_id)
        else {
            panic!(
                "partial start fanout must quarantine possible effects, not manufacture SagaFailed"
            );
        };
        assert!(
            reason.contains("required_path_delivery_shortfall"),
            "unexpected reason: {reason}"
        );
        assert!(
            reason.contains("required_min_delivered=3"),
            "expected minimum delivery requirement in reason, got: {reason}"
        );
        assert!(
            reason.contains("required_path=create_order(order-manager),risk_check(risk-engine)"),
            "expected required path participants in reason, got: {reason}"
        );
    }

    #[test]
    fn any_of_success_does_not_require_every_branch_delivery() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<AnyOfOrderLifecycleContract>()
            .expect("workflow contract registration should succeed");
        bus.subscribe_workflow_step_fn("order_lifecycle", "risk_check", |_| true)
            .expect("risk_check binding should succeed");
        bus.subscribe_workflow_step_fn("order_lifecycle", "manual_review", |_| false)
            .expect("manual_review binding should succeed");
        let _resolver = bus
            .attach_terminal_resolver_for_contract::<AnyOfOrderLifecycleContract>("test-resolver")
            .expect("terminal resolver should attach");
        let _risk_sub = bus.subscribe_saga_type_fn("order_lifecycle", |_event| true);

        let saga_id = SagaId::new(900_511);
        let publish = bus.publish_strict(SagaChoreographyEvent::SagaStarted {
            context: context("risk_check", saga_id.get()),
            payload: Vec::new(),
        });

        assert!(
            publish.is_ok(),
            "AnyOf success should not require every alternate branch delivery: {publish:?}"
        );
        assert!(
            bus.take_terminal_outcome(saga_id).is_none(),
            "AnyOf success should not terminally fail when one alternate branch is undelivered"
        );
    }

    #[test]
    fn required_path_dependency_delivery_shortfall_quarantines_immediately() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<MultiStepOrderLifecycleContract>()
            .expect("workflow contract registration should succeed");
        bus.subscribe_workflow_step_fn("order_lifecycle", "risk_check", |_| true)
            .expect("risk_check binding should succeed");
        let order_sub = bus
            .subscribe_workflow_step_fn("order_lifecycle", "create_order", |_| true)
            .expect("create_order binding should succeed");
        let _resolver = bus
            .attach_terminal_resolver_for_contract::<MultiStepOrderLifecycleContract>(
                "test-resolver",
            )
            .expect("terminal resolver should attach");
        let _risk_sub = bus.subscribe_saga_type_fn("order_lifecycle", |_event| true);

        let saga_id = SagaId::new(90052);
        let started = SagaChoreographyEvent::SagaStarted {
            context: context("risk_check", saga_id.get()),
            payload: Vec::new(),
        };
        let _ = bus.publish(started);
        assert!(
            bus.take_terminal_outcome(saga_id).is_none(),
            "initial start should be accepted with resolver and participant route live"
        );

        assert!(bus.unsubscribe(order_sub));
        let _ = bus.publish(SagaChoreographyEvent::StepCompleted {
            context: context("risk_check", saga_id.get()),
            output: Vec::new(),
            saga_input: Vec::new(),
            compensation_available: false,
        });

        // Earlier steps may already have effects, so route loss quarantines
        // (evidence-preserving) instead of manufacturing an ordinary failure.
        let Some(SagaTerminalOutcome::Quarantined { reason, .. }) =
            bus.take_terminal_outcome(saga_id)
        else {
            panic!("expected immediate quarantine for required dependency route loss");
        };
        assert!(
            reason.contains("required_path_delivery_shortfall"),
            "unexpected reason: {reason}"
        );
        assert!(
            reason.contains("event_type=step_completed"),
            "expected event type in reason, got: {reason}"
        );
        assert!(
            reason.contains("step=risk_check"),
            "expected source step in reason, got: {reason}"
        );
        assert!(
            reason.contains("required_path=create_order(order-manager),risk_check(risk-engine)"),
            "expected required path participants in reason, got: {reason}"
        );
    }

    #[test]
    fn strict_start_rejection_is_err_even_when_failure_notification_delivers() {
        // (1) no terminal policy registered for the saga type
        let bus = SagaChoreographyBus::new();
        let sub = bus.subscribe_saga_type_fn("order_lifecycle", |_event| true);
        let saga_id = SagaId::new(90061);
        let res = bus.publish_strict(SagaChoreographyEvent::SagaStarted {
            context: context("risk_check", saga_id.get()),
            payload: Vec::new(),
        });
        assert!(
            matches!(
                res,
                Err(super::SagaBusPublishError::AdmissionRejected { .. })
            ),
            "start without terminal policy must be Err, got {res:?}"
        );
        assert!(matches!(
            bus.take_terminal_outcome(saga_id),
            Some(SagaTerminalOutcome::Failed { .. })
        ));
        assert!(bus.unsubscribe(sub));

        // (2) start contract invalid (wrong first step)
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<OrderLifecycleContract>()
            .expect("workflow contract registration should succeed");
        let _sub = bus.subscribe_saga_type_fn("order_lifecycle", |_event| true);
        let saga_id = SagaId::new(90062);
        let res = bus.publish_strict(SagaChoreographyEvent::SagaStarted {
            context: context("not_the_first_step", saga_id.get()),
            payload: Vec::new(),
        });
        assert!(
            matches!(
                res,
                Err(super::SagaBusPublishError::AdmissionRejected { .. })
            ),
            "start violating contract must be Err, got {res:?}"
        );
        assert!(matches!(
            bus.take_terminal_outcome(saga_id),
            Some(SagaTerminalOutcome::Failed { .. })
        ));
    }

    #[test]
    fn publish_strict_reports_required_path_delivery_shortfall() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<MultiStepOrderLifecycleContract>()
            .expect("workflow contract registration should succeed");
        bus.subscribe_workflow_step_fn("order_lifecycle", "risk_check", |_| true)
            .expect("risk_check binding should succeed");
        bus.subscribe_workflow_step_fn("order_lifecycle", "create_order", |_| false)
            .expect("create_order binding should succeed");
        let _resolver = bus
            .attach_terminal_resolver_for_contract::<MultiStepOrderLifecycleContract>(
                "test-resolver",
            )
            .expect("terminal resolver should attach");

        let err = bus
            .publish_strict(SagaChoreographyEvent::StepCompleted {
                context: context("risk_check", 90053),
                output: Vec::new(),
                saga_input: Vec::new(),
                compensation_available: false,
            })
            .expect_err("strict publish should report required path shortfall");
        let super::SagaBusPublishError::RequiredPathDeliveryShortfall {
            step_name,
            event_type: "step_completed",
            required_min_delivered: 3,
            required_path,
            ..
        } = err
        else {
            panic!("unexpected publish error: {err:?}");
        };
        assert_eq!(step_name.as_ref(), "risk_check");
        assert_eq!(
            required_path.as_ref(),
            "create_order(order-manager),risk_check(risk-engine)"
        );
    }

    struct DeniedRequiredStepContract;

    impl SagaWorkflowContract for DeniedRequiredStepContract {
        fn saga_type() -> &'static str {
            "order_lifecycle"
        }

        fn first_step() -> &'static str {
            "create_order"
        }

        fn steps() -> &'static [SagaWorkflowStepContract] {
            &[SagaWorkflowStepContract {
                step_name: "create_order",
                participant_id: "order-manager",
                depends_on: WorkflowDependencySpec::OnSagaStart,
            }]
        }

        fn terminal_policy() -> TerminalPolicy {
            let mut denied = HashSet::new();
            denied.insert("create_order".into());
            let mut required_steps = HashSet::new();
            required_steps.insert("create_order".into());
            TerminalPolicy {
                saga_type: "order_lifecycle".into(),
                policy_id: "order_lifecycle/denied-required".into(),
                failure_authority: FailureAuthority::DenySteps(denied),
                success_criteria: SuccessCriteria::AllOf(required_steps),
                overall_timeout: Duration::from_secs(30),
                stalled_timeout: Duration::from_secs(30),
                workflow_steps: Self::steps(),
            }
        }
    }

    #[test]
    fn workflow_contract_rejects_terminal_required_step_without_failure_authority() {
        let bus = SagaChoreographyBus::new();
        let err = bus
            .register_workflow_contract_provider::<DeniedRequiredStepContract>()
            .expect_err("required terminal step denied failure authority must be rejected");
        assert!(
            err.contains("terminal required step lacks failure authority"),
            "unexpected error: {err}"
        );
        assert!(
            err.contains("required_step=create_order"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn subscribe_workflow_step_rejects_undeclared_step_with_contract() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<OrderLifecycleContract>()
            .expect("workflow contract registration should succeed");

        let err = bus
            .subscribe_workflow_step_fn("order_lifecycle", "risk_check", |_| true)
            .expect_err("undeclared step should be rejected");
        assert!(
            err.contains("bound workflow step is not declared by contract"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn register_workflow_contract_rejects_prebound_unknown_steps_without_partial_registration() {
        let bus = SagaChoreographyBus::new();
        bus.subscribe_workflow_step_fn("order_lifecycle", "rogue_step", |_| true)
            .expect("prebinding without contract should succeed");

        let err = bus
            .register_workflow_contract_provider::<OrderLifecycleContract>()
            .expect_err("contract registration should fail on unknown prebound steps");
        assert!(
            err.contains("bound step set contains steps not declared by workflow contract"),
            "unexpected error: {err}"
        );
        assert!(
            err.contains("unknown_steps=rogue_step"),
            "unexpected error payload: {err}"
        );

        let saga_id = SagaId::new(9006);
        let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
            context: context("create_order", saga_id.get()),
            payload: Vec::new(),
        });
        let Some(SagaTerminalOutcome::Failed { reason, .. }) = bus.take_terminal_outcome(saga_id)
        else {
            panic!("expected immediate terminal failure after failed contract registration");
        };
        assert!(
            reason.contains("terminal policy is required before saga start"),
            "failed registration must not partially register policy/contract, got: {reason}"
        );
    }

    #[test]
    fn register_workflow_contract_rejects_policy_saga_type_mismatch() {
        let bus = SagaChoreographyBus::new();
        let err = bus
            .register_workflow_contract_provider::<MismatchedPolicySagaTypeContract>()
            .expect_err("registration should fail for policy saga type mismatch");
        assert!(
            err.contains("workflow contract saga_type mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn subscribe_workflow_step_rejects_empty_values() {
        let bus = SagaChoreographyBus::new();
        let err = bus
            .subscribe_workflow_step_fn("", "create_order", |_| true)
            .expect_err("empty saga_type must be rejected");
        assert!(
            err.contains("saga_type and step_name must be non-empty"),
            "unexpected error: {err}"
        );

        let err = bus
            .subscribe_workflow_step_fn("order_lifecycle", "", |_| true)
            .expect_err("empty step_name must be rejected");
        assert!(
            err.contains("saga_type and step_name must be non-empty"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn watchdog_times_out_stalled_saga_without_new_events() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<MultiStepOrderLifecycleContract>()
            .expect("workflow contract registration should succeed");
        bus.subscribe_workflow_step_fn("order_lifecycle", "risk_check", |_| true)
            .expect("risk_check binding should succeed");
        bus.subscribe_workflow_step_fn("order_lifecycle", "create_order", |_| true)
            .expect("create_order binding should succeed");
        let mut required_steps = HashSet::new();
        required_steps.insert("create_order".into());
        let policy = TerminalPolicy {
            saga_type: "order_lifecycle".into(),
            policy_id: "watchdog/stall".into(),
            failure_authority: FailureAuthority::AnyParticipant,
            success_criteria: SuccessCriteria::AllOf(required_steps),
            overall_timeout: Duration::from_secs(5),
            stalled_timeout: Duration::from_millis(120),
            workflow_steps: MultiStepOrderLifecycleContract::steps(),
        };
        let _resolver_sub = bus
            .attach_terminal_resolver(policy, "terminal-resolver")
            .expect("terminal resolver should attach");
        let _risk_sub = bus.subscribe_saga_type_fn("order_lifecycle", |_event| true);
        let _order_sub = bus.subscribe_saga_type_fn("order_lifecycle", |_event| true);
        let saga_id = SagaId::new(8_001);
        let _ = bus.publish(SagaChoreographyEvent::SagaStarted {
            context: context("risk_check", saga_id.get()),
            payload: Vec::new(),
        });

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut outcome = None;
        while Instant::now() < deadline {
            if let Some(found) = bus.take_terminal_outcome(saga_id) {
                outcome = Some(found);
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }

        let Some(SagaTerminalOutcome::Failed { reason, .. }) = outcome else {
            panic!("expected stalled terminal failure, got: {outcome:?}");
        };
        assert!(
            reason.as_ref().contains("stalled_timeout"),
            "expected stalled_timeout reason, got: {reason}"
        );
    }
}

#[cfg(test)]
mod admission_tests {
    //! Bounded admission index, journal-authoritative reload and successful
    //! versus failed terminal-phase fencing, all through a real bus.

    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    use crate::{
        AcceptedStepTimeoutOutcome, FailureAuthority, InMemoryTerminalResolverJournal,
        SagaChoreographyEvent, SagaContext, SagaId, SagaTerminalOutcome, SagaWorkflowContract,
        SagaWorkflowStepContract, StepExecutionId, SuccessCriteria, TERMINAL_RESOLVER_STEP,
        TerminalPolicy, TerminalResolverJournal, TerminalResolverJournalEntry,
        TerminalResolverJournalError, WorkflowDependencySpec,
    };

    use super::{
        AdmissionIndex, AdmissionLimits, ResolverGate, RunPhase, SagaBusPublishError,
        SagaChoreographyBus, SagaReplyToHandle, SagaReplyToResult,
    };
    use icanact_core::local_sync;

    /// Registers an id-scoped waiter from an off-actor probe.
    fn register_pending_reply(
        bus: SagaChoreographyBus,
        saga_id: SagaId,
    ) -> (
        local_sync::PendingAsk<SagaReplyToResult>,
        local_sync::mpsc::ActorHandle,
    ) {
        let (probe_addr, probe_handle) = local_sync::mpsc::spawn(
            8,
            |(bus, saga_id, reply): (SagaChoreographyBus, SagaId, SagaReplyToHandle)| {
                let _ = bus.register_terminal_reply(saga_id, reply);
            },
        );
        let pending = probe_addr
            .ask_delegated(|reply| (bus, saga_id, reply))
            .expect("pending reply should be registered");
        (pending, probe_handle)
    }

    struct Chain;
    impl SagaWorkflowContract for Chain {
        fn saga_type() -> &'static str {
            "adm_chain"
        }
        fn first_step() -> &'static str {
            "a"
        }
        fn steps() -> &'static [SagaWorkflowStepContract] {
            &[
                SagaWorkflowStepContract {
                    step_name: "a",
                    participant_id: "a",
                    depends_on: WorkflowDependencySpec::OnSagaStart,
                },
                SagaWorkflowStepContract {
                    step_name: "b",
                    participant_id: "b",
                    depends_on: WorkflowDependencySpec::After("a"),
                },
            ]
        }
        fn terminal_policy() -> TerminalPolicy {
            let mut required: HashSet<Box<str>> = HashSet::new();
            required.insert("b".into());
            TerminalPolicy::new(
                "adm_chain".into(),
                "adm_chain/policy".into(),
                FailureAuthority::AnyParticipant,
                SuccessCriteria::AllOf(required),
                Duration::from_secs(60),
                Duration::from_secs(60),
                Self::steps(),
            )
        }
    }

    struct AnyOf;
    impl SagaWorkflowContract for AnyOf {
        fn saga_type() -> &'static str {
            "adm_any"
        }
        fn first_step() -> &'static str {
            "a"
        }
        fn steps() -> &'static [SagaWorkflowStepContract] {
            &[
                SagaWorkflowStepContract {
                    step_name: "a",
                    participant_id: "a",
                    depends_on: WorkflowDependencySpec::OnSagaStart,
                },
                SagaWorkflowStepContract {
                    step_name: "b",
                    participant_id: "b",
                    depends_on: WorkflowDependencySpec::OnSagaStart,
                },
            ]
        }
        fn terminal_policy() -> TerminalPolicy {
            let mut any: HashSet<Box<str>> = HashSet::new();
            any.insert("a".into());
            any.insert("b".into());
            TerminalPolicy::new(
                "adm_any".into(),
                "adm_any/policy".into(),
                FailureAuthority::AnyParticipant,
                SuccessCriteria::AnyOf(any),
                Duration::from_secs(60),
                Duration::from_secs(60),
                Self::steps(),
            )
        }
    }

    /// Journal with an honest per-saga lookup that counts and can fail it.
    #[derive(Default)]
    struct LookupJournal {
        inner: InMemoryTerminalResolverJournal,
        read_alls: AtomicUsize,
        lookups: AtomicUsize,
        fail_lookup: AtomicBool,
        fail_append: AtomicBool,
        append_failures: AtomicUsize,
    }

    impl TerminalResolverJournal for LookupJournal {
        fn append(
            &self,
            event: SagaChoreographyEvent,
        ) -> Result<u64, TerminalResolverJournalError> {
            if self.fail_append.load(Ordering::SeqCst) {
                self.append_failures.fetch_add(1, Ordering::SeqCst);
                return Err(TerminalResolverJournalError::Storage("append down".into()));
            }
            self.inner.append(event)
        }
        fn read_all(
            &self,
        ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
            self.read_alls.fetch_add(1, Ordering::SeqCst);
            self.inner.read_all()
        }
        fn supports_saga_lookup(&self) -> bool {
            true
        }
        fn read_saga(
            &self,
            saga_id: SagaId,
        ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
            self.lookups.fetch_add(1, Ordering::SeqCst);
            if self.fail_lookup.load(Ordering::SeqCst) {
                return Err(TerminalResolverJournalError::Storage("lookup down".into()));
            }
            self.inner.read_saga(saga_id)
        }
        fn read_saga_bounded(
            &self,
            saga_id: SagaId,
            max_entries: usize,
        ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
            self.lookups.fetch_add(1, Ordering::SeqCst);
            if self.fail_lookup.load(Ordering::SeqCst) {
                return Err(TerminalResolverJournalError::Storage("lookup down".into()));
            }
            self.inner.read_saga_bounded(saga_id, max_entries)
        }
    }

    type Seen = Arc<Mutex<Vec<SagaChoreographyEvent>>>;

    fn count(seen: &Seen, pred: impl Fn(&SagaChoreographyEvent) -> bool) -> usize {
        seen.lock().unwrap().iter().filter(|e| pred(e)).count()
    }

    fn is_start(e: &SagaChoreographyEvent) -> bool {
        matches!(e, SagaChoreographyEvent::SagaStarted { .. })
    }
    fn is_completed(e: &SagaChoreographyEvent) -> bool {
        matches!(e, SagaChoreographyEvent::SagaCompleted { .. })
    }
    fn is_quarantined(e: &SagaChoreographyEvent) -> bool {
        matches!(e, SagaChoreographyEvent::SagaQuarantined { .. })
    }

    fn ctx(saga_type: &str, id: u64, step: &str, started_at: u64) -> SagaContext {
        SagaContext {
            saga_id: SagaId::new(id),
            saga_type: saga_type.into(),
            step_name: step.into(),
            correlation_id: id,
            causation_id: 0,
            trace_id: id,
            step_index: 0,
            attempt: 0,
            initiator_peer_id: [0; 32],
            saga_started_at_millis: started_at,
            event_timestamp_millis: started_at,
        }
    }

    fn started(c: &SagaContext) -> SagaChoreographyEvent {
        SagaChoreographyEvent::SagaStarted {
            context: c.clone(),
            payload: vec![1],
        }
    }

    fn completed(
        c: &SagaContext,
        step: &str,
        trace: u64,
        compensable: bool,
    ) -> SagaChoreographyEvent {
        let mut context = c.next_step(step.into());
        context.trace_id = trace;
        SagaChoreographyEvent::StepCompleted {
            context,
            output: vec![1],
            saga_input: vec![1],
            compensation_available: compensable,
        }
    }

    fn accepted(c: &SagaContext, step: &str, execution: &str) -> SagaChoreographyEvent {
        SagaChoreographyEvent::StepAccepted {
            context: c.next_step(step.into()),
            participant_id: step.into(),
            execution_id: StepExecutionId::new(execution),
            deadline_at_millis: u64::MAX / 2,
            hard_deadline_at_millis: u64::MAX / 2,
            timeouts_enabled: true,
            timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: true,
            },
            compensation_available: true,
        }
    }

    fn saga_failed(c: &SagaContext) -> SagaChoreographyEvent {
        SagaChoreographyEvent::SagaFailed {
            context: c.next_step(TERMINAL_RESOLVER_STEP.into()),
            reason: "ordinary".into(),
            failure: None,
        }
    }

    fn wait_until(limit: Duration, mut pred: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + limit;
        while Instant::now() < deadline {
            if pred() {
                return true;
            }
            thread::sleep(Duration::from_millis(5));
        }
        pred()
    }

    fn bus_with<C: SagaWorkflowContract>(
        journal: Option<Arc<dyn TerminalResolverJournal>>,
        limits: AdmissionLimits,
    ) -> (SagaChoreographyBus, Seen) {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<C>().unwrap();
        for step in C::steps() {
            bus.subscribe_workflow_step_fn(C::saga_type(), step.step_name, |_| true)
                .unwrap();
        }
        let seen: Seen = Arc::default();
        let sink = Arc::clone(&seen);
        bus.subscribe_saga_type_fn(C::saga_type(), move |event| {
            sink.lock().unwrap().push(event.clone());
            true
        });
        for _ in 0..3 {
            bus.subscribe_saga_type_fn(C::saga_type(), |_| true);
        }
        let durable = journal.is_some();
        bus.attach_terminal_resolver_inner(C::terminal_policy(), "qa", journal, limits)
            .unwrap();
        if durable {
            bus.activate_terminal_resolver_recovery(C::saga_type())
                .unwrap();
        }
        (bus, seen)
    }

    fn limits(max_ids: usize, max_fingerprints: usize) -> AdmissionLimits {
        AdmissionLimits {
            max_ids,
            max_fingerprints,
        }
    }

    fn resident(bus: &SagaChoreographyBus, saga_type: &str) -> (usize, usize) {
        let gate = bus
            .read_state()
            .runtimes
            .get(saga_type)
            .map(|r| Arc::clone(&r.gate))
            .unwrap();
        let index = gate.index();
        (index.resident_ids(), index.fingerprint_count())
    }

    fn complete_chain(bus: &SagaChoreographyBus, c: &SagaContext, seen: &Seen, want: usize) {
        bus.publish_strict(started(c)).unwrap();
        bus.publish_strict(completed(c, "a", 1, true)).unwrap();
        bus.publish_strict(completed(c, "b", 2, true)).unwrap();
        assert!(
            wait_until(Duration::from_secs(5), || count(seen, is_completed) == want),
            "run {} completes",
            c.saga_id.get()
        );
    }

    fn rejected_reason(
        result: Result<icanact_core::local::PublishStats, SagaBusPublishError>,
    ) -> String {
        match result {
            Err(SagaBusPublishError::AdmissionRejected { reason, .. }) => reason.to_string(),
            other => panic!("expected admission rejection, got {other:?}"),
        }
    }

    // ------------------------------------------------ successful siblings

    #[test]
    fn accepted_and_compensable_siblings_after_anyof_success_never_quarantine() {
        for durable in [false, true] {
            let journal: Option<Arc<dyn TerminalResolverJournal>> =
                durable.then(|| Arc::new(InMemoryTerminalResolverJournal::default()) as Arc<_>);
            let (bus, seen) = bus_with::<AnyOf>(journal, AdmissionLimits::default());
            let c = ctx("adm_any", 10, "a", SagaContext::now_millis());
            bus.publish_strict(started(&c)).unwrap();
            bus.publish_strict(completed(&c, "a", 1, true)).unwrap();
            assert!(wait_until(Duration::from_secs(3), || count(
                &seen,
                is_completed
            ) == 1));
            bus.publish_strict(accepted(&c, "b", "late-exec")).unwrap();
            bus.publish_strict(completed(&c, "b", 99, true)).unwrap();
            thread::sleep(Duration::from_millis(200));
            assert_eq!(count(&seen, is_quarantined), 0, "durable={durable}");
        }
    }

    #[test]
    fn new_accepted_effect_after_failure_still_escalates_once() {
        let journal = Arc::new(LookupJournal::default());
        let c = ctx("adm_chain", 11, "a", 1_000_000);
        journal.inner.append(started(&c)).unwrap();
        journal.inner.append(accepted(&c, "a", "known")).unwrap();
        journal.inner.append(saga_failed(&c)).unwrap();
        let (bus, seen) = bus_with::<Chain>(Some(journal), AdmissionLimits::default());
        bus.publish_strict(accepted(&c, "a", "known")).unwrap();
        thread::sleep(Duration::from_millis(150));
        assert_eq!(count(&seen, is_quarantined), 0, "known replay is quiet");
        bus.publish_strict(accepted(&c, "b", "new")).unwrap();
        assert!(wait_until(Duration::from_secs(3), || count(
            &seen,
            is_quarantined
        ) == 1));
    }

    // ------------------------------------------------------ bounded index

    #[test]
    fn ephemeral_ordinary_runs_evict_fifo() {
        let (bus, seen) = bus_with::<Chain>(None, limits(2, 1_000));
        let base = SagaContext::now_millis();
        for offset in 0..200 {
            complete_chain(
                &bus,
                &ctx("adm_chain", 21 + offset, "a", base),
                &seen,
                offset as usize + 1,
            );
            assert!(resident(&bus, "adm_chain").0 <= 2);
        }
    }

    #[test]
    fn fingerprint_budget_refuses_admission_and_is_released_by_success() {
        let (bus, seen) = bus_with::<Chain>(None, limits(100, 2));
        let base = SagaContext::now_millis();
        let first = ctx("adm_chain", 31, "a", base);
        bus.publish_strict(started(&first)).unwrap();
        bus.publish_strict(completed(&first, "a", 1, true)).unwrap();
        bus.publish_strict(completed(&first, "x", 2, true)).unwrap();
        assert!(wait_until(Duration::from_secs(3), || resident(
            &bus,
            "adm_chain"
        )
        .1 == 2));
        let reason = rejected_reason(bus.publish_strict(started(&ctx("adm_chain", 32, "a", base))));
        assert!(reason.contains("capacity"), "{reason}");
        // Non-compensable success adds no fingerprint at the exact hard budget.
        bus.publish_strict(completed(&first, "b", 3, false))
            .unwrap();
        assert!(wait_until(Duration::from_secs(3), || count(
            &seen,
            is_completed
        ) == 1));
        assert!(
            wait_until(Duration::from_secs(3), || resident(&bus, "adm_chain").1
                == 0),
            "a successful run retains no effect fingerprints"
        );
        bus.publish_strict(started(&ctx("adm_chain", 32, "a", base)))
            .expect("budget released");
    }

    #[test]
    fn lookup_journal_bounds_cache_and_reloads_fences_without_global_reads() {
        let journal = Arc::new(LookupJournal::default());
        let (bus, seen) = bus_with::<Chain>(
            Some(Arc::clone(&journal) as Arc<dyn TerminalResolverJournal>),
            limits(3, 1_000),
        );
        let base = SagaContext::now_millis();
        for id in 100..112u64 {
            complete_chain(
                &bus,
                &ctx("adm_chain", id, "a", base),
                &seen,
                (id - 99) as usize,
            );
            thread::sleep(Duration::from_millis(30));
        }
        assert!(
            resident(&bus, "adm_chain").0 <= 4,
            "terminal fences are evicted, got {}",
            resident(&bus, "adm_chain").0
        );
        // Evicted run: replayed start is refused from the durable fence.
        let replay =
            rejected_reason(bus.publish_strict(started(&ctx("adm_chain", 100, "a", base))));
        assert!(replay.contains("replay"), "{replay}");
        // Evicted run: stale non-start event is dropped, not journaled or answered.
        let rows = journal.inner.read_all().unwrap().len();
        let outputs = count(&seen, |_| true);
        bus.publish_strict(SagaChoreographyEvent::StepFailed {
            context: ctx("adm_chain", 101, "a", base).next_step("a".into()),
            participant_id: "a".into(),
            error: "late".into(),
            error_code: None,
            requires_compensation: false,
        })
        .unwrap();
        thread::sleep(Duration::from_millis(200));
        assert_eq!(journal.inner.read_all().unwrap().len(), rows);
        assert_eq!(
            count(&seen, |_| true),
            outputs + 1,
            "only the published step is seen"
        );
        assert_eq!(
            journal.read_alls.load(Ordering::SeqCst),
            1,
            "history read once at attach"
        );
        assert!(journal.lookups.load(Ordering::SeqCst) > 0);
    }

    #[test]
    fn evicted_success_replay_stays_successful_and_evicted_failure_still_escalates() {
        let journal = Arc::new(LookupJournal::default());
        let base = SagaContext::now_millis();
        let ok = ctx("adm_any", 201, "a", base);
        let (bus, seen) = bus_with::<AnyOf>(
            Some(Arc::clone(&journal) as Arc<dyn TerminalResolverJournal>),
            limits(2, 1_000),
        );
        bus.publish_strict(started(&ok)).unwrap();
        bus.publish_strict(completed(&ok, "a", 1, true)).unwrap();
        assert!(wait_until(Duration::from_secs(3), || count(
            &seen,
            is_completed
        ) == 1));
        for id in 202..208u64 {
            let c = ctx("adm_any", id, "a", base);
            bus.publish_strict(started(&c)).unwrap();
            bus.publish_strict(completed(&c, "a", 1, true)).unwrap();
            assert!(wait_until(Duration::from_secs(3), || {
                count(&seen, is_completed) == (id - 200) as usize
            }));
            thread::sleep(Duration::from_millis(30));
        }
        bus.publish_strict(accepted(&ok, "b", "late")).unwrap();
        bus.publish_strict(completed(&ok, "b", 77, true)).unwrap();
        thread::sleep(Duration::from_millis(200));
        assert_eq!(count(&seen, is_quarantined), 0);
    }

    #[test]
    fn journal_lookup_failure_refuses_admission_visibly_then_recovers() {
        let journal = Arc::new(LookupJournal::default());
        let (bus, seen) = bus_with::<Chain>(
            Some(Arc::clone(&journal) as Arc<dyn TerminalResolverJournal>),
            AdmissionLimits::default(),
        );
        let c = ctx("adm_chain", 301, "a", SagaContext::now_millis());
        journal.fail_lookup.store(true, Ordering::SeqCst);
        let reason = rejected_reason(bus.publish_strict(started(&c)));
        assert!(reason.contains("lookup"), "{reason}");
        thread::sleep(Duration::from_millis(100));
        assert_eq!(
            count(&seen, is_start),
            0,
            "no fanout without proven history"
        );
        journal.fail_lookup.store(false, Ordering::SeqCst);
        bus.publish_strict(started(&c)).unwrap();
        assert!(wait_until(Duration::from_secs(2), || count(
            &seen, is_start
        ) == 1));
    }

    #[test]
    fn custom_journal_without_lookup_keeps_every_fence_and_refuses_at_capacity() {
        struct NoLookup(InMemoryTerminalResolverJournal);
        impl TerminalResolverJournal for NoLookup {
            fn append(
                &self,
                e: SagaChoreographyEvent,
            ) -> Result<u64, TerminalResolverJournalError> {
                self.0.append(e)
            }
            fn read_all(
                &self,
            ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError>
            {
                self.0.read_all()
            }
        }
        let journal: Arc<dyn TerminalResolverJournal> =
            Arc::new(NoLookup(InMemoryTerminalResolverJournal::default()));
        let (bus, seen) = bus_with::<Chain>(Some(journal), limits(2, 1_000));
        let base = SagaContext::now_millis();
        complete_chain(&bus, &ctx("adm_chain", 401, "a", base), &seen, 1);
        complete_chain(&bus, &ctx("adm_chain", 402, "a", base), &seen, 2);
        let reason =
            rejected_reason(bus.publish_strict(started(&ctx("adm_chain", 403, "a", base))));
        assert!(reason.contains("capacity"), "{reason}");
    }

    #[test]
    fn ephemeral_same_id_runs_remain_bounded() {
        let (bus, seen) = bus_with::<Chain>(None, limits(2, 1_000));
        let base = SagaContext::now_millis();
        for offset in 0..200 {
            complete_chain(
                &bus,
                &ctx("adm_chain", 450, "a", base + offset),
                &seen,
                offset as usize + 1,
            );
            assert!(resident(&bus, "adm_chain").0 <= 2);
        }
    }

    #[test]
    fn active_effect_fingerprints_cannot_grow_past_the_budget() {
        let (bus, seen) = bus_with::<Chain>(None, limits(10, 2));
        let c = ctx("adm_chain", 451, "a", SagaContext::now_millis());
        bus.publish_strict(started(&c)).unwrap();
        for trace in 1..=3 {
            bus.publish_strict(completed(&c, "a", trace, true)).unwrap();
        }
        assert!(
            wait_until(Duration::from_secs(3), || count(&seen, is_quarantined) == 1),
            "overflow of effect evidence must quarantine rather than silently grow"
        );
        assert!(resident(&bus, "adm_chain").1 <= 2);
        let later = ctx("adm_chain", 451, "a", c.saga_started_at_millis + 1);
        assert!(rejected_reason(bus.publish_strict(started(&later))).contains("quarantined"));
    }

    #[test]
    fn reloadable_eviction_never_discards_the_current_ids_old_run_fences() {
        let journal = Arc::new(LookupJournal::default());
        let (bus, seen) = bus_with::<Chain>(Some(journal), limits(2, 1_000));
        let base = SagaContext::now_millis();
        let old = ctx("adm_chain", 455, "a", base);
        complete_chain(&bus, &old, &seen, 1);
        complete_chain(&bus, &ctx("adm_chain", 456, "a", base), &seen, 2);
        let new = ctx("adm_chain", 455, "a", base + 1);
        bus.publish_strict(started(&new)).unwrap();
        let replay = rejected_reason(bus.publish_strict(started(&old)));
        assert!(
            replay.contains("replay"),
            "current id must keep its complete history: {replay}"
        );
        bus.publish_strict(completed(&old, "a", 99, true)).unwrap();
        thread::sleep(Duration::from_millis(100));
        assert_eq!(count(&seen, is_quarantined), 0);
        bus.publish_strict(completed(&new, "a", 1, true)).unwrap();
        bus.publish_strict(completed(&new, "b", 2, true)).unwrap();
        assert!(wait_until(Duration::from_secs(3), || count(
            &seen,
            is_completed
        ) == 3));
    }

    #[test]
    fn oversized_unresolved_restore_refuses_instead_of_exceeding_capacity() {
        let journal = Arc::new(LookupJournal::default());
        let base = SagaContext::now_millis();
        for id in 460..463 {
            journal
                .append(started(&ctx("adm_chain", id, "a", base)))
                .unwrap();
        }
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<Chain>().unwrap();
        for step in Chain::steps() {
            bus.subscribe_workflow_step_fn(Chain::saga_type(), step.step_name, |_| true)
                .unwrap();
        }
        let result = bus.attach_terminal_resolver_inner(
            Chain::terminal_policy(),
            "qa",
            Some(journal),
            limits(2, 2),
        );
        assert!(
            matches!(result, Err(ref reason) if reason.contains("capacity")),
            "cannot restore unresolved fences over the configured budget: {result:?}"
        );
    }

    #[cfg(feature = "lmdb")]
    #[test]
    fn lmdb_bounded_cache_survives_eviction_compaction_and_reopen() {
        use crate::LmdbTerminalResolverJournal;
        let dir = tempfile::tempdir().unwrap();
        let base = SagaContext::now_millis();
        {
            let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
            let (bus, seen) = bus_with::<Chain>(
                Some(Arc::clone(&journal) as Arc<dyn TerminalResolverJournal>),
                limits(2, 1_000),
            );
            for id in 500..506u64 {
                complete_chain(
                    &bus,
                    &ctx("adm_chain", id, "a", base),
                    &seen,
                    (id - 499) as usize,
                );
                thread::sleep(Duration::from_millis(30));
            }
            assert!(journal.compact_terminal_detail().unwrap() > 0);
        }
        let journal = Arc::new(LmdbTerminalResolverJournal::open(dir.path()).unwrap());
        let (bus, seen) = bus_with::<Chain>(
            Some(Arc::clone(&journal) as Arc<dyn TerminalResolverJournal>),
            limits(2, 1_000),
        );
        for id in 500..506u64 {
            let reason =
                rejected_reason(bus.publish_strict(started(&ctx("adm_chain", id, "a", base))));
            assert!(reason.contains("replay"), "id {id}: {reason}");
        }
        bus.publish_strict(started(&ctx("adm_chain", 900, "a", base)))
            .expect("new ids are still admitted under a bounded cache");
        assert!(wait_until(Duration::from_secs(2), || count(
            &seen, is_start
        ) == 1));
    }
    // ------------------------------------------------- P2-2 reclamation

    fn index_with_failed_run(
        fingerprint_budget: usize,
        failed_id: u64,
        can_evict: bool,
    ) -> (super::AdmissionIndex, SagaContext) {
        let mut index = super::AdmissionIndex::new(limits(100, fingerprint_budget), can_evict);
        let failed = ctx("adm_chain", failed_id, "a", 1_000);
        assert!(index.observe(&started(&failed)));
        assert!(index.observe(&completed(&failed, "a", 1, true)));
        assert!(index.observe(&completed(&failed, "b", 2, true)));
        assert!(index.observe(&saga_failed(&failed)));
        (index, failed)
    }

    #[test]
    fn steady_state_failed_fingerprints_are_reclaimed_for_an_active_run() {
        let (mut index, _failed) = index_with_failed_run(3, 1, true);
        let active = ctx("adm_chain", 2, "a", 1_000);
        assert!(index.observe(&started(&active)));
        assert!(index.observe(&completed(&active, "a", 1, true)));
        assert_eq!(
            index.fingerprint_count(),
            3,
            "steady state sits at the budget"
        );
        assert!(
            index.observe(&completed(&active, "b", 2, true)),
            "a reloadable failed id must be reclaimed instead of quarantining a healthy run"
        );
        assert!(
            !index.contains(SagaId::new(1)),
            "failed id was evicted whole"
        );
        assert!(index.contains(SagaId::new(2)));
        assert_eq!(index.fingerprint_count(), 2);
    }

    #[test]
    fn reclamation_never_evicts_the_current_ids_complete_history() {
        let (mut index, failed) = index_with_failed_run(3, 1, true);
        // A newer run of the same id needs room; its own older failed run is protected.
        let newer = ctx("adm_chain", 1, "a", 2_000);
        assert!(index.observe(&started(&newer)));
        assert!(index.observe(&completed(&newer, "a", 1, true)));
        assert!(
            !index.observe(&completed(&newer, "b", 2, true)),
            "no safe room: visible failure, not a partial run vector"
        );
        assert!(
            index.run(&failed).is_some(),
            "older failed fence is still resident"
        );
        assert!(index.run(&newer).is_some());
    }

    #[test]
    fn nonlookup_durable_index_never_reclaims_failed_fingerprints() {
        let (mut index, failed) = index_with_failed_run(3, 1, false);
        let active = ctx("adm_chain", 2, "a", 1_000);
        assert!(index.observe(&started(&active)));
        assert!(index.observe(&completed(&active, "a", 1, true)));
        assert!(!index.observe(&completed(&active, "b", 2, true)));
        assert!(
            index.run(&failed).is_some(),
            "non-lookup durable fences are never forgotten"
        );
    }

    #[test]
    fn transient_durable_overflow_recovers_when_capacity_returns() {
        let journal = Arc::new(LookupJournal::default());
        let (bus, seen) = bus_with::<Chain>(
            Some(Arc::clone(&journal) as Arc<dyn TerminalResolverJournal>),
            limits(2, 1_000),
        );
        let base = SagaContext::now_millis();
        let first = ctx("adm_chain", 701, "a", base);
        bus.publish_strict(started(&first)).unwrap();
        bus.publish_strict(started(&ctx("adm_chain", 702, "a", base)))
            .unwrap();
        // Evidence of a run that cannot be represented: visibly quarantined.
        bus.publish_strict(completed(
            &ctx("adm_chain", 702, "a", base + 1),
            "a",
            1,
            false,
        ))
        .unwrap();
        assert!(wait_until(Duration::from_secs(3), || count(
            &seen,
            is_quarantined
        ) >= 1));
        // Capacity genuinely returns when the first run resolves.
        bus.publish_strict(completed(&first, "a", 1, true)).unwrap();
        bus.publish_strict(completed(&first, "b", 2, true)).unwrap();
        assert!(wait_until(Duration::from_secs(3), || count(
            &seen,
            is_completed
        ) == 1));
        bus.publish_strict(started(&ctx("adm_chain", 704, "a", base)))
            .expect("durable overflow must not wedge admission once capacity returns");
    }

    #[test]
    fn delivery_fence_is_retried_even_when_resolver_ingress_is_full() {
        struct ParkJournal {
            inner: InMemoryTerminalResolverJournal,
            armed: AtomicBool,
            parked: std::sync::mpsc::Sender<()>,
            release: (Mutex<bool>, std::sync::Condvar),
        }
        impl TerminalResolverJournal for ParkJournal {
            fn append(
                &self,
                event: SagaChoreographyEvent,
            ) -> Result<u64, TerminalResolverJournalError> {
                if self.armed.swap(false, Ordering::SeqCst) {
                    self.parked.send(()).unwrap();
                    let (lock, wake) = &self.release;
                    let guard = lock.lock().unwrap();
                    drop(wake.wait_while(guard, |released| !*released).unwrap());
                }
                self.inner.append(event)
            }
            fn read_all(
                &self,
            ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError>
            {
                self.inner.read_all()
            }
        }
        struct Unpark(Arc<ParkJournal>);
        impl Drop for Unpark {
            fn drop(&mut self) {
                *self.0.release.0.lock().unwrap() = true;
                self.0.release.1.notify_all();
            }
        }
        let (tx, rx) = std::sync::mpsc::channel();
        let journal = Arc::new(ParkJournal {
            inner: Default::default(),
            armed: AtomicBool::new(false),
            parked: tx,
            release: (Mutex::new(false), std::sync::Condvar::new()),
        });
        let (bus, seen) = bus_with::<Chain>(Some(journal.clone()), limits(8, 128));
        let c = ctx("adm_chain", 7701, "a", SagaContext::now_millis());
        bus.publish_strict(started(&c)).unwrap();
        let waiter = register_run_waiter(&bus, &c);
        journal.armed.store(true, Ordering::SeqCst);
        bus.publish_strict(completed(&c, "a", 1, true)).unwrap();
        let unpark = Unpark(journal.clone());
        rx.recv_timeout(Duration::from_secs(3))
            .expect("resolver parked in journal");
        let (resolver, gate) = {
            let state = bus.read_state();
            let runtime = state.runtimes.get("adm_chain").unwrap();
            (runtime.resolver_ref.clone(), runtime.gate.clone())
        };
        while resolver.tell(super::TerminalResolverTell::PollTimeouts) {}
        assert!(bus.publish_strict(completed(&c, "b", 2, true)).is_err());
        assert_eq!(gate.phase(&c), Some(super::RunPhase::Quarantined));
        assert!(
            quarantine_reply(&waiter),
            "waiter settles while resolver remains parked"
        );
        let next = ctx(
            "adm_chain",
            c.saga_id.get(),
            "a",
            c.saga_started_at_millis + 1,
        );
        assert!(rejected_reason(bus.publish_strict(started(&next))).contains("quarantined"));
        drop(unpark);
        assert!(
            wait_until(Duration::from_secs(3), || gate
                .index()
                .quarantine_durable(&c)),
            "watchdog retries the durable fence without any new evidence"
        );
        assert_eq!(
            count(&seen, is_completed),
            0,
            "queued completion cannot escape quarantine"
        );
    }

    #[test]
    fn pending_fence_retry_is_activation_gated_bounded_and_stops_after_durability() {
        let journal = Arc::new(LookupJournal::default());
        let bus = SagaChoreographyBus::new();
        let gate = Arc::new(ResolverGate {
            journal: Some(journal.clone()),
            _release: bus.inner.release.guard(),
            activated: AtomicBool::new(false),
            admission: Mutex::new(AdmissionIndex::new(limits(32, 128), true)),
        });
        let mut actor = super::TerminalResolverActor {
            resolver: crate::TerminalResolver::new(Chain::terminal_policy()),
            recovery_events: Vec::new(),
            held_events: Vec::new(),
            held_quarantine_replies: Default::default(),
            activated: false,
            gate: Arc::clone(&gate),
            bus: Arc::downgrade(&bus.inner),
            responder: Arc::from("qa"),
            saga_type: "adm_chain".into(),
            _release: bus.inner.release.guard(),
        };
        let base = SagaContext::now_millis();
        for id in 0..20 {
            let context = ctx("adm_chain", 800 + id, "a", base);
            actor.ingest_event(&started(&context));
            actor.fence_live(&context);
            let quarantine = actor.fence_event(&context);
            actor.resolver.ingest(&quarantine);
            actor.track_pending_fence(&context);
        }
        assert_eq!(
            gate.index().pending_fence_keys.len(),
            20,
            "one key per resident run"
        );
        assert_eq!(
            gate.index().pending_durable_quarantine.len(),
            20,
            "duplicate tracking is bounded"
        );
        let quarantine_count = || {
            journal
                .inner
                .read_all()
                .unwrap()
                .iter()
                .filter(|row| is_quarantined(&row.event))
                .count()
        };
        actor.retry_pending_fences();
        assert_eq!(quarantine_count(), 0, "no retry before recovery activation");
        actor.activated = true;
        journal.fail_append.store(true, Ordering::SeqCst);
        actor.retry_pending_fences();
        assert_eq!(
            journal.append_failures.load(Ordering::SeqCst),
            1,
            "failed tick does not spin"
        );
        assert_eq!(gate.index().pending_durable_quarantine.len(), 20);
        journal.fail_append.store(false, Ordering::SeqCst);
        for (durable, pending) in [(8, 12), (16, 4), (20, 0)] {
            actor.retry_pending_fences();
            assert_eq!(quarantine_count(), durable, "at most eight fences per tick");
            assert_eq!(gate.index().pending_durable_quarantine.len(), pending);
            assert_eq!(gate.index().pending_fence_keys.len(), pending);
        }
        actor.retry_pending_fences();
        for id in 0..20 {
            let context = ctx("adm_chain", 800 + id, "a", base);
            actor.track_pending_fence(&context);
            assert!(!gate.quarantine_pending(&context));
        }
        assert_eq!(gate.index().pending_durable_quarantine.len(), 0);
        assert_eq!(quarantine_count(), 20, "durable rows are never retried");
        assert!(!gate.index().exceeds_capacity());
    }

    #[test]
    fn capacity_failure_of_unknown_newer_run_publishes_and_persists_the_active_run_quarantine() {
        let journal = Arc::new(LookupJournal::default());
        let (bus, seen) = bus_with::<Chain>(
            Some(Arc::clone(&journal) as Arc<dyn TerminalResolverJournal>),
            limits(1, 100),
        );
        let base = SagaContext::now_millis();
        let active = ctx("adm_chain", 741, "a", base);
        let unknown_newer = ctx("adm_chain", 741, "a", base + 1);
        bus.publish_strict(started(&active)).unwrap();
        let (pending, probe) = register_pending_reply(bus.clone(), active.saga_id);
        thread::sleep(Duration::from_millis(50));

        // The newer run cannot be represented (capacity 1 run), so its evidence
        // is quarantined; the resolver's cross-run rule fences the active run.
        bus.publish_strict(completed(&unknown_newer, "a", 1, true))
            .unwrap();

        let (tx, rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let _ = tx.send(pending.wait());
        });
        let reply = rx
            .recv_timeout(Duration::from_secs(3))
            .expect("the active run's waiter must resolve")
            .expect("waiter reply")
            .expect("quarantine is a terminal saga reply");
        assert!(matches!(
            reply.outcome,
            SagaTerminalOutcome::Quarantined { .. }
        ));
        probe.shutdown();
        let active_quarantined = |event: &SagaChoreographyEvent| {
            is_quarantined(event)
                && event.context().saga_started_at_millis == active.saga_started_at_millis
        };
        assert!(wait_until(Duration::from_secs(3), || count(
            &seen,
            active_quarantined
        ) >= 1));
        assert!(
            wait_until(Duration::from_secs(3), || journal
                .inner
                .read_saga(active.saga_id)
                .unwrap()
                .iter()
                .any(|entry| active_quarantined(&entry.event))),
            "the active run's quarantine is persisted"
        );
        let gate = bus
            .read_state()
            .runtimes
            .get("adm_chain")
            .map(|r| Arc::clone(&r.gate))
            .unwrap();
        assert_eq!(gate.index().phase(&active), Some(RunPhase::Quarantined));
        assert!(
            bus.publish_strict(started(&ctx("adm_chain", 741, "a", base + 2)))
                .is_err(),
            "the id stays refused behind the quarantined active run"
        );
    }

    #[test]
    fn known_run_durability_quarantine_cannot_later_publish_success() {
        let journal = Arc::new(LookupJournal::default());
        let (bus, seen) = bus_with::<Chain>(
            Some(Arc::clone(&journal) as Arc<dyn TerminalResolverJournal>),
            limits(8, 32),
        );
        let base = SagaContext::now_millis();
        let affected = ctx("adm_chain", 721, "a", base);
        bus.publish_strict(started(&affected)).unwrap();
        journal.fail_append.store(true, Ordering::SeqCst);
        bus.publish_strict(completed(&affected, "a", 1, true))
            .unwrap();
        assert!(
            wait_until(Duration::from_secs(3), || {
                journal.append_failures.load(Ordering::SeqCst) >= 2
                    && count(&seen, is_quarantined) == 1
            }),
            "both the failed result and its quarantine echo were processed"
        );
        journal.fail_append.store(false, Ordering::SeqCst);
        bus.publish_strict(completed(&affected, "a", 2, true))
            .unwrap();
        bus.publish_strict(completed(&affected, "b", 3, true))
            .unwrap();
        // A later run's completion is an inbox barrier, not a sleep-based absence check.
        let barrier = ctx("adm_chain", 722, "a", base);
        bus.publish_strict(started(&barrier)).unwrap();
        bus.publish_strict(completed(&barrier, "a", 1, true))
            .unwrap();
        bus.publish_strict(completed(&barrier, "b", 2, true))
            .unwrap();
        assert!(wait_until(Duration::from_secs(3), || {
            seen.lock()
                .unwrap()
                .iter()
                .any(|event| is_completed(event) && event.context().saga_id == barrier.saga_id)
        }));
        let succeeded = seen
            .lock()
            .unwrap()
            .iter()
            .any(|event| is_completed(event) && event.context().saga_id == affected.saga_id);
        assert!(
            !succeeded,
            "storage recovery cannot downgrade a published quarantine"
        );
        assert!(
            journal
                .inner
                .read_saga(affected.saga_id)
                .unwrap()
                .iter()
                .any(|entry| { is_quarantined(&entry.event) }),
            "recovered storage must retain the live quarantine before new evidence"
        );
    }

    #[test]
    fn complete_resident_merges_protected_history_and_preserves_prejournaled_start() {
        let journal = LookupJournal::default();
        let base = SagaContext::now_millis();
        let old = ctx("adm_chain", 731, "a", base);
        let new = ctx("adm_chain", 731, "a", base + 1);
        let other = ctx("adm_chain", 732, "a", base);
        let mut index = AdmissionIndex::new(limits(2, 100), true);
        assert!(index.observe(&started(&old)));
        index.mark_prejournaled(&old);
        assert!(index.observe(&saga_failed(&old)));
        assert!(index.observe(&started(&other)));
        assert!(!index.raise(new.saga_id, new.saga_started_at_millis, RunPhase::Active));
        assert!(index.ids.get(&old.saga_id).unwrap().incomplete);
        journal.inner.append(started(&old)).unwrap();
        journal.inner.append(saga_failed(&old)).unwrap();
        journal.inner.append(started(&new)).unwrap();
        journal
            .inner
            .append(SagaChoreographyEvent::SagaQuarantined {
                context: new.next_step(TERMINAL_RESOLVER_STEP.into()),
                reason: "original durable quarantine".into(),
                step: "a".into(),
                participant_id: "a".into(),
            })
            .unwrap();
        assert!(index.observe(&saga_failed(&other)));
        ResolverGate::complete_resident(&journal, &mut index, old.saga_id).unwrap();
        assert!(!index.ids.get(&old.saga_id).unwrap().incomplete);
        assert_eq!(index.phase(&old), Some(RunPhase::Failed));
        assert_eq!(index.phase(&new), Some(RunPhase::Quarantined));
        assert_eq!(index.runs, 2);
        assert!(!index.ids.contains_key(&other.saga_id));
        assert!(index.take_prejournaled(&old));
        assert_eq!(journal.lookups.load(Ordering::SeqCst), 1);
        assert_eq!(journal.read_alls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn unrepresented_durability_uncertainty_stays_fail_closed_when_room_returns() {
        let journal = Arc::new(LookupJournal::default());
        let (bus, seen) = bus_with::<Chain>(
            Some(Arc::clone(&journal) as Arc<dyn TerminalResolverJournal>),
            limits(1, 100),
        );
        let base = SagaContext::now_millis();
        let owner = ctx("adm_chain", 741, "a", base);
        let unknown = ctx("adm_chain", 742, "a", base);
        bus.publish_strict(started(&owner)).unwrap();
        journal.fail_append.store(true, Ordering::SeqCst);
        bus.publish_strict(completed(&unknown, "a", 1, true))
            .unwrap();
        assert!(wait_until(Duration::from_secs(3), || {
            journal.append_failures.load(Ordering::SeqCst) >= 2 && count(&seen, is_quarantined) >= 1
        }));
        journal.fail_append.store(false, Ordering::SeqCst);
        bus.publish_strict(completed(&owner, "a", 1, true)).unwrap();
        bus.publish_strict(completed(&owner, "b", 2, true)).unwrap();
        assert!(wait_until(Duration::from_secs(3), || count(
            &seen,
            is_completed
        ) == 1));
        assert!(
            rejected_reason(bus.publish_strict(started(&ctx("adm_chain", 743, "a", base))))
                .contains("capacity")
        );
    }

    #[test]
    fn ephemeral_overflow_stays_conservative_after_capacity_returns() {
        let (bus, seen) = bus_with::<Chain>(None, limits(2, 1_000));
        let base = SagaContext::now_millis();
        let first = ctx("adm_chain", 711, "a", base);
        bus.publish_strict(started(&first)).unwrap();
        bus.publish_strict(started(&ctx("adm_chain", 712, "a", base)))
            .unwrap();
        bus.publish_strict(completed(
            &ctx("adm_chain", 712, "a", base + 1),
            "a",
            1,
            false,
        ))
        .unwrap();
        assert!(wait_until(Duration::from_secs(3), || count(
            &seen,
            is_quarantined
        ) >= 1));
        bus.publish_strict(completed(&first, "a", 1, true)).unwrap();
        bus.publish_strict(completed(&first, "b", 2, true)).unwrap();
        assert!(wait_until(Duration::from_secs(3), || count(
            &seen,
            is_completed
        ) == 1));
        let reason =
            rejected_reason(bus.publish_strict(started(&ctx("adm_chain", 714, "a", base))));
        assert!(reason.contains("capacity"), "{reason}");
    }

    // ------------------------------------------- evicted known-run quarantine

    /// Registers a full-run waiter from an off-actor probe and waits for it on a
    /// helper thread, so the test thread only ever uses bounded receives.
    fn register_run_waiter(
        bus: &SagaChoreographyBus,
        context: &SagaContext,
    ) -> std::sync::mpsc::Receiver<Result<SagaReplyToResult, String>> {
        let (registered_tx, registered_rx) = std::sync::mpsc::channel();
        let (probe, handle) = local_sync::mpsc::spawn(
            8,
            move |(bus, context, reply): (SagaChoreographyBus, SagaContext, SagaReplyToHandle)| {
                let _ = registered_tx.send(bus.register_terminal_reply_for_run(&context, reply));
            },
        );
        let pending = probe
            .ask_delegated(|reply| (bus.clone(), context.clone(), reply))
            .expect("run waiter enqueued");
        let (tx, rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let _keep = handle;
            let _ = tx.send(pending.wait().map_err(|error| format!("{error:?}")));
        });
        registered_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("run waiter registration observed")
            .expect("run waiter registered");
        rx
    }

    fn participant_quarantine(c: &SagaContext, reason: &str) -> SagaChoreographyEvent {
        SagaChoreographyEvent::SagaQuarantined {
            context: c.next_step("a".into()),
            reason: reason.into(),
            step: "a".into(),
            participant_id: "a".into(),
        }
    }

    /// Resolves run `first_id` ordinarily, then resolves enough newer ids that
    /// `first_id` is evicted from the bounded resident index.
    fn evicted_resolved_run(
        journal: &Arc<LookupJournal>,
        first_id: u64,
        base: u64,
    ) -> (SagaChoreographyBus, SagaContext) {
        let (bus, seen) = bus_with::<Chain>(
            Some(Arc::clone(journal) as Arc<dyn TerminalResolverJournal>),
            limits(2, 1_000),
        );
        let run = ctx("adm_chain", first_id, "a", base);
        for (n, id) in (first_id..first_id + 4).enumerate() {
            complete_chain(&bus, &ctx("adm_chain", id, "a", base), &seen, n + 1);
            thread::sleep(Duration::from_millis(30));
        }
        let gate = bus
            .read_state()
            .runtimes
            .get("adm_chain")
            .map(|r| Arc::clone(&r.gate))
            .unwrap();
        assert!(
            !gate.index().contains(run.saga_id),
            "the ordinarily resolved run must be genuinely evicted"
        );
        (bus, run)
    }

    fn quarantine_reply(rx: &std::sync::mpsc::Receiver<Result<SagaReplyToResult, String>>) -> bool {
        match rx.recv_timeout(Duration::from_secs(3)) {
            Ok(Ok(Ok(reply))) => matches!(reply.outcome, SagaTerminalOutcome::Quarantined { .. }),
            _ => false,
        }
    }

    fn unresolved(rx: &std::sync::mpsc::Receiver<Result<SagaReplyToResult, String>>) -> bool {
        matches!(
            rx.recv_timeout(Duration::from_millis(400)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        )
    }

    #[test]
    fn participant_quarantine_of_evicted_resolved_run_resolves_later_run_waiter() {
        let journal = Arc::new(LookupJournal::default());
        let base = SagaContext::now_millis();
        let (bus, run) = evicted_resolved_run(&journal, 1_100, base);
        let waiter = register_run_waiter(&bus, &run);
        let global_reads = journal.read_alls.load(Ordering::SeqCst);
        let point_reads = journal.lookups.load(Ordering::SeqCst);

        bus.publish_strict(participant_quarantine(&run, "late participant uncertainty"))
            .unwrap();

        assert!(
            quarantine_reply(&waiter),
            "durable history proves the run, so its later waiter must be quarantined"
        );
        assert_eq!(
            journal.read_alls.load(Ordering::SeqCst),
            global_reads,
            "classification is a bounded point read, never a global scan"
        );
        assert_eq!(
            journal.lookups.load(Ordering::SeqCst),
            point_reads + 1,
            "the complete resident-miss reload serves classification and ingestion"
        );
    }

    #[test]
    fn evicted_run_quarantine_resolves_only_the_proven_full_run() {
        let journal = Arc::new(LookupJournal::default());
        let base = SagaContext::now_millis();
        let (bus, run) = evicted_resolved_run(&journal, 1_200, base);
        let unknown = ctx("adm_chain", 1_299, "a", base);
        let successor = ctx("adm_chain", run.saga_id.get(), "a", base + 9);
        let run_rx = register_run_waiter(&bus, &run);
        let unknown_rx = register_run_waiter(&bus, &unknown);
        let successor_rx = register_run_waiter(&bus, &successor);

        bus.publish_strict(participant_quarantine(&run, "proven run uncertainty"))
            .unwrap();

        assert!(quarantine_reply(&run_rx), "the proven full run resolves");
        assert!(
            unresolved(&unknown_rx),
            "an unknown id is not guessed owned"
        );
        assert!(
            unresolved(&successor_rx),
            "a different full run of the same id is not the proven run"
        );
    }

    #[test]
    fn known_run_lookup_preserves_resident_authority_and_refuses_unproved_history() {
        let journal = Arc::new(LookupJournal::default());
        let (bus, _) = bus_with::<Chain>(
            Some(Arc::clone(&journal) as Arc<dyn TerminalResolverJournal>),
            limits(2, 8),
        );
        let gate = bus
            .read_state()
            .runtimes
            .get("adm_chain")
            .map(|r| Arc::clone(&r.gate))
            .unwrap();
        let base = SagaContext::now_millis();
        let run = ctx("adm_chain", 1_400, "a", base);
        journal.inner.append(started(&run)).unwrap();
        journal
            .inner
            .append(SagaChoreographyEvent::SagaCompleted {
                context: run.next_step(TERMINAL_RESOLVER_STEP.into()),
            })
            .unwrap();
        let global_reads = journal.read_alls.load(Ordering::SeqCst);
        let point_reads = journal.lookups.load(Ordering::SeqCst);
        assert!(!gate.index().contains(run.saga_id));
        assert!(gate.known_run(&run));
        assert_eq!(gate.phase(&run), Some(RunPhase::Completed));
        assert_eq!(journal.lookups.load(Ordering::SeqCst), point_reads + 1);

        // An outage cannot erase an already represented exact-run owner.
        journal.fail_lookup.store(true, Ordering::SeqCst);
        assert!(gate.known_run(&run));
        let different_run = ctx("adm_chain", 1_400, "a", base + 9);
        assert!(!gate.known_run(&different_run));
        assert_eq!(journal.lookups.load(Ordering::SeqCst), point_reads + 1);
        let absent = ctx("adm_chain", 1_499, "a", base);
        assert!(!gate.known_run(&absent));
        assert!(!gate.index().contains(absent.saga_id));
        journal.fail_lookup.store(false, Ordering::SeqCst);
        assert!(!gate.known_run(&absent));

        // The row budget is max_ids + max_fingerprints = 10. An oversized
        // history must fail before any partial run can become authority.
        let oversized = ctx("adm_chain", 1_500, "a", base + 100);
        for n in 0..11 {
            journal
                .inner
                .append(started(&ctx("adm_chain", 1_500, "a", base + 100 + n)))
                .unwrap();
        }
        assert!(!gate.known_run(&oversized));
        assert!(!gate.index().contains(oversized.saga_id));
        assert!(gate.known_run(&run));
        assert_eq!(journal.read_alls.load(Ordering::SeqCst), global_reads);
    }

    #[test]
    fn evicted_run_quarantine_with_failed_lookup_stays_fail_closed() {
        let journal = Arc::new(LookupJournal::default());
        let base = SagaContext::now_millis();
        let (bus, run) = evicted_resolved_run(&journal, 1_300, base);
        let waiter = register_run_waiter(&bus, &run);
        journal.fail_lookup.store(true, Ordering::SeqCst);
        bus.publish_strict(participant_quarantine(&run, "lookup is down"))
            .unwrap();
        assert!(
            unresolved(&waiter),
            "unreadable durable history never proves ownership"
        );
    }
}

#[cfg(test)]
mod wave1b_tests {
    use super::*;
    use std::sync::mpsc;
    fn context(id: u64) -> SagaContext {
        SagaContext {
            saga_id: SagaId::new(id),
            saga_type: "wave1b".into(),
            step_name: "a".into(),
            correlation_id: id,
            causation_id: id,
            trace_id: id,
            step_index: 0,
            attempt: 0,
            initiator_peer_id: [0; 32],
            saga_started_at_millis: SagaContext::now_millis(),
            event_timestamp_millis: SagaContext::now_millis(),
        }
    }

    fn waiter(
        bus: &SagaChoreographyBus,
        context: &SagaContext,
        legacy: bool,
    ) -> (
        local_sync::PendingAsk<SagaReplyToResult>,
        Result<(), SagaBusPublishError>,
        local_sync::mpsc::ActorHandle,
    ) {
        let (tx, rx) = mpsc::channel();
        let (probe, handle) = local_sync::mpsc::spawn(
            8,
            move |(bus, context, reply): (SagaChoreographyBus, SagaContext, SagaReplyToHandle)| {
                let result = if legacy {
                    bus.register_terminal_reply(context.saga_id, reply)
                } else {
                    bus.register_terminal_reply_for_run(&context, reply)
                };
                tx.send(result).unwrap();
            },
        );
        let pending = probe
            .ask_delegated(|reply| (bus.clone(), context.clone(), reply))
            .unwrap();
        let result = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        (pending, result, handle)
    }

    crate::define_saga_workflow_contract! {
        struct Workflow {
            saga_type: "wave1b",
            first_step: a,
            failure_authority: any (),
            required_steps: [a],
            overall_timeout_ms: 30_000,
            stalled_timeout_ms: 10_000,
            steps: { a => { participant: "a", depends_on: on_start () } }
        }
    }

    #[test]
    fn role_subscription_owns_dynamic_registration_names() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<Workflow>()
            .unwrap();
        bus.attach_terminal_resolver_for_contract::<Workflow>("qa")
            .unwrap();
        {
            let saga_type = String::from("wave1b");
            let step_name = String::from("a");
            bus.subscribe_workflow_step_fn(&saga_type, &step_name, |_| true)
                .unwrap();
        }
        assert!(
            bus.publish_strict(SagaChoreographyEvent::SagaStarted {
                context: context(94),
                payload: Vec::new()
            })
            .is_ok()
        );
    }

    #[test]
    fn observers_cannot_mask_missing_roles_and_failed_observers_are_diagnostic_only() {
        for participant_live in [false, true] {
            let bus = SagaChoreographyBus::new();
            bus.register_workflow_contract_provider::<Workflow>()
                .unwrap();
            bus.attach_terminal_resolver_for_contract::<Workflow>("qa")
                .unwrap();
            bus.subscribe_workflow_step_fn("wave1b", "a", move |_| participant_live)
                .unwrap();
            for _ in 0..4 {
                bus.subscribe_saga_type_fn("wave1b", |_| true);
            }
            bus.subscribe_saga_type_fn("wave1b", |_| false);
            let c = context(95);
            let result = bus.publish_strict(SagaChoreographyEvent::SagaStarted {
                context: c.clone(),
                payload: Vec::new(),
            });
            if participant_live {
                assert!(result.is_ok());
                assert!(bus.take_terminal_outcome_for_run(&c).is_none());
            } else {
                assert!(matches!(
                    result,
                    Err(SagaBusPublishError::RequiredPathDeliveryShortfall { .. })
                ));
                assert!(matches!(
                    bus.take_terminal_outcome_for_run(&c),
                    Some(SagaTerminalOutcome::Quarantined { .. })
                ));
            }
        }
    }

    #[test]
    fn every_rejected_start_settles_run_and_eligible_legacy_waiters() {
        struct RefuseJournal;
        impl TerminalResolverJournal for RefuseJournal {
            fn append(
                &self,
                _: SagaChoreographyEvent,
            ) -> Result<u64, crate::TerminalResolverJournalError> {
                Err(crate::TerminalResolverJournalError::Storage(
                    "injected".into(),
                ))
            }
            fn read_all(
                &self,
            ) -> Result<Vec<crate::TerminalResolverJournalEntry>, crate::TerminalResolverJournalError>
            {
                Ok(Vec::new())
            }
        }
        for case in 0..6 {
            let bus = SagaChoreographyBus::new();
            if case != 0 {
                if case == 5 {
                    bus.attach_durable_terminal_resolver_for_contract::<Workflow, _>(
                        "qa",
                        Arc::new(RefuseJournal),
                    )
                    .unwrap();
                    bus.activate_terminal_resolver_recovery("wave1b").unwrap();
                } else {
                    bus.attach_terminal_resolver_for_contract::<Workflow>("qa")
                        .unwrap();
                }
            }
            if case != 1 {
                bus.register_workflow_contract_provider::<Workflow>()
                    .unwrap();
            }
            if case != 3 {
                bus.subscribe_workflow_step_fn("wave1b", "a", |_| true)
                    .unwrap();
            }
            let mut c = context(96);
            // This waiter is eligible for this future start, independent of clock granularity.
            c.saga_started_at_millis += 1_000;
            if case == 2 {
                c.step_name = "wrong".into();
            }
            if case == 4 {
                bus.gate("wave1b").unwrap().raise(&c, RunPhase::Completed);
            }
            let (legacy, result, h1) = waiter(&bus, &c, true);
            assert!(result.is_ok());
            let (run, result, h2) = waiter(&bus, &c, false);
            assert!(result.is_ok());
            assert!(
                bus.publish_strict(SagaChoreographyEvent::SagaStarted {
                    context: c,
                    payload: Vec::new()
                })
                .is_err(),
                "case={case}"
            );
            assert!(
                matches!(run.wait_timeout(Duration::from_secs(1)), Ok(Err(_))),
                "run case={case}"
            );
            assert!(
                matches!(legacy.wait_timeout(Duration::from_secs(1)), Ok(Err(_))),
                "legacy case={case}"
            );
            assert!(bus.read_state().legacy_waiter_floors.is_empty());
            for handle in [h1, h2] {
                handle.shutdown();
            }
        }
    }

    #[test]
    fn failed_topic_attachment_never_leaks_actor_or_commits_policy() {
        let bus = SagaChoreographyBus::new();
        let release = bus.release_waiter();
        let journal = Arc::new(crate::InMemoryTerminalResolverJournal::default());
        let mut policy = Workflow::terminal_policy();
        policy.saga_type = "".into();
        assert!(
            bus.attach_durable_terminal_resolver(policy, "qa", journal.clone())
                .is_err()
        );
        assert!(bus.read_state().terminal_policies_by_saga_type.is_empty());
        drop(bus);
        assert!(
            release.wait_timeout(Duration::from_millis(200)),
            "failed attachment leaked an unowned resolver"
        );
        assert_eq!(Arc::strong_count(&journal), 1);
    }

    #[test]
    fn conflicting_attachment_is_rejected_and_detach_removes_policy() {
        let bus = SagaChoreographyBus::new();
        let policy = Workflow::terminal_policy();
        let subscription = bus.attach_terminal_resolver(policy.clone(), "qa").unwrap();
        assert_eq!(
            bus.attach_terminal_resolver(Workflow::terminal_policy(), "qa")
                .unwrap(),
            subscription
        );
        let mut conflicting = policy.clone();
        conflicting.stalled_timeout += Duration::from_secs(1);
        assert!(bus.attach_terminal_resolver(conflicting, "qa").is_err());
        assert!(bus.attach_terminal_resolver(policy, "other").is_err());
        assert!(bus.detach_terminal_resolver("wave1b"));
        assert!(
            !bus.read_state()
                .terminal_policies_by_saga_type
                .contains_key("wave1b")
        );
    }

    #[test]
    fn delivery_receipt_fails_closed_on_checked_counter_overflow() {
        let mut receipt = DeliveryReceipt {
            attempted: u32::MAX,
            delivered: u32::MAX,
            ..Default::default()
        };
        receipt.record("a", true);
        assert!(
            receipt.overflowed,
            "overflow must never masquerade as successful coverage"
        );
    }

    #[test]
    fn quarantine_escalation_replaces_cached_reply_before_notifying() {
        let bus = SagaChoreographyBus::new();
        let c = context(99);
        let ordinary = SagaReplyTo {
            responder: "ordinary".into(),
            outcome: SagaTerminalOutcome::Completed { context: c.clone() },
        };
        bus.complete_terminal_reply_for_run(&c, ordinary);
        bus.fence_delivery(&c, "delivery uncertainty".into());
        assert!(matches!(
            bus.take_terminal_reply_for_run(&c).unwrap().outcome,
            SagaTerminalOutcome::Quarantined { .. }
        ));
    }

    #[test]
    fn resolver_shutdown_and_journal_drop_run_outside_state_lock() {
        struct Journal {
            state: Weak<RwLock<BusState>>,
            dropped: mpsc::Sender<bool>,
        }
        impl Drop for Journal {
            fn drop(&mut self) {
                let unlocked = self
                    .state
                    .upgrade()
                    .is_some_and(|state| state.try_write().is_ok());
                self.dropped.send(unlocked).unwrap();
            }
        }
        impl TerminalResolverJournal for Journal {
            fn append(
                &self,
                _: SagaChoreographyEvent,
            ) -> Result<u64, crate::TerminalResolverJournalError> {
                Ok(1)
            }
            fn read_all(
                &self,
            ) -> Result<Vec<crate::TerminalResolverJournalEntry>, crate::TerminalResolverJournalError>
            {
                Ok(Vec::new())
            }
        }
        let bus = SagaChoreographyBus::new();
        let (tx, rx) = mpsc::channel();
        let journal = Arc::new(Journal {
            state: Arc::downgrade(&bus.inner.state),
            dropped: tx,
        });
        bus.attach_durable_terminal_resolver(
            TerminalPolicy::order_lifecycle_default(),
            "qa",
            journal,
        )
        .unwrap();
        assert!(bus.detach_terminal_resolver("order_lifecycle"));
        assert!(rx.recv_timeout(Duration::from_secs(3)).unwrap());
    }

    #[test]
    fn ephemeral_fifo_evicts_failed_fingerprints_but_pins_active_and_quarantined() {
        let mut index = AdmissionIndex {
            ephemeral: true,
            ..AdmissionIndex::new(AdmissionLimits::with_capacity(3), false)
        };
        let active = context(1);
        let quarantine = context(2);
        let failed = context(3);
        index.raise(
            active.saga_id,
            active.saga_started_at_millis,
            RunPhase::Active,
        );
        index.raise(
            quarantine.saga_id,
            quarantine.saga_started_at_millis,
            RunPhase::Quarantined,
        );
        index.raise(
            failed.saga_id,
            failed.saga_started_at_millis,
            RunPhase::Active,
        );
        let event = SagaChoreographyEvent::StepCompleted {
            context: failed.clone(),
            output: Vec::new(),
            saga_input: Vec::new(),
            compensation_available: true,
        };
        assert!(index.observe(&event));
        index.raise(
            failed.saga_id,
            failed.saga_started_at_millis,
            RunPhase::Failed,
        );
        assert_eq!(index.fingerprints, 1);
        assert!(index.has_capacity_for(&context(4)));
        assert_eq!(index.phase(&active), Some(RunPhase::Active));
        assert_eq!(index.phase(&quarantine), Some(RunPhase::Quarantined));
        assert_eq!(index.phase(&failed), None);
        assert_eq!(index.fingerprints, 0);
        assert!(index.ordinary_runs.is_empty());
    }

    #[test]
    fn waiter_capacity_is_distinct_from_duplicate_and_cancel_releases_slot() {
        for legacy in [false, true] {
            let bus = SagaChoreographyBus::new();
            {
                let mut state = bus.write_state();
                state.pending_replies = CorrelationRegistry::with_ttl(1, DEFAULT_WAITER_TTL);
                state.pending_run_replies = CorrelationRegistry::with_ttl(1, DEFAULT_WAITER_TTL);
            }
            let first = context(1);
            let (pending, result, h1) = waiter(&bus, &first, legacy);
            assert!(result.is_ok());
            let (duplicate, result, h2) = waiter(&bus, &first, legacy);
            assert!(matches!(result, Err(SagaBusPublishError::DuplicateWaiter)));
            assert!(
                duplicate
                    .wait()
                    .unwrap()
                    .unwrap_err()
                    .contains("already registered")
            );
            let (full, result, h3) = waiter(&bus, &context(2), legacy);
            assert!(matches!(
                result,
                Err(SagaBusPublishError::WaiterRegistryFull)
            ));
            assert!(full.wait().unwrap().unwrap_err().contains("full"));
            assert!(if legacy {
                bus.cancel_terminal_reply(first.saga_id)
            } else {
                bus.cancel_terminal_reply_for_run(&first)
            });
            assert!(pending.wait().is_err());
            assert!(bus.read_state().legacy_waiter_floors.is_empty());
            let (_pending, result, h4) = waiter(&bus, &context(2), legacy);
            assert!(result.is_ok());
            for handle in [h1, h2, h3, h4] {
                handle.shutdown();
            }
        }
    }

    #[test]
    fn ttl_lazy_sweep_reclaims_both_registries_and_legacy_metadata() {
        let bus = SagaChoreographyBus::with_waiter_ttl(Duration::from_millis(2));
        let (id, result, h1) = waiter(&bus, &context(11), true);
        assert!(result.is_ok());
        let (run, result, h2) = waiter(&bus, &context(11), false);
        assert!(result.is_ok());
        thread::sleep(Duration::from_millis(5));
        bus.sweep_waiters();
        let state = bus.read_state();
        assert!(state.legacy_waiter_floors.is_empty());
        assert!(state.pending_replies.is_empty());
        assert!(state.pending_run_replies.is_empty());
        drop(state);
        assert!(id.wait().is_err());
        assert!(run.wait().is_err());
        h1.shutdown();
        h2.shutdown();
    }

    #[test]
    fn duplicate_active_start_and_capacity_rejection_settle_exact_waiter() {
        for duplicate in [true, false] {
            let bus = SagaChoreographyBus::new();
            let first = context(21);
            bus.attach_terminal_resolver_inner(
                TerminalPolicy {
                    saga_type: "wave1b".into(),
                    ..TerminalPolicy::order_lifecycle_default()
                },
                "qa",
                None,
                AdmissionLimits::with_capacity(1),
            )
            .unwrap();
            let start = SagaChoreographyEvent::SagaStarted {
                context: first.clone(),
                payload: Vec::new(),
            };
            bus.gate("wave1b")
                .unwrap()
                .admit_start(&start, &first)
                .unwrap_or_else(|_| panic!("first admitted"));
            let target = if duplicate { first } else { context(22) };
            let (pending, result, handle) = waiter(&bus, &target, false);
            assert!(result.is_ok());
            assert!(
                bus.publish_strict(SagaChoreographyEvent::SagaStarted {
                    context: target,
                    payload: Vec::new()
                })
                .is_err()
            );
            assert!(matches!(
                pending.wait_timeout(Duration::from_millis(200)),
                Ok(Err(_))
            ));
            handle.shutdown();
        }
    }

    #[test]
    fn repeated_create_drop_is_synchronous_and_starts_no_background_worker() {
        for _ in 0..500 {
            let bus = SagaChoreographyBus::new();
            assert!(!bus.inner.watchdog_started.load(Ordering::Acquire));
            let released = bus.release_waiter();
            drop(bus);
            assert!(released.wait_timeout(Duration::ZERO));
        }
    }

    #[test]
    fn terminal_cache_is_committed_before_waiter_wakes_from_actor_callback() {
        let bus = SagaChoreographyBus::new();
        let c = context(31);
        let (pending, result, waiter_handle) = waiter(&bus, &c, false);
        assert!(result.is_ok());
        let (tx, rx) = mpsc::channel();
        let reader = bus.clone();
        let read_context = c.clone();
        let join = thread::spawn(move || {
            pending
                .wait_timeout(Duration::from_secs(3))
                .unwrap()
                .unwrap();
            tx.send(reader.take_terminal_outcome_for_run(&read_context))
                .unwrap();
        });
        let (probe, handle) = local_sync::mpsc::spawn(8, move |(): ()| {
            bus.complete_terminal_reply_for_run(
                &c,
                SagaReplyTo {
                    responder: "qa".into(),
                    outcome: SagaTerminalOutcome::Completed { context: c.clone() },
                },
            );
        });
        probe.tell(());
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(3)).unwrap(),
            Some(SagaTerminalOutcome::Completed { .. })
        ));
        join.join().unwrap();
        handle.shutdown();
        waiter_handle.shutdown();
    }
}

#[cfg(test)]
mod streaming_attach_tests {
    use super::*;
    use crate::{
        InMemoryTerminalResolverJournal, TerminalResolverJournalEntry, TerminalResolverJournalError,
    };
    use std::sync::atomic::AtomicUsize;
    use std::time::Instant;

    struct StreamingOnly<J> {
        inner: J,
        visits: AtomicUsize,
    }

    impl<J: TerminalResolverJournal> TerminalResolverJournal for StreamingOnly<J> {
        fn append(
            &self,
            event: SagaChoreographyEvent,
        ) -> Result<u64, TerminalResolverJournalError> {
            self.inner.append(event)
        }
        fn read_all(
            &self,
        ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
            panic!("resolver attach must stream, never read_all")
        }
        fn supports_saga_lookup(&self) -> bool {
            self.inner.supports_saga_lookup()
        }
        fn read_saga_bounded(
            &self,
            id: SagaId,
            max: usize,
        ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
            self.inner.read_saga_bounded(id, max)
        }
        fn visit_unresolved_runs(
            &self,
            visitor: &mut dyn FnMut(
                &TerminalResolverJournalEntry,
            ) -> Result<(), TerminalResolverJournalError>,
        ) -> Result<(), TerminalResolverJournalError> {
            self.inner.visit_unresolved_runs(&mut |entry| {
                self.visits.fetch_add(1, Ordering::SeqCst);
                visitor(entry)
            })
        }
    }

    fn context(id: u64) -> SagaContext {
        let now = SagaContext::now_millis();
        SagaContext {
            saga_id: SagaId::new(id),
            saga_type: "order_lifecycle".into(),
            step_name: "create_order".into(),
            correlation_id: id,
            causation_id: id,
            trace_id: id,
            step_index: 0,
            attempt: 0,
            initiator_peer_id: [0; 32],
            saga_started_at_millis: now,
            event_timestamp_millis: now,
        }
    }

    fn exercise<J: TerminalResolverJournal>(inner: J, ordinary: u64) {
        for id in 1..=ordinary {
            let context = context(id);
            inner
                .append(SagaChoreographyEvent::SagaStarted {
                    context: context.clone(),
                    payload: vec![],
                })
                .unwrap();
            inner
                .append(SagaChoreographyEvent::SagaCompleted { context })
                .unwrap();
        }
        let active = context(ordinary + 1);
        let quarantined = context(ordinary + 2);
        for context in [&active, &quarantined] {
            inner
                .append(SagaChoreographyEvent::SagaStarted {
                    context: context.clone(),
                    payload: vec![],
                })
                .unwrap();
        }
        inner
            .append(SagaChoreographyEvent::SagaQuarantined {
                context: quarantined.clone(),
                reason: "uncertain".into(),
                step: quarantined.step_name.clone(),
                participant_id: "test".into(),
            })
            .unwrap();
        let journal = Arc::new(StreamingOnly {
            inner,
            visits: AtomicUsize::new(0),
        });
        let bus = SagaChoreographyBus::new();
        bus.attach_durable_terminal_resolver(
            TerminalPolicy::order_lifecycle_default(),
            "stream-test",
            journal.clone(),
        )
        .unwrap();
        assert_eq!(journal.visits.load(Ordering::SeqCst), 3);
        let gate = bus.read_state().runtimes["order_lifecycle"].gate.clone();
        assert_eq!(gate.index().resident_ids(), 2);
        assert_eq!(gate.phase(&active), Some(RunPhase::Active));
        assert_eq!(gate.phase(&quarantined), Some(RunPhase::Quarantined));
        bus.activate_terminal_resolver_recovery("order_lifecycle")
            .unwrap();
        let mut successor = quarantined;
        successor.saga_started_at_millis += 1;
        assert!(
            bus.publish_strict(SagaChoreographyEvent::SagaStarted {
                context: successor,
                payload: vec![]
            })
            .is_err()
        );
        bus.publish_strict(SagaChoreographyEvent::StepCompleted {
            context: active.clone(),
            output: vec![],
            saga_input: vec![],
            compensation_available: false,
        })
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(outcome) = bus.take_terminal_outcome(active.saga_id) {
                assert!(matches!(outcome, SagaTerminalOutcome::Completed { .. }));
                break;
            }
            assert!(
                Instant::now() < deadline,
                "restored active run did not complete"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn resolver_attach_never_calls_read_all() {
        exercise(InMemoryTerminalResolverJournal::default(), 0);
    }

    #[test]
    fn memory_resolver_attach_skips_5000_resolved_runs() {
        exercise(InMemoryTerminalResolverJournal::default(), 5_000);
    }

    #[cfg(feature = "lmdb")]
    #[test]
    fn lmdb_resolver_attach_skips_5000_resolved_runs() {
        let dir = tempfile::tempdir().unwrap();
        exercise(
            crate::LmdbTerminalResolverJournal::open(dir.path()).unwrap(),
            5_000,
        );
    }
    #[test]
    fn streaming_storage_error_does_not_fall_back_to_read_all() {
        struct Broken;
        impl TerminalResolverJournal for Broken {
            fn append(
                &self,
                _: SagaChoreographyEvent,
            ) -> Result<u64, TerminalResolverJournalError> {
                unreachable!()
            }
            fn read_all(
                &self,
            ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError>
            {
                panic!("must not mask streaming storage errors")
            }
            fn visit_unresolved_runs(
                &self,
                _: &mut dyn FnMut(
                    &TerminalResolverJournalEntry,
                ) -> Result<(), TerminalResolverJournalError>,
            ) -> Result<(), TerminalResolverJournalError> {
                Err(TerminalResolverJournalError::Storage(
                    "injected streaming failure".into(),
                ))
            }
        }
        let bus = SagaChoreographyBus::new();
        let err = bus
            .attach_durable_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "error-test",
                Arc::new(Broken),
            )
            .unwrap_err();
        assert!(err.contains("injected streaming failure"), "{err}");
        assert!(bus.read_state().runtimes.is_empty());
    }

    #[test]
    fn unsupported_streaming_discards_partial_visit_before_legacy_fallback() {
        struct Legacy;
        impl TerminalResolverJournal for Legacy {
            fn append(
                &self,
                _: SagaChoreographyEvent,
            ) -> Result<u64, TerminalResolverJournalError> {
                unreachable!()
            }
            fn read_all(
                &self,
            ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError>
            {
                Ok(vec![])
            }
            fn visit_unresolved_runs(
                &self,
                visitor: &mut dyn FnMut(
                    &TerminalResolverJournalEntry,
                )
                    -> Result<(), TerminalResolverJournalError>,
            ) -> Result<(), TerminalResolverJournalError> {
                visitor(&TerminalResolverJournalEntry {
                    sequence: 1,
                    event: SagaChoreographyEvent::SagaStarted {
                        context: context(1),
                        payload: vec![],
                    },
                })?;
                Err(TerminalResolverJournalError::Unsupported("legacy".into()))
            }
        }
        let bus = SagaChoreographyBus::new();
        bus.attach_durable_terminal_resolver(
            TerminalPolicy::order_lifecycle_default(),
            "legacy-test",
            Arc::new(Legacy),
        )
        .unwrap();
        assert_eq!(
            bus.read_state().runtimes["order_lifecycle"]
                .gate
                .index()
                .resident_ids(),
            0
        );
    }
}
