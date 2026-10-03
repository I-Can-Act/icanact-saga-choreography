use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use icanact_core::CorrelationRegistry;
use icanact_core::local::{FirehosePubSub, FirehoseSubscription, PublishStats};
use icanact_core::local_sync::{self, SyncActor};

use crate::reply_registry::{SagaReplyToHandle, SagaReplyToResult};
use crate::workflow_contract::required_path_steps_from_success_criteria;
use crate::{
    HasSagaWorkflowParticipants, SagaChoreographyEvent, SagaContext, SagaId, SagaReplyTo,
    SagaTerminalOutcome, SagaWorkflowContract, SagaWorkflowStepContract, TERMINAL_RESOLVER_STEP,
    TerminalPolicy, TerminalResolver, TerminalResolverJournal,
    required_steps_from_success_criteria, validate_workflow_contract,
};

#[derive(Clone, Debug)]
struct WorkflowContractState {
    first_step: Box<str>,
    declared_steps: HashSet<Box<str>>,
    required_path_steps: HashSet<Box<str>>,
    required_path_description: Box<str>,
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
struct BusStateActor {
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
    bound_steps_by_saga_type: HashMap<Box<str>, HashSet<Box<str>>>,
    // Last: notify only after all actor-owned fields have been destroyed.
    _release: Option<ReleaseGuard>,
}

#[derive(Debug)]
enum BusStateAsk {
    RegisterTerminalPolicy {
        saga_type: Box<str>,
        policy_id: Box<str>,
    },
    RegisterWorkflowContract {
        saga_type: Box<str>,
        contract_state: WorkflowContractState,
    },
    RegisterBoundWorkflowStep {
        saga_type: Box<str>,
        step_name: Box<str>,
    },
    HasTerminalPolicy {
        saga_type: Box<str>,
    },
    WorkflowContract {
        saga_type: Box<str>,
    },
    BoundSteps {
        saga_type: Box<str>,
    },
    RequiredPathDescription {
        saga_type: Box<str>,
    },
    RequiredPathExpectedMinDelivery {
        saga_type: Box<str>,
        step_name: Box<str>,
    },
    SagaStartExpectedMinDelivery {
        saga_type: Box<str>,
    },
    NoteAdmittedRun {
        run: RunKey,
    },
    StoreTerminalReply {
        run: RunKey,
        reply: SagaReplyTo,
        retention_limit: usize,
    },
    StoreTerminalOutcome {
        run: RunKey,
        outcome: SagaTerminalOutcome,
        retention_limit: usize,
    },
    TakeTerminalReply {
        run: RunKey,
    },
    TakeTerminalOutcome {
        run: RunKey,
    },
    /// Saga-id compatibility: the newest stored run for the id.
    TakeLatestTerminalReply {
        saga_id: SagaId,
    },
    TakeLatestTerminalOutcome {
        saga_id: SagaId,
    },
}

#[derive(Clone, Debug)]
enum BusStateReply {
    Unit,
    Bool(bool),
    WorkflowContract(Option<WorkflowContractState>),
    BoundSteps(HashSet<Box<str>>),
    String(String),
    OptionalU32(Option<u32>),
    TerminalReply(Option<SagaReplyTo>),
    TerminalOutcome(Option<SagaTerminalOutcome>),
}

impl BusStateActor {
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

impl SyncActor for BusStateActor {
    type Contract = local_sync::contract::AskOnly;
    type Tell = ();
    type Ask = BusStateAsk;
    type Reply = BusStateReply;
    type Channel = ();
    type PubSub = ();
    type Broadcast = ();

    fn handle_ask(&mut self, msg: Self::Ask) -> Self::Reply {
        match msg {
            BusStateAsk::RegisterTerminalPolicy {
                saga_type,
                policy_id,
            } => {
                self.terminal_policies_by_saga_type
                    .insert(saga_type, policy_id);
                BusStateReply::Unit
            }
            BusStateAsk::RegisterWorkflowContract {
                saga_type,
                contract_state,
            } => {
                self.workflow_contracts_by_saga_type
                    .insert(saga_type, contract_state);
                BusStateReply::Unit
            }
            BusStateAsk::RegisterBoundWorkflowStep {
                saga_type,
                step_name,
            } => {
                self.bound_steps_by_saga_type
                    .entry(saga_type)
                    .or_default()
                    .insert(step_name);
                BusStateReply::Unit
            }
            BusStateAsk::HasTerminalPolicy { saga_type } => BusStateReply::Bool(
                self.terminal_policies_by_saga_type
                    .contains_key(saga_type.as_ref()),
            ),
            BusStateAsk::WorkflowContract { saga_type } => BusStateReply::WorkflowContract(
                self.workflow_contracts_by_saga_type
                    .get(saga_type.as_ref())
                    .cloned(),
            ),
            BusStateAsk::BoundSteps { saga_type } => BusStateReply::BoundSteps(
                self.bound_steps_by_saga_type
                    .get(saga_type.as_ref())
                    .cloned()
                    .unwrap_or_default(),
            ),
            BusStateAsk::RequiredPathDescription { saga_type } => {
                let description = self
                    .workflow_contracts_by_saga_type
                    .get(saga_type.as_ref())
                    .map(|contract| contract.required_path_description.to_string())
                    .unwrap_or_default();
                BusStateReply::String(description)
            }
            BusStateAsk::RequiredPathExpectedMinDelivery {
                saga_type,
                step_name,
            } => {
                let expected = self
                    .workflow_contracts_by_saga_type
                    .get(saga_type.as_ref())
                    .and_then(|contract| {
                        if contract.required_path_steps.contains(step_name.as_ref()) {
                            Some(saturating_u32_from_usize(
                                contract.required_path_steps.len().saturating_add(1),
                            ))
                        } else {
                            None
                        }
                    });
                BusStateReply::OptionalU32(expected)
            }
            BusStateAsk::SagaStartExpectedMinDelivery { saga_type } => {
                let expected = self
                    .workflow_contracts_by_saga_type
                    .get(saga_type.as_ref())
                    .map(|contract| {
                        saturating_u32_from_usize(
                            contract.required_path_steps.len().saturating_add(1),
                        )
                    });
                BusStateReply::OptionalU32(expected)
            }
            BusStateAsk::NoteAdmittedRun { run } => {
                self.latest_run_by_id.insert(run.saga_id, run);
                BusStateReply::Unit
            }
            BusStateAsk::StoreTerminalReply {
                run,
                mut reply,
                retention_limit,
            } => {
                match self.insert_terminal_outcome(
                    run.clone(),
                    reply.outcome.clone(),
                    retention_limit,
                ) {
                    Some(outcome) => {
                        reply.outcome = outcome;
                        self.terminal_replies.insert(run, reply.clone());
                        BusStateReply::TerminalReply(Some(reply))
                    }
                    None => BusStateReply::TerminalReply(None),
                }
            }
            BusStateAsk::StoreTerminalOutcome {
                run,
                outcome,
                retention_limit,
            } => {
                self.insert_terminal_outcome(run, outcome, retention_limit);
                BusStateReply::Unit
            }
            BusStateAsk::TakeTerminalReply { run } => {
                BusStateReply::TerminalReply(self.take_reply(&run))
            }
            BusStateAsk::TakeTerminalOutcome { run } => {
                BusStateReply::TerminalOutcome(self.take_outcome(&run))
            }
            BusStateAsk::TakeLatestTerminalReply { saga_id } => {
                let run = self.latest_run_by_id.get(&saga_id).cloned();
                let reply = run.as_ref().and_then(|run| self.take_reply(run));
                BusStateReply::TerminalReply(reply)
            }
            BusStateAsk::TakeLatestTerminalOutcome { saga_id } => {
                let run = self.latest_run_by_id.get(&saga_id).cloned();
                let outcome = run.as_ref().and_then(|run| self.take_outcome(run));
                BusStateReply::TerminalOutcome(outcome)
            }
        }
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
            let _ = index.observe(event);
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
                    completed: HashSet::new(),
                    accepted: HashSet::new(),
                });
                entry.runs.len() - 1
            }
        };
        let run = &mut entry.runs[position];
        run.phase = run.phase.merge(phase);
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
                run.started_at_millis != context.saga_started_at_millis
                    && run.phase != RunPhase::Quarantined
            })
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
            let _ = index.observe(&entry.event);
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
            if !index.observe(&entry.event) {
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

    fn raise(&self, context: &SagaContext, phase: RunPhase) {
        self.index()
            .raise(context.saga_id, context.saga_started_at_millis, phase);
    }

    fn has_successor(&self, context: &SagaContext) -> bool {
        self.index().has_successor(context)
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
                reject_id_waiter: false,
            });
        }
        let Ok(mut index) = self.admission.lock() else {
            return Err(Refusal {
                reason: "terminal resolver admission index unavailable".into(),
                reject_run_waiter: true,
                reject_id_waiter: false,
            });
        };
        if let Err(reason) = self.ensure_resident(&mut index, context.saga_id) {
            return Err(Refusal {
                reason,
                reject_run_waiter: true,
                reject_id_waiter: false,
            });
        }
        if let Some(reason) = index.start_refusal(context) {
            return Err(Refusal {
                reason: reason.into(),
                reject_run_waiter: index.phase(context).is_none(),
                reject_id_waiter: !index.has_open_run(context.saga_id),
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
                reject_id_waiter: false,
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
                reject_id_waiter: false,
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
    shutdown: Arc<AtomicBool>,
    resolver_ref: local_sync::SyncActorRef<TerminalResolverActor>,
    handle: local_sync::ActorHandle,
}

enum TerminalResolverRegistryAsk {
    Existing(Box<str>),
    Register {
        saga_type: Box<str>,
        runtime: TerminalResolverRuntime,
    },
    ActivateRecovery(Box<str>),
    Gate(Box<str>),
    ShutdownAll,
}

enum TerminalResolverRegistryReply {
    Existing(Option<FirehoseSubscription>),
    Registered(FirehoseSubscription),
    RecoveryActivated(bool),
    Gate(Option<Arc<ResolverGate>>),
    ShutdownComplete,
}

#[derive(Default)]
struct TerminalResolverRegistryActor {
    /// Declared after the runtimes so their gates are released first.
    runtimes: HashMap<Box<str>, TerminalResolverRuntime>,
    _release: Option<ReleaseGuard>,
}

impl SyncActor for TerminalResolverRegistryActor {
    type Contract = local_sync::contract::AskOnly;
    type Tell = ();
    type Ask = TerminalResolverRegistryAsk;
    type Reply = TerminalResolverRegistryReply;
    type Channel = ();
    type PubSub = ();
    type Broadcast = ();

    fn handle_ask(&mut self, msg: Self::Ask) -> Self::Reply {
        match msg {
            TerminalResolverRegistryAsk::Existing(saga_type) => {
                TerminalResolverRegistryReply::Existing(
                    self.runtimes
                        .get(saga_type.as_ref())
                        .map(|runtime| runtime.subscription.clone()),
                )
            }
            TerminalResolverRegistryAsk::Register { saga_type, runtime } => {
                match self.runtimes.entry(saga_type) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let subscription = runtime.subscription.clone();
                        entry.insert(runtime);
                        TerminalResolverRegistryReply::Registered(subscription)
                    }
                    std::collections::hash_map::Entry::Occupied(entry) => {
                        runtime.shutdown.store(true, Ordering::Release);
                        runtime.handle.shutdown();
                        TerminalResolverRegistryReply::Registered(entry.get().subscription.clone())
                    }
                }
            }
            TerminalResolverRegistryAsk::ActivateRecovery(saga_type) => {
                let activated = self
                    .runtimes
                    .get(saga_type.as_ref())
                    .is_some_and(|runtime| {
                        let queued = runtime
                            .resolver_ref
                            .tell(TerminalResolverTell::ActivateRecovery);
                        if queued {
                            // Opened after the tell is queued so any start admitted
                            // afterwards is ordered behind the recovery output.
                            runtime.gate.activated.store(true, Ordering::Release);
                        }
                        queued
                    });
                TerminalResolverRegistryReply::RecoveryActivated(activated)
            }
            TerminalResolverRegistryAsk::Gate(saga_type) => TerminalResolverRegistryReply::Gate(
                self.runtimes
                    .get(saga_type.as_ref())
                    .map(|runtime| Arc::clone(&runtime.gate)),
            ),
            TerminalResolverRegistryAsk::ShutdownAll => {
                for (_, runtime) in self.runtimes.drain() {
                    runtime.shutdown.store(true, Ordering::Release);
                    runtime.handle.shutdown();
                }
                TerminalResolverRegistryReply::ShutdownComplete
            }
        }
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
    activated: bool,
    gate: Arc<ResolverGate>,
    bus: SagaChoreographyBus,
    responder: Arc<str>,
    saga_type: Box<str>,
    // Last, including the internal bus's subscriber resources, not just its gate.
    _release: ReleaseGuard,
}

impl TerminalResolverActor {
    fn publish_terminal_events(&mut self, terminal_events: Vec<SagaChoreographyEvent>) {
        if !self.activated {
            self.held_events.extend(terminal_events);
            return;
        }
        for terminal_event in terminal_events {
            let _ = self
                .bus
                .complete_terminal_reply_from_event(&terminal_event, self.responder.as_ref());
            if let Err(err) = self.bus.publish_strict(terminal_event) {
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
    /// Journals evidence for a fenced run without involving the resolver.
    fn retain_evidence(&mut self, event: &SagaChoreographyEvent) {
        // A quarantine may have been latched while the journal was unwritable.
        // Once storage returns, its fence must precede new retained evidence;
        // otherwise a restart could reconstruct an ordinary success from that
        // evidence and forget the already-published quarantine.
        let quarantined = self.gate.index().phase(event.context()) == Some(RunPhase::Quarantined);
        if quarantined
            && !matches!(event, SagaChoreographyEvent::SagaQuarantined { .. })
            && let Some(journal) = &self.gate.journal
        {
            let context = event.context();
            let fence = SagaChoreographyEvent::SagaQuarantined {
                context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
                reason: "terminal resolver retained quarantined ownership; reconciliation required"
                    .into(),
                step: context.step_name.clone(),
                participant_id: self.responder.as_ref().into(),
            };
            if let Err(error) = journal.append(fence) {
                tracing::error!(target: "core::saga", event = "terminal_resolver_quarantine_fence_append_failed",
                    saga_type = self.saga_type.as_ref(), saga_id = %context.saga_id, error = ?error);
                return;
            }
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
    }

    /// A compensable effect materialised after an ordinary terminal. The
    /// evidence is retained and the run escalates to quarantine. The index is
    /// raised first so a second queued late effect cannot escalate twice.
    fn escalate_late_effect(&mut self, event: &SagaChoreographyEvent) {
        let context = event.context();
        self.retain_evidence(event);
        self.gate.raise(context, RunPhase::Quarantined);
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
                {
                    let fence = self.gate.fence(&event);
                    match fence {
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
                            // Older-run uncertainty also fences an active successor.
                            // Do not bypass the resolver's cross-run quarantine rule.
                            let fence_successor =
                                matches!(*event, SagaChoreographyEvent::SagaQuarantined { .. })
                                    && self.gate.has_successor(event.context());
                            self.retain_evidence(&event);
                            if fence_successor {
                                let terminal_events = self.resolver.ingest(&event);
                                self.publish_terminal_events(terminal_events);
                            }
                            return;
                        }
                        Fence::Escalate => {
                            self.escalate_late_effect(&event);
                            return;
                        }
                    }
                }
                let already_journaled = matches!(*event, SagaChoreographyEvent::SagaStarted { .. })
                    && self.gate.journal.is_some()
                    && self.gate.take_prejournaled(event.context());
                if !already_journaled
                    && let Some(journal) = &self.gate.journal
                    && let Err(error) = journal.append((*event).clone())
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
                        matches!(*event, SagaChoreographyEvent::SagaQuarantined { .. });
                    let quarantine = if incoming_quarantine {
                        (*event).clone()
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
                    self.gate.raise(quarantine.context(), RunPhase::Quarantined);
                    let mut outputs = self.resolver.ingest(&quarantine);
                    for output in &outputs {
                        if matches!(output, SagaChoreographyEvent::SagaQuarantined { .. }) {
                            self.gate.raise(output.context(), RunPhase::Quarantined);
                        }
                    }
                    if !incoming_quarantine {
                        outputs.insert(0, quarantine);
                    }
                    self.publish_terminal_events(outputs);
                    return;
                }
                if !already_journaled && !self.gate.observe(&event) {
                    let context = event.context();
                    let quarantine = SagaChoreographyEvent::SagaQuarantined {
                        context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
                        reason: "terminal resolver admission evidence capacity unavailable; reconciliation required".into(),
                        step: context.step_name.clone(),
                        participant_id: "terminal_resolver".into(),
                    };
                    // Latch the resolver itself and retain the strong fence before
                    // replying. A subsequent watchdog must not emit ordinary failure.
                    let _ = self.resolver.ingest(&quarantine);
                    self.retain_evidence(&quarantine);
                    self.publish_terminal_events(vec![quarantine]);
                    return;
                }
                self.resolver.ingest(&event)
            }
            TerminalResolverTell::ActivateRecovery => {
                self.activated = true;
                let mut pending = std::mem::take(&mut self.recovery_events);
                pending.append(&mut self.held_events);
                self.publish_terminal_events(pending);
                return;
            }
            TerminalResolverTell::PollTimeouts => self.resolver.poll_timeouts(),
        };
        self.publish_terminal_events(terminal_events);
    }
}

pub struct SagaChoreographyBus {
    bus: FirehosePubSub<SagaChoreographyEvent>,
    /// Saga-id compatibility waiters; see `legacy_waiter_floors`.
    pending_replies: CorrelationRegistry<SagaId, SagaReplyToResult>,
    /// Run-scoped waiters keyed by saga type, id and start time.
    pending_run_replies: CorrelationRegistry<RunKey, SagaReplyToResult>,
    /// Binding of id-scoped waiters to a run, so a terminal of any other run
    /// cannot satisfy them.
    legacy_waiter_floors: Arc<Mutex<HashMap<SagaId, LegacyWaiter>>>,
    state_ref: local_sync::SyncActorRef<BusStateActor>,
    terminal_resolver_registry_ref: local_sync::SyncActorRef<TerminalResolverRegistryActor>,
    /// Completion coordination only (see [`ReleaseLatch`]); no resource ownership.
    release: Arc<ReleaseLatch>,
    /// Gates of attached resolvers, readable without an actor ask. Weak: the
    /// registry owns each gate, and the resolver's own bus clone shares this map,
    /// so a strong reference would keep the journal alive after shutdown.
    gates: Arc<Mutex<HashMap<Box<str>, std::sync::Weak<ResolverGate>>>>,
    // Public clones own shutdown; internal resolver clones must not create a
    // lifecycle -> registry -> resolver -> lifecycle ownership cycle.
    _lifecycle: Option<Arc<BusActorLifecycle>>,
}

struct BusActorLifecycle {
    pending_replies: CorrelationRegistry<SagaId, SagaReplyToResult>,
    pending_run_replies: CorrelationRegistry<RunKey, SagaReplyToResult>,
    state_handle: Option<local_sync::ActorHandle>,
    terminal_resolver_registry_ref: local_sync::SyncActorRef<TerminalResolverRegistryActor>,
    terminal_resolver_registry_handle: Option<local_sync::ActorHandle>,
    /// Alive exactly while a public owner exists; last after its owned fields.
    _release: ReleaseGuard,
}

impl Drop for BusActorLifecycle {
    fn drop(&mut self) {
        let _ = self
            .terminal_resolver_registry_ref
            .ask(TerminalResolverRegistryAsk::ShutdownAll);
        if let Some(handle) = self.terminal_resolver_registry_handle.take() {
            handle.shutdown();
        }
        if let Some(handle) = self.state_handle.take() {
            handle.shutdown();
        }

        for (_, reply) in self.pending_replies.drain() {
            let _ = reply.reply(Err("saga bus dropped".to_string()));
        }
        for (_, reply) in self.pending_run_replies.drain() {
            let _ = reply.reply(Err("saga bus dropped".to_string()));
        }
    }
}

const DEFAULT_TERMINAL_RETENTION_LIMIT: usize = 1024;
const DEFAULT_TERMINAL_WATCHDOG_TICK_MS: u64 = 100;

pub(crate) fn ensure_saga_sync_pool_capacity() {
    static CONFIGURED: OnceLock<()> = OnceLock::new();
    CONFIGURED.get_or_init(|| {
        let size = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(1)
            .max(256);
        let _ = local_sync::set_default_pool_config(local_sync::PoolConfig::new(size));
    });
}

fn saturating_u32_from_usize(value: usize) -> u32 {
    if value > u32::MAX as usize {
        return u32::MAX;
    }
    value as u32
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
        ensure_saga_sync_pool_capacity();
        let release = Arc::new(ReleaseLatch::default());
        let (state_ref, state_handle) = local_sync::spawn(BusStateActor {
            _release: Some(release.guard()),
            ..BusStateActor::default()
        });
        let (terminal_resolver_registry_ref, terminal_resolver_registry_handle) =
            local_sync::spawn(TerminalResolverRegistryActor {
                _release: Some(release.guard()),
                ..TerminalResolverRegistryActor::default()
            });
        let pending_replies = CorrelationRegistry::new();
        let pending_run_replies = CorrelationRegistry::new();
        let lifecycle = Arc::new(BusActorLifecycle {
            _release: release.guard(),
            pending_replies: pending_replies.clone(),
            pending_run_replies: pending_run_replies.clone(),
            state_handle: Some(state_handle),
            terminal_resolver_registry_ref: terminal_resolver_registry_ref.clone(),
            terminal_resolver_registry_handle: Some(terminal_resolver_registry_handle),
        });
        Self {
            bus: FirehosePubSub::new(),
            pending_replies,
            pending_run_replies,
            legacy_waiter_floors: Arc::default(),
            state_ref,
            terminal_resolver_registry_ref,
            release,
            gates: Arc::default(),
            _lifecycle: Some(lifecycle),
        }
    }

    fn ask_state(&self, msg: BusStateAsk) -> Option<BusStateReply> {
        match self.state_ref.ask(msg) {
            Ok(reply) => Some(reply),
            Err(err) => {
                tracing::error!(
                    target: "core::saga",
                    event = "saga_bus_state_actor_unavailable",
                    error = ?err
                );
                None
            }
        }
    }

    /// Returns a waiter that observes actual release of this bus's actors and
    /// resources after its last public owner is dropped. See
    /// [`SagaBusReleaseWaiter::wait_timeout`]. The global bus never releases.
    pub fn release_waiter(&self) -> SagaBusReleaseWaiter {
        SagaBusReleaseWaiter {
            latch: Arc::clone(&self.release),
        }
    }

    pub fn subscribe_fn<F>(&self, topic: &str, f: F) -> FirehoseSubscription
    where
        F: Fn(&SagaChoreographyEvent) -> bool + Send + Sync + 'static,
    {
        self.bus
            .subscribe_checked_fn(topic, f)
            .unwrap_or_else(|err| panic!("invalid saga topic '{topic}': {err}"))
    }

    pub fn unsubscribe(&self, sub: FirehoseSubscription) -> bool {
        self.bus.unsubscribe(sub)
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
    ) -> (PublishStats, Option<Box<str>>) {
        let event_type = event.event_type();
        let mut expected_min_delivery: Option<u32> = None;
        let mut expected_required_path: Box<str> = "".into();
        let mut expected_context: Option<crate::SagaContext> = None;
        if let SagaChoreographyEvent::SagaStarted { context, .. } = &event {
            // Durable ownership precedes contract diagnostics: never overwrite
            // retained quarantine or active history with a fabricated failure.
            // Serialized: the intent is journaled and the run reserved before
            // any fanout, so concurrent or back-to-back starts cannot both win.
            if let Err(refusal) = self.admit_start(&event, context) {
                tracing::error!(target: "core::saga", event = "saga_start_admission_rejected",
                    saga_type = context.saga_type.as_ref(), saga_id = context.saga_id.get(), reason = %refusal.reason);
                // A refused start never fails the waiter of the run that owns the id.
                if refusal.reject_run_waiter {
                    let _ = self.reject_terminal_reply_for_run(context, refusal.reason.to_string());
                }
                if refusal.reject_id_waiter {
                    let _ = self.resolve_id_waiter(
                        context.saga_id,
                        Some(context),
                        Err(refusal.reason.to_string()),
                    );
                }
                return (PublishStats::default(), Some(refusal.reason));
            }
            self.bind_id_waiter(context);
            let _ = self.ask_state(BusStateAsk::NoteAdmittedRun {
                run: RunKey::of(context),
            });
            if !self.has_terminal_policy_for_saga_type(context.saga_type.as_ref()) {
                let reason: Box<str> = format!(
                    "terminal policy is required before saga start; saga_type={} saga_id={}",
                    context.saga_type,
                    context.saga_id.get()
                )
                .into();
                let terminal = SagaChoreographyEvent::SagaFailed {
                    context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
                    reason: reason.clone(),
                    failure: None,
                };
                if let Some(outcome) = terminal.terminal_outcome() {
                    self.store_terminal_outcome(terminal.context(), outcome);
                }
                return (self.publish_event(terminal), Some(reason));
            }
            if let Some(reason) = self.saga_start_contract_violation_reason(context) {
                let reason: Box<str> = reason.into();
                let terminal = SagaChoreographyEvent::SagaFailed {
                    context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
                    reason: reason.clone(),
                    failure: None,
                };
                if let Some(outcome) = terminal.terminal_outcome() {
                    self.store_terminal_outcome(terminal.context(), outcome);
                }
                return (self.publish_event(terminal), Some(reason));
            }
            expected_min_delivery =
                self.saga_start_expected_min_delivery(context.saga_type.as_ref());
            expected_required_path = self
                .required_path_description(context.saga_type.as_ref())
                .into();
            expected_context = Some(context.clone());
        } else if let Some(required_min_delivery) =
            self.required_path_expected_min_delivery_for_event(&event)
        {
            expected_min_delivery = Some(required_min_delivery);
            expected_required_path = self
                .required_path_description(event.context().saga_type.as_ref())
                .into();
            expected_context = Some(event.context().clone());
        }
        let is_terminal_event = event.terminal_outcome().is_some();
        if let Some(outcome) = event.terminal_outcome() {
            self.store_terminal_outcome(event.context(), outcome);
        }
        let stats = self.publish_event(event);
        if let (Some(required_min_delivery), Some(context)) =
            (expected_min_delivery, expected_context)
            && stats.delivered < required_min_delivery
            && !is_terminal_event
        {
            let reason = format!(
                "required_path_delivery_shortfall: saga_type={} event_type={} step={} delivered={} attempted={} required_min_delivered={} required_path={}",
                context.saga_type,
                event_type,
                context.step_name,
                stats.delivered,
                stats.attempted,
                required_min_delivery,
                expected_required_path
            );
            // Start fanout may already have reached an effect owner too.
            let _ = self.publish_delivery_quarantine(&context, reason);
        }
        (stats, None)
    }

    pub fn publish_strict(
        &self,
        event: SagaChoreographyEvent,
    ) -> Result<PublishStats, SagaBusPublishError> {
        let (stats, rejected) = self.publish_with_admission(event.clone());
        if let Some(reason) = rejected {
            // The diagnostic SagaFailed was already published; the start itself
            // was not admitted, so the caller must not see success.
            let context = event.context();
            return Err(SagaBusPublishError::AdmissionRejected {
                saga_id: context.saga_id,
                saga_type: context.saga_type.clone(),
                reason,
            });
        }
        if let Some(required_min_delivery) =
            self.required_path_expected_min_delivery_for_event(&event)
            && stats.delivered < required_min_delivery
        {
            let context = event.context();
            return Err(SagaBusPublishError::RequiredPathDeliveryShortfall {
                saga_id: context.saga_id,
                saga_type: context.saga_type.clone(),
                step_name: context.step_name.clone(),
                event_type: event.event_type(),
                attempted: stats.attempted,
                delivered: stats.delivered,
                required_min_delivered: required_min_delivery,
                required_path: self
                    .required_path_description(context.saga_type.as_ref())
                    .into(),
            });
        }
        if stats.attempted == stats.delivered {
            return Ok(stats);
        }

        let context = event.context().clone();
        let attempted = stats.attempted;
        let delivered = stats.delivered;
        let partial = SagaBusPublishError::PartialDelivery {
            saga_id: context.saga_id,
            saga_type: context.saga_type.clone(),
            step_name: context.step_name.clone(),
            attempted,
            delivered,
        };

        let is_terminal = matches!(
            event,
            SagaChoreographyEvent::SagaCompleted { .. }
                | SagaChoreographyEvent::SagaFailed { .. }
                | SagaChoreographyEvent::SagaQuarantined { .. }
        );
        if is_terminal {
            return Err(partial);
        }

        // Effects or undo obligations may already exist, so an ordinary
        // SagaFailed would bypass rollback. Preserve evidence by quarantining;
        // the resolver journals it for operator reconciliation.
        let terminal_stats = self.publish_delivery_quarantine(
            &context,
            format!(
                "publish_partial_delivery attempted={} delivered={} event_type={} step={}",
                attempted,
                delivered,
                event.event_type(),
                context.step_name
            ),
        );
        if terminal_stats.attempted != terminal_stats.delivered {
            return Err(SagaBusPublishError::TerminalEscalationPartialDelivery {
                saga_id: context.saga_id,
                saga_type: context.saga_type,
                attempted: terminal_stats.attempted,
                delivered: terminal_stats.delivered,
            });
        }

        Err(partial)
    }

    fn register_terminal_policy(&self, policy: &TerminalPolicy) {
        let _ = self.ask_state(BusStateAsk::RegisterTerminalPolicy {
            saga_type: policy.saga_type.clone(),
            policy_id: policy.policy_id.clone(),
        });
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
        };

        let bound_for_type = match self.ask_state(BusStateAsk::BoundSteps {
            saga_type: C::saga_type().into(),
        }) {
            Some(BusStateReply::BoundSteps(steps)) => steps,
            _ => HashSet::new(),
        };
        if !bound_for_type.is_empty() {
            let mut unknown_steps: Vec<&str> = bound_for_type
                .iter()
                .filter(|step| !declared_steps.contains(step.as_ref()))
                .map(|step| step.as_ref())
                .collect();
            unknown_steps.sort_unstable();
            if !unknown_steps.is_empty() {
                return Err(format!(
                    "bound step set contains steps not declared by workflow contract: saga_type={} unknown_steps={}",
                    C::saga_type(),
                    unknown_steps.join(",")
                ));
            }
        }

        let _ = self.ask_state(BusStateAsk::RegisterWorkflowContract {
            saga_type: C::saga_type().into(),
            contract_state,
        });

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

    pub fn register_bound_workflow_step(
        &self,
        saga_type: &'static str,
        step_name: &'static str,
    ) -> Result<(), String> {
        if saga_type.is_empty() || step_name.is_empty() {
            return Err("saga_type and step_name must be non-empty".to_string());
        }
        let contract = match self.ask_state(BusStateAsk::WorkflowContract {
            saga_type: saga_type.into(),
        }) {
            Some(BusStateReply::WorkflowContract(contract)) => contract,
            _ => None,
        };
        if let Some(contract) = contract
            && !contract.declared_steps.contains(step_name)
        {
            return Err(format!(
                "bound workflow step is not declared by contract: saga_type={} step={}",
                saga_type, step_name
            ));
        }

        let _ = self.ask_state(BusStateAsk::RegisterBoundWorkflowStep {
            saga_type: saga_type.into(),
            step_name: step_name.into(),
        });
        Ok(())
    }

    pub fn register_bound_workflow_participants_for_actor<A: HasSagaWorkflowParticipants>(
        &self,
    ) -> Result<(), String> {
        for workflow in A::saga_workflows() {
            let step_name = workflow.step_name();
            for saga_type in workflow.saga_types() {
                self.register_bound_workflow_step(saga_type, step_name)?;
            }
        }
        Ok(())
    }

    pub fn publish_to_saga_type(
        &self,
        saga_type: &str,
        event: SagaChoreographyEvent,
    ) -> PublishStats {
        self.bus
            .publish_checked(saga_type, &event)
            .unwrap_or_else(|err| panic!("invalid saga topic '{saga_type}': {err}"))
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
    ) -> Result<(), Box<str>> {
        match self.pending_replies.register(saga_id, reply) {
            Ok(()) => {
                if let Ok(mut floors) = self.legacy_waiter_floors.lock() {
                    floors.insert(
                        saga_id,
                        LegacyWaiter {
                            registered_at_millis: SagaContext::now_millis(),
                            bound_run: None,
                        },
                    );
                }
                // A run admitted before this registration cannot bind the waiter
                // at admission; bind it to the unique active run of the id now.
                // Several active owners (other saga types) or none stay unbound.
                if let Some(run) = self.unique_active_run(saga_id) {
                    self.bind_id_waiter_to(saga_id, run);
                }
                Ok(())
            }
            Err(err) => {
                let reply = err.into_reply();
                let _ = reply.reply(Err("terminal reply already registered for saga id".into()));
                Err("terminal reply already registered for saga id".into())
            }
        }
    }

    /// Registers a terminal waiter for exactly the run identified by `context`
    /// (saga type, id and start time).
    pub fn register_terminal_reply_for_run(
        &self,
        context: &SagaContext,
        reply: SagaReplyToHandle,
    ) -> Result<(), Box<str>> {
        match self
            .pending_run_replies
            .register(RunKey::of(context), reply)
        {
            Ok(()) => Ok(()),
            Err(err) => {
                let reply = err.into_reply();
                let _ = reply.reply(Err("terminal reply already registered for saga run".into()));
                Err("terminal reply already registered for saga run".into())
            }
        }
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
        self.pending_run_replies
            .resolve(&RunKey::of(context), Err(reason.into()))
            .is_ok()
    }

    /// Binds a not-yet-bound id-scoped waiter to the run just admitted.
    fn bind_id_waiter(&self, context: &SagaContext) {
        self.bind_id_waiter_to(context.saga_id, RunKey::of(context));
    }

    fn bind_id_waiter_to(&self, saga_id: SagaId, run: RunKey) {
        if let Ok(mut waiters) = self.legacy_waiter_floors.lock()
            && let Some(waiter) = waiters.get_mut(&saga_id)
            && waiter.bound_run.is_none()
        {
            waiter.bound_run = Some(run);
        }
    }

    /// The single open run (type, id, start) of `saga_id` across every attached
    /// resolver, or `None` when there is none or ownership is ambiguous.
    /// Reads the gates directly, never asking an actor, so it is safe to call
    /// from inside a sync actor.
    fn unique_active_run(&self, saga_id: SagaId) -> Option<RunKey> {
        let gates = self.gates.lock().ok()?;
        let mut found: Option<RunKey> = None;
        for (saga_type, gate) in gates.iter() {
            let Some(gate) = gate.upgrade() else {
                continue;
            };
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
        if let Ok(mut floors) = self.legacy_waiter_floors.lock() {
            match (floors.get(&saga_id), context) {
                (Some(waiter), Some(context)) if !waiter.accepts(context) => return false,
                _ => {
                    floors.remove(&saga_id);
                }
            }
        }
        self.pending_replies.resolve(&saga_id, result).is_ok()
    }

    fn resolve_run_waiters(&self, context: &SagaContext, result: SagaReplyToResult) -> bool {
        let run = self
            .pending_run_replies
            .resolve(&RunKey::of(context), result.clone())
            .is_ok();
        let id = self.resolve_id_waiter(context.saga_id, Some(context), result);
        run || id
    }

    pub fn attach_terminal_resolver(
        &self,
        policy: TerminalPolicy,
        responder: &'static str,
    ) -> Result<FirehoseSubscription, String> {
        self.attach_terminal_resolver_inner(policy, responder, None, AdmissionLimits::from_env())
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
        let existing =
            match self
                .terminal_resolver_registry_ref
                .ask(TerminalResolverRegistryAsk::Existing(
                    saga_type_topic.clone(),
                )) {
                Ok(TerminalResolverRegistryReply::Existing(existing)) => existing,
                Ok(TerminalResolverRegistryReply::Registered(_)) => {
                    return Err(format!(
                        "terminal resolver registry returned register reply saga_type={}",
                        saga_type_topic
                    ));
                }
                Ok(TerminalResolverRegistryReply::ShutdownComplete) => {
                    return Err(format!(
                        "terminal resolver registry returned shutdown reply saga_type={}",
                        saga_type_topic
                    ));
                }
                Ok(TerminalResolverRegistryReply::RecoveryActivated(_))
                | Ok(TerminalResolverRegistryReply::Gate(_)) => {
                    return Err(format!(
                        "terminal resolver registry returned activation reply saga_type={}",
                        saga_type_topic
                    ));
                }
                Err(err) => {
                    return Err(format!(
                        "terminal resolver registry unavailable saga_type={}: {:?}",
                        saga_type_topic, err
                    ));
                }
            };
        if let Some(subscription) = existing {
            return Ok(subscription);
        }

        self.register_terminal_policy(&policy);
        let mut bus = self.clone();
        bus._lifecycle = None;
        let responder: Arc<str> = Arc::from(responder);
        let shutdown = Arc::new(AtomicBool::new(false));
        let (resolver, recovery_events, admission) = match &journal {
            Some(journal) => {
                let entries = journal.read_all().map_err(|error| {
                    format!(
                        "terminal resolver recovery read failed saga_type={}: {error}",
                        policy.saga_type
                    )
                })?;
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
                // The only full history read. Later starts and cache misses use
                // bounded per-saga lookups when the journal supports them.
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
                (resolver, recovery_events, admission)
            }
            None => (
                TerminalResolver::new(policy.clone()),
                Vec::new(),
                AdmissionIndex::new(limits, false),
            ),
        };
        let durable = journal.is_some();
        let gate = Arc::new(ResolverGate {
            journal,
            _release: self.release.guard(),
            activated: AtomicBool::new(!durable),
            admission: Mutex::new(admission),
        });
        let (resolver_ref, resolver_handle) = local_sync::spawn(TerminalResolverActor {
            resolver,
            _release: self.release.guard(),
            recovery_events,
            held_events: Vec::new(),
            activated: !durable,
            gate: Arc::clone(&gate),
            bus: bus.clone(),
            responder: Arc::clone(&responder),
            saga_type: saga_type_topic.clone(),
        });
        spawn_terminal_watchdog_if_needed(&policy, resolver_ref.clone(), Arc::clone(&shutdown))?;
        let subscription_saga_type = policy.saga_type.clone();
        let subscription_resolver_ref = resolver_ref.clone();
        let subscription = self.subscribe_fn(saga_type_topic.as_ref(), move |event| {
            if !subscription_resolver_ref
                .tell(TerminalResolverTell::Ingest(Box::new(event.clone())))
            {
                tracing::error!(
                    target: "core::saga",
                    event = "terminal_resolver_ingest_failed",
                    saga_type = subscription_saga_type.as_ref()
                );
            }
            true
        });
        let registered =
            match self
                .terminal_resolver_registry_ref
                .ask(TerminalResolverRegistryAsk::Register {
                    saga_type: saga_type_topic,
                    runtime: TerminalResolverRuntime {
                        gate,
                        subscription,
                        shutdown,
                        resolver_ref,
                        handle: resolver_handle,
                    },
                }) {
                Ok(TerminalResolverRegistryReply::Registered(subscription)) => subscription,
                Ok(TerminalResolverRegistryReply::Existing(_)) => {
                    return Err(
                        "terminal resolver registry returned unexpected existing reply".to_string(),
                    );
                }
                Ok(TerminalResolverRegistryReply::ShutdownComplete) => {
                    return Err("terminal resolver registry returned shutdown reply".to_string());
                }
                Ok(TerminalResolverRegistryReply::RecoveryActivated(_))
                | Ok(TerminalResolverRegistryReply::Gate(_)) => {
                    return Err(
                        "terminal resolver registry returned unexpected activation reply"
                            .to_string(),
                    );
                }
                Err(err) => {
                    return Err(format!("terminal resolver registry unavailable: {err:?}"));
                }
            };
        // Mirror the winning gate so id-scoped lookups need no actor ask.
        if let Ok(TerminalResolverRegistryReply::Gate(Some(gate))) = self
            .terminal_resolver_registry_ref
            .ask(TerminalResolverRegistryAsk::Gate(policy.saga_type.clone()))
            && let Ok(mut gates) = self.gates.lock()
        {
            gates.insert(policy.saga_type.clone(), Arc::downgrade(&gate));
        }
        Ok(registered)
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
        match self.terminal_resolver_registry_ref.ask(
            TerminalResolverRegistryAsk::ActivateRecovery(saga_type.into()),
        ) {
            Ok(TerminalResolverRegistryReply::RecoveryActivated(true)) => Ok(()),
            Ok(TerminalResolverRegistryReply::RecoveryActivated(false)) => Err(format!(
                "terminal resolver recovery activation failed saga_type={saga_type}"
            )),
            Ok(_) => Err(format!(
                "terminal resolver registry returned unexpected activation reply saga_type={saga_type}"
            )),
            Err(error) => Err(format!(
                "terminal resolver registry unavailable during recovery activation saga_type={saga_type}: {error:?}"
            )),
        }
    }

    pub fn activate_terminal_resolver_recovery_for_contract<C: SagaWorkflowContract>(
        &self,
    ) -> Result<(), String> {
        self.activate_terminal_resolver_recovery(C::saga_type())
    }

    /// Newest stored terminal reply for the id (compatibility API).
    pub fn take_terminal_reply(&self, saga_id: SagaId) -> Option<SagaReplyTo> {
        match self.ask_state(BusStateAsk::TakeLatestTerminalReply { saga_id }) {
            Some(BusStateReply::TerminalReply(reply)) => reply,
            _ => None,
        }
    }

    /// Newest stored terminal outcome for the id (compatibility API).
    pub fn take_terminal_outcome(&self, saga_id: SagaId) -> Option<crate::SagaTerminalOutcome> {
        match self.ask_state(BusStateAsk::TakeLatestTerminalOutcome { saga_id }) {
            Some(BusStateReply::TerminalOutcome(outcome)) => outcome,
            _ => None,
        }
    }

    pub fn take_terminal_reply_for_run(&self, context: &SagaContext) -> Option<SagaReplyTo> {
        match self.ask_state(BusStateAsk::TakeTerminalReply {
            run: RunKey::of(context),
        }) {
            Some(BusStateReply::TerminalReply(reply)) => reply,
            _ => None,
        }
    }

    pub fn take_terminal_outcome_for_run(
        &self,
        context: &SagaContext,
    ) -> Option<crate::SagaTerminalOutcome> {
        match self.ask_state(BusStateAsk::TakeTerminalOutcome {
            run: RunKey::of(context),
        }) {
            Some(BusStateReply::TerminalOutcome(outcome)) => outcome,
            _ => None,
        }
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
        let Some(outcome) = event.terminal_outcome() else {
            return false;
        };
        let reply = SagaReplyTo {
            responder: responder.into(),
            outcome,
        };
        // Core forbids waiting on an ask inside a sync actor. The cache request
        // is best-effort/enqueued; authoritative resolver notification must not
        // depend on receiving its synchronous return in this trusted path.
        let _ = self.store_terminal_reply(RunKey::of(event.context()), reply.clone());
        self.resolve_run_waiters(event.context(), Ok(reply))
    }

    /// Publishes evidence-preserving quarantine for a delivery failure.
    fn publish_delivery_quarantine(&self, context: &SagaContext, reason: String) -> PublishStats {
        let quarantine = SagaChoreographyEvent::SagaQuarantined {
            context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
            reason: reason.into(),
            step: context.step_name.clone(),
            participant_id: TERMINAL_RESOLVER_STEP.into(),
        };
        if let Some(outcome) = quarantine.terminal_outcome() {
            self.store_terminal_outcome(quarantine.context(), outcome);
        }
        self.publish_event(quarantine)
    }

    /// Fail-closed start admission against a durable resolver: recovery must
    /// be activated and retained history must not already fence this run.
    /// Uses the in-memory index built at attach; never rereads the journal.
    fn admit_start(
        &self,
        event: &SagaChoreographyEvent,
        context: &SagaContext,
    ) -> Result<(), Refusal> {
        let gate = match self
            .terminal_resolver_registry_ref
            .ask(TerminalResolverRegistryAsk::Gate(context.saga_type.clone()))
        {
            Ok(TerminalResolverRegistryReply::Gate(Some(gate))) => gate,
            Ok(TerminalResolverRegistryReply::Gate(None)) => return Ok(()),
            _ => {
                return Err(Refusal {
                    reason: "terminal resolver registry unavailable during start admission".into(),
                    reject_run_waiter: true,
                    reject_id_waiter: false,
                });
            }
        };
        gate.admit_start(event, context)
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

    fn has_terminal_policy_for_saga_type(&self, saga_type: &str) -> bool {
        match self.ask_state(BusStateAsk::HasTerminalPolicy {
            saga_type: saga_type.into(),
        }) {
            Some(BusStateReply::Bool(has_policy)) => has_policy,
            _ => false,
        }
    }

    fn saga_start_contract_violation_reason(&self, context: &crate::SagaContext) -> Option<String> {
        let saga_type = context.saga_type.as_ref();
        let contract = match self.ask_state(BusStateAsk::WorkflowContract {
            saga_type: saga_type.into(),
        }) {
            Some(BusStateReply::WorkflowContract(contract)) => contract,
            _ => None,
        };
        let Some(contract) = contract else {
            return Some(format!(
                "workflow contract is required before saga start; saga_type={} saga_id={}",
                saga_type,
                context.saga_id.get()
            ));
        };

        if context.step_name.as_ref() != contract.first_step.as_ref() {
            return Some(format!(
                "workflow contract first_step mismatch at saga start; saga_type={} expected_first_step={} received_first_step={}",
                saga_type, contract.first_step, context.step_name
            ));
        }

        let bound_for_type = match self.ask_state(BusStateAsk::BoundSteps {
            saga_type: saga_type.into(),
        }) {
            Some(BusStateReply::BoundSteps(steps)) => steps,
            _ => HashSet::new(),
        };
        let mut missing_steps: Vec<&str> = contract
            .declared_steps
            .iter()
            .filter(|step| !bound_for_type.contains(step.as_ref()))
            .map(|step| step.as_ref())
            .collect();
        missing_steps.sort_unstable();
        if !missing_steps.is_empty() {
            return Some(format!(
                "workflow contract violation: unbound participant steps; saga_type={} missing_steps={}",
                saga_type,
                missing_steps.join(",")
            ));
        }

        None
    }

    fn saga_start_expected_min_delivery(&self, saga_type: &str) -> Option<u32> {
        match self.ask_state(BusStateAsk::SagaStartExpectedMinDelivery {
            saga_type: saga_type.into(),
        }) {
            Some(BusStateReply::OptionalU32(expected)) => expected,
            _ => None,
        }
    }

    fn required_path_description(&self, saga_type: &str) -> String {
        match self.ask_state(BusStateAsk::RequiredPathDescription {
            saga_type: saga_type.into(),
        }) {
            Some(BusStateReply::String(description)) => description,
            _ => String::new(),
        }
    }

    fn required_path_expected_min_delivery_for_event(
        &self,
        event: &SagaChoreographyEvent,
    ) -> Option<u32> {
        if matches!(
            event,
            SagaChoreographyEvent::SagaCompleted { .. }
                | SagaChoreographyEvent::SagaFailed { .. }
                | SagaChoreographyEvent::SagaQuarantined { .. }
        ) {
            return None;
        }

        let context = event.context();
        if !self.has_terminal_policy_for_saga_type(context.saga_type.as_ref()) {
            return None;
        }
        match self.ask_state(BusStateAsk::RequiredPathExpectedMinDelivery {
            saga_type: context.saga_type.clone(),
            step_name: context.step_name.clone(),
        }) {
            Some(BusStateReply::OptionalU32(expected)) => expected,
            _ => None,
        }
    }

    fn store_terminal_reply(&self, run: RunKey, reply: SagaReplyTo) -> Option<SagaReplyTo> {
        let retention_limit = self.terminal_retention_limit();
        match self.ask_state(BusStateAsk::StoreTerminalReply {
            run,
            reply,
            retention_limit,
        }) {
            Some(BusStateReply::TerminalReply(reply)) => reply,
            _ => None,
        }
    }

    fn store_terminal_outcome(&self, context: &SagaContext, outcome: SagaTerminalOutcome) {
        let retention_limit = self.terminal_retention_limit();
        let _ = self.ask_state(BusStateAsk::StoreTerminalOutcome {
            run: RunKey::of(context),
            outcome,
            retention_limit,
        });
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

fn spawn_terminal_watchdog_if_needed(
    policy: &TerminalPolicy,
    resolver_ref: local_sync::SyncActorRef<TerminalResolverActor>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
    let saga_type = policy.saga_type.clone();
    let watchdog_name = format!("saga-terminal-watchdog:{saga_type}");
    let spawn_result = thread::Builder::new().name(watchdog_name).spawn(move || {
        loop {
            thread::sleep(terminal_watchdog_tick_interval());
            if shutdown.load(Ordering::Acquire) {
                break;
            }
            if !resolver_ref.tell(TerminalResolverTell::PollTimeouts) {
                tracing::error!(
                    target: "core::saga",
                    event = "terminal_watchdog_poll_failed",
                    saga_type = saga_type.as_ref()
                );
                break;
            }
        }
    });
    if let Err(err) = spawn_result {
        return Err(format!(
            "terminal watchdog spawn failed saga_type={}: {}",
            policy.saga_type, err
        ));
    }
    Ok(())
}

pub fn global_saga_choreography_bus() -> SagaChoreographyBus {
    static BUS: OnceLock<SagaChoreographyBus> = OnceLock::new();
    BUS.get_or_init(SagaChoreographyBus::new).clone()
}

impl Clone for SagaChoreographyBus {
    fn clone(&self) -> Self {
        Self {
            bus: self.bus.clone(),
            pending_replies: self.pending_replies.clone(),
            pending_run_replies: self.pending_run_replies.clone(),
            legacy_waiter_floors: Arc::clone(&self.legacy_waiter_floors),
            state_ref: self.state_ref.clone(),
            terminal_resolver_registry_ref: self.terminal_resolver_registry_ref.clone(),
            release: Arc::clone(&self.release),
            gates: Arc::clone(&self.gates),
            _lifecycle: self._lifecycle.clone(),
        }
    }
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
        let (probe_addr, probe_handle) = local_sync::mpsc::spawn(8, |msg: ProbeMsg| match msg {
            ProbeMsg::Register {
                bus,
                saga_id,
                reply,
            } => {
                let _ = bus.register_terminal_reply(saga_id, reply);
            }
        });
        let pending = probe_addr
            .ask_delegated(|reply| ProbeMsg::Register {
                bus,
                saga_id,
                reply,
            })
            .expect("pending reply should be registered");
        (pending, probe_handle)
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
        let lifecycle = Arc::downgrade(bus._lifecycle.as_ref().unwrap());
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
        let lifecycle = Arc::downgrade(bus._lifecycle.as_ref().unwrap());
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
                bus.register_bound_workflow_step("durable_multi_step", step)
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
        bus.register_terminal_policy(&TerminalPolicy::order_lifecycle_default());
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
        bus.register_bound_workflow_step("order_lifecycle", "create_order")
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
        bus.register_bound_workflow_step("order_lifecycle", "create_order")
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
        bus.register_bound_workflow_step("order_lifecycle", "create_order")
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
        bus.register_bound_workflow_step("order_lifecycle", "create_order")
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
        bus.register_bound_workflow_step("order_lifecycle", "risk_check")
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
        bus.register_bound_workflow_step("order_lifecycle", "risk_check")
            .expect("risk_check binding should succeed");
        bus.register_bound_workflow_step("order_lifecycle", "create_order")
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
        bus.register_bound_workflow_step("order_lifecycle", "risk_check")
            .expect("risk_check binding should succeed");
        bus.register_bound_workflow_step("order_lifecycle", "manual_review")
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
        bus.register_bound_workflow_step("order_lifecycle", "risk_check")
            .expect("risk_check binding should succeed");
        bus.register_bound_workflow_step("order_lifecycle", "create_order")
            .expect("create_order binding should succeed");
        let _resolver = bus
            .attach_terminal_resolver_for_contract::<MultiStepOrderLifecycleContract>(
                "test-resolver",
            )
            .expect("terminal resolver should attach");
        let _risk_sub = bus.subscribe_saga_type_fn("order_lifecycle", |_event| true);
        let order_sub = bus.subscribe_saga_type_fn("order_lifecycle", |_event| true);

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
        bus.register_bound_workflow_step("order_lifecycle", "risk_check")
            .expect("risk_check binding should succeed");
        bus.register_bound_workflow_step("order_lifecycle", "create_order")
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
    fn register_bound_workflow_step_rejects_undeclared_step_with_contract() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<OrderLifecycleContract>()
            .expect("workflow contract registration should succeed");

        let err = bus
            .register_bound_workflow_step("order_lifecycle", "risk_check")
            .expect_err("undeclared step should be rejected");
        assert!(
            err.contains("bound workflow step is not declared by contract"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn register_workflow_contract_rejects_prebound_unknown_steps_without_partial_registration() {
        let bus = SagaChoreographyBus::new();
        bus.register_bound_workflow_step("order_lifecycle", "rogue_step")
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
    fn register_bound_workflow_step_rejects_empty_values() {
        let bus = SagaChoreographyBus::new();
        let err = bus
            .register_bound_workflow_step("", "create_order")
            .expect_err("empty saga_type must be rejected");
        assert!(
            err.contains("saga_type and step_name must be non-empty"),
            "unexpected error: {err}"
        );

        let err = bus
            .register_bound_workflow_step("order_lifecycle", "")
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
        bus.register_bound_workflow_step("order_lifecycle", "risk_check")
            .expect("risk_check binding should succeed");
        bus.register_bound_workflow_step("order_lifecycle", "create_order")
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
        SagaChoreographyEvent, SagaContext, SagaId, SagaWorkflowContract, SagaWorkflowStepContract,
        StepExecutionId, SuccessCriteria, TERMINAL_RESOLVER_STEP, TerminalPolicy,
        TerminalResolverJournal, TerminalResolverJournalEntry, TerminalResolverJournalError,
        WorkflowDependencySpec,
    };

    use super::{
        AdmissionIndex, AdmissionLimits, ResolverGate, RunPhase, SagaBusPublishError,
        SagaChoreographyBus,
    };

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
            bus.register_bound_workflow_step(C::saga_type(), step.step_name)
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
            .gates
            .lock()
            .unwrap()
            .get(saga_type)
            .and_then(std::sync::Weak::upgrade)
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
    fn ephemeral_admission_refuses_at_capacity_before_fanout_and_never_evicts() {
        let (bus, seen) = bus_with::<Chain>(None, limits(2, 1_000));
        let base = SagaContext::now_millis();
        complete_chain(&bus, &ctx("adm_chain", 21, "a", base), &seen, 1);
        complete_chain(&bus, &ctx("adm_chain", 22, "a", base), &seen, 2);
        let starts_before = count(&seen, is_start);
        let reason = rejected_reason(bus.publish_strict(started(&ctx("adm_chain", 23, "a", base))));
        assert!(reason.contains("capacity"), "{reason}");
        thread::sleep(Duration::from_millis(100));
        assert_eq!(
            count(&seen, is_start),
            starts_before,
            "refused before fanout"
        );
        // Resolved fences were not forgotten to make room.
        let replay = rejected_reason(bus.publish_strict(started(&ctx("adm_chain", 21, "a", base))));
        assert!(replay.contains("replay"), "{replay}");
        assert_eq!(resident(&bus, "adm_chain").0, 2);
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
    fn ephemeral_capacity_also_bounds_runs_reusing_one_saga_id() {
        let (bus, seen) = bus_with::<Chain>(None, limits(2, 1_000));
        let base = SagaContext::now_millis();
        for offset in 0..2 {
            complete_chain(
                &bus,
                &ctx("adm_chain", 450, "a", base + offset),
                &seen,
                offset as usize + 1,
            );
        }
        let starts = count(&seen, is_start);
        let reason =
            rejected_reason(bus.publish_strict(started(&ctx("adm_chain", 450, "a", base + 2))));
        assert!(reason.contains("capacity"), "{reason}");
        assert_eq!(count(&seen, is_start), starts);
        let replay =
            rejected_reason(bus.publish_strict(started(&ctx("adm_chain", 450, "a", base))));
        assert!(
            replay.contains("replay"),
            "old fences must stay authoritative: {replay}"
        );
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
            bus.register_bound_workflow_step(Chain::saga_type(), step.step_name)
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
    fn ephemeral_index_never_reclaims_failed_fingerprints() {
        let (mut index, failed) = index_with_failed_run(3, 1, false);
        let active = ctx("adm_chain", 2, "a", 1_000);
        assert!(index.observe(&started(&active)));
        assert!(index.observe(&completed(&active, "a", 1, true)));
        assert!(!index.observe(&completed(&active, "b", 2, true)));
        assert!(
            index.run(&failed).is_some(),
            "ephemeral fences are never forgotten"
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
}
