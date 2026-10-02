use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
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
    /// Durable run index, built once from the journal at attach and kept
    /// current from admitted/ingested events. Serializes start admission.
    admission: Mutex<AdmissionIndex>,
}

/// Lifecycle phase of one run as known from retained durable history.
/// Ordered by strength: a phase never weakens.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum RunPhase {
    Active,
    Terminal,
    Quarantined,
}

/// What the resolver does with a non-start event, given durable history.
#[derive(Debug, PartialEq, Eq)]
enum Fence {
    /// Unknown or active run: the resolver handles it normally.
    Pass,
    /// Ordinarily resolved run: stale replay, no journal, no output.
    Drop,
    /// Quarantined run (or a quarantine of a resolved run): keep the evidence
    /// but never feed a resolver whose cache may have been evicted.
    RetainOnly,
    /// New compensable effect after an ordinary terminal: retain it and escalate.
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

#[derive(Default)]
struct AdmissionIndex {
    runs: HashMap<SagaId, Vec<(u64, RunPhase)>>,
    /// Starts whose admission intent is already journaled; the resolver must not
    /// append them a second time when the fanned-out event reaches it.
    prejournaled: HashSet<(SagaId, u64)>,
    completed_forward: HashSet<(SagaId, u64, Box<str>, u64)>,
    accepted_forward: HashSet<(SagaId, u64, Box<str>, crate::StepExecutionId)>,
}

impl AdmissionIndex {
    fn from_events<'a>(events: impl Iterator<Item = &'a SagaChoreographyEvent>) -> Self {
        let mut index = Self::default();
        for event in events {
            index.observe(event);
        }
        index
    }

    fn phase(&self, context: &SagaContext) -> Option<RunPhase> {
        self.runs
            .get(&context.saga_id)?
            .iter()
            .find(|(started, _)| *started == context.saga_started_at_millis)
            .map(|(_, phase)| *phase)
    }

    fn raise(&mut self, saga_id: SagaId, started: u64, phase: RunPhase) {
        let runs = self.runs.entry(saga_id).or_default();
        match runs.iter_mut().find(|(run, _)| *run == started) {
            Some((_, current)) => *current = (*current).max(phase),
            None => runs.push((started, phase)),
        }
    }

    fn observe(&mut self, event: &SagaChoreographyEvent) {
        let context = event.context();
        match event {
            SagaChoreographyEvent::StepCompleted { .. } => {
                self.completed_forward.insert((
                    context.saga_id,
                    context.saga_started_at_millis,
                    context.step_name.clone(),
                    context.trace_id,
                ));
            }
            SagaChoreographyEvent::StepAccepted { execution_id, .. } => {
                self.accepted_forward.insert((
                    context.saga_id,
                    context.saga_started_at_millis,
                    context.step_name.clone(),
                    execution_id.clone(),
                ));
            }
            _ => {}
        }
        let phase = match event {
            SagaChoreographyEvent::SagaCompleted { .. }
            | SagaChoreographyEvent::SagaFailed { .. } => RunPhase::Terminal,
            SagaChoreographyEvent::SagaQuarantined { .. } => RunPhase::Quarantined,
            _ => RunPhase::Active,
        };
        self.raise(context.saga_id, context.saga_started_at_millis, phase);
    }

    fn has_open_run(&self, saga_id: SagaId) -> bool {
        self.runs.get(&saga_id).is_some_and(|runs| {
            runs.iter()
                .any(|(_, phase)| matches!(phase, RunPhase::Active | RunPhase::Quarantined))
        })
    }

    /// Replay, quarantine and active-ownership rules for a new start.
    fn start_refusal(&self, start: &SagaContext) -> Option<String> {
        let runs = self.runs.get(&start.saga_id)?;
        if runs
            .iter()
            .any(|(_, phase)| *phase == RunPhase::Quarantined)
        {
            return Some(format!(
                "saga id is quarantined and unresolved; saga_id={}",
                start.saga_id.get()
            ));
        }
        if runs.iter().any(|(started, phase)| {
            *phase == RunPhase::Terminal && *started >= start.saga_started_at_millis
        }) {
            return Some(format!(
                "terminal saga run replay; saga_id={} run_started_at_millis={}",
                start.saga_id.get(),
                start.saga_started_at_millis
            ));
        }
        if runs.iter().any(|(started, phase)| {
            *phase == RunPhase::Active && *started != start.saga_started_at_millis
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
        self.runs.get(&context.saga_id).is_some_and(|runs| {
            runs.iter().any(|(started, phase)| {
                *started != context.saga_started_at_millis && *phase != RunPhase::Quarantined
            })
        })
    }

    fn fence(&self, event: &SagaChoreographyEvent) -> Fence {
        let context = event.context();
        match self.phase(context) {
            None | Some(RunPhase::Active) => Fence::Pass,
            Some(RunPhase::Quarantined) => Fence::RetainOnly,
            Some(RunPhase::Terminal) => match event {
                SagaChoreographyEvent::SagaQuarantined { .. } => Fence::RetainOnly,
                SagaChoreographyEvent::StepCompleted {
                    compensation_available: true,
                    ..
                } if !self.completed_forward.contains(&(
                    context.saga_id,
                    context.saga_started_at_millis,
                    context.step_name.clone(),
                    context.trace_id,
                )) =>
                {
                    Fence::Escalate
                }
                SagaChoreographyEvent::StepAccepted { execution_id, .. }
                    if !self.accepted_forward.contains(&(
                        context.saga_id,
                        context.saga_started_at_millis,
                        context.step_name.clone(),
                        execution_id.clone(),
                    )) =>
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
        // The index only ever grows monotonically, so a poisoned guard is still
        // a safe (conservative) view for the resolver actor.
        self.admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Serialized start admission: check, journal the intent strictly, then
    /// reserve the run. Nothing is fanned out unless this returns `Ok`.
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
        if let Some(reason) = index.start_refusal(context) {
            return Err(Refusal {
                reason: reason.into(),
                reject_run_waiter: index.phase(context).is_none(),
                reject_id_waiter: !index.has_open_run(context.saga_id),
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
            index
                .prejournaled
                .insert((context.saga_id, context.saga_started_at_millis));
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
    runtimes: HashMap<Box<str>, TerminalResolverRuntime>,
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
        if let Some(journal) = &self.gate.journal
            && let Err(error) = journal.append(event.clone())
        {
            tracing::error!(target: "core::saga", event = "terminal_resolver_evidence_append_failed",
                saga_type = self.saga_type.as_ref(), saga_id = %event.context().saga_id, error = ?error);
        }
        // Storage failure cannot weaken the live fence; restart reconciliation
        // still requires a writable journal/application-owned durable evidence.
        self.gate.index().observe(event);
    }

    /// A compensable effect materialised after an ordinary terminal. The
    /// evidence is retained and the run escalates to quarantine. The index is
    /// raised first so a second queued late effect cannot escalate twice.
    fn escalate_late_effect(&mut self, event: &SagaChoreographyEvent) {
        let context = event.context();
        self.retain_evidence(event);
        self.gate.index().raise(
            context.saga_id,
            context.saga_started_at_millis,
            RunPhase::Quarantined,
        );
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
                    let fence = self.gate.index().fence(&event);
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
                                    && self.gate.index().has_successor(event.context());
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
                    && self.gate.index().prejournaled.remove(&(
                        event.context().saga_id,
                        event.context().saga_started_at_millis,
                    ));
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
                    if !matches!(*event, SagaChoreographyEvent::SagaQuarantined { .. }) {
                        let context = event.context().next_step(TERMINAL_RESOLVER_STEP.into());
                        self.publish_terminal_events(vec![
                            SagaChoreographyEvent::SagaQuarantined {
                                context,
                                reason: format!("terminal resolver durability failed: {error}")
                                    .into(),
                                step: TERMINAL_RESOLVER_STEP.into(),
                                participant_id: self.responder.as_ref().into(),
                            },
                        ]);
                    }
                    return;
                }
                if !already_journaled {
                    self.gate.index().observe(&event);
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
        let (state_ref, state_handle) = local_sync::spawn(BusStateActor::default());
        let (terminal_resolver_registry_ref, terminal_resolver_registry_handle) =
            local_sync::spawn(TerminalResolverRegistryActor::default());
        let pending_replies = CorrelationRegistry::new();
        let pending_run_replies = CorrelationRegistry::new();
        let lifecycle = Arc::new(BusActorLifecycle {
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
    /// It is resolved only by a terminal of a run that started at or after the
    /// registration; prefer [`Self::register_terminal_reply_for_run`].
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
        if let Ok(mut waiters) = self.legacy_waiter_floors.lock()
            && let Some(waiter) = waiters.get_mut(&context.saga_id)
            && waiter.bound_run.is_none()
        {
            waiter.bound_run = Some(RunKey::of(context));
        }
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
        self.attach_terminal_resolver_inner(policy, responder, None)
    }

    pub fn attach_durable_terminal_resolver<J: TerminalResolverJournal>(
        &self,
        policy: TerminalPolicy,
        responder: &'static str,
        journal: Arc<J>,
    ) -> Result<FirehoseSubscription, String> {
        self.attach_terminal_resolver_inner(policy, responder, Some(journal))
    }

    fn attach_terminal_resolver_inner(
        &self,
        policy: TerminalPolicy,
        responder: &'static str,
        journal: Option<Arc<dyn TerminalResolverJournal>>,
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
                // The only full history read: later starts consult this index.
                let admission = AdmissionIndex::from_events(events.iter());
                let (resolver, recovery_events) =
                    TerminalResolver::restore_from_events(policy.clone(), &events);
                (resolver, recovery_events, admission)
            }
            None => (
                TerminalResolver::new(policy.clone()),
                Vec::new(),
                AdmissionIndex::default(),
            ),
        };
        let durable = journal.is_some();
        let gate = Arc::new(ResolverGate {
            journal,
            activated: AtomicBool::new(!durable),
            admission: Mutex::new(admission),
        });
        let (resolver_ref, resolver_handle) = local_sync::spawn(TerminalResolverActor {
            resolver,
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
        assert_eq!(
            Arc::strong_count(&journal),
            1,
            "resolver and gate must release LMDB ownership at shutdown"
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
