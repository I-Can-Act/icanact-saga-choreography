use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

use icanact_core::CorrelationRegistry;
use icanact_core::local::{FirehosePubSub, FirehoseSubscription, PublishStats};
use icanact_core::local_sync::{self, SyncActor};

use crate::events::AbortSource;
use crate::reply_registry::{SagaReplyToHandle, SagaReplyToResult};
use crate::resolver::{FailureAuthority, SuccessCriteria};
use crate::workflow_contract::required_path_steps_from_success_criteria;
use crate::{
    HasSagaWorkflowParticipants, RunIncarnation, RunKey, SagaChoreographyEvent, SagaContext,
    SagaId, SagaReplyTo, SagaTerminalOutcome, SagaWorkflowContract, SagaWorkflowStepContract,
    TERMINAL_RESOLVER_STEP, TerminalPolicy, TerminalResolver, TerminalResolverJournal,
    required_steps_from_success_criteria, validate_workflow_contract,
};

#[derive(Clone, Debug)]
struct WorkflowContractState {
    first_step: Box<str>,
    declared_steps: HashSet<Box<str>>,
    required_path_steps: HashSet<Box<str>>,
    required_path_description: Box<str>,
}

#[derive(Default)]
struct BusStateActor {
    terminal_replies: HashMap<RunKey, SagaReplyTo>,
    terminal_outcomes: HashMap<RunKey, SagaTerminalOutcome>,
    terminal_order: VecDeque<RunKey>,
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
    /// Required-path step names whose participants must receive an event of
    /// `saga_type`; `step_name: None` asks for the saga-start set.
    RequiredRecipients {
        saga_type: Box<str>,
        step_name: Option<Box<str>>,
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
    /// `SagaId` wrapper: resolves the newest cached incarnation (ADR-0001).
    TakeNewestTerminalReply {
        saga_id: SagaId,
    },
    /// `SagaId` wrapper: resolves the newest cached incarnation (ADR-0001).
    TakeNewestTerminalOutcome {
        saga_id: SagaId,
    },
    /// Is a strictly newer incarnation of the same `SagaId` cached?
    HasNewerTerminal {
        run: RunKey,
    },
}

#[derive(Clone, Debug)]
enum BusStateReply {
    Unit,
    Bool(bool),
    WorkflowContract(Option<WorkflowContractState>),
    BoundSteps(HashSet<Box<str>>),
    String(String),
    RequiredRecipients(Option<HashSet<Box<str>>>),
    TerminalReply(Option<SagaReplyTo>),
    TerminalOutcome(Option<SagaTerminalOutcome>),
}

impl BusStateActor {
    fn newest_cached_run(&self, saga_id: SagaId) -> Option<RunKey> {
        self.terminal_outcomes
            .keys()
            .chain(self.terminal_replies.keys())
            .filter(|run| run.saga_id() == saga_id)
            .max_by(|a, b| a.incarnation().cmp(&b.incarnation()).then_with(|| a.cmp(b)))
            .cloned()
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

    fn insert_terminal_outcome(
        &mut self,
        run: RunKey,
        outcome: SagaTerminalOutcome,
        retention_limit: usize,
    ) {
        let inserted_new = self
            .terminal_outcomes
            .insert(run.clone(), outcome)
            .is_none();
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
                break;
            }
        }
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
            BusStateAsk::RequiredRecipients {
                saga_type,
                step_name,
            } => {
                let required = self
                    .workflow_contracts_by_saga_type
                    .get(saga_type.as_ref())
                    .and_then(|contract| {
                        // The emitting step never receives its own event; for
                        // SagaStarted (`None`) the emitter is the start step.
                        let emitter = match &step_name {
                            Some(step) if !contract.required_path_steps.contains(step.as_ref()) => {
                                return None;
                            }
                            Some(step) => step.as_ref(),
                            None => contract.first_step.as_ref(),
                        };
                        let mut required = contract.required_path_steps.clone();
                        required.remove(emitter);
                        Some(required)
                    });
                BusStateReply::RequiredRecipients(required)
            }
            BusStateAsk::StoreTerminalReply {
                run,
                reply,
                retention_limit,
            } => {
                let outcome = reply.outcome.clone();
                self.terminal_replies.insert(run.clone(), reply);
                self.insert_terminal_outcome(run, outcome, retention_limit);
                BusStateReply::Unit
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
            BusStateAsk::TakeNewestTerminalReply { saga_id } => {
                let reply = self
                    .newest_cached_run(saga_id)
                    .and_then(|run| self.take_reply(&run));
                BusStateReply::TerminalReply(reply)
            }
            BusStateAsk::TakeNewestTerminalOutcome { saga_id } => {
                let outcome = self
                    .newest_cached_run(saga_id)
                    .and_then(|run| self.take_outcome(&run));
                BusStateReply::TerminalOutcome(outcome)
            }
            BusStateAsk::HasNewerTerminal { run } => BusStateReply::Bool(
                self.newest_cached_run(run.saga_id())
                    .is_some_and(|newest| newest.incarnation() > run.incarnation()),
            ),
        }
    }
}

/// Identity of the configuration a resolver was attached with: the full policy
/// (Debug fingerprint) plus the address of the durable journal, if any.
#[derive(Clone, PartialEq, Eq)]
struct ResolverAttachConfig {
    policy: String,
    journal: Option<usize>,
}

impl ResolverAttachConfig {
    fn new(policy: &TerminalPolicy, journal: Option<&Arc<dyn TerminalResolverJournal>>) -> Self {
        Self {
            policy: canonical_policy_fingerprint(policy),
            journal: journal.map(|journal| Arc::as_ptr(journal).cast::<()>() as usize),
        }
    }
}

fn sorted_steps(steps: &HashSet<Box<str>>) -> Vec<&str> {
    let mut sorted: Vec<&str> = steps.iter().map(AsRef::as_ref).collect();
    sorted.sort_unstable();
    sorted
}

/// Deterministic rendering of a policy: `HashSet`-backed members are sorted so
/// two equal policies always fingerprint identically.
fn canonical_policy_fingerprint(policy: &TerminalPolicy) -> String {
    let authority = match &policy.failure_authority {
        FailureAuthority::AnyParticipant => "AnyParticipant".to_string(),
        FailureAuthority::OnlySteps(steps) => format!("OnlySteps({:?})", sorted_steps(steps)),
        FailureAuthority::DenySteps(steps) => format!("DenySteps({:?})", sorted_steps(steps)),
    };
    let criteria = match &policy.success_criteria {
        SuccessCriteria::AllOf(steps) => format!("AllOf({:?})", sorted_steps(steps)),
        SuccessCriteria::AnyOf(steps) => format!("AnyOf({:?})", sorted_steps(steps)),
        SuccessCriteria::Quorum {
            group_steps,
            required_count,
        } => format!("Quorum({:?},{required_count})", sorted_steps(group_steps)),
    };
    format!(
        "saga_type={} policy_id={} authority={authority} criteria={criteria} overall={:?} stalled={:?} steps={:?} retry={} horizon={:?} loser={:?}",
        policy.saga_type,
        policy.policy_id,
        policy.overall_timeout,
        policy.stalled_timeout,
        policy.workflow_steps,
        policy.compensation_retry_limit(),
        policy.replay_horizon(),
        policy.loser_policy(),
    )
}

struct TerminalResolverRuntime {
    config: ResolverAttachConfig,
    subscription: FirehoseSubscription,
    shutdown: Arc<AtomicBool>,
    resolver_ref: local_sync::SyncActorRef<TerminalResolverActor>,
    handle: local_sync::ActorHandle,
}

enum TerminalResolverRegistryAsk {
    Existing(Box<str>),
    Register {
        saga_type: Box<str>,
        runtime: Box<TerminalResolverRuntime>,
    },
    ActivateRecovery(Box<str>),
    ShutdownAll,
}

enum TerminalResolverRegistryReply {
    Existing(Option<(FirehoseSubscription, ResolverAttachConfig)>),
    Registered(FirehoseSubscription),
    RecoveryActivated(bool),
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
                        .map(|runtime| (runtime.subscription.clone(), runtime.config.clone())),
                )
            }
            TerminalResolverRegistryAsk::Register { saga_type, runtime } => {
                match self.runtimes.entry(saga_type) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let subscription = runtime.subscription.clone();
                        entry.insert(*runtime);
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
                        runtime
                            .resolver_ref
                            .tell(TerminalResolverTell::ActivateRecovery)
                    });
                TerminalResolverRegistryReply::RecoveryActivated(activated)
            }
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
    /// Set by `ActivateRecovery`; until then the watchdog must not publish
    /// restored deadlines (the participants are not bound yet).
    activated: bool,
    journal: Option<Arc<dyn TerminalResolverJournal>>,
    bus: SagaChoreographyBus,
    responder: Arc<str>,
    saga_type: Box<str>,
}

impl TerminalResolverActor {
    /// Publishes resolver outputs. Replies are NOT resolved here: they follow the
    /// durable decision when the resolver ingests its own echoed terminal event
    /// (ADR-0003 §2.4). Returns the events whose publication failed, so the owner
    /// can retain them; each failure is logged with its `RunKey`.
    fn publish_terminal_events(
        &mut self,
        terminal_events: Vec<SagaChoreographyEvent>,
    ) -> Vec<SagaChoreographyEvent> {
        let mut failed = Vec::new();
        for terminal_event in terminal_events {
            if let Err(err) = self.bus.publish_strict(terminal_event.clone()) {
                let run = terminal_event.context().run_key();
                tracing::error!(
                    target: "core::saga",
                    event = "terminal_resolver_publish_failed",
                    saga_type = self.saga_type.as_ref(),
                    run = %run,
                    error = ?err
                );
                // A terminal decision that never reached the bus must not leave the waiter hanging.
                if terminal_event.terminal_outcome().is_some() {
                    self.bus.reject_terminal_reply_for_run(
                        &run,
                        format!("terminal publish failed: {err:?}"),
                    );
                }
                failed.push(terminal_event);
            }
        }
        failed
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
                if let Some(journal) = &self.journal
                    && let Err(error) = journal.append((*event).clone())
                {
                    let run = event.context().run_key();
                    tracing::error!(
                        target: "core::saga",
                        event = "terminal_resolver_journal_append_failed",
                        saga_type = self.saga_type.as_ref(),
                        run = %run,
                        error = ?error,
                        "event not ingested: resolver journal append failed"
                    );
                    let quarantine =
                        if matches!(*event, SagaChoreographyEvent::SagaQuarantined { .. }) {
                            (*event).clone()
                        } else {
                            let context = event.context().next_step(TERMINAL_RESOLVER_STEP.into());
                            SagaChoreographyEvent::SagaQuarantined {
                                context,
                                reason: format!("terminal resolver durability failed: {error}")
                                    .into(),
                                step: TERMINAL_RESOLVER_STEP.into(),
                                participant_id: self.responder.as_ref().into(),
                            }
                        };
                    // Durability failed: the only honest outcome for the waiter is quarantine.
                    self.bus
                        .complete_terminal_reply_from_event(&quarantine, self.responder.as_ref());
                    if !matches!(*event, SagaChoreographyEvent::SagaQuarantined { .. }) {
                        self.publish_terminal_events(vec![quarantine]);
                    }
                    return;
                }
                // The decision is durable (or no journal): now the reply may resolve.
                self.bus
                    .complete_terminal_reply_from_event(&event, self.responder.as_ref());
                match self
                    .resolver
                    .try_ingest_at(&event, SagaContext::now_millis())
                {
                    Ok(events) => events,
                    Err(error) => {
                        tracing::warn!(
                            target: "core::saga",
                            event = "terminal_resolver_run_identity_rejected",
                            run = %event.context().run_key(),
                            error = ?error
                        );
                        return;
                    }
                }
            }
            TerminalResolverTell::ActivateRecovery => {
                self.activated = true;
                let recovery_events = std::mem::take(&mut self.recovery_events);
                self.recovery_events = self.publish_terminal_events(recovery_events);
                // Overdue work that matured while recovery was inactive is
                // evaluated exactly once, now.
                self.resolver.poll_timeouts()
            }
            TerminalResolverTell::PollTimeouts if !self.activated => {
                tracing::debug!(
                    target: "core::saga",
                    event = "terminal_watchdog_poll_deferred_until_recovery_activation",
                    saga_type = self.saga_type.as_ref()
                );
                return;
            }
            TerminalResolverTell::PollTimeouts => self.resolver.poll_timeouts(),
        };
        self.publish_terminal_events(terminal_events);
    }
}

/// Test-only callback run between resolver subscribe and registry register.
#[cfg(test)]
type AttachHook = Arc<std::sync::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>>;

thread_local! {
    /// Addresses of attach locks the current thread holds (re-entrancy guard).
    static HELD_ATTACH_LOCKS: std::cell::RefCell<Vec<usize>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Holds `resolver_attach_lock` unless this thread already holds it.
struct AttachLockGuard<'a> {
    key: usize,
    _guard: Option<std::sync::MutexGuard<'a, ()>>,
}

impl<'a> AttachLockGuard<'a> {
    fn acquire(lock: &'a Arc<std::sync::Mutex<()>>) -> Self {
        let key = Arc::as_ptr(lock) as usize;
        if HELD_ATTACH_LOCKS.with(|held| held.borrow().contains(&key)) {
            return Self { key, _guard: None };
        }
        let guard = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        HELD_ATTACH_LOCKS.with(|held| held.borrow_mut().push(key));
        Self {
            key,
            _guard: Some(guard),
        }
    }
}

impl Drop for AttachLockGuard<'_> {
    fn drop(&mut self) {
        if self._guard.is_some() {
            HELD_ATTACH_LOCKS.with(|held| {
                let mut held = held.borrow_mut();
                if let Some(pos) = held.iter().rposition(|k| *k == self.key) {
                    held.remove(pos);
                }
            });
        }
    }
}

/// Result of [`SagaChoreographyBus::publish_with_admission`].
struct PublishOutcome {
    stats: PublishStats,
    /// Rejection reason when a `SagaStarted` was refused.
    rejected: Option<Box<str>>,
    /// Required recipients that did not receive the event.
    shortfall: Option<RequiredShortfall>,
}

impl PublishOutcome {
    fn delivered(stats: PublishStats) -> Self {
        Self {
            stats,
            rejected: None,
            shortfall: None,
        }
    }

    fn rejected(stats: PublishStats, reason: Box<str>) -> Self {
        Self {
            stats,
            rejected: Some(reason),
            shortfall: None,
        }
    }
}

/// Which subscriber roles accepted one publish. Observers are never recorded.
#[derive(Default)]
struct DeliveryReceipts {
    /// Step-tagged participants that accepted the event.
    steps: HashSet<Box<str>>,
    /// Untagged participants that accepted the event.
    unnamed_participants: usize,
    resolver: bool,
}

/// How a subscription counts toward required-recipient safety.
#[derive(Clone)]
enum SubscriberRole {
    /// A workflow participant owning `steps` (empty: step not known).
    Participant(Arc<[Box<str>]>),
    /// The attached terminal resolver.
    Resolver,
}

thread_local! {
    /// Receipt frames of in-flight publishes on this thread. Subscriber
    /// callbacks run synchronously inside `publish`, so the innermost frame
    /// belongs to the publish that is delivering; nested publishes push their own.
    static RECEIPT_FRAMES: std::cell::RefCell<Vec<DeliveryReceipts>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

struct ReceiptFrame {
    open: bool,
}

impl ReceiptFrame {
    fn open() -> Self {
        RECEIPT_FRAMES.with(|frames| frames.borrow_mut().push(DeliveryReceipts::default()));
        Self { open: true }
    }

    fn close(mut self) -> DeliveryReceipts {
        self.open = false;
        RECEIPT_FRAMES
            .with(|frames| frames.borrow_mut().pop())
            .unwrap_or_default()
    }
}

impl Drop for ReceiptFrame {
    fn drop(&mut self) {
        if self.open {
            // A publish unwound: drop its frame so outer frames stay aligned.
            RECEIPT_FRAMES.with(|frames| frames.borrow_mut().pop());
        }
    }
}

fn record_receipt(role: &SubscriberRole) {
    RECEIPT_FRAMES.with(|frames| {
        let mut frames = frames.borrow_mut();
        let Some(frame) = frames.last_mut() else {
            return;
        };
        match role {
            SubscriberRole::Resolver => frame.resolver = true,
            SubscriberRole::Participant(steps) if steps.is_empty() => {
                frame.unnamed_participants += 1;
            }
            SubscriberRole::Participant(steps) => {
                frame.steps.extend(steps.iter().cloned());
            }
        }
    });
}

struct RequiredShortfall {
    /// Comma-separated missing required roles (step names / terminal resolver).
    roles: Box<str>,
    /// Required participants plus the resolver.
    required_min_delivered: u32,
}

/// Required roles that did not receive the event, or `None` when every
/// required participant step and the resolver did. An untagged participant can
/// stand in for one otherwise-unmatched required step; observers never count.
fn required_shortfall(
    required: &HashSet<Box<str>>,
    receipts: &DeliveryReceipts,
) -> Option<RequiredShortfall> {
    let mut unmatched: Vec<&str> = required
        .iter()
        .filter(|step| !receipts.steps.contains(step.as_ref()))
        .map(|step| step.as_ref())
        .collect();
    unmatched.sort_unstable();
    let mut missing: Vec<&str> = if unmatched.len() > receipts.unnamed_participants {
        unmatched
    } else {
        Vec::new()
    };
    if !receipts.resolver {
        missing.push(TERMINAL_RESOLVER_STEP);
    }
    if missing.is_empty() {
        return None;
    }
    Some(RequiredShortfall {
        roles: missing.join(",").into(),
        required_min_delivered: saturating_u32_from_usize(required.len().saturating_add(1)),
    })
}

pub struct SagaChoreographyBus {
    bus: FirehosePubSub<SagaChoreographyEvent>,
    pending_replies: CorrelationRegistry<RunKey, SagaReplyToResult>,
    /// Waiters registered by bare `SagaId` (no run known); see `complete_terminal_reply_for_run`.
    legacy_pending_replies: CorrelationRegistry<SagaId, SagaReplyToResult>,
    state_ref: local_sync::SyncActorRef<BusStateActor>,
    terminal_resolver_registry_ref: local_sync::SyncActorRef<TerminalResolverRegistryActor>,
    /// Serializes resolver attach so setup/commit is atomic per bus.
    resolver_attach_lock: Arc<std::sync::Mutex<()>>,
    /// Per saga type: count of `SagaAbortRequested` events the attached
    /// resolver's own subscription successfully handed to the resolver actor.
    /// Short-held; never locked across a publish and never asks the registry
    /// actor, so the resolver thread can consult it without deadlock risk.
    resolver_abort_ingest: Arc<std::sync::Mutex<HashMap<Box<str>, Arc<AtomicU64>>>>,
    #[cfg(test)]
    attach_hook: AttachHook,
    _lifecycle: Arc<BusActorLifecycle>,
}

struct BusActorLifecycle {
    pending_replies: CorrelationRegistry<RunKey, SagaReplyToResult>,
    legacy_pending_replies: CorrelationRegistry<SagaId, SagaReplyToResult>,
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
        for (_, reply) in self.legacy_pending_replies.drain() {
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
    /// A `SagaAbortRequested` reached no attached resolver; a terminal
    /// `SagaFailed` fallback was published instead.
    AbortNotDelivered {
        saga_id: SagaId,
        saga_type: Box<str>,
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
        missing_roles: Box<str>,
    },
}

impl SagaChoreographyBus {
    pub fn new() -> Self {
        ensure_saga_sync_pool_capacity();
        let (state_ref, state_handle) = local_sync::spawn(BusStateActor::default());
        let (terminal_resolver_registry_ref, terminal_resolver_registry_handle) =
            local_sync::spawn(TerminalResolverRegistryActor::default());
        let pending_replies = CorrelationRegistry::new();
        let legacy_pending_replies = CorrelationRegistry::new();
        let lifecycle = Arc::new(BusActorLifecycle {
            pending_replies: pending_replies.clone(),
            legacy_pending_replies: legacy_pending_replies.clone(),
            state_handle: Some(state_handle),
            terminal_resolver_registry_ref: terminal_resolver_registry_ref.clone(),
            terminal_resolver_registry_handle: Some(terminal_resolver_registry_handle),
        });
        Self {
            bus: FirehosePubSub::new(),
            pending_replies,
            legacy_pending_replies,
            state_ref,
            terminal_resolver_registry_ref,
            resolver_attach_lock: Arc::new(std::sync::Mutex::new(())),
            resolver_abort_ingest: Arc::new(std::sync::Mutex::new(HashMap::new())),
            #[cfg(test)]
            attach_hook: Arc::new(std::sync::Mutex::new(None)),
            _lifecycle: lifecycle,
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

    /// Subscribes a workflow participant owning `steps`. Unlike
    /// [`Self::subscribe_fn`] (an observer, which never counts toward
    /// required-recipient safety), an accepted delivery here is a receipt for
    /// each of `steps`. An empty `steps` is an untagged participant.
    pub fn subscribe_participant_fn<F>(
        &self,
        topic: &str,
        steps: &[&str],
        f: F,
    ) -> FirehoseSubscription
    where
        F: Fn(&SagaChoreographyEvent) -> bool + Send + Sync + 'static,
    {
        let role = SubscriberRole::Participant(steps.iter().map(|s| Box::from(*s)).collect());
        self.subscribe_role_fn(topic, role, f)
    }

    fn subscribe_role_fn<F>(&self, topic: &str, role: SubscriberRole, f: F) -> FirehoseSubscription
    where
        F: Fn(&SagaChoreographyEvent) -> bool + Send + Sync + 'static,
    {
        self.subscribe_fn(topic, move |event| {
            let accepted = f(event);
            if accepted {
                record_receipt(&role);
            }
            accepted
        })
    }

    pub fn unsubscribe(&self, sub: FirehoseSubscription) -> bool {
        self.bus.unsubscribe(sub)
    }

    fn publish_event(&self, event: SagaChoreographyEvent) -> PublishStats {
        let saga_type = event.context().saga_type.clone();
        self.publish_to_saga_type(saga_type.as_ref(), event)
    }

    /// Publishes `event` and returns which subscriber roles accepted it.
    fn publish_event_collecting(
        &self,
        event: SagaChoreographyEvent,
    ) -> (PublishStats, DeliveryReceipts) {
        let frame = ReceiptFrame::open();
        let stats = self.publish_event(event);
        (stats, frame.close())
    }

    pub fn publish(&self, event: SagaChoreographyEvent) -> PublishStats {
        self.publish_with_admission(event).stats
    }

    /// Publishes `event`; the second value is the rejection reason when a
    /// `SagaStarted` was refused and replaced by a diagnostic `SagaFailed`.
    fn publish_with_admission(&self, event: SagaChoreographyEvent) -> PublishOutcome {
        if matches!(event, SagaChoreographyEvent::SagaAbortRequested { .. }) {
            return PublishOutcome::delivered(self.publish_abort_event(event).0);
        }
        let event_type = event.event_type();
        let mut required_recipients: Option<HashSet<Box<str>>> = None;
        let mut expected_required_path: Box<str> = "".into();
        let mut expected_context: Option<crate::SagaContext> = None;
        if let SagaChoreographyEvent::SagaStarted { context, .. } = &event {
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
                    self.store_terminal_outcome(terminal.context().run_key(), outcome);
                }
                return PublishOutcome::rejected(self.publish_event(terminal), reason);
            }
            if let Some(reason) = self.saga_start_contract_violation_reason(context) {
                let reason: Box<str> = reason.into();
                let terminal = SagaChoreographyEvent::SagaFailed {
                    context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
                    reason: reason.clone(),
                    failure: None,
                };
                if let Some(outcome) = terminal.terminal_outcome() {
                    self.store_terminal_outcome(terminal.context().run_key(), outcome);
                }
                return PublishOutcome::rejected(self.publish_event(terminal), reason);
            }
            required_recipients = self.saga_start_required_recipients(context.saga_type.as_ref());
            expected_required_path = self
                .required_path_description(context.saga_type.as_ref())
                .into();
            expected_context = Some(context.clone());
        } else if let Some(required) = self.required_recipients_for_event(&event) {
            required_recipients = Some(required);
            expected_required_path = self
                .required_path_description(event.context().saga_type.as_ref())
                .into();
            expected_context = Some(event.context().clone());
        }
        if let Some(outcome) = event.terminal_outcome() {
            self.store_terminal_outcome(event.context().run_key(), outcome);
        }
        let (stats, receipts) = self.publish_event_collecting(event);
        let mut shortfall = None;
        if let (Some(required), Some(context)) = (required_recipients, expected_context)
            && let Some(missing) = required_shortfall(&required, &receipts)
        {
            let reason: Box<str> = format!(
                "required_path_delivery_shortfall: saga_type={} event_type={} step={} delivered={} attempted={} required_min_delivered={} missing_roles={} required_path={}",
                context.saga_type,
                event_type,
                context.step_name,
                stats.delivered,
                stats.attempted,
                missing.required_min_delivered,
                missing.roles,
                expected_required_path
            )
            .into();
            tracing::error!(
                target: "core::saga",
                event = "required_path_delivery_shortfall",
                run = ?context.run_key(),
                missing_roles = %missing.roles,
                delivered = stats.delivered,
                attempted = stats.attempted,
                "required recipient did not receive the event"
            );
            let _ = self.publish_abort_or_fail(&context, reason, AbortSource::DeliveryShortfall);
            shortfall = Some(missing);
        }
        PublishOutcome {
            stats,
            rejected: None,
            shortfall,
        }
    }

    /// Requests a resolver-driven abort (ADR-0004 §2.6).
    fn publish_abort_or_fail(
        &self,
        context: &crate::SagaContext,
        reason: Box<str>,
        source: AbortSource,
    ) -> PublishStats {
        let abort = SagaChoreographyEvent::SagaAbortRequested {
            context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
            reason,
            source,
        };
        self.publish_abort_event(abort).0
    }

    /// Abort-ingest counter of the attached resolver for `saga_type`, if any.
    fn resolver_abort_counter(&self, saga_type: &str) -> Option<Arc<AtomicU64>> {
        self.resolver_abort_ingest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(saga_type)
            .cloned()
    }

    /// Publishes a `SagaAbortRequested`. The abort only counts as delivered
    /// when the attached resolver's own subscription handed it to the resolver
    /// actor (ADR-0004 §2.6); otherwise an `error!` is logged and the only
    /// safe event left, a terminal `SagaFailed`, is published. The `bool` is
    /// whether the abort reached a resolver.
    ///
    /// `resolver_attach_lock` is held across the attached check and the abort
    /// publish, so a concurrent attach (subscribe -> register) is either fully
    /// before or fully after the abort: exactly one authority (resolver or
    /// fallback) sees it. Attach never publishes while holding the lock; the
    /// lock is skipped when this thread already holds it (a subscriber that
    /// publishes an abort re-entrantly).
    fn publish_abort_event(&self, abort: SagaChoreographyEvent) -> (PublishStats, bool) {
        let context = abort.context().clone();
        let (reason, source) = match &abort {
            SagaChoreographyEvent::SagaAbortRequested { reason, source, .. } => {
                (reason.clone(), *source)
            }
            _ => (Box::<str>::from("abort"), AbortSource::DeliveryShortfall),
        };
        let (stats, resolver_attached, reached) = {
            let _guard = AttachLockGuard::acquire(&self.resolver_attach_lock);
            let counter = self.resolver_abort_counter(context.saga_type.as_ref());
            let before = counter.as_ref().map(|c| c.load(Ordering::Acquire));
            let stats = self.publish_event(abort);
            let reached = match (&counter, before) {
                (Some(counter), Some(before)) => counter.load(Ordering::Acquire) != before,
                _ => false,
            };
            (stats, counter.is_some(), reached)
        };
        if reached {
            return (stats, true);
        }
        tracing::error!(
            target: "core::saga",
            event = "saga_abort_request_undelivered",
            run = ?context.run_key(),
            source = ?source,
            reason = %reason,
            resolver_attached,
            resolver_received = reached,
            delivered = stats.delivered,
            "abort request reached no resolver; publishing terminal SagaFailed"
        );
        let terminal = SagaChoreographyEvent::SagaFailed {
            context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
            reason,
            failure: None,
        };
        if let Some(outcome) = terminal.terminal_outcome() {
            self.store_terminal_outcome(terminal.context().run_key(), outcome);
        }
        (self.publish_event(terminal), false)
    }

    pub fn publish_strict(
        &self,
        event: SagaChoreographyEvent,
    ) -> Result<PublishStats, SagaBusPublishError> {
        if matches!(event, SagaChoreographyEvent::SagaAbortRequested { .. }) {
            let (stats, resolver_delivered) = self.publish_abort_event(event.clone());
            if !resolver_delivered {
                let context = event.context();
                return Err(SagaBusPublishError::AbortNotDelivered {
                    saga_id: context.saga_id,
                    saga_type: context.saga_type.clone(),
                    attempted: stats.attempted,
                    delivered: stats.delivered,
                });
            }
            return Ok(stats);
        }
        let PublishOutcome {
            stats,
            rejected,
            shortfall,
        } = self.publish_with_admission(event.clone());
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
        if let Some(missing) = shortfall {
            let context = event.context();
            return Err(SagaBusPublishError::RequiredPathDeliveryShortfall {
                saga_id: context.saga_id,
                saga_type: context.saga_type.clone(),
                step_name: context.step_name.clone(),
                event_type: event.event_type(),
                attempted: stats.attempted,
                delivered: stats.delivered,
                required_min_delivered: missing.required_min_delivered,
                required_path: self
                    .required_path_description(context.saga_type.as_ref())
                    .into(),
                missing_roles: missing.roles,
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
                | SagaChoreographyEvent::SagaAbortRequested { .. }
        );
        if is_terminal {
            return Err(partial);
        }

        let reason: Box<str> = format!(
            "publish_partial_delivery attempted={} delivered={} event_type={} step={}",
            attempted,
            delivered,
            event.event_type(),
            context.step_name
        )
        .into();
        let terminal_stats =
            self.publish_abort_or_fail(&context, reason, AbortSource::PartialDelivery);
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

    fn register_terminal_policy(&self, policy: &TerminalPolicy) -> Result<(), String> {
        match self.ask_state(BusStateAsk::RegisterTerminalPolicy {
            saga_type: policy.saga_type.clone(),
            policy_id: policy.policy_id.clone(),
        }) {
            Some(_) => Ok(()),
            None => Err(format!(
                "terminal policy readiness not registered: bus state unavailable saga_type={} policy_id={}",
                policy.saga_type, policy.policy_id
            )),
        }
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

    /// Registers a waiter for the terminal reply of the exact `run` (ADR-0001).
    pub fn register_terminal_reply_for_run(
        &self,
        run: &RunKey,
        reply: SagaReplyToHandle,
    ) -> Result<(), Box<str>> {
        match self.pending_replies.register(run.clone(), reply) {
            Ok(()) => Ok(()),
            Err(err) => {
                let reply = err.into_reply();
                let _ = reply.reply(Err("terminal reply already registered for run".into()));
                tracing::warn!(
                    target: "core::saga",
                    event = "terminal_reply_already_registered",
                    run = %run
                );
                Err("terminal reply already registered for run".into())
            }
        }
    }

    /// `SagaId` wrapper: the run is unknown at registration, so the waiter resolves with the
    /// first terminal event of any incarnation of `saga_id` that is not superseded by a newer
    /// cached terminal. Prefer [`Self::register_terminal_reply_for_run`].
    pub fn register_terminal_reply(
        &self,
        saga_id: SagaId,
        reply: SagaReplyToHandle,
    ) -> Result<(), Box<str>> {
        match self.legacy_pending_replies.register(saga_id, reply) {
            Ok(()) => Ok(()),
            Err(err) => {
                let reply = err.into_reply();
                let _ = reply.reply(Err("terminal reply already registered for saga id".into()));
                Err("terminal reply already registered for saga id".into())
            }
        }
    }

    /// Caches and delivers `reply` for the exact `run` only (ADR-0001).
    pub fn complete_terminal_reply_for_run(&self, run: &RunKey, reply: SagaReplyTo) -> bool {
        self.store_terminal_reply(run.clone(), reply.clone());
        let exact = self.pending_replies.resolve(run, Ok(reply.clone())).is_ok();
        // Unscoped waiters never receive a reply from an older incarnation once a newer
        // incarnation has a cached terminal.
        let superseded = matches!(
            self.ask_state(BusStateAsk::HasNewerTerminal { run: run.clone() }),
            Some(BusStateReply::Bool(true))
        );
        let legacy = !superseded
            && self
                .legacy_pending_replies
                .resolve(&run.saga_id(), Ok(reply))
                .is_ok();
        exact || legacy
    }

    /// `SagaId` wrapper: delivers to unscoped waiters and caches under an unscoped key that
    /// sorts below every real incarnation.
    pub fn complete_terminal_reply(&self, saga_id: SagaId, reply: SagaReplyTo) -> bool {
        self.store_terminal_reply(unscoped_run(saga_id), reply.clone());
        self.legacy_pending_replies
            .resolve(&saga_id, Ok(reply))
            .is_ok()
    }

    pub fn reject_terminal_reply_for_run(&self, run: &RunKey, reason: impl Into<String>) -> bool {
        self.pending_replies
            .resolve(run, Err(reason.into()))
            .is_ok()
    }

    pub fn reject_terminal_reply(&self, saga_id: SagaId, reason: impl Into<String>) -> bool {
        self.legacy_pending_replies
            .resolve(&saga_id, Err(reason.into()))
            .is_ok()
    }

    pub fn attach_terminal_resolver(
        &self,
        policy: TerminalPolicy,
        responder: &'static str,
    ) -> Result<FirehoseSubscription, String> {
        self.attach_terminal_resolver_inner(policy, responder, None)
    }

    /// Attaches a durable resolver restored from `journal`.
    ///
    /// Durable resolvers must be attached **before** participants run startup
    /// recovery. A `SagaAbortRequested` published while no resolver is
    /// attached is answered with a fallback terminal `SagaFailed` that the
    /// resolver never journals; a durable resolver attached later could then
    /// emit a second, contradicting outcome for the same run (ADR-0004 §2.6).
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
        if let Err(error) = policy.validate() {
            tracing::error!(
                target: "core::saga",
                event = "terminal_resolver_policy_invalid",
                saga_type = policy.saga_type.as_ref(),
                policy_id = policy.policy_id.as_ref(),
                error = %error,
                "terminal policy rejected before resolver registration"
            );
            return Err(format!(
                "invalid terminal policy saga_type={} policy_id={}: {error}",
                policy.saga_type, policy.policy_id
            ));
        }
        // One attach at a time: reserve -> setup -> commit is atomic for publishers.
        let _attach_guard = AttachLockGuard::acquire(&self.resolver_attach_lock);
        let saga_type_topic = policy.saga_type.clone();
        let requested = ResolverAttachConfig::new(&policy, journal.as_ref());
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
                Ok(TerminalResolverRegistryReply::RecoveryActivated(_)) => {
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
        if let Some((subscription, attached)) = existing {
            if attached == requested {
                // A previous attach may have failed after registering the
                // runtime but before readiness; make the retry commit it.
                self.register_terminal_policy(&policy)?;
                return Ok(subscription);
            }
            tracing::error!(
                target: "core::saga",
                event = "terminal_resolver_attach_config_mismatch",
                saga_type = policy.saga_type.as_ref(),
                policy_id = policy.policy_id.as_ref(),
                "resolver already attached with a different configuration"
            );
            return Err(format!(
                "AlreadyAttachedWithDifferentConfig: terminal resolver saga_type={} policy_id={} is already attached with a different policy or journal",
                policy.saga_type, policy.policy_id
            ));
        }

        let bus = self.clone();
        let responder: Arc<str> = Arc::from(responder);
        let shutdown = Arc::new(AtomicBool::new(false));
        let (resolver, recovery_events) = match &journal {
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
                TerminalResolver::restore_from_events(policy.clone(), &events)
            }
            None => (TerminalResolver::new(policy.clone()), Vec::new()),
        };
        let (resolver_ref, resolver_handle) = local_sync::spawn(TerminalResolverActor {
            resolver,
            recovery_events,
            // Nothing is restored without a journal: no activation gate needed.
            activated: journal.is_none(),
            journal,
            bus: bus.clone(),
            responder: Arc::clone(&responder),
            saga_type: saga_type_topic.clone(),
        });
        if let Err(error) =
            spawn_terminal_watchdog_if_needed(&policy, resolver_ref.clone(), Arc::clone(&shutdown))
        {
            shutdown.store(true, Ordering::Release);
            resolver_handle.shutdown();
            return Err(error);
        }
        let subscription_saga_type = policy.saga_type.clone();
        let subscription_resolver_ref = resolver_ref.clone();
        let abort_ingested = Arc::new(AtomicU64::new(0));
        let subscription_abort_ingested = Arc::clone(&abort_ingested);
        let subscription = self.subscribe_role_fn(
            saga_type_topic.as_ref(),
            SubscriberRole::Resolver,
            move |event| {
                if !subscription_resolver_ref
                    .tell(TerminalResolverTell::Ingest(Box::new(event.clone())))
                {
                    tracing::error!(
                        target: "core::saga",
                        event = "terminal_resolver_ingest_failed",
                        saga_type = subscription_saga_type.as_ref(),
                        run = ?event.context().run_key(),
                        "resolver subscription could not hand the event to the resolver actor"
                    );
                    return false;
                }
                if matches!(event, SagaChoreographyEvent::SagaAbortRequested { .. }) {
                    subscription_abort_ingested.fetch_add(1, Ordering::AcqRel);
                }
                true
            },
        );
        #[cfg(test)]
        {
            let hook = self
                .attach_hook
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(hook) = hook {
                hook();
            }
        }
        let rollback_shutdown = Arc::clone(&shutdown);
        let rollback_subscription = subscription.clone();
        let runtime = Box::new(TerminalResolverRuntime {
            config: requested,
            subscription,
            shutdown,
            resolver_ref,
            handle: resolver_handle,
        });
        let registered =
            match self
                .terminal_resolver_registry_ref
                .ask(TerminalResolverRegistryAsk::Register {
                    saga_type: saga_type_topic.clone(),
                    runtime,
                }) {
                Ok(TerminalResolverRegistryReply::Registered(subscription)) => subscription,
                other => {
                    // Commit failed: tear the half-built resolver down; no readiness published.
                    rollback_shutdown.store(true, Ordering::Release);
                    self.unsubscribe(rollback_subscription);
                    tracing::error!(
                        target: "core::saga",
                        event = "terminal_resolver_register_failed",
                        saga_type = policy.saga_type.as_ref(),
                        "terminal resolver registration failed; setup rolled back"
                    );
                    return Err(match other {
                        Err(err) => format!("terminal resolver registry unavailable: {err:?}"),
                        Ok(_) => "terminal resolver registry returned unexpected reply".to_string(),
                    });
                }
            };
        self.resolver_abort_ingest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(saga_type_topic, abort_ingested);
        // Readiness is committed only now that the resolver is fully registered.
        self.register_terminal_policy(&policy)?;
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

    /// `SagaId` wrapper: takes the newest cached incarnation's reply.
    pub fn take_terminal_reply(&self, saga_id: SagaId) -> Option<SagaReplyTo> {
        match self.ask_state(BusStateAsk::TakeNewestTerminalReply { saga_id }) {
            Some(BusStateReply::TerminalReply(reply)) => reply,
            _ => None,
        }
    }

    pub fn take_terminal_reply_for_run(&self, run: &RunKey) -> Option<SagaReplyTo> {
        match self.ask_state(BusStateAsk::TakeTerminalReply { run: run.clone() }) {
            Some(BusStateReply::TerminalReply(reply)) => reply,
            _ => None,
        }
    }

    /// `SagaId` wrapper: takes the newest cached incarnation's outcome.
    pub fn take_terminal_outcome(&self, saga_id: SagaId) -> Option<crate::SagaTerminalOutcome> {
        match self.ask_state(BusStateAsk::TakeNewestTerminalOutcome { saga_id }) {
            Some(BusStateReply::TerminalOutcome(outcome)) => outcome,
            _ => None,
        }
    }

    pub fn take_terminal_outcome_for_run(
        &self,
        run: &RunKey,
    ) -> Option<crate::SagaTerminalOutcome> {
        match self.ask_state(BusStateAsk::TakeTerminalOutcome { run: run.clone() }) {
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
        self.complete_terminal_reply_for_run(
            &event.context().run_key(),
            SagaReplyTo {
                responder: responder.into(),
                outcome,
            },
        )
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

    fn saga_start_required_recipients(&self, saga_type: &str) -> Option<HashSet<Box<str>>> {
        match self.ask_state(BusStateAsk::RequiredRecipients {
            saga_type: saga_type.into(),
            step_name: None,
        }) {
            Some(BusStateReply::RequiredRecipients(required)) => required,
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

    fn required_recipients_for_event(
        &self,
        event: &SagaChoreographyEvent,
    ) -> Option<HashSet<Box<str>>> {
        if matches!(
            event,
            SagaChoreographyEvent::SagaCompleted { .. }
                | SagaChoreographyEvent::SagaFailed { .. }
                | SagaChoreographyEvent::SagaQuarantined { .. }
                | SagaChoreographyEvent::SagaAbortRequested { .. }
        ) {
            return None;
        }

        let context = event.context();
        if !self.has_terminal_policy_for_saga_type(context.saga_type.as_ref()) {
            return None;
        }
        match self.ask_state(BusStateAsk::RequiredRecipients {
            saga_type: context.saga_type.clone(),
            step_name: Some(context.step_name.clone()),
        }) {
            Some(BusStateReply::RequiredRecipients(required)) => required,
            _ => None,
        }
    }

    fn store_terminal_reply(&self, run: RunKey, reply: SagaReplyTo) {
        let retention_limit = self.terminal_retention_limit();
        let _ = self.ask_state(BusStateAsk::StoreTerminalReply {
            run,
            reply,
            retention_limit,
        });
    }

    fn store_terminal_outcome(&self, run: RunKey, outcome: SagaTerminalOutcome) {
        let retention_limit = self.terminal_retention_limit();
        let _ = self.ask_state(BusStateAsk::StoreTerminalOutcome {
            run,
            outcome,
            retention_limit,
        });
    }
}

/// Cache key for `SagaId`-only completions: empty type, incarnation 0 (below any real run).
fn unscoped_run(saga_id: SagaId) -> RunKey {
    RunKey::new("", saga_id, RunIncarnation::new(0))
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
            legacy_pending_replies: self.legacy_pending_replies.clone(),
            state_ref: self.state_ref.clone(),
            terminal_resolver_registry_ref: self.terminal_resolver_registry_ref.clone(),
            resolver_attach_lock: Arc::clone(&self.resolver_attach_lock),
            resolver_abort_ingest: Arc::clone(&self.resolver_abort_ingest),
            #[cfg(test)]
            attach_hook: Arc::clone(&self.attach_hook),
            _lifecycle: Arc::clone(&self._lifecycle),
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
    use icanact_core::local_sync::SyncActor;

    use super::{TerminalResolverActor, TerminalResolverTell};
    use crate::TerminalResolver;
    use crate::{
        AcceptedStepTimeoutOutcome, FailureAuthority, InMemoryTerminalResolverJournal,
        SagaChoreographyEvent, SagaContext, SagaId, SagaReplyToResult, SagaTerminalOutcome,
        SagaWorkflowContract, SagaWorkflowStepContract, StepExecutionId, SuccessCriteria,
        TERMINAL_RESOLVER_STEP, TerminalPolicy, TerminalResolverJournal,
        TerminalResolverJournalEntry, TerminalResolverJournalError, WorkflowDependencySpec,
    };

    use super::{BusStateAsk, DEFAULT_TERMINAL_RETENTION_LIMIT, SagaChoreographyBus};
    use crate::AbortSource;
    use icanact_core::local::FirehoseSubscription;

    /// Process-constant recent incarnation: all events of one saga must share one run.
    fn run_start_millis() -> u64 {
        static BASE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
        *BASE.get_or_init(SagaContext::now_millis)
    }

    fn context_for(saga_type: &str, step_name: &str, saga_id: u64) -> SagaContext {
        let now = run_start_millis();
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
        RegisterRun {
            bus: SagaChoreographyBus,
            run: crate::RunKey,
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
            ProbeMsg::RegisterRun { bus, run, reply } => {
                let _ = bus.register_terminal_reply_for_run(&run, reply);
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

    fn register_pending_reply_for_run(
        bus: SagaChoreographyBus,
        run: crate::RunKey,
    ) -> (
        local_sync::PendingAsk<SagaReplyToResult>,
        local_sync::mpsc::ActorHandle,
    ) {
        let (probe_addr, probe_handle) = local_sync::mpsc::spawn(8, |msg: ProbeMsg| {
            if let ProbeMsg::RegisterRun { bus, run, reply } = msg {
                let _ = bus.register_terminal_reply_for_run(&run, reply);
            }
        });
        let pending = probe_addr
            .ask_delegated(|reply| ProbeMsg::RegisterRun { bus, run, reply })
            .expect("pending reply should be registered");
        (pending, probe_handle)
    }

    fn run_terminal_event(saga_id: u64, incarnation: u64, failed: bool) -> SagaChoreographyEvent {
        let context = SagaContext {
            saga_started_at_millis: incarnation,
            step_name: TERMINAL_RESOLVER_STEP.into(),
            ..context("create_order", saga_id)
        };
        if failed {
            SagaChoreographyEvent::SagaFailed {
                context,
                reason: "run failed".into(),
                failure: None,
            }
        } else {
            SagaChoreographyEvent::SagaCompleted { context }
        }
    }

    #[test]
    fn reply_for_old_run_not_delivered_to_new_run() {
        let bus = SagaChoreographyBus::new();
        let old_event = run_terminal_event(4242, 1_000, false);
        let new_event = run_terminal_event(4242, 2_000, true);
        let old_run = old_event.context().run_key();
        let new_run = new_event.context().run_key();
        let (pending_old, probe_old) = register_pending_reply_for_run(bus.clone(), old_run.clone());
        let (pending_new, probe_new) = register_pending_reply_for_run(bus.clone(), new_run.clone());
        thread::sleep(Duration::from_millis(20));

        // Old run terminates first: only its own waiter and cache entry may see it.
        assert!(bus.complete_terminal_reply_from_event(&old_event, "resolver"));
        let old_reply = pending_old
            .wait_timeout(Duration::from_secs(2))
            .expect("old run waiter resolves")
            .expect("old run reply ok");
        assert!(matches!(
            old_reply.outcome,
            SagaTerminalOutcome::Completed { .. }
        ));
        assert!(
            bus.take_terminal_outcome_for_run(&new_run).is_none(),
            "old run's terminal outcome must not be cached for the new run"
        );

        assert!(bus.complete_terminal_reply_from_event(&new_event, "resolver"));
        let new_reply = pending_new
            .wait_timeout(Duration::from_secs(2))
            .expect("new run waiter resolves")
            .expect("new run reply ok");
        assert!(
            matches!(new_reply.outcome, SagaTerminalOutcome::Failed { .. }),
            "new run waiter must get its own outcome, got {:?}",
            new_reply.outcome
        );
        probe_old.shutdown();
        probe_new.shutdown();
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

    struct UnreadableTerminalResolverJournal;

    impl TerminalResolverJournal for UnreadableTerminalResolverJournal {
        fn append(
            &self,
            _event: SagaChoreographyEvent,
        ) -> Result<u64, TerminalResolverJournalError> {
            Ok(0)
        }

        fn read_all(
            &self,
        ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
            Err(TerminalResolverJournalError::Storage(
                "injected resolver journal read failure".into(),
            ))
        }
    }

    #[test]
    fn resolver_attach_failure_and_race_leave_one_consistent_registration() {
        // (1) journal read failure must not leave policy-only readiness behind.
        let bus = SagaChoreographyBus::new();
        let failed = bus.attach_durable_terminal_resolver(
            TerminalPolicy::order_lifecycle_default(),
            "terminal-resolver",
            Arc::new(UnreadableTerminalResolverJournal),
        );
        assert!(failed.is_err(), "unreadable journal must fail the attach");
        assert!(
            !bus.has_terminal_policy_for_saga_type("order_lifecycle"),
            "failed attach must not leave a registered terminal policy"
        );
        // A later attach with a healthy journal then succeeds from a clean slate.
        let journal = Arc::new(InMemoryTerminalResolverJournal::default());
        let durable = bus
            .attach_durable_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
                Arc::clone(&journal),
            )
            .expect("retry after a failed attach must succeed");
        assert!(bus.has_terminal_policy_for_saga_type("order_lifecycle"));
        // Same configuration is idempotent.
        let again = bus
            .attach_durable_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
                Arc::clone(&journal),
            )
            .expect("same-config reattach is idempotent");
        assert_eq!(durable, again);
        // (3) incompatible reattach is an explicit error, never silently the old one.
        let mismatch = bus
            .attach_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
            )
            .expect_err("durable -> in-memory reattach must be rejected");
        assert!(
            mismatch.contains("AlreadyAttachedWithDifferentConfig"),
            "got: {mismatch}"
        );
        let other_journal = Arc::new(InMemoryTerminalResolverJournal::default());
        let mismatch = bus
            .attach_durable_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
                other_journal,
            )
            .expect_err("different journal must be rejected");
        assert!(
            mismatch.contains("AlreadyAttachedWithDifferentConfig"),
            "got: {mismatch}"
        );

        // In-memory first, then durable.
        let bus = SagaChoreographyBus::new();
        bus.attach_terminal_resolver(
            TerminalPolicy::order_lifecycle_default(),
            "terminal-resolver",
        )
        .expect("in-memory attach");
        let mismatch = bus
            .attach_durable_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
                Arc::new(InMemoryTerminalResolverJournal::default()),
            )
            .expect_err("in-memory -> durable reattach must be rejected");
        assert!(
            mismatch.contains("AlreadyAttachedWithDifferentConfig"),
            "got: {mismatch}"
        );

        // (2) racing incompatible attaches: exactly one wins, the other is rejected.
        let bus = Arc::new(SagaChoreographyBus::new());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let in_memory = {
            let (bus, barrier) = (Arc::clone(&bus), Arc::clone(&barrier));
            thread::spawn(move || {
                barrier.wait();
                bus.attach_terminal_resolver(
                    TerminalPolicy::order_lifecycle_default(),
                    "terminal-resolver",
                )
            })
        };
        let durable = {
            let (bus, barrier) = (Arc::clone(&bus), Arc::clone(&barrier));
            thread::spawn(move || {
                barrier.wait();
                bus.attach_durable_terminal_resolver(
                    TerminalPolicy::order_lifecycle_default(),
                    "terminal-resolver",
                    Arc::new(InMemoryTerminalResolverJournal::default()),
                )
            })
        };
        let results = [
            in_memory.join().expect("in-memory attach thread"),
            durable.join().expect("durable attach thread"),
        ];
        assert_eq!(
            results.iter().filter(|result| result.is_ok()).count(),
            1,
            "exactly one racing attach may win: {results:?}"
        );
        let loser = results
            .iter()
            .find_map(|result| result.as_ref().err())
            .expect("one attach must be rejected");
        assert!(
            loser.contains("AlreadyAttachedWithDifferentConfig"),
            "got: {loser}"
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

    /// Rejects only resolver-originated `SagaCompleted` (the terminal decision).
    struct RejectingDecisionJournal;

    impl TerminalResolverJournal for RejectingDecisionJournal {
        fn append(
            &self,
            event: SagaChoreographyEvent,
        ) -> Result<u64, TerminalResolverJournalError> {
            if matches!(event, SagaChoreographyEvent::SagaCompleted { .. })
                && event.context().step_name.as_ref() == TERMINAL_RESOLVER_STEP
            {
                return Err(TerminalResolverJournalError::Storage(
                    "injected decision append failure".into(),
                ));
            }
            Ok(0)
        }

        fn read_all(
            &self,
        ) -> Result<Vec<TerminalResolverJournalEntry>, TerminalResolverJournalError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn terminal_reply_follows_durable_decision() {
        let bus = SagaChoreographyBus::new();
        let _resolver = bus
            .attach_durable_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
                Arc::new(RejectingDecisionJournal),
            )
            .expect("durable resolver should attach");
        let saga_id = SagaId::new(812);
        let (pending, probe) = register_pending_reply(bus.clone(), saga_id);

        bus.publish_strict(SagaChoreographyEvent::StepCompleted {
            context: context("create_order", saga_id.get()),
            output: Vec::new(),
            saga_input: Vec::new(),
            compensation_available: false,
        })
        .expect("step completion should reach the resolver");

        let reply = pending
            .wait()
            .expect("reply must resolve")
            .expect("terminal reply");
        assert!(
            matches!(reply.outcome, SagaTerminalOutcome::Quarantined { .. }),
            "a decision whose journal append failed must not be reported as success: {:?}",
            reply.outcome
        );
        probe.shutdown();
    }

    #[test]
    fn durable_resolver_restart_preserves_complete_compensation_scope() {
        let journal = Arc::new(InMemoryTerminalResolverJournal::default());
        let policy = DurableMultiStepContract::terminal_policy();

        {
            let bus = SagaChoreographyBus::new();
            bus.register_workflow_contract_provider::<DurableMultiStepContract>()
                .expect("workflow contract registration should succeed");
            for step in ["first_effect", "second_effect", "finalize"] {
                bus.register_bound_workflow_step("durable_multi_step", step)
                    .expect("step binding should succeed");
            }
            // Live participant lanes so the strict start meets its required delivery.
            let _participants: Vec<_> = ["first_effect", "second_effect", "finalize"]
                .into_iter()
                .map(|step| {
                    bus.subscribe_participant_fn("durable_multi_step", &[step], |_event| true)
                })
                .collect();
            let _resolver = bus
                .attach_durable_terminal_resolver(
                    policy.clone(),
                    "terminal-resolver",
                    Arc::clone(&journal),
                )
                .expect("durable resolver should attach");
            let start = context_for("durable_multi_step", "first_effect", 812);
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
        let requests: Arc<std::sync::Mutex<Vec<Vec<Box<str>>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let _capture = bus.subscribe_saga_type_fn("durable_multi_step", {
            let requests = Arc::clone(&requests);
            move |event| {
                if let SagaChoreographyEvent::CompensationRequested {
                    steps_to_compensate,
                    ..
                } = event
                {
                    requests
                        .lock()
                        .expect("capture lock should remain available")
                        .push(steps_to_compensate.to_vec());
                }
                true
            }
        });
        let request_count = || {
            requests
                .lock()
                .expect("capture lock should remain available")
                .len()
        };
        bus.publish_strict(SagaChoreographyEvent::StepFailed {
            context: context_for("durable_multi_step", "second_effect", 812),
            participant_id: "second-participant".into(),
            error_code: Some("exchange_reject".into()),
            error: "authoritative exchange failure".into(),
            requires_compensation: true,
        })
        .expect("recovered failure should publish");

        // Serial frontier: the later effect is undone first, alone.
        wait_until(Instant::now() + Duration::from_secs(1), || {
            request_count() >= 1
        });
        assert_eq!(
            requests.lock().expect("capture lock")[0],
            vec![Box::<str>::from("second_effect")]
        );
        assert_eq!(request_count(), 1, "first_effect waits for the ack");

        // After the ack, the earlier effect follows: the restored scope is complete.
        bus.publish_strict(SagaChoreographyEvent::CompensationCompleted {
            context: context_for("durable_multi_step", "second_effect", 812),
        })
        .expect("compensation ack should publish");
        wait_until(Instant::now() + Duration::from_secs(1), || {
            request_count() >= 2
        });
        assert_eq!(
            requests.lock().expect("capture lock")[1],
            vec![Box::<str>::from("first_effect")]
        );
    }

    #[test]
    fn durable_recovery_output_waits_for_explicit_post_binding_activation() {
        let journal = Arc::new(InMemoryTerminalResolverJournal::default());
        let saga_id = SagaId::new(813);
        journal
            .append(SagaChoreographyEvent::SagaStarted {
                context: context("create_order", saga_id.get()),
                payload: Vec::new(),
            })
            .expect("saga start should be journaled");
        journal
            .append(SagaChoreographyEvent::StepCompleted {
                context: context("create_order", saga_id.get()),
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
    fn restored_deadlines_do_not_publish_before_recovery_activation() {
        let saga_id = SagaId::new(816);
        let started = SagaChoreographyEvent::SagaStarted {
            context: SagaContext {
                saga_started_at_millis: SagaContext::now_millis(),
                event_timestamp_millis: SagaContext::now_millis(),
                ..context("create_order", saga_id.get())
            },
            payload: Vec::new(),
        };
        let mut required: HashSet<Box<str>> = HashSet::new();
        required.insert("create_order".into());
        let policy = TerminalPolicy::new(
            "order_lifecycle".into(),
            "order_lifecycle/short".into(),
            FailureAuthority::AnyParticipant,
            SuccessCriteria::AllOf(required),
            Duration::from_millis(60),
            Duration::from_millis(60),
            &[],
        );
        // Near expiry but not yet expired at restore time.
        let (resolver, recovery_events) =
            TerminalResolver::restore_from_events(policy, std::slice::from_ref(&started));
        assert!(recovery_events.is_empty(), "{recovery_events:?}");

        let bus = SagaChoreographyBus::new();
        let delivered = Arc::new(AtomicUsize::new(0));
        let _binding = bus.subscribe_saga_type_fn("order_lifecycle", {
            let delivered = Arc::clone(&delivered);
            move |_| {
                delivered.fetch_add(1, Ordering::Relaxed);
                true
            }
        });
        let mut actor = TerminalResolverActor {
            resolver,
            recovery_events,
            activated: false,
            journal: None,
            bus: bus.clone(),
            responder: Arc::from("terminal-resolver"),
            saga_type: "order_lifecycle".into(),
        };
        // Let the deadline pass while recovery is still not activated.
        let expired_at = SagaContext::now_millis() + 80;
        while SagaContext::now_millis() <= expired_at {
            thread::yield_now();
        }

        actor.handle_tell(TerminalResolverTell::PollTimeouts);
        thread::sleep(Duration::from_millis(30));
        assert_eq!(
            delivered.load(Ordering::Relaxed),
            0,
            "watchdog must not publish before ActivateRecovery"
        );

        actor.handle_tell(TerminalResolverTell::ActivateRecovery);
        wait_until(Instant::now() + Duration::from_secs(1), || {
            delivered.load(Ordering::Relaxed) >= 1
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

    fn many_step_policy(capacity: usize, reverse: bool) -> TerminalPolicy {
        let names: Vec<String> = (0..24).map(|i| format!("step_{i}")).collect();
        let mut all_of: HashSet<Box<str>> = HashSet::with_capacity(capacity);
        let mut only: HashSet<Box<str>> = HashSet::with_capacity(capacity);
        let iter: Box<dyn Iterator<Item = &String>> = if reverse {
            Box::new(names.iter().rev())
        } else {
            Box::new(names.iter())
        };
        for name in iter {
            all_of.insert(name.as_str().into());
            only.insert(name.as_str().into());
        }
        TerminalPolicy::new(
            "order_lifecycle".into(),
            "order_lifecycle/many".into(),
            FailureAuthority::OnlySteps(only),
            SuccessCriteria::AllOf(all_of),
            Duration::from_secs(30),
            Duration::from_secs(30),
            &[],
        )
    }

    #[test]
    fn same_config_reattach_with_multi_step_policy_is_always_ok() {
        for round in 0..64 {
            let bus = SagaChoreographyBus::new();
            let first = bus
                .attach_terminal_resolver(many_step_policy(round, false), "terminal-resolver")
                .expect("first attach");
            let second = bus
                .attach_terminal_resolver(
                    many_step_policy(round * 7 + 1, true),
                    "terminal-resolver",
                )
                .expect("same-config reattach must not depend on HashSet iteration order");
            assert_eq!(first, second);
        }
    }

    fn abort_event(saga_id: u64) -> SagaChoreographyEvent {
        SagaChoreographyEvent::SagaAbortRequested {
            context: context_for("order_lifecycle", "create_order", saga_id),
            reason: "participant abort".into(),
            source: AbortSource::StaleRecovery,
        }
    }

    #[test]
    fn abort_with_no_subscribers_errors_and_falls_back_to_saga_failed() {
        let bus = SagaChoreographyBus::new();
        let result = bus.publish_strict(abort_event(8101));
        assert!(
            result.is_err(),
            "undelivered abort must not be Ok: {result:?}"
        );
        assert!(
            matches!(
                bus.take_terminal_outcome(SagaId::new(8101)),
                Some(SagaTerminalOutcome::Failed { .. })
            ),
            "undelivered abort must fall back to a terminal SagaFailed"
        );
    }

    #[test]
    fn abort_reaching_only_non_resolver_subscriber_errors_and_falls_back() {
        let bus = SagaChoreographyBus::new();
        let _sub = bus.subscribe_saga_type_fn("order_lifecycle", |_: &SagaChoreographyEvent| true);
        let result = bus.publish_strict(abort_event(8102));
        assert!(
            result.is_err(),
            "abort with no resolver must not be Ok: {result:?}"
        );
        assert!(
            matches!(
                bus.take_terminal_outcome(SagaId::new(8102)),
                Some(SagaTerminalOutcome::Failed { .. })
            ),
            "abort with no attached resolver must fall back to SagaFailed"
        );
    }

    #[test]
    fn abort_with_attached_resolver_is_ok() {
        let bus = SagaChoreographyBus::new();
        bus.attach_terminal_resolver(
            TerminalPolicy::order_lifecycle_default(),
            "terminal-resolver",
        )
        .expect("attach");
        let result = bus.publish_strict(abort_event(8103));
        assert!(
            result.is_ok(),
            "abort delivered to resolver must be Ok: {result:?}"
        );
    }

    #[test]
    fn attach_fails_when_readiness_cannot_be_registered() {
        let mut bus = SagaChoreographyBus::new();
        Arc::get_mut(&mut bus._lifecycle)
            .expect("sole lifecycle owner")
            .state_handle
            .take()
            .expect("state handle")
            .shutdown();
        wait_until(Instant::now() + Duration::from_millis(500), || {
            bus.state_ref
                .ask(BusStateAsk::HasTerminalPolicy {
                    saga_type: "x".into(),
                })
                .is_err()
        });
        let result = bus.attach_terminal_resolver(
            TerminalPolicy::order_lifecycle_default(),
            "terminal-resolver",
        );
        assert!(result.is_err(), "attach without readiness must be Err");
    }

    /// P1-1: an abort published between the resolver's subscribe and its
    /// registry registration must reach exactly one authority.
    #[test]
    fn abort_during_attach_window_has_exactly_one_authority() {
        use std::sync::mpsc;
        let bus = SagaChoreographyBus::new();
        // Threads that delivered a SagaFailed; delivery is synchronous on the
        // publishing thread, so the fallback shows up as the publisher's id.
        let failed_on = Arc::new(std::sync::Mutex::new(Vec::<thread::ThreadId>::new()));
        let _capture = bus.subscribe_saga_type_fn("order_lifecycle", {
            let failed_on = Arc::clone(&failed_on);
            move |event: &SagaChoreographyEvent| {
                if matches!(event, SagaChoreographyEvent::SagaFailed { .. }) {
                    failed_on
                        .lock()
                        .expect("failed_on")
                        .push(thread::current().id());
                }
                true
            }
        });
        let (subscribed_tx, subscribed_rx) = mpsc::channel::<()>();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let go_rx = std::sync::Mutex::new(go_rx);
        let subscribed_tx = std::sync::Mutex::new(subscribed_tx);
        *bus.attach_hook.lock().expect("hook") = Some(Arc::new(move || {
            let _ = subscribed_tx.lock().expect("tx").send(());
            let _ = go_rx.lock().expect("rx").recv();
        }));
        let attacher = {
            let bus = bus.clone();
            thread::spawn(move || {
                bus.attach_terminal_resolver(
                    TerminalPolicy::order_lifecycle_default(),
                    "terminal-resolver",
                )
                .map(|_| ())
            })
        };
        subscribed_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("attach reached subscribe/register window");
        let (done_tx, done_rx) = mpsc::channel();
        let publisher = {
            let bus = bus.clone();
            thread::spawn(move || {
                let result = bus.publish_strict(abort_event(8201));
                let _ = done_tx.send(());
                (result, thread::current().id())
            })
        };
        // Fixed code: the publisher is parked on the attach lock and cannot
        // finish until the attach is released. Buggy code finishes at once.
        let finished_inside_window = done_rx.recv_timeout(Duration::from_millis(500)).is_ok();
        go_tx.send(()).expect("release attach");
        attacher
            .join()
            .expect("attach thread")
            .expect("attach succeeds");
        let (result, publisher_id) = publisher.join().expect("publisher thread");
        assert!(
            !finished_inside_window,
            "abort published inside the attach window must wait for the attach"
        );
        assert!(result.is_ok(), "resolver is the authority: {result:?}");
        assert!(
            !failed_on.lock().expect("failed_on").contains(&publisher_id),
            "no fallback SagaFailed may be published by the abort publisher"
        );
    }

    /// P2-1: unsubscribing the resolver's subscription means it no longer
    /// receives aborts; the abort must not count as delivered.
    #[test]
    fn abort_after_resolver_unsubscribed_is_not_delivered() {
        let bus = SagaChoreographyBus::new();
        let resolver_sub = bus
            .attach_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
            )
            .expect("attach");
        let failed = Arc::new(AtomicUsize::new(0));
        let _participant = bus.subscribe_saga_type_fn("order_lifecycle", {
            let failed = Arc::clone(&failed);
            move |event: &SagaChoreographyEvent| {
                if matches!(event, SagaChoreographyEvent::SagaFailed { .. }) {
                    failed.fetch_add(1, Ordering::SeqCst);
                }
                true
            }
        });
        assert!(bus.unsubscribe(resolver_sub));
        let result = bus.publish_strict(abort_event(8202));
        assert!(
            matches!(
                result,
                Err(super::SagaBusPublishError::AbortNotDelivered { .. })
            ),
            "abort lost with the resolver unsubscribed must be AbortNotDelivered: {result:?}"
        );
        assert_eq!(failed.load(Ordering::SeqCst), 1, "fallback SagaFailed");
    }

    /// P2-6: a retried attach that finds the runtime already registered must
    /// still commit readiness.
    #[test]
    fn retried_attach_registers_missing_readiness() {
        let bus = SagaChoreographyBus::new();
        bus.attach_terminal_resolver(
            TerminalPolicy::order_lifecycle_default(),
            "terminal-resolver",
        )
        .expect("first attach");
        // Model a first attempt that registered the runtime but failed
        // readiness: same registry, readiness state that never saw it.
        let mut retry_bus = bus.clone();
        let (fresh_state, _fresh_handle) = local_sync::spawn(super::BusStateActor::default());
        retry_bus.state_ref = fresh_state;
        assert!(!retry_bus.has_terminal_policy_for_saga_type("order_lifecycle"));
        retry_bus
            .attach_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
            )
            .expect("retry attach");
        assert!(
            retry_bus.has_terminal_policy_for_saga_type("order_lifecycle"),
            "retry returned Ok so readiness must be present"
        );
    }

    /// Deadlock check: the resolver thread publishes aborts through the bus
    /// (`publish_strict` partial delivery); dropping the last user handle
    /// while that is possible must not hang.
    #[test]
    fn dropping_bus_with_resolver_does_not_hang() {
        use std::sync::mpsc;
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let bus = SagaChoreographyBus::new();
            bus.attach_terminal_resolver(
                TerminalPolicy::order_lifecycle_default(),
                "terminal-resolver",
            )
            .expect("attach");
            let _ = bus.publish_strict(abort_event(8203));
            drop(bus);
            let _ = tx.send(());
        });
        rx.recv_timeout(Duration::from_secs(10))
            .expect("bus drop with an attached resolver must not hang");
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
        bus.register_terminal_policy(&TerminalPolicy::order_lifecycle_default())
            .expect("register policy");
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
            TerminalPolicy::new(
                "order_lifecycle".into(),
                "order_lifecycle/any-of".into(),
                FailureAuthority::AnyParticipant,
                SuccessCriteria::AnyOf(possible_success_steps),
                Duration::from_secs(30),
                Duration::from_secs(5),
                Self::steps(),
            )
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
            TerminalPolicy::new(
                "different_saga_type".into(),
                "different_saga_type/default".into(),
                FailureAuthority::AnyParticipant,
                SuccessCriteria::AllOf(required_steps),
                Duration::from_secs(30),
                Duration::from_secs(30),
                Self::steps(),
            )
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
    fn saga_started_with_live_delivery_shortfall_fails_immediately() {
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

        // Bind only one live participant subscriber even though the contract
        // declares two workflow steps.
        let _single_participant = bus.subscribe_saga_type_fn("order_lifecycle", |_event| true);

        let saga_id = SagaId::new(90051);
        let started = SagaChoreographyEvent::SagaStarted {
            context: context("risk_check", saga_id.get()),
            payload: Vec::new(),
        };

        let _ = bus.publish(started);

        // The bus now requests an abort; the resolver authors the terminal.
        let outcome = take_outcome_eventually(&bus, saga_id);
        let Some(SagaTerminalOutcome::Failed { reason, .. }) = outcome else {
            panic!("expected terminal failure for live delivery shortfall");
        };
        assert!(
            reason.contains("required_path_delivery_shortfall"),
            "unexpected reason: {reason}"
        );
        assert!(
            reason.contains("required_min_delivered=2"),
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
    fn required_path_dependency_delivery_shortfall_fails_immediately() {
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
        let _risk_sub =
            bus.subscribe_participant_fn("order_lifecycle", &["risk_check"], |_event| true);
        let order_sub =
            bus.subscribe_participant_fn("order_lifecycle", &["create_order"], |_event| true);

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

        // The bus now requests an abort; the resolver authors the terminal.
        let outcome = take_outcome_eventually(&bus, saga_id);
        let Some(SagaTerminalOutcome::Failed { reason, .. }) = outcome else {
            panic!("expected terminal failure for required dependency route loss");
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
            required_min_delivered: 2,
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

    fn captured_events(
        bus: &SagaChoreographyBus,
    ) -> (
        Arc<std::sync::Mutex<Vec<SagaChoreographyEvent>>>,
        FirehoseSubscription,
    ) {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sub = bus.subscribe_saga_type_fn("order_lifecycle", {
            let seen = Arc::clone(&seen);
            move |event| {
                seen.lock().expect("capture lock").push(event.clone());
                true
            }
        });
        (seen, sub)
    }

    fn take_outcome_eventually(
        bus: &SagaChoreographyBus,
        saga_id: SagaId,
    ) -> Option<SagaTerminalOutcome> {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if let Some(outcome) = bus.take_terminal_outcome(saga_id) {
                return Some(outcome);
            }
            thread::yield_now();
        }
        None
    }

    fn shortfall_bus() -> SagaChoreographyBus {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<MultiStepOrderLifecycleContract>()
            .expect("workflow contract registration should succeed");
        bus.register_bound_workflow_step("order_lifecycle", "risk_check")
            .expect("risk_check binding should succeed");
        bus.register_bound_workflow_step("order_lifecycle", "create_order")
            .expect("create_order binding should succeed");
        bus
    }

    fn shortfall_event(saga_id: u64) -> SagaChoreographyEvent {
        SagaChoreographyEvent::StepCompleted {
            context: context("risk_check", saga_id),
            output: Vec::new(),
            saga_input: Vec::new(),
            compensation_available: false,
        }
    }

    #[test]
    fn delivery_shortfall_requests_abort_not_terminal_failure() {
        let bus = shortfall_bus();
        let _resolver = bus
            .attach_terminal_resolver_for_contract::<MultiStepOrderLifecycleContract>(
                "test-resolver",
            )
            .expect("terminal resolver should attach");
        let (seen, _sub) = captured_events(&bus);

        let _ = bus.publish_strict(shortfall_event(90071));

        let events = seen.lock().expect("capture lock");
        let aborts: Vec<_> = events
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    SagaChoreographyEvent::SagaAbortRequested {
                        source: AbortSource::DeliveryShortfall,
                        ..
                    }
                )
            })
            .collect();
        assert_eq!(aborts.len(), 1, "exactly one abort request, got {events:?}");
        // The bus itself must not synthesize a terminal failure; any
        // SagaFailed must come after the abort (resolver-authored).
        let abort_pos = events
            .iter()
            .position(|e| matches!(e, SagaChoreographyEvent::SagaAbortRequested { .. }))
            .expect("abort present");
        assert!(
            !events[..abort_pos]
                .iter()
                .any(|e| matches!(e, SagaChoreographyEvent::SagaFailed { .. })),
            "bus published SagaFailed before/instead of abort: {events:?}"
        );
    }

    #[test]
    fn undelivered_abort_falls_back_to_saga_failed() {
        let bus = shortfall_bus();
        let resolver = bus
            .attach_terminal_resolver_for_contract::<MultiStepOrderLifecycleContract>(
                "test-resolver",
            )
            .expect("terminal resolver should attach");
        // Policy stays registered but nothing is subscribed: the abort
        // request reaches no resolver and must fall back to SagaFailed.
        assert!(bus.unsubscribe(resolver));
        let _ = bus.publish_strict(shortfall_event(90072));
        assert!(matches!(
            bus.take_terminal_outcome(SagaId::new(90072)),
            Some(SagaTerminalOutcome::Failed { .. })
        ));
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
            TerminalPolicy::new(
                "order_lifecycle".into(),
                "order_lifecycle/denied-required".into(),
                FailureAuthority::DenySteps(denied),
                SuccessCriteria::AllOf(required_steps),
                Duration::from_secs(30),
                Duration::from_secs(30),
                Self::steps(),
            )
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
        let policy = TerminalPolicy::new(
            "order_lifecycle".into(),
            "watchdog/stall".into(),
            FailureAuthority::AnyParticipant,
            SuccessCriteria::AllOf(required_steps),
            Duration::from_secs(5),
            Duration::from_millis(120),
            MultiStepOrderLifecycleContract::steps(),
        );
        let _resolver_sub = bus
            .attach_terminal_resolver(policy, "terminal-resolver")
            .expect("terminal resolver should attach");
        let _risk_sub =
            bus.subscribe_participant_fn("order_lifecycle", &["risk_check"], |_event| true);
        let _order_sub =
            bus.subscribe_participant_fn("order_lifecycle", &["create_order"], |_event| true);
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
mod t14b_required_recipient_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::{SagaBusPublishError, SagaChoreographyBus, TerminalResolverRegistryAsk};
    use crate::{
        SagaChoreographyEvent, SagaContext, SagaId, SagaWorkflowContract, SagaWorkflowStepContract,
        TerminalPolicy, WorkflowDependencySpec,
    };

    const SAGA: &str = "order_lifecycle";

    struct TwoStep;

    impl SagaWorkflowContract for TwoStep {
        fn saga_type() -> &'static str {
            SAGA
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

    struct Rig {
        bus: SagaChoreographyBus,
        b_accepts: Arc<AtomicBool>,
        _subs: Vec<icanact_core::local::FirehoseSubscription>,
    }

    /// Required A (`risk_check`) and B (`create_order`), the resolver, and an
    /// observer that always accepts.
    fn rig() -> Rig {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<TwoStep>()
            .expect("contract registers");
        bus.register_bound_workflow_step(SAGA, "risk_check")
            .expect("bind A");
        bus.register_bound_workflow_step(SAGA, "create_order")
            .expect("bind B");
        let resolver = bus
            .attach_terminal_resolver_for_contract::<TwoStep>("t14b-resolver")
            .expect("resolver attaches");
        let b_accepts = Arc::new(AtomicBool::new(true));
        let b_flag = Arc::clone(&b_accepts);
        let subs = vec![
            resolver,
            bus.subscribe_participant_fn(SAGA, &["risk_check"], |_| true),
            bus.subscribe_participant_fn(SAGA, &["create_order"], move |_| {
                b_flag.load(Ordering::Acquire)
            }),
            bus.subscribe_saga_type_fn(SAGA, |_| true),
        ];
        Rig {
            bus,
            b_accepts,
            _subs: subs,
        }
    }

    fn step_completed(saga_id: u64) -> SagaChoreographyEvent {
        let now = SagaContext::now_millis();
        SagaChoreographyEvent::StepCompleted {
            context: SagaContext {
                saga_id: SagaId::new(saga_id),
                saga_type: SAGA.into(),
                step_name: "risk_check".into(),
                correlation_id: saga_id,
                causation_id: saga_id,
                trace_id: saga_id,
                step_index: 0,
                attempt: 0,
                initiator_peer_id: [0; 32],
                saga_started_at_millis: now,
                event_timestamp_millis: now,
            },
            output: Vec::new(),
            saga_input: Vec::new(),
            compensation_available: false,
        }
    }

    fn missing_roles(err: SagaBusPublishError) -> String {
        match err {
            SagaBusPublishError::RequiredPathDeliveryShortfall { missing_roles, .. } => {
                missing_roles.to_string()
            }
            other => panic!("expected RequiredPathDeliveryShortfall, got {other:?}"),
        }
    }

    #[test]
    fn all_required_roles_receiving_is_success() {
        let rig = rig();
        rig.bus
            .publish_strict(step_completed(14_001))
            .expect("A, B and the resolver all received the event");
    }

    #[test]
    fn observer_delivery_cannot_mask_missing_required_participant() {
        let rig = rig();
        rig.b_accepts.store(false, Ordering::Release);
        let err = rig
            .bus
            .publish_strict(step_completed(14_002))
            .expect_err("the observer must not stand in for the failed participant B");
        let missing = missing_roles(err);
        assert!(missing.contains("create_order"), "missing: {missing}");
        assert!(!missing.contains("risk_check"), "missing: {missing}");
    }

    #[test]
    fn observer_delivery_cannot_mask_closed_resolver() {
        let rig = rig();
        let _ = rig
            .bus
            .terminal_resolver_registry_ref
            .ask(TerminalResolverRegistryAsk::ShutdownAll);
        let err = rig
            .bus
            .publish_strict(step_completed(14_003))
            .expect_err("the observer must not stand in for the closed resolver");
        let missing = missing_roles(err);
        assert!(
            missing.contains(crate::TERMINAL_RESOLVER_STEP),
            "missing: {missing}"
        );
    }

    #[test]
    fn saga_started_does_not_require_the_start_step_to_receive_it() {
        let bus = SagaChoreographyBus::new();
        bus.register_workflow_contract_provider::<TwoStep>()
            .expect("contract registers");
        bus.register_bound_workflow_step(SAGA, "risk_check")
            .expect("bind A");
        bus.register_bound_workflow_step(SAGA, "create_order")
            .expect("bind B");
        let _resolver = bus
            .attach_terminal_resolver_for_contract::<TwoStep>("t14b-resolver")
            .expect("resolver attaches");
        // Only the non-start participant listens; the start step emitted the event.
        let _b = bus.subscribe_participant_fn(SAGA, &["create_order"], |_| true);
        let SagaChoreographyEvent::StepCompleted { context, .. } = step_completed(14_004) else {
            unreachable!("fixture is StepCompleted");
        };
        bus.publish_strict(SagaChoreographyEvent::SagaStarted {
            context,
            payload: Vec::new(),
        })
        .expect("start step is the emitter and must not count as a missing recipient");
    }
}
