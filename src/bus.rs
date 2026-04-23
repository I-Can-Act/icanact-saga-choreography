use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use icanact_core::CorrelationRegistry;
use icanact_core::local::{EventBus, EventSubscription, PublishStats};

use crate::reply_registry::{SagaReplyToHandle, SagaReplyToResult};
use crate::{
    HasSagaWorkflowParticipants, SagaChoreographyEvent, SagaId, SagaReplyTo, SagaTerminalOutcome,
    SagaWorkflowContract, TERMINAL_RESOLVER_STEP, TerminalPolicy, TerminalResolver,
    required_steps_from_success_criteria, validate_workflow_contract,
};

#[derive(Clone, Debug)]
struct WorkflowContractState {
    first_step: Box<str>,
    declared_steps: HashSet<Box<str>>,
}

pub struct SagaChoreographyBus {
    bus: EventBus<SagaChoreographyEvent>,
    pending_replies: CorrelationRegistry<SagaId, SagaReplyToResult>,
    terminal_replies: Arc<Mutex<HashMap<SagaId, SagaReplyTo>>>,
    terminal_outcomes: Arc<Mutex<HashMap<SagaId, SagaTerminalOutcome>>>,
    terminal_order: Arc<Mutex<VecDeque<SagaId>>>,
    terminal_policies_by_saga_type: Arc<Mutex<HashMap<Box<str>, Box<str>>>>,
    workflow_contracts_by_saga_type: Arc<Mutex<HashMap<Box<str>, WorkflowContractState>>>,
    bound_steps_by_saga_type: Arc<Mutex<HashMap<Box<str>, HashSet<Box<str>>>>>,
    owned: bool,
}

const DEFAULT_TERMINAL_RETENTION_LIMIT: usize = 1024;
const DEFAULT_TERMINAL_WATCHDOG_TICK_MS: u64 = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SagaBusPublishError {
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
}

impl SagaChoreographyBus {
    pub fn new() -> Self {
        Self {
            bus: EventBus::new(),
            pending_replies: CorrelationRegistry::new(),
            terminal_replies: Arc::new(Mutex::new(HashMap::new())),
            terminal_outcomes: Arc::new(Mutex::new(HashMap::new())),
            terminal_order: Arc::new(Mutex::new(VecDeque::new())),
            terminal_policies_by_saga_type: Arc::new(Mutex::new(HashMap::new())),
            workflow_contracts_by_saga_type: Arc::new(Mutex::new(HashMap::new())),
            bound_steps_by_saga_type: Arc::new(Mutex::new(HashMap::new())),
            owned: true,
        }
    }

    pub fn subscribe_fn<F>(&self, topic: &str, f: F) -> EventSubscription
    where
        F: Fn(&SagaChoreographyEvent) -> bool + Send + Sync + 'static,
    {
        self.bus.subscribe_fn(topic, f)
    }

    pub fn unsubscribe(&self, sub: EventSubscription) -> bool {
        self.bus.unsubscribe(sub)
    }

    pub fn publish(&self, event: SagaChoreographyEvent) -> PublishStats {
        let mut started_expected_min_delivery: Option<u32> = None;
        let mut started_context: Option<crate::SagaContext> = None;
        if let SagaChoreographyEvent::SagaStarted { context, .. } = &event {
            if !self.has_terminal_policy_for_saga_type(context.saga_type.as_ref()) {
                let terminal = SagaChoreographyEvent::SagaFailed {
                    context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
                    reason: format!(
                        "terminal policy is required before saga start; saga_type={} saga_id={}",
                        context.saga_type,
                        context.saga_id.get()
                    )
                    .into(),
                    failure: None,
                };
                if let Some(outcome) = terminal.terminal_outcome() {
                    self.store_terminal_outcome(terminal.context().saga_id, outcome);
                }
                return self.bus.publish(terminal);
            }
            if let Some(reason) = self.saga_start_contract_violation_reason(context) {
                let terminal = SagaChoreographyEvent::SagaFailed {
                    context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
                    reason: reason.into(),
                    failure: None,
                };
                if let Some(outcome) = terminal.terminal_outcome() {
                    self.store_terminal_outcome(terminal.context().saga_id, outcome);
                }
                return self.bus.publish(terminal);
            }
            started_expected_min_delivery =
                self.saga_start_expected_min_delivery(context.saga_type.as_ref());
            started_context = Some(context.clone());
        }
        if let Some(outcome) = event.terminal_outcome() {
            self.store_terminal_outcome(event.context().saga_id, outcome);
        }
        let stats = self.bus.publish(event);
        if let (Some(required_min_delivery), Some(context)) =
            (started_expected_min_delivery, started_context)
        {
            if stats.delivered < required_min_delivery {
                let terminal = SagaChoreographyEvent::SagaFailed {
                    context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
                    reason: format!(
                        "saga start delivery shortfall: saga_type={} delivered={} attempted={} required_min_delivered={} (declared_steps + terminal_resolver)",
                        context.saga_type,
                        stats.delivered,
                        stats.attempted,
                        required_min_delivery
                    )
                    .into(),
                    failure: None,
                };
                if let Some(outcome) = terminal.terminal_outcome() {
                    self.store_terminal_outcome(terminal.context().saga_id, outcome);
                }
                let _ = self.bus.publish(terminal);
            }
        }
        stats
    }

    pub fn publish_strict(
        &self,
        event: SagaChoreographyEvent,
    ) -> Result<PublishStats, SagaBusPublishError> {
        let stats = self.publish(event.clone());
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

        let terminal = SagaChoreographyEvent::SagaFailed {
            context: context.next_step(TERMINAL_RESOLVER_STEP.into()),
            reason: format!(
                "publish_partial_delivery attempted={} delivered={} event_type={} step={}",
                attempted,
                delivered,
                event.event_type(),
                context.step_name
            )
            .into(),
            failure: None,
        };
        let terminal_stats = self.publish(terminal);
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
        self.terminal_policies_by_saga_type
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(policy.saga_type.clone(), policy.policy_id.clone());
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
        let contract_state = WorkflowContractState {
            first_step: C::first_step().into(),
            declared_steps: declared_steps.clone(),
        };

        {
            let bound = self
                .bound_steps_by_saga_type
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(bound_for_type) = bound.get(C::saga_type()) {
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
        }

        {
            let mut contracts = self
                .workflow_contracts_by_saga_type
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            contracts.insert(C::saga_type().into(), contract_state);
        }

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
        {
            let contracts = self
                .workflow_contracts_by_saga_type
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(contract) = contracts.get(saga_type) {
                if !contract.declared_steps.contains(step_name) {
                    return Err(format!(
                        "bound workflow step is not declared by contract: saga_type={} step={}",
                        saga_type, step_name
                    ));
                }
            }
        }

        let mut bound = self
            .bound_steps_by_saga_type
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        bound
            .entry(saga_type.into())
            .or_default()
            .insert(step_name.into());
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
        self.bus.publish_to(saga_type, event)
    }

    pub fn subscribe_saga_type_fn<F>(&self, saga_type: &str, f: F) -> EventSubscription
    where
        F: Fn(&SagaChoreographyEvent) -> bool + Send + Sync + 'static,
    {
        self.subscribe_fn(saga_type, f)
    }

    pub fn register_terminal_reply(
        &self,
        saga_id: SagaId,
        reply: SagaReplyToHandle,
    ) -> Result<(), Box<str>> {
        match self.pending_replies.register(saga_id, reply) {
            Ok(()) => Ok(()),
            Err(err) => {
                let reply = err.into_reply();
                let _ = reply.reply(Err("terminal reply already registered for saga id".into()));
                Err("terminal reply already registered for saga id".into())
            }
        }
    }

    pub fn complete_terminal_reply(&self, saga_id: SagaId, reply: SagaReplyTo) -> bool {
        self.store_terminal_reply(saga_id, reply.clone());
        self.pending_replies.resolve(&saga_id, Ok(reply)).is_ok()
    }

    pub fn reject_terminal_reply(&self, saga_id: SagaId, reason: impl Into<String>) -> bool {
        self.pending_replies
            .resolve(&saga_id, Err(reason.into()))
            .is_ok()
    }

    pub fn attach_terminal_resolver(
        &self,
        policy: TerminalPolicy,
        responder: &'static str,
    ) -> Result<EventSubscription, String> {
        self.register_terminal_policy(&policy);
        let resolver = Arc::new(Mutex::new(TerminalResolver::new(policy.clone())));
        let bus = self.clone();
        let responder: Arc<str> = Arc::from(responder);
        let saga_type_topic = policy.saga_type.clone();
        spawn_terminal_watchdog_if_needed(
            &policy,
            Arc::clone(&resolver),
            bus.clone(),
            Arc::clone(&responder),
        )?;
        Ok(self
            .bus
            .subscribe_fn(saga_type_topic.as_ref(), move |event| {
                let terminal_events = {
                    let mut resolver = match resolver.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    resolver.ingest(event)
                };

                for terminal_event in terminal_events {
                    let _ =
                        bus.complete_terminal_reply_from_event(&terminal_event, responder.as_ref());
                    if let Err(err) = bus.publish_strict(terminal_event) {
                        tracing::error!(
                            target: "core::saga",
                            event = "terminal_resolver_publish_failed",
                            saga_type = policy.saga_type.as_ref(),
                            error = ?err
                        );
                    }
                }

                true
            }))
    }

    pub fn attach_terminal_resolver_for_contract<C: SagaWorkflowContract>(
        &self,
        responder: &'static str,
    ) -> Result<EventSubscription, String> {
        self.attach_terminal_resolver(C::terminal_policy(), responder)
    }

    pub fn take_terminal_reply(&self, saga_id: SagaId) -> Option<SagaReplyTo> {
        let reply = self
            .terminal_replies
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&saga_id);
        if reply.is_some() {
            self.terminal_outcomes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&saga_id);
        }
        reply
    }

    pub fn take_terminal_outcome(&self, saga_id: SagaId) -> Option<crate::SagaTerminalOutcome> {
        let reply = self.take_terminal_reply(saga_id).map(|reply| reply.outcome);
        let direct = self
            .terminal_outcomes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&saga_id);
        reply.or(direct)
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
        self.complete_terminal_reply(
            event.context().saga_id,
            SagaReplyTo {
                responder: responder.into(),
                outcome,
            },
        )
    }

    fn terminal_retention_limit(&self) -> usize {
        match std::env::var("SAGA_TERMINAL_RETENTION_LIMIT") {
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
        }
    }

    fn has_terminal_policy_for_saga_type(&self, saga_type: &str) -> bool {
        self.terminal_policies_by_saga_type
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains_key(saga_type)
    }

    fn saga_start_contract_violation_reason(&self, context: &crate::SagaContext) -> Option<String> {
        let saga_type = context.saga_type.as_ref();
        let contract = {
            let contracts = self
                .workflow_contracts_by_saga_type
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            contracts.get(saga_type).cloned()
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

        let bound_for_type = {
            let bound = self
                .bound_steps_by_saga_type
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match bound.get(saga_type) {
                Some(steps) => steps.clone(),
                None => HashSet::new(),
            }
        };
        let mut missing_steps: Vec<&str> = contract
            .declared_steps
            .iter()
            .filter(|step| !bound_for_type.contains(step.as_ref()))
            .map(|step| step.as_ref())
            .collect();
        missing_steps.sort_unstable();
        if missing_steps.is_empty() {
            return None;
        }
        Some(format!(
            "workflow contract violation: unbound participant steps at saga start; saga_type={} missing_steps={}",
            saga_type,
            missing_steps.join(",")
        ))
    }

    fn saga_start_expected_min_delivery(&self, saga_type: &str) -> Option<u32> {
        let contracts = self
            .workflow_contracts_by_saga_type
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let contract = contracts.get(saga_type)?;
        let declared_steps = contract.declared_steps.len();
        let min_delivery = match declared_steps.saturating_add(1).try_into() {
            Ok(value) => value,
            Err(_err) => u32::MAX,
        };
        Some(min_delivery)
    }

    fn store_terminal_reply(&self, saga_id: SagaId, reply: SagaReplyTo) {
        let outcome = reply.outcome.clone();
        self.terminal_replies
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(saga_id, reply);
        self.store_terminal_outcome(saga_id, outcome);
    }

    fn store_terminal_outcome(&self, saga_id: SagaId, outcome: SagaTerminalOutcome) {
        let inserted_new = {
            let mut terminal_outcomes = self
                .terminal_outcomes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            terminal_outcomes.insert(saga_id, outcome).is_none()
        };

        if inserted_new {
            let limit = self.terminal_retention_limit();
            let evicted_id = {
                let mut order = self
                    .terminal_order
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let mut evicted_id = None;
                order.push_back(saga_id);
                while order.len() > limit {
                    let Some(candidate) = order.pop_front() else {
                        break;
                    };
                    if candidate != saga_id {
                        evicted_id = Some(candidate);
                        break;
                    }
                }
                evicted_id
            };
            if let Some(evicted_id) = evicted_id {
                self.remove_terminal_state(evicted_id);
            }
        }
    }

    fn remove_terminal_state(&self, saga_id: SagaId) -> Option<SagaTerminalOutcome> {
        self.terminal_replies
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&saga_id);
        self.terminal_outcomes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&saga_id)
    }
}

fn terminal_watchdog_tick_interval() -> Duration {
    match std::env::var("SAGA_TERMINAL_WATCHDOG_TICK_MS") {
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
    }
}

fn spawn_terminal_watchdog_if_needed(
    policy: &TerminalPolicy,
    resolver: Arc<Mutex<TerminalResolver>>,
    bus: SagaChoreographyBus,
    responder: Arc<str>,
) -> Result<(), String> {
    let saga_type = policy.saga_type.clone();
    let watchdog_name = format!("saga-terminal-watchdog:{saga_type}");
    let spawn_result = thread::Builder::new().name(watchdog_name).spawn(move || {
        loop {
            thread::sleep(terminal_watchdog_tick_interval());
            let terminal_events = {
                let mut guard = match resolver.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                guard.poll_timeouts()
            };
            for terminal_event in terminal_events {
                let _ = bus.complete_terminal_reply_from_event(&terminal_event, responder.as_ref());
                if let Err(err) = bus.publish_strict(terminal_event) {
                    tracing::error!(
                        target: "core::saga",
                        event = "terminal_watchdog_publish_failed",
                        saga_type = saga_type.as_ref(),
                        error = ?err
                    );
                }
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
            terminal_replies: Arc::clone(&self.terminal_replies),
            terminal_outcomes: Arc::clone(&self.terminal_outcomes),
            terminal_order: Arc::clone(&self.terminal_order),
            terminal_policies_by_saga_type: Arc::clone(&self.terminal_policies_by_saga_type),
            workflow_contracts_by_saga_type: Arc::clone(&self.workflow_contracts_by_saga_type),
            bound_steps_by_saga_type: Arc::clone(&self.bound_steps_by_saga_type),
            owned: false,
        }
    }
}

impl Drop for SagaChoreographyBus {
    fn drop(&mut self) {
        if !self.owned {
            return;
        }

        for (_, reply) in self.pending_replies.drain() {
            let _ = reply.reply(Err("saga bus dropped".to_string()));
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
        FailureAuthority, SagaChoreographyEvent, SagaContext, SagaId, SagaReplyToResult,
        SagaTerminalOutcome, SagaWorkflowContract, SagaWorkflowStepContract, SuccessCriteria,
        TERMINAL_RESOLVER_STEP, TerminalPolicy, WorkflowDependencySpec,
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

        let Some(SagaTerminalOutcome::Failed { reason, .. }) = bus.take_terminal_outcome(saga_id)
        else {
            panic!("expected immediate terminal failure for live delivery shortfall");
        };
        assert!(
            reason.contains("saga start delivery shortfall"),
            "unexpected reason: {reason}"
        );
        assert!(
            reason.contains("required_min_delivered=3"),
            "expected minimum delivery requirement in reason, got: {reason}"
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
