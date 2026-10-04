use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use crate::{
    AcceptedStepTimeoutOutcome, SagaChoreographyEvent, SagaContext, SagaFailureDetails, SagaId,
    SagaWorkflowStepContract, StepExecutionId, WorkflowDependencySpec,
};

pub const TERMINAL_RESOLVER_STEP: &str = "terminal_resolver";

#[derive(Clone, Debug)]
pub enum FailureAuthority {
    AnyParticipant,
    OnlySteps(HashSet<Box<str>>),
    DenySteps(HashSet<Box<str>>),
}

impl FailureAuthority {
    pub(crate) fn is_authorized(&self, step_name: &str) -> bool {
        match self {
            Self::AnyParticipant => true,
            Self::OnlySteps(steps) => steps.contains(step_name),
            Self::DenySteps(steps) => !steps.contains(step_name),
        }
    }
}

#[derive(Clone, Debug)]
pub enum SuccessCriteria {
    AllOf(HashSet<Box<str>>),
    AnyOf(HashSet<Box<str>>),
    Quorum {
        group_steps: HashSet<Box<str>>,
        required_count: usize,
    },
}

impl SuccessCriteria {
    fn is_satisfied(&self, completed: &HashSet<Box<str>>) -> bool {
        match self {
            Self::AllOf(steps) => steps.iter().all(|step| completed.contains(step)),
            Self::AnyOf(steps) => steps.iter().any(|step| completed.contains(step)),
            Self::Quorum {
                group_steps,
                required_count,
            } => {
                let count = group_steps
                    .iter()
                    .filter(|step| completed.contains(*step))
                    .count();
                count >= *required_count
            }
        }
    }

    fn missing_required_steps(&self, completed: &HashSet<Box<str>>) -> Vec<Box<str>> {
        match self {
            Self::AllOf(steps) => steps
                .iter()
                .filter(|step| !completed.contains(*step))
                .cloned()
                .collect(),
            Self::AnyOf(steps) => {
                if steps.iter().any(|step| completed.contains(step)) {
                    Vec::new()
                } else {
                    steps.iter().cloned().collect()
                }
            }
            Self::Quorum {
                group_steps,
                required_count,
            } => {
                let count = group_steps
                    .iter()
                    .filter(|step| completed.contains(*step))
                    .count();
                if count >= *required_count {
                    Vec::new()
                } else {
                    group_steps
                        .iter()
                        .filter(|step| !completed.contains(*step))
                        .cloned()
                        .collect()
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct TerminalPolicy {
    pub saga_type: Box<str>,
    pub policy_id: Box<str>,
    pub failure_authority: FailureAuthority,
    pub success_criteria: SuccessCriteria,
    /// Hard wall-clock budget measured from saga start.
    pub overall_timeout: Duration,
    /// Progress watchdog budget measured since last observed progress event.
    /// This window resets on each non-terminal participant progress event.
    pub stalled_timeout: Duration,
    /// Declared workflow graph used to diagnose stalled required paths.
    pub workflow_steps: &'static [SagaWorkflowStepContract],
}

impl TerminalPolicy {
    pub fn new(
        saga_type: Box<str>,
        policy_id: Box<str>,
        failure_authority: FailureAuthority,
        success_criteria: SuccessCriteria,
        overall_timeout: Duration,
        stalled_timeout: Duration,
        workflow_steps: &'static [SagaWorkflowStepContract],
    ) -> Self {
        Self {
            saga_type,
            policy_id,
            failure_authority,
            success_criteria,
            overall_timeout,
            stalled_timeout,
            workflow_steps,
        }
    }
}

/// Reasons a [`TerminalPolicy`] can never resolve (or resolves vacuously).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalPolicyError {
    EmptyAllOf,
    EmptyAnyOf,
    EmptyQuorumGroup,
    ZeroQuorum,
    QuorumExceedsGroup,
    ZeroOverallTimeout,
    ZeroStalledTimeout,
}

impl std::fmt::Display for TerminalPolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::EmptyAllOf => "AllOf success criteria has no steps",
            Self::EmptyAnyOf => "AnyOf success criteria has no steps",
            Self::EmptyQuorumGroup => "Quorum success criteria has an empty group",
            Self::ZeroQuorum => "Quorum success criteria requires zero steps",
            Self::QuorumExceedsGroup => "Quorum required_count exceeds group size",
            Self::ZeroOverallTimeout => "overall_timeout is zero",
            Self::ZeroStalledTimeout => "stalled_timeout is zero",
        })
    }
}

impl std::error::Error for TerminalPolicyError {}

impl TerminalPolicy {
    /// Rejects policies that are vacuous or can never be satisfied.
    pub fn validate(&self) -> Result<(), TerminalPolicyError> {
        match &self.success_criteria {
            SuccessCriteria::AllOf(steps) if steps.is_empty() => {
                return Err(TerminalPolicyError::EmptyAllOf);
            }
            SuccessCriteria::AnyOf(steps) if steps.is_empty() => {
                return Err(TerminalPolicyError::EmptyAnyOf);
            }
            SuccessCriteria::Quorum { group_steps, .. } if group_steps.is_empty() => {
                return Err(TerminalPolicyError::EmptyQuorumGroup);
            }
            SuccessCriteria::Quorum {
                required_count: 0, ..
            } => {
                return Err(TerminalPolicyError::ZeroQuorum);
            }
            SuccessCriteria::Quorum {
                group_steps,
                required_count,
            } if *required_count > group_steps.len() => {
                return Err(TerminalPolicyError::QuorumExceedsGroup);
            }
            _ => {}
        }
        if self.overall_timeout.is_zero() {
            return Err(TerminalPolicyError::ZeroOverallTimeout);
        }
        if self.stalled_timeout.is_zero() {
            return Err(TerminalPolicyError::ZeroStalledTimeout);
        }
        Ok(())
    }
}

#[cfg(test)]
impl TerminalPolicy {
    pub fn order_lifecycle_default() -> Self {
        let mut required_steps: HashSet<Box<str>> = HashSet::new();
        required_steps.insert("create_order".into());
        Self::new(
            "order_lifecycle".into(),
            "order_lifecycle/default".into(),
            FailureAuthority::AnyParticipant,
            SuccessCriteria::AllOf(required_steps),
            Duration::from_secs(30),
            Duration::from_secs(30),
            &[],
        )
    }
}

#[derive(Clone, Debug)]
struct SagaResolutionState {
    started_steps: HashSet<Box<str>>,
    acked_steps: HashSet<Box<str>>,
    completed_steps: HashSet<Box<str>>,
    failed_steps: HashSet<Box<str>>,
    compensable_steps: Vec<Box<str>>,
    /// Ordered rollback obligations; `Some` once rollback owns the saga.
    rollback: Option<RollbackPlan>,
    completed_compensation_steps: HashSet<Box<str>>,
    /// First authoritative failure of record; later failures never replace it.
    pending_failure: Option<SagaFailureDetails>,
    /// Participants that reported acceptance, kept as quarantine evidence.
    accepted_participants: HashMap<Box<str>, Box<str>>,
    accepted_steps: HashMap<Box<str>, AcceptedStepResolverState>,
    accepted_compensations: HashMap<Box<str>, AcceptedCompensationResolverState>,
    /// Run identity: `saga_started_at_millis` of the run this state belongs to.
    run_started_at_millis: u64,
    /// Anchor of the hard overall budget; moves forward when a forward timeout
    /// starts rollback so the undo phase gets its own budget.
    overall_anchor_millis: u64,
    last_progress_at_millis: u64,
    last_context: SagaContext,
    terminal_latched: bool,
    /// Event type of the first terminal outcome latched for this saga.
    terminal_outcome: Option<&'static str>,
}

/// Rollback phase: reverse-ordered queue with a single outstanding undo.
#[derive(Clone, Debug)]
struct RollbackPlan {
    /// Undo obligations not yet requested, next-to-request first.
    queue: VecDeque<Box<str>>,
    /// Undo requests issued and awaiting authoritative resolution.
    requested: Vec<Box<str>>,
    reason: Box<str>,
    /// Set when a forward timeout started the rollback.
    timeout_reason: Option<Box<str>>,
}

impl RollbackPlan {
    fn owns(&self, step: &str) -> bool {
        self.queue.iter().any(|s| s.as_ref() == step)
            || self.requested.iter().any(|s| s.as_ref() == step)
    }
}

#[derive(Clone, Debug)]
struct AcceptedStepResolverState {
    participant_id: Box<str>,
    execution_id: StepExecutionId,
    deadline_at_millis: u64,
    hard_deadline_at_millis: u64,
    timeouts_enabled: bool,
    timeout_outcome: AcceptedStepTimeoutOutcome,
}

#[derive(Clone, Debug)]
struct AcceptedCompensationResolverState {
    participant_id: Box<str>,
    execution_id: StepExecutionId,
    deadline_at_millis: u64,
    hard_deadline_at_millis: u64,
}

impl SagaResolutionState {
    fn new(seed_context: &SagaContext, now_millis: u64) -> Self {
        let started_at_millis = now_millis.max(seed_context.saga_started_at_millis);
        let progress_at_millis = now_millis.max(started_at_millis);
        Self {
            completed_steps: HashSet::new(),
            started_steps: HashSet::new(),
            acked_steps: HashSet::new(),
            failed_steps: HashSet::new(),
            compensable_steps: Vec::new(),
            rollback: None,
            completed_compensation_steps: HashSet::new(),
            pending_failure: None,
            accepted_participants: HashMap::new(),
            accepted_steps: HashMap::new(),
            accepted_compensations: HashMap::new(),
            run_started_at_millis: seed_context.saga_started_at_millis,
            overall_anchor_millis: started_at_millis,
            last_progress_at_millis: progress_at_millis,
            last_context: seed_context.clone(),
            terminal_latched: false,
            terminal_outcome: None,
        }
    }
}

#[derive(Debug)]
pub struct TerminalResolver {
    policy: TerminalPolicy,
    states: HashMap<SagaId, SagaResolutionState>,
    terminal_latched_order: VecDeque<SagaId>,
    terminal_latch_retention: usize,
}

impl TerminalResolver {
    pub fn new(policy: TerminalPolicy) -> Self {
        Self {
            policy,
            states: HashMap::new(),
            terminal_latched_order: VecDeque::new(),
            terminal_latch_retention: terminal_latch_retention_limit(),
        }
    }

    pub fn policy(&self) -> &TerminalPolicy {
        &self.policy
    }

    pub fn ingest(&mut self, event: &SagaChoreographyEvent) -> Vec<SagaChoreographyEvent> {
        self.ingest_at(event, SagaContext::now_millis())
    }

    pub fn poll_timeouts(&mut self) -> Vec<SagaChoreographyEvent> {
        self.poll_timeouts_at(SagaContext::now_millis())
    }

    pub(crate) fn restore_from_events(
        policy: TerminalPolicy,
        events: &[SagaChoreographyEvent],
    ) -> (Self, Vec<SagaChoreographyEvent>) {
        let mut resolver = Self::new(policy);
        let mut unpublished = Vec::new();
        for event in events {
            if let Some(index) = unpublished
                .iter()
                .position(|candidate| resolver_output_matches(candidate, event))
            {
                unpublished.remove(index);
            }
            unpublished.extend(resolver.ingest_at(event, event.context().event_timestamp_millis));
        }
        unpublished.extend(resolver.poll_timeouts());
        (resolver, unpublished)
    }

    fn ingest_at(
        &mut self,
        event: &SagaChoreographyEvent,
        now_millis: u64,
    ) -> Vec<SagaChoreographyEvent> {
        if event.context().saga_type.as_ref() != self.policy.saga_type.as_ref() {
            return Vec::new();
        }

        let saga_id = event.context().saga_id;
        let run_started_at = event.context().saga_started_at_millis;
        if let Some(existing) = self.states.get(&saga_id) {
            if run_started_at != existing.run_started_at_millis
                && let SagaChoreographyEvent::SagaQuarantined {
                    reason,
                    step,
                    participant_id,
                    ..
                } = event
            {
                // Uncertainty in any retained run fences the whole saga id. It
                // cannot be treated as a stale ordinary acknowledgement while
                // a successor is active (or during legacy reconciliation).
                if existing.terminal_outcome == Some("saga_quarantined") {
                    return Vec::new();
                }
                let context = terminal_context(&existing.last_context);
                let state = self.states.get_mut(&saga_id).expect("existing saga state");
                state.terminal_latched = true;
                state.terminal_outcome = Some("saga_quarantined");
                self.latch_terminal(saga_id);
                return vec![SagaChoreographyEvent::SagaQuarantined {
                    context,
                    reason: format!("unresolved quarantine in run {run_started_at}: {reason}")
                        .into(),
                    step: step.clone(),
                    participant_id: participant_id.clone(),
                }];
            }
            if run_started_at < existing.run_started_at_millis {
                tracing::warn!(
                    event = "saga_stale_run_event_ignored",
                    saga_id = %saga_id,
                    event_type = event.event_type(),
                    "ignoring event from an older run of this saga id"
                );
                return Vec::new();
            }
            if run_started_at > existing.run_started_at_millis {
                if !existing.terminal_latched
                    || existing.terminal_outcome == Some("saga_quarantined")
                {
                    // Active or quarantined ownership cannot be reset by a newer
                    // timestamp. Durable fences cover terminal cache eviction.
                    tracing::warn!(
                        event = "saga_newer_run_event_ignored_unresolved",
                        saga_id = %saga_id,
                        event_type = event.event_type(),
                        "ignoring newer-run event while the earlier run is unresolved"
                    );
                    if existing.terminal_outcome == Some("saga_quarantined")
                        && matches!(event, SagaChoreographyEvent::SagaStarted { .. })
                    {
                        // The older quarantined owner stays unresolved; its newer,
                        // already-admitted successor is quarantined visibly rather
                        // than silently ignored (restart or admission race).
                        return vec![SagaChoreographyEvent::SagaQuarantined {
                            context: terminal_context(event.context()),
                            reason: format!(
                                "a newer run started behind unresolved quarantined run {}; reconciliation required",
                                existing.run_started_at_millis
                            )
                            .into(),
                            step: event.context().step_name.clone(),
                            participant_id: "terminal_resolver".into(),
                        }];
                    }
                    return Vec::new();
                }
                self.states.remove(&saga_id);
                self.terminal_latched_order.retain(|id| *id != saga_id);
            }
        }

        let mut out = Vec::new();
        let state = self
            .states
            .entry(saga_id)
            .or_insert_with(|| SagaResolutionState::new(event.context(), now_millis));

        if state.terminal_latched {
            // An accepted effect can materialise after its undo and ordinary
            // failure. Terminal absorption must not hide this new obligation.
            // Any ordinary failure (with or without a rollback plan, and also
            // when restored from compacted terminal-only history) can be
            // followed by an effect the resolver never saw.
            if (state.rollback.is_some() || state.terminal_outcome == Some("saga_failed"))
                && state.terminal_outcome != Some("saga_quarantined")
                && let SagaChoreographyEvent::StepCompleted {
                    context,
                    compensation_available: true,
                    ..
                } = event
                && !state.completed_steps.contains(&context.step_name)
            {
                state.completed_steps.insert(context.step_name.clone());
                state.terminal_outcome = Some("saga_quarantined");
                return vec![SagaChoreographyEvent::SagaQuarantined {
                    context: terminal_context(context),
                    reason: "a compensable effect materialised after rollback resolved".into(),
                    step: context.step_name.clone(),
                    participant_id: state
                        .accepted_participants
                        .get(&context.step_name)
                        .cloned()
                        .unwrap_or_else(|| "unknown".into()),
                }];
            }
            warn_if_contradictory_terminal(Some(state), event);
            return Vec::new();
        }

        state.last_context = event.context().clone();
        if is_progress_event(event) {
            state.last_progress_at_millis = now_millis;
        }

        match event {
            SagaChoreographyEvent::SagaStarted { .. } => {}
            SagaChoreographyEvent::StepStarted { context } => {
                state.started_steps.insert(context.step_name.clone());
            }
            SagaChoreographyEvent::StepAccepted {
                context,
                participant_id,
                execution_id,
                deadline_at_millis,
                hard_deadline_at_millis,
                timeouts_enabled,
                timeout_outcome,
                compensation_available,
            } => {
                let step_name = context.step_name.clone();
                state
                    .accepted_participants
                    .insert(step_name.clone(), participant_id.clone());
                if state.rollback_owns(&step_name)
                    || (state.rollback.is_some()
                        && state.completed_compensation_steps.contains(&step_name))
                {
                    return out;
                }
                state.started_steps.insert(step_name.clone());
                if *compensation_available
                    && !state
                        .compensable_steps
                        .iter()
                        .any(|candidate| candidate == &step_name)
                {
                    state.compensable_steps.push(step_name.clone());
                    if state.rollback.is_some() {
                        // Late owned effect: it joins the remaining rollback.
                        adopt_late_effect(state, &step_name, context, &mut out);
                        return out;
                    }
                }
                state.accepted_steps.insert(
                    step_name,
                    AcceptedStepResolverState {
                        participant_id: participant_id.clone(),
                        execution_id: execution_id.clone(),
                        deadline_at_millis: *deadline_at_millis,
                        hard_deadline_at_millis: *hard_deadline_at_millis,
                        timeouts_enabled: *timeouts_enabled,
                        timeout_outcome: timeout_outcome.clone(),
                    },
                );
            }
            SagaChoreographyEvent::StepAck { context, .. } => {
                state.started_steps.insert(context.step_name.clone());
                state.acked_steps.insert(context.step_name.clone());
            }
            SagaChoreographyEvent::StepCompleted {
                context,
                compensation_available,
                ..
            } => {
                let step_name = context.step_name.clone();
                if state.rollback_owns(&step_name) {
                    return out;
                }
                let first_completion = !state.completed_steps.contains(&step_name);
                state.accepted_steps.remove(step_name.as_ref());
                state.started_steps.insert(step_name.clone());
                state.completed_steps.insert(step_name.clone());

                if state.rollback.is_some() {
                    // Rollback is absorbing for forward success.
                    if !*compensation_available {
                        state
                            .compensable_steps
                            .retain(|candidate| candidate != &step_name);
                    } else if state.completed_compensation_steps.contains(&step_name) {
                        if first_completion {
                            // The effect materialised after its undo already ran.
                            out.push(SagaChoreographyEvent::SagaQuarantined {
                                context: terminal_context(context),
                                reason: format!(
                                    "step={step_name} completed with a compensable effect after its compensation already finished"
                                )
                                .into(),
                                step: step_name.clone(),
                                participant_id: state
                                    .accepted_participants
                                    .get(&step_name)
                                    .cloned()
                                    .unwrap_or_else(|| "unknown".into()),
                            });
                            state.terminal_latched = true;
                        }
                    } else if !state.compensable_steps.contains(&step_name) {
                        state.compensable_steps.push(step_name.clone());
                        adopt_late_effect(state, &step_name, context, &mut out);
                    }
                } else {
                    if *compensation_available
                        && !state
                            .compensable_steps
                            .iter()
                            .any(|step| step == &step_name)
                    {
                        state.compensable_steps.push(step_name);
                    } else if !*compensation_available {
                        state
                            .compensable_steps
                            .retain(|candidate| candidate != &step_name);
                    }
                    if self
                        .policy
                        .success_criteria
                        .is_satisfied(&state.completed_steps)
                    {
                        out.push(SagaChoreographyEvent::SagaCompleted {
                            context: terminal_context(context),
                        });
                        state.terminal_latched = true;
                    }
                }
            }
            SagaChoreographyEvent::StepFailed {
                context,
                participant_id,
                error_code,
                error,
                requires_compensation,
            } => {
                state.accepted_steps.remove(context.step_name.as_ref());
                state.started_steps.insert(context.step_name.clone());
                state.failed_steps.insert(context.step_name.clone());
                close_definitive_accepted_potential(state, context, *requires_compensation);
                if !self
                    .policy
                    .failure_authority
                    .is_authorized(context.step_name.as_ref())
                {
                    return out;
                }

                apply_step_failure(
                    state,
                    context,
                    participant_id.clone(),
                    error_code.clone(),
                    error.clone(),
                    *requires_compensation,
                    &mut out,
                );
            }
            SagaChoreographyEvent::CompensationAccepted {
                context,
                participant_id,
                execution_id,
                deadline_at_millis,
                hard_deadline_at_millis,
            } => {
                if !state
                    .completed_compensation_steps
                    .contains(&context.step_name)
                {
                    state.accepted_compensations.insert(
                        context.step_name.clone(),
                        AcceptedCompensationResolverState {
                            participant_id: participant_id.clone(),
                            execution_id: execution_id.clone(),
                            deadline_at_millis: *deadline_at_millis,
                            hard_deadline_at_millis: *hard_deadline_at_millis,
                        },
                    );
                }
            }
            SagaChoreographyEvent::CompensationCompleted { context } => {
                state
                    .accepted_compensations
                    .remove(context.step_name.as_ref());
                let newly_completed = state
                    .completed_compensation_steps
                    .insert(context.step_name.clone());
                if newly_completed && let Some(plan) = state.rollback.as_mut() {
                    plan.requested
                        .retain(|step| step.as_ref() != context.step_name.as_ref());
                    plan.queue
                        .retain(|step| step.as_ref() != context.step_name.as_ref());
                    advance_rollback(state, context, &mut out);
                }
            }
            SagaChoreographyEvent::CompensationFailed {
                context,
                participant_id,
                error,
                ..
            } => {
                // Even a definitive failed final undo leaves an unreversed effect;
                // it is not authoritative successful resolution of that obligation.
                out.push(SagaChoreographyEvent::SagaQuarantined {
                    context: terminal_context(context),
                    reason: error.clone(),
                    step: context.step_name.clone(),
                    participant_id: participant_id.clone(),
                });
                state.terminal_latched = true;
            }
            SagaChoreographyEvent::CompensationRequested {
                context,
                failure,
                reason,
                steps_to_compensate,
                ..
            } => {
                if state.pending_failure.is_none() {
                    state.pending_failure = Some(failure.clone());
                }
                if state.rollback.is_none() {
                    // Adopt an externally issued request; resolver-known effects
                    // that it does not list stay queued behind it.
                    let queue: VecDeque<Box<str>> = state
                        .compensable_steps
                        .iter()
                        .rev()
                        .filter(|step| {
                            !steps_to_compensate.contains(*step)
                                && !state.completed_compensation_steps.contains(*step)
                        })
                        .cloned()
                        .collect();
                    state.rollback = Some(RollbackPlan {
                        queue,
                        requested: Vec::new(),
                        reason: reason.clone(),
                        timeout_reason: None,
                    });
                }
                for step in steps_to_compensate {
                    state.accepted_steps.remove(step.as_ref());
                    if state.completed_compensation_steps.contains(step) {
                        continue;
                    }
                    if let Some(plan) = state.rollback.as_mut() {
                        plan.queue.retain(|queued| queued != step);
                        if !plan.requested.contains(step) {
                            plan.requested.push(step.clone());
                        }
                    }
                }
                advance_rollback(state, context, &mut out);
            }
            SagaChoreographyEvent::SagaCompleted { .. }
            | SagaChoreographyEvent::SagaFailed { .. }
            | SagaChoreographyEvent::SagaQuarantined { .. } => {
                // An authoritative terminal event is absorbing: latch before
                // any timeout evaluation can emit a competing outcome.
                state.terminal_latched = true;
                state.terminal_outcome = Some(event.event_type());
            }
            SagaChoreographyEvent::CompensationStarted { .. } => {}
        }

        if state.terminal_latched && state.terminal_outcome.is_none() {
            state.terminal_outcome = out
                .iter()
                .find(|e| is_terminal_event(e))
                .map(SagaChoreographyEvent::event_type);
        }

        if !state.terminal_latched {
            out.extend(timeout_events(&self.policy, state, now_millis));
            if state.terminal_latched && state.terminal_outcome.is_none() {
                state.terminal_outcome = out
                    .iter()
                    .find(|e| is_terminal_event(e))
                    .map(SagaChoreographyEvent::event_type);
            }
        }

        if state.terminal_latched {
            self.latch_terminal(saga_id);
        }

        out
    }

    fn poll_timeouts_at(&mut self, now_millis: u64) -> Vec<SagaChoreographyEvent> {
        let mut out = Vec::new();
        let mut newly_latched = Vec::new();
        for (saga_id, state) in self.states.iter_mut() {
            if state.terminal_latched {
                continue;
            }
            let before = out.len();
            out.extend(timeout_events(&self.policy, state, now_millis));
            if state.terminal_latched {
                state.terminal_outcome = out[before..]
                    .iter()
                    .find(|e| is_terminal_event(e))
                    .map(SagaChoreographyEvent::event_type);
                newly_latched.push(*saga_id);
            }
        }
        for saga_id in newly_latched {
            self.latch_terminal(saga_id);
        }
        out
    }

    /// Records a terminal saga in the bounded cache.
    ///
    /// Eviction only bounds resolver memory; it is not a durable replay fence.
    /// Durable terminal fencing is the bus/participant responsibility.
    fn latch_terminal(&mut self, saga_id: SagaId) {
        if self.terminal_latched_order.contains(&saga_id) {
            return;
        }

        self.terminal_latched_order.push_back(saga_id);
        while self.terminal_latched_order.len() > self.terminal_latch_retention {
            let Some(evicted) = self.terminal_latched_order.pop_front() else {
                break;
            };
            self.states.remove(&evicted);
        }
    }
}

impl SagaResolutionState {
    fn rollback_owns(&self, step: &str) -> bool {
        self.rollback.as_ref().is_some_and(|plan| plan.owns(step))
    }
}

/// Queues a late owned effect at the front of the remaining rollback (it
/// completed last, so it is undone first) and issues it if nothing is in flight.
fn adopt_late_effect(
    state: &mut SagaResolutionState,
    step: &str,
    context: &SagaContext,
    out: &mut Vec<SagaChoreographyEvent>,
) {
    state.accepted_steps.remove(step);
    if let Some(plan) = state.rollback.as_mut()
        && !plan.owns(step)
    {
        plan.queue.push_front(step.into());
    }
    advance_rollback(state, context, out);
}

/// Issues the next singleton undo request, or resolves the saga when the plan
/// is exhausted. Never issues a request while one is still outstanding.
fn advance_rollback(
    state: &mut SagaResolutionState,
    context: &SagaContext,
    out: &mut Vec<SagaChoreographyEvent>,
) {
    let Some(failure) = state.pending_failure.clone() else {
        return;
    };
    let Some(plan) = state.rollback.as_mut() else {
        return;
    };
    if !plan.requested.is_empty() {
        return;
    }
    if let Some(step) = plan.queue.pop_front() {
        plan.requested.push(step.clone());
        out.push(SagaChoreographyEvent::CompensationRequested {
            context: terminal_context(context),
            failed_step: failure.step_name.clone(),
            reason: plan.reason.clone(),
            failure,
            steps_to_compensate: vec![step],
        });
        return;
    }
    let uncertain = uncertain_steps(state);
    let Some(plan) = state.rollback.as_ref() else {
        return;
    };
    if !uncertain.is_empty() {
        // Known effects are undone, but unresolved forward work may still
        // hold an effect: never resolve this as an ordinary failure.
        let prefix = plan.reason.clone();
        quarantine_uncertain(state, terminal_context(context), &uncertain, &prefix, out);
        return;
    }
    let (reason, failure) = match &plan.timeout_reason {
        Some(reason) => (reason.clone(), None),
        None => (
            format!(
                "compensation finished after failure at step={}",
                failure.step_name
            )
            .into(),
            Some(failure),
        ),
    };
    out.push(SagaChoreographyEvent::SagaFailed {
        context: terminal_context(context),
        reason,
        failure,
    });
    state.terminal_latched = true;
}

/// Starts the rollback phase for `failure` over every known compensable effect.
fn begin_rollback(
    state: &mut SagaResolutionState,
    failure: SagaFailureDetails,
    reason: Box<str>,
    timeout_reason: Option<Box<str>>,
) {
    let queue: VecDeque<Box<str>> = state
        .compensable_steps
        .iter()
        .rev()
        .filter(|step| !state.completed_compensation_steps.contains(*step))
        .cloned()
        .collect();
    for step in &queue {
        state.accepted_steps.remove(step.as_ref());
    }
    if state.pending_failure.is_none() {
        state.pending_failure = Some(failure);
    }
    state.rollback = Some(RollbackPlan {
        queue,
        requested: Vec::new(),
        reason,
        timeout_reason,
    });
}

fn is_terminal_event(event: &SagaChoreographyEvent) -> bool {
    matches!(
        event,
        SagaChoreographyEvent::SagaCompleted { .. }
            | SagaChoreographyEvent::SagaFailed { .. }
            | SagaChoreographyEvent::SagaQuarantined { .. }
    )
}

fn warn_if_contradictory_terminal(
    state: Option<&SagaResolutionState>,
    event: &SagaChoreographyEvent,
) {
    if !is_terminal_event(event) {
        return;
    }
    let Some(latched) = state.and_then(|state| state.terminal_outcome) else {
        return;
    };
    if latched != event.event_type() {
        tracing::warn!(
            event = "saga_contradictory_terminal_ignored",
            saga_id = %event.context().saga_id,
            latched,
            incoming = event.event_type(),
            "ignoring contradictory terminal event after terminal latch"
        );
    }
}

fn resolver_output_matches(
    expected: &SagaChoreographyEvent,
    observed: &SagaChoreographyEvent,
) -> bool {
    if expected == observed {
        return true;
    }
    match (expected, observed) {
        (
            SagaChoreographyEvent::SagaCompleted { context: left },
            SagaChoreographyEvent::SagaCompleted { context: right },
        ) => left.saga_id == right.saga_id,
        (
            SagaChoreographyEvent::SagaFailed {
                context: left_context,
                reason: left_reason,
                failure: left_failure,
            },
            SagaChoreographyEvent::SagaFailed {
                context: right_context,
                reason: right_reason,
                failure: right_failure,
            },
        ) => {
            left_context.saga_id == right_context.saga_id
                && left_reason == right_reason
                && left_failure == right_failure
        }
        (
            SagaChoreographyEvent::SagaQuarantined {
                context: left_context,
                reason: left_reason,
                step: left_step,
                participant_id: left_participant,
            },
            SagaChoreographyEvent::SagaQuarantined {
                context: right_context,
                reason: right_reason,
                step: right_step,
                participant_id: right_participant,
            },
        ) => {
            left_context.saga_id == right_context.saga_id
                && left_reason == right_reason
                && left_step == right_step
                && left_participant == right_participant
        }
        (
            SagaChoreographyEvent::CompensationRequested {
                context: left_context,
                failed_step: left_failed_step,
                reason: left_reason,
                failure: left_failure,
                steps_to_compensate: left_steps,
            },
            SagaChoreographyEvent::CompensationRequested {
                context: right_context,
                failed_step: right_failed_step,
                reason: right_reason,
                failure: right_failure,
                steps_to_compensate: right_steps,
            },
        ) => {
            left_context.saga_id == right_context.saga_id
                && left_failed_step == right_failed_step
                && left_reason == right_reason
                && left_failure == right_failure
                && left_steps == right_steps
        }
        (
            SagaChoreographyEvent::StepFailed {
                context: left_context,
                participant_id: left_participant,
                error_code: left_code,
                error: left_error,
                requires_compensation: left_requires,
            },
            SagaChoreographyEvent::StepFailed {
                context: right_context,
                participant_id: right_participant,
                error_code: right_code,
                error: right_error,
                requires_compensation: right_requires,
            },
        ) => {
            left_context.saga_id == right_context.saga_id
                && left_context.step_name == right_context.step_name
                && left_participant == right_participant
                && left_code == right_code
                && left_error == right_error
                && left_requires == right_requires
        }
        _ => false,
    }
}

fn terminal_latch_retention_limit() -> usize {
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| match std::env::var("SAGA_TERMINAL_LATCH_RETENTION") {
        Ok(raw) => match raw.parse::<usize>() {
            Ok(parsed) if parsed > 0 => parsed,
            _ => 4096,
        },
        Err(_) => 4096,
    })
}

fn terminal_context(context: &SagaContext) -> SagaContext {
    context.next_step(TERMINAL_RESOLVER_STEP.into())
}

fn terminal_context_at(context: &SagaContext, now_millis: u64) -> SagaContext {
    let mut next = terminal_context(context);
    next.event_timestamp_millis = now_millis;
    next
}

fn is_progress_event(event: &SagaChoreographyEvent) -> bool {
    matches!(
        event,
        SagaChoreographyEvent::SagaStarted { .. }
            | SagaChoreographyEvent::StepStarted { .. }
            | SagaChoreographyEvent::StepAccepted { .. }
            | SagaChoreographyEvent::StepAck { .. }
            | SagaChoreographyEvent::StepCompleted { .. }
            | SagaChoreographyEvent::StepFailed { .. }
            | SagaChoreographyEvent::CompensationRequested { .. }
            | SagaChoreographyEvent::CompensationStarted { .. }
            | SagaChoreographyEvent::CompensationAccepted { .. }
            | SagaChoreographyEvent::CompensationCompleted { .. }
            | SagaChoreographyEvent::CompensationFailed { .. }
    )
}

fn timeout_events(
    policy: &TerminalPolicy,
    state: &mut SagaResolutionState,
    now_millis: u64,
) -> Vec<SagaChoreographyEvent> {
    if let Some(event) = accepted_compensation_timeout_event(state, now_millis) {
        state.terminal_latched = true;
        return vec![event];
    }
    if let Some(events) = accepted_step_timeout_events(policy, state, now_millis) {
        return events;
    }
    if !state.accepted_compensations.is_empty() {
        if let Some(event) = accepted_compensation_overall_timeout_event(policy, state, now_millis)
        {
            state.terminal_latched = true;
            return vec![event];
        }
        return Vec::new();
    }
    resolve_generic_timeout(policy, state, now_millis)
}

fn accepted_compensation_overall_timeout_event(
    policy: &TerminalPolicy,
    state: &SagaResolutionState,
    now_millis: u64,
) -> Option<SagaChoreographyEvent> {
    let overall_timeout_ms = policy.overall_timeout.as_millis() as u64;
    if now_millis.saturating_sub(state.overall_anchor_millis) <= overall_timeout_ms {
        return None;
    }
    let (step_name, accepted) = state
        .accepted_compensations
        .iter()
        .min_by(|(left, _), (right, _)| left.cmp(right))?;
    Some(SagaChoreographyEvent::SagaQuarantined {
        context: terminal_context_at(&state.last_context, now_millis),
        reason: format!(
            "overall_timeout after {overall_timeout_ms}ms with unresolved accepted compensation: step={} execution_id={} policy={}",
            step_name, accepted.execution_id, policy.policy_id
        )
        .into(),
        step: step_name.clone(),
        participant_id: accepted.participant_id.clone(),
    })
}

fn accepted_compensation_timeout_event(
    state: &mut SagaResolutionState,
    now_millis: u64,
) -> Option<SagaChoreographyEvent> {
    let expired = state
        .accepted_compensations
        .iter()
        .find_map(|(step_name, accepted)| {
            if now_millis > accepted.hard_deadline_at_millis {
                Some((step_name.clone(), accepted.clone(), "hard"))
            } else if now_millis > accepted.deadline_at_millis {
                Some((step_name.clone(), accepted.clone(), "idle"))
            } else {
                None
            }
        })?;
    let (step_name, accepted, timeout_kind) = expired;
    state.accepted_compensations.remove(step_name.as_ref());
    let mut context = state.last_context.next_step(step_name.clone());
    context.event_timestamp_millis = now_millis;
    Some(SagaChoreographyEvent::SagaQuarantined {
        context: terminal_context_at(&context, now_millis),
        reason: format!(
            "accepted compensation {timeout_kind} timeout: step={} execution_id={} deadline_at_millis={} hard_deadline_at_millis={}",
            step_name,
            accepted.execution_id,
            accepted.deadline_at_millis,
            accepted.hard_deadline_at_millis
        )
        .into(),
        step: step_name,
        participant_id: accepted.participant_id,
    })
}

fn accepted_step_timeout_events(
    policy: &TerminalPolicy,
    state: &mut SagaResolutionState,
    now_millis: u64,
) -> Option<Vec<SagaChoreographyEvent>> {
    let expired: Vec<_> = state
        .accepted_steps
        .iter()
        .filter_map(|(step_name, accepted)| {
            if !accepted.timeouts_enabled {
                None
            } else if now_millis > accepted.hard_deadline_at_millis {
                Some((step_name.clone(), accepted.clone(), true))
            } else if now_millis > accepted.deadline_at_millis {
                Some((step_name.clone(), accepted.clone(), false))
            } else {
                None
            }
        })
        .collect();
    if expired.is_empty() {
        return None;
    }

    let mut timeout_events = Vec::new();
    for (step_name, accepted, hard_timeout) in expired {
        state.accepted_steps.remove(step_name.as_ref());

        let timeout_kind = if hard_timeout { "hard" } else { "idle" };
        let mut context = state.last_context.next_step(step_name.clone());
        context.event_timestamp_millis = now_millis;
        let reason: Box<str> = format!(
        "accepted step {timeout_kind} timeout: step={} execution_id={} deadline_at_millis={} hard_deadline_at_millis={}",
        step_name,
        accepted.execution_id,
        accepted.deadline_at_millis,
        accepted.hard_deadline_at_millis
    )
    .into();

        match accepted.timeout_outcome {
            AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation,
            } => {
                state.started_steps.insert(step_name.clone());
                state.failed_steps.insert(step_name.clone());
                close_definitive_accepted_potential(state, &context, requires_compensation);
                // Publish the authoritative per-step disposition first. Participants
                // must durably close matching accepted work before a later ordinary
                // failure reaches their terminal handling; SagaFailed alone cannot
                // prove a remote execution ended safely.
                timeout_events.push(SagaChoreographyEvent::StepFailed {
                    context: context.clone(),
                    participant_id: accepted.participant_id.clone(),
                    error_code: Some(timeout_kind.into()),
                    error: reason.clone(),
                    requires_compensation,
                });
                if !policy.failure_authority.is_authorized(step_name.as_ref()) {
                    continue;
                }
                apply_step_failure(
                    state,
                    &context,
                    accepted.participant_id,
                    Some(timeout_kind.into()),
                    reason,
                    requires_compensation,
                    &mut timeout_events,
                );
                if state.terminal_latched {
                    break;
                }
            }
            AcceptedStepTimeoutOutcome::QuarantineSaga => {
                state.terminal_latched = true;
                return Some(vec![SagaChoreographyEvent::SagaQuarantined {
                    context: terminal_context_at(&context, now_millis),
                    reason,
                    step: context.step_name.clone(),
                    participant_id: accepted.participant_id,
                }]);
            }
        }
    }
    Some(timeout_events)
}

/// A step's authoritative safe disposition is independent of whether that step
/// is authorized to fail the entire saga. It closes only potential accepted work.
fn close_definitive_accepted_potential(
    state: &mut SagaResolutionState,
    context: &SagaContext,
    requires_compensation: bool,
) {
    if requires_compensation
        || !state.accepted_participants.contains_key(&context.step_name)
        || state.completed_steps.contains(&context.step_name)
    {
        return;
    }
    let step = context.step_name.as_ref();
    match state.rollback.as_mut() {
        // Requested (or accepted) undo stays owned until acknowledged.
        Some(plan) if plan.requested.iter().any(|s| s.as_ref() == step) => return,
        // Queued but never requested: only a potential obligation, drop it.
        Some(plan) => plan.queue.retain(|s| s.as_ref() != step),
        None => {}
    }
    state
        .compensable_steps
        .retain(|candidate| candidate.as_ref() != step);
}

fn apply_step_failure(
    state: &mut SagaResolutionState,
    context: &SagaContext,
    participant_id: Box<str>,
    error_code: Option<Box<str>>,
    error: Box<str>,
    requires_compensation: bool,
    out: &mut Vec<SagaChoreographyEvent>,
) {
    let failure = SagaFailureDetails {
        step_name: context.step_name.clone(),
        participant_id,
        error_code,
        error_message: error.clone(),
        at_millis: context.event_timestamp_millis,
    };

    // Acceptance advertises *potential* undo metadata, not proof of an effect.
    // An authoritative safe/no-undo failure closes that potential obligation only
    // when no result or already-owned rollback proves something remains owed.
    // In particular it must not erase a completed effect or a prior undo request.
    close_definitive_accepted_potential(state, context, requires_compensation);

    // A non-compensating failure still cannot hide effects the resolver knows
    // about, so any known effect starts the ordinary rollback.
    if state.rollback.is_none() {
        if requires_compensation || has_pending_effects(state) {
            begin_rollback(state, failure.clone(), error.clone(), None);
            let nothing_to_undo = state
                .rollback
                .as_ref()
                .is_some_and(|plan| plan.queue.is_empty());
            if nothing_to_undo {
                resolve_without_undo(
                    state,
                    context,
                    "step failed and no compensations were pending".into(),
                    failure,
                    out,
                );
            } else {
                advance_rollback(state, context, out);
            }
        } else {
            resolve_without_undo(state, context, error, failure, out);
        }
    }
    // Otherwise rollback already owns the saga: the first failure stays the
    // failure of record, obligations are not re-planned, and a non-compensating
    // failure cannot bypass the undo obligations.
}

fn has_pending_effects(state: &SagaResolutionState) -> bool {
    state
        .compensable_steps
        .iter()
        .any(|step| !state.completed_compensation_steps.contains(step))
}

/// Forward work whose outcome is unknown: started, accepted or acked but
/// neither completed, failed nor undone. It may still materialise an effect, so no
/// ordinary terminal may be issued while any remains. Sorted for determinism.
fn uncertain_steps(state: &SagaResolutionState) -> Vec<Box<str>> {
    let resolved_failure = state
        .pending_failure
        .as_ref()
        .map(|failure| failure.step_name.as_ref());
    let mut steps: Vec<Box<str>> = state
        .started_steps
        .iter()
        .chain(state.accepted_steps.keys())
        .filter(|step| {
            !state.completed_steps.contains(*step)
                && !state.failed_steps.contains(*step)
                // An undone accepted effect is resolved; a later completion
                // is escalated by the terminal-absorption path.
                && !state.completed_compensation_steps.contains(*step)
                && Some(step.as_ref()) != resolved_failure
                && step.as_ref() != TERMINAL_RESOLVER_STEP
        })
        .cloned()
        .collect();
    steps.sort_unstable();
    steps.dedup();
    steps
}

/// Quarantine for unresolved forward work, keeping the evidence in the event.
fn quarantine_uncertain(
    state: &mut SagaResolutionState,
    context: SagaContext,
    uncertain: &[Box<str>],
    prefix: &str,
    out: &mut Vec<SagaChoreographyEvent>,
) {
    let step = uncertain
        .first()
        .cloned()
        .unwrap_or_else(|| TERMINAL_RESOLVER_STEP.into());
    let participant_id = state
        .accepted_participants
        .get(&step)
        .cloned()
        .unwrap_or_else(|| "unknown".into());
    out.push(SagaChoreographyEvent::SagaQuarantined {
        context,
        reason: format!(
            "{prefix} with unresolved forward work: steps={}",
            uncertain.join(",")
        )
        .into(),
        step,
        participant_id,
    });
    state.terminal_latched = true;
}

/// Ordinary failure when no undo is owed, unless forward work is unresolved.
fn resolve_without_undo(
    state: &mut SagaResolutionState,
    context: &SagaContext,
    reason: Box<str>,
    failure: SagaFailureDetails,
    out: &mut Vec<SagaChoreographyEvent>,
) {
    let uncertain = uncertain_steps(state);
    if uncertain.is_empty() {
        out.push(SagaChoreographyEvent::SagaFailed {
            context: terminal_context(context),
            reason,
            failure: Some(failure),
        });
        state.terminal_latched = true;
    } else {
        quarantine_uncertain(state, terminal_context(context), &uncertain, &reason, out);
    }
}

#[derive(Debug)]
struct TimeoutDiagnostics {
    missing_steps: String,
    ready_not_started: String,
    started_not_completed: String,
    blocked_steps: String,
    completed_steps: String,
    started_steps: String,
    failed_steps: String,
    acked_steps: String,
}

impl TimeoutDiagnostics {
    fn reason(&self, timeout_prefix: String, policy_id: &str) -> String {
        let mut parts = vec![timeout_prefix];
        push_non_empty(&mut parts, "missing_steps", &self.missing_steps);
        push_non_empty(&mut parts, "ready_not_started", &self.ready_not_started);
        push_non_empty(
            &mut parts,
            "started_not_completed",
            &self.started_not_completed,
        );
        push_non_empty(&mut parts, "blocked_steps", &self.blocked_steps);
        push_non_empty(&mut parts, "completed_steps", &self.completed_steps);
        push_non_empty(&mut parts, "started_steps", &self.started_steps);
        push_non_empty(&mut parts, "acked_steps", &self.acked_steps);
        push_non_empty(&mut parts, "failed_steps", &self.failed_steps);
        parts.push(format!("policy={policy_id}"));
        parts.join(" ")
    }
}

fn push_non_empty(parts: &mut Vec<String>, key: &str, value: &str) {
    if !value.is_empty() {
        parts.push(format!("{key}={value}"));
    }
}

fn sorted_set_values(values: &HashSet<Box<str>>) -> String {
    let mut sorted = values
        .iter()
        .map(|value| value.as_ref())
        .collect::<Vec<_>>();
    sorted.sort_unstable();
    sorted.join(",")
}

fn step_label(step: &SagaWorkflowStepContract) -> String {
    format!("{}({})", step.step_name, step.participant_id)
}

fn dependency_names(depends_on: WorkflowDependencySpec) -> Vec<&'static str> {
    match depends_on {
        WorkflowDependencySpec::OnSagaStart => Vec::new(),
        WorkflowDependencySpec::After(step) => vec![step],
        WorkflowDependencySpec::AnyOf(steps) | WorkflowDependencySpec::AllOf(steps) => {
            steps.to_vec()
        }
    }
}

fn missing_dependencies(
    depends_on: WorkflowDependencySpec,
    completed_steps: &HashSet<Box<str>>,
) -> Vec<&'static str> {
    match depends_on {
        WorkflowDependencySpec::OnSagaStart => Vec::new(),
        WorkflowDependencySpec::After(step) => {
            if completed_steps.contains(step) {
                Vec::new()
            } else {
                vec![step]
            }
        }
        WorkflowDependencySpec::AnyOf(steps) => {
            if steps.iter().any(|step| completed_steps.contains(*step)) {
                Vec::new()
            } else {
                steps.to_vec()
            }
        }
        WorkflowDependencySpec::AllOf(steps) => steps
            .iter()
            .filter(|step| !completed_steps.contains(**step))
            .copied()
            .collect(),
    }
}

fn collect_step_blockers(
    step_name: &str,
    by_step: &HashMap<&'static str, &SagaWorkflowStepContract>,
    state: &SagaResolutionState,
    ready_not_started: &mut Vec<String>,
    started_not_completed: &mut Vec<String>,
    blocked_steps: &mut Vec<String>,
    visited: &mut HashSet<Box<str>>,
) {
    if state.completed_steps.contains(step_name) {
        return;
    }
    if state.failed_steps.contains(step_name) {
        return;
    }
    if !visited.insert(step_name.into()) {
        return;
    }

    let Some(step) = by_step.get(step_name) else {
        ready_not_started.push(step_name.to_string());
        return;
    };

    if state.started_steps.contains(step_name) {
        started_not_completed.push(step_label(step));
        return;
    }

    let missing_deps = missing_dependencies(step.depends_on, &state.completed_steps);
    if missing_deps.is_empty() {
        ready_not_started.push(step_label(step));
        return;
    }

    blocked_steps.push(format!("{}<-{}", step_label(step), missing_deps.join("+")));
    for dependency in dependency_names(step.depends_on) {
        collect_step_blockers(
            dependency,
            by_step,
            state,
            ready_not_started,
            started_not_completed,
            blocked_steps,
            visited,
        );
    }
}

fn timeout_diagnostics(policy: &TerminalPolicy, state: &SagaResolutionState) -> TimeoutDiagnostics {
    let mut missing = policy
        .success_criteria
        .missing_required_steps(&state.completed_steps)
        .iter()
        .map(|step| step.to_string())
        .collect::<Vec<_>>();
    missing.sort_unstable();

    let mut by_step: HashMap<&'static str, &SagaWorkflowStepContract> = HashMap::new();
    for step in policy.workflow_steps {
        by_step.insert(step.step_name, step);
    }

    let mut ready_not_started = Vec::new();
    let mut started_not_completed = Vec::new();
    let mut blocked_steps = Vec::new();
    let mut visited = HashSet::new();
    for step in &missing {
        collect_step_blockers(
            step,
            &by_step,
            state,
            &mut ready_not_started,
            &mut started_not_completed,
            &mut blocked_steps,
            &mut visited,
        );
    }

    ready_not_started.sort_unstable();
    ready_not_started.dedup();
    started_not_completed.sort_unstable();
    started_not_completed.dedup();
    blocked_steps.sort_unstable();
    blocked_steps.dedup();

    TimeoutDiagnostics {
        missing_steps: missing.join(","),
        ready_not_started: ready_not_started.join(","),
        started_not_completed: started_not_completed.join(","),
        blocked_steps: blocked_steps.join(","),
        completed_steps: sorted_set_values(&state.completed_steps),
        started_steps: sorted_set_values(&state.started_steps),
        failed_steps: sorted_set_values(&state.failed_steps),
        acked_steps: sorted_set_values(&state.acked_steps),
    }
}

fn emit_timeout_diagnostic(
    policy: &TerminalPolicy,
    state: &SagaResolutionState,
    timeout_kind: &str,
    diagnostic: &TimeoutDiagnostics,
) {
    tracing::error!(
        target: "core::saga",
        event = "saga_terminal_timeout_diagnostic",
        saga_type = policy.saga_type.as_ref(),
        saga_id = state.last_context.saga_id.get(),
        policy = policy.policy_id.as_ref(),
        timeout_kind,
        missing_steps = diagnostic.missing_steps.as_str(),
        ready_not_started = diagnostic.ready_not_started.as_str(),
        started_not_completed = diagnostic.started_not_completed.as_str(),
        blocked_steps = diagnostic.blocked_steps.as_str(),
        completed_steps = diagnostic.completed_steps.as_str(),
        started_steps = diagnostic.started_steps.as_str(),
        acked_steps = diagnostic.acked_steps.as_str(),
        failed_steps = diagnostic.failed_steps.as_str(),
        last_step = state.last_context.step_name.as_ref()
    );
}

/// Forward overall/stalled expiry: `(kind, reason prefix)` when a budget is spent.
fn expired_budget(
    policy: &TerminalPolicy,
    state: &SagaResolutionState,
    now_millis: u64,
) -> Option<(&'static str, String)> {
    let elapsed_ms = now_millis.saturating_sub(state.overall_anchor_millis);
    let overall_timeout_ms = policy.overall_timeout.as_millis() as u64;
    if elapsed_ms > overall_timeout_ms {
        return Some((
            "overall_timeout",
            format!("overall_timeout after {overall_timeout_ms}ms"),
        ));
    }

    let stalled_ms = now_millis.saturating_sub(state.last_progress_at_millis);
    let stalled_timeout_ms = policy.stalled_timeout.as_millis() as u64;
    if stalled_ms > stalled_timeout_ms {
        return Some((
            "stalled_timeout",
            format!("stalled_timeout after {stalled_timeout_ms}ms without progress"),
        ));
    }
    None
}

/// Generic overall/stalled timeout handling:
/// - unresolved rollback: quarantine (evidence kept, no ordinary failure);
/// - known compensable effects: start rollback;
/// - otherwise: plain failure.
fn resolve_generic_timeout(
    policy: &TerminalPolicy,
    state: &mut SagaResolutionState,
    now_millis: u64,
) -> Vec<SagaChoreographyEvent> {
    let Some((kind, prefix)) = expired_budget(policy, state, now_millis) else {
        return Vec::new();
    };
    let diagnostic = timeout_diagnostics(policy, state);
    emit_timeout_diagnostic(policy, state, kind, &diagnostic);
    let reason: Box<str> = diagnostic.reason(prefix, &policy.policy_id).into();
    let context = terminal_context_at(&state.last_context, now_millis);

    if let Some(plan) = &state.rollback {
        let step = plan
            .requested
            .first()
            .or_else(|| plan.queue.front())
            .cloned()
            .unwrap_or_else(|| TERMINAL_RESOLVER_STEP.into());
        let participant_id = state
            .accepted_compensations
            .get(&step)
            .map(|accepted| accepted.participant_id.clone())
            .or_else(|| state.accepted_participants.get(&step).cloned())
            .unwrap_or_else(|| TERMINAL_RESOLVER_STEP.into());
        state.terminal_latched = true;
        return vec![SagaChoreographyEvent::SagaQuarantined {
            context,
            reason: format!("{reason} with unresolved rollback").into(),
            step,
            participant_id,
        }];
    }

    let has_effects = state
        .compensable_steps
        .iter()
        .any(|step| !state.completed_compensation_steps.contains(step));
    if has_effects {
        let failure = SagaFailureDetails {
            step_name: TERMINAL_RESOLVER_STEP.into(),
            participant_id: TERMINAL_RESOLVER_STEP.into(),
            error_code: Some(kind.into()),
            error_message: reason.clone(),
            at_millis: now_millis,
        };
        begin_rollback(state, failure, reason.clone(), Some(reason));
        // The undo phase gets its own budgets; both windows restart here.
        state.overall_anchor_millis = now_millis;
        state.last_progress_at_millis = now_millis;
        let mut out = Vec::new();
        let last = state.last_context.clone();
        advance_rollback(state, &last, &mut out);
        for event in &mut out {
            if let SagaChoreographyEvent::CompensationRequested { context, .. } = event {
                context.event_timestamp_millis = now_millis;
            }
        }
        return out;
    }

    let uncertain = uncertain_steps(state);
    let mut out = Vec::new();
    if uncertain.is_empty() {
        state.terminal_latched = true;
        out.push(SagaChoreographyEvent::SagaFailed {
            context,
            reason,
            failure: None,
        });
    } else {
        quarantine_uncertain(state, context, &uncertain, &reason, &mut out);
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use crate::{
        AcceptedStepTimeoutOutcome, SagaChoreographyEvent, SagaContext, SagaId,
        SagaWorkflowStepContract, StepExecutionId, WorkflowDependencySpec,
    };

    use super::{FailureAuthority, SuccessCriteria, TerminalPolicy, TerminalResolver};

    static OPEN_POSITION_STEPS: &[SagaWorkflowStepContract] = &[
        SagaWorkflowStepContract {
            step_name: "risk_check",
            participant_id: "account-balance",
            depends_on: WorkflowDependencySpec::OnSagaStart,
        },
        SagaWorkflowStepContract {
            step_name: "positions_check",
            participant_id: "positions",
            depends_on: WorkflowDependencySpec::OnSagaStart,
        },
        SagaWorkflowStepContract {
            step_name: "universe_filter_hold",
            participant_id: "options-universe",
            depends_on: WorkflowDependencySpec::OnSagaStart,
        },
        SagaWorkflowStepContract {
            step_name: "book_snapshot_check",
            participant_id: "options-books",
            depends_on: WorkflowDependencySpec::AllOf(&[
                "risk_check",
                "positions_check",
                "universe_filter_hold",
            ]),
        },
        SagaWorkflowStepContract {
            step_name: "create_order",
            participant_id: "order-manager",
            depends_on: WorkflowDependencySpec::After("book_snapshot_check"),
        },
    ];

    fn open_position_policy(stalled_timeout: Duration) -> TerminalPolicy {
        let mut required_steps = HashSet::new();
        required_steps.insert("create_order".into());
        TerminalPolicy {
            saga_type: "order_lifecycle".into(),
            policy_id: "open_position/default".into(),
            failure_authority: FailureAuthority::AnyParticipant,
            success_criteria: SuccessCriteria::AllOf(required_steps),
            overall_timeout: Duration::from_secs(5),
            stalled_timeout,
            workflow_steps: OPEN_POSITION_STEPS,
        }
    }

    fn ctx(step: &str) -> SagaContext {
        // One fixed run identity: contexts of a single saga share a start time.
        static RUN_STARTED_AT: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
        let run_started_at = *RUN_STARTED_AT.get_or_init(SagaContext::now_millis);
        SagaContext {
            saga_id: SagaId::new(9),
            saga_type: "order_lifecycle".into(),
            step_name: step.into(),
            correlation_id: 9,
            causation_id: 9,
            trace_id: 9,
            step_index: 0,
            attempt: 0,
            initiator_peer_id: [0; 32],
            saga_started_at_millis: run_started_at,
            event_timestamp_millis: SagaContext::now_millis(),
        }
    }

    fn ctx_at(
        step: &str,
        saga_id: u64,
        started_at_millis: u64,
        event_timestamp_millis: u64,
    ) -> SagaContext {
        SagaContext {
            saga_id: SagaId::new(saga_id),
            saga_type: "order_lifecycle".into(),
            step_name: step.into(),
            correlation_id: saga_id,
            causation_id: saga_id,
            trace_id: saga_id,
            step_index: 0,
            attempt: 0,
            initiator_peer_id: [0; 32],
            saga_started_at_millis: started_at_millis,
            event_timestamp_millis,
        }
    }

    #[test]
    fn allof_success_emits_single_saga_completed() {
        let mut required: HashSet<Box<str>> = HashSet::new();
        required.insert("a".into());
        required.insert("b".into());
        let policy = TerminalPolicy {
            saga_type: "order_lifecycle".into(),
            policy_id: "test".into(),
            failure_authority: FailureAuthority::AnyParticipant,
            success_criteria: SuccessCriteria::AllOf(required),
            overall_timeout: Duration::from_secs(60),
            stalled_timeout: Duration::from_secs(60),
            workflow_steps: &[],
        };
        let mut resolver = TerminalResolver::new(policy);

        let out1 = resolver.ingest(&SagaChoreographyEvent::StepCompleted {
            context: ctx("a"),
            output: vec![],
            saga_input: vec![],
            compensation_available: false,
        });
        assert!(out1.is_empty());

        let out2 = resolver.ingest(&SagaChoreographyEvent::StepCompleted {
            context: ctx("b"),
            output: vec![],
            saga_input: vec![],
            compensation_available: false,
        });
        assert!(matches!(
            out2.first(),
            Some(SagaChoreographyEvent::SagaCompleted { .. })
        ));
    }

    #[test]
    fn step_failed_without_compensation_is_terminal() {
        let mut resolver = TerminalResolver::new(TerminalPolicy::order_lifecycle_default());
        let out = resolver.ingest(&SagaChoreographyEvent::StepFailed {
            context: ctx("a"),
            participant_id: "actor-a".into(),
            error_code: Some("TEMP".into()),
            error: "try again".into(),
            requires_compensation: false,
        });
        assert!(matches!(
            out.first(),
            Some(SagaChoreographyEvent::SagaFailed { reason, .. }) if reason.as_ref() == "try again"
        ));
    }

    #[test]
    fn unauthorized_step_failure_is_ignored() {
        let mut only_steps = HashSet::new();
        only_steps.insert("allowed".into());
        let policy = TerminalPolicy {
            saga_type: "order_lifecycle".into(),
            policy_id: "test".into(),
            failure_authority: FailureAuthority::OnlySteps(only_steps),
            success_criteria: SuccessCriteria::AnyOf(HashSet::new()),
            overall_timeout: Duration::from_secs(30),
            stalled_timeout: Duration::from_secs(30),
            workflow_steps: &[],
        };
        let mut resolver = TerminalResolver::new(policy);
        let out = resolver.ingest(&SagaChoreographyEvent::StepFailed {
            context: ctx("denied"),
            participant_id: "actor-a".into(),
            error_code: None,
            error: "no".into(),
            requires_compensation: false,
        });
        assert!(out.is_empty());
    }

    #[test]
    fn authoritative_quarantine_is_absorbing_live_and_after_restore() {
        let mut required_steps = HashSet::new();
        required_steps.insert("create_order".into());
        let policy = TerminalPolicy {
            saga_type: "order_lifecycle".into(),
            policy_id: "absorbing".into(),
            failure_authority: FailureAuthority::AnyParticipant,
            success_criteria: SuccessCriteria::AllOf(required_steps),
            overall_timeout: Duration::from_millis(100),
            stalled_timeout: Duration::from_secs(60),
            workflow_steps: &[],
        };
        let started = SagaChoreographyEvent::SagaStarted {
            context: ctx_at("risk_check", 9, 1_000, 1_000),
            payload: Vec::new(),
        };
        let quarantined = SagaChoreographyEvent::SagaQuarantined {
            context: ctx_at("participant", 9, 1_000, 1_010),
            reason: "ambiguous".into(),
            step: "create_order".into(),
            participant_id: "order-manager".into(),
        };
        let late_completed = SagaChoreographyEvent::StepCompleted {
            context: ctx_at("create_order", 9, 1_000, 1_020),
            output: vec![],
            saga_input: vec![],
            compensation_available: false,
        };

        let mut resolver = TerminalResolver::new(policy.clone());
        assert!(resolver.ingest_at(&started, 1_000).is_empty());
        assert!(resolver.ingest_at(&quarantined, 1_010).is_empty());
        let late = resolver.ingest_at(&late_completed, 1_020);
        assert!(
            late.is_empty(),
            "late completion after quarantine: {late:?}"
        );
        let timed_out = resolver.poll_timeouts_at(5_000);
        assert!(
            timed_out.is_empty(),
            "timeout after quarantine: {timed_out:?}"
        );

        let (mut restored, unpublished) =
            TerminalResolver::restore_from_events(policy, std::slice::from_ref(&quarantined));
        assert!(unpublished.is_empty(), "restore emitted: {unpublished:?}");
        assert!(restored.poll_timeouts_at(5_000).is_empty());
        assert!(restored.ingest_at(&late_completed, 5_000).is_empty());
    }

    #[test]
    fn hard_timeout_triggers_without_new_events() {
        let mut required_steps = HashSet::new();
        required_steps.insert("create_order".into());
        let policy = TerminalPolicy {
            saga_type: "order_lifecycle".into(),
            policy_id: "hard-timeout".into(),
            failure_authority: FailureAuthority::AnyParticipant,
            success_criteria: SuccessCriteria::AllOf(required_steps),
            overall_timeout: Duration::from_millis(100),
            stalled_timeout: Duration::from_secs(60),
            workflow_steps: &[],
        };
        let mut resolver = TerminalResolver::new(policy);
        let start = SagaChoreographyEvent::SagaStarted {
            context: ctx_at("risk_check", 9, 1_000, 1_000),
            payload: Vec::new(),
        };
        let _ = resolver.ingest_at(&start, 1_000);

        assert!(resolver.poll_timeouts_at(1_099).is_empty());
        let timed_out = resolver.poll_timeouts_at(1_101);
        assert!(
            matches!(
                timed_out.first(),
                Some(SagaChoreographyEvent::SagaFailed { reason, .. })
                if reason.as_ref().contains("overall_timeout")
            ),
            "expected hard-timeout failure, got: {timed_out:?}"
        );
    }

    #[test]
    fn accepted_step_idle_timeout_is_enforced_by_terminal_resolver() {
        let mut resolver = TerminalResolver::new(open_position_policy(Duration::from_secs(5)));
        let start = SagaChoreographyEvent::SagaStarted {
            context: ctx_at("create_order", 9, 1_000, 1_000),
            payload: Vec::new(),
        };
        let _ = resolver.ingest_at(&start, 1_000);
        let accepted = SagaChoreographyEvent::StepAccepted {
            context: ctx_at("create_order", 9, 1_000, 1_010),
            participant_id: "order-manager".into(),
            execution_id: StepExecutionId::new("external-9"),
            deadline_at_millis: 1_110,
            hard_deadline_at_millis: 1_300,
            timeouts_enabled: true,
            timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                requires_compensation: false,
            },
            compensation_available: false,
        };
        let _ = resolver.ingest_at(&accepted, 1_010);

        assert!(resolver.poll_timeouts_at(1_110).is_empty());
        let timed_out = resolver.poll_timeouts_at(1_111);
        assert!(matches!(
            timed_out.as_slice(),
            [SagaChoreographyEvent::StepFailed { error_code: Some(code), requires_compensation: false, .. },
             SagaChoreographyEvent::SagaFailed { reason, failure: Some(failure), .. }]
                if code.as_ref() == "idle" && reason.contains("accepted step idle timeout")
                    && failure.step_name.as_ref() == "create_order"
                    && failure.participant_id.as_ref() == "order-manager"
        ));
    }

    #[test]
    fn accepted_step_hard_timeout_can_quarantine_from_terminal_resolver() {
        let mut resolver = TerminalResolver::new(open_position_policy(Duration::from_secs(5)));
        let start = SagaChoreographyEvent::SagaStarted {
            context: ctx_at("create_order", 9, 1_000, 1_000),
            payload: Vec::new(),
        };
        let _ = resolver.ingest_at(&start, 1_000);
        let accepted = SagaChoreographyEvent::StepAccepted {
            context: ctx_at("create_order", 9, 1_000, 1_010),
            participant_id: "order-manager".into(),
            execution_id: StepExecutionId::new("external-9"),
            deadline_at_millis: 1_110,
            hard_deadline_at_millis: 1_120,
            timeouts_enabled: true,
            timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
            compensation_available: false,
        };
        let _ = resolver.ingest_at(&accepted, 1_010);

        let timed_out = resolver.poll_timeouts_at(1_121);
        assert!(matches!(
            timed_out.as_slice(),
            [SagaChoreographyEvent::SagaQuarantined { reason, step, participant_id, .. }]
                if reason.contains("accepted step hard timeout")
                    && step.as_ref() == "create_order"
                    && participant_id.as_ref() == "order-manager"
        ));
    }

    #[test]
    fn accepted_step_quarantine_timeout_ignores_failure_authority() {
        let mut denied_steps = HashSet::new();
        denied_steps.insert("create_order".into());
        let mut required_steps = HashSet::new();
        required_steps.insert("create_order".into());
        let policy = TerminalPolicy {
            saga_type: "order_lifecycle".into(),
            policy_id: "accepted-timeout-deny".into(),
            failure_authority: FailureAuthority::DenySteps(denied_steps),
            success_criteria: SuccessCriteria::AllOf(required_steps),
            overall_timeout: Duration::from_secs(5),
            stalled_timeout: Duration::from_secs(5),
            workflow_steps: &[],
        };
        let mut resolver = TerminalResolver::new(policy);
        let start = SagaChoreographyEvent::SagaStarted {
            context: ctx_at("create_order", 9, 1_000, 1_000),
            payload: Vec::new(),
        };
        let _ = resolver.ingest_at(&start, 1_000);
        let accepted = SagaChoreographyEvent::StepAccepted {
            context: ctx_at("create_order", 9, 1_000, 1_010),
            participant_id: "order-manager".into(),
            execution_id: StepExecutionId::new("external-9"),
            deadline_at_millis: 1_110,
            hard_deadline_at_millis: 1_120,
            timeouts_enabled: true,
            timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
            compensation_available: false,
        };
        let _ = resolver.ingest_at(&accepted, 1_010);

        let timed_out = resolver.poll_timeouts_at(1_121);
        assert!(matches!(
            timed_out.as_slice(),
            [SagaChoreographyEvent::SagaQuarantined { step, participant_id, .. }]
                if step.as_ref() == "create_order"
                    && participant_id.as_ref() == "order-manager"
        ));
    }

    #[test]
    fn unauthorized_accepted_timeout_does_not_swallow_sibling_timeout_events() {
        let mut authorized = HashSet::new();
        authorized.insert("create_order".into());
        let mut required = HashSet::new();
        required.insert("create_order".into());
        let policy = TerminalPolicy {
            saga_type: "order_lifecycle".into(),
            policy_id: "accepted-timeout/sibling".into(),
            failure_authority: FailureAuthority::OnlySteps(authorized),
            success_criteria: SuccessCriteria::AllOf(required),
            overall_timeout: Duration::from_secs(5),
            stalled_timeout: Duration::from_secs(5),
            workflow_steps: OPEN_POSITION_STEPS,
        };
        let mut resolver = TerminalResolver::new(policy);
        let _ = resolver.ingest_at(
            &SagaChoreographyEvent::SagaStarted {
                context: ctx_at("risk_check", 19, 1_000, 1_000),
                payload: Vec::new(),
            },
            1_000,
        );
        let _ = resolver.ingest_at(
            &SagaChoreographyEvent::StepCompleted {
                context: ctx_at("risk_check", 19, 1_000, 1_010),
                output: Vec::new(),
                saga_input: Vec::new(),
                compensation_available: true,
            },
            1_010,
        );
        let _ = resolver.ingest_at(
            &SagaChoreographyEvent::StepAccepted {
                context: ctx_at("risk_check", 19, 1_000, 1_020),
                participant_id: "risk".into(),
                execution_id: StepExecutionId::new("external-risk"),
                deadline_at_millis: 1_100,
                hard_deadline_at_millis: 1_500,
                timeouts_enabled: true,
                timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                    requires_compensation: false,
                },
                compensation_available: false,
            },
            1_020,
        );
        let _ = resolver.ingest_at(
            &SagaChoreographyEvent::StepAccepted {
                context: ctx_at("create_order", 19, 1_000, 1_030),
                participant_id: "order-manager".into(),
                execution_id: StepExecutionId::new("external-order"),
                deadline_at_millis: 1_100,
                hard_deadline_at_millis: 1_500,
                timeouts_enabled: true,
                timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                    requires_compensation: true,
                },
                compensation_available: false,
            },
            1_030,
        );

        let timed_out = resolver.poll_timeouts_at(1_101);
        assert!(
            matches!(
                timed_out.iter().find(|event| matches!(event, SagaChoreographyEvent::CompensationRequested { .. })),
                Some(SagaChoreographyEvent::CompensationRequested { failed_step, steps_to_compensate, .. })
                    if failed_step.as_ref() == "create_order"
                        && steps_to_compensate.as_slice() == ["risk_check".into()]
            ),
            "unauthorized sibling timeout must not swallow compensation request: {timed_out:?}"
        );
        assert_eq!(
            timed_out
                .iter()
                .filter(|event| matches!(event, SagaChoreographyEvent::StepFailed { .. }))
                .count(),
            2,
            "both step dispositions must reach their owners without granting sibling failure authority"
        );
    }

    #[test]
    fn rejected_accepted_sibling_has_no_undo_obligation_without_saga_failure_authority() {
        for timeout in [false, true] {
            let mut policy = open_position_policy(Duration::from_secs(5));
            policy.failure_authority =
                FailureAuthority::OnlySteps(HashSet::from(["create_order".into()]));
            let mut resolver = TerminalResolver::new(policy);
            let seed = ctx_at("create_order", 20, 1_000, 1_000);
            resolver.ingest_at(
                &SagaChoreographyEvent::SagaStarted {
                    context: seed.clone(),
                    payload: vec![],
                },
                1_000,
            );
            resolver.ingest_at(
                &SagaChoreographyEvent::StepAccepted {
                    context: ctx_at("risk_check", 20, 1_000, 1_010),
                    participant_id: "risk".into(),
                    execution_id: StepExecutionId::new("risk-potential"),
                    deadline_at_millis: 1_100,
                    hard_deadline_at_millis: 1_500,
                    timeouts_enabled: true,
                    timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                        requires_compensation: false,
                    },
                    compensation_available: true,
                },
                1_010,
            );
            if timeout {
                let _ = resolver.poll_timeouts_at(1_101);
            } else {
                assert!(
                    resolver
                        .ingest_at(
                            &SagaChoreographyEvent::StepFailed {
                                context: ctx_at("risk_check", 20, 1_000, 1_100),
                                participant_id: "risk".into(),
                                error: "remote rejected".into(),
                                error_code: None,
                                requires_compensation: false,
                            },
                            1_100
                        )
                        .is_empty()
                );
            }
            let out = resolver.ingest_at(
                &SagaChoreographyEvent::StepFailed {
                    context: ctx_at("create_order", 20, 1_000, 1_110),
                    participant_id: "order".into(),
                    error: "safe rejection".into(),
                    error_code: None,
                    requires_compensation: false,
                },
                1_110,
            );
            assert!(
                matches!(out.as_slice(), [SagaChoreographyEvent::SagaFailed { .. }]),
                "a definitively rejected sibling owns no effect, even when it cannot fail the saga (timeout={timeout}): {out:?}"
            );
        }
    }

    #[test]
    fn terminal_cache_is_bounded_and_eviction_is_not_a_durable_fence() {
        let mut resolver = TerminalResolver::new(open_position_policy(Duration::from_secs(5)));
        resolver.terminal_latch_retention = 2;

        for (saga_id, started) in [(21_u64, 1_000_u64), (22, 2_000), (23, 3_000)] {
            let _ = resolver.ingest_at(
                &SagaChoreographyEvent::SagaStarted {
                    context: ctx_at("create_order", saga_id, started, started),
                    payload: Vec::new(),
                },
                started,
            );
            let completed = resolver.ingest_at(
                &SagaChoreographyEvent::StepCompleted {
                    context: ctx_at("create_order", saga_id, started, started + 10),
                    output: Vec::new(),
                    saga_input: Vec::new(),
                    compensation_available: false,
                },
                started + 10,
            );
            assert!(matches!(
                completed.as_slice(),
                [SagaChoreographyEvent::SagaCompleted { .. }]
            ));
            assert!(resolver.terminal_latched_order.len() <= 2);
            assert!(resolver.states.len() <= 2);
        }
        assert!(
            !resolver.states.contains_key(&SagaId::new(21)),
            "the oldest terminal saga is evicted at capacity"
        );
        // Still-cached terminal sagas stay absorbing.
        assert!(
            resolver
                .ingest_at(
                    &SagaChoreographyEvent::StepCompleted {
                        context: ctx_at("create_order", 23, 3_000, 4_000),
                        output: Vec::new(),
                        saga_input: Vec::new(),
                        compensation_available: false,
                    },
                    4_000,
                )
                .is_empty()
        );
    }

    fn completed_effect(base: &SagaContext, step: &str) -> SagaChoreographyEvent {
        SagaChoreographyEvent::StepCompleted {
            context: base.next_step(step.into()),
            output: Vec::new(),
            saga_input: Vec::new(),
            compensation_available: true,
        }
    }

    fn compensating_failure(base: &SagaContext, step: &str) -> SagaChoreographyEvent {
        SagaChoreographyEvent::StepFailed {
            context: base.next_step(step.into()),
            participant_id: step.into(),
            error_code: None,
            error: "boom".into(),
            requires_compensation: true,
        }
    }

    fn singleton_request(step: &str) -> impl Fn(&[SagaChoreographyEvent]) -> bool + '_ {
        move |events| {
            matches!(
                events,
                [SagaChoreographyEvent::CompensationRequested { steps_to_compensate, .. }]
                    if steps_to_compensate.as_slice() == [Box::<str>::from(step)]
            )
        }
    }

    #[test]
    fn accepted_undo_blocks_successor_until_it_resolves() {
        let mut resolver = TerminalResolver::new(TerminalPolicy::order_lifecycle_default());
        let base = ctx_at("a", 30, 1_000, 1_000);
        let _ = resolver.ingest_at(&completed_effect(&base, "a"), 1_001);
        let _ = resolver.ingest_at(&completed_effect(&base, "b"), 1_002);
        let first = resolver.ingest_at(&compensating_failure(&base, "c"), 1_010);
        assert!(singleton_request("b")(&first), "{first:?}");
        assert!(
            resolver
                .ingest_at(
                    &SagaChoreographyEvent::CompensationAccepted {
                        context: base.next_step("b".into()),
                        participant_id: "b".into(),
                        execution_id: StepExecutionId::new("undo-b"),
                        deadline_at_millis: 5_000,
                        hard_deadline_at_millis: 9_000,
                    },
                    1_020,
                )
                .is_empty(),
            "accepted undo must not release its successor"
        );
        assert!(resolver.poll_timeouts_at(1_500).is_empty());
        let next = resolver.ingest_at(
            &SagaChoreographyEvent::CompensationCompleted {
                context: base.next_step("b".into()),
            },
            1_600,
        );
        assert!(singleton_request("a")(&next), "{next:?}");
    }

    #[test]
    fn late_accepted_effect_during_rollback_joins_queue_first() {
        let mut resolver = TerminalResolver::new(TerminalPolicy::order_lifecycle_default());
        let base = ctx_at("a", 31, 1_000, 1_000);
        let _ = resolver.ingest_at(&completed_effect(&base, "a"), 1_001);
        let _ = resolver.ingest_at(&compensating_failure(&base, "x"), 1_002);
        assert!(
            resolver
                .ingest_at(
                    &SagaChoreographyEvent::StepAccepted {
                        context: base.next_step("late".into()),
                        participant_id: "late-p".into(),
                        execution_id: StepExecutionId::new("late-1"),
                        deadline_at_millis: 9_000,
                        hard_deadline_at_millis: 9_500,
                        timeouts_enabled: true,
                        timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
                        compensation_available: true,
                    },
                    1_003,
                )
                .is_empty()
        );
        let after_a = resolver.ingest_at(
            &SagaChoreographyEvent::CompensationCompleted {
                context: base.next_step("a".into()),
            },
            1_004,
        );
        assert!(singleton_request("late")(&after_a), "{after_a:?}");
    }

    #[test]
    fn effect_completing_after_its_undo_quarantines() {
        let mut resolver = TerminalResolver::new(TerminalPolicy::order_lifecycle_default());
        let base = ctx_at("a", 32, 1_000, 1_000);
        let _ = resolver.ingest_at(
            &SagaChoreographyEvent::StepAccepted {
                context: base.next_step("a".into()),
                participant_id: "a-p".into(),
                execution_id: StepExecutionId::new("a-1"),
                deadline_at_millis: 9_000,
                hard_deadline_at_millis: 9_500,
                timeouts_enabled: false,
                timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
                compensation_available: true,
            },
            1_001,
        );
        let _ = resolver.ingest_at(&compensating_failure(&base, "x"), 1_002);
        // A second obligation keeps the rollback open after a's undo resolves.
        let _ = resolver.ingest_at(&completed_effect(&base, "b"), 1_003);
        let out = resolver.ingest_at(
            &SagaChoreographyEvent::CompensationCompleted {
                context: base.next_step("a".into()),
            },
            1_004,
        );
        assert!(singleton_request("b")(&out), "{out:?}");
        let out = resolver.ingest_at(&completed_effect(&base, "a"), 1_005);
        assert!(matches!(
            out.as_slice(),
            [SagaChoreographyEvent::SagaQuarantined { step, participant_id, .. }]
                if step.as_ref() == "a" && participant_id.as_ref() == "a-p"
        ));
    }

    #[test]
    fn failed_undo_with_remaining_obligations_quarantines() {
        let mut resolver = TerminalResolver::new(TerminalPolicy::order_lifecycle_default());
        let base = ctx_at("a", 33, 1_000, 1_000);
        let _ = resolver.ingest_at(&completed_effect(&base, "a"), 1_001);
        let _ = resolver.ingest_at(&completed_effect(&base, "b"), 1_002);
        let _ = resolver.ingest_at(&compensating_failure(&base, "x"), 1_010);
        let out = resolver.ingest_at(
            &SagaChoreographyEvent::CompensationFailed {
                context: base.next_step("b".into()),
                participant_id: "b-p".into(),
                error: "undo failed".into(),
                is_ambiguous: false,
            },
            1_011,
        );
        assert!(matches!(
            out.as_slice(),
            [SagaChoreographyEvent::SagaQuarantined { step, .. }] if step.as_ref() == "b"
        ));
    }

    #[test]
    fn duplicate_compensation_request_does_not_reopen_a_completed_step() {
        let mut resolver = TerminalResolver::new(TerminalPolicy::order_lifecycle_default());
        let context = ctx_at("create_order", 23, 1_000, 1_000);
        let failure = crate::SagaFailureDetails {
            step_name: "create_order".into(),
            participant_id: "order-manager".into(),
            error_code: Some("exchange_rejected".into()),
            error_message: "create failed".into(),
            at_millis: 1_010,
        };
        let request = SagaChoreographyEvent::CompensationRequested {
            context: context.clone(),
            failed_step: "create_order".into(),
            reason: "create failed".into(),
            failure,
            steps_to_compensate: vec!["reserve".into(), "create_order".into()],
        };

        assert!(resolver.ingest_at(&request, 1_010).is_empty());
        assert!(
            resolver
                .ingest_at(
                    &SagaChoreographyEvent::CompensationCompleted {
                        context: context.next_step("reserve".into()),
                    },
                    1_020,
                )
                .is_empty()
        );
        assert!(resolver.ingest_at(&request, 1_030).is_empty());
        assert!(matches!(
            resolver
                .ingest_at(
                    &SagaChoreographyEvent::CompensationCompleted {
                        context: context.next_step("create_order".into()),
                    },
                    1_040,
                )
                .as_slice(),
            [SagaChoreographyEvent::SagaFailed { .. }]
        ));
    }

    #[test]
    fn compensation_request_suppresses_forward_timeout_for_the_same_step() {
        let mut resolver = TerminalResolver::new(TerminalPolicy::order_lifecycle_default());
        let context = ctx_at("reserve", 24, 1_000, 1_000);
        assert!(
            resolver
                .ingest_at(
                    &SagaChoreographyEvent::SagaStarted {
                        context: context.clone(),
                        payload: Vec::new(),
                    },
                    1_000,
                )
                .is_empty()
        );
        assert!(
            resolver
                .ingest_at(
                    &SagaChoreographyEvent::StepAccepted {
                        context: context.clone(),
                        participant_id: "reserve-participant".into(),
                        execution_id: StepExecutionId::new("reserve-24"),
                        deadline_at_millis: 1_100,
                        hard_deadline_at_millis: 1_200,
                        timeouts_enabled: true,
                        timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
                        compensation_available: true,
                    },
                    1_010,
                )
                .is_empty()
        );

        let failure_events = resolver.ingest_at(
            &SagaChoreographyEvent::StepFailed {
                context: context.next_step("create_order".into()),
                participant_id: "order-manager".into(),
                error_code: Some("exchange_rejected".into()),
                error: "create failed".into(),
                requires_compensation: true,
            },
            1_020,
        );
        assert!(matches!(
            failure_events.as_slice(),
            [SagaChoreographyEvent::CompensationRequested {
                steps_to_compensate,
                ..
            }] if steps_to_compensate.as_slice() == [Box::<str>::from("reserve")]
        ));
        assert!(
            resolver
                .ingest_at(
                    &SagaChoreographyEvent::StepAccepted {
                        context: context.clone(),
                        participant_id: "reserve-participant".into(),
                        execution_id: StepExecutionId::new("reserve-24-replayed"),
                        deadline_at_millis: 1_100,
                        hard_deadline_at_millis: 1_200,
                        timeouts_enabled: true,
                        timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
                        compensation_available: true,
                    },
                    1_030,
                )
                .is_empty()
        );
        assert!(
            resolver.poll_timeouts_at(1_201).is_empty(),
            "the forward timeout must not overwrite failure evidence while rollback owns the step"
        );
    }

    #[test]
    fn accepted_compensation_preserves_hard_overall_timeout() {
        let mut policy = TerminalPolicy::order_lifecycle_default();
        policy.overall_timeout = Duration::from_millis(100);
        policy.stalled_timeout = Duration::from_millis(50);
        let mut resolver = TerminalResolver::new(policy);
        let context = ctx_at("create_order", 25, 1_000, 1_000);
        assert!(
            resolver
                .ingest_at(
                    &SagaChoreographyEvent::SagaStarted {
                        context: context.clone(),
                        payload: Vec::new(),
                    },
                    1_000,
                )
                .is_empty()
        );
        assert!(
            resolver
                .ingest_at(
                    &SagaChoreographyEvent::CompensationRequested {
                        context: context.clone(),
                        failed_step: "create_order".into(),
                        reason: "create failed".into(),
                        failure: crate::SagaFailureDetails {
                            step_name: "create_order".into(),
                            participant_id: "order-manager".into(),
                            error_code: Some("exchange_rejected".into()),
                            error_message: "create failed".into(),
                            at_millis: 1_010,
                        },
                        steps_to_compensate: vec!["create_order".into()],
                    },
                    1_010,
                )
                .is_empty()
        );
        assert!(
            resolver
                .ingest_at(
                    &SagaChoreographyEvent::CompensationAccepted {
                        context: context.clone(),
                        participant_id: "order-manager".into(),
                        execution_id: StepExecutionId::new("cancel-25"),
                        deadline_at_millis: 5_000,
                        hard_deadline_at_millis: 10_000,
                    },
                    1_020,
                )
                .is_empty()
        );
        assert!(
            resolver.poll_timeouts_at(1_060).is_empty(),
            "accepted compensation should suppress only the stalled-progress timeout"
        );
        assert!(matches!(
            resolver.poll_timeouts_at(1_101).as_slice(),
            [SagaChoreographyEvent::SagaQuarantined {
                reason,
                step,
                ..
            }] if reason.contains("overall_timeout") && step.as_ref() == "create_order"
        ));
    }

    #[test]
    fn progress_timeout_resets_after_progress_event() {
        let mut required_steps = HashSet::new();
        required_steps.insert("create_order".into());
        let policy = TerminalPolicy {
            saga_type: "order_lifecycle".into(),
            policy_id: "progress-timeout".into(),
            failure_authority: FailureAuthority::AnyParticipant,
            success_criteria: SuccessCriteria::AllOf(required_steps),
            overall_timeout: Duration::from_secs(5),
            stalled_timeout: Duration::from_millis(100),
            workflow_steps: &[],
        };
        let mut resolver = TerminalResolver::new(policy);
        let start = SagaChoreographyEvent::SagaStarted {
            context: ctx_at("risk_check", 9, 1_000, 1_000),
            payload: Vec::new(),
        };
        let _ = resolver.ingest_at(&start, 1_000);

        assert!(resolver.poll_timeouts_at(1_080).is_empty());

        let progress = SagaChoreographyEvent::StepStarted {
            context: ctx_at("positions_check", 9, 1_000, 1_090),
        };
        let _ = resolver.ingest_at(&progress, 1_090);

        assert!(resolver.poll_timeouts_at(1_180).is_empty());
        let timed_out = resolver.poll_timeouts_at(1_191);
        assert!(
            matches!(
                timed_out.first(),
                Some(SagaChoreographyEvent::SagaQuarantined { reason, .. })
                if reason.as_ref().contains("stalled_timeout")
            ),
            // A started step is unresolved forward work: never an ordinary failure.
            "expected stalled-timeout quarantine, got: {timed_out:?}"
        );
    }

    #[test]
    fn stalled_timeout_reports_ready_root_blocker_and_dependency_chain() {
        let mut resolver = TerminalResolver::new(open_position_policy(Duration::from_millis(100)));
        let start = SagaChoreographyEvent::SagaStarted {
            context: ctx_at("risk_check", 10, 1_000, 1_000),
            payload: Vec::new(),
        };
        let _ = resolver.ingest_at(&start, 1_000);
        let _ = resolver.ingest_at(
            &SagaChoreographyEvent::StepCompleted {
                context: ctx_at("positions_check", 10, 1_000, 1_010),
                output: Vec::new(),
                saga_input: Vec::new(),
                compensation_available: false,
            },
            1_010,
        );
        let _ = resolver.ingest_at(
            &SagaChoreographyEvent::StepCompleted {
                context: ctx_at("universe_filter_hold", 10, 1_000, 1_020),
                output: Vec::new(),
                saga_input: Vec::new(),
                compensation_available: false,
            },
            1_020,
        );

        let timed_out = resolver.poll_timeouts_at(1_121);
        let Some(SagaChoreographyEvent::SagaFailed { reason, .. }) = timed_out.first() else {
            panic!("expected stalled timeout, got: {timed_out:?}");
        };
        assert!(
            reason.contains("missing_steps=create_order"),
            "missing terminal step was not reported: {reason}"
        );
        assert!(
            reason.contains("ready_not_started=risk_check(account-balance)"),
            "root ready step was not reported: {reason}"
        );
        assert!(
            reason.contains("book_snapshot_check(options-books)<-risk_check"),
            "intermediate dependency blocker was not reported: {reason}"
        );
        assert!(
            reason.contains("create_order(order-manager)<-book_snapshot_check"),
            "terminal dependency blocker was not reported: {reason}"
        );
        assert!(
            reason.contains("completed_steps=positions_check,universe_filter_hold"),
            "completed progress was not reported: {reason}"
        );
        assert!(
            reason.contains("started_steps=positions_check,universe_filter_hold"),
            "started progress was not reported: {reason}"
        );
    }

    #[test]
    fn stalled_timeout_reports_started_but_uncompleted_blocker() {
        let mut resolver = TerminalResolver::new(open_position_policy(Duration::from_millis(100)));
        let start = SagaChoreographyEvent::SagaStarted {
            context: ctx_at("risk_check", 11, 1_000, 1_000),
            payload: Vec::new(),
        };
        let _ = resolver.ingest_at(&start, 1_000);
        let _ = resolver.ingest_at(
            &SagaChoreographyEvent::StepStarted {
                context: ctx_at("risk_check", 11, 1_000, 1_010),
            },
            1_010,
        );

        let timed_out = resolver.poll_timeouts_at(1_111);
        let Some(SagaChoreographyEvent::SagaQuarantined { reason, .. }) = timed_out.first() else {
            panic!("expected stalled-timeout quarantine, got: {timed_out:?}");
        };
        assert!(
            reason.contains("started_not_completed=risk_check(account-balance)"),
            "started root blocker was not reported: {reason}"
        );
        assert!(
            !reason.contains("ready_not_started=risk_check(account-balance)"),
            "started blocker must not also be reported as never started: {reason}"
        );
    }

    #[test]
    fn durable_restore_reconstructs_complete_rollback_scope_before_recovered_failure() {
        let mut required = HashSet::new();
        required.insert(Box::<str>::from("finalize"));
        let policy = TerminalPolicy::new(
            "order_lifecycle".into(),
            "multi_step/test".into(),
            FailureAuthority::AnyParticipant,
            SuccessCriteria::AllOf(required),
            Duration::from_secs(3_600),
            Duration::from_secs(3_600),
            &[],
        );
        let now = SagaContext::now_millis();
        let events = vec![
            SagaChoreographyEvent::SagaStarted {
                context: ctx_at("first_effect", 41, now, now),
                payload: Vec::new(),
            },
            SagaChoreographyEvent::StepCompleted {
                context: ctx_at("first_effect", 41, now, now.saturating_add(1)),
                output: Vec::new(),
                saga_input: Vec::new(),
                compensation_available: true,
            },
            SagaChoreographyEvent::StepAccepted {
                context: ctx_at("second_effect", 41, now, now.saturating_add(2)),
                participant_id: "second-participant".into(),
                execution_id: StepExecutionId::new("external-41"),
                deadline_at_millis: now.saturating_add(30_000),
                hard_deadline_at_millis: now.saturating_add(60_000),
                timeouts_enabled: true,
                timeout_outcome: AcceptedStepTimeoutOutcome::FailStep {
                    requires_compensation: true,
                },
                compensation_available: true,
            },
        ];
        let (mut restored, unpublished) = TerminalResolver::restore_from_events(policy, &events);
        assert!(unpublished.is_empty());

        let recovered_failure = SagaChoreographyEvent::StepFailed {
            context: ctx_at("second_effect", 41, now, now.saturating_add(3)),
            participant_id: "second-participant".into(),
            error_code: Some("exchange_reject".into()),
            error: "authoritative failure recovered after restart".into(),
            requires_compensation: true,
        };
        let emitted = restored.ingest_at(&recovered_failure, now.saturating_add(3));

        assert!(
            matches!(
                emitted.as_slice(),
                [SagaChoreographyEvent::CompensationRequested {
                    steps_to_compensate,
                    ..
                }] if steps_to_compensate.as_slice() == [Box::<str>::from("second_effect")]
            ),
            "unexpected recovery output: {emitted:?}"
        );
        // The complete scope is preserved as a reverse-ordered queue: the next
        // undo is only released once the first resolves.
        let next = restored.ingest_at(
            &SagaChoreographyEvent::CompensationCompleted {
                context: ctx_at("second_effect", 41, now, now.saturating_add(4)),
            },
            now.saturating_add(4),
        );
        assert!(
            matches!(
                next.as_slice(),
                [SagaChoreographyEvent::CompensationRequested {
                    steps_to_compensate,
                    ..
                }] if steps_to_compensate.as_slice() == [Box::<str>::from("first_effect")]
            ),
            "unexpected follow-up output: {next:?}"
        );
    }

    #[test]
    fn accepted_step_without_compensation_yet_is_uncertain_at_generic_expiry() {
        let now = 1_000_000;
        let mut resolver = TerminalResolver::new(open_position_policy(Duration::from_millis(50)));
        resolver.ingest_at(
            &SagaChoreographyEvent::SagaStarted {
                context: ctx_at("risk_check", 60, now, now),
                payload: vec![],
            },
            now,
        );
        resolver.ingest_at(
            &SagaChoreographyEvent::StepAccepted {
                context: ctx_at("risk_check", 60, now, now),
                participant_id: "account-balance".into(),
                execution_id: StepExecutionId::new("exec-60"),
                deadline_at_millis: now + 3_600_000,
                hard_deadline_at_millis: now + 3_600_000,
                timeouts_enabled: true,
                timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
                compensation_available: false,
            },
            now,
        );
        let out = resolver.poll_timeouts_at(now + 1_000);
        assert!(
            matches!(
                out.as_slice(),
                [SagaChoreographyEvent::SagaQuarantined { step, participant_id, .. }]
                    if step.as_ref() == "risk_check" && participant_id.as_ref() == "account-balance"
            ),
            "unexpected output: {out:?}"
        );
    }

    #[test]
    fn restored_terminal_only_history_escalates_a_late_compensable_effect() {
        let now = 2_000_000;
        let started = SagaChoreographyEvent::SagaStarted {
            context: ctx_at("risk_check", 61, now, now),
            payload: vec![],
        };
        let failed = SagaChoreographyEvent::SagaFailed {
            context: ctx_at("saga_terminal_resolver", 61, now, now + 1),
            reason: "boom".into(),
            failure: None,
        };
        let (mut restored, unpublished) = TerminalResolver::restore_from_events(
            open_position_policy(Duration::from_secs(60)),
            &[started, failed],
        );
        assert!(unpublished.is_empty());
        let out = restored.ingest_at(
            &SagaChoreographyEvent::StepCompleted {
                context: ctx_at("risk_check", 61, now, now + 2),
                output: vec![],
                saga_input: vec![],
                compensation_available: true,
            },
            now + 2,
        );
        assert!(
            matches!(out.as_slice(), [SagaChoreographyEvent::SagaQuarantined { step, .. }] if step.as_ref() == "risk_check"),
            "unexpected output: {out:?}"
        );
    }

    // --- Review4 P2-1: a safe rejection is not an undo effect -------------

    fn accepted_potential(base: &SagaContext, step: &str) -> SagaChoreographyEvent {
        SagaChoreographyEvent::StepAccepted {
            context: base.next_step(step.into()),
            participant_id: step.into(),
            execution_id: StepExecutionId::new(format!("exec-{step}")),
            deadline_at_millis: u64::MAX / 2,
            hard_deadline_at_millis: u64::MAX / 2,
            timeouts_enabled: true,
            timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
            compensation_available: true,
        }
    }

    fn safe_rejection(base: &SagaContext, step: &str) -> SagaChoreographyEvent {
        SagaChoreographyEvent::StepFailed {
            context: base.next_step(step.into()),
            participant_id: step.into(),
            error_code: None,
            error: "safe rejection".into(),
            requires_compensation: false,
        }
    }

    fn undone(base: &SagaContext, step: &str) -> SagaChoreographyEvent {
        SagaChoreographyEvent::CompensationCompleted {
            context: base.next_step(step.into()),
        }
    }

    #[test]
    fn queued_safe_rejection_is_not_requested_and_real_sibling_undo_stays() {
        let mut resolver = TerminalResolver::new(TerminalPolicy::order_lifecycle_default());
        let base = ctx_at("a", 70, 1_000, 1_000);
        // b is only potential and queued behind the real effect a.
        let _ = resolver.ingest_at(&accepted_potential(&base, "b"), 1_001);
        let _ = resolver.ingest_at(&completed_effect(&base, "a"), 1_002);
        let first = resolver.ingest_at(&compensating_failure(&base, "x"), 1_003);
        assert!(singleton_request("a")(&first), "{first:?}");
        assert!(
            resolver
                .ingest_at(&safe_rejection(&base, "b"), 1_004)
                .is_empty()
        );
        let out = resolver.ingest_at(&undone(&base, "a"), 1_005);
        match out.as_slice() {
            [
                SagaChoreographyEvent::SagaFailed {
                    failure: Some(failure),
                    ..
                },
            ] => assert_eq!(failure.step_name.as_ref(), "x", "first failure kept"),
            other => panic!("rejected b must not be requested: {other:?}"),
        }
    }

    #[test]
    fn requested_undo_stays_owned_after_a_later_safe_rejection() {
        let mut resolver = TerminalResolver::new(TerminalPolicy::order_lifecycle_default());
        let base = ctx_at("a", 71, 1_000, 1_000);
        let _ = resolver.ingest_at(&completed_effect(&base, "a"), 1_001);
        let _ = resolver.ingest_at(&accepted_potential(&base, "b"), 1_002);
        let first = resolver.ingest_at(&compensating_failure(&base, "x"), 1_003);
        assert!(singleton_request("b")(&first), "{first:?}");
        assert!(
            resolver
                .ingest_at(&safe_rejection(&base, "b"), 1_004)
                .is_empty()
        );
        let next = resolver.ingest_at(&undone(&base, "b"), 1_005);
        assert!(singleton_request("a")(&next), "{next:?}");
        let done = resolver.ingest_at(&undone(&base, "a"), 1_006);
        assert!(matches!(
            done.as_slice(),
            [SagaChoreographyEvent::SagaFailed { .. }]
        ));
    }

    #[test]
    fn completed_effect_survives_a_queued_false_rejection() {
        let mut resolver = TerminalResolver::new(TerminalPolicy::order_lifecycle_default());
        let base = ctx_at("a", 72, 1_000, 1_000);
        let _ = resolver.ingest_at(&accepted_potential(&base, "b"), 1_001);
        let _ = resolver.ingest_at(&completed_effect(&base, "b"), 1_002);
        let _ = resolver.ingest_at(&completed_effect(&base, "a"), 1_003);
        let first = resolver.ingest_at(&compensating_failure(&base, "x"), 1_004);
        assert!(singleton_request("a")(&first), "{first:?}");
        let _ = resolver.ingest_at(&safe_rejection(&base, "b"), 1_005);
        let next = resolver.ingest_at(&undone(&base, "a"), 1_006);
        assert!(singleton_request("b")(&next), "{next:?}");
    }
}
