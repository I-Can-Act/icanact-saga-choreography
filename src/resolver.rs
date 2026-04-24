use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use crate::{
    SagaChoreographyEvent, SagaContext, SagaFailureDetails, SagaId, SagaWorkflowStepContract,
    WorkflowDependencySpec,
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
    compensation_requested: bool,
    pending_compensation_steps: HashSet<Box<str>>,
    pending_failure: Option<SagaFailureDetails>,
    started_at_millis: u64,
    last_progress_at_millis: u64,
    last_context: SagaContext,
    terminal_latched: bool,
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
            compensation_requested: false,
            pending_compensation_steps: HashSet::new(),
            pending_failure: None,
            started_at_millis,
            last_progress_at_millis: progress_at_millis,
            last_context: seed_context.clone(),
            terminal_latched: false,
        }
    }
}

#[derive(Debug)]
pub struct TerminalResolver {
    policy: TerminalPolicy,
    states: HashMap<SagaId, SagaResolutionState>,
    terminal_latched_order: VecDeque<SagaId>,
    terminal_latched_set: HashSet<SagaId>,
    terminal_latch_retention: usize,
}

impl TerminalResolver {
    pub fn new(policy: TerminalPolicy) -> Self {
        Self {
            policy,
            states: HashMap::new(),
            terminal_latched_order: VecDeque::new(),
            terminal_latched_set: HashSet::new(),
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

    fn ingest_at(
        &mut self,
        event: &SagaChoreographyEvent,
        now_millis: u64,
    ) -> Vec<SagaChoreographyEvent> {
        if event.context().saga_type.as_ref() != self.policy.saga_type.as_ref() {
            return Vec::new();
        }

        let saga_id = event.context().saga_id;
        let mut out = Vec::new();
        let mut should_latch_terminal = false;
        let state = self
            .states
            .entry(saga_id)
            .or_insert_with(|| SagaResolutionState::new(event.context(), now_millis));

        if state.terminal_latched {
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
                state.started_steps.insert(step_name.clone());
                state.completed_steps.insert(step_name.clone());
                if *compensation_available
                    && !state
                        .compensable_steps
                        .iter()
                        .any(|step| step == &step_name)
                {
                    state.compensable_steps.push(step_name);
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
            SagaChoreographyEvent::StepFailed {
                context,
                participant_id,
                error_code,
                error,
                requires_compensation,
            } => {
                state.started_steps.insert(context.step_name.clone());
                state.failed_steps.insert(context.step_name.clone());
                if !self
                    .policy
                    .failure_authority
                    .is_authorized(context.step_name.as_ref())
                {
                    return out;
                }

                let failure = SagaFailureDetails {
                    step_name: context.step_name.clone(),
                    participant_id: participant_id.clone(),
                    error_code: error_code.clone(),
                    error_message: error.clone(),
                    at_millis: context.event_timestamp_millis,
                };

                if *requires_compensation {
                    state.pending_failure = Some(failure.clone());
                    if !state.compensation_requested {
                        let steps_to_compensate: Vec<Box<str>> =
                            state.compensable_steps.iter().rev().cloned().collect();
                        state.pending_compensation_steps =
                            steps_to_compensate.iter().cloned().collect();
                        state.compensation_requested = true;

                        out.push(SagaChoreographyEvent::CompensationRequested {
                            context: terminal_context(context),
                            failed_step: context.step_name.clone(),
                            reason: error.clone(),
                            steps_to_compensate,
                        });
                    }

                    if state.pending_compensation_steps.is_empty() {
                        out.push(SagaChoreographyEvent::SagaFailed {
                            context: terminal_context(context),
                            reason: "step failed and no compensations were pending".into(),
                            failure: Some(failure),
                        });
                        state.terminal_latched = true;
                    }
                } else {
                    out.push(SagaChoreographyEvent::SagaFailed {
                        context: terminal_context(context),
                        reason: error.clone(),
                        failure: Some(failure),
                    });
                    state.terminal_latched = true;
                }
            }
            SagaChoreographyEvent::CompensationCompleted { context } => {
                if state.compensation_requested {
                    state
                        .pending_compensation_steps
                        .remove(context.step_name.as_ref());
                    if state.pending_compensation_steps.is_empty() {
                        let failure = state.pending_failure.clone();
                        let reason: Box<str> = failure
                            .as_ref()
                            .map(|f| {
                                format!(
                                    "compensation finished after failure at step={}",
                                    f.step_name
                                )
                            })
                            .unwrap_or_else(|| "compensation finished".to_string())
                            .into();
                        out.push(SagaChoreographyEvent::SagaFailed {
                            context: terminal_context(context),
                            reason,
                            failure,
                        });
                        state.terminal_latched = true;
                    }
                }
            }
            SagaChoreographyEvent::CompensationFailed {
                context,
                participant_id,
                error,
                is_ambiguous,
            } => {
                if *is_ambiguous {
                    out.push(SagaChoreographyEvent::SagaQuarantined {
                        context: terminal_context(context),
                        reason: error.clone(),
                        step: context.step_name.clone(),
                        participant_id: participant_id.clone(),
                    });
                } else {
                    let failure = state.pending_failure.clone();
                    out.push(SagaChoreographyEvent::SagaFailed {
                        context: terminal_context(context),
                        reason: error.clone(),
                        failure,
                    });
                }
                state.terminal_latched = true;
            }
            SagaChoreographyEvent::SagaCompleted { .. }
            | SagaChoreographyEvent::SagaFailed { .. }
            | SagaChoreographyEvent::SagaQuarantined { .. }
            | SagaChoreographyEvent::CompensationRequested { .. }
            | SagaChoreographyEvent::CompensationStarted { .. } => {}
        }

        if !state.terminal_latched {
            if let Some(timeout_event) = timeout_terminal_event(&self.policy, state, now_millis) {
                out.push(timeout_event);
                state.terminal_latched = true;
            }
        }

        if state.terminal_latched {
            should_latch_terminal = true;
        }

        if should_latch_terminal {
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
            if let Some(timeout_event) = timeout_terminal_event(&self.policy, state, now_millis) {
                state.terminal_latched = true;
                newly_latched.push(*saga_id);
                out.push(timeout_event);
            }
        }
        for saga_id in newly_latched {
            self.latch_terminal(saga_id);
        }
        out
    }

    fn latch_terminal(&mut self, saga_id: SagaId) {
        if !self.terminal_latched_set.insert(saga_id) {
            return;
        }

        self.terminal_latched_order.push_back(saga_id);
        while self.terminal_latched_order.len() > self.terminal_latch_retention {
            let Some(evicted) = self.terminal_latched_order.pop_front() else {
                break;
            };
            self.terminal_latched_set.remove(&evicted);
            self.states.remove(&evicted);
        }
    }
}

fn terminal_latch_retention_limit() -> usize {
    match std::env::var("SAGA_TERMINAL_LATCH_RETENTION") {
        Ok(raw) => match raw.parse::<usize>() {
            Ok(parsed) if parsed > 0 => parsed,
            _ => 4096,
        },
        Err(_) => 4096,
    }
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
            | SagaChoreographyEvent::StepAck { .. }
            | SagaChoreographyEvent::StepCompleted { .. }
            | SagaChoreographyEvent::StepFailed { .. }
            | SagaChoreographyEvent::CompensationRequested { .. }
            | SagaChoreographyEvent::CompensationStarted { .. }
            | SagaChoreographyEvent::CompensationCompleted { .. }
            | SagaChoreographyEvent::CompensationFailed { .. }
    )
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

fn timeout_terminal_event(
    policy: &TerminalPolicy,
    state: &SagaResolutionState,
    now_millis: u64,
) -> Option<SagaChoreographyEvent> {
    let elapsed_ms = now_millis.saturating_sub(state.started_at_millis);
    let overall_timeout_ms = policy.overall_timeout.as_millis() as u64;
    if elapsed_ms > overall_timeout_ms {
        let diagnostic = timeout_diagnostics(policy, state);
        emit_timeout_diagnostic(policy, state, "overall_timeout", &diagnostic);
        let reason = diagnostic.reason(
            format!("overall_timeout after {overall_timeout_ms}ms"),
            &policy.policy_id,
        );
        return Some(SagaChoreographyEvent::SagaFailed {
            context: terminal_context_at(&state.last_context, now_millis),
            reason: reason.into(),
            failure: None,
        });
    }

    let stalled_ms = now_millis.saturating_sub(state.last_progress_at_millis);
    let stalled_timeout_ms = policy.stalled_timeout.as_millis() as u64;
    if stalled_ms > stalled_timeout_ms {
        let diagnostic = timeout_diagnostics(policy, state);
        emit_timeout_diagnostic(policy, state, "stalled_timeout", &diagnostic);
        let reason = diagnostic.reason(
            format!("stalled_timeout after {stalled_timeout_ms}ms without progress"),
            &policy.policy_id,
        );
        return Some(SagaChoreographyEvent::SagaFailed {
            context: terminal_context_at(&state.last_context, now_millis),
            reason: reason.into(),
            failure: None,
        });
    }

    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use crate::{
        SagaChoreographyEvent, SagaContext, SagaId, SagaWorkflowStepContract,
        WorkflowDependencySpec,
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
            saga_started_at_millis: SagaContext::now_millis(),
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
                Some(SagaChoreographyEvent::SagaFailed { reason, .. })
                if reason.as_ref().contains("stalled_timeout")
            ),
            "expected stalled-timeout failure, got: {timed_out:?}"
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
        let Some(SagaChoreographyEvent::SagaFailed { reason, .. }) = timed_out.first() else {
            panic!("expected stalled timeout, got: {timed_out:?}");
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
}
