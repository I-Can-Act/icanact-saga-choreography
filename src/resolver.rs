use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use tracing::{error, warn};

use crate::{
    AcceptedStepTimeoutOutcome, KnownRuns, ReplayHorizon, RunAdmission, RunIdentityError, RunKey,
    RunStatus, SagaChoreographyEvent, SagaContext, SagaFailureDetails, SagaWorkflowStepContract,
    StepExecutionId, WorkflowDependencySpec, admit_run,
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

/// Default number of re-requests after `CompensationFailedRetryable` (ADR-0004).
pub const DEFAULT_COMPENSATION_RETRY_LIMIT: u32 = 3;

/// What happens to completed effects of losing AnyOf/Quorum branches (ADR-0004).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LoserPolicy {
    /// Undo completed compensable losing-branch steps before completion (ADR-0004).
    #[default]
    Compensate,
    /// Keep losing-branch effects and record them (ADR-0004).
    Keep,
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct TerminalPolicy {
    pub saga_type: Box<str>,
    pub policy_id: Box<str>,
    pub failure_authority: FailureAuthority,
    pub success_criteria: SuccessCriteria,
    /// Hard wall-clock budget for forward progress, measured from saga start.
    /// Rollback gets its own budget of the same length, measured from when
    /// rollback began, so a run can last up to `overall_timeout` plus that
    /// rollback budget (about 2x) before it is quarantined.
    pub overall_timeout: Duration,
    /// Progress watchdog budget measured since last observed progress event.
    /// This window resets on each non-terminal participant progress event.
    pub stalled_timeout: Duration,
    /// Declared workflow graph used to diagnose stalled required paths.
    pub workflow_steps: &'static [SagaWorkflowStepContract],
    compensation_retry_limit: u32,
    replay_horizon: Option<ReplayHorizon>,
    loser_policy: LoserPolicy,
}

impl TerminalPolicy {
    /// Builds a terminal policy for one saga type from its workflow contract (ADR-0004).
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
            compensation_retry_limit: DEFAULT_COMPENSATION_RETRY_LIMIT,
            replay_horizon: None,
            loser_policy: LoserPolicy::Compensate,
        }
    }

    /// Maximum re-requests after `CompensationFailedRetryable` (ADR-0004).
    pub fn with_compensation_retry_limit(mut self, limit: u32) -> Self {
        self.compensation_retry_limit = limit;
        self
    }

    /// Explicit replay horizon for finalized-run tombstones (ADR-0001).
    pub fn with_replay_horizon(mut self, horizon: ReplayHorizon) -> Self {
        self.replay_horizon = Some(horizon);
        self
    }

    /// Loser policy for AnyOf/Quorum groups (ADR-0004).
    pub fn with_loser_policy(mut self, policy: LoserPolicy) -> Self {
        self.loser_policy = policy;
        self
    }

    /// Maximum re-requests after `CompensationFailedRetryable` (ADR-0004).
    pub fn compensation_retry_limit(&self) -> u32 {
        self.compensation_retry_limit
    }

    /// Explicit horizon, else `max(2 × overall_timeout, MIN_REPLAY_HORIZON)` (ADR-0001).
    pub fn replay_horizon(&self) -> ReplayHorizon {
        self.replay_horizon
            .unwrap_or_else(|| ReplayHorizon::for_overall_timeout(self.overall_timeout))
    }

    /// Loser policy for AnyOf/Quorum groups (ADR-0004).
    pub fn loser_policy(&self) -> LoserPolicy {
        self.loser_policy
    }
}

/// Reasons a [`TerminalPolicy`] can never resolve (or resolves vacuously).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalPolicyError {
    EmptyAllOf,
    EmptyAnyOf,
    EmptyQuorumGroup,
    ZeroQuorum,
    QuorumExceedsGroup,
    ZeroOverallTimeout,
    ZeroStalledTimeout,
    ReplayHorizonShorterThanOverallTimeout,
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
            Self::ReplayHorizonShorterThanOverallTimeout => {
                "replay_horizon is shorter than overall_timeout"
            }
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
        if self
            .replay_horizon
            .is_some_and(|horizon| horizon.get() < self.overall_timeout)
        {
            return Err(TerminalPolicyError::ReplayHorizonShorterThanOverallTimeout);
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

/// Lifecycle phase of one run (ADR-0004 §2.1). Forward success is decided
/// only in `Running`; `Aborting` is a rollback in progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResolverPhase {
    Running,
    Aborting,
    Terminal,
    Quarantined,
}

#[derive(Clone, Debug)]
struct SagaResolutionState {
    phase: ResolverPhase,
    /// Effects that completed during rollback and cannot be undone.
    unresolved: Vec<Box<str>>,
    /// Set for internal aborts (timeouts, `SagaAbortRequested`): the settled
    /// `SagaFailed` carries this reason and no step failure.
    abort_reason: Option<Box<str>>,
    /// When rollback began; rollback timeouts are measured from here.
    aborting_since_millis: Option<u64>,
    started_steps: HashSet<Box<str>>,
    acked_steps: HashSet<Box<str>>,
    completed_steps: HashSet<Box<str>>,
    failed_steps: HashSet<Box<str>>,
    compensable_steps: Vec<Box<str>>,
    pending_compensation_steps: HashSet<Box<str>>,
    completed_compensation_steps: HashSet<Box<str>>,
    /// Steps whose `CompensationStarted` was observed (duplicate detection).
    started_compensation_steps: HashSet<Box<str>>,
    pending_failure: Option<SagaFailureDetails>,
    accepted_steps: HashMap<Box<str>, AcceptedStepResolverState>,
    accepted_compensations: HashMap<Box<str>, AcceptedCompensationResolverState>,
    started_at_millis: u64,
    /// Receive-time wall-clock mark of the last *novel* progress. Never moves
    /// backwards; duplicates/replays and regressing clocks do not refresh it.
    last_progress_at_millis: u64,
    last_context: SagaContext,
    terminal_latched: bool,
    /// Event type of the first terminal outcome latched for this saga.
    terminal_outcome: Option<&'static str>,
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
        // Stall window starts at receive time; a skewed/future-dated start
        // stamp must not extend it.
        let progress_at_millis = now_millis;
        Self {
            completed_steps: HashSet::new(),
            started_steps: HashSet::new(),
            acked_steps: HashSet::new(),
            failed_steps: HashSet::new(),
            compensable_steps: Vec::new(),
            phase: ResolverPhase::Running,
            unresolved: Vec::new(),
            abort_reason: None,
            aborting_since_millis: None,
            pending_compensation_steps: HashSet::new(),
            completed_compensation_steps: HashSet::new(),
            started_compensation_steps: HashSet::new(),
            pending_failure: None,
            accepted_steps: HashMap::new(),
            accepted_compensations: HashMap::new(),
            started_at_millis,
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
    states: HashMap<RunKey, SagaResolutionState>,
    terminal_latched_order: VecDeque<RunKey>,
    terminal_latched_set: HashSet<RunKey>,
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

    /// Wrapper over [`Self::try_ingest_at`]: a rejected run is logged with its
    /// `RunKey` and produces no events (no event is safe for a stale/expired run).
    fn ingest_at(
        &mut self,
        event: &SagaChoreographyEvent,
        now_millis: u64,
    ) -> Vec<SagaChoreographyEvent> {
        match self.try_ingest_at(event, now_millis) {
            Ok(events) => events,
            Err(error) => {
                warn!(
                    event = "saga_run_identity_rejected",
                    run = %event.context().run_key(),
                    %error,
                    "terminal resolver rejected event"
                );
                Vec::new()
            }
        }
    }

    fn run_status(&self, run: &RunKey) -> RunStatus {
        if self.terminal_latched_set.contains(run) {
            return RunStatus::Terminal;
        }
        match self.states.get(run) {
            Some(state) if state.terminal_latched => RunStatus::Terminal,
            Some(_) => RunStatus::Active,
            None => RunStatus::Unknown,
        }
    }

    fn known_runs(&self, run: &RunKey) -> KnownRuns {
        let mut known = KnownRuns::default();
        let latched = self.terminal_latched_set.iter().map(|key| (key, true));
        let live = self
            .states
            .iter()
            .map(|(key, state)| (key, !state.terminal_latched));
        for (key, active) in latched.chain(live) {
            if key == run || key.saga_id() != run.saga_id() {
                continue;
            }
            known.newest = known.newest.max(Some(key.incarnation()));
            if active && !self.terminal_latched_set.contains(key) {
                known.other_active += 1;
            }
        }
        known
    }

    /// Checked ingress (ADR-0001 §2.2): `Err` for a stale or expired run.
    pub fn try_ingest_at(
        &mut self,
        event: &SagaChoreographyEvent,
        now_millis: u64,
    ) -> Result<Vec<SagaChoreographyEvent>, RunIdentityError> {
        if event.context().saga_type.as_ref() != self.policy.saga_type.as_ref() {
            return Ok(Vec::new());
        }

        let run = event.context().run_key();
        let exact = self.run_status(&run);
        let known = if exact == RunStatus::Unknown {
            self.known_runs(&run)
        } else {
            KnownRuns::default()
        };
        let cutoff = self.policy.replay_horizon().cutoff(now_millis);
        let is_start = matches!(event, SagaChoreographyEvent::SagaStarted { .. });
        match admit_run(exact, known, &run, is_start, cutoff)? {
            RunAdmission::NewRun { concurrent: true } => warn!(
                event = "saga_concurrent_run_admitted",
                run = %run,
                other_active = known.other_active,
                "admitted a newer run while another run of the saga id is active"
            ),
            RunAdmission::DuplicateStart => return Ok(Vec::new()),
            RunAdmission::TerminalRun => {
                warn_if_contradictory_terminal(self.states.get(&run), event);
                return Ok(Vec::new());
            }
            RunAdmission::NewRun { .. } | RunAdmission::CurrentRun => {}
        }
        Ok(self.apply_event(run, event, now_millis))
    }

    fn apply_event(
        &mut self,
        run: RunKey,
        event: &SagaChoreographyEvent,
        now_millis: u64,
    ) -> Vec<SagaChoreographyEvent> {
        let mut out = Vec::new();
        let mut should_latch_terminal = false;
        let state = self
            .states
            .entry(run.clone())
            .or_insert_with(|| SagaResolutionState::new(event.context(), now_millis));

        if state.terminal_latched {
            warn_if_contradictory_terminal(Some(state), event);
            return Vec::new();
        }

        state.last_context = event.context().clone();
        // Only novel progress refreshes the stall clock, and the mark never
        // moves backwards (duplicates/replays and regressing clocks are inert).
        if is_progress_event(event) && is_novel_progress(state, event) {
            state.last_progress_at_millis = state.last_progress_at_millis.max(now_millis);
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
                if state.phase == ResolverPhase::Aborting
                    && state.pending_compensation_steps.contains(&step_name)
                {
                    return out;
                }
                state.started_steps.insert(step_name.clone());
                if state.phase == ResolverPhase::Aborting && *compensation_available {
                    // An accepted compensable step may produce an effect: it is
                    // owed an undo exactly like a late completion.
                    apply_late_completion(state, context, step_name, true, &mut out);
                    return out;
                }
                if *compensation_available
                    && !state
                        .compensable_steps
                        .iter()
                        .any(|candidate| candidate == &step_name)
                {
                    state.compensable_steps.push(step_name.clone());
                }
                // A heartbeat of the same execution never shortens a stored deadline.
                let (deadline_at_millis, hard_deadline_at_millis) =
                    match state.accepted_steps.get(step_name.as_ref()) {
                        Some(known) if known.execution_id == *execution_id => (
                            known.deadline_at_millis.max(*deadline_at_millis),
                            known.hard_deadline_at_millis.max(*hard_deadline_at_millis),
                        ),
                        _ => (*deadline_at_millis, *hard_deadline_at_millis),
                    };
                state.accepted_steps.insert(
                    step_name,
                    AcceptedStepResolverState {
                        participant_id: participant_id.clone(),
                        execution_id: execution_id.clone(),
                        deadline_at_millis,
                        hard_deadline_at_millis,
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
                if state.phase == ResolverPhase::Aborting
                    && state.pending_compensation_steps.contains(&step_name)
                {
                    return out;
                }
                if state.phase == ResolverPhase::Aborting
                    && state.completed_steps.contains(&step_name)
                {
                    // Redelivery of an effect already accounted for.
                    return out;
                }
                state.accepted_steps.remove(step_name.as_ref());
                state.started_steps.insert(step_name.clone());
                state.completed_steps.insert(step_name.clone());
                if state.phase == ResolverPhase::Aborting {
                    // Late forward effect during rollback: never a success.
                    apply_late_completion(
                        state,
                        context,
                        step_name,
                        *compensation_available,
                        &mut out,
                    );
                    return out;
                }
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
            SagaChoreographyEvent::CompensationCompleted { context } => {
                state
                    .accepted_compensations
                    .remove(context.step_name.as_ref());
                state
                    .completed_compensation_steps
                    .insert(context.step_name.clone());
                // A successful (retried) undo resolves an earlier retryable failure.
                state.unresolved.retain(|step| step != &context.step_name);
                if state.phase == ResolverPhase::Aborting {
                    state
                        .pending_compensation_steps
                        .remove(context.step_name.as_ref());
                    settle(state, context, None, &mut out);
                }
            }
            SagaChoreographyEvent::CompensationFailed {
                context,
                participant_id,
                error,
                is_ambiguous,
            } => {
                state
                    .accepted_compensations
                    .remove(context.step_name.as_ref());
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
            SagaChoreographyEvent::CompensationRequested {
                failure,
                steps_to_compensate,
                failed_step,
                reason,
                ..
            } => {
                if state.phase == ResolverPhase::Running
                    && failed_step.as_ref() == TERMINAL_RESOLVER_STEP
                {
                    // Replay of a journaled internal abort: rebuild what
                    // `begin_internal_abort` set so settlement matches live.
                    state.abort_reason = Some(reason.clone());
                }
                for step in steps_to_compensate {
                    state.accepted_steps.remove(step.as_ref());
                }
                let newly_pending: Vec<Box<str>> = steps_to_compensate
                    .iter()
                    .filter(|step| !state.completed_compensation_steps.contains(*step))
                    .cloned()
                    .collect();
                if state.phase == ResolverPhase::Aborting {
                    // Per-step requests (late effects) extend the owed set.
                    state.pending_compensation_steps.extend(newly_pending);
                } else {
                    state.pending_compensation_steps = newly_pending.into_iter().collect();
                }
                state.phase = ResolverPhase::Aborting;
                state.aborting_since_millis.get_or_insert(now_millis);
                state.pending_failure = Some(failure.clone());
            }
            SagaChoreographyEvent::SagaCompleted { .. }
            | SagaChoreographyEvent::SagaFailed { .. }
            | SagaChoreographyEvent::SagaQuarantined { .. } => {
                // An authoritative terminal event is absorbing: latch before
                // any timeout evaluation can emit a competing outcome.
                state.terminal_latched = true;
                state.terminal_outcome = Some(event.event_type());
            }
            SagaChoreographyEvent::CompensationStarted { context } => {
                state
                    .started_compensation_steps
                    .insert(context.step_name.clone());
            }
            SagaChoreographyEvent::SagaEffectsRetained { .. } => {}
            SagaChoreographyEvent::SagaAbortRequested {
                context,
                reason,
                source,
            } => {
                if state.phase == ResolverPhase::Running {
                    begin_internal_abort(
                        state,
                        terminal_context(context),
                        &format!("{source:?}"),
                        reason.clone(),
                        now_millis,
                        context.event_timestamp_millis,
                        &mut out,
                    );
                } else {
                    warn!(
                        event = "saga_abort_requested_while_rolling_back",
                        run = %context.run_key(),
                        reason = %reason,
                        "abort request recorded; rollback already in progress"
                    );
                }
            }
            SagaChoreographyEvent::CompensationFailedRetryable { context, error, .. } => {
                state
                    .accepted_compensations
                    .remove(context.step_name.as_ref());
                // The undo never ran: the effect is still live. Never a
                // `SagaFailed` (interim until bounded retries exist).
                error!(
                    event = "saga_compensation_failed_retryable",
                    run = %context.run_key(),
                    step = %context.step_name,
                    %error,
                    "compensation failed; effect remains unresolved"
                );
                if !state.unresolved.contains(&context.step_name) {
                    state.unresolved.push(context.step_name.clone());
                }
                if state.phase == ResolverPhase::Aborting {
                    state
                        .pending_compensation_steps
                        .remove(context.step_name.as_ref());
                    settle(state, context, None, &mut out);
                }
            }
        }

        if state.terminal_latched && state.terminal_outcome.is_none() {
            state.terminal_outcome = out
                .iter()
                .find(|e| is_terminal_event(e))
                .map(SagaChoreographyEvent::event_type);
        }
        sync_terminal_phase(state);

        if !state.terminal_latched {
            let before = out.len();
            out.extend(timeout_events(&self.policy, state, now_millis));
            if state.terminal_latched {
                state.terminal_outcome = out[before..]
                    .iter()
                    .find(|e| is_terminal_event(e))
                    .map(SagaChoreographyEvent::event_type);
                sync_terminal_phase(state);
            }
        }

        if state.terminal_latched {
            should_latch_terminal = true;
        }

        if should_latch_terminal {
            self.latch_terminal(run);
        }

        out
    }

    fn poll_timeouts_at(&mut self, now_millis: u64) -> Vec<SagaChoreographyEvent> {
        let mut out = Vec::new();
        let mut newly_latched = Vec::new();
        for (run, state) in self.states.iter_mut() {
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
                sync_terminal_phase(state);
                newly_latched.push(run.clone());
            }
        }
        for run in newly_latched {
            self.latch_terminal(run);
        }
        out
    }

    fn latch_terminal(&mut self, run: RunKey) {
        if !self.terminal_latched_set.insert(run.clone()) {
            return;
        }

        self.terminal_latched_order.push_back(run);
        while self.terminal_latched_order.len() > self.terminal_latch_retention {
            let Some(evicted) = self.terminal_latched_order.pop_front() else {
                break;
            };
            self.states.remove(&evicted);
        }
    }
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

/// Whether a progress-shaped event changes resolver state; replays of already
/// observed progress must not extend the stall deadline.
fn is_novel_progress(state: &SagaResolutionState, event: &SagaChoreographyEvent) -> bool {
    match event {
        SagaChoreographyEvent::SagaStarted { .. } => false,
        SagaChoreographyEvent::StepStarted { context } => {
            !state.started_steps.contains(&context.step_name)
        }
        SagaChoreographyEvent::StepAck { context, .. } => {
            !state.acked_steps.contains(&context.step_name)
        }
        SagaChoreographyEvent::StepCompleted { context, .. } => {
            !state.completed_steps.contains(&context.step_name)
        }
        SagaChoreographyEvent::StepFailed { context, .. } => {
            !state.failed_steps.contains(&context.step_name)
        }
        SagaChoreographyEvent::StepAccepted {
            context,
            execution_id,
            deadline_at_millis,
            hard_deadline_at_millis,
            ..
        } => state
            .accepted_steps
            .get(context.step_name.as_ref())
            .is_none_or(|known| {
                known.execution_id != *execution_id
                    || known.deadline_at_millis < *deadline_at_millis
                    || known.hard_deadline_at_millis < *hard_deadline_at_millis
            }),
        SagaChoreographyEvent::CompensationAccepted {
            context,
            execution_id,
            deadline_at_millis,
            hard_deadline_at_millis,
            ..
        } => state
            .accepted_compensations
            .get(context.step_name.as_ref())
            .is_none_or(|known| {
                known.execution_id != *execution_id
                    || known.deadline_at_millis != *deadline_at_millis
                    || known.hard_deadline_at_millis != *hard_deadline_at_millis
            }),
        SagaChoreographyEvent::CompensationCompleted { context } => !state
            .completed_compensation_steps
            .contains(&context.step_name),
        SagaChoreographyEvent::CompensationRequested {
            steps_to_compensate,
            ..
        } => {
            state.phase != ResolverPhase::Aborting
                || steps_to_compensate.iter().any(|step| {
                    !state.pending_compensation_steps.contains(step)
                        && !state.completed_compensation_steps.contains(step)
                })
        }
        SagaChoreographyEvent::CompensationStarted { context } => !state
            .started_compensation_steps
            .contains(&context.step_name),
        SagaChoreographyEvent::CompensationFailedRetryable { context, .. } => {
            !state.unresolved.contains(&context.step_name)
        }
        // `CompensationFailed` latches the run on first sight, so a second one
        // never reaches here; everything else is fresh progress.
        _ => true,
    }
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
            | SagaChoreographyEvent::CompensationFailedRetryable { .. }
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
    timeout_abort_events(policy, state, now_millis)
}

fn accepted_compensation_overall_timeout_event(
    policy: &TerminalPolicy,
    state: &SagaResolutionState,
    now_millis: u64,
) -> Option<SagaChoreographyEvent> {
    let overall_timeout_ms = policy.overall_timeout.as_millis() as u64;
    if now_millis.saturating_sub(state.started_at_millis) <= overall_timeout_ms {
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
                if !policy.failure_authority.is_authorized(step_name.as_ref()) {
                    continue;
                }
                state.started_steps.insert(step_name.clone());
                state.failed_steps.insert(step_name);
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

fn sync_terminal_phase(state: &mut SagaResolutionState) {
    if !state.terminal_latched {
        return;
    }
    state.phase = if state.terminal_outcome == Some("saga_quarantined") {
        ResolverPhase::Quarantined
    } else {
        ResolverPhase::Terminal
    };
}

/// A forward `StepCompleted` that lands while rolling back. Compensable
/// effects become owed an undo; the rest stay `unresolved` until settlement
/// quarantines the run. Never evaluates success criteria.
fn apply_late_completion(
    state: &mut SagaResolutionState,
    context: &SagaContext,
    step_name: Box<str>,
    compensation_available: bool,
    out: &mut Vec<SagaChoreographyEvent>,
) {
    let failure = state.pending_failure.clone();
    match failure {
        Some(failure) if compensation_available => {
            if state.completed_compensation_steps.contains(&step_name) {
                return;
            }
            if !state.compensable_steps.contains(&step_name) {
                state.compensable_steps.push(step_name.clone());
            }
            state.pending_compensation_steps.insert(step_name.clone());
            out.push(SagaChoreographyEvent::CompensationRequested {
                context: terminal_context(context),
                failed_step: failure.step_name.clone(),
                reason: "late_effect_during_rollback".into(),
                failure,
                steps_to_compensate: vec![step_name],
            });
        }
        _ => {
            error!(
                event = "saga_late_effect_not_compensable",
                run = %context.run_key(),
                step = %step_name,
                "effect completed during rollback and cannot be undone"
            );
            if !state.unresolved.contains(&step_name) {
                state.unresolved.push(step_name);
            }
        }
    }
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

    if state.phase == ResolverPhase::Aborting {
        // Rollback in progress: evidence only. No terminal, and the recorded
        // cause of the rollback is not overwritten.
        state.unresolved.retain(|step| step != &context.step_name);
        warn!(
            event = "saga_step_failed_while_rolling_back",
            run = %context.run_key(),
            step = %context.step_name,
            %error,
            "step failure recorded as evidence; rollback continues"
        );
        return;
    }

    if requires_compensation {
        state.pending_failure = Some(failure.clone());
        let steps_to_compensate: Vec<Box<str>> =
            state.compensable_steps.iter().rev().cloned().collect();
        for step in &steps_to_compensate {
            state.accepted_steps.remove(step.as_ref());
        }
        state.pending_compensation_steps = steps_to_compensate.iter().cloned().collect();
        state.phase = ResolverPhase::Aborting;
        state
            .aborting_since_millis
            .get_or_insert(context.event_timestamp_millis);

        out.push(SagaChoreographyEvent::CompensationRequested {
            context: terminal_context(context),
            failed_step: context.step_name.clone(),
            reason: error.clone(),
            failure,
            steps_to_compensate,
        });

        settle(
            state,
            context,
            Some("step failed and no compensations were pending"),
            out,
        );
    } else {
        out.push(SagaChoreographyEvent::SagaFailed {
            context: terminal_context(context),
            reason: error,
            failure: Some(failure),
        });
        state.terminal_latched = true;
    }
}

/// Steps that started/acked/were accepted and have neither completed, failed
/// nor been undone: their effect on the world is unknown.
fn in_flight_steps(state: &SagaResolutionState) -> Vec<Box<str>> {
    let mut steps: Vec<Box<str>> = state
        .started_steps
        .iter()
        .chain(state.acked_steps.iter())
        .chain(state.accepted_steps.keys())
        .filter(|step| {
            !state.completed_steps.contains(*step)
                && !state.failed_steps.contains(*step)
                && !state.completed_compensation_steps.contains(*step)
        })
        .cloned()
        .collect();
    steps.sort();
    steps.dedup();
    steps
}

/// Single settlement point for a rollback (ADR-0004 §2.3). With nothing left
/// owed: any unresolved effect or in-flight step quarantines the run, else it
/// fails. A no-op while undo is still pending or once latched.
fn settle(
    state: &mut SagaResolutionState,
    context: &SagaContext,
    clean_reason: Option<&str>,
    out: &mut Vec<SagaChoreographyEvent>,
) {
    if state.terminal_latched || !state.pending_compensation_steps.is_empty() {
        return;
    }
    let mut stuck: Vec<Box<str>> = state.unresolved.clone();
    stuck.extend(in_flight_steps(state));
    stuck.sort();
    stuck.dedup();
    if !stuck.is_empty() {
        let steps = stuck.join(", ");
        error!(
            event = "saga_quarantined_unresolved_effects",
            run = %context.run_key(),
            steps = %steps,
            "rollback settled with effects that are unresolved or in flight"
        );
        latch_event(
            state,
            out,
            SagaChoreographyEvent::SagaQuarantined {
                context: terminal_context(context),
                reason: match state.abort_reason.as_deref() {
                    Some(abort) => {
                        format!("{abort}; rollback settled with unresolved effects: {steps}")
                    }
                    None => format!("rollback settled with unresolved effects: {steps}"),
                }
                .into(),
                step: stuck[0].clone(),
                participant_id: TERMINAL_RESOLVER_STEP.into(),
            },
        );
        return;
    }
    let (failure, reason) = match state.abort_reason.clone() {
        Some(reason) => (None, reason),
        None => {
            let failure = state.pending_failure.clone();
            let reason: Box<str> = match (clean_reason, failure.as_ref()) {
                (Some(reason), _) => reason.into(),
                (None, Some(f)) => format!(
                    "compensation finished after failure at step={}",
                    f.step_name
                )
                .into(),
                (None, None) => "compensation finished".into(),
            };
            (failure, reason)
        }
    };
    latch_event(
        state,
        out,
        SagaChoreographyEvent::SagaFailed {
            context: terminal_context(context),
            reason,
            failure,
        },
    );
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

/// Overall/stalled timeouts are infrastructure aborts (ADR-0004 §2.2): in
/// `Running` they begin a rollback; while already rolling back they quarantine
/// the run with whatever is still owed.
fn timeout_abort_events(
    policy: &TerminalPolicy,
    state: &mut SagaResolutionState,
    now_millis: u64,
) -> Vec<SagaChoreographyEvent> {
    let rolling_back = state.phase == ResolverPhase::Aborting;
    // Rollback gets its own budget, measured from when it began.
    let since = match (rolling_back, state.aborting_since_millis) {
        (true, Some(since)) => since,
        _ => state.started_at_millis,
    };
    let elapsed_ms = now_millis.saturating_sub(since);
    let overall_timeout_ms = policy.overall_timeout.as_millis() as u64;
    let stalled_ms = now_millis.saturating_sub(state.last_progress_at_millis);
    let stalled_timeout_ms = policy.stalled_timeout.as_millis() as u64;
    let (code, prefix) = if elapsed_ms > overall_timeout_ms {
        (
            "overall_timeout",
            format!("overall_timeout after {overall_timeout_ms}ms"),
        )
    } else if stalled_ms > stalled_timeout_ms {
        (
            "stalled_timeout",
            format!("stalled_timeout after {stalled_timeout_ms}ms without progress"),
        )
    } else {
        return Vec::new();
    };
    let diagnostic = timeout_diagnostics(policy, state);
    emit_timeout_diagnostic(policy, state, code, &diagnostic);
    let reason: Box<str> = diagnostic.reason(prefix, &policy.policy_id).into();
    let context = terminal_context_at(&state.last_context, now_millis);
    let mut out = Vec::new();
    match state.phase {
        ResolverPhase::Running => {
            begin_internal_abort(
                state, context, code, reason, now_millis, now_millis, &mut out,
            );
        }
        ResolverPhase::Aborting => {
            let mut stuck: Vec<Box<str>> = state
                .pending_compensation_steps
                .iter()
                .cloned()
                .chain(state.unresolved.iter().cloned())
                .chain(in_flight_steps(state))
                .collect();
            stuck.sort();
            stuck.dedup();
            error!(
                event = "saga_quarantined_rollback_timeout",
                run = %state.last_context.run_key(),
                steps = %stuck.join(", "),
                "rollback timed out with undo outstanding"
            );
            let step = stuck
                .first()
                .cloned()
                .unwrap_or_else(|| TERMINAL_RESOLVER_STEP.into());
            latch_event(
                state,
                &mut out,
                SagaChoreographyEvent::SagaQuarantined {
                    context,
                    reason: format!("{reason}; rollback incomplete: {}", stuck.join(", ")).into(),
                    step,
                    participant_id: TERMINAL_RESOLVER_STEP.into(),
                },
            );
        }
        ResolverPhase::Terminal | ResolverPhase::Quarantined => {}
    }
    out
}

fn latch_event(
    state: &mut SagaResolutionState,
    out: &mut Vec<SagaChoreographyEvent>,
    event: SagaChoreographyEvent,
) {
    state.terminal_latched = true;
    state.terminal_outcome = Some(event.event_type());
    out.push(event);
    sync_terminal_phase(state);
}

/// Begin an internal abort (timeout / `SagaAbortRequested`): undo every known
/// compensable effect, then fail only at clean settlement. Nothing owed and
/// nothing in flight fails directly; unknown in-flight effects quarantine.
fn begin_internal_abort(
    state: &mut SagaResolutionState,
    context: SagaContext,
    code: &str,
    reason: Box<str>,
    now_millis: u64,
    at_millis: u64,
    out: &mut Vec<SagaChoreographyEvent>,
) {
    let owed: Vec<Box<str>> = state.compensable_steps.iter().rev().cloned().collect();

    if owed.is_empty() {
        // Nothing to undo: settle at once (fail, or quarantine in-flight steps).
        state.abort_reason = Some(reason.clone());
        state.pending_failure = None;
        settle(state, &context, None, out);
        return;
    }

    for step in &owed {
        state.accepted_steps.remove(step.as_ref());
    }
    let failure = SagaFailureDetails {
        step_name: TERMINAL_RESOLVER_STEP.into(),
        participant_id: TERMINAL_RESOLVER_STEP.into(),
        error_code: Some(code.into()),
        error_message: reason.clone(),
        at_millis,
    };
    state.pending_failure = Some(failure.clone());
    state.abort_reason = Some(reason.clone());
    state.pending_compensation_steps = owed.iter().cloned().collect();
    state.phase = ResolverPhase::Aborting;
    state.aborting_since_millis = Some(now_millis);
    state.last_progress_at_millis = now_millis;
    out.push(SagaChoreographyEvent::CompensationRequested {
        context,
        failed_step: TERMINAL_RESOLVER_STEP.into(),
        reason,
        failure,
        steps_to_compensate: owed,
    });
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use crate::{
        AcceptedStepTimeoutOutcome, RunIdentityError, SagaChoreographyEvent, SagaContext, SagaId,
        SagaWorkflowStepContract, StepExecutionId, WorkflowDependencySpec,
    };

    use super::{
        FailureAuthority, SuccessCriteria, TerminalPolicy, TerminalResolver, is_terminal_event,
    };

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
        TerminalPolicy::new(
            "order_lifecycle".into(),
            "open_position/default".into(),
            FailureAuthority::AnyParticipant,
            SuccessCriteria::AllOf(required_steps),
            Duration::from_secs(5),
            stalled_timeout,
            OPEN_POSITION_STEPS,
        )
    }

    fn run_key(saga_id: u64, started_at_millis: u64) -> crate::RunKey {
        ctx_at(
            "create_order",
            saga_id,
            started_at_millis,
            started_at_millis,
        )
        .run_key()
    }

    fn ctx(step: &str) -> SagaContext {
        static BASE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
        let base = *BASE.get_or_init(SagaContext::now_millis);
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
            saga_started_at_millis: base,
            event_timestamp_millis: base,
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
        let policy = TerminalPolicy::new(
            "order_lifecycle".into(),
            "test".into(),
            FailureAuthority::AnyParticipant,
            SuccessCriteria::AllOf(required),
            Duration::from_secs(60),
            Duration::from_secs(60),
            &[],
        );
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
        let policy = TerminalPolicy::new(
            "order_lifecycle".into(),
            "test".into(),
            FailureAuthority::OnlySteps(only_steps),
            SuccessCriteria::AnyOf(HashSet::new()),
            Duration::from_secs(30),
            Duration::from_secs(30),
            &[],
        );
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
        let policy = TerminalPolicy::new(
            "order_lifecycle".into(),
            "absorbing".into(),
            FailureAuthority::AnyParticipant,
            SuccessCriteria::AllOf(required_steps),
            Duration::from_millis(100),
            Duration::from_secs(60),
            &[],
        );
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
        let policy = TerminalPolicy::new(
            "order_lifecycle".into(),
            "hard-timeout".into(),
            FailureAuthority::AnyParticipant,
            SuccessCriteria::AllOf(required_steps),
            Duration::from_millis(100),
            Duration::from_secs(60),
            &[],
        );
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
            [SagaChoreographyEvent::SagaFailed { reason, failure: Some(failure), .. }]
                if reason.contains("accepted step idle timeout")
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
        let policy = TerminalPolicy::new(
            "order_lifecycle".into(),
            "accepted-timeout-deny".into(),
            FailureAuthority::DenySteps(denied_steps),
            SuccessCriteria::AllOf(required_steps),
            Duration::from_secs(5),
            Duration::from_secs(5),
            &[],
        );
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
        let policy = TerminalPolicy::new(
            "order_lifecycle".into(),
            "accepted-timeout/sibling".into(),
            FailureAuthority::OnlySteps(authorized),
            SuccessCriteria::AllOf(required),
            Duration::from_secs(5),
            Duration::from_secs(5),
            OPEN_POSITION_STEPS,
        );
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
                timed_out.as_slice(),
                [SagaChoreographyEvent::CompensationRequested { failed_step, steps_to_compensate, .. }]
                    if failed_step.as_ref() == "create_order"
                        && steps_to_compensate.as_slice() == ["risk_check".into()]
            ),
            "unauthorized sibling timeout must not swallow compensation request: {timed_out:?}"
        );
    }

    #[test]
    fn old_run_timeout_cannot_mutate_new_incarnation() {
        let mut resolver = TerminalResolver::new(open_position_policy(Duration::from_secs(5)));
        let start = |incarnation: u64| SagaChoreographyEvent::SagaStarted {
            context: ctx_at("create_order", 31, incarnation, incarnation),
            payload: Vec::new(),
        };
        assert!(resolver.try_ingest_at(&start(1_000), 1_000).is_ok());
        // Newer incarnation coexists with the still-active older one (ADR-0001 §2.2).
        assert!(resolver.try_ingest_at(&start(5_000), 5_000).is_ok());

        // Only the old run is past its overall deadline.
        let timeouts = resolver.poll_timeouts_at(6_500);
        assert!(
            timeouts
                .iter()
                .all(|event| event.context().saga_started_at_millis == 1_000),
            "timeout must carry only the old incarnation: {timeouts:?}"
        );
        assert!(
            timeouts.iter().any(is_terminal_event),
            "old run must time out: {timeouts:?}"
        );

        let completed = resolver.ingest_at(
            &SagaChoreographyEvent::StepCompleted {
                context: ctx_at("create_order", 31, 5_000, 7_000),
                output: Vec::new(),
                saga_input: Vec::new(),
                compensation_available: false,
            },
            7_000,
        );
        assert!(
            matches!(
                completed.as_slice(),
                [SagaChoreographyEvent::SagaCompleted { context }]
                    if context.saga_started_at_millis == 5_000
            ),
            "new incarnation must resolve independently: {completed:?}"
        );
    }

    #[test]
    fn stale_start_is_rejected_by_try_ingest_at() {
        let mut resolver = TerminalResolver::new(open_position_policy(Duration::from_secs(5)));
        let start = |incarnation: u64| SagaChoreographyEvent::SagaStarted {
            context: ctx_at("create_order", 32, incarnation, incarnation),
            payload: Vec::new(),
        };
        assert!(resolver.try_ingest_at(&start(2_000), 2_000).is_ok());
        let result = resolver.try_ingest_at(&start(1_000), 2_001);
        assert!(
            matches!(result, Err(RunIdentityError::StaleIncarnation { .. })),
            "stale start must be rejected: {result:?}"
        );
        assert!(
            resolver.ingest_at(&start(1_000), 2_002).is_empty(),
            "ingest_at wrapper yields no events on rejection"
        );
    }

    #[test]
    fn evicted_terminal_state_keeps_latch_tombstone() {
        let mut resolver = TerminalResolver::new(open_position_policy(Duration::from_secs(5)));
        resolver.terminal_latch_retention = 1;

        let _ = resolver.ingest_at(
            &SagaChoreographyEvent::SagaStarted {
                context: ctx_at("create_order", 21, 1_000, 1_000),
                payload: Vec::new(),
            },
            1_000,
        );
        let completed_first = resolver.ingest_at(
            &SagaChoreographyEvent::StepCompleted {
                context: ctx_at("create_order", 21, 1_000, 1_010),
                output: Vec::new(),
                saga_input: Vec::new(),
                compensation_available: false,
            },
            1_010,
        );
        assert!(matches!(
            completed_first.as_slice(),
            [SagaChoreographyEvent::SagaCompleted { .. }]
        ));

        let _ = resolver.ingest_at(
            &SagaChoreographyEvent::SagaStarted {
                context: ctx_at("create_order", 22, 2_000, 2_000),
                payload: Vec::new(),
            },
            2_000,
        );
        let _ = resolver.ingest_at(
            &SagaChoreographyEvent::StepCompleted {
                context: ctx_at("create_order", 22, 2_000, 2_010),
                output: Vec::new(),
                saga_input: Vec::new(),
                compensation_available: false,
            },
            2_010,
        );
        assert!(
            !resolver.states.contains_key(&run_key(21, 1_000)),
            "retention should evict detailed state for the oldest terminal saga"
        );

        let late = resolver.ingest_at(
            &SagaChoreographyEvent::StepCompleted {
                context: ctx_at("create_order", 21, 1_000, 3_000),
                output: Vec::new(),
                saga_input: Vec::new(),
                compensation_available: false,
            },
            3_000,
        );
        assert!(
            late.is_empty(),
            "late events for evicted terminal sagas must stay latched"
        );
        assert!(
            !resolver.states.contains_key(&run_key(21, 1_000)),
            "late event must not resurrect evicted terminal saga state"
        );
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
        let policy = TerminalPolicy::new(
            "order_lifecycle".into(),
            "progress-timeout".into(),
            FailureAuthority::AnyParticipant,
            SuccessCriteria::AllOf(required_steps),
            Duration::from_secs(5),
            Duration::from_millis(100),
            &[],
        );
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
            "expected stalled-timeout quarantine (positions_check in flight), got: {timed_out:?}"
        );
    }

    fn stall_policy() -> TerminalPolicy {
        let mut required_steps = HashSet::new();
        required_steps.insert("create_order".into());
        TerminalPolicy::new(
            "order_lifecycle".into(),
            "stall-dedupe".into(),
            FailureAuthority::AnyParticipant,
            SuccessCriteria::AllOf(required_steps),
            Duration::from_secs(5),
            Duration::from_millis(100),
            &[],
        )
    }

    fn is_stall_quarantine(events: &[SagaChoreographyEvent]) -> bool {
        matches!(
            events.first(),
            Some(SagaChoreographyEvent::SagaQuarantined { reason, .. })
            if reason.as_ref().contains("stalled_timeout")
        )
    }

    #[test]
    fn duplicate_progress_does_not_extend_stall_deadline() {
        let mut resolver = TerminalResolver::new(stall_policy());
        let start = SagaChoreographyEvent::SagaStarted {
            context: ctx_at("risk_check", 9, 1_000, 1_000),
            payload: Vec::new(),
        };
        let _ = resolver.ingest_at(&start, 1_000);
        let progress = SagaChoreographyEvent::StepStarted {
            context: ctx_at("positions_check", 9, 1_000, 1_050),
        };
        let _ = resolver.ingest_at(&progress, 1_050);
        // Replays of the same progress and a late duplicate SagaStarted.
        for at in [1_070, 1_090, 1_120] {
            let _ = resolver.ingest_at(&progress, at);
            let _ = resolver.ingest_at(&start, at);
        }
        assert!(resolver.poll_timeouts_at(1_150).is_empty());
        let timed_out = resolver.poll_timeouts_at(1_151);
        assert!(
            is_stall_quarantine(&timed_out),
            "duplicates must not extend the stall deadline past 1050+100: {timed_out:?}"
        );
    }

    #[test]
    fn regressing_progress_time_does_not_pull_stall_deadline_back() {
        let mut resolver = TerminalResolver::new(stall_policy());
        let start = SagaChoreographyEvent::SagaStarted {
            context: ctx_at("risk_check", 9, 1_000, 1_000),
            payload: Vec::new(),
        };
        let _ = resolver.ingest_at(&start, 1_000);
        let first = SagaChoreographyEvent::StepStarted {
            context: ctx_at("positions_check", 9, 1_000, 1_090),
        };
        let _ = resolver.ingest_at(&first, 1_090);
        // Novel progress observed with an older clock reading.
        let older = SagaChoreographyEvent::StepStarted {
            context: ctx_at("fraud_check", 9, 1_000, 1_020),
        };
        let _ = resolver.ingest_at(&older, 1_020);
        assert!(
            resolver.poll_timeouts_at(1_150).is_empty(),
            "a regressing clock must not move the progress mark backwards"
        );
        assert!(is_stall_quarantine(&resolver.poll_timeouts_at(1_191)));
    }

    #[test]
    fn future_dated_saga_start_does_not_extend_initial_stall_window() {
        let mut resolver = TerminalResolver::new(stall_policy());
        let start = SagaChoreographyEvent::SagaStarted {
            context: ctx_at("risk_check", 9, 50_000, 1_000),
            payload: Vec::new(),
        };
        let _ = resolver.ingest_at(&start, 1_000);
        let timed_out = resolver.poll_timeouts_at(1_101);
        assert!(
            is_stall_quarantine(&timed_out)
                || matches!(timed_out.first(), Some(SagaChoreographyEvent::SagaFailed { reason, .. }) if reason.contains("stalled_timeout")),
            "stall window is measured from receive time: {timed_out:?}"
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
            panic!("expected stalled timeout failure, got: {timed_out:?}");
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
            panic!("expected stalled timeout quarantine (in-flight step), got: {timed_out:?}");
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
                }] if steps_to_compensate.as_slice()
                    == [Box::<str>::from("second_effect"), Box::<str>::from("first_effect")]
            ),
            "unexpected recovery output: {emitted:?}"
        );
    }

    fn rollback_policy() -> TerminalPolicy {
        let mut required = HashSet::new();
        required.insert(Box::<str>::from("B"));
        TerminalPolicy::new(
            "order_lifecycle".into(),
            "late_completion/test".into(),
            FailureAuthority::AnyParticipant,
            SuccessCriteria::AllOf(required),
            Duration::from_secs(3_600),
            Duration::from_secs(3_600),
            &[],
        )
    }

    fn completed(step: &str, at: u64, compensable: bool) -> SagaChoreographyEvent {
        SagaChoreographyEvent::StepCompleted {
            context: ctx_at(step, 51, 1_000, at),
            output: Vec::new(),
            saga_input: Vec::new(),
            compensation_available: compensable,
        }
    }

    fn undo_ack(step: &str, at: u64) -> SagaChoreographyEvent {
        SagaChoreographyEvent::CompensationCompleted {
            context: ctx_at(step, 51, 1_000, at),
        }
    }

    /// A compensable done, B dispatched, C fails -> rollback owes {A}.
    fn rolling_back() -> (TerminalResolver, Vec<SagaChoreographyEvent>) {
        let mut resolver = TerminalResolver::new(rollback_policy());
        assert!(
            resolver
                .ingest_at(&completed("A", 1_010, true), 1_010)
                .is_empty()
        );
        let emitted = resolver.ingest_at(
            &SagaChoreographyEvent::StepFailed {
                context: ctx_at("C", 51, 1_000, 1_020),
                participant_id: "c".into(),
                error_code: None,
                error: "c failed".into(),
                requires_compensation: true,
            },
            1_020,
        );
        assert!(matches!(
            emitted.as_slice(),
            [SagaChoreographyEvent::CompensationRequested { .. }]
        ));
        (resolver, emitted)
    }

    fn no_completed(events: &[SagaChoreographyEvent]) -> bool {
        !events
            .iter()
            .any(|e| matches!(e, SagaChoreographyEvent::SagaCompleted { .. }))
    }

    #[test]
    fn late_forward_completion_during_rollback_never_completes_saga() {
        let (mut resolver, _) = rolling_back();
        let late = resolver.ingest_at(&completed("B", 1_030, true), 1_030);
        assert!(no_completed(&late), "success during rollback: {late:?}");
        assert!(
            matches!(
                late.as_slice(),
                [SagaChoreographyEvent::CompensationRequested { steps_to_compensate, .. }]
                    if steps_to_compensate.as_slice() == [Box::<str>::from("B")]
            ),
            "late compensable B must be owed an undo: {late:?}"
        );
        // A's ack alone must not settle: B's undo is still owed.
        assert!(resolver.ingest_at(&undo_ack("A", 1_040), 1_040).is_empty());
        let end = resolver.ingest_at(&undo_ack("B", 1_050), 1_050);
        assert!(matches!(
            end.as_slice(),
            [SagaChoreographyEvent::SagaFailed { .. }]
        ));
    }

    #[test]
    fn late_completion_after_undo_ack_order_never_completes_saga() {
        let (mut resolver, _) = rolling_back();
        let ack = resolver.ingest_at(&undo_ack("A", 1_030), 1_030);
        assert!(no_completed(&ack));
        let late = resolver.ingest_at(&completed("B", 1_040, true), 1_040);
        assert!(no_completed(&late), "success after rollback: {late:?}");
    }

    #[test]
    fn late_non_compensable_completion_during_rollback_quarantines() {
        let (mut resolver, _) = rolling_back();
        let late = resolver.ingest_at(&completed("B", 1_030, false), 1_030);
        assert!(late.is_empty(), "no verdict before undo settles: {late:?}");
        let end = resolver.ingest_at(&undo_ack("A", 1_040), 1_040);
        assert!(no_completed(&end));
        assert!(
            matches!(
                end.as_slice(),
                [SagaChoreographyEvent::SagaQuarantined { step, .. }] if step.as_ref() == "B"
            ),
            "B must stay unresolved in quarantine: {end:?}"
        );
    }

    fn timeout_policy(overall_ms: u64, stalled_ms: u64) -> TerminalPolicy {
        let mut policy = rollback_policy();
        policy.overall_timeout = Duration::from_millis(overall_ms);
        policy.stalled_timeout = Duration::from_millis(stalled_ms);
        policy
    }

    fn assert_timeout_rolls_back(policy: TerminalPolicy, fire_at: u64, needle: &str) {
        let mut resolver = TerminalResolver::new(policy);
        assert!(
            resolver
                .ingest_at(&completed("A", 1_010, true), 1_010)
                .is_empty()
        );
        let out = resolver.poll_timeouts_at(fire_at);
        assert!(
            matches!(
                out.as_slice(),
                [SagaChoreographyEvent::CompensationRequested { steps_to_compensate, reason, .. }]
                    if steps_to_compensate.as_slice() == [Box::<str>::from("A")]
                        && reason.contains(needle)
            ),
            "{needle} must request A's undo, not fail directly: {out:?}"
        );
        // Not re-quarantined by the very next poll while the undo is in flight.
        assert!(resolver.poll_timeouts_at(fire_at + 1).is_empty());
        let end = resolver.ingest_at(&undo_ack("A", fire_at + 2), fire_at + 2);
        assert!(
            matches!(
                end.as_slice(),
                [SagaChoreographyEvent::SagaFailed { reason, failure: None, .. }]
                    if reason.contains(needle)
            ),
            "clean settlement fails the saga: {end:?}"
        );
    }

    #[test]
    fn infrastructure_abort_compensates_known_effects_before_terminal_failure() {
        assert_timeout_rolls_back(timeout_policy(500, 3_600_000), 1_600, "overall_timeout");
        assert_timeout_rolls_back(timeout_policy(3_600_000, 500), 1_600, "stalled_timeout");
    }

    #[test]
    fn abort_request_compensates_known_effects_before_terminal_failure() {
        let mut resolver = TerminalResolver::new(rollback_policy());
        resolver.ingest_at(&completed("A", 1_010, true), 1_010);
        let out = resolver.ingest_at(
            &SagaChoreographyEvent::SagaAbortRequested {
                context: ctx_at("bus", 51, 1_000, 1_020),
                reason: "delivery shortfall".into(),
                source: crate::AbortSource::DeliveryShortfall,
            },
            1_020,
        );
        assert!(
            matches!(
                out.as_slice(),
                [SagaChoreographyEvent::CompensationRequested { steps_to_compensate, .. }]
                    if steps_to_compensate.as_slice() == [Box::<str>::from("A")]
            ),
            "{out:?}"
        );
    }

    #[test]
    fn abort_with_nothing_owed_fails_and_unknown_in_flight_quarantines() {
        let mut resolver = TerminalResolver::new(timeout_policy(500, 3_600_000));
        resolver.ingest_at(&completed("A", 1_010, false), 1_010);
        let out = resolver.poll_timeouts_at(1_600);
        assert!(
            matches!(out.as_slice(), [SagaChoreographyEvent::SagaFailed { .. }]),
            "{out:?}"
        );

        let mut resolver = TerminalResolver::new(timeout_policy(500, 3_600_000));
        resolver.ingest_at(
            &SagaChoreographyEvent::StepStarted {
                context: ctx_at("B", 52, 1_000, 1_010),
            },
            1_010,
        );
        let out = resolver.poll_timeouts_at(1_600);
        assert!(
            matches!(out.as_slice(), [SagaChoreographyEvent::SagaQuarantined { step, .. }] if step.as_ref() == "B"),
            "{out:?}"
        );
    }

    #[test]
    fn timeout_while_rolling_back_quarantines_owed_steps() {
        let mut resolver = TerminalResolver::new(timeout_policy(500, 3_600_000));
        resolver.ingest_at(&completed("A", 1_010, true), 1_010);
        resolver.poll_timeouts_at(1_600);
        let out = resolver.poll_timeouts_at(2_200);
        assert!(
            matches!(out.as_slice(), [SagaChoreographyEvent::SagaQuarantined { step, .. }] if step.as_ref() == "A"),
            "{out:?}"
        );
    }

    // ---- W3 review fixes (BLOCKER 2, HIGH 1-3, MEDIUM 2-4, LOW 3, LOW 5) ----

    fn started(step: &str, at: u64) -> SagaChoreographyEvent {
        SagaChoreographyEvent::StepStarted {
            context: ctx_at(step, 51, 1_000, at),
        }
    }

    fn step_failed(step: &str, at: u64, requires_compensation: bool) -> SagaChoreographyEvent {
        SagaChoreographyEvent::StepFailed {
            context: ctx_at(step, 51, 1_000, at),
            participant_id: step.into(),
            error_code: None,
            error: format!("{step} failed").into(),
            requires_compensation,
        }
    }

    fn accepted(
        step: &str,
        at: u64,
        deadline: u64,
        hard: u64,
        compensable: bool,
    ) -> SagaChoreographyEvent {
        SagaChoreographyEvent::StepAccepted {
            context: ctx_at(step, 51, 1_000, at),
            participant_id: step.into(),
            execution_id: StepExecutionId::new("exec-1"),
            deadline_at_millis: deadline,
            hard_deadline_at_millis: hard,
            timeouts_enabled: true,
            timeout_outcome: AcceptedStepTimeoutOutcome::QuarantineSaga,
            compensation_available: compensable,
        }
    }

    /// A compensable done, B started (in flight), C fails -> rollback owes {A}.
    fn rolling_back_with_b_in_flight() -> TerminalResolver {
        let mut resolver = TerminalResolver::new(rollback_policy());
        resolver.ingest_at(&completed("A", 1_010, true), 1_010);
        resolver.ingest_at(&started("B", 1_015), 1_015);
        let out = resolver.ingest_at(&step_failed("C", 1_020, true), 1_020);
        assert!(matches!(
            out.as_slice(),
            [SagaChoreographyEvent::CompensationRequested { .. }]
        ));
        resolver
    }

    #[test]
    fn rollback_with_step_in_flight_quarantines_instead_of_failing() {
        let mut resolver = rolling_back_with_b_in_flight();
        let end = resolver.ingest_at(&undo_ack("A", 1_040), 1_040);
        assert!(
            matches!(
                end.as_slice(),
                [SagaChoreographyEvent::SagaQuarantined { step, .. }] if step.as_ref() == "B"
            ),
            "B was dispatched and never finished: {end:?}"
        );
    }

    #[test]
    fn in_flight_step_resolved_by_failure_lets_rollback_fail_cleanly() {
        let mut resolver = rolling_back_with_b_in_flight();
        assert!(
            resolver
                .ingest_at(&step_failed("B", 1_030, false), 1_030)
                .is_empty()
        );
        let end = resolver.ingest_at(&undo_ack("A", 1_040), 1_040);
        assert!(
            matches!(end.as_slice(), [SagaChoreographyEvent::SagaFailed { .. }]),
            "{end:?}"
        );
    }

    #[test]
    fn failure_with_nothing_owed_and_step_in_flight_quarantines() {
        let mut resolver = TerminalResolver::new(rollback_policy());
        resolver.ingest_at(&started("B", 1_015), 1_015);
        let out = resolver.ingest_at(&step_failed("C", 1_020, true), 1_020);
        assert!(
            matches!(
                out.as_slice(),
                [
                    SagaChoreographyEvent::CompensationRequested { .. },
                    SagaChoreographyEvent::SagaQuarantined { step, .. },
                ] if step.as_ref() == "B"
            ),
            "{out:?}"
        );
    }

    #[test]
    fn accepted_compensable_step_during_rollback_is_owed_an_undo() {
        let (mut resolver, _) = rolling_back();
        let out = resolver.ingest_at(&accepted("B", 1_030, 9_000, 9_500, true), 1_030);
        assert!(
            matches!(
                out.as_slice(),
                [SagaChoreographyEvent::CompensationRequested { steps_to_compensate, .. }]
                    if steps_to_compensate.as_slice() == [Box::<str>::from("B")]
            ),
            "{out:?}"
        );
        assert!(resolver.ingest_at(&undo_ack("A", 1_040), 1_040).is_empty());
        let end = resolver.ingest_at(&undo_ack("B", 1_050), 1_050);
        assert!(
            matches!(end.as_slice(), [SagaChoreographyEvent::SagaFailed { .. }]),
            "{end:?}"
        );
    }

    #[test]
    fn step_failure_during_rollback_only_records_evidence() {
        let (mut resolver, _) = rolling_back();
        assert!(
            resolver
                .ingest_at(&step_failed("D", 1_030, false), 1_030)
                .is_empty(),
            "no terminal while undo is pending"
        );
        assert!(
            resolver
                .ingest_at(&step_failed("E", 1_031, true), 1_031)
                .is_empty()
        );
        let end = resolver.ingest_at(&undo_ack("A", 1_040), 1_040);
        assert!(
            matches!(
                end.as_slice(),
                [SagaChoreographyEvent::SagaFailed { failure: Some(f), .. }]
                    if f.step_name.as_ref() == "C"
            ),
            "first failure stays the recorded cause: {end:?}"
        );
    }

    fn retryable(step: &str, at: u64) -> SagaChoreographyEvent {
        SagaChoreographyEvent::CompensationFailedRetryable {
            context: ctx_at(step, 51, 1_000, at),
            participant_id: step.into(),
            error: "undo failed".into(),
        }
    }

    #[test]
    fn retryable_compensation_failure_during_rollback_quarantines() {
        let (mut resolver, _) = rolling_back();
        let out = resolver.ingest_at(&retryable("A", 1_030), 1_030);
        assert!(
            matches!(
                out.as_slice(),
                [SagaChoreographyEvent::SagaQuarantined { step, .. }] if step.as_ref() == "A"
            ),
            "undo never ran: {out:?}"
        );
    }

    #[test]
    fn successful_retried_undo_clears_unresolved_and_settles_failed() {
        let (mut resolver, _) = rolling_back();
        // B completes late and is owed an undo, so the run keeps rolling back.
        let late = resolver.ingest_at(&completed("B", 1_030, true), 1_030);
        assert_eq!(late.len(), 1, "late B owed an undo: {late:?}");
        assert!(resolver.ingest_at(&retryable("A", 1_040), 1_040).is_empty());
        // A's undo is retried and succeeds: no longer unresolved.
        assert!(resolver.ingest_at(&undo_ack("A", 1_050), 1_050).is_empty());
        let end = resolver.ingest_at(&undo_ack("B", 1_060), 1_060);
        assert!(
            matches!(end.as_slice(), [SagaChoreographyEvent::SagaFailed { .. }]),
            "clean rollback must not quarantine: {end:?}"
        );
    }

    #[test]
    fn retryable_compensation_failure_outside_rollback_never_fails_saga() {
        let mut resolver = TerminalResolver::new(rollback_policy());
        resolver.ingest_at(&completed("A", 1_010, true), 1_010);
        let out = resolver.ingest_at(&retryable("A", 1_030), 1_030);
        assert!(out.is_empty(), "{out:?}");
    }

    #[test]
    fn duplicate_completion_of_finished_step_during_rollback_is_ignored() {
        let mut resolver = TerminalResolver::new(rollback_policy());
        resolver.ingest_at(&completed("A", 1_010, true), 1_010);
        resolver.ingest_at(&completed("D", 1_012, false), 1_012);
        resolver.ingest_at(&step_failed("C", 1_020, true), 1_020);
        assert!(
            resolver
                .ingest_at(&completed("D", 1_030, false), 1_030)
                .is_empty()
        );
        let end = resolver.ingest_at(&undo_ack("A", 1_040), 1_040);
        assert!(
            matches!(end.as_slice(), [SagaChoreographyEvent::SagaFailed { .. }]),
            "redelivery is not a late effect: {end:?}"
        );
    }

    #[test]
    fn older_accepted_heartbeat_does_not_shorten_deadline() {
        let mut resolver = TerminalResolver::new(rollback_policy());
        resolver.ingest_at(&accepted("B", 1_010, 5_000, 6_000, false), 1_010);
        resolver.ingest_at(&accepted("B", 1_020, 3_000, 4_000, false), 1_020);
        assert!(
            resolver.poll_timeouts_at(4_500).is_empty(),
            "the older heartbeat must not pull the deadline back"
        );
    }

    /// Events journaled by a run that began an internal (overall-timeout) abort.
    fn rollback_events() -> (TerminalPolicy, Vec<SagaChoreographyEvent>) {
        let base = SagaContext::now_millis();
        let mut policy = timeout_policy(50, 3_600_000);
        let done = SagaChoreographyEvent::StepCompleted {
            context: ctx_at("A", 51, base, base + 1),
            output: Vec::new(),
            saga_input: Vec::new(),
            compensation_available: true,
        };
        let mut resolver = TerminalResolver::new(policy.clone());
        resolver.ingest_at(&done, base + 1);
        let request = resolver.poll_timeouts_at(base + 100);
        assert!(matches!(
            request.as_slice(),
            [SagaChoreographyEvent::CompensationRequested { .. }]
        ));
        // Restore happens at real time: give the restored policy room.
        policy.overall_timeout = Duration::from_secs(3_600);
        let mut events = vec![done];
        events.extend(request);
        (policy, events)
    }

    fn ack_a(base_ctx: &SagaContext) -> SagaChoreographyEvent {
        let mut context = base_ctx.clone();
        context.step_name = "A".into();
        context.event_timestamp_millis += 5;
        SagaChoreographyEvent::CompensationCompleted { context }
    }

    #[test]
    fn restore_mid_internal_abort_settles_with_timeout_reason_once() {
        let (policy, events) = rollback_events();
        let (mut resolver, unpublished) =
            TerminalResolver::restore_from_events(policy.clone(), &events);
        assert!(unpublished.is_empty(), "{unpublished:?}");
        let ack = ack_a(events[1].context());
        let end = resolver.ingest_at(&ack, ack.context().event_timestamp_millis);
        assert!(
            matches!(
                end.as_slice(),
                [SagaChoreographyEvent::SagaFailed { reason, failure: None, .. }]
                    if reason.contains("overall_timeout")
            ),
            "{end:?}"
        );
        // Journal already holds the terminal: restoring it again republishes nothing.
        let mut journal = events.clone();
        journal.push(ack);
        journal.extend(end);
        let (_, unpublished) = TerminalResolver::restore_from_events(policy, &journal);
        assert!(
            unpublished.is_empty(),
            "no duplicate terminal after restore: {unpublished:?}"
        );
    }

    #[test]
    fn restore_mid_internal_abort_with_step_in_flight_quarantines() {
        let (policy, mut events) = rollback_events();
        let mut context = events[1].context().clone();
        context.step_name = "B".into();
        events.insert(1, SagaChoreographyEvent::StepStarted { context });
        let ack = ack_a(events[2].context());
        let (mut resolver, _) = TerminalResolver::restore_from_events(policy, &events);
        let end = resolver.ingest_at(&ack, ack.context().event_timestamp_millis);
        assert!(
            matches!(
                end.as_slice(),
                [SagaChoreographyEvent::SagaQuarantined { step, .. }] if step.as_ref() == "B"
            ),
            "{end:?}"
        );
    }

    #[test]
    fn live_abort_failure_stamp_uses_event_timestamp() {
        let mut resolver = TerminalResolver::new(rollback_policy());
        resolver.ingest_at(&completed("A", 1_010, true), 1_010);
        let out = resolver.ingest_at(
            &SagaChoreographyEvent::SagaAbortRequested {
                context: ctx_at("bus", 51, 1_000, 1_020),
                reason: "delivery shortfall".into(),
                source: crate::AbortSource::DeliveryShortfall,
            },
            1_500,
        );
        assert!(
            matches!(
                out.as_slice(),
                [SagaChoreographyEvent::CompensationRequested { failure, .. }]
                    if failure.at_millis == 1_020
            ),
            "{out:?}"
        );
    }

    #[test]
    fn in_flight_step_that_fails_after_abort_does_not_quarantine_falsely() {
        let mut resolver = TerminalResolver::new(timeout_policy(500, 3_600_000));
        resolver.ingest_at(&completed("A", 1_010, true), 1_010);
        resolver.ingest_at(&started("B", 1_015), 1_015);
        let out = resolver.poll_timeouts_at(1_600);
        assert!(matches!(
            out.as_slice(),
            [SagaChoreographyEvent::CompensationRequested { .. }]
        ));
        assert!(
            resolver
                .ingest_at(&step_failed("B", 1_610, false), 1_610)
                .is_empty()
        );
        let end = resolver.ingest_at(&undo_ack("A", 1_620), 1_620);
        assert!(
            matches!(
                end.as_slice(),
                [SagaChoreographyEvent::SagaFailed { failure: None, .. }]
            ),
            "B resolved by its failure: {end:?}"
        );
    }

    #[test]
    fn timeout_latched_inside_apply_event_sets_terminal_outcome() {
        let mut resolver = TerminalResolver::new(rollback_policy());
        resolver.ingest_at(&accepted("B", 1_010, 1_100, 1_200, false), 1_010);
        let out = resolver.ingest_at(&started("X", 1_500), 1_500);
        assert!(
            matches!(
                out.as_slice(),
                [SagaChoreographyEvent::SagaQuarantined { .. }]
            ),
            "{out:?}"
        );
        let state = resolver.states.values().next().expect("state");
        assert_eq!(state.terminal_outcome, Some("saga_quarantined"));
        assert_eq!(state.phase, super::ResolverPhase::Quarantined);
    }

    #[test]
    fn duplicate_compensation_started_does_not_extend_stall_deadline() {
        let mut resolver = TerminalResolver::new(stall_policy());
        resolver.ingest_at(&completed("A", 1_000, true), 1_000);
        let comp = SagaChoreographyEvent::CompensationStarted {
            context: ctx_at("A", 51, 1_000, 1_050),
        };
        resolver.ingest_at(&comp, 1_050);
        for at in [1_070, 1_090, 1_120] {
            resolver.ingest_at(&comp, at);
        }
        assert!(resolver.poll_timeouts_at(1_150).is_empty());
        assert!(
            !resolver.poll_timeouts_at(1_151).is_empty(),
            "duplicate CompensationStarted must not extend the stall deadline"
        );
    }
}
