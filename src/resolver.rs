use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::{SagaChoreographyEvent, SagaContext, SagaFailureDetails, SagaId};

pub const TERMINAL_RESOLVER_STEP: &str = "terminal_resolver";

#[derive(Clone, Debug)]
pub enum FailureAuthority {
    AnyParticipant,
    OnlySteps(HashSet<Box<str>>),
    DenySteps(HashSet<Box<str>>),
}

impl FailureAuthority {
    fn is_authorized(&self, step_name: &str) -> bool {
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
    pub timeout: Option<Duration>,
}

/// Declarative workflow contract used to enforce terminal-policy registration.
///
/// Implementers provide the saga type and terminal policy that must be present
/// before `SagaStarted` events are accepted by the bus.
pub trait SagaTerminalPolicyProvider {
    fn terminal_policy() -> TerminalPolicy;
}

impl TerminalPolicy {
    pub fn order_lifecycle_default() -> Self {
        let mut required_steps: HashSet<Box<str>> = HashSet::new();
        required_steps.insert("create_order".into());
        Self {
            saga_type: "order_lifecycle".into(),
            policy_id: "order_lifecycle/default".into(),
            failure_authority: FailureAuthority::AnyParticipant,
            success_criteria: SuccessCriteria::AllOf(required_steps),
            timeout: Some(Duration::from_secs(30)),
        }
    }
}

#[derive(Clone, Debug)]
struct SagaResolutionState {
    completed_steps: HashSet<Box<str>>,
    compensable_steps: Vec<Box<str>>,
    compensation_requested: bool,
    pending_compensation_steps: HashSet<Box<str>>,
    pending_failure: Option<SagaFailureDetails>,
    started_at_millis: u64,
    terminal_latched: bool,
}

impl SagaResolutionState {
    fn new(started_at_millis: u64) -> Self {
        Self {
            completed_steps: HashSet::new(),
            compensable_steps: Vec::new(),
            compensation_requested: false,
            pending_compensation_steps: HashSet::new(),
            pending_failure: None,
            started_at_millis,
            terminal_latched: false,
        }
    }
}

#[derive(Debug)]
pub struct TerminalResolver {
    policy: TerminalPolicy,
    states: HashMap<SagaId, SagaResolutionState>,
}

impl TerminalResolver {
    pub fn new(policy: TerminalPolicy) -> Self {
        Self {
            policy,
            states: HashMap::new(),
        }
    }

    pub fn policy(&self) -> &TerminalPolicy {
        &self.policy
    }

    pub fn ingest(&mut self, event: &SagaChoreographyEvent) -> Vec<SagaChoreographyEvent> {
        if event.context().saga_type.as_ref() != self.policy.saga_type.as_ref() {
            return Vec::new();
        }

        let saga_id = event.context().saga_id;
        let state = self
            .states
            .entry(saga_id)
            .or_insert_with(|| SagaResolutionState::new(event.context().saga_started_at_millis));

        if state.terminal_latched {
            return Vec::new();
        }

        let mut out = Vec::new();

        match event {
            SagaChoreographyEvent::SagaStarted { .. } => {}
            SagaChoreographyEvent::StepCompleted {
                context,
                compensation_available,
                ..
            } => {
                let step_name = context.step_name.clone();
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
            | SagaChoreographyEvent::CompensationStarted { .. }
            | SagaChoreographyEvent::StepStarted { .. }
            | SagaChoreographyEvent::StepAck { .. } => {}
        }

        if !state.terminal_latched {
            if let Some(timeout) = self.policy.timeout {
                let now = SagaContext::now_millis();
                if now.saturating_sub(state.started_at_millis) > timeout.as_millis() as u64 {
                    let missing = self
                        .policy
                        .success_criteria
                        .missing_required_steps(&state.completed_steps);
                    let reason = if missing.is_empty() {
                        format!(
                            "timeout after {}ms policy={}",
                            timeout.as_millis(),
                            self.policy.policy_id
                        )
                    } else {
                        format!(
                            "timeout after {}ms missing_steps={} policy={}",
                            timeout.as_millis(),
                            missing
                                .iter()
                                .map(|s| s.as_ref())
                                .collect::<Vec<_>>()
                                .join(","),
                            self.policy.policy_id
                        )
                    };
                    out.push(SagaChoreographyEvent::SagaFailed {
                        context: terminal_context(event.context()),
                        reason: reason.into(),
                        failure: None,
                    });
                    state.terminal_latched = true;
                }
            }
        }

        out
    }
}

fn terminal_context(context: &SagaContext) -> SagaContext {
    context.next_step(TERMINAL_RESOLVER_STEP.into())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use crate::{SagaChoreographyEvent, SagaContext, SagaId};

    use super::{FailureAuthority, SuccessCriteria, TerminalPolicy, TerminalResolver};

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
            timeout: None,
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
            timeout: Some(Duration::from_secs(30)),
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
}
