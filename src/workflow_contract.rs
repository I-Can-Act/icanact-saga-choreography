use std::collections::{HashMap, HashSet};

use crate::{SuccessCriteria, TerminalPolicy};

#[derive(Clone, Copy, Debug)]
pub enum WorkflowDependencySpec {
    OnSagaStart,
    After(&'static str),
    AnyOf(&'static [&'static str]),
    AllOf(&'static [&'static str]),
}

#[derive(Clone, Copy, Debug)]
pub struct SagaWorkflowStepContract {
    pub step_name: &'static str,
    pub participant_id: &'static str,
    pub depends_on: WorkflowDependencySpec,
}

pub trait SagaWorkflowContract {
    fn saga_type() -> &'static str;
    fn first_step() -> &'static str;
    fn steps() -> &'static [SagaWorkflowStepContract];
    fn terminal_policy() -> TerminalPolicy;

    fn validate() -> Result<(), String> {
        let policy = Self::terminal_policy();
        validate_workflow_contract(
            Self::saga_type(),
            Self::first_step(),
            Self::steps(),
            &policy,
        )
    }
}

pub fn required_steps_from_success_criteria(criteria: &SuccessCriteria) -> HashSet<Box<str>> {
    match criteria {
        SuccessCriteria::AllOf(steps) | SuccessCriteria::AnyOf(steps) => steps.clone(),
        SuccessCriteria::Quorum { group_steps, .. } => group_steps.clone(),
    }
}

pub fn validate_workflow_contract(
    saga_type: &str,
    first_step: &str,
    steps: &[SagaWorkflowStepContract],
    policy: &TerminalPolicy,
) -> Result<(), String> {
    if policy.saga_type.as_ref() != saga_type {
        return Err(format!(
            "workflow contract saga_type mismatch: contract={} policy={}",
            saga_type, policy.saga_type
        ));
    }
    if first_step.is_empty() {
        return Err(format!(
            "workflow contract first_step cannot be empty: saga_type={saga_type}"
        ));
    }
    if steps.is_empty() {
        return Err(format!(
            "workflow contract must declare at least one step: saga_type={saga_type}"
        ));
    }

    let mut by_step: HashMap<&'static str, &SagaWorkflowStepContract> = HashMap::new();
    for step in steps {
        if step.step_name.is_empty() {
            return Err(format!(
                "workflow contract contains empty step_name: saga_type={saga_type}"
            ));
        }
        if step.participant_id.is_empty() {
            return Err(format!(
                "workflow contract contains empty participant_id: saga_type={} step={}",
                saga_type, step.step_name
            ));
        }
        if by_step.insert(step.step_name, step).is_some() {
            return Err(format!(
                "workflow contract duplicate step_name: saga_type={} step={}",
                saga_type, step.step_name
            ));
        }
    }

    if !by_step.contains_key(first_step) {
        return Err(format!(
            "workflow contract first_step is not declared: saga_type={} first_step={}",
            saga_type, first_step
        ));
    }

    for step in steps {
        for dependency in dependency_steps(step.depends_on) {
            if !by_step.contains_key(dependency) {
                return Err(format!(
                    "workflow contract dependency references undeclared step: saga_type={} step={} missing_dependency={}",
                    saga_type, step.step_name, dependency
                ));
            }
        }
    }

    let required = required_steps_from_success_criteria(&policy.success_criteria);
    for required_step in required {
        if !by_step.contains_key(required_step.as_ref()) {
            return Err(format!(
                "workflow contract terminal required step is not declared: saga_type={} required_step={}",
                saga_type, required_step
            ));
        }
    }

    detect_dependency_cycle(saga_type, &by_step)?;

    Ok(())
}

fn dependency_steps(depends_on: WorkflowDependencySpec) -> Vec<&'static str> {
    match depends_on {
        WorkflowDependencySpec::OnSagaStart => Vec::new(),
        WorkflowDependencySpec::After(step) => vec![step],
        WorkflowDependencySpec::AnyOf(steps) | WorkflowDependencySpec::AllOf(steps) => {
            steps.to_vec()
        }
    }
}

fn detect_dependency_cycle(
    saga_type: &str,
    by_step: &HashMap<&'static str, &SagaWorkflowStepContract>,
) -> Result<(), String> {
    #[derive(Clone, Copy, Eq, PartialEq)]
    enum Color {
        Visiting,
        Visited,
    }

    fn visit(
        saga_type: &str,
        step_name: &'static str,
        by_step: &HashMap<&'static str, &SagaWorkflowStepContract>,
        colors: &mut HashMap<&'static str, Color>,
    ) -> Result<(), String> {
        if let Some(color) = colors.get(step_name).copied() {
            if color == Color::Visiting {
                return Err(format!(
                    "workflow contract dependency cycle detected: saga_type={} step={}",
                    saga_type, step_name
                ));
            }
            return Ok(());
        }

        colors.insert(step_name, Color::Visiting);
        let step = match by_step.get(step_name) {
            Some(value) => *value,
            None => {
                return Err(format!(
                    "workflow contract internal validation failure: undeclared step during DFS: saga_type={} step={}",
                    saga_type, step_name
                ));
            }
        };
        for dependency in dependency_steps(step.depends_on) {
            visit(saga_type, dependency, by_step, colors)?;
        }
        colors.insert(step_name, Color::Visited);
        Ok(())
    }

    let mut colors: HashMap<&'static str, Color> = HashMap::new();
    for step_name in by_step.keys() {
        visit(saga_type, step_name, by_step, &mut colors)?;
    }
    Ok(())
}

#[doc(hidden)]
#[macro_export]
macro_rules! __saga_contract_check_dependency {
    ($enum_name:ident, on_start) => {};
    ($enum_name:ident, on_start ()) => {};
    ($enum_name:ident, after [$dep:ident $(,)?]) => {
        let _ = $enum_name::$dep;
    };
    ($enum_name:ident, any_of [$($dep:ident),+ $(,)?]) => {
        $(let _ = $enum_name::$dep;)+
    };
    ($enum_name:ident, all_of [$($dep:ident),+ $(,)?]) => {
        $(let _ = $enum_name::$dep;)+
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __saga_contract_dependency_spec {
    (on_start) => {
        $crate::WorkflowDependencySpec::OnSagaStart
    };
    (on_start ()) => {
        $crate::WorkflowDependencySpec::OnSagaStart
    };
    (after [$dep:ident $(,)?]) => {
        $crate::WorkflowDependencySpec::After(stringify!($dep))
    };
    (any_of [$($dep:ident),+ $(,)?]) => {
        $crate::WorkflowDependencySpec::AnyOf(&[$(stringify!($dep)),+])
    };
    (all_of [$($dep:ident),+ $(,)?]) => {
        $crate::WorkflowDependencySpec::AllOf(&[$(stringify!($dep)),+])
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __saga_contract_failure_authority {
    (any) => {
        $crate::FailureAuthority::AnyParticipant
    };
    (any ()) => {
        $crate::FailureAuthority::AnyParticipant
    };
    (only_steps [$($step:ident),+ $(,)?]) => {{
        let mut steps = std::collections::HashSet::new();
        $(steps.insert(stringify!($step).into());)+
        $crate::FailureAuthority::OnlySteps(steps)
    }};
    (deny_steps [$($step:ident),+ $(,)?]) => {{
        let mut steps = std::collections::HashSet::new();
        $(steps.insert(stringify!($step).into());)+
        $crate::FailureAuthority::DenySteps(steps)
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __saga_contract_required_steps_allof {
    ([$($step:ident),+ $(,)?]) => {{
        let mut steps = std::collections::HashSet::new();
        $(steps.insert(stringify!($step).into());)+
        $crate::SuccessCriteria::AllOf(steps)
    }};
}

#[macro_export]
macro_rules! define_saga_workflow_contract {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            saga_type: $saga_type:literal,
            first_step: $first_step:ident,
            failure_authority: $failure_authority:ident $failure_arg:tt,
            required_steps: [$($required_step:ident),+ $(,)?],
            overall_timeout_ms: $overall_timeout_ms:expr,
            stalled_timeout_ms: $stalled_timeout_ms:expr,
            steps: {
                $(
                    $step:ident => {
                        participant: $participant_id:expr,
                        depends_on: $depends_on:ident $depends_arg:tt
                    }
                ),+ $(,)?
            }
        }
    ) => {
        $(#[$meta])*
        $vis struct $name;

        const _: () = {
            #[allow(non_camel_case_types)]
            enum __ContractStep {
                $($step),+
            }
            let _ = __ContractStep::$first_step;
            $(let _ = __ContractStep::$required_step;)+
            $(
                $crate::__saga_contract_check_dependency!(__ContractStep, $depends_on $depends_arg);
            )+
        };

        impl $crate::SagaWorkflowContract for $name {
            fn saga_type() -> &'static str {
                $saga_type
            }

            fn first_step() -> &'static str {
                stringify!($first_step)
            }

            fn steps() -> &'static [$crate::SagaWorkflowStepContract] {
                &[
                    $(
                        $crate::SagaWorkflowStepContract {
                            step_name: stringify!($step),
                            participant_id: $participant_id,
                            depends_on: $crate::__saga_contract_dependency_spec!($depends_on $depends_arg),
                        },
                    )+
                ]
            }

            fn terminal_policy() -> $crate::TerminalPolicy {
                $crate::TerminalPolicy {
                    saga_type: Self::saga_type().into(),
                    policy_id: format!("{}/default", Self::saga_type()).into(),
                    failure_authority: $crate::__saga_contract_failure_authority!($failure_authority $failure_arg),
                    success_criteria: $crate::__saga_contract_required_steps_allof!([$($required_step),+]),
                    overall_timeout: std::time::Duration::from_millis($overall_timeout_ms as u64),
                    stalled_timeout: std::time::Duration::from_millis($stalled_timeout_ms as u64),
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use crate::{FailureAuthority, SuccessCriteria};

    use super::{
        required_steps_from_success_criteria, validate_workflow_contract, SagaWorkflowStepContract,
        WorkflowDependencySpec,
    };

    fn policy_all_of(saga_type: &str, required_steps: &[&str]) -> crate::TerminalPolicy {
        let mut required: HashSet<Box<str>> = HashSet::new();
        for step in required_steps {
            required.insert((*step).into());
        }
        crate::TerminalPolicy {
            saga_type: saga_type.into(),
            policy_id: format!("{saga_type}/default").into(),
            failure_authority: FailureAuthority::AnyParticipant,
            success_criteria: SuccessCriteria::AllOf(required),
            overall_timeout: Duration::from_secs(30),
            stalled_timeout: Duration::from_secs(10),
        }
    }

    #[test]
    fn validate_accepts_well_formed_contract() {
        let policy = policy_all_of("open_position", &["create_order"]);
        let steps = [
            SagaWorkflowStepContract {
                step_name: "risk_check",
                participant_id: "risk",
                depends_on: WorkflowDependencySpec::OnSagaStart,
            },
            SagaWorkflowStepContract {
                step_name: "positions_check",
                participant_id: "positions",
                depends_on: WorkflowDependencySpec::After("risk_check"),
            },
            SagaWorkflowStepContract {
                step_name: "create_order",
                participant_id: "order-manager",
                depends_on: WorkflowDependencySpec::AllOf(&["positions_check"]),
            },
        ];

        let result = validate_workflow_contract("open_position", "risk_check", &steps, &policy);
        assert!(result.is_ok(), "unexpected validation error: {result:?}");
    }

    #[test]
    fn validate_rejects_policy_saga_type_mismatch() {
        let policy = policy_all_of("close_position", &["create_order"]);
        let steps = [SagaWorkflowStepContract {
            step_name: "create_order",
            participant_id: "order-manager",
            depends_on: WorkflowDependencySpec::OnSagaStart,
        }];

        let result = validate_workflow_contract("open_position", "create_order", &steps, &policy);
        let err = result.expect_err("expected saga_type mismatch validation error");
        assert!(
            err.contains("workflow contract saga_type mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_rejects_empty_first_step() {
        let policy = policy_all_of("open_position", &["create_order"]);
        let steps = [SagaWorkflowStepContract {
            step_name: "create_order",
            participant_id: "order-manager",
            depends_on: WorkflowDependencySpec::OnSagaStart,
        }];

        let result = validate_workflow_contract("open_position", "", &steps, &policy);
        let err = result.expect_err("expected empty first_step validation error");
        assert!(
            err.contains("workflow contract first_step cannot be empty"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_rejects_empty_step_set() {
        let policy = policy_all_of("open_position", &["create_order"]);
        let result = validate_workflow_contract("open_position", "risk_check", &[], &policy);
        let err = result.expect_err("expected empty step set validation error");
        assert!(
            err.contains("workflow contract must declare at least one step"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_rejects_duplicate_step_names() {
        let policy = policy_all_of("open_position", &["create_order"]);
        let steps = [
            SagaWorkflowStepContract {
                step_name: "create_order",
                participant_id: "order-manager-a",
                depends_on: WorkflowDependencySpec::OnSagaStart,
            },
            SagaWorkflowStepContract {
                step_name: "create_order",
                participant_id: "order-manager-b",
                depends_on: WorkflowDependencySpec::OnSagaStart,
            },
        ];

        let result = validate_workflow_contract("open_position", "create_order", &steps, &policy);
        let err = result.expect_err("expected duplicate step validation error");
        assert!(
            err.contains("workflow contract duplicate step_name"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_rejects_first_step_not_declared() {
        let policy = policy_all_of("open_position", &["create_order"]);
        let steps = [SagaWorkflowStepContract {
            step_name: "create_order",
            participant_id: "order-manager",
            depends_on: WorkflowDependencySpec::OnSagaStart,
        }];

        let result = validate_workflow_contract("open_position", "risk_check", &steps, &policy);
        let err = result.expect_err("expected first_step undeclared validation error");
        assert!(
            err.contains("workflow contract first_step is not declared"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_rejects_dependency_on_undeclared_step() {
        let policy = policy_all_of("open_position", &["create_order"]);
        let steps = [
            SagaWorkflowStepContract {
                step_name: "risk_check",
                participant_id: "risk",
                depends_on: WorkflowDependencySpec::OnSagaStart,
            },
            SagaWorkflowStepContract {
                step_name: "create_order",
                participant_id: "order-manager",
                depends_on: WorkflowDependencySpec::After("book_snapshot_check"),
            },
        ];

        let result = validate_workflow_contract("open_position", "risk_check", &steps, &policy);
        let err = result.expect_err("expected missing dependency validation error");
        assert!(
            err.contains("workflow contract dependency references undeclared step"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_rejects_required_terminal_step_not_declared() {
        let policy = policy_all_of("open_position", &["create_order"]);
        let steps = [SagaWorkflowStepContract {
            step_name: "risk_check",
            participant_id: "risk",
            depends_on: WorkflowDependencySpec::OnSagaStart,
        }];

        let result = validate_workflow_contract("open_position", "risk_check", &steps, &policy);
        let err = result.expect_err("expected missing terminal step validation error");
        assert!(
            err.contains("workflow contract terminal required step is not declared"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_rejects_dependency_cycle() {
        let policy = policy_all_of("open_position", &["create_order"]);
        let steps = [
            SagaWorkflowStepContract {
                step_name: "risk_check",
                participant_id: "risk",
                depends_on: WorkflowDependencySpec::After("create_order"),
            },
            SagaWorkflowStepContract {
                step_name: "create_order",
                participant_id: "order-manager",
                depends_on: WorkflowDependencySpec::After("risk_check"),
            },
        ];

        let result = validate_workflow_contract("open_position", "risk_check", &steps, &policy);
        let err = result.expect_err("expected dependency cycle validation error");
        assert!(
            err.contains("workflow contract dependency cycle detected"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn required_steps_from_success_criteria_handles_all_variants() {
        let mut all_of = HashSet::new();
        all_of.insert("a".into());
        all_of.insert("b".into());
        let all_required = required_steps_from_success_criteria(&SuccessCriteria::AllOf(all_of));
        assert!(all_required.contains("a"));
        assert!(all_required.contains("b"));

        let mut any_of = HashSet::new();
        any_of.insert("x".into());
        let any_required = required_steps_from_success_criteria(&SuccessCriteria::AnyOf(any_of));
        assert!(any_required.contains("x"));

        let mut quorum_steps = HashSet::new();
        quorum_steps.insert("q1".into());
        quorum_steps.insert("q2".into());
        let quorum_required = required_steps_from_success_criteria(&SuccessCriteria::Quorum {
            group_steps: quorum_steps,
            required_count: 1,
        });
        assert!(quorum_required.contains("q1"));
        assert!(quorum_required.contains("q2"));
    }
}
