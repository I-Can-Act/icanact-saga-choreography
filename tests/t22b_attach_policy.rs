use std::collections::HashSet;
use std::time::Duration;

use icanact_saga_choreography::{
    FailureAuthority, MIN_REPLAY_HORIZON, ReplayHorizon, SagaChoreographyBus, SuccessCriteria,
    TerminalPolicy,
};

fn policy(criteria: SuccessCriteria, overall: Duration, stalled: Duration) -> TerminalPolicy {
    TerminalPolicy::new(
        "t22b_saga".into(),
        "t22b_saga/default".into(),
        FailureAuthority::AnyParticipant,
        criteria,
        overall,
        stalled,
        &[],
    )
}

fn all_of(steps: &[&str]) -> SuccessCriteria {
    SuccessCriteria::AllOf(
        steps
            .iter()
            .map(|s| Box::<str>::from(*s))
            .collect::<HashSet<_>>(),
    )
}

#[test]
fn attach_rejects_invalid_terminal_policy() {
    let secs = Duration::from_secs;
    let invalid = [
        ("empty all_of", policy(all_of(&[]), secs(30), secs(30))),
        (
            "zero overall",
            policy(all_of(&["a"]), Duration::ZERO, secs(30)),
        ),
        (
            "zero stalled",
            policy(all_of(&["a"]), secs(30), Duration::ZERO),
        ),
        (
            "horizon shorter than overall",
            policy(all_of(&["a"]), MIN_REPLAY_HORIZON * 10, secs(30)).with_replay_horizon(
                ReplayHorizon::new(MIN_REPLAY_HORIZON).expect("floor is valid"),
            ),
        ),
    ];

    for (name, bad) in invalid {
        let bus = SagaChoreographyBus::new();
        let result = bus.attach_terminal_resolver(bad.clone(), "t22b");
        assert!(result.is_err(), "{name}: invalid policy must be rejected");
        let result = bus.attach_terminal_resolver(bad, "t22b");
        assert!(
            result.is_err(),
            "{name}: rejection must not register a resolver"
        );
        // A valid policy for the same saga type still attaches afterwards.
        let good = policy(all_of(&["a"]), secs(30), secs(30));
        assert!(
            bus.attach_terminal_resolver(good, "t22b").is_ok(),
            "{name}: valid policy must attach after a rejected one"
        );
    }
}
