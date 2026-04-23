# I.Can.Act: Choreography

Choreography-based saga coordination for `icanact-core` actors.

## What It Provides

- Saga event model (`SagaChoreographyEvent`)
- Participant traits (`SagaParticipant`, `SagaWorkflowParticipant`)
- Embedded participant-local saga state (`SagaParticipantSupport<J, D>`)
- Ingress helpers and terminal resolver
- Contract-driven startup hardening for workflow safety

## Startup Contract Gate (Required)

Before publishing `SagaStarted`, register all of the following on the saga bus:

1. workflow contract (`register_workflow_contract_provider`)
2. terminal resolver/policy (`attach_terminal_resolver_for_contract` or `attach_terminal_resolver`)
3. participant step bindings (strict workflow binding or explicit bound step registration)

Registering a workflow contract alone is not enough; `attach_terminal_resolver*` must succeed.
If startup wiring is incomplete, saga start is failed immediately with a terminal event instead of stalling.

## Minimal Startup Example

```rust,ignore
use icanact_saga_choreography::{
    define_saga_workflow_contract, SagaChoreographyBus, SagaWorkflowContract,
};

define_saga_workflow_contract! {
    pub struct OpenPositionContract {
        saga_type: "open_position",
        first_step: risk_check,
        failure_authority: deny_steps [create_order],
        required_steps: [create_order],
        overall_timeout_ms: 30_000,
        stalled_timeout_ms: 10_000,
        steps: {
            risk_check => { participant: "risk", depends_on: on_start () },
            create_order => { participant: "order-manager", depends_on: after [risk_check] }
        }
    }
}

let bus = SagaChoreographyBus::new();
bus.register_workflow_contract_provider::<OpenPositionContract>()?;
let _resolver = bus.attach_terminal_resolver_for_contract::<OpenPositionContract>("resolver")?;

// Register bound steps if not using strict workflow binding helpers.
bus.register_bound_workflow_step("open_position", "risk_check")?;
bus.register_bound_workflow_step("open_position", "create_order")?;
```

## Timeout Semantics

- `overall_timeout`: hard wall-clock budget from saga start.
- `stalled_timeout`: resettable watchdog budget; resets on participant progress events.

## Testing

- Unit/integration: `cargo test`
- Harness-enabled e2e: `cargo test --features test-harness`

## Docs

- [docs/integration_guide.md](docs/integration_guide.md)
- [docs/architecture.md](docs/architecture.md)
