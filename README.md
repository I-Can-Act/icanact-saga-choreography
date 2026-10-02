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
2. durable terminal resolver/policy (`attach_durable_terminal_resolver_for_contract` in production)
3. participant step bindings: every bound step needs a **participant** subscription tagged with that step. Strict workflow binders (`bind_*_workflow_participant_*_strict`) tag automatically from the workflow contract; plain binders (`bind_*_participant_*`) take a `steps: &[&str]` argument that must list the steps the actor owns (an empty slice is rejected with `Err`: an untagged subscription could never satisfy a required step). `subscribe_fn` and `subscribe_saga_type_fn` subscribe **observers**, which never count as a participant receipt.
4. resolver recovery activation (`activate_terminal_resolver_recovery_for_contract`). Until activation a durable resolver publishes nothing, including live decisions, `CompensationRequested` events and terminal replies.

Registering a workflow contract alone is not enough; `attach_terminal_resolver*` must succeed.
If startup wiring is incomplete, saga start is failed immediately with a terminal event instead of stalling.
A durable resolver that is attached but not yet activated is incomplete wiring: `SagaStarted` is rejected with
`SagaBusPublishError::AdmissionRejected` until `activate_terminal_resolver_recovery*` has run.

## Minimal Startup Example

```rust,no_run
use std::path::Path;
use std::sync::Arc;
use icanact_saga_choreography::{
    define_saga_workflow_contract, LmdbTerminalResolverJournal, SagaChoreographyBus,
    SagaWorkflowContract,
};

fn main() -> Result<(), String> {
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
let journal = Arc::new(
    LmdbTerminalResolverJournal::open(Path::new("./runtime/open-position-resolver"))
        .map_err(|error| error.to_string())?,
);
let _resolver = bus.attach_durable_terminal_resolver_for_contract::<
    OpenPositionContract,
    _,
>("resolver", journal)?;

// Register bound steps if not using strict workflow binding helpers, and bind a
// step-tagged participant for each registered step (observers do not count).
bus.register_bound_workflow_step("open_position", "risk_check")?;
bus.register_bound_workflow_step("open_position", "create_order")?;
bus.activate_terminal_resolver_recovery_for_contract::<OpenPositionContract>()?;
Ok(())
}
```

The non-durable `attach_terminal_resolver*` methods are for isolated tests only. A
process-restart-capable deployment must use a durable resolver journal so recovery can
reconstruct compensation ownership across all participants.
Activation is deliberately separate from attachment: call it only after every participant
binding is live, so recovered compensation requests cannot be lost during startup.
Activation also gates live output: before it, a durable resolver neither publishes nor admits
new sagas. Retained failed resolver publishes are retried only on timeout polls and activation,
never on every ingested event, and are capped (see `docs/adr/0003-outbox.md`).
See `docs/upgrade.md` for breaking changes.

## Timeout Semantics

- `overall_timeout`: hard wall-clock budget from saga start.
- `stalled_timeout`: resettable watchdog budget; resets on participant progress events.

## Testing

- Unit/integration: `cargo test`
- Harness-enabled e2e: `cargo test --features test-harness`

## Docs

- [docs/integration_guide.md](docs/integration_guide.md)
- [docs/architecture.md](docs/architecture.md)
