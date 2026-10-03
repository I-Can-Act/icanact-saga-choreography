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
3. participant step bindings (strict workflow binding or explicit bound step registration)
4. resolver recovery activation (`activate_terminal_resolver_recovery_for_contract`)

Registering a workflow contract alone is not enough; `attach_terminal_resolver*` must succeed.
If startup wiring is incomplete, saga start is failed immediately with a terminal event instead of stalling.

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

// Register bound steps if not using strict workflow binding helpers.
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

Startup order is: attach resolver -> bind/hydrate participants -> activate recovery -> accept
new starts. Terminal and quarantine state is durably fenced so restarts do not repeat
managed terminal ingress side effects (managed ingress fences stale or replayed
terminal side-effect hooks; arbitrary application hooks must still be idempotent); rollback is serialized in reverse order; effect dispatch is explicit and
fails closed by default; terminal retention needs capacity/compaction maintenance
(quarantines are never auto-pruned). The library does not promise exactly-once effects:
use stable external idempotency keys and reconciliation. See
[docs/operations.md](docs/operations.md).

## Safety Policies (summary)

- Success keeps business effects; late evidence after failure quarantines, and a `SagaFailed`
  reply may be superseded by quarantine. Declared effects without undo are irreversible.
- `FailStep(requires_compensation=false)` is a safe/no-undo remote contract, not cancellation.
- Drain all in-flight sagas before upgrading from baseline; see
  [docs/migration.md](docs/migration.md). No repair/safe-clear API exists.

## Timeout Semantics

- `overall_timeout`: forward wall-clock budget; forward expiry with known effects starts
  rollback with renewed budgets instead of bypassing undo.
- `stalled_timeout`: resettable watchdog budget; resets on participant progress events.
- Expiry or failed undo while rollback is unresolved quarantines and retains evidence.

## Testing

- Unit/integration: `cargo test`
- Harness-enabled e2e: `cargo test --features test-harness`

## Docs

- [docs/integration_guide.md](docs/integration_guide.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/operations.md](docs/operations.md)
- [docs/migration.md](docs/migration.md) (breaking changes and release prerequisites)
