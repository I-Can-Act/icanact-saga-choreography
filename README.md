# I.Can.Act: Choreography

Robust workflow coordination for `icanact-core`.

This crate brings the SAGA pattern to `icanact-core` actors. It lets multiple actors participate in one workflow, persist their local saga progress, recover after failure, and compensate earlier work if a later step fails.

## What The SAGA Pattern Is

The SAGA pattern is a way to build reliable multi-step workflows without depending on one big distributed transaction. Instead of trying to make every step succeed or fail atomically, each step commits its own local work, and if something later breaks, earlier steps run compensation logic to undo or neutralize their effects.

That makes it useful for actor systems where work is naturally split across boundaries like risk, orders, positions, payments, inventory, or fulfillment. You keep actors autonomous, but still get a robust end-to-end workflow with recovery and rollback behavior when things go wrong.

This crate uses choreography, which means actors coordinate by publishing and reacting to saga events directly, rather than depending on a central orchestrator.

## Canonical Integration

The crate itself stays storage-generic, but a downstream app can keep its actor concrete by choosing one backend pair.

For example:

```rust
use icanact_saga_choreography::{
    HasSagaParticipantSupport, SagaParticipantSupport,
};
use crate::saga_durability::{LmdbDedupe, LmdbJournal};

pub struct MyActor {
    pub saga: SagaParticipantSupport<LmdbJournal, LmdbDedupe>,
}

impl HasSagaParticipantSupport for MyActor {
    type Journal = LmdbJournal;
    type Dedupe = LmdbDedupe;

    fn saga_support(&self) -> &SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &self.saga
    }

    fn saga_support_mut(&mut self) -> &mut SagaParticipantSupport<Self::Journal, Self::Dedupe> {
        &mut self.saga
    }
}
```

Then implement `SagaParticipant` for the actor’s business behavior and route incoming saga events through `apply_sync_participant_saga_ingress(...)` or `apply_async_participant_saga_ingress(...)` so emitted choreography events go back onto the attached bus.

If you do want backend injection, the same support object can also be embedded generically.

## Key Types

- `SagaParticipantSupport<J, D>`: embedded saga state, journal, dedupe, stats, recovery events, bus
- `HasSagaParticipantSupport`: actor access trait for the embedded support object
- `SagaParticipant`: step execution and compensation behavior
- `SagaChoreographyEvent`: event-bus model for saga progression
- `ParticipantJournal` / `ParticipantDedupeStore`: storage contracts
- `SagaTestWorld`: actor-ref-centric e2e saga harness behind the `test-harness` feature

## E2E Testing

For end-to-end saga tests, prefer the built-in testkit over replaying events by hand.

Enable the feature:

```toml
[dev-dependencies]
icanact-saga-choreography = { version = "...", features = ["test-harness"] }
```

Then spawn real actors, wire their normal saga message path, and drive the workflow through the shared saga bus:

```rust,ignore
use std::time::Duration;

use icanact_core::local_sync::{self, SyncActor};
use icanact_saga_choreography::{
    durability::apply_sync_participant_saga_ingress, DependencySpec,
    DeterministicContextBuilder, SagaChoreographyEvent, SagaParticipant, SagaTestWorld,
};

enum MyCmd {
    SagaEvent(SagaChoreographyEvent),
    // other business messages
}

impl SyncActor for MyActor {
    type Contract = local_sync::contract::TellOnly;
    type Tell = MyCmd;
    type Ask = ();
    type Reply = ();

    fn handle_tell(&mut self, msg: Self::Tell) {
        match msg {
            MyCmd::SagaEvent(event) => {
                apply_sync_participant_saga_ingress(self, event, |_actor, _incoming| {}, |_invalid| {});
            }
        }
    }
}

let world = SagaTestWorld::new();
let actor = world.spawn_sync_participant(MyActor::default(), MyCmd::SagaEvent);

let ctx = DeterministicContextBuilder::default()
    .with_saga_id(42)
    .with_saga_type("order_lifecycle")
    .with_step_name("start")
    .build();

world.start_saga(ctx.clone(), b"payload".to_vec());
let terminal = world.wait_for_terminal(ctx.saga_id, Duration::from_secs(1));

// assert on:
// - terminal outcome
// - world.transcript_for_saga(ctx.saga_id)
// - normal actor ask/snapshot APIs
// - shared journal/dedupe stores if your test provides them

actor.shutdown();
```

## Verification

```bash
cargo fmt --check
cargo check --all-targets
cargo test --all-targets
cargo check --all-targets --features test-harness
cargo test --all-targets --features test-harness
RUSTFLAGS='-D warnings' cargo test --all-targets --all-features
```

## More

- [docs/integration_guide.md](docs/integration_guide.md)
- [docs/architecture.md](docs/architecture.md)
