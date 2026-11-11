use std::time::Duration;

use icanact_core::local_sync::{self, PoolConfig, SyncActor};
use icanact_saga_choreography::SagaChoreographyBus;

struct BusOwner {
    _bus: SagaChoreographyBus,
}

impl SyncActor for BusOwner {
    type Contract = local_sync::contract::TellOnly;
    type Tell = ();
    type Ask = ();
    type Reply = ();
    type Channel = ();
    type PubSub = ();
    type Broadcast = ();
}

#[test]
fn dropping_last_bus_clone_from_actor_releases_bus_resources() {
    let _ = icanact_core::local_sync::set_default_pool_config(PoolConfig::new(1));
    let bus = SagaChoreographyBus::new();
    let waiter = bus.release_waiter();
    let (_actor_ref, handle) = local_sync::spawn(BusOwner { _bus: bus.clone() });
    drop(bus);

    std::thread::spawn(move || handle.shutdown());
    let released = waiter.wait_timeout(Duration::from_secs(10));
    assert!(
        released,
        "last-owner actor shutdown must release bus resources"
    );
}
