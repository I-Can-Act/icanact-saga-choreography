//! R03 / ADR-0004 §2.6 (T03D): stale startup recovery requests an abort (so the resolver rolls
//! back known effects) carrying the stored run identity; it never publishes a terminal
//! `SagaFailed` with a fabricated context.
#![cfg(all(feature = "lmdb", feature = "test-harness"))]

#[allow(unused_imports)] // each test crate uses a subset of the shared fixtures
mod support;

use icanact_saga_choreography::durability::collect_startup_recovery_events_for_saga_type;
use icanact_saga_choreography::{
    AbortSource, InMemoryDedupe, JournalEntry, JournalError, ParticipantEvent, ParticipantJournal,
    RunIncarnation, RunKey, RunTombstone, SagaChoreographyEvent, SagaId,
};

/// Read-only journal holding entries with explicit (old) record times.
struct AgedJournal {
    runs: Vec<(RunKey, Vec<JournalEntry>)>,
}

impl ParticipantJournal for AgedJournal {
    fn append(&self, _: SagaId, _: ParticipantEvent) -> Result<u64, JournalError> {
        Err(JournalError::Storage("read-only".into()))
    }
    fn read(&self, saga_id: SagaId) -> Result<Vec<JournalEntry>, JournalError> {
        Ok(self
            .runs
            .iter()
            .filter(|(run, _)| run.saga_id() == saga_id)
            .flat_map(|(_, entries)| entries.clone())
            .collect())
    }
    fn list_sagas(&self) -> Result<Vec<SagaId>, JournalError> {
        Ok(self.runs.iter().map(|(run, _)| run.saga_id()).collect())
    }
    fn prune(&self, _: SagaId) -> Result<(), JournalError> {
        Ok(())
    }
    fn append_run(&self, _: &RunKey, _: ParticipantEvent) -> Result<u64, JournalError> {
        Err(JournalError::Storage("read-only".into()))
    }
    fn read_run(&self, run: &RunKey) -> Result<Vec<JournalEntry>, JournalError> {
        Ok(self
            .runs
            .iter()
            .find(|(candidate, _)| candidate == run)
            .map(|(_, entries)| entries.clone())
            .unwrap_or_default())
    }
    fn list_runs(&self) -> Result<Vec<RunKey>, JournalError> {
        Ok(self.runs.iter().map(|(run, _)| run.clone()).collect())
    }
    fn finalize_run(&self, _: &RunTombstone, _: RunIncarnation) -> Result<(), JournalError> {
        Ok(())
    }
    fn run_tombstones(&self, _: &str, _: SagaId) -> Result<Vec<RunTombstone>, JournalError> {
        Ok(Vec::new())
    }
    fn prune_expired_tombstones(&self, _: RunIncarnation) -> Result<u64, JournalError> {
        Ok(0)
    }
}

#[test]
fn stale_recovery_requests_abort_with_stored_run_context() {
    let run = RunKey::new("r03_saga", SagaId::new(3), RunIncarnation::new(1_234_567));
    let journal = AgedJournal {
        runs: vec![(
            run.clone(),
            vec![JournalEntry {
                sequence: 1,
                recorded_at_millis: 0,
                event: ParticipantEvent::StepExecutionStarted {
                    attempt: 1,
                    started_at_millis: 0,
                },
            }],
        )],
    };
    let events = collect_startup_recovery_events_for_saga_type(
        &journal,
        &InMemoryDedupe::new(),
        "reserve",
        "r03_saga",
    )
    .expect("recovery should collect");

    assert!(
        !events
            .iter()
            .any(|event| matches!(event, SagaChoreographyEvent::SagaFailed { .. })),
        "stale recovery must not publish a terminal SagaFailed: {events:?}"
    );
    let [
        SagaChoreographyEvent::SagaAbortRequested {
            context, source, ..
        },
    ] = events.as_slice()
    else {
        panic!("expected exactly one SagaAbortRequested, got {events:?}");
    };
    assert_eq!(*source, AbortSource::StaleRecovery);
    assert_eq!(context.run_key(), run);
}
