//! Outbound obligations embedded in transition rows (ADR-0003).

use crate::{RunKey, SagaChoreographyEvent};

/// Stable identity of one outbound obligation: run, row sequence, index in the row (ADR-0003).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OutboxId {
    run: RunKey,
    sequence: u64,
    index: u32,
}

impl OutboxId {
    /// Build an obligation id (ADR-0003).
    pub fn new(run: RunKey, sequence: u64, index: u32) -> Self {
        Self {
            run,
            sequence,
            index,
        }
    }

    /// Run owning the obligation (ADR-0003).
    pub fn run(&self) -> &RunKey {
        &self.run
    }

    /// Journal row sequence holding the obligation (ADR-0003).
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Index of the obligation within its row (ADR-0003).
    pub fn index(&self) -> u32 {
        self.index
    }
}

impl std::fmt::Display for OutboxId {
    /// `{run}#{sequence}.{index}` (ADR-0003).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}.{}", self.run, self.sequence, self.index)
    }
}

/// One outbound event owed by a committed transition (ADR-0003).
#[derive(Clone, Debug)]
pub struct OutboxRecord {
    /// Identity of the obligation (ADR-0003).
    pub id: OutboxId,
    /// The event to publish (ADR-0003).
    pub event: SagaChoreographyEvent,
}
