//! Deterministic fault-injection fixtures shared by integration tests.
//! Use via `mod support;` from a top-level `tests/*.rs` file.
// Each test crate uses only a subset of these helpers.
#![allow(dead_code)]

pub mod effects;
pub mod faults;

pub use effects::{EffectError, EffectLedger};
pub use faults::{CutPoint, Cuts, FaultDedupe, FaultJournal, FaultTrigger, JournalOp, ManualClock};
pub mod canonical;
