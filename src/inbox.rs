//! Inbox transaction (ADR-0005).

use std::collections::HashSet;

use crate::{JournalEntry, ParticipantEvent};

/// Execution scheduled by an admitted input (ADR-0005).
#[derive(Clone, Debug, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct StepExecutionIntent {
    pub step_name: Box<str>,
    pub attempt: u32,
    pub input: Vec<u8>,
}

/// One atomic inbox commit: input mark + join progress + optional intent (ADR-0005).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboxTxn {
    pub input_key: Box<str>,
    pub dependency_step: Option<Box<str>>,
    pub execution_intent: Option<StepExecutionIntent>,
    pub admitted_at_millis: u64,
}

impl InboxTxn {
    /// The single journal row that makes this transaction atomic (ADR-0005).
    pub fn into_event(self) -> ParticipantEvent {
        ParticipantEvent::InboxCommitted {
            input_key: self.input_key,
            dependency_step: self.dependency_step,
            execution_intent: self.execution_intent,
            admitted_at_millis: self.admitted_at_millis,
        }
    }
}

/// Journal-derived admission state of one run (ADR-0005).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InboxState {
    admitted: HashSet<Box<str>>,
    completed_dependencies: HashSet<Box<str>>,
    execution_fired: bool,
}

impl InboxState {
    /// Rebuild the state from journal rows (ADR-0005).
    pub fn from_entries(entries: &[JournalEntry]) -> Self {
        let mut state = Self::default();
        for entry in entries {
            state.apply(&entry.event);
        }
        state
    }

    /// Fold one journal row into the state (ADR-0005).
    pub fn apply(&mut self, event: &ParticipantEvent) {
        match event.transition() {
            ParticipantEvent::InboxCommitted {
                input_key,
                dependency_step,
                execution_intent,
                ..
            } => {
                self.admitted.insert(input_key.clone());
                if let Some(step) = dependency_step {
                    self.completed_dependencies.insert(step.clone());
                }
                if execution_intent.is_some() {
                    self.execution_fired = true;
                }
            }
            ParticipantEvent::StepExecutionStarted { .. } => self.execution_fired = true,
            _ => {}
        }
    }

    /// Whether the input key was admitted (ADR-0005).
    pub fn is_admitted(&self, input_key: &str) -> bool {
        self.admitted.contains(input_key)
    }

    /// Dependencies whose completion was journaled (ADR-0005).
    pub fn completed_dependencies(&self) -> &HashSet<Box<str>> {
        &self.completed_dependencies
    }

    /// Whether execution was already scheduled or started (ADR-0005).
    pub fn execution_fired(&self) -> bool {
        self.execution_fired
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(sequence: u64, event: ParticipantEvent) -> JournalEntry {
        JournalEntry {
            sequence,
            recorded_at_millis: 0,
            event,
        }
    }

    fn commit(key: &str, dep: Option<&str>, intent: bool) -> ParticipantEvent {
        InboxTxn {
            input_key: key.into(),
            dependency_step: dep.map(Into::into),
            execution_intent: intent.then(|| StepExecutionIntent {
                step_name: "s".into(),
                attempt: 0,
                input: vec![1],
            }),
            admitted_at_millis: 5,
        }
        .into_event()
    }

    #[test]
    fn inbox_state_rebuilds_admissions_joins_and_intent() {
        let started = ParticipantEvent::StepExecutionStarted {
            attempt: 0,
            started_at_millis: 1,
        };
        let entries = [
            row(1, commit("k1", Some("a"), false)),
            row(2, commit("k2", Some("b"), false)),
        ];
        let partial = InboxState::from_entries(&entries);
        assert!(!partial.execution_fired());
        assert!(partial.is_admitted("k1") && partial.is_admitted("k2"));

        let entries = [
            row(1, commit("k1", Some("a"), false)),
            row(2, commit("k2", Some("b"), false)),
            row(3, commit("k3", None, true)),
        ];
        let state = InboxState::from_entries(&entries);
        assert!(state.is_admitted("k1") && state.is_admitted("k2") && state.is_admitted("k3"));
        assert!(!state.is_admitted("k4"));
        let expected: HashSet<Box<str>> = ["a".into(), "b".into()].into_iter().collect();
        assert_eq!(state.completed_dependencies(), &expected);
        assert!(state.execution_fired());

        let legacy = InboxState::from_entries(&[row(1, started.clone())]);
        assert!(legacy.execution_fired());
        assert!(!legacy.is_admitted("k1"));
        assert!(legacy.completed_dependencies().is_empty());

        let wrapped = InboxState::from_entries(&[row(
            1,
            ParticipantEvent::TransitionCommitted {
                transition: Box::new(started),
                outbox: Vec::new(),
            },
        )]);
        assert!(wrapped.execution_fired());
    }

    #[test]
    fn inbox_txn_into_event_is_field_for_field() {
        let intent = StepExecutionIntent {
            step_name: "s".into(),
            attempt: 2,
            input: vec![9],
        };
        let txn = InboxTxn {
            input_key: "key".into(),
            dependency_step: Some("dep".into()),
            execution_intent: Some(intent.clone()),
            admitted_at_millis: 77,
        };
        let ParticipantEvent::InboxCommitted {
            input_key,
            dependency_step,
            execution_intent,
            admitted_at_millis,
        } = txn.into_event()
        else {
            panic!("into_event must produce InboxCommitted");
        };
        assert_eq!(&*input_key, "key");
        assert_eq!(dependency_step.as_deref(), Some("dep"));
        assert_eq!(execution_intent, Some(intent));
        assert_eq!(admitted_at_millis, 77);
    }
}
