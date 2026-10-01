use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectCall {
    Forward { key: String, accepted: bool },
    Undo { key: String, accepted: bool },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectError {
    /// Undo refused because a prerequisite undo has not completed.
    UndoBlocked { key: String, waiting_on: String },
}

#[derive(Default)]
struct State {
    forward: HashMap<String, usize>,
    undo: HashMap<String, usize>,
    /// key -> key whose undo must complete first.
    undo_after: HashMap<String, String>,
    calls: Vec<EffectCall>,
}

/// Counted forward/undo side effects keyed by run key. Cloning shares state.
#[derive(Clone, Default)]
pub struct EffectLedger(Arc<Mutex<State>>);

impl EffectLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reject `undo(key)` until `undo(first)` has been applied at least once.
    pub fn require_undo_order(&self, key: &str, first: &str) {
        self.0
            .lock()
            .expect("ledger lock")
            .undo_after
            .insert(key.to_owned(), first.to_owned());
    }

    /// Records a forward effect; returns the total applications for `key`.
    pub fn forward(&self, key: &str) -> usize {
        let mut s = self.0.lock().expect("ledger lock");
        let n = s.forward.entry(key.to_owned()).or_default();
        *n += 1;
        let n = *n;
        s.calls.push(EffectCall::Forward {
            key: key.to_owned(),
            accepted: true,
        });
        n
    }

    pub fn undo(&self, key: &str) -> Result<usize, EffectError> {
        let mut s = self.0.lock().expect("ledger lock");
        if let Some(first) = s.undo_after.get(key).cloned()
            && s.undo.get(&first).copied().unwrap_or(0) == 0
        {
            s.calls.push(EffectCall::Undo {
                key: key.to_owned(),
                accepted: false,
            });
            return Err(EffectError::UndoBlocked {
                key: key.to_owned(),
                waiting_on: first,
            });
        }
        let n = s.undo.entry(key.to_owned()).or_default();
        *n += 1;
        let n = *n;
        s.calls.push(EffectCall::Undo {
            key: key.to_owned(),
            accepted: true,
        });
        Ok(n)
    }

    pub fn forward_count(&self, key: &str) -> usize {
        self.0
            .lock()
            .expect("ledger lock")
            .forward
            .get(key)
            .copied()
            .unwrap_or(0)
    }

    pub fn undo_count(&self, key: &str) -> usize {
        self.0
            .lock()
            .expect("ledger lock")
            .undo
            .get(key)
            .copied()
            .unwrap_or(0)
    }

    pub fn calls(&self) -> Vec<EffectCall> {
        self.0.lock().expect("ledger lock").calls.clone()
    }

    /// Keys whose forward effect was applied more than once.
    pub fn double_effects(&self) -> Vec<String> {
        let s = self.0.lock().expect("ledger lock");
        let mut keys: Vec<_> = s
            .forward
            .iter()
            .chain(s.undo.iter())
            .filter(|(_, n)| **n > 1)
            .map(|(k, _)| k.clone())
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }
}
