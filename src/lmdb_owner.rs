//! Shared ownership of heed's process-global, path-keyed environment cache.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

use heed::{Env, EnvOpenOptions};

static OWNERS: LazyLock<Mutex<HashMap<PathBuf, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug)]
pub(crate) struct LmdbEnvOwner {
    key: PathBuf,
    env: Option<Env>,
}

impl LmdbEnvOwner {
    pub(crate) fn open(path: &Path, options: &EnvOpenOptions) -> heed::Result<Self> {
        let key = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        // Serialize open + registration with last-owner release + prepare, so
        // no opener can acquire an env between its last release and closing.
        let mut owners = OWNERS.lock().unwrap_or_else(|poison| poison.into_inner());
        let env = unsafe { options.open(path) }?;
        *owners.entry(key.clone()).or_default() += 1;
        Ok(Self {
            key,
            env: Some(env),
        })
    }

    pub(crate) fn env(&self) -> &Env {
        self.env.as_ref().expect("live LMDB owner has an env")
    }
}

fn release(owners: &mut HashMap<PathBuf, usize>, key: &Path) -> bool {
    let Some(count) = owners.get_mut(key) else {
        return false;
    };
    if *count > 1 {
        *count -= 1;
        false
    } else {
        owners.remove(key);
        true
    }
}

impl Drop for LmdbEnvOwner {
    fn drop(&mut self) {
        let mut owners = OWNERS.lock().unwrap_or_else(|poison| poison.into_inner());
        let env = self.env.take();
        if release(&mut owners, &self.key) {
            // The owner must outlive all other env handles in its struct. Keep
            // the lock until prepare drops the final env; never wait here.
            // External heed users can have prepared this path already, so Drop
            // must absorb prepare_for_closing's panic in that case.
            if let Some(env) = env {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let _ = env.prepare_for_closing();
                }));
            }
        } else {
            drop(env);
        }
    }
}
