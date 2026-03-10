use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use icanact_core::local_sync::ReplyTo;

use crate::{SagaChoreographyEvent, SagaDelegatedReply, SagaId, TERMINAL_RESOLVER_STEP};

pub type SagaDelegatedReplyResult = Result<SagaDelegatedReply, String>;
pub type SagaDelegatedReplyHandle = ReplyTo<SagaDelegatedReplyResult>;

fn registry() -> &'static Mutex<HashMap<u64, SagaDelegatedReplyHandle>> {
    static REGISTRY: OnceLock<Mutex<HashMap<u64, SagaDelegatedReplyHandle>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn register_terminal_reply(
    saga_id: SagaId,
    reply: SagaDelegatedReplyHandle,
) -> Result<(), SagaDelegatedReplyHandle> {
    let mut guard = match registry().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if guard.contains_key(&saga_id.get()) {
        return Err(reply);
    }
    guard.insert(saga_id.get(), reply);
    Ok(())
}

pub fn complete_terminal_reply(saga_id: SagaId, reply: SagaDelegatedReply) -> bool {
    let reply_to = {
        let mut guard = match registry().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.remove(&saga_id.get())
    };
    let Some(reply_to) = reply_to else {
        return false;
    };
    reply_to.reply(Ok(reply)).is_ok()
}

pub fn complete_terminal_reply_from_event(
    event: &SagaChoreographyEvent,
    responder: impl Into<Box<str>>,
) -> bool {
    let is_resolver_terminal = match event {
        SagaChoreographyEvent::SagaCompleted { context }
        | SagaChoreographyEvent::SagaFailed { context, .. }
        | SagaChoreographyEvent::SagaQuarantined { context, .. } => {
            context.step_name.as_ref() == TERMINAL_RESOLVER_STEP
        }
        _ => false,
    };
    if !is_resolver_terminal {
        return false;
    }
    let Some(outcome) = event.terminal_outcome() else {
        return false;
    };
    complete_terminal_reply(
        event.context().saga_id,
        SagaDelegatedReply {
            responder: responder.into(),
            outcome,
        },
    )
}

pub fn reject_terminal_reply(saga_id: SagaId, reason: impl Into<String>) -> bool {
    let reply_to = {
        let mut guard = match registry().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.remove(&saga_id.get())
    };
    let Some(reply_to) = reply_to else {
        return false;
    };
    reply_to.reply(Err(reason.into())).is_ok()
}
