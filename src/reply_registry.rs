use icanact_core::local_sync;

use crate::SagaReplyTo;

pub type SagaReplyToResult = Result<SagaReplyTo, String>;
pub type SagaReplyToHandle = local_sync::ReplyTo<SagaReplyToResult>;
