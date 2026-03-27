use icanact_core::local_sync;

use crate::SagaDelegatedReply;

pub type SagaDelegatedReplyResult = Result<SagaDelegatedReply, String>;
pub type SagaDelegatedReplyHandle = local_sync::ReplyTo<SagaDelegatedReplyResult>;
