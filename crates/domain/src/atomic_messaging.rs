//! Bounded atomic messaging for one non-session primary queue.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{
    BrokerError, Command, CommandKind, CommandOutcome, EntityBinding, EntityPath, Timestamp,
};

mod input;

pub const MAX_ATOMIC_MESSAGING_ACTIONS: usize = 100;
pub const MAX_ATOMIC_MESSAGING_MESSAGES: usize = 100;
pub const MAX_ATOMIC_MESSAGING_CONTENT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_ATOMIC_MESSAGING_VALUE_ITEMS: usize = 65_536;
pub const MAX_ATOMIC_MESSAGING_READ_OPERATIONS: usize = 4_096;
pub const MAX_ATOMIC_MESSAGING_READ_KEY_BYTES: usize = 1024 * 1024;
pub const MAX_ATOMIC_MESSAGING_READ_VALUE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_ATOMIC_MESSAGING_MUTATION_KEYS: usize = 4_096;
pub const MAX_ATOMIC_MESSAGING_MUTATION_KEY_BYTES: usize = 1024 * 1024;
pub const MAX_ATOMIC_MESSAGING_MUTATION_VALUE_BYTES: usize = 16 * 1024 * 1024;

/// A separate envelope leaves ordinary command discriminants unchanged.
/// This is an atomic owner operation, not an idempotency key or wire transaction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AtomicMessagingCommand {
    pub binding: EntityBinding,
    pub issued_at: Timestamp,
    pub commands: Vec<Command>,
}

/// Effects are derived from the final committed ready-index mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AtomicMessagingApplication {
    pub outcomes: Vec<CommandOutcome>,
    pub enqueue_targets: Vec<EntityPath>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AtomicMessagingLimit {
    Actions,
    Messages,
    ContentBytes,
    ValueItems,
    ReadOperations,
    ReadKeyBytes,
    ReadValueBytes,
    MutationKeys,
    MutationKeyBytes,
    MutationValueBytes,
}

impl AtomicMessagingLimit {
    pub(crate) fn maximum(self) -> usize {
        match self {
            Self::Actions => MAX_ATOMIC_MESSAGING_ACTIONS,
            Self::Messages => MAX_ATOMIC_MESSAGING_MESSAGES,
            Self::ContentBytes => MAX_ATOMIC_MESSAGING_CONTENT_BYTES,
            Self::ValueItems => MAX_ATOMIC_MESSAGING_VALUE_ITEMS,
            Self::ReadOperations => MAX_ATOMIC_MESSAGING_READ_OPERATIONS,
            Self::ReadKeyBytes => MAX_ATOMIC_MESSAGING_READ_KEY_BYTES,
            Self::ReadValueBytes => MAX_ATOMIC_MESSAGING_READ_VALUE_BYTES,
            Self::MutationKeys => MAX_ATOMIC_MESSAGING_MUTATION_KEYS,
            Self::MutationKeyBytes => MAX_ATOMIC_MESSAGING_MUTATION_KEY_BYTES,
            Self::MutationValueBytes => MAX_ATOMIC_MESSAGING_MUTATION_VALUE_BYTES,
        }
    }

    pub(crate) fn exceeded(self) -> BrokerError {
        BrokerError::AtomicMessagingTooLarge {
            limit: self,
            maximum: self.maximum(),
        }
    }
}

impl fmt::Display for AtomicMessagingLimit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Actions => "action count",
            Self::Messages => "logical send count",
            Self::ContentBytes => "command content bytes",
            Self::ValueItems => "message value items",
            Self::ReadOperations => "point reads",
            Self::ReadKeyBytes => "point-read key bytes",
            Self::ReadValueBytes => "point-read value bytes",
            Self::MutationKeys => "unique mutation keys",
            Self::MutationKeyBytes => "unique mutation key bytes",
            Self::MutationValueBytes => "generated Put value bytes",
        })
    }
}

/// Checks the allowlist and shared borrowed input limits without store access.
pub fn validate_atomic_messaging_kinds(kinds: &[CommandKind]) -> Result<(), BrokerError> {
    validate_kinds(kinds.iter(), kinds.len())
}

pub(crate) fn validate_kinds<'a>(
    kinds: impl Iterator<Item = &'a CommandKind>,
    actions: usize,
) -> Result<(), BrokerError> {
    input::validate(kinds, actions)
}
