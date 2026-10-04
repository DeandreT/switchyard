use std::fmt;

use domain::{CommittedSend, EntityPath, NamespaceName, QueueConfig, Timestamp};

use crate::{
    QueueLogCommand,
    experimental_log::{queue_command_is_send, queue_entry_upper_bound},
};

use super::{QueueWriteError, QueueWriteRejection};

/// Timestamp-free restricted queue work. Business-invalid configuration and
/// message identifiers remain representable as committed refusals.
pub struct QueueIntent {
    command: QueueLogCommand,
    bytes: usize,
}

impl QueueIntent {
    pub fn create_queue(
        namespace: NamespaceName,
        entity: EntityPath,
        config: QueueConfig,
    ) -> Result<Self, QueueWriteError> {
        Self::checked(QueueLogCommand::create_queue(
            namespace,
            entity,
            Timestamp::UNIX_EPOCH,
            config,
        ))
    }

    pub fn send(
        namespace: NamespaceName,
        entity: EntityPath,
        message: CommittedSend,
    ) -> Result<Self, QueueWriteError> {
        Self::checked(QueueLogCommand::send(
            namespace,
            entity,
            Timestamp::UNIX_EPOCH,
            message,
        ))
    }

    fn checked(command: QueueLogCommand) -> Result<Self, QueueWriteError> {
        let bytes = queue_entry_upper_bound(&command)
            .map_err(|_| QueueWriteError::KnownRejected(QueueWriteRejection::InvalidIntent))?;
        Ok(Self { command, bytes })
    }

    pub(super) fn encoded_bytes(&self) -> usize {
        self.bytes
    }
    pub(super) fn is_send(&self) -> bool {
        queue_command_is_send(&self.command)
    }
    pub(super) fn into_command(self) -> QueueLogCommand {
        self.command
    }
}

impl fmt::Debug for QueueIntent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueIntent")
            .field("is_send", &self.is_send())
            .finish_non_exhaustive()
    }
}
