use storage::CommittedStore;

use crate::{
    CommittedApplication, CommittedApplyError, CommittedApplyResult, CommittedCheckpoint,
    CommittedCheckpointUpdate, CommittedEntryMark, CommittedMembership, CommittedQueueWork,
    CommittedStreamId,
    committed::{decode_checkpoint, encode_checkpoint, entry_fingerprint},
};

use super::{committed_prepare::CommittedPreparationError, *};

/// One synchronous committed-entry writer with a matching read-only machine.
/// This is not a consensus engine, retry-deduplication service, or snapshot API.
pub struct CommittedStateMachine<W: CommittedStore> {
    writer: W,
    machine: StateMachine<W::Reader>,
    stream: CommittedStreamId,
    poisoned: bool,
}

impl<W: CommittedStore> CommittedStateMachine<W> {
    /// Durably initializes only a pristine, never-committed replica store.
    pub fn create(mut writer: W, stream: CommittedStreamId) -> Result<Self, CommittedApplyError> {
        stream.validate()?;
        let reader = writer.reader();
        if writer.is_initialized()? || !reader.scan_prefix(&[], 1)?.is_empty() {
            return Err(CommittedApplyError::NotPristine);
        }
        let checkpoint = CommittedCheckpoint::initial(stream);
        writer.commit(WriteBatch::default().put(
            keys::committed_checkpoint(),
            encode_checkpoint(&checkpoint)?,
        ))?;
        Ok(Self {
            writer,
            machine: StateMachine::new(reader),
            stream,
            poisoned: false,
        })
    }

    /// Opens exact initialized progress; a missing checkpoint is never adopted.
    pub fn open(writer: W, stream: CommittedStreamId) -> Result<Self, CommittedApplyError> {
        stream.validate()?;
        let machine = Self {
            machine: StateMachine::new(writer.reader()),
            writer,
            stream,
            poisoned: false,
        };
        machine.checkpoint()?;
        Ok(machine)
    }

    pub fn reader(&self) -> W::Reader {
        self.machine.store().clone()
    }

    /// Reads current durable progress even after a physical write error.
    pub fn checkpoint(&self) -> Result<CommittedCheckpoint, CommittedApplyError> {
        let bytes = self.machine.store().get(&keys::committed_checkpoint())?;
        if !self.writer.is_initialized()? {
            return Err(
                if bytes.is_none() && self.machine.store().scan_prefix(&[], 1)?.is_empty() {
                    CommittedApplyError::NotInitialized
                } else {
                    CommittedApplyError::CorruptCheckpoint
                },
            );
        }
        let checkpoint = decode_checkpoint(&bytes.ok_or(CommittedApplyError::CorruptCheckpoint)?)?;
        if checkpoint.stream != self.stream {
            return Err(CommittedApplyError::WrongStream);
        }
        Ok(checkpoint)
    }

    /// Commits business mutations and exact progress in one storage batch.
    /// Normal refusals advance progress without business changes. Any physical
    /// commit error poisons further application until the writer is reopened.
    pub fn apply_committed(
        &mut self,
        update: &CommittedCheckpointUpdate,
        work: &CommittedQueueWork,
    ) -> Result<CommittedApplyResult, CommittedApplyError> {
        if self.poisoned {
            return Err(CommittedApplyError::Poisoned);
        }
        if update.stream != self.stream {
            return Err(CommittedApplyError::WrongStream);
        }
        let current = self.checkpoint()?;
        let position = CommittedEntryMark {
            id: update.entry,
            fingerprint: entry_fingerprint(update, work)?,
        };
        if let Some(last) = current.last
            && update.entry.index == last.id.index
        {
            return if position == last && update.expected_previous == current.previous {
                Ok(CommittedApplyResult::AlreadyApplied { position })
            } else {
                Err(CommittedApplyError::ReplayConflict)
            };
        }
        if update.expected_previous != current.last {
            return Err(CommittedApplyError::PreviousMismatch);
        }
        let next_index = current.last.map_or(Ok(0), |last| {
            last.id
                .index
                .checked_add(1)
                .ok_or(CommittedApplyError::IndexExhausted)
        })?;
        if update.entry.index != next_index
            || current
                .last
                .is_some_and(|last| update.entry.term < last.id.term)
        {
            return Err(CommittedApplyError::NonContiguous);
        }

        let mut next = current.clone();
        next.previous = current.last;
        next.last = Some(position);
        let (mut batch, application) = match work {
            CommittedQueueWork::Blank => {
                (WriteBatch::default(), CommittedApplication::CheckpointOnly)
            }
            CommittedQueueWork::Membership {
                schema_version,
                payload,
            } => {
                next.membership = Some(CommittedMembership {
                    source: update.entry,
                    schema_version: *schema_version,
                    payload: payload.clone(),
                });
                (WriteBatch::default(), CommittedApplication::CheckpointOnly)
            }
            CommittedQueueWork::Queue(queue) => {
                let command = queue.as_command();
                next.highest_timestamp = current.highest_timestamp.max(command.issued_at);
                match self
                    .machine
                    .prepare_committed_queue(command, current.highest_timestamp)
                {
                    Ok(prepared) => (
                        prepared.batch,
                        CommittedApplication::Queue(Box::new(prepared.application)),
                    ),
                    Err(CommittedPreparationError::Refused(error)) => {
                        (WriteBatch::default(), CommittedApplication::Refused(error))
                    }
                    Err(CommittedPreparationError::Fatal(error)) => return Err(error),
                }
            }
        };
        batch.push_put(keys::committed_checkpoint(), encode_checkpoint(&next)?);
        if let Err(error) = self.writer.commit(batch) {
            self.poisoned = true;
            return Err(CommittedApplyError::Storage(error));
        }
        Ok(CommittedApplyResult::Applied {
            position,
            application,
        })
    }
}

impl<W: CommittedStore> std::fmt::Debug for CommittedStateMachine<W> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommittedStateMachine")
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}
