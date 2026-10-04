use domain::{
    CommittedApplyError, CommittedApplyResult, CommittedCheckpoint, CommittedCheckpointUpdate,
    CommittedEntryId, CommittedQueueWork, CommittedStateMachine, CommittedStreamId,
};
use openraft::{EntryPayload, StoredMembership};
use storage::{BoundedStateStore, CommittedStore};

use crate::experimental_log::{
    LogEntry, LogId, MEMBERSHIP_SCHEMA_VERSION, decode_membership, encode_membership,
    queue_command_is_send,
};

use super::{
    AppliedState, LogApplication, StateMachineError,
    input::PreparedApply,
    response::{ExpectedApplication, application_for},
};

pub(super) struct StoreState<W: CommittedStore> {
    pub(super) machine: CommittedStateMachine<W>,
    pub(super) poisoned: bool,
    pub(super) image_export: Option<ImageExportCapability<CommittedStateMachine<W>>>,
}

type ImageExportCapability<M> =
    fn(&mut M) -> Result<domain::EncodedCommittedImage, domain::CommittedImageExportError>;

struct PreparedEntry {
    id: CommittedEntryId,
    expected: ExpectedApplication,
    work: CommittedQueueWork,
}

impl<W: CommittedStore> StoreState<W> {
    pub(super) fn create(writer: W, stream: CommittedStreamId) -> Result<Self, StateMachineError> {
        let machine = CommittedStateMachine::create(writer, stream).map_err(domain_error)?;
        Self::validated(machine)
    }

    pub(super) fn open(writer: W, stream: CommittedStreamId) -> Result<Self, StateMachineError> {
        let machine = CommittedStateMachine::open(writer, stream).map_err(domain_error)?;
        Self::validated(machine)
    }

    pub(super) fn create_with_image_export(
        writer: W,
        stream: CommittedStreamId,
    ) -> Result<Self, StateMachineError>
    where
        W::Reader: BoundedStateStore,
    {
        let mut state = Self::create(writer, stream)?;
        state.image_export = Some(CommittedStateMachine::<W>::export_create_send_image);
        Ok(state)
    }

    pub(super) fn open_with_image_export(
        writer: W,
        stream: CommittedStreamId,
    ) -> Result<Self, StateMachineError>
    where
        W::Reader: BoundedStateStore,
    {
        let mut state = Self::open(writer, stream)?;
        state.image_export = Some(CommittedStateMachine::<W>::export_create_send_image);
        Ok(state)
    }

    fn validated(machine: CommittedStateMachine<W>) -> Result<Self, StateMachineError> {
        let checkpoint = machine.checkpoint().map_err(domain_error)?;
        recover(&checkpoint)?;
        Ok(Self {
            machine,
            poisoned: false,
            image_export: None,
        })
    }

    pub(super) fn applied_state(&mut self) -> Result<AppliedState, StateMachineError> {
        self.ensure_healthy()?;
        let result = self
            .machine
            .checkpoint()
            .map_err(domain_error)
            .and_then(|checkpoint| recover(&checkpoint));
        self.finish(result)
    }

    pub(super) fn checkpoint(&mut self) -> Result<CommittedCheckpoint, StateMachineError> {
        self.ensure_healthy()?;
        let result = self
            .machine
            .checkpoint()
            .map_err(domain_error)
            .and_then(|checkpoint| {
                recover(&checkpoint)?;
                Ok(checkpoint)
            });
        self.finish(result)
    }

    pub(super) fn apply(
        &mut self,
        input: PreparedApply,
    ) -> Result<Vec<LogApplication>, StateMachineError> {
        self.ensure_healthy()?;
        let result = self.apply_inner(input);
        self.finish(result)
    }

    fn apply_inner(
        &mut self,
        input: PreparedApply,
    ) -> Result<Vec<LogApplication>, StateMachineError> {
        // Complete all conversions before the first durable entry. The packet
        // constructor already checks count, bytes, and adjacent full identities.
        let entries = input
            .into_entries()
            .into_iter()
            .map(prepare_entry)
            .collect::<Result<Vec<_>, _>>()?;
        let mut checkpoint = self.machine.checkpoint().map_err(domain_error)?;
        recover(&checkpoint)?;
        if let Some(first) = entries.first() {
            validate_first(&checkpoint, first.id)?;
        }

        let mut responses = Vec::with_capacity(entries.len());
        for entry in entries {
            let replay = checkpoint
                .last()
                .is_some_and(|last| last.id.index == entry.id.index);
            let update = CommittedCheckpointUpdate {
                stream: checkpoint.stream(),
                expected_previous: if replay {
                    checkpoint.previous()
                } else {
                    checkpoint.last()
                },
                entry: entry.id,
            };
            let response = match self
                .machine
                .apply_committed(&update, &entry.work)
                .map_err(domain_error)?
            {
                CommittedApplyResult::Applied { application, .. } => {
                    application_for(entry.expected, application)?
                }
                CommittedApplyResult::AlreadyApplied { .. } => {
                    LogApplication::AlreadyApplied { entry: entry.id }
                }
            };
            checkpoint = self.machine.checkpoint().map_err(domain_error)?;
            recover(&checkpoint)?;
            responses.push(response);
        }
        Ok(responses)
    }

    pub(super) fn ensure_healthy(&self) -> Result<(), StateMachineError> {
        if self.poisoned {
            Err(StateMachineError::Poisoned)
        } else {
            Ok(())
        }
    }

    fn finish<T>(&mut self, result: Result<T, StateMachineError>) -> Result<T, StateMachineError> {
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
}

fn prepare_entry(entry: LogEntry) -> Result<PreparedEntry, StateMachineError> {
    let (expected, work) = match entry.payload {
        EntryPayload::Blank => (
            ExpectedApplication::CheckpointOnly,
            CommittedQueueWork::Blank,
        ),
        EntryPayload::Membership(membership) => (
            ExpectedApplication::CheckpointOnly,
            CommittedQueueWork::Membership {
                schema_version: MEMBERSHIP_SCHEMA_VERSION,
                payload: encode_membership(&membership).map_err(|_| StateMachineError::Codec)?,
            },
        ),
        EntryPayload::Normal(command) => {
            let expected = if queue_command_is_send(&command) {
                ExpectedApplication::Send
            } else {
                ExpectedApplication::CreateQueue
            };
            (expected, command.into_committed_work())
        }
    };
    Ok(PreparedEntry {
        id: domain_id(entry.log_id),
        expected,
        work,
    })
}

fn validate_first(
    checkpoint: &CommittedCheckpoint,
    first: CommittedEntryId,
) -> Result<(), StateMachineError> {
    match checkpoint.last() {
        Some(last) if first == last.id => Ok(()),
        Some(last) if successor(last.id, first) => Ok(()),
        None if first.index == 0 => Ok(()),
        _ => Err(StateMachineError::InvalidApply),
    }
}

pub(super) fn recover(checkpoint: &CommittedCheckpoint) -> Result<AppliedState, StateMachineError> {
    match (checkpoint.last(), checkpoint.previous()) {
        (None, None) => {}
        (Some(last), None) if last.id.index == 0 => {}
        (Some(last), Some(previous)) if successor(previous.id, last.id) => {}
        _ => return Err(StateMachineError::InvalidState),
    }
    let membership = match checkpoint.membership() {
        None => StoredMembership::default(),
        Some(membership) => {
            let last = checkpoint.last().ok_or(StateMachineError::InvalidState)?;
            if membership.source.index > last.id.index
                || raft_id(membership.source) > raft_id(last.id)
                || (membership.source.index == last.id.index && membership.source != last.id)
                || checkpoint.previous().is_some_and(|previous| {
                    membership.source.index <= previous.id.index
                        && (raft_id(membership.source) > raft_id(previous.id)
                            || (membership.source.index == previous.id.index
                                && membership.source != previous.id))
                })
            {
                return Err(StateMachineError::InvalidState);
            }
            let value = decode_membership(membership.schema_version, &membership.payload)
                .map_err(|_| StateMachineError::InvalidState)?;
            StoredMembership::new(Some(raft_id(membership.source)), value)
        }
    };
    Ok((checkpoint.last().map(|last| raft_id(last.id)), membership))
}

fn successor(previous: CommittedEntryId, next: CommittedEntryId) -> bool {
    previous.index.checked_add(1) == Some(next.index) && raft_id(next) > raft_id(previous)
}

fn domain_id(id: LogId) -> CommittedEntryId {
    CommittedEntryId {
        term: id.leader_id.term,
        node_id: id.leader_id.node_id,
        index: id.index,
    }
}

fn raft_id(id: CommittedEntryId) -> LogId {
    LogId::new(
        openraft::CommittedLeaderId::new(id.term, id.node_id),
        id.index,
    )
}

fn domain_error(error: CommittedApplyError) -> StateMachineError {
    match error {
        CommittedApplyError::Storage(_) => StateMachineError::Storage,
        _ => StateMachineError::InvalidState,
    }
}

#[cfg(test)]
mod tests;
