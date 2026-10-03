use domain::{
    CommittedCheckpointUpdate, CommittedEntryId, CommittedEntryMark, CommittedMembership,
    CommittedQueueWork, Timestamp,
};
use openraft::{
    EntryPayload, RaftLogReader, StoredMembership,
    storage::{RaftLogStorage, RaftStateMachine},
};

use crate::{
    LogId, LogVote,
    experimental_log::{MEMBERSHIP_SCHEMA_VERSION, encode_membership, queue_command_timestamp},
};

use super::{ReplicaPreparationError as Error, ReplicaProgress, stores::OwnedStores};

pub(super) async fn validate(
    node_id: u64,
    stores: &mut OwnedStores,
) -> Result<ReplicaProgress, Error> {
    let (log, state) = stores.adapters()?;
    let profile = log.profile().await.map_err(|_| Error::Storage)?;
    let checkpoint = state.checkpoint().await.map_err(|_| Error::Storage)?;
    if profile.node_id() != node_id || profile.stream() != checkpoint.stream() {
        return Err(Error::ProfileMismatch);
    }
    let log_state = log.get_log_state().await.map_err(|_| Error::Storage)?;
    if log_state.last_purged_log_id.is_some() {
        return Err(Error::PurgedHistory);
    }
    let applied_state = state.applied_state().await.map_err(|_| Error::Storage)?;
    let applied = checkpoint.last().map(|mark| raft_id(mark.id));
    if applied_state.0 != applied {
        return Err(Error::CheckpointMismatch);
    }
    let vote = log.read_vote().await.map_err(|_| Error::Storage)?;
    let Some(tail) = log_state.last_log_id else {
        if applied.is_some() {
            return Err(Error::AppliedAhead);
        }
        if checkpoint.previous().is_some()
            || checkpoint.membership().is_some()
            || checkpoint.highest_timestamp() != Timestamp::UNIX_EPOCH
            || applied_state.1 != StoredMembership::default()
        {
            return Err(Error::CheckpointMismatch);
        }
        return Ok(ReplicaProgress {
            node_id,
            stream: profile.stream(),
            log_tail: None,
            applied: None,
            highest_timestamp: Timestamp::UNIX_EPOCH,
            membership: applied_state.1,
        });
    };
    if tail.index >= crate::MAX_RETAINED_ENTRIES {
        return Err(Error::InvalidHistory);
    }
    if applied.is_some_and(|id| id.index > tail.index) {
        return Err(Error::AppliedAhead);
    }
    let end = tail.index.checked_add(1).ok_or(Error::InvalidHistory)?;
    let mut next = 0;
    let mut previous_id: Option<LogId> = None;
    let mut last_mark: Option<CommittedEntryMark> = None;
    let mut previous_mark = None;
    let mut highest_timestamp = Timestamp::UNIX_EPOCH;
    let mut membership = None;
    let mut stored_membership = StoredMembership::default();
    let mut encoded_bytes = 0usize;
    while next < end {
        let entries = log
            .limited_get_log_entries(next, end)
            .await
            .map_err(|_| Error::Storage)?;
        if entries.is_empty() {
            return Err(Error::InvalidHistory);
        }
        for entry in entries {
            if entry.log_id.index != next
                || previous_id.is_some_and(|previous| entry.log_id <= previous)
            {
                return Err(Error::InvalidHistory);
            }
            if next == 0
                && (entry.log_id != LogId::default()
                    || !matches!(entry.payload, EntryPayload::Membership(_)))
            {
                return Err(Error::InvalidHistory);
            }
            encoded_bytes = encoded_bytes
                .checked_add(
                    crate::experimental_log::validated_entry_len(&entry)
                        .map_err(|_| Error::InvalidHistory)?,
                )
                .filter(|bytes| *bytes <= crate::MAX_RETAINED_BYTES as usize)
                .ok_or(Error::InvalidHistory)?;
            let id = entry.log_id;
            previous_id = Some(id);
            next = next.checked_add(1).ok_or(Error::InvalidHistory)?;
            if applied.is_none_or(|last| id.index > last.index) {
                continue;
            }
            if applied.is_some_and(|last| id.index == last.index && id != last) {
                return Err(Error::CheckpointMismatch);
            }
            let work = match entry.payload {
                EntryPayload::Blank => CommittedQueueWork::Blank,
                EntryPayload::Membership(value) => {
                    let payload = encode_membership(&value).map_err(|_| Error::InvalidHistory)?;
                    membership = Some(CommittedMembership {
                        source: domain_id(id),
                        schema_version: MEMBERSHIP_SCHEMA_VERSION,
                        payload: payload.clone(),
                    });
                    stored_membership = StoredMembership::new(Some(id), value);
                    CommittedQueueWork::Membership {
                        schema_version: MEMBERSHIP_SCHEMA_VERSION,
                        payload,
                    }
                }
                EntryPayload::Normal(command) => {
                    highest_timestamp = highest_timestamp.max(queue_command_timestamp(&command));
                    command.into_committed_work()
                }
            };
            previous_mark = last_mark;
            last_mark = Some(
                work.entry_mark(&CommittedCheckpointUpdate {
                    stream: profile.stream(),
                    expected_previous: previous_mark,
                    entry: domain_id(id),
                })
                .map_err(|_| Error::CheckpointMismatch)?,
            );
        }
    }
    if previous_id != Some(tail) || next != end {
        return Err(Error::InvalidHistory);
    }
    if last_mark != checkpoint.last()
        || previous_mark != checkpoint.previous()
        || highest_timestamp != checkpoint.highest_timestamp()
    {
        return Err(Error::CheckpointMismatch);
    }
    if membership.as_ref() != checkpoint.membership() || stored_membership != applied_state.1 {
        return Err(Error::MembershipMismatch);
    }
    let initial_unapplied = tail == LogId::default() && applied.is_none();
    if !initial_unapplied || vote.is_some_and(|vote| vote != LogVote::default()) {
        let minimum = LogVote::new_committed(tail.leader_id.term, tail.leader_id.node_id);
        if !vote.is_some_and(|vote| {
            matches!(
                vote.partial_cmp(&minimum),
                Some(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater)
            )
        }) {
            return Err(Error::VoteMismatch);
        }
    }
    Ok(ReplicaProgress {
        node_id,
        stream: profile.stream(),
        log_tail: Some(tail),
        applied,
        highest_timestamp,
        membership: applied_state.1,
    })
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
