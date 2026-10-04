use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Mutex,
};

use domain::{
    CommittedCheckpoint, CommittedCheckpointUpdate, CommittedEntryId, CommittedEntryMark,
    CommittedMembership, CommittedQueueWork, Timestamp,
};
use openraft::EntryPayload;
use storage::CommittedStore;
use tokio::sync::oneshot;

use super::{
    LogId, LogProfile, LogRetention, LogStorageError, LogVote, MAX_RETAINED_ENTRIES,
    MEMBERSHIP_SCHEMA_VERSION, encode_membership, queue_command_timestamp, state::StoreState,
    validated_entry_len,
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct FinalLogPrefix {
    pub(crate) mark: CommittedEntryMark,
    pub(crate) highest_timestamp: Timestamp,
}

/// Healthy retained content metadata, not a quorum or native-join receipt.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct FinalLogReport {
    pub(crate) profile: LogProfile,
    pub(crate) vote: Option<LogVote>,
    pub(crate) retention: LogRetention,
    pub(crate) prefixes: Vec<FinalLogPrefix>,
    pub(crate) membership_events: Vec<CommittedMembership>,
}

impl FinalLogReport {
    pub(crate) fn profile(&self) -> &LogProfile {
        &self.profile
    }

    pub(crate) fn retention(&self) -> LogRetention {
        self.retention
    }

    pub(crate) fn matches_checkpoint(&self, checkpoint: &CommittedCheckpoint) -> bool {
        if checkpoint.stream() != self.profile.stream() {
            return false;
        }
        let Some(last) = checkpoint.last() else {
            return checkpoint.previous().is_none()
                && checkpoint.highest_timestamp() == Timestamp::UNIX_EPOCH
                && checkpoint.membership().is_none();
        };
        let Ok(index) = usize::try_from(last.id.index) else {
            return false;
        };
        let Some(prefix) = self.prefixes.get(index) else {
            return false;
        };
        let previous = index
            .checked_sub(1)
            .and_then(|index| self.prefixes.get(index))
            .map(|prefix| prefix.mark);
        let membership = self
            .membership_events
            .iter()
            .rev()
            .find(|member| member.source.index <= last.id.index);
        prefix.mark == last
            && checkpoint.previous() == previous
            && checkpoint.highest_timestamp() == prefix.highest_timestamp
            && checkpoint.membership() == membership
    }
}

impl fmt::Debug for FinalLogReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FinalLogReport")
            .field("prefixes", &self.prefixes.len())
            .field("membership_events", &self.membership_events.len())
            .finish_non_exhaustive()
    }
}

pub(super) fn build<W: CommittedStore>(
    state: &StoreState<W>,
) -> Result<FinalLogReport, LogStorageError> {
    let profile = state.profile().map_err(LogStorageError::from)?;
    let retention = state.retention().map_err(LogStorageError::from)?;
    let vote = state.read_vote().map_err(LogStorageError::from)?;
    if retention.last_purged.is_some() || retention.retained_entries > MAX_RETAINED_ENTRIES {
        return Err(LogStorageError::Corrupt);
    }
    let end = retention.last_present.map_or(Ok(0), |last| {
        last.index.checked_add(1).ok_or(LogStorageError::Corrupt)
    })?;
    if end != retention.retained_entries {
        return Err(LogStorageError::Corrupt);
    }
    let mut report = FinalLogReport {
        profile,
        vote,
        retention,
        prefixes: Vec::new(),
        membership_events: Vec::new(),
    };
    let mut next = 0;
    let mut previous_id = None;
    let mut previous_mark = None;
    let mut bytes = 0_u64;
    let mut highest_timestamp = Timestamp::UNIX_EPOCH;
    while next < end {
        let entries = state
            .read_limited(next, end)
            .map_err(LogStorageError::from)?;
        if entries.is_empty() {
            return Err(LogStorageError::Corrupt);
        }
        for entry in entries {
            if entry.log_id.index != next
                || previous_id.is_some_and(|previous| entry.log_id <= previous)
                || next >= MAX_RETAINED_ENTRIES
            {
                return Err(LogStorageError::Corrupt);
            }
            bytes = bytes
                .checked_add(
                    u64::try_from(validated_entry_len(&entry).map_err(|_| LogStorageError::Codec)?)
                        .map_err(|_| LogStorageError::Corrupt)?,
                )
                .ok_or(LogStorageError::Corrupt)?;
            let id = entry.log_id;
            let domain_id = domain_id(id);
            let work = match entry.payload {
                EntryPayload::Blank => CommittedQueueWork::Blank,
                EntryPayload::Membership(member) => {
                    let payload = encode_membership(&member).map_err(|_| LogStorageError::Codec)?;
                    report.membership_events.push(CommittedMembership {
                        source: domain_id,
                        schema_version: MEMBERSHIP_SCHEMA_VERSION,
                        payload: payload.clone(),
                    });
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
            let mark = work
                .entry_mark(&CommittedCheckpointUpdate {
                    stream: report.profile.stream(),
                    expected_previous: previous_mark,
                    entry: domain_id,
                })
                .map_err(|_| LogStorageError::Codec)?;
            report.prefixes.push(FinalLogPrefix {
                mark,
                highest_timestamp,
            });
            previous_id = Some(id);
            previous_mark = Some(mark);
            next = next.checked_add(1).ok_or(LogStorageError::Corrupt)?;
        }
    }
    if previous_id != retention.last_present || bytes != retention.retained_bytes {
        return Err(LogStorageError::Corrupt);
    }
    Ok(report)
}

fn domain_id(id: LogId) -> CommittedEntryId {
    CommittedEntryId {
        term: id.leader_id.term,
        node_id: id.leader_id.node_id,
        index: id.index,
    }
}

enum SinkState {
    Available,
    Enabled(oneshot::Sender<Result<FinalLogReport, LogStorageError>>),
    Finished,
}

pub(super) struct ReportSink(Mutex<SinkState>);

impl Default for ReportSink {
    fn default() -> Self {
        Self(Mutex::new(SinkState::Available))
    }
}

impl ReportSink {
    pub(super) fn enable(
        &self,
    ) -> Result<oneshot::Receiver<Result<FinalLogReport, LogStorageError>>, LogStorageError> {
        let mut state = self.0.lock().map_err(|_| LogStorageError::Panicked)?;
        if !matches!(*state, SinkState::Available) {
            return Err(LogStorageError::Closed);
        }
        let (sender, receiver) = oneshot::channel();
        *state = SinkState::Enabled(sender);
        Ok(receiver)
    }

    pub(super) fn finish(&self, report: impl FnOnce() -> Result<FinalLogReport, LogStorageError>) {
        let (sender, poisoned) = {
            let (mut state, poisoned) = match self.0.lock() {
                Ok(state) => (state, false),
                Err(error) => (error.into_inner(), true),
            };
            let sender = match std::mem::replace(&mut *state, SinkState::Finished) {
                SinkState::Enabled(sender) => Some(sender),
                SinkState::Available | SinkState::Finished => None,
            };
            (sender, poisoned)
        };
        if let Some(sender) = sender {
            let result = if poisoned {
                Err(LogStorageError::Panicked)
            } else {
                catch_unwind(AssertUnwindSafe(report)).unwrap_or(Err(LogStorageError::Panicked))
            };
            // A report waiter cannot turn diagnostic publication into a native
            // owner failure through a synchronously panicking wake callback.
            let _ = catch_unwind(AssertUnwindSafe(|| {
                let _ = sender.send(result);
            }));
        }
    }
}

#[cfg(test)]
mod tests;
