use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use domain::{CommittedEntryId, Timestamp};
use openraft::{
    Raft,
    error::{CheckIsLeaderError, ClientWriteError, Fatal, RaftError},
};

use crate::{
    LogApplication, LogQueueRefusal, LogTypes, ReadOnlyLogReader,
    experimental_log::{
        LogRetention, MAX_RETAINED_BYTES, MAX_RETAINED_ENTRIES, stamp_queue_command,
    },
    experimental_state_machine::HealthyCheckpointReader,
};

use super::super::{network::NodeGeneration, node::StopSignal};
use super::{
    QueueWriteError as Error, QueueWriteOutcome, QueueWriteRejection as Rejection,
    QueueWriteResult, QueueWriteUnknown as Unknown,
    budget::Admission,
    completion::{ActiveSlot, Job},
    fatal,
};

pub(super) enum Flow {
    Continue,
    Retire,
}

pub(super) struct Context {
    pub(super) raft: Raft<LogTypes>,
    pub(super) checkpoint: HealthyCheckpointReader,
    pub(super) log: ReadOnlyLogReader,
    pub(super) generation: NodeGeneration,
    pub(super) stop: StopSignal,
    pub(super) admission: Arc<Admission>,
}

impl Context {
    fn closing(&self) -> bool {
        self.stop.is_requested() || self.admission.is_closed() || !self.generation.is_live()
    }

    fn refuse(&self, job: Job, cause: Rejection, retire: bool) -> Flow {
        if retire {
            fatal(&self.admission, &self.stop);
        }
        job.finish(Err(Error::KnownRejected(cause)));
        if retire { Flow::Retire } else { Flow::Continue }
    }

    pub(super) async fn process(&self, mut job: Job, slot: &ActiveSlot) -> Flow {
        if self.closing() {
            return self.refuse(job, Rejection::Closed, false);
        }
        let barrier = tokio::select! {
            biased;
            _ = self.stop.requested() => return self.refuse(job, Rejection::Closed, false),
            result = self.raft.ensure_linearizable() => result,
        };
        let barrier = match barrier {
            Ok(barrier) => barrier,
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(_))) => {
                return self.refuse(job, Rejection::NotLeader, false);
            }
            Err(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_))) => {
                return self.refuse(job, Rejection::QuorumUnavailable, false);
            }
            Err(_) => return self.refuse(job, Rejection::Storage, !self.stop.is_requested()),
        };
        let checkpoint = tokio::select! {
            biased;
            _ = self.stop.requested() => return self.refuse(job, Rejection::Closed, false),
            result = self.checkpoint.checkpoint() => match result {
                Ok(checkpoint) => checkpoint,
                Err(_) => return self.refuse(job, Rejection::Storage, true),
            },
        };
        if barrier.is_some_and(|barrier| {
            checkpoint.last().is_none_or(|mark| {
                mark.id.index < barrier.index
                    || (mark.id.index == barrier.index
                        && (mark.id.term != barrier.leader_id.term
                            || mark.id.node_id != barrier.leader_id.node_id))
            })
        }) {
            return self.refuse(job, Rejection::Storage, true);
        }
        let now = match epoch_millis(SystemTime::now())
            .and_then(|now| bounded_stamp(now, checkpoint.highest_timestamp()))
        {
            Ok(now) => now,
            Err(()) => return self.refuse(job, Rejection::Clock, false),
        };
        let retention = tokio::select! {
            biased;
            _ = self.stop.requested() => return self.refuse(job, Rejection::Closed, false),
            result = self.log.retention() => match result {
                Ok(retention) => retention,
                Err(_) => return self.refuse(job, Rejection::Storage, true),
            },
        };
        let Some(intent) = job.intent() else {
            return self.refuse(job, Rejection::Storage, true);
        };
        let is_send = intent.is_send();
        if !has_headroom(retention, intent.encoded_bytes()) {
            return self.refuse(job, Rejection::Headroom, false);
        }
        if self.closing() {
            return self.refuse(job, Rejection::Closed, false);
        }
        // Arming is a conservative submission-attempt boundary. A racing stop
        // can still win the select before the first native future poll.
        let intent = match job.arm(slot) {
            Ok(intent) => intent,
            Err(()) => return self.refuse(job, Rejection::Storage, true),
        };
        let mut command = intent.into_command();
        stamp_queue_command(&mut command, now);
        let result = tokio::select! {
            biased;
            _ = self.stop.requested() => {
                slot.reason(Unknown::Stopped);
                return Flow::Retire;
            }
            result = self.raft.client_write(command) => result,
        };
        match result {
            Ok(response) => {
                let id = response.log_id;
                let node = self.raft.metrics().borrow().id;
                let entry = CommittedEntryId {
                    term: id.leader_id.term,
                    node_id: id.leader_id.node_id,
                    index: id.index,
                };
                if response.membership.is_some() || id.leader_id.node_id != node || id.index == 0 {
                    slot.reason(Unknown::UnexpectedApplication);
                    fatal(&self.admission, &self.stop);
                    return Flow::Retire;
                }
                let outcome = match application(is_send, entry, response.data) {
                    Ok(outcome) => outcome,
                    Err(cause) => {
                        slot.reason(cause);
                        fatal(&self.admission, &self.stop);
                        return Flow::Retire;
                    }
                };
                slot.finish_now(Ok(QueueWriteResult { entry, outcome }));
                Flow::Continue
            }
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => {
                slot.finish_now(Err(Error::Unknown(Unknown::LeadershipChanged)));
                Flow::Continue
            }
            Err(error) => {
                let expected_stop =
                    self.stop.is_requested() && matches!(&error, RaftError::Fatal(Fatal::Stopped));
                slot.reason(match error {
                    RaftError::Fatal(Fatal::StorageError(_)) => Unknown::Storage,
                    RaftError::Fatal(Fatal::Stopped) => Unknown::Stopped,
                    _ => Unknown::OwnerLost,
                });
                if !expected_stop {
                    fatal(&self.admission, &self.stop);
                }
                Flow::Retire
            }
        }
    }
}

pub(super) fn epoch_millis(time: SystemTime) -> Result<Timestamp, ()> {
    let elapsed = time.duration_since(UNIX_EPOCH).map_err(|_| ())?;
    let millis = u64::try_from(elapsed.as_millis()).map_err(|_| ())?;
    Ok(Timestamp::from_millis(millis))
}

pub(super) fn bounded_stamp(now: Timestamp, watermark: Timestamp) -> Result<Timestamp, ()> {
    if watermark.as_millis().saturating_sub(now.as_millis()) > super::MAX_STAMP_AHEAD_MILLIS {
        Err(())
    } else {
        Ok(now.max(watermark))
    }
}

pub(super) fn has_headroom(retention: LogRetention, bytes: usize) -> bool {
    let Ok(bytes) = u64::try_from(bytes) else {
        return false;
    };
    retention.last_purged.is_none()
        && retention
            .retained_entries
            .checked_add(2)
            .is_some_and(|count| count <= MAX_RETAINED_ENTRIES)
        && retention
            .retained_bytes
            .checked_add(bytes)
            .and_then(|bytes| bytes.checked_add(64))
            .is_some_and(|bytes| bytes <= MAX_RETAINED_BYTES)
}

pub(super) fn application(
    is_send: bool,
    entry: CommittedEntryId,
    result: LogApplication,
) -> Result<QueueWriteOutcome, Unknown> {
    match result {
        LogApplication::QueueCreated if !is_send => Ok(QueueWriteOutcome::QueueCreated),
        LogApplication::Sent { sequence }
            if is_send && sequence > 0 && sequence <= domain::MAX_SEQUENCE_NUMBER =>
        {
            Ok(QueueWriteOutcome::Sent { sequence })
        }
        LogApplication::Refused(refusal) if refusal_matches(is_send, refusal) => {
            Ok(QueueWriteOutcome::Refused(refusal))
        }
        LogApplication::AlreadyApplied { entry: replay } if replay == entry => {
            Err(Unknown::OriginalResultUnavailable)
        }
        _ => Err(Unknown::UnexpectedApplication),
    }
}

fn refusal_matches(is_send: bool, refusal: LogQueueRefusal) -> bool {
    use LogQueueRefusal::*;
    match refusal {
        ClockRegression { .. }
        | DeadLetterQueueReserved
        | SubscriptionPathReserved
        | TargetExpansionTooLong { .. } => true,
        QueueAlreadyExists
        | EntityPathAlreadyExists
        | EntityIncarnationExhausted
        | InvalidQueueConfiguration(_) => !is_send,
        QueueNotFound
        | EntityKindMismatch
        | SequenceExhausted
        | SessionRequired
        | SessionNotSupported
        | MessageIdTooLong { .. }
        | MessageTooLarge { .. } => is_send,
    }
}
