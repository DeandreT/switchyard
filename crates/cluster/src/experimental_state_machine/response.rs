use domain::{
    BrokerError, CommandOutcome, CommittedApplication, CommittedEntryId, IdentifierError,
    QueueConfigError, QueueCounterKind,
};
use serde::{Deserialize, Serialize};

use super::StateMachineError;

/// Bounded application metadata, not a durable outcome record or a retry key.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LogApplication {
    CheckpointOnly,
    QueueCreated,
    /// An acknowledged allocation. Duplicate detection can discard its body.
    Sent {
        sequence: u64,
    },
    Refused(LogQueueRefusal),
    /// Exact replay provenance without reconstructing the original response.
    AlreadyApplied {
        entry: CommittedEntryId,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LogQueueRefusal {
    ClockRegression {
        last_applied_millis: u64,
        proposed_millis: u64,
    },
    DeadLetterQueueReserved,
    SubscriptionPathReserved,
    TargetExpansionTooLong {
        maximum_bytes: u64,
    },
    QueueAlreadyExists,
    EntityPathAlreadyExists,
    QueueNotFound,
    EntityKindMismatch,
    EntityIncarnationExhausted,
    SequenceExhausted,
    SessionRequired,
    SessionNotSupported,
    MessageIdTooLong {
        length: u64,
        maximum: u64,
    },
    MessageTooLarge {
        body_bytes: u64,
        maximum_bytes: u64,
    },
    InvalidQueueConfiguration(LogQueueConfigRefusal),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LogQueueConfigRefusal {
    LockDurationTooShort,
    LockDurationTooLong { maximum_millis: u64 },
    MaxDeliveryCountTooSmall,
    MaxMessageBytesTooSmall,
    TimeToLiveTooShort,
    DuplicateDetectionWindowTooShort { minimum_millis: u64 },
    DuplicateDetectionWindowTooLong { maximum_millis: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExpectedApplication {
    CheckpointOnly,
    CreateQueue,
    Send,
}

pub(super) fn application_for(
    expected: ExpectedApplication,
    application: CommittedApplication,
) -> Result<LogApplication, StateMachineError> {
    match application {
        CommittedApplication::CheckpointOnly if expected == ExpectedApplication::CheckpointOnly => {
            Ok(LogApplication::CheckpointOnly)
        }
        CommittedApplication::Queue(application) => {
            if application.dead_letters_enqueued
                || application.subscription_enqueues.is_some()
                || application.entity_deletions.is_some()
            {
                return Err(StateMachineError::UnexpectedApplication);
            }
            match (expected, application.outcome) {
                (ExpectedApplication::CreateQueue, CommandOutcome::QueueCreated) => {
                    Ok(LogApplication::QueueCreated)
                }
                (ExpectedApplication::Send, CommandOutcome::Sent { sequence }) => {
                    Ok(LogApplication::Sent {
                        sequence: sequence.as_u64(),
                    })
                }
                _ => Err(StateMachineError::UnexpectedApplication),
            }
        }
        CommittedApplication::Refused(error) => {
            refusal_for(expected, error).map(LogApplication::Refused)
        }
        _ => Err(StateMachineError::UnexpectedApplication),
    }
}

fn number(value: usize) -> Result<u64, StateMachineError> {
    value
        .try_into()
        .map_err(|_| StateMachineError::UnexpectedApplication)
}

fn refusal_for(
    expected: ExpectedApplication,
    error: BrokerError,
) -> Result<LogQueueRefusal, StateMachineError> {
    use ExpectedApplication::{CreateQueue, Send};
    use LogQueueRefusal as R;
    Ok(match (expected, error) {
        (
            CreateQueue | Send,
            BrokerError::ClockRegression {
                last_applied,
                proposed,
            },
        ) => R::ClockRegression {
            last_applied_millis: last_applied.as_millis(),
            proposed_millis: proposed.as_millis(),
        },
        (CreateQueue | Send, BrokerError::DeadLetterQueueIsReserved) => R::DeadLetterQueueReserved,
        (CreateQueue | Send, BrokerError::SubscriptionPathIsReserved) => {
            R::SubscriptionPathReserved
        }
        (
            CreateQueue | Send,
            BrokerError::Identifier(IdentifierError::TooLong {
                kind: "entity path",
                maximum,
            }),
        ) => R::TargetExpansionTooLong {
            maximum_bytes: number(maximum)?,
        },
        (CreateQueue, BrokerError::QueueAlreadyExists) => R::QueueAlreadyExists,
        (CreateQueue, BrokerError::EntityPathAlreadyExists) => R::EntityPathAlreadyExists,
        (CreateQueue, BrokerError::EntityIncarnationExhausted) => R::EntityIncarnationExhausted,
        (CreateQueue, BrokerError::QueueConfig(error)) => {
            R::InvalidQueueConfiguration(config_refusal(error))
        }
        (Send, BrokerError::QueueNotFound) => R::QueueNotFound,
        (Send, BrokerError::EntityKindMismatch) => R::EntityKindMismatch,
        (
            Send,
            BrokerError::QueueCounterExhausted {
                counter: QueueCounterKind::Sequence,
            },
        ) => R::SequenceExhausted,
        (Send, BrokerError::SessionRequired) => R::SessionRequired,
        (Send, BrokerError::SessionNotSupported) => R::SessionNotSupported,
        (Send, BrokerError::MessageIdTooLong { length, maximum }) => R::MessageIdTooLong {
            length: number(length)?,
            maximum: number(maximum)?,
        },
        (
            Send,
            BrokerError::MessageTooLarge {
                body_bytes,
                maximum_bytes,
            },
        ) => R::MessageTooLarge {
            body_bytes: number(body_bytes)?,
            maximum_bytes: number(maximum_bytes)?,
        },
        _ => return Err(StateMachineError::UnexpectedApplication),
    })
}

fn config_refusal(error: QueueConfigError) -> LogQueueConfigRefusal {
    use LogQueueConfigRefusal as R;
    match error {
        QueueConfigError::LockDurationTooShort => R::LockDurationTooShort,
        QueueConfigError::LockDurationTooLong { maximum_millis } => {
            R::LockDurationTooLong { maximum_millis }
        }
        QueueConfigError::MaxDeliveryCountTooSmall => R::MaxDeliveryCountTooSmall,
        QueueConfigError::MaxMessageBytesTooSmall => R::MaxMessageBytesTooSmall,
        QueueConfigError::TimeToLiveTooShort => R::TimeToLiveTooShort,
        QueueConfigError::DuplicateDetectionWindowTooShort { minimum_millis } => {
            R::DuplicateDetectionWindowTooShort { minimum_millis }
        }
        QueueConfigError::DuplicateDetectionWindowTooLong { maximum_millis } => {
            R::DuplicateDetectionWindowTooLong { maximum_millis }
        }
    }
}

#[cfg(test)]
mod tests;
