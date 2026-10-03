use domain::{CommandApplication, EntityPath, SequenceNumber, Timestamp};

use super::*;

fn queue(outcome: CommandOutcome) -> CommittedApplication {
    CommittedApplication::Queue(Box::new(CommandApplication {
        outcome,
        dead_letters_enqueued: false,
        subscription_enqueues: None,
        entity_deletions: None,
    }))
}

#[test]
fn exact_application_kinds_map_without_inventing_replay_results() {
    assert_eq!(
        application_for(
            ExpectedApplication::CheckpointOnly,
            CommittedApplication::CheckpointOnly
        )
        .unwrap(),
        LogApplication::CheckpointOnly
    );
    assert_eq!(
        application_for(
            ExpectedApplication::CreateQueue,
            queue(CommandOutcome::QueueCreated)
        )
        .unwrap(),
        LogApplication::QueueCreated
    );
    assert_eq!(
        application_for(
            ExpectedApplication::Send,
            queue(CommandOutcome::Sent {
                sequence: SequenceNumber::new(17)
            })
        )
        .unwrap(),
        LogApplication::Sent { sequence: 17 }
    );
    let replay = LogApplication::AlreadyApplied {
        entry: CommittedEntryId {
            term: 3,
            node_id: 7,
            index: 19,
        },
    };
    assert!(matches!(replay, LogApplication::AlreadyApplied { .. }));
}

#[test]
fn application_kind_mismatches_are_fatal() {
    for expected in [ExpectedApplication::CreateQueue, ExpectedApplication::Send] {
        assert_eq!(
            application_for(expected, CommittedApplication::CheckpointOnly),
            Err(StateMachineError::UnexpectedApplication)
        );
    }
    for (expected, outcome) in [
        (
            ExpectedApplication::CheckpointOnly,
            CommandOutcome::QueueCreated,
        ),
        (
            ExpectedApplication::CheckpointOnly,
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(1),
            },
        ),
        (
            ExpectedApplication::CreateQueue,
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(1),
            },
        ),
        (ExpectedApplication::Send, CommandOutcome::QueueCreated),
        (ExpectedApplication::Send, CommandOutcome::Completed),
        (ExpectedApplication::Send, CommandOutcome::TopicCreated),
        (
            ExpectedApplication::CreateQueue,
            CommandOutcome::QueueDeleted,
        ),
    ] {
        assert_eq!(
            application_for(expected, queue(outcome)),
            Err(StateMachineError::UnexpectedApplication)
        );
    }
}

#[test]
fn unexpected_effects_are_fatal_even_when_the_outcome_matches() {
    for expected in [ExpectedApplication::CreateQueue, ExpectedApplication::Send] {
        for effect in 0..5 {
            let outcome = if expected == ExpectedApplication::CreateQueue {
                CommandOutcome::QueueCreated
            } else {
                CommandOutcome::Sent {
                    sequence: SequenceNumber::new(1),
                }
            };
            let CommittedApplication::Queue(mut application) = queue(outcome) else {
                panic!("queue");
            };
            match effect {
                0 => application.dead_letters_enqueued = true,
                1 => application.subscription_enqueues = Some(Vec::new()),
                2 => application.entity_deletions = Some(Vec::new()),
                3 => {
                    application.subscription_enqueues =
                        Some(vec![EntityPath::new("private-path").unwrap()])
                }
                4 => {
                    application.entity_deletions =
                        Some(vec![EntityPath::new("private-path").unwrap()])
                }
                _ => unreachable!(),
            }
            assert_eq!(
                application_for(expected, CommittedApplication::Queue(application)),
                Err(StateMachineError::UnexpectedApplication)
            );
        }
    }
}

#[test]
fn common_refusals_are_mapped_only_for_queue_commands() {
    for (error, expected) in [
        (
            BrokerError::ClockRegression {
                last_applied: Timestamp::from_millis(20),
                proposed: Timestamp::from_millis(10),
            },
            LogQueueRefusal::ClockRegression {
                last_applied_millis: 20,
                proposed_millis: 10,
            },
        ),
        (
            BrokerError::DeadLetterQueueIsReserved,
            LogQueueRefusal::DeadLetterQueueReserved,
        ),
        (
            BrokerError::SubscriptionPathIsReserved,
            LogQueueRefusal::SubscriptionPathReserved,
        ),
        (
            BrokerError::Identifier(IdentifierError::TooLong {
                kind: "entity path",
                maximum: 260,
            }),
            LogQueueRefusal::TargetExpansionTooLong { maximum_bytes: 260 },
        ),
    ] {
        for kind in [ExpectedApplication::CreateQueue, ExpectedApplication::Send] {
            assert_eq!(
                application_for(kind, CommittedApplication::Refused(error.clone())).unwrap(),
                LogApplication::Refused(expected)
            );
        }
        assert_eq!(
            application_for(
                ExpectedApplication::CheckpointOnly,
                CommittedApplication::Refused(error)
            ),
            Err(StateMachineError::UnexpectedApplication)
        );
    }
}

#[test]
fn create_only_refusals_cannot_be_laundered_into_send_results() {
    for (error, expected) in [
        (
            BrokerError::QueueAlreadyExists,
            LogQueueRefusal::QueueAlreadyExists,
        ),
        (
            BrokerError::EntityPathAlreadyExists,
            LogQueueRefusal::EntityPathAlreadyExists,
        ),
        (
            BrokerError::EntityIncarnationExhausted,
            LogQueueRefusal::EntityIncarnationExhausted,
        ),
    ] {
        assert_eq!(
            application_for(
                ExpectedApplication::CreateQueue,
                CommittedApplication::Refused(error.clone())
            )
            .unwrap(),
            LogApplication::Refused(expected)
        );
        assert_eq!(
            application_for(
                ExpectedApplication::Send,
                CommittedApplication::Refused(error)
            ),
            Err(StateMachineError::UnexpectedApplication)
        );
    }
}

#[test]
fn send_only_refusals_keep_exact_numeric_fields() {
    for (error, expected) in [
        (BrokerError::QueueNotFound, LogQueueRefusal::QueueNotFound),
        (
            BrokerError::EntityKindMismatch,
            LogQueueRefusal::EntityKindMismatch,
        ),
        (
            BrokerError::QueueCounterExhausted {
                counter: QueueCounterKind::Sequence,
            },
            LogQueueRefusal::SequenceExhausted,
        ),
        (
            BrokerError::SessionRequired,
            LogQueueRefusal::SessionRequired,
        ),
        (
            BrokerError::SessionNotSupported,
            LogQueueRefusal::SessionNotSupported,
        ),
        (
            BrokerError::MessageIdTooLong {
                length: 129,
                maximum: 128,
            },
            LogQueueRefusal::MessageIdTooLong {
                length: 129,
                maximum: 128,
            },
        ),
        (
            BrokerError::MessageTooLarge {
                body_bytes: 1025,
                maximum_bytes: 1024,
            },
            LogQueueRefusal::MessageTooLarge {
                body_bytes: 1025,
                maximum_bytes: 1024,
            },
        ),
    ] {
        assert_eq!(
            application_for(
                ExpectedApplication::Send,
                CommittedApplication::Refused(error.clone())
            )
            .unwrap(),
            LogApplication::Refused(expected)
        );
        assert_eq!(
            application_for(
                ExpectedApplication::CreateQueue,
                CommittedApplication::Refused(error)
            ),
            Err(StateMachineError::UnexpectedApplication)
        );
    }
}

#[test]
fn every_caller_configuration_refusal_is_typed_and_create_only() {
    for (error, expected) in [
        (
            QueueConfigError::LockDurationTooShort,
            LogQueueConfigRefusal::LockDurationTooShort,
        ),
        (
            QueueConfigError::LockDurationTooLong {
                maximum_millis: 300_000,
            },
            LogQueueConfigRefusal::LockDurationTooLong {
                maximum_millis: 300_000,
            },
        ),
        (
            QueueConfigError::MaxDeliveryCountTooSmall,
            LogQueueConfigRefusal::MaxDeliveryCountTooSmall,
        ),
        (
            QueueConfigError::MaxMessageBytesTooSmall,
            LogQueueConfigRefusal::MaxMessageBytesTooSmall,
        ),
        (
            QueueConfigError::TimeToLiveTooShort,
            LogQueueConfigRefusal::TimeToLiveTooShort,
        ),
        (
            QueueConfigError::DuplicateDetectionWindowTooShort {
                minimum_millis: 20_000,
            },
            LogQueueConfigRefusal::DuplicateDetectionWindowTooShort {
                minimum_millis: 20_000,
            },
        ),
        (
            QueueConfigError::DuplicateDetectionWindowTooLong {
                maximum_millis: 604_800_000,
            },
            LogQueueConfigRefusal::DuplicateDetectionWindowTooLong {
                maximum_millis: 604_800_000,
            },
        ),
    ] {
        assert_eq!(
            application_for(
                ExpectedApplication::CreateQueue,
                CommittedApplication::Refused(BrokerError::QueueConfig(error))
            )
            .unwrap(),
            LogApplication::Refused(LogQueueRefusal::InvalidQueueConfiguration(expected))
        );
        assert_eq!(
            application_for(
                ExpectedApplication::Send,
                CommittedApplication::Refused(BrokerError::QueueConfig(error))
            ),
            Err(StateMachineError::UnexpectedApplication)
        );
    }
}

#[test]
fn storage_corruption_and_unknown_refusals_are_never_business_responses() {
    let errors = [
        BrokerError::Storage(storage::StorageError::Backend {
            operation: "private-operation",
            detail: "private-backend-path".into(),
        }),
        BrokerError::DanglingEntityMetadata,
        BrokerError::DanglingRuleMetadata,
        BrokerError::EntityBindingStale,
        BrokerError::InvalidMessageContent {
            reason: "private-body".into(),
        },
        BrokerError::QueueCounterExhausted {
            counter: QueueCounterKind::LockToken,
        },
        BrokerError::Identifier(IdentifierError::TooLong {
            kind: "private-field",
            maximum: 260,
        }),
        BrokerError::Identifier(IdentifierError::ControlCharacter {
            kind: "entity path",
        }),
        BrokerError::Identifier(IdentifierError::Empty {
            kind: "entity path",
        }),
    ];
    for error in errors {
        for kind in [
            ExpectedApplication::CheckpointOnly,
            ExpectedApplication::CreateQueue,
            ExpectedApplication::Send,
        ] {
            let result =
                application_for(kind, CommittedApplication::Refused(error.clone())).unwrap_err();
            assert_eq!(result, StateMachineError::UnexpectedApplication);
            assert!(!format!("{result:?} {result}").contains("private"));
        }
    }
}

#[test]
fn response_serde_has_only_bounded_numeric_and_fixed_variant_data() {
    let mut responses = vec![
        LogApplication::CheckpointOnly,
        LogApplication::QueueCreated,
        LogApplication::Sent { sequence: u64::MAX },
        LogApplication::AlreadyApplied {
            entry: CommittedEntryId {
                term: u64::MAX,
                node_id: u64::MAX,
                index: u64::MAX,
            },
        },
    ];
    let refusals = [
        LogQueueRefusal::ClockRegression {
            last_applied_millis: u64::MAX,
            proposed_millis: u64::MAX,
        },
        LogQueueRefusal::DeadLetterQueueReserved,
        LogQueueRefusal::SubscriptionPathReserved,
        LogQueueRefusal::TargetExpansionTooLong {
            maximum_bytes: u64::MAX,
        },
        LogQueueRefusal::QueueAlreadyExists,
        LogQueueRefusal::EntityPathAlreadyExists,
        LogQueueRefusal::QueueNotFound,
        LogQueueRefusal::EntityKindMismatch,
        LogQueueRefusal::EntityIncarnationExhausted,
        LogQueueRefusal::SequenceExhausted,
        LogQueueRefusal::SessionRequired,
        LogQueueRefusal::SessionNotSupported,
        LogQueueRefusal::MessageIdTooLong {
            length: u64::MAX,
            maximum: u64::MAX,
        },
        LogQueueRefusal::MessageTooLarge {
            body_bytes: u64::MAX,
            maximum_bytes: u64::MAX,
        },
        LogQueueRefusal::InvalidQueueConfiguration(
            LogQueueConfigRefusal::DuplicateDetectionWindowTooLong {
                maximum_millis: u64::MAX,
            },
        ),
    ];
    responses.extend(refusals.into_iter().map(LogApplication::Refused));
    for response in responses {
        let bytes = postcard::to_stdvec(&response).unwrap();
        assert!(bytes.len() <= 32);
        assert_eq!(
            postcard::from_bytes::<LogApplication>(&bytes).unwrap(),
            response
        );
    }
}
