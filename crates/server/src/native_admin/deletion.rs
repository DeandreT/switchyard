use domain::DeleteEntityTarget;

use super::*;

pub(super) fn target(
    target: AdminTarget,
    kind: EntityKind,
) -> Result<(EntityPath, DeleteEntityTarget), Status> {
    match (target, kind) {
        (AdminTarget::Primary(path), EntityKind::Unspecified) => {
            Ok((path, DeleteEntityTarget::Auto))
        }
        (AdminTarget::Primary(path), EntityKind::Queue) => Ok((path, DeleteEntityTarget::Queue)),
        (AdminTarget::Primary(path), EntityKind::Topic) => Ok((path, DeleteEntityTarget::Topic)),
        (
            AdminTarget::Subscription { topic, name },
            EntityKind::Unspecified | EntityKind::Subscription,
        ) => Ok((topic, DeleteEntityTarget::Subscription { name })),
        _ => Err(Status::invalid_argument(
            "entity kind does not match the deletion path",
        )),
    }
}

pub(super) fn matches_outcome(target: &DeleteEntityTarget, outcome: &CommandOutcome) -> bool {
    matches!(
        (target, outcome),
        (
            DeleteEntityTarget::Auto,
            CommandOutcome::QueueDeleted | CommandOutcome::TopicDeleted
        ) | (DeleteEntityTarget::Queue, CommandOutcome::QueueDeleted)
            | (DeleteEntityTarget::Topic, CommandOutcome::TopicDeleted)
            | (
                DeleteEntityTarget::Subscription { .. },
                CommandOutcome::SubscriptionDeleted
            )
    )
}

pub(super) fn status(error: SubmitError) -> Status {
    match error {
        SubmitError::Propose(ProposeError::Broker(
            BrokerError::QueueConfig(_)
            | BrokerError::TopicConfig(_)
            | BrokerError::SubscriptionConfig(_),
        )) => Status::internal("invalid stored entity metadata"),
        error => submit_status(error),
    }
}

#[cfg(test)]
mod tests {
    use domain::{EntityDeleteLimit, QueueConfigError, SubscriptionName, Timestamp};
    use storage::StorageError;

    use super::*;

    #[test]
    fn selectors_keep_primary_auto_resolution_in_the_owner() {
        for (kind, expected) in [
            (EntityKind::Unspecified, DeleteEntityTarget::Auto),
            (EntityKind::Queue, DeleteEntityTarget::Queue),
            (EntityKind::Topic, DeleteEntityTarget::Topic),
        ] {
            let path = EntityPath::new("Orders/$Management").unwrap();
            let (actual_path, actual) = target(AdminTarget::Primary(path.clone()), kind).unwrap();
            assert_eq!(actual_path, path);
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn subscriptions_use_the_canonical_child_without_folding_user_names() {
        for kind in [EntityKind::Unspecified, EntityKind::Subscription] {
            let parsed = topology::target("Orders/$Management/Subscriptions/Alpha").unwrap();
            let (path, actual) = target(parsed, kind).unwrap();
            assert_eq!(path.as_str(), "Orders/$Management");
            assert_eq!(
                actual,
                DeleteEntityTarget::Subscription {
                    name: SubscriptionName::new("Alpha").unwrap()
                }
            );
        }
    }

    #[test]
    fn incompatible_shapes_are_rejected() {
        for kind in [EntityKind::Queue, EntityKind::Topic] {
            let parsed = topology::target("Orders/subscriptions/Alpha").unwrap();
            assert_eq!(
                target(parsed, kind).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
        let parsed = topology::target("Orders").unwrap();
        assert_eq!(
            target(parsed, EntityKind::Subscription).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn only_the_committed_matching_outcome_is_accepted() {
        let outcomes = [
            CommandOutcome::QueueDeleted,
            CommandOutcome::TopicDeleted,
            CommandOutcome::SubscriptionDeleted,
            CommandOutcome::QueueCreated,
        ];
        for (target, expected) in [
            (DeleteEntityTarget::Auto, [true, true, false, false]),
            (DeleteEntityTarget::Queue, [true, false, false, false]),
            (DeleteEntityTarget::Topic, [false, true, false, false]),
            (
                DeleteEntityTarget::Subscription {
                    name: SubscriptionName::new("Alpha").unwrap(),
                },
                [false, false, true, false],
            ),
        ] {
            for (outcome, expected) in outcomes.iter().zip(expected) {
                assert_eq!(matches_outcome(&target, outcome), expected);
            }
        }
    }

    #[test]
    fn stored_configuration_failures_are_internal_only_for_deletion() {
        for error in [
            BrokerError::QueueConfig(QueueConfigError::MaxMessageBytesTooSmall),
            BrokerError::TopicConfig(QueueConfigError::TimeToLiveTooShort),
            BrokerError::SubscriptionConfig(QueueConfigError::MaxDeliveryCountTooSmall),
        ] {
            let error = SubmitError::Propose(ProposeError::Broker(error));
            assert_eq!(
                submit_status(error.clone()).code(),
                tonic::Code::InvalidArgument
            );
            assert_eq!(status(error).code(), tonic::Code::Internal);
        }
    }

    #[test]
    fn deletion_preserves_kind_limit_missing_storage_and_clock_statuses() {
        let mut errors = vec![
            (
                SubmitError::Propose(ProposeError::Broker(BrokerError::EntityKindMismatch)),
                tonic::Code::InvalidArgument,
            ),
            (
                SubmitError::Propose(ProposeError::Broker(BrokerError::QueueNotFound)),
                tonic::Code::NotFound,
            ),
            (
                SubmitError::Propose(ProposeError::Broker(BrokerError::TopicNotFound)),
                tonic::Code::NotFound,
            ),
            (
                SubmitError::Propose(ProposeError::Broker(BrokerError::SubscriptionNotFound)),
                tonic::Code::NotFound,
            ),
            (
                SubmitError::Propose(ProposeError::Broker(BrokerError::DanglingEntityMetadata)),
                tonic::Code::Internal,
            ),
            (
                SubmitError::Propose(ProposeError::Broker(BrokerError::Storage(
                    StorageError::Backend {
                        operation: "commit",
                        detail: "injected failure".to_owned(),
                    },
                ))),
                tonic::Code::Internal,
            ),
            (
                SubmitError::Propose(ProposeError::ClockWentBackward {
                    last_applied: Timestamp::from_millis(1_000),
                    now: Timestamp::UNIX_EPOCH,
                    allowed_millis: 500,
                }),
                tonic::Code::Unavailable,
            ),
            (SubmitError::BrokerStopped, tonic::Code::Unavailable),
        ];
        for limit in [
            EntityDeleteLimit::Keys,
            EntityDeleteLimit::KeyBytes,
            EntityDeleteLimit::ValueBytes,
        ] {
            errors.push((
                SubmitError::Propose(ProposeError::Broker(BrokerError::EntityDeleteTooLarge {
                    limit,
                    maximum: 1,
                })),
                tonic::Code::ResourceExhausted,
            ));
        }
        for (error, expected) in errors {
            assert_eq!(submit_status(error.clone()).code(), expected);
            assert_eq!(status(error).code(), expected);
        }
    }
}
