use admin_api::v1::{
    SubscriptionConfiguration, TopicConfiguration, subscription_configuration, topic_configuration,
};
use domain::{SubscriptionConfig, SubscriptionName, TopicConfig};
use protocol_amqp::EntityMetadata;

use super::*;

pub(super) fn target(path: &str) -> Result<AdminTarget, Status> {
    let entity =
        EntityPath::new(path).map_err(|error| Status::invalid_argument(error.to_string()))?;
    if entity.is_dead_letter_queue() {
        return Err(Status::invalid_argument(
            "dead-letter queues are not administrable entities",
        ));
    }
    if !entity.is_subscription_path() {
        return Ok(AdminTarget::Primary(entity));
    }
    let (prefix, leaf) = path.rsplit_once('/').ok_or_else(invalid_subscription)?;
    let (parent, marker) = prefix.rsplit_once('/').ok_or_else(invalid_subscription)?;
    if !marker.eq_ignore_ascii_case("subscriptions") {
        return Err(invalid_subscription());
    }
    let topic =
        EntityPath::new(parent).map_err(|error| Status::invalid_argument(error.to_string()))?;
    if topic.is_dead_letter_queue() || topic.is_subscription_path() {
        return Err(invalid_subscription());
    }
    let name =
        SubscriptionName::new(leaf).map_err(|error| Status::invalid_argument(error.to_string()))?;
    topic
        .subscription(&name)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(AdminTarget::Subscription { topic, name })
}

fn invalid_subscription() -> Status {
    Status::invalid_argument("invalid subscription entity path")
}

pub(super) fn requested_resource(path: &str) -> String {
    target(path)
        .and_then(|target| {
            target
                .canonical_entity()
                .map_err(|error| Status::invalid_argument(error.to_string()))
        })
        .map_or_else(|_| path.to_owned(), |entity| entity.as_str().to_owned())
}

pub(super) fn reject_other_configuration(
    input: &CreateEntityRequest,
    kind: EntityKind,
) -> Result<(), Status> {
    let has_legacy = input.default_ttl_millis != 0
        || input.lock_duration_millis != 0
        || input.max_delivery_count != 0
        || input.requires_session;
    let incompatible = match kind {
        EntityKind::Queue => input.topic_config.is_some() || input.subscription_config.is_some(),
        EntityKind::Topic => {
            input.queue_config.is_some() || input.subscription_config.is_some() || has_legacy
        }
        EntityKind::Subscription => {
            input.queue_config.is_some() || input.topic_config.is_some() || has_legacy
        }
        EntityKind::Unspecified => true,
    };
    if incompatible {
        return Err(Status::invalid_argument(
            "configuration does not match the entity kind",
        ));
    }
    Ok(())
}

pub(super) fn topic_configuration(
    input: Option<&TopicConfiguration>,
) -> Result<TopicConfig, Status> {
    let defaults = TopicConfig::default();
    let Some(input) = input else {
        return Ok(defaults);
    };
    let config = TopicConfig {
        default_time_to_live_millis: input.default_time_to_live.as_ref().and_then(
            |ttl| match ttl {
                topic_configuration::DefaultTimeToLive::DefaultTtlMillis(millis) => Some(*millis),
                topic_configuration::DefaultTimeToLive::DefaultTtlUnlimited(_) => None,
            },
        ),
        max_message_bytes: message_limit(input.max_message_bytes)?
            .unwrap_or(defaults.max_message_bytes),
        requires_duplicate_detection: input
            .requires_duplicate_detection
            .unwrap_or(defaults.requires_duplicate_detection),
        duplicate_detection_history_time_window_millis: input
            .duplicate_detection_history_time_window_millis
            .unwrap_or(defaults.duplicate_detection_history_time_window_millis),
    };
    config
        .validate()
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

pub(super) fn subscription_configuration(
    input: Option<&SubscriptionConfiguration>,
) -> Result<SubscriptionConfig, Status> {
    let defaults = SubscriptionConfig::default();
    let Some(input) = input else {
        return Ok(defaults);
    };
    let config = SubscriptionConfig {
        lock_duration_millis: input
            .lock_duration_millis
            .unwrap_or(defaults.lock_duration_millis),
        max_delivery_count: input
            .max_delivery_count
            .unwrap_or(defaults.max_delivery_count),
        default_time_to_live_millis: input.default_time_to_live.as_ref().and_then(
            |ttl| match ttl {
                subscription_configuration::DefaultTimeToLive::DefaultTtlMillis(millis) => {
                    Some(*millis)
                }
                subscription_configuration::DefaultTimeToLive::DefaultTtlUnlimited(_) => None,
            },
        ),
        max_message_bytes: message_limit(input.max_message_bytes)?
            .unwrap_or(defaults.max_message_bytes),
        requires_session: input.requires_session.unwrap_or(defaults.requires_session),
        dead_lettering_on_message_expiration: input
            .dead_lettering_on_message_expiration
            .unwrap_or(defaults.dead_lettering_on_message_expiration),
    };
    config
        .validate()
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn message_limit(bytes: Option<u64>) -> Result<Option<usize>, Status> {
    bytes
        .map(|bytes| {
            usize::try_from(bytes)
                .map_err(|_| Status::invalid_argument("message limit exceeds this platform"))
        })
        .transpose()
}

pub(super) fn response(
    namespace: &NamespaceName,
    path: &EntityPath,
    metadata: EntityMetadata,
) -> Result<Entity, Status> {
    let mut entity = Entity {
        namespace: namespace.as_str().to_owned(),
        path: path.as_str().to_owned(),
        ..Entity::default()
    };
    match metadata {
        EntityMetadata::Queue(config) => return Ok(entity_response(namespace, path, config)),
        EntityMetadata::Topic(config) => {
            entity.kind = EntityKind::Topic as i32;
            entity.topic_config = Some(TopicConfiguration {
                default_time_to_live: Some(match config.default_time_to_live_millis {
                    Some(millis) => {
                        topic_configuration::DefaultTimeToLive::DefaultTtlMillis(millis)
                    }
                    None => topic_configuration::DefaultTimeToLive::DefaultTtlUnlimited(
                        v1::UnlimitedTimeToLive {},
                    ),
                }),
                max_message_bytes: Some(config.max_message_bytes as u64),
                requires_duplicate_detection: Some(config.requires_duplicate_detection),
                duplicate_detection_history_time_window_millis: Some(
                    config.duplicate_detection_history_time_window_millis,
                ),
            });
        }
        EntityMetadata::Subscription(config) => {
            entity.kind = EntityKind::Subscription as i32;
            entity.subscription_config = Some(SubscriptionConfiguration {
                lock_duration_millis: Some(config.lock_duration_millis),
                max_delivery_count: Some(config.max_delivery_count),
                default_time_to_live: Some(match config.default_time_to_live_millis {
                    Some(millis) => {
                        subscription_configuration::DefaultTimeToLive::DefaultTtlMillis(millis)
                    }
                    None => subscription_configuration::DefaultTimeToLive::DefaultTtlUnlimited(
                        v1::UnlimitedTimeToLive {},
                    ),
                }),
                max_message_bytes: Some(config.max_message_bytes as u64),
                requires_session: Some(config.requires_session),
                dead_lettering_on_message_expiration: Some(
                    config.dead_lettering_on_message_expiration,
                ),
            });
        }
        EntityMetadata::DeadLetter(_) => {
            return Err(Status::internal("unexpected native entity metadata"));
        }
    }
    Ok(entity)
}
