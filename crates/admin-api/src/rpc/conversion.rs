use domain::AdminCatalogKind;
use tonic::Status;

use crate::{
    CreateEntityRequest, EntityAddress, EntityDefinition, EntityKind, EntityOptions, EntityPage,
    EntityProfile, GetEntityRequest, ListEntitiesRequest, wire,
};

use super::{internal, invalid};

fn address(
    path: String,
    topic: Option<String>,
    name: Option<String>,
) -> Result<EntityAddress, Status> {
    match (topic, name) {
        (None, None) => Ok(EntityAddress::Entity { path }),
        (Some(topic), Some(name)) if path.is_empty() => {
            Ok(EntityAddress::Subscription { topic, name })
        }
        _ => Err(invalid()),
    }
}

impl TryFrom<wire::CreateEntityRequest> for CreateEntityRequest {
    type Error = Status;

    fn try_from(value: wire::CreateEntityRequest) -> Result<Self, Status> {
        let kind = match wire::EntityKind::try_from(value.kind) {
            Ok(wire::EntityKind::Queue) => EntityKind::Queue,
            Ok(wire::EntityKind::Topic) => EntityKind::Topic,
            Ok(wire::EntityKind::Subscription) => EntityKind::Subscription,
            _ => return Err(invalid()),
        };
        Ok(Self {
            namespace: value.namespace,
            address: address(
                value.path,
                value.subscription_topic,
                value.subscription_name,
            )?,
            kind,
            options: EntityOptions {
                default_time_to_live_millis: value.default_ttl_millis,
                lock_duration_millis: value.lock_duration_millis,
                max_delivery_count: value.max_delivery_count,
                max_message_bytes: value
                    .max_message_bytes
                    .map(usize::try_from)
                    .transpose()
                    .map_err(|_| invalid())?,
                requires_session: value.requires_session,
                requires_duplicate_detection: value.requires_duplicate_detection,
                // Unlike optional fields, legacy history has no presence bit.
                // Zero is the queue default; other kinds must see omission.
                duplicate_detection_history_millis: (value.duplicate_detection_history_millis != 0)
                    .then_some(value.duplicate_detection_history_millis),
                placement_group_id: value.placement_group_id,
                max_size_bytes: value.max_size_bytes,
                used_logical_bytes: value.used_logical_bytes,
            },
        })
    }
}

impl TryFrom<wire::GetEntityRequest> for GetEntityRequest {
    type Error = Status;

    fn try_from(value: wire::GetEntityRequest) -> Result<Self, Status> {
        Ok(Self {
            namespace: value.namespace,
            address: address(
                value.path,
                value.subscription_topic,
                value.subscription_name,
            )?,
        })
    }
}

impl TryFrom<wire::ListEntitiesRequest> for ListEntitiesRequest {
    type Error = Status;

    fn try_from(value: wire::ListEntitiesRequest) -> Result<Self, Status> {
        #[allow(deprecated)]
        if !value.page_token.is_empty() {
            return Err(invalid());
        }
        let kind = match wire::EntitySelection::try_from(value.selection) {
            Ok(wire::EntitySelection::All) => AdminCatalogKind::All,
            Ok(wire::EntitySelection::Queues) => AdminCatalogKind::Queues,
            Ok(wire::EntitySelection::Topics) => AdminCatalogKind::Topics,
            Ok(wire::EntitySelection::Subscriptions) => AdminCatalogKind::Subscriptions,
            _ => return Err(invalid()),
        };
        Ok(Self {
            namespace: value.namespace,
            kind,
            continuation: value.continuation,
            limit: usize::try_from(value.page_size).map_err(|_| invalid())?,
        })
    }
}

impl TryFrom<EntityDefinition> for wire::Entity {
    type Error = Status;

    fn try_from(value: EntityDefinition) -> Result<Self, Status> {
        let mut entity = Self {
            namespace: value.namespace.as_str().into(),
            path: value.path.as_str().into(),
            // The local service has no placement/capacity/usage authority.
            placement_group_id: None,
            max_size_bytes: None,
            used_logical_bytes: None,
            ..Self::default()
        };
        match value.profile {
            EntityProfile::Queue(config) => {
                entity.kind = wire::EntityKind::Queue.into();
                entity.default_ttl_millis = config.default_time_to_live_millis;
                entity.lock_duration_millis = Some(config.lock_duration_millis);
                entity.max_delivery_count = Some(config.max_delivery_count);
                entity.max_message_bytes =
                    Some(u64::try_from(config.max_message_bytes).map_err(|_| internal())?);
                entity.requires_session = Some(config.requires_session);
                entity.requires_duplicate_detection = config.requires_duplicate_detection;
                entity.duplicate_detection_history_millis =
                    config.duplicate_detection_history_millis;
            }
            EntityProfile::Topic(config) => {
                entity.kind = wire::EntityKind::Topic.into();
                entity.default_ttl_millis = config.default_time_to_live_millis;
                entity.max_message_bytes =
                    Some(u64::try_from(config.max_message_bytes).map_err(|_| internal())?);
            }
            EntityProfile::Subscription(config) => {
                entity.kind = wire::EntityKind::Subscription.into();
                entity.default_ttl_millis = config.default_time_to_live_millis;
                entity.lock_duration_millis = Some(config.lock_duration_millis);
                entity.max_delivery_count = Some(config.max_delivery_count);
                // The typed service exposes canonical composite paths. Split the
                // validated marker, never revalidate a composite as a public path.
                let (topic, name) = value
                    .path
                    .as_str()
                    .split_once("/subscriptions/")
                    .ok_or_else(internal)?;
                entity.subscription_topic = Some(topic.into());
                entity.subscription_name = Some(name.into());
            }
        }
        Ok(entity)
    }
}

impl TryFrom<EntityPage> for wire::ListEntitiesResponse {
    type Error = Status;

    fn try_from(value: EntityPage) -> Result<Self, Status> {
        Ok(Self {
            entities: value
                .entities
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            continuation: value.continuation,
            ..Self::default()
        })
    }
}
