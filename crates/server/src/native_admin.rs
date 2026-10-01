//! Namespace-bound entity administration through the broker owner.

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use admin_api::v1::{
    self, CreateEntityRequest, DeleteEntityRequest, Entity, EntityKind, GetEntityRequest,
    ListEntitiesRequest, ListEntitiesResponse, Operation, QueueConfiguration, UpdateEntityRequest,
    entity_service_server::EntityService, queue_configuration::DefaultTimeToLive,
};
use auth::{Permission, ResourceScope, ResourceScopeError, SharedAccessPolicy};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use domain::{
    BrokerError, CommandKind, CommandOutcome, EntityPath, MAX_QUEUE_PAGE_SIZE, NamespaceName,
    QueueConfig, QueueConfigUpdate, QueueCursor, QueueTimeToLiveUpdate,
};
use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Request, Response, Status};

use crate::{AdminTarget, BrokerHandle, ProposeError, SubmitError};

mod deletion;
mod paging;
mod queue_paging;
mod topology;

pub use queue_paging::{MAX_NATIVE_QUEUE_SCAN_ROUNDS, MAX_NATIVE_QUEUE_SCAN_ROWS};

const DEFAULT_PAGE_SIZE: usize = 100;
const MAX_PAGE_TOKEN_BYTES: usize = 512;
const MAX_CONCURRENT_REQUESTS: usize = 128;

#[derive(Clone)]
struct Authentication {
    policy: SharedAccessPolicy,
    scope: ResourceScope,
}

#[derive(Clone)]
pub struct NativeAdminService {
    broker: BrokerHandle,
    namespace: NamespaceName,
    authentication: Option<Authentication>,
    admission: Arc<Semaphore>,
}

impl NativeAdminService {
    pub fn new(broker: BrokerHandle, namespace: NamespaceName) -> Self {
        Self {
            broker,
            namespace,
            authentication: None,
            admission: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
        }
    }

    pub fn with_shared_access_policy(
        mut self,
        policy: SharedAccessPolicy,
        audience_host: impl AsRef<str>,
    ) -> Result<Self, ResourceScopeError> {
        let scope = ResourceScope::namespace(audience_host)?;
        self.authentication = Some(Authentication { policy, scope });
        Ok(self)
    }

    pub(crate) fn requires_authentication(&self) -> bool {
        self.authentication.is_some()
    }

    fn begin_request<T>(
        &self,
        request: &Request<T>,
        namespace: &str,
        entity_path: Option<&str>,
    ) -> Result<OwnedSemaphorePermit, Status> {
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("administration request limit reached"))?;
        if let Some(authentication) = &self.authentication {
            let token = request
                .metadata()
                .get("authorization")
                .ok_or_else(|| Status::unauthenticated("shared-access token required"))?
                .to_str()
                .map_err(|_| Status::unauthenticated("invalid shared-access token"))?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Status::unavailable("authentication clock unavailable"))?
                .as_secs();
            let grant = authentication
                .policy
                .authenticate_sas(token, now)
                .map_err(|_| Status::unauthenticated("invalid or expired shared-access token"))?;
            let scope = match entity_path {
                Some(path) => ResourceScope::entity(authentication.scope.host(), path)
                    .map_err(|_| Status::invalid_argument("invalid entity resource path"))?,
                None => authentication.scope.clone(),
            };
            if !grant.allows(&scope, Permission::Manage, now) {
                return Err(Status::permission_denied(
                    "entity management permission required",
                ));
            }
        }
        let namespace = NamespaceName::new(namespace)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if namespace != self.namespace {
            return Err(Status::permission_denied(
                "namespace is outside this endpoint",
            ));
        }
        Ok(permit)
    }

    fn entity_path(&self, path: &str) -> Result<EntityPath, Status> {
        let path =
            EntityPath::new(path).map_err(|error| Status::invalid_argument(error.to_string()))?;
        if path.is_dead_letter_queue() {
            return Err(Status::invalid_argument(
                "dead-letter queues are not administrable entities",
            ));
        }
        if path.is_subscription_path() {
            return Err(Status::invalid_argument(
                "subscription paths are not administrable through the queue API",
            ));
        }
        Ok(path)
    }

    async fn read_entity(&self, path: EntityPath) -> Result<Entity, Status> {
        let entity = self.read_target(AdminTarget::Primary(path)).await?;
        if entity.kind != EntityKind::Queue as i32 {
            return Err(Status::internal("unexpected queue metadata"));
        }
        Ok(entity)
    }

    async fn read_target(&self, target: AdminTarget) -> Result<Entity, Status> {
        let path = target
            .canonical_entity()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let metadata = self
            .broker
            .admin_entity_metadata(self.namespace.clone(), target)
            .await
            .map_err(read_status)?
            .ok_or_else(|| Status::not_found("entity does not exist"))?;
        topology::response(&self.namespace, &path, metadata)
    }
}

#[tonic::async_trait]
impl EntityService for NativeAdminService {
    async fn create_entity(
        &self,
        request: Request<CreateEntityRequest>,
    ) -> Result<Response<Entity>, Status> {
        let input = request.get_ref();
        let resource = topology::requested_resource(&input.path);
        let _permit = self.begin_request(&request, &input.namespace, Some(&resource))?;
        let kind = EntityKind::try_from(input.kind)
            .map_err(|_| Status::invalid_argument("unknown entity kind"))?;
        if kind == EntityKind::Unspecified {
            return Err(Status::invalid_argument("entity kind is required"));
        }
        if !input.placement_group_id.is_empty() {
            return Err(Status::unimplemented(
                "placement groups are not implemented",
            ));
        }
        if input.max_size_bytes != 0 {
            return Err(Status::unimplemented(
                "entity storage capacity is not implemented",
            ));
        }
        topology::reject_other_configuration(input, kind)?;
        let (path, command, expected, metadata) = match kind {
            EntityKind::Queue => {
                let path = self.entity_path(&input.path)?;
                let config = create_configuration(input)?;
                (
                    path,
                    CommandKind::CreateQueue { config },
                    CommandOutcome::QueueCreated,
                    protocol_amqp::EntityMetadata::Queue(config),
                )
            }
            EntityKind::Topic => {
                let path = self.entity_path(&input.path)?;
                let config = topology::topic_configuration(input.topic_config.as_ref())?;
                (
                    path,
                    CommandKind::CreateTopic { config },
                    CommandOutcome::TopicCreated,
                    protocol_amqp::EntityMetadata::Topic(config),
                )
            }
            EntityKind::Subscription => {
                let AdminTarget::Subscription { topic, name } = topology::target(&input.path)?
                else {
                    return Err(Status::invalid_argument(
                        "a subscription entity path is required",
                    ));
                };
                let path = topic
                    .subscription(&name)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                path.dead_letter_queue()
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                let config =
                    topology::subscription_configuration(input.subscription_config.as_ref())?;
                let command = CommandKind::CreateSubscription { name, config };
                let outcome = self
                    .broker
                    .submit(self.namespace.clone(), topic, command)
                    .await
                    .map_err(submit_status)?;
                if outcome != CommandOutcome::SubscriptionCreated {
                    return Err(Status::internal("unexpected subscription creation result"));
                }
                return Ok(Response::new(topology::response(
                    &self.namespace,
                    &path,
                    protocol_amqp::EntityMetadata::Subscription(config),
                )?));
            }
            EntityKind::Unspecified => {
                return Err(Status::invalid_argument("entity kind is required"));
            }
        };
        let outcome = self
            .broker
            .submit(self.namespace.clone(), path.clone(), command)
            .await
            .map_err(submit_status)?;
        if outcome != expected {
            return Err(Status::internal("unexpected entity creation result"));
        }
        Ok(Response::new(topology::response(
            &self.namespace,
            &path,
            metadata,
        )?))
    }

    async fn get_entity(
        &self,
        request: Request<GetEntityRequest>,
    ) -> Result<Response<Entity>, Status> {
        let input = request.get_ref();
        let resource = topology::requested_resource(&input.path);
        let _permit = self.begin_request(&request, &input.namespace, Some(&resource))?;
        Ok(Response::new(
            self.read_target(topology::target(&input.path)?).await?,
        ))
    }

    async fn list_entities(
        &self,
        request: Request<ListEntitiesRequest>,
    ) -> Result<Response<ListEntitiesResponse>, Status> {
        let input = request.get_ref();
        let scope =
            (input.kind == EntityKind::Subscription as i32).then_some(input.parent_topic.as_str());
        let _permit = self.begin_request(&request, &input.namespace, scope)?;
        let kind = EntityKind::try_from(input.kind)
            .map_err(|_| Status::invalid_argument("unknown entity kind"))?;
        if kind != EntityKind::Subscription && !input.parent_topic.is_empty() {
            return Err(Status::invalid_argument(
                "parent topic is only valid for subscription listings",
            ));
        }
        let page_size = if input.page_size == 0 {
            DEFAULT_PAGE_SIZE
        } else {
            usize::try_from(input.page_size)
                .map_err(|_| Status::invalid_argument("invalid page size"))?
        };
        if page_size > MAX_QUEUE_PAGE_SIZE {
            return Err(Status::invalid_argument("page size exceeds 1024"));
        }
        match kind {
            EntityKind::Topic => {
                return Ok(Response::new(self.list_topics(input, page_size).await?));
            }
            EntityKind::Subscription => {
                return Ok(Response::new(
                    self.list_subscriptions(input, page_size).await?,
                ));
            }
            EntityKind::Queue | EntityKind::Unspecified => {}
        }
        Ok(Response::new(self.list_queues(input, page_size).await?))
    }

    async fn update_entity(
        &self,
        request: Request<UpdateEntityRequest>,
    ) -> Result<Response<Entity>, Status> {
        let input = request.get_ref();
        let resource = topology::requested_resource(&input.path);
        let _permit = self.begin_request(&request, &input.namespace, Some(&resource))?;
        let target = topology::target(&input.path)?;
        let kind = topology::update_kind(input)?;
        if matches!(&target, AdminTarget::Subscription { .. }) != (kind == EntityKind::Subscription)
        {
            return Err(Status::invalid_argument(
                "configuration does not match the entity path",
            ));
        }
        let metadata = self
            .broker
            .admin_entity_metadata(self.namespace.clone(), target.clone())
            .await
            .map_err(read_status)?;
        let wrong_kind = matches!(
            (kind, metadata),
            (
                EntityKind::Queue,
                Some(protocol_amqp::EntityMetadata::Topic(_))
            ) | (
                EntityKind::Topic,
                Some(protocol_amqp::EntityMetadata::Queue(_))
            )
        );
        if wrong_kind {
            return Err(Status::invalid_argument(
                "configuration does not match the entity kind",
            ));
        }
        let (path, command, expected) = match (&target, kind) {
            (AdminTarget::Primary(path), EntityKind::Queue) => (
                path.clone(),
                CommandKind::UpdateQueue {
                    update: configuration_update(input.queue_config.as_ref().ok_or_else(
                        || Status::invalid_argument("queue configuration patch is required"),
                    )?)?,
                },
                CommandOutcome::QueueUpdated,
            ),
            (AdminTarget::Primary(path), EntityKind::Topic) => (
                path.clone(),
                CommandKind::UpdateTopic {
                    update: topology::topic_configuration_update(
                        input.topic_config.as_ref().ok_or_else(|| {
                            Status::invalid_argument("topic configuration patch is required")
                        })?,
                    )?,
                },
                CommandOutcome::TopicUpdated,
            ),
            (AdminTarget::Subscription { topic, name }, EntityKind::Subscription) => (
                topic.clone(),
                CommandKind::UpdateSubscription {
                    name: name.clone(),
                    update: topology::subscription_configuration_update(
                        input.subscription_config.as_ref().ok_or_else(|| {
                            Status::invalid_argument("subscription configuration patch is required")
                        })?,
                    )?,
                },
                CommandOutcome::SubscriptionUpdated,
            ),
            _ => {
                return Err(Status::invalid_argument(
                    "configuration does not match the entity path",
                ));
            }
        };
        let outcome = self
            .broker
            .submit(self.namespace.clone(), path, command)
            .await
            .map_err(submit_status)?;
        if outcome != expected {
            return Err(Status::internal("unexpected entity update result"));
        }
        let entity = self.read_target(target).await?;
        if entity.kind != kind as i32 {
            return Err(Status::internal("unexpected updated entity metadata"));
        }
        Ok(Response::new(entity))
    }

    async fn delete_entity(
        &self,
        request: Request<DeleteEntityRequest>,
    ) -> Result<Response<Operation>, Status> {
        let input = request.get_ref();
        let resource = topology::requested_resource(&input.path);
        let _permit = self.begin_request(&request, &input.namespace, Some(&resource))?;
        let kind = EntityKind::try_from(input.kind)
            .map_err(|_| Status::invalid_argument("unknown entity kind"))?;
        let (path, target) = deletion::target(topology::target(&input.path)?, kind)?;
        let outcome = self
            .broker
            .submit(
                self.namespace.clone(),
                path,
                CommandKind::DeleteEntity {
                    target: target.clone(),
                },
            )
            .await
            .map_err(deletion::status)?;
        if !deletion::matches_outcome(&target, &outcome) {
            return Err(Status::internal("unexpected entity deletion result"));
        }
        Ok(Response::new(Operation {
            operation_id: String::new(),
            state: "completed".to_owned(),
            error: String::new(),
        }))
    }
}

fn create_configuration(input: &CreateEntityRequest) -> Result<QueueConfig, Status> {
    let defaults = QueueConfig::default();
    let config = if let Some(config) = &input.queue_config {
        if input.default_ttl_millis != 0
            || input.lock_duration_millis != 0
            || input.max_delivery_count != 0
            || input.requires_session
        {
            return Err(Status::invalid_argument(
                "legacy and queue configuration settings cannot be mixed",
            ));
        }
        let patch = configuration_update(config)?;
        QueueConfig {
            lock_duration_millis: patch
                .lock_duration_millis
                .unwrap_or(defaults.lock_duration_millis),
            max_delivery_count: patch
                .max_delivery_count
                .unwrap_or(defaults.max_delivery_count),
            default_time_to_live_millis: match patch.default_time_to_live_millis {
                Some(QueueTimeToLiveUpdate::Finite { millis }) => Some(millis),
                None | Some(QueueTimeToLiveUpdate::Unlimited) => None,
            },
            max_message_bytes: patch
                .max_message_bytes
                .unwrap_or(defaults.max_message_bytes),
            requires_session: patch.requires_session.unwrap_or(defaults.requires_session),
            requires_duplicate_detection: patch
                .requires_duplicate_detection
                .unwrap_or(defaults.requires_duplicate_detection),
            duplicate_detection_history_time_window_millis: patch
                .duplicate_detection_history_time_window_millis
                .unwrap_or(defaults.duplicate_detection_history_time_window_millis),
            dead_lettering_on_message_expiration: patch
                .dead_lettering_on_message_expiration
                .unwrap_or(defaults.dead_lettering_on_message_expiration),
        }
    } else {
        QueueConfig {
            lock_duration_millis: if input.lock_duration_millis == 0 {
                defaults.lock_duration_millis
            } else {
                input.lock_duration_millis
            },
            max_delivery_count: if input.max_delivery_count == 0 {
                defaults.max_delivery_count
            } else {
                input.max_delivery_count
            },
            default_time_to_live_millis: (input.default_ttl_millis != 0)
                .then_some(input.default_ttl_millis),
            requires_session: input.requires_session,
            ..defaults
        }
    };
    config
        .validate()
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn configuration_update(config: &QueueConfiguration) -> Result<QueueConfigUpdate, Status> {
    Ok(QueueConfigUpdate {
        lock_duration_millis: config.lock_duration_millis,
        max_delivery_count: config.max_delivery_count,
        default_time_to_live_millis: config.default_time_to_live.as_ref().map(|ttl| match ttl {
            DefaultTimeToLive::DefaultTtlMillis(millis) => {
                QueueTimeToLiveUpdate::Finite { millis: *millis }
            }
            DefaultTimeToLive::DefaultTtlUnlimited(_) => QueueTimeToLiveUpdate::Unlimited,
        }),
        max_message_bytes: config
            .max_message_bytes
            .map(|bytes| {
                usize::try_from(bytes)
                    .map_err(|_| Status::invalid_argument("message limit exceeds this platform"))
            })
            .transpose()?,
        requires_session: config.requires_session,
        requires_duplicate_detection: config.requires_duplicate_detection,
        duplicate_detection_history_time_window_millis: config
            .duplicate_detection_history_time_window_millis,
        dead_lettering_on_message_expiration: config.dead_lettering_on_message_expiration,
    })
}

fn entity_response(namespace: &NamespaceName, path: &EntityPath, config: QueueConfig) -> Entity {
    Entity {
        namespace: namespace.as_str().to_owned(),
        path: path.as_str().to_owned(),
        kind: EntityKind::Queue as i32,
        placement_group_id: String::new(),
        max_size_bytes: None,
        used_logical_bytes: None,
        queue_config: Some(QueueConfiguration {
            lock_duration_millis: Some(config.lock_duration_millis),
            max_delivery_count: Some(config.max_delivery_count),
            default_time_to_live: Some(match config.default_time_to_live_millis {
                Some(millis) => DefaultTimeToLive::DefaultTtlMillis(millis),
                None => DefaultTimeToLive::DefaultTtlUnlimited(v1::UnlimitedTimeToLive {}),
            }),
            max_message_bytes: Some(config.max_message_bytes as u64),
            requires_session: Some(config.requires_session),
            requires_duplicate_detection: Some(config.requires_duplicate_detection),
            duplicate_detection_history_time_window_millis: Some(
                config.duplicate_detection_history_time_window_millis,
            ),
            dead_lettering_on_message_expiration: Some(config.dead_lettering_on_message_expiration),
        }),
        topic_config: None,
        subscription_config: None,
    }
}

fn submit_status(error: SubmitError) -> Status {
    match error {
        SubmitError::BrokerStopped => Status::unavailable("broker owner is unavailable"),
        SubmitError::Propose(ProposeError::ClockWentBackward { .. }) => {
            Status::unavailable("broker clock is unavailable")
        }
        SubmitError::Propose(ProposeError::Broker(error)) => match error {
            BrokerError::QueueNotFound
            | BrokerError::TopicNotFound
            | BrokerError::EntityBindingStale
            | BrokerError::SubscriptionNotFound => Status::not_found(error.to_string()),
            BrokerError::QueueAlreadyExists
            | BrokerError::TopicAlreadyExists
            | BrokerError::SubscriptionAlreadyExists
            | BrokerError::EntityPathAlreadyExists => Status::already_exists(error.to_string()),
            BrokerError::QueuePropertyIsImmutable { .. }
            | BrokerError::TopicPropertyIsImmutable { .. }
            | BrokerError::SubscriptionPropertyIsImmutable { .. } => {
                Status::failed_precondition(error.to_string())
            }
            BrokerError::QueueConfig(_)
            | BrokerError::TopicConfig(_)
            | BrokerError::SubscriptionConfig(_)
            | BrokerError::EntityKindMismatch
            | BrokerError::InvalidEntityBinding
            | BrokerError::Identifier(_)
            | BrokerError::DeadLetterQueueIsReserved
            | BrokerError::SubscriptionPathIsReserved
            | BrokerError::QueuePageLimitExceeded { .. }
            | BrokerError::QueueCursorNamespaceMismatch { .. }
            | BrokerError::TopicPageLimitExceeded { .. }
            | BrokerError::TopicCursorNamespaceMismatch { .. } => {
                Status::invalid_argument(error.to_string())
            }
            BrokerError::TopicDataPlaneNotImplemented => Status::unimplemented(error.to_string()),
            BrokerError::SubscriptionLimitExceeded { .. }
            | BrokerError::EntityIncarnationExhausted
            | BrokerError::EntityDeleteTooLarge { .. }
            | BrokerError::TopicFanoutTooLarge { .. } => {
                Status::resource_exhausted(error.to_string())
            }
            _ => Status::internal("broker operation failed"),
        },
        SubmitError::Propose(ProposeError::UnexpectedOutcome { .. }) => {
            Status::internal("unexpected broker operation result")
        }
    }
}

fn read_status(error: SubmitError) -> Status {
    match error {
        SubmitError::Propose(ProposeError::Broker(
            BrokerError::QueueConfig(_)
            | BrokerError::TopicConfig(_)
            | BrokerError::SubscriptionConfig(_)
            | BrokerError::Identifier(_),
        )) => Status::internal("invalid stored entity metadata"),
        error => submit_status(error),
    }
}
