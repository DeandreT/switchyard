//! Local administration contract. No transport, authorization or runtime startup.

use std::{error::Error, fmt, future::Future};

use domain::{
    AdminCatalogCursor, AdminCatalogEntry, AdminCatalogError, AdminCatalogKind, AdminCatalogPage,
    CommandKind, CommandOutcome, EntityPath, IdentifierError, MAX_ADMIN_CATALOG_PAGE,
    NamespaceName, QueueConfig, SubscriptionConfig, SubscriptionName, TopicConfig,
};
use serde::{Deserialize, Serialize};

use crate::{EntityDefinition, EntityKind};

/// Subscription components are validated separately, never parsed as an ordinary
/// path (their composite can legitimately exceed the ordinary path length limit).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EntityAddress {
    Entity { path: String },
    Subscription { topic: String, name: String },
}

/// Validated dispatch address. Adapters must still enforce their service scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EntityTarget {
    Entity(EntityPath),
    Subscription {
        topic: EntityPath,
        name: SubscriptionName,
        path: EntityPath,
    },
}

impl EntityTarget {
    pub fn path(&self) -> &EntityPath {
        match self {
            Self::Entity(path) | Self::Subscription { path, .. } => path,
        }
    }
}

/// Omission uses the domain default. Explicit zero limits reach domain validation
/// on the owner, except duplicate-history zero, whose legacy meaning is default.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EntityOptions {
    pub default_time_to_live_millis: Option<u64>,
    pub lock_duration_millis: Option<u64>,
    pub max_delivery_count: Option<u32>,
    pub max_message_bytes: Option<usize>,
    pub requires_session: Option<bool>,
    pub requires_duplicate_detection: Option<bool>,
    pub duplicate_detection_history_millis: Option<u64>,
    pub placement_group_id: Option<String>,
    pub max_size_bytes: Option<u64>,
    pub used_logical_bytes: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateEntityRequest {
    pub namespace: String,
    pub address: EntityAddress,
    pub kind: EntityKind,
    pub options: EntityOptions,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetEntityRequest {
    pub namespace: String,
    pub address: EntityAddress,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListEntitiesRequest {
    pub namespace: String,
    pub kind: AdminCatalogKind,
    pub continuation: Option<Vec<u8>>,
    /// Explicitly bounded to 1..=MAX_ADMIN_CATALOG_PAGE; zero is not a default.
    pub limit: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityProfile {
    Queue(QueueConfig),
    Topic(TopicConfig),
    /// Receive-side settings only. Message size is inherited from the topic,
    /// not an independently configurable subscription limit or entity capacity.
    Subscription(SubscriptionConfig),
}

impl EntityProfile {
    pub fn kind(&self) -> EntityKind {
        match self {
            Self::Queue(_) => EntityKind::Queue,
            Self::Topic(_) => EntityKind::Topic,
            Self::Subscription(_) => EntityKind::Subscription,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityPage {
    pub entities: Vec<EntityDefinition>,
    /// Opaque local-version bytes; may accompany an empty page.
    pub continuation: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnsupportedFeature {
    Placement,
    Capacity,
    Usage,
    LockDuration,
    MaxDeliveryCount,
    MessageSize,
    Sessions,
    DuplicateDetection,
    DuplicateHistory,
}

/// Backend errors remain typed. In the real adapter this includes the exact
/// domain/storage/clock/stopped error, rather than its display string.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceError<E> {
    Identifier(IdentifierError),
    ForeignNamespace {
        expected: NamespaceName,
        actual: NamespaceName,
    },
    InvalidEntityPath,
    AddressKindMismatch,
    Unsupported(UnsupportedFeature),
    Catalog(AdminCatalogError),
    NotFound {
        target: EntityTarget,
    },
    Backend(E),
    UnexpectedOutcome {
        expected: Box<CommandOutcome>,
        actual: Box<CommandOutcome>,
    },
}

impl<E: fmt::Display> fmt::Display for ServiceError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identifier(error) => fmt::Display::fmt(error, f),
            Self::ForeignNamespace { expected, actual } => {
                write!(
                    f,
                    "namespace {actual} does not match bound namespace {expected}"
                )
            }
            Self::InvalidEntityPath => f.write_str("a public queue/topic path is required"),
            Self::AddressKindMismatch => f.write_str("entity kind does not match address"),
            Self::Unsupported(feature) => write!(f, "unsupported entity setting: {feature:?}"),
            Self::Catalog(error) => fmt::Display::fmt(error, f),
            Self::NotFound { target } => write!(f, "entity {} does not exist", target.path()),
            Self::Backend(error) => fmt::Display::fmt(error, f),
            Self::UnexpectedOutcome { expected, actual } => {
                write!(f, "expected {expected:?}, received {actual:?}")
            }
        }
    }
}

impl<E: Error + 'static> Error for ServiceError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identifier(error) => Some(error),
            Self::Catalog(error) => Some(error),
            Self::Backend(error) => Some(error),
            _ => None,
        }
    }
}

/// Each read must be one indivisible owner dispatch, serialized against writes.
/// Create must use the same owner-stamped path as existing broker commands.
pub trait EntityBackend: Clone + Send + Sync {
    type Error: Error + Send + Sync + 'static;

    fn create(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        command: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, ServiceError<Self::Error>>> + Send;

    fn get(
        &self,
        namespace: NamespaceName,
        target: EntityTarget,
    ) -> impl Future<Output = Result<Option<AdminCatalogEntry>, ServiceError<Self::Error>>> + Send;

    fn list(
        &self,
        namespace: NamespaceName,
        kind: AdminCatalogKind,
        after: Option<AdminCatalogCursor>,
        limit: usize,
    ) -> impl Future<Output = Result<AdminCatalogPage, ServiceError<Self::Error>>> + Send;
}

#[derive(Clone, Debug)]
pub struct EntityService<B> {
    namespace: NamespaceName,
    backend: B,
}

impl<B: EntityBackend> EntityService<B> {
    /// The bound namespace is already canonical. This is scope, not authorization.
    pub fn new(namespace: NamespaceName, backend: B) -> Self {
        Self { namespace, backend }
    }

    pub fn namespace(&self) -> &NamespaceName {
        &self.namespace
    }

    fn check_namespace(&self, value: String) -> Result<(), ServiceError<B::Error>> {
        let actual = NamespaceName::new(value).map_err(ServiceError::Identifier)?;
        if actual != self.namespace {
            return Err(ServiceError::ForeignNamespace {
                expected: self.namespace.clone(),
                actual,
            });
        }
        Ok(())
    }

    pub async fn create(
        &self,
        request: CreateEntityRequest,
    ) -> Result<EntityDefinition, ServiceError<B::Error>> {
        self.check_namespace(request.namespace)?;
        let target = validate_address(request.address)?;
        if matches!(target, EntityTarget::Subscription { .. })
            != (request.kind == EntityKind::Subscription)
        {
            return Err(ServiceError::AddressKindMismatch);
        }
        let profile = request.options.profile(request.kind)?;
        let (entity, command, expected) = match (&target, &profile) {
            (EntityTarget::Entity(path), EntityProfile::Queue(config)) => (
                path.clone(),
                CommandKind::CreateQueue { config: *config },
                CommandOutcome::QueueCreated,
            ),
            (EntityTarget::Entity(path), EntityProfile::Topic(config)) => (
                path.clone(),
                CommandKind::CreateTopic { config: *config },
                CommandOutcome::TopicCreated,
            ),
            (
                EntityTarget::Subscription { topic, name, path },
                EntityProfile::Subscription(config),
            ) => (
                topic.clone(),
                CommandKind::CreateSubscription {
                    name: name.clone(),
                    config: *config,
                },
                CommandOutcome::SubscriptionCreated {
                    entity: path.clone(),
                },
            ),
            _ => return Err(ServiceError::AddressKindMismatch),
        };
        // No existence preflight or early domain-config validation: duplicate,
        // clock and domain-refusal ordering remains the existing owner's policy.
        let actual = self
            .backend
            .create(self.namespace.clone(), entity, command)
            .await?;
        if actual != expected {
            return Err(ServiceError::UnexpectedOutcome {
                expected: Box::new(expected),
                actual: Box::new(actual),
            });
        }
        Ok(definition(
            self.namespace.clone(),
            target.path().clone(),
            profile,
        ))
    }

    pub async fn get(
        &self,
        request: GetEntityRequest,
    ) -> Result<EntityDefinition, ServiceError<B::Error>> {
        self.check_namespace(request.namespace)?;
        let target = validate_address(request.address)?;
        let entry = self
            .backend
            .get(self.namespace.clone(), target.clone())
            .await?
            .ok_or(ServiceError::NotFound { target })?;
        Ok(from_entry(self.namespace.clone(), entry))
    }

    pub async fn list(
        &self,
        request: ListEntitiesRequest,
    ) -> Result<EntityPage, ServiceError<B::Error>> {
        self.check_namespace(request.namespace)?;
        if !(1..=MAX_ADMIN_CATALOG_PAGE).contains(&request.limit) {
            return Err(ServiceError::Catalog(AdminCatalogError::InvalidPageSize {
                requested: request.limit,
            }));
        }
        let after = request
            .continuation
            .as_deref()
            .map(AdminCatalogCursor::from_bytes)
            .transpose()
            .map_err(ServiceError::Catalog)?;
        if let Some(after) = &after {
            after
                .validate_scope(&self.namespace, request.kind)
                .map_err(ServiceError::Catalog)?;
        }
        let page = self
            .backend
            .list(self.namespace.clone(), request.kind, after, request.limit)
            .await?;
        Ok(EntityPage {
            entities: page
                .entries
                .into_iter()
                .map(|entry| from_entry(self.namespace.clone(), entry))
                .collect(),
            continuation: page.continuation.map(|cursor| cursor.as_bytes().to_vec()),
        })
    }
}

fn public_path<E>(value: String) -> Result<EntityPath, ServiceError<E>> {
    let path = EntityPath::new(value).map_err(ServiceError::Identifier)?;
    if path.is_dead_letter_queue() || path.is_subscription() || path.is_management() {
        return Err(ServiceError::InvalidEntityPath);
    }
    Ok(path)
}

fn validate_address<E>(address: EntityAddress) -> Result<EntityTarget, ServiceError<E>> {
    match address {
        EntityAddress::Entity { path } => public_path(path).map(EntityTarget::Entity),
        EntityAddress::Subscription { topic, name } => {
            let topic = public_path(topic)?;
            let name = SubscriptionName::new(name).map_err(ServiceError::Identifier)?;
            let path = topic
                .subscription(&name)
                .map_err(ServiceError::Identifier)?;
            Ok(EntityTarget::Subscription { topic, name, path })
        }
    }
}

impl EntityOptions {
    fn profile<E>(self, kind: EntityKind) -> Result<EntityProfile, ServiceError<E>> {
        let unsupported = if self.placement_group_id.is_some() {
            Some(UnsupportedFeature::Placement)
        } else if self.max_size_bytes.is_some() {
            Some(UnsupportedFeature::Capacity)
        } else if self.used_logical_bytes.is_some() {
            Some(UnsupportedFeature::Usage)
        } else if kind == EntityKind::Topic && self.lock_duration_millis.is_some() {
            Some(UnsupportedFeature::LockDuration)
        } else if kind == EntityKind::Topic && self.max_delivery_count.is_some() {
            Some(UnsupportedFeature::MaxDeliveryCount)
        } else if kind == EntityKind::Subscription && self.max_message_bytes.is_some() {
            Some(UnsupportedFeature::MessageSize)
        } else if kind != EntityKind::Queue && self.requires_session.is_some() {
            Some(UnsupportedFeature::Sessions)
        } else if kind != EntityKind::Queue && self.requires_duplicate_detection.is_some() {
            Some(UnsupportedFeature::DuplicateDetection)
        } else if kind != EntityKind::Queue && self.duplicate_detection_history_millis.is_some() {
            Some(UnsupportedFeature::DuplicateHistory)
        } else {
            None
        };
        if let Some(feature) = unsupported {
            return Err(ServiceError::Unsupported(feature));
        }
        Ok(match kind {
            EntityKind::Queue => {
                let defaults = QueueConfig::default();
                EntityProfile::Queue(QueueConfig {
                    default_time_to_live_millis: self.default_time_to_live_millis,
                    lock_duration_millis: self
                        .lock_duration_millis
                        .unwrap_or(defaults.lock_duration_millis),
                    max_delivery_count: self
                        .max_delivery_count
                        .unwrap_or(defaults.max_delivery_count),
                    max_message_bytes: self.max_message_bytes.unwrap_or(defaults.max_message_bytes),
                    requires_session: self.requires_session.unwrap_or(defaults.requires_session),
                    requires_duplicate_detection: self
                        .requires_duplicate_detection
                        .unwrap_or(defaults.requires_duplicate_detection),
                    duplicate_detection_history_millis: self
                        .duplicate_detection_history_millis
                        .filter(|value| *value != 0)
                        .unwrap_or(defaults.duplicate_detection_history_millis),
                })
            }
            EntityKind::Topic => EntityProfile::Topic(TopicConfig {
                default_time_to_live_millis: self.default_time_to_live_millis,
                max_message_bytes: self
                    .max_message_bytes
                    .unwrap_or(TopicConfig::default().max_message_bytes),
                ..TopicConfig::default()
            }),
            EntityKind::Subscription => {
                let defaults = SubscriptionConfig::default();
                EntityProfile::Subscription(SubscriptionConfig {
                    default_time_to_live_millis: self.default_time_to_live_millis,
                    lock_duration_millis: self
                        .lock_duration_millis
                        .unwrap_or(defaults.lock_duration_millis),
                    max_delivery_count: self
                        .max_delivery_count
                        .unwrap_or(defaults.max_delivery_count),
                })
            }
        })
    }
}

fn definition(
    namespace: NamespaceName,
    path: EntityPath,
    profile: EntityProfile,
) -> EntityDefinition {
    EntityDefinition {
        namespace,
        path,
        profile,
        placement_group_id: None,
        max_size_bytes: None,
        used_logical_bytes: None,
    }
}

fn from_entry(namespace: NamespaceName, entry: AdminCatalogEntry) -> EntityDefinition {
    match entry {
        AdminCatalogEntry::Queue(queue) => {
            definition(namespace, queue.path, EntityProfile::Queue(queue.config))
        }
        AdminCatalogEntry::Topic(topic) => {
            definition(namespace, topic.path, EntityProfile::Topic(topic.config))
        }
        AdminCatalogEntry::Subscription(subscription) => definition(
            namespace,
            subscription.path,
            EntityProfile::Subscription(subscription.config),
        ),
    }
}
