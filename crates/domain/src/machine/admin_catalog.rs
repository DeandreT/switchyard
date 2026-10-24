//! Namespace-local administration reads, independent of timer enumeration.
//!
//! The caller must serialize each whole read against every writer, including
//! cloned/raw store handles. These reads neither acquire a snapshot nor prove
//! whole-store health, and continuation does not preserve a cross-page snapshot.

use serde::{Serialize, de::DeserializeOwned};
use storage::{StateStore, StorageError};
use thiserror::Error;

use crate::{
    BrokerError, DEAD_LETTER_QUEUE_SUFFIX, EntityPath, NamespaceName, QueueConfig,
    SubscriptionConfig, SubscriptionName, TopicConfig, codec,
    identifier::SUBSCRIPTION_PATH_SEGMENT, keys,
};

use super::{StateMachine, incarnations::Kind};

/// Maximum number of public entities returned by one administration page.
pub const MAX_ADMIN_CATALOG_PAGE: usize = 100;
/// Maximum raw namespace rows consumed by one page, excluding one lookahead row.
///
/// Internal queues count toward this bound. A page may therefore be empty with
/// a continuation; following it always advances beyond at least one raw row.
pub const MAX_ADMIN_CATALOG_SCAN: usize = 256;

/// Which public entity kinds an administration page selects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminCatalogKind {
    /// Ordinary queues only, never subscription backing queues or DLQs.
    Queues,
    /// Topics only.
    Topics,
    /// Explicit subscription memberships only.
    Subscriptions,
    /// Queues, then topics, then subscriptions; byte-key order within each kind.
    All,
}

/// An ordinary queue's selected, validated public configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminQueue {
    /// Canonical queue path, without an internal suffix.
    pub path: EntityPath,
    /// Validated queue settings; no usage or capacity estimate is included.
    pub config: QueueConfig,
}

/// A topic's selected, validated public configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminTopic {
    /// Canonical topic path.
    pub path: EntityPath,
    /// Validated topic settings, not a certificate for all its children.
    pub config: TopicConfig,
}

/// One explicit subscription with its parent-derived backing profile checked.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminSubscription {
    /// Canonical parent topic path.
    pub topic: EntityPath,
    /// Canonical name within the topic.
    pub name: SubscriptionName,
    /// Canonical backing path, exposed only as an explicit subscription.
    pub path: EntityPath,
    /// Validated subscription settings; inherited message size stays on the topic.
    pub config: SubscriptionConfig,
}

/// One public entity in a namespace administration page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminCatalogEntry {
    /// An ordinary queue.
    Queue(AdminQueue),
    /// A topic.
    Topic(AdminTopic),
    /// An explicit subscription.
    Subscription(AdminSubscription),
}

/// A bounded read result, valid only under the caller's serialized-read contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminCatalogPage {
    /// Selected entries; any selected corruption rejects the entire page.
    pub entries: Vec<AdminCatalogEntry>,
    /// Present exactly when unconsumed raw rows remain in the selected kinds.
    /// Internal-only rows can produce an empty subsequent page.
    pub continuation: Option<AdminCatalogCursor>,
}

/// An opaque, namespace-and-kind-bound position, not authority or a snapshot.
///
/// Bytes are a bounded, versioned local continuation format, not durable schema.
/// They may be round-tripped during this process/version and are strictly
/// validated before use. Deleting the position row does not invalidate the cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminCatalogCursor(Vec<u8>);

impl AdminCatalogCursor {
    /// Parses a canonical position, refusing unsupported, malformed or oversized bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AdminCatalogError> {
        parse_cursor(bytes)?;
        Ok(Self(bytes.to_vec()))
    }

    /// Returns the opaque local continuation bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Checks the namespace and kind binding without consulting any store.
    ///
    /// Adapters can reject a foreign continuation before requesting a reader.
    /// This establishes neither authorization nor a cross-page snapshot.
    pub fn validate_scope(
        &self,
        namespace: &NamespaceName,
        kind: AdminCatalogKind,
    ) -> Result<(), AdminCatalogError> {
        let (bound_kind, bound_namespace, _, _) = parse_cursor(self.as_bytes())?;
        if bound_kind != kind || &bound_namespace != namespace {
            return Err(AdminCatalogError::CursorScopeMismatch);
        }
        Ok(())
    }
}

/// Invalid requests, selected catalog corruption, or an underlying read failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum AdminCatalogError {
    /// A page must request between one and the documented maximum entries.
    #[error(
        "administration page size {requested} is outside 1..={}",
        MAX_ADMIN_CATALOG_PAGE
    )]
    InvalidPageSize {
        /// Rejected requested size.
        requested: usize,
    },
    /// A Get requires an ordinary queue/topic path, not an internal endpoint.
    #[error("administration Get requires a public queue or topic path")]
    InvalidEntityPath,
    /// The cursor bytes are not a canonical supported local position.
    #[error("invalid administration continuation")]
    InvalidCursor,
    /// A valid cursor was presented for a different namespace or kind selection.
    #[error("administration continuation belongs to a different namespace or kind")]
    CursorScopeMismatch,
    /// A selected key, value, owner or required topology/profile companion is invalid.
    #[error("selected administration catalog is corrupt")]
    CorruptCatalog,
    /// The backend could not complete a read.
    #[error(transparent)]
    Storage(#[from] StorageError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Queue,
    Topic,
    Subscription,
}

impl AdminCatalogKind {
    fn phases(self) -> &'static [Phase] {
        match self {
            Self::Queues => &[Phase::Queue],
            Self::Topics => &[Phase::Topic],
            Self::Subscriptions => &[Phase::Subscription],
            Self::All => &[Phase::Queue, Phase::Topic, Phase::Subscription],
        }
    }

    fn tag(self) -> u8 {
        match self {
            Self::Queues => 0,
            Self::Topics => 1,
            Self::Subscriptions => 2,
            Self::All => 3,
        }
    }
}

fn primary(path: &EntityPath) -> bool {
    EntityPath::new(path.as_str()).is_ok()
        && !path.is_dead_letter_queue()
        && !path.is_subscription()
        && !path.is_management()
}

fn subscription_path(path: &EntityPath) -> Option<(EntityPath, SubscriptionName)> {
    let (topic, name) = path.as_str().split_once(SUBSCRIPTION_PATH_SEGMENT)?;
    let topic = EntityPath::new(topic).ok()?;
    let name = SubscriptionName::new(name).ok()?;
    (primary(&topic) && topic.subscription(&name).ok()?.as_str() == path.as_str())
        .then_some((topic, name))
}

fn internal_queue(path: &EntityPath) -> Result<bool, AdminCatalogError> {
    if primary(path) {
        return Ok(false);
    }
    if subscription_path(path).is_some() {
        return Ok(true);
    }
    if let Some(owner) = path.as_str().strip_suffix(DEAD_LETTER_QUEUE_SUFFIX) {
        let owner =
            EntityPath::from_internal(owner).map_err(|_| AdminCatalogError::CorruptCatalog)?;
        if primary(&owner) || subscription_path(&owner).is_some() {
            return Ok(true);
        }
    }
    Err(AdminCatalogError::CorruptCatalog)
}

fn row_identity(key: &[u8]) -> Result<(NamespaceName, Phase), AdminCatalogError> {
    if let Some((namespace, path)) = keys::catalog_entity_parts(key) {
        if key == keys::queue_config(&namespace, &path) {
            internal_queue(&path)?;
            return Ok((namespace, Phase::Queue));
        }
        if key == keys::topic_config(&namespace, &path) && primary(&path) {
            return Ok((namespace, Phase::Topic));
        }
    }
    if let Some((namespace, topic, _)) = keys::catalog_subscription_parts(key)
        && primary(&topic)
    {
        return Ok((namespace, Phase::Subscription));
    }
    Err(AdminCatalogError::CorruptCatalog)
}

fn parse_cursor(
    bytes: &[u8],
) -> Result<(AdminCatalogKind, NamespaceName, Phase, &[u8]), AdminCatalogError> {
    // Two header bytes plus the longest namespace/subscription key. Keep a hard
    // cap before parsing or allocating any untrusted continuation content.
    if bytes.len() > 512 || bytes.first() != Some(&1) {
        return Err(AdminCatalogError::InvalidCursor);
    }
    let kind = match bytes.get(1) {
        Some(0) => AdminCatalogKind::Queues,
        Some(1) => AdminCatalogKind::Topics,
        Some(2) => AdminCatalogKind::Subscriptions,
        Some(3) => AdminCatalogKind::All,
        _ => return Err(AdminCatalogError::InvalidCursor),
    };
    let key = bytes.get(2..).ok_or(AdminCatalogError::InvalidCursor)?;
    let (namespace, phase) = row_identity(key).map_err(|_| AdminCatalogError::InvalidCursor)?;
    if !kind.phases().contains(&phase) {
        return Err(AdminCatalogError::InvalidCursor);
    }
    Ok((kind, namespace, phase, key))
}

fn cursor(kind: AdminCatalogKind, key: &[u8]) -> AdminCatalogCursor {
    let mut bytes = vec![1, kind.tag()];
    bytes.extend_from_slice(key);
    AdminCatalogCursor(bytes)
}

fn exact<T: DeserializeOwned + Serialize>(raw: &[u8]) -> Result<T, AdminCatalogError> {
    let value = codec::decode(raw).map_err(|_| AdminCatalogError::CorruptCatalog)?;
    if codec::encode(&value).map_err(|_| AdminCatalogError::CorruptCatalog)? != raw {
        return Err(AdminCatalogError::CorruptCatalog);
    }
    Ok(value)
}

impl<S: StateStore> StateMachine<S> {
    /// Gets one ordinary queue without reading or advancing Clock.
    ///
    /// Requires the caller to exclude all writers for the complete call. `None`
    /// means no selected entity or orphaned required companions were found (a
    /// healthy topic at this name is not a queue). Present rows require exact
    /// canonical values, a live queue owner and the matching real DLQ profile.
    pub fn admin_queue(
        &self,
        namespace: &NamespaceName,
        path: &EntityPath,
    ) -> Result<Option<AdminQueue>, AdminCatalogError> {
        if !primary(path) {
            return Err(AdminCatalogError::InvalidEntityPath);
        }
        let queue = self.store().get(&keys::queue_config(namespace, path))?;
        let topic = self.store().get(&keys::topic_config(namespace, path))?;
        let Some(raw) = queue else {
            if let Some(raw) = topic {
                self.admin_topic_profile(namespace, path, &raw)?;
            } else {
                self.admin_absent(namespace, path)?;
            }
            return Ok(None);
        };
        if topic.is_some() {
            return Err(AdminCatalogError::CorruptCatalog);
        }
        let config: QueueConfig = exact(&raw)?;
        config
            .validate()
            .map_err(|_| AdminCatalogError::CorruptCatalog)?;
        self.admin_owner(namespace, path, Kind::Queue)?;
        let shadow = path
            .dead_letter_queue()
            .map_err(|_| AdminCatalogError::CorruptCatalog)?;
        self.admin_expected(
            &keys::queue_config(namespace, &shadow),
            &config.dead_letter_shadow(),
        )?;
        self.admin_missing(&keys::topic_config(namespace, &shadow))?;
        if !self
            .store()
            .scan_prefix(&keys::topic_subscription_prefix(namespace, path), 1)?
            .is_empty()
        {
            return Err(AdminCatalogError::CorruptCatalog);
        }
        Ok(Some(AdminQueue {
            path: path.clone(),
            config,
        }))
    }

    /// Gets one topic under the same serialized-read contract as [`Self::admin_queue`].
    ///
    /// Checks the selected topic profile/owner and forbidden queue/DLQ companions,
    /// not all children, rules or messages. A healthy queue at this name returns `None`.
    pub fn admin_topic(
        &self,
        namespace: &NamespaceName,
        path: &EntityPath,
    ) -> Result<Option<AdminTopic>, AdminCatalogError> {
        if !primary(path) {
            return Err(AdminCatalogError::InvalidEntityPath);
        }
        let Some(raw) = self.store().get(&keys::topic_config(namespace, path))? else {
            self.admin_queue(namespace, path)?;
            return Ok(None);
        };
        self.admin_topic_profile(namespace, path, &raw).map(Some)
    }

    /// Gets an explicit membership, never an unlisted subscription backing queue.
    ///
    /// The whole call requires serialized readers/exclusive writers. The selected
    /// membership bytes, parent topic, subscription owner, inherited backing
    /// configuration and actual DLQ must agree. Missing membership with surviving
    /// child companions is corruption; rules and unrelated children are not read.
    pub fn admin_subscription(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        name: &SubscriptionName,
    ) -> Result<Option<AdminSubscription>, AdminCatalogError> {
        if !primary(topic) {
            return Err(AdminCatalogError::InvalidEntityPath);
        }
        let path = topic
            .subscription(name)
            .map_err(|_| AdminCatalogError::InvalidEntityPath)?;
        let membership = self
            .store()
            .get(&keys::topic_subscription(namespace, topic, name))?;
        let parent = self.admin_topic(namespace, topic)?;
        let Some(raw) = membership else {
            self.admin_absent(namespace, &path)?;
            return Ok(None);
        };
        let parent = parent.ok_or(AdminCatalogError::CorruptCatalog)?;
        if raw != codec::encode(&path).map_err(|_| AdminCatalogError::CorruptCatalog)? {
            return Err(AdminCatalogError::CorruptCatalog);
        }
        let raw = self
            .store()
            .get(&keys::queue_config(namespace, &path))?
            .ok_or(AdminCatalogError::CorruptCatalog)?;
        let backing: QueueConfig = exact(&raw)?;
        let config = SubscriptionConfig {
            lock_duration_millis: backing.lock_duration_millis,
            max_delivery_count: backing.max_delivery_count,
            default_time_to_live_millis: backing.default_time_to_live_millis,
        };
        config
            .validate()
            .map_err(|_| AdminCatalogError::CorruptCatalog)?;
        let expected = config.queue_config(parent.config);
        if backing != expected {
            return Err(AdminCatalogError::CorruptCatalog);
        }
        self.admin_owner(namespace, &path, Kind::Subscription)?;
        self.admin_missing(&keys::topic_config(namespace, &path))?;
        let shadow = path
            .dead_letter_queue()
            .map_err(|_| AdminCatalogError::CorruptCatalog)?;
        self.admin_expected(
            &keys::queue_config(namespace, &shadow),
            &expected.dead_letter_shadow(),
        )?;
        self.admin_missing(&keys::topic_config(namespace, &shadow))?;
        Ok(Some(AdminSubscription {
            topic: topic.clone(),
            name: name.clone(),
            path,
            config,
        }))
    }

    /// Reads a bounded namespace-local page without writes or Clock access.
    ///
    /// All pages require the caller to exclude every writer during each complete
    /// call; there is no cross-page snapshot. Ordering is kind-major (queue,
    /// topic, subscription), then canonical storage-key bytes. At most
    /// [`MAX_ADMIN_CATALOG_SCAN`] raw rows plus one lookahead are examined, and
    /// selected lookahead corruption also refuses the whole page. Internal rows
    /// are skipped, not treated as public queues, and can exhaust the scan budget.
    /// A continuation means raw work remains, not that another public item exists.
    pub fn admin_catalog_page(
        &self,
        namespace: &NamespaceName,
        kind: AdminCatalogKind,
        after: Option<&AdminCatalogCursor>,
        limit: usize,
    ) -> Result<AdminCatalogPage, AdminCatalogError> {
        if !(1..=MAX_ADMIN_CATALOG_PAGE).contains(&limit) {
            return Err(AdminCatalogError::InvalidPageSize { requested: limit });
        }
        let position = after
            .map(|after| {
                after.validate_scope(namespace, kind)?;
                parse_cursor(after.as_bytes())
            })
            .transpose()?;
        let mut entries = Vec::with_capacity(limit);
        let mut budget = MAX_ADMIN_CATALOG_SCAN;
        let mut last = None;
        let mut started = position.is_none();
        for &phase in kind.phases() {
            if let Some((_, _, bound_phase, _)) = position
                && !started
            {
                started = phase == bound_phase;
            }
            if !started {
                continue;
            }
            let prefix = match phase {
                Phase::Queue => keys::namespace_queue_config_prefix(namespace),
                Phase::Topic => keys::namespace_topic_config_prefix(namespace),
                Phase::Subscription => keys::namespace_subscription_prefix(namespace),
            };
            let mut start = prefix.clone();
            if let Some((_, _, bound_phase, key)) = position
                && phase == bound_phase
            {
                start = key.to_vec();
                start.push(0); // Smallest byte string strictly greater than the position.
            }
            for (key, _) in self.store().scan_from(&prefix, &start, budget + 1)? {
                let entry = self.admin_catalog_row(namespace, phase, &key)?;
                if budget == 0 || (entries.len() == limit && entry.is_some()) {
                    let last = last.as_deref().ok_or(AdminCatalogError::CorruptCatalog)?;
                    return Ok(AdminCatalogPage {
                        entries,
                        continuation: Some(cursor(kind, last)),
                    });
                }
                budget -= 1;
                last = Some(key);
                if let Some(entry) = entry {
                    entries.push(entry);
                }
            }
        }
        Ok(AdminCatalogPage {
            entries,
            continuation: None,
        })
    }

    fn admin_catalog_row(
        &self,
        namespace: &NamespaceName,
        phase: Phase,
        key: &[u8],
    ) -> Result<Option<AdminCatalogEntry>, AdminCatalogError> {
        let (actual_namespace, actual_phase) = row_identity(key)?;
        if &actual_namespace != namespace || actual_phase != phase {
            return Err(AdminCatalogError::CorruptCatalog);
        }
        match phase {
            Phase::Queue | Phase::Topic => {
                let (_, path) =
                    keys::catalog_entity_parts(key).ok_or(AdminCatalogError::CorruptCatalog)?;
                if phase == Phase::Queue {
                    if internal_queue(&path)? {
                        return Ok(None);
                    }
                    self.admin_queue(namespace, &path)?
                        .map(AdminCatalogEntry::Queue)
                } else {
                    self.admin_topic(namespace, &path)?
                        .map(AdminCatalogEntry::Topic)
                }
            }
            Phase::Subscription => {
                let (_, topic, name) = keys::catalog_subscription_parts(key)
                    .ok_or(AdminCatalogError::CorruptCatalog)?;
                self.admin_subscription(namespace, &topic, &name)?
                    .map(AdminCatalogEntry::Subscription)
            }
        }
        .map(Some)
        .ok_or(AdminCatalogError::CorruptCatalog)
    }

    fn admin_topic_profile(
        &self,
        namespace: &NamespaceName,
        path: &EntityPath,
        raw: &[u8],
    ) -> Result<AdminTopic, AdminCatalogError> {
        let config: TopicConfig = exact(raw)?;
        config
            .validate()
            .map_err(|_| AdminCatalogError::CorruptCatalog)?;
        self.admin_owner(namespace, path, Kind::Topic)?;
        self.admin_missing(&keys::queue_config(namespace, path))?;
        let shadow = path
            .dead_letter_queue()
            .map_err(|_| AdminCatalogError::CorruptCatalog)?;
        self.admin_missing(&keys::queue_config(namespace, &shadow))?;
        self.admin_missing(&keys::topic_config(namespace, &shadow))?;
        Ok(AdminTopic {
            path: path.clone(),
            config,
        })
    }

    fn admin_absent(
        &self,
        namespace: &NamespaceName,
        path: &EntityPath,
    ) -> Result<(), AdminCatalogError> {
        let shadow = path
            .dead_letter_queue()
            .map_err(|_| AdminCatalogError::CorruptCatalog)?;
        for path in [path, &shadow] {
            self.admin_missing(&keys::entity_metadata(namespace, path))?;
            self.admin_missing(&keys::queue_config(namespace, path))?;
            self.admin_missing(&keys::topic_config(namespace, path))?;
        }
        if !self
            .store()
            .scan_prefix(&keys::topic_subscription_prefix(namespace, path), 1)?
            .is_empty()
        {
            return Err(AdminCatalogError::CorruptCatalog);
        }
        Ok(())
    }

    fn admin_missing(&self, key: &[u8]) -> Result<(), AdminCatalogError> {
        if self.store().get(key)?.is_some() {
            return Err(AdminCatalogError::CorruptCatalog);
        }
        Ok(())
    }

    fn admin_expected<T: Serialize>(
        &self,
        key: &[u8],
        expected: &T,
    ) -> Result<(), AdminCatalogError> {
        if self.store().get(key)?
            != Some(codec::encode(expected).map_err(|_| AdminCatalogError::CorruptCatalog)?)
        {
            return Err(AdminCatalogError::CorruptCatalog);
        }
        Ok(())
    }

    fn admin_owner(
        &self,
        namespace: &NamespaceName,
        path: &EntityPath,
        kind: Kind,
    ) -> Result<(), AdminCatalogError> {
        self.require_live_owner(namespace, path, kind)
            .map_err(|error| match error {
                BrokerError::Storage(error) => AdminCatalogError::Storage(error),
                _ => AdminCatalogError::CorruptCatalog,
            })
    }
}
