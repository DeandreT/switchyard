//! Typed topology administration is bounded and uses committed owner reads.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use admin_api::v1::{
    CreateEntityRequest, Entity, EntityKind, GetEntityRequest, ListEntitiesRequest,
    QueueConfiguration, SubscriptionConfiguration, TopicConfiguration, UnlimitedTimeToLive,
    UpdateEntityRequest, entity_service_server::EntityService, subscription_configuration,
    topic_configuration,
};
use domain::{
    EntityPath, NamespaceName, StateMachine, SubscriptionConfig, SubscriptionName, TopicConfig,
    codec, keys,
};
use server::{Broker, LocalProposer, ManualClock, NativeAdminService};
use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;
use tonic::{Code, Request};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Observations {
    reads: AtomicUsize,
    writes: AtomicUsize,
    fail_next: AtomicBool,
    puts: Mutex<Vec<Key>>,
    scans: Mutex<Vec<(Key, usize)>>,
}

#[derive(Clone)]
struct ObservedStore<S> {
    inner: S,
    observations: Arc<Observations>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.observations.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        assert!(limit <= domain::MAX_TOPIC_PAGE_SIZE + 1);
        self.observations.reads.fetch_add(1, Ordering::SeqCst);
        self.observations
            .scans
            .lock()
            .expect("scans")
            .push((prefix.to_vec(), limit));
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observations.writes.fetch_add(1, Ordering::SeqCst);
        self.observations
            .puts
            .lock()
            .expect("puts")
            .extend(
                batch
                    .mutations()
                    .iter()
                    .filter_map(|mutation| match mutation {
                        Mutation::Put { key, .. } => Some(key.clone()),
                        _ => None,
                    }),
            );
        if self.observations.fail_next.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected native topology commit failure".into(),
            });
        }
        self.inner.apply(batch)
    }
}

struct Node<P: StoreProvider> {
    broker: Broker,
    service: NativeAdminService,
    store: ObservedStore<P::Store>,
    clock: ManualClock,
    provider: P,
}

impl<P: StoreProvider> Node<P> {
    fn start(provider: P) -> TestResult<Self> {
        let store = ObservedStore {
            inner: provider.open()?,
            observations: Arc::new(Observations::default()),
        };
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let service = NativeAdminService::new(broker.handle(), NamespaceName::new("tenant")?);
        Ok(Self {
            broker,
            service,
            store,
            clock,
            provider,
        })
    }

    fn reopen(self) -> TestResult<Self> {
        let Self {
            broker,
            service,
            store,
            clock: _,
            provider,
        } = self;
        drop(service);
        drop(broker);
        drop(store);
        Self::start(provider)
    }

    fn reads(&self) -> usize {
        self.store.observations.reads.load(Ordering::SeqCst)
    }
    fn writes(&self) -> usize {
        self.store.observations.writes.load(Ordering::SeqCst)
    }
    fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.store.snapshot()?)
    }
    fn unchanged(&self, snapshot: &StoreSnapshot, writes: usize) -> TestResult {
        assert_eq!(&self.snapshot()?, snapshot);
        assert_eq!(self.writes(), writes);
        Ok(())
    }

    async fn create(&self, request: CreateEntityRequest) -> Result<Entity, tonic::Status> {
        Ok(
            tokio::time::timeout(DEADLINE, self.service.create_entity(Request::new(request)))
                .await
                .expect("bounded create")?
                .into_inner(),
        )
    }

    async fn get(&self, path: &str) -> Result<Entity, tonic::Status> {
        Ok(tokio::time::timeout(
            DEADLINE,
            self.service.get_entity(Request::new(GetEntityRequest {
                namespace: "tenant".into(),
                path: path.into(),
            })),
        )
        .await
        .expect("bounded get")?
        .into_inner())
    }
}

fn topic(path: &str, config: Option<TopicConfiguration>) -> CreateEntityRequest {
    CreateEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        kind: EntityKind::Topic as i32,
        topic_config: config,
        ..CreateEntityRequest::default()
    }
}

fn subscription(
    parent: &str,
    name: &str,
    config: Option<SubscriptionConfiguration>,
) -> TestResult<CreateEntityRequest> {
    Ok(CreateEntityRequest {
        namespace: "tenant".into(),
        path: EntityPath::new(parent)?
            .subscription(&SubscriptionName::new(name)?)?
            .as_str()
            .into(),
        kind: EntityKind::Subscription as i32,
        subscription_config: config,
        ..CreateEntityRequest::default()
    })
}

fn queue(path: &str) -> CreateEntityRequest {
    CreateEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        kind: EntityKind::Queue as i32,
        ..CreateEntityRequest::default()
    }
}

fn list(kind: EntityKind, parent: &str, size: u32, token: &str) -> ListEntitiesRequest {
    ListEntitiesRequest {
        namespace: "tenant".into(),
        kind: kind as i32,
        parent_topic: parent.into(),
        page_size: size,
        page_token: token.into(),
    }
}

fn code<T: std::fmt::Debug>(result: Result<T, tonic::Status>, expected: Code) {
    assert_eq!(result.expect_err("expected refusal").code(), expected);
}

fn kind(entity: &Entity, expected: EntityKind) {
    assert_eq!(entity.kind, expected as i32);
    assert_eq!(entity.queue_config.is_some(), expected == EntityKind::Queue);
    assert_eq!(entity.topic_config.is_some(), expected == EntityKind::Topic);
    assert_eq!(
        entity.subscription_config.is_some(),
        expected == EntityKind::Subscription
    );
    assert!(entity.placement_group_id.is_empty());
    assert_eq!(entity.max_size_bytes, None);
    assert_eq!(entity.used_logical_bytes, None);
}

async fn complete_typed_creation_and_reads_survive_reopen_without_clock_stamps<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    let defaults = node.create(topic("default", None)).await?;
    kind(&defaults, EntityKind::Topic);
    let config = defaults.topic_config.as_ref().expect("topic config");
    assert_eq!(
        config.max_message_bytes,
        Some(TopicConfig::default().max_message_bytes as u64)
    );
    assert_eq!(config.requires_duplicate_detection, Some(false));
    assert_eq!(
        config.duplicate_detection_history_time_window_millis,
        Some(TopicConfig::default().duplicate_detection_history_time_window_millis)
    );
    assert!(matches!(
        config.default_time_to_live,
        Some(topic_configuration::DefaultTimeToLive::DefaultTtlUnlimited(
            _
        ))
    ));
    let custom = node
        .create(topic(
            "Orders",
            Some(TopicConfiguration {
                default_time_to_live: Some(
                    topic_configuration::DefaultTimeToLive::DefaultTtlMillis(20_000),
                ),
                max_message_bytes: Some(32_768),
                requires_duplicate_detection: Some(true),
                duplicate_detection_history_time_window_millis: Some(60_000),
            }),
        ))
        .await?;
    kind(&custom, EntityKind::Topic);
    let mut request = subscription(
        "Orders",
        "Alpha",
        Some(SubscriptionConfiguration {
            lock_duration_millis: Some(30_000),
            max_delivery_count: Some(7),
            default_time_to_live: Some(
                subscription_configuration::DefaultTimeToLive::DefaultTtlUnlimited(
                    UnlimitedTimeToLive {},
                ),
            ),
            max_message_bytes: Some(8_192),
            requires_session: Some(true),
            dead_lettering_on_message_expiration: Some(true),
        }),
    )?;
    request.path = "Orders/SUBSCRIPTIONS/Alpha".into();
    let child = node.create(request).await?;
    kind(&child, EntityKind::Subscription);
    assert_eq!(child.path, "Orders/subscriptions/Alpha");
    let config = child
        .subscription_config
        .as_ref()
        .expect("subscription config");
    assert_eq!(config.lock_duration_millis, Some(30_000));
    assert_eq!(config.max_delivery_count, Some(7));
    assert_eq!(config.requires_session, Some(true));
    assert_eq!(config.dead_lettering_on_message_expiration, Some(true));
    assert_eq!(config.max_message_bytes, Some(8_192));
    assert!(matches!(
        config.default_time_to_live,
        Some(subscription_configuration::DefaultTimeToLive::DefaultTtlUnlimited(_))
    ));
    let parent = EntityPath::new("Orders")?;
    let child_path = parent.subscription(&SubscriptionName::new("Alpha")?)?;
    assert_eq!(
        node.store
            .get(&keys::queue_config(&NamespaceName::new("tenant")?, &parent))?,
        None
    );
    assert_eq!(
        node.store.get(&keys::queue_config(
            &NamespaceName::new("tenant")?,
            &parent.dead_letter_queue()?
        ))?,
        None
    );
    let backing: domain::QueueConfig = codec::decode(
        &node
            .store
            .get(&keys::queue_config(
                &NamespaceName::new("tenant")?,
                &child_path,
            ))?
            .expect("backing"),
    )?;
    assert!(backing.requires_session);
    assert!(!backing.requires_duplicate_detection);
    let before = node.snapshot()?;
    let writes = node.writes();
    let applied = node.broker.handle().last_applied_blocking()?;
    node.clock.set(0);
    assert_eq!(node.get("Orders").await?, custom);
    assert_eq!(node.get("Orders/SuBsCrIpTiOnS/Alpha").await?, child);
    node.unchanged(&before, writes)?;
    assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
    let node = node.reopen()?;
    assert_eq!(node.snapshot()?, before);
    node.clock.set(0);
    assert_eq!(node.get("Orders").await?, custom);
    assert_eq!(node.get(&child.path).await?, child);
    assert_eq!(node.snapshot()?, before);
    Ok(())
}

async fn native_primary_paths_and_final_subscription_markers_preserve_literal_bytes<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    for parent in ["/Orders/$Management", "a/Subscriptions", "Orders"] {
        let created = node.create(topic(parent, None)).await?;
        assert_eq!(created.path, parent);
        assert_eq!(node.get(parent).await?, created);
        let created = node
            .create(subscription(parent, "Subscriptions", None)?)
            .await?;
        let mixed = format!("{parent}/SuBsCrIpTiOnS/Subscriptions");
        assert_eq!(node.get(&mixed).await?, created);
        assert_eq!(
            created.path,
            format!("{parent}/subscriptions/Subscriptions")
        );
    }
    node.create(topic("orders", None)).await?;
    let upper = node.create(subscription("Orders", "Alpha", None)?).await?;
    let lower = node.create(subscription("Orders", "alpha", None)?).await?;
    assert_ne!(upper.path, lower.path);
    assert_eq!(node.get(&upper.path).await?, upper);
    assert_eq!(node.get(&lower.path).await?, lower);
    code(node.get("orders/subscriptions/Alpha").await, Code::NotFound);
    let primary = node.create(queue("/queue/$Management")).await?;
    kind(&primary, EntityKind::Queue);
    assert_eq!(node.get("/queue/$Management").await?, primary);
    Ok(())
}

#[path = "native_topic_admin/failures.rs"]
mod failures;
#[path = "native_topic_admin/paging.rs"]
mod paging;

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 12, super::$case(::testkit::MemoryProvider::new())).await? })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 12, super::$case(::testkit::DurableProvider::temporary()?)).await? })+ }
    };
}

for_each_backend! {
    complete_typed_creation_and_reads_survive_reopen_without_clock_stamps,
    native_primary_paths_and_final_subscription_markers_preserve_literal_bytes,
}
