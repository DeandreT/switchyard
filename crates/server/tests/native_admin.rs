//! The native administration service exercises the same owner on both stores.

use std::{
    error::Error,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use admin_api::v1::{
    CreateEntityRequest, DeleteEntityRequest, EntityKind, GetEntityRequest, ListEntitiesRequest,
    QueueConfiguration, UnlimitedTimeToLive, UpdateEntityRequest,
    entity_service_server::EntityService, queue_configuration::DefaultTimeToLive,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{
    CommandKind, CommandOutcome, EntityPath, NamespaceName, QueueConfig, StateMachine,
    SubscriptionConfig, SubscriptionName, TopicConfig, keys,
};
use hmac::{Hmac, Mac};
use server::{Broker, LocalProposer, ManualClock, NativeAdminService};
use sha2::Sha256;
use storage::{StateStore, StorageError, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;
use tonic::{Code, Request, Status};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type ReadGate = (flume::Sender<()>, flume::Receiver<()>);

const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "native-administration-test-key";
const EXPIRY: u64 = 4_102_444_800;

#[derive(Default)]
struct Observations {
    reads: AtomicUsize,
    writes: AtomicUsize,
    gate: Mutex<Option<ReadGate>>,
}

#[derive(Clone)]
struct ObservedStore<S> {
    inner: S,
    observations: Arc<Observations>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.observations.reads.fetch_add(1, Ordering::SeqCst);
        if key.starts_with(&keys::queue_config_prefix()) {
            let gate = self.observations.gate.lock().expect("read gate").take();
            if let Some((entered, release)) = gate {
                entered.send(()).expect("observe owner read");
                release.recv().expect("release owner read");
            }
        }
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observations.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        assert!(limit <= domain::MAX_QUEUE_PAGE_SIZE + 1);
        self.observations.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }
}

struct Node<P: StoreProvider> {
    broker: Broker,
    service: NativeAdminService,
    store: ObservedStore<P::Store>,
    clock: ManualClock,
    _provider: P,
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
            _provider: provider,
        })
    }

    fn reads(&self) -> usize {
        self.store.observations.reads.load(Ordering::SeqCst)
    }

    fn writes(&self) -> usize {
        self.store.observations.writes.load(Ordering::SeqCst)
    }

    fn assert_unchanged(&self, before: &StoreSnapshot, writes: usize) {
        assert_eq!(&self.store.snapshot().expect("snapshot"), before);
        assert_eq!(self.writes(), writes);
    }
}

fn create(path: &str, config: Option<QueueConfiguration>) -> CreateEntityRequest {
    CreateEntityRequest {
        namespace: "tenant".to_owned(),
        path: path.to_owned(),
        kind: EntityKind::Queue as i32,
        queue_config: config,
        ..CreateEntityRequest::default()
    }
}

fn get(path: &str) -> GetEntityRequest {
    GetEntityRequest {
        namespace: "tenant".to_owned(),
        path: path.to_owned(),
    }
}

fn patch(path: &str, config: QueueConfiguration) -> UpdateEntityRequest {
    UpdateEntityRequest {
        namespace: "tenant".to_owned(),
        path: path.to_owned(),
        queue_config: Some(config),
    }
}

fn list(size: u32, token: String) -> ListEntitiesRequest {
    ListEntitiesRequest {
        namespace: "tenant".to_owned(),
        page_size: size,
        page_token: token,
    }
}

fn assert_code<T: std::fmt::Debug>(result: Result<T, Status>, code: Code) {
    assert_eq!(result.expect_err("expected refusal").code(), code);
}

async fn creation_and_presence_updates<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider)?;
    let created = node
        .service
        .create_entity(Request::new(create("orders", None)))
        .await?
        .into_inner();
    let config = created.queue_config.expect("complete configuration");
    let defaults = QueueConfig::default();
    assert_eq!(
        config.lock_duration_millis,
        Some(defaults.lock_duration_millis)
    );
    assert_eq!(config.max_delivery_count, Some(defaults.max_delivery_count));
    assert_eq!(
        config.max_message_bytes,
        Some(defaults.max_message_bytes as u64)
    );
    assert_eq!(config.requires_session, Some(false));
    assert_eq!(config.requires_duplicate_detection, Some(false));
    assert_eq!(config.dead_lettering_on_message_expiration, Some(false));
    assert_eq!(
        config.duplicate_detection_history_time_window_millis,
        Some(defaults.duplicate_detection_history_time_window_millis)
    );
    assert!(matches!(
        config.default_time_to_live,
        Some(DefaultTimeToLive::DefaultTtlUnlimited(_))
    ));
    assert!(created.placement_group_id.is_empty());
    assert_eq!(created.max_size_bytes, None);
    assert_eq!(created.used_logical_bytes, None);

    node.clock.set(2_000);
    let finite = node
        .service
        .update_entity(Request::new(patch(
            "orders",
            QueueConfiguration {
                default_time_to_live: Some(DefaultTimeToLive::DefaultTtlMillis(12_000)),
                dead_lettering_on_message_expiration: Some(true),
                max_message_bytes: Some(32_768),
                ..QueueConfiguration::default()
            },
        )))
        .await?
        .into_inner()
        .queue_config
        .expect("configuration");
    assert_eq!(
        finite.default_time_to_live,
        Some(DefaultTimeToLive::DefaultTtlMillis(12_000))
    );
    assert_eq!(finite.dead_lettering_on_message_expiration, Some(true));
    let reset = node
        .service
        .update_entity(Request::new(patch(
            "orders",
            QueueConfiguration {
                default_time_to_live: Some(DefaultTimeToLive::DefaultTtlUnlimited(
                    UnlimitedTimeToLive {},
                )),
                dead_lettering_on_message_expiration: Some(false),
                requires_session: Some(false),
                requires_duplicate_detection: Some(false),
                ..QueueConfiguration::default()
            },
        )))
        .await?
        .into_inner()
        .queue_config
        .expect("configuration");
    assert_eq!(reset.dead_lettering_on_message_expiration, Some(false));
    assert_eq!(reset.max_message_bytes, Some(32_768));
    assert!(matches!(
        reset.default_time_to_live.as_ref(),
        Some(DefaultTimeToLive::DefaultTtlUnlimited(_))
    ));

    let before = node.store.snapshot()?;
    let writes = node.writes();
    for invalid in [
        QueueConfiguration {
            lock_duration_millis: Some(0),
            ..QueueConfiguration::default()
        },
        QueueConfiguration {
            max_delivery_count: Some(0),
            ..QueueConfiguration::default()
        },
        QueueConfiguration {
            max_message_bytes: Some(0),
            ..QueueConfiguration::default()
        },
        QueueConfiguration {
            default_time_to_live: Some(DefaultTimeToLive::DefaultTtlMillis(0)),
            ..QueueConfiguration::default()
        },
        QueueConfiguration {
            duplicate_detection_history_time_window_millis: Some(0),
            ..QueueConfiguration::default()
        },
    ] {
        assert_code(
            node.service
                .create_entity(Request::new(create("invalid", Some(invalid))))
                .await,
            Code::InvalidArgument,
        );
        assert_code(
            node.service
                .update_entity(Request::new(patch("orders", invalid)))
                .await,
            Code::InvalidArgument,
        );
        node.assert_unchanged(&before, writes);
    }
    assert_code(
        node.service
            .update_entity(Request::new(patch(
                "orders",
                QueueConfiguration {
                    max_delivery_count: Some(99),
                    requires_session: Some(true),
                    ..QueueConfiguration::default()
                },
            )))
            .await,
        Code::FailedPrecondition,
    );
    node.assert_unchanged(&before, writes);
    assert_code(
        node.service
            .update_entity(Request::new(patch(
                "orders",
                QueueConfiguration {
                    requires_duplicate_detection: Some(true),
                    ..QueueConfiguration::default()
                },
            )))
            .await,
        Code::FailedPrecondition,
    );
    node.assert_unchanged(&before, writes);

    // A pure Get still succeeds after a clock regression that would refuse writes.
    node.clock.set(0);
    assert_eq!(
        node.service
            .get_entity(Request::new(get("orders")))
            .await?
            .into_inner()
            .queue_config,
        Some(reset)
    );
    node.assert_unchanged(&before, writes);
    Ok(())
}

async fn independent_patches_do_not_overwrite_each_other<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    let configured = QueueConfiguration {
        lock_duration_millis: Some(90_000),
        max_delivery_count: Some(18),
        max_message_bytes: Some(64_000),
        default_time_to_live: Some(DefaultTimeToLive::DefaultTtlMillis(5_000)),
        requires_session: Some(true),
        requires_duplicate_detection: Some(true),
        duplicate_detection_history_time_window_millis: Some(30_000),
        dead_lettering_on_message_expiration: Some(true),
    };
    assert_eq!(
        node.service
            .create_entity(Request::new(create("orders", Some(configured))))
            .await?
            .into_inner()
            .queue_config,
        Some(configured)
    );
    let clone = node.service.clone();
    let (first, second) = tokio::join!(
        node.service.update_entity(Request::new(patch(
            "orders",
            QueueConfiguration {
                lock_duration_millis: Some(120_000),
                ..QueueConfiguration::default()
            }
        ))),
        clone.update_entity(Request::new(patch(
            "orders",
            QueueConfiguration {
                max_delivery_count: Some(24),
                ..QueueConfiguration::default()
            }
        )))
    );
    first?;
    second?;
    let expected = QueueConfiguration {
        lock_duration_millis: Some(120_000),
        max_delivery_count: Some(24),
        ..configured
    };
    assert_eq!(
        node.service
            .get_entity(Request::new(get("orders")))
            .await?
            .into_inner()
            .queue_config,
        Some(expected)
    );
    Ok(())
}

async fn legacy_configuration_and_errors<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider)?;
    let mut legacy = create("legacy", None);
    legacy.default_ttl_millis = 8_000;
    legacy.lock_duration_millis = 120_000;
    legacy.max_delivery_count = 23;
    legacy.requires_session = true;
    let config = node
        .service
        .create_entity(Request::new(legacy))
        .await?
        .into_inner()
        .queue_config
        .expect("configuration");
    assert_eq!(
        config.default_time_to_live,
        Some(DefaultTimeToLive::DefaultTtlMillis(8_000))
    );
    assert_eq!(config.lock_duration_millis, Some(120_000));
    assert_eq!(config.max_delivery_count, Some(23));
    assert_eq!(config.requires_session, Some(true));

    let before = node.store.snapshot()?;
    let writes = node.writes();
    assert_code(
        node.service
            .create_entity(Request::new(create("legacy", None)))
            .await,
        Code::AlreadyExists,
    );
    assert_code(
        node.service.get_entity(Request::new(get("missing"))).await,
        Code::NotFound,
    );
    assert_code(
        node.service
            .update_entity(Request::new(patch(
                "missing",
                QueueConfiguration::default(),
            )))
            .await,
        Code::NotFound,
    );
    assert_code(
        node.service
            .update_entity(Request::new(UpdateEntityRequest {
                namespace: "tenant".to_owned(),
                path: "legacy".to_owned(),
                queue_config: None,
            }))
            .await,
        Code::InvalidArgument,
    );
    let mut mixed = create("mixed", Some(QueueConfiguration::default()));
    mixed.lock_duration_millis = 120_000;
    assert_code(
        node.service.create_entity(Request::new(mixed)).await,
        Code::InvalidArgument,
    );
    for kind in [EntityKind::Topic, EntityKind::Subscription] {
        let mut unsupported = create("unsupported", None);
        unsupported.kind = kind as i32;
        assert_code(
            node.service.create_entity(Request::new(unsupported)).await,
            Code::Unimplemented,
        );
    }
    for field in ["capacity", "placement"] {
        let mut unsupported = create("unsupported", None);
        if field == "capacity" {
            unsupported.max_size_bytes = 1;
        } else {
            unsupported.placement_group_id = "pg".to_owned();
        }
        assert_code(
            node.service.create_entity(Request::new(unsupported)).await,
            Code::Unimplemented,
        );
    }
    assert_code(
        node.service
            .delete_entity(Request::new(DeleteEntityRequest {
                namespace: "tenant".to_owned(),
                path: "legacy".to_owned(),
            }))
            .await,
        Code::Unimplemented,
    );
    for path in ["legacy/$deadletterqueue", "legacy/$DeadLetterQueue"] {
        assert_code(
            node.service.get_entity(Request::new(get(path))).await,
            Code::InvalidArgument,
        );
        assert_code(
            node.service
                .create_entity(Request::new(create(path, None)))
                .await,
            Code::InvalidArgument,
        );
        assert_code(
            node.service
                .update_entity(Request::new(patch(path, QueueConfiguration::default())))
                .await,
            Code::InvalidArgument,
        );
        assert_code(
            node.service
                .delete_entity(Request::new(DeleteEntityRequest {
                    namespace: "tenant".to_owned(),
                    path: path.to_owned(),
                }))
                .await,
            Code::InvalidArgument,
        );
    }
    node.assert_unchanged(&before, writes);
    let reads = node.reads();
    let mut foreign = get("legacy");
    foreign.namespace = "other".to_owned();
    assert_code(
        node.service.get_entity(Request::new(foreign)).await,
        Code::PermissionDenied,
    );
    let mut foreign = create("not-created", None);
    foreign.namespace = "other".to_owned();
    assert_code(
        node.service.create_entity(Request::new(foreign)).await,
        Code::PermissionDenied,
    );
    assert_eq!(node.reads(), reads);
    node.assert_unchanged(&before, writes);
    Ok(())
}

async fn ordered_bounded_pagination<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider)?;
    let expected: Vec<_> = (0..105).map(|n| format!("q{n:03}")).collect();
    for path in expected.iter().rev() {
        node.service
            .create_entity(Request::new(create(path, None)))
            .await?;
    }
    node.broker.handle().submit_blocking(
        NamespaceName::new("neighbor")?,
        EntityPath::new("hidden")?,
        domain::CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let before = node.store.snapshot()?;
    let writes = node.writes();
    node.clock.set(0);
    let first = node
        .service
        .list_entities(Request::new(list(0, String::new())))
        .await?
        .into_inner();
    assert_eq!(first.entities.len(), 100);
    assert!(first.next_page_token.starts_with("v1."));
    assert!(first.next_page_token.len() <= 512);
    assert_eq!(
        first
            .entities
            .iter()
            .map(|entry| entry.path.clone())
            .collect::<Vec<_>>(),
        expected[..100]
    );
    assert!(
        first
            .entities
            .iter()
            .all(|entry| entry.namespace == "tenant" && entry.queue_config.is_some())
    );
    let second = node
        .service
        .list_entities(Request::new(list(0, first.next_page_token.clone())))
        .await?
        .into_inner();
    assert_eq!(
        second
            .entities
            .iter()
            .map(|entry| entry.path.clone())
            .collect::<Vec<_>>(),
        expected[100..]
    );
    assert!(second.next_page_token.is_empty());
    let mut collected = Vec::new();
    let mut token = String::new();
    loop {
        let page = node
            .service
            .list_entities(Request::new(list(7, token)))
            .await?
            .into_inner();
        assert!(page.entities.len() <= 7);
        collected.extend(page.entities.into_iter().map(|entry| entry.path));
        token = page.next_page_token;
        if token.is_empty() {
            break;
        }
    }
    assert_eq!(collected, expected);
    assert_eq!(
        node.service
            .list_entities(Request::new(list(1024, String::new())))
            .await?
            .into_inner()
            .entities
            .len(),
        105
    );
    assert_code(
        node.service
            .list_entities(Request::new(list(1025, String::new())))
            .await,
        Code::InvalidArgument,
    );
    for token in ["v2.AA".to_owned(), "v1.!".to_owned(), "x".repeat(513)] {
        assert_code(
            node.service
                .list_entities(Request::new(list(7, token)))
                .await,
            Code::InvalidArgument,
        );
    }
    let foreign = NativeAdminService::new(node.broker.handle(), NamespaceName::new("neighbor")?);
    assert_code(
        foreign
            .list_entities(Request::new(ListEntitiesRequest {
                namespace: "neighbor".to_owned(),
                page_token: first.next_page_token,
                page_size: 7,
            }))
            .await,
        Code::InvalidArgument,
    );
    node.assert_unchanged(&before, writes);
    Ok(())
}

fn policy() -> TestResult<SharedAccessPolicy> {
    Ok(SharedAccessPolicy::new([
        SharedAccessRule::new(
            "manage",
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::MANAGE,
        )?,
        SharedAccessRule::new(
            "send",
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::SEND,
        )?,
    ])?)
}

fn token(audience: &str, rule: &str, expiry: u64) -> String {
    let resource: String = url::form_urlencoded::byte_serialize(audience.as_bytes()).collect();
    let mut hmac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).expect("test key");
    hmac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(hmac.finalize().into_bytes());
    let signature: String = url::form_urlencoded::byte_serialize(signature.as_bytes()).collect();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={rule}")
}

fn authorized<T>(body: T, token: &str) -> Request<T> {
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("authorization", token.parse().expect("ASCII token"));
    request
}

async fn sas_authentication_and_scope<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider)?;
    assert!(matches!(
        node.service
            .clone()
            .with_shared_access_policy(policy()?, format!("{HOST}/orders")),
        Err(auth::ResourceScopeError::InvalidUri)
    ));
    node.service
        .create_entity(Request::new(create("orders", None)))
        .await?;
    let service = node
        .service
        .clone()
        .with_shared_access_policy(policy()?, HOST)?;
    let namespace_token = token(&format!("amqps://{HOST}"), "manage", EXPIRY);
    let entity_token = token(&format!("amqps://{HOST}/orders"), "manage", EXPIRY);
    let reads = node.reads();
    let before = node.store.snapshot()?;
    let writes = node.writes();
    assert_code(
        service.get_entity(Request::new(get("orders"))).await,
        Code::Unauthenticated,
    );
    for invalid in [
        "not-a-token".to_owned(),
        token(&format!("amqps://{HOST}"), "manage", 1),
        namespace_token.replace("sig=", "sig=bad"),
    ] {
        assert_code(
            service
                .get_entity(authorized(get("orders"), &invalid))
                .await,
            Code::Unauthenticated,
        );
    }
    assert_code(
        service
            .get_entity(authorized(
                get("orders"),
                &token(&format!("amqps://{HOST}"), "send", EXPIRY),
            ))
            .await,
        Code::PermissionDenied,
    );
    assert_code(
        service
            .get_entity(authorized(get("other"), &entity_token))
            .await,
        Code::PermissionDenied,
    );
    assert_code(
        service
            .list_entities(authorized(list(0, String::new()), &entity_token))
            .await,
        Code::PermissionDenied,
    );
    let mut foreign = get("orders");
    foreign.namespace = "other".to_owned();
    assert_code(
        service
            .get_entity(authorized(foreign, &namespace_token))
            .await,
        Code::PermissionDenied,
    );
    assert_code(
        service
            .delete_entity(Request::new(DeleteEntityRequest {
                namespace: "tenant".to_owned(),
                path: "orders".to_owned(),
            }))
            .await,
        Code::Unauthenticated,
    );
    let mut unsupported = create("topic", None);
    unsupported.kind = EntityKind::Topic as i32;
    assert_code(
        service.create_entity(Request::new(unsupported)).await,
        Code::Unauthenticated,
    );
    assert_eq!(node.reads(), reads);
    node.assert_unchanged(&before, writes);

    assert_eq!(
        service
            .get_entity(authorized(get("orders"), &entity_token))
            .await?
            .into_inner()
            .path,
        "orders"
    );
    assert_eq!(
        service
            .list_entities(authorized(list(0, String::new()), &namespace_token))
            .await?
            .into_inner()
            .entities
            .len(),
        1
    );
    service
        .create_entity(authorized(create("created", None), &namespace_token))
        .await?;
    service
        .update_entity(authorized(
            patch(
                "created",
                QueueConfiguration {
                    dead_lettering_on_message_expiration: Some(true),
                    ..QueueConfiguration::default()
                },
            ),
            &namespace_token,
        ))
        .await?;
    assert_code(
        service
            .delete_entity(authorized(
                DeleteEntityRequest {
                    namespace: "tenant".to_owned(),
                    path: "created".to_owned(),
                },
                &namespace_token,
            ))
            .await,
        Code::Unimplemented,
    );
    Ok(())
}

async fn subscription_backing_queues_are_not_administrable<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    for path in ["alpha", "omega"] {
        node.service
            .create_entity(Request::new(create(path, None)))
            .await?;
    }
    let namespace = NamespaceName::new("tenant")?;
    let topic = EntityPath::new("m-events")?;
    let handle = node.broker.handle();
    assert_eq!(
        handle
            .submit(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
            )
            .await?,
        CommandOutcome::TopicCreated
    );
    for name in ["accounting", "audit"] {
        assert_eq!(
            handle
                .submit(
                    namespace.clone(),
                    topic.clone(),
                    CommandKind::CreateSubscription {
                        name: SubscriptionName::new(name)?,
                        config: SubscriptionConfig::default(),
                    },
                )
                .await?,
            CommandOutcome::SubscriptionCreated
        );
    }
    let before = node.store.snapshot()?;
    let writes = node.writes();
    let mut token = String::new();
    let mut listed = Vec::new();
    loop {
        let page = node
            .service
            .list_entities(Request::new(list(1, token)))
            .await?
            .into_inner();
        assert_eq!(page.entities.len(), 1);
        assert_eq!(page.entities[0].kind, EntityKind::Queue as i32);
        listed.push(page.entities[0].path.clone());
        token = page.next_page_token;
        if token.is_empty() {
            break;
        }
    }
    assert_eq!(listed, ["alpha", "omega"]);
    for path in [
        "m-events/subscriptions/accounting",
        "m-events/Subscriptions/audit",
        "m-events/subscriptions/accounting/$deadletterqueue",
    ] {
        let reads = node.reads();
        assert_code(
            node.service.get_entity(Request::new(get(path))).await,
            Code::InvalidArgument,
        );
        assert_code(
            node.service
                .create_entity(Request::new(create(path, None)))
                .await,
            Code::InvalidArgument,
        );
        assert_code(
            node.service
                .update_entity(Request::new(patch(path, QueueConfiguration::default())))
                .await,
            Code::InvalidArgument,
        );
        assert_eq!(node.reads(), reads, "reserved paths never reach the owner");
    }
    assert_code(
        node.service
            .create_entity(Request::new(create("m-events", None)))
            .await,
        Code::AlreadyExists,
    );
    node.assert_unchanged(&before, writes);
    Ok(())
}

async fn stopped_owner<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider)?;
    node.service
        .create_entity(Request::new(create("orders", None)))
        .await?;
    let Node {
        broker,
        service,
        store,
        clock: _,
        _provider: provider,
    } = node;
    let before = store.snapshot()?;
    let reads = store.observations.reads.load(Ordering::SeqCst);
    let writes = store.observations.writes.load(Ordering::SeqCst);
    drop(broker);
    assert_code(
        service.get_entity(Request::new(get("orders"))).await,
        Code::Unavailable,
    );
    assert_code(
        service
            .list_entities(Request::new(list(0, String::new())))
            .await,
        Code::Unavailable,
    );
    assert_code(
        service
            .create_entity(Request::new(create("new", None)))
            .await,
        Code::Unavailable,
    );
    assert_code(
        service
            .update_entity(Request::new(patch("orders", QueueConfiguration::default())))
            .await,
        Code::Unavailable,
    );
    assert_eq!(store.snapshot()?, before);
    assert_eq!(store.observations.reads.load(Ordering::SeqCst), reads);
    assert_eq!(store.observations.writes.load(Ordering::SeqCst), writes);
    drop(store);
    drop(provider);
    Ok(())
}

async fn shared_admission_is_nonblocking<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider)?;
    node.service
        .create_entity(Request::new(create("orders", None)))
        .await?;
    let (entered, waiting) = flume::bounded(1);
    let (release, blocked) = flume::bounded(1);
    *node.store.observations.gate.lock().expect("read gate") = Some((entered, blocked));
    let clone = node.service.clone();
    let mut queries = Vec::new();
    for _ in 0..128 {
        let mut query = Box::pin(node.service.get_entity(Request::new(get("orders"))));
        std::future::poll_fn(|context| {
            assert!(query.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        queries.push(query);
    }
    tokio::time::timeout(Duration::from_secs(5), waiting.recv_async()).await??;
    assert_code(
        clone.get_entity(Request::new(get("orders"))).await,
        Code::ResourceExhausted,
    );
    // Cancellation releases service admission even while its owner request remains queued.
    drop(queries.pop());
    let mut replacement = Box::pin(clone.get_entity(Request::new(get("orders"))));
    std::future::poll_fn(|context| {
        assert!(replacement.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_code(
        node.service.get_entity(Request::new(get("orders"))).await,
        Code::ResourceExhausted,
    );
    release.send(())?;
    for query in queries {
        tokio::time::timeout(Duration::from_secs(5), query).await??;
    }
    tokio::time::timeout(Duration::from_secs(5), replacement).await??;
    clone.get_entity(Request::new(get("orders"))).await?;
    Ok(())
}

macro_rules! suite {
    ($module:ident,$provider:expr) => {
        mod $module {
            use super::*;
            #[tokio::test]
            async fn defaults_presence_and_atomic_updates() -> TestResult {
                creation_and_presence_updates($provider).await
            }
            #[tokio::test]
            async fn independent_atomic_patches_preserve_other_settings() -> TestResult {
                independent_patches_do_not_overwrite_each_other($provider).await
            }

            #[tokio::test]
            async fn legacy_errors_and_reserved_paths() -> TestResult {
                legacy_configuration_and_errors($provider).await
            }
            #[tokio::test]
            async fn ordered_parent_only_pagination() -> TestResult {
                ordered_bounded_pagination($provider).await
            }
            #[tokio::test]
            async fn typed_subscription_queues_remain_hidden_and_reserved() -> TestResult {
                subscription_backing_queues_are_not_administrable($provider).await
            }
            #[tokio::test]
            async fn per_request_sas_authentication_and_scoping() -> TestResult {
                sas_authentication_and_scope($provider).await
            }
            #[tokio::test]
            async fn unavailable_owner_has_no_side_effects() -> TestResult {
                stopped_owner($provider).await
            }
            #[tokio::test]
            async fn clones_share_nonblocking_admission() -> TestResult {
                shared_admission_is_nonblocking($provider).await
            }
        }
    };
}

suite!(memory, testkit::MemoryProvider::new());
suite!(durable, testkit::DurableProvider::temporary()?);
