//! Real owner coverage on both stores. Raw fixture edits happen only while the
//! owner is stopped; normal service work has exactly one writer.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context, Poll, Waker},
    thread::{self, ThreadId},
    time::Duration,
};

use admin_api::{
    CreateEntityRequest, EntityAddress, EntityBackend, EntityKind, EntityOptions, EntityProfile,
    EntityService, EntityTarget, GetEntityRequest, ListEntitiesRequest, ServiceError,
    UnsupportedFeature,
};
use domain::{
    AdminCatalogCursor, AdminCatalogEntry, AdminCatalogError, AdminCatalogKind, AdminCatalogPage,
    BrokerError, CommandKind, CommandOutcome, EntityPath, MAX_ADMIN_CATALOG_PAGE,
    MAX_ADMIN_CATALOG_SCAN, NamespaceName, QueueConfig, QueueConfigError, StateMachine,
    SubscriptionConfig, SubscriptionConfigError, SubscriptionName, Timestamp, TopicConfig,
    TopicConfigError, codec, keys,
};
use server::{
    AdminError, AdminService, Broker, Clock, LocalProposer, ManualClock, ProposeError, SubmitError,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

fn ns(value: &str) -> NamespaceName {
    NamespaceName::new(value).unwrap()
}
fn path(value: &str) -> EntityPath {
    EntityPath::new(value).unwrap()
}
fn address(value: &str) -> EntityAddress {
    EntityAddress::Entity { path: value.into() }
}
fn subscription(topic: &str, name: &str) -> EntityAddress {
    EntityAddress::Subscription {
        topic: topic.into(),
        name: name.into(),
    }
}
fn create(address: EntityAddress, kind: EntityKind, options: EntityOptions) -> CreateEntityRequest {
    CreateEntityRequest {
        namespace: "TENANT".into(),
        address,
        kind,
        options,
    }
}
fn get(address: EntityAddress) -> GetEntityRequest {
    GetEntityRequest {
        namespace: "tenant".into(),
        address,
    }
}
fn list(
    kind: AdminCatalogKind,
    continuation: Option<Vec<u8>>,
    limit: usize,
) -> ListEntitiesRequest {
    ListEntitiesRequest {
        namespace: "tenant".into(),
        kind,
        continuation,
        limit,
    }
}
fn refused(error: BrokerError) -> AdminError {
    ServiceError::Backend(SubmitError::Propose(ProposeError::Broker(error)))
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Operation {
    Get(Vec<u8>),
    Scan(Vec<u8>, usize),
    Apply,
    Snapshot,
}
#[derive(Clone, Debug)]
struct Event {
    operation: Operation,
    thread: ThreadId,
}
struct Gate {
    key: Vec<u8>,
    entered: mpsc::Sender<()>,
    resume: mpsc::Receiver<()>,
}
#[derive(Default)]
struct Control {
    events: Mutex<Vec<Event>>,
    gate: Mutex<Option<Gate>>,
    fail_get: Mutex<Option<Vec<u8>>>,
    fail_apply: AtomicBool,
}

impl Control {
    fn record(&self, operation: Operation) {
        self.events.lock().unwrap().push(Event {
            operation,
            thread: thread::current().id(),
        });
    }
    fn clear(&self) {
        self.events.lock().unwrap().clear();
    }
    fn arm(&self, key: Vec<u8>) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (entered, waiting) = mpsc::channel();
        let (resume, paused) = mpsc::channel();
        *self.gate.lock().unwrap() = Some(Gate {
            key,
            entered,
            resume: paused,
        });
        (waiting, resume)
    }
    fn pause(&self, key: &[u8]) {
        let gate = {
            let mut gate = self.gate.lock().unwrap();
            if gate.as_ref().is_some_and(|gate| gate.key == key) {
                gate.take()
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.entered.send(()).unwrap();
            gate.resume
                .recv_timeout(Duration::from_secs(30))
                .expect("test releases owner gate");
        }
    }
}

#[derive(Clone)]
struct Observed<S> {
    store: S,
    control: Arc<Control>,
}
impl<S: StateStore> StateStore for Observed<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.control.record(Operation::Get(key.to_vec()));
        self.control.pause(key);
        if self.control.fail_get.lock().unwrap().as_deref() == Some(key) {
            return Err(StorageError::LockPoisoned);
        }
        self.store.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.control.record(Operation::Apply);
        if self.control.fail_apply.load(Ordering::SeqCst) {
            return Err(StorageError::LockPoisoned);
        }
        self.store.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.control.record(Operation::Snapshot);
        self.store.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.control.record(Operation::Scan(prefix.to_vec(), limit));
        self.store.scan_from(prefix, start, limit)
    }
}

#[derive(Clone)]
struct CountingClock {
    clock: ManualClock,
    calls: Arc<AtomicUsize>,
}
impl CountingClock {
    fn new() -> Self {
        Self {
            clock: ManualClock::at(10_000),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}
impl Clock for CountingClock {
    fn now(&self) -> Timestamp {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.clock.now()
    }
}
fn owner<S: StateStore>(store: S, clock: CountingClock) -> (Broker, AdminService) {
    let broker = Broker::spawn(LocalProposer::new(StateMachine::new(store), clock));
    let service = AdminService::new(ns("tenant"), broker.handle());
    (broker, service)
}

// One poll establishes exact admission without sleeping, scheduler timing or a
// helper task that might be aborted before it has ever polled the service call.
fn poll_pending<F: Future>(future: Pin<&mut F>) {
    let waker = Waker::noop();
    assert!(matches!(
        future.poll(&mut Context::from_waker(waker)),
        Poll::Pending
    ));
}

async fn profiles_and_errors<P: StoreProvider>(provider: P) {
    let raw = provider.open().unwrap();
    let control = Arc::new(Control::default());
    let store = Observed {
        store: raw.clone(),
        control: control.clone(),
    };
    let clock = CountingClock::new();
    let (broker, service) = owner(store.clone(), clock.clone());

    for (name, kind, expected) in [
        (
            "queue",
            EntityKind::Queue,
            EntityProfile::Queue(QueueConfig::default()),
        ),
        (
            "topic",
            EntityKind::Topic,
            EntityProfile::Topic(TopicConfig::default()),
        ),
    ] {
        let entity = service
            .create(create(address(name), kind, EntityOptions::default()))
            .await
            .unwrap();
        assert_eq!(entity.profile, expected);
        assert_eq!(entity.namespace, ns("tenant"));
        assert_eq!(entity.path, path(name));
        assert_eq!(entity.placement_group_id, None);
        assert_eq!(entity.max_size_bytes, None);
        assert_eq!(entity.used_logical_bytes, None);
        assert_eq!(service.get(get(address(name))).await.unwrap(), entity);
    }
    let child = subscription("topic", "child");
    let entity = service
        .create(create(
            child.clone(),
            EntityKind::Subscription,
            EntityOptions::default(),
        ))
        .await
        .unwrap();
    assert_eq!(
        entity.profile,
        EntityProfile::Subscription(SubscriptionConfig::default())
    );
    assert_eq!(service.get(get(child)).await.unwrap(), entity);
    assert_eq!(
        service
            .get(get(subscription("TOPIC", "CHILD")))
            .await
            .unwrap(),
        entity
    );
    assert_eq!(
        service.get(get(address("QUEUE"))).await.unwrap().path,
        path("queue")
    );

    let custom = EntityOptions {
        default_time_to_live_millis: Some(1234),
        lock_duration_millis: Some(4567),
        max_delivery_count: Some(3),
        max_message_bytes: Some(19),
        requires_session: Some(true),
        requires_duplicate_detection: Some(true),
        duplicate_detection_history_millis: Some(0),
        ..EntityOptions::default()
    };
    let configured = service
        .create(create(address("custom"), EntityKind::Queue, custom))
        .await
        .unwrap();
    assert_eq!(
        configured.profile,
        EntityProfile::Queue(QueueConfig {
            default_time_to_live_millis: Some(1234),
            lock_duration_millis: 4567,
            max_delivery_count: 3,
            max_message_bytes: 19,
            requires_session: true,
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        })
    );
    let topic = service
        .create(create(
            address("limited"),
            EntityKind::Topic,
            EntityOptions {
                default_time_to_live_millis: Some(600),
                max_message_bytes: Some(5),
                ..EntityOptions::default()
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        topic.profile,
        EntityProfile::Topic(TopicConfig {
            default_time_to_live_millis: Some(600),
            max_message_bytes: 5
        })
    );
    let child = service
        .create(create(
            subscription("limited", "receiver"),
            EntityKind::Subscription,
            EntityOptions {
                default_time_to_live_millis: Some(500),
                lock_duration_millis: Some(100),
                max_delivery_count: Some(2),
                ..EntityOptions::default()
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        child.profile,
        EntityProfile::Subscription(SubscriptionConfig {
            default_time_to_live_millis: Some(500),
            lock_duration_millis: 100,
            max_delivery_count: 2,
        })
    );
    assert_eq!(
        service
            .get(get(subscription("limited", "receiver")))
            .await
            .unwrap(),
        child
    );
    // Parent message-size enforcement remains operational, not entity capacity.
    let before = raw.snapshot().unwrap();
    assert_eq!(
        broker
            .handle()
            .submit(
                ns("tenant"),
                path("limited"),
                CommandKind::Send {
                    message_id: "large".into(),
                    body: vec![0; 6],
                    time_to_live_millis: None,
                    session_id: None,
                    scheduled_enqueue_at: None,
                    envelope: None,
                }
            )
            .await,
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::MessageTooLarge {
                body_bytes: 6,
                maximum_bytes: 5
            }
        )))
    );
    assert_eq!(raw.snapshot().unwrap(), before);

    let dedupe = service
        .create(create(
            address("dedupe"),
            EntityKind::Queue,
            EntityOptions {
                requires_duplicate_detection: Some(true),
                duplicate_detection_history_millis: Some(20_000),
                ..EntityOptions::default()
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        dedupe.profile,
        EntityProfile::Queue(QueueConfig {
            requires_duplicate_detection: true,
            duplicate_detection_history_millis: 20_000,
            ..QueueConfig::default()
        })
    );

    // All domain validation happens after admission and clock stamping. Exact
    // image conservation includes Clock, owners, companions and all indexes.
    clock.clock.set(20_000);
    let invalid = [
        (
            EntityKind::Queue,
            EntityOptions {
                lock_duration_millis: Some(0),
                ..EntityOptions::default()
            },
            BrokerError::QueueConfig(QueueConfigError::LockDurationTooShort),
        ),
        (
            EntityKind::Queue,
            EntityOptions {
                max_delivery_count: Some(0),
                ..EntityOptions::default()
            },
            BrokerError::QueueConfig(QueueConfigError::MaxDeliveryCountTooSmall),
        ),
        (
            EntityKind::Queue,
            EntityOptions {
                max_message_bytes: Some(0),
                ..EntityOptions::default()
            },
            BrokerError::QueueConfig(QueueConfigError::MaxMessageBytesTooSmall),
        ),
        (
            EntityKind::Queue,
            EntityOptions {
                default_time_to_live_millis: Some(0),
                ..EntityOptions::default()
            },
            BrokerError::QueueConfig(QueueConfigError::TimeToLiveTooShort),
        ),
        (
            EntityKind::Queue,
            EntityOptions {
                duplicate_detection_history_millis: Some(1),
                ..EntityOptions::default()
            },
            BrokerError::QueueConfig(QueueConfigError::DuplicateDetectionHistoryTooShort {
                minimum_millis: 20_000,
            }),
        ),
        (
            EntityKind::Topic,
            EntityOptions {
                max_message_bytes: Some(0),
                ..EntityOptions::default()
            },
            BrokerError::TopicConfig(TopicConfigError::MaxMessageBytesTooSmall),
        ),
        (
            EntityKind::Topic,
            EntityOptions {
                default_time_to_live_millis: Some(0),
                ..EntityOptions::default()
            },
            BrokerError::TopicConfig(TopicConfigError::TimeToLiveTooShort),
        ),
        (
            EntityKind::Subscription,
            EntityOptions {
                lock_duration_millis: Some(0),
                ..EntityOptions::default()
            },
            BrokerError::SubscriptionConfig(SubscriptionConfigError::LockDurationTooShort),
        ),
        (
            EntityKind::Subscription,
            EntityOptions {
                max_delivery_count: Some(0),
                ..EntityOptions::default()
            },
            BrokerError::SubscriptionConfig(SubscriptionConfigError::MaxDeliveryCountTooSmall),
        ),
        (
            EntityKind::Subscription,
            EntityOptions {
                default_time_to_live_millis: Some(0),
                ..EntityOptions::default()
            },
            BrokerError::SubscriptionConfig(SubscriptionConfigError::TimeToLiveTooShort),
        ),
    ];
    for (index, (kind, options, error)) in invalid.into_iter().enumerate() {
        let target = if kind == EntityKind::Subscription {
            subscription("topic", &format!("invalid-{index}"))
        } else {
            address(&format!("invalid-{index}"))
        };
        let before = raw.snapshot().unwrap();
        assert_eq!(
            service.create(create(target.clone(), kind, options)).await,
            Err(refused(error))
        );
        assert_eq!(raw.snapshot().unwrap(), before);
        assert!(matches!(
            service.get(get(target)).await,
            Err(ServiceError::NotFound { .. })
        ));
    }
    for (target, kind, error) in [
        (
            address("queue"),
            EntityKind::Queue,
            BrokerError::QueueAlreadyExists,
        ),
        (
            address("topic"),
            EntityKind::Topic,
            BrokerError::TopicAlreadyExists,
        ),
        (
            subscription("topic", "child"),
            EntityKind::Subscription,
            BrokerError::SubscriptionAlreadyExists,
        ),
        (
            address("queue"),
            EntityKind::Topic,
            BrokerError::EntityAlreadyExists,
        ),
        (
            subscription("missing", "child"),
            EntityKind::Subscription,
            BrokerError::TopicNotFound,
        ),
    ] {
        let before = raw.snapshot().unwrap();
        assert_eq!(
            service
                .create(create(
                    target,
                    kind,
                    EntityOptions {
                        default_time_to_live_millis: Some(0),
                        ..EntityOptions::default()
                    }
                ))
                .await,
            Err(refused(error))
        );
        assert_eq!(raw.snapshot().unwrap(), before);
    }
    clock.clock.set(1);
    let before = raw.snapshot().unwrap();
    assert_eq!(
        service
            .create(create(
                address("queue"),
                EntityKind::Queue,
                EntityOptions {
                    lock_duration_millis: Some(0),
                    ..EntityOptions::default()
                }
            ))
            .await,
        Err(ServiceError::Backend(SubmitError::Propose(
            ProposeError::ClockWentBackward {
                last_applied: Timestamp::from_millis(10_000),
                now: Timestamp::from_millis(1),
                allowed_millis: 500,
            }
        )))
    );
    assert_eq!(raw.snapshot().unwrap(), before);
    clock.clock.set(20_000);
    control.fail_apply.store(true, Ordering::SeqCst);
    let before = raw.snapshot().unwrap();
    assert_eq!(
        service
            .create(create(
                address("storage-failure"),
                EntityKind::Queue,
                EntityOptions::default()
            ))
            .await,
        Err(refused(BrokerError::Storage(StorageError::LockPoisoned)))
    );
    assert_eq!(raw.snapshot().unwrap(), before);
    control.fail_apply.store(false, Ordering::SeqCst);
    *control.fail_get.lock().unwrap() = Some(keys::queue_config(&ns("tenant"), &path("queue")));
    assert_eq!(
        service.get(get(address("queue"))).await,
        Err(ServiceError::Catalog(AdminCatalogError::Storage(
            StorageError::LockPoisoned
        )))
    );
    *control.fail_get.lock().unwrap() = Some(keys::clock());
    assert_eq!(
        service
            .create(create(
                address("stamp-failure"),
                EntityKind::Queue,
                EntityOptions::default()
            ))
            .await,
        Err(refused(BrokerError::Storage(StorageError::LockPoisoned)))
    );
    *control.fail_get.lock().unwrap() = None;
    assert_eq!(raw.snapshot().unwrap(), before);

    let long_topic = "t".repeat(260);
    service
        .create(create(
            address(&long_topic),
            EntityKind::Topic,
            EntityOptions::default(),
        ))
        .await
        .unwrap();
    let long_child = subscription(&long_topic, &"s".repeat(50));
    let long = service
        .create(create(
            long_child.clone(),
            EntityKind::Subscription,
            EntityOptions::default(),
        ))
        .await
        .unwrap();
    assert!(long.path.as_str().len() > 260);
    assert_eq!(service.get(get(long_child)).await.unwrap(), long);

    let before = raw.snapshot().unwrap();
    let calls = clock.calls.load(Ordering::SeqCst);
    control.clear();
    let mut expected = Vec::new();
    let mut cursor = None;
    loop {
        let page = service
            .list(list(AdminCatalogKind::All, cursor, 2))
            .await
            .unwrap();
        for entity in &page.entities {
            let address = if let EntityProfile::Subscription(_) = entity.profile {
                let (topic, name) = entity.path.as_str().split_once("/subscriptions/").unwrap();
                subscription(topic, name)
            } else {
                address(entity.path.as_str())
            };
            assert_eq!(service.get(get(address)).await.unwrap(), *entity);
        }
        expected.extend(page.entities);
        cursor = page.continuation;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(expected.len(), 9);
    assert_eq!(clock.calls.load(Ordering::SeqCst), calls);
    let events = control.events.lock().unwrap().clone();
    assert!(!events.is_empty());
    let owner_thread = events[0].thread;
    assert_ne!(owner_thread, thread::current().id());
    for event in events {
        assert_eq!(event.thread, owner_thread);
        match event.operation {
            Operation::Get(key) => assert_ne!(key, keys::clock()),
            Operation::Scan(_, limit) => assert!(limit <= MAX_ADMIN_CATALOG_SCAN + 1),
            other => panic!("read used {other:?}"),
        }
    }
    assert_eq!(raw.snapshot().unwrap(), before);
    drop(service);
    drop(broker);
    drop(store);
    drop(raw);
    let reopened = provider.open().unwrap();
    assert_eq!(reopened.snapshot().unwrap(), before);
    let (broker, service) = owner(reopened.clone(), CountingClock::new());
    assert_eq!(
        service.get(get(address("custom"))).await.unwrap(),
        configured
    );
    drop(service);
    drop(broker);
    drop(reopened);
}

async fn rejected_before_owner<P: StoreProvider>(provider: P) {
    let raw = provider.open().unwrap();
    let control = Arc::new(Control::default());
    let store = Observed {
        store: raw.clone(),
        control: control.clone(),
    };
    let clock = CountingClock::new();
    let (broker, service) = owner(store, clock.clone());
    for name in ["a", "b"] {
        service
            .create(create(
                address(name),
                EntityKind::Queue,
                EntityOptions::default(),
            ))
            .await
            .unwrap();
    }
    let cursor = service
        .list(list(AdminCatalogKind::Queues, None, 1))
        .await
        .unwrap()
        .continuation
        .unwrap();
    let foreign = AdminService::new(ns("other"), broker.handle());
    for name in ["a", "b"] {
        let mut request = create(address(name), EntityKind::Queue, EntityOptions::default());
        request.namespace = "other".into();
        foreign.create(request).await.unwrap();
    }
    let foreign_cursor = foreign
        .list(ListEntitiesRequest {
            namespace: "other".into(),
            kind: AdminCatalogKind::Queues,
            continuation: None,
            limit: 1,
        })
        .await
        .unwrap()
        .continuation
        .unwrap();
    let mut foreign_only = create(
        address("foreign-only"),
        EntityKind::Queue,
        EntityOptions::default(),
    );
    foreign_only.namespace = "other".into();
    foreign.create(foreign_only).await.unwrap();
    let local = service
        .list(list(AdminCatalogKind::All, None, 100))
        .await
        .unwrap();
    assert_eq!(local.entities.len(), 2);
    assert!(
        local
            .entities
            .iter()
            .all(|entity| entity.namespace == ns("tenant") && entity.path != path("foreign-only"))
    );
    let before = raw.snapshot().unwrap();
    let mut broker = Some(broker);
    // Repeat against both a live and a stopped owner; no validation accidentally
    // becomes a success/empty result or gets masked by the stopped error.
    for stopped in [false, true] {
        if stopped {
            drop(broker.take());
        }
        control.clear();
        let calls = clock.calls.load(Ordering::SeqCst);
        let mut wrong = get(address("a"));
        wrong.namespace = "other".into();
        assert!(matches!(
            service.get(wrong.clone()).await,
            Err(ServiceError::ForeignNamespace { .. })
        ));
        let mut wrong_create = create(address("new"), EntityKind::Queue, EntityOptions::default());
        wrong_create.namespace = "other".into();
        assert!(matches!(
            service.create(wrong_create).await,
            Err(ServiceError::ForeignNamespace { .. })
        ));
        let mut wrong_list = list(AdminCatalogKind::All, None, 1);
        wrong_list.namespace = "other".into();
        assert!(matches!(
            service.list(wrong_list).await,
            Err(ServiceError::ForeignNamespace { .. })
        ));
        wrong.namespace = "bad\0namespace".into();
        assert!(matches!(
            service.get(wrong).await,
            Err(ServiceError::Identifier(_))
        ));
        for target in [
            address("q/$deadletterqueue"),
            address("q/$management"),
            address("q/subscriptions"),
            address("q/subscriptions/s"),
            subscription("q/$management", "s"),
            address("bad\0path"),
            address(&"x".repeat(261)),
            subscription("q", "bad/name"),
        ] {
            assert!(matches!(
                service.get(get(target.clone())).await,
                Err(ServiceError::InvalidEntityPath | ServiceError::Identifier(_))
            ));
            assert!(
                service
                    .create(create(target, EntityKind::Queue, EntityOptions::default()))
                    .await
                    .is_err()
            );
        }
        assert_eq!(
            service
                .create(create(
                    subscription("topic", "s"),
                    EntityKind::Queue,
                    EntityOptions::default()
                ))
                .await,
            Err(ServiceError::AddressKindMismatch)
        );
        assert_eq!(
            service
                .create(create(
                    address("topic"),
                    EntityKind::Subscription,
                    EntityOptions::default()
                ))
                .await,
            Err(ServiceError::AddressKindMismatch)
        );
        for limit in [0, MAX_ADMIN_CATALOG_PAGE + 1] {
            assert_eq!(
                service.list(list(AdminCatalogKind::All, None, limit)).await,
                Err(ServiceError::Catalog(AdminCatalogError::InvalidPageSize {
                    requested: limit
                }))
            );
        }
        assert_eq!(
            service
                .list(list(AdminCatalogKind::Topics, Some(cursor.clone()), 1))
                .await,
            Err(ServiceError::Catalog(
                AdminCatalogError::CursorScopeMismatch
            ))
        );
        assert_eq!(
            service
                .list(list(
                    AdminCatalogKind::Queues,
                    Some(foreign_cursor.clone()),
                    1
                ))
                .await,
            Err(ServiceError::Catalog(
                AdminCatalogError::CursorScopeMismatch
            ))
        );
        for bytes in [vec![], vec![1; 513], vec![99, 0]] {
            assert_eq!(
                service
                    .list(list(AdminCatalogKind::All, Some(bytes), 1))
                    .await,
                Err(ServiceError::Catalog(AdminCatalogError::InvalidCursor))
            );
        }
        for (kind, options, feature) in [
            (
                EntityKind::Queue,
                EntityOptions {
                    placement_group_id: Some(String::new()),
                    ..EntityOptions::default()
                },
                UnsupportedFeature::Placement,
            ),
            (
                EntityKind::Queue,
                EntityOptions {
                    max_size_bytes: Some(0),
                    ..EntityOptions::default()
                },
                UnsupportedFeature::Capacity,
            ),
            (
                EntityKind::Queue,
                EntityOptions {
                    used_logical_bytes: Some(0),
                    ..EntityOptions::default()
                },
                UnsupportedFeature::Usage,
            ),
            (
                EntityKind::Topic,
                EntityOptions {
                    lock_duration_millis: Some(0),
                    ..EntityOptions::default()
                },
                UnsupportedFeature::LockDuration,
            ),
            (
                EntityKind::Topic,
                EntityOptions {
                    max_delivery_count: Some(0),
                    ..EntityOptions::default()
                },
                UnsupportedFeature::MaxDeliveryCount,
            ),
            (
                EntityKind::Topic,
                EntityOptions {
                    requires_session: Some(false),
                    ..EntityOptions::default()
                },
                UnsupportedFeature::Sessions,
            ),
            (
                EntityKind::Topic,
                EntityOptions {
                    requires_duplicate_detection: Some(true),
                    ..EntityOptions::default()
                },
                UnsupportedFeature::DuplicateDetection,
            ),
            (
                EntityKind::Topic,
                EntityOptions {
                    duplicate_detection_history_millis: Some(0),
                    ..EntityOptions::default()
                },
                UnsupportedFeature::DuplicateHistory,
            ),
            (
                EntityKind::Subscription,
                EntityOptions {
                    max_message_bytes: Some(5),
                    ..EntityOptions::default()
                },
                UnsupportedFeature::MessageSize,
            ),
            (
                EntityKind::Subscription,
                EntityOptions {
                    requires_session: Some(true),
                    ..EntityOptions::default()
                },
                UnsupportedFeature::Sessions,
            ),
            (
                EntityKind::Subscription,
                EntityOptions {
                    requires_duplicate_detection: Some(false),
                    ..EntityOptions::default()
                },
                UnsupportedFeature::DuplicateDetection,
            ),
        ] {
            let target = if kind == EntityKind::Subscription {
                subscription("topic", "s")
            } else {
                address("new")
            };
            assert_eq!(
                service.create(create(target, kind, options)).await,
                Err(ServiceError::Unsupported(feature))
            );
        }
        assert!(control.events.lock().unwrap().is_empty());
        assert_eq!(clock.calls.load(Ordering::SeqCst), calls);
        assert_eq!(raw.snapshot().unwrap(), before);
    }
    control.clear();
    assert_eq!(
        service.get(get(address("a"))).await,
        Err(ServiceError::Backend(SubmitError::BrokerStopped))
    );
    assert_eq!(
        service
            .create(create(
                address("new"),
                EntityKind::Queue,
                EntityOptions::default()
            ))
            .await,
        Err(ServiceError::Backend(SubmitError::BrokerStopped))
    );
    assert_eq!(
        service.list(list(AdminCatalogKind::All, None, 1)).await,
        Err(ServiceError::Backend(SubmitError::BrokerStopped))
    );
    assert_eq!(
        service
            .list(list(AdminCatalogKind::Topics, Some(cursor), 1))
            .await,
        Err(ServiceError::Catalog(
            AdminCatalogError::CursorScopeMismatch
        ))
    );
    assert_eq!(
        service
            .list(list(AdminCatalogKind::Queues, Some(foreign_cursor), 1))
            .await,
        Err(ServiceError::Catalog(
            AdminCatalogError::CursorScopeMismatch
        ))
    );
    let mut wrong = get(address("a"));
    wrong.namespace = "other".into();
    assert!(matches!(
        service.get(wrong).await,
        Err(ServiceError::ForeignNamespace { .. })
    ));
    assert_eq!(
        service.get(get(address("a/$management"))).await,
        Err(ServiceError::InvalidEntityPath)
    );
    assert!(control.events.lock().unwrap().is_empty());
    drop(foreign);
    drop(service);
    drop(raw);
}

async fn pages_and_corruption<P: StoreProvider>(provider: P) {
    let raw = provider.open().unwrap();
    let control = Arc::new(Control::default());
    let store = Observed {
        store: raw.clone(),
        control: control.clone(),
    };
    let (broker, service) = owner(store.clone(), CountingClock::new());
    service
        .create(create(
            address("topic"),
            EntityKind::Topic,
            EntityOptions::default(),
        ))
        .await
        .unwrap();
    for index in 0..130 {
        service
            .create(create(
                subscription("topic", &format!("s{index:03}")),
                EntityKind::Subscription,
                EntityOptions::default(),
            ))
            .await
            .unwrap();
    }
    let before = raw.snapshot().unwrap();
    control.clear();
    let page = service
        .list(list(AdminCatalogKind::Queues, None, 100))
        .await
        .unwrap();
    assert!(page.entities.is_empty());
    assert!(page.continuation.is_some());
    let page = service
        .list(list(AdminCatalogKind::Queues, page.continuation, 100))
        .await
        .unwrap();
    assert!(page.entities.is_empty());
    assert!(page.continuation.is_none());
    let mut next = None;
    let mut entities = Vec::new();
    loop {
        let page = service
            .list(list(AdminCatalogKind::All, next, 100))
            .await
            .unwrap();
        assert!(page.entities.len() <= 100);
        next = page.continuation;
        entities.extend(page.entities);
        if next.is_none() {
            break;
        }
    }
    assert_eq!(entities.len(), 131);
    assert_eq!(entities[0].profile.kind(), EntityKind::Topic);
    assert!(
        entities[1..]
            .iter()
            .all(|entity| entity.profile.kind() == EntityKind::Subscription)
    );
    let scans = control.events.lock().unwrap().clone();
    assert!(scans.iter().any(|event| matches!(&event.operation, Operation::Scan(prefix, limit) if *limit == MAX_ADMIN_CATALOG_SCAN + 1 && *prefix == keys::namespace_queue_config_prefix(&ns("tenant")))));
    assert_eq!(raw.snapshot().unwrap(), before);
    for name in ["a", "b"] {
        service
            .create(create(
                address(name),
                EntityKind::Queue,
                EntityOptions::default(),
            ))
            .await
            .unwrap();
    }
    let continuation = service
        .list(list(AdminCatalogKind::Queues, None, 1))
        .await
        .unwrap()
        .continuation
        .unwrap();
    drop(service);
    drop(broker);
    // A deleted position remains a valid cursor, not an authority or snapshot.
    let removed: Vec<_> = [path("a"), path("a").dead_letter_queue().unwrap()]
        .into_iter()
        .map(|path| {
            let key = keys::queue_config(&ns("tenant"), &path);
            let value = raw.get(&key).unwrap().unwrap();
            (key, value)
        })
        .collect();
    let mut deletion = WriteBatch::default();
    for (key, _) in &removed {
        deletion.push_delete(key.clone());
    }
    raw.apply(deletion).unwrap();
    let (broker, service) = owner(store.clone(), CountingClock::new());
    let continued = service
        .list(list(AdminCatalogKind::Queues, Some(continuation), 1))
        .await
        .unwrap();
    assert_eq!(continued.entities[0].path, path("b"));
    drop(service);
    drop(broker);
    let mut restoration = WriteBatch::default();
    for (key, value) in removed {
        restoration.push_put(key, value);
    }
    raw.apply(restoration).unwrap();

    let child_name = SubscriptionName::new("s000").unwrap();
    let child_path = path("topic").subscription(&child_name).unwrap();
    for (key, replacement, selected) in [
        (
            keys::queue_config(&ns("tenant"), &path("b")),
            Some(vec![0xff]),
            address("b"),
        ),
        (
            keys::entity_metadata(&ns("tenant"), &path("a")),
            Some(vec![0xff]),
            address("a"),
        ),
        (
            keys::queue_config(&ns("tenant"), &path("a").dead_letter_queue().unwrap()),
            None,
            address("a"),
        ),
        (
            keys::topic_config(&ns("tenant"), &path("topic")),
            Some(vec![0xff]),
            address("topic"),
        ),
        (
            keys::topic_subscription(&ns("tenant"), &path("topic"), &child_name),
            Some(vec![0xff]),
            subscription("topic", "s000"),
        ),
        (
            keys::queue_config(&ns("tenant"), &child_path),
            Some(
                codec::encode(&QueueConfig {
                    requires_session: true,
                    ..QueueConfig::default()
                })
                .unwrap(),
            ),
            subscription("topic", "s000"),
        ),
    ] {
        let original = raw.get(&key).unwrap().unwrap();
        let batch = match replacement {
            Some(value) => WriteBatch::default().put(key.clone(), value),
            None => WriteBatch::default().delete(key.clone()),
        };
        raw.apply(batch).unwrap();
        let before = raw.snapshot().unwrap();
        let (broker, service) = owner(store.clone(), CountingClock::new());
        let kind = match &selected {
            EntityAddress::Subscription { .. } => AdminCatalogKind::Subscriptions,
            EntityAddress::Entity { path } if path == "topic" => AdminCatalogKind::Topics,
            _ => AdminCatalogKind::Queues,
        };
        assert_eq!(
            service.get(get(selected)).await,
            Err(ServiceError::Catalog(AdminCatalogError::CorruptCatalog))
        );
        // The corrupt b row is selected as lookahead when a fills a one-item page.
        assert_eq!(
            service.list(list(kind, None, 1)).await,
            Err(ServiceError::Catalog(AdminCatalogError::CorruptCatalog))
        );
        assert_eq!(raw.snapshot().unwrap(), before);
        drop(service);
        drop(broker);
        raw.apply(WriteBatch::default().put(key, original)).unwrap();
    }
    // Malformed persisted Clock is irrelevant to these reads. They neither
    // inspect it nor repair it, and still return all three public kinds.
    raw.apply(WriteBatch::default().put(keys::clock(), vec![0xff]))
        .unwrap();
    let before = raw.snapshot().unwrap();
    let clock = CountingClock::new();
    let (broker, service) = owner(store.clone(), clock.clone());
    control.clear();
    for target in [
        address("a"),
        address("topic"),
        subscription("topic", "s000"),
    ] {
        service.get(get(target)).await.unwrap();
    }
    service
        .list(list(AdminCatalogKind::All, None, 100))
        .await
        .unwrap();
    assert_eq!(clock.calls.load(Ordering::SeqCst), 0);
    assert!(
        control
            .events
            .lock()
            .unwrap()
            .iter()
            .all(|event| match &event.operation {
                Operation::Get(key) => *key != keys::clock(),
                Operation::Scan(_, _) => true,
                _ => false,
            })
    );
    assert!(matches!(
        service
            .create(create(
                address("corrupt-clock"),
                EntityKind::Queue,
                EntityOptions::default()
            ))
            .await,
        Err(ServiceError::Backend(SubmitError::Propose(
            ProposeError::Broker(BrokerError::Codec(_))
        )))
    ));
    assert_eq!(raw.snapshot().unwrap(), before);
    drop(service);
    drop(broker);
    drop(store);
    drop(raw);
    assert_eq!(provider.open().unwrap().snapshot().unwrap(), before);
}

async fn serialized_reads_and_cancel<P: StoreProvider>(provider: P) {
    let raw = provider.open().unwrap();
    let control = Arc::new(Control::default());
    let store = Observed {
        store: raw.clone(),
        control: control.clone(),
    };
    let clock = CountingClock::new();
    let (broker, service) = owner(store.clone(), clock.clone());
    service
        .create(create(
            address("topic"),
            EntityKind::Topic,
            EntityOptions::default(),
        ))
        .await
        .unwrap();
    // For Get, pause after the queue lookup but before the topic lookup; for
    // List, pause while validating a selected topic. A queued mutation cannot
    // interleave with either whole read and uses the same owner thread after it.
    for page in [false, true] {
        control.clear();
        if page {
            service
                .list(list(AdminCatalogKind::All, None, 100))
                .await
                .unwrap();
        } else {
            service.get(get(address("topic"))).await.unwrap();
        }
        let uninterrupted: Vec<_> = control
            .events
            .lock()
            .unwrap()
            .iter()
            .map(|event| event.operation.clone())
            .collect();
        let key = if page {
            keys::topic_config(&ns("tenant"), &path("topic"))
        } else {
            keys::queue_config(&ns("tenant"), &path("topic"))
        };
        let (entered, resume) = control.arm(key);
        control.clear();
        let read_service = service.clone();
        let reader = tokio::spawn(async move {
            if page {
                read_service
                    .list(list(AdminCatalogKind::All, None, 100))
                    .await
                    .map(|_| ())
            } else {
                read_service.get(get(address("topic"))).await.map(|_| ())
            }
        });
        entered.recv_timeout(Duration::from_secs(30)).unwrap();
        let mut mutation = Box::pin(service.create(create(
            address(if page { "after-list" } else { "after-get" }),
            EntityKind::Queue,
            EntityOptions::default(),
        )));
        poll_pending(mutation.as_mut());
        assert!(
            !control
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|event| matches!(event.operation, Operation::Apply))
        );
        resume.send(()).unwrap();
        reader.await.unwrap().unwrap();
        mutation.await.unwrap();
        let events = control.events.lock().unwrap().clone();
        assert!(events.iter().all(|event| event.thread == events[0].thread));
        let stamp = events
            .iter()
            .position(
                |event| matches!(&event.operation, Operation::Get(key) if *key == keys::clock()),
            )
            .unwrap();
        assert_eq!(
            events[..stamp]
                .iter()
                .map(|event| event.operation.clone())
                .collect::<Vec<_>>(),
            uninterrupted,
            "the complete read, not merely its first probe, precedes the queued mutation"
        );
        let last_read = events[..stamp].last().unwrap();
        assert!(matches!(
            last_read.operation,
            Operation::Get(_) | Operation::Scan(_, _)
        ));
        assert!(
            events[stamp..]
                .iter()
                .any(|event| matches!(event.operation, Operation::Apply))
        );
    }

    // Hold the owner in a read, admit a Create into its queue, then drop the
    // reply future before execution. Admitted work must still commit.
    let (entered, resume) = control.arm(keys::queue_config(&ns("tenant"), &path("topic")));
    let read_service = service.clone();
    let reader = tokio::spawn(async move { read_service.get(get(address("topic"))).await });
    entered.recv_timeout(Duration::from_secs(30)).unwrap();
    let mut accepted = Box::pin(service.create(create(
        address("cancelled-reply"),
        EntityKind::Queue,
        EntityOptions::default(),
    )));
    poll_pending(accepted.as_mut());
    drop(accepted);
    resume.send(()).unwrap();
    reader.await.unwrap().unwrap();
    service.get(get(address("cancelled-reply"))).await.unwrap();
    let before = raw.snapshot().unwrap();
    drop(service);
    drop(broker);
    drop(store);
    drop(raw);
    assert_eq!(provider.open().unwrap().snapshot().unwrap(), before);
}

macro_rules! paired {
    ($memory:ident, $fjall:ident, $suite:ident) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $memory() {
            $suite(MemoryProvider::new()).await;
        }
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $fjall() {
            $suite(DurableProvider::temporary().unwrap()).await;
        }
    };
}
paired!(
    memory_profiles_and_errors,
    fjall_profiles_and_errors,
    profiles_and_errors
);
paired!(
    memory_rejected_before_owner,
    fjall_rejected_before_owner,
    rejected_before_owner
);
paired!(
    memory_pages_and_corruption,
    fjall_pages_and_corruption,
    pages_and_corruption
);
paired!(
    memory_serialized_reads_and_cancel,
    fjall_serialized_reads_and_cancel,
    serialized_reads_and_cancel
);

// Only impossible backend outcomes need a seam; all ordinary behavior above
// uses Broker/LocalProposer and a real Memory/Fjall store.
#[derive(Clone)]
struct OutcomeBackend(Result<CommandOutcome, AdminError>);
impl EntityBackend for OutcomeBackend {
    type Error = SubmitError;
    async fn create(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, AdminError> {
        self.0.clone()
    }
    async fn get(
        &self,
        _: NamespaceName,
        _: EntityTarget,
    ) -> Result<Option<AdminCatalogEntry>, AdminError> {
        panic!("Create must not preflight")
    }
    async fn list(
        &self,
        _: NamespaceName,
        _: AdminCatalogKind,
        _: Option<AdminCatalogCursor>,
        _: usize,
    ) -> Result<AdminCatalogPage, AdminError> {
        panic!("Create must not preflight")
    }
}

#[tokio::test]
async fn every_create_checks_exact_outcome_and_subscription_path() {
    for (target, kind, expected, actual) in [
        (
            address("q"),
            EntityKind::Queue,
            CommandOutcome::QueueCreated,
            CommandOutcome::TopicCreated,
        ),
        (
            address("t"),
            EntityKind::Topic,
            CommandOutcome::TopicCreated,
            CommandOutcome::QueueCreated,
        ),
        (
            subscription("t", "s"),
            EntityKind::Subscription,
            CommandOutcome::SubscriptionCreated {
                entity: path("t/subscriptions/s"),
            },
            CommandOutcome::QueueCreated,
        ),
        (
            subscription("t", "s"),
            EntityKind::Subscription,
            CommandOutcome::SubscriptionCreated {
                entity: path("t/subscriptions/s"),
            },
            CommandOutcome::SubscriptionCreated {
                entity: path("t/subscriptions/wrong"),
            },
        ),
    ] {
        let service = EntityService::new(ns("tenant"), OutcomeBackend(Ok(actual.clone())));
        assert_eq!(
            service
                .create(create(target, kind, EntityOptions::default()))
                .await,
            Err(ServiceError::UnexpectedOutcome {
                expected: Box::new(expected),
                actual: Box::new(actual),
            })
        );
    }
}

#[tokio::test]
async fn backend_unexpected_outcome_error_is_not_string_erased() {
    let error = ServiceError::Backend(SubmitError::Propose(ProposeError::UnexpectedOutcome {
        outcome: "backend disagreement".into(),
    }));
    let service = EntityService::new(ns("tenant"), OutcomeBackend(Err(error.clone())));
    assert_eq!(
        service
            .create(create(
                address("q"),
                EntityKind::Queue,
                EntityOptions::default()
            ))
            .await,
        Err(error)
    );
}
