//! Link planning reads committed definitions without proposing commands.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use domain::{
    BrokerError, CommandKind, EntityPath, MAX_TOPIC_SUBSCRIPTIONS, NamespaceName, QueueConfig,
    StateMachine, SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use protocol_amqp::{Attachment, EntityMetadata};
use server::{AdminTarget, Broker, Clock, LocalProposer, ManualClock, ProposeError, SubmitError};
use storage::{StateStore, StorageError, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Default)]
struct Observations {
    reads: AtomicUsize,
    membership_limits: Mutex<Vec<usize>>,
    rule_limits: Mutex<Vec<usize>>,
    fail_get: Mutex<Option<Vec<u8>>>,
}

#[derive(Clone)]
struct ObservedStore<S> {
    inner: S,
    observed: Arc<Observations>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.observed.reads.fetch_add(1, Ordering::SeqCst);
        let mut failure = self.observed.fail_get.lock().expect("read failure");
        if failure.as_deref() == Some(key) {
            failure.take();
            return Err(StorageError::Backend {
                operation: "read entity definition",
                detail: "injected metadata read failure".into(),
            });
        }
        drop(failure);
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
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
        self.observed.reads.fetch_add(1, Ordering::SeqCst);
        if prefix.first() == Some(&0x0f) {
            self.observed
                .membership_limits
                .lock()
                .expect("scan limits")
                .push(limit);
        }
        if prefix.first() == Some(&0x10) {
            self.observed
                .rule_limits
                .lock()
                .expect("rule scan limits")
                .push(limit);
        }
        self.inner.scan_from(prefix, start, limit)
    }
}

#[path = "entity_metadata/rules.rs"]
mod rule_reads;

#[path = "entity_metadata/bindings.rs"]
mod binding_reads;

#[derive(Clone)]
struct ProbeClock {
    inner: ManualClock,
    forbidden: Arc<AtomicBool>,
    reads: Arc<AtomicUsize>,
}

impl Clock for ProbeClock {
    fn now(&self) -> Timestamp {
        assert!(
            !self.forbidden.load(Ordering::SeqCst),
            "metadata read consulted the clock"
        );
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.now()
    }
}

struct Node<P: StoreProvider> {
    broker: Broker,
    store: ObservedStore<P::Store>,
    clock: ProbeClock,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    fn new(provider: P) -> TestResult<Self> {
        let store = ObservedStore {
            inner: provider.open()?,
            observed: Arc::default(),
        };
        let clock = ProbeClock {
            inner: ManualClock::at(1_000),
            forbidden: Arc::default(),
            reads: Arc::default(),
        };
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        Ok(Self {
            broker,
            store,
            clock,
            _provider: provider,
        })
    }

    fn submit(&self, entity: &str, kind: CommandKind) -> TestResult {
        self.broker
            .handle()
            .submit_blocking(namespace(), EntityPath::new(entity)?, kind)?;
        Ok(())
    }

    fn queue(&self, entity: &str, config: QueueConfig) -> TestResult {
        self.submit(entity, CommandKind::CreateQueue { config })
    }

    fn topic(&self, entity: &str, config: TopicConfig) -> TestResult {
        self.submit(entity, CommandKind::CreateTopic { config })
    }

    fn subscription(&self, topic: &str, name: &str, config: SubscriptionConfig) -> TestResult {
        self.submit(
            topic,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new(name)?,
                config,
            },
        )
    }

    fn query(&self, target: Attachment) -> Result<Option<EntityMetadata>, SubmitError> {
        self.broker
            .handle()
            .entity_metadata_blocking(namespace(), target)
    }

    fn replace(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> TestResult {
        let mut batch = WriteBatch::default();
        match value {
            Some(value) => batch.push_put(key, value),
            None => batch.push_delete(key),
        }
        self.store.apply(batch)?;
        Ok(())
    }
}

fn namespace() -> NamespaceName {
    NamespaceName::new("tenant").expect("namespace")
}

fn primary(entity: &str) -> Attachment {
    Attachment::Queue(EntityPath::new(entity).expect("entity"))
}

fn shadow(entity: &str) -> Attachment {
    Attachment::DeadLetter(EntityPath::new(entity).expect("entity"))
}

fn subscription(topic: &str, name: &str, dead_letter: bool) -> Attachment {
    let topic = EntityPath::new(topic).expect("topic");
    let subscription = SubscriptionName::new(name).expect("subscription");
    if dead_letter {
        Attachment::SubscriptionDeadLetter {
            topic,
            subscription,
        }
    } else {
        Attachment::Subscription {
            topic,
            subscription,
        }
    }
}

fn refused(result: Result<Option<EntityMetadata>, SubmitError>) -> BrokerError {
    match result {
        Err(SubmitError::Propose(ProposeError::Broker(error))) => error,
        other => panic!("expected a broker rejection, got {other:?}"),
    }
}

async fn all_entity_kinds_are_read_without_stamping_or_clock_access<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let queue = QueueConfig {
        lock_duration_millis: 5_000,
        default_time_to_live_millis: Some(99),
        ..QueueConfig::default()
    };
    let topic = TopicConfig {
        requires_duplicate_detection: true,
        ..TopicConfig::default()
    };
    let member = SubscriptionConfig {
        requires_session: true,
        lock_duration_millis: 2_000,
        ..SubscriptionConfig::default()
    };
    node.queue("orders", queue)?;
    node.topic("events", topic)?;
    node.subscription("events", "Alpha", member)?;
    let expected = [
        (primary("orders"), EntityMetadata::Queue(queue)),
        (primary("events"), EntityMetadata::Topic(topic)),
        (
            shadow("orders"),
            EntityMetadata::DeadLetter(queue.dead_letter_shadow()),
        ),
        (
            subscription("events", "Alpha", false),
            EntityMetadata::Subscription(member),
        ),
        (
            subscription("events", "Alpha", true),
            EntityMetadata::DeadLetter(member.to_queue_config().dead_letter_shadow()),
        ),
    ];
    let snapshot = node.store.snapshot()?;
    let clock_reads = node.clock.reads.load(Ordering::SeqCst);
    node.clock.inner.set(0);
    node.clock.forbidden.store(true, Ordering::SeqCst);
    let handle = node.broker.handle();
    for (target, metadata) in expected {
        assert_eq!(node.query(target.clone())?, Some(metadata));
        assert_eq!(
            handle.entity_metadata(namespace(), target).await?,
            Some(metadata)
        );
    }
    assert_eq!(node.clock.reads.load(Ordering::SeqCst), clock_reads);
    assert_eq!(
        handle.last_applied_blocking()?,
        Timestamp::from_millis(1_000)
    );
    assert_eq!(node.store.snapshot()?, snapshot);
    assert!(
        node.store
            .observed
            .membership_limits
            .lock()
            .expect("scan limits")
            .iter()
            .all(|limit| *limit == MAX_TOPIC_SUBSCRIPTIONS + 1)
    );
    drop(node);
    assert!(
        handle
            .entity_metadata(namespace(), primary("orders"))
            .await
            .is_err()
    );
    assert!(
        handle
            .entity_metadata_blocking(namespace(), primary("orders"))
            .is_err()
    );
    Ok(())
}

fn missing_targets_do_not_borrow_other_namespaces_or_invent_topic_shadows<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("events", TopicConfig::default())?;
    node.subscription("events", "Alpha", SubscriptionConfig::default())?;
    let snapshot = node.store.snapshot()?;
    for target in [
        primary("absent"),
        shadow("absent"),
        shadow("events"),
        subscription("events", "alpha", false),
        subscription("events", "alpha", true),
        subscription("absent", "Alpha", false),
    ] {
        assert_eq!(node.query(target)?, None);
    }
    assert_eq!(
        node.broker
            .handle()
            .entity_metadata_blocking(NamespaceName::new("other")?, primary("events"))?,
        None
    );
    assert_eq!(node.store.snapshot()?, snapshot);
    Ok(())
}

fn queue_shadow_orphans_mismatches_and_ambiguous_primary_kinds_are_not_absence<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let config = QueueConfig::default();
    node.queue("orders", config)?;
    let entity = EntityPath::new("orders")?;
    let shadow_key = keys::queue_config(&namespace(), &entity.dead_letter_queue()?);
    for value in [
        None,
        Some(codec::encode(&QueueConfig {
            max_message_bytes: config.max_message_bytes + 1,
            ..config.dead_letter_shadow()
        })?),
    ] {
        node.replace(shadow_key.clone(), value)?;
        let snapshot = node.store.snapshot()?;
        assert_eq!(
            refused(node.query(shadow("orders"))),
            BrokerError::DanglingEntityMetadata
        );
        assert_eq!(node.store.snapshot()?, snapshot);
    }
    node.replace(
        shadow_key,
        Some(codec::encode(&config.dead_letter_shadow())?),
    )?;
    let parent_key = keys::queue_config(&namespace(), &entity);
    node.replace(parent_key.clone(), None)?;
    assert_eq!(
        refused(node.query(shadow("orders"))),
        BrokerError::DanglingEntityMetadata
    );
    node.replace(parent_key, Some(codec::encode(&config)?))?;
    node.replace(
        keys::topic_config(&namespace(), &entity),
        Some(codec::encode(&TopicConfig::default())?),
    )?;
    let snapshot = node.store.snapshot()?;
    assert_eq!(
        refused(node.query(primary("orders"))),
        BrokerError::DanglingEntityMetadata
    );
    assert_eq!(node.store.snapshot()?, snapshot);
    Ok(())
}

fn topic_and_subscription_queries_validate_complete_stored_topology<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("events", TopicConfig::default())?;
    node.subscription("events", "Alpha", SubscriptionConfig::default())?;
    let topic = EntityPath::new("events")?;
    let name = SubscriptionName::new("Alpha")?;
    let backing = topic.subscription(&name)?;
    node.replace(keys::queue_config(&namespace(), &backing), None)?;
    let snapshot = node.store.snapshot()?;
    for target in [
        primary("events"),
        subscription("events", "Alpha", false),
        subscription("events", "Alpha", true),
    ] {
        assert_eq!(
            refused(node.query(target)),
            BrokerError::DanglingSubscriptionMetadata
        );
    }
    assert_eq!(node.store.snapshot()?, snapshot);
    node.replace(
        keys::queue_config(&namespace(), &backing),
        Some(codec::encode(
            &SubscriptionConfig::default().to_queue_config(),
        )?),
    )?;
    node.replace(
        keys::subscription(&namespace(), &topic, &name),
        Some(vec![255]),
    )?;
    let snapshot = node.store.snapshot()?;
    assert!(matches!(
        refused(node.query(primary("events"))),
        BrokerError::Codec(_)
    ));
    assert!(matches!(
        refused(node.query(subscription("events", "Alpha", false))),
        BrokerError::Codec(_)
    ));
    assert_eq!(node.store.snapshot()?, snapshot);
    node.replace(
        keys::subscription(&namespace(), &topic, &name),
        Some(codec::encode(&SubscriptionConfig::default())?),
    )?;
    for entity in [&backing, &backing.dead_letter_queue()?] {
        let key = keys::topic_config(&namespace(), entity);
        node.replace(key.clone(), Some(codec::encode(&TopicConfig::default())?))?;
        let snapshot = node.store.snapshot()?;
        for target in [
            primary("events"),
            subscription("events", "Alpha", false),
            subscription("events", "Alpha", true),
        ] {
            assert_eq!(
                refused(node.query(target)),
                BrokerError::DanglingEntityMetadata
            );
        }
        assert_eq!(node.store.snapshot()?, snapshot);
        node.replace(key, None)?;
    }
    node.replace(
        keys::queue_config(&namespace(), &topic),
        Some(codec::encode(&QueueConfig::default())?),
    )?;
    let snapshot = node.store.snapshot()?;
    for target in [
        primary("events"),
        subscription("events", "Alpha", false),
        subscription("events", "Alpha", true),
    ] {
        assert_eq!(
            refused(node.query(target)),
            BrokerError::DanglingEntityMetadata
        );
    }
    assert_eq!(node.store.snapshot()?, snapshot);
    Ok(())
}

fn malformed_typed_requests_fail_before_store_reads<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("events", TopicConfig::default())?;
    node.subscription("events", "Alpha", SubscriptionConfig::default())?;
    for target in [
        primary("events/subscriptions/Alpha"),
        shadow("events/subscriptions/Alpha"),
        subscription("events/subscriptions/Alpha", "nested", false),
        shadow("orders/$deadletterqueue"),
    ] {
        node.store.observed.reads.store(0, Ordering::SeqCst);
        let snapshot = node.store.snapshot()?;
        assert_eq!(
            refused(node.query(target)),
            BrokerError::DanglingEntityMetadata
        );
        assert_eq!(node.store.observed.reads.load(Ordering::SeqCst), 0);
        assert_eq!(node.store.snapshot()?, snapshot);
    }
    Ok(())
}

fn numeric_config_and_storage_failures_preserve_their_source_and_state<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.queue("orders", QueueConfig::default())?;
    let key = keys::queue_config(&namespace(), &EntityPath::new("orders")?);
    node.replace(
        key.clone(),
        Some(codec::encode(&QueueConfig {
            max_message_bytes: 0,
            ..QueueConfig::default()
        })?),
    )?;
    let snapshot = node.store.snapshot()?;
    assert!(matches!(
        refused(node.query(primary("orders"))),
        BrokerError::QueueConfig(_)
    ));
    assert_eq!(node.store.snapshot()?, snapshot);
    node.replace(key.clone(), Some(codec::encode(&QueueConfig::default())?))?;
    *node.store.observed.fail_get.lock().expect("read failure") = Some(key);
    let snapshot = node.store.snapshot()?;
    assert_eq!(
        refused(node.query(primary("orders"))),
        BrokerError::Storage(StorageError::Backend {
            operation: "read entity definition",
            detail: "injected metadata read failure".into()
        })
    );
    assert_eq!(node.store.snapshot()?, snapshot);
    assert_eq!(
        node.query(primary("orders"))?,
        Some(EntityMetadata::Queue(QueueConfig::default()))
    );
    Ok(())
}

async fn native_reads_preserve_literal_names_without_relaxing_wire_targets<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.queue("/legacy/$Management", QueueConfig::default())?;
    let config = SubscriptionConfig {
        max_delivery_count: 7,
        ..SubscriptionConfig::default()
    };
    for topic in ["a/Subscriptions", "a/$Management"] {
        node.topic(topic, TopicConfig::default())?;
        node.subscription(topic, "Subscriptions", config)?;
    }
    let snapshot = node.store.snapshot()?;
    let clock_reads = node.clock.reads.load(Ordering::SeqCst);
    node.clock.inner.set(0);
    node.clock.forbidden.store(true, Ordering::SeqCst);
    let handle = node.broker.handle();
    let queue = AdminTarget::Primary(EntityPath::new("/legacy/$Management")?);
    assert_eq!(
        handle.admin_entity_metadata_blocking(namespace(), queue.clone())?,
        Some(EntityMetadata::Queue(QueueConfig::default()))
    );
    assert_eq!(
        handle.admin_entity_metadata(namespace(), queue).await?,
        Some(EntityMetadata::Queue(QueueConfig::default()))
    );
    for topic in ["a/Subscriptions", "a/$Management"] {
        let parent = EntityPath::new(topic)?;
        let name = SubscriptionName::new("Subscriptions")?;
        let target = AdminTarget::Subscription {
            topic: parent.clone(),
            name: name.clone(),
        };
        assert_eq!(
            handle.admin_entity_metadata_blocking(namespace(), target.clone())?,
            Some(EntityMetadata::Subscription(config))
        );
        assert_eq!(
            handle.admin_entity_metadata(namespace(), target).await?,
            Some(EntityMetadata::Subscription(config))
        );
        let expected = vec![domain::SubscriptionDefinition {
            name: name.clone(),
            entity: parent.subscription(&name)?,
            config,
        }];
        assert_eq!(
            handle.subscriptions_blocking(namespace(), parent.clone())?,
            expected
        );
        assert_eq!(handle.subscriptions(namespace(), parent).await?, expected);
    }
    assert_eq!(node.clock.reads.load(Ordering::SeqCst), clock_reads);
    assert_eq!(node.store.snapshot()?, snapshot);
    {
        let limits = node
            .store
            .observed
            .membership_limits
            .lock()
            .expect("scan limits");
        assert!(!limits.is_empty());
        assert!(
            limits
                .iter()
                .all(|&limit| limit == MAX_TOPIC_SUBSCRIPTIONS + 1)
        );
    }
    for target in [
        primary("/legacy/$Management"),
        subscription("a/$Management", "Subscriptions", false),
    ] {
        node.store.observed.reads.store(0, Ordering::SeqCst);
        assert_eq!(
            refused(node.query(target)),
            BrokerError::DanglingEntityMetadata
        );
        assert_eq!(node.store.observed.reads.load(Ordering::SeqCst), 0);
    }
    drop(node.broker);
    let target = AdminTarget::Primary(EntityPath::new("/legacy/$Management")?);
    assert_eq!(
        handle.admin_entity_metadata_blocking(namespace(), target.clone()),
        Err(SubmitError::BrokerStopped)
    );
    assert_eq!(
        handle.admin_entity_metadata(namespace(), target).await,
        Err(SubmitError::BrokerStopped)
    );
    let parent = EntityPath::new("a/Subscriptions")?;
    assert_eq!(
        handle.subscriptions_blocking(namespace(), parent.clone()),
        Err(SubmitError::BrokerStopped)
    );
    assert_eq!(
        handle.subscriptions(namespace(), parent).await,
        Err(SubmitError::BrokerStopped)
    );
    Ok(())
}

fn native_typed_requests_reject_reserved_shapes_before_store_reads<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    for target in [
        AdminTarget::Primary(EntityPath::new("orders/$deadletterqueue")?),
        AdminTarget::Primary(EntityPath::new("orders/Subscriptions/Alpha")?),
        AdminTarget::Subscription {
            topic: EntityPath::new("orders/Subscriptions/Alpha")?,
            name: SubscriptionName::new("beta")?,
        },
    ] {
        node.store.observed.reads.store(0, Ordering::SeqCst);
        let error = refused(
            node.broker
                .handle()
                .admin_entity_metadata_blocking(namespace(), target),
        );
        assert!(matches!(
            error,
            BrokerError::DeadLetterQueueIsReserved | BrokerError::SubscriptionPathIsReserved
        ));
        assert_eq!(node.store.observed.reads.load(Ordering::SeqCst), 0);
    }
    for parent in ["orders/$deadletterqueue", "orders/Subscriptions/Alpha"] {
        node.store.observed.reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            node.broker
                .handle()
                .subscriptions_blocking(namespace(), EntityPath::new(parent)?),
            Err(SubmitError::Propose(ProposeError::Broker(
                BrokerError::DeadLetterQueueIsReserved | BrokerError::SubscriptionPathIsReserved
            )))
        ));
        assert_eq!(node.store.observed.reads.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

fn old_metadata_errors_precede_missing_capacity_mode_without_clock_or_mutation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let config = QueueConfig::default();
    node.queue("orders", config)?;
    let entity = EntityPath::new("orders")?;
    node.replace(keys::queue_capacity_mode(&namespace(), &entity), None)?;
    node.clock.forbidden.store(true, Ordering::SeqCst);
    for (key, replacement, target, bind, error) in [
        (
            keys::queue_config(&namespace(), &entity),
            Some(codec::encode(&QueueConfig {
                max_delivery_count: 0,
                ..config
            })?),
            primary("orders"),
            false,
            BrokerError::QueueConfig(domain::QueueConfigError::MaxDeliveryCountTooSmall),
        ),
        (
            keys::queue_config(&namespace(), &entity.dead_letter_queue()?),
            None,
            shadow("orders"),
            false,
            BrokerError::DanglingEntityMetadata,
        ),
        (
            keys::entity_incarnation(&namespace(), &entity),
            None,
            primary("orders"),
            true,
            BrokerError::DanglingEntityMetadata,
        ),
    ] {
        let original = node.store.get(&key)?;
        node.replace(key.clone(), replacement)?;
        let before = node.store.snapshot()?;
        let result = if bind {
            node.broker
                .handle()
                .bind_blocking(namespace(), target)
                .map(|admission| admission.map(|admission| admission.metadata))
        } else {
            node.query(target)
        };
        assert_eq!(refused(result), error);
        assert_eq!(node.store.snapshot()?, before);
        node.replace(key, original)?;
    }
    let before = node.store.snapshot()?;
    for target in [primary("orders"), shadow("orders")] {
        assert_eq!(
            refused(node.query(target.clone())),
            BrokerError::QueueCapacityCorrupt
        );
        assert_eq!(
            refused(
                node.broker
                    .handle()
                    .bind_blocking(namespace(), target)
                    .map(|admission| admission.map(|admission| admission.metadata))
            ),
            BrokerError::QueueCapacityCorrupt
        );
        assert_eq!(node.store.snapshot()?, before);
    }
    Ok(())
}

fn native_binding_metadata<P: StoreProvider>(
    node: &Node<P>,
    target: AdminTarget,
) -> TestResult<Result<Option<EntityMetadata>, SubmitError>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let handle = node.broker.handle();
    let result = runtime.block_on(async {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handle.bind_admin(namespace(), target),
        )
        .await
    })?;
    Ok(result.map(|admission| admission.map(|admission| admission.metadata)))
}

fn assert_metadata_and_binding_error<P: StoreProvider>(
    node: &Node<P>,
    target: Attachment,
    error: &BrokerError,
) -> TestResult {
    macro_rules! one_scan {
        ($read:expr) => {{
            let before = node.store.observed.membership_limits.lock().unwrap().len();
            let result = $read;
            let after = node.store.observed.membership_limits.lock().unwrap().len();
            assert!(
                after <= before + 1,
                "final capacity proof must not rescan membership"
            );
            result
        }};
    }
    let before = node.store.snapshot()?;
    assert_eq!(&refused(one_scan!(node.query(target.clone()))), error);
    assert_eq!(
        &refused(one_scan!(
            node.broker
                .handle()
                .bind_blocking(namespace(), target.clone())
                .map(|admission| admission.map(|admission| admission.metadata))
        )),
        error
    );
    let native = match target {
        Attachment::Queue(entity) => Some(AdminTarget::Primary(entity)),
        Attachment::Subscription {
            topic,
            subscription,
        } => Some(AdminTarget::Subscription {
            topic,
            name: subscription,
        }),
        _ => None,
    };
    if let Some(native) = native {
        assert_eq!(
            &refused(one_scan!(
                node.broker
                    .handle()
                    .admin_entity_metadata_blocking(namespace(), native.clone())
            )),
            error
        );
        assert_eq!(
            &refused(one_scan!(native_binding_metadata(node, native)?)),
            error
        );
    }
    assert_eq!(node.store.snapshot()?, before);
    Ok(())
}

fn excluded_owner_topology_and_identity_errors_precede_capacity_sidecars<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("events", TopicConfig::default())?;
    node.subscription("events", "Alpha", SubscriptionConfig::default())?;
    let topic = EntityPath::new("events")?;
    let child = topic.subscription(&SubscriptionName::new("Alpha")?)?;
    let topic_mode = keys::queue_capacity_mode(&namespace(), &topic);
    node.replace(topic_mode.clone(), Some(vec![255]))?;
    node.clock.forbidden.store(true, Ordering::SeqCst);
    let parent_queue = keys::queue_config(&namespace(), &topic);
    node.replace(
        parent_queue.clone(),
        Some(codec::encode(&QueueConfig::default())?),
    )?;
    assert_metadata_and_binding_error(
        &node,
        primary("events"),
        &BrokerError::DanglingEntityMetadata,
    )?;
    node.replace(parent_queue, None)?;

    let child_backing = keys::queue_config(&namespace(), &child);
    let original_backing = node.store.get(&child_backing)?;
    node.replace(child_backing.clone(), None)?;
    for target in [
        primary("events"),
        subscription("events", "Alpha", false),
        subscription("events", "Alpha", true),
    ] {
        assert_metadata_and_binding_error(
            &node,
            target,
            &BrokerError::DanglingSubscriptionMetadata,
        )?;
    }
    node.replace(child_backing.clone(), original_backing.clone())?;

    let topic_identity = keys::entity_incarnation(&namespace(), &topic);
    let original_identity = node.store.get(&topic_identity)?;
    node.replace(topic_identity.clone(), None)?;
    let before = node.store.snapshot()?;
    for target in [primary("events"), shadow("events")] {
        assert_eq!(
            refused(
                node.broker
                    .handle()
                    .bind_blocking(namespace(), target)
                    .map(|admission| admission.map(|admission| admission.metadata))
            ),
            BrokerError::DanglingEntityMetadata
        );
        assert_eq!(node.store.snapshot()?, before);
    }
    assert_eq!(
        refused(native_binding_metadata(
            &node,
            AdminTarget::Primary(topic.clone())
        )?),
        BrokerError::DanglingEntityMetadata
    );
    assert_eq!(node.store.snapshot()?, before);
    node.replace(topic_identity, original_identity)?;
    for target in [
        primary("events"),
        shadow("events"),
        subscription("events", "Alpha", false),
    ] {
        assert_metadata_and_binding_error(&node, target, &BrokerError::QueueCapacityCorrupt)?;
    }
    node.replace(topic_mode, None)?;

    let child_mode = keys::queue_capacity_mode(&namespace(), &child);
    node.replace(child_mode.clone(), Some(vec![255]))?;
    node.replace(
        child_backing.clone(),
        Some(codec::encode(&QueueConfig {
            max_delivery_count: SubscriptionConfig::default().max_delivery_count + 1,
            ..SubscriptionConfig::default().to_queue_config()
        })?),
    )?;
    for target in [
        primary("events"),
        subscription("events", "Alpha", false),
        subscription("events", "Alpha", true),
    ] {
        assert_metadata_and_binding_error(
            &node,
            target,
            &BrokerError::DanglingSubscriptionMetadata,
        )?;
    }
    node.replace(child_backing, original_backing)?;
    let child_identity = keys::entity_incarnation(&namespace(), &child);
    let original_identity = node.store.get(&child_identity)?;
    node.replace(child_identity.clone(), None)?;
    let before = node.store.snapshot()?;
    for target in [
        subscription("events", "Alpha", false),
        subscription("events", "Alpha", true),
    ] {
        assert_eq!(
            refused(
                node.broker
                    .handle()
                    .bind_blocking(namespace(), target)
                    .map(|admission| admission.map(|admission| admission.metadata))
            ),
            BrokerError::DanglingEntityMetadata
        );
        assert_eq!(node.store.snapshot()?, before);
    }
    assert_eq!(
        refused(native_binding_metadata(
            &node,
            AdminTarget::Subscription {
                topic: topic.clone(),
                name: SubscriptionName::new("Alpha")?,
            }
        )?),
        BrokerError::DanglingEntityMetadata
    );
    assert_eq!(node.store.snapshot()?, before);
    node.replace(child_identity, original_identity)?;
    for target in [
        primary("events"),
        subscription("events", "Alpha", false),
        subscription("events", "Alpha", true),
    ] {
        assert_metadata_and_binding_error(&node, target, &BrokerError::QueueCapacityCorrupt)?;
    }
    node.replace(child_mode, None)?;
    Ok(())
}

fn scoped_absent_owners_refuse_primary_and_shadow_sidecars<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.queue("retired", QueueConfig::default())?;
    node.submit(
        "retired",
        CommandKind::DeleteEntity {
            target: domain::DeleteEntityTarget::Queue,
        },
    )?;
    node.topic("events", TopicConfig::default())?;
    node.clock.forbidden.store(true, Ordering::SeqCst);
    for owner in [EntityPath::new("absent")?, EntityPath::new("retired")?] {
        for physical in [&owner, &owner.dead_letter_queue()?] {
            for key in [
                keys::queue_capacity_mode(&namespace(), physical),
                keys::queue_capacity_usage(&namespace(), physical),
            ] {
                node.replace(key.clone(), Some(vec![255]))?;
                for target in [
                    Attachment::Queue(owner.clone()),
                    Attachment::DeadLetter(owner.clone()),
                ] {
                    assert_metadata_and_binding_error(
                        &node,
                        target,
                        &BrokerError::QueueCapacityCorrupt,
                    )?;
                }
                node.replace(key, None)?;
            }
        }
    }
    let topic = EntityPath::new("events")?;
    let missing_child = topic.subscription(&SubscriptionName::new("missing")?)?;
    for physical in [&missing_child, &missing_child.dead_letter_queue()?] {
        for key in [
            keys::queue_capacity_mode(&namespace(), physical),
            keys::queue_capacity_usage(&namespace(), physical),
        ] {
            node.replace(key.clone(), Some(vec![255]))?;
            for target in [
                subscription("events", "missing", false),
                subscription("events", "missing", true),
            ] {
                assert_metadata_and_binding_error(
                    &node,
                    target,
                    &BrokerError::QueueCapacityCorrupt,
                )?;
            }
            node.replace(key, None)?;
        }
    }
    for physical in [&topic, &topic.dead_letter_queue()?] {
        for key in [
            keys::queue_capacity_mode(&namespace(), physical),
            keys::queue_capacity_usage(&namespace(), physical),
        ] {
            node.replace(key.clone(), Some(vec![255]))?;
            assert_metadata_and_binding_error(
                &node,
                shadow("events"),
                &BrokerError::QueueCapacityCorrupt,
            )?;
            node.replace(key, None)?;
        }
    }
    let before = node.store.snapshot()?;
    for target in [
        primary("absent"),
        shadow("absent"),
        primary("retired"),
        shadow("retired"),
        shadow("events"),
        subscription("events", "missing", false),
        subscription("events", "missing", true),
    ] {
        assert_eq!(node.query(target.clone())?, None);
        assert_eq!(
            node.broker.handle().bind_blocking(namespace(), target)?,
            None
        );
    }
    assert_eq!(node.store.snapshot()?, before);
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    missing_targets_do_not_borrow_other_namespaces_or_invent_topic_shadows,
    queue_shadow_orphans_mismatches_and_ambiguous_primary_kinds_are_not_absence,
    topic_and_subscription_queries_validate_complete_stored_topology,
    malformed_typed_requests_fail_before_store_reads,
    numeric_config_and_storage_failures_preserve_their_source_and_state,
    native_typed_requests_reject_reserved_shapes_before_store_reads,
    old_metadata_errors_precede_missing_capacity_mode_without_clock_or_mutation,
    excluded_owner_topology_and_identity_errors_precede_capacity_sidecars,
    scoped_absent_owners_refuse_primary_and_shadow_sidecars,
}

#[tokio::test]
async fn memory_all_kinds_are_clock_free() -> TestResult {
    all_entity_kinds_are_read_without_stamping_or_clock_access(testkit::MemoryProvider::new()).await
}

#[tokio::test]
async fn durable_all_kinds_are_clock_free() -> TestResult {
    all_entity_kinds_are_read_without_stamping_or_clock_access(
        testkit::DurableProvider::temporary()?
    )
    .await
}

#[tokio::test]
async fn memory_native_reads_are_clock_free() -> TestResult {
    native_reads_preserve_literal_names_without_relaxing_wire_targets(testkit::MemoryProvider::new()).await
}

#[tokio::test]
async fn durable_native_reads_are_clock_free() -> TestResult {
    native_reads_preserve_literal_names_without_relaxing_wire_targets(
        testkit::DurableProvider::temporary()?,
    )
    .await
}
