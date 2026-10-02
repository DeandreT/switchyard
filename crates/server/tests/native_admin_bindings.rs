//! Native bindings preserve admin spelling without broadening AMQP address admission.

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use domain::{
    BrokerError, CommandKind, CommandOutcome, DeleteEntityTarget, EntityIncarnation,
    EntityIncarnationKind, EntityPath, NamespaceName, QueueConfig, StateMachine,
    SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use protocol_amqp::{Attachment, EntityAdmission, EntityMetadata};
use server::{AdminTarget, Broker, Clock, LocalProposer, ManualClock, ProposeError, SubmitError};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct ObservedStore<S> {
    inner: S,
    writes: Arc<AtomicUsize>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
    }
}

#[derive(Clone)]
struct CountingClock {
    manual: ManualClock,
    reads: Arc<AtomicUsize>,
}

impl Clock for CountingClock {
    fn now(&self) -> Timestamp {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.manual.now()
    }
}

struct Node<P: StoreProvider> {
    broker: Broker,
    store: ObservedStore<P::Store>,
    clock: CountingClock,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    fn new(provider: P) -> TestResult<Self> {
        let store = ObservedStore {
            inner: provider.open()?,
            writes: Arc::new(AtomicUsize::new(0)),
        };
        let clock = CountingClock {
            manual: ManualClock::at(1_000),
            reads: Arc::new(AtomicUsize::new(0)),
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

    async fn submit(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        kind: CommandKind,
    ) -> TestResult<CommandOutcome> {
        Ok(tokio::time::timeout(
            DEADLINE,
            self.broker
                .handle()
                .submit(namespace.clone(), entity.clone(), kind),
        )
        .await??)
    }

    async fn bind(
        &self,
        namespace: &NamespaceName,
        target: AdminTarget,
    ) -> Result<Option<EntityAdmission>, SubmitError> {
        tokio::time::timeout(
            DEADLINE,
            self.broker.handle().bind_admin(namespace.clone(), target),
        )
        .await
        .expect("bounded native binding")
    }

    fn checkpoint(&self) -> TestResult<(StoreSnapshot, usize, usize)> {
        Ok((
            self.store.snapshot()?,
            self.store.writes.load(Ordering::SeqCst),
            self.clock.reads.load(Ordering::SeqCst),
        ))
    }

    fn unchanged(&self, checkpoint: &(StoreSnapshot, usize, usize)) -> TestResult {
        assert_eq!(self.store.snapshot()?, checkpoint.0);
        assert_eq!(self.store.writes.load(Ordering::SeqCst), checkpoint.1);
        assert_eq!(self.clock.reads.load(Ordering::SeqCst), checkpoint.2);
        Ok(())
    }
}

fn check(
    admission: EntityAdmission,
    namespace: &NamespaceName,
    entity: &EntityPath,
    kind: EntityIncarnationKind,
    generation: u64,
) {
    assert_eq!(admission.binding.namespace(), namespace);
    assert_eq!(admission.binding.target(), entity);
    assert_eq!(admission.binding.owner(), entity);
    assert_eq!(admission.binding.kind(), kind);
    assert_eq!(admission.binding.generation(), generation);
    assert!(matches!(
        (admission.metadata, kind),
        (EntityMetadata::Queue(_), EntityIncarnationKind::Queue)
            | (EntityMetadata::Topic(_), EntityIncarnationKind::Topic)
            | (
                EntityMetadata::Subscription(_),
                EntityIncarnationKind::Subscription
            )
    ));
}

async fn exact_literal_native_bindings_are_clock_free_and_do_not_change_amqp<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let namespace = NamespaceName::new("tenant")?;
    let queue = EntityPath::new("/Queue/$Management")?;
    let topic = EntityPath::new("/Orders/$Management")?;
    let name = SubscriptionName::new("Subscriptions")?;
    let child = topic.subscription(&name)?;
    node.submit(
        &namespace,
        &queue,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )
    .await?;
    node.submit(
        &namespace,
        &topic,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )
    .await?;
    node.submit(
        &namespace,
        &topic,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig::default(),
        },
    )
    .await?;
    let other_namespace = NamespaceName::new("Other")?;
    node.submit(
        &other_namespace,
        &queue,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )
    .await?;
    let leading_only = EntityPath::new("/LeadingQueue")?;
    let control_only = EntityPath::new("Queue/$Management")?;
    for entity in [&leading_only, &control_only] {
        node.submit(
            &namespace,
            entity,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        )
        .await?;
    }
    let before = node.checkpoint()?;
    node.clock.manual.set(0);
    for (namespace, target, entity, kind) in [
        (
            &namespace,
            AdminTarget::Primary(queue.clone()),
            &queue,
            EntityIncarnationKind::Queue,
        ),
        (
            &namespace,
            AdminTarget::Primary(topic.clone()),
            &topic,
            EntityIncarnationKind::Topic,
        ),
        (
            &namespace,
            AdminTarget::Subscription {
                topic: topic.clone(),
                name: name.clone(),
            },
            &child,
            EntityIncarnationKind::Subscription,
        ),
        (
            &other_namespace,
            AdminTarget::Primary(queue.clone()),
            &queue,
            EntityIncarnationKind::Queue,
        ),
        (
            &namespace,
            AdminTarget::Primary(leading_only.clone()),
            &leading_only,
            EntityIncarnationKind::Queue,
        ),
        (
            &namespace,
            AdminTarget::Primary(control_only.clone()),
            &control_only,
            EntityIncarnationKind::Queue,
        ),
    ] {
        check(
            node.bind(namespace, target).await?.expect("actual entity"),
            namespace,
            entity,
            kind,
            1,
        );
    }
    for attachment in [
        Attachment::Queue(queue),
        Attachment::Queue(leading_only),
        Attachment::Queue(control_only),
        Attachment::Queue(topic.clone()),
        Attachment::Subscription {
            topic,
            subscription: name,
        },
    ] {
        let error = tokio::time::timeout(
            DEADLINE,
            node.broker.handle().bind(namespace.clone(), attachment),
        )
        .await?
        .expect_err("AMQP literal control address refused");
        assert!(matches!(
            error,
            SubmitError::Propose(ProposeError::Broker(BrokerError::DanglingEntityMetadata))
        ));
    }
    node.unchanged(&before)
}

async fn missing_retired_and_recreated_native_targets_keep_generation_fences<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let namespace = NamespaceName::new("tenant")?;
    let queue = EntityPath::new("queue")?;
    let topic = EntityPath::new("topic")?;
    let name = SubscriptionName::new("Child")?;
    let child = topic.subscription(&name)?;
    let targets = [
        AdminTarget::Primary(queue.clone()),
        AdminTarget::Primary(topic.clone()),
        AdminTarget::Subscription {
            topic: topic.clone(),
            name: name.clone(),
        },
    ];
    let before = node.checkpoint()?;
    for target in &targets {
        assert!(node.bind(&namespace, target.clone()).await?.is_none());
    }
    node.unchanged(&before)?;
    node.submit(
        &namespace,
        &queue,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )
    .await?;
    node.submit(
        &namespace,
        &topic,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )
    .await?;
    node.submit(
        &namespace,
        &topic,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig::default(),
        },
    )
    .await?;
    node.submit(
        &namespace,
        &queue,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    )
    .await?;
    node.submit(
        &namespace,
        &topic,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Topic,
        },
    )
    .await?;
    for entity in [&queue, &topic, &child] {
        let bytes = node
            .store
            .get(&keys::entity_incarnation(&namespace, entity))?
            .expect("tombstone");
        assert!(codec::decode::<EntityIncarnation>(&bytes)?.is_retired());
    }
    let before = node.checkpoint()?;
    node.clock.manual.set(0);
    for target in &targets {
        assert!(node.bind(&namespace, target.clone()).await?.is_none());
    }
    node.unchanged(&before)?;
    node.clock.manual.set(1_000);
    node.submit(
        &namespace,
        &queue,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )
    .await?;
    node.submit(
        &namespace,
        &topic,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )
    .await?;
    node.submit(
        &namespace,
        &topic,
        CommandKind::CreateSubscription {
            name,
            config: SubscriptionConfig::default(),
        },
    )
    .await?;
    let before = node.checkpoint()?;
    node.clock.manual.set(0);
    for (target, entity, kind) in [
        (targets[0].clone(), queue, EntityIncarnationKind::Queue),
        (targets[1].clone(), topic, EntityIncarnationKind::Topic),
        (
            targets[2].clone(),
            child,
            EntityIncarnationKind::Subscription,
        ),
    ] {
        check(
            node.bind(&namespace, target)
                .await?
                .expect("recreated entity"),
            &namespace,
            &entity,
            kind,
            2,
        );
    }
    node.unchanged(&before)
}

async fn corrupt_incarnations_and_invalid_admin_targets_refuse_without_repairs<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let namespace = NamespaceName::new("tenant")?;
    let queue = EntityPath::new("queue")?;
    let topic = EntityPath::new("topic")?;
    let name = SubscriptionName::new("Child")?;
    let child = topic.subscription(&name)?;
    node.submit(
        &namespace,
        &queue,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )
    .await?;
    node.submit(
        &namespace,
        &topic,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )
    .await?;
    node.submit(
        &namespace,
        &topic,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig::default(),
        },
    )
    .await?;
    node.clock.manual.set(0);
    for (target, entity, kind, config_key) in [
        (
            AdminTarget::Primary(queue.clone()),
            &queue,
            EntityIncarnationKind::Queue,
            keys::queue_config(&namespace, &queue),
        ),
        (
            AdminTarget::Primary(topic.clone()),
            &topic,
            EntityIncarnationKind::Topic,
            keys::topic_config(&namespace, &topic),
        ),
        (
            AdminTarget::Subscription {
                topic: topic.clone(),
                name: name.clone(),
            },
            &child,
            EntityIncarnationKind::Subscription,
            keys::queue_config(&namespace, &child),
        ),
    ] {
        let key = keys::entity_incarnation(&namespace, entity);
        let original = node.store.get(&key)?.expect("live identity");
        let wrong_kind = if kind == EntityIncarnationKind::Queue {
            EntityIncarnationKind::Topic
        } else {
            EntityIncarnationKind::Queue
        };
        let invalid = [
            None,
            Some(codec::encode(&(0_u64, kind, false))?),
            Some(codec::encode(&EntityIncarnation::new(
                1, wrong_kind, false,
            )?)?),
            Some(codec::encode(&EntityIncarnation::new(1, kind, true)?)?),
        ];
        for replacement in invalid {
            let mut batch = WriteBatch::default();
            match replacement {
                Some(bytes) => batch.push_put(key.clone(), bytes),
                None => batch.push_delete(key.clone()),
            }
            node.store.apply(batch)?;
            let before = node.checkpoint()?;
            assert!(matches!(
                node.bind(&namespace, target.clone()).await,
                Err(SubmitError::Propose(ProposeError::Broker(
                    BrokerError::DanglingEntityMetadata
                )))
            ));
            node.unchanged(&before)?;
        }
        let mut restore = WriteBatch::default();
        restore.push_put(key, original);
        node.store.apply(restore)?;
        let config = node.store.get(&config_key)?.expect("live config");
        let mut remove = WriteBatch::default();
        remove.push_delete(config_key.clone());
        node.store.apply(remove)?;
        let before = node.checkpoint()?;
        assert!(matches!(
            node.bind(&namespace, target).await,
            Err(SubmitError::Propose(ProposeError::Broker(
                BrokerError::DanglingEntityMetadata | BrokerError::DanglingSubscriptionMetadata
            )))
        ));
        node.unchanged(&before)?;
        let mut restore = WriteBatch::default();
        restore.push_put(config_key, config);
        node.store.apply(restore)?;
    }
    let invalid_namespace: NamespaceName = codec::decode(&codec::encode(&"")?)?;
    let invalid_path: EntityPath = codec::decode(&codec::encode(&"")?)?;
    let before = node.checkpoint()?;
    for (namespace, target) in [
        (&invalid_namespace, AdminTarget::Primary(queue)),
        (&namespace, AdminTarget::Primary(invalid_path)),
        (&namespace, AdminTarget::Primary(child)),
        (&namespace, AdminTarget::Primary(topic.dead_letter_queue()?)),
    ] {
        assert!(matches!(
            node.bind(namespace, target).await,
            Err(SubmitError::Propose(ProposeError::Broker(
                BrokerError::Identifier(_)
                    | BrokerError::SubscriptionPathIsReserved
                    | BrokerError::DeadLetterQueueIsReserved
            )))
        ));
    }
    node.unchanged(&before)
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case()
            -> super::TestResult { tokio::time::timeout(super::DEADLINE * 12,
                super::$case(::testkit::MemoryProvider::new())).await? })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case()
            -> super::TestResult { tokio::time::timeout(super::DEADLINE * 12,
                super::$case(::testkit::DurableProvider::temporary()?)).await? })+ }
    };
}

for_each_backend! {
    exact_literal_native_bindings_are_clock_free_and_do_not_change_amqp,
    missing_retired_and_recreated_native_targets_keep_generation_fences,
    corrupt_incarnations_and_invalid_admin_targets_refuse_without_repairs,
}
