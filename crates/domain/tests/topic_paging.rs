//! Topic pages remain bounded and disjoint from receiving queue discovery.

use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, EntityPath, MAX_TOPIC_PAGE_SIZE,
    NamespaceName, QueueConfig, StateMachine, SubscriptionConfig, SubscriptionName, Timestamp,
    TopicConfig, TopicCursor, TopicPage, codec, keys,
};
use storage::{StateStore, StorageError, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScanCall {
    prefix: Vec<u8>,
    start: Vec<u8>,
    limit: usize,
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    calls: Arc<Mutex<Vec<ScanCall>>>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
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
        if prefix.first() == keys::topic_config_prefix().first() {
            assert!(limit <= MAX_TOPIC_PAGE_SIZE + 1);
            self.calls.lock().expect("scan recorder").push(ScanCall {
                prefix: prefix.to_vec(),
                start: start.to_vec(),
                limit,
            });
        }
        self.inner.scan_from(prefix, start, limit)
    }
}

struct Node<P: StoreProvider> {
    machine: StateMachine<ObservedStore<P::Store>>,
    calls: Arc<Mutex<Vec<ScanCall>>>,
    provider: P,
}

impl<P: StoreProvider> Node<P> {
    fn new(provider: P) -> TestResult<Self> {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let machine = StateMachine::new(ObservedStore {
            inner: provider.open()?,
            calls: Arc::clone(&calls),
        });
        Ok(Self {
            machine,
            calls,
            provider,
        })
    }

    fn restart(self) -> TestResult<Self> {
        let Self {
            machine,
            calls,
            provider,
        } = self;
        drop(machine);
        let machine = StateMachine::new(ObservedStore {
            inner: provider.open()?,
            calls: Arc::clone(&calls),
        });
        Ok(Self {
            machine,
            calls,
            provider,
        })
    }

    fn topic(&self, namespace: &str, entity: &str) -> TestResult {
        assert_eq!(
            self.apply(
                namespace,
                entity,
                CommandKind::CreateTopic {
                    config: TopicConfig::default()
                }
            )?,
            CommandOutcome::TopicCreated
        );
        Ok(())
    }

    fn apply(
        &self,
        namespace: &str,
        entity: &str,
        kind: CommandKind,
    ) -> TestResult<CommandOutcome> {
        Ok(self.machine.apply(&Command::new(
            NamespaceName::new(namespace)?,
            EntityPath::new(entity)?,
            Timestamp::UNIX_EPOCH,
            kind,
        ))?)
    }

    fn clear_calls(&self) {
        self.calls.lock().expect("scan recorder").clear();
    }
    fn calls(&self) -> Vec<ScanCall> {
        self.calls.lock().expect("scan recorder").clone()
    }
}

fn position(namespace: &str, entity: &str) -> TopicCursor {
    TopicCursor {
        namespace: NamespaceName::new(namespace).expect("namespace"),
        entity: EntityPath::new(entity).expect("entity"),
    }
}

fn collect<P: StoreProvider>(
    node: &Node<P>,
    namespace: Option<&NamespaceName>,
    mut after: Option<TopicCursor>,
    limit: usize,
) -> TestResult<Vec<(NamespaceName, EntityPath)>> {
    let mut all = Vec::new();
    loop {
        let page = node.machine.topics_page(namespace, after.as_ref(), limit)?;
        assert!(page.topics.len() <= limit);
        if let Some(cursor) = &page.continuation {
            assert_eq!(
                page.topics.last(),
                Some(&(cursor.namespace.clone(), cursor.entity.clone()))
            );
            assert_ne!(after.as_ref(), Some(cursor));
        }
        all.extend(page.topics);
        after = page.continuation;
        if after.is_none() {
            return Ok(all);
        }
    }
}

fn global_and_scoped_pages_exclude_queues_and_subscription_shadows<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    for namespace in ["alpha", "beta", "beta-other"] {
        for topic in ["orders", "orders/nested"] {
            node.topic(namespace, topic)?;
        }
        node.apply(
            namespace,
            "queue",
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        )?;
        node.apply(
            namespace,
            "orders",
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("accounting")?,
                config: SubscriptionConfig::default(),
            },
        )?;
    }
    let expected: Vec<_> = ["alpha", "beta", "beta-other"]
        .into_iter()
        .flat_map(|namespace| {
            ["orders", "orders/nested"].map(|entity| {
                (
                    NamespaceName::new(namespace).expect("namespace"),
                    EntityPath::new(entity).expect("topic"),
                )
            })
        })
        .collect();
    let snapshot = node.machine.store().snapshot()?;
    assert_eq!(collect(&node, None, None, 2)?, expected);
    let queues = node.machine.queues_page(None, None, 32)?.queues;
    assert_eq!(queues.len(), 12);
    assert!(
        queues
            .iter()
            .all(|(_, entity)| entity.as_str() != "orders" && entity.as_str() != "orders/nested")
    );
    for name in ["alpha", "beta", "beta-other", "absent"] {
        let namespace = NamespaceName::new(name)?;
        let scoped = expected
            .iter()
            .filter(|(stored, _)| stored == &namespace)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(collect(&node, Some(&namespace), None, 1)?, scoped);
    }
    let first = node.machine.topics_page(None, None, 2)?;
    let node = node.restart()?;
    let mut resumed = first.topics;
    resumed.extend(collect(&node, None, first.continuation, 2)?);
    assert_eq!(resumed, expected);
    assert_eq!(node.machine.store().snapshot()?, snapshot);
    Ok(())
}

fn limits_and_scope_errors_precede_scans_and_use_one_lookahead<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    for name in ["a", "b", "c"] {
        node.topic("tenant", name)?;
    }
    let namespace = NamespaceName::new("tenant")?;
    let foreign = position("other", "a");
    let snapshot = node.machine.store().snapshot()?;
    node.clear_calls();
    for limit in [MAX_TOPIC_PAGE_SIZE + 1, usize::MAX] {
        assert_eq!(
            node.machine.topics_page(None, None, limit),
            Err(BrokerError::TopicPageLimitExceeded {
                limit,
                maximum: MAX_TOPIC_PAGE_SIZE
            })
        );
    }
    assert_eq!(
        node.machine
            .topics_page(Some(&namespace), Some(&foreign), 0),
        Err(BrokerError::TopicCursorNamespaceMismatch {
            namespace: namespace.clone(),
            cursor_namespace: foreign.namespace.clone()
        })
    );
    assert_eq!(
        node.machine.topics_page(Some(&namespace), None, 0)?,
        TopicPage {
            topics: Vec::new(),
            continuation: None
        }
    );
    assert!(node.calls().is_empty());
    let first = node.machine.topics_page(Some(&namespace), None, 1)?;
    assert_eq!(
        first.topics,
        vec![(namespace.clone(), EntityPath::new("a")?)]
    );
    assert_eq!(first.continuation, Some(position("tenant", "a")));
    let prefix = keys::namespace_topic_config_prefix(&namespace);
    assert_eq!(
        node.calls(),
        vec![ScanCall {
            prefix: prefix.clone(),
            start: prefix,
            limit: 2
        }]
    );
    node.clear_calls();
    let second = node
        .machine
        .topics_page(Some(&namespace), first.continuation.as_ref(), 1)?;
    assert_eq!(
        second.topics,
        vec![(namespace.clone(), EntityPath::new("b")?)]
    );
    let mut start = keys::topic_config(&namespace, &EntityPath::new("a")?);
    start.push(0);
    assert_eq!(
        node.calls(),
        vec![ScanCall {
            prefix: keys::namespace_topic_config_prefix(&namespace),
            start,
            limit: 2
        }]
    );
    assert_eq!(node.machine.store().snapshot()?, snapshot);
    Ok(())
}

fn deleted_cursor_and_insertions_resume_exclusively_after_restart<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    for name in ["a", "m", "z"] {
        node.topic("tenant", name)?;
    }
    let first = node.machine.topics_page(None, None, 1)?;
    let cursor = first.continuation.expect("remaining topics");
    let mut batch = WriteBatch::default();
    batch.push_delete(keys::topic_config(&cursor.namespace, &cursor.entity));
    node.machine.store().apply(batch)?;
    node.topic("tenant", "0-before")?;
    node.topic("tenant", "b-after")?;
    let snapshot = node.machine.store().snapshot()?;
    let node = node.restart()?;
    assert_eq!(
        collect(&node, None, Some(cursor), 1)?
            .into_iter()
            .map(|(_, entity)| entity.to_string())
            .collect::<Vec<_>>(),
        ["b-after", "m", "z"]
    );
    assert_eq!(node.machine.store().snapshot()?, snapshot);
    Ok(())
}

fn exact_page_end_and_empty_ranges_have_no_continuation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    for name in ["a", "b"] {
        node.topic("tenant", name)?;
    }
    let all = node.machine.topics_page(None, None, 2)?;
    assert_eq!(all.topics.len(), 2);
    assert_eq!(all.continuation, None);
    let first = node.machine.topics_page(None, None, 1)?;
    let second = node
        .machine
        .topics_page(None, first.continuation.as_ref(), 1)?;
    assert_eq!(second.topics, all.topics[1..]);
    assert_eq!(second.continuation, None);
    for cursor in [position("tenant", "b"), position("zzzz", "z")] {
        assert_eq!(
            node.machine.topics_page(None, Some(&cursor), 1)?,
            TopicPage {
                topics: Vec::new(),
                continuation: None
            }
        );
    }
    assert_eq!(
        node.machine
            .topics_page(Some(&NamespaceName::new("absent")?), None, 1)?,
        TopicPage {
            topics: Vec::new(),
            continuation: None
        }
    );
    Ok(())
}

fn malformed_topic_keys_are_reported_without_mutation<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("tenant", "z")?;
    let namespace = NamespaceName::new("tenant")?;
    let mut key = keys::topic_config(&namespace, &EntityPath::new("bad")?);
    key.push(1);
    let mut batch = WriteBatch::default();
    batch.push_put(key, codec::encode(&TopicConfig::default())?);
    node.machine.store().apply(batch)?;
    let snapshot = node.machine.store().snapshot()?;
    assert_eq!(
        node.machine.topics_page(None, None, 2),
        Err(BrokerError::MalformedIndexKey)
    );
    let node = node.restart()?;
    assert_eq!(
        node.machine.topics_page(Some(&namespace), None, 2),
        Err(BrokerError::MalformedIndexKey)
    );
    assert_eq!(node.machine.store().snapshot()?, snapshot);
    Ok(())
}

fn discovery_does_not_decode_config_values_or_borrow_queue_authority<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("tenant", "orders")?;
    let namespace = NamespaceName::new("tenant")?;
    let entity = EntityPath::new("orders")?;
    let mut batch = WriteBatch::default();
    batch.push_put(keys::topic_config(&namespace, &entity), vec![255]);
    node.machine.store().apply(batch)?;
    let snapshot = node.machine.store().snapshot()?;
    assert_eq!(
        node.machine.topics_page(None, None, 1)?.topics,
        vec![(namespace.clone(), entity.clone())]
    );
    assert_eq!(node.machine.queue_config(&namespace, &entity)?, None);
    assert!(matches!(
        node.machine.topic_config(&namespace, &entity),
        Err(BrokerError::Codec(_))
    ));
    assert_eq!(node.machine.store().snapshot()?, snapshot);
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    global_and_scoped_pages_exclude_queues_and_subscription_shadows,
    limits_and_scope_errors_precede_scans_and_use_one_lookahead,
    deleted_cursor_and_insertions_resume_exclusively_after_restart,
    exact_page_end_and_empty_ranges_have_no_continuation,
    malformed_topic_keys_are_reported_without_mutation,
    discovery_does_not_decode_config_values_or_borrow_queue_authority,
}
