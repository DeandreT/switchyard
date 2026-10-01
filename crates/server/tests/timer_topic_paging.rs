//! Topic discovery, cleanup, and broker effects remain independent of queues.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use domain::{
    CommandKind, CommandOutcome, EntityPath, MAX_TOPIC_PAGE_SIZE, NamespaceName, QueueConfig,
    StateMachine, SubscriptionConfig, SubscriptionName, TIMER_SCAN_LIMIT, Timestamp, TopicConfig,
    TopicCursor, codec, keys,
};
use server::{
    Broker, BrokerHandle, Clock, LocalProposer, MAX_QUEUES_PER_SWEEP, MAX_ROUNDS_PER_INDEX,
    MAX_TOPICS_PER_SWEEP, ManualClock, TimerWorker,
};
use storage::{StateStore, StorageError, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;

#[path = "timer_topic_paging/scheduling_cases.rs"]
mod scheduling_cases;
#[path = "timer_topic_paging/wakeup_cases.rs"]
mod wakeup_cases;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Clone, Debug, Eq, PartialEq)]
struct PageScan {
    tag: u8,
    start: Vec<u8>,
    limit: usize,
}

#[derive(Default, Debug)]
struct Observed {
    pages: Mutex<Vec<PageScan>>,
    topic_reads: Mutex<Vec<TopicCursor>>,
    entity_scans: Mutex<Vec<(u8, EntityPath, usize)>>,
    fail_queue_page: AtomicUsize,
    fail_topic_page: AtomicUsize,
    fail_apply: AtomicBool,
    observe_commit_reads: AtomicBool,
    committed: AtomicBool,
    post_commit_reads: AtomicUsize,
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    observed: Arc<Observed>,
}

impl<S: StateStore> ObservedStore<S> {
    fn clear(&self) {
        self.observed.pages.lock().expect("pages").clear();
        self.observed
            .topic_reads
            .lock()
            .expect("topic reads")
            .clear();
        self.observed
            .entity_scans
            .lock()
            .expect("entity scans")
            .clear();
    }

    fn pages(&self, tag: u8) -> Vec<PageScan> {
        self.observed
            .pages
            .lock()
            .expect("pages")
            .iter()
            .filter(|scan| scan.tag == tag)
            .cloned()
            .collect()
    }

    fn topic_reads(&self) -> Vec<TopicCursor> {
        self.observed
            .topic_reads
            .lock()
            .expect("topic reads")
            .clone()
    }

    fn fail_page(&self, tag: u8, matching_scan: usize) {
        let counter = match tag {
            1 => &self.observed.fail_queue_page,
            14 => &self.observed.fail_topic_page,
            _ => panic!("not a discovery tag"),
        };
        counter.store(matching_scan, Ordering::SeqCst);
    }
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        if self.observed.observe_commit_reads.load(Ordering::SeqCst)
            && self.observed.committed.load(Ordering::SeqCst)
        {
            self.observed
                .post_commit_reads
                .fetch_add(1, Ordering::SeqCst);
        }
        if key.starts_with(&keys::topic_config_prefix())
            && let Some((namespace, entity)) = keys::entity_scope_parts(key)
        {
            self.observed
                .topic_reads
                .lock()
                .expect("topic reads")
                .push(TopicCursor {
                    namespace: NamespaceName::new(namespace).expect("stored namespace"),
                    entity: EntityPath::new(entity).expect("stored entity"),
                });
        }
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        if self.observed.fail_apply.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Backend {
                operation: "commit topic publish",
                detail: "injected failure".to_owned(),
            });
        }
        self.inner.apply(batch)?;
        if self.observed.observe_commit_reads.load(Ordering::SeqCst) {
            self.observed.committed.store(true, Ordering::SeqCst);
        }
        Ok(())
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
        if self.observed.observe_commit_reads.load(Ordering::SeqCst)
            && self.observed.committed.load(Ordering::SeqCst)
        {
            self.observed
                .post_commit_reads
                .fetch_add(1, Ordering::SeqCst);
        }
        if prefix == keys::queue_config_prefix() || prefix == keys::topic_config_prefix() {
            let tag = prefix[0];
            self.observed.pages.lock().expect("pages").push(PageScan {
                tag,
                start: start.to_vec(),
                limit,
            });
            let counter = if tag == 1 {
                &self.observed.fail_queue_page
            } else {
                &self.observed.fail_topic_page
            };
            if counter
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok_and(|remaining| remaining == 1)
            {
                return Err(StorageError::Backend {
                    operation: "scan entity page",
                    detail: format!("injected tag {tag} failure"),
                });
            }
        }
        if let Some((_, entity)) = keys::entity_scope_parts(prefix) {
            self.observed
                .entity_scans
                .lock()
                .expect("entity scans")
                .push((prefix[0], EntityPath::new(entity).expect("entity"), limit));
        }
        self.inner.scan_from(prefix, start, limit)
    }
}

#[derive(Clone, Debug)]
struct ProbeClock {
    inner: ManualClock,
    forbidden: Arc<AtomicBool>,
    reads: Arc<AtomicUsize>,
}

impl Clock for ProbeClock {
    fn now(&self) -> Timestamp {
        assert!(
            !self.forbidden.load(Ordering::SeqCst),
            "discovery consulted a disabled clock"
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
            observed: Arc::new(Observed::default()),
        };
        let clock = ProbeClock {
            inner: ManualClock::at(1_000),
            forbidden: Arc::new(AtomicBool::new(false)),
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

    fn handle(&self) -> BrokerHandle {
        self.broker.handle()
    }

    fn submit(
        &self,
        namespace: &str,
        entity: &str,
        kind: CommandKind,
    ) -> TestResult<CommandOutcome> {
        Ok(self.handle().submit_blocking(
            NamespaceName::new(namespace)?,
            EntityPath::new(entity)?,
            kind,
        )?)
    }

    fn topic(&self, namespace: &str, entity: &str) -> TestResult {
        assert_eq!(
            self.submit(
                namespace,
                entity,
                CommandKind::CreateTopic {
                    config: TopicConfig {
                        requires_duplicate_detection: true,
                        ..TopicConfig::default()
                    }
                }
            )?,
            CommandOutcome::TopicCreated
        );
        Ok(())
    }

    fn queue(&self, entity: &str) -> TestResult {
        assert_eq!(
            self.submit(
                "tenant",
                entity,
                CommandKind::CreateQueue {
                    config: QueueConfig::default()
                }
            )?,
            CommandOutcome::QueueCreated
        );
        Ok(())
    }

    fn subscription(
        &self,
        namespace: &str,
        topic: &str,
        name: &str,
        config: SubscriptionConfig,
    ) -> TestResult<EntityPath> {
        let name = SubscriptionName::new(name)?;
        self.submit(
            namespace,
            topic,
            CommandKind::CreateSubscription {
                name: name.clone(),
                config,
            },
        )?;
        Ok(EntityPath::new(topic)?.subscription(&name)?)
    }

    fn seed_history(&self, entity: &str, message_id: &str, expires_at: u64) -> TestResult {
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new(entity)?;
        let expires_at = Timestamp::from_millis(expires_at);
        let mut batch = WriteBatch::default();
        batch.push_put(
            keys::duplicate_history(&namespace, &entity, message_id),
            codec::encode(&expires_at)?,
        );
        batch.push_put(
            keys::duplicate_history_expiry(&namespace, &entity, expires_at, message_id),
            Vec::new(),
        );
        self.store.apply(batch)?;
        Ok(())
    }

    fn history(&self, entity: &str, message_id: &str) -> TestResult<Option<Timestamp>> {
        Ok(self
            .store
            .get(&keys::duplicate_history(
                &NamespaceName::new("tenant")?,
                &EntityPath::new(entity)?,
                message_id,
            ))?
            .map(|bytes| codec::decode(&bytes))
            .transpose()?)
    }

    fn poison_topic(&self, entity: &str) -> TestResult {
        let mut batch = WriteBatch::default();
        batch.push_put(
            keys::topic_config(&NamespaceName::new("tenant")?, &EntityPath::new(entity)?),
            vec![255],
        );
        self.store.apply(batch)?;
        Ok(())
    }
}

fn cursor(entity: &str) -> TopicCursor {
    TopicCursor {
        namespace: NamespaceName::new("tenant").expect("namespace"),
        entity: EntityPath::new(entity).expect("entity"),
    }
}

async fn owner_page_queries_do_not_stamp_or_read_a_regressed_disabled_clock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    for namespace in ["tenant", "tenant-other"] {
        for entity in ["a", "b"] {
            node.topic(namespace, entity)?;
        }
    }
    node.queue("queue")?;
    node.subscription("tenant", "a", "sub", SubscriptionConfig::default())?;
    let handle = node.handle();
    let snapshot = node.store.snapshot()?;
    let last = handle.last_applied_blocking()?;
    let clock_reads = node.clock.reads.load(Ordering::SeqCst);
    node.clock.inner.set(0);
    node.clock.forbidden.store(true, Ordering::SeqCst);
    let scope = Some(NamespaceName::new("tenant")?);
    let first = handle.topics_page_blocking(scope.clone(), None, 1)?;
    assert_eq!(
        first.topics,
        vec![(NamespaceName::new("tenant")?, EntityPath::new("a")?)]
    );
    assert_eq!(first.continuation, Some(cursor("a")));
    let remaining = handle.topics_page(scope, first.continuation, 8).await?;
    assert_eq!(
        remaining.topics,
        vec![(NamespaceName::new("tenant")?, EntityPath::new("b")?)]
    );
    assert_eq!(remaining.continuation, None);
    assert_eq!(handle.topics_page(None, None, 0).await?.topics, Vec::new());
    assert!(
        handle
            .topics_page(None, None, MAX_TOPIC_PAGE_SIZE + 1)
            .await
            .is_err()
    );
    assert_eq!(handle.last_applied_blocking()?, last);
    assert_eq!(node.clock.reads.load(Ordering::SeqCst), clock_reads);
    assert_eq!(node.store.snapshot()?, snapshot);
    drop(node);
    assert!(handle.topics_page(None, None, 1).await.is_err());
    assert!(handle.topics_page_blocking(None, None, 1).is_err());
    Ok(())
}

fn independent_pages_wrap_without_starving_either_entity_kind<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    for index in 0..=MAX_TOPICS_PER_SWEEP {
        node.topic("tenant", &format!("topic-{index:04}"))?;
    }
    for index in 0..=MAX_QUEUES_PER_SWEEP / 2 {
        node.queue(&format!("queue-{index:04}"))?;
    }
    let handle = node.handle();
    let worker = TimerWorker::new(&handle);
    node.store.clear();
    let first = worker.sweep_once()?;
    assert_eq!(first.topics_swept, MAX_TOPICS_PER_SWEEP);
    assert_eq!(first.queues_swept, MAX_QUEUES_PER_SWEEP);
    assert!(first.is_idle());
    let reads = node.store.topic_reads();
    assert_eq!(reads.len(), MAX_TOPICS_PER_SWEEP * 3 + MAX_QUEUES_PER_SWEEP);
    assert_eq!(
        reads
            .iter()
            .filter(|read| read.entity.as_str().starts_with("queue-"))
            .count(),
        MAX_QUEUES_PER_SWEEP
    );
    assert_eq!(
        reads
            .iter()
            .filter(|read| read.entity.as_str().starts_with("topic-"))
            .count(),
        MAX_TOPICS_PER_SWEEP * 3
    );
    assert_eq!(node.store.pages(14).len(), 1);
    assert_eq!(node.store.pages(1).len(), 1);
    let scans = node
        .store
        .observed
        .entity_scans
        .lock()
        .expect("entity scans");
    assert!(
        scans
            .iter()
            .filter(|(_, entity, _)| entity.as_str().starts_with("topic-"))
            .all(|(tag, _, limit)| match tag {
                11 | 13 => *limit == TIMER_SCAN_LIMIT,
                15 => *limit == domain::MAX_TOPIC_SUBSCRIPTIONS + 1,
                _ => false,
            })
    );
    drop(scans);
    node.store.clear();
    let tail = worker.sweep_once()?;
    assert_eq!(tail.topics_swept, 1);
    assert_eq!(tail.queues_swept, 2);
    let mut expected = vec![
        cursor(&format!("queue-{:04}", MAX_QUEUES_PER_SWEEP / 2)),
        cursor(&format!(
            "queue-{:04}/$deadletterqueue",
            MAX_QUEUES_PER_SWEEP / 2
        )),
    ];
    expected.extend(vec![
        cursor(&format!("topic-{:04}", MAX_TOPICS_PER_SWEEP));
        3
    ]);
    assert_eq!(node.store.topic_reads(), expected);
    node.store.clear();
    let wrapped = worker.sweep_once()?;
    assert_eq!(wrapped.topics_swept, MAX_TOPICS_PER_SWEEP);
    assert_eq!(wrapped.queues_swept, MAX_QUEUES_PER_SWEEP);
    assert_eq!(node.store.pages(14)[0].start, keys::topic_config_prefix());
    assert_eq!(node.store.pages(1)[0].start, keys::queue_config_prefix());
    Ok(())
}

fn discovery_failure_in_one_family_still_cleans_the_other<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("tenant", "topic")?;
    node.queue("queue")?;
    let handle = node.handle();
    let worker = TimerWorker::new(&handle);
    node.seed_history("topic", "first", 1_000)?;
    node.seed_history("queue", "first", 1_000)?;
    node.store.fail_page(1, 1);
    assert!(worker.sweep_once().is_err());
    assert_eq!(node.history("topic", "first")?, None);
    assert!(node.history("queue", "first")?.is_some());
    assert_eq!(worker.sweep_once()?.duplicate_history_expired, 1);
    node.seed_history("topic", "second", 1_000)?;
    node.seed_history("queue", "second", 1_000)?;
    node.store.fail_page(14, 1);
    assert!(worker.sweep_once().is_err());
    assert_eq!(node.history("queue", "second")?, None);
    assert!(node.history("topic", "second")?.is_some());
    assert_eq!(worker.sweep_once()?.duplicate_history_expired, 1);
    node.store.fail_page(1, 1);
    node.store.fail_page(14, 1);
    let error = worker
        .sweep_once()
        .expect_err("both family discovery reads fail");
    assert!(error.to_string().contains("tag 1 failure"));
    assert_eq!(
        node.store.observed.fail_topic_page.load(Ordering::SeqCst),
        0
    );
    let mut corrupt = WriteBatch::default();
    corrupt.push_put(
        keys::queue_config(&NamespaceName::new("tenant")?, &EntityPath::new("queue")?),
        vec![255],
    );
    node.store.apply(corrupt)?;
    node.seed_history("topic", "after-queue-command-failure", 1_000)?;
    assert!(matches!(
        worker.sweep_once(),
        Err(server::SubmitError::Propose(server::ProposeError::Broker(
            domain::BrokerError::Codec(_)
        )))
    ));
    assert_eq!(node.history("topic", "after-queue-command-failure")?, None);
    Ok(())
}

fn corrupt_topic_and_transient_discovery_advance_only_the_attempted_cursor<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("tenant", "a-poison")?;
    node.topic("tenant", "z-due")?;
    node.queue("queue")?;
    node.poison_topic("a-poison")?;
    node.seed_history("queue", "first", 1_000)?;
    node.seed_history("z-due", "due", 1_000)?;
    let handle = node.handle();
    let worker = TimerWorker::new(&handle);
    node.store.clear();
    assert!(worker.sweep_once().is_err());
    assert_eq!(
        node.store.topic_reads(),
        vec![
            cursor("queue"),
            cursor("queue/$deadletterqueue"),
            cursor("a-poison")
        ]
    );
    assert_eq!(node.history("queue", "first")?, None);
    assert!(node.history("z-due", "due")?.is_some());
    node.seed_history("queue", "second", 1_000)?;
    node.store.clear();
    node.store.fail_page(14, 1);
    assert!(worker.sweep_once().is_err());
    assert_eq!(node.history("queue", "second")?, None);
    let failed_start = node.store.pages(14)[0].start.clone();
    let mut expected_start = keys::topic_config(
        &NamespaceName::new("tenant")?,
        &EntityPath::new("a-poison")?,
    );
    expected_start.push(0);
    assert_eq!(failed_start, expected_start);
    node.store.clear();
    let resumed = worker.sweep_once()?;
    assert_eq!(resumed.topics_swept, 1);
    assert_eq!(resumed.duplicate_history_expired, 1);
    assert_eq!(node.store.pages(14)[0].start, failed_start);
    assert_eq!(
        node.store.topic_reads(),
        vec![
            cursor("queue"),
            cursor("queue/$deadletterqueue"),
            cursor("z-due"),
            cursor("z-due"),
            cursor("z-due")
        ]
    );
    assert_eq!(node.history("z-due", "due")?, None);
    Ok(())
}

fn deleted_cursor_empty_page_wraps_even_after_a_failed_wrap<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("tenant", "only")?;
    node.queue("queue")?;
    node.poison_topic("only")?;
    let handle = node.handle();
    let worker = TimerWorker::new(&handle);
    assert!(worker.sweep_once().is_err());
    let mut batch = WriteBatch::default();
    batch.push_delete(keys::topic_config(
        &NamespaceName::new("tenant")?,
        &EntityPath::new("only")?,
    ));
    node.store.apply(batch)?;
    node.store.clear();
    node.store.fail_page(14, 2);
    assert!(worker.sweep_once().is_err());
    let failed = node.store.pages(14);
    assert_eq!(failed.len(), 2);
    assert_eq!(failed[1].start, keys::topic_config_prefix());
    node.store.clear();
    node.seed_history("queue", "due", 1_000)?;
    let report = worker.sweep_once()?;
    assert_eq!(report.topics_swept, 0);
    assert_eq!(report.duplicate_history_expired, 1);
    assert_eq!(node.store.pages(14), failed);
    Ok(())
}

fn topic_cleanup_bounds_backlogs_and_preserves_fresh_history<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.topic("tenant", "topic")?;
    node.queue("queue")?;
    let namespace = NamespaceName::new("tenant")?;
    let entity = EntityPath::new("topic")?;
    let maximum = MAX_ROUNDS_PER_INDEX * TIMER_SCAN_LIMIT;
    let mut batch = WriteBatch::default();
    for index in 0..=maximum {
        let id = format!("id-{index:04}");
        batch.push_put(
            keys::duplicate_history(&namespace, &entity, &id),
            codec::encode(&Timestamp::from_millis(1_000))?,
        );
        batch.push_put(
            keys::duplicate_history_expiry(&namespace, &entity, Timestamp::from_millis(1_000), &id),
            Vec::new(),
        );
    }
    node.store.apply(batch)?;
    node.seed_history("queue", "queue-id", 1_000)?;
    node.seed_history("topic", "fresh", 3_000)?;
    let mut stale = WriteBatch::default();
    stale.push_put(
        keys::duplicate_history_expiry(&namespace, &entity, Timestamp::from_millis(1_000), "fresh"),
        Vec::new(),
    );
    node.store.apply(stale)?;
    let handle = node.handle();
    let worker = TimerWorker::new(&handle);
    node.store.clear();
    let first = worker.sweep_once()?;
    assert_eq!(first.duplicate_history_expired as usize, maximum + 1);
    assert_eq!(node.history("queue", "queue-id")?, None);
    assert_eq!(
        node.history("topic", "fresh")?,
        Some(Timestamp::from_millis(3_000))
    );
    assert!(
        node.history("topic", &format!("id-{maximum:04}"))?
            .is_some()
    );
    let scans = node
        .store
        .observed
        .entity_scans
        .lock()
        .expect("entity scans");
    assert_eq!(
        scans
            .iter()
            .filter(|(tag, entity, _)| *tag == 13 && entity.as_str() == "topic")
            .count(),
        MAX_ROUNDS_PER_INDEX
    );
    drop(scans);
    assert_eq!(worker.sweep_once()?.duplicate_history_expired, 2);
    assert_eq!(node.history("topic", &format!("id-{maximum:04}"))?, None);
    assert_eq!(
        node.history("topic", "fresh")?,
        Some(Timestamp::from_millis(3_000))
    );
    assert_eq!(worker.sweep_once()?.duplicate_history_expired, 0);
    node.clock.inner.set(3_000);
    assert_eq!(worker.sweep_once()?.duplicate_history_expired, 1);
    assert_eq!(node.history("topic", "fresh")?, None);
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    independent_pages_wrap_without_starving_either_entity_kind,
    discovery_failure_in_one_family_still_cleans_the_other,
    corrupt_topic_and_transient_discovery_advance_only_the_attempted_cursor,
    deleted_cursor_empty_page_wraps_even_after_a_failed_wrap,
    topic_cleanup_bounds_backlogs_and_preserves_fresh_history,
}

#[tokio::test]
async fn memory_owner_pages_do_not_stamp() -> TestResult {
    owner_page_queries_do_not_stamp_or_read_a_regressed_disabled_clock(
        testkit::MemoryProvider::new(),
    )
    .await
}

#[tokio::test]
async fn durable_owner_pages_do_not_stamp() -> TestResult {
    owner_page_queries_do_not_stamp_or_read_a_regressed_disabled_clock(
        testkit::DurableProvider::temporary()?,
    )
    .await
}
