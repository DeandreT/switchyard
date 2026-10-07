//! Timer pages remain fair across the full queue keyspace and failed ticks.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use domain::{
    CommandKind, CommandOutcome, DeleteEntityTarget, EntityPath, NamespaceName, QueueConfig,
    QueueCursor, ScheduledMessage, StateMachine, Timestamp, codec, keys,
};
use server::{Broker, LocalProposer, MAX_QUEUES_PER_SWEEP, ManualClock, TimerWorker};
use storage::{StateStore, StorageError, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type PageScan = (Vec<u8>, usize);

#[derive(Default)]
struct Observations {
    attempted: Mutex<Vec<QueueCursor>>,
    profile_reads: Mutex<Vec<QueueCursor>>,
    in_profile: AtomicBool,
    fail_scheduled_scan: Mutex<Option<Vec<u8>>>,
    pages: Mutex<Vec<PageScan>>,
    fail_after_pages: AtomicUsize,
}

#[derive(Clone)]
struct ObservedStore<S> {
    inner: S,
    observed: Arc<Observations>,
}

impl<S: StateStore> ObservedStore<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            observed: Arc::new(Observations::default()),
        }
    }

    fn clear(&self) {
        self.observed.attempted.lock().expect("attempts").clear();
        self.observed
            .profile_reads
            .lock()
            .expect("profile reads")
            .clear();
        self.observed.in_profile.store(false, Ordering::SeqCst);
        self.observed.pages.lock().expect("pages").clear();
    }

    fn attempted(&self) -> Vec<QueueCursor> {
        let mut attempted = self.observed.attempted.lock().expect("attempts").clone();
        attempted.dedup();
        attempted
    }

    fn raw_attempted(&self) -> Vec<QueueCursor> {
        self.observed.attempted.lock().expect("attempts").clone()
    }

    fn profile_reads(&self) -> Vec<QueueCursor> {
        self.observed
            .profile_reads
            .lock()
            .expect("profile reads")
            .clone()
    }

    fn fail_scheduled_scan(&self, namespace: &str, entity: &str) -> TestResult {
        *self
            .observed
            .fail_scheduled_scan
            .lock()
            .expect("scheduled failure") = Some(keys::scheduled_prefix(
            &NamespaceName::new(namespace)?,
            &EntityPath::new(entity)?,
        ));
        Ok(())
    }

    fn pages(&self) -> Vec<PageScan> {
        self.observed.pages.lock().expect("pages").clone()
    }

    fn fail_page(&self, matching_scan: usize) {
        self.observed
            .fail_after_pages
            .store(matching_scan, Ordering::SeqCst);
    }
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        // In these ordinary timer handlers, the incarnation starts capacity.finish.
        // The clock read resets the boundary for every command, not every sweep.
        if key == keys::clock().as_slice() {
            self.observed.in_profile.store(false, Ordering::SeqCst);
        }
        if let Some((namespace, entity)) = keys::entity_scope_parts(key) {
            let namespace = NamespaceName::new(namespace).expect("stored namespace");
            let entity = EntityPath::new(entity).expect("stored entity");
            if key == keys::entity_incarnation(&namespace, &entity).as_slice() {
                self.observed.in_profile.store(true, Ordering::SeqCst);
            }
            if key.starts_with(&keys::queue_config_prefix()) {
                let reads = if self.observed.in_profile.load(Ordering::SeqCst) {
                    &self.observed.profile_reads
                } else {
                    &self.observed.attempted
                };
                reads
                    .lock()
                    .expect("configuration reads")
                    .push(QueueCursor { namespace, entity });
            }
        }
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
        if prefix == keys::queue_config_prefix() {
            self.observed
                .pages
                .lock()
                .expect("pages")
                .push((start.to_vec(), limit));
            let failure = self
                .observed
                .fail_after_pages
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok_and(|remaining| remaining == 1);
            if failure {
                return Err(StorageError::Backend {
                    operation: "scan queue page",
                    detail: "temporary page failure".to_owned(),
                });
            }
        }
        let mut failure = self
            .observed
            .fail_scheduled_scan
            .lock()
            .expect("scheduled failure");
        if failure.as_deref() == Some(prefix) {
            *failure = None;
            return Err(StorageError::Backend {
                operation: "scan scheduled messages",
                detail: "temporary scheduled scan failure".to_owned(),
            });
        }
        drop(failure);
        self.inner.scan_from(prefix, start, limit)
    }
}

struct Node<P: StoreProvider> {
    broker: Broker,
    store: ObservedStore<P::Store>,
    clock: ManualClock,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    fn start(provider: P) -> TestResult<Self> {
        let store = ObservedStore::new(provider.open()?);
        let clock = ManualClock::at(1_000);
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

    fn submit(
        &self,
        namespace: &str,
        entity: &str,
        kind: CommandKind,
    ) -> TestResult<CommandOutcome> {
        Ok(self.broker.handle().submit_blocking(
            NamespaceName::new(namespace)?,
            EntityPath::new(entity)?,
            kind,
        )?)
    }

    fn create(&self, namespace: &str, entity: &str) -> TestResult {
        assert_eq!(
            self.submit(
                namespace,
                entity,
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            )?,
            CommandOutcome::QueueCreated
        );
        Ok(())
    }

    fn delete(&self, namespace: &str, entity: &str) -> TestResult {
        assert_eq!(
            self.submit(
                namespace,
                entity,
                CommandKind::DeleteEntity {
                    target: DeleteEntityTarget::Queue
                },
            )?,
            CommandOutcome::QueueDeleted
        );
        Ok(())
    }

    fn schedule(&self, namespace: &str, entity: &str, at: u64) -> TestResult {
        assert!(matches!(
            self.submit(
                namespace,
                entity,
                CommandKind::Schedule {
                    messages: vec![ScheduledMessage {
                        message_id: format!("due-{at}"),
                        body: b"scheduled payload".to_vec(),
                        time_to_live_millis: None,
                        session_id: None,
                        enqueue_at: Timestamp::from_millis(at),
                    }],
                },
            )?,
            CommandOutcome::Scheduled { .. }
        ));
        Ok(())
    }

    fn replace_config(&self, namespace: &str, entity: &str, value: Option<Vec<u8>>) -> TestResult {
        let key = keys::queue_config(&NamespaceName::new(namespace)?, &EntityPath::new(entity)?);
        let mut batch = WriteBatch::default();
        match value {
            Some(value) => batch.push_put(key, value),
            None => batch.push_delete(key),
        }
        self.store.apply(batch)?;
        Ok(())
    }
}

fn cursors(queues: &[(NamespaceName, EntityPath)]) -> Vec<QueueCursor> {
    queues
        .iter()
        .map(|(namespace, entity)| QueueCursor {
            namespace: namespace.clone(),
            entity: entity.clone(),
        })
        .collect()
}

fn cursor(namespace: &str, entity: &str) -> QueueCursor {
    QueueCursor {
        namespace: NamespaceName::new(namespace).expect("namespace"),
        entity: EntityPath::new(entity).expect("entity"),
    }
}

fn every_configuration_gets_a_turn_across_namespaces<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider)?;
    for index in 0..513 {
        let namespace = if index < 256 { "tenant-a" } else { "tenant-b" };
        node.create(namespace, &format!("queue-{index:04}"))?;
    }
    node.schedule("tenant-b", "queue-0512", 1_001)?;
    let handle = node.broker.handle();
    let expected = cursors(&handle.queues_blocking(1_026)?);
    assert_eq!(expected.len(), 1_026);
    node.clock.set(1_001);
    let worker = TimerWorker::new(&handle);

    node.store.clear();
    let first = worker.sweep_once()?;
    assert_eq!(first.queues_swept, MAX_QUEUES_PER_SWEEP);
    assert_eq!(first.messages_activated, 0);
    assert_eq!(node.store.attempted(), expected[..MAX_QUEUES_PER_SWEEP]);
    assert_eq!(node.store.pages().len(), 1);

    node.store.clear();
    let second = worker.sweep_once()?;
    assert_eq!(second.queues_swept, 2);
    assert_eq!(second.messages_activated, 1);
    assert_eq!(node.store.attempted(), expected[MAX_QUEUES_PER_SWEEP..]);
    assert_eq!(node.store.pages().len(), 1);

    node.store.clear();
    let wrapped = worker.sweep_once()?;
    assert_eq!(wrapped.queues_swept, MAX_QUEUES_PER_SWEEP);
    assert!(wrapped.is_idle());
    assert_eq!(node.store.attempted(), expected[..MAX_QUEUES_PER_SWEEP]);
    assert_eq!(node.store.pages().len(), 1);
    Ok(())
}

fn an_exact_full_page_wraps_without_an_empty_discovery_tick<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    for index in 0..512 {
        node.create("tenant", &format!("queue-{index:04}"))?;
    }
    let handle = node.broker.handle();
    let worker = TimerWorker::new(&handle);
    node.store.clear();
    assert_eq!(worker.sweep_once()?.queues_swept, MAX_QUEUES_PER_SWEEP);
    assert_eq!(node.store.pages().len(), 1);

    node.schedule("tenant", "queue-0000", 1_001)?;
    node.clock.set(1_001);
    node.store.clear();
    let wrapped = worker.sweep_once()?;
    assert_eq!(wrapped.queues_swept, MAX_QUEUES_PER_SWEEP);
    assert_eq!(wrapped.messages_activated, 1);
    assert_eq!(
        node.store.pages(),
        vec![(keys::queue_config_prefix(), 1_025)]
    );
    Ok(())
}

fn failed_queues_and_deleted_cursors_do_not_starve_later_queues<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.create("tenant", "m-poison")?;
    node.create("tenant", "z-due")?;
    node.schedule("tenant", "z-due", 1_001)?;
    node.replace_config("tenant", "m-poison", Some(vec![0xff]))?;
    node.clock.set(1_001);
    let handle = node.broker.handle();
    let worker = TimerWorker::new(&handle);
    node.store.clear();
    assert!(worker.sweep_once().is_err());
    assert_eq!(node.store.attempted(), vec![cursor("tenant", "m-poison")]);

    node.create("tenant", "a-before")?;
    node.create("tenant", "n-after")?;
    node.schedule("tenant", "a-before", 1_002)?;
    node.schedule("tenant", "n-after", 1_002)?;
    node.replace_config(
        "tenant",
        "m-poison",
        Some(codec::encode(&QueueConfig::default())?),
    )?;
    node.delete("tenant", "m-poison")?;
    node.clock.set(1_002);
    node.store.clear();
    let resumed = worker.sweep_once()?;
    assert_eq!(resumed.queues_swept, 4);
    assert_eq!(resumed.messages_activated, 2);
    assert_eq!(
        node.store.attempted(),
        vec![
            cursor("tenant", "n-after"),
            cursor("tenant", "n-after/$deadletterqueue"),
            cursor("tenant", "z-due"),
            cursor("tenant", "z-due/$deadletterqueue"),
        ]
    );
    node.store.clear();
    let wrapped = worker.sweep_once()?;
    assert_eq!(wrapped.queues_swept, 6);
    assert_eq!(wrapped.messages_activated, 1);
    assert_eq!(node.store.attempted()[0], cursor("tenant", "a-before"));
    Ok(())
}

fn a_transient_discovery_error_keeps_the_last_attempted_cursor<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.create("tenant", "m-poison")?;
    node.create("tenant", "z-due")?;
    node.schedule("tenant", "z-due", 1_001)?;
    node.replace_config("tenant", "m-poison", Some(vec![0xff]))?;
    node.clock.set(1_001);
    let handle = node.broker.handle();
    let worker = TimerWorker::new(&handle);
    assert!(worker.sweep_once().is_err());
    let before = node.store.snapshot()?;
    node.store.clear();
    node.store.fail_page(1);
    assert!(worker.sweep_once().is_err());
    assert!(node.store.attempted().is_empty());
    assert_eq!(node.store.snapshot()?, before);
    let failed_start = node.store.pages()[0].0.clone();

    node.replace_config(
        "tenant",
        "m-poison",
        Some(codec::encode(&QueueConfig::default())?),
    )?;
    node.store.clear();
    let resumed = worker.sweep_once()?;
    assert_eq!(resumed.queues_swept, 3);
    assert_eq!(resumed.messages_activated, 1);
    assert_eq!(node.store.pages()[0].0, failed_start);
    assert_eq!(
        node.store.attempted(),
        vec![
            cursor("tenant", "m-poison/$deadletterqueue"),
            cursor("tenant", "z-due"),
            cursor("tenant", "z-due/$deadletterqueue"),
        ]
    );
    Ok(())
}

fn an_empty_exclusive_page_wraps_in_the_same_tick_even_after_a_failed_wrap<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.create("tenant", "only")?;
    node.store
        .fail_scheduled_scan("tenant", "only/$deadletterqueue")?;
    let handle = node.broker.handle();
    let worker = TimerWorker::new(&handle);
    let healthy = node.store.snapshot()?;
    node.store.clear();
    assert!(worker.sweep_once().is_err());
    assert_eq!(
        node.store.attempted(),
        vec![
            cursor("tenant", "only"),
            cursor("tenant", "only/$deadletterqueue")
        ]
    );
    assert_eq!(node.store.snapshot()?, healthy);
    node.delete("tenant", "only")?;
    node.clock.set(1_001);
    let before = node.store.snapshot()?;
    node.store.clear();
    node.store.fail_page(2);
    assert!(worker.sweep_once().is_err());
    assert!(node.store.attempted().is_empty());
    assert_eq!(node.store.snapshot()?, before);
    let failed_pages = node.store.pages();
    assert_eq!(failed_pages.len(), 2);
    let mut exclusive_shadow = keys::queue_config(
        &NamespaceName::new("tenant")?,
        &EntityPath::new("only/$deadletterqueue")?,
    );
    exclusive_shadow.push(0);
    assert_eq!(failed_pages[0].0, exclusive_shadow);
    assert_eq!(failed_pages[1].0, keys::queue_config_prefix());

    node.create("tenant", "only")?;
    node.schedule("tenant", "only", 1_002)?;
    node.clock.set(1_002);
    node.store.clear();
    let wrapped = worker.sweep_once()?;
    assert_eq!(wrapped.queues_swept, 2);
    assert_eq!(wrapped.messages_activated, 1);
    assert_eq!(node.store.pages(), failed_pages);
    assert_eq!(
        node.store.attempted(),
        vec![
            cursor("tenant", "only"),
            cursor("tenant", "only/$deadletterqueue")
        ]
    );
    Ok(())
}

fn profile_reads_do_not_hide_later_commands_or_extra_handler_reads<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.create("tenant", "one")?;
    node.create("tenant", "two")?;
    let before = node.store.snapshot()?;
    let namespace = NamespaceName::new("tenant")?;
    let one = EntityPath::new("one")?;
    let two = EntityPath::new("two")?;
    let shadow = one.dead_letter_queue()?;
    node.store.clear();
    node.store.get(&keys::clock())?;
    node.store.get(&keys::queue_config(&namespace, &one))?;
    node.store.get(&keys::queue_config(&namespace, &one))?;
    node.store
        .get(&keys::entity_incarnation(&namespace, &one))?;
    node.store.get(&keys::queue_config(&namespace, &shadow))?;
    node.store.get(&keys::clock())?;
    node.store.get(&keys::queue_config(&namespace, &two))?;
    assert_eq!(
        node.store.raw_attempted(),
        vec![
            cursor("tenant", "one"),
            cursor("tenant", "one"),
            cursor("tenant", "two")
        ]
    );
    assert_eq!(
        node.store.profile_reads(),
        vec![cursor("tenant", "one/$deadletterqueue")]
    );
    assert_eq!(node.store.snapshot()?, before);
    Ok(())
}

async fn owner_page_queries_forward_scope_and_cursor_without_stamping<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.create("tenant", "one")?;
    node.create("tenant", "two")?;
    node.create("tenant-neighbor", "three")?;
    let handle = node.broker.handle();
    let before = node.store.snapshot()?;
    node.clock.set(0);
    let scope = Some(NamespaceName::new("tenant")?);
    let first = handle.queues_page_blocking(scope.clone(), None, 1)?;
    assert_eq!(cursors(&first.queues), vec![cursor("tenant", "one")]);
    assert_eq!(first.continuation, Some(cursor("tenant", "one")));
    let rest = handle.queues_page(scope, first.continuation, 8).await?;
    assert_eq!(
        cursors(&rest.queues),
        vec![
            cursor("tenant", "one/$deadletterqueue"),
            cursor("tenant", "two"),
            cursor("tenant", "two/$deadletterqueue"),
        ]
    );
    assert_eq!(rest.continuation, None);
    assert_eq!(handle.queues_blocking(8)?.len(), 6);
    assert!(handle.queues_page(None, None, 1_025).await.is_err());
    assert_eq!(node.store.snapshot()?, before);
    assert_eq!(
        handle.last_applied_blocking()?,
        Timestamp::from_millis(1_000)
    );
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(
            #[test]
            fn $case() -> super::TestResult {
                super::$case(::testkit::MemoryProvider::new())
            }
        )+ }
        mod durable { $(
            #[test]
            fn $case() -> super::TestResult {
                super::$case(::testkit::DurableProvider::temporary()?)
            }
        )+ }
    };
}

for_each_backend! {
    every_configuration_gets_a_turn_across_namespaces,
    an_exact_full_page_wraps_without_an_empty_discovery_tick,
    failed_queues_and_deleted_cursors_do_not_starve_later_queues,
    a_transient_discovery_error_keeps_the_last_attempted_cursor,
    an_empty_exclusive_page_wraps_in_the_same_tick_even_after_a_failed_wrap,
    profile_reads_do_not_hide_later_commands_or_extra_handler_reads,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_owner_page_queries_forward_scope_and_cursor_without_stamping() -> TestResult {
    owner_page_queries_forward_scope_and_cursor_without_stamping(testkit::MemoryProvider::new())
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_owner_page_queries_forward_scope_and_cursor_without_stamping() -> TestResult {
    owner_page_queries_forward_scope_and_cursor_without_stamping(
        testkit::DurableProvider::temporary()?,
    )
    .await
}
