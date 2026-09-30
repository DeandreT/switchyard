//! Administration reads use the owner without stamping or mutating commands.

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

use domain::{
    BrokerError, CommandKind, CommandOutcome, EntityPath, NamespaceName, QueueConfig,
    QueueConfigUpdate, QueueTimeToLiveUpdate, StateMachine, Timestamp, keys,
};
use server::{Broker, Clock, LocalProposer, ManualClock, ProposeError, SubmitError};
use storage::{StateStore, StorageError, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type ReadGate = (flume::Sender<()>, flume::Receiver<()>);

#[derive(Clone)]
struct ObservedClock {
    clock: ManualClock,
    reads: Arc<AtomicUsize>,
}

impl ObservedClock {
    fn new() -> Self {
        Self {
            clock: ManualClock::at(1_000),
            reads: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn read_count(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl Clock for ObservedClock {
    fn now(&self) -> Timestamp {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.clock.now()
    }
}

#[derive(Default)]
struct Observations {
    writes: AtomicUsize,
    threads: Mutex<Vec<String>>,
    fail_key: Mutex<Option<Vec<u8>>>,
    gate: Mutex<Option<ReadGate>>,
}

#[derive(Clone)]
struct ObservedStore<S> {
    inner: S,
    observations: Arc<Observations>,
}

impl<S: StateStore> ObservedStore<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            observations: Arc::new(Observations::default()),
        }
    }

    fn write_count(&self) -> usize {
        self.observations.writes.load(Ordering::SeqCst)
    }

    fn clear_threads(&self) {
        self.observations
            .threads
            .lock()
            .expect("read threads")
            .clear();
    }

    fn assert_owner_reads(&self) {
        let threads = self.observations.threads.lock().expect("read threads");
        assert!(!threads.is_empty());
        assert!(threads.iter().all(|name| name == "switchyard-broker"));
    }

    fn block_next_read(&self) -> (flume::Receiver<()>, flume::Sender<()>) {
        let (entered, waiting) = flume::bounded(1);
        let (release, blocked) = flume::bounded(1);
        *self.observations.gate.lock().expect("read gate") = Some((entered, blocked));
        (waiting, release)
    }
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        if key.starts_with(&keys::queue_config_prefix()) {
            self.observations
                .threads
                .lock()
                .expect("read threads")
                .push(
                    std::thread::current()
                        .name()
                        .unwrap_or("unnamed")
                        .to_owned(),
                );
            let gate = self.observations.gate.lock().expect("read gate").take();
            if let Some((entered, release)) = gate {
                entered.send(()).expect("observe owner read");
                release.recv().expect("release owner read");
            }
            if self
                .observations
                .fail_key
                .lock()
                .expect("failure key")
                .as_deref()
                == Some(key)
            {
                return Err(StorageError::Backend {
                    operation: "read queue configuration",
                    detail: "injected read failure".to_owned(),
                });
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
        self.inner.scan_from(prefix, start, limit)
    }
}

struct Node<P: StoreProvider> {
    broker: Broker,
    store: ObservedStore<P::Store>,
    clock: ObservedClock,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    fn start(provider: P) -> TestResult<Self> {
        let store = ObservedStore::new(provider.open()?);
        let clock = ObservedClock::new();
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

    fn create(&self, namespace: &NamespaceName, entity: &EntityPath, config: QueueConfig) {
        assert_eq!(
            self.broker
                .handle()
                .submit_blocking(
                    namespace.clone(),
                    entity.clone(),
                    CommandKind::CreateQueue { config },
                )
                .expect("create queue"),
            CommandOutcome::QueueCreated
        );
    }

    fn assert_unchanged(&self, snapshot: &StoreSnapshot, writes: usize, clock_reads: usize) {
        assert_eq!(&self.store.snapshot().expect("snapshot"), snapshot);
        assert_eq!(self.store.write_count(), writes);
        assert_eq!(self.clock.read_count(), clock_reads);
    }
}

fn names() -> (NamespaceName, EntityPath) {
    (
        NamespaceName::new("tenant").expect("namespace"),
        EntityPath::new("orders").expect("entity"),
    )
}

async fn missing_existing_scoped_and_shadow_reads<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider)?;
    let handle = node.broker.handle();
    let (namespace, entity) = names();
    let neighbor = NamespaceName::new("neighbor")?;
    let config = QueueConfig {
        lock_duration_millis: 90_000,
        max_delivery_count: 17,
        default_time_to_live_millis: Some(5_000),
        max_message_bytes: 65_536,
        requires_session: true,
        requires_duplicate_detection: true,
        dead_lettering_on_message_expiration: true,
        ..QueueConfig::default()
    };
    node.create(&namespace, &entity, config);
    node.create(&neighbor, &entity, QueueConfig::default());
    let shadow = entity.dead_letter_queue()?;
    let before = node.store.snapshot()?;
    let writes = node.store.write_count();
    let clock_reads = node.clock.read_count();
    node.clock.clock.set(0);
    node.store.clear_threads();

    assert_eq!(
        handle.queue_config_blocking(namespace.clone(), entity.clone())?,
        Some(config)
    );
    assert_eq!(
        handle
            .queue_config(namespace.clone(), entity.clone())
            .await?,
        Some(config)
    );
    assert_eq!(
        handle.queue_config(neighbor, entity.clone()).await?,
        Some(QueueConfig::default())
    );
    assert_eq!(
        handle.queue_config_blocking(namespace.clone(), EntityPath::new("missing")?)?,
        None
    );
    assert_eq!(
        handle
            .queue_config(NamespaceName::new("absent")?, entity)
            .await?,
        None
    );
    assert_eq!(
        handle.queue_config(namespace, shadow).await?,
        Some(QueueConfig {
            max_delivery_count: u32::MAX,
            default_time_to_live_millis: None,
            requires_session: false,
            requires_duplicate_detection: false,
            dead_lettering_on_message_expiration: false,
            ..config
        })
    );
    assert_eq!(
        handle.last_applied_blocking()?,
        Timestamp::from_millis(1_000)
    );
    node.assert_unchanged(&before, writes, clock_reads);
    node.store.assert_owner_reads();
    Ok(())
}

async fn queries_observe_committed_updates<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider)?;
    let (namespace, entity) = names();
    let original = QueueConfig {
        default_time_to_live_millis: Some(1_000),
        ..QueueConfig::default()
    };
    node.create(&namespace, &entity, original);
    let handle = node.broker.handle();
    node.clock.clock.set(2_000);
    assert_eq!(
        handle
            .submit(
                namespace.clone(),
                entity.clone(),
                CommandKind::UpdateQueue {
                    update: QueueConfigUpdate {
                        lock_duration_millis: Some(120_000),
                        max_delivery_count: Some(21),
                        default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Unlimited),
                        dead_lettering_on_message_expiration: Some(true),
                        ..QueueConfigUpdate::default()
                    },
                }
            )
            .await?,
        CommandOutcome::QueueUpdated
    );
    let expected = QueueConfig {
        lock_duration_millis: 120_000,
        max_delivery_count: 21,
        default_time_to_live_millis: None,
        dead_lettering_on_message_expiration: true,
        ..original
    };
    let before = node.store.snapshot()?;
    let writes = node.store.write_count();
    let clock_reads = node.clock.read_count();
    node.clock.clock.set(0);
    node.store.clear_threads();
    assert_eq!(
        handle
            .queue_config(namespace.clone(), entity.clone())
            .await?,
        Some(expected)
    );
    assert_eq!(
        handle.queue_config_blocking(namespace, entity)?,
        Some(expected)
    );
    assert_eq!(
        handle.last_applied_blocking()?,
        Timestamp::from_millis(2_000)
    );
    node.assert_unchanged(&before, writes, clock_reads);
    node.store.assert_owner_reads();
    Ok(())
}

async fn failed_reads_are_typed_and_do_not_stamp_or_write<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    let (namespace, entity) = names();
    let config = QueueConfig::default();
    node.create(&namespace, &entity, config);
    let broken = EntityPath::new("broken")?;
    node.store
        .apply(WriteBatch::default().put(keys::queue_config(&namespace, &broken), Vec::new()))?;
    let before = node.store.snapshot()?;
    let writes = node.store.write_count();
    let clock_reads = node.clock.read_count();
    node.clock.clock.set(0);
    node.store.clear_threads();
    let handle = node.broker.handle();
    assert!(matches!(
        handle.queue_config_blocking(namespace.clone(), broken.clone()),
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::Codec(_)
        )))
    ));
    assert!(matches!(
        handle.queue_config(namespace.clone(), broken).await,
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::Codec(_)
        )))
    ));

    *node
        .store
        .observations
        .fail_key
        .lock()
        .expect("failure key") = Some(keys::queue_config(&namespace, &entity));
    let expected = SubmitError::Propose(ProposeError::Broker(BrokerError::Storage(
        StorageError::Backend {
            operation: "read queue configuration",
            detail: "injected read failure".to_owned(),
        },
    )));
    assert_eq!(
        handle.queue_config_blocking(namespace.clone(), entity.clone()),
        Err(expected.clone())
    );
    assert_eq!(
        handle.queue_config(namespace.clone(), entity.clone()).await,
        Err(expected)
    );
    *node
        .store
        .observations
        .fail_key
        .lock()
        .expect("failure key") = None;
    assert_eq!(handle.queue_config(namespace, entity).await?, Some(config));
    assert_eq!(
        handle.last_applied_blocking()?,
        Timestamp::from_millis(1_000)
    );
    node.assert_unchanged(&before, writes, clock_reads);
    node.store.assert_owner_reads();
    Ok(())
}

async fn canceled_query_reply_does_not_stop_the_owner<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider)?;
    let (namespace, entity) = names();
    node.create(&namespace, &entity, QueueConfig::default());
    let before = node.store.snapshot()?;
    let writes = node.store.write_count();
    let clock_reads = node.clock.read_count();
    node.clock.clock.set(0);
    node.store.clear_threads();
    let handle = node.broker.handle();
    let (entered, release) = node.store.block_next_read();
    let mut query = Box::pin(handle.queue_config(namespace.clone(), entity.clone()));
    std::future::poll_fn(|context| {
        assert!(query.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    tokio::time::timeout(Duration::from_secs(5), entered.recv_async()).await??;
    drop(query);
    release.send(())?;
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(5),
            handle.queue_config(namespace, entity)
        )
        .await??,
        Some(QueueConfig::default())
    );
    node.assert_unchanged(&before, writes, clock_reads);
    node.store.assert_owner_reads();
    Ok(())
}

async fn stopped_owner_reports_unavailable_without_touching_store<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    let (namespace, entity) = names();
    node.create(&namespace, &entity, QueueConfig::default());
    let before = node.store.snapshot()?;
    let writes = node.store.write_count();
    let clock_reads = node.clock.read_count();
    let handle = node.broker.handle();
    let Node {
        broker,
        store,
        clock,
        _provider: provider,
    } = node;
    drop(broker);
    assert_eq!(
        handle.queue_config_blocking(namespace.clone(), entity.clone()),
        Err(SubmitError::BrokerStopped)
    );
    assert_eq!(
        handle.queue_config(namespace, entity).await,
        Err(SubmitError::BrokerStopped)
    );
    assert_eq!(store.snapshot()?, before);
    assert_eq!(store.write_count(), writes);
    assert_eq!(clock.read_count(), clock_reads);
    drop(store);
    drop(provider);
    Ok(())
}

macro_rules! suite {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;

            #[tokio::test]
            async fn reads_are_scoped_pure_and_allow_raw_shadow_queries() -> TestResult {
                missing_existing_scoped_and_shadow_reads($provider).await
            }

            #[tokio::test]
            async fn owner_reads_observe_committed_queue_updates() -> TestResult {
                queries_observe_committed_updates($provider).await
            }

            #[tokio::test]
            async fn read_failures_are_typed_without_store_or_clock_mutation() -> TestResult {
                failed_reads_are_typed_and_do_not_stamp_or_write($provider).await
            }

            #[tokio::test]
            async fn a_canceled_read_reply_keeps_the_owner_available() -> TestResult {
                canceled_query_reply_does_not_stop_the_owner($provider).await
            }

            #[tokio::test]
            async fn a_stopped_owner_refuses_both_query_forms() -> TestResult {
                stopped_owner_reports_unavailable_without_touching_store($provider).await
            }
        }
    };
}

suite!(memory, testkit::MemoryProvider::new());
suite!(durable, testkit::DurableProvider::temporary()?);
