use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use storage::{Key, Mutation, StorageError, StoreSnapshot, Value};

use super::*;

#[derive(Debug, Default)]
struct Observations {
    commits: usize,
    puts: Vec<Key>,
    scans: Vec<(Key, usize)>,
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    fail_next: Arc<AtomicBool>,
    observations: Arc<Mutex<Observations>>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(key)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.observations
            .lock()
            .expect("observations")
            .scans
            .push((prefix.to_vec(), limit));
        self.inner.scan_from(prefix, start, limit)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let mut observed = self.observations.lock().expect("observations");
        observed.commits += 1;
        observed
            .puts
            .extend(batch.mutations().iter().filter_map(|mutation| {
                if let Mutation::Put { key, .. } = mutation {
                    Some(key.clone())
                } else {
                    None
                }
            }));
        drop(observed);
        if self.fail_next.swap(false, Ordering::Relaxed) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected topology failure".into(),
            });
        }
        self.inner.apply(batch)
    }
}

struct ObservedProvider<P> {
    inner: P,
    fail_next: Arc<AtomicBool>,
    observations: Arc<Mutex<Observations>>,
}

impl<P: StoreProvider> StoreProvider for ObservedProvider<P> {
    type Store = ObservedStore<P::Store>;
    fn open(&self) -> Result<Self::Store, StorageError> {
        Ok(ObservedStore {
            inner: self.inner.open()?,
            fail_next: self.fail_next.clone(),
            observations: self.observations.clone(),
        })
    }
}

fn atomic_topology_commits_and_failed_creation_retry_without_partial_state<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fail_next = Arc::new(AtomicBool::new(false));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut fixture = topic(ObservedProvider {
        inner: provider,
        fail_next: fail_next.clone(),
        observations: observations.clone(),
    })?;
    let failure = BrokerError::Storage(StorageError::Backend {
        operation: "commit",
        detail: "injected topology failure".into(),
    });
    fixture.entity = EntityPath::new("retry")?;
    let command = fixture.command(
        10,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    );
    *observations.lock().expect("observations") = Observations::default();
    fail_next.store(true, Ordering::Relaxed);
    reject(&fixture, command.clone(), failure.clone())?;
    assert_eq!(observations.lock().expect("observations").commits, 1);
    let fixture = fixture.restart()?;
    *observations.lock().expect("observations") = Observations::default();
    assert_eq!(
        fixture.machine.apply(&command)?,
        CommandOutcome::TopicCreated
    );
    assert_eq!(observations.lock().expect("observations").commits, 1);
    assert_eq!(
        observations
            .lock()
            .expect("observations")
            .puts
            .iter()
            .filter(|key| {
                **key == keys::entity_incarnation(&fixture.namespace, &fixture.entity)
            })
            .count(),
        1
    );
    let name = SubscriptionName::new("billing")?;
    let entity = fixture.entity.subscription(&name)?;
    let dlq = entity.dead_letter_queue()?;
    let command = fixture.command(
        11,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig::default(),
        },
    );
    *observations.lock().expect("observations") = Observations::default();
    fail_next.store(true, Ordering::Relaxed);
    reject(&fixture, command.clone(), failure)?;
    assert_eq!(observations.lock().expect("observations").commits, 1);
    let before = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    *observations.lock().expect("observations") = Observations::default();
    assert_eq!(
        fixture.machine.apply(&command)?,
        CommandOutcome::SubscriptionCreated
    );
    let observed = observations.lock().expect("observations");
    assert_eq!(observed.commits, 1);
    for key in [
        keys::subscription(&fixture.namespace, &fixture.entity, &name),
        keys::queue_config(&fixture.namespace, &entity),
        keys::queue_config(&fixture.namespace, &dlq),
        keys::entity_incarnation(&fixture.namespace, &entity),
        keys::clock(),
    ] {
        assert_eq!(
            observed
                .puts
                .iter()
                .filter(|actual| **actual == key)
                .count(),
            1
        );
    }
    assert!(
        !observed
            .puts
            .contains(&keys::queue_counters(&fixture.namespace, &entity))
    );
    assert!(
        !observed
            .puts
            .contains(&keys::queue_counters(&fixture.namespace, &dlq))
    );
    assert!(
        !observed
            .puts
            .contains(&keys::entity_incarnation(&fixture.namespace, &dlq))
    );
    drop(observed);
    assert_eq!(
        fixture
            .machine
            .subscriptions(&fixture.namespace, &fixture.entity)?
            .len(),
        1
    );
    Ok(())
}

fn membership_reader_uses_one_bounded_lookahead_instead_of_an_unbounded_scan<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let observations = Arc::new(Mutex::new(Observations::default()));
    let fixture = topic(ObservedProvider {
        inner: provider,
        fail_next: Arc::new(AtomicBool::new(false)),
        observations: observations.clone(),
    })?;
    for index in 0..MAX_TOPIC_SUBSCRIPTIONS {
        create(
            &fixture,
            &format!("sub-{index:02}"),
            SubscriptionConfig::default(),
            1,
        )?;
    }
    let prefix = keys::subscription_prefix(&fixture.namespace, &fixture.entity);
    *observations.lock().expect("observations") = Observations::default();
    assert_eq!(
        fixture
            .machine
            .subscriptions(&fixture.namespace, &fixture.entity)?
            .len(),
        MAX_TOPIC_SUBSCRIPTIONS
    );
    assert_eq!(
        observations.lock().expect("observations").scans,
        vec![(prefix.clone(), MAX_TOPIC_SUBSCRIPTIONS + 1)]
    );
    let name = SubscriptionName::new("sub-extra")?;
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::subscription(&fixture.namespace, &fixture.entity, &name),
        codec::encode(&SubscriptionConfig::default())?,
    ))?;
    let before = fixture.machine.store().snapshot()?;
    *observations.lock().expect("observations") = Observations::default();
    assert_eq!(
        fixture
            .machine
            .subscriptions(&fixture.namespace, &fixture.entity),
        Err(BrokerError::SubscriptionLimitExceeded {
            maximum: MAX_TOPIC_SUBSCRIPTIONS
        })
    );
    let observed = observations.lock().expect("observations");
    assert_eq!(observed.scans, vec![(prefix, MAX_TOPIC_SUBSCRIPTIONS + 1)]);
    assert_eq!(observed.commits, 0);
    drop(observed);
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    atomic_topology_commits_and_failed_creation_retry_without_partial_state,
    membership_reader_uses_one_bounded_lookahead_instead_of_an_unbounded_scan,
}
