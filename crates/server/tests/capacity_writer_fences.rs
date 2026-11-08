//! Actual local proposer/broker boundary, including watcher permits. Raw fixture
//! setup is serialized before ownership; every Fjall handle drops on reopen.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Waker},
};

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, EntityPath, LockToken, NamespaceName,
    QueueCapacity, QueueConfig, SequenceNumber, StateMachine, Timestamp, capacity_fence, keys,
    retained_accounting::{CapacityOwner, RetainedUsage, keys::usage, usage_codec},
};
use protocol_amqp::Broker as _;
use server::{Broker, Clock, LocalProposer, ProposeError, SubmitError};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

#[derive(Default)]
struct Counts {
    persisted_clock: AtomicUsize,
    host_clock: AtomicUsize,
    state_reads: AtomicUsize,
    scans: AtomicUsize,
    writes: AtomicUsize,
}
#[derive(Clone)]
struct Observed<S> {
    inner: S,
    counts: Arc<Counts>,
}
impl<S: StateStore> StateStore for Observed<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        if key == [0] {
            self.counts.persisted_clock.fetch_add(1, Ordering::SeqCst);
        }
        if matches!(key.first(), Some(2..=13)) {
            self.counts.state_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.counts.writes.fetch_add(1, Ordering::SeqCst);
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
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.counts.scans.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }
}
struct CountedClock(Arc<Counts>);
impl Clock for CountedClock {
    fn now(&self) -> Timestamp {
        self.0.host_clock.fetch_add(1, Ordering::SeqCst);
        Timestamp::from_millis(100)
    }
}
fn names() -> (NamespaceName, EntityPath, EntityPath) {
    (
        NamespaceName::new("n").unwrap(),
        EntityPath::new("q").unwrap(),
        EntityPath::new("other").unwrap(),
    )
}
fn pending(future: Pin<&mut impl Future<Output = ()>>) {
    assert!(
        future
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}
fn complete() -> CommandKind {
    CommandKind::Complete {
        sequence: SequenceNumber::new(1),
        lock_token: LockToken::new(1),
    }
}
fn send() -> CommandKind {
    CommandKind::Send {
        message_id: "id".into(),
        body: vec![1],
        time_to_live_millis: None,
        session_id: None,
        scheduled_enqueue_at: None,
        envelope: None,
    }
}
fn no_effects(counts: &Counts) {
    assert_eq!(counts.persisted_clock.load(Ordering::SeqCst), 0);
    assert_eq!(counts.host_clock.load(Ordering::SeqCst), 0);
    assert_eq!(counts.state_reads.load(Ordering::SeqCst), 0);
    assert_eq!(counts.writes.load(Ordering::SeqCst), 0);
}

async fn runtime<P: StoreProvider>(provider: P, case: u8) {
    let (namespace, parent, other) = names();
    let owner = CapacityOwner::new(namespace.clone(), parent.clone(), 1).unwrap();
    let config = QueueConfig {
        capacity: QueueCapacity::Finite {
            max_size_bytes: domain::QUEUE_CAPACITY_GRANULARITY_BYTES,
        },
        ..QueueConfig::default()
    };
    let store = provider.open().unwrap();
    let machine = StateMachine::new(store.clone());
    for (entity, config) in [(&parent, config), (&other, QueueConfig::default())] {
        machine
            .apply(&Command::new(
                namespace.clone(),
                entity.clone(),
                Timestamp::UNIX_EPOCH,
                CommandKind::CreateQueue { config },
            ))
            .unwrap();
    }
    let bindings = [owner.parent(), owner.shadow()]
        .iter()
        .map(|entity| machine.bind_entity(&namespace, entity).unwrap())
        .collect::<Vec<_>>();
    let mut batch = WriteBatch::default();
    if case == 3 {
        // Orphan isolated data cannot relabel a historical V3 owner either.
        batch.push_put(
            capacity_fence::data_key(&keys::message(
                &namespace,
                owner.shadow(),
                SequenceNumber::new(1),
            ))
            .unwrap(),
            vec![0xff],
        );
    } else {
        let shadow = QueueConfig {
            max_delivery_count: u32::MAX,
            ..QueueConfig::default()
        };
        batch.push_put(
            keys::queue_config(&namespace, &parent),
            capacity_fence::encode_profile(&owner, &parent, &config).unwrap(),
        );
        batch.push_put(
            keys::queue_config(&namespace, owner.shadow()),
            capacity_fence::encode_profile(&owner, owner.shadow(), &shadow).unwrap(),
        );
        match case {
            1 => batch.push_put(usage(&owner), vec![0xff; 72]),
            2 => batch.push_put(
                usage(&owner),
                usage_codec::encode(&owner, RetainedUsage::new(1, 0, 0, 0).unwrap()).unwrap(),
            ),
            _ => {}
        }
    }
    store.apply(batch).unwrap();
    let before = store.snapshot().unwrap();
    drop(machine);
    drop(store);
    let store = provider.open().unwrap();
    assert_eq!(store.snapshot().unwrap(), before);
    let counts = Arc::new(Counts::default());
    let proposer = LocalProposer::new(
        StateMachine::new(Observed {
            inner: store.clone(),
            counts: Arc::clone(&counts),
        }),
        CountedClock(Arc::clone(&counts)),
    );
    let broker = Broker::spawn(proposer);
    let handle = broker.handle();
    let mut parent_wait = Box::pin(handle.deliverable(&namespace, &parent));
    let mut shadow_wait = Box::pin(handle.deliverable(&namespace, owner.shadow()));
    let mut other_wait = Box::pin(handle.deliverable(&namespace, &other));
    pending(parent_wait.as_mut());
    pending(shadow_wait.as_mut());
    pending(other_wait.as_mut());
    let refusal = match case {
        0 => BrokerError::CapacityUsageMissing,
        1 | 3 => BrokerError::CapacityStateCorrupt,
        _ => BrokerError::CapacityStateUnsupported,
    };
    for binding in bindings {
        for kind in [
            send(),
            complete(),
            CommandKind::ExpireMessages,
            CommandKind::ExpireSessionLocks,
        ] {
            assert_eq!(
                handle
                    .submit(namespace.clone(), binding.target().clone(), kind.clone())
                    .await,
                Err(SubmitError::Propose(ProposeError::Broker(refusal.clone())))
            );
            assert_eq!(
                handle.submit_bound(binding.clone(), kind).await,
                Err(SubmitError::Propose(ProposeError::Broker(refusal.clone())))
            );
            no_effects(&counts);
            assert_eq!(store.snapshot().unwrap(), before);
            pending(parent_wait.as_mut());
            pending(shadow_wait.as_mut());
            pending(other_wait.as_mut());
        }
    }
    // The refusal is not a broker-wide admission gate. A real successful send
    // on the neighboring owner writes and wakes exactly that owner's watcher.
    assert!(matches!(
        handle
            .submit(namespace.clone(), other.clone(), send())
            .await,
        Ok(CommandOutcome::Sent { .. })
    ));
    assert!(counts.persisted_clock.load(Ordering::SeqCst) > 0);
    assert_eq!(counts.host_clock.load(Ordering::SeqCst), 1);
    assert_eq!(counts.writes.load(Ordering::SeqCst), 1);
    assert!(
        other_wait
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    pending(parent_wait.as_mut());
    pending(shadow_wait.as_mut());
    let after = store.snapshot().unwrap();
    drop(parent_wait);
    drop(shadow_wait);
    drop(other_wait);
    drop(handle);
    drop(broker);
    drop(store);
    let reopened = provider.open().unwrap();
    assert_eq!(reopened.snapshot().unwrap(), after);
}

#[tokio::test]
async fn memory_runtime_refuses_before_clock_writes_and_watcher_permits() {
    for case in 0..4 {
        runtime(MemoryProvider::new(), case).await;
    }
}
#[tokio::test]
async fn fjall_runtime_refuses_before_clock_writes_and_watcher_permits_after_reopen() {
    for case in 0..4 {
        runtime(DurableProvider::temporary().unwrap(), case).await;
    }
}

fn stale_bound<P: StoreProvider>(provider: P, corrupt: bool) {
    let (namespace, parent, _) = names();
    let store = provider.open().unwrap();
    let machine = StateMachine::new(store.clone());
    machine
        .apply(&Command::new(
            namespace.clone(),
            parent.clone(),
            Timestamp::UNIX_EPOCH,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ))
        .unwrap();
    let bindings = [parent.clone(), parent.dead_letter_queue().unwrap()]
        .iter()
        .map(|entity| machine.bind_entity(&namespace, entity).unwrap())
        .collect::<Vec<_>>();
    store
        .apply(WriteBatch::default().put(
            keys::entity_metadata(&namespace, &parent),
            if corrupt { vec![255] } else { vec![1, 2, 0, 0] },
        ))
        .unwrap();
    let before = store.snapshot().unwrap();
    drop(machine);
    drop(store);
    let store = provider.open().unwrap();
    let counts = Arc::new(Counts::default());
    let proposer = LocalProposer::new(
        StateMachine::new(Observed {
            inner: store.clone(),
            counts: Arc::clone(&counts),
        }),
        CountedClock(Arc::clone(&counts)),
    );
    for binding in bindings {
        assert_eq!(
            proposer.propose_bound(&binding, complete()),
            Err(ProposeError::Broker(if corrupt {
                BrokerError::EntityMetadataCorrupt
            } else {
                BrokerError::StaleEntityBinding
            }))
        );
        no_effects(&counts);
        assert_eq!(counts.scans.load(Ordering::SeqCst), 0);
        assert_eq!(store.snapshot().unwrap(), before);
    }
}
#[test]
fn stale_and_corrupt_bound_authority_remains_scan_free_before_both_clocks() {
    for corrupt in [false, true] {
        stale_bound(MemoryProvider::new(), corrupt);
        stale_bound(DurableProvider::temporary().unwrap(), corrupt);
    }
}
#[test]
fn capacity_refusals_use_the_existing_internal_error_bridge() {
    for error in [
        BrokerError::CapacityStateUnsupported,
        BrokerError::CapacityStateCorrupt,
        BrokerError::CapacityUsageMissing,
    ] {
        assert_eq!(
            protocol_amqp::condition_for(&error),
            protocol_amqp::INTERNAL_ERROR
        );
        assert!(!protocol_amqp::is_retryable(&error));
    }
}
