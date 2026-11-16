//! Generation replacement is a stopped-owner domain fixture, not native traffic
//! or a production deletion/migration API. Requests use genuinely captured binds.

use super::*;
use domain::{
    BrokerError, MessageContent, QueueCapacity, QueueConfig, RawMessage, ReceiveMode,
    capacity_conservation::TestGenesis,
};
use std::{
    error::Error,
    future::Future,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};
use storage::{Key, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

#[path = "../../../domain/tests/support/capacity_oracle.rs"]
mod oracle;

type TestResult = Result<(), Box<dyn Error>>;
#[derive(Clone)]
struct SpyClock(Arc<AtomicUsize>);
impl Clock for SpyClock {
    fn now(&self) -> Timestamp {
        self.0.fetch_add(1, Ordering::SeqCst);
        Timestamp::from_millis(1000)
    }
}
#[derive(Clone)]
struct CountApply<S> {
    inner: S,
    calls: Arc<AtomicUsize>,
    clock_reads: Arc<AtomicUsize>,
}
impl<S: StateStore> StateStore for CountApply<S> {
    fn get(&self, k: &[u8]) -> Result<Option<Value>, StorageError> {
        if k == domain::keys::clock() {
            self.clock_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(k)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn scan_from(&self, p: &[u8], s: &[u8], n: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(p, s, n)
    }
    fn apply(&self, b: WriteBatch) -> Result<(), StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(b)
    }
}
fn permit(notify: &Notify) -> bool {
    // Owner-loop reply follows notification synchronously, so a single poll is
    // an exact permit witness, not a timeout or an invented wake identity.
    let mut ready = pin!(notify.notified());
    matches!(
        ready.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(())
    )
}
fn send() -> CommandKind {
    CommandKind::SendContent {
        content: MessageContent::Raw(RawMessage {
            body: vec![7],
            ..RawMessage::default()
        }),
    }
}
fn receive() -> CommandKind {
    CommandKind::Receive {
        mode: ReceiveMode::PeekLock,
        lock_duration_millis: None,
        session: None,
    }
}

fn captured_stale<P: StoreProvider>(provider: P) -> TestResult {
    let old = TestGenesis::new(
        NamespaceName::new("tenant")?,
        EntityPath::new("orders")?,
        QueueConfig {
            capacity: QueueCapacity::Finite {
                max_size_bytes: 1024 * 1024,
            },
            ..QueueConfig::default()
        },
    )?;
    let next = old.successor_fixture()?;
    assert_eq!(next.owner().generation(), old.owner().generation() + 1);
    let clock = SpyClock(Arc::new(AtomicUsize::new(0)));
    let applies = Arc::new(AtomicUsize::new(0));
    let clock_reads = Arc::new(AtomicUsize::new(0));
    let store = provider.open()?;
    old.initialize(&store)?;
    let observed = CountApply {
        inner: store.clone(),
        calls: applies.clone(),
        clock_reads: clock_reads.clone(),
    };
    let old_broker = Broker::spawn_capacity_test(CapacityTestProposer::new(
        CapacityHarness::open(observed.clone(), old.clone())?,
        clock.clone(),
    ));
    let old_handle = old_broker.handle();
    let captured = old_handle.bind_entity_blocking(
        old.owner().namespace().clone(),
        old.owner().parent().clone(),
    )?;
    old_handle.submit_bound_blocking(captured.clone(), send())?;
    oracle::observe(store.snapshot()?.entries(), old.owner());
    let CommandOutcome::Received(Some(delivery)) =
        old_handle.submit_bound_blocking(captured.clone(), receive())?
    else {
        panic!()
    };
    let sequence = delivery.sequence;
    let token = delivery.lock.unwrap().token;
    oracle::observe(store.snapshot()?.entries(), old.owner());
    // Retire/join the exact old Broker owner before ANY fixture edit/reopen.
    drop(old_broker);
    assert!(matches!(
        old_handle.submit_bound_blocking(captured.clone(), receive()),
        Err(SubmitError::BrokerStopped)
    ));
    drop(old_handle);
    drop(observed);
    drop(store);
    let store = provider.open()?;
    let original_image = store.snapshot()?;
    assert!(
        next.initialize(&store).is_err(),
        "successor is not populated-state adoption"
    );
    assert_eq!(store.snapshot()?, original_image);
    let mut clear = WriteBatch::default();
    for (k, _) in store.snapshot()?.entries() {
        clear.push_delete(k.clone());
    }
    store.apply(clear)?;
    next.initialize(&store)?;
    let observed = CountApply {
        inner: store.clone(),
        calls: applies.clone(),
        clock_reads: clock_reads.clone(),
    };
    let broker = Broker::spawn_capacity_test(CapacityTestProposer::new(
        CapacityHarness::open(observed.clone(), next.clone())?,
        clock.clone(),
    ));
    let handle = broker.handle();
    let current = handle.bind_entity_blocking(
        next.owner().namespace().clone(),
        next.owner().parent().clone(),
    )?;
    assert_ne!(captured.generation(), current.generation());
    let wake = handle
        .watchers
        .watch(next.owner().namespace(), next.owner().parent());
    handle.submit_bound_blocking(current.clone(), send())?;
    oracle::observe(store.snapshot()?.entries(), next.owner());
    assert!(permit(&wake));
    assert!(!permit(&wake));
    let CommandOutcome::Received(Some(replacement)) =
        handle.submit_bound_blocking(current.clone(), receive())?
    else {
        panic!()
    };
    assert_eq!(replacement.sequence, sequence);
    assert_eq!(
        replacement.lock.as_ref().unwrap().token,
        token,
        "the stale receiver's numeric lock also names the new message"
    );
    let before = store.snapshot()?;
    let usage_key = domain::retained_accounting::keys::usage(next.owner());
    oracle::observe(before.entries(), next.owner());
    let usage = store.get(&usage_key)?.unwrap();
    clock.0.store(0, Ordering::SeqCst);
    clock_reads.store(0, Ordering::SeqCst);
    applies.store(0, Ordering::SeqCst);
    let refusal = handle.submit_bound_blocking(
        captured,
        CommandKind::Complete {
            sequence,
            lock_token: token,
        },
    );
    assert!(matches!(
        refusal,
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::StaleEntityBinding
        )))
    ));
    assert_eq!(
        clock.0.load(Ordering::SeqCst),
        0,
        "no host clock before stale authority refusal"
    );
    assert_eq!(applies.load(Ordering::SeqCst), 0);
    assert_eq!(
        clock_reads.load(Ordering::SeqCst),
        0,
        "stale authority precedes stored Clock too"
    );
    assert_eq!(store.snapshot()?, before);
    oracle::observe(store.snapshot()?.entries(), next.owner());
    assert_eq!(store.get(&usage_key)?.unwrap(), usage);
    assert!(store.scan_prefix(&[0xF1], 1)?.is_empty());
    assert!(!permit(&wake));
    // Positive controls use the genuinely captured current binding and the
    // ordinary unrelated-queue path through this SAME concrete test adapter.
    handle.submit_bound_blocking(
        current,
        CommandKind::Complete {
            sequence,
            lock_token: token,
        },
    )?;
    oracle::observe(store.snapshot()?.entries(), next.owner());
    assert_eq!(clock.0.load(Ordering::SeqCst), 1);
    assert_eq!(applies.load(Ordering::SeqCst), 1);
    let other = EntityPath::new("other")?;
    handle.submit_blocking(
        next.owner().namespace().clone(),
        other.clone(),
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let other_binding =
        handle.bind_entity_blocking(next.owner().namespace().clone(), other.clone())?;
    let other_wake = handle.watchers.watch(next.owner().namespace(), &other);
    handle.submit_bound_blocking(other_binding, send())?;
    assert!(permit(&other_wake));
    assert!(!permit(&wake));
    assert_eq!(clock.0.load(Ordering::SeqCst), 3);
    let final_image = store.snapshot()?;
    oracle::observe(final_image.entries(), next.owner());
    drop(broker);
    drop(handle);
    drop(observed);
    drop(store);
    let store = provider.open()?;
    assert_eq!(store.snapshot()?, final_image);
    drop(CapacityHarness::open(store, next)?);
    Ok(())
}
#[test]
fn memory_captured_stale_receiver_before_clock() -> TestResult {
    captured_stale(MemoryProvider::new())
}
#[test]
fn fjall_captured_stale_receiver_before_clock() -> TestResult {
    captured_stale(DurableProvider::temporary()?)
}
