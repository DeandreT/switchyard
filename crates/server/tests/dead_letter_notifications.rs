//! Dead-letter waiters are notified only by committed shadow enqueues.

use std::{
    error::Error,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
    time::Duration,
};

use domain::{
    CommandKind, CommandOutcome, DeadLetterReason, DeliveryBudget, EntityPath, NamespaceName,
    QueueConfig, ReceiveMode, SequenceNumber, SessionHold, SessionId, StateMachine,
};
use protocol_amqp::Broker as _;
use server::{Broker, LocalProposer, ManualClock};
use storage::{StateStore, StorageError, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Clone)]
struct FailingStore<S> {
    inner: S,
    fail_apply: Arc<AtomicBool>,
}

impl<S: StateStore> StateStore for FailingStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        if self.fail_apply.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Backend {
                operation: "commit test batch",
                detail: "transient write failure".to_owned(),
            });
        }
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
    store: FailingStore<P::Store>,
    clock: ManualClock,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    fn start(provider: P) -> TestResult<Self> {
        let store = FailingStore {
            inner: provider.open()?,
            fail_apply: Arc::new(AtomicBool::new(false)),
        };
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

    fn submit(&self, entity: &EntityPath, kind: CommandKind) -> TestResult<CommandOutcome> {
        Ok(self.broker.handle().submit_blocking(
            NamespaceName::new("tenant")?,
            entity.clone(),
            kind,
        )?)
    }

    fn create(&self, entity: &EntityPath, path: Path, dead_letter_expiry: bool) -> TestResult {
        assert_eq!(
            self.submit(
                entity,
                CommandKind::CreateQueue {
                    config: QueueConfig {
                        requires_session: path.requires_session(),
                        dead_lettering_on_message_expiration: dead_letter_expiry,
                        ..QueueConfig::default()
                    },
                },
            )?,
            CommandOutcome::QueueCreated
        );
        Ok(())
    }

    fn seed(&self, entity: &EntityPath, path: Path) -> TestResult<Seed> {
        let session_id = path
            .requires_session()
            .then(|| SessionId::new("held-session"))
            .transpose()?;
        let CommandOutcome::Sent { sequence } = self.submit(
            entity,
            CommandKind::Send {
                message_id: "expired".to_owned(),
                body: b"expired payload".to_vec(),
                time_to_live_millis: Some(5),
                session_id: session_id.clone(),
            },
        )?
        else {
            panic!("the fixture message must be sent");
        };
        let hold = if let Some(session_id) = session_id {
            let CommandOutcome::SessionAccepted(Some(accepted)) = self.submit(
                entity,
                CommandKind::AcceptSession {
                    session_id: Some(session_id),
                    lock_duration_millis: None,
                },
            )?
            else {
                panic!("the fixture session must be held");
            };
            Some(accepted.hold())
        } else {
            None
        };
        if path.is_deferred() {
            let CommandOutcome::Received(Some(delivery)) = self.submit(
                entity,
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: hold.clone(),
                },
            )?
            else {
                panic!("the fixture message must be locked");
            };
            assert_eq!(delivery.sequence, sequence);
            assert_eq!(
                self.submit(
                    entity,
                    CommandKind::Defer {
                        sequence,
                        lock_token: delivery.lock.expect("fixture lock").token,
                    },
                )?,
                CommandOutcome::Deferred
            );
        }
        Ok(Seed { sequence, hold })
    }
}

struct Seed {
    sequence: SequenceNumber,
    hold: Option<SessionHold>,
}

#[derive(Clone, Copy)]
enum Path {
    Ready(ReceiveMode),
    LegacyDeferred(ReceiveMode),
    BoundedDeferred(ReceiveMode),
    HeldDeferred(ReceiveMode),
    HeldSessionDeferred(ReceiveMode),
}

impl Path {
    fn requires_session(self) -> bool {
        matches!(self, Self::HeldSessionDeferred(_))
    }

    fn is_deferred(self) -> bool {
        !matches!(self, Self::Ready(_))
    }

    fn command(self, seed: &Seed, invalid_later_sequence: bool) -> CommandKind {
        let mut sequences = vec![seed.sequence];
        if invalid_later_sequence {
            sequences.push(SequenceNumber::new(999_999));
        }
        let budget = DeliveryBudget {
            max_bytes: 1024 * 1024,
            per_message_overhead_bytes: 64,
        };
        match self {
            Self::Ready(mode) => CommandKind::Receive {
                mode,
                lock_duration_millis: None,
                session: seed.hold.clone(),
            },
            Self::LegacyDeferred(mode) => CommandKind::ReceiveDeferred {
                sequences,
                mode,
                lock_duration_millis: None,
                session_id: None,
            },
            Self::BoundedDeferred(mode) => CommandKind::ReceiveDeferredBounded {
                sequences,
                mode,
                lock_duration_millis: None,
                session_id: None,
                budget,
            },
            Self::HeldDeferred(mode) | Self::HeldSessionDeferred(mode) => {
                CommandKind::ReceiveDeferredHeld {
                    sequences,
                    mode,
                    lock_duration_millis: None,
                    session: seed.hold.clone(),
                    budget,
                }
            }
        }
    }
}

const PATHS: [Path; 10] = [
    Path::Ready(ReceiveMode::PeekLock),
    Path::Ready(ReceiveMode::ReceiveAndDelete),
    Path::LegacyDeferred(ReceiveMode::PeekLock),
    Path::LegacyDeferred(ReceiveMode::ReceiveAndDelete),
    Path::BoundedDeferred(ReceiveMode::PeekLock),
    Path::BoundedDeferred(ReceiveMode::ReceiveAndDelete),
    Path::HeldDeferred(ReceiveMode::PeekLock),
    Path::HeldDeferred(ReceiveMode::ReceiveAndDelete),
    Path::HeldSessionDeferred(ReceiveMode::PeekLock),
    Path::HeldSessionDeferred(ReceiveMode::ReceiveAndDelete),
];

fn assert_empty(outcome: CommandOutcome) {
    assert!(match outcome {
        CommandOutcome::Received(None) => true,
        CommandOutcome::DeferredReceived(deliveries) => deliveries.is_empty(),
        _ => false,
    });
}

async fn assert_pending<F: Future<Output = ()>>(waiting: &mut Pin<Box<F>>) {
    std::future::poll_fn(|context| {
        assert!(
            waiting.as_mut().poll(context).is_pending(),
            "unexpected wakeup"
        );
        Poll::Ready(())
    })
    .await;
}

async fn assert_woken<F: Future<Output = ()>>(waiting: &mut Pin<Box<F>>) {
    tokio::time::timeout(Duration::from_millis(250), waiting.as_mut())
        .await
        .expect("a committed dead letter must wake its shadow without a timer");
}

fn drain_expired<P: StoreProvider>(node: &Node<P>, shadow: &EntityPath) -> TestResult {
    let CommandOutcome::Received(Some(delivery)) = node.submit(
        shadow,
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("the notified dead-letter queue must contain the message");
    };
    assert_eq!(delivery.body, b"expired payload");
    assert_eq!(
        delivery.dead_letter.expect("dead-letter details").reason,
        DeadLetterReason::TimeToLiveExpired
    );
    assert_eq!(delivery.session_id, None);
    Ok(())
}

async fn committed_lazy_expiry_wakes_every_receive_path<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    let handle = node.broker.handle();
    let namespace = NamespaceName::new("tenant")?;
    for (index, path) in PATHS.into_iter().enumerate() {
        let entity = EntityPath::new(format!("expiry-{index}"))?;
        node.create(&entity, path, true)?;
        let seed = node.seed(&entity, path)?;
        let shadow = entity.dead_letter_queue()?;
        let mut waiting = Box::pin(handle.deliverable(&namespace, &shadow));
        node.clock.advance(6);
        assert_empty(node.submit(&entity, path.command(&seed, false))?);
        assert_woken(&mut waiting).await;
        drain_expired(&node, &shadow)?;
    }
    Ok(())
}

async fn drop_policy_and_noops_do_not_wake_but_the_same_watch_can_be_reused<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    let handle = node.broker.handle();
    let namespace = NamespaceName::new("tenant")?;
    for (index, path) in PATHS.into_iter().enumerate() {
        let entity = EntityPath::new(format!("drop-{index}"))?;
        node.create(&entity, path, false)?;
        let shadow = entity.dead_letter_queue()?;
        let mut waiting = Box::pin(handle.deliverable(&namespace, &shadow));
        let before = node.store.snapshot()?;
        assert_empty(node.submit(
            &shadow,
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        )?);
        assert_eq!(node.store.snapshot()?, before);
        assert_pending(&mut waiting).await;

        let seed = node.seed(&entity, path)?;
        node.clock.advance(6);
        assert_empty(node.submit(&entity, path.command(&seed, false))?);
        assert_pending(&mut waiting).await;
        let held = seed.hold;
        node.submit(
            &entity,
            CommandKind::Send {
                message_id: "manual".to_owned(),
                body: b"manual dead letter".to_vec(),
                time_to_live_millis: None,
                session_id: held.as_ref().map(|hold| hold.session_id.clone()),
            },
        )?;
        assert_pending(&mut waiting).await;
        let CommandOutcome::Received(Some(delivery)) = node.submit(
            &entity,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: held,
            },
        )?
        else {
            panic!("the manually dead-lettered message must be live");
        };
        assert_eq!(
            node.submit(
                &entity,
                CommandKind::DeadLetter {
                    sequence: delivery.sequence,
                    lock_token: delivery.lock.expect("delivery lock").token,
                    reason: "manual".to_owned(),
                    description: "explicit dead letter".to_owned(),
                },
            )?,
            CommandOutcome::DeadLettered
        );
        assert_woken(&mut waiting).await;
    }
    Ok(())
}

async fn invalid_later_deferred_records_roll_back_without_waking<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    let handle = node.broker.handle();
    let namespace = NamespaceName::new("tenant")?;
    for (index, path) in PATHS
        .into_iter()
        .enumerate()
        .filter(|(_, path)| path.is_deferred())
    {
        let entity = EntityPath::new(format!("rollback-{index}"))?;
        node.create(&entity, path, true)?;
        let seed = node.seed(&entity, path)?;
        let shadow = entity.dead_letter_queue()?;
        let mut waiting = Box::pin(handle.deliverable(&namespace, &shadow));
        node.clock.advance(6);
        let before = node.store.snapshot()?;
        assert!(node.submit(&entity, path.command(&seed, true)).is_err());
        assert_eq!(node.store.snapshot()?, before);
        assert_pending(&mut waiting).await;
        assert_empty(node.submit(&entity, path.command(&seed, false))?);
        assert_woken(&mut waiting).await;
        drain_expired(&node, &shadow)?;
    }
    Ok(())
}

async fn a_storage_failure_does_not_wake_and_a_retained_watch_observes_retry<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    let handle = node.broker.handle();
    let namespace = NamespaceName::new("tenant")?;
    for (index, path) in PATHS.into_iter().enumerate() {
        let entity = EntityPath::new(format!("storage-{index}"))?;
        node.create(&entity, path, true)?;
        let seed = node.seed(&entity, path)?;
        let shadow = entity.dead_letter_queue()?;
        let mut waiting = Box::pin(handle.deliverable(&namespace, &shadow));
        node.clock.advance(6);
        let before = node.store.snapshot()?;
        node.store.fail_apply.store(true, Ordering::SeqCst);
        assert!(node.submit(&entity, path.command(&seed, false)).is_err());
        assert_eq!(node.store.snapshot()?, before);
        assert_pending(&mut waiting).await;
        assert_empty(node.submit(&entity, path.command(&seed, false))?);
        assert_woken(&mut waiting).await;
        drain_expired(&node, &shadow)?;
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                super::$case(::testkit::MemoryProvider::new()).await
            }
        )+ }
        mod durable { $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                super::$case(::testkit::DurableProvider::temporary()?).await
            }
        )+ }
    };
}

for_each_backend! {
    committed_lazy_expiry_wakes_every_receive_path,
    drop_policy_and_noops_do_not_wake_but_the_same_watch_can_be_reused,
    invalid_later_deferred_records_roll_back_without_waking,
    a_storage_failure_does_not_wake_and_a_retained_watch_observes_retry,
}
