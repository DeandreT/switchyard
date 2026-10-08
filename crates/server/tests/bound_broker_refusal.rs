use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use domain::{
    BrokerError, CommandKind, EntityBinding, EntityBindingKind, EntityPath, LockToken,
    NamespaceName, QueueConfig, SequenceNumber, SessionHold, SessionId, StateMachine,
    SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use protocol_amqp::Broker as ProtocolBroker;
use server::{Broker, Clock, LocalProposer, ProposeError, SubmitError};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;

#[derive(Clone, Default)]
struct CountingClock(Arc<AtomicUsize>);

impl Clock for CountingClock {
    fn now(&self) -> Timestamp {
        self.0.fetch_add(1, Ordering::SeqCst);
        Timestamp::from_millis(1_000)
    }
}

#[derive(Default)]
struct Observations {
    clock_reads: AtomicUsize,
    applies: AtomicUsize,
    scans: AtomicUsize,
    snapshots: AtomicUsize,
    fail_get: Mutex<Option<Key>>,
}

impl Observations {
    fn reset(&self, clock: &CountingClock) {
        clock.0.store(0, Ordering::SeqCst);
        for count in [
            &self.clock_reads,
            &self.applies,
            &self.scans,
            &self.snapshots,
        ] {
            count.store(0, Ordering::SeqCst);
        }
    }

    fn assert_read_only_points(&self, clock: &CountingClock) {
        assert_eq!(clock.0.load(Ordering::SeqCst), 0, "host clock");
        assert_eq!(self.clock_reads.load(Ordering::SeqCst), 0, "stored clock");
        assert_eq!(self.applies.load(Ordering::SeqCst), 0, "apply");
        assert_eq!(self.scans.load(Ordering::SeqCst), 0, "scan fallback");
        assert_eq!(
            self.snapshots.load(Ordering::SeqCst),
            0,
            "snapshot fallback"
        );
    }
}

#[derive(Clone)]
struct Observed<S> {
    inner: S,
    observations: Arc<Observations>,
}

fn read_failure() -> StorageError {
    StorageError::Backend {
        operation: "read owner",
        detail: "injected point-read failure".into(),
    }
}

impl<S: StateStore> StateStore for Observed<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        if key == keys::clock() {
            self.observations.clock_reads.fetch_add(1, Ordering::SeqCst);
        }
        if self
            .observations
            .fail_get
            .lock()
            .expect("failure lock")
            .as_deref()
            == Some(key)
        {
            return Err(read_failure());
        }
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observations.applies.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.observations.snapshots.fetch_add(1, Ordering::SeqCst);
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.observations.scans.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }
}

fn send() -> CommandKind {
    CommandKind::Send {
        message_id: "unchanged".into(),
        body: vec![7],
        time_to_live_millis: None,
        session_id: None,
        scheduled_enqueue_at: None,
        envelope: None,
    }
}

fn bypass_commands() -> Vec<CommandKind> {
    let token = LockToken::new(1);
    let session = SessionHold::new(SessionId::new("session").expect("session"), token);
    vec![
        send(),
        CommandKind::Complete {
            sequence: SequenceNumber::new(1),
            lock_token: token,
        },
        CommandKind::GetSessionState {
            session: session.clone(),
        },
        CommandKind::SetSessionState {
            session: session.clone(),
            state: vec![3],
        },
        CommandKind::ReleaseSession { session },
    ]
}

fn kind_tag(binding: &EntityBinding) -> u32 {
    match binding.kind() {
        EntityBindingKind::Queue => 0,
        EntityBindingKind::Topic => 1,
        EntityBindingKind::Subscription => 2,
    }
}

async fn queued_refusals_do_not_observe_clock_apply_or_wake<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let raw = provider.open()?;
    let observations = Arc::new(Observations::default());
    let clock = CountingClock::default();
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(Observed {
            inner: raw.clone(),
            observations: Arc::clone(&observations),
        }),
        clock.clone(),
    ));
    let handle = broker.handle();
    let namespace = NamespaceName::new("tenant")?;
    let queue = EntityPath::new("queue")?;
    let topic = EntityPath::new("topic")?;
    let name = SubscriptionName::new("child")?;
    let child = topic.subscription(&name)?;
    for (target, kind) in [
        (
            queue.clone(),
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ),
        (
            topic.clone(),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
        (
            topic.clone(),
            CommandKind::CreateSubscription {
                name,
                config: SubscriptionConfig::default(),
            },
        ),
    ] {
        handle.submit_blocking(namespace.clone(), target, kind)?;
    }

    for target in [
        queue.clone(),
        topic,
        child.clone(),
        queue.dead_letter_queue()?,
        child.dead_letter_queue()?,
    ] {
        let mut wake = Box::pin(handle.deliverable(&namespace, &target));
        let before_bind = raw.snapshot()?;
        observations.reset(&clock);
        let binding = handle.bind_entity_blocking(namespace.clone(), target.clone())?;
        observations.assert_read_only_points(&clock);
        assert_eq!(raw.snapshot()?, before_bind);
        let key = keys::entity_metadata(&namespace, binding.owner());
        let original = raw.get(&key)?.expect("created owner");
        let shadow_key = keys::entity_metadata(&namespace, &binding.owner().dead_letter_queue()?);
        let config_key = if binding.kind() == EntityBindingKind::Topic {
            keys::topic_config(&namespace, &target)
        } else {
            keys::queue_config(&namespace, &target)
        };
        let config = raw.get(&config_key)?.expect("target profile");
        // The tuple mirrors the private V1 head fields without a public constructor.
        let tag = kind_tag(&binding);
        let faults = [
            (
                WriteBatch::default().put(key.clone(), codec::encode(&(2_u64, tag, false))?),
                BrokerError::StaleEntityBinding,
            ),
            (
                WriteBatch::default().delete(key.clone()),
                BrokerError::EntityMetadataCorrupt,
            ),
            (
                WriteBatch::default().put(key.clone(), vec![255]),
                BrokerError::EntityMetadataCorrupt,
            ),
            (
                WriteBatch::default().put(key.clone(), codec::encode(&(0_u64, tag, false))?),
                BrokerError::EntityMetadataCorrupt,
            ),
            (
                WriteBatch::default().put(key.clone(), codec::encode(&(1_u64, tag, true))?),
                BrokerError::EntityMetadataCorrupt,
            ),
            (
                WriteBatch::default()
                    .put(key.clone(), codec::encode(&(1_u64, (tag + 1) % 3, false))?),
                BrokerError::EntityMetadataCorrupt,
            ),
            (
                WriteBatch::default().put(shadow_key.clone(), original.clone()),
                BrokerError::EntityMetadataCorrupt,
            ),
            (
                WriteBatch::default().delete(config_key.clone()),
                BrokerError::StaleEntityBinding,
            ),
        ];
        for (fault, expected) in faults {
            raw.apply(fault)?;
            let before = raw.snapshot()?;
            observations.reset(&clock);
            for kind in bypass_commands() {
                assert_eq!(
                    handle.submit_bound_blocking(binding.clone(), kind.clone()),
                    Err(SubmitError::Propose(ProposeError::Broker(expected.clone())))
                );
                assert_eq!(
                    handle.submit_bound(binding.clone(), kind).await,
                    Err(SubmitError::Propose(ProposeError::Broker(expected.clone())))
                );
            }
            observations.assert_read_only_points(&clock);
            assert_eq!(raw.snapshot()?, before, "logical state including sequences");
            assert!(
                tokio::time::timeout(Duration::from_millis(5), wake.as_mut())
                    .await
                    .is_err(),
                "refusal must not notify"
            );
            raw.apply(
                WriteBatch::default()
                    .put(key.clone(), original.clone())
                    .delete(shadow_key.clone())
                    .put(config_key.clone(), config.clone()),
            )?;
        }

        *observations.fail_get.lock().expect("failure lock") = Some(key.clone());
        let before = raw.snapshot()?;
        observations.reset(&clock);
        let error =
            SubmitError::Propose(ProposeError::Broker(BrokerError::Storage(read_failure())));
        assert_eq!(
            handle.submit_bound_blocking(binding.clone(), send()),
            Err(error.clone())
        );
        assert_eq!(
            handle.submit_bound(binding.clone(), send()).await,
            Err(error)
        );
        observations.assert_read_only_points(&clock);
        assert_eq!(raw.snapshot()?, before);
        *observations.fail_get.lock().expect("failure lock") = None;

        observations.reset(&clock);
        assert_eq!(
            handle
                .bind_entity(namespace.clone(), target.clone())
                .await?,
            binding
        );
        observations.assert_read_only_points(&clock);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), wake.as_mut())
                .await
                .is_err()
        );
    }
    Ok(())
}

macro_rules! paired {
    ($name:ident) => {
        mod memory {
            #[test]
            fn $name() -> Result<(), Box<dyn std::error::Error>> {
                tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()?
                    .block_on(super::$name(testkit::MemoryProvider::new()))
            }
        }
        mod durable {
            #[test]
            fn $name() -> Result<(), Box<dyn std::error::Error>> {
                tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()?
                    .block_on(super::$name(testkit::DurableProvider::temporary()?))
            }
        }
    };
}

paired!(queued_refusals_do_not_observe_clock_apply_or_wake);
