//! Selected persisted profiles are validated without a global catalog preflight.

use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use domain::{
    BoundCommand, BrokerError, CodecError, Command, CommandKind, CommandOutcome, DurableProposal,
    EntityPath, IndexedApplyError, IndexedApplyOutcome, IndexedWriter, MAX_DEFERRED_RECEIVE_BATCH,
    MAX_DUPLICATE_DETECTION_HISTORY_MILLIS, MAX_LOCK_DURATION_MILLIS,
    MIN_DUPLICATE_DETECTION_HISTORY_MILLIS, MessageEnvelope, NamespaceName, QueueConfig,
    QueueConfigUpdate, ReceiveMode, SequenceNumber, SessionId, StateMachine, SubscriptionConfig,
    SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use serde::Serialize;
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

#[derive(Clone, Debug, Default)]
struct Trace {
    gets: Vec<Key>,
    scans: usize,
    snapshots: usize,
    batches: Vec<WriteBatch>,
}

#[derive(Debug, Default)]
struct Controls {
    trace: Trace,
    fail_get: Option<Key>,
}

#[derive(Clone, Debug)]
struct Observed<S> {
    inner: S,
    controls: Arc<Mutex<Controls>>,
}

fn read_failure() -> StorageError {
    StorageError::Backend {
        operation: "read selected profile owner",
        detail: String::from("injected owner read failure"),
    }
}

impl<S: StateStore> StateStore for Observed<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        let mut controls = self.controls.lock().unwrap();
        controls.trace.gets.push(key.to_vec());
        let fail = controls.fail_get.as_deref() == Some(key);
        drop(controls);
        if fail {
            Err(read_failure())
        } else {
            self.inner.get(key)
        }
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.controls
            .lock()
            .unwrap()
            .trace
            .batches
            .push(batch.clone());
        self.inner.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.controls.lock().unwrap().trace.snapshots += 1;
        self.inner.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.controls.lock().unwrap().trace.scans += 1;
        self.inner.scan_from(prefix, start, limit)
    }
}

struct Fixture<P: StoreProvider> {
    machine: StateMachine<Observed<P::Store>>,
    namespace: NamespaceName,
    provider: P,
}

impl<P: StoreProvider> Fixture<P> {
    fn new(provider: P) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            machine: StateMachine::new(Observed {
                inner: provider.open()?,
                controls: Arc::new(Mutex::new(Controls::default())),
            }),
            namespace: NamespaceName::new("tenant")?,
            provider,
        })
    }
    fn command(&self, entity: &EntityPath, at: u64, kind: CommandKind) -> Command {
        Command::new(
            self.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(at),
            kind,
        )
    }
    fn at(
        &self,
        entity: &EntityPath,
        at: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&self.command(entity, at, kind))
    }
    fn create(&self, name: &str, config: QueueConfig) -> Result<EntityPath, Box<dyn Error>> {
        let entity = EntityPath::new(name)?;
        assert_eq!(
            self.at(&entity, 0, CommandKind::CreateQueue { config })?,
            CommandOutcome::QueueCreated
        );
        Ok(entity)
    }
    fn topic(&self) -> Result<(EntityPath, EntityPath), Box<dyn Error>> {
        let topic = EntityPath::new("events")?;
        assert_eq!(
            self.at(
                &topic,
                0,
                CommandKind::CreateTopic {
                    config: TopicConfig::default()
                }
            )?,
            CommandOutcome::TopicCreated
        );
        let CommandOutcome::SubscriptionCreated { entity } = self.at(
            &topic,
            0,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("audit")?,
                config: SubscriptionConfig::default(),
            },
        )?
        else {
            panic!("actual subscription creation");
        };
        Ok((topic, entity))
    }
    fn raw(&self) -> &P::Store {
        &self.machine.store().inner
    }
    fn put(&self, key: Key, value: Value) -> Result<(), StorageError> {
        self.raw().apply(WriteBatch::default().put(key, value))
    }
    fn profile(&self, entity: &EntityPath, config: QueueConfig) -> Result<(), Box<dyn Error>> {
        self.put(
            keys::queue_config(&self.namespace, entity),
            codec::encode(&config)?,
        )?;
        Ok(())
    }
    fn reset(&self) {
        self.machine.store().controls.lock().unwrap().trace = Trace::default();
    }
    fn trace(&self) -> Trace {
        self.machine.store().controls.lock().unwrap().trace.clone()
    }
    fn unchanged(&self, before: &StoreSnapshot) -> Result<(), Box<dyn Error>> {
        assert_eq!(&self.raw().snapshot()?, before);
        let trace = self.trace();
        assert!(trace.batches.is_empty());
        assert_eq!(trace.snapshots, 0);
        Ok(())
    }
    fn metadata_only(&self) {
        let trace = self.trace();
        assert_eq!(trace.scans, 0);
        assert!(
            trace
                .gets
                .iter()
                .all(|key| matches!(key.first(), Some(0x00 | 0x01 | 0x0E | 0x11)))
        );
    }
    fn restart(self) -> Result<Self, Box<dyn Error>> {
        let Self {
            machine,
            namespace,
            provider,
        } = self;
        drop(machine);
        Ok(Self {
            machine: StateMachine::new(Observed {
                inner: provider.open()?,
                controls: Arc::new(Mutex::new(Controls::default())),
            }),
            namespace,
            provider,
        })
    }
    fn restore(&self, before: &StoreSnapshot) -> Result<(), StorageError> {
        let mut batch = WriteBatch::default();
        for (key, _) in self.raw().snapshot()?.entries() {
            batch.push_delete(key.clone());
        }
        for (key, value) in before.entries() {
            batch.push_put(key.clone(), value.clone());
        }
        self.raw().apply(batch)
    }
}

fn receive(duration: Option<u64>) -> CommandKind {
    CommandKind::Receive {
        mode: ReceiveMode::PeekLock,
        lock_duration_millis: duration,
        session: None,
    }
}

fn send(id: &str, session: Option<SessionId>) -> CommandKind {
    CommandKind::Send {
        message_id: id.to_owned(),
        body: b"body".to_vec(),
        time_to_live_millis: None,
        session_id: session,
        scheduled_enqueue_at: None,
        envelope: None,
    }
}

fn invalid_profiles() -> Vec<QueueConfig> {
    let config = QueueConfig {
        requires_duplicate_detection: false,
        ..QueueConfig::default()
    };
    vec![
        QueueConfig {
            lock_duration_millis: 0,
            ..config
        },
        QueueConfig {
            lock_duration_millis: MAX_LOCK_DURATION_MILLIS + 1,
            ..config
        },
        QueueConfig {
            max_delivery_count: 0,
            ..config
        },
        QueueConfig {
            max_message_bytes: 0,
            ..config
        },
        QueueConfig {
            default_time_to_live_millis: Some(0),
            ..config
        },
        QueueConfig {
            duplicate_detection_history_millis: MIN_DUPLICATE_DETECTION_HISTORY_MILLIS - 1,
            ..config
        },
        QueueConfig {
            duplicate_detection_history_millis: MAX_DUPLICATE_DETECTION_HISTORY_MILLIS + 1,
            ..config
        },
    ]
}

fn selected_fields<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let mut f = Fixture::new(provider)?;
    let queue = f.create("orders", QueueConfig::default())?;
    let (_, backing) = f.topic()?;
    let targets = [
        queue.clone(),
        backing.clone(),
        queue.dead_letter_queue()?,
        backing.dead_letter_queue()?,
    ];
    for target in targets {
        let healthy = f.machine.queue_config(&f.namespace, &target)?.unwrap();
        let binding = f.machine.bind_entity(&f.namespace, &target)?;
        for invalid in invalid_profiles() {
            assert!(invalid.validate().is_err());
            f.profile(&target, invalid)?;
            f = f.restart()?;
            let before = f.raw().snapshot()?;
            f.reset();
            assert_eq!(
                f.machine.queue_config(&f.namespace, &target),
                Err(BrokerError::EntityMetadataCorrupt)
            );
            for duration in [None, Some(100)] {
                assert_eq!(
                    f.at(&target, 1, receive(duration)),
                    Err(BrokerError::EntityMetadataCorrupt)
                );
            }
            assert_eq!(
                f.machine.validate_binding(&binding),
                Err(BrokerError::EntityMetadataCorrupt)
            );
            assert_eq!(
                f.machine.bind_entity(&f.namespace, &target),
                Err(BrokerError::EntityMetadataCorrupt)
            );
            f.metadata_only();
            f.unchanged(&before)?;
            f.profile(&target, healthy)?;
            assert_eq!(
                f.machine.queue_config(&f.namespace, &target)?,
                Some(healthy)
            );
            for duration in [None, Some(100)] {
                assert_eq!(
                    f.at(&target, 1, receive(duration))?,
                    CommandOutcome::Received(None)
                );
            }
        }
        for valid in [
            QueueConfig {
                lock_duration_millis: 1,
                max_delivery_count: 1,
                max_message_bytes: 1,
                default_time_to_live_millis: Some(1),
                duplicate_detection_history_millis: MIN_DUPLICATE_DETECTION_HISTORY_MILLIS,
                ..healthy
            },
            QueueConfig {
                lock_duration_millis: MAX_LOCK_DURATION_MILLIS,
                max_delivery_count: u32::MAX,
                max_message_bytes: usize::MAX,
                default_time_to_live_millis: Some(u64::MAX),
                duplicate_detection_history_millis: MAX_DUPLICATE_DETECTION_HISTORY_MILLIS,
                ..healthy
            },
        ] {
            f.profile(&target, valid)?;
            f.reset();
            let before = f.raw().snapshot()?;
            assert_eq!(f.machine.queue_config(&f.namespace, &target)?, Some(valid));
            f.unchanged(&before)?;
        }
        f.profile(&target, healthy)?;
    }
    Ok(())
}

#[test]
fn memory_selected_profile_fields_and_valid_boundaries() -> Result<(), Box<dyn Error>> {
    selected_fields(MemoryProvider::new())
}

#[test]
fn fjall_selected_profile_fields_and_valid_boundaries() -> Result<(), Box<dyn Error>> {
    selected_fields(DurableProvider::temporary()?)
}

fn operational_paths<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let mut f = Fixture::new(provider)?;
    let config = QueueConfig {
        lock_duration_millis: 1_000,
        ..QueueConfig::default()
    };
    let queue = f.create("orders", config)?;
    let session_config = QueueConfig {
        requires_session: true,
        ..config
    };
    let session_queue = f.create("sessions", session_config)?;
    f.at(&queue, 10, send("deferred", None))?;
    let CommandOutcome::Received(Some(deferred)) = f.at(&queue, 11, receive(None))? else {
        panic!("original deferred delivery");
    };
    f.at(
        &queue,
        12,
        CommandKind::Defer {
            sequence: deferred.sequence,
            lock_token: deferred.lock.unwrap().token,
            replacement_envelope: None,
        },
    )?;
    f.at(&queue, 13, send("locked", None))?;
    let CommandOutcome::Received(Some(locked)) = f.at(&queue, 14, receive(None))? else {
        panic!("original locked delivery");
    };
    let token = locked.lock.unwrap().token;
    f.at(&queue, 15, send("ready", None))?;
    let held_id = SessionId::new("held")?;
    let available_id = SessionId::new("available")?;
    f.at(
        &session_queue,
        16,
        send("held-message", Some(held_id.clone())),
    )?;
    f.at(
        &session_queue,
        17,
        send("available-message", Some(available_id.clone())),
    )?;
    let CommandOutcome::SessionAccepted(Some(accepted)) = f.at(
        &session_queue,
        18,
        CommandKind::AcceptSession {
            session_id: Some(held_id.clone()),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("original session hold");
    };
    let hold = accepted.hold();
    let healthy = f.raw().snapshot()?;
    f.profile(
        &queue,
        QueueConfig {
            lock_duration_millis: 0,
            ..config
        },
    )?;
    f.profile(
        &session_queue,
        QueueConfig {
            lock_duration_millis: 0,
            ..session_config
        },
    )?;
    f = f.restart()?;
    let corrupt = f.raw().snapshot()?;
    for duration in [None, Some(100)] {
        let commands = [
            (queue.clone(), receive(duration)),
            (
                queue.clone(),
                CommandKind::ReceiveDeferred {
                    sequences: vec![deferred.sequence],
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: duration,
                    session: None,
                },
            ),
            (
                queue.clone(),
                CommandKind::RenewLock {
                    sequence: locked.sequence,
                    lock_token: token,
                    lock_duration_millis: duration,
                },
            ),
            (
                session_queue.clone(),
                CommandKind::AcceptSession {
                    session_id: Some(available_id.clone()),
                    lock_duration_millis: duration,
                },
            ),
            (
                session_queue.clone(),
                CommandKind::AcceptSession {
                    session_id: None,
                    lock_duration_millis: duration,
                },
            ),
            (
                session_queue.clone(),
                CommandKind::RenewSessionLock {
                    session: hold.clone(),
                    lock_duration_millis: duration,
                },
            ),
            (
                session_queue.clone(),
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: duration,
                    session: Some(hold.clone()),
                },
            ),
        ];
        for (target, kind) in commands {
            f.reset();
            assert_eq!(
                f.at(&target, 20, kind.clone()),
                Err(BrokerError::EntityMetadataCorrupt)
            );
            f.metadata_only();
            f.unchanged(&corrupt)?;
            f.restore(&healthy)?;
            let expected = duration.unwrap_or(config.lock_duration_millis);
            match f.at(&target, 20, kind)? {
                CommandOutcome::Received(Some(delivery)) => {
                    let lock = delivery.lock.unwrap();
                    assert_eq!(lock.lock_duration_millis, expected);
                    assert_eq!(lock.locked_until, Timestamp::from_millis(20 + expected));
                }
                CommandOutcome::DeferredReceived(deliveries) => {
                    assert_eq!(deliveries.len(), 1);
                    assert_eq!(deliveries[0].sequence, deferred.sequence);
                    assert_eq!(
                        deliveries[0].lock.unwrap().locked_until,
                        Timestamp::from_millis(20 + expected)
                    );
                }
                CommandOutcome::LockRenewed {
                    locked_until,
                    lock_duration_millis,
                } => {
                    assert_eq!(lock_duration_millis, expected);
                    assert_eq!(locked_until, Timestamp::from_millis(20 + expected));
                }
                CommandOutcome::SessionAccepted(Some(session)) => {
                    assert_eq!(session.session_id, available_id);
                    assert_eq!(
                        session.lock.locked_until,
                        Timestamp::from_millis(20 + expected)
                    );
                }
                CommandOutcome::SessionLockRenewed { locked_until } => {
                    assert_eq!(locked_until, Timestamp::from_millis(20 + expected));
                }
                other => panic!("repair must preserve original available state: {other:?}"),
            }
            f.restore(&corrupt)?;
        }
    }
    let replacement = Some(MessageEnvelope::new(b"replacement".to_vec()));
    for kind in [
        CommandKind::Abandon {
            sequence: locked.sequence,
            lock_token: token,
            replacement_envelope: replacement.clone(),
        },
        CommandKind::Defer {
            sequence: locked.sequence,
            lock_token: token,
            replacement_envelope: replacement.clone(),
        },
        CommandKind::DeadLetter {
            sequence: locked.sequence,
            lock_token: token,
            reason: String::from("reason"),
            description: String::from("description"),
            replacement_envelope: replacement,
        },
        CommandKind::ExpireLocks,
        CommandKind::ActivateScheduled,
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 1,
            session: None,
        },
    ] {
        f.reset();
        assert_eq!(
            f.at(&queue, 20, kind.clone()),
            Err(BrokerError::EntityMetadataCorrupt)
        );
        f.metadata_only();
        f.unchanged(&corrupt)?;
        f.restore(&healthy)?;
        assert!(f.at(&queue, 20, kind).is_ok());
        f.restore(&corrupt)?;
    }
    f.profile(
        &queue,
        QueueConfig {
            max_message_bytes: 0,
            ..config
        },
    )?;
    let before = f.raw().snapshot()?;
    f.reset();
    assert_eq!(
        f.at(&queue, 20, send("size-priority", None)),
        Err(BrokerError::EntityMetadataCorrupt)
    );
    f.metadata_only();
    f.unchanged(&before)?;
    f.reset();
    assert_eq!(
        f.at(&session_queue, 20, receive(None)),
        Err(BrokerError::EntityMetadataCorrupt)
    );
    f.metadata_only();
    f.unchanged(&before)?;
    f.restore(&healthy)?;
    assert_eq!(
        f.at(&session_queue, 20, receive(None)),
        Err(BrokerError::SessionRequired)
    );
    Ok(())
}

#[test]
fn memory_invalid_profiles_refuse_default_and_explicit_operational_durations()
-> Result<(), Box<dyn Error>> {
    operational_paths(MemoryProvider::new())
}

#[test]
fn fjall_invalid_profiles_refuse_default_and_explicit_operational_durations()
-> Result<(), Box<dyn Error>> {
    operational_paths(DurableProvider::temporary()?)
}

// Queue is the first existing owner-kind tag; this only seeds controlled corrupt/replaced heads.
#[derive(Serialize)]
enum HeadKind {
    Queue,
}

#[derive(Serialize)]
struct Head {
    generation: u64,
    kind: HeadKind,
    retired: bool,
}

fn priorities<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let f = Fixture::new(provider)?;
    let queue = f.create("orders", QueueConfig::default())?;
    let binding = f.machine.bind_entity(&f.namespace, &queue)?;
    let healthy = f.raw().snapshot()?;
    let profile_key = keys::queue_config(&f.namespace, &queue);
    let head_key = keys::entity_metadata(&f.namespace, &queue);
    let codec_error = BrokerError::Codec(CodecError::UnsupportedVersion { version: 255 });

    f.put(profile_key.clone(), vec![255])?;
    f.machine.store().controls.lock().unwrap().fail_get = Some(head_key.clone());
    let before = f.raw().snapshot()?;
    f.reset();
    assert_eq!(
        f.machine.queue_config(&f.namespace, &queue),
        Err(codec_error.clone())
    );
    assert_eq!(f.trace().gets, vec![profile_key.clone()]);
    f.unchanged(&before)?;

    f.profile(
        &queue,
        QueueConfig {
            lock_duration_millis: 0,
            ..QueueConfig::default()
        },
    )?;
    let before = f.raw().snapshot()?;
    f.reset();
    assert_eq!(
        f.machine.queue_config(&f.namespace, &queue),
        Err(BrokerError::Storage(read_failure()))
    );
    assert!(f.trace().gets.contains(&head_key));
    f.unchanged(&before)?;
    f.machine.store().controls.lock().unwrap().fail_get = None;

    f.put(keys::clock(), vec![255])?;
    let before = f.raw().snapshot()?;
    let command = f.command(&queue, 1, receive(None));
    f.reset();
    assert_eq!(f.machine.apply(&command), Err(codec_error));
    assert_eq!(f.trace().gets, vec![keys::clock()]);
    f.unchanged(&before)?;
    f.reset();
    assert_eq!(
        f.machine
            .apply_bound(&BoundCommand::new(binding.clone(), command.clone())),
        Err(BrokerError::EntityMetadataCorrupt)
    );
    assert!(!f.trace().gets.contains(&keys::clock()));
    f.unchanged(&before)?;

    f.put(
        head_key.clone(),
        codec::encode(&Head {
            generation: 2,
            kind: HeadKind::Queue,
            retired: false,
        })?,
    )?;
    let before = f.raw().snapshot()?;
    f.reset();
    assert_eq!(
        f.machine
            .apply_bound(&BoundCommand::new(binding.clone(), command)),
        Err(BrokerError::StaleEntityBinding)
    );
    assert!(!f.trace().gets.contains(&profile_key));
    assert!(!f.trace().gets.contains(&keys::clock()));
    f.unchanged(&before)?;
    f.reset();
    let wrong = f.command(&EntityPath::new("other")?, 1, receive(None));
    assert_eq!(
        f.machine.apply_bound(&BoundCommand::new(binding, wrong)),
        Err(BrokerError::InvalidEntityBinding)
    );
    assert!(f.trace().gets.is_empty());
    f.unchanged(&before)?;

    f.restore(&healthy)?;
    f.put(head_key.clone(), vec![255])?;
    f.profile(
        &queue,
        QueueConfig {
            lock_duration_millis: 0,
            ..QueueConfig::default()
        },
    )?;
    let before = f.raw().snapshot()?;
    f.reset();
    assert_eq!(
        f.machine.queue_config(&f.namespace, &queue),
        Err(BrokerError::EntityMetadataCorrupt)
    );
    assert!(f.trace().gets.contains(&head_key));
    f.unchanged(&before)?;

    f.restore(&healthy)?;
    f.raw()
        .apply(WriteBatch::default().delete(profile_key.clone()))?;
    let before = f.raw().snapshot()?;
    f.reset();
    assert_eq!(f.machine.queue_config(&f.namespace, &queue)?, None);
    assert_eq!(f.trace().gets, vec![profile_key]);
    assert_eq!(
        f.at(&queue, 1, receive(None)),
        Err(BrokerError::QueueNotFound)
    );
    assert!(!f.trace().gets.contains(&head_key));
    f.unchanged(&before)?;

    f.restore(&healthy)?;
    f.at(&queue, 10, send("advance-clock", None))?;
    f.profile(
        &queue,
        QueueConfig {
            lock_duration_millis: 0,
            ..QueueConfig::default()
        },
    )?;
    let before = f.raw().snapshot()?;
    f.reset();
    assert_eq!(
        f.at(&queue, 9, receive(None)),
        Err(BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(10),
            proposed: Timestamp::from_millis(9),
        })
    );
    assert_eq!(f.trace().gets, vec![keys::clock()]);
    f.unchanged(&before)?;
    Ok(())
}

#[test]
fn memory_profile_codec_owner_binding_and_clock_priorities() -> Result<(), Box<dyn Error>> {
    priorities(MemoryProvider::new())
}

#[test]
fn fjall_profile_codec_owner_binding_and_clock_priorities() -> Result<(), Box<dyn Error>> {
    priorities(DurableProvider::temporary()?)
}

fn topology_limits<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let mut f = Fixture::new(provider)?;
    let queue = f.create("orders", QueueConfig::default())?;
    let (topic, backing) = f.topic()?;
    let healthy = f.raw().snapshot()?;
    let shadow = backing.dead_letter_queue()?;
    for target in [backing.clone(), shadow] {
        let config = f.machine.queue_config(&f.namespace, &target)?.unwrap();
        f.profile(
            &target,
            QueueConfig {
                lock_duration_millis: 0,
                ..config
            },
        )?;
        f = f.restart()?;
        let before = f.raw().snapshot()?;
        f.reset();
        assert_eq!(
            f.machine.queue_config(&f.namespace, &target),
            Err(BrokerError::EntityMetadataCorrupt)
        );
        assert_eq!(
            f.machine.subscriptions(&f.namespace, &topic, 10),
            Err(BrokerError::TopicTopologyCorrupt)
        );
        assert_eq!(
            f.at(&topic, 1, send("publication", None)),
            Err(BrokerError::TopicTopologyCorrupt)
        );
        f.unchanged(&before)?;
        f.restore(&healthy)?;
    }
    f.put(keys::queue_config(&f.namespace, &backing), vec![255])?;
    let before = f.raw().snapshot()?;
    f.reset();
    assert_eq!(
        f.machine.subscriptions(&f.namespace, &topic, 10),
        Err(BrokerError::Codec(CodecError::UnsupportedVersion {
            version: 255
        }))
    );
    assert_eq!(
        f.at(&topic, 1, send("raw-publication", None)),
        Err(BrokerError::Codec(CodecError::UnsupportedVersion {
            version: 255
        }))
    );
    f.unchanged(&before)?;
    f.restore(&healthy)?;

    // Only the selected row is validated; these are not whole-topology health proofs.
    for parent in [queue, backing] {
        let shadow = parent.dead_letter_queue()?;
        let parent_config = f.machine.queue_config(&f.namespace, &parent)?.unwrap();
        let shadow_config = f.machine.queue_config(&f.namespace, &shadow)?.unwrap();
        f.profile(
            &parent,
            QueueConfig {
                lock_duration_millis: 0,
                ..parent_config
            },
        )?;
        let before = f.raw().snapshot()?;
        f.reset();
        assert_eq!(
            f.machine.queue_config(&f.namespace, &shadow)?,
            Some(shadow_config)
        );
        assert_eq!(
            f.at(&shadow, 1, receive(None))?,
            CommandOutcome::Received(None)
        );
        f.unchanged(&before)?;
        f.restore(&healthy)?;
        f.profile(
            &shadow,
            QueueConfig {
                lock_duration_millis: 0,
                ..shadow_config
            },
        )?;
        let before = f.raw().snapshot()?;
        f.reset();
        assert_eq!(
            f.machine.queue_config(&f.namespace, &parent)?,
            Some(parent_config)
        );
        assert_eq!(
            f.at(&parent, 1, receive(None))?,
            CommandOutcome::Received(None)
        );
        f.unchanged(&before)?;
        f.restore(&healthy)?;
    }
    Ok(())
}

#[test]
fn memory_selected_profiles_preserve_raw_topic_and_unselected_row_limits()
-> Result<(), Box<dyn Error>> {
    topology_limits(MemoryProvider::new())
}

#[test]
fn fjall_selected_profiles_preserve_raw_topic_and_unselected_row_limits()
-> Result<(), Box<dyn Error>> {
    topology_limits(DurableProvider::temporary()?)
}

fn profile_free_paths<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let mut f = Fixture::new(provider)?;
    let config = QueueConfig {
        lock_duration_millis: 1_000,
        ..QueueConfig::default()
    };
    let queue = f.create("orders", config)?;
    let session_config = QueueConfig {
        requires_session: true,
        ..config
    };
    let session_queue = f.create("sessions", session_config)?;
    let (_, backing) = f.topic()?;
    f.at(&queue, 10, send("complete-original", None))?;
    let CommandOutcome::Received(Some(delivery)) = f.at(&queue, 11, receive(None))? else {
        panic!("actual original message lock");
    };
    let binding = f.machine.bind_entity(&f.namespace, &queue)?;
    let CommandOutcome::SessionAccepted(Some(accepted)) = f.at(
        &session_queue,
        12,
        CommandKind::AcceptSession {
            session_id: Some(SessionId::new("held")?),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("actual original session lock");
    };
    let hold = accepted.hold();
    let CommandOutcome::SessionAccepted(Some(expiring)) = f.at(
        &session_queue,
        13,
        CommandKind::AcceptSession {
            session_id: Some(SessionId::new("expiring")?),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("actual expiring session lock");
    };
    let queue_shadow = queue.dead_letter_queue()?;
    for target in [&queue, &session_queue, &backing, &queue_shadow] {
        let current = f.machine.queue_config(&f.namespace, target)?.unwrap();
        f.profile(
            target,
            QueueConfig {
                lock_duration_millis: 0,
                ..current
            },
        )?;
    }
    f = f.restart()?;
    let complete = CommandKind::Complete {
        sequence: delivery.sequence,
        lock_token: delivery.lock.unwrap().token,
    };
    let before = f.raw().snapshot()?;
    f.reset();
    assert_eq!(
        f.machine.apply_bound(&BoundCommand::new(
            binding,
            f.command(&queue, 20, complete.clone())
        )),
        Err(BrokerError::EntityMetadataCorrupt)
    );
    f.unchanged(&before)?;
    f.reset();
    assert_eq!(f.at(&queue, 20, complete)?, CommandOutcome::Completed);
    assert!(f.trace().gets.iter().all(|key| key.first() != Some(&0x01)));
    f.reset();
    assert_eq!(
        f.at(
            &session_queue,
            21,
            CommandKind::SetSessionState {
                session: hold.clone(),
                state: b"retained".to_vec()
            }
        )?,
        CommandOutcome::SessionStateSet
    );
    assert!(f.trace().gets.iter().all(|key| key.first() != Some(&0x01)));
    let before = f.raw().snapshot()?;
    f.reset();
    assert_eq!(
        f.at(
            &session_queue,
            22,
            CommandKind::GetSessionState {
                session: hold.clone()
            }
        )?,
        CommandOutcome::SessionState(b"retained".to_vec())
    );
    assert!(f.trace().gets.iter().all(|key| key.first() != Some(&0x01)));
    f.unchanged(&before)?;
    f.reset();
    assert_eq!(
        f.at(
            &session_queue,
            23,
            CommandKind::ReleaseSession { session: hold }
        )?,
        CommandOutcome::SessionReleased
    );
    assert!(f.trace().gets.iter().all(|key| key.first() != Some(&0x01)));
    f.reset();
    assert_eq!(
        f.at(&session_queue, 2_000, CommandKind::ExpireSessionLocks)?,
        CommandOutcome::SessionLocksExpired { released: 1 }
    );
    assert!(f.trace().gets.iter().all(|key| key.first() != Some(&0x01)));
    assert_eq!(
        f.at(
            &session_queue,
            2_001,
            CommandKind::ReleaseSession {
                session: expiring.hold()
            }
        ),
        Err(BrokerError::SessionLockNotHeld {
            session_id: expiring.session_id
        })
    );

    let sequence = SequenceNumber::new(1);
    let earlier = [
        (
            queue.clone(),
            CommandKind::Peek {
                from_sequence: sequence,
                max_messages: 0,
                session: None,
            },
            BrokerError::EmptyPeek,
        ),
        (
            queue.clone(),
            CommandKind::ReceiveDeferred {
                sequences: vec![],
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
            BrokerError::EmptyDeferredReceive,
        ),
        (
            queue.clone(),
            CommandKind::ReceiveDeferred {
                sequences: vec![sequence, sequence],
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
            BrokerError::DuplicateDeferredSequence { sequence },
        ),
        (
            queue.clone(),
            CommandKind::ReceiveDeferred {
                sequences: vec![sequence; MAX_DEFERRED_RECEIVE_BATCH + 1],
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
            BrokerError::DeferredReceiveBatchTooLarge {
                count: MAX_DEFERRED_RECEIVE_BATCH + 1,
                maximum: MAX_DEFERRED_RECEIVE_BATCH,
            },
        ),
        (
            queue.clone(),
            CommandKind::CancelScheduled { sequences: vec![] },
            BrokerError::EmptyScheduledCancellation,
        ),
        (
            queue.clone(),
            CommandKind::CancelScheduled {
                sequences: vec![sequence, sequence],
            },
            BrokerError::DuplicateScheduledSequence { sequence },
        ),
        (
            queue.clone(),
            CommandKind::SendBatch { messages: vec![] },
            BrokerError::EmptyMessageBatch,
        ),
        (
            backing,
            send("direct-backing", None),
            BrokerError::SubscriptionSendNotAllowed,
        ),
        (
            queue.dead_letter_queue()?,
            send("direct-dlq", None),
            BrokerError::DeadLetterQueueIsReserved,
        ),
        (
            queue.dead_letter_queue()?,
            CommandKind::CancelScheduled {
                sequences: vec![sequence],
            },
            BrokerError::DeadLetterQueueIsReserved,
        ),
    ];
    let before = f.raw().snapshot()?;
    for (target, kind, expected) in earlier {
        f.reset();
        assert_eq!(f.at(&target, 2_001, kind), Err(expected));
        assert!(f.trace().gets.iter().all(|key| key.first() != Some(&0x01)));
        f.unchanged(&before)?;
    }
    f.reset();
    assert_eq!(
        f.at(
            &queue,
            2_001,
            CommandKind::CreateQueue {
                config: QueueConfig::default()
            }
        ),
        Err(BrokerError::QueueAlreadyExists)
    );
    assert_eq!(
        f.trace().gets,
        vec![keys::clock(), keys::queue_config(&f.namespace, &queue)]
    );
    f.unchanged(&before)?;
    f.reset();
    assert_eq!(
        f.at(
            &queue,
            2_001,
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate::default()
            }
        ),
        Err(BrokerError::EntityMetadataCorrupt)
    );
    f.unchanged(&before)?;
    Ok(())
}

#[test]
fn memory_profile_free_operations_and_earlier_refusals_are_unchanged() -> Result<(), Box<dyn Error>>
{
    profile_free_paths(MemoryProvider::new())
}

#[test]
fn fjall_profile_free_operations_and_earlier_refusals_are_unchanged() -> Result<(), Box<dyn Error>>
{
    profile_free_paths(DurableProvider::temporary()?)
}

fn indexed_boundary<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let mut f = Fixture::new(provider)?;
    let queue = EntityPath::new("orders")?;
    let config = QueueConfig::default();
    let create =
        DurableProposal::unbound(f.command(&queue, 10, CommandKind::CreateQueue { config }));
    let mut writer = IndexedWriter::open(f.machine.store().clone())?;
    assert_eq!(
        writer.apply(1, &create)?,
        IndexedApplyOutcome::Applied(CommandOutcome::QueueCreated)
    );
    let binding = f.machine.bind_entity(&f.namespace, &queue)?;
    let clock = f.raw().get(&keys::clock())?;
    f.profile(
        &queue,
        QueueConfig {
            lock_duration_millis: 0,
            ..config
        },
    )?;
    drop(writer);
    f = f.restart()?;
    let mut writer = IndexedWriter::open(f.machine.store().clone())?;
    f.put(keys::clock(), vec![255])?;
    let before = f.raw().snapshot()?;
    f.reset();
    assert_eq!(
        writer.apply(1, &create)?,
        IndexedApplyOutcome::AlreadyApplied
    );
    assert!(f.trace().gets.is_empty());
    assert_eq!(f.trace().scans, 0);
    f.unchanged(&before)?;

    let command = f.command(&queue, 11, send("repair-companion", None));
    let bound = DurableProposal::bound(BoundCommand::new(binding, command.clone()))?;
    f.reset();
    assert_eq!(
        writer.apply(2, &bound),
        Err(IndexedApplyError::Domain(
            BrokerError::EntityMetadataCorrupt
        ))
    );
    assert!(!f.trace().gets.contains(&keys::clock()));
    assert_eq!(writer.applied_index()?, 1);
    f.unchanged(&before)?;
    f.put(keys::clock(), clock.clone().unwrap())?;
    let before = f.raw().snapshot()?;
    f.reset();
    let next_receive = DurableProposal::unbound(f.command(&queue, 11, receive(None)));
    assert_eq!(
        writer.apply(2, &next_receive),
        Err(IndexedApplyError::Domain(
            BrokerError::EntityMetadataCorrupt
        ))
    );
    assert_eq!(writer.applied_index()?, 1);
    f.metadata_only();
    f.unchanged(&before)?;
    let next = DurableProposal::unbound(command);
    f.reset();
    assert_eq!(
        writer.apply(2, &next),
        Err(IndexedApplyError::Domain(
            BrokerError::EntityMetadataCorrupt
        ))
    );
    assert_eq!(writer.applied_index()?, 1);
    f.metadata_only();
    f.unchanged(&before)?;
    assert_eq!(f.raw().get(&keys::clock())?, clock);

    f.profile(&queue, config)?;
    f.reset();
    assert_eq!(
        writer.apply(2, &next)?,
        IndexedApplyOutcome::Applied(CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        })
    );
    assert_eq!(writer.applied_index()?, 2);
    assert_eq!(f.trace().batches.len(), 1);
    assert_eq!(f.machine.last_applied_time()?, Timestamp::from_millis(11));
    drop(writer);
    f = f.restart()?;
    let mut writer = IndexedWriter::open(f.machine.store().clone())?;
    f.reset();
    assert_eq!(writer.apply(2, &next)?, IndexedApplyOutcome::AlreadyApplied);
    assert!(f.trace().gets.is_empty());
    assert!(f.trace().batches.is_empty());
    drop(writer);
    let CommandOutcome::Received(Some(delivery)) = f.at(&queue, 12, receive(None))? else {
        panic!("actual repaired receive");
    };
    assert_eq!(delivery.sequence, SequenceNumber::new(1));
    assert_eq!(delivery.body, b"body");
    assert_eq!(
        delivery.lock.unwrap().locked_until,
        Timestamp::from_millis(12 + config.lock_duration_millis)
    );
    Ok(())
}

#[test]
fn memory_invalid_profile_blocks_next_index_but_not_latest_read_free_marker()
-> Result<(), Box<dyn Error>> {
    indexed_boundary(MemoryProvider::new())
}

#[test]
fn fjall_invalid_profile_blocks_next_index_but_not_latest_read_free_marker()
-> Result<(), Box<dyn Error>> {
    indexed_boundary(DurableProvider::temporary()?)
}
