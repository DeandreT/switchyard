//! Ordinary queue profile updates preserve all retained authority and data.

use std::{
    collections::BTreeSet,
    error::Error,
    sync::{Arc, Mutex},
};

use domain::{
    BoundCommand, BrokerError, Command, CommandKind, CommandOutcome, EntityPath, LockToken,
    MAX_DUPLICATE_DETECTION_HISTORY_MILLIS, MAX_LOCK_DURATION_MILLIS,
    MIN_DUPLICATE_DETECTION_HISTORY_MILLIS, MessageInput, NamespaceName, QueueConfig,
    QueueConfigError, QueueConfigUpdate, QueueTimeToLiveUpdate, ReceiveMode, RuleFilter, RuleName,
    SequenceNumber, SessionHold, SessionId, StateMachine, SubscriptionConfig, SubscriptionName,
    Timestamp, TopicConfig, codec, keys,
};
use serde::Serialize;
use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
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
    fail_apply: bool,
    fail_get: Option<Key>,
}

#[derive(Clone, Debug)]
struct Observed<S> {
    inner: S,
    controls: Arc<Mutex<Controls>>,
}

fn failure(operation: &'static str) -> StorageError {
    StorageError::Backend {
        operation,
        detail: String::from("injected queue update failure"),
    }
}

impl<S: StateStore> StateStore for Observed<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        let mut controls = self.controls.lock().unwrap();
        controls.trace.gets.push(key.to_vec());
        if controls.fail_get.as_deref() == Some(key) {
            return Err(failure("get update metadata"));
        }
        drop(controls);
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let mut controls = self.controls.lock().unwrap();
        controls.trace.batches.push(batch.clone());
        let fail = std::mem::take(&mut controls.fail_apply);
        drop(controls);
        if fail {
            Err(failure("apply update batch"))
        } else {
            self.inner.apply(batch)
        }
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
    namespace: NamespaceName,
    queue: EntityPath,
    machine: StateMachine<Observed<P::Store>>,
    provider: P,
}

impl<P: StoreProvider> Fixture<P> {
    fn new(provider: P, config: QueueConfig) -> Result<Self, Box<dyn Error>> {
        let fixture = Self {
            namespace: NamespaceName::new("tenant")?,
            queue: EntityPath::new("orders")?,
            machine: StateMachine::new(Observed {
                inner: provider.open()?,
                controls: Arc::new(Mutex::new(Controls::default())),
            }),
            provider,
        };
        assert_eq!(
            fixture.at(&fixture.queue, 0, CommandKind::CreateQueue { config })?,
            CommandOutcome::QueueCreated
        );
        Ok(fixture)
    }
    fn at(
        &self,
        entity: &EntityPath,
        millis: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&Command::new(
            self.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(millis),
            kind,
        ))
    }
    fn update(
        &self,
        millis: u64,
        update: QueueConfigUpdate,
    ) -> Result<CommandOutcome, BrokerError> {
        self.at(&self.queue, millis, CommandKind::UpdateQueue { update })
    }
    fn raw(&self) -> &P::Store {
        &self.machine.store().inner
    }
    fn reset(&self) {
        self.machine.store().controls.lock().unwrap().trace = Trace::default();
    }
    fn trace(&self) -> Trace {
        self.machine.store().controls.lock().unwrap().trace.clone()
    }
    fn restore(&self, snapshot: &StoreSnapshot) -> Result<(), StorageError> {
        let mut batch = WriteBatch::default();
        for (key, _) in self.raw().snapshot()?.entries() {
            batch.push_delete(key.clone());
        }
        for (key, value) in snapshot.entries() {
            batch.push_put(key.clone(), value.clone());
        }
        self.raw().apply(batch)
    }
    fn restart(self) -> Result<Self, Box<dyn Error>> {
        let Self {
            namespace,
            queue,
            machine,
            provider,
        } = self;
        drop(machine);
        Ok(Self {
            namespace,
            queue,
            machine: StateMachine::new(Observed {
                inner: provider.open()?,
                controls: Arc::new(Mutex::new(Controls::default())),
            }),
            provider,
        })
    }
    fn send(
        &self,
        millis: u64,
        id: &str,
        bytes: &[u8],
        schedule: Option<u64>,
    ) -> Result<SequenceNumber, BrokerError> {
        let CommandOutcome::Sent { sequence } = self.at(
            &self.queue,
            millis,
            CommandKind::Send {
                message_id: id.to_owned(),
                body: bytes.to_vec(),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: schedule.map(Timestamp::from_millis),
                envelope: None,
            },
        )?
        else {
            panic!("actual stored send");
        };
        Ok(sequence)
    }
    fn receive(&self, millis: u64) -> Result<domain::Delivery, BrokerError> {
        let CommandOutcome::Received(Some(delivery)) = self.at(
            &self.queue,
            millis,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
        )?
        else {
            panic!("actual locked delivery");
        };
        Ok(delivery)
    }
    fn assert_read_set(&self) -> Result<(), Box<dyn Error>> {
        let shadow = self.queue.dead_letter_queue()?;
        let allowed = BTreeSet::from([
            keys::clock(),
            keys::queue_config(&self.namespace, &self.queue),
            keys::queue_config(&self.namespace, &shadow),
            keys::topic_config(&self.namespace, &self.queue),
            keys::topic_config(&self.namespace, &shadow),
            keys::entity_metadata(&self.namespace, &self.queue),
            keys::entity_metadata(&self.namespace, &shadow),
        ]);
        let trace = self.trace();
        assert_eq!(trace.scans, 0);
        assert_eq!(trace.snapshots, 0);
        assert!(trace.gets.len() <= 7, "update performs bounded point reads");
        assert!(
            trace.gets.iter().all(|key| allowed.contains(key)),
            "update reads only bounded profile/owner metadata"
        );
        Ok(())
    }
}

fn shadow(config: QueueConfig) -> QueueConfig {
    QueueConfig {
        max_delivery_count: u32::MAX,
        default_time_to_live_millis: None,
        requires_session: false,
        requires_duplicate_detection: false,
        ..config
    }
}

fn profile(config: QueueConfig) -> QueueConfigUpdate {
    QueueConfigUpdate {
        lock_duration_millis: Some(config.lock_duration_millis),
        max_delivery_count: Some(config.max_delivery_count),
        default_time_to_live_millis: Some(
            config
                .default_time_to_live_millis
                .map_or(QueueTimeToLiveUpdate::Unlimited, |millis| {
                    QueueTimeToLiveUpdate::Finite { millis }
                }),
        ),
        max_message_bytes: Some(config.max_message_bytes),
        requires_session: Some(config.requires_session),
        requires_duplicate_detection: Some(config.requires_duplicate_detection),
        duplicate_detection_history_millis: Some(config.duplicate_detection_history_millis),
    }
}

fn remaining(snapshot: &StoreSnapshot, excluded: &BTreeSet<Key>) -> Vec<(Key, Value)> {
    snapshot
        .entries()
        .iter()
        .filter(|(key, _)| !excluded.contains(key))
        .cloned()
        .collect()
}

fn updated_writes<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let initial = QueueConfig {
        lock_duration_millis: 100,
        default_time_to_live_millis: Some(1_000),
        requires_duplicate_detection: true,
        duplicate_detection_history_millis: 20_000,
        ..QueueConfig::default()
    };
    let fixture = Fixture::new(provider, initial)?;
    let locked_sequence = fixture.send(1, "locked", b"locked", None)?;
    let locked = fixture.receive(2)?;
    assert_eq!(locked.sequence, locked_sequence);
    let source = fixture.send(3, "shadow", b"shadow", None)?;
    let doomed = fixture.receive(4)?;
    assert_eq!(doomed.sequence, source);
    assert_eq!(
        fixture.at(
            &fixture.queue,
            5,
            CommandKind::DeadLetter {
                sequence: source,
                lock_token: doomed.lock.unwrap().token,
                reason: String::from("test"),
                description: String::from("kept"),
                replacement_envelope: None
            }
        )?,
        CommandOutcome::DeadLettered
    );
    fixture.send(6, "ready", b"ready", None)?;
    fixture.send(7, "scheduled", b"scheduled", Some(10_000))?;
    let dlq = fixture.queue.dead_letter_queue()?;
    let parent_binding = fixture
        .machine
        .bind_entity(&fixture.namespace, &fixture.queue)?;
    let dlq_binding = fixture.machine.bind_entity(&fixture.namespace, &dlq)?;
    let before = fixture.raw().snapshot()?;
    let updated = QueueConfig {
        lock_duration_millis: 40,
        max_delivery_count: 3,
        default_time_to_live_millis: Some(500),
        max_message_bytes: 10,
        duplicate_detection_history_millis: 40_000,
        ..initial
    };
    fixture.reset();
    assert_eq!(
        fixture.update(10, profile(updated))?,
        CommandOutcome::QueueUpdated
    );
    fixture.assert_read_set()?;
    let trace = fixture.trace();
    assert_eq!(trace.batches.len(), 1);
    assert_eq!(
        trace.batches[0].mutations(),
        &[
            Mutation::Put {
                key: keys::queue_config(&fixture.namespace, &fixture.queue),
                value: codec::encode(&updated)?
            },
            Mutation::Put {
                key: keys::queue_config(&fixture.namespace, &dlq),
                value: codec::encode(&shadow(updated))?
            },
            Mutation::Put {
                key: keys::clock(),
                value: codec::encode(&Timestamp::from_millis(10))?
            },
        ]
    );
    let after = fixture.raw().snapshot()?;
    let excluded = BTreeSet::from([
        keys::clock(),
        keys::queue_config(&fixture.namespace, &fixture.queue),
        keys::queue_config(&fixture.namespace, &dlq),
    ]);
    assert_eq!(remaining(&before, &excluded), remaining(&after, &excluded));
    assert_eq!(
        fixture
            .machine
            .bind_entity(&fixture.namespace, &fixture.queue)?,
        parent_binding
    );
    assert_eq!(
        fixture.machine.bind_entity(&fixture.namespace, &dlq)?,
        dlq_binding
    );
    fixture.machine.validate_binding(&parent_binding)?;
    fixture.machine.validate_binding(&dlq_binding)?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.raw().snapshot()?, after);
    fixture.machine.validate_binding(&parent_binding)?;
    fixture.machine.validate_binding(&dlq_binding)?;
    // A parent-only setting never rewrites an unchanged shadow profile.
    fixture.reset();
    assert_eq!(
        fixture.update(
            11,
            QueueConfigUpdate {
                max_delivery_count: Some(4),
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Unlimited),
                ..QueueConfigUpdate::default()
            }
        )?,
        CommandOutcome::QueueUpdated
    );
    let parent_only = QueueConfig {
        max_delivery_count: 4,
        default_time_to_live_millis: None,
        ..updated
    };
    assert_eq!(
        fixture.trace().batches[0].mutations(),
        &[
            Mutation::Put {
                key: keys::queue_config(&fixture.namespace, &fixture.queue),
                value: codec::encode(&parent_only)?
            },
            Mutation::Put {
                key: keys::clock(),
                value: codec::encode(&Timestamp::from_millis(11))?
            },
        ]
    );
    assert_eq!(
        fixture.machine.queue_config(&fixture.namespace, &dlq)?,
        Some(shadow(updated))
    );
    let after = fixture.raw().snapshot()?;
    for binding in [parent_binding, dlq_binding] {
        let command = Command::new(
            fixture.namespace.clone(),
            binding.target().clone(),
            Timestamp::from_millis(12),
            CommandKind::Peek {
                from_sequence: SequenceNumber::new(0),
                max_messages: 10,
                session: None,
            },
        );
        let CommandOutcome::Peeked(records) = fixture
            .machine
            .apply_bound(&BoundCommand::new(binding, command))?
        else {
            panic!("retained endpoint authority still works");
        };
        assert!(!records.is_empty());
    }
    assert_eq!(fixture.raw().snapshot()?, after);
    assert_eq!(fixture.restart()?.raw().snapshot()?, after);
    Ok(())
}

#[test]
fn paired_updates_write_only_changed_profiles_and_clock_with_retained_bindings()
-> Result<(), Box<dyn Error>> {
    updated_writes(MemoryProvider::new())?;
    updated_writes(DurableProvider::temporary()?)
}

fn noops_and_validation<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider, QueueConfig::default())?;
    let snapshot = fixture.raw().snapshot()?;
    for update in [
        QueueConfigUpdate::default(),
        profile(QueueConfig::default()),
    ] {
        fixture.reset();
        assert_eq!(fixture.update(100, update)?, CommandOutcome::QueueUpdated);
        assert_eq!(fixture.raw().snapshot()?, snapshot);
        assert!(fixture.trace().batches.is_empty());
        fixture.assert_read_set()?;
        assert_eq!(fixture.machine.last_applied_time()?, Timestamp::UNIX_EPOCH);
    }
    let invalid = [
        (
            QueueConfigUpdate {
                lock_duration_millis: Some(0),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::LockDurationTooShort,
        ),
        (
            QueueConfigUpdate {
                lock_duration_millis: Some(MAX_LOCK_DURATION_MILLIS + 1),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::LockDurationTooLong {
                maximum_millis: MAX_LOCK_DURATION_MILLIS,
            },
        ),
        (
            QueueConfigUpdate {
                max_delivery_count: Some(0),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::MaxDeliveryCountTooSmall,
        ),
        (
            QueueConfigUpdate {
                max_message_bytes: Some(0),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::MaxMessageBytesTooSmall,
        ),
        (
            QueueConfigUpdate {
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 0 }),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::TimeToLiveTooShort,
        ),
        (
            QueueConfigUpdate {
                duplicate_detection_history_millis: Some(
                    MIN_DUPLICATE_DETECTION_HISTORY_MILLIS - 1,
                ),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::DuplicateDetectionHistoryTooShort {
                minimum_millis: MIN_DUPLICATE_DETECTION_HISTORY_MILLIS,
            },
        ),
        (
            QueueConfigUpdate {
                duplicate_detection_history_millis: Some(
                    MAX_DUPLICATE_DETECTION_HISTORY_MILLIS + 1,
                ),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::DuplicateDetectionHistoryTooLong {
                maximum_millis: MAX_DUPLICATE_DETECTION_HISTORY_MILLIS,
            },
        ),
        (
            QueueConfigUpdate {
                requires_session: Some(true),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::RequiresSessionImmutable,
        ),
        (
            QueueConfigUpdate {
                requires_duplicate_detection: Some(true),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::RequiresDuplicateDetectionImmutable,
        ),
        // A valid earlier field must not stage anything before a later refusal.
        (
            QueueConfigUpdate {
                lock_duration_millis: Some(1),
                max_message_bytes: Some(0),
                ..QueueConfigUpdate::default()
            },
            QueueConfigError::MaxMessageBytesTooSmall,
        ),
    ];
    for (update, error) in invalid {
        fixture.reset();
        assert_eq!(
            fixture.update(100, update),
            Err(BrokerError::QueueConfig(error))
        );
        assert_eq!(fixture.raw().snapshot()?, snapshot);
        assert!(fixture.trace().batches.is_empty());
        fixture.assert_read_set()?;
        assert_eq!(fixture.machine.last_applied_time()?, Timestamp::UNIX_EPOCH);
    }
    assert_eq!(fixture.restart()?.raw().snapshot()?, snapshot);
    Ok(())
}

#[test]
fn paired_noops_invalid_patches_and_immutable_changes_never_commit_or_advance_clock()
-> Result<(), Box<dyn Error>> {
    noops_and_validation(MemoryProvider::new())?;
    noops_and_validation(DurableProvider::temporary()?)
}

fn immutable_restatements<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider, QueueConfig::default())?;
    for sessions in [false, true] {
        for duplicates in [false, true] {
            let entity = EntityPath::new(format!("queue-{sessions}-{duplicates}"))?;
            let config = QueueConfig {
                requires_session: sessions,
                requires_duplicate_detection: duplicates,
                default_time_to_live_millis: Some(100),
                ..QueueConfig::default()
            };
            let issued = fixture.machine.last_applied_time()?.as_millis();
            fixture.at(&entity, issued, CommandKind::CreateQueue { config })?;
            let accepted = if sessions {
                let CommandOutcome::SessionAccepted(Some(accepted)) = fixture.at(
                    &entity,
                    issued,
                    CommandKind::AcceptSession {
                        session_id: Some(SessionId::new("held-profile")?),
                        lock_duration_millis: None,
                    },
                )?
                else {
                    panic!("actual held session on ordinary queue");
                };
                fixture.at(
                    &entity,
                    issued,
                    CommandKind::SetSessionState {
                        session: accepted.hold(),
                        state: b"retained-state".to_vec(),
                    },
                )?;
                Some(accepted)
            } else {
                None
            };
            let before = fixture.raw().snapshot()?;
            fixture.reset();
            assert_eq!(
                fixture.at(
                    &entity,
                    100,
                    CommandKind::UpdateQueue {
                        update: profile(config)
                    }
                )?,
                CommandOutcome::QueueUpdated
            );
            assert!(fixture.trace().batches.is_empty());
            assert_eq!(fixture.raw().snapshot()?, before);
            for update in [
                QueueConfigUpdate {
                    requires_session: Some(!sessions),
                    lock_duration_millis: Some(2),
                    ..QueueConfigUpdate::default()
                },
                QueueConfigUpdate {
                    requires_duplicate_detection: Some(!duplicates),
                    lock_duration_millis: Some(2),
                    ..QueueConfigUpdate::default()
                },
            ] {
                fixture.reset();
                let expected = if update.requires_session.is_some() {
                    QueueConfigError::RequiresSessionImmutable
                } else {
                    QueueConfigError::RequiresDuplicateDetectionImmutable
                };
                assert_eq!(
                    fixture.at(&entity, 100, CommandKind::UpdateQueue { update }),
                    Err(BrokerError::QueueConfig(expected))
                );
                assert!(fixture.trace().batches.is_empty());
                assert_eq!(fixture.trace().scans, 0);
                assert_eq!(fixture.raw().snapshot()?, before);
            }
            if let Some(accepted) = accepted {
                let binding = fixture.machine.bind_entity(&fixture.namespace, &entity)?;
                let session_before =
                    fixture
                        .machine
                        .session(&fixture.namespace, &entity, &accepted.session_id)?;
                fixture.reset();
                assert_eq!(
                    fixture.at(
                        &entity,
                        issued + 1,
                        CommandKind::UpdateQueue {
                            update: QueueConfigUpdate {
                                lock_duration_millis: Some(2),
                                requires_session: Some(true),
                                requires_duplicate_detection: Some(duplicates),
                                ..QueueConfigUpdate::default()
                            }
                        }
                    )?,
                    CommandOutcome::QueueUpdated
                );
                assert_eq!(
                    fixture
                        .machine
                        .session(&fixture.namespace, &entity, &accepted.session_id)?,
                    session_before
                );
                assert_eq!(
                    fixture.machine.bind_entity(&fixture.namespace, &entity)?,
                    binding
                );
                let excluded = BTreeSet::from([
                    keys::clock(),
                    keys::queue_config(&fixture.namespace, &entity),
                    keys::queue_config(&fixture.namespace, &entity.dead_letter_queue()?),
                ]);
                assert_eq!(
                    remaining(&before, &excluded),
                    remaining(&fixture.raw().snapshot()?, &excluded)
                );
            }
        }
    }
    let before = fixture.raw().snapshot()?;
    assert_eq!(fixture.restart()?.raw().snapshot()?, before);
    Ok(())
}

#[test]
fn paired_session_and_duplicate_modes_allow_only_identical_restatement()
-> Result<(), Box<dyn Error>> {
    immutable_restatements(MemoryProvider::new())?;
    immutable_restatements(DurableProvider::temporary()?)
}

#[derive(Clone, Copy, Serialize)]
enum HeadKind {
    Queue,
    Topic,
    Subscription,
}
#[derive(Serialize)]
struct Head {
    generation: u64,
    kind: HeadKind,
    retired: bool,
}

fn corrupt_metadata<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new(provider, QueueConfig::default())?;
    let baseline = fixture.raw().snapshot()?;
    let parent = keys::queue_config(&fixture.namespace, &fixture.queue);
    let dlq = fixture.queue.dead_letter_queue()?;
    let shadow_key = keys::queue_config(&fixture.namespace, &dlq);
    let owner = keys::entity_metadata(&fixture.namespace, &fixture.queue);
    let mut noncanonical = codec::encode(&QueueConfig::default())?;
    noncanonical.push(0);
    let cases = vec![
        (owner.clone(), None),
        (owner.clone(), Some(vec![1, 255])),
        (
            owner.clone(),
            Some(codec::encode(&Head {
                generation: 0,
                kind: HeadKind::Queue,
                retired: false,
            })?),
        ),
        (
            owner.clone(),
            Some(codec::encode(&Head {
                generation: 1,
                kind: HeadKind::Topic,
                retired: false,
            })?),
        ),
        (
            owner.clone(),
            Some(codec::encode(&Head {
                generation: 1,
                kind: HeadKind::Subscription,
                retired: false,
            })?),
        ),
        (
            owner,
            Some(codec::encode(&Head {
                generation: 1,
                kind: HeadKind::Queue,
                retired: true,
            })?),
        ),
        (
            keys::entity_metadata(&fixture.namespace, &dlq),
            Some(codec::encode(&Head {
                generation: 1,
                kind: HeadKind::Queue,
                retired: false,
            })?),
        ),
        (parent.clone(), None),
        (parent.clone(), Some(vec![2, 0])),
        (parent.clone(), Some(noncanonical)),
        (
            parent,
            Some(codec::encode(&QueueConfig {
                lock_duration_millis: 0,
                ..QueueConfig::default()
            })?),
        ),
        (shadow_key.clone(), None),
        (shadow_key.clone(), Some(vec![2, 0])),
        (
            shadow_key.clone(),
            Some(codec::encode(&QueueConfig {
                lock_duration_millis: 1,
                ..shadow(QueueConfig::default())
            })?),
        ),
        (
            shadow_key.clone(),
            Some(codec::encode(&QueueConfig {
                requires_session: true,
                ..shadow(QueueConfig::default())
            })?),
        ),
        (
            shadow_key.clone(),
            Some(codec::encode(&QueueConfig {
                requires_duplicate_detection: true,
                ..shadow(QueueConfig::default())
            })?),
        ),
        (
            shadow_key,
            Some(codec::encode(&QueueConfig {
                default_time_to_live_millis: Some(1),
                ..shadow(QueueConfig::default())
            })?),
        ),
        (
            keys::topic_config(&fixture.namespace, &fixture.queue),
            Some(codec::encode(&TopicConfig::default())?),
        ),
        (
            keys::topic_config(&fixture.namespace, &dlq),
            Some(codec::encode(&TopicConfig::default())?),
        ),
    ];
    for (key, value) in cases {
        fixture.restore(&baseline)?;
        let mut tamper = WriteBatch::default();
        match value {
            Some(value) => tamper.push_put(key, value),
            None => tamper.push_delete(key),
        }
        fixture.raw().apply(tamper)?;
        let corrupt = fixture.raw().snapshot()?;
        for update in [
            QueueConfigUpdate::default(),
            QueueConfigUpdate {
                lock_duration_millis: Some(2),
                ..QueueConfigUpdate::default()
            },
        ] {
            fixture.reset();
            assert_eq!(
                fixture.update(100, update),
                Err(BrokerError::EntityMetadataCorrupt)
            );
            assert_eq!(fixture.raw().snapshot()?, corrupt);
            assert!(fixture.trace().batches.is_empty());
            fixture.assert_read_set()?;
        }
        fixture = fixture.restart()?;
        assert_eq!(fixture.raw().snapshot()?, corrupt);
        assert_eq!(
            fixture.update(100, QueueConfigUpdate::default()),
            Err(BrokerError::EntityMetadataCorrupt)
        );
        assert_eq!(fixture.raw().snapshot()?, corrupt);
    }
    Ok(())
}

#[test]
fn paired_unhealthy_owner_parent_and_exact_shadow_refuse_without_repair()
-> Result<(), Box<dyn Error>> {
    corrupt_metadata(MemoryProvider::new())?;
    corrupt_metadata(DurableProvider::temporary()?)
}

fn reserved_scopes<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider, QueueConfig::default())?;
    let topic = EntityPath::new("events")?;
    fixture.at(
        &topic,
        1,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let name = SubscriptionName::new("alpha")?;
    fixture.at(
        &topic,
        2,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig::default(),
        },
    )?;
    let child = topic.subscription(&name)?;
    let before = fixture.raw().snapshot()?;
    for (entity, expected) in [
        (
            fixture.queue.dead_letter_queue()?,
            BrokerError::DeadLetterQueueIsReserved,
        ),
        (child.clone(), BrokerError::EntityPathReserved),
        (
            child.dead_letter_queue()?,
            BrokerError::DeadLetterQueueIsReserved,
        ),
        (
            EntityPath::new("orders/$management")?,
            BrokerError::EntityPathReserved,
        ),
        (
            EntityPath::new("events/subscriptions")?,
            BrokerError::EntityPathReserved,
        ),
        (topic, BrokerError::QueueNotFound),
        (EntityPath::new("missing")?, BrokerError::QueueNotFound),
    ] {
        fixture.reset();
        assert_eq!(
            fixture.at(
                &entity,
                100,
                CommandKind::UpdateQueue {
                    update: QueueConfigUpdate {
                        lock_duration_millis: Some(2),
                        ..QueueConfigUpdate::default()
                    }
                }
            ),
            Err(expected)
        );
        assert_eq!(fixture.raw().snapshot()?, before);
        assert!(fixture.trace().batches.is_empty());
        assert_eq!(fixture.trace().scans, 0);
        assert_eq!(
            fixture.machine.last_applied_time()?,
            Timestamp::from_millis(2)
        );
    }
    assert_eq!(fixture.restart()?.raw().snapshot()?, before);
    Ok(())
}

#[test]
fn paired_reserved_subscription_topic_and_missing_paths_are_not_queue_updates()
-> Result<(), Box<dyn Error>> {
    reserved_scopes(MemoryProvider::new())?;
    reserved_scopes(DurableProvider::temporary()?)
}

fn storage_failures<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new(provider, QueueConfig::default())?;
    let before = fixture.raw().snapshot()?;
    let update = QueueConfigUpdate {
        lock_duration_millis: Some(2),
        ..QueueConfigUpdate::default()
    };
    for key in [
        keys::clock(),
        keys::queue_config(&fixture.namespace, &fixture.queue),
        keys::entity_metadata(&fixture.namespace, &fixture.queue),
        keys::queue_config(&fixture.namespace, &fixture.queue.dead_letter_queue()?),
    ] {
        fixture.reset();
        fixture.machine.store().controls.lock().unwrap().fail_get = Some(key);
        assert_eq!(
            fixture.update(100, update),
            Err(BrokerError::Storage(failure("get update metadata")))
        );
        assert!(fixture.trace().batches.is_empty());
        fixture.machine.store().controls.lock().unwrap().fail_get = None;
        assert_eq!(fixture.raw().snapshot()?, before);
        assert_eq!(fixture.machine.last_applied_time()?, Timestamp::UNIX_EPOCH);
    }
    fixture.reset();
    fixture.machine.store().controls.lock().unwrap().fail_apply = true;
    assert_eq!(
        fixture.update(100, update),
        Err(BrokerError::Storage(failure("apply update batch")))
    );
    assert_eq!(fixture.trace().batches.len(), 1);
    assert_eq!(fixture.trace().batches[0].mutations().len(), 3);
    assert_eq!(fixture.raw().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, Timestamp::UNIX_EPOCH);
    let fixture = fixture.restart()?;
    assert_eq!(fixture.raw().snapshot()?, before);
    assert_eq!(fixture.update(100, update)?, CommandOutcome::QueueUpdated);
    let after = fixture.raw().snapshot()?;
    assert_eq!(fixture.restart()?.raw().snapshot()?, after);
    Ok(())
}

#[test]
fn paired_storage_get_and_atomic_apply_failures_preserve_clock_and_reopen_state()
-> Result<(), Box<dyn Error>> {
    storage_failures(MemoryProvider::new())?;
    storage_failures(DurableProvider::temporary()?)
}

fn future_settings<P: StoreProvider>(provider: P) -> Result<(), Box<dyn Error>> {
    let initial = QueueConfig {
        lock_duration_millis: 100,
        max_message_bytes: 16,
        default_time_to_live_millis: Some(1_000),
        requires_duplicate_detection: true,
        duplicate_detection_history_millis: 20_000,
        ..QueueConfig::default()
    };
    let fixture = Fixture::new(provider, initial)?;
    let old = fixture.send(1, "old", b"old-large", None)?;
    let held = fixture.receive(2)?;
    assert_eq!(held.sequence, old);
    assert_eq!(held.lock.unwrap().locked_until, Timestamp::from_millis(102));
    let scheduled = fixture.send(3, "scheduled", b"schedule", Some(500))?;
    let old_record = fixture
        .machine
        .message(&fixture.namespace, &fixture.queue, old)?
        .unwrap();
    let scheduled_record = fixture
        .machine
        .message(&fixture.namespace, &fixture.queue, scheduled)?
        .unwrap();
    assert_eq!(old_record.expires_at, Some(Timestamp::from_millis(1_001)));
    assert_eq!(
        scheduled_record.expires_at,
        Some(Timestamp::from_millis(1_500))
    );
    assert_eq!(
        fixture
            .machine
            .duplicate_history_deadline(&fixture.namespace, &fixture.queue, "old")?,
        Some(Timestamp::from_millis(20_001))
    );
    let update = QueueConfigUpdate {
        lock_duration_millis: Some(30),
        max_delivery_count: Some(2),
        max_message_bytes: Some(4),
        default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 50 }),
        duplicate_detection_history_millis: Some(40_000),
        ..QueueConfigUpdate::default()
    };
    assert_eq!(fixture.update(10, update)?, CommandOutcome::QueueUpdated);
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.queue, old)?,
        Some(old_record.clone())
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.queue, scheduled)?,
        Some(scheduled_record)
    );
    assert_eq!(
        fixture
            .machine
            .duplicate_history_deadline(&fixture.namespace, &fixture.queue, "old")?,
        Some(Timestamp::from_millis(20_001))
    );
    let fresh = fixture.send(11, "fresh", b"new", None)?;
    let fresh_delivery = fixture.receive(12)?;
    assert_eq!(fresh_delivery.sequence, fresh);
    assert_eq!(fresh_delivery.expires_at, Some(Timestamp::from_millis(61)));
    assert_eq!(
        fresh_delivery.lock.unwrap().locked_until,
        Timestamp::from_millis(42)
    );
    assert_eq!(fresh_delivery.lock.unwrap().lock_duration_millis, 30);
    assert_eq!(
        fixture
            .machine
            .duplicate_history_deadline(&fixture.namespace, &fixture.queue, "fresh")?,
        Some(Timestamp::from_millis(40_011))
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.queue, old)?,
        Some(old_record)
    );
    let before_refusal = fixture.raw().snapshot()?;
    assert_eq!(
        fixture.send(13, "oversized", b"12345", None),
        Err(BrokerError::MessageTooLarge {
            body_bytes: 5,
            maximum_bytes: 4
        })
    );
    assert_eq!(fixture.raw().snapshot()?, before_refusal);
    assert!(matches!(
        fixture.at(
            &fixture.queue,
            13,
            CommandKind::Send {
                message_id: String::from("old"),
                body: b"new".to_vec(),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None
            }
        )?,
        CommandOutcome::DuplicateSuppressed { .. }
    ));
    assert_eq!(
        fixture
            .machine
            .duplicate_history_deadline(&fixture.namespace, &fixture.queue, "old")?,
        Some(Timestamp::from_millis(20_001))
    );
    assert_eq!(
        fixture.at(
            &fixture.queue,
            14,
            CommandKind::Complete {
                sequence: old,
                lock_token: held.lock.unwrap().token
            }
        )?,
        CommandOutcome::Completed
    );
    assert_eq!(
        fixture.at(&fixture.queue, 500, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 1,
            deliverable_entities: vec![fixture.queue.clone()]
        }
    );
    let CommandOutcome::Peeked(records) = fixture.at(
        &fixture.queue,
        500,
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 10,
            session: None,
        },
    )?
    else {
        panic!("actual stored scheduled activation");
    };
    let activated = records
        .iter()
        .find(|record| record.message_id == "scheduled")
        .unwrap();
    assert_eq!(activated.expires_at, Some(Timestamp::from_millis(1_500)));
    assert_eq!(activated.body, b"schedule");
    assert_eq!(
        fixture.at(&fixture.queue, 20_001, CommandKind::ExpireDuplicateHistory)?,
        CommandOutcome::DuplicateHistoryExpired { removed: 1 }
    );
    assert_eq!(
        fixture
            .machine
            .duplicate_history_deadline(&fixture.namespace, &fixture.queue, "old")?,
        None
    );
    fixture.send(20_002, "old", b"new", None)?;
    assert_eq!(
        fixture
            .machine
            .duplicate_history_deadline(&fixture.namespace, &fixture.queue, "old")?,
        Some(Timestamp::from_millis(60_002))
    );
    let before_unlimited = fixture.raw().snapshot()?;
    assert_eq!(
        fixture.update(
            20_004,
            QueueConfigUpdate {
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Unlimited),
                ..QueueConfigUpdate::default()
            }
        )?,
        CommandOutcome::QueueUpdated
    );
    let excluded = BTreeSet::from([
        keys::clock(),
        keys::queue_config(&fixture.namespace, &fixture.queue),
    ]);
    assert_eq!(
        remaining(&before_unlimited, &excluded),
        remaining(&fixture.raw().snapshot()?, &excluded)
    );
    let unlimited = fixture.send(20_005, "unlimited", b"new", None)?;
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.queue, unlimited)?
            .unwrap()
            .expires_at
            .is_none()
    );
    let after = fixture.raw().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.raw().snapshot()?, after);
    assert_eq!(
        fixture
            .machine
            .duplicate_history_deadline(&fixture.namespace, &fixture.queue, "old")?,
        Some(Timestamp::from_millis(60_002))
    );
    Ok(())
}

#[test]
fn paired_future_settings_do_not_rebase_existing_lock_expiry_schedule_or_duplicate_deadlines()
-> Result<(), Box<dyn Error>> {
    future_settings(MemoryProvider::new())?;
    future_settings(DurableProvider::temporary()?)
}

#[test]
fn existing_command_discriminants_and_queue_storage_shape_remain_unchanged()
-> Result<(), Box<dyn Error>> {
    let sequence = SequenceNumber::new(1);
    let token = LockToken::new(1);
    let session = SessionHold {
        session_id: SessionId::new("session")?,
        token,
    };
    let kinds = vec![
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("alpha")?,
            config: SubscriptionConfig::default(),
        },
        CommandKind::CreateRule {
            name: RuleName::new("rule")?,
            filter: RuleFilter::True,
        },
        CommandKind::DeleteRule {
            name: RuleName::new("rule")?,
        },
        CommandKind::ListRules {
            skip: 0,
            max_rules: 1,
        },
        CommandKind::Send {
            message_id: String::from("id"),
            body: vec![],
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
        CommandKind::SendBatch {
            messages: vec![MessageInput::default()],
        },
        CommandKind::CancelScheduled {
            sequences: vec![sequence],
        },
        CommandKind::Peek {
            from_sequence: sequence,
            max_messages: 1,
            session: None,
        },
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
        CommandKind::Complete {
            sequence,
            lock_token: token,
        },
        CommandKind::Abandon {
            sequence,
            lock_token: token,
            replacement_envelope: None,
        },
        CommandKind::Defer {
            sequence,
            lock_token: token,
            replacement_envelope: None,
        },
        CommandKind::ReceiveDeferred {
            sequences: vec![sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
        CommandKind::DeadLetter {
            sequence,
            lock_token: token,
            reason: String::from("reason"),
            description: String::from("description"),
            replacement_envelope: None,
        },
        CommandKind::RenewLock {
            sequence,
            lock_token: token,
            lock_duration_millis: None,
        },
        CommandKind::AcceptSession {
            session_id: None,
            lock_duration_millis: None,
        },
        CommandKind::ReleaseSession {
            session: session.clone(),
        },
        CommandKind::RenewSessionLock {
            session: session.clone(),
            lock_duration_millis: None,
        },
        CommandKind::SetSessionState {
            session: session.clone(),
            state: vec![],
        },
        CommandKind::GetSessionState { session },
        CommandKind::ExpireLocks,
        CommandKind::ExpireMessages,
        CommandKind::ExpireSessionLocks,
        CommandKind::ActivateScheduled,
        CommandKind::ExpireDuplicateHistory,
    ];
    assert_eq!(kinds.len(), 27);
    for (old_index, kind) in kinds.iter().enumerate() {
        let encoded = codec::encode(kind)?;
        assert_eq!(&encoded[..2], &[codec::VALUE_FORMAT_V1, old_index as u8]);
        assert_eq!(codec::decode::<CommandKind>(&encoded)?, *kind);
    }
    let kind = CommandKind::UpdateQueue {
        update: QueueConfigUpdate::default(),
    };
    let encoded = codec::encode(&kind)?;
    assert_eq!(&encoded[..2], &[codec::VALUE_FORMAT_V1, 27]);
    assert_eq!(codec::decode::<CommandKind>(&encoded)?, kind);
    // Freeze the pre-update field order independently of QueueConfig itself.
    #[derive(Serialize)]
    struct OriginalQueueConfig {
        lock_duration_millis: u64,
        max_delivery_count: u32,
        default_time_to_live_millis: Option<u64>,
        max_message_bytes: usize,
        requires_session: bool,
        requires_duplicate_detection: bool,
        duplicate_detection_history_millis: u64,
    }
    let config = QueueConfig {
        default_time_to_live_millis: Some(123),
        requires_session: true,
        requires_duplicate_detection: true,
        ..QueueConfig::default()
    };
    let original = OriginalQueueConfig {
        lock_duration_millis: config.lock_duration_millis,
        max_delivery_count: config.max_delivery_count,
        default_time_to_live_millis: config.default_time_to_live_millis,
        max_message_bytes: config.max_message_bytes,
        requires_session: config.requires_session,
        requires_duplicate_detection: config.requires_duplicate_detection,
        duplicate_detection_history_millis: config.duplicate_detection_history_millis,
    };
    assert_eq!(codec::encode(&config)?, codec::encode(&original)?);
    assert_eq!(
        codec::decode::<QueueConfig>(&codec::encode(&original)?)?,
        config
    );
    Ok(())
}

#[test]
fn explicit_lifetime_patches_roundtrip_and_preserve_unspecified_fields_at_valid_bounds()
-> Result<(), Box<dyn Error>> {
    let current = QueueConfig {
        default_time_to_live_millis: Some(100),
        ..QueueConfig::default()
    };
    for ttl in [
        QueueTimeToLiveUpdate::Finite { millis: 1 },
        QueueTimeToLiveUpdate::Finite { millis: u64::MAX },
        QueueTimeToLiveUpdate::Unlimited,
    ] {
        let patch = QueueConfigUpdate {
            default_time_to_live_millis: Some(ttl),
            ..QueueConfigUpdate::default()
        };
        assert_eq!(
            codec::decode::<QueueConfigUpdate>(&codec::encode(&patch)?)?,
            patch
        );
        assert_eq!(
            patch.apply(current)?,
            QueueConfig {
                default_time_to_live_millis: match ttl {
                    QueueTimeToLiveUpdate::Finite { millis } => Some(millis),
                    QueueTimeToLiveUpdate::Unlimited => None,
                },
                ..current
            }
        );
    }
    for history in [
        MIN_DUPLICATE_DETECTION_HISTORY_MILLIS,
        MAX_DUPLICATE_DETECTION_HISTORY_MILLIS,
    ] {
        let patch = QueueConfigUpdate {
            lock_duration_millis: Some(MAX_LOCK_DURATION_MILLIS),
            max_delivery_count: Some(u32::MAX),
            max_message_bytes: Some(usize::MAX),
            duplicate_detection_history_millis: Some(history),
            ..QueueConfigUpdate::default()
        };
        assert_eq!(
            patch.apply(current)?,
            QueueConfig {
                lock_duration_millis: MAX_LOCK_DURATION_MILLIS,
                max_delivery_count: u32::MAX,
                max_message_bytes: usize::MAX,
                duplicate_detection_history_millis: history,
                ..current
            }
        );
    }
    assert_eq!(
        QueueConfigUpdate {
            lock_duration_millis: Some(1),
            ..QueueConfigUpdate::default()
        }
        .apply(current)?,
        QueueConfig {
            lock_duration_millis: 1,
            ..current
        }
    );
    assert_eq!(
        codec::encode(&QueueTimeToLiveUpdate::Finite { millis: 1 })?,
        vec![codec::VALUE_FORMAT_V1, 0, 1]
    );
    assert_eq!(
        codec::encode(&QueueTimeToLiveUpdate::Unlimited)?,
        vec![codec::VALUE_FORMAT_V1, 1]
    );
    Ok(())
}
