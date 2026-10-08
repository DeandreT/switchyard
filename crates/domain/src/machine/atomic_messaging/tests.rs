use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::EntityBinding;
use storage::{Key, MemoryStore, StorageError, StoreSnapshot, Value};

use super::*;

#[derive(Clone, Default)]
struct PointStore {
    memory: MemoryStore,
    commits: Arc<AtomicUsize>,
    scans: Arc<AtomicUsize>,
    metadata_probes: Arc<AtomicUsize>,
    topic_mode_probe: Option<Key>,
    gets: Arc<AtomicUsize>,
    fail: Arc<std::sync::atomic::AtomicBool>,
}

impl StateStore for PointStore {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.memory.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: String::from("injected"),
            });
        }
        self.memory.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        Err(StorageError::Backend {
            operation: "snapshot",
            detail: String::from("forbidden"),
        })
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        if self.topic_mode_probe.as_deref() == Some(prefix) && start == prefix && limit == 1 {
            self.metadata_probes.fetch_add(1, Ordering::SeqCst);
            return self.memory.scan_from(prefix, start, limit);
        }
        self.scans.fetch_add(1, Ordering::SeqCst);
        Err(StorageError::Backend {
            operation: "scan",
            detail: String::from("forbidden"),
        })
    }
}

fn fixture() -> Result<(StateMachine<PointStore>, EntityBinding), BrokerError> {
    let mut store = PointStore::default();
    let setup = StateMachine::new(store.memory.clone());
    let namespace = NamespaceName::new("test")?;
    let entity = EntityPath::new("queue")?;
    setup.apply(&Command::new(
        namespace.clone(),
        entity.clone(),
        Timestamp::from_millis(1),
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    ))?;
    store.topic_mode_probe = Some(keys::subscription_topic_mode_prefix(&namespace, &entity));
    let machine = StateMachine::new(store);
    let binding = machine
        .bind_entity(&namespace, &entity, &entity, EntityIncarnationKind::Queue)?
        .ok_or(BrokerError::QueueNotFound)?;
    machine.store.commits.store(0, Ordering::SeqCst);
    machine.store.metadata_probes.store(0, Ordering::SeqCst);
    machine.store.gets.store(0, Ordering::SeqCst);
    Ok((machine, binding))
}

fn envelope(binding: EntityBinding, kinds: Vec<CommandKind>) -> AtomicMessagingCommand {
    let issued_at = Timestamp::from_millis(2);
    let commands = kinds
        .into_iter()
        .map(|kind| {
            Command::new(
                binding.namespace().clone(),
                binding.target().clone(),
                issued_at,
                kind,
            )
        })
        .collect();
    AtomicMessagingCommand {
        binding,
        issued_at,
        commands,
    }
}

fn send(id: &str) -> CommandKind {
    CommandKind::Send {
        message_id: id.to_owned(),
        body: vec![1],
        time_to_live_millis: None,
        session_id: None,
    }
}

#[test]
fn preparation_allows_only_scoped_metadata_probes_and_one_real_commit() -> Result<(), BrokerError> {
    let (machine, binding) = fixture()?;
    let application = machine
        .apply_atomic_messaging(&envelope(binding.clone(), vec![send("one"), send("two")]))?;
    assert_eq!(application.outcomes.len(), 2);
    assert_eq!(application.enqueue_targets, vec![binding.target().clone()]);
    assert_eq!(machine.store.commits.load(Ordering::SeqCst), 1);
    assert_eq!(machine.store.scans.load(Ordering::SeqCst), 0);
    // Admission plus an ingress and capacity proof for each send.
    assert_eq!(machine.store.metadata_probes.load(Ordering::SeqCst), 5);
    let counters = machine
        .read::<QueueCounters>(&keys::queue_counters(binding.namespace(), binding.target()))?
        .ok_or(BrokerError::DanglingEntityMetadata)?;
    assert_eq!(counters.next_sequence, 3);
    Ok(())
}

#[test]
fn late_settlement_error_and_failed_commit_publish_nothing() -> Result<(), BrokerError> {
    let (machine, binding) = fixture()?;
    let before = machine.store.memory.snapshot()?;
    let failed = envelope(
        binding.clone(),
        vec![
            send("one"),
            CommandKind::Complete {
                sequence: SequenceNumber::new(999),
                lock_token: LockToken::new(1),
            },
        ],
    );
    assert!(machine.apply_atomic_messaging(&failed).is_err());
    assert_eq!(machine.store.memory.snapshot()?, before);
    assert_eq!(machine.store.commits.load(Ordering::SeqCst), 0);
    machine.store.fail.store(true, Ordering::SeqCst);
    assert!(matches!(
        machine.apply_atomic_messaging(&envelope(binding, vec![send("one")])),
        Err(BrokerError::Storage(_))
    ));
    assert_eq!(machine.store.memory.snapshot()?, before);
    Ok(())
}

#[test]
fn empty_envelope_ignores_clock_but_still_validates_current_identity() -> Result<(), BrokerError> {
    let (machine, binding) = fixture()?;
    let before = machine.store.memory.snapshot()?;
    let empty = AtomicMessagingCommand {
        binding: binding.clone(),
        issued_at: Timestamp::UNIX_EPOCH,
        commands: Vec::new(),
    };
    machine.validate_atomic_messaging(&empty)?;
    let result = machine.apply_atomic_messaging(&empty)?;
    assert!(result.outcomes.is_empty() && result.enqueue_targets.is_empty());
    assert_eq!(machine.store.memory.snapshot()?, before);
    assert_eq!(machine.store.commits.load(Ordering::SeqCst), 0);
    assert_eq!(machine.store.metadata_probes.load(Ordering::SeqCst), 2);
    machine
        .store
        .memory
        .apply(WriteBatch::default().delete(keys::entity_incarnation(
            binding.namespace(),
            binding.owner(),
        )))?;
    assert_eq!(
        machine.apply_atomic_messaging(&empty),
        Err(BrokerError::DanglingEntityMetadata)
    );
    Ok(())
}

#[test]
fn every_allowed_held_settlement_allows_only_scoped_metadata_probes() -> Result<(), BrokerError> {
    for operation in 0..5 {
        let (machine, binding) = fixture()?;
        let seeder = StateMachine::new(machine.store.memory.clone());
        seeder.apply(&Command::new(
            binding.namespace().clone(),
            binding.target().clone(),
            Timestamp::from_millis(1),
            send("held"),
        ))?;
        let received = seeder.apply(&Command::new(
            binding.namespace().clone(),
            binding.target().clone(),
            Timestamp::from_millis(1),
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
        ))?;
        let CommandOutcome::Received(Some(delivery)) = received else {
            return Err(BrokerError::QueueNotFound);
        };
        let token = delivery
            .lock
            .ok_or(BrokerError::MessageNotLocked {
                sequence: delivery.sequence,
            })?
            .token;
        let kind = match operation {
            0 => CommandKind::Complete {
                sequence: delivery.sequence,
                lock_token: token,
            },
            1 => CommandKind::Abandon {
                sequence: delivery.sequence,
                lock_token: token,
            },
            2 => CommandKind::Defer {
                sequence: delivery.sequence,
                lock_token: token,
            },
            3 => CommandKind::DeadLetter {
                sequence: delivery.sequence,
                lock_token: token,
                reason: String::from("test"),
                description: String::new(),
            },
            _ => CommandKind::Settle {
                sequence: delivery.sequence,
                lock_token: token,
                disposition: SettlementDisposition::Abandon,
                properties_to_modify: BTreeMap::from([(
                    String::from("flag"),
                    MessageValue::Bool(true),
                )]),
            },
        };
        let result = machine.apply_atomic_messaging(&envelope(binding.clone(), vec![kind]))?;
        let expected = match operation {
            1 | 4 => vec![binding.target().clone()],
            3 => vec![binding.target().dead_letter_queue()?],
            _ => Vec::new(),
        };
        assert_eq!(result.enqueue_targets, expected);
        assert_eq!(machine.store.commits.load(Ordering::SeqCst), 1);
        assert_eq!(machine.store.scans.load(Ordering::SeqCst), 0);
        assert_eq!(machine.store.metadata_probes.load(Ordering::SeqCst), 2);
    }
    Ok(())
}

#[test]
fn atomic_prelude_priority_rejects_before_store_work() -> Result<(), BrokerError> {
    #[derive(serde::Serialize)]
    struct BindingBytes {
        namespace: NamespaceName,
        target: EntityPath,
        owner: EntityPath,
        kind: EntityIncarnationKind,
        generation: u64,
    }
    let (machine, binding) = fixture()?;
    let invalid: EntityBinding = codec::decode(&codec::encode(&BindingBytes {
        namespace: binding.namespace().clone(),
        target: binding.target().clone(),
        owner: binding.owner().clone(),
        kind: EntityIncarnationKind::Topic,
        generation: 0,
    })?)?;
    let valid_topic = EntityBinding::new(
        binding.namespace().clone(),
        binding.owner().clone(),
        binding.owner().clone(),
        EntityIncarnationKind::Topic,
        binding.generation(),
    )?;
    let mut wrong_scope = envelope(invalid.clone(), vec![send("scope")]);
    wrong_scope.commands[0].namespace = NamespaceName::new("other")?;
    let mut unsupported = wrong_scope.clone();
    unsupported.commands[0].kind = CommandKind::ExpireLocks;
    let mut too_many = wrong_scope.clone();
    too_many.commands =
        vec![wrong_scope.commands[0].clone(); crate::MAX_ATOMIC_MESSAGING_ACTIONS + 1];
    for (input, expected) in [
        (
            unsupported,
            BrokerError::AtomicMessagingOperationNotSupported,
        ),
        (too_many, crate::AtomicMessagingLimit::Actions.exceeded()),
        (wrong_scope, BrokerError::InvalidAtomicMessagingCommand),
        (envelope(invalid, vec![]), BrokerError::InvalidEntityBinding),
        (
            envelope(valid_topic, vec![]),
            BrokerError::AtomicMessagingOperationNotSupported,
        ),
    ] {
        assert_eq!(
            machine.validate_atomic_messaging(&input),
            Err(expected.clone())
        );
        assert_eq!(machine.apply_atomic_messaging(&input), Err(expected));
        assert_eq!(machine.store.gets.load(Ordering::SeqCst), 0);
        assert_eq!(machine.store.metadata_probes.load(Ordering::SeqCst), 0);
        assert_eq!(machine.store.scans.load(Ordering::SeqCst), 0);
        assert_eq!(machine.store.commits.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[test]
fn atomic_gate_rejects_ghost_topic_mode_without_staging_or_commit() -> Result<(), BrokerError> {
    let (machine, binding) = fixture()?;
    let child = binding
        .owner()
        .subscription(&crate::SubscriptionName::new("ghost")?)?;
    machine.store.memory.apply(
        WriteBatch::default().put(keys::topic_mode(binding.namespace(), &child), vec![255]),
    )?;
    let before = machine.store.memory.snapshot()?;
    let input = envelope(binding, vec![send("not-staged")]);
    assert_eq!(
        machine.validate_atomic_messaging(&input),
        Err(BrokerError::TopicCapacityCorrupt)
    );
    assert_eq!(
        machine.apply_atomic_messaging(&input),
        Err(BrokerError::TopicCapacityCorrupt)
    );
    assert_eq!(machine.store.metadata_probes.load(Ordering::SeqCst), 2);
    assert_eq!(machine.store.scans.load(Ordering::SeqCst), 0);
    assert_eq!(machine.store.commits.load(Ordering::SeqCst), 0);
    assert_eq!(machine.store.memory.snapshot()?, before);
    Ok(())
}
