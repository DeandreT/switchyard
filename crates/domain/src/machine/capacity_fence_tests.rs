//! Exercise the unchanged pre-ledger dispatcher below the new preflight too.
//! The legacy codecs and every handler remain the production implementations;
//! this is not a second hand-written model of Complete or timer behavior.

use super::*;
use crate::{
    BoundCommand, EntityBinding, QueueCapacity, RawMessage, RuleFilter, RuleName, SessionId,
    SubscriptionConfig, SubscriptionConfigUpdate, SubscriptionName, TopicConfig, TopicConfigUpdate,
    capacity_fence::{self, CapacityIntent},
    retained_accounting::{CapacityOwner, RetainedUsage, keys::usage, usage_codec},
};
use storage::{Key, StorageError, StoreSnapshot, Value};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

fn owner() -> CapacityOwner {
    CapacityOwner::new(
        NamespaceName::new("n").unwrap(),
        EntityPath::new("q").unwrap(),
        1,
    )
    .unwrap()
}

fn finite() -> QueueConfig {
    QueueConfig {
        capacity: QueueCapacity::Finite {
            max_size_bytes: crate::QUEUE_CAPACITY_GRANULARITY_BYTES,
        },
        ..QueueConfig::default()
    }
}

fn command(entity: &EntityPath, kind: CommandKind) -> Command {
    Command::new(
        owner().namespace().clone(),
        entity.clone(),
        Timestamp::from_millis(2),
        kind,
    )
}

fn commands() -> Vec<CommandKind> {
    let sequence = SequenceNumber::new(1);
    let lock_token = LockToken::new(1);
    let session = SessionHold {
        session_id: SessionId::new("s").unwrap(),
        token: lock_token,
    };
    let name = SubscriptionName::new("s").unwrap();
    vec![
        CommandKind::CreateQueue { config: finite() },
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig::default(),
        },
        CommandKind::CreateRule {
            name: RuleName::new("r").unwrap(),
            filter: RuleFilter::True,
        },
        CommandKind::DeleteRule {
            name: RuleName::new("r").unwrap(),
        },
        CommandKind::ListRules {
            skip: 0,
            max_rules: 1,
        },
        CommandKind::Send {
            message_id: "id".into(),
            body: vec![1],
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
        CommandKind::SendBatch { messages: vec![] },
        CommandKind::SendContent {
            content: MessageContent::Raw(RawMessage::default()),
        },
        CommandKind::SendContentBatch { messages: vec![] },
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
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
        CommandKind::Complete {
            sequence,
            lock_token,
        },
        CommandKind::Abandon {
            sequence,
            lock_token,
            replacement_envelope: None,
        },
        CommandKind::Defer {
            sequence,
            lock_token,
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
            lock_token,
            reason: "r".into(),
            description: "d".into(),
            replacement_envelope: None,
        },
        CommandKind::AbandonWithPatch {
            sequence,
            lock_token,
            patch: vec![],
        },
        CommandKind::DeferWithPatch {
            sequence,
            lock_token,
            patch: vec![],
        },
        CommandKind::DeadLetterWithPatch {
            sequence,
            lock_token,
            reason: "r".into(),
            description: "d".into(),
            patch: vec![],
        },
        CommandKind::RenewLock {
            sequence,
            lock_token,
            lock_duration_millis: None,
        },
        CommandKind::AcceptSession {
            session_id: Some(session.session_id.clone()),
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
            state: vec![7],
        },
        CommandKind::GetSessionState { session },
        CommandKind::ExpireLocks,
        CommandKind::ExpireMessages,
        CommandKind::ExpireSessionLocks,
        CommandKind::ActivateScheduled,
        CommandKind::ExpireDuplicateHistory,
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate::default(),
        },
        CommandKind::UpdateTopic {
            update: TopicConfigUpdate::default(),
        },
        CommandKind::UpdateSubscription {
            name,
            update: SubscriptionConfigUpdate::default(),
        },
        CommandKind::CreateRuleWithAction {
            name: RuleName::new("action-rule").unwrap(),
            filter: RuleFilter::True,
            action: crate::SqlRuleAction::new("SET copied = 1"),
        },
    ]
}

fn setup<S: StateStore + Clone>(store: S, populated: bool) -> Vec<EntityBinding> {
    let owner = owner();
    let machine = StateMachine::new(store.clone());
    machine
        .apply(&command(
            owner.parent(),
            CommandKind::CreateQueue { config: finite() },
        ))
        .unwrap();
    let bindings = [owner.parent(), owner.shadow()]
        .iter()
        .map(|entity| machine.bind_entity(owner.namespace(), entity).unwrap())
        .collect();
    let config = finite();
    let shadow = config.dead_letter_shadow();
    let mut batch = WriteBatch::default()
        .put(
            keys::queue_config(owner.namespace(), owner.parent()),
            capacity_fence::encode_profile(&owner, owner.parent(), &config).unwrap(),
        )
        .put(
            keys::queue_config(owner.namespace(), owner.shadow()),
            capacity_fence::encode_profile(&owner, owner.shadow(), &shadow).unwrap(),
        )
        .put(
            usage(&owner),
            usage_codec::encode(&owner, RetainedUsage::new(1, 0, 0, 0).unwrap()).unwrap(),
        );
    if populated {
        for entity in [owner.parent(), owner.shadow()] {
            let sequence = SequenceNumber::new(1);
            let locked_until = Timestamp::from_millis(100);
            let record = MessageRecord {
                sequence,
                message_id: "held".into(),
                body: vec![1, 2],
                enqueued_at: Timestamp::from_millis(1),
                scheduled_enqueue_at: None,
                expires_at: None,
                delivery_count: 1,
                state: MessageState::Locked {
                    token: LockToken::new(1),
                    locked_until,
                    origin: DeliveryOrigin::Ready,
                },
                session_id: None,
                dead_letter: (entity == owner.shadow()).then(|| DeadLetterInfo {
                    reason: DeadLetterReason::MaxDeliveryCountExceeded,
                    description: "the message reached its maximum delivery count".into(),
                    dead_lettered_at: Timestamp::from_millis(1),
                }),
                envelope: None,
                format: MessageRecordFormat::V1,
            };
            batch.push_put(
                capacity_fence::data_key(&keys::message(owner.namespace(), entity, sequence))
                    .unwrap(),
                codec::message::encode(&record).unwrap(),
            );
            batch.push_put(
                capacity_fence::data_key(&keys::lock(
                    owner.namespace(),
                    entity,
                    locked_until,
                    sequence,
                ))
                .unwrap(),
                vec![],
            );
            batch.push_put(
                capacity_fence::data_key(&keys::queue_counters(owner.namespace(), entity)).unwrap(),
                codec::encode(&QueueCounters::default()).unwrap(),
            );
        }
        // Intentionally stale/corrupt isolated rows: no old writer may discover
        // or manufacture any old-layout companions while trying to repair them.
        for entity in [owner.parent(), owner.shadow()] {
            for tag in 2..=13 {
                let mut key = vec![capacity_fence::DATA_TAG, tag];
                key.extend_from_slice(&keys::queue_config(owner.namespace(), entity)[1..]);
                key.extend_from_slice(&[0; 16]);
                batch.push_put(key, vec![0xff]);
            }
        }
    }
    store.apply(batch).unwrap();
    bindings
}

fn legacy_paths<P: StoreProvider>(provider: &P, populated: bool)
where
    P::Store: Clone,
{
    let store = provider.open().unwrap();
    let bindings = setup(store.clone(), populated);
    let before = store.snapshot().unwrap();
    let machine = StateMachine::new(store.clone());
    for entity in [owner().parent(), owner().shadow()] {
        assert!(matches!(
            machine.held_lock(
                &command(entity, CommandKind::ExpireMessages),
                SequenceNumber::new(1),
                LockToken::new(1)
            ),
            Err(BrokerError::MessageNotFound { .. })
        ));
        for kind in commands() {
            let command = command(entity, kind);
            if let Ok((_, batch)) = machine.prepare_unfenced(&command) {
                assert!(
                    batch.is_empty(),
                    "old writer would write: {:?}",
                    command.kind
                );
            }
            assert_eq!(store.snapshot().unwrap(), before);
        }
    }
    drop(machine);
    drop(store);
    let reopened = provider.open().unwrap();
    assert_eq!(reopened.snapshot().unwrap(), before);
    let guarded = StateMachine::new(NoClock(reopened.clone()));
    for binding in bindings {
        for kind in commands() {
            let command = command(binding.target(), kind);
            assert_eq!(
                guarded.apply(&command),
                Err(BrokerError::CapacityStateUnsupported)
            );
            assert_eq!(
                guarded.apply_bound(&BoundCommand::new(binding.clone(), command)),
                Err(BrokerError::CapacityStateUnsupported)
            );
        }
    }
    assert_eq!(reopened.snapshot().unwrap(), before);
}

#[test]
fn preledger_all_handlers_cannot_create_or_change_parent_or_shadow_rows_memory() {
    // A fresh provider is needed per fixture because MemoryProvider persists.
    for populated in [false, true] {
        legacy_paths(&MemoryProvider::new(), populated);
    }
}

#[test]
fn preledger_all_handlers_cannot_create_or_change_parent_or_shadow_rows_fjall() {
    for populated in [false, true] {
        legacy_paths(&DurableProvider::temporary().unwrap(), populated);
    }
}

#[derive(Clone)]
struct NoClock<S>(S);
impl<S: StateStore> StateStore for NoClock<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        assert_ne!(key, [0], "capacity refusal must precede Clock");
        assert!(
            matches!(key.first(), Some(1 | 0x0E | 0x11 | 0x12)),
            "refusal must not read counters or state"
        );
        let (namespace, entity) = keys::entity_scope_parts(key).unwrap();
        assert_eq!(namespace, owner().namespace().as_str());
        assert!(entity == owner().parent().as_str() || entity == owner().shadow().as_str());
        self.0.get(key)
    }
    fn apply(&self, _: WriteBatch) -> Result<(), StorageError> {
        panic!("refusal must not write")
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.0.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        assert_eq!(prefix.first(), Some(&capacity_fence::DATA_TAG));
        assert!(matches!(prefix.get(1), Some(2..=13)));
        assert_eq!(prefix, start);
        assert_eq!(limit, 1);
        let (namespace, entity) = keys::entity_scope_parts(&prefix[1..]).unwrap();
        assert_eq!(namespace, owner().namespace().as_str());
        assert!(entity == owner().parent().as_str() || entity == owner().shadow().as_str());
        self.0.scan_from(prefix, start, limit)
    }
}

#[test]
fn every_new_direct_and_bound_path_refuses_before_clock_or_writes() {
    for condition in 0..6 {
        let store = storage::MemoryStore::default();
        let bindings = setup(store.clone(), true);
        let owner = owner();
        match condition {
            1 => store
                .apply(WriteBatch::default().delete(usage(&owner)))
                .unwrap(),
            2 => store
                .apply(WriteBatch::default().put(usage(&owner), vec![0; 72]))
                .unwrap(),
            3 => store
                .apply(WriteBatch::default().put(
                    keys::queue_config(owner.namespace(), owner.shadow()),
                    codec::queue::encode(&finite().dead_letter_shadow()).unwrap(),
                ))
                .unwrap(),
            4 => {
                // Never adopt mixed legacy rows under a new profile. Even a
                // valid old locked primary must not let Complete bypass it.
                let mut batch = WriteBatch::default();
                for entity in [owner.parent(), owner.shadow()] {
                    let old_key = keys::message(owner.namespace(), entity, SequenceNumber::new(1));
                    let value = store
                        .get(&capacity_fence::data_key(&old_key).unwrap())
                        .unwrap()
                        .unwrap();
                    batch.push_put(old_key, value);
                }
                store.apply(batch).unwrap();
            }
            5 => {
                let stale =
                    CapacityOwner::new(owner.namespace().clone(), owner.parent().clone(), 2)
                        .unwrap();
                store
                    .apply(
                        WriteBatch::default().put(
                            usage(&owner),
                            usage_codec::encode(&stale, RetainedUsage::new(2, 0, 0, 0).unwrap())
                                .unwrap(),
                        ),
                    )
                    .unwrap();
            }
            _ => {}
        }
        let expected = match condition {
            1 => BrokerError::CapacityUsageMissing,
            2 | 3 | 5 => BrokerError::CapacityStateCorrupt,
            _ => BrokerError::CapacityStateUnsupported,
        };
        let before = store.snapshot().unwrap();
        let machine = StateMachine::new(NoClock(store.clone()));
        for binding in bindings {
            for kind in commands() {
                let command = command(binding.target(), kind);
                assert_eq!(
                    machine.apply(&command),
                    Err(expected.clone()),
                    "{:?}",
                    command.kind
                );
                assert_eq!(
                    machine.apply_bound(&BoundCommand::new(binding.clone(), command)),
                    Err(expected.clone())
                );
                assert_eq!(store.snapshot().unwrap(), before);
            }
        }
    }
}

#[test]
fn refusal_is_selected_owner_only_and_legacy_v7_does_not_become_owned() {
    let store = storage::MemoryStore::default();
    setup(store.clone(), true);
    let machine = StateMachine::new(store.clone());
    let other = EntityPath::new("other").unwrap();
    let proposal = crate::DurableProposal::unbound(command(
        &other,
        CommandKind::CreateQueue { config: finite() },
    ));
    let historical = proposal.encode().unwrap();
    assert_eq!(&historical[4..8], &7_u32.to_be_bytes());
    machine.apply(proposal.command()).unwrap();
    let raw = store
        .get(&keys::queue_config(owner().namespace(), &other))
        .unwrap()
        .unwrap();
    assert_eq!(raw[0], 3);
    machine
        .apply(&command(&other, commands().remove(6)))
        .unwrap();
    let old_owner = CapacityOwner::new(owner().namespace().clone(), other, 1).unwrap();
    assert!(store.get(&usage(&old_owner)).unwrap().is_none());
    let intent = CapacityIntent::capture(old_owner, proposal).unwrap();
    assert_eq!(intent.original().encode().unwrap(), historical);
}

#[test]
fn capacity_refusals_are_not_checkpointed_or_implicitly_adopted() {
    for missing in [false, true] {
        let store = storage::MemoryStore::default();
        let mut writer = crate::IndexedWriter::open(store.clone()).unwrap();
        // Serialized raw fixture injection, not supported concurrent raw writes.
        setup(store.clone(), false);
        if missing {
            store
                .apply(WriteBatch::default().delete(usage(&owner())))
                .unwrap();
        }
        let before = store.snapshot().unwrap();
        let proposal = crate::DurableProposal::unbound(command(
            owner().shadow(),
            CommandKind::Complete {
                sequence: SequenceNumber::new(1),
                lock_token: LockToken::new(1),
            },
        ));
        let error = if missing {
            BrokerError::CapacityUsageMissing
        } else {
            BrokerError::CapacityStateUnsupported
        };
        assert_eq!(
            writer.apply(1, &proposal),
            Err(crate::IndexedApplyError::Domain(error))
        );
        assert_eq!(writer.applied_index(), Ok(0));
        assert_eq!(store.snapshot().unwrap(), before);
        drop(writer);
        assert!(crate::IndexedWriter::open(store.clone()).is_err());
        assert_eq!(store.snapshot().unwrap(), before);
    }
}

#[test]
fn orphan_isolated_rows_never_adopt_legacy_profiles_even_after_bound_validation() {
    let owner = owner();
    let store = storage::MemoryStore::default();
    let machine = StateMachine::new(store.clone());
    machine
        .apply(&command(
            owner.parent(),
            CommandKind::CreateQueue { config: finite() },
        ))
        .unwrap();
    let binding = machine
        .bind_entity(owner.namespace(), owner.shadow())
        .unwrap();
    let key = capacity_fence::data_key(&keys::message(
        owner.namespace(),
        owner.shadow(),
        SequenceNumber::new(1),
    ))
    .unwrap();
    store
        .apply(WriteBatch::default().put(key, vec![0xff]))
        .unwrap();
    let before = store.snapshot().unwrap();
    let guarded = StateMachine::new(NoClock(store.clone()));
    let command = command(
        owner.shadow(),
        CommandKind::Complete {
            sequence: SequenceNumber::new(1),
            lock_token: LockToken::new(1),
        },
    );
    assert_eq!(
        guarded.apply(&command),
        Err(BrokerError::CapacityStateCorrupt)
    );
    assert_eq!(
        guarded.apply_bound(&BoundCommand::new(binding, command)),
        Err(BrokerError::CapacityStateCorrupt)
    );
    assert_eq!(store.snapshot().unwrap(), before);
}
