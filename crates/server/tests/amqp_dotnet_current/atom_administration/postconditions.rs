use std::collections::BTreeSet;

use domain::{
    CommandKind, CommandOutcome, EntityPath, FiniteQueueCapacity, MessageState, NamespaceName,
    QueueCapacityStatus, QueueConfig, SequenceNumber, SessionId, StateMachine, Timestamp, keys,
};
use server::{Broker, BrokerHandle, LocalProposer, ManualClock};
use storage::{MemoryStore, Mutation, StateStore, StoreSnapshot, WriteBatch};

use super::{TestResult, process::AtomScenario};

pub(super) const SUFFIXES: [&str; 2] = ["named", "connection"];

pub(super) fn default_config() -> QueueConfig {
    QueueConfig {
        duplicate_detection_history_time_window_millis: 60_000,
        ..QueueConfig::default()
    }
}

pub(super) fn definition_config(stage: usize) -> QueueConfig {
    let (lock, message, deliveries, ttl, dead_letter) = match stage {
        0 => (15_000, 4 * 1024, 3, Some(45_000), true),
        1 => (10_000, 16 * 1024, 6, Some(90_000), false),
        2 => (20_000, 8 * 1024, 4, None, true),
        _ => panic!("closed SDK definition stage"),
    };
    QueueConfig {
        lock_duration_millis: lock,
        max_message_bytes: message,
        max_delivery_count: deliveries,
        default_time_to_live_millis: ttl,
        dead_lettering_on_message_expiration: dead_letter,
        ..default_config()
    }
}

pub(super) fn retention_config(updated: bool) -> QueueConfig {
    if updated {
        QueueConfig {
            lock_duration_millis: 10_000,
            max_message_bytes: 4 * 1024,
            max_delivery_count: 3,
            default_time_to_live_millis: None,
            dead_lettering_on_message_expiration: true,
            ..default_config()
        }
    } else {
        QueueConfig {
            default_time_to_live_millis: Some(600_000),
            ..default_config()
        }
    }
}

pub(super) fn mib(value: u64) -> FiniteQueueCapacity {
    FiniteQueueCapacity::new(value * 1024 * 1024).expect("fixed SDK logical limit")
}

pub(super) struct Oracle {
    broker: Broker,
    store: MemoryStore,
}

impl Oracle {
    pub(super) fn new() -> Self {
        let store = MemoryStore::default();
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            ManualClock::at(1_000),
        ));
        Self { broker, store }
    }

    pub(super) fn handle(&self) -> BrokerHandle {
        self.broker.handle()
    }

    pub(super) fn compare(&self, actual: &StoreSnapshot) -> TestResult {
        assert_eq!(
            actual,
            &self.store.snapshot()?,
            "official SDK diverged from complete native Memory oracle"
        );
        Ok(())
    }

    pub(super) fn advance(&self, namespace: &NamespaceName, scenario: AtomScenario) -> TestResult {
        let handle = self.handle();
        match scenario {
            AtomScenario::Create => {
                for suffix in SUFFIXES {
                    create(
                        &handle,
                        namespace,
                        &format!("sdk-atom-default-{suffix}"),
                        default_config(),
                        1024,
                    )?;
                    create(
                        &handle,
                        namespace,
                        &format!("sdk-atom-definition-{suffix}"),
                        definition_config(0),
                        2,
                    )?;
                }
            }
            AtomScenario::Update => {
                for suffix in SUFFIXES {
                    let entity = EntityPath::new(format!("sdk-atom-definition-{suffix}"))?;
                    handle.update_atom_finite_queue_blocking(
                        namespace.clone(),
                        entity.clone(),
                        definition_config(1),
                        mib(4),
                    )?;
                    handle.update_atom_finite_queue_blocking(
                        namespace.clone(),
                        entity,
                        definition_config(2),
                        mib(3),
                    )?;
                }
            }
            AtomScenario::Retention => {
                for suffix in SUFFIXES {
                    handle.update_atom_finite_queue_blocking(
                        namespace.clone(),
                        EntityPath::new(format!("sdk-atom-retention-{suffix}"))?,
                        retention_config(true),
                        mib(3),
                    )?;
                }
            }
            AtomScenario::Delete => {
                for suffix in SUFFIXES {
                    for kind in ["default", "definition"] {
                        handle.delete_atom_finite_queue_blocking(
                            namespace.clone(),
                            EntityPath::new(format!("sdk-atom-{kind}-{suffix}"))?,
                        )?;
                    }
                    create(
                        &handle,
                        namespace,
                        &format!("sdk-atom-default-{suffix}"),
                        default_config(),
                        1024,
                    )?;
                }
            }
            AtomScenario::Paging => {
                for index in 0..101 {
                    create(
                        &handle,
                        namespace,
                        &format!("sdk-atom-page-{index:03}"),
                        default_config(),
                        1024,
                    )?;
                }
                for index in 0..101 {
                    handle.delete_atom_finite_queue_blocking(
                        namespace.clone(),
                        EntityPath::new(format!("sdk-atom-page-{index:03}"))?,
                    )?;
                }
            }
            AtomScenario::Empty
            | AtomScenario::Noop
            | AtomScenario::Refusals
            | AtomScenario::Quota
            | AtomScenario::Denied
            | AtomScenario::TlsRefused => {}
        }
        Ok(())
    }
}

fn create(
    handle: &BrokerHandle,
    namespace: &NamespaceName,
    name: &str,
    config: QueueConfig,
    limit: u64,
) -> TestResult {
    handle.create_finite_queue_blocking(
        namespace.clone(),
        EntityPath::new(name)?,
        config,
        mib(limit),
    )?;
    Ok(())
}

pub(super) fn seed_quota(handle: &BrokerHandle, namespace: &NamespaceName) -> TestResult {
    for suffix in SUFFIXES {
        let name = format!("sdk-atom-quota-{suffix}");
        create(handle, namespace, &name, default_config(), 2)?;
        for sequence in 1..=5 {
            let outcome = handle.submit_blocking(
                namespace.clone(),
                EntityPath::new(&name)?,
                CommandKind::Send {
                    message_id: format!("sdk-quota-{suffix}-{sequence}"),
                    body: vec![sequence as u8; 256 * 1024],
                    time_to_live_millis: None,
                    session_id: None,
                },
            )?;
            assert_eq!(
                outcome,
                CommandOutcome::Sent {
                    sequence: SequenceNumber::new(sequence)
                }
            );
        }
    }
    Ok(())
}

pub(super) fn seed_retention(handle: &BrokerHandle, namespace: &NamespaceName) -> TestResult {
    for suffix in SUFFIXES {
        let name = format!("sdk-atom-retention-{suffix}");
        create(handle, namespace, &name, retention_config(false), 2)?;
        let outcome = handle.submit_blocking(
            namespace.clone(),
            EntityPath::new(&name)?,
            CommandKind::Send {
                message_id: format!("sdk-retained-{suffix}"),
                body: vec![0x72; 16 * 1024],
                time_to_live_millis: None,
                session_id: Some(SessionId::new(format!("metadata-{suffix}"))?),
            },
        )?;
        assert_eq!(
            outcome,
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(1)
            }
        );
    }
    Ok(())
}

pub(super) fn retained_update_only(
    namespace: &NamespaceName,
    before: &StoreSnapshot,
    after: &StoreSnapshot,
    batches: &[WriteBatch],
) -> TestResult {
    let mut allowed = BTreeSet::from([keys::clock()]);
    let mut expected_batches = BTreeSet::new();
    for suffix in SUFFIXES {
        let entity = EntityPath::new(format!("sdk-atom-retention-{suffix}"))?;
        let expected = BTreeSet::from([
            keys::queue_config(namespace, &entity),
            keys::queue_config(namespace, &entity.dead_letter_queue()?),
            keys::queue_capacity_mode(namespace, &entity),
            keys::clock(),
        ]);
        allowed.extend(expected.clone());
        expected_batches.insert(expected);
    }
    let untouched = |snapshot: &StoreSnapshot| {
        snapshot
            .entries()
            .iter()
            .filter(|(key, _)| !allowed.contains(key))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        untouched(before),
        untouched(after),
        "definition update changed retained records, opaque Usage/Charge, counters, metadata or deadlines"
    );
    assert_eq!(
        batches.len(),
        2,
        "each constructor's definition must commit one batch"
    );
    let mut actual_batches = BTreeSet::new();
    for batch in batches {
        assert_eq!(batch.mutations().len(), 4);
        let mut changed = BTreeSet::new();
        for mutation in batch.mutations() {
            let Mutation::Put { key, .. } = mutation else {
                panic!("definition update deleted runtime state");
            };
            assert!(changed.insert(key.clone()), "duplicate definition mutation");
        }
        actual_batches.insert(changed);
    }
    assert_eq!(
        actual_batches, expected_batches,
        "definition commit touched keys outside its prepared mutation"
    );
    Ok(())
}

pub(super) fn check_retained<S: StateStore>(store: &S, namespace: &NamespaceName) -> TestResult {
    let machine = StateMachine::new(store.clone());
    for suffix in SUFFIXES {
        let default = EntityPath::new(format!("sdk-atom-default-{suffix}"))?;
        let default_view = machine
            .describe_queue_capacity(namespace, &default)?
            .expect("recreated finite SDK owner");
        assert_eq!(
            default_view.binding.generation(),
            2,
            "delete/recreate did not replace the owner's identity"
        );
        assert_eq!(
            default_view.capacity,
            QueueCapacityStatus::FiniteV1 {
                limit: mib(1024),
                reserved_bytes: 0,
                message_count: 0
            }
        );
        assert_eq!(
            machine.queue_config(namespace, &default)?,
            Some(default_config())
        );
        let deleted = EntityPath::new(format!("sdk-atom-definition-{suffix}"))?;
        assert!(machine.queue_config(namespace, &deleted)?.is_none());
        assert!(
            machine
                .queue_config(namespace, &deleted.dead_letter_queue()?)?
                .is_none()
        );
        for (kind, count, body_bytes, config, limit, expires_at) in [
            ("quota", 5, 256 * 1024, default_config(), 2, None),
            (
                "retention",
                1,
                16 * 1024,
                retention_config(true),
                3,
                Some(Timestamp::from_millis(601_000)),
            ),
        ] {
            let entity = EntityPath::new(format!("sdk-atom-{kind}-{suffix}"))?;
            assert_eq!(machine.queue_config(namespace, &entity)?, Some(config));
            let shadow = entity.dead_letter_queue()?;
            assert_eq!(
                machine.queue_config(namespace, &shadow)?,
                Some(config.dead_letter_shadow())
            );
            let capacity = machine
                .describe_queue_capacity(namespace, &entity)?
                .expect("finite SDK owner");
            let QueueCapacityStatus::FiniteV1 {
                limit: actual_limit,
                reserved_bytes,
                message_count,
            } = capacity.capacity
            else {
                panic!("SDK owner became NonFinite");
            };
            assert_eq!(actual_limit, mib(limit));
            assert_eq!(message_count, count);
            assert!(
                reserved_bytes > count * body_bytes as u64 && reserved_bytes <= mib(limit).bytes()
            );
            let expected_messages = (1..=count)
                .map(|sequence| keys::message(namespace, &entity, SequenceNumber::new(sequence)))
                .collect::<BTreeSet<_>>();
            let expected_ready = (1..=count)
                .map(|sequence| keys::ready(namespace, &entity, SequenceNumber::new(sequence)))
                .collect::<BTreeSet<_>>();
            assert_index(
                store,
                keys::message_prefix(namespace, &entity),
                &expected_messages,
                false,
            )?;
            assert_index(
                store,
                keys::ready_prefix(namespace, &entity),
                &expected_ready,
                true,
            )?;
            let expected_charges = (1..=count)
                .map(|sequence| {
                    keys::message_charge(namespace, &entity, SequenceNumber::new(sequence))
                })
                .collect::<BTreeSet<_>>();
            assert_index(
                store,
                keys::message_charge_prefix(namespace, &entity),
                &expected_charges,
                false,
            )?;
            assert!(
                store
                    .get(&keys::queue_capacity_usage(namespace, &entity))?
                    .is_some()
            );
            let mut expected_expiry = BTreeSet::new();
            for sequence in 1..=count {
                let sequence = SequenceNumber::new(sequence);
                let record = machine
                    .message(namespace, &entity, sequence)?
                    .expect("retained SDK seed");
                assert_eq!(record.sequence, sequence);
                assert_eq!(
                    record.body,
                    vec![
                        if kind == "quota" {
                            sequence.as_u64() as u8
                        } else {
                            0x72
                        };
                        body_bytes
                    ]
                );
                assert_eq!(
                    record.message_id,
                    if kind == "quota" {
                        format!("sdk-quota-{suffix}-{}", sequence.as_u64())
                    } else {
                        format!("sdk-retained-{suffix}")
                    }
                );
                assert_eq!(record.enqueued_at, Timestamp::from_millis(1_000));
                assert_eq!(record.expires_at, expires_at);
                assert_eq!(record.state, MessageState::Ready);
                assert_eq!(record.delivery_count, 0);
                assert_eq!(
                    record.session_id,
                    if kind == "retention" {
                        Some(SessionId::new(format!("metadata-{suffix}"))?)
                    } else {
                        None
                    }
                );
                assert!(
                    record.envelope.is_none()
                        && record.dead_letter.is_none()
                        && record.scheduled_enqueue_time.is_none()
                );
                if let Some(deadline) = expires_at {
                    expected_expiry.insert(keys::expiry(namespace, &entity, deadline, sequence));
                }
            }
            assert_index(
                store,
                keys::expiry_prefix(namespace, &entity),
                &expected_expiry,
                true,
            )?;
            for prefix in [
                keys::lock_prefix(namespace, &entity),
                keys::scheduled_prefix(namespace, &entity),
                keys::session_lock_prefix(namespace, &entity),
                keys::entity_session_prefix(namespace, &entity),
                keys::duplicate_history_prefix(namespace, &entity),
                keys::duplicate_history_expiry_prefix(namespace, &entity),
                keys::message_prefix(namespace, &shadow),
                keys::ready_prefix(namespace, &shadow),
                keys::expiry_prefix(namespace, &shadow),
                keys::message_charge_prefix(namespace, &shadow),
            ] {
                assert!(
                    store.scan_prefix(&prefix, 1)?.is_empty(),
                    "unexpected runtime retention in {kind}"
                );
            }
        }
    }
    Ok(())
}

fn assert_index<S: StateStore>(
    store: &S,
    prefix: Vec<u8>,
    expected: &BTreeSet<Vec<u8>>,
    marker: bool,
) -> TestResult {
    let rows = store.scan_prefix(&prefix, expected.len() + 1)?;
    assert_eq!(rows.len(), expected.len());
    assert_eq!(
        rows.iter()
            .map(|(key, _)| key.clone())
            .collect::<BTreeSet<_>>(),
        *expected
    );
    assert!(rows.iter().all(|(_, value)| if marker {
        value.is_empty()
    } else {
        !value.is_empty()
    }));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desired_omissions_reset_ttl_and_disabled_history() {
        assert_eq!(
            definition_config(0).default_time_to_live_millis,
            Some(45_000)
        );
        assert_eq!(
            definition_config(1).default_time_to_live_millis,
            Some(90_000)
        );
        assert_eq!(definition_config(2).default_time_to_live_millis, None);
        assert_eq!(
            definition_config(2).duplicate_detection_history_time_window_millis,
            60_000
        );
        assert_eq!(
            retention_config(false).default_time_to_live_millis,
            Some(600_000)
        );
        assert_eq!(retention_config(true).default_time_to_live_millis, None);
    }
}
