use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use storage::{Key, MemoryStore, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};

use crate::queue_capacity::observe_record;
use crate::{DeadLetterInfo, DeadLetterReason, MessageRecord, MessageState, Timestamp};

use super::*;

#[derive(Clone, Default)]
struct CountingStore {
    inner: MemoryStore,
    gets: Arc<AtomicUsize>,
    applies: Arc<AtomicUsize>,
    scans: Arc<AtomicUsize>,
    snapshots: Arc<AtomicUsize>,
}

impl StateStore for CountingStore {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.snapshots.fetch_add(1, Ordering::SeqCst);
        self.inner.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }
}

fn names() -> (NamespaceName, EntityPath) {
    (
        NamespaceName::new("tenant").unwrap(),
        EntityPath::new("orders").unwrap(),
    )
}

fn record(sequence: u64) -> MessageRecord {
    MessageRecord {
        sequence: SequenceNumber::new(sequence),
        message_id: "id".into(),
        body: vec![1, 2, 3],
        enqueued_at: Timestamp::from_millis(1),
        expires_at: None,
        delivery_count: 0,
        state: MessageState::Ready,
        session_id: None,
        dead_letter: None,
        scheduled_enqueue_time: None,
        envelope: None,
    }
}

fn fixture(limit: Option<u64>) -> (StateMachine<CountingStore>, NamespaceName, EntityPath) {
    let (namespace, owner) = names();
    let store = CountingStore::default();
    let config = QueueConfig::default();
    let mode = match limit {
        Some(bytes) => {
            QueueCapacityMode::finite_v1(1, std::num::NonZeroU64::new(bytes).unwrap()).unwrap()
        }
        None => QueueCapacityMode::non_finite(1).unwrap(),
    };
    let identity = EntityIncarnation::new(1, EntityIncarnationKind::Queue, false).unwrap();
    let mut batch = WriteBatch::default()
        .put(
            keys::queue_config(&namespace, &owner),
            codec::encode(&config).unwrap(),
        )
        .put(
            keys::queue_config(&namespace, &owner.dead_letter_queue().unwrap()),
            codec::encode(&config.dead_letter_shadow()).unwrap(),
        )
        .put(
            keys::entity_incarnation(&namespace, &owner),
            codec::encode(&identity).unwrap(),
        )
        .put(
            keys::queue_capacity_mode(&namespace, &owner),
            mode.encode().unwrap(),
        );
    if limit.is_some() {
        batch.push_put(
            keys::queue_capacity_usage(&namespace, &owner),
            QueueCapacityUsage::new(1, 0, 0).unwrap().encode().unwrap(),
        );
    }
    store.inner.apply(batch).unwrap();
    (StateMachine::new(store), namespace, owner)
}

fn persist_record(
    machine: &StateMachine<CountingStore>,
    namespace: &NamespaceName,
    owner: &EntityPath,
    value: &MessageRecord,
) {
    let charge = MessageCharge::for_new_record(1, value).unwrap();
    machine
        .store()
        .inner
        .apply(
            WriteBatch::default()
                .put(
                    keys::message(namespace, owner, value.sequence),
                    b"opaque original payload".to_vec(),
                )
                .put(
                    keys::message_charge(namespace, owner, value.sequence),
                    charge.encode().unwrap(),
                )
                .put(
                    keys::queue_capacity_usage(namespace, owner),
                    QueueCapacityUsage::new(1, charge.charged_bytes(), 1)
                        .unwrap()
                        .encode()
                        .unwrap(),
                ),
        )
        .unwrap();
}

fn usage(
    machine: &StateMachine<CountingStore>,
    namespace: &NamespaceName,
    owner: &EntityPath,
) -> QueueCapacityUsage {
    QueueCapacityUsage::decode(
        &machine
            .store()
            .inner
            .get(&keys::queue_capacity_usage(namespace, owner))
            .unwrap()
            .unwrap(),
        1,
    )
    .unwrap()
}

#[test]
fn primary_and_shadow_profiles_share_nine_clock_free_point_reads() {
    for shadow in [false, true] {
        let (machine, namespace, owner) = fixture(Some(10_000));
        let target = if shadow {
            owner.dead_letter_queue().unwrap()
        } else {
            owner.clone()
        };
        let profile = validate_owner_profile(&machine, &namespace, &target).unwrap();
        assert_eq!(profile.owner(), &owner);
        assert_eq!(profile.target(), &target);
        assert_eq!(profile.generation(), 1);
        assert_eq!(profile.kind(), EntityIncarnationKind::Queue);
        assert_eq!(profile.config(), QueueConfig::default());
        assert_eq!(profile.usage().unwrap().reserved_bytes(), 0);
        assert_eq!(machine.store().gets.load(Ordering::SeqCst), 9);
        assert_eq!(machine.store().scans.load(Ordering::SeqCst), 0);
        assert_eq!(machine.store().snapshots.load(Ordering::SeqCst), 0);
        assert_eq!(machine.store().applies.load(Ordering::SeqCst), 0);
        assert!(machine.store().inner.get(&keys::clock()).unwrap().is_none());
    }
}

#[test]
fn zero_event_finish_requires_mode_and_emits_no_usage_or_clock_write() {
    let (machine, namespace, owner) = fixture(Some(10_000));
    let mut batch = WriteBatch::default();
    CapacityPlan::existing(&namespace, &owner)
        .finish(&machine, &mut batch)
        .unwrap();
    assert!(batch.is_empty());
    machine
        .store()
        .inner
        .apply(WriteBatch::default().delete(keys::queue_capacity_mode(&namespace, &owner)))
        .unwrap();
    assert_eq!(
        CapacityPlan::existing(&namespace, &owner).finish(&machine, &mut batch),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    assert!(batch.is_empty());
}

#[test]
fn multiple_retains_use_one_aggregate_and_do_not_apply_or_scan() {
    let (machine, namespace, owner) = fixture(Some(10_000));
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    for sequence in 1..=2 {
        let value = record(sequence);
        plan.record_new(&owner, value.sequence, observe_record(&value))
            .unwrap();
    }
    let mut batch = WriteBatch::default();
    plan.finish(&machine, &mut batch).unwrap();
    assert_eq!(machine.store().gets.load(Ordering::SeqCst), 13);
    assert_eq!(machine.store().applies.load(Ordering::SeqCst), 0);
    assert_eq!(machine.store().scans.load(Ordering::SeqCst), 0);
    machine.store().inner.apply(batch).unwrap();
    let one = MessageCharge::for_new_record(1, &record(1))
        .unwrap()
        .charged_bytes();
    assert_eq!(
        usage(&machine, &namespace, &owner).reserved_bytes(),
        2 * one
    );
    assert_eq!(usage(&machine, &namespace, &owner).message_count(), 2);
}

#[test]
fn later_quota_refusal_appends_nothing_to_the_existing_private_batch() {
    let bytes = MessageCharge::for_new_record(1, &record(1))
        .unwrap()
        .charged_bytes();
    let (machine, namespace, owner) = fixture(Some(bytes));
    let before = machine.store().inner.snapshot().unwrap();
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    for sequence in 1..=2 {
        plan.record_new(
            &owner,
            SequenceNumber::new(sequence),
            observe_record(&record(sequence)),
        )
        .unwrap();
    }
    let mut batch =
        WriteBatch::default().put(b"old staged sentinel".to_vec(), b"unchanged".to_vec());
    assert_eq!(
        plan.finish(&machine, &mut batch),
        Err(BrokerError::QueueCapacityFull)
    );
    assert_eq!(batch.mutations().len(), 1);
    assert_eq!(machine.store().inner.snapshot().unwrap(), before);
}

#[test]
fn proposed_usage_byte_overflow_is_full_and_preserves_batch_and_backing_state() {
    for replace in [false, true] {
        let (machine, namespace, owner) = fixture(Some(u64::MAX));
        let old = record(1);
        if replace {
            persist_record(&machine, &namespace, &owner, &old);
        }
        let initial_count = if replace { 2 } else { 1 };
        machine
            .store()
            .inner
            .apply(
                WriteBatch::default().put(
                    keys::queue_capacity_usage(&namespace, &owner),
                    QueueCapacityUsage::new(1, u64::MAX, initial_count)
                        .unwrap()
                        .encode()
                        .unwrap(),
                ),
            )
            .unwrap();
        let before = machine.store().inner.snapshot().unwrap();
        let mut plan = CapacityPlan::existing(&namespace, &owner);
        if replace {
            let mut bigger = old.clone();
            bigger.body.push(4);
            plan.record_replace(
                &owner,
                old.sequence,
                observe_record(&old),
                observe_record(&bigger),
            )
            .unwrap();
        } else {
            let new = record(2);
            plan.record_new(&owner, new.sequence, observe_record(&new))
                .unwrap();
        }
        let mut batch = WriteBatch::default().put(b"old sentinel".to_vec(), b"unchanged".to_vec());
        assert_eq!(
            plan.finish(&machine, &mut batch),
            Err(BrokerError::QueueCapacityFull)
        );
        assert_eq!(batch.mutations().len(), 1);
        assert!(matches!(&batch.mutations()[0], Mutation::Put { key, value }
            if key.as_slice() == b"old sentinel" && value.as_slice() == b"unchanged"));
        assert_eq!(machine.store().inner.snapshot().unwrap(), before);
        assert_eq!(machine.store().applies.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn numeric_observation_errors_are_deferred_and_non_finite_records_are_unchanged() {
    for finite in [false, true] {
        let (machine, namespace, owner) = fixture(finite.then_some(10_000));
        let mut plan = CapacityPlan::existing(&namespace, &owner);
        plan.record_new(
            &owner,
            SequenceNumber::new(1),
            Err(QueueCapacityError::SaturatedContentTally),
        )
        .unwrap();
        assert_eq!(machine.store().gets.load(Ordering::SeqCst), 0);
        let mut batch = WriteBatch::default();
        let result = plan.finish(&machine, &mut batch);
        if finite {
            assert_eq!(result, Err(BrokerError::QueueCapacityCorrupt));
        } else {
            result.unwrap();
        }
        assert!(batch.is_empty());
    }
}

#[test]
fn unchanged_record_check_requires_ledger_and_does_not_rewrite_usage() {
    let (machine, namespace, owner) = fixture(Some(10_000));
    let value = record(1);
    persist_record(&machine, &namespace, &owner, &value);
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    plan.record_check(&owner, value.sequence, observe_record(&value))
        .unwrap();
    let mut batch = WriteBatch::default();
    plan.finish(&machine, &mut batch).unwrap();
    assert!(batch.is_empty());
    machine
        .store()
        .inner
        .apply(WriteBatch::default().delete(keys::message_charge(
            &namespace,
            &owner,
            value.sequence,
        )))
        .unwrap();
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    plan.record_check(&owner, value.sequence, observe_record(&value))
        .unwrap();
    assert_eq!(
        plan.finish(&machine, &mut batch),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    assert!(batch.is_empty());
}

#[test]
fn valid_base_check_cannot_hide_behind_zero_aggregate_usage() {
    let (machine, namespace, owner) = fixture(Some(10_000));
    let old = record(1);
    persist_record(&machine, &namespace, &owner, &old);
    machine
        .store()
        .inner
        .apply(WriteBatch::default().put(
            keys::queue_capacity_usage(&namespace, &owner),
            QueueCapacityUsage::new(1, 0, 0).unwrap().encode().unwrap(),
        ))
        .unwrap();
    let before = machine.store().inner.snapshot().unwrap();
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    plan.record_check(&owner, old.sequence, observe_record(&old))
        .unwrap();
    let mut batch = WriteBatch::default().put(b"old sentinel".to_vec(), b"unchanged".to_vec());
    assert_eq!(
        plan.finish(&machine, &mut batch),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    assert_eq!(batch.mutations().len(), 1);
    assert_eq!(machine.store().inner.snapshot().unwrap(), before);
    assert_eq!(machine.store().gets.load(Ordering::SeqCst), 10);
    assert_eq!(machine.store().scans.load(Ordering::SeqCst), 0);
}

#[test]
fn distinct_known_base_charges_must_fit_both_initial_count_and_bytes() {
    let charge = MessageCharge::for_new_record(1, &record(1)).unwrap();
    for count_understated in [true, false] {
        let (machine, namespace, owner) = fixture(Some(10_000));
        let mut setup = WriteBatch::default();
        let mut plan = CapacityPlan::existing(&namespace, &owner);
        for sequence in 1..=2 {
            let old = record(sequence);
            setup.push_put(
                keys::message(&namespace, &owner, old.sequence),
                b"opaque source".to_vec(),
            );
            setup.push_put(
                keys::message_charge(&namespace, &owner, old.sequence),
                charge.encode().unwrap(),
            );
            plan.record_check(&owner, old.sequence, observe_record(&old))
                .unwrap();
        }
        let bytes = 2 * charge.charged_bytes() - u64::from(!count_understated);
        let count = if count_understated { 1 } else { 2 };
        setup.push_put(
            keys::queue_capacity_usage(&namespace, &owner),
            QueueCapacityUsage::new(1, bytes, count)
                .unwrap()
                .encode()
                .unwrap(),
        );
        machine.store().inner.apply(setup).unwrap();
        let before = machine.store().inner.snapshot().unwrap();
        let mut batch = WriteBatch::default().put(b"old sentinel".to_vec(), b"unchanged".to_vec());
        assert_eq!(
            plan.finish(&machine, &mut batch),
            Err(BrokerError::QueueCapacityCorrupt)
        );
        assert_eq!(batch.mutations().len(), 1);
        assert_eq!(machine.store().inner.snapshot().unwrap(), before);
        assert_eq!(
            machine.store().gets.load(Ordering::SeqCst),
            if count_understated { 10 } else { 11 }
        );
    }
}

#[test]
fn known_base_source_must_leave_a_structurally_plausible_unknown_residual() {
    for insufficient_remaining_bytes in [true, false] {
        let (machine, namespace, owner) = fixture(Some(10_000));
        let mut old = record(1);
        if insufficient_remaining_bytes {
            old.body.extend_from_slice(&[0; 522]);
        }
        persist_record(&machine, &namespace, &owner, &old);
        let bytes = MessageCharge::for_new_record(1, &old)
            .unwrap()
            .charged_bytes();
        let aggregate = if insufficient_remaining_bytes {
            QueueCapacityUsage::new(1, bytes, 2).unwrap()
        } else {
            QueueCapacityUsage::new(1, bytes + 1, 1).unwrap()
        };
        machine
            .store()
            .inner
            .apply(WriteBatch::default().put(
                keys::queue_capacity_usage(&namespace, &owner),
                aggregate.encode().unwrap(),
            ))
            .unwrap();
        let before = machine.store().inner.snapshot().unwrap();
        let mut plan = CapacityPlan::existing(&namespace, &owner);
        plan.record_check(&owner, old.sequence, observe_record(&old))
            .unwrap();
        let mut batch = WriteBatch::default().put(b"old sentinel".to_vec(), b"unchanged".to_vec());
        assert_eq!(
            plan.finish(&machine, &mut batch),
            Err(BrokerError::QueueCapacityCorrupt)
        );
        assert_eq!(batch.mutations().len(), 1);
        assert!(matches!(&batch.mutations()[0], Mutation::Put { key, value }
            if key.as_slice() == b"old sentinel" && value.as_slice() == b"unchanged"));
        assert_eq!(machine.store().inner.snapshot().unwrap(), before);
        assert_eq!(machine.store().gets.load(Ordering::SeqCst), 10);
    }
}

#[test]
fn repeated_cached_base_check_counts_coverage_once() {
    let (machine, namespace, owner) = fixture(Some(10_000));
    let old = record(1);
    persist_record(&machine, &namespace, &owner, &old);
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    for _ in 0..2 {
        plan.record_check(&owner, old.sequence, observe_record(&old))
            .unwrap();
    }
    let mut batch = WriteBatch::default();
    plan.finish(&machine, &mut batch).unwrap();
    assert!(batch.is_empty());
    assert_eq!(machine.store().gets.load(Ordering::SeqCst), 10);
}

#[test]
fn staged_new_and_transferred_checks_do_not_claim_base_source_coverage() {
    for initially_retained in [true, false] {
        let (machine, namespace, owner) = fixture(Some(10_000));
        let old = record(1);
        if initially_retained {
            persist_record(&machine, &namespace, &owner, &old);
        }
        let shadow = owner.dead_letter_queue().unwrap();
        let mut moved = old.clone();
        moved.dead_letter = Some(DeadLetterInfo {
            reason: DeadLetterReason::TimeToLiveExpired,
            description: "the message exceeded its time to live".into(),
            dead_lettered_at: Timestamp::from_millis(2),
        });
        let mut plan = CapacityPlan::existing(&namespace, &owner);
        if !initially_retained {
            plan.record_new(&owner, old.sequence, observe_record(&old))
                .unwrap();
        }
        plan.record_check(&owner, old.sequence, observe_record(&old))
            .unwrap();
        plan.record_transfer(
            &owner,
            old.sequence,
            &shadow,
            old.sequence,
            observe_record(&old),
            observe_record(&moved),
        )
        .unwrap();
        plan.record_check(&shadow, moved.sequence, observe_record(&moved))
            .unwrap();
        let mut batch = WriteBatch::default();
        plan.finish(&machine, &mut batch).unwrap();
        assert_eq!(
            machine.store().gets.load(Ordering::SeqCst),
            if initially_retained { 12 } else { 13 }
        );
        machine.store().inner.apply(batch).unwrap();
        assert_eq!(usage(&machine, &namespace, &owner).message_count(), 1);
        assert_eq!(
            usage(&machine, &namespace, &owner).reserved_bytes(),
            MessageCharge::for_new_record(1, &old)
                .unwrap()
                .charged_bytes()
        );
        assert_eq!(machine.store().scans.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn remove_before_new_reuses_credit_but_reverse_order_refuses() {
    let old = record(1);
    let bytes = MessageCharge::for_new_record(1, &old)
        .unwrap()
        .charged_bytes();
    for remove_first in [true, false] {
        let (machine, namespace, owner) = fixture(Some(bytes));
        persist_record(&machine, &namespace, &owner, &old);
        let mut plan = CapacityPlan::existing(&namespace, &owner);
        if remove_first {
            plan.record_remove(&owner, old.sequence, observe_record(&old))
                .unwrap();
        }
        plan.record_new(&owner, SequenceNumber::new(2), observe_record(&record(2)))
            .unwrap();
        if !remove_first {
            plan.record_remove(&owner, old.sequence, observe_record(&old))
                .unwrap();
        }
        let mut batch = WriteBatch::default();
        if remove_first {
            plan.finish(&machine, &mut batch).unwrap();
            machine.store().inner.apply(batch).unwrap();
            assert_eq!(usage(&machine, &namespace, &owner).reserved_bytes(), bytes);
            assert_eq!(usage(&machine, &namespace, &owner).message_count(), 1);
        } else {
            assert_eq!(
                plan.finish(&machine, &mut batch),
                Err(BrokerError::QueueCapacityFull)
            );
            assert!(batch.is_empty());
        }
    }
}

#[test]
fn automatic_transfer_at_full_capacity_preserves_saved_session_credit() {
    let mut old = record(1);
    old.session_id = Some(crate::SessionId::new("session").unwrap());
    let bytes = MessageCharge::for_new_record(1, &old)
        .unwrap()
        .charged_bytes();
    let (machine, namespace, owner) = fixture(Some(bytes));
    persist_record(&machine, &namespace, &owner, &old);
    let shadow = owner.dead_letter_queue().unwrap();
    let mut moved = old.clone();
    moved.session_id = None;
    moved.dead_letter = Some(DeadLetterInfo {
        reason: DeadLetterReason::TimeToLiveExpired,
        description: "the message exceeded its time to live".into(),
        dead_lettered_at: Timestamp::from_millis(2),
    });
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    plan.record_transfer(
        &owner,
        old.sequence,
        &shadow,
        old.sequence,
        observe_record(&old),
        observe_record(&moved),
    )
    .unwrap();
    let mut batch = WriteBatch::default();
    plan.finish(&machine, &mut batch).unwrap();
    machine.store().inner.apply(batch).unwrap();
    assert!(
        machine
            .store()
            .inner
            .get(&keys::message_charge(&namespace, &owner, old.sequence))
            .unwrap()
            .is_none()
    );
    let charge = MessageCharge::decode(
        &machine
            .store()
            .inner
            .get(&keys::message_charge(&namespace, &shadow, old.sequence))
            .unwrap()
            .unwrap(),
        1,
    )
    .unwrap();
    assert_eq!(charge.original_session_bytes(), 12);
    assert_eq!(charge.dead_letter_projection_bytes(), 137);
    assert_eq!(charge.charged_bytes(), bytes);
    assert_eq!(usage(&machine, &namespace, &owner).message_count(), 1);
    assert_eq!(usage(&machine, &namespace, &owner).reserved_bytes(), bytes);
}

#[test]
fn transfer_vector_counts_original_messages_and_bounds_both_ledger_sides() {
    let one = MessageCharge::for_new_record(1, &record(1)).unwrap();
    let total = one.charged_bytes() * MAX_CAPACITY_MESSAGES as u64;
    let (machine, namespace, owner) = fixture(Some(total));
    let shadow = owner.dead_letter_queue().unwrap();
    let mut setup = WriteBatch::default();
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    for sequence in 1..=MAX_CAPACITY_MESSAGES {
        let old = record(sequence as u64);
        setup.push_put(
            keys::message(&namespace, &owner, old.sequence),
            b"opaque source".to_vec(),
        );
        setup.push_put(
            keys::message_charge(&namespace, &owner, old.sequence),
            one.encode().unwrap(),
        );
        let mut moved = old.clone();
        moved.dead_letter = Some(DeadLetterInfo {
            reason: DeadLetterReason::TimeToLiveExpired,
            description: "the message exceeded its time to live".into(),
            dead_lettered_at: Timestamp::from_millis(2),
        });
        plan.record_transfer(
            &owner,
            old.sequence,
            &shadow,
            old.sequence,
            observe_record(&old),
            observe_record(&moved),
        )
        .unwrap();
    }
    setup.push_put(
        keys::queue_capacity_usage(&namespace, &owner),
        QueueCapacityUsage::new(1, total, MAX_CAPACITY_MESSAGES as u64)
            .unwrap()
            .encode()
            .unwrap(),
    );
    machine.store().inner.apply(setup).unwrap();
    assert_eq!(plan.touched.len(), MAX_CAPACITY_MESSAGES);
    assert_eq!(plan.events.len(), MAX_CAPACITY_MESSAGES);
    let mut batch = WriteBatch::default();
    plan.finish(&machine, &mut batch).unwrap();
    assert_eq!(batch.mutations().len(), MAX_CAPACITY_LEDGER_KEYS + 1);
    assert_eq!(
        machine.store().gets.load(Ordering::SeqCst),
        9 + 3 * MAX_CAPACITY_MESSAGES
    );
    machine.store().inner.apply(batch).unwrap();
    assert_eq!(usage(&machine, &namespace, &owner).reserved_bytes(), total);
    assert_eq!(
        usage(&machine, &namespace, &owner).message_count(),
        MAX_CAPACITY_MESSAGES as u64
    );
    assert_eq!(machine.store().scans.load(Ordering::SeqCst), 0);
    assert_eq!(machine.store().applies.load(Ordering::SeqCst), 0);

    let mut plan = CapacityPlan::existing(&namespace, &owner);
    for sequence in 1..=MAX_CAPACITY_MESSAGES + 1 {
        let old = record(sequence as u64);
        let mut moved = old.clone();
        moved.dead_letter = Some(DeadLetterInfo {
            reason: DeadLetterReason::TimeToLiveExpired,
            description: "the message exceeded its time to live".into(),
            dead_lettered_at: Timestamp::from_millis(2),
        });
        let result = plan.record_transfer(
            &owner,
            old.sequence,
            &shadow,
            old.sequence,
            observe_record(&old),
            observe_record(&moved),
        );
        if sequence <= MAX_CAPACITY_MESSAGES {
            result.unwrap();
        } else {
            assert_eq!(result, Err(BrokerError::QueueCapacityWorkLimitExceeded));
        }
    }
    assert_eq!(plan.touched.len(), MAX_CAPACITY_MESSAGES);
    assert_eq!(plan.events.len(), MAX_CAPACITY_MESSAGES);
}

#[test]
fn transfer_to_colliding_opaque_message_refuses_without_decoding_or_mutations() {
    let (machine, namespace, owner) = fixture(Some(10_000));
    let old = record(1);
    persist_record(&machine, &namespace, &owner, &old);
    machine
        .store()
        .inner
        .apply(WriteBatch::default().put(
            keys::message(&namespace, &owner, SequenceNumber::new(2)),
            vec![255],
        ))
        .unwrap();
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    plan.record_transfer(
        &owner,
        old.sequence,
        &owner,
        SequenceNumber::new(2),
        observe_record(&old),
        observe_record(&record(2)),
    )
    .unwrap();
    let mut batch = WriteBatch::default();
    assert_eq!(
        plan.finish(&machine, &mut batch),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    assert!(batch.is_empty());
}

#[test]
fn original_numeric_observation_controls_replace_and_terminal_refund() {
    let (machine, namespace, owner) = fixture(Some(10_000));
    let old = record(1);
    persist_record(&machine, &namespace, &owner, &old);
    let original = observe_record(&old);
    let mut bigger = old.clone();
    bigger.body.extend_from_slice(&[0; 100]);
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    plan.record_replace(&owner, old.sequence, original, observe_record(&bigger))
        .unwrap();
    let mut batch = WriteBatch::default();
    plan.finish(&machine, &mut batch).unwrap();
    machine.store().inner.apply(batch).unwrap();
    assert_eq!(
        usage(&machine, &namespace, &owner).reserved_bytes(),
        MessageCharge::for_new_record(1, &bigger)
            .unwrap()
            .charged_bytes()
    );
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    plan.record_remove(&owner, bigger.sequence, observe_record(&bigger))
        .unwrap();
    let mut batch = WriteBatch::default();
    plan.finish(&machine, &mut batch).unwrap();
    machine.store().inner.apply(batch).unwrap();
    assert_eq!(usage(&machine, &namespace, &owner).reserved_bytes(), 0);
    assert_eq!(usage(&machine, &namespace, &owner).message_count(), 0);
}

#[test]
fn prepared_owner_uses_staged_generation_and_retired_owner_never_decodes_usage() {
    let (machine, namespace, owner) = fixture(None);
    let mode = QueueCapacityMode::finite_v1(2, std::num::NonZeroU64::new(10_000).unwrap()).unwrap();
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    plan.prepare_owner(
        QueueConfig::default(),
        EntityIncarnation::new(2, EntityIncarnationKind::Queue, false).unwrap(),
        mode,
    )
    .unwrap();
    let mut batch = WriteBatch::default();
    plan.finish(&machine, &mut batch).unwrap();
    assert_eq!(machine.store().gets.load(Ordering::SeqCst), 0);
    assert_eq!(batch.mutations().len(), 1);
    let (machine, namespace, owner) = fixture(Some(10_000));
    machine
        .store()
        .inner
        .apply(WriteBatch::default().put(keys::queue_capacity_usage(&namespace, &owner), vec![255]))
        .unwrap();
    assert!(validate_owner_profile(&machine, &namespace, &owner).is_err());
    let checked = validate_owner_mode(&machine, &namespace, &owner).unwrap();
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    plan.mark_retired(checked).unwrap();
    let mut batch = WriteBatch::default();
    plan.finish(&machine, &mut batch).unwrap();
    assert!(batch.is_empty());
}

#[test]
fn mode_schema_generation_shadow_ownership_and_overlimit_usage_fail_closed() {
    for corruption in 0..4 {
        let (machine, namespace, owner) = fixture(Some(517));
        let mutation = match corruption {
            0 => WriteBatch::default().put(
                keys::queue_capacity_mode(&namespace, &owner),
                QueueCapacityMode::non_finite(2).unwrap().encode().unwrap(),
            ),
            1 => WriteBatch::default().put(
                keys::queue_capacity_mode(&namespace, &owner),
                vec![11, 2, 1, 0],
            ),
            2 => WriteBatch::default().put(
                keys::queue_capacity_mode(&namespace, &owner.dead_letter_queue().unwrap()),
                QueueCapacityMode::non_finite(1).unwrap().encode().unwrap(),
            ),
            _ => WriteBatch::default().put(
                keys::queue_capacity_usage(&namespace, &owner),
                QueueCapacityUsage::new(1, 518, 1)
                    .unwrap()
                    .encode()
                    .unwrap(),
            ),
        };
        machine.store().inner.apply(mutation).unwrap();
        assert!(matches!(
            validate_owner_profile(&machine, &namespace, &owner),
            Err(BrokerError::QueueCapacityCorrupt)
        ));
    }
}

#[test]
fn finite_vector_preflight_caps_only_finite_and_skips_non_finite_event_work() {
    for finite in [true, false] {
        let (machine, namespace, owner) = fixture(finite.then_some(10_000));
        let mut plan = CapacityPlan::existing(&namespace, &owner);
        let result = plan.check_input_count(&machine, MAX_CAPACITY_MESSAGES + 1);
        if finite {
            assert_eq!(result, Err(BrokerError::QueueCapacityWorkLimitExceeded));
        } else {
            result.unwrap();
            for _ in 0..MAX_CAPACITY_EVENTS + 1 {
                plan.record_check(
                    &owner,
                    SequenceNumber::new(1),
                    Err(QueueCapacityError::InvalidCharge),
                )
                .unwrap();
            }
            assert!(plan.events.is_empty());
        }
    }
}

#[test]
fn within_cap_preflight_defers_profile_corruption_and_performs_no_reads() {
    let (machine, namespace, owner) = fixture(Some(10_000));
    machine
        .store()
        .inner
        .apply(WriteBatch::default().delete(keys::queue_capacity_mode(&namespace, &owner)))
        .unwrap();
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    for count in [0, 1, MAX_CAPACITY_MESSAGES] {
        plan.check_input_count(&machine, count).unwrap();
    }
    assert!(plan.resolved.is_none());
    assert_eq!(machine.store().gets.load(Ordering::SeqCst), 0);
    plan.record_new(
        &owner,
        SequenceNumber::new(1),
        Err(QueueCapacityError::InvalidCharge),
    )
    .unwrap();
    assert_eq!(machine.store().gets.load(Ordering::SeqCst), 0);
    let mut batch = WriteBatch::default();
    assert_eq!(
        plan.finish(&machine, &mut batch),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    assert!(batch.is_empty());
}

#[test]
fn retired_override_accepts_only_existing_primary_target_and_primary_proof() {
    let (machine, namespace, owner) = fixture(None);
    let shadow = owner.dead_letter_queue().unwrap();
    let checked = validate_owner_mode(&machine, &namespace, &owner).unwrap();
    let shadow_checked = validate_owner_mode(&machine, &namespace, &shadow).unwrap();
    let mut shadow_plan = CapacityPlan::existing(&namespace, &shadow);
    assert_eq!(
        shadow_plan.mark_retired(checked.clone()),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    assert_eq!(
        plan.mark_retired(shadow_checked),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    plan.prepare_owner(
        QueueConfig::default(),
        EntityIncarnation::new(2, EntityIncarnationKind::Queue, false).unwrap(),
        QueueCapacityMode::non_finite(2).unwrap(),
    )
    .unwrap();
    assert_eq!(
        plan.mark_retired(checked),
        Err(BrokerError::QueueCapacityCorrupt)
    );
}

#[test]
fn validated_topic_and_subscription_profiles_refuse_accounting_markers() {
    for kind in [
        EntityIncarnationKind::Topic,
        EntityIncarnationKind::Subscription,
    ] {
        let (machine, namespace, primary) = fixture(None);
        let owner = if kind == EntityIncarnationKind::Subscription {
            primary
                .subscription(&crate::SubscriptionName::new("active").unwrap())
                .unwrap()
        } else {
            primary
        };
        let mut setup = WriteBatch::default()
            .put(
                keys::entity_incarnation(&namespace, &owner),
                codec::encode(&EntityIncarnation::new(1, kind, false).unwrap()).unwrap(),
            )
            .delete(keys::queue_capacity_mode(&namespace, &owner));
        if kind == EntityIncarnationKind::Topic {
            setup.push_delete(keys::queue_config(&namespace, &owner));
            setup.push_delete(keys::queue_config(
                &namespace,
                &owner.dead_letter_queue().unwrap(),
            ));
            setup.push_put(
                keys::topic_config(&namespace, &owner),
                codec::encode(&TopicConfig::default()).unwrap(),
            );
        } else {
            setup.push_put(
                keys::queue_config(&namespace, &owner),
                codec::encode(&QueueConfig::default()).unwrap(),
            );
            setup.push_put(
                keys::queue_config(&namespace, &owner.dead_letter_queue().unwrap()),
                codec::encode(&QueueConfig::default().dead_letter_shadow()).unwrap(),
            );
        }
        machine.store().inner.apply(setup).unwrap();
        let mut plan = CapacityPlan::existing(&namespace, &owner);
        plan.check_input_count(&machine, usize::MAX).unwrap();
        plan.record_new(
            &owner,
            SequenceNumber::new(1),
            Err(QueueCapacityError::InvalidCharge),
        )
        .unwrap();
        let mut batch = WriteBatch::default();
        plan.finish(&machine, &mut batch).unwrap();
        assert!(batch.is_empty());
        machine
            .store()
            .inner
            .apply(WriteBatch::default().put(
                keys::queue_capacity_mode(&namespace, &owner),
                QueueCapacityMode::non_finite(1).unwrap().encode().unwrap(),
            ))
            .unwrap();
        assert_eq!(
            CapacityPlan::existing(&namespace, &owner).finish(&machine, &mut batch),
            Err(BrokerError::QueueCapacityCorrupt)
        );
    }
}

#[test]
fn contradictory_nonqueue_incarnation_cannot_bypass_ordinary_queue_mode() {
    let (machine, namespace, owner) = fixture(None);
    machine
        .store()
        .inner
        .apply(
            WriteBatch::default()
                .put(
                    keys::entity_incarnation(&namespace, &owner),
                    codec::encode(
                        &EntityIncarnation::new(1, EntityIncarnationKind::Topic, false).unwrap(),
                    )
                    .unwrap(),
                )
                .delete(keys::queue_capacity_mode(&namespace, &owner)),
        )
        .unwrap();
    let mut batch = WriteBatch::default();
    assert_eq!(
        CapacityPlan::existing(&namespace, &owner).finish(&machine, &mut batch),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    assert!(batch.is_empty());
}

#[test]
fn fresh_topic_override_checks_markers_without_rereading_unstaged_base_topology() {
    let (namespace, owner) = names();
    let machine = StateMachine::new(CountingStore::default());
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    plan.prepare_excluded_owner(
        EntityIncarnation::new(1, EntityIncarnationKind::Topic, false).unwrap(),
    )
    .unwrap();
    let mut batch = WriteBatch::default();
    plan.finish(&machine, &mut batch).unwrap();
    assert!(batch.is_empty());
    assert_eq!(machine.store().gets.load(Ordering::SeqCst), 4);
    machine
        .store()
        .inner
        .apply(WriteBatch::default().put(keys::queue_capacity_mode(&namespace, &owner), vec![255]))
        .unwrap();
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    plan.prepare_excluded_owner(
        EntityIncarnation::new(1, EntityIncarnationKind::Topic, false).unwrap(),
    )
    .unwrap();
    assert_eq!(
        plan.finish(&machine, &mut batch),
        Err(BrokerError::QueueCapacityCorrupt)
    );
    assert!(batch.is_empty());
}

#[test]
fn observation_sequence_and_physical_shadow_cannot_refund_a_different_ledger() {
    let (machine, namespace, owner) = fixture(Some(10_000));
    let old = record(1);
    persist_record(&machine, &namespace, &owner, &old);
    for shadow in [false, true] {
        let mut plan = CapacityPlan::existing(&namespace, &owner);
        let target = if shadow {
            owner.dead_letter_queue().unwrap()
        } else {
            owner.clone()
        };
        let sequence = if shadow {
            old.sequence
        } else {
            SequenceNumber::new(2)
        };
        plan.record_remove(&target, sequence, observe_record(&old))
            .unwrap();
        let mut batch = WriteBatch::default();
        assert_eq!(
            plan.finish(&machine, &mut batch),
            Err(BrokerError::QueueCapacityCorrupt)
        );
        assert!(batch.is_empty());
    }
}

#[test]
fn stored_unsupported_finite_configuration_is_corruption() {
    let (machine, namespace, owner) = fixture(Some(10_000));
    let config = QueueConfig {
        requires_session: true,
        ..QueueConfig::default()
    };
    machine
        .store()
        .inner
        .apply(
            WriteBatch::default()
                .put(
                    keys::queue_config(&namespace, &owner),
                    codec::encode(&config).unwrap(),
                )
                .put(
                    keys::queue_config(&namespace, &owner.dead_letter_queue().unwrap()),
                    codec::encode(&config.dead_letter_shadow()).unwrap(),
                ),
        )
        .unwrap();
    assert!(matches!(
        validate_owner_profile(&machine, &namespace, &owner),
        Err(BrokerError::QueueCapacityCorrupt)
    ));
}

#[test]
fn materialized_value_limit_is_logical_and_never_decodes_the_offered_value() {
    let store = CountingStore::default();
    store
        .inner
        .apply(WriteBatch::default().put(
            b"oversized".to_vec(),
            vec![255; MAX_CAPACITY_READ_VALUE_BYTES + 1],
        ))
        .unwrap();
    let mut budget = ReadBudget::default();
    assert_eq!(
        budget.get(&store, b"oversized"),
        Err(BrokerError::QueueCapacityWorkLimitExceeded)
    );
    assert_eq!(store.gets.load(Ordering::SeqCst), 1);
    assert_eq!(store.applies.load(Ordering::SeqCst), 0);
}

#[test]
fn read_operation_and_key_byte_limits_refuse_before_an_extra_backend_read() {
    let store = CountingStore::default();
    let mut budget = ReadBudget::default();
    for _ in 0..MAX_CAPACITY_READS {
        budget.get(&store, b"").unwrap();
    }
    assert_eq!(
        budget.get(&store, b""),
        Err(BrokerError::QueueCapacityWorkLimitExceeded)
    );
    assert_eq!(store.gets.load(Ordering::SeqCst), MAX_CAPACITY_READS);
    let mut budget = ReadBudget::default();
    assert_eq!(
        budget.get(&store, &vec![0; MAX_CAPACITY_READ_KEY_BYTES + 1]),
        Err(BrokerError::QueueCapacityWorkLimitExceeded)
    );
    assert_eq!(store.gets.load(Ordering::SeqCst), MAX_CAPACITY_READS);
}

#[test]
fn touched_and_event_limits_are_checked_before_unbounded_plan_growth() {
    let (machine, namespace, owner) = fixture(Some(10_000));
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    for sequence in 1..=MAX_CAPACITY_MESSAGES {
        plan.record_new(
            &owner,
            SequenceNumber::new(sequence as u64),
            observe_record(&record(sequence as u64)),
        )
        .unwrap();
    }
    assert_eq!(
        plan.record_new(
            &owner,
            SequenceNumber::new(2_000),
            observe_record(&record(2_000))
        ),
        Err(BrokerError::QueueCapacityWorkLimitExceeded)
    );
    assert_eq!(plan.events.len(), MAX_CAPACITY_MESSAGES);
    let mut plan = CapacityPlan::existing(&namespace, &owner);
    for _ in 0..MAX_CAPACITY_EVENTS {
        plan.record_check(&owner, SequenceNumber::new(1), observe_record(&record(1)))
            .unwrap();
    }
    assert_eq!(
        plan.record_check(&owner, SequenceNumber::new(1), observe_record(&record(1))),
        Err(BrokerError::QueueCapacityWorkLimitExceeded)
    );
    assert_eq!(plan.events.len(), MAX_CAPACITY_EVENTS);
    assert_eq!(machine.store().applies.load(Ordering::SeqCst), 0);
}
