//! Paired complete-image message graphs. Supported commands and logical reopen
//! are distinct from explicitly injected compatibility/corruption shapes.
//! Partial success is not session/duplicate/allocation, backend or recovery health.

#[path = "snapshot_state/fixtures.rs"]
mod fixtures;

use domain::{
    CommandKind, CommandOutcome, DeliveryOrigin, EntityPath, MessageEnvelope, MessageState,
    QueueConfig, QueueConfigUpdate, ReceiveMode, SequenceNumber, SessionId, SessionLock,
    SessionRecord, SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
    snapshot_validation::{SnapshotCatalogError, SnapshotStateError, validate_message_rows},
};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

use fixtures::*;

fn ordinary<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let seq = f.send(&f.queue, 2, "one", None, None);
    f.accept(&f.image());
    let delivery = f.receive(&f.queue, 3, ReceiveMode::PeekLock, None);
    let token = delivery.lock.unwrap().token;
    let locked = f.image();
    f.accept(&locked);
    assert!(!locked.iter().any(|(key, _)| key[0] == 0x06));
    f.at(
        &f.queue,
        4,
        CommandKind::RenewLock {
            sequence: seq,
            lock_token: token,
            lock_duration_millis: Some(20),
        },
    );
    f.accept(&f.image());
    assert!(!f.image().iter().any(
        |(key, _)| key == &keys::lock(&f.namespace, &f.queue, Timestamp::from_millis(13), seq)
    ));
    f.at(
        &f.queue,
        5,
        CommandKind::Abandon {
            sequence: seq,
            lock_token: token,
            replacement_envelope: None,
        },
    );
    f.accept(&f.image());
    let delivery = f.receive(&f.queue, 6, ReceiveMode::PeekLock, None);
    f.at(
        &f.queue,
        7,
        CommandKind::Complete {
            sequence: seq,
            lock_token: delivery.lock.unwrap().token,
        },
    );
    assert_eq!(f.accept(&f.image()).message_rows(), 0);
    f.send(&f.queue, 8, "delete", None, None);
    f.receive(&f.queue, 9, ReceiveMode::ReceiveAndDelete, None);
    assert_eq!(f.accept(&f.image()).message_rows(), 0);
    let retained = f.send(&f.queue, 10, "retained", None, None);
    f.receive(&f.queue, 11, ReceiveMode::PeekLock, None);
    let other = EntityPath::new("other").unwrap();
    f.create(&other, 100, QueueConfig::default());
    let expired_lock = f.image();
    assert!(matches!(f.record(&expired_lock, &f.queue, retained).state,
        MessageState::Locked { locked_until, .. } if locked_until < Timestamp::from_millis(100)));
    f.accept(&expired_lock); // No sweep, settlement or deadline rebasing.
    let batch = f.at(
        &f.queue,
        101,
        CommandKind::SendBatch {
            messages: ["batch-one", "batch-two"]
                .into_iter()
                .map(|message_id| domain::MessageInput {
                    message_id: message_id.to_owned(),
                    body: vec![0, 255],
                    ..domain::MessageInput::default()
                })
                .collect(),
        },
    );
    assert!(
        matches!(batch, CommandOutcome::BatchSent { ref sequences, .. } if sequences.len() == 2)
    );
    let image = f.image();
    f.accept(&image);
    let reopened = f.restart();
    assert_eq!(reopened.image(), image);
    reopened.accept(&image);
}

#[test]
fn memory_supported_ready_lock_renewal_settlement_and_retained_expiry() {
    ordinary(MemoryProvider::new());
}
#[test]
fn fjall_supported_ready_lock_renewal_settlement_and_retained_expiry() {
    ordinary(DurableProvider::temporary().unwrap());
}

fn deferred<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let seq = f.send(&f.queue, 2, "deferred", None, None);
    let delivery = f.receive(&f.queue, 3, ReceiveMode::PeekLock, None);
    f.at(
        &f.queue,
        4,
        CommandKind::Defer {
            sequence: seq,
            lock_token: delivery.lock.unwrap().token,
            replacement_envelope: None,
        },
    );
    let image = f.image();
    assert!(f.record(&image, &f.queue, seq).expires_at.is_some());
    assert!(!image.iter().any(|(key, _)| key[0] == 0x06));
    f.accept(&image);
    let delivery = match f.at(
        &f.queue,
        5,
        CommandKind::ReceiveDeferred {
            sequences: vec![seq],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    ) {
        CommandOutcome::DeferredReceived(mut deliveries) => deliveries.remove(0),
        outcome => panic!("{outcome:?}"),
    };
    assert_eq!(delivery.origin, DeliveryOrigin::Deferred);
    let token = delivery.lock.unwrap().token;
    f.accept(&f.image());
    f.at(
        &f.queue,
        6,
        CommandKind::RenewLock {
            sequence: seq,
            lock_token: token,
            lock_duration_millis: Some(20),
        },
    );
    f.accept(&f.image());
    f.at(
        &f.queue,
        7,
        CommandKind::Abandon {
            sequence: seq,
            lock_token: token,
            replacement_envelope: None,
        },
    );
    assert_eq!(
        f.record(&f.image(), &f.queue, seq).state,
        MessageState::Deferred
    );
    f.accept(&f.image());
    f.at(
        &f.queue,
        8,
        CommandKind::ReceiveDeferred {
            sequences: vec![seq],
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    );
    assert_eq!(f.accept(&f.image()).message_rows(), 0);
    let seq = f.send(&f.queue, 9, "elapsed", None, None);
    let delivery = f.receive(&f.queue, 10, ReceiveMode::PeekLock, None);
    f.at(
        &f.queue,
        11,
        CommandKind::Defer {
            sequence: seq,
            lock_token: delivery.lock.unwrap().token,
            replacement_envelope: None,
        },
    );
    f.create(
        &EntityPath::new("later").unwrap(),
        100,
        QueueConfig::default(),
    );
    let image = f.image();
    assert!(
        f.record(&image, &f.queue, seq)
            .is_expired_at(Timestamp::from_millis(100))
    );
    f.accept(&image);
    let reopened = f.restart();
    assert_eq!(reopened.image(), image);
    reopened.accept(&image);
}

#[test]
fn memory_supported_deferred_origins_and_elapsed_retained_deadlines() {
    deferred(MemoryProvider::new());
}
#[test]
fn fjall_supported_deferred_origins_and_elapsed_retained_deadlines() {
    deferred(DurableProvider::temporary().unwrap());
}

fn sessions<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let queue = EntityPath::new("sessions").unwrap();
    let id = SessionId::new("Cart").unwrap();
    let lower = SessionId::new("cart").unwrap();
    f.create(
        &queue,
        2,
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    );
    let seq = f.send(&queue, 3, "case", Some(id.clone()), None);
    f.send(&queue, 4, "lower", Some(lower.clone()), None);
    let before_record = f.image();
    assert!(!before_record.iter().any(|(key, _)| key[0] == 0x08));
    assert!(
        before_record
            .iter()
            .any(|(key, _)| key == &keys::session_ready(&f.namespace, &queue, &id, seq))
    );
    assert_eq!(f.accept(&before_record).pending_session_rows(), 0);
    let hold = match f.at(
        &queue,
        5,
        CommandKind::AcceptSession {
            session_id: Some(id.clone()),
            lock_duration_millis: None,
        },
    ) {
        CommandOutcome::SessionAccepted(Some(session)) => session.hold(),
        outcome => panic!("{outcome:?}"),
    };
    f.at(
        &queue,
        6,
        CommandKind::SetSessionState {
            session: hold.clone(),
            state: vec![255, 0, 1],
        },
    );
    let delivery = f.receive(&queue, 7, ReceiveMode::PeekLock, Some(hold.clone()));
    f.accept(&f.image());
    f.at(&queue, 8, CommandKind::ReleaseSession { session: hold });
    let released = f.image();
    assert!(matches!(
        f.record(&released, &queue, seq).state,
        MessageState::Locked { .. }
    ));
    assert_eq!(
        codec::decode::<SessionRecord>(&value(
            &released,
            &keys::session(&f.namespace, &queue, &id)
        ))
        .unwrap()
        .state,
        vec![255, 0, 1]
    );
    f.accept(&released);
    f.at(
        &queue,
        9,
        CommandKind::Abandon {
            sequence: seq,
            lock_token: delivery.lock.unwrap().token,
            replacement_envelope: None,
        },
    );
    f.accept(&f.image());
    let image = f.image();
    let reopened = f.restart();
    assert_eq!(reopened.image(), image);
    reopened.accept(&image);
}

#[test]
fn memory_supported_case_sensitive_session_routes_and_implicit_records() {
    sessions(MemoryProvider::new());
}
#[test]
fn fjall_supported_case_sensitive_session_routes_and_implicit_records() {
    sessions(DurableProvider::temporary().unwrap());
}

fn scheduled<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let placeholder = f.send(&f.queue, 2, "scheduled", None, Some(10));
    f.accept(&f.image());
    f.create(
        &EntityPath::new("advance").unwrap(),
        20,
        QueueConfig::default(),
    );
    f.accept(&f.image()); // Due placeholders are retained until an activation command.
    f.at(&f.queue, 21, CommandKind::ActivateScheduled);
    let active = f.image();
    let sequence = f
        .machine
        .ready_sequences(&f.namespace, &f.queue, 10)
        .unwrap()[0];
    assert_ne!(sequence, placeholder);
    let record = f.record(&active, &f.queue, sequence);
    assert_eq!(record.enqueued_at, Timestamp::from_millis(21));
    assert_eq!(
        record.scheduled_enqueue_at,
        Some(Timestamp::from_millis(10))
    );
    assert_eq!(record.expires_at, Some(Timestamp::from_millis(51)));
    f.accept(&active);
    let cancel = f.send(&f.queue, 22, "cancel", None, Some(30));
    f.at(
        &f.queue,
        23,
        CommandKind::CancelScheduled {
            sequences: vec![cancel],
        },
    );
    f.accept(&f.image());
    let topic = EntityPath::new("events").unwrap();
    f.at(
        &topic,
        24,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    );
    let name = SubscriptionName::new("first").unwrap();
    let child = topic.subscription(&name).unwrap();
    f.at(
        &topic,
        25,
        CommandKind::CreateSubscription {
            name,
            config: SubscriptionConfig::default(),
        },
    );
    let placeholder = f.send(&topic, 26, "topic", None, Some(40));
    f.accept(&f.image());
    f.at(&topic, 41, CommandKind::ActivateScheduled);
    let copied = f.image();
    let sequence = f.machine.ready_sequences(&f.namespace, &child, 10).unwrap()[0];
    assert_ne!(sequence, placeholder);
    assert_eq!(
        f.record(&copied, &child, sequence).scheduled_enqueue_at,
        Some(Timestamp::from_millis(40))
    );
    assert!(
        !copied
            .iter()
            .any(|(key, _)| key == &keys::message(&f.namespace, &topic, placeholder))
    );
    f.accept(&copied);
    let empty = EntityPath::new("empty-topic").unwrap();
    f.at(
        &empty,
        42,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    );
    f.send(&empty, 43, "no-subs", None, None);
    let placeholder = f.send(&empty, 44, "no-subs-later", None, Some(45));
    f.accept(&f.image());
    f.at(&empty, 46, CommandKind::ActivateScheduled);
    let image = f.image();
    assert!(
        !image
            .iter()
            .any(|(key, _)| key == &keys::message(&f.namespace, &empty, placeholder))
    );
    f.accept(&image);
    let reopened = f.restart();
    assert_eq!(reopened.image(), image);
    reopened.accept(&image);
}

#[test]
fn memory_supported_queue_topic_placeholders_activation_cancellation_and_empty_fanout() {
    scheduled(MemoryProvider::new());
}
#[test]
fn fjall_supported_queue_topic_placeholders_activation_cancellation_and_empty_fanout() {
    scheduled(DurableProvider::temporary().unwrap());
}

fn dead_letters<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let shadow = f.queue.dead_letter_queue().unwrap();
    let seq = f.send(&f.queue, 2, "application", None, None);
    let delivery = f.receive(&f.queue, 3, ReceiveMode::PeekLock, None);
    f.at(
        &f.queue,
        4,
        CommandKind::DeadLetter {
            sequence: seq,
            lock_token: delivery.lock.unwrap().token,
            reason: "application".to_owned(),
            description: "preserved".to_owned(),
            replacement_envelope: None,
        },
    );
    let moved = f.image();
    assert!(
        !moved
            .iter()
            .any(|(key, _)| key == &keys::message(&f.namespace, &f.queue, seq))
    );
    let record = f.record(&moved, &shadow, seq);
    assert!(record.expires_at.is_none() && record.session_id.is_none());
    assert_eq!(record.dead_letter.unwrap().description, "preserved");
    f.accept(&moved);
    let delivery = f.receive(&shadow, 5, ReceiveMode::PeekLock, None);
    f.at(
        &shadow,
        6,
        CommandKind::Defer {
            sequence: seq,
            lock_token: delivery.lock.unwrap().token,
            replacement_envelope: None,
        },
    );
    f.accept(&f.image()); // DLQ deferral is a supported operation, not a nested DLQ.
    let delivery = match f.at(
        &shadow,
        7,
        CommandKind::ReceiveDeferred {
            sequences: vec![seq],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    ) {
        CommandOutcome::DeferredReceived(mut deliveries) => deliveries.remove(0),
        outcome => panic!("{outcome:?}"),
    };
    assert_eq!(delivery.origin, DeliveryOrigin::Deferred);
    f.accept(&f.image());
    f.at(
        &shadow,
        8,
        CommandKind::Abandon {
            sequence: seq,
            lock_token: delivery.lock.unwrap().token,
            replacement_envelope: None,
        },
    );
    f.at(
        &shadow,
        9,
        CommandKind::ReceiveDeferred {
            sequences: vec![seq],
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    );
    f.accept(&f.image());
    let seq = match f.at(
        &f.queue,
        10,
        CommandKind::Send {
            message_id: "ttl".to_owned(),
            body: vec![],
            time_to_live_millis: Some(1),
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
    ) {
        CommandOutcome::Sent { sequence } => sequence,
        outcome => panic!("{outcome:?}"),
    };
    f.at(&f.queue, 12, CommandKind::ExpireMessages);
    assert_eq!(
        f.record(&f.image(), &shadow, seq)
            .dead_letter
            .unwrap()
            .reason,
        domain::DeadLetterReason::TimeToLiveExpired
    );
    f.accept(&f.image());
    let delivery = f.receive(&shadow, 13, ReceiveMode::PeekLock, None);
    f.at(
        &shadow,
        14,
        CommandKind::Complete {
            sequence: seq,
            lock_token: delivery.lock.unwrap().token,
        },
    );
    f.at(
        &f.queue,
        15,
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                max_delivery_count: Some(1),
                ..QueueConfigUpdate::default()
            },
        },
    );
    let seq = f.send(&f.queue, 16, "limit", None, None);
    let delivery = f.receive(&f.queue, 17, ReceiveMode::PeekLock, None);
    f.at(
        &f.queue,
        18,
        CommandKind::Abandon {
            sequence: seq,
            lock_token: delivery.lock.unwrap().token,
            replacement_envelope: None,
        },
    );
    let image = f.image();
    assert_eq!(
        f.record(&image, &shadow, seq).dead_letter.unwrap().reason,
        domain::DeadLetterReason::MaxDeliveryCountExceeded
    );
    f.accept(&image);
    let reopened = f.restart();
    assert_eq!(reopened.image(), image);
    reopened.accept(&image);
}

#[test]
fn memory_supported_dlq_provenance_ready_locks_and_deferred_origins() {
    dead_letters(MemoryProvider::new());
}
#[test]
fn fjall_supported_dlq_provenance_ready_locks_and_deferred_origins() {
    dead_letters(DurableProvider::temporary().unwrap());
}

#[test]
fn memory_injected_all_state_canonical_forms_preserve_original_ordinals() {
    canonical(MemoryProvider::new());
}
#[test]
fn fjall_injected_all_state_canonical_forms_preserve_original_ordinals() {
    canonical(DurableProvider::temporary().unwrap());
}

fn graphs<P: StoreProvider>(provider: P) {
    let f = forms(provider);
    let image = f.image();
    let one = SequenceNumber::new(1);
    let locked = EntityPath::new("locked").unwrap();
    let deferred = EntityPath::new("deferred").unwrap();
    let sessions = EntityPath::new("sessions").unwrap();
    let id = SessionId::new("CaSe").unwrap();
    let MessageState::Locked { locked_until, .. } = f.record(&image, &locked, one).state else {
        panic!("locked fixture must retain a delivery lock");
    };
    let different_locked_until = locked_until
        .as_millis()
        .checked_add(1)
        .map(Timestamp::from_millis)
        .unwrap_or(Timestamp::UNIX_EPOCH);
    assert_ne!(different_locked_until, locked_until);
    for (index, primary) in [
        (
            keys::ready(&f.namespace, &f.queue, one),
            keys::message(&f.namespace, &f.queue, one),
        ),
        (
            keys::expiry(&f.namespace, &f.queue, Timestamp::from_millis(32), one),
            keys::message(&f.namespace, &f.queue, one),
        ),
        (
            keys::lock(&f.namespace, &locked, locked_until, one),
            keys::message(&f.namespace, &locked, one),
        ),
        (
            keys::deferred(&f.namespace, &deferred, one),
            keys::message(&f.namespace, &deferred, one),
        ),
        (
            keys::session_ready(&f.namespace, &sessions, &id, one),
            keys::message(&f.namespace, &sessions, one),
        ),
        (
            keys::scheduled(
                &f.namespace,
                &f.queue,
                Timestamp::from_millis(40),
                SequenceNumber::new(2),
            ),
            keys::message(&f.namespace, &f.queue, SequenceNumber::new(2)),
        ),
    ] {
        let mut broken = image.clone();
        remove(&mut broken, &index);
        assert_eq!(error_row(f.reject(&broken)), row(&broken, &primary));
        let mut orphan = image.clone();
        remove(&mut orphan, &primary);
        let first_orphan = if index[0] == 0x06 {
            keys::ready(&f.namespace, &f.queue, one)
        } else {
            index
        };
        assert_eq!(error_row(f.reject(&orphan)), row(&orphan, &first_orphan));
    }
    for extra in [
        keys::ready(&f.namespace, &f.queue, SequenceNumber::new(999)),
        keys::ready(
            &domain::NamespaceName::new("foreign").unwrap(),
            &f.queue,
            one,
        ),
        keys::lock(&f.namespace, &f.queue, Timestamp::from_millis(20), one),
        keys::expiry(&f.namespace, &f.queue, Timestamp::from_millis(33), one),
        keys::expiry(&f.namespace, &deferred, Timestamp::from_millis(40), one),
        keys::lock(&f.namespace, &locked, different_locked_until, one),
        keys::session_ready(
            &f.namespace,
            &sessions,
            &SessionId::new("case").unwrap(),
            one,
        ),
        keys::ready(&f.namespace, &f.queue, SequenceNumber::new(2)),
    ] {
        let mut broken = image.clone();
        put(&mut broken, extra.clone(), vec![]);
        let error = f.reject(&broken);
        assert!(matches!(
            error,
            SnapshotStateError::InconsistentMessage { .. }
        ));
        assert_eq!(error_row(error), row(&broken, &extra));
    }
    let primary = keys::message(&f.namespace, &f.queue, one);
    let mut wrong = f.record(&image, &f.queue, one);
    wrong.sequence = SequenceNumber::new(9);
    let mut broken = image.clone();
    put(&mut broken, primary.clone(), codec::encode(&wrong).unwrap());
    assert_eq!(error_row(f.reject(&broken)), row(&broken, &primary));
    let mut wrong = f.record(&image, &locked, one);
    if let MessageState::Locked { origin, .. } = &mut wrong.state {
        *origin = DeliveryOrigin::Scheduled;
    }
    let mut broken = image.clone();
    f.put_record(&mut broken, &locked, &wrong);
    assert_eq!(
        error_row(f.reject(&broken)),
        row(&broken, &keys::message(&f.namespace, &locked, one))
    );
    let mut wrong = f.record(&image, &f.queue, SequenceNumber::new(2));
    wrong.scheduled_enqueue_at = None;
    let mut broken = image.clone();
    f.put_record(&mut broken, &f.queue, &wrong);
    assert_eq!(
        error_row(f.reject(&broken)),
        row(
            &broken,
            &keys::message(&f.namespace, &f.queue, wrong.sequence)
        )
    );
    owner_roles(&f, &image);
}

#[test]
fn memory_injected_bidirectional_missing_extra_wrong_scope_state_and_deadline_indexes() {
    graphs(MemoryProvider::new());
}
#[test]
fn fjall_injected_bidirectional_missing_extra_wrong_scope_state_and_deadline_indexes() {
    graphs(DurableProvider::temporary().unwrap());
}

fn partial<P: StoreProvider>(provider: P) {
    let f = forms(provider);
    let image = f.image();
    let mut pending = image.clone();
    let absent = EntityPath::new("unvalidated").unwrap();
    let id = SessionId::new("Pending").unwrap();
    put(
        &mut pending,
        keys::session(&f.namespace, &absent, &id),
        codec::encode(&SessionRecord {
            lock: Some(SessionLock {
                token: domain::LockToken::new(0),
                locked_until: Timestamp::from_millis(0),
            }),
            state: vec![255, 0],
        })
        .unwrap(),
    );
    put(
        &mut pending,
        keys::session_lock(&f.namespace, &absent, Timestamp::from_millis(99), &id),
        vec![],
    );
    put(
        &mut pending,
        keys::duplicate_id(&f.namespace, &absent, "Unicode-\u{e9}\0EXACT"),
        codec::encode(&Timestamp::from_millis(5)).unwrap(),
    );
    put(
        &mut pending,
        keys::duplicate_expiry(
            &f.namespace,
            &absent,
            Timestamp::from_millis(6),
            "Unicode-\u{e9}\0EXACT",
        ),
        vec![],
    );
    put(
        &mut pending,
        keys::queue_counters(&f.namespace, &f.queue),
        codec::encode(&domain::QueueCounters {
            next_sequence: 1,
            next_lock_token: 1,
        })
        .unwrap(),
    );
    put(&mut pending, vec![0xF0], vec![255]);
    put(&mut pending, vec![0xF1], vec![]);
    let report = f.accept(&pending); // Canonical injected pending facts, NOT healthy session/duplicate/allocation graphs.
    assert_eq!(report.pending_session_rows(), 4);
    assert_eq!(report.pending_duplicate_rows(), 4);
    assert_eq!(report.catalog().unvalidated_external_rows(), 2);
    assert!(!report.allocation_relations_checked());
    let primary = keys::message(&f.namespace, &f.queue, SequenceNumber::new(1));
    let ready = keys::ready(&f.namespace, &f.queue, SequenceNumber::new(1));
    let mut broken = image.clone();
    remove(&mut broken, &ready);
    let mut catalog_first = broken.clone();
    put(
        &mut catalog_first,
        keys::queue_config(&f.namespace, &f.queue),
        vec![255],
    );
    assert!(matches!(
        f.reject(&catalog_first),
        SnapshotStateError::Catalog(SnapshotCatalogError::InvalidValue { .. })
    ));
    let duplicate = keys::duplicate_id(
        &f.namespace,
        &EntityPath::new("sessions").unwrap(),
        "duplicate\0exact",
    );
    let mut decode_first = broken.clone();
    put(&mut decode_first, duplicate.clone(), vec![255]);
    assert_eq!(
        error_row(f.reject(&decode_first)),
        row(&decode_first, &duplicate)
    );
    let orphan = keys::ready(&f.namespace, &f.queue, SequenceNumber::new(999));
    put(&mut broken, orphan, vec![]);
    assert_eq!(
        error_row(f.reject(&broken)),
        row(&broken, &primary),
        "forward message phase precedes reverse indexes"
    );
    let mut order = image.clone();
    order.swap(0, 1);
    assert_eq!(
        f.reject(&order),
        SnapshotStateError::Catalog(SnapshotCatalogError::InputOrder { row: 1 })
    );
    let empty = vec![(vec![], vec![])];
    assert_eq!(
        f.reject(&empty),
        SnapshotStateError::Catalog(SnapshotCatalogError::EmptyKey { row: 0 })
    );
    // Full-width key fields are decoded without arithmetic; allocation is pending.
    for number in [0, u64::MAX] {
        let deadline = Timestamp::from_millis(number);
        for state in [
            MessageState::Ready,
            MessageState::Deferred,
            MessageState::Scheduled,
            MessageState::Locked {
                token: domain::LockToken::new(number),
                locked_until: deadline,
                origin: DeliveryOrigin::Ready,
            },
        ] {
            let mut injected = image.clone();
            let mut record = f.record(&image, &f.queue, SequenceNumber::new(1));
            record.sequence = SequenceNumber::new(number);
            record.expires_at = None;
            record.state = state;
            let index = match &record.state {
                MessageState::Ready => {
                    record.expires_at = Some(deadline);
                    put(
                        &mut injected,
                        keys::expiry(&f.namespace, &f.queue, deadline, record.sequence),
                        vec![],
                    );
                    keys::ready(&f.namespace, &f.queue, record.sequence)
                }
                MessageState::Deferred => keys::deferred(&f.namespace, &f.queue, record.sequence),
                MessageState::Scheduled => {
                    record.scheduled_enqueue_at = Some(deadline);
                    keys::scheduled(&f.namespace, &f.queue, deadline, record.sequence)
                }
                MessageState::Locked { .. } => {
                    keys::lock(&f.namespace, &f.queue, deadline, record.sequence)
                }
            };
            f.put_record(&mut injected, &f.queue, &record);
            put(&mut injected, index, vec![]);
            f.accept(&injected); // Canonical DTO composition, not counter or reachability health.
        }
        let sessions = EntityPath::new("sessions").unwrap();
        let mut injected = image.clone();
        let mut record = f.record(&image, &sessions, SequenceNumber::new(1));
        record.sequence = SequenceNumber::new(number);
        record.expires_at = None;
        f.put_record(&mut injected, &sessions, &record);
        put(
            &mut injected,
            keys::session_ready(
                &f.namespace,
                &sessions,
                record.session_id.as_ref().unwrap(),
                record.sequence,
            ),
            vec![],
        );
        put(
            &mut injected,
            keys::session_lock(&f.namespace, &absent, deadline, &id),
            vec![],
        );
        put(
            &mut injected,
            keys::duplicate_expiry(&f.namespace, &absent, deadline, "boundary\0EXACT"),
            vec![],
        );
        f.accept(&injected); // Session/duplicate generation relations are explicitly pending.
    }
    // Lowered future-admission limits do not reclassify retained opaque envelopes.
    let opaque = f.at(
        &f.queue,
        14,
        CommandKind::Send {
            message_id: "opaque".to_owned(),
            body: vec![4; 50],
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: Some(MessageEnvelope::new(vec![255, 0, 8, 9])),
        },
    );
    assert!(matches!(opaque, CommandOutcome::Sent { .. }));
    f.at(
        &f.queue,
        15,
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                max_message_bytes: Some(1),
                ..QueueConfigUpdate::default()
            },
        },
    );
    let retained = f.image();
    f.accept(&retained);
    let reopened = f.restart();
    assert_eq!(reopened.image(), retained);
    reopened.accept(&retained);
    assert!(
        validate_message_rows(&[])
            .unwrap()
            .catalog()
            .clock()
            .is_none()
    );
}

#[test]
fn memory_injected_pending_facets_priority_extremes_and_supported_profile_retention() {
    partial(MemoryProvider::new());
}
#[test]
fn fjall_injected_pending_facets_priority_extremes_and_supported_profile_retention() {
    partial(DurableProvider::temporary().unwrap());
}
