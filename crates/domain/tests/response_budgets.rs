//! Batch response limits are checked before any message or expiry mutation.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use domain::{
    BROKER_HEADER_RESERVE_BYTES, BrokerError, Command, CommandKind, CommandOutcome, Delivery,
    DeliveryBudget, EntityPath, MessageBody, MessageEnvelope, MessageIdentifier, MessageProperties,
    MessageRecord, MessageState, MessageValue, QueueConfig, ReceiveMode, ScheduledMessage,
    SequenceNumber, SessionHold, SessionId, Timestamp,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

const ENTRY_OVERHEAD: u64 = 64;

fn budget(max_bytes: u64) -> DeliveryBudget {
    DeliveryBudget {
        max_bytes,
        per_message_overhead_bytes: ENTRY_OVERHEAD,
    }
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    id: &str,
    body: Vec<u8>,
    ttl: Option<u64>,
    session_id: Option<SessionId>,
) -> Result<SequenceNumber, BrokerError> {
    let CommandOutcome::Sent { sequence } = fixture.at(
        millis,
        CommandKind::Send {
            message_id: id.to_owned(),
            body,
            time_to_live_millis: ttl,
            session_id,
        },
    )?
    else {
        panic!("message sent")
    };
    Ok(sequence)
}

fn record<P: StoreProvider>(fixture: &QueueFixture<P>, sequence: SequenceNumber) -> MessageRecord {
    fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, sequence)
        .expect("message query")
        .expect("stored message")
}

fn defer_next<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    session: Option<SessionHold>,
) -> Result<Delivery, BrokerError> {
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(1_000),
            session,
        },
    )?
    else {
        panic!("message received")
    };
    fixture.at(
        millis,
        CommandKind::Defer {
            sequence: delivery.sequence,
            lock_token: delivery.lock.expect("message lock").token,
        },
    )?;
    Ok(delivery)
}

fn receive_bounded(
    sequences: Vec<SequenceNumber>,
    mode: ReceiveMode,
    max_bytes: u64,
) -> CommandKind {
    CommandKind::ReceiveDeferredBounded {
        sequences,
        mode,
        lock_duration_millis: Some(1_000),
        session_id: None,
        budget: budget(max_bytes),
    }
}

fn peek_bounded(from: u64, count: u32, max_bytes: u64) -> CommandKind {
    CommandKind::PeekBounded {
        from_sequence: SequenceNumber::new(from),
        max_messages: count,
        session_id: None,
        budget: budget(max_bytes),
    }
}

fn peeked(outcome: CommandOutcome) -> Vec<Delivery> {
    let CommandOutcome::Peeked(deliveries) = outcome else {
        panic!("peek response")
    };
    deliveries
}

fn deferred_batches_fit_exactly_or_reject_without_writes<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    for (index, mode) in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete]
        .into_iter()
        .enumerate()
    {
        let base = 10 + index as u64 * 100;
        let first = send(&fixture, base, "first", vec![1; 512], None, None)?;
        let second = send(&fixture, base, "second", vec![2; 768], None, None)?;
        defer_next(&fixture, base + 1, None)?;
        defer_next(&fixture, base + 1, None)?;
        let exact = [first, second]
            .iter()
            .map(|sequence| {
                record(&fixture, *sequence).delivery_size_upper_bound() + ENTRY_OVERHEAD
            })
            .sum::<u64>();
        let snapshot = fixture.machine.store().snapshot()?;
        assert_eq!(
            fixture.at(
                base + 2,
                receive_bounded(vec![first, second], mode, exact - 1)
            ),
            Err(BrokerError::MessageTooLarge {
                body_bytes: exact as usize,
                maximum_bytes: (exact - 1) as usize
            })
        );
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
        fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
        let CommandOutcome::DeferredReceived(deliveries) =
            fixture.at(base + 2, receive_bounded(vec![first, second], mode, exact))?
        else {
            panic!("deferred response")
        };
        assert_eq!(
            deliveries
                .iter()
                .map(|delivery| delivery.sequence)
                .collect::<Vec<_>>(),
            vec![first, second]
        );
        for delivery in deliveries {
            assert_eq!(delivery.delivery_count, 2);
            if let Some(lock) = delivery.lock {
                fixture.at(
                    base + 3,
                    CommandKind::Complete {
                        sequence: delivery.sequence,
                        lock_token: lock.token,
                    },
                )?;
            } else {
                assert!(
                    fixture
                        .machine
                        .message(&fixture.namespace, &fixture.entity, delivery.sequence)?
                        .is_none()
                );
            }
        }
    }
    Ok(())
}

fn expired_deferred_records_charge_before_drop_or_dead_letter_cleanup<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    for (index, dead_letter) in [false, true].into_iter().enumerate() {
        if dead_letter {
            fixture.entity = EntityPath::new("expiry-dlq")?;
            fixture.at(
                20,
                CommandKind::CreateQueue {
                    config: QueueConfig {
                        dead_lettering_on_message_expiration: true,
                        ..QueueConfig::default()
                    },
                },
            )?;
        }
        let base = 10 + index as u64 * 20;
        let expired = send(&fixture, base, "expired", vec![3; 512], Some(5), None)?;
        let live = send(&fixture, base, "live", vec![4; 512], None, None)?;
        defer_next(&fixture, base + 1, None)?;
        defer_next(&fixture, base + 1, None)?;
        let expired_bytes = record(&fixture, expired).delivery_size_upper_bound() + ENTRY_OVERHEAD;
        let exact =
            expired_bytes + record(&fixture, live).delivery_size_upper_bound() + ENTRY_OVERHEAD;
        let snapshot = fixture.machine.store().snapshot()?;
        assert!(matches!(
            fixture.at(
                base + 6,
                receive_bounded(
                    vec![expired, live],
                    ReceiveMode::ReceiveAndDelete,
                    expired_bytes
                )
            ),
            Err(BrokerError::MessageTooLarge { .. })
        ));
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
        fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
        let CommandOutcome::DeferredReceived(deliveries) = fixture.at(
            base + 6,
            receive_bounded(vec![expired, live], ReceiveMode::ReceiveAndDelete, exact),
        )?
        else {
            panic!("deferred response")
        };
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].sequence, live);
        assert!(
            fixture
                .machine
                .message(&fixture.namespace, &fixture.entity, expired)?
                .is_none()
        );
        assert_eq!(
            fixture
                .machine
                .message(
                    &fixture.namespace,
                    &fixture.entity.dead_letter_queue()?,
                    expired
                )?
                .is_some(),
            dead_letter
        );
    }
    Ok(())
}

fn peek_returns_a_fitting_prefix_and_rejects_an_oversized_first_result<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let sequences = (0..3)
        .map(|index| {
            send(
                &fixture,
                10,
                &format!("m-{index}"),
                vec![index as u8; 512],
                None,
                None,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let one = record(&fixture, sequences[0]).delivery_size_upper_bound() + ENTRY_OVERHEAD;
    let snapshot = fixture.machine.store().snapshot()?;
    assert!(matches!(
        fixture.at(20, peek_bounded(1, 10, one - 1)),
        Err(BrokerError::MessageTooLarge { .. })
    ));
    let first = peeked(fixture.at(20, peek_bounded(1, 10, one * 2))?);
    assert_eq!(
        first
            .iter()
            .map(|delivery| delivery.sequence)
            .collect::<Vec<_>>(),
        sequences[..2]
    );
    let last = peeked(fixture.at(20, peek_bounded(3, 10, one))?);
    assert_eq!(last[0].sequence, sequences[2]);
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    let fixture = fixture.restart()?;
    assert_eq!(peeked(fixture.at(20, peek_bounded(1, 0, 0))?).len(), 0);
    assert_eq!(
        peeked(fixture.at(
            20,
            CommandKind::Peek {
                from_sequence: SequenceNumber::new(1),
                max_messages: 10,
                session_id: None,
            }
        )?)
        .len(),
        3
    );
    Ok(())
}

fn rich_estimates_use_the_envelope_not_the_compatibility_body<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let envelope = MessageEnvelope {
        properties: MessageProperties {
            message_id: Some(MessageIdentifier::String(String::from("typed"))),
            ..MessageProperties::default()
        },
        application_properties: BTreeMap::from([(
            String::from("metadata"),
            MessageValue::String("x".repeat(1_000)),
        )]),
        body: MessageBody::Data(vec![vec![7; 512], vec![8; 512]]),
        ..MessageEnvelope::default()
    };
    let content_bytes = envelope.content_size() as u64;
    let CommandOutcome::Sent { sequence } = fixture.at(
        10,
        CommandKind::SendEnvelope {
            message_id: String::from("typed"),
            body: vec![0; 20_000],
            time_to_live_millis: None,
            session_id: None,
            envelope: Box::new(envelope.clone()),
        },
    )?
    else {
        panic!("rich send")
    };
    assert_eq!(
        record(&fixture, sequence).delivery_size_upper_bound(),
        content_bytes + BROKER_HEADER_RESERVE_BYTES as u64
    );
    let exact = content_bytes + BROKER_HEADER_RESERVE_BYTES as u64 + ENTRY_OVERHEAD;
    assert!(matches!(
        fixture.at(20, peek_bounded(1, 1, exact - 1)),
        Err(BrokerError::MessageTooLarge { .. })
    ));
    let deliveries = peeked(fixture.at(20, peek_bounded(1, 1, exact))?);
    assert_eq!(deliveries[0].envelope.as_deref(), Some(&envelope));
    assert_eq!(deliveries[0].body.len(), 20_000);
    defer_next(&fixture, 20, None)?;
    let CommandOutcome::DeferredReceived(deliveries) = fixture.at(
        21,
        receive_bounded(vec![sequence], ReceiveMode::ReceiveAndDelete, exact),
    )?
    else {
        panic!("rich deferred response")
    };
    assert_eq!(deliveries[0].envelope.as_deref(), Some(&envelope));
    Ok(())
}

fn session_content_is_charged_and_wrong_session_rejections_are_atomic<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    )?;
    let session = SessionId::new("cart-🚀")?;
    let sequence = send(
        &fixture,
        10,
        "session",
        vec![1; 512],
        None,
        Some(session.clone()),
    )?;
    let stored = record(&fixture, sequence);
    assert_eq!(
        stored.delivery_size_upper_bound(),
        (512 + 5 + "session".len() + BROKER_HEADER_RESERVE_BYTES + 5 + session.as_str().len())
            as u64
    );
    let CommandOutcome::SessionAccepted(Some(accepted)) = fixture.at(
        11,
        CommandKind::AcceptSession {
            session_id: Some(session.clone()),
            lock_duration_millis: Some(1_000),
        },
    )?
    else {
        panic!("session accepted")
    };
    defer_next(&fixture, 12, Some(accepted.hold()))?;
    let exact = stored.delivery_size_upper_bound() + ENTRY_OVERHEAD;
    let snapshot = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.at(
            13,
            receive_bounded(vec![sequence], ReceiveMode::ReceiveAndDelete, exact)
        ),
        Err(BrokerError::SessionRequired)
    );
    for (session_id, max_bytes, expected) in [
        (
            SessionId::new("other")?,
            exact,
            BrokerError::MessageNotDeferred { sequence },
        ),
        (
            session.clone(),
            exact - 1,
            BrokerError::MessageTooLarge {
                body_bytes: exact as usize,
                maximum_bytes: (exact - 1) as usize,
            },
        ),
    ] {
        assert_eq!(
            fixture.at(
                13,
                CommandKind::ReceiveDeferredBounded {
                    sequences: vec![sequence],
                    mode: ReceiveMode::ReceiveAndDelete,
                    lock_duration_millis: None,
                    session_id: Some(session_id),
                    budget: budget(max_bytes),
                }
            ),
            Err(expected)
        );
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    }
    let deliveries = peeked(fixture.at(
        13,
        CommandKind::PeekBounded {
            from_sequence: sequence,
            max_messages: 1,
            session_id: Some(session),
            budget: budget(exact),
        },
    )?);
    assert_eq!(deliveries[0].sequence, sequence);
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    Ok(())
}

fn dead_letter_details_are_part_of_the_delivery_budget<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let sequence = send(&fixture, 10, "dlq", vec![1; 128], None, None)?;
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        11,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("message received")
    };
    fixture.at(
        12,
        CommandKind::DeadLetter {
            sequence,
            lock_token: delivery.lock.expect("lock").token,
            reason: "reason".repeat(100),
            description: "detail".repeat(200),
        },
    )?;
    fixture.entity = fixture.entity.dead_letter_queue()?;
    let stored = record(&fixture, sequence);
    let no_details =
        (stored.body.len() + stored.message_id.len() + 5 + BROKER_HEADER_RESERVE_BYTES) as u64;
    let exact = stored.delivery_size_upper_bound() + ENTRY_OVERHEAD;
    assert!(exact > no_details + 1_700);
    assert!(matches!(
        fixture.at(13, peek_bounded(1, 1, no_details + ENTRY_OVERHEAD)),
        Err(BrokerError::MessageTooLarge { .. })
    ));
    assert_eq!(
        peeked(fixture.at(13, peek_bounded(1, 1, exact))?)[0].dead_letter,
        stored.dead_letter
    );
    defer_next(&fixture, 13, None)?;
    let snapshot = fixture.machine.store().snapshot()?;
    assert!(matches!(
        fixture.at(
            14,
            receive_bounded(vec![sequence], ReceiveMode::ReceiveAndDelete, exact - 1)
        ),
        Err(BrokerError::MessageTooLarge { .. })
    ));
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    let fixture = fixture.restart()?;
    let CommandOutcome::DeferredReceived(deliveries) = fixture.at(
        14,
        receive_bounded(vec![sequence], ReceiveMode::ReceiveAndDelete, exact),
    )?
    else {
        panic!("DLQ deferred response")
    };
    assert!(deliveries[0].dead_letter.is_some());
    Ok(())
}

fn aggregate_overflow_and_later_invalid_records_do_not_commit_earlier_locks<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let first = send(&fixture, 10, "deferred", vec![1; 32], None, None)?;
    defer_next(&fixture, 11, None)?;
    let ready = send(&fixture, 12, "ready", vec![2; 32], None, None)?;
    let snapshot = fixture.machine.store().snapshot()?;
    assert!(matches!(
        fixture.at(
            20,
            CommandKind::ReceiveDeferredBounded {
                sequences: vec![first],
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session_id: None,
                budget: DeliveryBudget {
                    max_bytes: u64::MAX,
                    per_message_overhead_bytes: u64::MAX
                },
            }
        ),
        Err(BrokerError::MessageTooLarge { .. })
    ));
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    assert_eq!(
        fixture.at(
            20,
            receive_bounded(vec![first, ready], ReceiveMode::PeekLock, u64::MAX)
        ),
        Err(BrokerError::MessageNotDeferred { sequence: ready })
    );
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    assert!(matches!(
        fixture.at(
            20,
            receive_bounded(vec![first, first], ReceiveMode::ReceiveAndDelete, u64::MAX)
        ),
        Err(BrokerError::InvalidMessageContent { .. })
    ));
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    let fixture = fixture.restart()?;
    assert_eq!(record(&fixture, first).state, MessageState::Deferred);
    let CommandOutcome::DeferredReceived(deliveries) = fixture.at(
        20,
        CommandKind::ReceiveDeferred {
            sequences: vec![first],
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session_id: None,
        },
    )?
    else {
        panic!("legacy deferred response")
    };
    assert_eq!(deliveries.len(), 1);
    Ok(())
}

fn peek_scan_fairness_and_scheduled_state_remain_bounded<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    for index in 0..256 {
        send(
            &fixture,
            10,
            &format!("expired-{index}"),
            vec![0; 1],
            Some(1),
            None,
        )?;
    }
    let live = send(&fixture, 10, "live", vec![1; 64], None, None)?;
    let CommandOutcome::Scheduled { sequences } = fixture.at(
        11,
        CommandKind::Schedule {
            messages: vec![ScheduledMessage {
                message_id: String::from("scheduled"),
                body: vec![2; 64],
                time_to_live_millis: Some(5),
                session_id: None,
                enqueue_at: Timestamp::from_millis(100),
            }],
        },
    )?
    else {
        panic!("scheduled")
    };
    let snapshot = fixture.machine.store().snapshot()?;
    assert!(peeked(fixture.at(20, peek_bounded(1, 10, u64::MAX))?).is_empty());
    let exact = record(&fixture, live).delivery_size_upper_bound()
        + record(&fixture, sequences[0]).delivery_size_upper_bound()
        + ENTRY_OVERHEAD * 2;
    let deliveries = peeked(fixture.at(20, peek_bounded(live.as_u64(), 10, exact))?);
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[1].status, domain::MessageStatus::Scheduled);
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    Ok(())
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    largest_scan: Arc<AtomicUsize>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
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
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.largest_scan.fetch_max(limit, Ordering::Relaxed);
        self.inner.scan_from(prefix, start, limit)
    }
}

struct ObservedProvider<P> {
    inner: P,
    largest_scan: Arc<AtomicUsize>,
}

impl<P: StoreProvider> StoreProvider for ObservedProvider<P> {
    type Store = ObservedStore<P::Store>;
    fn open(&self) -> Result<Self::Store, StorageError> {
        Ok(ObservedStore {
            inner: self.inner.open()?,
            largest_scan: self.largest_scan.clone(),
        })
    }
}

fn bounded_peek_never_materializes_a_large_storage_page<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let largest_scan = Arc::new(AtomicUsize::new(0));
    let fixture = QueueFixture::with_defaults(
        ObservedProvider {
            inner: provider,
            largest_scan: largest_scan.clone(),
        },
        "tenant",
        "orders",
    )?;
    let first = send(&fixture, 10, "first", vec![0; 16_000], None, None)?;
    send(&fixture, 10, "second", vec![1; 16_000], None, None)?;
    let exact = record(&fixture, first).delivery_size_upper_bound() + ENTRY_OVERHEAD;
    largest_scan.store(0, Ordering::Relaxed);
    assert_eq!(peeked(fixture.at(20, peek_bounded(1, 2, exact))?).len(), 1);
    assert_eq!(largest_scan.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn new_command_discriminants_are_appended_without_changing_legacy_shapes()
-> Result<(), Box<dyn Error>> {
    let legacy = CommandKind::ReceiveDeferred {
        sequences: vec![SequenceNumber::new(7)],
        mode: ReceiveMode::ReceiveAndDelete,
        lock_duration_millis: Some(9),
        session_id: None,
    };
    assert_eq!(postcard::to_stdvec(&legacy)?, vec![11, 1, 7, 1, 1, 9, 0]);
    let legacy = CommandKind::Peek {
        from_sequence: SequenceNumber::new(7),
        max_messages: 2,
        session_id: None,
    };
    assert_eq!(postcard::to_stdvec(&legacy)?, vec![5, 7, 2, 0]);
    for (discriminant, kind) in [
        (
            25,
            receive_bounded(vec![SequenceNumber::new(7)], ReceiveMode::PeekLock, 1_024),
        ),
        (26, peek_bounded(7, 2, 1_024)),
    ] {
        let encoded = postcard::to_stdvec(&kind)?;
        assert_eq!(encoded[0], discriminant);
        assert_eq!(postcard::from_bytes::<CommandKind>(&encoded)?, kind);
    }
    let command = Command::new(
        domain::NamespaceName::new("tenant")?,
        EntityPath::new("orders")?,
        Timestamp::from_millis(20),
        peek_bounded(7, 2, 1_024),
    );
    assert_eq!(
        postcard::from_bytes::<Command>(&postcard::to_stdvec(&command)?)?,
        command
    );
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    deferred_batches_fit_exactly_or_reject_without_writes,
    expired_deferred_records_charge_before_drop_or_dead_letter_cleanup,
    peek_returns_a_fitting_prefix_and_rejects_an_oversized_first_result,
    rich_estimates_use_the_envelope_not_the_compatibility_body,
    session_content_is_charged_and_wrong_session_rejections_are_atomic,
    dead_letter_details_are_part_of_the_delivery_budget,
    aggregate_overflow_and_later_invalid_records_do_not_commit_earlier_locks,
    peek_scan_fairness_and_scheduled_state_remain_bounded,
    bounded_peek_never_materializes_a_large_storage_page,
}
