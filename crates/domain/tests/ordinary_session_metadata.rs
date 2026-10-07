//! Ordinary ingress retains session metadata without granting session affinity.

use std::{collections::BTreeMap, error::Error};

use domain::{
    BrokerError, CommandKind, CommandOutcome, CommittedApplication, CommittedApplyResult,
    CommittedCheckpointUpdate, CommittedEntryId, CommittedQueueCommand, CommittedQueueWork,
    CommittedSend, CommittedStateMachine, CommittedStreamId, DeadLetterReason,
    DecodedCommittedImage, Delivery, DeliveryBudget, EntityPath, IngressBatchLimit,
    IngressEnvelope, LockToken, MAX_INGRESS_BATCH_CONTENT_BYTES, MessageBody, MessageEnvelope,
    MessageIdentifier, MessageProperties, MessageState, MessageValue, NamespaceName, QueueConfig,
    QueueConfigUpdate, ReceiveMode, ScheduledEnvelope, ScheduledMessage, SequenceNumber,
    SessionHold, SessionId, SettlementDisposition, Timestamp, ValidatedCreateSendLayout17Image,
    keys,
};
use storage::{BoundedStateStore, CommittedStore, StateStore};
use testkit::{QueueFixture, StoreProvider};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn member(id: &str, session: Option<&str>) -> IngressEnvelope {
    IngressEnvelope {
        message_id: id.into(),
        body: id.as_bytes().to_vec(),
        time_to_live_millis: None,
        session_id: session.map(|value| SessionId::new(value).expect("valid session metadata")),
        scheduled_enqueue_time: None,
        envelope: MessageEnvelope {
            properties: MessageProperties {
                message_id: Some(MessageIdentifier::String(id.into())),
                ..MessageProperties::default()
            },
            body: MessageBody::Data(vec![id.as_bytes().to_vec()]),
            ..MessageEnvelope::default()
        },
    }
}

fn rich(message: IngressEnvelope) -> CommandKind {
    CommandKind::SendEnvelope {
        message_id: message.message_id,
        body: message.body,
        time_to_live_millis: message.time_to_live_millis,
        session_id: message.session_id,
        envelope: Box::new(message.envelope),
    }
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    mode: ReceiveMode,
) -> TestResult<Option<Delivery>> {
    let CommandOutcome::Received(delivery) = fixture.at(
        millis,
        CommandKind::Receive {
            mode,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("receive outcome");
    };
    Ok(delivery)
}

fn metadata(delivery: &Delivery, session: Option<&str>) {
    assert_eq!(delivery.session_id.as_ref().map(SessionId::as_str), session);
}

fn no_session_rows<P: StoreProvider>(fixture: &QueueFixture<P>) -> TestResult {
    for prefix in [
        keys::entity_session_ready_prefix(&fixture.namespace, &fixture.entity),
        keys::entity_session_prefix(&fixture.namespace, &fixture.entity),
    ] {
        assert!(fixture.machine.store().scan_prefix(&prefix, 1)?.is_empty());
    }
    Ok(())
}

fn ingress_metadata_uses_global_ready_order_after_restart<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    assert_eq!(
        fixture.at(
            1,
            CommandKind::Send {
                message_id: "legacy".into(),
                body: b"legacy".to_vec(),
                time_to_live_millis: None,
                session_id: Some(SessionId::new("cart-a")?),
            }
        )?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    let envelope = member("rich", Some("cart-b"));
    fixture.at(2, rich(envelope.clone()))?;
    let batch = [member("unnamed", None), member("repeat", Some("cart-a"))];
    assert_eq!(
        fixture.at(
            3,
            CommandKind::SendBatch {
                messages: batch.to_vec()
            }
        )?,
        CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(3), SequenceNumber::new(4)]
        }
    );
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 10)?,
        (1..=4).map(SequenceNumber::new).collect::<Vec<_>>()
    );
    no_session_rows(&fixture)?;
    let before = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    for (index, (id, session)) in [
        ("legacy", Some("cart-a")),
        ("rich", Some("cart-b")),
        ("unnamed", None),
        ("repeat", Some("cart-a")),
    ]
    .into_iter()
    .enumerate()
    {
        let delivery =
            receive(&fixture, 4, ReceiveMode::ReceiveAndDelete)?.expect("ordinary delivery");
        assert_eq!(delivery.sequence, SequenceNumber::new(index as u64 + 1));
        assert_eq!(delivery.message_id, id);
        assert_eq!(delivery.lock, None);
        metadata(&delivery, session);
        if index == 1 {
            assert_eq!(delivery.envelope.as_deref(), Some(&envelope.envelope));
        }
        if index >= 2 {
            assert_eq!(
                delivery.envelope.as_deref(),
                Some(&batch[index - 2].envelope)
            );
        }
    }
    assert!(receive(&fixture, 4, ReceiveMode::ReceiveAndDelete)?.is_none());
    no_session_rows(&fixture)
}

fn ordinary_ownership_is_strict_while_metadata_survives_message_lifecycle<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            lock_duration_millis: 5,
            max_delivery_count: 10,
            ..QueueConfig::default()
        },
    )?;
    fixture.at(1, rich(member("content", Some("cart"))))?;
    let hold = SessionHold::new(SessionId::new("cart")?, LockToken::new(1));
    for kind in [
        CommandKind::AcceptSession {
            session_id: None,
            lock_duration_millis: None,
        },
        CommandKind::AcceptSession {
            session_id: Some(hold.session_id.clone()),
            lock_duration_millis: None,
        },
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: Some(hold.clone()),
        },
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 1,
            session_id: Some(hold.session_id.clone()),
        },
        CommandKind::ReceiveDeferredHeld {
            sequences: vec![SequenceNumber::new(1)],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: Some(hold),
            budget: DeliveryBudget {
                max_bytes: u64::MAX,
                per_message_overhead_bytes: 0,
            },
        },
    ] {
        let before = fixture.machine.store().snapshot()?;
        assert_eq!(fixture.at(2, kind), Err(BrokerError::SessionNotSupported));
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    let CommandOutcome::Peeked(peeked) = fixture.at(
        3,
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 1,
            session_id: None,
        },
    )?
    else {
        panic!("peek outcome");
    };
    metadata(&peeked[0], Some("cart"));
    assert_eq!(peeked[0].lock, None);
    let first = receive(&fixture, 4, ReceiveMode::PeekLock)?.expect("first lock");
    let token = first.lock.expect("message lock").token;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.at(
            5,
            CommandKind::Complete {
                sequence: first.sequence,
                lock_token: LockToken::new(token.as_u64() + 1),
            }
        ),
        Err(BrokerError::LockTokenMismatch {
            sequence: first.sequence
        })
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    fixture.at(
        5,
        CommandKind::Settle {
            sequence: first.sequence,
            lock_token: token,
            disposition: SettlementDisposition::Abandon,
            properties_to_modify: BTreeMap::from([(
                "stage".into(),
                MessageValue::String("abandoned".into()),
            )]),
        },
    )?;
    let second = receive(&fixture, 6, ReceiveMode::PeekLock)?.expect("abandoned metadata");
    metadata(&second, Some("cart"));
    assert_eq!(second.delivery_count, 2);
    assert_eq!(
        fixture.at(11, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 1,
            dead_lettered: 0,
            dropped: 0,
        }
    );
    let third = receive(&fixture, 12, ReceiveMode::PeekLock)?.expect("expired lock requeue");
    metadata(&third, Some("cart"));
    assert_eq!(third.delivery_count, 3);
    assert_eq!(
        fixture.at(
            13,
            CommandKind::Settle {
                sequence: third.sequence,
                lock_token: third.lock.expect("third lock").token,
                disposition: SettlementDisposition::Defer,
                properties_to_modify: BTreeMap::from([(
                    "stage".into(),
                    MessageValue::String("deferred".into())
                )]),
            }
        )?,
        CommandOutcome::Deferred
    );
    assert!(receive(&fixture, 14, ReceiveMode::PeekLock)?.is_none());
    let fixture = fixture.restart()?;
    let CommandOutcome::DeferredReceived(deliveries) = fixture.at(
        15,
        CommandKind::ReceiveDeferredHeld {
            sequences: vec![third.sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
            budget: DeliveryBudget {
                max_bytes: u64::MAX,
                per_message_overhead_bytes: 0,
            },
        },
    )?
    else {
        panic!("deferred outcome");
    };
    assert_eq!(deliveries.len(), 1);
    metadata(&deliveries[0], Some("cart"));
    assert_eq!(
        deliveries[0]
            .envelope
            .as_ref()
            .expect("retained envelope")
            .application_properties
            .get("stage"),
        Some(&MessageValue::String("deferred".into()))
    );
    assert_eq!(
        fixture.at(
            16,
            CommandKind::Complete {
                sequence: third.sequence,
                lock_token: deliveries[0].lock.expect("deferred lock").token,
            }
        )?,
        CommandOutcome::Completed
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, third.sequence)?
            .is_none()
    );
    no_session_rows(&fixture)
}

fn mixed_scheduling_retains_metadata_without_creating_session_indexes<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let cart = SessionId::new("cart")?;
    assert_eq!(
        fixture.at(
            1,
            CommandKind::Schedule {
                messages: vec![ScheduledMessage {
                    message_id: "legacy".into(),
                    body: Vec::new(),
                    time_to_live_millis: None,
                    session_id: Some(cart.clone()),
                    enqueue_at: Timestamp::from_millis(100),
                }]
            }
        )?,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(1)]
        }
    );
    let typed = member("typed", Some("other"));
    fixture.at(
        2,
        CommandKind::ScheduleEnvelopes {
            messages: vec![ScheduledEnvelope {
                message_id: typed.message_id.clone(),
                body: typed.body.clone(),
                time_to_live_millis: None,
                session_id: typed.session_id.clone(),
                envelope: typed.envelope.clone(),
                enqueue_at: Timestamp::from_millis(100),
            }],
        },
    )?;
    let mut cancelled = member("cancelled", Some("cart"));
    cancelled.scheduled_enqueue_time = Some(Timestamp::from_millis(100));
    let mut unnamed = member("unnamed", None);
    unnamed.scheduled_enqueue_time = Some(Timestamp::from_millis(100));
    fixture.at(
        3,
        CommandKind::SendBatch {
            messages: vec![cancelled, unnamed],
        },
    )?;
    fixture.at(4, rich(member("active", Some("cart"))))?;
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture.at(
            5,
            CommandKind::CancelScheduled {
                sequences: vec![SequenceNumber::new(3)]
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    assert_eq!(
        fixture.at(99, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 0 }
    );
    assert_eq!(
        fixture.at(100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 3 }
    );
    for (id, session, sequence) in [
        ("active", Some("cart"), 5),
        ("legacy", Some("cart"), 6),
        ("typed", Some("other"), 7),
        ("unnamed", None, 8),
    ] {
        let delivery = receive(&fixture, 101, ReceiveMode::ReceiveAndDelete)?
            .expect("global activation order");
        assert_eq!(delivery.message_id, id);
        assert_eq!(delivery.sequence, SequenceNumber::new(sequence));
        metadata(&delivery, session);
        if id == "typed" {
            assert_eq!(delivery.envelope.as_deref(), Some(&typed.envelope));
        }
    }
    assert!(receive(&fixture, 101, ReceiveMode::ReceiveAndDelete)?.is_none());
    no_session_rows(&fixture)
}

fn dead_letters_keep_existing_session_stripping_for_all_transitions<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_delivery_count: 2,
            lock_duration_millis: 5,
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        },
    )?;
    for (millis, id, ttl) in [
        (1, "explicit", None),
        (2, "limit", None),
        (3, "locked-ttl", Some(20)),
        (4, "ready-ttl", Some(20)),
    ] {
        let mut message = member(id, Some("cart"));
        message.time_to_live_millis = ttl;
        fixture.at(millis, rich(message))?;
    }
    let first = receive(&fixture, 5, ReceiveMode::PeekLock)?.expect("explicit");
    fixture.at(
        6,
        CommandKind::DeadLetter {
            sequence: first.sequence,
            lock_token: first.lock.expect("lock").token,
            reason: "manual".into(),
            description: "requested".into(),
        },
    )?;
    let second = receive(&fixture, 7, ReceiveMode::PeekLock)?.expect("limit first");
    fixture.at(
        8,
        CommandKind::Abandon {
            sequence: second.sequence,
            lock_token: second.lock.expect("lock").token,
        },
    )?;
    let second = receive(&fixture, 9, ReceiveMode::PeekLock)?.expect("limit second");
    assert_eq!(
        fixture.at(
            10,
            CommandKind::Abandon {
                sequence: second.sequence,
                lock_token: second.lock.expect("lock").token
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: true,
            dropped: false
        }
    );
    let third = receive(&fixture, 21, ReceiveMode::PeekLock)?.expect("locked ttl");
    assert_eq!(third.message_id, "locked-ttl");
    assert_eq!(
        fixture.at(26, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 0,
            dead_lettered: 1,
            dropped: 0,
        }
    );
    assert_eq!(
        fixture.at(26, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 1,
            dropped: 0,
            processed: 1
        }
    );
    let shadow = fixture.entity.dead_letter_queue()?;
    let fixture = fixture.restart()?;
    for (sequence, reason) in [
        (1, DeadLetterReason::Application("manual".into())),
        (2, DeadLetterReason::MaxDeliveryCountExceeded),
        (3, DeadLetterReason::TimeToLiveExpired),
        (4, DeadLetterReason::TimeToLiveExpired),
    ] {
        let record = fixture
            .machine
            .message(&fixture.namespace, &shadow, SequenceNumber::new(sequence))?
            .expect("dead letter");
        assert_eq!(record.state, MessageState::Ready);
        assert_eq!(record.session_id, None);
        assert_eq!(record.expires_at, None);
        assert_eq!(record.dead_letter.as_ref().expect("reason").reason, reason);
        let mut command = fixture.command(
            27,
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        );
        command.entity = shadow.clone();
        let CommandOutcome::Received(Some(delivery)) = fixture.machine.apply(&command)? else {
            panic!("DLQ delivery");
        };
        assert_eq!(delivery.sequence, SequenceNumber::new(sequence));
        metadata(&delivery, None);
        assert_eq!(delivery.time_to_live_millis, None);
    }
    no_session_rows(&fixture)
}

fn metadata_is_charged_to_existing_message_and_batch_limits<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let message = member("sized", Some("cart"));
    let size = message.envelope.content_size() + 5 + "cart".len();
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_message_bytes: size - 1,
            ..QueueConfig::default()
        },
    )?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.at(1, rich(message.clone())),
        Err(BrokerError::MessageTooLarge {
            body_bytes: size,
            maximum_bytes: size - 1
        })
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    fixture.at(
        1,
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                max_message_bytes: Some(size),
                ..QueueConfigUpdate::default()
            },
        },
    )?;
    fixture.at(2, rich(message))?;
    fixture.at(
        3,
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                max_message_bytes: Some(MAX_INGRESS_BATCH_CONTENT_BYTES),
                ..QueueConfigUpdate::default()
            },
        },
    )?;
    let mut exact = member("normalized", Some("cart"));
    exact.envelope.body = MessageBody::Empty;
    let overhead = exact.envelope.content_size() + exact.message_id.len() + "cart".len();
    exact.body = vec![0; MAX_INGRESS_BATCH_CONTENT_BYTES - overhead];
    let mut excess = exact.clone();
    excess.body.push(0);
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.at(
            4,
            CommandKind::SendBatch {
                messages: vec![excess]
            }
        ),
        Err(BrokerError::IngressBatchLimitExceeded {
            limit: IngressBatchLimit::ContentBytes,
            actual: MAX_INGRESS_BATCH_CONTENT_BYTES + 1,
            maximum: MAX_INGRESS_BATCH_CONTENT_BYTES
        })
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    fixture.at(
        4,
        CommandKind::SendBatch {
            messages: vec![exact],
        },
    )?;
    no_session_rows(&fixture)
}

fn closed_committed_profile_keeps_plain_session_refusal<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let stream = CommittedStreamId::new([7; 16])?;
    let namespace = NamespaceName::new("tenant")?;
    let entity = EntityPath::new("orders")?;
    let mut machine = CommittedStateMachine::create(writer, stream)?;
    let initialized = machine.reader().snapshot()?;
    assert_eq!(initialized.entries().len(), 1);
    let checkpoint_key = initialized.entries()[0].0.clone();
    let create = CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
        namespace.clone(),
        entity.clone(),
        Timestamp::from_millis(1),
        QueueConfig {
            max_message_bytes: 1,
            ..QueueConfig::default()
        },
    ));
    let update = CommittedCheckpointUpdate {
        stream,
        expected_previous: None,
        entry: CommittedEntryId {
            term: 1,
            node_id: 9,
            index: 0,
        },
    };
    assert!(matches!(
        machine.apply_committed(&update, &create)?,
        CommittedApplyResult::Applied {
            application: CommittedApplication::Queue(_),
            ..
        }
    ));
    let before = machine.reader().snapshot()?;
    let old_clock = machine.reader().get(&keys::clock())?;
    let update = CommittedCheckpointUpdate {
        stream,
        expected_previous: machine.checkpoint()?.last(),
        entry: CommittedEntryId {
            term: 1,
            node_id: 9,
            index: 1,
        },
    };
    let work = CommittedQueueWork::Queue(CommittedQueueCommand::send(
        namespace.clone(),
        entity.clone(),
        Timestamp::from_millis(2),
        CommittedSend {
            message_id: "metadata".into(),
            body: vec![1, 2],
            time_to_live_millis: None,
            session_id: Some(SessionId::new("cart")?),
        },
    ));
    assert!(matches!(
        machine.apply_committed(&update, &work)?,
        CommittedApplyResult::Applied {
            application: CommittedApplication::Refused(BrokerError::SessionNotSupported),
            ..
        }
    ));
    let after = machine.reader().snapshot()?;
    assert_eq!(
        before
            .entries()
            .iter()
            .filter(|(key, _)| key != &checkpoint_key)
            .collect::<Vec<_>>(),
        after
            .entries()
            .iter()
            .filter(|(key, _)| key != &checkpoint_key)
            .collect::<Vec<_>>()
    );
    assert_eq!(machine.reader().get(&keys::clock())?, old_clock);
    assert_eq!(
        machine
            .checkpoint()?
            .last()
            .expect("refusal progress")
            .id
            .index,
        1
    );
    assert!(
        machine
            .reader()
            .get(&keys::queue_counters(&namespace, &entity))?
            .is_none()
    );
    let update = CommittedCheckpointUpdate {
        stream,
        expected_previous: machine.checkpoint()?.last(),
        entry: CommittedEntryId {
            term: 1,
            node_id: 9,
            index: 2,
        },
    };
    let accepted = CommittedQueueWork::Queue(CommittedQueueCommand::send(
        namespace,
        entity,
        Timestamp::from_millis(3),
        CommittedSend {
            message_id: "ordinary".into(),
            body: vec![1],
            time_to_live_millis: None,
            session_id: None,
        },
    ));
    let CommittedApplyResult::Applied {
        application: CommittedApplication::Queue(application),
        ..
    } = machine.apply_committed(&update, &accepted)?
    else {
        panic!("ordinary committed ingress");
    };
    assert_eq!(
        application.outcome,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    let image = machine.export_create_send_image()?;
    let checked = ValidatedCreateSendLayout17Image::validate(DecodedCommittedImage::decode(
        image.as_bytes(),
    )?)?;
    assert_eq!((checked.queue_count(), checked.message_count()), (1, 1));
    Ok(())
}

macro_rules! suite {
    ($module:ident, $provider:expr, $committed:expr) => {
        mod $module {
            use super::*;
            #[test]
            fn ingress_metadata_uses_global_ready_order_after_restart() -> TestResult {
                super::ingress_metadata_uses_global_ready_order_after_restart($provider)
            }
            #[test]
            fn ordinary_ownership_is_strict_while_metadata_survives_message_lifecycle() -> TestResult {
                super::ordinary_ownership_is_strict_while_metadata_survives_message_lifecycle($provider)
            }
            #[test]
            fn mixed_scheduling_retains_metadata_without_creating_session_indexes() -> TestResult {
                super::mixed_scheduling_retains_metadata_without_creating_session_indexes($provider)
            }
            #[test]
            fn dead_letters_keep_existing_session_stripping_for_all_transitions() -> TestResult {
                super::dead_letters_keep_existing_session_stripping_for_all_transitions($provider)
            }
            #[test]
            fn metadata_is_charged_to_existing_message_and_batch_limits() -> TestResult {
                super::metadata_is_charged_to_existing_message_and_batch_limits($provider)
            }
            #[test]
            fn closed_committed_profile_keeps_plain_session_refusal() -> TestResult {
                $committed
            }
        }
    };
}

suite!(
    memory,
    testkit::MemoryProvider::new(),
    super::closed_committed_profile_keeps_plain_session_refusal(storage::MemoryReplicaStore::new())
);
suite!(durable, testkit::DurableProvider::temporary()?, {
    let directory = testkit::DurableProvider::temporary()?;
    super::closed_committed_profile_keeps_plain_session_refusal(storage::FjallReplicaStore::open(
        directory.path(),
    )?)
});
