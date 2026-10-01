//! Immediate topic publication is one mutation with independently settled copies.

use std::{collections::BTreeMap, error::Error};

use domain::{
    AnnotationKey, BrokerError, Command, CommandApplication, CommandKind, CommandOutcome, Delivery,
    EntityPath, IngressBatchLimit, IngressEnvelope, MAX_INGRESS_BATCH_CONTENT_BYTES,
    MAX_INGRESS_BATCH_MESSAGES, MAX_SEQUENCE_NUMBER, MAX_TOPIC_FANOUT_CONTENT_BYTES,
    MAX_TOPIC_FANOUT_COPIES, MAX_TOPIC_FANOUT_VALUE_ITEMS, MessageBody, MessageEnvelope,
    MessageHeader, MessageIdentifier, MessageProperties, MessageState, MessageValue, NamespaceName,
    QueueCounterKind, QueueCounters, ReceiveMode, SequenceNumber, SubscriptionConfig,
    SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use storage::{StateStore, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn topic<P: StoreProvider>(provider: P, config: TopicConfig) -> TestResult<QueueFixture<P>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "anchor")?;
    fixture.entity = EntityPath::new("orders")?;
    fixture.at(0, CommandKind::CreateTopic { config })?;
    Ok(fixture)
}

fn subscribe<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    name: &str,
    config: SubscriptionConfig,
    millis: u64,
) -> TestResult<EntityPath> {
    let name = SubscriptionName::new(name)?;
    fixture.at(
        millis,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config,
        },
    )?;
    Ok(fixture.entity.subscription(&name)?)
}

fn member(id: &str) -> IngressEnvelope {
    IngressEnvelope {
        message_id: id.into(),
        body: b"payload".to_vec(),
        time_to_live_millis: None,
        session_id: None,
        scheduled_enqueue_time: None,
        envelope: MessageEnvelope {
            properties: MessageProperties {
                message_id: Some(MessageIdentifier::String(id.into())),
                ..MessageProperties::default()
            },
            body: MessageBody::Data(vec![b"payload".to_vec()]),
            ..MessageEnvelope::default()
        },
    }
}

fn legacy(id: &str) -> CommandKind {
    CommandKind::Send {
        message_id: id.into(),
        body: id.as_bytes().to_vec(),
        time_to_live_millis: None,
        session_id: None,
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

fn apply<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    kind: CommandKind,
) -> Result<CommandApplication, BrokerError> {
    fixture
        .machine
        .apply_with_effects(&fixture.command(millis, kind))
}

fn at<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
    millis: u64,
    kind: CommandKind,
) -> Result<CommandOutcome, BrokerError> {
    fixture.machine.apply(&Command::new(
        fixture.namespace.clone(),
        entity.clone(),
        Timestamp::from_millis(millis),
        kind,
    ))
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
    millis: u64,
    mode: ReceiveMode,
) -> TestResult<Option<Delivery>> {
    let CommandOutcome::Received(delivery) = at(
        fixture,
        entity,
        millis,
        CommandKind::Receive {
            mode,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("receive outcome")
    };
    Ok(delivery)
}

fn counters<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
) -> TestResult<Option<QueueCounters>> {
    let Some(bytes) = fixture
        .machine
        .store()
        .get(&keys::queue_counters(&fixture.namespace, entity))?
    else {
        return Ok(None);
    };
    Ok(Some(codec::decode(&bytes)?))
}

fn reject<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    kind: CommandKind,
    error: BrokerError,
) -> TestResult {
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(apply(fixture, millis, kind), Err(error));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn effects(application: &CommandApplication, targets: &[EntityPath]) {
    assert_eq!(application.subscription_enqueues.as_deref(), Some(targets));
    assert!(!application.dead_letters_enqueued);
}

fn shared_sequences_and_exact_rich_content_survive_restart_without_child_allocations<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
    let alpha = subscribe(&fixture, "alpha", SubscriptionConfig::default(), 0)?;
    let targets = [alpha.clone(), beta.clone()];
    let first = apply(&fixture, 1, legacy("legacy"))?;
    assert_eq!(
        first.outcome,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    effects(&first, &targets);
    let mut content = member("rich");
    content.envelope.header = Some(MessageHeader {
        durable: true,
        priority: 7,
        first_acquirer: true,
    });
    content.envelope.properties.subject = Some("own subject".into());
    content.envelope.properties.correlation_id = Some(MessageIdentifier::Ulong(19));
    content.envelope.application_properties = BTreeMap::from([
        ("empty".into(), MessageValue::Null),
        ("attempt".into(), MessageValue::Uint(2)),
    ]);
    content.envelope.message_annotations.insert(
        AnnotationKey::Symbol("private:trace".into()),
        MessageValue::Uuid([3; 16]),
    );
    content
        .envelope
        .footer
        .insert(AnnotationKey::Ulong(8), MessageValue::Binary(vec![9, 8]));
    let second = apply(&fixture, 1, rich(content.clone()))?;
    assert_eq!(
        second.outcome,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(2)
        }
    );
    effects(&second, &targets);
    let originals = [member("batch-one"), member("batch-two")];
    let batch = apply(
        &fixture,
        1,
        CommandKind::SendBatch {
            messages: originals.to_vec(),
        },
    )?;
    assert_eq!(
        batch.outcome,
        CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(3), SequenceNumber::new(4)]
        }
    );
    effects(&batch, &targets);
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("topic counter")
            .next_sequence,
        5
    );
    for target in &targets {
        assert_eq!(counters(&fixture, target)?, None);
        for (sequence, original) in [(2, &content), (3, &originals[0]), (4, &originals[1])] {
            let record = fixture
                .machine
                .message(&fixture.namespace, target, SequenceNumber::new(sequence))?
                .expect("subscription copy");
            assert_eq!(record.sequence, SequenceNumber::new(sequence));
            assert_eq!(record.envelope.as_deref(), Some(&original.envelope));
            assert_eq!(record.body, original.body);
            assert_eq!(record.state, MessageState::Ready);
        }
    }
    let before = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    for target in &targets {
        for sequence in 1..=4 {
            assert_eq!(
                receive(&fixture, target, 2, ReceiveMode::ReceiveAndDelete)?
                    .expect("FIFO copy")
                    .sequence,
                SequenceNumber::new(sequence)
            );
        }
        assert!(receive(&fixture, target, 2, ReceiveMode::ReceiveAndDelete)?.is_none());
        assert_eq!(counters(&fixture, target)?, None);
    }
    let mut ordinary = fixture.command(2, legacy("ordinary"));
    ordinary.entity = EntityPath::new("anchor")?;
    assert_eq!(
        fixture
            .machine
            .apply_with_effects(&ordinary)?
            .subscription_enqueues,
        None
    );
    Ok(())
}

fn empty_membership_and_late_subscriptions_use_topic_sequences_without_replay<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let first = apply(&fixture, 1, legacy("before"))?;
    effects(&first, &[]);
    assert_eq!(
        first.outcome,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    let alpha = subscribe(&fixture, "alpha", SubscriptionConfig::default(), 1)?;
    let duplicate = apply(&fixture, 2, legacy("before"))?;
    effects(&duplicate, &[]);
    assert_eq!(
        duplicate.outcome,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(2)
        }
    );
    assert!(receive(&fixture, &alpha, 2, ReceiveMode::ReceiveAndDelete)?.is_none());
    effects(
        &apply(&fixture, 2, legacy("after"))?,
        std::slice::from_ref(&alpha),
    );
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 2)?;
    effects(
        &apply(&fixture, 3, legacy("later"))?,
        &[alpha.clone(), beta.clone()],
    );
    assert_eq!(
        receive(&fixture, &alpha, 3, ReceiveMode::ReceiveAndDelete)?
            .expect("first later copy")
            .sequence,
        SequenceNumber::new(3)
    );
    assert_eq!(
        receive(&fixture, &alpha, 3, ReceiveMode::ReceiveAndDelete)?
            .expect("second later copy")
            .sequence,
        SequenceNumber::new(4)
    );
    assert_eq!(
        receive(&fixture, &beta, 3, ReceiveMode::ReceiveAndDelete)?
            .expect("new subscriber starts at current ingress")
            .sequence,
        SequenceNumber::new(4)
    );
    assert!(receive(&fixture, &beta, 3, ReceiveMode::ReceiveAndDelete)?.is_none());
    let before = fixture.machine.store().snapshot()?;
    let empty = apply(&fixture, 100, CommandKind::SendBatch { messages: vec![] })?;
    effects(&empty, &[]);
    assert_eq!(
        empty.outcome,
        CommandOutcome::BatchSent { sequences: vec![] }
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn topic_deduplication_is_once_per_ingress_and_never_creates_child_history<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let alpha = subscribe(&fixture, "alpha", SubscriptionConfig::default(), 0)?;
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
    apply(&fixture, 1, legacy("known"))?;
    let history_key = keys::duplicate_history(&fixture.namespace, &fixture.entity, "known");
    let deadline = fixture.machine.store().get(&history_key)?;
    let mixed = apply(
        &fixture,
        2,
        CommandKind::SendBatch {
            messages: vec![
                member("known"),
                member("new"),
                member("new"),
                member(""),
                member(""),
            ],
        },
    )?;
    assert_eq!(
        mixed.outcome,
        CommandOutcome::BatchSent {
            sequences: (2..=6).map(SequenceNumber::new).collect()
        }
    );
    effects(&mixed, &[alpha.clone(), beta.clone()]);
    assert_eq!(fixture.machine.store().get(&history_key)?, deadline);
    effects(&apply(&fixture, 3, rich(member("new")))?, &[]);
    for target in [&alpha, &beta] {
        assert!(
            fixture
                .machine
                .store()
                .scan_prefix(
                    &keys::duplicate_history_prefix(&fixture.namespace, target),
                    16
                )?
                .is_empty()
        );
        for sequence in [1, 3, 5, 6] {
            assert_eq!(
                receive(&fixture, target, 3, ReceiveMode::ReceiveAndDelete)?
                    .expect("nonduplicate copy")
                    .sequence,
                SequenceNumber::new(sequence)
            );
        }
        assert!(receive(&fixture, target, 3, ReceiveMode::ReceiveAndDelete)?.is_none());
    }
    let fixture = fixture.restart()?;
    effects(&apply(&fixture, 4, legacy("known"))?, &[]);
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("ingress counter")
            .next_sequence,
        9
    );
    Ok(())
}

fn ttl_and_settlement_mutate_only_the_selected_copy_and_its_shadow<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            default_time_to_live_millis: Some(50),
            ..TopicConfig::default()
        },
    )?;
    let alpha = subscribe(
        &fixture,
        "alpha",
        SubscriptionConfig {
            default_time_to_live_millis: Some(25),
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    let beta = subscribe(
        &fixture,
        "beta",
        SubscriptionConfig {
            dead_lettering_on_message_expiration: true,
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    let mut message = member("ttl");
    message.time_to_live_millis = Some(75);
    apply(&fixture, 10, rich(message))?;
    for (target, expires_at) in [(&alpha, 35), (&beta, 60)] {
        assert_eq!(
            fixture
                .machine
                .message(&fixture.namespace, target, SequenceNumber::new(1))?
                .expect("copy")
                .expires_at,
            Some(Timestamp::from_millis(expires_at))
        );
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::expiry(
                    &fixture.namespace,
                    target,
                    Timestamp::from_millis(expires_at),
                    SequenceNumber::new(1)
                ))?
                .is_some()
        );
    }
    let alpha_delivery = receive(&fixture, &alpha, 11, ReceiveMode::PeekLock)?.expect("alpha copy");
    at(
        &fixture,
        &alpha,
        12,
        CommandKind::Complete {
            sequence: alpha_delivery.sequence,
            lock_token: alpha_delivery.lock.expect("lock").token,
        },
    )?;
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &alpha, SequenceNumber::new(1))?
            .is_none()
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &beta, SequenceNumber::new(1))?
            .is_some()
    );
    let beta_delivery = receive(&fixture, &beta, 13, ReceiveMode::PeekLock)?.expect("beta copy");
    at(
        &fixture,
        &beta,
        14,
        CommandKind::Abandon {
            sequence: beta_delivery.sequence,
            lock_token: beta_delivery.lock.expect("lock").token,
        },
    )?;
    at(&fixture, &beta, 60, CommandKind::ExpireMessages)?;
    let dlq = beta.dead_letter_queue()?;
    let dead =
        receive(&fixture, &dlq, 60, ReceiveMode::ReceiveAndDelete)?.expect("beta expiry DLQ");
    assert_eq!(dead.sequence, SequenceNumber::new(1));
    assert_eq!(
        dead.dead_letter
            .as_ref()
            .expect("expiry reason")
            .reason
            .as_str(),
        "TTLExpiredException"
    );
    assert!(
        receive(
            &fixture,
            &alpha.dead_letter_queue()?,
            60,
            ReceiveMode::ReceiveAndDelete
        )?
        .is_none()
    );
    assert_eq!(
        counters(&fixture, &alpha)?
            .expect("lock counter")
            .next_sequence,
        1
    );
    assert_eq!(
        counters(&fixture, &beta)?
            .expect("lock counter")
            .next_sequence,
        1
    );
    Ok(())
}

fn namespaces_parent_prefixes_and_case_distinct_subscriptions_are_isolated<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let upper = subscribe(&fixture, "Alpha", SubscriptionConfig::default(), 0)?;
    let lower = subscribe(&fixture, "alpha", SubscriptionConfig::default(), 0)?;
    let mut other = fixture.command(
        0,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    );
    other.entity = EntityPath::new("orders-extra")?;
    fixture.machine.apply(&other)?;
    other.kind = CommandKind::CreateSubscription {
        name: SubscriptionName::new("alpha")?,
        config: SubscriptionConfig::default(),
    };
    fixture.machine.apply(&other)?;
    let foreign_parent = other.entity.clone();
    other.namespace = NamespaceName::new("tenant-extra")?;
    other.entity = fixture.entity.clone();
    other.kind = CommandKind::CreateTopic {
        config: TopicConfig::default(),
    };
    fixture.machine.apply(&other)?;
    other.kind = CommandKind::CreateSubscription {
        name: SubscriptionName::new("alpha")?,
        config: SubscriptionConfig::default(),
    };
    fixture.machine.apply(&other)?;
    effects(
        &apply(&fixture, 1, legacy("one"))?,
        &[upper.clone(), lower.clone()],
    );
    assert!(
        fixture
            .machine
            .message(
                &fixture.namespace,
                &foreign_parent.subscription(&SubscriptionName::new("alpha")?)?,
                SequenceNumber::new(1)
            )?
            .is_none()
    );
    assert!(
        fixture
            .machine
            .message(&other.namespace, &lower, SequenceNumber::new(1))?
            .is_none()
    );
    assert!(receive(&fixture, &upper, 2, ReceiveMode::ReceiveAndDelete)?.is_some());
    assert!(receive(&fixture, &lower, 2, ReceiveMode::ReceiveAndDelete)?.is_some());
    Ok(())
}

fn deferred_receive_and_lock_expiry_keep_shared_sequence_copies_independent<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let alpha = subscribe(
        &fixture,
        "alpha",
        SubscriptionConfig {
            lock_duration_millis: 10,
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    let beta = subscribe(
        &fixture,
        "beta",
        SubscriptionConfig {
            lock_duration_millis: 30,
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    apply(&fixture, 1, legacy("shared"))?;
    let deferred = receive(&fixture, &alpha, 2, ReceiveMode::PeekLock)?.expect("alpha lock");
    let beta_first = receive(&fixture, &beta, 3, ReceiveMode::PeekLock)?.expect("beta lock");
    at(
        &fixture,
        &alpha,
        4,
        CommandKind::Defer {
            sequence: deferred.sequence,
            lock_token: deferred.lock.expect("alpha token").token,
        },
    )?;
    assert!(receive(&fixture, &alpha, 4, ReceiveMode::ReceiveAndDelete)?.is_none());
    assert_eq!(
        at(&fixture, &beta, 33, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 1,
            dead_lettered: 0,
            dropped: 0
        }
    );
    let beta_second =
        receive(&fixture, &beta, 33, ReceiveMode::PeekLock)?.expect("beta expired copy returns");
    assert_eq!(beta_second.sequence, deferred.sequence);
    assert_ne!(
        beta_second.lock.expect("second beta token").token,
        beta_first.lock.expect("first beta token").token
    );
    let CommandOutcome::DeferredReceived(deliveries) = at(
        &fixture,
        &alpha,
        34,
        CommandKind::ReceiveDeferred {
            sequences: vec![deferred.sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session_id: None,
        },
    )?
    else {
        panic!("deferred receive")
    };
    assert_eq!(deliveries.len(), 1);
    let delivery = &deliveries[0];
    assert_eq!(delivery.sequence, beta_second.sequence);
    at(
        &fixture,
        &alpha,
        35,
        CommandKind::Complete {
            sequence: delivery.sequence,
            lock_token: delivery.lock.expect("deferred token").token,
        },
    )?;
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &alpha, delivery.sequence)?
            .is_none()
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &beta, beta_second.sequence)?
            .expect("beta unaffected")
            .state,
        MessageState::Locked {
            token: beta_second.lock.expect("beta token").token,
            locked_until: Timestamp::from_millis(63)
        }
    );
    Ok(())
}

#[test]
fn identical_publications_and_settlements_produce_identical_backend_bytes() -> TestResult {
    fn replay<P: StoreProvider>(provider: P) -> TestResult<storage::StoreSnapshot> {
        let fixture = topic(
            provider,
            TopicConfig {
                requires_duplicate_detection: true,
                ..TopicConfig::default()
            },
        )?;
        let alpha = subscribe(&fixture, "alpha", SubscriptionConfig::default(), 0)?;
        subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
        apply(
            &fixture,
            1,
            CommandKind::SendBatch {
                messages: vec![member("one"), member("one"), member("two")],
            },
        )?;
        let delivery =
            receive(&fixture, &alpha, 2, ReceiveMode::PeekLock)?.expect("replicated lock");
        at(
            &fixture,
            &alpha,
            3,
            CommandKind::Abandon {
                sequence: delivery.sequence,
                lock_token: delivery.lock.expect("replicated token").token,
            },
        )?;
        let before = fixture.machine.store().snapshot()?;
        let fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, before);
        Ok(before)
    }
    assert_eq!(
        replay(testkit::MemoryProvider::new())?,
        replay(testkit::DurableProvider::temporary()?)?
    );
    Ok(())
}

#[path = "topic_fanout/atomicity.rs"]
mod atomicity;
#[path = "topic_fanout/limits.rs"]
mod limits;

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    shared_sequences_and_exact_rich_content_survive_restart_without_child_allocations,
    empty_membership_and_late_subscriptions_use_topic_sequences_without_replay,
    topic_deduplication_is_once_per_ingress_and_never_creates_child_history,
    ttl_and_settlement_mutate_only_the_selected_copy_and_its_shadow,
    namespaces_parent_prefixes_and_case_distinct_subscriptions_are_isolated,
    deferred_receive_and_lock_expiry_keep_shared_sequence_copies_independent,
}
