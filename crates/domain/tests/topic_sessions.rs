//! Session routing is per subscription; topic publication remains one mutation.

use std::{collections::BTreeMap, error::Error};

use domain::{
    AcceptedSession, BrokerError, Command, CommandApplication, CommandKind, CommandOutcome,
    DeadLetterReason, Delivery, DeliveryBudget, EntityPath, IngressBatchLimit, IngressEnvelope,
    MAX_TOPIC_FANOUT_CONTENT_BYTES, MAX_TOPIC_FANOUT_COPIES, MAX_TOPIC_FANOUT_VALUE_ITEMS,
    MessageBody, MessageEnvelope, MessageIdentifier, MessageProperties, MessageValue,
    QueueCounters, ReceiveMode, SequenceNumber, SessionHold, SessionId, SettlementDisposition,
    SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use storage::StateStore;
use testkit::{QueueFixture, StoreProvider};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const SESSION_LOCK_MILLIS: u64 = 100;
const MISSING_SESSION_DESCRIPTION: &str =
    "Session enabled entity doesn't allow a message whose session identifier is null.";

fn topic<P: StoreProvider>(provider: P, duplicate_detection: bool) -> TestResult<QueueFixture<P>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "anchor")?;
    fixture.entity = EntityPath::new("orders")?;
    fixture.at(
        0,
        CommandKind::CreateTopic {
            config: TopicConfig {
                requires_duplicate_detection: duplicate_detection,
                ..TopicConfig::default()
            },
        },
    )?;
    Ok(fixture)
}

fn subscribe<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    name: &str,
    requires_session: bool,
) -> TestResult<EntityPath> {
    let name = SubscriptionName::new(name)?;
    fixture.at(
        0,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig {
                requires_session,
                lock_duration_millis: SESSION_LOCK_MILLIS,
                max_delivery_count: 3,
                ..SubscriptionConfig::default()
            },
        },
    )?;
    Ok(fixture.entity.subscription(&name)?)
}

fn member(id: &str, session: Option<&str>) -> IngressEnvelope {
    IngressEnvelope {
        message_id: id.into(),
        body: b"payload".to_vec(),
        time_to_live_millis: None,
        session_id: session.map(|value| SessionId::new(value).expect("session id")),
        scheduled_enqueue_time: None,
        envelope: MessageEnvelope {
            properties: MessageProperties {
                message_id: Some(MessageIdentifier::String(id.into())),
                subject: Some("mixed-session-copy".into()),
                ..MessageProperties::default()
            },
            application_properties: BTreeMap::from([(
                "original".into(),
                MessageValue::String("unchanged".into()),
            )]),
            body: MessageBody::Data(vec![b"payload".to_vec()]),
            ..MessageEnvelope::default()
        },
    }
}

fn legacy(id: &str, session: Option<&str>) -> CommandKind {
    CommandKind::Send {
        message_id: id.into(),
        body: id.as_bytes().to_vec(),
        time_to_live_millis: None,
        session_id: session.map(|value| SessionId::new(value).expect("session id")),
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

fn accept<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
    millis: u64,
    session: Option<&str>,
) -> TestResult<Option<AcceptedSession>> {
    let CommandOutcome::SessionAccepted(accepted) = at(
        fixture,
        entity,
        millis,
        CommandKind::AcceptSession {
            session_id: session.map(|value| SessionId::new(value).expect("session id")),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("session acceptance outcome")
    };
    Ok(accepted)
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
    millis: u64,
    session: Option<&SessionHold>,
) -> TestResult<Option<Delivery>> {
    let CommandOutcome::Received(delivery) = at(
        fixture,
        entity,
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: session.cloned(),
        },
    )?
    else {
        panic!("receive outcome")
    };
    Ok(delivery)
}

fn complete<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
    millis: u64,
    delivery: &Delivery,
) -> TestResult {
    assert_eq!(
        at(
            fixture,
            entity,
            millis,
            CommandKind::Complete {
                sequence: delivery.sequence,
                lock_token: delivery.lock.expect("delivery lock").token
            }
        )?,
        CommandOutcome::Completed
    );
    Ok(())
}

fn effects(application: &CommandApplication, mut targets: Vec<EntityPath>) {
    targets.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    targets.dedup();
    assert_eq!(application.subscription_enqueues, Some(targets));
}

fn counters<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
) -> TestResult<Option<QueueCounters>> {
    fixture
        .machine
        .store()
        .get(&keys::queue_counters(&fixture.namespace, entity))?
        .map(|bytes| codec::decode(&bytes))
        .transpose()
        .map_err(Into::into)
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

fn mixed_routes_preserve_content_and_use_their_own_ready_indexes<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, false)?;
    let plain = subscribe(&fixture, "plain", false)?;
    let session = subscribe(&fixture, "session", true)?;
    let shadow = session.dead_letter_queue()?;
    for (millis, command) in [
        (1, legacy("legacy", Some("cart"))),
        (2, rich(member("rich", Some("cart")))),
    ] {
        effects(
            &apply(&fixture, millis, command)?,
            vec![plain.clone(), session.clone()],
        );
    }
    let application = apply(
        &fixture,
        3,
        CommandKind::SendBatch {
            messages: vec![member("batch", Some("cart")), member("null", None)],
        },
    )?;
    assert_eq!(
        application.outcome,
        CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(3), SequenceNumber::new(4)]
        }
    );
    effects(
        &application,
        vec![plain.clone(), session.clone(), shadow.clone()],
    );
    let id = SessionId::new("cart")?;
    for sequence in 1..=4 {
        let sequence = SequenceNumber::new(sequence);
        let ordinary = fixture
            .machine
            .message(&fixture.namespace, &plain, sequence)?
            .expect("ordinary copy");
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::ready(&fixture.namespace, &plain, sequence))?
                .is_some()
        );
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::session_ready(
                    &fixture.namespace,
                    &plain,
                    &id,
                    sequence
                ))?
                .is_none()
        );
        assert_eq!(
            ordinary.session_id,
            if sequence.as_u64() < 4 {
                Some(id.clone())
            } else {
                None
            }
        );
        if sequence.as_u64() < 4 {
            let required = fixture
                .machine
                .message(&fixture.namespace, &session, sequence)?
                .expect("session copy");
            assert_eq!(ordinary.body, required.body);
            assert_eq!(ordinary.envelope, required.envelope);
            assert_eq!(ordinary.sequence, required.sequence);
            assert!(
                fixture
                    .machine
                    .store()
                    .get(&keys::session_ready(
                        &fixture.namespace,
                        &session,
                        &id,
                        sequence
                    ))?
                    .is_some()
            );
            assert!(
                fixture
                    .machine
                    .store()
                    .get(&keys::ready(&fixture.namespace, &session, sequence))?
                    .is_none()
            );
        }
    }
    for entity in [&plain, &session, &shadow] {
        assert_eq!(counters(&fixture, entity)?, None);
    }
    let delivery =
        receive(&fixture, &plain, 10, None)?.expect("ordinary receiver sees session property");
    assert_eq!(delivery.session_id, Some(id));
    assert_eq!(delivery.sequence, SequenceNumber::new(1));
    complete(&fixture, &plain, 11, &delivery)?;
    let accepted =
        accept(&fixture, &session, 12, Some("cart"))?.expect("required session accepted");
    let delivery =
        receive(&fixture, &session, 13, Some(&accepted.hold()))?.expect("session copy remains");
    assert_eq!(delivery.sequence, SequenceNumber::new(1));
    Ok(())
}

fn maximum_mixed_routes_report_all_sixty_four_destinations_exactly_once<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, false)?;
    let mut targets = Vec::new();
    let mut pairs = Vec::new();
    for index in 0..domain::MAX_TOPIC_SUBSCRIPTIONS {
        let backing = subscribe(&fixture, &format!("member{index:02}"), true)?;
        let shadow = backing.dead_letter_queue()?;
        targets.extend([backing.clone(), shadow.clone()]);
        pairs.push((backing, shadow));
    }
    let application = apply(
        &fixture,
        1,
        CommandKind::SendBatch {
            messages: vec![member("session", Some("A")), member("null", None)],
        },
    )?;
    assert_eq!(
        application.outcome,
        CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)],
        }
    );
    assert_eq!(
        application
            .subscription_enqueues
            .as_ref()
            .expect("destinations")
            .len(),
        64
    );
    effects(&application, targets);
    assert!(!application.dead_letters_enqueued);
    for (backing, shadow) in pairs {
        let active = fixture
            .machine
            .message(&fixture.namespace, &backing, SequenceNumber::new(1))?
            .expect("session copy");
        assert_eq!(active.sequence, SequenceNumber::new(1));
        assert_eq!(active.session_id, Some(SessionId::new("A")?));
        let dead = fixture
            .machine
            .message(&fixture.namespace, &shadow, SequenceNumber::new(2))?
            .expect("null copy");
        assert_eq!(dead.sequence, SequenceNumber::new(2));
        assert_eq!(
            dead.dead_letter.expect("typed reason").reason,
            DeadLetterReason::MissingSessionId
        );
        assert_eq!(
            fixture
                .machine
                .message(&fixture.namespace, &backing, SequenceNumber::new(2))?,
            None
        );
        assert_eq!(
            fixture
                .machine
                .message(&fixture.namespace, &shadow, SequenceNumber::new(1))?,
            None
        );
        assert_eq!(counters(&fixture, &backing)?, None);
        assert_eq!(counters(&fixture, &shadow)?, None);
    }
    for sequence in [1, 2] {
        assert_eq!(
            fixture.machine.message(
                &fixture.namespace,
                &fixture.entity.dead_letter_queue()?,
                SequenceNumber::new(sequence)
            )?,
            None
        );
    }
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("topic counter")
            .next_sequence,
        3
    );
    Ok(())
}

fn missing_ids_dead_letter_only_required_copies_and_strip_lifetime<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, false)?;
    let plain = subscribe(&fixture, "plain", false)?;
    let first = subscribe(&fixture, "first", true)?;
    let second = subscribe(&fixture, "second", true)?;
    let mut message = member("missing", None);
    message.time_to_live_millis = Some(50);
    let original = message.envelope.clone();
    let application = apply(&fixture, 1, rich(message))?;
    let shadows = [first.dead_letter_queue()?, second.dead_letter_queue()?];
    effects(
        &application,
        vec![plain.clone(), shadows[0].clone(), shadows[1].clone()],
    );
    let ordinary = fixture
        .machine
        .message(&fixture.namespace, &plain, SequenceNumber::new(1))?
        .expect("ordinary copy");
    assert_eq!(ordinary.expires_at, Some(Timestamp::from_millis(51)));
    for (target, shadow) in [(&first, &shadows[0]), (&second, &shadows[1])] {
        assert_eq!(
            fixture
                .machine
                .message(&fixture.namespace, target, SequenceNumber::new(1))?,
            None
        );
        let dead = fixture
            .machine
            .message(&fixture.namespace, shadow, SequenceNumber::new(1))?
            .expect("missing-ID copy");
        assert_eq!(dead.sequence, ordinary.sequence);
        assert_eq!(dead.body, ordinary.body);
        assert_eq!(dead.session_id, None);
        assert_eq!(dead.expires_at, None);
        assert_eq!(
            dead.dead_letter.as_ref().expect("reason").reason,
            DeadLetterReason::MissingSessionId
        );
        assert_eq!(
            dead.dead_letter.as_ref().expect("reason").reason.as_str(),
            "Session ID is null"
        );
        assert_eq!(
            dead.dead_letter.as_ref().expect("reason").description,
            MISSING_SESSION_DESCRIPTION
        );
        assert_eq!(
            dead.dead_letter.as_ref().expect("reason").dead_lettered_at,
            Timestamp::from_millis(1)
        );
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::ready(&fixture.namespace, shadow, dead.sequence))?
                .is_some()
        );
        let envelope = dead.envelope.as_ref().expect("projected envelope");
        assert_eq!(envelope.as_ref(), &original);
        assert_eq!(accept(&fixture, target, 2, None)?, None);
    }
    let dead = receive(&fixture, &shadows[0], 3, None)?.expect("DLQ receive without session");
    complete(&fixture, &shadows[0], 4, &dead)?;
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &shadows[1], SequenceNumber::new(1))?
            .is_some()
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &plain, SequenceNumber::new(1))?
            .is_some()
    );
    Ok(())
}

fn equal_session_ids_have_independent_holds_state_and_fifo<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, false)?;
    let plain = subscribe(&fixture, "plain", false)?;
    let first = subscribe(&fixture, "first", true)?;
    let second = subscribe(&fixture, "second", true)?;
    apply(
        &fixture,
        1,
        CommandKind::SendBatch {
            messages: vec![
                member("a1", Some("a")),
                member("b1", Some("b")),
                member("a2", Some("a")),
                member("b2", Some("b")),
            ],
        },
    )?;
    let a_first = accept(&fixture, &first, 10, Some("a"))?.expect("first a");
    let a_second = accept(&fixture, &second, 11, Some("a"))?.expect("second a independent");
    assert_eq!(
        at(
            &fixture,
            &first,
            12,
            CommandKind::AcceptSession {
                session_id: Some(SessionId::new("a")?),
                lock_duration_millis: None
            }
        ),
        Err(BrokerError::SessionAlreadyLocked {
            session_id: SessionId::new("a")?
        })
    );
    for (entity, accepted, state) in [
        (&first, &a_first, b"first-state".as_slice()),
        (&second, &a_second, b"second-state".as_slice()),
    ] {
        assert_eq!(
            at(
                &fixture,
                entity,
                13,
                CommandKind::SetSessionState {
                    session: accepted.hold(),
                    state: state.to_vec()
                }
            )?,
            CommandOutcome::SessionStateSet
        );
        assert_eq!(
            at(
                &fixture,
                entity,
                13,
                CommandKind::GetSessionState {
                    session: accepted.hold()
                }
            )?,
            CommandOutcome::SessionState(state.to_vec())
        );
    }
    for (millis, sequence) in [(15, 1), (17, 3)] {
        for (entity, accepted) in [(&first, &a_first), (&second, &a_second)] {
            let delivery =
                receive(&fixture, entity, millis, Some(&accepted.hold()))?.expect("a FIFO");
            assert_eq!(delivery.sequence, SequenceNumber::new(sequence));
            complete(&fixture, entity, millis, &delivery)?;
        }
    }
    let next = accept(&fixture, &first, 19, None)?.expect("next unlocked b");
    assert_eq!(next.session_id, SessionId::new("b")?);
    assert_eq!(
        receive(&fixture, &first, 20, Some(&next.hold()))?
            .expect("b1")
            .sequence,
        SequenceNumber::new(2)
    );
    assert_eq!(
        at(
            &fixture,
            &first,
            21,
            CommandKind::ReleaseSession {
                session: a_first.hold()
            }
        )?,
        CommandOutcome::SessionReleased
    );
    assert_eq!(
        at(
            &fixture,
            &second,
            22,
            CommandKind::GetSessionState {
                session: a_second.hold()
            }
        )?,
        CommandOutcome::SessionState(b"second-state".to_vec())
    );
    let replacement = accept(&fixture, &first, 23, Some("a"))?.expect("first a released");
    assert_eq!(replacement.state, b"first-state");
    for (millis, sequence) in [(24, 1), (26, 2), (28, 3), (30, 4)] {
        let delivery = receive(&fixture, &plain, millis, None)?.expect("ordinary global FIFO");
        assert_eq!(delivery.sequence, SequenceNumber::new(sequence));
        complete(&fixture, &plain, millis + 1, &delivery)?;
    }
    Ok(())
}

fn ordinary_copy_session_properties_survive_settlement_updates_and_requeue<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, false)?;
    let plain = subscribe(&fixture, "plain", false)?;
    let required = subscribe(&fixture, "required", true)?;
    apply(&fixture, 1, rich(member("content", Some("cart"))))?;
    let delivery = receive(&fixture, &plain, 2, None)?.expect("ordinary copy");
    let updates = BTreeMap::from([("stage".into(), MessageValue::String("abandoned".into()))]);
    assert_eq!(
        at(
            &fixture,
            &plain,
            3,
            CommandKind::Settle {
                sequence: delivery.sequence,
                lock_token: delivery.lock.expect("lock").token,
                disposition: SettlementDisposition::Abandon,
                properties_to_modify: updates
            }
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: false,
            dropped: false
        }
    );
    let delivery = receive(&fixture, &plain, 4, None)?.expect("global requeue");
    assert_eq!(delivery.session_id, Some(SessionId::new("cart")?));
    assert_eq!(
        delivery
            .envelope
            .as_ref()
            .expect("envelope")
            .application_properties
            .get("stage"),
        Some(&MessageValue::String("abandoned".into()))
    );
    assert_eq!(
        at(
            &fixture,
            &plain,
            5,
            CommandKind::Settle {
                sequence: delivery.sequence,
                lock_token: delivery.lock.expect("lock").token,
                disposition: SettlementDisposition::Defer,
                properties_to_modify: BTreeMap::from([(
                    "stage".into(),
                    MessageValue::String("deferred".into())
                )])
            }
        )?,
        CommandOutcome::Deferred
    );
    assert_eq!(receive(&fixture, &plain, 6, None)?, None);
    let CommandOutcome::DeferredReceived(mut deliveries) = at(
        &fixture,
        &plain,
        7,
        CommandKind::ReceiveDeferredHeld {
            sequences: vec![delivery.sequence],
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
        panic!("deferred outcome")
    };
    assert_eq!(deliveries.len(), 1);
    let delivery = deliveries.remove(0);
    assert_eq!(delivery.session_id, Some(SessionId::new("cart")?));
    complete(&fixture, &plain, 8, &delivery)?;
    let sibling = fixture
        .machine
        .message(&fixture.namespace, &required, SequenceNumber::new(1))?
        .expect("sibling unaffected");
    assert_eq!(
        sibling
            .envelope
            .as_ref()
            .expect("envelope")
            .application_properties
            .get("stage"),
        None
    );
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        at(
            &fixture,
            &EntityPath::new("anchor")?,
            9,
            legacy("strict", Some("cart"))
        ),
        Err(BrokerError::SessionNotSupported)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

#[path = "topic_sessions/atomicity.rs"]
mod atomicity;
#[path = "topic_sessions/budgets.rs"]
mod budgets;

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    mixed_routes_preserve_content_and_use_their_own_ready_indexes,
    maximum_mixed_routes_report_all_sixty_four_destinations_exactly_once,
    missing_ids_dead_letter_only_required_copies_and_strip_lifetime,
    equal_session_ids_have_independent_holds_state_and_fifo,
    ordinary_copy_session_properties_survive_settlement_updates_and_requeue,
}
