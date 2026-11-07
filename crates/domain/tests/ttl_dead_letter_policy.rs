//! `dead_lettering_on_message_expiration` on every expiry path, both backends.
//!
//! Disabled, an elapsed lifetime deletes the message and every index entry it
//! owns without writing a dead-letter copy. Enabled (the default), behavior and
//! stored profile bytes are exactly the legacy always-dead-letter ones.

use std::error::Error;

use domain::{
    Command, CommandKind, CommandOutcome, DeadLetterReason, EntityPath, NamespaceName, QueueConfig,
    QueueConfigUpdate, ReceiveMode, SequenceNumber, StateMachine, SubscriptionConfig,
    SubscriptionConfigUpdate, SubscriptionName, Timestamp, TopicConfig, codec, keys,
    snapshot_validation::{SnapshotCatalogError, validate_catalog, validate_complete_state},
};
use serde::Serialize;
use storage::{StateStore, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

const LOCK_MILLIS: u64 = 30_000;
const TTL_MILLIS: u64 = 100;

type Result<T = (), E = Box<dyn Error>> = std::result::Result<T, E>;

/// The pre-policy queue profile field order, frozen independently of the
/// public configuration and the production codec.
#[derive(Serialize)]
struct LegacyQueue {
    lock_duration_millis: u64,
    max_delivery_count: u32,
    default_time_to_live_millis: Option<u64>,
    max_message_bytes: usize,
    requires_session: bool,
    requires_duplicate_detection: bool,
    duplicate_detection_history_millis: u64,
}

fn legacy_bytes(config: QueueConfig) -> Vec<u8> {
    codec::encode(&LegacyQueue {
        lock_duration_millis: config.lock_duration_millis,
        max_delivery_count: config.max_delivery_count,
        default_time_to_live_millis: config.default_time_to_live_millis,
        max_message_bytes: config.max_message_bytes,
        requires_session: config.requires_session,
        requires_duplicate_detection: config.requires_duplicate_detection,
        duplicate_detection_history_millis: config.duplicate_detection_history_millis,
    })
    .unwrap()
}

fn config(dead_letter: bool) -> QueueConfig {
    QueueConfig {
        lock_duration_millis: LOCK_MILLIS,
        dead_lettering_on_message_expiration: dead_letter,
        ..QueueConfig::default()
    }
}

fn queue<P: StoreProvider>(provider: P, dead_letter: bool) -> Result<QueueFixture<P>> {
    Ok(QueueFixture::new(
        provider,
        "tenant",
        "orders",
        config(dead_letter),
    )?)
}

fn send_at<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    scheduled_enqueue_at: Option<u64>,
) -> Result<SequenceNumber> {
    match fixture.at(
        millis,
        CommandKind::Send {
            message_id: format!("m-{millis}"),
            body: b"perishable".to_vec(),
            time_to_live_millis: Some(TTL_MILLIS),
            session_id: None,
            scheduled_enqueue_at: scheduled_enqueue_at.map(Timestamp::from_millis),
            envelope: None,
        },
    )? {
        CommandOutcome::Sent { sequence } => Ok(sequence),
        other => panic!("expected a send, got {other:?}"),
    }
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
    millis: u64,
) -> Result<Option<domain::Delivery>> {
    match fixture.machine.apply(&Command::new(
        fixture.namespace.clone(),
        entity.clone(),
        Timestamp::from_millis(millis),
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    ))? {
        CommandOutcome::Received(delivery) => Ok(delivery),
        other => panic!("expected a receive, got {other:?}"),
    }
}

/// Every message, ready, lock, expiry, deferred and scheduled row of `entity`.
fn message_rows<S: StateStore>(
    machine: &StateMachine<S>,
    namespace: &NamespaceName,
    entity: &EntityPath,
) -> Result<usize> {
    let mut rows = 0;
    for prefix in [
        keys::message_prefix(namespace, entity),
        keys::ready_prefix(namespace, entity),
        keys::lock_prefix(namespace, entity),
        keys::expiry_prefix(namespace, entity),
        keys::deferred_prefix(namespace, entity),
        keys::scheduled_prefix(namespace, entity),
    ] {
        rows += machine.store().scan_prefix(&prefix, 16)?.len();
    }
    Ok(rows)
}

/// The expired message is gone from the entity and absent from its shadow, or
/// sits in the shadow as a TTL dead-letter, depending on the policy.
fn assert_expired<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    sequence: SequenceNumber,
    dead_letter: bool,
) -> Result {
    let shadow = fixture.entity.dead_letter_queue()?;
    assert_eq!(
        message_rows(&fixture.machine, &fixture.namespace, &fixture.entity)?,
        0,
        "the source keeps no record or index entry"
    );
    let copy =
        fixture
            .machine
            .dead_lettered_message(&fixture.namespace, &fixture.entity, sequence)?;
    if dead_letter {
        assert_eq!(
            copy.and_then(|record| record.dead_letter_info().map(|info| info.reason.clone())),
            Some(DeadLetterReason::TimeToLiveExpired)
        );
    } else {
        assert!(copy.is_none(), "no dead-letter copy is written");
        assert_eq!(
            message_rows(&fixture.machine, &fixture.namespace, &shadow)?,
            0,
            "the shadow stays empty"
        );
    }
    Ok(())
}

// ---- profiles --------------------------------------------------------------

fn profiles_keep_legacy_bytes_unless_the_policy_is_disabled<P: StoreProvider>(
    provider: P,
) -> Result {
    let fixture = queue(provider, true)?;
    let other = EntityPath::new("deleting")?;
    fixture.machine.apply(&Command::new(
        fixture.namespace.clone(),
        other.clone(),
        Timestamp::from_millis(1),
        CommandKind::CreateQueue {
            config: config(false),
        },
    ))?;
    let raw = |entity: &EntityPath| {
        fixture
            .machine
            .store()
            .get(&keys::queue_config(&fixture.namespace, entity))
            .unwrap()
            .unwrap()
    };
    let shadow_bytes = legacy_bytes(QueueConfig {
        max_delivery_count: u32::MAX,
        ..config(true)
    });
    assert_eq!(raw(&fixture.entity), legacy_bytes(config(true)));
    assert_eq!(raw(&fixture.entity.dead_letter_queue()?), shadow_bytes);
    assert_eq!(
        raw(&other).first(),
        Some(&codec::queue::VALUE_FORMAT_V2),
        "a disabled policy needs the new record version"
    );
    assert_eq!(codec::queue::decode(&raw(&other))?, config(false));
    assert_eq!(
        raw(&other.dead_letter_queue()?),
        shadow_bytes,
        "the shadow never expires messages, so it keeps the legacy shape"
    );
    assert_eq!(
        fixture.machine.queue_config(&fixture.namespace, &other)?,
        Some(config(false))
    );
    Ok(())
}

// ---- expiry paths ----------------------------------------------------------

fn the_expiry_sweep_applies_the_policy<P: StoreProvider>(provider: P, dead_letter: bool) -> Result {
    let fixture = queue(provider, dead_letter)?;
    let sequence = send_at(&fixture, 10, None)?;
    assert_eq!(
        fixture.at(10 + TTL_MILLIS, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: u32::from(dead_letter),
            deleted: u32::from(!dead_letter),
            index_entries_retired: 0,
        }
    );
    assert_expired(&fixture, sequence, dead_letter)?;
    Ok(())
}

fn a_receive_never_delivers_an_expired_message<P: StoreProvider>(
    provider: P,
    dead_letter: bool,
) -> Result {
    let fixture = queue(provider, dead_letter)?;
    let sequence = send_at(&fixture, 10, None)?;
    assert!(receive(&fixture, &fixture.entity, 10 + TTL_MILLIS)?.is_none());
    assert_expired(&fixture, sequence, dead_letter)?;
    Ok(())
}

fn abandon_after_the_lifetime_applies_the_policy<P: StoreProvider>(
    provider: P,
    dead_letter: bool,
) -> Result {
    // The lifetime elapsed under a live lock.
    let fixture = queue(provider, dead_letter)?;
    let sequence = send_at(&fixture, 10, None)?;
    let delivery = receive(&fixture, &fixture.entity, 20)?.expect("deliverable");
    assert_eq!(
        fixture.at(
            500,
            CommandKind::Abandon {
                sequence,
                lock_token: delivery.lock.expect("peek-lock").token,
                replacement_envelope: None,
            },
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: dead_letter
        }
    );
    assert_expired(&fixture, sequence, dead_letter)?;
    Ok(())
}

/// The lock lapses after the lifetime: TTL beats redelivery either way.
fn lock_expiry_after_the_lifetime_applies_the_policy<P: StoreProvider>(
    provider: P,
    dead_letter: bool,
) -> Result {
    let fixture = queue(provider, dead_letter)?;
    let sequence = send_at(&fixture, 10, None)?;
    receive(&fixture, &fixture.entity, 20)?.expect("deliverable");
    assert_eq!(
        fixture.at(20 + LOCK_MILLIS, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 0,
            dead_lettered: u32::from(dead_letter),
            deleted: u32::from(!dead_letter),
        }
    );
    assert_expired(&fixture, sequence, dead_letter)?;
    Ok(())
}

fn deferred_retrieval_after_the_lifetime_applies_the_policy<P: StoreProvider>(
    provider: P,
    dead_letter: bool,
) -> Result {
    let fixture = queue(provider, dead_letter)?;
    let sequence = send_at(&fixture, 10, None)?;
    let delivery = receive(&fixture, &fixture.entity, 20)?.expect("deliverable");
    fixture.at(
        30,
        CommandKind::Defer {
            sequence,
            lock_token: delivery.lock.expect("peek-lock").token,
            replacement_envelope: None,
        },
    )?;
    // The sweep leaves deferred messages for explicit retrieval.
    assert_eq!(
        fixture.at(10 + TTL_MILLIS, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            deleted: 0,
            index_entries_retired: 0,
        }
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequence)?
            .is_some()
    );
    assert_eq!(
        fixture.at(
            500,
            CommandKind::ReceiveDeferred {
                sequences: vec![sequence],
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
        )?,
        CommandOutcome::DeferredReceived(Vec::new())
    );
    assert_expired(&fixture, sequence, dead_letter)?;
    Ok(())
}

fn scheduled_messages_expire_after_activation_by_the_policy<P: StoreProvider>(
    provider: P,
    dead_letter: bool,
) -> Result {
    let fixture = queue(provider, dead_letter)?;
    send_at(&fixture, 10, Some(1_000))?;
    // Activated late; the lifetime runs from activation, then elapses.
    let CommandOutcome::ScheduledActivated { activated: 1, .. } =
        fixture.at(5_000, CommandKind::ActivateScheduled)?
    else {
        panic!("expected one activation");
    };
    let active = receive(&fixture, &fixture.entity, 5_000 + TTL_MILLIS - 1)?
        .expect("deliverable within its lifetime");
    fixture.at(
        5_000 + TTL_MILLIS,
        CommandKind::Abandon {
            sequence: active.sequence,
            lock_token: active.lock.expect("peek-lock").token,
            replacement_envelope: None,
        },
    )?;
    assert_expired(&fixture, active.sequence, dead_letter)?;

    // A second scheduled message reaped by the sweep after activation.
    send_at(&fixture, 6_000, Some(7_000))?;
    fixture.at(7_000, CommandKind::ActivateScheduled)?;
    assert_eq!(
        fixture.at(7_000 + TTL_MILLIS, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: u32::from(dead_letter),
            deleted: u32::from(!dead_letter),
            index_entries_retired: 0,
        }
    );
    assert_eq!(
        message_rows(&fixture.machine, &fixture.namespace, &fixture.entity)?,
        0
    );
    Ok(())
}

fn other_dead_letter_reasons_ignore_the_policy<P: StoreProvider>(provider: P) -> Result {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_delivery_count: 1,
            ..config(false)
        },
    )?;
    let first = send_at(&fixture, 10, None)?;
    let delivery = receive(&fixture, &fixture.entity, 20)?.expect("deliverable");
    assert_eq!(
        fixture.at(
            30,
            CommandKind::Abandon {
                sequence: first,
                lock_token: delivery.lock.expect("peek-lock").token,
                replacement_envelope: None,
            },
        )?,
        CommandOutcome::Abandoned {
            dead_lettered: true
        }
    );
    let second = send_at(&fixture, 40, None)?;
    let delivery = receive(&fixture, &fixture.entity, 50)?.expect("deliverable");
    fixture.at(
        60,
        CommandKind::DeadLetter {
            sequence: second,
            lock_token: delivery.lock.expect("peek-lock").token,
            reason: String::from("Poison"),
            description: String::new(),
            replacement_envelope: None,
        },
    )?;
    let reason = |sequence| {
        fixture
            .machine
            .dead_lettered_message(&fixture.namespace, &fixture.entity, sequence)
            .unwrap()
            .and_then(|record| record.dead_letter_info().map(|info| info.reason.clone()))
    };
    assert_eq!(
        reason(first),
        Some(DeadLetterReason::MaxDeliveryCountExceeded)
    );
    assert_eq!(
        reason(second),
        Some(DeadLetterReason::Application(String::from("Poison")))
    );

    // The terminal shadow keeps both copies however far time runs on.
    let shadow = fixture.entity.dead_letter_queue()?;
    fixture.machine.apply(&Command::new(
        fixture.namespace.clone(),
        shadow.clone(),
        Timestamp::from_millis(u64::MAX / 2),
        CommandKind::ExpireMessages,
    ))?;
    assert!(receive(&fixture, &shadow, u64::MAX / 2)?.is_some());
    assert!(receive(&fixture, &shadow, u64::MAX / 2)?.is_some());
    Ok(())
}

fn session_expiries_leave_no_session_rows_when_deleting<P: StoreProvider>(provider: P) -> Result {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            ..config(false)
        },
    )?;
    let session = |name: &str| domain::SessionId::new(name).unwrap();
    for (name, ttl) in [
        ("swept", TTL_MILLIS),
        ("received", 50),
        ("locked", TTL_MILLIS),
    ] {
        fixture.at(
            10,
            CommandKind::Send {
                message_id: name.into(),
                body: Vec::new(),
                time_to_live_millis: Some(ttl),
                session_id: Some(session(name)),
                scheduled_enqueue_at: None,
                envelope: None,
            },
        )?;
    }
    let accept = |millis, name: &str| match fixture.at(
        millis,
        CommandKind::AcceptSession {
            session_id: Some(session(name)),
            lock_duration_millis: Some(LOCK_MILLIS * 2),
        },
    ) {
        Ok(CommandOutcome::SessionAccepted(Some(accepted))) => accepted.hold(),
        other => panic!("expected a session, got {other:?}"),
    };
    let receive_in = |millis, hold: &domain::SessionHold| match fixture.at(
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: Some(hold.clone()),
        },
    ) {
        Ok(CommandOutcome::Received(delivery)) => delivery,
        other => panic!("expected a receive, got {other:?}"),
    };
    let received = accept(20, "received");
    let locked = accept(20, "locked");
    assert!(receive_in(30, &locked).is_some());
    // Session receive skips and deletes the elapsed message.
    assert!(receive_in(60, &received).is_none());
    assert_eq!(
        fixture.at(10 + TTL_MILLIS, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired {
            dead_lettered: 0,
            deleted: 1,
            index_entries_retired: 0,
        }
    );
    assert_eq!(
        fixture.at(30 + LOCK_MILLIS, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 0,
            dead_lettered: 0,
            deleted: 1,
        }
    );
    assert_eq!(
        message_rows(&fixture.machine, &fixture.namespace, &fixture.entity)?,
        0
    );
    assert!(
        fixture
            .machine
            .store()
            .scan_prefix(
                &keys::entity_session_ready_prefix(&fixture.namespace, &fixture.entity),
                16
            )?
            .is_empty(),
        "no session ready entry survives"
    );
    assert_eq!(
        message_rows(
            &fixture.machine,
            &fixture.namespace,
            &fixture.entity.dead_letter_queue()?
        )?,
        0
    );
    let image = fixture.machine.store().snapshot()?.entries().to_vec();
    validate_complete_state(&image).expect("the session image stays complete");
    Ok(())
}

// ---- subscriptions ---------------------------------------------------------

fn subscription_copies_follow_their_own_policy<P: StoreProvider>(provider: P) -> Result {
    let namespace = NamespaceName::new("tenant")?;
    let topic = EntityPath::new("events")?;
    let machine = StateMachine::new(provider.open()?);
    let at = |entity: &EntityPath, millis: u64, kind: CommandKind| {
        machine.apply(&Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(millis),
            kind,
        ))
    };
    at(
        &topic,
        0,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let mut children = Vec::new();
    for (name, dead_letter) in [("keep", true), ("drop", false)] {
        let name = SubscriptionName::new(name)?;
        at(
            &topic,
            1,
            CommandKind::CreateSubscription {
                name: name.clone(),
                config: SubscriptionConfig {
                    dead_lettering_on_message_expiration: dead_letter,
                    ..SubscriptionConfig::default()
                },
            },
        )?;
        children.push((topic.subscription(&name)?, dead_letter));
    }
    let CommandOutcome::Published { sequences, .. } = at(
        &topic,
        10,
        CommandKind::Send {
            message_id: String::from("m"),
            body: Vec::new(),
            time_to_live_millis: Some(TTL_MILLIS),
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
    )?
    else {
        panic!("expected a publish");
    };
    let sequence = sequences[0];
    for (child, dead_letter) in &children {
        let backing = machine
            .store()
            .get(&keys::queue_config(&namespace, child))?
            .unwrap();
        assert_eq!(
            backing.first() == Some(&codec::queue::VALUE_FORMAT_V2),
            !dead_letter
        );
        let shadow = machine
            .store()
            .get(&keys::queue_config(&namespace, &child.dead_letter_queue()?))?
            .unwrap();
        assert_eq!(shadow.first(), Some(&codec::VALUE_FORMAT_V1));
        assert_eq!(
            at(child, 10 + TTL_MILLIS, CommandKind::ExpireMessages)?,
            CommandOutcome::MessagesExpired {
                dead_lettered: u32::from(*dead_letter),
                deleted: u32::from(!*dead_letter),
                index_entries_retired: 0,
            }
        );
        assert_eq!(message_rows(&machine, &namespace, child)?, 0);
        assert_eq!(
            machine
                .dead_lettered_message(&namespace, child, sequence)?
                .is_some(),
            *dead_letter
        );
    }
    let [(keep, _), (drop_child, _)] = &children[..] else {
        unreachable!()
    };
    assert_eq!(
        machine
            .admin_subscription(&namespace, &topic, &SubscriptionName::new("drop")?)?
            .map(|subscription| subscription.config.dead_lettering_on_message_expiration),
        Some(false)
    );
    assert!(machine.queue_config(&namespace, keep)?.is_some());
    assert!(
        !machine
            .queue_config(&namespace, drop_child)?
            .expect("backing")
            .dead_lettering_on_message_expiration
    );

    // A snapshot image with new-format backings validates completely.
    let image = machine.store().snapshot()?.entries().to_vec();
    validate_complete_state(&image).expect("the new records validate");
    Ok(())
}

// ---- persistence and updates -----------------------------------------------

fn the_policy_survives_reopen_and_updates<P: StoreProvider>(provider: P) -> Result {
    let fixture = queue(provider, false)?.restart()?;
    assert_eq!(
        fixture
            .machine
            .queue_config(&fixture.namespace, &fixture.entity)?,
        Some(config(false))
    );
    let sequence = send_at(&fixture, 10, None)?;
    fixture.at(10 + TTL_MILLIS, CommandKind::ExpireMessages)?;
    assert_expired(&fixture, sequence, false)?;

    // Enabling returns the profile to its exact legacy bytes.
    assert_eq!(
        fixture.at(
            200,
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate {
                    dead_lettering_on_message_expiration: Some(true),
                    ..QueueConfigUpdate::default()
                },
            },
        )?,
        CommandOutcome::QueueUpdated
    );
    let fixture = fixture.restart()?;
    let key = keys::queue_config(&fixture.namespace, &fixture.entity);
    assert_eq!(
        fixture.machine.store().get(&key)?,
        Some(legacy_bytes(config(true)))
    );
    let sequence = send_at(&fixture, 300, None)?;
    fixture.at(300 + TTL_MILLIS, CommandKind::ExpireMessages)?;
    assert_expired(&fixture, sequence, true)?;

    // An identical update stages nothing; disabling again takes effect.
    for millis in [500, 600] {
        fixture.at(
            millis,
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate {
                    dead_lettering_on_message_expiration: Some(millis == 500),
                    ..QueueConfigUpdate::default()
                },
            },
        )?;
    }
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture
            .machine
            .store()
            .get(&key)?
            .as_deref()
            .map(|raw| raw[0]),
        Some(codec::queue::VALUE_FORMAT_V2)
    );
    let image = fixture.machine.store().snapshot()?.entries().to_vec();
    validate_complete_state(&image).expect("the updated records validate");
    Ok(())
}

fn subscription_updates_toggle_the_policy<P: StoreProvider>(provider: P) -> Result {
    let namespace = NamespaceName::new("tenant")?;
    let topic = EntityPath::new("events")?;
    let name = SubscriptionName::new("worker")?;
    let child = topic.subscription(&name)?;
    let machine = StateMachine::new(provider.open()?);
    let at = |millis: u64, kind: CommandKind| {
        machine.apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::from_millis(millis),
            kind,
        ))
    };
    at(
        0,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    at(
        1,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig::default(),
        },
    )?;
    let legacy = machine
        .store()
        .get(&keys::queue_config(&namespace, &child))?;
    for (millis, dead_letter) in [(2, false), (3, true)] {
        assert_eq!(
            at(
                millis,
                CommandKind::UpdateSubscription {
                    name: name.clone(),
                    update: SubscriptionConfigUpdate {
                        dead_lettering_on_message_expiration: Some(dead_letter),
                        ..SubscriptionConfigUpdate::default()
                    },
                },
            )?,
            CommandOutcome::SubscriptionUpdated
        );
        assert_eq!(
            machine
                .queue_config(&namespace, &child)?
                .map(|config| config.dead_lettering_on_message_expiration),
            Some(dead_letter)
        );
    }
    assert_eq!(
        machine
            .store()
            .get(&keys::queue_config(&namespace, &child))?,
        legacy,
        "re-enabling restores the exact legacy backing bytes"
    );
    // A topic update keeps the child's independent policy.
    at(
        4,
        CommandKind::UpdateSubscription {
            name: name.clone(),
            update: SubscriptionConfigUpdate {
                dead_lettering_on_message_expiration: Some(false),
                ..SubscriptionConfigUpdate::default()
            },
        },
    )?;
    at(
        5,
        CommandKind::UpdateTopic {
            update: domain::TopicConfigUpdate {
                max_message_bytes: Some(1024),
                ..domain::TopicConfigUpdate::default()
            },
        },
    )?;
    let config = machine.queue_config(&namespace, &child)?.expect("backing");
    assert_eq!(config.max_message_bytes, 1024);
    assert!(!config.dead_lettering_on_message_expiration);
    Ok(())
}

fn snapshot_validation_refuses_redundant_and_mismatched_records<P: StoreProvider>(
    provider: P,
) -> Result {
    let fixture = queue(provider, false)?;
    let key = keys::queue_config(&fixture.namespace, &fixture.entity);
    let image = fixture.machine.store().snapshot()?.entries().to_vec();
    validate_catalog(&image).expect("a disabled-policy queue validates");
    let row = image
        .iter()
        .position(|(candidate, _)| candidate == &key)
        .unwrap();

    // A V2 record that the legacy version represents is noncanonical.
    let mut redundant = image.clone();
    let mut bytes = image[row].1.clone();
    *bytes.last_mut().unwrap() = 1;
    redundant[row].1 = bytes;
    assert_eq!(
        validate_catalog(&redundant).map(drop),
        Err(SnapshotCatalogError::InvalidValue {
            row,
            detail: "catalog value is not canonical",
        })
    );
    let mut unknown = image.clone();
    unknown[row].1[0] = 3;
    assert_eq!(
        validate_catalog(&unknown).map(drop),
        Err(SnapshotCatalogError::InvalidValue {
            row,
            detail: "catalog value cannot be decoded",
        })
    );

    // A shadow that inherits a disabled policy is not the exact parent shadow.
    let shadow_key = keys::queue_config(&fixture.namespace, &fixture.entity.dead_letter_queue()?);
    let shadow_row = image
        .iter()
        .position(|(candidate, _)| candidate == &shadow_key)
        .unwrap();
    let mut inherited = image.clone();
    inherited[shadow_row].1 = codec::queue::encode(&QueueConfig {
        max_delivery_count: u32::MAX,
        ..config(false)
    })?;
    assert_eq!(
        validate_catalog(&inherited).map(drop),
        Err(SnapshotCatalogError::InconsistentCatalog {
            row: shadow_row,
            detail: "dead-letter profile is not the exact parent shadow",
        })
    );

    // The live machine refuses the same mismatched shadow before any update.
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().put(shadow_key.clone(), inherited[shadow_row].1.clone()))?;
    assert_eq!(
        fixture.at(
            10,
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate {
                    dead_lettering_on_message_expiration: Some(true),
                    ..QueueConfigUpdate::default()
                },
            },
        ),
        Err(domain::BrokerError::EntityMetadataCorrupt)
    );
    assert_eq!(
        fixture.machine.store().get(&key)?,
        Some(image[row].1.clone())
    );
    Ok(())
}

// ---- instantiation ---------------------------------------------------------

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory {
            $(
                #[test]
                fn $case() -> super::Result {
                    super::$case(::testkit::MemoryProvider::new())
                }
            )+
        }

        mod durable {
            $(
                #[test]
                fn $case() -> super::Result {
                    super::$case(::testkit::DurableProvider::temporary()?)
                }
            )+
        }
    };
}

/// Each case once per backend and policy.
macro_rules! for_each_backend_and_policy {
    ($($case:ident,)+) => {
        mod policy {
            $(
                mod $case {
                    #[test]
                    fn memory_dead_letters() -> crate::Result {
                        crate::$case(::testkit::MemoryProvider::new(), true)
                    }
                    #[test]
                    fn memory_deletes() -> crate::Result {
                        crate::$case(::testkit::MemoryProvider::new(), false)
                    }
                    #[test]
                    fn durable_dead_letters() -> crate::Result {
                        crate::$case(::testkit::DurableProvider::temporary()?, true)
                    }
                    #[test]
                    fn durable_deletes() -> crate::Result {
                        crate::$case(::testkit::DurableProvider::temporary()?, false)
                    }
                }
            )+
        }
    };
}

for_each_backend! {
    profiles_keep_legacy_bytes_unless_the_policy_is_disabled,
    other_dead_letter_reasons_ignore_the_policy,
    session_expiries_leave_no_session_rows_when_deleting,
    subscription_copies_follow_their_own_policy,
    the_policy_survives_reopen_and_updates,
    subscription_updates_toggle_the_policy,
    snapshot_validation_refuses_redundant_and_mismatched_records,
}

for_each_backend_and_policy! {
    the_expiry_sweep_applies_the_policy,
    a_receive_never_delivers_an_expired_message,
    abandon_after_the_lifetime_applies_the_policy,
    lock_expiry_after_the_lifetime_applies_the_policy,
    deferred_retrieval_after_the_lifetime_applies_the_policy,
    scheduled_messages_expire_after_activation_by_the_policy,
}
