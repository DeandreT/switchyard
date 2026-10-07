//! Conservative session takeover barriers follow actual message-lock exits.

use std::sync::atomic::Ordering;

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, DeleteEntityTarget, EntityDeleteLimit,
    EntityPath, LockToken, MessageRecord, MessageState, NamespaceName, QueueConfig, ReceiveMode,
    SequenceNumber, SessionId, SessionPageOutcome, SettlementDisposition, SubscriptionConfig,
    SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use storage::{StateStore, WriteBatch};
use testkit::StoreProvider;

#[path = "session_lock_tracking/fixture.rs"]
mod fixture;

use fixture::{Node, Owner, TestResult, defer, named, renew, required, settle, unlimited};

fn original_held_receive_and_deferred_stamp_the_presented_generation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    let sequence = node.send(&node.entity, 1, "original", &sid, None)?;
    let hold = node.accept(&node.entity, 2, &sid)?;
    let delivery = node.receive(&node.entity, 3, Some(hold.clone()), 100)?;
    assert_eq!(delivery.sequence, sequence);
    node.assert_row(&node.entity, &delivery, Owner::HeldGeneration(hold.token))?;
    node.assert_counts(&node.entity, &sid, Some(hold.token), 1, 0)?;
    let snapshot = node.machine.store().snapshot()?;
    node = node.restart()?;
    assert_eq!(node.machine.store().snapshot()?, snapshot);
    node.at(4, defer(&delivery))?;
    node.assert_empty(&node.entity, &sid, sequence)?;
    let deferred = node
        .deferred(5, vec![sequence], Some(hold.clone()))?
        .remove(0);
    assert_ne!(
        deferred.lock.expect("new lock").token,
        delivery.lock.expect("old lock").token
    );
    node.assert_row(&node.entity, &deferred, Owner::HeldGeneration(hold.token))?;
    node.at(
        6,
        settle(
            &deferred,
            Some(hold.clone()),
            SettlementDisposition::Complete,
        ),
    )?;
    node.at(6, CommandKind::ReleaseSession { session: hold })?;
    node.assert_empty(&node.entity, &sid, sequence)?;

    let plain = EntityPath::new("ordinary")?;
    node.create(&plain, QueueConfig::default())?;
    node.send(&plain, 7, "metadata", &sid, None)?;
    let metadata = node.receive(&plain, 8, None, 100)?;
    assert_eq!(metadata.session_id, Some(sid.clone()));
    node.assert_empty(&plain, &sid, metadata.sequence)?;

    let deleted = node.send(&node.entity, 9, "receive-and-delete", &sid, None)?;
    let hold = node.accept(&node.entity, 9, &sid)?;
    assert!(matches!(node.at(10, CommandKind::Receive {
        mode: ReceiveMode::ReceiveAndDelete, lock_duration_millis: None, session: Some(hold),
    })?, CommandOutcome::Received(Some(delivery)) if delivery.sequence == deleted));
    node.assert_empty(&node.entity, &sid, deleted)?;

    let topic = EntityPath::new("events")?;
    let subscription = SubscriptionName::new("required")?;
    node.at_entity(
        &topic,
        11,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    node.at_entity(
        &topic,
        11,
        CommandKind::CreateSubscription {
            name: subscription.clone(),
            config: SubscriptionConfig {
                requires_session: true,
                ..SubscriptionConfig::default()
            },
        },
    )?;
    node.send(&topic, 12, "publication", &sid, None)?;
    let backing = topic.subscription(&subscription)?;
    let hold = node.accept(&backing, 13, &sid)?;
    let copy = node.receive(&backing, 14, Some(hold.clone()), 100)?;
    node.assert_row(&backing, &copy, Owner::HeldGeneration(hold.token))?;
    node.at_entity(
        &backing,
        15,
        settle(&copy, Some(hold), SettlementDisposition::Complete),
    )?;
    node.assert_empty(&backing, &sid, copy.sequence)?;
    Ok(())
}

fn legacy_deferred_remains_unowned_even_with_a_live_same_id_holder<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    let sequence = node.send(&node.entity, 1, "message", &sid, None)?;
    let original = node.accept(&node.entity, 2, &sid)?;
    let delivery = node.receive(&node.entity, 3, Some(original.clone()), 100)?;
    node.at(4, defer(&delivery))?;
    node.send(&node.entity, 4, "owned-sibling", &sid, None)?;
    let sibling = node.receive(&node.entity, 4, Some(original.clone()), 1_000)?;
    let CommandOutcome::DeferredReceived(mut deliveries) = node.at(
        5,
        CommandKind::ReceiveDeferred {
            sequences: vec![sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(100),
            session_id: Some(sid.clone()),
        },
    )?
    else {
        panic!("trusted ID-only deferred delivery")
    };
    let unowned = deliveries.remove(0);
    node.assert_row(&node.entity, &unowned, Owner::TrustedUnowned)?;
    node.assert_counts(&node.entity, &sid, Some(original.token), 1, 1)?;
    node.at(6, renew(&unowned, original.clone(), 200))?;
    let mut extended = unowned.clone();
    extended.lock.as_mut().expect("lock").locked_until = Timestamp::from_millis(206);
    node.assert_row(&node.entity, &extended, Owner::TrustedUnowned)?;
    node.at(
        7,
        settle(
            &extended,
            Some(original.clone()),
            SettlementDisposition::Defer,
        ),
    )?;
    assert_eq!(node.row(&node.entity, sequence)?, None);
    node.assert_counts(&node.entity, &sid, Some(original.token), 1, 0)?;
    let CommandOutcome::DeferredReceived(mut deliveries) = node.at(
        8,
        CommandKind::ReceiveDeferredBounded {
            sequences: vec![sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(100),
            session_id: Some(sid.clone()),
            budget: unlimited(),
        },
    )?
    else {
        panic!("bounded ID-only deferred delivery")
    };
    let unowned = deliveries.remove(0);
    node.assert_row(&node.entity, &unowned, Owner::TrustedUnowned)?;
    node.at(
        9,
        CommandKind::ReleaseSession {
            session: original.clone(),
        },
    )?;
    node.refuses(
        &node.entity,
        10,
        named(&sid),
        BrokerError::SessionTakeoverPending {
            session_id: sid.clone(),
        },
    )?;
    node.refuses(
        &node.entity,
        10,
        renew(&unowned, original.clone(), 100),
        BrokerError::SessionLockNotHeld {
            session_id: sid.clone(),
        },
    )?;
    node.at(
        10,
        CommandKind::RenewLock {
            sequence,
            lock_token: unowned.lock.expect("unowned lock").token,
            lock_duration_millis: Some(100),
        },
    )?;
    let mut extended = unowned;
    extended.lock.as_mut().expect("lock").locked_until = Timestamp::from_millis(110);
    node.assert_row(&node.entity, &extended, Owner::TrustedUnowned)?;
    node.refuses(
        &node.entity,
        111,
        named(&sid),
        BrokerError::SessionTakeoverPending {
            session_id: sid.clone(),
        },
    )?;
    // No deadline-only admission: a real sweep removes the expired unowned lock.
    assert_eq!(
        node.at(110, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 1,
            dead_lettered: 0,
            dropped: 0,
        }
    );
    assert_eq!(node.row(&node.entity, sequence)?, None);
    node.assert_counts(&node.entity, &sid, Some(original.token), 1, 0)?;
    node.refuses(
        &node.entity,
        111,
        named(&sid),
        BrokerError::SessionTakeoverPending {
            session_id: sid.clone(),
        },
    )?;
    assert_eq!(
        node.at(
            111,
            CommandKind::Complete {
                sequence: sibling.sequence,
                lock_token: sibling.lock.expect("owned sibling lock").token,
            }
        )?,
        CommandOutcome::Completed
    );
    node.assert_empty(&node.entity, &sid, sequence)?;
    let replacement = node.accept(&node.entity, 112, &sid)?;
    assert_ne!(replacement.token, original.token);
    let owned = node.receive(&node.entity, 113, Some(replacement.clone()), 100)?;
    node.assert_row(
        &node.entity,
        &owned,
        Owner::HeldGeneration(replacement.token),
    )?;
    node.at(
        114,
        settle(&owned, Some(replacement), SettlementDisposition::Complete),
    )?;
    node.assert_empty(&node.entity, &sid, sequence)?;

    let entity = EntityPath::new("unowned-only")?;
    node.create(&entity, required())?;
    let sequence = node.send(&entity, 115, "deferred", &sid, None)?;
    node.send(&entity, 115, "ready", &sid, None)?;
    let original = node.accept(&entity, 116, &sid)?;
    let delivery = node.receive(&entity, 117, Some(original.clone()), 100)?;
    node.at_entity(&entity, 118, defer(&delivery))?;
    node.at_entity(
        &entity,
        119,
        CommandKind::ReleaseSession {
            session: original.clone(),
        },
    )?;
    let CommandOutcome::DeferredReceived(mut deliveries) = node.at_entity(
        &entity,
        120,
        CommandKind::ReceiveDeferredBounded {
            sequences: vec![sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(5),
            session_id: Some(sid.clone()),
            budget: unlimited(),
        },
    )?
    else {
        panic!("actual unowned-only delivery")
    };
    let unowned = deliveries.remove(0);
    node.assert_row(&entity, &unowned, Owner::TrustedUnowned)?;
    node.assert_counts(&entity, &sid, None, 0, 1)?;
    node.refuses(
        &entity,
        121,
        named(&sid),
        BrokerError::SessionTakeoverPending {
            session_id: sid.clone(),
        },
    )?;
    for (kind, expected) in [
        (
            CommandKind::AcceptSession {
                session_id: None,
                lock_duration_millis: None,
            },
            CommandOutcome::SessionAccepted(None),
        ),
        (
            CommandKind::AcceptNextSessionPage {
                after: None,
                lock_duration_millis: None,
            },
            CommandOutcome::SessionPage(SessionPageOutcome::End),
        ),
    ] {
        let snapshot = node.machine.store().snapshot()?;
        let clock = node.machine.last_applied_time()?;
        node.reset();
        assert_eq!(node.at_entity(&entity, 121, kind)?, expected);
        assert_eq!(node.machine.store().snapshot()?, snapshot);
        assert_eq!(node.machine.last_applied_time()?, clock);
        assert_eq!(node.commits.load(Ordering::SeqCst), 0);
    }
    assert_eq!(
        node.at_entity(&entity, 125, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 1,
            dead_lettered: 0,
            dropped: 0
        }
    );
    node.assert_empty(&entity, &sid, sequence)?;
    assert_ne!(node.accept(&entity, 126, &sid)?.token, original.token);
    Ok(())
}

fn all_settlement_ttl_delivery_limit_and_expiry_exits_remove_tracking<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    for (index, branch) in [
        "complete",
        "defer",
        "abandon",
        "dead-letter",
        "ttl-drop",
        "ttl-dlq",
        "max-delivery",
        "expire-ready",
        "expire-drop",
        "expire-dlq",
        "expire-max",
        "ttl-repair",
    ]
    .into_iter()
    .enumerate()
    {
        let at = 100 + index as u64 * 100;
        let entity = EntityPath::new(format!("case-{index}"))?;
        let sid = SessionId::new("A")?;
        let ttl = branch.contains("ttl") || branch.contains("drop") || branch == "expire-dlq";
        let config = QueueConfig {
            max_delivery_count: if branch.contains("max") { 1 } else { 10 },
            dead_lettering_on_message_expiration: branch.ends_with("dlq"),
            ..required()
        };
        node.create(&entity, config)?;
        let sequence = node.send(&entity, at + 10, branch, &sid, ttl.then_some(1))?;
        let hold = node.accept(&entity, at + 10, &sid)?;
        let delivery = node.receive(&entity, at + 10, Some(hold.clone()), 5)?;
        node.assert_counts(&entity, &sid, Some(hold.token), 1, 0)?;
        if branch == "ttl-repair" {
            node.raw(WriteBatch::default().put(
                keys::expiry(
                    &node.namespace,
                    &entity,
                    Timestamp::from_millis(at + 11),
                    sequence,
                ),
                Vec::new(),
            ))?;
            assert_eq!(
                node.at_entity(&entity, at + 11, CommandKind::ExpireMessages)?,
                CommandOutcome::MessagesExpired {
                    dead_lettered: 0,
                    dropped: 0,
                    processed: 1
                }
            );
            node.assert_row(&entity, &delivery, Owner::HeldGeneration(hold.token))?;
        }
        if branch.starts_with("expire-") {
            let expected = if branch.ends_with("dlq") || branch.ends_with("max") {
                CommandOutcome::LocksExpired {
                    returned_to_ready: 0,
                    dead_lettered: 1,
                    dropped: 0,
                }
            } else if branch.ends_with("drop") {
                CommandOutcome::LocksExpired {
                    returned_to_ready: 0,
                    dead_lettered: 0,
                    dropped: 1,
                }
            } else {
                CommandOutcome::LocksExpired {
                    returned_to_ready: 1,
                    dead_lettered: 0,
                    dropped: 0,
                }
            };
            assert_eq!(
                node.at_entity(&entity, at + 15, CommandKind::ExpireLocks)?,
                expected
            );
        } else {
            let disposition = match branch {
                "complete" => SettlementDisposition::Complete,
                "defer" => SettlementDisposition::Defer,
                "dead-letter" => SettlementDisposition::DeadLetter {
                    reason: "reason".into(),
                    description: "description".into(),
                },
                _ => SettlementDisposition::Abandon,
            };
            node.at_entity(&entity, at + 11, settle(&delivery, Some(hold), disposition))?;
        }
        node.assert_empty(&entity, &sid, sequence)?;
        let record = node.machine.message(&node.namespace, &entity, sequence)?;
        if branch == "defer" {
            assert_eq!(
                record.expect("deferred record").state,
                MessageState::Deferred
            );
        } else if branch == "abandon" || branch == "expire-ready" {
            assert_eq!(record.expect("ready record").state, MessageState::Ready);
        } else {
            assert!(record.is_none());
        }
        if branch == "dead-letter" || branch.ends_with("dlq") || branch.contains("max") {
            let shadow = entity.dead_letter_queue()?;
            let dead = node.receive(&shadow, at + 16, None, 100)?;
            assert_eq!(dead.session_id, None);
            node.assert_empty(&shadow, &sid, sequence)?;
            node.at_entity(
                &shadow,
                at + 17,
                settle(&dead, None, SettlementDisposition::Complete),
            )?;
        }
    }
    Ok(())
}

fn multirow_deferred_and_expiry_counts_survive_restart<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    let mut sequences = Vec::new();
    for index in 0..260 {
        sequences.push(node.send(&node.entity, 1, &format!("message-{index}"), &sid, None)?);
    }
    let hold = node.accept(&node.entity, 2, &sid)?;
    for sequence in &sequences {
        let delivery = node.receive(&node.entity, 3, Some(hold.clone()), 5)?;
        assert_eq!(&delivery.sequence, sequence);
        node.at(3, defer(&delivery))?;
    }
    node.reset();
    let deliveries = node.deferred(3, sequences.clone(), Some(hold.clone()))?;
    assert_eq!(deliveries.len(), 260);
    assert_eq!(node.commits.load(Ordering::SeqCst), 1);
    node.assert_counts(&node.entity, &sid, Some(hold.token), 260, 0)?;
    for delivery in &deliveries {
        node.assert_row(&node.entity, delivery, Owner::HeldGeneration(hold.token))?;
    }
    let snapshot = node.machine.store().snapshot()?;
    node = node.restart()?;
    assert_eq!(node.machine.store().snapshot()?, snapshot);
    node.at(4, renew(&deliveries[0], hold.clone(), 10))?;
    node.at(
        4,
        CommandKind::RenewLock {
            sequence: deliveries[1].sequence,
            lock_token: deliveries[1].lock.expect("original lock").token,
            lock_duration_millis: Some(10),
        },
    )?;
    node.reset();
    assert_eq!(
        node.at(8, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 256,
            dead_lettered: 0,
            dropped: 0,
        }
    );
    assert_eq!(node.commits.load(Ordering::SeqCst), 1);
    node.assert_counts(&node.entity, &sid, Some(hold.token), 4, 0)?;
    let snapshot = node.machine.store().snapshot()?;
    node = node.restart()?;
    assert_eq!(node.machine.store().snapshot()?, snapshot);
    assert_eq!(
        node.at(8, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 2,
            dead_lettered: 0,
            dropped: 0,
        }
    );
    node.assert_counts(&node.entity, &sid, Some(hold.token), 2, 0)?;
    assert_eq!(
        node.at(14, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 2,
            dead_lettered: 0,
            dropped: 0,
        }
    );
    node.assert_counts(&node.entity, &sid, None, 0, 0)?;
    for sequence in &sequences {
        node.assert_empty(&node.entity, &sid, *sequence)?;
        assert_eq!(
            node.machine
                .message(&node.namespace, &node.entity, *sequence)?
                .expect("original message")
                .state,
            MessageState::Ready
        );
    }
    node.at(
        15,
        CommandKind::ReleaseSession {
            session: hold.clone(),
        },
    )?;
    assert_ne!(node.accept(&node.entity, 16, &sid)?.token, hold.token);
    Ok(())
}

fn all_acceptance_paths_block_only_sessions_with_outstanding_rows<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let mut busy = Vec::new();
    for index in 0..32 {
        let sid = SessionId::new(format!("S{index:03}"))?;
        node.send(&node.entity, 4, &format!("locked-{index}"), &sid, None)?;
        node.send(&node.entity, 4, &format!("ready-{index}"), &sid, None)?;
        let hold = node.accept(&node.entity, 4, &sid)?;
        let delivery = node.receive(&node.entity, 4, Some(hold.clone()), 5)?;
        node.at(
            4,
            CommandKind::ReleaseSession {
                session: hold.clone(),
            },
        )?;
        busy.push((sid, hold, delivery));
    }
    let healthy = SessionId::new("Z")?;
    node.send(&node.entity, 4, "healthy", &healthy, None)?;
    node.refuses(
        &node.entity,
        20,
        named(&busy[0].0),
        BrokerError::SessionTakeoverPending {
            session_id: busy[0].0.clone(),
        },
    )?;
    let snapshot = node.machine.store().snapshot()?;
    node.reset();
    assert_eq!(
        node.at(
            20,
            CommandKind::AcceptSession {
                session_id: None,
                lock_duration_millis: None,
            }
        )?,
        CommandOutcome::SessionAccepted(None)
    );
    assert_eq!(node.machine.store().snapshot()?, snapshot);
    assert_eq!(node.commits.load(Ordering::SeqCst), 0);
    assert_eq!(node.scans.lock().expect("scans").len(), 32);
    node.reset();
    let CommandOutcome::SessionPage(SessionPageOutcome::Continue(cursor)) = node.at(
        20,
        CommandKind::AcceptNextSessionPage {
            after: None,
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("full tracked page")
    };
    assert_eq!(cursor.session_id, busy[31].0);
    assert_eq!(node.machine.store().snapshot()?, snapshot);
    assert_eq!(node.commits.load(Ordering::SeqCst), 0);
    assert_eq!(node.scans.lock().expect("scans").len(), 32);
    node.reset();
    let CommandOutcome::SessionPage(SessionPageOutcome::Accepted(accepted)) = node.at(
        21,
        CommandKind::AcceptNextSessionPage {
            after: Some(cursor),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("healthy session grant")
    };
    assert_eq!(accepted.session_id, healthy);
    let probes = node.scans.lock().expect("scans").clone();
    assert_eq!(probes.len(), 2);
    assert_eq!(
        probes[1],
        (
            keys::session_message_lock_forward_prefix(&node.namespace, &node.entity, &healthy,),
            1
        )
    );
    let healthy_record = node
        .machine
        .session(&node.namespace, &node.entity, &healthy)?;
    // Deadlines have elapsed, but only committed expiry removes the busy-session barrier.
    node.at(22, CommandKind::ExpireLocks)?;
    for (sid, _, delivery) in &busy {
        node.assert_empty(&node.entity, sid, delivery.sequence)?;
    }
    let CommandOutcome::SessionAccepted(Some(legacy)) = node.at(
        23,
        CommandKind::AcceptSession {
            session_id: None,
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("legacy next now passes actual exit")
    };
    assert_eq!(legacy.session_id, busy[0].0);
    assert_eq!(
        node.machine
            .session(&node.namespace, &node.entity, &healthy)?,
        healthy_record
    );
    Ok(())
}

fn local_index_corruption_refuses_whole_commands_without_clock_or_apply<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    for (index, damage) in [
        "missing-reverse",
        "missing-forward",
        "namespace",
        "entity",
        "session",
        "sequence",
        "message-token",
        "deadline",
        "zero-token",
        "zero-generation",
        "summary-missing",
        "summary-zero",
        "summary-generation",
        "summary-overflow",
        "changed-forward",
        "record-sequence",
        "record-ready",
        "zero-actual-token",
        "stale-general-index",
        "late-expiry",
        "coherent-other-generation",
    ]
    .into_iter()
    .enumerate()
    {
        let entity = EntityPath::new(format!("corrupt-{index}"))?;
        node.create(&entity, required())?;
        let sid = SessionId::new("A")?;
        node.send(&entity, 10, "first", &sid, None)?;
        node.send(&entity, 10, "second", &sid, None)?;
        let hold = node.accept(&entity, 10, &sid)?;
        let first = node.receive(&entity, 10, Some(hold.clone()), 5)?;
        let second = node.receive(&entity, 10, Some(hold.clone()), 5)?;
        let reverse = keys::session_message_lock_reverse(&node.namespace, &entity, first.sequence);
        let forward = keys::session_message_lock_forward(
            &node.namespace,
            &entity,
            &sid,
            Some(hold.token),
            first.sequence,
        );
        let summary_key = keys::session_message_lock_summary(&node.namespace, &entity, &sid);
        let mut row = node.row(&entity, first.sequence)?.expect("original row");
        let mut summary = node.summary(&entity, &sid)?.expect("original summary");
        let mut mutation = WriteBatch::default();
        let mut expiration = false;
        let mut expected = BrokerError::MalformedIndexKey;
        match damage {
            "missing-reverse" => mutation.push_delete(reverse.clone()),
            "missing-forward" => mutation.push_delete(forward.clone()),
            "namespace" => row.0 = NamespaceName::new("other")?,
            "entity" => row.1 = EntityPath::new("other")?,
            "session" => row.2 = SessionId::new("other")?,
            "sequence" => row.3 = second.sequence,
            "message-token" => row.5 = LockToken::new(u64::MAX),
            "deadline" => row.6 = Timestamp::from_millis(16),
            "zero-token" => row.5 = LockToken::new(0),
            "zero-generation" => row.4 = Owner::HeldGeneration(LockToken::new(0)),
            "summary-missing" => mutation.push_delete(summary_key.clone()),
            "summary-zero" => {
                summary.3 = None;
                summary.4 = 0;
            }
            "summary-generation" => summary.3 = Some(LockToken::new(u64::MAX)),
            "summary-overflow" => {
                summary.4 = u64::MAX;
                summary.5 = 1;
            }
            "changed-forward" => {
                let mut changed = row.clone();
                changed.5 = LockToken::new(u64::MAX);
                mutation.push_put(forward.clone(), codec::encode(&changed)?);
            }
            "record-sequence" | "record-ready" | "zero-actual-token" => {
                let mut record = node
                    .machine
                    .message(&node.namespace, &entity, first.sequence)?
                    .expect("original stored message");
                match damage {
                    "record-sequence" => record.sequence = second.sequence,
                    "record-ready" => record.state = MessageState::Ready,
                    _ => {
                        record.state = MessageState::Locked {
                            token: LockToken::new(0),
                            locked_until: row.6,
                        };
                        row.5 = LockToken::new(0);
                        mutation.push_put(reverse.clone(), codec::encode(&row)?);
                        mutation.push_put(forward.clone(), codec::encode(&row)?);
                    }
                }
                mutation.push_put(
                    keys::message(&node.namespace, &entity, first.sequence),
                    codec::encode(&record)?,
                );
                expiration = true;
            }
            "stale-general-index" => {
                mutation.push_put(
                    keys::lock(
                        &node.namespace,
                        &entity,
                        Timestamp::from_millis(14),
                        first.sequence,
                    ),
                    Vec::new(),
                );
                expiration = true;
            }
            "late-expiry" => {
                mutation.push_delete(keys::session_message_lock_forward(
                    &node.namespace,
                    &entity,
                    &sid,
                    Some(hold.token),
                    second.sequence,
                ));
                expiration = true;
            }
            "coherent-other-generation" => {
                let other = LockToken::new(u64::MAX);
                row.4 = Owner::HeldGeneration(other);
                summary.3 = Some(other);
                mutation.push_delete(forward.clone());
                mutation.push_put(
                    keys::session_message_lock_forward(
                        &node.namespace,
                        &entity,
                        &sid,
                        Some(other),
                        first.sequence,
                    ),
                    codec::encode(&row)?,
                );
                expected = BrokerError::SessionLockNotHeld {
                    session_id: sid.clone(),
                };
            }
            _ => unreachable!(),
        }
        if matches!(
            damage,
            "namespace"
                | "entity"
                | "session"
                | "sequence"
                | "message-token"
                | "deadline"
                | "zero-token"
                | "zero-generation"
                | "coherent-other-generation"
        ) {
            mutation.push_put(reverse, codec::encode(&row)?);
        }
        if matches!(
            damage,
            "summary-zero"
                | "summary-generation"
                | "summary-overflow"
                | "coherent-other-generation"
        ) {
            mutation.push_put(summary_key, codec::encode(&summary)?);
        }
        node.raw(mutation)?;
        let command = if expiration {
            CommandKind::ExpireLocks
        } else {
            settle(&first, Some(hold), SettlementDisposition::Complete)
        };
        node.refuses(&entity, if expiration { 15 } else { 11 }, command, expected)?;
    }

    let entity = EntityPath::new("overflow")?;
    node.create(&entity, required())?;
    let sid = SessionId::new("A")?;
    node.send(&entity, 20, "locked", &sid, None)?;
    let hold = node.accept(&entity, 20, &sid)?;
    node.receive(&entity, 20, Some(hold.clone()), 100)?;
    let mut summary = node.summary(&entity, &sid)?.expect("summary");
    summary.4 = u64::MAX;
    node.raw(WriteBatch::default().put(
        keys::session_message_lock_summary(&node.namespace, &entity, &sid),
        codec::encode(&summary)?,
    ))?;
    node.send(&entity, 21, "ready", &sid, None)?;
    node.refuses(
        &entity,
        22,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(100),
            session: Some(hold.clone()),
        },
        BrokerError::MalformedIndexKey,
    )?;

    let entity = EntityPath::new("unexpected-ready-reverse")?;
    node.create(&entity, required())?;
    let sequence = node.send(&entity, 30, "ready", &sid, None)?;
    let hold = node.accept(&entity, 30, &sid)?;
    node.raw(WriteBatch::default().put(
        keys::session_message_lock_reverse(&node.namespace, &entity, sequence),
        vec![11, 0],
    ))?;
    node.refuses(
        &entity,
        31,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(100),
            session: Some(hold),
        },
        BrokerError::MalformedIndexKey,
    )?;

    let entity = EntityPath::new("orphan-tail")?;
    node.create(&entity, required())?;
    let sequence = node.send(&entity, 40, "original", &sid, None)?;
    let hold = node.accept(&entity, 40, &sid)?;
    let delivery = node.receive(&entity, 40, Some(hold.clone()), 100)?;
    node.raw(WriteBatch::default().put(
        keys::session_message_lock_forward(
            &node.namespace,
            &entity,
            &sid,
            Some(hold.token),
            SequenceNumber::new(u64::MAX),
        ),
        vec![11, 0],
    ))?;
    node.at_entity(
        &entity,
        41,
        settle(
            &delivery,
            Some(hold.clone()),
            SettlementDisposition::Complete,
        ),
    )?;
    assert_eq!(node.row(&entity, sequence)?, None);
    assert_eq!(node.summary(&entity, &sid)?, None);
    node.at_entity(&entity, 42, CommandKind::ReleaseSession { session: hold })?;
    node.refuses(&entity, 43, named(&sid), BrokerError::MalformedIndexKey)?;
    let probes = node.scans.lock().expect("scans").clone();
    assert_eq!(
        probes,
        vec![(
            keys::session_message_lock_forward_prefix(&node.namespace, &entity, &sid,),
            1
        )]
    );
    Ok(())
}

fn bounded_entity_deletion_erases_all_three_families_and_keeps_counter_tombstones<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let mut node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    let sequence = node.send(&node.entity, 1, "original", &sid, None)?;
    let hold = node.accept(&node.entity, 2, &sid)?;
    let delivery = node.receive(&node.entity, 3, Some(hold.clone()), 100)?;
    node.assert_row(&node.entity, &delivery, Owner::HeldGeneration(hold.token))?;
    let counter_key = keys::queue_counters(&node.namespace, &node.entity);
    let counters = node
        .machine
        .store()
        .get(&counter_key)?
        .expect("counter tombstone");
    assert_eq!(
        node.at(
            4,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Queue
            }
        )?,
        CommandOutcome::QueueDeleted
    );
    for prefix in [
        keys::message_prefix(&node.namespace, &node.entity),
        keys::ready_prefix(&node.namespace, &node.entity),
        keys::lock_prefix(&node.namespace, &node.entity),
        keys::expiry_prefix(&node.namespace, &node.entity),
        keys::entity_session_prefix(&node.namespace, &node.entity),
        keys::entity_session_ready_prefix(&node.namespace, &node.entity),
        keys::session_lock_prefix(&node.namespace, &node.entity),
        keys::scheduled_prefix(&node.namespace, &node.entity),
        keys::duplicate_history_prefix(&node.namespace, &node.entity),
        keys::duplicate_history_expiry_prefix(&node.namespace, &node.entity),
        keys::session_message_lock_reverse_prefix(&node.namespace, &node.entity),
        keys::session_message_lock_forward_prefix(&node.namespace, &node.entity, &sid),
        keys::session_message_lock_summary_prefix(&node.namespace, &node.entity),
    ] {
        assert!(node.machine.store().scan_prefix(&prefix, 1)?.is_empty());
    }
    assert!(
        node.machine
            .message(&node.namespace, &node.entity, sequence)?
            .is_none()
    );
    assert_eq!(
        node.machine.queue_config(&node.namespace, &node.entity)?,
        None
    );
    assert_eq!(
        node.machine.store().get(&counter_key)?,
        Some(counters.clone())
    );
    let snapshot = node.machine.store().snapshot()?;
    node = node.restart()?;
    assert_eq!(node.machine.store().snapshot()?, snapshot);
    node.at(5, CommandKind::CreateQueue { config: required() })?;
    let replacement = node.accept(&node.entity, 6, &sid)?;
    assert!(replacement.token.as_u64() > delivery.lock.expect("old message lock").token.as_u64());
    assert!(
        node.send(&node.entity, 7, "new incarnation", &sid, None)?
            .as_u64()
            > sequence.as_u64()
    );

    for family in [
        EntityDeleteLimit::Keys,
        EntityDeleteLimit::KeyBytes,
        EntityDeleteLimit::ValueBytes,
    ] {
        let entity = EntityPath::new(format!("budget-{family:?}"))?;
        node.create(&entity, required())?;
        let prefix = keys::session_message_lock_forward_prefix(&node.namespace, &entity, &sid);
        let mut batch = WriteBatch::default();
        let maximum = match family {
            EntityDeleteLimit::Keys => {
                for index in 0..=domain::MAX_ENTITY_DELETE_KEYS {
                    let mut key = prefix.clone();
                    key.extend_from_slice(&(index as u64).to_be_bytes());
                    batch.push_put(key, Vec::new());
                }
                domain::MAX_ENTITY_DELETE_KEYS
            }
            EntityDeleteLimit::KeyBytes => {
                for index in 0..32u64 {
                    let mut key = prefix.clone();
                    key.extend_from_slice(&index.to_be_bytes());
                    key.resize(40_000, b'x');
                    batch.push_put(key, Vec::new());
                }
                domain::MAX_ENTITY_DELETE_KEY_BYTES
            }
            EntityDeleteLimit::ValueBytes => {
                batch.push_put(prefix, vec![0; domain::MAX_ENTITY_DELETE_VALUE_BYTES + 1]);
                domain::MAX_ENTITY_DELETE_VALUE_BYTES
            }
        };
        node.raw(batch)?;
        node.refuses(
            &entity,
            8,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Queue,
            },
            BrokerError::EntityDeleteTooLarge {
                limit: family,
                maximum,
            },
        )?;
    }
    Ok(())
}

#[test]
fn tracking_preserves_command_record_and_value_version_bytes() -> TestResult {
    let sid = SessionId::new("A")?;
    let record = MessageRecord {
        sequence: SequenceNumber::new(7),
        message_id: "m".into(),
        body: vec![1],
        enqueued_at: Timestamp::from_millis(13),
        expires_at: None,
        delivery_count: 1,
        state: MessageState::Locked {
            token: LockToken::new(9),
            locked_until: Timestamp::from_millis(13),
        },
        session_id: Some(sid.clone()),
        dead_letter: None,
        scheduled_enqueue_time: None,
        envelope: None,
    };
    let bytes = vec![
        11, 7, 1, b'm', 1, 1, 13, 0, 1, 1, 9, 13, 1, 1, b'A', 0, 0, 0,
    ];
    assert_eq!(codec::encode(&record)?, bytes);
    assert_eq!(MessageRecord::decode(&bytes)?, record);
    let session = domain::SessionRecord {
        lock: Some(domain::SessionLock {
            token: LockToken::new(9),
            locked_until: Timestamp::from_millis(13),
        }),
        state: vec![1, 2],
    };
    assert_eq!(codec::encode(&session)?, vec![11, 1, 9, 13, 2, 1, 2]);
    for (command, bytes) in [
        (
            CommandKind::AcceptSession {
                session_id: None,
                lock_duration_millis: None,
            },
            vec![12, 0, 0],
        ),
        (
            CommandKind::RenewLock {
                sequence: SequenceNumber::new(7),
                lock_token: LockToken::new(9),
                lock_duration_millis: None,
            },
            vec![10, 7, 9, 0],
        ),
        (
            CommandKind::RenewLockHeld {
                sequence: SequenceNumber::new(7),
                lock_token: LockToken::new(9),
                session: None,
                lock_duration_millis: None,
            },
            vec![39, 7, 9, 0, 0],
        ),
        (
            CommandKind::AcceptNextSessionPage {
                after: None,
                lock_duration_millis: None,
            },
            vec![40, 0, 0],
        ),
    ] {
        assert_eq!(postcard::to_stdvec(&command)?, bytes);
        assert_eq!(postcard::from_bytes::<CommandKind>(&bytes)?, command);
    }
    assert_eq!(codec::ACTIVE_VALUE_FORMAT, 11);
    assert_eq!(storage::ACTIVE_STORE_FORMAT, 17);
    Ok(())
}

#[test]
fn tracking_does_not_widen_atomic_or_create_send_profiles() -> TestResult {
    let node = Node::new(testkit::MemoryProvider::new())?;
    let sid = SessionId::new("A")?;
    node.send(&node.entity, 1, "existing", &sid, None)?;
    let hold = node.accept(&node.entity, 2, &sid)?;
    let delivery = node.receive(&node.entity, 3, Some(hold.clone()), 100)?;
    let binding = node
        .machine
        .bind_entity(
            &node.namespace,
            &node.entity,
            &node.entity,
            domain::EntityIncarnationKind::Queue,
        )?
        .expect("binding");
    let mut usage = domain::AtomicMessagingInputUsage::default();
    let before_usage = usage;
    let held = settle(&delivery, Some(hold), SettlementDisposition::Complete);
    assert_eq!(
        usage.try_extend(&held),
        Err(BrokerError::AtomicMessagingOperationNotSupported)
    );
    assert_eq!(usage, before_usage);
    for kind in [
        held,
        CommandKind::Complete {
            sequence: delivery.sequence,
            lock_token: delivery.lock.expect("lock").token,
        },
    ] {
        let transaction = domain::AtomicMessagingCommand {
            binding: binding.clone(),
            issued_at: Timestamp::from_millis(4),
            commands: vec![Command::new(
                node.namespace.clone(),
                node.entity.clone(),
                Timestamp::from_millis(4),
                kind,
            )],
        };
        let snapshot = node.machine.store().snapshot()?;
        let clock = node.machine.last_applied_time()?;
        node.reset();
        assert_eq!(
            node.machine.apply_atomic_messaging(&transaction),
            Err(BrokerError::AtomicMessagingOperationNotSupported)
        );
        assert_eq!(node.machine.store().snapshot()?, snapshot);
        assert_eq!(node.machine.last_applied_time()?, clock);
        assert_eq!(node.commits.load(Ordering::SeqCst), 0);
    }
    let stream = domain::CommittedStreamId::new([7; 16])?;
    let committed =
        domain::CommittedStateMachine::create(storage::MemoryReplicaStore::new(), stream)?;
    let original = committed.reader().snapshot()?;
    let image = domain::EncodedCommittedImage::encode(
        domain::CommittedImageRole::CreateSendLayout17V1,
        stream,
        &original,
    )?;
    assert!(
        domain::ValidatedCreateSendLayout17Image::validate(domain::DecodedCommittedImage::decode(
            image.as_bytes()
        )?,)
        .is_ok()
    );
    for tag in [0x13, 0x14, 0x15] {
        let store = storage::MemoryStore::default();
        let mut batch = WriteBatch::default();
        for (key, value) in original.entries() {
            batch.push_put(key.clone(), value.clone());
        }
        batch.push_put(vec![tag], vec![11, 0]);
        store.apply(batch)?;
        let image = domain::EncodedCommittedImage::encode(
            domain::CommittedImageRole::CreateSendLayout17V1,
            stream,
            &store.snapshot()?,
        )?;
        assert!(matches!(
            domain::ValidatedCreateSendLayout17Image::validate(
                domain::DecodedCommittedImage::decode(image.as_bytes())?,
            ),
            Err(domain::CommittedImageValidationError::UnsupportedProfile)
        ));
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory { $(#[test] fn $case() -> super::TestResult {
            super::$case(testkit::MemoryProvider::new())
        })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult {
            super::$case(testkit::DurableProvider::temporary()?)
        })+ }
    };
}

for_each_backend!(
    original_held_receive_and_deferred_stamp_the_presented_generation,
    legacy_deferred_remains_unowned_even_with_a_live_same_id_holder,
    all_settlement_ttl_delivery_limit_and_expiry_exits_remove_tracking,
    multirow_deferred_and_expiry_counts_survive_restart,
    all_acceptance_paths_block_only_sessions_with_outstanding_rows,
    local_index_corruption_refuses_whole_commands_without_clock_or_apply,
    bounded_entity_deletion_erases_all_three_families_and_keeps_counter_tombstones,
);
