//! Original-generation retirement uses real stored relations and one ordinary batch.
use domain::*;
use storage::{StateStore, WriteBatch};
use testkit::StoreProvider;

#[path = "session_generation_retirement/fixture.rs"]
mod fixture;
use fixture::*;

fn released_owned_generation_retires_before_message_deadline<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    let (hold, deliveries) = node.owned(&sid, 2, 100)?;
    node.at(
        10,
        CommandKind::SetSessionState {
            session: hold.clone(),
            state: vec![0, 255, 17],
        },
    )?;
    let original = deliveries
        .iter()
        .map(|d| node.record(&node.entity, d.sequence))
        .collect::<TestResult<Vec<_>>>()?;
    let counters = node
        .machine
        .store()
        .get(&keys::queue_counters(&node.namespace, &node.entity))?;
    node.release(&hold)?;
    let session = node.machine.session(&node.namespace, &node.entity, &sid)?;
    node.refuses(
        12,
        CommandKind::AcceptSession {
            session_id: Some(sid.clone()),
            lock_duration_millis: None,
        },
        BrokerError::SessionTakeoverPending {
            session_id: sid.clone(),
        },
    )?;
    node.reset();
    let outcome = node.retire(12, None)?;
    counts(&outcome, 2, 0, 0);
    assert_eq!(outcome.page, SessionRetirementPage::End);
    assert_eq!(node.reads().commits.len(), 1);
    for (delivery, record) in deliveries.iter().zip(&original) {
        node.assert_ready(delivery, record)?;
    }
    assert_eq!(node.summary(&sid)?, None);
    assert!(
        node.machine
            .store()
            .scan_prefix(
                &keys::session_message_lock_forward_prefix(&node.namespace, &node.entity, &sid),
                1
            )?
            .is_empty()
    );
    assert_eq!(
        node.machine.session(&node.namespace, &node.entity, &sid)?,
        session
    );
    assert_eq!(
        node.machine
            .store()
            .get(&keys::queue_counters(&node.namespace, &node.entity))?,
        counters
    );
    let snapshot = node.snapshot()?;
    node = node.restart()?;
    assert_eq!(node.snapshot()?, snapshot);
    let CommandOutcome::SessionAccepted(Some(replacement)) = node.at(
        13,
        CommandKind::AcceptSession {
            session_id: Some(sid),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("genuine grant after exits")
    };
    assert_ne!(replacement.lock.token, hold.token);
    assert_eq!(replacement.state, vec![0, 255, 17]);
    Ok(())
}

fn matching_expired_generation_can_retire_without_clearing_session_record<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    let (hold, deliveries) = node.owned(&sid, 1, 10)?;
    node.at(
        10,
        CommandKind::SetSessionState {
            session: hold.clone(),
            state: vec![9, 0, 8],
        },
    )?;
    let before = node.record(&node.entity, deliveries[0].sequence)?;
    let session_key = keys::session(&node.namespace, &node.entity, &sid);
    let session = node.machine.store().get(&session_key)?;
    let lock_key = keys::session_lock(
        &node.namespace,
        &node.entity,
        Timestamp::from_millis(20),
        &sid,
    );
    let index = node.machine.store().get(&lock_key)?;
    counts(&node.retire(20, None)?, 1, 0, 0);
    node.assert_ready(&deliveries[0], &before)?;
    assert_eq!(node.machine.store().get(&session_key)?, session);
    assert_eq!(node.machine.store().get(&lock_key)?, index);
    assert_eq!(
        node.at(20, CommandKind::ExpireSessionLocks)?,
        CommandOutcome::SessionLocksExpired { released: 1 }
    );
    assert_eq!(
        node.machine
            .session(&node.namespace, &node.entity, &sid)?
            .expect("persistent state")
            .state,
        vec![9, 0, 8]
    );
    Ok(())
}

fn live_original_generation_skips_without_clock_or_counter_mutation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    let (_hold, deliveries) = node.owned(&sid, 1, 1_000)?;
    let snapshot = node.snapshot()?;
    node.reset();
    let outcome = node.retire(11, None)?;
    counts(&outcome, 0, 0, 0);
    assert_eq!(outcome.page, SessionRetirementPage::End);
    assert_eq!(node.snapshot()?, snapshot);
    assert_eq!(
        node.machine.last_applied_time()?,
        Timestamp::from_millis(10)
    );
    assert!(node.reads().commits.is_empty());
    assert_eq!(
        node.at(
            11,
            renewal(
                &deliveries[0],
                deliveries[0].lock.expect("original token").token
            )
        )?,
        CommandOutcome::LockRenewed {
            locked_until: Timestamp::from_millis(111)
        }
    );
    let snapshot = node.snapshot()?;
    counts(&node.retire(12, None)?, 0, 0, 0);
    assert_eq!(node.snapshot()?, snapshot);
    Ok(())
}

fn trusted_unowned_rows_never_become_retirement_authority<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    let (hold, deliveries) = node.owned(&sid, 3, 100)?;
    node.at(10, defer(&deliveries[0]))?;
    let CommandOutcome::DeferredReceived(unowned) = node.at(
        10,
        CommandKind::ReceiveDeferred {
            sequences: vec![deliveries[0].sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(200_000),
            session_id: Some(sid.clone()),
        },
    )?
    else {
        panic!("actual legacy deferred receipt")
    };
    assert_eq!(node.row(unowned[0].sequence)?.4, Owner::TrustedUnowned);
    node.release(&hold)?;
    node.at(
        12,
        renewal(&unowned[0], unowned[0].lock.expect("unowned token").token),
    )?;
    let unowned_record = node.record(&node.entity, unowned[0].sequence)?;
    let unowned_row = node.row(unowned[0].sequence)?;
    node.refuses(
        12,
        renewal(
            &deliveries[1],
            deliveries[1].lock.expect("owned token").token,
        ),
        BrokerError::SessionLockNotHeld {
            session_id: sid.clone(),
        },
    )?;
    assert_eq!(
        node.at(12, complete(&deliveries[1]))?,
        CommandOutcome::Completed
    );
    counts(&node.retire(12, None)?, 1, 0, 0);
    assert_eq!(
        node.record(&node.entity, unowned[0].sequence)?,
        unowned_record
    );
    assert_eq!(node.row(unowned[0].sequence)?, unowned_row);
    assert_eq!(
        node.summary(&sid)?.expect("unowned barrier"),
        (
            node.namespace.clone(),
            node.entity.clone(),
            sid.clone(),
            None,
            0,
            1
        )
    );
    node.refuses(
        13,
        CommandKind::AcceptSession {
            session_id: Some(sid.clone()),
            lock_duration_millis: None,
        },
        BrokerError::SessionTakeoverPending { session_id: sid },
    )?;
    let before = node.snapshot()?;
    counts(&node.retire(13, None)?, 0, 0, 0);
    assert_eq!(node.snapshot()?, before);
    Ok(())
}

fn normal_and_held_deferred_receipts_preserve_original_generation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    let (hold, deliveries) = node.owned(&sid, 3, 100)?;
    node.at(10, defer(&deliveries[1]))?;
    node.at(10, defer(&deliveries[2]))?;
    let CommandOutcome::DeferredReceived(held) = node.at(
        10,
        CommandKind::ReceiveDeferredHeld {
            sequences: vec![deliveries[1].sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(200_000),
            session: Some(hold.clone()),
            budget: DeliveryBudget {
                max_bytes: u64::MAX,
                per_message_overhead_bytes: 0,
            },
        },
    )?
    else {
        panic!("original held deferred")
    };
    let CommandOutcome::DeferredReceived(unowned) = node.at(
        10,
        CommandKind::ReceiveDeferred {
            sequences: vec![deliveries[2].sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(200_000),
            session_id: Some(sid.clone()),
        },
    )?
    else {
        panic!("legacy ID-only deferred")
    };
    assert_eq!(
        node.row(held[0].sequence)?.4,
        Owner::HeldGeneration(hold.token)
    );
    assert_eq!(node.row(unowned[0].sequence)?.4, Owner::TrustedUnowned);
    let sibling = EntityPath::new("sibling")?;
    node.create(&sibling, required())?;
    node.send(&sibling, Some(sid.clone()), "sibling")?;
    let sibling_hold = node.accept(&sibling, &sid, 100)?;
    let sibling_delivery = node.receive(&sibling, Some(sibling_hold))?;
    let sibling_record = node.record(&sibling, sibling_delivery.sequence)?;
    let topic = EntityPath::new("topic")?;
    let name = SubscriptionName::new("sub")?;
    node.at_entity(
        &topic,
        10,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    node.at_entity(
        &topic,
        10,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig {
                requires_session: true,
                ..SubscriptionConfig::default()
            },
        },
    )?;
    node.send(&topic, Some(sid.clone()), "subscription")?;
    let subscription = topic.subscription(&name)?;
    let subscription_hold = node.accept(&subscription, &sid, 100)?;
    let subscription_delivery = node.receive(&subscription, Some(subscription_hold))?;
    let subscription_record = node.record(&subscription, subscription_delivery.sequence)?;
    let (foreign_namespace, foreign_delivery, foreign_record) = node.other_namespace_owned(&sid)?;
    node.release(&hold)?;
    counts(&node.retire(11, None)?, 2, 0, 0);
    assert_eq!(
        node.record(&sibling, sibling_delivery.sequence)?,
        sibling_record
    );
    assert_eq!(
        node.record(&subscription, subscription_delivery.sequence)?,
        subscription_record
    );
    assert_eq!(
        node.machine
            .message(&foreign_namespace, &node.entity, foreign_delivery.sequence)?,
        Some(foreign_record)
    );
    let shadow = node.entity.dead_letter_queue()?;
    assert_eq!(
        node.retire_entity(&shadow, 11, None)?.page,
        SessionRetirementPage::End
    );
    let ordinary = EntityPath::new("ordinary")?;
    node.create(&ordinary, QueueConfig::default())?;
    node.at_entity(
        &ordinary,
        11,
        CommandKind::Send {
            message_id: "metadata".to_owned(),
            body: vec![7],
            time_to_live_millis: None,
            session_id: Some(sid),
        },
    )?;
    let CommandOutcome::Received(Some(metadata)) = node.at_entity(
        &ordinary,
        11,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("ordinary metadata delivery")
    };
    node.at_entity(
        &ordinary,
        11,
        renewal(&metadata, metadata.lock.expect("ordinary token").token),
    )?;
    let before = node.snapshot()?;
    assert_eq!(
        node.retire_entity(&ordinary, 11, None)?.page,
        SessionRetirementPage::End
    );
    assert_eq!(node.snapshot()?, before);
    Ok(())
}

fn owned_generation_requires_existing_coherent_session_record<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    let (hold, _) = node.owned(&sid, 1, 10)?;
    let key = keys::session(&node.namespace, &node.entity, &sid);
    let original = node.snapshot()?;
    for (bytes, expected) in [
        (None, BrokerError::MalformedIndexKey),
        (Some(vec![]), BrokerError::Codec(CodecError::EmptyEnvelope)),
        (
            Some(vec![99]),
            BrokerError::Codec(CodecError::UnsupportedVersion { version: 99 }),
        ),
        (Some(vec![11, 255]), BrokerError::Codec(CodecError::Decode)),
        (
            Some(codec::encode(&SessionRecord {
                lock: Some(SessionLock {
                    token: LockToken::new(0),
                    locked_until: Timestamp::from_millis(20),
                }),
                state: vec![1],
            })?),
            BrokerError::MalformedIndexKey,
        ),
        (
            Some(codec::encode(&SessionRecord {
                lock: Some(SessionLock {
                    token: LockToken::new(hold.token.as_u64() + 1),
                    locked_until: Timestamp::from_millis(20),
                }),
                state: vec![1],
            })?),
            BrokerError::MalformedIndexKey,
        ),
    ] {
        node.restore(&original)?;
        let mut batch = WriteBatch::default();
        match bytes {
            Some(bytes) => batch.push_put(key.clone(), bytes),
            None => batch.push_delete(key.clone()),
        }
        node.raw(batch)?;
        node.refuses(20, retirement(None), expected)?;
    }
    for version in 1..=11 {
        node.restore(&original)?;
        let mut bytes = codec::encode(&SessionRecord {
            lock: Some(SessionLock {
                token: hold.token,
                locked_until: Timestamp::from_millis(21),
            }),
            state: vec![0, 255],
        })?;
        bytes[0] = version;
        node.raw(WriteBatch::default().put(key.clone(), bytes.clone()))?;
        let before = node.snapshot()?;
        counts(&node.retire(20, None)?, 0, 0, 0);
        assert_eq!(node.snapshot()?, before);
        assert_eq!(node.machine.store().get(&key)?, Some(bytes));
    }
    node.restore(&original)?;
    node.raw(WriteBatch::default().put(
        key.clone(),
        codec::encode(&SessionRecord {
            lock: None,
            state: vec![0, 255],
        })?,
    ))?;
    counts(&node.retire(20, None)?, 1, 0, 0);
    assert_eq!(
        node.machine
            .session(&node.namespace, &node.entity, &sid)?
            .expect("retained record")
            .state,
        vec![0, 255]
    );
    Ok(())
}

fn walked_forward_reverse_and_general_lock_relations_are_exact<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    let (hold, deliveries) = node.owned(&sid, 1, 10)?;
    node.release(&hold)?;
    let row = node.row(deliveries[0].sequence)?;
    let original = node.snapshot()?;
    let mut rows = Vec::new();
    let mut changed = row.clone();
    changed.0 = NamespaceName::new("foreign")?;
    rows.push(changed);
    let mut changed = row.clone();
    changed.1 = EntityPath::new("foreign")?;
    rows.push(changed);
    let mut changed = row.clone();
    changed.2 = SessionId::new("foreign")?;
    rows.push(changed);
    let mut changed = row.clone();
    changed.3 = SequenceNumber::new(999);
    rows.push(changed);
    let mut changed = row.clone();
    changed.4 = Owner::HeldGeneration(LockToken::new(0));
    rows.push(changed);
    let mut changed = row.clone();
    changed.5 = LockToken::new(0);
    rows.push(changed);
    let mut changed = row.clone();
    changed.5 = LockToken::new(row.5.as_u64() + 1);
    rows.push(changed);
    let mut changed = row.clone();
    changed.6 = Timestamp::from_millis(row.6.as_millis() + 1);
    rows.push(changed);
    for changed in rows {
        node.restore(&original)?;
        node.raw(WriteBatch::default().put(node.forward(&row), codec::encode(&changed)?))?;
        node.refuses(12, retirement(None), BrokerError::MalformedIndexKey)?;
    }
    let reverse = keys::session_message_lock_reverse(&node.namespace, &node.entity, row.3);
    let general = keys::lock(&node.namespace, &node.entity, row.6, row.3);
    for batch in [
        WriteBatch::default().delete(reverse),
        WriteBatch::default().delete(general.clone()),
        WriteBatch::default().put(general, vec![1]),
        WriteBatch::default().put(
            {
                let mut key = node.forward(&row);
                key.push(0);
                key
            },
            codec::encode(&row)?,
        ),
    ] {
        node.restore(&original)?;
        node.raw(batch)?;
        node.refuses(12, retirement(None), BrokerError::MalformedIndexKey)?;
    }
    node.restore(&original)?;
    node.raw(WriteBatch::default().delete(keys::message(&node.namespace, &node.entity, row.3)))?;
    node.refuses(
        12,
        retirement(None),
        BrokerError::DanglingIndexEntry { sequence: row.3 },
    )?;
    Ok(())
}

fn late_corruption_rolls_back_prior_exits_and_clock<P: StoreProvider>(provider: P) -> TestResult {
    let mut node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    let (hold, deliveries) = node.owned(&sid, 3, 10)?;
    node.release(&hold)?;
    let original = node.snapshot()?;
    let mut late = node.row(deliveries[2].sequence)?;
    late.5 = LockToken::new(0);
    node.raw(WriteBatch::default().put(
        keys::session_message_lock_reverse(&node.namespace, &node.entity, late.3),
        codec::encode(&late)?,
    ))?;
    node.refuses(20, retirement(None), BrokerError::MalformedIndexKey)?;
    assert!(node.reads().gets.iter().any(|key| key == &keys::message(&node.namespace, &node.entity, deliveries[0].sequence)));
    node.restore(&original)?;
    let mut record = node.record(&node.entity, deliveries[2].sequence)?;
    record.delivery_count = u32::MAX;
    record
        .envelope
        .as_mut()
        .expect("producer envelope")
        .application_properties
        .insert(
            "oversized".to_owned(),
            MessageValue::String("x".repeat(MAX_MESSAGE_PROPERTY_BYTES + 1)),
        );
    let expected = record
        .envelope
        .as_ref()
        .expect("envelope")
        .validate()
        .expect_err("real oversized property");
    node.put_record(&record)?;
    node.refuses(20, retirement(None), expected)?;
    let snapshot = node.snapshot()?;
    node = node.restart()?;
    assert_eq!(node.snapshot()?, snapshot);
    Ok(())
}

fn owned_row_pages_resume_intragroup_and_across_restart<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    for i in 0..65 {
        node.send_ttl(
            &node.entity,
            Some(sid.clone()),
            &format!("ttl-{i}"),
            Some(100),
        )?;
    }
    let hold = node.accept(&node.entity, &sid, 100)?;
    let mut deliveries = Vec::new();
    for _ in 0..65 {
        deliveries.push(node.receive(&node.entity, Some(hold.clone()))?);
    }
    node.release(&hold)?;
    node.reset();
    let first = node.retire(11, None)?;
    counts(&first, 32, 0, 0);
    let cursor = continuation(&first);
    assert_eq!(
        cursor.position,
        SessionRetirementPosition::OwnedRows {
            generation: hold.token,
            after_sequence: deliveries[31].sequence
        }
    );
    let prefix = keys::session_message_lock_generation_prefix(
        &node.namespace,
        &node.entity,
        &sid,
        hold.token,
    );
    let reads = node.reads();
    assert_eq!(
        reads
            .scans
            .iter()
            .filter(|(p, _, _, _)| p == &prefix)
            .count(),
        32
    );
    assert!(reads.scans.iter().all(|(_, _, limit, _)| *limit == 1));
    assert_eq!(node.summary(&sid)?.expect("staged counts").4, 33);
    let snapshot = node.snapshot()?;
    node = node.restart()?;
    assert_eq!(node.snapshot()?, snapshot);
    // The actual TTL command removes the already-retired cursor row; remaining Locked rows stay indexed.
    node.at(110, CommandKind::ExpireMessages)?;
    assert!(
        node.machine
            .message(&node.namespace, &node.entity, deliveries[31].sequence)?
            .is_none()
    );
    let second = node.retire(110, Some(cursor))?;
    counts(&second, 0, 0, 32);
    let cursor = continuation(&second);
    assert_eq!(
        cursor.position,
        SessionRetirementPosition::OwnedRows {
            generation: hold.token,
            after_sequence: deliveries[63].sequence
        }
    );
    node.reset();
    let last = node.retire(110, Some(cursor))?;
    counts(&last, 0, 0, 1);
    assert_eq!(last.page, SessionRetirementPage::End);
    assert_eq!(node.summary(&sid)?, None);
    assert!(node.machine.store().scan_prefix(&prefix, 1)?.is_empty());
    let maximum = keys::session_message_lock_forward(
        &node.namespace,
        &node.entity,
        &sid,
        Some(hold.token),
        SequenceNumber::new(u64::MAX),
    );
    let mut expected = maximum.clone();
    expected.push(0);
    assert_eq!(
        keys::session_message_lock_exclusive_start(&maximum),
        expected
    );
    assert_eq!(
        node.machine.last_applied_time()?,
        Timestamp::from_millis(110)
    );
    Ok(())
}

fn group_pages_skip_live_and_unowned_prefixes_without_fairness_claim<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let mut groups = Vec::new();
    for i in 0..65 {
        let sid = SessionId::new(format!("s{i:03}"))?;
        let (hold, delivery) = node.owned(&sid, 1, 1_000)?;
        groups.push((sid, hold, delivery[0].clone()));
    }
    for (sid, _, delivery) in &groups[32..64] {
        node.at(10, defer(delivery))?;
        node.at(
            10,
            CommandKind::ReceiveDeferred {
                sequences: vec![delivery.sequence],
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: Some(200_000),
                session_id: Some(sid.clone()),
            },
        )?;
    }
    node.release(&groups[64].1)?;
    let before = node.snapshot()?;
    node.reset();
    let first = node.retire(11, None)?;
    counts(&first, 0, 0, 0);
    let cursor = continuation(&first);
    assert_eq!(cursor.session_id, groups[31].0);
    assert_eq!(cursor.position, SessionRetirementPosition::AfterSession);
    let summary_prefix = keys::session_message_lock_summary_prefix(&node.namespace, &node.entity);
    assert_eq!(
        node.reads()
            .scans
            .iter()
            .filter(|(p, _, _, _)| p == &summary_prefix)
            .count(),
        32
    );
    assert_eq!(node.snapshot()?, before);
    assert!(node.reads().commits.is_empty());
    // A real trusted completion deletes the cursor group's summary between pages.
    node.at(12, complete(&groups[31].2))?;
    assert_eq!(node.summary(&groups[31].0)?, None);
    let second = node.retire(12, Some(cursor))?;
    counts(&second, 0, 0, 0);
    let cursor = continuation(&second);
    assert_eq!(cursor.session_id, groups[63].0);
    let last = node.retire(12, Some(cursor))?;
    counts(&last, 1, 0, 0);
    assert_eq!(last.page, SessionRetirementPage::End);
    assert_eq!(node.row(groups[32].2.sequence)?.4, Owner::TrustedUnowned);
    assert_eq!(
        node.record(&node.entity, groups[0].2.sequence)?.state,
        MessageState::Locked {
            token: groups[0].2.lock.expect("live original token").token,
            locked_until: Timestamp::from_millis(200_010)
        }
    );
    // A stale within-generation cursor never adopts a genuinely new generation.
    let CommandOutcome::SessionAccepted(Some(new)) = node.at(
        13,
        CommandKind::AcceptSession {
            session_id: Some(groups[64].0.clone()),
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("new original hold")
    };
    let CommandOutcome::Received(Some(_)) = node.at(
        13,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: Some(new.hold()),
        },
    )?
    else {
        panic!("new generation row")
    };
    let stale = SessionRetirementCursor {
        namespace: node.namespace.clone(),
        entity: node.entity.clone(),
        session_id: groups[64].0.clone(),
        position: SessionRetirementPosition::OwnedRows {
            generation: groups[64].1.token,
            after_sequence: groups[64].2.sequence,
        },
    };
    let snapshot = node.snapshot()?;
    counts(&node.retire(14, Some(stale))?, 0, 0, 0);
    assert_eq!(node.snapshot()?, snapshot);
    Ok(())
}

fn ttl_delivery_limit_and_dead_letter_transitions_reuse_existing_policy<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    for (id, ttl) in [
        ("ttl", Some(1)),
        ("count", None),
        ("ready-expiry", Some(90)),
        ("ready", None),
    ] {
        node.send_ttl(&node.entity, Some(sid.clone()), id, ttl)?;
    }
    let hold = node.accept(&node.entity, &sid, 100)?;
    let mut deliveries = Vec::new();
    for _ in 0..4 {
        deliveries.push(node.receive(&node.entity, Some(hold.clone()))?);
    }
    let mut ttl = node.record(&node.entity, deliveries[0].sequence)?;
    ttl.delivery_count = u32::MAX;
    node.put_record(&ttl)?;
    let mut count = node.record(&node.entity, deliveries[1].sequence)?;
    count.delivery_count = u32::MAX;
    node.put_record(&count)?;
    let ready = node.record(&node.entity, deliveries[2].sequence)?;
    let dropped = EntityPath::new("drop-ttl")?;
    node.create(&dropped, required())?;
    node.send_ttl(&dropped, Some(sid.clone()), "drop", Some(1))?;
    let drop_hold = node.accept(&dropped, &sid, 100)?;
    let drop_delivery = node.receive(&dropped, Some(drop_hold.clone()))?;
    let mut drop_record = node.record(&dropped, drop_delivery.sequence)?;
    drop_record.delivery_count = u32::MAX;
    node.raw(WriteBatch::default().put(
        keys::message(&node.namespace, &dropped, drop_record.sequence),
        codec::encode(&drop_record)?,
    ))?;
    node.at(
        10,
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                dead_lettering_on_message_expiration: Some(true),
                ..QueueConfigUpdate::default()
            },
        },
    )?;
    node.release(&hold)?;
    node.at_entity(
        &dropped,
        11,
        CommandKind::ReleaseSession { session: drop_hold },
    )?;
    node.reset();
    counts(&node.retire(11, None)?, 2, 2, 0);
    let shadow = node.entity.dead_letter_queue()?;
    for (delivery, reason) in [
        (&deliveries[0], DeadLetterReason::TimeToLiveExpired),
        (&deliveries[1], DeadLetterReason::MaxDeliveryCountExceeded),
    ] {
        let dlq = node.record(&shadow, delivery.sequence)?;
        assert_eq!(dlq.dead_letter.as_ref().expect("reason").reason, reason);
        assert_eq!(dlq.state, MessageState::Ready);
        assert_eq!(dlq.session_id, None);
        assert_eq!(dlq.expires_at, None);
        assert_eq!(dlq.delivery_count, u32::MAX);
    }
    assert_eq!(
        node.machine.store().get(&keys::expiry(
            &node.namespace,
            &node.entity,
            Timestamp::from_millis(100),
            ready.sequence
        ))?,
        Some(Vec::new())
    );
    assert_eq!(node.summary(&sid)?, None);
    counts(&node.retire_entity(&dropped, 11, None)?, 0, 0, 1);
    assert!(
        node.machine
            .message(&node.namespace, &dropped, drop_delivery.sequence)?
            .is_none()
    );
    assert!(
        node.machine
            .message(
                &node.namespace,
                &dropped.dead_letter_queue()?,
                drop_delivery.sequence
            )?
            .is_none()
    );
    Ok(())
}

fn read_and_mutation_caps_refuse_whole_selected_command<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    let (hold, _) = node.owned(&sid, 2, 1_000)?;
    let key = keys::session(&node.namespace, &node.entity, &sid);
    let original = node.snapshot()?;
    node.reset();
    counts(&node.retire(11, None)?, 0, 0, 0);
    let reads = node.reads();
    let session_reads = reads.values.iter().filter(|(k, _)| k == &key).count();
    assert!(session_reads > 0);
    let other = reads.scan_value_bytes
        + reads
            .values
            .iter()
            .filter(|(k, _)| k != &key && k.first() != Some(&0))
            .map(|(_, n)| *n)
            .sum::<usize>();
    let mut low = 0;
    let mut high = MAX_SESSION_RETIREMENT_READ_VALUE_BYTES;
    while low < high {
        let middle = (low + high).div_ceil(2);
        let record = SessionRecord {
            lock: Some(SessionLock {
                token: hold.token,
                locked_until: Timestamp::from_millis(1_010),
            }),
            state: vec![0; middle],
        };
        if other + session_reads * codec::encode(&record)?.len()
            <= MAX_SESSION_RETIREMENT_READ_VALUE_BYTES
        {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let mut record = SessionRecord {
        lock: Some(SessionLock {
            token: hold.token,
            locked_until: Timestamp::from_millis(1_010),
        }),
        state: vec![0; low],
    };
    node.at(
        10,
        CommandKind::SetSessionState {
            session: hold.clone(),
            state: record.state.clone(),
        },
    )?;
    let before = node.snapshot()?;
    counts(&node.retire(11, None)?, 0, 0, 0);
    assert_eq!(node.snapshot()?, before);
    record.state.push(0);
    node.at(
        10,
        CommandKind::SetSessionState {
            session: hold.clone(),
            state: record.state,
        },
    )?;
    node.refuses(
        11,
        retirement(None),
        BrokerError::SessionRetirementTooLarge {
            limit: SessionRetirementLimit::ReadValueBytes,
            maximum: MAX_SESSION_RETIREMENT_READ_VALUE_BYTES,
        },
    )?;
    node.restore(&original)?;
    let b = SessionId::new("B")?;
    let (other_hold, _) = node.owned(&b, 1, 1_000)?;
    node.at(
        10,
        CommandKind::SetSessionState {
            session: other_hold,
            state: vec![0; MAX_SESSION_RETIREMENT_READ_VALUE_BYTES],
        },
    )?;
    node.release(&hold)?;
    node.refuses(
        12,
        retirement(None),
        BrokerError::SessionRetirementTooLarge {
            limit: SessionRetirementLimit::ReadValueBytes,
            maximum: MAX_SESSION_RETIREMENT_READ_VALUE_BYTES,
        },
    )?;
    node.restore(&original)?;
    node.release(&hold)?;
    node.reset();
    counts(&node.retire(12, None)?, 2, 0, 0);
    let commits = node.reads().commits;
    assert_eq!(commits.len(), 1);
    let (entries, key_bytes, values) = batch_usage(&commits[0]);
    assert!(entries <= MAX_SESSION_RETIREMENT_MUTATION_ENTRIES);
    assert!(key_bytes <= MAX_SESSION_RETIREMENT_MUTATION_KEY_BYTES);
    assert!(values <= MAX_SESSION_RETIREMENT_MUTATION_VALUE_BYTES);
    let summary = keys::session_message_lock_summary(&node.namespace, &node.entity, &sid);
    assert!(commits[0].mutations().iter().filter(|m| matches!(m, storage::Mutation::Put { key, .. } | storage::Mutation::Delete { key } if key == &summary)).count() > 1);
    assert_eq!(
        commits[0]
            .mutations()
            .iter()
            .filter(|m| matches!(m, storage::Mutation::Put { key, .. } if key == &[0]))
            .count(),
        1
    );
    Ok(())
}

fn appended_retirement_preserves_old_command_and_closed_profile_shapes<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    assert_serialization(&node)?;
    let ordinary = EntityPath::new("atomic")?;
    node.create(&ordinary, QueueConfig::default())?;
    let binding = node
        .machine
        .bind_entity(
            &node.namespace,
            &ordinary,
            &ordinary,
            EntityIncarnationKind::Queue,
        )?
        .expect("original binding");
    let kind = retirement(None);
    let mut usage = AtomicMessagingInputUsage::default();
    usage.try_extend(&CommandKind::Complete {
        sequence: SequenceNumber::new(7),
        lock_token: LockToken::new(9),
    })?;
    let before_usage = usage;
    assert_eq!(
        usage.try_extend(&kind),
        Err(BrokerError::AtomicMessagingOperationNotSupported)
    );
    assert_eq!(usage, before_usage);
    assert_eq!(
        validate_atomic_messaging_kinds(std::slice::from_ref(&kind)),
        Err(BrokerError::AtomicMessagingOperationNotSupported)
    );
    let transaction = AtomicMessagingCommand {
        binding,
        issued_at: Timestamp::from_millis(11),
        commands: vec![node.command(&ordinary, 11, kind)],
    };
    let snapshot = node.snapshot()?;
    node.reset();
    assert_eq!(
        node.machine.apply_atomic_messaging(&transaction),
        Err(BrokerError::AtomicMessagingOperationNotSupported)
    );
    assert_eq!(node.snapshot()?, snapshot);
    assert_eq!(node.machine.last_applied_time()?, Timestamp::UNIX_EPOCH);
    assert!(node.reads().commits.is_empty());
    Ok(())
}

fn cursor_and_renewal_errors_preserve_authoritative_priority<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    let sid = SessionId::new("A")?;
    let (hold, deliveries) = node.owned(&sid, 1, 10)?;
    let base = SessionRetirementCursor {
        namespace: node.namespace.clone(),
        entity: node.entity.clone(),
        session_id: sid.clone(),
        position: SessionRetirementPosition::AfterSession,
    };
    let mut cursors = Vec::new();
    let mut invalid = base.clone();
    invalid.session_id = postcard::from_bytes(&postcard::to_stdvec(&"bad\0id")?)?;
    cursors.push(invalid);
    let mut invalid = base.clone();
    invalid.position = SessionRetirementPosition::OwnedRows {
        generation: LockToken::new(0),
        after_sequence: SequenceNumber::new(1),
    };
    cursors.push(invalid);
    let mut invalid = base.clone();
    invalid.position = SessionRetirementPosition::OwnedRows {
        generation: hold.token,
        after_sequence: SequenceNumber::new(0),
    };
    cursors.push(invalid);
    for invalid in cursors {
        node.refuses(
            11,
            retirement(Some(invalid.clone())),
            BrokerError::InvalidSessionCursor,
        )?;
        assert!(node.reads().scans.is_empty());
        node.refuses(
            9,
            retirement(Some(invalid)),
            BrokerError::ClockRegression {
                last_applied: Timestamp::from_millis(10),
                proposed: Timestamp::from_millis(9),
            },
        )?;
        assert!(node.reads().scans.is_empty());
    }
    let mut foreign = base.clone();
    foreign.entity = EntityPath::new("foreign")?;
    node.refuses(
        11,
        retirement(Some(foreign.clone())),
        BrokerError::SessionCursorScopeMismatch {
            namespace: node.namespace.clone(),
            entity: node.entity.clone(),
            cursor_namespace: foreign.namespace.clone(),
            cursor_entity: foreign.entity.clone(),
        },
    )?;
    let snapshot = node.snapshot()?;
    let missing = EntityPath::new("missing")?;
    assert_eq!(
        node.at_entity(&missing, 11, retirement(Some(foreign.clone()))),
        Err(BrokerError::QueueNotFound)
    );
    assert_eq!(node.snapshot()?, snapshot);
    let ordinary = EntityPath::new("ordinary")?;
    node.create(&ordinary, QueueConfig::default())?;
    assert_eq!(
        node.retire_entity(&ordinary, 11, Some(foreign.clone()))?
            .page,
        SessionRetirementPage::End
    );
    assert_eq!(
        node.retire_entity(&node.entity.dead_letter_queue()?, 11, Some(foreign))?
            .page,
        SessionRetirementPage::End
    );
    node.release(&hold)?;
    let token = deliveries[0].lock.expect("original message token").token;
    node.refuses(
        12,
        renewal(&deliveries[0], LockToken::new(token.as_u64() + 1)),
        BrokerError::LockTokenMismatch {
            sequence: deliveries[0].sequence,
        },
    )?;
    node.refuses(
        200_010,
        renewal(&deliveries[0], token),
        BrokerError::LockExpired {
            sequence: deliveries[0].sequence,
            locked_until: Timestamp::from_millis(200_010),
        },
    )?;
    node.refuses(
        12,
        CommandKind::RenewLockHeld {
            sequence: deliveries[0].sequence,
            lock_token: LockToken::new(0),
            session: Some(hold),
            lock_duration_millis: None,
        },
        BrokerError::SessionLockNotHeld { session_id: sid },
    )?;
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?) })+ }
    };
}
for_each_backend!(
    released_owned_generation_retires_before_message_deadline,
    matching_expired_generation_can_retire_without_clearing_session_record,
    live_original_generation_skips_without_clock_or_counter_mutation,
    trusted_unowned_rows_never_become_retirement_authority,
    normal_and_held_deferred_receipts_preserve_original_generation,
    owned_generation_requires_existing_coherent_session_record,
    walked_forward_reverse_and_general_lock_relations_are_exact,
    late_corruption_rolls_back_prior_exits_and_clock,
    owned_row_pages_resume_intragroup_and_across_restart,
    group_pages_skip_live_and_unowned_prefixes_without_fairness_claim,
    ttl_delivery_limit_and_dead_letter_transitions_reuse_existing_policy,
    read_and_mutation_caps_refuse_whole_selected_command,
    appended_retirement_preserves_old_command_and_closed_profile_shapes,
    cursor_and_renewal_errors_preserve_authoritative_priority,
);
