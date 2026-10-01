use super::*;

fn queue_deletion_purges_every_live_state_but_not_counters_or_neighbors<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = observed_queue(
        provider,
        QueueConfig {
            requires_session: true,
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )?;
    let parent = fixture.entity.clone();
    let shadow = parent.dead_letter_queue()?;
    let session = SessionId::new("cart")?;
    for id in ["locked", "deferred", "dead", "ready"] {
        send(&fixture, 10, id, Some(&session))?;
    }
    schedule(&fixture, 10, "future", Some(&session))?;
    let hold = accept(&fixture, &parent, 11, &session)?;
    receive(&fixture, &parent, 12, Some(&hold))?;
    let deferred = receive(&fixture, &parent, 12, Some(&hold))?;
    at(
        &fixture,
        &parent,
        12,
        CommandKind::Defer {
            sequence: deferred.sequence,
            lock_token: deferred.lock.expect("lock").token,
        },
    )?;
    let dead = receive(&fixture, &parent, 12, Some(&hold))?;
    at(
        &fixture,
        &parent,
        12,
        CommandKind::DeadLetter {
            sequence: dead.sequence,
            lock_token: dead.lock.expect("lock").token,
            reason: "retained".into(),
            description: "dead letter".into(),
        },
    )?;
    receive(&fixture, &shadow, 12, None)?;
    at(
        &fixture,
        &parent,
        12,
        CommandKind::SetSessionState {
            session: hold,
            state: b"held state".to_vec(),
        },
    )?;
    let released = accept(&fixture, &parent, 13, &SessionId::new("released")?)?;
    at(
        &fixture,
        &parent,
        13,
        CommandKind::SetSessionState {
            session: released.clone(),
            state: b"released state".to_vec(),
        },
    )?;
    at(
        &fixture,
        &parent,
        13,
        CommandKind::ReleaseSession { session: released },
    )?;
    for path in ["orders-two", "Orders"] {
        at(
            &fixture,
            &EntityPath::new(path)?,
            14,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        )?;
    }
    let mut other_namespace = fixture.command(
        14,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    );
    other_namespace.namespace = NamespaceName::new("neighbor")?;
    fixture.machine.apply(&other_namespace)?;
    let parent_counter = counter_bytes(&fixture, &parent)?;
    let shadow_counter = counter_bytes(&fixture, &shadow)?;
    let before = fixture.machine.store().snapshot()?;
    reset(&fixture);
    delete(
        &fixture,
        20,
        DeleteEntityTarget::Queue,
        CommandOutcome::QueueDeleted,
        &[parent.clone(), shadow.clone()],
    )?;
    assert_exact_purge(
        &fixture,
        &before,
        &[parent.clone(), shadow.clone()],
        &[],
        &[],
        20,
    )?;
    assert_eq!(counter_bytes(&fixture, &parent)?, parent_counter);
    assert_eq!(counter_bytes(&fixture, &shadow)?, shadow_counter);
    assert_eq!(
        fixture.machine.queue_config(&fixture.namespace, &parent)?,
        None
    );
    assert_eq!(
        fixture.machine.queue_config(&fixture.namespace, &shadow)?,
        None
    );
    {
        let observations = fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations");
        assert_eq!(observations.commits, 1);
        assert_eq!(
            observations.puts,
            vec![
                keys::entity_incarnation(&fixture.namespace, &parent),
                keys::clock()
            ]
        );
        assert!(
            !observations
                .deleted
                .contains(&keys::queue_counters(&fixture.namespace, &parent))
        );
        assert!(
            !observations
                .deleted
                .contains(&keys::queue_counters(&fixture.namespace, &shadow))
        );
        assert!(observations.scans.iter().all(|(_, limit, _)| *limit == 1));
    }
    let after = fixture.machine.store().snapshot()?;
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, after);
    Ok(())
}

fn subscription_deletion_removes_only_its_runtime_rules_and_membership<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider)?;
    let alpha = subscribe(
        &fixture,
        "Alpha",
        SubscriptionConfig {
            requires_session: true,
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    let lowercase = subscribe(&fixture, "alpha", SubscriptionConfig::default(), 0)?;
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
    let session = SessionId::new("cart")?;
    for id in ["locked", "deferred", "dead"] {
        send(&fixture, 10, id, Some(&session))?;
    }
    let pending = schedule(&fixture, 10, "future", None)?;
    fixture.at(
        10,
        CommandKind::CreateRule {
            subscription: SubscriptionName::new("Alpha")?,
            name: RuleName::new("additional")?,
            filter: RuleFilter::False,
        },
    )?;
    let hold = accept(&fixture, &alpha, 11, &session)?;
    receive(&fixture, &alpha, 12, Some(&hold))?;
    let deferred = receive(&fixture, &alpha, 12, Some(&hold))?;
    at(
        &fixture,
        &alpha,
        12,
        CommandKind::Defer {
            sequence: deferred.sequence,
            lock_token: deferred.lock.expect("lock").token,
        },
    )?;
    let dead = receive(&fixture, &alpha, 12, Some(&hold))?;
    at(
        &fixture,
        &alpha,
        12,
        CommandKind::DeadLetter {
            sequence: dead.sequence,
            lock_token: dead.lock.expect("lock").token,
            reason: "retained".into(),
            description: "dead letter".into(),
        },
    )?;
    let shadow = alpha.dead_letter_queue()?;
    receive(&fixture, &shadow, 12, None)?;
    let name = SubscriptionName::new("Alpha")?;
    let before = fixture.machine.store().snapshot()?;
    let topic_counter = counter_bytes(&fixture, &fixture.entity)?;
    delete(
        &fixture,
        20,
        DeleteEntityTarget::Subscription { name: name.clone() },
        CommandOutcome::SubscriptionDeleted,
        &[alpha.clone(), shadow.clone()],
    )?;
    assert_exact_purge(
        &fixture,
        &before,
        &[alpha.clone(), shadow],
        &[keys::rule_prefix(
            &fixture.namespace,
            &fixture.entity,
            &name,
        )],
        &[keys::subscription(
            &fixture.namespace,
            &fixture.entity,
            &name,
        )],
        20,
    )?;
    assert_eq!(counter_bytes(&fixture, &fixture.entity)?, topic_counter);
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, pending)?
            .is_some()
    );
    assert_eq!(
        fixture
            .machine
            .subscription_config(&fixture.namespace, &fixture.entity, &name)?,
        None
    );
    assert_eq!(
        fixture
            .machine
            .subscriptions(&fixture.namespace, &fixture.entity)?
            .iter()
            .map(|sub| sub.name.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );
    let next = send(&fixture, 21, "next", None)?;
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &alpha, next)?
            .is_none()
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &lowercase, next)?
            .is_some()
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &beta, next)?
            .is_some()
    );
    Ok(())
}

fn topic_deletion_cascades_all_members_with_exact_sixty_five_effects<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider)?;
    let mut paths = vec![fixture.entity.clone()];
    for index in 0..MAX_TOPIC_SUBSCRIPTIONS {
        let name = format!("member{index:02}");
        let child = subscribe(
            &fixture,
            &name,
            SubscriptionConfig {
                requires_session: index == 0,
                ..SubscriptionConfig::default()
            },
            0,
        )?;
        fixture.at(
            0,
            CommandKind::CreateRule {
                subscription: SubscriptionName::new(&name)?,
                name: RuleName::new("additional")?,
                filter: RuleFilter::False,
            },
        )?;
        paths.extend([child.clone(), child.dead_letter_queue()?]);
    }
    send(&fixture, 10, "publication", None)?;
    schedule(&fixture, 11, "future", None)?;
    receive(&fixture, &paths[3], 12, None)?;
    receive(&fixture, &paths[2], 12, None)?;
    at(
        &fixture,
        &EntityPath::new("orders-two")?,
        13,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let mut neighbor = fixture.command(
        13,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    );
    neighbor.namespace = NamespaceName::new("neighbor")?;
    fixture.machine.apply(&neighbor)?;
    let before = fixture.machine.store().snapshot()?;
    let saved_counters = paths
        .iter()
        .map(|path| counter_bytes(&fixture, path))
        .collect::<TestResult<Vec<_>>>()?;
    assert_eq!(paths.len(), 65);
    delete(
        &fixture,
        20,
        DeleteEntityTarget::Auto,
        CommandOutcome::TopicDeleted,
        &paths,
    )?;
    assert_exact_purge(&fixture, &before, &paths, &[], &[], 20)?;
    for (path, bytes) in paths.iter().zip(saved_counters) {
        assert_eq!(counter_bytes(&fixture, path)?, bytes);
    }
    assert_eq!(
        fixture
            .machine
            .topic_config(&fixture.namespace, &fixture.entity)?,
        None
    );
    let after = fixture.machine.store().snapshot()?;
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, after);
    Ok(())
}

fn maximum_length_topic_deletion_keeps_source_fences_without_a_shadow<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = observed_queue(provider, QueueConfig::default())?;
    fixture.entity = EntityPath::new("t".repeat(domain::MAX_ENTITY_PATH_BYTES))?;
    assert!(fixture.entity.dead_letter_queue().is_err());
    reset(&fixture);
    for (target, error) in [
        (DeleteEntityTarget::Topic, BrokerError::TopicNotFound),
        (DeleteEntityTarget::Queue, BrokerError::QueueNotFound),
        (DeleteEntityTarget::Auto, BrokerError::QueueNotFound),
    ] {
        reject(
            &fixture,
            &fixture.entity,
            1,
            CommandKind::DeleteEntity { target },
            error,
        )?;
    }
    assert_eq!(
        fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations")
            .commits,
        0
    );
    for (index, target) in [DeleteEntityTarget::Topic, DeleteEntityTarget::Auto]
        .into_iter()
        .enumerate()
    {
        let base = 10 + index as u64 * 10;
        fixture.at(
            base,
            CommandKind::CreateTopic {
                config: TopicConfig {
                    requires_duplicate_detection: true,
                    ..TopicConfig::default()
                },
            },
        )?;
        let pending = schedule(&fixture, base + 1, "future", None)?;
        assert_eq!(pending.as_u64(), index as u64 + 1);
        let record = fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, pending)?
            .expect("retained scheduled source");
        assert!(matches!(
            record.state,
            domain::MessageState::Scheduled { enqueue_at, .. }
                if enqueue_at == Timestamp::from_millis(1_000)
        ));
        let source_counter = counter_bytes(&fixture, &fixture.entity)?;
        assert_eq!(
            counter(&fixture, &fixture.entity)?.next_sequence,
            pending.as_u64() + 1
        );
        let before = fixture.machine.store().snapshot()?;
        reset(&fixture);
        delete(
            &fixture,
            base + 2,
            target,
            CommandOutcome::TopicDeleted,
            std::slice::from_ref(&fixture.entity),
        )?;
        assert_exact_purge(
            &fixture,
            &before,
            std::slice::from_ref(&fixture.entity),
            &[],
            &[],
            base + 2,
        )?;
        assert_eq!(counter_bytes(&fixture, &fixture.entity)?, source_counter);
        assert_eq!(
            fixture
                .machine
                .topic_config(&fixture.namespace, &fixture.entity)?,
            None
        );
        assert_eq!(
            fixture
                .machine
                .message(&fixture.namespace, &fixture.entity, pending)?,
            None
        );
        {
            let observations = fixture
                .machine
                .store()
                .observations
                .lock()
                .expect("observations");
            assert_eq!(observations.commits, 1);
            assert_eq!(
                observations.puts,
                vec![
                    keys::entity_incarnation(&fixture.namespace, &fixture.entity),
                    keys::clock()
                ]
            );
            assert!(
                !observations
                    .deleted
                    .contains(&keys::queue_counters(&fixture.namespace, &fixture.entity,))
            );
        }
        let after = fixture.machine.store().snapshot()?;
        fixture = fixture.restart()?;
        assert_eq!(fixture.machine.store().snapshot()?, after);
    }
    reset(&fixture);
    for (target, error) in [
        (DeleteEntityTarget::Topic, BrokerError::TopicNotFound),
        (DeleteEntityTarget::Queue, BrokerError::QueueNotFound),
        (DeleteEntityTarget::Auto, BrokerError::QueueNotFound),
    ] {
        reject(
            &fixture,
            &fixture.entity,
            30,
            CommandKind::DeleteEntity { target },
            error,
        )?;
    }
    assert_eq!(
        fixture
            .machine
            .store()
            .observations
            .lock()
            .expect("observations")
            .commits,
        0
    );
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}
for_each_backend! {
    queue_deletion_purges_every_live_state_but_not_counters_or_neighbors,
    subscription_deletion_removes_only_its_runtime_rules_and_membership,
    topic_deletion_cascades_all_members_with_exact_sixty_five_effects,
    maximum_length_topic_deletion_keeps_source_fences_without_a_shadow,
}
