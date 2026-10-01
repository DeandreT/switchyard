use super::*;

fn admission_is_pure_exact_and_scoped_to_the_incarnation_owner<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider)?;
    let queue = fixture.entity.clone();
    let shadow = queue.dead_letter_queue()?;
    let topic = EntityPath::new("events")?;
    create_topic(&fixture, &topic, 1)?;
    let name = SubscriptionName::new("Alpha")?;
    let child = subscribe(&fixture, &topic, &name, 1)?;
    let child_shadow = child.dead_letter_queue()?;
    at(
        &fixture,
        &EntityPath::new("Orders")?,
        1,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let mut other = fixture.command(
        1,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    );
    other.namespace = NamespaceName::new("neighbor")?;
    fixture.machine.apply(&other)?;
    let before = fixture.machine.store().snapshot()?;
    let clock = fixture.machine.last_applied_time()?;
    reset(&fixture);
    for (target, owner, kind) in [
        (&queue, &queue, EntityIncarnationKind::Queue),
        (&shadow, &queue, EntityIncarnationKind::Queue),
        (&topic, &topic, EntityIncarnationKind::Topic),
        (&child, &child, EntityIncarnationKind::Subscription),
        (&child_shadow, &child, EntityIncarnationKind::Subscription),
    ] {
        let binding = bind(&fixture, target, owner, kind)?;
        assert_eq!(binding.namespace(), &fixture.namespace);
        assert_eq!(binding.target(), target);
        assert_eq!(binding.owner(), owner);
        assert_eq!(binding.kind(), kind);
        assert_eq!(binding.generation(), 1);
        let stored = incarnation(&fixture, owner)?;
        assert_eq!(stored.generation(), 1);
        assert_eq!(stored.kind(), kind);
        assert!(!stored.is_retired());
    }
    for shadow in [&shadow, &child_shadow] {
        assert_eq!(
            fixture
                .machine
                .store()
                .get(&keys::entity_incarnation(&fixture.namespace, shadow))?,
            None,
        );
    }
    let upper = bind(
        &fixture,
        &EntityPath::new("Orders")?,
        &EntityPath::new("Orders")?,
        EntityIncarnationKind::Queue,
    )?;
    let original = bind(&fixture, &queue, &queue, EntityIncarnationKind::Queue)?;
    assert_ne!(upper, original);
    let neighbor = fixture
        .machine
        .bind_entity(
            &other.namespace,
            &queue,
            &queue,
            EntityIncarnationKind::Queue,
        )?
        .expect("other namespace");
    assert_ne!(neighbor, original);
    assert_eq!(
        fixture.machine.bind_entity(
            &fixture.namespace,
            &EntityPath::new("missing")?,
            &EntityPath::new("missing")?,
            EntityIncarnationKind::Queue,
        )?,
        None
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, clock);
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

fn primary_generation_fences_queue_topic_aliases_and_survives_reopen<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = fixture(provider)?;
    let path = fixture.entity.clone();
    let queue = bind(&fixture, &path, &path, EntityIncarnationKind::Queue)?;
    let old = fenced(&fixture, &queue, &path, 0, send("old"));
    assert_eq!(
        fixture.machine.apply_fenced(&old)?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    fixture.at(
        1,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    )?;
    assert!(incarnation(&fixture, &path)?.is_retired());
    assert_eq!(incarnation(&fixture, &path)?.generation(), 1);
    reject_fenced(&fixture, &old, BrokerError::EntityBindingStale)?;
    let before = fixture.machine.store().snapshot()?;
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    create_topic(&fixture, &path, 2)?;
    let topic = bind(&fixture, &path, &path, EntityIncarnationKind::Topic)?;
    assert_eq!(topic.generation(), 2);
    reject_fenced(&fixture, &old, BrokerError::EntityBindingStale)?;
    let topic_command = fenced(&fixture, &topic, &path, 2, send("topic"));
    assert_eq!(
        fixture.machine.apply_fenced(&topic_command)?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(2)
        }
    );
    fixture.at(
        3,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Auto,
        },
    )?;
    assert_eq!(incarnation(&fixture, &path)?.generation(), 2);
    assert!(incarnation(&fixture, &path)?.is_retired());
    fixture.at(
        4,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let recreated = bind(&fixture, &path, &path, EntityIncarnationKind::Queue)?;
    assert_eq!(recreated.generation(), 3);
    reject_fenced(&fixture, &old, BrokerError::EntityBindingStale)?;
    reject_fenced(&fixture, &topic_command, BrokerError::EntityBindingStale)?;
    assert_eq!(
        fixture
            .machine
            .apply_fenced(&fenced(&fixture, &recreated, &path, 4, send("fresh")))?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(3)
        }
    );
    Ok(())
}

fn subscription_rule_guards_follow_the_child_not_the_parent_command<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider)?;
    let topic = EntityPath::new("events")?;
    create_topic(&fixture, &topic, 1)?;
    let alpha = SubscriptionName::new("Alpha")?;
    let beta = SubscriptionName::new("beta")?;
    let child = subscribe(&fixture, &topic, &alpha, 1)?;
    let sibling = subscribe(&fixture, &topic, &beta, 1)?;
    let parent_guard = bind(&fixture, &topic, &topic, EntityIncarnationKind::Topic)?;
    let old = bind(
        &fixture,
        &child,
        &child,
        EntityIncarnationKind::Subscription,
    )?;
    let sibling_guard = bind(
        &fixture,
        &sibling,
        &sibling,
        EntityIncarnationKind::Subscription,
    )?;
    let command = fenced(
        &fixture,
        &old,
        &topic,
        2,
        CommandKind::CreateRule {
            subscription: alpha.clone(),
            name: RuleName::new("chosen")?,
            filter: RuleFilter::False,
        },
    );
    fixture.machine.validate_fenced_intent(
        &old,
        &fixture.namespace,
        &topic,
        &command.command.kind,
    )?;
    assert_eq!(
        fixture.machine.apply_fenced(&command)?,
        CommandOutcome::RuleCreated
    );
    assert_eq!(
        fixture
            .machine
            .rules_fenced(&old, &fixture.namespace, &topic, &alpha)?
            .len(),
        2
    );
    at(
        &fixture,
        &topic,
        3,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Subscription {
                name: alpha.clone(),
            },
        },
    )?;
    subscribe(&fixture, &topic, &alpha, 4)?;
    let fresh = bind(
        &fixture,
        &child,
        &child,
        EntityIncarnationKind::Subscription,
    )?;
    assert_eq!(fresh.generation(), 2);
    assert_eq!(
        bind(&fixture, &topic, &topic, EntityIncarnationKind::Topic)?,
        parent_guard
    );
    assert_eq!(
        bind(
            &fixture,
            &sibling,
            &sibling,
            EntityIncarnationKind::Subscription
        )?,
        sibling_guard
    );
    reject_fenced(&fixture, &command, BrokerError::EntityBindingStale)?;
    let before = fixture.machine.store().snapshot()?;
    let clock = fixture.machine.last_applied_time()?;
    reset(&fixture);
    assert_eq!(
        fixture
            .machine
            .rules_fenced(&old, &fixture.namespace, &topic, &alpha),
        Err(BrokerError::EntityBindingStale)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, clock);
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
    assert_eq!(
        fixture
            .machine
            .rules_fenced(&fresh, &fixture.namespace, &topic, &alpha)?
            .len(),
        1
    );
    assert_eq!(
        fixture.machine.apply_fenced(&fenced(
            &fixture,
            &fresh,
            &topic,
            4,
            CommandKind::CreateRule {
                subscription: alpha.clone(),
                name: RuleName::new("chosen")?,
                filter: RuleFilter::True,
            }
        ))?,
        CommandOutcome::RuleCreated
    );
    assert_eq!(
        fixture.machine.apply_fenced(&fenced(
            &fixture,
            &fresh,
            &topic,
            5,
            CommandKind::DeleteRule {
                subscription: alpha,
                name: RuleName::new("chosen")?,
            }
        ))?,
        CommandOutcome::RuleDeleted
    );
    Ok(())
}

fn shadow_guard_reuses_its_owner_generation_but_not_its_physical_target<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider)?;
    let parent = fixture.entity.clone();
    let shadow = parent.dead_letter_queue()?;
    let parent_guard = bind(&fixture, &parent, &parent, EntityIncarnationKind::Queue)?;
    let old = bind(&fixture, &shadow, &parent, EntityIncarnationKind::Queue)?;
    assert_eq!(old.generation(), parent_guard.generation());
    assert_ne!(old, parent_guard);
    reject_fenced(
        &fixture,
        &fenced(
            &fixture,
            &parent_guard,
            &shadow,
            1,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
        ),
        BrokerError::InvalidEntityBinding,
    )?;
    fixture.at(1, send("dead"))?;
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        1,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("locked delivery")
    };
    fixture.at(
        1,
        CommandKind::DeadLetter {
            sequence: delivery.sequence,
            lock_token: delivery.lock.expect("lock").token,
            reason: "held".into(),
            description: "for shadow".into(),
        },
    )?;
    let CommandOutcome::Received(Some(copy)) = fixture.machine.apply_fenced(&fenced(
        &fixture,
        &old,
        &shadow,
        1,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    ))?
    else {
        panic!("shadow delivery")
    };
    fixture.at(
        2,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    )?;
    fixture.at(
        3,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let fresh = bind(&fixture, &shadow, &parent, EntityIncarnationKind::Queue)?;
    assert_eq!(fresh.generation(), 2);
    reject_fenced(
        &fixture,
        &fenced(
            &fixture,
            &old,
            &shadow,
            0,
            CommandKind::Complete {
                sequence: copy.sequence,
                lock_token: copy.lock.expect("lock").token,
            },
        ),
        BrokerError::EntityBindingStale,
    )?;
    assert_eq!(
        fixture.machine.apply_fenced(&fenced(
            &fixture,
            &fresh,
            &shadow,
            3,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            }
        ))?,
        CommandOutcome::Received(None)
    );
    Ok(())
}

fn configuration_changes_and_noops_leave_all_admitted_identities_live<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider)?;
    let parent = fixture.entity.clone();
    let topic = EntityPath::new("events")?;
    create_topic(&fixture, &topic, 1)?;
    let name = SubscriptionName::new("Alpha")?;
    let child = subscribe(&fixture, &topic, &name, 1)?;
    let guards = [
        bind(&fixture, &parent, &parent, EntityIncarnationKind::Queue)?,
        bind(
            &fixture,
            &parent.dead_letter_queue()?,
            &parent,
            EntityIncarnationKind::Queue,
        )?,
        bind(&fixture, &topic, &topic, EntityIncarnationKind::Topic)?,
        bind(
            &fixture,
            &child,
            &child,
            EntityIncarnationKind::Subscription,
        )?,
        bind(
            &fixture,
            &child.dead_letter_queue()?,
            &child,
            EntityIncarnationKind::Subscription,
        )?,
    ];
    let stored = [parent.clone(), topic.clone(), child.clone()]
        .into_iter()
        .map(|owner| {
            Ok((
                owner.clone(),
                fixture
                    .machine
                    .store()
                    .get(&keys::entity_incarnation(&fixture.namespace, &owner))?,
            ))
        })
        .collect::<TestResult<Vec<_>>>()?;
    fixture.at(
        2,
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                max_delivery_count: Some(2),
                ..QueueConfigUpdate::default()
            },
        },
    )?;
    at(
        &fixture,
        &topic,
        2,
        CommandKind::UpdateTopic {
            update: TopicConfigUpdate {
                max_message_bytes: Some(8_192),
                ..TopicConfigUpdate::default()
            },
        },
    )?;
    at(
        &fixture,
        &topic,
        2,
        CommandKind::UpdateSubscription {
            name: name.clone(),
            update: SubscriptionConfigUpdate {
                dead_lettering_on_filter_evaluation_exceptions: Some(false),
                ..SubscriptionConfigUpdate::default()
            },
        },
    )?;
    fixture.at(
        3,
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate::default(),
        },
    )?;
    at(
        &fixture,
        &topic,
        3,
        CommandKind::UpdateTopic {
            update: TopicConfigUpdate::default(),
        },
    )?;
    at(
        &fixture,
        &topic,
        3,
        CommandKind::UpdateSubscription {
            name,
            update: SubscriptionConfigUpdate::default(),
        },
    )?;
    for (owner, bytes) in stored {
        assert_eq!(
            fixture
                .machine
                .store()
                .get(&keys::entity_incarnation(&fixture.namespace, &owner))?,
            bytes
        );
    }
    for guard in guards {
        assert_eq!(
            bind(&fixture, guard.target(), guard.owner(), guard.kind())?,
            guard
        );
    }
    assert_eq!(
        fixture.machine.apply_fenced(&fenced(
            &fixture,
            &bind(&fixture, &parent, &parent, EntityIncarnationKind::Queue)?,
            &parent,
            2,
            send("queue")
        ))?,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
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
    admission_is_pure_exact_and_scoped_to_the_incarnation_owner,
    primary_generation_fences_queue_topic_aliases_and_survives_reopen,
    subscription_rule_guards_follow_the_child_not_the_parent_command,
    shadow_guard_reuses_its_owner_generation_but_not_its_physical_target,
    configuration_changes_and_noops_leave_all_admitted_identities_live,
}
