use super::*;

use domain::{QueueTimeToLiveUpdate, SubscriptionConfigUpdate};

fn changed() -> SubscriptionConfig {
    SubscriptionConfig {
        lock_duration_millis: 30_000,
        max_delivery_count: 6,
        default_time_to_live_millis: Some(90_000),
        dead_lettering_on_message_expiration: true,
        dead_lettering_on_filter_evaluation_exceptions: false,
        ..SubscriptionConfig::default()
    }
}

fn update<S: StateStore>(
    fixture: &Fixture<S>,
    config: SubscriptionConfig,
) -> Result<SubscriptionConfig, AtomSubscriptionOwnerError> {
    fixture.handle().update_atom_subscription_blocking(
        fixture.namespace.clone(),
        fixture.topic.clone(),
        fixture.name.clone(),
        config,
    )
}

fn native_update(config: SubscriptionConfig, name: SubscriptionName) -> CommandKind {
    CommandKind::UpdateSubscription {
        name,
        update: SubscriptionConfigUpdate {
            lock_duration_millis: Some(config.lock_duration_millis),
            max_delivery_count: Some(config.max_delivery_count),
            default_time_to_live_millis: Some(match config.default_time_to_live_millis {
                Some(millis) => QueueTimeToLiveUpdate::Finite { millis },
                None => QueueTimeToLiveUpdate::Unlimited,
            }),
            max_message_bytes: None,
            requires_session: Some(false),
            dead_lettering_on_message_expiration: Some(config.dead_lettering_on_message_expiration),
            dead_lettering_on_filter_evaluation_exceptions: Some(
                config.dead_lettering_on_filter_evaluation_exceptions,
            ),
        },
    }
}

fn config_keys<S: StateStore>(fixture: &Fixture<S>) -> TestResult<BTreeSet<Key>> {
    let child = fixture.child()?;
    Ok(BTreeSet::from([
        keys::subscription(&fixture.namespace, &fixture.topic, &fixture.name),
        keys::queue_config(&fixture.namespace, &child),
        keys::queue_config(&fixture.namespace, &child.dead_letter_queue()?),
    ]))
}

fn projections<S: StateStore>(fixture: &Fixture<S>, config: SubscriptionConfig) -> TestResult {
    let child = fixture.child()?;
    let bytes = fixture
        .store
        .inner
        .get(&keys::subscription(
            &fixture.namespace,
            &fixture.topic,
            &fixture.name,
        ))?
        .expect("current subscription membership");
    assert_eq!(SubscriptionConfig::decode(&bytes)?, config);
    for (entity, expected) in [
        (child.clone(), config.to_queue_config()),
        (
            child.dead_letter_queue()?,
            config.to_queue_config().dead_letter_shadow(),
        ),
    ] {
        let bytes = fixture
            .store
            .inner
            .get(&keys::queue_config(&fixture.namespace, &entity))?
            .expect("current backing/shadow config");
        assert_eq!(QueueConfig::decode(&bytes)?, expected);
        assert!(
            fixture
                .store
                .inner
                .get(&keys::queue_capacity_mode(&fixture.namespace, &entity))?
                .is_none()
        );
        assert!(
            fixture
                .store
                .inner
                .get(&keys::queue_capacity_usage(&fixture.namespace, &entity))?
                .is_none()
        );
        assert!(
            fixture
                .store
                .inner
                .scan_prefix(&keys::message_charge_prefix(&fixture.namespace, &entity), 1)?
                .is_empty()
        );
    }
    Ok(())
}

fn unchanged_outside(before: &StoreSnapshot, after: &StoreSnapshot, changed_keys: &BTreeSet<Key>) {
    let retained = |snapshot: &StoreSnapshot| {
        snapshot
            .entries()
            .iter()
            .filter(|(key, _)| !changed_keys.contains(key) && key != &keys::clock())
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        retained(before),
        retained(after),
        "update rewrote retained or unrelated rows"
    );
}

async fn full_replacement_returns_prepared_config_and_noop_keeps_clock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let before = fixture.store.snapshot()?;
    let reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.store.arm(false);
    assert_eq!(update(&fixture, changed())?, changed());
    let observed = fixture.store.disarm();
    assert_owner(&observed, 1);
    let mut expected = config_keys(&fixture)?;
    expected.insert(keys::clock());
    assert_eq!(observed.mutations.len(), 4);
    assert_eq!(
        observed
            .mutations
            .iter()
            .map(|mutation| match mutation {
                Mutation::Put { key, .. } => key.clone(),
                Mutation::Delete { .. } => panic!("config update deleted a row"),
            })
            .collect::<BTreeSet<_>>(),
        expected
    );
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads + 1);
    projections(&fixture, changed())?;
    unchanged_outside(&before, &fixture.store.snapshot()?, &config_keys(&fixture)?);

    let before = fixture.store.snapshot()?;
    let reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.manual.set(2_000);
    fixture.store.arm(false);
    assert_eq!(update(&fixture, changed())?, changed());
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads + 1);
    assert_eq!(
        fixture.store.snapshot()?,
        before,
        "no-op persisted a new Clock"
    );

    fixture.store.arm(false);
    assert_eq!(
        bounded(
            "async complete subscription reset",
            fixture.handle().update_atom_subscription(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                fixture.name.clone(),
                SubscriptionConfig::default(),
            )
        )
        .await?,
        SubscriptionConfig::default()
    );
    assert_owner(&fixture.store.disarm(), 1);
    projections(&fixture, SubscriptionConfig::default())?;
    unchanged_outside(&before, &fixture.store.snapshot()?, &config_keys(&fixture)?);
    let expected = fixture.store.snapshot()?;
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, expected);
    Ok(())
}

async fn failed_update_is_atomic_and_desired_refusals_never_stamp<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let before = fixture.store.snapshot()?;
    fixture.store.arm(false);
    fixture.store.observation.lock().unwrap().fail_next = true;
    assert!(matches!(
        update(&fixture, changed()),
        Err(AtomSubscriptionOwnerError::Submit(SubmitError::Propose(
            ProposeError::Broker(BrokerError::Storage(StorageError::Backend { .. }))
        )))
    ));
    assert_owner(&fixture.store.disarm(), 1);
    assert_eq!(fixture.store.snapshot()?, before);
    let reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    for config in [
        SubscriptionConfig {
            requires_session: true,
            ..changed()
        },
        SubscriptionConfig {
            max_message_bytes: 4_096,
            ..changed()
        },
        SubscriptionConfig {
            lock_duration_millis: 4_999,
            ..changed()
        },
        SubscriptionConfig {
            lock_duration_millis: 300_001,
            ..changed()
        },
        SubscriptionConfig {
            max_delivery_count: 0,
            ..changed()
        },
        SubscriptionConfig {
            default_time_to_live_millis: Some(999),
            ..changed()
        },
    ] {
        fixture.store.arm(true);
        assert_eq!(
            update(&fixture, config),
            Err(AtomSubscriptionOwnerError::UnsupportedDefinition)
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads);
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.store.arm(false);
    assert_eq!(update(&fixture, changed())?, changed());
    assert_owner(&fixture.store.disarm(), 1);
    projections(&fixture, changed())?;
    unchanged_outside(&before, &fixture.store.snapshot()?, &config_keys(&fixture)?);
    Ok(())
}

async fn current_profile_and_corruption_win_before_desired_validation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    for (index, config) in [
        SubscriptionConfig {
            requires_session: true,
            ..SubscriptionConfig::default()
        },
        SubscriptionConfig {
            max_message_bytes: 4_096,
            ..SubscriptionConfig::default()
        },
        SubscriptionConfig {
            lock_duration_millis: 1_000,
            ..SubscriptionConfig::default()
        },
    ]
    .into_iter()
    .enumerate()
    {
        let name = SubscriptionName::new(format!("native-{index}"))?;
        fixture.submit(
            &fixture.topic,
            CommandKind::CreateSubscription {
                name: name.clone(),
                config,
            },
        )?;
        let before = fixture.store.snapshot()?;
        let reads = fixture.clock.reads.load(Ordering::SeqCst);
        fixture.clock.forbidden.store(true, Ordering::SeqCst);
        fixture.store.arm(true);
        assert_eq!(
            fixture.handle().update_atom_subscription_blocking(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                name.clone(),
                changed(),
            ),
            Err(AtomSubscriptionOwnerError::UnsupportedDefinition)
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads);
        assert_eq!(fixture.store.snapshot()?, before);
        let key =
            keys::queue_capacity_mode(&fixture.namespace, &fixture.topic.subscription(&name)?);
        fixture
            .store
            .inner
            .apply(WriteBatch::default().put(key.clone(), vec![255]))?;
        let corrupt = fixture.store.snapshot()?;
        fixture.store.arm(true);
        assert_eq!(
            fixture.handle().update_atom_subscription_blocking(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                name,
                SubscriptionConfig {
                    requires_session: true,
                    ..changed()
                },
            ),
            Err(wrapped(BrokerError::QueueCapacityCorrupt))
        );
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads);
        assert_eq!(fixture.store.snapshot()?, corrupt);
        fixture
            .store
            .inner
            .apply(WriteBatch::default().delete(key))?;
        fixture.clock.forbidden.store(false, Ordering::SeqCst);
    }
    Ok(())
}

async fn absent_update_keeps_original_diagnostics_and_clock_priority<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    for orphan in [false, true] {
        if orphan {
            fixture.store.inner.apply(
                WriteBatch::default()
                    .put(
                        keys::message(
                            &fixture.namespace,
                            &fixture.child()?,
                            SequenceNumber::new(1),
                        ),
                        vec![255],
                    )
                    .put(
                        keys::rule(
                            &fixture.namespace,
                            &fixture.topic,
                            &fixture.name,
                            &RuleName::new("orphan")?,
                        ),
                        vec![255],
                    ),
            )?;
        }
        let before = fixture.store.snapshot()?;
        let original = StateMachine::new(fixture.store.inner.clone())
            .apply(&Command::new(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                Timestamp::from_millis(1_000),
                native_update(changed(), fixture.name.clone()),
            ))
            .expect_err("original absent update refuses");
        let reads = fixture.clock.reads.load(Ordering::SeqCst);
        fixture.store.arm(false);
        assert_eq!(update(&fixture, changed()), Err(wrapped(original)));
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads + 1);
        assert_eq!(fixture.store.snapshot()?, before);
    }
    let before = fixture.store.snapshot()?;
    let reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.manual.set(0);
    fixture.store.arm(false);
    assert!(matches!(
        update(&fixture, changed()),
        Err(AtomSubscriptionOwnerError::Submit(SubmitError::Propose(
            ProposeError::ClockWentBackward { .. }
        )))
    ));
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads + 1);
    assert_eq!(fixture.store.snapshot()?, before);
    Ok(())
}

async fn old_child_fence_is_refused_before_clock_and_by_name_updates_current<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    let target = AdminTarget::Subscription {
        topic: fixture.topic.clone(),
        name: fixture.name.clone(),
    };
    let old = bounded(
        "old update identity",
        fixture
            .handle()
            .bind_admin(fixture.namespace.clone(), target.clone()),
    )
    .await?
    .unwrap()
    .binding;
    fixture.delete()?;
    fixture.create(SubscriptionConfig::default())?;
    let current = bounded(
        "new update identity",
        fixture
            .handle()
            .bind_admin(fixture.namespace.clone(), target),
    )
    .await?
    .unwrap()
    .binding;
    assert_eq!(current.generation(), old.generation() + 1);
    let before = fixture.store.snapshot()?;
    let reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    fixture.store.arm(true);
    assert_eq!(
        fixture.handle().submit_fenced_blocking(
            old,
            fixture.topic.clone(),
            native_update(changed(), fixture.name.clone()),
        ),
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::EntityBindingStale
        )))
    );
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads);
    assert_eq!(fixture.store.snapshot()?, before);
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.store.arm(false);
    assert_eq!(update(&fixture, changed())?, changed());
    assert_owner(&fixture.store.disarm(), 1);
    unchanged_outside(&before, &fixture.store.snapshot()?, &config_keys(&fixture)?);
    let record = StateMachine::new(fixture.store.inner.clone())
        .entity_incarnation(&fixture.namespace, &fixture.child()?)?
        .unwrap();
    assert!(!record.is_retired());
    assert_eq!(record.generation(), current.generation());
    Ok(())
}

async fn update_preserves_retained_records_rules_and_all_waiters<P: StoreProvider>(
    provider: P,
) -> TestResult {
    use protocol_amqp::Broker as _;
    use std::{future::poll_fn, task::Poll};
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig {
        default_time_to_live_millis: Some(45_000),
        ..SubscriptionConfig::default()
    })?;
    let sibling = SubscriptionName::new("sibling")?;
    fixture.submit(
        &fixture.topic,
        CommandKind::CreateSubscription {
            name: sibling.clone(),
            config: SubscriptionConfig::default(),
        },
    )?;
    for index in 1..=4 {
        assert_eq!(
            fixture.submit(
                &fixture.topic,
                CommandKind::Send {
                    message_id: format!("retained-{index}"),
                    body: vec![index as u8; 32],
                    time_to_live_millis: None,
                    session_id: None,
                }
            )?,
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(index)
            }
        );
    }
    let child = fixture.child()?;
    for index in 1..=3 {
        let CommandOutcome::Received(Some(delivery)) = fixture.submit(
            &child,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
        )?
        else {
            panic!("retained message not received");
        };
        assert_eq!(delivery.sequence, SequenceNumber::new(index));
        let lock = delivery.lock.expect("retained lock");
        match index {
            1 => {
                fixture.submit(
                    &child,
                    CommandKind::DeadLetter {
                        sequence: delivery.sequence,
                        lock_token: lock.token,
                        reason: "update retention".into(),
                        description: "original DLQ record".into(),
                    },
                )?;
            }
            2 => {
                fixture.submit(
                    &child,
                    CommandKind::Defer {
                        sequence: delivery.sequence,
                        lock_token: lock.token,
                    },
                )?;
            }
            _ => {}
        }
    }
    fixture.submit(
        &fixture.topic,
        CommandKind::DeleteRule {
            subscription: fixture.name.clone(),
            name: RuleName::new("$Default")?,
        },
    )?;
    fixture.submit(
        &fixture.topic,
        CommandKind::CreateRule {
            subscription: fixture.name.clone(),
            name: RuleName::new("custom")?,
            filter: RuleFilter::False,
        },
    )?;
    let before = fixture.store.snapshot()?;
    for entity in [&child, &child.dead_letter_queue()?] {
        assert!(
            !fixture
                .store
                .inner
                .scan_prefix(&keys::message_prefix(&fixture.namespace, entity), 1)?
                .is_empty()
        );
    }
    assert!(
        !fixture
            .store
            .inner
            .scan_prefix(&keys::lock_prefix(&fixture.namespace, &child), 1)?
            .is_empty()
    );
    assert!(
        !fixture
            .store
            .inner
            .scan_prefix(&keys::ready_prefix(&fixture.namespace, &child), 1)?
            .is_empty()
    );
    assert!(
        !fixture
            .store
            .inner
            .scan_prefix(&keys::expiry_prefix(&fixture.namespace, &child), 1)?
            .is_empty()
    );
    let handle = fixture.handle();
    let shadow = child.dead_letter_queue()?;
    let sibling_entity = fixture.topic.subscription(&sibling)?;
    let mut child_wait = Box::pin(handle.deliverable(&fixture.namespace, &child));
    let mut shadow_wait = Box::pin(handle.deliverable(&fixture.namespace, &shadow));
    let mut sibling_wait = Box::pin(handle.deliverable(&fixture.namespace, &sibling_entity));
    poll_fn(|cx| {
        assert!(child_wait.as_mut().poll(cx).is_pending());
        assert!(shadow_wait.as_mut().poll(cx).is_pending());
        assert!(sibling_wait.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    fixture.store.arm(false);
    assert_eq!(
        update(&fixture, SubscriptionConfig::default())?,
        SubscriptionConfig::default()
    );
    assert_owner(&fixture.store.disarm(), 1);
    projections(&fixture, SubscriptionConfig::default())?;
    unchanged_outside(&before, &fixture.store.snapshot()?, &config_keys(&fixture)?);
    poll_fn(|cx| {
        assert!(child_wait.as_mut().poll(cx).is_pending());
        assert!(shadow_wait.as_mut().poll(cx).is_pending());
        assert!(sibling_wait.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(child_wait);
    drop(shadow_wait);
    drop(sibling_wait);
    drop(handle);
    let expected = fixture.store.snapshot()?;
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, expected);
    Ok(())
}

for_each_subscription_backend! {
    full_replacement_returns_prepared_config_and_noop_keeps_clock,
    update_preserves_retained_records_rules_and_all_waiters,
    failed_update_is_atomic_and_desired_refusals_never_stamp,
    current_profile_and_corruption_win_before_desired_validation,
    absent_update_keeps_original_diagnostics_and_clock_priority,
    old_child_fence_is_refused_before_clock_and_by_name_updates_current,
}
