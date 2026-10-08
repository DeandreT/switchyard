use super::*;

use admin_api::v1::{EntityKind, ListEntitiesRequest, entity_service_server::EntityService};
use protocol_amqp::Broker as _;
use server::{AtomRuleDefinition, AtomRuleOwnerError, NativeAdminService};
use std::{future::poll_fn, task::Poll};

fn rule_error(error: BrokerError) -> AtomRuleOwnerError {
    AtomRuleOwnerError::Submit(SubmitError::Propose(ProposeError::Broker(error)))
}

fn submit_error(error: BrokerError) -> SubmitError {
    SubmitError::Propose(ProposeError::Broker(error))
}

fn restore<S: StateStore>(store: &S, key: Vec<u8>, value: Option<Vec<u8>>) -> TestResult {
    store.apply(match value {
        Some(value) => WriteBatch::default().put(key, value),
        None => WriteBatch::default().delete(key),
    })?;
    Ok(())
}

fn seed_retained<S: StateStore>(fixture: &Fixture<S>) -> TestResult {
    for index in 0..3 {
        fixture.submit(
            &fixture.topic,
            CommandKind::Send {
                message_id: format!("mode-retained-{index}"),
                body: vec![index; 17],
                time_to_live_millis: Some(60_000),
                session_id: None,
            },
        )?;
        if index < 2 {
            let CommandOutcome::Received(Some(delivery)) = fixture.submit(
                &fixture.child()?,
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None,
                },
            )?
            else {
                panic!("retained mode fixture delivery");
            };
            if index == 0 {
                fixture.submit(
                    &fixture.child()?,
                    CommandKind::DeadLetter {
                        sequence: delivery.sequence,
                        lock_token: delivery.lock.expect("held delivery").token,
                        reason: "mode-retained".into(),
                        description: "unchanged".into(),
                    },
                )?;
            }
        }
    }
    Ok(())
}

fn stale_mode<S: StateStore>(fixture: &Fixture<S>) -> TestResult<Vec<u8>> {
    let other = EntityPath::new("zz-mode-generation-two")?;
    for generation in 1..=2 {
        assert_eq!(
            fixture.submit(
                &other,
                CommandKind::CreateTopic {
                    config: TopicConfig::default()
                },
            )?,
            CommandOutcome::TopicCreated,
        );
        if generation == 1 {
            assert_eq!(
                fixture.submit(
                    &other,
                    CommandKind::DeleteEntity {
                        target: DeleteEntityTarget::Topic
                    },
                )?,
                CommandOutcome::TopicDeleted,
            );
        }
    }
    Ok(fixture
        .store
        .inner
        .get(&keys::topic_mode(&fixture.namespace, &other))?
        .unwrap())
}

async fn parent_mode_corruption_refuses_owner_admission_before_both_clocks<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.create(SubscriptionConfig::default())?;
    seed_retained(&fixture)?;
    let mode_key = keys::topic_mode(&fixture.namespace, &fixture.topic);
    let valid = fixture
        .store
        .inner
        .get(&mode_key)?
        .expect("created topic mode");
    let stale = stale_mode(&fixture)?;
    assert_ne!(stale, valid);
    let child = fixture.child()?;
    let shadow = child.dead_letter_queue()?;
    let ghost = fixture
        .topic
        .subscription(&SubscriptionName::new("absent")?)?;
    let faults = [
        (mode_key.clone(), None),
        (mode_key, Some(vec![255])),
        (
            keys::topic_mode(&fixture.namespace, &fixture.topic),
            Some(stale),
        ),
        (
            keys::topic_mode(&fixture.namespace, &child),
            Some(valid.clone()),
        ),
        (
            keys::topic_mode(&fixture.namespace, &shadow),
            Some(valid.clone()),
        ),
        (
            keys::topic_mode(&fixture.namespace, &fixture.topic.dead_letter_queue()?),
            Some(valid.clone()),
        ),
        (keys::topic_mode(&fixture.namespace, &ghost), Some(valid)),
    ];
    let handle = fixture.handle();
    let mut child_wait = Box::pin(handle.deliverable(&fixture.namespace, &child));
    let mut shadow_wait = Box::pin(handle.deliverable(&fixture.namespace, &shadow));
    poll_fn(|cx| {
        assert!(child_wait.as_mut().poll(cx).is_pending());
        assert!(shadow_wait.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let service = NativeAdminService::new(handle.clone(), fixture.namespace.clone());
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    for (key, value) in faults {
        let original = fixture.store.inner.get(&key)?;
        restore(&fixture.store.inner, key.clone(), value)?;
        let before = fixture.store.snapshot()?;
        let host_reads = fixture.clock.reads.load(Ordering::SeqCst);
        fixture.store.arm(true);
        assert_eq!(
            fixture.get(),
            Err(wrapped(BrokerError::TopicCapacityCorrupt))
        );
        assert_eq!(
            fixture.handle().create_atom_subscription_blocking(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                SubscriptionName::new("new-child")?,
                SubscriptionConfig::default(),
            ),
            Err(wrapped(BrokerError::TopicCapacityCorrupt)),
        );
        assert_eq!(
            fixture.handle().update_atom_subscription_blocking(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                fixture.name.clone(),
                SubscriptionConfig::default(),
            ),
            Err(wrapped(BrokerError::TopicCapacityCorrupt)),
        );
        assert_eq!(
            fixture.delete(),
            Err(wrapped(BrokerError::TopicCapacityCorrupt))
        );
        assert_eq!(
            handle.entity_metadata_blocking(
                fixture.namespace.clone(),
                Attachment::Queue(fixture.topic.clone()),
            ),
            Err(submit_error(BrokerError::TopicCapacityCorrupt)),
        );
        assert_eq!(
            handle.entity_metadata_blocking(
                fixture.namespace.clone(),
                Attachment::Subscription {
                    topic: fixture.topic.clone(),
                    subscription: fixture.name.clone(),
                },
            ),
            Err(submit_error(BrokerError::TopicCapacityCorrupt)),
        );
        for target in [
            AdminTarget::Primary(fixture.topic.clone()),
            AdminTarget::Subscription {
                topic: fixture.topic.clone(),
                name: fixture.name.clone(),
            },
        ] {
            assert_eq!(
                tokio::time::timeout(
                    DEADLINE,
                    handle.admin_entity_metadata(fixture.namespace.clone(), target.clone())
                )
                .await?,
                Err(submit_error(BrokerError::TopicCapacityCorrupt)),
            );
            assert_eq!(
                tokio::time::timeout(
                    DEADLINE,
                    handle.bind_admin(fixture.namespace.clone(), target)
                )
                .await?,
                Err(submit_error(BrokerError::TopicCapacityCorrupt)),
            );
        }
        let name = RuleName::new("mode-check")?;
        assert_eq!(
            handle.create_atom_rule_blocking(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                fixture.name.clone(),
                AtomRuleDefinition {
                    name: name.clone(),
                    filter: RuleFilter::True,
                    action: None
                },
            ),
            Err(rule_error(BrokerError::TopicCapacityCorrupt)),
        );
        assert_eq!(
            handle.get_atom_rule_blocking(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                fixture.name.clone(),
                RuleName::new("$Default")?,
            ),
            Err(rule_error(BrokerError::TopicCapacityCorrupt)),
        );
        assert_eq!(
            handle.list_atom_rules_blocking(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                fixture.name.clone(),
                1000,
                1,
            ),
            Err(rule_error(BrokerError::TopicCapacityCorrupt)),
        );
        assert_eq!(
            handle.delete_atom_rule_blocking(
                fixture.namespace.clone(),
                fixture.topic.clone(),
                fixture.name.clone(),
                name,
            ),
            Err(rule_error(BrokerError::TopicCapacityCorrupt)),
        );
        // Trusted unauthenticated service calls do not prove native auth or transport.
        for kind in [EntityKind::Topic, EntityKind::Subscription] {
            let error = tokio::time::timeout(
                DEADLINE,
                service.list_entities(tonic::Request::new(ListEntitiesRequest {
                    namespace: fixture.namespace.as_str().into(),
                    kind: kind as i32,
                    page_size: 1,
                    page_token: String::new(),
                    parent_topic: if kind == EntityKind::Subscription {
                        fixture.topic.as_str().into()
                    } else {
                        String::new()
                    },
                })),
            )
            .await?
            .expect_err("corrupt topic mode must refuse a native list");
            assert_eq!(error.code(), tonic::Code::Internal);
        }
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), host_reads);
        assert_eq!(fixture.store.snapshot()?, before);
        poll_fn(|cx| {
            assert!(child_wait.as_mut().poll(cx).is_pending());
            assert!(shadow_wait.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        restore(&fixture.store.inner, key, original)?;
        let healthy = fixture.store.snapshot()?;
        fixture.store.arm(true);
        assert_eq!(fixture.get()?, Some(SubscriptionConfig::default()));
        assert_owner(&fixture.store.disarm(), 0);
        assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), host_reads);
        assert_eq!(fixture.store.snapshot()?, healthy);
    }
    drop(child_wait);
    drop(shadow_wait);
    drop(service);
    drop(handle);
    let expected = fixture.store.snapshot()?;
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, expected);
    Ok(())
}

async fn absent_child_and_stale_fences_preserve_mode_admission_priority<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = Fixture::new(provider.open()?)?;
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    let valid = fixture
        .store
        .inner
        .get(&keys::topic_mode(&fixture.namespace, &fixture.topic))?
        .unwrap();
    fixture.store.inner.apply(
        WriteBatch::default().delete(keys::topic_mode(&fixture.namespace, &fixture.topic)),
    )?;
    let before = fixture.store.snapshot()?;
    let reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.store.arm(true);
    for topic in [fixture.topic.clone(), EntityPath::new("missing-parent")?] {
        assert_eq!(
            fixture.handle().get_atom_subscription_blocking(
                fixture.namespace.clone(),
                topic.clone(),
                fixture.name.clone(),
            )?,
            None
        );
        assert_eq!(
            fixture.handle().get_atom_rule_blocking(
                fixture.namespace.clone(),
                topic,
                fixture.name.clone(),
                RuleName::new("$Default")?,
            ),
            Err(rule_error(BrokerError::SubscriptionNotFound))
        );
    }
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads);
    assert_eq!(fixture.store.snapshot()?, before);
    restore(
        &fixture.store.inner,
        keys::topic_mode(&fixture.namespace, &fixture.topic),
        Some(valid.clone()),
    )?;
    fixture.clock.forbidden.store(false, Ordering::SeqCst);
    fixture.create(SubscriptionConfig::default())?;
    let target = AdminTarget::Subscription {
        topic: fixture.topic.clone(),
        name: fixture.name.clone(),
    };
    let old = tokio::time::timeout(
        DEADLINE,
        fixture
            .handle()
            .bind_admin(fixture.namespace.clone(), target.clone()),
    )
    .await??
    .unwrap()
    .binding;
    fixture.delete()?;
    fixture.create(SubscriptionConfig::default())?;
    let current = tokio::time::timeout(
        DEADLINE,
        fixture
            .handle()
            .bind_admin(fixture.namespace.clone(), target),
    )
    .await??
    .unwrap()
    .binding;
    assert_eq!(current.generation(), old.generation() + 1);
    fixture.store.inner.apply(
        WriteBatch::default().delete(keys::topic_mode(&fixture.namespace, &fixture.topic)),
    )?;
    let before = fixture.store.snapshot()?;
    let reads = fixture.clock.reads.load(Ordering::SeqCst);
    fixture.clock.forbidden.store(true, Ordering::SeqCst);
    fixture.store.arm(true);
    for (binding, error) in [
        (old, BrokerError::EntityBindingStale),
        (current, BrokerError::TopicCapacityCorrupt),
    ] {
        assert_eq!(
            fixture.handle().submit_fenced_blocking(
                binding,
                fixture.topic.clone(),
                CommandKind::DeleteEntity {
                    target: DeleteEntityTarget::Subscription {
                        name: fixture.name.clone()
                    },
                },
            ),
            Err(submit_error(error))
        );
    }
    assert_owner(&fixture.store.disarm(), 0);
    assert_eq!(fixture.clock.reads.load(Ordering::SeqCst), reads);
    assert_eq!(fixture.store.snapshot()?, before);
    restore(
        &fixture.store.inner,
        keys::topic_mode(&fixture.namespace, &fixture.topic),
        Some(valid),
    )?;
    assert_eq!(fixture.get()?, Some(SubscriptionConfig::default()));
    let expected = fixture.store.snapshot()?;
    drop(fixture);
    assert_eq!(provider.open()?.snapshot()?, expected);
    Ok(())
}

for_each_subscription_backend! {
    parent_mode_corruption_refuses_owner_admission_before_both_clocks,
    absent_child_and_stale_fences_preserve_mode_admission_priority,
}
