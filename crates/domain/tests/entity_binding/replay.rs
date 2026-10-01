use super::*;

fn standalone_fenced_commands_roundtrip_without_changing_legacy_command_bytes<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = fixture(provider)?;
    let path = fixture.entity.clone();
    let binding = bind(&fixture, &path, &path, EntityIncarnationKind::Queue)?;
    let command = fixture.command(1, send("replicated"));
    let legacy = codec::encode(&command)?;
    assert_eq!(codec::encode(&command.kind)?.get(1), Some(&1));
    let wrapper = FencedCommand::new(binding.clone(), command.clone());
    let encoded = codec::encode(&wrapper)?;
    let decoded: FencedCommand = codec::decode(&encoded)?;
    assert_eq!(decoded, wrapper);
    assert_eq!(decoded.binding, binding);
    assert_eq!(codec::encode(&decoded.command)?, legacy);
    let application = fixture.machine.apply_fenced_with_effects(&decoded)?;
    assert_eq!(
        application.outcome,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    assert_eq!(application.entity_deletions, None);
    assert_eq!(application.subscription_enqueues, None);
    let after = fixture.machine.store().snapshot()?;
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, after);
    fixture.machine.validate_fenced_intent(
        &decoded.binding,
        &decoded.command.namespace,
        &decoded.command.entity,
        &decoded.command.kind,
    )?;
    fixture.at(
        2,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Auto,
        },
    )?;
    fixture.at(
        3,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    let replayed: FencedCommand = codec::decode(&encoded)?;
    reject_fenced(&fixture, &replayed, BrokerError::EntityBindingStale)?;
    assert_eq!(codec::encode(&replayed.command)?, legacy);
    assert_eq!(
        codec::encode(&CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Auto
        })?
        .get(1),
        Some(&36)
    );
    Ok(())
}

fn guarded_intent_cannot_change_namespace_physical_target_or_rule_child<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider)?;
    let path = fixture.entity.clone();
    let binding = bind(&fixture, &path, &path, EntityIncarnationKind::Queue)?;
    let mut different_namespace = fenced(&fixture, &binding, &path, 1, send("other"));
    different_namespace.command.namespace = NamespaceName::new("neighbor")?;
    reject_fenced(
        &fixture,
        &different_namespace,
        BrokerError::InvalidEntityBinding,
    )?;
    reject_fenced(
        &fixture,
        &fenced(
            &fixture,
            &binding,
            &EntityPath::new("Orders")?,
            1,
            send("case"),
        ),
        BrokerError::InvalidEntityBinding,
    )?;
    let shadow = path.dead_letter_queue()?;
    let shadow_binding = bind(&fixture, &shadow, &path, EntityIncarnationKind::Queue)?;
    reject_fenced(
        &fixture,
        &fenced(
            &fixture,
            &shadow_binding,
            &path,
            1,
            CommandKind::Peek {
                from_sequence: SequenceNumber::new(1),
                max_messages: 1,
                session_id: None,
            },
        ),
        BrokerError::InvalidEntityBinding,
    )?;
    for (namespace, target, owner, generation) in [
        (fixture.namespace.clone(), path.clone(), path.clone(), 0),
        (
            fixture.namespace.clone(),
            path.clone(),
            EntityPath::new("neighbor")?,
            1,
        ),
        (
            codec::decode::<NamespaceName>(&codec::encode(&"tenant\0neighbor")?)?,
            path.clone(),
            path.clone(),
            1,
        ),
    ] {
        let forged: EntityBinding = codec::decode(&codec::encode(&(
            namespace,
            target,
            owner,
            EntityIncarnationKind::Queue,
            generation,
        ))?)?;
        reject_fenced(
            &fixture,
            &fenced(&fixture, &forged, &path, 0, send("decoded forgery")),
            BrokerError::InvalidEntityBinding,
        )?;
    }
    let topic = EntityPath::new("events")?;
    create_topic(&fixture, &topic, 1)?;
    let alpha = SubscriptionName::new("Alpha")?;
    let child = subscribe(&fixture, &topic, &alpha, 1)?;
    let beta = SubscriptionName::new("beta")?;
    subscribe(&fixture, &topic, &beta, 1)?;
    let child_binding = bind(
        &fixture,
        &child,
        &child,
        EntityIncarnationKind::Subscription,
    )?;
    let parent_binding = bind(&fixture, &topic, &topic, EntityIncarnationKind::Topic)?;
    for guard in [&child_binding, &parent_binding] {
        reject_fenced(
            &fixture,
            &fenced(
                &fixture,
                guard,
                &topic,
                2,
                CommandKind::CreateRule {
                    subscription: beta.clone(),
                    name: RuleName::new("forged")?,
                    filter: RuleFilter::True,
                },
            ),
            BrokerError::InvalidEntityBinding,
        )?;
    }
    let before = fixture.machine.store().snapshot()?;
    let clock = fixture.machine.last_applied_time()?;
    reset(&fixture);
    assert_eq!(
        fixture
            .machine
            .rules_fenced(&child_binding, &fixture.namespace, &topic, &beta),
        Err(BrokerError::InvalidEntityBinding)
    );
    assert_eq!(
        fixture
            .machine
            .rules_fenced(&parent_binding, &fixture.namespace, &topic, &alpha),
        Err(BrokerError::InvalidEntityBinding)
    );
    assert_eq!(
        fixture.machine.rules_fenced(
            &child_binding,
            &NamespaceName::new("neighbor")?,
            &topic,
            &alpha
        ),
        Err(BrokerError::InvalidEntityBinding)
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

fn stale_endpoints_refuse_every_data_and_management_intent_before_clock_regression<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = fixture(provider)?;
    let path = fixture.entity.clone();
    let binding = bind(&fixture, &path, &path, EntityIncarnationKind::Queue)?;
    let sequence = SequenceNumber::new(1);
    let token = LockToken::new(1);
    let session_id = SessionId::new("cart")?;
    let session = SessionHold::new(session_id.clone(), token);
    let budget = DeliveryBudget {
        max_bytes: 1_024,
        per_message_overhead_bytes: 0,
    };
    let operations = vec![
        send("stale"),
        CommandKind::SendEnvelope {
            message_id: "stale".into(),
            body: Vec::new(),
            time_to_live_millis: None,
            session_id: None,
            envelope: Box::new(MessageEnvelope::default()),
        },
        CommandKind::SendBatch {
            messages: vec![IngressEnvelope {
                message_id: "stale".into(),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None,
                envelope: MessageEnvelope::default(),
                scheduled_enqueue_time: None,
            }],
        },
        CommandKind::Schedule {
            messages: vec![ScheduledMessage {
                message_id: "stale".into(),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None,
                enqueue_at: Timestamp::from_millis(100),
            }],
        },
        CommandKind::ScheduleEnvelopes {
            messages: vec![ScheduledEnvelope {
                message_id: "stale".into(),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None,
                enqueue_at: Timestamp::from_millis(100),
                envelope: MessageEnvelope::default(),
            }],
        },
        CommandKind::CancelScheduled {
            sequences: vec![sequence],
        },
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
        CommandKind::Peek {
            from_sequence: sequence,
            max_messages: 1,
            session_id: None,
        },
        CommandKind::PeekBounded {
            from_sequence: sequence,
            max_messages: 1,
            session_id: None,
            budget,
        },
        CommandKind::Complete {
            sequence,
            lock_token: token,
        },
        CommandKind::Abandon {
            sequence,
            lock_token: token,
        },
        CommandKind::Defer {
            sequence,
            lock_token: token,
        },
        CommandKind::DeadLetter {
            sequence,
            lock_token: token,
            reason: "held".into(),
            description: "stale".into(),
        },
        CommandKind::Settle {
            sequence,
            lock_token: token,
            disposition: SettlementDisposition::Complete,
            properties_to_modify: BTreeMap::new(),
        },
        CommandKind::RenewLock {
            sequence,
            lock_token: token,
            lock_duration_millis: None,
        },
        CommandKind::ReceiveDeferred {
            sequences: vec![sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session_id: None,
        },
        CommandKind::ReceiveDeferredBounded {
            sequences: vec![sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session_id: None,
            budget,
        },
        CommandKind::ReceiveDeferredHeld {
            sequences: vec![sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
            budget,
        },
        CommandKind::AcceptSession {
            session_id: Some(session_id),
            lock_duration_millis: None,
        },
        CommandKind::ReleaseSession {
            session: session.clone(),
        },
        CommandKind::RenewSessionLock {
            session: session.clone(),
            lock_duration_millis: None,
        },
        CommandKind::SetSessionState {
            session: session.clone(),
            state: b"stale".to_vec(),
        },
        CommandKind::GetSessionState { session },
    ];
    fixture.at(
        10,
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    )?;
    fixture.at(
        11,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    fixture.at(12, send("fresh"))?;
    let before = fixture.machine.store().snapshot()?;
    reset(&fixture);
    for kind in operations {
        assert_eq!(
            fixture
                .machine
                .validate_fenced_intent(&binding, &fixture.namespace, &path, &kind),
            Err(BrokerError::EntityBindingStale)
        );
        reject_fenced(
            &fixture,
            &fenced(&fixture, &binding, &path, 0, kind),
            BrokerError::EntityBindingStale,
        )?;
    }
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(12)
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
    standalone_fenced_commands_roundtrip_without_changing_legacy_command_bytes,
    guarded_intent_cannot_change_namespace_physical_target_or_rule_child,
    stale_endpoints_refuse_every_data_and_management_intent_before_clock_regression,
}
