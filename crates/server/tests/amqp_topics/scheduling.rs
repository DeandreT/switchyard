use amqp::{Array, FilterSet, Source, decode_message};
use server::TimerWorker;

use super::{
    management::{Management, map, status},
    *,
};

fn scheduled(mut message: Message, at: i64) -> Message {
    message
        .message_annotations
        .get_or_insert_with(Default::default)
        .insert(
            Symbol::from(protocol_amqp::SCHEDULED_ENQUEUE_TIME_ANNOTATION),
            Value::Timestamp(at.into()),
        );
    message
}

fn schedule_body(messages: &[Message]) -> TestResult<OrderedMap<Value, Value>> {
    Ok(map([(
        protocol_amqp::MESSAGES,
        Value::List(
            messages
                .iter()
                .map(|message| {
                    Ok(Value::Map(map([(
                        protocol_amqp::MESSAGE,
                        Value::Binary(encode_message(message)?.into()),
                    )])))
                })
                .collect::<TestResult<Vec<_>>>()?,
        ),
    )]))
}

fn response_body(message: &Message) -> &OrderedMap<Value, Value> {
    let Body::Value(Value::Map(body)) = &message.body else {
        panic!("management map")
    };
    body
}

fn handles(message: &Message) -> Vec<i64> {
    status(message, 200);
    let Some(Value::Array(numbers)) =
        response_body(message).get(&Value::String(protocol_amqp::SEQUENCE_NUMBERS.into()))
    else {
        panic!("scheduled handles")
    };
    numbers
        .iter()
        .map(|number| match number {
            Value::Long(number) => *number,
            other => panic!("scheduled handle: {other:?}"),
        })
        .collect()
}

async fn schedule(
    management: &mut Management,
    id: &str,
    messages: &[Message],
) -> TestResult<Vec<i64>> {
    Ok(handles(
        &management
            .request(
                id,
                protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
                None,
                schedule_body(messages)?,
            )
            .await?,
    ))
}

async fn cancel(management: &mut Management, id: &str, numbers: &[i64]) -> TestResult<Message> {
    management
        .request(
            id,
            protocol_amqp::CANCEL_SCHEDULED_MESSAGE_OPERATION,
            None,
            map([(
                protocol_amqp::SEQUENCE_NUMBERS,
                Value::Array(Array::from(
                    numbers.iter().copied().map(Value::Long).collect::<Vec<_>>(),
                )),
            )]),
        )
        .await
}

async fn peek(management: &mut Management, id: &str) -> TestResult<Vec<Message>> {
    let response = management
        .request(
            id,
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            None,
            map([
                (protocol_amqp::FROM_SEQUENCE_NUMBER, Value::Long(1)),
                (protocol_amqp::MESSAGE_COUNT, Value::Int(32)),
            ]),
        )
        .await?;
    status(&response, 200);
    let Some(Value::List(messages)) =
        response_body(&response).get(&Value::String(protocol_amqp::MESSAGES.into()))
    else {
        panic!("peek messages")
    };
    messages
        .iter()
        .map(|message| {
            let Value::Map(entry) = message else {
                panic!("peek entry")
            };
            assert!(
                entry
                    .get(&Value::String(protocol_amqp::LOCK_TOKEN.into()))
                    .is_none()
            );
            let Some(Value::Binary(encoded)) =
                entry.get(&Value::String(protocol_amqp::MESSAGE.into()))
            else {
                panic!("peek binary")
            };
            Ok(decode_message(encoded)?)
        })
        .collect()
}

fn state(message: &Message, expected: i32) {
    assert_eq!(
        message
            .message_annotations
            .as_ref()
            .and_then(|annotations| annotations
                .get(Symbol::from(protocol_amqp::MESSAGE_STATE_ANNOTATION))),
        Some(&Value::Int(expected))
    );
}

fn condition(message: &Message, expected: &str) {
    assert_eq!(
        message
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::ERROR_CONDITION_PROPERTY)),
        Some(&Value::Symbol(Symbol::from(expected)))
    );
}

fn copy(message: &Message, original: &Message, enqueued_at: i64, ttl: u32) {
    assert_eq!(message.body, original.body);
    assert_eq!(
        message.application_properties,
        original.application_properties
    );
    assert_eq!(message.footer, original.footer);
    let mut expected = original.properties.clone().expect("original properties");
    expected.creation_time = Some(enqueued_at);
    expected.absolute_expiry_time = Some(enqueued_at + i64::from(ttl));
    assert_eq!(message.properties.as_ref(), Some(&expected));
    assert_eq!(message.header.as_ref().expect("header").ttl, Some(ttl));
    scheduled_time(message, original);
}

fn scheduled_time(message: &Message, original: &Message) {
    let annotation = Symbol::from(protocol_amqp::SCHEDULED_ENQUEUE_TIME_ANNOTATION);
    let expected = original
        .message_annotations
        .as_ref()
        .expect("scheduled annotations")
        .get(annotation.clone())
        .expect("scheduled time");
    assert_eq!(
        message
            .message_annotations
            .as_ref()
            .and_then(|annotations| annotations.get(annotation)),
        Some(expected)
    );
}

async fn subscription<P: StoreProvider>(
    node: &Node<P>,
    name: &str,
    config: SubscriptionConfig,
) -> TestResult<EntityPath> {
    let name = SubscriptionName::new(name)?;
    node.submit(
        &node.topic,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config,
        },
    )
    .await?;
    Ok(node.topic.subscription(&name)?)
}

fn sweep<P: StoreProvider>(node: &Node<P>, at: u64) -> TestResult {
    node.clock.set(at);
    TimerWorker::new(&node.broker.as_ref().expect("broker").handle()).sweep_once()?;
    Ok(())
}

async fn wait_released<P: StoreProvider>(
    node: &Node<P>,
    entity: &EntityPath,
    id: &str,
) -> TestResult {
    let id = domain::SessionId::new(id)?;
    timeout(DEADLINE, async {
        loop {
            let record = StateMachine::new(node.store.as_ref().expect("store").clone()).session(
                &node.namespace,
                entity,
                &id,
            )?;
            if record.is_none_or(|record| record.lock.is_none()) {
                return Ok::<(), Box<dyn Error>>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}

async fn at_stage<T, E: std::fmt::Display>(
    stage: &str,
    operation: impl std::future::Future<Output = Result<T, E>>,
) -> TestResult<T> {
    operation
        .await
        .map_err(|error| std::io::Error::other(format!("{stage}: {error}")).into())
}

async fn parent_handles_cancel_and_activate_into_fresh_shared_copies<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(
        provider,
        "Orders",
        TopicConfig {
            default_time_to_live_millis: Some(4_000),
            ..TopicConfig::default()
        },
    )
    .await?;
    let alpha = subscription(
        &node,
        "Alpha",
        SubscriptionConfig {
            default_time_to_live_millis: Some(3_000),
            ..SubscriptionConfig::default()
        },
    )
    .await?;
    let beta = subscription(
        &node,
        "beta",
        SubscriptionConfig {
            default_time_to_live_millis: Some(5_000),
            ..SubscriptionConfig::default()
        },
    )
    .await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut sender = ClientSender::attach(&mut session, "publisher", node.topic.as_str()).await?;
    let mut management = Management::attach(&mut session, "topic", "Orders/$MANAGEMENT").await?;
    let mut child_management =
        Management::attach(&mut session, "child", &format!("{alpha}/$management")).await?;
    let before = node.snapshot()?;
    let refused = child_management
        .request(
            "child-schedule",
            protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
            None,
            schedule_body(&[scheduled(rich(99), 5_000)])?,
        )
        .await?;
    status(&refused, 400);
    condition(&refused, "amqp:not-allowed");
    assert_eq!(node.snapshot()?, before);
    let refused = management
        .request(
            "parent-session-filter",
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            None,
            map([
                (protocol_amqp::FROM_SEQUENCE_NUMBER, Value::Long(1)),
                (protocol_amqp::MESSAGE_COUNT, Value::Int(32)),
                (protocol_amqp::SESSION_ID, Value::String("A".into())),
            ]),
        )
        .await?;
    status(&refused, 400);
    condition(&refused, "amqp:not-allowed");
    assert_eq!(node.snapshot()?, before);
    let originals = [
        scheduled(rich(0), 5_000),
        scheduled(rich(1), 5_000),
        scheduled(rich(2), 5_000),
        scheduled(rich(3), 5_000),
        scheduled(rich(4), 5_000),
    ];
    assert_eq!(
        schedule(&mut management, "schedule-two", &originals[..2]).await?,
        vec![1, 2]
    );
    accepted(sender.send(originals[2].clone()).await?);
    let batch = Message {
        body: Body::Data(
            originals[3..]
                .iter()
                .map(encode_message)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(Into::into)
                .collect(),
        ),
        ..Message::default()
    };
    accepted(
        sender
            .send_with_message_format(batch, protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT)
            .await?,
    );
    let future = peek(&mut management, "future-parent").await?;
    assert_eq!(
        future.iter().map(sequence).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    for message in &future {
        state(message, 2);
    }
    for entity in [&alpha, &beta] {
        assert!(node.peek(entity).await?.is_empty());
        assert!(node.peek(&entity.dead_letter_queue()?).await?.is_empty());
    }
    status(
        &cancel(&mut management, "cancel-selected", &[2, 3, 4]).await?,
        200,
    );
    assert_eq!(
        peek(&mut management, "remaining-parent")
            .await?
            .iter()
            .map(sequence)
            .collect::<Vec<_>>(),
        vec![1, 5]
    );
    let immediate = rich(5);
    accepted(sender.send(immediate.clone()).await?);
    for entity in [&alpha, &beta] {
        assert_eq!(
            node.peek(entity)
                .await?
                .iter()
                .map(|delivery| delivery.sequence.as_u64())
                .collect::<Vec<_>>(),
            vec![6]
        );
    }
    sweep(&node, 4_999)?;
    assert_eq!(peek(&mut management, "not-due").await?.len(), 2);
    sweep(&node, 5_000)?;
    assert!(peek(&mut management, "activated-parent").await?.is_empty());
    for entity in [&alpha, &beta] {
        assert_eq!(
            node.peek(entity)
                .await?
                .iter()
                .map(|delivery| delivery.sequence.as_u64())
                .collect::<Vec<_>>(),
            vec![7, 8]
        );
    }
    // The earlier immediate copy expired before activation; scheduled TTL did not.
    let old_cancel = cancel(&mut management, "old-handle", &[1]).await?;
    status(&old_cancel, 404);
    let mut first = ClientReceiver::attach(&mut session, "alpha-reader", alpha.as_str()).await?;
    let mut second = ClientReceiver::attach(&mut session, "beta-reader", beta.as_str()).await?;
    for (original, number) in [(&originals[0], 7), (&originals[4], 8)] {
        let a = recv(&mut first).await?;
        let b = recv(&mut second).await?;
        assert_eq!(sequence(a.message()), number);
        assert_eq!(sequence(b.message()), number);
        copy(a.message(), original, 5_000, 3_000);
        copy(b.message(), original, 5_000, 4_000);
        first.accept(&a).await?;
        node.wait_len(&alpha, (8 - number) as usize).await?;
        assert_eq!(node.peek(&beta).await?.len(), (9 - number) as usize);
        second.accept(&b).await?;
        node.wait_len(&beta, (8 - number) as usize).await?;
    }
    connection.close().await?;
    Ok(())
}

async fn named_receiver(
    session: &mut ClientSession,
    name: &str,
    entity: &EntityPath,
    id: &str,
) -> TestResult<ClientReceiver> {
    let mut filter = FilterSet::default();
    filter.insert(
        Symbol::from(protocol_amqp::SESSION_FILTER),
        Value::String(id.into()),
    );
    Ok(ClientReceiver::builder()
        .name(name)
        .source(
            Source::builder()
                .address(entity.as_str())
                .filter(filter)
                .build(),
        )
        .attach(session)
        .await?)
}

async fn mixed_scheduled_sessions_use_activation_time_members_and_independent_shadows<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, "Orders", TopicConfig::default()).await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut management = Management::attach(&mut session, "topic", "Orders/$management").await?;
    let mut originals = [
        scheduled(rich(0), 5_000),
        scheduled(rich(1), 5_000),
        scheduled(rich(2), 5_000),
    ];
    originals[1]
        .properties
        .as_mut()
        .expect("properties")
        .group_id = Some("A".into());
    originals[2]
        .properties
        .as_mut()
        .expect("properties")
        .group_id = Some("B".into());
    assert_eq!(
        schedule(&mut management, "mixed-schedule", &originals).await?,
        vec![1, 2, 3]
    );
    assert_eq!(
        peek(&mut management, "parent-before-members").await?.len(),
        3
    );
    // Resolving membership at activation is this implementation's local policy.
    let alpha = subscription(
        &node,
        "Alpha",
        SubscriptionConfig {
            requires_session: true,
            ..SubscriptionConfig::default()
        },
    )
    .await?;
    let beta = subscription(
        &node,
        "beta",
        SubscriptionConfig {
            requires_session: true,
            ..SubscriptionConfig::default()
        },
    )
    .await?;
    let ordinary = node.subscription("ordinary").await?;
    for entity in [&alpha, &beta, &ordinary] {
        assert!(node.peek(entity).await?.is_empty());
    }
    sweep(&node, 5_000)?;
    assert!(
        peek(&mut management, "parent-after-fanout")
            .await?
            .is_empty()
    );
    for (prefix, entity) in [("alpha", &alpha), ("beta", &beta)] {
        for (index, id) in [(1, "A"), (2, "B")] {
            let stage = format!("{prefix}/{id}");
            let mut receiver = at_stage(
                &format!("{stage} attach"),
                named_receiver(&mut session, &format!("{prefix}-{id}"), entity, id),
            )
            .await?;
            let delivery = at_stage(&format!("{stage} receive"), recv(&mut receiver)).await?;
            assert_eq!(sequence(delivery.message()), index as i64 + 4);
            copy(delivery.message(), &originals[index], 5_000, 50_000);
            at_stage(&format!("{stage} accept"), receiver.accept(&delivery)).await?;
            at_stage(&format!("{stage} close"), receiver.close()).await?;
            at_stage(
                &format!("{stage} release"),
                wait_released(&node, entity, id),
            )
            .await?;
        }
        at_stage(
            &format!("{prefix} completed copies"),
            node.wait_len(entity, 0),
        )
        .await?;
        let shadow = entity.dead_letter_queue()?;
        let mut receiver =
            ClientReceiver::attach(&mut session, format!("{prefix}-shadow"), shadow.as_str())
                .await?;
        let delivery = at_stage(&format!("{prefix} SDLQ receive"), recv(&mut receiver)).await?;
        assert_eq!(sequence(delivery.message()), 4);
        assert_eq!(delivery.message().body, originals[0].body);
        scheduled_time(delivery.message(), &originals[0]);
        let properties = delivery
            .message()
            .properties
            .as_ref()
            .expect("shadow properties");
        assert_eq!(properties.group_id, None);
        assert_eq!(properties.absolute_expiry_time, None);
        assert_eq!(
            delivery
                .message()
                .header
                .as_ref()
                .expect("shadow header")
                .ttl,
            None
        );
        let application = delivery
            .message()
            .application_properties
            .as_ref()
            .expect("shadow reason");
        assert_eq!(
            application.get("DeadLetterReason"),
            Some(&Value::String("Session ID is null".into()))
        );
        assert_eq!(
            application.get("DeadLetterErrorDescription"),
            Some(&Value::String(
                "Session enabled entity doesn't allow a message whose session identifier is null."
                    .into()
            ))
        );
        at_stage(&format!("{prefix} SDLQ accept"), receiver.accept(&delivery)).await?;
        at_stage(
            &format!("{prefix} SDLQ completed copy"),
            node.wait_len(&shadow, 0),
        )
        .await?;
        at_stage(&format!("{prefix} SDLQ close"), receiver.close()).await?;
    }
    let mut receiver = ClientReceiver::attach(&mut session, "ordinary", ordinary.as_str()).await?;
    for (index, original) in originals.iter().enumerate() {
        let delivery = at_stage(&format!("ordinary/{index} receive"), recv(&mut receiver)).await?;
        assert_eq!(sequence(delivery.message()), index as i64 + 4);
        copy(delivery.message(), original, 5_000, 50_000);
        at_stage(
            &format!("ordinary/{index} accept"),
            receiver.accept(&delivery),
        )
        .await?;
    }
    at_stage("ordinary completed copies", node.wait_len(&ordinary, 0)).await?;
    at_stage("mixed scheduling connection close", connection.close()).await?;
    Ok(())
}

async fn cancellation_keeps_topic_dedup_history_across_scheduled_and_immediate_ingress<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(
        provider,
        "Orders",
        TopicConfig {
            requires_duplicate_detection: true,
            duplicate_detection_history_time_window_millis: 20_000,
            ..TopicConfig::default()
        },
    )
    .await?;
    let alpha = node.subscription("Alpha").await?;
    let beta = node.subscription("beta").await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut sender = ClientSender::attach(&mut session, "publisher", node.topic.as_str()).await?;
    let mut management = Management::attach(&mut session, "topic", "Orders/$management").await?;
    let cancelled = scheduled(rich(0), 5_000);
    let cancellation = schedule(
        &mut management,
        "original-schedule",
        std::slice::from_ref(&cancelled),
    )
    .await?;
    accepted(sender.send(rich(0)).await?);
    for entity in [&alpha, &beta] {
        assert!(node.peek(entity).await?.is_empty());
    }
    status(
        &cancel(&mut management, "cancel-original", &cancellation).await?,
        200,
    );
    assert_eq!(
        schedule(&mut management, "duplicate-after-cancel", &[cancelled])
            .await?
            .len(),
        1
    );
    assert!(peek(&mut management, "cancelled-history").await?.is_empty());
    accepted(sender.send(rich(1)).await?);
    assert_eq!(
        schedule(
            &mut management,
            "scheduled-duplicate-of-immediate",
            &[scheduled(rich(1), 5_000)]
        )
        .await?
        .len(),
        1
    );
    assert!(peek(&mut management, "immediate-history").await?.is_empty());
    let retained = scheduled(rich(2), 5_000);
    let retained_handle = schedule(
        &mut management,
        "new-schedule",
        std::slice::from_ref(&retained),
    )
    .await?[0];
    accepted(sender.send(retained.clone()).await?);
    assert_eq!(peek(&mut management, "scheduled-history").await?.len(), 1);
    sweep(&node, 5_000)?;
    assert!(
        peek(&mut management, "history-bypassed-at-activation")
            .await?
            .is_empty()
    );
    let mut first = ClientReceiver::attach(&mut session, "alpha", alpha.as_str()).await?;
    let mut second = ClientReceiver::attach(&mut session, "beta", beta.as_str()).await?;
    for (index, original) in [rich(1), retained].into_iter().enumerate() {
        let a = recv(&mut first).await?;
        let b = recv(&mut second).await?;
        assert_eq!(a.message().body, original.body);
        assert_eq!(b.message().body, original.body);
        assert_eq!(sequence(a.message()), sequence(b.message()));
        if index == 1 {
            assert!(sequence(a.message()) > retained_handle);
        }
        first.accept(&a).await?;
        second.accept(&b).await?;
    }
    node.wait_len(&alpha, 0).await?;
    node.wait_len(&beta, 0).await?;
    connection.close().await?;
    Ok(())
}

macro_rules! backends {
    ($($case:ident),+ $(,)?) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 8, super::$case(testkit::MemoryProvider::new())).await? })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 8, super::$case(testkit::DurableProvider::temporary()?)).await? })+ }
    };
}

backends!(
    parent_handles_cancel_and_activate_into_fresh_shared_copies,
    mixed_scheduled_sessions_use_activation_time_members_and_independent_shadows,
    cancellation_keeps_topic_dedup_history_across_scheduled_and_immediate_ingress,
);
