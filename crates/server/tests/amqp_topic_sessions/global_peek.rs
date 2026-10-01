use amqp::decode_message;

use super::*;

fn at_stage<T, E: std::fmt::Display>(stage: &str, result: Result<T, E>) -> TestResult<T> {
    result.map_err(|error| std::io::Error::other(format!("{stage}: {error}")).into())
}

fn peek_body(from: u64, count: i32, session: Option<Value>) -> OrderedMap<Value, Value> {
    let mut body = map([
        (
            protocol_amqp::FROM_SEQUENCE_NUMBER,
            Value::Long(from as i64),
        ),
        (protocol_amqp::MESSAGE_COUNT, Value::Int(count)),
    ]);
    if let Some(session) = session {
        body.insert(Value::String(protocol_amqp::SESSION_ID.into()), session);
    }
    body
}

fn peeked(response: &Message) -> TestResult<Vec<Message>> {
    status(response, 200);
    let Some(Value::List(messages)) =
        response_map(response).get(&Value::String(protocol_amqp::MESSAGES.into()))
    else {
        panic!("peek message list")
    };
    messages
        .iter()
        .map(|entry| {
            let Value::Map(entry) = entry else {
                panic!("peek message entry")
            };
            let Some(Value::Binary(encoded)) =
                entry.get(&Value::String(protocol_amqp::MESSAGE.into()))
            else {
                panic!("peek encoded message")
            };
            assert!(
                entry
                    .get(&Value::String(protocol_amqp::LOCK_TOKEN.into()))
                    .is_none(),
                "peek granted a delivery receipt"
            );
            Ok(decode_message(encoded)?)
        })
        .collect()
}

fn contents(messages: &[Message], expected: &[(u64, &str, &str, i32)]) {
    assert_eq!(messages.len(), expected.len());
    for (message, (number, text, id, state)) in messages.iter().zip(expected) {
        assert_eq!(sequence(message), *number);
        assert_eq!(body(message), text.as_bytes());
        assert_eq!(group(message), Some(*id));
        assert_eq!(
            message
                .message_annotations
                .as_ref()
                .and_then(|annotations| annotations
                    .get(Symbol::from(protocol_amqp::MESSAGE_STATE_ANNOTATION))),
            Some(&Value::Int(*state))
        );
        assert!(
            message
                .message_annotations
                .as_ref()
                .and_then(|annotations| annotations.get(Symbol::from("x-opt-locked-until")))
                .is_none(),
            "peek exposed a destructive lock"
        );
    }
}

async fn browse<P: StoreProvider>(
    node: &Node<P>,
    management: &mut Management,
    from: u64,
    count: i32,
    session: Option<Value>,
    stage: &str,
) -> TestResult<Vec<Message>> {
    let snapshot = node.snapshot()?;
    let submissions = node.submissions();
    let response = at_stage(
        stage,
        management
            .request_unassociated(
                stage,
                protocol_amqp::PEEK_MESSAGE_OPERATION,
                peek_body(from, count, session),
            )
            .await,
    )?;
    let messages = peeked(&response)?;
    assert_eq!(
        node.snapshot()?,
        snapshot,
        "{stage}: browse must not alter messages, holds, counters, indexes or persisted time"
    );
    assert_eq!(
        node.submissions(),
        submissions + 1,
        "{stage}: valid peek keeps its ordinary command admission"
    );
    Ok(messages)
}

async fn required_entity_management_only_browse<P: StoreProvider>(
    provider: P,
    subscription: bool,
) -> TestResult {
    let node = at_stage(
        "start-node",
        Node::start_for_peek(provider, !subscription).await,
    )?;
    let entity = if subscription {
        node.alpha.clone()
    } else {
        EntityPath::new("Sessions")?
    };
    let publisher = if subscription { &node.topic } else { &entity };
    let mut connection = at_stage("connect", node.connect().await)?;
    let mut session = at_stage("begin-session", timeout(DEADLINE, connection.begin()).await)??;
    let mut sender = at_stage(
        "attach-publisher",
        timeout(
            DEADLINE,
            ClientSender::attach(&mut session, "browse-publisher", publisher.as_str()),
        )
        .await,
    )??;
    for (text, id) in [("A-first", "A"), ("B", "B"), ("A-second", "A")] {
        accepted(at_stage(
            &format!("publish-{text}"),
            timeout(DEADLINE, sender.send(message(text, Some(id)))).await,
        )??);
    }
    let mut management = at_stage(
        "attach-global-browser",
        Management::attach(
            &mut session,
            "global-browser",
            &format!("{entity}/$Management"),
        )
        .await,
    )?;
    // No data receiver, associated link, or held session is needed to browse.
    contents(
        &browse(&node, &mut management, 1, 2, None, "first-global-page").await?,
        &[(1, "A-first", "A", 0), (2, "B", "B", 0)],
    );
    contents(
        &browse(&node, &mut management, 3, 2, None, "resume-global-page").await?,
        &[(3, "A-second", "A", 0)],
    );
    assert!(
        browse(&node, &mut management, 4, 2, None, "global-exhausted")
            .await?
            .is_empty()
    );
    assert!(
        StateMachine::new(node.store.clone())
            .session(&node.namespace, &entity, &domain::SessionId::new("A")?)?
            .is_none()
    );
    contents(
        &browse(
            &node,
            &mut management,
            1,
            8,
            Some(Value::String("A".into())),
            "named-before-hold",
        )
        .await?,
        &[(1, "A-first", "A", 0), (3, "A-second", "A", 0)],
    );
    for (index, malformed) in [
        Value::Null,
        Value::Int(1),
        Value::String(String::new()),
        Value::String("x".repeat(129)),
    ]
    .into_iter()
    .enumerate()
    {
        let snapshot = node.snapshot()?;
        let submissions = node.submissions();
        let response = at_stage(
            &format!("malformed-session-{index}"),
            management
                .request_unassociated(
                    "malformed-session",
                    protocol_amqp::PEEK_MESSAGE_OPERATION,
                    peek_body(1, 8, Some(malformed)),
                )
                .await,
        )?;
        status(&response, 400);
        assert_eq!(node.submissions(), submissions);
        assert_eq!(node.snapshot()?, snapshot);
    }
    let mut owner = at_stage(
        "attach-A-owner",
        receiving(&mut session, "A-owner", entity.as_str(), Some("A")).await,
    )?;
    let first = at_stage("receive-A-first", recv(&mut owner).await)?;
    assert_eq!(sequence(first.message()), 1);
    at_stage(
        "defer-A-first",
        timeout(
            DEADLINE,
            owner.modify(
                &first,
                Modified {
                    undeliverable_here: Some(true),
                    ..Modified::default()
                },
            ),
        )
        .await,
    )??;
    at_stage(
        "wait-A-first-deferred",
        node.wait_deferred(&entity, 1).await,
    )?;
    let second = at_stage("receive-A-second", recv(&mut owner).await)?;
    assert_eq!(sequence(second.message()), 3);
    let before_hold = node.session(&entity, "A")?;
    node.clock.set(2_000);
    contents(
        &browse(&node, &mut management, 1, 8, None, "global-held-deferred").await?,
        &[
            (1, "A-first", "A", 1),
            (2, "B", "B", 0),
            (3, "A-second", "A", 0),
        ],
    );
    contents(
        &browse(
            &node,
            &mut management,
            1,
            8,
            Some(Value::String("A".into())),
            "named-held-deferred",
        )
        .await?,
        &[(1, "A-first", "A", 1), (3, "A-second", "A", 0)],
    );
    assert_eq!(node.session(&entity, "A")?, before_hold);
    assert!(matches!(
        node.record(&entity, 1)?.expect("deferred A").state,
        domain::MessageState::Deferred
    ));
    assert!(matches!(
        node.record(&entity, 2)?.expect("ready B").state,
        domain::MessageState::Ready
    ));
    assert!(matches!(
        node.record(&entity, 3)?.expect("locked A").state,
        domain::MessageState::Locked { .. }
    ));
    let other = if subscription {
        node.beta.clone()
    } else {
        EntityPath::new("healthy")?
    };
    let mut sibling = at_stage(
        "attach-other-browser",
        Management::attach(
            &mut session,
            "other-browser",
            &format!("{other}/$management"),
        )
        .await,
    )?;
    let other_messages = browse(&node, &mut sibling, 1, 8, None, "other-entity").await?;
    if subscription {
        contents(
            &other_messages,
            &[
                (1, "A-first", "A", 0),
                (2, "B", "B", 0),
                (3, "A-second", "A", 0),
            ],
        );
        assert!(
            StateMachine::new(node.store.clone())
                .session(&node.namespace, &other, &domain::SessionId::new("A")?)?
                .is_none()
        );
    } else {
        assert!(
            other_messages.is_empty(),
            "entity-wide browse crossed into another queue"
        );
    }
    at_stage(
        "complete-A-second",
        timeout(DEADLINE, owner.accept(&second)).await,
    )??;
    at_stage("wait-A-second-removed", node.wait_removed(&entity, 3).await)?;
    at_stage("wait-A-owner-idle", node.wait_waiting(&entity).await)?;
    let deferred = at_stage(
        "retrieve-A-first",
        management
            .request(
                "retrieve-A",
                protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
                "A-owner",
                sequences(1, "A"),
            )
            .await,
    )?;
    status(&deferred, 200);
    let Some(Value::Uuid(lock)) =
        first_entry(&deferred).get(&Value::String(protocol_amqp::LOCK_TOKEN.into()))
    else {
        panic!("deferred receipt")
    };
    let response = at_stage(
        "complete-A-first",
        management
            .request(
                "complete-A",
                protocol_amqp::UPDATE_DISPOSITION_OPERATION,
                "A-owner",
                map([
                    (
                        protocol_amqp::LOCK_TOKENS,
                        Value::Array(Array::from(vec![Value::Uuid(lock.clone())])),
                    ),
                    (
                        protocol_amqp::DISPOSITION_STATUS,
                        Value::String("completed".into()),
                    ),
                ]),
            )
            .await,
    )?;
    status(&response, 200);
    at_stage("wait-A-first-removed", node.wait_removed(&entity, 1).await)?;
    at_stage("close-A-owner", timeout(DEADLINE, owner.close()).await)??;
    at_stage(
        "wait-A-hold-released",
        node.wait_released(&entity, "A").await,
    )?;
    let mut other_session = at_stage(
        "attach-B-owner",
        receiving(&mut session, "B-owner", entity.as_str(), Some("B")).await,
    )?;
    let delivery = at_stage("receive-B", recv(&mut other_session).await)?;
    assert_eq!(body(delivery.message()), b"B");
    at_stage(
        "complete-B",
        timeout(DEADLINE, other_session.accept(&delivery)).await,
    )??;
    at_stage("wait-B-removed", node.wait_removed(&entity, 2).await)?;
    at_stage(
        "close-B-owner",
        timeout(DEADLINE, other_session.close()).await,
    )??;
    at_stage(
        "wait-B-hold-released",
        node.wait_released(&entity, "B").await,
    )?;
    assert!(
        browse(&node, &mut management, 1, 8, None, "empty-after-release")
            .await?
            .is_empty()
    );
    at_stage(
        "close-connection",
        timeout(DEADLINE, connection.close()).await,
    )??;
    Ok(())
}

async fn required_queue_can_be_browsed_without_accepting_a_session<P: StoreProvider>(
    provider: P,
) -> TestResult {
    required_entity_management_only_browse(provider, false).await
}

async fn required_subscription_can_be_browsed_without_accepting_a_session<P: StoreProvider>(
    provider: P,
) -> TestResult {
    required_entity_management_only_browse(provider, true).await
}

for_each_backend!(
    required_queue_can_be_browsed_without_accepting_a_session,
    required_subscription_can_be_browsed_without_accepting_a_session
);
