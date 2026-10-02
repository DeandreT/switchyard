use amqp::{ApplicationProperties, ClientConnection, ClientReceiver, ClientSender, OrderedMap};
use domain::{
    Command, CommandKind, CommandOutcome, EntityPath, NamespaceName, SequenceNumber, StateMachine,
};

use super::*;

fn stage<T, E: std::fmt::Display>(name: &str, result: Result<T, E>) -> TestResult<T> {
    result.map_err(|error| std::io::Error::other(format!("{name}: {error}")).into())
}

async fn ordinary_refuses_coordinator(peer: &mut Peer) -> TestResult {
    peer.begin(CONTROL).await?;
    let mut request = Peer::attach_request(CONTROL, CONTROL_HANDLE, "", Role::Sender);
    request.target = Some(Coordinator::default().into());
    peer.attach(CONTROL, request).await?;
    let (channel, response) = peer.control().await?;
    assert_eq!(channel, peer.local(CONTROL));
    let Performative::End(end) = response else {
        return Err("ordinary endpoint admitted transaction control".into());
    };
    assert_eq!(
        end.error
            .expect("default unsupported refusal")
            .condition
            .as_symbol()
            .as_str(),
        "amqp:not-implemented"
    );
    peer.send(
        CONTROL,
        Performative::End(amqp::End { error: None }),
        vec![],
    )
    .await?;
    Ok(())
}

async fn seed(node: &BinaryNode) -> TestResult {
    node.create_queue().await?;
    let mut ordinary = node.peer(node.addresses.ordinary).await?;
    ordinary.producer(POST, "orders").await?;
    ordinary_refuses_coordinator(&mut ordinary).await?;
    // The independent ordinary link remains usable after the scoped refusal.
    let id = ordinary
        .transfer(
            POST,
            POST_HANDLE,
            None,
            0,
            &message("held-original", b"held-body"),
        )
        .await?;
    let disposition = ordinary.disposition(POST, Role::Receiver, id).await?;
    assert!(disposition.settled && matches!(disposition.state, Some(DeliveryState::Accepted(_))));
    ordinary.barrier(POST).await?;
    ordinary.close().await?;
    Ok(())
}

pub(super) async fn default_listener() -> TestResult {
    let node = BinaryNode::start(false, false, false).await?;
    seed(&node).await?;
    let messages = peek(&node).await?;
    assert_contents(&messages, &[("held-original", b"held-body")]);
    let (output, directory) = node.kill().await?;
    assert!(directory.is_none());
    assert!(
        !output
            .stdout
            .contains("accepting experimental atomic messaging connections")
    );
    Ok(())
}

pub(super) async fn mixed_roundtrip(node: &BinaryNode) -> TestResult {
    stage("seed ordinary queue", seed(node).await)?;
    let mut peer = stage(
        "connect experimental socket",
        node.peer(node.addresses.experimental.expect("explicit socket"))
            .await,
    )?;
    let original = stage("admit actual held original", peer.setup().await)?;
    assert_eq!(original.id, 0);
    assert_eq!(
        original
            .message
            .properties
            .as_ref()
            .and_then(|properties| properties.message_id.clone()),
        Some("held-original".into())
    );
    assert!(
        matches!(&original.message.body, Body::Data(parts) if parts.len() == 1 && parts[0].as_ref() == b"held-body")
    );
    let sequence = original
        .message
        .message_annotations
        .as_ref()
        .and_then(|annotations| annotations.get(Symbol::from("x-opt-sequence-number")));
    assert_eq!(
        sequence,
        Some(&Value::Long(1)),
        "wire alias is not the canonical queue sequence"
    );
    let transaction = stage("declare rollback", peer.declare().await)?;
    peer.retire(&original, &transaction).await?;
    peer.provisional(RECEIVE, Role::Sender, original.id, &transaction)
        .await?;
    let control = peer.discharge(&transaction, true).await?;
    stage(
        "rollback rearm without source ACK or resend",
        peer.rollback_control(&original, &transaction, control)
            .await,
    )?;
    peer.barrier(RECEIVE).await?;
    assert_contents(&peek(node).await?, &[("held-original", b"held-body")]);
    let transaction = stage("declare mixed commit", peer.declare().await)?;
    peer.retire(&original, &transaction).await?;
    peer.provisional(RECEIVE, Role::Sender, original.id, &transaction)
        .await?;
    let input = batch(&[message("mixed-a", b"new-a"), message("mixed-b", b"new-b")])?;
    let post = peer
        .transfer(
            POST,
            POST_HANDLE,
            Some(&transaction),
            protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
            &input,
        )
        .await?;
    peer.provisional(POST, Role::Receiver, post, &transaction)
        .await?;
    let control = peer.discharge(&transaction, false).await?;
    stage(
        "exact resources before final control ACK",
        peer.committed(&original, post, control).await,
    )?;
    peer.barrier(RECEIVE).await?;
    // Peek includes any legitimate fetch-next lock; no second delivery is consumed here.
    assert_contents(
        &peek(node).await?,
        &[("mixed-a", b"new-a"), ("mixed-b", b"new-b")],
    );
    peer.close().await?;
    Ok(())
}

pub(super) async fn mixed_binary(durable: bool) -> TestResult {
    let node = BinaryNode::start(durable, true, false).await?;
    mixed_roundtrip(&node).await?;
    let (output, directory) = node.kill().await?;
    assert!(output.stdout.contains("accepting AMQP connections"));
    assert!(
        output
            .stdout
            .contains("accepting experimental atomic messaging connections")
    );
    if durable {
        let directory = directory.expect("Fjall process directory");
        let reopened = storage::FjallStore::open(directory.path())?;
        let machine = StateMachine::new(reopened);
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        assert!(
            machine
                .message(&namespace, &entity, SequenceNumber::new(1))?
                .is_none()
        );
        let outcome = machine.apply(&Command::new(
            namespace.clone(),
            entity.clone(),
            machine.last_applied_time()?,
            CommandKind::Peek {
                from_sequence: SequenceNumber::new(0),
                max_messages: 8,
                session_id: None,
            },
        ))?;
        let CommandOutcome::Peeked(deliveries) = outcome else {
            return Err("pure durable Peek expected".into());
        };
        assert_eq!(
            deliveries
                .iter()
                .map(|delivery| (
                    delivery.sequence.as_u64(),
                    delivery.message_id.as_str(),
                    delivery.body.as_slice()
                ))
                .collect::<Vec<_>>(),
            [
                (2, "mixed-a", b"new-a".as_slice()),
                (3, "mixed-b", b"new-b".as_slice())
            ]
        );
        for sequence in [2, 3] {
            assert!(matches!(
                machine
                    .message(&namespace, &entity, SequenceNumber::new(sequence))?
                    .expect("acknowledged posted record")
                    .state,
                domain::MessageState::Ready | domain::MessageState::Locked { .. }
            ));
        }
    } else {
        assert!(directory.is_none());
    }
    Ok(())
}

pub(super) async fn peek(node: &BinaryNode) -> TestResult<Vec<Message>> {
    let mut connection = match &node.security {
        Some(security) => {
            super::security::management_connection(node.addresses.ordinary, security).await?
        }
        None => {
            timeout(
                DEADLINE,
                ClientConnection::builder()
                    .container_id("binary-readonly-browser")
                    .open(&format!("amqp://{}", node.addresses.ordinary)),
            )
            .await??
        }
    };
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let reply = "binary-readonly-replies";
    let address = "orders/$management";
    let mut receiver = timeout(
        DEADLINE,
        ClientReceiver::builder()
            .name("binary-browser-response")
            .source(address)
            .target(reply)
            .attach(&mut session),
    )
    .await??;
    let mut sender = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "binary-browser-request", address),
    )
    .await??;
    let body: OrderedMap<Value, Value> = [
        (
            Value::String(protocol_amqp::FROM_SEQUENCE_NUMBER.into()),
            Value::Long(1),
        ),
        (
            Value::String(protocol_amqp::MESSAGE_COUNT.into()),
            Value::Int(8),
        ),
    ]
    .into_iter()
    .collect();
    let request = Message::builder()
        .properties(Properties {
            message_id: Some("binary-peek".into()),
            reply_to: Some(reply.into()),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert(
                    protocol_amqp::OPERATION_PROPERTY,
                    protocol_amqp::PEEK_MESSAGE_OPERATION.to_owned(),
                )
                .build(),
        )
        .body(Body::Value(Value::Map(body)))
        .build();
    assert!(matches!(
        timeout(DEADLINE, sender.send(request)).await??,
        Outcome::Accepted(_)
    ));
    let delivery = timeout(DEADLINE, receiver.recv()).await??;
    let response = delivery.message();
    assert_eq!(
        response
            .properties
            .as_ref()
            .and_then(|properties| properties.correlation_id.clone()),
        Some("binary-peek".into())
    );
    assert_eq!(
        response
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY)),
        Some(&Value::Int(200))
    );
    let Body::Value(Value::Map(body)) = &response.body else {
        return Err("management response map expected".into());
    };
    let Some(Value::List(entries)) = body.get(&Value::String(protocol_amqp::MESSAGES.into()))
    else {
        return Err("management message list expected".into());
    };
    let mut messages = Vec::new();
    for entry in entries {
        let Value::Map(entry) = entry else {
            return Err("management message entry expected".into());
        };
        assert!(
            entry
                .get(&Value::String(protocol_amqp::LOCK_TOKEN.into()))
                .is_none()
        );
        let Some(Value::Binary(bytes)) = entry.get(&Value::String(protocol_amqp::MESSAGE.into()))
        else {
            return Err("management encoded message expected".into());
        };
        messages.push(decode_message(bytes)?);
    }
    timeout(DEADLINE, receiver.accept(&delivery)).await??;
    timeout(DEADLINE, connection.close()).await??;
    Ok(messages)
}

fn assert_contents(messages: &[Message], expected: &[(&str, &[u8])]) {
    assert_eq!(messages.len(), expected.len());
    for (message, (id, body)) in messages.iter().zip(expected) {
        assert_eq!(
            message
                .properties
                .as_ref()
                .and_then(|properties| properties.message_id.clone()),
            Some((*id).into())
        );
        assert!(
            matches!(&message.body, Body::Data(parts) if parts.len() == 1 && parts[0].as_ref() == *body)
        );
    }
}
