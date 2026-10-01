use amqp::{Begin, FilterSet, Source};

use super::*;

fn message(text: &str, session: Option<&str>) -> Message {
    Message {
        properties: Some(Properties {
            message_id: Some(format!("id-{text}").into()),
            subject: Some("websocket-rich".into()),
            group_id: session.map(str::to_owned),
            ..Properties::default()
        }),
        application_properties: Some(
            ApplicationProperties::builder()
                .insert("member", 7_i32)
                .insert("nullable", Value::Null)
                .build(),
        ),
        body: Body::Data(vec![text.as_bytes().to_vec().into()]),
        ..Message::default()
    }
}

fn body(message: &Message) -> &[u8] {
    let Body::Data(parts) = &message.body else {
        panic!("data body")
    };
    assert_eq!(parts.len(), 1);
    parts[0].as_ref()
}

fn sequence(message: &Message) -> u64 {
    let value = message
        .message_annotations
        .as_ref()
        .expect("sequence annotations")
        .get(Symbol::from("x-opt-sequence-number"))
        .expect("sequence");
    match value {
        Value::Long(value) => *value as u64,
        Value::Ulong(value) => *value,
        other => panic!("sequence {other:?}"),
    }
}

fn accepted(outcome: Outcome) {
    assert!(matches!(outcome, Outcome::Accepted(_)), "send {outcome:?}");
}

async fn sender(session: &mut ClientSession, name: &str, path: &str) -> TestResult<ClientSender> {
    Ok(timeout(DEADLINE, ClientSender::attach(session, name, path)).await??)
}

async fn receiver(
    session: &mut ClientSession,
    name: &str,
    path: &str,
) -> TestResult<ClientReceiver> {
    Ok(timeout(DEADLINE, ClientReceiver::attach(session, name, path)).await??)
}

pub(super) async fn rich_queue_roundtrip<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, false, false).await?;
    let mut connection = node.connect().await?;
    let mut outgoing = timeout(DEADLINE, connection.begin()).await??;
    let mut incoming = timeout(DEADLINE, connection.begin()).await??;
    let mut publisher = sender(&mut outgoing, "publish", "orders").await?;
    let mut consumer = receiver(&mut incoming, "consume", "orders").await?;
    accepted(timeout(DEADLINE, publisher.send(message("rich", None))).await??);
    let first = timeout(DEADLINE, consumer.recv()).await??;
    assert_eq!(body(first.message()), b"rich");
    assert_eq!(
        first
            .message()
            .properties
            .as_ref()
            .expect("properties")
            .subject
            .as_deref(),
        Some("websocket-rich")
    );
    assert_eq!(
        first
            .message()
            .application_properties
            .as_ref()
            .expect("application properties")
            .get("member"),
        Some(&Value::Int(7))
    );
    assert_eq!(sequence(first.message()), 1);
    timeout(DEADLINE, consumer.release(&first)).await??;
    let second = timeout(DEADLINE, consumer.recv()).await??;
    assert_eq!(sequence(second.message()), 1);
    assert_eq!(body(second.message()), b"rich");
    assert_eq!(
        second
            .message()
            .header
            .as_ref()
            .expect("delivery header")
            .delivery_count,
        1
    );
    timeout(DEADLINE, consumer.accept(&second)).await??;
    node.wait_removed("orders", 1).await?;
    timeout(DEADLINE, consumer.close()).await??;
    let mut second_mode = timeout(
        DEADLINE,
        ClientReceiver::builder()
            .name("second-mode")
            .source("orders")
            .receiver_settle_mode(amqp::ReceiverSettleMode::Second)
            .attach(&mut incoming),
    )
    .await??;
    let large = "x".repeat(128 * 1024);
    let mut large_message = message(&large, None);
    large_message
        .properties
        .as_mut()
        .expect("properties")
        .message_id = Some("large-websocket-chunks".into());
    accepted(timeout(DEADLINE, publisher.send(large_message)).await??);
    let delivery = timeout(DEADLINE, second_mode.recv()).await??;
    assert_eq!(
        body(delivery.message()),
        large.as_bytes(),
        "frames cross multiple 16 KiB WS writes"
    );
    assert_eq!(sequence(delivery.message()), 2);
    timeout(DEADLINE, second_mode.accept(&delivery)).await??;
    node.wait_removed("orders", 2).await?;
    timeout(DEADLINE, connection.close()).await??;
    node.reusable(None).await?;
    Ok(())
}

pub(super) async fn topic_subscriptions_settle_independently<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, false, false).await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut publisher = sender(&mut session, "topic-publish", "Topic").await?;
    let mut alpha = receiver(&mut session, "alpha", "Topic/SUBSCRIPTIONS/Alpha").await?;
    let mut beta = receiver(&mut session, "beta", "Topic/subscriptions/beta").await?;
    accepted(timeout(DEADLINE, publisher.send(message("two-copies", None))).await??);
    let a = timeout(DEADLINE, alpha.recv()).await??;
    let b = timeout(DEADLINE, beta.recv()).await??;
    assert_eq!(sequence(a.message()), 1);
    assert_eq!(sequence(b.message()), 1);
    assert_eq!(body(a.message()), body(b.message()));
    timeout(DEADLINE, alpha.accept(&a)).await??;
    node.wait_removed("Topic/subscriptions/Alpha", 1).await?;
    assert!(
        StateMachine::new(node.store.clone())
            .message(
                &node.namespace,
                &EntityPath::new("Topic/subscriptions/beta")?,
                SequenceNumber::new(1)
            )?
            .is_some()
    );
    timeout(DEADLINE, beta.reject(&b, None)).await??;
    node.wait_removed("Topic/subscriptions/beta", 1).await?;
    let mut dlq = receiver(
        &mut session,
        "beta-dead",
        "Topic/subscriptions/beta/$DEADLETTERQUEUE",
    )
    .await?;
    let dead = timeout(DEADLINE, dlq.recv()).await??;
    assert_eq!(body(dead.message()), b"two-copies");
    assert!(
        dead.message()
            .application_properties
            .as_ref()
            .expect("dead-letter properties")
            .get("DeadLetterReason")
            .is_some()
    );
    timeout(DEADLINE, dlq.accept(&dead)).await??;
    node.wait_removed("Topic/subscriptions/beta/$deadletterqueue", 1)
        .await?;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn session_receiver(
    session: &mut ClientSession,
    name: &str,
    id: &str,
) -> TestResult<ClientReceiver> {
    let mut filter = FilterSet::default();
    filter.insert(
        Symbol::from(protocol_amqp::SESSION_FILTER),
        Value::String(id.into()),
    );
    Ok(timeout(
        DEADLINE,
        ClientReceiver::builder()
            .name(name)
            .source(Source::builder().address("sessions").filter(filter).build())
            .attach(session),
    )
    .await??)
}

pub(super) async fn session_fifo_and_release<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, false, false).await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut publisher = sender(&mut session, "session-publish", "sessions").await?;
    for (text, id) in [("other", "B"), ("first", "A"), ("second", "A")] {
        accepted(timeout(DEADLINE, publisher.send(message(text, Some(id)))).await??);
    }
    let mut consumer = session_receiver(&mut session, "A-owner", "A").await?;
    let grant = consumer
        .source()
        .as_ref()
        .expect("source")
        .filter
        .as_ref()
        .expect("filter")
        .get(&Symbol::from(protocol_amqp::SESSION_FILTER));
    assert_eq!(grant, Some(&Value::String("A".into())));
    for (number, text) in [(2, "first"), (3, "second")] {
        let delivery = timeout(DEADLINE, consumer.recv()).await??;
        assert_eq!(sequence(delivery.message()), number);
        assert_eq!(body(delivery.message()), text.as_bytes());
        timeout(DEADLINE, consumer.accept(&delivery)).await??;
        node.wait_removed("sessions", number).await?;
    }
    timeout(DEADLINE, consumer.close()).await??;
    timeout(DEADLINE, async {
        loop {
            if StateMachine::new(node.store.clone())
                .session(
                    &node.namespace,
                    &EntityPath::new("sessions")?,
                    &domain::SessionId::new("A")?,
                )?
                .is_some_and(|session| session.lock.is_none())
            {
                return Ok::<(), Box<dyn Error>>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    let mut replacement = session_receiver(&mut session, "B-owner", "B").await?;
    let delivery = timeout(DEADLINE, replacement.recv()).await??;
    assert_eq!(sequence(delivery.message()), 1);
    timeout(DEADLINE, replacement.accept(&delivery)).await??;
    node.wait_removed("sessions", 1).await?;
    timeout(DEADLINE, replacement.close()).await??;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

pub(super) async fn admitted_links_cannot_follow_recreated_entities<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, false, false).await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut old = sender(&mut session, "old", "orders").await?;
    node.submit(
        "orders",
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    )
    .await?;
    node.submit(
        "orders",
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )
    .await?;
    let before = node.snapshot()?;
    let outcome = timeout(DEADLINE, old.send(message("stale", None))).await??;
    let Outcome::Rejected(rejected) = outcome else {
        panic!("stale endpoint outcome {outcome:?}")
    };
    assert_eq!(
        rejected.error.expect("stale refusal").condition.as_symbol(),
        Symbol::from(protocol_amqp::NOT_FOUND)
    );
    assert_eq!(node.snapshot()?, before);
    let mut current = sender(&mut session, "new", "orders").await?;
    accepted(timeout(DEADLINE, current.send(message("current", None))).await??);
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

pub(super) async fn split_coalesced_and_fragmented_frames<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, false, false).await?;
    let before = node.snapshot()?;
    let mut peer = RawPeer::new(node.websocket().await?);
    peer.header(ProtocolHeader::AMQP).await?;
    let mut coalesced = encode_frame(&amqp_frame(0, Performative::Open(Open::new("coalesced"))))?;
    coalesced.extend(encode_frame(&amqp_frame(
        5,
        Performative::Begin(Begin::default()),
    ))?);
    ws_send(&mut peer.socket, WsMessage::Binary(coalesced.into())).await?;
    assert!(matches!(
        peer.read().await?,
        Frame::Amqp {
            performative: Some(Performative::Open(_)),
            ..
        }
    ));
    assert!(
        matches!(peer.read().await?, Frame::Amqp { performative: Some(Performative::Begin(begin)), .. } if begin.remote_channel == Some(5))
    );
    let bytes = encode_frame(&amqp_frame(7, Performative::Begin(Begin::default())))?;
    ws_send(
        &mut peer.socket,
        WsMessage::Binary(bytes[..3].to_vec().into()),
    )
    .await?;
    ws_send(
        &mut peer.socket,
        WsMessage::Binary(bytes[3..].to_vec().into()),
    )
    .await?;
    assert!(
        matches!(peer.read().await?, Frame::Amqp { performative: Some(Performative::Begin(begin)), .. } if begin.remote_channel == Some(7))
    );
    let bytes = encode_frame(&amqp_frame(9, Performative::Begin(Begin::default())))?;
    ws_send(
        &mut peer.socket,
        WsMessage::Frame(WsFrame::message(
            bytes[..5].to_vec(),
            OpCode::Data(Data::Binary),
            false,
        )),
    )
    .await?;
    ws_send(
        &mut peer.socket,
        WsMessage::Ping(b"mid-fragment".to_vec().into()),
    )
    .await?;
    ws_send(
        &mut peer.socket,
        WsMessage::Frame(WsFrame::message(
            bytes[5..].to_vec(),
            OpCode::Data(Data::Continue),
            true,
        )),
    )
    .await?;
    assert!(
        matches!(peer.read().await?, Frame::Amqp { performative: Some(Performative::Begin(begin)), .. } if begin.remote_channel == Some(9))
    );
    if peer.pongs.is_empty() {
        match ws_next(&mut peer.socket).await? {
            WsMessage::Pong(bytes) => peer.pongs.push(bytes.to_vec()),
            other => panic!("interleaved Ping must receive Pong: {other:?}"),
        }
    }
    assert_eq!(peer.pongs, vec![b"mid-fragment".to_vec()]);
    peer.close().await?;
    assert_eq!(
        node.snapshot()?,
        before,
        "transport and sessions without links do not mutate domain"
    );
    node.reusable(None).await?;
    Ok(())
}

pub(super) async fn close_and_drop_release_admission<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, false, false).await?;
    let before = node.snapshot()?;
    let mut peer = RawPeer::new(node.websocket().await?);
    peer.open().await?;
    peer.close().await?;
    node.reusable(None).await?;
    let mut abrupt = RawPeer::new(node.websocket().await?);
    abrupt.open().await?;
    drop(abrupt);
    node.reusable(None).await?;
    assert_eq!(node.snapshot()?, before);
    Ok(())
}

pub(super) async fn wss_trust_and_plain_authentication<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, true, true).await?;
    let before = node.snapshot()?;
    assert!(
        node.transport(false).await.is_err(),
        "untrusted WSS certificate accepted"
    );
    node.reusable(Some(plain(KEY))).await?;
    assert!(
        node.connect_with(Some(plain("wrong-key"))).await.is_err(),
        "wrong SASL key accepted"
    );
    node.reusable(Some(plain(KEY))).await?;
    assert_eq!(
        node.snapshot()?,
        before,
        "TLS/SASL negotiation changes no domain state"
    );
    let mut connection = node.connect_with(Some(plain(KEY))).await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut publisher = sender(&mut session, "wss-publish", "orders").await?;
    let mut consumer = receiver(&mut session, "wss-consume", "orders").await?;
    accepted(timeout(DEADLINE, publisher.send(message("secure", None))).await??);
    let delivery = timeout(DEADLINE, consumer.recv()).await??;
    assert_eq!(body(delivery.message()), b"secure");
    timeout(DEADLINE, consumer.accept(&delivery)).await??;
    node.wait_removed("orders", 1).await?;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}
