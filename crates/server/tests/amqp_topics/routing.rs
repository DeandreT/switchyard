use amqp::{
    Attach, Begin, Detach, Flow, Frame, Open, Performative, ProtocolHeader, Role, Source, Target,
    Transfer, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use tokio::net::TcpStream;

use super::*;

struct Peer {
    stream: TcpStream,
    sent: u32,
}

impl Peer {
    async fn connect(address: &str) -> TestResult<Self> {
        let stream = timeout(DEADLINE, TcpStream::connect(address)).await??;
        let mut peer = Self { stream, sent: 0 };
        timeout(
            DEADLINE,
            write_protocol_header(&mut peer.stream, ProtocolHeader::AMQP),
        )
        .await??;
        assert_eq!(
            timeout(DEADLINE, read_protocol_header(&mut peer.stream)).await??,
            ProtocolHeader::AMQP
        );
        peer.send(Performative::Open(Open::new("topic-routing-peer")), vec![])
            .await?;
        assert!(matches!(
            peer.read().await?,
            Frame::Amqp {
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        peer.send(Performative::Begin(Begin::default()), vec![])
            .await?;
        assert!(
            matches!(peer.read().await?, Frame::Amqp { channel: 0, performative: Some(Performative::Begin(begin)), .. } if begin.remote_channel == Some(0))
        );
        Ok(peer)
    }

    async fn send(&mut self, performative: Performative, payload: Vec<u8>) -> TestResult {
        timeout(
            DEADLINE,
            write_frame(
                &mut self.stream,
                &Frame::Amqp {
                    channel: 0,
                    performative: Some(performative),
                    payload,
                },
            ),
        )
        .await??;
        Ok(())
    }

    async fn read(&mut self) -> TestResult<Frame> {
        Ok(timeout(DEADLINE, read_frame(&mut self.stream)).await??)
    }

    async fn attach(&mut self, name: &str, handle: u32, role: Role, address: &str) -> TestResult {
        self.send(
            Performative::Attach(Box::new(Attach {
                name: name.into(),
                handle,
                role: role.clone(),
                snd_settle_mode: amqp::SenderSettleMode::Unsettled,
                rcv_settle_mode: amqp::ReceiverSettleMode::First,
                source: (role == Role::Receiver).then(|| Source {
                    address: Some(address.into()),
                    ..Source::default()
                }),
                target: (role == Role::Sender).then(|| Target {
                    address: Some(address.into()),
                    ..Target::default()
                }),
                initial_delivery_count: (role == Role::Sender).then_some(0),
                unsettled: None,
                incomplete_unsettled: false,
                max_message_size: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            })),
            vec![],
        )
        .await
    }

    async fn refused(
        &mut self,
        name: &str,
        role: Role,
        address: &str,
        condition: &str,
    ) -> TestResult {
        self.attach(name, 1, role.clone(), address).await?;
        let local_handle = loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Attach(attach)),
                    ..
                } => {
                    assert_eq!(attach.name, name);
                    assert_ne!(attach.role, role);
                    break attach.handle;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("own Attach must precede refusal: {other:?}"),
            }
        };
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Detach(detach)),
                    ..
                } => {
                    assert_eq!(detach.handle, local_handle);
                    assert!(detach.closed);
                    assert_eq!(
                        detach.error.expect("refusal reason").condition.as_symbol(),
                        Symbol::from(condition)
                    );
                    break;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("link-local refusal expected: {other:?}"),
            }
        }
        self.send(
            Performative::Detach(Detach {
                handle: 1,
                closed: true,
                error: None,
            }),
            vec![],
        )
        .await?;
        self.barrier().await
    }

    async fn barrier(&mut self) -> TestResult {
        self.send(
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: 2_048,
                next_outgoing_id: self.sent,
                outgoing_window: 2_048,
                echo: true,
                ..Flow::default()
            }),
            vec![],
        )
        .await?;
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Flow(flow)),
                    ..
                } if flow.handle.is_none() && flow.next_incoming_id == Some(self.sent) => {
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("healthy session barrier expected: {other:?}"),
            }
        }
    }

    async fn healthy_sender(&mut self) -> TestResult {
        self.attach("healthy", 0, Role::Sender, "healthy").await?;
        let mut attached = false;
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Attach(attach)),
                    ..
                } => {
                    assert_eq!(attach.name, "healthy");
                    assert_eq!(attach.handle, 0);
                    attached = true;
                }
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Flow(flow)),
                    ..
                } if flow.handle == Some(0) && flow.link_credit.unwrap_or(0) > 0 => {
                    assert!(attached);
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("healthy sender negotiation: {other:?}"),
            }
        }
    }

    async fn publish_healthy(&mut self) -> TestResult {
        let id = self.sent;
        self.send(
            Performative::Transfer(Transfer {
                handle: 0,
                delivery_id: Some(id),
                delivery_tag: Some(id.to_be_bytes().to_vec().into()),
                message_format: Some(0),
                settled: Some(false),
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            }),
            encode_message(&rich(id as usize))?,
        )
        .await?;
        self.sent += 1;
        loop {
            match self.read().await? {
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                } => {
                    assert_eq!(disposition.first, id);
                    assert_eq!(disposition.role, Role::Receiver);
                    assert!(disposition.settled);
                    assert!(matches!(
                        disposition.state,
                        Some(amqp::DeliveryState::Accepted(_))
                    ));
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("healthy transfer refused: {other:?}"),
            }
        }
    }
}

async fn missing_forbidden_and_session_targets_refuse_before_mutation_with_healthy_sibling<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, "orders", TopicConfig::default()).await?;
    node.subscription("Alpha").await?;
    node.submit(
        &node.topic,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("sessions")?,
            config: SubscriptionConfig {
                requires_session: true,
                ..SubscriptionConfig::default()
            },
        },
    )
    .await?;
    let healthy = EntityPath::new("healthy")?;
    node.submit(
        &healthy,
        CommandKind::CreateQueue {
            config: domain::QueueConfig::default(),
        },
    )
    .await?;
    let mut peer = Peer::connect(&node.address).await?;
    peer.healthy_sender().await?;
    for (index, role, address, condition) in [
        (0, Role::Sender, "absent", "amqp:not-found"),
        (
            1,
            Role::Receiver,
            "orders/subscriptions/absent",
            "amqp:not-found",
        ),
        (
            2,
            Role::Receiver,
            "absent/subscriptions/Alpha",
            "amqp:not-found",
        ),
        (
            3,
            Role::Sender,
            "orders/SUBSCRIPTIONS/Alpha",
            "amqp:not-allowed",
        ),
        (
            4,
            Role::Sender,
            "orders/subscriptions/Alpha/$DeadLetterQueue",
            "amqp:not-allowed",
        ),
        (5, Role::Receiver, "orders", "amqp:not-allowed"),
        (
            6,
            Role::Receiver,
            "orders/subscriptions/sessions",
            "amqp:not-implemented",
        ),
        (
            7,
            Role::Receiver,
            "orders/subscriptions/Alpha/extra",
            "amqp:invalid-field",
        ),
        (
            8,
            Role::Receiver,
            "orders/subscriptions/Alpha/subscriptions/leaf",
            "amqp:invalid-field",
        ),
        (
            9,
            Role::Receiver,
            "orders/subscriptions/-bad",
            "amqp:invalid-field",
        ),
    ] {
        let before = node.snapshot()?;
        peer.refused(&format!("refused-{index}"), role, address, condition)
            .await?;
        assert_eq!(
            node.snapshot()?,
            before,
            "{address} must not submit commands or stamp the clock"
        );
    }
    peer.publish_healthy().await?;
    assert_eq!(node.peek(&healthy).await?.len(), 1);
    assert!(
        node.peek(&node.topic.subscription(&SubscriptionName::new("Alpha")?)?)
            .await?
            .is_empty()
    );
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut shadow = ClientReceiver::attach(
        &mut session,
        "session-shadow",
        "orders/subscriptions/sessions/$DeadLetterQueue",
    )
    .await?;
    assert!(
        timeout(Duration::from_millis(150), shadow.recv())
            .await
            .is_err(),
        "the session subscription's ordinary DLQ must stay open and empty"
    );
    shadow.close().await?;
    connection.close().await?;
    Ok(())
}

async fn only_terminal_controls_fold_while_topic_and_member_names_keep_case<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, "a/Subscriptions", TopicConfig::default()).await?;
    let upper = node.subscription("Subscriptions").await?;
    let lower = node.subscription("subscriptions").await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut sender = ClientSender::attach(&mut session, "publisher", "a/Subscriptions").await?;
    accepted(sender.send(rich(0)).await?);
    let address = "a/Subscriptions/SuBsCrIpTiOnS/Subscriptions";
    let mut first = ClientReceiver::attach(&mut session, "upper", address).await?;
    assert_eq!(
        first
            .source()
            .as_ref()
            .and_then(|source| source.address.as_deref()),
        Some(address)
    );
    let a = recv(&mut first).await?;
    assert_eq!(sequence(a.message()), 1);
    first.accept(&a).await?;
    node.wait_len(&upper, 0).await?;
    assert_eq!(node.peek(&lower).await?.len(), 1);
    let mut second = ClientReceiver::attach(
        &mut session,
        "lower",
        "a/Subscriptions/SUBSCRIPTIONS/subscriptions",
    )
    .await?;
    let b = recv(&mut second).await?;
    content(b.message(), &rich(0));
    second.accept(&b).await?;
    node.wait_len(&lower, 0).await?;
    connection.close().await?;
    Ok(())
}

async fn identical_topics_and_members_on_two_listeners_remain_namespace_scoped<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = Node::start(provider, "Orders", TopicConfig::default()).await?;
    let alpha = node.subscription("Alpha").await?;
    let neighboring = EntityPath::new("Orders-extra")?;
    node.submit(
        &neighboring,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )
    .await?;
    node.submit(
        &neighboring,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("Alpha")?,
            config: SubscriptionConfig::default(),
        },
    )
    .await?;
    let foreign = NamespaceName::new("tenant-other")?;
    node.submit_in(
        &foreign,
        &node.topic,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )
    .await?;
    node.submit_in(
        &foreign,
        &node.topic,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("Alpha")?,
            config: SubscriptionConfig::default(),
        },
    )
    .await?;
    let address = node.listen(foreign.clone()).await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut sender = ClientSender::attach(&mut session, "publisher", "Orders").await?;
    accepted(sender.send(rich(0)).await?);
    assert!(
        node.peek(&neighboring.subscription(&SubscriptionName::new("Alpha")?)?)
            .await?
            .is_empty()
    );
    assert_eq!(
        node.submit_in(
            &foreign,
            &alpha,
            CommandKind::Peek {
                from_sequence: SequenceNumber::new(0),
                max_messages: 1,
                session_id: None
            }
        )
        .await?,
        CommandOutcome::Peeked(vec![])
    );
    let mut other = connect(&address).await?;
    let mut other_session = other.begin().await?;
    let mut other_sender =
        ClientSender::attach(&mut other_session, "other-publisher", "Orders").await?;
    accepted(other_sender.send(rich(1)).await?);
    let mut first = ClientReceiver::attach(&mut session, "first-reader", alpha.as_str()).await?;
    let mut second =
        ClientReceiver::attach(&mut other_session, "second-reader", alpha.as_str()).await?;
    let a = recv(&mut first).await?;
    let b = recv(&mut second).await?;
    content(a.message(), &rich(0));
    content(b.message(), &rich(1));
    assert_eq!(sequence(a.message()), 1);
    assert_eq!(sequence(b.message()), 1);
    first.accept(&a).await?;
    second.accept(&b).await?;
    node.wait_len(&alpha, 0).await?;
    timeout(DEADLINE, async {
        loop {
            let outcome = node
                .submit_in(
                    &foreign,
                    &alpha,
                    CommandKind::Peek {
                        from_sequence: SequenceNumber::new(0),
                        max_messages: 1,
                        session_id: None,
                    },
                )
                .await?;
            if outcome == CommandOutcome::Peeked(vec![]) {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(
        node.submit_in(
            &foreign,
            &alpha,
            CommandKind::Peek {
                from_sequence: SequenceNumber::new(0),
                max_messages: 1,
                session_id: None
            }
        )
        .await?,
        CommandOutcome::Peeked(vec![])
    );
    connection.close().await?;
    other.close().await?;
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 8, super::$case(::testkit::MemoryProvider::new())).await? })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 8, super::$case(::testkit::DurableProvider::temporary()?)).await? })+ }
    };
}

for_each_backend! {
    missing_forbidden_and_session_targets_refuse_before_mutation_with_healthy_sibling,
    only_terminal_controls_fold_while_topic_and_member_names_keep_case,
    identical_topics_and_members_on_two_listeners_remain_namespace_scoped,
}
