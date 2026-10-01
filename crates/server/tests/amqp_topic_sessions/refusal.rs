use amqp::{
    Attach, Begin, Close, Detach, FilterSet, Flow, Frame, Open, Performative, ProtocolHeader, Role,
    Source, Target, Transfer, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use tokio::net::TcpStream;

use super::*;

struct Peer(TcpStream);

impl Peer {
    async fn connect(address: &str) -> TestResult<Self> {
        let mut peer = Self(timeout(DEADLINE, TcpStream::connect(address)).await??);
        timeout(
            DEADLINE,
            write_protocol_header(&mut peer.0, ProtocolHeader::AMQP),
        )
        .await??;
        assert_eq!(
            timeout(DEADLINE, read_protocol_header(&mut peer.0)).await??,
            ProtocolHeader::AMQP
        );
        peer.send(
            Performative::Open(Open::new("session-refusal-peer")),
            vec![],
        )
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
        assert!(matches!(peer.read().await?, Frame::Amqp { channel: 0,
            performative: Some(Performative::Begin(begin)), .. } if begin.remote_channel == Some(0)));
        Ok(peer)
    }

    async fn send(&mut self, performative: Performative, payload: Vec<u8>) -> TestResult {
        timeout(
            DEADLINE,
            write_frame(
                &mut self.0,
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
        Ok(timeout(DEADLINE, read_frame(&mut self.0)).await??)
    }

    async fn attach(
        &mut self,
        name: &str,
        handle: u32,
        role: Role,
        source: Option<Source>,
    ) -> TestResult {
        self.send(
            Performative::Attach(Box::new(Attach {
                name: name.into(),
                handle,
                role: role.clone(),
                snd_settle_mode: amqp::SenderSettleMode::Unsettled,
                rcv_settle_mode: amqp::ReceiverSettleMode::First,
                source,
                target: (role == Role::Sender).then(|| Target::new("healthy").into()),
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

    async fn healthy(&mut self) -> TestResult {
        self.attach("healthy", 0, Role::Sender, None).await?;
        assert!(
            matches!(self.read().await?, Frame::Amqp { performative: Some(Performative::Attach(attach)), .. }
            if attach.name == "healthy" && attach.handle == 0 && attach.role == Role::Receiver)
        );
        loop {
            match self.read().await? {
                Frame::Amqp {
                    performative: Some(Performative::Flow(flow)),
                    ..
                } if flow.handle == Some(0) && flow.link_credit.unwrap_or(0) > 0 => return Ok(()),
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("healthy link credit: {other:?}"),
            }
        }
    }

    async fn refused(
        &mut self,
        name: &str,
        address: &str,
        filter: Option<Value>,
        condition: &str,
    ) -> TestResult {
        let filter = filter.map(|value| {
            let mut filter = FilterSet::default();
            filter.insert(Symbol::from(protocol_amqp::SESSION_FILTER), value);
            filter
        });
        self.attach(
            name,
            1,
            Role::Receiver,
            Some(Source {
                address: Some(address.into()),
                filter,
                ..Source::default()
            }),
        )
        .await?;
        let local = loop {
            match self.read().await? {
                Frame::Amqp {
                    performative: Some(Performative::Attach(attach)),
                    ..
                } => {
                    assert_eq!(attach.name, name);
                    assert_eq!(attach.role, Role::Sender);
                    break attach.handle;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("own Attach precedes refusal: {other:?}"),
            }
        };
        let Frame::Amqp {
            performative: Some(Performative::Detach(detach)),
            ..
        } = self.read().await?
        else {
            panic!("immediate error Detach")
        };
        assert_eq!(detach.handle, local);
        assert!(detach.closed);
        assert_eq!(
            detach.error.expect("refusal reason").condition.as_symbol(),
            Symbol::from(condition)
        );
        self.send(
            Performative::Detach(Detach {
                handle: 1,
                closed: true,
                error: None,
            }),
            vec![],
        )
        .await?;
        self.send(
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: 2_048,
                next_outgoing_id: 0,
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
                    performative: Some(Performative::Flow(flow)),
                    ..
                } if flow.handle.is_none() => return Ok(()),
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("healthy session barrier: {other:?}"),
            }
        }
    }

    async fn publish_healthy(&mut self) -> TestResult {
        self.send(
            Performative::Transfer(Transfer {
                handle: 0,
                delivery_id: Some(0),
                delivery_tag: Some(vec![0].into()),
                message_format: Some(0),
                settled: Some(false),
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            }),
            encode_message(&Message::data(b"healthy".to_vec()))?,
        )
        .await?;
        loop {
            match self.read().await? {
                Frame::Amqp {
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                } => {
                    assert_eq!(disposition.first, 0);
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
                other => panic!("healthy settlement: {other:?}"),
            }
        }
    }

    async fn close(mut self) -> TestResult {
        self.send(Performative::Close(Close::default()), vec![])
            .await?;
        loop {
            match self.read().await? {
                Frame::Amqp {
                    performative: Some(Performative::Close(close)),
                    ..
                } => {
                    assert!(close.error.is_none());
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("clean close: {other:?}"),
            }
        }
    }
}

async fn missing_or_malformed_subscription_filters_refuse_before_commands<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut peer = Peer::connect(&node.address).await?;
    peer.healthy().await?;
    for (index, address, filter, condition) in [
        (0, "Orders/Subscriptions/Alpha", None, "amqp:not-allowed"),
        (
            1,
            "Orders/SUBSCRIPTIONS/beta",
            Some(Value::Int(1)),
            "amqp:invalid-field",
        ),
        (
            2,
            "Orders/subscriptions/Alpha",
            Some(Value::String(String::new())),
            "amqp:invalid-field",
        ),
        (
            3,
            "Orders/subscriptions/beta",
            Some(Value::String("x".repeat(129))),
            "amqp:invalid-field",
        ),
    ] {
        let before = node.snapshot()?;
        let submits = node.submissions();
        peer.refused(&format!("refused-{index}"), address, filter, condition)
            .await?;
        assert_eq!(node.snapshot()?, before);
        assert_eq!(
            node.submissions(),
            submits,
            "planning refusal must not stamp or submit"
        );
    }
    peer.publish_healthy().await?;
    assert_eq!(
        node.record(&EntityPath::new("healthy")?, 1)?
            .expect("healthy queue copy")
            .body,
        b"healthy"
    );
    peer.close().await?;
    Ok(())
}

for_each_backend!(missing_or_malformed_subscription_filters_refuse_before_commands);
