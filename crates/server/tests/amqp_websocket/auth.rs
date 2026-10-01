use std::time::{SystemTime, UNIX_EPOCH};

use amqp::{
    Accepted, Attach, Begin, DeliveryState, Disposition, Flow, ReceiverSettleMode, Role,
    SenderSettleMode, Source, Target, Transfer, decode_message, encode_message,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use url::form_urlencoded::byte_serialize;

use super::*;

const AUDIENCE: &str = "amqps://tenant.servicebus.windows.net/orders";
const REPLY: &str = "websocket-cbs-reply";

fn at_stage(stage: &'static str) -> impl FnOnce(Box<dyn Error>) -> Box<dyn Error> {
    move |error| -> Box<dyn Error> { Box::new(io::Error::other(format!("{stage}: {error}"))) }
}

fn token(rule: &str) -> String {
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock")
        .as_secs()
        + 600;
    let resource: String = byte_serialize(AUDIENCE.as_bytes()).collect();
    let mut signer = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).expect("test key");
    signer.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(signer.finalize().into_bytes());
    let signature: String = byte_serialize(signature.as_bytes()).collect();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={rule}")
}

struct Peer {
    wire: RawPeer,
    sent: u32,
    received: u32,
}

impl Peer {
    async fn open<P: StoreProvider>(node: &Node<P>) -> TestResult<Self> {
        let mut wire = RawPeer::new(node.websocket().await?);
        wire.header(ProtocolHeader::SASL).await?;
        assert!(matches!(
            wire.read().await?,
            Frame::Sasl(amqp::SaslPerformative::Mechanisms(_))
        ));
        let init = SaslInit {
            mechanism: Symbol::from("ANONYMOUS"),
            initial_response: None,
            hostname: Some(HOST.into()),
        };
        ws_send(
            &mut wire.socket,
            WsMessage::Binary(
                encode_frame(&Frame::Sasl(amqp::SaslPerformative::Init(init)))?.into(),
            ),
        )
        .await?;
        assert!(
            matches!(wire.read().await?, Frame::Sasl(amqp::SaslPerformative::Outcome(outcome)) if outcome.code == amqp::SaslCode::Ok)
        );
        wire.header(ProtocolHeader::AMQP).await?;
        wire.send(
            0,
            Performative::Open(Open {
                hostname: Some(HOST.into()),
                ..Open::new("scoped-wss")
            }),
        )
        .await?;
        assert!(matches!(
            wire.read().await?,
            Frame::Amqp {
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        wire.send(0, Performative::Begin(Begin::default())).await?;
        assert!(matches!(
            wire.read().await?,
            Frame::Amqp {
                performative: Some(Performative::Begin(_)),
                ..
            }
        ));
        Ok(Self {
            wire,
            sent: 0,
            received: 0,
        })
    }

    async fn read(&mut self) -> TestResult<Frame> {
        let frame = self.wire.read().await?;
        if matches!(
            frame,
            Frame::Amqp {
                performative: Some(Performative::Transfer(_)),
                ..
            }
        ) {
            self.received += 1;
        }
        Ok(frame)
    }

    async fn attach(&mut self, handle: u32, role: Role, path: &str) -> TestResult {
        self.attach_reply(handle, role, path, None).await
    }

    async fn attach_reply(
        &mut self,
        handle: u32,
        role: Role,
        path: &str,
        reply: Option<&str>,
    ) -> TestResult {
        let attach = Attach {
            name: format!("scoped-{handle}"),
            handle,
            role: role.clone(),
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: (role == Role::Receiver).then(|| Source::new(path)),
            target: Some(Target::new(reply.unwrap_or(
                if path == protocol_amqp::CBS_NODE && role == Role::Receiver {
                    REPLY
                } else {
                    path
                },
            ))),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: (role == Role::Sender).then_some(0),
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };
        self.wire
            .send(0, Performative::Attach(Box::new(attach)))
            .await
    }

    async fn attached(&mut self, handle: u32) -> TestResult {
        for _ in 0..32 {
            match self.read().await? {
                Frame::Amqp {
                    performative: Some(Performative::Attach(attach)),
                    ..
                } => {
                    assert_eq!(attach.handle, handle);
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("Attach response expected: {other:?}"),
            }
        }
        Err("Attach did not finish within bounded frame count".into())
    }

    async fn credit(&mut self, handle: u32, delivered: u32) -> TestResult {
        self.wire
            .send(
                0,
                Performative::Flow(Flow {
                    next_incoming_id: Some(self.received),
                    incoming_window: 2_048,
                    next_outgoing_id: self.sent,
                    outgoing_window: 2_048,
                    handle: Some(handle),
                    delivery_count: Some(delivered),
                    link_credit: Some(10),
                    ..Flow::default()
                }),
            )
            .await
    }

    async fn send_message(&mut self, handle: u32, message: &Message) -> TestResult<u32> {
        let id = self.sent;
        let frame = Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Transfer(Transfer {
                handle,
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
            })),
            payload: encode_message(message)?,
        };
        ws_send(
            &mut self.wire.socket,
            WsMessage::Binary(encode_frame(&frame)?.into()),
        )
        .await?;
        self.sent += 1;
        Ok(id)
    }

    async fn accepted(&mut self, id: u32) -> TestResult {
        for _ in 0..32 {
            match self.read().await? {
                Frame::Amqp {
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                } => {
                    assert_eq!(disposition.role, Role::Receiver);
                    assert_eq!(disposition.first, id);
                    assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
                    assert!(disposition.settled);
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("Accepted response expected: {other:?}"),
            }
        }
        Err("send was not accepted within bounded frame count".into())
    }

    async fn grant(&mut self, rule: &str, response_count: u32) -> TestResult {
        self.credit(1, response_count).await?;
        let message_id = format!("grant-{response_count}");
        let message = Message {
            properties: Some(Properties {
                message_id: Some(message_id.clone().into()),
                reply_to: Some(REPLY.into()),
                ..Properties::default()
            }),
            application_properties: Some(
                ApplicationProperties::builder()
                    .insert("operation", "put-token".to_owned())
                    .insert("type", "servicebus.windows.net:sastoken".to_owned())
                    .insert("name", AUDIENCE.to_owned())
                    .build(),
            ),
            body: Body::Value(Value::String(token(rule))),
            ..Message::default()
        };
        let sent = self.send_message(0, &message).await?;
        let mut accepted = false;
        let mut replied = false;
        for _ in 0..32 {
            match self.read().await? {
                Frame::Amqp {
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                } => {
                    assert_eq!(disposition.first, sent);
                    assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
                    accepted = true;
                }
                Frame::Amqp {
                    performative: Some(Performative::Transfer(transfer)),
                    payload,
                    ..
                } => {
                    assert_eq!(transfer.handle, 1);
                    assert!(!transfer.more);
                    let response = decode_message(&payload)?;
                    assert_eq!(
                        response
                            .properties
                            .as_ref()
                            .expect("correlated CBS response")
                            .correlation_id,
                        Some(message_id.clone().into())
                    );
                    assert_eq!(
                        response
                            .application_properties
                            .as_ref()
                            .expect("CBS status")
                            .get("status-code"),
                        Some(&Value::Int(202))
                    );
                    self.ack(transfer.delivery_id.expect("response ID")).await?;
                    replied = true;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("CBS response expected: {other:?}"),
            }
            if accepted && replied {
                return Ok(());
            }
        }
        Err("CBS exchange did not finish within bounded frame count".into())
    }

    async fn ack(&mut self, id: u32) -> TestResult {
        self.wire
            .send(
                0,
                Performative::Disposition(Disposition {
                    role: Role::Receiver,
                    first: id,
                    last: None,
                    settled: true,
                    state: Some(DeliveryState::Accepted(Accepted)),
                    batchable: false,
                }),
            )
            .await
    }

    async fn denied(&mut self, handle: u32, role: Role, path: &str) -> TestResult {
        self.attach(handle, role, path).await?;
        let mut attach_seen = false;
        for _ in 0..32 {
            match self.read().await? {
                Frame::Amqp {
                    performative: Some(Performative::Attach(attach)),
                    ..
                } => {
                    assert_eq!(attach.handle, handle);
                    attach_seen = true;
                }
                Frame::Amqp {
                    performative: Some(Performative::Detach(detach)),
                    ..
                } => {
                    assert!(attach_seen, "refusal must publish its own Attach");
                    assert_eq!(detach.handle, handle);
                    assert!(detach.closed);
                    assert_eq!(
                        detach
                            .error
                            .expect("permission refusal")
                            .condition
                            .as_symbol(),
                        Symbol::from("amqp:unauthorized-access")
                    );
                    self.wire
                        .send(
                            0,
                            Performative::Detach(amqp::Detach {
                                handle,
                                closed: true,
                                error: None,
                            }),
                        )
                        .await?;
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(flow)),
                    ..
                } => {
                    // The healthy publisher can replenish credit during refusal.
                    assert!(
                        flow.handle.is_none_or(|flow_handle| {
                            flow_handle == 0 || flow_handle == 2 || flow_handle == handle
                        }),
                        "unexpected link-credit Flow {flow:?}"
                    );
                }
                other => panic!("permission refusal expected: {other:?}"),
            }
        }
        Err("permission refusal did not finish within bounded frame count".into())
    }

    async fn denied_peek(&mut self) -> TestResult {
        const MANAGEMENT_REPLY: &str = "websocket-management-reply";
        self.attach(8, Role::Sender, "orders/$management").await?;
        self.attached(8).await?;
        self.attach_reply(
            9,
            Role::Receiver,
            "orders/$management",
            Some(MANAGEMENT_REPLY),
        )
        .await?;
        self.attached(9).await?;
        self.credit(9, 0).await?;
        let message_id = "scoped-denied-peek";
        let message = Message {
            properties: Some(Properties {
                message_id: Some(message_id.into()),
                reply_to: Some(MANAGEMENT_REPLY.into()),
                ..Properties::default()
            }),
            application_properties: Some(
                ApplicationProperties::builder()
                    .insert(
                        protocol_amqp::OPERATION_PROPERTY,
                        protocol_amqp::PEEK_MESSAGE_OPERATION.to_owned(),
                    )
                    .build(),
            ),
            body: Body::Value(Value::Map(
                [
                    (
                        Value::String(protocol_amqp::FROM_SEQUENCE_NUMBER.into()),
                        Value::Long(1),
                    ),
                    (
                        Value::String(protocol_amqp::MESSAGE_COUNT.into()),
                        Value::Int(1),
                    ),
                ]
                .into_iter()
                .collect(),
            )),
            ..Message::default()
        };
        let sent = self.send_message(8, &message).await?;
        let mut accepted = false;
        let mut replied = false;
        for _ in 0..32 {
            match self.read().await? {
                Frame::Amqp {
                    performative: Some(Performative::Disposition(disposition)),
                    ..
                } => {
                    assert_eq!(disposition.first, sent);
                    assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
                    accepted = true;
                }
                Frame::Amqp {
                    performative: Some(Performative::Transfer(transfer)),
                    payload,
                    ..
                } => {
                    assert_eq!(transfer.handle, 9);
                    assert!(!transfer.more);
                    let response = decode_message(&payload)?;
                    assert_eq!(
                        response
                            .properties
                            .as_ref()
                            .expect("correlated response")
                            .correlation_id,
                        Some(message_id.into())
                    );
                    let properties = response
                        .application_properties
                        .as_ref()
                        .expect("management status");
                    assert_eq!(
                        properties.get(protocol_amqp::STATUS_CODE_PROPERTY),
                        Some(&Value::Int(401))
                    );
                    assert_eq!(
                        properties.get(protocol_amqp::ERROR_CONDITION_PROPERTY),
                        Some(&Value::Symbol(Symbol::from("amqp:unauthorized-access")))
                    );
                    assert_eq!(response.body, Body::Value(Value::Null));
                    self.ack(transfer.delivery_id.expect("reply delivery ID"))
                        .await?;
                    replied = true;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("management permission response expected: {other:?}"),
            }
            if accepted && replied {
                return Ok(());
            }
        }
        Err("management refusal did not finish within bounded frame count".into())
    }
}

pub(super) async fn wss_cbs_grants_do_not_follow_the_http_host<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::scoped(provider)
        .await
        .map_err(at_stage("start scoped WSS node"))?;
    let before = node.snapshot()?;
    let mut peer = Peer::open(&node)
        .await
        .map_err(at_stage("WSS/SASL/Open/Begin"))?;
    peer.attach(0, Role::Sender, protocol_amqp::CBS_NODE)
        .await?;
    peer.attached(0)
        .await
        .map_err(at_stage("CBS request Attach"))?;
    peer.attach(1, Role::Receiver, protocol_amqp::CBS_NODE)
        .await?;
    peer.attached(1)
        .await
        .map_err(at_stage("CBS response Attach"))?;
    peer.grant(RULE, 0)
        .await
        .map_err(at_stage("initial scoped Send CBS grant"))?;
    assert_eq!(
        node.snapshot()?,
        before,
        "CBS grant is not a domain mutation"
    );
    peer.attach(2, Role::Sender, "orders").await?;
    peer.attached(2)
        .await
        .map_err(at_stage("authorized Queue producer Attach"))?;
    let message = Message {
        body: Body::Data(vec![b"scoped-wss".to_vec().into()]),
        ..Message::default()
    };
    let id = peer.send_message(2, &message).await?;
    peer.accepted(id)
        .await
        .map_err(at_stage("authorized Queue publication Accepted"))?;
    let after_send = node.snapshot()?;
    for (handle, role, path) in [(3, Role::Receiver, "orders"), (5, Role::Sender, "Topic")] {
        peer.denied(handle, role, path).await.map_err(|error| {
            Box::new(io::Error::other(format!(
                "denied scoped Attach handle={handle} path={path}: {error}"
            ))) as Box<dyn Error>
        })?;
        assert_eq!(
            node.snapshot()?,
            after_send,
            "denied WSS scope mutated storage"
        );
    }
    peer.denied_peek()
        .await
        .map_err(at_stage("Send-only management Peek refusal"))?;
    assert_eq!(
        node.snapshot()?,
        after_send,
        "denied management operation mutated storage"
    );
    peer.grant(LISTEN_RULE, 1)
        .await
        .map_err(at_stage("fresh scoped Listen CBS grant"))?;
    assert_eq!(node.snapshot()?, after_send);
    peer.attach(6, Role::Receiver, "orders").await?;
    peer.attached(6)
        .await
        .map_err(at_stage("fresh Listen Queue consumer Attach"))?;
    peer.credit(6, 0)
        .await
        .map_err(at_stage("fresh Listen Queue consumer credit"))?;
    let mut delivery = None;
    for _ in 0..32 {
        match peer
            .read()
            .await
            .map_err(at_stage("fresh Listen Queue delivery"))?
        {
            Frame::Amqp {
                performative: Some(Performative::Transfer(transfer)),
                payload,
                ..
            } => {
                assert_eq!(transfer.handle, 6);
                assert_eq!(decode_message(&payload)?.body, message.body);
                delivery = Some(transfer.delivery_id.expect("queue delivery ID"));
                break;
            }
            Frame::Amqp {
                performative: Some(Performative::Flow(_)),
                ..
            } => {}
            other => panic!("fresh Listen grant must receive queued message: {other:?}"),
        }
    }
    peer.ack(delivery.expect("fresh Listen grant delivery"))
        .await?;
    node.wait_removed("orders", 1)
        .await
        .map_err(at_stage("Queue Complete owner barrier"))?;
    peer.wire
        .close()
        .await
        .map_err(at_stage("final AMQP/WS close exchange"))?;
    Ok(())
}
