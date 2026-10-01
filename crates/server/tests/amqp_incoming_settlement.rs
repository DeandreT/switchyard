//! Incoming delivery ownership and settlement through the public TCP engine.

use std::{collections::HashMap, error::Error, time::Duration};

use amqp::{
    Accepted, Attach, Begin, ClientConnection, ClientReceiver, ConnectionOptions, Delivery,
    DeliveryState, Detach, Disposition, End, EngineError, Flow, Frame, LinkEndpoint, Message, Open,
    Outcome, Performative, ProtocolHeader, Receiver, ReceiverSettleMode, Role, SenderSettleMode,
    ServerConnection, ServerSession, Source, Symbol, Target, Transfer, encode_message, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const IO_TIMEOUT: Duration = Duration::from_secs(10);

struct Peer {
    stream: TcpStream,
    frames_sent: HashMap<u16, u32>,
}

impl Peer {
    async fn send(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) -> TestResult {
        if matches!(performative, Performative::Transfer(_)) {
            let count = self.frames_sent.entry(channel).or_default();
            *count = count.wrapping_add(1);
        }
        timeout(
            IO_TIMEOUT,
            write_frame(
                &mut self.stream,
                &Frame::Amqp {
                    channel,
                    performative: Some(performative),
                    payload,
                },
            ),
        )
        .await??;
        Ok(())
    }

    async fn read(&mut self) -> TestResult<Frame> {
        Ok(timeout(IO_TIMEOUT, read_frame(&mut self.stream)).await??)
    }

    // An echoed session Flow is ordered after all earlier peer frames and local
    // settlement commands, unlike a successful socket write alone.
    async fn barrier(&mut self, channel: u16) -> TestResult<Vec<Frame>> {
        let expected = self.frames_sent.get(&channel).copied().unwrap_or(0);
        self.send(
            channel,
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: 2_048,
                next_outgoing_id: expected,
                outgoing_window: 2_048,
                echo: true,
                ..Flow::default()
            }),
            Vec::new(),
        )
        .await?;
        let mut observed = Vec::new();
        loop {
            let frame = self.read().await?;
            let done = matches!(&frame, Frame::Amqp {
                channel: received,
                performative: Some(Performative::Flow(flow)),
                ..
            } if *received == channel && flow.handle.is_none() && flow.next_incoming_id == Some(expected));
            assert!(
                !matches!(
                    &frame,
                    Frame::Amqp {
                        performative: Some(Performative::Close(_) | Performative::End(_)),
                        ..
                    }
                ),
                "unrelated connection/session must remain open: {frame:?}"
            );
            observed.push(frame);
            if done {
                return Ok(observed);
            }
        }
    }

    async fn disposition(
        &mut self,
        channel: u16,
        id: u32,
        settled: bool,
    ) -> TestResult<Disposition> {
        loop {
            let frame = self.read().await?;
            match frame {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Disposition(value)),
                    ..
                } => {
                    assert_eq!(actual, channel);
                    assert_eq!(value.role, Role::Receiver);
                    assert_eq!(value.first, id);
                    assert_eq!(value.last, None);
                    assert_eq!(value.settled, settled);
                    assert_eq!(value.state, Some(DeliveryState::Accepted(Accepted)));
                    return Ok(value);
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("expected an accepted disposition, got {other:?}"),
            }
        }
    }

    async fn detach(&mut self, channel: u16, handle: u32, condition: Option<&str>) -> TestResult {
        loop {
            let frame = self.read().await?;
            match frame {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Detach(detach)),
                    ..
                } => {
                    assert_eq!(actual, channel);
                    assert_eq!(detach.handle, handle);
                    assert!(detach.closed);
                    assert_eq!(
                        detach
                            .error
                            .as_ref()
                            .map(|error| error.condition.as_symbol()),
                        condition.map(Symbol::from)
                    );
                    return Ok(());
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                other => panic!("expected only an affected-link Detach, got {other:?}"),
            }
        }
    }

    async fn acknowledge(&mut self, channel: u16, first: u32, last: Option<u32>) -> TestResult {
        self.send(
            channel,
            Performative::Disposition(Disposition {
                role: Role::Sender,
                first,
                last,
                settled: true,
                state: None,
                batchable: false,
            }),
            Vec::new(),
        )
        .await
    }
}

struct Harness {
    connection: ServerConnection,
    peer: Peer,
}

impl Harness {
    async fn new() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut stream = TcpStream::connect(listener.local_addr()?).await?;
        stream.set_nodelay(true)?;
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let server = tokio::spawn(ServerConnection::accept_with_options(
            socket,
            "incoming-settlement-server",
            None,
            ConnectionOptions::default().idle_timeout_millis(0),
        ));
        write_protocol_header(&mut stream, ProtocolHeader::AMQP).await?;
        assert_eq!(
            timeout(IO_TIMEOUT, read_protocol_header(&mut stream)).await??,
            ProtocolHeader::AMQP
        );
        write_frame(
            &mut stream,
            &Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(Open::new("raw-incoming-peer"))),
                payload: Vec::new(),
            },
        )
        .await?;
        assert!(matches!(
            timeout(IO_TIMEOUT, read_frame(&mut stream)).await??,
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        let connection = timeout(IO_TIMEOUT, server).await???;
        Ok(Self {
            connection,
            peer: Peer {
                stream,
                frames_sent: HashMap::new(),
            },
        })
    }

    async fn begin(&mut self, channel: u16) -> TestResult<ServerSession> {
        self.peer.frames_sent.insert(channel, 0);
        self.peer
            .send(channel, Performative::Begin(Begin::default()), Vec::new())
            .await?;
        let incoming = timeout(IO_TIMEOUT, self.connection.next_incoming_session())
            .await?
            .expect("incoming session");
        let session = timeout(IO_TIMEOUT, self.connection.accept_session(incoming)).await??;
        assert!(matches!(self.peer.read().await?, Frame::Amqp {
            channel: actual, performative: Some(Performative::Begin(begin)), ..
        } if actual == channel && begin.remote_channel == Some(channel)));
        Ok(session)
    }

    async fn attach(
        &mut self,
        session: &mut ServerSession,
        channel: u16,
        handle: u32,
        mode: ReceiverSettleMode,
    ) -> TestResult<Receiver> {
        self.attach_with_sender_mode(session, channel, handle, mode, SenderSettleMode::Mixed)
            .await
    }

    async fn attach_with_sender_mode(
        &mut self,
        session: &mut ServerSession,
        channel: u16,
        handle: u32,
        mode: ReceiverSettleMode,
        sender_mode: SenderSettleMode,
    ) -> TestResult<Receiver> {
        self.peer
            .send(
                channel,
                Performative::Attach(Box::new(Attach {
                    name: format!("raw-{channel}-{handle}"),
                    handle,
                    role: Role::Sender,
                    snd_settle_mode: sender_mode,
                    rcv_settle_mode: mode.clone(),
                    source: None,
                    target: Some(Target::new("queue").into()),
                    unsettled: None,
                    incomplete_unsettled: false,
                    initial_delivery_count: Some(0),
                    max_message_size: Some(4 * 1024 * 1024),
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                })),
                Vec::new(),
            )
            .await?;
        let attach = timeout(IO_TIMEOUT, session.next_incoming_attach())
            .await?
            .expect("incoming attach");
        let LinkEndpoint::Receiver(receiver) =
            timeout(IO_TIMEOUT, session.accept_attach(attach, 4 * 1024 * 1024)).await??
        else {
            panic!("peer sender must create a public receiving endpoint");
        };
        let mut attached = false;
        let mut credited = false;
        while !attached || !credited {
            match self.peer.read().await? {
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Attach(attach)),
                    ..
                } => {
                    assert_eq!(actual, channel);
                    assert_eq!(attach.handle, handle);
                    assert_eq!(attach.role, Role::Receiver);
                    assert_eq!(attach.rcv_settle_mode, mode);
                    attached = true;
                }
                Frame::Amqp {
                    channel: actual,
                    performative: Some(Performative::Flow(flow)),
                    ..
                } => {
                    if actual == channel && flow.handle == Some(handle) {
                        assert_eq!(flow.delivery_count, Some(0));
                        assert_eq!(flow.link_credit, Some(32));
                        credited = true;
                    }
                }
                other => panic!("unexpected attach response: {other:?}"),
            }
        }
        Ok(receiver)
    }

    async fn deliver(
        &mut self,
        receiver: &mut Receiver,
        channel: u16,
        handle: u32,
        id: u32,
        tag: Vec<u8>,
        settled: bool,
    ) -> TestResult<Delivery> {
        let mut transfer = initial(handle, id, tag, false);
        transfer.settled = Some(settled);
        self.peer
            .send(channel, Performative::Transfer(transfer), encoded(id)?)
            .await?;
        Ok(timeout(IO_TIMEOUT, receiver.recv()).await??)
    }

    async fn finish(self) {
        self.connection.shutdown().await;
    }
}

fn initial(handle: u32, id: u32, tag: Vec<u8>, more: bool) -> Transfer {
    Transfer {
        delivery_id: Some(id),
        delivery_tag: Some(tag.into()),
        message_format: Some(0),
        more,
        ..continuation(handle)
    }
}

fn continuation(handle: u32) -> Transfer {
    Transfer {
        handle,
        delivery_id: None,
        delivery_tag: None,
        message_format: None,
        settled: None,
        more: false,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

fn encoded(id: u32) -> TestResult<Vec<u8>> {
    Ok(encode_message(&Message::data(id.to_be_bytes().to_vec()))?)
}

fn assert_no_dispositions(frames: &[Frame]) {
    assert!(
        !frames.iter().any(|frame| matches!(
            frame,
            Frame::Amqp {
                performative: Some(Performative::Disposition(_)),
                ..
            }
        )),
        "no settlement frame was permitted: {frames:?}"
    );
}

fn assert_invalid_owner(result: Result<(), EngineError>) {
    assert!(
        matches!(result, Err(EngineError::InvalidState(_))),
        "wrong owner/generation must refuse locally: {result:?}"
    );
}

#[tokio::test]
async fn delivery_ownership_covers_links_sessions_connections_and_sender_settled_noops()
-> TestResult {
    let mut first = Harness::new().await?;
    let mut session = first.begin(0).await?;
    let mut owner = first
        .attach(&mut session, 0, 0, ReceiverSettleMode::First)
        .await?;
    let other_link = first
        .attach(&mut session, 0, 1, ReceiverSettleMode::First)
        .await?;
    let mut other_session = first.begin(1).await?;
    let another_session = first
        .attach(&mut other_session, 1, 0, ReceiverSettleMode::First)
        .await?;
    let mut second = Harness::new().await?;
    let mut second_session = second.begin(0).await?;
    let another_connection = second
        .attach(&mut second_session, 0, 0, ReceiverSettleMode::First)
        .await?;
    for settled in [false, true] {
        let delivery = first
            .deliver(&mut owner, 0, 0, 17, vec![17], settled)
            .await?;
        assert_invalid_owner(other_link.accept(&delivery).await);
        assert_invalid_owner(another_session.release(&delivery).await);
        assert_invalid_owner(another_connection.reject(&delivery, None).await);
        assert_no_dispositions(&first.peer.barrier(0).await?);
        assert_no_dispositions(&first.peer.barrier(1).await?);
        assert_no_dispositions(&second.peer.barrier(0).await?);
        owner.accept(&delivery).await?;
        if !settled {
            first.peer.disposition(0, 17, true).await?;
        }
        owner.release(&delivery).await?;
        assert_no_dispositions(&first.peer.barrier(0).await?);
    }
    first.finish().await;
    second.finish().await;
    Ok(())
}

#[tokio::test]
async fn duplicate_session_id_refuses_only_the_new_link_and_preserves_complete_or_partial_owner()
-> TestResult {
    for partial in [false, true] {
        let mut node = Harness::new().await?;
        let mut session = node.begin(0).await?;
        let mut owner = node
            .attach(&mut session, 0, 0, ReceiverSettleMode::First)
            .await?;
        let mut offender = node
            .attach(&mut session, 0, 1, ReceiverSettleMode::First)
            .await?;
        let bytes = encoded(41)?;
        node.peer
            .send(
                0,
                Performative::Transfer(initial(0, 41, vec![9], partial)),
                if partial {
                    bytes[..2].to_vec()
                } else {
                    bytes.clone()
                },
            )
            .await?;
        assert_no_dispositions(&node.peer.barrier(0).await?);
        node.peer
            .send(
                0,
                Performative::Transfer(initial(1, 41, vec![10], false)),
                bytes.clone(),
            )
            .await?;
        node.peer.detach(0, 1, Some("amqp:invalid-field")).await?;
        assert!(matches!(
            timeout(IO_TIMEOUT, offender.recv()).await?,
            Err(EngineError::RemoteDetached)
        ));
        if partial {
            node.peer
                .send(
                    0,
                    Performative::Transfer(continuation(0)),
                    bytes[2..].to_vec(),
                )
                .await?;
        }
        let delivery = timeout(IO_TIMEOUT, owner.recv()).await??;
        assert_eq!(
            delivery.message(),
            &Message::data(41_u32.to_be_bytes().to_vec())
        );
        owner.accept(&delivery).await?;
        node.peer.disposition(0, 41, true).await?;
        assert_no_dispositions(&node.peer.barrier(0).await?);
        node.finish().await;
    }
    Ok(())
}

#[tokio::test]
async fn tags_are_link_scoped_but_a_live_tag_cannot_alias_two_deliveries_on_one_link() -> TestResult
{
    let mut node = Harness::new().await?;
    let mut session = node.begin(0).await?;
    let mut first = node
        .attach(&mut session, 0, 0, ReceiverSettleMode::First)
        .await?;
    let mut second = node
        .attach(&mut session, 0, 1, ReceiverSettleMode::First)
        .await?;
    let original = node.deliver(&mut first, 0, 0, 1, vec![3], false).await?;
    let healthy = node.deliver(&mut second, 0, 1, 2, vec![3], false).await?;
    node.peer
        .send(
            0,
            Performative::Transfer(initial(0, 3, vec![3], false)),
            encoded(3)?,
        )
        .await?;
    node.peer.detach(0, 0, Some("amqp:invalid-field")).await?;
    assert_invalid_owner(first.accept(&original).await);
    second.accept(&healthy).await?;
    node.peer.disposition(0, 2, true).await?;
    let next = node.deliver(&mut second, 0, 1, 1, vec![3], false).await?;
    second.accept(&next).await?;
    node.peer.disposition(0, 1, true).await?;
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn second_mode_keeps_aliases_until_sender_ack_with_no_state_and_does_not_affect_other_sessions()
-> TestResult {
    let mut node = Harness::new().await?;
    let mut session = node.begin(0).await?;
    let mut owner = node
        .attach(&mut session, 0, 0, ReceiverSettleMode::Second)
        .await?;
    let mut offender = node
        .attach(&mut session, 0, 1, ReceiverSettleMode::First)
        .await?;
    let mut healthy_session = node.begin(1).await?;
    let mut healthy = node
        .attach(&mut healthy_session, 1, 0, ReceiverSettleMode::First)
        .await?;
    let delivery = node.deliver(&mut owner, 0, 0, 7, vec![7], false).await?;
    owner.accept(&delivery).await?;
    node.peer.disposition(0, 7, false).await?;
    owner.release(&delivery).await?;
    assert_no_dispositions(&node.peer.barrier(0).await?);
    node.peer
        .send(
            0,
            Performative::Transfer(initial(1, 7, vec![8], false)),
            encoded(7)?,
        )
        .await?;
    node.peer.detach(0, 1, Some("amqp:invalid-field")).await?;
    assert!(matches!(
        timeout(IO_TIMEOUT, offender.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));
    let other = node.deliver(&mut healthy, 1, 0, 7, vec![7], false).await?;
    healthy.accept(&other).await?;
    node.peer.disposition(1, 7, true).await?;
    node.peer.acknowledge(0, 7, None).await?;
    assert_no_dispositions(&node.peer.barrier(0).await?);
    let retry = node.deliver(&mut owner, 0, 0, 7, vec![7], false).await?;
    owner.accept(&delivery).await?;
    assert_no_dispositions(&node.peer.barrier(0).await?);
    owner.accept(&retry).await?;
    node.peer.disposition(0, 7, false).await?;
    node.peer.acknowledge(0, 7, None).await?;
    assert_no_dispositions(&node.peer.barrier(0).await?);
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn early_wrapping_sender_ack_covers_partial_and_complete_deliveries_without_a_receiver_frame()
-> TestResult {
    let mut node = Harness::new().await?;
    let mut session = node.begin(0).await?;
    let mut first = node
        .attach(&mut session, 0, 0, ReceiverSettleMode::Second)
        .await?;
    let mut second = node
        .attach(&mut session, 0, 1, ReceiverSettleMode::Second)
        .await?;
    let complete = node
        .deliver(&mut first, 0, 0, u32::MAX, vec![1], false)
        .await?;
    let bytes = encoded(0)?;
    node.peer
        .send(
            0,
            Performative::Transfer(initial(1, 0, vec![2], true)),
            bytes[..2].to_vec(),
        )
        .await?;
    node.peer.acknowledge(0, u32::MAX, Some(0)).await?;
    assert_no_dispositions(&node.peer.barrier(0).await?);
    node.peer
        .send(
            0,
            Performative::Transfer(continuation(1)),
            bytes[2..].to_vec(),
        )
        .await?;
    let partial = timeout(IO_TIMEOUT, second.recv()).await??;
    first.accept(&complete).await?;
    second.accept(&partial).await?;
    assert_no_dispositions(&node.peer.barrier(0).await?);
    let replacement = node
        .deliver(&mut first, 0, 0, u32::MAX, vec![1], false)
        .await?;
    first.release(&complete).await?;
    assert_no_dispositions(&node.peer.barrier(0).await?);
    first.accept(&replacement).await?;
    node.peer.disposition(0, u32::MAX, false).await?;
    node.peer.acknowledge(0, u32::MAX, None).await?;
    assert_no_dispositions(&node.peer.barrier(0).await?);
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn completing_transfer_uses_its_override_or_the_negotiated_mode_not_the_first_frame_override()
-> TestResult {
    for final_first in [false, true] {
        let mut node = Harness::new().await?;
        let mut session = node.begin(0).await?;
        let mut receiver = node
            .attach(&mut session, 0, 0, ReceiverSettleMode::Second)
            .await?;
        let bytes = encoded(5)?;
        let mut first = initial(0, 5, vec![5], true);
        first.rcv_settle_mode = (!final_first).then_some(ReceiverSettleMode::First);
        node.peer
            .send(0, Performative::Transfer(first), bytes[..2].to_vec())
            .await?;
        let mut last = continuation(0);
        last.rcv_settle_mode = final_first.then_some(ReceiverSettleMode::First);
        node.peer
            .send(0, Performative::Transfer(last), bytes[2..].to_vec())
            .await?;
        let delivery = timeout(IO_TIMEOUT, receiver.recv()).await??;
        receiver.accept(&delivery).await?;
        node.peer.disposition(0, 5, final_first).await?;
        if !final_first {
            node.peer.acknowledge(0, 5, None).await?;
        }
        assert_no_dispositions(&node.peer.barrier(0).await?);
        node.finish().await;
    }
    Ok(())
}

#[tokio::test]
async fn early_sender_settlement_ignores_receiver_mode_but_does_not_replace_required_transfer_flags()
-> TestResult {
    let mut node = Harness::new().await?;
    let mut session = node.begin(0).await?;
    let mut mixed = node
        .attach(&mut session, 0, 0, ReceiverSettleMode::First)
        .await?;
    let mut settled = node
        .attach_with_sender_mode(
            &mut session,
            0,
            1,
            ReceiverSettleMode::First,
            SenderSettleMode::Settled,
        )
        .await?;
    for handle in [0, 1] {
        let id = handle + 100;
        let bytes = encoded(id)?;
        node.peer
            .send(
                0,
                Performative::Transfer(initial(handle, id, vec![handle as u8], true)),
                bytes[..2].to_vec(),
            )
            .await?;
        node.peer.acknowledge(0, id, None).await?;
        assert_no_dispositions(&node.peer.barrier(0).await?);
        let mut last = continuation(handle);
        last.rcv_settle_mode = Some(ReceiverSettleMode::Second);
        node.peer
            .send(0, Performative::Transfer(last), bytes[2..].to_vec())
            .await?;
        if handle == 0 {
            // Local interpretation of the receiver-mode exception: a preceding
            // Sender settlement counts, although no Transfer settled flag does.
            let delivery = timeout(IO_TIMEOUT, mixed.recv()).await??;
            mixed.accept(&delivery).await?;
            assert_no_dispositions(&node.peer.barrier(0).await?);
        } else {
            node.peer.detach(0, 1, Some("amqp:invalid-field")).await?;
            assert!(matches!(
                timeout(IO_TIMEOUT, settled.recv()).await?,
                Err(EngineError::RemoteDetached)
            ));
        }
    }
    let retry = node.deliver(&mut mixed, 0, 0, 101, vec![1], false).await?;
    mixed.accept(&retry).await?;
    node.peer.disposition(0, 101, true).await?;
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn abort_and_detach_release_aliases_but_old_link_generations_cannot_settle_replacements()
-> TestResult {
    let mut node = Harness::new().await?;
    let mut session = node.begin(0).await?;
    let mut old = node
        .attach(&mut session, 0, 0, ReceiverSettleMode::First)
        .await?;
    let bytes = encoded(3)?;
    node.peer
        .send(
            0,
            Performative::Transfer(initial(0, 3, vec![3], true)),
            bytes[..2].to_vec(),
        )
        .await?;
    let mut aborted = continuation(0);
    aborted.aborted = true;
    aborted.more = true;
    node.peer
        .send(0, Performative::Transfer(aborted), vec![255; 20])
        .await?;
    assert_no_dispositions(&node.peer.barrier(0).await?);
    let delivery = node.deliver(&mut old, 0, 0, 3, vec![3], true).await?;
    node.peer
        .send(
            0,
            Performative::Detach(Detach {
                handle: 0,
                closed: true,
                error: None,
            }),
            Vec::new(),
        )
        .await?;
    node.peer.detach(0, 0, None).await?;
    let mut replacement = node
        .attach(&mut session, 0, 0, ReceiverSettleMode::First)
        .await?;
    let current = node
        .deliver(&mut replacement, 0, 0, 3, vec![3], false)
        .await?;
    assert_invalid_owner(old.accept(&delivery).await);
    assert_invalid_owner(replacement.accept(&delivery).await);
    assert_no_dispositions(&node.peer.barrier(0).await?);
    replacement.accept(&current).await?;
    node.peer.disposition(0, 3, true).await?;
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn channel_and_handle_reuse_cannot_redirect_a_retired_delivery_outcome() -> TestResult {
    let mut node = Harness::new().await?;
    let mut old_session = node.begin(0).await?;
    let mut old = node
        .attach(&mut old_session, 0, 0, ReceiverSettleMode::First)
        .await?;
    let delivery = node.deliver(&mut old, 0, 0, 19, vec![19], false).await?;
    node.peer
        .send(0, Performative::End(End::default()), Vec::new())
        .await?;
    loop {
        match node.peer.read().await? {
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::End(end)),
                ..
            } => {
                assert_eq!(end.error, None);
                break;
            }
            Frame::Amqp {
                performative: Some(Performative::Flow(_)),
                ..
            } => {}
            other => panic!("expected session End: {other:?}"),
        }
    }
    let mut new_session = node.begin(0).await?;
    let mut replacement = node
        .attach(&mut new_session, 0, 0, ReceiverSettleMode::First)
        .await?;
    let current = node
        .deliver(&mut replacement, 0, 0, 19, vec![19], false)
        .await?;
    assert_invalid_owner(old.accept(&delivery).await);
    assert_invalid_owner(replacement.accept(&delivery).await);
    assert_no_dispositions(&node.peer.barrier(0).await?);
    replacement.accept(&current).await?;
    node.peer.disposition(0, 19, true).await?;
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn a_thousand_live_deliveries_hit_only_the_link_metadata_cap_and_free_capacity_on_detach()
-> TestResult {
    let mut node = Harness::new().await?;
    let mut session = node.begin(0).await?;
    let mut saturated = node
        .attach(&mut session, 0, 0, ReceiverSettleMode::First)
        .await?;
    let mut healthy = node
        .attach(&mut session, 0, 1, ReceiverSettleMode::First)
        .await?;
    let held_healthy = node
        .deliver(&mut healthy, 0, 1, u32::MAX, vec![255], false)
        .await?;
    let mut held = Vec::with_capacity(1_024);
    for id in 0_u32..1_024 {
        held.push(
            node.deliver(&mut saturated, 0, 0, id, id.to_be_bytes().to_vec(), false)
                .await?,
        );
        // Consumption replenishes link slots; settlement metadata intentionally remains.
        assert_no_dispositions(&node.peer.barrier(0).await?);
    }
    node.peer
        .send(
            0,
            Performative::Transfer(initial(0, 1_024, vec![0, 4], false)),
            encoded(1_024)?,
        )
        .await?;
    node.peer
        .detach(0, 0, Some("amqp:resource-limit-exceeded"))
        .await?;
    assert_invalid_owner(saturated.accept(&held[0]).await);
    healthy.accept(&held_healthy).await?;
    node.peer.disposition(0, u32::MAX, true).await?;
    let reused = node
        .deliver(&mut healthy, 0, 1, 0, vec![0, 0, 0, 0], false)
        .await?;
    healthy.accept(&reused).await?;
    node.peer.disposition(0, 0, true).await?;
    node.finish().await;
    Ok(())
}

#[tokio::test]
async fn public_client_second_mode_builder_and_owned_tokens_complete_the_sender_ack_handshake()
-> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let stream = TcpStream::connect(listener.local_addr()?).await?;
    stream.set_nodelay(true)?;
    let (socket, _) = listener.accept().await?;
    socket.set_nodelay(true)?;
    let accepting = tokio::spawn(ServerConnection::accept_with_options(
        socket,
        "public-second-server",
        None,
        ConnectionOptions::default().idle_timeout_millis(0),
    ));
    let mut client = timeout(
        IO_TIMEOUT,
        ClientConnection::builder()
            .container_id("public-second-client")
            .idle_timeout_millis(0)
            .open_with_stream(stream),
    )
    .await??;
    let mut server = timeout(IO_TIMEOUT, accepting).await???;
    let (mut client_session, mut server_session) = timeout(IO_TIMEOUT, async {
        tokio::try_join!(client.begin(), async {
            let incoming = server
                .next_incoming_session()
                .await
                .expect("client session request");
            server.accept_session(incoming).await
        })
    })
    .await??;
    let mut receivers = Vec::new();
    let mut senders = Vec::new();
    for name in ["owner", "other"] {
        let (receiver, endpoint) = timeout(IO_TIMEOUT, async {
            tokio::try_join!(
                ClientReceiver::builder()
                    .name(name)
                    .source(Source::new("queue"))
                    .receiver_settle_mode(ReceiverSettleMode::Second)
                    .attach(&mut client_session),
                async {
                    let attach = server_session
                        .next_incoming_attach()
                        .await
                        .expect("client receiver attach");
                    assert_eq!(attach.role, Role::Receiver);
                    assert_eq!(attach.rcv_settle_mode, ReceiverSettleMode::Second);
                    server_session.accept_attach(attach, 4 * 1024 * 1024).await
                }
            )
        })
        .await??;
        let LinkEndpoint::Sender(sender) = endpoint else {
            panic!("client receiver must create a server sender");
        };
        receivers.push(receiver);
        senders.push(sender);
    }
    let (owner, other) = receivers.split_at_mut(1);
    let owner = &mut owner[0];
    let other = &mut other[0];
    let message = Message::data(b"public second mode".to_vec());
    let (pending, old) = timeout(IO_TIMEOUT, async {
        tokio::try_join!(
            senders[0].send_with_settlement(message.clone(), vec![1].into()),
            async {
                let delivery = owner.recv().await?;
                assert_eq!(delivery.message(), &message);
                assert_invalid_owner(other.reject(&delivery, None).await);
                owner.accept(&delivery).await?;
                Ok::<_, EngineError>(delivery)
            }
        )
    })
    .await??;
    assert_eq!(pending.outcome(), &Outcome::Accepted(Accepted));
    timeout(IO_TIMEOUT, pending.accept()).await??;
    timeout(IO_TIMEOUT, owner.release(&old)).await??;

    // A subsequent delivery on each link proves that the no-op old token and
    // incorrect owner did not damage either actor's independent live ledger.
    for (sender, receiver) in senders.iter_mut().zip([owner, other]) {
        let next = Message::data(b"healthy next message".to_vec());
        let (pending, ()) = timeout(IO_TIMEOUT, async {
            tokio::try_join!(
                sender.send_with_settlement(next.clone(), vec![2].into()),
                async {
                    let delivery = receiver.recv().await?;
                    assert_eq!(delivery.message(), &next);
                    receiver.accept(&delivery).await
                }
            )
        })
        .await??;
        assert_eq!(pending.outcome(), &Outcome::Accepted(Accepted));
        timeout(IO_TIMEOUT, pending.accept()).await??;
    }
    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}
