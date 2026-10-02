use std::collections::HashMap;

use amqp::{
    Accepted, Attach, Begin, Body, ConnectionOptions, Coordinator, CoordinatorEndpoint,
    CoordinatorRequest, Declare, DeliveryState, Discharge, Disposition, Flow, Frame, Message,
    NativeReadySubmission, NativeTransactionIdentity, NativeTransactionResources,
    NativeTransactionState, Open, Outcome, Performative, ProtocolHeader, ReceiverSettleMode, Role,
    SenderSettleMode, ServerConnection, ServerSession, Source, Symbol, Target, TransactionCommand,
    TransactionId, TransactionalIngress, TransactionalReceiver, TransactionalState, Transfer,
    Value, encode_message, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{io::DuplexStream, time::timeout};

use super::{DEADLINE, TestResult};

pub(super) const CONTROL: u16 = 19;
const POST: u16 = 23;
const CONTROL_HANDLE: u32 = 17;
const POST_HANDLE: u32 = 31;

pub(super) struct NativeFixture {
    connection: ServerConnection,
    peer: Peer,
    _coordinator: CoordinatorEndpoint,
    _receiver: Option<TransactionalReceiver>,
    pub(super) observer: NativeTransactionIdentity,
    posts: usize,
}

struct Peer {
    io: DuplexStream,
    transfers: HashMap<u16, u32>,
}

impl Peer {
    async fn send(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) -> TestResult {
        if matches!(&performative, Performative::Transfer(_)) {
            *self.transfers.entry(channel).or_default() += 1;
        }
        timeout(
            DEADLINE,
            write_frame(
                &mut self.io,
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

    async fn frame(&mut self) -> TestResult<Frame> {
        Ok(timeout(DEADLINE, read_frame(&mut self.io)).await??)
    }

    async fn non_flow(&mut self) -> TestResult<Frame> {
        for _ in 0..16 {
            let frame = self.frame().await?;
            if matches!(&frame, Frame::Amqp { channel: CONTROL | POST, performative: Some(Performative::Flow(_)), payload } if payload.is_empty())
            {
                continue;
            }
            return Ok(frame);
        }
        Err("native response exceeded bounded Flow allowance".into())
    }

    async fn disposition(&mut self, expected_channel: u16, id: u32) -> TestResult<Disposition> {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Disposition(disposition)),
            payload,
        } = self.non_flow().await?
        else {
            return Err("native disposition missing".into());
        };
        assert_eq!(channel, expected_channel);
        assert!(payload.is_empty());
        assert_eq!(disposition.role, Role::Receiver);
        assert_eq!(disposition.first, id);
        assert!(disposition.last.is_none());
        Ok(disposition)
    }

    async fn command(&mut self, id: u32, command: TransactionCommand) -> TestResult {
        let message = Message {
            body: Body::Value(Value::from(command)),
            ..Message::default()
        };
        self.send(
            CONTROL,
            Performative::Transfer(first(CONTROL_HANDLE, id, None)),
            encode_message(&message)?,
        )
        .await
    }
}

fn first(handle: u32, id: u32, transaction: Option<TransactionId>) -> Transfer {
    Transfer {
        handle,
        delivery_id: Some(id),
        delivery_tag: Some(vec![id as u8].into()),
        message_format: Some(0),
        settled: Some(false),
        more: false,
        rcv_settle_mode: None,
        state: transaction.map(|txn_id| {
            DeliveryState::Transactional(TransactionalState {
                txn_id,
                outcome: None,
            })
        }),
        resume: false,
        aborted: false,
        batchable: false,
    }
}

fn sender_attach(name: &str, handle: u32) -> Attach {
    Attach {
        name: name.to_owned(),
        handle,
        role: Role::Sender,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: None,
        target: None,
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: Some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

async fn session(
    connection: &mut ServerConnection,
    peer: &mut Peer,
    channel: u16,
) -> TestResult<ServerSession> {
    peer.send(channel, Performative::Begin(Begin::default()), Vec::new())
        .await?;
    let incoming = timeout(DEADLINE, connection.next_incoming_session())
        .await?
        .ok_or("native incoming session missing")?;
    let (session, frame) = tokio::join!(connection.accept_session(incoming), peer.non_flow());
    let Frame::Amqp {
        channel: actual,
        performative: Some(Performative::Begin(begin)),
        payload,
    } = frame?
    else {
        return Err("native Begin response missing".into());
    };
    assert_eq!(actual, channel);
    assert_eq!(begin.remote_channel, Some(channel));
    assert!(payload.is_empty());
    Ok(session?)
}

async fn attached(peer: &mut Peer, channel: u16, handle: u32) -> TestResult {
    let Frame::Amqp {
        channel: actual,
        performative: Some(Performative::Attach(attach)),
        payload,
    } = peer.non_flow().await?
    else {
        return Err("native Attach response missing".into());
    };
    assert_eq!(actual, channel);
    assert_eq!(attach.handle, handle);
    assert!(payload.is_empty());
    let Frame::Amqp {
        channel: actual,
        performative: Some(Performative::Flow(flow)),
        payload,
    } = peer.frame().await?
    else {
        return Err("native link credit missing".into());
    };
    assert_eq!(actual, channel);
    assert_eq!(flow.handle, Some(handle));
    assert!(flow.link_credit.is_some_and(|credit| credit > 0));
    assert!(payload.is_empty());
    Ok(())
}

impl NativeFixture {
    pub(super) async fn ready(empty: bool) -> TestResult<(Self, NativeReadySubmission)> {
        timeout(DEADLINE, async {
            let (server, io) = tokio::io::duplex(16_384);
            let mut peer = Peer { io, transfers: HashMap::new() };
            let opening = async {
                write_protocol_header(&mut peer.io, ProtocolHeader::AMQP).await?;
                assert_eq!(read_protocol_header(&mut peer.io).await?, ProtocolHeader::AMQP);
                peer.send(0, Performative::Open(Open::new("native-owner-peer")), Vec::new()).await?;
                assert!(matches!(peer.frame().await?, Frame::Amqp { performative: Some(Performative::Open(_)), .. }));
                Ok::<_, Box<dyn std::error::Error>>(peer)
            };
            let (connection, peer) = tokio::join!(ServerConnection::accept_with_transactional_ingress(server, "native-owner-server", None, ConnectionOptions::default()), opening);
            let mut connection = connection?;
            let mut peer = peer?;
            let mut control = session(&mut connection, &mut peer, CONTROL).await?;
            let mut attach = sender_attach("native-control", CONTROL_HANDLE);
            attach.snd_settle_mode = SenderSettleMode::Unsettled;
            attach.rcv_settle_mode = ReceiverSettleMode::First;
            attach.initial_delivery_count = Some(0);
            attach.target = Some(Coordinator::default().into());
            attach.source = Some(Source { outcomes: Some(vec![Symbol::from("amqp:declared:list"), Symbol::from("amqp:accepted:list"), Symbol::from("amqp:rejected:list")].into()), ..Source::default() });
            peer.send(CONTROL, Performative::Attach(Box::new(attach)), Vec::new()).await?;
            let incoming = control.next_incoming_attach().await.ok_or("native coordinator approval missing")?;
            let (coordinator, response) = tokio::join!(control.accept_coordinator(incoming, 0), attached(&mut peer, CONTROL, CONTROL_HANDLE));
            response?;
            let mut coordinator = coordinator?;
            let transaction = TransactionId::new([91])?;
            peer.command(0, TransactionCommand::Declare(Declare::default())).await?;
            let CoordinatorRequest::Declare(receipt) = coordinator.recv().await? else { return Err("native Declare receipt missing".into()); };
            let (observer, declared) = tokio::join!(receipt.declared(transaction.clone()), peer.disposition(CONTROL, 0));
            let observer = observer?;
            let declared = declared?;
            assert!(declared.settled);
            assert!(matches!(declared.state, Some(DeliveryState::Declared(amqp::Declared { txn_id })) if txn_id == transaction));
            let (receiver, prepared) = if empty {
                (None, Vec::new())
            } else {
                let mut data = session(&mut connection, &mut peer, POST).await?;
                let mut attach = sender_attach("native-data", POST_HANDLE);
                attach.snd_settle_mode = SenderSettleMode::Unsettled;
                attach.rcv_settle_mode = ReceiverSettleMode::First;
                attach.initial_delivery_count = Some(0);
                attach.source = Some(Source::default());
                attach.target = Some(Target::new("orders").into());
                peer.send(POST, Performative::Attach(Box::new(attach)), Vec::new()).await?;
                let incoming = data.next_incoming_attach().await.ok_or("native data approval missing")?;
                let (receiver, response) = tokio::join!(data.accept_transactional_receiver(incoming, 0), attached(&mut peer, POST, POST_HANDLE));
                response?;
                let mut receiver = receiver?;
                let message = Message::data(b"retained native posting".to_vec());
                peer.send(POST, Performative::Transfer(first(POST_HANDLE, 0, Some(transaction.clone()))), encode_message(&message)?).await?;
                let TransactionalIngress::Posting(posting) = receiver.recv().await? else { return Err("native posting missing".into()); };
                assert_eq!(posting.message(), &message);
                let (prepared, provisional) = tokio::join!(posting.provisional_accept(), peer.disposition(POST, 0));
                let provisional = provisional?;
                assert!(!provisional.settled);
                assert!(matches!(provisional.state, Some(DeliveryState::Transactional(TransactionalState { txn_id, outcome: Some(Outcome::Accepted(_)) })) if txn_id == transaction));
                (Some(receiver), vec![prepared?])
            };
            peer.command(1, TransactionCommand::Discharge(Discharge { txn_id: transaction, fail: Some(false) })).await?;
            let CoordinatorRequest::Discharge(sealed) = coordinator.recv().await? else { return Err("native Discharge missing".into()); };
            sealed.wait_ready().await?;
            assert_eq!(observer.state(), NativeTransactionState::Ready);
            let ready = sealed.prepare(prepared)?;
            Ok((Self { connection, peer, _coordinator: coordinator, _receiver: receiver, observer, posts: usize::from(!empty) }, ready))
        }).await?
    }

    pub(super) async fn close_data(&mut self) -> TestResult {
        self.peer
            .send(
                POST,
                Performative::Detach(amqp::Detach {
                    handle: POST_HANDLE,
                    closed: true,
                    error: None,
                }),
                Vec::new(),
            )
            .await?;
        let Frame::Amqp {
            channel: POST,
            performative: Some(Performative::Detach(detach)),
            payload,
        } = self.peer.non_flow().await?
        else {
            return Err("native data Detach acknowledgement missing".into());
        };
        assert_eq!(detach.handle, POST_HANDLE);
        assert!(detach.closed);
        assert!(detach.error.is_none());
        assert!(payload.is_empty());
        self.posts = 0;
        Ok(())
    }

    pub(super) async fn barrier(&mut self) -> TestResult {
        self.peer
            .send(
                CONTROL,
                Performative::Flow(Flow {
                    next_incoming_id: Some(0),
                    incoming_window: 1_000,
                    next_outgoing_id: self.peer.transfers.get(&CONTROL).copied().unwrap_or(0),
                    outgoing_window: 1_000,
                    handle: None,
                    delivery_count: None,
                    link_credit: None,
                    available: None,
                    drain: false,
                    echo: true,
                    properties: None,
                }),
                Vec::new(),
            )
            .await?;
        for _ in 0..16 {
            match self.peer.frame().await? {
                Frame::Amqp {
                    channel: CONTROL,
                    performative: Some(Performative::Flow(flow)),
                    payload,
                } if flow.handle.is_none() && payload.is_empty() => return Ok(()),
                Frame::Amqp {
                    channel: CONTROL | POST,
                    performative: Some(Performative::Flow(_)),
                    payload,
                } if payload.is_empty() => {}
                frame => return Err(format!("unexpected native barrier frame: {frame:?}").into()),
            }
        }
        Err("native barrier response missing".into())
    }

    pub(super) async fn finish(
        &mut self,
        resources: NativeTransactionResources,
        expected: NativeTransactionState,
    ) -> TestResult {
        let replies = async {
            if expected == NativeTransactionState::Indeterminate {
                let Frame::Amqp {
                    channel: CONTROL,
                    performative: Some(Performative::Detach(detach)),
                    ..
                } = self.peer.non_flow().await?
                else {
                    return Err("indeterminate native control refusal missing".into());
                };
                assert_eq!(detach.handle, CONTROL_HANDLE);
                assert_eq!(
                    detach
                        .error
                        .ok_or("indeterminate error missing")?
                        .condition
                        .as_symbol()
                        .as_str(),
                    "amqp:internal-error"
                );
                return Ok(());
            }
            for _ in 0..self.posts {
                let post = self.peer.disposition(POST, 0).await?;
                assert!(post.settled);
                if expected == NativeTransactionState::Committed {
                    assert!(matches!(
                        post.state,
                        Some(DeliveryState::Accepted(Accepted))
                    ));
                } else {
                    assert!(post.state.is_none());
                }
            }
            let control = self.peer.disposition(CONTROL, 1).await?;
            assert!(control.settled);
            if expected == NativeTransactionState::Committed {
                assert!(matches!(control.state, Some(DeliveryState::Accepted(_))));
            } else {
                assert!(
                    matches!(control.state, Some(DeliveryState::Rejected(amqp::Rejected { error: Some(error) })) if error.condition.as_symbol().as_str() == "amqp:transaction:rollback")
                );
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        };
        let (result, replies) = tokio::join!(timeout(DEADLINE, resources.finish()), replies);
        replies?;
        if expected == NativeTransactionState::Indeterminate {
            assert!(result?.is_err());
        } else {
            result??;
        }
        assert_eq!(self.observer.state(), expected);
        Ok(())
    }

    pub(super) async fn shutdown(&self) -> TestResult {
        timeout(DEADLINE, self.connection.shutdown()).await?;
        Ok(())
    }
}
