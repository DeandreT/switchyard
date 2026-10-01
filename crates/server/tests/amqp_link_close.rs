//! Consuming a queued delivery must not grant link credit after local Detach.

use std::{error::Error, time::Duration};

use amqp::{
    Accepted, Begin, ClientConnection, ClientReceiver, ClientSession, Close, DeliveryState, Detach,
    Flow, Frame, Message, Open, Performative, ProtocolHeader, Role, Source, Transfer,
    encode_message, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const CLOSING_HANDLE: u32 = 0;
const SIBLING_HANDLE: u32 = 1;

struct Peer {
    stream: TcpStream,
    next_outgoing_id: u32,
}

impl Peer {
    async fn send(&mut self, performative: Performative, payload: Vec<u8>) -> TestResult {
        if matches!(performative, Performative::Transfer(_)) {
            self.next_outgoing_id = self.next_outgoing_id.wrapping_add(1);
        }
        timeout(
            IO_TIMEOUT,
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

    async fn read(&mut self) -> TestResult<Performative> {
        let frame = timeout(IO_TIMEOUT, read_frame(&mut self.stream)).await??;
        let Frame::Amqp {
            channel: 0,
            performative: Some(performative),
            payload,
        } = frame
        else {
            panic!("expected channel-zero control frame: {frame:?}");
        };
        assert!(payload.is_empty());
        Ok(performative)
    }

    async fn read_after_local_detach(&mut self) -> TestResult<Performative> {
        let performative = self.read().await?;
        assert!(
            !matches!(&performative, Performative::Flow(flow) if flow.handle == Some(CLOSING_HANDLE)),
            "a locally closing receiver must not publish link Flow: {performative:?}"
        );
        assert!(
            !matches!(&performative, Performative::End(_) | Performative::Close(_)),
            "closing one link must not terminate its healthy sibling: {performative:?}"
        );
        Ok(performative)
    }

    async fn barrier(&mut self, after_detach: bool) -> TestResult {
        self.send(
            Performative::Flow(Flow {
                next_incoming_id: Some(0),
                incoming_window: 2_048,
                next_outgoing_id: self.next_outgoing_id,
                outgoing_window: 2_048,
                echo: true,
                ..Flow::default()
            }),
            Vec::new(),
        )
        .await?;
        loop {
            let performative = if after_detach {
                self.read_after_local_detach().await?
            } else {
                self.read().await?
            };
            match performative {
                Performative::Flow(flow) if flow.handle.is_none() => {
                    assert_eq!(flow.next_incoming_id, Some(self.next_outgoing_id));
                    return Ok(());
                }
                Performative::Flow(_) => {}
                other => panic!("session Flow barrier expected: {other:?}"),
            }
        }
    }

    async fn delivery(&mut self, handle: u32, id: u32, settled: bool) -> TestResult<Message> {
        let message = Message::data(id.to_be_bytes().to_vec());
        self.send(
            Performative::Transfer(Transfer {
                handle,
                delivery_id: Some(id),
                delivery_tag: Some(id.to_be_bytes().to_vec().into()),
                message_format: Some(0),
                settled: Some(settled),
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            }),
            encode_message(&message)?,
        )
        .await?;
        Ok(message)
    }
}

async fn open() -> TestResult<(ClientConnection, Peer)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let stream = TcpStream::connect(listener.local_addr()?).await?;
    stream.set_nodelay(true)?;
    let (mut socket, _) = listener.accept().await?;
    socket.set_nodelay(true)?;
    let opening = tokio::spawn(
        ClientConnection::builder()
            .container_id("closing-link-client")
            .idle_timeout_millis(0)
            .open_with_stream(stream),
    );
    assert_eq!(
        timeout(IO_TIMEOUT, read_protocol_header(&mut socket)).await??,
        ProtocolHeader::AMQP
    );
    timeout(
        IO_TIMEOUT,
        write_protocol_header(&mut socket, ProtocolHeader::AMQP),
    )
    .await??;
    assert!(matches!(
        timeout(IO_TIMEOUT, read_frame(&mut socket)).await??,
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(_)),
            ..
        }
    ));
    timeout(
        IO_TIMEOUT,
        write_frame(
            &mut socket,
            &Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(Open::new("raw-closing-link-server"))),
                payload: Vec::new(),
            },
        ),
    )
    .await??;
    let connection = timeout(IO_TIMEOUT, opening).await???;
    Ok((
        connection,
        Peer {
            stream: socket,
            next_outgoing_id: 0,
        },
    ))
}

async fn begin(connection: &mut ClientConnection, peer: &mut Peer) -> TestResult<ClientSession> {
    let (session, ()) = timeout(IO_TIMEOUT, async {
        tokio::try_join!(
            async { Ok::<_, Box<dyn Error>>(connection.begin().await?) },
            async {
                let Performative::Begin(begin) = peer.read().await? else {
                    panic!("client session Begin");
                };
                assert!(begin.remote_channel.is_none());
                peer.send(
                    Performative::Begin(Begin {
                        remote_channel: Some(0),
                        ..Begin::default()
                    }),
                    Vec::new(),
                )
                .await
            }
        )
    })
    .await??;
    Ok(session)
}

async fn attach(
    session: &mut ClientSession,
    peer: &mut Peer,
    handle: u32,
) -> TestResult<ClientReceiver> {
    let (receiver, ()) = timeout(IO_TIMEOUT, async {
        tokio::try_join!(
            async {
                Ok::<_, Box<dyn Error>>(
                    ClientReceiver::builder()
                        .name(format!("closing-test-{handle}"))
                        .source(Source::new("queue"))
                        .sender_settle_mode(amqp::SenderSettleMode::Mixed)
                        .attach(session)
                        .await?,
                )
            },
            async {
                let Performative::Attach(request) = peer.read().await? else {
                    panic!("client receiver Attach");
                };
                assert_eq!(request.handle, handle);
                assert_eq!(request.role, Role::Receiver);
                let response = request.response(request.source.clone(), request.target.clone());
                peer.send(Performative::Attach(Box::new(response)), Vec::new())
                    .await?;
                let Performative::Flow(flow) = peer.read().await? else {
                    panic!("initial receiving link credit");
                };
                assert_eq!(flow.handle, Some(handle));
                assert_eq!(flow.delivery_count, Some(0));
                assert_eq!(flow.link_credit, Some(32));
                Ok::<_, Box<dyn Error>>(())
            }
        )
    })
    .await??;
    Ok(receiver)
}

#[tokio::test]
async fn consuming_a_queued_delivery_after_local_detach_cannot_refill_the_closing_link()
-> TestResult {
    let (mut connection, mut peer) = open().await?;
    let mut session = begin(&mut connection, &mut peer).await?;
    let mut closing = attach(&mut session, &mut peer, CLOSING_HANDLE).await?;
    let mut sibling = attach(&mut session, &mut peer, SIBLING_HANDLE).await?;
    let closing_message = peer.delivery(CLOSING_HANDLE, 0, true).await?;
    let sibling_message = peer.delivery(SIBLING_HANDLE, 1, true).await?;
    peer.barrier(false).await?;

    // Withhold the peer acknowledgement until Detach is observed, then cancel
    // only the waiting API future. The queued delivery is consumed afterward,
    // so its notification cannot run before the actor starts closing the link.
    timeout(IO_TIMEOUT, async {
        tokio::select! {
            result = closing.close() => {
                panic!("local close must await the held peer acknowledgement: {result:?}");
            }
            result = async {
                let Performative::Detach(detach) = peer.read().await? else {
                    panic!("local receiver Detach");
                };
                assert_eq!(detach.handle, CLOSING_HANDLE);
                assert!(detach.closed);
                assert!(detach.error.is_none());
                Ok::<_, Box<dyn Error>>(())
            } => result,
        }
    })
    .await??;

    let delivery = timeout(IO_TIMEOUT, closing.recv()).await??;
    assert_eq!(delivery.message(), &closing_message);
    let delivery = timeout(IO_TIMEOUT, sibling.recv()).await??;
    assert_eq!(delivery.message(), &sibling_message);

    // Seeing the sibling refill proves consumption processing ran. The session
    // echo then orders the entire refresh pass, including either map order.
    loop {
        match peer.read_after_local_detach().await? {
            Performative::Flow(flow) if flow.handle == Some(SIBLING_HANDLE) => {
                assert_eq!(flow.delivery_count, Some(1));
                assert_eq!(flow.link_credit, Some(32));
                break;
            }
            Performative::Flow(_) => {}
            other => panic!("healthy sibling refill expected: {other:?}"),
        }
    }
    peer.barrier(true).await?;
    peer.send(
        Performative::Detach(Detach {
            handle: CLOSING_HANDLE,
            closed: true,
            error: None,
        }),
        Vec::new(),
    )
    .await?;
    let message = peer.delivery(SIBLING_HANDLE, 2, false).await?;
    let delivery = timeout(IO_TIMEOUT, sibling.recv()).await??;
    assert_eq!(delivery.message(), &message);
    timeout(IO_TIMEOUT, sibling.accept(&delivery)).await??;
    loop {
        match peer.read_after_local_detach().await? {
            Performative::Disposition(disposition) => {
                assert_eq!(disposition.role, Role::Receiver);
                assert_eq!(disposition.first, 2);
                assert_eq!(disposition.last, None);
                assert!(disposition.settled);
                assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
                break;
            }
            Performative::Flow(_) => {}
            other => panic!("healthy sibling outcome expected: {other:?}"),
        }
    }
    peer.barrier(true).await?;
    let (closed, peer_closed) = timeout(IO_TIMEOUT, async {
        tokio::join!(connection.close(), async {
            loop {
                match peer.read().await? {
                    Performative::Close(close) => {
                        assert!(close.error.is_none());
                        peer.send(Performative::Close(Close::default()), Vec::new())
                            .await?;
                        return Ok::<_, Box<dyn Error>>(());
                    }
                    Performative::Flow(flow) => {
                        assert_ne!(flow.handle, Some(CLOSING_HANDLE));
                    }
                    other => panic!("clean connection Close expected: {other:?}"),
                }
            }
        })
    })
    .await?;
    closed?;
    peer_closed?;
    connection.shutdown().await;
    Ok(())
}
