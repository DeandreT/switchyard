use std::{
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};

use tokio::io::{AsyncReadExt, DuplexStream, ReadBuf};

use super::*;
use crate::{RetainedDelivery, Source, Target};

const DEADLINE: Duration = Duration::from_secs(3);
const PEER_CHANNEL: u16 = 7;
const PEER_HANDLE: u32 = 11;

struct FaultIo {
    inner: DuplexStream,
    panic_write: Arc<AtomicBool>,
}

impl AsyncRead for FaultIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, bytes)
    }
}

impl AsyncWrite for FaultIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        assert!(
            !self.panic_write.load(Ordering::Acquire),
            "driver write unwind"
        );
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

async fn next_frame(peer: &mut DuplexStream) -> Frame {
    tokio::time::timeout(DEADLINE, read_frame(peer))
        .await
        .expect("bounded frame response")
        .expect("valid AMQP frame")
}

async fn eof(peer: &mut DuplexStream) {
    let mut remaining = Vec::new();
    tokio::time::timeout(DEADLINE, peer.read_to_end(&mut remaining))
        .await
        .expect("native reader and writer release the socket")
        .expect("read until EOF");
    assert!(
        remaining.is_empty(),
        "unexpected bytes before EOF: {remaining:?}"
    );
}

async fn server_pair() -> (ServerConnection, DuplexStream, Arc<AtomicBool>) {
    tokio::time::timeout(DEADLINE, async {
        let (wire, mut peer) = tokio::io::duplex(4_096);
        let panic_write = Arc::new(AtomicBool::new(false));
        let opening = async {
            write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("peer header");
            expect_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("server header");
            write_amqp(
                &mut peer,
                0,
                Performative::Open(Open::new("same-container")),
                Vec::new(),
            )
            .await
            .expect("peer Open");
            assert!(matches!(
                next_frame(&mut peer).await,
                Frame::Amqp {
                    performative: Some(Performative::Open(_)),
                    ..
                }
            ));
            peer
        };
        let stream = FaultIo {
            inner: wire,
            panic_write: Arc::clone(&panic_write),
        };
        let (connection, peer) = tokio::join!(
            ServerConnection::accept(stream, "same-container", None),
            opening
        );
        (connection.expect("server connection"), peer, panic_write)
    })
    .await
    .expect("bounded server negotiation")
}

#[cfg(feature = "test-client")]
async fn client_pair() -> (ClientConnection, DuplexStream, Arc<AtomicBool>) {
    tokio::time::timeout(DEADLINE, async {
        let (wire, mut peer) = tokio::io::duplex(4_096);
        let panic_write = Arc::new(AtomicBool::new(false));
        let opening = async {
            expect_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("client header");
            write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("peer header");
            assert!(matches!(
                next_frame(&mut peer).await,
                Frame::Amqp {
                    performative: Some(Performative::Open(_)),
                    ..
                }
            ));
            write_amqp(
                &mut peer,
                0,
                Performative::Open(Open::new("same-container")),
                Vec::new(),
            )
            .await
            .expect("peer Open");
            peer
        };
        let stream = FaultIo {
            inner: wire,
            panic_write: Arc::clone(&panic_write),
        };
        let (connection, peer) = tokio::join!(
            ClientConnection::open(stream, "same-container", None),
            opening
        );
        (connection.expect("client connection"), peer, panic_write)
    })
    .await
    .expect("bounded client negotiation")
}

async fn server_receipt(
    connection: &mut ServerConnection,
    peer: &mut DuplexStream,
) -> (ServerSession, Receiver, RetainedDelivery) {
    tokio::time::timeout(DEADLINE, async {
        write_amqp(
            peer,
            PEER_CHANNEL,
            Performative::Begin(Begin::default()),
            Vec::new(),
        )
        .await
        .expect("peer Begin");
        let incoming = connection
            .next_incoming_session()
            .await
            .expect("incoming session");
        let (session, frame) = tokio::join!(connection.accept_session(incoming), next_frame(peer));
        let mut session = session.expect("accepted session");
        assert!(matches!(
            frame,
            Frame::Amqp {
                channel: PEER_CHANNEL,
                performative: Some(Performative::Begin(_)),
                ..
            }
        ));
        let attach = Attach {
            name: String::from("same-link"),
            handle: PEER_HANDLE,
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(Source::new("same-queue")),
            target: Some(Target::new("same-queue").into()),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: Some(0),
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };
        write_amqp(
            peer,
            PEER_CHANNEL,
            Performative::Attach(Box::new(attach)),
            Vec::new(),
        )
        .await
        .expect("peer Attach");
        let incoming = session
            .next_incoming_attach()
            .await
            .expect("incoming Attach");
        let accepting = session.accept_attach(incoming, 0);
        let responses = async {
            assert!(matches!(
                next_frame(peer).await,
                Frame::Amqp {
                    performative: Some(Performative::Attach(_)),
                    ..
                }
            ));
            assert!(matches!(
                next_frame(peer).await,
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                }
            ));
        };
        let (endpoint, ()) = tokio::join!(accepting, responses);
        let LinkEndpoint::Receiver(mut receiver) = endpoint.expect("receiving endpoint") else {
            panic!("receiver endpoint")
        };
        post(peer, PEER_CHANNEL, PEER_HANDLE).await;
        let receipt = tokio::time::timeout(DEADLINE, receiver.recv_retained())
            .await
            .expect("bounded receipt")
            .expect("retained receipt");
        (session, receiver, receipt)
    })
    .await
    .expect("bounded session, link, and delivery admission")
}

async fn post(peer: &mut DuplexStream, channel: u16, handle: u32) {
    write_amqp(
        peer,
        channel,
        Performative::Transfer(Transfer {
            handle,
            delivery_id: Some(0),
            delivery_tag: Some(vec![42].into()),
            message_format: Some(0),
            settled: Some(false),
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
        }),
        encode_message(&Message::data(b"same-message".to_vec())).expect("message encoding"),
    )
    .await
    .expect("peer Transfer");
}

async fn disposition_frame(peer: &mut DuplexStream) {
    for _ in 0..3 {
        match next_frame(peer).await {
            Frame::Amqp {
                performative: Some(Performative::Disposition(disposition)),
                ..
            } => {
                assert_eq!(disposition.first, 0);
                assert!(disposition.settled);
                assert!(matches!(
                    disposition.state,
                    Some(DeliveryState::Accepted(_))
                ));
                return;
            }
            Frame::Amqp {
                performative: Some(Performative::Flow(_)),
                ..
            } => {}
            frame => panic!("unexpected settlement response: {frame:?}"),
        }
    }
    panic!("no terminal disposition within bounded response frames");
}

async fn server_close(connection: &ServerConnection, peer: &mut DuplexStream) {
    let answering = async {
        let mut closing = false;
        for _ in 0..3 {
            match next_frame(peer).await {
                Frame::Amqp {
                    performative: Some(Performative::Close(_)),
                    ..
                } => {
                    closing = true;
                    break;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(flow)),
                    ..
                } if flow.handle == Some(PEER_HANDLE) => {}
                frame => panic!("unexpected connection Close response: {frame:?}"),
            }
        }
        assert!(closing, "bounded connection Close response");
        write_amqp(peer, 0, Performative::Close(Close::default()), Vec::new())
            .await
            .expect("peer Close ACK");
    };
    let (result, ()) = tokio::join!(connection.close(), answering);
    result.expect("graceful server Close");
    eof(peer).await;
}

#[tokio::test]
async fn observer_clones_are_inert_and_identity_stays_stable_after_retirement() {
    let (connection, mut peer, _) = server_pair().await;
    let identity = connection.connection_identity().clone();
    let clone = identity.clone();
    assert!(identity.is_active());
    assert!(identity.same_connection(&clone));
    assert!(identity.same_connection(connection.connection_identity()));
    assert_eq!(
        format!("{identity:?}"),
        "NativeConnectionIdentity { active: true }"
    );
    drop(clone);
    assert!(identity.is_active());
    server_close(&connection, &mut peer).await;
    assert!(!identity.is_active());
    assert_eq!(
        format!("{identity:?}"),
        "NativeConnectionIdentity { active: false }"
    );
    assert!(identity.same_connection(connection.connection_identity()));
    let clone = identity.clone();
    assert!(clone.same_connection(&identity));
    assert!(!clone.is_active());
}

#[tokio::test]
async fn retained_receipts_use_exact_connection_not_identical_numeric_labels() {
    let (mut first, mut first_peer, _) = server_pair().await;
    let (mut second, mut second_peer, _) = server_pair().await;
    let (_first_session, first_receiver, first_receipt) =
        server_receipt(&mut first, &mut first_peer).await;
    let (_second_session, second_receiver, second_receipt) =
        server_receipt(&mut second, &mut second_peer).await;
    assert_eq!(first_receiver.channel, second_receiver.channel);
    assert_eq!(first_receiver.handle, second_receiver.handle);
    assert_eq!(
        first_receipt.inner().identity.id(),
        second_receipt.inner().identity.id()
    );
    assert_eq!(first_receipt.message(), second_receipt.message());
    assert!(first_receipt.belongs_to_connection(first.connection_identity()));
    assert!(second_receipt.belongs_to_connection(second.connection_identity()));
    assert!(!first_receipt.belongs_to_connection(second.connection_identity()));
    assert!(!second_receipt.belongs_to_connection(first.connection_identity()));
    assert!(
        !first
            .connection_identity()
            .same_connection(second.connection_identity())
    );
    assert!(matches!(
        second_receiver.accept_retained(&first_receipt).await,
        Err(EngineError::InvalidState(_))
    ));
    assert!(
        first_receipt
            .connection_identity()
            .expect("bound receipt")
            .same_connection(first.connection_identity())
    );
    first.shutdown().await;
    assert!(!first_receipt.belongs_to_connection(first.connection_identity()));
    assert!(
        first_receipt
            .connection_identity()
            .expect("stable proof")
            .same_connection(first.connection_identity())
    );
    assert!(second_receipt.belongs_to_connection(second.connection_identity()));
    second.shutdown().await;
    assert!(!second_receipt.belongs_to_connection(second.connection_identity()));
}

#[tokio::test]
async fn settlement_and_session_retirement_do_not_retire_connection_proof() {
    let (mut connection, mut peer, _) = server_pair().await;
    let (mut session, receiver, receipt) = server_receipt(&mut connection, &mut peer).await;
    let (result, ()) = tokio::join!(
        receiver.accept_retained(&receipt),
        disposition_frame(&mut peer)
    );
    result.expect("ordinary acceptance");
    assert!(receipt.belongs_to_connection(connection.connection_identity()));
    write_amqp(
        &mut peer,
        PEER_CHANNEL,
        Performative::End(End::default()),
        Vec::new(),
    )
    .await
    .expect("peer session End");
    let mut ended = false;
    for _ in 0..3 {
        match next_frame(&mut peer).await {
            Frame::Amqp {
                performative: Some(Performative::End(_)),
                ..
            } => {
                ended = true;
                break;
            }
            Frame::Amqp {
                performative: Some(Performative::Flow(_)),
                ..
            } => {}
            frame => panic!("unexpected session End response: {frame:?}"),
        }
    }
    assert!(ended, "bounded session End acknowledgement");
    assert!(
        tokio::time::timeout(DEADLINE, session.next_incoming_attach())
            .await
            .expect("session retirement")
            .is_none()
    );
    assert!(receiver.identity.is_retired());
    assert!(session.identity.is_retired());
    assert!(connection.connection_identity().is_active());
    assert!(receipt.belongs_to_connection(connection.connection_identity()));
    server_close(&connection, &mut peer).await;
    assert!(!receipt.belongs_to_connection(connection.connection_identity()));
    assert_eq!(receipt.message(), &Message::data(b"same-message".to_vec()));
}

#[tokio::test]
async fn server_eof_protocol_failure_and_explicit_shutdown_retire_observers() {
    for cause in 0..3 {
        let (connection, mut peer, _) = server_pair().await;
        let identity = connection.connection_identity().clone();
        match cause {
            0 => drop(peer),
            1 => {
                write_amqp(
                    &mut peer,
                    0,
                    Performative::Open(Open::new("duplicate")),
                    Vec::new(),
                )
                .await
                .expect("invalid duplicate Open");
                eof(&mut peer).await;
            }
            2 => {
                tokio::time::timeout(DEADLINE, connection.shutdown())
                    .await
                    .expect("bounded shutdown");
                eof(&mut peer).await;
            }
            _ => unreachable!(),
        }
        tokio::time::timeout(DEADLINE, connection.shutdown())
            .await
            .expect("termination barrier");
        assert!(!identity.is_active());
        assert!(identity.same_connection(connection.connection_identity()));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_connection_requests_cancellation_until_actor_runs() {
    let (mut connection, mut peer, _) = server_pair().await;
    let (_session, _receiver, receipt) = server_receipt(&mut connection, &mut peer).await;
    let identity = connection.connection_identity().clone();
    drop(connection);
    assert!(
        identity.is_active(),
        "handle Drop cannot impersonate actor retirement"
    );
    assert!(receipt.belongs_to_connection(&identity));
    eof(&mut peer).await;
    assert!(!identity.is_active());
    assert!(!receipt.belongs_to_connection(&identity));
    assert!(
        receipt
            .connection_identity()
            .expect("receipt keeps proof")
            .same_connection(&identity)
    );
}

#[tokio::test]
async fn server_driver_unwind_retires_before_shutdown_completion() {
    let (connection, _peer, panic_write) = server_pair().await;
    let identity = connection.connection_identity().clone();
    panic_write.store(true, Ordering::Release);
    let result = tokio::time::timeout(DEADLINE, connection.close())
        .await
        .expect("unwind still signals termination");
    assert!(matches!(result, Err(EngineError::Stopped)));
    assert!(!identity.is_active());
}

#[tokio::test]
async fn unbound_synthetic_receipt_fails_closed_for_real_connection() {
    let (connection, _peer, _) = server_pair().await;
    let mut receipts = Vec::new();
    for owner in [LinkIdentity::new(), SessionIdentity::new().new_link()] {
        let mut ledger = IncomingLedger::new();
        let identity = ledger
            .reserve(&owner, 0, &[42])
            .expect("synthetic reservation");
        let receipt = RetainedDelivery::new(Delivery {
            id: 0,
            settled: false,
            message_format: 0,
            message: Message::data(b"same-message".to_vec()),
            identity,
            content_lease: None,
        });
        assert!(receipt.connection_identity().is_none());
        assert!(!receipt.belongs_to_connection(connection.connection_identity()));
        receipts.push(receipt);
    }
    connection.shutdown().await;
    for receipt in receipts {
        assert!(receipt.connection_identity().is_none());
        assert!(!receipt.belongs_to_connection(connection.connection_identity()));
    }
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn client_eof_protocol_failure_and_shutdown_retire_exact_observer() {
    for cause in 0..3 {
        let (connection, mut peer, _) = client_pair().await;
        let identity = connection.connection_identity().clone();
        assert!(identity.same_connection(connection.connection_identity()));
        match cause {
            0 => drop(peer),
            1 => {
                write_amqp(
                    &mut peer,
                    0,
                    Performative::Open(Open::new("duplicate")),
                    Vec::new(),
                )
                .await
                .expect("invalid duplicate Open");
                eof(&mut peer).await;
            }
            2 => {
                tokio::time::timeout(DEADLINE, connection.shutdown())
                    .await
                    .expect("bounded client shutdown");
                eof(&mut peer).await;
            }
            _ => unreachable!(),
        }
        tokio::time::timeout(DEADLINE, connection.wait_terminated())
            .await
            .expect("client termination fence");
        assert!(!identity.is_active());
        assert!(identity.same_connection(connection.connection_identity()));
    }
}

#[cfg(feature = "test-client")]
#[tokio::test(flavor = "current_thread")]
async fn dropping_client_handle_only_requests_actor_cancellation() {
    let (connection, mut peer, _) = client_pair().await;
    let identity = connection.connection_identity().clone();
    let clone = identity.clone();
    drop(clone);
    drop(connection);
    assert!(identity.is_active());
    eof(&mut peer).await;
    assert!(!identity.is_active());
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn client_driver_unwind_retires_before_shutdown_completion() {
    let (connection, _peer, panic_write) = client_pair().await;
    let identity = connection.connection_identity().clone();
    panic_write.store(true, Ordering::Release);
    let result = tokio::time::timeout(DEADLINE, connection.close())
        .await
        .expect("client unwind signals termination");
    assert!(matches!(result, Err(EngineError::Stopped)));
    assert!(!identity.is_active());
}

#[tokio::test]
async fn canceling_unpolled_actor_future_retires_before_termination_signal() {
    let (lifecycle, _cancellation, exit) = ConnectionLifecycle::new();
    let identity = lifecycle.identity.clone();
    let future = Box::pin(async move {
        let _actor_exit = exit;
        std::future::pending::<()>().await;
    });
    assert!(identity.is_active());
    assert!(!*lifecycle.terminated.borrow());
    drop(future);
    assert!(!identity.is_active());
    assert!(*lifecycle.terminated.borrow());
    tokio::time::timeout(DEADLINE, lifecycle.wait_terminated())
        .await
        .expect("pre-first-poll retirement fence");
}

#[tokio::test]
async fn canceling_started_actor_future_retires_before_termination_signal() {
    let (lifecycle, _cancellation, exit) = ConnectionLifecycle::new();
    let identity = lifecycle.identity.clone();
    let (started, starting) = oneshot::channel();
    let task = tokio::spawn(async move {
        let _actor_exit = exit;
        started.send(()).expect("observed first actor poll");
        std::future::pending::<()>().await;
    });
    tokio::time::timeout(DEADLINE, starting)
        .await
        .expect("bounded start")
        .expect("actor starts");
    assert!(identity.is_active());
    task.abort();
    assert!(
        tokio::time::timeout(DEADLINE, task)
            .await
            .expect("bounded abort")
            .expect_err("task cancellation")
            .is_cancelled()
    );
    tokio::time::timeout(DEADLINE, lifecycle.wait_terminated())
        .await
        .expect("termination publication");
    assert!(!identity.is_active());
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn opposite_drivers_keep_distinct_receipt_proofs_through_settlement_and_end() {
    tokio::time::timeout(DEADLINE, async {
        let (server_io, client_io) = tokio::io::duplex(4_096);
        let (server, client) = tokio::join!(
            ServerConnection::accept(server_io, "same-container", None),
            ClientConnection::open(client_io, "same-container", None),
        );
        let mut server = server.expect("server connection");
        let mut client = client.expect("client connection");
        let server_identity = server.connection_identity().clone();
        let client_identity = client.connection_identity().clone();
        assert!(!server_identity.same_connection(&client_identity));
        let opening = async {
            let incoming = server
                .next_incoming_session()
                .await
                .expect("server incoming session");
            server
                .accept_session(incoming)
                .await
                .expect("server session")
        };
        let (mut server_session, client_session) = tokio::join!(opening, client.begin());
        let mut client_session = client_session.expect("client session");
        let accepting = async {
            let attach = server_session
                .next_incoming_attach()
                .await
                .expect("server incoming sender");
            let LinkEndpoint::Receiver(receiver) = server_session
                .accept_attach(attach, 0)
                .await
                .expect("server receiver")
            else {
                panic!("receiving endpoint")
            };
            receiver
        };
        let (mut server_receiver, client_sender) = tokio::join!(
            accepting,
            ClientSender::attach(&mut client_session, "same-link", "same-queue")
        );
        let mut client_sender = client_sender.expect("client sender");
        let receiving = async {
            let receipt = server_receiver
                .recv_retained()
                .await
                .expect("server retained receipt");
            server_receiver
                .accept_retained(&receipt)
                .await
                .expect("server ordinary settlement");
            receipt
        };
        let (server_receipt, outcome) = tokio::join!(
            receiving,
            client_sender.send(Message::data(b"same-message".to_vec()))
        );
        assert!(matches!(
            outcome.expect("client send"),
            Outcome::Accepted(_)
        ));
        assert!(server_receipt.belongs_to_connection(&server_identity));
        assert!(!server_receipt.belongs_to_connection(&client_identity));
        let accepting = async {
            let attach = server_session
                .next_incoming_attach()
                .await
                .expect("server incoming receiver");
            let LinkEndpoint::Sender(sender) = server_session
                .accept_attach(attach, 0)
                .await
                .expect("server sender")
            else {
                panic!("sending endpoint")
            };
            sender
        };
        let (mut server_sender, client_receiver) = tokio::join!(
            accepting,
            ClientReceiver::attach(&mut client_session, "return-link", "same-queue")
        );
        let mut client_receiver = client_receiver.expect("client receiver");
        let receiving = async {
            let receipt = client_receiver
                .recv_retained()
                .await
                .expect("client retained receipt");
            client_receiver
                .accept_retained(&receipt)
                .await
                .expect("client ordinary settlement");
            receipt
        };
        let (client_receipt, outcome) = tokio::join!(
            receiving,
            server_sender.send(Message::data(b"same-message".to_vec()), vec![42].into())
        );
        assert!(matches!(
            outcome.expect("server send"),
            Outcome::Accepted(_)
        ));
        assert!(client_receipt.belongs_to_connection(&client_identity));
        assert!(!client_receipt.belongs_to_connection(&server_identity));
        assert_eq!(server_receipt.message(), client_receipt.message());
        assert!(
            client_receipt
                .connection_identity()
                .expect("client receipt provenance")
                .same_connection(&client_identity)
        );
        assert!(
            server_receipt
                .connection_identity()
                .expect("server receipt provenance")
                .same_connection(&server_identity)
        );
        client_receiver.close().await.expect("ordinary link close");
        assert!(client_receipt.belongs_to_connection(&client_identity));
        client_session.end().await.expect("ordinary session End");
        assert!(server_receipt.belongs_to_connection(&server_identity));
        assert!(client_receipt.belongs_to_connection(&client_identity));
        client.close().await.expect("graceful client Close");
        server.shutdown().await;
        assert!(!server_identity.is_active());
        assert!(!client_identity.is_active());
        assert!(!server_receipt.belongs_to_connection(&server_identity));
        assert!(!client_receipt.belongs_to_connection(&client_identity));
        assert!(
            server_receipt
                .connection_identity()
                .expect("retired server proof")
                .same_connection(&server_identity)
        );
        assert!(
            client_receipt
                .connection_identity()
                .expect("retired client proof")
                .same_connection(&client_identity)
        );
    })
    .await
    .expect("bounded two-way retained provenance exchange");
}
