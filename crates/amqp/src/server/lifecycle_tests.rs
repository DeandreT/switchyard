use tokio::{
    io::{AsyncReadExt, DuplexStream},
    time::timeout,
};

use super::*;

const SHORT_CLOSE: Duration = Duration::from_millis(40);
const TEST_DEADLINE: Duration = Duration::from_secs(2);

async fn next_frame(peer: &mut DuplexStream) -> Frame {
    timeout(TEST_DEADLINE, read_frame(peer))
        .await
        .expect("the engine responds")
        .expect("valid AMQP frame")
}

async fn assert_eof(peer: &mut DuplexStream) -> Vec<u8> {
    let mut remaining = Vec::new();
    timeout(TEST_DEADLINE, peer.read_to_end(&mut remaining))
        .await
        .expect("both engine halves release the socket")
        .expect("reading until EOF succeeds");
    remaining
}

async fn server_pair() -> (ServerConnection, DuplexStream) {
    let (wire, mut peer) = tokio::io::duplex(4 * 1024);
    let opening = async {
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("write protocol header");
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("server protocol header");
        write_amqp(
            &mut peer,
            0,
            Performative::Open(Open::new("raw-peer")),
            Vec::new(),
        )
        .await
        .expect("write Open");
        assert!(matches!(
            next_frame(&mut peer).await,
            Frame::Amqp {
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        peer
    };
    let (connection, peer) = tokio::join!(ServerConnection::accept(wire, "server", None), opening);
    (connection.expect("connection opens"), peer)
}

async fn begin_session(
    connection: &mut ServerConnection,
    peer: &mut DuplexStream,
) -> ServerSession {
    write_amqp(peer, 0, Performative::Begin(Begin::default()), Vec::new())
        .await
        .expect("begin session");
    let incoming = connection
        .next_incoming_session()
        .await
        .expect("incoming session");
    let (session, frame) = tokio::join!(connection.accept_session(incoming), next_frame(peer));
    assert!(matches!(
        frame,
        Frame::Amqp {
            performative: Some(Performative::Begin(_)),
            ..
        }
    ));
    session.expect("session accepted")
}

#[tokio::test]
async fn protocol_failure_releases_a_silent_peer_without_another_read() {
    let (connection, mut peer) = server_pair().await;
    write_amqp(
        &mut peer,
        0,
        Performative::Open(Open::new("duplicate")),
        Vec::new(),
    )
    .await
    .expect("send invalid state");
    assert!(assert_eof(&mut peer).await.is_empty());
    connection.shutdown().await;
}

#[tokio::test]
async fn dropping_the_connection_cancels_despite_live_session_handles() {
    let (mut connection, mut peer) = server_pair().await;
    let mut session = begin_session(&mut connection, &mut peer).await;
    drop(connection);
    assert!(assert_eof(&mut peer).await.is_empty());
    assert!(session.next_incoming_attach().await.is_none());
}

#[tokio::test]
async fn close_timeout_cancels_and_joins_a_silent_peer() {
    let (connection, mut peer) = server_pair().await;
    let connection = connection.with_close_timeout(SHORT_CLOSE);
    let closing = connection.close();
    let observing = async {
        assert!(matches!(
            next_frame(&mut peer).await,
            Frame::Amqp {
                performative: Some(Performative::Close(_)),
                ..
            }
        ));
        assert_eof(&mut peer).await
    };
    let (result, remaining) = tokio::join!(closing, observing);
    assert!(matches!(result, Err(EngineError::Timeout(_))));
    assert!(remaining.is_empty());
    connection.shutdown().await;
}

#[tokio::test]
async fn a_successful_close_waits_for_cleanup_without_reporting_timeout() {
    let (connection, mut peer) = server_pair().await;
    let answering = async {
        assert!(matches!(
            next_frame(&mut peer).await,
            Frame::Amqp {
                performative: Some(Performative::Close(_)),
                ..
            }
        ));
        write_amqp(
            &mut peer,
            0,
            Performative::Close(Close::default()),
            Vec::new(),
        )
        .await
        .expect("acknowledge Close");
        assert!(assert_eof(&mut peer).await.is_empty());
    };
    let (result, ()) = tokio::join!(connection.close(), answering);
    result.expect("acknowledged Close succeeds");
}

#[tokio::test]
async fn close_deadline_also_interrupts_a_blocked_socket_write() {
    let (connection, mut peer) = server_pair().await;
    let connection = connection.with_close_timeout(SHORT_CLOSE);
    let result = connection
        .close_with_error(Error::new(
            crate::AmqpError::InternalError,
            "x".repeat(64 * 1024),
            None,
        ))
        .await;
    assert!(matches!(result, Err(EngineError::Timeout(_))));
    let remaining = assert_eof(&mut peer).await;
    assert!(!remaining.is_empty());
    assert!(remaining.len() <= 4 * 1024);
}

#[tokio::test]
async fn close_deadline_interrupts_a_full_session_dispatch_queue() {
    let (connection, mut peer) = server_pair().await;
    let connection = connection.with_close_timeout(SHORT_CLOSE);
    for channel in 0..34 {
        write_amqp(
            &mut peer,
            channel,
            Performative::Begin(Begin::default()),
            Vec::new(),
        )
        .await
        .expect("send session burst");
    }
    timeout(TEST_DEADLINE, async {
        while connection.incoming_sessions.len() != 32 {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
    })
    .await
    .expect("dispatch queue fills");
    assert!(matches!(
        connection.close().await,
        Err(EngineError::Timeout(_))
    ));
    assert_eof(&mut peer).await;
}

#[tokio::test]
async fn canceling_a_close_future_cancels_the_retained_connection() {
    let (connection, mut peer) = server_pair().await;
    let connection = connection.with_close_timeout(Duration::from_secs(30));
    assert!(timeout(SHORT_CLOSE, connection.close()).await.is_err());
    assert_eof(&mut peer).await;
    connection.shutdown().await;
}

#[tokio::test]
async fn concurrent_close_requests_cannot_wait_outside_their_deadlines() {
    let (connection, mut peer) = server_pair().await;
    let connection = connection.with_close_timeout(SHORT_CLOSE);
    let (first, second) = timeout(TEST_DEADLINE, async {
        tokio::join!(connection.close(), connection.close())
    })
    .await
    .expect("concurrent Close calls finish");
    assert!(first.is_err());
    assert!(second.is_err());
    assert_eof(&mut peer).await;
}

#[tokio::test]
async fn concurrent_close_callers_share_one_frame_and_successful_acknowledgment() {
    let (connection, mut peer) = server_pair().await;
    let answering = async {
        assert!(matches!(
            next_frame(&mut peer).await,
            Frame::Amqp {
                performative: Some(Performative::Close(_)),
                ..
            }
        ));
        assert!(
            timeout(Duration::from_millis(10), read_frame(&mut peer))
                .await
                .is_err()
        );
        write_amqp(
            &mut peer,
            0,
            Performative::Close(Close::default()),
            Vec::new(),
        )
        .await
        .expect("acknowledge the single Close");
        assert!(assert_eof(&mut peer).await.is_empty());
    };
    let (first, second, ()) = tokio::join!(connection.close(), connection.close(), answering);
    first.expect("first Close caller succeeds");
    second.expect("second Close caller succeeds");
}

#[tokio::test]
async fn zero_close_timeout_cancels_without_writing_another_frame() {
    let (connection, mut peer) = server_pair().await;
    let connection = connection.with_close_timeout(Duration::ZERO);
    assert!(matches!(
        connection.close().await,
        Err(EngineError::Timeout(_))
    ));
    assert!(assert_eof(&mut peer).await.is_empty());
}

fn receiving_attach() -> Attach {
    Attach {
        name: String::from("lifecycle-receiver"),
        handle: 0,
        role: Role::Receiver,
        snd_settle_mode: SenderSettleMode::Settled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(crate::Source::new("queue")),
        target: None,
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: None,
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

async fn grant_two(peer: &mut DuplexStream) {
    write_amqp(
        peer,
        0,
        Performative::Flow(Flow {
            incoming_window: SESSION_WINDOW,
            outgoing_window: SESSION_WINDOW,
            handle: Some(0),
            delivery_count: Some(0),
            link_credit: Some(2),
            ..Flow::default()
        }),
        Vec::new(),
    )
    .await
    .expect("grant two messages");
}

async fn write_late_frames(peer: &mut DuplexStream) {
    for (channel, performative, payload) in [
        (1, Performative::Begin(Begin::default()), Vec::new()),
        (
            0,
            Performative::Attach(Box::new(receiving_attach())),
            Vec::new(),
        ),
        (
            0,
            Performative::Transfer(Transfer {
                handle: 777,
                delivery_id: Some(99),
                delivery_tag: Some(vec![99].into()),
                message_format: Some(0),
                settled: Some(true),
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            }),
            b"not an encoded message".to_vec(),
        ),
    ] {
        write_amqp(peer, channel, performative, payload)
            .await
            .expect("send crossing traffic after Close");
    }
}

#[tokio::test]
async fn server_close_refuses_new_sends_and_discards_crossing_link_frames() {
    let (mut connection, mut peer) = server_pair().await;
    let session = begin_session(&mut connection, &mut peer).await;
    write_amqp(
        &mut peer,
        0,
        Performative::Attach(Box::new(receiving_attach())),
        Vec::new(),
    )
    .await
    .expect("attach receiver");
    let mut session = session;
    let incoming = session
        .next_incoming_attach()
        .await
        .expect("incoming attach");
    let (endpoint, _) = tokio::join!(
        session.accept_attach(incoming, 1_024),
        next_frame(&mut peer)
    );
    let LinkEndpoint::Sender(mut sender) = endpoint.expect("attach accepted") else {
        panic!("server sending endpoint");
    };
    grant_two(&mut peer).await;
    let first = sender.send(Message::data(vec![1]), vec![1].into());
    let (outcome, frame) = tokio::join!(first, next_frame(&mut peer));
    assert!(matches!(
        outcome.expect("initial send"),
        Outcome::Accepted(_)
    ));
    assert!(matches!(
        frame,
        Frame::Amqp {
            performative: Some(Performative::Transfer(_)),
            ..
        }
    ));

    let closing = connection.close();
    tokio::pin!(closing);
    let frame = tokio::select! {
        result = &mut closing => panic!("Close finished before acknowledgment: {result:?}"),
        frame = next_frame(&mut peer) => frame,
    };
    assert!(matches!(
        frame,
        Frame::Amqp {
            performative: Some(Performative::Close(_)),
            ..
        }
    ));
    write_late_frames(&mut peer).await;
    assert!(matches!(
        sender.send(Message::data(vec![2]), vec![2].into()).await,
        Err(EngineError::RemoteClosed)
    ));
    assert!(
        timeout(Duration::from_millis(10), read_frame(&mut peer))
            .await
            .is_err()
    );
    assert_eq!(connection.incoming_sessions.len(), 0);
    assert_eq!(session.incoming_attaches.len(), 0);
    write_amqp(
        &mut peer,
        0,
        Performative::Close(Close::default()),
        Vec::new(),
    )
    .await
    .expect("acknowledge Close");
    closing
        .await
        .expect("crossing traffic does not prevent Close");
    assert!(assert_eof(&mut peer).await.is_empty());
}

#[cfg(feature = "test-client")]
async fn client_pair() -> (ClientConnection, DuplexStream) {
    let (wire, mut peer) = tokio::io::duplex(4 * 1024);
    let opening = async {
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("client header");
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("server header");
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
            Performative::Open(Open::new("raw-server")),
            Vec::new(),
        )
        .await
        .expect("server Open");
        peer
    };
    let (connection, peer) = tokio::join!(ClientConnection::open(wire, "client", None), opening);
    (connection.expect("client opens"), peer)
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn dropping_client_connection_cancels_with_live_session_handles() {
    let (mut connection, mut peer) = client_pair().await;
    let answering = async {
        assert!(matches!(
            next_frame(&mut peer).await,
            Frame::Amqp {
                performative: Some(Performative::Begin(_)),
                ..
            }
        ));
        write_amqp(
            &mut peer,
            0,
            Performative::Begin(Begin {
                remote_channel: Some(0),
                ..Begin::default()
            }),
            Vec::new(),
        )
        .await
        .expect("answer Begin");
    };
    let (session, ()) = tokio::join!(connection.begin(), answering);
    let _session = session.expect("client session opens");
    drop(connection);
    assert!(assert_eof(&mut peer).await.is_empty());
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn client_protocol_failure_cleans_up_even_when_the_peer_goes_silent() {
    let (connection, mut peer) = client_pair().await;
    write_amqp(
        &mut peer,
        0,
        Performative::Open(Open::new("duplicate")),
        Vec::new(),
    )
    .await
    .expect("invalid duplicate Open");
    assert!(assert_eof(&mut peer).await.is_empty());
    connection.shutdown().await;
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn client_close_timeout_and_external_cancellation_release_the_socket() {
    for external_cancellation in [false, true] {
        let (connection, mut peer) = client_pair().await;
        let connection = connection.with_close_timeout(if external_cancellation {
            Duration::from_secs(30)
        } else {
            SHORT_CLOSE
        });
        if external_cancellation {
            assert!(timeout(SHORT_CLOSE, connection.close()).await.is_err());
        } else {
            assert!(matches!(
                connection.close().await,
                Err(EngineError::Timeout(_))
            ));
        }
        assert_eof(&mut peer).await;
        connection.shutdown().await;
    }
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn concurrent_client_close_requests_are_bounded() {
    let (connection, mut peer) = client_pair().await;
    let connection = connection.with_close_timeout(SHORT_CLOSE);
    let (first, second) = timeout(TEST_DEADLINE, async {
        tokio::join!(connection.close(), connection.close())
    })
    .await
    .expect("both client Close calls terminate");
    assert!(first.is_err());
    assert!(second.is_err());
    assert_eof(&mut peer).await;
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn concurrent_client_close_callers_share_one_successful_close() {
    let (connection, mut peer) = client_pair().await;
    let answering = async {
        assert!(matches!(
            next_frame(&mut peer).await,
            Frame::Amqp {
                performative: Some(Performative::Close(_)),
                ..
            }
        ));
        assert!(
            timeout(Duration::from_millis(10), read_frame(&mut peer))
                .await
                .is_err()
        );
        write_amqp(
            &mut peer,
            0,
            Performative::Close(Close::default()),
            Vec::new(),
        )
        .await
        .expect("acknowledge client Close");
        assert!(assert_eof(&mut peer).await.is_empty());
    };
    let (first, second, ()) = tokio::join!(connection.close(), connection.close(), answering);
    first.expect("first client Close succeeds");
    second.expect("second client Close succeeds");
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn zero_client_close_timeout_is_immediate_cancellation() {
    let (connection, mut peer) = client_pair().await;
    let connection = connection.with_close_timeout(Duration::ZERO);
    assert!(matches!(
        connection.close().await,
        Err(EngineError::Timeout(_))
    ));
    assert!(assert_eof(&mut peer).await.is_empty());
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn client_close_refuses_sends_and_discards_crossing_transfers() {
    let (mut connection, mut peer) = client_pair().await;
    let answering = async {
        next_frame(&mut peer).await;
        write_amqp(
            &mut peer,
            0,
            Performative::Begin(Begin {
                remote_channel: Some(0),
                ..Begin::default()
            }),
            Vec::new(),
        )
        .await
        .expect("answer client Begin");
    };
    let (session, ()) = tokio::join!(connection.begin(), answering);
    let mut session = session.expect("client session");
    let answering = async {
        let Frame::Amqp {
            performative: Some(Performative::Attach(attach)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("client sender Attach");
        };
        let response = attach.response(attach.source.clone(), attach.target.clone());
        write_amqp(
            &mut peer,
            0,
            Performative::Attach(Box::new(response)),
            Vec::new(),
        )
        .await
        .expect("answer Attach");
        grant_two(&mut peer).await;
    };
    let (sender, ()) = tokio::join!(session.attach_sender("client-sender", "queue"), answering);
    let mut sender = sender.expect("client sender");
    let acknowledging = async {
        let Frame::Amqp {
            performative: Some(Performative::Transfer(transfer)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("initial client Transfer");
        };
        write_amqp(
            &mut peer,
            0,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: transfer.delivery_id.expect("delivery id"),
                last: None,
                settled: true,
                state: Some(DeliveryState::Accepted(Accepted)),
                batchable: false,
            }),
            Vec::new(),
        )
        .await
        .expect("acknowledge initial delivery");
    };
    let (sent, ()) = tokio::join!(sender.send(Message::data(vec![1])), acknowledging);
    sent.expect("initial send succeeds");

    let closing = connection.close();
    tokio::pin!(closing);
    let frame = tokio::select! {
        result = &mut closing => panic!("client Close ended early: {result:?}"),
        frame = next_frame(&mut peer) => frame,
    };
    assert!(matches!(
        frame,
        Frame::Amqp {
            performative: Some(Performative::Close(_)),
            ..
        }
    ));
    write_late_frames(&mut peer).await;
    assert!(matches!(
        sender.send(Message::data(vec![2])).await,
        Err(EngineError::RemoteClosed)
    ));
    assert!(
        timeout(Duration::from_millis(10), read_frame(&mut peer))
            .await
            .is_err()
    );
    write_amqp(
        &mut peer,
        0,
        Performative::Close(Close::default()),
        Vec::new(),
    )
    .await
    .expect("answer client Close");
    closing
        .await
        .expect("client Close succeeds after crossing input");
    assert!(assert_eof(&mut peer).await.is_empty());
}
