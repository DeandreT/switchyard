use tokio::{io::DuplexStream, time::timeout};

use super::*;

const DEADLINE: Duration = Duration::from_secs(2);
type BeginReply = oneshot::Receiver<Result<(u16, SessionIdentity), EngineError>>;
type EndReply = oneshot::Receiver<Result<(), EngineError>>;

async fn frame(peer: &mut DuplexStream) -> (u16, Performative) {
    let Frame::Amqp {
        channel,
        performative: Some(performative),
        payload,
    } = timeout(DEADLINE, read_frame(peer))
        .await
        .expect("prompt peer frame")
        .expect("complete peer frame")
    else {
        panic!("AMQP performative");
    };
    assert!(payload.is_empty());
    (channel, performative)
}

async fn silent(peer: &mut DuplexStream) {
    assert!(
        timeout(Duration::from_millis(20), read_frame(peer))
            .await
            .is_err()
    );
}

async fn pair(maximum: u16) -> (ClientConnection, DuplexStream) {
    let (wire, mut peer) = tokio::io::duplex(64 * 1024);
    let responding = async {
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("client header");
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        assert!(matches!(frame(&mut peer).await, (0, Performative::Open(_))));
        write_amqp(
            &mut peer,
            0,
            Performative::Open(Open {
                channel_max: maximum,
                ..Open::new("channel-peer")
            }),
            Vec::new(),
        )
        .await
        .expect("peer Open");
        peer
    };
    let (connection, peer) = timeout(DEADLINE, async {
        tokio::join!(
            ClientConnection::open(wire, "channel-client", None),
            responding
        )
    })
    .await
    .expect("prompt client handshake");
    (connection.expect("client connection"), peer)
}

async fn begin(connection: &ClientConnection, peer: &mut DuplexStream) -> (u16, BeginReply) {
    let reply = begin_command(connection).await;
    let (channel, performative) = frame(peer).await;
    let Performative::Begin(begin) = performative else {
        panic!("client Begin");
    };
    assert_eq!(begin.remote_channel, None);
    (channel, reply)
}

async fn begin_command(connection: &ClientConnection) -> BeginReply {
    let (reply, result) = oneshot::channel();
    connection
        .commands
        .send(ClientCommand::Begin { reply })
        .await
        .expect("Begin command");
    result
}

async fn answer(peer: &mut DuplexStream, peer_channel: u16, local_channel: u16) {
    write_amqp(
        peer,
        peer_channel,
        Performative::Begin(Begin {
            remote_channel: Some(local_channel),
            ..Begin::default()
        }),
        Vec::new(),
    )
    .await
    .expect("peer Begin response");
}

async fn accepted(reply: BeginReply, local: u16) -> SessionIdentity {
    let (actual, identity) = timeout(DEADLINE, reply)
        .await
        .expect("prompt Begin result")
        .expect("Begin response")
        .expect("accepted Begin");
    assert_eq!(actual, local);
    identity
}

async fn end(
    connection: &ClientConnection,
    peer: &mut DuplexStream,
    channel: u16,
    identity: &SessionIdentity,
) -> EndReply {
    let (reply, result) = oneshot::channel();
    connection
        .commands
        .send(ClientCommand::End {
            channel,
            identity: identity.clone(),
            reply,
        })
        .await
        .expect("End command");
    assert!(matches!(frame(peer).await, (actual, Performative::End(_)) if actual == channel));
    result
}

async fn echo(peer: &mut DuplexStream, channel: u16) {
    write_amqp(
        peer,
        channel,
        Performative::Flow(Flow {
            incoming_window: SESSION_WINDOW,
            outgoing_window: SESSION_WINDOW,
            echo: true,
            ..Flow::default()
        }),
        Vec::new(),
    )
    .await
    .expect("peer echo request");
}

async fn close(peer: &mut DuplexStream, condition: &str) {
    let (channel, performative) = frame(peer).await;
    assert_eq!(channel, 0);
    let Performative::Close(close) = performative else {
        panic!("connection Close");
    };
    assert_eq!(
        close.error.expect("Close error").condition.as_symbol(),
        Symbol::from(condition)
    );
}

async fn cleanup(connection: ClientConnection) {
    timeout(DEADLINE, connection.shutdown())
        .await
        .expect("owned client cleanup");
}

#[tokio::test]
async fn crossed_pending_responses_and_end_acknowledgements_use_remote_associations() {
    let (connection, mut peer) = pair(1).await;
    let (first_channel, first_reply) = begin(&connection, &mut peer).await;
    let (second_channel, second_reply) = begin(&connection, &mut peer).await;
    assert_eq!((first_channel, second_channel), (0, 1));
    answer(&mut peer, 0, second_channel).await;
    answer(&mut peer, 1, first_channel).await;
    let second = accepted(second_reply, second_channel).await;
    let first = accepted(first_reply, first_channel).await;
    assert!(!first.same_session(&second));
    echo(&mut peer, 0).await;
    assert!(matches!(frame(&mut peer).await, (1, Performative::Flow(_))));
    echo(&mut peer, 1).await;
    assert!(matches!(frame(&mut peer).await, (0, Performative::Flow(_))));

    let mut pending_end = end(&connection, &mut peer, first_channel, &first).await;
    write_amqp(&mut peer, 0, Performative::End(End::default()), Vec::new())
        .await
        .expect("second peer End");
    assert!(matches!(frame(&mut peer).await, (1, Performative::End(_))));
    assert!(second.is_retired());
    assert!(matches!(
        pending_end.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    echo(&mut peer, 1).await;
    write_amqp(
        &mut peer,
        1,
        Performative::Begin(Begin::default()),
        Vec::new(),
    )
    .await
    .expect("discarded ending Begin");
    silent(&mut peer).await;
    write_amqp(&mut peer, 1, Performative::End(End::default()), Vec::new())
        .await
        .expect("first End acknowledgement");
    timeout(DEADLINE, pending_end)
        .await
        .expect("prompt mapped End result")
        .expect("End reply")
        .expect("acknowledged End");
    silent(&mut peer).await;

    let (fresh_channel, fresh_reply) = begin(&connection, &mut peer).await;
    assert_eq!(fresh_channel, first_channel);
    answer(&mut peer, 77, fresh_channel).await;
    let fresh = accepted(fresh_reply, fresh_channel).await;
    assert!(!fresh.same_session(&first));
    let (reply, result) = oneshot::channel();
    connection
        .commands
        .send(ClientCommand::End {
            channel: first_channel,
            identity: first,
            reply,
        })
        .await
        .expect("stale End command");
    assert!(matches!(
        timeout(DEADLINE, result)
            .await
            .expect("prompt stale reply")
            .expect("stale reply"),
        Err(EngineError::RemoteDetached)
    ));
    silent(&mut peer).await;
    echo(&mut peer, 77).await;
    assert!(matches!(frame(&mut peer).await, (0, Performative::Flow(_))));
    assert!(!fresh.is_retired());
    cleanup(connection).await;
}

#[tokio::test]
async fn missing_or_wrong_remote_channel_never_resolves_a_pending_begin() {
    for remote_channel in [None, Some(1), Some(u16::MAX)] {
        let (connection, mut peer) = pair(0).await;
        let (channel, reply) = begin(&connection, &mut peer).await;
        assert_eq!(channel, 0);
        write_amqp(
            &mut peer,
            17,
            Performative::Begin(Begin {
                remote_channel,
                ..Begin::default()
            }),
            Vec::new(),
        )
        .await
        .expect("invalid Begin response");
        close(
            &mut peer,
            if remote_channel.is_none() {
                "amqp:not-implemented"
            } else {
                "amqp:connection:framing-error"
            },
        )
        .await;
        assert!(matches!(
            timeout(DEADLINE, reply)
                .await
                .expect("prompt rejected Begin")
                .expect("Begin reply"),
            Err(EngineError::RemoteClosed)
        ));
        cleanup(connection).await;
    }
}

#[tokio::test]
async fn duplicate_peer_channel_cannot_claim_another_pending_begin() {
    let (connection, mut peer) = pair(1).await;
    let (first_channel, first_reply) = begin(&connection, &mut peer).await;
    let (second_channel, second_reply) = begin(&connection, &mut peer).await;
    answer(&mut peer, 17, first_channel).await;
    let first = accepted(first_reply, first_channel).await;
    answer(&mut peer, 17, second_channel).await;
    close(&mut peer, "amqp:connection:framing-error").await;
    assert!(matches!(
        timeout(DEADLINE, second_reply)
            .await
            .expect("prompt rejected association")
            .expect("Begin reply"),
        Err(EngineError::RemoteClosed)
    ));
    assert!(first.is_retired());
    cleanup(connection).await;
}

#[tokio::test]
async fn duplicate_response_to_an_already_bound_local_channel_is_refused() {
    for peer_channel in [17, 18] {
        let (connection, mut peer) = pair(0).await;
        let (channel, reply) = begin(&connection, &mut peer).await;
        answer(&mut peer, 17, channel).await;
        let identity = accepted(reply, channel).await;
        answer(&mut peer, peer_channel, channel).await;
        close(&mut peer, "amqp:connection:framing-error").await;
        assert!(identity.is_retired());
        cleanup(connection).await;
    }
}

#[tokio::test]
async fn unbound_end_on_a_local_channel_is_a_connection_error_not_an_acknowledgement() {
    let (connection, mut peer) = pair(0).await;
    let (channel, reply) = begin(&connection, &mut peer).await;
    answer(&mut peer, 17, channel).await;
    accepted(reply, channel).await;
    write_amqp(
        &mut peer,
        channel,
        Performative::End(End::default()),
        Vec::new(),
    )
    .await
    .expect("unbound End");
    close(&mut peer, "amqp:connection:framing-error").await;
    cleanup(connection).await;
}

#[tokio::test]
async fn unassociated_end_cannot_resolve_a_pending_begin_by_numeric_channel() {
    let (connection, mut peer) = pair(0).await;
    let (channel, reply) = begin(&connection, &mut peer).await;
    write_amqp(
        &mut peer,
        channel,
        Performative::End(End::default()),
        Vec::new(),
    )
    .await
    .expect("End before association");
    close(&mut peer, "amqp:connection:framing-error").await;
    assert!(matches!(
        timeout(DEADLINE, reply)
            .await
            .expect("prompt rejected Begin")
            .expect("Begin reply"),
        Err(EngineError::RemoteClosed)
    ));
    cleanup(connection).await;
}

#[tokio::test]
async fn pending_and_ending_output_channels_remain_reserved_until_mapped_peer_end() {
    let (connection, mut peer) = pair(0).await;
    let (channel, reply) = begin(&connection, &mut peer).await;
    let excess = begin_command(&connection).await;
    assert!(matches!(
        timeout(DEADLINE, excess)
            .await
            .expect("prompt pending-range refusal")
            .expect("Begin reply"),
        Err(EngineError::InvalidState(_))
    ));
    silent(&mut peer).await;
    answer(&mut peer, 17, channel).await;
    let old = accepted(reply, channel).await;
    let ended = end(&connection, &mut peer, channel, &old).await;
    let excess = begin_command(&connection).await;
    assert!(matches!(
        timeout(DEADLINE, excess)
            .await
            .expect("prompt ending-range refusal")
            .expect("Begin reply"),
        Err(EngineError::InvalidState(_))
    ));
    echo(&mut peer, 17).await;
    write_amqp(
        &mut peer,
        17,
        Performative::Begin(Begin::default()),
        Vec::new(),
    )
    .await
    .expect("discarded duplicate Begin");
    silent(&mut peer).await;
    write_amqp(&mut peer, 17, Performative::End(End::default()), Vec::new())
        .await
        .expect("mapped End acknowledgement");
    timeout(DEADLINE, ended)
        .await
        .expect("prompt acknowledged End")
        .expect("End reply")
        .expect("End success");
    let (fresh_channel, fresh_reply) = begin(&connection, &mut peer).await;
    assert_eq!(fresh_channel, channel);
    answer(&mut peer, 19, fresh_channel).await;
    let fresh = accepted(fresh_reply, fresh_channel).await;
    assert!(!fresh.same_session(&old));
    assert!(!fresh.is_retired());
    cleanup(connection).await;
}

#[tokio::test]
async fn duplicate_open_on_an_ending_peer_binding_fails_end_and_terminates_the_driver() {
    let (connection, mut peer) = pair(0).await;
    let (channel, reply) = begin(&connection, &mut peer).await;
    answer(&mut peer, 17, channel).await;
    let identity = accepted(reply, channel).await;
    let ended = end(&connection, &mut peer, channel, &identity).await;
    write_amqp(
        &mut peer,
        17,
        Performative::Open(Open::new("duplicate-peer")),
        Vec::new(),
    )
    .await
    .expect("duplicate Open on the mapped ending channel");
    assert!(matches!(
        timeout(DEADLINE, ended)
            .await
            .expect("prompt failed End")
            .expect("End reply"),
        Err(EngineError::RemoteClosed)
    ));
    timeout(DEADLINE, connection.lifecycle.wait_terminated())
        .await
        .expect("duplicate Open terminates the driver");
    assert!(*connection.closed.borrow());
    assert!(matches!(
        timeout(DEADLINE, read_frame(&mut peer)).await.expect("reader joined promptly"),
        Err(ref error) if error.kind() == io::ErrorKind::UnexpectedEof
    ));
    cleanup(connection).await;
}
