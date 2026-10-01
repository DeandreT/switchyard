//! Independently owned link handles within independently owned session channels.

use std::{cell::Cell, error::Error};

use amqp::{End, EngineError, Frame, Message, Performative, Role, encode_message};
use tokio::time::timeout;

#[path = "support/asymmetric_handle_fixture.rs"]
mod fixture;

#[path = "support/error_closing_cases.rs"]
mod error_closing_cases;

#[path = "support/error_delivery_cases.rs"]
mod error_delivery_cases;

#[path = "support/error_name_cases.rs"]
mod error_name_cases;

#[path = "support/error_handle_cases.rs"]
mod error_handle_cases;

use fixture::{
    CASE_TIMEOUT, Connection, IO_TIMEOUT, Link, Node, Session, TestResult, attach, channels,
    detach, transfer,
};

fn only_session_flow(frames: &[Frame], outgoing: u16) {
    assert!(
        frames.iter().all(|frame| matches!(frame, Frame::Amqp {
        channel, performative: Some(Performative::Flow(flow)), ..
    } if *channel == outgoing && flow.handle.is_none())),
        "unpublished or refused link must not emit traffic: {frames:?}"
    );
}

async fn both_roles(server: bool) -> TestResult {
    let mut node = Node::new(server).await?;
    let channels = channels(server, 0);
    let mut session = node.session(channels, 1).await?;
    let receiving = Link {
        channels,
        peer: 55,
        local: 0,
    };
    let sending = Link {
        channels,
        peer: 56,
        local: 1,
    };
    let mut receiver = node.receiver(&mut session, receiving).await?;
    let mut sender = node.sender(&mut session, sending).await?;
    node.incoming_message(&mut receiver, receiving, 0, 1)
        .await?;
    node.outgoing_message(&mut sender, sending, 2).await?;

    let mut echo = node.peer.flow(channels);
    echo.handle = Some(receiving.peer);
    echo.delivery_count = Some(1);
    echo.echo = true;
    node.peer
        .send(channels.incoming, Performative::Flow(echo), Vec::new())
        .await?;
    let frames = node.peer.barrier(channels).await?;
    assert!(frames.iter().any(|frame| matches!(frame, Frame::Amqp {
        channel, performative: Some(Performative::Flow(flow)), ..
    } if *channel == channels.outgoing && flow.handle == Some(receiving.local))));
    assert!(frames.iter().all(|frame| matches!(frame, Frame::Amqp {
        channel, performative: Some(Performative::Flow(flow)), ..
    } if *channel == channels.outgoing && (flow.handle.is_none() || flow.handle == Some(receiving.local)))));

    node.close_receiver(&receiver, receiving).await?;
    node.peer
        .send(
            channels.incoming,
            Performative::Detach(detach(sending.peer)),
            Vec::new(),
        )
        .await?;
    node.peer.detach(sending).await?;
    node.peer.barrier(channels).await?;
    let replacement = Link {
        channels,
        peer: 57,
        local: 0,
    };
    let mut receiver = node.receiver(&mut session, replacement).await?;
    node.incoming_message(&mut receiver, replacement, 1, 3)
        .await?;
    node.shutdown().await;
    Ok(())
}

async fn crossed_handles_and_closing_ownership(server: bool) -> TestResult {
    let mut node = Node::new(server).await?;
    let channels = channels(server, 0);
    let mut session = node.session(channels, 1).await?;
    let first = Link {
        channels,
        peer: 55,
        local: 0,
    };
    let second = Link {
        channels,
        peer: 0,
        local: 1,
    };
    let mut receiver_a = node.receiver(&mut session, first).await?;
    let mut receiver_b = node.receiver(&mut session, second).await?;
    node.incoming_message(&mut receiver_b, second, 0, 4).await?;
    node.incoming_message(&mut receiver_a, first, 1, 5).await?;
    node.peer.barrier(channels).await?;

    let first_close_finished = Cell::new(false);
    let closing = async {
        receiver_a.close().await?;
        first_close_finished.set(true);
        Ok::<_, Box<dyn Error>>(())
    };
    let replacement = Link {
        channels,
        peer: 7,
        local: 1,
    };
    let ((), mut receiver_c) = tokio::try_join!(closing, async {
        node.peer.detach(first).await?;
        node.peer
            .send(
                channels.incoming,
                Performative::Detach(detach(second.peer)),
                Vec::new(),
            )
            .await?;
        node.peer.detach(second).await?;
        node.peer.barrier(channels).await?;
        if !server {
            assert!(
                !first_close_finished.get(),
                "peer handle0 must not acknowledge own handle0 bound to peer55"
            );
        }
        assert!(matches!(
            timeout(IO_TIMEOUT, receiver_b.recv()).await?,
            Err(EngineError::RemoteDetached)
        ));
        let mut receiver_c = node.receiver(&mut session, replacement).await?;
        node.incoming_message(&mut receiver_c, replacement, 2, 6)
            .await?;
        node.peer.barrier(channels).await?;
        if !server {
            assert!(
                !first_close_finished.get(),
                "other-link activity must retain the exact pending close"
            );
        }
        node.peer
            .send(
                channels.incoming,
                Performative::Detach(detach(first.peer)),
                Vec::new(),
            )
            .await?;
        Ok::<_, Box<dyn Error>>(receiver_c)
    })?;
    assert!(first_close_finished.get());
    node.peer.barrier(channels).await?;
    let reused = Link {
        channels,
        peer: 55,
        local: 0,
    };
    let mut sender = node.sender(&mut session, reused).await?;
    node.outgoing_message(&mut sender, reused, 7).await?;
    node.incoming_message(&mut receiver_c, replacement, 3, 8)
        .await?;
    node.shutdown().await;
    Ok(())
}

async fn server_pending_echo_waits_for_own_attach() -> TestResult {
    let mut node = Node::new(true).await?;
    let main = channels(true, 0);
    let sibling = channels(true, 1);
    let mut session = node.session(main, 0).await?;
    let mut sibling_session = node.session(sibling, 0).await?;
    let sibling_link = Link {
        channels: sibling,
        peer: 77,
        local: 0,
    };
    let mut sibling_receiver = node.receiver(&mut sibling_session, sibling_link).await?;
    let pending = Link {
        channels: main,
        peer: 55,
        local: 0,
    };
    let incoming = node.pending(&mut session, pending, Role::Sender).await?;
    let mut echo = node.peer.flow(main);
    echo.handle = Some(pending.peer);
    echo.delivery_count = Some(0);
    echo.echo = true;
    node.peer
        .send(main.incoming, Performative::Flow(echo), Vec::new())
        .await?;
    only_session_flow(&node.peer.barrier(sibling).await?, sibling.outgoing);
    let mut receiver = node.approve_receiver(&session, incoming, pending).await?;
    node.incoming_message(&mut receiver, pending, 0, 9).await?;
    node.incoming_message(&mut sibling_receiver, sibling_link, 0, 10)
        .await?;
    node.shutdown().await;
    Ok(())
}

async fn server_pending_detach_publishes_own_handle_before_acknowledging() -> TestResult {
    let mut node = Node::new(true).await?;
    let main = channels(true, 0);
    let sibling = channels(true, 1);
    let mut session = node.session(main, 0).await?;
    let mut sibling_session = node.session(sibling, 0).await?;
    let sibling_link = Link {
        channels: sibling,
        peer: 77,
        local: 0,
    };
    let mut sibling_receiver = node.receiver(&mut sibling_session, sibling_link).await?;
    let pending = Link {
        channels: main,
        peer: 55,
        local: 0,
    };
    let incoming = node.pending(&mut session, pending, Role::Sender).await?;
    assert_eq!(incoming.attach().handle, 55);
    node.peer
        .send(
            main.incoming,
            Performative::Detach(detach(pending.peer)),
            Vec::new(),
        )
        .await?;
    let own = node.peer.own_attach(pending, Role::Receiver).await?;
    assert_eq!(own.name, incoming.attach().name);
    assert!(own.source.is_none());
    assert!(own.target.is_none());
    node.peer.detach(pending).await?;
    only_session_flow(&node.peer.barrier(sibling).await?, sibling.outgoing);
    let Session::Server(server_session) = &session else {
        panic!("server session")
    };
    assert!(matches!(
        timeout(IO_TIMEOUT, server_session.accept_attach(incoming, 4096)).await?,
        Err(EngineError::RemoteDetached)
    ));
    only_session_flow(&node.peer.barrier(sibling).await?, sibling.outgoing);
    let replacement = Link {
        channels: main,
        peer: 0,
        local: 0,
    };
    let mut receiver = node.receiver(&mut session, replacement).await?;
    node.incoming_message(&mut receiver, replacement, 0, 11)
        .await?;
    node.incoming_message(&mut sibling_receiver, sibling_link, 0, 12)
        .await?;
    node.shutdown().await;
    Ok(())
}

async fn client_unpublished_peer_handle_ends_only_its_session() -> TestResult {
    let mut node = Node::new(false).await?;
    let main = channels(false, 0);
    let sibling = channels(false, 1);
    let mut session = node.session(main, 0).await?;
    let mut sibling_session = node.session(sibling, 0).await?;
    let sibling_link = Link {
        channels: sibling,
        peer: 77,
        local: 0,
    };
    let mut sibling_receiver = node.receiver(&mut sibling_session, sibling_link).await?;
    let Session::Client(client_session) = &mut session else {
        panic!("client session")
    };
    let unpublished = Link {
        channels: main,
        peer: 55,
        local: 0,
    };
    let (result, wire) = tokio::join!(
        client_session.attach_receiver("unpublished", "queue"),
        async {
            node.peer.own_attach(unpublished, Role::Receiver).await?;
            let mut flow = node.peer.flow(main);
            flow.handle = Some(unpublished.local);
            flow.delivery_count = Some(0);
            node.peer
                .send(main.incoming, Performative::Flow(flow), Vec::new())
                .await?;
            node.peer
                .end(main, "amqp:session:unattached-handle")
                .await?;
            node.peer
                .send(main.incoming, Performative::End(End::default()), Vec::new())
                .await
        }
    );
    wire?;
    assert!(
        result.is_err(),
        "no peer handle was published by an Attach response"
    );
    node.peer.barrier(sibling).await?;
    node.incoming_message(&mut sibling_receiver, sibling_link, 0, 13)
        .await?;
    node.shutdown().await;
    Ok(())
}

async fn zero_maximum_accepts_high_peer_handles_for_both_roles(server: bool) -> TestResult {
    for receiving in [true, false] {
        let mut node = Node::new(server).await?;
        let channels = channels(server, 0);
        let mut session = node.session(channels, 0).await?;
        let link = Link {
            channels,
            peer: 55,
            local: 0,
        };
        if receiving {
            let mut receiver = node.receiver(&mut session, link).await?;
            node.incoming_message(&mut receiver, link, 0, 14).await?;
            node.close_receiver(&receiver, link).await?;
        } else {
            let mut sender = node.sender(&mut session, link).await?;
            node.outgoing_message(&mut sender, link, 15).await?;
            node.peer
                .send(
                    channels.incoming,
                    Performative::Detach(detach(link.peer)),
                    Vec::new(),
                )
                .await?;
            node.peer.detach(link).await?;
        }
        node.shutdown().await;
    }
    Ok(())
}

async fn server_handle_range_exhaustion_preserves_sibling() -> TestResult {
    let mut node = Node::new(true).await?;
    let main = channels(true, 0);
    let sibling = channels(true, 1);
    let mut session = node.session(main, 0).await?;
    let mut sibling_session = node.session(sibling, 0).await?;
    let existing = Link {
        channels: main,
        peer: 55,
        local: 0,
    };
    let mut receiver = node.receiver(&mut session, existing).await?;
    let sibling_link = Link {
        channels: sibling,
        peer: 77,
        local: 0,
    };
    let mut sibling_receiver = node.receiver(&mut sibling_session, sibling_link).await?;
    let overflow = Link {
        channels: main,
        peer: 56,
        local: 0,
    };
    node.peer
        .send(
            main.incoming,
            Performative::Attach(Box::new(attach(overflow, Role::Sender))),
            Vec::new(),
        )
        .await?;
    node.peer.end(main, "amqp:resource-limit-exceeded").await?;
    let Session::Server(server_session) = &mut session else {
        panic!("server session")
    };
    assert!(
        timeout(IO_TIMEOUT, server_session.next_incoming_attach())
            .await?
            .is_none(),
        "overflow owns no approval receipt"
    );
    assert!(matches!(
        timeout(IO_TIMEOUT, receiver.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));
    node.peer
        .send(main.incoming, Performative::End(End::default()), Vec::new())
        .await?;
    node.peer.barrier(sibling).await?;
    node.incoming_message(&mut sibling_receiver, sibling_link, 0, 16)
        .await?;
    node.shutdown().await;
    Ok(())
}

async fn client_handle_range_refusal_and_retry_preserve_sibling() -> TestResult {
    let mut node = Node::new(false).await?;
    let main = channels(false, 0);
    let sibling = channels(false, 1);
    let mut session = node.session(main, 0).await?;
    let mut sibling_session = node.session(sibling, 0).await?;
    let existing = Link {
        channels: main,
        peer: 55,
        local: 0,
    };
    let mut receiver = node.receiver(&mut session, existing).await?;
    let sibling_link = Link {
        channels: sibling,
        peer: 77,
        local: 0,
    };
    let mut sibling_receiver = node.receiver(&mut sibling_session, sibling_link).await?;
    let Session::Client(client_session) = &mut session else {
        panic!("client session")
    };
    assert!(matches!(
        timeout(
            IO_TIMEOUT,
            client_session.attach_receiver("overflow", "queue")
        )
        .await?,
        Err(EngineError::InvalidState(_))
    ));
    only_session_flow(&node.peer.barrier(sibling).await?, sibling.outgoing);
    node.incoming_message(&mut receiver, existing, 0, 17)
        .await?;
    node.close_receiver(&receiver, existing).await?;
    node.peer.barrier(main).await?;
    let replacement = Link {
        channels: main,
        peer: 56,
        local: 0,
    };
    let mut receiver = node.receiver(&mut session, replacement).await?;
    node.incoming_message(&mut receiver, replacement, 1, 18)
        .await?;
    node.incoming_message(&mut sibling_receiver, sibling_link, 0, 19)
        .await?;
    node.shutdown().await;
    Ok(())
}

async fn numeric_local_handle_is_not_a_peer_alias(server: bool) -> TestResult {
    let mut node = Node::new(server).await?;
    let main = channels(server, 0);
    let sibling = channels(server, 1);
    let mut session = node.session(main, 0).await?;
    let mut sibling_session = node.session(sibling, 0).await?;
    let existing = Link {
        channels: main,
        peer: 55,
        local: 0,
    };
    let _receiver = node.receiver(&mut session, existing).await?;
    let sibling_link = Link {
        channels: sibling,
        peer: 77,
        local: 0,
    };
    let mut sibling_receiver = node.receiver(&mut sibling_session, sibling_link).await?;
    node.peer
        .send(
            main.incoming,
            Performative::Transfer(transfer(existing.local, 0)),
            encode_message(&Message::data(vec![20]))?,
        )
        .await?;
    node.peer
        .end(main, "amqp:session:unattached-handle")
        .await?;
    node.peer
        .send(main.incoming, Performative::End(End::default()), Vec::new())
        .await?;
    node.peer.barrier(sibling).await?;
    node.incoming_message(&mut sibling_receiver, sibling_link, 0, 21)
        .await?;
    node.shutdown().await;
    Ok(())
}

#[derive(Clone, Copy)]
enum ServerDuplicateStage {
    Installed,
    Pending,
    Closing,
}

async fn server_approval_queue_is_retired(session: &mut Session) -> TestResult {
    let Session::Server(session) = session else {
        panic!("server approval queue")
    };
    assert!(
        timeout(IO_TIMEOUT, session.next_incoming_attach())
            .await?
            .is_none(),
        "a connection refusal must retire the whole approval queue"
    );
    Ok(())
}

async fn server_duplicate_attach_closes_connection(stage: ServerDuplicateStage) -> TestResult {
    let mut node = Node::new(true).await?;
    let main = channels(true, 0);
    let sibling = channels(true, 1);
    let waiting = channels(true, 2);
    let mut session = node.session(main, 0).await?;
    let mut sibling_session = node.session(sibling, 0).await?;
    let mut waiting_session = node.session(waiting, 0).await?;
    let existing = Link {
        channels: main,
        peer: 55,
        local: 0,
    };
    let mut pending = None;
    let mut receiver = match stage {
        ServerDuplicateStage::Pending => {
            pending = Some(node.pending(&mut session, existing, Role::Sender).await?);
            None
        }
        ServerDuplicateStage::Installed | ServerDuplicateStage::Closing => {
            Some(node.receiver(&mut session, existing).await?)
        }
    };
    if matches!(stage, ServerDuplicateStage::Closing) {
        timeout(
            IO_TIMEOUT,
            receiver.as_ref().expect("installed link").close(),
        )
        .await??;
        node.peer.detach(existing).await?;
    }
    let sibling_link = Link {
        channels: sibling,
        peer: 77,
        local: 0,
    };
    let mut sibling_receiver = node.receiver(&mut sibling_session, sibling_link).await?;
    let waiting_link = Link {
        channels: waiting,
        peer: 88,
        local: 0,
    };
    let mut sender = node
        .sender_without_credit(&mut waiting_session, waiting_link)
        .await?;
    let sending = sender.send(Message::data(vec![22]));
    tokio::pin!(sending);
    let frames = tokio::select! {
        biased;
        result = &mut sending => panic!("a sender without credit must remain blocked: {result:?}"),
        frames = node.peer.barrier(waiting) => frames?,
    };
    only_session_flow(&frames, waiting.outgoing);

    let mut duplicate = attach(existing, Role::Sender);
    duplicate.name = "different-name-same-bound-handle".into();
    node.peer
        .send(
            main.incoming,
            Performative::Attach(Box::new(duplicate)),
            Vec::new(),
        )
        .await?;
    node.peer.close("amqp:session:handle-in-use").await?;

    // Authority is retired before the peer acknowledges the connection Close.
    assert!(matches!(
        timeout(IO_TIMEOUT, &mut sending).await?,
        Err(EngineError::RemoteDetached | EngineError::RemoteClosed)
    ));
    if let Some(receiver) = receiver.as_mut() {
        assert!(matches!(
            timeout(IO_TIMEOUT, receiver.recv()).await?,
            Err(EngineError::RemoteDetached)
        ));
    }
    assert!(matches!(
        timeout(IO_TIMEOUT, sibling_receiver.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));
    if let Some(incoming) = pending {
        let Session::Server(server_session) = &session else {
            panic!("server receipt")
        };
        assert!(matches!(
            timeout(IO_TIMEOUT, server_session.accept_attach(incoming, 4096)).await?,
            Err(EngineError::RemoteDetached)
        ));
    }
    server_approval_queue_is_retired(&mut session).await?;
    server_approval_queue_is_retired(&mut sibling_session).await?;
    server_approval_queue_is_retired(&mut waiting_session).await?;
    node.peer.acknowledge_close_and_expect_eof().await?;
    let Connection::Server(connection) = &mut node.connection else {
        panic!("server connection queue")
    };
    assert!(
        timeout(IO_TIMEOUT, connection.next_incoming_session())
            .await?
            .is_none()
    );
    node.shutdown().await;
    Ok(())
}

#[derive(Clone, Copy)]
enum ClientDuplicateStage {
    UnmatchedResponse,
    PendingResponse,
    Closing,
}

async fn client_duplicate_attach_closes_connection(stage: ClientDuplicateStage) -> TestResult {
    let mut node = Node::new(false).await?;
    let main = channels(false, 0);
    let sibling = channels(false, 1);
    let mut session = node
        .session(
            main,
            u32::from(matches!(stage, ClientDuplicateStage::PendingResponse)),
        )
        .await?;
    let mut sibling_session = node.session(sibling, 0).await?;
    let existing = Link {
        channels: main,
        peer: 55,
        local: 0,
    };
    let mut receiver = node.receiver(&mut session, existing).await?;
    let sibling_link = Link {
        channels: sibling,
        peer: 77,
        local: 0,
    };
    let mut sibling_receiver = node.receiver(&mut sibling_session, sibling_link).await?;
    let mut duplicate = attach(existing, Role::Sender);
    duplicate.name = "unmatched-name-same-bound-handle".into();
    match stage {
        ClientDuplicateStage::UnmatchedResponse => {
            let Connection::Client(connection) = &mut node.connection else {
                panic!("client connection")
            };
            let (result, wire) = tokio::join!(timeout(IO_TIMEOUT, connection.begin()), async {
                assert!(matches!(node.peer.read().await?, Frame::Amqp {
                        channel: 2, performative: Some(Performative::Begin(begin)), ..
                    } if begin.remote_channel.is_none()));
                node.peer
                    .send(
                        main.incoming,
                        Performative::Attach(Box::new(duplicate)),
                        Vec::new(),
                    )
                    .await?;
                node.peer.close("amqp:session:handle-in-use").await
            });
            wire?;
            assert!(matches!(result?, Err(EngineError::RemoteClosed)));
        }
        ClientDuplicateStage::PendingResponse => {
            let Session::Client(client_session) = &mut session else {
                panic!("client pending Attach")
            };
            let collision = Link {
                channels: main,
                peer: existing.peer,
                local: 1,
            };
            let (result, wire) = tokio::join!(
                timeout(
                    IO_TIMEOUT,
                    client_session.attach_receiver("colliding-response", "queue")
                ),
                async {
                    let request = node.peer.own_attach(collision, Role::Receiver).await?;
                    let mut response =
                        request.response(request.source.clone(), request.target.clone());
                    response.handle = existing.peer;
                    node.peer
                        .send(
                            main.incoming,
                            Performative::Attach(Box::new(response)),
                            Vec::new(),
                        )
                        .await?;
                    node.peer.close("amqp:session:handle-in-use").await
                }
            );
            wire?;
            assert!(matches!(result?, Err(EngineError::RemoteClosed)));
        }
        ClientDuplicateStage::Closing => {
            let (result, wire) = tokio::join!(timeout(IO_TIMEOUT, receiver.close()), async {
                node.peer.detach(existing).await?;
                node.peer
                    .send(
                        main.incoming,
                        Performative::Attach(Box::new(duplicate)),
                        Vec::new(),
                    )
                    .await?;
                node.peer.close("amqp:session:handle-in-use").await
            });
            wire?;
            assert!(matches!(result?, Err(EngineError::RemoteClosed)));
        }
    }
    assert!(matches!(
        timeout(IO_TIMEOUT, receiver.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        timeout(IO_TIMEOUT, sibling_receiver.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));
    for session in [&mut session, &mut sibling_session] {
        let Session::Client(session) = session else {
            panic!("client retired session")
        };
        assert!(matches!(
            timeout(IO_TIMEOUT, session.attach_receiver("after-close", "queue")).await?,
            Err(EngineError::RemoteDetached)
        ));
    }
    node.peer.acknowledge_close_and_expect_eof().await?;
    node.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn server_routes_both_link_roles_with_distinct_channels_and_handles() -> TestResult {
    timeout(CASE_TIMEOUT, both_roles(true)).await?
}

#[tokio::test]
async fn client_routes_both_link_roles_with_distinct_channels_and_handles() -> TestResult {
    timeout(CASE_TIMEOUT, both_roles(false)).await?
}

#[tokio::test]
async fn server_crossed_handles_keep_exact_closing_and_reused_link_owners() -> TestResult {
    timeout(CASE_TIMEOUT, crossed_handles_and_closing_ownership(true)).await?
}

#[tokio::test]
async fn client_crossed_handles_keep_exact_closing_and_reused_link_owners() -> TestResult {
    timeout(CASE_TIMEOUT, crossed_handles_and_closing_ownership(false)).await?
}

#[tokio::test]
async fn server_pending_echo_is_deferred_until_its_own_attach_publishes_the_handle() -> TestResult {
    timeout(CASE_TIMEOUT, server_pending_echo_waits_for_own_attach()).await?
}

#[tokio::test]
async fn server_pending_cancel_publishes_own_attach_then_detach_and_retires_receipt() -> TestResult
{
    timeout(
        CASE_TIMEOUT,
        server_pending_detach_publishes_own_handle_before_acknowledging(),
    )
    .await?
}

#[tokio::test]
async fn client_flow_before_peer_attach_ends_only_the_unpublished_link_session() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        client_unpublished_peer_handle_ends_only_its_session(),
    )
    .await?
}

#[tokio::test]
async fn server_zero_handle_maximum_still_receives_high_peer_handles_for_both_roles() -> TestResult
{
    timeout(
        CASE_TIMEOUT,
        zero_maximum_accepts_high_peer_handles_for_both_roles(true),
    )
    .await?
}

#[tokio::test]
async fn client_zero_handle_maximum_still_receives_high_peer_handles_for_both_roles() -> TestResult
{
    timeout(
        CASE_TIMEOUT,
        zero_maximum_accepts_high_peer_handles_for_both_roles(false),
    )
    .await?
}

#[tokio::test]
async fn server_output_handle_exhaustion_refuses_its_session_without_harming_a_sibling()
-> TestResult {
    timeout(
        CASE_TIMEOUT,
        server_handle_range_exhaustion_preserves_sibling(),
    )
    .await?
}

#[tokio::test]
async fn client_output_handle_exhaustion_is_local_and_retries_after_exact_detach_ack() -> TestResult
{
    timeout(
        CASE_TIMEOUT,
        client_handle_range_refusal_and_retry_preserve_sibling(),
    )
    .await?
}

#[tokio::test]
async fn server_local_handle_number_is_not_an_incoming_peer_alias() -> TestResult {
    timeout(CASE_TIMEOUT, numeric_local_handle_is_not_a_peer_alias(true)).await?
}

#[tokio::test]
async fn client_local_handle_number_is_not_an_incoming_peer_alias() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        numeric_local_handle_is_not_a_peer_alias(false),
    )
    .await?
}

#[tokio::test]
async fn server_duplicate_installed_peer_handle_closes_every_mapped_session() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        server_duplicate_attach_closes_connection(ServerDuplicateStage::Installed),
    )
    .await?
}

#[tokio::test]
async fn server_duplicate_pending_peer_handle_closes_and_retires_approval() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        server_duplicate_attach_closes_connection(ServerDuplicateStage::Pending),
    )
    .await?
}

#[tokio::test]
async fn server_duplicate_normally_closing_peer_handle_is_still_connection_fatal() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        server_duplicate_attach_closes_connection(ServerDuplicateStage::Closing),
    )
    .await?
}

#[tokio::test]
async fn client_duplicate_peer_handle_is_checked_before_pending_attach_name_lookup() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        client_duplicate_attach_closes_connection(ClientDuplicateStage::UnmatchedResponse),
    )
    .await?
}

#[tokio::test]
async fn client_colliding_attach_response_closes_connection_and_fails_pending_caller() -> TestResult
{
    timeout(
        CASE_TIMEOUT,
        client_duplicate_attach_closes_connection(ClientDuplicateStage::PendingResponse),
    )
    .await?
}

#[tokio::test]
async fn client_duplicate_normally_closing_peer_handle_fails_exact_pending_detach() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        client_duplicate_attach_closes_connection(ClientDuplicateStage::Closing),
    )
    .await?
}
