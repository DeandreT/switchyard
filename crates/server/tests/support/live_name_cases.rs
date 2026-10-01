use std::{future::Future, pin::Pin, task::Poll};

use amqp::{Attach, End, EngineError, IncomingAttach, Performative, Role};
use tokio::time::timeout;

use super::{
    error_name_cases::{deliver, only_flows, strict_end},
    fixture::{
        CASE_TIMEOUT, IO_TIMEOUT, Link, Node, Receiving, Sending, Session, TestResult, attach,
        channels, detach,
    },
};

const NAME: &str = "one-live-direction";

#[derive(Clone, Copy)]
enum Stage {
    Pending,
    Installed,
    Closing,
}

fn link(server: bool, session: u16, peer: u32, local: u32) -> Link {
    Link {
        channels: channels(server, session),
        peer,
        local,
    }
}

fn named(link: Link, role: Role) -> Attach {
    let mut request = attach(link, role);
    request.name = NAME.into();
    request
}

async fn pending_receiver(
    node: &mut Node,
    session: &mut Session,
    link: Link,
) -> TestResult<IncomingAttach> {
    node.peer
        .send(
            link.channels.incoming,
            Performative::Attach(Box::new(named(link, Role::Sender))),
            Vec::new(),
        )
        .await?;
    let Session::Server(session) = session else {
        panic!("server approval")
    };
    let receipt = timeout(IO_TIMEOUT, session.next_incoming_attach())
        .await?
        .expect("original approval");
    assert_eq!(receipt.attach().name, NAME);
    assert_eq!(receipt.attach().handle, link.peer);
    Ok(receipt)
}

async fn assert_no_approval(session: &mut Session) -> TestResult {
    let Session::Server(session) = session else {
        panic!("server approval")
    };
    assert!(
        timeout(IO_TIMEOUT, session.next_incoming_attach())
            .await?
            .is_none(),
        "refused session must publish no duplicate-name approval"
    );
    Ok(())
}

async fn server_duplicate(stage: Stage, cross_session: bool) -> TestResult {
    let mut node = Node::new(true).await?;
    let mut original_session = node.session(channels(true, 0), 1).await?;
    let mut other_session = node.session(channels(true, 1), 1).await?;
    let mut survivor_session = node.session(channels(true, 2), 0).await?;
    let original_link = link(true, 0, 55, 0);
    let mut receipt = None;
    let mut original = match stage {
        Stage::Pending => {
            receipt =
                Some(pending_receiver(&mut node, &mut original_session, original_link).await?);
            None
        }
        Stage::Installed | Stage::Closing => Some(
            node.receiver_named(&mut original_session, original_link, NAME)
                .await?,
        ),
    };
    if matches!(stage, Stage::Closing) {
        timeout(
            IO_TIMEOUT,
            original.as_ref().expect("installed owner").close(),
        )
        .await??;
        node.peer.detach(original_link).await?;
    }
    let target = if cross_session {
        channels(true, 1)
    } else {
        channels(true, 0)
    };
    let target_link = Link {
        channels: target,
        peer: 0,
        local: u32::from(!cross_session),
    };
    let mut target_receiver = if cross_session {
        node.receiver(&mut other_session, target_link).await?
    } else {
        node.receiver(&mut original_session, target_link).await?
    };
    let survivor_link = link(true, 2, 77, 0);
    let mut survivor = node.receiver(&mut survivor_session, survivor_link).await?;
    let duplicate = Link {
        channels: target,
        peer: 56,
        local: 1,
    };
    node.peer
        .send(
            target.incoming,
            Performative::Attach(Box::new(named(duplicate, Role::Sender))),
            Vec::new(),
        )
        .await?;
    strict_end(&mut node, target, Some("amqp:not-implemented")).await?;
    assert!(matches!(
        timeout(IO_TIMEOUT, target_receiver.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));
    if cross_session {
        assert_no_approval(&mut other_session).await?;
    } else {
        assert_no_approval(&mut original_session).await?;
    }

    if cross_session {
        if let Some(receipt) = receipt.take() {
            original = Some(
                node.approve_receiver(&original_session, receipt, original_link)
                    .await?,
            );
        }
        if matches!(stage, Stage::Closing) {
            let healthy_link = link(true, 0, 0, 1);
            let mut healthy = node.receiver(&mut original_session, healthy_link).await?;
            deliver(&mut node, &mut healthy, healthy_link, 0, 1).await?;
            node.peer
                .send(
                    original_link.channels.incoming,
                    Performative::Detach(detach(original_link.peer)),
                    Vec::new(),
                )
                .await?;
            only_flows(&node.peer.barrier(original_link.channels).await?);
        } else {
            let original = original.as_mut().expect("original endpoint survived");
            deliver(&mut node, original, original_link, 0, 1).await?;
            node.close_receiver(original, original_link).await?;
        }
        // Refusing the duplicate must not create error history for the live owner's name.
        let replacement_link = link(true, 0, 56, 0);
        let mut replacement = node
            .receiver_named(&mut original_session, replacement_link, NAME)
            .await?;
        deliver(&mut node, &mut replacement, replacement_link, 1, 1).await?;
    } else if let Some(receipt) = receipt.take() {
        let Session::Server(session) = &original_session else {
            unreachable!()
        };
        assert!(matches!(
            timeout(IO_TIMEOUT, session.accept_attach(receipt, 4 * 1024 * 1024)).await?,
            Err(EngineError::RemoteDetached)
        ));
    }
    deliver(&mut node, &mut survivor, survivor_link, 0, 1).await?;
    node.peer
        .send(
            target.incoming,
            Performative::End(End::default()),
            Vec::new(),
        )
        .await?;
    only_flows(&node.peer.barrier(survivor_link.channels).await?);
    if !cross_session {
        let fresh_channels = super::fixture::Channels {
            incoming: 58,
            outgoing: 0,
        };
        let mut fresh_session = node.session(fresh_channels, 0).await?;
        let fresh_link = Link {
            channels: fresh_channels,
            peer: 55,
            local: 0,
        };
        let mut fresh = node
            .receiver_named(&mut fresh_session, fresh_link, NAME)
            .await?;
        deliver(&mut node, &mut fresh, fresh_link, 0, 1).await?;
    }
    node.shutdown().await;
    Ok(())
}

async fn local_refusal(session: &mut Session) -> TestResult {
    let Session::Client(session) = session else {
        panic!("client local attach")
    };
    assert!(
        matches!(
            timeout(IO_TIMEOUT, session.attach_receiver(NAME, "queue")).await?,
            Err(EngineError::InvalidState(_))
        ),
        "duplicate local name must fail before publishing an Attach"
    );
    Ok(())
}

async fn client_duplicate(stage: Stage, cross_session: bool) -> TestResult {
    let mut node = Node::new(false).await?;
    let mut original_session = node.session(channels(false, 0), 1).await?;
    let mut other_session = node.session(channels(false, 1), 1).await?;
    let mut survivor_session = node.session(channels(false, 2), 0).await?;
    let original_link = link(false, 0, 55, 0);
    let survivor_link = link(false, 2, 77, 0);
    let mut survivor = node.receiver(&mut survivor_session, survivor_link).await?;
    let mut original = node
        .receiver_named(&mut original_session, original_link, NAME)
        .await?;
    if matches!(stage, Stage::Closing) {
        timeout(IO_TIMEOUT, async {
            tokio::select! {
                result = original.close() => panic!("close must wait for the peer ACK: {result:?}"),
                result = node.peer.detach(original_link) => result,
            }
        })
        .await??;
    }
    if cross_session {
        local_refusal(&mut other_session).await?;
    } else {
        local_refusal(&mut original_session).await?;
    }
    only_flows(&node.peer.barrier(original_link.channels).await?);
    only_flows(&node.peer.barrier(channels(false, 1)).await?);
    let fresh_link = if cross_session {
        link(false, 1, 56, 0)
    } else {
        link(false, 0, 56, 1)
    };
    let mut fresh = if cross_session {
        node.receiver_named(&mut other_session, fresh_link, "different-live-name")
            .await?
    } else {
        node.receiver_named(&mut original_session, fresh_link, "different-live-name")
            .await?
    };
    deliver(&mut node, &mut fresh, fresh_link, 0, 1).await?;
    if matches!(stage, Stage::Closing) {
        assert!(matches!(
            timeout(IO_TIMEOUT, original.recv()).await?,
            Err(EngineError::RemoteDetached)
        ));
        node.peer
            .send(
                original_link.channels.incoming,
                Performative::Detach(detach(original_link.peer)),
                Vec::new(),
            )
            .await?;
        only_flows(&node.peer.barrier(original_link.channels).await?);
    } else {
        deliver(
            &mut node,
            &mut original,
            original_link,
            u32::from(!cross_session),
            1,
        )
        .await?;
        node.close_receiver(&original, original_link).await?;
    }
    let released_link = link(false, 0, 57, u32::from(cross_session));
    let mut released = node
        .receiver_named(&mut original_session, released_link, NAME)
        .await?;
    let released_id = match (stage, cross_session) {
        (Stage::Closing, true) => 0,
        (Stage::Closing, false) => 1,
        (_, true) => 1,
        (_, false) => 2,
    };
    deliver(&mut node, &mut released, released_link, released_id, 1).await?;
    deliver(&mut node, &mut survivor, survivor_link, 0, 1).await?;
    node.shutdown().await;
    Ok(())
}

async fn require_pending<T>(future: Pin<&mut impl Future<Output = T>>) {
    let mut future = future;
    std::future::poll_fn(|cx| {
        assert!(
            future.as_mut().poll(cx).is_pending(),
            "unanswered owner must remain pending"
        );
        Poll::Ready(())
    })
    .await;
}

async fn client_pending_duplicate() -> TestResult {
    let mut node = Node::new(false).await?;
    let mut original_session = node.session(channels(false, 0), 1).await?;
    let mut target_session = node.session(channels(false, 1), 1).await?;
    let mut survivor_session = node.session(channels(false, 2), 0).await?;
    let original_link = link(false, 0, 55, 0);
    let survivor_link = link(false, 2, 77, 0);
    let mut survivor = node.receiver(&mut survivor_session, survivor_link).await?;
    let Session::Client(session) = &mut original_session else {
        unreachable!()
    };
    let mut pending = Box::pin(session.attach_receiver(NAME, "queue"));
    let request = timeout(IO_TIMEOUT, async {
        tokio::select! {
            _ = &mut pending => panic!("first attach awaits its peer"),
            result = node.peer.own_attach(original_link, Role::Receiver) => result,
        }
    })
    .await??;
    assert_eq!(request.name, NAME);
    local_refusal(&mut target_session).await?;
    only_flows(&node.peer.barrier(original_link.channels).await?);
    only_flows(&node.peer.barrier(channels(false, 1)).await?);
    require_pending(pending.as_mut()).await;
    let mut response = request.response(request.source.clone(), request.target.clone());
    response.handle = original_link.peer;
    node.peer
        .send(
            original_link.channels.incoming,
            Performative::Attach(Box::new(response)),
            Vec::new(),
        )
        .await?;
    let mut original = Receiving::Client(Box::new(timeout(IO_TIMEOUT, pending).await??));
    node.peer.receiving_flow(original_link).await?;
    deliver(&mut node, &mut original, original_link, 0, 1).await?;
    let fresh_link = link(false, 1, 56, 0);
    let mut fresh = node
        .receiver_named(
            &mut target_session,
            fresh_link,
            "fresh-after-pending-refusal",
        )
        .await?;
    deliver(&mut node, &mut fresh, fresh_link, 0, 1).await?;
    deliver(&mut node, &mut survivor, survivor_link, 0, 1).await?;
    node.shutdown().await;
    Ok(())
}

async fn pending_opposite_roles() -> TestResult {
    let mut node = Node::new(false).await?;
    let mut sending_session = node.session(channels(false, 0), 0).await?;
    let mut receiving_session = node.session(channels(false, 1), 0).await?;
    let mut survivor_session = node.session(channels(false, 2), 0).await?;
    let sending_link = link(false, 0, 55, 0);
    let receiving_link = link(false, 1, 56, 0);
    let survivor_link = link(false, 2, 77, 0);
    let mut survivor = node.receiver(&mut survivor_session, survivor_link).await?;
    let Session::Client(sending_session) = &mut sending_session else {
        unreachable!()
    };
    let Session::Client(receiving_session) = &mut receiving_session else {
        unreachable!()
    };
    let mut sending = Box::pin(sending_session.attach_sender(NAME, "queue"));
    let mut receiving = Box::pin(receiving_session.attach_receiver(NAME, "queue"));
    let sender_request = timeout(IO_TIMEOUT, async {
        tokio::select! {
            _ = &mut sending => panic!("sender awaits its peer response"),
            result = node.peer.own_attach(sending_link, Role::Sender) => result,
        }
    })
    .await??;
    let receiver_request = timeout(IO_TIMEOUT, async {
        tokio::select! {
            _ = &mut sending => panic!("sender remains pending while the opposite role attaches"),
            _ = &mut receiving => panic!("receiver awaits its peer response"),
            result = node.peer.own_attach(receiving_link, Role::Receiver) => result,
        }
    })
    .await??;
    assert_eq!(sender_request.name, NAME);
    assert_eq!(receiver_request.name, NAME);

    let mut wrong_channel =
        sender_request.response(sender_request.source.clone(), sender_request.target.clone());
    wrong_channel.handle = 58;
    // Exact-role ownership elsewhere suppresses fallback to the opposite pending role.
    node.peer
        .send(
            receiving_link.channels.incoming,
            Performative::Attach(Box::new(wrong_channel)),
            Vec::new(),
        )
        .await?;
    only_flows(&node.peer.barrier(receiving_link.channels).await?);
    require_pending(sending.as_mut()).await;
    require_pending(receiving.as_mut()).await;

    let mut response = receiver_request.response(
        receiver_request.source.clone(),
        receiver_request.target.clone(),
    );
    response.handle = receiving_link.peer;
    node.peer
        .send(
            receiving_link.channels.incoming,
            Performative::Attach(Box::new(response)),
            Vec::new(),
        )
        .await?;
    let mut receiver = Receiving::Client(Box::new(timeout(IO_TIMEOUT, receiving).await??));
    node.peer.receiving_flow(receiving_link).await?;
    require_pending(sending.as_mut()).await;

    let mut response =
        sender_request.response(sender_request.source.clone(), sender_request.target.clone());
    response.handle = sending_link.peer;
    node.peer
        .send(
            sending_link.channels.incoming,
            Performative::Attach(Box::new(response)),
            Vec::new(),
        )
        .await?;
    let mut sender = Sending::Client(timeout(IO_TIMEOUT, sending).await??);
    let mut flow = node.peer.flow(sending_link.channels);
    flow.handle = Some(sending_link.peer);
    flow.delivery_count = Some(0);
    flow.link_credit = Some(2);
    node.peer
        .send(
            sending_link.channels.incoming,
            Performative::Flow(flow),
            Vec::new(),
        )
        .await?;
    node.outgoing_message(&mut sender, sending_link, 7).await?;
    deliver(&mut node, &mut receiver, receiving_link, 0, 1).await?;
    deliver(&mut node, &mut survivor, survivor_link, 0, 1).await?;
    node.shutdown().await;
    Ok(())
}

async fn installed_opposite_roles(server: bool, cross_session: bool) -> TestResult {
    let mut node = Node::new(server).await?;
    let mut first = node.session(channels(server, 0), 1).await?;
    let mut second = node.session(channels(server, 1), 0).await?;
    let mut third = node.session(channels(server, 2), 0).await?;
    let receiving_link = link(server, 0, 55, 0);
    let sending_link = if cross_session {
        link(server, 1, 56, 0)
    } else {
        link(server, 0, 56, 1)
    };
    let survivor_link = link(server, 2, 77, 0);
    let mut receiver = node
        .receiver_named(&mut first, receiving_link, NAME)
        .await?;
    let mut sender = if cross_session {
        node.sender_named(&mut second, sending_link, NAME).await?
    } else {
        node.sender_named(&mut first, sending_link, NAME).await?
    };
    let mut survivor = node.receiver(&mut third, survivor_link).await?;
    deliver(&mut node, &mut receiver, receiving_link, 0, 1).await?;
    node.outgoing_message(&mut sender, sending_link, 8).await?;
    deliver(&mut node, &mut survivor, survivor_link, 0, 1).await?;
    node.shutdown().await;
    Ok(())
}

async fn normal_ack_releases_name(server: bool) -> TestResult {
    let mut node = Node::new(server).await?;
    let mut first = node.session(channels(server, 0), 0).await?;
    let mut second = node.session(channels(server, 1), 0).await?;
    let original_link = link(server, 0, 55, 0);
    let survivor_link = link(server, 1, 77, 0);
    let original = node.receiver_named(&mut first, original_link, NAME).await?;
    let mut survivor = node.receiver(&mut second, survivor_link).await?;
    node.close_receiver(&original, original_link).await?;
    only_flows(&node.peer.barrier(original_link.channels).await?);
    let replacement_link = link(server, 0, 56, 0);
    let mut replacement = node
        .receiver_named(&mut first, replacement_link, NAME)
        .await?;
    deliver(&mut node, &mut replacement, replacement_link, 0, 1).await?;
    deliver(&mut node, &mut survivor, survivor_link, 0, 1).await?;
    node.shutdown().await;
    Ok(())
}

macro_rules! case {
    ($name:ident, $function:ident $(, $argument:expr)*) => {
        #[tokio::test]
        async fn $name() -> TestResult {
            timeout(CASE_TIMEOUT, $function($($argument),*)).await??;
            Ok(())
        }
    };
}

case!(
    server_pending_name_collision_ends_only_the_same_session,
    server_duplicate,
    Stage::Pending,
    false
);
case!(
    server_pending_name_collision_preserves_the_other_session_owner,
    server_duplicate,
    Stage::Pending,
    true
);
case!(
    server_installed_name_collision_ends_only_the_same_session,
    server_duplicate,
    Stage::Installed,
    false
);
case!(
    server_installed_name_collision_preserves_the_other_session_owner,
    server_duplicate,
    Stage::Installed,
    true
);
case!(
    server_normal_closing_name_stays_reserved_on_the_same_session,
    server_duplicate,
    Stage::Closing,
    false
);
case!(
    server_normal_closing_name_stays_reserved_across_sessions,
    server_duplicate,
    Stage::Closing,
    true
);
case!(
    client_pending_same_role_name_refusal_preserves_the_original_reply,
    client_pending_duplicate
);
case!(
    client_installed_same_role_name_refusal_preserves_same_session_capacity,
    client_duplicate,
    Stage::Installed,
    false
);
case!(
    client_installed_same_role_name_refusal_preserves_other_session_capacity,
    client_duplicate,
    Stage::Installed,
    true
);
case!(
    client_normal_closing_same_role_name_refusal_preserves_same_session_capacity,
    client_duplicate,
    Stage::Closing,
    false
);
case!(
    client_normal_closing_same_role_name_refusal_preserves_other_session_capacity,
    client_duplicate,
    Stage::Closing,
    true
);
case!(
    client_pending_opposite_role_names_accept_reversed_responses_and_ignore_wrong_channel_echo,
    pending_opposite_roles
);
case!(
    server_same_session_opposite_role_names_route_both_directions,
    installed_opposite_roles,
    true,
    false
);
case!(
    server_other_session_opposite_role_names_route_both_directions,
    installed_opposite_roles,
    true,
    true
);
case!(
    client_same_session_opposite_role_names_route_both_directions,
    installed_opposite_roles,
    false,
    false
);
case!(
    client_other_session_opposite_role_names_route_both_directions,
    installed_opposite_roles,
    false,
    true
);
case!(
    server_normal_detach_ack_releases_only_live_name_ownership,
    normal_ack_releases_name,
    true
);
case!(
    client_normal_detach_ack_releases_only_live_name_ownership,
    normal_ack_releases_name,
    false
);
