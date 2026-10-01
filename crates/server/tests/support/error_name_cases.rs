use amqp::{
    Attach, End, EngineError, Frame, Message, OrderedMap, Performative, Role, Symbol,
    encode_message,
};
use tokio::time::timeout;

use super::fixture::{
    CASE_TIMEOUT, Channels, IO_TIMEOUT, Link, Node, Receiving, Session, TestResult, attach,
    channels, detach, transfer,
};

pub(super) const ERROR_NAME: &str = "errored-receiver";

pub(super) struct ErrorFixture {
    pub node: Node,
    pub session: Session,
    _other_session: Session,
    errored: Receiving,
    pub healthy: Receiving,
    pub survivor: Receiving,
    pub main: Channels,
    pub error_link: Link,
    pub healthy_link: Link,
    pub survivor_link: Link,
}

impl ErrorFixture {
    pub async fn new(server: bool) -> TestResult<Self> {
        let mut node = Node::new(server).await?;
        let main = channels(server, 0);
        let other = channels(server, 1);
        let mut session = node.session(main, 1).await?;
        let mut other_session = node.session(other, 0).await?;
        let error_link = Link {
            channels: main,
            peer: 55,
            local: 0,
        };
        let healthy_link = Link {
            channels: main,
            peer: 0,
            local: 1,
        };
        let survivor_link = Link {
            channels: other,
            peer: 77,
            local: 0,
        };
        let errored = node
            .receiver_named(&mut session, error_link, ERROR_NAME)
            .await?;
        let healthy = node.receiver(&mut session, healthy_link).await?;
        let survivor = node.receiver(&mut other_session, survivor_link).await?;
        Ok(Self {
            node,
            session,
            _other_session: other_session,
            errored,
            healthy,
            survivor,
            main,
            error_link,
            healthy_link,
            survivor_link,
        })
    }

    pub async fn error(&mut self) -> TestResult {
        let mut unsupported = transfer(self.error_link.peer, 0);
        unsupported.message_format = Some(999);
        self.node
            .peer
            .send(
                self.main.incoming,
                Performative::Transfer(unsupported),
                Vec::new(),
            )
            .await?;
        self.node
            .peer
            .error_detach(self.error_link, "amqp:not-implemented")
            .await?;
        assert!(matches!(
            timeout(IO_TIMEOUT, self.errored.recv()).await?,
            Err(EngineError::RemoteDetached)
        ));
        Ok(())
    }

    pub async fn ack(&mut self) -> TestResult {
        self.node
            .peer
            .send(
                self.main.incoming,
                Performative::Detach(detach(self.error_link.peer)),
                Vec::new(),
            )
            .await?;
        only_flows(&self.node.peer.barrier(self.main).await?);
        Ok(())
    }

    pub async fn prove_siblings(&mut self) -> TestResult {
        deliver(&mut self.node, &mut self.healthy, self.healthy_link, 1, 1).await?;
        deliver(&mut self.node, &mut self.survivor, self.survivor_link, 0, 1).await
    }

    pub fn replacement(&self, peer: u32) -> Link {
        Link {
            channels: self.main,
            peer,
            local: 0,
        }
    }

    pub async fn request(&mut self, request: Attach) -> TestResult {
        self.node
            .peer
            .send(
                self.main.incoming,
                Performative::Attach(Box::new(request)),
                Vec::new(),
            )
            .await
    }

    pub async fn finish_end(&mut self, condition: &str) -> TestResult {
        strict_end(&mut self.node, self.main, Some(condition)).await?;
        self.finish_ended().await
    }

    pub async fn finish_ended(&mut self) -> TestResult {
        assert!(matches!(
            timeout(IO_TIMEOUT, self.healthy.recv()).await?,
            Err(EngineError::RemoteDetached)
        ));
        if let Session::Server(session) = &mut self.session {
            assert!(
                timeout(IO_TIMEOUT, session.next_incoming_attach())
                    .await?
                    .is_none()
            );
        }
        deliver(&mut self.node, &mut self.survivor, self.survivor_link, 1, 2).await?;
        self.node
            .peer
            .send(
                self.main.incoming,
                Performative::End(End::default()),
                Vec::new(),
            )
            .await?;
        only_flows(&self.node.peer.barrier(self.survivor_link.channels).await?);
        self.node.shutdown().await;
        Ok(())
    }

    async fn replace_session(&mut self, server: bool) -> TestResult {
        self.node
            .peer
            .send(
                self.main.incoming,
                Performative::End(End::default()),
                Vec::new(),
            )
            .await?;
        strict_end(&mut self.node, self.main, None).await?;
        only_flows(&self.node.peer.barrier(self.survivor_link.channels).await?);
        self.main = Channels {
            incoming: if server { 57 } else { 29 },
            outgoing: if server { 0 } else { 2 },
        };
        self.session = self.node.session(self.main, 1).await?;
        Ok(())
    }
}

pub(super) async fn deliver(
    node: &mut Node,
    receiver: &mut Receiving,
    link: Link,
    id: u32,
    delivery_count: u32,
) -> TestResult {
    let message = Message::data(vec![60; 8]);
    node.peer
        .send(
            link.channels.incoming,
            Performative::Transfer(transfer(link.peer, id)),
            encode_message(&message)?,
        )
        .await?;
    let delivery = timeout(IO_TIMEOUT, receiver.recv()).await??;
    assert_eq!(delivery.message(), &message);
    let expected_incoming = node.peer.flow(link.channels).next_outgoing_id;
    let credit = node.peer.read().await?;
    assert!(
        matches!(&credit, Frame::Amqp {
        channel, performative: Some(Performative::Flow(flow)), payload,
    } if *channel == link.channels.outgoing
        && flow.handle == Some(link.local)
        && flow.delivery_count == Some(delivery_count)
        && flow.link_credit == Some(32)
        && flow.next_incoming_id == Some(expected_incoming)
        && payload.is_empty()),
        "consume exact credit before triggering refusal: {credit:?}"
    );
    timeout(IO_TIMEOUT, receiver.accept(&delivery)).await??;
    node.peer.accepted(link.channels, id).await
}

pub(super) fn only_flows(frames: &[Frame]) {
    assert!(
        frames.iter().all(|frame| matches!(
            frame,
            Frame::Amqp {
                performative: Some(Performative::Flow(_)),
                ..
            }
        )),
        "no link traffic, disposition, End, or Close may precede the barrier: {frames:?}"
    );
}

pub(super) async fn strict_end(
    node: &mut Node,
    channels: Channels,
    condition: Option<&str>,
) -> TestResult {
    let frame = node.peer.read().await?;
    let Frame::Amqp {
        channel,
        performative: Some(Performative::End(end)),
        payload,
    } = &frame
    else {
        panic!("expected immediate mapped End: {frame:?}")
    };
    assert_eq!(*channel, channels.outgoing);
    assert!(payload.is_empty());
    assert_eq!(
        end.error.as_ref().map(|error| error.condition.as_symbol()),
        condition.map(Symbol::from)
    );
    Ok(())
}

pub(super) fn named_request(link: Link, name: &str, empty: bool) -> Attach {
    let mut request = attach(link, Role::Sender);
    request.name = name.into();
    request.unsettled = empty.then(OrderedMap::new);
    request
}

async fn known_null_name(server: bool, after_end: bool) -> TestResult {
    let mut fixture = ErrorFixture::new(server).await?;
    fixture.error().await?;
    fixture.ack().await?;
    fixture.prove_siblings().await?;
    if after_end {
        fixture.replace_session(server).await?;
    }
    fixture
        .request(named_request(fixture.replacement(56), ERROR_NAME, false))
        .await?;
    fixture.finish_end("amqp:session:errant-link").await
}

async fn known_empty_name(server: bool) -> TestResult {
    let mut fixture = ErrorFixture::new(server).await?;
    fixture.error().await?;
    fixture.ack().await?;
    fixture.prove_siblings().await?;
    let replacement = fixture.replacement(56);
    fixture
        .request(named_request(replacement, ERROR_NAME, true))
        .await?;
    if server {
        let response = fixture
            .node
            .peer
            .own_attach(replacement, Role::Receiver)
            .await?;
        assert_eq!(response.name, ERROR_NAME);
        assert!(response.source.is_none() && response.target.is_none());
        fixture
            .node
            .peer
            .error_detach(replacement, "amqp:not-implemented")
            .await?;
        deliver(
            &mut fixture.node,
            &mut fixture.healthy,
            fixture.healthy_link,
            2,
            2,
        )
        .await?;
        deliver(
            &mut fixture.node,
            &mut fixture.survivor,
            fixture.survivor_link,
            1,
            2,
        )
        .await?;
        fixture.node.shutdown().await;
        Ok(())
    } else {
        fixture.finish_end("amqp:not-implemented").await
    }
}

async fn occupied_error_null(server: bool) -> TestResult {
    for name in [ERROR_NAME, "unrelated-name"] {
        let mut fixture = ErrorFixture::new(server).await?;
        fixture.error().await?;
        fixture.prove_siblings().await?;
        fixture
            .request(named_request(fixture.error_link, name, false))
            .await?;
        fixture.finish_end("amqp:session:errant-link").await?;
    }
    Ok(())
}

async fn occupied_error_empty(server: bool) -> TestResult {
    let mut fixture = ErrorFixture::new(server).await?;
    fixture.error().await?;
    fixture.prove_siblings().await?;
    fixture
        .request(named_request(fixture.error_link, ERROR_NAME, true))
        .await?;
    fixture.finish_end("amqp:not-implemented").await
}

async fn opposite_role_name(server: bool) -> TestResult {
    let mut fixture = ErrorFixture::new(server).await?;
    fixture.error().await?;
    fixture.ack().await?;
    fixture.prove_siblings().await?;
    let replacement = fixture.replacement(56);
    let mut sender = fixture
        .node
        .sender_named(&mut fixture.session, replacement, ERROR_NAME)
        .await?;
    fixture
        .node
        .outgoing_message(&mut sender, replacement, 61)
        .await?;
    deliver(
        &mut fixture.node,
        &mut fixture.healthy,
        fixture.healthy_link,
        2,
        2,
    )
    .await?;
    deliver(
        &mut fixture.node,
        &mut fixture.survivor,
        fixture.survivor_link,
        1,
        2,
    )
    .await?;
    fixture.node.shutdown().await;
    Ok(())
}

async fn fresh_empty_name(server: bool) -> TestResult {
    let mut fixture = ErrorFixture::new(server).await?;
    fixture.error().await?;
    fixture.ack().await?;
    fixture.prove_siblings().await?;
    let replacement = fixture.replacement(56);
    let name = "genuinely-fresh-empty-map";
    let mut receiver = if let Session::Server(session) = &mut fixture.session {
        fixture
            .node
            .peer
            .send(
                fixture.main.incoming,
                Performative::Attach(Box::new(named_request(replacement, name, true))),
                Vec::new(),
            )
            .await?;
        let incoming = timeout(IO_TIMEOUT, session.next_incoming_attach())
            .await?
            .expect("fresh empty map approval");
        assert_eq!(incoming.attach().name, name);
        fixture
            .node
            .approve_receiver(&fixture.session, incoming, replacement)
            .await?
    } else {
        let Session::Client(session) = &mut fixture.session else {
            unreachable!()
        };
        let (receiver, wire) = tokio::join!(
            timeout(IO_TIMEOUT, session.attach_receiver(name, "queue")),
            async {
                let request = fixture
                    .node
                    .peer
                    .own_attach(replacement, Role::Receiver)
                    .await?;
                assert_eq!(request.name, name);
                let mut response = request.response(request.source.clone(), request.target.clone());
                response.handle = replacement.peer;
                response.unsettled = Some(OrderedMap::new());
                fixture
                    .node
                    .peer
                    .send(
                        fixture.main.incoming,
                        Performative::Attach(Box::new(response)),
                        Vec::new(),
                    )
                    .await
            }
        );
        wire?;
        fixture.node.peer.receiving_flow(replacement).await?;
        Receiving::Client(Box::new(receiver??))
    };
    deliver(&mut fixture.node, &mut receiver, replacement, 2, 1).await?;
    deliver(
        &mut fixture.node,
        &mut fixture.healthy,
        fixture.healthy_link,
        3,
        2,
    )
    .await?;
    deliver(
        &mut fixture.node,
        &mut fixture.survivor,
        fixture.survivor_link,
        1,
        2,
    )
    .await?;
    fixture.node.shutdown().await;
    Ok(())
}

async fn client_local_null_name_refusal() -> TestResult {
    let mut fixture = ErrorFixture::new(false).await?;
    fixture.error().await?;
    fixture.ack().await?;
    fixture.prove_siblings().await?;
    let Session::Client(session) = &mut fixture.session else {
        unreachable!()
    };
    assert!(matches!(
        timeout(IO_TIMEOUT, session.attach_receiver(ERROR_NAME, "queue")).await?,
        Err(EngineError::InvalidState(_))
    ));
    only_flows(&fixture.node.peer.barrier(fixture.main).await?);
    let replacement = fixture.replacement(56);
    let mut receiver = fixture
        .node
        .receiver_named(
            &mut fixture.session,
            replacement,
            "fresh-after-local-refusal",
        )
        .await?;
    deliver(&mut fixture.node, &mut receiver, replacement, 2, 1).await?;
    deliver(
        &mut fixture.node,
        &mut fixture.survivor,
        fixture.survivor_link,
        1,
        2,
    )
    .await?;
    fixture.node.shutdown().await;
    Ok(())
}

macro_rules! name_case {
    ($name:ident, $case:expr) => {
        #[tokio::test]
        async fn $name() -> TestResult {
            timeout(CASE_TIMEOUT, $case).await?
        }
    };
}

name_case!(
    server_known_error_name_null_attach_after_ack_ends_session,
    known_null_name(true, false)
);
name_case!(
    client_known_error_name_null_response_after_ack_ends_session,
    known_null_name(false, false)
);
name_case!(
    server_known_error_name_survives_session_end,
    known_null_name(true, true)
);
name_case!(
    client_known_error_name_survives_session_end,
    known_null_name(false, true)
);
name_case!(
    server_known_error_name_empty_map_is_an_explicit_unsupported_resume,
    known_empty_name(true)
);
name_case!(
    client_known_error_name_unsolicited_empty_map_ends_not_implemented,
    known_empty_name(false)
);
name_case!(
    server_occupied_error_handle_null_attach_ends_errant_for_same_or_unrelated_name,
    occupied_error_null(true)
);
name_case!(
    client_occupied_error_handle_null_attach_ends_errant_for_same_or_unrelated_name,
    occupied_error_null(false)
);
name_case!(
    server_occupied_error_handle_known_empty_resume_ends_without_replacement,
    occupied_error_empty(true)
);
name_case!(
    client_occupied_error_handle_known_empty_resume_ends_without_replacement,
    occupied_error_empty(false)
);
name_case!(
    server_known_error_name_is_scoped_to_exact_link_direction,
    opposite_role_name(true)
);
name_case!(
    client_known_error_name_is_scoped_to_exact_link_direction,
    opposite_role_name(false)
);
name_case!(
    server_fresh_empty_unsettled_map_keeps_existing_attach_behavior,
    fresh_empty_name(true)
);
name_case!(
    client_fresh_empty_unsettled_map_keeps_existing_attach_behavior,
    fresh_empty_name(false)
);
name_case!(
    client_local_known_error_name_refusal_publishes_no_attach_and_preserves_fresh_capacity,
    client_local_null_name_refusal()
);
