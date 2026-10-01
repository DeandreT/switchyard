use amqp::{
    Accepted, AmqpError, DeliveryState, Disposition, End, EngineError, Error, Frame, Message,
    Outcome, Performative, Role, Transfer, encode_message,
};
use tokio::time::timeout;

use super::fixture::{
    CASE_TIMEOUT, Channels, IO_TIMEOUT, Link, Node, Peer, Receiving, Sending, Session, TestResult,
    channels, detach, transfer,
};

struct HistoryFixture {
    node: Node,
    session: Session,
    _other_session: Session,
    survivor: Receiving,
    survivor_link: Link,
    main: Channels,
    errored: Link,
    healthy: Link,
}

impl HistoryFixture {
    async fn new(server: bool) -> TestResult<Self> {
        let mut node = Node::new(server).await?;
        let main = channels(server, 0);
        let other = channels(server, 1);
        let session = node.session(main, 1).await?;
        let mut other_session = node.session(other, 0).await?;
        let survivor_link = Link {
            channels: other,
            peer: 77,
            local: 0,
        };
        let survivor = node.receiver(&mut other_session, survivor_link).await?;
        Ok(Self {
            node,
            session,
            _other_session: other_session,
            survivor,
            survivor_link,
            main,
            errored: Link {
                channels: main,
                peer: 55,
                local: 0,
            },
            healthy: Link {
                channels: main,
                peer: 0,
                local: 1,
            },
        })
    }

    async fn error_partial(&mut self) -> TestResult {
        let encoded = encode_message(&Message::data(vec![40; 8]))?;
        let mut initial = transfer(self.errored.peer, 0);
        initial.more = true;
        self.node
            .peer
            .send(
                self.main.incoming,
                Performative::Transfer(initial),
                encoded[..2].to_vec(),
            )
            .await?;
        let mut malformed = transfer(self.errored.peer, 0);
        malformed.delivery_id = None;
        malformed.delivery_tag = None;
        malformed.message_format = Some(999);
        self.node
            .peer
            .send(
                self.main.incoming,
                Performative::Transfer(malformed),
                encoded[2..].to_vec(),
            )
            .await?;
        self.node
            .peer
            .error_detach(self.errored, "amqp:invalid-field")
            .await
    }

    async fn ack_error(&mut self) -> TestResult {
        self.node
            .peer
            .send(
                self.main.incoming,
                Performative::Detach(detach(self.errored.peer)),
                Vec::new(),
            )
            .await?;
        only_flows(&self.node.peer.barrier(self.main).await?);
        Ok(())
    }

    async fn survive(&mut self, id: u32) -> TestResult {
        self.node
            .incoming_message(&mut self.survivor, self.survivor_link, id, 41)
            .await
    }

    async fn drain(&mut self) -> TestResult {
        for channels in [self.main, self.survivor_link.channels] {
            only_flows(&self.node.peer.barrier(channels).await?);
        }
        Ok(())
    }

    async fn finish_refused_session(&mut self) -> TestResult {
        self.survive(1).await?;
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
}

fn only_flows(frames: &[Frame]) {
    assert!(
        frames.iter().all(|frame| matches!(
            frame,
            Frame::Amqp {
                performative: Some(Performative::Flow(_)),
                ..
            }
        )),
        "no disposition, link traffic, End, or Close may precede the barrier: {frames:?}"
    );
}

fn disposition(role: Role, first: u32, last: Option<u32>) -> Disposition {
    let state = (role == Role::Receiver).then_some(DeliveryState::Accepted(Accepted));
    Disposition {
        role,
        first,
        last,
        settled: true,
        state,
        batchable: false,
    }
}

async fn errant_end(peer: &mut Peer, channels: Channels) -> TestResult {
    let frame = peer.read().await?;
    let Frame::Amqp {
        channel,
        performative: Some(Performative::End(end)),
        payload,
    } = &frame
    else {
        panic!("errored delivery disposition must emit immediate session End: {frame:?}")
    };
    assert_eq!(*channel, channels.outgoing);
    assert!(payload.is_empty());
    assert_eq!(
        end.error
            .as_ref()
            .expect("errant-link refusal")
            .condition
            .as_symbol()
            .as_str(),
        "amqp:session:errant-link"
    );
    Ok(())
}

async fn read_outgoing(peer: &mut Peer, link: Link) -> TestResult<Transfer> {
    loop {
        match peer.read().await? {
            Frame::Amqp {
                channel,
                performative: Some(Performative::Transfer(transfer)),
                ..
            } => {
                assert_eq!(channel, link.channels.outgoing);
                assert_eq!(transfer.handle, link.local);
                assert!(!transfer.more);
                assert!(!transfer.settled.unwrap_or(false));
                return Ok(transfer);
            }
            Frame::Amqp {
                performative: Some(Performative::Flow(_)),
                ..
            } => {}
            frame => panic!("expected complete mapped outgoing delivery: {frame:?}"),
        }
    }
}

async fn observe_outcome(
    sender: &mut Sending,
    message: Message,
    tag: u8,
) -> Result<Outcome, EngineError> {
    match sender {
        Sending::Server(sender) => sender
            .send_with_settlement(message, vec![tag].into())
            .await
            .map(|receipt| receipt.outcome().clone()),
        Sending::Client(sender) => sender.send(message).await,
    }
}

async fn opposite_outgoing_zero(node: &mut Node, sender: &mut Sending, link: Link) -> TestResult {
    let observed = observe_outcome(sender, Message::data(vec![45; 8]), 3);
    tokio::pin!(observed);
    let outgoing = tokio::select! {
        biased;
        result = &mut observed => panic!("opposite delivery must await its disposition: {result:?}"),
        transfer = read_outgoing(&mut node.peer, link) => transfer?,
    };
    assert_eq!(outgoing.delivery_id, Some(0));
    node.peer
        .send(
            link.channels.incoming,
            Performative::Disposition(disposition(Role::Receiver, 0, None)),
            Vec::new(),
        )
        .await?;
    assert_eq!(
        timeout(IO_TIMEOUT, observed).await??,
        Outcome::Accepted(Accepted)
    );
    Ok(())
}

async fn outgoing_error_sender(fixture: &mut HistoryFixture) -> TestResult<Sending> {
    let Session::Client(session) = &mut fixture.session else {
        return fixture
            .node
            .sender(&mut fixture.session, fixture.errored)
            .await;
    };
    let link = fixture.errored;
    let (sender, wire) = tokio::join!(
        timeout(
            IO_TIMEOUT,
            session.attach_sender("bounded-error-sender", "queue")
        ),
        async {
            let request = fixture.node.peer.own_attach(link, Role::Sender).await?;
            let mut response = request.response(request.source.clone(), request.target.clone());
            response.handle = link.peer;
            response.max_message_size = Some(64);
            fixture
                .node
                .peer
                .send(
                    link.channels.incoming,
                    Performative::Attach(Box::new(response)),
                    Vec::new(),
                )
                .await?;
            let mut flow = fixture.node.peer.flow(link.channels);
            flow.handle = Some(link.peer);
            flow.delivery_count = Some(0);
            flow.link_credit = Some(2);
            fixture
                .node
                .peer
                .send(link.channels.incoming, Performative::Flow(flow), Vec::new())
                .await
        }
    );
    wire?;
    Ok(Sending::Client(sender??))
}

async fn error_outgoing(fixture: &mut HistoryFixture, sender: &mut Sending) -> TestResult {
    {
        let abandoned = observe_outcome(sender, Message::data(vec![42; 8]), 1);
        tokio::pin!(abandoned);
        let transfer = tokio::select! {
            biased;
            result = &mut abandoned => panic!("first delivery must await its held disposition: {result:?}"),
            transfer = read_outgoing(&mut fixture.node.peer, fixture.errored) => transfer?,
        };
        assert_eq!(transfer.delivery_id, Some(0));
    }
    let condition = match sender {
        Sending::Server(sender) => {
            timeout(
                IO_TIMEOUT,
                sender.close_with_error(Error::new(
                    AmqpError::NotImplemented,
                    "error delivery history test",
                    None,
                )),
            )
            .await??;
            "amqp:not-implemented"
        }
        Sending::Client(sender) => {
            assert!(matches!(
                timeout(IO_TIMEOUT, sender.send(Message::data(vec![43; 128]))).await?,
                Err(EngineError::MessageSizeExceeded {
                    maximum_bytes: 64,
                    ..
                })
            ));
            "amqp:link:message-size-exceeded"
        }
    };
    fixture
        .node
        .peer
        .error_detach(fixture.errored, condition)
        .await
}

async fn incoming_history_survives_lifecycle(server: bool, after_ack: bool) -> TestResult {
    let mut fixture = HistoryFixture::new(server).await?;
    let mut errored = fixture
        .node
        .receiver(&mut fixture.session, fixture.errored)
        .await?;
    let mut healthy = fixture
        .node
        .receiver(&mut fixture.session, fixture.healthy)
        .await?;
    fixture
        .node
        .peer
        .send(
            fixture.main.incoming,
            Performative::Transfer(transfer(fixture.healthy.peer, u32::MAX)),
            encode_message(&Message::data(vec![44]))?,
        )
        .await?;
    let healthy_delivery = timeout(IO_TIMEOUT, healthy.recv()).await??;
    let credit = fixture.node.peer.read().await?;
    assert!(
        matches!(&credit, Frame::Amqp {
        channel, performative: Some(Performative::Flow(flow)), payload,
    } if *channel == fixture.main.outgoing
        && flow.handle == Some(fixture.healthy.local)
        && flow.delivery_count == Some(1)
        && flow.link_credit == Some(32)
        && flow.next_incoming_id == Some(1)
        && payload.is_empty()),
        "healthy consumption must publish its exact credit before the error: {credit:?}"
    );
    fixture.error_partial().await?;
    assert!(matches!(
        timeout(IO_TIMEOUT, errored.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));
    fixture
        .node
        .incoming_message(&mut healthy, fixture.healthy, 1, 51)
        .await?;
    if after_ack {
        fixture.ack_error().await?;
        let replacement = Link {
            channels: fixture.main,
            peer: 56,
            local: 0,
        };
        let mut opposite = fixture
            .node
            .sender(&mut fixture.session, replacement)
            .await?;
        opposite_outgoing_zero(&mut fixture.node, &mut opposite, replacement).await?;
    } else {
        fixture
            .node
            .peer
            .send(
                fixture.main.incoming,
                Performative::Disposition(disposition(Role::Receiver, 0, None)),
                Vec::new(),
            )
            .await?;
    }
    fixture.survive(0).await?;
    fixture.drain().await?;
    fixture
        .node
        .peer
        .send(
            fixture.main.incoming,
            Performative::Disposition(disposition(Role::Sender, u32::MAX, Some(0))),
            Vec::new(),
        )
        .await?;
    errant_end(&mut fixture.node.peer, fixture.main).await?;
    assert!(
        timeout(IO_TIMEOUT, healthy.accept(&healthy_delivery))
            .await?
            .is_err()
    );
    assert!(matches!(
        timeout(IO_TIMEOUT, healthy.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));
    fixture.finish_refused_session().await
}

async fn outgoing_history_survives_lifecycle(server: bool, after_ack: bool) -> TestResult {
    let mut fixture = HistoryFixture::new(server).await?;
    let mut errored = outgoing_error_sender(&mut fixture).await?;
    let mut healthy = fixture
        .node
        .sender(&mut fixture.session, fixture.healthy)
        .await?;
    error_outgoing(&mut fixture, &mut errored).await?;
    let waiting = observe_outcome(&mut healthy, Message::data(vec![46; 8]), 2);
    tokio::pin!(waiting);
    let healthy_transfer = tokio::select! {
        biased;
        result = &mut waiting => panic!("healthy delivery must await its held disposition: {result:?}"),
        transfer = read_outgoing(&mut fixture.node.peer, fixture.healthy) => transfer?,
    };
    assert_eq!(healthy_transfer.delivery_id, Some(1));
    if after_ack {
        fixture.ack_error().await?;
        let replacement = Link {
            channels: fixture.main,
            peer: 56,
            local: 0,
        };
        let mut opposite = fixture
            .node
            .receiver(&mut fixture.session, replacement)
            .await?;
        fixture
            .node
            .incoming_message(&mut opposite, replacement, 0, 47)
            .await?;
    } else {
        fixture
            .node
            .peer
            .send(
                fixture.main.incoming,
                Performative::Disposition(disposition(Role::Sender, 0, None)),
                Vec::new(),
            )
            .await?;
    }
    fixture.survive(0).await?;
    fixture.drain().await?;
    fixture
        .node
        .peer
        .send(
            fixture.main.incoming,
            Performative::Disposition(disposition(Role::Receiver, u32::MAX, Some(1))),
            Vec::new(),
        )
        .await?;
    errant_end(&mut fixture.node.peer, fixture.main).await?;
    assert!(
        matches!(
            timeout(IO_TIMEOUT, &mut waiting).await?,
            Err(EngineError::RemoteDetached)
        ),
        "mixed range must not release a healthy Accepted outcome before refusing its session"
    );
    fixture.finish_refused_session().await
}

async fn incoming_reassignment_clears_only_after_valid_ownership(
    server: bool,
    valid: bool,
) -> TestResult {
    let mut fixture = HistoryFixture::new(server).await?;
    let _errored = fixture
        .node
        .receiver(&mut fixture.session, fixture.errored)
        .await?;
    let _healthy = fixture
        .node
        .receiver(&mut fixture.session, fixture.healthy)
        .await?;
    fixture.error_partial().await?;
    fixture.ack_error().await?;
    let replacement_link = Link {
        channels: fixture.main,
        peer: 56,
        local: 0,
    };
    let mut replacement = fixture
        .node
        .receiver(&mut fixture.session, replacement_link)
        .await?;
    if valid {
        fixture
            .node
            .incoming_message(&mut replacement, replacement_link, 0, 48)
            .await?;
    } else {
        let mut invalid = transfer(replacement_link.peer, 0);
        invalid.delivery_tag = Some(vec![49; 33].into());
        fixture
            .node
            .peer
            .send(
                fixture.main.incoming,
                Performative::Transfer(invalid),
                encode_message(&Message::data(vec![49]))?,
            )
            .await?;
        fixture
            .node
            .peer
            .error_detach(replacement_link, "amqp:invalid-field")
            .await?;
    }
    fixture.survive(0).await?;
    fixture.drain().await?;
    fixture
        .node
        .peer
        .send(
            fixture.main.incoming,
            Performative::Disposition(disposition(Role::Sender, 0, None)),
            Vec::new(),
        )
        .await?;
    if valid {
        only_flows(&fixture.node.peer.barrier(fixture.main).await?);
        fixture
            .node
            .incoming_message(&mut replacement, replacement_link, 1, 50)
            .await?;
        fixture.survive(1).await?;
        fixture.node.shutdown().await;
        Ok(())
    } else {
        errant_end(&mut fixture.node.peer, fixture.main).await?;
        assert!(matches!(
            timeout(IO_TIMEOUT, replacement.recv()).await?,
            Err(EngineError::RemoteDetached)
        ));
        fixture.finish_refused_session().await
    }
}

macro_rules! history_case {
    ($name:ident, $case:expr) => {
        #[tokio::test]
        async fn $name() -> TestResult {
            timeout(CASE_TIMEOUT, $case).await?
        }
    };
}

history_case!(
    server_incoming_error_id_rejects_mixed_wrapping_disposition_before_detach_ack,
    incoming_history_survives_lifecycle(true, false)
);
history_case!(
    client_incoming_error_id_rejects_mixed_wrapping_disposition_before_detach_ack,
    incoming_history_survives_lifecycle(false, false)
);
history_case!(
    server_incoming_error_id_survives_detach_ack_and_opposite_role_handle_reuse,
    incoming_history_survives_lifecycle(true, true)
);
history_case!(
    client_incoming_error_id_survives_detach_ack_and_opposite_role_handle_reuse,
    incoming_history_survives_lifecycle(false, true)
);
history_case!(
    server_outgoing_error_id_rejects_mixed_wrapping_disposition_before_detach_ack,
    outgoing_history_survives_lifecycle(true, false)
);
history_case!(
    client_outgoing_error_id_rejects_mixed_wrapping_disposition_before_detach_ack,
    outgoing_history_survives_lifecycle(false, false)
);
history_case!(
    server_outgoing_error_id_survives_detach_ack_and_opposite_role_handle_reuse,
    outgoing_history_survives_lifecycle(true, true)
);
history_case!(
    client_outgoing_error_id_survives_detach_ack_and_opposite_role_handle_reuse,
    outgoing_history_survives_lifecycle(false, true)
);
history_case!(
    server_valid_incoming_reassignment_replaces_the_old_numeric_error_id,
    incoming_reassignment_clears_only_after_valid_ownership(true, true)
);
history_case!(
    client_valid_incoming_reassignment_replaces_the_old_numeric_error_id,
    incoming_reassignment_clears_only_after_valid_ownership(false, true)
);
history_case!(
    server_failed_incoming_reassignment_keeps_the_old_error_id,
    incoming_reassignment_clears_only_after_valid_ownership(true, false)
);
history_case!(
    client_failed_incoming_reassignment_keeps_the_old_error_id,
    incoming_reassignment_clears_only_after_valid_ownership(false, false)
);
