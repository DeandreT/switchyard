use amqp::{End, EngineError, Frame, Message, Performative, encode_message};
use tokio::time::timeout;

use super::{
    fixture::{
        CASE_TIMEOUT, Channels, IO_TIMEOUT, Link, Node, Peer, Receiving, Session, TestResult,
        channels, detach, transfer,
    },
    only_session_flow,
};

struct ClosingFixture {
    node: Node,
    main_session: Session,
    _other_session: Session,
    closing: Receiving,
    same_session: Receiving,
    other_session: Receiving,
    closing_link: Link,
    same_session_link: Link,
    other_session_link: Link,
}

impl ClosingFixture {
    async fn new(server: bool) -> TestResult<Self> {
        let mut node = Node::new(server).await?;
        let main = channels(server, 0);
        let other = channels(server, 1);
        let mut main_session = node.session(main, 1).await?;
        let mut other_session = node.session(other, 0).await?;
        let closing_link = Link {
            channels: main,
            peer: 55,
            local: 0,
        };
        let same_session_link = Link {
            channels: main,
            peer: 0,
            local: 1,
        };
        let other_session_link = Link {
            channels: other,
            peer: 77,
            local: 0,
        };
        let closing = node.receiver(&mut main_session, closing_link).await?;
        let same_session = node.receiver(&mut main_session, same_session_link).await?;
        let other_receiver = node
            .receiver(&mut other_session, other_session_link)
            .await?;
        Ok(Self {
            node,
            main_session,
            _other_session: other_session,
            closing,
            same_session,
            other_session: other_receiver,
            closing_link,
            same_session_link,
            other_session_link,
        })
    }

    async fn error_close(&mut self) -> TestResult {
        let mut unsupported = transfer(self.closing_link.peer, 0);
        unsupported.message_format = Some(999);
        self.node
            .peer
            .send(
                self.closing_link.channels.incoming,
                Performative::Transfer(unsupported),
                Vec::new(),
            )
            .await?;
        self.node
            .peer
            .error_detach(self.closing_link, "amqp:not-implemented")
            .await?;
        assert!(matches!(
            timeout(IO_TIMEOUT, self.closing.recv()).await?,
            Err(EngineError::RemoteDetached)
        ));
        Ok(())
    }

    async fn healthy_siblings_before_late_traffic(&mut self) -> TestResult {
        self.node
            .incoming_message(&mut self.same_session, self.same_session_link, 1, 23)
            .await?;
        self.node
            .incoming_message(&mut self.other_session, self.other_session_link, 0, 24)
            .await?;
        for channels in [self.closing_link.channels, self.other_session_link.channels] {
            assert_healthy_flows(
                &self.node.peer.barrier(channels).await?,
                &[self.same_session_link, self.other_session_link],
            );
        }
        Ok(())
    }
}

fn assert_healthy_flows(frames: &[Frame], links: &[Link]) {
    assert!(
        frames.iter().all(
            |frame| links.iter().any(|link| matches!(frame, Frame::Amqp {
            channel, performative: Some(Performative::Flow(flow)), ..
        } if *channel == link.channels.outgoing
            && (flow.handle.is_none() || flow.handle == Some(link.local))))
        ),
        "only healthy mapped links or their sessions may emit Flow: {frames:?}"
    );
}

async fn errant_end(peer: &mut Peer, channels: Channels) -> TestResult {
    let frame = peer.read().await?;
    let Frame::Amqp {
        channel,
        performative: Some(Performative::End(end)),
        payload,
    } = &frame
    else {
        panic!("late error-link traffic must emit immediate session End: {frame:?}")
    };
    assert_eq!(*channel, channels.outgoing);
    assert!(payload.is_empty());
    assert_eq!(
        end.error
            .as_ref()
            .expect("errant-link session refusal")
            .condition
            .as_symbol()
            .as_str(),
        "amqp:session:errant-link"
    );
    Ok(())
}

#[derive(Clone, Copy)]
enum LateTraffic {
    Flow,
    Transfer,
}

async fn late_error_link_traffic_ends_only_its_session(
    server: bool,
    traffic: LateTraffic,
) -> TestResult {
    let mut fixture = ClosingFixture::new(server).await?;
    fixture.error_close().await?;
    fixture.healthy_siblings_before_late_traffic().await?;
    let main = fixture.closing_link.channels;
    match traffic {
        LateTraffic::Flow => {
            let mut flow = fixture.node.peer.flow(main);
            flow.handle = Some(fixture.closing_link.peer);
            flow.delivery_count = Some(0);
            flow.echo = true;
            fixture
                .node
                .peer
                .send(main.incoming, Performative::Flow(flow), Vec::new())
                .await?;
        }
        LateTraffic::Transfer => {
            fixture
                .node
                .peer
                .send(
                    main.incoming,
                    Performative::Transfer(transfer(fixture.closing_link.peer, 2)),
                    encode_message(&Message::data(vec![25]))?,
                )
                .await?;
        }
    }
    errant_end(&mut fixture.node.peer, main).await?;
    assert!(matches!(
        timeout(IO_TIMEOUT, fixture.same_session.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));
    fixture
        .node
        .incoming_message(
            &mut fixture.other_session,
            fixture.other_session_link,
            1,
            26,
        )
        .await?;
    fixture
        .node
        .peer
        .send(main.incoming, Performative::End(End::default()), Vec::new())
        .await?;
    assert_healthy_flows(
        &fixture
            .node
            .peer
            .barrier(fixture.other_session_link.channels)
            .await?,
        &[fixture.other_session_link],
    );
    fixture.node.shutdown().await;
    Ok(())
}

async fn mapped_error_detach_ack_releases_only_its_alias(server: bool) -> TestResult {
    let mut fixture = ClosingFixture::new(server).await?;
    fixture.error_close().await?;
    fixture.healthy_siblings_before_late_traffic().await?;
    let main = fixture.closing_link.channels;
    fixture
        .node
        .peer
        .send(
            main.incoming,
            Performative::Detach(detach(fixture.closing_link.peer)),
            Vec::new(),
        )
        .await?;
    assert_healthy_flows(
        &fixture.node.peer.barrier(main).await?,
        &[fixture.same_session_link, fixture.other_session_link],
    );
    let replacement_link = Link {
        channels: main,
        peer: 56,
        local: 0,
    };
    let mut replacement = fixture
        .node
        .receiver(&mut fixture.main_session, replacement_link)
        .await?;
    fixture
        .node
        .incoming_message(&mut replacement, replacement_link, 2, 27)
        .await?;
    fixture
        .node
        .incoming_message(&mut fixture.same_session, fixture.same_session_link, 3, 28)
        .await?;
    fixture
        .node
        .incoming_message(
            &mut fixture.other_session,
            fixture.other_session_link,
            1,
            29,
        )
        .await?;
    fixture.node.shutdown().await;
    Ok(())
}

async fn normal_close_discards_crossing_flow_and_transfer(server: bool) -> TestResult {
    let ClosingFixture {
        mut node,
        main_session: _main_session,
        _other_session,
        mut closing,
        mut same_session,
        mut other_session,
        closing_link,
        same_session_link,
        other_session_link,
    } = ClosingFixture::new(server).await?;
    let main = closing_link.channels;
    let (result, wire) = tokio::join!(timeout(IO_TIMEOUT, closing.close()), async {
        node.peer.detach(closing_link).await?;
        let mut flow = node.peer.flow(main);
        flow.handle = Some(closing_link.peer);
        flow.delivery_count = Some(0);
        flow.echo = true;
        node.peer
            .send(main.incoming, Performative::Flow(flow), Vec::new())
            .await?;
        node.peer
            .send(
                main.incoming,
                Performative::Transfer(transfer(closing_link.peer, 0)),
                encode_message(&Message::data(vec![30]))?,
            )
            .await?;
        only_session_flow(&node.peer.barrier(main).await?, main.outgoing);
        node.incoming_message(&mut same_session, same_session_link, 1, 31)
            .await?;
        node.incoming_message(&mut other_session, other_session_link, 0, 32)
            .await?;
        node.peer
            .send(
                main.incoming,
                Performative::Detach(detach(closing_link.peer)),
                Vec::new(),
            )
            .await?;
        assert_healthy_flows(
            &node.peer.barrier(main).await?,
            &[same_session_link, other_session_link],
        );
        Ok::<_, Box<dyn std::error::Error>>(())
    });
    wire?;
    result??;
    assert!(matches!(
        timeout(IO_TIMEOUT, closing.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));
    node.incoming_message(&mut same_session, same_session_link, 2, 33)
        .await?;
    node.incoming_message(&mut other_session, other_session_link, 1, 34)
        .await?;
    node.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn server_late_flow_on_exact_error_closed_handle_ends_only_its_session() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        late_error_link_traffic_ends_only_its_session(true, LateTraffic::Flow),
    )
    .await?
}

#[tokio::test]
async fn client_late_flow_on_exact_error_closed_handle_ends_only_its_session() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        late_error_link_traffic_ends_only_its_session(false, LateTraffic::Flow),
    )
    .await?
}

#[tokio::test]
async fn server_late_transfer_on_exact_error_closed_handle_ends_only_its_session() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        late_error_link_traffic_ends_only_its_session(true, LateTraffic::Transfer),
    )
    .await?
}

#[tokio::test]
async fn client_late_transfer_on_exact_error_closed_handle_ends_only_its_session() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        late_error_link_traffic_ends_only_its_session(false, LateTraffic::Transfer),
    )
    .await?
}

#[tokio::test]
async fn server_error_detach_ack_uses_peer_handle_and_reuses_only_assigned_output() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        mapped_error_detach_ack_releases_only_its_alias(true),
    )
    .await?
}

#[tokio::test]
async fn client_error_detach_ack_uses_peer_handle_and_reuses_only_assigned_output() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        mapped_error_detach_ack_releases_only_its_alias(false),
    )
    .await?
}

#[tokio::test]
async fn server_normal_closing_handle_discards_crossing_flow_and_transfer() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        normal_close_discards_crossing_flow_and_transfer(true),
    )
    .await?
}

#[tokio::test]
async fn client_normal_closing_handle_discards_crossing_flow_and_transfer() -> TestResult {
    timeout(
        CASE_TIMEOUT,
        normal_close_discards_crossing_flow_and_transfer(false),
    )
    .await?
}
