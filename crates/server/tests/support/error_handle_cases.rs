use amqp::{EngineError, Message, Performative, encode_message};
use tokio::time::timeout;

use super::{
    error_name_cases::{ERROR_NAME, ErrorFixture, deliver, named_request, strict_end},
    fixture::{CASE_TIMEOUT, IO_TIMEOUT, TestResult, transfer},
};

#[derive(Clone, Copy)]
enum LateTraffic {
    Flow,
    Transfer,
}

async fn old_peer_handle_survives_output_only_reuse(
    server: bool,
    traffic: LateTraffic,
) -> TestResult {
    let mut fixture = ErrorFixture::new(server).await?;
    fixture.error().await?;
    fixture.ack().await?;
    fixture.prove_siblings().await?;
    let replacement_link = fixture.replacement(56);
    let mut replacement = fixture
        .node
        .receiver_named(
            &mut fixture.session,
            replacement_link,
            "different-peer-same-output",
        )
        .await?;
    deliver(&mut fixture.node, &mut replacement, replacement_link, 2, 1).await?;
    match traffic {
        LateTraffic::Flow => {
            let mut flow = fixture.node.peer.flow(fixture.main);
            flow.handle = Some(fixture.error_link.peer);
            flow.delivery_count = Some(0);
            flow.echo = true;
            fixture
                .node
                .peer
                .send(fixture.main.incoming, Performative::Flow(flow), Vec::new())
                .await?;
        }
        LateTraffic::Transfer => {
            fixture
                .node
                .peer
                .send(
                    fixture.main.incoming,
                    Performative::Transfer(transfer(fixture.error_link.peer, 3)),
                    encode_message(&Message::data(vec![62]))?,
                )
                .await?;
        }
    }
    strict_end(
        &mut fixture.node,
        fixture.main,
        Some("amqp:session:errant-link"),
    )
    .await?;
    assert!(matches!(
        timeout(IO_TIMEOUT, replacement.recv()).await?,
        Err(EngineError::RemoteDetached)
    ));
    fixture.finish_ended().await
}

async fn valid_peer_rebind_keeps_occupied_normal_close_priority(server: bool) -> TestResult {
    let mut fixture = ErrorFixture::new(server).await?;
    fixture.error().await?;
    fixture.ack().await?;
    fixture.prove_siblings().await?;
    let replacement_link = fixture.replacement(fixture.error_link.peer);
    let mut replacement = fixture
        .node
        .receiver_named(
            &mut fixture.session,
            replacement_link,
            "fresh-binding-replaces-error-handle",
        )
        .await?;
    deliver(&mut fixture.node, &mut replacement, replacement_link, 2, 1).await?;
    let mut flow = fixture.node.peer.flow(fixture.main);
    flow.handle = Some(replacement_link.peer);
    flow.delivery_count = Some(1);
    flow.echo = true;
    fixture
        .node
        .peer
        .send(fixture.main.incoming, Performative::Flow(flow), Vec::new())
        .await?;
    let frames = fixture.node.peer.barrier(fixture.main).await?;
    assert!(
        frames
            .iter()
            .any(|frame| matches!(frame, amqp::Frame::Amqp {
        channel, performative: Some(Performative::Flow(flow)), ..
    } if *channel == fixture.main.outgoing && flow.handle == Some(replacement_link.local)))
    );
    assert!(
        frames
            .iter()
            .all(|frame| matches!(frame, amqp::Frame::Amqp {
        channel, performative: Some(Performative::Flow(flow)), ..
    } if *channel == fixture.main.outgoing
        && (flow.handle.is_none() || flow.handle == Some(replacement_link.local))))
    );
    fixture
        .request(named_request(replacement_link, ERROR_NAME, false))
        .await?;
    fixture
        .node
        .peer
        .close("amqp:session:handle-in-use")
        .await?;
    for receiver in [
        &mut replacement,
        &mut fixture.healthy,
        &mut fixture.survivor,
    ] {
        assert!(matches!(
            timeout(IO_TIMEOUT, receiver.recv()).await?,
            Err(EngineError::RemoteDetached)
        ));
    }
    fixture.node.peer.acknowledge_close_and_expect_eof().await?;
    fixture.node.shutdown().await;
    Ok(())
}

macro_rules! handle_case {
    ($name:ident, $case:expr) => {
        #[tokio::test]
        async fn $name() -> TestResult {
            timeout(CASE_TIMEOUT, $case).await?
        }
    };
}

handle_case!(
    server_historical_peer_handle_flow_is_errant_after_ack_and_output_reuse,
    old_peer_handle_survives_output_only_reuse(true, LateTraffic::Flow)
);
handle_case!(
    client_historical_peer_handle_flow_is_errant_after_ack_and_output_reuse,
    old_peer_handle_survives_output_only_reuse(false, LateTraffic::Flow)
);
handle_case!(
    server_historical_peer_handle_transfer_is_errant_after_ack_and_output_reuse,
    old_peer_handle_survives_output_only_reuse(true, LateTraffic::Transfer)
);
handle_case!(
    client_historical_peer_handle_transfer_is_errant_after_ack_and_output_reuse,
    old_peer_handle_survives_output_only_reuse(false, LateTraffic::Transfer)
);
handle_case!(
    server_fresh_peer_binding_replaces_numeric_marker_but_normal_alias_close_wins,
    valid_peer_rebind_keeps_occupied_normal_close_priority(true)
);
handle_case!(
    client_fresh_peer_binding_replaces_numeric_marker_but_normal_alias_close_wins,
    valid_peer_rebind_keeps_occupied_normal_close_priority(false)
);
