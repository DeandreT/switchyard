use super::{super::*, fixture::*};

async fn workflow<const MESSAGING: bool>(websocket: bool) -> TestResult {
    let mut fixture = Fixture::with_anchor::<MESSAGING>(4, (), websocket).await?;
    let setup = if websocket {
        fixture.websocket().await
    } else {
        Ok(())
    };
    let opened = if setup.is_ok() {
        fixture.hello().await
    } else {
        Err("setup was not completed".into())
    };
    let mut observations = Vec::new();
    if opened.is_ok() {
        observations.push(fixture.begin(0).await);
        observations.push(fixture.begin(1).await);
        if observations.iter().all(Result::is_ok) {
            observations.push(
                fixture
                    .attached(0, if MESSAGING { consumer(0) } else { producer(0) })
                    .await,
            );
            observations.push(fixture.attached(1, controller(0)).await);
            if observations.iter().all(Result::is_ok) {
                observations.push(
                    fixture
                        .send(CHANNELS[0], amqp::Performative::End(amqp::End::default()))
                        .await,
                );
                observations.push(
                    fixture
                        .send(CHANNELS[1], amqp::Performative::End(amqp::End::default()))
                        .await,
                );
                let controls = fixture
                    .root
                    .collector
                    .as_ref()
                    .expect("sole bound collector")
                    .scoped_controls();
                observations.push(
                    fixture
                        .drive(async move {
                            while controls.worker_stopped() < 2 {
                                tokio::task::yield_now().await;
                            }
                            Ok(())
                        })
                        .await,
                );
            }
        }
    }
    let binds = fixture.recorder.binds.clone();
    let submitted = fixture.recorder.submitted.clone();
    let cleanup = fixture.complete().await;
    let facts = evidence(&cleanup);
    let ws_close = cleanup
        .report
        .as_ref()
        .and_then(|r| r.socket.as_ref())
        .is_some_and(|s| s.outcomes().websocket_close.is_some());
    let panicked = dispose(cleanup);
    setup?;
    opened?;
    for observation in observations {
        observation?;
    }
    assert!(!panicked);
    assert_eq!(facts, (2, 2, 2, true));
    assert_eq!(binds.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(submitted.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(ws_close, websocket);
    Ok(())
}

#[tokio::test]
async fn posting_tcp_open_two_begin_one_owner_five_plus_k_original_roles() -> TestResult {
    workflow::<false>(false).await
}
#[tokio::test]
async fn messaging_tcp_open_two_begin_one_owner_five_plus_k_original_roles() -> TestResult {
    workflow::<true>(false).await
}
#[tokio::test]
async fn websocket_open_two_begin_keeps_one_wrapper_and_no_extra_pump() -> TestResult {
    workflow::<false>(true).await
}

#[tokio::test]
async fn original_open_rejection_has_no_collector_or_session_roles() -> TestResult {
    let mut fixture = Fixture::tcp::<false>(2).await?;
    let observed = {
        let Fixture { root, peer, .. } = &mut fixture;
        drive_rejection(root, peer.as_mut().expect("original peer")).await
    };
    // The accept-error branch is observed through its original fixed outcome cell.
    let published = tokio::time::timeout(DEADLINE, async {
        while !fixture.root.socket.published().0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let cleanup = fixture.complete().await;
    let absent = cleanup
        .report
        .as_ref()
        .is_some_and(|r| r.collector.is_none() && r.context.is_none());
    let original = cleanup.report.as_ref().and_then(|r| r.socket.as_ref()).is_some_and(|s| matches!(&s.outcomes().primary, Some(crate::listener::retained_connection::RetainedConnectionOutcome::Finished(Err(error))) if error.downcast_ref::<amqp::EngineError>().is_some()));
    let panicked = dispose(cleanup);
    observed?;
    published?;
    assert!(!panicked && absent && original);
    Ok(())
}

async fn drive_rejection(
    root: &mut Root<(), super::recorder::Recorder>,
    peer: &mut Peer,
) -> TestResult {
    let Peer::Tcp(peer) = peer else {
        return Err("TCP rejection fixture required".into());
    };
    // Header followed by a genuinely invalid Open minimum frame size.
    tokio::time::timeout(DEADLINE, async {
        tokio::select! {
            result = async {
                amqp::write_protocol_header(peer, amqp::ProtocolHeader::AMQP).await?;
                let _ = amqp::read_protocol_header(peer).await?;
                let mut open = amqp::Open::new("invalid-peer");
                open.max_frame_size = 64;
                amqp::write_frame(peer, &amqp::Frame::Amqp { channel:0, performative:Some(amqp::Performative::Open(open)), payload:Vec::new() }).await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            } => result,
            () = root.drive() => unreachable!("borrowed aggregate drive"),
        }
    }).await?
}
