use super::*;
use futures_util::{SinkExt, StreamExt};
use tokio::io::DuplexStream;
use tokio_tungstenite::{
    WebSocketStream, client_async,
    tungstenite::{Message, client::IntoClientRequest},
};

type Client = WebSocketStream<DuplexStream>;

async fn binary(client: &mut Client) -> TestResult<Vec<u8>> {
    match client.next().await {
        Some(Ok(Message::Binary(bytes))) => Ok(bytes.to_vec()),
        Some(Err(error)) => Err(error.into()),
        _ => Err(io::Error::other("missing binary fixture message").into()),
    }
}

async fn open_websocket(h: &mut Harness) -> TestResult<Client> {
    let peer = h
        .peer
        .take()
        .ok_or_else(|| io::Error::other("missing WebSocket peer"))?;
    let mut request = "ws://localhost/$servicebus/websocket/".into_client_request()?;
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "amqp".parse()?);
    let (mut client, _) = client_async(request, peer).await?;
    client
        .send(Message::Binary(amqp::AMQP_HEADER.to_vec().into()))
        .await?;
    if binary(&mut client).await? != amqp::AMQP_HEADER {
        return Err(io::Error::other("unexpected WebSocket protocol header").into());
    }
    client
        .send(Message::Binary(
            amqp::encode_frame(&amqp::Frame::Amqp {
                channel: 0,
                performative: Some(amqp::Performative::Open(amqp::Open::new("peer"))),
                payload: Vec::new(),
            })?
            .into(),
        ))
        .await?;
    let frame_bytes = binary(&mut client).await?;
    let mut frame_bytes = frame_bytes.as_slice();
    if !matches!(
        amqp::read_frame(&mut frame_bytes).await?,
        amqp::Frame::Amqp {
            channel: 0,
            performative: Some(amqp::Performative::Open(_)),
            ..
        }
    ) {
        return Err(io::Error::other("unexpected WebSocket Open").into());
    }
    client
        .send(Message::Binary(
            amqp::encode_frame(&amqp::Frame::Amqp {
                channel: 0,
                performative: Some(amqp::Performative::Begin(amqp::Begin {
                    remote_channel: None,
                    next_outgoing_id: 0,
                    incoming_window: 16,
                    outgoing_window: 16,
                    handle_max: u32::MAX,
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                })),
                payload: Vec::new(),
            })?
            .into(),
        ))
        .await?;
    Ok(client)
}

#[derive(Clone, Copy)]
enum Fault {
    AbortPending,
    UnwindPending,
    UnwindReady,
    None,
}

async fn close_custody(fault: Fault) -> TestResult {
    let mut h = Harness::new((), TestDriver, true);
    h.wait_begin();
    h.io.shutdown_gate.blocked.store(true, Ordering::SeqCst);
    let primary = Arc::new(PayloadWitness::default());
    let closed = Arc::new(PayloadWitness::default());
    let panic = Arc::new(PayloadWitness::default());
    h.error(primary.clone());
    *crate::listener::retained_connection::locked(&h.io.shutdown_error) = Some(closed.clone());
    let mut setup = bounded(open_websocket(&mut h)).await;
    let exchange = bounded(async {
        if let Ok(Ok(client)) = &mut setup {
            let message = client
                .next()
                .await
                .ok_or_else(|| io::Error::other("missing WebSocket Close"))??;
            if !matches!(message, Message::Close(_)) {
                return Err(io::Error::other("unexpected WebSocket close message").into());
            }
            client.flush().await?;
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await;
    let pending = bounded(async {
        while !h.io.shutdown_gate.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let observed = bounded(h.controls.wait_for(|m| m.close_pending)).await;
    let before = (
        primary.drops.load(Ordering::SeqCst),
        closed.drops.load(Ordering::SeqCst),
    );
    let published_pending = h.owner.published();
    let finish_observation = {
        let mut borrowed = Box::pin(h.owner.finish());
        let observed = tokio::time::timeout(Duration::from_millis(20), &mut borrowed).await;
        drop(borrowed);
        observed
    };
    match fault {
        Fault::AbortPending => h.owner.abort_wrapper(),
        Fault::UnwindPending => {
            h.controls.panic_at(
                Site::ClosePending,
                Box::new(PayloadError {
                    witness: panic.clone(),
                    name: "private-real-close-pending-panic",
                }),
            );
            // Reschedule without releasing: the REAL close future returns Pending again.
            h.io.shutdown_gate.pulse();
        }
        Fault::UnwindReady => {
            h.controls.panic_at(
                Site::CloseReady,
                Box::new(PayloadError {
                    witness: panic.clone(),
                    name: "private-real-close-ready-panic",
                }),
            );
            h.io.shutdown_gate.release();
        }
        Fault::None => h.io.shutdown_gate.release(),
    }
    // For Pending unwind, keep the real Pending condition until the underlying
    // Io destructor is entered. Actual task joins still occur in finish below.
    let completion = if matches!(fault, Fault::UnwindPending) {
        Some(
            bounded(async {
                while h.io.dropped.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await,
        )
    } else {
        None
    };
    let report = h.finish().await.expect("all socket role barriers");
    setup
        .as_ref()
        .map_err(|_| io::Error::other("HTTP setup observation expired"))?
        .as_ref()
        .map_err(|_| io::Error::other("original WebSocket setup failed"))?;
    exchange??;
    pending?;
    observed?;
    if let Some(completion) = completion {
        completion?;
    }
    assert!(finish_observation.is_err());
    assert_eq!(before, (0, 0));
    assert_eq!(published_pending, (true, false));
    assert!(matches!(report.actor(), Some(Ok(()))));
    assert!(normal_reader_join(report.reader()));
    assert!(
        primary_error(&report)
            .and_then(|e| e.downcast_ref::<PayloadError>())
            .is_some_and(|e| Arc::ptr_eq(&e.witness, &primary))
    );
    match fault {
        Fault::AbortPending | Fault::UnwindPending => {
            assert!(report.outcomes().websocket_close.is_none());
            assert!(report.wrapper().is_some_and(|r| r.as_ref().is_err_and(|e| {
                if matches!(fault, Fault::AbortPending) {
                    e.is_cancelled()
                } else {
                    e.is_panic()
                }
            })));
        }
        Fault::UnwindReady | Fault::None => {
            let error = report
                .outcomes()
                .websocket_close
                .as_ref()
                .and_then(|r| r.as_ref().err())
                .and_then(|e| e.downcast_ref::<io::Error>());
            assert!(
                error
                    .and_then(io::Error::get_ref)
                    .and_then(|e| e.downcast_ref::<PayloadError>())
                    .is_some_and(|e| Arc::ptr_eq(&e.witness, &closed))
            );
            assert!(if matches!(fault, Fault::UnwindReady) {
                report
                    .wrapper()
                    .is_some_and(|r| r.as_ref().is_err_and(|e| e.is_panic()))
            } else {
                matches!(report.wrapper(), Some(Ok(())))
            });
        }
    }
    // Setup's client and any original setup/observation errors stay owned until
    // all three actual barriers, not just the Close/actor-exit notifications.
    drop(setup);
    drop(report);
    drop(finish_observation);
    assert_eq!(primary.drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        closed.drops.load(Ordering::SeqCst),
        usize::from(matches!(fault, Fault::UnwindReady | Fault::None))
    );
    assert_eq!(
        panic.drops.load(Ordering::SeqCst),
        usize::from(matches!(fault, Fault::UnwindPending | Fault::UnwindReady))
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrapper_abort_retains_ready_primary_while_real_websocket_close_is_pending() -> TestResult {
    close_custody(Fault::AbortPending).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrapper_unwind_retains_ready_primary_while_real_websocket_close_is_pending() -> TestResult
{
    close_custody(Fault::UnwindPending).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_primary_and_close_errors_remain_independent_until_all_socket_joins() -> TestResult
{
    close_custody(Fault::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_close_ready_error_is_rooted_before_later_wrapper_unwind() -> TestResult {
    close_custody(Fault::UnwindReady).await
}
