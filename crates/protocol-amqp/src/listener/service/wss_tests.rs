//! Positive wire witnesses and explicitly qualified fault compositions.

use std::{
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind, panic_any},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

use amqp::{
    Attach, Begin, Frame, Open, Performative, ProtocolHeader, Role, Target, Transfer, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use domain::CommandKind;
use storage::StateStore;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::oneshot,
    time::timeout,
};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http::StatusCode};
use tracing::instrument::WithSubscriber;

use super::{
    test_support::{
        Actor, OBSERVER, Observer, Point, QuietBroker, WAIT, drive, wss_owner, wss_test_client_tls,
        wss_test_server_tls,
    },
    *,
};
use crate::websocket::{
    AMQP_WEBSOCKET_STANDARD_SUBPROTOCOL, AMQP_WEBSOCKET_SUBPROTOCOL, SERVICE_BUS_WEBSOCKET_PATH,
    WebSocketIo,
};

async fn pending_once<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    assert!(
        tokio::task::unconstrained(poll_fn(|context| {
            Poll::Ready(future.as_mut().poll(context))
        }))
        .await
        .is_pending()
    );
}

fn frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

async fn upgrade<S>(stream: S, address: SocketAddr, subprotocol: &str) -> WebSocketIo<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut request = format!("ws://{address}{SERVICE_BUS_WEBSOCKET_PATH}")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("sec-websocket-protocol", subprotocol.parse().unwrap());
    let (socket, response) = timeout(WAIT, tokio_tungstenite::client_async(request, stream))
        .await
        .expect("actual HTTP upgrade response")
        .unwrap();
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(response.headers()["sec-websocket-protocol"], subprotocol);
    WebSocketIo::new(socket)
}

async fn request_open<S: AsyncRead + AsyncWrite + Unpin>(peer: &mut S) {
    write_protocol_header(peer, ProtocolHeader::AMQP)
        .await
        .unwrap();
    assert_eq!(
        read_protocol_header(peer).await.unwrap(),
        ProtocolHeader::AMQP
    );
    write_frame(
        peer,
        &frame(0, Performative::Open(Open::new("wss-owner-peer"))),
    )
    .await
    .unwrap();
    peer.flush().await.unwrap();
}

async fn open<S: AsyncRead + AsyncWrite + Unpin>(peer: &mut S) {
    request_open(peer).await;
    assert!(matches!(
        read_frame(peer).await.unwrap(),
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(_)),
            ..
        }
    ));
}

async fn mechanisms<S: AsyncRead + AsyncWrite + Unpin>(peer: &mut S) {
    write_protocol_header(peer, ProtocolHeader::SASL)
        .await
        .unwrap();
    assert_eq!(
        read_protocol_header(peer).await.unwrap(),
        ProtocolHeader::SASL
    );
    assert!(matches!(
        read_frame(peer).await.unwrap(),
        Frame::Sasl(amqp::SaslPerformative::Mechanisms(_))
    ));
}

async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(peer: &mut S) {
    mechanisms(peer).await;
    write_frame(
        peer,
        &Frame::Sasl(amqp::SaslPerformative::Init(amqp::SaslInit {
            mechanism: amqp::Symbol::from("ANONYMOUS"),
            initial_response: None,
            hostname: None,
        })),
    )
    .await
    .unwrap();
    assert!(matches!(
        read_frame(peer).await.unwrap(),
        Frame::Sasl(amqp::SaslPerformative::Outcome(amqp::SaslOutcome {
            code: amqp::SaslCode::Ok,
            ..
        }))
    ));
}

fn authentication() -> crate::SharedAccessAuthentication {
    crate::SharedAccessAuthentication::new(
        auth::SharedAccessPolicy::new([]).unwrap(),
        "tenant.example",
    )
    .unwrap()
}

fn assert_success<B: Broker>(service: &mut AmqpListenerService<B>) {
    assert!(matches!(
        service.take_exit(),
        Some(AmqpListenerExit::Complete(Ok(())))
    ));
    assert!(service.take_exit().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn actual_wss_cached_admission_retains_socket_receipt_and_original_id_across_cancellation() {
    for point in [
        Point::Accepted,
        Point::Prepared,
        Point::ReceiptCached,
        Point::Adopted,
    ] {
        let broker = QuietBroker::default();
        let (mut service, address) = wss_owner(broker.clone()).await;
        assert!(matches!(service.binding, Binding::WebSocket));
        let observer = Arc::new(Observer::default());
        observer.hold(point);
        let client = TcpStream::connect(address).await.unwrap();
        drive(&mut service, Arc::clone(&observer), observer.reached(point)).await;
        let facts = observer.facts(point);
        assert!(facts.finished.is_empty());
        match point {
            Point::Accepted | Point::Prepared => {
                assert!(facts.accepted && facts.receipt.is_none() && facts.pending.is_empty());
            }
            Point::ReceiptCached => {
                assert!(!facts.accepted && facts.receipt.is_some() && facts.pending.is_empty());
            }
            Point::Adopted => {
                assert!(!facts.accepted && facts.receipt.is_none());
                assert_eq!(facts.pending.len(), 1);
            }
            Point::Reaped => unreachable!(),
        }
        observer.release();
        service.finish().await;
        if let Some(id) = facts.receipt.or_else(|| facts.pending.first().copied()) {
            assert_eq!(service.family.finished_ids(), vec![id]);
            assert!(service.family.finished()[0].joined.is_ok());
        }
        assert!(broker.calls.lock().unwrap().is_empty());
        assert_success(&mut service);
        drop(client);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_wss_parent_fault_after_receipt_cache_drains_before_resuming_original_payload() {
    let (mut service, address) = wss_owner(QuietBroker::default()).await;
    let observer = Arc::new(Observer::default());
    let raw = Arc::new("WSS parent fault after original receipt".to_owned());
    observer.panic_at(Point::ReceiptCached, Arc::clone(&raw));
    let client = TcpStream::connect(address).await.unwrap();
    OBSERVER.scope(Arc::clone(&observer), service.serve()).await;
    let id = observer.facts(Point::ReceiptCached).receipt.unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| service.take_exit())).is_err());
    service.finish().await;
    assert_eq!(service.family.finished_ids(), vec![id]);
    assert!(service.family.finished()[0].joined.is_ok());
    let payload = match catch_unwind(AssertUnwindSafe(|| service.take_exit())) {
        Err(payload) => payload,
        Ok(_) => panic!("primary panic resumes only after drain"),
    };
    assert!(Arc::ptr_eq(
        &payload.downcast::<Arc<String>>().unwrap(),
        &raw
    ));
    assert!(service.take_exit().is_none());
    drop(client);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_wss_active_retirement_keeps_primary_absent_and_drains_same_upgrade_task_once() {
    let broker = QuietBroker::default();
    let (mut service, address) = wss_owner(broker.clone()).await;
    let observer = Arc::new(Observer::default());
    let retirement = service.retirement_handle();
    let client = TcpStream::connect(address).await.unwrap();
    timeout(
        WAIT,
        OBSERVER.scope(Arc::clone(&observer), async {
            tokio::join!(service.serve(), async {
                observer.reached(Point::Adopted).await;
                observer.stalled(Stage::Upgrade).await;
                retirement.request();
            });
        }),
    )
    .await
    .expect("active WSS owner observes retirement");
    let id = observer.stalled_id(Stage::Upgrade);
    assert!(service.primary.is_none());
    assert!(service.listener.is_some(), "serve still requires finish");
    assert_eq!(service.family.pending_ids(), vec![id]);
    assert!(catch_unwind(AssertUnwindSafe(|| service.take_exit())).is_err());
    service.finish().await;
    assert_eq!(service.family.finished_ids(), vec![id]);
    assert!(service.family.finished()[0].joined.is_ok());
    assert!(broker.calls.lock().unwrap().is_empty());
    assert_success(&mut service);
    drop(client);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_wss_unfinished_tls_and_http_upgrade_retire_without_native_or_broker_work() {
    for scenario in ["tls", "no-http", "partial-http"] {
        let broker = QuietBroker::default();
        let (mut service, address) = wss_owner(broker.clone()).await;
        if scenario == "tls" {
            service.config.tls_acceptor = Some(tokio_rustls::TlsAcceptor::from(Arc::new(
                wss_test_server_tls(),
            )));
        }
        let observer = Arc::new(Observer::default());
        let mut client = TcpStream::connect(address).await.unwrap();
        if scenario == "partial-http" {
            client
                .write_all(b"GET /$servicebus/websocket HTTP/1.1\r\nHost: localhost\r\n")
                .await
                .unwrap();
        }
        let stage = if scenario == "tls" {
            Stage::Tls
        } else {
            Stage::Upgrade
        };
        drive(&mut service, Arc::clone(&observer), observer.stalled(stage)).await;
        let id = observer.stalled_id(stage);
        assert!(observer.native_ids().is_empty());
        service.finish().await;
        assert_eq!(service.family.finished_ids(), vec![id]);
        assert!(service.family.finished()[0].joined.is_ok());
        assert!(broker.calls.lock().unwrap().is_empty());
        assert_success(&mut service);
        drop(client);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_wss_http101_subprotocol_and_header_sasl_open_stalls_retire_same_original() {
    for subprotocol in [
        AMQP_WEBSOCKET_SUBPROTOCOL,
        AMQP_WEBSOCKET_STANDARD_SUBPROTOCOL,
    ] {
        for phase in ["header", "sasl-init", "open"] {
            let broker = QuietBroker::default();
            let (mut service, address) = wss_owner(broker.clone()).await;
            if phase == "sasl-init" {
                service.config.shared_access_authentication = Some(authentication());
            }
            let observer = Arc::new(Observer::default());
            let stream = TcpStream::connect(address).await.unwrap();
            let client = drive(&mut service, Arc::clone(&observer), async {
                let mut client = upgrade(stream, address, subprotocol).await;
                observer.stalled(Stage::Protocol).await;
                if phase == "sasl-init" {
                    mechanisms(&mut client).await;
                } else if phase == "open" {
                    write_protocol_header(&mut client, ProtocolHeader::AMQP)
                        .await
                        .unwrap();
                    assert_eq!(
                        read_protocol_header(&mut client).await.unwrap(),
                        ProtocolHeader::AMQP
                    );
                }
                client
            })
            .await;
            // The actual response proves the subphase; first Pending alone does not.
            let id = observer.stalled_id(Stage::Protocol);
            assert!(observer.native_ids().is_empty());
            service.finish().await;
            assert_eq!(service.family.finished_ids(), vec![id]);
            assert!(service.family.finished()[0].joined.is_ok());
            assert!(broker.calls.lock().unwrap().is_empty());
            assert_success(&mut service);
            drop(client);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_wss_trusted_tls_http101_and_native_open_share_one_retained_original() {
    let broker = QuietBroker::default();
    let (mut service, address) = wss_owner(broker.clone()).await;
    service.config.tls_acceptor = Some(tokio_rustls::TlsAcceptor::from(Arc::new(
        wss_test_server_tls(),
    )));
    let observer = Arc::new(Observer::default());
    let stream = TcpStream::connect(address).await.unwrap();
    let client = drive(&mut service, Arc::clone(&observer), async {
        let stream = wss_test_client_tls()
            .connect(
                rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                stream,
            )
            .await
            .unwrap();
        let mut client = upgrade(stream, address, AMQP_WEBSOCKET_SUBPROTOCOL).await;
        open(&mut client).await;
        observer.native().await;
        client
    })
    .await;
    let id = observer.native_ids()[0];
    assert_eq!(observer.stalled_id(Stage::Tls), id);
    assert_eq!(observer.stalled_id(Stage::Protocol), id);
    service.finish().await;
    assert_eq!(service.family.finished_ids(), vec![id]);
    assert!(service.family.finished()[0].joined.is_ok());
    assert!(broker.calls.lock().unwrap().is_empty());
    assert_success(&mut service);
    drop(client);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_wss_tls_error_reaps_original_then_admits_a_healthy_tls_websocket() {
    let broker = QuietBroker::default();
    let (mut service, address) = wss_owner(broker.clone()).await;
    service.config.tls_acceptor = Some(tokio_rustls::TlsAcceptor::from(Arc::new(
        wss_test_server_tls(),
    )));
    let observer = Arc::new(Observer::default());
    let mut bad = TcpStream::connect(address).await.unwrap();
    drive(&mut service, Arc::clone(&observer), async {
        bad.write_all(b"GET /not-tls HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        observer.reached(Point::Reaped).await;
    })
    .await;
    let failed_id = observer.facts(Point::Reaped).reaped.unwrap();
    assert!(service.family.is_empty());
    let stream = TcpStream::connect(address).await.unwrap();
    let healthy = drive(&mut service, Arc::clone(&observer), async {
        let stream = wss_test_client_tls()
            .connect(
                rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                stream,
            )
            .await
            .unwrap();
        let mut healthy = upgrade(stream, address, AMQP_WEBSOCKET_SUBPROTOCOL).await;
        open(&mut healthy).await;
        observer.native().await;
        healthy
    })
    .await;
    let healthy_id = observer.native_ids()[0];
    assert_ne!(failed_id, healthy_id);
    service.finish().await;
    assert_eq!(service.family.finished_ids(), vec![healthy_id]);
    let Some(AmqpListenerExit::Complete(Err(AmqpListenerFailure::Returned(error)))) =
        service.take_exit()
    else {
        panic!("raw TLS ioError retained");
    };
    assert!(error.downcast_ref::<io::Error>().is_some());
    assert!(service.take_exit().is_none());
    assert!(broker.calls.lock().unwrap().is_empty());
    drop(healthy);
    drop(bad);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_wss_native_custody_before_authorization_drains_same_original_after_fault() {
    for authenticated in [false, true] {
        let broker = QuietBroker::default();
        let (mut service, address) = wss_owner(broker.clone()).await;
        if authenticated {
            service.config.shared_access_authentication = Some(authentication());
        }
        let observer = Arc::new(Observer::default());
        let raw = Arc::new("WSS native custody installed before authorization".to_owned());
        observer.panic_after_native(Arc::clone(&raw));
        let stream = TcpStream::connect(address).await.unwrap();
        let client = drive(&mut service, Arc::clone(&observer), async {
            let mut client = upgrade(stream, address, AMQP_WEBSOCKET_SUBPROTOCOL).await;
            if authenticated {
                authenticate(&mut client).await;
            }
            // Native acceptance faults before a server Open reply is guaranteed.
            request_open(&mut client).await;
            observer.native().await;
            observer.reached(Point::Reaped).await;
            client
        })
        .await;
        let id = observer.native_ids()[0];
        assert_eq!(observer.facts(Point::Reaped).reaped, Some(id));
        assert!(service.family.is_empty());
        service.finish().await;
        let Some(AmqpListenerExit::Complete(Err(AmqpListenerFailure::Join(error)))) =
            service.take_exit()
        else {
            panic!("original outer join retained");
        };
        assert_eq!(error.id(), id);
        assert!(Arc::ptr_eq(
            &error.into_panic().downcast::<Arc<String>>().unwrap(),
            &raw
        ));
        assert!(service.take_exit().is_none());
        assert!(broker.calls.lock().unwrap().is_empty());
        drop(client);
    }
}

struct Reporter {
    reached: Arc<AtomicUsize>,
    payload: Arc<String>,
}

impl tracing::Subscriber for Reporter {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target().ends_with("::service::connection_family")
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {
        self.reached.fetch_add(1, Ordering::SeqCst);
        panic_any(Arc::clone(&self.payload));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test(flavor = "current_thread")]
async fn actual_wss_rejected_upgrade_and_reached_reporter_panic_preserve_healthy_admission() {
    let broker = QuietBroker::default();
    let (mut service, address) = wss_owner(broker.clone()).await;
    let observer = Arc::new(Observer::default());
    let reached = Arc::new(AtomicUsize::new(0));
    let dispatch = tracing::Dispatch::new(Reporter {
        reached: Arc::clone(&reached),
        payload: Arc::new("reached WSS family reporter".to_owned()),
    });
    let bad = TcpStream::connect(address).await.unwrap();
    drive(&mut service, Arc::clone(&observer), async {
        let mut request = format!("ws://{address}/wrong-path")
            .into_client_request()
            .unwrap();
        request.headers_mut().insert(
            "sec-websocket-protocol",
            AMQP_WEBSOCKET_SUBPROTOCOL.parse().unwrap(),
        );
        let error = tokio_tungstenite::client_async(request, bad)
            .await
            .unwrap_err();
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
            panic!("actual HTTP path refusal");
        };
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        observer.reached(Point::Reaped).await;
    })
    .with_subscriber(dispatch)
    .await;
    assert_eq!(reached.load(Ordering::SeqCst), 1);
    assert!(service.family.is_empty());
    let failed_id = observer.facts(Point::Reaped).reaped.unwrap();
    let stream = TcpStream::connect(address).await.unwrap();
    let healthy = drive(&mut service, Arc::clone(&observer), async {
        let mut healthy = upgrade(stream, address, AMQP_WEBSOCKET_STANDARD_SUBPROTOCOL).await;
        open(&mut healthy).await;
        observer.native().await;
        healthy
    })
    .await;
    let healthy_id = observer.native_ids()[0];
    assert_ne!(healthy_id, failed_id);
    service.finish().await;
    assert_eq!(service.family.finished_ids(), vec![healthy_id]);
    let Some(AmqpListenerExit::Complete(Err(AmqpListenerFailure::Returned(error)))) =
        service.take_exit()
    else {
        panic!("upgrade error outranks reached reporter panic");
    };
    assert!(
        matches!(error.downcast_ref::<tokio_tungstenite::tungstenite::Error>(),
        Some(tokio_tungstenite::tungstenite::Error::Http(response))
            if response.status() == StatusCode::NOT_FOUND)
    );
    assert!(service.take_exit().is_none());
    assert!(broker.calls.lock().unwrap().is_empty());
    drop(healthy);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_wss_healthy_live_join_is_pruned_while_another_http_upgrade_is_held() {
    let (mut service, address) = wss_owner(QuietBroker::default()).await;
    let observer = Arc::new(Observer::default());
    let held = TcpStream::connect(address).await.unwrap();
    drive(
        &mut service,
        Arc::clone(&observer),
        observer.stalled(Stage::Upgrade),
    )
    .await;
    let held_id = observer.stalled_id(Stage::Upgrade);
    let stream = TcpStream::connect(address).await.unwrap();
    let healthy = drive(&mut service, Arc::clone(&observer), async {
        let mut healthy = upgrade(stream, address, AMQP_WEBSOCKET_SUBPROTOCOL).await;
        open(&mut healthy).await;
        write_frame(
            &mut healthy,
            &frame(0, Performative::Close(amqp::Close::default())),
        )
        .await
        .unwrap();
        assert!(matches!(
            read_frame(&mut healthy).await.unwrap(),
            Frame::Amqp {
                performative: Some(Performative::Close(_)),
                ..
            }
        ));
        observer.reached(Point::Reaped).await;
        healthy
    })
    .await;
    let facts = observer.facts(Point::Reaped);
    assert_ne!(facts.reaped, Some(held_id));
    assert!(
        facts.finished.is_empty(),
        "healthy live joins do not accumulate"
    );
    assert_eq!(service.family.pending_ids(), vec![held_id]);
    service.finish().await;
    assert_eq!(service.family.finished_ids(), vec![held_id]);
    assert!(service.family.finished()[0].joined.is_ok());
    assert_success(&mut service);
    drop(healthy);
    drop(held);
}

#[derive(Debug)]
struct RawError(Arc<String>);

impl fmt::Display for RawError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(output)
    }
}
impl Error for RawError {}

#[tokio::test(flavor = "current_thread")]
async fn qualified_wss_fault_compositions_keep_raw_priority_and_packets_across_two_cancelled_drains()
 {
    // Injected typed packets and primary, not reached socket/authentication faults.
    for origin in ["report", "returned", "accept", "parent"] {
        let (mut service, address) = wss_owner(QuietBroker::default()).await;
        let raw = Arc::new(format!("composed WSS {origin}"));
        let error: Box<dyn Error + Send + Sync> = Box::new(RawError(Arc::clone(&raw)));
        let error_address = std::ptr::from_ref(&*error) as *const () as usize;
        let report = PreparedTask::new().spawn(
            address,
            std::future::ready(ConnectionTaskExit::ReportOnly(Box::new(Arc::clone(&raw)))),
        );
        let report_id = report.id;
        let (release, held) = oneshot::channel();
        let returned = (origin != "report").then_some(error);
        // The accept error is a separately boxed original and never cloned.
        let accept_address = if origin == "accept" {
            let error: Box<dyn Error + Send + Sync> = Box::new(RawError(Arc::clone(&raw)));
            let address = std::ptr::from_ref(&*error) as *const () as usize;
            service.primary = Some(Ok(Err(io::Error::other(error))));
            Some(address)
        } else {
            None
        };
        if origin == "parent" {
            service.primary = Some(Err(Box::new(Arc::clone(&raw))));
        }
        let second = PreparedTask::new().spawn(address, async {
            held.await.unwrap();
            ConnectionTaskExit::Complete(match returned {
                Some(error) => Err(error),
                None => Ok(()),
            })
        });
        let second_id = second.id;
        let request = second.retirement.clone();
        service.family.adopt(report);
        service.family.adopt(second);
        let observer = Arc::new(Observer::default());
        OBSERVER
            .scope(Arc::clone(&observer), async {
                let drain = service.finish();
                tokio::pin!(drain);
                tokio::select! { biased; () = observer.joined(report_id) => {},
                () = &mut drain => panic!("composed original remains held"), }
            })
            .await;
        assert!(request.is_requested());
        assert_eq!(service.family.pending_ids(), vec![second_id]);
        assert_eq!(service.family.finished_ids(), vec![report_id]);
        let packet_address = std::ptr::from_ref(&*service.family.finished()[0]) as usize;
        assert!(catch_unwind(AssertUnwindSafe(|| service.take_exit())).is_err());
        {
            let drain = service.finish();
            tokio::pin!(drain);
            pending_once(drain.as_mut()).await;
        }
        assert_eq!(
            std::ptr::from_ref(&*service.family.finished()[0]) as usize,
            packet_address
        );
        release.send(()).unwrap();
        service.finish().await;
        assert_eq!(service.family.finished_ids(), vec![report_id, second_id]);
        match origin {
            "report" => {
                let Some(AmqpListenerExit::ReportOnly(payload)) = service.take_exit() else {
                    panic!("known report-only packet stays distinct");
                };
                assert!(Arc::ptr_eq(
                    &payload.downcast::<Arc<String>>().unwrap(),
                    &raw
                ));
            }
            "returned" => {
                let Some(AmqpListenerExit::Complete(Err(AmqpListenerFailure::Returned(error)))) =
                    service.take_exit()
                else {
                    panic!("returned error outranks report-only");
                };
                assert_eq!(
                    std::ptr::from_ref(&*error) as *const () as usize,
                    error_address
                );
                assert!(Arc::ptr_eq(
                    &error.downcast_ref::<RawError>().unwrap().0,
                    &raw
                ));
            }
            "accept" => {
                let Some(AmqpListenerExit::Complete(Err(AmqpListenerFailure::Accept(error)))) =
                    service.take_exit()
                else {
                    panic!("cached accept error outranks report-only");
                };
                assert_eq!(
                    std::ptr::from_ref(error.get_ref().unwrap()) as *const () as usize,
                    accept_address.unwrap()
                );
                assert!(Arc::ptr_eq(
                    &error
                        .get_ref()
                        .unwrap()
                        .downcast_ref::<RawError>()
                        .unwrap()
                        .0,
                    &raw
                ));
            }
            "parent" => {
                let payload = match catch_unwind(AssertUnwindSafe(|| service.take_exit())) {
                    Err(payload) => payload,
                    Ok(_) => panic!("original parent panic resumes"),
                };
                assert!(Arc::ptr_eq(
                    &payload.downcast::<Arc<String>>().unwrap(),
                    &raw
                ));
            }
            _ => unreachable!(),
        }
        assert!(service.take_exit().is_none());
    }
}

async fn sender<S: AsyncRead + AsyncWrite + Unpin>(peer: &mut S) {
    write_frame(
        peer,
        &frame(
            1,
            Performative::Begin(Begin {
                remote_channel: None,
                next_outgoing_id: 0,
                incoming_window: 10,
                outgoing_window: 10,
                handle_max: 10,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            }),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(
        read_frame(peer).await.unwrap(),
        Frame::Amqp {
            performative: Some(Performative::Begin(_)),
            ..
        }
    ));
    write_frame(
        peer,
        &frame(
            1,
            Performative::Attach(Box::new(Attach {
                name: "wss-held-send".to_owned(),
                handle: 1,
                role: Role::Sender,
                snd_settle_mode: amqp::SenderSettleMode::Unsettled,
                rcv_settle_mode: amqp::ReceiverSettleMode::First,
                source: None,
                target: Some(Target::new("orders")),
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: Some(0),
                max_message_size: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            })),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(
        read_frame(peer).await.unwrap(),
        Frame::Amqp {
            performative: Some(Performative::Attach(_)),
            ..
        }
    ));
    assert!(matches!(
        read_frame(peer).await.unwrap(),
        Frame::Amqp {
            performative: Some(Performative::Flow(_)),
            ..
        }
    ));
}

async fn send<S: AsyncWrite + Unpin>(peer: &mut S) {
    let payload = amqp::encode_message(&amqp::Message {
        properties: Some(amqp::Properties {
            message_id: Some(amqp::MessageId::String("wss-held-send".to_owned())),
            ..Default::default()
        }),
        body: amqp::Body::Data(vec![b"wss-held-send".to_vec().into()]),
        ..Default::default()
    })
    .unwrap();
    write_frame(
        peer,
        &Frame::Amqp {
            channel: 1,
            performative: Some(Performative::Transfer(Transfer {
                handle: 1,
                delivery_id: Some(0),
                delivery_tag: Some(vec![0].into()),
                message_format: Some(0),
                settled: Some(false),
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            })),
            payload,
        },
    )
    .await
    .unwrap();
    peer.flush().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn actual_wss_memory_and_fjall_send_commit_keep_originals_across_two_cancelled_drains_and_reopen()
 {
    for durable in [false, true] {
        for after in [false, true] {
            let mut actor = Actor::new(durable);
            actor.gate.arm(actor.key());
            let (mut service, address) = wss_owner(actor.broker.as_ref().unwrap().clone()).await;
            let observer = Arc::new(Observer::default());
            let stream = TcpStream::connect(address).await.unwrap();
            let quiet = drive(&mut service, Arc::clone(&observer), async {
                let mut quiet = upgrade(stream, address, AMQP_WEBSOCKET_SUBPROTOCOL).await;
                open(&mut quiet).await;
                observer.native().await;
                quiet
            })
            .await;
            let quiet_id = observer.native_ids()[0];
            let stream = TcpStream::connect(address).await.unwrap();
            let sending = drive(&mut service, Arc::clone(&observer), async {
                let mut sending =
                    upgrade(stream, address, AMQP_WEBSOCKET_STANDARD_SUBPROTOCOL).await;
                open(&mut sending).await;
                sender(&mut sending).await;
                send(&mut sending).await;
                actor.gate.reached(false).await;
                if after {
                    actor.gate.release(false);
                    actor.gate.reached(true).await;
                }
                sending
            })
            .await;
            let ids = observer.native_ids();
            assert_eq!(ids.len(), 2);
            let held_id = ids[1];
            assert_eq!(actor.store().get(&actor.key()).unwrap().is_some(), after);
            OBSERVER
                .scope(Arc::clone(&observer), async {
                    let drain = service.finish();
                    tokio::pin!(drain);
                    tokio::select! { biased; () = observer.joined(quiet_id) => {},
                    () = &mut drain => panic!("real Send keeps its native owner pending"), }
                })
                .await;
            assert_eq!(service.family.pending_ids(), vec![held_id]);
            assert_eq!(service.family.finished_ids(), vec![quiet_id]);
            assert!(service.listener.is_none());
            let packet_address = std::ptr::from_ref(&*service.family.finished()[0]) as usize;
            assert!(catch_unwind(AssertUnwindSafe(|| service.take_exit())).is_err());
            {
                let drain = service.finish();
                tokio::pin!(drain);
                pending_once(drain.as_mut()).await;
            }
            assert_eq!(
                std::ptr::from_ref(&*service.family.finished()[0]) as usize,
                packet_address
            );
            actor.gate.release(false);
            actor.gate.reached(true).await;
            actor.gate.release(true);
            timeout(WAIT, service.finish())
                .await
                .expect("released real commit joins originals");
            let finished = service.family.finished_ids();
            assert_eq!(finished.len(), 2);
            assert!(finished.contains(&quiet_id) && finished.contains(&held_id));
            assert!(
                service
                    .family
                    .finished()
                    .iter()
                    .all(|packet| packet.joined.is_ok())
            );
            assert_eq!(actor.gate.commits(), 1);
            let submissions = actor
                .log
                .lock()
                .unwrap()
                .iter()
                .filter(|entry| matches!(entry.kind, CommandKind::Send { .. }))
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(submissions.len(), 2);
            assert!(!submissions[0].returned && submissions[1].returned);
            assert_eq!(submissions[0].worker, submissions[1].worker);
            assert_success(&mut service);
            drop(service);
            drop(sending);
            drop(quiet);
            let committed = actor.store().snapshot().unwrap();
            assert!(actor.store().get(&actor.key()).unwrap().is_some());
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), committed);
        }
    }
}
