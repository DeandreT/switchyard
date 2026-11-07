//! Actual staged clients against one aggregate admission budget per socket.

use std::{num::NonZeroUsize, time::Duration};

use amqp::{
    Begin, EngineError, Frame, Open, Performative, ProtocolHeader, SaslCode, SaslInit, SaslOutcome,
    SaslPerformative, Symbol, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{Instant, sleep, timeout},
};
use tokio_tungstenite::tungstenite::{self, client::IntoClientRequest, http::StatusCode};

use super::{
    test_support::{
        Observer, Point, QuietBroker, WAIT, drive, wss_test_client_tls, wss_test_server_tls,
    },
    *,
};
use crate::websocket::{AMQP_WEBSOCKET_SUBPROTOCOL, SERVICE_BUS_WEBSOCKET_PATH, WebSocketIo};

const BUDGET: Duration = Duration::from_millis(200);

fn frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

fn authentication() -> crate::SharedAccessAuthentication {
    crate::SharedAccessAuthentication::new(
        auth::SharedAccessPolicy::new([]).unwrap(),
        "tenant.example",
    )
    .unwrap()
}

async fn listener(
    broker: QuietBroker,
    wss: bool,
    tls: bool,
    sasl: bool,
    budget: Duration,
) -> (AmqpListenerService<QuietBroker>, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut config = AmqpListener::new(broker, domain::NamespaceName::new("tenant").unwrap())
        .with_max_connections(NonZeroUsize::MIN)
        .with_admission_timeout(budget);
    if tls {
        config = config.with_tls(wss_test_server_tls());
    }
    if sasl {
        config = config.with_shared_access_authentication(authentication());
    }
    let service = if wss {
        config.into_wss_service(listener)
    } else {
        config.into_tcp_service(listener)
    };
    (service, address)
}

async fn tls_client(stream: TcpStream) -> io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    wss_test_client_tls()
        .connect(
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            stream,
        )
        .await
}

async fn upgrade<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    address: SocketAddr,
) -> Result<WebSocketIo<S>, tungstenite::Error> {
    let mut request = format!("ws://{address}{SERVICE_BUS_WEBSOCKET_PATH}")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "sec-websocket-protocol",
        AMQP_WEBSOCKET_SUBPROTOCOL.parse().unwrap(),
    );
    let (socket, response) = tokio_tungstenite::client_async(request, stream).await?;
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    Ok(WebSocketIo::new(socket))
}

async fn mechanisms<S: AsyncRead + AsyncWrite + Unpin>(peer: &mut S) -> Result<(), EngineError> {
    write_protocol_header(peer, ProtocolHeader::SASL).await?;
    peer.flush().await?;
    assert_eq!(read_protocol_header(peer).await?, ProtocolHeader::SASL);
    assert!(matches!(
        read_frame(peer).await?,
        Frame::Sasl(SaslPerformative::Mechanisms(_))
    ));
    Ok(())
}

async fn sasl_init<S: AsyncRead + AsyncWrite + Unpin>(peer: &mut S) -> Result<(), EngineError> {
    write_frame(
        peer,
        &Frame::Sasl(SaslPerformative::Init(SaslInit {
            mechanism: Symbol::from("ANONYMOUS"),
            initial_response: None,
            hostname: None,
        })),
    )
    .await?;
    peer.flush().await?;
    assert!(matches!(
        read_frame(peer).await?,
        Frame::Sasl(SaslPerformative::Outcome(SaslOutcome {
            code: SaslCode::Ok,
            ..
        }))
    ));
    Ok(())
}

async fn amqp_header<S: AsyncRead + AsyncWrite + Unpin>(peer: &mut S) -> Result<(), EngineError> {
    write_protocol_header(peer, ProtocolHeader::AMQP).await?;
    peer.flush().await?;
    assert_eq!(read_protocol_header(peer).await?, ProtocolHeader::AMQP);
    Ok(())
}

async fn open<S: AsyncRead + AsyncWrite + Unpin>(peer: &mut S) -> Result<(), EngineError> {
    amqp_header(peer).await?;
    write_frame(
        peer,
        &frame(0, Performative::Open(Open::new("deadline-peer"))),
    )
    .await?;
    peer.flush().await?;
    assert!(matches!(
        read_frame(peer).await?,
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(_)),
            ..
        }
    ));
    Ok(())
}

/// A healthy peer: every applicable stage, promptly.
async fn healthy(stream: TcpStream, address: SocketAddr, wss: bool, tls: bool, sasl: bool) {
    async fn amqp<S: AsyncRead + AsyncWrite + Unpin>(mut peer: S, sasl: bool) {
        if sasl {
            mechanisms(&mut peer).await.unwrap();
            sasl_init(&mut peer).await.unwrap();
        }
        open(&mut peer).await.unwrap();
    }
    match (tls, wss) {
        (true, true) => {
            let stream = tls_client(stream).await.unwrap();
            amqp(upgrade(stream, address).await.unwrap(), sasl).await;
        }
        (true, false) => amqp(tls_client(stream).await.unwrap(), sasl).await,
        (false, true) => amqp(upgrade(stream, address).await.unwrap(), sasl).await,
        (false, false) => amqp(stream, sasl).await,
    }
}

/// The peer observes its socket closed: EOF, a reset for unread bytes, or a
/// WebSocket peer's unclean end of stream.
async fn closed<S: AsyncRead + Unpin + ?Sized>(stream: &mut S) {
    let mut rest = Vec::new();
    match timeout(WAIT, stream.read_to_end(&mut rest))
        .await
        .expect("the expired handshake closes its socket")
    {
        Ok(_) => {}
        Err(error) => assert!(
            matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::Other
            ),
            "{error}"
        ),
    }
}

/// Whether `error` is the admission deadline's `TimedOut`, at whichever stage.
fn expired(error: &(dyn Error + Send + Sync + 'static)) -> bool {
    let io = error
        .downcast_ref::<io::Error>()
        .or_else(|| match error.downcast_ref::<EngineError>() {
            Some(EngineError::Io(error)) => Some(error),
            _ => None,
        })
        .or_else(|| match error.downcast_ref::<tungstenite::Error>() {
            Some(tungstenite::Error::Io(error)) => Some(error),
            _ => None,
        });
    io.is_some_and(|error| error.kind() == io::ErrorKind::TimedOut)
}

fn assert_expired_exit<B: Broker>(service: &mut AmqpListenerService<B>) {
    let Some(AmqpListenerExit::Complete(Err(AmqpListenerFailure::Returned(error)))) =
        service.take_exit()
    else {
        panic!("the expired original's typed error is retained");
    };
    assert!(expired(&*error), "admission expiry, not {error}");
}

/// Everything after `mark` up to the first reap.
fn before_reaped(observer: &Observer, mark: usize) -> Vec<Point> {
    observer.points()[mark..]
        .iter()
        .copied()
        .take_while(|point| *point != Point::Reaped)
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum Stalled {
    /// TCP connected, nothing sent.
    Silent,
    /// A TLS record header without the rest of its ClientHello.
    TlsHello,
    /// An HTTP upgrade request without its terminating blank line.
    PartialUpgrade,
    /// SASL mechanisms received, no SASL init sent.
    SaslInit,
    /// AMQP headers exchanged, no Open sent.
    Open,
}

impl Stalled {
    fn stage(self) -> Stage {
        match self {
            Self::TlsHello => Stage::Tls,
            Self::PartialUpgrade => Stage::Upgrade,
            Self::Silent | Self::SaslInit | Self::Open => Stage::Protocol,
        }
    }
}

/// Drives `held` to the client side of its stalled stage, upgrading a WSS
/// peer first when it stalls after the upgrade.
async fn stall(
    mut held: TcpStream,
    address: SocketAddr,
    wss: bool,
    stalled: Stalled,
    observer: &Observer,
) -> Box<dyn AsyncReadWrite> {
    let mut held: Box<dyn AsyncReadWrite> = match stalled {
        Stalled::SaslInit | Stalled::Open if wss => Box::new(upgrade(held, address).await.unwrap()),
        Stalled::TlsHello => {
            held.write_all(&[0x16, 0x03, 0x01, 0x02, 0x00, 0x01])
                .await
                .unwrap();
            Box::new(held)
        }
        Stalled::PartialUpgrade => {
            held.write_all(
                format!("GET {SERVICE_BUS_WEBSOCKET_PATH} HTTP/1.1\r\nHost: localhost\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
            Box::new(held)
        }
        Stalled::Silent | Stalled::SaslInit | Stalled::Open => Box::new(held),
    };
    observer.stalled(stalled.stage()).await;
    match stalled {
        Stalled::SaslInit => mechanisms(&mut held).await.unwrap(),
        Stalled::Open => amqp_header(&mut held).await.unwrap(),
        Stalled::Silent | Stalled::TlsHello | Stalled::PartialUpgrade => {}
    }
    held
}

/// One stalled client at each stage holds the only slot until the budget
/// closes it; its original is then reaped and a waiting healthy peer admitted.
async fn stalled_stage_expires_and_releases_the_slot(wss: bool, stalled: Stalled) {
    let tls = matches!(stalled, Stalled::TlsHello);
    let sasl = matches!(stalled, Stalled::SaslInit);
    let broker = QuietBroker::default();
    let (mut service, address) = listener(broker.clone(), wss, tls, sasl, BUDGET).await;
    let observer = Arc::new(Observer::default());
    let started = Instant::now();
    let stream = TcpStream::connect(address).await.unwrap();
    let mut held = drive(
        &mut service,
        Arc::clone(&observer),
        stall(stream, address, wss, stalled, &observer),
    )
    .await;
    let held_id = observer.stalled_id(stalled.stage());
    let waiting = TcpStream::connect(address).await.unwrap();
    let mark = observer.mark();
    drive(&mut service, Arc::clone(&observer), async {
        observer.reached_since(Point::Reaped, mark).await;
        closed(&mut held).await;
    })
    .await;
    let elapsed = started.elapsed();
    assert!(elapsed >= BUDGET, "{stalled:?}: expired after {elapsed:?}");
    assert_eq!(observer.facts(Point::Reaped).reaped, Some(held_id));
    assert!(
        !before_reaped(&observer, mark).contains(&Point::Accepted),
        "{stalled:?}: the waiting peer is not accepted while the slot is held"
    );
    drive(
        &mut service,
        Arc::clone(&observer),
        healthy(waiting, address, wss, tls, sasl),
    )
    .await;
    let facts = observer.facts(Point::Accepted);
    assert_eq!(
        facts.reaped,
        Some(held_id),
        "{stalled:?}: admitted after join"
    );
    assert!(facts.pending.is_empty());
    service.finish().await;
    assert_eq!(service.occupied(), 0);
    assert!(service.family.finished()[0].joined.is_ok());
    assert!(broker.calls.lock().unwrap().is_empty());
    assert_expired_exit(&mut service);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_tcp_stalled_tcp_tls_sasl_and_open_expire_and_release_their_slot() {
    for stalled in [
        Stalled::Silent,
        Stalled::TlsHello,
        Stalled::SaslInit,
        Stalled::Open,
    ] {
        stalled_stage_expires_and_releases_the_slot(false, stalled).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_wss_stalled_tls_upgrade_sasl_and_open_expire_and_release_their_slot() {
    for stalled in [
        Stalled::TlsHello,
        Stalled::PartialUpgrade,
        Stalled::SaslInit,
        Stalled::Open,
    ] {
        stalled_stage_expires_and_releases_the_slot(true, stalled).await;
    }
}

/// A peer that pauses before its ClientHello, its upgrade request, SASL init
/// and Open. Each listener stage alone (TLS `pause`, upgrade `pause`, SASL and
/// Open `2 * pause`) fits the budget, so a timer reset per stage would admit
/// it. Returns the stages finished before the server closed the socket.
async fn slow_drip(stream: TcpStream, address: SocketAddr, wss: bool, pause: Duration) -> usize {
    let mut finished = 0;
    sleep(pause).await;
    let Ok(stream) = tls_client(stream).await else {
        return finished;
    };
    finished += 1;
    let mut peer: Box<dyn AsyncReadWrite> = if wss {
        sleep(pause).await;
        match upgrade(stream, address).await {
            Ok(stream) => {
                finished += 1;
                Box::new(stream)
            }
            Err(_) => return finished,
        }
    } else {
        Box::new(stream)
    };
    if mechanisms(&mut peer).await.is_err() {
        return finished;
    }
    finished += 1;
    sleep(pause).await;
    if sasl_init(&mut peer).await.is_err() {
        return finished;
    }
    finished += 1;
    sleep(pause).await;
    if open(&mut peer).await.is_err() {
        return finished;
    }
    panic!("a slow-drip peer was admitted past its aggregate budget");
}

trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncReadWrite for S {}

#[tokio::test(flavor = "current_thread")]
async fn actual_slow_drip_across_stages_expires_on_the_aggregate_not_a_per_stage_budget() {
    // Any one stage (at most 2 * pause) fits the budget, the TCP total (3 *
    // pause) doesn't, and TLS still has budget - pause of headroom on a busy host.
    let budget = Duration::from_millis(1500);
    let pause = Duration::from_millis(600);
    for wss in [false, true] {
        let broker = QuietBroker::default();
        let (mut service, address) = listener(broker.clone(), wss, true, true, budget).await;
        let observer = Arc::new(Observer::default());
        let started = Instant::now();
        let stream = TcpStream::connect(address).await.unwrap();
        let finished = drive(&mut service, Arc::clone(&observer), async {
            let finished = timeout(WAIT, slow_drip(stream, address, wss, pause))
                .await
                .expect("the aggregate budget closes a slow-drip peer");
            observer.reached(Point::Reaped).await;
            finished
        })
        .await;
        let elapsed = started.elapsed();
        assert!(finished >= 1, "wss {wss}: TLS completed before expiry");
        assert!(elapsed >= budget, "wss {wss}: expired after {elapsed:?}");
        service.finish().await;
        assert_eq!(service.occupied(), 0);
        assert!(broker.calls.lock().unwrap().is_empty());
        assert_expired_exit(&mut service);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_open_connections_outlive_the_admission_budget() {
    for wss in [false, true] {
        let (mut service, address) =
            listener(QuietBroker::default(), wss, true, true, BUDGET).await;
        let observer = Arc::new(Observer::default());
        let stream = TcpStream::connect(address).await.unwrap();
        drive(&mut service, Arc::clone(&observer), async {
            let stream = tls_client(stream).await.unwrap();
            let mut peer: Box<dyn AsyncReadWrite> = if wss {
                Box::new(upgrade(stream, address).await.unwrap())
            } else {
                Box::new(stream)
            };
            mechanisms(&mut peer).await.unwrap();
            sasl_init(&mut peer).await.unwrap();
            open(&mut peer).await.unwrap();
            observer.native().await;
            // Outliving the budget is the condition under test, not a witness.
            sleep(BUDGET * 2).await;
            write_frame(&mut peer, &frame(1, Performative::Begin(Begin::default())))
                .await
                .unwrap();
            peer.flush().await.unwrap();
            assert!(matches!(
                timeout(WAIT, read_frame(&mut peer))
                    .await
                    .expect("the open connection still answers")
                    .unwrap(),
                Frame::Amqp {
                    performative: Some(Performative::Begin(_)),
                    ..
                }
            ));
        })
        .await;
        assert!(!observer.points().contains(&Point::Reaped));
        service.finish().await;
        assert!(matches!(
            service.take_exit(),
            Some(AmqpListenerExit::Complete(Ok(())))
        ));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_expiry_under_a_cancelled_borrower_is_joined_by_finish() {
    let broker = QuietBroker::default();
    let (mut service, address) = listener(broker.clone(), false, false, false, BUDGET).await;
    let observer = Arc::new(Observer::default());
    let mut held = TcpStream::connect(address).await.unwrap();
    drive(
        &mut service,
        Arc::clone(&observer),
        observer.stalled(Stage::Protocol),
    )
    .await;
    let id = observer.stalled_id(Stage::Protocol);
    // No borrower runs: the original expires on its own task.
    closed(&mut held).await;
    assert_eq!(service.family.pending_ids(), vec![id]);
    service.finish().await;
    assert_eq!(service.family.finished_ids(), vec![id]);
    assert!(broker.calls.lock().unwrap().is_empty());
    assert_expired_exit(&mut service);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_retirement_before_expiry_keeps_a_clean_exit() {
    let (mut service, address) = listener(
        QuietBroker::default(),
        false,
        false,
        false,
        Duration::from_secs(3600),
    )
    .await;
    let observer = Arc::new(Observer::default());
    let mut held = TcpStream::connect(address).await.unwrap();
    drive(
        &mut service,
        Arc::clone(&observer),
        observer.stalled(Stage::Protocol),
    )
    .await;
    service.finish().await;
    closed(&mut held).await;
    assert!(matches!(
        service.take_exit(),
        Some(AmqpListenerExit::Complete(Ok(())))
    ));
}

#[test]
fn the_listener_budget_defaults_to_sixty_seconds_and_refuses_zero() {
    let config = AmqpListener::new(
        QuietBroker::default(),
        domain::NamespaceName::new("tenant").unwrap(),
    );
    assert_eq!(config.admission_timeout, Duration::from_secs(60));
    assert_eq!(crate::DEFAULT_ADMISSION_TIMEOUT, Duration::from_secs(60));
    let config = config.with_admission_timeout(Duration::from_millis(1));
    assert_eq!(config.admission_timeout, Duration::from_millis(1));
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            config.with_admission_timeout(Duration::ZERO)
        }))
        .is_err()
    );
}

#[test]
fn an_unrepresentable_budget_saturates_instead_of_panicking() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let _guard = runtime.enter();
    assert!(admission_deadline(Duration::MAX) > Instant::now() + Duration::from_secs(3600));
}
