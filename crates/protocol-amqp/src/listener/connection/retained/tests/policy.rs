use super::*;
use fixture::{authentication, sasl_hello, tcp_pair};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const NEGOTIATION: Duration = Duration::from_secs(1);

#[derive(Clone, Copy)]
enum PublicMode {
    Ordinary,
    Posting,
    Messaging,
}

fn listener(broker: NoBroker) -> AmqpListener<NoBroker> {
    AmqpListener::new(
        broker,
        NamespaceName::new("tenant").expect("fixture namespace"),
    )
}

// These cases stop before certificate selection; there is no successful TLS
// connection, certificate owner, or new crypto provider in this fixture.
fn pre_certificate_tls() -> TestResult<rustls::ServerConfig> {
    Ok(rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
    .with_no_client_auth()
    .with_cert_resolver(Arc::new(rustls::server::ResolvesServerCertUsingSni::new())))
}

async fn primary_published<A>(owner: &RetainedConnectionOwner<A>) {
    while !owner.published().0 {
        tokio::task::yield_now().await;
    }
}

async fn finish_socket<A>(
    owner: &mut RetainedConnectionOwner<A>,
    controls: &Controls,
    peer: TcpStream,
) -> Option<RetainedConnectionJoinReport<A>> {
    controls.release();
    owner.stop();
    drop(peer);
    owner.finish().await
}

fn assert_pre_engine<A>(report: &RetainedConnectionJoinReport<A>, broker: &NoBroker) {
    assert!(matches!(report.wrapper(), Some(Ok(()))));
    assert!(report.actor().is_none() && report.reader().is_none());
    assert!(report.outcomes().websocket_close.is_none());
    assert_eq!(broker.plan.calls.load(Ordering::SeqCst), 0);
}

fn assert_original_timeout<A>(report: &RetainedConnectionJoinReport<A>) {
    let error = primary_error(report)
        .and_then(|error| error.downcast_ref::<io::Error>())
        .expect("existing handshake timeout boundary error");
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(
        error.to_string(),
        "TLS/HTTP/SASL/AMQP Open negotiation exceeded the handshake deadline",
    );
}

async fn public_open_close(mode: PublicMode) -> TestResult {
    let (stream, mut peer) = tcp_pair().await?;
    let broker = NoBroker::default();
    let anchor = std::rc::Rc::new(());
    let (mut owner, starter) =
        RetainedConnectionOwner::new(tokio::runtime::Handle::current(), &anchor);
    let controls = owner.controls();
    let configured = listener(broker.clone());
    let original = match mode {
        PublicMode::Ordinary => configured.start_retained_connection(stream, starter),
        PublicMode::Posting => configured.start_retained_atomic_posting_ingress(stream, starter),
        PublicMode::Messaging => {
            configured.start_retained_atomic_messaging_ingress(stream, starter)
        }
    };
    let setup = bounded(async {
        hello(&mut peer).await?;
        amqp::write_frame(
            &mut peer,
            &amqp::Frame::Amqp {
                channel: 0,
                performative: Some(amqp::Performative::Close(amqp::Close::default())),
                payload: Vec::new(),
            },
        )
        .await?;
        amqp::read_frame(&mut peer).await
    })
    .await;
    let observed = bounded(primary_published(&owner)).await;
    let report = finish_socket(&mut owner, &controls, peer)
        .await
        .expect("finished external one-socket owner");

    original.map_err(|_| io::Error::other("unexpected public start failure"))?;
    let closed = setup??;
    observed?;
    assert!(matches!(
        closed,
        amqp::Frame::Amqp {
            channel: 0,
            performative: Some(amqp::Performative::Close(_)),
            ..
        }
    ));
    successful_socket_joins(&report);
    assert!(matches!(
        report.outcomes().primary,
        Some(Outcome::Finished(Ok(())))
    ));
    assert!(report.outcomes().websocket_close.is_none());
    assert!(std::ptr::eq(*report.anchor(), &anchor));
    assert_eq!(broker.plan.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn public_ordinary_tcp_open_close_joins_created_wrapper_actor_reader() -> TestResult {
    public_open_close(PublicMode::Ordinary).await
}

#[tokio::test]
async fn public_posting_tcp_open_close_joins_created_wrapper_actor_reader() -> TestResult {
    public_open_close(PublicMode::Posting).await
}

#[tokio::test]
async fn public_messaging_tcp_open_close_joins_created_wrapper_actor_reader() -> TestResult {
    public_open_close(PublicMode::Messaging).await
}

#[tokio::test]
async fn public_tls_invalid_record_retains_returned_rustls_cause_without_engine_roles() -> TestResult
{
    let tls = pre_certificate_tls()?;
    let (stream, mut peer) = tcp_pair().await?;
    let broker = NoBroker::default();
    let (mut owner, starter) = RetainedConnectionOwner::new(tokio::runtime::Handle::current(), ());
    let controls = owner.controls();
    let original = listener(broker.clone())
        .with_tls(tls)
        .with_handshake_timeout(NEGOTIATION)
        .start_retained_connection(stream, starter);
    let setup = bounded(peer.write_all(b"\x17\x03\x03\x00\x01\x00")).await;
    let observed = bounded(primary_published(&owner)).await;
    let report = finish_socket(&mut owner, &controls, peer)
        .await
        .expect("TLS rejection report after original wrapper join");

    original.map_err(|_| io::Error::other("unexpected public TLS start failure"))?;
    setup??;
    observed?;
    assert_pre_engine(&report, &broker);
    let error = primary_error(&report)
        .and_then(|error| error.downcast_ref::<io::Error>())
        .expect("returned TLS io error");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error
            .get_ref()
            .is_some_and(|cause| cause.is::<rustls::Error>())
    );
    Ok(())
}

#[tokio::test]
async fn public_incomplete_tls_record_expires_at_existing_absolute_deadline() -> TestResult {
    let tls = pre_certificate_tls()?;
    let (stream, mut peer) = tcp_pair().await?;
    let broker = NoBroker::default();
    let (mut owner, starter) = RetainedConnectionOwner::new(tokio::runtime::Handle::current(), ());
    let controls = owner.controls();
    let original = listener(broker.clone())
        .with_tls(tls)
        .with_handshake_timeout(NEGOTIATION)
        .start_retained_connection(stream, starter);
    let setup = bounded(peer.write_all(b"\x16\x03\x03\x00\x10\x01")).await;
    let first_poll = bounded(controls.wait_for(|marks| marks.first_poll)).await;
    let observed = bounded(primary_published(&owner)).await;
    let report = finish_socket(&mut owner, &controls, peer)
        .await
        .expect("TLS timeout report after original wrapper join");

    original.map_err(|_| io::Error::other("unexpected public TLS start failure"))?;
    setup??;
    first_poll?;
    observed?;
    assert_pre_engine(&report, &broker);
    assert_original_timeout(&report);
    Ok(())
}

#[tokio::test]
async fn public_http_wrong_endpoint_retains_existing_upgrade_boundary_error() -> TestResult {
    let (stream, mut peer) = tcp_pair().await?;
    let broker = NoBroker::default();
    let (mut owner, starter) = RetainedConnectionOwner::new(tokio::runtime::Handle::current(), ());
    let controls = owner.controls();
    let original = listener(broker.clone())
        .with_websocket()
        .with_handshake_timeout(NEGOTIATION)
        .start_retained_connection(stream, starter);
    let setup = bounded(async {
        peer.write_all(
            b"GET /not-the-endpoint HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: amqp\r\n\r\n",
        )
        .await?;
        let mut response = Vec::new();
        peer.read_to_end(&mut response).await?;
        Ok::<_, io::Error>(response)
    })
    .await;
    let observed = bounded(primary_published(&owner)).await;
    let report = finish_socket(&mut owner, &controls, peer)
        .await
        .expect("HTTP rejection report after original wrapper join");

    original.map_err(|_| io::Error::other("unexpected public HTTP start failure"))?;
    let response = setup??;
    observed?;
    assert_pre_engine(&report, &broker);
    assert!(response.starts_with(b"HTTP/1.1 404 "));
    let error = primary_error(&report)
        .and_then(|error| error.downcast_ref::<io::Error>())
        .expect("existing HTTP upgrade boundary error");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "WebSocket HTTP upgrade failed");
    // The existing adapter erased the underlying handshake cause already.
    assert!(
        !error
            .get_ref()
            .is_some_and(|cause| cause.is::<tokio_tungstenite::tungstenite::Error>())
    );
    Ok(())
}

#[tokio::test]
async fn public_incomplete_http_upgrade_expires_at_existing_absolute_deadline() -> TestResult {
    let (stream, mut peer) = tcp_pair().await?;
    let broker = NoBroker::default();
    let (mut owner, starter) = RetainedConnectionOwner::new(tokio::runtime::Handle::current(), ());
    let controls = owner.controls();
    let original = listener(broker.clone())
        .with_websocket()
        .with_handshake_timeout(NEGOTIATION)
        .start_retained_connection(stream, starter);
    let setup =
        bounded(peer.write_all(b"GET /$servicebus/websocket/ HTTP/1.1\r\nHost: localhost\r\n"))
            .await;
    let first_poll = bounded(controls.wait_for(|marks| marks.first_poll)).await;
    let observed = bounded(primary_published(&owner)).await;
    let report = finish_socket(&mut owner, &controls, peer)
        .await
        .expect("HTTP timeout report after original wrapper join");

    original.map_err(|_| io::Error::other("unexpected public HTTP start failure"))?;
    setup??;
    first_poll?;
    observed?;
    assert_pre_engine(&report, &broker);
    assert_original_timeout(&report);
    Ok(())
}

#[tokio::test]
async fn public_sasl_unsupported_mechanism_retains_returned_permission_denied_cause() -> TestResult
{
    let authentication = authentication()?;
    let (stream, mut peer) = tcp_pair().await?;
    let broker = NoBroker::default();
    let (mut owner, starter) = RetainedConnectionOwner::new(tokio::runtime::Handle::current(), ());
    let controls = owner.controls();
    let original = listener(broker.clone())
        .with_shared_access_authentication(authentication)
        .with_handshake_timeout(NEGOTIATION)
        .start_retained_connection(stream, starter);
    let setup = bounded(sasl_hello(&mut peer, "NOT-OFFERED")).await;
    let observed = bounded(primary_published(&owner)).await;
    let report = finish_socket(&mut owner, &controls, peer)
        .await
        .expect("SASL rejection report after original wrapper join");

    original.map_err(|_| io::Error::other("unexpected public SASL start failure"))?;
    let code = setup??;
    observed?;
    assert_eq!(code, amqp::SaslCode::Auth);
    assert_pre_engine(&report, &broker);
    let engine = primary_error(&report).and_then(|error| error.downcast_ref::<amqp::EngineError>());
    let Some(amqp::EngineError::Io(error)) = engine else {
        panic!("returned SASL negotiation engine io error");
    };
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(error.to_string(), "SASL authentication failed");
    Ok(())
}
