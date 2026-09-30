//! Negotiation deadlines, connection admission, and socket-owner cleanup.

use std::{error::Error, io, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use amqp::{
    ClientConnection, Frame, Open, Performative, ProtocolHeader, SaslInit, SaslPerformative,
    Symbol, encode_frame, read_frame, read_protocol_header, write_frame, write_protocol_header,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{NamespaceName, StateMachine};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::{
    ClientConfig, RootCertStore,
    crypto::ring,
    pki_types::{CertificateDer, ServerName},
    version::{TLS12, TLS13},
};
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_rustls::TlsConnector;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_millis(250);
const CBS_TIMEOUT: Duration = Duration::from_millis(80);
const TEST_TIMEOUT: Duration = Duration::from_secs(4);
const HOST: &str = "tenant.servicebus.windows.net";

struct Node {
    _broker: Broker,
    listener: JoinHandle<io::Result<()>>,
    address: SocketAddr,
    certificate: Option<CertificateDer<'static>>,
    sasl: bool,
}

impl Node {
    async fn start(tls: bool, sasl: bool, handshake_timeout: Duration) -> TestResult<Self> {
        Self::start_with_idle(tls, sasl, handshake_timeout, 60_000).await
    }

    async fn start_with_idle(
        tls: bool,
        sasl: bool,
        handshake_timeout: Duration,
        idle_timeout_millis: u32,
    ) -> TestResult<Self> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            ManualClock::at(1_000),
        ));
        let namespace = NamespaceName::new("tenant")?;
        let mut acceptor = protocol_amqp::AmqpListener::new(broker.handle(), namespace)
            .with_max_connections(NonZeroUsize::new(1).expect("positive admission limit"))
            .with_handshake_timeout(handshake_timeout)
            .with_idle_timeout_millis(idle_timeout_millis);
        let certificate = if tls {
            let CertifiedKey { cert, key_pair } =
                generate_simple_self_signed(vec![String::from("localhost")])?;
            acceptor = acceptor.with_tls(protocol_amqp::tls_server_config(
                cert.pem().as_bytes(),
                key_pair.serialize_pem().as_bytes(),
            )?);
            Some(cert.der().clone())
        } else {
            None
        };
        if sasl {
            let rule = SharedAccessRule::new(
                "rule",
                ResourceScope::namespace(HOST)?,
                SharedAccessKey::new("test-secret")?,
                None,
                PermissionSet::MANAGE,
            )?;
            acceptor = acceptor.with_shared_access_authentication(
                protocol_amqp::SharedAccessAuthentication::new(
                    SharedAccessPolicy::new([rule])?,
                    HOST,
                )?
                .with_authorization_timeout(CBS_TIMEOUT),
            );
        }
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let listener = tokio::spawn(acceptor.serve(listener));
        Ok(Self {
            _broker: broker,
            listener,
            address,
            certificate,
            sasl,
        })
    }

    async fn tls_connect(&self) -> TestResult<tokio_rustls::client::TlsStream<TcpStream>> {
        let mut roots = RootCertStore::empty();
        roots.add(self.certificate.clone().expect("TLS node certificate"))?;
        let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_protocol_versions(&[&TLS13, &TLS12])?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let stream = TcpStream::connect(self.address).await?;
        Ok(TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from("localhost")?, stream)
            .await?)
    }

    async fn connect(&self) -> TestResult<ClientConnection> {
        let init = self.sasl.then(anonymous_init);
        if self.certificate.is_some() {
            Ok(ClientConnection::open(self.tls_connect().await?, "lifecycle-test", init).await?)
        } else {
            Ok(ClientConnection::open(
                TcpStream::connect(self.address).await?,
                "lifecycle-test",
                init,
            )
            .await?)
        }
    }

    async fn assert_permit_reusable(&self) -> TestResult {
        timeout(TEST_TIMEOUT, async {
            loop {
                match self.connect().await {
                    Ok(connection) => {
                        connection.close().await?;
                        return Ok::<(), Box<dyn Error>>(());
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
                }
            }
        })
        .await??;
        Ok(())
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

fn anonymous_init() -> SaslInit {
    SaslInit {
        mechanism: Symbol::from("ANONYMOUS"),
        initial_response: None,
        hostname: Some(String::from(HOST)),
    }
}

async fn assert_transport_closed<R: AsyncRead + Unpin>(reader: &mut R) -> TestResult<Vec<u8>> {
    let mut bytes = Vec::new();
    let result = timeout(TEST_TIMEOUT, reader.read_to_end(&mut bytes)).await?;
    match result {
        Ok(_) => {}
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
            ) => {}
        Err(error) => return Err(error.into()),
    }
    Ok(bytes)
}

async fn begin_sasl<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> TestResult {
    write_protocol_header(stream, ProtocolHeader::SASL).await?;
    assert_eq!(read_protocol_header(stream).await?, ProtocolHeader::SASL);
    assert!(matches!(
        read_frame(stream).await?,
        Frame::Sasl(SaslPerformative::Mechanisms(_))
    ));
    Ok(())
}

async fn finish_sasl<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> TestResult {
    write_frame(
        stream,
        &Frame::Sasl(SaslPerformative::Init(anonymous_init())),
    )
    .await?;
    assert!(matches!(
        read_frame(stream).await?,
        Frame::Sasl(SaslPerformative::Outcome(_))
    ));
    Ok(())
}

async fn open_amqp<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> TestResult {
    open_amqp_with_idle(stream, None).await?;
    Ok(())
}

async fn open_amqp_with_idle<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    peer_idle: Option<u32>,
) -> TestResult<Open> {
    write_protocol_header(stream, ProtocolHeader::AMQP).await?;
    assert_eq!(read_protocol_header(stream).await?, ProtocolHeader::AMQP);
    write_frame(
        stream,
        &Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(Open {
                idle_time_out: peer_idle,
                ..Open::new("raw-peer")
            })),
            payload: Vec::new(),
        },
    )
    .await?;
    match read_frame(stream).await? {
        Frame::Amqp {
            performative: Some(Performative::Open(open)),
            ..
        } => Ok(open),
        other => panic!("expected Open, received {other:?}"),
    }
}

#[tokio::test]
async fn silent_and_partial_protocol_headers_expire_and_release_admission() -> TestResult {
    for partial_header in [false, true] {
        let node = Node::start(false, false, HANDSHAKE_TIMEOUT).await?;
        let mut stalled = TcpStream::connect(node.address).await?;
        if partial_header {
            stalled.write_all(b"AM").await?;
        }
        assert_transport_closed(&mut stalled).await?;
        node.assert_permit_reusable().await?;
    }
    Ok(())
}

#[tokio::test]
async fn partial_progress_does_not_restart_the_absolute_open_deadline() -> TestResult {
    let node = Node::start(false, false, Duration::from_millis(400)).await?;
    let mut stalled = TcpStream::connect(node.address).await?;
    stalled.write_all(b"AM").await?;
    tokio::time::sleep(Duration::from_millis(250)).await;
    stalled.write_all(&amqp::AMQP_HEADER[2..]).await?;
    assert_eq!(
        read_protocol_header(&mut stalled).await?,
        ProtocolHeader::AMQP
    );
    // Open never arrives. Completing the header must not grant another 400ms.
    timeout(
        Duration::from_millis(300),
        assert_transport_closed(&mut stalled),
    )
    .await??;
    node.assert_permit_reusable().await?;
    Ok(())
}

#[tokio::test]
async fn a_silent_sasl_init_expires_before_cbs_authorization_begins() -> TestResult {
    let node = Node::start(false, true, HANDSHAKE_TIMEOUT).await?;
    let mut stalled = TcpStream::connect(node.address).await?;
    begin_sasl(&mut stalled).await?;
    assert_transport_closed(&mut stalled).await?;
    node.assert_permit_reusable().await?;
    Ok(())
}

#[tokio::test]
async fn sasl_success_does_not_allow_a_silent_amqp_open_to_hold_admission() -> TestResult {
    let node = Node::start(false, true, HANDSHAKE_TIMEOUT).await?;
    let mut stalled = TcpStream::connect(node.address).await?;
    begin_sasl(&mut stalled).await?;
    finish_sasl(&mut stalled).await?;
    write_protocol_header(&mut stalled, ProtocolHeader::AMQP).await?;
    assert_eq!(
        read_protocol_header(&mut stalled).await?,
        ProtocolHeader::AMQP
    );
    assert_transport_closed(&mut stalled).await?;
    node.assert_permit_reusable().await?;
    Ok(())
}

#[tokio::test]
async fn silent_tls_and_tls_then_silent_sasl_share_a_bounded_handshake() -> TestResult {
    let node = Node::start(true, true, HANDSHAKE_TIMEOUT).await?;
    let mut stalled = TcpStream::connect(node.address).await?;
    assert_transport_closed(&mut stalled).await?;
    node.assert_permit_reusable().await?;

    let mut tls = node.tls_connect().await?;
    begin_sasl(&mut tls).await?;
    assert_transport_closed(&mut tls).await?;
    node.assert_permit_reusable().await?;
    Ok(())
}

#[tokio::test]
async fn admission_refuses_excess_sockets_while_a_negotiated_connection_is_alive() -> TestResult {
    let node = Node::start(false, false, HANDSHAKE_TIMEOUT).await?;
    let connection = node.connect().await?;
    let mut refused = TcpStream::connect(node.address).await?;
    assert!(assert_transport_closed(&mut refused).await?.is_empty());
    connection.close().await?;
    node.assert_permit_reusable().await?;
    Ok(())
}

#[tokio::test]
async fn admission_also_counts_an_unfinished_sasl_handshake() -> TestResult {
    let node = Node::start(false, true, HANDSHAKE_TIMEOUT).await?;
    let mut negotiating = TcpStream::connect(node.address).await?;
    begin_sasl(&mut negotiating).await?;
    let mut refused = TcpStream::connect(node.address).await?;
    assert!(assert_transport_closed(&mut refused).await?.is_empty());
    assert_transport_closed(&mut negotiating).await?;
    node.assert_permit_reusable().await?;
    Ok(())
}

#[tokio::test]
async fn missing_cbs_token_and_ignored_close_cannot_retain_the_only_permit() -> TestResult {
    let node = Node::start(false, true, HANDSHAKE_TIMEOUT).await?;
    let mut peer = TcpStream::connect(node.address).await?;
    begin_sasl(&mut peer).await?;
    finish_sasl(&mut peer).await?;
    open_amqp(&mut peer).await?;
    let started = Instant::now();
    let Frame::Amqp {
        performative: Some(Performative::Close(close)),
        ..
    } = timeout(TEST_TIMEOUT, read_frame(&mut peer)).await??
    else {
        panic!("CBS deadline produces Close");
    };
    assert_eq!(
        close
            .error
            .expect("authorization error")
            .condition
            .as_symbol(),
        Symbol::from("amqp:unauthorized-access")
    );
    // Deliberately ignore Close: its own deadline must cancel the engine reader.
    assert_transport_closed(&mut peer).await?;
    assert!(started.elapsed() < TEST_TIMEOUT);
    node.assert_permit_reusable().await?;
    Ok(())
}

#[tokio::test]
async fn malformed_state_followed_by_silence_releases_socket_and_permit() -> TestResult {
    let node = Node::start(false, false, HANDSHAKE_TIMEOUT).await?;
    let mut peer = TcpStream::connect(node.address).await?;
    open_amqp(&mut peer).await?;
    let duplicate = Frame::Amqp {
        channel: 0,
        performative: Some(Performative::Open(Open::new("duplicate"))),
        payload: Vec::new(),
    };
    peer.write_all(&encode_frame(&duplicate)?).await?;
    assert_transport_closed(&mut peer).await?;
    node.assert_permit_reusable().await?;
    Ok(())
}

#[tokio::test]
async fn client_drop_releases_admission_even_with_a_live_client_session() -> TestResult {
    let node = Node::start(false, false, HANDSHAKE_TIMEOUT).await?;
    let mut connection = node.connect().await?;
    let _session = connection.begin().await?;
    drop(connection);
    node.assert_permit_reusable().await?;
    Ok(())
}

#[tokio::test]
async fn zero_handshake_duration_is_an_immediate_deadline_not_an_unlimited_one() -> TestResult {
    let node = Node::start(false, false, Duration::ZERO).await?;
    let mut peer = TcpStream::connect(node.address).await?;
    assert_transport_closed(&mut peer).await?;
    Ok(())
}

async fn receive_idle_close<S: AsyncRead + Unpin>(stream: &mut S) -> TestResult {
    let Frame::Amqp {
        channel: 0,
        performative: Some(Performative::Close(close)),
        payload,
    } = timeout(TEST_TIMEOUT, read_frame(stream)).await??
    else {
        panic!("receive silence must produce Close");
    };
    assert!(payload.is_empty());
    assert_eq!(
        close
            .error
            .expect("idle timeout error")
            .condition
            .as_symbol(),
        Symbol::from("amqp:connection:forced")
    );
    Ok(())
}

async fn close_raw<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> TestResult {
    write_frame(
        stream,
        &Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Close(amqp::Close::default())),
            payload: Vec::new(),
        },
    )
    .await?;
    let Frame::Amqp {
        performative: Some(Performative::Close(close)),
        ..
    } = timeout(TEST_TIMEOUT, read_frame(stream)).await??
    else {
        panic!("expected graceful Close acknowledgment");
    };
    assert!(close.error.is_none(), "connection must still be live");
    assert!(assert_transport_closed(stream).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn receive_idle_timeout_and_ignored_close_release_admission_over_tcp_and_tls() -> TestResult {
    for tls in [false, true] {
        let node = Node::start_with_idle(tls, false, HANDSHAKE_TIMEOUT, 1_000).await?;
        if tls {
            let mut peer = node.tls_connect().await?;
            assert_eq!(
                open_amqp_with_idle(&mut peer, Some(0)).await?.idle_time_out,
                Some(1_000)
            );
            receive_idle_close(&mut peer).await?;
            assert!(assert_transport_closed(&mut peer).await?.is_empty());
            node.assert_permit_reusable().await?;
        } else {
            let mut peer = TcpStream::connect(node.address).await?;
            assert_eq!(
                open_amqp_with_idle(&mut peer, Some(0)).await?.idle_time_out,
                Some(1_000)
            );
            receive_idle_close(&mut peer).await?;
            assert!(assert_transport_closed(&mut peer).await?.is_empty());
            node.assert_permit_reusable().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn outbound_keepalives_survive_disabled_local_receive_timeout() -> TestResult {
    let node = Node::start_with_idle(false, false, HANDSHAKE_TIMEOUT, 0).await?;
    let mut peer = TcpStream::connect(node.address).await?;
    assert_eq!(
        open_amqp_with_idle(&mut peer, Some(1_000))
            .await?
            .idle_time_out,
        Some(0)
    );
    let started = Instant::now();
    for _ in 0..5 {
        assert!(matches!(
            timeout(Duration::from_millis(900), read_frame(&mut peer)).await??,
            Frame::Amqp { channel: 0, performative: None, payload } if payload.is_empty()
        ));
    }
    assert!(started.elapsed() >= Duration::from_secs(2));
    close_raw(&mut peer).await?;
    node.assert_permit_reusable().await?;
    Ok(())
}

#[tokio::test]
async fn partial_frame_progress_does_not_restart_receive_idle_timeout() -> TestResult {
    let node = Node::start_with_idle(false, false, HANDSHAKE_TIMEOUT, 1_000).await?;
    let mut peer = TcpStream::connect(node.address).await?;
    open_amqp_with_idle(&mut peer, Some(0)).await?;
    let heartbeat = encode_frame(&Frame::Amqp {
        channel: 0,
        performative: None,
        payload: Vec::new(),
    })?;
    peer.write_all(&heartbeat[..4]).await?;
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    peer.write_all(&heartbeat[4..7]).await?;
    timeout(Duration::from_millis(1_500), receive_idle_close(&mut peer)).await??;
    assert!(assert_transport_closed(&mut peer).await?.is_empty());
    node.assert_permit_reusable().await?;
    Ok(())
}

#[tokio::test]
async fn complete_heartbeats_on_an_unbound_valid_channel_keep_a_connection_alive() -> TestResult {
    let node = Node::start_with_idle(false, false, HANDSHAKE_TIMEOUT, 1_000).await?;
    let mut peer = TcpStream::connect(node.address).await?;
    let open = open_amqp_with_idle(&mut peer, Some(0)).await?;
    assert!(open.channel_max >= 7);
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(900)).await;
        write_frame(
            &mut peer,
            &Frame::Amqp {
                channel: 7,
                performative: None,
                payload: Vec::new(),
            },
        )
        .await?;
    }
    close_raw(&mut peer).await?;
    node.assert_permit_reusable().await?;
    Ok(())
}

#[tokio::test]
async fn invalid_idle_or_write_limits_are_refused_before_listener_admission() -> TestResult {
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(MemoryStore::default()),
        ManualClock::at(1_000),
    ));
    for idle in [1, 999] {
        let acceptor =
            protocol_amqp::AmqpListener::new(broker.handle(), NamespaceName::new("tenant")?)
                .with_idle_timeout_millis(idle);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let error = timeout(TEST_TIMEOUT, acceptor.serve(listener))
            .await?
            .expect_err("invalid idle limit");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
    for duration in [Duration::ZERO, Duration::MAX] {
        let acceptor =
            protocol_amqp::AmqpListener::new(broker.handle(), NamespaceName::new("tenant")?)
                .with_write_timeout(duration);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let error = timeout(TEST_TIMEOUT, acceptor.serve(listener))
            .await?
            .expect_err("invalid write limit");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
    Ok(())
}
