use std::{
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::{
    ClientConfig, RootCertStore,
    crypto::ring,
    pki_types::{CertificateDer, ServerName},
    version::{TLS12, TLS13},
};
use server::{Broker, LocalProposer, ManualClock};
use tokio::{
    io::ReadBuf,
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_rustls::TlsConnector;

use super::*;

pub(super) trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}
pub(super) type Socket = WebSocketStream<Box<dyn Io>>;
pub(super) const LISTEN_RULE: &str = "websocket-listen-rule";

pub(super) struct Node<P: StoreProvider> {
    pub(super) store: P::Store,
    pub(super) namespace: NamespaceName,
    pub(super) address: std::net::SocketAddr,
    pub(super) broker: Broker,
    pub(super) certificate: Option<CertificateDer<'static>>,
    listener: JoinHandle<io::Result<()>>,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    pub(super) async fn start(provider: P, tls: bool, auth: bool) -> TestResult<Self> {
        Self::with_timeout(provider, tls, auth, Duration::from_secs(3)).await
    }

    pub(super) async fn with_timeout(
        provider: P,
        tls: bool,
        auth: bool,
        handshake_timeout: Duration,
    ) -> TestResult<Self> {
        Self::configured(provider, tls, auth, false, handshake_timeout).await
    }

    pub(super) async fn scoped(provider: P) -> TestResult<Self> {
        Self::configured(provider, true, true, true, Duration::from_secs(3)).await
    }

    async fn configured(
        provider: P,
        tls: bool,
        auth: bool,
        scoped: bool,
        handshake_timeout: Duration,
    ) -> TestResult<Self> {
        let store = provider.open()?;
        let namespace = NamespaceName::new("tenant")?;
        let machine = StateMachine::new(store.clone());
        for (name, requires_session) in [("orders", false), ("sessions", true)] {
            machine.apply(&Command::new(
                namespace.clone(),
                EntityPath::new(name)?,
                Timestamp::from_millis(1_000),
                CommandKind::CreateQueue {
                    config: QueueConfig {
                        requires_session,
                        ..QueueConfig::default()
                    },
                },
            ))?;
        }
        let topic = EntityPath::new("Topic")?;
        machine.apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::from_millis(1_000),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ))?;
        for name in ["Alpha", "beta"] {
            machine.apply(&Command::new(
                namespace.clone(),
                topic.clone(),
                Timestamp::from_millis(1_000),
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(name)?,
                    config: SubscriptionConfig::default(),
                },
            ))?;
        }
        let broker = Broker::spawn(LocalProposer::new(machine, ManualClock::at(1_000)));
        let mut acceptor = protocol_amqp::AmqpListener::new(broker.handle(), namespace.clone())
            .with_websocket()
            .with_max_connections(NonZeroUsize::new(1).expect("one connection"))
            .with_handshake_timeout(handshake_timeout);
        let certificate = if tls {
            let CertifiedKey { cert, key_pair } =
                generate_simple_self_signed(vec!["localhost".into()])?;
            acceptor = acceptor.with_tls(protocol_amqp::tls_server_config(
                cert.pem().as_bytes(),
                key_pair.serialize_pem().as_bytes(),
            )?);
            Some(cert.der().clone())
        } else {
            None
        };
        if auth {
            let scope = if scoped {
                ResourceScope::entity(HOST, "orders")?
            } else {
                ResourceScope::namespace(HOST)?
            };
            let rule = SharedAccessRule::new(
                RULE,
                scope.clone(),
                SharedAccessKey::new(KEY)?,
                None,
                if scoped {
                    PermissionSet::SEND
                } else {
                    PermissionSet::MANAGE
                },
            )?;
            let mut rules = vec![rule];
            if scoped {
                rules.push(SharedAccessRule::new(
                    LISTEN_RULE,
                    scope,
                    SharedAccessKey::new(KEY)?,
                    None,
                    PermissionSet::LISTEN,
                )?);
            }
            acceptor = acceptor.with_shared_access_authentication(
                protocol_amqp::SharedAccessAuthentication::new(
                    SharedAccessPolicy::new(rules)?,
                    HOST,
                )?
                .with_authorization_timeout(Duration::from_secs(3)),
            );
        }
        let tcp = TcpListener::bind("127.0.0.1:0").await?;
        let address = tcp.local_addr()?;
        let listener = tokio::spawn(acceptor.serve(tcp));
        Ok(Self {
            store,
            namespace,
            address,
            broker,
            certificate,
            listener,
            _provider: provider,
        })
    }

    pub(super) async fn transport(&self, trust: bool) -> TestResult<Box<dyn Io>> {
        let tcp = timeout(DEADLINE, TcpStream::connect(self.address)).await??;
        if let Some(certificate) = &self.certificate {
            let mut roots = RootCertStore::empty();
            if trust {
                roots.add(certificate.clone())?;
            }
            let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
                .with_protocol_versions(&[&TLS13, &TLS12])?
                .with_root_certificates(roots)
                .with_no_client_auth();
            let tls = timeout(
                DEADLINE,
                TlsConnector::from(Arc::new(config))
                    .connect(ServerName::try_from("localhost")?, tcp),
            )
            .await??;
            Ok(Box::new(tls))
        } else {
            Ok(Box::new(tcp))
        }
    }

    pub(super) fn request(
        &self,
        path: &str,
        protocol: Option<&str>,
    ) -> TestResult<tokio_tungstenite::tungstenite::handshake::client::Request> {
        let mut request =
            format!("ws://localhost:{}{path}", self.address.port()).into_client_request()?;
        if let Some(protocol) = protocol {
            request
                .headers_mut()
                .insert("Sec-WebSocket-Protocol", protocol.parse()?);
        }
        Ok(request)
    }

    pub(super) async fn websocket(&self) -> TestResult<Socket> {
        timeout(DEADLINE, async {
            loop {
                let stream = match self.transport(true).await {
                    Ok(stream) => stream,
                    Err(_) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                };
                let result = client_async(self.request(PATH, Some("amqp"))?, stream).await;
                match result {
                    Ok((socket, response)) => {
                        assert_eq!(response.status().as_u16(), 101);
                        assert_eq!(response.headers()["Sec-WebSocket-Protocol"], "amqp");
                        return Ok::<_, Box<dyn Error>>(socket);
                    }
                    Err(
                        tokio_tungstenite::tungstenite::Error::Io(_)
                        | tokio_tungstenite::tungstenite::Error::Protocol(_),
                    ) => tokio::task::yield_now().await,
                    Err(error) => return Err(error.into()),
                }
            }
        })
        .await?
    }

    pub(super) async fn connect(&self) -> TestResult<ClientConnection> {
        self.connect_with(None).await
    }

    pub(super) async fn connect_with(
        &self,
        sasl: Option<SaslInit>,
    ) -> TestResult<ClientConnection> {
        let stream = BinaryStream::new(self.websocket().await?);
        Ok(timeout(DEADLINE, ClientConnection::open(stream, "ws-client", sasl)).await??)
    }

    pub(super) async fn reusable(&self, sasl: Option<SaslInit>) -> TestResult {
        let mut peer = RawPeer::new(self.websocket().await?);
        if let Some(init) = sasl {
            peer.header(ProtocolHeader::SASL).await?;
            assert!(matches!(
                peer.read().await?,
                Frame::Sasl(amqp::SaslPerformative::Mechanisms(_))
            ));
            ws_send(
                &mut peer.socket,
                WsMessage::Binary(
                    encode_frame(&Frame::Sasl(amqp::SaslPerformative::Init(init)))?.into(),
                ),
            )
            .await?;
            assert!(
                matches!(peer.read().await?, Frame::Sasl(amqp::SaslPerformative::Outcome(outcome)) if outcome.code == amqp::SaslCode::Ok)
            );
        }
        peer.open().await?;
        peer.close().await?;
        tokio::task::yield_now().await;
        Ok(())
    }

    pub(super) fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.store.snapshot()?)
    }

    pub(super) async fn submit(
        &self,
        entity: &str,
        kind: CommandKind,
    ) -> TestResult<CommandOutcome> {
        Ok(timeout(
            DEADLINE,
            self.broker
                .handle()
                .submit(self.namespace.clone(), EntityPath::new(entity)?, kind),
        )
        .await??)
    }

    pub(super) async fn wait_removed(&self, entity: &str, sequence: u64) -> TestResult {
        let entity = EntityPath::new(entity)?;
        timeout(DEADLINE, async {
            loop {
                if self
                    .store
                    .get(&keys::message(
                        &self.namespace,
                        &entity,
                        SequenceNumber::new(sequence),
                    ))?
                    .is_none()
                {
                    return Ok::<(), Box<dyn Error>>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        Ok(())
    }
}

impl<P: StoreProvider> Drop for Node<P> {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

pub(super) fn plain(password: &str) -> SaslInit {
    SaslInit {
        mechanism: Symbol::from("PLAIN"),
        initial_response: Some(
            [
                b"\0".as_slice(),
                RULE.as_bytes(),
                b"\0",
                password.as_bytes(),
            ]
            .concat()
            .into(),
        ),
        hostname: Some(HOST.into()),
    }
}

pub(super) struct BinaryStream {
    socket: Socket,
    bytes: Vec<u8>,
    position: usize,
    eof: bool,
}

impl BinaryStream {
    pub(super) fn new(socket: Socket) -> Self {
        Self {
            socket,
            bytes: Vec::new(),
            position: 0,
            eof: false,
        }
    }
}

fn websocket_io(error: tokio_tungstenite::tungstenite::Error) -> io::Error {
    io::Error::other(error)
}

impl AsyncRead for BinaryStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            if self.position < self.bytes.len() {
                let length = output.remaining().min(self.bytes.len() - self.position);
                output.put_slice(&self.bytes[self.position..self.position + length]);
                self.position += length;
                return Poll::Ready(Ok(()));
            }
            if self.eof {
                return Poll::Ready(Ok(()));
            }
            match std::task::ready!(Pin::new(&mut self.socket).poll_next(cx)) {
                Some(Ok(WsMessage::Binary(bytes))) => {
                    self.bytes = bytes.to_vec();
                    self.position = 0;
                }
                Some(Ok(WsMessage::Close(_))) | None => {
                    self.eof = true;
                }
                Some(Ok(WsMessage::Ping(_) | WsMessage::Pong(_))) => {}
                Some(Ok(other)) => {
                    return Poll::Ready(Err(io::Error::other(format!(
                        "unexpected WS data: {other:?}"
                    ))));
                }
                Some(Err(error)) => return Poll::Ready(Err(websocket_io(error))),
            }
        }
    }
}

impl AsyncWrite for BinaryStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        std::task::ready!(Pin::new(&mut self.socket).poll_ready(cx)).map_err(websocket_io)?;
        let length = bytes.len().min(16 * 1024);
        Pin::new(&mut self.socket)
            .start_send(WsMessage::Binary(bytes[..length].to_vec().into()))
            .map_err(websocket_io)?;
        Poll::Ready(Ok(length))
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket)
            .poll_flush(cx)
            .map_err(websocket_io)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket)
            .poll_close(cx)
            .map_err(websocket_io)
    }
}

pub(super) async fn ws_next(socket: &mut Socket) -> TestResult<WsMessage> {
    Ok(timeout(DEADLINE, socket.next())
        .await?
        .ok_or("unexpected websocket EOF")??)
}

pub(super) async fn ws_send(socket: &mut Socket, message: WsMessage) -> TestResult {
    timeout(DEADLINE, socket.send(message)).await??;
    Ok(())
}

pub(super) async fn assert_close(socket: &mut Socket, code: CloseCode) -> TestResult {
    for _ in 0..8 {
        match ws_next(socket).await? {
            WsMessage::Close(frame) => {
                assert_eq!(frame.expect("explicit refusal close code").code, code);
                timeout(DEADLINE, socket.flush()).await??;
                return Ok(());
            }
            WsMessage::Ping(_) | WsMessage::Pong(_) => {}
            other => panic!("expected WS Close({code:?}), got {other:?}"),
        }
    }
    Err("close exchange exceeded the bounded control-frame count".into())
}

pub(super) fn amqp_frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

pub(super) struct RawPeer {
    pub(super) socket: Socket,
    pending: Vec<u8>,
    pub(super) pongs: Vec<Vec<u8>>,
}

impl RawPeer {
    pub(super) fn new(socket: Socket) -> Self {
        Self {
            socket,
            pending: Vec::new(),
            pongs: Vec::new(),
        }
    }

    pub(super) async fn header(&mut self, header: ProtocolHeader) -> TestResult {
        let bytes = match header {
            ProtocolHeader::AMQP => amqp::AMQP_HEADER,
            ProtocolHeader::SASL => amqp::SASL_HEADER,
            other => panic!("unexpected protocol header: {other:?}"),
        };
        ws_send(&mut self.socket, WsMessage::Binary(bytes.to_vec().into())).await?;
        assert_eq!(
            ws_next(&mut self.socket).await?,
            WsMessage::Binary(bytes.to_vec().into())
        );
        Ok(())
    }

    pub(super) async fn open(&mut self) -> TestResult {
        self.header(ProtocolHeader::AMQP).await?;
        self.send(0, Performative::Open(Open::new("raw-ws")))
            .await?;
        assert!(matches!(
            self.read().await?,
            Frame::Amqp {
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        Ok(())
    }

    pub(super) async fn send(&mut self, channel: u16, performative: Performative) -> TestResult {
        ws_send(
            &mut self.socket,
            WsMessage::Binary(encode_frame(&amqp_frame(channel, performative))?.into()),
        )
        .await
    }

    pub(super) async fn read(&mut self) -> TestResult<Frame> {
        timeout(DEADLINE, async {
            loop {
                if self.pending.len() >= 4 {
                    let length =
                        u32::from_be_bytes(self.pending[..4].try_into().expect("four bytes"))
                            as usize;
                    assert!(
                        (8..=MESSAGE_LIMIT).contains(&length),
                        "bounded AMQP frame {length}"
                    );
                    if self.pending.len() >= length {
                        let tail = self.pending.split_off(length);
                        let bytes = std::mem::replace(&mut self.pending, tail);
                        return Ok::<_, Box<dyn Error>>(
                            amqp::read_frame(&mut bytes.as_slice()).await?,
                        );
                    }
                }
                match self.socket.next().await.ok_or("unexpected WS EOF")?? {
                    WsMessage::Binary(bytes) => self.pending.extend_from_slice(&bytes),
                    WsMessage::Pong(bytes) => self.pongs.push(bytes.to_vec()),
                    WsMessage::Ping(_) => {}
                    other => panic!("AMQP frame expected, got {other:?}"),
                }
            }
        })
        .await?
    }

    pub(super) async fn close(mut self) -> TestResult {
        self.send(0, Performative::Close(amqp::Close { error: None }))
            .await?;
        assert!(matches!(
            self.read().await?,
            Frame::Amqp {
                performative: Some(Performative::Close(_)),
                ..
            }
        ));
        assert!(matches!(
            ws_next(&mut self.socket).await?,
            WsMessage::Close(_)
        ));
        timeout(DEADLINE, self.socket.flush()).await??;
        match timeout(DEADLINE, self.socket.next()).await? {
            None | Some(Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed)) => {}
            other => panic!("WS close ACK must terminate transport: {other:?}"),
        }
        Ok(())
    }
}
