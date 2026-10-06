//! Opt-in offline JWT CBS over actual TLS/WSS; existing SAS policy remains active.
use amqp::{
    ApplicationProperties, Body, ClientConnection, ClientReceiver, ClientSender, ClientSession,
    EngineError, Message, Outcome, Properties, SaslInit, Value,
};
use auth::{
    JwtPolicy, PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule,
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use domain::{
    Command, CommandKind, EntityPath, NamespaceName, QueueConfig, QueueCounters, StateMachine,
    Timestamp, keys,
};
use futures_util::{FutureExt, Sink, SinkExt, Stream, StreamExt};
use hmac::{Hmac, Mac};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::{
    ClientConfig, RootCertStore, SignatureScheme,
    crypto::ring,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, ServerName},
    version::{TLS12, TLS13},
};
use server::{Broker, BrokerHandle, LocalProposer, ManualClock};
use sha2::Sha256;
use std::{
    error::Error,
    io,
    panic::{AssertUnwindSafe, resume_unwind},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use storage::StateStore;
use testkit::StoreProvider;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::{TcpListener, TcpStream},
    task::{JoinError, JoinHandle},
    time::timeout,
};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::{
    WebSocketStream, client_async,
    tungstenite::{Message as WsMessage, client::IntoClientRequest},
};

const DEADLINE: Duration = Duration::from_secs(5);
const HOST: &str = "tenant.example";
const AUDIENCE: &str = "amqps://tenant.example/orders";
const PATH: &str = "/$servicebus/websocket/";
type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type Observed =
    Result<Result<TestResult, tokio::time::error::Elapsed>, Box<dyn std::any::Any + Send>>;
const MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
// Public RSA PKCS1 fixture shared with the pure auth tests, never live credentials.
const PRIVATE_DER: &str = concat!(
    "MIIEpAIBAAKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTL",
    "UTv4l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2V",
    "rUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8H",
    "oGfG/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBI",
    "Mc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/",
    "by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQABAoIBAHREk0I0O9DvECKd",
    "WUpAmF3mY7oY9PNQiu44Yaf+AoSuyRpRUGTMIgc3u3eivOE8ALX0BmYUO5JtuRNZ",
    "Dpvt4SAwqCnVUinIf6C+eH/wSurCpapSM0BAHp4aOA7igptyOMgMPYBHNA1e9A7j",
    "E0dCxKWMl3DSWNyjQTk4zeRGEAEfbNjHrq6YCtjHSZSLmWiG80hnfnYos9hOr5Jn",
    "LnyS7ZmFE/5P3XVrxLc/tQ5zum0R4cbrgzHiQP5RgfxGJaEi7XcgherCCOgurJSS",
    "bYH29Gz8u5fFbS+Yg8s+OiCss3cs1rSgJ9/eHZuzGEdUZVARH6hVMjSuwvqVTFaE",
    "8AgtleECgYEA+uLMn4kNqHlJS2A5uAnCkj90ZxEtNm3E8hAxUrhssktY5XSOAPBl",
    "xyf5RuRGIImGtUVIr4HuJSa5TX48n3Vdt9MYCprO/iYl6moNRSPt5qowIIOJmIjY",
    "2mqPDfDt/zw+fcDD3lmCJrFlzcnh0uea1CohxEbQnL3cypeLt+WbU6kCgYEAzSp1",
    "9m1ajieFkqgoB0YTpt/OroDx38vvI5unInJlEeOjQ+oIAQdN2wpxBvTrRorMU6P0",
    "7mFUbt1j+Co6CbNiw+X8HcCaqYLR5clbJOOWNR36PuzOpQLkfK8woupBxzW9B8gZ",
    "mY8rB1mbJ+/WTPrEJy6YGmIEBkWylQ2VpW8O4O0CgYEApdbvvfFBlwD9YxbrcGz7",
    "MeNCFbMz+MucqQntIKoKJ91ImPxvtc0y6e/Rhnv0oyNlaUOwJVu0yNgNG117w0g4",
    "t/+Q38mvVC5xV7/cn7x9UMFk6MkqVir3dYGEqIl/OP1grY2Tq9HtB5iyG9L8NIam",
    "QOLMyUqqMUILxdthHyFmiGkCgYEAn9+PjpjGMPHxL0gj8Q8VbzsFtou6b1deIRRA",
    "2CHmSltltR1gYVTMwXxQeUhPMmgkMqUXzs4/WijgpthY44hK1TaZEKIuoxrS70nJ",
    "4WQLf5a9k1065fDsFZD6yGjdGxvwEmlGMZgTwqV7t1I4X0Ilqhav5hcs5apYL7gn",
    "PYPeRz0CgYALHCj/Ji8XSsDoF/MhVhnGdIs2P99NNdmo3R2Pv0CuZbDKMU559LJH",
    "UvrKS8WkuWRDuKrz1W/EQKApFjDGpdqToZqriUFQzwy7mR3ayIiogzNtHcvbDHx8",
    "oFnGY0OFksX/ye0/XGpy2SFxYRwGU98HPYeBvAQQrVjdkzfy7BmXQQ==",
);

fn jwt_policy() -> TestResult<JwtPolicy> {
    Ok(JwtPolicy::from_json(&format!(
        r#"{{"version":1,"issuer":"https://issuer.example/","audience":"urn:switchyard:tenant","keys":[{{"kid":"key-1","kty":"RSA","alg":"RS256","use":"sig","n":"{MODULUS}","e":"AQAB"}}],"bindings":[{{"subject":"producer","scope":"{AUDIENCE}","permissions":["send"]}}]}}"#
    ))?)
}
fn authentication(enabled: bool) -> TestResult<protocol_amqp::SharedAccessAuthentication> {
    let policy = SharedAccessPolicy::new([SharedAccessRule::new(
        "producer",
        ResourceScope::parse(AUDIENCE)?,
        SharedAccessKey::new("secret")?,
        None,
        PermissionSet::LISTEN,
    )?])?;
    let config = protocol_amqp::SharedAccessAuthentication::new(policy, HOST)?;
    Ok(if enabled {
        config.with_offline_jwt_policy(jwt_policy()?)
    } else {
        config
    })
}
fn empty_sas_authentication() -> TestResult<protocol_amqp::SharedAccessAuthentication> {
    Ok(
        protocol_amqp::SharedAccessAuthentication::new(SharedAccessPolicy::new([])?, HOST)?
            .with_offline_jwt_policy(jwt_policy()?),
    )
}
fn epoch() -> TestResult<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}
fn jwt_token(issuer: &str) -> TestResult<String> {
    let now = epoch()?;
    let header = r#"{"alg":"RS256","kid":"key-1","typ":"at+jwt"}"#;
    let claims = format!(
        r#"{{"iss":"{issuer}","sub":"producer","aud":"urn:switchyard:tenant","iat":{now},"exp":{}}}"#,
        now.checked_add(120).ok_or("fixture epoch overflow")?
    );
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header),
        URL_SAFE_NO_PAD.encode(claims)
    );
    let provider = ring::default_provider();
    let key = provider
        .key_provider
        .load_private_key(PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(
            STANDARD.decode(PRIVATE_DER)?,
        )))?;
    let signer = key
        .choose_scheme(&[SignatureScheme::RSA_PKCS1_SHA256])
        .ok_or("public fixture RS256 signing unavailable")?;
    Ok(format!(
        "{input}.{}",
        URL_SAFE_NO_PAD.encode(signer.sign(input.as_bytes())?)
    ))
}
fn percent_encoded(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("%{byte:02X}")).collect()
}
fn sas_token() -> TestResult<String> {
    let resource = percent_encoded(AUDIENCE.as_bytes());
    let expiry = epoch()?.checked_add(120).ok_or("fixture epoch overflow")?;
    let mut hmac = Hmac::<Sha256>::new_from_slice(b"secret")?;
    hmac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(hmac.finalize().into_bytes());
    Ok(format!(
        "SharedAccessSignature sr={resource}&sig={}&se={expiry}&skn=producer",
        percent_encoded(signature.as_bytes())
    ))
}
trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}
type Socket = WebSocketStream<Box<dyn Io>>;
struct ClientWebSocketState {
    socket: Socket,
    peer_close_received: bool,
}

struct ClientWebSocket {
    returned: tokio::sync::oneshot::Receiver<ClientWebSocketState>,
    state: Option<ClientWebSocketState>,
}

impl ClientWebSocket {
    async fn finish(&mut self) -> TestResult {
        if self.state.is_none() {
            let returned = (&mut self.returned)
                .await
                .map_err(|_| io::Error::other("original client WebSocket was not returned"))?;
            self.state = Some(returned);
        }
        let state = self
            .state
            .as_mut()
            .expect("original client WebSocket retained");
        match SinkExt::close(&mut state.socket).await {
            Ok(()) => (),
            Err(
                tokio_tungstenite::tungstenite::Error::ConnectionClosed
                | tokio_tungstenite::tungstenite::Error::AlreadyClosed,
            ) if state.peer_close_received => (),
            Err(_) => return Err(io::Error::other("client WebSocket close write failed").into()),
        }
        while !state.peer_close_received {
            match state.socket.next().await {
                Some(Ok(WsMessage::Close(_))) => state.peer_close_received = true,
                Some(Ok(WsMessage::Binary(_) | WsMessage::Ping(_) | WsMessage::Pong(_))) => (),
                Some(Ok(_)) => {
                    return Err(
                        io::Error::other("unexpected client WebSocket close data kind").into(),
                    );
                }
                Some(Err(_)) => {
                    return Err(io::Error::other("client WebSocket close exchange failed").into());
                }
                None => {
                    return Err(io::Error::other("actual peer WebSocket Close required").into());
                }
            }
        }
        match state.socket.flush().await {
            Ok(())
            | Err(
                tokio_tungstenite::tungstenite::Error::ConnectionClosed
                | tokio_tungstenite::tungstenite::Error::AlreadyClosed,
            ) => (),
            Err(_) => return Err(io::Error::other("client WebSocket close flush failed").into()),
        }
        state.socket.get_mut().shutdown().await?;
        Ok(())
    }
}

struct BinaryStream {
    socket: Option<Socket>,
    returned: Option<tokio::sync::oneshot::Sender<ClientWebSocketState>>,
    bytes: Vec<u8>,
    position: usize,
    eof: bool,
    peer_close_received: bool,
}

impl BinaryStream {
    fn new(socket: Socket, returned: tokio::sync::oneshot::Sender<ClientWebSocketState>) -> Self {
        Self {
            socket: Some(socket),
            returned: Some(returned),
            bytes: Vec::new(),
            position: 0,
            eof: false,
            peer_close_received: false,
        }
    }

    fn socket(&mut self) -> &mut Socket {
        self.socket
            .as_mut()
            .expect("original fixture WebSocket before Drop")
    }
}

impl Drop for BinaryStream {
    fn drop(&mut self) {
        if let (Some(socket), Some(returned)) = (self.socket.take(), self.returned.take()) {
            let _ = returned.send(ClientWebSocketState {
                socket,
                peer_close_received: self.peer_close_received,
            });
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
            match std::task::ready!(Pin::new(self.socket()).poll_next(cx)) {
                Some(Ok(WsMessage::Binary(bytes))) => {
                    self.bytes = bytes.to_vec();
                    self.position = 0;
                }
                Some(Ok(WsMessage::Close(_))) => {
                    self.peer_close_received = true;
                    self.eof = true;
                }
                None => self.eof = true,
                Some(Ok(WsMessage::Ping(_) | WsMessage::Pong(_))) => {}
                Some(Ok(_)) => {
                    return Poll::Ready(Err(io::Error::other("unexpected WebSocket data kind")));
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
        std::task::ready!(Pin::new(self.socket()).poll_ready(cx)).map_err(websocket_io)?;
        let length = bytes.len().min(16 * 1024);
        Pin::new(self.socket())
            .start_send(WsMessage::Binary(bytes[..length].to_vec().into()))
            .map_err(websocket_io)?;
        Poll::Ready(Ok(length))
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.socket()).poll_flush(cx).map_err(websocket_io)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.socket()).poll_close(cx).map_err(websocket_io)
    }
}

struct Node<P: StoreProvider> {
    store: P::Store,
    namespace: NamespaceName,
    _broker: Broker,
    address: std::net::SocketAddr,
    certificate: CertificateDer<'static>,
    websocket: bool,
    client_websocket: Option<ClientWebSocket>,
    listener: Option<JoinHandle<io::Result<()>>>,
    pending: Option<(TcpListener, protocol_amqp::AmqpListener<BrokerHandle>)>,
    owner: Option<protocol_amqp::RetainedConnectionOwner<()>>,
    start_error: Option<protocol_amqp::RetainedConnectionStartError<BrokerHandle>>,
}
struct Cleanup {
    client_websocket: Option<Observed>,
    listener: Option<Result<io::Result<()>, JoinError>>,
    retained: Option<protocol_amqp::RetainedConnectionJoinReport<()>>,
    start_error: Option<protocol_amqp::RetainedConnectionStartError<BrokerHandle>>,
}
impl<P: StoreProvider> Node<P> {
    async fn start(
        store: P::Store,
        websocket: bool,
        retained: bool,
        enabled: bool,
        empty_sas: bool,
    ) -> TestResult<Self> {
        let namespace = NamespaceName::new(format!(
            "tenant-{websocket}-{retained}-{enabled}-{empty_sas}"
        ))?;
        let machine = StateMachine::new(store.clone());
        for name in ["orders", "orders-archive"] {
            machine.apply(&Command::new(
                namespace.clone(),
                EntityPath::new(name)?,
                Timestamp::from_millis(1000),
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            ))?;
        }
        let broker = Broker::spawn(LocalProposer::new(machine, ManualClock::at(1000)));
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec!["localhost".into()])?;
        let tls = protocol_amqp::tls_server_config(
            cert.pem().as_bytes(),
            key_pair.serialize_pem().as_bytes(),
        )?;
        let mut configured = protocol_amqp::AmqpListener::new(broker.handle(), namespace.clone())
            .with_tls(tls)
            .with_shared_access_authentication(if empty_sas {
                empty_sas_authentication()?
            } else {
                authentication(enabled)?
            })
            .with_handshake_timeout(DEADLINE);
        if websocket {
            configured = configured.with_websocket();
        }
        let tcp = TcpListener::bind("127.0.0.1:0").await?;
        let address = tcp.local_addr()?;
        let (listener, pending) = if retained {
            (None, Some((tcp, configured)))
        } else {
            (Some(tokio::spawn(configured.serve(tcp))), None)
        };
        Ok(Self {
            store,
            namespace,
            _broker: broker,
            address,
            certificate: cert.der().clone(),
            websocket,
            client_websocket: None,
            listener,
            pending,
            owner: None,
            start_error: None,
        })
    }
    async fn transport(&mut self, trust: bool) -> TestResult<Box<dyn Io>> {
        let peer = if let Some((listener, configured)) = self.pending.take() {
            let (peer, accepted) =
                tokio::join!(TcpStream::connect(self.address), listener.accept());
            let peer = peer?;
            let (stream, _) = accepted?;
            let (owner, starter) =
                protocol_amqp::RetainedConnectionOwner::new(tokio::runtime::Handle::current(), ());
            self.owner = Some(owner);
            if let Err(error) = configured.start_retained_connection(stream, starter) {
                self.start_error = Some(error);
                return Err(io::Error::other("valid retained fixture start refused").into());
            }
            peer
        } else {
            TcpStream::connect(self.address).await?
        };
        let mut roots = RootCertStore::empty();
        if trust {
            roots.add(self.certificate.clone())?;
        }
        let tls = TlsConnector::from(Arc::new(
            ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
                .with_protocol_versions(&[&TLS13, &TLS12])?
                .with_root_certificates(roots)
                .with_no_client_auth(),
        ))
        .connect(ServerName::try_from("localhost")?, peer)
        .await?;
        Ok(Box::new(tls))
    }
    async fn connect(&mut self) -> TestResult<ClientConnection> {
        let stream = self.transport(true).await?;
        let stream: Box<dyn Io> = if self.websocket {
            let mut request =
                format!("ws://localhost:{}{PATH}", self.address.port()).into_client_request()?;
            request
                .headers_mut()
                .insert("Sec-WebSocket-Protocol", "amqp".parse()?);
            if self.client_websocket.is_some() {
                return Err("original client WebSocket holder already installed".into());
            }
            let (socket, response) = client_async(request, stream).await?;
            let (returned, receiver) = tokio::sync::oneshot::channel();
            self.client_websocket = Some(ClientWebSocket {
                returned: receiver,
                state: None,
            });
            let stream = BinaryStream::new(socket, returned);
            assert_eq!(response.status().as_u16(), 101);
            Box::new(stream)
        } else {
            stream
        };
        Ok(ClientConnection::open(
            stream,
            "offline-jwt-client",
            Some(SaslInit {
                mechanism: "ANONYMOUS".into(),
                initial_response: None,
                hostname: Some(HOST.into()),
            }),
        )
        .await?
        .with_close_timeout(DEADLINE))
    }
    fn counters(&self, entity: &str) -> TestResult<Option<QueueCounters>> {
        let value = self.store.get(&keys::queue_counters(
            &self.namespace,
            &EntityPath::new(entity)?,
        ))?;
        Ok(value
            .map(|value| domain::codec::decode(&value))
            .transpose()?)
    }
    async fn stop(mut self) -> Cleanup {
        let client_websocket = match self.client_websocket.as_mut() {
            Some(holder) => Some(
                AssertUnwindSafe(timeout(DEADLINE, holder.finish()))
                    .catch_unwind()
                    .await,
            ),
            None => None,
        };
        let retained = if let Some(owner) = self.owner.as_mut() {
            owner.stop();
            owner.finish().await
        } else {
            None
        };
        let listener = if let Some(listener) = self.listener.take() {
            listener.abort();
            Some(listener.await)
        } else {
            None
        };
        Cleanup {
            client_websocket,
            listener,
            retained,
            start_error: self.start_error.take(),
        }
    }
}
async fn finish<P: StoreProvider>(
    node: Node<P>,
    connection: Option<ClientConnection>,
    observed: Observed,
    require_retained: bool,
) -> TestResult {
    let closing = AssertUnwindSafe(async {
        if let Some(connection) = connection.as_ref() {
            connection.close().await?;
        }
        Ok::<(), Box<dyn Error>>(())
    })
    .catch_unwind()
    .await;
    let require_websocket = node.websocket;
    let cleanup = node.stop().await;
    match observed {
        Err(payload) => resume_unwind(payload),
        Ok(result) => result??,
    }
    match closing {
        Err(payload) => resume_unwind(payload),
        Ok(result) => result?,
    }
    assert_eq!(cleanup.client_websocket.is_some(), require_websocket);
    if let Some(closed) = cleanup.client_websocket {
        match closed {
            Err(payload) => resume_unwind(payload),
            Ok(result) => result??,
        }
    }
    assert!(cleanup.start_error.is_none());
    if let Some(joined) = &cleanup.listener {
        match joined {
            Ok(result) => {
                result
                    .as_ref()
                    .map_err(|_| io::Error::other("listener failed"))?;
            }
            Err(error) if error.is_cancelled() => (),
            Err(_) => return Err(io::Error::other("original listener task panicked").into()),
        }
    }
    if require_retained {
        let report = cleanup
            .retained
            .as_ref()
            .ok_or("original retained report required")?;
        assert!(matches!(report.wrapper(), Some(Ok(()))));
        assert!(matches!(report.actor(), Some(Ok(()))));
        let peer = report
            .native_observations()
            .peer_close()
            .ok_or("actual peer Close observation required")?;
        assert_eq!(peer.channel(), 0);
        assert!(
            peer.payload().is_empty() && peer.close().error.is_none() && !peer.locally_closing()
        );
        assert_eq!(peer.reply_state(), amqp::ServerPeerCloseReplyState::Ready);
        assert!(matches!(peer.reply_result(), Some(Ok(()))));
        match (report.reader(), report.native_observations().reader()) {
            (Some(Ok(())), _) => (),
            (Some(Err(error)), Some(reader)) => {
                assert!(error.is_cancelled() && error.id() == reader.id());
                assert!(
                    reader.requested_by(amqp::ServerConnectionAbortSource::ActorReaderShutdown)
                );
            }
            _ => return Err("original Reader completion required".into()),
        }
        assert!(matches!(
            report.outcomes().primary,
            Some(protocol_amqp::RetainedConnectionOutcome::Finished(Ok(())))
        ));
        if let Some(closed) = &report.outcomes().websocket_close {
            assert!(closed.is_ok(), "actual WebSocket close outcome: {closed:?}");
        }
    }
    Ok(())
}
async fn put_token(
    session: &mut ClientSession,
    number: u32,
    token_type: &str,
    token: String,
    audience: &str,
) -> TestResult<i32> {
    let address = format!("cbs-reply-{number}");
    let mut responses = ClientReceiver::builder()
        .name(format!("cbs-response-{number}"))
        .source(protocol_amqp::CBS_NODE)
        .target(address.clone())
        .attach(session)
        .await?;
    let mut requests = ClientSender::attach(
        session,
        format!("cbs-request-{number}"),
        protocol_amqp::CBS_NODE,
    )
    .await?;
    let id = format!("cbs-{number}");
    let request = Message::builder()
        .properties(Properties {
            message_id: Some(id.clone().into()),
            reply_to: Some(address),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert("operation", "put-token".to_owned())
                .insert("type", token_type.to_owned())
                .insert("name", audience.to_owned())
                .build(),
        )
        .body(Body::Value(Value::String(token)))
        .build();
    assert!(matches!(
        requests.send(request).await?,
        Outcome::Accepted(_)
    ));
    let response = responses.recv().await?;
    assert_eq!(
        response
            .message()
            .properties
            .as_ref()
            .and_then(|props| props.correlation_id.clone()),
        Some(id.into())
    );
    let Some(Value::Int(status)) = response
        .message()
        .application_properties
        .as_ref()
        .and_then(|props| props.get("status-code"))
    else {
        return Err("typed CBS status required".into());
    };
    let status = *status;
    responses.accept(&response).await?;
    requests.close().await?;
    responses.close().await?;
    Ok(status)
}
async fn send(session: &mut ClientSession, name: &str) -> TestResult {
    let mut sender = ClientSender::attach(session, name, "orders").await?;
    assert!(matches!(
        sender
            .send(
                Message::builder()
                    .properties(Properties {
                        message_id: Some(name.to_owned().into()),
                        ..Properties::default()
                    })
                    .body(Body::Data(vec![name.as_bytes().to_vec().into()]))
                    .build()
            )
            .await?,
        Outcome::Accepted(_)
    ));
    sender.close().await?;
    Ok(())
}
async fn tls_and_wss_jwt_scope_refresh_and_sas_coexist<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let store = provider.open()?;
    for websocket in [false, true] {
        for retained in [false, true] {
            let mut node =
                Node::<P>::start(store.clone(), websocket, retained, true, false).await?;
            let mut connection = None;
            let observed = AssertUnwindSafe(timeout(DEADLINE, async {
                connection = Some(node.connect().await?);
                let mut session = connection
                    .as_mut()
                    .expect("original client")
                    .begin()
                    .await?;
                assert_eq!(
                    put_token(
                        &mut session,
                        1,
                        "jwt",
                        jwt_token("https://issuer.example/")?,
                        AUDIENCE
                    )
                    .await?,
                    202
                );
                send(&mut session, "jwt-first").await?;
                let mut denied =
                    ClientReceiver::attach(&mut session, "no-listen", "orders").await?;
                assert!(denied.source().is_none());
                assert!(matches!(
                    denied.recv().await,
                    Err(EngineError::RemoteDetached)
                ));
                assert_eq!(
                    put_token(
                        &mut session,
                        2,
                        "jwt",
                        jwt_token("https://other-issuer.example/")?,
                        AUDIENCE
                    )
                    .await?,
                    401
                );
                assert_eq!(
                    put_token(
                        &mut session,
                        3,
                        "jwt",
                        jwt_token("https://issuer.example/")?,
                        "amqps://tenant.example/orders-archive"
                    )
                    .await?,
                    401
                );
                assert_eq!(
                    put_token(
                        &mut session,
                        4,
                        "servicebus.windows.net:sastoken",
                        sas_token()?,
                        AUDIENCE
                    )
                    .await?,
                    202
                );
                assert_eq!(
                    put_token(
                        &mut session,
                        5,
                        "jwt",
                        jwt_token("https://issuer.example/")?,
                        AUDIENCE
                    )
                    .await?,
                    202
                );
                send(&mut session, "jwt-refreshed").await?;
                let mut sibling =
                    ClientReceiver::attach(&mut session, "no-sibling", "orders-archive").await?;
                assert!(sibling.source().is_none());
                assert!(matches!(
                    sibling.recv().await,
                    Err(EngineError::RemoteDetached)
                ));
                let mut receiver =
                    ClientReceiver::attach(&mut session, "sas-listen", "orders").await?;
                for expected in ["jwt-first", "jwt-refreshed"] {
                    let delivery = receiver.recv().await?;
                    assert_eq!(
                        delivery.message().body,
                        Body::Data(vec![expected.as_bytes().to_vec().into()])
                    );
                    receiver.accept(&delivery).await?;
                }
                receiver.close().await?;
                assert_eq!(node.counters("orders-archive")?, None);
                session.end().await?;
                Ok::<(), Box<dyn Error>>(())
            }))
            .catch_unwind()
            .await;
            finish(node, connection, observed, retained).await?;
        }
    }
    for websocket in [false, true] {
        let mut node = Node::<P>::start(store.clone(), websocket, true, true, true).await?;
        let mut connection = None;
        let observed = AssertUnwindSafe(timeout(DEADLINE, async {
            connection = Some(node.connect().await?);
            let mut session = connection
                .as_mut()
                .expect("original empty-SAS client")
                .begin()
                .await?;
            assert_eq!(
                put_token(
                    &mut session,
                    1,
                    "jwt",
                    jwt_token("https://issuer.example/")?,
                    AUDIENCE
                )
                .await?,
                202
            );
            send(&mut session, "empty-sas-jwt").await?;
            let mut denied =
                ClientReceiver::attach(&mut session, "empty-sas-no-listen", "orders").await?;
            assert!(denied.source().is_none());
            assert!(matches!(
                denied.recv().await,
                Err(EngineError::RemoteDetached)
            ));
            assert_eq!(
                node.counters("orders")?,
                Some(QueueCounters {
                    next_sequence: 2,
                    ..QueueCounters::default()
                })
            );
            assert_eq!(node.counters("orders-archive")?, None);
            session.end().await?;
            Ok::<(), Box<dyn Error>>(())
        }))
        .catch_unwind()
        .await;
        finish(node, connection, observed, true).await?;
    }
    Ok(())
}
async fn disabled_jwt_and_unknown_type_do_not_replace_sas<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let store = provider.open()?;
    for websocket in [false, true] {
        let mut node = Node::<P>::start(store.clone(), websocket, true, false, false).await?;
        let mut connection = None;
        let observed = AssertUnwindSafe(timeout(DEADLINE, async {
            connection = Some(node.connect().await?);
            let mut session = connection
                .as_mut()
                .expect("original client")
                .begin()
                .await?;
            assert_eq!(
                put_token(&mut session, 1, "jwt", "not-a-token".into(), AUDIENCE).await?,
                401
            );
            assert_eq!(
                put_token(
                    &mut session,
                    2,
                    "unsupported",
                    "not-a-token".into(),
                    AUDIENCE
                )
                .await?,
                400
            );
            assert_eq!(
                put_token(
                    &mut session,
                    3,
                    "servicebus.windows.net:sastoken",
                    sas_token()?,
                    AUDIENCE
                )
                .await?,
                202
            );
            let receiver = ClientReceiver::attach(&mut session, "sas-listen", "orders").await?;
            receiver.close().await?;
            assert_eq!(node.counters("orders")?, None);
            session.end().await?;
            Ok::<(), Box<dyn Error>>(())
        }))
        .catch_unwind()
        .await;
        finish(node, connection, observed, true).await?;
    }
    Ok(())
}
#[derive(Clone, Copy)]
enum PublicMode {
    Ordinary,
    Posting,
    Messaging,
}
fn plain_listener(
    handle: BrokerHandle,
    namespace: &NamespaceName,
    websocket: bool,
    empty_sas: bool,
) -> TestResult<protocol_amqp::AmqpListener<BrokerHandle>> {
    let mut listener = protocol_amqp::AmqpListener::new(handle, namespace.clone())
        .with_shared_access_authentication(if empty_sas {
            empty_sas_authentication()?
        } else {
            authentication(true)?
        });
    if websocket {
        listener = listener.with_websocket();
    }
    Ok(listener)
}
fn setup_refusal(
    error: protocol_amqp::RetainedConnectionStartError<BrokerHandle>,
    original_address: std::net::SocketAddr,
    original_nodelay: bool,
) -> TestResult {
    let (cause, request) = error.into_parts();
    let protocol_amqp::RetainedConnectionStartCause::Setup(error) = cause else {
        return Err("original typed Setup refusal required".into());
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "offline JWT authentication requires a TLS listener"
    );
    assert_eq!(request.stream.local_addr()?, original_address);
    assert_eq!(request.stream.nodelay()?, original_nodelay);
    Ok(())
}
async fn tcp_pair() -> TestResult<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let (peer, server) = tokio::join!(
        TcpStream::connect(listener.local_addr()?),
        listener.accept()
    );
    Ok((peer?, server?.0))
}
async fn plaintext_jwt_public_entries_refuse_before_accept_or_claim<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let store = provider.open()?;
    let namespace = NamespaceName::new("tenant")?;
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(store.clone()),
        ManualClock::at(1000),
    ));
    let before = store.snapshot()?;
    for empty_sas in [false, true] {
        for websocket in [false, true] {
            for mode in [
                PublicMode::Ordinary,
                PublicMode::Posting,
                PublicMode::Messaging,
            ] {
                let configured = plain_listener(broker.handle(), &namespace, websocket, empty_sas)?;
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let result = timeout(DEADLINE, async {
                    match mode {
                        PublicMode::Ordinary => configured.serve(listener).await,
                        PublicMode::Posting => {
                            configured.serve_atomic_posting_ingress(listener).await
                        }
                        PublicMode::Messaging => {
                            configured.serve_atomic_messaging_ingress(listener).await
                        }
                    }
                })
                .await?;
                let error = result.expect_err("TLS required before accepting any socket");
                assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                assert_eq!(
                    error.to_string(),
                    "offline JWT authentication requires a TLS listener"
                );
                let (peer, stream) = tcp_pair().await?;
                let original_address = stream.local_addr()?;
                let original_nodelay = stream.nodelay()?;
                let (mut owner, starter) = protocol_amqp::RetainedConnectionOwner::new(
                    tokio::runtime::Handle::current(),
                    (),
                );
                let configured = plain_listener(broker.handle(), &namespace, websocket, empty_sas)?;
                let result = match mode {
                    PublicMode::Ordinary => configured.start_retained_connection(stream, starter),
                    PublicMode::Posting => {
                        configured.start_retained_atomic_posting_ingress(stream, starter)
                    }
                    PublicMode::Messaging => {
                        configured.start_retained_atomic_messaging_ingress(stream, starter)
                    }
                };
                let report = owner
                    .finish()
                    .await
                    .ok_or("original cold owner report required")?;
                assert!(
                    report.wrapper().is_none()
                        && report.actor().is_none()
                        && report.reader().is_none()
                );
                setup_refusal(
                    result.expect_err("TLS required before original starter claim"),
                    original_address,
                    original_nodelay,
                )?;
                drop(peer);
            }
            let (peer, stream) = tcp_pair().await?;
            let original_address = stream.local_addr()?;
            let original_nodelay = stream.nodelay()?;
            let limits = protocol_amqp::RetainedAtomicMessagingLimits::new(1, 1)?;
            let (mut owner, starter) = protocol_amqp::RetainedAtomicMessagingOwner::<
                (),
                BrokerHandle,
            >::new(
                tokio::runtime::Handle::current(), limits, ()
            )
            .map_err(|_| io::Error::other("valid cold collected root refused"))?;
            let result = plain_listener(broker.handle(), &namespace, websocket, empty_sas)?
                .start_retained_collected_atomic_messaging(stream, starter);
            let report = owner
                .finish()
                .await
                .ok_or("original cold collected report required")?;
            assert!(
                report.socket().wrapper().is_none()
                    && report.socket().actor().is_none()
                    && report.socket().reader().is_none()
            );
            setup_refusal(
                result.expect_err("TLS required before collected starter claim"),
                original_address,
                original_nodelay,
            )?;
            drop(peer);
        }
    }
    assert!(store.snapshot()? == before);
    Ok(())
}
async fn failed_tls_handshake_never_creates_native_authorization<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = Node::<P>::start(provider.open()?, false, true, true, false).await?;
    let before = node.store.snapshot()?;
    let observed = AssertUnwindSafe(timeout(DEADLINE, async {
        let rejected = node.transport(false).await;
        assert!(
            rejected.is_err(),
            "a client without certificate trust cannot open TLS"
        );
        assert!(node.store.snapshot()? == before);
        Ok::<(), Box<dyn Error>>(())
    }))
    .catch_unwind()
    .await;
    let cleanup = node.stop().await;
    match observed {
        Err(payload) => resume_unwind(payload),
        Ok(result) => result??,
    }
    assert!(cleanup.start_error.is_none());
    let report = cleanup
        .retained
        .as_ref()
        .ok_or("actual TLS failure report required")?;
    assert!(matches!(report.wrapper(), Some(Ok(()))));
    assert!(report.actor().is_none() && report.reader().is_none());
    assert!(matches!(
        report.outcomes().primary,
        Some(protocol_amqp::RetainedConnectionOutcome::Finished(Err(_)))
    ));
    Ok(())
}
macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::new()).await })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?).await })+ }
    };
}
for_each_backend!(
    tls_and_wss_jwt_scope_refresh_and_sas_coexist,
    disabled_jwt_and_unknown_type_do_not_replace_sas,
    plaintext_jwt_public_entries_refuse_before_accept_or_claim,
    failed_tls_handshake_never_creates_native_authorization
);
