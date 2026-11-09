use std::{
    error::Error,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use amqp::{ClientConnection as Connection, SaslInit, Symbol};
use auth::{
    PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule,
    workload::{PreparedWorkloadTls, WorkloadPolicy},
};
use domain::{CommandKind, CommandOutcome, EntityBinding, EntityPath, NamespaceName};
use rcgen::{
    Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    SanType,
};
use rustls::{
    ClientConfig, RootCertStore,
    crypto::ring,
    pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName},
};
use serde_json::{Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    time::{Instant, timeout},
};
use tokio_rustls::{TlsConnector, client::TlsStream};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use super::super::{test_support::WAIT, *};
use crate::{BrokerRejection, SharedAccessAuthentication, websocket::WebSocketIo};

pub(super) const HOST: &str = "tenant.servicebus.windows.net";
pub(super) const ROOT: &str = "amqps://tenant.servicebus.windows.net";
pub(super) const RESOURCE: &str = "amqps://tenant.servicebus.windows.net/orders";
pub(super) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub(super) struct Identity {
    pub(super) cert: Certificate,
    pub(super) key: KeyPair,
}

#[derive(Debug)]
struct MalformedClient(Arc<rustls::sign::CertifiedKey>);
impl rustls::client::ResolvesClientCert for MalformedClient {
    fn resolve(
        &self,
        _: &[&[u8]],
        _: &[rustls::SignatureScheme],
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(self.0.clone())
    }
    fn has_certs(&self) -> bool {
        true
    }
}

fn root() -> Identity {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let key = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    Identity { cert, key }
}

fn signed(params: CertificateParams, issuer: &Identity) -> Identity {
    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, &issuer.cert, &issuer.key).unwrap();
    Identity { cert, key }
}

pub(super) struct TlsFixture {
    ca: Identity,
    server_ca: Identity,
    server: Identity,
    pub(super) sender: Identity,
    pub(super) listener: Identity,
    pub(super) manager: Identity,
    pub(super) short: Identity,
}

impl TlsFixture {
    pub(super) fn new(short_lifetime: u64) -> Self {
        let ca = root();
        let server_ca = root();
        let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server = signed(params, &server_ca);
        let sender = Self::identity(&ca, "sender", None);
        let listener = Self::identity(&ca, "listener", None);
        let manager = Self::identity(&ca, "manager", None);
        let short = Self::identity(&ca, "short", Some(now() + short_lifetime));
        Self {
            ca,
            server_ca,
            server,
            sender,
            listener,
            manager,
            short,
        }
    }
    fn identity(ca: &Identity, principal: &str, expiry: Option<u64>) -> Identity {
        let mut params = CertificateParams::default();
        params.subject_alt_names = vec![SanType::URI(
            format!("spiffe://example.test/{principal}")
                .try_into()
                .unwrap(),
        )];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        if let Some(expiry) = expiry {
            params.not_after = time::OffsetDateTime::from_unix_timestamp(expiry as i64).unwrap();
        }
        signed(params, ca)
    }
    pub(super) fn untrusted(&self) -> Identity {
        Self::identity(&root(), "sender", None)
    }
    pub(super) fn unbound(&self) -> Identity {
        Self::identity(&self.ca, "unbound", None)
    }
    pub(super) fn bad_usage(&self) -> Identity {
        let mut params = CertificateParams::default();
        params.subject_alt_names = vec![
            SanType::URI("spiffe://example.test/sender".try_into().unwrap()),
            SanType::DnsName("example.test".try_into().unwrap()),
        ];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        signed(params, &self.ca)
    }
    pub(super) fn config(&self, optional: bool, cap: u64) -> Value {
        json!({"version":1,"trust_domain":"example.test","roots_pem":[self.ca.cert.pem()],
        "client_auth":if optional {"optional"} else {"required"},"max_connection_seconds":cap,
        "bindings":[
            {"principal":"spiffe://example.test/sender","scope":RESOURCE,"permissions":["send"]},
            {"principal":"spiffe://example.test/listener","scope":RESOURCE,"permissions":["listen"]},
            {"principal":"spiffe://example.test/manager","scope":ROOT,"permissions":["manage"]},
            {"principal":"spiffe://example.test/short","scope":RESOURCE,"permissions":["send"]}
        ]})
    }
    pub(super) fn endpoint(&self, optional: bool, cap: u64) -> PreparedWorkloadTls {
        self.endpoint_config(self.config(optional, cap), HOST)
    }
    pub(super) fn endpoint_config(&self, config: Value, host: &str) -> PreparedWorkloadTls {
        let policy =
            WorkloadPolicy::from_json(&config.to_string(), ResourceScope::namespace(host).unwrap())
                .unwrap();
        PreparedWorkloadTls::new(
            policy,
            self.server.cert.pem().as_bytes(),
            self.server.key.serialize_pem().as_bytes(),
            vec![],
            std::time::Duration::from_secs(30),
        )
        .unwrap()
    }
    pub(super) fn legacy_tls(&self) -> rustls::ServerConfig {
        crate::tls_server_config(
            self.server.cert.pem().as_bytes(),
            self.server.key.serialize_pem().as_bytes(),
        )
        .unwrap()
    }
    pub(super) fn client(&self, identity: Option<&Identity>) -> TlsConnector {
        let mut roots = RootCertStore::empty();
        roots.add(self.server_ca.cert.der().clone()).unwrap();
        let builder = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots);
        let config = match identity {
            Some(id) => builder
                .with_client_auth_cert(
                    vec![id.cert.der().clone()],
                    PrivatePkcs8KeyDer::from(id.key.serialize_der()).into(),
                )
                .unwrap(),
            None => builder.with_no_client_auth(),
        };
        TlsConnector::from(Arc::new(config))
    }
    pub(super) fn malformed_client(&self) -> TlsConnector {
        let mut roots = RootCertStore::empty();
        roots.add(self.server_ca.cert.der().clone()).unwrap();
        let key = ring::sign::any_supported_type(
            &PrivatePkcs8KeyDer::from(self.sender.key.serialize_der()).into(),
        )
        .unwrap();
        let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_client_cert_resolver(Arc::new(MalformedClient(Arc::new(
                rustls::sign::CertifiedKey::new(vec![CertificateDer::from(vec![0; 100])], key),
            ))));
        TlsConnector::from(Arc::new(config))
    }
    pub(super) async fn tls(
        &self,
        address: SocketAddr,
        identity: Option<&Identity>,
    ) -> Result<TlsStream<TcpStream>, Box<dyn Error + Send + Sync>> {
        let socket = TcpStream::connect(address).await?;
        Ok(self
            .client(identity)
            .connect(ServerName::try_from("localhost").unwrap(), socket)
            .await?)
    }
    pub(super) async fn connect(
        &self,
        address: SocketAddr,
        websocket: bool,
        identity: Option<&Identity>,
    ) -> Result<Connection, Box<dyn Error + Send + Sync>> {
        self.connect_with_connector(address, websocket, self.client(identity))
            .await
    }
    pub(super) async fn connect_with_connector(
        &self,
        address: SocketAddr,
        websocket: bool,
        connector: TlsConnector,
    ) -> Result<Connection, Box<dyn Error + Send + Sync>> {
        let stream = connector
            .connect(
                ServerName::try_from("localhost").unwrap(),
                TcpStream::connect(address).await?,
            )
            .await?;
        let builder = Connection::builder()
            .container_id("workload-test")
            .sasl(SaslInit {
                mechanism: Symbol::from("ANONYMOUS"),
                initial_response: None,
                hostname: Some("foreign.example".into()),
            });
        if websocket {
            let mut request =
                format!("wss://{address}/$servicebus/websocket").into_client_request()?;
            request
                .headers_mut()
                .insert("sec-websocket-protocol", "AMQPWSB10".parse().unwrap());
            let (socket, _) = tokio_tungstenite::client_async(request, stream).await?;
            Ok(builder.open_with_stream(WebSocketIo::new(socket)).await?)
        } else {
            Ok(builder.open_with_stream(stream).await?)
        }
    }
}

pub(super) fn alternative(jwt: bool) -> SharedAccessAuthentication {
    let rule = SharedAccessRule::new(
        "sas",
        ResourceScope::namespace(HOST).unwrap(),
        SharedAccessKey::new("secret").unwrap(),
        None,
        PermissionSet::MANAGE,
    )
    .unwrap();
    let auth =
        SharedAccessAuthentication::new(SharedAccessPolicy::new([rule]).unwrap(), HOST).unwrap();
    if jwt {
        auth.with_jwt_policy(super::super::jwt_tests::policy())
    } else {
        auth
    }
}

#[derive(Clone)]
pub(super) struct HandleBroker(pub(super) server::BrokerHandle);

fn rejection(error: server::SubmitError) -> BrokerRejection {
    match error {
        server::SubmitError::Propose(server::ProposeError::Broker(error)) => {
            BrokerRejection::Refused(error)
        }
        other => BrokerRejection::Unavailable(other.to_string()),
    }
}

// Unit tests have their own protocol crate identity; use the real handle's
// inherent methods rather than its external-library Broker implementation.
impl Broker for HandleBroker {
    async fn bind_entity(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<EntityBinding, BrokerRejection> {
        self.0
            .bind_entity(namespace, entity)
            .await
            .map_err(rejection)
    }
    async fn submit_bound(
        &self,
        binding: EntityBinding,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.0.submit_bound(binding, kind).await.map_err(rejection)
    }
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.0
            .submit(namespace, entity, kind)
            .await
            .map_err(rejection)
    }
    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        std::future::pending().await
    }
}

#[derive(Clone)]
pub(super) struct Recording<B> {
    pub(super) inner: B,
    pub(super) calls: Arc<Mutex<Vec<CommandKind>>>,
    pub(super) binds: Arc<Mutex<Vec<EntityPath>>>,
}
impl<B: Broker> Recording<B> {
    pub(super) fn new(inner: B) -> Self {
        Self {
            inner,
            calls: Arc::default(),
            binds: Arc::default(),
        }
    }
}
impl<B: Broker> Broker for Recording<B> {
    fn bind_entity(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> impl Future<Output = Result<EntityBinding, BrokerRejection>> + Send {
        self.binds.lock().unwrap().push(entity.clone());
        self.inner.bind_entity(namespace, entity)
    }
    fn submit_bound(
        &self,
        binding: EntityBinding,
        kind: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        self.calls.lock().unwrap().push(kind.clone());
        self.inner.submit_bound(binding, kind)
    }
    fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        self.calls.lock().unwrap().push(kind.clone());
        self.inner.submit(namespace, entity, kind)
    }
    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        self.inner.deliverable(namespace, entity)
    }
}

pub(super) struct Node<B: Broker> {
    pub(super) service: AmqpListenerService<B>,
    pub(super) address: SocketAddr,
}
impl<B: Broker> Node<B> {
    pub(super) async fn new(
        broker: B,
        endpoint: PreparedWorkloadTls,
        auth: Option<SharedAccessAuthentication>,
        websocket: bool,
    ) -> Self {
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let mut listener = AmqpListener::new(broker, NamespaceName::new("tenant").unwrap());
        if let Some(auth) = auth {
            listener = listener.with_shared_access_authentication(auth);
        }
        let listener = listener.with_workload_tls(endpoint).unwrap();
        let service = if websocket {
            listener.into_wss_service(socket)
        } else {
            listener.into_tcp_service(socket)
        };
        Self { service, address }
    }
    pub(super) async fn exercise<F: Future>(&mut self, scenario: F) -> F::Output {
        timeout(WAIT, async { tokio::select! { output = scenario => output, () = self.service.serve() => panic!("listener ended before scenario") }}).await.unwrap()
    }
    pub(super) async fn finish(mut self) {
        timeout(WAIT, self.service.finish())
            .await
            .expect("original connection/session/link/worker families joined");
        assert!(matches!(
            self.service.take_exit(),
            Some(AmqpListenerExit::Complete(
                Ok(()) | Err(AmqpListenerFailure::Returned(_))
            ))
        ));
    }
}

pub(super) fn owner() -> server::Broker {
    let owner = server::Broker::spawn(server::LocalProposer::new(
        domain::StateMachine::new(storage::MemoryStore::default()),
        server::ManualClock::at(1000),
    ));
    for name in ["orders", "orders-archive"] {
        owner
            .handle()
            .submit_blocking(
                NamespaceName::new("tenant").unwrap(),
                EntityPath::new(name).unwrap(),
                CommandKind::CreateQueue {
                    config: domain::QueueConfig::default(),
                },
            )
            .unwrap();
    }
    owner
}

pub(super) async fn peer(
    fixture: &TlsFixture,
    endpoint: PreparedWorkloadTls,
    identity: &Identity,
) -> (
    crate::workload::WorkloadPeer,
    auth::workload::AdmittedWorkloadStream<TcpStream>,
    TlsStream<TcpStream>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let server = async {
        let (stream, _) = socket.accept().await.unwrap();
        let mut stream = endpoint
            .accept_before(stream, Instant::now() + WAIT)
            .await
            .unwrap();
        let peer = crate::workload::WorkloadPeer::from_admitted(endpoint, &stream).unwrap();
        stream.write_all(b"proof").await.unwrap();
        stream.flush().await.unwrap();
        (peer, stream)
    };
    let client = async {
        let mut stream = fixture.tls(address, Some(identity)).await.unwrap();
        let mut bytes = [0; 5];
        stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"proof");
        stream
    };
    let ((peer, server), client) = timeout(WAIT, async { tokio::join!(server, client) })
        .await
        .unwrap();
    (peer, server, client)
}
