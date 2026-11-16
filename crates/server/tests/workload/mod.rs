//! Test-only private roots and actual clients. No machine trust/signing setup.
#![allow(dead_code)]
use auth::{
    ResourceScope,
    workload::{PreparedWorkloadTls, WorkloadPolicy},
};
use rcgen::{
    Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    SanType,
};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{PrivatePkcs8KeyDer, ServerName},
};
use serde_json::{Value, json};
use std::{
    error::Error,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::{TlsConnector, client::TlsStream};
use tonic::transport::{
    Certificate as TonicCertificate, Channel, ClientTlsConfig, Endpoint, Identity as TonicIdentity,
};
pub const ROOT: &str = "amqps://tenant.servicebus.windows.net";
pub const SUB: &str = "/events/Subscriptions/consumer?api-version=2024-05&enrich=False";
pub const WAIT: Duration = Duration::from_secs(120);
// One-second certificate lease plus five seconds of scheduling/IO margin. This
// MUST stay below the process's nearest ordinary HTTP deadline (10 seconds).
pub const IO_LEASE_SECONDS: u64 = 1;
pub const LEASE_IO_BOUND: Duration = Duration::from_secs(IO_LEASE_SECONDS + 5);
pub type Result<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
pub struct Identity {
    pub cert: Certificate,
    pub key: KeyPair,
}
pub fn root() -> Identity {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    Identity {
        cert: params.self_signed(&key).unwrap(),
        key,
    }
}
pub fn client_identity(ca: &Identity, name: &str, expiry: Option<u64>) -> Identity {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::default();
    params.subject_alt_names = vec![SanType::URI(
        format!("spiffe://fixture.test/{name}").try_into().unwrap(),
    )];
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    if let Some(expiry) = expiry {
        params.not_after = time::OffsetDateTime::from_unix_timestamp(expiry as i64).unwrap();
    }
    Identity {
        cert: params.signed_by(&key, &ca.cert, &ca.key).unwrap(),
        key,
    }
}
pub struct Fixture {
    pub old: Identity,
    pub new: Identity,
    pub server_ca: Identity,
    pub server: Identity,
    pub manager: Identity,
    pub entity: Identity,
    pub sender: Identity,
    pub next: Identity,
}
impl Fixture {
    pub fn new() -> Self {
        let old = root();
        let new = root();
        let server_ca = root();
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["endpoint.fixture.test".into()]).unwrap();
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server = Identity {
            cert: params
                .signed_by(&key, &server_ca.cert, &server_ca.key)
                .unwrap(),
            key,
        };
        let manager = client_identity(&old, "manager", None);
        let entity = client_identity(&old, "entity", None);
        let sender = client_identity(&old, "sender", None);
        let next = client_identity(&new, "manager", None);
        Self {
            old,
            new,
            server_ca,
            server,
            manager,
            entity,
            sender,
            next,
        }
    }
    pub fn policy(&self, roots: &[&Identity], optional: bool, cap: u64) -> Value {
        json!({"version":1,"trust_domain":"fixture.test","roots_pem":roots.iter().map(|id|id.cert.pem()).collect::<Vec<_>>(),
            "client_auth":if optional {"optional"} else {"required"},"max_connection_seconds":cap,
            "bindings":[{"principal":"spiffe://fixture.test/manager","scope":ROOT,"permissions":["manage"]},
            {"principal":"spiffe://fixture.test/entity","scope":format!("{ROOT}/events/subscriptions/consumer"),"permissions":["manage"]},
            {"principal":"spiffe://fixture.test/sender","scope":ROOT,"permissions":["send"]}]})
    }
    pub fn endpoint(&self, policy: Value, alpn: &[u8]) -> PreparedWorkloadTls {
        PreparedWorkloadTls::new(
            WorkloadPolicy::from_json(&policy.to_string(), ResourceScope::parse(ROOT).unwrap())
                .unwrap(),
            self.server.cert.pem().as_bytes(),
            self.server.key.serialize_pem().as_bytes(),
            if alpn.is_empty() {
                vec![]
            } else {
                vec![alpn.to_vec()]
            },
            Duration::from_secs(30),
        )
        .unwrap()
    }
    pub fn connector(&self, id: Option<&Identity>, alpn: &[u8]) -> TlsConnector {
        self.connector_with_versions(id, alpn, rustls::DEFAULT_VERSIONS)
    }
    fn connector_with_versions(
        &self,
        id: Option<&Identity>,
        alpn: &[u8],
        versions: &[&'static rustls::SupportedProtocolVersion],
    ) -> TlsConnector {
        let mut roots = RootCertStore::empty();
        roots.add(self.server_ca.cert.der().clone()).unwrap();
        let builder =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(versions)
                .unwrap()
                .with_root_certificates(roots);
        let mut config = match id {
            Some(id) => builder
                .with_client_auth_cert(
                    vec![id.cert.der().clone()],
                    PrivatePkcs8KeyDer::from(id.key.serialize_der()).into(),
                )
                .unwrap(),
            None => builder.with_no_client_auth(),
        };
        config.alpn_protocols = if alpn.is_empty() {
            vec![]
        } else {
            vec![alpn.to_vec()]
        };
        TlsConnector::from(Arc::new(config))
    }
    /// TLS 1.2's final server Finished follows client-certificate verification,
    /// unlike the client's early TLS 1.3 completion. Used for causal IO controls.
    pub async fn tls12(
        &self,
        address: SocketAddr,
        id: Option<&Identity>,
        alpn: &[u8],
    ) -> Result<TlsStream<TcpStream>> {
        Ok(self
            .connector_with_versions(id, alpn, &[&rustls::version::TLS12])
            .connect(
                ServerName::try_from("endpoint.fixture.test").unwrap(),
                TcpStream::connect(address).await?,
            )
            .await?)
    }
    pub async fn tls(
        &self,
        address: SocketAddr,
        id: Option<&Identity>,
        alpn: &[u8],
    ) -> Result<TlsStream<TcpStream>> {
        Ok(self
            .connector(id, alpn)
            .connect(
                ServerName::try_from("endpoint.fixture.test").unwrap(),
                TcpStream::connect(address).await?,
            )
            .await?)
    }
    pub async fn rpc(
        &self,
        address: SocketAddr,
        id: Option<&Identity>,
    ) -> Result<admin_api::wire::entity_service_client::EntityServiceClient<Channel>> {
        let mut tls = ClientTlsConfig::new()
            .ca_certificate(TonicCertificate::from_pem(self.server_ca.cert.pem()))
            .domain_name("endpoint.fixture.test");
        if let Some(id) = id {
            tls = tls.identity(TonicIdentity::from_pem(
                id.cert.pem(),
                id.key.serialize_pem(),
            ));
        }
        let channel = Endpoint::from_shared(format!("https://{address}"))?
            .tls_config(tls)?
            .connect_timeout(WAIT)
            .timeout(WAIT)
            .connect()
            .await?;
        Ok(admin_api::wire::entity_service_client::EntityServiceClient::new(channel))
    }
    pub async fn atom(
        &self,
        address: SocketAddr,
        id: Option<&Identity>,
        auth: &str,
        target: &str,
    ) -> Result<String> {
        let mut stream = self.tls(address, id, b"http/1.1").await?;
        stream
            .write_all(
                format!("GET {target} HTTP/1.1\r\nHost: untrusted.example\r\n{auth}\r\n")
                    .as_bytes(),
            )
            .await?;
        let mut bytes = Vec::new();
        tokio::time::timeout(WAIT, stream.read_to_end(&mut bytes)).await??;
        Ok(String::from_utf8(bytes)?)
    }
    pub async fn amqp(
        &self,
        address: SocketAddr,
        id: Option<&Identity>,
        wss: bool,
    ) -> Result<amqp::ClientConnection> {
        let stream = self.tls(address, id, b"").await?;
        let builder = amqp::ClientConnection::builder()
            .container_id("actual-workload")
            .sasl(amqp::SaslInit {
                mechanism: "ANONYMOUS".into(),
                initial_response: None,
                hostname: Some("untrusted.example".into()),
            });
        if wss {
            use tokio_tungstenite::tungstenite::client::IntoClientRequest;
            let mut request =
                format!("wss://{address}/$servicebus/websocket").into_client_request()?;
            request
                .headers_mut()
                .insert("sec-websocket-protocol", "AMQPWSB10".parse()?);
            let (stream, _) = tokio_tungstenite::client_async(request, stream).await?;
            Ok(builder
                .open_with_stream(protocol_amqp::WebSocketIo::new(stream))
                .await?)
        } else {
            Ok(builder.open_with_stream(stream).await?)
        }
    }
}
