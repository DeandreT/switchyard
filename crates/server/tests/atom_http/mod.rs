//! Strict, process-local private CA; no machine trust or permissive verifier.
use admin_api::atom::https::{Configuration, Limits};
use auth::{JwtPolicy, ResourceScope};
use domain::NamespaceName;
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, ServerName},
};
use std::{error::Error, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::{TlsConnector, client::TlsStream};
pub type TestResult<T = ()> = Result<T, Box<dyn Error>>;
pub const WAIT: Duration = Duration::from_secs(120);
pub const TARGET: &str = "/orders?api-version=2024-05&enrich=False";

pub struct Identity {
    pub chain: String,
    pub key: String,
    pub ca_pem: String,
    pub ca: CertificateDer<'static>,
}
impl Identity {
    pub fn new() -> TestResult<Self> {
        let ca_key = KeyPair::generate()?;
        let mut params = CertificateParams::new(vec!["private-test-ca".into()])?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = params.self_signed(&ca_key)?;
        let key = KeyPair::generate()?;
        let leaf =
            CertificateParams::new(vec!["localhost".into()])?.signed_by(&key, &ca, &ca_key)?;
        Ok(Self {
            chain: format!("{}{}", leaf.pem(), ca.pem()),
            key: key.serialize_pem(),
            ca_pem: ca.pem(),
            ca: ca.der().clone(),
        })
    }
    pub fn atom(&self) -> TestResult<Configuration> {
        Ok(Configuration::new(
            self.chain.as_bytes(),
            self.key.as_bytes(),
            JwtPolicy::from_json(&crate::admin_tls::policy().to_string())?,
            NamespaceName::new("tenant")?,
            ResourceScope::parse(crate::admin_tls::ROOT)?,
        )?
        .with_limits(Limits::default())?)
    }
    pub fn native(&self) -> TestResult<ServerConfig> {
        Ok(protocol_amqp::tls_server_config(
            self.chain.as_bytes(),
            self.key.as_bytes(),
        )?)
    }
}
pub async fn connect(address: SocketAddr, identity: &Identity) -> TestResult<TlsStream<TcpStream>> {
    let mut roots = RootCertStore::empty();
    roots.add(identity.ca.clone())?;
    let mut config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(tokio::time::timeout(
        WAIT,
        TlsConnector::from(Arc::new(config)).connect(
            ServerName::try_from("localhost")?,
            TcpStream::connect(address).await?,
        ),
    )
    .await??)
}
pub fn wire(token: &str, target: &str) -> String {
    format!(
        "GET {target} HTTP/1.1\r\nHost: foreign-peer.example\r\nAuthorization: Bearer {token}\r\n\r\n"
    )
}
pub async fn read(stream: &mut TlsStream<TcpStream>) -> TestResult<String> {
    let mut bytes = Vec::new();
    tokio::time::timeout(WAIT, stream.take(1024 * 1024).read_to_end(&mut bytes)).await??;
    Ok(String::from_utf8(bytes)?)
}
pub async fn exchange(
    address: SocketAddr,
    identity: &Identity,
    token: &str,
    target: &str,
) -> TestResult<String> {
    let mut stream = connect(address, identity).await?;
    stream.write_all(wire(token, target).as_bytes()).await?;
    read(&mut stream).await
}
