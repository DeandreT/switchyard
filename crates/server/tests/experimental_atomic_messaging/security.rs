use std::{
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};

use amqp::{ClientConnection, SaslCode, SaslInit, SaslPerformative};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use sha2::Sha256;
use tokio_rustls::{TlsConnector, client::TlsStream};
use url::form_urlencoded::byte_serialize;

use super::*;

pub(super) fn token() -> TestResult<String> {
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .checked_add(300)
        .ok_or("test authorization expiry is unavailable")?;
    let audience = format!("amqps://{HOST}");
    let resource: String = byte_serialize(audience.as_bytes()).collect();
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes())
        .map_err(|_| "test signing key is unavailable")?;
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    let signature: String = byte_serialize(signature.as_bytes()).collect();
    Ok(format!(
        "SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={RULE}"
    ))
}

async fn tls(address: SocketAddr, credentials: &Credentials) -> TestResult<TlsStream<TcpStream>> {
    let mut roots = RootCertStore::empty();
    roots.add(credentials.certificate.clone())?;
    let config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
            .with_root_certificates(roots)
            .with_no_client_auth();
    let stream = timeout(DEADLINE, TcpStream::connect(address)).await??;
    stream.set_nodelay(true)?;
    Ok(timeout(
        DEADLINE,
        TlsConnector::from(Arc::new(config)).connect(ServerName::try_from("localhost")?, stream),
    )
    .await??)
}

fn plain(password: &str) -> SaslInit {
    SaslInit {
        mechanism: Symbol::from("PLAIN"),
        initial_response: Some(
            [
                b"\0".as_slice(),
                RULE.as_bytes(),
                b"\0".as_slice(),
                password.as_bytes(),
            ]
            .concat()
            .into(),
        ),
        hostname: Some(HOST.to_owned()),
    }
}

async fn sasl(stream: &mut TlsStream<TcpStream>, password: &str) -> TestResult<SaslCode> {
    timeout(
        DEADLINE,
        write_protocol_header(stream, ProtocolHeader::SASL),
    )
    .await??;
    assert_eq!(
        timeout(DEADLINE, read_protocol_header(stream)).await??,
        ProtocolHeader::SASL
    );
    let Frame::Sasl(SaslPerformative::Mechanisms(mechanisms)) =
        timeout(DEADLINE, read_frame(stream)).await??
    else {
        return Err("SASL mechanisms missing on authenticated listener".into());
    };
    assert!(
        mechanisms
            .mechanisms
            .iter()
            .any(|mechanism| mechanism.as_str() == "PLAIN")
    );
    timeout(
        DEADLINE,
        write_frame(
            stream,
            &Frame::Sasl(SaslPerformative::Init(plain(password))),
        ),
    )
    .await??;
    let Frame::Sasl(SaslPerformative::Outcome(outcome)) =
        timeout(DEADLINE, read_frame(stream)).await??
    else {
        return Err("SASL outcome missing on authenticated listener".into());
    };
    Ok(outcome.code)
}

pub(super) async fn authenticated(
    address: SocketAddr,
    credentials: &Credentials,
) -> TestResult<Peer> {
    let mut stream = tls(address, credentials).await?;
    assert_eq!(sasl(&mut stream, KEY).await?, SaslCode::Ok);
    timeout(DEADLINE, Peer::open(Box::new(stream))).await?
}

pub(super) async fn management_connection(
    address: SocketAddr,
    credentials: &Credentials,
) -> TestResult<ClientConnection> {
    let stream = tls(address, credentials).await?;
    Ok(timeout(
        DEADLINE,
        ClientConnection::builder()
            .container_id("binary-secure-readonly-browser")
            .sasl(plain(KEY))
            .open_with_stream(stream),
    )
    .await??)
}

async fn plaintext_is_refused(address: SocketAddr) -> TestResult {
    let mut stream = timeout(DEADLINE, TcpStream::connect(address)).await??;
    stream.set_nodelay(true)?;
    let result = timeout(DEADLINE, async {
        write_protocol_header(&mut stream, ProtocolHeader::AMQP).await?;
        read_protocol_header(&mut stream).await
    })
    .await?;
    assert!(
        result.is_err(),
        "TLS listener accepted an unauthenticated plaintext AMQP header"
    );
    Ok(())
}

async fn wrong_password_is_refused(address: SocketAddr, credentials: &Credentials) -> TestResult {
    let mut stream = tls(address, credentials).await?;
    assert_eq!(
        sasl(&mut stream, "incorrect-binary-password").await?,
        SaslCode::Auth
    );
    Ok(())
}

pub(super) async fn inherited_security() -> TestResult {
    let node = BinaryNode::start(false, true, true).await?;
    let result = async {
        let credentials = node.security.as_ref().ok_or("test TLS identity missing")?;
        for address in [
            node.addresses.ordinary,
            node.addresses
                .experimental
                .ok_or("experimental address missing")?,
        ] {
            plaintext_is_refused(address).await?;
            wrong_password_is_refused(address, credentials).await?;
        }
        super::wire::mixed_roundtrip(&node).await
    }
    .await;
    let (_, directory) = node.kill().await?;
    result?;
    assert!(directory.is_none());
    Ok(())
}
