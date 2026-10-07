use std::{
    error::Error,
    ffi::OsString,
    fs,
    net::{SocketAddr, TcpListener as Reservation},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Method, Request, StatusCode, body::Bytes};
use hyper_util::rt::TokioIo;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
use sha2::Sha256;
use tempfile::TempDir;
use tokio::{
    net::TcpStream,
    time::{sleep, timeout},
};
use tokio_rustls::TlsConnector;

use super::TestResult;

pub(super) const ATOM_RULE: &str = "atom-manage";
pub(super) const ATOM_KEY: &str = "atom-cli-private-fixture-key";
pub(super) const LEGACY_RULE: &str = "legacy-manage";
pub(super) const LEGACY_KEY: &str = "legacy-cli-private-fixture-key";
pub(super) const COLLECTION: &str =
    "/$Resources/queues?api-version=2024-05&enrich=False&$skip=0&$top=100";
pub(super) const ENTITY: &str = "/orders?api-version=2024-05";
pub(super) const CREATE: &[u8] = br#"<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><LockDuration>PT15S</LockDuration><MaxSizeInMegabytes>2</MaxSizeInMegabytes><RequiresDuplicateDetection>false</RequiresDuplicateDetection><RequiresSession>false</RequiresSession><DefaultMessageTimeToLive>PT45S</DefaultMessageTimeToLive><DeadLetteringOnMessageExpiration>true</DeadLetteringOnMessageExpiration><DuplicateDetectionHistoryTimeWindow>PT1M</DuplicateDetectionHistoryTimeWindow><MaxDeliveryCount>3</MaxDeliveryCount><MaxMessageSizeInKilobytes>4</MaxMessageSizeInKilobytes></QueueDescription></content></entry>"#;
const REQUEST_DEADLINE: Duration = Duration::from_secs(3);
const RESPONSE_LIMIT: usize = 65_536;

pub(super) struct Files {
    pub(super) directory: TempDir,
    certificate: PathBuf,
    private_key: PathBuf,
    pub(super) atom_key: PathBuf,
    legacy_key: PathBuf,
    pub(super) store: PathBuf,
    ca: CertificateDer<'static>,
    wrong_ca: CertificateDer<'static>,
    private_key_pem: String,
}

impl Files {
    pub(super) fn new() -> TestResult<Self> {
        let directory = TempDir::new()?;
        let (ca, ca_key) = authority("Atom CLI private CA")?;
        let (wrong_ca, _) = authority("Atom CLI unrelated CA")?;
        let mut parameters = CertificateParams::new(vec!["localhost".into()])?;
        parameters
            .distinguished_name
            .push(DnType::CommonName, "Atom CLI localhost");
        parameters.use_authority_key_identifier_extension = true;
        parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let key = KeyPair::generate()?;
        let leaf = parameters.signed_by(&key, &ca, &ca_key)?;
        let private_key_pem = key.serialize_pem();
        let certificate = directory.path().join("certificate-chain.pem");
        let private_key = directory.path().join("private-key.pem");
        let atom_key = directory.path().join("atom-key");
        let legacy_key = directory.path().join("legacy-key");
        let store = directory.path().join("uncreated-parent/store");
        fs::write(&certificate, format!("{}{}", leaf.pem(), ca.pem()))?;
        fs::write(&private_key, &private_key_pem)?;
        fs::write(&atom_key, format!("{ATOM_KEY}\r\n"))?;
        fs::write(&legacy_key, format!("{LEGACY_KEY}\n"))?;
        Ok(Self {
            directory,
            certificate,
            private_key,
            atom_key,
            legacy_key,
            store,
            ca: ca.der().clone(),
            wrong_ca: wrong_ca.der().clone(),
            private_key_pem,
        })
    }

    pub(super) fn sensitive(&self) -> [&str; 3] {
        [ATOM_KEY, LEGACY_KEY, self.private_key_pem.as_str()]
    }

    pub(super) fn arguments(&self, atom_address: SocketAddr, atom_key: &Path) -> Vec<OsString> {
        let mut arguments = vec![
            "--namespace".into(),
            "localhost".into(),
            "--storage".into(),
            "fjall".into(),
            "--data-dir".into(),
            self.store.as_os_str().to_owned(),
            "--sweep-interval-millis".into(),
            "600000".into(),
            "--shared-access-key-name".into(),
            LEGACY_RULE.into(),
            "--atom-admin-listen".into(),
            atom_address.to_string().into(),
            "--atom-admin-audience-host".into(),
            "localhost".into(),
            "--atom-admin-key-name".into(),
            ATOM_RULE.into(),
        ];
        for (flag, path) in [
            ("--tls-certificate", self.certificate.as_path()),
            ("--tls-private-key", self.private_key.as_path()),
            ("--shared-access-key-file", self.legacy_key.as_path()),
            ("--atom-admin-key-file", atom_key),
        ] {
            arguments.push(flag.into());
            arguments.push(path.as_os_str().to_owned());
        }
        arguments
    }

    pub(super) fn live_arguments(&self) -> TestResult<(Vec<OsString>, SocketAddr)> {
        let amqp = Reservation::bind("127.0.0.1:0")?;
        let atom = Reservation::bind("127.0.0.1:0")?;
        let address = atom.local_addr()?;
        let mut arguments = self.arguments(address, &self.atom_key);
        arguments.extend(["--listen".into(), amqp.local_addr()?.to_string().into()]);
        drop((amqp, atom));
        Ok((arguments, address))
    }

    pub(super) fn unopened_storage(&self) {
        assert!(!self.store.exists());
        assert!(!self.store.parent().unwrap().exists());
    }

    pub(super) async fn ready(&self, address: SocketAddr) -> TestResult {
        timeout(Duration::from_secs(5), async {
            loop {
                match timeout(Duration::from_millis(250), TcpStream::connect(address)).await {
                    Ok(Ok(stream)) => {
                        let (status, body) = self
                            .exchange_connected(
                                stream,
                                Method::GET,
                                COLLECTION,
                                &token("https://localhost", ATOM_RULE, ATOM_KEY)?,
                                b"",
                            )
                            .await?;
                        assert_eq!(status, StatusCode::OK);
                        assert_eq!(body, b"<feed xmlns=\"http://www.w3.org/2005/Atom\"></feed>");
                        return Ok::<_, Box<dyn Error>>(());
                    }
                    Ok(Err(error)) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                        sleep(Duration::from_millis(25)).await
                    }
                    Ok(Err(error)) => return Err(error.into()),
                    Err(error) => return Err(error.into()),
                }
            }
        })
        .await?
    }

    pub(super) async fn exchange(
        &self,
        address: SocketAddr,
        method: Method,
        path: &str,
        authorization: &str,
        body: &[u8],
    ) -> TestResult<(StatusCode, Vec<u8>)> {
        let stream = timeout(REQUEST_DEADLINE, TcpStream::connect(address)).await??;
        self.exchange_connected(stream, method, path, authorization, body)
            .await
    }

    async fn exchange_connected(
        &self,
        stream: TcpStream,
        method: Method,
        path: &str,
        authorization: &str,
        body: &[u8],
    ) -> TestResult<(StatusCode, Vec<u8>)> {
        timeout(REQUEST_DEADLINE, async {
            let stream = connector(&self.ca)?
                .connect(ServerName::try_from("localhost".to_owned())?, stream)
                .await?;
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake::<_, Full<Bytes>>(TokioIo::new(stream))
                    .await?;
            let request = Request::builder()
                .method(method)
                .uri(path)
                .header("host", "untrusted.example")
                .header("connection", "close")
                .header("content-type", "application/atom+xml")
                .header("authorization", authorization)
                .body(Full::new(Bytes::copy_from_slice(body)))?;
            let response = async {
                let response = sender.send_request(request).await?;
                assert_eq!(response.headers().get("connection").unwrap(), "close");
                let status = response.status();
                let body = Limited::new(response.into_body(), RESPONSE_LIMIT)
                    .collect()
                    .await
                    .map_err(|error| -> Box<dyn Error> { error })?
                    .to_bytes()
                    .to_vec();
                drop(sender);
                Ok::<_, Box<dyn Error>>((status, body))
            };
            // Drive and join the original HTTP future inline; no client task is detached.
            let (response, driven) = tokio::join!(response, connection);
            driven?;
            response
        })
        .await?
    }

    pub(super) async fn assert_tls_refusals(&self, address: SocketAddr) -> TestResult {
        for (ca, name, wrong_name) in [
            (&self.wrong_ca, "localhost", false),
            (&self.ca, "127.0.0.1", true),
        ] {
            let stream = timeout(REQUEST_DEADLINE, TcpStream::connect(address)).await??;
            let error = match timeout(
                REQUEST_DEADLINE,
                connector(ca)?.connect(ServerName::try_from(name.to_owned())?, stream),
            )
            .await?
            {
                Ok(_) => return Err("invalid private CA or certificate name was accepted".into()),
                Err(error) => error,
            };
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
            let cause = error
                .get_ref()
                .and_then(|cause| cause.downcast_ref::<rustls::Error>());
            if wrong_name {
                assert!(matches!(
                    cause,
                    Some(rustls::Error::InvalidCertificate(
                        rustls::CertificateError::NotValidForNameContext { .. }
                    ))
                ));
            } else {
                assert!(matches!(
                    cause,
                    Some(rustls::Error::InvalidCertificate(
                        rustls::CertificateError::UnknownIssuer
                    ))
                ));
            }
        }
        Ok(())
    }
}

fn authority(name: &str) -> TestResult<(rcgen::Certificate, KeyPair)> {
    let mut parameters = CertificateParams::new(Vec::<String>::new())?;
    parameters.distinguished_name.push(DnType::CommonName, name);
    parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    parameters.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let key = KeyPair::generate()?;
    Ok((parameters.self_signed(&key)?, key))
}

fn connector(certificate: &CertificateDer<'static>) -> TestResult<TlsConnector> {
    let mut roots = RootCertStore::empty();
    roots.add(certificate.clone())?;
    let mut config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
            .with_root_certificates(roots)
            .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsConnector::from(Arc::new(config)))
}

pub(super) fn token(audience: &str, rule: &str, key: &str) -> TestResult<String> {
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .checked_add(120)
        .ok_or("fixture expiry overflow")?;
    let resource = url::form_urlencoded::byte_serialize(audience.as_bytes()).collect::<String>();
    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes())?;
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = url::form_urlencoded::byte_serialize(
        STANDARD.encode(mac.finalize().into_bytes()).as_bytes(),
    )
    .collect::<String>();
    let rule = url::form_urlencoded::byte_serialize(rule.as_bytes()).collect::<String>();
    Ok(format!(
        "SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={rule}"
    ))
}
