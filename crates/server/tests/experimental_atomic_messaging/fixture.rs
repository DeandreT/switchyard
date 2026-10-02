use std::{net::SocketAddr, path::PathBuf};

use admin_api::v1::{
    CreateEntityRequest, EntityKind, QueueConfiguration, entity_service_client::EntityServiceClient,
};
use tempfile::TempDir;
use tonic::{
    Request,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};

use super::process::{Addresses, Output, Process};
use super::*;

pub(super) struct Credentials {
    _directory: TempDir,
    pub(super) certificate: rustls::pki_types::CertificateDer<'static>,
    pub(super) pem: String,
    certificate_path: PathBuf,
    private_key_path: PathBuf,
    key_path: PathBuf,
}

impl Credentials {
    fn new() -> TestResult<Self> {
        let directory = TempDir::new()?;
        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
        let certificate_path = directory.path().join("certificate.pem");
        let private_key_path = directory.path().join("private-key.pem");
        let key_path = directory.path().join("shared-access-key");
        let pem = cert.pem();
        std::fs::write(&certificate_path, &pem)?;
        std::fs::write(&private_key_path, key_pair.serialize_pem())?;
        std::fs::write(&key_path, format!("{KEY}\n"))?;
        Ok(Self {
            _directory: directory,
            certificate: cert.der().clone(),
            pem,
            certificate_path,
            private_key_path,
            key_path,
        })
    }

    fn arguments(&self, arguments: &mut Vec<String>) {
        for (flag, path) in [
            ("--tls-certificate", &self.certificate_path),
            ("--tls-private-key", &self.private_key_path),
            ("--shared-access-key-file", &self.key_path),
        ] {
            arguments.extend([flag.into(), path.to_string_lossy().into_owned()]);
        }
        arguments.extend(["--shared-access-key-name".into(), RULE.into()]);
    }
}

pub(super) struct BinaryNode {
    process: Process,
    pub(super) addresses: Addresses,
    pub(super) directory: Option<TempDir>,
    pub(super) security: Option<Credentials>,
}

impl BinaryNode {
    pub(super) async fn start(durable: bool, experimental: bool, secure: bool) -> TestResult<Self> {
        let directory = durable.then(TempDir::new).transpose()?;
        let security = secure.then(Credentials::new).transpose()?;
        let mut arguments = base_arguments();
        if let Some(directory) = &directory {
            arguments.extend([
                "--storage".into(),
                "fjall".into(),
                "--data-dir".into(),
                directory.path().to_string_lossy().into_owned(),
            ]);
        }
        if experimental {
            arguments.extend([
                "--experimental-atomic-messaging-listen".into(),
                "127.0.0.1:0".into(),
            ]);
        }
        if let Some(security) = &security {
            security.arguments(&mut arguments);
        }
        let process = Process::spawn(&arguments)?;
        let addresses = process.ready(experimental).await?;
        assert_ne!(addresses.ordinary, addresses.admin);
        if experimental {
            assert_ne!(
                addresses.ordinary,
                addresses.experimental.expect("experimental address")
            );
            assert_ne!(
                addresses.admin,
                addresses.experimental.expect("experimental address")
            );
        } else {
            assert!(addresses.experimental.is_none());
        }
        Ok(Self {
            process,
            addresses,
            directory,
            security,
        })
    }

    pub(super) async fn create_queue(&self) -> TestResult {
        let mut endpoint = Endpoint::from_shared(format!(
            "{}://{}",
            if self.security.is_some() {
                "https"
            } else {
                "http"
            },
            self.addresses.admin
        ))?
        .connect_timeout(DEADLINE)
        .timeout(DEADLINE);
        if let Some(security) = &self.security {
            endpoint = endpoint.tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(&security.pem))
                    .domain_name("localhost"),
            )?;
        }
        let channel: Channel = timeout(DEADLINE, endpoint.connect()).await??;
        let mut client = EntityServiceClient::new(channel);
        let mut request = Request::new(CreateEntityRequest {
            namespace: "tenant".into(),
            path: "orders".into(),
            kind: EntityKind::Queue as i32,
            queue_config: Some(QueueConfiguration {
                lock_duration_millis: Some(60_000),
                ..QueueConfiguration::default()
            }),
            ..CreateEntityRequest::default()
        });
        if self.security.is_some() {
            request
                .metadata_mut()
                .insert("authorization", super::security::token()?.parse()?);
        }
        let entity = timeout(DEADLINE, client.create_entity(request))
            .await??
            .into_inner();
        assert_eq!(entity.path, "orders");
        Ok(())
    }

    pub(super) async fn peer(&self, address: SocketAddr) -> TestResult<Peer> {
        match &self.security {
            Some(credentials) => super::security::authenticated(address, credentials).await,
            None => Peer::connect(address).await,
        }
    }

    pub(super) async fn kill(self) -> TestResult<(Output, Option<TempDir>)> {
        let Self {
            process,
            directory,
            security,
            ..
        } = self;
        let output = process.finish(true).await?;
        drop(security);
        assert!(
            !output.status.success(),
            "the test deliberately kills, not gracefully stops, the binary"
        );
        assert!(!output.stdout.contains(KEY) && !output.stderr.contains(KEY));
        Ok((output, directory))
    }
}

pub(super) fn base_arguments() -> Vec<String> {
    [
        "--namespace",
        HOST,
        "--listen",
        "127.0.0.1:0",
        "--admin-listen",
        "127.0.0.1:0",
        "--sweep-interval-millis",
        "60000",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

pub(super) async fn production_refusal() -> TestResult {
    let temporary = TempDir::new()?;
    let directory = temporary.path().join("never-opened-data");
    let missing = temporary.path().join("never-read-credentials");
    let arguments = [
        "--mode".to_owned(),
        "production".into(),
        "--voters".into(),
        "3".into(),
        "--experimental-atomic-messaging-listen".into(),
        "127.0.0.1:0".into(),
        "--storage".into(),
        "fjall".into(),
        "--data-dir".into(),
        directory.to_string_lossy().into_owned(),
        "--tls-certificate".into(),
        missing.to_string_lossy().into_owned(),
        "--tls-private-key".into(),
        missing.to_string_lossy().into_owned(),
        "--shared-access-key-name".into(),
        RULE.into(),
        "--shared-access-key-file".into(),
        missing.to_string_lossy().into_owned(),
    ];
    let output = Process::spawn(&arguments)?.finish(false).await?;
    assert!(!output.status.success());
    assert!(
        output.stderr.contains(
            "--experimental-atomic-messaging-listen is only available in development mode"
        ),
        "{}",
        output.stderr
    );
    assert!(!output.stderr.contains("never-read-credentials"));
    assert!(!output.stdout.contains("configuration is valid"));
    assert!(!output.stdout.contains("accepting "));
    assert!(!directory.exists());
    assert!(!missing.exists());
    Ok(())
}

pub(super) async fn occupied_socket() -> TestResult {
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = occupied.local_addr()?;
    let mut arguments = base_arguments();
    arguments.extend([
        "--experimental-atomic-messaging-listen".into(),
        address.to_string(),
    ]);
    let output = Process::spawn(&arguments)?.finish(false).await?;
    assert!(!output.status.success());
    assert!(
        output
            .stderr
            .contains(&format!("could not listen on {address}")),
        "{}",
        output.stderr
    );
    assert!(
        !output.stdout.contains("accepting "),
        "configured sockets must bind before serving any endpoint: {}",
        output.stdout
    );
    assert!(!output.stdout.contains(KEY) && !output.stderr.contains(KEY));
    drop(occupied);
    Ok(())
}
