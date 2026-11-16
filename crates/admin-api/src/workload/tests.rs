use super::*;
use crate::{
    CreateEntityRequest, EntityAddress, EntityBackend, EntityKind, EntityOptions, EntityService,
    EntityTarget, GetEntityRequest, ListEntitiesRequest, ServiceError,
};
use auth::workload::WorkloadPolicy;
use domain::{
    AdminCatalogCursor, AdminCatalogEntry, AdminCatalogKind, AdminCatalogPage, CommandKind,
    CommandOutcome, EntityPath, NamespaceName,
};
use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose, SanType};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{PrivatePkcs8KeyDer, ServerName},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::Instant,
};
pub(crate) const ROOT: &str = "amqps://tenant.servicebus.windows.net";

pub(crate) struct Fixture {
    pub(crate) endpoint: PreparedWorkloadTls,
    pub(crate) connector: tokio_rustls::TlsConnector,
}
impl Fixture {
    pub(crate) fn new(cap: u64, alpn: &[u8]) -> Self {
        let ca_key = KeyPair::generate().unwrap();
        let mut p = CertificateParams::default();
        p.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        p.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        let ca = p.self_signed(&ca_key).unwrap();
        let server_key = KeyPair::generate().unwrap();
        let server = CertificateParams::new(vec!["localhost".into()])
            .unwrap()
            .signed_by(&server_key, &ca, &ca_key)
            .unwrap();
        let client_key = KeyPair::generate().unwrap();
        let mut p = CertificateParams::default();
        p.subject_alt_names = vec![SanType::URI(
            "spiffe://fixture.test/manager".try_into().unwrap(),
        )];
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client = p.signed_by(&client_key, &ca, &ca_key).unwrap();
        let policy = serde_json::json!({"version":1,"trust_domain":"fixture.test","roots_pem":[ca.pem()],"max_connection_seconds":cap,"bindings":[{"principal":"spiffe://fixture.test/manager","scope":ROOT,"permissions":["manage"]}]});
        let endpoint = PreparedWorkloadTls::new(
            WorkloadPolicy::from_json(&policy.to_string(), ResourceScope::parse(ROOT).unwrap())
                .unwrap(),
            server.pem().as_bytes(),
            server_key.serialize_pem().as_bytes(),
            vec![alpn.to_vec()],
            Duration::from_secs(30),
        )
        .unwrap();
        let mut roots = RootCertStore::empty();
        roots.add(ca.der().clone()).unwrap();
        let mut config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_client_auth_cert(
                    vec![client.der().clone()],
                    PrivatePkcs8KeyDer::from(client_key.serialize_der()).into(),
                )
                .unwrap();
        config.alpn_protocols = vec![alpn.to_vec()];
        Self {
            endpoint,
            connector: tokio_rustls::TlsConnector::from(Arc::new(config)),
        }
    }
    pub(crate) async fn pair(
        &self,
    ) -> (
        Connection,
        AdmittedWorkloadStream<TcpStream>,
        tokio_rustls::client::TlsStream<TcpStream>,
    ) {
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let server = async {
            let (stream, _) = socket.accept().await.unwrap();
            let mut stream = self
                .endpoint
                .accept_before(stream, Instant::now() + Duration::from_secs(30))
                .await
                .unwrap();
            let connection = Connection::from_admitted(self.endpoint.clone(), &stream).unwrap();
            stream.write_all(b"proof").await.unwrap();
            stream.flush().await.unwrap();
            (connection, stream)
        };
        let client = async {
            let mut stream = self
                .connector
                .connect(
                    ServerName::try_from("localhost").unwrap(),
                    TcpStream::connect(address).await.unwrap(),
                )
                .await
                .unwrap();
            let mut proof = [0; 5];
            stream.read_exact(&mut proof).await.unwrap();
            assert_eq!(&proof, b"proof");
            stream
        };
        let ((context, server), client) = tokio::join!(server, client);
        (context, server, client)
    }
}
#[derive(Clone, Default)]
struct Backend {
    calls: Arc<AtomicUsize>,
    release: Arc<Notify>,
    started: Arc<Notify>,
}
impl EntityBackend for Backend {
    type Error = std::io::Error;
    fn create(
        &self,
        _: NamespaceName,
        _entity: EntityPath,
        _: CommandKind,
    ) -> impl std::future::Future<Output = Result<CommandOutcome, ServiceError<Self::Error>>> + Send
    {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        async move {
            self.release.notified().await;
            Ok(CommandOutcome::QueueCreated {})
        }
    }
    fn get(
        &self,
        _: NamespaceName,
        _: EntityTarget,
    ) -> impl std::future::Future<
        Output = Result<Option<AdminCatalogEntry>, ServiceError<Self::Error>>,
    > + Send {
        self.calls.fetch_add(1, Ordering::SeqCst);
        async { Ok(None) }
    }
    fn list(
        &self,
        _: NamespaceName,
        _: AdminCatalogKind,
        _: Option<AdminCatalogCursor>,
        _: usize,
    ) -> impl std::future::Future<Output = Result<AdminCatalogPage, ServiceError<Self::Error>>> + Send
    {
        self.calls.fetch_add(1, Ordering::SeqCst);
        async { Err(ServiceError::AuthorizationExpired) }
    }
}
fn create() -> CreateEntityRequest {
    CreateEntityRequest {
        namespace: "tenant".into(),
        address: EntityAddress::Entity {
            path: "orders".into(),
        },
        kind: EntityKind::Queue,
        options: EntityOptions::default(),
    }
}

#[tokio::test]
async fn only_matching_actual_endpoint_context_is_accepted_and_late_prepared_backend_methods_never_begin()
 {
    let f = Fixture::new(1, b"h2");
    let (context, stream, _client) = f.pair().await;
    let other = Fixture::new(10, b"h2");
    assert!(matches!(
        Connection::from_admitted(other.endpoint, &stream),
        Err(WorkloadError::WrongEndpoint)
    ));
    let backend = Backend::default();
    let service = EntityService::new(NamespaceName::new("tenant").unwrap(), backend.clone())
        .with_connection(Some(context.clone()));
    let create = service.create(create());
    let get = service.get(GetEntityRequest {
        namespace: "tenant".into(),
        address: EntityAddress::Entity {
            path: "orders".into(),
        },
    });
    let list = service.list(ListEntitiesRequest {
        namespace: "tenant".into(),
        kind: AdminCatalogKind::Queues,
        continuation: None,
        limit: 10,
    });
    context.expired().await;
    assert!(matches!(
        create.await,
        Err(ServiceError::AuthorizationExpired)
    ));
    assert!(matches!(get.await, Err(ServiceError::AuthorizationExpired)));
    assert!(matches!(
        list.await,
        Err(ServiceError::AuthorizationExpired)
    ));
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn actual_backend_original_begun_before_cutoff_returns_once_after_expiry_without_reinvocation()
 {
    let f = Fixture::new(1, b"h2");
    let (context, _stream, _client) = f.pair().await;
    let backend = Backend::default();
    let service = EntityService::new(NamespaceName::new("tenant").unwrap(), backend.clone())
        .with_connection(Some(context.clone()));
    let future = service.create(create());
    tokio::pin!(future);
    tokio::select! {result=&mut future=>panic!("original returned before release: {result:?}"),()=backend.started.notified()=>{}}
    context.expired().await;
    backend.release.notify_one();
    assert!(future.await.is_ok());
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
}
