//! Native requests traverse HTTP/2 and TLS before reaching the storage owner.

use std::{
    error::Error,
    num::NonZeroUsize,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use admin_api::v1::{
    CreateEntityRequest, EntityKind, GetEntityRequest, ListEntitiesRequest, QueueConfiguration,
    UnlimitedTimeToLive, UpdateEntityRequest, entity_service_client::EntityServiceClient,
    queue_configuration::DefaultTimeToLive,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{EntityPath, NamespaceName, StateMachine};
use hmac::{Hmac, Mac};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use server::{
    Broker, LocalProposer, ManualClock, NativeAdminError, NativeAdminListener, NativeAdminService,
};
use sha2::Sha256;
use testkit::StoreProvider;
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};
use tonic::{
    Code, Request,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use url::form_urlencoded::byte_serialize;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "native-test-secret";
const DEADLINE: Duration = Duration::from_secs(5);

fn policy() -> TestResult<SharedAccessPolicy> {
    Ok(SharedAccessPolicy::new([SharedAccessRule::new(
        "manage",
        ResourceScope::namespace(HOST)?,
        SharedAccessKey::new(KEY)?,
        None,
        PermissionSet::MANAGE,
    )?])?)
}

fn token() -> String {
    let resource = byte_serialize(format!("amqps://{HOST}").as_bytes()).collect::<String>();
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the system clock follows the epoch")
        .as_secs()
        + 300;
    let mut signature = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).expect("an HMAC key");
    signature.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(signature.finalize().into_bytes());
    let signature = byte_serialize(signature.as_bytes()).collect::<String>();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn=manage")
}

fn authorized<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        token().parse().expect("ASCII SAS metadata"),
    );
    request
}

fn create(path: &str) -> CreateEntityRequest {
    CreateEntityRequest {
        namespace: "tenant".to_owned(),
        path: path.to_owned(),
        kind: EntityKind::Queue as i32,
        queue_config: Some(QueueConfiguration {
            lock_duration_millis: Some(30_000),
            default_time_to_live: Some(DefaultTimeToLive::DefaultTtlMillis(60_000)),
            ..Default::default()
        }),
        ..Default::default()
    }
}

struct Node<P> {
    broker: Broker,
    endpoint: String,
    certificate: Option<String>,
    listener: JoinHandle<()>,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    async fn start(provider: P, tls: bool) -> TestResult<Self> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(provider.open()?),
            ManualClock::at(1_000),
        ));
        let service = NativeAdminService::new(broker.handle(), NamespaceName::new("tenant")?);
        let service = if tls {
            service.with_shared_access_policy(policy()?, HOST)?
        } else {
            service
        };
        let mut admin = NativeAdminListener::new(service);
        let certificate = if tls {
            let CertifiedKey { cert, key_pair } =
                generate_simple_self_signed(vec!["localhost".to_owned()])?;
            let pem = cert.pem();
            admin = admin.with_tls(pem.as_bytes(), key_pair.serialize_pem().as_bytes())?;
            Some(pem)
        } else {
            None
        };
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!(
            "{}://{}",
            if tls { "https" } else { "http" },
            socket.local_addr()?
        );
        let listener = tokio::spawn(async move {
            let _ = admin.serve(socket).await;
        });
        Ok(Self {
            broker,
            endpoint,
            certificate,
            listener,
            _provider: provider,
        })
    }

    async fn client(&self) -> TestResult<EntityServiceClient<Channel>> {
        let mut endpoint = Endpoint::from_shared(self.endpoint.clone())?
            .connect_timeout(DEADLINE)
            .timeout(DEADLINE);
        if let Some(certificate) = &self.certificate {
            endpoint = endpoint.tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(certificate))
                    .domain_name("localhost"),
            )?;
        }
        Ok(EntityServiceClient::new(endpoint.connect().await?))
    }
}

impl<P> Drop for Node<P> {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

async fn plaintext_round_trip<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, false).await?;
    let mut client = node.client().await?;
    let created = client.create_entity(create("orders")).await?.into_inner();
    assert_eq!(created.path, "orders");
    assert_eq!(
        created.queue_config.as_ref().unwrap().lock_duration_millis,
        Some(30_000)
    );
    assert_eq!(
        client
            .get_entity(GetEntityRequest {
                namespace: "tenant".to_owned(),
                path: "orders".to_owned(),
            })
            .await?
            .into_inner(),
        created
    );
    let updated = client
        .update_entity(UpdateEntityRequest {
            namespace: "tenant".to_owned(),
            path: "orders".to_owned(),
            queue_config: Some(QueueConfiguration {
                default_time_to_live: Some(DefaultTimeToLive::DefaultTtlUnlimited(
                    UnlimitedTimeToLive {},
                )),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?
        .into_inner();
    assert!(matches!(
        updated.queue_config.unwrap().default_time_to_live,
        Some(DefaultTimeToLive::DefaultTtlUnlimited(_))
    ));
    let config = node
        .broker
        .handle()
        .queue_config(NamespaceName::new("tenant")?, EntityPath::new("orders")?)
        .await?
        .expect("the native creation was committed");
    assert_eq!(config.default_time_to_live_millis, None);
    assert_eq!(config.lock_duration_millis, 30_000);
    let listed = client
        .list_entities(ListEntitiesRequest {
            namespace: "tenant".to_owned(),
            page_size: 1,
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(listed.entities.len(), 1);
    assert_eq!(listed.entities[0].path, "orders");
    assert!(listed.next_page_token.is_empty(), "the shadow is hidden");

    let mut oversized = create("oversized");
    oversized.path = "x".repeat(64 * 1024);
    assert_eq!(
        client.create_entity(oversized).await.unwrap_err().code(),
        Code::OutOfRange
    );
    client
        .create_entity(create("healthy-after-refusal"))
        .await?;
    Ok(())
}

async fn authenticated_tls<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, true).await?;
    let mut client = node.client().await?;
    assert_eq!(
        client
            .create_entity(create("orders"))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    assert_eq!(
        client
            .create_entity(authorized(create("orders")))
            .await?
            .into_inner()
            .path,
        "orders"
    );
    let endpoint = Endpoint::from_shared(node.endpoint.clone())?
        .connect_timeout(DEADLINE)
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(node.certificate.as_ref().unwrap()))
                .domain_name("wrong.example"),
        )?;
    assert!(
        endpoint.connect().await.is_err(),
        "the TLS name is verified"
    );
    let address = node.endpoint.trim_start_matches("https://");
    let mut plaintext = TcpStream::connect(address).await?;
    tokio::io::AsyncWriteExt::write_all(&mut plaintext, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await?;
    let mut bytes = [0; 64];
    let result = timeout(DEADLINE, plaintext.read(&mut bytes)).await?;
    assert!(
        matches!(result, Ok(0) | Err(_)) || bytes[0] == 21,
        "plaintext never reaches the administration handler"
    );
    Ok(())
}

#[tokio::test]
async fn configured_authentication_refuses_a_plaintext_listener() -> TestResult {
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(storage::MemoryStore::default()),
        ManualClock::at(1_000),
    ));
    let service = NativeAdminService::new(broker.handle(), NamespaceName::new("tenant")?)
        .with_shared_access_policy(policy()?, HOST)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    assert!(matches!(
        NativeAdminListener::new(service).serve(listener).await,
        Err(NativeAdminError::AuthenticationRequiresTls)
    ));
    Ok(())
}

#[tokio::test]
async fn a_stalled_tls_handshake_holds_admission_until_its_deadline() -> TestResult {
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(storage::MemoryStore::default()),
        ManualClock::at(1_000),
    ));
    let service = NativeAdminService::new(broker.handle(), NamespaceName::new("tenant")?);
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let admin = NativeAdminListener::new(service)
        .with_tls(cert.pem().as_bytes(), key_pair.serialize_pem().as_bytes())?
        .with_connection_limit(NonZeroUsize::new(1).unwrap())
        .with_handshake_timeout(Duration::from_secs(1));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(admin.serve(listener));
    let mut first = TcpStream::connect(address).await?;
    let mut second = TcpStream::connect(address).await?;
    let mut byte = [0];
    let second_read = timeout(Duration::from_millis(500), second.read(&mut byte)).await?;
    assert!(
        matches!(second_read, Ok(0) | Err(_)),
        "excess admission is refused"
    );
    let first_read = timeout(Duration::from_secs(2), first.read(&mut byte)).await?;
    assert!(
        matches!(first_read, Ok(0) | Err(_)),
        "the handshake has a deadline"
    );
    task.abort();
    let _ = task.await;
    Ok(())
}

macro_rules! backend_tests {
    ($name:ident, $provider:expr) => {
        mod $name {
            use super::*;

            #[tokio::test]
            async fn queue_requests_traverse_the_real_http2_transport() -> TestResult {
                plaintext_round_trip($provider).await
            }

            #[tokio::test]
            async fn management_tokens_are_required_over_verified_tls() -> TestResult {
                authenticated_tls($provider).await
            }
        }
    };
}

backend_tests!(memory, testkit::MemoryProvider::default());
backend_tests!(durable, testkit::DurableProvider::temporary()?);
