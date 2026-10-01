//! Empty queue progress pages survive native HTTP/2 and TLS round trips.

use std::{collections::HashSet, error::Error, time::Duration};

use admin_api::v1::{
    CreateEntityRequest, EntityKind, GetEntityRequest, ListEntitiesRequest,
    entity_service_client::EntityServiceClient,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{
    CommandKind, EntityPath, NamespaceName, QueueConfig, StateMachine, SubscriptionConfig,
    SubscriptionName, TopicConfig,
};
use hmac::{Hmac, Mac};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use server::{Broker, LocalProposer, ManualClock, NativeAdminListener, NativeAdminService};
use sha2::Sha256;
use testkit::StoreProvider;
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};
use tonic::{
    Code, Request,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use url::form_urlencoded::byte_serialize;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "queue-scan-transport-secret";
const DEADLINE: Duration = Duration::from_secs(5);

struct Node<P> {
    broker: Broker,
    clock: ManualClock,
    endpoint: String,
    certificate: Option<String>,
    listener: JoinHandle<()>,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    async fn start(provider: P, tls: bool) -> TestResult<Self> {
        let clock = ManualClock::at(10_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(provider.open()?),
            clock.clone(),
        ));
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("a-hidden")?;
        broker.handle().submit_blocking(
            namespace.clone(),
            topic.clone(),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        for index in 0..domain::MAX_TOPIC_SUBSCRIPTIONS {
            broker.handle().submit_blocking(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(format!("Member-{index:02}"))?,
                    config: SubscriptionConfig::default(),
                },
            )?;
        }
        for queue in ["z-visible", "zz-last"] {
            broker.handle().submit_blocking(
                namespace.clone(),
                EntityPath::new(queue)?,
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            )?;
        }
        let service = NativeAdminService::new(broker.handle(), namespace);
        let service = if tls {
            let policy = SharedAccessPolicy::new([SharedAccessRule::new(
                "manage",
                ResourceScope::namespace(HOST)?,
                SharedAccessKey::new(KEY)?,
                None,
                PermissionSet::MANAGE,
            )?])?;
            service.with_shared_access_policy(policy, HOST)?
        } else {
            service
        };
        let mut admin = NativeAdminListener::new(service);
        let certificate = if tls {
            let CertifiedKey { cert, key_pair } =
                generate_simple_self_signed(vec!["localhost".into()])?;
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
            clock,
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
        Ok(EntityServiceClient::new(
            timeout(DEADLINE, endpoint.connect()).await??,
        ))
    }

    fn request<T>(&self, input: T) -> Request<T> {
        let mut request = Request::new(input);
        request.set_timeout(DEADLINE);
        if self.certificate.is_some() {
            request
                .metadata_mut()
                .insert("authorization", token().parse().expect("ASCII SAS token"));
        }
        request
    }
}

impl<P> Drop for Node<P> {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

fn token() -> String {
    let resource = byte_serialize(format!("amqps://{HOST}").as_bytes()).collect::<String>();
    let expiry = 4_102_444_800_u64;
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).expect("HMAC key");
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature =
        byte_serialize(STANDARD.encode(mac.finalize().into_bytes()).as_bytes()).collect::<String>();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn=manage")
}

fn list(cursor: &str, explicit_kind: bool) -> ListEntitiesRequest {
    ListEntitiesRequest {
        namespace: "tenant".into(),
        page_size: 1,
        page_token: cursor.into(),
        kind: if explicit_kind {
            EntityKind::Queue as i32
        } else {
            EntityKind::Unspecified as i32
        },
        ..Default::default()
    }
}

async fn empty_progress_resumes_on_one_connection<P: StoreProvider>(
    provider: P,
    tls: bool,
) -> TestResult {
    let node = Node::start(provider, tls).await?;
    let mut client = node.client().await?;
    if tls {
        assert_eq!(
            timeout(DEADLINE, client.list_entities(list("", false)))
                .await?
                .expect_err("TLS endpoint requires namespace Manage token")
                .code(),
            Code::Unauthenticated
        );
    }
    node.clock.set(0);
    let applied = node.broker.handle().last_applied_blocking()?;
    let first = timeout(
        DEADLINE,
        client.list_entities(node.request(list("", false))),
    )
    .await??
    .into_inner();
    assert!(
        first.entities.is_empty(),
        "bounded hidden prefix is a valid empty visible page"
    );
    assert!(first.next_page_token.starts_with("queue.scan.v1."));
    let mut cursor = first.next_page_token;
    let mut cursors = HashSet::new();
    cursors.insert(cursor.clone());
    let mut found = Vec::new();
    let mut exhausted = false;
    for index in 0..12 {
        let page = timeout(
            DEADLINE,
            client.list_entities(node.request(list(&cursor, index % 2 == 0))),
        )
        .await??
        .into_inner();
        assert!(page.entities.len() <= 1);
        for entity in page.entities {
            assert_eq!(entity.kind, EntityKind::Queue as i32);
            assert!(entity.queue_config.is_some());
            assert!(entity.topic_config.is_none());
            assert!(entity.subscription_config.is_none());
            assert!(matches!(entity.path.as_str(), "z-visible" | "zz-last"));
            found.push(entity.path);
        }
        if page.next_page_token.is_empty() {
            exhausted = true;
            break;
        }
        assert!(page.next_page_token.len() <= 512);
        assert!(
            cursors.insert(page.next_page_token.clone()),
            "every continuation advances"
        );
        cursor = page.next_page_token;
    }
    assert!(
        exhausted,
        "finite hidden prefix and two queues must reach the end"
    );
    assert_eq!(found, ["z-visible", "zz-last"]);
    assert_eq!(
        node.broker.handle().last_applied_blocking()?,
        applied,
        "scanning never stamps a command at the regressed clock"
    );
    let healthy = timeout(
        DEADLINE,
        client.get_entity(node.request(GetEntityRequest {
            namespace: "tenant".into(),
            path: "z-visible".into(),
        })),
    )
    .await??
    .into_inner();
    assert_eq!(healthy.kind, EntityKind::Queue as i32);
    assert_eq!(healthy.path, "z-visible");
    assert_eq!(node.broker.handle().last_applied_blocking()?, applied);

    node.clock.set(10_001);
    for path in ["0-first", "0-second"] {
        timeout(
            DEADLINE,
            client.create_entity(node.request(CreateEntityRequest {
                namespace: "tenant".into(),
                path: path.into(),
                kind: EntityKind::Queue as i32,
                ..Default::default()
            })),
        )
        .await??;
    }
    node.clock.set(0);
    let dense = timeout(DEADLINE, client.list_entities(node.request(list("", true))))
        .await??
        .into_inner();
    assert_eq!(dense.entities.len(), 1);
    assert_eq!(dense.entities[0].path, "0-first");
    assert!(
        dense.next_page_token.starts_with("v1."),
        "dense pages retain legacy cursor format"
    );
    let second = timeout(
        DEADLINE,
        client.list_entities(node.request(list(&dense.next_page_token, false))),
    )
    .await??
    .into_inner();
    assert_eq!(second.entities.len(), 1);
    assert_eq!(second.entities[0].path, "0-second");
    let healthy = timeout(
        DEADLINE,
        client.get_entity(node.request(GetEntityRequest {
            namespace: "tenant".into(),
            path: "0-second".into(),
        })),
    )
    .await??
    .into_inner();
    assert_eq!(healthy.path, "0-second");
    Ok(())
}

async fn plaintext_empty_progress_and_legacy_dense_pages<P: StoreProvider>(
    provider: P,
) -> TestResult {
    empty_progress_resumes_on_one_connection(provider, false).await
}

async fn verified_tls_empty_progress_and_legacy_dense_pages<P: StoreProvider>(
    provider: P,
) -> TestResult {
    empty_progress_resumes_on_one_connection(provider, true).await
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                tokio::time::timeout(super::DEADLINE * 8,
                    super::$case(::testkit::MemoryProvider::new())).await?
            })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                tokio::time::timeout(super::DEADLINE * 8,
                    super::$case(::testkit::DurableProvider::temporary()?)).await?
            })+ }
    };
}

for_each_backend! {
    plaintext_empty_progress_and_legacy_dense_pages,
    verified_tls_empty_progress_and_legacy_dense_pages,
}
