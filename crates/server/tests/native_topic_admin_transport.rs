//! Typed topology requests traverse bounded native HTTP/2 and verified TLS.

use std::{error::Error, time::Duration};

use admin_api::v1::{
    CreateEntityRequest, Entity, EntityKind, GetEntityRequest, ListEntitiesRequest,
    QueueConfiguration, SubscriptionConfiguration, TopicConfiguration, UnlimitedTimeToLive,
    UpdateEntityRequest, entity_service_client::EntityServiceClient,
    subscription_configuration::DefaultTimeToLive as SubscriptionTtl,
    topic_configuration::DefaultTimeToLive as TopicTtl,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{EntityPath, NamespaceName, StateMachine};
use hmac::{Hmac, Mac};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use server::{
    AdminTarget, Broker, LocalProposer, ManualClock, NativeAdminListener, NativeAdminService,
};
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
const KEY: &str = "native-topic-transport-secret";
const DEADLINE: Duration = Duration::from_secs(5);

struct Node<P> {
    broker: Broker,
    endpoint: String,
    certificate: Option<String>,
    clock: ManualClock,
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
        let service = NativeAdminService::new(broker.handle(), NamespaceName::new("tenant")?);
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
            clock,
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
        request(input, self.certificate.as_ref().map(|_| sas("")))
    }
}

impl<P> Drop for Node<P> {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

fn sas(path: &str) -> String {
    let audience = if path.is_empty() {
        format!("amqps://{HOST}")
    } else {
        format!("amqps://{HOST}/{path}")
    };
    let resource = byte_serialize(audience.as_bytes()).collect::<String>();
    let expiry = 4_102_444_800_u64;
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).expect("HMAC key");
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature =
        byte_serialize(STANDARD.encode(mac.finalize().into_bytes()).as_bytes()).collect::<String>();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn=manage")
}

fn request<T>(input: T, token: Option<String>) -> Request<T> {
    let mut request = Request::new(input);
    request.set_timeout(DEADLINE);
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert("authorization", token.parse().expect("ASCII SAS token"));
    }
    request
}

fn create(path: &str, kind: EntityKind) -> CreateEntityRequest {
    CreateEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        kind: kind as i32,
        ..Default::default()
    }
}

fn get(path: &str) -> GetEntityRequest {
    GetEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
    }
}

fn list(kind: EntityKind, parent: &str, page_size: u32, page_token: &str) -> ListEntitiesRequest {
    ListEntitiesRequest {
        namespace: "tenant".into(),
        kind: kind as i32,
        parent_topic: parent.into(),
        page_size,
        page_token: page_token.into(),
    }
}

fn topic_only(entity: &Entity) {
    assert_eq!(entity.kind, EntityKind::Topic as i32);
    assert!(entity.topic_config.is_some());
    assert!(entity.queue_config.is_none());
    assert!(entity.subscription_config.is_none());
    assert!(entity.max_size_bytes.is_none());
    assert!(entity.used_logical_bytes.is_none());
}

fn subscription_only(entity: &Entity) {
    assert_eq!(entity.kind, EntityKind::Subscription as i32);
    assert!(entity.subscription_config.is_some());
    assert!(entity.queue_config.is_none());
    assert!(entity.topic_config.is_none());
    assert!(entity.max_size_bytes.is_none());
    assert!(entity.used_logical_bytes.is_none());
}

async fn typed_topology_round_trip<P: StoreProvider>(provider: P, tls: bool) -> TestResult {
    let node = Node::start(provider, tls).await?;
    let mut client = node.client().await?;
    if tls {
        assert_eq!(
            timeout(
                DEADLINE,
                client.create_entity(create("Orders", EntityKind::Topic))
            )
            .await?
            .expect_err("TLS endpoint requires a token")
            .code(),
            Code::Unauthenticated
        );
    }
    let mut input = create("Orders", EntityKind::Topic);
    input.topic_config = Some(TopicConfiguration {
        default_time_to_live: Some(TopicTtl::DefaultTtlMillis(90_000)),
        max_message_bytes: Some(1_024),
        requires_duplicate_detection: Some(true),
        duplicate_detection_history_time_window_millis: Some(300_000),
    });
    let created_topic = timeout(DEADLINE, client.create_entity(node.request(input)))
        .await??
        .into_inner();
    topic_only(&created_topic);
    let topic_config = created_topic
        .topic_config
        .as_ref()
        .expect("topic configuration");
    assert_eq!(topic_config.max_message_bytes, Some(1_024));
    assert_eq!(topic_config.requires_duplicate_detection, Some(true));
    assert!(matches!(
        topic_config.default_time_to_live,
        Some(TopicTtl::DefaultTtlMillis(90_000))
    ));
    let mut sibling = create("Orders-old", EntityKind::Topic);
    sibling.topic_config = Some(TopicConfiguration {
        default_time_to_live: Some(TopicTtl::DefaultTtlUnlimited(UnlimitedTimeToLive {})),
        ..Default::default()
    });
    let sibling = timeout(DEADLINE, client.create_entity(node.request(sibling)))
        .await??
        .into_inner();
    topic_only(&sibling);
    assert!(matches!(
        sibling.topic_config.as_ref().unwrap().default_time_to_live,
        Some(TopicTtl::DefaultTtlUnlimited(_))
    ));
    let mut input = create("Orders/SUBSCRIPTIONS/Alpha", EntityKind::Subscription);
    input.subscription_config = Some(SubscriptionConfiguration {
        lock_duration_millis: Some(17_000),
        max_delivery_count: Some(4),
        default_time_to_live: Some(SubscriptionTtl::DefaultTtlUnlimited(UnlimitedTimeToLive {})),
        max_message_bytes: Some(512),
        requires_session: Some(true),
        dead_lettering_on_message_expiration: Some(true),
        dead_lettering_on_filter_evaluation_exceptions: Some(false),
    });
    let created_sub = timeout(DEADLINE, client.create_entity(node.request(input)))
        .await??
        .into_inner();
    subscription_only(&created_sub);
    assert_eq!(created_sub.path, "Orders/subscriptions/Alpha");
    let config = created_sub
        .subscription_config
        .as_ref()
        .expect("subscription configuration");
    assert_eq!(config.lock_duration_millis, Some(17_000));
    assert_eq!(config.max_delivery_count, Some(4));
    assert_eq!(config.max_message_bytes, Some(512));
    assert_eq!(config.requires_session, Some(true));
    assert_eq!(config.dead_lettering_on_message_expiration, Some(true));
    assert_eq!(
        config.dead_lettering_on_filter_evaluation_exceptions,
        Some(false)
    );
    assert!(matches!(
        config.default_time_to_live,
        Some(SubscriptionTtl::DefaultTtlUnlimited(_))
    ));
    let default_sub = timeout(
        DEADLINE,
        client.create_entity(node.request(create(
            "Orders/subscriptions/Beta",
            EntityKind::Subscription,
        ))),
    )
    .await??
    .into_inner();
    assert_eq!(
        default_sub
            .subscription_config
            .as_ref()
            .expect("config")
            .dead_lettering_on_filter_evaluation_exceptions,
        Some(true)
    );
    let mut enabled = create("Orders-old/subscriptions/Enabled", EntityKind::Subscription);
    enabled.subscription_config = Some(SubscriptionConfiguration {
        dead_lettering_on_filter_evaluation_exceptions: Some(true),
        ..Default::default()
    });
    let enabled = timeout(DEADLINE, client.create_entity(node.request(enabled)))
        .await??
        .into_inner();
    assert_eq!(
        enabled
            .subscription_config
            .as_ref()
            .expect("config")
            .dead_lettering_on_filter_evaluation_exceptions,
        Some(true)
    );
    let created_topic = timeout(
        DEADLINE,
        client.update_entity(node.request(UpdateEntityRequest {
            namespace: "tenant".into(),
            path: "Orders".into(),
            topic_config: Some(TopicConfiguration {
                default_time_to_live: Some(TopicTtl::DefaultTtlUnlimited(UnlimitedTimeToLive {})),
                max_message_bytes: Some(2_048),
                ..Default::default()
            }),
            ..Default::default()
        })),
    )
    .await??
    .into_inner();
    topic_only(&created_topic);
    let config = created_topic.topic_config.as_ref().expect("updated topic");
    assert_eq!(config.max_message_bytes, Some(2_048));
    assert_eq!(config.requires_duplicate_detection, Some(true));
    assert_eq!(
        config.duplicate_detection_history_time_window_millis,
        Some(300_000)
    );
    assert!(matches!(
        config.default_time_to_live,
        Some(TopicTtl::DefaultTtlUnlimited(_))
    ));
    let created_sub = timeout(
        DEADLINE,
        client.update_entity(node.request(UpdateEntityRequest {
            namespace: "tenant".into(),
            path: "Orders/SUBSCRIPTIONS/Alpha".into(),
            subscription_config: Some(SubscriptionConfiguration {
                lock_duration_millis: Some(19_000),
                default_time_to_live: Some(SubscriptionTtl::DefaultTtlMillis(45_000)),
                dead_lettering_on_message_expiration: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        })),
    )
    .await??
    .into_inner();
    subscription_only(&created_sub);
    let config = created_sub
        .subscription_config
        .as_ref()
        .expect("updated subscription");
    assert_eq!(config.lock_duration_millis, Some(19_000));
    assert_eq!(config.max_delivery_count, Some(4));
    assert_eq!(config.requires_session, Some(true));
    assert_eq!(config.dead_lettering_on_message_expiration, Some(false));
    assert_eq!(
        config.dead_lettering_on_filter_evaluation_exceptions,
        Some(false)
    );
    assert!(matches!(
        config.default_time_to_live,
        Some(SubscriptionTtl::DefaultTtlMillis(45_000))
    ));
    for input in [
        UpdateEntityRequest {
            namespace: "tenant".into(),
            path: "Orders".into(),
            topic_config: Some(TopicConfiguration {
                requires_duplicate_detection: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        },
        UpdateEntityRequest {
            namespace: "tenant".into(),
            path: "Orders/subscriptions/Alpha".into(),
            subscription_config: Some(SubscriptionConfiguration {
                requires_session: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        },
    ] {
        assert_eq!(
            timeout(DEADLINE, client.update_entity(node.request(input)))
                .await?
                .expect_err("creation-only setting cannot change")
                .code(),
            Code::FailedPrecondition
        );
    }
    node.clock.set(0);
    assert_eq!(
        timeout(
            DEADLINE,
            client.get_entity(node.request(get(&enabled.path)))
        )
        .await??
        .into_inner(),
        enabled
    );
    assert_eq!(
        timeout(DEADLINE, client.get_entity(node.request(get("Orders"))))
            .await??
            .into_inner(),
        created_topic
    );
    assert_eq!(
        timeout(
            DEADLINE,
            client.get_entity(node.request(get("Orders/Subscriptions/Alpha")))
        )
        .await??
        .into_inner(),
        created_sub
    );
    let first = timeout(
        DEADLINE,
        client.list_entities(node.request(list(EntityKind::Topic, "", 1, ""))),
    )
    .await??
    .into_inner();
    assert_eq!(first.entities, vec![created_topic]);
    assert!(first.next_page_token.starts_with("topic.v1."));
    let second = timeout(
        DEADLINE,
        client.list_entities(node.request(list(EntityKind::Topic, "", 1, &first.next_page_token))),
    )
    .await??
    .into_inner();
    assert_eq!(second.entities, vec![sibling]);
    assert!(second.next_page_token.is_empty());
    let first_sub = timeout(
        DEADLINE,
        client.list_entities(node.request(list(EntityKind::Subscription, "Orders", 1, ""))),
    )
    .await??
    .into_inner();
    assert_eq!(first_sub.entities, vec![created_sub]);
    assert!(first_sub.next_page_token.starts_with("subscription.v1."));
    let second_sub = timeout(
        DEADLINE,
        client.list_entities(node.request(list(
            EntityKind::Subscription,
            "Orders",
            1,
            &first_sub.next_page_token,
        ))),
    )
    .await??
    .into_inner();
    assert_eq!(second_sub.entities.len(), 1);
    assert_eq!(second_sub.entities[0].path, "Orders/subscriptions/Beta");
    subscription_only(&second_sub.entities[0]);
    assert!(second_sub.next_page_token.is_empty());
    assert_eq!(
        timeout(
            DEADLINE,
            client.list_entities(node.request(list(
                EntityKind::Topic,
                "",
                1,
                &first_sub.next_page_token
            )))
        )
        .await?
        .expect_err("wrong cursor family")
        .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        timeout(
            DEADLINE,
            client.list_entities(node.request(list(
                EntityKind::Subscription,
                "Orders-old",
                1,
                &first_sub.next_page_token
            )))
        )
        .await?
        .expect_err("wrong cursor parent")
        .code(),
        Code::InvalidArgument
    );
    node.clock.set(10_001);
    let queue = timeout(
        DEADLINE,
        client.create_entity(node.request(create("Healthy", EntityKind::Queue))),
    )
    .await??
    .into_inner();
    assert_eq!(queue.kind, EntityKind::Queue as i32);
    assert!(queue.queue_config.is_some());
    assert!(queue.topic_config.is_none());
    assert!(queue.subscription_config.is_none());
    let queue = timeout(
        DEADLINE,
        client.update_entity(node.request(UpdateEntityRequest {
            namespace: "tenant".into(),
            path: "Healthy".into(),
            queue_config: Some(QueueConfiguration {
                lock_duration_millis: Some(29_000),
                ..Default::default()
            }),
            ..Default::default()
        })),
    )
    .await??
    .into_inner();
    assert_eq!(queue.kind, EntityKind::Queue as i32);
    assert_eq!(
        queue
            .queue_config
            .as_ref()
            .expect("legacy queue update")
            .lock_duration_millis,
        Some(29_000)
    );
    assert_eq!(
        timeout(DEADLINE, client.get_entity(node.request(get("Healthy"))))
            .await??
            .into_inner(),
        queue
    );
    let queues = timeout(
        DEADLINE,
        client.list_entities(node.request(list(EntityKind::Unspecified, "", 100, ""))),
    )
    .await??
    .into_inner();
    assert_eq!(queues.entities, vec![queue]);
    assert!(queues.next_page_token.is_empty());
    let Some(protocol_amqp::EntityMetadata::Topic(config)) =
        node.broker.handle().admin_entity_metadata_blocking(
            NamespaceName::new("tenant")?,
            AdminTarget::Primary(EntityPath::new("Orders")?),
        )?
    else {
        panic!("typed topic creation committed")
    };
    assert!(config.requires_duplicate_detection);
    assert_eq!(config.max_message_bytes, 2_048);
    Ok(())
}

async fn plaintext_typed_create_get_and_list<P: StoreProvider>(provider: P) -> TestResult {
    typed_topology_round_trip(provider, false).await
}

async fn verified_tls_typed_create_get_and_list<P: StoreProvider>(provider: P) -> TestResult {
    typed_topology_round_trip(provider, true).await
}

async fn tls_exact_child_token_cannot_list_or_access_neighbor_entities<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, true).await?;
    let mut client = node.client().await?;
    timeout(
        DEADLINE,
        client.create_entity(node.request(create("Orders", EntityKind::Topic))),
    )
    .await??;
    let exact = "Orders/subscriptions/Private";
    let child = timeout(
        DEADLINE,
        client.create_entity(request(
            create("Orders/Subscriptions/Private", EntityKind::Subscription),
            Some(sas(exact)),
        )),
    )
    .await??
    .into_inner();
    assert_eq!(child.path, exact);
    subscription_only(&child);
    node.clock.set(10_001);
    let child = timeout(
        DEADLINE,
        client.update_entity(request(
            UpdateEntityRequest {
                namespace: "tenant".into(),
                path: "Orders/SUBSCRIPTIONS/Private".into(),
                subscription_config: Some(SubscriptionConfiguration {
                    max_delivery_count: Some(4),
                    dead_lettering_on_filter_evaluation_exceptions: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Some(sas(exact)),
        )),
    )
    .await??
    .into_inner();
    assert_eq!(
        child
            .subscription_config
            .as_ref()
            .expect("updated child")
            .max_delivery_count,
        Some(4)
    );
    assert_eq!(
        child
            .subscription_config
            .as_ref()
            .expect("updated child")
            .dead_lettering_on_filter_evaluation_exceptions,
        Some(false)
    );
    node.clock.set(0);
    for denied in [
        "Orders",
        "Orders/subscriptions/Other",
        "orders/subscriptions/Private",
    ] {
        assert_eq!(
            timeout(
                DEADLINE,
                client.get_entity(request(get(denied), Some(sas(exact))))
            )
            .await?
            .expect_err("exact child token cannot access sibling or parent")
            .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            timeout(
                DEADLINE,
                client.update_entity(request(
                    UpdateEntityRequest {
                        namespace: "tenant".into(),
                        path: denied.into(),
                        subscription_config: Some(SubscriptionConfiguration::default()),
                        ..Default::default()
                    },
                    Some(sas(exact))
                ))
            )
            .await?
            .expect_err("exact child token cannot update sibling or parent")
            .code(),
            Code::PermissionDenied
        );
    }
    assert_eq!(
        timeout(
            DEADLINE,
            client.list_entities(request(
                list(EntityKind::Subscription, "Orders", 1, ""),
                Some(sas(exact))
            ))
        )
        .await?
        .expect_err("exact child token cannot enumerate siblings")
        .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        timeout(
            DEADLINE,
            client.get_entity(request(
                get(exact),
                Some(sas("Orders/subscriptions/Private/$management"))
            ))
        )
        .await?
        .expect_err("management endpoint token cannot administer base")
        .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        timeout(
            DEADLINE,
            client.get_entity(request(
                get("Orders/SUBSCRIPTIONS/Private"),
                Some(sas(exact))
            ))
        )
        .await??
        .into_inner(),
        child
    );
    node.clock.set(10_001);
    let queue = timeout(
        DEADLINE,
        client.create_entity(node.request(create("Healthy", EntityKind::Queue))),
    )
    .await??
    .into_inner();
    assert_eq!(
        timeout(DEADLINE, client.get_entity(node.request(get("Healthy"))))
            .await??
            .into_inner(),
        queue
    );
    Ok(())
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
    plaintext_typed_create_get_and_list,
    verified_tls_typed_create_get_and_list,
    tls_exact_child_token_cannot_list_or_access_neighbor_entities,
}
