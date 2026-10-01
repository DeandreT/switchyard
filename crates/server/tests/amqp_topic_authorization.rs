//! Scoped topic/subscription credentials are checked before topology discovery.

#[path = "amqp_topic_authorization/rules.rs"]
mod rules;

use std::{
    error::Error,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use amqp::{
    ApplicationProperties, Body, ClientConnection as Connection, ClientReceiver as Receiver,
    ClientSender as Sender, ClientSession as Session, Message, OrderedMap, Outcome, Properties,
    SaslInit, Symbol, Value,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{
    CommandKind, CommandOutcome, EntityPath, NamespaceName, QueueConfig, ReceiveMode, StateMachine,
    SubscriptionConfig, SubscriptionName, TopicConfig,
};
use hmac::{Hmac, Mac};
use protocol_amqp::{Attachment, BrokerRejection, EntityMetadata};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::{
    ClientConfig, RootCertStore,
    crypto::ring,
    pki_types::{CertificateDer, ServerName},
    version::{TLS12, TLS13},
};
use server::{Broker, BrokerHandle, LocalProposer, ManualClock};
use sha2::Sha256;
use storage::MemoryStore;
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_rustls::TlsConnector;
use url::form_urlencoded::byte_serialize;

const HOST: &str = "tenant.servicebus.windows.net";
const RULE: &str = "scoped-rule";
const KEY: &str = "scoped-secret";
const DEADLINE: Duration = Duration::from_secs(30);

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Clone)]
struct CountingBroker {
    inner: BrokerHandle,
    reads: Arc<Mutex<Vec<(NamespaceName, Attachment)>>>,
}

impl protocol_amqp::Broker for CountingBroker {
    async fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<protocol_amqp::EntityAdmission>, BrokerRejection> {
        self.reads
            .lock()
            .expect("metadata reads")
            .push((namespace.clone(), target.clone()));
        protocol_amqp::Broker::bind(&self.inner, namespace, target).await
    }

    fn submit_fenced(
        &self,
        binding: domain::EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        protocol_amqp::Broker::submit_fenced(&self.inner, binding, entity, kind)
    }

    fn rules_fenced(
        &self,
        binding: domain::EntityBinding,
        topic: EntityPath,
        subscription: domain::SubscriptionName,
    ) -> impl Future<Output = Result<Vec<domain::RuleDefinition>, BrokerRejection>> + Send {
        protocol_amqp::Broker::rules_fenced(&self.inner, binding, topic, subscription)
    }

    fn rules(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: domain::SubscriptionName,
    ) -> impl std::future::Future<Output = Result<Vec<domain::RuleDefinition>, BrokerRejection>> + Send
    {
        protocol_amqp::Broker::rules(&self.inner, namespace, topic, subscription)
    }

    fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        protocol_amqp::Broker::submit(&self.inner, namespace, entity, kind)
    }

    async fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        self.reads
            .lock()
            .expect("metadata reads")
            .push((namespace.clone(), target.clone()));
        protocol_amqp::Broker::entity_metadata(&self.inner, namespace, target).await
    }

    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        protocol_amqp::Broker::deliverable(&self.inner, namespace, entity)
    }
}

struct Node {
    broker: Broker,
    address: String,
    certificate: CertificateDer<'static>,
    reads: Arc<Mutex<Vec<(NamespaceName, Attachment)>>>,
    next_name: AtomicUsize,
    listener: JoinHandle<()>,
}

impl Node {
    async fn start(rule_path: &str, permissions: PermissionSet) -> TestResult<Self> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            ManualClock::at(1_000),
        ));
        let namespace = NamespaceName::new("tenant")?;
        let maximum_name = "a".repeat(domain::MAX_SUBSCRIPTION_NAME_BYTES);
        for topic in [
            "Orders",
            "orders",
            "Orders-old",
            "Subscriptions",
            "subscriptions",
            "Outer/Subscriptions",
        ] {
            broker.handle().submit_blocking(
                namespace.clone(),
                EntityPath::new(topic)?,
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
            )?;
            for name in [
                "Accounting",
                "accounting",
                "Billing",
                "Subscriptions",
                "subscriptions",
                "a",
                "a.b-c_d",
                maximum_name.as_str(),
            ] {
                broker.handle().submit_blocking(
                    namespace.clone(),
                    EntityPath::new(topic)?,
                    CommandKind::CreateSubscription {
                        name: SubscriptionName::new(name)?,
                        config: SubscriptionConfig::default(),
                    },
                )?;
            }
            broker.handle().submit_blocking(
                namespace.clone(),
                EntityPath::new(topic)?,
                send("seed"),
            )?;
        }
        broker.handle().submit_blocking(
            namespace.clone(),
            EntityPath::new("Ordinary")?,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        )?;
        broker.handle().submit_blocking(
            namespace.clone(),
            EntityPath::new("Ordinary")?,
            send("seed"),
        )?;
        let scope = if rule_path.is_empty() {
            ResourceScope::namespace(HOST)?
        } else {
            ResourceScope::entity(HOST, rule_path)?
        };
        let rule =
            SharedAccessRule::new(RULE, scope, SharedAccessKey::new(KEY)?, None, permissions)?;
        let authentication =
            protocol_amqp::SharedAccessAuthentication::new(SharedAccessPolicy::new([rule])?, HOST)?
                .with_authorization_timeout(DEADLINE);
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec!["localhost".to_owned()])?;
        let tls = protocol_amqp::tls_server_config(
            cert.pem().as_bytes(),
            key_pair.serialize_pem().as_bytes(),
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let reads = Arc::new(Mutex::new(Vec::new()));
        let counting = CountingBroker {
            inner: broker.handle(),
            reads: Arc::clone(&reads),
        };
        let task = tokio::spawn(async move {
            let _ = protocol_amqp::AmqpListener::new(counting, namespace)
                .with_tls(tls)
                .with_shared_access_authentication(authentication)
                .serve(listener)
                .await;
        });
        Ok(Self {
            broker,
            address,
            certificate: cert.der().clone(),
            reads,
            next_name: AtomicUsize::new(0),
            listener: task,
        })
    }

    fn name(&self, prefix: &str) -> String {
        format!("{prefix}-{}", self.next_name.fetch_add(1, Ordering::SeqCst))
    }
    fn clear_reads(&self) {
        self.reads.lock().expect("metadata reads").clear();
    }
    fn reads(&self) -> Vec<EntityPath> {
        self.reads
            .lock()
            .expect("metadata reads")
            .iter()
            .map(|(namespace, target)| {
                assert_eq!(namespace.as_str(), "tenant");
                target
                    .canonical_entity()
                    .expect("canonical metadata target")
            })
            .collect()
    }

    async fn connect(&self) -> TestResult<Connection> {
        let mut roots = RootCertStore::empty();
        roots.add(self.certificate.clone())?;
        let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_protocol_versions(&[&TLS13, &TLS12])?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tcp = TcpStream::connect(&self.address).await?;
        let tls = TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from("localhost")?, tcp)
            .await?;
        Ok(Connection::builder()
            .container_id(self.name("client"))
            .sasl(SaslInit {
                mechanism: Symbol::from("ANONYMOUS"),
                initial_response: None,
                hostname: Some(HOST.to_owned()),
            })
            .open_with_stream(tls)
            .await?)
    }

    fn submit(&self, entity: &str, kind: CommandKind) -> TestResult<CommandOutcome> {
        Ok(self.broker.handle().submit_blocking(
            NamespaceName::new("tenant")?,
            EntityPath::new(entity)?,
            kind,
        )?)
    }

    fn dead_letter(&self, topic: &str, name: &str) -> TestResult {
        let entity = EntityPath::new(topic)?.subscription(&SubscriptionName::new(name)?)?;
        let CommandOutcome::Received(Some(delivery)) = self.submit(
            entity.as_str(),
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
        )?
        else {
            panic!("seed delivery exists")
        };
        let lock = delivery.lock.expect("peek-lock delivery");
        assert_eq!(
            self.submit(
                entity.as_str(),
                CommandKind::DeadLetter {
                    sequence: delivery.sequence,
                    lock_token: lock.token,
                    reason: "auth-probe".to_owned(),
                    description: String::new()
                }
            )?,
            CommandOutcome::DeadLettered
        );
        Ok(())
    }

    async fn authorize(&self, connection: &mut Connection, path: &str) -> TestResult {
        let mut session = Session::begin(connection).await?;
        let audience = audience(path);
        assert_eq!(
            put_token(self, &mut session, &audience, sas_token(&audience)).await?,
            202
        );
        session.end().await?;
        Ok(())
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

fn send(id: &str) -> CommandKind {
    CommandKind::Send {
        message_id: id.to_owned(),
        body: b"seed".to_vec(),
        time_to_live_millis: None,
        session_id: None,
    }
}

fn audience(path: &str) -> String {
    if path.is_empty() {
        format!("amqps://{HOST}")
    } else {
        format!("amqps://{HOST}/{path}")
    }
}

fn sas_token(audience: &str) -> String {
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs()
        + 300;
    let encoded_resource: String = byte_serialize(audience.as_bytes()).collect();
    let mut hmac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).expect("HMAC key");
    hmac.update(format!("{encoded_resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(hmac.finalize().into_bytes());
    let encoded_signature: String = byte_serialize(signature.as_bytes()).collect();
    format!(
        "SharedAccessSignature sr={encoded_resource}&sig={encoded_signature}&se={expiry}&skn={RULE}"
    )
}

async fn put_token(
    node: &Node,
    session: &mut Session,
    audience: &str,
    token: String,
) -> TestResult<i32> {
    let reply_to = node.name("cbs-reply");
    let mut requests =
        Sender::attach(session, node.name("cbs-request"), protocol_amqp::CBS_NODE).await?;
    let mut responses = Receiver::builder()
        .name(node.name("cbs-response"))
        .source(protocol_amqp::CBS_NODE)
        .target(reply_to.clone())
        .attach(session)
        .await?;
    let id = node.name("token");
    let request = Message::builder()
        .properties(Properties {
            message_id: Some(id.clone().into()),
            reply_to: Some(reply_to),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert("operation", "put-token".to_owned())
                .insert("type", "servicebus.windows.net:sastoken".to_owned())
                .insert("name", audience.to_owned())
                .build(),
        )
        .body(Body::Value(Value::String(token)))
        .build();
    assert!(matches!(
        requests.send(request).await?,
        Outcome::Accepted(_)
    ));
    let response = responses.recv().await?;
    assert_eq!(
        response
            .message()
            .properties
            .as_ref()
            .and_then(|properties| properties.correlation_id.clone()),
        Some(id.into())
    );
    let status = match response
        .message()
        .application_properties
        .as_ref()
        .and_then(|properties| properties.get("status-code"))
    {
        Some(Value::Int(status)) => *status,
        other => panic!("CBS status expected, got {other:?}"),
    };
    responses.accept(&response).await?;
    requests.close().await?;
    responses.close().await?;
    Ok(status)
}

async fn receive(node: &Node, connection: &mut Connection, path: &str) -> TestResult {
    let mut session = Session::begin(connection).await?;
    let mut receiver = Receiver::attach(&mut session, node.name("receiver"), path).await?;
    let delivery = receiver.recv().await?;
    assert!(
        matches!(&delivery.message().body, Body::Data(sections) if sections.len() == 1 && sections[0].as_ref() == b"seed")
    );
    receiver.accept(&delivery).await?;
    receiver.close().await?;
    session.end().await?;
    Ok(())
}

async fn refuse_receiver(node: &Node, connection: &mut Connection, path: &str) -> TestResult {
    let mut session = Session::begin(connection).await?;
    if let Ok(mut receiver) =
        Receiver::attach(&mut session, node.name("refused-receiver"), path).await
    {
        assert!(
            receiver.recv().await.is_err(),
            "receiver unexpectedly authorized: {path}"
        );
    }
    let _ = session.end().await;
    Ok(())
}

async fn refuse_sender(node: &Node, connection: &mut Connection, path: &str) -> TestResult {
    let mut session = Session::begin(connection).await?;
    if let Ok(mut sender) = Sender::attach(&mut session, node.name("refused-sender"), path).await {
        assert!(
            sender
                .send(Message::data(b"forbidden".to_vec()))
                .await
                .is_err(),
            "sender unexpectedly authorized: {path}"
        );
    }
    let _ = session.end().await;
    Ok(())
}

async fn peek(node: &Node, connection: &mut Connection, path: &str) -> TestResult {
    let mut session = Session::begin(connection).await?;
    let reply_to = node.name("management-reply");
    let mut responses = Receiver::builder()
        .name(node.name("management-response"))
        .source(path)
        .target(reply_to.clone())
        .attach(&mut session)
        .await?;
    let mut requests = Sender::attach(&mut session, node.name("management-request"), path).await?;
    let id = node.name("peek");
    let mut body = OrderedMap::new();
    body.insert(
        Value::String(protocol_amqp::FROM_SEQUENCE_NUMBER.to_owned()),
        Value::Long(0),
    );
    body.insert(
        Value::String(protocol_amqp::MESSAGE_COUNT.to_owned()),
        Value::Int(1),
    );
    let request = Message::builder()
        .properties(Properties {
            message_id: Some(id.clone().into()),
            reply_to: Some(reply_to),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert(
                    protocol_amqp::OPERATION_PROPERTY,
                    protocol_amqp::PEEK_MESSAGE_OPERATION.to_owned(),
                )
                .build(),
        )
        .body(Body::Value(Value::Map(body)))
        .build();
    assert!(matches!(
        requests.send(request).await?,
        Outcome::Accepted(_)
    ));
    let response = responses.recv().await?;
    assert_eq!(
        response
            .message()
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY)),
        Some(&Value::Int(200))
    );
    assert_eq!(
        response
            .message()
            .properties
            .as_ref()
            .and_then(|properties| properties.correlation_id.clone()),
        Some(id.into())
    );
    responses.accept(&response).await?;
    requests.close().await?;
    responses.close().await?;
    session.end().await?;
    Ok(())
}

async fn scoped_subscription_denies_existing_and_missing_siblings_before_metadata() -> TestResult {
    let node = Node::start("Orders/subscriptions/Accounting", PermissionSet::LISTEN).await?;
    let mut connection = node.connect().await?;
    node.authorize(&mut connection, "Orders/Subscriptions/Accounting")
        .await?;
    node.clear_reads();
    for path in [
        "Orders/Subscriptions/Billing",
        "Orders/Subscriptions/Missing",
        "Orders/Subscriptions/accounting",
        "orders/Subscriptions/Accounting",
        "Orders-old/Subscriptions/Accounting",
        "Ordinary",
    ] {
        refuse_receiver(&node, &mut connection, path).await?;
        assert!(
            node.reads().is_empty(),
            "authorization queried metadata for {path}"
        );
    }
    refuse_sender(&node, &mut connection, "Orders").await?;
    assert!(node.reads().is_empty());
    receive(&node, &mut connection, "Orders/subscriptions/Accounting").await?;
    assert_eq!(
        node.reads(),
        vec![EntityPath::new("Orders/subscriptions/Accounting")?]
    );
    connection.close().await?;
    Ok(())
}

async fn topic_scope_inherits_subscriptions_but_not_neighbor_topics() -> TestResult {
    let node = Node::start("Orders", PermissionSet::LISTEN).await?;
    let mut connection = node.connect().await?;
    node.authorize(&mut connection, "Orders").await?;
    node.clear_reads();
    refuse_receiver(
        &node,
        &mut connection,
        "Orders-old/Subscriptions/Accounting",
    )
    .await?;
    assert!(node.reads().is_empty());
    receive(&node, &mut connection, "Orders/Subscriptions/Accounting").await?;
    receive(&node, &mut connection, "Orders/SUBSCRIPTIONS/Billing").await?;
    assert_eq!(
        node.reads(),
        vec![
            EntityPath::new("Orders/subscriptions/Accounting")?,
            EntityPath::new("Orders/subscriptions/Billing")?
        ]
    );
    connection.close().await?;
    Ok(())
}

async fn namespace_scope_inherits_queues_and_subscriptions() -> TestResult {
    let node = Node::start("", PermissionSet::LISTEN).await?;
    let mut connection = node.connect().await?;
    node.authorize(&mut connection, "").await?;
    node.clear_reads();
    for path in [
        "Orders/Subscriptions/Accounting",
        "orders/Subscriptions/accounting",
        "Ordinary",
    ] {
        receive(&node, &mut connection, path).await?;
    }
    assert_eq!(node.reads().len(), 3);
    connection.close().await?;
    Ok(())
}

async fn exact_dlq_scope_does_not_authorize_its_base_or_sibling() -> TestResult {
    let node = Node::start(
        "Orders/subscriptions/Accounting/$deadletterqueue",
        PermissionSet::LISTEN,
    )
    .await?;
    node.dead_letter("Orders", "Accounting")?;
    let mut connection = node.connect().await?;
    node.authorize(
        &mut connection,
        "Orders/Subscriptions/Accounting/$DeadLetterQueue",
    )
    .await?;
    node.clear_reads();
    for path in [
        "Orders/Subscriptions/Accounting",
        "Orders/Subscriptions/Billing/$DeadLetterQueue",
        "Orders/Subscriptions/accounting/$DeadLetterQueue",
    ] {
        refuse_receiver(&node, &mut connection, path).await?;
        assert!(node.reads().is_empty());
    }
    receive(
        &node,
        &mut connection,
        "Orders/Subscriptions/Accounting/$DeadLetterQueue",
    )
    .await?;
    assert_eq!(
        node.reads(),
        vec![EntityPath::new(
            "Orders/subscriptions/Accounting/$deadletterqueue"
        )?]
    );
    connection.close().await?;
    Ok(())
}

async fn exact_management_scope_remains_management_only() -> TestResult {
    let node = Node::start(
        "Orders/subscriptions/Accounting/$management",
        PermissionSet::LISTEN,
    )
    .await?;
    let mut connection = node.connect().await?;
    node.authorize(
        &mut connection,
        "Orders/Subscriptions/Accounting/$management",
    )
    .await?;
    node.clear_reads();
    for path in [
        "Orders/Subscriptions/Accounting",
        "Orders/Subscriptions/Accounting/$DeadLetterQueue",
        "Orders/Subscriptions/Billing/$management",
    ] {
        refuse_receiver(&node, &mut connection, path).await?;
        assert!(node.reads().is_empty());
    }
    peek(
        &node,
        &mut connection,
        "Orders/Subscriptions/Accounting/$MaNaGeMeNt",
    )
    .await?;
    assert_eq!(
        node.reads(),
        vec![EntityPath::new("Orders/subscriptions/Accounting")?; 2]
    );
    connection.close().await?;
    Ok(())
}

async fn base_subscription_scope_inherits_dlq_and_management() -> TestResult {
    let node = Node::start("Orders/Subscriptions/Accounting", PermissionSet::LISTEN).await?;
    node.dead_letter("Orders", "Accounting")?;
    node.submit("Orders", send("second"))?;
    let mut connection = node.connect().await?;
    node.authorize(&mut connection, "Orders/subscriptions/Accounting")
        .await?;
    node.clear_reads();
    receive(
        &node,
        &mut connection,
        "Orders/Subscriptions/Accounting/$DeadLetterQueue",
    )
    .await?;
    peek(
        &node,
        &mut connection,
        "Orders/Subscriptions/Accounting/$management",
    )
    .await?;
    receive(&node, &mut connection, "Orders/subscriptions/Accounting").await?;
    assert_eq!(
        node.reads(),
        vec![
            EntityPath::new("Orders/subscriptions/Accounting/$deadletterqueue")?,
            EntityPath::new("Orders/subscriptions/Accounting")?,
            EntityPath::new("Orders/subscriptions/Accounting")?,
            EntityPath::new("Orders/subscriptions/Accounting")?,
        ]
    );
    connection.close().await?;
    Ok(())
}

async fn typed_leaf_boundaries_and_literal_control_names_do_not_drift() -> TestResult {
    let node = Node::start("", PermissionSet::LISTEN).await?;
    let maximum = "a".repeat(domain::MAX_SUBSCRIPTION_NAME_BYTES);
    for name in ["a", "a.b-c_d", maximum.as_str(), "Subscriptions"] {
        let name = SubscriptionName::new(name)?;
        let mut connection = node.connect().await?;
        node.authorize(&mut connection, &format!("Orders/Subscriptions/{name}"))
            .await?;
        node.clear_reads();
        receive(
            &node,
            &mut connection,
            &format!("Orders/subscriptions/{name}"),
        )
        .await?;
        assert_eq!(
            node.reads(),
            vec![EntityPath::new("Orders")?.subscription(&name)?]
        );
        connection.close().await?;
    }
    let mut connection = node.connect().await?;
    node.authorize(&mut connection, "Subscriptions/Subscriptions/Subscriptions")
        .await?;
    node.clear_reads();
    for path in [
        "subscriptions/Subscriptions/Subscriptions",
        "Subscriptions/Subscriptions/subscriptions",
    ] {
        refuse_receiver(&node, &mut connection, path).await?;
        assert!(node.reads().is_empty());
    }
    receive(
        &node,
        &mut connection,
        "Subscriptions/subscriptions/Subscriptions",
    )
    .await?;
    connection.close().await?;
    let mut connection = node.connect().await?;
    node.authorize(
        &mut connection,
        "Outer/Subscriptions/Subscriptions/Accounting",
    )
    .await?;
    node.clear_reads();
    receive(
        &node,
        &mut connection,
        "Outer/Subscriptions/subscriptions/Accounting",
    )
    .await?;
    assert_eq!(
        node.reads(),
        vec![EntityPath::new(
            "Outer/Subscriptions/subscriptions/Accounting"
        )?]
    );
    connection.close().await?;
    Ok(())
}

async fn forged_or_foreign_cbs_requests_never_query_entity_metadata() -> TestResult {
    let node = Node::start("", PermissionSet::LISTEN).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let requested = audience("Orders/Subscriptions/Accounting");
    let forged = sas_token(&requested).replace("%2FSubscriptions%2F", "%2Fsubscriptions%2F");
    assert_eq!(
        put_token(&node, &mut session, &requested, forged).await?,
        401
    );
    let foreign = "amqps://other.servicebus.windows.net/Orders/Subscriptions/Accounting";
    assert_eq!(
        put_token(&node, &mut session, foreign, sas_token(&requested)).await?,
        401
    );
    assert!(node.reads().is_empty());
    assert_eq!(
        put_token(&node, &mut session, &requested, sas_token(&requested)).await?,
        202
    );
    session.end().await?;
    receive(&node, &mut connection, "Orders/subscriptions/Accounting").await?;
    assert_eq!(node.reads().len(), 1);
    connection.close().await?;
    Ok(())
}

macro_rules! cases {
    ($($case:ident,)+) => {
        mod wire { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE, super::$case()).await? })+ }
    };
}

cases! {
    scoped_subscription_denies_existing_and_missing_siblings_before_metadata,
    topic_scope_inherits_subscriptions_but_not_neighbor_topics,
    namespace_scope_inherits_queues_and_subscriptions,
    exact_dlq_scope_does_not_authorize_its_base_or_sibling,
    exact_management_scope_remains_management_only,
    base_subscription_scope_inherits_dlq_and_management,
    typed_leaf_boundaries_and_literal_control_names_do_not_drift,
    forged_or_foreign_cbs_requests_never_query_entity_metadata,
}
