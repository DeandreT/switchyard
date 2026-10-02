use domain::{RuleDefinition, RuleFilter, RuleName};
use storage::StateStore;
use testkit::StoreProvider;
use tokio::time::timeout;

use super::*;

#[derive(Default)]
struct Calls {
    metadata: usize,
    rules: Vec<(EntityPath, SubscriptionName)>,
    submits: Vec<(EntityPath, CommandKind)>,
}

#[derive(Clone)]
struct RuleBroker {
    inner: BrokerHandle,
    calls: Arc<Mutex<Calls>>,
}

impl protocol_amqp::Broker for RuleBroker {
    async fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<protocol_amqp::EntityAdmission>, BrokerRejection> {
        self.calls.lock().expect("calls").metadata += 1;
        protocol_amqp::Broker::bind(&self.inner, namespace, target).await
    }

    async fn submit_fenced(
        &self,
        binding: domain::EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.calls
            .lock()
            .expect("calls")
            .submits
            .push((entity.clone(), kind.clone()));
        protocol_amqp::Broker::submit_fenced(&self.inner, binding, entity, kind).await
    }

    async fn rules_fenced(
        &self,
        binding: domain::EntityBinding,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        self.calls
            .lock()
            .expect("calls")
            .rules
            .push((topic.clone(), subscription.clone()));
        protocol_amqp::Broker::rules_fenced(&self.inner, binding, topic, subscription).await
    }

    async fn rules(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        self.calls
            .lock()
            .expect("calls")
            .rules
            .push((topic.clone(), subscription.clone()));
        protocol_amqp::Broker::rules(&self.inner, namespace, topic, subscription).await
    }
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.calls
            .lock()
            .expect("calls")
            .submits
            .push((entity.clone(), kind.clone()));
        protocol_amqp::Broker::submit(&self.inner, namespace, entity, kind).await
    }
    async fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        self.calls.lock().expect("calls").metadata += 1;
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

struct RuleNode<P: StoreProvider> {
    broker: Broker,
    store: P::Store,
    address: String,
    certificate: CertificateDer<'static>,
    calls: Arc<Mutex<Calls>>,
    listener: JoinHandle<()>,
    _provider: P,
}

impl<P: StoreProvider> RuleNode<P> {
    async fn start(provider: P, scope_path: &str, permissions: PermissionSet) -> TestResult<Self> {
        let store = provider.open()?;
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            ManualClock::at(1_000),
        ));
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("Orders")?;
        broker.handle().submit_blocking(
            namespace.clone(),
            topic.clone(),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        for name in ["Alpha", "Billing"] {
            broker.handle().submit_blocking(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(name)?,
                    config: SubscriptionConfig::default(),
                },
            )?;
        }
        let scope = if scope_path.is_empty() {
            ResourceScope::namespace(HOST)?
        } else {
            ResourceScope::entity(HOST, scope_path)?
        };
        let policy = SharedAccessPolicy::new([SharedAccessRule::new(
            RULE,
            scope,
            SharedAccessKey::new(KEY)?,
            None,
            permissions,
        )?])?;
        let authentication = protocol_amqp::SharedAccessAuthentication::new(policy, HOST)?
            .with_authorization_timeout(DEADLINE);
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec!["localhost".into()])?;
        let tls = protocol_amqp::tls_server_config(
            cert.pem().as_bytes(),
            key_pair.serialize_pem().as_bytes(),
        )?;
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?.to_string();
        let calls = Arc::new(Mutex::new(Calls::default()));
        let counted = RuleBroker {
            inner: broker.handle(),
            calls: Arc::clone(&calls),
        };
        let listener = tokio::spawn(async move {
            let _ = protocol_amqp::AmqpListener::new(counted, namespace)
                .with_tls(tls)
                .with_shared_access_authentication(authentication)
                .serve(socket)
                .await;
        });
        Ok(Self {
            broker,
            store,
            address,
            certificate: cert.der().clone(),
            calls,
            listener,
            _provider: provider,
        })
    }

    async fn connect(&self) -> TestResult<Connection> {
        let mut roots = RootCertStore::empty();
        roots.add(self.certificate.clone())?;
        let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_protocol_versions(&[&TLS13, &TLS12])?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tcp = timeout(DEADLINE, TcpStream::connect(&self.address)).await??;
        let tls = timeout(
            DEADLINE,
            TlsConnector::from(Arc::new(config)).connect(ServerName::try_from("localhost")?, tcp),
        )
        .await??;
        Ok(timeout(
            DEADLINE,
            Connection::builder()
                .container_id("rule-auth-client")
                .sasl(SaslInit {
                    mechanism: Symbol::from("ANONYMOUS"),
                    initial_response: None,
                    hostname: Some(HOST.into()),
                })
                .open_with_stream(tls),
        )
        .await??)
    }

    async fn authorize(&self, connection: &mut Connection, path: &str) -> TestResult {
        timeout(DEADLINE, async {
            let mut session = connection.begin().await?;
            let mut requests =
                Sender::attach(&mut session, "rule-cbs-requests", protocol_amqp::CBS_NODE).await?;
            let mut responses = Receiver::builder()
                .name("rule-cbs-responses")
                .source(protocol_amqp::CBS_NODE)
                .target("rule-cbs-replies")
                .attach(&mut session)
                .await?;
            let audience = audience(path);
            let message = Message::builder()
                .properties(Properties {
                    message_id: Some("rule-token".into()),
                    reply_to: Some("rule-cbs-replies".into()),
                    ..Properties::default()
                })
                .application_properties(
                    ApplicationProperties::builder()
                        .insert("operation", String::from("put-token"))
                        .insert("type", String::from("servicebus.windows.net:sastoken"))
                        .insert("name", audience.clone())
                        .build(),
                )
                .body(Body::Value(Value::String(sas_token(&audience))))
                .build();
            assert!(matches!(
                requests.send(message).await?,
                Outcome::Accepted(_)
            ));
            let response = responses.recv().await?;
            assert_eq!(
                response
                    .message()
                    .application_properties
                    .as_ref()
                    .and_then(|properties| properties.get("status-code")),
                Some(&Value::Int(202))
            );
            responses.accept(&response).await?;
            requests.close().await?;
            responses.close().await?;
            session.end().await?;
            Ok::<_, Box<dyn Error>>(())
        })
        .await?
    }

    fn clear(&self) {
        *self.calls.lock().expect("calls") = Calls::default();
    }

    fn no_calls(&self) {
        let calls = self.calls.lock().expect("calls");
        assert_eq!(calls.metadata, 0);
        assert!(calls.rules.is_empty());
        assert!(calls.submits.is_empty());
    }

    fn rules(&self, subscription: &str) -> TestResult<Vec<RuleDefinition>> {
        Ok(self.broker.handle().rules_blocking(
            NamespaceName::new("tenant")?,
            EntityPath::new("Orders")?,
            SubscriptionName::new(subscription)?,
        )?)
    }

    async fn shutdown(&mut self) {
        self.listener.abort();
        let _ = (&mut self.listener).await;
    }
}

impl<P: StoreProvider> Drop for RuleNode<P> {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

fn map(entries: impl IntoIterator<Item = (&'static str, Value)>) -> OrderedMap<Value, Value> {
    entries
        .into_iter()
        .map(|(key, value)| (Value::String(key.into()), value))
        .collect()
}

fn add(name: &str) -> OrderedMap<Value, Value> {
    map([
        ("rule-name", Value::String(name.into())),
        (
            "rule-description",
            Value::Map(map([
                ("rule-name", Value::String(name.into())),
                (
                    "sql-filter",
                    Value::Map(map([(
                        "expression",
                        Value::String("sys.MessageId IS NOT NULL".into()),
                    )])),
                ),
                ("sql-rule-action", Value::Null),
            ])),
        ),
    ])
}

struct RuleClient {
    sender: Sender,
    receiver: Receiver,
    reply_to: String,
}

impl RuleClient {
    async fn attach(session: &mut Session, address: &str) -> TestResult<Self> {
        let reply_to = String::from("auth-rule-replies");
        let receiver = timeout(
            DEADLINE,
            Receiver::builder()
                .name("auth-rule-responses")
                .source(address)
                .target(reply_to.clone())
                .attach(session),
        )
        .await??;
        let sender = timeout(
            DEADLINE,
            Sender::attach(session, "auth-rule-requests", address),
        )
        .await??;
        Ok(Self {
            sender,
            receiver,
            reply_to,
        })
    }

    async fn request(
        &mut self,
        operation: &str,
        body: OrderedMap<Value, Value>,
        associated: Option<&str>,
    ) -> TestResult<Message> {
        timeout(DEADLINE, async {
            let mut properties = ApplicationProperties::builder()
                .insert(protocol_amqp::OPERATION_PROPERTY, operation.to_owned())
                .build();
            if let Some(name) = associated {
                properties.0.insert(
                    protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY.into(),
                    Value::String(name.into()),
                );
            }
            assert!(matches!(
                self.sender
                    .send(
                        Message::builder()
                            .properties(Properties {
                                message_id: Some("auth-rule-operation".into()),
                                reply_to: Some(self.reply_to.clone()),
                                ..Properties::default()
                            })
                            .application_properties(properties)
                            .body(Body::Value(Value::Map(body)))
                            .build()
                    )
                    .await?,
                Outcome::Accepted(_)
            ));
            let delivery = self.receiver.recv().await?;
            let response = delivery.message().clone();
            self.receiver.accept(&delivery).await?;
            Ok::<_, Box<dyn Error>>(response)
        })
        .await?
    }
}

fn status(message: &Message, code: i32) {
    assert_eq!(
        message
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY)),
        Some(&Value::Int(code))
    );
}

async fn exact_subscription_management_listen_can_create_remove_and_enumerate<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = RuleNode::start(
        provider,
        "Orders/subscriptions/Alpha/$management",
        PermissionSet::LISTEN,
    )
    .await?;
    let mut connection = node.connect().await?;
    node.authorize(&mut connection, "Orders/Subscriptions/Alpha/$Management")
        .await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut client =
        RuleClient::attach(&mut session, "Orders/SUBSCRIPTIONS/Alpha/$MANAGEMENT").await?;
    node.clear();
    status(
        &client
            .request(
                protocol_amqp::ADD_RULE_OPERATION,
                add("listen-created"),
                None,
            )
            .await?,
        200,
    );
    let definitions = node.rules("Alpha")?;
    let created = definitions
        .iter()
        .find(|rule| rule.name.as_str() == "listen-created")
        .expect("created rule");
    let domain::RuleFilter::Sql(filter) = &created.filter else {
        panic!("compiled SQL rule");
    };
    assert_eq!(filter.expression(), "sys.MessageId IS NOT NULL");
    status(
        &client
            .request(
                protocol_amqp::ENUMERATE_RULES_OPERATION,
                map([("top", Value::Int(100)), ("skip", Value::Int(0))]),
                None,
            )
            .await?,
        200,
    );
    status(
        &client
            .request(
                protocol_amqp::REMOVE_RULE_OPERATION,
                map([("rule-name", Value::String("listen-created".into()))]),
                None,
            )
            .await?,
        200,
    );
    {
        let calls = node.calls.lock().expect("calls");
        assert_eq!(calls.metadata, 0);
        assert_eq!(
            calls.rules,
            [(EntityPath::new("Orders")?, SubscriptionName::new("Alpha")?)]
        );
        assert_eq!(calls.submits.len(), 2);
        assert!(
            calls
                .submits
                .iter()
                .all(|(entity, _)| entity.as_str() == "Orders")
        );
    }
    assert_eq!(node.rules("Alpha")?[0].name.as_str(), "$Default");
    timeout(DEADLINE, connection.close()).await??;
    node.shutdown().await;
    Ok(())
}

async fn send_only_management_operations_deny_before_reads_and_submits<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = RuleNode::start(
        provider,
        "Orders/subscriptions/Alpha/$management",
        PermissionSet::SEND,
    )
    .await?;
    let mut connection = node.connect().await?;
    node.authorize(&mut connection, "Orders/subscriptions/Alpha/$management")
        .await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut client =
        RuleClient::attach(&mut session, "Orders/subscriptions/Alpha/$management").await?;
    node.clear();
    let before = node.store.snapshot()?;
    for (operation, body) in [
        (protocol_amqp::ADD_RULE_OPERATION, add("forbidden")),
        (
            protocol_amqp::REMOVE_RULE_OPERATION,
            map([("rule-name", Value::String("$Default".into()))]),
        ),
        (
            protocol_amqp::ENUMERATE_RULES_OPERATION,
            map([("top", Value::Int(100)), ("skip", Value::Int(0))]),
        ),
    ] {
        status(
            &client
                .request(operation, body, Some("foreign-sibling-link"))
                .await?,
            401,
        );
        node.no_calls();
        assert_eq!(node.store.snapshot()?, before);
    }
    timeout(DEADLINE, connection.close()).await??;
    node.shutdown().await;
    Ok(())
}

async fn child_scopes_do_not_authorize_parent_or_sibling_metadata<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = RuleNode::start(
        provider,
        "Orders/subscriptions/Alpha",
        PermissionSet::LISTEN,
    )
    .await?;
    let mut connection = node.connect().await?;
    node.authorize(&mut connection, "Orders/Subscriptions/Alpha")
        .await?;
    for address in [
        "Orders/$management",
        "Orders/subscriptions/Billing/$management",
        "Orders/subscriptions/Missing/$management",
    ] {
        let mut session = timeout(DEADLINE, connection.begin()).await??;
        node.clear();
        let before = node.store.snapshot()?;
        if let Ok(mut receiver) = timeout(
            DEADLINE,
            Receiver::builder()
                .name("denied-rules-response")
                .source(address)
                .target("denied-rule-replies")
                .attach(&mut session),
        )
        .await?
        {
            assert!(
                timeout(DEADLINE, receiver.recv()).await?.is_err(),
                "scope unexpectedly admitted {address}"
            );
        }
        node.no_calls();
        assert_eq!(node.store.snapshot()?, before);
        let _ = timeout(DEADLINE, session.end()).await?;
    }
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut client =
        RuleClient::attach(&mut session, "Orders/subscriptions/Alpha/$management").await?;
    status(
        &client
            .request(
                protocol_amqp::ENUMERATE_RULES_OPERATION,
                map([("top", Value::Int(100)), ("skip", Value::Int(0))]),
                None,
            )
            .await?,
        200,
    );
    timeout(DEADLINE, connection.close()).await??;
    node.shutdown().await;
    Ok(())
}

async fn foreign_associated_name_cannot_redirect_the_canonical_subscription<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = RuleNode::start(
        provider,
        "Orders/subscriptions/Alpha/$management",
        PermissionSet::LISTEN,
    )
    .await?;
    node.broker.handle().submit_blocking(
        NamespaceName::new("tenant")?,
        EntityPath::new("Orders")?,
        CommandKind::DeleteRule {
            subscription: SubscriptionName::new("Billing")?,
            name: RuleName::new("$Default")?,
        },
    )?;
    let mut connection = node.connect().await?;
    node.authorize(&mut connection, "Orders/subscriptions/Alpha/$management")
        .await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut client =
        RuleClient::attach(&mut session, "Orders/subscriptions/Alpha/$management").await?;
    node.clear();
    status(
        &client
            .request(
                protocol_amqp::ADD_RULE_OPERATION,
                add("scoped"),
                Some("Orders/subscriptions/Billing"),
            )
            .await?,
        200,
    );
    let response = client
        .request(
            protocol_amqp::ENUMERATE_RULES_OPERATION,
            map([("top", Value::Int(100)), ("skip", Value::Int(0))]),
            Some("foreign-sibling-link"),
        )
        .await?;
    status(&response, 200);
    assert_eq!(
        node.rules("Alpha")?
            .iter()
            .map(|rule| rule.name.as_str())
            .collect::<Vec<_>>(),
        ["$Default", "scoped"]
    );
    assert!(node.rules("Billing")?.is_empty());
    {
        let calls = node.calls.lock().expect("calls");
        assert_eq!(
            calls.rules,
            [(EntityPath::new("Orders")?, SubscriptionName::new("Alpha")?)]
        );
        assert!(
            matches!(&calls.submits[..], [(entity, CommandKind::CreateRule { subscription, filter: RuleFilter::Sql(filter), .. })] if entity.as_str() == "Orders" && subscription.as_str() == "Alpha" && filter.expression() == "sys.MessageId IS NOT NULL")
        );
    }
    timeout(DEADLINE, connection.close()).await??;
    node.shutdown().await;
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 3, super::$case(::testkit::MemoryProvider::new())).await? })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 3, super::$case(::testkit::DurableProvider::temporary()?)).await? })+ }
    };
}

for_each_backend! {
    exact_subscription_management_listen_can_create_remove_and_enumerate,
    send_only_management_operations_deny_before_reads_and_submits,
    child_scopes_do_not_authorize_parent_or_sibling_metadata,
    foreign_associated_name_cannot_redirect_the_canonical_subscription,
}

#[path = "rules/actions.rs"]
mod actions;
