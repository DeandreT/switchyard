//! Absolute SDK addresses retain authority validation before entity authorization.

use std::{error::Error, time::Duration};

use amqp::{
    ApplicationProperties, Body, ClientConnection as Connection, ClientReceiver as Receiver,
    ClientSender as Sender, ClientSession as Session, Message, OrderedMap, Outcome, Properties,
    SaslInit, Symbol, Value,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{
    CommandKind, EntityPath, NamespaceName, QueueConfig, StateMachine, SubscriptionConfig,
    SubscriptionName, TopicConfig,
};
use serde_amqp::{described::Described, descriptor::Descriptor};
use server::{Broker, LocalProposer, ManualClock};
use storage::{FjallStore, MemoryStore, StateStore};
use tokio::{net::TcpListener, task::JoinHandle};

const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "public-absolute-address-fixture";
type TestResult = Result<(), Box<dyn Error>>;

struct Node {
    broker: Broker,
    address: String,
    listener: JoinHandle<std::io::Result<()>>,
}
impl Node {
    async fn start<S: StateStore>(store: S, scope: ResourceScope) -> Result<Self, Box<dyn Error>> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store),
            ManualClock::at(1000),
        ));
        let namespace = NamespaceName::new("tenant")?;
        for (name, requires_session) in [("orders", false), ("other", false), ("sessions", true)] {
            broker.handle().submit_blocking(
                namespace.clone(),
                EntityPath::new(name)?,
                CommandKind::CreateQueue {
                    config: QueueConfig {
                        requires_session,
                        ..Default::default()
                    },
                },
            )?;
        }
        broker.handle().submit_blocking(
            namespace.clone(),
            EntityPath::new("events")?,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        broker.handle().submit_blocking(
            namespace.clone(),
            EntityPath::new("events")?,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("audit")?,
                config: SubscriptionConfig::default(),
            },
        )?;
        let authentication = protocol_amqp::SharedAccessAuthentication::new(
            SharedAccessPolicy::new([SharedAccessRule::new(
                "fixture",
                scope,
                SharedAccessKey::new(KEY)?,
                None,
                PermissionSet::MANAGE,
            )?])?,
            HOST,
        )?;
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = format!("amqp://{}", socket.local_addr()?);
        let listener = tokio::spawn(
            protocol_amqp::AmqpListener::new(broker.handle(), namespace)
                .with_shared_access_authentication(authentication)
                .serve(socket),
        );
        Ok(Self {
            broker,
            address,
            listener,
        })
    }
    async fn connect(&self) -> Result<Connection, Box<dyn Error>> {
        Ok(Connection::builder()
            .sasl(SaslInit {
                mechanism: Symbol::from("PLAIN"),
                initial_response: Some([b"\0fixture\0".as_slice(), KEY.as_bytes()].concat().into()),
                // This peer-controlled value must not choose the trusted authority.
                hostname: Some("foreign.example".into()),
            })
            .open(&self.address)
            .await?)
    }
    async fn close(self) {
        self.listener.abort();
        let _ = self.listener.await;
        drop(self.broker);
    }
}

fn address(path: &str) -> String {
    format!("amqps://{HOST}/{path}")
}
fn message() -> Message {
    Message::builder()
        .body(Body::Data(vec![b"absolute-address".to_vec().into()]))
        .build()
}

async fn peek(session: &mut Session, path: &str, index: usize) -> TestResult {
    let management = address(&format!("{path}/$management"));
    let reply_to = format!("reply-{index}");
    let mut replies = Receiver::builder()
        .name(format!("management-response-{index}"))
        .source(management.clone())
        .target(reply_to.clone())
        .attach(session)
        .await?;
    let mut requests =
        Sender::attach(session, format!("management-request-{index}"), management).await?;
    let mut body = OrderedMap::new();
    body.insert(Value::String("from-sequence-number".into()), Value::Long(0));
    body.insert(Value::String("message-count".into()), Value::Int(1));
    let request = Message::builder()
        .properties(Properties {
            message_id: Some(format!("peek-{index}").into()),
            reply_to: Some(reply_to),
            ..Default::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert("operation", "com.microsoft:peek-message")
                .build(),
        )
        .body(Body::Value(Value::Map(body)))
        .build();
    assert!(matches!(
        requests.send(request).await?,
        Outcome::Accepted(_)
    ));
    let response = tokio::time::timeout(Duration::from_secs(5), replies.recv()).await??;
    let status = response
        .message()
        .application_properties
        .as_ref()
        .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY));
    assert!(
        matches!(status, Some(Value::Int(200 | 204))),
        "unexpected management status: {status:?}"
    );
    replies.accept(&response).await?;
    replies.close().await?;
    requests.close().await?;
    Ok(())
}

async fn round_trip(node: Node) -> TestResult {
    let result = async {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        for (index, (send_path, receive_path)) in [
            ("Orders", "Orders"),
            ("Events", "Events/Subscriptions/Audit"),
        ]
        .into_iter()
        .enumerate()
        {
            let mut sender =
                Sender::attach(&mut session, format!("send-{index}"), address(send_path)).await?;
            let mut receiver = Receiver::attach(
                &mut session,
                format!("receive-{index}"),
                address(receive_path),
            )
            .await?;
            assert!(matches!(
                sender.send(message()).await?,
                Outcome::Accepted(_)
            ));
            let delivery = tokio::time::timeout(Duration::from_secs(5), receiver.recv()).await??;
            receiver.reject(&delivery, None).await?;
            receiver.close().await?;
            let dead_path = format!("{receive_path}/$DeadLetterQueue");
            let mut dead =
                Receiver::attach(&mut session, format!("dead-{index}"), address(&dead_path))
                    .await?;
            let delivery = tokio::time::timeout(Duration::from_secs(5), dead.recv()).await??;
            dead.accept(&delivery).await?;
            dead.close().await?;
            sender.close().await?;
            peek(&mut session, receive_path, index * 2).await?;
            peek(&mut session, &dead_path, index * 2 + 1).await?;
        }
        connection.close().await?;
        Ok(())
    }
    .await;
    node.close().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absolute_data_and_management_addresses_use_memory_entities() -> TestResult {
    round_trip(Node::start(MemoryStore::default(), ResourceScope::namespace(HOST)?).await?).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absolute_data_and_management_addresses_use_durable_entities() -> TestResult {
    let directory = tempfile::tempdir()?;
    round_trip(
        Node::start(
            FjallStore::open(directory.path())?,
            ResourceScope::namespace(HOST)?,
        )
        .await?,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absolute_addresses_cannot_expand_entity_or_namespace_authority() -> TestResult {
    let node = Node::start(
        MemoryStore::default(),
        ResourceScope::entity(HOST, "orders")?,
    )
    .await?;
    let result = async {
        for (index, target) in [
            "amqps://foreign.example/orders".to_owned(),
            address("other"),
            format!("amqps://user@{HOST}/orders"),
            format!("amqps://{HOST}/orders?query"),
            format!("amqps://{HOST}/orders#fragment"),
            format!("amqps://{HOST}/orders/../other"),
            format!("amqps://{HOST}/orders%2fother"),
        ]
        .into_iter()
        .enumerate()
        {
            let mut connection = node.connect().await?;
            let mut session = Session::begin(&mut connection).await?;
            match Sender::attach(&mut session, format!("denied-{index}"), target).await {
                Ok(mut sender) => assert!(
                    tokio::time::timeout(Duration::from_secs(5), sender.send(message()))
                        .await?
                        .is_err()
                ),
                Err(_) => {}
            }
            let _ = connection.close().await;
        }
        Ok(())
    }
    .await;
    node.close().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn described_session_filters_hold_the_grant_for_management_and_settlement() -> TestResult {
    let node = Node::start(MemoryStore::default(), ResourceScope::namespace(HOST)?).await?;
    let result = async {
        let mut connection = node.connect().await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut sender =
            Sender::attach(&mut session, "session-sender", address("sessions")).await?;
        for (index, descriptor) in [
            Descriptor::Code(0x0000_0013_7000_000c),
            Descriptor::Name(Symbol::from(protocol_amqp::SESSION_FILTER)),
        ]
        .into_iter()
        .enumerate()
        {
            let session_id = format!("session-{index}");
            let body = Message::builder()
                .properties(Properties {
                    group_id: Some(session_id.clone()),
                    ..Default::default()
                })
                .body(Body::Data(vec![b"session-body".to_vec().into()]))
                .build();
            assert!(matches!(sender.send(body).await?, Outcome::Accepted(_)));
            let name = format!("session-receiver-{index}");
            let mut filters = amqp::FilterSet::default();
            filters.insert(
                Symbol::from(protocol_amqp::SESSION_FILTER),
                Value::Described(Box::new(Described {
                    descriptor: descriptor.clone(),
                    value: if index == 0 {
                        Value::Null
                    } else {
                        Value::String(session_id.clone())
                    },
                })),
            );
            let mut receiver = Receiver::builder()
                .name(name.clone())
                .source(amqp::Source {
                    address: Some(address("sessions")),
                    filter: Some(filters),
                    ..Default::default()
                })
                .attach(&mut session)
                .await?;
            let filter = receiver
                .source()
                .as_ref()
                .and_then(|source| source.filter.as_ref())
                .and_then(|filter| filter.get(&Symbol::from(protocol_amqp::SESSION_FILTER)))
                .ok_or("missing session filter")?;
            assert_eq!(
                filter,
                &Value::Described(Box::new(Described {
                    descriptor,
                    value: Value::String(session_id.clone())
                }))
            );
            let reply_to = format!("session-management-{index}");
            let mut replies = Receiver::builder()
                .name(format!("session-replies-{index}"))
                .source(address("sessions/$management"))
                .target(reply_to.clone())
                .attach(&mut session)
                .await?;
            let mut requests = Sender::attach(
                &mut session,
                format!("session-requests-{index}"),
                address("sessions/$management"),
            )
            .await?;
            let mut body = OrderedMap::new();
            body.insert(
                Value::String("session-id".into()),
                Value::String(session_id),
            );
            let request = Message::builder()
                .properties(Properties {
                    message_id: Some(format!("renew-{index}").into()),
                    reply_to: Some(reply_to),
                    ..Default::default()
                })
                .application_properties(
                    ApplicationProperties::builder()
                        .insert("operation", "com.microsoft:renew-session-lock")
                        .insert("associated-link-name", name)
                        .build(),
                )
                .body(Body::Value(Value::Map(body)))
                .build();
            assert!(matches!(
                requests.send(request).await?,
                Outcome::Accepted(_)
            ));
            let reply = tokio::time::timeout(Duration::from_secs(5), replies.recv()).await??;
            assert_eq!(
                reply
                    .message()
                    .application_properties
                    .as_ref()
                    .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY)),
                Some(&Value::Int(200))
            );
            replies.accept(&reply).await?;
            let delivery = tokio::time::timeout(Duration::from_secs(5), receiver.recv()).await??;
            receiver.accept(&delivery).await?;
            receiver.close().await?;
            replies.close().await?;
            requests.close().await?;
        }
        connection.close().await?;
        Ok(())
    }
    .await;
    node.close().await;
    result
}
