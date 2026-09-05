//! Subscription rule management and filtered fanout over a real AMQP socket.

use std::{error::Error, time::Duration};

use amqp::{
    ApplicationProperties, Body, ClientConnection as Connection, ClientReceiver as Receiver,
    ClientSender as Sender, ClientSession as Session, Message, OrderedMap, Outcome, Properties,
    Value,
};
use domain::{CommandKind, StateMachine, SubscriptionConfig, SubscriptionName, TopicConfig};
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::net::TcpListener;

const TOPIC: &str = "filtered-events";
const SUBSCRIPTION: &str = "filtered-events/subscriptions/priority";
const MANAGEMENT: &str = "filtered-events/subscriptions/priority/$management";
const REPLY_TO: &str = "rule-management-replies";

struct Node {
    _broker: Broker,
    address: String,
}

impl Node {
    async fn start() -> Result<Self, Box<dyn Error>> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            ManualClock::at(1_700_000_000_000),
        ));
        let namespace = domain::NamespaceName::new("tenant")?;
        let topic = domain::EntityPath::new(TOPIC)?;
        broker.handle().submit_blocking(
            namespace.clone(),
            topic.clone(),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        broker.handle().submit_blocking(
            namespace.clone(),
            topic,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("priority")?,
                config: SubscriptionConfig::default(),
            },
        )?;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let handle = broker.handle();
        tokio::spawn(async move {
            let _ = protocol_amqp::AmqpListener::new(handle, namespace)
                .serve(listener)
                .await;
        });
        Ok(Self {
            _broker: broker,
            address,
        })
    }

    async fn connect(&self) -> Result<Connection, Box<dyn Error>> {
        Ok(Connection::builder()
            .container_id("rule-management-client")
            .open(format!("amqp://{}", self.address).as_str())
            .await?)
    }
}

fn map(entries: impl IntoIterator<Item = (&'static str, Value)>) -> OrderedMap<Value, Value> {
    entries
        .into_iter()
        .map(|(name, value)| (Value::String(name.to_owned()), value))
        .collect()
}

async fn request(
    requests: &mut Sender,
    responses: &mut Receiver,
    id: &str,
    operation: &str,
    body: OrderedMap<Value, Value>,
) -> Result<Message, Box<dyn Error>> {
    let request = Message::builder()
        .properties(Properties {
            message_id: Some(id.into()),
            reply_to: Some(REPLY_TO.to_owned()),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert(protocol_amqp::OPERATION_PROPERTY, operation)
                .build(),
        )
        .body(Body::Value(Value::Map(body)))
        .build();
    assert!(matches!(
        requests.send(request).await?,
        Outcome::Accepted(_)
    ));
    let response = tokio::time::timeout(Duration::from_secs(2), responses.recv()).await??;
    let message = response.message().clone();
    responses.accept(&response).await?;
    Ok(message)
}

fn assert_status(message: &Message, expected: i32) {
    assert_eq!(
        message
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY)),
        Some(&Value::Int(expected))
    );
}

fn correlation_rule(name: &str) -> OrderedMap<Value, Value> {
    let filter_properties = map([
        ("Priority", Value::Int(7)),
        ("region", Value::String(String::from("west"))),
    ]);
    let correlation = map([
        ("message-id", Value::String(String::from("rule-message"))),
        ("label", Value::String(String::from("rule.subject"))),
        ("properties", Value::Map(filter_properties)),
    ]);
    let description = map([
        ("correlation-filter", Value::Map(correlation)),
        ("sql-rule-action", Value::Null),
        ("rule-name", Value::String(name.to_owned())),
    ]);
    map([
        ("rule-name", Value::String(name.to_owned())),
        ("rule-description", Value::Map(description)),
    ])
}

fn publication(body: &str, priority: i32) -> Message {
    Message::builder()
        .properties(Properties {
            message_id: Some("rule-message".into()),
            subject: Some(String::from("rule.subject")),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert("priority", priority)
                .insert("region", "west")
                .build(),
        )
        .body(Body::Data(vec![body.as_bytes().to_vec().into()]))
        .build()
}

fn body_text(message: &Message) -> String {
    match &message.body {
        Body::Data(sections) => sections
            .iter()
            .flat_map(|section| section.iter().copied())
            .map(char::from)
            .collect(),
        _ => String::new(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rule_management_controls_atomic_topic_fanout() -> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut responses = Receiver::builder()
        .name("rule-management-response")
        .source(MANAGEMENT)
        .target(REPLY_TO)
        .attach(&mut session)
        .await?;
    let mut requests = Sender::attach(&mut session, "rule-management-request", MANAGEMENT).await?;

    let delete_default = request(
        &mut requests,
        &mut responses,
        "delete-default",
        "com.microsoft:remove-rule",
        map([("rule-name", Value::String(String::from("$Default")))]),
    )
    .await?;
    assert_status(&delete_default, 200);

    let add = request(
        &mut requests,
        &mut responses,
        "add-correlation",
        "com.microsoft:add-rule",
        correlation_rule("Priority"),
    )
    .await?;
    assert_status(&add, 200);

    let listed = request(
        &mut requests,
        &mut responses,
        "list-correlation",
        "com.microsoft:enumerate-rules",
        map([("skip", Value::Int(0)), ("top", Value::Int(100))]),
    )
    .await?;
    assert_status(&listed, 200);
    let Body::Value(Value::Map(list_body)) = &listed.body else {
        return Err("rule listing did not return a map".into());
    };
    let Some(Value::List(rules)) = list_body.get(&Value::String(String::from("rules"))) else {
        return Err("rule listing omitted rules".into());
    };
    assert_eq!(rules.len(), 1);

    let mut sender = Sender::attach(&mut session, "filtered-topic-sender", TOPIC).await?;
    let mut receiver =
        Receiver::attach(&mut session, "filtered-subscription", SUBSCRIPTION).await?;
    assert!(matches!(
        sender.send(publication("not-selected", 8)).await?,
        Outcome::Accepted(_)
    ));
    assert!(matches!(
        sender.send(publication("selected", 7)).await?,
        Outcome::Accepted(_)
    ));
    let selected = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    assert_eq!(body_text(selected.message()), "selected");
    receiver.accept(&selected).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(250), receiver.recv())
            .await
            .is_err(),
        "the rejected publication reached the subscription"
    );

    let delete = request(
        &mut requests,
        &mut responses,
        "delete-correlation",
        "com.microsoft:remove-rule",
        map([("rule-name", Value::String(String::from("pRIORITY")))]),
    )
    .await?;
    assert_status(&delete, 200);
    sender.send(publication("after-delete", 7)).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(250), receiver.recv())
            .await
            .is_err(),
        "an empty rule set selected a publication"
    );

    connection.close().await?;
    Ok(())
}
