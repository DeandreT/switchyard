//! SQL subscription rules managed and evaluated over a real AMQP socket.

use std::{error::Error, time::Duration};

use amqp::{
    ApplicationProperties, Body, ClientConnection as Connection, ClientReceiver as Receiver,
    ClientSender as Sender, ClientSession as Session, Message, OrderedMap, Outcome, Properties,
    Value,
};
use domain::{CommandKind, StateMachine, SubscriptionConfig, SubscriptionName, TopicConfig};
use serde_amqp::descriptor::Descriptor;
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::net::TcpListener;

#[path = "../src/test_deadline.rs"]
mod test_deadline;

use test_deadline::WAIT;

const TOPIC: &str = "typed-events";
const REPLY_TO: &str = "sql-rule-replies";
const SQL_FILTER_CODE: u64 = 0x0000_0013_7000_0006;
const TRUE_FILTER_CODE: u64 = 0x0000_0013_7000_0007;

/// One subscription per typed property, each selected by a SQL rule naming it.
/// The `errors` rule adds a number to a string, so it fails for the string
/// publication and is unknown for every publication lacking `region`.
const ROUTES: [(&str, &str, &str); 7] = [
    ("ints", "IntRule", "count > 5"),
    ("longs", "LongRule", "big > 4000000000"),
    ("doubles", "DoubleRule", "ratio > 0.5 AND ratio < 1.0"),
    ("flags", "FlagRule", "flag = TRUE"),
    ("strings", "StringRule", "region = 'west'"),
    ("nulls", "NullRule", "EXISTS(maybe) AND maybe IS NULL"),
    ("errors", "Arithmetic", "region + 1 > 0"),
];

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
        for (subscription, _, _) in ROUTES {
            broker.handle().submit_blocking(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(subscription)?,
                    config: SubscriptionConfig::default(),
                },
            )?;
        }

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
            .container_id("sql-rule-client")
            .open(format!("amqp://{}", self.address).as_str())
            .await?)
    }
}

/// A request/response link pair on one subscription's management node.
struct Management {
    requests: Sender,
    responses: Receiver,
    next: u32,
}

impl Management {
    async fn attach(session: &mut Session, subscription: &str) -> Result<Self, Box<dyn Error>> {
        let node = format!("{TOPIC}/subscriptions/{subscription}/$management");
        let responses = Receiver::builder()
            .name(format!("{subscription}-management-response"))
            .source(node.as_str())
            .target(REPLY_TO)
            .attach(session)
            .await?;
        let requests = Sender::attach(
            session,
            format!("{subscription}-management-request"),
            node.as_str(),
        )
        .await?;
        Ok(Self {
            requests,
            responses,
            next: 0,
        })
    }

    async fn request(
        &mut self,
        operation: &str,
        body: OrderedMap<Value, Value>,
    ) -> Result<Message, Box<dyn Error>> {
        self.next += 1;
        let request = Message::builder()
            .properties(Properties {
                message_id: Some(format!("request-{}", self.next).into()),
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
            self.requests.send(request).await?,
            Outcome::Accepted(_)
        ));
        let response = tokio::time::timeout(WAIT, self.responses.recv()).await??;
        let message = response.message().clone();
        self.responses.accept(&response).await?;
        Ok(message)
    }

    async fn remove(&mut self, name: &str) -> Result<Message, Box<dyn Error>> {
        self.request(
            "com.microsoft:remove-rule",
            map([("rule-name", Value::String(name.to_owned()))]),
        )
        .await
    }

    async fn add_sql(&mut self, name: &str, expression: &str) -> Result<Message, Box<dyn Error>> {
        let filter = map([("expression", Value::String(expression.to_owned()))]);
        let description = map([
            ("sql-filter", Value::Map(filter)),
            ("sql-rule-action", Value::Null),
            ("rule-name", Value::String(name.to_owned())),
        ]);
        self.request(
            "com.microsoft:add-rule",
            map([
                ("rule-name", Value::String(name.to_owned())),
                ("rule-description", Value::Map(description)),
            ]),
        )
        .await
    }

    /// Returns each listed rule's name and described filter.
    async fn list(&mut self) -> Result<Vec<(String, Descriptor, Value)>, Box<dyn Error>> {
        let listed = self
            .request(
                "com.microsoft:enumerate-rules",
                map([("skip", Value::Int(0)), ("top", Value::Int(100))]),
            )
            .await?;
        if status(&listed) == Some(204) {
            return Ok(Vec::new());
        }
        assert_eq!(status(&listed), Some(200));
        let Body::Value(Value::Map(body)) = &listed.body else {
            return Err("rule listing did not return a map".into());
        };
        let Some(Value::List(rules)) = body.get(&Value::String(String::from("rules"))) else {
            return Err("rule listing omitted rules".into());
        };
        let mut listed = Vec::new();
        for rule in rules {
            let Value::Map(rule) = rule else {
                return Err("rule entry is not a map".into());
            };
            let Some(Value::Described(description)) =
                rule.get(&Value::String(String::from("rule-description")))
            else {
                return Err("rule entry omitted its described description".into());
            };
            let Value::List(fields) = &description.value else {
                return Err("rule description is not a list".into());
            };
            let (Value::Described(filter), Value::String(name)) = (&fields[0], &fields[2]) else {
                return Err("rule description has unexpected fields".into());
            };
            listed.push((
                name.clone(),
                filter.descriptor.clone(),
                filter.value.clone(),
            ));
        }
        Ok(listed)
    }
}

fn map(entries: impl IntoIterator<Item = (&'static str, Value)>) -> OrderedMap<Value, Value> {
    entries
        .into_iter()
        .map(|(name, value)| (Value::String(name.to_owned()), value))
        .collect()
}

fn status(message: &Message) -> Option<i32> {
    match message
        .application_properties
        .as_ref()
        .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY))
    {
        Some(Value::Int(code)) => Some(*code),
        _ => None,
    }
}

fn status_description(message: &Message) -> String {
    match message
        .application_properties
        .as_ref()
        .and_then(|properties| properties.get(protocol_amqp::STATUS_DESCRIPTION_PROPERTY))
    {
        Some(Value::String(text)) => text.clone(),
        _ => String::new(),
    }
}

fn publication(body: &str, name: &str, value: Value) -> Message {
    Message::builder()
        .properties(Properties {
            message_id: Some(body.into()),
            ..Properties::default()
        })
        .application_properties(ApplicationProperties::builder().insert(name, value).build())
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

async fn expect_only(receiver: &mut Receiver, expected: &str) -> Result<Message, Box<dyn Error>> {
    let delivery = tokio::time::timeout(WAIT, receiver.recv()).await??;
    let message = delivery.message().clone();
    assert_eq!(body_text(&message), expected);
    receiver.accept(&delivery).await?;
    expect_nothing(receiver, expected).await;
    Ok(message)
}

async fn expect_nothing(receiver: &mut Receiver, context: &str) {
    assert!(
        tokio::time::timeout(Duration::from_millis(250), receiver.recv())
            .await
            .is_err(),
        "an unexpected copy arrived after {context}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_rules_route_typed_properties_and_dead_letter_filter_errors()
-> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;

    for (subscription, rule, expression) in ROUTES {
        let mut management = Management::attach(&mut session, subscription).await?;
        assert_eq!(status(&management.remove("$Default").await?), Some(200));
        let added = management.add_sql(rule, expression).await?;
        assert_eq!(status(&added), Some(200), "{}", status_description(&added));
        assert_eq!(
            management.list().await?,
            vec![(
                rule.to_owned(),
                Descriptor::Code(SQL_FILTER_CODE),
                Value::List(vec![Value::String(expression.to_owned()), Value::Int(20)]),
            )],
            "{subscription} did not enumerate its exact SQL text"
        );
    }

    let mut sender = Sender::attach(&mut session, "typed-topic-sender", TOPIC).await?;
    let publications = [
        ("int-seven", "count", Value::Int(7)),
        ("long-six-billion", "big", Value::Long(6_000_000_000)),
        ("double-three-quarters", "ratio", Value::Double(0.75.into())),
        ("bool-true", "flag", Value::Bool(true)),
        ("string-west", "region", Value::String(String::from("west"))),
        ("explicit-null", "maybe", Value::Null),
        // Fails every rule's predicate: none of these subscriptions is offered it.
        ("int-low", "count", Value::Int(3)),
        ("bool-false", "flag", Value::Bool(false)),
    ];
    for (body, name, value) in publications {
        assert!(matches!(
            sender.send(publication(body, name, value)).await?,
            Outcome::Accepted(_)
        ));
    }

    let expected = [
        ("ints", "int-seven"),
        ("longs", "long-six-billion"),
        ("doubles", "double-three-quarters"),
        ("flags", "bool-true"),
        ("strings", "string-west"),
        ("nulls", "explicit-null"),
    ];
    for (subscription, body) in expected {
        let mut receiver = Receiver::attach(
            &mut session,
            format!("{subscription}-receiver"),
            format!("{TOPIC}/subscriptions/{subscription}").as_str(),
        )
        .await?;
        expect_only(&mut receiver, body).await?;
    }

    // The failing rule offers nothing to the subscription itself and one
    // dead-lettered copy of the string publication only.
    let mut errors = Receiver::attach(
        &mut session,
        "errors-receiver",
        format!("{TOPIC}/subscriptions/errors").as_str(),
    )
    .await?;
    expect_nothing(&mut errors, "a filter error").await;
    let mut dead_letters = Receiver::attach(
        &mut session,
        "errors-dlq-receiver",
        format!("{TOPIC}/subscriptions/errors/$deadletterqueue").as_str(),
    )
    .await?;
    let dead_lettered = expect_only(&mut dead_letters, "string-west").await?;
    let properties = dead_lettered
        .application_properties
        .as_ref()
        .expect("the dead-lettered copy carries application properties");
    assert_eq!(
        properties.0.get(protocol_amqp::DEAD_LETTER_REASON_PROPERTY),
        Some(&Value::String(String::from("FilterException")))
    );
    assert_eq!(
        properties
            .0
            .get(protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY),
        Some(&Value::String(String::from(
            "rule Arithmetic: SQL operands have incompatible types"
        )))
    );
    assert_eq!(
        properties.0.get("region"),
        Some(&Value::String(String::from("west")))
    );

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_rule_management_refuses_bad_sql_and_removes_rules() -> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut management = Management::attach(&mut session, "strings").await?;

    let refused = management.add_sql("Broken", "region =").await?;
    assert_eq!(status(&refused), Some(400));
    assert_eq!(
        status_description(&refused),
        "SQL filter cannot be compiled: invalid SQL predicate syntax"
    );

    // The SDK true filter stays a true filter beside a stored SQL rule.
    assert_eq!(
        status(&management.add_sql("Everything", "1=1").await?),
        Some(200)
    );
    assert_eq!(
        status(&management.add_sql("West", "region = 'west'").await?),
        Some(200)
    );
    assert_eq!(
        management
            .list()
            .await?
            .into_iter()
            .map(|(name, descriptor, _)| (name, descriptor))
            .collect::<Vec<_>>(),
        vec![
            (String::from("$Default"), Descriptor::Code(TRUE_FILTER_CODE)),
            (
                String::from("Everything"),
                Descriptor::Code(TRUE_FILTER_CODE)
            ),
            (String::from("West"), Descriptor::Code(SQL_FILTER_CODE)),
        ]
    );

    for name in ["$Default", "everything", "WEST"] {
        assert_eq!(status(&management.remove(name).await?), Some(200), "{name}");
    }
    assert!(management.list().await?.is_empty());

    let mut sender = Sender::attach(&mut session, "removed-rule-sender", TOPIC).await?;
    sender
        .send(publication(
            "after-remove",
            "region",
            Value::String(String::from("west")),
        ))
        .await?;
    let mut receiver = Receiver::attach(
        &mut session,
        "removed-rule-receiver",
        format!("{TOPIC}/subscriptions/strings").as_str(),
    )
    .await?;
    expect_nothing(&mut receiver, "removing every rule").await;

    connection.close().await?;
    Ok(())
}
