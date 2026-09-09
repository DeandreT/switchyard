//! Scheduled topic publication over a real AMQP TCP socket.

use std::{error::Error, time::Duration};

use amqp::{
    AnnotationKey, ApplicationProperties, Array, Body, ClientConnection as Connection,
    ClientReceiver as Receiver, ClientSender as Sender, ClientSession as Session, Message,
    MessageAnnotations, OrderedMap, Outcome, Properties, Value,
};
use domain::{
    CommandKind, CommandOutcome, CorrelationFilter, EntityPath, RuleFilter, RuleName, StateMachine,
    SubscriptionConfig, SubscriptionName, TopicConfig,
};
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::net::TcpListener;

const TOPIC: &str = "scheduled-events";
const SELECTED: &str = "scheduled-events/subscriptions/selected";
const BLOCKED: &str = "scheduled-events/subscriptions/blocked";
const REPLY_TO: &str = "scheduled-topic-replies";

struct Node {
    broker: Broker,
    clock: ManualClock,
    address: String,
}

impl Node {
    async fn start() -> Result<Self, Box<dyn Error>> {
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            clock.clone(),
        ));
        let namespace = domain::NamespaceName::new("tenant")?;
        let topic = EntityPath::new(TOPIC)?;
        broker.handle().submit_blocking(
            namespace.clone(),
            topic.clone(),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;

        for (name, subject) in [("selected", "scheduled.match"), ("blocked", "never.match")] {
            let subscription_name = SubscriptionName::new(name)?;
            broker.handle().submit_blocking(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateSubscription {
                    name: subscription_name.clone(),
                    config: SubscriptionConfig::default(),
                },
            )?;
            let subscription = topic.subscription(&subscription_name)?;
            broker.handle().submit_blocking(
                namespace.clone(),
                subscription.clone(),
                CommandKind::DeleteRule {
                    name: RuleName::new(domain::DEFAULT_RULE_NAME)?,
                },
            )?;
            broker.handle().submit_blocking(
                namespace.clone(),
                subscription,
                CommandKind::CreateRule {
                    name: RuleName::new("subject-match")?,
                    filter: RuleFilter::Correlation(CorrelationFilter {
                        subject: Some(subject.to_owned()),
                        ..CorrelationFilter::default()
                    }),
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
            broker,
            clock,
            address,
        })
    }

    async fn connect(&self) -> Result<Connection, Box<dyn Error>> {
        Ok(Connection::builder()
            .container_id("scheduled-topic-test-client")
            .open(format!("amqp://{}", self.address).as_str())
            .await?)
    }

    fn activate_at(&self, millis: u64) -> Result<(u32, Vec<EntityPath>), Box<dyn Error>> {
        self.clock.set(millis);
        match self.broker.handle().submit_blocking(
            domain::NamespaceName::new("tenant")?,
            EntityPath::new(TOPIC)?,
            CommandKind::ActivateScheduled,
        )? {
            CommandOutcome::ScheduledActivated {
                activated,
                deliverable_entities,
            } => Ok((activated, deliverable_entities)),
            other => Err(format!("unexpected activation outcome: {other:?}").into()),
        }
    }
}

fn scheduled_message(message_id: &str, text: &str, enqueue_at: i64) -> Message {
    let mut message = Message::builder()
        .properties(Properties {
            message_id: Some(message_id.to_owned().into()),
            subject: Some(String::from("scheduled.match")),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert("scheduled-source", "raw-topic-e2e")
                .build(),
        )
        .body(Body::Data(vec![text.as_bytes().to_vec().into()]))
        .build();
    let mut annotations = MessageAnnotations::default();
    annotations.insert(
        "x-opt-scheduled-enqueue-time",
        Value::Timestamp(enqueue_at.into()),
    );
    message.message_annotations = Some(annotations);
    message
}

fn schedule_entry(message_id: &str, message: &Message) -> Value {
    let mut entry = OrderedMap::new();
    entry.insert(
        Value::String(String::from("message")),
        Value::Binary(
            amqp::encode_message(message)
                .expect("scheduled topic message encodes")
                .into(),
        ),
    );
    entry.insert(
        Value::String(String::from("message-id")),
        Value::String(message_id.to_owned()),
    );
    Value::Map(entry)
}

fn map(entries: impl IntoIterator<Item = (&'static str, Value)>) -> OrderedMap<Value, Value> {
    entries
        .into_iter()
        .map(|(name, value)| (Value::String(name.to_owned()), value))
        .collect()
}

async fn management_request(
    requests: &mut Sender,
    responses: &mut Receiver,
    message_id: &str,
    operation: &str,
    body: OrderedMap<Value, Value>,
    expected_status: i32,
) -> Result<Message, Box<dyn Error>> {
    let request = Message::builder()
        .properties(Properties {
            message_id: Some(message_id.to_owned().into()),
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
    assert_eq!(
        response
            .message()
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY)),
        Some(&Value::Int(expected_status))
    );
    let message = response.message().clone();
    responses.accept(&response).await?;
    Ok(message)
}

fn sequence_response(response: &Message) -> Result<Vec<i64>, Box<dyn Error>> {
    let Body::Value(Value::Map(body)) = &response.body else {
        return Err("schedule response must be an AMQP value map".into());
    };
    let Some(Value::Array(values)) = body.get(&Value::String(String::from("sequence-numbers")))
    else {
        return Err("schedule response omitted sequence-numbers".into());
    };
    values
        .iter()
        .map(|value| match value {
            Value::Long(sequence) => Ok(*sequence),
            _ => Err("a scheduled sequence number was not an AMQP long".into()),
        })
        .collect()
}

fn peeked_messages(response: &Message) -> Result<Vec<Message>, Box<dyn Error>> {
    let Body::Value(Value::Map(body)) = &response.body else {
        return Err("peek response must be an AMQP value map".into());
    };
    let Some(Value::List(entries)) = body.get(&Value::String(String::from("messages"))) else {
        return Err("peek response omitted messages".into());
    };
    entries
        .iter()
        .map(|entry| {
            let Value::Map(entry) = entry else {
                return Err("peek entry must be an AMQP map".into());
            };
            let Some(Value::Binary(encoded)) = entry.get(&Value::String(String::from("message")))
            else {
                return Err("peek entry omitted its encoded message".into());
            };
            amqp::decode_message(encoded).map_err(Into::into)
        })
        .collect()
}

fn peek_body() -> OrderedMap<Value, Value> {
    map([
        ("from-sequence-number", Value::Long(1)),
        ("message-count", Value::Int(10)),
    ])
}

fn text_of(message: &Message) -> String {
    match &message.body {
        Body::Data(sections) => sections
            .iter()
            .flat_map(|section| section.iter().copied())
            .map(char::from)
            .collect(),
        _ => String::new(),
    }
}

fn sequence_of(message: &Message) -> i64 {
    match message
        .message_annotations
        .as_ref()
        .and_then(|annotations| annotations.get(&AnnotationKey::from("x-opt-sequence-number")))
    {
        Some(Value::Long(sequence)) => *sequence,
        other => panic!("expected a sequence annotation, got {other:?}"),
    }
}

fn assert_scheduled_peek(message: &Message, enqueue_at: i64) {
    let annotations = message
        .message_annotations
        .as_ref()
        .expect("scheduled topic peek annotations");
    assert_eq!(
        annotations.get(&AnnotationKey::from("x-opt-message-state")),
        Some(&Value::Int(2))
    );
    assert!(matches!(
        annotations.get(&AnnotationKey::from("x-opt-scheduled-enqueue-time")),
        Some(Value::Timestamp(value)) if value.milliseconds() == enqueue_at
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn scheduled_topic_management_and_annotated_sends_filter_when_due_and_wake_receivers()
-> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;

    let management = format!("{TOPIC}/$management");
    let mut responses = Receiver::builder()
        .name("scheduled-topic-management-response")
        .source(management.clone())
        .target(REPLY_TO)
        .attach(&mut session)
        .await?;
    let mut requests = Sender::attach(
        &mut session,
        "scheduled-topic-management-request",
        management,
    )
    .await?;

    let cancel = scheduled_message("topic-cancel", "cancel-me", 5_000);
    let selected = scheduled_message("topic-management", "management-due", 5_000);
    let schedule_response = management_request(
        &mut requests,
        &mut responses,
        "schedule-topic",
        protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
        map([(
            "messages",
            Value::List(vec![
                schedule_entry("topic-cancel", &cancel),
                schedule_entry("topic-management", &selected),
            ]),
        )]),
        200,
    )
    .await?;
    let placeholders = sequence_response(&schedule_response)?;
    assert_eq!(placeholders, vec![1, 2]);

    let mut topic_sender = Sender::attach(&mut session, "annotated-topic-sender", TOPIC).await?;
    assert!(matches!(
        topic_sender
            .send(scheduled_message("topic-annotated", "annotated-due", 6_000,))
            .await?,
        Outcome::Accepted(_)
    ));

    let peeked = peeked_messages(
        &management_request(
            &mut requests,
            &mut responses,
            "peek-topic-before-cancel",
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            peek_body(),
            200,
        )
        .await?,
    )?;
    assert_eq!(
        peeked.iter().map(text_of).collect::<Vec<_>>(),
        vec!["cancel-me", "management-due", "annotated-due"]
    );
    assert_eq!(
        peeked.iter().map(sequence_of).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    for (message, enqueue_at) in peeked.iter().zip([5_000, 5_000, 6_000]) {
        assert_scheduled_peek(message, enqueue_at);
    }

    management_request(
        &mut requests,
        &mut responses,
        "cancel-topic",
        protocol_amqp::CANCEL_SCHEDULED_MESSAGE_OPERATION,
        map([(
            "sequence-numbers",
            Value::Array(Array::from(vec![Value::Long(placeholders[0])])),
        )]),
        200,
    )
    .await?;

    let after_cancel = peeked_messages(
        &management_request(
            &mut requests,
            &mut responses,
            "peek-topic-after-cancel",
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            peek_body(),
            200,
        )
        .await?,
    )?;
    assert_eq!(
        after_cancel.iter().map(sequence_of).collect::<Vec<_>>(),
        vec![2, 3]
    );

    let mut selected_receiver =
        Receiver::attach(&mut session, "selected-subscription", SELECTED).await?;
    let mut blocked_receiver =
        Receiver::attach(&mut session, "blocked-subscription", BLOCKED).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(250), selected_receiver.recv())
            .await
            .is_err(),
        "the topic publication was visible before activation"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(250), blocked_receiver.recv())
            .await
            .is_err(),
        "the nonmatching subscription received a scheduled publication early"
    );

    let started = std::time::Instant::now();
    let (activated, deliverable) = node.activate_at(5_000)?;
    assert_eq!(activated, 1);
    assert_eq!(deliverable, vec![EntityPath::new(SELECTED)?]);
    let management_delivery =
        tokio::time::timeout(Duration::from_secs(2), selected_receiver.recv()).await??;
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the subscription waited for the three-second fallback poll"
    );
    assert_eq!(text_of(management_delivery.message()), "management-due");
    assert_ne!(sequence_of(management_delivery.message()), placeholders[1]);
    selected_receiver.accept(&management_delivery).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), blocked_receiver.recv())
            .await
            .is_err(),
        "activation ignored the subscription correlation rule"
    );

    let remaining = peeked_messages(
        &management_request(
            &mut requests,
            &mut responses,
            "peek-topic-between-activations",
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            peek_body(),
            200,
        )
        .await?,
    )?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(text_of(&remaining[0]), "annotated-due");
    assert_scheduled_peek(&remaining[0], 6_000);

    let (activated, deliverable) = node.activate_at(6_000)?;
    assert_eq!(activated, 1);
    assert_eq!(deliverable, vec![EntityPath::new(SELECTED)?]);
    let annotated_delivery =
        tokio::time::timeout(Duration::from_secs(2), selected_receiver.recv()).await??;
    assert_eq!(text_of(annotated_delivery.message()), "annotated-due");
    assert_ne!(sequence_of(annotated_delivery.message()), 3);
    selected_receiver.accept(&annotated_delivery).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), blocked_receiver.recv())
            .await
            .is_err(),
        "the annotated scheduled publication bypassed filtering"
    );

    management_request(
        &mut requests,
        &mut responses,
        "peek-topic-empty",
        protocol_amqp::PEEK_MESSAGE_OPERATION,
        peek_body(),
        204,
    )
    .await?;
    assert_eq!(node.activate_at(6_000)?, (0, Vec::new()));

    connection.close().await?;
    Ok(())
}
