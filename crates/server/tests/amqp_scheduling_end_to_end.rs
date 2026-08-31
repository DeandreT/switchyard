//! Scheduled-message interoperability over a real AMQP socket.

use std::{error::Error, time::Duration};

use amqp::{
    AnnotationKey, ApplicationProperties, Array, Body, ClientConnection as Connection,
    ClientReceiver as Receiver, ClientSender as Sender, ClientSession as Session, FilterSet,
    Message, MessageAnnotations, OrderedMap, Outcome, Properties, Source, Symbol, Value,
};
use domain::{CommandKind, CommandOutcome, QueueConfig, StateMachine};
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::net::TcpListener;

struct Node {
    broker: Broker,
    clock: ManualClock,
    address: String,
}

impl Node {
    async fn start() -> Result<Self, Box<dyn Error>> {
        Self::start_queue("orders", QueueConfig::default()).await
    }

    async fn start_session_queue() -> Result<Self, Box<dyn Error>> {
        Self::start_queue(
            "session-orders",
            QueueConfig {
                requires_session: true,
                ..QueueConfig::default()
            },
        )
        .await
    }

    async fn start_queue(queue: &str, config: QueueConfig) -> Result<Self, Box<dyn Error>> {
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            clock.clone(),
        ));
        let namespace = domain::NamespaceName::new("tenant")?;
        broker.handle().submit_blocking(
            namespace.clone(),
            domain::EntityPath::new(queue)?,
            CommandKind::CreateQueue { config },
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
            broker,
            clock,
            address,
        })
    }

    async fn connect(&self) -> Result<Connection, Box<dyn Error>> {
        Ok(Connection::builder()
            .container_id("scheduling-test-client")
            .open(format!("amqp://{}", self.address).as_str())
            .await?)
    }

    fn activate_at(&self, millis: u64) -> Result<u32, Box<dyn Error>> {
        self.activate_queue_at("orders", millis)
    }

    fn activate_queue_at(&self, queue: &str, millis: u64) -> Result<u32, Box<dyn Error>> {
        self.clock.set(millis);
        match self.broker.handle().submit_blocking(
            domain::NamespaceName::new("tenant")?,
            domain::EntityPath::new(queue)?,
            CommandKind::ActivateScheduled,
        )? {
            CommandOutcome::ScheduledActivated { activated } => Ok(activated),
            other => Err(format!("unexpected activation outcome: {other:?}").into()),
        }
    }
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

fn scheduled_message(message_id: &str, text: &str, enqueue_at: i64) -> Message {
    let mut message = Message::builder()
        .properties(Properties {
            message_id: Some(message_id.to_owned().into()),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert("scheduled-source", "raw-e2e")
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

fn scheduled_session_message(
    message_id: &str,
    text: &str,
    enqueue_at: i64,
    session_id: &str,
) -> Message {
    let mut message = scheduled_message(message_id, text, enqueue_at);
    message
        .properties
        .as_mut()
        .expect("scheduled messages have properties")
        .group_id = Some(session_id.to_owned());
    message
}

fn schedule_entry(message_id: &str, message: &Message) -> Value {
    let mut entry = OrderedMap::new();
    entry.insert(
        Value::String(String::from("message")),
        Value::Binary(
            amqp::encode_message(message)
                .expect("scheduled message encodes")
                .into(),
        ),
    );
    entry.insert(
        Value::String(String::from("message-id")),
        Value::String(message_id.to_owned()),
    );
    if let Some(session_id) = message
        .properties
        .as_ref()
        .and_then(|properties| properties.group_id.as_ref())
    {
        entry.insert(
            Value::String(String::from("session-id")),
            Value::String(session_id.clone()),
        );
    }
    Value::Map(entry)
}

fn session_source(queue: &str, session_id: &str) -> Source {
    let mut filter = FilterSet::default();
    filter.insert(
        Symbol::from(protocol_amqp::SESSION_FILTER),
        Value::String(session_id.to_owned()),
    );
    Source::builder().address(queue).filter(filter).build()
}

async fn management_request(
    requests: &mut Sender,
    responses: &mut Receiver,
    reply_to: &str,
    message_id: &str,
    operation: &str,
    body: OrderedMap<Value, Value>,
    expected_status: i32,
) -> Result<Message, Box<dyn Error>> {
    let request = Message::builder()
        .properties(Properties {
            message_id: Some(message_id.to_owned().into()),
            reply_to: Some(reply_to.to_owned()),
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
        .expect("scheduled peek annotations");
    assert_eq!(
        annotations.get(&AnnotationKey::from("x-opt-message-state")),
        Some(&Value::Int(2))
    );
    assert!(matches!(
        annotations.get(&AnnotationKey::from("x-opt-scheduled-enqueue-time")),
        Some(Value::Timestamp(value)) if value.milliseconds() == enqueue_at
    ));
    assert!(
        annotations
            .get(&AnnotationKey::from("x-opt-locked-until"))
            .is_none()
    );
    assert!(
        message
            .delivery_annotations
            .as_ref()
            .and_then(|annotations| annotations.get(&AnnotationKey::from("x-opt-lock-token")))
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schedule_cancel_peek_and_direct_annotated_send_share_one_lifecycle()
-> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;

    let reply_to = "scheduling-management-replies";
    let mut responses = Receiver::builder()
        .name("scheduling-management-response")
        .source("orders/$management")
        .target(reply_to)
        .attach(&mut session)
        .await?;
    let mut requests = Sender::attach(
        &mut session,
        "scheduling-management-request",
        "orders/$management",
    )
    .await?;

    let first = scheduled_message("scheduled-one", "cancel-me", 5_000);
    let second = scheduled_message("scheduled-two", "management-later", 5_000);
    let mut schedule_body = OrderedMap::new();
    schedule_body.insert(
        Value::String(String::from("messages")),
        Value::List(vec![
            schedule_entry("scheduled-one", &first),
            schedule_entry("scheduled-two", &second),
        ]),
    );
    let schedule_response = management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "schedule-1",
        protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
        schedule_body,
        200,
    )
    .await?;
    let placeholders = sequence_response(&schedule_response)?;
    assert_eq!(placeholders, vec![1, 2]);

    let mut peek_body = OrderedMap::new();
    peek_body.insert(
        Value::String(String::from("from-sequence-number")),
        Value::Long(1),
    );
    peek_body.insert(Value::String(String::from("message-count")), Value::Int(10));
    let peek_response = management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "peek-scheduled-1",
        protocol_amqp::PEEK_MESSAGE_OPERATION,
        peek_body,
        200,
    )
    .await?;
    let peeked = peeked_messages(&peek_response)?;
    assert_eq!(
        peeked.iter().map(text_of).collect::<Vec<_>>(),
        vec!["cancel-me", "management-later"]
    );
    for message in &peeked {
        assert_scheduled_peek(message, 5_000);
    }

    let mut malformed_body = OrderedMap::new();
    malformed_body.insert(
        Value::String(String::from("messages")),
        Value::List(vec![
            schedule_entry(
                "valid-but-atomic",
                &scheduled_message("valid-but-atomic", "valid", 5_000),
            ),
            {
                let mut malformed = OrderedMap::new();
                malformed.insert(
                    Value::String(String::from("message")),
                    Value::Binary(b"not an AMQP message".to_vec().into()),
                );
                malformed.insert(
                    Value::String(String::from("message-id")),
                    Value::String(String::from("malformed")),
                );
                Value::Map(malformed)
            },
        ]),
    );
    management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "schedule-malformed",
        protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
        malformed_body,
        400,
    )
    .await?;

    let mut cancel_body = OrderedMap::new();
    cancel_body.insert(
        Value::String(String::from("sequence-numbers")),
        Value::Array(Array::from(vec![Value::Long(placeholders[0])])),
    );
    management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "cancel-1",
        protocol_amqp::CANCEL_SCHEDULED_MESSAGE_OPERATION,
        cancel_body,
        200,
    )
    .await?;

    let mut ordinary_sender = Sender::attach(&mut session, "ordinary-scheduler", "orders").await?;
    assert!(matches!(
        ordinary_sender
            .send(scheduled_message("direct", "direct-later", 6_000))
            .await?,
        Outcome::Accepted(_)
    ));

    let mut early = Receiver::attach(&mut session, "early-receiver", "orders").await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(250), early.recv())
            .await
            .is_err(),
        "a scheduled message was deliverable before activation"
    );
    early.close().await?;

    // The malformed batch consumed no sequence: the direct scheduled send is
    // the third placeholder. Cancellation removed only the first placeholder.
    let mut final_peek_body = OrderedMap::new();
    final_peek_body.insert(
        Value::String(String::from("from-sequence-number")),
        Value::Long(1),
    );
    final_peek_body.insert(Value::String(String::from("message-count")), Value::Int(10));
    let final_peek = peeked_messages(
        &management_request(
            &mut requests,
            &mut responses,
            reply_to,
            "peek-scheduled-2",
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            final_peek_body,
            200,
        )
        .await?,
    )?;
    assert_eq!(
        final_peek.iter().map(sequence_of).collect::<Vec<_>>(),
        vec![placeholders[1], 3]
    );
    assert_eq!(
        final_peek.iter().map(text_of).collect::<Vec<_>>(),
        vec!["management-later", "direct-later"]
    );

    assert_eq!(node.activate_at(5_000)?, 1);
    let mut receiver = Receiver::attach(&mut session, "active-receiver", "orders").await?;
    let activated = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    assert_eq!(text_of(activated.message()), "management-later");
    assert_ne!(sequence_of(activated.message()), placeholders[1]);
    assert!(sequence_of(activated.message()) > 3);
    assert!(matches!(
        activated
            .message()
            .message_annotations
            .as_ref()
            .and_then(|annotations| annotations.get(&AnnotationKey::from("x-opt-scheduled-enqueue-time"))),
        Some(Value::Timestamp(value)) if value.milliseconds() == 5_000
    ));
    receiver.accept(&activated).await?;

    assert_eq!(node.activate_at(6_000)?, 1);
    let direct = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    assert_eq!(text_of(direct.message()), "direct-later");
    assert_ne!(sequence_of(direct.message()), 3);
    assert!(
        direct
            .message()
            .message_annotations
            .as_ref()
            .and_then(|annotations| annotations.get(&AnnotationKey::from("x-opt-message-state")))
            .is_none(),
        "an activated message retained Scheduled state"
    );
    receiver.accept(&direct).await?;

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_a_scheduled_and_active_sequence_is_atomic() -> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;

    let reply_to = "atomic-cancel-replies";
    let mut responses = Receiver::builder()
        .name("atomic-cancel-response")
        .source("orders/$management")
        .target(reply_to)
        .attach(&mut session)
        .await?;
    let mut requests =
        Sender::attach(&mut session, "atomic-cancel-request", "orders/$management").await?;

    let scheduled = scheduled_message("atomic-scheduled", "must-remain", 5_000);
    let mut schedule_body = OrderedMap::new();
    schedule_body.insert(
        Value::String(String::from("messages")),
        Value::List(vec![schedule_entry("atomic-scheduled", &scheduled)]),
    );
    let placeholders = sequence_response(
        &management_request(
            &mut requests,
            &mut responses,
            reply_to,
            "atomic-schedule",
            protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
            schedule_body,
            200,
        )
        .await?,
    )?;
    let [placeholder] = placeholders.as_slice() else {
        return Err("one scheduled message must return one placeholder".into());
    };

    let mut ordinary_sender = Sender::attach(&mut session, "active-sender", "orders").await?;
    assert!(matches!(
        ordinary_sender
            .send(
                Message::builder()
                    .properties(Properties {
                        message_id: Some("already-active".into()),
                        ..Properties::default()
                    })
                    .body(Body::Data(vec![b"cannot-cancel".to_vec().into()]))
                    .build()
            )
            .await?,
        Outcome::Accepted(_)
    ));

    let mut peek_body = OrderedMap::new();
    peek_body.insert(
        Value::String(String::from("from-sequence-number")),
        Value::Long(1),
    );
    peek_body.insert(Value::String(String::from("message-count")), Value::Int(10));
    let before = peeked_messages(
        &management_request(
            &mut requests,
            &mut responses,
            reply_to,
            "atomic-peek-before",
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            peek_body.clone(),
            200,
        )
        .await?,
    )?;
    let active_sequence = before
        .iter()
        .find(|message| text_of(message) == "cannot-cancel")
        .map(sequence_of)
        .ok_or("peek did not return the active message")?;

    let mut cancel_body = OrderedMap::new();
    cancel_body.insert(
        Value::String(String::from("sequence-numbers")),
        Value::Array(Array::from(vec![
            Value::Long(*placeholder),
            Value::Long(active_sequence),
        ])),
    );
    management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "atomic-cancel",
        protocol_amqp::CANCEL_SCHEDULED_MESSAGE_OPERATION,
        cancel_body,
        404,
    )
    .await?;

    let after = peeked_messages(
        &management_request(
            &mut requests,
            &mut responses,
            reply_to,
            "atomic-peek-after",
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            peek_body,
            200,
        )
        .await?,
    )?;
    let remaining = after
        .iter()
        .find(|message| sequence_of(message) == *placeholder)
        .ok_or("the failed cancellation removed its valid scheduled message")?;
    assert_eq!(text_of(remaining), "must-remain");
    assert_scheduled_peek(remaining, 5_000);
    assert!(
        after
            .iter()
            .any(|message| sequence_of(message) == active_sequence),
        "the failed atomic cancellation also changed the active message"
    );

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn scheduled_session_message_activates_only_for_its_session() -> Result<(), Box<dyn Error>> {
    let node = Node::start_session_queue().await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;

    let reply_to = "scheduled-session-replies";
    let mut responses = Receiver::builder()
        .name("scheduled-session-response")
        .source("session-orders/$management")
        .target(reply_to)
        .attach(&mut session)
        .await?;
    let mut requests = Sender::attach(
        &mut session,
        "scheduled-session-request",
        "session-orders/$management",
    )
    .await?;

    let message = scheduled_session_message(
        "scheduled-session-message",
        "session-payload",
        5_000,
        "cart-1",
    );
    let mut schedule_body = OrderedMap::new();
    schedule_body.insert(
        Value::String(String::from("messages")),
        Value::List(vec![schedule_entry("scheduled-session-message", &message)]),
    );
    let placeholders = sequence_response(
        &management_request(
            &mut requests,
            &mut responses,
            reply_to,
            "schedule-session",
            protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
            schedule_body,
            200,
        )
        .await?,
    )?;
    let [placeholder] = placeholders.as_slice() else {
        return Err("one scheduled session message must return one placeholder".into());
    };

    let mut correct = Receiver::builder()
        .name("correct-session")
        .source(session_source("session-orders", "cart-1"))
        .attach(&mut session)
        .await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(250), correct.recv())
            .await
            .is_err(),
        "the session message was available before its scheduled activation"
    );

    let mut wrong = Receiver::builder()
        .name("wrong-session")
        .source(session_source("session-orders", "cart-2"))
        .attach(&mut session)
        .await?;
    assert_eq!(node.activate_queue_at("session-orders", 5_000)?, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(250), wrong.recv())
            .await
            .is_err(),
        "the activated message leaked to a different session"
    );

    let activated = tokio::time::timeout(Duration::from_secs(2), correct.recv()).await??;
    assert_eq!(text_of(activated.message()), "session-payload");
    assert_ne!(sequence_of(activated.message()), *placeholder);
    assert_eq!(
        activated
            .message()
            .properties
            .as_ref()
            .and_then(|properties| properties.group_id.as_deref()),
        Some("cart-1")
    );
    correct.accept(&activated).await?;

    connection.close().await?;
    Ok(())
}
