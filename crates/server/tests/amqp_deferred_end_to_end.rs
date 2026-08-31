//! Deferred-message interoperability over a real AMQP socket.

use std::{error::Error, time::Duration};

use amqp::{
    AnnotationKey, ApplicationProperties, Array, Body, ClientConnection as Connection,
    ClientReceiver as Receiver, ClientSender as Sender, ClientSession as Session, Fields, Message,
    Modified, OrderedMap, Outcome, Properties, Symbol, Value,
};
use domain::{CommandKind, QueueConfig, StateMachine};
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::net::TcpListener;

struct Node {
    _broker: Broker,
    address: String,
}

impl Node {
    async fn start() -> Result<Self, Box<dyn Error>> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            ManualClock::at(1_000),
        ));
        let namespace = domain::NamespaceName::new("tenant")?;
        broker.handle().submit_blocking(
            namespace.clone(),
            domain::EntityPath::new("orders")?,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
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
            .container_id("deferred-test-client")
            .open(format!("amqp://{}", self.address).as_str())
            .await?)
    }
}

fn body(text: &str) -> Body {
    Body::Data(vec![text.as_bytes().to_vec().into()])
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

async fn management_request(
    requests: &mut Sender,
    responses: &mut Receiver,
    reply_to: &str,
    message_id: &str,
    operation: &str,
    body: OrderedMap<Value, Value>,
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
        Some(&Value::Int(200))
    );
    let message = response.message().clone();
    responses.accept(&response).await?;
    Ok(message)
}

fn deferred_response(response: Message) -> Result<(Message, amqp::Uuid), Box<dyn Error>> {
    let Body::Value(Value::Map(response_body)) = response.body else {
        return Err("the deferred response must carry an AMQP value map".into());
    };
    let Some(Value::List(entries)) = response_body.get(&Value::String(String::from("messages")))
    else {
        return Err("the deferred response did not contain messages".into());
    };
    let [Value::Map(entry)] = entries.as_slice() else {
        return Err("the deferred response did not contain exactly one message".into());
    };
    let Some(Value::Binary(encoded)) = entry.get(&Value::String(String::from("message"))) else {
        return Err("the deferred response did not contain an encoded message".into());
    };
    let message = amqp::decode_message(encoded)?;
    let lock_token = match entry.get(&Value::String(String::from("lock-token"))) {
        Some(Value::Uuid(token)) => token.clone(),
        _ => return Err("the deferred response did not contain a lock token".into()),
    };
    Ok((message, lock_token))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deferred_message_is_retrieved_and_settled_by_sequence() -> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "deferred-sender", "orders").await?;
    sender
        .send(
            Message::builder()
                .application_properties(
                    ApplicationProperties::builder()
                        .insert("stage", "received")
                        .insert("preserved", "original")
                        .build(),
                )
                .body(body("park-me"))
                .build(),
        )
        .await?;

    let mut receiver = Receiver::attach(&mut session, "ordinary-receiver", "orders").await?;
    let first = receiver.recv().await?;
    let sequence = match first
        .message()
        .message_annotations
        .as_ref()
        .and_then(|annotations| annotations.get(&AnnotationKey::from("x-opt-sequence-number")))
    {
        Some(Value::Long(sequence)) => *sequence,
        other => panic!("expected a sequence annotation, got {other:?}"),
    };
    let mut changes = Fields::new();
    changes.insert(Symbol::from("stage"), Value::String(String::from("parked")));
    changes.insert(Symbol::from("attempt"), Value::Int(2));
    receiver
        .modify(
            &first,
            Modified {
                undeliverable_here: Some(true),
                message_annotations: Some(changes),
                ..Modified::default()
            },
        )
        .await?;

    let hidden = tokio::time::timeout(Duration::from_millis(300), receiver.recv()).await;
    assert!(
        hidden.is_err(),
        "an ordinary receive returned a deferred message"
    );

    // These request/reply links have no associated ordinary receive link. The
    // entity-scoped management registry must carry the returned lock through
    // renewal and completion on its own.
    let reply_to = "deferred-management-replies";
    let mut responses = Receiver::builder()
        .name("deferred-management-response")
        .source("orders/$management")
        .target(reply_to)
        .attach(&mut session)
        .await?;
    let mut requests = Sender::attach(
        &mut session,
        "deferred-management-request",
        "orders/$management",
    )
    .await?;

    let mut receive_body = OrderedMap::new();
    receive_body.insert(
        Value::String(String::from("sequence-numbers")),
        Value::Array(Array::from(vec![Value::Long(sequence)])),
    );
    receive_body.insert(
        Value::String(String::from("receiver-settle-mode")),
        Value::Uint(1),
    );
    let response = management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "receive-deferred-1",
        protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
        receive_body.clone(),
    )
    .await?;
    let (deferred, lock_token) = deferred_response(response)?;
    assert_eq!(text_of(&deferred), "park-me");
    assert_eq!(
        deferred.header.as_ref().map(|header| header.delivery_count),
        Some(1)
    );
    assert_eq!(
        deferred
            .message_annotations
            .as_ref()
            .and_then(|annotations| annotations.get(&AnnotationKey::from("x-opt-message-state"))),
        Some(&Value::Int(1))
    );
    let properties = deferred
        .application_properties
        .as_ref()
        .expect("the deferred message keeps application properties");
    assert_eq!(
        properties.0.get("stage"),
        Some(&Value::String(String::from("parked")))
    );
    assert_eq!(properties.0.get("attempt"), Some(&Value::Int(2)));
    assert_eq!(
        properties.0.get("preserved"),
        Some(&Value::String(String::from("original")))
    );
    let mut locked_body = OrderedMap::new();
    locked_body.insert(
        Value::String(String::from(protocol_amqp::LOCK_TOKENS)),
        Value::Array(Array::from(vec![Value::Uuid(lock_token)])),
    );
    management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "renew-deferred-1",
        protocol_amqp::RENEW_LOCK_OPERATION,
        locked_body.clone(),
    )
    .await?;
    let mut abandon_properties = OrderedMap::new();
    abandon_properties.insert(
        Value::String(String::from("stage")),
        Value::String(String::from("abandoned")),
    );
    locked_body.insert(
        Value::String(String::from("disposition-status")),
        Value::String(String::from("abandoned")),
    );
    locked_body.insert(
        Value::String(String::from("properties-to-modify")),
        Value::Map(abandon_properties),
    );
    management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "abandon-deferred-1",
        protocol_amqp::UPDATE_DISPOSITION_OPERATION,
        locked_body,
    )
    .await?;

    let hidden_after_abandon =
        tokio::time::timeout(Duration::from_millis(300), receiver.recv()).await;
    assert!(
        hidden_after_abandon.is_err(),
        "abandoning a deferred message made it ordinarily visible"
    );
    let response = management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "receive-deferred-2",
        protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
        receive_body,
    )
    .await?;
    let (abandoned, lock_token) = deferred_response(response)?;
    assert_eq!(
        abandoned
            .header
            .as_ref()
            .map(|header| header.delivery_count),
        Some(2)
    );
    let properties = abandoned
        .application_properties
        .as_ref()
        .expect("the abandoned deferred message keeps application properties");
    assert_eq!(
        properties.0.get("stage"),
        Some(&Value::String(String::from("abandoned")))
    );
    assert_eq!(
        properties.0.get("preserved"),
        Some(&Value::String(String::from("original")))
    );

    let mut dead_letter_properties = OrderedMap::new();
    dead_letter_properties.insert(
        Value::String(String::from("stage")),
        Value::String(String::from("deadlettered")),
    );
    let mut dead_letter_body = OrderedMap::new();
    dead_letter_body.insert(
        Value::String(String::from(protocol_amqp::LOCK_TOKENS)),
        Value::Array(Array::from(vec![Value::Uuid(lock_token)])),
    );
    dead_letter_body.insert(
        Value::String(String::from("disposition-status")),
        Value::String(String::from("suspended")),
    );
    dead_letter_body.insert(
        Value::String(String::from("properties-to-modify")),
        Value::Map(dead_letter_properties),
    );
    dead_letter_body.insert(
        Value::String(String::from("deadletter-reason")),
        Value::String(String::from("DeferredRejected")),
    );
    dead_letter_body.insert(
        Value::String(String::from("deadletter-description")),
        Value::String(String::from("deferred validation failed")),
    );
    management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "deadletter-deferred-1",
        protocol_amqp::UPDATE_DISPOSITION_OPERATION,
        dead_letter_body,
    )
    .await?;

    let mut dead_letters = Receiver::attach(
        &mut session,
        "deferred-dlq-receiver",
        "orders/$deadletterqueue",
    )
    .await?;
    let dead_lettered = tokio::time::timeout(Duration::from_secs(2), dead_letters.recv()).await??;
    assert_eq!(text_of(dead_lettered.message()), "park-me");
    let properties = dead_lettered
        .message()
        .application_properties
        .as_ref()
        .expect("the dead-lettered message carries application properties");
    assert_eq!(
        properties.0.get("stage"),
        Some(&Value::String(String::from("deadlettered")))
    );
    assert_eq!(
        properties.0.get("preserved"),
        Some(&Value::String(String::from("original")))
    );
    assert_eq!(
        properties.0.get(protocol_amqp::DEAD_LETTER_REASON_PROPERTY),
        Some(&Value::String(String::from("DeferredRejected")))
    );
    assert_eq!(
        properties
            .0
            .get(protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY),
        Some(&Value::String(String::from("deferred validation failed")))
    );
    dead_letters.accept(&dead_lettered).await?;
    connection.close().await?;
    Ok(())
}
