//! A real AMQP client against the real listener.
//!
//! Everything below the socket is the production path: the acceptor, the command
//! bus, the state machine, and a store. Only the store's location and the clock
//! are test-owned.

use std::error::Error;

use amqp::{
    ApplicationProperties, Array, Body, ClientConnection as Connection, ClientReceiver as Receiver,
    ClientSender as Sender, ClientSession as Session, FilterSet, Message, Modified, OrderedMap,
    Outcome, Properties, SenderSettleMode, Source, Symbol, Uuid, Value, decode_message,
    encode_message,
};
use domain::{CommandKind, QueueConfig, StateMachine};
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::net::TcpListener;

/// A listener on an ephemeral port, with the queue already created.
struct Node {
    _broker: Broker,
    address: String,
    clock: ManualClock,
}

impl Node {
    async fn start(queue: &str, config: QueueConfig) -> Result<Self, Box<dyn Error>> {
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
            _broker: broker,
            address,
            clock,
        })
    }

    async fn connect(&self) -> Result<Connection, Box<dyn Error>> {
        Ok(Connection::builder()
            .container_id("test-client")
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

#[tokio::test(flavor = "multi_thread")]
async fn a_client_sends_a_message_and_another_receives_it() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", QueueConfig::default()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;

    let mut message = Message::builder().body(body("payload")).build();
    message.properties = Some(Properties {
        message_id: Some(String::from("order-1").into()),
        ..Properties::default()
    });
    // The broker accepts only after the command committed, so this outcome
    // means the message is durable rather than merely received.
    assert!(matches!(sender.send(message).await?, Outcome::Accepted(_)));

    let mut receiver = Receiver::attach(&mut session, "test-receiver", "orders").await?;
    let delivery = receiver.recv().await?;
    assert_eq!(text_of(delivery.message()), "payload");
    assert_eq!(
        delivery
            .message()
            .properties
            .as_ref()
            .and_then(|properties| properties.message_id.clone()),
        Some(String::from("order-1").into())
    );
    receiver.accept(&delivery).await?;

    sender.close().await?;
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_peeks_without_locking_or_consuming() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", QueueConfig::default()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    sender
        .send(Message::builder().body(body("peek-at-me")).build())
        .await?;

    let reply_to = "peek-management-replies";
    let mut responses = Receiver::builder()
        .name("peek-management-response")
        .source("orders/$management")
        .target(reply_to)
        .attach(&mut session)
        .await?;
    let mut requests = Sender::attach(
        &mut session,
        "peek-management-request",
        "orders/$management",
    )
    .await?;

    let mut request_body = OrderedMap::new();
    request_body.insert(
        Value::String(String::from(protocol_amqp::FROM_SEQUENCE_NUMBER)),
        Value::Long(1),
    );
    request_body.insert(
        Value::String(String::from(protocol_amqp::MESSAGE_COUNT)),
        Value::Int(1),
    );
    let request = Message::builder()
        .properties(Properties {
            message_id: Some("peek-1".into()),
            reply_to: Some(reply_to.to_owned()),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert(
                    protocol_amqp::OPERATION_PROPERTY,
                    protocol_amqp::PEEK_MESSAGE_OPERATION,
                )
                .build(),
        )
        .body(Body::Value(Value::Map(request_body)))
        .build();
    assert!(matches!(
        requests.send(request).await?,
        Outcome::Accepted(_)
    ));

    let response =
        tokio::time::timeout(std::time::Duration::from_secs(2), responses.recv()).await??;
    assert_eq!(
        response
            .message()
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY)),
        Some(&Value::Int(200))
    );
    let Body::Value(Value::Map(body)) = &response.message().body else {
        panic!("the peek response must carry an AMQP value map");
    };
    let messages = body
        .get(&Value::String(String::from(protocol_amqp::MESSAGES)))
        .expect("peek response carries messages");
    let Value::List(messages) = messages else {
        panic!("messages must be an AMQP list, got {messages:?}");
    };
    let [Value::Map(entry)] = messages.as_slice() else {
        panic!("expected exactly one peeked message, got {messages:?}");
    };
    let Some(Value::Binary(encoded)) =
        entry.get(&Value::String(String::from(protocol_amqp::MESSAGE)))
    else {
        panic!("the peeked entry must carry encoded message bytes");
    };
    let peeked = decode_message(encoded)?;
    assert_eq!(text_of(&peeked), "peek-at-me");
    assert_eq!(
        peeked
            .message_annotations
            .as_ref()
            .and_then(|annotations| annotations.get(Symbol::from("x-opt-sequence-number"))),
        Some(&Value::Long(1))
    );
    responses.accept(&response).await?;

    let mut receiver = Receiver::attach(&mut session, "test-receiver", "orders").await?;
    let delivery = receiver.recv().await?;
    assert_eq!(text_of(delivery.message()), "peek-at-me");
    receiver.accept(&delivery).await?;

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_gets_successful_acknowledgements_for_duplicate_sends()
-> Result<(), Box<dyn Error>> {
    let node = Node::start(
        "orders",
        QueueConfig {
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )
    .await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    for text in ["original", "retried-with-different-body"] {
        let message = Message::builder()
            .properties(Properties {
                message_id: Some("retry-id".into()),
                ..Properties::default()
            })
            .body(body(text))
            .build();
        assert!(matches!(sender.send(message).await?, Outcome::Accepted(_)));
    }
    let peek = || {
        node._broker.handle().submit_blocking(
            domain::NamespaceName::new("tenant").expect("a valid namespace"),
            domain::EntityPath::new("orders").expect("a valid entity"),
            CommandKind::Peek {
                from_sequence: domain::SequenceNumber::new(0),
                max_messages: 10,
                session_id: None,
            },
        )
    };
    let domain::CommandOutcome::Peeked(messages) = peek()? else {
        panic!("peek outcome");
    };
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body, b"original");
    let mut receiver = Receiver::attach(&mut session, "test-receiver", "orders").await?;
    let delivery =
        tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv()).await??;
    assert_eq!(text_of(delivery.message()), "original");
    receiver.accept(&delivery).await?;
    receiver.close().await?;
    assert!(matches!(
        sender
            .send(
                Message::builder()
                    .properties(Properties {
                        message_id: Some("retry-id".into()),
                        ..Properties::default()
                    })
                    .body(body("retry-after-completion"))
                    .build()
            )
            .await?,
        Outcome::Accepted(_)
    ));
    assert_eq!(peek()?, domain::CommandOutcome::Peeked(Vec::new()));
    sender.close().await?;
    session.end().await?;
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_overlong_message_id_is_rejected_and_the_sender_stays_usable()
-> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", QueueConfig::default()).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    let invalid = Message::builder()
        .properties(Properties {
            message_id: Some("a".repeat(129).into()),
            ..Properties::default()
        })
        .body(body("invalid"))
        .build();
    let Outcome::Rejected(rejected) = sender.send(invalid).await? else {
        panic!("an overlong ID must be rejected");
    };
    assert_eq!(
        rejected
            .error
            .expect("rejection condition")
            .condition
            .as_symbol(),
        Symbol::from(protocol_amqp::INVALID_FIELD)
    );
    let valid = Message::builder()
        .properties(Properties {
            message_id: Some("a".repeat(128).into()),
            ..Properties::default()
        })
        .body(body("valid"))
        .build();
    assert!(matches!(sender.send(valid).await?, Outcome::Accepted(_)));
    let mut receiver = Receiver::attach(&mut session, "test-receiver", "orders").await?;
    let delivery =
        tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv()).await??;
    assert_eq!(text_of(delivery.message()), "valid");
    receiver.accept(&delivery).await?;
    sender.close().await?;
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_defers_and_receives_by_sequence() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", QueueConfig::default()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    sender
        .send(Message::builder().body(body("later")).build())
        .await?;

    let link_name = "test-receiver";
    let mut receiver = Receiver::attach(&mut session, link_name, "orders").await?;
    let delivery = receiver.recv().await?;
    receiver
        .modify(
            &delivery,
            Modified {
                undeliverable_here: Some(true),
                ..Modified::default()
            },
        )
        .await?;
    let starved =
        tokio::time::timeout(std::time::Duration::from_millis(300), receiver.recv()).await;
    assert!(starved.is_err(), "a deferred message was still ready");

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

    let mut request_body = OrderedMap::new();
    request_body.insert(
        Value::String(String::from(protocol_amqp::SEQUENCE_NUMBERS)),
        Value::Array(Array::from(vec![Value::Long(1)])),
    );
    request_body.insert(
        Value::String(String::from(protocol_amqp::RECEIVER_SETTLE_MODE)),
        Value::Uint(1),
    );
    let request = Message::builder()
        .properties(Properties {
            message_id: Some("receive-deferred-1".into()),
            reply_to: Some(reply_to.to_owned()),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert(
                    protocol_amqp::OPERATION_PROPERTY,
                    protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
                )
                .insert(protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY, link_name)
                .build(),
        )
        .body(Body::Value(Value::Map(request_body)))
        .build();
    assert!(matches!(
        requests.send(request).await?,
        Outcome::Accepted(_)
    ));

    let response =
        tokio::time::timeout(std::time::Duration::from_secs(2), responses.recv()).await??;
    assert_eq!(
        response
            .message()
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY)),
        Some(&Value::Int(200))
    );
    let Body::Value(Value::Map(body)) = &response.message().body else {
        panic!("the deferred receive response must carry an AMQP value map");
    };
    let messages = body
        .get(&Value::String(String::from(protocol_amqp::MESSAGES)))
        .expect("deferred receive response carries messages");
    let Value::List(messages) = messages else {
        panic!("messages must be an AMQP list, got {messages:?}");
    };
    let [Value::Map(entry)] = messages.as_slice() else {
        panic!("expected exactly one deferred message, got {messages:?}");
    };
    let Some(Value::Binary(encoded)) =
        entry.get(&Value::String(String::from(protocol_amqp::MESSAGE)))
    else {
        panic!("the deferred entry must carry encoded message bytes");
    };
    let Some(Value::Uuid(lock_token)) =
        entry.get(&Value::String(String::from(protocol_amqp::LOCK_TOKEN)))
    else {
        panic!("the deferred entry must carry a lock token");
    };
    let deferred = decode_message(encoded)?;
    assert_eq!(text_of(&deferred), "later");
    responses.accept(&response).await?;

    let mut complete_body = OrderedMap::new();
    complete_body.insert(
        Value::String(String::from(protocol_amqp::LOCK_TOKENS)),
        Value::Array(Array::from(vec![Value::Uuid(lock_token.clone())])),
    );
    complete_body.insert(
        Value::String(String::from(protocol_amqp::DISPOSITION_STATUS)),
        Value::String(String::from("completed")),
    );
    let complete = Message::builder()
        .properties(Properties {
            message_id: Some("complete-deferred-1".into()),
            reply_to: Some(reply_to.to_owned()),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert(
                    protocol_amqp::OPERATION_PROPERTY,
                    protocol_amqp::UPDATE_DISPOSITION_OPERATION,
                )
                .insert(protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY, link_name)
                .build(),
        )
        .body(Body::Value(Value::Map(complete_body)))
        .build();
    assert!(matches!(
        requests.send(complete).await?,
        Outcome::Accepted(_)
    ));
    let completed =
        tokio::time::timeout(std::time::Duration::from_secs(2), responses.recv()).await??;
    assert_eq!(
        completed
            .message()
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY)),
        Some(&Value::Int(200))
    );
    responses.accept(&completed).await?;

    let gone = tokio::time::timeout(std::time::Duration::from_millis(300), receiver.recv()).await;
    assert!(gone.is_err(), "a completed deferred message came back");

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_completed_message_is_not_delivered_again() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", QueueConfig::default()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    sender
        .send(Message::builder().body(body("once")).build())
        .await?;

    let mut receiver = Receiver::attach(&mut session, "test-receiver", "orders").await?;
    let delivery = receiver.recv().await?;
    receiver.accept(&delivery).await?;

    // Accepting settles the message, so nothing is left to hand out. The
    // receiver would otherwise sit here until the test timed out.
    let starved =
        tokio::time::timeout(std::time::Duration::from_millis(300), receiver.recv()).await;
    assert!(starved.is_err(), "a settled message came back");

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_renews_a_live_message_lock_over_the_management_node() -> Result<(), Box<dyn Error>>
{
    let node = Node::start(
        "orders",
        QueueConfig {
            lock_duration_millis: 30_000,
            ..QueueConfig::default()
        },
    )
    .await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    sender
        .send(Message::builder().body(body("renew-me")).build())
        .await?;

    let mut receiver = Receiver::attach(&mut session, "renewable-receiver", "orders").await?;
    let delivery = receiver.recv().await?;

    let reply_to = String::from("management-replies");
    let mut responses = Receiver::builder()
        .name("management-response")
        .source("orders/$management")
        .target(reply_to.clone())
        .attach(&mut session)
        .await?;
    let mut requests =
        Sender::attach(&mut session, "management-request", "orders/$management").await?;

    // A fresh queue's first peek-lock token is one. On the wire that token is
    // the same 16-byte UUID carried as the original delivery tag.
    let mut token = [0_u8; 16];
    token[8..].copy_from_slice(&1_u64.to_be_bytes());
    let mut request_body = OrderedMap::new();
    request_body.insert(
        Value::String(String::from(protocol_amqp::LOCK_TOKENS)),
        Value::Array(Array::from(vec![Value::Uuid(Uuid::from(token))])),
    );
    let request = Message::builder()
        .properties(Properties {
            message_id: Some("renew-1".into()),
            reply_to: Some(reply_to),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert(
                    protocol_amqp::OPERATION_PROPERTY,
                    protocol_amqp::RENEW_LOCK_OPERATION,
                )
                .insert(
                    protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY,
                    "renewable-receiver",
                )
                .build(),
        )
        .body(Body::Value(Value::Map(request_body)))
        .build();
    assert!(matches!(
        requests.send(request).await?,
        Outcome::Accepted(_)
    ));

    let response =
        tokio::time::timeout(std::time::Duration::from_secs(2), responses.recv()).await??;
    assert_eq!(
        response
            .message()
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY)),
        Some(&Value::Int(200))
    );
    let Body::Value(Value::Map(body)) = &response.message().body else {
        panic!("the renewal response must carry an AMQP value map");
    };
    let expirations = body.iter().find_map(|(key, value)| {
        (key == &Value::String(String::from(protocol_amqp::EXPIRATIONS))).then_some(value)
    });
    assert!(
        matches!(expirations, Some(Value::Array(values)) if matches!(values.as_slice(), [Value::Timestamp(value)] if value.milliseconds() == 31_000)),
        "unexpected renewal expirations: {expirations:?}"
    );
    responses.accept(&response).await?;

    // Renewal kept the delivery token valid, so ordinary link settlement still
    // completes it.
    receiver.accept(&delivery).await?;
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_released_message_comes_round_again() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", QueueConfig::default()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    sender
        .send(Message::builder().body(body("retry-me")).build())
        .await?;

    let mut receiver = Receiver::attach(&mut session, "test-receiver", "orders").await?;
    let first = receiver.recv().await?;
    receiver.release(&first).await?;

    // Releasing abandons the lock, so the message returns to the queue with its
    // delivery count already counted against it.
    let second = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv()).await??;
    assert_eq!(text_of(second.message()), "retry-me");
    receiver.accept(&second).await?;

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pre_settled_receiver_gets_at_most_once() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", QueueConfig::default()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    sender
        .send(Message::builder().body(body("fire-and-forget")).build())
        .await?;

    // Asking for pre-settled transfers is asking for receive-and-delete: the
    // broker deletes before the transfer, so nothing is ever redelivered.
    let mut receiver = Receiver::builder()
        .name("test-receiver")
        .source("orders")
        .sender_settle_mode(SenderSettleMode::Settled)
        .attach(&mut session)
        .await?;
    let delivery = receiver.recv().await?;
    assert_eq!(text_of(delivery.message()), "fire-and-forget");

    // Never settled by the client, and still gone: at-most-once means the
    // deletion committed before the transfer.
    let starved =
        tokio::time::timeout(std::time::Duration::from_millis(300), receiver.recv()).await;
    assert!(starved.is_err(), "the message survived receive-and-delete");

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_receiver_waiting_on_an_empty_queue_is_woken_by_a_send() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", QueueConfig::default()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;

    // The receiver goes first and the queue is empty, so it is parked on the
    // broker's wakeup rather than a poll.
    let mut receiver = Receiver::attach(&mut session, "test-receiver", "orders").await?;
    let waiting = tokio::spawn(async move {
        let delivery = receiver.recv().await.map_err(|error| error.to_string())?;
        receiver
            .accept(&delivery)
            .await
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(text_of(delivery.message()))
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let started = std::time::Instant::now();
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    sender
        .send(Message::builder().body(body("wake-up")).build())
        .await?;

    let received = tokio::time::timeout(std::time::Duration::from_secs(2), waiting)
        .await??
        .map_err(|error| -> Box<dyn Error> { error.into() })?;
    assert_eq!(received, "wake-up");
    // Under the 3-second fallback: the delivery came from the wakeup, not from
    // the safety-net re-poll.
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "delivery took {:?}, which is the fallback, not the wakeup",
        started.elapsed()
    );

    connection.close().await?;
    Ok(())
}

fn session_source(queue: &str, session: Option<&str>) -> Source {
    let mut filter = FilterSet::default();
    filter.insert(
        Symbol::from(protocol_amqp::SESSION_FILTER),
        session.map_or(Value::Null, |id| Value::String(id.to_owned())),
    );
    Source::builder().address(queue).filter(filter).build()
}

fn session_of(source: &Option<Source>) -> Option<String> {
    source
        .as_ref()?
        .filter
        .as_ref()?
        .get(&Symbol::from(protocol_amqp::SESSION_FILTER))
        .and_then(|value| match value {
            Value::String(id) => Some(id.clone()),
            _ => None,
        })
}

fn with_session(text: &str, session: &str) -> Message {
    let mut message = Message::builder().body(body(text)).build();
    message.properties = Some(Properties {
        group_id: Some(session.to_owned()),
        ..Properties::default()
    });
    message
}

fn session_queue_config() -> QueueConfig {
    QueueConfig {
        requires_session: true,
        ..QueueConfig::default()
    }
}

async fn session_management_request(
    requests: &mut Sender,
    responses: &mut Receiver,
    reply_to: &str,
    message_id: &str,
    operation: &str,
    link_name: &str,
    body: OrderedMap<Value, Value>,
) -> Result<Message, Box<dyn Error>> {
    let response = management_request(
        requests,
        responses,
        reply_to,
        message_id,
        operation,
        Some(link_name),
        body,
    )
    .await?;
    assert_eq!(
        response
            .application_properties
            .as_ref()
            .and_then(|properties| { properties.get(protocol_amqp::STATUS_CODE_PROPERTY) }),
        Some(&Value::Int(200))
    );
    Ok(response)
}

async fn management_request(
    requests: &mut Sender,
    responses: &mut Receiver,
    reply_to: &str,
    message_id: &str,
    operation: &str,
    link_name: Option<&str>,
    body: OrderedMap<Value, Value>,
) -> Result<Message, Box<dyn Error>> {
    let mut properties = ApplicationProperties::default();
    properties.insert(protocol_amqp::OPERATION_PROPERTY, operation.to_owned());
    if let Some(link_name) = link_name {
        properties.insert(
            protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY,
            link_name.to_owned(),
        );
    }
    let request = Message::builder()
        .properties(Properties {
            message_id: Some(message_id.to_owned().into()),
            reply_to: Some(reply_to.to_owned()),
            ..Properties::default()
        })
        .application_properties(properties)
        .body(Body::Value(Value::Map(body)))
        .build();
    assert!(matches!(
        requests.send(request).await?,
        Outcome::Accepted(_)
    ));

    let response =
        tokio::time::timeout(std::time::Duration::from_secs(2), responses.recv()).await??;
    let message = response.message().clone();
    responses.accept(&response).await?;
    Ok(message)
}

fn scheduled_wire_message(text: &str, enqueue_at: i64) -> Message {
    let mut message = Message::data(text.as_bytes().to_vec());
    let mut annotations = OrderedMap::new();
    annotations.insert(
        Symbol::from(protocol_amqp::SCHEDULED_ENQUEUE_TIME_ANNOTATION),
        Value::Timestamp(enqueue_at.into()),
    );
    message.message_annotations = Some(annotations.into());
    message
}

fn schedule_request_body(messages: &[Message]) -> Result<OrderedMap<Value, Value>, Box<dyn Error>> {
    let mut entries = Vec::new();
    for message in messages {
        let mut entry = OrderedMap::new();
        entry.insert(
            Value::String(protocol_amqp::MESSAGE.to_owned()),
            Value::Binary(encode_message(message)?.into()),
        );
        entry.insert(Value::String("message-id".to_owned()), Value::Null);
        entries.push(Value::Map(entry));
    }
    let mut body = OrderedMap::new();
    body.insert(
        Value::String(protocol_amqp::MESSAGES.to_owned()),
        Value::List(entries),
    );
    Ok(body)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_schedules_peeks_cancels_and_receives_after_activation()
-> Result<(), Box<dyn Error>> {
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        scheduled_message_workflow(),
    )
    .await
    .expect("scheduling workflow completes")
}

async fn scheduled_message_workflow() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", QueueConfig::default()).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let reply_to = "scheduling-replies";
    let mut responses = Receiver::builder()
        .name("scheduling-responses")
        .source("orders/$management")
        .target(reply_to)
        .attach(&mut session)
        .await?;
    let mut requests =
        Sender::attach(&mut session, "scheduling-requests", "orders/$management").await?;
    // Scheduling requires no ordinary sending link and accepts SDK-generated null IDs.
    let scheduled = management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "schedule-1",
        protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
        None,
        schedule_request_body(&[
            scheduled_wire_message("cancel-me", 2_000),
            scheduled_wire_message("activate-me", 2_000),
        ])?,
    )
    .await?;
    assert_eq!(
        scheduled
            .application_properties
            .as_ref()
            .and_then(|properties| { properties.get(protocol_amqp::STATUS_CODE_PROPERTY) }),
        Some(&Value::Int(200))
    );
    let Body::Value(Value::Map(body)) = scheduled.body else {
        panic!("scheduling responds with a map");
    };
    let Some(Value::Array(sequences)) =
        body.get(&Value::String(protocol_amqp::SEQUENCE_NUMBERS.to_owned()))
    else {
        panic!("scheduling responds with an array of long sequence numbers");
    };
    assert_eq!(sequences.len(), 2);
    assert!(
        sequences
            .iter()
            .all(|sequence| matches!(sequence, Value::Long(_)))
    );
    let scheduled_sequence = sequences[1].clone();
    let mut cancel = OrderedMap::new();
    cancel.insert(
        Value::String(protocol_amqp::SEQUENCE_NUMBERS.to_owned()),
        Value::Array(Array::from(vec![sequences[0].clone()])),
    );
    let cancelled = management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "cancel-1",
        protocol_amqp::CANCEL_SCHEDULED_MESSAGE_OPERATION,
        None,
        cancel,
    )
    .await?;
    assert_eq!(
        cancelled
            .application_properties
            .as_ref()
            .and_then(|properties| { properties.get(protocol_amqp::STATUS_CODE_PROPERTY) }),
        Some(&Value::Int(200))
    );
    let mut peek = OrderedMap::new();
    peek.insert(
        Value::String(protocol_amqp::FROM_SEQUENCE_NUMBER.to_owned()),
        Value::Long(0),
    );
    peek.insert(
        Value::String(protocol_amqp::MESSAGE_COUNT.to_owned()),
        Value::Int(10),
    );
    let peeked = management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "peek-scheduled",
        protocol_amqp::PEEK_MESSAGE_OPERATION,
        None,
        peek,
    )
    .await?;
    let Body::Value(Value::Map(body)) = peeked.body else {
        panic!("peek responds with a map");
    };
    let Some(Value::List(messages)) = body.get(&Value::String(protocol_amqp::MESSAGES.to_owned()))
    else {
        panic!("peek responds with encoded messages");
    };
    assert_eq!(
        messages.len(),
        1,
        "cancellation removed the first scheduled message"
    );
    let Value::Map(entry) = &messages[0] else {
        panic!("peek entries are maps");
    };
    let Some(Value::Binary(encoded)) = entry.get(&Value::String(protocol_amqp::MESSAGE.to_owned()))
    else {
        panic!("peek messages are binary");
    };
    let peeked_message = decode_message(encoded)?;
    assert_eq!(text_of(&peeked_message), "activate-me");
    let annotations = peeked_message
        .message_annotations
        .as_ref()
        .expect("peek annotations");
    assert_eq!(
        annotations.get(Symbol::from(protocol_amqp::MESSAGE_STATE_ANNOTATION)),
        Some(&Value::Int(2))
    );
    assert_eq!(
        annotations.get(Symbol::from(
            protocol_amqp::SCHEDULED_ENQUEUE_TIME_ANNOTATION
        )),
        Some(&Value::Timestamp(2_000_i64.into()))
    );
    let mut receiver = Receiver::attach(&mut session, "scheduled-receiver", "orders").await?;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), receiver.recv())
            .await
            .is_err()
    );
    node.clock.set(2_000);
    assert_eq!(
        server::TimerWorker::new(&node._broker.handle())
            .sweep_once()?
            .messages_activated,
        1
    );
    let delivery =
        tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv()).await??;
    assert_eq!(text_of(delivery.message()), "activate-me");
    let annotations = delivery
        .message()
        .message_annotations
        .as_ref()
        .expect("delivery annotations");
    assert_ne!(
        annotations.get(Symbol::from("x-opt-sequence-number")),
        Some(&scheduled_sequence)
    );
    assert_eq!(
        annotations.get(Symbol::from(protocol_amqp::MESSAGE_STATE_ANNOTATION)),
        Some(&Value::Int(0))
    );
    assert_eq!(
        annotations.get(Symbol::from(
            protocol_amqp::SCHEDULED_ENQUEUE_TIME_ANNOTATION
        )),
        Some(&Value::Timestamp(2_000_i64.into()))
    );
    receiver.accept(&delivery).await?;
    tokio::time::timeout(std::time::Duration::from_secs(10), receiver.close())
        .await
        .expect("scheduled receiver close completes")?;
    tokio::time::timeout(std::time::Duration::from_secs(10), requests.close())
        .await
        .expect("scheduling request link close completes")?;
    tokio::time::timeout(std::time::Duration::from_secs(10), responses.close())
        .await
        .expect("scheduling response link close completes")?;
    tokio::time::timeout(std::time::Duration::from_secs(10), session.end())
        .await
        .expect("scheduling session end completes")?;
    tokio::time::timeout(std::time::Duration::from_secs(10), connection.close())
        .await
        .expect("scheduling connection close completes")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scheduled_annotation_on_an_ordinary_send_waits_until_due() -> Result<(), Box<dyn Error>>
{
    let node = Node::start("orders", QueueConfig::default()).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    assert!(matches!(
        sender
            .send(scheduled_wire_message("scheduled-transfer", 2_000))
            .await?,
        Outcome::Accepted(_)
    ));
    let mut receiver = Receiver::attach(&mut session, "test-receiver", "orders").await?;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), receiver.recv())
            .await
            .is_err()
    );
    node.clock.set(2_000);
    server::TimerWorker::new(&node._broker.handle()).sweep_once()?;
    let delivery =
        tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv()).await??;
    assert_eq!(text_of(delivery.message()), "scheduled-transfer");
    receiver.accept(&delivery).await?;
    sender.close().await?;
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_scheduled_batch_does_not_enqueue_earlier_entries() -> Result<(), Box<dyn Error>>
{
    let node = Node::start("orders", QueueConfig::default()).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let reply_to = "malformed-scheduling-replies";
    let mut responses = Receiver::builder()
        .name("scheduling-responses")
        .source("orders/$management")
        .target(reply_to)
        .attach(&mut session)
        .await?;
    let mut requests =
        Sender::attach(&mut session, "scheduling-requests", "orders/$management").await?;
    let response = management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "invalid-schedule",
        protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
        None,
        schedule_request_body(&[
            scheduled_wire_message("valid-first", 2_000),
            Message::data(b"missing-timestamp".to_vec()),
        ])?,
    )
    .await?;
    assert_eq!(
        response
            .application_properties
            .as_ref()
            .and_then(|properties| { properties.get(protocol_amqp::STATUS_CODE_PROPERTY) }),
        Some(&Value::Int(400))
    );
    assert_eq!(
        node._broker.handle().submit_blocking(
            domain::NamespaceName::new("tenant")?,
            domain::EntityPath::new("orders")?,
            CommandKind::Peek {
                from_sequence: domain::SequenceNumber::new(0),
                max_messages: 10,
                session_id: None,
            },
        )?,
        domain::CommandOutcome::Peeked(Vec::new()),
    );
    requests.close().await?;
    responses.close().await?;
    session.end().await?;
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_receiver_gets_only_its_session_in_order() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", session_queue_config()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    sender.send(with_session("other", "cart-2")).await?;
    sender.send(with_session("first", "cart-1")).await?;
    sender.send(with_session("second", "cart-1")).await?;

    let mut receiver = Receiver::builder()
        .name("test-receiver")
        .source(session_source("orders", Some("cart-1")))
        .attach(&mut session)
        .await?;

    // FIFO within the session, and nothing from any other session.
    for expected in ["first", "second"] {
        let delivery = receiver.recv().await?;
        assert_eq!(text_of(delivery.message()), expected);
        receiver.accept(&delivery).await?;
    }
    let starved =
        tokio::time::timeout(std::time::Duration::from_millis(300), receiver.recv()).await;
    assert!(starved.is_err(), "another session's message leaked through");

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_receiver_manages_its_lock_and_state() -> Result<(), Box<dyn Error>> {
    let node = Node::start(
        "orders",
        QueueConfig {
            lock_duration_millis: 30_000,
            ..session_queue_config()
        },
    )
    .await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    sender.send(with_session("payload", "cart-1")).await?;

    let link_name = "managed-session-receiver";
    let mut receiver = Receiver::builder()
        .name(link_name)
        .source(session_source("orders", Some("cart-1")))
        .attach(&mut session)
        .await?;
    let reply_to = "session-management-replies";
    let mut responses = Receiver::builder()
        .name("session-management-response")
        .source("orders/$management")
        .target(reply_to)
        .attach(&mut session)
        .await?;
    let mut requests = Sender::attach(
        &mut session,
        "session-management-request",
        "orders/$management",
    )
    .await?;

    let mut set_body = OrderedMap::new();
    set_body.insert(
        Value::String(String::from(protocol_amqp::SESSION_ID)),
        Value::String(String::from("cart-1")),
    );
    set_body.insert(
        Value::String(String::from(protocol_amqp::SESSION_STATE)),
        Value::Binary(b"checkout-step-2".to_vec().into()),
    );
    let set_response = session_management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "set-session-state-1",
        protocol_amqp::SET_SESSION_STATE_OPERATION,
        link_name,
        set_body,
    )
    .await?;
    assert_eq!(set_response.body, Body::Value(Value::Null));

    let mut session_body = OrderedMap::new();
    session_body.insert(
        Value::String(String::from(protocol_amqp::SESSION_ID)),
        Value::String(String::from("cart-1")),
    );
    let get_response = session_management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "get-session-state-1",
        protocol_amqp::GET_SESSION_STATE_OPERATION,
        link_name,
        session_body.clone(),
    )
    .await?;
    let Body::Value(Value::Map(get_body)) = get_response.body else {
        panic!("the state response must carry an AMQP value map");
    };
    assert_eq!(
        get_body.get(&Value::String(String::from(protocol_amqp::SESSION_STATE))),
        Some(&Value::Binary(b"checkout-step-2".to_vec().into()))
    );

    let renew_response = session_management_request(
        &mut requests,
        &mut responses,
        reply_to,
        "renew-session-lock-1",
        protocol_amqp::RENEW_SESSION_LOCK_OPERATION,
        link_name,
        session_body,
    )
    .await?;
    let Body::Value(Value::Map(renew_body)) = renew_response.body else {
        panic!("the renewal response must carry an AMQP value map");
    };
    assert!(matches!(
        renew_body.get(&Value::String(String::from(protocol_amqp::EXPIRATION))),
        Some(Value::Timestamp(value)) if value.milliseconds() == 31_000
    ));

    let delivery = receiver.recv().await?;
    assert_eq!(text_of(delivery.message()), "payload");
    receiver.accept(&delivery).await?;
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_next_available_receiver_learns_which_session_it_got() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", session_queue_config()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    sender.send(with_session("payload", "cart-9")).await?;

    // A null filter asks for whichever session the broker grants; the echoed
    // attach carries the granted identifier.
    let mut receiver = Receiver::builder()
        .name("test-receiver")
        .source(session_source("orders", None))
        .attach(&mut session)
        .await?;
    assert_eq!(session_of(receiver.source()), Some(String::from("cart-9")));

    let delivery = receiver.recv().await?;
    assert_eq!(text_of(delivery.message()), "payload");
    receiver.accept(&delivery).await?;

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_held_session_is_refused_until_its_link_closes() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", session_queue_config()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let holder = Receiver::builder()
        .name("holder")
        .source(session_source("orders", Some("cart-1")))
        .attach(&mut session)
        .await?;

    // Isolate the rival's transport session: its initial credit may cross the
    // error Detach and end that AMQP session without retiring the holder.
    let mut rival_session = Session::begin(&mut connection).await?;
    let mut rival = Receiver::builder()
        .name("rival")
        .source(session_source("orders", Some("cart-1")))
        .attach(&mut rival_session)
        .await?;
    let refused = tokio::time::timeout(std::time::Duration::from_secs(2), rival.recv()).await?;
    assert!(refused.is_err(), "a held session was granted twice");

    // Closing the holder releases the session rather than waiting out its lock.
    holder.close().await?;
    let mut next = Receiver::builder()
        .name("next")
        .source(session_source("orders", Some("cart-1")))
        .attach(&mut session)
        .await?;
    let waiting = tokio::time::timeout(std::time::Duration::from_millis(300), next.recv()).await;
    assert!(
        waiting.is_err(),
        "a healthy link waits on an empty session instead of erroring"
    );

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_message_is_drained_from_the_dead_letter_queue() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", QueueConfig::default()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    sender
        .send(Message::builder().body(body("poison")).build())
        .await?;

    // Rejecting a delivery dead-letters it rather than redelivering it.
    let mut receiver = Receiver::attach(&mut session, "test-receiver", "orders").await?;
    let delivery = receiver.recv().await?;
    receiver.reject(&delivery, None).await?;

    // The dead-letter queue is addressed as a sub-queue and drained like one.
    let mut dead_letter_receiver =
        Receiver::attach(&mut session, "dlq-receiver", "orders/$deadletterqueue").await?;
    let poisoned = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        dead_letter_receiver.recv(),
    )
    .await??;
    assert_eq!(text_of(poisoned.message()), "poison");
    // The drained message says why it is there, in the properties the SDKs read.
    let reason = poisoned
        .message()
        .application_properties
        .as_ref()
        .and_then(|properties| properties.0.get("DeadLetterReason"))
        .cloned();
    assert_eq!(
        reason,
        Some(Value::String(String::from("RejectedByReceiver")))
    );
    dead_letter_receiver.accept(&poisoned).await?;

    // Completing in the dead-letter queue removes the message permanently.
    let drained = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        dead_letter_receiver.recv(),
    )
    .await;
    assert!(drained.is_err(), "the dead-letter queue still held it");

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sender_cannot_attach_to_a_dead_letter_queue() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", QueueConfig::default()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut smuggler = Sender::attach(&mut session, "smuggler", "orders/$deadletterqueue").await?;

    // The attach completes and the link is then refused; the send never lands.
    let outcome = smuggler
        .send(Message::builder().body(body("smuggled")).build())
        .await;
    assert!(outcome.is_err(), "a send into the dead-letter queue landed");

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn attaching_to_a_queue_that_does_not_exist_is_refused() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", QueueConfig::default()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "invoices").await?;

    // The link attaches — the broker learns the queue is missing only when a
    // command reaches it — and the send is rejected rather than silently lost.
    let outcome = sender
        .send(Message::builder().body(body("nowhere")).build())
        .await?;
    assert!(
        matches!(outcome, Outcome::Rejected(_)),
        "expected a rejection, got {outcome:?}"
    );

    connection.close().await?;
    Ok(())
}
