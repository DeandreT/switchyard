//! A real AMQP client against the real listener.
//!
//! Everything below the socket is the production path: the acceptor, the command
//! bus, the state machine, and a store. Only the store's location and the clock
//! are test-owned.

use std::error::Error;

use amqp::{
    AnnotationKey, ApplicationProperties, Array, Body, ClientConnection as Connection,
    ClientReceiver as Receiver, ClientSender as Sender, ClientSession as Session,
    DeliveryAnnotations, Error as AmqpError, ErrorCondition, Fields, FilterSet, Footer, Header,
    Message, MessageAnnotations, Modified, OrderedMap, Outcome, Properties, SenderSettleMode,
    Source, Symbol, Uuid, Value,
};
use domain::{CommandKind, QueueConfig, StateMachine};
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::net::TcpListener;

/// A listener on an ephemeral port, with the queue already created.
struct Node {
    _broker: Broker,
    address: String,
}

impl Node {
    async fn start(queue: &str, config: QueueConfig) -> Result<Self, Box<dyn Error>> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            ManualClock::at(1_000),
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
async fn a_rich_envelope_survives_release_and_broker_overlays() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", QueueConfig::default()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;

    let mut delivery_annotations = DeliveryAnnotations::default();
    delivery_annotations.insert("x-custom-delivery", "delivery-value");
    delivery_annotations.insert(700_u64, 701_i32);
    delivery_annotations.insert("x-opt-lock-token", "forged-lock-token");
    let mut message_annotations = MessageAnnotations::default();
    message_annotations.insert("x-custom-message", "message-value");
    message_annotations.insert(800_u64, 801_i32);
    message_annotations.insert("x-opt-deadletter-source", "forged-source");
    let mut footer = Footer::default();
    footer.insert("x-custom-footer", "footer-value");
    footer.insert(900_u64, 901_i32);
    let sent = Message::builder()
        .header(Header {
            durable: true,
            priority: 8,
            ttl: Some(5_000),
            first_acquirer: true,
            // The broker owns this field and must replace the sender's value.
            delivery_count: 99,
        })
        .delivery_annotations(delivery_annotations)
        .message_annotations(message_annotations)
        .properties(Properties {
            message_id: Some(amqp::MessageId::Ulong(42)),
            user_id: Some(b"user-1".to_vec().into()),
            to: Some(String::from("logical-orders")),
            subject: Some(String::from("created")),
            reply_to: Some(String::from("replies")),
            correlation_id: Some(amqp::MessageId::Ulong(43)),
            content_type: Some(Symbol::from("application/octet-stream")),
            content_encoding: Some(Symbol::from("identity")),
            creation_time: Some(-123),
            group_sequence: Some(7),
            reply_to_group_id: Some(String::from("reply-group")),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert("custom-string", "application-value")
                .insert("custom-number", 44_i32)
                .build(),
        )
        .body(Body::Data(vec![
            b"section-one-".to_vec().into(),
            b"section-two".to_vec().into(),
        ]))
        .footer(footer)
        .build();
    sender.send(sent.clone()).await?;

    let mut receiver = Receiver::attach(&mut session, "test-receiver", "orders").await?;
    let first = receiver.recv().await?;
    assert_rich_envelope(first.message(), &sent, 0, 1);
    receiver.release(&first).await?;

    let second = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv()).await??;
    assert_rich_envelope(second.message(), &sent, 1, 2);
    receiver.accept(&second).await?;

    connection.close().await?;
    Ok(())
}

fn assert_rich_envelope(actual: &Message, sent: &Message, delivery_count: u32, lock_token: u64) {
    let header = actual.header.as_ref().expect("the header survives");
    assert!(header.durable);
    assert_eq!(header.priority, 8);
    assert_eq!(header.ttl, Some(5_000));
    assert!(header.first_acquirer);
    assert_eq!(header.delivery_count, delivery_count);

    let delivery_annotations = actual
        .delivery_annotations
        .as_ref()
        .expect("delivery annotations are present");
    assert_eq!(
        delivery_annotations.get(&AnnotationKey::from("x-custom-delivery")),
        Some(&Value::String(String::from("delivery-value")))
    );
    assert_eq!(
        delivery_annotations.get(&AnnotationKey::from(700_u64)),
        Some(&Value::Int(701))
    );
    assert!(matches!(
        delivery_annotations.get(&AnnotationKey::from("x-opt-lock-token")),
        Some(Value::Uuid(value))
            if value.as_inner()[..8] == [0; 8]
                && value.as_inner()[8..] == lock_token.to_be_bytes()
    ));
    let annotations = actual
        .message_annotations
        .as_ref()
        .expect("message annotations are present");
    assert_eq!(
        annotations.get(&AnnotationKey::from("x-custom-message")),
        Some(&Value::String(String::from("message-value")))
    );
    assert_eq!(
        annotations.get(&AnnotationKey::from(800_u64)),
        Some(&Value::Int(801))
    );
    assert_eq!(
        annotations.get(&AnnotationKey::from("x-opt-sequence-number")),
        Some(&Value::Long(1))
    );
    assert_eq!(
        annotations.get(&AnnotationKey::from("x-opt-enqueue-sequence-number")),
        Some(&Value::Long(1))
    );
    assert!(matches!(
        annotations.get(&AnnotationKey::from("x-opt-enqueued-time")),
        Some(Value::Timestamp(value)) if value.milliseconds() == 1_000
    ));
    assert!(matches!(
        annotations.get(&AnnotationKey::from("x-opt-locked-until")),
        Some(Value::Timestamp(value)) if value.milliseconds() == 61_000
    ));
    assert_eq!(
        annotations.get(&AnnotationKey::from("x-opt-deadletter-source")),
        None,
        "a sender forged the broker-owned dead-letter source"
    );

    let actual_properties = actual.properties.as_ref().expect("properties survive");
    let sent_properties = sent.properties.as_ref().expect("properties were sent");
    assert_eq!(actual_properties.message_id, sent_properties.message_id);
    assert_eq!(actual_properties.user_id, sent_properties.user_id);
    assert_eq!(actual_properties.to, sent_properties.to);
    assert_eq!(actual_properties.subject, sent_properties.subject);
    assert_eq!(actual_properties.reply_to, sent_properties.reply_to);
    assert_eq!(
        actual_properties.correlation_id,
        sent_properties.correlation_id
    );
    assert_eq!(actual_properties.content_type, sent_properties.content_type);
    assert_eq!(
        actual_properties.content_encoding,
        sent_properties.content_encoding
    );
    assert_eq!(actual_properties.absolute_expiry_time, Some(6_000));
    assert_eq!(actual_properties.creation_time, Some(-123));
    assert_eq!(actual_properties.group_sequence, Some(7));
    assert_eq!(
        actual_properties.reply_to_group_id.as_deref(),
        Some("reply-group")
    );

    assert_eq!(
        actual.application_properties, sent.application_properties,
        "application properties changed"
    );
    assert_eq!(actual.body, sent.body, "body sections were collapsed");
    assert_eq!(actual.footer, sent.footer);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_modified_message_comes_round_again() -> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", QueueConfig::default()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    sender
        .send(Message::builder().body(body("retry-me")).build())
        .await?;

    let mut receiver = Receiver::attach(&mut session, "test-receiver", "orders").await?;
    let first = receiver.recv().await?;
    // This is the outcome Azure.Messaging.ServiceBus sends for an abandon with
    // no properties to modify.
    receiver.modify(&first, Modified::default()).await?;

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
    let request = Message::builder()
        .properties(Properties {
            message_id: Some(message_id.to_owned().into()),
            reply_to: Some(reply_to.to_owned()),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert(protocol_amqp::OPERATION_PROPERTY, operation)
                .insert(protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY, link_name)
                .build(),
        )
        .body(Body::Value(Value::Map(body)))
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
    let message = response.message().clone();
    responses.accept(&response).await?;
    Ok(message)
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

    // The second claimant's attach completes, then the link is refused: its
    // first receive reports the session as held.
    let mut rival = Receiver::builder()
        .name("rival")
        .source(session_source("orders", Some("cart-1")))
        .attach(&mut session)
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
async fn a_service_bus_rejection_preserves_custom_dead_letter_metadata()
-> Result<(), Box<dyn Error>> {
    let node = Node::start("orders", QueueConfig::default()).await?;

    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;
    sender
        .send(
            Message::builder()
                .properties(Properties {
                    // Absolute expiry is broker-owned. With no TTL configured,
                    // this forged sender value must be cleared on every receive.
                    absolute_expiry_time: Some(i64::MAX),
                    ..Properties::default()
                })
                .application_properties(
                    ApplicationProperties::builder()
                        .insert("custom-trace-id", "trace-42")
                        .insert(protocol_amqp::DEAD_LETTER_REASON_PROPERTY, "forged")
                        .insert(protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY, "forged")
                        .build(),
                )
                .body(body("poison"))
                .build(),
        )
        .await?;

    let mut receiver = Receiver::attach(&mut session, "test-receiver", "orders").await?;
    let delivery = receiver.recv().await?;
    assert_eq!(
        delivery
            .message()
            .properties
            .as_ref()
            .and_then(|properties| properties.absolute_expiry_time),
        None,
        "a sender forged an expiry the broker did not record"
    );
    let live_properties = delivery
        .message()
        .application_properties
        .as_ref()
        .expect("the custom application property survives");
    assert_eq!(
        live_properties.0.get("custom-trace-id"),
        Some(&Value::String(String::from("trace-42")))
    );
    assert_eq!(
        live_properties
            .0
            .get(protocol_amqp::DEAD_LETTER_REASON_PROPERTY),
        None,
        "a sender forged a dead-letter reason on a live message"
    );
    assert_eq!(
        live_properties
            .0
            .get(protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY),
        None,
        "a sender forged a dead-letter description on a live message"
    );
    // Azure.Messaging.ServiceBus puts custom dead-letter metadata in the info
    // map of a rejection carrying this vendor condition.
    let mut info = Fields::new();
    info.insert(
        Symbol::from(protocol_amqp::DEAD_LETTER_REASON_PROPERTY),
        Value::String(String::from("InvalidOrder")),
    );
    info.insert(
        Symbol::from(protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY),
        Value::String(String::from("the order total is negative")),
    );
    receiver
        .reject(
            &delivery,
            Some(AmqpError {
                condition: ErrorCondition::Custom(Symbol::from("com.microsoft:dead-letter")),
                description: None,
                info: Some(info),
            }),
        )
        .await?;

    let mut dead_letter_receiver =
        Receiver::attach(&mut session, "dlq-receiver", "orders/$deadletterqueue").await?;
    let poisoned = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        dead_letter_receiver.recv(),
    )
    .await??;
    assert_eq!(text_of(poisoned.message()), "poison");
    assert_eq!(
        poisoned
            .message()
            .properties
            .as_ref()
            .and_then(|properties| properties.absolute_expiry_time),
        None,
        "dead-lettering restored a stale sender expiry"
    );
    assert_eq!(
        poisoned
            .message()
            .message_annotations
            .as_ref()
            .and_then(|annotations| {
                annotations.get(&AnnotationKey::from("x-opt-deadletter-source"))
            }),
        Some(&Value::String(String::from("orders")))
    );
    let properties = poisoned
        .message()
        .application_properties
        .as_ref()
        .expect("a dead-lettered message carries its metadata");
    assert_eq!(
        properties.0.get(protocol_amqp::DEAD_LETTER_REASON_PROPERTY),
        Some(&Value::String(String::from("InvalidOrder")))
    );
    assert_eq!(
        properties
            .0
            .get(protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY),
        Some(&Value::String(String::from("the order total is negative")))
    );
    assert_eq!(
        properties.0.get("custom-trace-id"),
        Some(&Value::String(String::from("trace-42"))),
        "dead-letter metadata replaced a custom application property"
    );
    dead_letter_receiver.accept(&poisoned).await?;

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
