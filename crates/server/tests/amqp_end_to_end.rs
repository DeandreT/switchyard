//! A real AMQP client against the real listener.
//!
//! Everything below the socket is the production path: the acceptor, the command
//! bus, the state machine, and a store. Only the store's location and the clock
//! are test-owned.

use std::error::Error;

use domain::{CommandKind, QueueConfig, StateMachine};
use amqp_runtime::{
    Connection, Receiver, Sender, Session,
    connection::ConnectionHandle,
    types::{
        messaging::{Body, Message, Outcome, Properties},
        primitives::Binary,
    },
};
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

    async fn connect(&self) -> Result<ConnectionHandle<()>, Box<dyn Error>> {
        Ok(Connection::builder()
            .container_id("test-client")
            .open(format!("amqp://{}", self.address).as_str())
            .await?)
    }
}

fn body(text: &str) -> Body<Binary> {
    Body::Data(amqp_runtime::types::messaging::Batch::new(vec![
        amqp_runtime::types::messaging::Data(Binary::from(text.as_bytes().to_vec())),
    ]))
}

fn text_of(message: &Message<Body<Binary>>) -> String {
    match &message.body {
        Body::Data(sections) => sections
            .iter()
            .flat_map(|section| section.0.iter().copied())
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
    let delivery = receiver.recv::<Body<Binary>>().await?;
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
    let delivery = receiver.recv::<Body<Binary>>().await?;
    receiver.accept(&delivery).await?;

    // Accepting settles the message, so nothing is left to hand out. The
    // receiver would otherwise sit here until the test timed out.
    let starved = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        receiver.recv::<Body<Binary>>(),
    )
    .await;
    assert!(starved.is_err(), "a settled message came back");

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
    let first = receiver.recv::<Body<Binary>>().await?;
    receiver.release(&first).await?;

    // Releasing abandons the lock, so the message returns to the queue with its
    // delivery count already counted against it.
    let second = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        receiver.recv::<Body<Binary>>(),
    )
    .await??;
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
        .sender_settle_mode(amqp_runtime::types::definitions::SenderSettleMode::Settled)
        .attach(&mut session)
        .await?;
    let delivery = receiver.recv::<Body<Binary>>().await?;
    assert_eq!(text_of(delivery.message()), "fire-and-forget");

    // Never settled by the client, and still gone: at-most-once means the
    // deletion committed before the transfer.
    let starved = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        receiver.recv::<Body<Binary>>(),
    )
    .await;
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
        let delivery = receiver
            .recv::<Body<Binary>>()
            .await
            .map_err(|error| error.to_string())?;
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

fn session_source(queue: &str, session: Option<&str>) -> amqp_runtime::types::messaging::Source {
    use amqp_runtime::types::{
        messaging::FilterSet,
        primitives::{Symbol, Value},
    };
    let mut filter = FilterSet::default();
    filter.insert(
        Symbol::from(protocol_amqp::SESSION_FILTER),
        session.map_or(Value::Null, |id| Value::String(id.to_owned())),
    );
    amqp_runtime::types::messaging::Source::builder()
        .address(queue)
        .filter(filter)
        .build()
}

fn session_of(source: &Option<amqp_runtime::types::messaging::Source>) -> Option<String> {
    use amqp_runtime::types::primitives::{Symbol, Value};
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

fn with_session(text: &str, session: &str) -> Message<Body<Binary>> {
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
        let delivery = receiver.recv::<Body<Binary>>().await?;
        assert_eq!(text_of(delivery.message()), expected);
        receiver.accept(&delivery).await?;
    }
    let starved = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        receiver.recv::<Body<Binary>>(),
    )
    .await;
    assert!(starved.is_err(), "another session's message leaked through");

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

    let delivery = receiver.recv::<Body<Binary>>().await?;
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
    let refused = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        rival.recv::<Body<Binary>>(),
    )
    .await?;
    assert!(refused.is_err(), "a held session was granted twice");

    // Closing the holder releases the session rather than waiting out its lock.
    holder.close().await?;
    let mut next = Receiver::builder()
        .name("next")
        .source(session_source("orders", Some("cart-1")))
        .attach(&mut session)
        .await?;
    let waiting = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        next.recv::<Body<Binary>>(),
    )
    .await;
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
    let delivery = receiver.recv::<Body<Binary>>().await?;
    receiver.reject(&delivery, None).await?;

    // The dead-letter queue is addressed as a sub-queue and drained like one.
    let mut dead_letter_receiver =
        Receiver::attach(&mut session, "dlq-receiver", "orders/$deadletterqueue").await?;
    let poisoned = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        dead_letter_receiver.recv::<Body<Binary>>(),
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
        Some(amqp_runtime::types::primitives::SimpleValue::String(
            String::from("RejectedByReceiver")
        ))
    );
    dead_letter_receiver.accept(&poisoned).await?;

    // Completing in the dead-letter queue removes the message permanently.
    let drained = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        dead_letter_receiver.recv::<Body<Binary>>(),
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
