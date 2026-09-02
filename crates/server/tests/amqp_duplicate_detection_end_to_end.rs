//! Duplicate detection through the real AMQP listener.

use std::{error::Error, io, time::Duration};

use amqp::{
    AmqpError, Body, ClientConnection as Connection, ClientReceiver as Receiver,
    ClientSender as Sender, ClientSession as Session, ErrorCondition, Message, MessageAnnotations,
    Outcome, Properties, Value, encode_message,
};
use domain::{
    CommandKind, CommandOutcome, Delivery, DeliveryOrigin, MAX_MESSAGE_ID_CHARACTERS, QueueConfig,
    SequenceNumber, StateMachine,
};
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::net::TcpListener;

struct Node {
    broker: Broker,
    address: String,
}

impl Node {
    async fn start(config: QueueConfig) -> Result<Self, Box<dyn Error>> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            ManualClock::at(1_000),
        ));
        let namespace = domain::NamespaceName::new("tenant")?;
        broker.handle().submit_blocking(
            namespace.clone(),
            domain::EntityPath::new("orders")?,
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
        Ok(Self { broker, address })
    }

    async fn connect(&self) -> Result<Connection, Box<dyn Error>> {
        Ok(Connection::builder()
            .container_id("duplicate-detection-test-client")
            .open(format!("amqp://{}", self.address).as_str())
            .await?)
    }

    fn peek(&self) -> Result<Vec<Delivery>, Box<dyn Error>> {
        match self.broker.handle().submit_blocking(
            domain::NamespaceName::new("tenant")?,
            domain::EntityPath::new("orders")?,
            CommandKind::Peek {
                from_sequence: SequenceNumber::new(0),
                max_messages: 250,
                session: None,
            },
        )? {
            CommandOutcome::Peeked(deliveries) => Ok(deliveries),
            other => Err(format!("unexpected peek outcome: {other:?}").into()),
        }
    }

    fn cancel_scheduled(&self, sequence: SequenceNumber) -> Result<(), Box<dyn Error>> {
        let outcome = self.broker.handle().submit_blocking(
            domain::NamespaceName::new("tenant")?,
            domain::EntityPath::new("orders")?,
            CommandKind::CancelScheduled {
                sequences: vec![sequence],
            },
        )?;
        if outcome == (CommandOutcome::ScheduledCancelled { cancelled: 1 }) {
            Ok(())
        } else {
            Err(format!("unexpected cancellation outcome: {outcome:?}").into())
        }
    }
}

fn dedupe_config() -> QueueConfig {
    QueueConfig {
        requires_duplicate_detection: true,
        ..QueueConfig::default()
    }
}

fn message(message_id: Option<&str>, text: &str) -> Message {
    let properties = message_id.map(|message_id| Properties {
        message_id: Some(message_id.to_owned().into()),
        ..Properties::default()
    });
    Message {
        properties,
        body: Body::Data(vec![text.as_bytes().to_vec().into()]),
        ..Message::default()
    }
}

fn scheduled_message(message_id: &str, text: &str, enqueue_at: i64) -> Message {
    let mut message = message(Some(message_id), text);
    let mut annotations = MessageAnnotations::default();
    annotations.insert(
        "x-opt-scheduled-enqueue-time",
        Value::Timestamp(enqueue_at.into()),
    );
    message.message_annotations = Some(annotations);
    message
}

fn service_bus_batch(children: &[Message]) -> Result<Message, io::Error> {
    let sections = children
        .iter()
        .map(encode_message)
        .map(|encoded| encoded.map(Into::into))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Message::builder().body(Body::Data(sections)).build())
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

async fn receive_texts(
    receiver: &mut Receiver,
    count: usize,
) -> Result<Vec<String>, Box<dyn Error>> {
    let mut texts = Vec::with_capacity(count);
    for _ in 0..count {
        let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
        texts.push(text_of(delivery.message()));
        receiver.accept(&delivery).await?;
    }
    Ok(texts)
}

async fn assert_no_delivery(receiver: &mut Receiver) {
    assert!(
        tokio::time::timeout(Duration::from_millis(250), receiver.recv())
            .await
            .is_err(),
        "an unexpected message remained receivable"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_singular_duplicate_is_accepted_but_only_the_first_copy_is_visible()
-> Result<(), Box<dyn Error>> {
    let node = Node::start(dedupe_config()).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "dedupe-sender", "orders").await?;

    assert!(matches!(
        sender.send(message(Some("same-id"), "first-body")).await?,
        Outcome::Accepted(_)
    ));
    assert!(matches!(
        sender.send(message(Some("same-id"), "second-body")).await?,
        Outcome::Accepted(_)
    ));
    assert_eq!(
        node.peek()?
            .iter()
            .map(|delivery| delivery.body.as_slice())
            .collect::<Vec<_>>(),
        vec![b"first-body".as_slice()]
    );

    let mut receiver = Receiver::attach(&mut session, "dedupe-receiver", "orders").await?;
    assert_eq!(receive_texts(&mut receiver, 1).await?, ["first-body"]);
    assert_no_delivery(&mut receiver).await;

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mixed_duplicate_batch_is_accepted_and_stores_only_first_copies()
-> Result<(), Box<dyn Error>> {
    let node = Node::start(dedupe_config()).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "batch-dedupe-sender", "orders").await?;
    let children = [
        message(Some("batch-a"), "batch-a-first"),
        message(Some("batch-a"), "batch-a-second"),
        message(Some("batch-b"), "batch-b-first"),
        message(Some("batch-b"), "batch-b-second"),
    ];

    assert!(matches!(
        sender
            .send_with_format(
                service_bus_batch(&children)?,
                protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
            )
            .await?,
        Outcome::Accepted(_)
    ));
    assert_eq!(
        node.peek()?
            .iter()
            .map(|delivery| String::from_utf8_lossy(&delivery.body).into_owned())
            .collect::<Vec<_>>(),
        ["batch-a-first", "batch-b-first"]
    );

    let mut receiver = Receiver::attach(&mut session, "batch-dedupe-receiver", "orders").await?;
    assert_eq!(
        receive_texts(&mut receiver, 2).await?,
        ["batch-a-first", "batch-b-first"]
    );
    assert_no_delivery(&mut receiver).await;

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn scheduled_and_immediate_sends_suppress_each_other_in_acceptance_order()
-> Result<(), Box<dyn Error>> {
    let node = Node::start(dedupe_config()).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "cross-dedupe-sender", "orders").await?;

    for candidate in [
        scheduled_message("scheduled-first", "scheduled-winner", 10_000),
        message(Some("scheduled-first"), "immediate-loser"),
    ] {
        assert!(matches!(
            sender.send(candidate).await?,
            Outcome::Accepted(_)
        ));
    }
    let scheduled = node.peek()?;
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].body, b"scheduled-winner");
    assert_eq!(scheduled[0].origin, DeliveryOrigin::Scheduled);
    node.cancel_scheduled(scheduled[0].sequence)?;

    for candidate in [
        message(Some("immediate-first"), "immediate-winner"),
        scheduled_message("immediate-first", "scheduled-loser", 10_000),
    ] {
        assert!(matches!(
            sender.send(candidate).await?,
            Outcome::Accepted(_)
        ));
    }
    let immediate = node.peek()?;
    assert_eq!(immediate.len(), 1);
    assert_eq!(immediate[0].body, b"immediate-winner");
    assert_eq!(immediate[0].origin, DeliveryOrigin::Ready);

    let mut receiver = Receiver::attach(&mut session, "cross-dedupe-receiver", "orders").await?;
    assert_eq!(receive_texts(&mut receiver, 1).await?, ["immediate-winner"]);
    assert_no_delivery(&mut receiver).await;
    assert!(node.peek()?.is_empty());

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn overlong_string_ids_are_invalid_but_missing_ids_remain_distinct()
-> Result<(), Box<dyn Error>> {
    let node = Node::start(dedupe_config()).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "id-validation-sender", "orders").await?;

    let overlong = "x".repeat(MAX_MESSAGE_ID_CHARACTERS + 1);
    let Outcome::Rejected(rejected) = sender
        .send(message(Some(&overlong), "must-not-land"))
        .await?
    else {
        panic!("an overlong string message ID was not rejected");
    };
    assert_eq!(
        rejected.error.as_ref().map(|error| &error.condition),
        Some(&ErrorCondition::Amqp(AmqpError::InvalidField))
    );

    assert!(matches!(
        sender.send(message(None, "missing-one")).await?,
        Outcome::Accepted(_)
    ));
    assert!(matches!(
        sender.send(message(None, "missing-two")).await?,
        Outcome::Accepted(_)
    ));
    let mut receiver = Receiver::attach(&mut session, "id-validation-receiver", "orders").await?;
    assert_eq!(
        receive_texts(&mut receiver, 2).await?,
        ["missing-one", "missing-two"]
    );
    assert_no_delivery(&mut receiver).await;

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_queue_without_duplicate_detection_keeps_repeated_ids() -> Result<(), Box<dyn Error>> {
    let node = Node::start(QueueConfig::default()).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "control-sender", "orders").await?;

    assert!(matches!(
        sender
            .send(message(Some("control-id"), "control-first"))
            .await?,
        Outcome::Accepted(_)
    ));
    assert!(matches!(
        sender
            .send(message(Some("control-id"), "control-second"))
            .await?,
        Outcome::Accepted(_)
    ));
    let mut receiver = Receiver::attach(&mut session, "control-receiver", "orders").await?;
    assert_eq!(
        receive_texts(&mut receiver, 2).await?,
        ["control-first", "control-second"]
    );
    assert_no_delivery(&mut receiver).await;

    connection.close().await?;
    Ok(())
}
