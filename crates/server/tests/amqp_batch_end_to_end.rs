//! Service Bus batch transfers and concurrent unsettled deliveries end to end.

use std::{error::Error, io, time::Duration};

use amqp::{
    AnnotationKey, ApplicationProperties, Body, ClientConnection as Connection,
    ClientReceiver as Receiver, ClientSender as Sender, ClientSession as Session, Message,
    MessageAnnotations, Outcome, Properties, Value, encode_message,
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
        Ok(Self {
            _broker: broker,
            address,
        })
    }

    async fn connect(&self) -> Result<Connection, Box<dyn Error>> {
        Ok(Connection::builder()
            .container_id("batch-test-client")
            .open(format!("amqp://{}", self.address).as_str())
            .await?)
    }
}

fn child(index: usize, body_bytes: usize) -> Message {
    let mut application_properties = ApplicationProperties::default();
    application_properties.insert("batch-index", i32::try_from(index).expect("small index"));
    Message::builder()
        .properties(Properties {
            message_id: Some(format!("batch-{index}").into()),
            subject: Some(String::from("batch.child")),
            ..Properties::default()
        })
        .application_properties(application_properties)
        .body(Body::Data(vec![
            vec![b'a' + index as u8; body_bytes].into(),
        ]))
        .build()
}

fn service_bus_batch(children: &[Message]) -> Result<Message, io::Error> {
    let sections = children
        .iter()
        .map(encode_message)
        .map(|encoded| encoded.map(Into::into))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Message::builder().body(Body::Data(sections)).build())
}

fn child_index(message: &Message) -> Option<i32> {
    message
        .application_properties
        .as_ref()?
        .get("batch-index")
        .and_then(|value| match value {
            Value::Int(index) => Some(*index),
            _ => None,
        })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_service_bus_batch_is_durable_and_settles_out_of_order() -> Result<(), Box<dyn Error>> {
    let node = Node::start(QueueConfig::default()).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "batch-sender", "orders").await?;
    let children = [child(0, 3), child(1, 3), child(2, 3)];

    assert!(matches!(
        sender
            .send_with_format(
                service_bus_batch(&children)?,
                protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
            )
            .await?,
        Outcome::Accepted(_)
    ));

    let mut receiver = Receiver::attach(&mut session, "batch-receiver", "orders").await?;
    let mut deliveries = Vec::new();
    for expected in 0..3 {
        let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
        assert_eq!(child_index(delivery.message()), Some(expected));
        assert_eq!(
            delivery
                .message()
                .properties
                .as_ref()
                .and_then(|properties| properties.message_id.clone()),
            Some(format!("batch-{expected}").into())
        );
        deliveries.push(delivery);
    }

    // All three deliveries are outstanding together. Settlement order must be
    // independent of queue order and of the order their outcomes reach the
    // server.
    receiver.accept(&deliveries[2]).await?;
    receiver.accept(&deliveries[0]).await?;
    receiver.accept(&deliveries[1]).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(250), receiver.recv())
            .await
            .is_err(),
        "a completed batch child remained in the queue"
    );

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn one_oversized_child_rejects_the_entire_wire_batch() -> Result<(), Box<dyn Error>> {
    let node = Node::start(QueueConfig {
        max_message_bytes: 256,
        ..QueueConfig::default()
    })
    .await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "batch-sender", "orders").await?;
    let children = [child(0, 3), child(1, 512), child(2, 3)];

    assert!(matches!(
        sender
            .send_with_format(
                service_bus_batch(&children)?,
                protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
            )
            .await?,
        Outcome::Rejected(_)
    ));
    sender.send(child(9, 3)).await?;

    let mut receiver = Receiver::attach(&mut session, "batch-receiver", "orders").await?;
    let only = receiver.recv().await?;
    assert_eq!(child_index(only.message()), Some(9));
    let annotations: &MessageAnnotations = only
        .message()
        .message_annotations
        .as_ref()
        .expect("broker annotations are present");
    assert_eq!(
        annotations.get(&AnnotationKey::from("x-opt-sequence-number")),
        Some(&Value::Long(1)),
        "the rejected batch must consume no sequence numbers"
    );
    receiver.accept(&only).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(250), receiver.recv())
            .await
            .is_err(),
        "a child of the rejected batch became visible"
    );

    connection.close().await?;
    Ok(())
}
