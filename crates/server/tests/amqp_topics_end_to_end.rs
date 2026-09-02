//! Immediate match-all topic fanout over a real AMQP socket.

use std::{error::Error, io, time::Duration};

use amqp::{
    AnnotationKey, ApplicationProperties, Body, ClientConnection as Connection,
    ClientReceiver as Receiver, ClientSender as Sender, ClientSession as Session,
    DeliveryAnnotations, Footer, Header, Message, MessageAnnotations, Outcome, Properties, Symbol,
    Value, encode_message,
};
use domain::{
    CommandKind, CommandOutcome, StateMachine, SubscriptionConfig, SubscriptionName, TopicConfig,
};
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::net::TcpListener;

const TOPIC: &str = "events";
const ACCOUNTING: &str = "events/Subscriptions/accounting";
const ANALYTICS: &str = "events/Subscriptions/analytics";

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
        let topic = domain::EntityPath::new(TOPIC)?;
        assert_eq!(
            broker.handle().submit_blocking(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
            )?,
            CommandOutcome::TopicCreated
        );
        for name in ["accounting", "analytics"] {
            let outcome = broker.handle().submit_blocking(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(name)?,
                    config: SubscriptionConfig::default(),
                },
            )?;
            assert!(
                matches!(outcome, CommandOutcome::SubscriptionCreated { .. }),
                "subscription creation produced {outcome:?}"
            );
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

    async fn connect(&self, container_id: &str) -> Result<Connection, Box<dyn Error>> {
        Ok(Connection::builder()
            .container_id(container_id)
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
async fn a_publish_wakes_both_subscribers_and_their_settlement_is_independent()
-> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let mut connection = node.connect("topic-wakeup-client").await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut accounting = Receiver::attach(&mut session, "accounting-waiter", ACCOUNTING).await?;
    let mut analytics = Receiver::attach(&mut session, "analytics-waiter", ANALYTICS).await?;

    // Both receives are active while the topic is empty. A publish must notify
    // each concrete subscription instead of waiting for the three-second
    // safety-net poll or notifying only the non-receivable topic path.
    let accounting_waiter = tokio::spawn(async move {
        let first = accounting.recv().await.map_err(|error| error.to_string())?;
        let received_at = std::time::Instant::now();
        let text = text_of(first.message());
        accounting
            .accept(&first)
            .await
            .map_err(|error| error.to_string())?;
        if tokio::time::timeout(Duration::from_millis(250), accounting.recv())
            .await
            .is_ok()
        {
            return Err(String::from("the completed accounting copy came back"));
        }
        Ok::<_, String>((received_at, text))
    });
    let analytics_waiter = tokio::spawn(async move {
        let first = analytics.recv().await.map_err(|error| error.to_string())?;
        let received_at = std::time::Instant::now();
        let first_text = text_of(first.message());
        analytics
            .release(&first)
            .await
            .map_err(|error| error.to_string())?;
        let second = tokio::time::timeout(Duration::from_secs(1), analytics.recv())
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        let second_text = text_of(second.message());
        let delivery_count = second
            .message()
            .header
            .as_ref()
            .map(|header| header.delivery_count);
        analytics
            .accept(&second)
            .await
            .map_err(|error| error.to_string())?;
        Ok::<_, String>((received_at, first_text, second_text, delivery_count))
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started = std::time::Instant::now();
    let mut sender = Sender::attach(&mut session, "topic-sender", TOPIC).await?;
    assert!(matches!(
        sender
            .send(Message::builder().body(body("invoice-1")).build())
            .await?,
        Outcome::Accepted(_)
    ));

    let accounting_result = tokio::time::timeout(Duration::from_secs(2), accounting_waiter)
        .await??
        .map_err(|error| -> Box<dyn Error> { error.into() })?;
    let analytics_result = tokio::time::timeout(Duration::from_secs(2), analytics_waiter)
        .await??
        .map_err(|error| -> Box<dyn Error> { error.into() })?;
    assert_eq!(accounting_result.1, "invoice-1");
    assert_eq!(analytics_result.1, "invoice-1");
    assert_eq!(analytics_result.2, "invoice-1");
    assert_eq!(analytics_result.3, Some(1));
    for received_at in [accounting_result.0, analytics_result.0] {
        assert!(
            received_at.duration_since(started) < Duration::from_secs(2),
            "a subscription waited for the fallback poll"
        );
    }

    connection.close().await?;
    Ok(())
}

fn rich_child(index: usize) -> Message {
    let mut delivery_annotations = DeliveryAnnotations::default();
    delivery_annotations.insert("x-topic-delivery", format!("delivery-{index}"));
    let mut message_annotations = MessageAnnotations::default();
    message_annotations.insert("x-topic-message", format!("message-{index}"));
    message_annotations.insert("x-opt-sequence-number", 999_i64);
    let mut footer = Footer::default();
    footer.insert("x-topic-footer", format!("footer-{index}"));

    Message::builder()
        .header(Header {
            durable: true,
            priority: 7,
            ttl: None,
            first_acquirer: true,
            delivery_count: 99,
        })
        .delivery_annotations(delivery_annotations)
        .message_annotations(message_annotations)
        .properties(Properties {
            message_id: Some(format!("topic-batch-{index}").into()),
            correlation_id: Some(format!("correlation-{index}").into()),
            subject: Some(String::from("topic.child")),
            content_type: Some(Symbol::from("application/octet-stream")),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert("batch-index", i32::try_from(index).expect("small index"))
                .insert("preserved", format!("property-{index}"))
                .build(),
        )
        .body(Body::Data(vec![
            format!("child-{index}-part-one-").into_bytes().into(),
            b"part-two".to_vec().into(),
        ]))
        .footer(footer)
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

async fn receive_batch(
    receiver: &mut Receiver,
    count: usize,
) -> Result<Vec<Message>, Box<dyn Error>> {
    let mut messages = Vec::with_capacity(count);
    for _ in 0..count {
        let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
        messages.push(delivery.message().clone());
        receiver.accept(&delivery).await?;
    }
    Ok(messages)
}

fn assert_rich_copy(actual: &Message, expected: &Message, sequence: i64) {
    let actual_header = actual.header.as_ref().expect("the header survives fanout");
    let expected_header = expected.header.as_ref().expect("the child has a header");
    assert_eq!(actual_header.durable, expected_header.durable);
    assert_eq!(actual_header.priority, expected_header.priority);
    assert_eq!(actual_header.first_acquirer, expected_header.first_acquirer);
    assert_eq!(actual_header.delivery_count, 0);

    let index = usize::try_from(sequence - 1).expect("positive test sequence");
    let delivery_annotations = actual
        .delivery_annotations
        .as_ref()
        .expect("delivery annotations survive fanout");
    assert_eq!(
        delivery_annotations.get(&AnnotationKey::from("x-topic-delivery")),
        Some(&Value::String(format!("delivery-{index}")))
    );
    let message_annotations = actual
        .message_annotations
        .as_ref()
        .expect("message annotations survive fanout");
    assert_eq!(
        message_annotations.get(&AnnotationKey::from("x-topic-message")),
        Some(&Value::String(format!("message-{index}")))
    );
    assert_eq!(
        message_annotations.get(&AnnotationKey::from("x-opt-sequence-number")),
        Some(&Value::Long(sequence)),
        "the broker must replace a forged sequence annotation"
    );

    assert_eq!(actual.properties, expected.properties);
    assert_eq!(
        actual.application_properties,
        expected.application_properties
    );
    assert_eq!(actual.body, expected.body);
    assert_eq!(actual.footer, expected.footer);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_service_bus_wire_batch_fans_out_with_each_child_envelope_intact()
-> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let mut connection = node.connect("topic-batch-client").await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "topic-batch-sender", TOPIC).await?;
    let children = [rich_child(0), rich_child(1)];

    assert!(matches!(
        sender
            .send_with_format(
                service_bus_batch(&children)?,
                protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
            )
            .await?,
        Outcome::Accepted(_)
    ));

    let mut accounting = Receiver::attach(&mut session, "accounting-batch", ACCOUNTING).await?;
    let mut analytics = Receiver::attach(&mut session, "analytics-batch", ANALYTICS).await?;
    let accounting_copies = receive_batch(&mut accounting, children.len()).await?;
    let analytics_copies = receive_batch(&mut analytics, children.len()).await?;
    for copies in [&accounting_copies, &analytics_copies] {
        for (index, copy) in copies.iter().enumerate() {
            assert_rich_copy(copy, &children[index], i64::try_from(index + 1)?);
        }
    }

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_dead_letter_queue_is_addressable_without_an_auto_forward_source()
-> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let mut connection = node.connect("topic-dlq-client").await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "topic-poison-sender", TOPIC).await?;
    sender
        .send(Message::builder().body(body("poison")).build())
        .await?;

    let mut receiver = Receiver::attach(&mut session, "subscription-receiver", ACCOUNTING).await?;
    let poison = receiver.recv().await?;
    receiver.reject(&poison, None).await?;

    // Service Bus clients capitalize both well-known path segments. The wire
    // address still resolves to the canonical lowercase shadow key. A direct
    // DLQ drain has no DeadLetterSource; Azure sets that only after forwarding.
    let mut dead_letters = Receiver::attach(
        &mut session,
        "subscription-dlq-receiver",
        "events/Subscriptions/accounting/$DeadLetterQueue",
    )
    .await?;
    let dead_lettered = tokio::time::timeout(Duration::from_secs(2), dead_letters.recv()).await??;
    assert_eq!(text_of(dead_lettered.message()), "poison");
    assert_eq!(
        dead_lettered
            .message()
            .message_annotations
            .as_ref()
            .and_then(|annotations| {
                annotations.get(&AnnotationKey::from("x-opt-deadletter-source"))
            }),
        None
    );
    dead_letters.accept(&dead_lettered).await?;

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn topic_link_roles_are_enforced() -> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;

    let mut send_connection = node.connect("subscription-role-client").await?;
    let mut send_session = Session::begin(&mut send_connection).await?;
    for (name, address) in [
        ("subscription-smuggler", ACCOUNTING),
        (
            "subscription-dlq-smuggler",
            "events/Subscriptions/accounting/$DeadLetterQueue",
        ),
    ] {
        let mut smuggler = Sender::attach(&mut send_session, name, address).await?;
        assert!(
            smuggler
                .send(Message::builder().body(body("smuggled")).build())
                .await
                .is_err(),
            "a sender attached to receive-only address {address}"
        );
    }
    send_connection.close().await?;

    let mut receive_connection = node.connect("topic-role-client").await?;
    let mut receive_session = Session::begin(&mut receive_connection).await?;
    let mut topic_receiver =
        Receiver::attach(&mut receive_session, "topic-receiver", TOPIC).await?;
    let refused = tokio::time::timeout(Duration::from_secs(2), topic_receiver.recv()).await?;
    assert!(
        refused.is_err(),
        "a receiver consumed directly from a topic"
    );

    receive_connection.close().await?;
    Ok(())
}
