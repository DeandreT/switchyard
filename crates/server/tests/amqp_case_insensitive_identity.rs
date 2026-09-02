//! Raw AMQP evidence that entity identities follow Service Bus casing rules.

use std::{error::Error, time::Duration};

use amqp::{
    Body, ClientConnection as Connection, ClientReceiver as Receiver, ClientSender as Sender,
    ClientSession as Session, Message, Outcome,
};
use domain::{
    CommandKind, CommandOutcome, QueueConfig, StateMachine, SubscriptionConfig, SubscriptionName,
    TopicConfig,
};
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::net::TcpListener;

const QUEUE: &str = "Case-Identity-Queue";
const TOPIC: &str = "Case-Identity-Topic";
const SUBSCRIPTION: &str = "Case-Identity-Subscription";

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
        assert_eq!(
            broker.handle().submit_blocking(
                namespace.clone(),
                domain::EntityPath::new(QUEUE)?,
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            )?,
            CommandOutcome::QueueCreated
        );
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
        assert!(matches!(
            broker.handle().submit_blocking(
                namespace.clone(),
                topic,
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(SUBSCRIPTION)?,
                    config: SubscriptionConfig::default(),
                },
            )?,
            CommandOutcome::SubscriptionCreated { .. }
        ));

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
            .container_id("case-insensitive-identity-client")
            .open(format!("amqp://{}", self.address).as_str())
            .await?)
    }
}

fn message(text: &str) -> Message {
    Message::builder()
        .body(Body::Data(vec![text.as_bytes().to_vec().into()]))
        .build()
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
async fn differently_cased_queue_and_topic_subscription_addresses_share_one_identity()
-> Result<(), Box<dyn Error>> {
    let node = Node::start().await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;

    let mut queue_sender = Sender::attach(
        &mut session,
        "mixed-case-queue-sender",
        "cASE-iDENTITY-qUEUE",
    )
    .await?;
    let mut queue_receiver = Receiver::attach(
        &mut session,
        "upper-case-queue-receiver",
        "CASE-IDENTITY-QUEUE",
    )
    .await?;
    assert!(matches!(
        queue_sender.send(message("queue-by-alias")).await?,
        Outcome::Accepted(_)
    ));
    let queued = tokio::time::timeout(Duration::from_secs(2), queue_receiver.recv()).await??;
    assert_eq!(text_of(queued.message()), "queue-by-alias");
    queue_receiver.accept(&queued).await?;

    let mut topic_sender = Sender::attach(
        &mut session,
        "lower-case-topic-sender",
        "case-identity-topic",
    )
    .await?;
    let mut subscription_receiver = Receiver::attach(
        &mut session,
        "mixed-case-subscription-receiver",
        "cASE-iDENTITY-tOPIC/sUBSCRIPTIONS/cASE-iDENTITY-sUBSCRIPTION",
    )
    .await?;
    assert!(matches!(
        topic_sender.send(message("topic-by-alias")).await?,
        Outcome::Accepted(_)
    ));
    let published =
        tokio::time::timeout(Duration::from_secs(2), subscription_receiver.recv()).await??;
    assert_eq!(text_of(published.message()), "topic-by-alias");
    subscription_receiver.accept(&published).await?;

    connection.close().await?;
    Ok(())
}
