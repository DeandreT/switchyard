//! Topic publications cross real sockets and settle independently in each subscription.

use std::{error::Error, time::Duration};

use amqp::{
    ApplicationProperties, Body, ClientConnection, ClientDelivery, ClientReceiver, ClientSender,
    ClientSession, Header, Message, MessageId, OrderedMap, Outcome, Properties, Symbol, Value,
    encode_message,
};
use domain::{
    Command, CommandKind, CommandOutcome, EntityPath, NamespaceName, QueueCounters, SequenceNumber,
    StateMachine, SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use server::{Broker, LocalProposer, ManualClock};
use storage::{StateStore, StoreSnapshot};
use testkit::StoreProvider;
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(8);

struct Node<P: StoreProvider> {
    broker: Option<Broker>,
    store: Option<P::Store>,
    provider: P,
    clock: ManualClock,
    namespace: NamespaceName,
    topic: EntityPath,
    address: String,
    listeners: Vec<JoinHandle<()>>,
}

impl<P: StoreProvider> Node<P> {
    async fn start(provider: P, topic: &str, config: TopicConfig) -> TestResult<Self> {
        let store = provider.open()?;
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new(topic)?;
        StateMachine::new(store.clone()).apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::from_millis(1_000),
            CommandKind::CreateTopic { config },
        ))?;
        let mut node = Self {
            broker: None,
            store: Some(store),
            provider,
            clock: ManualClock::at(1_000),
            namespace,
            topic,
            address: String::new(),
            listeners: Vec::new(),
        };
        node.spawn().await?;
        Ok(node)
    }

    async fn spawn(&mut self) -> TestResult {
        self.broker = Some(Broker::spawn(LocalProposer::new(
            StateMachine::new(self.store.as_ref().expect("open store").clone()),
            self.clock.clone(),
        )));
        self.address = self.listen(self.namespace.clone()).await?;
        Ok(())
    }

    async fn listen(&mut self, namespace: NamespaceName) -> TestResult<String> {
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?.to_string();
        let handle = self.broker.as_ref().expect("broker").handle();
        self.listeners.push(tokio::spawn(async move {
            let _ = protocol_amqp::AmqpListener::new(handle, namespace)
                .serve(socket)
                .await;
        }));
        Ok(address)
    }

    async fn restart(&mut self) -> TestResult {
        for listener in self.listeners.drain(..) {
            listener.abort();
            let _ = listener.await;
        }
        drop(self.broker.take());
        drop(self.store.take());
        self.store = Some(self.provider.open()?);
        self.spawn().await
    }

    async fn connect(&self) -> TestResult<ClientConnection> {
        connect(&self.address).await
    }

    async fn submit_in(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        kind: CommandKind,
    ) -> TestResult<CommandOutcome> {
        Ok(timeout(
            DEADLINE,
            self.broker.as_ref().expect("broker").handle().submit(
                namespace.clone(),
                entity.clone(),
                kind,
            ),
        )
        .await??)
    }

    async fn submit(&self, entity: &EntityPath, kind: CommandKind) -> TestResult<CommandOutcome> {
        self.submit_in(&self.namespace, entity, kind).await
    }

    async fn subscription(&self, name: &str) -> TestResult<EntityPath> {
        let name = SubscriptionName::new(name)?;
        self.submit(
            &self.topic,
            CommandKind::CreateSubscription {
                name: name.clone(),
                config: SubscriptionConfig::default(),
            },
        )
        .await?;
        Ok(self.topic.subscription(&name)?)
    }

    async fn peek(&self, entity: &EntityPath) -> TestResult<Vec<domain::Delivery>> {
        let CommandOutcome::Peeked(messages) = self
            .submit(
                entity,
                CommandKind::Peek {
                    from_sequence: SequenceNumber::new(0),
                    max_messages: 32,
                    session_id: None,
                },
            )
            .await?
        else {
            panic!("peek outcome")
        };
        Ok(messages)
    }

    async fn wait_len(&self, entity: &EntityPath, expected: usize) -> TestResult {
        timeout(DEADLINE, async {
            loop {
                if self.peek(entity).await?.len() == expected {
                    return Ok::<(), Box<dyn Error>>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        Ok(())
    }

    fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.store.as_ref().expect("store").snapshot()?)
    }
}

impl<P: StoreProvider> Drop for Node<P> {
    fn drop(&mut self) {
        for listener in self.listeners.drain(..) {
            listener.abort();
        }
    }
}

async fn connect(address: &str) -> TestResult<ClientConnection> {
    Ok(timeout(
        DEADLINE,
        ClientConnection::builder()
            .container_id("topic-client")
            .open(&format!("amqp://{address}")),
    )
    .await??)
}

async fn recv(receiver: &mut ClientReceiver) -> TestResult<ClientDelivery> {
    Ok(timeout(DEADLINE, receiver.recv()).await??)
}

fn rich(index: usize) -> Message {
    let mut message = Message {
        header: Some(Header {
            durable: true,
            priority: 7,
            ttl: Some(50_000),
            ..Header::default()
        }),
        properties: Some(Properties {
            message_id: Some(format!("member-{index}").into()),
            correlation_id: Some(MessageId::Ulong(100 + index as u64)),
            subject: Some(format!("subject-{index}")),
            content_type: Some(Symbol::from("application/octet-stream")),
            content_encoding: Some(Symbol::from("utf-8")),
            ..Properties::default()
        }),
        application_properties: Some(
            ApplicationProperties::builder()
                .insert("member", index as i32)
                .insert("nullable", Value::Null)
                .build(),
        ),
        body: Body::Data(vec![
            format!("body-{index}").into_bytes().into(),
            vec![0, index as u8, 255].into(),
        ]),
        ..Message::default()
    };
    message.message_annotations = Some(
        [(Symbol::from("producer-index"), Value::Int(index as i32))]
            .into_iter()
            .collect::<OrderedMap<_, _>>()
            .into(),
    );
    message.footer = Some(
        [(
            Symbol::from("checksum"),
            Value::String(format!("sum-{index}")),
        )]
        .into_iter()
        .collect::<OrderedMap<_, _>>()
        .into(),
    );
    message
}

fn sequence(message: &Message) -> i64 {
    match message
        .message_annotations
        .as_ref()
        .and_then(|annotations| annotations.get(Symbol::from("x-opt-sequence-number")))
    {
        Some(Value::Long(sequence)) => *sequence,
        other => panic!("sequence annotation: {other:?}"),
    }
}

fn content(actual: &Message, expected: &Message) {
    assert_eq!(actual.body, expected.body);
    let mut properties = expected.properties.clone().expect("producer properties");
    properties.creation_time = Some(1_000);
    properties.absolute_expiry_time = Some(51_000);
    assert_eq!(actual.properties.as_ref(), Some(&properties));
    assert_eq!(
        actual.application_properties,
        expected.application_properties
    );
    assert_eq!(actual.footer, expected.footer);
    assert_eq!(
        actual
            .message_annotations
            .as_ref()
            .and_then(|annotations| annotations.get(Symbol::from("producer-index"))),
        expected
            .message_annotations
            .as_ref()
            .and_then(|annotations| annotations.get(Symbol::from("producer-index")))
    );
    assert_eq!(actual.header.as_ref().expect("header").ttl, Some(50_000));
}

fn accepted(outcome: Outcome) {
    assert!(matches!(outcome, Outcome::Accepted(_)), "{outcome:?}");
}

async fn fanout_wakes_waiting_receivers_and_settles_independent_rich_copies<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = Node::start(provider, "orders", TopicConfig::default()).await?;
    let alpha = node.subscription("Alpha").await?;
    let beta = node.subscription("beta").await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut sender = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "topic-publisher", "orders"),
    )
    .await??;
    let mut first = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "alpha-reader", alpha.as_str()),
    )
    .await??;
    let mut second = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "beta-reader", beta.as_str()),
    )
    .await??;
    let message = rich(0);
    let (outcome, a, b) = tokio::try_join!(
        async { Ok::<_, Box<dyn Error>>(timeout(DEADLINE, sender.send(message.clone())).await??) },
        recv(&mut first),
        recv(&mut second)
    )?;
    accepted(outcome);
    content(a.message(), &message);
    content(b.message(), &message);
    assert_eq!(sequence(a.message()), 1);
    assert_eq!(sequence(b.message()), 1);
    timeout(DEADLINE, first.accept(&a)).await??;
    node.wait_len(&alpha, 0).await?;
    assert_eq!(node.peek(&beta).await?.len(), 1);
    timeout(DEADLINE, second.accept(&b)).await??;
    node.wait_len(&beta, 0).await?;
    timeout(DEADLINE, first.close()).await??;
    timeout(DEADLINE, second.close()).await??;
    accepted(timeout(DEADLINE, sender.send(rich(1))).await??);
    timeout(DEADLINE, connection.close()).await??;
    let before = node.snapshot()?;
    node.restart().await?;
    assert_eq!(node.snapshot()?, before);
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    for (name, entity) in [("alpha-reopened", &alpha), ("beta-reopened", &beta)] {
        let mut receiver = timeout(
            DEADLINE,
            ClientReceiver::attach(&mut session, name, entity.as_str()),
        )
        .await??;
        let delivery = recv(&mut receiver).await?;
        content(delivery.message(), &rich(1));
        assert_eq!(sequence(delivery.message()), 2);
        timeout(DEADLINE, receiver.accept(&delivery)).await??;
        node.wait_len(entity, 0).await?;
    }
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn a_subscription_dead_letters_only_its_own_copy_and_drains_the_reason<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, "Orders", TopicConfig::default()).await?;
    let alpha = node.subscription("Alpha").await?;
    let beta = node.subscription("beta").await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut sender = ClientSender::attach(&mut session, "publisher", "Orders").await?;
    accepted(sender.send(rich(0)).await?);
    let mut first =
        ClientReceiver::attach(&mut session, "alpha", "Orders/SuBsCrIpTiOnS/Alpha").await?;
    let mut second = ClientReceiver::attach(&mut session, "beta", beta.as_str()).await?;
    let a = recv(&mut first).await?;
    let b = recv(&mut second).await?;
    first.reject(&a, None).await?;
    node.wait_len(&alpha, 0).await?;
    assert_eq!(node.peek(&beta).await?.len(), 1);
    let peeked = management::peek_one(
        &mut session,
        "Orders/SUBSCRIPTIONS/Alpha/$DeadLetterQueue/$MANAGEMENT",
    )
    .await?;
    assert_eq!(sequence(&peeked), 1);
    assert_eq!(peeked.body, rich(0).body);
    let mut dlq = ClientReceiver::attach(
        &mut session,
        "alpha-dlq",
        "Orders/SUBSCRIPTIONS/Alpha/$DeadLetterQueue",
    )
    .await?;
    let dead = recv(&mut dlq).await?;
    assert_eq!(sequence(dead.message()), 1);
    assert_eq!(dead.message().body, rich(0).body);
    assert_eq!(
        dead.message()
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get("DeadLetterReason")),
        Some(&Value::String("RejectedByReceiver".into()))
    );
    dlq.accept(&dead).await?;
    second.accept(&b).await?;
    node.wait_len(&alpha.dead_letter_queue()?, 0).await?;
    node.wait_len(&beta, 0).await?;
    assert!(node.peek(&beta.dead_letter_queue()?).await?.is_empty());
    connection.close().await?;
    Ok(())
}

#[path = "amqp_topics/batch.rs"]
mod batch;
#[path = "amqp_topics/management.rs"]
mod management;

#[path = "amqp_topics/routing.rs"]
mod routing;
#[path = "amqp_topics/scheduling.rs"]
mod scheduling;

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 8, super::$case(::testkit::MemoryProvider::new())).await? })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 8, super::$case(::testkit::DurableProvider::temporary()?)).await? })+ }
    };
}

for_each_backend! {
    fanout_wakes_waiting_receivers_and_settles_independent_rich_copies,
    a_subscription_dead_letters_only_its_own_copy_and_drains_the_reason,
}
