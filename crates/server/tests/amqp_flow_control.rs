//! Flow control through real sockets, the broker owner, and both stores.

use std::{error::Error, time::Duration};

use amqp::{
    Body, ClientConnection as Connection, ClientReceiver as Receiver, ClientSender as Sender,
    ClientSession as Session, Message, Outcome, SenderSettleMode,
};
use domain::{
    CommandKind, CommandOutcome, EntityPath, NamespaceName, QueueConfig, SequenceNumber,
    StateMachine,
};
use server::{Broker, LocalProposer, ManualClock};
use testkit::StoreProvider;
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(10);

struct Node<P> {
    broker: Broker,
    namespace: NamespaceName,
    address: String,
    listener: JoinHandle<()>,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    async fn start(provider: P, config: QueueConfig) -> TestResult<Self> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(provider.open()?),
            ManualClock::at(1_000),
        ));
        let namespace = NamespaceName::new("tenant")?;
        for name in ["orders", "other"] {
            broker
                .handle()
                .submit(
                    namespace.clone(),
                    EntityPath::new(name)?,
                    CommandKind::CreateQueue { config },
                )
                .await?;
        }
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?.to_string();
        let handle = broker.handle();
        let listener_namespace = namespace.clone();
        let listener = tokio::spawn(async move {
            let _ = protocol_amqp::AmqpListener::new(handle, listener_namespace)
                .serve(socket)
                .await;
        });
        Ok(Self {
            broker,
            namespace,
            address,
            listener,
            _provider: provider,
        })
    }

    async fn connect(&self, maximum_frame_size: u32) -> TestResult<Connection> {
        Ok(Connection::builder()
            .container_id("flow-client")
            .max_frame_size(maximum_frame_size)
            .open(&format!("amqp://{}", self.address))
            .await?)
    }

    async fn seed(&self, queue: &str, id: &str, body: Vec<u8>) -> TestResult {
        let outcome = self
            .broker
            .handle()
            .submit(
                self.namespace.clone(),
                EntityPath::new(queue)?,
                CommandKind::Send {
                    message_id: id.to_owned(),
                    body,
                    time_to_live_millis: None,
                    session_id: None,
                },
            )
            .await?;
        assert!(matches!(outcome, CommandOutcome::Sent { .. }));
        Ok(())
    }

    async fn head(&self, queue: &str) -> TestResult<Option<u64>> {
        let CommandOutcome::Peeked(messages) = self
            .broker
            .handle()
            .submit(
                self.namespace.clone(),
                EntityPath::new(queue)?,
                CommandKind::Peek {
                    from_sequence: SequenceNumber::new(1),
                    max_messages: 1,
                    session_id: None,
                },
            )
            .await?
        else {
            panic!("peek returns the next retained message");
        };
        Ok(messages.first().map(|message| message.sequence.as_u64()))
    }

    async fn wait_empty(&self, queue: &str) -> TestResult {
        timeout(DEADLINE, async {
            while self.head(queue).await?.is_some() {
                tokio::task::yield_now().await;
            }
            Ok::<(), Box<dyn Error>>(())
        })
        .await
        .expect("settlement commits before the queue is observed empty")?;
        Ok(())
    }
}

impl<P> Drop for Node<P> {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

fn message(bytes: Vec<u8>) -> Message {
    Message::builder()
        .body(Body::Data(vec![bytes.into()]))
        .build()
}

fn bytes_of(message: &Message) -> Vec<u8> {
    match &message.body {
        Body::Data(sections) => sections
            .iter()
            .flat_map(|section| section.iter().copied())
            .collect(),
        body => panic!("expected data body, got {body:?}"),
    }
}

async fn long_lived_links<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, QueueConfig::default()).await?;
    let mut connection = node.connect(262_144).await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "long-lived-sender", "orders").await?;
    let mut receiver = Receiver::attach(&mut session, "long-lived-receiver", "orders").await?;
    timeout(Duration::from_secs(120), async {
        for index in 0_u32..2_100 {
            let body = index.to_be_bytes().to_vec();
            assert!(matches!(
                timeout(DEADLINE, sender.send(message(body.clone())))
                    .await
                    .unwrap_or_else(|_| panic!("send stalled at message {index}"))?,
                Outcome::Accepted(_)
            ));
            let delivery = timeout(DEADLINE, receiver.recv())
                .await
                .unwrap_or_else(|_| panic!("receive stalled at message {index}"))?;
            assert_eq!(bytes_of(delivery.message()), body);
            timeout(DEADLINE, receiver.accept(&delivery))
                .await
                .unwrap_or_else(|_| panic!("settlement stalled at message {index}"))?;
        }
        Ok::<(), Box<dyn Error>>(())
    })
    .await
    .expect("both link and session credit continue beyond 2048 messages")?;
    node.wait_empty("orders").await?;
    sender.close().await?;
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    Ok(())
}

async fn paused_receiver<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, QueueConfig::default()).await?;
    for index in 1_u32..=96 {
        node.seed(
            "orders",
            &format!("queued-{index}"),
            index.to_be_bytes().to_vec(),
        )
        .await?;
    }
    let mut connection = node.connect(262_144).await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut paused = Receiver::builder()
        .name("paused-receiver")
        .source("orders")
        .sender_settle_mode(SenderSettleMode::Settled)
        .attach(&mut session)
        .await?;

    // The edge acquires the next receive-delete message before awaiting link
    // credit. Seeing sequence 34 (or later) proves the 32 delivery slots filled.
    timeout(DEADLINE, async {
        loop {
            if node.head("orders").await?.is_none_or(|head| head >= 34) {
                break;
            }
            tokio::task::yield_now().await;
        }
        Ok::<(), Box<dyn Error>>(())
    })
    .await
    .expect("paused delivery queue fills without an arbitrary sleep")?;

    timeout(DEADLINE, async {
        let mut sender = Sender::attach(&mut session, "other-sender", "other").await?;
        let mut receiver = Receiver::attach(&mut session, "other-receiver", "other").await?;
        let body = b"another link stays responsive".to_vec();
        assert!(matches!(
            sender.send(message(body.clone())).await?,
            Outcome::Accepted(_)
        ));
        let delivery = receiver.recv().await?;
        assert_eq!(bytes_of(delivery.message()), body);
        receiver.accept(&delivery).await?;
        node.wait_empty("other").await?;
        sender.close().await?;
        receiver.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
    .await
    .expect("a paused receiver cannot block unrelated attach, transfer, or settlement")?;

    timeout(DEADLINE, async {
        for index in 1_u32..=96 {
            let delivery = paused.recv().await?;
            assert_eq!(bytes_of(delivery.message()), index.to_be_bytes());
        }
        Ok::<(), Box<dyn Error>>(())
    })
    .await
    .expect("taking messages replenishes the paused receiver's credit")?;
    node.wait_empty("orders").await?;
    paused.close().await?;
    session.end().await?;
    connection.close().await?;
    Ok(())
}

async fn large_fragmented_delivery<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(
        provider,
        QueueConfig {
            max_message_bytes: 2 * 1024 * 1024,
            ..QueueConfig::default()
        },
    )
    .await?;
    let body: Vec<u8> = (0..1_100_000).map(|index| (index % 251) as u8).collect();
    node.seed("orders", "fragmented-message", body.clone())
        .await?;
    let mut connection = node.connect(512).await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut receiver = Receiver::attach(&mut session, "fragmented-receiver", "orders").await?;
    let delivery = timeout(DEADLINE, receiver.recv())
        .await
        .expect("one delivery completes across more than 2048 fragments")?;
    assert_eq!(bytes_of(delivery.message()), body);
    receiver.accept(&delivery).await?;
    node.wait_empty("orders").await?;
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    Ok(())
}

macro_rules! suite {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;

            #[tokio::test]
            async fn links_and_session_windows_replenish_beyond_2048_messages() -> TestResult {
                long_lived_links($provider).await
            }

            #[tokio::test]
            async fn a_paused_receiver_does_not_block_other_links_and_can_resume() -> TestResult {
                paused_receiver($provider).await
            }

            #[tokio::test]
            async fn one_delivery_can_span_more_than_one_session_window() -> TestResult {
                large_fragmented_delivery($provider).await
            }
        }
    };
}

suite!(memory, testkit::MemoryProvider::new());
suite!(durable, testkit::DurableProvider::temporary()?);
