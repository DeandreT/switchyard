//! One producer transfer publishes every batch member or no member at all.

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use amqp::{
    ApplicationProperties, Body, ClientConnection, ClientReceiver, ClientSender, EngineError,
    Header, Message, MessageId, OrderedMap, Outcome, Properties, Symbol, Value, encode_message,
};
use domain::{
    Command, CommandKind, CommandOutcome, EntityPath, MAX_SEQUENCE_NUMBER, MessageStatus,
    NamespaceName, QueueConfig, QueueCounters, SequenceNumber, SessionId, StateMachine, Timestamp,
    codec, keys,
};
use server::{Broker, LocalProposer, ManualClock};
use storage::{Key, StateStore, StorageError, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(8);

#[derive(Clone, Debug)]
struct FailingStore<S> {
    inner: S,
    fail_next: Arc<AtomicBool>,
}

impl<S: StateStore> StateStore for FailingStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<storage::Value>, StorageError> {
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        if self.fail_next.swap(false, Ordering::Relaxed) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected batch commit failure".to_owned(),
            });
        }
        self.inner.apply(batch)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, storage::Value)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }
}

struct Node<P: StoreProvider> {
    broker: Option<Broker>,
    store: Option<P::Store>,
    provider: P,
    clock: ManualClock,
    namespace: NamespaceName,
    entity: EntityPath,
    address: String,
    listener: Option<JoinHandle<()>>,
    fail_next: Arc<AtomicBool>,
}

impl<P: StoreProvider> Node<P> {
    async fn start(
        provider: P,
        config: QueueConfig,
        counters: Option<QueueCounters>,
    ) -> TestResult<Self> {
        let store = provider.open()?;
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        let machine = StateMachine::new(store.clone());
        machine.apply(&Command {
            namespace: namespace.clone(),
            entity: entity.clone(),
            issued_at: Timestamp::from_millis(1_000),
            kind: CommandKind::CreateQueue { config },
        })?;
        if let Some(counters) = counters {
            let mut batch = WriteBatch::default();
            batch.push_put(
                keys::queue_counters(&namespace, &entity),
                codec::encode(&counters)?,
            );
            store.apply(batch)?;
        }
        drop(machine);
        let mut node = Self {
            broker: None,
            store: Some(store),
            provider,
            clock: ManualClock::at(1_000),
            namespace,
            entity,
            address: String::new(),
            listener: None,
            fail_next: Arc::new(AtomicBool::new(false)),
        };
        node.spawn().await?;
        Ok(node)
    }

    async fn spawn(&mut self) -> TestResult {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(FailingStore {
                inner: self.store.as_ref().expect("open store").clone(),
                fail_next: self.fail_next.clone(),
            }),
            self.clock.clone(),
        ));
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        self.address = socket.local_addr()?.to_string();
        let handle = broker.handle();
        let namespace = self.namespace.clone();
        self.listener = Some(tokio::spawn(async move {
            let _ = protocol_amqp::AmqpListener::new(handle, namespace)
                .serve(socket)
                .await;
        }));
        self.broker = Some(broker);
        Ok(())
    }

    async fn restart(&mut self) -> TestResult {
        if let Some(listener) = self.listener.take() {
            listener.abort();
            let _ = listener.await;
        }
        drop(self.broker.take());
        drop(self.store.take());
        self.store = Some(self.provider.open()?);
        self.spawn().await
    }

    async fn connect(&self) -> TestResult<ClientConnection> {
        Ok(timeout(
            DEADLINE,
            ClientConnection::builder()
                .container_id("batch-ingress-client")
                .open(&format!("amqp://{}", self.address)),
        )
        .await??)
    }

    async fn submit(&self, kind: CommandKind) -> TestResult<CommandOutcome> {
        Ok(self
            .broker
            .as_ref()
            .expect("running owner")
            .handle()
            .submit(self.namespace.clone(), self.entity.clone(), kind)
            .await?)
    }

    async fn peek(&self, session_id: Option<SessionId>) -> TestResult<Vec<domain::Delivery>> {
        let CommandOutcome::Peeked(messages) = self
            .submit(CommandKind::Peek {
                from_sequence: SequenceNumber::new(0),
                max_messages: 20,
                session_id,
            })
            .await?
        else {
            panic!("peek outcome")
        };
        Ok(messages)
    }

    fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.store.as_ref().expect("open store").snapshot()?)
    }

    async fn wait_empty(&self) -> TestResult {
        timeout(DEADLINE, async {
            loop {
                if self.peek(None).await?.is_empty() {
                    return Ok::<(), Box<dyn Error>>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        Ok(())
    }
}

impl<P: StoreProvider> Drop for Node<P> {
    fn drop(&mut self) {
        if let Some(listener) = self.listener.take() {
            listener.abort();
        }
    }
}

fn rich_message(index: usize) -> Message {
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
            user_id: Some(vec![index as u8, 0, 255].into()),
            subject: Some(format!("subject-{index}")),
            to: Some(format!("destination-{index}")),
            reply_to: Some(format!("reply-{index}")),
            reply_to_group_id: Some(format!("reply-session-{index}")),
            content_type: Some(Symbol::from("application/octet-stream")),
            content_encoding: Some(Symbol::from("utf-8")),
            group_sequence: Some(index as u32),
            ..Properties::default()
        }),
        application_properties: Some(
            ApplicationProperties::builder()
                .insert("member", Value::Int(index as i32))
                .insert("nullable", Value::Null)
                .insert("binary", Value::Binary(vec![index as u8, 0, 255].into()))
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

fn batch_encoded(entries: Vec<Vec<u8>>) -> Message {
    Message {
        body: Body::Data(entries.into_iter().map(Into::into).collect()),
        ..Message::default()
    }
}

fn batch(messages: &[Message]) -> TestResult<Message> {
    Ok(batch_encoded(
        messages
            .iter()
            .map(encode_message)
            .collect::<Result<_, _>>()?,
    ))
}

async fn send_batch(sender: &mut ClientSender, message: Message) -> TestResult<Outcome> {
    Ok(timeout(
        DEADLINE,
        sender.send_with_message_format(message, protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT),
    )
    .await??)
}

fn accepted(outcome: Outcome) {
    assert!(matches!(outcome, Outcome::Accepted(_)), "{outcome:?}");
}

fn rejected(outcome: Outcome) {
    let Outcome::Rejected(rejected) = outcome else {
        panic!("batch should be rejected: {outcome:?}")
    };
    assert!(rejected.error.is_some(), "refusal must explain the failure");
}

fn rejected_with_condition(outcome: Outcome, condition: &str) {
    let Outcome::Rejected(rejected) = outcome else {
        panic!("batch should be rejected: {outcome:?}")
    };
    assert_eq!(
        rejected.error.expect("refusal error").condition.as_symbol(),
        Symbol::from(condition),
    );
}

fn assert_content(actual: &Message, expected: &Message) {
    assert_eq!(actual.body, expected.body);
    let actual_application = actual
        .application_properties
        .as_ref()
        .expect("received application properties");
    let expected_application = expected
        .application_properties
        .as_ref()
        .expect("producer application properties");
    assert_eq!(actual_application.0.len(), expected_application.0.len());
    for (key, value) in &expected_application.0 {
        assert_eq!(
            actual_application.get(key),
            Some(value),
            "application property {key}"
        );
    }
    let actual_footer = actual.footer.as_ref().expect("received footer");
    let expected_footer = expected.footer.as_ref().expect("producer footer");
    assert_eq!(actual_footer.len(), expected_footer.len());
    for (key, value) in expected_footer.iter() {
        assert_eq!(
            actual_footer.get(key),
            Some(value),
            "footer property {key:?}"
        );
    }
    let actual_properties = actual.properties.as_ref().expect("received properties");
    let expected_properties = expected.properties.as_ref().expect("producer properties");
    assert_eq!(actual_properties.message_id, expected_properties.message_id);
    assert_eq!(
        actual_properties.correlation_id,
        expected_properties.correlation_id
    );
    assert_eq!(actual_properties.user_id, expected_properties.user_id);
    assert_eq!(actual_properties.subject, expected_properties.subject);
    assert_eq!(actual_properties.to, expected_properties.to);
    assert_eq!(actual_properties.reply_to, expected_properties.reply_to);
    assert_eq!(
        actual_properties.reply_to_group_id,
        expected_properties.reply_to_group_id
    );
    assert_eq!(
        actual_properties.content_type,
        expected_properties.content_type
    );
    assert_eq!(
        actual_properties.content_encoding,
        expected_properties.content_encoding
    );
    assert_eq!(
        actual_properties.group_sequence,
        expected_properties.group_sequence
    );
    assert_eq!(actual_properties.group_id, expected_properties.group_id);
    let header = actual.header.as_ref().expect("received header");
    assert_eq!(header.ttl, Some(50_000));
    assert!(header.durable);
    assert_eq!(header.priority, 7);
    assert_eq!(header.delivery_count, 0);
    assert_eq!(actual_properties.creation_time, Some(1_000));
    assert_eq!(actual_properties.absolute_expiry_time, Some(51_000));
    assert_eq!(
        actual
            .message_annotations
            .as_ref()
            .expect("annotations")
            .get(Symbol::from("producer-index")),
        expected
            .message_annotations
            .as_ref()
            .expect("producer annotations")
            .get(Symbol::from("producer-index")),
    );
}

async fn rich_members<P: StoreProvider>(provider: P) -> TestResult {
    let mut node = Node::start(provider, QueueConfig::default(), None).await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut sender = ClientSender::attach(&mut session, "batch-producer", "orders").await?;
    let mut messages = [rich_message(0), rich_message(1), rich_message(2)];
    messages[0].properties.as_mut().unwrap().message_id = None;
    messages[1].properties.as_mut().unwrap().message_id = Some("".into());
    messages[2].properties.as_mut().unwrap().message_id = Some(MessageId::Ulong(42));
    for members in [&messages[..1], &messages[1..]] {
        let mut outer = batch(members)?;
        outer.header = Some(Header {
            ttl: Some(1),
            ..Header::default()
        });
        outer.properties = Some(Properties {
            message_id: Some("outer-id-is-not-an-inner-id".into()),
            subject: Some("outer-subject".to_owned()),
            ..Properties::default()
        });
        outer.application_properties = Some(
            ApplicationProperties::builder()
                .insert("outer-only", true)
                .build(),
        );
        accepted(send_batch(&mut sender, outer).await?);
    }
    let stored = node.peek(None).await?;
    assert_eq!(stored.len(), 3);
    for (index, message) in stored.iter().enumerate() {
        assert_eq!(message.sequence, SequenceNumber::new(index as u64 + 1));
        assert_eq!(message.delivery_count, 0);
        assert_eq!(message.expires_at, Some(Timestamp::from_millis(51_000)));
        assert_eq!(message.time_to_live_millis, Some(50_000));
        let incoming = protocol_amqp::read_incoming(&messages[index])?;
        assert_eq!(message.envelope.as_deref(), Some(&incoming.envelope));
    }
    connection.close().await?;
    let before = node.snapshot()?;
    node.restart().await?;
    assert_eq!(node.snapshot()?, before);
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut receiver = ClientReceiver::builder()
        .name("batch-consumer")
        .source("orders")
        .attach(&mut session)
        .await?;
    for (index, expected) in messages.iter().enumerate() {
        let delivery = timeout(DEADLINE, receiver.recv()).await??;
        assert_content(delivery.message(), expected);
        assert_eq!(
            delivery
                .message()
                .message_annotations
                .as_ref()
                .unwrap()
                .get(Symbol::from("x-opt-sequence-number")),
            Some(&Value::Long(index as i64 + 1)),
        );
        receiver.accept(&delivery).await?;
    }
    node.wait_empty().await?;
    connection.close().await?;
    Ok(())
}

fn null_array(count: u32) -> Vec<u8> {
    let mut encoded = vec![0x00, 0x53, 0x77, 0xf0];
    encoded.extend_from_slice(&5_u32.to_be_bytes());
    encoded.extend_from_slice(&count.to_be_bytes());
    encoded.push(0x40);
    encoded
}

fn described_null_array(count: u32, name_bytes: usize) -> Vec<u8> {
    let mut constructor = vec![0x00, 0xb3];
    constructor.extend_from_slice(&(name_bytes as u32).to_be_bytes());
    constructor.extend(std::iter::repeat_n(b'd', name_bytes));
    constructor.push(0x40);
    let mut encoded = vec![0x00, 0x53, 0x77, 0xf0];
    encoded.extend_from_slice(&(4_u32 + constructor.len() as u32).to_be_bytes());
    encoded.extend_from_slice(&count.to_be_bytes());
    encoded.extend(constructor);
    encoded
}

async fn malformed_batches<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, QueueConfig::default(), None).await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut sender = ClientSender::attach(&mut session, "invalid-batch-producer", "orders").await?;
    let good = encode_message(&rich_message(0))?;
    let mut trailing = good.clone();
    trailing.extend_from_slice(&[0x00, 0x53, 0x70, 0x45]);
    let cases = vec![
        batch_encoded(vec![good.clone(), vec![0x00, 0x53, 0x77, 0xa0, 2, 1]]),
        batch_encoded(vec![good.clone(), trailing]),
        Message {
            body: Body::Value(Value::Binary(good.clone().into())),
            ..Message::default()
        },
        Message {
            body: Body::Sequence(vec![vec![Value::Binary(good.clone().into())]]),
            ..Message::default()
        },
        batch_encoded(Vec::new()),
        batch_encoded(vec![Vec::new(); domain::MAX_INGRESS_BATCH_MESSAGES + 1]),
        batch_encoded(vec![null_array(40_000); 4]),
        batch_encoded(vec![described_null_array(1_024, 2_048); 2]),
    ];
    let before = node.snapshot()?;
    for (index, message) in cases.into_iter().enumerate() {
        rejected_with_condition(
            send_batch(&mut sender, message).await?,
            if index == 5 {
                protocol_amqp::RESOURCE_LIMIT_EXCEEDED
            } else {
                protocol_amqp::INVALID_FIELD
            },
        );
        assert_eq!(
            node.snapshot()?,
            before,
            "a refused batch changed persisted state"
        );
    }
    assert!(node.peek(None).await?.is_empty());
    accepted(send_batch(&mut sender, batch(&[rich_message(9)])?).await?);
    assert_eq!(node.peek(None).await?[0].sequence, SequenceNumber::new(1));
    connection.close().await?;
    Ok(())
}

async fn invalid_later_members<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, QueueConfig::default(), None).await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut sender =
        ClientSender::attach(&mut session, "validation-batch-producer", "orders").await?;
    let mut invalid_id = rich_message(1);
    invalid_id.properties.as_mut().unwrap().message_id = Some("x".repeat(129).into());
    let mut invalid_app = rich_message(1);
    invalid_app
        .application_properties
        .as_mut()
        .unwrap()
        .insert("compound", Value::List(vec![Value::Null]));
    let mut invalid_time = rich_message(1);
    invalid_time.message_annotations.as_mut().unwrap().insert(
        Symbol::from(protocol_amqp::SCHEDULED_ENQUEUE_TIME_ANNOTATION),
        Value::String("not a timestamp".to_owned()),
    );
    let before = node.snapshot()?;
    for invalid in [invalid_id, invalid_app, invalid_time] {
        rejected(send_batch(&mut sender, batch(&[rich_message(0), invalid])?).await?);
        assert_eq!(node.snapshot()?, before);
    }
    accepted(send_batch(&mut sender, batch(&[rich_message(0), rich_message(1)])?).await?);
    assert_eq!(node.peek(None).await?.len(), 2);
    connection.close().await?;
    Ok(())
}

async fn sessions<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(
        provider,
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
        None,
    )
    .await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut sender = ClientSender::attach(&mut session, "session-batch-producer", "orders").await?;
    let mut first = rich_message(0);
    first.properties.as_mut().unwrap().group_id = Some("session-a".to_owned());
    let mut different = rich_message(1);
    different.properties.as_mut().unwrap().group_id = Some("session-b".to_owned());
    let before = node.snapshot()?;
    for second in [different, rich_message(1)] {
        let mut outer = batch(&[first.clone(), second])?;
        outer.properties = Some(Properties {
            group_id: Some("session-a".to_owned()),
            ..Properties::default()
        });
        rejected(send_batch(&mut sender, outer).await?);
        assert_eq!(node.snapshot()?, before);
    }
    let mut second = rich_message(1);
    second.properties.as_mut().unwrap().group_id = Some("session-a".to_owned());
    accepted(send_batch(&mut sender, batch(&[first, second])?).await?);
    let messages = node.peek(Some(SessionId::new("session-a")?)).await?;
    assert_eq!(
        messages
            .iter()
            .map(|message| message.sequence.as_u64())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(
        messages
            .iter()
            .all(|message| message.session_id.as_ref().unwrap().as_str() == "session-a")
    );
    assert!(
        node.peek(Some(SessionId::new("session-b")?))
            .await?
            .is_empty()
    );
    connection.close().await?;
    Ok(())
}

async fn duplicates<P: StoreProvider>(provider: P) -> TestResult {
    let mut node = Node::start(
        provider,
        QueueConfig {
            requires_duplicate_detection: true,
            duplicate_detection_history_time_window_millis: 20_000,
            ..QueueConfig::default()
        },
        None,
    )
    .await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut sender = ClientSender::attach(&mut session, "dedup-batch-producer", "orders").await?;
    let first = rich_message(0);
    let mut duplicate = rich_message(1);
    duplicate.properties.as_mut().unwrap().message_id =
        first.properties.as_ref().unwrap().message_id.clone();
    let distinct = rich_message(2);
    accepted(
        send_batch(
            &mut sender,
            batch(&[first.clone(), duplicate.clone(), distinct.clone()])?,
        )
        .await?,
    );
    let initial = node.peek(None).await?;
    assert_eq!(initial.len(), 2);
    assert_eq!(
        initial
            .iter()
            .map(|message| message.sequence.as_u64())
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
    node.clock.set(2_000);
    accepted(send_batch(&mut sender, batch(&[duplicate, distinct])?).await?);
    assert_eq!(node.peek(None).await?, initial);
    connection.close().await?;
    let before = node.snapshot()?;
    node.restart().await?;
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.peek(None).await?, initial);
    Ok(())
}

async fn mixed_schedule<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, QueueConfig::default(), None).await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut sender =
        ClientSender::attach(&mut session, "scheduled-batch-producer", "orders").await?;
    let mut ordinary = rich_message(0);
    ordinary.header.as_mut().unwrap().ttl = None;
    let mut past = rich_message(1);
    past.header.as_mut().unwrap().ttl = Some(2_000);
    let mut future = rich_message(2);
    future.header.as_mut().unwrap().ttl = Some(1_000);
    for (message, millis) in [(&mut past, 500_i64), (&mut future, 3_000_i64)] {
        message.message_annotations.as_mut().unwrap().insert(
            Symbol::from(protocol_amqp::SCHEDULED_ENQUEUE_TIME_ANNOTATION),
            Value::Timestamp(millis.into()),
        );
    }
    accepted(send_batch(&mut sender, batch(&[ordinary, past, future])?).await?);
    let messages = node.peek(None).await?;
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0].status, MessageStatus::Active);
    assert_eq!(messages[0].scheduled_enqueue_time, None);
    assert_eq!(messages[0].expires_at, None);
    assert_eq!(messages[1].status, MessageStatus::Active);
    assert_eq!(
        messages[1].scheduled_enqueue_time,
        Some(Timestamp::from_millis(500))
    );
    assert_eq!(messages[1].expires_at, Some(Timestamp::from_millis(3_000)));
    assert_eq!(messages[2].status, MessageStatus::Scheduled);
    assert_eq!(messages[2].expires_at, None);
    node.clock.set(3_000);
    node.submit(CommandKind::ActivateScheduled).await?;
    let activated = node.peek(None).await?;
    let future = activated
        .iter()
        .find(|message| message.message_id == "member-2")
        .expect("activated member");
    assert_eq!(future.sequence, SequenceNumber::new(4));
    assert_eq!(future.status, MessageStatus::Active);
    assert_eq!(future.enqueued_at, Timestamp::from_millis(3_000));
    assert_eq!(
        future.scheduled_enqueue_time,
        Some(Timestamp::from_millis(3_000))
    );
    assert_eq!(future.expires_at, Some(Timestamp::from_millis(4_000)));
    node.clock.set(3_999);
    assert!(
        node.peek(None)
            .await?
            .iter()
            .any(|message| message.sequence == SequenceNumber::new(4))
    );
    node.clock.set(4_000);
    node.submit(CommandKind::ExpireMessages).await?;
    assert_eq!(node.peek(None).await?.len(), 1);
    connection.close().await?;
    Ok(())
}

async fn exhausted_batch<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(
        provider,
        QueueConfig::default(),
        Some(QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER,
            next_lock_token: 1,
        }),
    )
    .await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut sender =
        ClientSender::attach(&mut session, "exhaustion-batch-producer", "orders").await?;
    let before = node.snapshot()?;
    rejected(send_batch(&mut sender, batch(&[rich_message(0), rich_message(1)])?).await?);
    assert_eq!(node.snapshot()?, before);
    accepted(send_batch(&mut sender, batch(&[rich_message(0)])?).await?);
    assert_eq!(
        node.peek(None).await?[0].sequence,
        SequenceNumber::new(MAX_SEQUENCE_NUMBER)
    );
    let after = node.snapshot()?;
    rejected(send_batch(&mut sender, batch(&[rich_message(1)])?).await?);
    assert_eq!(node.snapshot()?, after);
    connection.close().await?;
    Ok(())
}

async fn failed_commit<P: StoreProvider>(provider: P) -> TestResult {
    let mut node = Node::start(provider, QueueConfig::default(), None).await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut sender = ClientSender::attach(&mut session, "failure-batch-producer", "orders").await?;
    let before = node.snapshot()?;
    node.clock.set(2_000);
    node.fail_next.store(true, Ordering::Relaxed);
    rejected(send_batch(&mut sender, batch(&[rich_message(0), rich_message(1)])?).await?);
    assert_eq!(node.snapshot()?, before);
    accepted(send_batch(&mut sender, batch(&[rich_message(0), rich_message(1)])?).await?);
    assert_eq!(
        node.peek(None)
            .await?
            .iter()
            .map(|message| message.sequence.as_u64())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    connection.close().await?;
    let after = node.snapshot()?;
    node.restart().await?;
    assert_eq!(node.snapshot()?, after);
    assert_eq!(node.peek(None).await?.len(), 2);
    Ok(())
}

async fn unsupported_links<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, QueueConfig::default(), None).await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut management = ClientSender::attach(
        &mut session,
        "management-batch-is-unsupported",
        "orders/$management",
    )
    .await?;
    let before = node.snapshot()?;
    let error = timeout(
        DEADLINE,
        management.send_with_message_format(
            batch(&[rich_message(0)])?,
            protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
        ),
    )
    .await?
    .expect_err("management cannot accept batch format");
    assert!(matches!(error, EngineError::RemoteDetached), "{error:?}");
    assert_eq!(node.snapshot()?, before);
    let mut unknown = ClientSender::attach(&mut session, "unknown-format", "orders").await?;
    let error = timeout(
        DEADLINE,
        unknown.send_with_message_format(rich_message(0), 0x80013701),
    )
    .await?
    .expect_err("an unregistered format is unsupported");
    assert!(matches!(error, EngineError::RemoteDetached), "{error:?}");
    assert_eq!(node.snapshot()?, before);
    let mut healthy = ClientSender::attach(&mut session, "healthy-format", "orders").await?;
    accepted(send_batch(&mut healthy, batch(&[rich_message(0)])?).await?);
    assert_eq!(node.peek(None).await?.len(), 1);
    connection.close().await?;
    Ok(())
}

macro_rules! suite {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;
            #[tokio::test]
            async fn forced_single_and_multiple_rich_members_survive_restart_and_delivery() -> TestResult {
                rich_members($provider).await
            }
            #[tokio::test]
            async fn malformed_outer_and_inner_batches_are_atomic_and_the_link_can_retry() -> TestResult {
                malformed_batches($provider).await
            }
            #[tokio::test]
            async fn invalid_later_member_does_not_publish_earlier_members() -> TestResult {
                invalid_later_members($provider).await
            }
            #[tokio::test]
            async fn batch_sessions_must_agree_and_outer_session_cannot_repair_missing_inner_session() -> TestResult {
                sessions($provider).await
            }
            #[tokio::test]
            async fn within_batch_and_replayed_duplicates_keep_original_content() -> TestResult {
                duplicates($provider).await
            }
            #[tokio::test]
            async fn ordinary_past_and_future_members_use_their_own_enqueue_time_and_ttl() -> TestResult {
                mixed_schedule($provider).await
            }
            #[tokio::test]
            async fn mid_batch_counter_exhaustion_rolls_back_and_last_identifier_can_retry() -> TestResult {
                exhausted_batch($provider).await
            }
            #[tokio::test]
            async fn failed_commit_publishes_nothing_and_valid_retry_survives_restart() -> TestResult {
                failed_commit($provider).await
            }
            #[tokio::test]
            async fn format_refusal_is_link_local_and_management_is_not_a_producer() -> TestResult {
                unsupported_links($provider).await
            }
        }
    };
}

suite!(memory, testkit::MemoryProvider::new());
suite!(durable, testkit::DurableProvider::temporary()?);
