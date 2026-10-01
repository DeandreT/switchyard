//! Queue identifiers remain exact at the signed Service Bus wire boundary.

use std::{error::Error, fmt::Display, future::Future, time::Duration};

use amqp::{
    ApplicationProperties, Array, Body, ClientConnection, ClientReceiver, ClientSender,
    ClientSession, EngineError, Message, MessageId, Modified, OrderedMap, Outcome, Properties,
    SenderSettleMode, Symbol, Value, decode_message, encode_message,
};
use domain::{
    Command, CommandKind, CommandOutcome, EntityPath, MAX_SEQUENCE_NUMBER, MessageStatus,
    NamespaceName, QueueConfig, QueueCounters, SequenceNumber, StateMachine, Timestamp, codec,
    keys,
};
use server::{Broker, LocalProposer, ManualClock};
use storage::{StateStore, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(5);

async fn bounded<T, E: Display>(
    operation: &str,
    future: impl Future<Output = Result<T, E>>,
) -> TestResult<T> {
    timeout(DEADLINE, future)
        .await
        .map_err(|error| std::io::Error::other(format!("{operation}: {error}")))?
        .map_err(|error| std::io::Error::other(format!("{operation}: {error}")).into())
}

struct Node<P: StoreProvider> {
    broker: Broker,
    store: P::Store,
    clock: ManualClock,
    namespace: NamespaceName,
    entity: EntityPath,
    address: String,
    listener: JoinHandle<()>,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    async fn start(provider: P, counters: QueueCounters, seed: bool) -> TestResult<Self> {
        let store = provider.open()?;
        let machine = StateMachine::new(store.clone());
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        for path in [entity.clone(), EntityPath::new("other")?] {
            machine.apply(&Command {
                namespace: namespace.clone(),
                entity: path,
                issued_at: Timestamp::from_millis(1_000),
                kind: CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            })?;
        }
        if seed {
            machine.apply(&Command {
                namespace: namespace.clone(),
                entity: entity.clone(),
                issued_at: Timestamp::from_millis(1_000),
                kind: CommandKind::Send {
                    message_id: "retained".to_owned(),
                    body: b"retained".to_vec(),
                    time_to_live_millis: None,
                    session_id: None,
                },
            })?;
        }
        // Install boundary fixtures before transferring ownership to the broker.
        let mut batch = WriteBatch::default();
        batch.push_put(
            keys::queue_counters(&namespace, &entity),
            codec::encode(&counters)?,
        );
        store.apply(batch)?;
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(machine, clock.clone()));
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
            store,
            clock,
            namespace,
            entity,
            address,
            listener,
            _provider: provider,
        })
    }

    async fn connect(&self) -> TestResult<ClientConnection> {
        Ok(ClientConnection::builder()
            .container_id("counter-client")
            .open(&format!("amqp://{}", self.address))
            .await?)
    }

    async fn submit(&self, kind: CommandKind) -> TestResult<CommandOutcome> {
        Ok(self
            .broker
            .handle()
            .submit(self.namespace.clone(), self.entity.clone(), kind)
            .await?)
    }

    fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.store.snapshot()?)
    }

    async fn wait_deferred(&self, sequence: u64) -> TestResult {
        timeout(DEADLINE, async {
            loop {
                let CommandOutcome::Peeked(messages) = self
                    .submit(CommandKind::Peek {
                        from_sequence: SequenceNumber::new(sequence),
                        max_messages: 1,
                        session_id: None,
                    })
                    .await?
                else {
                    panic!("peek outcome")
                };
                if messages
                    .first()
                    .is_some_and(|message| message.status == MessageStatus::Deferred)
                {
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
        self.listener.abort();
    }
}

struct Management {
    requests: ClientSender,
    responses: ClientReceiver,
    next_id: u64,
}

impl Management {
    async fn attach(session: &mut ClientSession) -> TestResult<Self> {
        let responses = ClientReceiver::builder()
            .name("counter-responses")
            .source("orders/$management")
            .target("counter-replies")
            .attach(session)
            .await?;
        let requests =
            ClientSender::attach(session, "counter-requests", "orders/$management").await?;
        Ok(Self {
            requests,
            responses,
            next_id: 0,
        })
    }

    async fn request(
        &mut self,
        operation: &str,
        body: OrderedMap<Value, Value>,
    ) -> TestResult<Message> {
        self.next_id += 1;
        let request = Message::builder()
            .properties(Properties {
                message_id: Some(MessageId::Ulong(self.next_id)),
                reply_to: Some("counter-replies".to_owned()),
                ..Properties::default()
            })
            .application_properties(
                ApplicationProperties::builder()
                    .insert(protocol_amqp::OPERATION_PROPERTY, operation)
                    .insert(
                        protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY,
                        "counter-receiver",
                    )
                    .build(),
            )
            .body(Body::Value(Value::Map(body)))
            .build();
        assert!(matches!(
            timeout(DEADLINE, self.requests.send(request)).await??,
            Outcome::Accepted(_)
        ));
        let delivery = timeout(DEADLINE, self.responses.recv()).await??;
        let response = delivery.message().clone();
        self.responses.accept(&delivery).await?;
        assert_eq!(
            response
                .properties
                .as_ref()
                .and_then(|p| p.correlation_id.as_ref()),
            Some(&MessageId::Ulong(self.next_id))
        );
        Ok(response)
    }
}

fn status(message: &Message) -> Option<&Value> {
    message
        .application_properties
        .as_ref()?
        .get(protocol_amqp::STATUS_CODE_PROPERTY)
}

fn sequence(message: &Message) -> Option<&Value> {
    message
        .message_annotations
        .as_ref()?
        .get(Symbol::from("x-opt-sequence-number"))
}

fn map(entries: impl IntoIterator<Item = (&'static str, Value)>) -> OrderedMap<Value, Value> {
    entries
        .into_iter()
        .map(|(key, value)| (Value::String(key.to_owned()), value))
        .collect()
}

fn response_messages(message: &Message) -> TestResult<Vec<Message>> {
    let Body::Value(Value::Map(body)) = &message.body else {
        panic!("response map")
    };
    let Some(Value::List(entries)) = body.get(&Value::String(protocol_amqp::MESSAGES.to_owned()))
    else {
        panic!("response message list")
    };
    entries
        .iter()
        .map(|entry| {
            let Value::Map(entry) = entry else {
                panic!("message entry map")
            };
            let Some(Value::Binary(bytes)) =
                entry.get(&Value::String(protocol_amqp::MESSAGE.to_owned()))
            else {
                panic!("encoded message")
            };
            Ok(decode_message(bytes)?)
        })
        .collect()
}

fn schedule_body() -> TestResult<OrderedMap<Value, Value>> {
    let mut message = Message::data(b"scheduled".to_vec());
    message.message_annotations = Some(
        [(
            Symbol::from(protocol_amqp::SCHEDULED_ENQUEUE_TIME_ANNOTATION),
            Value::Timestamp(3_000_i64.into()),
        )]
        .into_iter()
        .collect::<OrderedMap<_, _>>()
        .into(),
    );
    Ok(map([(
        protocol_amqp::MESSAGES,
        Value::List(vec![Value::Map(map([(
            protocol_amqp::MESSAGE,
            Value::Binary(encode_message(&message)?.into()),
        )]))]),
    )]))
}

async fn signed_boundary<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(
        provider,
        QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER - 1,
            next_lock_token: 1,
        },
        false,
    )
    .await?;
    let mut connection = node.connect().await?;
    let mut session = ClientSession::begin(&mut connection).await?;
    let mut management = Management::attach(&mut session).await?;
    let scheduled = management
        .request(protocol_amqp::SCHEDULE_MESSAGE_OPERATION, schedule_body()?)
        .await?;
    assert_eq!(status(&scheduled), Some(&Value::Int(200)));
    let Body::Value(Value::Map(body)) = &scheduled.body else {
        panic!("schedule response map")
    };
    assert_eq!(
        body.get(&Value::String(protocol_amqp::SEQUENCE_NUMBERS.to_owned())),
        Some(&Value::Array(Array::from(vec![Value::Long(i64::MAX - 1)])))
    );
    let mut sender = ClientSender::attach(&mut session, "counter-sender", "orders").await?;
    assert!(matches!(
        sender.send(Message::data(b"last".to_vec())).await?,
        Outcome::Accepted(_)
    ));

    let before = node.snapshot()?;
    let Outcome::Rejected(rejected) = sender.send(Message::data(b"refused".to_vec())).await? else {
        panic!("exhaustion refuses send")
    };
    assert_eq!(
        rejected
            .error
            .as_ref()
            .map(|error| error.condition.as_symbol()),
        Some(Symbol::from(protocol_amqp::RESOURCE_LIMIT_EXCEEDED))
    );
    assert_eq!(node.snapshot()?, before);
    let refused = management
        .request(protocol_amqp::SCHEDULE_MESSAGE_OPERATION, schedule_body()?)
        .await?;
    assert_eq!(status(&refused), Some(&Value::Int(403)));
    assert_eq!(
        refused
            .application_properties
            .as_ref()
            .and_then(|p| p.get(protocol_amqp::ERROR_CONDITION_PROPERTY)),
        Some(&Value::Symbol(Symbol::from(
            protocol_amqp::RESOURCE_LIMIT_EXCEEDED
        )))
    );
    assert_eq!(node.snapshot()?, before);

    let peek = management
        .request(
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            map([
                (
                    protocol_amqp::FROM_SEQUENCE_NUMBER,
                    Value::Long(i64::MAX - 1),
                ),
                (protocol_amqp::MESSAGE_COUNT, Value::Int(2)),
            ]),
        )
        .await?;
    assert_eq!(status(&peek), Some(&Value::Int(200)));
    let messages = response_messages(&peek)?;
    assert_eq!(messages.len(), 2);
    assert_eq!(sequence(&messages[0]), Some(&Value::Long(i64::MAX - 1)));
    assert_eq!(sequence(&messages[1]), Some(&Value::Long(i64::MAX)));
    let cancelled = management
        .request(
            protocol_amqp::CANCEL_SCHEDULED_MESSAGE_OPERATION,
            map([(
                protocol_amqp::SEQUENCE_NUMBERS,
                Value::Array(Array::from(vec![Value::Long(i64::MAX - 1)])),
            )]),
        )
        .await?;
    assert_eq!(status(&cancelled), Some(&Value::Int(200)));

    let mut receiver = ClientReceiver::attach(&mut session, "counter-receiver", "orders").await?;
    let delivery = timeout(DEADLINE, receiver.recv()).await??;
    assert_eq!(sequence(delivery.message()), Some(&Value::Long(i64::MAX)));
    receiver
        .modify(
            &delivery,
            Modified {
                undeliverable_here: Some(true),
                ..Modified::default()
            },
        )
        .await?;
    node.wait_deferred(MAX_SEQUENCE_NUMBER).await?;
    let deferred = management
        .request(
            protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
            map([
                (
                    protocol_amqp::SEQUENCE_NUMBERS,
                    Value::Array(Array::from(vec![Value::Long(i64::MAX)])),
                ),
                (protocol_amqp::RECEIVER_SETTLE_MODE, Value::Uint(0)),
            ]),
        )
        .await?;
    assert_eq!(status(&deferred), Some(&Value::Int(200)));
    let messages = response_messages(&deferred)?;
    assert_eq!(messages.len(), 1);
    assert_eq!(sequence(&messages[0]), Some(&Value::Long(i64::MAX)));
    let CommandOutcome::Peeked(retained) = node
        .submit(CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 10,
            session_id: None,
        })
        .await?
    else {
        panic!("peek outcome")
    };
    assert!(retained.is_empty());
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    Ok(())
}

async fn exhausted_locks<P: StoreProvider>(provider: P) -> TestResult {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let node = Node::start(
        provider,
        QueueCounters {
            next_sequence: 2,
            next_lock_token: u64::MAX,
        },
        true,
    )
    .await?;
    let mut connection = bounded("connect", node.connect()).await?;
    let mut session = bounded("begin", ClientSession::begin(&mut connection)).await?;
    let before = node.snapshot()?;
    let mut receiver = bounded(
        "attach exhausted receiver",
        ClientReceiver::attach(&mut session, "counter-receiver", "orders"),
    )
    .await?;
    let error = timeout(DEADLINE, receiver.recv())
        .await?
        .expect_err("no lock token can be allocated");
    assert!(matches!(error, EngineError::RemoteDetached), "{error}");
    assert_eq!(node.snapshot()?, before);
    let mut other = bounded(
        "attach unaffected sender",
        ClientSender::attach(&mut session, "unaffected-sender", "other"),
    )
    .await?;
    assert!(matches!(
        bounded(
            "send unaffected",
            other.send(Message::data(b"unaffected".to_vec()))
        )
        .await?,
        Outcome::Accepted(_)
    ));
    let mut drain = bounded(
        "attach delete receiver",
        ClientReceiver::builder()
            .name("delete-receiver")
            .source("orders")
            .sender_settle_mode(SenderSettleMode::Settled)
            .attach(&mut session),
    )
    .await?;
    let delivery = bounded("receive delete", drain.recv()).await?;
    assert_eq!(sequence(delivery.message()), Some(&Value::Long(1)));
    bounded("close delete receiver", drain.close()).await?;
    bounded("end", session.end()).await?;
    bounded("close", connection.close()).await?;
    Ok(())
}

async fn activated_boundary<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(
        provider,
        QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER - 1,
            next_lock_token: 1,
        },
        false,
    )
    .await?;
    let mut connection = node.connect().await?;
    let mut session = ClientSession::begin(&mut connection).await?;
    let mut management = Management::attach(&mut session).await?;
    let scheduled = management
        .request(protocol_amqp::SCHEDULE_MESSAGE_OPERATION, schedule_body()?)
        .await?;
    assert_eq!(status(&scheduled), Some(&Value::Int(200)));
    node.clock.set(3_000);
    assert_eq!(
        node.submit(CommandKind::ActivateScheduled).await?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    let mut receiver = ClientReceiver::attach(&mut session, "counter-receiver", "orders").await?;
    let delivery = timeout(DEADLINE, receiver.recv()).await??;
    assert_eq!(sequence(delivery.message()), Some(&Value::Long(i64::MAX)));
    receiver.accept(&delivery).await?;
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    Ok(())
}

macro_rules! backend_tests {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;
            #[tokio::test(flavor = "multi_thread")]
            async fn signed_sequence_round_trips_and_exhaustion_is_atomic() -> TestResult {
                signed_boundary($provider).await
            }
            #[tokio::test(flavor = "multi_thread")]
            async fn exhausted_locks_refuse_only_peek_lock_and_preserve_other_links() -> TestResult
            {
                exhausted_locks($provider).await
            }
            #[tokio::test(flavor = "multi_thread")]
            async fn activation_uses_the_last_exact_wire_sequence() -> TestResult {
                activated_boundary($provider).await
            }
        }
    };
}

backend_tests!(memory, testkit::MemoryProvider::new());
backend_tests!(durable, testkit::DurableProvider::temporary()?);
