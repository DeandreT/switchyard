//! Invalid deferred batches leave both records and lock allocation unchanged.

use std::{error::Error, time::Duration};

use amqp::{
    ApplicationProperties, Array, Body, ClientConnection, ClientReceiver, ClientSender,
    ClientSession, Message, MessageId, OrderedMap, Outcome, Properties, Symbol, Uuid, Value,
    decode_message,
};
use domain::{
    CommandKind, CommandOutcome, Delivery, EntityPath, LockToken, MessageStatus, NamespaceName,
    QueueConfig, ReceiveMode, SequenceNumber, StateMachine,
};
use server::{Broker, LocalProposer, ManualClock};
use testkit::StoreProvider;
use tokio::{net::TcpListener, task::JoinHandle};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct Node<P> {
    broker: Broker,
    address: String,
    listener: JoinHandle<()>,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    async fn start(provider: P) -> TestResult<Self> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(provider.open()?),
            ManualClock::at(1_000),
        ));
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        let handle = broker.handle();
        handle.submit_blocking(
            namespace.clone(),
            entity.clone(),
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        )?;
        for index in 1..=2 {
            let CommandOutcome::Sent { sequence } = handle.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::Send {
                    message_id: format!("deferred-{index}"),
                    body: format!("payload-{index}").into_bytes(),
                    time_to_live_millis: None,
                    session_id: None,
                },
            )?
            else {
                panic!("the fixture message was not sent");
            };
            assert_eq!(sequence, SequenceNumber::new(index));
            let CommandOutcome::Received(Some(delivery)) = handle.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None,
                },
            )?
            else {
                panic!("the fixture message was not locked");
            };
            let token = delivery.lock.expect("the fixture grants a lock").token;
            assert_eq!(token, LockToken::new(index));
            assert_eq!(
                handle.submit_blocking(
                    namespace.clone(),
                    entity.clone(),
                    CommandKind::Defer {
                        sequence,
                        lock_token: token,
                    },
                )?,
                CommandOutcome::Deferred
            );
        }

        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?.to_string();
        let listener = tokio::spawn(async move {
            let _ = protocol_amqp::AmqpListener::new(handle, namespace)
                .serve(socket)
                .await;
        });
        Ok(Self {
            broker,
            address,
            listener,
            _provider: provider,
        })
    }

    fn submit(&self, kind: CommandKind) -> TestResult<CommandOutcome> {
        Ok(self.broker.handle().submit_blocking(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            kind,
        )?)
    }

    fn peek(&self) -> TestResult<Vec<Delivery>> {
        let CommandOutcome::Peeked(deliveries) = self.submit(CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 100,
            session_id: None,
        })?
        else {
            panic!("the fixture expected a peek outcome");
        };
        Ok(deliveries)
    }
}

impl<P> Drop for Node<P> {
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
            .name("batch-responses")
            .source("orders/$management")
            .target("batch-replies")
            .attach(session)
            .await?;
        let requests =
            ClientSender::attach(session, "batch-requests", "orders/$management").await?;
        Ok(Self {
            requests,
            responses,
            next_id: 0,
        })
    }

    async fn receive_deferred(&mut self, sequences: &[i64], mode: u32) -> TestResult<Message> {
        self.next_id += 1;
        let message_id = MessageId::Ulong(self.next_id);
        let mut body = OrderedMap::new();
        body.insert(
            key(protocol_amqp::SEQUENCE_NUMBERS),
            Value::Array(Array::from(
                sequences
                    .iter()
                    .copied()
                    .map(Value::Long)
                    .collect::<Vec<_>>(),
            )),
        );
        body.insert(key(protocol_amqp::RECEIVER_SETTLE_MODE), Value::Uint(mode));
        let mut properties = ApplicationProperties::default();
        properties.insert(
            protocol_amqp::OPERATION_PROPERTY,
            protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
        );
        properties.insert(
            protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY,
            "batch-receiver",
        );
        let request = Message::builder()
            .properties(Properties {
                message_id: Some(message_id.clone()),
                reply_to: Some("batch-replies".to_owned()),
                ..Properties::default()
            })
            .application_properties(properties)
            .body(Body::Value(Value::Map(body)))
            .build();
        assert!(matches!(
            self.requests.send(request).await?,
            Outcome::Accepted(_)
        ));
        let response =
            tokio::time::timeout(Duration::from_secs(2), self.responses.recv()).await??;
        let message = response.message().clone();
        assert_eq!(
            message
                .properties
                .as_ref()
                .and_then(|properties| properties.correlation_id.as_ref()),
            Some(&message_id)
        );
        self.responses.accept(&response).await?;
        Ok(message)
    }
}

fn key(name: &str) -> Value {
    Value::String(name.to_owned())
}

fn assert_status(message: &Message, status: i32, condition: Option<&str>) {
    let properties = message
        .application_properties
        .as_ref()
        .expect("management response properties");
    assert_eq!(
        properties.get(protocol_amqp::STATUS_CODE_PROPERTY),
        Some(&Value::Int(status)),
        "response: {message:?}"
    );
    assert_eq!(
        properties.get(protocol_amqp::ERROR_CONDITION_PROPERTY),
        condition
            .map(|condition| Value::Symbol(Symbol::from(condition)))
            .as_ref()
    );
}

fn messages(message: &Message) -> TestResult<Vec<(Message, Option<Uuid>)>> {
    let Body::Value(Value::Map(body)) = &message.body else {
        panic!("management response must contain a map");
    };
    let Some(Value::List(entries)) = body.get(&key(protocol_amqp::MESSAGES)) else {
        panic!("management response must contain messages");
    };
    entries
        .iter()
        .map(|entry| {
            let Value::Map(entry) = entry else {
                panic!("each entry must be a map");
            };
            let Some(Value::Binary(encoded)) = entry.get(&key(protocol_amqp::MESSAGE)) else {
                panic!("each entry must contain an encoded message");
            };
            let lock_token = match entry.get(&key(protocol_amqp::LOCK_TOKEN)) {
                Some(Value::Uuid(token)) => Some(token.clone()),
                None => None,
                other => panic!("unexpected lock token: {other:?}"),
            };
            Ok((decode_message(encoded)?, lock_token))
        })
        .collect()
}

fn token(number: u64) -> Uuid {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&number.to_be_bytes());
    bytes.into()
}

async fn duplicate_batch_is_atomic_and_management_remains_usable<P: StoreProvider>(
    provider: P,
    mode: u32,
) -> TestResult {
    let node = Node::start(provider).await?;
    let before = node.peek()?;
    assert_eq!(before.len(), 2);
    assert!(before.iter().all(|delivery| {
        delivery.status == MessageStatus::Deferred && delivery.delivery_count == 1
    }));
    let mut connection = ClientConnection::builder()
        .container_id("deferred-batch-client")
        .open(&format!("amqp://{}", node.address))
        .await?;
    let mut session = ClientSession::begin(&mut connection).await?;
    let mut management = Management::attach(&mut session).await?;

    let rejected = management.receive_deferred(&[1, 2, 1], mode).await?;
    assert_status(&rejected, 400, Some(protocol_amqp::INVALID_FIELD));
    assert_eq!(node.peek()?, before);

    let accepted = management.receive_deferred(&[1, 2], mode).await?;
    assert_status(&accepted, 200, None);
    let deliveries = messages(&accepted)?;
    assert_eq!(deliveries.len(), 2);
    for (index, (message, lock_token)) in deliveries.iter().enumerate() {
        let number = index + 1;
        assert_eq!(
            message
                .properties
                .as_ref()
                .and_then(|properties| properties.message_id.as_ref()),
            Some(&MessageId::String(format!("deferred-{number}")))
        );
        assert_eq!(
            message.body,
            Body::Data(vec![format!("payload-{number}").into_bytes().into()])
        );
        assert_eq!(
            message
                .header
                .as_ref()
                .expect("delivery header")
                .delivery_count,
            1
        );
        if mode == 1 {
            assert_eq!(lock_token.as_ref(), Some(&token(index as u64 + 3)));
        } else {
            assert_eq!(*lock_token, None);
        }
    }

    if mode == 1 {
        let after = node.peek()?;
        assert_eq!(after.len(), 2);
        assert!(after.iter().all(|delivery| {
            delivery.status == MessageStatus::Active && delivery.delivery_count == 2
        }));
    } else {
        assert!(node.peek()?.is_empty());
        node.submit(CommandKind::Send {
            message_id: "next-lock".to_owned(),
            body: Vec::new(),
            time_to_live_millis: None,
            session_id: None,
        })?;
        let CommandOutcome::Received(Some(delivery)) = node.submit(CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        })?
        else {
            panic!("the next message must receive the next unused lock");
        };
        assert_eq!(delivery.lock.expect("lock").token, LockToken::new(3));
    }
    connection.close().await?;
    Ok(())
}

macro_rules! for_each_backend {
    ($($backend:ident => $provider:expr,)+) => {$ (
        mod $backend {
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn duplicate_peek_lock_batch() -> super::TestResult {
                super::duplicate_batch_is_atomic_and_management_remains_usable($provider, 1).await
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn duplicate_receive_and_delete_batch() -> super::TestResult {
                super::duplicate_batch_is_atomic_and_management_remains_usable($provider, 0).await
            }
        }
    )+};
}

for_each_backend! {
    memory => ::testkit::MemoryProvider::new(),
    durable => ::testkit::DurableProvider::temporary()?,
}
