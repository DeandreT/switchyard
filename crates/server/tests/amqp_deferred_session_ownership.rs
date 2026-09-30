//! Deferred receive requires this connection's live session ownership.

use std::{error::Error, time::Duration};

use amqp::{
    ApplicationProperties, Array, Body, ClientConnection, ClientReceiver, ClientSender,
    ClientSession, FilterSet, Message, MessageId, OrderedMap, Outcome, Properties, Source, Symbol,
    Uuid, Value, decode_message,
};
use domain::{
    CommandKind, CommandOutcome, Delivery, EntityPath, LockToken, NamespaceName, QueueConfig,
    ReceiveMode, SequenceNumber, SessionHold, SessionId, StateMachine,
};
use server::{Broker, LocalProposer, ManualClock};
use storage::{StateStore, StoreSnapshot};
use testkit::StoreProvider;
use tokio::{net::TcpListener, task::JoinHandle};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct Node<P: StoreProvider> {
    broker: Broker,
    store: P::Store,
    clock: ManualClock,
    address: String,
    listener: JoinHandle<()>,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    async fn start(provider: P) -> TestResult<Self> {
        let store = provider.open()?;
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        let handle = broker.handle();
        handle.submit_blocking(
            namespace.clone(),
            entity.clone(),
            CommandKind::CreateQueue {
                config: QueueConfig {
                    requires_session: true,
                    ..QueueConfig::default()
                },
            },
        )?;
        for (index, id) in ["session-a", "session-b"].into_iter().enumerate() {
            let session_id = SessionId::new(id)?;
            let CommandOutcome::Sent { sequence } = handle.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::Send {
                    message_id: format!("deferred-{id}"),
                    body: format!("payload-{id}").into_bytes(),
                    time_to_live_millis: Some(120_000),
                    session_id: Some(session_id.clone()),
                },
            )?
            else {
                panic!("the fixture message was not sent");
            };
            assert_eq!(sequence, SequenceNumber::new(index as u64 + 1));
            let CommandOutcome::SessionAccepted(Some(accepted)) = handle.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::AcceptSession {
                    session_id: Some(session_id),
                    lock_duration_millis: None,
                },
            )?
            else {
                panic!("the fixture session was not accepted");
            };
            let CommandOutcome::Received(Some(delivery)) = handle.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: Some(accepted.hold()),
                },
            )?
            else {
                panic!("the fixture message was not locked");
            };
            assert_eq!(delivery.sequence, sequence);
            handle.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::Defer {
                    sequence,
                    lock_token: delivery.lock.expect("fixture message lock").token,
                },
            )?;
            handle.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::ReleaseSession {
                    session: accepted.hold(),
                },
            )?;
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
            store,
            clock,
            address,
            listener,
            _provider: provider,
        })
    }

    async fn connect(&self) -> TestResult<ClientConnection> {
        Ok(ClientConnection::builder()
            .container_id("deferred-session-client")
            .open(&format!("amqp://{}", self.address))
            .await?)
    }

    fn submit(&self, kind: CommandKind) -> TestResult<CommandOutcome> {
        Ok(self.broker.handle().submit_blocking(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            kind,
        )?)
    }

    fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.store.snapshot()?)
    }

    fn peek(&self, id: &str) -> TestResult<Vec<Delivery>> {
        let CommandOutcome::Peeked(deliveries) = self.submit(CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 100,
            session_id: Some(SessionId::new(id)?),
        })?
        else {
            panic!("the fixture expected a peek outcome");
        };
        Ok(deliveries)
    }
}

impl<P: StoreProvider> Drop for Node<P> {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

async fn session_receiver(
    session: &mut ClientSession,
    name: &str,
    id: &str,
) -> TestResult<ClientReceiver> {
    let mut filter = FilterSet::default();
    filter.insert(
        Symbol::from(protocol_amqp::SESSION_FILTER),
        Value::String(id.to_owned()),
    );
    Ok(ClientReceiver::builder()
        .name(name)
        .source(Source::builder().address("orders").filter(filter).build())
        .attach(session)
        .await?)
}

struct Management {
    requests: ClientSender,
    responses: ClientReceiver,
    next_id: u64,
}

impl Management {
    async fn attach(session: &mut ClientSession) -> TestResult<Self> {
        let responses = ClientReceiver::builder()
            .name("ownership-responses")
            .source("orders/$management")
            .target("ownership-replies")
            .attach(session)
            .await?;
        let requests =
            ClientSender::attach(session, "ownership-requests", "orders/$management").await?;
        Ok(Self {
            requests,
            responses,
            next_id: 0,
        })
    }

    async fn receive_deferred(
        &mut self,
        sequence: i64,
        mode: u32,
        session_id: Option<Value>,
        associated_link: Option<&str>,
    ) -> TestResult<Message> {
        self.next_id += 1;
        let message_id = MessageId::Ulong(self.next_id);
        let mut body = OrderedMap::new();
        body.insert(
            key(protocol_amqp::SEQUENCE_NUMBERS),
            Value::Array(Array::from(vec![Value::Long(sequence)])),
        );
        body.insert(key(protocol_amqp::RECEIVER_SETTLE_MODE), Value::Uint(mode));
        if let Some(session_id) = session_id {
            body.insert(key(protocol_amqp::SESSION_ID), session_id);
        }
        let mut application_properties = ApplicationProperties::default();
        application_properties.insert(
            protocol_amqp::OPERATION_PROPERTY,
            protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
        );
        if let Some(link_name) = associated_link {
            application_properties.insert(protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY, link_name);
        }
        let request = Message::builder()
            .properties(Properties {
                message_id: Some(message_id.clone()),
                reply_to: Some("ownership-replies".to_owned()),
                ..Properties::default()
            })
            .application_properties(application_properties)
            .body(Body::Value(Value::Map(body)))
            .build();
        assert!(matches!(
            self.requests.send(request).await?,
            Outcome::Accepted(_)
        ));
        let delivery =
            tokio::time::timeout(Duration::from_secs(3), self.responses.recv()).await??;
        let response = delivery.message().clone();
        self.responses.accept(&delivery).await?;
        assert_eq!(
            response
                .properties
                .as_ref()
                .and_then(|properties| properties.correlation_id.as_ref()),
            Some(&message_id)
        );
        Ok(response)
    }
}

fn key(name: &str) -> Value {
    Value::String(name.to_owned())
}

fn session_value(id: &str) -> Option<Value> {
    Some(Value::String(id.to_owned()))
}

fn assert_status(message: &Message, status: i32, condition: Option<&str>) {
    let properties = message
        .application_properties
        .as_ref()
        .expect("response properties");
    assert_eq!(
        properties.get(protocol_amqp::STATUS_CODE_PROPERTY),
        Some(&Value::Int(status))
    );
    assert_eq!(
        properties.get(protocol_amqp::ERROR_CONDITION_PROPERTY),
        condition
            .map(|condition| Value::Symbol(Symbol::from(condition)))
            .as_ref()
    );
}

fn message_entry(response: &Message) -> TestResult<(Message, Option<Uuid>)> {
    let Body::Value(Value::Map(body)) = &response.body else {
        panic!("the reply must contain a map");
    };
    let Some(Value::List(messages)) = body.get(&key(protocol_amqp::MESSAGES)) else {
        panic!("the reply must contain messages");
    };
    let [Value::Map(entry)] = messages.as_slice() else {
        panic!("the reply must contain exactly one message");
    };
    let Some(Value::Binary(encoded)) = entry.get(&key(protocol_amqp::MESSAGE)) else {
        panic!("the reply must contain encoded message data");
    };
    let lock = match entry.get(&key(protocol_amqp::LOCK_TOKEN)) {
        Some(Value::Uuid(lock)) => Some(lock.clone()),
        None => None,
        other => panic!("unexpected message lock: {other:?}"),
    };
    Ok((decode_message(encoded)?, lock))
}

fn token(number: u64) -> Uuid {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&number.to_be_bytes());
    bytes.into()
}

fn assert_payload(message: &Message, id: &str) {
    let Body::Data(body) = &message.body else {
        panic!("the deferred payload must retain its body shape");
    };
    assert_eq!(body.len(), 1);
    assert_eq!(body[0].as_ref(), format!("payload-{id}").as_bytes());
    assert_eq!(
        message
            .properties
            .as_ref()
            .and_then(|properties| properties.message_id.as_ref()),
        Some(&MessageId::String(format!("deferred-{id}")))
    );
    assert_eq!(message.header.as_ref().expect("header").delivery_count, 1);
}

async fn another_connection_cannot_take_a_held_session<P: StoreProvider>(
    provider: P,
    mode: u32,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut owner_connection = node.connect().await?;
    let mut owner_session = ClientSession::begin(&mut owner_connection).await?;
    let _owner = session_receiver(&mut owner_session, "exact-owner-name", "session-a").await?;
    let mut owner_management = Management::attach(&mut owner_session).await?;

    let mut rival_connection = node.connect().await?;
    let mut rival_session = ClientSession::begin(&mut rival_connection).await?;
    let _rival = session_receiver(&mut rival_session, "rival-b", "session-b").await?;
    let mut rival_management = Management::attach(&mut rival_session).await?;
    let before = node.snapshot()?;

    for associated in [Some("exact-owner-name"), Some("rival-b")] {
        let response = rival_management
            .receive_deferred(1, mode, session_value("session-a"), associated)
            .await?;
        assert_status(&response, 410, Some(protocol_amqp::SESSION_LOCK_LOST));
        assert_eq!(node.snapshot()?, before);
    }
    let response = rival_management
        .receive_deferred(1, mode, session_value("session-a"), None)
        .await?;
    assert_status(&response, 400, None);
    assert_eq!(node.snapshot()?, before);

    for malformed in [Value::Null, Value::Int(7), Value::String(String::new())] {
        let response = owner_management
            .receive_deferred(1, mode, Some(malformed), Some("exact-owner-name"))
            .await?;
        assert_status(&response, 400, None);
        assert_eq!(node.snapshot()?, before);
    }
    let response = owner_management
        .receive_deferred(1, mode, None, Some("exact-owner-name"))
        .await?;
    assert_status(&response, 400, Some(protocol_amqp::NOT_ALLOWED));
    assert_eq!(node.snapshot()?, before);

    let response = owner_management
        .receive_deferred(
            1,
            mode,
            session_value("session-a"),
            Some("exact-owner-name"),
        )
        .await?;
    assert_status(&response, 200, None);
    let (message, lock) = message_entry(&response)?;
    assert_payload(&message, "session-a");
    assert_eq!(lock, (mode == 1).then(|| token(7)));
    let after = node.peek("session-a")?;
    if mode == 1 {
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].delivery_count, 2);
    } else {
        assert!(after.is_empty());
    }
    let response = rival_management
        .receive_deferred(2, 1, session_value("session-b"), Some("rival-b"))
        .await?;
    assert_status(&response, 200, None);
    let (message, lock) = message_entry(&response)?;
    assert_payload(&message, "session-b");
    assert_eq!(lock, Some(token(if mode == 1 { 8 } else { 7 })));
    owner_connection.close().await?;
    rival_connection.close().await?;
    Ok(())
}

async fn another_connection_cannot_peek_lock_a_held_session<P: StoreProvider>(
    provider: P,
) -> TestResult {
    another_connection_cannot_take_a_held_session(provider, 1).await
}

async fn another_connection_cannot_delete_a_held_session<P: StoreProvider>(
    provider: P,
) -> TestResult {
    another_connection_cannot_take_a_held_session(provider, 0).await
}

async fn expired_session_ownership_cannot_trigger_deferred_expiry_cleanup<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut connection = node.connect().await?;
    let mut session = ClientSession::begin(&mut connection).await?;
    let _owner = session_receiver(&mut session, "expired-owner", "session-a").await?;
    let mut management = Management::attach(&mut session).await?;
    node.clock.advance(121_000);
    let before = node.snapshot()?;
    let response = management
        .receive_deferred(1, 0, session_value("session-a"), Some("expired-owner"))
        .await?;
    assert_status(&response, 410, Some(protocol_amqp::SESSION_LOCK_LOST));
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.peek("session-a")?.len(), 1);

    let mut replacement_connection = node.connect().await?;
    let mut replacement_session = ClientSession::begin(&mut replacement_connection).await?;
    let _replacement =
        session_receiver(&mut replacement_session, "replacement-owner", "session-a").await?;
    let mut replacement_management = Management::attach(&mut replacement_session).await?;
    let response = replacement_management
        .receive_deferred(1, 0, session_value("session-a"), Some("replacement-owner"))
        .await?;
    assert_status(&response, 404, Some(protocol_amqp::MESSAGE_NOT_FOUND));
    assert!(node.peek("session-a")?.is_empty());
    assert_eq!(node.peek("session-b")?.len(), 1);
    connection.close().await?;
    replacement_connection.close().await?;
    Ok(())
}

async fn a_released_and_replaced_session_token_cannot_receive_deferred<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut connection = node.connect().await?;
    let mut session = ClientSession::begin(&mut connection).await?;
    let _owner = session_receiver(&mut session, "stale-owner", "session-a").await?;
    let mut management = Management::attach(&mut session).await?;
    let old = SessionHold::new(SessionId::new("session-a")?, LockToken::new(5));
    assert_eq!(
        node.submit(CommandKind::ReleaseSession { session: old })?,
        CommandOutcome::SessionReleased
    );
    let CommandOutcome::SessionAccepted(Some(replacement)) =
        node.submit(CommandKind::AcceptSession {
            session_id: Some(SessionId::new("session-a")?),
            lock_duration_millis: None,
        })?
    else {
        panic!("the replacement session must be accepted");
    };
    assert_eq!(replacement.lock.token, LockToken::new(6));
    let before = node.snapshot()?;
    let response = management
        .receive_deferred(1, 1, session_value("session-a"), Some("stale-owner"))
        .await?;
    assert_status(&response, 410, Some(protocol_amqp::SESSION_LOCK_LOST));
    assert_eq!(node.snapshot()?, before);
    node.submit(CommandKind::ReleaseSession {
        session: replacement.hold(),
    })?;

    let mut replacement_connection = node.connect().await?;
    let mut replacement_session = ClientSession::begin(&mut replacement_connection).await?;
    let _replacement =
        session_receiver(&mut replacement_session, "current-owner", "session-a").await?;
    let mut replacement_management = Management::attach(&mut replacement_session).await?;
    let response = replacement_management
        .receive_deferred(1, 1, session_value("session-a"), Some("current-owner"))
        .await?;
    assert_status(&response, 200, None);
    let (message, lock) = message_entry(&response)?;
    assert_payload(&message, "session-a");
    assert_eq!(lock, Some(token(8)));
    connection.close().await?;
    replacement_connection.close().await?;
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                super::$case(::testkit::MemoryProvider::new()).await
            }
        )+ }
        mod durable { $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                super::$case(::testkit::DurableProvider::temporary()?).await
            }
        )+ }
    };
}

for_each_backend! {
    another_connection_cannot_peek_lock_a_held_session,
    another_connection_cannot_delete_a_held_session,
    expired_session_ownership_cannot_trigger_deferred_expiry_cleanup,
    a_released_and_replaced_session_token_cannot_receive_deferred,
}
