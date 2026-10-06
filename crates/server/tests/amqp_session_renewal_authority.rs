//! Message renewal retains the original direct or deferred session authority.

use amqp::{
    ApplicationProperties, Array, Body, ClientConnection, ClientReceiver, ClientSender,
    ClientSession, FilterSet, Message, MessageId, OrderedMap, Outcome, Properties,
    ReceiverSettleMode, Source, Symbol, Uuid, Value,
};
use domain::{
    BrokerError, CommandKind, CommandOutcome, EntityPath, MessageBody, MessageEnvelope,
    MessageValue, NamespaceName, QueueConfig, ReceiveMode, SequenceNumber, SessionHold, SessionId,
    StateMachine,
};
use futures_util::FutureExt;
use protocol_amqp::{Attachment, BrokerRejection, EntityMetadata};
use server::{Broker, BrokerHandle, LocalProposer, ManualClock};
use std::{
    collections::BTreeMap,
    error::Error,
    future::Future,
    panic::{AssertUnwindSafe, resume_unwind},
    sync::{Arc, Mutex},
    time::Duration,
};
use storage::{StateStore, StoreSnapshot};
use testkit::StoreProvider;
use tokio::{
    net::TcpListener,
    sync::Notify,
    task::{JoinError, JoinHandle},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type Decision = (CommandKind, Result<CommandOutcome, BrokerRejection>);
const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct RecordedBroker {
    inner: BrokerHandle,
    decisions: Arc<Mutex<Vec<Decision>>>,
    changed: Arc<Notify>,
}

impl protocol_amqp::Broker for RecordedBroker {
    fn receive_fenced_owned(
        &self,
        submission: protocol_amqp::OwnedReceiveSubmission,
    ) -> impl Future<Output = Result<Option<domain::Delivery>, protocol_amqp::ReceiveSubmitError>>
    + Send
    + 'static {
        protocol_amqp::Broker::receive_fenced_owned(&self.inner, submission)
    }

    fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> impl Future<Output = Result<Option<protocol_amqp::EntityAdmission>, BrokerRejection>> + Send
    {
        protocol_amqp::Broker::bind(&self.inner, namespace, target)
    }

    async fn submit_fenced(
        &self,
        binding: domain::EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        let recorded = matches!(kind, CommandKind::RenewLockHeld { .. }).then(|| kind.clone());
        let result = protocol_amqp::Broker::submit_fenced(&self.inner, binding, entity, kind).await;
        if let Some(kind) = recorded {
            self.decisions
                .lock()
                .expect("decision records")
                .push((kind, result.clone()));
        }
        self.changed.notify_waiters();
        result
    }

    fn rules_fenced(
        &self,
        binding: domain::EntityBinding,
        topic: EntityPath,
        subscription: domain::SubscriptionName,
    ) -> impl Future<Output = Result<Vec<domain::RuleDefinition>, BrokerRejection>> + Send {
        protocol_amqp::Broker::rules_fenced(&self.inner, binding, topic, subscription)
    }

    fn rules(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: domain::SubscriptionName,
    ) -> impl Future<Output = Result<Vec<domain::RuleDefinition>, BrokerRejection>> + Send {
        protocol_amqp::Broker::rules(&self.inner, namespace, topic, subscription)
    }

    fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> impl Future<Output = Result<Option<EntityMetadata>, BrokerRejection>> + Send {
        protocol_amqp::Broker::entity_metadata(&self.inner, namespace, target)
    }

    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        let result = protocol_amqp::Broker::submit(&self.inner, namespace, entity, kind).await;
        self.changed.notify_waiters();
        result
    }

    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        protocol_amqp::Broker::deliverable(&self.inner, namespace, entity)
    }
}

struct Node<P: StoreProvider> {
    store: P::Store,
    namespace: NamespaceName,
    entity: EntityPath,
    address: String,
    clock: ManualClock,
    recorded: RecordedBroker,
    broker: Broker,
    listener: JoinHandle<std::io::Result<()>>,
    provider: P,
}

impl<P: StoreProvider> Node<P> {
    async fn start(provider: P) -> TestResult<Self> {
        let store = provider.open()?;
        let clock = ManualClock::at(1_000);
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        broker.handle().submit_blocking(
            namespace.clone(),
            entity.clone(),
            CommandKind::CreateQueue {
                config: QueueConfig {
                    requires_session: true,
                    lock_duration_millis: 1_000,
                    ..QueueConfig::default()
                },
            },
        )?;
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?.to_string();
        let recorded = RecordedBroker {
            inner: broker.handle(),
            decisions: Arc::default(),
            changed: Arc::default(),
        };
        let listener_broker = recorded.clone();
        let listener_namespace = namespace.clone();
        let listener = tokio::spawn(async move {
            protocol_amqp::AmqpListener::new(listener_broker, listener_namespace)
                .serve(socket)
                .await
        });
        Ok(Self {
            store,
            namespace,
            entity,
            address,
            clock,
            recorded,
            broker,
            listener,
            provider,
        })
    }

    fn submit(&self, kind: CommandKind) -> TestResult<CommandOutcome> {
        Ok(self.broker.handle().submit_blocking(
            self.namespace.clone(),
            self.entity.clone(),
            kind,
        )?)
    }

    fn machine(&self) -> StateMachine<P::Store> {
        StateMachine::new(self.store.clone())
    }

    fn send(&self, session_id: &SessionId) -> TestResult<SequenceNumber> {
        match self.submit(CommandKind::SendEnvelope {
            message_id: session_id.as_str().to_owned(),
            body: vec![1, 2, 3],
            time_to_live_millis: None,
            session_id: Some(session_id.clone()),
            envelope: Box::new(MessageEnvelope {
                body: MessageBody::Data(vec![vec![1, 2, 3]]),
                application_properties: BTreeMap::from([(
                    "original".into(),
                    MessageValue::String("preserved".into()),
                )]),
                ..MessageEnvelope::default()
            }),
        })? {
            CommandOutcome::Sent { sequence } => Ok(sequence),
            other => panic!("expected send, got {other:?}"),
        }
    }

    fn hold(&self, session_id: &SessionId) -> TestResult<SessionHold> {
        let record = self
            .machine()
            .session(&self.namespace, &self.entity, session_id)?
            .expect("session record");
        Ok(SessionHold::new(
            session_id.clone(),
            record.lock.expect("session lock").token,
        ))
    }

    async fn decision(&self, index: usize) -> Decision {
        loop {
            let notified = self.recorded.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(row) = self
                .recorded
                .decisions
                .lock()
                .expect("decision records")
                .get(index)
                .cloned()
            {
                return row;
            }
            notified.await;
        }
    }

    async fn released(&self, session_id: &SessionId) -> TestResult {
        loop {
            let notified = self.recorded.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .machine()
                .session(&self.namespace, &self.entity, session_id)?
                .is_some_and(|record| record.lock.is_none())
            {
                return Ok(());
            }
            notified.await;
        }
    }

    async fn stop(self) -> Result<std::io::Result<()>, JoinError> {
        self.listener.abort();
        let joined = self.listener.await;
        drop(self.broker);
        drop(self.provider);
        joined
    }
}

async fn receiving(
    session: &mut ClientSession,
    name: &str,
    id: &SessionId,
) -> TestResult<ClientReceiver> {
    let mut filter = FilterSet::default();
    filter.insert(
        Symbol::from(protocol_amqp::SESSION_FILTER),
        Value::String(id.as_str().to_owned()),
    );
    Ok(ClientReceiver::builder()
        .name(name)
        .source(Source::builder().address("orders").filter(filter).build())
        .receiver_settle_mode(ReceiverSettleMode::Second)
        .attach(session)
        .await?)
}

fn key(name: &str) -> Value {
    Value::String(name.to_owned())
}

fn token_uuid(token: domain::LockToken) -> Uuid {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&token.as_u64().to_be_bytes());
    Uuid::from(bytes)
}

fn assert_status(message: &Message, expected: u16, condition: Option<&str>) {
    let properties = message
        .application_properties
        .as_ref()
        .expect("response properties");
    assert_eq!(
        properties.get(protocol_amqp::STATUS_CODE_PROPERTY),
        Some(&Value::Int(i32::from(expected)))
    );
    assert_eq!(
        properties.get(protocol_amqp::ERROR_CONDITION_PROPERTY),
        condition
            .map(|value| Value::Symbol(Symbol::from(value)))
            .as_ref()
    );
}

struct Management {
    requests: ClientSender,
    responses: ClientReceiver,
    next_id: u64,
}

impl Management {
    async fn attach(session: &mut ClientSession) -> TestResult<Self> {
        let responses = ClientReceiver::builder()
            .name("authority-replies")
            .source("orders/$management")
            .target("authority-reply-address")
            .attach(session)
            .await?;
        let requests =
            ClientSender::attach(session, "authority-requests", "orders/$management").await?;
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
        let id = format!("authority-{}", self.next_id);
        let mut properties = ApplicationProperties::default();
        properties.insert(protocol_amqp::OPERATION_PROPERTY, operation);
        properties.insert(protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY, "owner");
        let request = Message::builder()
            .properties(Properties {
                message_id: Some(id.clone().into()),
                reply_to: Some("authority-reply-address".into()),
                ..Properties::default()
            })
            .application_properties(properties)
            .body(Body::Value(Value::Map(body)))
            .build();
        assert!(matches!(
            self.requests.send(request).await?,
            Outcome::Accepted(_)
        ));
        let response = self.responses.recv().await?;
        assert_eq!(
            response
                .message()
                .properties
                .as_ref()
                .and_then(|properties| properties.correlation_id.as_ref()),
            Some(&MessageId::String(id))
        );
        let message = response.message().clone();
        self.responses.accept(&response).await?;
        Ok(message)
    }
}

async fn live_session_delivery_renewal_returns_original_token_expiration<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut connection = None;
    let observed = AssertUnwindSafe(timeout(DEADLINE, async {
        let id = SessionId::new("A")?;
        let sequence = node.send(&id)?;
        connection = Some(ClientConnection::builder().container_id("renew-live").open(&format!("amqp://{}", node.address)).await?);
        let mut session = connection.as_mut().expect("connection").begin().await?;
        let mut receiver = receiving(&mut session, "owner", &id).await?;
        let delivery = receiver.recv().await?;
        let hold = node.hold(&id)?;
        let original = node.machine().message(&node.namespace, &node.entity, sequence)?.expect("locked record");
        let domain::MessageState::Locked { token, locked_until } = &original.state else { panic!("message lock") };
        let (token, locked_until) = (*token, *locked_until);
        assert_eq!(locked_until, domain::Timestamp::from_millis(2_000));
        let before_session = node.machine().session(&node.namespace, &node.entity, &id)?;
        let mut management = Management::attach(&mut session).await?;
        node.clock.set(1_100);
        let mut body = OrderedMap::new();
        body.insert(key(protocol_amqp::LOCK_TOKENS), Value::Array(Array::from(vec![Value::Uuid(token_uuid(token))])));
        let response = management.request(protocol_amqp::RENEW_LOCK_OPERATION, body).await?;
        assert_status(&response, 200, None);
        let Body::Value(Value::Map(ref body)) = response.body else { panic!("response map") };
        assert!(matches!(body.get(&key(protocol_amqp::EXPIRATIONS)), Some(Value::Array(values))
            if matches!(values.as_slice(), [Value::Timestamp(until)] if until.milliseconds() == 2_100)));
        let (kind, result) = node.decision(0).await;
        assert!(matches!(kind, CommandKind::RenewLockHeld { sequence: actual, lock_token: actual_token, session: Some(ref original_hold), lock_duration_millis: None }
            if actual == sequence && actual_token == token && original_hold == &hold));
        assert_eq!(result, Ok(CommandOutcome::LockRenewed { locked_until: domain::Timestamp::from_millis(2_100) }));
        let mut renewed = node.machine().message(&node.namespace, &node.entity, sequence)?.expect("renewed record");
        assert_eq!(renewed.state, domain::MessageState::Locked { token, locked_until: domain::Timestamp::from_millis(2_100) });
        renewed.state = original.state.clone();
        assert_eq!(renewed, original);
        assert_eq!(node.machine().session(&node.namespace, &node.entity, &id)?, before_session);
        receiver.accept(&delivery).await?;
        receiver.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })).catch_unwind().await;
    finish(node, connection, observed).await
}

async fn deferred_renewal_refuses_same_name_reaccepted_receiver<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut connection = None;
    let observed = AssertUnwindSafe(timeout(DEADLINE, async {
        let id = SessionId::new("A")?;
        let sequence = node.send(&id)?;
        let CommandOutcome::SessionAccepted(Some(accepted)) = node.submit(CommandKind::AcceptSession {
            session_id: Some(id.clone()), lock_duration_millis: None })? else { panic!("accepted session") };
        let CommandOutcome::Received(Some(delivery)) = node.submit(CommandKind::Receive {
            mode: ReceiveMode::PeekLock, lock_duration_millis: Some(10_000), session: Some(accepted.hold()) })?
            else { panic!("received message") };
        node.submit(CommandKind::Defer { sequence, lock_token: delivery.lock.expect("message lock").token })?;
        node.submit(CommandKind::ReleaseSession { session: accepted.hold() })?;
        connection = Some(ClientConnection::builder().container_id("renew-reaccept").open(&format!("amqp://{}", node.address)).await?);
        let mut session = connection.as_mut().expect("connection").begin().await?;
        let original = receiving(&mut session, "owner", &id).await?;
        let hold = node.hold(&id)?;
        let mut management = Management::attach(&mut session).await?;
        let mut body = OrderedMap::new();
        body.insert(key(protocol_amqp::SEQUENCE_NUMBERS), Value::Array(Array::from(vec![Value::Long(sequence.as_u64() as i64)])));
        body.insert(key(protocol_amqp::RECEIVER_SETTLE_MODE), Value::Uint(1));
        body.insert(key(protocol_amqp::SESSION_ID), Value::String(id.as_str().into()));
        let response = management.request(protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION, body).await?;
        assert_status(&response, 200, None);
        let Body::Value(Value::Map(ref body)) = response.body else { panic!("response map") };
        let Some(Value::List(messages)) = body.get(&key(protocol_amqp::MESSAGES)) else { panic!("message list") };
        assert_eq!(messages.len(), 1);
        let Value::Map(ref entry) = messages[0] else { panic!("message entry") };
        let Some(Value::Uuid(token)) = entry.get(&key(protocol_amqp::LOCK_TOKEN)) else { panic!("message token") };
        let token = token.clone();
        original.close().await?;
        node.released(&id).await?;
        let replacement = receiving(&mut session, "owner", &id).await?;
        let current = node.hold(&id)?;
        assert_ne!(current.token, hold.token);
        let before: StoreSnapshot = node.store.snapshot()?;
        for index in 0..2 {
            let mut body = OrderedMap::new();
            body.insert(key(protocol_amqp::LOCK_TOKENS), Value::Array(Array::from(vec![Value::Uuid(token.clone())])));
            let response = management.request(protocol_amqp::RENEW_LOCK_OPERATION, body).await?;
            assert_status(&response, 410, Some(protocol_amqp::SESSION_LOCK_LOST));
            let (kind, result) = node.decision(index).await;
            assert!(matches!(kind, CommandKind::RenewLockHeld { sequence: actual, session: Some(ref original_hold), .. } if actual == sequence && original_hold == &hold));
            assert_eq!(result, Err(BrokerRejection::Refused(BrokerError::SessionLockNotHeld { session_id: id.clone() })));
            assert_eq!(node.store.snapshot()?, before);
        }
        replacement.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })).catch_unwind().await;
    finish(node, connection, observed).await
}

type Observed =
    Result<Result<TestResult, tokio::time::error::Elapsed>, Box<dyn std::any::Any + Send>>;

async fn finish<P: StoreProvider>(
    node: Node<P>,
    mut connection: Option<ClientConnection>,
    observed: Observed,
) -> TestResult {
    let closing = AssertUnwindSafe(timeout(DEADLINE, async {
        if let Some(connection) = connection.as_mut() {
            connection.close().await?;
        }
        Ok::<(), Box<dyn Error>>(())
    }))
    .catch_unwind()
    .await;
    let joined = node.stop().await;
    match observed {
        Err(payload) => resume_unwind(payload),
        Ok(result) => result??,
    }
    match closing {
        Err(payload) => resume_unwind(payload),
        Ok(result) => result??,
    }
    match joined {
        Ok(result) => result?,
        Err(error) if error.is_cancelled() => (),
        Err(error) => return Err(Box::new(error)),
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::new()).await })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?).await })+ }
    };
}

for_each_backend!(
    live_session_delivery_renewal_returns_original_token_expiration,
    deferred_renewal_refuses_same_name_reaccepted_receiver,
);
