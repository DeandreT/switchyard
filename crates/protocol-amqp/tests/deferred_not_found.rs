//! Deferred receive reports message absence after the broker commits cleanup.

use std::{
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};

use amqp::{
    ApplicationProperties, Array, Body, ClientConnection, ClientReceiver, ClientSender,
    ClientSession, Message, MessageId, OrderedMap, Outcome, Properties, Symbol, Value,
    decode_message,
};
use domain::{
    BrokerError, CommandKind, CommandOutcome, Delivery, EntityBinding, EntityIncarnationKind,
    EntityPath, MessageStatus, NamespaceName, ReceiveMode, SequenceNumber, Timestamp,
};
use protocol_amqp::{
    AmqpListener, Attachment, Broker, BrokerRejection, EntityAdmission, EntityMetadata,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type Decision = Result<CommandOutcome, BrokerRejection>;

#[derive(Clone)]
struct ControlledBroker(Arc<BrokerState>);

struct BrokerState {
    started: mpsc::Sender<()>,
    decision: Mutex<Option<oneshot::Receiver<Decision>>>,
}

impl Broker for ControlledBroker {
    async fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityAdmission>, BrokerRejection> {
        let entity = target.canonical_entity().expect("fixture target");
        Ok(self
            .entity_metadata(namespace.clone(), target)
            .await?
            .map(|metadata| EntityAdmission {
                metadata,
                binding: EntityBinding::new(
                    namespace,
                    entity.clone(),
                    entity,
                    EntityIncarnationKind::Queue,
                    1,
                )
                .expect("fixture binding"),
            }))
    }

    async fn submit_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Decision {
        self.submit(binding.namespace().clone(), entity, kind).await
    }

    async fn rules_fenced(
        &self,
        binding: EntityBinding,
        topic: EntityPath,
        subscription: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        self.rules(binding.namespace().clone(), topic, subscription)
            .await
    }

    async fn rules(
        &self,
        _namespace: NamespaceName,
        _topic: EntityPath,
        _subscription: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        Err(BrokerRejection::Unavailable("unexpected rule read".into()))
    }

    async fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        Ok((namespace.as_str() == "tenant"
            && matches!(target, Attachment::Queue(entity) if entity.as_str() == "orders"))
        .then_some(EntityMetadata::Queue(domain::QueueConfig::default())))
    }

    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Decision {
        assert_eq!(namespace.as_str(), "tenant");
        assert_eq!(entity.as_str(), "orders");
        let CommandKind::ReceiveDeferredHeld {
            sequences,
            mode,
            session,
            budget,
            ..
        } = kind
        else {
            panic!("unexpected command: {kind:?}");
        };
        assert_eq!(sequences, vec![SequenceNumber::new(7)]);
        assert_eq!(mode, ReceiveMode::ReceiveAndDelete);
        assert_eq!(session, None);
        assert!(budget.max_bytes > 0 && budget.max_bytes <= 4 * 1024 * 1024);
        assert_eq!(budget.per_message_overhead_bytes, 64);
        self.0
            .started
            .send(())
            .await
            .expect("the test is listening");
        let decision = self
            .0
            .decision
            .lock()
            .expect("decision mutex")
            .take()
            .expect("one deferred request");
        decision.await.expect("the test commits the decision")
    }

    async fn deliverable(&self, _namespace: &NamespaceName, _entity: &EntityPath) {
        std::future::pending::<()>().await;
    }
}

struct Fixture {
    _connection: ClientConnection,
    requests: Option<ClientSender>,
    responses: ClientReceiver,
    started: mpsc::Receiver<()>,
    decision: Option<oneshot::Sender<Decision>>,
    listener: JoinHandle<()>,
}

impl Fixture {
    async fn start() -> TestResult<Self> {
        let (started_tx, started) = mpsc::channel(1);
        let (decision_tx, decision_rx) = oneshot::channel();
        let broker = ControlledBroker(Arc::new(BrokerState {
            started: started_tx,
            decision: Mutex::new(Some(decision_rx)),
        }));
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?;
        let listener = tokio::spawn(async move {
            let _ = AmqpListener::new(broker, NamespaceName::new("tenant").expect("namespace"))
                .serve(socket)
                .await;
        });
        let mut connection = ClientConnection::builder()
            .container_id("deferred-absence-client")
            .open(&format!("amqp://{address}"))
            .await?;
        let mut session = ClientSession::begin(&mut connection).await?;
        let responses = ClientReceiver::builder()
            .name("deferred-responses")
            .source("orders/$management")
            .target("deferred-replies")
            .attach(&mut session)
            .await?;
        let requests =
            ClientSender::attach(&mut session, "deferred-requests", "orders/$management").await?;
        Ok(Self {
            _connection: connection,
            requests: Some(requests),
            responses,
            started,
            decision: Some(decision_tx),
            listener,
        })
    }

    fn send_request(&mut self) -> JoinHandle<Result<Outcome, amqp::EngineError>> {
        let mut requests = self.requests.take().expect("one request");
        let mut body = OrderedMap::new();
        body.insert(
            Value::String(protocol_amqp::SEQUENCE_NUMBERS.to_owned()),
            Value::Array(Array::from(vec![Value::Long(7)])),
        );
        body.insert(
            Value::String(protocol_amqp::RECEIVER_SETTLE_MODE.to_owned()),
            Value::Uint(0),
        );
        let mut application_properties = ApplicationProperties::default();
        application_properties.insert(
            protocol_amqp::OPERATION_PROPERTY,
            protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
        );
        application_properties.insert(protocol_amqp::TRACKING_ID_PROPERTY, "deferred-trace");
        let request = Message::builder()
            .properties(Properties {
                message_id: Some(MessageId::Ulong(11)),
                reply_to: Some("deferred-replies".to_owned()),
                ..Properties::default()
            })
            .application_properties(application_properties)
            .body(Body::Value(Value::Map(body)))
            .build();
        tokio::spawn(async move { requests.send(request).await })
    }

    async fn wait_for_command(&mut self) -> TestResult {
        tokio::time::timeout(Duration::from_secs(2), self.started.recv())
            .await?
            .expect("the command reached the broker");
        Ok(())
    }

    fn commit(&mut self, decision: Decision) {
        self.decision
            .take()
            .expect("one decision")
            .send(decision)
            .expect("the management request still awaits the broker");
    }

    async fn response(&mut self) -> TestResult<Message> {
        let delivery =
            tokio::time::timeout(Duration::from_secs(2), self.responses.recv()).await??;
        let message = delivery.message().clone();
        self.responses.accept(&delivery).await?;
        assert_eq!(
            message
                .properties
                .as_ref()
                .and_then(|properties| properties.correlation_id.as_ref()),
            Some(&MessageId::Ulong(11))
        );
        assert_eq!(
            message
                .application_properties
                .as_ref()
                .and_then(|properties| { properties.get(protocol_amqp::TRACKING_ID_PROPERTY) }),
            Some(&Value::String("deferred-trace".to_owned()))
        );
        Ok(message)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

fn assert_response(message: &Message, status: i32, condition: Option<&str>) {
    let properties = message
        .application_properties
        .as_ref()
        .expect("management response properties");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_deferred_result_reports_message_absence_after_the_commit() -> TestResult {
    let mut fixture = Fixture::start().await?;
    let sent = fixture.send_request();
    fixture.wait_for_command().await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), fixture.responses.recv())
            .await
            .is_err(),
        "no response may precede the command's committed cleanup"
    );
    fixture.commit(Ok(CommandOutcome::DeferredReceived(Vec::new())));
    assert!(matches!(sent.await??, Outcome::Accepted(_)));
    let response = fixture.response().await?;
    assert_response(&response, 404, Some("com.microsoft:message-not-found"));
    assert_eq!(response.body, Body::Value(Value::Null));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_message_is_distinct_from_a_missing_queue() -> TestResult {
    for (error, condition) in [
        (
            BrokerError::MessageNotFound {
                sequence: SequenceNumber::new(7),
            },
            "com.microsoft:message-not-found",
        ),
        (
            BrokerError::MessageNotDeferred {
                sequence: SequenceNumber::new(7),
            },
            "com.microsoft:message-not-found",
        ),
        (BrokerError::QueueNotFound, "amqp:not-found"),
    ] {
        let mut fixture = Fixture::start().await?;
        let sent = fixture.send_request();
        fixture.wait_for_command().await?;
        fixture.commit(Err(BrokerRejection::Refused(error)));
        assert!(matches!(sent.await??, Outcome::Accepted(_)));
        let response = fixture.response().await?;
        assert_response(&response, 404, Some(condition));
        assert_eq!(response.body, Body::Value(Value::Null));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_nonempty_deferred_result_remains_a_successful_message_response() -> TestResult {
    let mut fixture = Fixture::start().await?;
    let sent = fixture.send_request();
    fixture.wait_for_command().await?;
    fixture.commit(Ok(CommandOutcome::DeferredReceived(vec![Delivery {
        sequence: SequenceNumber::new(7),
        message_id: "deferred-message".to_owned(),
        body: b"preserved payload".to_vec(),
        enqueued_at: Timestamp::from_millis(1_000),
        expires_at: None,
        time_to_live_millis: None,
        envelope: None,
        delivery_count: 1,
        status: MessageStatus::Deferred,
        scheduled_enqueue_time: None,
        lock: None,
        session_id: None,
        dead_letter: None,
    }])));
    assert!(matches!(sent.await??, Outcome::Accepted(_)));
    let response = fixture.response().await?;
    assert_response(&response, 200, None);
    let Body::Value(Value::Map(body)) = &response.body else {
        panic!("a successful deferred response carries a map");
    };
    let Some(Value::List(messages)) = body.get(&Value::String(protocol_amqp::MESSAGES.to_owned()))
    else {
        panic!("a successful deferred response carries messages");
    };
    assert_eq!(messages.len(), 1);
    let Value::Map(entry) = &messages[0] else {
        panic!("the response entry must be a map");
    };
    let Some(Value::Binary(encoded)) = entry.get(&Value::String(protocol_amqp::MESSAGE.to_owned()))
    else {
        panic!("the response entry must contain an encoded message");
    };
    assert_eq!(
        decode_message(encoded.as_ref())?.body,
        Body::Data(vec![b"preserved payload".to_vec().into()])
    );
    Ok(())
}
