//! Application-property updates accompany settlement through both stores.

use std::{error::Error, time::Duration};

use amqp::{
    Annotations, ApplicationProperties, Array, Body, ClientConnection as Connection,
    ClientReceiver as Receiver, ClientSender as Sender, ClientSession as Session, Described,
    Descriptor, Error as AmqpError, ErrorCondition, Fields, Header, Message, MessageId, Modified,
    OrderedMap, Outcome, Properties, Symbol, Uuid, Value, decode_message,
};
use domain::{CommandKind, QueueConfig, StateMachine};
use server::{Broker, LocalProposer, ManualClock};
use testkit::StoreProvider;
use tokio::{net::TcpListener, task::JoinHandle};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct Node<P> {
    broker: Broker,
    address: String,
    listener: JoinHandle<()>,
    provider: P,
}

impl<P: StoreProvider> Node<P> {
    async fn start(provider: P, config: Option<QueueConfig>) -> TestResult<Self> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(provider.open()?),
            ManualClock::at(1_000),
        ));
        let namespace = domain::NamespaceName::new("tenant")?;
        if let Some(config) = config {
            broker.handle().submit_blocking(
                namespace.clone(),
                domain::EntityPath::new("orders")?,
                CommandKind::CreateQueue { config },
            )?;
        }
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?.to_string();
        let handle = broker.handle();
        let listener = tokio::spawn(async move {
            let _ = protocol_amqp::AmqpListener::new(handle, namespace)
                .serve(socket)
                .await;
        });
        Ok(Self {
            broker,
            address,
            listener,
            provider,
        })
    }

    async fn connect(&self) -> TestResult<Connection> {
        Ok(Connection::builder()
            .container_id("settlement-client")
            .open(&format!("amqp://{}", self.address))
            .await?)
    }

    async fn stop(self) -> P {
        self.listener.abort();
        let _ = self.listener.await;
        drop(self.broker);
        self.provider
    }
}

struct Management {
    requests: Sender,
    responses: Receiver,
    next_id: u64,
}

impl Management {
    async fn attach(session: &mut Session) -> TestResult<Self> {
        let responses = Receiver::builder()
            .name("settlement-management-responses")
            .source("orders/$management")
            .target("settlement-management-replies")
            .attach(session)
            .await?;
        let requests = Sender::attach(
            session,
            "settlement-management-requests",
            "orders/$management",
        )
        .await?;
        Ok(Self {
            requests,
            responses,
            next_id: 0,
        })
    }

    async fn request(
        &mut self,
        operation: &str,
        link_name: Option<&str>,
        body: OrderedMap<Value, Value>,
    ) -> TestResult<Message> {
        self.next_id += 1;
        let id = format!("settlement-request-{}", self.next_id);
        let mut application_properties = ApplicationProperties::default();
        application_properties.insert(protocol_amqp::OPERATION_PROPERTY, operation);
        if let Some(link_name) = link_name {
            application_properties.insert(protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY, link_name);
        }
        let request = Message::builder()
            .properties(Properties {
                message_id: Some(id.clone().into()),
                reply_to: Some("settlement-management-replies".to_owned()),
                ..Properties::default()
            })
            .application_properties(application_properties)
            .body(Body::Value(Value::Map(body)))
            .build();
        assert!(matches!(
            self.requests.send(request).await?,
            Outcome::Accepted(_)
        ));
        let response =
            tokio::time::timeout(Duration::from_secs(2), self.responses.recv()).await??;
        assert_eq!(
            response
                .message()
                .properties
                .as_ref()
                .and_then(|properties| properties.correlation_id.as_ref()),
            Some(&MessageId::String(id)),
        );
        let message = response.message().clone();
        self.responses.accept(&response).await?;
        Ok(message)
    }

    async fn peek(&mut self) -> TestResult<Vec<(Message, Option<Uuid>)>> {
        let mut body = OrderedMap::new();
        body.insert(key(protocol_amqp::FROM_SEQUENCE_NUMBER), Value::Long(0));
        body.insert(key(protocol_amqp::MESSAGE_COUNT), Value::Int(100));
        let response = self
            .request(protocol_amqp::PEEK_MESSAGE_OPERATION, None, body)
            .await?;
        assert_status(&response, 200, None);
        entries(&response)
    }

    async fn receive_deferred(
        &mut self,
        sequence: Value,
        link_name: &str,
    ) -> TestResult<(Message, Uuid)> {
        let mut body = OrderedMap::new();
        body.insert(
            key(protocol_amqp::SEQUENCE_NUMBERS),
            Value::Array(Array::from(vec![sequence])),
        );
        body.insert(key(protocol_amqp::RECEIVER_SETTLE_MODE), Value::Uint(1));
        let response = self
            .request(
                protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
                Some(link_name),
                body,
            )
            .await?;
        assert_status(&response, 200, None);
        let mut messages = entries(&response)?;
        assert_eq!(messages.len(), 1);
        let (message, token) = messages.pop().expect("one deferred message");
        Ok((message, token.expect("deferred receive grants a lock")))
    }

    async fn settle(
        &mut self,
        link_name: &str,
        token: Uuid,
        disposition: &str,
        properties: Option<Value>,
        reason: Option<&str>,
        description: Option<&str>,
    ) -> TestResult<Message> {
        let mut body = OrderedMap::new();
        body.insert(
            key(protocol_amqp::LOCK_TOKENS),
            Value::Array(Array::from(vec![Value::Uuid(token)])),
        );
        body.insert(
            key(protocol_amqp::DISPOSITION_STATUS),
            Value::String(disposition.to_owned()),
        );
        if let Some(properties) = properties {
            body.insert(key(protocol_amqp::PROPERTIES_TO_MODIFY), properties);
        }
        if let Some(reason) = reason {
            body.insert(
                key(protocol_amqp::DEAD_LETTER_REASON),
                Value::String(reason.to_owned()),
            );
        }
        if let Some(description) = description {
            body.insert(
                key(protocol_amqp::DEAD_LETTER_DESCRIPTION),
                Value::String(description.to_owned()),
            );
        }
        self.request(
            protocol_amqp::UPDATE_DISPOSITION_OPERATION,
            Some(link_name),
            body,
        )
        .await
    }
}

fn key(name: &str) -> Value {
    Value::String(name.to_owned())
}

fn token(number: u64) -> Uuid {
    // Fresh entity lock tokens are deterministic and monotonically allocated.
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&number.to_be_bytes());
    bytes.into()
}

fn annotation<'a>(message: &'a Message, name: &str) -> &'a Value {
    message
        .message_annotations
        .as_ref()
        .and_then(|annotations| annotations.get(Symbol::from(name)))
        .expect("annotation exists")
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
    if let Some(condition) = condition {
        assert_eq!(
            properties.get(protocol_amqp::ERROR_CONDITION_PROPERTY),
            Some(&Value::Symbol(Symbol::from(condition)))
        );
    }
}

fn entries(response: &Message) -> TestResult<Vec<(Message, Option<Uuid>)>> {
    let Body::Value(Value::Map(body)) = &response.body else {
        panic!("management response is a value map");
    };
    let Some(Value::List(messages)) = body.get(&key(protocol_amqp::MESSAGES)) else {
        panic!("management response contains messages");
    };
    messages
        .iter()
        .map(|entry| {
            let Value::Map(entry) = entry else {
                panic!("message entry is a map");
            };
            let Some(Value::Binary(encoded)) = entry.get(&key(protocol_amqp::MESSAGE)) else {
                panic!("message entry contains encoded data");
            };
            let token = match entry.get(&key(protocol_amqp::LOCK_TOKEN)) {
                Some(Value::Uuid(token)) => Some(token.clone()),
                None => None,
                other => panic!("unexpected lock token: {other:?}"),
            };
            Ok((decode_message(encoded)?, token))
        })
        .collect()
}

fn source_message() -> Message {
    let mut properties = ApplicationProperties::default();
    properties.insert("keep", "original");
    properties.insert("attempts", Value::Int(1));
    properties.insert("nullable", "replace me");
    let mut annotations = Annotations::new();
    annotations.insert(
        Symbol::from("producer:annotation"),
        Value::String("keep annotation".to_owned()),
    );
    let mut footer = Annotations::new();
    footer.insert(17_u64, Value::Binary(vec![1, 0, 255].into()));
    Message {
        header: Some(Header {
            durable: true,
            priority: 6,
            ..Header::default()
        }),
        properties: Some(Properties {
            message_id: Some("settlement-source".into()),
            correlation_id: Some(MessageId::Ulong(17)),
            subject: Some("original subject".to_owned()),
            content_type: Some(Symbol::from("application/octet-stream")),
            ..Properties::default()
        }),
        application_properties: Some(properties),
        message_annotations: Some(annotations),
        footer: Some(footer),
        body: Body::Data(vec![vec![0, 1].into(), vec![128, 255].into()]),
        ..Message::default()
    }
}

fn updates() -> Fields {
    let mut fields = Fields::new();
    fields.insert(Symbol::from("attempts"), Value::Uint(2));
    fields.insert(Symbol::from("nullable"), Value::Null);
    fields.insert(
        Symbol::from("elapsed"),
        Value::Described(Box::new(Described {
            descriptor: Descriptor::Name(Symbol::from("com.microsoft:timespan")),
            value: Value::Long(123_456),
        })),
    );
    fields.insert(
        Symbol::from("bits"),
        Value::Double(f64::from_bits(0xfff8_0123_4567_89ab).into()),
    );
    fields
}

fn property_map(fields: &Fields) -> Value {
    Value::Map(
        fields
            .iter()
            .map(|(name, value)| (key(name.as_str()), value.clone()))
            .collect(),
    )
}

fn apply_updates(message: &mut Message, fields: &Fields) {
    let properties = message
        .application_properties
        .get_or_insert_with(ApplicationProperties::default);
    for (name, value) in fields.iter() {
        properties.insert(name.as_str(), value.clone());
    }
}

fn assert_preserved(actual: &Message, expected: &Message) {
    assert_eq!(actual.body, expected.body);
    let actual_properties = actual.properties.as_ref().expect("message properties");
    let expected_properties = expected.properties.as_ref().expect("source properties");
    assert_eq!(actual_properties.message_id, expected_properties.message_id);
    assert_eq!(
        actual_properties.correlation_id,
        expected_properties.correlation_id
    );
    assert_eq!(actual_properties.subject, expected_properties.subject);
    assert_eq!(
        actual_properties.content_type,
        expected_properties.content_type
    );
    let actual_header = actual.header.as_ref().expect("message header");
    assert!(actual_header.durable);
    assert_eq!(actual_header.priority, 6);
    assert!(!actual_header.first_acquirer);
    assert_eq!(
        annotation(actual, "producer:annotation"),
        annotation(expected, "producer:annotation")
    );
    assert_eq!(actual.footer, expected.footer);
    let actual_application = actual
        .application_properties
        .as_ref()
        .expect("application properties");
    for (name, expected) in expected
        .application_properties
        .as_ref()
        .expect("expected properties")
        .0
        .iter()
    {
        let actual = actual_application
            .get(name)
            .expect("application property survives");
        match (actual, expected) {
            (Value::Double(actual), Value::Double(expected)) => {
                assert_eq!(actual.0.to_bits(), expected.0.to_bits())
            }
            _ => assert_eq!(actual, expected, "property {name}"),
        }
    }
    assert!(
        actual
            .message_annotations
            .as_ref()
            .expect("message annotations")
            .get(Symbol::from("attempts"))
            .is_none()
    );
}

async fn wait_deferred(management: &mut Management, expected: &Message) -> TestResult<Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let messages = management.peek().await?;
        assert_eq!(messages.len(), 1);
        let message = &messages[0].0;
        if annotation(message, protocol_amqp::MESSAGE_STATE_ANNOTATION) == &Value::Int(1) {
            assert_preserved(message, expected);
            return Ok(annotation(message, "x-opt-sequence-number").clone());
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "defer did not settle"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn link_abandon_updates_survive_restart<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, Some(QueueConfig::default())).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "abandon-sender", "orders").await?;
    let mut expected = source_message();
    assert!(matches!(
        sender.send(expected.clone()).await?,
        Outcome::Accepted(_)
    ));
    let mut receiver = Receiver::attach(&mut session, "abandon-receiver", "orders").await?;
    let first = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    let fields = updates();
    receiver
        .modify(
            &first,
            Modified {
                message_annotations: Some(fields.clone()),
                ..Modified::default()
            },
        )
        .await?;
    apply_updates(&mut expected, &fields);
    let again = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    assert_preserved(again.message(), &expected);
    assert_eq!(
        again
            .message()
            .header
            .as_ref()
            .expect("redelivery header")
            .delivery_count,
        2
    );
    // Defer is a deterministic barrier against the client's automatic credit
    // acquiring the ready message again while the broker is being restarted.
    receiver
        .modify(
            &again,
            Modified {
                undeliverable_here: Some(true),
                ..Modified::default()
            },
        )
        .await?;
    let mut management = Management::attach(&mut session).await?;
    let sequence = wait_deferred(&mut management, &expected).await?;
    receiver.close().await?;
    sender.close().await?;
    session.end().await?;
    connection.close().await?;
    drop(management);
    let provider = node.stop().await;

    let node = Node::start(provider, None).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let receiver = Receiver::attach(&mut session, "restart-receiver", "orders").await?;
    let mut management = Management::attach(&mut session).await?;
    let (message, token) = management
        .receive_deferred(sequence, "restart-receiver")
        .await?;
    assert_preserved(&message, &expected);
    assert_status(
        &management
            .settle("restart-receiver", token, "completed", None, None, None)
            .await?,
        200,
        None,
    );
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    node.stop().await;
    Ok(())
}

async fn link_defer_updates_are_visible_to_management<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, Some(QueueConfig::default())).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "defer-sender", "orders").await?;
    let mut expected = source_message();
    assert!(matches!(
        sender.send(expected.clone()).await?,
        Outcome::Accepted(_)
    ));
    let mut receiver = Receiver::attach(&mut session, "defer-receiver", "orders").await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    let fields = updates();
    receiver
        .modify(
            &delivery,
            Modified {
                undeliverable_here: Some(true),
                message_annotations: Some(fields.clone()),
                ..Modified::default()
            },
        )
        .await?;
    apply_updates(&mut expected, &fields);
    let mut management = Management::attach(&mut session).await?;
    let sequence = wait_deferred(&mut management, &expected).await?;
    let (message, token) = management
        .receive_deferred(sequence, "defer-receiver")
        .await?;
    assert_preserved(&message, &expected);
    assert_status(
        &management
            .settle("defer-receiver", token, "completed", None, None, None)
            .await?,
        200,
        None,
    );
    sender.close().await?;
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    node.stop().await;
    Ok(())
}

async fn sdk_rejection_info_updates_dead_letters<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, Some(QueueConfig::default())).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "rejection-sender", "orders").await?;
    let mut expected = source_message();
    assert!(matches!(
        sender.send(expected.clone()).await?,
        Outcome::Accepted(_)
    ));
    let mut receiver = Receiver::attach(&mut session, "rejection-receiver", "orders").await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    let mut fields = updates();
    fields.insert(
        Symbol::from(protocol_amqp::DEAD_LETTER_REASON_PROPERTY),
        Value::String("invalid-order".to_owned()),
    );
    fields.insert(
        Symbol::from(protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY),
        Value::String("the order is incomplete".to_owned()),
    );
    receiver
        .reject(
            &delivery,
            Some(AmqpError {
                condition: ErrorCondition::Custom(Symbol::from("com.microsoft:dead-letter")),
                description: Some(
                    "generic diagnostic must not override the DLQ description".to_owned(),
                ),
                info: Some(fields.clone()),
            }),
        )
        .await?;
    apply_updates(&mut expected, &fields);
    let mut dead_letters = Receiver::attach(
        &mut session,
        "dead-letter-receiver",
        "orders/$deadletterqueue",
    )
    .await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), dead_letters.recv()).await??;
    assert_preserved(delivery.message(), &expected);
    assert!(
        delivery
            .message()
            .header
            .as_ref()
            .expect("dead letter header")
            .ttl
            .is_none()
    );
    dead_letters.accept(&delivery).await?;
    sender.close().await?;
    receiver.close().await?;
    dead_letters.close().await?;
    session.end().await?;
    connection.close().await?;
    node.stop().await;
    Ok(())
}

async fn management_settlements_merge_properties_and_dead_letter_fields<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, Some(QueueConfig::default())).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "management-sender", "orders").await?;
    let mut expected = source_message();
    assert!(matches!(
        sender.send(expected.clone()).await?,
        Outcome::Accepted(_)
    ));
    let mut receiver = Receiver::attach(&mut session, "managed-receiver", "orders").await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    receiver
        .modify(
            &delivery,
            Modified {
                undeliverable_here: Some(true),
                ..Modified::default()
            },
        )
        .await?;
    let mut management = Management::attach(&mut session).await?;
    let sequence = wait_deferred(&mut management, &expected).await?;
    let (_, held) = management
        .receive_deferred(sequence, "managed-receiver")
        .await?;
    let fields = updates();
    assert_status(
        &management
            .settle(
                "managed-receiver",
                held,
                "abandoned",
                Some(property_map(&fields)),
                None,
                None,
            )
            .await?,
        200,
        None,
    );
    apply_updates(&mut expected, &fields);
    let again = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    assert_preserved(again.message(), &expected);
    receiver
        .modify(
            &again,
            Modified {
                undeliverable_here: Some(true),
                ..Modified::default()
            },
        )
        .await?;
    let sequence = wait_deferred(&mut management, &expected).await?;
    let (_, held) = management
        .receive_deferred(sequence, "managed-receiver")
        .await?;
    let mut deferred = Fields::new();
    deferred.insert(Symbol::from("stage"), Value::String("deferred".to_owned()));
    assert_status(
        &management
            .settle(
                "managed-receiver",
                held,
                "defered",
                Some(property_map(&deferred)),
                None,
                None,
            )
            .await?,
        200,
        None,
    );
    apply_updates(&mut expected, &deferred);
    let sequence = wait_deferred(&mut management, &expected).await?;
    let (message, held) = management
        .receive_deferred(sequence, "managed-receiver")
        .await?;
    assert_preserved(&message, &expected);
    let mut dead_letter = Fields::new();
    dead_letter.insert(
        Symbol::from(protocol_amqp::DEAD_LETTER_REASON_PROPERTY),
        Value::String("property reason".to_owned()),
    );
    dead_letter.insert(
        Symbol::from(protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY),
        Value::String("property description".to_owned()),
    );
    dead_letter.insert(
        Symbol::from("stage"),
        Value::String("dead lettered".to_owned()),
    );
    assert_status(
        &management
            .settle(
                "managed-receiver",
                held,
                "suspended",
                Some(property_map(&dead_letter)),
                None,
                None,
            )
            .await?,
        200,
        None,
    );
    apply_updates(&mut expected, &dead_letter);
    let mut dead_letters = Receiver::attach(
        &mut session,
        "managed-dead-letters",
        "orders/$deadletterqueue",
    )
    .await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), dead_letters.recv()).await??;
    assert_preserved(delivery.message(), &expected);
    dead_letters.accept(&delivery).await?;

    let mut expected = source_message();
    expected.properties.as_mut().expect("properties").message_id =
        Some("explicit-dead-letter".into());
    assert!(matches!(
        sender.send(expected.clone()).await?,
        Outcome::Accepted(_)
    ));
    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    receiver
        .modify(
            &delivery,
            Modified {
                undeliverable_here: Some(true),
                ..Modified::default()
            },
        )
        .await?;
    let sequence = wait_deferred(&mut management, &expected).await?;
    let (_, held) = management
        .receive_deferred(sequence, "managed-receiver")
        .await?;
    assert_status(
        &management
            .settle(
                "managed-receiver",
                held,
                "suspended",
                Some(property_map(&dead_letter)),
                Some("explicit reason"),
                Some("explicit description"),
            )
            .await?,
        200,
        None,
    );
    apply_updates(&mut expected, &dead_letter);
    let properties = expected
        .application_properties
        .as_mut()
        .expect("properties");
    properties.insert(
        protocol_amqp::DEAD_LETTER_REASON_PROPERTY,
        "explicit reason",
    );
    properties.insert(
        protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY,
        "explicit description",
    );
    let delivery = tokio::time::timeout(Duration::from_secs(2), dead_letters.recv()).await??;
    assert_preserved(delivery.message(), &expected);
    dead_letters.accept(&delivery).await?;
    sender.close().await?;
    receiver.close().await?;
    dead_letters.close().await?;
    session.end().await?;
    connection.close().await?;
    node.stop().await;
    Ok(())
}

async fn invalid_management_updates_leave_the_lock_and_payload_unchanged<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(
        provider,
        Some(QueueConfig {
            max_message_bytes: 1_024,
            ..QueueConfig::default()
        }),
    )
    .await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "invalid-update-sender", "orders").await?;
    let expected = source_message();
    assert!(matches!(
        sender.send(expected.clone()).await?,
        Outcome::Accepted(_)
    ));
    let mut receiver = Receiver::attach(&mut session, "invalid-update-receiver", "orders").await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    let sequence = annotation(delivery.message(), "x-opt-sequence-number").clone();
    let mut management = Management::attach(&mut session).await?;
    let original = management.peek().await?.pop().expect("held message").0;
    let mut compound = Fields::new();
    compound.insert(Symbol::from("attempts"), Value::Uint(9));
    compound.insert(Symbol::from("invalid"), Value::List(vec![Value::Null]));
    let mut oversized = Fields::new();
    oversized.insert(Symbol::from("attempts"), Value::Uint(9));
    oversized.insert(
        Symbol::from("too big"),
        Value::Binary(vec![7; 2_048].into()),
    );
    let mut malformed_reason = Fields::new();
    malformed_reason.insert(Symbol::from("attempts"), Value::Uint(9));
    malformed_reason.insert(
        Symbol::from(protocol_amqp::DEAD_LETTER_REASON_PROPERTY),
        Value::Null,
    );
    for (disposition, properties, status, condition) in [
        (
            "abandoned",
            property_map(&compound),
            400,
            protocol_amqp::INVALID_FIELD,
        ),
        (
            "defered",
            property_map(&oversized),
            403,
            protocol_amqp::MESSAGE_SIZE_EXCEEDED,
        ),
        (
            "suspended",
            property_map(&malformed_reason),
            400,
            protocol_amqp::INVALID_FIELD,
        ),
        ("abandoned", Value::Null, 400, protocol_amqp::INVALID_FIELD),
    ] {
        let response = management
            .settle(
                "invalid-update-receiver",
                token(1),
                disposition,
                Some(properties),
                None,
                None,
            )
            .await?;
        assert_status(&response, status, Some(condition));
        let peeked = management.peek().await?.pop().expect("message remains").0;
        assert_preserved(&peeked, &expected);
        assert_eq!(
            peeked.application_properties,
            original.application_properties
        );
        assert_eq!(peeked.body, original.body);
        assert_eq!(annotation(&peeked, "x-opt-sequence-number"), &sequence);
        assert_eq!(peeked.message_annotations, original.message_annotations);
        assert_eq!(
            peeked.header.as_ref().expect("peek header").delivery_count,
            1
        );
        assert_eq!(
            annotation(&peeked, protocol_amqp::MESSAGE_STATE_ANNOTATION),
            &Value::Int(0)
        );
    }
    assert_status(
        &management
            .settle(
                "invalid-update-receiver",
                token(1),
                "completed",
                None,
                None,
                None,
            )
            .await?,
        200,
        None,
    );
    sender.close().await?;
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    node.stop().await;
    Ok(())
}

macro_rules! both_backends {
    ($($case:ident),+ $(,)?) => {
        mod memory {
            $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                super::$case(testkit::MemoryProvider::new()).await
            })+
        }
        mod durable {
            $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                super::$case(testkit::DurableProvider::temporary()?).await
            })+
        }
    };
}

both_backends!(
    link_abandon_updates_survive_restart,
    link_defer_updates_are_visible_to_management,
    sdk_rejection_info_updates_dead_letters,
    management_settlements_merge_properties_and_dead_letter_fields,
    invalid_management_updates_leave_the_lock_and_payload_unchanged,
);
