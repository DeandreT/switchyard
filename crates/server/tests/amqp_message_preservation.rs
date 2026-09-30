//! Message preservation through the socket, command log, and both stores.

use std::{error::Error, time::Duration};

use amqp::{
    Annotations, ApplicationProperties, Array, Body, ClientConnection as Connection,
    ClientReceiver as Receiver, ClientSender as Sender, ClientSession as Session, Described,
    Descriptor, FilterSet, Header, Message, MessageId, Modified, OrderedMap, Outcome, Properties,
    SenderSettleMode, Source, Symbol, Uuid, Value, decode_message, encode_message,
};
use domain::{CommandKind, QueueConfig, StateMachine};
use server::{Broker, LocalProposer, ManualClock, TimerWorker};
use testkit::StoreProvider;
use tokio::{net::TcpListener, task::JoinHandle};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct Node<P> {
    broker: Broker,
    address: String,
    clock: ManualClock,
    listener: JoinHandle<()>,
    provider: P,
}

impl<P: StoreProvider> Node<P> {
    async fn start(provider: P, config: Option<QueueConfig>) -> TestResult<Self> {
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(provider.open()?),
            clock.clone(),
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
            clock,
            listener,
            provider,
        })
    }

    async fn connect(&self) -> TestResult<Connection> {
        Ok(Connection::builder()
            .container_id("preservation-client")
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
        Self::attach_entity(session, "orders").await
    }

    async fn attach_entity(session: &mut Session, entity: &str) -> TestResult<Self> {
        let address = format!("{entity}/$management");
        let responses = Receiver::builder()
            .name("preservation-management-responses")
            .source(address.clone())
            .target("preservation-management-replies")
            .attach(session)
            .await?;
        let requests = Sender::attach(session, "preservation-management-requests", address).await?;
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
        let id = format!("preservation-request-{}", self.next_id);
        let mut application_properties = ApplicationProperties::default();
        application_properties.insert(protocol_amqp::OPERATION_PROPERTY, operation);
        if let Some(link_name) = link_name {
            application_properties.insert(protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY, link_name);
        }
        let request = Message::builder()
            .properties(Properties {
                message_id: Some(id.clone().into()),
                reply_to: Some(String::from("preservation-management-replies")),
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
        assert_eq!(
            response
                .message()
                .application_properties
                .as_ref()
                .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY)),
            Some(&Value::Int(200)),
        );
        let message = response.message().clone();
        self.responses.accept(&response).await?;
        Ok(message)
    }

    async fn peek(&mut self) -> TestResult<Message> {
        let mut body = OrderedMap::new();
        body.insert(
            Value::String(protocol_amqp::FROM_SEQUENCE_NUMBER.to_owned()),
            Value::Long(0),
        );
        body.insert(
            Value::String(protocol_amqp::MESSAGE_COUNT.to_owned()),
            Value::Int(100),
        );
        self.request(protocol_amqp::PEEK_MESSAGE_OPERATION, None, body)
            .await
    }
}

fn delivery_count(message: &Message) -> u32 {
    message
        .header
        .as_ref()
        .expect("broker delivery header")
        .delivery_count
}

fn special_values() -> Vec<(&'static str, Value)> {
    vec![
        ("null", Value::Null),
        ("boolean", Value::Bool(true)),
        ("byte", Value::Byte(-3)),
        ("ubyte", Value::Ubyte(250)),
        ("short", Value::Short(-30_000)),
        ("ushort", Value::Ushort(60_000)),
        ("int", Value::Int(-2_000_000)),
        ("uint", Value::Uint(4_000_000_000)),
        ("long", Value::Long(i64::MIN + 10)),
        ("ulong", Value::Ulong(u64::MAX - 10)),
        ("decimal32", Value::Decimal32([1, 2, 3, 4].into())),
        (
            "decimal64",
            Value::Decimal64([1, 2, 3, 4, 5, 6, 7, 8].into()),
        ),
        ("decimal128", Value::Decimal128([13; 16].into())),
        (
            "float-nan",
            Value::Float(f32::from_bits(0x7fc0_1234).into()),
        ),
        (
            "double-nan",
            Value::Double(f64::from_bits(0x7ff8_0000_0000_1234).into()),
        ),
        ("negative-zero", Value::Double((-0.0).into())),
        ("char", Value::Char('q')),
        ("timestamp", Value::Timestamp((-123_456_i64).into())),
        ("uuid", Value::Uuid(Uuid::from([7; 16]))),
        ("binary", Value::Binary(vec![0, 1, 128, 255].into())),
        ("string", Value::String(String::from("preserved text"))),
        ("symbol", Value::Symbol(Symbol::from("preserved-symbol"))),
        (
            "timespan",
            Value::Described(Box::new(Described {
                descriptor: Descriptor::Name(Symbol::from("com.microsoft:timespan")),
                value: Value::Long(12_345_678),
            })),
        ),
    ]
}

fn rich_message(body: Body) -> Message {
    let data_body = matches!(body, Body::Data(_));
    let mut application_properties = ApplicationProperties::default();
    for (key, value) in special_values() {
        application_properties.insert(key, value);
    }
    application_properties.insert(protocol_amqp::DEAD_LETTER_REASON_PROPERTY, "forged-reason");
    application_properties.insert(
        protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY,
        "forged-description",
    );
    let mut annotations = Annotations::new();
    annotations.insert(
        Symbol::from("x-opt-user-label"),
        Value::String(String::from("persisted annotation")),
    );
    annotations.insert(9_001_u64, Value::Long(73));
    let mut footer = Annotations::new();
    footer.insert(
        Symbol::from("x-opt-checksum"),
        Value::Binary(vec![1, 3, 5].into()),
    );
    footer.insert(9_002_u64, Value::Ulong(17));
    let mut delivery_annotations = Annotations::new();
    delivery_annotations.insert(
        Symbol::from("x-opt-hop-only"),
        Value::String(String::from("must not be forwarded")),
    );
    Message {
        header: Some(Header {
            durable: true,
            priority: 7,
            ttl: Some(45_000),
            first_acquirer: true,
            delivery_count: 777,
        }),
        delivery_annotations: Some(delivery_annotations),
        message_annotations: Some(annotations),
        properties: Some(Properties {
            message_id: Some(MessageId::Ulong(17)),
            user_id: Some(vec![2, 4, 6, 8].into()),
            to: Some(String::from("logical-destination")),
            subject: Some(String::from("preserved subject")),
            reply_to: Some(String::from("reply-queue")),
            correlation_id: Some(MessageId::Binary(vec![0, 12, 255].into())),
            content_type: data_body.then(|| Symbol::from("application/octet-stream")),
            content_encoding: data_body.then(|| Symbol::from("gzip")),
            absolute_expiry_time: Some(43_766),
            creation_time: Some(-1_234),
            group_id: None,
            group_sequence: Some(51),
            reply_to_group_id: Some(String::from("reply-session")),
        }),
        application_properties: Some(application_properties),
        body,
        footer: Some(footer),
    }
}

fn body_wire(body: &Body) -> TestResult<Vec<u8>> {
    Ok(encode_message(
        &Message::builder().body(body.clone()).build(),
    )?)
}

fn assert_value_preserved(actual: &Value, expected: &Value) -> TestResult {
    assert_eq!(
        body_wire(&Body::Value(actual.clone()))?,
        body_wire(&Body::Value(expected.clone()))?,
        "value types and IEEE float bits must survive",
    );
    Ok(())
}

#[derive(Clone, Copy)]
enum Lifetime {
    Active(i64),
    Scheduled,
    DeadLetter,
}

fn assert_preserved(actual: &Message, expected: &Message, lifetime: Lifetime) -> TestResult {
    assert_eq!(body_wire(&actual.body)?, body_wire(&expected.body)?);
    let mut expected_properties = expected.properties.clone().unwrap_or_default();
    match lifetime {
        Lifetime::Active(enqueued_at) => {
            if let Some(ttl) = expected.header.as_ref().and_then(|header| header.ttl) {
                expected_properties.creation_time = Some(enqueued_at);
                expected_properties.absolute_expiry_time = Some(enqueued_at + i64::from(ttl));
            }
        }
        Lifetime::Scheduled => {}
        Lifetime::DeadLetter => {
            expected_properties.absolute_expiry_time = None;
            expected_properties.group_id = None;
        }
    }
    assert_eq!(
        actual.properties.clone().unwrap_or_default(),
        expected_properties,
    );
    if let Some(expected_header) = &expected.header {
        let actual_header = actual.header.as_ref().expect("delivery carries a header");
        assert_eq!(actual_header.durable, expected_header.durable);
        assert_eq!(actual_header.priority, expected_header.priority);
        assert!(
            !actual_header.first_acquirer,
            "producer cannot assert broker acquisition history"
        );
        let ttl = match lifetime {
            Lifetime::DeadLetter => None,
            _ => expected_header.ttl,
        };
        assert_eq!(actual_header.ttl, ttl);
        assert_ne!(
            actual_header.delivery_count, 777,
            "sender cannot forge broker delivery count"
        );
    }
    if let Some(expected_properties) = &expected.application_properties {
        let actual_properties = actual
            .application_properties
            .as_ref()
            .expect("application properties survive");
        for (key, value) in expected_properties.0.iter() {
            if [
                protocol_amqp::DEAD_LETTER_REASON_PROPERTY,
                protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY,
            ]
            .contains(&key.as_str())
            {
                if !matches!(lifetime, Lifetime::DeadLetter) {
                    assert_eq!(
                        actual_properties.get(key),
                        None,
                        "reserved dead-letter properties cannot be forged"
                    );
                }
                continue;
            }
            assert_value_preserved(
                actual_properties
                    .get(key)
                    .expect("application property survives"),
                value,
            )?;
        }
        let expected_len = expected_properties
            .0
            .iter()
            .filter(|(key, _)| {
                matches!(lifetime, Lifetime::DeadLetter)
                    || ![
                        protocol_amqp::DEAD_LETTER_REASON_PROPERTY,
                        protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY,
                    ]
                    .contains(&key.as_str())
            })
            .count();
        assert_eq!(actual_properties.0.len(), expected_len);
    }
    if let Some(expected_annotations) = &expected.message_annotations {
        let actual_annotations = actual
            .message_annotations
            .as_ref()
            .expect("message annotations survive");
        for (key, value) in expected_annotations.iter() {
            if let amqp::AnnotationKey::Symbol(symbol) = key
                && [
                    "x-opt-sequence-number",
                    "x-opt-enqueued-time",
                    "x-opt-locked-until",
                    protocol_amqp::MESSAGE_STATE_ANNOTATION,
                ]
                .contains(&symbol.as_str())
            {
                continue;
            }
            assert_value_preserved(
                actual_annotations
                    .get(key)
                    .expect("message annotation survives"),
                value,
            )?;
        }
    }
    match (&actual.footer, &expected.footer) {
        (Some(actual), Some(expected)) => {
            assert_eq!(actual.0.len(), expected.0.len());
            for (key, value) in expected.iter() {
                assert_value_preserved(actual.get(key).expect("footer field survives"), value)?;
            }
        }
        (None, None) => {}
        other => panic!("footer presence changed: {other:?}"),
    }
    assert!(
        actual.delivery_annotations.is_none(),
        "hop-specific annotations must not cross the broker"
    );
    Ok(())
}

fn annotation<'a>(message: &'a Message, name: &str) -> &'a Value {
    message
        .message_annotations
        .as_ref()
        .and_then(|annotations| annotations.get(Symbol::from(name)))
        .expect("broker annotation exists")
}

fn management_entries(response: &Message) -> TestResult<Vec<(Message, Option<Uuid>)>> {
    let Body::Value(Value::Map(body)) = &response.body else {
        panic!("management response carries a value map");
    };
    let Some(Value::List(entries)) = body.get(&Value::String(protocol_amqp::MESSAGES.to_owned()))
    else {
        panic!("management response carries encoded messages");
    };
    entries
        .iter()
        .map(|entry| {
            let Value::Map(entry) = entry else {
                panic!("management entry is a map");
            };
            let Some(Value::Binary(encoded)) =
                entry.get(&Value::String(protocol_amqp::MESSAGE.to_owned()))
            else {
                panic!("management message is binary");
            };
            let token = match entry.get(&Value::String(protocol_amqp::LOCK_TOKEN.to_owned())) {
                Some(Value::Uuid(token)) => Some(token.clone()),
                None => None,
                value => panic!("unexpected lock token {value:?}"),
            };
            Ok((decode_message(encoded)?, token))
        })
        .collect()
}

async fn each_body_and_special_property_survives_restart<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, Some(QueueConfig::default())).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "preservation-sender", "orders").await?;
    let mut value_map = OrderedMap::new();
    value_map.insert(
        Value::String(String::from("nested")),
        Value::List(vec![Value::Ulong(12), Value::Null]),
    );
    value_map.insert(
        Value::String(String::from("array")),
        Value::Array(Array::from(vec![Value::Uint(1), Value::Uint(2)])),
    );
    let bodies = vec![
        Body::Data(vec![
            vec![0, 1].into(),
            Vec::<u8>::new().into(),
            vec![128, 255].into(),
        ]),
        Body::Sequence(vec![
            vec![
                Value::Int(5),
                special_values().pop().expect("timespan value").1,
                Value::Double(f64::from_bits(0x7ff8_0000_0000_5678).into()),
            ],
            Vec::new(),
            vec![Value::String(String::from("last section"))],
        ]),
        Body::Value(Value::Map(value_map)),
        Body::Value(Value::Array(Array::from(vec![Value::Null, Value::Null]))),
        Body::Value(Value::Array(Array::from(vec![
            Value::List(Vec::new()),
            Value::List(vec![Value::Null]),
        ]))),
        Body::Value(Value::Array(Array::from(vec![
            Value::Array(Array::from(vec![Value::Uint(1), Value::Uint(2)])),
            Value::Array(Array::from(vec![Value::String(String::from("inner"))])),
        ]))),
        Body::Value(Value::Array(Array::from(vec![
            Value::Described(Box::new(Described {
                descriptor: Descriptor::Name(Symbol::from("x-opt-described-null")),
                value: Value::Null,
            })),
            Value::Described(Box::new(Described {
                descriptor: Descriptor::Name(Symbol::from("x-opt-described-null")),
                value: Value::Null,
            })),
        ]))),
        Body::Value(Value::Array(Array::from(vec![
            Value::Described(Box::new(Described {
                descriptor: Descriptor::Code(90_001),
                value: Value::Long(i64::MAX),
            })),
            Value::Described(Box::new(Described {
                descriptor: Descriptor::Code(90_001),
                value: Value::Long(-1),
            })),
        ]))),
        Body::Value(Value::Null),
        Body::Empty,
    ];
    let mut messages = Vec::new();
    for (index, body) in bodies.into_iter().enumerate() {
        let mut message = rich_message(body);
        message
            .properties
            .as_mut()
            .expect("rich properties")
            .message_id = Some(format!("body-{index}").into());
        assert!(matches!(
            sender.send(message.clone()).await?,
            Outcome::Accepted(_)
        ));
        messages.push(message);
    }
    sender.close().await?;
    session.end().await?;
    connection.close().await?;
    let provider = node.stop().await;
    let node = Node::start(provider, None).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut receiver = Receiver::attach(&mut session, "preservation-receiver", "orders").await?;
    for expected in messages {
        let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
        assert_preserved(delivery.message(), &expected, Lifetime::Active(1_000))?;
        receiver.accept(&delivery).await?;
    }
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    node.stop().await;
    Ok(())
}

async fn producer_creation_time_survives_without_a_finite_lifetime<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, Some(QueueConfig::default())).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "creation-time-sender", "orders").await?;
    let mut expected = rich_message(Body::Value(Value::String(String::from("no expiry"))));
    expected.header.as_mut().expect("rich header").ttl = None;
    expected
        .properties
        .as_mut()
        .expect("rich properties")
        .absolute_expiry_time = None;
    assert!(matches!(
        sender.send(expected.clone()).await?,
        Outcome::Accepted(_)
    ));
    let mut receiver = Receiver::attach(&mut session, "creation-time-receiver", "orders").await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    assert_preserved(delivery.message(), &expected, Lifetime::Active(1_000))?;
    assert_eq!(
        delivery
            .message()
            .properties
            .as_ref()
            .and_then(|properties| properties.creation_time),
        Some(-1_234),
    );
    receiver.accept(&delivery).await?;
    sender.close().await?;
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    node.stop().await;
    Ok(())
}

async fn typed_missing_and_empty_ids_remain_distinct<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, Some(QueueConfig::default())).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "typed-id-sender", "orders").await?;
    let mut receiver = Receiver::attach(&mut session, "typed-id-receiver", "orders").await?;
    for id in [
        Some(MessageId::Ulong(42)),
        Some(MessageId::String(String::from("42"))),
        Some(MessageId::Uuid(Uuid::from([11; 16]))),
        Some(MessageId::Binary(vec![0, 5, 255].into())),
        None,
        Some(MessageId::String(String::new())),
        Some(MessageId::Binary(Vec::<u8>::new().into())),
    ] {
        let expected = Message::builder()
            .properties(Properties {
                message_id: id.clone(),
                ..Properties::default()
            })
            .body(Body::Value(Value::String(String::from("typed-id"))))
            .build();
        assert!(matches!(
            sender.send(expected.clone()).await?,
            Outcome::Accepted(_)
        ));
        let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
        assert_eq!(
            delivery
                .message()
                .properties
                .as_ref()
                .and_then(|properties| properties.message_id.clone()),
            id
        );
        assert_preserved(delivery.message(), &expected, Lifetime::Active(1_000))?;
        receiver.accept(&delivery).await?;
    }
    let expected = Message::data(vec![9, 8, 7]);
    assert!(matches!(
        sender.send(expected.clone()).await?,
        Outcome::Accepted(_)
    ));
    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    assert_eq!(
        delivery
            .message()
            .properties
            .as_ref()
            .and_then(|properties| properties.message_id.as_ref()),
        None
    );
    receiver.accept(&delivery).await?;
    sender.close().await?;
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    node.stop().await;
    Ok(())
}

async fn redelivery_preserves_payload_and_advances_only_broker_fields<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, Some(QueueConfig::default())).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "redelivery-sender", "orders").await?;
    let expected = rich_message(Body::Sequence(vec![
        vec![Value::String(String::from("retry"))],
        vec![Value::Long(9)],
    ]));
    assert!(matches!(
        sender.send(expected.clone()).await?,
        Outcome::Accepted(_)
    ));
    let mut management = Management::attach(&mut session).await?;
    let peeked = management_entries(&management.peek().await?)?;
    assert_eq!(peeked.len(), 1);
    assert_eq!(delivery_count(&peeked[0].0), 0);
    let mut receiver = Receiver::attach(&mut session, "redelivery-receiver", "orders").await?;
    let first = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    assert_preserved(first.message(), &expected, Lifetime::Active(1_000))?;
    assert_eq!(delivery_count(first.message()), 0);
    let peeked = management_entries(&management.peek().await?)?;
    assert_eq!(delivery_count(&peeked[0].0), 1);
    let sequence = annotation(first.message(), "x-opt-sequence-number").clone();
    receiver
        .modify(
            &first,
            Modified {
                delivery_failed: Some(true),
                ..Modified::default()
            },
        )
        .await?;
    let again = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    assert_preserved(again.message(), &expected, Lifetime::Active(1_000))?;
    assert!(
        !again
            .message()
            .header
            .as_ref()
            .expect("redelivery header")
            .first_acquirer,
        "redelivery cannot claim to be the first acquisition"
    );
    assert_eq!(delivery_count(again.message()), 1);
    let peeked = management_entries(&management.peek().await?)?;
    assert_eq!(delivery_count(&peeked[0].0), 2);
    assert_eq!(
        annotation(again.message(), "x-opt-sequence-number"),
        &sequence
    );
    receiver.accept(&again).await?;
    sender.close().await?;
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    node.stop().await;
    Ok(())
}

async fn receive_and_delete_emits_zero_for_the_first_acquisition<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, Some(QueueConfig::default())).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut management = Management::attach(&mut session).await?;
    let mut sender = Sender::attach(&mut session, "delete-count-sender", "orders").await?;
    let expected = rich_message(Body::Value(Value::String("delete once".to_owned())));
    assert!(matches!(
        sender.send(expected.clone()).await?,
        Outcome::Accepted(_)
    ));
    let peeked = management_entries(&management.peek().await?)?;
    assert_eq!(peeked.len(), 1);
    assert_eq!(delivery_count(&peeked[0].0), 0);
    let mut receiver = Receiver::builder()
        .name("delete-count-receiver")
        .source("orders")
        .sender_settle_mode(SenderSettleMode::Settled)
        .attach(&mut session)
        .await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    assert_preserved(delivery.message(), &expected, Lifetime::Active(1_000))?;
    assert_eq!(delivery_count(delivery.message()), 0);
    assert!(management_entries(&management.peek().await?)?.is_empty());
    sender.close().await?;
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    node.stop().await;
    Ok(())
}

async fn deferred_management_returns_preserved_messages<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, Some(QueueConfig::default())).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "deferred-sender", "orders").await?;
    let expected = rich_message(Body::Value(Value::String(String::from("deferred value"))));
    assert!(matches!(
        sender.send(expected.clone()).await?,
        Outcome::Accepted(_)
    ));
    let link_name = "deferred-preservation-receiver";
    let mut receiver = Receiver::attach(&mut session, link_name, "orders").await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    let sequence = annotation(delivery.message(), "x-opt-sequence-number").clone();
    assert_eq!(delivery_count(delivery.message()), 0);
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
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let entries = management_entries(&management.peek().await?)?;
        assert_eq!(entries.len(), 1);
        if annotation(&entries[0].0, protocol_amqp::MESSAGE_STATE_ANNOTATION) == &Value::Int(1) {
            assert_preserved(&entries[0].0, &expected, Lifetime::Active(1_000))?;
            assert_eq!(delivery_count(&entries[0].0), 1);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "deferred settlement was not applied"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    receiver.close().await?;
    let link_name = "another-deferred-preservation-receiver";
    let receiver = Receiver::attach(&mut session, link_name, "orders").await?;
    let mut body = OrderedMap::new();
    body.insert(
        Value::String(protocol_amqp::SEQUENCE_NUMBERS.to_owned()),
        Value::Array(Array::from(vec![sequence])),
    );
    body.insert(
        Value::String(protocol_amqp::RECEIVER_SETTLE_MODE.to_owned()),
        Value::Uint(1),
    );
    let response = management
        .request(
            protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
            Some(link_name),
            body,
        )
        .await?;
    let entries = management_entries(&response)?;
    assert_eq!(entries.len(), 1);
    assert_preserved(&entries[0].0, &expected, Lifetime::Active(1_000))?;
    let header = entries[0]
        .0
        .header
        .as_ref()
        .expect("deferred delivery header");
    assert!(!header.first_acquirer);
    assert_eq!(header.delivery_count, 1);
    let peeked = management_entries(&management.peek().await?)?;
    assert_eq!(delivery_count(&peeked[0].0), 2);
    let token = entries[0]
        .1
        .clone()
        .expect("deferred receive grants a lock");
    let mut body = OrderedMap::new();
    body.insert(
        Value::String(protocol_amqp::LOCK_TOKENS.to_owned()),
        Value::Array(Array::from(vec![Value::Uuid(token)])),
    );
    body.insert(
        Value::String(protocol_amqp::DISPOSITION_STATUS.to_owned()),
        Value::String(String::from("completed")),
    );
    management
        .request(
            protocol_amqp::UPDATE_DISPOSITION_OPERATION,
            Some(link_name),
            body,
        )
        .await?;
    sender.close().await?;
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    node.stop().await;
    Ok(())
}

async fn scheduled_management_preserves_metadata_before_and_after_activation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, Some(QueueConfig::default())).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut management = Management::attach(&mut session).await?;
    let mut expected = rich_message(Body::Sequence(vec![
        vec![Value::Ulong(9)],
        vec![Value::Bool(false)],
    ]));
    expected
        .properties
        .as_mut()
        .expect("rich properties")
        .message_id = Some(MessageId::Binary(vec![3, 0, 5].into()));
    expected
        .message_annotations
        .as_mut()
        .expect("rich annotations")
        .insert(
            Symbol::from(protocol_amqp::SCHEDULED_ENQUEUE_TIME_ANNOTATION),
            Value::Timestamp(2_000_i64.into()),
        );
    let mut entry = OrderedMap::new();
    entry.insert(
        Value::String(protocol_amqp::MESSAGE.to_owned()),
        Value::Binary(encode_message(&expected)?.into()),
    );
    entry.insert(Value::String(String::from("message-id")), Value::Null);
    let mut body = OrderedMap::new();
    body.insert(
        Value::String(protocol_amqp::MESSAGES.to_owned()),
        Value::List(vec![Value::Map(entry)]),
    );
    let scheduled = management
        .request(protocol_amqp::SCHEDULE_MESSAGE_OPERATION, None, body)
        .await?;
    let Body::Value(Value::Map(body)) = scheduled.body else {
        panic!("scheduled response is a map");
    };
    let Some(Value::Array(handles)) =
        body.get(&Value::String(protocol_amqp::SEQUENCE_NUMBERS.to_owned()))
    else {
        panic!("scheduled response has handles");
    };
    assert_eq!(handles.len(), 1);
    let entries = management_entries(&management.peek().await?)?;
    assert_eq!(entries.len(), 1);
    assert_preserved(&entries[0].0, &expected, Lifetime::Scheduled)?;
    assert_eq!(delivery_count(&entries[0].0), 0);
    assert_eq!(
        annotation(&entries[0].0, protocol_amqp::MESSAGE_STATE_ANNOTATION),
        &Value::Int(2)
    );
    assert_eq!(
        annotation(&entries[0].0, "x-opt-sequence-number"),
        &handles[0]
    );
    node.clock.set(2_000);
    assert_eq!(
        TimerWorker::new(&node.broker.handle())
            .sweep_once()?
            .messages_activated,
        1
    );
    let peeked = management_entries(&management.peek().await?)?;
    assert_eq!(peeked.len(), 1);
    assert_eq!(delivery_count(&peeked[0].0), 0);
    let mut receiver =
        Receiver::attach(&mut session, "activated-preservation-receiver", "orders").await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    assert_preserved(delivery.message(), &expected, Lifetime::Active(2_000))?;
    assert_eq!(delivery_count(delivery.message()), 0);
    assert_eq!(
        annotation(delivery.message(), protocol_amqp::MESSAGE_STATE_ANNOTATION),
        &Value::Int(0)
    );
    assert_ne!(
        annotation(delivery.message(), "x-opt-sequence-number"),
        &handles[0]
    );
    receiver.accept(&delivery).await?;
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;
    node.stop().await;
    Ok(())
}

async fn dead_letters_preserve_user_fields_and_overlay_the_reason<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(
        provider,
        Some(QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        }),
    )
    .await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "dead-letter-sender", "orders").await?;
    let mut expected = rich_message(Body::Data(vec![vec![0, 4].into(), vec![8, 12].into()]));
    expected
        .properties
        .as_mut()
        .expect("rich properties")
        .group_id = Some(String::from("preserved-session"));
    assert!(matches!(
        sender.send(expected.clone()).await?,
        Outcome::Accepted(_)
    ));
    let mut filter = FilterSet::default();
    filter.insert(
        Symbol::from(protocol_amqp::SESSION_FILTER),
        Value::String(String::from("preserved-session")),
    );
    let mut receiver = Receiver::builder()
        .name("dead-letter-original-receiver")
        .source(Source::builder().address("orders").filter(filter).build())
        .attach(&mut session)
        .await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    assert_preserved(delivery.message(), &expected, Lifetime::Active(1_000))?;
    assert_eq!(delivery_count(delivery.message()), 0);
    receiver.reject(&delivery, None).await?;
    let mut management = Management::attach_entity(&mut session, "orders/$deadletterqueue").await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let peeked = management_entries(&management.peek().await?)?;
        if !peeked.is_empty() {
            assert_eq!(peeked.len(), 1);
            // The shadow retains this broker's source acquisition counter.
            assert_eq!(delivery_count(&peeked[0].0), 1);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "dead-letter settlement was not applied"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut dead_letters = Receiver::attach(
        &mut session,
        "dead-letter-drainer",
        "orders/$deadletterqueue",
    )
    .await?;
    let dead_letter = tokio::time::timeout(Duration::from_secs(2), dead_letters.recv()).await??;
    assert_preserved(dead_letter.message(), &expected, Lifetime::DeadLetter)?;
    assert_eq!(delivery_count(dead_letter.message()), 1);
    let peeked = management_entries(&management.peek().await?)?;
    assert_eq!(delivery_count(&peeked[0].0), 2);
    let properties = dead_letter
        .message()
        .application_properties
        .as_ref()
        .expect("dead-letter application properties");
    assert_eq!(
        properties.get(protocol_amqp::DEAD_LETTER_REASON_PROPERTY),
        Some(&Value::String(String::from("RejectedByReceiver")))
    );
    assert_eq!(
        properties.get(protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY),
        Some(&Value::String(String::from(
            "the receiver rejected the message"
        )))
    );
    dead_letters.accept(&dead_letter).await?;
    sender.close().await?;
    receiver.close().await?;
    dead_letters.close().await?;
    session.end().await?;
    connection.close().await?;
    node.stop().await;
    Ok(())
}

async fn broker_annotations_cannot_be_forged_by_the_sender<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, Some(QueueConfig::default())).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "annotation-spoof-sender", "orders").await?;
    let mut expected = rich_message(Body::Value(Value::Bool(true)));
    let annotations = expected
        .message_annotations
        .as_mut()
        .expect("rich annotations");
    annotations.insert(Symbol::from("x-opt-sequence-number"), Value::Long(999_999));
    annotations.insert(
        Symbol::from("x-opt-enqueued-time"),
        Value::Timestamp(999_999_i64.into()),
    );
    annotations.insert(
        Symbol::from(protocol_amqp::MESSAGE_STATE_ANNOTATION),
        Value::Int(2),
    );
    annotations.insert(
        Symbol::from("x-opt-locked-until"),
        Value::Timestamp((-500_i64).into()),
    );
    assert!(matches!(
        sender.send(expected.clone()).await?,
        Outcome::Accepted(_)
    ));
    let mut receiver =
        Receiver::attach(&mut session, "annotation-spoof-receiver", "orders").await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??;
    assert_preserved(delivery.message(), &expected, Lifetime::Active(1_000))?;
    assert_eq!(
        annotation(delivery.message(), "x-opt-sequence-number"),
        &Value::Long(1)
    );
    assert_eq!(
        annotation(delivery.message(), "x-opt-enqueued-time"),
        &Value::Timestamp(1_000_i64.into())
    );
    assert_eq!(
        annotation(delivery.message(), protocol_amqp::MESSAGE_STATE_ANNOTATION),
        &Value::Int(0)
    );
    assert_eq!(
        annotation(delivery.message(), "x-opt-locked-until"),
        &Value::Timestamp(61_000_i64.into()),
    );
    receiver.accept(&delivery).await?;
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
    each_body_and_special_property_survives_restart,
    producer_creation_time_survives_without_a_finite_lifetime,
    typed_missing_and_empty_ids_remain_distinct,
    redelivery_preserves_payload_and_advances_only_broker_fields,
    receive_and_delete_emits_zero_for_the_first_acquisition,
    deferred_management_returns_preserved_messages,
    scheduled_management_preserves_metadata_before_and_after_activation,
    dead_letters_preserve_user_fields_and_overlay_the_reason,
    broker_annotations_cannot_be_forged_by_the_sender,
);
