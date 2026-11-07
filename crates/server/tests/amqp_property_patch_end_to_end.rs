//! Property-modifying dispositions over a real AMQP socket: each patch stays
//! on its own copy and renders over the original on every later read.

use std::{error::Error, time::Duration};

use amqp::{
    AnnotationKey, ApplicationProperties, Array, Body, ClientConnection as Connection,
    ClientDelivery, ClientReceiver as Receiver, ClientSender as Sender, ClientSession as Session,
    Error as AmqpError, ErrorCondition, Fields, FilterSet, Message, MessageAnnotations, Modified,
    OrderedMap, Outcome, Properties, Source, Symbol, Value, encode_message,
};
use domain::{
    CommandKind, CommandOutcome, QueueConfig, StateMachine, SubscriptionConfig, SubscriptionName,
    TopicConfig,
};
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::net::TcpListener;

#[path = "../src/test_deadline.rs"]
mod test_deadline;

use test_deadline::WAIT;

struct Node {
    broker: Broker,
    clock: ManualClock,
    address: String,
}

impl Node {
    async fn queue(config: QueueConfig) -> Result<Self, Box<dyn Error>> {
        Self::start(vec![("orders", CommandKind::CreateQueue { config })]).await
    }

    async fn topic() -> Result<Self, Box<dyn Error>> {
        let mut setup = vec![(
            "events",
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )];
        for name in ["accounting", "analytics"] {
            setup.push((
                "events",
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(name)?,
                    config: SubscriptionConfig::default(),
                },
            ));
        }
        Self::start(setup).await
    }

    async fn start(setup: Vec<(&str, CommandKind)>) -> Result<Self, Box<dyn Error>> {
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            clock.clone(),
        ));
        let namespace = domain::NamespaceName::new("tenant")?;
        for (entity, kind) in setup {
            broker.handle().submit_blocking(
                namespace.clone(),
                domain::EntityPath::new(entity)?,
                kind,
            )?;
        }

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let handle = broker.handle();
        tokio::spawn(async move {
            let _ = protocol_amqp::AmqpListener::new(handle, namespace)
                .serve(listener)
                .await;
        });
        Ok(Self {
            broker,
            clock,
            address,
        })
    }

    async fn connect(&self) -> Result<Connection, Box<dyn Error>> {
        Ok(Connection::builder()
            .container_id("property-patch-test-client")
            .open(format!("amqp://{}", self.address).as_str())
            .await?)
    }

    fn submit(&self, entity: &str, kind: CommandKind) -> Result<CommandOutcome, Box<dyn Error>> {
        Ok(self.broker.handle().submit_blocking(
            domain::NamespaceName::new("tenant")?,
            domain::EntityPath::new(entity)?,
            kind,
        )?)
    }
}

/// A request/reply pair on one entity's management node.
struct Management {
    requests: Sender,
    responses: Receiver,
    reply_to: String,
    next_id: u64,
}

impl Management {
    async fn attach(session: &mut Session, entity: &str) -> Result<Self, Box<dyn Error>> {
        let node = format!("{entity}/$management");
        let reply_to = format!("{entity}-replies");
        let responses = Receiver::builder()
            .name(format!("{entity}-management-response"))
            .source(node.as_str())
            .target(reply_to.as_str())
            .attach(session)
            .await?;
        let requests = Sender::attach(
            session,
            format!("{entity}-management-request"),
            node.as_str(),
        )
        .await?;
        Ok(Self {
            requests,
            responses,
            reply_to,
            next_id: 0,
        })
    }

    /// Sends one request and returns its status code and response.
    async fn request(
        &mut self,
        operation: &str,
        link_name: Option<&str>,
        body: OrderedMap<Value, Value>,
    ) -> Result<(i32, Message), Box<dyn Error>> {
        self.next_id += 1;
        let mut properties = ApplicationProperties::builder()
            .insert(protocol_amqp::OPERATION_PROPERTY, operation)
            .build();
        if let Some(link_name) = link_name {
            properties.insert(protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY, link_name);
        }
        let request = Message::builder()
            .properties(Properties {
                message_id: Some(format!("request-{}", self.next_id).into()),
                reply_to: Some(self.reply_to.clone()),
                ..Properties::default()
            })
            .application_properties(properties)
            .body(Body::Value(Value::Map(body)))
            .build();
        assert!(matches!(
            self.requests.send(request).await?,
            Outcome::Accepted(_)
        ));
        let response = tokio::time::timeout(WAIT, self.responses.recv()).await??;
        let message = response.message().clone();
        self.responses.accept(&response).await?;
        let status = match message
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY))
        {
            Some(Value::Int(status)) => *status,
            other => return Err(format!("the response carried no status: {other:?}").into()),
        };
        Ok((status, message))
    }

    async fn peek(&mut self, from: i64) -> Result<Vec<Message>, Box<dyn Error>> {
        let (status, response) = self
            .request(
                protocol_amqp::PEEK_MESSAGE_OPERATION,
                None,
                map([
                    ("from-sequence-number", Value::Long(from)),
                    ("message-count", Value::Int(10)),
                ]),
            )
            .await?;
        assert_eq!(status, 200, "peek found nothing");
        encoded_messages(response)
    }

    async fn update_disposition(
        &mut self,
        link_name: &str,
        lock_token: amqp::Uuid,
        status: &str,
        properties: &[(&str, Value)],
    ) -> Result<(i32, Message), Box<dyn Error>> {
        let mut changes = OrderedMap::new();
        for (name, value) in properties {
            changes.insert(Value::String((*name).to_owned()), value.clone());
        }
        self.request(
            protocol_amqp::UPDATE_DISPOSITION_OPERATION,
            Some(link_name),
            map([
                (
                    protocol_amqp::LOCK_TOKENS,
                    Value::Array(Array::from(vec![Value::Uuid(lock_token)])),
                ),
                ("disposition-status", Value::String(status.to_owned())),
                ("properties-to-modify", Value::Map(changes)),
            ]),
        )
        .await
    }
}

fn map<const N: usize>(entries: [(&str, Value); N]) -> OrderedMap<Value, Value> {
    let mut map = OrderedMap::new();
    for (name, value) in entries {
        map.insert(Value::String(name.to_owned()), value);
    }
    map
}

fn encoded_messages(response: Message) -> Result<Vec<Message>, Box<dyn Error>> {
    let Body::Value(Value::Map(body)) = response.body else {
        return Err("the response must carry an AMQP value map".into());
    };
    let Some(Value::List(entries)) = body.get(&Value::String(String::from("messages"))) else {
        return Err("the response did not contain messages".into());
    };
    entries
        .iter()
        .map(|entry| {
            let Value::Map(entry) = entry else {
                return Err("a response entry must be an AMQP map".into());
            };
            let Some(Value::Binary(encoded)) = entry.get(&Value::String(String::from("message")))
            else {
                return Err("a response entry did not contain an encoded message".into());
            };
            amqp::decode_message(encoded).map_err(Into::into)
        })
        .collect()
}

fn body(text: &str) -> Body {
    Body::Data(vec![text.as_bytes().to_vec().into()])
}

fn text_of(message: &Message) -> String {
    match &message.body {
        Body::Data(sections) => sections
            .iter()
            .flat_map(|section| section.iter().copied())
            .map(char::from)
            .collect(),
        _ => String::new(),
    }
}

fn sent(text: &str, properties: &[(&str, Value)]) -> Message {
    let mut application = ApplicationProperties::default();
    for (name, value) in properties {
        application.insert(*name, value.clone());
    }
    Message::builder()
        .application_properties(application)
        .body(body(text))
        .build()
}

fn string(text: &str) -> Value {
    Value::String(text.to_owned())
}

fn properties_of(message: &Message) -> Vec<(String, Value)> {
    message
        .application_properties
        .as_ref()
        .map(|properties| {
            properties
                .0
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect()
        })
        .unwrap_or_default()
}

fn expect_properties(message: &Message, expected: &[(&str, Value)]) {
    let expected: Vec<_> = expected
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect();
    assert_eq!(properties_of(message), expected);
}

fn annotation<'a>(message: &'a Message, name: &str) -> Option<&'a Value> {
    message
        .message_annotations
        .as_ref()
        .and_then(|annotations| annotations.get(&AnnotationKey::from(name)))
}

fn sequence_of(message: &Message) -> i64 {
    match annotation(message, "x-opt-sequence-number") {
        Some(Value::Long(sequence)) => *sequence,
        other => panic!("expected a sequence annotation, got {other:?}"),
    }
}

fn lock_token_of(delivery: &ClientDelivery) -> amqp::Uuid {
    match delivery
        .message()
        .delivery_annotations
        .as_ref()
        .and_then(|annotations| annotations.get(&AnnotationKey::from("x-opt-lock-token")))
    {
        Some(Value::Uuid(token)) => token.clone(),
        other => panic!("expected a lock token, got {other:?}"),
    }
}

fn changes(properties: &[(&str, Value)]) -> Fields {
    let mut fields = Fields::new();
    for (name, value) in properties {
        fields.insert(Symbol::from(*name), value.clone());
    }
    fields
}

async fn abandon_with(
    receiver: &Receiver,
    delivery: &ClientDelivery,
    properties: &[(&str, Value)],
) -> Result<(), Box<dyn Error>> {
    receiver
        .modify(
            delivery,
            Modified {
                message_annotations: Some(changes(properties)),
                ..Modified::default()
            },
        )
        .await?;
    Ok(())
}

async fn next(receiver: &mut Receiver) -> Result<ClientDelivery, Box<dyn Error>> {
    Ok(tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await??)
}

async fn assert_nothing_more(receiver: &mut Receiver) {
    assert!(
        tokio::time::timeout(Duration::from_millis(250), receiver.recv())
            .await
            .is_err(),
        "an unexpected message was delivered"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn patches_accumulate_across_abandons_and_reach_the_dead_letter_queue()
-> Result<(), Box<dyn Error>> {
    let node = Node::queue(QueueConfig::default()).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "patch-sender", "orders").await?;
    sender
        .send(sent(
            "order",
            &[
                ("stage", string("received")),
                ("kept", string("original")),
                (protocol_amqp::DEAD_LETTER_REASON_PROPERTY, string("forged")),
            ],
        ))
        .await?;
    let mut management = Management::attach(&mut session, "orders").await?;
    let mut receiver = Receiver::attach(&mut session, "patch-receiver", "orders").await?;

    let first = next(&mut receiver).await?;
    let sequence = sequence_of(first.message());
    let unpatched = [("stage", string("received")), ("kept", string("original"))];
    expect_properties(first.message(), &unpatched);
    let [peeked] = management
        .peek(sequence)
        .await?
        .try_into()
        .expect("one peek");
    expect_properties(&peeked, &unpatched);

    abandon_with(
        &receiver,
        &first,
        &[
            ("stage", string("retry-1")),
            ("attempt", Value::Int(1)),
            (
                protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY,
                string("forged in a patch"),
            ),
        ],
    )
    .await?;
    let second = next(&mut receiver).await?;
    let once = [
        ("stage", string("retry-1")),
        ("kept", string("original")),
        ("attempt", Value::Int(1)),
    ];
    expect_properties(second.message(), &once);
    assert_eq!(text_of(second.message()), "order");
    assert_eq!(
        second
            .message()
            .header
            .as_ref()
            .map(|header| header.delivery_count),
        Some(1)
    );
    let [peeked] = management
        .peek(sequence)
        .await?
        .try_into()
        .expect("one peek");
    expect_properties(&peeked, &once);

    abandon_with(
        &receiver,
        &second,
        &[("attempt", Value::Int(2)), ("note", Value::Null)],
    )
    .await?;
    let third = next(&mut receiver).await?;
    expect_properties(
        third.message(),
        &[
            ("stage", string("retry-1")),
            ("kept", string("original")),
            ("attempt", Value::Int(2)),
            ("note", Value::Null),
        ],
    );

    let mut info = changes(&[
        (
            protocol_amqp::DEAD_LETTER_REASON_PROPERTY,
            string("InvalidOrder"),
        ),
        (
            protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY,
            string("the order has no customer"),
        ),
    ]);
    info.insert(Symbol::from("stage"), string("dead"));
    receiver
        .reject(
            &third,
            Some(AmqpError {
                condition: ErrorCondition::Custom(Symbol::from("com.microsoft:dead-letter")),
                description: None,
                info: Some(info),
            }),
        )
        .await?;

    let dead = [
        ("stage", string("dead")),
        ("kept", string("original")),
        ("attempt", Value::Int(2)),
        ("note", Value::Null),
        (
            protocol_amqp::DEAD_LETTER_REASON_PROPERTY,
            string("InvalidOrder"),
        ),
        (
            protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY,
            string("the order has no customer"),
        ),
    ];
    let mut dead_letters = Receiver::attach(
        &mut session,
        "patch-dlq-receiver",
        "orders/$deadletterqueue",
    )
    .await?;
    let dead_lettered = next(&mut dead_letters).await?;
    expect_properties(dead_lettered.message(), &dead);
    let mut dead_letter_management =
        Management::attach(&mut session, "orders/$deadletterqueue").await?;
    let [peeked] = dead_letter_management
        .peek(sequence)
        .await?
        .try_into()
        .expect("one dead-letter peek");
    expect_properties(&peeked, &dead);
    dead_letters.accept(&dead_lettered).await?;
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deferred_patch_survives_receive_by_sequence_and_a_further_patch()
-> Result<(), Box<dyn Error>> {
    let node = Node::queue(QueueConfig::default()).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "defer-sender", "orders").await?;
    sender
        .send(sent("parked", &[("kept", string("original"))]))
        .await?;
    let mut management = Management::attach(&mut session, "orders").await?;
    let mut receiver = Receiver::attach(&mut session, "defer-receiver", "orders").await?;
    let first = next(&mut receiver).await?;
    let sequence = sequence_of(first.message());
    receiver
        .modify(
            &first,
            Modified {
                undeliverable_here: Some(true),
                message_annotations: Some(changes(&[("deferred", Value::Bool(true))])),
                ..Modified::default()
            },
        )
        .await?;

    let receive_body = || {
        map([
            (
                "sequence-numbers",
                Value::Array(Array::from(vec![Value::Long(sequence)])),
            ),
            ("receiver-settle-mode", Value::Uint(1)),
        ])
    };
    let lock_of = |response: &Message| -> amqp::Uuid {
        let Body::Value(Value::Map(body)) = &response.body else {
            panic!("the deferred response must be a map")
        };
        let Some(Value::List(entries)) = body.get(&Value::String(String::from("messages"))) else {
            panic!("the deferred response has messages")
        };
        let [Value::Map(entry)] = entries.as_slice() else {
            panic!("exactly one deferred message")
        };
        match entry.get(&Value::String(String::from("lock-token"))) {
            Some(Value::Uuid(token)) => token.clone(),
            other => panic!("expected a lock token, got {other:?}"),
        }
    };

    let (status, response) = management
        .request(
            protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
            None,
            receive_body(),
        )
        .await?;
    assert_eq!(status, 200);
    let lock_token = lock_of(&response);
    let [deferred] = encoded_messages(response)?.try_into().expect("one message");
    expect_properties(
        &deferred,
        &[
            ("kept", string("original")),
            ("deferred", Value::Bool(true)),
        ],
    );

    // A further patched abandon of the request/response lock accumulates.
    let (status, _) = management
        .update_disposition(
            "unused",
            lock_token,
            "abandoned",
            &[("deferred", Value::Bool(false)), ("retried", Value::Int(1))],
        )
        .await?;
    assert_eq!(status, 200);
    let (status, response) = management
        .request(
            protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
            None,
            receive_body(),
        )
        .await?;
    assert_eq!(status, 200);
    let [again] = encoded_messages(response)?.try_into().expect("one message");
    expect_properties(
        &again,
        &[
            ("kept", string("original")),
            ("deferred", Value::Bool(false)),
            ("retried", Value::Int(1)),
        ],
    );
    assert_nothing_more(&mut receiver).await;
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn management_can_patch_a_link_received_lock() -> Result<(), Box<dyn Error>> {
    let node = Node::queue(QueueConfig::default()).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "link-sender", "orders").await?;
    sender.send(sent("linked", &[])).await?;
    let mut management = Management::attach(&mut session, "orders").await?;
    let mut receiver = Receiver::attach(&mut session, "link-receiver", "orders").await?;
    let first = next(&mut receiver).await?;

    let (status, response) = management
        .update_disposition(
            "link-receiver",
            lock_token_of(&first),
            "abandoned",
            &[("reviewed-by", string("management"))],
        )
        .await?;
    assert_eq!(status, 200, "{response:?}");
    let second = next(&mut receiver).await?;
    expect_properties(second.message(), &[("reviewed-by", string("management"))]);
    receiver.accept(&second).await?;
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_patch_past_the_headroom_is_message_size_exceeded_and_changes_nothing()
-> Result<(), Box<dyn Error>> {
    const LIMIT: usize = 1024;
    let node = Node::queue(QueueConfig {
        max_message_bytes: LIMIT,
        ..QueueConfig::default()
    })
    .await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "size-sender", "orders").await?;
    let message = sent(&"x".repeat(900), &[]);
    let original_bytes = encode_message(&message)?.len();
    assert!(matches!(sender.send(message).await?, Outcome::Accepted(_)));
    let mut management = Management::attach(&mut session, "orders").await?;
    let mut receiver = Receiver::attach(&mut session, "size-receiver", "orders").await?;
    let first = next(&mut receiver).await?;

    // The copy is charged its original bytes plus each overlay name and value;
    // a binary value over 255 bytes encodes with a five-byte prefix.
    let fitting = LIMIT + domain::PROPERTY_PATCH_HEADROOM_BYTES - original_bytes - "pad".len() - 5;
    let pad = |len: usize| [("pad", Value::Binary(vec![0_u8; len].into()))];
    let (status, response) = management
        .update_disposition(
            "size-receiver",
            lock_token_of(&first),
            "abandoned",
            &pad(fitting + 1),
        )
        .await?;
    assert_eq!(status, 403, "{response:?}");
    assert_eq!(
        response
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::ERROR_CONDITION_PROPERTY)),
        Some(&Value::Symbol(Symbol::from(
            protocol_amqp::MESSAGE_SIZE_EXCEEDED
        )))
    );

    // The refusal kept the lock, so the same lock still takes a fitting patch.
    let (status, response) = management
        .update_disposition(
            "size-receiver",
            lock_token_of(&first),
            "abandoned",
            &pad(fitting),
        )
        .await?;
    assert_eq!(status, 200, "{response:?}");
    let second = next(&mut receiver).await?;
    assert_eq!(
        properties_of(second.message()),
        vec![(
            String::from("pad"),
            Value::Binary(vec![0_u8; fitting].into())
        )]
    );
    receiver.accept(&second).await?;
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_activated_scheduled_message_takes_a_patch() -> Result<(), Box<dyn Error>> {
    let node = Node::queue(QueueConfig::default()).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "scheduled-sender", "orders").await?;
    let mut message = sent("later", &[("kept", string("original"))]);
    let mut annotations = MessageAnnotations::default();
    annotations.insert(
        "x-opt-scheduled-enqueue-time",
        Value::Timestamp(5_000_i64.into()),
    );
    message.message_annotations = Some(annotations);
    sender.send(message).await?;
    let mut receiver = Receiver::attach(&mut session, "scheduled-receiver", "orders").await?;
    assert_nothing_more(&mut receiver).await;

    node.clock.set(5_000);
    assert!(matches!(
        node.submit("orders", CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated { activated: 1, .. }
    ));
    let first = next(&mut receiver).await?;
    let scheduled = annotation(first.message(), "x-opt-scheduled-enqueue-time").cloned();
    abandon_with(&receiver, &first, &[("patched", Value::Bool(true))]).await?;
    let second = next(&mut receiver).await?;
    expect_properties(
        second.message(),
        &[("kept", string("original")), ("patched", Value::Bool(true))],
    );
    assert_eq!(
        annotation(second.message(), "x-opt-scheduled-enqueue-time").cloned(),
        scheduled,
        "the patch leaves broker scheduling state as it was"
    );
    receiver.accept(&second).await?;
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_patch_on_one_batch_child_leaves_its_sibling_unchanged() -> Result<(), Box<dyn Error>> {
    let node = Node::queue(QueueConfig::default()).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "batch-sender", "orders").await?;
    let children = [
        sent("first", &[("index", Value::Int(0))]),
        sent("second", &[("index", Value::Int(1))]),
    ];
    let sections = children
        .iter()
        .map(|child| encode_message(child).map(Into::into))
        .collect::<Result<Vec<_>, _>>()?;
    assert!(matches!(
        sender
            .send_with_format(
                Message::builder().body(Body::Data(sections)).build(),
                protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
            )
            .await?,
        Outcome::Accepted(_)
    ));
    let mut receiver = Receiver::attach(&mut session, "batch-receiver", "orders").await?;
    let first = next(&mut receiver).await?;
    let second = next(&mut receiver).await?;
    assert_eq!(text_of(first.message()), "first");
    abandon_with(&receiver, &first, &[("patched", Value::Bool(true))]).await?;
    receiver.release(&second).await?;

    let mut seen = Vec::new();
    for _ in 0..2 {
        let delivery = next(&mut receiver).await?;
        seen.push((
            text_of(delivery.message()),
            properties_of(delivery.message()),
        ));
        receiver.accept(&delivery).await?;
    }
    seen.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        seen,
        vec![
            (
                String::from("first"),
                vec![
                    (String::from("index"), Value::Int(0)),
                    (String::from("patched"), Value::Bool(true)),
                ]
            ),
            (
                String::from("second"),
                vec![(String::from("index"), Value::Int(1))]
            ),
        ]
    );
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_patched_session_message_stays_in_its_session() -> Result<(), Box<dyn Error>> {
    let node = Node::queue(QueueConfig {
        requires_session: true,
        ..QueueConfig::default()
    })
    .await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "session-sender", "orders").await?;
    let mut message = sent("in-session", &[]);
    message.properties = Some(Properties {
        group_id: Some(String::from("cart-1")),
        ..Properties::default()
    });
    sender.send(message).await?;
    let mut filter = FilterSet::default();
    filter.insert(
        Symbol::from(protocol_amqp::SESSION_FILTER),
        string("cart-1"),
    );
    let mut receiver = Receiver::builder()
        .name("session-receiver")
        .source(Source::builder().address("orders").filter(filter).build())
        .attach(&mut session)
        .await?;
    let first = next(&mut receiver).await?;
    abandon_with(&receiver, &first, &[("patched", Value::Bool(true))]).await?;
    let second = next(&mut receiver).await?;
    expect_properties(second.message(), &[("patched", Value::Bool(true))]);
    assert_eq!(
        second
            .message()
            .properties
            .as_ref()
            .and_then(|properties| properties.group_id.as_deref()),
        Some("cart-1")
    );
    receiver.accept(&second).await?;
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_detection_still_holds_after_a_patch() -> Result<(), Box<dyn Error>> {
    let node = Node::queue(QueueConfig {
        requires_duplicate_detection: true,
        ..QueueConfig::default()
    })
    .await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "dedupe-sender", "orders").await?;
    let with_id = |text: &str| {
        let mut message = sent(text, &[]);
        message.properties = Some(Properties {
            message_id: Some(String::from("same-id").into()),
            ..Properties::default()
        });
        message
    };
    sender.send(with_id("first")).await?;
    let mut receiver = Receiver::attach(&mut session, "dedupe-receiver", "orders").await?;
    let first = next(&mut receiver).await?;
    abandon_with(&receiver, &first, &[("patched", Value::Bool(true))]).await?;
    let second = next(&mut receiver).await?;
    assert!(matches!(
        sender.send(with_id("duplicate")).await?,
        Outcome::Accepted(_)
    ));
    assert_eq!(text_of(second.message()), "first");
    expect_properties(second.message(), &[("patched", Value::Bool(true))]);
    receiver.accept(&second).await?;
    assert_nothing_more(&mut receiver).await;
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_patch_on_one_subscription_copy_is_invisible_to_another() -> Result<(), Box<dyn Error>> {
    let node = Node::topic().await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "topic-sender", "events").await?;
    sender
        .send(sent("fanout", &[("kept", string("original"))]))
        .await?;
    let accounting_path = "events/Subscriptions/accounting";
    let analytics_path = "events/Subscriptions/analytics";
    let mut accounting = Receiver::attach(&mut session, "accounting", accounting_path).await?;
    let first = next(&mut accounting).await?;
    let sequence = sequence_of(first.message());
    abandon_with(&accounting, &first, &[("patched", Value::Bool(true))]).await?;
    let patched = next(&mut accounting).await?;
    expect_properties(
        patched.message(),
        &[("kept", string("original")), ("patched", Value::Bool(true))],
    );

    let unpatched = [("kept", string("original"))];
    let mut analytics_management = Management::attach(&mut session, analytics_path).await?;
    let peeked = analytics_management.peek(sequence).await?;
    let [peeked] = peeked.as_slice() else {
        return Err("the sibling subscription holds one copy".into());
    };
    expect_properties(peeked, &unpatched);
    let mut analytics = Receiver::attach(&mut session, "analytics", analytics_path).await?;
    let sibling = next(&mut analytics).await?;
    expect_properties(sibling.message(), &unpatched);
    analytics.accept(&sibling).await?;
    accounting.accept(&patched).await?;
    connection.close().await?;
    Ok(())
}
