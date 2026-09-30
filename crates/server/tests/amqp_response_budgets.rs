//! Management replies honor aggregate and peer limits before taking locks.

use std::{error::Error, time::Duration};

use amqp::{
    ApplicationProperties, Array, Body, ClientConnection, ClientReceiver, ClientSender,
    ClientSession, Message, MessageId, OrderedMap, Outcome, Properties, Symbol, Uuid, Value,
    decode_message, encode_message,
};
use domain::{
    CommandKind, CommandOutcome, Delivery, EntityPath, LockToken, MessageBody, MessageEnvelope,
    MessageIdentifier, MessageProperties, MessageStatus, NamespaceName, QueueConfig, ReceiveMode,
    SequenceNumber, StateMachine,
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
    async fn start(provider: P, bodies: Vec<Vec<u8>>, deferred: bool) -> TestResult<Self> {
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
        for (index, body) in bodies.into_iter().enumerate() {
            let id = format!("message-{}", index + 1);
            let envelope = MessageEnvelope {
                properties: MessageProperties {
                    message_id: Some(MessageIdentifier::String(id.clone())),
                    subject: Some("retained subject".to_owned()),
                    ..MessageProperties::default()
                },
                body: MessageBody::Data(vec![body.clone()]),
                ..MessageEnvelope::default()
            };
            let CommandOutcome::Sent { sequence } = handle.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::SendEnvelope {
                    message_id: id,
                    body,
                    time_to_live_millis: Some(120_000),
                    session_id: None,
                    envelope: Box::new(envelope),
                },
            )?
            else {
                panic!("the fixture message was not sent");
            };
            if deferred {
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
                assert_eq!(delivery.sequence, sequence);
                assert_eq!(
                    handle.submit_blocking(
                        namespace.clone(),
                        entity.clone(),
                        CommandKind::Defer {
                            sequence,
                            lock_token: delivery.lock.expect("fixture lock").token,
                        },
                    )?,
                    CommandOutcome::Deferred
                );
            }
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

    async fn connect(&self) -> TestResult<ClientConnection> {
        Ok(ClientConnection::builder()
            .container_id("response-budget-client")
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
    reply_to: String,
    next_id: u64,
}

impl Management {
    async fn attach(session: &mut ClientSession, maximum: Option<u64>) -> TestResult<Self> {
        Self::attach_named(session, maximum, "default").await
    }

    async fn attach_named(
        session: &mut ClientSession,
        maximum: Option<u64>,
        name: &str,
    ) -> TestResult<Self> {
        let reply_to = format!("budget-replies-{name}");
        let builder = ClientReceiver::builder()
            .name(format!("budget-responses-{name}"))
            .source("orders/$management")
            .target(reply_to.clone());
        let builder = match maximum {
            Some(maximum) => builder.max_message_size(maximum),
            None => builder,
        };
        let responses = builder.attach(session).await?;
        let requests = ClientSender::attach(
            session,
            format!("budget-requests-{name}"),
            "orders/$management",
        )
        .await?;
        Ok(Self {
            requests,
            responses,
            reply_to,
            next_id: 0,
        })
    }

    async fn request(
        &mut self,
        operation: &str,
        body: OrderedMap<Value, Value>,
    ) -> TestResult<Message> {
        self.next_id += 1;
        self.request_with_metadata(operation, body, MessageId::Ulong(self.next_id), None)
            .await
    }

    async fn request_with_metadata(
        &mut self,
        operation: &str,
        body: OrderedMap<Value, Value>,
        message_id: MessageId,
        tracking_id: Option<&str>,
    ) -> TestResult<Message> {
        let request = management_request(
            operation,
            body,
            message_id.clone(),
            tracking_id,
            &self.reply_to,
        );
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
        assert_eq!(
            response
                .application_properties
                .as_ref()
                .and_then(|properties| properties.get(protocol_amqp::TRACKING_ID_PROPERTY)),
            tracking_id
                .map(|tracking| Value::String(tracking.to_owned()))
                .as_ref()
        );
        Ok(response)
    }

    async fn peek(&mut self, from: u64) -> TestResult<Message> {
        self.request(protocol_amqp::PEEK_MESSAGE_OPERATION, peek_body(from))
            .await
    }

    async fn receive_deferred(&mut self, sequences: &[u64], mode: u32) -> TestResult<Message> {
        self.request(
            protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
            deferred_body(sequences, mode),
        )
        .await
    }
}

fn management_request(
    operation: &str,
    body: OrderedMap<Value, Value>,
    message_id: MessageId,
    tracking_id: Option<&str>,
    reply_to: &str,
) -> Message {
    let mut application_properties = ApplicationProperties::default();
    application_properties.insert(protocol_amqp::OPERATION_PROPERTY, operation);
    application_properties.insert(
        protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY,
        "budget-receiver",
    );
    if let Some(tracking_id) = tracking_id {
        application_properties.insert(protocol_amqp::TRACKING_ID_PROPERTY, tracking_id);
    }
    Message::builder()
        .properties(Properties {
            message_id: Some(message_id),
            reply_to: Some(reply_to.to_owned()),
            ..Properties::default()
        })
        .application_properties(application_properties)
        .body(Body::Value(Value::Map(body)))
        .build()
}

fn key(name: &str) -> Value {
    Value::String(name.to_owned())
}

fn deferred_body(sequences: &[u64], mode: u32) -> OrderedMap<Value, Value> {
    OrderedMap::from_iter([
        (
            key(protocol_amqp::SEQUENCE_NUMBERS),
            Value::Array(Array::from(
                sequences
                    .iter()
                    .map(|sequence| Value::Long(*sequence as i64))
                    .collect::<Vec<_>>(),
            )),
        ),
        (key(protocol_amqp::RECEIVER_SETTLE_MODE), Value::Uint(mode)),
    ])
}

fn peek_body(from: u64) -> OrderedMap<Value, Value> {
    OrderedMap::from_iter([
        (
            key(protocol_amqp::FROM_SEQUENCE_NUMBER),
            Value::Long(from as i64),
        ),
        (key(protocol_amqp::MESSAGE_COUNT), Value::Int(100)),
    ])
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

fn entries(response: &Message) -> TestResult<Vec<(Message, Option<Uuid>)>> {
    let Body::Value(Value::Map(body)) = &response.body else {
        panic!("management response must contain a map");
    };
    let Some(Value::List(entries)) = body.get(&key(protocol_amqp::MESSAGES)) else {
        panic!("management response must contain messages");
    };
    entries
        .iter()
        .map(|entry| {
            let Value::Map(entry) = entry else {
                panic!("message entry must contain a map");
            };
            let Some(Value::Binary(encoded)) = entry.get(&key(protocol_amqp::MESSAGE)) else {
                panic!("message entry must contain encoded data");
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

fn token(number: u64) -> Uuid {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&number.to_be_bytes());
    bytes.into()
}

fn assert_data(message: &Message, expected: &[u8]) {
    let Body::Data(sections) = &message.body else {
        panic!("expected a data body");
    };
    assert_eq!(sections.len(), 1);
    assert!(
        sections[0].as_ref() == expected,
        "the payload must be preserved"
    );
}

async fn oversized_deferred_batch_is_atomic<P: StoreProvider>(
    provider: P,
    mode: u32,
) -> TestResult {
    let payload = vec![0x5a; 224 * 1024];
    let node = Node::start(provider, vec![payload.clone(); 20], true).await?;
    let before = node.peek()?;
    assert_eq!(before.len(), 20);
    assert!(before.iter().all(|message| {
        message.status == MessageStatus::Deferred
            && message.delivery_count == 1
            && message.expires_at.is_some()
            && message.lock.is_none()
    }));
    let mut connection = node.connect().await?;
    let mut session = ClientSession::begin(&mut connection).await?;
    let mut management = Management::attach(&mut session, None).await?;
    let all = (1..=20).collect::<Vec<u64>>();
    let rejected = management.receive_deferred(&all, mode).await?;
    assert_status(&rejected, 403, Some(protocol_amqp::MESSAGE_SIZE_EXCEEDED));
    assert!(
        node.peek()? == before,
        "quota rejection must preserve content, state and expiry"
    );

    for sequences in all.chunks(10) {
        let accepted = management.receive_deferred(sequences, mode).await?;
        assert_status(&accepted, 200, None);
        let messages = entries(&accepted)?;
        assert_eq!(messages.len(), 10);
        for ((message, lock_token), sequence) in messages.iter().zip(sequences) {
            assert_data(message, &payload);
            assert_eq!(
                message
                    .properties
                    .as_ref()
                    .and_then(|properties| properties.message_id.as_ref()),
                Some(&MessageId::String(format!("message-{sequence}")))
            );
            assert_eq!(message.header.as_ref().expect("header").delivery_count, 1);
            if mode == 1 {
                assert_eq!(lock_token.as_ref(), Some(&token(20 + *sequence)));
            } else {
                assert_eq!(lock_token, &None);
            }
        }
    }
    if mode == 1 {
        let after = node.peek()?;
        assert_eq!(after.len(), 20);
        assert!(after.iter().all(|message| {
            message.status == MessageStatus::Active
                && message.delivery_count == 2
                && message.expires_at == before[0].expires_at
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
        assert_eq!(delivery.lock.expect("lock").token, LockToken::new(21));
    }
    connection.close().await?;
    Ok(())
}

async fn oversized_peek_lock_batch<P: StoreProvider>(provider: P) -> TestResult {
    oversized_deferred_batch_is_atomic(provider, 1).await
}

async fn oversized_receive_and_delete_batch<P: StoreProvider>(provider: P) -> TestResult {
    oversized_deferred_batch_is_atomic(provider, 0).await
}

async fn a_small_peer_receives_fitting_peek_prefixes_without_mutation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let first = vec![0x11; 900];
    let second = vec![0x22; 900];
    let node = Node::start(provider, vec![first.clone(), second.clone()], false).await?;
    let before = node.peek()?;
    let mut connection = node.connect().await?;
    let mut session = ClientSession::begin(&mut connection).await?;
    let mut management = Management::attach(&mut session, Some(2_048)).await?;
    for (from, payload) in [(1, first), (2, second)] {
        let response = management.peek(from).await?;
        assert_status(&response, 200, None);
        assert!(encode_message(&response)?.len() <= 2_048);
        let messages = entries(&response)?;
        assert_eq!(messages.len(), 1);
        assert_data(&messages[0].0, &payload);
        assert!(messages[0].1.is_none());
        assert_eq!(
            messages[0]
                .0
                .header
                .as_ref()
                .expect("peek header")
                .delivery_count,
            0
        );
    }
    assert!(node.peek()? == before, "peeking must not mutate the store");
    connection.close().await?;
    Ok(())
}

async fn an_oversized_first_peek_item_is_refused_but_later_pages_remain_readable<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fitting = vec![0x22; 900];
    let node = Node::start(provider, vec![vec![0x11; 3_000], fitting.clone()], false).await?;
    let before = node.peek()?;
    let mut connection = node.connect().await?;
    let mut session = ClientSession::begin(&mut connection).await?;
    let mut management = Management::attach(&mut session, Some(2_048)).await?;
    let response = management.peek(1).await?;
    assert_status(&response, 403, Some(protocol_amqp::MESSAGE_SIZE_EXCEEDED));
    assert!(encode_message(&response)?.len() <= 2_048);
    assert!(
        node.peek()? == before,
        "quota rejection must not mutate the store"
    );
    let response = management.peek(2).await?;
    assert_status(&response, 200, None);
    let messages = entries(&response)?;
    assert_eq!(messages.len(), 1);
    assert_data(&messages[0].0, &fitting);
    assert!(node.peek()? == before);
    connection.close().await?;
    Ok(())
}

async fn correlation_and_tracking_fields_reduce_the_remaining_reply_budget<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let payload = vec![0x33; 900];
    let node = Node::start(provider, vec![payload.clone()], false).await?;
    let before = node.peek()?;
    let mut connection = node.connect().await?;
    let mut session = ClientSession::begin(&mut connection).await?;
    let mut management = Management::attach(&mut session, Some(2_048)).await?;
    let correlation = MessageId::String("c".repeat(700));
    let tracking = "t".repeat(700);
    let response = management
        .request_with_metadata(
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            peek_body(1),
            correlation,
            Some(&tracking),
        )
        .await?;
    assert_status(&response, 403, Some(protocol_amqp::MESSAGE_SIZE_EXCEEDED));
    assert!(encode_message(&response)?.len() <= 2_048);
    assert!(node.peek()? == before);
    let response = management.peek(1).await?;
    assert_status(&response, 200, None);
    let messages = entries(&response)?;
    assert_eq!(messages.len(), 1);
    assert_data(&messages[0].0, &payload);
    assert!(node.peek()? == before);
    connection.close().await?;
    Ok(())
}

fn assert_rejected(outcome: Outcome, condition: &str) {
    let Outcome::Rejected(rejected) = outcome else {
        panic!("the request must be rejected before mutation: {outcome:?}");
    };
    assert_eq!(
        rejected
            .error
            .expect("rejection error")
            .condition
            .as_symbol()
            .to_string(),
        condition
    );
}

async fn a_missing_reply_route_rejects_mutation_until_a_reply_link_attaches<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let payload = vec![0x44; 256];
    let node = Node::start(provider, vec![payload.clone()], true).await?;
    let before = node.peek()?;
    let mut connection = node.connect().await?;
    let mut session = ClientSession::begin(&mut connection).await?;
    let mut requests =
        ClientSender::attach(&mut session, "unrouted-requests", "orders/$management").await?;
    let request = management_request(
        protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
        deferred_body(&[1], 0),
        MessageId::Ulong(1),
        None,
        "not-yet-attached",
    );
    assert_rejected(
        tokio::time::timeout(Duration::from_secs(5), requests.send(request)).await??,
        protocol_amqp::PRECONDITION_FAILED,
    );
    assert!(
        node.peek()? == before,
        "a request without a reply route must not remove deferred messages"
    );
    let mut management = Management::attach_named(&mut session, None, "routed").await?;
    let response = management.receive_deferred(&[1], 0).await?;
    assert_status(&response, 200, None);
    let messages = entries(&response)?;
    assert_eq!(messages.len(), 1);
    assert_data(&messages[0].0, &payload);
    assert!(node.peek()?.is_empty());
    connection.close().await?;
    Ok(())
}

async fn a_peer_too_small_for_the_wrapper_rejects_before_mutation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let payload = vec![0x55; 256];
    let node = Node::start(provider, vec![payload.clone()], true).await?;
    let before = node.peek()?;
    let mut connection = node.connect().await?;
    let mut session = ClientSession::begin(&mut connection).await?;
    let mut tiny = Management::attach_named(&mut session, Some(128), "tiny").await?;
    let request = management_request(
        protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
        deferred_body(&[1], 0),
        MessageId::Ulong(1),
        None,
        &tiny.reply_to,
    );
    assert_rejected(
        tiny.requests.send(request).await?,
        protocol_amqp::MESSAGE_SIZE_EXCEEDED,
    );
    assert!(
        node.peek()? == before,
        "a request with no room for a reply must preserve deferred messages"
    );
    let mut management = Management::attach_named(&mut session, Some(2_048), "usable").await?;
    let response = management.receive_deferred(&[1], 0).await?;
    assert_status(&response, 200, None);
    let messages = entries(&response)?;
    assert_eq!(messages.len(), 1);
    assert_data(&messages[0].0, &payload);
    assert!(node.peek()?.is_empty());
    connection.close().await?;
    Ok(())
}

fn schedule_body(count: usize) -> TestResult<OrderedMap<Value, Value>> {
    let mut entries = Vec::with_capacity(count);
    for index in 0..count {
        let mut annotations = OrderedMap::new();
        annotations.insert(
            Symbol::from(protocol_amqp::SCHEDULED_ENQUEUE_TIME_ANNOTATION),
            Value::Timestamp(20_000_i64.into()),
        );
        let message = Message {
            properties: Some(Properties {
                message_id: Some(MessageId::String(format!("scheduled-{index}"))),
                ..Properties::default()
            }),
            message_annotations: Some(annotations.into()),
            body: Body::Data(vec![vec![0x66].into()]),
            ..Message::default()
        };
        entries.push(Value::Map(OrderedMap::from_iter([(
            key(protocol_amqp::MESSAGE),
            Value::Binary(encode_message(&message)?.into()),
        )])));
    }
    Ok(OrderedMap::from_iter([(
        key(protocol_amqp::MESSAGES),
        Value::List(entries),
    )]))
}

async fn scheduled_handle_replies_are_budgeted_before_enqueue<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, Vec::new(), false).await?;
    let mut connection = node.connect().await?;
    let mut session = ClientSession::begin(&mut connection).await?;
    let mut management = Management::attach(&mut session, Some(2_048)).await?;
    let response = management
        .request(
            protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
            schedule_body(256)?,
        )
        .await?;
    assert_status(&response, 403, Some(protocol_amqp::MESSAGE_SIZE_EXCEEDED));
    assert!(
        node.peek()?.is_empty(),
        "rejected scheduling must not enqueue"
    );
    let response = management
        .request(protocol_amqp::SCHEDULE_MESSAGE_OPERATION, schedule_body(1)?)
        .await?;
    assert_status(&response, 200, None);
    let Body::Value(Value::Map(body)) = &response.body else {
        panic!("the scheduling reply must contain sequence numbers");
    };
    let Some(Value::Array(sequences)) = body.get(&key(protocol_amqp::SEQUENCE_NUMBERS)) else {
        panic!("the scheduling reply must contain an array");
    };
    assert_eq!(sequences.as_slice(), &[Value::Long(1)]);
    let scheduled = node.peek()?;
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].status, MessageStatus::Scheduled);
    assert_eq!(scheduled[0].sequence, SequenceNumber::new(1));
    connection.close().await?;
    Ok(())
}

async fn a_compact_large_null_array_survives_send_peek_and_deferred_receive<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, Vec::new(), false).await?;
    let mut connection = node.connect().await?;
    let mut session = ClientSession::begin(&mut connection).await?;
    let mut management = Management::attach(&mut session, None).await?;
    let mut sender = ClientSender::attach(&mut session, "compact-sender", "orders").await?;
    let message = Message::builder()
        .properties(Properties {
            message_id: Some(MessageId::String("compact-nulls".to_owned())),
            ..Properties::default()
        })
        .body(Body::Value(Value::Array(Array::from(vec![
            Value::Null;
            65_000
        ]))))
        .build();
    assert!(
        encode_message(&message)?.len() < 1_024,
        "the array has a zero-width constructor"
    );
    assert!(matches!(sender.send(message).await?, Outcome::Accepted(_)));
    let response = management.peek(1).await?;
    assert_status(&response, 200, None);
    let messages = entries(&response)?;
    assert_eq!(messages.len(), 1);
    assert_null_array(&messages[0].0);
    let CommandOutcome::Received(Some(delivery)) = node.submit(CommandKind::Receive {
        mode: ReceiveMode::PeekLock,
        lock_duration_millis: None,
        session: None,
    })?
    else {
        panic!("the compact message must be available");
    };
    node.submit(CommandKind::Defer {
        sequence: delivery.sequence,
        lock_token: delivery.lock.expect("lock").token,
    })?;
    let response = management
        .receive_deferred(&[delivery.sequence.as_u64()], 0)
        .await?;
    assert_status(&response, 200, None);
    let messages = entries(&response)?;
    assert_eq!(messages.len(), 1);
    assert_null_array(&messages[0].0);
    assert_eq!(
        messages[0]
            .0
            .header
            .as_ref()
            .expect("transfer header")
            .delivery_count,
        1
    );
    assert!(node.peek()?.is_empty());
    sender.close().await?;
    connection.close().await?;
    Ok(())
}

fn assert_null_array(message: &Message) {
    let Body::Value(Value::Array(elements)) = &message.body else {
        panic!("the compact null array must retain its body shape");
    };
    assert_eq!(elements.len(), 65_000);
    assert!(elements.iter().all(|element| element == &Value::Null));
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
    oversized_peek_lock_batch,
    oversized_receive_and_delete_batch,
    a_small_peer_receives_fitting_peek_prefixes_without_mutation,
    an_oversized_first_peek_item_is_refused_but_later_pages_remain_readable,
    correlation_and_tracking_fields_reduce_the_remaining_reply_budget,
    a_missing_reply_route_rejects_mutation_until_a_reply_link_attaches,
    a_peer_too_small_for_the_wrapper_rejects_before_mutation,
    scheduled_handle_replies_are_budgeted_before_enqueue,
    a_compact_large_null_array_survives_send_peek_and_deferred_receive,
}
