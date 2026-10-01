use amqp::{Array, Modified, Uuid, decode_message};

use super::*;

struct Management {
    sender: ClientSender,
    receiver: ClientReceiver,
    reply_to: String,
}

impl Management {
    async fn attach(session: &mut ClientSession, name: &str, address: &str) -> TestResult<Self> {
        let reply_to = format!("{name}-replies");
        let receiver = ClientReceiver::builder()
            .name(format!("{name}-responses"))
            .source(address)
            .target(reply_to.clone())
            .attach(session)
            .await?;
        let sender = ClientSender::attach(session, format!("{name}-requests"), address).await?;
        Ok(Self {
            sender,
            receiver,
            reply_to,
        })
    }

    async fn request(
        &mut self,
        id: &str,
        operation: &str,
        link: Option<&str>,
        body: OrderedMap<Value, Value>,
    ) -> TestResult<Message> {
        let mut properties = ApplicationProperties::builder()
            .insert(protocol_amqp::OPERATION_PROPERTY, operation.to_owned())
            .build();
        if let Some(link) = link {
            properties.0.insert(
                protocol_amqp::ASSOCIATED_LINK_NAME_PROPERTY.into(),
                Value::String(link.into()),
            );
        }
        accepted(
            self.sender
                .send(
                    Message::builder()
                        .properties(Properties {
                            message_id: Some(id.to_owned().into()),
                            reply_to: Some(self.reply_to.clone()),
                            ..Properties::default()
                        })
                        .application_properties(properties)
                        .body(Body::Value(Value::Map(body)))
                        .build(),
                )
                .await?,
        );
        let response = recv(&mut self.receiver).await?;
        let message = response.message().clone();
        assert_eq!(
            message
                .properties
                .as_ref()
                .and_then(|properties| properties.correlation_id.clone()),
            Some(id.to_owned().into())
        );
        self.receiver.accept(&response).await?;
        Ok(message)
    }
}

fn map(entries: impl IntoIterator<Item = (&'static str, Value)>) -> OrderedMap<Value, Value> {
    entries
        .into_iter()
        .map(|(key, value)| (Value::String(key.into()), value))
        .collect()
}

fn status(message: &Message, expected: i32) {
    assert_eq!(
        message
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY)),
        Some(&Value::Int(expected))
    );
}

fn first_entry(message: &Message) -> &OrderedMap<Value, Value> {
    let Body::Value(Value::Map(body)) = &message.body else {
        panic!("management body")
    };
    let Some(Value::List(messages)) = body.get(&Value::String(protocol_amqp::MESSAGES.into()))
    else {
        panic!("message list")
    };
    let [Value::Map(entry)] = messages.as_slice() else {
        panic!("one entry: {messages:?}")
    };
    entry
}

fn decoded(message: &Message) -> TestResult<Message> {
    let Some(Value::Binary(bytes)) =
        first_entry(message).get(&Value::String(protocol_amqp::MESSAGE.into()))
    else {
        panic!("encoded message")
    };
    Ok(decode_message(bytes)?)
}

fn token(number: u64) -> Uuid {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&number.to_be_bytes());
    Uuid::from(bytes)
}

pub(super) async fn peek_one(session: &mut ClientSession, address: &str) -> TestResult<Message> {
    let mut client = Management::attach(session, "dlq-management", address).await?;
    let response = client
        .request(
            "peek-dlq",
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            None,
            map([
                (protocol_amqp::FROM_SEQUENCE_NUMBER, Value::Long(1)),
                (protocol_amqp::MESSAGE_COUNT, Value::Int(1)),
            ]),
        )
        .await?;
    status(&response, 200);
    let message = decoded(&response)?;
    client.sender.close().await?;
    client.receiver.close().await?;
    Ok(message)
}

async fn subscription_management_scopes_peek_renew_defer_and_completion<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, "Orders", TopicConfig::default()).await?;
    let alpha = node.subscription("Alpha").await?;
    let beta = node.subscription("beta").await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut sender = ClientSender::attach(&mut session, "publisher", "Orders").await?;
    accepted(sender.send(rich(0)).await?);
    let mut first = ClientReceiver::attach(&mut session, "alpha-link", alpha.as_str()).await?;
    let mut second = ClientReceiver::attach(&mut session, "beta-link", beta.as_str()).await?;
    let a = recv(&mut first).await?;
    let b = recv(&mut second).await?;
    let mut management = Management::attach(
        &mut session,
        "alpha",
        "Orders/SUBSCRIPTIONS/Alpha/$MANAGEMENT",
    )
    .await?;
    let mut sibling = Management::attach(
        &mut session,
        "beta",
        "Orders/subscriptions/beta/$management",
    )
    .await?;
    for client in [&mut management, &mut sibling] {
        let peeked = client
            .request(
                "peek",
                protocol_amqp::PEEK_MESSAGE_OPERATION,
                None,
                map([
                    (protocol_amqp::FROM_SEQUENCE_NUMBER, Value::Long(1)),
                    (protocol_amqp::MESSAGE_COUNT, Value::Int(1)),
                ]),
            )
            .await?;
        status(&peeked, 200);
        content(&decoded(&peeked)?, &rich(0));
        assert_eq!(sequence(&decoded(&peeked)?), 1);
    }
    let before = node.snapshot()?;
    let wrong = sibling
        .request(
            "wrong-owner",
            protocol_amqp::RENEW_LOCK_OPERATION,
            Some("alpha-link"),
            map([(
                protocol_amqp::LOCK_TOKENS,
                Value::Array(Array::from(vec![Value::Uuid(token(1))])),
            )]),
        )
        .await?;
    status(&wrong, 410);
    assert_eq!(node.snapshot()?, before);
    node.clock.set(2_000);
    let renewed = management
        .request(
            "renew-alpha",
            protocol_amqp::RENEW_LOCK_OPERATION,
            Some("alpha-link"),
            map([(
                protocol_amqp::LOCK_TOKENS,
                Value::Array(Array::from(vec![Value::Uuid(token(1))])),
            )]),
        )
        .await?;
    status(&renewed, 200);
    let Body::Value(Value::Map(body)) = renewed.body else {
        panic!("renew body")
    };
    let lock_millis = SubscriptionConfig::default().lock_duration_millis;
    assert!(
        matches!(body.get(&Value::String(protocol_amqp::EXPIRATIONS.into())), Some(Value::Array(values)) if matches!(values.as_slice(), [Value::Timestamp(time)] if time.milliseconds() == (2_000 + lock_millis) as i64))
    );
    let beta_record: domain::MessageRecord = codec::decode(
        &node
            .store
            .as_ref()
            .expect("store")
            .get(&keys::message(
                &node.namespace,
                &beta,
                SequenceNumber::new(1),
            ))?
            .expect("beta copy"),
    )?;
    assert!(
        matches!(beta_record.state, domain::MessageState::Locked { locked_until, .. } if locked_until == Timestamp::from_millis(1_000 + lock_millis))
    );
    first
        .modify(
            &a,
            Modified {
                undeliverable_here: Some(true),
                ..Modified::default()
            },
        )
        .await?;
    timeout(DEADLINE, async {
        loop {
            let bytes = node
                .store
                .as_ref()
                .expect("store")
                .get(&keys::message(
                    &node.namespace,
                    &alpha,
                    SequenceNumber::new(1),
                ))?
                .expect("deferred copy");
            let record: domain::MessageRecord = codec::decode(&bytes)?;
            if matches!(record.state, domain::MessageState::Deferred) {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    let deferred = management
        .request(
            "receive-deferred",
            protocol_amqp::RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
            Some("alpha-link"),
            map([
                (
                    protocol_amqp::SEQUENCE_NUMBERS,
                    Value::Array(Array::from(vec![Value::Long(1)])),
                ),
                (protocol_amqp::RECEIVER_SETTLE_MODE, Value::Uint(1)),
            ]),
        )
        .await?;
    status(&deferred, 200);
    content(&decoded(&deferred)?, &rich(0));
    let Some(Value::Uuid(lock)) =
        first_entry(&deferred).get(&Value::String(protocol_amqp::LOCK_TOKEN.into()))
    else {
        panic!("deferred lock")
    };
    let completed = management
        .request(
            "complete-deferred",
            protocol_amqp::UPDATE_DISPOSITION_OPERATION,
            Some("alpha-link"),
            map([
                (
                    protocol_amqp::LOCK_TOKENS,
                    Value::Array(Array::from(vec![Value::Uuid(lock.clone())])),
                ),
                (
                    protocol_amqp::DISPOSITION_STATUS,
                    Value::String("completed".into()),
                ),
            ]),
        )
        .await?;
    status(&completed, 200);
    node.wait_len(&alpha, 0).await?;
    assert_eq!(node.peek(&beta).await?.len(), 1);
    second.accept(&b).await?;
    node.wait_len(&beta, 0).await?;
    connection.close().await?;
    Ok(())
}

async fn a_topic_management_endpoint_attaches_without_claiming_queue_operations<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, "Orders", TopicConfig::default()).await?;
    node.subscription("Alpha").await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut management = Management::attach(&mut session, "topic", "Orders/$MANAGEMENT").await?;
    let before = node.snapshot()?;
    let peeked = management
        .request(
            "topic-peek",
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            None,
            map([
                (protocol_amqp::FROM_SEQUENCE_NUMBER, Value::Long(1)),
                (protocol_amqp::MESSAGE_COUNT, Value::Int(1)),
            ]),
        )
        .await?;
    status(&peeked, 404);
    assert_eq!(
        peeked
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::ERROR_CONDITION_PROPERTY)),
        Some(&Value::Symbol(Symbol::from("amqp:not-found")))
    );
    let mut message = rich(0);
    let mut annotations = OrderedMap::new();
    annotations.insert(
        Symbol::from(protocol_amqp::SCHEDULED_ENQUEUE_TIME_ANNOTATION),
        Value::Timestamp(5_000_i64.into()),
    );
    message.message_annotations = Some(annotations.into());
    let entry = map([(
        protocol_amqp::MESSAGE,
        Value::Binary(encode_message(&message)?.into()),
    )]);
    let scheduled = management
        .request(
            "topic-schedule",
            protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
            None,
            map([(
                protocol_amqp::MESSAGES,
                Value::List(vec![Value::Map(entry)]),
            )]),
        )
        .await?;
    status(&scheduled, 500);
    assert_eq!(
        scheduled
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::ERROR_CONDITION_PROPERTY)),
        Some(&Value::Symbol(Symbol::from("amqp:not-implemented")))
    );
    assert_eq!(node.snapshot()?, before);
    connection.close().await?;
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 8, super::$case(::testkit::MemoryProvider::new())).await? })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 8, super::$case(::testkit::DurableProvider::temporary()?)).await? })+ }
    };
}

for_each_backend! {
    subscription_management_scopes_peek_renew_defer_and_completion,
    a_topic_management_endpoint_attaches_without_claiming_queue_operations,
}
