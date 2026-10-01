use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use amqp::{Array, FilterSet, Source, decode_message};
use protocol_amqp::{Attachment, BrokerRejection, EntityMetadata};
use server::BrokerHandle;

use super::*;

#[derive(Clone)]
struct CountedBroker {
    inner: BrokerHandle,
    submits: Arc<AtomicUsize>,
    waiting: Arc<Mutex<Vec<EntityPath>>>,
}

struct PendingWait {
    waiting: Arc<Mutex<Vec<EntityPath>>>,
    entity: EntityPath,
}

impl Drop for PendingWait {
    fn drop(&mut self) {
        let mut waiting = self.waiting.lock().expect("waiting receivers");
        let position = waiting
            .iter()
            .position(|entity| entity == &self.entity)
            .expect("registered receiver wait");
        waiting.remove(position);
    }
}

impl protocol_amqp::Broker for CountedBroker {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.submits.fetch_add(1, Ordering::SeqCst);
        protocol_amqp::Broker::submit(&self.inner, namespace, entity, kind).await
    }

    fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> impl Future<Output = Result<Option<EntityMetadata>, BrokerRejection>> + Send {
        protocol_amqp::Broker::entity_metadata(&self.inner, namespace, target)
    }

    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        let wait = protocol_amqp::Broker::deliverable(&self.inner, namespace, entity);
        let waiting = Arc::clone(&self.waiting);
        let entity = entity.clone();
        async move {
            waiting
                .lock()
                .expect("waiting receivers")
                .push(entity.clone());
            let _pending = PendingWait { waiting, entity };
            wait.await;
        }
    }
}

pub(super) struct Node<P: StoreProvider> {
    pub(super) store: P::Store,
    pub(super) clock: ManualClock,
    pub(super) namespace: NamespaceName,
    pub(super) topic: EntityPath,
    pub(super) alpha: EntityPath,
    pub(super) beta: EntityPath,
    pub(super) ordinary: EntityPath,
    pub(super) address: String,
    submits: Arc<AtomicUsize>,
    waiting: Arc<Mutex<Vec<EntityPath>>>,
    _broker: Broker,
    listener: JoinHandle<()>,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    pub(super) async fn start(provider: P) -> TestResult<Self> {
        Self::start_for_peek(provider, false).await
    }

    pub(super) async fn start_for_peek(provider: P, session_queue: bool) -> TestResult<Self> {
        let store = provider.open()?;
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("Orders")?;
        let machine = StateMachine::new(store.clone());
        machine.apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::from_millis(1_000),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ))?;
        if session_queue {
            machine.apply(&Command::new(
                namespace.clone(),
                EntityPath::new("Sessions")?,
                Timestamp::from_millis(1_000),
                CommandKind::CreateQueue {
                    config: domain::QueueConfig {
                        requires_session: true,
                        lock_duration_millis: 30_000,
                        ..domain::QueueConfig::default()
                    },
                },
            ))?;
        }
        for (name, requires_session) in [("Alpha", true), ("beta", true), ("ordinary", false)] {
            machine.apply(&Command::new(
                namespace.clone(),
                topic.clone(),
                Timestamp::from_millis(1_000),
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(name)?,
                    config: SubscriptionConfig {
                        requires_session,
                        lock_duration_millis: 30_000,
                        ..SubscriptionConfig::default()
                    },
                },
            ))?;
        }
        machine.apply(&Command::new(
            namespace.clone(),
            EntityPath::new("healthy")?,
            Timestamp::from_millis(1_000),
            CommandKind::CreateQueue {
                config: domain::QueueConfig::default(),
            },
        ))?;
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(machine, clock.clone()));
        let submits = Arc::new(AtomicUsize::new(0));
        let waiting = Arc::new(Mutex::new(Vec::new()));
        let counted = CountedBroker {
            inner: broker.handle(),
            submits: Arc::clone(&submits),
            waiting: Arc::clone(&waiting),
        };
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?.to_string();
        let ns = namespace.clone();
        let listener = tokio::spawn(async move {
            let _ = protocol_amqp::AmqpListener::new(counted, ns)
                .serve(socket)
                .await;
        });
        Ok(Self {
            alpha: topic.subscription(&SubscriptionName::new("Alpha")?)?,
            beta: topic.subscription(&SubscriptionName::new("beta")?)?,
            ordinary: topic.subscription(&SubscriptionName::new("ordinary")?)?,
            store,
            clock,
            namespace,
            topic,
            address,
            submits,
            waiting,
            _broker: broker,
            listener,
            _provider: provider,
        })
    }

    pub(super) async fn connect(&self) -> TestResult<ClientConnection> {
        Ok(timeout(
            DEADLINE,
            ClientConnection::builder()
                .container_id("topic-session-client")
                .open(&format!("amqp://{}", self.address)),
        )
        .await??)
    }

    pub(super) fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.store.snapshot()?)
    }

    pub(super) fn submissions(&self) -> usize {
        self.submits.load(Ordering::SeqCst)
    }

    pub(super) async fn wait_waiting(&self, entity: &EntityPath) -> TestResult {
        timeout(DEADLINE, async {
            loop {
                if self
                    .waiting
                    .lock()
                    .expect("waiting receivers")
                    .contains(entity)
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }

    pub(super) async fn wait_waiter_count(
        &self,
        entity: &EntityPath,
        expected: usize,
    ) -> TestResult {
        timeout(DEADLINE, async {
            loop {
                let count = self
                    .waiting
                    .lock()
                    .expect("waiting receivers")
                    .iter()
                    .filter(|waiting| *waiting == entity)
                    .count();
                if count == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }

    pub(super) fn record(
        &self,
        entity: &EntityPath,
        number: u64,
    ) -> TestResult<Option<domain::MessageRecord>> {
        self.store
            .get(&keys::message(
                &self.namespace,
                entity,
                SequenceNumber::new(number),
            ))?
            .map(|bytes| domain::MessageRecord::decode(&bytes).map_err(Into::into))
            .transpose()
    }

    pub(super) async fn wait_released(&self, entity: &EntityPath, id: &str) -> TestResult {
        let session_id = domain::SessionId::new(id)?;
        timeout(DEADLINE, async {
            loop {
                let session = StateMachine::new(self.store.clone()).session(
                    &self.namespace,
                    entity,
                    &session_id,
                )?;
                if session.is_none_or(|session| session.lock.is_none()) {
                    return Ok::<(), Box<dyn Error>>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        Ok(())
    }

    pub(super) async fn wait_removed(&self, entity: &EntityPath, number: u64) -> TestResult {
        timeout(DEADLINE, async {
            loop {
                if self.record(entity, number)?.is_none() {
                    return Ok::<(), Box<dyn Error>>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        Ok(())
    }

    pub(super) async fn wait_deferred(&self, entity: &EntityPath, number: u64) -> TestResult {
        timeout(DEADLINE, async {
            loop {
                if self
                    .record(entity, number)?
                    .is_some_and(|record| matches!(record.state, domain::MessageState::Deferred))
                {
                    return Ok::<(), Box<dyn Error>>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        Ok(())
    }

    pub(super) fn session(
        &self,
        entity: &EntityPath,
        id: &str,
    ) -> TestResult<domain::SessionRecord> {
        Ok(StateMachine::new(self.store.clone())
            .session(&self.namespace, entity, &domain::SessionId::new(id)?)?
            .expect("session record"))
    }
}

impl<P: StoreProvider> Drop for Node<P> {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

pub(super) async fn receiving(
    session: &mut ClientSession,
    name: &str,
    address: &str,
    id: Option<&str>,
) -> TestResult<ClientReceiver> {
    let mut filter = FilterSet::default();
    filter.insert(
        Symbol::from(protocol_amqp::SESSION_FILTER),
        id.map_or(Value::Null, |id| Value::String(id.into())),
    );
    Ok(timeout(
        DEADLINE,
        ClientReceiver::builder()
            .name(name)
            .source(Source::builder().address(address).filter(filter).build())
            .attach(session),
    )
    .await??)
}

pub(super) fn granted(receiver: &ClientReceiver) -> Option<&str> {
    match receiver
        .source()
        .as_ref()?
        .filter
        .as_ref()?
        .get(&Symbol::from(protocol_amqp::SESSION_FILTER))?
    {
        Value::String(id) => Some(id),
        _ => None,
    }
}

pub(super) fn message(body: &str, id: Option<&str>) -> Message {
    Message {
        header: Some(Header {
            ttl: Some(50_000),
            ..Header::default()
        }),
        properties: Some(Properties {
            message_id: Some(format!("id-{body}").into()),
            group_id: id.map(str::to_owned),
            ..Properties::default()
        }),
        body: Body::Data(vec![body.as_bytes().to_vec().into()]),
        ..Message::default()
    }
}

pub(super) fn body(message: &Message) -> &[u8] {
    let Body::Data(parts) = &message.body else {
        panic!("data body")
    };
    assert_eq!(parts.len(), 1);
    parts[0].as_ref()
}

pub(super) fn group(message: &Message) -> Option<&str> {
    message
        .properties
        .as_ref()
        .and_then(|properties| properties.group_id.as_deref())
}

pub(super) fn sequence(message: &Message) -> u64 {
    match message
        .message_annotations
        .as_ref()
        .and_then(|annotations| annotations.get(Symbol::from("x-opt-sequence-number")))
    {
        Some(Value::Long(sequence)) => u64::try_from(*sequence).expect("positive sequence"),
        other => panic!("sequence annotation: {other:?}"),
    }
}

pub(super) async fn recv(receiver: &mut ClientReceiver) -> TestResult<ClientDelivery> {
    Ok(timeout(DEADLINE, receiver.recv()).await??)
}

pub(super) fn accepted(outcome: Outcome) {
    assert!(matches!(outcome, Outcome::Accepted(_)), "{outcome:?}");
}

pub(super) fn map(
    entries: impl IntoIterator<Item = (&'static str, Value)>,
) -> OrderedMap<Value, Value> {
    entries
        .into_iter()
        .map(|(key, value)| (Value::String(key.into()), value))
        .collect()
}

pub(super) fn session_body(id: &str) -> OrderedMap<Value, Value> {
    map([(protocol_amqp::SESSION_ID, Value::String(id.into()))])
}

pub(super) fn status(message: &Message, expected: i32) {
    assert_eq!(
        message
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY)),
        Some(&Value::Int(expected))
    );
}

pub(super) fn response_map(message: &Message) -> &OrderedMap<Value, Value> {
    let Body::Value(Value::Map(body)) = &message.body else {
        panic!("management map")
    };
    body
}

pub(super) fn first_entry(message: &Message) -> &OrderedMap<Value, Value> {
    let Some(Value::List(messages)) =
        response_map(message).get(&Value::String(protocol_amqp::MESSAGES.into()))
    else {
        panic!("message list")
    };
    let [Value::Map(entry)] = messages.as_slice() else {
        panic!("one deferred message")
    };
    entry
}

pub(super) fn decoded(message: &Message) -> TestResult<Message> {
    let Some(Value::Binary(bytes)) =
        first_entry(message).get(&Value::String(protocol_amqp::MESSAGE.into()))
    else {
        panic!("encoded message")
    };
    Ok(decode_message(bytes)?)
}

pub(super) fn sequences(number: u64, id: &str) -> OrderedMap<Value, Value> {
    map([
        (
            protocol_amqp::SEQUENCE_NUMBERS,
            Value::Array(Array::from(vec![Value::Long(number as i64)])),
        ),
        (protocol_amqp::RECEIVER_SETTLE_MODE, Value::Uint(1)),
        (protocol_amqp::SESSION_ID, Value::String(id.into())),
    ])
}

pub(super) struct Management {
    sender: ClientSender,
    receiver: ClientReceiver,
    reply_to: String,
}

impl Management {
    pub(super) async fn attach(
        session: &mut ClientSession,
        name: &str,
        address: &str,
    ) -> TestResult<Self> {
        let reply_to = format!("{name}-replies");
        let receiver = timeout(
            DEADLINE,
            ClientReceiver::builder()
                .name(format!("{name}-responses"))
                .source(address)
                .target(reply_to.clone())
                .attach(session),
        )
        .await??;
        let sender = timeout(
            DEADLINE,
            ClientSender::attach(session, format!("{name}-requests"), address),
        )
        .await??;
        Ok(Self {
            sender,
            receiver,
            reply_to,
        })
    }

    pub(super) async fn request(
        &mut self,
        id: &str,
        operation: &str,
        link: &str,
        body: OrderedMap<Value, Value>,
    ) -> TestResult<Message> {
        self.request_inner(id, operation, Some(link), body).await
    }

    pub(super) async fn request_unassociated(
        &mut self,
        id: &str,
        operation: &str,
        body: OrderedMap<Value, Value>,
    ) -> TestResult<Message> {
        self.request_inner(id, operation, None, body).await
    }

    async fn request_inner(
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
                Value::String(link.to_owned()),
            );
        }
        accepted(
            timeout(
                DEADLINE,
                self.sender.send(
                    Message::builder()
                        .properties(Properties {
                            message_id: Some(id.to_owned().into()),
                            reply_to: Some(self.reply_to.clone()),
                            ..Properties::default()
                        })
                        .application_properties(properties)
                        .body(Body::Value(Value::Map(body)))
                        .build(),
                ),
            )
            .await??,
        );
        let delivery = recv(&mut self.receiver).await?;
        let response = delivery.message().clone();
        assert_eq!(
            response
                .properties
                .as_ref()
                .and_then(|properties| properties.correlation_id.clone()),
            Some(id.to_owned().into())
        );
        timeout(DEADLINE, self.receiver.accept(&delivery)).await??;
        Ok(response)
    }
}
