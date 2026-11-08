//! Actual attached senders must not acquire authority over a newer owner.
//! Generation changes here are test-owned metadata faults, not a delete API.

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use amqp::{
    ApplicationProperties, Body, ClientConnection as Connection, ClientReceiver as Receiver,
    ClientSender as Sender, ClientSession as Session, Message, MessageAnnotations, OrderedMap,
    Outcome, Properties, Value, encode_message,
};
use domain::{
    CommandKind, EntityPath, NamespaceName, QueueConfig, StateMachine, SubscriptionConfig,
    SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use protocol_amqp::Broker as ProtocolBroker;
use server::{Broker, Clock, LocalProposer};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value as StoredValue, WriteBatch};
use testkit::StoreProvider;
use tokio::{net::TcpListener, time::timeout};

#[path = "../src/test_deadline.rs"]
mod test_deadline;
use test_deadline::WAIT;

#[derive(Clone, Default)]
struct Observations {
    host_clock: Arc<AtomicUsize>,
    stored_clock: Arc<AtomicUsize>,
    applies: Arc<AtomicUsize>,
}

impl Observations {
    fn reset(&self) {
        for count in [&self.host_clock, &self.stored_clock, &self.applies] {
            count.store(0, Ordering::SeqCst);
        }
    }

    fn assert_no_effect(&self) {
        assert_eq!(self.host_clock.load(Ordering::SeqCst), 0, "host clock");
        assert_eq!(
            self.stored_clock.load(Ordering::SeqCst),
            0,
            "domain clock path"
        );
        assert_eq!(self.applies.load(Ordering::SeqCst), 0, "state apply");
    }
}

impl Clock for Observations {
    fn now(&self) -> Timestamp {
        self.host_clock.fetch_add(1, Ordering::SeqCst);
        Timestamp::from_millis(1_000)
    }
}

#[derive(Clone)]
struct Observed<S> {
    raw: S,
    observations: Observations,
}

impl<S: StateStore> StateStore for Observed<S> {
    fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StorageError> {
        if key == keys::clock() {
            self.observations
                .stored_clock
                .fetch_add(1, Ordering::SeqCst);
        }
        self.raw.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observations.applies.fetch_add(1, Ordering::SeqCst);
        self.raw.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.raw.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, StoredValue)>, StorageError> {
        self.raw.scan_from(prefix, start, limit)
    }
}

fn message(id: &str, scheduled: bool) -> Message {
    let mut message = Message::builder()
        .properties(Properties {
            message_id: Some(id.to_owned().into()),
            ..Properties::default()
        })
        .body(Body::Data(vec![id.as_bytes().to_vec().into()]))
        .build();
    if scheduled {
        let mut annotations = MessageAnnotations::default();
        annotations.insert(
            "x-opt-scheduled-enqueue-time",
            Value::Timestamp(10_000.into()),
        );
        message.message_annotations = Some(annotations);
    }
    message
}

fn batch(scheduled: bool) -> Result<Message, Box<dyn Error>> {
    Ok(Message::builder()
        .body(Body::Data(vec![
            encode_message(&message("batch-one", scheduled))?.into(),
            encode_message(&message("batch-two", scheduled))?.into(),
        ]))
        .build())
}

fn schedule_request(id: &str, malformed: bool) -> Result<Message, Box<dyn Error>> {
    let mut body = OrderedMap::new();
    if !malformed {
        let mut entry = OrderedMap::new();
        entry.insert(
            Value::String("message-id".into()),
            Value::String(id.to_owned()),
        );
        entry.insert(
            Value::String("message".into()),
            Value::Binary(encode_message(&message(id, true))?.into()),
        );
        body.insert(
            Value::String("messages".into()),
            Value::List(vec![Value::Map(entry)]),
        );
    }
    Ok(Message::builder()
        .properties(Properties {
            message_id: Some(id.to_owned().into()),
            reply_to: Some("replies".into()),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert(
                    protocol_amqp::OPERATION_PROPERTY,
                    protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
                )
                .build(),
        )
        .body(Body::Value(Value::Map(body)))
        .build())
}

async fn schedule(
    requests: &mut Sender,
    responses: &mut Receiver,
    id: &str,
    malformed: bool,
    status: i32,
) -> Result<(), Box<dyn Error>> {
    assert!(matches!(
        requests.send(schedule_request(id, malformed)?).await?,
        Outcome::Accepted(_)
    ));
    let response = timeout(WAIT, responses.recv()).await??;
    let properties = response
        .message()
        .application_properties
        .as_ref()
        .expect("response properties");
    assert_eq!(
        properties.get(protocol_amqp::STATUS_CODE_PROPERTY),
        Some(&Value::Int(status)),
        "{properties:?}"
    );
    if status == 404 {
        assert_eq!(
            properties.get(protocol_amqp::ERROR_CONDITION_PROPERTY),
            Some(&Value::Symbol(protocol_amqp::NOT_FOUND.into()))
        );
    }
    responses.accept(&response).await?;
    Ok(())
}

fn rejected(outcome: Outcome, condition: &str) {
    let Outcome::Rejected(rejected) = outcome else {
        panic!("expected refusal, got {outcome:?}");
    };
    assert_eq!(
        rejected
            .error
            .expect("refusal condition")
            .condition
            .as_symbol()
            .as_str(),
        condition
    );
}

async fn attached_senders_reject_drift<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    for topic in [false, true] {
        let raw = provider.open()?;
        let observations = Observations::default();
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(Observed {
                raw: raw.clone(),
                observations: observations.clone(),
            }),
            observations.clone(),
        ));
        let handle = broker.handle();
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new(if topic { "topic" } else { "queue" })?;
        handle
            .submit(
                namespace.clone(),
                entity.clone(),
                if topic {
                    CommandKind::CreateTopic {
                        config: TopicConfig::default(),
                    }
                } else {
                    CommandKind::CreateQueue {
                        config: QueueConfig::default(),
                    }
                },
            )
            .await?;
        let wake_target = if topic {
            let name = SubscriptionName::new("child")?;
            handle
                .submit(
                    namespace.clone(),
                    entity.clone(),
                    CommandKind::CreateSubscription {
                        name: name.clone(),
                        config: SubscriptionConfig::default(),
                    },
                )
                .await?;
            entity.subscription(&name)?
        } else {
            entity.clone()
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let mut service = protocol_amqp::AmqpListener::new(handle.clone(), namespace.clone())
            .into_tcp_service(listener);
        let retirement = service.retirement_handle();
        let service_task = tokio::spawn(async move {
            service.serve().await;
            service.finish().await;
            service.take_exit()
        });
        let mut connection = Connection::builder()
            .container_id("retained-sender-client")
            .open(format!("amqp://{address}").as_str())
            .await?;
        let mut session = Session::begin(&mut connection).await?;
        let mut sender = Sender::attach(&mut session, "original", entity.as_str()).await?;
        let management_address = format!("{entity}/$management");
        let mut responses = Receiver::builder()
            .name("response")
            .source(management_address.clone())
            .target("replies")
            .attach(&mut session)
            .await?;
        let mut requests = Sender::attach(
            &mut session,
            "original-schedule",
            management_address.clone(),
        )
        .await?;
        // Neither sending link has sent anything yet. Capture must happen at
        // attach, not on its first transfer or first schedule operation.
        let head = keys::entity_metadata(&namespace, &entity);
        raw.apply(WriteBatch::default().put(
            head,
            codec::encode(&(2_u64, if topic { 1_u32 } else { 0 }, false))?,
        ))?;
        let before = raw.snapshot()?;
        observations.reset();
        let mut wake = Box::pin(handle.deliverable(&namespace, &wake_target));

        rejected(
            sender.send(message("stale-single", false)).await?,
            protocol_amqp::NOT_FOUND,
        );
        rejected(
            sender
                .send_with_format(
                    batch(false)?,
                    protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
                )
                .await?,
            protocol_amqp::NOT_FOUND,
        );
        rejected(
            sender.send(message("stale-scheduled", true)).await?,
            protocol_amqp::NOT_FOUND,
        );
        rejected(
            sender
                .send_with_format(
                    batch(true)?,
                    protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
                )
                .await?,
            protocol_amqp::NOT_FOUND,
        );
        // Decode/conversion refusals win over stale authority and leave the
        // link usable, even when every well-formed transfer must now refuse.
        rejected(
            sender
                .send_with_format(message("bad-format", false), 7)
                .await?,
            protocol_amqp::INVALID_FIELD,
        );
        rejected(
            sender
                .send_with_format(
                    Message::builder()
                        .body(Body::Data(vec![vec![255].into()]))
                        .build(),
                    protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
                )
                .await?,
            protocol_amqp::INVALID_FIELD,
        );
        schedule(&mut requests, &mut responses, "malformed", true, 400).await?;
        schedule(&mut requests, &mut responses, "stale-schedule", false, 404).await?;
        schedule(&mut requests, &mut responses, "still-stale", false, 404).await?;
        observations.assert_no_effect();
        assert_eq!(
            raw.snapshot()?,
            before,
            "including clock and allocation keys"
        );
        assert!(
            timeout(Duration::from_millis(10), wake.as_mut())
                .await
                .is_err(),
            "stale sends must not notify"
        );

        let mut fresh = Sender::attach(&mut session, "fresh", entity.as_str()).await?;
        let mut fresh_requests =
            Sender::attach(&mut session, "fresh-schedule", management_address).await?;
        observations.assert_no_effect();
        assert!(matches!(
            fresh.send(message("healthy", false)).await?,
            Outcome::Accepted(_)
        ));
        timeout(WAIT, wake.as_mut()).await?;
        assert!(matches!(
            fresh
                .send_with_format(
                    batch(false)?,
                    protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT
                )
                .await?,
            Outcome::Accepted(_)
        ));
        assert!(matches!(
            fresh.send(message("healthy-scheduled", true)).await?,
            Outcome::Accepted(_)
        ));
        schedule(
            &mut fresh_requests,
            &mut responses,
            "healthy-management",
            false,
            200,
        )
        .await?;
        assert!(observations.host_clock.load(Ordering::SeqCst) > 0);
        let committed = raw.snapshot()?;
        observations.reset();
        rejected(
            sender.send(message("old-link-after-new", false)).await?,
            protocol_amqp::NOT_FOUND,
        );
        schedule(
            &mut requests,
            &mut responses,
            "old-schedule-after-new",
            false,
            404,
        )
        .await?;
        observations.assert_no_effect();
        assert_eq!(raw.snapshot()?, committed);

        if !topic {
            // A management link remains usable for other operations when its
            // scheduling capture fails. Creating the name later must not let
            // that same link recapture authority on its first schedule.
            let mut missing =
                Sender::attach(&mut session, "missing-schedule", "missing/$management").await?;
            handle
                .submit(
                    namespace.clone(),
                    EntityPath::new("missing")?,
                    CommandKind::CreateQueue {
                        config: QueueConfig::default(),
                    },
                )
                .await?;
            let created = raw.snapshot()?;
            observations.reset();
            schedule(&mut missing, &mut responses, "missing-malformed", true, 400).await?;
            schedule(
                &mut missing,
                &mut responses,
                "missing-original-capture",
                false,
                404,
            )
            .await?;
            observations.assert_no_effect();
            assert_eq!(raw.snapshot()?, created);
            let peek = Message::builder()
                .properties(Properties {
                    message_id: Some("missing-capture-listen".to_owned().into()),
                    reply_to: Some("replies".into()),
                    ..Properties::default()
                })
                .application_properties(
                    ApplicationProperties::builder()
                        .insert(
                            protocol_amqp::OPERATION_PROPERTY,
                            protocol_amqp::PEEK_MESSAGE_OPERATION,
                        )
                        .build(),
                )
                .body(Body::Value(Value::Map(OrderedMap::from_iter([
                    (Value::String("from-sequence-number".into()), Value::Long(0)),
                    (Value::String("message-count".into()), Value::Int(1)),
                ]))))
                .build();
            assert!(matches!(missing.send(peek).await?, Outcome::Accepted(_)));
            let response = timeout(WAIT, responses.recv()).await??;
            assert_eq!(
                response
                    .message()
                    .application_properties
                    .as_ref()
                    .unwrap()
                    .get(protocol_amqp::STATUS_CODE_PROPERTY),
                Some(&Value::Int(204)),
                "failed scheduling authority must not fence unrelated Listen operations",
            );
            responses.accept(&response).await?;
            let mut now_present =
                Sender::attach(&mut session, "now-present-schedule", "missing/$management").await?;
            schedule(&mut now_present, &mut responses, "new-capture", false, 200).await?;
        }

        connection.close().await?;
        retirement.request();
        assert!(timeout(WAIT, service_task).await??.is_some());
        drop(broker);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_attached_senders_reject_drift() -> Result<(), Box<dyn Error>> {
    timeout(
        WAIT,
        attached_senders_reject_drift(testkit::MemoryProvider::new()),
    )
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_attached_senders_reject_drift() -> Result<(), Box<dyn Error>> {
    timeout(
        WAIT,
        attached_senders_reject_drift(testkit::DurableProvider::temporary()?),
    )
    .await?
}
