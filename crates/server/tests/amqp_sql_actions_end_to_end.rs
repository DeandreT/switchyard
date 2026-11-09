//! AMQP-installed actions, isolated copies and joined-owner durable recovery.

use std::error::Error;

use amqp::{
    ApplicationProperties, Body, ClientConnection as Connection, ClientReceiver as Receiver,
    ClientSender as Sender, ClientSession as Session, Message, MessageAnnotations, OrderedMap,
    Outcome, Properties, Value,
};
use domain::{
    CommandKind, CommandOutcome, EntityPath, NamespaceName, QueueConfig, RuleName, StateMachine,
    SubscriptionConfig, SubscriptionName, TopicConfig, keys,
};
use protocol_amqp::{AmqpListener, AmqpListenerExit, AmqpListenerRetirement};
use serde_amqp::{descriptor::Descriptor, primitives::Timestamp};
use server::{Broker, BrokerHandle, LocalProposer, ManualClock};
use storage::{StateStore, WriteBatch};
use testkit::StoreProvider;
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

#[path = "../src/test_deadline.rs"]
mod test_deadline;
use test_deadline::WAIT;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const TOPIC: &str = "actions";
const ACTION_CODE: u64 = 0x0000_0137_0000_0006;
const EMPTY_CODE: u64 = 0x0000_0137_0000_0005;
const ALPHA: &str = "SET byte=2;SET ubyte=2;SET short=2;SET ushort=2;SET int=2;SET uint=2;SET long=2;SET ulong=2;SET flag=FALSE;SET text='changed';SET fresh=3;SET maybe='present';REMOVE gone;REMOVE absent;SET route='alpha'";

fn namespace() -> NamespaceName {
    NamespaceName::new("tenant").unwrap()
}
fn entity(name: &str) -> EntityPath {
    EntityPath::new(name).unwrap()
}
fn path(subscription: &str) -> String {
    format!("{TOPIC}/subscriptions/{subscription}")
}
fn map(entries: impl IntoIterator<Item = (&'static str, Value)>) -> OrderedMap<Value, Value> {
    entries
        .into_iter()
        .map(|(key, value)| (Value::String(key.into()), value))
        .collect()
}
fn status(message: &Message) -> i32 {
    match message
        .application_properties
        .as_ref()
        .unwrap()
        .get(protocol_amqp::STATUS_CODE_PROPERTY)
    {
        Some(Value::Int(code)) => *code,
        _ => panic!("status code missing"),
    }
}

struct Wire {
    connection: Connection,
    session: Session,
    retirement: AmqpListenerRetirement,
    owner: JoinHandle<Option<AmqpListenerExit>>,
}

impl Wire {
    async fn start(handle: BrokerHandle) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let mut service = AmqpListener::new(handle, namespace()).into_tcp_service(listener);
        let retirement = service.retirement_handle();
        let owner = tokio::spawn(async move {
            service.serve().await;
            service.finish().await;
            service.take_exit()
        });
        let mut connection = Connection::builder()
            .container_id("sql-actions-client")
            .open(format!("amqp://{address}").as_str())
            .await?;
        let session = Session::begin(&mut connection).await?;
        Ok(Self {
            connection,
            session,
            retirement,
            owner,
        })
    }

    async fn finish(self) -> TestResult {
        eprintln!("sql-actions: closing original connection");
        self.connection.close().await?;
        self.retirement.request();
        assert!(
            matches!(
                timeout(WAIT, self.owner).await??,
                Some(AmqpListenerExit::Complete(Ok(())))
            ),
            "listener must drain, not time out or report-only"
        );
        eprintln!("sql-actions: original listener joined");
        Ok(())
    }
}

struct Management {
    requests: Sender,
    responses: Receiver,
    next: u32,
    reply: String,
}
impl Management {
    async fn attach(session: &mut Session, address: &str) -> TestResult<Self> {
        let node = format!("{address}/$management");
        let reply = format!("{address}-replies");
        let responses = Receiver::builder()
            .name(format!("{address}-responses"))
            .source(node.as_str())
            .target(reply.as_str())
            .attach(session)
            .await?;
        let requests =
            Sender::attach(session, format!("{address}-requests"), node.as_str()).await?;
        Ok(Self {
            requests,
            responses,
            next: 0,
            reply,
        })
    }
    async fn request(
        &mut self,
        operation: &str,
        body: OrderedMap<Value, Value>,
    ) -> TestResult<Message> {
        self.next += 1;
        let request = Message::builder()
            .properties(Properties {
                message_id: Some(format!("request-{}", self.next).into()),
                reply_to: Some(self.reply.clone()),
                ..Properties::default()
            })
            .application_properties(
                ApplicationProperties::builder()
                    .insert(protocol_amqp::OPERATION_PROPERTY, operation)
                    .build(),
            )
            .body(Body::Value(Value::Map(body)))
            .build();
        assert!(matches!(
            self.requests.send(request).await?,
            Outcome::Accepted(_)
        ));
        let delivery = timeout(WAIT, self.responses.recv()).await??;
        let message = delivery.message().clone();
        self.responses.accept(&delivery).await?;
        Ok(message)
    }
    async fn add(&mut self, name: &str, filter: &str, action: Option<&str>) -> TestResult {
        let response = self
            .request(
                protocol_amqp::ADD_RULE_OPERATION,
                map([
                    ("rule-name", Value::String(name.into())),
                    (
                        "rule-description",
                        Value::Map(map([
                            (
                                "sql-filter",
                                Value::Map(map([("expression", Value::String(filter.into()))])),
                            ),
                            (
                                "sql-rule-action",
                                action
                                    .map(|source| {
                                        Value::Map(map([(
                                            "expression",
                                            Value::String(source.into()),
                                        )]))
                                    })
                                    .unwrap_or(Value::Null),
                            ),
                        ])),
                    ),
                ]),
            )
            .await?;
        assert_eq!(status(&response), 200, "{response:?}");
        Ok(())
    }
    async fn remove(&mut self, name: &str) -> TestResult {
        let response = self
            .request(
                protocol_amqp::REMOVE_RULE_OPERATION,
                map([("rule-name", Value::String(name.into()))]),
            )
            .await?;
        assert_eq!(status(&response), 200, "{response:?}");
        Ok(())
    }
    async fn list(&mut self) -> TestResult<Vec<(String, Value)>> {
        let response = self
            .request(
                protocol_amqp::ENUMERATE_RULES_OPERATION,
                map([("skip", Value::Int(0)), ("top", Value::Int(100))]),
            )
            .await?;
        if status(&response) == 204 {
            return Ok(vec![]);
        }
        assert_eq!(status(&response), 200, "{response:?}");
        let Body::Value(Value::Map(body)) = response.body else {
            panic!("map")
        };
        let Value::List(entries) = body.get(&Value::String("rules".into())).unwrap() else {
            panic!("list")
        };
        Ok(entries
            .iter()
            .map(|entry| {
                let Value::Map(entry) = entry else {
                    panic!("entry")
                };
                let Value::Described(description) = entry
                    .get(&Value::String("rule-description".into()))
                    .unwrap()
                else {
                    panic!("description")
                };
                assert_eq!(
                    description.descriptor,
                    Descriptor::Code(0x0000_0137_0000_0004)
                );
                let Value::List(fields) = &description.value else {
                    panic!("fields")
                };
                assert_eq!(fields.len(), 4);
                let Value::String(name) = &fields[2] else {
                    panic!("name")
                };
                (name.clone(), fields[1].clone())
            })
            .collect())
    }
    async fn empty(&mut self) -> TestResult {
        let response = self
            .request(
                protocol_amqp::PEEK_MESSAGE_OPERATION,
                map([
                    ("from-sequence-number", Value::Long(1)),
                    ("message-count", Value::Int(100)),
                ]),
            )
            .await?;
        assert_eq!(
            status(&response),
            204,
            "unexpected extra copies: {response:?}"
        );
        Ok(())
    }
}

fn publication(label: &str, scheduled: bool) -> Message {
    let mut application = ApplicationProperties::default();
    for (name, value) in [
        ("byte", Value::Byte(1)),
        ("ubyte", Value::Ubyte(1)),
        ("short", Value::Short(1)),
        ("ushort", Value::Ushort(1)),
        ("int", Value::Int(1)),
        ("uint", Value::Uint(1)),
        ("long", Value::Long(1)),
        ("ulong", Value::Ulong(1)),
        ("flag", Value::Bool(true)),
        ("text", Value::String("original".into())),
        ("maybe", Value::Null),
        ("gone", Value::Null),
        ("fraction", Value::Double(0.5.into())),
        ("tiny", Value::Ubyte(1)),
    ] {
        application.insert(name, value);
    }
    let mut annotations = MessageAnnotations::default();
    if scheduled {
        annotations.insert(
            "x-opt-scheduled-enqueue-time",
            Value::Timestamp(Timestamp::from_milliseconds(5_000)),
        );
    }
    Message {
        properties: Some(Properties {
            message_id: Some(label.to_owned().into()),
            correlation_id: Some("correlation".into()),
            subject: Some("untouched".into()),
            ..Properties::default()
        }),
        application_properties: Some(application),
        message_annotations: scheduled.then_some(annotations),
        body: Body::Data(vec![
            vec![0, 255, 1, 254].into(),
            label.as_bytes().to_vec().into(),
        ]),
        ..Message::default()
    }
}

async fn copies(
    session: &mut Session,
    address: &str,
    count: usize,
    original: &Message,
) -> TestResult<Vec<Message>> {
    let mut receiver = Receiver::attach(session, format!("{address}-receiver"), address).await?;
    let mut messages = Vec::new();
    for _ in 0..count {
        let delivery = timeout(WAIT, receiver.recv()).await??;
        let message = delivery.message().clone();
        assert_eq!(message.body, original.body);
        assert_eq!(
            message.properties, original.properties,
            "IDs and system properties stay original"
        );
        receiver.accept(&delivery).await?;
        messages.push(message);
    }
    Ok(messages)
}

fn property<'a>(message: &'a Message, name: &str) -> Option<&'a Value> {
    message
        .application_properties
        .as_ref()
        .and_then(|properties| properties.get(name))
}
fn assert_ordinary(messages: &[Message], original: &Message) {
    assert_eq!(
        messages.len(),
        3,
        "one deduplicated actionless and two independent actions"
    );
    let actionless = messages
        .iter()
        .find(|message| property(message, "route").is_none())
        .unwrap();
    assert_eq!(
        actionless.application_properties,
        original.application_properties
    );
    let alpha = messages
        .iter()
        .find(|message| property(message, "route") == Some(&Value::String("alpha".into())))
        .unwrap();
    for (name, value) in [
        ("byte", Value::Byte(2)),
        ("ubyte", Value::Ubyte(2)),
        ("short", Value::Short(2)),
        ("ushort", Value::Ushort(2)),
        ("int", Value::Int(2)),
        ("uint", Value::Uint(2)),
        ("long", Value::Long(2)),
        ("ulong", Value::Ulong(2)),
        ("flag", Value::Bool(false)),
        ("text", Value::String("changed".into())),
        ("fresh", Value::Long(3)),
        ("maybe", Value::String("present".into())),
    ] {
        assert_eq!(
            property(alpha, name),
            Some(&value),
            "exact constructor {name}"
        );
    }
    assert_eq!(property(alpha, "gone"), None, "REMOVE is not null");
    assert_eq!(property(alpha, "absent"), None);
    let beta = messages
        .iter()
        .find(|message| property(message, "route") == Some(&Value::String("beta".into())))
        .unwrap();
    for (key, value) in &original.application_properties.as_ref().unwrap().0 {
        assert_eq!(
            property(beta, key),
            Some(value),
            "no alpha leakage into beta"
        );
    }
}

async fn finish_phase<S: StateStore>(
    wire: Wire,
    broker: Broker,
    raw: S,
) -> TestResult<storage::StoreSnapshot> {
    wire.finish().await?;
    // Broker::drop joins its actual owner; no store reopen before this returns.
    drop(broker);
    eprintln!("sql-actions: original broker owner joined");
    let snapshot = raw.snapshot()?;
    drop(raw);
    Ok(snapshot)
}

async fn exercise<P: StoreProvider>(provider: P) -> TestResult {
    let raw = provider.open()?;
    let clock = ManualClock::at(1_000);
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(raw.clone()),
        clock.clone(),
    ));
    let handle = broker.handle();
    handle
        .submit(
            namespace(),
            entity(TOPIC),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )
        .await?;
    handle
        .submit(
            namespace(),
            entity("healthy"),
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        )
        .await?;
    for (subscription, dead_letter) in [("retain", true), ("discard", false)] {
        handle
            .submit(
                namespace(),
                entity(TOPIC),
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(subscription)?,
                    config: SubscriptionConfig {
                        dead_lettering_on_filter_evaluation_exceptions: dead_letter,
                        ..SubscriptionConfig::default()
                    },
                },
            )
            .await?;
    }
    let mut wire = Wire::start(handle.clone()).await?;
    let mut managers = Vec::new();
    for subscription in ["retain", "discard"] {
        let mut management = Management::attach(&mut wire.session, &path(subscription)).await?;
        management.remove("$Default").await?;
        for (name, filter, action) in [
            ("plain-a", "1=1", None),
            ("plain-b", "1=1", None),
            ("alpha", "1=1", Some(ALPHA)),
            ("beta", "1=1", Some("SET route='beta'")),
            ("never", "route='alpha'", Some("SET leaked=TRUE")),
            ("bad-type", "1=1", Some("SET text=1")),
            ("bad-overflow", "1=1", Some("SET tiny=256")),
            ("bad-target", "1=1", Some("SET fraction=1")),
        ] {
            management.add(name, filter, action).await?;
        }
        let listed = management.list().await?;
        assert_eq!(listed.len(), 8);
        for (name, action) in listed {
            let Value::Described(action) = action else {
                panic!("action descriptor")
            };
            if name.starts_with("plain-") {
                assert_eq!(action.descriptor, Descriptor::Code(EMPTY_CODE));
                assert_eq!(action.value, Value::List(vec![]));
            } else {
                assert_eq!(action.descriptor, Descriptor::Code(ACTION_CODE));
                let Value::List(fields) = action.value else {
                    panic!("action list")
                };
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[1], Value::Int(20));
                if name == "alpha" {
                    assert_eq!(fields[0], Value::String(ALPHA.into()));
                }
            }
        }
        managers.push(management);
    }
    let mut sender = Sender::attach(&mut wire.session, "publications", TOPIC).await?;
    let ordinary = publication("ordinary", false);
    assert!(matches!(
        sender.send(ordinary.clone()).await?,
        Outcome::Accepted(_)
    ));
    for (index, subscription) in ["retain", "discard"].into_iter().enumerate() {
        assert_ordinary(
            &copies(&mut wire.session, &path(subscription), 3, &ordinary).await?,
            &ordinary,
        );
        managers[index].empty().await?;
        let address = format!("{}/$deadletterqueue", path(subscription));
        if index == 0 {
            let failed = copies(&mut wire.session, &address, 3, &ordinary).await?;
            for message in failed {
                assert_eq!(
                    property(&message, protocol_amqp::DEAD_LETTER_REASON_PROPERTY),
                    Some(&Value::String("ActionException".into()))
                );
                assert_eq!(
                    property(&message, protocol_amqp::DEAD_LETTER_DESCRIPTION_PROPERTY),
                    Some(&Value::String(
                        "SQL action failed; no changes were applied".into()
                    ))
                );
                for (key, value) in &ordinary.application_properties.as_ref().unwrap().0 {
                    assert_eq!(
                        property(&message, key),
                        Some(value),
                        "failure has no partial changes"
                    );
                }
                assert_eq!(property(&message, "route"), None);
            }
        }
        Management::attach(&mut wire.session, &address)
            .await?
            .empty()
            .await?;
    }
    eprintln!("sql-actions: ordinary exact copies and failure policies checked");
    let scheduled = publication("scheduled", true);
    assert!(matches!(
        sender.send(scheduled.clone()).await?,
        Outcome::Accepted(_)
    ));
    for management in &mut managers {
        for name in [
            "alpha",
            "beta",
            "never",
            "bad-type",
            "bad-overflow",
            "bad-target",
        ] {
            management.remove(name).await?;
        }
        management
            .add("beta", "1=1", Some("SET route='current'"))
            .await?;
        management
            .add("new", "1=1", Some("REMOVE gone;SET activated=TRUE"))
            .await?;
        management.empty().await?;
    }
    drop(sender);
    drop(managers);
    drop(handle);
    let before_reopen = finish_phase(wire, broker, raw).await?;
    let raw = provider.open()?;
    assert_eq!(
        raw.snapshot()?,
        before_reopen,
        "joined-owner reopen keeps every record/index/checkpoint byte"
    );
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(raw.clone()),
        clock.clone(),
    ));
    let handle = broker.handle();
    clock.set(5_000);
    assert!(matches!(
        handle
            .submit(namespace(), entity(TOPIC), CommandKind::ActivateScheduled)
            .await?,
        CommandOutcome::ScheduledActivated { activated: 1, .. }
    ));
    let mut wire = Wire::start(handle.clone()).await?;
    for subscription in ["retain", "discard"] {
        let received = copies(&mut wire.session, &path(subscription), 3, &scheduled).await?;
        assert_eq!(
            received
                .iter()
                .filter(
                    |message| property(message, "route") == Some(&Value::String("current".into()))
                )
                .count(),
            1
        );
        assert_eq!(
            received
                .iter()
                .filter(|message| property(message, "activated") == Some(&Value::Bool(true)))
                .count(),
            1
        );
        for message in received {
            assert_eq!(
                property(&message, "int"),
                Some(&Value::Int(1)),
                "deleted alpha never runs at activation"
            );
            assert_ne!(
                property(&message, "route"),
                Some(&Value::String("beta".into()))
            );
            if property(&message, "activated").is_some() {
                assert_eq!(property(&message, "gone"), None);
                assert_eq!(property(&message, "route"), None);
            } else {
                assert_eq!(property(&message, "gone"), Some(&Value::Null));
            }
        }
        let mut management = Management::attach(&mut wire.session, &path(subscription)).await?;
        management.empty().await?;
        assert_eq!(management.list().await?.len(), 4);
        for name in ["plain-a", "plain-b", "beta", "new"] {
            management.remove(name).await?;
        }
        assert!(management.list().await?.is_empty());
    }
    eprintln!("sql-actions: scheduled activation uses recovered current rules");

    // Inject only an owned catalog row. A touched corrupt topic is atomic; it
    // must not become an empty action or poison an unrelated healthy queue.
    let mut management = Management::attach(&mut wire.session, &path("retain")).await?;
    management
        .add("corrupt", "1=1", Some("REMOVE gone"))
        .await?;
    let key = keys::subscription_rule(
        &namespace(),
        &entity(&path("retain")),
        &RuleName::new("corrupt")?,
    );
    {
        let baseline = publication("healthy-before-corruption", false);
        let mut healthy_before =
            Sender::attach(&mut wire.session, "healthy-before-corruption", "healthy").await?;
        assert!(matches!(
            healthy_before.send(baseline.clone()).await?,
            Outcome::Accepted(_)
        ));
        let mut receiver = Receiver::attach(
            &mut wire.session,
            "healthy-before-corruption-receiver",
            "healthy",
        )
        .await?;
        let delivery = timeout(WAIT, receiver.recv()).await??;
        let received = delivery.message();
        assert_eq!(received.body, baseline.body);
        assert_eq!(received.properties, baseline.properties);
        assert_eq!(
            received.application_properties,
            baseline.application_properties
        );
        receiver.accept(&delivery).await?;
        // Client link drop does not detach. Close this baseline's links so an
        // abandoned receiver cannot consume the later AFTER-control message.
        receiver.close().await?;
        healthy_before.close().await?;
    }
    eprintln!("sql-actions: healthy queue baseline before catalog corruption checked");

    // First-mode accept writes a disposition; even link close does not prove
    // that the broker's Complete has committed. Drain the entire original
    // connection/listener and join the broker before freezing a store image.
    // Otherwise delayed baseline settlement could cross the corruption
    // snapshot frontier and look like mutation by a rejected rule operation.
    drop(management);
    drop(handle);
    let baseline_image = finish_phase(wire, broker, raw).await?;
    let raw = provider.open()?;
    assert_eq!(
        raw.snapshot()?,
        baseline_image,
        "baseline joined-owner reopen preserves the complete canonical image"
    );
    // These are positive completion witnesses, not exclusions from the whole
    // image comparison: no baseline message or expiry row may remain persisted.
    for prefix in [
        keys::message_prefix(&namespace(), &entity("healthy")),
        keys::expiry_prefix(&namespace(), &entity("healthy")),
    ] {
        assert!(
            raw.scan_prefix(&prefix, 1)?.is_empty(),
            "baseline Complete must be persisted before catalog corruption"
        );
    }
    eprintln!("sql-actions: baseline Complete persisted after original-owner join and reopen");
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(raw.clone()),
        clock.clone(),
    ));
    let handle = broker.handle();
    let mut wire = Wire::start(handle.clone()).await?;
    let mut management = Management::attach(&mut wire.session, &path("retain")).await?;
    raw.apply(WriteBatch::default().put(key.clone(), vec![3, 0xff]))?;
    let corrupt_snapshot = raw.snapshot()?;
    let response = management
        .request(
            protocol_amqp::ENUMERATE_RULES_OPERATION,
            map([("skip", Value::Int(0)), ("top", Value::Int(100))]),
        )
        .await?;
    assert_eq!(status(&response), 500);
    assert_eq!(
        raw.snapshot()?,
        corrupt_snapshot,
        "corrupt enumeration cannot mutate any catalog/index/checkpoint"
    );
    let mut sender = Sender::attach(&mut wire.session, "corrupt-topic", TOPIC).await?;
    assert!(matches!(
        sender.send(publication("refused", false)).await?,
        Outcome::Rejected(_)
    ));
    assert_eq!(
        raw.snapshot()?,
        corrupt_snapshot,
        "corrupt fanout cannot partially mutate a sibling"
    );
    let mut healthy = Sender::attach(&mut wire.session, "healthy-queue", "healthy").await?;
    assert!(matches!(
        healthy.send(ordinary.clone()).await?,
        Outcome::Accepted(_)
    ));
    assert_eq!(
        copies(&mut wire.session, "healthy", 1, &ordinary)
            .await?
            .len(),
        1
    );
    Management::attach(&mut wire.session, "healthy")
        .await?
        .empty()
        .await?;
    // Remove the fixture damage before recovery validation, preserving the
    // exact corrupt-state refusal proof above rather than weakening admission.
    raw.apply(WriteBatch::default().delete(key))?;
    drop(management);
    drop(sender);
    drop(healthy);
    drop(handle);
    let final_snapshot = finish_phase(wire, broker, raw).await?;
    let reopened = provider.open()?;
    assert_eq!(reopened.snapshot()?, final_snapshot);
    eprintln!("sql-actions: final joined-owner recovery checked");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_sql_actions_wire_lifecycle() -> TestResult {
    exercise(testkit::MemoryProvider::new()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjall_sql_actions_wire_lifecycle() -> TestResult {
    exercise(testkit::DurableProvider::temporary()?).await
}
