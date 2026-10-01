use amqp::Descriptor;

use super::{
    management::{map, status},
    *,
};

const RULE_DESCRIPTION_CODE: u64 = 0x0000013700000004;
const EMPTY_ACTION_CODE: u64 = 0x0000013700000005;
const TRUE_FILTER_CODE: u64 = 0x000001370000007;
const FALSE_FILTER_CODE: u64 = 0x000001370000008;
const CORRELATION_FILTER_CODE: u64 = 0x000001370000009;

struct RulesClient {
    sender: ClientSender,
    receiver: ClientReceiver,
    reply_to: String,
}

impl RulesClient {
    async fn attach(
        session: &mut ClientSession,
        name: &str,
        address: &str,
        maximum: u64,
    ) -> TestResult<Self> {
        let reply_to = format!("{name}-replies");
        let receiver = timeout(
            DEADLINE,
            ClientReceiver::builder()
                .name(format!("{name}-responses"))
                .source(address)
                .target(reply_to.clone())
                .max_message_size(maximum)
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

    async fn request(
        &mut self,
        id: &str,
        operation: &str,
        body: OrderedMap<Value, Value>,
    ) -> TestResult<Message> {
        timeout(DEADLINE, async {
            accepted(
                self.sender
                    .send(
                        Message::builder()
                            .properties(Properties {
                                message_id: Some(id.to_owned().into()),
                                reply_to: Some(self.reply_to.clone()),
                                ..Properties::default()
                            })
                            .application_properties(
                                ApplicationProperties::builder()
                                    .insert(protocol_amqp::OPERATION_PROPERTY, operation.to_owned())
                                    .build(),
                            )
                            .body(Body::Value(Value::Map(body)))
                            .build(),
                    )
                    .await?,
            );
            let delivery = self.receiver.recv().await?;
            let response = delivery.message().clone();
            assert_eq!(
                response
                    .properties
                    .as_ref()
                    .and_then(|properties| properties.correlation_id.clone()),
                Some(id.to_owned().into())
            );
            self.receiver.accept(&delivery).await?;
            Ok::<_, Box<dyn Error>>(response)
        })
        .await?
    }

    async fn add(&mut self, name: &str, field: &'static str, filter: Value) -> TestResult<Message> {
        self.request(
            name,
            protocol_amqp::ADD_RULE_OPERATION,
            map([
                ("rule-name", Value::String(name.into())),
                (
                    "rule-description",
                    Value::Map(map([
                        ("rule-name", Value::String(name.into())),
                        (field, filter),
                        ("sql-rule-action", Value::Null),
                    ])),
                ),
            ]),
        )
        .await
    }

    async fn remove(&mut self, name: &str) -> TestResult<Message> {
        self.request(
            name,
            protocol_amqp::REMOVE_RULE_OPERATION,
            map([("rule-name", Value::String(name.into()))]),
        )
        .await
    }

    async fn list(&mut self, top: i32, skip: i32) -> TestResult<Message> {
        self.request(
            "list",
            protocol_amqp::ENUMERATE_RULES_OPERATION,
            map([("top", Value::Int(top)), ("skip", Value::Int(skip))]),
        )
        .await
    }
}

fn sql(expression: &str) -> Value {
    Value::Map(map([("expression", Value::String(expression.into()))]))
}

fn correlation(subject: &str, properties: OrderedMap<Value, Value>) -> Value {
    Value::Map(map([
        ("label", Value::String(subject.into())),
        ("properties", Value::Map(properties)),
        ("correlation-id", Value::Null),
        ("message-id", Value::Null),
        ("to", Value::Null),
        ("reply-to", Value::Null),
        ("session-id", Value::Null),
        ("reply-to-session-id", Value::Null),
        ("content-type", Value::Null),
    ]))
}

fn described(value: &Value, code: u64) -> &[Value] {
    let Value::Described(described) = value else {
        panic!("described value: {value:?}")
    };
    assert_eq!(described.descriptor, Descriptor::Code(code));
    let Value::List(fields) = &described.value else {
        panic!("described list")
    };
    fields
}

fn listed(response: &Message) -> Vec<(String, Value, i64)> {
    status(response, 200);
    let Body::Value(Value::Map(body)) = &response.body else {
        panic!("rule response map")
    };
    let Some(Value::List(rules)) = body.get(&Value::String("rules".into())) else {
        panic!("rules list")
    };
    rules
        .iter()
        .map(|rule| {
            let Value::Map(entry) = rule else {
                panic!("rule entry map")
            };
            assert_eq!(entry.len(), 1);
            let fields = described(
                entry
                    .get(&Value::String("rule-description".into()))
                    .expect("rule description"),
                RULE_DESCRIPTION_CODE,
            );
            let [
                filter,
                action,
                Value::String(name),
                Value::Timestamp(created),
            ] = fields
            else {
                panic!("rule fields: {fields:?}")
            };
            assert!(described(action, EMPTY_ACTION_CODE).is_empty());
            (name.clone(), filter.clone(), created.milliseconds())
        })
        .collect()
}

async fn default_rules_crud_and_scalar_filters_preserve_independent_copies<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, "Orders", TopicConfig::default()).await?;
    let alpha = node.subscription("Alpha").await?;
    let beta = node.subscription("beta").await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut publisher = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "rule-publisher", "Orders"),
    )
    .await??;
    let mut first = RulesClient::attach(
        &mut session,
        "alpha-rules",
        "Orders/SUBSCRIPTIONS/Alpha/$MANAGEMENT",
        16_384,
    )
    .await?;
    let mut second = RulesClient::attach(
        &mut session,
        "beta-rules",
        "Orders/subscriptions/beta/$management",
        16_384,
    )
    .await?;
    for client in [&mut first, &mut second] {
        let rules = listed(&client.list(100, 0).await?);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].0, "$Default");
        assert!(described(&rules[0].1, TRUE_FILTER_CODE).is_empty());
        assert_eq!(rules[0].2, 1_000);
    }
    status(&first.remove("$Default").await?, 200);
    assert!(listed(&first.list(100, 0).await?).is_empty());
    accepted(timeout(DEADLINE, publisher.send(rich(0))).await??);
    assert!(node.peek(&alpha).await?.is_empty());
    assert_eq!(node.peek(&beta).await?.len(), 1);
    status(&first.add("off", "sql-filter", sql("1=0")).await?, 200);
    status(
        &first
            .add(
                "exact",
                "correlation-filter",
                correlation(
                    "subject-1",
                    map([("member", Value::Int(1)), ("nullable", Value::Null)]),
                ),
            )
            .await?,
        200,
    );
    let original = rich(1);
    accepted(timeout(DEADLINE, publisher.send(original.clone())).await??);
    let mut wrong = rich(2);
    wrong.properties.as_mut().expect("properties").subject = Some("subject-1".into());
    accepted(timeout(DEADLINE, publisher.send(wrong)).await??);
    assert_eq!(node.peek(&alpha).await?.len(), 1);
    assert_eq!(node.peek(&beta).await?.len(), 3);
    status(
        &first
            .add(
                "overlap",
                "correlation-filter",
                correlation("subject-1", map([])),
            )
            .await?,
        200,
    );
    let mut overlapped = rich(3);
    overlapped.properties.as_mut().expect("properties").subject = Some("subject-1".into());
    overlapped
        .application_properties
        .as_mut()
        .expect("application properties")
        .0
        .insert("member".into(), Value::Int(1));
    accepted(timeout(DEADLINE, publisher.send(overlapped.clone())).await??);
    assert_eq!(node.peek(&alpha).await?.len(), 2);
    assert_eq!(node.peek(&beta).await?.len(), 4);
    let rules = listed(&first.list(100, 0).await?);
    assert_eq!(
        rules.iter().map(|rule| rule.0.as_str()).collect::<Vec<_>>(),
        ["exact", "off", "overlap"]
    );
    let fields = described(&rules[0].1, CORRELATION_FILTER_CODE);
    assert_eq!(fields.len(), 9);
    assert_eq!(fields[4], Value::String("subject-1".into()));
    let Value::Map(properties) = &fields[8] else {
        panic!("correlation properties")
    };
    assert_eq!(
        properties.get(&Value::String("member".into())),
        Some(&Value::Int(1))
    );
    assert_eq!(
        properties.get(&Value::String("nullable".into())),
        Some(&Value::Null)
    );
    assert!(described(&rules[1].1, FALSE_FILTER_CODE).is_empty());
    assert_eq!(listed(&second.list(100, 0).await?)[0].0, "$Default");
    let mut receiver = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "alpha-delivery", alpha.as_str()),
    )
    .await??;
    for (expected, expected_sequence) in [(&original, 2), (&overlapped, 4)] {
        let delivery = recv(&mut receiver).await?;
        assert_eq!(sequence(delivery.message()), expected_sequence);
        content(delivery.message(), expected);
        timeout(DEADLINE, receiver.accept(&delivery)).await??;
    }
    node.wait_len(&alpha, 0).await?;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn rules_precede_null_session_dlq_and_scheduled_activation_uses_current_rules<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, "Orders", TopicConfig::default()).await?;
    let plain = node.subscription("plain").await?;
    let name = SubscriptionName::new("session")?;
    node.submit(
        &node.topic,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig {
                requires_session: true,
                ..SubscriptionConfig::default()
            },
        },
    )
    .await?;
    let required = node.topic.subscription(&name)?;
    let shadow = required.dead_letter_queue()?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut publisher = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "session-rule-publisher", "Orders"),
    )
    .await??;
    let mut required_rules = RulesClient::attach(
        &mut session,
        "required-rules",
        "Orders/subscriptions/session/$management",
        16_384,
    )
    .await?;
    let mut plain_rules = RulesClient::attach(
        &mut session,
        "plain-rules",
        "Orders/subscriptions/plain/$management",
        16_384,
    )
    .await?;
    status(&required_rules.remove("$Default").await?, 200);
    status(
        &required_rules
            .add("keep", "correlation-filter", correlation("keep", map([])))
            .await?,
        200,
    );
    accepted(timeout(DEADLINE, publisher.send(rich(0))).await??);
    assert!(node.peek(&shadow).await?.is_empty());
    let mut included = rich(1);
    included.properties.as_mut().expect("properties").subject = Some("keep".into());
    accepted(timeout(DEADLINE, publisher.send(included.clone())).await??);
    assert_eq!(node.peek(&shadow).await?.len(), 1);
    assert!(node.peek(&required).await?.is_empty());
    let mut dlq = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "rule-dlq", shadow.as_str()),
    )
    .await??;
    let dead = recv(&mut dlq).await?;
    assert_eq!(sequence(dead.message()), 2);
    assert_eq!(dead.message().body, included.body);
    assert_eq!(
        dead.message()
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get("DeadLetterReason")),
        Some(&Value::String("Session ID is null".into()))
    );
    timeout(DEADLINE, dlq.accept(&dead)).await??;
    node.wait_len(&shadow, 0).await?;
    status(&required_rules.remove("keep").await?, 200);
    status(
        &required_rules.add("off", "sql-filter", sql("1=0")).await?,
        200,
    );
    accepted(timeout(DEADLINE, publisher.send(rich(2))).await??);
    let old = node.peek(&plain).await?;
    assert_eq!(old.len(), 3);
    status(&plain_rules.remove("$Default").await?, 200);
    status(
        &plain_rules.add("off", "sql-filter", sql("1=0")).await?,
        200,
    );
    let mut future = rich(3);
    future.properties.as_mut().expect("properties").subject = Some("later".into());
    future
        .message_annotations
        .as_mut()
        .expect("annotations")
        .insert(
            Symbol::from(protocol_amqp::SCHEDULED_ENQUEUE_TIME_ANNOTATION),
            Value::Timestamp(2_000.into()),
        );
    accepted(timeout(DEADLINE, publisher.send(future.clone())).await??);
    assert_eq!(node.peek(&plain).await?, old);
    status(&plain_rules.remove("off").await?, 200);
    status(
        &plain_rules
            .add("later", "correlation-filter", correlation("later", map([])))
            .await?,
        200,
    );
    node.clock.set(2_000);
    assert_eq!(
        node.submit(&node.topic, CommandKind::ActivateScheduled)
            .await?,
        CommandOutcome::ScheduledActivated { activated: 1 }
    );
    let current = node.peek(&plain).await?;
    assert_eq!(current.len(), 4);
    assert_eq!(current[..3], old);
    assert_eq!(current[3].sequence.as_u64(), 5);
    assert!(node.peek(&required).await?.is_empty());
    assert!(node.peek(&shadow).await?.is_empty());
    let mut receiver = timeout(
        DEADLINE,
        ClientReceiver::attach(&mut session, "current-rule-deliveries", plain.as_str()),
    )
    .await??;
    for index in 0..4 {
        let delivery = recv(&mut receiver).await?;
        if index == 3 {
            assert_eq!(sequence(delivery.message()), 5);
            assert_eq!(delivery.message().body, future.body);
            assert_eq!(
                delivery.message().application_properties,
                future.application_properties
            );
            assert_eq!(
                delivery
                    .message()
                    .properties
                    .as_ref()
                    .and_then(|properties| properties.creation_time),
                Some(2_000)
            );
            assert_eq!(
                delivery
                    .message()
                    .message_annotations
                    .as_ref()
                    .and_then(|annotations| annotations.get(Symbol::from(
                        protocol_amqp::SCHEDULED_ENQUEUE_TIME_ANNOTATION
                    ))),
                Some(&Value::Timestamp(2_000.into()))
            );
        }
        timeout(DEADLINE, receiver.accept(&delivery)).await??;
    }
    node.wait_len(&plain, 0).await?;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn malformed_targets_and_complete_oversized_pages_refuse_without_mutation<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, "Orders", TopicConfig::default()).await?;
    node.subscription("Alpha").await?;
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut normal = RulesClient::attach(
        &mut session,
        "normal-rule-page",
        "Orders/subscriptions/Alpha/$management",
        16_384,
    )
    .await?;
    for (field, filter, expected) in [
        (
            "correlation-filter",
            Value::Map(map([("label", Value::Int(1))])),
            400,
        ),
        ("sql-filter", sql("newid()=NULL"), 501),
        (
            "correlation-filter",
            correlation("subject", map([("compound", Value::List(vec![]))])),
            501,
        ),
    ] {
        let before = node.snapshot()?;
        status(&normal.add("invalid", field, filter).await?, expected);
        assert_eq!(node.snapshot()?, before);
    }
    let mut topic_rules = RulesClient::attach(
        &mut session,
        "wrong-topic-rules",
        "Orders/$management",
        16_384,
    )
    .await?;
    let before = node.snapshot()?;
    status(
        &topic_rules
            .add("wrong-target", "sql-filter", sql("1=1"))
            .await?,
        400,
    );
    assert_eq!(node.snapshot()?, before);
    status(
        &normal
            .add(
                "large",
                "correlation-filter",
                correlation(&"x".repeat(2_048), map([])),
            )
            .await?,
        200,
    );
    let mut small = RulesClient::attach(
        &mut session,
        "small-rule-page",
        "Orders/subscriptions/Alpha/$management",
        1_024,
    )
    .await?;
    assert_eq!(listed(&small.list(1, 0).await?).len(), 1);
    let before = node.snapshot()?;
    let refused = small.list(100, 0).await?;
    status(&refused, 403);
    assert_eq!(refused.body, Body::Value(Value::Null));
    assert_eq!(node.snapshot()?, before);
    assert_eq!(listed(&normal.list(100, 0).await?).len(), 2);
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 8, super::$case(::testkit::MemoryProvider::new())).await? })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 8, super::$case(::testkit::DurableProvider::temporary()?)).await? })+ }
    };
}

for_each_backend! {
    default_rules_crud_and_scalar_filters_preserve_independent_copies,
    rules_precede_null_session_dlq_and_scheduled_activation_uses_current_rules,
    malformed_targets_and_complete_oversized_pages_refuse_without_mutation,
}

#[path = "sql_rules.rs"]
mod sql_rules;
