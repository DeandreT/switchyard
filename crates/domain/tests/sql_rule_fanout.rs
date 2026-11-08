//! SQL subscription rules: compilation before mutation, typed evaluation over
//! the stored filter projection during ordinary and scheduled fanout, null
//! semantics, filter-error dead-lettering versus non-matching skips, the
//! rule-record fence, and persistence across reopen on every storage backend.

use std::{collections::BTreeMap, error::Error, time::Instant};

use domain::{
    BrokerError, CONTENT_LIMITS, CodecError, Command, CommandKind, CommandOutcome,
    CorrelationFilter, CorrelationValue, DEFAULT_RULE_NAME, DeadLetterReason, EntityPath,
    FILTER_EVALUATION_DEAD_LETTER_REASON, FilterProperties, MAX_SQL_EXPRESSION_BYTES,
    MAX_TOPIC_SQL_RULES, MessageContent, MessageEnvelope, MessageState, NamespaceName,
    RuleConfigError, RuleDefinition, RuleFilter, RuleName, SequenceNumber, SqlCompileError,
    SqlCompileLimit, SqlRuleFilter, StateMachine, SubscriptionConfig, SubscriptionName, Timestamp,
    TopicConfig, codec, keys,
    legacy_content::validate_legacy_content,
    snapshot_validation::{validate_catalog, validate_complete_state},
};
use message_content::{
    AnnotationKey, Binary, Body, Content, Field, MessageId, Properties, Symbol, Text, Value, Width,
    encode_content,
};
use storage::{Key, StateStore, Value as Stored, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

macro_rules! paired {
    ($memory:ident, $fjall:ident, $body:ident) => {
        #[test]
        fn $memory() -> TestResult {
            $body(MemoryProvider::new())
        }
        #[test]
        fn $fjall() -> TestResult {
            $body(DurableProvider::temporary()?)
        }
    };
}

fn text(value: &str) -> Text {
    Text {
        text: value.into(),
        width: Width::One,
    }
}

fn string(value: &str) -> Value {
    Value::String(text(value))
}

/// AMQP content with a subject (`sys.Label`) and application properties.
fn message(id: &str, subject: Option<&str>, properties: &[(&str, Value)]) -> MessageContent {
    let content = Content {
        properties: Some(Properties {
            message_id: Field::Value(MessageId::String(text(id))),
            // Fields before a present subject must be encoded, as null.
            user_id: subject.map_or(Field::Absent, |_| Field::Null),
            to: subject.map_or(Field::Absent, |_| Field::Null),
            subject: subject.map_or(Field::Absent, |subject| Field::Value(text(subject))),
            ..Properties::default()
        }),
        application_properties: Some(
            properties
                .iter()
                .map(|(name, value)| (text(name), value.clone()))
                .collect(),
        ),
        body: Body::Data(vec![Binary {
            bytes: id.as_bytes().to_vec(),
            width: Width::Four,
        }]),
        ..Content::default()
    };
    MessageContent::Amqp(encode_content(&content, CONTENT_LIMITS).expect("test content encodes"))
}

fn scheduled(id: &str, at: u64, properties: &[(&str, Value)]) -> MessageContent {
    let MessageContent::Amqp(bytes) = message(id, None, properties) else {
        unreachable!()
    };
    let mut content = message_content::decode_content(&bytes, CONTENT_LIMITS).unwrap();
    content.message_annotations = Some(vec![(
        AnnotationKey::Symbol(Symbol {
            text: "x-opt-scheduled-enqueue-time".into(),
            width: Width::One,
        }),
        Value::Timestamp(at as i64),
    )]);
    MessageContent::Amqp(encode_content(&content, CONTENT_LIMITS).unwrap())
}

fn sql(expression: &str) -> RuleFilter {
    RuleFilter::Sql(SqlRuleFilter::new(expression))
}

struct Topology {
    namespace: NamespaceName,
    topic: EntityPath,
}

impl Topology {
    fn new() -> TestResult<Self> {
        Ok(Self {
            namespace: NamespaceName::new("tenant")?,
            topic: EntityPath::new("events")?,
        })
    }

    fn subscription(&self, name: &str) -> EntityPath {
        self.topic
            .subscription(&SubscriptionName::new(name).unwrap())
            .unwrap()
    }

    fn apply<S: StateStore>(
        &self,
        machine: &StateMachine<S>,
        entity: &EntityPath,
        at: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        machine.apply(&Command::new(
            self.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(at),
            kind,
        ))
    }

    /// Creates a subscription whose `$Default` rule is replaced by `rules`.
    fn filtered<S: StateStore>(
        &self,
        machine: &StateMachine<S>,
        name: &str,
        at: u64,
        rules: &[(&str, RuleFilter)],
    ) -> TestResult {
        self.apply(
            machine,
            &self.topic,
            at,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new(name)?,
                config: SubscriptionConfig::default(),
            },
        )?;
        let entity = self.subscription(name);
        self.apply(
            machine,
            &entity,
            at,
            CommandKind::DeleteRule {
                name: RuleName::new(DEFAULT_RULE_NAME)?,
            },
        )?;
        for (rule, filter) in rules {
            assert_eq!(
                self.apply(
                    machine,
                    &entity,
                    at,
                    CommandKind::CreateRule {
                        name: RuleName::new(*rule)?,
                        filter: filter.clone(),
                    },
                )?,
                CommandOutcome::RuleCreated
            );
        }
        Ok(())
    }

    fn publish<S: StateStore>(
        &self,
        machine: &StateMachine<S>,
        at: u64,
        content: MessageContent,
    ) -> Result<(SequenceNumber, Vec<EntityPath>), BrokerError> {
        match self.apply(
            machine,
            &self.topic,
            at,
            CommandKind::SendContent { content },
        )? {
            CommandOutcome::Published {
                mut sequences,
                subscriptions,
            } => Ok((sequences.remove(0), subscriptions)),
            other => panic!("expected a publication, got {other:?}"),
        }
    }

    fn ready<S: StateStore>(
        &self,
        machine: &StateMachine<S>,
        entity: &EntityPath,
        sequence: SequenceNumber,
    ) -> bool {
        machine
            .store()
            .get(&keys::ready(&self.namespace, entity, sequence))
            .unwrap()
            .is_some()
    }

    /// Where one sequence landed for one subscription.
    fn landed<S: StateStore>(
        &self,
        machine: &StateMachine<S>,
        name: &str,
        sequence: SequenceNumber,
    ) -> Landing {
        let entity = self.subscription(name);
        let delivered = machine.message(&self.namespace, &entity, sequence).unwrap();
        let dead = machine
            .dead_lettered_message(&self.namespace, &entity, sequence)
            .unwrap();
        match (delivered, dead) {
            (None, None) => Landing::Skipped,
            (Some(record), None) => {
                assert!(record.dead_letter.is_none());
                assert!(self.ready(machine, &entity, sequence));
                Landing::Delivered
            }
            (None, Some(record)) => {
                assert_eq!(record.state, MessageState::Ready);
                assert_eq!(record.expires_at, None);
                assert_eq!(record.sequence, sequence);
                assert!(self.ready(machine, &entity.dead_letter_queue().unwrap(), sequence));
                let info = record.dead_letter.expect("filter-error information");
                assert_eq!(
                    info.reason,
                    DeadLetterReason::Application(FILTER_EVALUATION_DEAD_LETTER_REASON.into())
                );
                assert_eq!(info.reason.as_str(), "FilterException");
                Landing::DeadLettered(info.description)
            }
            (Some(_), Some(_)) => panic!("one sequence landed twice for {name}"),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum Landing {
    Delivered,
    DeadLettered(String),
    Skipped,
}

fn dead(description: &str) -> Landing {
    Landing::DeadLettered(description.into())
}

fn image<S: StateStore>(machine: &StateMachine<S>) -> Vec<(Key, Stored)> {
    machine.store().snapshot().unwrap().entries().to_vec()
}

fn validated<S: StateStore>(machine: &StateMachine<S>) {
    let image = image(machine);
    validate_catalog(&image).unwrap();
    validate_complete_state(&image).unwrap();
    validate_legacy_content(&image).unwrap();
}

const SUBSCRIPTIONS: [&str; 5] = ["hot", "unset", "strict", "mixed", "plain"];

fn install<S: StateStore>(t: &Topology, machine: &StateMachine<S>) -> TestResult {
    t.apply(
        machine,
        &t.topic,
        1,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    t.filtered(
        machine,
        "hot",
        2,
        &[("hot", sql("priority > 5 AND sys.Label = 'urgent'"))],
    )?;
    t.filtered(machine, "unset", 2, &[("unset", sql("region IS NULL"))])?;
    t.filtered(machine, "strict", 2, &[("numeric", sql("region = 1"))])?;
    // A failing rule does not hide a matching sibling.
    t.filtered(
        machine,
        "mixed",
        2,
        &[
            ("a-numeric", sql("region = 1")),
            ("b-any", sql("priority > 0")),
        ],
    )?;
    t.apply(
        machine,
        &t.topic,
        2,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("plain")?,
            config: SubscriptionConfig::default(),
        },
    )?;
    Ok(())
}

/// Publishes the routing matrix and checks every landing. Returns the
/// sequences so a reopened store can be checked again.
fn route_matrix<S: StateStore>(
    t: &Topology,
    machine: &StateMachine<S>,
    at: u64,
) -> TestResult<Vec<SequenceNumber>> {
    let mismatch = "rule numeric: SQL operands have incompatible types";
    let cases = [
        (
            message(
                "typed",
                Some("urgent"),
                &[("Priority", Value::Int(7)), ("region", string("west"))],
            ),
            [
                Landing::Delivered,
                Landing::Skipped,
                dead(mismatch),
                Landing::Delivered,
                Landing::Delivered,
            ],
        ),
        // Missing property: comparisons are Unknown, IS NULL is True.
        (
            message("missing", Some("urgent"), &[("priority", Value::Long(1))]),
            [
                Landing::Skipped,
                Landing::Delivered,
                Landing::Skipped,
                Landing::Delivered,
                Landing::Delivered,
            ],
        ),
        // Explicit null behaves like a missing value in comparisons.
        (
            message(
                "null",
                Some("calm"),
                &[("priority", Value::UByte(9)), ("region", Value::Null)],
            ),
            [
                Landing::Skipped,
                Landing::Delivered,
                Landing::Skipped,
                Landing::Delivered,
                Landing::Delivered,
            ],
        ),
        // A referenced unsupported AMQP type is a filter error, not a null.
        (
            message(
                "timestamp",
                Some("urgent"),
                &[("priority", Value::Timestamp(5))],
            ),
            [
                dead("rule hot: SQL operand uses an unsupported input value"),
                Landing::Delivered,
                Landing::Skipped,
                dead("rule b-any: SQL operand uses an unsupported input value"),
                Landing::Delivered,
            ],
        ),
    ];
    let mut sequences = Vec::new();
    for (index, (content, expected)) in cases.into_iter().enumerate() {
        let (sequence, populated) = t.publish(machine, at + index as u64, content)?;
        let landed = SUBSCRIPTIONS
            .iter()
            .map(|name| t.landed(machine, name, sequence))
            .collect::<Vec<_>>();
        assert_eq!(landed, expected, "case {index}");
        // Delivered subscriptions first, then dead-letter queues, each in
        // canonical topology (name) order.
        let mut ordered = SUBSCRIPTIONS.iter().zip(&expected).collect::<Vec<_>>();
        ordered.sort_by_key(|(name, _)| **name);
        let mut wakes = Vec::new();
        for (name, landing) in &ordered {
            if **landing == Landing::Delivered {
                wakes.push(t.subscription(name));
            }
        }
        for (name, landing) in ordered {
            if matches!(landing, Landing::DeadLettered(_)) {
                wakes.push(t.subscription(name).dead_letter_queue()?);
            }
        }
        assert_eq!(populated, wakes, "case {index}");
        sequences.push(sequence);
    }
    Ok(sequences)
}

fn ordinary_fanout<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    install(&t, &machine)?;
    route_matrix(&t, &machine, 10)?;
    validated(&machine);
    let rules = machine.rules(&t.namespace, &t.subscription("mixed"), 0, 10)?;
    assert_eq!(
        rules.iter().map(|rule| &rule.filter).collect::<Vec<_>>(),
        [&sql("region = 1"), &sql("priority > 0")]
    );
    let before = image(&machine);
    drop(machine);

    // Reopened rules recompile from their stored text and route identically.
    let machine = StateMachine::new(provider.open()?);
    assert_eq!(image(&machine), before);
    assert_eq!(
        machine.rules(&t.namespace, &t.subscription("mixed"), 0, 10)?,
        rules
    );
    route_matrix(&t, &machine, 20)?;
    validated(&machine);
    Ok(())
}

paired!(
    ordinary_fanout_memory,
    ordinary_fanout_fjall,
    ordinary_fanout
);

fn refusal_without_mutation<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    install(&t, &machine)?;
    let entity = t.subscription("hot");
    let before = image(&machine);
    let oversized = format!("a = '{}'", "x".repeat(MAX_SQL_EXPRESSION_BYTES));
    for (expression, expected) in [
        ("priority >", SqlCompileError::Syntax),
        (
            "newid() = 1",
            SqlCompileError::Unsupported {
                feature: "function",
            },
        ),
        (
            oversized.as_str(),
            SqlCompileError::Limit {
                kind: SqlCompileLimit::SourceBytes,
                maximum: MAX_SQL_EXPRESSION_BYTES,
            },
        ),
    ] {
        let error = t
            .apply(
                &machine,
                &entity,
                30,
                CommandKind::CreateRule {
                    name: RuleName::new("rejected")?,
                    filter: sql(expression),
                },
            )
            .unwrap_err();
        match (error, expected) {
            (
                BrokerError::RuleConfig(RuleConfigError::Sql(SqlCompileError::Unsupported {
                    ..
                })),
                SqlCompileError::Unsupported { .. },
            ) => {}
            (error, expected) => assert_eq!(
                error,
                BrokerError::RuleConfig(RuleConfigError::Sql(expected)),
                "{expression}"
            ),
        }
        assert_eq!(image(&machine), before);
    }
    // A valid SQL rule under an existing name refuses without mutation too.
    assert_eq!(
        t.apply(
            &machine,
            &entity,
            30,
            CommandKind::CreateRule {
                name: RuleName::new("HOT")?,
                filter: sql("1 = 1"),
            },
        ),
        Err(BrokerError::RuleAlreadyExists {
            name: RuleName::new("hot")?
        })
    );
    assert_eq!(image(&machine), before);
    Ok(())
}

paired!(
    refusal_without_mutation_memory,
    refusal_without_mutation_fjall,
    refusal_without_mutation
);

fn activation_time_snapshot<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    t.apply(
        &machine,
        &t.topic,
        1,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    // At publication this rule would match; it is replaced before activation.
    t.filtered(&machine, "replaced", 2, &[("west", sql("region = 'west'"))])?;
    let (placeholder, populated) = t.publish(
        &machine,
        5,
        scheduled("later", 100, &[("region", string("west"))]),
    )?;
    assert!(populated.is_empty());
    let replaced = t.subscription("replaced");
    t.apply(
        &machine,
        &replaced,
        6,
        CommandKind::DeleteRule {
            name: RuleName::new("west")?,
        },
    )?;
    t.apply(
        &machine,
        &replaced,
        6,
        CommandKind::CreateRule {
            name: RuleName::new("east")?,
            filter: sql("region = 'east'"),
        },
    )?;
    // Created after publication, these still observe the activated message.
    t.filtered(&machine, "late", 7, &[("west", sql("region = 'west'"))])?;
    t.filtered(&machine, "broken", 7, &[("numeric", sql("region > 1"))])?;
    drop(machine);

    let machine = StateMachine::new(provider.open()?);
    assert_eq!(
        t.apply(&machine, &t.topic, 50, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 0,
            deliverable_entities: Vec::new(),
        }
    );
    let outcome = t.apply(&machine, &t.topic, 100, CommandKind::ActivateScheduled)?;
    let broken = t.subscription("broken");
    assert_eq!(
        outcome,
        CommandOutcome::ScheduledActivated {
            activated: 1,
            deliverable_entities: vec![t.subscription("late"), broken.dead_letter_queue()?],
        }
    );
    let active = SequenceNumber::new(placeholder.as_u64() + 1);
    assert_eq!(t.landed(&machine, "replaced", active), Landing::Skipped);
    assert_eq!(t.landed(&machine, "late", active), Landing::Delivered);
    assert_eq!(
        t.landed(&machine, "broken", active),
        dead("rule numeric: SQL operands have incompatible types")
    );
    let copy = machine
        .dead_lettered_message(&t.namespace, &broken, active)?
        .unwrap();
    assert_eq!(copy.enqueued_at, Timestamp::from_millis(100));
    assert_eq!(
        copy.dead_letter.unwrap().dead_lettered_at,
        Timestamp::from_millis(100)
    );
    validated(&machine);
    Ok(())
}

paired!(
    activation_time_snapshot_memory,
    activation_time_snapshot_fjall,
    activation_time_snapshot
);

/// A stored SQL rule this build cannot compile is corruption: readers,
/// fanout and snapshot validation refuse it without writing anything.
fn uncompilable_stored_rule<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    install(&t, &machine)?;
    let entity = t.subscription("hot");
    let name = RuleName::new("hot")?;
    let key = keys::subscription_rule(&t.namespace, &entity, &name);
    let corrupt = codec::rule::encode(&RuleDefinition {
        name,
        filter: sql("priority >"),
        created_at: Timestamp::from_millis(2),
    })?;
    assert_eq!(corrupt[0], codec::rule::VALUE_FORMAT_V2);
    machine
        .store()
        .apply(WriteBatch::default().put(key, corrupt))?;
    drop(machine);

    let machine = StateMachine::new(provider.open()?);
    let before = image(&machine);
    assert_eq!(
        machine.rules(&t.namespace, &entity, 0, 10),
        Err(BrokerError::EntityMetadataCorrupt)
    );
    assert_eq!(
        t.publish(&machine, 10, message("m", None, &[])),
        Err(BrokerError::EntityMetadataCorrupt)
    );
    assert!(validate_catalog(&before).is_err());
    assert_eq!(image(&machine), before);
    Ok(())
}

paired!(
    uncompilable_stored_rule_memory,
    uncompilable_stored_rule_fjall,
    uncompilable_stored_rule
);

#[test]
fn rule_record_fence_keeps_legacy_bytes_and_versions_sql() {
    let name = RuleName::new("Hot").unwrap();
    for filter in [RuleFilter::True, RuleFilter::False] {
        let definition = RuleDefinition {
            name: name.clone(),
            filter,
            created_at: Timestamp::from_millis(7),
        };
        let legacy = codec::encode(&definition).unwrap();
        assert_eq!(codec::rule::encode(&definition).unwrap(), legacy);
        assert_eq!(codec::rule::decode(&legacy).unwrap(), definition);
    }
    let definition = RuleDefinition {
        name: name.clone(),
        filter: sql("a = 1"),
        created_at: Timestamp::from_millis(7),
    };
    let encoded = codec::rule::encode(&definition).unwrap();
    assert_eq!(
        encoded,
        [2, 3, b'H', b'o', b't', 5, b'a', b' ', b'=', b' ', b'1', 7]
    );
    let decoded = codec::rule::decode(&encoded).unwrap();
    assert_eq!(decoded, definition);
    assert_eq!(decoded.name.display_name(), "Hot");
    // Readers that predate the fence refuse before their DTO.
    assert_eq!(
        codec::decode::<RuleDefinition>(&encoded),
        Err(CodecError::UnsupportedVersion { version: 2 })
    );
    for cut in 0..encoded.len() {
        assert!(codec::rule::decode(&encoded[..cut]).is_err());
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_eq!(codec::rule::decode(&trailing), Err(CodecError::Decode));
    // Nonminimal created_at varint.
    let mut alias = encoded[..encoded.len() - 1].to_vec();
    alias.extend([0x87, 0]);
    assert_eq!(codec::rule::decode(&alias), Err(CodecError::Decode));
    // A SQL filter inside the generic V1 envelope is not a rule record.
    let smuggled = codec::encode(&definition).unwrap();
    assert_eq!(smuggled[0], codec::VALUE_FORMAT_V1);
    assert_eq!(codec::rule::decode(&smuggled), Err(CodecError::Decode));
    // An invalid stored name never decodes.
    let mut bad_name = encoded;
    bad_name[2] = b' ';
    assert_eq!(codec::rule::decode(&bad_name), Err(CodecError::Decode));
    assert_eq!(
        codec::rule::decode(&[99, 0]),
        Err(CodecError::UnsupportedVersion { version: 99 })
    );
}

fn create_topic<S: StateStore>(
    t: &Topology,
    machine: &StateMachine<S>,
    topic: &EntityPath,
) -> TestResult {
    t.apply(
        machine,
        topic,
        1,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    Ok(())
}

/// A SQL error never hides a correlation or TRUE match, and dead-letters only
/// when no sibling of either kind matches.
fn mixed_rule_kinds<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    create_topic(&t, &machine, &t.topic)?;
    let urgent = RuleFilter::Correlation(CorrelationFilter {
        subject: Some("urgent".into()),
        ..CorrelationFilter::default()
    });
    t.filtered(
        &machine,
        "correlated",
        2,
        &[("a-sql", sql("region = 1")), ("b-correlation", urgent)],
    )?;
    t.filtered(
        &machine,
        "always",
        2,
        &[("a-sql", sql("region = 1")), ("b-true", RuleFilter::True)],
    )?;
    let failure = "rule a-sql: SQL operands have incompatible types";
    for (subject, correlated) in [("urgent", Landing::Delivered), ("calm", dead(failure))] {
        let (sequence, _) = t.publish(
            &machine,
            10,
            message(subject, Some(subject), &[("region", string("west"))]),
        )?;
        assert_eq!(t.landed(&machine, "correlated", sequence), correlated);
        assert_eq!(t.landed(&machine, "always", sequence), Landing::Delivered);
    }
    validated(&machine);
    Ok(())
}

paired!(
    mixed_rule_kinds_memory,
    mixed_rule_kinds_fjall,
    mixed_rule_kinds
);

/// One batch command stages a dead-letter copy per failing child and reports
/// the dead-letter queue once.
fn batch_dead_letters<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    create_topic(&t, &machine, &t.topic)?;
    t.filtered(&machine, "strict", 2, &[("numeric", sql("region = 1"))])?;
    let outcome = t.apply(
        &machine,
        &t.topic,
        10,
        CommandKind::SendContentBatch {
            messages: vec![
                message("west", None, &[("region", string("west"))]),
                message("missing", None, &[]),
                message("one", None, &[("region", Value::Int(1))]),
                message("east", None, &[("region", string("east"))]),
            ],
        },
    )?;
    let strict = t.subscription("strict");
    let CommandOutcome::Published {
        sequences,
        subscriptions,
    } = outcome
    else {
        panic!("expected a publication, got {outcome:?}");
    };
    assert_eq!(subscriptions, [strict.clone(), strict.dead_letter_queue()?]);
    let failure = "rule numeric: SQL operands have incompatible types";
    let landed = sequences
        .iter()
        .map(|sequence| t.landed(&machine, "strict", *sequence))
        .collect::<Vec<_>>();
    assert_eq!(
        landed,
        [
            dead(failure),
            Landing::Skipped,
            Landing::Delivered,
            dead(failure)
        ]
    );
    assert_eq!(
        machine.ready_sequences(&t.namespace, &strict.dead_letter_queue()?, 10)?,
        [sequences[0], sequences[3]]
    );
    validated(&machine);
    Ok(())
}

paired!(
    batch_dead_letters_memory,
    batch_dead_letters_fjall,
    batch_dead_letters
);

/// The topic-wide SQL quota refuses the next SQL rule deterministically, while
/// other filter kinds keep their own limits and a topic at the cap publishes.
fn sql_rule_quota<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    create_topic(&t, &machine, &t.topic)?;
    t.filtered(&machine, "a", 2, &[])?;
    t.filtered(&machine, "b", 2, &[])?;
    // Distinct texts, half of them literal LIKE patterns. Seeding rows
    // directly keeps the setup linear; the last rule goes through a command.
    let expression = |index: usize| {
        if index.is_multiple_of(2) {
            format!("name LIKE 'n{index}%'")
        } else {
            format!("priority = {index}")
        }
    };
    let mut seeded = WriteBatch::default();
    for index in 0..MAX_TOPIC_SQL_RULES - 1 {
        let entity = t.subscription(if index < 1_000 { "a" } else { "b" });
        let name = RuleName::new(format!("r{index:04}"))?;
        seeded.push_put(
            keys::subscription_rule(&t.namespace, &entity, &name),
            codec::rule::encode(&RuleDefinition {
                name,
                filter: sql(&expression(index)),
                created_at: Timestamp::from_millis(2),
            })?,
        );
    }
    machine.store().apply(seeded)?;
    let create = |entity: &str, name: &str, filter: RuleFilter| {
        t.apply(
            &machine,
            &t.subscription(entity),
            3,
            CommandKind::CreateRule {
                name: RuleName::new(name).unwrap(),
                filter,
            },
        )
    };
    assert_eq!(
        create("b", "r1999", sql(&expression(1_999))),
        Ok(CommandOutcome::RuleCreated)
    );
    let before = image(&machine);
    for entity in ["a", "b"] {
        assert_eq!(
            create(entity, "over", sql("1 = 1")),
            Err(BrokerError::SqlRuleLimitExceeded {
                maximum: MAX_TOPIC_SQL_RULES
            })
        );
        assert_eq!(image(&machine), before);
    }
    assert_eq!(
        create("a", "plain", RuleFilter::False),
        Ok(CommandOutcome::RuleCreated)
    );

    let started = Instant::now();
    let (sequence, populated) = t.publish(
        &machine,
        4,
        message(
            "capped",
            None,
            &[("name", string("n4x")), ("priority", Value::Long(1_001))],
        ),
    )?;
    let elapsed = started.elapsed();
    assert_eq!(populated, [t.subscription("a"), t.subscription("b")]);
    assert_eq!(t.landed(&machine, "a", sequence), Landing::Delivered);
    assert_eq!(t.landed(&machine, "b", sequence), Landing::Delivered);
    assert!(
        elapsed.as_secs() < 60,
        "publication at the SQL rule cap took {elapsed:?}"
    );
    validated(&machine);
    Ok(())
}

paired!(sql_rule_quota_memory, sql_rule_quota_fjall, sql_rule_quota);

/// An undecodable projected value is corruption once SQL rules must read it:
/// the command refuses without effects instead of routing a filter error.
fn undecodable_projection<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    install(&t, &machine)?;
    let plain = EntityPath::new("plain-events")?;
    t.apply(
        &machine,
        &plain,
        3,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    t.apply(
        &machine,
        &plain,
        3,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("all")?,
            config: SubscriptionConfig::default(),
        },
    )?;
    let send = || CommandKind::Send {
        message_id: "corrupt".into(),
        body: Vec::new(),
        time_to_live_millis: None,
        session_id: None,
        scheduled_enqueue_at: None,
        envelope: Some(
            MessageEnvelope::new(b"opaque".to_vec()).with_filter_properties(FilterProperties {
                application_properties: BTreeMap::from([(
                    "region".into(),
                    CorrelationValue::new(vec![0xff]).unwrap(),
                )]),
                ..FilterProperties::default()
            }),
        ),
    };
    let before = image(&machine);
    assert_eq!(
        t.apply(&machine, &t.topic, 10, send()),
        Err(BrokerError::Codec(CodecError::Decode))
    );
    assert_eq!(image(&machine), before);
    // Without SQL rules the projection is never decoded, as before.
    assert!(matches!(
        t.apply(&machine, &plain, 10, send())?,
        CommandOutcome::Published { .. }
    ));
    Ok(())
}

paired!(
    undecodable_projection_memory,
    undecodable_projection_fjall,
    undecodable_projection
);
