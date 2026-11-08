//! The subscription setting `dead_lettering_on_filter_evaluation_exceptions`:
//! discard versus dead-letter for ordinary, batch and scheduled fanout, the
//! membership-record fence, updates, reopen and snapshot validation on every
//! storage backend.

use std::error::Error;

use domain::{
    BrokerError, CONTENT_LIMITS, CodecError, Command, CommandKind, CommandOutcome,
    DEFAULT_RULE_NAME, EntityPath, MessageContent, NamespaceName, RuleFilter, RuleName,
    SequenceNumber, SqlRuleFilter, StateMachine, SubscriptionConfig, SubscriptionConfigUpdate,
    SubscriptionName, Timestamp, TopicConfig, codec, keys,
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

fn content(id: &str, region: Option<Value>) -> Content {
    Content {
        properties: Some(Properties {
            message_id: Field::Value(MessageId::String(text(id))),
            ..Properties::default()
        }),
        application_properties: Some(
            region
                .map(|value| (text("region"), value))
                .into_iter()
                .collect(),
        ),
        body: Body::Data(vec![Binary {
            bytes: id.as_bytes().to_vec(),
            width: Width::Four,
        }]),
        ..Content::default()
    }
}

fn message(id: &str, region: Option<Value>) -> MessageContent {
    MessageContent::Amqp(encode_content(&content(id, region), CONTENT_LIMITS).unwrap())
}

fn scheduled(id: &str, at: u64, region: Option<Value>) -> MessageContent {
    let mut content = content(id, region);
    content.message_annotations = Some(vec![(
        AnnotationKey::Symbol(Symbol {
            text: "x-opt-scheduled-enqueue-time".into(),
            width: Width::One,
        }),
        Value::Timestamp(at as i64),
    )]);
    MessageContent::Amqp(encode_content(&content, CONTENT_LIMITS).unwrap())
}

fn west() -> Option<Value> {
    Some(Value::String(text("west")))
}

/// Comparing a string with an int is a filter evaluation error.
fn mismatch() -> Option<Value> {
    Some(Value::Int(1))
}

const FAILURE: &str = "rule numeric: SQL operands have incompatible types";

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

    fn membership<S: StateStore>(&self, machine: &StateMachine<S>, name: &str) -> Stored {
        machine
            .store()
            .get(&keys::topic_subscription(
                &self.namespace,
                &self.topic,
                &SubscriptionName::new(name).unwrap(),
            ))
            .unwrap()
            .expect("membership")
    }

    /// Creates `name` with one SQL rule, `region = 'west'`, in place of `$Default`.
    fn filtered<S: StateStore>(
        &self,
        machine: &StateMachine<S>,
        name: &str,
        at: u64,
        config: SubscriptionConfig,
    ) -> TestResult {
        self.apply(
            machine,
            &self.topic,
            at,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new(name)?,
                config,
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
        self.apply(
            machine,
            &entity,
            at,
            CommandKind::CreateRule {
                name: RuleName::new("numeric")?,
                filter: RuleFilter::Sql(SqlRuleFilter::new("region = 'west'")),
            },
        )?;
        Ok(())
    }

    fn update<S: StateStore>(
        &self,
        machine: &StateMachine<S>,
        name: &str,
        at: u64,
        update: SubscriptionConfigUpdate,
    ) -> Result<CommandOutcome, BrokerError> {
        self.apply(
            machine,
            &self.topic,
            at,
            CommandKind::UpdateSubscription {
                name: SubscriptionName::new(name).unwrap(),
                update,
            },
        )
    }

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
            (None, None) => Landing::Absent,
            (Some(record), None) => {
                assert!(record.dead_letter.is_none());
                Landing::Delivered
            }
            (None, Some(record)) => {
                let info = record.dead_letter.expect("filter-error information");
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
    Absent,
}

fn dead() -> Landing {
    Landing::DeadLettered(FAILURE.into())
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

fn discarding() -> SubscriptionConfig {
    SubscriptionConfig {
        dead_lettering_on_filter_evaluation_exceptions: false,
        ..SubscriptionConfig::default()
    }
}

/// "drop" discards filter-error copies; "keep" keeps the default.
fn install<S: StateStore>(t: &Topology, machine: &StateMachine<S>) -> TestResult {
    t.apply(
        machine,
        &t.topic,
        1,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    t.filtered(machine, "drop", 2, discarding())?;
    t.filtered(machine, "keep", 2, SubscriptionConfig::default())?;
    Ok(())
}

/// One batch: an error, a skip, a match and another error.
fn route_batch<S: StateStore>(
    t: &Topology,
    machine: &StateMachine<S>,
    at: u64,
) -> TestResult<Vec<SequenceNumber>> {
    let outcome = t.apply(
        machine,
        &t.topic,
        at,
        CommandKind::SendContentBatch {
            messages: vec![
                message("first", mismatch()),
                message("east", Some(Value::String(text("east")))),
                message("west", west()),
                message("second", mismatch()),
            ],
        },
    )?;
    let CommandOutcome::Published {
        sequences,
        subscriptions,
    } = outcome
    else {
        panic!("expected a publication, got {outcome:?}");
    };
    // The discarding subscription reports no dead-letter queue.
    assert_eq!(
        subscriptions,
        [
            t.subscription("drop"),
            t.subscription("keep"),
            t.subscription("keep").dead_letter_queue()?
        ]
    );
    for (name, expected) in [
        (
            "drop",
            [
                Landing::Absent,
                Landing::Absent,
                Landing::Delivered,
                Landing::Absent,
            ],
        ),
        (
            "keep",
            [dead(), Landing::Absent, Landing::Delivered, dead()],
        ),
    ] {
        assert_eq!(
            sequences
                .iter()
                .map(|sequence| t.landed(machine, name, *sequence))
                .collect::<Vec<_>>(),
            expected
        );
    }
    Ok(sequences)
}

fn ordinary_fanout<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    install(&t, &machine)?;

    // A single erroring message reaches only the dead-lettering subscription.
    let outcome = t.apply(
        &machine,
        &t.topic,
        5,
        CommandKind::SendContent {
            content: message("single", mismatch()),
        },
    )?;
    assert_eq!(
        outcome,
        CommandOutcome::Published {
            sequences: vec![SequenceNumber::new(1)],
            subscriptions: vec![t.subscription("keep").dead_letter_queue()?],
        }
    );
    assert_eq!(
        t.landed(&machine, "drop", SequenceNumber::new(1)),
        Landing::Absent
    );
    assert_eq!(t.landed(&machine, "keep", SequenceNumber::new(1)), dead());
    route_batch(&t, &machine, 10)?;
    assert!(
        machine
            .ready_sequences(
                &t.namespace,
                &t.subscription("drop").dead_letter_queue()?,
                10
            )?
            .is_empty()
    );
    validated(&machine);
    let before = image(&machine);
    drop(machine);

    // Reopened, the setting is still read from the membership record.
    let machine = StateMachine::new(provider.open()?);
    assert_eq!(image(&machine), before);
    route_batch(&t, &machine, 20)?;
    validated(&machine);
    let admin = |name: &str| {
        machine
            .admin_subscription(
                &t.namespace,
                &t.topic,
                &SubscriptionName::new(name).unwrap(),
            )
            .unwrap()
            .unwrap()
            .config
    };
    assert_eq!(admin("drop"), discarding());
    assert_eq!(admin("keep"), SubscriptionConfig::default());
    Ok(())
}

paired!(
    ordinary_fanout_memory,
    ordinary_fanout_fjall,
    ordinary_fanout
);

fn scheduled_fanout<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    install(&t, &machine)?;
    let (placeholder, populated) = match t.apply(
        &machine,
        &t.topic,
        5,
        CommandKind::SendContent {
            content: scheduled("later", 100, mismatch()),
        },
    )? {
        CommandOutcome::Published {
            sequences,
            subscriptions,
        } => (sequences[0], subscriptions),
        other => panic!("expected a publication, got {other:?}"),
    };
    assert!(populated.is_empty());
    drop(machine);

    let machine = StateMachine::new(provider.open()?);
    assert_eq!(
        t.apply(&machine, &t.topic, 100, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: 1,
            deliverable_entities: vec![t.subscription("keep").dead_letter_queue()?],
        }
    );
    let active = SequenceNumber::new(placeholder.as_u64() + 1);
    assert_eq!(t.landed(&machine, "drop", active), Landing::Absent);
    assert_eq!(t.landed(&machine, "keep", active), dead());
    validated(&machine);
    Ok(())
}

paired!(
    scheduled_fanout_memory,
    scheduled_fanout_fjall,
    scheduled_fanout
);

/// Updates rewrite only the membership, independently of expiry policy, and
/// an update naming the current value writes nothing.
fn updates<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    install(&t, &machine)?;
    let drop_entity = t.subscription("drop");
    let legacy = codec::encode(&drop_entity)?;
    let discard = codec::membership::encode(&drop_entity, false)?;
    assert_eq!(t.membership(&machine, "drop"), discard);
    let backing = machine
        .store()
        .get(&keys::queue_config(&t.namespace, &drop_entity))?;

    let before = image(&machine);
    assert_eq!(
        t.update(
            &machine,
            "drop",
            3,
            SubscriptionConfigUpdate {
                dead_lettering_on_filter_evaluation_exceptions: Some(false),
                ..SubscriptionConfigUpdate::default()
            },
        )?,
        CommandOutcome::SubscriptionUpdated
    );
    assert_eq!(image(&machine), before);

    t.update(
        &machine,
        "drop",
        4,
        SubscriptionConfigUpdate {
            dead_lettering_on_filter_evaluation_exceptions: Some(true),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(t.membership(&machine, "drop"), legacy);
    assert_eq!(
        machine
            .store()
            .get(&keys::queue_config(&t.namespace, &drop_entity))?,
        backing
    );
    let sequence = match t.apply(
        &machine,
        &t.topic,
        5,
        CommandKind::SendContent {
            content: message("now", mismatch()),
        },
    )? {
        CommandOutcome::Published { sequences, .. } => sequences[0],
        other => panic!("expected a publication, got {other:?}"),
    };
    assert_eq!(t.landed(&machine, "drop", sequence), dead());

    // Expiry and filter-error settings change independently.
    t.update(
        &machine,
        "drop",
        6,
        SubscriptionConfigUpdate {
            dead_lettering_on_message_expiration: Some(false),
            dead_lettering_on_filter_evaluation_exceptions: Some(false),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(t.membership(&machine, "drop"), discard);
    t.update(
        &machine,
        "drop",
        7,
        SubscriptionConfigUpdate {
            dead_lettering_on_message_expiration: Some(true),
            ..SubscriptionConfigUpdate::default()
        },
    )?;
    assert_eq!(t.membership(&machine, "drop"), discard);
    assert_eq!(
        machine
            .store()
            .get(&keys::queue_config(&t.namespace, &drop_entity))?,
        backing
    );
    validated(&machine);
    let before = image(&machine);
    drop(machine);
    let machine = StateMachine::new(provider.open()?);
    assert_eq!(image(&machine), before);
    assert_eq!(
        machine
            .admin_subscription(&t.namespace, &t.topic, &SubscriptionName::new("drop")?)?
            .unwrap()
            .config,
        discarding()
    );
    Ok(())
}

paired!(updates_memory, updates_fjall, updates);

/// A subscription that both discards filter-error copies and deletes expired
/// messages stores each setting in its own record.
fn independent_expiry<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    install(&t, &machine)?;
    t.filtered(
        &machine,
        "both",
        3,
        SubscriptionConfig {
            dead_lettering_on_message_expiration: false,
            ..discarding()
        },
    )?;
    let both = t.subscription("both");
    assert_eq!(
        t.membership(&machine, "both")[0],
        codec::membership::VALUE_FORMAT_V2
    );
    let backing = machine
        .store()
        .get(&keys::queue_config(&t.namespace, &both))?
        .unwrap();
    assert_eq!(backing[0], codec::queue::VALUE_FORMAT_V2);
    let config = codec::queue::decode(&backing)?;
    assert!(!config.dead_lettering_on_message_expiration);
    assert!(!config.capacity.is_finite());
    // "drop" kept the expiry default, so its backing keeps the legacy bytes.
    let drop_backing = machine
        .store()
        .get(&keys::queue_config(&t.namespace, &t.subscription("drop")))?
        .unwrap();
    assert_eq!(drop_backing[0], codec::VALUE_FORMAT_V1);
    validated(&machine);
    Ok(())
}

paired!(
    independent_expiry_memory,
    independent_expiry_fjall,
    independent_expiry
);

#[test]
fn membership_record_fence_keeps_legacy_bytes_and_versions_discard() -> TestResult {
    let t = Topology::new()?;
    let entity = t.subscription("drop");
    // The default membership is exactly the legacy V1 envelope of the child.
    let path = entity.as_str().as_bytes();
    assert_eq!(path, b"events/subscriptions/drop");
    let mut legacy_golden = vec![1, path.len() as u8];
    legacy_golden.extend(path);
    assert_eq!(codec::membership::encode(&entity, true)?, legacy_golden);
    assert_eq!(codec::encode(&entity)?, legacy_golden);
    let mut golden = vec![2, path.len() as u8];
    golden.extend(path);
    golden.push(0);
    assert_eq!(codec::membership::encode(&entity, false)?, golden);
    assert_eq!(codec::membership::decode(&entity, &golden), Ok(false));
    assert_eq!(codec::membership::decode(&entity, &legacy_golden), Ok(true));

    // A reader built before the setting compares memberships with the V1
    // envelope of the child path, so the new record reads as corrupt topology;
    // it can't decode as that envelope either.
    assert_ne!(codec::encode(&entity)?, golden);
    assert_eq!(
        codec::decode::<EntityPath>(&golden),
        Err(CodecError::UnsupportedVersion { version: 2 })
    );
    Ok(())
}

/// Noncanonical, foreign or unknown membership records refuse without writes.
fn corrupt_membership<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    install(&t, &machine)?;
    let drop_entity = t.subscription("drop");
    let key = keys::topic_subscription(&t.namespace, &t.topic, &SubscriptionName::new("drop")?);
    let mut redundant = codec::membership::encode(&drop_entity, false)?;
    let last = redundant.len() - 1;
    redundant[last] = 1;
    let mut trailing = codec::membership::encode(&drop_entity, false)?;
    trailing.push(0);
    let mut unknown = codec::membership::encode(&drop_entity, false)?;
    unknown[0] = 3;
    for corrupt in [
        redundant,
        trailing,
        unknown,
        codec::membership::encode(&t.subscription("keep"), false)?,
    ] {
        machine
            .store()
            .apply(WriteBatch::default().put(key.clone(), corrupt))?;
        let before = image(&machine);
        assert_eq!(
            t.apply(
                &machine,
                &t.topic,
                5,
                CommandKind::SendContent {
                    content: message("west", west()),
                },
            ),
            Err(BrokerError::TopicTopologyCorrupt)
        );
        assert_eq!(image(&machine), before);
        assert!(validate_catalog(&before).is_err());
    }
    Ok(())
}

paired!(
    corrupt_membership_memory,
    corrupt_membership_fjall,
    corrupt_membership
);
