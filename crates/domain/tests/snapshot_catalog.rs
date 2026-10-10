//! Catalog-only validation of immutable complete images on both real backends.
//! Corruption and saturated/capacity shapes are injected image fixtures, not
//! transition reachability, state-health, capture, or crash certifications.

use std::collections::BTreeMap;

use domain::{
    Command, CommandKind, CommandOutcome, CorrelationFilter, CorrelationValue, DEFAULT_RULE_NAME,
    DurableProposal, EntityPath, IndexedApplyOutcome, IndexedWriter, MAX_ENTITY_PATH_BYTES,
    MAX_NAMESPACE_NAME_BYTES, MAX_SUBSCRIPTION_RULES, MAX_TOPIC_SUBSCRIPTIONS, NamespaceName,
    QueueConfig, QueueConfigUpdate, QueueCounters, QueueTimeToLiveUpdate, ReceiveMode,
    RuleDefinition, RuleFilter, RuleName, StateMachine, SubscriptionConfig, SubscriptionName,
    Timestamp, TopicConfig, codec, keys,
    snapshot_validation::{CatalogValidation, SnapshotCatalogError, validate_catalog},
};
use serde::Serialize;
use storage::{Key, StateStore, Value};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

type Image = Vec<(Key, Value)>;

struct Fixture<P: StoreProvider> {
    machine: StateMachine<P::Store>,
    provider: P,
    namespace: NamespaceName,
    queue: EntityPath,
    topic: EntityPath,
    subscription: EntityPath,
}

impl<P: StoreProvider> Fixture<P> {
    fn new(provider: P) -> Self {
        let namespace = NamespaceName::new("tenant").unwrap();
        let queue = EntityPath::new("orders").unwrap();
        let topic = EntityPath::new("events").unwrap();
        let name = SubscriptionName::new("first").unwrap();
        let subscription = topic.subscription(&name).unwrap();
        let machine = StateMachine::new(provider.open().unwrap());
        let f = Self {
            provider,
            machine,
            namespace,
            queue,
            topic,
            subscription,
        };
        f.at(
            &f.queue,
            1,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        );
        f.at(
            &f.topic,
            2,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        );
        f.at(
            &f.topic,
            3,
            CommandKind::CreateSubscription {
                name,
                config: SubscriptionConfig::default(),
            },
        );
        f.at(
            &f.subscription,
            4,
            CommandKind::CreateRule {
                name: RuleName::new("MiXeD").unwrap(),
                filter: RuleFilter::Correlation(CorrelationFilter {
                    application_properties: BTreeMap::from([(
                        "Region".to_owned(),
                        CorrelationValue::new(vec![1, 2]).unwrap(),
                    )]),
                    ..CorrelationFilter::default()
                }),
            },
        );
        f.at(
            &f.queue,
            5,
            CommandKind::Send {
                message_id: "one".to_owned(),
                body: vec![7],
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            },
        );
        f
    }

    fn at(&self, entity: &EntityPath, time: u64, kind: CommandKind) -> CommandOutcome {
        self.machine
            .apply(&Command::new(
                self.namespace.clone(),
                entity.clone(),
                Timestamp::from_millis(time),
                kind,
            ))
            .unwrap()
    }

    fn image(&self) -> Image {
        self.machine.store().snapshot().unwrap().entries().to_vec()
    }

    fn accept(&self, image: &Image) -> CatalogValidation {
        let source = image.clone();
        let stored = self.image();
        let report = validate_catalog(image).expect("complete catalog");
        assert_eq!(image, &source, "validation preserves every input byte");
        assert_eq!(
            self.image(),
            stored,
            "validation does not mutate the backend"
        );
        assert_eq!(
            report.catalog_rows()
                + report.unvalidated_state_rows()
                + report.unvalidated_external_rows(),
            image.len()
        );
        report
    }

    fn reject(&self, image: &Image) -> SnapshotCatalogError {
        let source = image.clone();
        let stored = self.image();
        let error = validate_catalog(image).expect_err("refuse malformed catalog");
        assert_eq!(image, &source, "refusal preserves every input byte");
        assert_eq!(self.image(), stored, "refusal does not mutate the backend");
        error
    }

    fn restart(self) -> Self {
        let Self {
            provider,
            machine,
            namespace,
            queue,
            topic,
            subscription,
        } = self;
        drop(machine);
        Self {
            machine: StateMachine::new(provider.open().unwrap()),
            provider,
            namespace,
            queue,
            topic,
            subscription,
        }
    }
}

fn value(image: &Image, key: &[u8]) -> Value {
    image
        .iter()
        .find(|(candidate, _)| candidate == key)
        .expect("fixture row")
        .1
        .clone()
}

fn put(image: &mut Image, key: Key, value: Value) {
    image.retain(|(candidate, _)| candidate != &key);
    image.push((key, value));
    image.sort_by(|a, b| a.0.cmp(&b.0));
}

fn remove(image: &mut Image, key: &[u8]) {
    let before = image.len();
    image.retain(|(candidate, _)| candidate != key);
    assert_eq!(image.len() + 1, before);
}

fn row(image: &Image, key: &[u8]) -> usize {
    image
        .iter()
        .position(|(candidate, _)| candidate == key)
        .unwrap()
}

fn error_row(error: SnapshotCatalogError) -> usize {
    match error {
        SnapshotCatalogError::EmptyKey { row }
        | SnapshotCatalogError::InputOrder { row }
        | SnapshotCatalogError::UnsupportedTag { row, .. }
        | SnapshotCatalogError::InvalidKey { row, .. }
        | SnapshotCatalogError::InvalidValue { row, .. }
        | SnapshotCatalogError::InconsistentCatalog { row, .. } => row,
        SnapshotCatalogError::MissingClock => panic!("expected an original row ordinal"),
    }
}

fn healthy<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let image = f.image();
    let report = f.accept(&image);
    assert_eq!(report.clock(), Some(Timestamp::from_millis(5)));
    assert!(report.unvalidated_state_rows() > 0);
    assert_eq!(report.unvalidated_external_rows(), 0);
    let rule: RuleDefinition = codec::decode(&value(
        &image,
        &keys::subscription_rule(
            &f.namespace,
            &f.subscription,
            &RuleName::new("mixed").unwrap(),
        ),
    ))
    .unwrap();
    assert_eq!(rule.name.display_name(), "MiXeD");
    let session_queue = EntityPath::new("session-orders").unwrap();
    f.at(
        &session_queue,
        6,
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_session: true,
                requires_duplicate_detection: true,
                default_time_to_live_millis: Some(123),
                ..QueueConfig::default()
            },
        },
    );
    f.at(
        &session_queue,
        7,
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                lock_duration_millis: Some(40),
                max_delivery_count: Some(12),
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 456 }),
                ..QueueConfigUpdate::default()
            },
        },
    );
    f.at(
        &f.subscription,
        8,
        CommandKind::DeleteRule {
            name: RuleName::new(DEFAULT_RULE_NAME).unwrap(),
        },
    );
    f.at(
        &f.subscription,
        8,
        CommandKind::DeleteRule {
            name: RuleName::new("mixed").unwrap(),
        },
    );
    let zero_topic = EntityPath::new("no-subs").unwrap();
    f.at(
        &zero_topic,
        8,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    );
    let before = f.image();
    f.accept(&before);
    assert_eq!(
        f.at(
            &f.subscription,
            9,
            CommandKind::ListRules {
                skip: 0,
                max_rules: 1
            }
        ),
        CommandOutcome::RulesListed { rules: vec![] }
    );
    assert_eq!(f.image(), before, "ordinary no-op does not advance Clock");
    assert_eq!(
        f.machine.apply(&Command::new(
            f.namespace.clone(),
            session_queue,
            Timestamp::from_millis(10),
            CommandKind::CreateQueue {
                config: QueueConfig::default()
            }
        )),
        Err(domain::BrokerError::QueueAlreadyExists)
    );
    assert_eq!(
        f.image(),
        before,
        "ordinary refusal preserves the complete image"
    );
    assert_eq!(f.accept(&before).clock(), Some(Timestamp::from_millis(8)));
    let reopened = f.restart();
    assert_eq!(reopened.image(), before);
    reopened.accept(&before);
}

#[test]
fn memory_supported_catalogs_updates_empty_rules_and_reopen() {
    healthy(MemoryProvider::new());
}
#[test]
fn fjall_supported_catalogs_updates_empty_rules_and_reopen() {
    healthy(DurableProvider::temporary().unwrap());
}

fn companions<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let image = f.image();
    let mut missing_clock = image.clone();
    remove(&mut missing_clock, &keys::clock());
    assert!(matches!(
        f.reject(&missing_clock),
        SnapshotCatalogError::MissingClock
    ));
    let name = SubscriptionName::new("first").unwrap();
    for key in [
        keys::entity_metadata(&f.namespace, &f.queue),
        keys::queue_config(&f.namespace, &f.queue),
        keys::queue_config(&f.namespace, &f.queue.dead_letter_queue().unwrap()),
        keys::entity_metadata(&f.namespace, &f.topic),
        keys::topic_config(&f.namespace, &f.topic),
        keys::topic_subscription(&f.namespace, &f.topic, &name),
        keys::entity_metadata(&f.namespace, &f.subscription),
        keys::queue_config(&f.namespace, &f.subscription),
        keys::queue_config(&f.namespace, &f.subscription.dead_letter_queue().unwrap()),
    ] {
        let mut broken = image.clone();
        remove(&mut broken, &key);
        assert!(matches!(
            f.reject(&broken),
            SnapshotCatalogError::InconsistentCatalog { .. }
        ));
    }
    let mut shadow = image.clone();
    let key = keys::queue_config(&f.namespace, &f.queue.dead_letter_queue().unwrap());
    let mut config: QueueConfig = codec::decode(&value(&shadow, &key)).unwrap();
    config.max_delivery_count = 10;
    put(&mut shadow, key, codec::encode(&config).unwrap());
    assert!(matches!(
        f.reject(&shadow),
        SnapshotCatalogError::InconsistentCatalog { .. }
    ));
    let mut backing = image.clone();
    let key = keys::queue_config(&f.namespace, &f.subscription);
    let mut config: QueueConfig = codec::decode(&value(&backing, &key)).unwrap();
    config.max_message_bytes += 1;
    put(&mut backing, key, codec::encode(&config).unwrap());
    assert!(matches!(
        f.reject(&backing),
        SnapshotCatalogError::InconsistentCatalog { .. }
    ));
    let mut collision = image.clone();
    put(
        &mut collision,
        keys::topic_config(&f.namespace, &f.queue),
        codec::encode(&TopicConfig::default()).unwrap(),
    );
    assert!(matches!(
        f.reject(&collision),
        SnapshotCatalogError::InconsistentCatalog { .. }
    ));
    let mut shadow_head = image.clone();
    put(
        &mut shadow_head,
        keys::entity_metadata(&f.namespace, &f.queue.dead_letter_queue().unwrap()),
        value(&image, &keys::entity_metadata(&f.namespace, &f.queue)),
    );
    assert!(matches!(
        f.reject(&shadow_head),
        SnapshotCatalogError::InvalidKey { .. } | SnapshotCatalogError::InconsistentCatalog { .. }
    ));
    let mut topic_shadow = image.clone();
    put(
        &mut topic_shadow,
        keys::queue_config(&f.namespace, &f.topic.dead_letter_queue().unwrap()),
        value(
            &image,
            &keys::queue_config(&f.namespace, &f.queue.dead_letter_queue().unwrap()),
        ),
    );
    assert!(matches!(
        f.reject(&topic_shadow),
        SnapshotCatalogError::InconsistentCatalog { .. }
    ));
    let mut orphan_head = image.clone();
    put(
        &mut orphan_head,
        keys::entity_metadata(&f.namespace, &EntityPath::new("orphan").unwrap()),
        value(&image, &keys::entity_metadata(&f.namespace, &f.queue)),
    );
    assert!(matches!(
        f.reject(&orphan_head),
        SnapshotCatalogError::InconsistentCatalog { .. }
    ));
}

#[test]
fn memory_catalog_companions_shadows_backing_and_collisions() {
    companions(MemoryProvider::new());
}
#[test]
fn fjall_catalog_companions_shadows_backing_and_collisions() {
    companions(DurableProvider::temporary().unwrap());
}

fn canonical_rows<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let image = f.image();
    for (key, _) in image
        .iter()
        .filter(|(key, _)| matches!(key[0], 0 | 1 | 2 | 14 | 15 | 16 | 17))
    {
        for raw in [vec![], vec![255], vec![1, 255]] {
            let mut broken = image.clone();
            put(&mut broken, key.clone(), raw);
            let error = f.reject(&broken);
            assert_eq!(error_row(error), row(&broken, key));
            assert!(matches!(error, SnapshotCatalogError::InvalidValue { .. }));
        }
        let mut trailing = image.clone();
        let mut raw = value(&image, key);
        raw.push(0);
        put(&mut trailing, key.clone(), raw);
        assert!(matches!(
            f.reject(&trailing),
            SnapshotCatalogError::InvalidValue { .. }
        ));
    }
    let queue_key = keys::queue_config(&f.namespace, &f.queue);
    let mut bad_clock_key = image.clone();
    remove(&mut bad_clock_key, &keys::clock());
    put(
        &mut bad_clock_key,
        vec![0, 0],
        codec::encode(&Timestamp::from_millis(5)).unwrap(),
    );
    assert!(matches!(
        f.reject(&bad_clock_key),
        SnapshotCatalogError::InvalidKey { row: 0, .. }
    ));
    for mutated in [
        {
            let mut key = queue_key.clone();
            key[1] = b'T';
            key
        },
        {
            let mut key = queue_key.clone();
            key.push(0);
            key
        },
        {
            let mut key = queue_key.clone();
            key[1] = 255;
            key
        },
        {
            let mut key = queue_key.clone();
            key[1] = b'\n';
            key
        },
    ] {
        let mut broken = image.clone();
        remove(&mut broken, &queue_key);
        put(&mut broken, mutated.clone(), value(&image, &queue_key));
        let error = f.reject(&broken);
        assert_eq!(error_row(error), row(&broken, &mutated));
        assert!(matches!(error, SnapshotCatalogError::InvalidKey { .. }));
    }
    let mut membership = image.clone();
    let key = keys::topic_subscription(
        &f.namespace,
        &f.topic,
        &SubscriptionName::new("first").unwrap(),
    );
    put(
        &mut membership,
        key,
        codec::encode(&f.subscription.as_str().to_ascii_uppercase()).unwrap(),
    );
    assert!(matches!(
        f.reject(&membership),
        SnapshotCatalogError::InvalidValue { .. }
    ));
    let mut invalid = image.clone();
    put(
        &mut invalid,
        queue_key,
        codec::encode(&QueueConfig {
            lock_duration_millis: 0,
            ..QueueConfig::default()
        })
        .unwrap(),
    );
    assert!(matches!(
        f.reject(&invalid),
        SnapshotCatalogError::InvalidValue { .. }
    ));
    let mut invalid_topic = image.clone();
    put(
        &mut invalid_topic,
        keys::topic_config(&f.namespace, &f.topic),
        codec::encode(&TopicConfig {
            max_message_bytes: 0,
            ..TopicConfig::default()
        })
        .unwrap(),
    );
    assert!(matches!(
        f.reject(&invalid_topic),
        SnapshotCatalogError::InvalidValue { .. }
    ));
}

#[test]
fn memory_catalog_canonical_keys_values_and_profile_policy() {
    canonical_rows(MemoryProvider::new());
}
#[test]
fn fjall_catalog_canonical_keys_values_and_profile_policy() {
    canonical_rows(DurableProvider::temporary().unwrap());
}

fn unselected<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let namespace = NamespaceName::new("zz-unselected").unwrap();
    let entity = EntityPath::new("late").unwrap();
    f.machine
        .apply(&Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(6),
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        ))
        .unwrap();
    let image = f.image();
    f.accept(&image);
    assert_eq!(
        f.machine.queue_config(&f.namespace, &f.queue).unwrap(),
        Some(QueueConfig::default())
    );
    let mut broken = image.clone();
    put(
        &mut broken,
        keys::queue_config(&namespace, &entity),
        vec![255],
    );
    assert!(matches!(
        f.reject(&broken),
        SnapshotCatalogError::InvalidValue { .. }
    ));
    let mut orphan = image.clone();
    let unknown = EntityPath::new("unknown").unwrap();
    put(
        &mut orphan,
        keys::queue_counters(&namespace, &unknown),
        codec::encode(&QueueCounters::default()).unwrap(),
    );
    assert_eq!(
        error_row(f.reject(&orphan)),
        row(&orphan, &keys::queue_counters(&namespace, &unknown))
    );
    assert!(matches!(
        f.reject(&orphan),
        SnapshotCatalogError::InconsistentCatalog { .. }
    ));
    let mut foreign_rule = image.clone();
    let rule: RuleDefinition = codec::decode(&value(
        &image,
        &keys::subscription_rule(
            &f.namespace,
            &f.subscription,
            &RuleName::new("mixed").unwrap(),
        ),
    ))
    .unwrap();
    put(
        &mut foreign_rule,
        keys::subscription_rule(&namespace, &unknown, &rule.name),
        codec::encode(&rule).unwrap(),
    );
    assert!(matches!(
        f.reject(&foreign_rule),
        SnapshotCatalogError::InvalidKey { .. } | SnapshotCatalogError::InconsistentCatalog { .. }
    ));
}

#[test]
fn memory_complete_image_checks_unselected_namespace_and_orphans() {
    unselected(MemoryProvider::new());
}
#[test]
fn fjall_complete_image_checks_unselected_namespace_and_orphans() {
    unselected(DurableProvider::temporary().unwrap());
}

fn rule_policy<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let image = f.image();
    let key = keys::subscription_rule(
        &f.namespace,
        &f.subscription,
        &RuleName::new("mixed").unwrap(),
    );
    let rule: RuleDefinition = codec::decode(&value(&image, &key)).unwrap();
    for filter in [
        RuleFilter::Correlation(CorrelationFilter::default()),
        RuleFilter::Correlation(CorrelationFilter {
            session_id: Some("not-supported".to_owned()),
            ..CorrelationFilter::default()
        }),
        RuleFilter::Correlation(CorrelationFilter {
            application_properties: BTreeMap::from([(
                "Region".to_owned(),
                CorrelationValue::new(vec![1]).unwrap(),
            )]),
            ..CorrelationFilter::default()
        }),
        RuleFilter::Correlation(CorrelationFilter {
            application_properties: BTreeMap::from([
                ("Region".to_owned(), CorrelationValue::new(vec![1]).unwrap()),
                ("region".to_owned(), CorrelationValue::new(vec![2]).unwrap()),
            ]),
            ..CorrelationFilter::default()
        }),
        RuleFilter::Correlation(CorrelationFilter {
            correlation_id: Some("x".repeat(domain::MAX_CORRELATION_FILTER_BYTES + 1)),
            ..CorrelationFilter::default()
        }),
    ] {
        let mut broken = image.clone();
        put(
            &mut broken,
            key.clone(),
            codec::encode(&RuleDefinition {
                filter,
                ..rule.clone()
            })
            .unwrap(),
        );
        assert!(matches!(
            f.reject(&broken),
            SnapshotCatalogError::InvalidValue { .. }
        ));
    }
    let mut wrong_name = image.clone();
    put(
        &mut wrong_name,
        key.clone(),
        codec::encode(&RuleDefinition {
            name: RuleName::new("elsewhere").unwrap(),
            ..rule.clone()
        })
        .unwrap(),
    );
    assert!(matches!(
        f.reject(&wrong_name),
        SnapshotCatalogError::InvalidValue { .. }
            | SnapshotCatalogError::InconsistentCatalog { .. }
    ));
    let mut future = image.clone();
    put(
        &mut future,
        key,
        codec::encode(&RuleDefinition {
            created_at: Timestamp::from_millis(6),
            ..rule
        })
        .unwrap(),
    );
    assert!(matches!(
        f.reject(&future),
        SnapshotCatalogError::InconsistentCatalog { .. }
    ));
}

#[test]
fn memory_rule_canonical_policy_and_created_at_clock_companion() {
    rule_policy(MemoryProvider::new());
}
#[test]
fn fjall_rule_canonical_policy_and_created_at_clock_companion() {
    rule_policy(DurableProvider::temporary().unwrap());
}

// Source-shaped private head DTO, used only to inject boundary/corrupt images.
#[derive(Clone, Copy, Serialize)]
enum HeadKind {
    Queue,
    Topic,
    Subscription,
}
#[derive(Clone, Copy, Serialize)]
struct Head {
    generation: u64,
    kind: HeadKind,
    retired: bool,
}

fn bounds<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let image = f.image();
    for counters in [
        QueueCounters {
            next_sequence: 0,
            next_lock_token: 1,
        },
        QueueCounters {
            next_sequence: 1,
            next_lock_token: 0,
        },
    ] {
        let mut broken = image.clone();
        put(
            &mut broken,
            keys::queue_counters(&f.namespace, &f.queue),
            codec::encode(&counters).unwrap(),
        );
        assert!(matches!(
            f.reject(&broken),
            SnapshotCatalogError::InvalidValue { .. }
        ));
    }
    let head_key = keys::entity_metadata(&f.namespace, &f.queue);
    for head in [
        Head {
            generation: 0,
            kind: HeadKind::Queue,
            retired: false,
        },
        Head {
            generation: 1,
            kind: HeadKind::Queue,
            retired: true,
        },
        Head {
            generation: 1,
            kind: HeadKind::Topic,
            retired: false,
        },
    ] {
        let mut broken = image.clone();
        put(&mut broken, head_key.clone(), codec::encode(&head).unwrap());
        assert!(matches!(
            f.reject(&broken),
            SnapshotCatalogError::InvalidValue { .. }
        ));
    }
    for raw in [vec![1; 33], codec::encode(&(1_u64, 3_u8, false)).unwrap()] {
        let mut broken = image.clone();
        put(&mut broken, head_key.clone(), raw);
        assert!(matches!(
            f.reject(&broken),
            SnapshotCatalogError::InvalidValue { .. }
        ));
    }
    let mut saturated = image.clone();
    put(
        &mut saturated,
        keys::clock(),
        codec::encode(&Timestamp::from_millis(u64::MAX)).unwrap(),
    );
    for (entity, kind) in [
        (&f.queue, HeadKind::Queue),
        (&f.topic, HeadKind::Topic),
        (&f.subscription, HeadKind::Subscription),
    ] {
        put(
            &mut saturated,
            keys::entity_metadata(&f.namespace, entity),
            codec::encode(&Head {
                generation: u64::MAX,
                kind,
                retired: false,
            })
            .unwrap(),
        );
        put(
            &mut saturated,
            keys::queue_counters(&f.namespace, entity),
            codec::encode(&QueueCounters {
                next_sequence: u64::MAX,
                next_lock_token: u64::MAX,
            })
            .unwrap(),
        );
    }
    assert_eq!(
        f.accept(&saturated).clock(),
        Some(Timestamp::from_millis(u64::MAX))
    );
    let namespace = NamespaceName::new("n".repeat(MAX_NAMESPACE_NAME_BYTES)).unwrap();
    let topic = EntityPath::new("t".repeat(MAX_ENTITY_PATH_BYTES)).unwrap();
    let name = SubscriptionName::new("s".repeat(50)).unwrap();
    let subscription = topic.subscription(&name).unwrap();
    f.machine
        .apply(&Command::new(
            namespace.clone(),
            topic.clone(),
            Timestamp::from_millis(6),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ))
        .unwrap();
    f.machine
        .apply(&Command::new(
            namespace.clone(),
            topic,
            Timestamp::from_millis(7),
            CommandKind::CreateSubscription {
                name,
                config: SubscriptionConfig::default(),
            },
        ))
        .unwrap();
    f.machine
        .apply(&Command::new(
            namespace,
            subscription.clone(),
            Timestamp::from_millis(8),
            CommandKind::CreateRule {
                name: RuleName::new("R".repeat(50)).unwrap(),
                filter: RuleFilter::False,
            },
        ))
        .unwrap();
    assert!(subscription.dead_letter_queue().unwrap().as_str().len() > MAX_ENTITY_PATH_BYTES);
    f.accept(&f.image());
}

#[test]
fn memory_catalog_maximum_composite_paths_and_injected_saturated_counters() {
    bounds(MemoryProvider::new());
}
#[test]
fn fjall_catalog_maximum_composite_paths_and_injected_saturated_counters() {
    bounds(DurableProvider::temporary().unwrap());
}

fn opaque_and_clock<P: StoreProvider>(provider: P) {
    let store = provider.open().unwrap();
    let empty = store.snapshot().unwrap().entries().to_vec();
    let report = validate_catalog(&empty).unwrap();
    assert_eq!(report.clock(), None);
    assert_eq!(report.catalog_rows(), 0);
    let mut writer = IndexedWriter::open(store.clone()).unwrap();
    let command = Command::new(
        NamespaceName::new("tenant").unwrap(),
        EntityPath::new("absent").unwrap(),
        Timestamp::from_millis(1),
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    );
    assert!(matches!(
        writer.apply(1, &DurableProposal::unbound(command)).unwrap(),
        IndexedApplyOutcome::Refused(_)
    ));
    let checkpoint = store.snapshot().unwrap().entries().to_vec();
    assert_eq!(checkpoint.len(), 2);
    let report = validate_catalog(&checkpoint).unwrap();
    assert_eq!(report.clock(), None);
    assert_eq!(report.unvalidated_external_rows(), 2);
    assert_eq!(store.snapshot().unwrap().entries(), checkpoint);
    // Malformed external suffixes/values and state keys are deliberately pending.
    let mut opaque = vec![(vec![0xF0], vec![255]), (vec![0xF1], vec![])];
    let original = opaque.clone();
    assert_eq!(
        validate_catalog(&opaque)
            .unwrap()
            .unvalidated_external_rows(),
        2
    );
    assert_eq!(opaque, original);
    for tag in 3..=13 {
        put(&mut opaque, vec![tag], vec![255]);
    }
    assert!(matches!(
        validate_catalog(&opaque),
        Err(SnapshotCatalogError::MissingClock)
    ));
    let before = opaque.clone();
    put(
        &mut opaque,
        keys::clock(),
        codec::encode(&Timestamp::from_millis(0)).unwrap(),
    );
    let report = validate_catalog(&opaque).unwrap();
    assert_eq!(report.catalog_rows(), 1);
    assert_eq!(report.unvalidated_state_rows(), 11);
    assert_eq!(report.unvalidated_external_rows(), 2);
    assert_eq!(&opaque[1..], before);
    for time in [0, u64::MAX] {
        let clock_only = vec![(
            keys::clock(),
            codec::encode(&Timestamp::from_millis(time)).unwrap(),
        )];
        assert_eq!(
            validate_catalog(&clock_only).unwrap().clock(),
            Some(Timestamp::from_millis(time))
        );
    }
    assert!(matches!(
        validate_catalog(&[(vec![], vec![])]),
        Err(SnapshotCatalogError::EmptyKey { row: 0 })
    ));
    let zero = codec::encode(&Timestamp::from_millis(0)).unwrap();
    assert!(matches!(
        validate_catalog(&[(keys::clock(), zero.clone()), (keys::clock(), zero.clone())]),
        Err(SnapshotCatalogError::InputOrder { row: 1 })
    ));
    assert!(matches!(
        validate_catalog(&[(vec![0xF0], vec![]), (keys::clock(), zero.clone())]),
        Err(SnapshotCatalogError::InputOrder { row: 1 })
    ));
    assert!(matches!(
        validate_catalog(&[(keys::clock(), zero), (vec![0x12], vec![])]),
        Err(SnapshotCatalogError::UnsupportedTag { row: 1, tag: 0x12 })
    ));
    drop(writer);
    drop(store);
}

#[test]
fn memory_global_clock_companion_and_explicit_state_external_pending_rows() {
    opaque_and_clock(MemoryProvider::new());
}
#[test]
fn fjall_global_clock_companion_and_explicit_state_external_pending_rows() {
    opaque_and_clock(DurableProvider::temporary().unwrap());
}

fn capacities<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let second_name = SubscriptionName::new("second").unwrap();
    let second = f.topic.subscription(&second_name).unwrap();
    f.at(
        &f.topic,
        6,
        CommandKind::CreateSubscription {
            name: second_name,
            config: SubscriptionConfig::default(),
        },
    );
    let image = f.image();
    let prefix = keys::subscription_rule_prefix(&f.namespace, &f.subscription);
    let mut full_rules = image.clone();
    full_rules.retain(|(key, _)| !key.starts_with(&prefix));
    for i in 0..MAX_SUBSCRIPTION_RULES {
        let rule = RuleDefinition {
            name: RuleName::new(format!("R{i:04}")).unwrap(),
            filter: RuleFilter::False,
            created_at: Timestamp::from_millis(6),
        };
        full_rules.push((
            keys::subscription_rule(&f.namespace, &f.subscription, &rule.name),
            codec::encode(&rule).unwrap(),
        ));
    }
    full_rules.sort_by(|a, b| a.0.cmp(&b.0));
    assert!(full_rules.iter().any(|(key, _)| key.starts_with(&keys::subscription_rule_prefix(&f.namespace, &second))));
    f.accept(&full_rules); // More than 2,000 globally is legal; the bound is per owner.
    let extra = RuleDefinition {
        name: RuleName::new("overflow").unwrap(),
        filter: RuleFilter::True,
        created_at: Timestamp::from_millis(6),
    };
    put(
        &mut full_rules,
        keys::subscription_rule(&f.namespace, &f.subscription, &extra.name),
        codec::encode(&extra).unwrap(),
    );
    assert!(matches!(
        f.reject(&full_rules),
        SnapshotCatalogError::InconsistentCatalog { .. }
    ));

    // Build complete source-shaped catalog images without 2,000 durable commands.
    let mut full_members = vec![
        (keys::clock(), value(&image, &keys::clock())),
        (
            keys::topic_config(&f.namespace, &f.topic),
            value(&image, &keys::topic_config(&f.namespace, &f.topic)),
        ),
        (
            keys::entity_metadata(&f.namespace, &f.topic),
            value(&image, &keys::entity_metadata(&f.namespace, &f.topic)),
        ),
    ];
    let backing = value(&image, &keys::queue_config(&f.namespace, &f.subscription));
    let shadow = value(
        &image,
        &keys::queue_config(&f.namespace, &f.subscription.dead_letter_queue().unwrap()),
    );
    let head = value(
        &image,
        &keys::entity_metadata(&f.namespace, &f.subscription),
    );
    for i in 0..MAX_TOPIC_SUBSCRIPTIONS {
        let name = SubscriptionName::new(format!("s{i:04}")).unwrap();
        let entity = f.topic.subscription(&name).unwrap();
        full_members.push((
            keys::topic_subscription(&f.namespace, &f.topic, &name),
            codec::encode(&entity).unwrap(),
        ));
        full_members.push((keys::queue_config(&f.namespace, &entity), backing.clone()));
        full_members.push((
            keys::queue_config(&f.namespace, &entity.dead_letter_queue().unwrap()),
            shadow.clone(),
        ));
        full_members.push((keys::entity_metadata(&f.namespace, &entity), head.clone()));
    }
    full_members.sort_by(|a, b| a.0.cmp(&b.0));
    let other_topic = EntityPath::new("other-topic").unwrap();
    let other_name = SubscriptionName::new("first").unwrap();
    let other_child = other_topic.subscription(&other_name).unwrap();
    put(
        &mut full_members,
        keys::topic_config(&f.namespace, &other_topic),
        value(&image, &keys::topic_config(&f.namespace, &f.topic)),
    );
    put(
        &mut full_members,
        keys::entity_metadata(&f.namespace, &other_topic),
        value(&image, &keys::entity_metadata(&f.namespace, &f.topic)),
    );
    put(
        &mut full_members,
        keys::topic_subscription(&f.namespace, &other_topic, &other_name),
        codec::encode(&other_child).unwrap(),
    );
    put(
        &mut full_members,
        keys::queue_config(&f.namespace, &other_child),
        backing.clone(),
    );
    put(
        &mut full_members,
        keys::queue_config(&f.namespace, &other_child.dead_letter_queue().unwrap()),
        shadow.clone(),
    );
    put(
        &mut full_members,
        keys::entity_metadata(&f.namespace, &other_child),
        head.clone(),
    );
    f.accept(&full_members);
    let name = SubscriptionName::new("overflow").unwrap();
    let entity = f.topic.subscription(&name).unwrap();
    put(
        &mut full_members,
        keys::topic_subscription(&f.namespace, &f.topic, &name),
        codec::encode(&entity).unwrap(),
    );
    put(
        &mut full_members,
        keys::queue_config(&f.namespace, &entity),
        backing,
    );
    put(
        &mut full_members,
        keys::queue_config(&f.namespace, &entity.dead_letter_queue().unwrap()),
        shadow,
    );
    put(
        &mut full_members,
        keys::entity_metadata(&f.namespace, &entity),
        head,
    );
    assert!(matches!(
        f.reject(&full_members),
        SnapshotCatalogError::InconsistentCatalog { .. }
    ));
}

#[test]
fn memory_source_shaped_healthy_full_and_overfull_per_owner_catalogs() {
    capacities(MemoryProvider::new());
}
#[test]
fn fjall_source_shaped_healthy_full_and_overfull_per_owner_catalogs() {
    capacities(DurableProvider::temporary().unwrap());
}
