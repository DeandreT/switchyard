use std::collections::BTreeMap;

use serde::{Serialize, de::DeserializeOwned};
use storage::{Key, Value};

use crate::{
    DEAD_LETTER_QUEUE_SUFFIX, EntityBindingKind, EntityPath, MAX_SUBSCRIPTION_RULES,
    MAX_TOPIC_SUBSCRIPTIONS, NamespaceName, QueueConfig, QueueCounters, RuleDefinition,
    SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec,
    identifier::SUBSCRIPTION_PATH_SEGMENT, keys, machine::decode_catalog_owner,
};

use super::{CatalogValidation, SnapshotCatalogError};

type Scope = (NamespaceName, EntityPath);

struct Located<T> {
    row: usize,
    value: T,
}

struct Catalog<'a> {
    report: CatalogValidation,
    queues: BTreeMap<Scope, Located<QueueConfig>>,
    topics: BTreeMap<Scope, Located<TopicConfig>>,
    counters: BTreeMap<Scope, usize>,
    heads: BTreeMap<Scope, Located<&'a [u8]>>,
    memberships: BTreeMap<Scope, Located<Scope>>,
    rules: Vec<(Scope, Located<RuleDefinition>)>,
}

/// Validates every catalog row and companion in one complete logical image.
///
/// Input keys must be strictly ordered. Recognized state tags 03..0D and
/// external F0/F1 rows are counted but not decoded or certified; all original
/// bytes remain untouched. Present domain rows require a canonical Clock.
/// A canonical Clock-only image is permitted without a reachability claim.
/// Ordinary temporary indexes allocate in proportion to the supplied catalog;
/// this is not a hostile-heap bound, a capture operation or full state health.
pub fn validate_catalog(
    records: &[(Key, Value)],
) -> Result<CatalogValidation, SnapshotCatalogError> {
    let catalog = Catalog::read(records)?;
    catalog.validate_topics()?;
    catalog.validate_queues()?;
    catalog.validate_memberships()?;
    catalog.validate_heads()?;
    catalog.validate_counters()?;
    catalog.validate_rules()?;
    Ok(catalog.report)
}

impl<'a> Catalog<'a> {
    fn read(records: &'a [(Key, Value)]) -> Result<Self, SnapshotCatalogError> {
        for (row, (key, _)) in records.iter().enumerate() {
            if key.is_empty() {
                return Err(SnapshotCatalogError::EmptyKey { row });
            }
            if row > 0 && records[row - 1].0.as_slice() >= key.as_slice() {
                return Err(SnapshotCatalogError::InputOrder { row });
            }
        }
        let mut catalog = Self {
            report: CatalogValidation {
                clock: None,
                catalog_rows: 0,
                unvalidated_state_rows: 0,
                unvalidated_external_rows: 0,
            },
            queues: BTreeMap::new(),
            topics: BTreeMap::new(),
            counters: BTreeMap::new(),
            heads: BTreeMap::new(),
            memberships: BTreeMap::new(),
            rules: Vec::new(),
        };
        let mut populated_domain = false;
        for (row, (key, value)) in records.iter().enumerate() {
            let Some(&tag) = key.first() else {
                return Err(SnapshotCatalogError::EmptyKey { row });
            };
            populated_domain |= matches!(tag, 0x01..=0x11);
            match tag {
                0x03..=0x0D => {
                    catalog.report.unvalidated_state_rows += 1;
                    continue;
                }
                0xF0 | 0xF1 => {
                    catalog.report.unvalidated_external_rows += 1;
                    continue;
                }
                0x00 => {
                    if key.as_slice() != [0x00] {
                        return Err(invalid_key(row, "Clock key is not the singleton"));
                    }
                    catalog.report.clock = Some(decode::<Timestamp>(value, row)?);
                }
                0x01 => {
                    let scope = entity_parts(key, row)?;
                    let config = decode::<QueueConfig>(value, row)?;
                    config
                        .validate()
                        .map_err(|_| invalid_value(row, "queue profile violates current policy"))?;
                    catalog.queues.insert(scope, Located { row, value: config });
                }
                0x02 => {
                    let scope = entity_parts(key, row)?;
                    let counters = decode::<QueueCounters>(value, row)?;
                    if counters.next_sequence == 0 || counters.next_lock_token == 0 {
                        return Err(invalid_value(row, "allocation counters must be nonzero"));
                    }
                    catalog.counters.insert(scope, row);
                }
                0x0E => {
                    let scope = entity_parts(key, row)?;
                    let config = decode::<TopicConfig>(value, row)?;
                    config
                        .validate()
                        .map_err(|_| invalid_value(row, "topic profile violates current policy"))?;
                    catalog.topics.insert(scope, Located { row, value: config });
                }
                0x0F => {
                    let (namespace, topic, name) = keys::catalog_subscription_parts(key)
                        .ok_or_else(|| invalid_key(row, "membership key is not canonical"))?;
                    let child = topic
                        .subscription(&name)
                        .map_err(|_| invalid_key(row, "subscription path cannot be formed"))?;
                    let expected = codec::encode(&child)
                        .map_err(|_| invalid_value(row, "membership cannot be encoded"))?;
                    if *value != expected {
                        return Err(invalid_value(
                            row,
                            "membership does not encode its exact canonical child",
                        ));
                    }
                    let parent = (namespace.clone(), topic);
                    if catalog
                        .memberships
                        .insert((namespace, child), Located { row, value: parent })
                        .is_some()
                    {
                        return Err(inconsistent(row, "membership aliases another child"));
                    }
                }
                0x10 => {
                    let (namespace, subscription, name) = keys::catalog_rule_parts(key)
                        .ok_or_else(|| invalid_key(row, "rule key is not canonical"))?;
                    let definition = decode::<RuleDefinition>(value, row)?;
                    if definition.name.as_str() != name.as_str() {
                        return Err(invalid_value(row, "rule name does not match its key"));
                    }
                    let canonical = definition
                        .filter
                        .canonicalized()
                        .map_err(|_| invalid_value(row, "rule filter violates current policy"))?;
                    if canonical != definition.filter {
                        return Err(invalid_value(row, "rule filter is not canonical"));
                    }
                    catalog.rules.push((
                        (namespace, subscription),
                        Located {
                            row,
                            value: definition,
                        },
                    ));
                }
                0x11 => {
                    catalog
                        .heads
                        .insert(entity_parts(key, row)?, Located { row, value });
                }
                _ => return Err(SnapshotCatalogError::UnsupportedTag { row, tag }),
            }
            catalog.report.catalog_rows += 1;
        }
        if populated_domain && catalog.report.clock.is_none() {
            return Err(SnapshotCatalogError::MissingClock);
        }
        Ok(catalog)
    }

    fn validate_topics(&self) -> Result<(), SnapshotCatalogError> {
        for (scope, topic) in &self.topics {
            if primary_path(scope.1.as_str()).is_none() {
                return Err(inconsistent(topic.row, "topic occupies a reserved path"));
            }
            if self.queues.contains_key(scope) {
                return Err(inconsistent(topic.row, "topic and queue profiles collide"));
            }
            self.require_head(scope, topic.row)?;
        }
        Ok(())
    }

    fn validate_queues(&self) -> Result<(), SnapshotCatalogError> {
        for (scope, queue) in &self.queues {
            let (owner, kind, shadow) = queue_owner(&scope.1).ok_or_else(|| {
                inconsistent(queue.row, "queue occupies an unsupported reserved path")
            })?;
            if self.topics.contains_key(scope) {
                return Err(inconsistent(queue.row, "queue and topic profiles collide"));
            }
            if shadow {
                let owner_scope = (scope.0.clone(), owner);
                let parent = self
                    .queues
                    .get(&owner_scope)
                    .ok_or_else(|| inconsistent(queue.row, "dead-letter profile has no parent"))?;
                if queue.value != parent.value.dead_letter_shadow() {
                    return Err(inconsistent(
                        queue.row,
                        "dead-letter profile is not the exact parent shadow",
                    ));
                }
                if self.heads.contains_key(scope) {
                    return Err(inconsistent(
                        queue.row,
                        "dead-letter queue has its own owner head",
                    ));
                }
                continue;
            }
            self.require_head(scope, queue.row)?;
            let shadow_scope = shadow_scope(scope, queue.row)?;
            let shadow = self
                .queues
                .get(&shadow_scope)
                .ok_or_else(|| inconsistent(queue.row, "queue has no dead-letter shadow"))?;
            if shadow.value != queue.value.dead_letter_shadow() {
                return Err(inconsistent(
                    shadow.row,
                    "dead-letter profile is not the exact parent shadow",
                ));
            }
            if kind == EntityBindingKind::Subscription {
                let membership = self.memberships.get(scope).ok_or_else(|| {
                    inconsistent(queue.row, "subscription backing has no membership")
                })?;
                let topic = self
                    .topics
                    .get(&membership.value)
                    .ok_or_else(|| inconsistent(queue.row, "subscription has no parent topic"))?;
                let config = SubscriptionConfig {
                    lock_duration_millis: queue.value.lock_duration_millis,
                    max_delivery_count: queue.value.max_delivery_count,
                    default_time_to_live_millis: queue.value.default_time_to_live_millis,
                };
                config.validate().map_err(|_| {
                    invalid_value(queue.row, "subscription profile violates current policy")
                })?;
                if queue.value != config.queue_config(topic.value) {
                    return Err(inconsistent(
                        queue.row,
                        "subscription backing differs from the parent-derived profile",
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_memberships(&self) -> Result<(), SnapshotCatalogError> {
        let mut counts = BTreeMap::<&Scope, usize>::new();
        for (child, membership) in &self.memberships {
            if !self.topics.contains_key(&membership.value) {
                return Err(inconsistent(
                    membership.row,
                    "membership has no parent topic",
                ));
            }
            if !self.queues.contains_key(child) {
                return Err(inconsistent(
                    membership.row,
                    "membership has no subscription backing",
                ));
            }
            let count = counts.entry(&membership.value).or_default();
            *count += 1;
            if *count > MAX_TOPIC_SUBSCRIPTIONS {
                return Err(inconsistent(
                    membership.row,
                    "topic membership exceeds current capacity",
                ));
            }
        }
        Ok(())
    }

    fn validate_heads(&self) -> Result<(), SnapshotCatalogError> {
        for (scope, head) in &self.heads {
            let kind = if self.topics.contains_key(scope) {
                EntityBindingKind::Topic
            } else if self.queues.contains_key(scope) {
                let (_, kind, shadow) = queue_owner(&scope.1)
                    .ok_or_else(|| inconsistent(head.row, "owner has an unsupported path"))?;
                if shadow {
                    return Err(inconsistent(
                        head.row,
                        "dead-letter queue has its own owner head",
                    ));
                }
                kind
            } else {
                return Err(inconsistent(head.row, "owner head has no catalog owner"));
            };
            decode_catalog_owner(head.value, kind)
                .map_err(|_| invalid_value(head.row, "owner head is not canonical and live"))?;
        }
        Ok(())
    }

    fn validate_counters(&self) -> Result<(), SnapshotCatalogError> {
        for (scope, row) in &self.counters {
            if !self.queues.contains_key(scope) && !self.topics.contains_key(scope) {
                return Err(inconsistent(*row, "counter has no catalog endpoint"));
            }
        }
        Ok(())
    }

    fn validate_rules(&self) -> Result<(), SnapshotCatalogError> {
        let mut counts = BTreeMap::<&Scope, usize>::new();
        for (scope, rule) in &self.rules {
            if !self.memberships.contains_key(scope) {
                return Err(inconsistent(
                    rule.row,
                    "rule has no subscription membership",
                ));
            }
            let Some(clock) = self.report.clock else {
                return Err(SnapshotCatalogError::MissingClock);
            };
            if rule.value.created_at > clock {
                return Err(inconsistent(
                    rule.row,
                    "rule was created after the captured Clock",
                ));
            }
            let count = counts.entry(scope).or_default();
            *count += 1;
            if *count > MAX_SUBSCRIPTION_RULES {
                return Err(inconsistent(
                    rule.row,
                    "subscription rules exceed current capacity",
                ));
            }
        }
        Ok(())
    }

    fn require_head(&self, scope: &Scope, row: usize) -> Result<(), SnapshotCatalogError> {
        if self.heads.contains_key(scope) {
            Ok(())
        } else {
            Err(inconsistent(row, "catalog owner has no live head"))
        }
    }
}

fn decode<T: DeserializeOwned + Serialize>(
    raw: &[u8],
    row: usize,
) -> Result<T, SnapshotCatalogError> {
    let value = codec::decode::<T>(raw)
        .map_err(|_| invalid_value(row, "catalog value cannot be decoded"))?;
    let canonical =
        codec::encode(&value).map_err(|_| invalid_value(row, "catalog value cannot be encoded"))?;
    if canonical != raw {
        return Err(invalid_value(row, "catalog value is not canonical"));
    }
    Ok(value)
}

fn entity_parts(key: &[u8], row: usize) -> Result<Scope, SnapshotCatalogError> {
    keys::catalog_entity_parts(key)
        .ok_or_else(|| invalid_key(row, "entity catalog key is not canonical"))
}

fn primary_path(value: &str) -> Option<EntityPath> {
    let path = EntityPath::new(value).ok()?;
    (!path.is_dead_letter_queue() && !path.is_subscription() && !path.is_management())
        .then_some(path)
}

fn queue_owner(entity: &EntityPath) -> Option<(EntityPath, EntityBindingKind, bool)> {
    let (value, shadow) = entity
        .as_str()
        .strip_suffix(DEAD_LETTER_QUEUE_SUFFIX)
        .map_or((entity.as_str(), false), |owner| (owner, true));
    let (owner, kind) = if let Some((topic, name)) = value.split_once(SUBSCRIPTION_PATH_SEGMENT) {
        let topic = primary_path(topic)?;
        let name = SubscriptionName::new(name).ok()?;
        (
            topic.subscription(&name).ok()?,
            EntityBindingKind::Subscription,
        )
    } else {
        (primary_path(value)?, EntityBindingKind::Queue)
    };
    let canonical = if shadow {
        owner.dead_letter_queue().ok()? == *entity
    } else {
        owner == *entity
    };
    canonical.then_some((owner, kind, shadow))
}

fn shadow_scope(scope: &Scope, row: usize) -> Result<Scope, SnapshotCatalogError> {
    let shadow = scope
        .1
        .dead_letter_queue()
        .map_err(|_| inconsistent(row, "dead-letter path cannot be formed"))?;
    Ok((scope.0.clone(), shadow))
}

fn invalid_key(row: usize, detail: &'static str) -> SnapshotCatalogError {
    SnapshotCatalogError::InvalidKey { row, detail }
}

fn invalid_value(row: usize, detail: &'static str) -> SnapshotCatalogError {
    SnapshotCatalogError::InvalidValue { row, detail }
}

fn inconsistent(row: usize, detail: &'static str) -> SnapshotCatalogError {
    SnapshotCatalogError::InconsistentCatalog { row, detail }
}
