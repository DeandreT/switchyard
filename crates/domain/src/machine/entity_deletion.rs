use crate::{
    DeleteEntityTarget, EntityDeleteLimit, MAX_SEQUENCE_NUMBER, MAX_SUBSCRIPTION_RULES,
    SubscriptionName,
};

use super::*;

/// Unique keys retained by the atomic deletion plan, including metadata.
pub const MAX_ENTITY_DELETE_KEYS: usize = 4_096;
pub const MAX_ENTITY_DELETE_KEY_BYTES: usize = 1024 * 1024;
/// Returned purge/discovery scan values, including probes and repeated scans.
/// Fixed bounded configuration/counter validation reads are additional.
pub const MAX_ENTITY_DELETE_VALUE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Default)]
struct DeletionPlan {
    keys: BTreeSet<Vec<u8>>,
    key_bytes: usize,
    value_bytes: usize,
}

impl DeletionPlan {
    fn add(&mut self, key: Vec<u8>) -> Result<(), BrokerError> {
        if self.keys.contains(&key) {
            return Ok(());
        }
        let count = self
            .keys
            .len()
            .checked_add(1)
            .ok_or_else(|| delete_limit(EntityDeleteLimit::Keys, MAX_ENTITY_DELETE_KEYS))?;
        if count > MAX_ENTITY_DELETE_KEYS {
            return Err(delete_limit(
                EntityDeleteLimit::Keys,
                MAX_ENTITY_DELETE_KEYS,
            ));
        }
        let bytes = self.key_bytes.checked_add(key.len()).ok_or_else(|| {
            delete_limit(EntityDeleteLimit::KeyBytes, MAX_ENTITY_DELETE_KEY_BYTES)
        })?;
        if bytes > MAX_ENTITY_DELETE_KEY_BYTES {
            return Err(delete_limit(
                EntityDeleteLimit::KeyBytes,
                MAX_ENTITY_DELETE_KEY_BYTES,
            ));
        }
        self.key_bytes = bytes;
        self.keys.insert(key);
        Ok(())
    }

    fn charge_values(&mut self, bytes: usize) -> Result<(), BrokerError> {
        let total = self.value_bytes.checked_add(bytes).ok_or_else(|| {
            delete_limit(EntityDeleteLimit::ValueBytes, MAX_ENTITY_DELETE_VALUE_BYTES)
        })?;
        if total > MAX_ENTITY_DELETE_VALUE_BYTES {
            return Err(delete_limit(
                EntityDeleteLimit::ValueBytes,
                MAX_ENTITY_DELETE_VALUE_BYTES,
            ));
        }
        self.value_bytes = total;
        Ok(())
    }
}

fn delete_limit(limit: EntityDeleteLimit, maximum: usize) -> BrokerError {
    BrokerError::EntityDeleteTooLarge { limit, maximum }
}

#[derive(Default)]
struct RuntimeEvidence {
    any: bool,
    message: bool,
    local_token: bool,
}

impl<S: StateStore> StateMachine<S> {
    pub(super) fn delete_entity(
        &self,
        command: &Command,
        target: &DeleteEntityTarget,
        batch: &mut WriteBatch,
    ) -> Result<(CommandOutcome, Vec<EntityPath>), BrokerError> {
        Self::require_primary_entity_path(&command.entity)?;
        let mut plan = DeletionPlan::default();
        let queue = self
            .store
            .get(&keys::queue_config(&command.namespace, &command.entity))?;
        let topic = self
            .store
            .get(&keys::topic_config(&command.namespace, &command.entity))?;
        if queue.is_some() && topic.is_some() {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        let (outcome, mut removed) = match target {
            DeleteEntityTarget::Subscription { .. } if queue.is_some() => {
                return Err(BrokerError::EntityKindMismatch);
            }
            DeleteEntityTarget::Subscription { name } => {
                let removed = self.plan_subscription_delete(command, name, &mut plan)?;
                (CommandOutcome::SubscriptionDeleted, removed)
            }
            DeleteEntityTarget::Queue if topic.is_some() => {
                return Err(BrokerError::EntityKindMismatch);
            }
            DeleteEntityTarget::Topic if queue.is_some() => {
                return Err(BrokerError::EntityKindMismatch);
            }
            DeleteEntityTarget::Auto | DeleteEntityTarget::Queue if queue.is_some() => {
                let bytes = queue
                    .as_deref()
                    .ok_or(BrokerError::DanglingEntityMetadata)?;
                let config = QueueConfig::decode(bytes)?.validate()?;
                let removed = self.plan_queue_delete(command, config, &mut plan)?;
                (CommandOutcome::QueueDeleted, removed)
            }
            DeleteEntityTarget::Auto | DeleteEntityTarget::Topic if topic.is_some() => {
                let bytes = topic
                    .as_deref()
                    .ok_or(BrokerError::DanglingEntityMetadata)?;
                codec::decode::<crate::TopicConfig>(bytes)?
                    .validate()
                    .map_err(BrokerError::TopicConfig)?;
                let removed = self.plan_topic_delete(command, &mut plan)?;
                (CommandOutcome::TopicDeleted, removed)
            }
            _ => {
                self.reject_unowned_topology(command, &command.entity, &mut plan)?;
                self.require_no_runtime(command, &command.entity, &mut plan)?;
                if self
                    .entity_incarnation(&command.namespace, &command.entity)?
                    .is_some_and(|record| !record.is_retired())
                {
                    return Err(BrokerError::DanglingEntityMetadata);
                }
                if let Ok(shadow) = command.entity.dead_letter_queue() {
                    if self
                        .store
                        .get(&keys::queue_config(&command.namespace, &shadow))?
                        .is_some()
                        || self
                            .store
                            .get(&keys::topic_config(&command.namespace, &shadow))?
                            .is_some()
                    {
                        return Err(BrokerError::DanglingEntityMetadata);
                    }
                    self.reject_unowned_topology(command, &shadow, &mut plan)?;
                    self.require_no_runtime(command, &shadow, &mut plan)?;
                }
                return Err(if matches!(target, DeleteEntityTarget::Topic) {
                    BrokerError::TopicNotFound
                } else {
                    BrokerError::QueueNotFound
                });
            }
        };
        let mut owners = Vec::new();
        match outcome {
            CommandOutcome::QueueDeleted => {
                owners.push((&command.entity, crate::EntityIncarnationKind::Queue));
            }
            CommandOutcome::TopicDeleted => {
                owners.push((&command.entity, crate::EntityIncarnationKind::Topic));
                owners.extend(
                    removed
                        .iter()
                        .skip(1)
                        .filter(|entity| !entity.is_dead_letter_queue())
                        .map(|entity| (entity, crate::EntityIncarnationKind::Subscription)),
                );
            }
            CommandOutcome::SubscriptionDeleted => {
                let owner = removed.first().ok_or(BrokerError::DanglingEntityMetadata)?;
                owners.push((owner, crate::EntityIncarnationKind::Subscription));
            }
            _ => return Err(BrokerError::DanglingEntityMetadata),
        }
        for (owner, kind) in owners {
            let record = self.require_live_incarnation(&command.namespace, owner, kind)?;
            batch.push_put(
                keys::entity_incarnation(&command.namespace, owner),
                codec::encode(&record.retire())?,
            );
        }
        removed.sort();
        removed.dedup();
        for key in plan.keys {
            batch.push_delete(key);
        }
        Ok((outcome, removed))
    }

    fn plan_queue_delete(
        &self,
        command: &Command,
        config: QueueConfig,
        plan: &mut DeletionPlan,
    ) -> Result<Vec<EntityPath>, BrokerError> {
        let shadow = command.entity.dead_letter_queue()?;
        self.require_delete_shadow(command, &shadow, config.dead_letter_shadow())?;
        self.reject_unowned_topology(command, &command.entity, plan)?;
        self.reject_unowned_topology(command, &shadow, plan)?;
        let source = self.delete_counters(command, &command.entity)?;
        self.plan_runtime_delete(command, &command.entity, source, true, plan)?;
        self.plan_runtime_delete(command, &shadow, source, false, plan)?;
        plan.add(keys::queue_config(&command.namespace, &command.entity))?;
        plan.add(keys::queue_config(&command.namespace, &shadow))?;
        Ok(vec![command.entity.clone(), shadow])
    }

    fn plan_subscription_delete(
        &self,
        command: &Command,
        name: &SubscriptionName,
        plan: &mut DeletionPlan,
    ) -> Result<Vec<EntityPath>, BrokerError> {
        let entity = command.entity.subscription(name)?;
        let shadow = entity.dead_letter_queue()?;
        self.require_delete_child_kind(command, &entity)?;
        self.require_delete_child_kind(command, &shadow)?;
        let Some(_) = self.subscription_config(&command.namespace, &command.entity, name)? else {
            if self.deletion_probe(
                &keys::rule_prefix(&command.namespace, &command.entity, name),
                plan,
            )? {
                return Err(BrokerError::DanglingRuleMetadata);
            }
            self.reject_unowned_topology(command, &entity, plan)?;
            self.reject_unowned_topology(command, &shadow, plan)?;
            self.require_no_runtime(command, &entity, plan)?;
            self.require_no_runtime(command, &shadow, plan)?;
            if self
                .entity_incarnation(&command.namespace, &entity)?
                .is_some_and(|record| !record.is_retired())
            {
                return Err(BrokerError::DanglingEntityMetadata);
            }
            return Err(
                if self
                    .bind_entity(
                        &command.namespace,
                        &command.entity,
                        &command.entity,
                        crate::EntityIncarnationKind::Topic,
                    )?
                    .is_some()
                {
                    BrokerError::SubscriptionNotFound
                } else {
                    BrokerError::TopicNotFound
                },
            );
        };
        self.bind_entity(
            &command.namespace,
            &command.entity,
            &command.entity,
            crate::EntityIncarnationKind::Topic,
        )?
        .ok_or(BrokerError::DanglingEntityMetadata)?;
        self.reject_unowned_topology(command, &entity, plan)?;
        self.reject_unowned_topology(command, &shadow, plan)?;
        let source = self.delete_counters(command, &command.entity)?;
        self.plan_runtime_delete(command, &entity, source, false, plan)?;
        self.plan_runtime_delete(command, &shadow, source, false, plan)?;
        self.plan_rule_delete(command, Some(name), &BTreeSet::from([name.as_str()]), plan)?;
        plan.add(keys::subscription(
            &command.namespace,
            &command.entity,
            name,
        ))?;
        plan.add(keys::queue_config(&command.namespace, &entity))?;
        plan.add(keys::queue_config(&command.namespace, &shadow))?;
        Ok(vec![entity, shadow])
    }

    fn plan_topic_delete(
        &self,
        command: &Command,
        plan: &mut DeletionPlan,
    ) -> Result<Vec<EntityPath>, BrokerError> {
        let subscriptions = self.subscriptions(&command.namespace, &command.entity)?;
        let mut removed = vec![command.entity.clone()];
        let mut children = BTreeSet::new();
        let mut names = BTreeSet::new();
        for subscription in &subscriptions {
            let shadow = subscription.entity.dead_letter_queue()?;
            self.require_delete_child_kind(command, &subscription.entity)?;
            self.require_delete_child_kind(command, &shadow)?;
            names.insert(subscription.name.as_str());
            children.insert(subscription.entity.as_str());
            removed.push(subscription.entity.clone());
            removed.push(shadow);
        }
        for entity in removed.iter().skip(1) {
            children.insert(entity.as_str());
        }
        self.validate_delete_descendants(command, &children, plan)?;
        let source = self.delete_counters(command, &command.entity)?;
        self.plan_runtime_delete(command, &command.entity, source, true, plan)?;
        for entity in removed.iter().skip(1) {
            self.plan_runtime_delete(command, entity, source, false, plan)?;
            plan.add(keys::queue_config(&command.namespace, entity))?;
        }
        let prefix = keys::subscription_prefix(&command.namespace, &command.entity);
        self.deletion_scan(&prefix, plan, |key, plan| {
            let name = keys::subscription_name_parts(&prefix, &key)
                .ok_or(BrokerError::MalformedIndexKey)?;
            if !names.contains(name) {
                return Err(BrokerError::DanglingSubscriptionMetadata);
            }
            plan.add(key)
        })?;
        self.plan_rule_delete(command, None, &names, plan)?;
        // Topics have no parent DLQ and may use the full primary path length.
        if let Ok(shadow) = command.entity.dead_letter_queue() {
            if self
                .store
                .get(&keys::queue_config(&command.namespace, &shadow))?
                .is_some()
                || self
                    .store
                    .get(&keys::topic_config(&command.namespace, &shadow))?
                    .is_some()
            {
                return Err(BrokerError::DanglingEntityMetadata);
            }
            self.reject_unowned_topology(command, &shadow, plan)?;
            self.require_no_runtime(command, &shadow, plan)?;
        }
        plan.add(keys::topic_config(&command.namespace, &command.entity))?;
        Ok(removed)
    }

    fn require_delete_shadow(
        &self,
        command: &Command,
        shadow: &EntityPath,
        expected: QueueConfig,
    ) -> Result<(), BrokerError> {
        self.require_delete_child_kind(command, shadow)?;
        if self.queue_config(&command.namespace, shadow)? != Some(expected) {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        Ok(())
    }

    fn require_delete_child_kind(
        &self,
        command: &Command,
        entity: &EntityPath,
    ) -> Result<(), BrokerError> {
        if self
            .store
            .get(&keys::topic_config(&command.namespace, entity))?
            .is_some()
        {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        Ok(())
    }

    fn delete_counters(&self, command: &Command, entity: &EntityPath) -> Result<bool, BrokerError> {
        let Some(counters) =
            self.read::<QueueCounters>(&keys::queue_counters(&command.namespace, entity))?
        else {
            return Ok(false);
        };
        if counters.next_sequence == 0
            || counters.next_sequence > MAX_SEQUENCE_NUMBER + 1
            || counters.next_lock_token == 0
        {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        Ok(true)
    }

    fn plan_runtime_delete(
        &self,
        command: &Command,
        entity: &EntityPath,
        source_counter: bool,
        primary: bool,
        plan: &mut DeletionPlan,
    ) -> Result<(), BrokerError> {
        let local_counter = self.delete_counters(command, entity)?;
        let mut evidence = RuntimeEvidence::default();
        for (prefix, kind) in keys::entity_runtime_prefixes(&command.namespace, entity) {
            self.deletion_scan(&prefix, plan, |key, plan| {
                evidence.any = true;
                evidence.message |= kind == keys::RuntimeKind::Message;
                evidence.local_token |= kind == keys::RuntimeKind::LocalToken;
                plan.add(key)
            })?;
        }
        if (!source_counter && (evidence.message || (primary && evidence.any)))
            || (!local_counter && evidence.local_token)
        {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        Ok(())
    }

    fn plan_rule_delete(
        &self,
        command: &Command,
        subscription: Option<&SubscriptionName>,
        names: &BTreeSet<&str>,
        plan: &mut DeletionPlan,
    ) -> Result<(), BrokerError> {
        let owner_prefix = keys::topic_rule_prefix(&command.namespace, &command.entity);
        let prefix = subscription.map_or_else(
            || owner_prefix.clone(),
            |name| keys::rule_prefix(&command.namespace, &command.entity, name),
        );
        let mut counts = BTreeMap::<String, usize>::new();
        self.deletion_scan(&prefix, plan, |key, plan| {
            let (name, _) = keys::topic_rule_parts(&owner_prefix, &key)
                .ok_or(BrokerError::MalformedIndexKey)?;
            if !names.contains(name) {
                return Err(BrokerError::DanglingRuleMetadata);
            }
            let count = counts.entry(name.to_owned()).or_default();
            *count += 1;
            if *count > MAX_SUBSCRIPTION_RULES {
                return Err(BrokerError::RuleLimitExceeded {
                    maximum: MAX_SUBSCRIPTION_RULES,
                });
            }
            plan.add(key)
        })
    }

    fn validate_delete_descendants(
        &self,
        command: &Command,
        owners: &BTreeSet<&str>,
        plan: &mut DeletionPlan,
    ) -> Result<(), BrokerError> {
        let namespace = &command.namespace;
        let topic = &command.entity;
        for prefix in [
            keys::subscription_topic_config_prefix(namespace, topic),
            keys::subscription_membership_descendant_prefix(namespace, topic),
            keys::subscription_rule_descendant_prefix(namespace, topic),
        ] {
            if self.deletion_probe(&prefix, plan)? {
                return Err(BrokerError::DanglingSubscriptionMetadata);
            }
        }
        let prefix = keys::subscription_backing_config_prefix(namespace, topic);
        self.deletion_scan(&prefix, plan, |key, plan| {
            let (ns, entity) =
                keys::entity_scope_parts(&key).ok_or(BrokerError::MalformedIndexKey)?;
            if ns != namespace.as_str() || !owners.contains(entity) {
                return Err(BrokerError::DanglingSubscriptionMetadata);
            }
            let entity = EntityPath::new(entity).map_err(|_| BrokerError::MalformedIndexKey)?;
            if keys::queue_config(namespace, &entity) != key {
                return Err(BrokerError::MalformedIndexKey);
            }
            plan.add(key)
        })?;
        for (prefix, _) in keys::subscription_runtime_prefixes(namespace, topic) {
            self.deletion_scan(&prefix, plan, |key, plan| {
                let (ns, entity) =
                    keys::entity_scope_parts(&key).ok_or(BrokerError::MalformedIndexKey)?;
                if ns != namespace.as_str() || !owners.contains(entity) {
                    return Err(BrokerError::DanglingSubscriptionMetadata);
                }
                plan.add(key)
            })?;
        }
        Ok(())
    }

    fn reject_unowned_topology(
        &self,
        command: &Command,
        entity: &EntityPath,
        plan: &mut DeletionPlan,
    ) -> Result<(), BrokerError> {
        if self.deletion_probe(&keys::topic_rule_prefix(&command.namespace, entity), plan)?
            || self.deletion_probe(
                &keys::subscription_rule_descendant_prefix(&command.namespace, entity),
                plan,
            )?
        {
            return Err(BrokerError::DanglingRuleMetadata);
        }
        for prefix in [
            keys::subscription_prefix(&command.namespace, entity),
            keys::subscription_backing_config_prefix(&command.namespace, entity),
            keys::subscription_topic_config_prefix(&command.namespace, entity),
            keys::subscription_membership_descendant_prefix(&command.namespace, entity),
        ] {
            if self.deletion_probe(&prefix, plan)? {
                return Err(BrokerError::DanglingSubscriptionMetadata);
            }
        }
        for (prefix, _) in keys::subscription_runtime_prefixes(&command.namespace, entity) {
            if self.deletion_probe(&prefix, plan)? {
                return Err(BrokerError::DanglingSubscriptionMetadata);
            }
        }
        Ok(())
    }

    fn require_no_runtime(
        &self,
        command: &Command,
        entity: &EntityPath,
        plan: &mut DeletionPlan,
    ) -> Result<(), BrokerError> {
        for (prefix, _) in keys::entity_runtime_prefixes(&command.namespace, entity) {
            if self.deletion_probe(&prefix, plan)? {
                return Err(BrokerError::DanglingEntityMetadata);
            }
        }
        Ok(())
    }

    fn deletion_probe(&self, prefix: &[u8], plan: &mut DeletionPlan) -> Result<bool, BrokerError> {
        let Some((_, value)) = self.store.scan_prefix(prefix, 1)?.into_iter().next() else {
            return Ok(false);
        };
        plan.charge_values(value.len())?;
        Ok(true)
    }

    fn deletion_scan(
        &self,
        prefix: &[u8],
        plan: &mut DeletionPlan,
        mut visit: impl FnMut(Vec<u8>, &mut DeletionPlan) -> Result<(), BrokerError>,
    ) -> Result<(), BrokerError> {
        let mut start = prefix.to_vec();
        while let Some((key, value)) = self.store.scan_from(prefix, &start, 1)?.into_iter().next() {
            plan.charge_values(value.len())?;
            drop(value);
            start.clone_from(&key);
            start.push(0);
            visit(key, plan)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
