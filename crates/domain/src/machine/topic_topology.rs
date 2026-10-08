use crate::{
    MAX_TOPIC_SUBSCRIPTIONS, RuleDefinition, RuleFilter, RuleName, SubscriptionConfig,
    SubscriptionDefinition, SubscriptionName, TopicConfig,
};

use super::*;

impl<S: StateStore> StateMachine<S> {
    /// Reads and validates the topic's distinct metadata and mandatory mode.
    pub fn topic_config(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> Result<Option<TopicConfig>, BrokerError> {
        let config = self.topic_config_topology(namespace, entity)?;
        if config.is_some() {
            self.validate_capacity_binding_profile(
                namespace,
                entity,
                entity,
                crate::EntityIncarnationKind::Topic,
            )?;
        } else {
            self.reject_orphaned_topic_mode(namespace, entity)?;
        }
        Ok(config)
    }

    /// Reads numeric topic topology only, without capacity or identity proof.
    pub fn topic_config_topology(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> Result<Option<TopicConfig>, BrokerError> {
        let Some(config) = self.read::<TopicConfig>(&keys::topic_config(namespace, entity))? else {
            return Ok(None);
        };
        let config = config.validate().map_err(BrokerError::TopicConfig)?;
        Ok(Some(config))
    }

    /// Reads complete subscription metadata before validating capacity exclusion.
    pub fn subscription_config(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        name: &SubscriptionName,
    ) -> Result<Option<SubscriptionConfig>, BrokerError> {
        let config = self.subscription_config_topology(namespace, topic, name)?;
        let entity = topic.subscription(name)?;
        self.validate_capacity_binding_profile(
            namespace,
            &entity,
            &entity,
            crate::EntityIncarnationKind::Subscription,
        )?;
        if config.is_some() {
            self.validate_capacity_binding_profile(
                namespace,
                topic,
                topic,
                crate::EntityIncarnationKind::Topic,
            )?;
        }
        Ok(config)
    }

    /// Reads complete subscription topology without capacity or identity proof.
    /// An absent subscription has no membership, backing queue, or DLQ record.
    /// Any partial topology is reported rather than interpreted as absence.
    pub fn subscription_config_topology(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        name: &SubscriptionName,
    ) -> Result<Option<SubscriptionConfig>, BrokerError> {
        let entity = topic.subscription(name)?;
        let shadow = entity.dead_letter_queue()?;
        let config = self
            .store
            .get(&keys::subscription(namespace, topic, name))?
            .map(|bytes| SubscriptionConfig::decode(&bytes))
            .transpose()?;
        let backing = self.queue_config(namespace, &entity)?;
        let dead_letter = self.queue_config(namespace, &shadow)?;
        if config.is_none() && backing.is_none() && dead_letter.is_none() {
            return Ok(None);
        }
        let parent = self.read::<TopicConfig>(&keys::topic_config(namespace, topic))?;
        let (Some(config), Some(backing), Some(dead_letter), Some(parent)) =
            (config, backing, dead_letter, parent)
        else {
            return Err(BrokerError::DanglingSubscriptionMetadata);
        };
        if parent.validate().is_err() || config.validate().is_err() {
            return Err(BrokerError::DanglingSubscriptionMetadata);
        }
        let expected = config.to_queue_config();
        if backing != expected || dead_letter != expected.dead_letter_shadow() {
            return Err(BrokerError::DanglingSubscriptionMetadata);
        }
        Ok(Some(config))
    }

    /// Returns complete bounded membership after validating all capacity exclusions.
    pub fn subscriptions(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
    ) -> Result<Vec<SubscriptionDefinition>, BrokerError> {
        let definitions = self.subscriptions_topology(namespace, topic)?;
        self.validate_capacity_binding_profile(
            namespace,
            topic,
            topic,
            crate::EntityIncarnationKind::Topic,
        )?;
        for definition in &definitions {
            self.validate_capacity_binding_profile(
                namespace,
                &definition.entity,
                &definition.entity,
                crate::EntityIncarnationKind::Subscription,
            )?;
        }
        Ok(definitions)
    }

    /// Reads complete bounded membership without capacity or identity proof.
    pub fn subscriptions_topology(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
    ) -> Result<Vec<SubscriptionDefinition>, BrokerError> {
        let prefix = keys::subscription_prefix(namespace, topic);
        let entries = self
            .store
            .scan_prefix(&prefix, MAX_TOPIC_SUBSCRIPTIONS + 1)?;
        let Some(parent) = self.read::<TopicConfig>(&keys::topic_config(namespace, topic))? else {
            return Err(if entries.is_empty() {
                BrokerError::TopicNotFound
            } else {
                BrokerError::DanglingSubscriptionMetadata
            });
        };
        if parent.validate().is_err() {
            return Err(BrokerError::DanglingSubscriptionMetadata);
        }
        if entries.len() > MAX_TOPIC_SUBSCRIPTIONS {
            return Err(BrokerError::SubscriptionLimitExceeded {
                maximum: MAX_TOPIC_SUBSCRIPTIONS,
            });
        }
        let mut definitions = Vec::with_capacity(entries.len());
        for (key, bytes) in entries {
            let name = keys::subscription_name_parts(&prefix, &key)
                .ok_or(BrokerError::MalformedIndexKey)?;
            let name = SubscriptionName::new(name).map_err(|_| BrokerError::MalformedIndexKey)?;
            if keys::subscription(namespace, topic, &name) != key {
                return Err(BrokerError::MalformedIndexKey);
            }
            let config = SubscriptionConfig::decode(&bytes)?;
            if config.validate().is_err() {
                return Err(BrokerError::DanglingSubscriptionMetadata);
            }
            let entity = topic.subscription(&name)?;
            let shadow = entity.dead_letter_queue()?;
            let expected = config.to_queue_config();
            if self.queue_config(namespace, &entity)? != Some(expected)
                || self.queue_config(namespace, &shadow)? != Some(expected.dead_letter_shadow())
            {
                return Err(BrokerError::DanglingSubscriptionMetadata);
            }
            definitions.push(SubscriptionDefinition {
                name,
                entity,
                config,
            });
        }
        Ok(definitions)
    }

    pub(super) fn create_topic(
        &self,
        command: &Command,
        config: TopicConfig,
        batch: &mut WriteBatch,
        capacity: &mut CapacityPlan,
    ) -> Result<CommandOutcome, BrokerError> {
        Self::require_primary_entity_path(&command.entity)?;
        let key = keys::topic_config(&command.namespace, &command.entity);
        if self.store.get(&key)?.is_some() {
            return Err(BrokerError::TopicAlreadyExists);
        }
        if self
            .store
            .get(&keys::queue_config(&command.namespace, &command.entity))?
            .is_some()
        {
            return Err(BrokerError::EntityPathAlreadyExists);
        }
        let config = config.validate().map_err(BrokerError::TopicConfig)?;
        let incarnation = self.stage_create_incarnation(
            &command.namespace,
            &command.entity,
            crate::EntityIncarnationKind::Topic,
            batch,
        )?;
        let mode = crate::topic_mode::NonFiniteTopicMode::non_finite(incarnation.generation())?;
        batch.push_put(key, codec::encode(&config)?);
        batch.push_put(
            keys::topic_mode(&command.namespace, &command.entity),
            mode.encode()?,
        );
        capacity.prepare_excluded_owner(incarnation, mode)?;
        Ok(CommandOutcome::TopicCreated)
    }

    pub(super) fn create_subscription(
        &self,
        command: &Command,
        name: &SubscriptionName,
        config: SubscriptionConfig,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        Self::require_primary_entity_path(&command.entity)?;
        self.topic_config(&command.namespace, &command.entity)?
            .ok_or(BrokerError::TopicNotFound)?;
        self.bind_entity(
            &command.namespace,
            &command.entity,
            &command.entity,
            crate::EntityIncarnationKind::Topic,
        )?
        .ok_or(BrokerError::DanglingEntityMetadata)?;
        let config = config.validate().map_err(BrokerError::SubscriptionConfig)?;
        let entity = command.entity.subscription(name)?;
        let shadow = entity.dead_letter_queue()?;
        let key = keys::subscription(&command.namespace, &command.entity, name);
        if self.store.get(&key)?.is_some() {
            return Err(BrokerError::SubscriptionAlreadyExists);
        }
        for path in [&entity, &shadow] {
            if self
                .store
                .get(&keys::queue_config(&command.namespace, path))?
                .is_some()
                || self
                    .store
                    .get(&keys::topic_config(&command.namespace, path))?
                    .is_some()
            {
                return Err(BrokerError::EntityPathAlreadyExists);
            }
        }
        if !self
            .store
            .scan_prefix(
                &keys::rule_prefix(&command.namespace, &command.entity, name),
                1,
            )?
            .is_empty()
        {
            return Err(BrokerError::DanglingRuleMetadata);
        }
        if self
            .subscriptions(&command.namespace, &command.entity)?
            .len()
            == MAX_TOPIC_SUBSCRIPTIONS
        {
            return Err(BrokerError::SubscriptionLimitExceeded {
                maximum: MAX_TOPIC_SUBSCRIPTIONS,
            });
        }
        let backing = config.to_queue_config();
        self.stage_create_incarnation(
            &command.namespace,
            &entity,
            crate::EntityIncarnationKind::Subscription,
            batch,
        )?;
        batch.push_put(key, codec::encode(&config)?);
        batch.push_put(
            keys::queue_config(&command.namespace, &entity),
            codec::encode(&backing)?,
        );
        batch.push_put(
            keys::queue_config(&command.namespace, &shadow),
            codec::encode(&backing.dead_letter_shadow())?,
        );
        let default = RuleDefinition {
            name: RuleName::new("$Default")?,
            filter: RuleFilter::True,
            created_at: command.issued_at,
            action: None,
        };
        batch.push_put(
            keys::rule(&command.namespace, &command.entity, name, &default.name),
            codec::encode(&default)?,
        );
        Ok(CommandOutcome::SubscriptionCreated)
    }

    pub(super) fn require_primary_entity_path(entity: &EntityPath) -> Result<(), BrokerError> {
        if entity.is_dead_letter_queue() {
            return Err(BrokerError::DeadLetterQueueIsReserved);
        }
        if entity.is_subscription_path() {
            return Err(BrokerError::SubscriptionPathIsReserved);
        }
        Ok(())
    }

    pub(super) fn require_queue_ingress_target(
        &self,
        command: &Command,
    ) -> Result<(), BrokerError> {
        Self::require_primary_entity_path(&command.entity)?;
        if self
            .topic_config(&command.namespace, &command.entity)?
            .is_some()
        {
            return Err(BrokerError::TopicDataPlaneNotImplemented);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "topic_topology/topic_mode_tests.rs"]
mod topic_mode_tests;
