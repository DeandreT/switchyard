use crate::{SubscriptionConfigUpdate, SubscriptionName, TopicConfig, TopicConfigUpdate};

use super::*;

impl<S: StateStore> StateMachine<S> {
    pub(super) fn update_topic(
        &self,
        command: &Command,
        update: TopicConfigUpdate,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        Self::require_primary_entity_path(&command.entity)?;
        let current = self.topology_update_parent(command)?;
        // Complete membership validation also rejects orphan membership when
        // the parent is absent. Configuration updates never repair topology.
        let subscriptions = self.subscriptions(&command.namespace, &command.entity)?;
        for subscription in &subscriptions {
            self.require_update_child_kinds(command, &subscription.entity)?;
        }
        let current = current.ok_or(BrokerError::TopicNotFound)?;
        let config = update.apply_to(current)?;
        if config != current {
            batch.push_put(
                keys::topic_config(&command.namespace, &command.entity),
                codec::encode(&config)?,
            );
        }
        Ok(CommandOutcome::TopicUpdated)
    }

    pub(super) fn update_subscription(
        &self,
        command: &Command,
        name: &SubscriptionName,
        update: SubscriptionConfigUpdate,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        Self::require_primary_entity_path(&command.entity)?;
        let parent = self.topology_update_parent(command)?;
        let entity = command.entity.subscription(name)?;
        self.require_update_child_kinds(command, &entity)?;
        let Some(current) = self.subscription_config(&command.namespace, &command.entity, name)?
        else {
            return Err(if parent.is_some() {
                BrokerError::SubscriptionNotFound
            } else {
                BrokerError::TopicNotFound
            });
        };
        let config = update.apply_to(current)?;
        if config != current {
            let shadow = entity.dead_letter_queue()?;
            let backing = config.to_queue_config();
            batch.push_put(
                keys::subscription(&command.namespace, &command.entity, name),
                codec::encode(&config)?,
            );
            batch.push_put(
                keys::queue_config(&command.namespace, &entity),
                codec::encode(&backing)?,
            );
            batch.push_put(
                keys::queue_config(&command.namespace, &shadow),
                codec::encode(&backing.dead_letter_shadow())?,
            );
        }
        Ok(CommandOutcome::SubscriptionUpdated)
    }

    fn topology_update_parent(
        &self,
        command: &Command,
    ) -> Result<Option<TopicConfig>, BrokerError> {
        let topic = self
            .store
            .get(&keys::topic_config(&command.namespace, &command.entity))?;
        if topic.is_some()
            && self
                .store
                .get(&keys::queue_config(&command.namespace, &command.entity))?
                .is_some()
        {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        topic
            .map(|bytes| {
                codec::decode::<TopicConfig>(&bytes)?
                    .validate()
                    .map_err(BrokerError::TopicConfig)
            })
            .transpose()
    }

    fn require_update_child_kinds(
        &self,
        command: &Command,
        entity: &EntityPath,
    ) -> Result<(), BrokerError> {
        let shadow = entity.dead_letter_queue()?;
        if self
            .store
            .get(&keys::topic_config(&command.namespace, entity))?
            .is_some()
            || self
                .store
                .get(&keys::topic_config(&command.namespace, &shadow))?
                .is_some()
        {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        Ok(())
    }
}
