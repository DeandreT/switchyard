use protocol_amqp::{Attachment, EntityMetadata, parse_attachment};

use super::*;

impl<S: StateStore, C: Clock> LocalProposer<S, C> {
    /// Reads validated link topology without consulting or advancing the clock.
    pub fn entity_metadata(
        &self,
        namespace: &NamespaceName,
        target: &Attachment,
    ) -> Result<Option<EntityMetadata>, ProposeError> {
        NamespaceName::new(namespace.as_str()).map_err(BrokerError::from)?;
        let entity = target
            .canonical_entity()
            .map_err(|_| BrokerError::DanglingEntityMetadata)?;
        if parse_attachment(entity.as_str()).ok().as_ref() != Some(target) {
            return Err(BrokerError::DanglingEntityMetadata.into());
        }
        match target {
            Attachment::Queue(entity) => self.primary_entity_metadata(namespace, entity),
            Attachment::DeadLetter(parent) => {
                let parent_metadata = self.primary_entity_metadata(namespace, parent)?;
                let shadow = self.machine.queue_config(namespace, &entity)?;
                if self.machine.topic_config(namespace, &entity)?.is_some() {
                    return Err(BrokerError::DanglingEntityMetadata.into());
                }
                match parent_metadata {
                    Some(EntityMetadata::Queue(parent)) => {
                        if shadow != Some(parent.dead_letter_shadow()) {
                            return Err(BrokerError::DanglingEntityMetadata.into());
                        }
                        Ok(shadow.map(EntityMetadata::DeadLetter))
                    }
                    _ if shadow.is_some() => Err(BrokerError::DanglingEntityMetadata.into()),
                    _ => Ok(None),
                }
            }
            Attachment::Subscription {
                topic,
                subscription,
            }
            | Attachment::SubscriptionDeadLetter {
                topic,
                subscription,
            } => {
                let backing = topic
                    .subscription(subscription)
                    .map_err(BrokerError::from)?;
                let shadow = backing.dead_letter_queue().map_err(BrokerError::from)?;
                if self.machine.queue_config(namespace, topic)?.is_some()
                    || self.machine.topic_config(namespace, &backing)?.is_some()
                    || self.machine.topic_config(namespace, &shadow)?.is_some()
                {
                    return Err(BrokerError::DanglingEntityMetadata.into());
                }
                let config = self
                    .machine
                    .subscription_config(namespace, topic, subscription)?;
                Ok(config.map(|config| match target {
                    Attachment::Subscription { .. } => EntityMetadata::Subscription(config),
                    _ => EntityMetadata::DeadLetter(config.to_queue_config().dead_letter_shadow()),
                }))
            }
        }
    }

    fn primary_entity_metadata(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> Result<Option<EntityMetadata>, ProposeError> {
        let queue = self.machine.queue_config(namespace, entity)?;
        let topic = self.machine.topic_config(namespace, entity)?;
        match (queue, topic) {
            (Some(_), Some(_)) => Err(BrokerError::DanglingEntityMetadata.into()),
            (Some(queue), None) => Ok(Some(EntityMetadata::Queue(
                queue.validate().map_err(BrokerError::from)?,
            ))),
            (None, Some(topic)) => {
                for subscription in self.machine.subscriptions(namespace, entity)? {
                    let shadow = subscription
                        .entity
                        .dead_letter_queue()
                        .map_err(BrokerError::from)?;
                    if self
                        .machine
                        .topic_config(namespace, &subscription.entity)?
                        .is_some()
                        || self.machine.topic_config(namespace, &shadow)?.is_some()
                    {
                        return Err(BrokerError::DanglingEntityMetadata.into());
                    }
                }
                Ok(Some(EntityMetadata::Topic(topic)))
            }
            (None, None) => Ok(None),
        }
    }
}
