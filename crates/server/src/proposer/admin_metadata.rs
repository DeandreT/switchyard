use domain::{EntityIncarnationKind, SubscriptionDefinition, SubscriptionName};
use protocol_amqp::{EntityAdmission, EntityMetadata};

use super::entity_metadata::EntityMetadataTopology;
use super::*;

/// Native administration targets preserve literal primary entity names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminTarget {
    Primary(EntityPath),
    Subscription {
        topic: EntityPath,
        name: SubscriptionName,
    },
}

impl AdminTarget {
    pub fn canonical_entity(&self) -> Result<EntityPath, BrokerError> {
        match self {
            Self::Primary(entity) => {
                validate_primary(entity)?;
                Ok(entity.clone())
            }
            Self::Subscription { topic, name } => {
                validate_primary(topic)?;
                let name = SubscriptionName::new(name.as_str())?;
                let entity = topic.subscription(&name)?;
                entity.dead_letter_queue()?;
                Ok(entity)
            }
        }
    }
}

fn validate_primary(entity: &EntityPath) -> Result<(), BrokerError> {
    EntityPath::new(entity.as_str())?;
    if entity.is_dead_letter_queue() {
        return Err(BrokerError::DeadLetterQueueIsReserved);
    }
    if entity.is_subscription_path() {
        return Err(BrokerError::SubscriptionPathIsReserved);
    }
    Ok(())
}

impl<S: StateStore, C: Clock> LocalProposer<S, C> {
    /// Reads complete subscription rules without consulting the host clock.
    pub fn rules(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        subscription: &SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, ProposeError> {
        NamespaceName::new(namespace.as_str()).map_err(BrokerError::from)?;
        validate_primary(topic)?;
        SubscriptionName::new(subscription.as_str()).map_err(BrokerError::from)?;
        Ok(self.machine.rules(namespace, topic, subscription)?)
    }

    /// Reads native topology in one owner turn, without AMQP address restrictions.
    pub fn admin_entity_metadata(
        &self,
        namespace: &NamespaceName,
        target: &AdminTarget,
    ) -> Result<Option<EntityMetadata>, ProposeError> {
        NamespaceName::new(namespace.as_str()).map_err(BrokerError::from)?;
        target.canonical_entity()?;
        match target {
            AdminTarget::Primary(entity) => self.primary_entity_metadata(namespace, entity),
            AdminTarget::Subscription { topic, name } => Ok(self
                .subscription_entity_metadata(namespace, topic, name)?
                .map(EntityMetadata::Subscription)),
        }
    }

    /// Captures validated native topology and its identity without AMQP parsing or a clock read.
    pub fn bind_admin_entity(
        &self,
        namespace: &NamespaceName,
        target: &AdminTarget,
    ) -> Result<Option<EntityAdmission>, ProposeError> {
        NamespaceName::new(namespace.as_str()).map_err(BrokerError::from)?;
        let entity = target.canonical_entity()?;
        let topology = match target {
            AdminTarget::Primary(entity) => {
                self.primary_entity_metadata_topology(namespace, entity)?
            }
            AdminTarget::Subscription { topic, name } => {
                let (config, capacity_owners) =
                    self.subscription_entity_metadata_topology(namespace, topic, name)?;
                EntityMetadataTopology {
                    metadata: config.map(EntityMetadata::Subscription),
                    capacity_owners,
                }
            }
        };
        let Some(metadata) = topology.metadata else {
            if self
                .machine
                .entity_incarnation(namespace, &entity)?
                .is_some_and(|incarnation| !incarnation.is_retired())
            {
                return Err(BrokerError::DanglingEntityMetadata.into());
            }
            self.validate_metadata_capacity(namespace, &topology.capacity_owners, None)?;
            return Ok(None);
        };
        let kind = match metadata {
            EntityMetadata::Queue(_) => EntityIncarnationKind::Queue,
            EntityMetadata::Topic(_) => EntityIncarnationKind::Topic,
            EntityMetadata::Subscription(_) => EntityIncarnationKind::Subscription,
            EntityMetadata::DeadLetter(_) => return Err(BrokerError::InvalidEntityBinding.into()),
        };
        let binding = self
            .machine
            .bind_entity(namespace, &entity, &entity, kind)?
            .ok_or(BrokerError::DanglingEntityMetadata)?;
        self.validate_metadata_capacity(
            namespace,
            &topology.capacity_owners,
            Some((&entity, kind)),
        )?;
        Ok(Some(EntityAdmission { metadata, binding }))
    }

    /// Reads complete, validated subscription membership before a caller paginates it.
    pub fn subscriptions(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
    ) -> Result<Vec<SubscriptionDefinition>, ProposeError> {
        NamespaceName::new(namespace.as_str()).map_err(BrokerError::from)?;
        validate_primary(topic)?;
        if self.machine.queue_config(namespace, topic)?.is_some()
            && self
                .machine
                .topic_config_topology(namespace, topic)?
                .is_some()
        {
            return Err(BrokerError::DanglingEntityMetadata.into());
        }
        self.complete_subscriptions(namespace, topic)
    }
}
