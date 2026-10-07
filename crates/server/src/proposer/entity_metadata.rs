use domain::{EntityIncarnationKind, SubscriptionConfig, SubscriptionDefinition, SubscriptionName};
use protocol_amqp::{Attachment, EntityMetadata, parse_attachment};

use super::*;

pub(super) enum CapacityOwner {
    Queue(EntityPath),
    Excluded(EntityPath, EntityIncarnationKind),
    AbsentPrimary(EntityPath),
}

pub(super) struct EntityMetadataTopology {
    pub(super) metadata: Option<EntityMetadata>,
    pub(super) capacity_owners: Vec<CapacityOwner>,
}

impl<S: StateStore, C: Clock> LocalProposer<S, C> {
    /// Reads validated link topology without consulting or advancing the clock.
    pub fn entity_metadata(
        &self,
        namespace: &NamespaceName,
        target: &Attachment,
    ) -> Result<Option<EntityMetadata>, ProposeError> {
        let topology = self.entity_metadata_topology(namespace, target)?;
        self.validate_metadata_capacity(namespace, &topology.capacity_owners, None)?;
        Ok(topology.metadata)
    }

    // Binding callers defer captured owner proofs until after original identity checks.
    pub(super) fn entity_metadata_topology(
        &self,
        namespace: &NamespaceName,
        target: &Attachment,
    ) -> Result<EntityMetadataTopology, ProposeError> {
        NamespaceName::new(namespace.as_str()).map_err(BrokerError::from)?;
        let entity = target
            .canonical_entity()
            .map_err(|_| BrokerError::DanglingEntityMetadata)?;
        if parse_attachment(entity.as_str()).ok().as_ref() != Some(target) {
            return Err(BrokerError::DanglingEntityMetadata.into());
        }
        match target {
            Attachment::Queue(entity) => self.primary_entity_metadata_topology(namespace, entity),
            Attachment::DeadLetter(parent) => {
                let mut topology = self.primary_entity_metadata_topology(namespace, parent)?;
                let shadow = self.machine.queue_config(namespace, &entity)?;
                if self
                    .machine
                    .topic_config_topology(namespace, &entity)?
                    .is_some()
                {
                    return Err(BrokerError::DanglingEntityMetadata.into());
                }
                topology.metadata = match topology.metadata {
                    Some(EntityMetadata::Queue(parent)) => {
                        if shadow != Some(parent.dead_letter_shadow()) {
                            return Err(BrokerError::DanglingEntityMetadata.into());
                        }
                        shadow.map(EntityMetadata::DeadLetter)
                    }
                    _ if shadow.is_some() => return Err(BrokerError::DanglingEntityMetadata.into()),
                    _ => None,
                };
                Ok(topology)
            }
            Attachment::Subscription {
                topic,
                subscription,
            }
            | Attachment::SubscriptionDeadLetter {
                topic,
                subscription,
            } => {
                let (config, capacity_owners) =
                    self.subscription_entity_metadata_topology(namespace, topic, subscription)?;
                Ok(EntityMetadataTopology {
                    metadata: config.map(|config| match target {
                        Attachment::Subscription { .. } => EntityMetadata::Subscription(config),
                        _ => EntityMetadata::DeadLetter(
                            config.to_queue_config().dead_letter_shadow(),
                        ),
                    }),
                    capacity_owners,
                })
            }
        }
    }

    pub(super) fn primary_entity_metadata(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> Result<Option<EntityMetadata>, ProposeError> {
        let topology = self.primary_entity_metadata_topology(namespace, entity)?;
        self.validate_metadata_capacity(namespace, &topology.capacity_owners, None)?;
        Ok(topology.metadata)
    }

    pub(super) fn primary_entity_metadata_topology(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> Result<EntityMetadataTopology, ProposeError> {
        let queue = self.machine.queue_config(namespace, entity)?;
        let topic = self.machine.topic_config_topology(namespace, entity)?;
        match (queue, topic) {
            (Some(_), Some(_)) => Err(BrokerError::DanglingEntityMetadata.into()),
            (Some(queue), None) => {
                queue.validate().map_err(BrokerError::from)?;
                Ok(EntityMetadataTopology {
                    metadata: Some(EntityMetadata::Queue(queue)),
                    capacity_owners: vec![CapacityOwner::Queue(entity.clone())],
                })
            }
            (None, Some(topic)) => {
                let (_, capacity_owners) =
                    self.complete_subscriptions_topology(namespace, entity)?;
                Ok(EntityMetadataTopology {
                    metadata: Some(EntityMetadata::Topic(topic)),
                    capacity_owners,
                })
            }
            (None, None) => Ok(EntityMetadataTopology {
                metadata: None,
                capacity_owners: vec![CapacityOwner::AbsentPrimary(entity.clone())],
            }),
        }
    }

    pub(super) fn subscription_entity_metadata(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        name: &SubscriptionName,
    ) -> Result<Option<SubscriptionConfig>, ProposeError> {
        let (config, capacity_owners) =
            self.subscription_entity_metadata_topology(namespace, topic, name)?;
        self.validate_metadata_capacity(namespace, &capacity_owners, None)?;
        Ok(config)
    }

    pub(super) fn subscription_entity_metadata_topology(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        name: &SubscriptionName,
    ) -> Result<(Option<SubscriptionConfig>, Vec<CapacityOwner>), ProposeError> {
        let backing = topic.subscription(name).map_err(BrokerError::from)?;
        let shadow = backing.dead_letter_queue().map_err(BrokerError::from)?;
        if self.machine.queue_config(namespace, topic)?.is_some()
            || self
                .machine
                .topic_config_topology(namespace, &backing)?
                .is_some()
            || self
                .machine
                .topic_config_topology(namespace, &shadow)?
                .is_some()
        {
            return Err(BrokerError::DanglingEntityMetadata.into());
        }
        let config = self
            .machine
            .subscription_config_topology(namespace, topic, name)?;
        let mut capacity_owners = vec![CapacityOwner::Excluded(
            backing,
            EntityIncarnationKind::Subscription,
        )];
        if config.is_some() {
            capacity_owners.push(CapacityOwner::Excluded(
                topic.clone(),
                EntityIncarnationKind::Topic,
            ));
        }
        Ok((config, capacity_owners))
    }

    pub(super) fn complete_subscriptions(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
    ) -> Result<Vec<SubscriptionDefinition>, ProposeError> {
        let (subscriptions, capacity_owners) =
            self.complete_subscriptions_topology(namespace, topic)?;
        self.validate_metadata_capacity(namespace, &capacity_owners, None)?;
        Ok(subscriptions)
    }

    pub(super) fn complete_subscriptions_topology(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
    ) -> Result<(Vec<SubscriptionDefinition>, Vec<CapacityOwner>), ProposeError> {
        let subscriptions = self.machine.subscriptions_topology(namespace, topic)?;
        let mut capacity_owners = Vec::with_capacity(subscriptions.len() + 1);
        capacity_owners.push(CapacityOwner::Excluded(
            topic.clone(),
            EntityIncarnationKind::Topic,
        ));
        for subscription in &subscriptions {
            let shadow = subscription
                .entity
                .dead_letter_queue()
                .map_err(BrokerError::from)?;
            if self
                .machine
                .topic_config_topology(namespace, &subscription.entity)?
                .is_some()
                || self
                    .machine
                    .topic_config_topology(namespace, &shadow)?
                    .is_some()
            {
                return Err(BrokerError::DanglingEntityMetadata.into());
            }
            capacity_owners.push(CapacityOwner::Excluded(
                subscription.entity.clone(),
                EntityIncarnationKind::Subscription,
            ));
        }
        Ok((subscriptions, capacity_owners))
    }

    pub(super) fn validate_metadata_capacity(
        &self,
        namespace: &NamespaceName,
        owners: &[CapacityOwner],
        already_bound: Option<(&EntityPath, EntityIncarnationKind)>,
    ) -> Result<(), ProposeError> {
        for owner in owners {
            if matches!((owner, already_bound),
                (CapacityOwner::Queue(owner), Some((bound, EntityIncarnationKind::Queue))) if owner == bound)
                || matches!((owner, already_bound),
                    (CapacityOwner::Excluded(owner, kind), Some((bound, bound_kind))) if owner == bound && *kind == bound_kind)
            {
                continue;
            }
            match owner {
                CapacityOwner::Queue(owner) => {
                    self.machine
                        .describe_queue_capacity(namespace, owner)?
                        .ok_or(BrokerError::QueueCapacityCorrupt)?;
                }
                CapacityOwner::Excluded(owner, kind) => {
                    self.machine
                        .validate_capacity_binding_profile(namespace, owner, owner, *kind)?;
                }
                CapacityOwner::AbsentPrimary(owner) => {
                    self.machine.describe_queue_capacity(namespace, owner)?;
                    self.machine
                        .validate_capacity_sidecar_absence(namespace, owner)?;
                }
            }
        }
        Ok(())
    }
}
