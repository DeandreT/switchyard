use crate::{EntityBinding, EntityIncarnation, EntityIncarnationKind, FencedCommand};

use super::*;

impl<S: StateStore> StateMachine<S> {
    /// Reads a retained identity without stamping or mutating a command.
    pub fn entity_incarnation(
        &self,
        namespace: &NamespaceName,
        owner: &EntityPath,
    ) -> Result<Option<EntityIncarnation>, BrokerError> {
        if namespace.as_str().len() > crate::MAX_NAMESPACE_NAME_BYTES
            || owner.as_str().len() > crate::MAX_ENTITY_PATH_BYTES
            || NamespaceName::new(namespace.as_str()).is_err()
            || EntityPath::new(owner.as_str()).is_err()
        {
            return Err(BrokerError::InvalidEntityBinding);
        }
        let record = self.read::<EntityIncarnation>(&keys::entity_incarnation(namespace, owner))?;
        if let Some(record) = record {
            record.validate()?;
            EntityBinding::new(
                namespace.clone(),
                owner.clone(),
                owner.clone(),
                record.kind(),
                record.generation(),
            )
            .map_err(|_| BrokerError::DanglingEntityMetadata)?;
        }
        Ok(record)
    }

    /// Binds minimal live metadata; the edge separately validates complete topology.
    pub fn bind_entity(
        &self,
        namespace: &NamespaceName,
        target: &EntityPath,
        owner: &EntityPath,
        kind: EntityIncarnationKind,
    ) -> Result<Option<EntityBinding>, BrokerError> {
        if namespace.as_str().len() > crate::MAX_NAMESPACE_NAME_BYTES
            || target.as_str().len() > crate::MAX_ENTITY_PATH_BYTES
            || owner.as_str().len() > crate::MAX_ENTITY_PATH_BYTES
        {
            return Err(BrokerError::InvalidEntityBinding);
        }
        EntityBinding::new(namespace.clone(), target.clone(), owner.clone(), kind, 1)?;
        let queue = self.store.get(&keys::queue_config(namespace, owner))?;
        let topic = self.store.get(&keys::topic_config(namespace, owner))?;
        let record = self.entity_incarnation(namespace, owner)?;
        if queue.is_some() && topic.is_some() {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        if queue.is_none() && topic.is_none() {
            return if record.is_some_and(|record| !record.is_retired()) {
                Err(BrokerError::DanglingEntityMetadata)
            } else {
                Ok(None)
            };
        }
        let config = match kind {
            EntityIncarnationKind::Queue | EntityIncarnationKind::Subscription => {
                let Some(bytes) = queue else {
                    return Err(if kind == EntityIncarnationKind::Subscription {
                        BrokerError::DanglingEntityMetadata
                    } else {
                        BrokerError::InvalidEntityBinding
                    });
                };
                Some(QueueConfig::decode(&bytes)?.validate()?)
            }
            EntityIncarnationKind::Topic => {
                let bytes = topic.ok_or(BrokerError::InvalidEntityBinding)?;
                codec::decode::<crate::TopicConfig>(&bytes)?
                    .validate()
                    .map_err(BrokerError::TopicConfig)?;
                None
            }
        };
        let record = record.ok_or(BrokerError::DanglingEntityMetadata)?;
        if record.is_retired() || record.kind() != kind {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        if target != owner {
            let shadow = self.queue_config(namespace, target)?;
            if self
                .store
                .get(&keys::topic_config(namespace, target))?
                .is_some()
                || shadow != config.map(QueueConfig::dead_letter_shadow)
            {
                return Err(BrokerError::DanglingEntityMetadata);
            }
        }
        Ok(Some(EntityBinding::new(
            namespace.clone(),
            target.clone(),
            owner.clone(),
            kind,
            record.generation(),
        )?))
    }

    /// Checks identity before the proposer consults its clock.
    pub fn validate_fenced_intent(
        &self,
        binding: &EntityBinding,
        namespace: &NamespaceName,
        entity: &EntityPath,
        kind: &CommandKind,
    ) -> Result<(), BrokerError> {
        let child = match kind {
            CommandKind::CreateRule { subscription, .. }
            | CommandKind::DeleteRule { subscription, .. } => {
                Some(entity.subscription(subscription)?)
            }
            CommandKind::CreateSubscription { name, .. }
            | CommandKind::UpdateSubscription { name, .. }
            | CommandKind::DeleteEntity {
                target: crate::DeleteEntityTarget::Subscription { name },
            } => Some(entity.subscription(name)?),
            _ => None,
        };
        self.validate_binding_target(binding, namespace, child.as_ref().unwrap_or(entity))
    }

    pub fn apply_fenced(&self, command: &FencedCommand) -> Result<CommandOutcome, BrokerError> {
        Ok(self.apply_fenced_with_effects(command)?.outcome)
    }

    /// The replay guard runs before ordinary clock regression and all mutation.
    pub fn apply_fenced_with_effects(
        &self,
        command: &FencedCommand,
    ) -> Result<CommandApplication, BrokerError> {
        self.validate_fenced_intent(
            &command.binding,
            &command.command.namespace,
            &command.command.entity,
            &command.command.kind,
        )?;
        self.apply_with_effects(&command.command)
    }

    pub fn rules_fenced(
        &self,
        binding: &EntityBinding,
        namespace: &NamespaceName,
        topic: &EntityPath,
        subscription: &crate::SubscriptionName,
    ) -> Result<Vec<crate::RuleDefinition>, BrokerError> {
        let target = topic.subscription(subscription)?;
        self.validate_binding_target(binding, namespace, &target)?;
        self.rules(namespace, topic, subscription)
    }

    pub(super) fn validate_binding_target(
        &self,
        binding: &EntityBinding,
        namespace: &NamespaceName,
        target: &EntityPath,
    ) -> Result<(), BrokerError> {
        binding.validate()?;
        if binding.namespace() != namespace || binding.target() != target {
            return Err(BrokerError::InvalidEntityBinding);
        }
        let Some(record) = self.entity_incarnation(namespace, binding.owner())? else {
            if self
                .store
                .get(&keys::queue_config(namespace, binding.owner()))?
                .is_some()
                || self
                    .store
                    .get(&keys::topic_config(namespace, binding.owner()))?
                    .is_some()
            {
                return Err(BrokerError::DanglingEntityMetadata);
            }
            return Err(BrokerError::EntityBindingStale);
        };
        if record.is_retired()
            || record.kind() != binding.kind()
            || record.generation() != binding.generation()
        {
            self.validate_incarnation_metadata(namespace, binding.owner(), record)?;
            return Err(BrokerError::EntityBindingStale);
        }
        Ok(())
    }

    fn validate_incarnation_metadata(
        &self,
        namespace: &NamespaceName,
        owner: &EntityPath,
        record: EntityIncarnation,
    ) -> Result<(), BrokerError> {
        let queue = self.store.get(&keys::queue_config(namespace, owner))?;
        let topic = self.store.get(&keys::topic_config(namespace, owner))?;
        if record.is_retired() {
            return if queue.is_none() && topic.is_none() {
                Ok(())
            } else {
                Err(BrokerError::DanglingEntityMetadata)
            };
        }
        match (record.kind(), queue, topic) {
            (
                EntityIncarnationKind::Queue | EntityIncarnationKind::Subscription,
                Some(bytes),
                None,
            ) => {
                QueueConfig::decode(&bytes)?.validate()?;
            }
            (EntityIncarnationKind::Topic, None, Some(bytes)) => {
                codec::decode::<crate::TopicConfig>(&bytes)?
                    .validate()
                    .map_err(BrokerError::TopicConfig)?;
            }
            _ => return Err(BrokerError::DanglingEntityMetadata),
        }
        Ok(())
    }

    pub(super) fn stage_create_incarnation(
        &self,
        namespace: &NamespaceName,
        owner: &EntityPath,
        kind: EntityIncarnationKind,
        batch: &mut WriteBatch,
    ) -> Result<(), BrokerError> {
        let previous = self.entity_incarnation(namespace, owner)?;
        if let Some(previous) = previous
            && (!previous.is_retired()
                || ((previous.kind() == EntityIncarnationKind::Subscription)
                    != (kind == EntityIncarnationKind::Subscription)))
        {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        let generation = previous.map_or(Ok(1), |previous| {
            previous
                .generation()
                .checked_add(1)
                .ok_or(BrokerError::EntityIncarnationExhausted)
        })?;
        let record = EntityIncarnation::new(generation, kind, false)?;
        batch.push_put(
            keys::entity_incarnation(namespace, owner),
            codec::encode(&record)?,
        );
        Ok(())
    }

    pub(super) fn require_live_incarnation(
        &self,
        namespace: &NamespaceName,
        owner: &EntityPath,
        kind: EntityIncarnationKind,
    ) -> Result<EntityIncarnation, BrokerError> {
        let record = self
            .entity_incarnation(namespace, owner)?
            .ok_or(BrokerError::DanglingEntityMetadata)?;
        if record.is_retired() || record.kind() != kind {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests;
