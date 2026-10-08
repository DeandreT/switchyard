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
            if record.is_some_and(|record| !record.is_retired()) {
                return Err(BrokerError::DanglingEntityMetadata);
            }
            self.reject_orphaned_topic_mode(namespace, owner)?;
            return Ok(None);
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
        self.validate_capacity_binding_profile(namespace, target, owner, kind)?;
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
            | CommandKind::CreateRuleWithAction { subscription, .. }
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
        let target = child.as_ref().unwrap_or(entity);
        if binding.kind() == EntityIncarnationKind::Queue
            && binding.target() == binding.owner()
            && matches!(
                kind,
                CommandKind::DeleteEntity {
                    target: crate::DeleteEntityTarget::Auto | crate::DeleteEntityTarget::Queue,
                }
            )
        {
            self.validate_binding_identity(binding, namespace, target)?;
            // Whole-owner deletion purges runtime values opaquely. The identity
            // fence and authoritative mode still have to be intact.
            queue_capacity::validate_owner_mode(self, namespace, target)?;
            Ok(())
        } else {
            self.validate_binding_target(binding, namespace, target)
        }
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
        self.validate_binding_identity(binding, namespace, target)?;
        self.validate_capacity_binding_profile(namespace, target, binding.owner(), binding.kind())
    }

    pub(super) fn validate_binding_identity(
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

    /// Checks capacity for a topology-checked owner, not binding identity or the whole ledger.
    /// Queues require their owner profile, topics their mandatory mode, and live
    /// complete subscriptions their parent mode plus child-sidecar absence.
    pub fn validate_capacity_binding_profile(
        &self,
        namespace: &NamespaceName,
        target: &EntityPath,
        owner: &EntityPath,
        kind: EntityIncarnationKind,
    ) -> Result<(), BrokerError> {
        match kind {
            EntityIncarnationKind::Queue => {
                queue_capacity::validate_owner_profile(self, namespace, target)?;
                Ok(())
            }
            EntityIncarnationKind::Topic => {
                queue_capacity::validate_topic_mode_profile(self, namespace, owner)
            }
            EntityIncarnationKind::Subscription => {
                self.validate_capacity_sidecar_absence(namespace, owner)?;
                self.validate_topic_mode_sidecar_absence(namespace, owner)?;
                self.validate_subscription_topic_mode_parent(namespace, owner)
            }
        }
    }

    pub(super) fn validate_subscription_topic_mode_parent(
        &self,
        namespace: &NamespaceName,
        owner: &EntityPath,
    ) -> Result<(), BrokerError> {
        let Some((parent, name)) = owner.as_str().rsplit_once(crate::SUBSCRIPTION_PATH_SEGMENT)
        else {
            return Err(BrokerError::DanglingSubscriptionMetadata);
        };
        let parent =
            EntityPath::new(parent).map_err(|_| BrokerError::DanglingSubscriptionMetadata)?;
        let name = crate::SubscriptionName::new(name)
            .map_err(|_| BrokerError::DanglingSubscriptionMetadata)?;
        if parent.is_dead_letter_queue()
            || parent.is_subscription_path()
            || parent.subscription(&name)? != *owner
        {
            return Err(BrokerError::DanglingSubscriptionMetadata);
        }
        if self
            .subscription_config_topology(namespace, &parent, &name)?
            .is_some()
        {
            queue_capacity::validate_topic_mode_profile(self, namespace, &parent)?;
        }
        Ok(())
    }

    pub(super) fn validate_topic_mode_sidecar_absence(
        &self,
        namespace: &NamespaceName,
        owner: &EntityPath,
    ) -> Result<(), BrokerError> {
        let shadow = owner.dead_letter_queue().ok();
        for entity in std::iter::once(owner).chain(shadow.as_ref()) {
            if self
                .store
                .get(&keys::topic_mode(namespace, entity))?
                .is_some()
            {
                return Err(BrokerError::TopicCapacityCorrupt);
            }
        }
        Ok(())
    }

    pub(super) fn reject_orphaned_topic_mode(
        &self,
        namespace: &NamespaceName,
        owner: &EntityPath,
    ) -> Result<(), BrokerError> {
        self.validate_topic_mode_sidecar_absence(namespace, owner)?;
        if !self
            .store
            .scan_prefix(&keys::subscription_topic_mode_prefix(namespace, owner), 1)?
            .is_empty()
        {
            return Err(BrokerError::TopicCapacityCorrupt);
        }
        Ok(())
    }

    /// Checks only Mode/Usage absence on a supplied owner and its representable DLQ.
    /// Callers establish their required topology/identity checks first.
    /// This is not a whole-ledger or metadata-absence proof.
    pub fn validate_capacity_sidecar_absence(
        &self,
        namespace: &NamespaceName,
        owner: &EntityPath,
    ) -> Result<(), BrokerError> {
        let shadow = owner.dead_letter_queue().ok();
        for entity in std::iter::once(owner).chain(shadow.as_ref()) {
            if self
                .store
                .get(&keys::queue_capacity_mode(namespace, entity))?
                .is_some()
                || self
                    .store
                    .get(&keys::queue_capacity_usage(namespace, entity))?
                    .is_some()
            {
                return Err(BrokerError::QueueCapacityCorrupt);
            }
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
    ) -> Result<EntityIncarnation, BrokerError> {
        self.reject_orphaned_capacity(namespace, owner)?;
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
        Ok(record)
    }

    pub(super) fn reject_orphaned_capacity(
        &self,
        namespace: &NamespaceName,
        owner: &EntityPath,
    ) -> Result<(), BrokerError> {
        let shadow = owner.dead_letter_queue().ok();
        for entity in std::iter::once(owner).chain(shadow.as_ref()) {
            if self
                .store
                .get(&keys::queue_capacity_mode(namespace, entity))?
                .is_some()
                || self
                    .store
                    .get(&keys::queue_capacity_usage(namespace, entity))?
                    .is_some()
                || !self
                    .store
                    .scan_prefix(&keys::message_charge_prefix(namespace, entity), 1)?
                    .is_empty()
            {
                return Err(BrokerError::QueueCapacityCorrupt);
            }
        }
        if !self
            .store
            .scan_prefix(
                &keys::subscription_capacity_mode_prefix(namespace, owner),
                1,
            )?
            .is_empty()
        {
            return Err(BrokerError::QueueCapacityCorrupt);
        }
        for (prefix, _) in keys::subscription_runtime_prefixes(namespace, owner) {
            if matches!(prefix.first(), Some(0x17 | 0x18))
                && !self.store.scan_prefix(&prefix, 1)?.is_empty()
            {
                return Err(BrokerError::QueueCapacityCorrupt);
            }
        }
        self.reject_orphaned_topic_mode(namespace, owner)?;
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
