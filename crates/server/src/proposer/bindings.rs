use domain::{EntityBinding, EntityIncarnationKind, FencedCommand, SubscriptionName};
use protocol_amqp::EntityAdmission;

use super::*;

impl<S: StateStore, C: Clock> LocalProposer<S, C> {
    /// Reads topology and its persistent identity in one clock-free owner turn.
    pub fn bind_entity(
        &self,
        namespace: &NamespaceName,
        target: &protocol_amqp::Attachment,
    ) -> Result<Option<EntityAdmission>, ProposeError> {
        let metadata = self.entity_metadata(namespace, target)?;
        let entity = target
            .canonical_entity()
            .map_err(|_| BrokerError::InvalidEntityBinding)?;
        let owner = match target {
            protocol_amqp::Attachment::Queue(owner)
            | protocol_amqp::Attachment::DeadLetter(owner) => owner.clone(),
            protocol_amqp::Attachment::Subscription {
                topic,
                subscription,
            }
            | protocol_amqp::Attachment::SubscriptionDeadLetter {
                topic,
                subscription,
            } => topic
                .subscription(subscription)
                .map_err(BrokerError::from)?,
        };
        let Some(metadata) = metadata else {
            // A live topic owns no DLQ. Its absent shadow is not an orphaned
            // incarnation, but the owner's identity must still be consistent.
            if matches!(target, protocol_amqp::Attachment::DeadLetter(_))
                && self.machine.topic_config(namespace, &owner)?.is_some()
            {
                self.machine
                    .bind_entity(namespace, &owner, &owner, EntityIncarnationKind::Topic)?
                    .ok_or(BrokerError::DanglingEntityMetadata)?;
                return Ok(None);
            }
            if self
                .machine
                .entity_incarnation(namespace, &owner)?
                .is_some_and(|incarnation| !incarnation.is_retired())
            {
                return Err(BrokerError::DanglingEntityMetadata.into());
            }
            return Ok(None);
        };
        let kind = match metadata {
            protocol_amqp::EntityMetadata::Queue(_) => EntityIncarnationKind::Queue,
            protocol_amqp::EntityMetadata::Topic(_) => EntityIncarnationKind::Topic,
            protocol_amqp::EntityMetadata::Subscription(_) => EntityIncarnationKind::Subscription,
            protocol_amqp::EntityMetadata::DeadLetter(_) => match target {
                protocol_amqp::Attachment::DeadLetter(_) => EntityIncarnationKind::Queue,
                protocol_amqp::Attachment::SubscriptionDeadLetter { .. } => {
                    EntityIncarnationKind::Subscription
                }
                _ => return Err(BrokerError::InvalidEntityBinding.into()),
            },
        };
        let binding = self
            .machine
            .bind_entity(namespace, &entity, &owner, kind)?
            .ok_or(BrokerError::DanglingEntityMetadata)?;
        Ok(Some(EntityAdmission { metadata, binding }))
    }

    /// Refuses stale identities before consulting the host clock, then records
    /// the same guard in the deterministic instruction used by replicas.
    pub fn propose_fenced_with_effects(
        &self,
        binding: &EntityBinding,
        entity: &EntityPath,
        kind: CommandKind,
    ) -> Result<CommandApplication, ProposeError> {
        self.machine
            .validate_fenced_intent(binding, binding.namespace(), entity, &kind)?;
        let issued_at = self.stamp()?;
        let command = Command::new(binding.namespace().clone(), entity.clone(), issued_at, kind);
        Ok(self.machine.apply_fenced_with_effects(&FencedCommand {
            binding: binding.clone(),
            command,
        })?)
    }

    /// Guards a pure rule read against the subscription identity, not merely
    /// its parent topic's still-live identity.
    pub fn rules_fenced(
        &self,
        binding: &EntityBinding,
        topic: &EntityPath,
        subscription: &SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, ProposeError> {
        Ok(self
            .machine
            .rules_fenced(binding, binding.namespace(), topic, subscription)?)
    }
}
