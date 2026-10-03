use crate::{EntityIncarnation, EntityIncarnationKind, MAX_SEQUENCE_NUMBER};

use super::*;

pub(super) enum PrimaryMetadata {
    Absent(Option<EntityIncarnation>),
    Queue(QueueConfig),
    Topic,
}

impl<S: StateStore> StateMachine<S> {
    pub(super) fn committed_primary_metadata(
        &self,
        command: &Command,
        shadow: &EntityPath,
    ) -> Result<PrimaryMetadata, CommittedPreparationError> {
        let queue = self.committed_queue_config(&command.namespace, &command.entity)?;
        let topic = self
            .topic_config(&command.namespace, &command.entity)
            .map_err(CommittedPreparationError::business_state)?;
        let incarnation = self
            .entity_incarnation(&command.namespace, &command.entity)
            .map_err(CommittedPreparationError::business_state)?;
        let shadow_queue = self.committed_queue_config(&command.namespace, shadow)?;
        let shadow_topic = self
            .topic_config(&command.namespace, shadow)
            .map_err(CommittedPreparationError::business_state)?;

        match (queue, topic, incarnation) {
            (None, None, previous) => {
                if shadow_queue.is_some()
                    || shadow_topic.is_some()
                    || previous.is_some_and(|record| {
                        !record.is_retired() || record.kind() == EntityIncarnationKind::Subscription
                    })
                {
                    return Err(inconsistent_metadata());
                }
                self.committed_absent_metadata(command, shadow, previous)?;
                Ok(PrimaryMetadata::Absent(previous))
            }
            (Some(config), None, Some(record))
                if !record.is_retired() && record.kind() == EntityIncarnationKind::Queue =>
            {
                if shadow_queue != Some(config.dead_letter_shadow()) || shadow_topic.is_some() {
                    return Err(inconsistent_metadata());
                }
                Ok(PrimaryMetadata::Queue(config))
            }
            (None, Some(_), Some(record))
                if !record.is_retired() && record.kind() == EntityIncarnationKind::Topic =>
            {
                if shadow_queue.is_some() || shadow_topic.is_some() {
                    return Err(inconsistent_metadata());
                }
                Ok(PrimaryMetadata::Topic)
            }
            _ => Err(inconsistent_metadata()),
        }
    }

    fn committed_queue_config(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> Result<Option<QueueConfig>, CommittedPreparationError> {
        self.queue_config(namespace, entity)
            .map_err(CommittedPreparationError::business_state)?
            .map(|config| {
                config
                    .validate()
                    .map_err(CommittedPreparationError::business_state)
            })
            .transpose()
    }

    fn committed_absent_metadata(
        &self,
        command: &Command,
        shadow: &EntityPath,
        previous: Option<EntityIncarnation>,
    ) -> Result<(), CommittedPreparationError> {
        for entity in [&command.entity, shadow] {
            let counters: Option<QueueCounters> = self
                .read(&keys::queue_counters(&command.namespace, entity))
                .map_err(CommittedPreparationError::business_state)?;
            if previous.is_none() && counters.is_some() {
                return Err(inconsistent_metadata());
            }
            if let Some(counters) = counters {
                validate_counters(counters)?;
            }
            // Deletion retains counters but purges every runtime family.
            for (prefix, _) in keys::entity_runtime_prefixes(&command.namespace, entity) {
                if !self
                    .store
                    .scan_from(&prefix, &prefix, 1)
                    .map_err(CommittedPreparationError::business_state)?
                    .is_empty()
                {
                    return Err(inconsistent_metadata());
                }
            }
        }
        Ok(())
    }

    pub(super) fn committed_queue_counters(
        &self,
        command: &Command,
    ) -> Result<QueueCounters, CommittedPreparationError> {
        let stored: Option<QueueCounters> = self
            .read(&keys::queue_counters(&command.namespace, &command.entity))
            .map_err(CommittedPreparationError::business_state)?;
        let counters = stored.unwrap_or_default();
        validate_counters(counters)?;
        let prefix = keys::message_prefix(&command.namespace, &command.entity);
        let start = if stored.is_some() {
            keys::message(
                &command.namespace,
                &command.entity,
                SequenceNumber::new(counters.next_sequence),
            )
        } else {
            prefix.clone()
        };
        // Any retained row at or beyond allocation proves a regressed counter;
        // absent counters are safe only when the canonical message prefix is empty.
        if !self
            .store
            .scan_from(&prefix, &start, 1)
            .map_err(CommittedPreparationError::business_state)?
            .is_empty()
        {
            return Err(inconsistent_metadata());
        }
        Ok(counters)
    }
}

fn validate_counters(counters: QueueCounters) -> Result<(), CommittedPreparationError> {
    // MAX + 1 is the valid exhausted state written by the final allocation.
    if counters.next_sequence == 0
        || counters.next_sequence > MAX_SEQUENCE_NUMBER + 1
        || counters.next_lock_token == 0
    {
        return Err(inconsistent_metadata());
    }
    Ok(())
}

fn inconsistent_metadata() -> CommittedPreparationError {
    CommittedPreparationError::business_state(BrokerError::DanglingEntityMetadata)
}
