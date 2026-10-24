//! Duplicate-detection history and bounded expiry.

use std::collections::HashMap;

use storage::{StateStore, WriteBatch};

use crate::{
    BrokerError, Command, CommandOutcome, EntityPath, NamespaceName, QueueConfig, Timestamp,
    TopicConfig, codec, keys,
};

use super::{StateMachine, TIMER_SCAN_LIMIT};

impl<S: StateStore> StateMachine<S> {
    /// Classifies each supplied identifier as stored (`true`) or suppressed
    /// (`false`) and stages history mutations for the stored identifiers.
    ///
    /// Callers validate the complete send before entering here. That ordering
    /// prevents an invalid later batch child from poisoning history for an
    /// earlier one.
    pub(super) fn stage_duplicate_history<'a>(
        &self,
        command: &Command,
        config: &QueueConfig,
        message_ids: impl IntoIterator<Item = &'a str>,
        batch: &mut WriteBatch,
    ) -> Result<Vec<bool>, BrokerError> {
        self.stage_history(
            command,
            config.requires_duplicate_detection,
            config.duplicate_detection_history_millis,
            message_ids,
            true,
            batch,
        )
    }

    pub(super) fn stage_topic_duplicate_history<'a>(
        &self,
        command: &Command,
        config: &TopicConfig,
        message_ids: impl IntoIterator<Item = &'a str>,
        batch: &mut WriteBatch,
    ) -> Result<Vec<bool>, BrokerError> {
        self.stage_history(
            command,
            config.requires_duplicate_detection,
            config.duplicate_detection_history_millis,
            message_ids,
            false,
            batch,
        )
    }

    fn stage_history<'a>(
        &self,
        command: &Command,
        enabled: bool,
        window_millis: u64,
        message_ids: impl IntoIterator<Item = &'a str>,
        queue_batch_semantics: bool,
        batch: &mut WriteBatch,
    ) -> Result<Vec<bool>, BrokerError> {
        let message_ids = message_ids.into_iter().collect::<Vec<_>>();
        if !enabled {
            return Ok(vec![true; message_ids.len()]);
        }

        let mut accepted_in_batch = HashMap::with_capacity(message_ids.len());
        let mut stored = Vec::with_capacity(message_ids.len());
        for message_id in message_ids {
            // An absent raw AMQP identifier acts like a broker-assigned unique
            // identifier. It must never collapse every anonymous message onto
            // the same empty-string history entry.
            if message_id.is_empty() {
                stored.push(true);
                continue;
            }
            let lookup = keys::duplicate_id(&command.namespace, &command.entity, message_id);
            let previous = if let Some(&deadline) = accepted_in_batch.get(message_id) {
                // Keep established queue same-batch semantics. Topic history is
                // strictly deadline-based, even at saturated timestamp MAX.
                if queue_batch_semantics {
                    stored.push(false);
                    continue;
                }
                Some(deadline)
            } else {
                self.read::<Timestamp>(&lookup)?
            };
            if let Some(deadline) = previous.filter(|deadline| *deadline > command.issued_at) {
                accepted_in_batch.insert(message_id, deadline);
                stored.push(false);
                continue;
            }

            // A stale generation may remain when its timer sweep has not run.
            // Retire its ordered entry before replacing the exact lookup.
            if let Some(deadline) = previous {
                batch.push_delete(keys::duplicate_expiry(
                    &command.namespace,
                    &command.entity,
                    deadline,
                    message_id,
                ));
            }
            let deadline = command.issued_at.saturating_add_millis(window_millis);
            accepted_in_batch.insert(message_id, deadline);
            batch.push_put(lookup, codec::encode(&deadline)?);
            batch.push_put(
                keys::duplicate_expiry(&command.namespace, &command.entity, deadline, message_id),
                Vec::new(),
            );
            stored.push(true);
        }
        Ok(stored)
    }

    pub(super) fn expire_duplicate_history(
        &self,
        command: &Command,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        if !self.topic_exists(command)? {
            self.load_config(command)?;
        }
        let prefix = keys::duplicate_expiry_prefix(&command.namespace, &command.entity);
        let expiring = self.store().scan_prefix(&prefix, TIMER_SCAN_LIMIT)?;

        let mut removed = 0_u32;
        for (expiry_key, _) in expiring {
            let (deadline, message_id) = keys::duplicate_expiry_parts(&prefix, &expiry_key)
                .ok_or(BrokerError::MalformedIndexKey)?;
            if deadline > command.issued_at {
                break;
            }

            let lookup = keys::duplicate_id(&command.namespace, &command.entity, message_id);
            let current: Option<Timestamp> = self.read(&lookup)?;
            // An old expiry entry must not erase a newer generation accepted
            // after the original window elapsed.
            if current == Some(deadline) {
                batch.push_delete(lookup);
            }
            batch.push_delete(expiry_key);
            removed = removed.saturating_add(1);
        }

        Ok(CommandOutcome::DuplicateHistoryExpired { removed })
    }

    /// The live or stale deadline currently indexed for an exact identifier.
    /// Primarily useful for deterministic storage and restart verification.
    pub fn duplicate_history_deadline(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        message_id: &str,
    ) -> Result<Option<Timestamp>, BrokerError> {
        self.read(&keys::duplicate_id(namespace, entity, message_id))
    }
}
