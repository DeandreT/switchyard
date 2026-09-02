//! Duplicate-detection history and bounded expiry.

use std::collections::HashSet;

use storage::{StateStore, WriteBatch};

use crate::{
    BrokerError, Command, CommandOutcome, EntityPath, NamespaceName, QueueConfig, Timestamp, codec,
    keys,
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
        let message_ids = message_ids.into_iter().collect::<Vec<_>>();
        if !config.requires_duplicate_detection {
            return Ok(vec![true; message_ids.len()]);
        }

        let mut accepted_in_batch = HashSet::with_capacity(message_ids.len());
        let mut stored = Vec::with_capacity(message_ids.len());
        for message_id in message_ids {
            // An absent raw AMQP identifier acts like a broker-assigned unique
            // identifier. It must never collapse every anonymous message onto
            // the same empty-string history entry.
            if message_id.is_empty() {
                stored.push(true);
                continue;
            }
            if !accepted_in_batch.insert(message_id) {
                stored.push(false);
                continue;
            }

            let lookup = keys::duplicate_id(&command.namespace, &command.entity, message_id);
            let previous: Option<Timestamp> = self.read(&lookup)?;
            if previous.is_some_and(|deadline| deadline > command.issued_at) {
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
            let deadline = command
                .issued_at
                .saturating_add_millis(config.duplicate_detection_history_millis);
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
        self.load_config(command)?;
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
