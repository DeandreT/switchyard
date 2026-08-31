//! Scheduled message cancellation and activation.

use std::collections::HashSet;

use storage::{StateStore, WriteBatch};

use crate::{
    BrokerError, Command, CommandOutcome, MessageRecord, MessageState, SequenceNumber, Timestamp,
    codec, keys,
};

use super::{StateMachine, TIMER_SCAN_LIMIT};

impl<S: StateStore> StateMachine<S> {
    pub(super) fn cancel_scheduled(
        &self,
        command: &Command,
        sequences: &[SequenceNumber],
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        validate_cancellation(sequences)?;
        if command.entity.is_dead_letter_queue() {
            return Err(BrokerError::DeadLetterQueueIsReserved);
        }
        self.load_config(command)?;

        // Resolve the full request before adding any mutation. A stale or
        // already-activated sequence therefore cannot partially cancel the
        // messages that precede it.
        let mut scheduled = Vec::with_capacity(sequences.len());
        for &sequence in sequences {
            let record = self.load_message(command, sequence)?;
            if record.state != MessageState::Scheduled {
                return Err(BrokerError::MessageNotScheduled { sequence });
            }
            let enqueue_at = record
                .scheduled_enqueue_at
                .ok_or(BrokerError::ScheduledEnqueueTimeMissing { sequence })?;
            scheduled.push((sequence, enqueue_at));
        }

        for (sequence, enqueue_at) in scheduled {
            batch.push_delete(keys::scheduled(
                &command.namespace,
                &command.entity,
                enqueue_at,
                sequence,
            ));
            batch.push_delete(keys::message(&command.namespace, &command.entity, sequence));
        }

        Ok(CommandOutcome::ScheduledCancelled {
            cancelled: u32::try_from(sequences.len()).unwrap_or(u32::MAX),
        })
    }

    pub(super) fn activate_scheduled(
        &self,
        command: &Command,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        self.load_config(command)?;
        let namespace = &command.namespace;
        let entity = &command.entity;
        let due = self
            .store()
            .scan_prefix(&keys::scheduled_prefix(namespace, entity), TIMER_SCAN_LIMIT)?;

        let mut counters = self.load_counters(command)?;
        let mut activated = 0_u32;
        for (index_key, _) in due {
            let (enqueue_at, placeholder_sequence) =
                keys::trailing_deadline(&index_key).ok_or(BrokerError::MalformedIndexKey)?;
            if enqueue_at > command.issued_at {
                break;
            }

            let mut record = self
                .message(namespace, entity, placeholder_sequence)?
                .ok_or(BrokerError::DanglingIndexEntry {
                    sequence: placeholder_sequence,
                })?;
            if record.state != MessageState::Scheduled {
                return Err(BrokerError::MessageNotScheduled {
                    sequence: placeholder_sequence,
                });
            }
            let recorded_enqueue_at =
                record
                    .scheduled_enqueue_at
                    .ok_or(BrokerError::ScheduledEnqueueTimeMissing {
                        sequence: placeholder_sequence,
                    })?;
            if recorded_enqueue_at != enqueue_at {
                return Err(BrokerError::MalformedIndexKey);
            }

            let lifetime_millis = scheduled_lifetime(&record, enqueue_at);
            let active_sequence = SequenceNumber::new(counters.next_sequence);
            counters.next_sequence = counters.next_sequence.saturating_add(1);

            batch.push_delete(index_key);
            batch.push_delete(keys::message(namespace, entity, placeholder_sequence));

            record.sequence = active_sequence;
            record.enqueued_at = command.issued_at;
            record.expires_at =
                lifetime_millis.map(|millis| command.issued_at.saturating_add_millis(millis));
            record.state = MessageState::Ready;
            batch.push_put(
                keys::message(namespace, entity, active_sequence),
                codec::encode(&record)?,
            );
            batch.push_put(self.ready_key(command, &record), Vec::new());
            if let Some(expires_at) = record.expires_at {
                batch.push_put(
                    keys::expiry(namespace, entity, expires_at, active_sequence),
                    Vec::new(),
                );
            }
            activated += 1;
        }

        if activated > 0 {
            batch.push_put(
                keys::queue_counters(namespace, entity),
                codec::encode(&counters)?,
            );
        }
        Ok(CommandOutcome::ScheduledActivated { activated })
    }
}

fn scheduled_lifetime(record: &MessageRecord, enqueue_at: Timestamp) -> Option<u64> {
    record.expires_at.map(|expires_at| {
        expires_at
            .as_millis()
            .saturating_sub(enqueue_at.as_millis())
    })
}

fn validate_cancellation(sequences: &[SequenceNumber]) -> Result<(), BrokerError> {
    if sequences.is_empty() {
        return Err(BrokerError::EmptyScheduledCancellation);
    }
    let mut distinct = HashSet::with_capacity(sequences.len());
    for &sequence in sequences {
        if !distinct.insert(sequence) {
            return Err(BrokerError::DuplicateScheduledSequence { sequence });
        }
    }
    Ok(())
}
