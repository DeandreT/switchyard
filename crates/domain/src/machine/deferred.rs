//! Deferring locked messages and retrieving them explicitly by sequence.

use std::collections::HashSet;

use storage::{StateStore, WriteBatch};

use crate::{
    BrokerError, Command, CommandOutcome, Delivery, DeliveryLock, DeliveryOrigin, LockToken,
    MessageContentError, MessageEnvelope, MessageRecord, MessageRecordFormat, MessageState,
    QueueConfig, ReceiveMode, SequenceNumber, SessionHold, codec, keys,
};

use super::{
    MAX_DEFERRED_RECEIVE_BATCH, StateMachine, patch::ContentRewrite, require_session_agreement,
};

impl<S: StateStore> StateMachine<S> {
    pub(super) fn defer(
        &self,
        command: &Command,
        sequence: SequenceNumber,
        lock_token: LockToken,
        rewrite: ContentRewrite<'_>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let config = self.load_config(command)?;
        let (mut record, locked_until, _) = self.held_lock(command, sequence, lock_token)?;
        rewrite.apply(&config, &mut record)?;

        let namespace = &command.namespace;
        let entity = &command.entity;
        record.state = MessageState::Deferred;
        batch.push_delete(keys::lock(namespace, entity, locked_until, sequence));
        // A deferred message retains its absolute expiry in the record, but it
        // is checked lazily only when explicitly retrieved.
        if let Some(expires_at) = record.expires_at {
            batch.push_delete(keys::expiry(namespace, entity, expires_at, sequence));
        }
        batch.push_put(
            keys::message(namespace, entity, sequence),
            codec::message::encode(&record)?,
        );
        batch.push_put(keys::deferred(namespace, entity, sequence), Vec::new());
        Ok(CommandOutcome::Deferred)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn receive_deferred(
        &self,
        command: &Command,
        sequences: &[SequenceNumber],
        mode: ReceiveMode,
        lock_duration_millis: Option<u64>,
        session: Option<&SessionHold>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        validate_sequences(sequences)?;
        let config = self.load_config(command)?;
        require_session_agreement(&config, session.is_some())?;
        if let Some(hold) = session {
            self.held_session(command, hold)?;
        }

        // Resolve and validate the complete request before adding a mutation.
        // One bad sequence therefore cannot partially consume or lock the
        // messages before it.
        let mut records = Vec::with_capacity(sequences.len());
        for &sequence in sequences {
            let record = self.load_message(command, sequence)?;
            if record.state != MessageState::Deferred {
                return Err(BrokerError::MessageNotDeferred { sequence });
            }
            let expected_session = session.map(|hold| &hold.session_id);
            if record.session_id.as_ref() != expected_session {
                return Err(BrokerError::DeferredMessageSessionMismatch { sequence });
            }
            records.push(record);
        }

        let namespace = &command.namespace;
        let entity = &command.entity;
        let mut counters = self.load_counters(command)?;
        let mut used_lock_token = false;
        let mut deliveries = Vec::with_capacity(records.len());

        for mut record in records {
            let sequence = record.sequence;
            if record.is_expired_at(command.issued_at) {
                self.expire_message(
                    command,
                    &config,
                    record,
                    "the deferred message exceeded its time to live",
                    batch,
                )?;
                continue;
            }

            record.delivery_count = record.delivery_count.saturating_add(1);
            let delivery_count = record.delivery_count;
            let lock = match mode {
                ReceiveMode::PeekLock => {
                    let token = LockToken::new(counters.next_lock_token);
                    counters.next_lock_token = counters
                        .next_lock_token
                        .checked_add(1)
                        .ok_or(BrokerError::LockTokenExhausted)?;
                    used_lock_token = true;
                    let lock_duration_millis =
                        lock_duration_millis.unwrap_or(config.lock_duration_millis);
                    let locked_until = command
                        .issued_at
                        .saturating_add_millis(lock_duration_millis);
                    record.state = MessageState::Locked {
                        token,
                        locked_until,
                        origin: DeliveryOrigin::Deferred,
                    };
                    batch.push_delete(keys::deferred(namespace, entity, sequence));
                    batch.push_put(
                        keys::message(namespace, entity, sequence),
                        codec::message::encode(&record)?,
                    );
                    batch.push_put(
                        keys::lock(namespace, entity, locked_until, sequence),
                        Vec::new(),
                    );
                    Some(DeliveryLock {
                        token,
                        locked_until,
                        lock_duration_millis,
                    })
                }
                ReceiveMode::ReceiveAndDelete => {
                    batch.push_delete(keys::deferred(namespace, entity, sequence));
                    batch.push_delete(keys::message(namespace, entity, sequence));
                    if let Some(expires_at) = record.expires_at {
                        batch.push_delete(keys::expiry(namespace, entity, expires_at, sequence));
                    }
                    None
                }
            };

            deliveries.push(Delivery {
                sequence,
                message_id: record.message_id,
                body: record.body,
                enqueued_at: record.enqueued_at,
                scheduled_enqueue_at: record.scheduled_enqueue_at,
                expires_at: record.expires_at,
                delivery_count,
                origin: DeliveryOrigin::Deferred,
                lock,
                session_id: record.session_id,
                dead_letter: record.dead_letter,
                envelope: record.envelope,
            });
        }

        if used_lock_token {
            batch.push_put(
                keys::queue_counters(namespace, entity),
                codec::encode(&counters)?,
            );
        }
        Ok(CommandOutcome::DeferredReceived(deliveries))
    }

    /// Restores an unsuccessfully processed delivery to the index it came
    /// from. Callers handle TTL and maximum-delivery dead-lettering first.
    pub(super) fn return_unsettled(
        &self,
        command: &Command,
        mut record: MessageRecord,
        locked_until: crate::Timestamp,
        origin: DeliveryOrigin,
        batch: &mut WriteBatch,
    ) -> Result<(), BrokerError> {
        let namespace = &command.namespace;
        let entity = &command.entity;
        let sequence = record.sequence;
        batch.push_delete(keys::lock(namespace, entity, locked_until, sequence));
        match origin {
            DeliveryOrigin::Ready => {
                record.state = MessageState::Ready;
                batch.push_put(self.ready_key(command, &record), Vec::new());
                if let Some(expires_at) = record.expires_at {
                    batch.push_put(
                        keys::expiry(namespace, entity, expires_at, sequence),
                        Vec::new(),
                    );
                }
            }
            DeliveryOrigin::Deferred => {
                record.state = MessageState::Deferred;
                batch.push_put(keys::deferred(namespace, entity, sequence), Vec::new());
            }
            // Scheduled placeholders never enter a lock. Treating this as a
            // ready return keeps a malformed persisted origin from recreating
            // a scheduled index after activation.
            DeliveryOrigin::Scheduled => {
                record.state = MessageState::Ready;
                batch.push_put(self.ready_key(command, &record), Vec::new());
                if let Some(expires_at) = record.expires_at {
                    batch.push_put(
                        keys::expiry(namespace, entity, expires_at, sequence),
                        Vec::new(),
                    );
                }
            }
        }
        batch.push_put(
            keys::message(namespace, entity, sequence),
            codec::message::encode(&record)?,
        );
        Ok(())
    }
}

pub(super) fn replace_envelope(
    config: &QueueConfig,
    record: &mut MessageRecord,
    replacement: Option<&MessageEnvelope>,
) -> Result<(), BrokerError> {
    let Some(envelope) = replacement else {
        return Ok(());
    };
    // A whole replacement carries no overlay, so accepting one over a patched
    // copy would silently drop the patch. The AMQP edge sends property changes
    // as V4 patches and never builds a replacement; a legacy V1 replacement
    // that reaches a patched copy is refused.
    if record
        .envelope
        .as_ref()
        .is_some_and(|current| !current.overlay().is_empty() || !current.removals().is_empty())
    {
        return Err(MessageContentError::ReplacementWouldDropOverlay.into());
    }
    if envelope.len() > config.max_message_bytes {
        return Err(BrokerError::MessageTooLarge {
            body_bytes: envelope.len(),
            maximum_bytes: config.max_message_bytes,
        });
    }
    record.envelope = Some(envelope.clone());
    if record.format.uses_text_ids() {
        // A legacy whole-envelope replacement may reach a current record
        // through the domain API. Derive V3 projection text from its typed
        // content, never accept a caller's legacy system-ID projection as V3.
        let input = crate::MessageContent::AmqpTextIds(envelope.as_bytes().to_vec()).derive()?;
        record.envelope = input.envelope;
    }
    // A V2 record stores exactly one content form, so the replacement becomes
    // its AMQP content. Only a copy without an overlay reaches this point, and
    // the replacement carries none either.
    if record.format != MessageRecordFormat::V1 {
        record.body.clear();
    }
    Ok(())
}

fn validate_sequences(sequences: &[SequenceNumber]) -> Result<(), BrokerError> {
    if sequences.is_empty() {
        return Err(BrokerError::EmptyDeferredReceive);
    }
    if sequences.len() > MAX_DEFERRED_RECEIVE_BATCH {
        return Err(BrokerError::DeferredReceiveBatchTooLarge {
            count: sequences.len(),
            maximum: MAX_DEFERRED_RECEIVE_BATCH,
        });
    }

    let mut distinct = HashSet::with_capacity(sequences.len());
    for &sequence in sequences {
        if !distinct.insert(sequence) {
            return Err(BrokerError::DuplicateDeferredSequence { sequence });
        }
    }
    Ok(())
}
