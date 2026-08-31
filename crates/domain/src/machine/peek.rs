//! Read-only message browsing in primary-record sequence order.

use storage::StateStore;

use crate::{
    BrokerError, Command, CommandOutcome, Delivery, DeliveryOrigin, MessageRecord, MessageState,
    SequenceNumber, SessionHold, keys,
};

use super::StateMachine;

/// Largest page returned by the Service Bus peek operation.
pub const MAX_PEEK_BATCH: u32 = 250;

/// Primary records fetched in one storage scan while filling a peek page.
pub const MAX_PEEK_SCAN: usize = 256;

impl<S: StateStore> StateMachine<S> {
    pub(super) fn peek(
        &self,
        command: &Command,
        from_sequence: SequenceNumber,
        max_messages: u32,
        session: Option<&SessionHold>,
    ) -> Result<CommandOutcome, BrokerError> {
        validate_page_size(max_messages)?;
        let config = self.load_config(command)?;
        let session_id = match session {
            Some(_) if !config.requires_session => {
                return Err(BrokerError::SessionNotSupported);
            }
            Some(hold) => {
                self.held_session(command, hold)?;
                Some(&hold.session_id)
            }
            // A regular receiver may browse across every session without
            // acquiring one. Session receivers supply their hold above.
            None => None,
        };

        let namespace = &command.namespace;
        let entity = &command.entity;
        let prefix = keys::message_prefix(namespace, entity);
        let mut start = keys::message(namespace, entity, from_sequence);
        let page_size = usize::try_from(max_messages.min(MAX_PEEK_BATCH))
            .expect("the peek page limit fits in usize");
        let mut deliveries = Vec::with_capacity(page_size);

        loop {
            let records = self
                .store()
                .scan_from(&prefix, &start, MAX_PEEK_SCAN)
                .map_err(BrokerError::from)?;
            if records.is_empty() {
                break;
            }
            let exhausted = records.len() < MAX_PEEK_SCAN;
            let mut last_sequence = None;

            for (key, encoded) in records {
                let sequence =
                    keys::trailing_sequence(&key).ok_or(BrokerError::MalformedIndexKey)?;
                last_sequence = Some(sequence);
                let record = decode_record(&encoded)?;
                if session_id.is_some_and(|expected| record.session_id.as_ref() != Some(expected)) {
                    continue;
                }
                // Ready messages cease to be browseable once their TTL has
                // elapsed, even if the asynchronous expiry sweep has not run.
                // Locks and deferred state retain the protection already
                // defined by their lifecycle and remain visible here.
                if record.state == MessageState::Ready && record.is_expired_at(command.issued_at) {
                    continue;
                }

                deliveries.push(peeked_delivery(record));
                if deliveries.len() == page_size {
                    return Ok(CommandOutcome::Peeked(deliveries));
                }
            }

            if exhausted {
                break;
            }
            let Some(last_sequence) = last_sequence else {
                break;
            };
            let Some(next_sequence) = last_sequence.as_u64().checked_add(1) else {
                break;
            };
            start = keys::message(namespace, entity, SequenceNumber::new(next_sequence));
        }
        Ok(CommandOutcome::Peeked(deliveries))
    }
}

fn validate_page_size(max_messages: u32) -> Result<(), BrokerError> {
    if max_messages == 0 {
        return Err(BrokerError::EmptyPeek);
    }
    Ok(())
}

fn decode_record(encoded: &[u8]) -> Result<MessageRecord, BrokerError> {
    MessageRecord::decode(encoded).map_err(BrokerError::from)
}

fn peeked_delivery(record: MessageRecord) -> Delivery {
    let origin = match record.state {
        MessageState::Ready => DeliveryOrigin::Ready,
        MessageState::Deferred => DeliveryOrigin::Deferred,
        MessageState::Scheduled => DeliveryOrigin::Scheduled,
        MessageState::Locked { origin, .. } => origin,
    };
    Delivery {
        sequence: record.sequence,
        message_id: record.message_id,
        body: record.body,
        enqueued_at: record.enqueued_at,
        scheduled_enqueue_at: record.scheduled_enqueue_at,
        expires_at: record.expires_at,
        delivery_count: record.delivery_count,
        origin,
        lock: None,
        session_id: record.session_id,
        dead_letter: record.dead_letter,
        envelope: record.envelope,
    }
}
