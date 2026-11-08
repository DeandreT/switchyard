use std::collections::BTreeSet;

use storage::{Key, Value};

use super::{AccountingError, CapacityOwner, encoded_row_bytes};
use crate::{
    DeadLetterInfo, DeadLetterReason, DeliveryOrigin, EntityPath, LockToken, MessageRecord,
    MessageState, SequenceNumber, Timestamp, codec, keys,
};

/// Required explicit coordination with the capacity-owned message fence.
/// Message values are canonical unframed V1/V2/V3. Key framing is selected
/// explicitly; a future value-framing allowance covers every header/trailer
/// byte and still requires its own codec/proof before framed values are used.
/// This is not journal, ledger or filesystem overhead.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapacityFramingBudget {
    /// Proves bounds for today's encoding only; NOT permission to activate.
    CurrentUnframedEncoding,
    /// Maximum additional bytes in any automatic state and either queue path.
    AdditionalMessageValueBytes(u64),
    /// Capacity keyspace V1: tag 13 before EACH canonical key; no value frame.
    /// Inputs must already use that layout. V4 message values still refuse.
    IsolatedKeyspaceV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MessageReservation {
    pub retained_bytes: u64,
    pub reserved_growth_bytes: u64,
    pub ceiling_bytes: u64,
}

/// Reserve from the exact message plus its complete current companion indexes.
/// Canonical content/projection/overlay bytes are preserved. No application
/// rewrite is covered: after any external content, property or DLQ metadata
/// change the caller must recompute and check the new committed total first.
/// Session/duplicate profiles remain the caller's unsupported-policy check;
/// a session-bearing record is refused here.
pub fn reserve_message(
    owner: &CapacityOwner,
    rows: &[(Key, Value)],
    framing: CapacityFramingBudget,
) -> Result<MessageReservation, AccountingError> {
    if framing == CapacityFramingBudget::IsolatedKeyspaceV1 {
        let original = rows
            .iter()
            .map(|(key, value)| {
                Ok((
                    crate::capacity_fence::original_key(key)?.to_vec(),
                    value.clone(),
                ))
            })
            .collect::<Result<Vec<_>, AccountingError>>()?;
        return reserve_original(
            owner,
            &original,
            framing,
            crate::capacity_fence::DATA_KEY_OVERHEAD,
        );
    }
    reserve_original(owner, rows, framing, 0)
}

fn reserve_original(
    owner: &CapacityOwner,
    rows: &[(Key, Value)],
    framing: CapacityFramingBudget,
    key_overhead: u64,
) -> Result<MessageReservation, AccountingError> {
    let mut primary = None;
    let mut actual = 0_u64;
    let mut supplied = BTreeSet::new();
    for (key, value) in rows {
        if !supplied.insert(key.clone()) {
            return Err(AccountingError::DuplicateBeforeImage);
        }
        let parsed = keys::state_parts(key).ok_or(AccountingError::InvalidKey)?;
        let (namespace, entity) = parsed.scope();
        if !owner.owns(namespace, entity) {
            return Err(AccountingError::UnownedKey);
        }
        actual = actual
            .checked_add(encoded_row_bytes(key, value)?)
            .and_then(|bytes| bytes.checked_add(key_overhead))
            .ok_or(AccountingError::Overflow)?;
        if let keys::StateKey::Message(_, _, sequence) = &parsed {
            if primary.is_some() {
                return Err(AccountingError::InvalidMessage);
            }
            // This proof is explicitly V1..V3 even if the general decoder
            // later learns V4 deletion overlays or capacity-owned framing.
            if !matches!(value.first(), Some(1..=3)) {
                return Err(AccountingError::InvalidMessage);
            }
            let record = codec::message::decode_canonical(value)
                .map_err(|_| AccountingError::InvalidMessage)?;
            if record.sequence != *sequence || record.session_id.is_some() {
                return Err(AccountingError::InvalidMessage);
            }
            primary = Some((entity.clone(), record));
        } else if !value.is_empty() {
            return Err(AccountingError::InvalidMessage);
        }
    }
    let (entity, record) = primary.ok_or(AccountingError::InvalidMessage)?;
    let shadow = &entity == owner.shadow();
    if shadow != record.dead_letter.is_some()
        || (shadow && (record.expires_at.is_some() || record.state == MessageState::Scheduled))
    {
        return Err(AccountingError::InvalidMessage);
    }
    let expected: BTreeSet<_> = message_keys(owner, &entity, &record)?.into_iter().collect();
    if supplied != expected {
        return Err(AccountingError::InvalidMessage);
    }

    // All automatically mutable unsigned integers use their longest Postcard
    // varints: ten bytes for u64, five for u32. Optional-field presence and all
    // immutable variable-size content stay exactly as currently encoded.
    let mut maximum = record;
    maximum.sequence = SequenceNumber::new(u64::MAX);
    maximum.enqueued_at = Timestamp::from_millis(u64::MAX);
    maximum.scheduled_enqueue_at = maximum
        .scheduled_enqueue_at
        .map(|_| Timestamp::from_millis(u64::MAX));
    maximum.expires_at = maximum.expires_at.map(|_| Timestamp::from_millis(u64::MAX));
    maximum.delivery_count = u32::MAX;
    if let Some(info) = &mut maximum.dead_letter {
        info.dead_lettered_at = Timestamp::from_millis(u64::MAX);
    }
    let mut ceiling = peak_states(owner, &entity, &maximum, key_overhead)?;
    if !shadow {
        maximum.expires_at = None;
        for (reason, description) in [
            (
                DeadLetterReason::MaxDeliveryCountExceeded,
                "the message reached its maximum delivery count",
            ),
            (
                DeadLetterReason::TimeToLiveExpired,
                "the message exceeded its time to live",
            ),
            (
                DeadLetterReason::TimeToLiveExpired,
                "the deferred message exceeded its time to live",
            ),
        ] {
            maximum.dead_letter = Some(DeadLetterInfo {
                reason,
                description: description.to_owned(),
                dead_lettered_at: Timestamp::from_millis(u64::MAX),
            });
            ceiling = ceiling.max(peak_states(owner, owner.shadow(), &maximum, key_overhead)?);
        }
    }
    let extra = match framing {
        CapacityFramingBudget::CurrentUnframedEncoding
        | CapacityFramingBudget::IsolatedKeyspaceV1 => 0,
        CapacityFramingBudget::AdditionalMessageValueBytes(bytes) => bytes,
    };
    ceiling = ceiling
        .checked_add(extra)
        .ok_or(AccountingError::Overflow)?;
    let reserved_growth_bytes = ceiling
        .checked_sub(actual)
        .ok_or(AccountingError::InvalidMessage)?;
    Ok(MessageReservation {
        retained_bytes: actual,
        reserved_growth_bytes,
        ceiling_bytes: ceiling,
    })
}

fn peak_states(
    owner: &CapacityOwner,
    entity: &EntityPath,
    maximum: &MessageRecord,
    key_overhead: u64,
) -> Result<u64, AccountingError> {
    let mut states = vec![
        MessageState::Ready,
        MessageState::Deferred,
        MessageState::Locked {
            token: LockToken::new(u64::MAX),
            locked_until: Timestamp::from_millis(u64::MAX),
            origin: DeliveryOrigin::Ready,
        },
        MessageState::Locked {
            token: LockToken::new(u64::MAX),
            locked_until: Timestamp::from_millis(u64::MAX),
            origin: DeliveryOrigin::Deferred,
        },
    ];
    if !entity.is_dead_letter_queue() && maximum.scheduled_enqueue_at.is_some() {
        states.push(MessageState::Scheduled);
    }
    let mut ceiling = 0;
    for state in states {
        let mut candidate = maximum.clone();
        candidate.state = state;
        let value =
            codec::message::encode(&candidate).map_err(|_| AccountingError::InvalidMessage)?;
        let mut size = 0_u64;
        for key in message_keys(owner, entity, &candidate)? {
            let bytes = if key[0] == 0x03 {
                value.as_slice()
            } else {
                &[]
            };
            size = size
                .checked_add(encoded_row_bytes(&key, bytes)?)
                .and_then(|bytes| bytes.checked_add(key_overhead))
                .ok_or(AccountingError::Overflow)?;
        }
        ceiling = ceiling.max(size);
    }
    Ok(ceiling)
}

fn message_keys(
    owner: &CapacityOwner,
    entity: &EntityPath,
    record: &MessageRecord,
) -> Result<Vec<Key>, AccountingError> {
    let n = owner.namespace();
    let sequence = record.sequence;
    let mut result = vec![keys::message(n, entity, sequence)];
    match record.state {
        MessageState::Ready => {
            result.push(keys::ready(n, entity, sequence));
            if let Some(deadline) = record.expires_at {
                result.push(keys::expiry(n, entity, deadline, sequence));
            }
        }
        MessageState::Deferred => result.push(keys::deferred(n, entity, sequence)),
        MessageState::Scheduled => result.push(keys::scheduled(
            n,
            entity,
            record
                .scheduled_enqueue_at
                .ok_or(AccountingError::InvalidMessage)?,
            sequence,
        )),
        MessageState::Locked {
            locked_until,
            origin,
            ..
        } => {
            if origin == DeliveryOrigin::Scheduled {
                return Err(AccountingError::InvalidMessage);
            }
            result.push(keys::lock(n, entity, locked_until, sequence));
        }
    }
    Ok(result)
}
