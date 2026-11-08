//! Pure retained-KV accounting. No command, store, Clock or admission is changed.
//!
//! Callers supply one immutable before image and a complete atomic batch. These
//! calculations are not a writer fence or proof that a store image is healthy.

pub mod keys;
mod reservation;
pub mod usage_codec;

use std::collections::{BTreeMap, BTreeSet};

use storage::{Key, Mutation, Value, WriteBatch};
use thiserror::Error;

use crate::{EntityPath, NamespaceName, keys as domain_keys};

pub use reservation::{CapacityFramingBudget, MessageReservation, reserve_message};

/// Frozen reservation semantics for the current V1/V2/V3 message encodings.
pub const RESERVATION_MODEL_V1: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct CapacityOwner {
    namespace: NamespaceName,
    parent: EntityPath,
    shadow: EntityPath,
    generation: u64,
}

impl CapacityOwner {
    /// Only ordinary parent queues are supported. This is an identity supplied
    /// by a trusted caller, not evidence of a live catalog generation or fence.
    pub fn new(
        namespace: NamespaceName,
        parent: EntityPath,
        generation: u64,
    ) -> Result<Self, AccountingError> {
        if generation == 0
            || parent.is_dead_letter_queue()
            || parent.is_subscription()
            || parent.is_management()
        {
            return Err(AccountingError::InvalidOwner);
        }
        let shadow = parent
            .dead_letter_queue()
            .map_err(|_| AccountingError::InvalidOwner)?;
        Ok(Self {
            namespace,
            parent,
            shadow,
            generation,
        })
    }

    pub fn namespace(&self) -> &NamespaceName {
        &self.namespace
    }
    pub fn parent(&self) -> &EntityPath {
        &self.parent
    }
    pub fn shadow(&self) -> &EntityPath {
        &self.shadow
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }

    fn owns(&self, namespace: &NamespaceName, entity: &EntityPath) -> bool {
        namespace == &self.namespace && (entity == &self.parent || entity == &self.shadow)
    }
}

/// Actual bytes and prepaid automatic growth are deliberately separate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetainedUsage {
    generation: u64,
    retained_bytes: u64,
    reserved_growth_bytes: u64,
    message_count: u64,
}

impl RetainedUsage {
    pub fn new(
        generation: u64,
        retained_bytes: u64,
        reserved_growth_bytes: u64,
        message_count: u64,
    ) -> Result<Self, AccountingError> {
        if generation == 0
            || (message_count == 0 && (retained_bytes != 0 || reserved_growth_bytes != 0))
            || retained_bytes < message_count
        {
            return Err(AccountingError::CorruptUsage);
        }
        retained_bytes
            .checked_add(reserved_growth_bytes)
            .ok_or(AccountingError::Overflow)?;
        Ok(Self {
            generation,
            retained_bytes,
            reserved_growth_bytes,
            message_count,
        })
    }
    pub fn generation(self) -> u64 {
        self.generation
    }
    pub fn retained_bytes(self) -> u64 {
        self.retained_bytes
    }
    pub fn reserved_growth_bytes(self) -> u64 {
        self.reserved_growth_bytes
    }
    pub fn message_count(self) -> u64 {
        self.message_count
    }
    pub fn committed_bytes(self) -> Result<u64, AccountingError> {
        self.retained_bytes
            .checked_add(self.reserved_growth_bytes)
            .ok_or(AccountingError::Overflow)
    }

    /// Headroom must be recomputed from the resulting messages, not guessed
    /// from this raw byte delta. Underflow refuses a stale/corrupt ledger.
    pub fn apply(
        self,
        delta: &OwnerDelta,
        reservations: ReservationDelta,
    ) -> Result<Self, AccountingError> {
        if self.generation != delta.owner.generation {
            return Err(AccountingError::GenerationMismatch);
        }
        Self::new(
            self.generation,
            replace(self.retained_bytes, delta.removed_bytes, delta.added_bytes)?,
            replace(
                self.reserved_growth_bytes,
                reservations.removed,
                reservations.added,
            )?,
            replace(
                self.message_count,
                delta.removed_messages,
                delta.added_messages,
            )?,
        )
    }
}

fn replace(current: u64, removed: u64, added: u64) -> Result<u64, AccountingError> {
    current
        .checked_sub(removed)
        .ok_or(AccountingError::Underflow)?
        .checked_add(added)
        .ok_or(AccountingError::Overflow)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReservationDelta {
    pub removed: u64,
    pub added: u64,
}

/// Aggregate before/final totals avoid signed arithmetic and transient-order
/// underflow. Each touched key contributes once on either side.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerDelta {
    pub owner: CapacityOwner,
    pub removed_bytes: u64,
    pub added_bytes: u64,
    pub removed_messages: u64,
    pub added_messages: u64,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum AccountingError {
    #[error("invalid ordinary queue owner or zero generation")]
    InvalidOwner,
    #[error("duplicate or ambiguous owner scope")]
    DuplicateOwner,
    #[error("duplicate before-image key")]
    DuplicateBeforeImage,
    #[error("unknown or malformed key")]
    InvalidKey,
    #[error("charged key has no supplied owner")]
    UnownedKey,
    #[error("retained accounting overflow")]
    Overflow,
    #[error("retained accounting underflow")]
    Underflow,
    #[error("corrupt usage framing or inconsistent totals")]
    CorruptUsage,
    #[error("unsupported usage or reservation model version")]
    UnsupportedVersion,
    #[error("usage generation does not match its owner")]
    GenerationMismatch,
    #[error("message or companion rows are not canonical ordinary queue state")]
    InvalidMessage,
}

/// Exact logical retained charge, never filesystem/journal/LSM size.
pub fn encoded_row_bytes(key: &[u8], value: &[u8]) -> Result<u64, AccountingError> {
    u64::try_from(key.len())
        .ok()
        .and_then(|k| {
            u64::try_from(value.len())
                .ok()
                .and_then(|v| k.checked_add(v))
        })
        .ok_or(AccountingError::Overflow)
}

fn charged(key: &[u8]) -> Result<Option<domain_keys::StateKey>, AccountingError> {
    let key = if key.first() == Some(&crate::capacity_fence::DATA_TAG) {
        crate::capacity_fence::original_key(key)?
    } else {
        key
    };
    match key.first().copied() {
        Some(0x03..=0x07 | 0x0B) => domain_keys::state_parts(key)
            .map(Some)
            .ok_or(AccountingError::InvalidKey),
        // Known exclusions, including the separately versioned usage row.
        Some(0x00..=0x02 | 0x08..=0x0A | 0x0C..=0x12 | 0xF0 | 0xF1) => Ok(None),
        _ => Err(AccountingError::InvalidKey),
    }
}

/// Last-write-wins net effect. Absence in `before` means genuinely absent, not
/// an omitted read. Supply all touched original rows (a complete snapshot also
/// works if every charged scope has an owner). No I/O or mutation occurs.
pub fn calculate_batch(
    owners: &[CapacityOwner],
    before: &[(Key, Value)],
    batch: &WriteBatch,
) -> Result<Vec<OwnerDelta>, AccountingError> {
    let mut identities = BTreeSet::new();
    for owner in owners {
        if !identities.insert((&owner.namespace, &owner.parent)) {
            return Err(AccountingError::DuplicateOwner);
        }
    }
    let mut original = BTreeMap::new();
    for (key, value) in before {
        if original.insert(key.as_slice(), value.as_slice()).is_some() {
            return Err(AccountingError::DuplicateBeforeImage);
        }
        owner_index(owners, key)?;
    }
    let mut final_values = BTreeMap::new();
    for mutation in batch.mutations() {
        let (key, value) = match mutation {
            Mutation::Put { key, value } => (key.as_slice(), Some(value.as_slice())),
            Mutation::Delete { key } => (key.as_slice(), None),
        };
        // Validate even overwritten mutations: never hide an unknown scope.
        owner_index(owners, key)?;
        final_values.insert(key, value);
    }
    let mut result: Vec<_> = owners
        .iter()
        .map(|owner| OwnerDelta {
            owner: owner.clone(),
            removed_bytes: 0,
            added_bytes: 0,
            removed_messages: 0,
            added_messages: 0,
        })
        .collect();
    for (key, value) in final_values {
        let Some(index) = owner_index(owners, key)? else {
            continue;
        };
        let delta = &mut result[index];
        if let Some(old) = original.get(key) {
            add(&mut delta.removed_bytes, encoded_row_bytes(key, old)?)?;
            if primary_key(key) {
                add(&mut delta.removed_messages, 1)?;
            }
        }
        if let Some(new) = value {
            add(&mut delta.added_bytes, encoded_row_bytes(key, new)?)?;
            if primary_key(key) {
                add(&mut delta.added_messages, 1)?;
            }
        }
    }
    Ok(result)
}

fn primary_key(key: &[u8]) -> bool {
    key.first() == Some(&3) || key.get(..2) == Some(&[crate::capacity_fence::DATA_TAG, 3])
}

fn owner_index(owners: &[CapacityOwner], key: &[u8]) -> Result<Option<usize>, AccountingError> {
    let Some(parsed) = charged(key)? else {
        return Ok(None);
    };
    let (namespace, entity) = parsed.scope();
    owners
        .iter()
        .position(|owner| owner.owns(namespace, entity))
        .map(Some)
        .ok_or(AccountingError::UnownedKey)
}

fn add(total: &mut u64, value: u64) -> Result<(), AccountingError> {
    *total = total.checked_add(value).ok_or(AccountingError::Overflow)?;
    Ok(())
}
