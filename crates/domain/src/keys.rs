//! Explicit big-endian key encoding for the broker state machine.
//!
//! Keys are built so that lexicographic byte order is the order the state
//! machine needs to walk them:
//!
//! - the ready index sorts by sequence number, which is queue FIFO order;
//! - the lock index sorts by lock deadline, so the expiry sweep stops at the
//!   first entry that has not elapsed;
//! - the expiry index sorts by message deadline for the same reason.
//!
//! Every entity-scoped key is `tag || namespace || 0x00 || path || 0x00 || ..`.
//! The terminator is safe because [`crate::NamespaceName`] and
//! [`crate::EntityPath`] reject control characters, so no name can contain a
//! zero byte and forge another entity's prefix.

use crate::{EntityPath, NamespaceName, SequenceNumber, Timestamp};

const TAG_CLOCK: u8 = 0x00;
const TAG_QUEUE_CONFIG: u8 = 0x01;
const TAG_QUEUE_COUNTERS: u8 = 0x02;
const TAG_MESSAGE: u8 = 0x03;
const TAG_READY: u8 = 0x04;
const TAG_LOCK: u8 = 0x05;
const TAG_EXPIRY: u8 = 0x06;
const TAG_DEAD_LETTER: u8 = 0x07;

const SEPARATOR: u8 = 0x00;

fn entity_scope(tag: u8, namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    let namespace = namespace.as_str().as_bytes();
    let entity = entity.as_str().as_bytes();
    let mut key = Vec::with_capacity(namespace.len() + entity.len() + 3);
    key.push(tag);
    key.extend_from_slice(namespace);
    key.push(SEPARATOR);
    key.extend_from_slice(entity);
    key.push(SEPARATOR);
    key
}

fn with_u64(mut key: Vec<u8>, value: u64) -> Vec<u8> {
    key.extend_from_slice(&value.to_be_bytes());
    key
}

/// The single record holding the highest timestamp the machine has applied.
pub fn clock() -> Vec<u8> {
    vec![TAG_CLOCK]
}

pub fn queue_config(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_QUEUE_CONFIG, namespace, entity)
}

pub fn queue_counters(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_QUEUE_COUNTERS, namespace, entity)
}

pub fn message(
    namespace: &NamespaceName,
    entity: &EntityPath,
    sequence: SequenceNumber,
) -> Vec<u8> {
    with_u64(
        entity_scope(TAG_MESSAGE, namespace, entity),
        sequence.as_u64(),
    )
}

pub fn ready_prefix(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_READY, namespace, entity)
}

pub fn ready(namespace: &NamespaceName, entity: &EntityPath, sequence: SequenceNumber) -> Vec<u8> {
    with_u64(ready_prefix(namespace, entity), sequence.as_u64())
}

pub fn lock_prefix(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_LOCK, namespace, entity)
}

pub fn lock(
    namespace: &NamespaceName,
    entity: &EntityPath,
    locked_until: Timestamp,
    sequence: SequenceNumber,
) -> Vec<u8> {
    let key = with_u64(lock_prefix(namespace, entity), locked_until.as_millis());
    with_u64(key, sequence.as_u64())
}

pub fn expiry_prefix(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_EXPIRY, namespace, entity)
}

pub fn expiry(
    namespace: &NamespaceName,
    entity: &EntityPath,
    expires_at: Timestamp,
    sequence: SequenceNumber,
) -> Vec<u8> {
    let key = with_u64(expiry_prefix(namespace, entity), expires_at.as_millis());
    with_u64(key, sequence.as_u64())
}

pub fn dead_letter_prefix(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_DEAD_LETTER, namespace, entity)
}

pub fn dead_letter(
    namespace: &NamespaceName,
    entity: &EntityPath,
    sequence: SequenceNumber,
) -> Vec<u8> {
    with_u64(dead_letter_prefix(namespace, entity), sequence.as_u64())
}

/// Reads the sequence number from a ready or dead-letter index key.
pub fn trailing_sequence(key: &[u8]) -> Option<SequenceNumber> {
    let bytes: [u8; 8] = key.get(key.len().checked_sub(8)?..)?.try_into().ok()?;
    Some(SequenceNumber::new(u64::from_be_bytes(bytes)))
}

/// Reads the deadline and sequence number from a lock or expiry index key.
pub fn trailing_deadline(key: &[u8]) -> Option<(Timestamp, SequenceNumber)> {
    let sequence = trailing_sequence(key)?;
    let start = key.len().checked_sub(16)?;
    let bytes: [u8; 8] = key.get(start..start + 8)?.try_into().ok()?;
    Some((Timestamp::from_millis(u64::from_be_bytes(bytes)), sequence))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn namespace() -> NamespaceName {
        NamespaceName::new("tenant").expect("valid namespace")
    }

    fn entity() -> EntityPath {
        EntityPath::new("orders").expect("valid entity path")
    }

    #[test]
    fn ready_keys_sort_in_queue_order() {
        let mut keys = [
            ready(&namespace(), &entity(), SequenceNumber::new(2)),
            ready(&namespace(), &entity(), SequenceNumber::new(10)),
            ready(&namespace(), &entity(), SequenceNumber::new(1)),
        ];
        keys.sort();
        assert_eq!(
            keys.iter()
                .filter_map(|key| trailing_sequence(key))
                .collect::<Vec<_>>(),
            vec![
                SequenceNumber::new(1),
                SequenceNumber::new(2),
                SequenceNumber::new(10)
            ]
        );
    }

    #[test]
    fn lock_keys_sort_by_deadline_before_sequence() {
        let early = lock(
            &namespace(),
            &entity(),
            Timestamp::from_millis(100),
            SequenceNumber::new(9),
        );
        let late = lock(
            &namespace(),
            &entity(),
            Timestamp::from_millis(200),
            SequenceNumber::new(1),
        );
        assert!(early < late);
        assert_eq!(
            trailing_deadline(&early),
            Some((Timestamp::from_millis(100), SequenceNumber::new(9)))
        );
    }

    #[test]
    fn entity_prefixes_do_not_collide_across_similar_names() {
        let short = EntityPath::new("orders").expect("valid entity path");
        let long = EntityPath::new("orders-archive").expect("valid entity path");
        let short_prefix = ready_prefix(&namespace(), &short);
        let long_prefix = ready_prefix(&namespace(), &long);
        assert!(!long_prefix.starts_with(&short_prefix));
    }

    #[test]
    fn scopes_are_separated_across_namespaces() {
        let left = NamespaceName::new("tenant-a").expect("valid namespace");
        let right = NamespaceName::new("tenant-ab").expect("valid namespace");
        assert!(!ready_prefix(&right, &entity()).starts_with(&ready_prefix(&left, &entity())));
    }
}
