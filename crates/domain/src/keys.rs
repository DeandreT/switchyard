//! Explicit big-endian key encoding for the broker state machine.
//!
//! Keys are built so that lexicographic byte order is the order the state
//! machine needs to walk them:
//!
//! - the ready index sorts by sequence number, which is queue FIFO order;
//! - the lock index sorts by lock deadline, so the expiry sweep stops at the
//!   first entry that has not elapsed;
//! - the expiry index sorts by message deadline for the same reason;
//! - the scheduled index sorts by requested enqueue time, then placeholder
//!   sequence number, so activation is deterministic and bounded;
//! - the deferred index sorts by sequence number for explicit retrieval and
//!   browsing without making those messages ordinarily ready;
//! - the session ready index sorts by session first and sequence second, so one
//!   session's messages are contiguous and in order within the group, and a
//!   receiver looking for a session to accept walks the groups in turn;
//! - the session lock index sorts by lock deadline, like the message one.
//! - the duplicate-history expiry index sorts by retention deadline, then
//!   exact message identifier, so cleanup is deterministic and bounded.
//!
//! Every entity-scoped key is `tag || namespace || 0x00 || path || 0x00 || ..`,
//! and a session-scoped key appends `session || 0x00` to that. The terminators
//! are safe because [`crate::NamespaceName`], [`crate::EntityPath`], and
//! [`crate::SessionId`] reject control characters, so no name can contain a zero
//! byte and forge another scope's prefix.

use crate::{EntityPath, NamespaceName, SequenceNumber, SessionId, Timestamp};

const TAG_CLOCK: u8 = 0x00;
const TAG_QUEUE_CONFIG: u8 = 0x01;
const TAG_QUEUE_COUNTERS: u8 = 0x02;
const TAG_MESSAGE: u8 = 0x03;
const TAG_READY: u8 = 0x04;
const TAG_LOCK: u8 = 0x05;
const TAG_EXPIRY: u8 = 0x06;
const TAG_DEFERRED: u8 = 0x07;
const TAG_SESSION: u8 = 0x08;
const TAG_SESSION_READY: u8 = 0x09;
const TAG_SESSION_LOCK: u8 = 0x0A;
const TAG_SCHEDULED: u8 = 0x0B;
const TAG_DUPLICATE_ID: u8 = 0x0C;
const TAG_DUPLICATE_EXPIRY: u8 = 0x0D;

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

fn session_scope(
    tag: u8,
    namespace: &NamespaceName,
    entity: &EntityPath,
    session_id: &SessionId,
) -> Vec<u8> {
    let mut key = entity_scope(tag, namespace, entity);
    key.extend_from_slice(session_id.as_str().as_bytes());
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

/// Every queue configuration in the store, across every namespace. Walking it is
/// how the timer worker learns what there is to sweep.
pub fn queue_config_prefix() -> Vec<u8> {
    vec![TAG_QUEUE_CONFIG]
}

/// Reads the namespace and entity path back out of an entity-scoped key.
pub fn entity_scope_parts(key: &[u8]) -> Option<(&str, &str)> {
    let rest = key.get(1..)?;
    let namespace_end = rest.iter().position(|byte| *byte == SEPARATOR)?;
    let namespace = std::str::from_utf8(rest.get(..namespace_end)?).ok()?;

    let tail = rest.get(namespace_end + 1..)?;
    let entity_end = tail.iter().position(|byte| *byte == SEPARATOR)?;
    let entity = std::str::from_utf8(tail.get(..entity_end)?).ok()?;
    Some((namespace, entity))
}

pub fn queue_counters(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_QUEUE_COUNTERS, namespace, entity)
}

pub fn message(
    namespace: &NamespaceName,
    entity: &EntityPath,
    sequence: SequenceNumber,
) -> Vec<u8> {
    with_u64(message_prefix(namespace, entity), sequence.as_u64())
}

/// Every primary message record in an entity, ordered by sequence number.
pub fn message_prefix(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_MESSAGE, namespace, entity)
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

pub fn scheduled_prefix(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_SCHEDULED, namespace, entity)
}

/// A scheduled placeholder ordered by requested enqueue time and then its
/// temporary sequence number.
pub fn scheduled(
    namespace: &NamespaceName,
    entity: &EntityPath,
    enqueue_at: Timestamp,
    sequence: SequenceNumber,
) -> Vec<u8> {
    let key = with_u64(scheduled_prefix(namespace, entity), enqueue_at.as_millis());
    with_u64(key, sequence.as_u64())
}

/// Exact nonpartitioned message-identifier lookup for duplicate detection.
///
/// The identifier is the final key component, so it needs no separator or
/// escaping even when it contains arbitrary Unicode.
pub fn duplicate_id(namespace: &NamespaceName, entity: &EntityPath, message_id: &str) -> Vec<u8> {
    let mut key = entity_scope(TAG_DUPLICATE_ID, namespace, entity);
    key.extend_from_slice(message_id.as_bytes());
    key
}

pub fn duplicate_expiry_prefix(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_DUPLICATE_EXPIRY, namespace, entity)
}

/// One duplicate-history generation ordered by deadline before identifier.
pub fn duplicate_expiry(
    namespace: &NamespaceName,
    entity: &EntityPath,
    expires_at: Timestamp,
    message_id: &str,
) -> Vec<u8> {
    let mut key = with_u64(
        duplicate_expiry_prefix(namespace, entity),
        expires_at.as_millis(),
    );
    key.extend_from_slice(message_id.as_bytes());
    key
}

/// Reads the deadline and exact identifier from a duplicate expiry key.
pub fn duplicate_expiry_parts<'a>(prefix: &[u8], key: &'a [u8]) -> Option<(Timestamp, &'a str)> {
    let rest = key.get(prefix.len()..)?;
    let deadline: [u8; 8] = rest.get(..8)?.try_into().ok()?;
    let message_id = std::str::from_utf8(rest.get(8..)?).ok()?;
    Some((
        Timestamp::from_millis(u64::from_be_bytes(deadline)),
        message_id,
    ))
}

pub fn deferred_prefix(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_DEFERRED, namespace, entity)
}

pub fn deferred(
    namespace: &NamespaceName,
    entity: &EntityPath,
    sequence: SequenceNumber,
) -> Vec<u8> {
    with_u64(deferred_prefix(namespace, entity), sequence.as_u64())
}

/// The record holding one session's lock and state.
pub fn session(namespace: &NamespaceName, entity: &EntityPath, session_id: &SessionId) -> Vec<u8> {
    session_scope(TAG_SESSION, namespace, entity, session_id)
}

/// Ready messages of one session, ordered by sequence — the FIFO order a
/// session guarantees.
pub fn session_ready_prefix(
    namespace: &NamespaceName,
    entity: &EntityPath,
    session_id: &SessionId,
) -> Vec<u8> {
    session_scope(TAG_SESSION_READY, namespace, entity, session_id)
}

pub fn session_ready(
    namespace: &NamespaceName,
    entity: &EntityPath,
    session_id: &SessionId,
    sequence: SequenceNumber,
) -> Vec<u8> {
    with_u64(
        session_ready_prefix(namespace, entity, session_id),
        sequence.as_u64(),
    )
}

/// Every ready message in the entity, grouped by session and ordered by session
/// identifier. Walking it is how a receiver finds a session to accept.
pub fn entity_session_ready_prefix(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_SESSION_READY, namespace, entity)
}

/// The key sorting immediately after every ready entry of `session_id`, which is
/// where a walk resumes once it has rejected that session.
///
/// Session identifiers cannot contain a control character, so replacing the
/// terminator with `0x01` sorts past every key in the session and before the
/// first key of any later one.
pub fn after_session_ready(
    namespace: &NamespaceName,
    entity: &EntityPath,
    session_id: &SessionId,
) -> Vec<u8> {
    let mut key = entity_scope(TAG_SESSION_READY, namespace, entity);
    key.extend_from_slice(session_id.as_str().as_bytes());
    key.push(SEPARATOR + 1);
    key
}

/// Session locks ordered by deadline, so a sweep stops at the first lock that is
/// still held.
pub fn session_lock_prefix(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_SESSION_LOCK, namespace, entity)
}

pub fn session_lock(
    namespace: &NamespaceName,
    entity: &EntityPath,
    locked_until: Timestamp,
    session_id: &SessionId,
) -> Vec<u8> {
    let mut key = with_u64(
        session_lock_prefix(namespace, entity),
        locked_until.as_millis(),
    );
    key.extend_from_slice(session_id.as_str().as_bytes());
    key
}

/// Reads the session identifier from a key built on `prefix`, which must be the
/// entity-scoped prefix that key was built from.
pub fn session_id_after<'a>(prefix: &[u8], key: &'a [u8]) -> Option<&'a str> {
    let rest = key.get(prefix.len()..)?;
    let end = rest.iter().position(|byte| *byte == SEPARATOR)?;
    std::str::from_utf8(rest.get(..end)?).ok()
}

/// Reads the deadline and session identifier from a session lock index key.
pub fn session_lock_parts<'a>(prefix: &[u8], key: &'a [u8]) -> Option<(Timestamp, &'a str)> {
    let rest = key.get(prefix.len()..)?;
    let bytes: [u8; 8] = rest.get(..8)?.try_into().ok()?;
    let session_id = std::str::from_utf8(rest.get(8..)?).ok()?;
    Some((
        Timestamp::from_millis(u64::from_be_bytes(bytes)),
        session_id,
    ))
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
    fn scheduled_keys_sort_by_enqueue_time_before_placeholder_sequence() {
        let mut keys = [
            scheduled(
                &namespace(),
                &entity(),
                Timestamp::from_millis(200),
                SequenceNumber::new(1),
            ),
            scheduled(
                &namespace(),
                &entity(),
                Timestamp::from_millis(100),
                SequenceNumber::new(9),
            ),
            scheduled(
                &namespace(),
                &entity(),
                Timestamp::from_millis(100),
                SequenceNumber::new(2),
            ),
        ];
        keys.sort();

        assert_eq!(
            keys.iter()
                .filter_map(|key| trailing_deadline(key))
                .collect::<Vec<_>>(),
            vec![
                (Timestamp::from_millis(100), SequenceNumber::new(2)),
                (Timestamp::from_millis(100), SequenceNumber::new(9)),
                (Timestamp::from_millis(200), SequenceNumber::new(1)),
            ]
        );
    }

    #[test]
    fn duplicate_expiry_keys_sort_by_deadline_and_preserve_exact_ids() {
        let prefix = duplicate_expiry_prefix(&namespace(), &entity());
        let mut keys = [
            duplicate_expiry(
                &namespace(),
                &entity(),
                Timestamp::from_millis(200),
                "same\0bytes",
            ),
            duplicate_expiry(&namespace(), &entity(), Timestamp::from_millis(100), "zeta"),
            duplicate_expiry(
                &namespace(),
                &entity(),
                Timestamp::from_millis(100),
                "alpha",
            ),
        ];
        keys.sort();

        assert_eq!(
            keys.iter()
                .filter_map(|key| duplicate_expiry_parts(&prefix, key))
                .collect::<Vec<_>>(),
            vec![
                (Timestamp::from_millis(100), "alpha"),
                (Timestamp::from_millis(100), "zeta"),
                (Timestamp::from_millis(200), "same\0bytes"),
            ]
        );
        assert_ne!(
            duplicate_id(&namespace(), &entity(), "42"),
            duplicate_id(&namespace(), &entity(), "042")
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

    fn session_id(value: &str) -> SessionId {
        SessionId::new(value).expect("valid session id")
    }

    #[test]
    fn session_ready_keys_sort_by_session_then_sequence() {
        let mut keys = [
            session_ready(
                &namespace(),
                &entity(),
                &session_id("b"),
                SequenceNumber::new(1),
            ),
            session_ready(
                &namespace(),
                &entity(),
                &session_id("a"),
                SequenceNumber::new(10),
            ),
            session_ready(
                &namespace(),
                &entity(),
                &session_id("a"),
                SequenceNumber::new(2),
            ),
        ];
        keys.sort();

        let prefix = entity_session_ready_prefix(&namespace(), &entity());
        assert_eq!(
            keys.iter()
                .filter_map(|key| Some((
                    session_id_after(&prefix, key)?,
                    trailing_sequence(key)?.as_u64()
                )))
                .collect::<Vec<_>>(),
            vec![("a", 2), ("a", 10), ("b", 1)]
        );
    }

    #[test]
    fn a_walk_resumes_past_every_entry_of_a_session() {
        let prefix = entity_session_ready_prefix(&namespace(), &entity());
        let resume = after_session_ready(&namespace(), &entity(), &session_id("a"));

        // Past every key of session "a", including its highest sequence...
        assert!(
            session_ready(
                &namespace(),
                &entity(),
                &session_id("a"),
                SequenceNumber::new(u64::MAX)
            ) < resume
        );
        // ...and before the first key of any session that sorts after it, even
        // one that has "a" as a prefix.
        for later in ["ab", "b"] {
            assert!(
                resume
                    < session_ready(
                        &namespace(),
                        &entity(),
                        &session_id(later),
                        SequenceNumber::new(0)
                    )
            );
        }
        assert!(resume.starts_with(&prefix));
    }

    #[test]
    fn session_lock_keys_sort_by_deadline_and_carry_their_session() {
        let prefix = session_lock_prefix(&namespace(), &entity());
        let early = session_lock(
            &namespace(),
            &entity(),
            Timestamp::from_millis(100),
            &session_id("late-session"),
        );
        let late = session_lock(
            &namespace(),
            &entity(),
            Timestamp::from_millis(200),
            &session_id("early-session"),
        );

        assert!(early < late);
        assert_eq!(
            session_lock_parts(&prefix, &early),
            Some((Timestamp::from_millis(100), "late-session"))
        );
    }

    #[test]
    fn sessions_do_not_collide_across_similar_names() {
        let short = session_ready_prefix(&namespace(), &entity(), &session_id("cart"));
        let long = session_ready_prefix(&namespace(), &entity(), &session_id("cart-2"));
        assert!(!long.starts_with(&short));
    }

    #[test]
    fn an_entity_scope_reads_back_out_of_its_key() {
        let key = queue_config(&namespace(), &entity());
        assert!(key.starts_with(&queue_config_prefix()));
        assert_eq!(entity_scope_parts(&key), Some(("tenant", "orders")));

        // Index keys carry a payload after the scope, which must not confuse it.
        let ready = ready(&namespace(), &entity(), SequenceNumber::new(7));
        assert_eq!(entity_scope_parts(&ready), Some(("tenant", "orders")));
        assert_eq!(entity_scope_parts(&clock()), None);
    }

    #[test]
    fn scopes_are_separated_across_namespaces() {
        let left = NamespaceName::new("tenant-a").expect("valid namespace");
        let right = NamespaceName::new("tenant-ab").expect("valid namespace");
        assert!(!ready_prefix(&right, &entity()).starts_with(&ready_prefix(&left, &entity())));
    }
}
