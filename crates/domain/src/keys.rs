//! Explicit big-endian key encoding for the broker state machine.
//!
//! Keys are built so that lexicographic byte order is the order the state
//! machine needs to walk them:
//!
//! - the ready index sorts by sequence number, which is queue FIFO order;
//! - the lock index sorts by lock deadline, so the expiry sweep stops at the
//!   first entry that has not elapsed;
//! - the expiry index sorts by message deadline for the same reason;
//! - the session ready index sorts by session first and sequence second, so one
//!   session's messages are contiguous and in order within the group, and a
//!   receiver looking for a session to accept walks the groups in turn;
//! - the session lock index sorts by lock deadline, like the message one.
//! - the scheduled index sorts by enqueue time, then cancellation handle.
//! - duplicate history expires in deadline order without inspecting messages.
//!
//! Every entity-scoped key is `tag || namespace || 0x00 || path || 0x00 || ..`,
//! and a session-scoped key appends `session || 0x00` to that. The terminators
//! are safe because [`crate::NamespaceName`], [`crate::EntityPath`], and
//! [`crate::SessionId`] reject control characters, so no name can contain a zero
//! byte and forge another scope's prefix.

use crate::{
    EntityPath, LockToken, NamespaceName, RuleName, SequenceNumber, SessionId, SubscriptionName,
    Timestamp,
};

const TAG_CLOCK: u8 = 0x00;
const TAG_QUEUE_CONFIG: u8 = 0x01;
const TAG_QUEUE_COUNTERS: u8 = 0x02;
const TAG_MESSAGE: u8 = 0x03;
const TAG_READY: u8 = 0x04;
const TAG_LOCK: u8 = 0x05;
const TAG_EXPIRY: u8 = 0x06;
// 0x07 held the dead-letter keyspace before dead-letter queues became queues;
// retired with store format 2 and not to be reused.
const TAG_SESSION: u8 = 0x08;
const TAG_SESSION_READY: u8 = 0x09;
const TAG_SESSION_LOCK: u8 = 0x0A;
const TAG_SCHEDULED: u8 = 0x0B;
const TAG_DUPLICATE_HISTORY: u8 = 0x0C;
const TAG_DUPLICATE_HISTORY_EXPIRY: u8 = 0x0D;
const TAG_TOPIC_CONFIG: u8 = 0x0E;
const TAG_TOPIC_SUBSCRIPTION: u8 = 0x0F;
const TAG_SUBSCRIPTION_RULE: u8 = 0x10;
const TAG_ENTITY_INCARNATION: u8 = 0x11;
const TAG_COMMITTED_CHECKPOINT: u8 = 0x12;
const TAG_SESSION_MESSAGE_LOCK_REVERSE: u8 = 0x13;
const TAG_SESSION_MESSAGE_LOCK_FORWARD: u8 = 0x14;
const TAG_SESSION_MESSAGE_LOCK_SUMMARY: u8 = 0x15;

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

/// Private replicated-store progress, never written by the standalone machine.
pub(crate) fn committed_checkpoint() -> Vec<u8> {
    vec![TAG_COMMITTED_CHECKPOINT]
}

pub fn queue_config(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_QUEUE_CONFIG, namespace, entity)
}

/// Every queue configuration in the store, across every namespace. Walking it is
/// how the timer worker learns what there is to sweep.
pub fn queue_config_prefix() -> Vec<u8> {
    vec![TAG_QUEUE_CONFIG]
}

/// Queue configurations in exactly one namespace, without neighboring names.
pub fn namespace_queue_config_prefix(namespace: &NamespaceName) -> Vec<u8> {
    let mut prefix = queue_config_prefix();
    prefix.extend_from_slice(namespace.as_str().as_bytes());
    prefix.push(SEPARATOR);
    prefix
}

pub fn topic_config(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_TOPIC_CONFIG, namespace, entity)
}

pub fn topic_config_prefix() -> Vec<u8> {
    vec![TAG_TOPIC_CONFIG]
}

pub fn namespace_topic_config_prefix(namespace: &NamespaceName) -> Vec<u8> {
    let mut prefix = topic_config_prefix();
    prefix.extend_from_slice(namespace.as_str().as_bytes());
    prefix.push(SEPARATOR);
    prefix
}

/// Membership records scoped to exactly one parent topic.
pub fn subscription_prefix(namespace: &NamespaceName, topic: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_TOPIC_SUBSCRIPTION, namespace, topic)
}

pub fn subscription(
    namespace: &NamespaceName,
    topic: &EntityPath,
    name: &SubscriptionName,
) -> Vec<u8> {
    let mut key = subscription_prefix(namespace, topic);
    key.extend_from_slice(name.as_str().as_bytes());
    key.push(SEPARATOR);
    key
}

/// Requires a complete membership key with a valid single name and no extra tail.
pub fn subscription_name_parts<'a>(prefix: &[u8], key: &'a [u8]) -> Option<&'a str> {
    if prefix.first().copied()? != TAG_TOPIC_SUBSCRIPTION {
        return None;
    }
    let rest = key.strip_prefix(prefix)?.strip_suffix(&[SEPARATOR])?;
    let name = std::str::from_utf8(rest).ok()?;
    SubscriptionName::validate(name).ok()?;
    Some(name)
}

pub fn rule_prefix(
    namespace: &NamespaceName,
    topic: &EntityPath,
    subscription: &SubscriptionName,
) -> Vec<u8> {
    let mut prefix = topic_rule_prefix(namespace, topic);
    prefix.extend_from_slice(subscription.as_str().as_bytes());
    prefix.push(SEPARATOR);
    prefix
}

pub fn rule(
    namespace: &NamespaceName,
    topic: &EntityPath,
    subscription: &SubscriptionName,
    name: &RuleName,
) -> Vec<u8> {
    let mut key = rule_prefix(namespace, topic, subscription);
    key.extend_from_slice(name.as_str().as_bytes());
    key.push(SEPARATOR);
    key
}

pub fn topic_rule_prefix(namespace: &NamespaceName, topic: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_SUBSCRIPTION_RULE, namespace, topic)
}

pub fn topic_rule_parts<'a>(prefix: &[u8], key: &'a [u8]) -> Option<(&'a str, &'a str)> {
    if prefix.first().copied()? != TAG_SUBSCRIPTION_RULE {
        return None;
    }
    let rest = key.strip_prefix(prefix)?;
    let separator = rest.iter().position(|byte| *byte == SEPARATOR)?;
    let subscription = std::str::from_utf8(rest.get(..separator)?).ok()?;
    SubscriptionName::validate(subscription).ok()?;
    let rule = std::str::from_utf8(rest.get(separator + 1..)?.strip_suffix(&[SEPARATOR])?).ok()?;
    RuleName::validate(rule).ok()?;
    Some((subscription, rule))
}

pub fn subscription_backing_config_prefix(
    namespace: &NamespaceName,
    topic: &EntityPath,
) -> Vec<u8> {
    subscription_descendant_scope(TAG_QUEUE_CONFIG, namespace, topic)
}

pub fn subscription_topic_config_prefix(namespace: &NamespaceName, topic: &EntityPath) -> Vec<u8> {
    subscription_descendant_scope(TAG_TOPIC_CONFIG, namespace, topic)
}

pub(crate) fn subscription_membership_descendant_prefix(
    namespace: &NamespaceName,
    topic: &EntityPath,
) -> Vec<u8> {
    subscription_descendant_scope(TAG_TOPIC_SUBSCRIPTION, namespace, topic)
}

pub(crate) fn subscription_rule_descendant_prefix(
    namespace: &NamespaceName,
    topic: &EntityPath,
) -> Vec<u8> {
    subscription_descendant_scope(TAG_SUBSCRIPTION_RULE, namespace, topic)
}

fn subscription_descendant_scope(
    tag: u8,
    namespace: &NamespaceName,
    topic: &EntityPath,
) -> Vec<u8> {
    let mut prefix = entity_scope(tag, namespace, topic);
    prefix.pop();
    prefix.extend_from_slice(b"/subscriptions/");
    prefix
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum RuntimeKind {
    Message,
    LocalToken,
    Other,
}

const RUNTIME_FAMILIES: [(u8, RuntimeKind); 13] = [
    (TAG_MESSAGE, RuntimeKind::Message),
    (TAG_READY, RuntimeKind::Other),
    (TAG_LOCK, RuntimeKind::LocalToken),
    (TAG_EXPIRY, RuntimeKind::Other),
    (TAG_SESSION, RuntimeKind::LocalToken),
    (TAG_SESSION_READY, RuntimeKind::Other),
    (TAG_SESSION_LOCK, RuntimeKind::LocalToken),
    (TAG_SCHEDULED, RuntimeKind::Other),
    (TAG_DUPLICATE_HISTORY, RuntimeKind::Other),
    (TAG_DUPLICATE_HISTORY_EXPIRY, RuntimeKind::Other),
    (TAG_SESSION_MESSAGE_LOCK_REVERSE, RuntimeKind::LocalToken),
    (TAG_SESSION_MESSAGE_LOCK_FORWARD, RuntimeKind::LocalToken),
    (TAG_SESSION_MESSAGE_LOCK_SUMMARY, RuntimeKind::LocalToken),
];

pub(crate) fn entity_runtime_prefixes(
    namespace: &NamespaceName,
    entity: &EntityPath,
) -> [(Vec<u8>, RuntimeKind); 13] {
    RUNTIME_FAMILIES.map(|(tag, kind)| (entity_scope(tag, namespace, entity), kind))
}

pub(crate) fn subscription_runtime_prefixes(
    namespace: &NamespaceName,
    topic: &EntityPath,
) -> [(Vec<u8>, RuntimeKind); 13] {
    RUNTIME_FAMILIES.map(|(tag, kind)| (subscription_descendant_scope(tag, namespace, topic), kind))
}

pub fn rule_name_parts<'a>(prefix: &[u8], key: &'a [u8]) -> Option<&'a str> {
    if prefix.first().copied()? != TAG_SUBSCRIPTION_RULE {
        return None;
    }
    let rest = key.strip_prefix(prefix)?.strip_suffix(&[SEPARATOR])?;
    let name = std::str::from_utf8(rest).ok()?;
    RuleName::validate(name).ok()?;
    Some(name)
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

/// Retained incarnation ownership; a DLQ uses its parent owner's key.
pub fn entity_incarnation(namespace: &NamespaceName, owner: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_ENTITY_INCARNATION, namespace, owner)
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

pub fn scheduled(
    namespace: &NamespaceName,
    entity: &EntityPath,
    enqueue_at: Timestamp,
    sequence: SequenceNumber,
) -> Vec<u8> {
    let key = with_u64(scheduled_prefix(namespace, entity), enqueue_at.as_millis());
    with_u64(key, sequence.as_u64())
}

pub fn duplicate_history_prefix(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_DUPLICATE_HISTORY, namespace, entity)
}

/// Message identifiers are unstructured strings and may contain zero bytes.
/// They occupy the whole remaining key, so no identifier can forge a suffix.
pub fn duplicate_history(
    namespace: &NamespaceName,
    entity: &EntityPath,
    message_id: &str,
) -> Vec<u8> {
    let mut key = duplicate_history_prefix(namespace, entity);
    key.extend_from_slice(message_id.as_bytes());
    key
}

pub fn duplicate_history_expiry_prefix(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_DUPLICATE_HISTORY_EXPIRY, namespace, entity)
}

pub fn duplicate_history_expiry(
    namespace: &NamespaceName,
    entity: &EntityPath,
    expires_at: Timestamp,
    message_id: &str,
) -> Vec<u8> {
    let mut key = with_u64(
        duplicate_history_expiry_prefix(namespace, entity),
        expires_at.as_millis(),
    );
    key.extend_from_slice(message_id.as_bytes());
    key
}

pub fn duplicate_history_expiry_parts<'a>(
    prefix: &[u8],
    key: &'a [u8],
) -> Option<(Timestamp, &'a str)> {
    let rest = key.strip_prefix(prefix)?;
    let bytes: [u8; 8] = rest.get(..8)?.try_into().ok()?;
    let message_id = std::str::from_utf8(rest.get(8..)?).ok()?;
    Some((
        Timestamp::from_millis(u64::from_be_bytes(bytes)),
        message_id,
    ))
}

/// The record holding one session's lock and state.
pub fn session(namespace: &NamespaceName, entity: &EntityPath, session_id: &SessionId) -> Vec<u8> {
    session_scope(TAG_SESSION, namespace, entity, session_id)
}

pub fn entity_session_prefix(namespace: &NamespaceName, entity: &EntityPath) -> Vec<u8> {
    entity_scope(TAG_SESSION, namespace, entity)
}

pub fn session_message_lock_reverse_prefix(
    namespace: &NamespaceName,
    entity: &EntityPath,
) -> Vec<u8> {
    entity_scope(TAG_SESSION_MESSAGE_LOCK_REVERSE, namespace, entity)
}

pub fn session_message_lock_reverse(
    namespace: &NamespaceName,
    entity: &EntityPath,
    sequence: SequenceNumber,
) -> Vec<u8> {
    with_u64(
        session_message_lock_reverse_prefix(namespace, entity),
        sequence.as_u64(),
    )
}

fn session_message_lock_scope_is_valid(prefix: &[u8], tag: u8, session_scoped: bool) -> bool {
    if prefix.first().copied() != Some(tag) {
        return false;
    }
    let Some((namespace, entity)) = entity_scope_parts(prefix) else {
        return false;
    };
    let (Ok(namespace), Ok(entity)) = (NamespaceName::new(namespace), EntityPath::new(entity))
    else {
        return false;
    };
    let expected = entity_scope(tag, &namespace, &entity);
    if !session_scoped {
        return expected == prefix;
    }
    let Some(session) = prefix
        .strip_prefix(expected.as_slice())
        .and_then(|tail| tail.strip_suffix(&[SEPARATOR]))
    else {
        return false;
    };
    let Ok(session) = std::str::from_utf8(session) else {
        return false;
    };
    let Ok(session) = SessionId::new(session) else {
        return false;
    };
    session_scope(tag, &namespace, &entity, &session) == prefix
}

pub fn session_message_lock_reverse_parts(prefix: &[u8], key: &[u8]) -> Option<SequenceNumber> {
    if !session_message_lock_scope_is_valid(prefix, TAG_SESSION_MESSAGE_LOCK_REVERSE, false) {
        return None;
    }
    let bytes: [u8; 8] = key.strip_prefix(prefix)?.try_into().ok()?;
    Some(SequenceNumber::new(u64::from_be_bytes(bytes)))
}

pub fn session_message_lock_forward_prefix(
    namespace: &NamespaceName,
    entity: &EntityPath,
    session_id: &SessionId,
) -> Vec<u8> {
    session_scope(
        TAG_SESSION_MESSAGE_LOCK_FORWARD,
        namespace,
        entity,
        session_id,
    )
}

/// None means trusted ID-only acquisition, never an inferred session generation.
pub fn session_message_lock_forward(
    namespace: &NamespaceName,
    entity: &EntityPath,
    session_id: &SessionId,
    generation: Option<LockToken>,
    sequence: SequenceNumber,
) -> Vec<u8> {
    let mut key = session_message_lock_forward_prefix(namespace, entity, session_id);
    key.push(u8::from(generation.is_some()));
    let key = with_u64(key, generation.map_or(0, LockToken::as_u64));
    with_u64(key, sequence.as_u64())
}

pub fn session_message_lock_forward_parts(
    prefix: &[u8],
    key: &[u8],
) -> Option<(Option<LockToken>, SequenceNumber)> {
    if !session_message_lock_scope_is_valid(prefix, TAG_SESSION_MESSAGE_LOCK_FORWARD, true) {
        return None;
    }
    let rest = key.strip_prefix(prefix)?;
    if rest.len() != 17 {
        return None;
    }
    let generation = u64::from_be_bytes(rest[1..9].try_into().ok()?);
    let generation = match (rest[0], generation) {
        (0, 0) => None,
        (1, token) if token > 0 => Some(LockToken::new(token)),
        _ => return None,
    };
    Some((
        generation,
        SequenceNumber::new(u64::from_be_bytes(rest[9..17].try_into().ok()?)),
    ))
}

pub fn session_message_lock_summary(
    namespace: &NamespaceName,
    entity: &EntityPath,
    session_id: &SessionId,
) -> Vec<u8> {
    session_scope(
        TAG_SESSION_MESSAGE_LOCK_SUMMARY,
        namespace,
        entity,
        session_id,
    )
}

pub fn session_message_lock_summary_prefix(
    namespace: &NamespaceName,
    entity: &EntityPath,
) -> Vec<u8> {
    entity_scope(TAG_SESSION_MESSAGE_LOCK_SUMMARY, namespace, entity)
}

pub fn session_message_lock_summary_parts<'a>(prefix: &[u8], key: &'a [u8]) -> Option<&'a str> {
    if !session_message_lock_scope_is_valid(prefix, TAG_SESSION_MESSAGE_LOCK_SUMMARY, false) {
        return None;
    }
    let session =
        std::str::from_utf8(key.strip_prefix(prefix)?.strip_suffix(&[SEPARATOR])?).ok()?;
    SessionId::new(session).ok()?;
    Some(session)
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

/// Reads the deadline and sequence number from a lock, expiry, or scheduled index key.
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
    fn incarnation_key_has_its_own_exact_owner_scope() {
        let key = entity_incarnation(&namespace(), &entity());
        assert_eq!(key, b"\x11tenant\0orders\0");
        assert_ne!(key, queue_config(&namespace(), &entity()));
        assert_ne!(key, queue_counters(&namespace(), &entity()));
        assert_ne!(
            key,
            entity_incarnation(
                &namespace(),
                &entity().dead_letter_queue().expect("valid shadow")
            )
        );
        assert_ne!(
            key,
            entity_incarnation(
                &NamespaceName::new("tenant-other").expect("valid namespace"),
                &entity()
            )
        );
    }

    #[test]
    fn topic_rule_parser_requires_exact_valid_owner_and_rule_tails() {
        let subscription = SubscriptionName::new("Alpha").expect("valid name");
        let name = RuleName::new("$Default").expect("valid name");
        let prefix = topic_rule_prefix(&namespace(), &entity());
        let key = rule(&namespace(), &entity(), &subscription, &name);
        assert_eq!(topic_rule_parts(&prefix, &key), Some(("Alpha", "$Default")));
        assert_eq!(
            topic_rule_parts(&subscription_prefix(&namespace(), &entity()), &key),
            None
        );
        assert_eq!(topic_rule_parts(&[], &key), None);
        let foreign = topic_rule_prefix(
            &NamespaceName::new("tenant-other").expect("valid namespace"),
            &entity(),
        );
        assert_eq!(topic_rule_parts(&foreign, &key), None);
        for suffix in [
            b"".as_slice(),
            b"Alpha\0".as_slice(),
            b"Alpha\0rule".as_slice(),
            b"Alpha\0rule\0tail".as_slice(),
            b"Alpha\0rule\0\0".as_slice(),
            b"Alpha\0a/b\0".as_slice(),
            b"a/b\0rule\0".as_slice(),
            b"Alpha\0\xff\0".as_slice(),
        ] {
            let mut malformed = prefix.clone();
            malformed.extend_from_slice(suffix);
            assert_eq!(topic_rule_parts(&prefix, &malformed), None, "{suffix:?}");
        }
    }

    #[test]
    fn descendant_prefixes_keep_canonical_parent_and_namespace_boundaries() {
        let topic = EntityPath::new("orders/Subscriptions").expect("valid parent");
        let name = SubscriptionName::new("Subscriptions").expect("valid name");
        let child = topic.subscription(&name).expect("valid child");
        let shadow = child.dead_letter_queue().expect("valid shadow");
        let prefix = subscription_backing_config_prefix(&namespace(), &topic);
        assert!(queue_config(&namespace(), &child).starts_with(&prefix));
        assert!(queue_config(&namespace(), &shadow).starts_with(&prefix));
        assert!(!queue_config(&namespace(), &topic).starts_with(&prefix));
        assert!(!queue_counters(&namespace(), &child).starts_with(&prefix));
        let similar = EntityPath::new("orders/Subscriptions-extra")
            .expect("valid parent")
            .subscription(&name)
            .expect("valid child");
        assert!(!queue_config(&namespace(), &similar).starts_with(&prefix));
        let foreign = NamespaceName::new("tenant-other").expect("valid namespace");
        assert!(!queue_config(&foreign, &child).starts_with(&prefix));
        assert!(
            topic_config(&namespace(), &child)
                .starts_with(&subscription_topic_config_prefix(&namespace(), &topic))
        );
        assert!(subscription(&namespace(), &child, &name).starts_with(
            &subscription_membership_descendant_prefix(&namespace(), &topic)
        ));
        assert!(
            rule(
                &namespace(),
                &child,
                &name,
                &RuleName::new("r").expect("valid rule")
            )
            .starts_with(&subscription_rule_descendant_prefix(&namespace(), &topic))
        );
    }

    #[test]
    fn deletion_runtime_families_cover_live_indexes_but_not_counter_tombstones() {
        let exact = entity_runtime_prefixes(&namespace(), &entity());
        let tags = exact
            .iter()
            .map(|(prefix, _)| prefix[0])
            .collect::<Vec<_>>();
        assert_eq!(
            tags,
            vec![
                0x03, 0x04, 0x05, 0x06, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x13, 0x14, 0x15
            ]
        );
        assert!(matches!(exact[0].1, RuntimeKind::Message));
        for index in [2, 4, 6, 10, 11, 12] {
            assert!(matches!(exact[index].1, RuntimeKind::LocalToken));
        }
        assert_eq!(entity_session_prefix(&namespace(), &entity()), exact[4].0);
        let child = entity()
            .subscription(&SubscriptionName::new("Alpha").expect("valid name"))
            .expect("valid child");
        let descendants = subscription_runtime_prefixes(&namespace(), &entity());
        let child_exact = entity_runtime_prefixes(&namespace(), &child);
        for ((descendant, _), (child_prefix, _)) in descendants.iter().zip(&child_exact) {
            assert!(child_prefix.starts_with(descendant));
        }
        assert!(
            exact
                .iter()
                .all(|(prefix, _)| !queue_counters(&namespace(), &entity()).starts_with(prefix))
        );
        assert!(
            descendants
                .iter()
                .all(|(prefix, _)| !queue_counters(&namespace(), &child).starts_with(prefix))
        );
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
    fn scheduled_keys_sort_by_enqueue_time_then_cancellation_handle() {
        let mut entries = [
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
        entries.sort();
        assert_eq!(
            entries
                .iter()
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
    fn duplicate_history_keys_preserve_embedded_zero_bytes() {
        let plain = duplicate_history(&namespace(), &entity(), "id");
        let with_zero = duplicate_history(&namespace(), &entity(), "id\0suffix");
        assert_ne!(plain, with_zero);
        let key = duplicate_history_expiry(
            &namespace(),
            &entity(),
            Timestamp::from_millis(20_000),
            "id\0suffix",
        );
        let prefix = duplicate_history_expiry_prefix(&namespace(), &entity());
        assert_eq!(
            duplicate_history_expiry_parts(&prefix, &key),
            Some((Timestamp::from_millis(20_000), "id\0suffix"))
        );
        assert_eq!(
            duplicate_history_expiry_parts(&ready_prefix(&namespace(), &entity()), &key),
            None
        );
    }

    #[test]
    fn duplicate_history_cleanup_orders_by_deadline_before_identifier() {
        let early =
            duplicate_history_expiry(&namespace(), &entity(), Timestamp::from_millis(10), "z");
        let late =
            duplicate_history_expiry(&namespace(), &entity(), Timestamp::from_millis(20), "a");
        assert!(early < late);
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
    fn topology_keyspaces_have_distinct_tags_and_exact_scope_boundaries() {
        let namespace = namespace();
        let topic = entity();
        assert_eq!(topic_config(&namespace, &topic), b"\x0etenant\0orders\0");
        assert_eq!(topic_config_prefix(), [0x0e]);
        assert_eq!(namespace_topic_config_prefix(&namespace), b"\x0etenant\0");
        assert_eq!(
            subscription_prefix(&namespace, &topic),
            b"\x0ftenant\0orders\0"
        );
        assert_ne!(
            topic_config(&namespace, &topic),
            queue_config(&namespace, &topic)
        );
        let neighbor_namespace = NamespaceName::new("tenant-a").expect("valid namespace");
        let neighbor_topic = EntityPath::new("orders-a").expect("valid topic path");
        assert!(
            !topic_config(&neighbor_namespace, &topic)
                .starts_with(&namespace_topic_config_prefix(&namespace))
        );
        assert!(
            !subscription_prefix(&namespace, &neighbor_topic)
                .starts_with(&subscription_prefix(&namespace, &topic))
        );
    }

    #[test]
    fn subscription_membership_keys_sort_by_name_and_parse_exactly() {
        let prefix = subscription_prefix(&namespace(), &entity());
        let mut keys = ["z", "a-2", "a-1"].map(|name| {
            subscription(
                &namespace(),
                &entity(),
                &SubscriptionName::new(name).expect("valid name"),
            )
        });
        keys.sort();
        assert_eq!(
            keys.iter()
                .filter_map(|key| subscription_name_parts(&prefix, key))
                .collect::<Vec<_>>(),
            ["a-1", "a-2", "z"]
        );
        assert_eq!(keys[0], b"\x0ftenant\0orders\0a-1\0");
    }

    #[test]
    fn subscription_membership_parser_rejects_malformed_or_foreign_keys() {
        let prefix = subscription_prefix(&namespace(), &entity());
        for suffix in [
            b"".as_slice(),
            b"a".as_slice(),
            b"\0".as_slice(),
            b"a\0tail".as_slice(),
            b"a\0\0".as_slice(),
            b"a/b\0".as_slice(),
            b"..\0".as_slice(),
            b"_a\0".as_slice(),
            b"a-\0".as_slice(),
            b"\xff\0".as_slice(),
        ] {
            let mut key = prefix.clone();
            key.extend_from_slice(suffix);
            assert_eq!(subscription_name_parts(&prefix, &key), None, "{suffix:?}");
        }
        let mut overlong = prefix.clone();
        overlong.extend_from_slice(&[b'a'; crate::MAX_SUBSCRIPTION_NAME_BYTES + 1]);
        overlong.push(SEPARATOR);
        assert_eq!(subscription_name_parts(&prefix, &overlong), None);
        let name = SubscriptionName::new("accounting").expect("valid name");
        let key = subscription(&namespace(), &entity(), &name);
        assert_eq!(
            subscription_name_parts(&topic_config(&namespace(), &entity()), &key),
            None
        );
        assert_eq!(subscription_name_parts(&[], &key), None);
        assert_eq!(
            subscription_name_parts(
                &subscription_prefix(
                    &NamespaceName::new("tenant-a").expect("valid namespace"),
                    &entity()
                ),
                &key
            ),
            None
        );
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
