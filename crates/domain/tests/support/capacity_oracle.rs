//! Independent raw-image accounting oracle, shared only by test binaries.
//! No production reservation, batch calculator, finalizer or ledger total is
//! used to derive expectations. Real frozen codecs measure candidate values.

// Different integration binaries use different observations/entry points.
#![allow(dead_code)]

use domain::{
    DeadLetterInfo, DeadLetterReason, DeliveryOrigin, EntityPath, LockToken, MessageRecord,
    MessageState, QueueCapacity, QueueCounters, SequenceNumber, Timestamp, capacity_fence, codec,
    keys,
    retained_accounting::{CapacityOwner, RetainedUsage, usage_codec},
};
use std::collections::{BTreeMap, BTreeSet};
use storage::{Key, Value};

#[derive(Debug)]
pub struct Observation {
    pub usage: RetainedUsage,
    pub ceilings: BTreeMap<SequenceNumber, u64>,
    pub messages: BTreeMap<SequenceNumber, (EntityPath, MessageRecord)>,
}

fn physical(key: Vec<u8>) -> Vec<u8> {
    let mut result = vec![0x13];
    result.extend(key);
    result
}

fn image(
    owner: &CapacityOwner,
    endpoint: &EntityPath,
    message: &MessageRecord,
) -> BTreeMap<Key, Value> {
    let n = owner.namespace();
    let s = message.sequence;
    let mut rows = BTreeMap::from([(
        physical(keys::message(n, endpoint, s)),
        codec::message::encode(message).unwrap(),
    )]);
    let mut indexes = Vec::new();
    match message.state {
        MessageState::Ready => {
            indexes.push(keys::ready(n, endpoint, s));
            if let Some(t) = message.expires_at {
                indexes.push(keys::expiry(n, endpoint, t, s));
            }
        }
        MessageState::Deferred => indexes.push(keys::deferred(n, endpoint, s)),
        MessageState::Scheduled => indexes.push(keys::scheduled(
            n,
            endpoint,
            message.scheduled_enqueue_at.unwrap(),
            s,
        )),
        MessageState::Locked {
            locked_until,
            origin,
            ..
        } => {
            assert_ne!(origin, DeliveryOrigin::Scheduled);
            indexes.push(keys::lock(n, endpoint, locked_until, s));
        }
    }
    for key in indexes {
        assert!(rows.insert(physical(key), Vec::new()).is_none());
    }
    rows
}

fn bytes(rows: &BTreeMap<Key, Value>) -> u64 {
    rows.iter()
        .try_fold(0u64, |sum, (k, v)| {
            sum.checked_add(u64::try_from(k.len() + v.len()).unwrap())
        })
        .unwrap()
}

/// Independently enumerates model1 peak states, deliberately not its helper.
pub fn ceiling(owner: &CapacityOwner, endpoint: &EntityPath, original: &MessageRecord) -> u64 {
    let mut record = original.clone();
    record.sequence = SequenceNumber::new(u64::MAX);
    record.enqueued_at = Timestamp::from_millis(u64::MAX);
    record.scheduled_enqueue_at = record
        .scheduled_enqueue_at
        .map(|_| Timestamp::from_millis(u64::MAX));
    record.expires_at = record.expires_at.map(|_| Timestamp::from_millis(u64::MAX));
    record.delivery_count = u32::MAX;
    if let Some(info) = &mut record.dead_letter {
        info.dead_lettered_at = Timestamp::from_millis(u64::MAX);
    }
    let mut endpoints = vec![(endpoint.clone(), record.clone())];
    if endpoint == owner.parent() {
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
            let mut shadow = record.clone();
            shadow.expires_at = None;
            shadow.dead_letter = Some(DeadLetterInfo {
                reason,
                description: description.into(),
                dead_lettered_at: Timestamp::from_millis(u64::MAX),
            });
            endpoints.push((owner.shadow().clone(), shadow));
        }
    }
    let mut peak = 0;
    for (endpoint, mut candidate) in endpoints {
        let mut states = vec![MessageState::Ready, MessageState::Deferred];
        for origin in [DeliveryOrigin::Ready, DeliveryOrigin::Deferred] {
            states.push(MessageState::Locked {
                token: LockToken::new(u64::MAX),
                locked_until: Timestamp::from_millis(u64::MAX),
                origin,
            });
        }
        if &endpoint == owner.parent() && candidate.scheduled_enqueue_at.is_some() {
            states.push(MessageState::Scheduled);
        }
        for state in states {
            candidate.state = state;
            peak = peak.max(bytes(&image(owner, &endpoint, &candidate)));
        }
    }
    peak
}

pub fn observe(rows: &[(Key, Value)], owner: &CapacityOwner) -> Observation {
    compute(rows, owner, true)
}

pub fn recompute(rows: &[(Key, Value)], owner: &CapacityOwner) -> Observation {
    compute(rows, owner, false)
}

fn compute(rows: &[(Key, Value)], owner: &CapacityOwner, compare_ledger: bool) -> Observation {
    assert!(rows.windows(2).all(|w| w[0].0 < w[1].0));
    let all: BTreeMap<_, _> = rows.iter().cloned().collect();
    let parent = capacity_fence::decode_profile(
        owner,
        owner.parent(),
        &all[&keys::queue_config(owner.namespace(), owner.parent())],
    )
    .unwrap();
    let shadow = capacity_fence::decode_profile(
        owner,
        owner.shadow(),
        &all[&keys::queue_config(owner.namespace(), owner.shadow())],
    )
    .unwrap();
    assert_eq!(
        shadow,
        domain::QueueConfig {
            max_delivery_count: u32::MAX,
            default_time_to_live_millis: None,
            requires_session: false,
            requires_duplicate_detection: false,
            dead_lettering_on_message_expiration: true,
            capacity: QueueCapacity::NonFinite,
            ..parent
        }
    );
    assert!(all.contains_key(&keys::entity_metadata(owner.namespace(), owner.parent())));
    assert!(!all.contains_key(&keys::entity_metadata(owner.namespace(), owner.shadow())));
    let mut charged = BTreeMap::new();
    let mut messages = BTreeMap::new();
    let mut counters = BTreeMap::new();
    for (key, value) in rows {
        if key.first() != Some(&0x13) {
            continue;
        }
        // Parse actual bytes, not the production isolated-key parser.
        let original = &key[1..];
        let mut parts = original[1..].splitn(3, |b| *b == 0);
        let namespace = parts.next().unwrap();
        let endpoint = parts.next().unwrap();
        let suffix = parts.next().expect("terminated endpoint");
        assert_eq!(namespace, owner.namespace().as_str().as_bytes());
        let endpoint = if endpoint == owner.parent().as_str().as_bytes() {
            owner.parent()
        } else {
            assert_eq!(endpoint, owner.shadow().as_str().as_bytes());
            owner.shadow()
        };
        match original[0] {
            2 => {
                assert!(suffix.is_empty());
                let counter: QueueCounters = codec::decode(value).unwrap();
                assert_eq!(*value, codec::encode(&counter).unwrap());
                assert!(counters.insert(endpoint.clone(), counter).is_none());
            }
            3 => {
                assert_eq!(suffix.len(), 8);
                assert!(
                    matches!(value.first(), Some(1..=3)),
                    "unsupported owned message version"
                );
                let sequence = SequenceNumber::new(u64::from_be_bytes(suffix.try_into().unwrap()));
                let record = codec::message::decode(value).unwrap();
                assert_eq!(*value, codec::message::encode(&record).unwrap());
                assert_eq!(record.sequence, sequence);
                assert!(record.session_id.is_none());
                assert_eq!(endpoint == owner.shadow(), record.dead_letter.is_some());
                if endpoint == owner.shadow() {
                    assert!(record.expires_at.is_none());
                    assert_ne!(record.state, MessageState::Scheduled);
                }
                assert!(
                    messages
                        .insert(sequence, (endpoint.clone(), record))
                        .is_none(),
                    "parent/DLQ sequence alias"
                );
                charged.insert(key.clone(), value.clone());
            }
            4..=7 | 11 => {
                assert!(value.is_empty());
                charged.insert(key.clone(), value.clone());
            }
            other => panic!("unknown owned tag {other}"),
        }
    }
    let mut expected = BTreeMap::new();
    let mut ceilings = BTreeMap::new();
    let mut reserved = 0u64;
    let mut tokens = BTreeSet::new();
    for (sequence, (endpoint, record)) in &messages {
        assert!(
            sequence.as_u64() > 0
                && sequence.as_u64() < counters.get(owner.parent()).map_or(1, |c| c.next_sequence)
        );
        if let MessageState::Locked { token, .. } = record.state {
            assert!(
                token.as_u64() > 0
                    && token.as_u64() < counters.get(endpoint).map_or(1, |c| c.next_lock_token)
            );
            assert!(tokens.insert((endpoint.clone(), token)));
        }
        let group = image(owner, endpoint, record);
        let peak = ceiling(owner, endpoint, record);
        reserved = reserved
            .checked_add(peak.checked_sub(bytes(&group)).unwrap())
            .unwrap();
        ceilings.insert(*sequence, peak);
        for (key, value) in group {
            assert!(expected.insert(key, value).is_none());
        }
    }
    assert_eq!(
        charged, expected,
        "exact forward/reverse physical companion sets"
    );
    // No mixed legacy rows are allowed in this owner's endpoint scopes.
    for endpoint in [owner.parent(), owner.shadow()] {
        for tag in 2..=16 {
            let mut prefix = keys::queue_config(owner.namespace(), endpoint);
            prefix[0] = tag;
            assert!(!rows.iter().any(|(key, _)| key.starts_with(&prefix)));
        }
    }
    let independently_computed = RetainedUsage::new(
        owner.generation(),
        bytes(&charged),
        reserved,
        u64::try_from(messages.len()).unwrap(),
    )
    .unwrap();
    let mut ledger_key = vec![0x12];
    ledger_key.extend_from_slice(owner.namespace().as_str().as_bytes());
    ledger_key.push(0);
    ledger_key.extend_from_slice(owner.parent().as_str().as_bytes());
    ledger_key.push(0);
    let ledger = &all[&ledger_key];
    assert_eq!(ledger.len(), 72);
    assert_eq!(&ledger[4..8], &[0, 1, 0, 1]);
    if compare_ledger {
        assert_eq!(
            usage_codec::decode(owner, ledger).unwrap(),
            independently_computed
        );
    }
    let QueueCapacity::Finite { max_size_bytes } = parent.capacity else {
        panic!("not finite")
    };
    assert!(independently_computed.committed_bytes().unwrap() <= max_size_bytes);
    Observation {
        usage: independently_computed,
        ceilings,
        messages,
    }
}
