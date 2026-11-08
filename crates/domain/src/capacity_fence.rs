//! Inert compatibility framing for ordinary retained-byte owners.
//!
//! These pure codecs do not create, migrate or admit a finite queue. Runtime and
//! recovery deliberately refuse this state until lifecycle conservation exists.
//! All owner-local mutable data (including counters) uses an isolated keyspace:
//! a pre-ledger writer must not discover even stale indexes through old prefixes.

use sha2::{Digest, Sha256};
use storage::StateStore;

use crate::{
    BrokerError, DEAD_LETTER_QUEUE_SUFFIX, EntityBindingKind, EntityPath, NamespaceName,
    QueueCapacity, QueueConfig, codec, keys,
    retained_accounting::{AccountingError, CapacityOwner, keys::usage, usage_codec},
};

mod intent;
pub use intent::CapacityIntent;

pub const PROFILE_VERSION: u8 = 4;
pub const DATA_TAG: u8 = 0x13;
/// One extra byte on EVERY retained message/index key, not on message values.
pub const DATA_KEY_OVERHEAD: u64 = 1;
const PROFILE_HEADER: usize = 13;
const PROFILE_DIGEST: usize = 32;
const PROFILE_SCOPE: &[u8] = b"switchyard capacity profile v4\0";

/// Wrap a canonical counter or charged state key. Session/duplicate rows are
/// intentionally unsupported. Neither this function nor the profile codec writes.
pub fn data_key(key: &[u8]) -> Result<Vec<u8>, AccountingError> {
    if key.first() == Some(&2) {
        keys::catalog_entity_parts(key).ok_or(AccountingError::InvalidKey)?;
    } else if !matches!(key.first(), Some(3..=7 | 11)) || keys::state_parts(key).is_none() {
        return Err(AccountingError::InvalidKey);
    }
    let mut framed = Vec::with_capacity(key.len() + 1);
    framed.push(DATA_TAG);
    framed.extend_from_slice(key);
    Ok(framed)
}

pub fn original_key(key: &[u8]) -> Result<&[u8], AccountingError> {
    let original = key
        .strip_prefix(&[DATA_TAG])
        .ok_or(AccountingError::InvalidKey)?;
    if data_key(original)? != key {
        return Err(AccountingError::InvalidKey);
    }
    Ok(original)
}

fn profile_key(owner: &CapacityOwner, entity: &EntityPath) -> Result<Vec<u8>, AccountingError> {
    if entity != owner.parent() && entity != owner.shadow() {
        return Err(AccountingError::UnownedKey);
    }
    Ok(keys::queue_config(owner.namespace(), entity))
}

/// V4 wraps the exact historical profile plus a fixed generation, length and
/// key-bound digest. Both parent and publicly non-finite shadow MUST be wrapped.
pub fn encode_profile(
    owner: &CapacityOwner,
    entity: &EntityPath,
    config: &QueueConfig,
) -> Result<Vec<u8>, AccountingError> {
    let key = profile_key(owner, entity)?;
    config
        .validate()
        .map_err(|_| AccountingError::InvalidOwner)?;
    if config.requires_session
        || config.requires_duplicate_detection
        || (entity == owner.parent()) != matches!(config.capacity, QueueCapacity::Finite { .. })
    {
        return Err(AccountingError::InvalidOwner);
    }
    let inner = codec::queue::encode(config).map_err(|_| AccountingError::InvalidOwner)?;
    let length = u32::try_from(inner.len()).map_err(|_| AccountingError::Overflow)?;
    let mut result = vec![PROFILE_VERSION];
    result.extend_from_slice(&owner.generation().to_be_bytes());
    result.extend_from_slice(&length.to_be_bytes());
    result.extend_from_slice(&inner);
    result.extend_from_slice(&profile_digest(&key, &result));
    Ok(result)
}

pub fn decode_profile(
    owner: &CapacityOwner,
    entity: &EntityPath,
    bytes: &[u8],
) -> Result<QueueConfig, AccountingError> {
    let key = profile_key(owner, entity)?;
    if bytes.len() < PROFILE_HEADER + PROFILE_DIGEST || bytes[0] != PROFILE_VERSION {
        return Err(AccountingError::UnsupportedVersion);
    }
    let end = bytes.len() - PROFILE_DIGEST;
    if bytes[1..9] != owner.generation().to_be_bytes()
        || u32::from_be_bytes(bytes[9..13].try_into().expect("fixed length")) as usize
            != end - PROFILE_HEADER
        || bytes[end..] != profile_digest(&key, &bytes[..end])
    {
        return Err(AccountingError::CorruptUsage);
    }
    let config = codec::queue::decode(&bytes[PROFILE_HEADER..end])
        .map_err(|_| AccountingError::InvalidOwner)?;
    if encode_profile(owner, entity, &config)? != bytes {
        return Err(AccountingError::InvalidOwner);
    }
    Ok(config)
}

fn profile_digest(key: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(PROFILE_SCOPE);
    hash.update(key);
    hash.update(bytes);
    hash.finalize().into()
}

/// A selected-owner preflight, before Clock, handlers, counters or batches.
/// Legacy V3 remains metadata-only. Usage or isolated data never adopts it.
/// Proposers must run this before timestamp selection, and application rechecks
/// it under the same external write exclusion. This is not authority, admission
/// or a health certificate that can be transferred to another store image.
pub fn check_command<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    entity: &EntityPath,
) -> Result<(), BrokerError> {
    check_selected(store, namespace, entity, true)
}

/// Version/usage point reads only, before retained-authority validation. Legacy
/// stale/corrupt bindings retain their scan-free refusal ordering. A successful
/// binding still runs the complete command preflight, including orphan data.
/// Proposers follow this with existing binding validation, then [`check_command`]
/// before reading either host time or persisted Clock. It is not binding validation.
pub fn check_bound_profile<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    entity: &EntityPath,
) -> Result<(), BrokerError> {
    check_selected(store, namespace, entity, false)
}

fn check_selected<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    entity: &EntityPath,
    scan_isolated: bool,
) -> Result<(), BrokerError> {
    let path = entity
        .as_str()
        .strip_suffix(DEAD_LETTER_QUEUE_SUFFIX)
        .unwrap_or(entity.as_str());
    let Ok(parent) = EntityPath::new(path) else {
        return Ok(());
    };
    let Ok(probe) = CapacityOwner::new(namespace.clone(), parent, 1) else {
        return Ok(());
    };
    let raw_parent = store.get(&keys::queue_config(namespace, probe.parent()))?;
    let raw_shadow = store.get(&keys::queue_config(namespace, probe.shadow()))?;
    let raw_usage = store.get(&usage(&probe))?;
    let mut isolated = false;
    if scan_isolated {
        for endpoint in [probe.parent(), probe.shadow()] {
            for tag in 2..=13 {
                let mut prefix = vec![DATA_TAG, tag];
                prefix.extend_from_slice(&keys::queue_config(namespace, endpoint)[1..]);
                isolated |= !store.scan_prefix(&prefix, 1)?.is_empty();
            }
        }
    }
    let fenced = [&raw_parent, &raw_shadow].iter().any(|raw| {
        raw.as_ref()
            .is_some_and(|bytes| bytes.first() == Some(&PROFILE_VERSION))
    });
    if !fenced && raw_usage.is_none() && !isolated {
        return Ok(());
    }
    let raw_head = store
        .get(&keys::entity_metadata(namespace, probe.parent()))?
        .ok_or(BrokerError::CapacityStateCorrupt)?;
    let generation = crate::machine::decode_catalog_owner(&raw_head, EntityBindingKind::Queue)
        .map_err(|_| BrokerError::CapacityStateCorrupt)?;
    let owner = CapacityOwner::new(namespace.clone(), probe.parent().clone(), generation)
        .map_err(|_| BrokerError::CapacityStateCorrupt)?;
    let parent = decode_profile(
        &owner,
        owner.parent(),
        raw_parent.as_deref().unwrap_or_default(),
    )
    .map_err(|_| BrokerError::CapacityStateCorrupt)?;
    let shadow = decode_profile(
        &owner,
        owner.shadow(),
        raw_shadow.as_deref().unwrap_or_default(),
    )
    .map_err(|_| BrokerError::CapacityStateCorrupt)?;
    if shadow != parent.dead_letter_shadow() {
        return Err(BrokerError::CapacityStateCorrupt);
    }
    let raw_usage = raw_usage.ok_or(BrokerError::CapacityUsageMissing)?;
    usage_codec::decode(&owner, &raw_usage).map_err(|_| BrokerError::CapacityStateCorrupt)?;
    // No lifecycle writer exists yet, even for an empty owner with valid usage.
    Err(BrokerError::CapacityStateUnsupported)
}

/// Offline recovery may not preserve opaque new-owned state without proving
/// reservations. Refuse the entire recovery image before any mutation instead.
/// This is deliberately separate from the selected-owner runtime preflight.
pub fn check_recovery<S: StateStore>(store: &S) -> Result<(), BrokerError> {
    for tag in [0x12, DATA_TAG] {
        if !store.scan_prefix(&[tag], 1)?.is_empty() {
            return Err(BrokerError::CapacityStateUnsupported);
        }
    }
    let prefix = keys::queue_config_prefix();
    let mut cursor = prefix.clone();
    loop {
        let page = store.scan_from(&prefix, &cursor, 32)?;
        if page.is_empty() {
            return Ok(());
        }
        for (_, raw) in &page {
            if raw.first() == Some(&PROFILE_VERSION) {
                return Err(BrokerError::CapacityStateUnsupported);
            }
        }
        cursor = page.last().expect("nonempty page").0.clone();
        cursor.push(0);
    }
}
