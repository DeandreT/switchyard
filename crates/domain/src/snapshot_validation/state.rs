//! Same-image facts for independently composed state validation facets.

use std::collections::BTreeMap;

use serde::{Serialize, de::DeserializeOwned};
use storage::{Key, Value};
use thiserror::Error;

use crate::{
    EntityBindingKind, EntityPath, MessageRecord, NamespaceName, QueueConfig, QueueCounters,
    SessionRecord, Timestamp, TopicConfig, codec,
    keys::{self, StateKey},
};

use super::{CatalogValidation, SnapshotCatalogError, validate_catalog};

mod duplicates;
mod messages;
mod sessions;

pub use duplicates::{DuplicateRowsValidation, SnapshotDuplicateError, validate_duplicate_rows};
pub use sessions::{SessionRowsValidation, SnapshotSessionError, validate_session_rows};

pub(super) type Scope = (NamespaceName, EntityPath);

/// Observations of message graphs only, never a complete-state certificate.
/// Session ownership, duplicate generations, allocation and F0/F1 are pending.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MessageRowsValidation {
    catalog: CatalogValidation,
    message_rows: usize,
    message_index_rows: usize,
    pending_session_rows: usize,
    pending_duplicate_rows: usize,
    pending_counter_rows: usize,
}

impl MessageRowsValidation {
    pub fn catalog(&self) -> CatalogValidation {
        self.catalog
    }
    pub fn message_rows(&self) -> usize {
        self.message_rows
    }
    /// Includes 09 routing to a message, not session ownership validation.
    pub fn message_index_rows(&self) -> usize {
        self.message_index_rows
    }
    /// Canonical 08/0A forms whose relationships remain unvalidated.
    pub fn pending_session_rows(&self) -> usize {
        self.pending_session_rows
    }
    /// Canonical 0C/0D forms whose generations remain unvalidated.
    pub fn pending_duplicate_rows(&self) -> usize {
        self.pending_duplicate_rows
    }
    /// Present counter forms; even zero rows do not certify allocation relations.
    pub fn pending_counter_rows(&self) -> usize {
        self.pending_counter_rows
    }
    pub fn allocation_relations_checked(&self) -> bool {
        false
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SnapshotStateError {
    #[error(transparent)]
    Catalog(#[from] SnapshotCatalogError),
    #[error("snapshot row {row} has an invalid state key: {detail}")]
    InvalidKey { row: usize, detail: &'static str },
    #[error("snapshot row {row} has an invalid state value: {detail}")]
    InvalidValue { row: usize, detail: &'static str },
    #[error("snapshot row {row} has an inconsistent message relation: {detail}")]
    InconsistentMessage { row: usize, detail: &'static str },
}

pub(super) struct Located<'a, T> {
    pub(super) ordinal: usize,
    pub(super) key: &'a [u8],
    #[allow(
        dead_code,
        reason = "Original values stay borrowed for later state facets."
    )]
    pub(super) raw: &'a [u8],
    pub(super) value: T,
}

#[derive(Clone)]
pub(super) enum Profile {
    Queue(QueueConfig),
    #[allow(
        dead_code,
        reason = "The topic profile is a shared fact for state allocation."
    )]
    Topic(TopicConfig),
}

pub(super) struct OwnerFacts {
    pub(super) profile: Profile,
    /// Subscription sequences come from the topic; DLQ sequences from its parent.
    #[allow(dead_code, reason = "Allocation relations are deliberately pending.")]
    pub(super) sequence_owner: Scope,
    pub(super) shadow: bool,
    pub(super) subscription: bool,
}

pub(super) enum StateValue {
    Message(Box<MessageRecord>),
    #[allow(dead_code, reason = "Decoded once for the pending session facet.")]
    Session(SessionRecord),
    #[allow(dead_code, reason = "Decoded once for the pending duplicate facet.")]
    Duplicate(Timestamp),
    Empty,
}

pub(super) struct StateRow {
    pub(super) key: StateKey,
    pub(super) value: StateValue,
}

pub(super) struct Facts<'a> {
    pub(super) catalog: CatalogValidation,
    pub(super) owners: BTreeMap<Scope, Located<'a, OwnerFacts>>,
    pub(super) counters: BTreeMap<Scope, Located<'a, QueueCounters>>,
    /// Catalog has already validated these original owner-head generations.
    #[allow(
        dead_code,
        reason = "Original head observations are retained for later composition."
    )]
    pub(super) heads: BTreeMap<Scope, Located<'a, u64>>,
    pub(super) rows: Vec<Located<'a, StateRow>>,
    pub(super) by_key: BTreeMap<&'a [u8], usize>,
}

/// Checks canonical state forms and complete message/index relationships on the
/// same image as catalog validation. Expired/due rows are not swept, opaque
/// body/envelope/session bytes are not interpreted, and current profile limits
/// are not reapplied to retained messages. Temporary facts are proportional to
/// input, not a hostile-heap bound. No StateStore read, write or capture occurs.
pub fn validate_message_rows(
    records: &[(Key, Value)],
) -> Result<MessageRowsValidation, SnapshotStateError> {
    let facts = Facts::read(records)?;
    messages::validate(&facts)?;
    Ok(facts.report())
}

impl<'a> Facts<'a> {
    fn read(records: &'a [(Key, Value)]) -> Result<Self, SnapshotStateError> {
        // Keep the public catalog error order unchanged, on this exact slice.
        let catalog = validate_catalog(records)?;
        let mut facts = Self {
            catalog,
            owners: BTreeMap::new(),
            counters: BTreeMap::new(),
            heads: BTreeMap::new(),
            rows: Vec::new(),
            by_key: BTreeMap::new(),
        };
        for (ordinal, (key, raw)) in records.iter().enumerate() {
            match key[0] {
                0x01 | 0x0E => {
                    let scope =
                        keys::catalog_entity_parts(key).ok_or_else(|| invalid_key(ordinal))?;
                    let profile = if key[0] == 0x01 {
                        Profile::Queue(decode(raw, ordinal)?)
                    } else {
                        Profile::Topic(decode(raw, ordinal)?)
                    };
                    let shadow = scope.1.is_dead_letter_queue();
                    let subscription = scope.1.is_subscription();
                    let owner_path = scope
                        .1
                        .as_str()
                        .strip_suffix(crate::DEAD_LETTER_QUEUE_SUFFIX)
                        .unwrap_or(scope.1.as_str());
                    let sequence_owner_path = owner_path
                        .split_once(crate::identifier::SUBSCRIPTION_PATH_SEGMENT)
                        .map_or(owner_path, |(topic, _)| topic);
                    let sequence_owner = (
                        scope.0.clone(),
                        EntityPath::from_internal(sequence_owner_path)
                            .map_err(|_| invalid_key(ordinal))?,
                    );
                    facts.owners.insert(
                        scope,
                        Located {
                            ordinal,
                            key,
                            raw,
                            value: OwnerFacts {
                                profile,
                                sequence_owner,
                                shadow,
                                subscription,
                            },
                        },
                    );
                }
                0x02 => {
                    let scope =
                        keys::catalog_entity_parts(key).ok_or_else(|| invalid_key(ordinal))?;
                    facts.counters.insert(
                        scope,
                        Located {
                            ordinal,
                            key,
                            raw,
                            value: decode(raw, ordinal)?,
                        },
                    );
                }
                0x11 => {
                    let scope =
                        keys::catalog_entity_parts(key).ok_or_else(|| invalid_key(ordinal))?;
                    let owner = facts
                        .owners
                        .get(&scope)
                        .ok_or_else(|| invalid_key(ordinal))?;
                    let kind = match &owner.value.profile {
                        Profile::Topic(_) => EntityBindingKind::Topic,
                        Profile::Queue(_) if owner.value.subscription => {
                            EntityBindingKind::Subscription
                        }
                        Profile::Queue(_) => EntityBindingKind::Queue,
                    };
                    let generation = crate::machine::decode_catalog_owner(raw, kind)
                        .map_err(|_| invalid_value(ordinal, "owner head cannot be decoded"))?;
                    facts.heads.insert(
                        scope,
                        Located {
                            ordinal,
                            key,
                            raw,
                            value: generation,
                        },
                    );
                }
                0x03..=0x0D => {
                    let parsed = keys::state_parts(key).ok_or_else(|| invalid_key(ordinal))?;
                    let value = match &parsed {
                        StateKey::Message(..) => {
                            StateValue::Message(Box::new(decode(raw, ordinal)?))
                        }
                        StateKey::Session(..) => StateValue::Session(decode(raw, ordinal)?),
                        StateKey::DuplicateId(..) => StateValue::Duplicate(decode(raw, ordinal)?),
                        _ if raw.is_empty() => StateValue::Empty,
                        _ => return Err(invalid_value(ordinal, "index value is not empty")),
                    };
                    facts.by_key.insert(key, facts.rows.len());
                    facts.rows.push(Located {
                        ordinal,
                        key,
                        raw,
                        value: StateRow { key: parsed, value },
                    });
                }
                _ => {}
            }
        }
        Ok(facts)
    }

    fn report(&self) -> MessageRowsValidation {
        let mut report = MessageRowsValidation {
            catalog: self.catalog,
            message_rows: 0,
            message_index_rows: 0,
            pending_session_rows: 0,
            pending_duplicate_rows: 0,
            pending_counter_rows: self.counters.len(),
        };
        for row in &self.rows {
            match &row.value.key {
                StateKey::Message(..) => report.message_rows += 1,
                StateKey::Session(..) | StateKey::SessionLock(..) => {
                    report.pending_session_rows += 1
                }
                StateKey::DuplicateId(..) | StateKey::DuplicateExpiry(..) => {
                    report.pending_duplicate_rows += 1
                }
                _ => report.message_index_rows += 1,
            }
        }
        report
    }

    pub(super) fn row(&self, key: &[u8]) -> Option<&Located<'a, StateRow>> {
        self.by_key.get(key).map(|index| &self.rows[*index])
    }
}

fn decode<T: DeserializeOwned + Serialize>(
    raw: &[u8],
    row: usize,
) -> Result<T, SnapshotStateError> {
    let value =
        codec::decode(raw).map_err(|_| invalid_value(row, "state value cannot be decoded"))?;
    if codec::encode(&value).map_err(|_| invalid_value(row, "state value cannot be encoded"))?
        != raw
    {
        return Err(invalid_value(row, "state value is not canonical"));
    }
    Ok(value)
}

fn invalid_key(row: usize) -> SnapshotStateError {
    SnapshotStateError::InvalidKey {
        row,
        detail: "state key is not canonical",
    }
}

fn invalid_value(row: usize, detail: &'static str) -> SnapshotStateError {
    SnapshotStateError::InvalidValue { row, detail }
}
