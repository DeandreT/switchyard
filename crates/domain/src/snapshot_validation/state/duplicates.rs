//! Duplicate-history checks composed after the unchanged message facet.

use storage::{Key, Value};
use thiserror::Error;

use crate::{
    EntityPath, NamespaceName,
    keys::{self, StateKey},
    snapshot_validation::CatalogValidation,
};

use super::{Facts, MessageRowsValidation, Profile, SnapshotStateError, StateValue, messages};

/// Checked message/history observations, not complete state or recovery health.
/// Session ownership, allocation and external F0/F1 relationships remain pending
/// even when their observed row counts are zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DuplicateRowsValidation {
    messages: MessageRowsValidation,
    lookup_rows: usize,
    expiry_rows: usize,
    stale_compatibility_rows: usize,
}

impl DuplicateRowsValidation {
    pub fn catalog(&self) -> CatalogValidation {
        self.messages.catalog()
    }
    pub fn message_rows(&self) -> usize {
        self.messages.message_rows()
    }
    pub fn message_index_rows(&self) -> usize {
        self.messages.message_index_rows()
    }
    pub fn lookup_rows(&self) -> usize {
        self.lookup_rows
    }
    pub fn expiry_rows(&self) -> usize {
        self.expiry_rows
    }
    /// Extra elapsed older expiry rows accepted by the pinned compatibility rule.
    /// This count is not evidence that ordinary transitions emitted those rows.
    pub fn stale_compatibility_rows(&self) -> usize {
        self.stale_compatibility_rows
    }
    pub fn pending_session_rows(&self) -> usize {
        self.messages.pending_session_rows()
    }
    pub fn pending_counter_rows(&self) -> usize {
        self.messages.pending_counter_rows()
    }
    pub fn pending_external_rows(&self) -> usize {
        self.catalog().unvalidated_external_rows()
    }
    pub fn session_relations_checked(&self) -> bool {
        false
    }
    pub fn allocation_relations_checked(&self) -> bool {
        false
    }
    pub fn external_relations_checked(&self) -> bool {
        false
    }
}

/// The original catalog/canonical/message errors retain their existing priority.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SnapshotDuplicateError {
    #[error(transparent)]
    State(#[from] SnapshotStateError),
    #[error("snapshot row {row} has an inconsistent duplicate relation: {detail}")]
    InconsistentDuplicate { row: usize, detail: &'static str },
}

/// Checks the same immutable image once, with messages before duplicate history.
/// Every current lookup requires its exact expiry; no extant message is required.
/// Extra expiries are compatible only beside that same lookup when strictly
/// older than its current deadline and elapsed at the image's canonical Clock.
/// They are qualified compatibility observations, not natural-history evidence.
/// No window rebasing, sweeping, StateStore access, capture or repair occurs.
/// Temporary facts are proportional to input, not a hostile-heap bound.
pub fn validate_duplicate_rows(
    records: &[(Key, Value)],
) -> Result<DuplicateRowsValidation, SnapshotDuplicateError> {
    let facts = Facts::read(records)?;
    messages::validate(&facts)?;
    let stale_compatibility_rows = validate(&facts)?;
    let mut report = DuplicateRowsValidation {
        messages: facts.report(),
        lookup_rows: 0,
        expiry_rows: 0,
        stale_compatibility_rows,
    };
    for row in &facts.rows {
        match &row.value.key {
            StateKey::DuplicateId(..) => report.lookup_rows += 1,
            StateKey::DuplicateExpiry(..) => report.expiry_rows += 1,
            _ => {}
        }
    }
    Ok(report)
}

fn validate(facts: &Facts<'_>) -> Result<usize, SnapshotDuplicateError> {
    // Current companions are mandatory before any stale reverse-row tolerance.
    for row in &facts.rows {
        let StateKey::DuplicateId(namespace, entity, id) = &row.value.key else {
            continue;
        };
        require_owner(facts, namespace, entity, id, row.ordinal)?;
        let StateValue::Duplicate(deadline) = &row.value.value else {
            return Err(inconsistent(row.ordinal, "lookup has no decoded deadline"));
        };
        if facts
            .row(&keys::duplicate_expiry(namespace, entity, *deadline, id))
            .is_none()
        {
            return Err(inconsistent(
                row.ordinal,
                "lookup is missing its exact current expiry",
            ));
        }
    }
    let mut stale = 0;
    for row in &facts.rows {
        let StateKey::DuplicateExpiry(namespace, entity, deadline, id) = &row.value.key else {
            continue;
        };
        require_owner(facts, namespace, entity, id, row.ordinal)?;
        let primary = facts
            .row(&keys::duplicate_id(namespace, entity, id))
            .ok_or_else(|| inconsistent(row.ordinal, "expiry has no exact current lookup"))?;
        let StateValue::Duplicate(current) = &primary.value.value else {
            return Err(inconsistent(
                row.ordinal,
                "expiry has no decoded current lookup",
            ));
        };
        if deadline == current {
            continue;
        }
        if deadline < current
            && facts
                .catalog
                .clock()
                .is_some_and(|clock| *deadline <= clock)
        {
            stale += 1;
        } else {
            return Err(inconsistent(
                row.ordinal,
                "expiry is neither current nor elapsed older compatibility",
            ));
        }
    }
    Ok(stale)
}

fn require_owner(
    facts: &Facts<'_>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    id: &str,
    row: usize,
) -> Result<(), SnapshotDuplicateError> {
    let owner = facts
        .owners
        .get(&(namespace.clone(), entity.clone()))
        .ok_or_else(|| inconsistent(row, "duplicate history has no catalog endpoint"))?;
    if !matches!(&owner.value.profile, Profile::Queue(config) if config.requires_duplicate_detection)
        || owner.value.shadow
        || owner.value.subscription
    {
        return Err(inconsistent(
            row,
            "duplicate history endpoint is not an enabled ordinary queue",
        ));
    }
    if id.is_empty() {
        return Err(inconsistent(
            row,
            "anonymous identifiers do not own duplicate history",
        ));
    }
    Ok(())
}

fn inconsistent(row: usize, detail: &'static str) -> SnapshotDuplicateError {
    SnapshotDuplicateError::InconsistentDuplicate { row, detail }
}
