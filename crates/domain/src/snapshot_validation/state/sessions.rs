//! Session ownership checks composed after the unchanged message facet.

use storage::{Key, Value};
use thiserror::Error;

use crate::{
    EntityPath, NamespaceName,
    keys::{self, StateKey},
};

use super::{Facts, MessageRowsValidation, Profile, SnapshotStateError, StateValue, messages};
use crate::snapshot_validation::CatalogValidation;

/// Checked message/session observations, not complete state or recovery health.
/// Duplicate generations, allocation and external F0/F1 relationships remain
/// pending even when their observed row counts are zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionRowsValidation {
    messages: MessageRowsValidation,
    session_rows: usize,
    session_lock_rows: usize,
    session_ready_rows: usize,
}

impl SessionRowsValidation {
    pub fn catalog(&self) -> CatalogValidation {
        self.messages.catalog()
    }
    pub fn message_rows(&self) -> usize {
        self.messages.message_rows()
    }
    /// Includes 09 routing rows, which are also counted by session_ready_rows.
    pub fn message_index_rows(&self) -> usize {
        self.messages.message_index_rows()
    }
    pub fn session_rows(&self) -> usize {
        self.session_rows
    }
    pub fn session_lock_rows(&self) -> usize {
        self.session_lock_rows
    }
    pub fn session_ready_rows(&self) -> usize {
        self.session_ready_rows
    }
    pub fn pending_duplicate_rows(&self) -> usize {
        self.messages.pending_duplicate_rows()
    }
    pub fn pending_counter_rows(&self) -> usize {
        self.messages.pending_counter_rows()
    }
    pub fn pending_external_rows(&self) -> usize {
        self.catalog().unvalidated_external_rows()
    }
    pub fn duplicate_relations_checked(&self) -> bool {
        false
    }
    pub fn allocation_relations_checked(&self) -> bool {
        false
    }
    pub fn external_relations_checked(&self) -> bool {
        false
    }
}

/// Refusals preserve the existing catalog/canonical/message phase and errors.
/// Session relation ordinals refer to the original complete input slice.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SnapshotSessionError {
    #[error(transparent)]
    State(#[from] SnapshotStateError),
    #[error("snapshot row {row} has an inconsistent session relation: {detail}")]
    InconsistentSession { row: usize, detail: &'static str },
}

/// Reads facts once from this immutable image, checks messages, then sessions.
/// Absent session records are implicit unlocked sessions; elapsed locks retain
/// their exact index until a supported transition clears them. No Clock clamp,
/// session-state interpretation, sweeping, StateStore access or capture occurs.
/// Temporary facts are proportional to input, not a hostile-heap bound.
pub fn validate_session_rows(
    records: &[(Key, Value)],
) -> Result<SessionRowsValidation, SnapshotSessionError> {
    let facts = Facts::read(records)?;
    messages::validate(&facts)?;
    validate(&facts)?;
    let mut report = SessionRowsValidation {
        messages: facts.report(),
        session_rows: 0,
        session_lock_rows: 0,
        session_ready_rows: 0,
    };
    for row in &facts.rows {
        match &row.value.key {
            StateKey::Session(..) => report.session_rows += 1,
            StateKey::SessionLock(..) => report.session_lock_rows += 1,
            StateKey::SessionReady(..) => report.session_ready_rows += 1,
            _ => {}
        }
    }
    Ok(report)
}

fn validate(facts: &Facts<'_>) -> Result<(), SnapshotSessionError> {
    // A session record's exact companion wins before any reverse orphan.
    for row in &facts.rows {
        match &row.value.key {
            StateKey::Session(namespace, entity, session) => {
                require_owner(facts, namespace, entity, row.ordinal)?;
                let StateValue::Session(record) = &row.value.value else {
                    return Err(inconsistent(row.ordinal, "session has no decoded record"));
                };
                if let Some(lock) = record.lock {
                    let expected =
                        keys::session_lock(namespace, entity, lock.locked_until, session);
                    if facts.row(&expected).is_none() {
                        return Err(inconsistent(
                            row.ordinal,
                            "session is missing its exact lock index",
                        ));
                    }
                }
            }
            StateKey::SessionReady(namespace, entity, ..) => {
                // The message facet already checked exact session/sequence routing.
                // No 08 record or live session lock is required for readiness.
                require_owner(facts, namespace, entity, row.ordinal)?;
            }
            _ => {}
        }
    }
    for row in &facts.rows {
        let StateKey::SessionLock(namespace, entity, deadline, session) = &row.value.key else {
            continue;
        };
        require_owner(facts, namespace, entity, row.ordinal)?;
        let primary = facts
            .row(&keys::session(namespace, entity, session))
            .ok_or_else(|| inconsistent(row.ordinal, "session lock has no exact session record"))?;
        let StateValue::Session(record) = &primary.value.value else {
            return Err(inconsistent(
                row.ordinal,
                "session lock has no decoded session",
            ));
        };
        if !record
            .lock
            .is_some_and(|lock| lock.locked_until == *deadline)
        {
            return Err(inconsistent(
                row.ordinal,
                "session lock differs from its record deadline",
            ));
        }
    }
    Ok(())
}

fn require_owner(
    facts: &Facts<'_>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    row: usize,
) -> Result<(), SnapshotSessionError> {
    let owner = facts
        .owners
        .get(&(namespace.clone(), entity.clone()))
        .ok_or_else(|| inconsistent(row, "session has no catalog endpoint"))?;
    if !matches!(&owner.value.profile, Profile::Queue(config) if config.requires_session)
        || owner.value.shadow
        || owner.value.subscription
    {
        return Err(inconsistent(row, "session endpoint is not a session queue"));
    }
    Ok(())
}

fn inconsistent(row: usize, detail: &'static str) -> SnapshotSessionError {
    SnapshotSessionError::InconsistentSession { row, detail }
}
