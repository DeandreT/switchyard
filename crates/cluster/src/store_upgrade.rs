//! Explicit offline outer-store upgrade from format 2 to format 3.
//!
//! One immutable native image is checked by the domain's bounded legacy-content
//! preflight and, only where those namespaces are present, by the existing
//! journal (F0) and indexed-owner (F1) structural validators. Populated
//! unindexed stores stay eligible: complete snapshot eligibility, indexed
//! adoption and full-state health are not required or certified. Every refusal
//! precedes the single marker commit, and no record is re-encoded, replayed,
//! normalized or migrated. Historical proposal intent is decoded unchanged and
//! never executed, so intentionally refused input does not block the upgrade.
//!
//! The caller must exclude every writer, raw clone, keyspace/topology mutator and
//! other owner of the directory for the complete call. A returned storage error
//! from the commit, or an escaping panic, leaves the marker at either version;
//! close everything and reopen to observe which. Nothing is retried.

use std::{path::Path, sync::Arc};

use domain::{
    IndexedApplyError, IndexedWriter,
    legacy_content::{LegacyContentError, validate_legacy_content},
};
use storage::{
    SnapshotWithProvenance, StoreFormatCommitError, StoreFormatInspection, StoreFormatUpgradeError,
    StoreFormatUpgradeOwner,
};
use thiserror::Error;

use crate::{
    CommittedReplay, Journal, JournalError, ReplayError, journal::validate_snapshot_namespace,
    snapshot::FrozenImage,
};

/// Operational result only, not a health, recovery or durability certificate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreUpgradeOutcome {
    /// The version 2 marker was replaced by version 3; records are unchanged.
    Upgraded,
    /// The store was already version 3; nothing was validated or written.
    AlreadyCurrent,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum StoreUpgradeError {
    #[error(transparent)]
    Capacity(#[from] domain::BrokerError),
    #[error(transparent)]
    Storage(#[from] StoreFormatUpgradeError),
    #[error(transparent)]
    Content(#[from] LegacyContentError),
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Indexed(#[from] IndexedApplyError),
    #[error(transparent)]
    Replay(#[from] ReplayError),
}

/// Upgrades the existing store in `directory`, never creating one.
pub fn upgrade_store(
    directory: impl AsRef<Path>,
) -> Result<StoreUpgradeOutcome, StoreUpgradeError> {
    let mut owner = StoreFormatUpgradeOwner::open_existing(directory)?;
    let prepared = match owner.inspect()? {
        StoreFormatInspection::AlreadyCurrent => return Ok(StoreUpgradeOutcome::AlreadyCurrent),
        StoreFormatInspection::Eligible(prepared) => prepared,
    };
    prepared
        .commit_validated(validate_upgrade_image)
        .map_err(|error| match error {
            StoreFormatCommitError::Validation(error) => error,
            StoreFormatCommitError::Upgrade(error) => error.into(),
        })?;
    Ok(StoreUpgradeOutcome::Upgraded)
}

/// Compatibility preflight of exactly the inspected image, without live reads.
fn validate_upgrade_image(image: &Arc<SnapshotWithProvenance>) -> Result<(), StoreUpgradeError> {
    domain::capacity_fence::check_recovery(&FrozenImage(Arc::clone(image)))?;
    let records = image.snapshot().entries();
    validate_legacy_content(records)?;
    let mut journal = false;
    let mut indexed = false;
    for (key, _) in records {
        validate_snapshot_namespace(key)?;
        journal |= key.first() == Some(&0xF0);
        indexed |= key.first() == Some(&0xF1);
    }
    let frozen = FrozenImage(Arc::clone(image));
    match (journal, indexed) {
        // Committed schemas, anchors and the latest checkpoint identity.
        (true, true) => drop(CommittedReplay::open(frozen)?),
        (true, false) => {
            let journal = Journal::open(frozen)?;
            let mut cursor = journal.removed_through()?;
            let committed = journal.committed_index()?;
            while cursor < committed {
                let entries = journal
                    .read_committed(cursor + 1, crate::journal::JOURNAL_VALIDATION_PAGE_ENTRIES)?;
                if entries.is_empty() {
                    return Err(JournalError::Corrupt {
                        detail: "committed upgrade page is empty",
                    }
                    .into());
                }
                for entry in entries {
                    // The journal-only upgrade intentionally does not certify
                    // ordinary proposal semantics, but cannot relabel new
                    // capacity-owned intent as supported legacy content.
                    if entry.payload().get(..8) == Some(b"SWDP\0\0\0\x0b") {
                        return Err(domain::BrokerError::CapacityStateUnsupported.into());
                    }
                    cursor = entry.index();
                }
            }
        }
        // The indexed owner without journal history; domain rows stay unadopted.
        (false, true) => drop(IndexedWriter::open(frozen)?),
        (false, false) => {}
    }
    Ok(())
}
