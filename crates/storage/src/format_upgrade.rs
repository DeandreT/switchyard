//! Explicit offline outer-store format upgrade, not migration or certification.
//!
//! The caller must exclude ALL writers, raw clones, keyspace/topology mutators,
//! and every other owner of the directory until completion; the directory lock
//! refuses only a second live database handle. The only mutation is the existing
//! metadata marker, from version 2 to 3, in one synchronous native batch. Record
//! bytes are never rewritten. Native recovery may still perform its own I/O, so
//! database files are not byte-identical and the image is not a health report.
//!
//! Storage is content-neutral and cannot certify the records itself: the marker
//! commit exists only as [`PreparedStoreFormatUpgrade::commit_validated`], which
//! runs the caller's validation on the bound image before any write.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use fjall::{Database, Keyspace, PersistMode};
use thiserror::Error;

use crate::{
    SnapshotWithProvenance, StorageError,
    durable::{
        ACTIVE_STORE_FORMAT, FORMAT_VERSION_KEY, STORE_FORMAT_V2, STORE_FORMAT_V3, inspect_native,
        recover_existing,
    },
};

/// A refusal from the caller's validation, or from the upgrade itself.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum StoreFormatCommitError<E> {
    #[error("store format upgrade validation refused the image: {0}")]
    Validation(E),
    #[error(transparent)]
    Upgrade(#[from] StoreFormatUpgradeError),
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum StoreFormatUpgradeError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("store format upgrade owner is retired; close all handles and reopen")]
    Retired,
    #[error("store format {found} has no explicit upgrade to format {target}")]
    UnsupportedSource { found: u32, target: u32 },
}

/// Non-Clone owner of one existing Fjall directory. It never creates a
/// database, keyspace or marker, and exposes no store or raw handle.
///
/// ```compile_fail
/// use storage::StoreFormatUpgradeOwner;
/// let owner = StoreFormatUpgradeOwner::open_existing("store").unwrap();
/// let duplicate = owner.clone();
/// ```
pub struct StoreFormatUpgradeOwner {
    database: Database,
    meta: Keyspace,
    records: Keyspace,
    directory: PathBuf,
    retired: bool,
    #[cfg(test)]
    fault: tests::Fault,
}

impl std::fmt::Debug for StoreFormatUpgradeOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoreFormatUpgradeOwner")
            .field("directory", &self.directory)
            .field("retired", &self.retired)
            .finish_non_exhaustive()
    }
}

/// Inspection outcome. `AlreadyCurrent` performs no write and certifies nothing
/// beyond the exact native metadata checks.
pub enum StoreFormatInspection<'a> {
    AlreadyCurrent,
    Eligible(PreparedStoreFormatUpgrade<'a>),
}

impl StoreFormatUpgradeOwner {
    /// Recovers an existing directory only. A missing directory, missing native
    /// version file or any keyspace inventory other than exactly `meta` and
    /// `records` refuses before a create-or-open keyspace lookup.
    pub fn open_existing(directory: impl AsRef<Path>) -> Result<Self, StoreFormatUpgradeError> {
        let directory = directory.as_ref().to_path_buf();
        let (database, meta, records) = recover_existing(&directory)?;
        Ok(Self {
            database,
            meta,
            records,
            directory,
            retired: false,
            #[cfg(test)]
            fault: tests::Fault::None,
        })
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn is_retired(&self) -> bool {
        self.retired
    }

    /// Checks inventory, original handle identity and the sole marker row, then
    /// reads the marker and every record through one native snapshot.
    ///
    /// Version 3 is already current; version 2 is eligible; every other marker
    /// refuses. Both outcomes other than eligibility are decided from the marker
    /// before any record is materialized. The eligible image is immutable and
    /// bound to this owner. Refusal and abandonment leave the owner usable.
    pub fn inspect(&mut self) -> Result<StoreFormatInspection<'_>, StoreFormatUpgradeError> {
        if self.retired {
            return Err(StoreFormatUpgradeError::Retired);
        }
        let mut found = 0;
        let image = inspect_native(&self.database, &self.meta, &self.records, |recorded| {
            found = u32::from_be_bytes(recorded.try_into().expect("checked marker width"));
            Ok(found == STORE_FORMAT_V2)
        })?;
        match (found, image) {
            (STORE_FORMAT_V2, Some(image)) => Ok(StoreFormatInspection::Eligible(
                PreparedStoreFormatUpgrade {
                    owner: self,
                    image: Arc::new(image),
                },
            )),
            (STORE_FORMAT_V3, _) => Ok(StoreFormatInspection::AlreadyCurrent),
            (found, _) => Err(StoreFormatUpgradeError::UnsupportedSource {
                found,
                target: ACTIVE_STORE_FORMAT,
            }),
        }
    }
}

/// Single-use, target-bound marker commit for one inspected version 2 image.
pub struct PreparedStoreFormatUpgrade<'a> {
    owner: &'a mut StoreFormatUpgradeOwner,
    image: Arc<SnapshotWithProvenance>,
}

impl PreparedStoreFormatUpgrade<'_> {
    /// The exact inspected image, including its actual version 2 provenance.
    pub fn image(&self) -> &Arc<SnapshotWithProvenance> {
        &self.image
    }

    /// Runs `validate` on exactly the bound image, then writes only the version
    /// 3 marker with `SyncAll`.
    ///
    /// A validation refusal writes nothing and leaves the owner usable. The
    /// owner retires before the native commit and is restored only on success.
    /// A returned error or escaping original panic leaves it retired: the
    /// durable marker is then either version 2 or 3, and only a full close and
    /// reopen observes which. There is no retry, rollback or panic catching.
    pub fn commit_validated<E>(
        self,
        validate: impl FnOnce(&Arc<SnapshotWithProvenance>) -> Result<(), E>,
    ) -> Result<(), StoreFormatCommitError<E>> {
        validate(&self.image).map_err(StoreFormatCommitError::Validation)?;
        Ok(self.commit()?)
    }

    fn commit(self) -> Result<(), StoreFormatUpgradeError> {
        let owner = self.owner;
        owner.retired = true;
        #[cfg(test)]
        owner.fault.before()?;
        let mut batch = owner
            .database
            .batch()
            .durability(Some(PersistMode::SyncAll));
        batch.insert(
            &owner.meta,
            FORMAT_VERSION_KEY,
            STORE_FORMAT_V3.to_be_bytes().to_vec(),
        );
        batch.commit().map_err(|error| {
            StoreFormatUpgradeError::from(StorageError::backend(
                "commit the store format marker",
                &error,
            ))
        })?;
        #[cfg(test)]
        owner.fault.after()?;
        owner.retired = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
