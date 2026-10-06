//! Explicit protected-state profile; ordinary stores never adopt this layout.

use std::{
    fmt,
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use fjall::{Database, Keyspace};

use crate::{
    ProtectedStateError, ProtectedStatePublication, ProtectedStateReader, StoredProtectedState,
};

mod acquisition;
mod publication;
mod view;

pub const ACTIVE_PROTECTED_STATE_STORE_FORMAT: u32 = 0xd000_0000 | super::ACTIVE_STORE_FORMAT;
const PROFILE_KEY: &[u8] = b"replica_profile";
const PROFILE: &[u8] = b"protected-state-publication-v1";
const INITIALIZED_KEY: &[u8] = b"replica_initialized";
const METADATA_KEY: &[u8] = b"snapshot_meta";
const ARTIFACT_KEY: &[u8] = b"snapshot_image";
const FENCE_KEY: &[u8] = b"protected_state_fence";
const KEYS: [&[u8]; 6] = [
    super::FORMAT_VERSION_KEY,
    PROFILE_KEY,
    INITIALIZED_KEY,
    METADATA_KEY,
    ARTIFACT_KEY,
    FENCE_KEY,
];

/// Static acquisition categories deliberately discard backend/path details.
/// A failed prefix can have filesystem effects; no cleanup or retry is implied.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum FjallProtectedStateOpenError {
    #[error("protected profile platform is unsupported")]
    UnsupportedPlatform,
    #[error("protected profile directory is unavailable")]
    DirectoryUnavailable,
    #[error("protected profile backend acquisition failed")]
    Backend,
    #[error("protected profile layout is invalid")]
    InvalidLayout,
    #[error("protected profile limit exceeded")]
    LimitExceeded,
    #[error("protected profile stamp outcome is unknown")]
    StampUnknown,
}

/// Unique writer selected explicitly by a trusted external path owner.
/// This Unix profile requires working file/directory sync and honest storage.
/// Recovery may mutate native files; neither acquisition nor Drop certifies
/// native-worker joins, SafeReopen, arbitrary corruption or power-loss testing.
///
/// ```compile_fail
/// let writer = storage::FjallProtectedStateStore::create_new("selected").unwrap();
/// let other = writer.clone();
/// ```
///
/// ```compile_fail
/// use storage::StateStore;
/// let writer = storage::FjallProtectedStateStore::create_new("selected").unwrap();
/// writer.apply(storage::WriteBatch::default());
/// ```
///
/// ```compile_fail
/// use storage::CommittedStore;
/// let mut writer = storage::FjallProtectedStateStore::create_new("selected").unwrap();
/// writer.commit(storage::WriteBatch::default());
/// ```
///
/// ```compile_fail
/// use storage::{CatalogCommittedStore, SnapshotCatalogRecord};
/// let mut writer = storage::FjallProtectedStateStore::create_new("selected").unwrap();
/// writer.commit_with_catalog(storage::WriteBatch::default(), SnapshotCatalogRecord::new(b"", b"").unwrap());
/// ```
///
/// ```compile_fail
/// let writer = storage::FjallProtectedStateStore::create_new("selected").unwrap();
/// let native = writer.database();
/// ```
///
/// ```no_run
/// use storage::{FjallProtectedStateStore, ProtectedStatePublication, ProtectedStateReader, SnapshotCatalogRecord};
/// // Parent must already be durably established and externally protected.
/// let path = std::path::Path::new("trusted-parent/selected-child");
/// let mut writer = FjallProtectedStateStore::create_new(path)?;
/// let rows: &[(&[u8], &[u8])] = &[(b"key", b"value")];
/// writer.publish(ProtectedStatePublication::new(rows, SnapshotCatalogRecord::new(b"meta", b"image")?, b"fence")?)?;
/// let reader = writer.reader();
/// let capture = reader.capture_protected_state()?;
/// drop(reader);
/// drop(writer);
/// let reopened = FjallProtectedStateStore::open_existing(path)?;
/// let next = reopened.reader().capture_protected_state()?;
/// assert_eq!(capture.records(), next.records());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct FjallProtectedStateStore {
    inner: Arc<Inner>,
}

/// Read-only clone of the exact originating generation, not a second opener.
/// Owned captures retain no native handle and confer no publication authority.
///
/// ```compile_fail
/// use storage::{ProtectedStatePublication, SnapshotCatalogRecord};
/// let writer = storage::FjallProtectedStateStore::create_new("selected").unwrap();
/// let mut reader = writer.reader();
/// reader.publish(ProtectedStatePublication::new(&[], SnapshotCatalogRecord::new(b"", b"").unwrap(), b"f").unwrap());
/// ```
#[derive(Clone)]
pub struct FjallProtectedStateReader {
    inner: Arc<Inner>,
}

struct Inner {
    database: Database,
    records: Keyspace,
    meta: Keyspace,
    gate: RwLock<()>,
    poisoned: AtomicBool,
    #[cfg(test)]
    controls: Controls,
}

impl Inner {
    fn new(database: Database, records: Keyspace, meta: Keyspace) -> Self {
        Self {
            database,
            records,
            meta,
            gate: RwLock::new(()),
            poisoned: AtomicBool::new(false),
            #[cfg(test)]
            controls: Controls::default(),
        }
    }

    fn healthy(&self) -> Result<(), ProtectedStateError> {
        if self.poisoned.load(Ordering::SeqCst) || self.gate.is_poisoned() {
            self.poison();
            Err(ProtectedStateError::Poisoned)
        } else {
            Ok(())
        }
    }

    fn poison(&self) {
        self.poisoned.store(true, Ordering::SeqCst);
    }
}

// Mark terminal failure before releasing admission, including destructor unwind.
struct ReadGuard<'a> {
    inner: &'a Inner,
    gate: Option<std::sync::RwLockReadGuard<'a, ()>>,
    armed: bool,
}

impl<'a> ReadGuard<'a> {
    fn acquire(inner: &'a Inner) -> Result<Self, ProtectedStateError> {
        inner.healthy()?;
        let gate = inner.gate.read().map_err(|_| {
            inner.poison();
            ProtectedStateError::Poisoned
        })?;
        inner.healthy()?;
        Ok(Self {
            inner,
            gate: Some(gate),
            armed: true,
        })
    }

    fn finish<T>(
        mut self,
        result: Result<T, ProtectedStateError>,
    ) -> Result<T, ProtectedStateError> {
        match &result {
            Ok(_) => {
                self.inner.healthy()?;
                self.armed = false;
            }
            Err(ProtectedStateError::Allocation | ProtectedStateError::UnchangedFence) => {
                self.armed = false;
            }
            Err(_) => {}
        }
        result
    }
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.inner.poison();
        }
        drop(self.gate.take());
    }
}

impl FjallProtectedStateStore {
    /// Reserves only an absent child under an already established trusted parent.
    /// Failed native prefixes remain on disk; they are not automatically reopened.
    pub fn create_new(directory: impl AsRef<Path>) -> Result<Self, FjallProtectedStateOpenError> {
        acquisition::create_new(directory.as_ref())
    }

    /// Explicit native recovery, not read-only physical inspection or repair.
    /// All prior writers/readers/batches/snapshots must first be released.
    pub fn open_existing(
        directory: impl AsRef<Path>,
    ) -> Result<Self, FjallProtectedStateOpenError> {
        acquisition::open_existing(directory.as_ref())
    }

    pub fn reader(&self) -> FjallProtectedStateReader {
        FjallProtectedStateReader {
            inner: self.inner.clone(),
        }
    }

    /// One complete required SyncAll batch. An entered failure is terminal and
    /// unknown, not rollback, a retry permit or a reconstructible success.
    pub fn publish(
        &mut self,
        input: ProtectedStatePublication<'_>,
    ) -> Result<(), ProtectedStateError> {
        publication::publish(&self.inner, input)
    }
}

impl ProtectedStateReader for FjallProtectedStateReader {
    fn capture_protected_state(&self) -> Result<StoredProtectedState, ProtectedStateError> {
        view::capture(&self.inner)
    }
}

impl fmt::Debug for FjallProtectedStateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FjallProtectedStateStore")
            .field("poisoned", &self.inner.poisoned.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for FjallProtectedStateReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FjallProtectedStateReader")
            .field("poisoned", &self.inner.poisoned.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum Fault {
    #[default]
    None,
    Prepare,
    CaptureAllocation,
    CurrentBackend,
    Before,
    After,
    PanicBefore,
    PanicAfter,
    ExitBefore,
    ExitAfter,
}

#[cfg(test)]
#[derive(Default)]
struct Controls {
    fault: std::sync::Mutex<Fault>,
    views: std::sync::atomic::AtomicUsize,
    copies: std::sync::atomic::AtomicUsize,
    preparations: std::sync::atomic::AtomicUsize,
    entries: std::sync::atomic::AtomicUsize,
    native_calls: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl Controls {
    fn fault(&self) -> Fault {
        *self.fault.lock().expect("test fault lock")
    }
}

#[cfg(all(test, unix))]
mod tests;
