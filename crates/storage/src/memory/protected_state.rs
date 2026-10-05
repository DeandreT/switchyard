use std::{
    fmt,
    sync::{Arc, RwLock, RwLockWriteGuard},
};

use crate::protected_state::{copy_parts, copy_state};
use crate::{
    ProtectedStateError, ProtectedStatePublication, ProtectedStateReader, StoredProtectedState,
};

/// Unique low-level writer for a fresh, separate Memory object. No legacy
/// storage traits, wrapping/conversion, raw batch escape or native path exist.
/// Complete publications couple records, initialization, live pair and opaque
/// fence; distinct bytes are not canonical selection or consensus authority.
///
/// ```compile_fail
/// let writer = storage::MemoryProtectedStateStore::new();
/// let second = writer.clone();
/// ```
///
/// ```compile_fail
/// use storage::StateStore;
/// let writer = storage::MemoryProtectedStateStore::new();
/// writer.apply(storage::WriteBatch::default());
/// ```
///
/// ```compile_fail
/// use storage::CommittedStore;
/// let mut writer = storage::MemoryProtectedStateStore::new();
/// writer.commit(storage::WriteBatch::default());
/// ```
///
/// ```compile_fail
/// use storage::{CatalogCommittedStore, SnapshotCatalogRecord};
/// let mut writer = storage::MemoryProtectedStateStore::new();
/// writer.commit_with_catalog(storage::WriteBatch::default(),
///     SnapshotCatalogRecord::new(b"", b"").unwrap());
/// ```
///
/// ```compile_fail
/// let writer = storage::MemoryProtectedStateStore::new();
/// let ordinary: storage::MemoryStore = writer.into();
/// ```
///
/// ```no_run
/// use storage::{MemoryProtectedStateStore, ProtectedStatePublication,
///     ProtectedStateReader, SnapshotCatalogRecord, PROTECTED_STATE_RECORD_LIMITS,
///     MAX_PROTECTED_STATE_FENCE_BYTES, MAX_PROTECTED_STATE_BYTES};
/// let view = {
///     let mut writer = MemoryProtectedStateStore::new();
///     {
///         let rows: &[(&[u8], &[u8])] = &[(b"key", b"value")];
///         let pair = SnapshotCatalogRecord::new(b"metadata", b"artifact")?;
///         let input = ProtectedStatePublication::new(rows, pair, b"fence-one")?;
///         writer.publish(input)?;
///     }
///     let reader = writer.reader();
///     let another = reader.clone();
///     another.capture_protected_state()?
/// };
/// assert!(view.is_initialized());
/// let _ = (view.records(), view.live_catalog(), view.fence(), view.logical_payload_bytes());
/// let _ = (PROTECTED_STATE_RECORD_LIMITS, MAX_PROTECTED_STATE_FENCE_BYTES, MAX_PROTECTED_STATE_BYTES);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct MemoryProtectedStateStore {
    cell: Arc<RwLock<Cell>>,
    #[cfg(test)]
    fault: Fault,
}

/// Cloneable complete read-only origin. Cloning never creates a writer.
///
/// ```compile_fail
/// use storage::StateStore;
/// let reader = storage::MemoryProtectedStateStore::new().reader();
/// reader.apply(storage::WriteBatch::default());
/// ```
#[derive(Clone)]
pub struct MemoryProtectedStateReader {
    cell: Arc<RwLock<Cell>>,
}

struct Cell {
    poisoned: bool,
    data: StoredProtectedState,
    #[cfg(test)]
    copies: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    entries: std::sync::atomic::AtomicUsize,
}

impl Default for MemoryProtectedStateStore {
    fn default() -> Self {
        Self {
            cell: Arc::new(RwLock::new(Cell {
                poisoned: false,
                data: StoredProtectedState::pristine(),
                #[cfg(test)]
                copies: std::sync::atomic::AtomicUsize::new(0),
                #[cfg(test)]
                entries: std::sync::atomic::AtomicUsize::new(0),
            })),
            #[cfg(test)]
            fault: Fault::None,
        }
    }
}

impl MemoryProtectedStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reader(&self) -> MemoryProtectedStateReader {
        MemoryProtectedStateReader {
            cell: self.cell.clone(),
        }
    }

    pub fn publish(
        &mut self,
        input: ProtectedStatePublication<'_>,
    ) -> Result<(), ProtectedStateError> {
        {
            let cell = self
                .cell
                .read()
                .map_err(|_| ProtectedStateError::Poisoned)?;
            admit(&cell, input.fence)?;
        }
        #[cfg(test)]
        if self.fault == Fault::Prepare {
            return Err(ProtectedStateError::Allocation);
        }
        let candidate = copy_parts(
            input.rows.iter().copied(),
            &input.live,
            input.fence,
            input.shape,
        )?;
        let cell = self
            .cell
            .write()
            .map_err(|_| ProtectedStateError::Poisoned)?;
        admit(&cell, input.fence)?;
        #[cfg(test)]
        {
            cell.copies
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            cell.entries
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        let mut entry = Entry {
            guard: Some(cell),
            retired: None,
            completed: false,
        };
        #[cfg(test)]
        if self.fault == Fault::Before {
            return Err(ProtectedStateError::PublishUnknown);
        }
        #[cfg(test)]
        if self.fault == Fault::Panic {
            panic!("protected state entered fault");
        }
        entry.replace(candidate)?;
        #[cfg(test)]
        if self.fault == Fault::After {
            return Err(ProtectedStateError::PublishUnknown);
        }
        entry.completed = true;
        drop(entry);
        Ok(())
    }
}

fn admit(cell: &Cell, fence: &[u8]) -> Result<(), ProtectedStateError> {
    if cell.poisoned {
        return Err(ProtectedStateError::Poisoned);
    }
    cell.data.check()?;
    if cell.data.fence.as_deref() == Some(fence) {
        return Err(ProtectedStateError::UnchangedFence);
    }
    Ok(())
}

// Own the write guard and displaced bytes so unlock always precedes disposal.
struct Entry<'a> {
    guard: Option<RwLockWriteGuard<'a, Cell>>,
    retired: Option<StoredProtectedState>,
    completed: bool,
}

impl Entry<'_> {
    fn replace(&mut self, candidate: StoredProtectedState) -> Result<(), ProtectedStateError> {
        let cell = self
            .guard
            .as_mut()
            .ok_or(ProtectedStateError::PublishUnknown)?;
        self.retired = Some(std::mem::replace(&mut cell.data, candidate));
        Ok(())
    }
}

impl Drop for Entry<'_> {
    fn drop(&mut self) {
        if !self.completed
            && let Some(cell) = &mut self.guard
        {
            cell.poisoned = true;
        }
        drop(self.guard.take());
        drop(self.retired.take());
    }
}

impl ProtectedStateReader for MemoryProtectedStateReader {
    fn capture_protected_state(&self) -> Result<StoredProtectedState, ProtectedStateError> {
        let cell = self
            .cell
            .read()
            .map_err(|_| ProtectedStateError::Poisoned)?;
        if cell.poisoned {
            return Err(ProtectedStateError::Poisoned);
        }
        let shape = cell.data.check()?;
        #[cfg(test)]
        cell.copies
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        copy_state(&cell.data, shape)
    }
}

impl fmt::Debug for MemoryProtectedStateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryProtectedStateStore")
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for MemoryProtectedStateReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryProtectedStateReader")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Eq, PartialEq)]
enum Fault {
    None,
    Prepare,
    Before,
    After,
    Panic,
}

#[cfg(test)]
mod tests;
