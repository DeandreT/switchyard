use std::path::Path;

use super::{FjallProtectedStateOpenError as Error, FjallProtectedStateStore, Inner};

pub(super) fn create_new(path: &Path) -> Result<FjallProtectedStateStore, Error> {
    #[cfg(unix)]
    {
        create(path, &mut Acquisition::default())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(Error::UnsupportedPlatform)
    }
}

pub(super) fn open_existing(path: &Path) -> Result<FjallProtectedStateStore, Error> {
    #[cfg(unix)]
    {
        let database = fjall::Database::recover(
            fjall::Database::builder(path)
                .worker_threads(1)
                .into_config(),
        )
        .map_err(|_| Error::Backend)?;
        require_spaces(&database)?;
        let records = database
            .keyspace(
                super::super::RECORDS_KEYSPACE,
                fjall::KeyspaceCreateOptions::default,
            )
            .map_err(|_| Error::Backend)?;
        let meta = database
            .keyspace(
                super::super::META_KEYSPACE,
                fjall::KeyspaceCreateOptions::default,
            )
            .map_err(|_| Error::Backend)?;
        let inner = Inner::new(database, records, meta);
        let snapshot = inner.database.snapshot();
        let result = super::view::measure(&inner, &snapshot).map_err(|error| match error {
            crate::ProtectedStateError::LimitExceeded => Error::LimitExceeded,
            crate::ProtectedStateError::Poisoned => Error::Backend,
            _ => Error::InvalidLayout,
        });
        drop(snapshot);
        result?;
        Ok(FjallProtectedStateStore {
            inner: std::sync::Arc::new(inner),
        })
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(Error::UnsupportedPlatform)
    }
}

#[cfg(unix)]
fn require_spaces(database: &fjall::Database) -> Result<(), Error> {
    if database.keyspace_count() != 2
        || !database.keyspace_exists(super::super::RECORDS_KEYSPACE)
        || !database.keyspace_exists(super::super::META_KEYSPACE)
    {
        return Err(Error::InvalidLayout);
    }
    Ok(())
}

#[cfg(unix)]
fn directory(path: &Path) -> Result<std::fs::File, Error> {
    let handle = std::fs::File::open(path).map_err(|_| Error::DirectoryUnavailable)?;
    if !handle
        .metadata()
        .map_err(|_| Error::DirectoryUnavailable)?
        .is_dir()
    {
        return Err(Error::DirectoryUnavailable);
    }
    Ok(handle)
}

#[cfg(unix)]
fn sync(handle: &std::fs::File) -> Result<(), Error> {
    handle.sync_all().map_err(|_| Error::DirectoryUnavailable)
}

#[cfg(unix)]
fn create(path: &Path, acquisition: &mut Acquisition) -> Result<FjallProtectedStateStore, Error> {
    let parent_path = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = directory(parent_path)?;
    std::fs::create_dir(path).map_err(|_| Error::DirectoryUnavailable)?;
    let selected = directory(path)?;
    sync(&parent)?;
    acquisition.step(Step::ParentReserved)?;

    // Close this descriptor before the native lock starts; never reopen it.
    let lock =
        std::fs::File::create_new(path.join("lock")).map_err(|_| Error::DirectoryUnavailable)?;
    acquisition.step(Step::FixedFileSync)?;
    sync(&lock)?;
    drop(lock);
    acquisition.step(Step::LockClosed)?;
    let database = fjall::Database::create_new(
        fjall::Database::builder(path)
            .worker_threads(1)
            .into_config(),
    )
    .map_err(|_| Error::Backend)?;
    acquisition.step(Step::NativeCreated)?;
    let records = database
        .keyspace(
            super::super::RECORDS_KEYSPACE,
            fjall::KeyspaceCreateOptions::default,
        )
        .map_err(|_| Error::Backend)?;
    acquisition.step(Step::RecordsCreated)?;
    let meta = database
        .keyspace(
            super::super::META_KEYSPACE,
            fjall::KeyspaceCreateOptions::default,
        )
        .map_err(|_| Error::Backend)?;
    acquisition.step(Step::MetaCreated)?;
    require_spaces(&database)?;

    // These physical names depend on pinned standard-tree creation order.
    let keyspaces = directory(&path.join("keyspaces"))?;
    let mut children = Vec::new();
    for id in ["0", "1", "2"] {
        let tree = directory(&path.join("keyspaces").join(id))?;
        let tables = directory(&path.join("keyspaces").join(id).join("tables"))?;
        children.push((tree, tables));
    }
    acquisition.step(Step::ChildDirectorySync)?;
    for (index, (tree, tables)) in children.iter().enumerate() {
        sync(tables)?;
        acquisition.step(Step::TablesSynced(index))?;
        sync(tree)?;
        acquisition.step(Step::TreeSynced(index))?;
    }
    sync(&keyspaces)?;
    acquisition.step(Step::KeyspacesSynced)?;
    sync(&selected)?;
    acquisition.step(Step::SelectedSynced)?;
    sync(&parent)?;
    acquisition.step(Step::ParentSynced)?;

    let mut batch = database
        .batch()
        .durability(Some(fjall::PersistMode::SyncAll));
    batch.insert(
        &meta,
        super::super::FORMAT_VERSION_KEY,
        super::ACTIVE_PROTECTED_STATE_STORE_FORMAT.to_be_bytes(),
    );
    batch.insert(&meta, super::PROFILE_KEY, super::PROFILE);
    batch.insert(&meta, super::INITIALIZED_KEY, &[0][..]);
    acquisition.step(Step::StampEntered)?;
    batch.commit().map_err(|_| Error::StampUnknown)?;
    acquisition.step(Step::StampSucceeded)?;
    // All synchronization handles stay live through the known stamp decision.
    drop(children);
    drop(keyspaces);
    drop(selected);
    drop(parent);
    Ok(FjallProtectedStateStore {
        inner: std::sync::Arc::new(Inner::new(database, records, meta)),
    })
}

#[cfg(unix)]
#[derive(Default)]
struct Acquisition {
    #[cfg(test)]
    fault: Option<Step>,
    #[cfg(test)]
    events: Vec<Step>,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Step {
    ParentReserved,
    FixedFileSync,
    LockClosed,
    NativeCreated,
    RecordsCreated,
    MetaCreated,
    ChildDirectorySync,
    TablesSynced(usize),
    TreeSynced(usize),
    KeyspacesSynced,
    SelectedSynced,
    ParentSynced,
    StampEntered,
    StampSucceeded,
}

#[cfg(unix)]
impl Acquisition {
    fn step(&mut self, step: Step) -> Result<(), Error> {
        #[cfg(test)]
        {
            self.events.push(step);
            if self.fault == Some(step) {
                return Err(Error::DirectoryUnavailable);
            }
        }
        #[cfg(not(test))]
        let _ = step;
        Ok(())
    }
}

#[cfg(all(test, unix))]
pub(super) fn observed_create(
    path: &Path,
    fixed_file: bool,
    child_directories: bool,
) -> (Result<FjallProtectedStateStore, Error>, Vec<u8>) {
    let fault = if fixed_file {
        Some(Step::FixedFileSync)
    } else if child_directories {
        Some(Step::ChildDirectorySync)
    } else {
        None
    };
    let mut acquisition = Acquisition {
        fault,
        events: Vec::new(),
    };
    let result = create(path, &mut acquisition);
    let events = acquisition
        .events
        .into_iter()
        .map(|step| match step {
            Step::ParentReserved => 0,
            Step::FixedFileSync => 1,
            Step::LockClosed => 2,
            Step::NativeCreated => 3,
            Step::RecordsCreated => 4,
            Step::MetaCreated => 5,
            Step::ChildDirectorySync => 6,
            Step::TablesSynced(i) => 7 + i as u8 * 2,
            Step::TreeSynced(i) => 8 + i as u8 * 2,
            Step::KeyspacesSynced => 13,
            Step::SelectedSynced => 14,
            Step::ParentSynced => 15,
            Step::StampEntered => 16,
            Step::StampSucceeded => 17,
        })
        .collect();
    (result, events)
}
