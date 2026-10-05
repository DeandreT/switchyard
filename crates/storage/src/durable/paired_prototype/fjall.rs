use std::{
    fmt,
    path::{Path, PathBuf},
};

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable};

use super::super::{FORMAT_VERSION_KEY, META_KEYSPACE, RECORDS_KEYSPACE};
use super::control::{BINDING_KEY, INIT_KEY, PROFILE_KEY, PhysicalPairBinding, Role, STAMP_KEY};
use super::inventory::{LOGICAL, RoleData, RoleInventory, RoleStatus, Shape, fixed_records};
use super::{Error, Result, Rows, copy};

pub(super) enum AcquisitionCause {
    Io(std::io::Error),
    Native(fjall::Error),
}

pub(super) struct AcquisitionFailure {
    pub(super) cause: AcquisitionCause,
    pub(super) paths: [PathBuf; 2],
}

impl fmt::Debug for AcquisitionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AcquisitionFailure { .. }")
    }
}
impl fmt::Display for AcquisitionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("controlled fixture acquisition failed")
    }
}
impl std::error::Error for AcquisitionFailure {}

pub(super) struct ControlledLocations {
    pub(super) paths: [PathBuf; 2],
}

impl ControlledLocations {
    pub(super) fn reserve(parent: &Path) -> std::result::Result<Self, AcquisitionFailure> {
        let paths = [parent.join("state"), parent.join("log")];
        for path in &paths {
            if let Err(error) = std::fs::create_dir(path) {
                return Err(AcquisitionFailure {
                    cause: AcquisitionCause::Io(error),
                    paths,
                });
            }
        }
        Ok(Self { paths })
    }
}

pub(super) struct NativeCapsule {
    pub(super) database: Database,
    pub(super) meta: Keyspace,
    pub(super) records: Keyspace,
    role: Role,
    binding: PhysicalPairBinding,
    poisoned: bool,
}

pub(super) struct PairedFjallState {
    pub(super) capsule: NativeCapsule,
}
pub(super) struct PairedFjallLog {
    pub(super) capsule: NativeCapsule,
}

impl NativeCapsule {
    pub(super) fn create(
        path: &Path,
        role: Role,
        binding: PhysicalPairBinding,
    ) -> fjall::Result<Self> {
        let database =
            Database::create_new(Database::builder(path).worker_threads(1).into_config())?;
        let meta = database.keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)?;
        let records = database.keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)?;
        Ok(Self {
            database,
            meta,
            records,
            role,
            binding,
            poisoned: false,
        })
    }

    fn recover(path: &Path, role: Role, binding: PhysicalPairBinding) -> Result<Self> {
        // Only controlled successful-acquisition layouts; native recovery effects are allowed.
        let database = Database::recover(Database::builder(path).worker_threads(1).into_config())
            .map_err(|_| Error::Backend)?;
        if database.keyspace_count() != 2
            || !database.keyspace_exists(META_KEYSPACE)
            || !database.keyspace_exists(RECORDS_KEYSPACE)
        {
            return Err(Error::InvalidLogical);
        }
        let meta = database
            .keyspace(META_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(|_| Error::Backend)?;
        let records = database
            .keyspace(RECORDS_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(|_| Error::Backend)?;
        Ok(Self {
            database,
            meta,
            records,
            role,
            binding,
            poisoned: false,
        })
    }

    pub(super) fn poison(&mut self) {
        self.poisoned = true;
    }

    pub(super) fn capture(&self) -> Result<RoleInventory> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let snapshot = self.database.snapshot();
        self.capture_view(&snapshot)
    }

    pub(super) fn capture_view(&self, snapshot: &fjall::Snapshot) -> Result<RoleInventory> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let mut shape = Shape::new(self.role);
        for (space, metadata) in [(&self.meta, true), (&self.records, false)] {
            let mut previous: Option<fjall::UserKey> = None;
            for guard in snapshot.iter(space) {
                let key = guard.key().map_err(|_| Error::Backend)?;
                if previous
                    .as_ref()
                    .is_some_and(|p| p.as_ref() >= key.as_ref())
                {
                    return Err(Error::InvalidLogical);
                }
                let size = snapshot
                    .size_of(space, &key)
                    .map_err(|_| Error::Backend)?
                    .ok_or(Error::InvalidLogical)?;
                let size = usize::try_from(size).map_err(|_| Error::Limit)?;
                if metadata {
                    shape.metadata(&key, size)?;
                } else {
                    shape.record(&key, size)?;
                }
                previous = Some(key);
            }
        }
        let status = if shape.finish()? {
            RoleStatus::Empty
        } else {
            let format = self.fixed(snapshot, FORMAT_VERSION_KEY, 4)?;
            let profile = self.fixed(snapshot, PROFILE_KEY, self.role.profile().len())?;
            let init = self.fixed(snapshot, INIT_KEY, 1)?;
            let binding = self.fixed(snapshot, BINDING_KEY, 112)?;
            let stamp = self.fixed(snapshot, STAMP_KEY, 112)?;
            fixed_records(
                self.role,
                self.binding,
                [&format, &profile, &init, &binding, &stamp],
            )?
        };
        let (meta_count, record_count) = shape.counts();
        Ok(RoleInventory {
            status,
            metadata: self.copy_space(snapshot, &self.meta, meta_count)?,
            records: self.copy_space(snapshot, &self.records, record_count)?,
        })
    }

    fn fixed(
        &self,
        snapshot: &fjall::Snapshot,
        key: &[u8],
        bytes: usize,
    ) -> Result<fjall::UserValue> {
        if snapshot
            .size_of(&self.meta, key)
            .map_err(|_| Error::Backend)?
            .map(|size| size as u64)
            != Some(bytes as u64)
        {
            return Err(Error::InvalidLogical);
        }
        let value = snapshot
            .get(&self.meta, key)
            .map_err(|_| Error::Backend)?
            .ok_or(Error::InvalidLogical)?;
        if value.len() != bytes {
            return Err(Error::InvalidLogical);
        }
        Ok(value)
    }

    fn copy_space(
        &self,
        snapshot: &fjall::Snapshot,
        space: &Keyspace,
        count: usize,
    ) -> Result<Rows> {
        let mut rows = Vec::new();
        rows.try_reserve_exact(count)
            .map_err(|_| Error::Allocation)?;
        for guard in snapshot.iter(space) {
            let key = guard.key().map_err(|_| Error::Backend)?;
            let bytes = snapshot
                .size_of(space, &key)
                .map_err(|_| Error::Backend)?
                .ok_or(Error::InvalidLogical)?;
            let value = snapshot
                .get(space, &key)
                .map_err(|_| Error::Backend)?
                .ok_or(Error::InvalidLogical)?;
            if value.len() as u64 != u64::from(bytes) || rows.len() == count {
                return Err(Error::InvalidLogical);
            }
            rows.push((copy(&key)?, copy(&value)?));
        }
        if rows.len() != count {
            return Err(Error::InvalidLogical);
        }
        Ok(rows)
    }

    pub(super) fn seed(&mut self, candidate: &RoleData) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let mut batch = self.database.batch().durability(Some(PersistMode::SyncAll));
        for (key, value) in &candidate.metadata {
            batch.insert(&self.meta, key.as_slice(), value.as_slice());
        }
        for (key, value) in &candidate.records {
            batch.insert(&self.records, key.as_slice(), value.as_slice());
        }
        batch.commit().map_err(|_| Error::Backend)
    }

    pub(super) fn ready(&mut self, candidate: &RoleData) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let value = candidate
            .metadata
            .get(STAMP_KEY)
            .ok_or(Error::InvalidLogical)?;
        let mut batch = self.database.batch().durability(Some(PersistMode::SyncAll));
        batch.insert(&self.meta, STAMP_KEY, value.as_slice());
        batch.commit().map_err(|_| Error::Backend)
    }

    pub(super) fn composite(&mut self, candidate: &RoleData, old_keys: &[Vec<u8>]) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let mut batch = self.database.batch().durability(Some(PersistMode::SyncAll));
        for key in old_keys {
            batch.remove(&self.records, key.as_slice());
        }
        for (key, value) in &candidate.records {
            batch.insert(&self.records, key.as_slice(), value.as_slice());
        }
        for key in [INIT_KEY, LOGICAL[1], LOGICAL[5], LOGICAL[6]] {
            let value = candidate.metadata.get(key).ok_or(Error::InvalidLogical)?;
            batch.insert(&self.meta, key, value.as_slice());
        }
        batch.commit().map_err(|_| Error::Backend)
    }
}

pub(super) fn reopen_controlled_fixture(
    locations: &ControlledLocations,
    binding: PhysicalPairBinding,
) -> Result<super::inventory::PairInventory> {
    let state = NativeCapsule::recover(&locations.paths[0], Role::State, binding)?;
    let log = NativeCapsule::recover(&locations.paths[1], Role::Log, binding)?;
    let value = super::inventory::PairInventory {
        state: state.capture()?,
        log: log.capture()?,
    };
    drop(log);
    drop(state);
    Ok(value)
}
