use std::path::{Path, PathBuf};

use fjall::Readable;

use super::*;
use crate::{ProtectedStateReader, SnapshotCatalogRecord};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
pub(super) type Rows = Vec<(Vec<u8>, Vec<u8>)>;

pub(super) fn fresh() -> TestResult<(tempfile::TempDir, PathBuf, FjallProtectedStateStore)> {
    let parent = tempfile::TempDir::new()?;
    let path = parent.path().join("selected");
    let writer = FjallProtectedStateStore::create_new(&path)?;
    Ok((parent, path, writer))
}

pub(super) fn publish(
    writer: &mut FjallProtectedStateStore,
    rows: &[(&[u8], &[u8])],
    metadata: &[u8],
    artifact: &[u8],
    fence: &[u8],
) -> TestResult {
    let pair = SnapshotCatalogRecord::new(metadata, artifact)?;
    writer.publish(ProtectedStatePublication::new(rows, pair, fence)?)?;
    Ok(())
}

pub(super) fn token(
    writer: &mut FjallProtectedStateStore,
    value: u8,
) -> Result<(), ProtectedStateError> {
    let bytes = [value];
    let rows: &[(&[u8], &[u8])] = &[(b"token", &bytes)];
    let pair = SnapshotCatalogRecord::new(&bytes, &bytes)
        .map_err(|_| ProtectedStateError::LimitExceeded)?;
    writer.publish(ProtectedStatePublication::new(rows, pair, &bytes)?)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Logical {
    pub(super) initialized: bool,
    pub(super) records: Rows,
    pub(super) live: Option<(Vec<u8>, Vec<u8>)>,
    pub(super) fence: Option<Vec<u8>>,
    pub(super) bytes: usize,
}

pub(super) fn logical(view: &StoredProtectedState) -> Logical {
    Logical {
        initialized: view.is_initialized(),
        records: view.records().entries().to_vec(),
        live: view
            .live_catalog()
            .map(|live| (live.metadata().to_vec(), live.artifact().to_vec())),
        fence: view.fence().map(<[u8]>::to_vec),
        bytes: view.logical_payload_bytes(),
    }
}

pub(super) fn assert_token(view: &StoredProtectedState, value: u8) {
    assert_eq!(
        logical(view),
        Logical {
            initialized: true,
            records: vec![(b"token".to_vec(), vec![value])],
            live: Some((vec![value], vec![value])),
            fence: Some(vec![value]),
            bytes: 9
        }
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Counts {
    pub(super) views: usize,
    pub(super) copies: usize,
    pub(super) preparations: usize,
    pub(super) entries: usize,
    pub(super) native: usize,
}

pub(super) fn counts(writer: &FjallProtectedStateStore) -> Counts {
    let c = &writer.inner.controls;
    Counts {
        views: c.views.load(Ordering::SeqCst),
        copies: c.copies.load(Ordering::SeqCst),
        preparations: c.preparations.load(Ordering::SeqCst),
        entries: c.entries.load(Ordering::SeqCst),
        native: c.native_calls.load(Ordering::SeqCst),
    }
}

pub(super) fn fault(writer: &FjallProtectedStateStore, value: Fault) {
    *writer.inner.controls.fault.lock().expect("test fault lock") = value;
}

pub(super) fn raw_space(
    database: &Database,
    snapshot: &fjall::Snapshot,
    name: &str,
) -> TestResult<Option<Rows>> {
    if !database.keyspace_exists(name) {
        return Ok(None);
    }
    let space = database.keyspace(name, fjall::KeyspaceCreateOptions::default)?;
    let mut rows = Vec::new();
    for guard in snapshot.iter(&space) {
        let key = guard.key()?;
        let value = snapshot.get(&space, &key)?.ok_or("missing fixture value")?;
        rows.push((key.to_vec(), value.to_vec()));
    }
    Ok(Some(rows))
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct Dictionary {
    pub(super) names: Vec<String>,
    pub(super) records: Option<Rows>,
    pub(super) meta: Option<Rows>,
}

pub(super) fn dictionary(database: &Database) -> TestResult<Dictionary> {
    let mut names: Vec<_> = database
        .list_keyspace_names()
        .into_iter()
        .map(|name| name.to_string())
        .collect();
    names.sort();
    let snapshot = database.snapshot();
    Ok(Dictionary {
        names,
        records: raw_space(database, &snapshot, super::super::super::RECORDS_KEYSPACE)?,
        meta: raw_space(database, &snapshot, super::super::super::META_KEYSPACE)?,
    })
}

pub(super) fn observe(path: &Path) -> TestResult<Dictionary> {
    let database = Database::recover(Database::builder(path).worker_threads(1).into_config())?;
    dictionary(&database)
}

pub(super) fn twice(path: &Path) -> TestResult<(StoredProtectedState, StoredProtectedState)> {
    let first = {
        let writer = FjallProtectedStateStore::open_existing(path)?;
        writer.reader().capture_protected_state()?
    };
    let second = {
        let writer = FjallProtectedStateStore::open_existing(path)?;
        writer.reader().capture_protected_state()?
    };
    Ok((first, second))
}

pub(super) fn put_meta(writer: &FjallProtectedStateStore, key: &[u8], value: &[u8]) -> TestResult {
    let mut batch = writer
        .inner
        .database
        .batch()
        .durability(Some(fjall::PersistMode::SyncAll));
    batch.insert(&writer.inner.meta, key, value);
    batch.commit()?;
    Ok(())
}

// Native batches reject empty keys before writing; seeded damage uses valid native keys.
#[derive(Clone, Copy, Debug)]
pub(super) enum Damage {
    WrongFormat,
    WrongProfile,
    BadInit,
    Unknown,
    PartialMetadata,
    PartialArtifact,
    PartialFence,
    Orphan,
    LongKey,
    LongValue,
    LargeArtifact,
}

pub(super) fn damage(writer: &FjallProtectedStateStore, damage: Damage) -> TestResult {
    let mut batch = writer
        .inner
        .database
        .batch()
        .durability(Some(fjall::PersistMode::SyncAll));
    match damage {
        Damage::WrongFormat => batch.insert(
            &writer.inner.meta,
            KEYS[0],
            super::super::super::ACTIVE_STORE_FORMAT.to_be_bytes(),
        ),
        Damage::WrongProfile => batch.insert(&writer.inner.meta, PROFILE_KEY, b"foreign"),
        Damage::BadInit => batch.insert(&writer.inner.meta, INITIALIZED_KEY, &[2][..]),
        Damage::Unknown => batch.insert(&writer.inner.meta, b"paired_binding", b"foreign"),
        Damage::PartialMetadata => batch.remove(&writer.inner.meta, METADATA_KEY),
        Damage::PartialArtifact => batch.remove(&writer.inner.meta, ARTIFACT_KEY),
        Damage::PartialFence => batch.remove(&writer.inner.meta, FENCE_KEY),
        Damage::Orphan => {
            batch.insert(&writer.inner.meta, INITIALIZED_KEY, &[0][..]);
            for key in [METADATA_KEY, ARTIFACT_KEY, FENCE_KEY] {
                batch.remove(&writer.inner.meta, key);
            }
        }
        Damage::LongKey => batch.insert(&writer.inner.records, vec![b'z'; 1025], b"value"),
        Damage::LongValue => batch.insert(&writer.inner.records, b"large", vec![0; 266_241]),
        Damage::LargeArtifact => {
            batch.insert(&writer.inner.meta, ARTIFACT_KEY, vec![0; 67_108_865])
        }
    }
    batch.commit()?;
    Ok(())
}
