use std::sync::atomic::Ordering;

use super::*;
use crate::{SnapshotCatalogRecord, StoreSnapshot, StoredSnapshotCatalog};

pub(super) type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Eq, PartialEq)]
pub(super) struct View {
    pub(super) initialized: bool,
    pub(super) rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub(super) live: Option<(Vec<u8>, Vec<u8>)>,
    pub(super) fence: Option<Vec<u8>>,
    pub(super) bytes: usize,
}

pub(super) fn view(reader: &MemoryProtectedStateReader) -> Result<View, ProtectedStateError> {
    let state = reader.capture_protected_state()?;
    Ok(View {
        initialized: state.is_initialized(),
        rows: state.records().entries().to_vec(),
        live: state
            .live_catalog()
            .map(|live| (live.metadata().to_vec(), live.artifact().to_vec())),
        fence: state.fence().map(<[u8]>::to_vec),
        bytes: state.logical_payload_bytes(),
    })
}

pub(super) fn publish(
    writer: &mut MemoryProtectedStateStore,
    rows: &[(&[u8], &[u8])],
    metadata: &[u8],
    artifact: &[u8],
    fence: &[u8],
) -> Result<(), ProtectedStateError> {
    let pair = SnapshotCatalogRecord::new(metadata, artifact)
        .map_err(|_| ProtectedStateError::LimitExceeded)?;
    writer.publish(ProtectedStatePublication::new(rows, pair, fence)?)
}

pub(super) fn initialized() -> MemoryProtectedStateStore {
    let mut writer = MemoryProtectedStateStore::new();
    publish(&mut writer, &[(b"a", b"old")], b"meta", b"body", b"one").unwrap();
    writer
}

pub(super) fn counts(writer: &MemoryProtectedStateStore) -> (usize, usize) {
    let cell = writer
        .cell
        .read()
        .unwrap_or_else(|error| error.into_inner());
    (
        cell.copies.load(Ordering::SeqCst),
        cell.entries.load(Ordering::SeqCst),
    )
}

pub(super) fn data(rows: Vec<(Vec<u8>, Vec<u8>)>, fence: &[u8]) -> StoredProtectedState {
    let live = StoredSnapshotCatalog::copy_from_parts(b"m", b"a").unwrap();
    let bytes = rows.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() + 2 + fence.len();
    StoredProtectedState {
        initialized: true,
        records: StoreSnapshot { entries: rows },
        live: Some(live),
        fence: Some(fence.to_vec()),
        logical_bytes: bytes,
    }
}
