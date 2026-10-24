//! Offline, exact-image, non-destructive anchor attachment. No pruning API.
//!
//! Installation and attachment are separate atomic operations. All writers,
//! raw clones, keyspaces and prior indexed/replay owners must be excluded by
//! the caller. Neither these checks nor checksums authenticate history.

use std::sync::Arc;

use domain::{DurableProposal, IndexedApplyError, IndexedWriter};
use sha2::{Digest, Sha256};
use storage::{
    ImageInstallBackend, SnapshotProvenance, SnapshotWithProvenance, StateStore, StorageError,
    WriteBatch,
};
use thiserror::Error;

use super::capture::{FrozenImage, validate_image};
use super::format::RecordShape;
use super::{
    SNAPSHOT_FORMAT_VERSION, SnapshotCapture, SnapshotCaptureError, SnapshotFormat,
    SnapshotFormatError, SnapshotLimits, SnapshotManifest,
};
use crate::journal::{
    JournalError, MAX_JOURNAL_PAYLOAD_BYTES, anchor_key, anchored_root, decode_entry, entry_key,
};

pub const SNAPSHOT_ANCHOR_VERSION: u32 = 1;
pub const SNAPSHOT_ANCHOR_HASH_SCOPE: &[u8] = b"switchyard snapshot anchor v1\0";
const MAGIC: &[u8; 4] = b"SWSA";
const HEADER_BYTES: usize = 116;
const CHECKSUM_BYTES: usize = 32;
const MAX_ORIGINAL_BYTES: usize = MAX_JOURNAL_PAYLOAD_BYTES + 48;

/// Immutable source identity and exact original journal envelope. B is distinct
/// from A and initially zero; only synthetic fixtures can remove a prefix here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotAnchor {
    removed_through: u64,
    source_format: u32,
    source_manifest: SnapshotManifest,
    source_frame_digest: [u8; 32],
    original_entry: Vec<u8>,
}

impl SnapshotAnchor {
    pub fn removed_through(&self) -> u64 {
        self.removed_through
    }
    pub fn applied_index(&self) -> u64 {
        self.source_manifest.applied_index()
    }
    pub fn source_format_version(&self) -> u32 {
        self.source_format
    }
    pub fn source_manifest(&self) -> SnapshotManifest {
        self.source_manifest
    }
    pub fn source_frame_digest(&self) -> [u8; 32] {
        self.source_frame_digest
    }
    pub fn original_entry(&self) -> &[u8] {
        &self.original_entry
    }

    pub(crate) fn original_proposal(&self) -> Result<DurableProposal, JournalError> {
        let entry = decode_entry(&entry_key(self.applied_index()), &self.original_entry)?;
        DurableProposal::decode(entry.payload())
            .map_err(|_| corrupt("anchor original proposal is invalid"))
    }

    fn validate(&self) -> Result<(), JournalError> {
        validate_original(
            self.removed_through,
            self.source_format,
            self.source_manifest,
            &self.original_entry,
        )
        .map(|_| ())
    }

    fn encoded_size(original_entry: &[u8]) -> Result<usize, SnapshotAnchorError> {
        u32::try_from(original_entry.len()).map_err(|_| SnapshotAnchorError::SizeOverflow)?;
        HEADER_BYTES
            .checked_add(original_entry.len())
            .and_then(|size| size.checked_add(CHECKSUM_BYTES))
            .ok_or(SnapshotAnchorError::SizeOverflow)
    }

    fn encode(&self) -> Result<Vec<u8>, SnapshotAnchorError> {
        let mut bytes = Vec::with_capacity(Self::encoded_size(&self.original_entry)?);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&SNAPSHOT_ANCHOR_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.removed_through.to_be_bytes());
        bytes.extend_from_slice(&self.source_format.to_be_bytes());
        bytes.extend_from_slice(&self.source_manifest.layout_version().to_be_bytes());
        bytes.extend_from_slice(&self.applied_index().to_be_bytes());
        bytes.extend_from_slice(&self.source_manifest.committed_index().to_be_bytes());
        bytes.extend_from_slice(&self.source_manifest.last_appended_index().to_be_bytes());
        bytes.extend_from_slice(
            &self
                .source_manifest
                .latest_applied_digest()
                .expect("validated nonzero A"),
        );
        bytes.extend_from_slice(&self.source_frame_digest);
        bytes.extend_from_slice(&(self.original_entry.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&self.original_entry);
        bytes.extend_from_slice(&checksum(&bytes));
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, JournalError> {
        if !(HEADER_BYTES + CHECKSUM_BYTES..=HEADER_BYTES + MAX_ORIGINAL_BYTES + CHECKSUM_BYTES)
            .contains(&bytes.len())
        {
            return Err(corrupt("anchor encoded size is outside its bound"));
        }
        if &bytes[..4] != MAGIC || u32_at(bytes, 4) != SNAPSHOT_ANCHOR_VERSION {
            return Err(corrupt("anchor framing or version is unsupported"));
        }
        let length = u32_at(bytes, 112) as usize;
        if length > MAX_ORIGINAL_BYTES || HEADER_BYTES + length + CHECKSUM_BYTES != bytes.len() {
            return Err(corrupt("anchor original length is not canonical"));
        }
        let end = bytes.len() - CHECKSUM_BYTES;
        if bytes[end..] != checksum(&bytes[..end]) {
            return Err(corrupt("anchor checksum differs"));
        }
        let source_manifest = SnapshotManifest::new(
            u32_at(bytes, 20),
            u64_at(bytes, 24),
            u64_at(bytes, 32),
            u64_at(bytes, 40),
            Some(bytes[48..80].try_into().expect("fixed identity")),
        )
        .map_err(|_| corrupt("anchor source manifest is invalid"))?;
        let anchor = Self {
            removed_through: u64_at(bytes, 8),
            source_format: u32_at(bytes, 16),
            source_manifest,
            source_frame_digest: bytes[80..112].try_into().expect("fixed frame identity"),
            original_entry: bytes[HEADER_BYTES..end].to_vec(),
        };
        anchor.validate()?;
        Ok(anchor)
    }
}

fn validate_original(
    removed_through: u64,
    source_format: u32,
    source_manifest: SnapshotManifest,
    original_entry: &[u8],
) -> Result<DurableProposal, JournalError> {
    if source_format != SNAPSHOT_FORMAT_VERSION {
        return Err(corrupt("anchor source format is unsupported"));
    }
    let applied = source_manifest.applied_index();
    if applied == 0 || removed_through > applied {
        return Err(corrupt("anchor floor and applied index are invalid"));
    }
    let entry = decode_entry(&entry_key(applied), original_entry)?;
    let original = DurableProposal::decode(entry.payload())
        .map_err(|_| corrupt("anchor original proposal is invalid"))?;
    let digest = IndexedWriter::<storage::MemoryStore>::proposal_digest(&original)?;
    if source_manifest.latest_applied_digest() != Some(digest) {
        return Err(corrupt("anchor original and indexed identity differ"));
    }
    Ok(original)
}

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("bounded header"),
    )
}
fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("bounded header"),
    )
}
fn checksum(bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(SNAPSHOT_ANCHOR_HASH_SCOPE);
    hash.update(bytes);
    hash.finalize().into()
}
fn corrupt(detail: &'static str) -> JournalError {
    JournalError::Corrupt { detail }
}

/// Post-read bounds for both the current and resulting images and the three
/// attachment puts. No backend allocation, physical space or latency bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotAnchorLimits {
    pub snapshot: SnapshotLimits,
    pub max_mutation_bytes: usize,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SnapshotAnchorError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Target(#[from] SnapshotCaptureError),
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Indexed(#[from] IndexedApplyError),
    #[error(transparent)]
    Format(#[from] SnapshotFormatError),
    #[error("anchor attachment requires a nonzero applied proposal")]
    Unapplied,
    #[error("target records differ from the exact installed capture")]
    TargetMismatch,
    #[error("target already has an anchor; attachment cannot replace history")]
    AlreadyAnchored,
    #[error("attachment needs {requested} mutation bytes, maximum {maximum}")]
    MutationLimit { requested: usize, maximum: usize },
    #[error("attachment size is not representable")]
    SizeOverflow,
    #[error("offline anchor owner is retired; close all handles and reopen")]
    Retired,
}

/// Prebuilt operational result, not authentication or a recovery certificate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotAnchorReceipt {
    pub source_manifest: SnapshotManifest,
    pub source_frame_digest: [u8; 32],
    pub target_provenance: SnapshotProvenance,
    pub mutations: usize,
    pub logical_mutation_bytes: usize,
}

/// Input-only plan: the original envelope still belongs to the inspected image.
#[derive(Clone, Copy, Debug)]
struct BorrowedAnchor<'a> {
    source_manifest: SnapshotManifest,
    source_frame_digest: [u8; 32],
    original_entry: &'a [u8],
}

/// Only this checked plan can stage an owned anchor and its three-put batch.
#[derive(Debug)]
struct CheckedAttachment<'a> {
    anchor: BorrowedAnchor<'a>,
    anchor_key: Vec<u8>,
    root_key: Vec<u8>,
    root_value: [u8; 4],
    mutation_bytes: usize,
}

impl<'a> BorrowedAnchor<'a> {
    fn preflight(
        self,
        records: &[(storage::Key, storage::Value)],
        owner_shape: (&[u8], usize),
        limits: SnapshotAnchorLimits,
    ) -> Result<CheckedAttachment<'a>, SnapshotAnchorError> {
        let anchor_bytes = SnapshotAnchor::encoded_size(self.original_entry)?;
        let anchor_key = anchor_key();
        let (root_key, root_value) = anchored_root();
        let mut replacements = [
            RecordShape {
                key: &anchor_key,
                value_bytes: anchor_bytes,
            },
            RecordShape {
                key: &root_key,
                value_bytes: root_value.len(),
            },
            RecordShape {
                key: owner_shape.0,
                value_bytes: owner_shape.1,
            },
        ];
        let mutation_bytes = replacements.iter().try_fold(0usize, |bytes, shape| {
            bytes
                .checked_add(shape.key.len())
                .and_then(|bytes| bytes.checked_add(shape.value_bytes))
                .ok_or(SnapshotAnchorError::SizeOverflow)
        })?;
        if mutation_bytes > limits.max_mutation_bytes {
            return Err(SnapshotAnchorError::MutationLimit {
                requested: mutation_bytes,
                maximum: limits.max_mutation_bytes,
            });
        }
        replacements.sort_unstable_by_key(|shape| shape.key);
        SnapshotFormat::preflight_shapes(
            true,
            resulting_record_shapes(records, replacements),
            limits.snapshot,
        )?;
        Ok(CheckedAttachment {
            anchor: self,
            anchor_key,
            root_key,
            root_value,
            mutation_bytes,
        })
    }
}

impl CheckedAttachment<'_> {
    fn stage<S: StateStore>(
        self,
        writer: &IndexedWriter<S>,
        original: &DurableProposal,
    ) -> Result<WriteBatch, SnapshotAnchorError> {
        let anchor = SnapshotAnchor {
            removed_through: 0,
            source_format: SNAPSHOT_FORMAT_VERSION,
            source_manifest: self.anchor.source_manifest,
            source_frame_digest: self.anchor.source_frame_digest,
            original_entry: self.anchor.original_entry.to_vec(),
        };
        let mut batch = writer.prepare_snapshot_anchor_owner(original)?;
        batch.push_put(self.root_key, self.root_value.to_vec());
        batch.push_put(self.anchor_key, anchor.encode()?);
        Ok(batch)
    }
}

/// Merge three replacements into the sorted borrowed image without copying
/// records or allocating an image-sized array just to measure the result.
fn resulting_record_shapes<'a>(
    records: &'a [(storage::Key, storage::Value)],
    replacements: [RecordShape<'a>; 3],
) -> impl Iterator<Item = RecordShape<'a>> + Clone {
    let mut records = records.iter().peekable();
    let mut replacements = replacements.into_iter().peekable();
    std::iter::from_fn(move || {
        match (records.peek(), replacements.peek()) {
            (Some((key, _)), Some(replacement)) => match key.as_slice().cmp(replacement.key) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    records.next();
                    return replacements.next();
                }
                std::cmp::Ordering::Greater => return replacements.next(),
            },
            (None, _) => return replacements.next(),
            (_, None) => {}
        }
        records.next().map(|(key, value)| RecordShape {
            key,
            value_bytes: value.len(),
        })
    })
}

/// Non-Clone offline owner; ALL other writers and previous owners must be gone.
/// A returned apply error or escaping original panic retires all I/O and release
/// APIs until reopen, without retries or compensating writes.
///
/// ```compile_fail
/// use cluster::SnapshotAnchorOwner;
/// use storage::MemoryStore;
/// let owner = SnapshotAnchorOwner::new(MemoryStore::default());
/// let duplicate = owner.clone();
/// ```
#[derive(Debug)]
pub struct SnapshotAnchorOwner<S: ImageInstallBackend> {
    store: S,
    retired: bool,
}

impl<S: ImageInstallBackend> SnapshotAnchorOwner<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            retired: false,
        }
    }
    pub fn is_retired(&self) -> bool {
        self.retired
    }
    fn require_usable(&self) -> Result<(), SnapshotAnchorError> {
        if self.retired {
            Err(SnapshotAnchorError::Retired)
        } else {
            Ok(())
        }
    }
    pub fn snapshot(&self) -> Result<SnapshotWithProvenance, SnapshotAnchorError> {
        self.require_usable()?;
        Ok(self.store.inspect_image()?)
    }
    pub fn into_store(self) -> Result<S, SnapshotAnchorError> {
        self.require_usable()?;
        Ok(self.store)
    }

    /// Inspects the actual backend once; validates and compares those immutable
    /// records before building the batch. No suffix reconciliation or deletion.
    pub fn prepare<'a>(
        &'a mut self,
        capture: &SnapshotCapture,
        limits: SnapshotAnchorLimits,
    ) -> Result<PreparedSnapshotAnchor<'a, S>, SnapshotAnchorError> {
        self.require_usable()?;
        if capture.manifest().applied_index() == 0 {
            return Err(SnapshotAnchorError::Unapplied);
        }
        // The same sealed native inspection as #208; its replacement primitive
        // intentionally cannot be used here because it stages record deletes.
        let image = Arc::new(self.store.inspect_image()?);
        validate_image(Arc::clone(&image), limits.snapshot)?;
        let records = image.snapshot().entries();
        let anchor_key = anchor_key();
        if records
            .binary_search_by(|(key, _)| key.cmp(&anchor_key))
            .is_ok()
        {
            return Err(SnapshotAnchorError::AlreadyAnchored);
        }
        if image.snapshot() != capture.image().snapshot() {
            return Err(SnapshotAnchorError::TargetMismatch);
        }
        let original_key = entry_key(capture.manifest().applied_index());
        let original = records
            .binary_search_by(|(key, _)| key.cmp(&original_key))
            .map_err(|_| corrupt("original applied entry is missing"))?;
        let anchor = BorrowedAnchor {
            source_manifest: capture.manifest(),
            source_frame_digest: capture.frame_digest(),
            original_entry: &records[original].1,
        };
        let original = validate_original(
            0,
            SNAPSHOT_FORMAT_VERSION,
            anchor.source_manifest,
            anchor.original_entry,
        )?;
        let writer = IndexedWriter::open(FrozenImage(Arc::clone(&image)))?;
        let (owner_key, owner_bytes) = writer.snapshot_anchor_owner_shape(&original)?;
        // All output caps precede the owned anchor, encoding and batch. The
        // exact virtual merge replaces both fences and inserts only the anchor.
        let checked = anchor.preflight(records, (&owner_key, owner_bytes), limits)?;
        let receipt = SnapshotAnchorReceipt {
            source_manifest: capture.manifest(),
            source_frame_digest: capture.frame_digest(),
            target_provenance: *image.provenance(),
            mutations: 3,
            logical_mutation_bytes: checked.mutation_bytes,
        };
        let batch = checked.stage(&writer, &original)?;
        Ok(PreparedSnapshotAnchor {
            owner: self,
            batch,
            receipt,
        })
    }

    pub fn attach(
        &mut self,
        capture: &SnapshotCapture,
        limits: SnapshotAnchorLimits,
    ) -> Result<SnapshotAnchorReceipt, SnapshotAnchorError> {
        self.prepare(capture, limits)?.apply()
    }
}

/// Single-use, non-Clone preparation borrowing the exact target owner. Dropping
/// it is mutation-free; no caller can substitute another target or batch.
///
/// ```compile_fail
/// use cluster::{PreparedSnapshotAnchor, SnapshotAnchorOwner, SnapshotAnchorLimits, SnapshotCapture};
/// use storage::MemoryStore;
/// fn detach(owner: &mut SnapshotAnchorOwner<MemoryStore>, capture: &SnapshotCapture,
///           limits: SnapshotAnchorLimits) -> PreparedSnapshotAnchor<'static, MemoryStore> {
///     owner.prepare(capture, limits).unwrap()
/// }
/// ```
pub struct PreparedSnapshotAnchor<'a, S: ImageInstallBackend> {
    owner: &'a mut SnapshotAnchorOwner<S>,
    batch: WriteBatch,
    receipt: SnapshotAnchorReceipt,
}

impl<S: ImageInstallBackend> PreparedSnapshotAnchor<'_, S> {
    pub fn apply(self) -> Result<SnapshotAnchorReceipt, SnapshotAnchorError> {
        apply_attachment(
            &self.owner.store,
            &mut self.owner.retired,
            self.batch,
            self.receipt,
        )
    }
}

fn apply_attachment<S: StateStore>(
    store: &S,
    retired: &mut bool,
    batch: WriteBatch,
    receipt: SnapshotAnchorReceipt,
) -> Result<SnapshotAnchorReceipt, SnapshotAnchorError> {
    *retired = true;
    store.apply(batch)?;
    *retired = false;
    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use storage::{FjallStore, Key, MemoryStore, Mutation, StoreSnapshot, Value};
    use tempfile::TempDir;

    fn bytes(hex: &str) -> Vec<u8> {
        hex.as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    #[test]
    fn literal_anchor_v1_freezes_manifest_frame_original_and_checksum_domains() {
        // The original is the independent domain V1 golden (n/q, time 7).
        let golden = bytes(concat!(
            "535753410000000100000000000000030000000100000002",
            "00000000000000070000000000000009000000000000000b",
            "12990f2f23abdd1af2f4f0cc930794532eb64ac351bfb41e2c4966275fd61d18",
            "2222222222222222222222222222222222222222222222222222222222222222",
            "0000004300000001000000000000000700000013",
            "b25644b87329c09bd814562aa7019f696aaa3c65cb20e18661776e89f2af1568",
            "53574450000000010000000700016e01710719",
            "6facb1edaab89fda5d2dc07bca2fb3c8f34038b0dcf9fc2e188d710d37697719"
        ));
        let decoded = SnapshotAnchor::decode(&golden).unwrap();
        assert_eq!(decoded.encode().unwrap(), golden);
        assert_eq!(
            SnapshotAnchor::encoded_size(decoded.original_entry()).unwrap(),
            golden.len()
        );
        assert_eq!(decoded.removed_through(), 3);
        assert_eq!(decoded.applied_index(), 7);
        assert_eq!(decoded.source_frame_digest(), [0x22; 32]);
        assert_eq!(
            decoded.original_proposal().unwrap().command().issued_at,
            domain::Timestamp::from_millis(7)
        );
        for len in 0..golden.len() {
            assert!(SnapshotAnchor::decode(&golden[..len]).is_err());
        }
        let mut trailing = golden.clone();
        trailing.push(0);
        assert!(SnapshotAnchor::decode(&trailing).is_err());
        let mut version = golden;
        version[7] = 2;
        let end = version.len() - 32;
        let digest = checksum(&version[..end]);
        version[end..].copy_from_slice(&digest);
        assert!(SnapshotAnchor::decode(&version).is_err());
    }

    fn limits() -> SnapshotAnchorLimits {
        SnapshotAnchorLimits {
            snapshot: SnapshotLimits {
                max_encoded_bytes: 1 << 20,
                max_records: 100,
                max_key_bytes: 1024,
                max_value_bytes: 1 << 19,
            },
            max_mutation_bytes: 1 << 20,
        }
    }
    fn seed<S: StateStore>(store: &S) -> SnapshotCapture {
        let proposal = DurableProposal::unbound(domain::Command::new(
            domain::NamespaceName::new("n").unwrap(),
            domain::EntityPath::new("q").unwrap(),
            domain::Timestamp::from_millis(7),
            domain::CommandKind::ExpireSessionLocks,
        ));
        let mut journal = crate::Journal::open(store.clone()).unwrap();
        journal.append(1, &proposal.encode().unwrap()).unwrap();
        journal.commit(1).unwrap();
        IndexedWriter::open(store.clone())
            .unwrap()
            .apply(1, &proposal)
            .unwrap();
        drop(journal);
        SnapshotCapture::logical(store, limits().snapshot).unwrap()
    }

    #[test]
    fn borrowed_plan_checks_all_output_bounds_before_owned_staging() {
        let store = MemoryStore::default();
        let capture = seed(&store);
        let image = Arc::new(store.inspect_image().unwrap());
        let records = image.snapshot().entries();
        let original_key = entry_key(capture.manifest().applied_index());
        let original_entry = &records[records
            .binary_search_by(|(key, _)| key.cmp(&original_key))
            .unwrap()]
        .1;
        let original = validate_original(
            0,
            SNAPSHOT_FORMAT_VERSION,
            capture.manifest(),
            original_entry,
        )
        .unwrap();
        let writer = IndexedWriter::open(FrozenImage(Arc::clone(&image))).unwrap();
        let (owner_key, owner_bytes) = writer.snapshot_anchor_owner_shape(&original).unwrap();
        let input = BorrowedAnchor {
            source_manifest: capture.manifest(),
            source_frame_digest: capture.frame_digest(),
            original_entry,
        };
        assert_eq!(
            input
                .preflight(records, (&owner_key, usize::MAX), limits())
                .unwrap_err(),
            SnapshotAnchorError::SizeOverflow
        );
        let checked = input
            .preflight(records, (&owner_key, owner_bytes), limits())
            .unwrap();
        assert!(std::ptr::eq(
            checked.anchor.original_entry,
            original_entry.as_slice()
        ));
        let mutation_bytes = checked.mutation_bytes;
        let batch = checked.stage(&writer, &original).unwrap();
        let mut resulting = records
            .iter()
            .cloned()
            .collect::<std::collections::BTreeMap<_, _>>();
        for mutation in batch.mutations() {
            let Mutation::Put { key, value } = mutation else {
                panic!("attachment must only put")
            };
            resulting.insert(key.clone(), value.clone());
        }
        let resulting = resulting.into_iter().collect::<Vec<_>>();
        let encoded =
            SnapshotFormat::encode(&capture.manifest(), &resulting, limits().snapshot).unwrap();
        let exact = SnapshotAnchorLimits {
            snapshot: SnapshotLimits {
                max_encoded_bytes: encoded.len(),
                max_records: resulting.len(),
                max_key_bytes: resulting.iter().map(|(key, _)| key.len()).max().unwrap(),
                max_value_bytes: resulting
                    .iter()
                    .map(|(_, value)| value.len())
                    .max()
                    .unwrap(),
            },
            max_mutation_bytes: mutation_bytes,
        };
        let checked = input
            .preflight(records, (&owner_key, owner_bytes), exact)
            .unwrap();
        assert!(std::ptr::eq(
            checked.anchor.original_entry,
            original_entry.as_slice()
        ));
        assert_eq!(
            checked.stage(&writer, &original).unwrap().mutations(),
            batch.mutations()
        );
        for boundary in 0..5 {
            for zero in [false, true] {
                let mut smaller = exact;
                let cap = match boundary {
                    0 => &mut smaller.max_mutation_bytes,
                    1 => &mut smaller.snapshot.max_encoded_bytes,
                    2 => &mut smaller.snapshot.max_records,
                    3 => &mut smaller.snapshot.max_key_bytes,
                    _ => &mut smaller.snapshot.max_value_bytes,
                };
                *cap = if zero { 0 } else { *cap - 1 };
                // A failed borrowed preflight cannot produce the type whose
                // stage method clones/encodes the anchor or creates its batch.
                let error = input
                    .preflight(records, (&owner_key, owner_bytes), smaller)
                    .unwrap_err();
                if boundary == 0 {
                    assert_eq!(
                        error,
                        SnapshotAnchorError::MutationLimit {
                            requested: mutation_bytes,
                            maximum: smaller.max_mutation_bytes,
                        }
                    );
                } else {
                    let expected = match boundary {
                        1 => super::super::SnapshotLimit::EncodedBytes,
                        2 => super::super::SnapshotLimit::Records,
                        3 => super::super::SnapshotLimit::KeyBytes,
                        _ => super::super::SnapshotLimit::ValueBytes,
                    };
                    assert!(matches!(error, SnapshotAnchorError::Format(
                        SnapshotFormatError::LimitExceeded { kind, .. }
                    ) if kind == expected));
                }
            }
        }
        assert_eq!(store.snapshot().unwrap(), *image.snapshot());
    }

    #[test]
    fn virtual_merge_matches_canonical_replacement_without_copying_old_values() {
        let records = vec![
            (vec![1], vec![0; 11]),
            (vec![3], vec![0; 13]),
            (vec![5], vec![0; 15]),
        ];
        for replacements in [
            [(0, 20), (3, 23), (6, 26)],
            [(1, 21), (2, 22), (5, 25)],
            [(2, 22), (3, 23), (4, 24)],
        ] {
            let keys = replacements.map(|(key, _)| [key]);
            let shapes = std::array::from_fn(|index| RecordShape {
                key: &keys[index],
                value_bytes: replacements[index].1,
            });
            let actual = resulting_record_shapes(&records, shapes)
                .map(|shape| (shape.key.to_vec(), shape.value_bytes))
                .collect::<Vec<_>>();
            let mut expected = records
                .iter()
                .map(|(key, value)| (key.clone(), value.len()))
                .collect::<std::collections::BTreeMap<_, _>>();
            for shape in shapes {
                expected.insert(shape.key.to_vec(), shape.value_bytes);
            }
            assert_eq!(actual, expected.into_iter().collect::<Vec<_>>());
        }
    }

    #[derive(Clone)]
    struct Faulty<S> {
        inner: S,
        mode: u8,
        calls: Arc<AtomicUsize>,
        payload: Arc<String>,
        observed: Arc<Mutex<Vec<Mutation>>>,
    }
    impl<S: StateStore> StateStore for Faulty<S> {
        fn get(&self, _: &[u8]) -> Result<Option<Value>, StorageError> {
            panic!("apply must not read")
        }
        fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
            panic!("apply must not recapture")
        }
        fn scan_from(
            &self,
            _: &[u8],
            _: &[u8],
            _: usize,
        ) -> Result<Vec<(Key, Value)>, StorageError> {
            panic!("apply must not scan")
        }
        fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.observed.lock().unwrap() = batch.mutations().to_vec();
            if self.mode == 1 {
                return Err(StorageError::LockPoisoned);
            }
            if self.mode == 2 {
                panic_any(Arc::clone(&self.payload));
            }
            self.inner.apply(batch)?;
            if self.mode == 3 {
                return Err(StorageError::LockPoisoned);
            }
            if self.mode == 4 {
                panic_any(Arc::clone(&self.payload));
            }
            Ok(())
        }
    }
    fn faults<S: ImageInstallBackend>(store: S, mode: u8) -> StoreSnapshot {
        let capture = seed(&store);
        let before = store.snapshot().unwrap();
        let mut owner = SnapshotAnchorOwner::new(store);
        let PreparedSnapshotAnchor {
            owner: borrowed,
            batch,
            receipt,
        } = owner.prepare(&capture, limits()).unwrap();
        let expected = batch.mutations().to_vec();
        assert_eq!(expected.len(), 3);
        assert!(
            expected
                .iter()
                .all(|mutation| matches!(mutation, Mutation::Put { .. }))
        );
        // Private test seam at the *same* production apply function. The public
        // backend remains sealed; no production provenance/fault escape hatch.
        let calls = Arc::new(AtomicUsize::new(0));
        let payload = Arc::new("original attachment panic".to_owned());
        let observed = Arc::new(Mutex::new(Vec::new()));
        let fault = Faulty {
            inner: borrowed.store.clone(),
            mode,
            calls: Arc::clone(&calls),
            payload: Arc::clone(&payload),
            observed: Arc::clone(&observed),
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            apply_attachment(&fault, &mut borrowed.retired, batch, receipt)
        }));
        match mode {
            0 => assert_eq!(result.unwrap().unwrap(), receipt),
            1 | 3 => assert_eq!(
                result.unwrap(),
                Err(SnapshotAnchorError::Storage(StorageError::LockPoisoned))
            ),
            2 | 4 => assert!(Arc::ptr_eq(
                &result.unwrap_err().downcast::<Arc<String>>().unwrap(),
                &payload
            )),
            _ => unreachable!(),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(*observed.lock().unwrap(), expected);
        let actual = fault.inner.snapshot().unwrap();
        if mode == 1 || mode == 2 {
            assert_eq!(actual, before);
        } else {
            let mut expected_rows = before
                .entries()
                .iter()
                .cloned()
                .collect::<std::collections::BTreeMap<_, _>>();
            for mutation in &expected {
                let Mutation::Put { key, value } = mutation else {
                    unreachable!()
                };
                expected_rows.insert(key.clone(), value.clone());
            }
            let expected_rows = expected_rows.into_iter().collect::<Vec<_>>();
            assert_eq!(actual.entries(), expected_rows.as_slice());
            crate::CommittedReplay::open(fault.inner.clone()).unwrap();
        }
        drop(fault);
        if mode != 0 {
            assert!(owner.is_retired());
            assert_eq!(owner.snapshot(), Err(SnapshotAnchorError::Retired));
            assert!(matches!(
                owner.prepare(&capture, limits()),
                Err(SnapshotAnchorError::Retired)
            ));
            assert!(matches!(
                owner.into_store(),
                Err(SnapshotAnchorError::Retired)
            ));
        } else {
            assert!(!owner.is_retired());
            drop(owner.into_store().unwrap());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        actual
    }
    #[test]
    fn memory_exact_three_put_boundary_errors_and_original_panics_retire() {
        for mode in 0..5 {
            faults(MemoryStore::default(), mode);
        }
    }
    #[test]
    fn fjall_exact_three_put_boundary_errors_and_original_panics_full_reopen() {
        for mode in 0..5 {
            let dir = TempDir::new().unwrap();
            let expected = faults(FjallStore::open(dir.path()).unwrap(), mode);
            assert_eq!(
                FjallStore::open(dir.path()).unwrap().snapshot().unwrap(),
                expected
            );
        }
    }
}
