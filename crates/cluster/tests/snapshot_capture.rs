//! Offline Memory/Fjall capture controls, not online-writer or recovery proof.
//! Raw edits are fixture setup. Every database handle drops before reopening.

use std::{
    collections::BTreeSet,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use cluster::{
    Journal, ReplayError, SnapshotCapture, SnapshotCaptureError, SnapshotFormat,
    SnapshotFormatError, SnapshotLimit, SnapshotLimits,
};
use domain::{
    BoundCommand, Command, CommandKind, CommandOutcome, DurableProposal, DurableProposalError,
    EntityPath, IndexedApplyError, IndexedApplyOutcome, IndexedWriter, MessageEnvelope,
    NamespaceName, QueueConfig, QueueCounters, ReceiveMode, RuleFilter, RuleName, SessionId,
    StateMachine, SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
};
use sha2::{Digest, Sha256};
use storage::{
    ACTIVE_STORE_FORMAT, FjallStore, Key, MemoryStore, SnapshotProvenance, StateStore,
    StorageError, StoreSnapshot, Value, WriteBatch,
};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error>>;
type Rows = Vec<(Key, Value)>;
const F0: &[u8] = b"\xF0switchyard/journal\0";
const F1: &[u8] = b"\xF1switchyard/replay\0";

fn limits() -> SnapshotLimits {
    SnapshotLimits {
        max_encoded_bytes: 32 * 1024 * 1024,
        max_records: 100_000,
        max_key_bytes: 4096,
        max_value_bytes: 2 * 1024 * 1024,
    }
}

#[derive(Clone, Debug)]
enum Backend {
    Memory(MemoryStore),
    Fjall(FjallStore),
}

impl StateStore for Backend {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        match self {
            Self::Memory(store) => store.get(key),
            Self::Fjall(store) => store.get(key),
        }
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        match self {
            Self::Memory(store) => store.apply(batch),
            Self::Fjall(store) => store.apply(batch),
        }
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        match self {
            Self::Memory(store) => store.snapshot(),
            Self::Fjall(store) => store.snapshot(),
        }
    }
    fn scan_from(&self, prefix: &[u8], start: &[u8], limit: usize) -> Result<Rows, StorageError> {
        match self {
            Self::Memory(store) => store.scan_from(prefix, start, limit),
            Self::Fjall(store) => store.scan_from(prefix, start, limit),
        }
    }
}

struct Fixture {
    store: Option<Backend>,
    directory: Option<TempDir>,
}

impl Fixture {
    fn new(durable: bool) -> Self {
        let directory = durable.then(|| TempDir::new().unwrap());
        let store = match &directory {
            Some(directory) => Backend::Fjall(FjallStore::open(directory.path()).unwrap()),
            None => Backend::Memory(MemoryStore::default()),
        };
        Self {
            store: Some(store),
            directory,
        }
    }
    fn store(&self) -> &Backend {
        self.store.as_ref().unwrap()
    }
    fn rows(&self) -> Rows {
        self.store().snapshot().unwrap().entries().to_vec()
    }
    fn raw(&self, batch: WriteBatch) {
        self.store().apply(batch).unwrap();
    }
    fn log(&self, payloads: &[Vec<u8>], committed: u64) {
        let mut journal = Journal::open(self.store().clone()).unwrap();
        for (offset, payload) in payloads.iter().enumerate() {
            journal.append(offset as u64 + 1, payload).unwrap();
        }
        journal.commit(committed).unwrap();
    }
    fn seed(&self, proposals: &[DurableProposal]) {
        let mut writer = IndexedWriter::open(self.store().clone()).unwrap();
        for (offset, proposal) in proposals.iter().enumerate() {
            writer.apply(offset as u64 + 1, proposal).unwrap();
        }
    }
    fn capture(&self, limits: SnapshotLimits) -> Result<SnapshotCapture, SnapshotCaptureError> {
        match self.store() {
            Backend::Memory(store) => SnapshotCapture::logical(store, limits),
            Backend::Fjall(store) => SnapshotCapture::fjall(store, limits),
        }
    }
    fn check(&self) -> SnapshotCapture {
        let before = self.rows();
        let captured = self.capture(limits()).unwrap();
        assert_eq!(captured.image().snapshot().entries(), before);
        let view = SnapshotFormat::decode(captured.encoded(), limits()).unwrap();
        assert_eq!(view.manifest(), captured.manifest());
        assert_eq!(
            view.records()
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect::<Rows>(),
            before
        );
        match (self.store(), captured.provenance()) {
            (Backend::Memory(_), SnapshotProvenance::LogicalOnly) => {}
            (Backend::Fjall(_), SnapshotProvenance::Fjall(metadata)) => {
                assert_eq!(metadata.format_version(), ACTIVE_STORE_FORMAT);
                assert_eq!(metadata.format_marker(), &ACTIVE_STORE_FORMAT.to_be_bytes());
            }
            _ => panic!("capture provenance does not match the selected API"),
        }
        assert_eq!(
            self.rows(),
            before,
            "capture preserves every source and Clock byte"
        );
        captured
    }
    fn reject(&self) -> SnapshotCaptureError {
        let before = self.rows();
        let error = self.capture(limits()).unwrap_err();
        assert_eq!(
            self.rows(),
            before,
            "refusal preserves every source and Clock byte"
        );
        error
    }
    fn reopen(&mut self) {
        // Callers drop all writers, wrappers and cloned handles first.
        self.store = Some(match self.store.take().unwrap() {
            Backend::Memory(store) => Backend::Memory(store),
            Backend::Fjall(store) => {
                drop(store);
                Backend::Fjall(FjallStore::open(self.directory.as_ref().unwrap().path()).unwrap())
            }
        });
    }
}

fn ns() -> NamespaceName {
    NamespaceName::new("tenant").unwrap()
}
fn path(name: &str) -> EntityPath {
    EntityPath::new(name).unwrap()
}
fn proposal(at: u64, entity: &str, kind: CommandKind) -> DurableProposal {
    DurableProposal::unbound(Command::new(
        ns(),
        path(entity),
        Timestamp::from_millis(at),
        kind,
    ))
}
fn create(at: u64) -> DurableProposal {
    proposal(
        at,
        "orders",
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )
}
fn noop(at: u64) -> DurableProposal {
    proposal(at, "orders", CommandKind::ExpireSessionLocks)
}
fn key(prefix: &[u8], tag: u8) -> Key {
    let mut key = prefix.to_vec();
    key.push(tag);
    key
}
fn entry_key(index: u64) -> Key {
    let mut key = key(F0, 2);
    key.extend_from_slice(&index.to_be_bytes());
    key
}
fn hash(scope: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(scope);
    hash.update(bytes);
    hash.finalize().into()
}
fn checkpoint(index: u64, proposal: &DurableProposal) -> Value {
    let mut value = index.to_be_bytes().to_vec();
    value.extend_from_slice(&hash(
        b"switchyard indexed proposal v1\0",
        &proposal.encode().unwrap(),
    ));
    value.extend_from_slice(&hash(b"switchyard indexed checkpoint v1\0", &value));
    value
}
fn frontier(index: u64) -> Value {
    let mut value = 1_u32.to_be_bytes().to_vec();
    value.extend_from_slice(&index.to_be_bytes());
    value.extend_from_slice(&hash(b"switchyard journal frontier v1\0", &value));
    value
}
fn owner(version: u32) -> Value {
    let mut value = b"SWIA".to_vec();
    value.extend_from_slice(&version.to_be_bytes());
    value.extend_from_slice(&hash(b"switchyard indexed owner v1\0", &value));
    value
}
fn value(fixture: &Fixture, key: &[u8]) -> Value {
    fixture.store().get(key).unwrap().unwrap()
}
fn flip(mut value: Value, offset: usize) -> Value {
    value[offset] ^= 1;
    value
}

fn empty_and_adoption(durable: bool) {
    for initialized in [false, true] {
        let mut fixture = Fixture::new(durable);
        if initialized {
            fixture.log(&[vec![0xff]], 0);
            fixture.raw(WriteBatch::default().delete(entry_key(1)));
        }
        let captured = fixture.check();
        assert_eq!(captured.manifest().applied_index(), 0);
        assert_eq!(captured.manifest().committed_index(), 0);
        assert_eq!(captured.manifest().last_appended_index(), 0);
        assert_eq!(captured.manifest().latest_applied_digest(), None);
        fixture.reopen();
        assert_eq!(fixture.check(), captured);
    }
    // An empty domain may be waiting for application, including opaque tails.
    let fixture = Fixture::new(durable);
    fixture.log(&[create(1).encode().unwrap(), vec![0xff]], 1);
    let captured = fixture.check();
    assert_eq!(captured.manifest().applied_index(), 0);
    assert_eq!(captured.manifest().committed_index(), 1);
    assert_eq!(captured.manifest().last_appended_index(), 2);

    // A deterministic refusal can establish a nonzero checkpoint with NO domain.
    let fixture = Fixture::new(durable);
    let original = proposal(
        1,
        "missing",
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    );
    let mut writer = IndexedWriter::open(fixture.store().clone()).unwrap();
    assert!(matches!(
        writer.apply(1, &original),
        Ok(IndexedApplyOutcome::Refused(_))
    ));
    drop(writer);
    fixture.log(&[original.encode().unwrap()], 1);
    assert!(fixture.rows().iter().all(|(key, _)| key[0] >= 0xf0));
    assert_eq!(fixture.check().manifest().applied_index(), 1);
    // Clock-only remains legal with valid ownership, without asserting reachability.
    fixture.raw(WriteBatch::default().put(
        keys::clock(),
        codec::encode(&Timestamp::from_millis(1)).unwrap(),
    ));
    fixture.check();

    for clock_only in [false, true] {
        let fixture = Fixture::new(durable);
        if clock_only {
            fixture.raw(WriteBatch::default().put(
                keys::clock(),
                codec::encode(&Timestamp::from_millis(1)).unwrap(),
            ));
        } else {
            StateMachine::new(fixture.store().clone())
                .apply(create(1).command())
                .unwrap();
        }
        assert!(matches!(
            fixture.reject(),
            SnapshotCaptureError::Replay(ReplayError::Indexed(IndexedApplyError::PopulatedStore))
        ));
    }
}

fn frontiers_identity_and_opaque_tail(durable: bool) {
    let mut fixture = Fixture::new(durable);
    let originals = (1..=70)
        .map(|i| if i == 1 { create(i) } else { noop(i) })
        .collect::<Vec<_>>();
    let mut payloads = originals
        .iter()
        .map(|p| p.encode().unwrap())
        .collect::<Vec<_>>();
    payloads.extend([Vec::new(), vec![0xff, 0, 0x80]]);
    fixture.log(&payloads, 70);
    fixture.seed(&originals[..1]);
    let captured = fixture.check();
    assert_eq!(captured.manifest().applied_index(), 1);
    assert_eq!(captured.manifest().committed_index(), 70);
    assert_eq!(captured.manifest().last_appended_index(), 72);
    let expected = hash(
        b"switchyard indexed proposal v1\0",
        &originals[0].encode().unwrap(),
    );
    assert_eq!(captured.manifest().latest_applied_digest(), Some(expected));
    assert_ne!(expected.as_slice(), &value(&fixture, &entry_key(1))[16..48]);
    let logical = SnapshotCapture::logical(fixture.store(), limits()).unwrap();
    assert_eq!(logical.provenance(), &SnapshotProvenance::LogicalOnly);
    assert_eq!(
        logical.encoded(),
        captured.encoded(),
        "frames do not encode provenance"
    );
    fixture.reopen();
    assert_eq!(fixture.check(), captured);
    fixture.seed_more(2, &originals[1..]);
    assert_eq!(fixture.check().manifest().applied_index(), 70);
    // The same opaque payload becomes schema-invalid only when committed.
    let mut journal = Journal::open(fixture.store().clone()).unwrap();
    journal.commit(71).unwrap();
    drop(journal);
    assert!(matches!(
        fixture.reject(),
        SnapshotCaptureError::Replay(ReplayError::Proposal { index: 71, .. })
    ));
    assert_eq!(
        captured.manifest().applied_index(),
        1,
        "earlier captures remain immutable"
    );
}

impl Fixture {
    fn seed_more(&self, from: u64, proposals: &[DurableProposal]) {
        let mut writer = IndexedWriter::open(self.store().clone()).unwrap();
        for (offset, proposal) in proposals.iter().enumerate() {
            writer.apply(from + offset as u64, proposal).unwrap();
        }
    }
}

fn corrupt_reserved_rows(durable: bool) {
    for case in 0..31 {
        let fixture = Fixture::new(durable);
        let first = create(1);
        fixture.log(&[first.encode().unwrap(), noop(2).encode().unwrap()], 1);
        fixture.seed(std::slice::from_ref(&first));
        let batch = match case {
            0 => WriteBatch::default().put(vec![0xf0], vec![]),
            1 => WriteBatch::default().put(b"\xF0other".to_vec(), vec![]),
            2 => WriteBatch::default().delete(key(F0, 0)),
            3 => WriteBatch::default().put(key(F0, 0), u32::MAX.to_be_bytes().to_vec()),
            4 => WriteBatch::default().delete(key(F0, 1)),
            5 => WriteBatch::default().put(key(F0, 1), flip(value(&fixture, &key(F0, 1)), 20)),
            6 => WriteBatch::default().put(key(F0, 1), frontier(3)),
            7 => WriteBatch::default().delete(entry_key(1)),
            8 => WriteBatch::default().put(entry_key(0), value(&fixture, &entry_key(1))),
            9 => {
                let mut key = entry_key(2);
                key.push(0);
                WriteBatch::default().put(key, value(&fixture, &entry_key(2)))
            }
            10 => WriteBatch::default().put(key(F0, 3), vec![]),
            11 => WriteBatch::default().put(entry_key(2), flip(value(&fixture, &entry_key(2)), 20)),
            12 => WriteBatch::default().put(key(F0, 0), vec![1]),
            13 => WriteBatch::default().put(F0.to_vec(), vec![]),
            14 => WriteBatch::default().put(entry_key(2), value(&fixture, &entry_key(1))),
            15 => {
                let mut bytes = value(&fixture, &entry_key(2));
                bytes[..4].copy_from_slice(&u32::MAX.to_be_bytes());
                WriteBatch::default().put(entry_key(2), bytes)
            }
            16 => WriteBatch::default().put(entry_key(2), flip(value(&fixture, &entry_key(2)), 15)),
            17 => {
                let mut bytes = value(&fixture, &entry_key(2));
                bytes.push(0);
                WriteBatch::default().put(entry_key(2), bytes)
            }
            18 => WriteBatch::default().delete(key(F1, 0)),
            19 => WriteBatch::default().delete(key(F1, 1)),
            20 => WriteBatch::default().put(b"\xF1other".to_vec(), vec![]),
            21 => {
                let mut key = key(F1, 0);
                key.push(0);
                WriteBatch::default()
                    .delete(self::key(F1, 0))
                    .put(key, owner(1))
            }
            22 => WriteBatch::default().put(key(F1, 0), owner(u32::MAX)),
            23 => WriteBatch::default().put(key(F1, 0), flip(owner(1), 10)),
            24 => WriteBatch::default().put(key(F1, 1), flip(value(&fixture, &key(F1, 1)), 40)),
            25 => WriteBatch::default().put(key(F1, 1), checkpoint(0, &first)),
            26 => WriteBatch::default().put(key(F1, 1), checkpoint(2, &noop(2))),
            27 => WriteBatch::default().put(key(F1, 1), checkpoint(1, &noop(1))),
            28 => WriteBatch::default().put(F1.to_vec(), vec![]),
            29 => WriteBatch::default().put(key(F1, 1), vec![0; 71]),
            _ => WriteBatch::default().put(
                entry_key(2),
                vec![0; cluster::MAX_JOURNAL_PAYLOAD_BYTES + 49],
            ),
        };
        fixture.raw(batch);
        let error = fixture.reject();
        match case {
            0..=1 => assert!(matches!(error, SnapshotCaptureError::JournalNamespace(_))),
            2..=17 | 30 => assert!(
                matches!(error, SnapshotCaptureError::Replay(ReplayError::Journal(_))),
                "case {case}: {error:?}"
            ),
            26 => assert!(matches!(
                error,
                SnapshotCaptureError::Replay(ReplayError::AppliedAhead {
                    applied: 2,
                    committed: 1
                })
            )),
            27 => assert!(matches!(
                error,
                SnapshotCaptureError::Replay(ReplayError::Indexed(
                    IndexedApplyError::ConflictingProposal { index: 1 }
                ))
            )),
            _ => assert!(
                matches!(error, SnapshotCaptureError::Replay(ReplayError::Indexed(_))),
                "case {case}: {error:?}"
            ),
        }
    }
    let fixture = Fixture::new(durable);
    fixture.seed(&[create(1)]);
    assert!(matches!(
        fixture.reject(),
        SnapshotCaptureError::Replay(ReplayError::AppliedAhead {
            applied: 1,
            committed: 0
        })
    ));
}

fn committed_schemas(durable: bool) {
    let good = noop(2).encode().unwrap();
    let mut version = good.clone();
    // A genuinely unsupported version, including after proposal V2 is introduced.
    version[4..8].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        DurableProposal::decode(&version),
        Err(DurableProposalError::UnsupportedVersion(u32::MAX))
    );
    let mut trailing = good.clone();
    trailing.push(0);
    let mut length = good.clone();
    length[11] ^= 1;
    let mut alias = good.clone();
    alias[12] |= 128;
    alias.insert(13, 0);
    alias[11] += 1;
    assert_eq!(
        DurableProposal::decode(&alias),
        Err(DurableProposalError::NonCanonical)
    );
    let mut tag = good;
    tag[12] = 127;
    let machine = StateMachine::new(MemoryStore::default());
    machine.apply(create(1).command()).unwrap();
    let binding = machine.bind_entity(&ns(), &path("orders")).unwrap();
    let mut authority =
        DurableProposal::bound(BoundCommand::new(binding, noop(2).command().clone()))
            .unwrap()
            .encode()
            .unwrap();
    let command_namespace = authority
        .windows(6)
        .rposition(|bytes| bytes == b"tenant")
        .unwrap();
    authority[command_namespace..command_namespace + 6].copy_from_slice(b"otherx");
    assert_eq!(
        DurableProposal::decode(&authority),
        Err(DurableProposalError::InvalidAuthority)
    );
    for malformed in [
        Vec::new(),
        vec![0xff],
        version,
        trailing,
        length,
        alias,
        tag,
        authority,
    ] {
        for (bad_index, applied) in [(1, 0), (33, 0), (70, 0), (1, 2)] {
            let fixture = Fixture::new(durable);
            let originals = (1..=70)
                .map(|i| if i == 1 { create(i) } else { noop(i) })
                .collect::<Vec<_>>();
            let mut payloads = originals
                .iter()
                .map(|p| p.encode().unwrap())
                .collect::<Vec<_>>();
            payloads[bad_index - 1] = malformed.clone();
            fixture.log(&payloads, 70);
            fixture.seed(&originals[..applied]);
            assert!(
                matches!(fixture.reject(), SnapshotCaptureError::Replay(ReplayError::Proposal { index, .. }) if index == bad_index as u64)
            );
        }
    }
}

fn domain_and_limit_refusals(durable: bool) {
    for case in 0..8 {
        let fixture = Fixture::new(durable);
        let first = create(1);
        fixture.log(&[first.encode().unwrap()], 1);
        fixture.seed(&[first]);
        let batch = match case {
            0 => WriteBatch::default().put(vec![0x12], vec![]),
            1 => WriteBatch::default().put(keys::clock(), vec![0xff]),
            2 => WriteBatch::default().delete(keys::clock()),
            3 => WriteBatch::default().put(vec![0x03], vec![]),
            4 => {
                WriteBatch::default().put(keys::entity_metadata(&ns(), &path("orders")), vec![0xff])
            }
            5 => WriteBatch::default().put(Vec::new(), vec![]),
            6 => WriteBatch::default().put(vec![0xf2], vec![]),
            _ => WriteBatch::default().put(b"format_version".to_vec(), vec![0, 0, 0, 2]),
        };
        fixture.raw(batch);
        assert!(matches!(fixture.reject(), SnapshotCaptureError::State(_)));
    }
    // Empty, journal-only and applied images cover both identity-header sizes.
    for shape in 0..3 {
        let fixture = Fixture::new(durable);
        if shape != 0 {
            let first = create(1);
            fixture.log(&[first.encode().unwrap()], 1);
            if shape == 2 {
                fixture.seed(&[first]);
            }
        }
        shared_limit_boundaries(&fixture);
    }
}

fn shared_limit_boundaries(fixture: &Fixture) {
    let before = fixture.rows();
    let captured = fixture.check();
    let manifest = captured.manifest();
    let exact = exact_limits(&before, captured.encoded().len());
    assert_eq!(fixture.capture(exact).unwrap(), captured);
    assert_eq!(
        SnapshotFormat::encode(&manifest, &before, exact).unwrap(),
        captured.encoded()
    );
    assert_eq!(
        SnapshotFormat::decode(captured.encoded(), exact)
            .unwrap()
            .manifest(),
        manifest
    );
    for kind in [
        SnapshotLimit::Records,
        SnapshotLimit::KeyBytes,
        SnapshotLimit::ValueBytes,
        SnapshotLimit::EncodedBytes,
    ] {
        let mut smaller = exact;
        let cap = match kind {
            SnapshotLimit::Records => &mut smaller.max_records,
            SnapshotLimit::KeyBytes => &mut smaller.max_key_bytes,
            SnapshotLimit::ValueBytes => &mut smaller.max_value_bytes,
            SnapshotLimit::EncodedBytes => &mut smaller.max_encoded_bytes,
        };
        let requested = *cap;
        if requested == 0 {
            continue;
        }
        *cap -= 1;
        let expected = SnapshotFormatError::LimitExceeded {
            kind,
            requested: u64::try_from(requested).unwrap(),
            maximum: requested - 1,
        };
        assert_eq!(
            SnapshotFormat::encode(&manifest, &before, smaller).unwrap_err(),
            expected
        );
        assert_eq!(
            SnapshotFormat::decode(captured.encoded(), smaller).unwrap_err(),
            expected
        );
        assert_eq!(
            fixture.capture(smaller),
            Err(SnapshotCaptureError::Format(expected))
        );
        assert_eq!(fixture.rows(), before);
    }
}

fn exact_limits(records: &[(Key, Value)], encoded_bytes: usize) -> SnapshotLimits {
    SnapshotLimits {
        max_encoded_bytes: encoded_bytes,
        max_records: records.len(),
        max_key_bytes: records.iter().map(|(k, _)| k.len()).max().unwrap_or(0),
        max_value_bytes: records.iter().map(|(_, v)| v.len()).max().unwrap_or(0),
    }
}

fn limits_precede_domain_and_committed_decoding(durable: bool) {
    for applied in [false, true] {
        for malformed_domain in [false, true] {
            let fixture = Fixture::new(durable);
            let first = create(1);
            // The malformed tail is opaque until committed, so both identity
            // widths start with a valid image and can be captured at exact caps.
            fixture.log(&[first.encode().unwrap(), vec![0xff]], 1);
            if applied {
                fixture.seed(&[first]);
            }
            shared_limit_boundaries(&fixture);
            let baseline = fixture.check();
            assert_eq!(
                baseline.manifest().latest_applied_digest().is_some(),
                applied
            );
            if malformed_domain {
                fixture.raw(WriteBatch::default().put(keys::clock(), vec![0xff]));
            } else {
                Journal::open(fixture.store().clone())
                    .unwrap()
                    .commit(2)
                    .unwrap();
            }
            let before = fixture.rows();
            // The earlier valid manifest supplies only the identity width for
            // this structural size calculation; it cannot certify corrupt rows.
            let encoded = SnapshotFormat::encode(&baseline.manifest(), &before, limits()).unwrap();
            let exact = exact_limits(&before, encoded.len());
            let error = fixture.capture(exact).unwrap_err();
            if malformed_domain {
                assert!(matches!(error, SnapshotCaptureError::State(_)));
            } else {
                assert!(matches!(
                    error,
                    SnapshotCaptureError::Replay(ReplayError::Proposal { index: 2, .. })
                ));
            }
            assert_eq!(fixture.rows(), before);
            for kind in [
                SnapshotLimit::Records,
                SnapshotLimit::KeyBytes,
                SnapshotLimit::ValueBytes,
                SnapshotLimit::EncodedBytes,
            ] {
                let mut smaller = exact;
                let cap = match kind {
                    SnapshotLimit::Records => &mut smaller.max_records,
                    SnapshotLimit::KeyBytes => &mut smaller.max_key_bytes,
                    SnapshotLimit::ValueBytes => &mut smaller.max_value_bytes,
                    SnapshotLimit::EncodedBytes => &mut smaller.max_encoded_bytes,
                };
                let requested = *cap;
                *cap -= 1;
                let expected = SnapshotFormatError::LimitExceeded {
                    kind,
                    requested: u64::try_from(requested).unwrap(),
                    maximum: requested - 1,
                };
                assert_eq!(
                    fixture.capture(smaller),
                    Err(SnapshotCaptureError::Format(expected))
                );
                assert_eq!(fixture.rows(), before);
            }
        }
    }
}

/// Live access is forbidden. A single owned response is all validation can see.
struct SnapshotOnly {
    store: Backend,
    snapshots: Arc<AtomicUsize>,
    failure: Option<StorageError>,
    after_image: Option<WriteBatch>,
}
impl Clone for SnapshotOnly {
    fn clone(&self) -> Self {
        panic!("capture must not clone the live source")
    }
}
impl StateStore for SnapshotOnly {
    fn get(&self, _: &[u8]) -> Result<Option<Value>, StorageError> {
        panic!("live get during capture")
    }
    fn apply(&self, _: WriteBatch) -> Result<(), StorageError> {
        panic!("live apply during capture")
    }
    fn scan_from(&self, _: &[u8], _: &[u8], _: usize) -> Result<Rows, StorageError> {
        panic!("live scan during capture")
    }
    fn scan_prefix(&self, _: &[u8], _: usize) -> Result<Rows, StorageError> {
        panic!("live prefix scan during capture")
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        assert_eq!(
            self.snapshots.fetch_add(1, Ordering::SeqCst),
            0,
            "no capture retry"
        );
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let image = self.store.snapshot()?;
        if let Some(batch) = &self.after_image {
            self.store.apply(batch.clone())?;
        }
        Ok(image)
    }
}

fn one_image_and_failure(durable: bool) {
    let fixture = Fixture::new(durable);
    let first = create(1);
    fixture.log(&[first.encode().unwrap()], 1);
    fixture.seed(&[first]);
    let before = fixture.rows();
    // These are synthetic source faults, including typed metadata variants.
    // Native metadata/keyspace corruption stays in the storage owner's tests;
    // the real Fjall capture/reopen cases here exercise valid provenance only.
    for failure in [
        None,
        Some(StorageError::LockPoisoned),
        Some(StorageError::Backend {
            operation: "test snapshot",
            detail: "original failure".into(),
        }),
        Some(StorageError::CorruptMetadata {
            detail: "original metadata failure".into(),
        }),
        Some(StorageError::UnsupportedStoreFormat {
            found: u32::MAX,
            expected: ACTIVE_STORE_FORMAT,
        }),
    ] {
        let snapshots = Arc::new(AtomicUsize::new(0));
        let observed = SnapshotOnly {
            store: fixture.store().clone(),
            snapshots: Arc::clone(&snapshots),
            failure: failure.clone(),
            after_image: None,
        };
        let result = SnapshotCapture::logical(&observed, limits());
        match failure {
            Some(error) => assert_eq!(result, Err(SnapshotCaptureError::Storage(error))),
            None => assert_eq!(result.unwrap().image().snapshot().entries(), before),
        }
        assert_eq!(snapshots.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.rows(), before);
    }

    let corrupt = Fixture::new(durable);
    corrupt.raw(WriteBatch::default().put(b"\xF0other".to_vec(), vec![]));
    let corrupt_before = corrupt.rows();
    let snapshots = Arc::new(AtomicUsize::new(0));
    let observed = SnapshotOnly {
        store: corrupt.store().clone(),
        snapshots: Arc::clone(&snapshots),
        failure: None,
        after_image: None,
    };
    assert!(matches!(
        SnapshotCapture::logical(&observed, limits()),
        Err(SnapshotCaptureError::JournalNamespace(_))
    ));
    assert_eq!(snapshots.load(Ordering::SeqCst), 1);
    assert_eq!(corrupt.rows(), corrupt_before);

    // Synthetic boundary control, NOT an online-capture guarantee: publish one
    // complete later batch after taking the response image. A paged live walk
    // would observe the replacement; validation/encoding must retain only old bytes.
    let replacement = Fixture::new(durable);
    let newer = [
        create(1),
        proposal(
            2,
            "orders",
            CommandKind::Send {
                message_id: "later".into(),
                body: vec![0xff, 0, 0x80],
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            },
        ),
    ];
    replacement.log(
        &newer
            .iter()
            .map(|p| p.encode().unwrap())
            .collect::<Vec<_>>(),
        2,
    );
    replacement.seed(&newer);
    assert_ne!(
        value(&fixture, &keys::clock()),
        value(&replacement, &keys::clock())
    );
    let mut batch = WriteBatch::default();
    for (key, _) in &before {
        batch.push_delete(key.clone());
    }
    for (key, value) in replacement.rows() {
        batch.push_put(key, value);
    }
    let snapshots = Arc::new(AtomicUsize::new(0));
    let observed = SnapshotOnly {
        store: fixture.store().clone(),
        snapshots: Arc::clone(&snapshots),
        failure: None,
        after_image: Some(batch),
    };
    let captured = SnapshotCapture::logical(&observed, limits()).unwrap();
    assert_eq!(captured.image().snapshot().entries(), before);
    assert_eq!(captured.manifest().applied_index(), 1);
    assert_eq!(captured.manifest().committed_index(), 1);
    assert_eq!(snapshots.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.rows(), replacement.rows());
    let later = fixture.check();
    assert_eq!(later.manifest().applied_index(), 2);
    assert_eq!(later.manifest().committed_index(), 2);
}

struct History {
    writer: IndexedWriter<Backend>,
    journal: Journal<Backend>,
    index: u64,
}
impl History {
    fn new(fixture: &Fixture) -> Self {
        Self {
            writer: IndexedWriter::open(fixture.store().clone()).unwrap(),
            journal: Journal::open(fixture.store().clone()).unwrap(),
            index: 0,
        }
    }
    fn apply(&mut self, entity: &str, kind: CommandKind) -> CommandOutcome {
        self.index += 1;
        let proposal = proposal(self.index, entity, kind);
        self.journal
            .append(self.index, &proposal.encode().unwrap())
            .unwrap();
        match self.writer.apply(self.index, &proposal).unwrap() {
            IndexedApplyOutcome::Applied(outcome) => outcome,
            other => panic!("fixture transition failed: {other:?}"),
        }
    }
    fn send(&mut self, entity: &str, id: &str, session_id: Option<SessionId>, scheduled: bool) {
        self.apply(
            entity,
            CommandKind::Send {
                message_id: id.into(),
                body: vec![0, 0xff, 0x80],
                time_to_live_millis: Some(100_000),
                session_id,
                scheduled_enqueue_at: scheduled.then(|| Timestamp::from_millis(50_000)),
                envelope: Some(MessageEnvelope::new(vec![0, 0xff, 7])),
            },
        );
    }
    fn receive(&mut self, entity: &str) -> domain::Delivery {
        match self.apply(
            entity,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: Some(10_000),
                session: None,
            },
        ) {
            CommandOutcome::Received(Some(delivery)) => delivery,
            other => panic!("receive fixture: {other:?}"),
        }
    }
}

fn every_record_roundtrip(durable: bool) {
    let mut fixture = Fixture::new(durable);
    let mut h = History::new(&fixture);
    h.apply(
        "orders",
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_duplicate_detection: true,
                duplicate_detection_history_millis: 100_000,
                ..QueueConfig::default()
            },
        },
    );
    h.send("orders", "deferred", None, false);
    let deferred = h.receive("orders");
    h.apply(
        "orders",
        CommandKind::Defer {
            sequence: deferred.sequence,
            lock_token: deferred.lock.unwrap().token,
            replacement_envelope: None,
        },
    );
    h.send("orders", "locked", None, false);
    h.receive("orders");
    h.send("orders", "ready", None, false);
    h.send("orders", "scheduled", None, true);
    h.apply(
        "sessions",
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_session: true,
                ..QueueConfig::default()
            },
        },
    );
    let session = SessionId::new("one").unwrap();
    h.send("sessions", "session-ready", Some(session.clone()), false);
    let accepted = match h.apply(
        "sessions",
        CommandKind::AcceptSession {
            session_id: Some(session),
            lock_duration_millis: Some(10_000),
        },
    ) {
        CommandOutcome::SessionAccepted(Some(accepted)) => accepted,
        other => panic!("accept fixture: {other:?}"),
    };
    h.apply(
        "sessions",
        CommandKind::SetSessionState {
            session: accepted.hold(),
            state: vec![0, 0xff, 0x80],
        },
    );
    h.apply(
        "events",
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    );
    h.apply(
        "events",
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("worker").unwrap(),
            config: SubscriptionConfig::default(),
        },
    );
    h.apply(
        "events/subscriptions/worker",
        CommandKind::CreateRule {
            name: RuleName::new("extra").unwrap(),
            filter: RuleFilter::False,
        },
    );
    h.journal.commit(h.index).unwrap();
    drop(h);
    let before = fixture.rows();
    let tags = before
        .iter()
        .map(|(key, _)| key[0])
        .collect::<BTreeSet<_>>();
    for tag in 0x00..=0x11 {
        assert!(
            tags.contains(&tag),
            "fixture must cover domain tag {tag:#x}"
        );
    }
    assert!(tags.contains(&0xf0) && tags.contains(&0xf1));
    let captured = fixture.check();
    fixture.reopen();
    assert_eq!(fixture.check(), captured);
    // A complete-state allocation defect is rejected, not just catalog corruption.
    fixture.raw(
        WriteBatch::default().put(
            keys::queue_counters(&ns(), &path("orders")),
            codec::encode(&QueueCounters {
                next_sequence: 1,
                next_lock_token: 1,
            })
            .unwrap(),
        ),
    );
    assert!(matches!(fixture.reject(), SnapshotCaptureError::State(_)));
}

macro_rules! paired {
    ($memory:ident, $fjall:ident, $body:ident) => {
        #[test]
        fn $memory() -> TestResult {
            $body(false);
            Ok(())
        }
        #[test]
        fn $fjall() -> TestResult {
            $body(true);
            Ok(())
        }
    };
}
paired!(
    memory_empty_and_adoption,
    fjall_empty_and_adoption,
    empty_and_adoption
);
paired!(
    memory_frontiers_identity_and_tail,
    fjall_frontiers_identity_and_tail,
    frontiers_identity_and_opaque_tail
);
paired!(
    memory_reserved_corruption,
    fjall_reserved_corruption,
    corrupt_reserved_rows
);
paired!(
    memory_committed_schemas,
    fjall_committed_schemas,
    committed_schemas
);
paired!(
    memory_domain_and_limits,
    fjall_domain_and_limits,
    domain_and_limit_refusals
);
paired!(
    memory_limits_precede_semantic_decoding,
    fjall_limits_precede_semantic_decoding,
    limits_precede_domain_and_committed_decoding
);
paired!(
    memory_one_image_and_failure,
    fjall_one_image_and_failure,
    one_image_and_failure
);
paired!(
    memory_every_record_roundtrip,
    fjall_every_record_roundtrip,
    every_record_roundtrip
);
