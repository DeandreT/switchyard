//! Explicit offline V2-to-V3 upgrade of real Fjall directories. Legacy markers
//! are stamped through raw native handles, standing in for an older build.

use std::path::Path;

use cluster::{
    CommittedReplay, Journal, JournalError, SNAPSHOT_ANCHOR_HASH_SCOPE, SNAPSHOT_LAYOUT_V2,
    SNAPSHOT_LAYOUT_VERSION, SnapshotAnchorLimits, SnapshotAnchorOwner, SnapshotCapture,
    SnapshotCaptureError, SnapshotCompactionError, SnapshotCompactionLimits,
    SnapshotCompactionOwner, SnapshotInstallError, SnapshotInstallLimits, SnapshotInstallOwner,
    SnapshotInstallPolicy, SnapshotLimits, SnapshotRecoveryClass, StoreUpgradeError,
    StoreUpgradeOutcome, select_recovery, upgrade_store,
};
use std::collections::BTreeMap;

use domain::{
    BoundCommand, Command, CommandKind, CorrelationFilter, CorrelationValue, DurableProposal,
    EntityPath, FilterProperties, IndexedWriter, MessageEnvelope, MessageRecord,
    MessageRecordFormat, MessageState, NamespaceName, OverlayProperty, QueueConfig, ReceiveMode,
    RuleDefinition, RuleFilter, RuleName, SequenceNumber, StateMachine, SubscriptionName,
    Timestamp, codec, keys,
    legacy_content::{ContentError, LegacyContentError, LegacyContentField},
};
use fjall::{Database, KeyspaceCreateOptions, PersistMode, Readable};
use sha2::{Digest, Sha256};
use storage::{
    FjallStore, ImageInstallLimits, Key, MemoryStore, STORE_FORMAT_V2, STORE_FORMAT_V3, StateStore,
    StorageError, StoreFormatUpgradeError, Value, WriteBatch,
};
use tempfile::TempDir;

const F0: &[u8] = b"\xF0switchyard/journal\0";
const F1: &[u8] = b"\xF1switchyard/replay\0";

fn ns() -> NamespaceName {
    NamespaceName::new("tenant").unwrap()
}

fn orders() -> EntityPath {
    EntityPath::new("orders").unwrap()
}

fn command(at: u64, kind: CommandKind) -> Command {
    Command::new(ns(), orders(), Timestamp::from_millis(at), kind)
}

fn send(id: &str, envelope: Option<Vec<u8>>, scheduled: Option<u64>) -> CommandKind {
    CommandKind::Send {
        message_id: id.into(),
        // Raw bodies are opaque and never parsed as AMQP.
        body: vec![0xff, 0x00, 0x7f],
        time_to_live_millis: None,
        session_id: None,
        scheduled_enqueue_at: scheduled.map(Timestamp::from_millis),
        envelope: envelope.map(MessageEnvelope::new),
    }
}

/// A one-byte `data` section.
fn valid_envelope() -> Vec<u8> {
    vec![0x00, 0x53, 0x75, 0xa0, 1, 7]
}

fn marker(directory: &Path) -> Vec<u8> {
    let database = Database::builder(directory).open().unwrap();
    let meta = database
        .keyspace("meta", KeyspaceCreateOptions::default)
        .unwrap();
    database
        .snapshot()
        .get(&meta, b"format_version")
        .unwrap()
        .unwrap()
        .to_vec()
}

/// Rewrites only the marker, as an older build's store would hold it.
fn stamp(directory: &Path, version: u32) {
    let database = Database::builder(directory).open().unwrap();
    let meta = database
        .keyspace("meta", KeyspaceCreateOptions::default)
        .unwrap();
    let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
    batch.insert(&meta, b"format_version", version.to_be_bytes());
    batch.commit().unwrap();
}

fn records(directory: &Path) -> Vec<(Key, Value)> {
    FjallStore::open(directory)
        .unwrap()
        .snapshot()
        .unwrap()
        .entries()
        .to_vec()
}

/// Writes through a current-format handle, then fences the marker back to 2.
fn legacy(directory: &Path, write: impl FnOnce(&FjallStore)) -> Vec<(Key, Value)> {
    let store = FjallStore::open(directory).unwrap();
    write(&store);
    drop(store);
    let rows = records(directory);
    stamp(directory, STORE_FORMAT_V2);
    rows
}

fn assert_refused_without_mutation(directory: &Path, rows: &[(Key, Value)]) {
    assert_eq!(marker(directory), STORE_FORMAT_V2.to_be_bytes());
    assert_eq!(
        FjallStore::open(directory).err(),
        Some(StorageError::UnsupportedStoreFormat {
            found: STORE_FORMAT_V2,
            expected: STORE_FORMAT_V3,
        })
    );
    stamp(directory, STORE_FORMAT_V3);
    assert_eq!(records(directory), rows);
    stamp(directory, STORE_FORMAT_V2);
}

fn assert_upgraded(directory: &Path, rows: &[(Key, Value)]) {
    assert_eq!(upgrade_store(directory), Ok(StoreUpgradeOutcome::Upgraded));
    assert_eq!(marker(directory), STORE_FORMAT_V3.to_be_bytes());
    assert_eq!(records(directory), rows, "exact all-record equality");
    for _ in 0..2 {
        assert_eq!(
            upgrade_store(directory),
            Ok(StoreUpgradeOutcome::AlreadyCurrent)
        );
        assert_eq!(records(directory), rows);
    }
}

#[test]
fn v3_id_projection_rows_are_preflighted_without_rewriting_bytes() {
    let directory = TempDir::new().unwrap();
    // Synthetic marker downgrade exercises the shared upgrade preflight; it
    // does not assert that an old writer could have produced a V3 record.
    let rows = legacy(directory.path(), |store| {
        let machine = StateMachine::new(store.clone());
        machine
            .apply(&command(
                0,
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            ))
            .unwrap();
        let mut bytes = vec![0, 0x53, 0x73, 0xc0, 18, 1, 0x98];
        bytes.extend(0_u8..16);
        for content in [
            domain::MessageContent::Amqp(bytes.clone()),
            domain::MessageContent::AmqpTextIds(bytes),
        ] {
            machine
                .apply(&command(1, CommandKind::SendContent { content }))
                .unwrap();
        }
    });
    let versions: Vec<_> = rows
        .iter()
        .filter(|(key, _)| key[0] == 3)
        .map(|(_, value)| value[0])
        .collect();
    assert_eq!(versions, vec![2, 3]);
    assert_upgraded(directory.path(), &rows);
}

#[test]
fn empty_and_populated_unindexed_stores_upgrade_without_adoption() {
    let directory = TempDir::new().unwrap();
    let rows = legacy(directory.path(), |_| {});
    assert!(rows.is_empty());
    assert_upgraded(directory.path(), &rows);

    let directory = TempDir::new().unwrap();
    let rows = legacy(directory.path(), |store| {
        let machine = StateMachine::new(store.clone());
        machine
            .apply(&command(
                1,
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            ))
            .unwrap();
        machine
            .apply(&command(2, send("ready", Some(valid_envelope()), None)))
            .unwrap();
        machine
            .apply(&command(3, send("plain", None, None)))
            .unwrap();
        machine
            .apply(&command(
                4,
                send("scheduled", Some(valid_envelope()), Some(9_000)),
            ))
            .unwrap();
        machine
            .apply(&command(
                5,
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: Some(60_000),
                    session: None,
                },
            ))
            .unwrap();
    });
    assert!(
        rows.iter().any(|(key, _)| key[0] == 0x05),
        "a locked message"
    );
    assert!(!rows.iter().any(|(key, _)| key[0] >= 0xF0), "unindexed");
    // Complete snapshot eligibility is not required: capture refuses this image.
    assert!(SnapshotCapture::logical(&memory(&rows), caps().snapshot).is_err());
    assert_upgraded(directory.path(), &rows);
}

#[test]
fn unsafe_legacy_content_refuses_before_any_marker_write() {
    for (envelope, error) in [
        (vec![0xff, b'S'], ContentError::InvalidConstructor),
        (vec![0x00, 0x53, 0x75, 0xa0, 2, 7], ContentError::Truncated),
    ] {
        let directory = TempDir::new().unwrap();
        let rows = legacy(directory.path(), |store| {
            let machine = StateMachine::new(store.clone());
            machine
                .apply(&command(
                    1,
                    CommandKind::CreateQueue {
                        config: QueueConfig::default(),
                    },
                ))
                .unwrap();
            machine
                .apply(&command(2, send("ok", Some(valid_envelope()), None)))
                .unwrap();
            machine
                .apply(&command(3, send("unsafe", Some(envelope.clone()), None)))
                .unwrap();
        });
        let row = rows.iter().rposition(|(key, _)| key[0] == 0x03).unwrap();
        for _ in 0..2 {
            assert_eq!(
                upgrade_store(directory.path()),
                Err(StoreUpgradeError::Content(LegacyContentError::Content {
                    row,
                    field: LegacyContentField::Envelope,
                    error,
                }))
            );
            assert_refused_without_mutation(directory.path(), &rows);
        }
    }
}

#[test]
fn missing_unsupported_and_foreign_targets_refuse() {
    let parent = TempDir::new().unwrap();
    let missing = parent.path().join("absent");
    assert!(matches!(
        upgrade_store(&missing),
        Err(StoreUpgradeError::Storage(
            StoreFormatUpgradeError::Storage(StorageError::Backend { .. })
        ))
    ));
    assert!(!missing.exists());

    let directory = TempDir::new().unwrap();
    legacy(directory.path(), |_| {});
    stamp(directory.path(), 1);
    assert_eq!(
        upgrade_store(directory.path()),
        Err(StoreUpgradeError::Storage(
            StoreFormatUpgradeError::UnsupportedSource {
                found: 1,
                target: STORE_FORMAT_V3,
            }
        ))
    );
    assert_eq!(marker(directory.path()), 1_u32.to_be_bytes());

    let directory = TempDir::new().unwrap();
    let rows = legacy(directory.path(), |store| {
        store
            .apply(WriteBatch::default().put(b"\xF0foreign".to_vec(), vec![]))
            .unwrap();
    });
    assert!(matches!(
        upgrade_store(directory.path()),
        Err(StoreUpgradeError::Journal(JournalError::Corrupt { .. }))
    ));
    assert_refused_without_mutation(directory.path(), &rows);
}

fn caps() -> SnapshotAnchorLimits {
    SnapshotAnchorLimits {
        snapshot: SnapshotLimits {
            max_encoded_bytes: 8 * 1024 * 1024,
            max_records: 10_000,
            max_key_bytes: 4096,
            max_value_bytes: 2 * 1024 * 1024,
        },
        max_mutation_bytes: 2 * 1024 * 1024,
    }
}

fn install_caps() -> SnapshotInstallLimits {
    SnapshotInstallLimits {
        snapshot: caps().snapshot,
        replacement: ImageInstallLimits {
            max_records: 10_000,
            max_key_bytes: 4096,
            max_value_bytes: 2 * 1024 * 1024,
            max_mutations: 20_000,
            max_mutation_bytes: 16 * 1024 * 1024,
        },
    }
}

fn memory(rows: &[(Key, Value)]) -> MemoryStore {
    let store = MemoryStore::default();
    let mut batch = WriteBatch::default();
    for (key, value) in rows {
        batch.push_put(key.clone(), value.clone());
    }
    store.apply(batch).unwrap();
    store
}

/// Applied P=2 retaining `envelope`, committed C=3 with historical intent that
/// the domain would refuse (and whose envelope is unsafe), plus an opaque tail.
fn seed<S: StateStore>(store: &S, envelope: Vec<u8>) {
    seed_with(store, envelope, None);
}

/// As [`seed`], but committed entry 3 is an applicable send retaining
/// `committed` when it is supplied, rather than refused historical intent.
fn seed_with<S: StateStore>(store: &S, envelope: Vec<u8>, committed: Option<Vec<u8>>) {
    let mut journal = Journal::open(store.clone()).unwrap();
    let mut writer = IndexedWriter::open(store.clone()).unwrap();
    let first = DurableProposal::unbound(command(
        10,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    ));
    journal.append(1, &first.encode().unwrap()).unwrap();
    writer.apply(1, &first).unwrap();
    let binding = StateMachine::new(store.clone())
        .bind_entity(&ns(), &orders())
        .unwrap();
    let second = DurableProposal::bound(BoundCommand::new(
        binding.clone(),
        command(20, send("original", Some(envelope), None)),
    ))
    .unwrap();
    journal.append(2, &second.encode().unwrap()).unwrap();
    writer.apply(2, &second).unwrap();
    let third = match committed {
        Some(envelope) => DurableProposal::bound(BoundCommand::new(
            binding,
            command(30, send("later", Some(envelope), None)),
        ))
        .unwrap(),
        None => DurableProposal::unbound(Command::new(
            ns(),
            EntityPath::new("missing").unwrap(),
            Timestamp::from_millis(30),
            send("refused", Some(vec![0xff, b'S']), None),
        )),
    };
    journal.append(3, &third.encode().unwrap()).unwrap();
    journal.commit(3).unwrap();
    journal.append(4, &[0xff, 0, 0x80]).unwrap();
}

fn resign_anchor(bytes: &mut [u8]) {
    let end = bytes.len() - 32;
    let mut hash = Sha256::new();
    hash.update(SNAPSHOT_ANCHOR_HASH_SCOPE);
    hash.update(&bytes[..end]);
    let hash: [u8; 32] = hash.finalize().into();
    bytes[end..].copy_from_slice(&hash);
}

fn anchor_key() -> Key {
    [F0, &[3]].concat()
}

#[test]
fn anchored_f0_f1_store_upgrades_with_exact_bytes_and_historical_anchor_layout() {
    let source = MemoryStore::default();
    seed(&source, valid_envelope());
    let capture = SnapshotCapture::logical(&source, caps().snapshot).unwrap();
    let directory = TempDir::new().unwrap();
    let rows = legacy(directory.path(), |store| {
        let mut install = SnapshotInstallOwner::new(store.clone());
        install
            .install(
                &capture,
                install_caps(),
                SnapshotInstallPolicy::AllowCrossBackend,
            )
            .unwrap();
        drop(install);
        let mut anchor = SnapshotAnchorOwner::new(store.clone());
        anchor.attach(&capture, caps()).unwrap();
        drop(anchor);
        // An anchor attached before the upgrade recorded layout 2.
        let mut bytes = store.get(&anchor_key()).unwrap().unwrap();
        assert_eq!(bytes[20..24], SNAPSHOT_LAYOUT_VERSION.to_be_bytes());
        bytes[20..24].copy_from_slice(&SNAPSHOT_LAYOUT_V2.to_be_bytes());
        resign_anchor(&mut bytes);
        store
            .apply(WriteBatch::default().put(anchor_key(), bytes))
            .unwrap();
    });
    assert!(rows.iter().any(|(key, _)| key.starts_with(F1)));
    assert_upgraded(directory.path(), &rows);

    let store = FjallStore::open(directory.path()).unwrap();
    let journal = Journal::open(store.clone()).unwrap();
    let anchor = journal.snapshot_anchor().unwrap().unwrap();
    assert_eq!(
        anchor.source_manifest().layout_version(),
        SNAPSHOT_LAYOUT_V2
    );
    drop(journal);
    let replay = CommittedReplay::open(store.clone()).unwrap();
    assert_eq!(
        (
            replay.applied_index().unwrap(),
            replay.committed_index().unwrap(),
            replay.last_appended_index().unwrap()
        ),
        (2, 3, 4)
    );
    drop(replay);
    // Recapture uses the current layout; the anchor keeps its recorded source.
    let recapture = SnapshotCapture::fjall(&store, caps().snapshot).unwrap();
    assert_eq!(
        recapture.manifest().layout_version(),
        SNAPSHOT_LAYOUT_VERSION
    );
    assert_eq!(recapture.image().snapshot().entries(), rows.as_slice());

    // Compaction rewrites only B and the checksum: the recorded layout stays 2.
    let mut owner = SnapshotCompactionOwner::new(store.clone());
    let receipt = owner.compact(compaction_caps()).unwrap();
    assert_eq!(receipt.removed_through_after, 2);
    drop(owner);
    let anchor = store.get(&anchor_key()).unwrap().unwrap();
    assert_eq!(anchor[20..24], SNAPSHOT_LAYOUT_V2.to_be_bytes());
    let selection = select_recovery(&store, caps().snapshot).unwrap();
    assert_eq!(selection.class, SnapshotRecoveryClass::Compacted);
    assert_eq!(selection.removed_through, 2);
    let mut replay = CommittedReplay::open(store.clone()).unwrap();
    assert_eq!(replay.replay_batch(64).unwrap().processed, 1);
    assert_eq!(replay.applied_index().unwrap(), 3);
}

fn compaction_caps() -> SnapshotCompactionLimits {
    SnapshotCompactionLimits {
        snapshot: caps().snapshot,
        max_deleted_entries: 10_000,
        max_mutation_bytes: 2 * 1024 * 1024,
    }
}

/// Ordinary writes do not enforce the profile yet: replaying a committed send
/// after the upgrade retains out-of-profile content in a valid anchored store.
/// Capture, recovery selection and compaction all refuse it without writes.
#[test]
fn out_of_profile_content_blocks_compaction_and_recovery_selection() {
    let source = MemoryStore::default();
    seed_with(&source, valid_envelope(), Some(vec![0xff, b'S']));
    let capture = SnapshotCapture::logical(&source, caps().snapshot).unwrap();
    let directory = TempDir::new().unwrap();
    legacy(directory.path(), |store| {
        let mut install = SnapshotInstallOwner::new(store.clone());
        install
            .install(
                &capture,
                install_caps(),
                SnapshotInstallPolicy::AllowCrossBackend,
            )
            .unwrap();
        drop(install);
        SnapshotAnchorOwner::new(store.clone())
            .attach(&capture, caps())
            .unwrap();
    });
    assert_eq!(
        upgrade_store(directory.path()),
        Ok(StoreUpgradeOutcome::Upgraded)
    );
    let store = FjallStore::open(directory.path()).unwrap();
    let mut replay = CommittedReplay::open(store.clone()).unwrap();
    assert_eq!(replay.replay_batch(64).unwrap().processed, 1);
    drop(replay);
    let before = store.snapshot().unwrap();

    let content = |error: &SnapshotCaptureError| {
        matches!(
            error,
            SnapshotCaptureError::Content(LegacyContentError::Content {
                field: LegacyContentField::Envelope,
                error: ContentError::InvalidConstructor,
                ..
            })
        )
    };
    assert!(content(
        &SnapshotCapture::fjall(&store, caps().snapshot).unwrap_err()
    ));
    assert!(content(
        &select_recovery(&store, caps().snapshot).unwrap_err()
    ));
    let mut owner = SnapshotCompactionOwner::new(store.clone());
    for _ in 0..2 {
        match owner.compact(compaction_caps()) {
            Err(SnapshotCompactionError::Target(error)) => assert!(content(&error)),
            other => panic!("compaction must refuse, got {other:?}"),
        }
        assert!(!owner.is_retired());
    }
    drop(owner);
    assert_eq!(store.snapshot().unwrap(), before);
    drop(store);
    assert_eq!(records(directory.path()), before.entries());
}

#[test]
fn corrupt_present_owners_refuse_without_mutation() {
    let source = MemoryStore::default();
    seed(&source, valid_envelope());
    let capture = SnapshotCapture::logical(&source, caps().snapshot).unwrap();
    for case in 0..3 {
        let directory = TempDir::new().unwrap();
        let rows = legacy(directory.path(), |store| {
            let mut install = SnapshotInstallOwner::new(store.clone());
            install
                .install(
                    &capture,
                    install_caps(),
                    SnapshotInstallPolicy::AllowCrossBackend,
                )
                .unwrap();
            drop(install);
            let batch = match case {
                // Indexed owner only: its rows must still be canonical.
                0 => {
                    let mut batch = WriteBatch::default();
                    for (key, _) in store.snapshot().unwrap().entries() {
                        if key.starts_with(F0) {
                            batch.push_delete(key.clone());
                        }
                    }
                    batch.put([F1, &[0]].concat(), vec![0xff])
                }
                // Journal only: its frontier must still be checksummed.
                1 => {
                    let mut batch = WriteBatch::default();
                    for (key, _) in store.snapshot().unwrap().entries() {
                        if key.starts_with(F1) {
                            batch.push_delete(key.clone());
                        }
                    }
                    batch.put([F0, &[1]].concat(), vec![0xff])
                }
                // Both: a committed schema must still decode.
                _ => {
                    WriteBatch::default().put([F0, &[2], &2_u64.to_be_bytes()].concat(), vec![0xff])
                }
            };
            store.apply(batch).unwrap();
        });
        let result = upgrade_store(directory.path());
        match case {
            0 => assert!(
                matches!(result, Err(StoreUpgradeError::Indexed(_))),
                "{result:?}"
            ),
            1 => assert!(
                matches!(result, Err(StoreUpgradeError::Journal(_))),
                "{result:?}"
            ),
            _ => assert!(
                matches!(result, Err(StoreUpgradeError::Replay(_))),
                "{result:?}"
            ),
        }
        assert_refused_without_mutation(directory.path(), &rows);
    }
}

/// Raw legacy rows: a rule and a message whose only unsafe content is one
/// standalone property value. The upgrade needs no complete-state eligibility.
#[test]
fn unsafe_rule_and_filter_values_refuse_without_marker_change() {
    let truncated = CorrelationValue::new(vec![0xa1, 9]).unwrap();
    let properties = BTreeMap::from([
        (
            String::from("a"),
            CorrelationValue::new(vec![0x71, 0, 0, 0, 5]).unwrap(),
        ),
        (String::from("b"), truncated),
    ]);
    let subscription = orders()
        .subscription(&SubscriptionName::new("audit").unwrap())
        .unwrap();
    let rule_name = RuleName::new("match").unwrap();
    let rule = (
        keys::subscription_rule(&ns(), &subscription, &rule_name),
        codec::encode(&RuleDefinition {
            action: None,
            name: rule_name,
            filter: RuleFilter::Correlation(CorrelationFilter {
                application_properties: properties.clone(),
                ..CorrelationFilter::default()
            }),
            created_at: Timestamp::from_millis(1),
        })
        .unwrap(),
    );
    let record = MessageRecord {
        sequence: SequenceNumber::new(1),
        message_id: String::from("m"),
        body: vec![0xff],
        enqueued_at: Timestamp::from_millis(1),
        scheduled_enqueue_at: None,
        expires_at: None,
        delivery_count: 0,
        state: MessageState::Ready,
        session_id: None,
        dead_letter: None,
        envelope: Some(
            MessageEnvelope::new(valid_envelope()).with_filter_properties(FilterProperties {
                application_properties: properties,
                ..FilterProperties::default()
            }),
        ),
        format: MessageRecordFormat::V1,
    };
    let message = (
        keys::message(&ns(), &orders(), record.sequence),
        codec::message::encode(&record).unwrap(),
    );
    for (row, field) in [
        (rule, LegacyContentField::RuleProperty { ordinal: 1 }),
        (message, LegacyContentField::FilterProperty { ordinal: 1 }),
    ] {
        let directory = TempDir::new().unwrap();
        let rows = legacy(directory.path(), |store| {
            store
                .apply(WriteBatch::default().put(row.0.clone(), row.1.clone()))
                .unwrap();
        });
        for _ in 0..2 {
            assert_eq!(
                upgrade_store(directory.path()),
                Err(StoreUpgradeError::Content(LegacyContentError::Content {
                    row: 0,
                    field,
                    error: ContentError::Truncated,
                }))
            );
            assert_refused_without_mutation(directory.path(), &rows);
        }
    }
}

/// Ordinary writes do not enforce the profile yet, so a format 3 target can
/// hold out-of-profile content. Install's complete old-target validation
/// refuses it before any write, under either provenance policy.
fn refuses_out_of_profile_target<S: storage::ImageInstallBackend>(target: S, native: bool) {
    seed(&target, vec![0xff, b'S']);
    assert!(matches!(
        SnapshotCapture::logical(&target, caps().snapshot),
        Err(SnapshotCaptureError::Content(_))
    ));
    let source = MemoryStore::default();
    seed(&source, valid_envelope());
    let capture = SnapshotCapture::logical(&source, caps().snapshot).unwrap();
    let before = target.snapshot().unwrap();
    let mut owner = SnapshotInstallOwner::new(target);
    for policy in [
        SnapshotInstallPolicy::MatchingProvenance,
        SnapshotInstallPolicy::AllowCrossBackend,
    ] {
        let result = owner.install(&capture, install_caps(), policy);
        // The class check precedes old-target validation for a native target.
        if native && policy == SnapshotInstallPolicy::MatchingProvenance {
            assert_eq!(result, Err(SnapshotInstallError::ProvenanceMismatch));
        } else {
            assert!(
                matches!(
                    result,
                    Err(SnapshotInstallError::Target(SnapshotCaptureError::Content(
                        LegacyContentError::Content {
                            field: LegacyContentField::Envelope,
                            error: ContentError::InvalidConstructor,
                            ..
                        }
                    )))
                ),
                "{result:?}"
            );
        }
        assert!(!owner.is_retired());
        assert_eq!(owner.snapshot().unwrap().snapshot(), &before);
    }
    assert_eq!(owner.into_store().unwrap().snapshot().unwrap(), before);
}

#[test]
fn install_refuses_an_out_of_profile_target_without_writes() {
    refuses_out_of_profile_target(MemoryStore::default(), false);
    let directory = TempDir::new().unwrap();
    refuses_out_of_profile_target(FjallStore::open(directory.path()).unwrap(), true);
    let reopened = FjallStore::open(directory.path()).unwrap();
    assert!(SnapshotCapture::fjall(&reopened, caps().snapshot).is_err());
}

fn version_two_record(sequence: u64, envelope: Option<MessageEnvelope>) -> MessageRecord {
    MessageRecord {
        sequence: SequenceNumber::new(sequence),
        message_id: format!("m-{sequence}"),
        body: if envelope.is_some() {
            Vec::new()
        } else {
            vec![0xff, 0x00]
        },
        enqueued_at: Timestamp::from_millis(1),
        scheduled_enqueue_at: None,
        expires_at: None,
        delivery_count: 0,
        state: MessageState::Ready,
        session_id: None,
        dead_letter: None,
        envelope,
        format: MessageRecordFormat::V2,
    }
}

fn version_two_amqp(original: Vec<u8>, overlay: Vec<u8>, projection: Vec<u8>) -> MessageEnvelope {
    MessageEnvelope::new(original)
        .with_filter_properties(FilterProperties {
            application_properties: BTreeMap::from([(
                String::from("p"),
                CorrelationValue::new(projection).unwrap(),
            )]),
            ..FilterProperties::default()
        })
        .with_overlay(vec![
            OverlayProperty {
                name: String::from("null"),
                value: vec![0x40],
            },
            OverlayProperty {
                name: String::from("set"),
                value: overlay,
            },
        ])
}

fn write_messages(store: &FjallStore, records: &[MessageRecord]) {
    let mut batch = WriteBatch::default();
    for record in records {
        batch.push_put(
            keys::message(&ns(), &orders(), record.sequence),
            codec::message::encode(record).unwrap(),
        );
    }
    store.apply(batch).unwrap();
}

#[test]
fn version_two_message_rows_upgrade_or_refuse_by_their_content() {
    let int = vec![0x71, 0, 0, 0, 5];
    let safe = [
        version_two_record(1, None),
        version_two_record(
            2,
            Some(version_two_amqp(valid_envelope(), int.clone(), int.clone())),
        ),
    ];
    let directory = TempDir::new().unwrap();
    let rows = legacy(directory.path(), |store| write_messages(store, &safe));
    assert!(
        rows.iter().all(|(_, value)| value[0] == 2),
        "frozen V2 rows"
    );
    assert_upgraded(directory.path(), &rows);

    for (unsafe_record, field, error) in [
        (
            version_two_amqp(vec![0xff, b'S'], int.clone(), int.clone()),
            LegacyContentField::Envelope,
            ContentError::InvalidConstructor,
        ),
        (
            version_two_amqp(valid_envelope(), vec![0xa1, 9], int.clone()),
            LegacyContentField::OverlayProperty { ordinal: 1 },
            ContentError::Truncated,
        ),
        (
            version_two_amqp(valid_envelope(), int.clone(), vec![0xa1, 9]),
            LegacyContentField::FilterProperty { ordinal: 0 },
            ContentError::Truncated,
        ),
    ] {
        let mut records = safe.to_vec();
        records.push(version_two_record(3, Some(unsafe_record)));
        let directory = TempDir::new().unwrap();
        let rows = legacy(directory.path(), |store| write_messages(store, &records));
        for _ in 0..2 {
            assert_eq!(
                upgrade_store(directory.path()),
                Err(StoreUpgradeError::Content(LegacyContentError::Content {
                    row: 2,
                    field,
                    error,
                }))
            );
            assert_refused_without_mutation(directory.path(), &rows);
        }
    }
}
