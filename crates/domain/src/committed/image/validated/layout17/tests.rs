use sha2::{Digest, Sha256};

use crate::{
    CommittedEntryId, CommittedEntryMark, EntityIncarnation, EntityIncarnationKind, EntityPath,
    MessageRecord, MessageState, NamespaceName, QueueConfig, QueueCounters, SequenceNumber,
    SessionId, Timestamp, codec, committed::encode_checkpoint, keys as store_keys,
};

use super::super::super::{CommittedImageError, EncodedCommittedImage, Limits, encode_entries};
use super::super::{ValidatedCreateSendImage, records};
use super::*;

type OwnedRows = Vec<(Vec<u8>, Vec<u8>)>;

fn stream() -> CommittedStreamId {
    CommittedStreamId::new([31; 16]).unwrap()
}
fn namespace() -> NamespaceName {
    NamespaceName::new("Tenant").unwrap()
}
fn entity() -> EntityPath {
    EntityPath::new("/Orders/$Management/literal").unwrap()
}

fn checkpoint() -> CommittedCheckpoint {
    CommittedCheckpoint {
        stream: stream(),
        last: Some(CommittedEntryMark {
            id: CommittedEntryId {
                term: 1,
                node_id: 1,
                index: 1,
            },
            fingerprint: [2; 32],
        }),
        previous: Some(CommittedEntryMark {
            id: CommittedEntryId {
                term: 1,
                node_id: 1,
                index: 0,
            },
            fingerprint: [1; 32],
        }),
        highest_timestamp: Timestamp::from_millis(100),
        membership: None,
    }
}

fn message() -> MessageRecord {
    MessageRecord {
        sequence: SequenceNumber::new(1),
        message_id: "private-id\0literal".into(),
        body: b"private-body".to_vec(),
        enqueued_at: Timestamp::from_millis(100),
        expires_at: None,
        delivery_count: 0,
        state: MessageState::Ready,
        session_id: None,
        dead_letter: None,
        scheduled_enqueue_time: None,
        envelope: None,
    }
}

fn mode_key(namespace: &str, entity: &str) -> Vec<u8> {
    let mut key = vec![0x16];
    key.extend_from_slice(namespace.as_bytes());
    key.push(0);
    key.extend_from_slice(entity.as_bytes());
    key.push(0);
    key
}

fn mode_row() -> (Vec<u8>, Vec<u8>) {
    (
        mode_key(namespace().as_str(), entity().as_str()),
        vec![11, 1, 1, 0],
    )
}

// Fixed test-owned business rows, not current owner/export/bootstrap authority.
fn rows(include_mode: bool) -> OwnedRows {
    let ns = namespace();
    let queue = entity();
    let config = QueueConfig::default();
    let mut rows = vec![
        (store_keys::clock(), codec::encode(&100_u64).unwrap()),
        (
            store_keys::queue_config(&ns, &queue),
            codec::encode(&config).unwrap(),
        ),
        (
            store_keys::queue_config(&ns, &queue.dead_letter_queue().unwrap()),
            codec::encode(&config.dead_letter_shadow()).unwrap(),
        ),
        (
            store_keys::queue_counters(&ns, &queue),
            codec::encode(&QueueCounters {
                next_sequence: 2,
                next_lock_token: 1,
            })
            .unwrap(),
        ),
        (
            store_keys::message(&ns, &queue, SequenceNumber::new(1)),
            codec::encode(&message()).unwrap(),
        ),
        (
            store_keys::ready(&ns, &queue, SequenceNumber::new(1)),
            Vec::new(),
        ),
        (
            store_keys::entity_incarnation(&ns, &queue),
            codec::encode(&EntityIncarnation::new(1, EntityIncarnationKind::Queue, false).unwrap())
                .unwrap(),
        ),
        (
            store_keys::committed_checkpoint(),
            encode_checkpoint(&checkpoint()).unwrap(),
        ),
    ];
    if include_mode {
        rows.push(mode_row());
    }
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows
}

fn encode(role: CommittedImageRole, rows: &OwnedRows) -> EncodedCommittedImage {
    encode_entries(role, stream(), rows, Limits::CURRENT).unwrap()
}

fn validation_error(rows: &OwnedRows) -> Option<Error> {
    let image = encode(CommittedImageRole::CreateSendLayout17V1, rows);
    ValidatedCreateSendLayout17Image::validate(
        DecodedCommittedImage::decode(image.as_bytes()).unwrap(),
    )
    .err()
}

fn replace(rows: &mut OwnedRows, key: &[u8], value: Vec<u8>) {
    rows.iter_mut().find(|(found, _)| found == key).unwrap().1 = value;
}

fn add(rows: &mut OwnedRows, row: (Vec<u8>, Vec<u8>)) {
    assert!(!rows.iter().any(|(key, _)| key == &row.0));
    rows.push(row);
    rows.sort_by(|left, right| left.0.cmp(&right.0));
}

// Independent frozen framing, including arbitrary unknown role tags.
fn raw_container(role: u16, rows: &OwnedRows) -> Vec<u8> {
    let mut bytes = b"SWYI".to_vec();
    bytes.extend_from_slice(&1_u16.to_be_bytes());
    bytes.extend_from_slice(&role.to_be_bytes());
    bytes.extend_from_slice(stream().as_bytes());
    bytes.extend_from_slice(&u32::try_from(rows.len()).unwrap().to_be_bytes());
    for (key, value) in rows {
        bytes.extend_from_slice(&u32::try_from(key.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(&u32::try_from(value.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
    }
    let checksum: [u8; 32] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&checksum);
    bytes
}

#[test]
fn role1_and_role2_framing_are_distinct_with_role1_bytes_unchanged() {
    let rows = rows(false);
    let old = encode(CommittedImageRole::CreateSendV1, &rows);
    let new = encode(CommittedImageRole::CreateSendLayout17V1, &rows);
    assert_eq!(old.as_bytes(), raw_container(1, &rows));
    assert_eq!(new.as_bytes(), raw_container(2, &rows));
    assert_eq!(&old.as_bytes()[..8], b"SWYI\0\x01\0\x01");
    assert_eq!(&new.as_bytes()[..8], b"SWYI\0\x01\0\x02");
    assert_eq!(
        &old.as_bytes()[8..old.len() - 32],
        &new.as_bytes()[8..new.len() - 32]
    );
    assert_ne!(
        &old.as_bytes()[old.len() - 32..],
        &new.as_bytes()[new.len() - 32..]
    );
    assert_eq!(
        DecodedCommittedImage::decode(old.as_bytes())
            .unwrap()
            .role(),
        CommittedImageRole::CreateSendV1
    );
    assert_eq!(
        DecodedCommittedImage::decode(new.as_bytes())
            .unwrap()
            .role(),
        CommittedImageRole::CreateSendLayout17V1
    );
    for unknown in [0, 3, u16::MAX] {
        assert_eq!(
            DecodedCommittedImage::decode(&raw_container(unknown, &rows)).unwrap_err(),
            CommittedImageError::UnsupportedFormat
        );
    }
}

#[test]
fn layout17_proof_counts_and_rows_borrow_original_bytes_without_debug_disclosure() {
    let rows = rows(true);
    let encoded = encode(CommittedImageRole::CreateSendLayout17V1, &rows);
    let proof = ValidatedCreateSendLayout17Image::validate(
        DecodedCommittedImage::decode(encoded.as_bytes()).unwrap(),
    )
    .unwrap();
    assert_eq!(proof.role(), CommittedImageRole::CreateSendLayout17V1);
    assert_eq!(proof.stream(), stream());
    assert_eq!(proof.checkpoint(), &checkpoint());
    assert_eq!(proof.queue_count(), 1);
    assert_eq!(proof.message_count(), 1);
    assert_eq!(proof.row_count(), 9);
    assert_eq!(
        proof
            .rows()
            .map(|row| (row.key(), row.value()))
            .collect::<Vec<_>>(),
        rows.iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect::<Vec<_>>()
    );
    let message = proof
        .rows()
        .find(|row| row.key().first() == Some(&3))
        .unwrap();
    let parsed = records::message(message.value()).unwrap();
    for borrowed in [parsed.body, parsed.message_id.as_bytes()] {
        let start = borrowed.as_ptr() as usize;
        let backing = encoded.as_bytes().as_ptr() as usize;
        assert!(start >= backing && start + borrowed.len() <= backing + encoded.len());
    }
    let debug = format!("{proof:?}");
    assert!(!debug.contains("private"));
    assert!(!debug.contains(namespace().as_str()));
    assert!(!debug.contains(entity().as_str()));
}

#[test]
fn mode_bijection_refuses_missing_orphan_shadow_and_substituted_namespace_rows() {
    assert_eq!(
        validation_error(&rows(false)),
        Some(Error::InconsistentMetadata)
    );
    let mut orphan = rows(true);
    add(
        &mut orphan,
        (mode_key(namespace().as_str(), "orphan"), vec![11, 1, 1, 0]),
    );
    assert_eq!(validation_error(&orphan), Some(Error::InconsistentMetadata));
    let mut shadow = rows(true);
    add(
        &mut shadow,
        (
            mode_key(
                namespace().as_str(),
                entity().dead_letter_queue().unwrap().as_str(),
            ),
            vec![11, 1, 1, 0],
        ),
    );
    assert_eq!(validation_error(&shadow), Some(Error::InconsistentMetadata));
    let mut substituted = rows(false);
    add(
        &mut substituted,
        (mode_key("tenant", entity().as_str()), vec![11, 1, 1, 0]),
    );
    assert_eq!(
        validation_error(&substituted),
        Some(Error::InconsistentMetadata)
    );
}

#[test]
fn mode_keys_require_exact_utf8_scope_segments_and_no_tail() {
    let mut bad_keys = vec![
        vec![0x16],
        b"\x16\0orders\0".to_vec(),
        b"\x16tenant\0\0".to_vec(),
        b"\x16ten\nant\0orders\0".to_vec(),
        b"\x16tenant\0\xff\0".to_vec(),
        b"\x16tenant\0orders".to_vec(),
    ];
    let mut extra = mode_row().0;
    extra.push(0);
    bad_keys.push(extra);
    bad_keys.push(mode_key(
        &"n".repeat(crate::MAX_NAMESPACE_NAME_BYTES + 1),
        entity().as_str(),
    ));
    for key in bad_keys {
        let mut fixture = rows(false);
        add(&mut fixture, (key, vec![11, 1, 1, 0]));
        assert_eq!(validation_error(&fixture), Some(Error::InvalidKey));
    }
}

#[test]
fn independent_malformed_mode_values_refuse_noncanonical_or_inconsistent_records() {
    for (value, expected) in [
        (Vec::new(), Error::InvalidRecord),
        (vec![11, 1], Error::InvalidRecord),
        (vec![11, 1, 0, 0], Error::InvalidRecord),
        (vec![11, 1, 1, 2], Error::InvalidRecord),
        (vec![11, 1, 1, 1, 0], Error::InvalidRecord),
        (vec![11, 1, 1, 0, 0], Error::InvalidRecord),
        (vec![11, 1, 0x81, 0, 0], Error::InvalidRecord),
        (
            vec![11; crate::queue_capacity::MAX_CAPACITY_RECORD_BYTES + 1],
            Error::InvalidRecord,
        ),
        (vec![11, 2, 1, 0], Error::UnsupportedProfile),
        (vec![10, 1, 1, 0], Error::UnsupportedProfile),
        (vec![99, 1, 1, 0], Error::UnsupportedProfile),
    ] {
        let mut fixture = rows(true);
        replace(&mut fixture, &mode_row().0, value);
        assert_eq!(validation_error(&fixture), Some(expected));
    }
}

#[test]
fn finite_modes_usage_and_charge_families_are_outside_the_narrow_role() {
    let mut finite = rows(true);
    replace(&mut finite, &mode_row().0, vec![11, 1, 1, 1, 1]);
    assert_eq!(validation_error(&finite), Some(Error::UnsupportedProfile));
    for tag in [0x17, 0x18] {
        let mut fixture = rows(true);
        let mut key = mode_row().0;
        key[0] = tag;
        if tag == 0x18 {
            key.extend_from_slice(&1_u64.to_be_bytes());
        }
        add(&mut fixture, (key, vec![11, 1, 1, 1, 0, 0]));
        assert_eq!(validation_error(&fixture), Some(Error::UnsupportedProfile));
    }
}

#[test]
fn later_business_generation_remains_unsupported_while_stale_mode_is_inconsistent() {
    let mut later = rows(true);
    replace(
        &mut later,
        &store_keys::entity_incarnation(&namespace(), &entity()),
        codec::encode(&EntityIncarnation::new(2, EntityIncarnationKind::Queue, false).unwrap())
            .unwrap(),
    );
    replace(&mut later, &mode_row().0, vec![11, 1, 2, 0]);
    assert_eq!(validation_error(&later), Some(Error::UnsupportedProfile));
    let mut stale = rows(true);
    replace(&mut stale, &mode_row().0, vec![11, 1, 2, 0]);
    assert_eq!(validation_error(&stale), Some(Error::InconsistentMetadata));
}

#[test]
fn shared_legacy_value_envelopes_are_unsupported_not_migrated() {
    for version in [0, 1, 10, 12, 255] {
        let mut fixture = rows(true);
        let mut value = codec::encode(&QueueConfig::default()).unwrap();
        value[0] = version;
        replace(
            &mut fixture,
            &store_keys::queue_config(&namespace(), &entity()),
            value,
        );
        assert_eq!(validation_error(&fixture), Some(Error::UnsupportedProfile));
    }
}

#[test]
fn non_finite_role_keeps_the_old_closed_session_and_duplicate_profile() {
    let config = QueueConfig {
        requires_session: true,
        requires_duplicate_detection: true,
        ..QueueConfig::default()
    };
    let mut fixture = rows(true);
    let ns = namespace();
    let queue = entity();
    replace(
        &mut fixture,
        &store_keys::queue_config(&ns, &queue),
        codec::encode(&config).unwrap(),
    );
    replace(
        &mut fixture,
        &store_keys::queue_config(&ns, &queue.dead_letter_queue().unwrap()),
        codec::encode(&config.dead_letter_shadow()).unwrap(),
    );
    let mut record = message();
    let session = SessionId::new("Session.Case").unwrap();
    record.session_id = Some(session.clone());
    replace(
        &mut fixture,
        &store_keys::message(&ns, &queue, SequenceNumber::new(1)),
        codec::encode(&record).unwrap(),
    );
    fixture.retain(|(key, _)| *key != store_keys::ready(&ns, &queue, SequenceNumber::new(1)));
    add(
        &mut fixture,
        (
            store_keys::session_ready(&ns, &queue, &session, SequenceNumber::new(1)),
            Vec::new(),
        ),
    );
    let deadline =
        Timestamp::from_millis(100 + config.duplicate_detection_history_time_window_millis);
    add(
        &mut fixture,
        (
            store_keys::duplicate_history(&ns, &queue, &record.message_id),
            codec::encode(&deadline).unwrap(),
        ),
    );
    add(
        &mut fixture,
        (
            store_keys::duplicate_history_expiry(&ns, &queue, deadline, &record.message_id),
            Vec::new(),
        ),
    );
    assert_eq!(validation_error(&fixture), None);
    fixture.retain(|(key, _)| key.first() != Some(&0x16));
    let old = encode(CommittedImageRole::CreateSendV1, &fixture);
    assert!(
        ValidatedCreateSendImage::validate(DecodedCommittedImage::decode(old.as_bytes()).unwrap())
            .is_ok()
    );
}

#[test]
fn checked_types_remain_role_specific_and_role1_rejects_new_sidecars() {
    let old = encode(CommittedImageRole::CreateSendV1, &rows(false));
    assert!(
        ValidatedCreateSendImage::validate(DecodedCommittedImage::decode(old.as_bytes()).unwrap())
            .is_ok()
    );
    assert_eq!(
        ValidatedCreateSendLayout17Image::validate(
            DecodedCommittedImage::decode(old.as_bytes()).unwrap()
        )
        .err(),
        Some(Error::UnsupportedProfile)
    );
    let new = encode(CommittedImageRole::CreateSendLayout17V1, &rows(true));
    assert_eq!(
        ValidatedCreateSendImage::validate(DecodedCommittedImage::decode(new.as_bytes()).unwrap())
            .err(),
        Some(Error::UnsupportedProfile)
    );
    let smuggled = encode(CommittedImageRole::CreateSendV1, &rows(true));
    assert_eq!(
        ValidatedCreateSendImage::validate(
            DecodedCommittedImage::decode(smuggled.as_bytes()).unwrap()
        )
        .err(),
        Some(Error::UnsupportedProfile)
    );
}

#[test]
fn unchanged_business_relation_checks_still_refuse_bad_metadata_message_index_and_clock() {
    let mut missing_incarnation = rows(true);
    missing_incarnation.retain(|(key, _)| key.first() != Some(&0x11));
    assert_eq!(
        validation_error(&missing_incarnation),
        Some(Error::InconsistentMetadata)
    );
    let mut no_ready = rows(true);
    no_ready.retain(|(key, _)| key.first() != Some(&4));
    assert_eq!(validation_error(&no_ready), Some(Error::InconsistentIndex));
    let mut bad_message = rows(true);
    let mut record = message();
    record.sequence = SequenceNumber::new(2);
    replace(
        &mut bad_message,
        &store_keys::message(&namespace(), &entity(), SequenceNumber::new(1)),
        codec::encode(&record).unwrap(),
    );
    assert_eq!(
        validation_error(&bad_message),
        Some(Error::InconsistentMessage)
    );
    let mut bad_clock = rows(true);
    replace(
        &mut bad_clock,
        &store_keys::clock(),
        codec::encode(&101_u64).unwrap(),
    );
    assert_eq!(validation_error(&bad_clock), Some(Error::InvalidClock));
}

#[test]
fn broader_message_option_refuses_without_decoding_its_missing_recursive_payload() {
    let mut fixture = rows(true);
    let mut value = codec::encode(&message()).unwrap();
    assert_eq!(value.last(), Some(&0));
    *value.last_mut().unwrap() = 1;
    replace(
        &mut fixture,
        &store_keys::message(&namespace(), &entity(), SequenceNumber::new(1)),
        value,
    );
    assert_eq!(validation_error(&fixture), Some(Error::UnsupportedProfile));
}

#[test]
fn initial_layout17_profile_has_no_queues_or_modes_and_remains_pure_data() {
    let rows = vec![(
        store_keys::committed_checkpoint(),
        encode_checkpoint(&CommittedCheckpoint::initial(stream())).unwrap(),
    )];
    let encoded = encode(CommittedImageRole::CreateSendLayout17V1, &rows);
    let proof = ValidatedCreateSendLayout17Image::validate(
        DecodedCommittedImage::decode(encoded.as_bytes()).unwrap(),
    )
    .unwrap();
    assert_eq!(proof.queue_count(), 0);
    assert_eq!(proof.message_count(), 0);
    assert_eq!(proof.row_count(), 1);
    assert_eq!(proof.checkpoint(), &CommittedCheckpoint::initial(stream()));
}

#[test]
fn role2_container_keeps_exact_checksum_exhaustion_and_truncation_guards() {
    let original = raw_container(2, &rows(true));
    for end in 0..original.len() {
        assert!(DecodedCommittedImage::decode(&original[..end]).is_err());
    }
    let mut corrupted = original.clone();
    *corrupted.last_mut().unwrap() ^= 1;
    assert_eq!(
        DecodedCommittedImage::decode(&corrupted).unwrap_err(),
        CommittedImageError::ChecksumMismatch
    );
    let mut trailing = original;
    trailing.push(0);
    assert!(DecodedCommittedImage::decode(&trailing).is_err());
}
