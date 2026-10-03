use sha2::{Digest, Sha256};

use super::*;

mod entry_mark;

fn stream() -> CommittedStreamId {
    CommittedStreamId::new([1; 16]).expect("a stream")
}

fn update() -> CommittedCheckpointUpdate {
    CommittedCheckpointUpdate {
        stream: stream(),
        expected_previous: None,
        entry: CommittedEntryId {
            term: 1,
            node_id: 2,
            index: 0,
        },
    }
}

fn send(body: Vec<u8>, message_id: String) -> CommittedQueueWork {
    CommittedQueueWork::Queue(CommittedQueueCommand::send(
        NamespaceName::new("test").expect("namespace"),
        EntityPath::new("queue").expect("entity"),
        Timestamp::from_millis(10),
        CommittedSend {
            message_id,
            body,
            time_to_live_millis: None,
            session_id: None,
        },
    ))
}

#[test]
fn baseline_checkpoint_has_frozen_v1_bytes() {
    let checkpoint = CommittedCheckpoint::initial(stream());
    let mut expected = b"SWYC\x01".to_vec();
    expected.extend_from_slice(&[1; 16]);
    expected.extend_from_slice(&[0, 0, 0, 0]);
    assert_eq!(encode_checkpoint(&checkpoint).expect("encoding"), expected);
    assert_eq!(decode_checkpoint(&expected).expect("decoding"), checkpoint);
}

#[test]
fn canonical_blank_entry_has_frozen_v1_bytes() {
    let mut expected = b"SWYE\x01".to_vec();
    expected.extend_from_slice(&[1; 16]);
    expected.extend_from_slice(&[0, 1, 2, 0, 0]);
    let hash: [u8; 32] = Sha256::digest(&expected).into();
    assert_eq!(
        entry_fingerprint(&update(), &CommittedQueueWork::Blank).expect("hash"),
        hash
    );
}

#[test]
fn body_and_encoded_entry_limits_are_independent() {
    assert!(
        entry_fingerprint(
            &update(),
            &send(vec![7; MAX_COMMITTED_BODY_BYTES], "id".into())
        )
        .is_ok()
    );
    assert_eq!(
        entry_fingerprint(
            &update(),
            &send(vec![7; MAX_COMMITTED_BODY_BYTES + 1], "id".into())
        ),
        Err(CommittedApplyError::TooLarge {
            resource: "body",
            maximum: MAX_COMMITTED_BODY_BYTES
        }),
    );
    assert_eq!(
        entry_fingerprint(
            &update(),
            &send(Vec::new(), "i".repeat(MAX_COMMITTED_ENTRY_BYTES))
        ),
        Err(CommittedApplyError::TooLarge {
            resource: "entry",
            maximum: MAX_COMMITTED_ENTRY_BYTES
        }),
    );
}

#[test]
fn membership_bound_and_schema_are_checked_before_hashing() {
    let membership = |schema_version, length| CommittedQueueWork::Membership {
        schema_version,
        payload: vec![5; length],
    };
    assert!(entry_fingerprint(&update(), &membership(1, MAX_COMMITTED_MEMBERSHIP_BYTES)).is_ok());
    assert_eq!(
        entry_fingerprint(&update(), &membership(0, 0)),
        Err(CommittedApplyError::InvalidMembershipSchema)
    );
    assert_eq!(
        entry_fingerprint(
            &update(),
            &membership(1, MAX_COMMITTED_MEMBERSHIP_BYTES + 1)
        ),
        Err(CommittedApplyError::TooLarge {
            resource: "membership",
            maximum: MAX_COMMITTED_MEMBERSHIP_BYTES
        }),
    );
}

#[test]
fn all_content_and_position_fields_change_the_entry_fingerprint() {
    let base = entry_fingerprint(&update(), &send(vec![1], "id".into())).expect("hash");
    let mut changed = update();
    changed.entry.node_id += 1;
    assert_ne!(
        base,
        entry_fingerprint(&changed, &send(vec![1], "id".into())).expect("hash")
    );
    changed = update();
    changed.expected_previous = Some(CommittedEntryMark {
        id: changed.entry,
        fingerprint: [3; 32],
    });
    assert_ne!(
        base,
        entry_fingerprint(&changed, &send(vec![1], "id".into())).expect("hash")
    );
    assert_ne!(
        base,
        entry_fingerprint(&update(), &send(vec![2], "id".into())).expect("hash")
    );
    assert_ne!(
        base,
        entry_fingerprint(&update(), &send(vec![1], "other".into())).expect("hash")
    );
    let altered = CommittedQueueWork::Queue(CommittedQueueCommand::send(
        NamespaceName::new("test").expect("namespace"),
        EntityPath::new("queue").expect("entity"),
        Timestamp::from_millis(11),
        CommittedSend {
            message_id: "id".into(),
            body: vec![1],
            time_to_live_millis: Some(1),
            session_id: None,
        },
    ));
    assert_ne!(base, entry_fingerprint(&update(), &altered).expect("hash"));
}

#[test]
fn serde_identifiers_cannot_bypass_canonical_entry_validation() {
    let namespace: NamespaceName =
        postcard::from_bytes(&postcard::to_stdvec("bad\0scope").expect("encoding"))
            .expect("raw deserialization");
    let work = CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
        namespace,
        EntityPath::new("queue").expect("entity"),
        Timestamp::UNIX_EPOCH,
        QueueConfig::default(),
    ));
    assert_eq!(
        entry_fingerprint(&update(), &work),
        Err(CommittedApplyError::InvalidIdentifier(
            IdentifierError::ControlCharacter { kind: "namespace" }
        ))
    );
}

#[test]
fn checkpoint_rejects_other_headers_versions_trailing_and_oversize_bytes() {
    let bytes = encode_checkpoint(&CommittedCheckpoint::initial(stream())).expect("encoding");
    for offset in 0..5 {
        let mut corrupt = bytes.clone();
        corrupt[offset] ^= 1;
        assert_eq!(
            decode_checkpoint(&corrupt),
            Err(CommittedApplyError::CorruptCheckpoint)
        );
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert_eq!(
        decode_checkpoint(&trailing),
        Err(CommittedApplyError::CorruptCheckpoint)
    );
    assert_eq!(
        decode_checkpoint(&vec![0; MAX_COMMITTED_CHECKPOINT_BYTES + 1]),
        Err(CommittedApplyError::CorruptCheckpoint)
    );
}

#[test]
fn checkpoint_rejects_noncanonical_varints_and_unavailable_borrowed_payloads() {
    let baseline = encode_checkpoint(&CommittedCheckpoint::initial(stream())).expect("encoding");
    let mut noncanonical = baseline.clone();
    // The initial watermark follows stream, last, and predecessor: replace its
    // zero varint with an overlong zero encoding.
    noncanonical.splice(23..24, [0x80, 0]);
    assert_eq!(
        decode_checkpoint(&noncanonical),
        Err(CommittedApplyError::CorruptCheckpoint)
    );

    let mut checkpoint = CommittedCheckpoint::initial(stream());
    checkpoint.last = Some(CommittedEntryMark {
        id: update().entry,
        fingerprint: [5; 32],
    });
    checkpoint.membership = Some(CommittedMembership {
        source: update().entry,
        schema_version: 1,
        payload: vec![99],
    });
    let mut bytes = encode_checkpoint(&checkpoint).expect("encoding");
    let length_offset = bytes.len() - 2;
    bytes.splice(
        length_offset..length_offset + 1,
        [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01],
    );
    assert_eq!(
        decode_checkpoint(&bytes),
        Err(CommittedApplyError::CorruptCheckpoint)
    );
}

#[test]
fn checkpoint_rejects_impossible_predecessor_and_membership_positions() {
    let mut checkpoint = CommittedCheckpoint::initial(stream());
    let last = CommittedEntryMark {
        id: update().entry,
        fingerprint: [5; 32],
    };
    checkpoint.previous = Some(last);
    assert_eq!(
        encode_checkpoint(&checkpoint),
        Err(CommittedApplyError::CorruptCheckpoint)
    );
    checkpoint.previous = None;
    checkpoint.last = Some(last);
    checkpoint.membership = Some(CommittedMembership {
        source: CommittedEntryId {
            index: 1,
            ..last.id
        },
        schema_version: 1,
        payload: Vec::new(),
    });
    assert_eq!(
        encode_checkpoint(&checkpoint),
        Err(CommittedApplyError::CorruptCheckpoint)
    );
}

#[test]
fn uninitialized_checkpoint_cannot_hide_membership_or_timestamp() {
    let mut checkpoint = CommittedCheckpoint::initial(stream());
    checkpoint.highest_timestamp = Timestamp::from_millis(1);
    assert_eq!(
        encode_checkpoint(&checkpoint),
        Err(CommittedApplyError::CorruptCheckpoint)
    );
    checkpoint.highest_timestamp = Timestamp::UNIX_EPOCH;
    checkpoint.membership = Some(CommittedMembership {
        source: update().entry,
        schema_version: 1,
        payload: Vec::new(),
    });
    assert_eq!(
        encode_checkpoint(&checkpoint),
        Err(CommittedApplyError::CorruptCheckpoint)
    );
}

#[test]
fn membership_source_must_match_the_retained_predecessor_identity() {
    let mut checkpoint = CommittedCheckpoint::initial(stream());
    let previous = CommittedEntryMark {
        id: update().entry,
        fingerprint: [5; 32],
    };
    checkpoint.previous = Some(previous);
    checkpoint.last = Some(CommittedEntryMark {
        id: CommittedEntryId {
            index: 1,
            ..previous.id
        },
        fingerprint: [6; 32],
    });
    checkpoint.membership = Some(CommittedMembership {
        source: CommittedEntryId {
            node_id: 999,
            ..previous.id
        },
        schema_version: 1,
        payload: Vec::new(),
    });
    assert_eq!(
        encode_checkpoint(&checkpoint),
        Err(CommittedApplyError::CorruptCheckpoint)
    );
    checkpoint.membership.as_mut().expect("membership").source = previous.id;
    assert!(encode_checkpoint(&checkpoint).is_ok());
}

#[test]
fn older_membership_cannot_have_a_newer_term_than_the_retained_predecessor() {
    let mut checkpoint = CommittedCheckpoint::initial(stream());
    checkpoint.previous = Some(CommittedEntryMark {
        id: CommittedEntryId {
            term: 1,
            node_id: 2,
            index: 3,
        },
        fingerprint: [5; 32],
    });
    checkpoint.last = Some(CommittedEntryMark {
        id: CommittedEntryId {
            term: 2,
            node_id: 2,
            index: 4,
        },
        fingerprint: [6; 32],
    });
    checkpoint.membership = Some(CommittedMembership {
        source: CommittedEntryId {
            term: 2,
            node_id: 2,
            index: 0,
        },
        schema_version: 1,
        payload: Vec::new(),
    });
    assert_eq!(
        encode_checkpoint(&checkpoint),
        Err(CommittedApplyError::CorruptCheckpoint)
    );
}

#[test]
fn debug_output_does_not_expose_message_or_membership_content() {
    let work = send(b"private body".to_vec(), "private identifier".into());
    let membership = CommittedQueueWork::Membership {
        schema_version: 1,
        payload: b"private membership".to_vec(),
    };
    let output = format!("{work:?} {membership:?}");
    assert!(!output.contains("private"));
    assert!(!output.contains("112, 114, 105"));
}

#[test]
fn zero_streams_are_refused_even_when_deserialized() {
    assert_eq!(
        CommittedStreamId::new([0; 16]),
        Err(CommittedApplyError::InvalidStreamId)
    );
    let zero: CommittedStreamId = postcard::from_bytes(&[0; 16]).expect("raw deserialization");
    assert_eq!(
        entry_fingerprint(
            &CommittedCheckpointUpdate {
                stream: zero,
                ..update()
            },
            &CommittedQueueWork::Blank
        ),
        Err(CommittedApplyError::InvalidStreamId)
    );
}
