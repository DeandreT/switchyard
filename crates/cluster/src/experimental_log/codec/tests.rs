use std::cell::Cell;

use super::*;

fn id(index: u64) -> LogId {
    LogId::new(openraft::CommittedLeaderId::new(3, 7), index)
}

fn send(body: Vec<u8>, message_id: &str) -> QueueLogCommand {
    QueueLogCommand::send(
        NamespaceName::new("test").unwrap(),
        EntityPath::new("orders").unwrap(),
        Timestamp::from_millis(42),
        CommittedSend {
            message_id: message_id.to_owned(),
            body,
            time_to_live_millis: Some(800),
            session_id: Some(SessionId::new("session").unwrap()),
        },
    )
}

fn normal(index: u64, command: QueueLogCommand) -> LogEntry {
    LogEntry {
        log_id: id(index),
        payload: openraft::EntryPayload::Normal(command),
    }
}

fn blank(index: u64) -> LogEntry {
    LogEntry {
        log_id: id(index),
        payload: openraft::EntryPayload::Blank,
    }
}

fn profile() -> LogProfile {
    LogProfile::new(7, domain::CommittedStreamId::new([9; 16]).unwrap()).unwrap()
}

fn membership() -> openraft::Membership<u64, openraft::BasicNode> {
    openraft::Membership::new(
        vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([2, 3, 4])],
        BTreeMap::from_iter((1..=5).map(|id| {
            (
                id,
                openraft::BasicNode {
                    addr: format!("node-{id}"),
                },
            )
        })),
    )
}

#[test]
fn frozen_blank_vote_and_empty_progress_bytes() {
    let entry = LogEntry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(1, 2), 3),
        payload: openraft::EntryPayload::Blank,
    };
    assert_eq!(
        encode_entry(&entry).unwrap().bytes(),
        b"SWLE\x01\x01\x02\x03\x00"
    );
    assert_eq!(
        encode_vote(&LogVote::new_committed(1, 2)).unwrap(),
        b"SWLV\x01\x01\x02\x01"
    );
    assert_eq!(
        encode_progress(&LogProgress::default()).unwrap(),
        b"SWLS\x01\x00\x00\x00\x00\x00"
    );
}

#[test]
fn frozen_profile_stamps_role_leader_mode_and_all_caps() {
    let mut expected = b"SWLP\x01\x0equeue-log-only\x07".to_vec();
    expected.extend_from_slice(&[9; 16]);
    expected.extend_from_slice(&[
        1, 1, 0x80, 0x80, 0x10, 0x80, 0xa0, 0x10, 0x80, 0x9f, 0x10, 0x80, 0x20, 0x80, 0x40, 32,
        0x80, 0x80, 0x80, 2, 0x80, 2, 0x80, 0x80, 0x80, 32, 32, 0x80, 0x80, 0x80, 2,
    ]);
    assert_eq!(encode_profile(&profile()).unwrap(), expected);
    assert_eq!(decode_profile(&expected).unwrap(), profile());
}

#[test]
fn all_entry_payloads_and_full_identity_round_trip() {
    let entries = [
        blank(0),
        normal(
            1,
            QueueLogCommand::create_queue(
                NamespaceName::new("test").unwrap(),
                EntityPath::new("orders").unwrap(),
                Timestamp::from_millis(8),
                QueueConfig::default(),
            ),
        ),
        normal(2, send(vec![0, 255, 7], "message")),
        LogEntry {
            log_id: id(3),
            payload: openraft::EntryPayload::Membership(membership()),
        },
    ];
    for entry in entries {
        let encoded = encode_entry(&entry).unwrap();
        assert_eq!(encoded.id(), entry.log_id);
        assert_eq!(decode_entry(encoded.bytes()).unwrap(), entry);
        let retained = validate_encoded_entry(encoded.bytes().to_vec()).unwrap();
        assert_eq!(retained.bytes(), encoded.bytes());
        assert_eq!(retained.id(), entry.log_id);
    }
}

#[test]
fn queue_serde_is_owned_and_keeps_business_invalid_inputs() {
    fn decode_owned<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> T {
        postcard::from_bytes(bytes).unwrap()
    }
    let config = QueueConfig {
        lock_duration_millis: 0,
        max_delivery_count: 0,
        max_message_bytes: 0,
        ..QueueConfig::default()
    };
    let create = QueueLogCommand::create_queue(
        NamespaceName::new("test").unwrap(),
        EntityPath::new("orders").unwrap(),
        Timestamp::UNIX_EPOCH,
        config,
    );
    let bytes = postcard::to_stdvec(&create).unwrap();
    assert_eq!(decode_owned::<QueueLogCommand>(&bytes), create);
    let long_message_id = "x".repeat(129);
    let command = send(Vec::new(), &long_message_id);
    let bytes = postcard::to_stdvec(&command).unwrap();
    assert_eq!(decode_owned::<QueueLogCommand>(&bytes), command);
    let QueueLogKind::CreateQueue { config: stored, .. } = create.0.as_ref() else {
        panic!("create");
    };
    assert_eq!(*stored, config);
}

#[test]
fn body_exact_cap_succeeds_and_next_byte_is_refused() {
    let exact = normal(0, send(vec![4; MAX_LOG_BODY_BYTES], "body"));
    let bytes = encode_entry(&exact).unwrap();
    assert_eq!(decode_entry(bytes.bytes()).unwrap(), exact);
    let oversized = normal(0, send(vec![4; MAX_LOG_BODY_BYTES + 1], "body"));
    assert_eq!(
        encode_entry(&oversized).unwrap_err(),
        LogCodecError::TooLarge {
            resource: LogResource::Body,
            maximum: MAX_LOG_BODY_BYTES
        }
    );
    assert!(postcard::to_stdvec(&send(vec![4; MAX_LOG_BODY_BYTES + 1], "body")).is_err());
}

#[test]
fn combined_entry_size_is_checked_before_publication() {
    let oversized = normal(0, send(vec![0; MAX_LOG_BODY_BYTES], &"m".repeat(4096)));
    assert_eq!(
        encode_entry(&oversized).unwrap_err(),
        LogCodecError::TooLarge {
            resource: LogResource::Entry,
            maximum: MAX_LOG_QUEUE_BYTES
        }
    );
    assert_eq!(
        decode_entry(&vec![0; MAX_LOG_ENTRY_BYTES + 1]).unwrap_err(),
        LogCodecError::TooLarge {
            resource: LogResource::Entry,
            maximum: MAX_LOG_ENTRY_BYTES
        }
    );
}

#[test]
fn borrowed_validation_retains_original_bytes_without_copying_the_body() {
    let bytes = encode_entry(&normal(0, send(vec![8; MAX_LOG_BODY_BYTES], "body")))
        .unwrap()
        .bytes()
        .to_vec();
    let wire: EntryV1<'_> = decode(
        ENTRY_HEADER,
        &bytes,
        MAX_LOG_ENTRY_BYTES,
        LogResource::Entry,
    )
    .unwrap();
    let PayloadV1::Normal(QueueV1::Send {
        body,
        namespace,
        message_id,
        ..
    }) = &wire.payload
    else {
        panic!("send");
    };
    assert!(matches!(body.0, Cow::Borrowed(_)));
    assert!(matches!(namespace.0, Cow::Borrowed(_)));
    assert!(matches!(message_id.0, Cow::Borrowed(_)));
    drop(wire);
    let pointer = bytes.as_ptr();
    let retained = validate_encoded_entry(bytes).unwrap();
    assert_eq!(retained.bytes().as_ptr(), pointer);
}

#[test]
fn queue_payload_reserves_the_worst_case_domain_wrapper() {
    #[derive(Serialize)]
    struct Wrapper<'a> {
        domain: [u8; 4],
        version: u8,
        stream: [u8; 16],
        previous: Option<domain::CommittedEntryMark>,
        entry: domain::CommittedEntryId,
        queue: QueueV1<'a>,
    }
    let entry = domain::CommittedEntryId {
        term: u64::MAX,
        node_id: u64::MAX,
        index: u64::MAX,
    };
    let previous = domain::CommittedEntryMark {
        id: entry,
        fingerprint: [255; 32],
    };
    let command = send(vec![0; MAX_LOG_BODY_BYTES], "m");
    let queue = QueueV1::from_command(&command).unwrap();
    let payload_size = encoded_size(&queue, MAX_LOG_QUEUE_BYTES, LogResource::Entry).unwrap();
    let wrapper = Wrapper {
        domain: *b"SWYE",
        version: 1,
        stream: [255; 16],
        previous: Some(previous),
        entry,
        queue,
    };
    let wrapper_size = encoded_size(&wrapper, MAX_LOG_ENTRY_BYTES, LogResource::Entry).unwrap();
    assert_eq!(wrapper_size - payload_size, 114);
    assert!(MAX_LOG_ENTRY_BYTES - MAX_LOG_QUEUE_BYTES >= wrapper_size - payload_size);
}

#[test]
fn near_limit_converted_work_is_a_business_refusal_not_an_apply_failure() {
    let mut low: usize = 0;
    let mut high: usize = 4096;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if encode_entry(&normal(
            1,
            send(vec![0; MAX_LOG_BODY_BYTES], &"m".repeat(middle)),
        ))
        .is_ok()
        {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    assert!(low > 128);
    let command = send(vec![0; MAX_LOG_BODY_BYTES], &"m".repeat(low));
    assert_eq!(
        encoded_size(
            &QueueV1::from_command(&command).unwrap(),
            MAX_LOG_QUEUE_BYTES,
            LogResource::Entry
        )
        .unwrap(),
        MAX_LOG_QUEUE_BYTES
    );
    assert!(
        encode_entry(&normal(
            1,
            send(vec![0; MAX_LOG_BODY_BYTES], &"m".repeat(low + 1))
        ))
        .is_err()
    );
    let stream = domain::CommittedStreamId::new([255; 16]).unwrap();
    let mut machine =
        domain::CommittedStateMachine::create(storage::MemoryReplicaStore::new(), stream).unwrap();
    let create = QueueLogCommand::create_queue(
        NamespaceName::new("test").unwrap(),
        EntityPath::new("orders").unwrap(),
        Timestamp::UNIX_EPOCH,
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    );
    let update = domain::CommittedCheckpointUpdate {
        stream,
        expected_previous: None,
        entry: domain::CommittedEntryId {
            term: u64::MAX,
            node_id: u64::MAX,
            index: 0,
        },
    };
    machine
        .apply_committed(&update, &create.into_committed_work())
        .unwrap();
    let update = domain::CommittedCheckpointUpdate {
        stream,
        expected_previous: machine.checkpoint().unwrap().last(),
        entry: domain::CommittedEntryId {
            term: u64::MAX,
            node_id: u64::MAX,
            index: 1,
        },
    };
    let bytes = encode_entry(&normal(1, command)).unwrap();
    let openraft::EntryPayload::Normal(command) = decode_entry(bytes.bytes()).unwrap().payload
    else {
        panic!("normal");
    };
    let result = machine
        .apply_committed(&update, &command.into_committed_work())
        .unwrap();
    assert!(matches!(
        result,
        domain::CommittedApplyResult::Applied {
            application: domain::CommittedApplication::Refused(
                domain::BrokerError::MessageIdTooLong { .. }
            ),
            ..
        }
    ));
    assert_eq!(
        machine.checkpoint().unwrap().last().unwrap().id,
        update.entry
    );
}

#[test]
fn append_limits_consume_at_most_one_over_bound_and_publish_no_packet() {
    let consumed = Cell::new(0);
    let entries = (0..100).map(|index| {
        consumed.set(consumed.get() + 1);
        blank(index)
    });
    assert!(matches!(
        EncodedAppend::from_entries(entries),
        Err(LogCodecError::TooLarge {
            resource: LogResource::AppendEntries,
            maximum: MAX_APPEND_ENTRIES
        })
    ));
    assert_eq!(consumed.get(), MAX_APPEND_ENTRIES + 1);
    let packet = EncodedAppend::from_entries((0..32).map(blank)).unwrap();
    assert_eq!(packet.entries().len(), 32);
    assert_eq!(
        packet.encoded_bytes(),
        packet
            .entries()
            .iter()
            .map(EncodedEntry::encoded_len)
            .sum::<usize>()
    );
    let oversized = (0..32).map(|index| normal(index, send(vec![0; MAX_LOG_BODY_BYTES], "body")));
    assert!(matches!(
        EncodedAppend::from_entries(oversized),
        Err(LogCodecError::TooLarge {
            resource: LogResource::AppendBytes,
            maximum: MAX_APPEND_BYTES
        })
    ));
}

#[test]
fn trailing_nonminimal_varints_and_wrong_versions_are_refused() {
    let mut bytes = encode_entry(&blank(0)).unwrap().bytes().to_vec();
    bytes.push(0);
    assert_eq!(
        decode_entry(&bytes).unwrap_err(),
        LogCodecError::NonCanonical
    );
    let nonminimal = b"SWLE\x01\x83\x00\x07\x00\x00";
    assert_eq!(
        decode_entry(nonminimal).unwrap_err(),
        LogCodecError::NonCanonical
    );
    assert_eq!(
        decode_entry(b"SWLE\x02\x03\x07\x00\x00").unwrap_err(),
        LogCodecError::UnsupportedRecord
    );
    assert_eq!(
        decode_entry(b"SWLE\x01\x80").unwrap_err(),
        LogCodecError::Malformed
    );
}

#[test]
fn structural_names_are_rejected_without_becoming_business_refusals() {
    for (namespace, entity, session) in [
        ("", "orders", None),
        ("test", "bad\npath", None),
        ("test", "orders", Some("")),
    ] {
        let wire = EntryV1 {
            id: IdV1::from_id(id(0)),
            payload: PayloadV1::Normal(QueueV1::Send {
                namespace: Text(Cow::Borrowed(namespace)),
                entity: Text(Cow::Borrowed(entity)),
                issued_at: 42,
                message_id: Text(Cow::Borrowed("")),
                body: Body(Cow::Borrowed(&[])),
                time_to_live_millis: None,
                session_id: session.map(|value| Text(Cow::Borrowed(value))),
            }),
        };
        let bytes = encode(ENTRY_HEADER, &wire, MAX_LOG_ENTRY_BYTES, LogResource::Entry).unwrap();
        assert_eq!(
            decode_entry(&bytes).unwrap_err(),
            LogCodecError::InvalidIdentifier
        );
        assert_eq!(
            validate_encoded_entry(bytes).unwrap_err(),
            LogCodecError::InvalidIdentifier
        );
    }
}

#[test]
fn borrowed_log_parser_rejects_impossible_lengths_and_huge_sequence_hints() {
    let bytes = b"SWLE\x01\x03\x07\x00\x02\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01";
    assert_eq!(decode_entry(bytes).unwrap_err(), LogCodecError::Malformed);
    let wire = QueueV1::Send {
        namespace: Text(Cow::Borrowed("test")),
        entity: Text(Cow::Borrowed("orders")),
        issued_at: 0,
        message_id: Text(Cow::Borrowed("message")),
        body: Body(Cow::Borrowed(&[])),
        time_to_live_millis: None,
        session_id: None,
    };
    let mut bytes = postcard::to_stdvec(&wire).unwrap();
    // Truncate where the message-id bytes begin; a borrowed field must not
    // copy or accept the rest of a structurally incomplete record.
    bytes.truncate(17);
    assert!(postcard::from_bytes::<QueueLogCommand>(&bytes).is_err());
}

#[test]
fn membership_shape_is_exact_and_does_not_invent_missing_nodes() {
    let wire = MembershipV1 {
        configs: Bounded(vec![Bounded(vec![1, 2, 3])]),
        nodes: Bounded(vec![NodeV1 {
            id: 1,
            address: Text(Cow::Borrowed("node")),
        }]),
    };
    assert_eq!(
        wire.validate().unwrap_err(),
        LogCodecError::InvalidMembership
    );
    let wire = MembershipV1 {
        configs: Bounded(vec![Bounded(vec![1])]),
        nodes: Bounded(vec![
            NodeV1 {
                id: 1,
                address: Text(Cow::Borrowed("first")),
            },
            NodeV1 {
                id: 1,
                address: Text(Cow::Borrowed("duplicate")),
            },
        ]),
    };
    assert_eq!(
        wire.validate().unwrap_err(),
        LogCodecError::InvalidMembership
    );
    let bytes = b"SWLE\x01\x03\x07\x00\x02\x03";
    assert_eq!(decode_entry(bytes).unwrap_err(), LogCodecError::Malformed);
    let wire = MembershipV1 {
        configs: Bounded(vec![Bounded(vec![2, 1])]),
        nodes: Bounded(vec![
            NodeV1 {
                id: 1,
                address: Text(Cow::Borrowed("1")),
            },
            NodeV1 {
                id: 2,
                address: Text(Cow::Borrowed("2")),
            },
        ]),
    };
    assert_eq!(
        wire.validate().unwrap_err(),
        LogCodecError::InvalidMembership
    );
    let entry = LogEntry {
        log_id: id(0),
        payload: openraft::EntryPayload::Membership(openraft::Membership::default()),
    };
    assert_eq!(
        encode_entry(&entry).unwrap_err(),
        LogCodecError::InvalidMembership
    );
}

#[test]
fn membership_structural_and_total_byte_caps_are_independent() {
    let nodes = BTreeMap::from_iter((0..33).map(|id| {
        (
            id,
            openraft::BasicNode {
                addr: String::new(),
            },
        )
    }));
    let entry = LogEntry {
        log_id: id(0),
        payload: openraft::EntryPayload::Membership(openraft::Membership::new(
            vec![BTreeSet::from([1])],
            nodes,
        )),
    };
    assert_eq!(
        encode_entry(&entry).unwrap_err(),
        LogCodecError::InvalidMembership
    );
    let nodes = BTreeMap::from_iter((0..9).map(|id| {
        (
            id,
            openraft::BasicNode {
                addr: "x".repeat(MAX_ADDRESS_BYTES),
            },
        )
    }));
    let entry = LogEntry {
        log_id: id(0),
        payload: openraft::EntryPayload::Membership(openraft::Membership::new(
            vec![BTreeSet::from([1])],
            nodes,
        )),
    };
    assert_eq!(
        encode_entry(&entry).unwrap_err(),
        LogCodecError::TooLarge {
            resource: LogResource::Membership,
            maximum: MAX_LOG_MEMBERSHIP_BYTES
        }
    );
}

#[test]
fn all_vote_bits_and_full_u64_ids_are_preserved() {
    for vote in [
        LogVote::new(0, 0),
        LogVote::new(u64::MAX, u64::MAX),
        LogVote::new_committed(u64::MAX, u64::MAX),
    ] {
        assert_eq!(decode_vote(&encode_vote(&vote).unwrap()).unwrap(), vote);
    }
    let entry = LogEntry {
        log_id: LogId::new(
            openraft::CommittedLeaderId::new(u64::MAX, u64::MAX),
            u64::MAX,
        ),
        payload: openraft::EntryPayload::Blank,
    };
    assert_eq!(
        decode_entry(encode_entry(&entry).unwrap().bytes()).unwrap(),
        entry
    );
}

#[test]
fn progress_accepts_empty_purged_history_and_exact_contiguous_suffix() {
    let progress = LogProgress {
        vote: Some(LogVote::new_committed(3, 7)),
        last_purged: Some(id(500)),
        ..LogProgress::default()
    };
    assert_eq!(
        decode_progress(&encode_progress(&progress).unwrap()).unwrap(),
        progress
    );
    let progress = LogProgress {
        vote: None,
        last_purged: Some(id(5)),
        last_present: Some(id(7)),
        retained_entries: 2,
        retained_bytes: 20,
    };
    assert_eq!(
        decode_progress(&encode_progress(&progress).unwrap()).unwrap(),
        progress
    );
    let progress = LogProgress {
        last_purged: Some(id(u64::MAX - 1)),
        last_present: Some(id(u64::MAX)),
        retained_entries: 1,
        retained_bytes: 20,
        vote: None,
    };
    assert_eq!(
        decode_progress(&encode_progress(&progress).unwrap()).unwrap(),
        progress
    );
}

#[test]
fn invalid_progress_never_encodes_or_decodes() {
    for progress in [
        LogProgress {
            retained_entries: 1,
            ..LogProgress::default()
        },
        LogProgress {
            last_present: Some(id(0)),
            retained_entries: 1,
            retained_bytes: 0,
            ..LogProgress::default()
        },
        LogProgress {
            last_present: Some(id(3)),
            retained_entries: 3,
            retained_bytes: 20,
            ..LogProgress::default()
        },
        LogProgress {
            last_present: Some(id(256)),
            retained_entries: 257,
            retained_bytes: 20,
            ..LogProgress::default()
        },
        LogProgress {
            last_present: Some(id(0)),
            retained_entries: 1,
            retained_bytes: MAX_RETAINED_BYTES + 1,
            ..LogProgress::default()
        },
    ] {
        assert_eq!(
            encode_progress(&progress).unwrap_err(),
            LogCodecError::InvalidProgress
        );
        let wire = ProgressV1 {
            vote: progress.vote.map(VoteV1::from_vote),
            last_purged: progress.last_purged.map(IdV1::from_id),
            last_present: progress.last_present.map(IdV1::from_id),
            retained_entries: progress.retained_entries,
            retained_bytes: progress.retained_bytes,
        };
        let bytes = encode(
            PROGRESS_HEADER,
            &wire,
            MAX_LOG_METADATA_BYTES,
            LogResource::Metadata,
        )
        .unwrap();
        assert_eq!(
            decode_progress(&bytes).unwrap_err(),
            LogCodecError::InvalidProgress
        );
    }
}

#[test]
fn changed_role_caps_or_stream_fail_closed() {
    let mut wire = profile_wire(&profile());
    wire.role = Text(Cow::Borrowed("state-machine"));
    let bytes = encode(
        PROFILE_HEADER,
        &wire,
        MAX_LOG_METADATA_BYTES,
        LogResource::Metadata,
    )
    .unwrap();
    assert_eq!(
        decode_profile(&bytes).unwrap_err(),
        LogCodecError::InvalidProfile
    );
    let mut wire = profile_wire(&profile());
    wire.append_entries += 1;
    let bytes = encode(
        PROFILE_HEADER,
        &wire,
        MAX_LOG_METADATA_BYTES,
        LogResource::Metadata,
    )
    .unwrap();
    assert_eq!(
        decode_profile(&bytes).unwrap_err(),
        LogCodecError::InvalidProfile
    );
    let mut wire = profile_wire(&profile());
    wire.stream = [0; 16];
    let bytes = encode(
        PROFILE_HEADER,
        &wire,
        MAX_LOG_METADATA_BYTES,
        LogResource::Metadata,
    )
    .unwrap();
    assert_eq!(
        decode_profile(&bytes).unwrap_err(),
        LogCodecError::InvalidProfile
    );
    assert_eq!(
        decode_profile(&vec![0; MAX_LOG_METADATA_BYTES + 1]).unwrap_err(),
        LogCodecError::TooLarge {
            resource: LogResource::Metadata,
            maximum: MAX_LOG_METADATA_BYTES
        }
    );
}

#[test]
fn redacted_debug_and_errors_do_not_print_payloads() {
    let command = send(b"private-body".to_vec(), "private-id");
    let rendered = format!("{command:?}");
    assert!(!rendered.contains("private"));
    let entry = encode_entry(&normal(0, command)).unwrap();
    assert!(!format!("{entry:?}").contains("private"));
    for error in [
        LogCodecError::Malformed,
        LogCodecError::NonCanonical,
        LogCodecError::InvalidIdentifier,
    ] {
        assert!(!error.to_string().contains("private"));
    }
}
