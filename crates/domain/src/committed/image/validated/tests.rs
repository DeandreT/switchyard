use crate::{
    EntityPath, LockToken, MessageRecord, MessageState, NamespaceName, SequenceNumber, SessionId,
    Timestamp, codec, keys as store_keys,
};

use super::{
    CommittedImageValidationError as Error,
    keys::{Key, Scope},
    records,
};

fn record() -> MessageRecord {
    MessageRecord {
        sequence: SequenceNumber::new(7),
        message_id: "id\0with-tail".into(),
        body: vec![0, 127, 128, 255],
        enqueued_at: Timestamp::from_millis(100),
        expires_at: Some(Timestamp::from_millis(100)),
        delivery_count: 0,
        state: MessageState::Ready,
        session_id: Some(SessionId::new("session").expect("valid fixture")),
        dead_letter: None,
        scheduled_enqueue_time: None,
        envelope: None,
    }
}

#[test]
fn current_message_wire_borrows_body_ids_and_session_without_normalization() {
    let source = record();
    let encoded = codec::encode(&source).expect("fixture encode");
    let decoded = records::message(&encoded).expect("supported wire");
    assert_eq!(decoded.sequence, 7);
    assert_eq!(decoded.message_id, source.message_id);
    assert_eq!(decoded.body, source.body);
    assert_eq!(decoded.session_id, Some("session"));
    assert_eq!(decoded.expires_at, Some(100));
    for bytes in [
        decoded.body,
        decoded.message_id.as_bytes(),
        decoded.session_id.expect("session").as_bytes(),
    ] {
        let start = bytes.as_ptr() as usize;
        let backing = encoded.as_ptr() as usize;
        assert!(start >= backing && start + bytes.len() <= backing + encoded.len());
    }
    assert_eq!(
        postcard::to_stdvec(&decoded).expect("fixture encoding"),
        encoded[1..]
    );
    assert_eq!(codec::ACTIVE_VALUE_FORMAT, 11);
}

#[test]
fn streaming_comparison_rejects_overlong_varints_and_trailing_supported_records() {
    let original = codec::encode(&record()).expect("fixture encode");
    let mut nonminimal = original.clone();
    nonminimal.splice(1..2, [0x87, 0x00]);
    assert_eq!(
        records::message(&nonminimal).err(),
        Some(Error::InvalidRecord)
    );
    let mut trailing = original;
    trailing.push(0);
    assert_eq!(
        records::message(&trailing).err(),
        Some(Error::InvalidRecord)
    );
    assert_eq!(
        records::decode::<u64>(&[11, 0x80, 0]).err(),
        Some(Error::InvalidRecord)
    );
}

#[test]
fn legacy_and_unknown_value_envelopes_are_unsupported_not_migrated() {
    let original = codec::encode(&record()).expect("fixture encode");
    for version in [0, 1, 6, 10, 12, 255] {
        let mut encoded = original.clone();
        encoded[0] = version;
        assert_eq!(
            records::message(&encoded).err(),
            Some(Error::UnsupportedProfile)
        );
    }
    assert_eq!(records::message(&[]).err(), Some(Error::InvalidRecord));
}

#[test]
fn broader_states_and_delivery_history_are_conservatively_unsupported() {
    for state in [
        MessageState::Locked {
            token: LockToken::new(1),
            locked_until: Timestamp::from_millis(200),
        },
        MessageState::Deferred,
        MessageState::Scheduled {
            enqueue_at: Timestamp::from_millis(200),
            time_to_live_millis: None,
        },
    ] {
        let mut source = record();
        source.state = state;
        assert_eq!(
            records::message(&codec::encode(&source).expect("fixture encode")).err(),
            Some(Error::UnsupportedProfile)
        );
    }
    let mut source = record();
    source.delivery_count = 1;
    assert_eq!(
        records::message(&codec::encode(&source).expect("fixture encode")).err(),
        Some(Error::UnsupportedProfile)
    );
}

#[test]
fn unsupported_some_flags_refuse_before_decoding_any_inner_value() {
    let mut source = record();
    source.session_id = None;
    let original = codec::encode(&source).expect("fixture encode");
    assert_eq!(&original[original.len() - 3..], &[0, 0, 0]);
    for offset in 0..3 {
        let flag = original.len() - 3 + offset;
        let mut encoded = original[..=flag].to_vec();
        encoded[flag] = 1;
        // Deliberately no inner bytes; Some is an unsupported profile even
        // before attempting a rich/dead-letter/scheduled metadata decode.
        assert_eq!(
            records::message(&encoded).err(),
            Some(Error::UnsupportedProfile)
        );
    }
}

#[test]
fn malformed_option_state_and_declared_length_inputs_are_refused() {
    let original = codec::encode(&record()).expect("fixture encode");
    let mut bad_option = original.clone();
    let end = bad_option.len();
    bad_option[end - 1] = 2;
    assert_eq!(
        records::message(&bad_option).err(),
        Some(Error::InvalidRecord)
    );
    // Sequence 1 followed by a string length u64::MAX, with no supplied bytes.
    let mut huge_length = vec![11, 1];
    huge_length.extend(postcard::to_stdvec(&u64::MAX).expect("fixture encode"));
    assert_eq!(
        records::message(&huge_length).err(),
        Some(Error::InvalidRecord)
    );
    assert_eq!(records::message(&[11, 0]).err(), Some(Error::InvalidRecord));
    let source = record();
    let prefix = postcard::to_stdvec(&(
        source.sequence,
        source.message_id.as_str(),
        source.body.as_slice(),
        source.enqueued_at,
        source.expires_at,
        source.delivery_count,
    ))
    .expect("fixture prefix");
    let mut unknown_state = vec![11];
    unknown_state.extend(prefix);
    unknown_state.push(4);
    assert_eq!(
        records::message(&unknown_state).err(),
        Some(Error::InvalidRecord)
    );
}

#[test]
fn message_scalar_identifier_and_body_limits_are_checked() {
    let mut source = record();
    source.sequence = SequenceNumber::new(0);
    assert_eq!(
        records::message(&codec::encode(&source).expect("fixture encode")).err(),
        Some(Error::InvalidRecord)
    );
    source = record();
    source.message_id = "i".repeat(crate::MAX_MESSAGE_ID_LENGTH + 1);
    assert_eq!(
        records::message(&codec::encode(&source).expect("fixture encode")).err(),
        Some(Error::InvalidRecord)
    );
    source = record();
    source.body = vec![0; crate::MAX_COMMITTED_BODY_BYTES + 1];
    assert_eq!(
        records::message(&codec::encode(&source).expect("fixture encode")).err(),
        Some(Error::InvalidRecord)
    );
    source = record();
    source.expires_at = Some(Timestamp::from_millis(99));
    assert_eq!(
        records::message(&codec::encode(&source).expect("fixture encode")).err(),
        Some(Error::InvalidRecord)
    );
}

#[test]
fn all_supported_scoped_keys_match_current_key_construction() {
    let namespace = NamespaceName::new("Tenant").expect("fixture namespace");
    let entity = EntityPath::new("/Orders/$Management/literal").expect("fixture entity");
    let session = SessionId::new("Case.Session").expect("fixture session");
    let sequence = SequenceNumber::new(9);
    let deadline = Timestamp::from_millis(123);
    let scope = Scope {
        namespace: namespace.as_str(),
        entity: entity.as_str(),
    };
    assert!(
        matches!(Key::parse(&store_keys::queue_config(&namespace, &entity)), Ok(Key::Config(found)) if found == scope)
    );
    assert!(
        matches!(Key::parse(&store_keys::queue_counters(&namespace, &entity)), Ok(Key::Counters(found)) if found == scope)
    );
    assert!(
        matches!(Key::parse(&store_keys::message(&namespace, &entity, sequence)), Ok(Key::Message(found, 9)) if found == scope)
    );
    assert!(
        matches!(Key::parse(&store_keys::ready(&namespace, &entity, sequence)), Ok(Key::Ready(found, 9)) if found == scope)
    );
    assert!(
        matches!(Key::parse(&store_keys::expiry(&namespace, &entity, deadline, sequence)), Ok(Key::Expiry(found, 123, 9)) if found == scope)
    );
    assert!(
        matches!(Key::parse(&store_keys::session_ready(&namespace, &entity, &session, sequence)), Ok(Key::SessionReady(found, "Case.Session", 9)) if found == scope)
    );
    assert!(
        matches!(Key::parse(&store_keys::entity_incarnation(&namespace, &entity)), Ok(Key::Incarnation(found)) if found == scope)
    );
}

#[test]
fn history_key_ids_consume_the_whole_utf8_remainder_including_nul() {
    let namespace = NamespaceName::new("tenant").expect("fixture namespace");
    let entity = EntityPath::new("orders").expect("fixture entity");
    let id = "id\0eight\0tail";
    let key = store_keys::duplicate_history(&namespace, &entity, id);
    assert!(matches!(Key::parse(&key), Ok(Key::History(_, found)) if found == id));
    let key =
        store_keys::duplicate_history_expiry(&namespace, &entity, Timestamp::from_millis(200), id);
    assert!(matches!(Key::parse(&key), Ok(Key::HistoryExpiry(_, 200, found)) if found == id));
    assert_eq!(
        Key::parse(b"\x0ctenant\0orders\0").err(),
        Some(Error::InvalidKey)
    );
}

#[test]
fn key_parser_rejects_unknown_families_and_nonexact_tails() {
    for tag in [0x05, 0x07, 0x08, 0x0a, 0x0b, 0x0e, 0x0f, 0x10, 0x13, 255] {
        assert_eq!(Key::parse(&[tag]).err(), Some(Error::UnsupportedProfile));
    }
    for key in [
        b"\x00extra".as_slice(),
        b"\x12extra".as_slice(),
        b"\x01tenant\0orders\0tail".as_slice(),
        b"\x03tenant\0orders\0".as_slice(),
        b"\x04tenant\0orders\0\0\0\0\0\0\0\0\0".as_slice(),
        b"\x09tenant\0orders\0\0\0\0\0\0\0\0\0\x01".as_slice(),
        b"\x06tenant\0orders\0short".as_slice(),
    ] {
        assert_eq!(Key::parse(key).err(), Some(Error::InvalidKey));
    }
}

#[test]
fn key_scope_identifiers_validate_utf8_controls_and_exact_lengths() {
    for key in [
        b"\x01\0orders\0".as_slice(),
        b"\x01tenant\0\0".as_slice(),
        b"\x01ten\ntenant\0orders\0".as_slice(),
        b"\x01tenant\0\xff\0".as_slice(),
    ] {
        assert_eq!(Key::parse(key).err(), Some(Error::InvalidKey));
    }
    let mut key = vec![1];
    key.extend_from_slice(&[b'n'; crate::MAX_NAMESPACE_NAME_BYTES + 1]);
    key.extend_from_slice(b"\0orders\0");
    assert_eq!(Key::parse(&key).err(), Some(Error::InvalidKey));
}

#[test]
fn primary_scope_uses_literal_case_with_only_reserved_branch_classification() {
    assert!(
        Scope {
            namespace: "tenant",
            entity: "/Orders/$Management/literal"
        }
        .is_primary()
    );
    assert!(
        !Scope {
            namespace: "tenant",
            entity: "Orders/Subscriptions/child"
        }
        .is_primary()
    );
    assert!(
        Scope {
            namespace: "tenant",
            entity: "Orders/$DEADLETTERQUEUE"
        }
        .is_shadow()
    );
}

#[test]
fn error_diagnostics_are_static_and_do_not_include_source_content() {
    for error in [
        Error::UnsupportedProfile,
        Error::InvalidKey,
        Error::InvalidRecord,
        Error::InconsistentMetadata,
        Error::InconsistentMessage,
        Error::InconsistentIndex,
        Error::InconsistentHistory,
        Error::InvalidClock,
    ] {
        let rendered = format!("{error:?}: {error}");
        assert!(!rendered.contains("id\0with-tail"));
        assert!(!rendered.contains("Tenant"));
    }
}
