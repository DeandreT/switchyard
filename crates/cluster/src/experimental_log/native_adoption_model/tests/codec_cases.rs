use serde::Deserialize;

use super::*;

#[test]
fn paired_header_golden_frame_and_distinct_owner_roles() -> TestResult {
    let binding = Binding {
        pair: [1; 16],
        state: [2; 16],
        log: [3; 16],
        node: 7,
        stream: [7; 16],
    };
    let value = Header {
        binding,
        seed: [9; 32],
        limits: 1,
    };
    let bytes = frame::encode(Kind::Header, Role::Log, &value, MAX_CONTROL)?;
    let mut expected = b"SWAI\0\x01\x01\x02\0\0\0\x62".to_vec();
    for field in [[1; 16], [2; 16], [3; 16]] {
        expected.extend_from_slice(&field);
    }
    expected.push(7);
    expected.extend_from_slice(&[7; 16]);
    expected.extend_from_slice(&[9; 32]);
    expected.push(1);
    let checksum = digest(&expected);
    expected.extend_from_slice(&checksum);
    assert_eq!(bytes, expected);
    assert!(header(Role::Log, &bytes)? == value);
    assert_eq!(
        header(Role::State, &bytes).err(),
        Some(ModelCodecError::Format)
    );
    Ok(())
}

#[test]
fn every_fixed_record_kind_roundtrips_through_its_borrowed_shape() -> TestResult {
    let case = fixture::Case::new()?;
    let intent = decode_intent(&case.intent)?;
    assert_eq!(encode_intent(&intent)?, case.intent);
    assert!(header(Role::Log, &case.header)?.binding == case.binding);
    assert!(progress(&case.old_progress)?.vote == case.vote);
    let base = baseline(&case.old_baseline)?;
    assert!(base.metadata.0 == case.old.metadata);
    assert!(base.metadata.0.as_ptr() >= case.old_baseline.as_ptr());
    assert!(
        base.metadata.0.as_ptr()
            < case
                .old_baseline
                .as_ptr()
                .wrapping_add(case.old_baseline.len())
    );
    assert!(state_fence(&case.old_fence)?.phase == Phase::Ready);
    let state = case.state(true, true)?;
    stage_binding(
        &state
            .iter()
            .find(|(key, _)| key == &[0x20, 3])
            .ok_or("stage missing")?
            .1,
    )?;
    let log = case.log(true)?;
    log_fence(
        &log.iter()
            .find(|(key, _)| key == &[0x20, 5])
            .ok_or("final fence missing")?
            .1,
    )?;
    Ok(())
}

#[test]
fn headers_lengths_checksums_trailing_and_old_schemas_are_refused() -> TestResult {
    let case = fixture::Case::new()?;
    for index in [0, 4, 5, 6, 7, 8, case.header.len() - 1] {
        let mut bytes = case.header.clone();
        bytes[index] ^= 1;
        assert!(header(Role::Log, &bytes).is_err());
    }
    let mut bytes = case.header.clone();
    bytes.push(0);
    assert!(header(Role::Log, &bytes).is_err());
    for bytes in [
        &b"SWLQ\x01queue-log-local-compaction-v1\0"[..],
        &b"SWLF\0\x01\0\x01"[..],
    ] {
        assert!(baseline(bytes).is_err());
    }
    assert_eq!(
        header(Role::Log, &vec![0; MAX_CONTROL + 1]).err(),
        Some(ModelCodecError::Limit)
    );
    Ok(())
}

#[test]
fn borrow_only_visitors_refuse_owned_sequence_and_huge_lengths() {
    use serde::de::value::{BytesDeserializer, Error};
    assert!(
        Blob::<MAX_MEMBER>::deserialize(BytesDeserializer::<Error>::new(b"owned fallback"))
            .is_err()
    );
    let huge = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
    assert!(frame::decode_payload::<Blob<'_, MAX_MEMBER>>(&huge, MAX_NATIVE).is_err());
    let mut checkpoint = [7; 16].to_vec();
    checkpoint.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 1]);
    checkpoint.extend_from_slice(&huge);
    assert!(frame::decode_payload::<CheckpointFields<'_>>(&checkpoint, MAX_CHECKPOINT).is_err());
}

#[test]
fn noncanonical_varints_and_invalid_binding_serials_are_refused() -> TestResult {
    let case = fixture::Case::new()?;
    let mut bytes = case.header.clone();
    bytes.insert(12 + 48, 0x87);
    bytes[12 + 49] = 0;
    let len = bytes.len() - 44;
    bytes[8..12].copy_from_slice(&(len as u32).to_be_bytes());
    let end = bytes.len() - 32;
    let checksum = digest(&bytes[..end]);
    bytes[end..].copy_from_slice(&checksum);
    assert!(header(Role::Log, &bytes).is_err());
    let mut binding = case.binding;
    binding.state = binding.log;
    assert_eq!(binding.check(), Err(ModelCodecError::Fields));
    binding = case.binding;
    binding.pair = [0; 16];
    assert_eq!(binding.check(), Err(ModelCodecError::Fields));
    let mut intent = case.view()?;
    intent.serial = u64::MAX;
    assert!(encode_intent(&intent).is_err());
    let mut fence = state_fence(&case.old_fence)?;
    fence.serial = 1;
    let bytes = frame::encode(Kind::StateFence, Role::State, &fence, MAX_CONTROL)?;
    assert!(state_fence(&bytes).is_err());
    Ok(())
}

#[test]
fn checkpoint_and_member_limits_are_distinct_and_checked_before_copy() -> TestResult {
    assert_eq!(MAX_CHECKPOINT, 8192);
    assert_eq!(MAX_MEMBER, 4096);
    let payload = vec![0; MAX_MEMBER];
    let id = domain::CommittedEntryId {
        term: 1,
        node_id: 7,
        index: 0,
    };
    let fields = CheckpointFields {
        stream: [7; 16],
        last: Some(domain::CommittedEntryMark {
            id,
            fingerprint: [1; 32],
        }),
        previous: None,
        timestamp: 0,
        membership: Some(Member {
            source: id,
            schema: 1,
            payload: Blob(&payload),
        }),
    };
    fields.check()?;
    let bytes = frame::encode_payload(&fields, MAX_CHECKPOINT)?;
    assert!(bytes.len() > MAX_MEMBER && bytes.len() < MAX_CHECKPOINT);
    let decoded: CheckpointFields<'_> = frame::decode_payload(&bytes, MAX_CHECKPOINT)?;
    assert_eq!(
        decoded.membership.ok_or("member absent")?.payload.0.len(),
        MAX_MEMBER
    );
    let oversized = vec![0; MAX_MEMBER + 1];
    let mut fields = fields;
    fields.membership = Some(Member {
        source: id,
        schema: 1,
        payload: Blob(&oversized),
    });
    assert!(fields.check().is_err());
    assert!(frame::encode_payload(&fields, MAX_CHECKPOINT).is_err());
    assert_eq!(
        frame::decode_payload::<CheckpointFields<'_>>(&vec![0; MAX_CHECKPOINT + 1], MAX_CHECKPOINT)
            .err(),
        Some(ModelCodecError::Limit)
    );
    Ok(())
}

#[test]
fn frozen_checkpoint_rejects_invalid_previous_membership_and_initial_shapes() -> TestResult {
    let image = fixture::Image::populated()?;
    let original = CheckpointFields::from_checkpoint(&image.domain_checkpoint);
    for changed in [0, 1, 2, 3] {
        let mut fields = original;
        match changed {
            0 => fields.stream = [0; 16],
            1 => fields.previous = None,
            2 => {
                fields
                    .membership
                    .as_mut()
                    .ok_or("member missing")?
                    .source
                    .index = u64::MAX
            }
            _ => fields.membership.as_mut().ok_or("member missing")?.schema = 0,
        }
        assert!(fields.check().is_err());
    }
    let initial = fixture::Image::initial()?;
    let mut fields = CheckpointFields::from_checkpoint(&initial.domain_checkpoint);
    fields.timestamp = 1;
    assert!(fields.check().is_err());
    Ok(())
}

#[test]
fn intent_total_bound_counts_every_nested_slot() -> TestResult {
    let case = fixture::Case::new()?;
    let bytes8 = vec![0; 8192];
    let bytes16 = vec![0; 16384];
    let bytes256 = vec![0; 256];
    let image = ImageIdentity {
        checkpoint: Blob(&bytes8),
        digest: [1; 32],
        bytes: u64::MAX,
    };
    let mut intent = case.view()?;
    intent.binding.node = u64::MAX;
    intent.serial = u64::MAX;
    intent.old = image;
    intent.old_catalog = Some(CatalogIdentity {
        image,
        metadata: Blob(&bytes8),
    });
    intent.selected = image;
    intent.selected_metadata = Blob(&bytes8);
    intent.selected_native = Blob(&bytes8);
    intent.old_header = Blob(&bytes256);
    intent.old_fence = Some(Blob(&bytes256));
    intent.old_progress = Blob(&bytes256);
    intent.old_baseline = Blob(&bytes16);
    intent.final_progress = Blob(&bytes256);
    intent.final_baseline = Blob(&bytes16);
    intent.selected_recipe = Blob(&bytes256);
    intent.final_recipe = Blob(&bytes256);
    intent.old_entries.count = u64::MAX;
    intent.old_entries.bytes = u64::MAX;
    intent.final_entries = intent.old_entries;
    // Structural upper bound only: deliberately invalid semantic test fields.
    let bytes = frame::encode(Kind::Intent, Role::Log, &intent, MAX_INTENT)?;
    assert_eq!(bytes.len(), 83880);
    assert!(decode_intent(&bytes).is_err());
    assert!(bytes.len() < MAX_INTENT);
    assert_eq!(
        frame::decode::<ModelIntent<'_>>(
            Kind::Intent,
            Role::Log,
            &vec![0; MAX_INTENT + 1],
            MAX_INTENT
        )
        .err(),
        Some(ModelCodecError::Limit)
    );
    Ok(())
}

#[test]
fn native_factory_checks_all_ids_membership_and_exact_snapshot_spelling() -> TestResult {
    let image = fixture::Image::populated()?;
    let pair = crate::DecodedNativeSnapshotPair::decode(&image.metadata, &image.artifact)?;
    let meta = pair.snapshot_meta()?;
    assert!(identity::native_matches(
        &image.metadata,
        image.identity(),
        &image.native
    )?);
    let mut altered_membership = native_fields(&image.native)?.membership.0.to_vec();
    *altered_membership.last_mut().ok_or("membership missing")? ^= 1;
    for changed in [0, 1, 2, 3] {
        let mut fields = native_fields(&image.native)?;
        match changed {
            0 => fields.last.as_mut().ok_or("last missing")?.index ^= 1,
            1 => {
                fields
                    .member_source
                    .as_mut()
                    .ok_or("source missing")?
                    .node_id ^= 1
            }
            2 => fields.snapshot_digest[0] ^= 1,
            _ => fields.membership = Blob(&altered_membership),
        }
        let bytes = frame::encode_payload(&fields, MAX_NATIVE)?;
        assert!(!identity::native_matches(
            &image.metadata,
            image.identity(),
            &bytes
        )?);
    }
    let mut bad = meta.clone();
    bad.snapshot_id.push('0');
    assert!(identity::native_factory(&bad).is_err());
    bad = meta;
    bad.snapshot_id.replace_range(15..16, "A");
    assert!(identity::native_factory(&bad).is_err());
    Ok(())
}

#[test]
fn absent_default_native_membership_is_private_report_only_and_not_shared_encoded() -> TestResult {
    let initial = fixture::Image::initial()?;
    let fields = native_fields(&initial.native)?;
    assert!(fields.last.is_none() && fields.member_source.is_none());
    assert_eq!(fields.member_schema, 0);
    assert!(fields.membership.0.is_empty());
    assert!(identity::native_matches(
        &initial.metadata,
        initial.identity(),
        &initial.native
    )?);
    let empty = openraft::Membership::<u64, openraft::BasicNode>::default();
    assert!(super::super::super::codec::encode_membership(&empty).is_err());
    let pair = crate::DecodedNativeSnapshotPair::decode(&initial.metadata, &initial.artifact)?;
    let mut meta = pair.snapshot_meta()?;
    meta.last_membership = openraft::StoredMembership::new(
        None,
        openraft::Membership::new(
            vec![std::collections::BTreeSet::from([7])],
            std::collections::BTreeMap::from([(7, openraft::BasicNode::new("node"))]),
        ),
    );
    assert_eq!(
        identity::native_factory(&meta).err(),
        Some(ModelCodecError::Fields)
    );
    let mut malformed = fields;
    malformed.member_schema = 1;
    let encoded = frame::encode_payload(&malformed, MAX_NATIVE)?;
    assert!(native_fields(&encoded).is_err());
    Ok(())
}

#[test]
fn native_factory_rejects_already_owned_oversized_membership_before_encoding() -> TestResult {
    use std::collections::{BTreeMap, BTreeSet};
    let image = fixture::Image::populated()?;
    let pair = crate::DecodedNativeSnapshotPair::decode(&image.metadata, &image.artifact)?;
    let mut meta = pair.snapshot_meta()?;
    let membership = openraft::Membership::new(
        vec![BTreeSet::from([7])],
        BTreeMap::from([(7, openraft::BasicNode::new("x".repeat(513)))]),
    );
    meta.last_membership =
        openraft::StoredMembership::new(*meta.last_membership.log_id(), membership);
    assert_eq!(
        identity::native_factory(&meta).err(),
        Some(ModelCodecError::Limit)
    );
    Ok(())
}

#[test]
fn record_control_bounds_cover_maximal_numeric_wire_shapes() -> TestResult {
    let case = fixture::Case::new()?;
    let mut binding = case.binding;
    binding.node = u64::MAX;
    let header_value = Header {
        binding,
        seed: [1; 32],
        limits: 1,
    };
    assert_eq!(
        frame::encode(Kind::Header, Role::Log, &header_value, MAX_CONTROL)?.len(),
        151
    );
    let state = StateFence {
        binding,
        phase: Phase::Selected,
        serial: u64::MAX,
        seed: [2; 32],
        intent: Some([3; 32]),
        selection: [4; 32],
    };
    assert_eq!(
        frame::encode(Kind::StateFence, Role::State, &state, MAX_CONTROL)?.len(),
        226
    );
    let log = LogFence {
        binding,
        serial: u64::MAX,
        intent: [1; 32],
        controls: [2; 32],
        entries: [3; 32],
    };
    assert_eq!(
        frame::encode(Kind::LogFence, Role::Log, &log, MAX_CONTROL)?.len(),
        224
    );
    let stage = StageBinding {
        binding,
        serial: u64::MAX,
        intent: [1; 32],
        selection: [2; 32],
    };
    assert_eq!(
        frame::encode(Kind::StageBinding, Role::State, &stage, MAX_CONTROL)?.len(),
        192
    );
    let metadata = vec![0; MAX_NATIVE];
    let base = Baseline {
        binding,
        ordinal: u64::MAX,
        metadata: Blob(&metadata),
    };
    assert_eq!(
        frame::encode(Kind::Baseline, Role::Log, &base, MAX_BASELINE)?.len(),
        8322
    );
    Ok(())
}

#[test]
fn static_error_and_classification_formatting_contains_no_identity_data() -> TestResult {
    let case = fixture::Case::new()?;
    let classification = case.classify(true, true, true)?;
    for text in std::iter::once(format!("{classification:?}")).chain(
        [
            ModelCodecError::Limit,
            ModelCodecError::Allocation,
            ModelCodecError::Format,
            ModelCodecError::Fields,
            ModelCodecError::Observation,
        ]
        .map(|error| format!("{error:?}: {error}")),
    ) {
        for secret in [
            "PRIVATE",
            "tenant",
            "orders",
            "sha256",
            "/tmp/",
            "fingerprint",
            "snapshot",
        ] {
            assert!(!text.contains(secret));
        }
    }
    Ok(())
}
