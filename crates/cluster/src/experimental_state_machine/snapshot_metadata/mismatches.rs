use domain::{MessageRecord, SequenceNumber, ValidatedCreateSendLayout17Image};

use super::{bootstrap_fixture as captured, *};

#[test]
fn every_frozen_field_mismatch_is_refused_with_a_recomputed_canonical_checksum() -> TestResult {
    let source = captured::selected(false)?;
    let original = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    macro_rules! differs {
        ($wire:ident, $change:block) => {{
            let mut $wire = super::super::codec::decode(original.as_bytes())?;
            $change
            let changed = super::super::codec::encode(&$wire)?;
            assert_eq!(DecodedNativeSnapshotPair::decode(&changed, source.image.as_bytes()).err(), Some(NativeSnapshotMetadataError::ImageMismatch));
        }};
    }
    differs!(wire, {
        wire.stream[0] ^= 1;
    });
    differs!(wire, {
        wire.artifact_bytes += 1;
    });
    differs!(wire, {
        wire.digest[0] ^= 1;
    });
    differs!(wire, {
        wire.last = None;
    });
    differs!(wire, {
        wire.last.as_mut().expect("last").id.term += 1;
    });
    differs!(wire, {
        wire.last.as_mut().expect("last").id.node_id += 1;
    });
    differs!(wire, {
        wire.last.as_mut().expect("last").id.index += 1;
    });
    differs!(wire, {
        wire.last.as_mut().expect("last").fingerprint[0] ^= 1;
    });
    differs!(wire, {
        wire.previous = None;
    });
    differs!(wire, {
        wire.previous.as_mut().expect("previous").id.term += 1;
    });
    differs!(wire, {
        wire.previous.as_mut().expect("previous").id.node_id += 1;
    });
    differs!(wire, {
        wire.previous.as_mut().expect("previous").id.index += 1;
    });
    differs!(wire, {
        wire.previous.as_mut().expect("previous").fingerprint[0] ^= 1;
    });
    differs!(wire, {
        wire.highest_timestamp += 1;
    });
    differs!(wire, {
        wire.membership = None;
    });
    differs!(wire, {
        wire.membership.as_mut().expect("member").source.term += 1;
    });
    differs!(wire, {
        wire.membership.as_mut().expect("member").source.node_id += 1;
    });
    differs!(wire, {
        wire.membership.as_mut().expect("member").source.index += 1;
    });
    differs!(wire, {
        wire.membership.as_mut().expect("member").schema_version += 1;
    });
    differs!(wire, {
        wire.membership.as_mut().expect("member").payload = b"PRIVATE-wrong-member";
    });
    let empty = captured::initial()?;
    let metadata = EncodedNativeSnapshotMetadata::encode(empty.image.as_bytes())?;
    for field in 0..3 {
        let original = super::super::codec::decode(original.as_bytes())?;
        let mut wire = super::super::codec::decode(metadata.as_bytes())?;
        match field {
            0 => wire.last = original.last,
            1 => wire.previous = original.previous,
            _ => wire.membership = original.membership,
        }
        let changed = super::super::codec::encode(&wire)?;
        assert_eq!(
            DecodedNativeSnapshotPair::decode(&changed, empty.image.as_bytes()).err(),
            Some(NativeSnapshotMetadataError::ImageMismatch)
        );
    }
    Ok(())
}

#[test]
fn same_full_checkpoint_with_different_valid_body_bytes_has_a_different_snapshot_id() -> TestResult
{
    let source = captured::selected(false)?;
    let key = domain::keys::message(
        &captured::namespace()?,
        &captured::entity()?,
        SequenceNumber::new(1),
    );
    let value = source
        .snapshot
        .entries()
        .iter()
        .find(|(stored, _)| stored == &key)
        .map(|(_, value)| value)
        .ok_or("missing original row")?;
    let mut record: MessageRecord = domain::codec::decode(value)?;
    record.body = b"PRIVATE-other-valid-body".to_vec();
    let changed = captured::with_record(&source, &key, domain::codec::encode(&record)?)?;
    captured::supported(&changed)?;
    assert_eq!(source.checkpoint, changed.checkpoint);
    assert_ne!(source.image.as_bytes(), changed.image.as_bytes());
    let first = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let second = EncodedNativeSnapshotMetadata::encode(changed.image.as_bytes())?;
    let a = DecodedNativeSnapshotPair::decode(first.as_bytes(), source.image.as_bytes())?
        .snapshot_meta()?;
    let b = DecodedNativeSnapshotPair::decode(second.as_bytes(), changed.image.as_bytes())?
        .snapshot_meta()?;
    assert_eq!(a.last_log_id, b.last_log_id);
    assert_eq!(a.last_membership, b.last_membership);
    assert_ne!(a.snapshot_id, b.snapshot_id);
    assert_ne!(first.as_bytes(), second.as_bytes());
    assert_eq!(
        DecodedNativeSnapshotPair::decode(first.as_bytes(), changed.image.as_bytes()).err(),
        Some(NativeSnapshotMetadataError::ImageMismatch)
    );
    assert_eq!(
        DecodedNativeSnapshotPair::decode(second.as_bytes(), source.image.as_bytes()).err(),
        Some(NativeSnapshotMetadataError::ImageMismatch)
    );
    Ok(())
}

#[test]
fn digest_pins_the_complete_artifact_including_its_existing_footer() -> TestResult {
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let wire = super::super::codec::decode(metadata.as_bytes())?;
    assert_eq!(wire.digest, captured::digest(source.image.as_bytes()));
    let excluded = captured::digest(&source.image.as_bytes()[..source.image.len() - 32]);
    assert_ne!(wire.digest, excluded);
    let mut wrong = wire;
    wrong.digest = excluded;
    let metadata = super::super::codec::encode(&wrong)?;
    assert_eq!(
        DecodedNativeSnapshotPair::decode(&metadata, source.image.as_bytes()).err(),
        Some(NativeSnapshotMetadataError::ImageMismatch)
    );
    Ok(())
}

#[test]
fn encoding_and_pair_decoding_both_require_business_semantics_not_a_structural_role() -> TestResult
{
    use storage::{MemoryStore, StateStore, WriteBatch};
    let source = captured::selected(false)?;
    let valid = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let unknown = captured::with_record(&source, &[0x7f, 0x01], b"PRIVATE-unknown".to_vec())?;
    let store = MemoryStore::default();
    let mut batch = WriteBatch::default();
    for (key, value) in source.snapshot.entries() {
        batch.push_put(key.clone(), value.clone());
    }
    batch.push_delete(domain::keys::session_ready(
        &captured::namespace()?,
        &captured::entity()?,
        &domain::SessionId::new("s".repeat(domain::MAX_SESSION_ID_BYTES))?,
        SequenceNumber::new(1),
    ));
    store.apply(batch)?;
    let inconsistent = captured::from_snapshot(store.snapshot()?)?;
    for invalid in [&unknown, &inconsistent] {
        assert!(DecodedCommittedImage::decode(invalid.image.as_bytes()).is_ok());
        assert!(
            ValidatedCreateSendLayout17Image::validate(DecodedCommittedImage::decode(
                invalid.image.as_bytes()
            )?)
            .is_err()
        );
        assert_eq!(
            EncodedNativeSnapshotMetadata::encode(invalid.image.as_bytes()).err(),
            Some(NativeSnapshotMetadataError::InvalidImage)
        );
        assert_eq!(
            DecodedNativeSnapshotPair::decode(valid.as_bytes(), invalid.image.as_bytes()).err(),
            Some(NativeSnapshotMetadataError::InvalidImage)
        );
    }
    Ok(())
}

#[test]
fn domain_valid_but_native_incompatible_identity_and_membership_cannot_form_a_pair() -> TestResult {
    let source = captured::selected(false)?;
    let valid = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let variants = vec![
        captured::altered_checkpoint(&source, |wire| {
            wire.previous.as_mut().expect("previous").id.node_id = 9;
        })?,
        captured::altered_checkpoint(&source, |wire| {
            wire.last.as_mut().expect("last").id.node_id = 6;
        })?,
        captured::altered_checkpoint(&source, |wire| {
            wire.membership.as_mut().expect("member").source.node_id = 9;
        })?,
        captured::altered_checkpoint(&source, |wire| {
            wire.membership.as_mut().expect("member").schema_version = 2;
        })?,
        captured::altered_checkpoint(&source, |wire| {
            wire.membership.as_mut().expect("member").payload = b"PRIVATE-opaque-member".to_vec();
        })?,
        captured::altered_checkpoint(&source, |wire| {
            wire.membership.as_mut().expect("member").payload.push(0);
        })?,
    ];
    for incompatible in variants {
        captured::supported(&incompatible)?;
        assert_eq!(
            EncodedNativeSnapshotMetadata::encode(incompatible.image.as_bytes()).err(),
            Some(NativeSnapshotMetadataError::IncompatibleCheckpoint)
        );
        assert_eq!(
            DecodedNativeSnapshotPair::decode(valid.as_bytes(), incompatible.image.as_bytes())
                .err(),
            Some(NativeSnapshotMetadataError::IncompatibleCheckpoint)
        );
    }
    Ok(())
}

#[test]
fn full_4096_byte_canonical_native_membership_fits_the_frozen_metadata_bound() -> TestResult {
    use std::collections::{BTreeMap, BTreeSet};
    let ids = (u64::MAX - 31..=u64::MAX).collect::<BTreeSet<_>>();
    let nodes = ids
        .iter()
        .enumerate()
        .map(|(offset, id)| {
            (
                *id,
                openraft::BasicNode::new("a".repeat(if offset == 0 { 124 } else { 96 })),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let member = openraft::Membership::new(vec![ids.clone(), ids], nodes);
    let payload = crate::experimental_log::encode_membership(&member)?;
    assert_eq!(payload.len(), 4096);
    let source = captured::selected(false)?;
    let changed = captured::altered_checkpoint(&source, |wire| {
        wire.membership.as_mut().expect("member").payload = payload;
    })?;
    captured::supported(&changed)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(changed.image.as_bytes())?;
    assert!(metadata.len() <= super::super::codec::MAX_FROZEN_METADATA_BYTES);
    assert!(metadata.len() < MAX_NATIVE_SNAPSHOT_METADATA_BYTES);
    let wire = super::super::codec::decode(metadata.as_bytes())?;
    let borrowed = wire.membership.expect("member").payload;
    let start = metadata.as_bytes().as_ptr() as usize;
    let position = borrowed.as_ptr() as usize;
    assert!(position >= start && position + borrowed.len() <= start + metadata.len());
    let pair = DecodedNativeSnapshotPair::decode(metadata.as_bytes(), changed.image.as_bytes())?;
    assert_eq!(pair.snapshot_meta()?.last_membership.membership(), &member);
    Ok(())
}
