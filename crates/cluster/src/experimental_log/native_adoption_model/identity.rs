use openraft::{BasicNode, SnapshotMeta};
use sha2::{Digest, Sha256};

use crate::experimental_state_machine::NativeCheckpointSummary;

use super::{records::*, wire::*, *};

pub(super) fn summary(
    metadata: &[u8],
    identity: ImageIdentity<'_>,
) -> Result<NativeCheckpointSummary> {
    let fields = identity.fields()?;
    let value = NativeCheckpointSummary::decode(metadata).map_err(|_| ModelCodecError::Fields)?;
    if fields.stream != *value.stream.as_bytes()
        || fields.last != value.last
        || fields.previous != value.previous
        || fields.timestamp != value.highest_timestamp.as_millis()
        || identity.digest != value.digest
        || identity.bytes != value.artifact_bytes
        || !member_matches(fields.membership, value.membership.as_ref())
    {
        return Err(ModelCodecError::Fields);
    }
    Ok(value)
}

fn member_matches(
    expected: Option<Member<'_>>,
    actual: Option<&domain::CommittedMembership>,
) -> bool {
    match (expected, actual) {
        (None, None) => true,
        (Some(expected), Some(actual)) => {
            expected.source == actual.source
                && expected.schema == actual.schema_version
                && expected.payload.0 == actual.payload.as_slice()
        }
        _ => false,
    }
}

pub(super) fn image_matches(bytes: &[u8], identity: ImageIdentity<'_>) -> Result<bool> {
    if bytes.len() > MAX_IMAGE {
        return Err(ModelCodecError::Limit);
    }
    // Existing semantic recovery may allocate bounded domain/native state.
    let encoded = crate::EncodedNativeSnapshotMetadata::encode(bytes)
        .map_err(|_| ModelCodecError::Observation)?;
    let actual = NativeCheckpointSummary::decode(encoded.as_bytes())
        .map_err(|_| ModelCodecError::Observation)?;
    let fields = identity.fields()?;
    Ok(fields.stream == *actual.stream.as_bytes()
        && fields.last == actual.last
        && fields.previous == actual.previous
        && fields.timestamp == actual.highest_timestamp.as_millis()
        && member_matches(fields.membership, actual.membership.as_ref())
        && identity.bytes == actual.artifact_bytes
        && identity.digest == actual.digest)
}

pub(super) fn native_matches(
    metadata: &[u8],
    image: ImageIdentity<'_>,
    bytes: &[u8],
) -> Result<bool> {
    let expected = native_fields(bytes)?;
    let value = summary(metadata, image)?;
    let recovered = value.recover().map_err(|_| ModelCodecError::Fields)?;
    let (schema, membership) = native_membership(&recovered.1)?;
    Ok(expected.last == recovered.0.map(committed_id)
        && expected.member_source == recovered.1.log_id().map(committed_id)
        && expected.member_schema == schema
        && expected.membership.0 == membership.as_slice()
        && expected.snapshot_digest == value.digest)
}

pub(super) fn native_factory(meta: &SnapshotMeta<u64, BasicNode>) -> Result<Vec<u8>> {
    if meta.snapshot_id.len() != 79 {
        return Err(ModelCodecError::Fields);
    }
    let (schema, membership) = native_membership(&meta.last_membership)?;
    let text = meta
        .snapshot_id
        .as_bytes()
        .strip_prefix(b"swyi-v1-sha256:")
        .ok_or(ModelCodecError::Fields)?;
    let mut digest = [0; 32];
    for (output, pair) in digest.iter_mut().zip(text.chunks_exact(2)) {
        let digit = |byte| match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(ModelCodecError::Fields),
        };
        *output = (digit(pair[0])? << 4) | digit(pair[1])?;
    }
    frame::encode_payload(
        &NativeFields {
            last: meta.last_log_id.map(committed_id),
            member_source: meta.last_membership.log_id().map(committed_id),
            member_schema: schema,
            membership: Blob(&membership),
            snapshot_digest: digest,
        },
        MAX_NATIVE,
    )
}

fn native_membership(value: &openraft::StoredMembership<u64, BasicNode>) -> Result<(u16, Vec<u8>)> {
    if value.log_id().is_none() {
        if value.membership() != &openraft::Membership::<u64, BasicNode>::default() {
            return Err(ModelCodecError::Fields);
        }
        // Private model sentinel only; do not change the shared nonempty codec.
        return Ok((0, Vec::new()));
    }
    super::super::bounded_membership_len(value.membership()).map_err(|_| ModelCodecError::Limit)?;
    let bytes =
        super::super::encode_membership(value.membership()).map_err(|_| ModelCodecError::Fields)?;
    Ok((super::super::MEMBERSHIP_SCHEMA_VERSION, bytes))
}

fn hash_fields<T: serde::Serialize>(domain: &[u8], value: &T, cap: usize) -> Result<[u8; 32]> {
    let bytes = frame::encode_payload(value, cap)?;
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Ok(hash.finalize().into())
}

pub(super) fn old_selection(intent: &ModelIntent<'_>) -> Result<[u8; 32]> {
    hash_fields(
        b"SWAI-OLD-STATE-1",
        &(intent.old, intent.old_catalog),
        MAX_INTENT,
    )
}

pub(super) fn selected_selection(intent: &ModelIntent<'_>) -> Result<[u8; 32]> {
    hash_fields(
        b"SWAI-SELECTED-STATE-1",
        &(
            intent.selected,
            intent.selected_metadata,
            intent.selected_native,
        ),
        MAX_INTENT,
    )
}

pub(super) fn final_controls(intent: &ModelIntent<'_>) -> Result<[u8; 32]> {
    hash_fields(
        b"SWAI-FINAL-CONTROLS-1",
        &(
            intent.binding,
            intent.old_header,
            intent.final_progress,
            intent.final_baseline,
        ),
        MAX_INTENT,
    )
}

pub(super) fn seed_manifest(intent: &ModelIntent<'_>) -> Result<[u8; 32]> {
    hash_fields(
        b"SWAI-SEED-MANIFEST-1",
        &(
            intent.binding,
            intent.old,
            intent.old_catalog,
            intent.old_progress,
            intent.old_baseline,
            intent.old_entries,
        ),
        MAX_INTENT,
    )
}
