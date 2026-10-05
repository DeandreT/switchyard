use domain::{
    CommittedCheckpointUpdate, CommittedEntryId, CommittedMembership, CommittedQueueWork,
};
use openraft::EntryPayload;
use sha2::{Digest, Sha256};

use crate::experimental_state_machine::NativeCheckpointSummary;

use super::{records::*, wire::*, *};

pub(super) fn is_entry(row: Row<'_>) -> bool {
    row.key.first() == Some(&0x21)
}

pub(super) fn key(index: u64) -> [u8; 9] {
    let mut key = [0x21; 9];
    key[1..].copy_from_slice(&index.to_be_bytes());
    key
}

pub(super) fn manifest(rows: &[Row<'_>]) -> Result<EntryManifest> {
    manifest_from(rows, None)
}

fn manifest_from(rows: &[Row<'_>], after: Option<u64>) -> Result<EntryManifest> {
    if rows.len() > crate::MAX_RETAINED_ENTRIES as usize + 5 {
        return Err(ModelCodecError::Limit);
    }
    let mut count = 0u64;
    let mut bytes = 0u64;
    let mut previous = None;
    for row in rows.iter().copied().filter(|row| is_entry(*row)) {
        if row.key.len() != 9 || row.value.len() > crate::MAX_LOG_ENTRY_BYTES {
            return Err(ModelCodecError::Limit);
        }
        let index = u64::from_be_bytes(
            row.key[1..]
                .try_into()
                .map_err(|_| ModelCodecError::Observation)?,
        );
        if after.is_some_and(|after| index <= after) {
            continue;
        }
        if previous.is_some_and(|previous| previous >= index) {
            return Err(ModelCodecError::Observation);
        }
        previous = Some(index);
        count = count
            .checked_add(1)
            .filter(|count| *count <= crate::MAX_RETAINED_ENTRIES)
            .ok_or(ModelCodecError::Limit)?;
        bytes = bytes
            .checked_add(row.value.len() as u64)
            .filter(|bytes| *bytes <= crate::MAX_RETAINED_BYTES)
            .ok_or(ModelCodecError::Limit)?;
    }
    let mut hash = Sha256::new();
    hash.update(b"SWAIROW1");
    hash.update(count.to_be_bytes());
    hash.update(bytes.to_be_bytes());
    for row in rows.iter().copied().filter(|row| is_entry(*row)) {
        let index = u64::from_be_bytes(
            row.key[1..]
                .try_into()
                .map_err(|_| ModelCodecError::Observation)?,
        );
        if after.is_some_and(|after| index <= after) {
            continue;
        }
        hash.update(9u64.to_be_bytes());
        hash.update(row.key);
        hash.update((row.value.len() as u64).to_be_bytes());
        hash.update(row.value);
    }
    Ok(EntryManifest {
        count,
        bytes,
        digest: hash.finalize().into(),
    })
}

pub(super) fn suffix_manifest(
    rows: &[Row<'_>],
    selected: CommittedEntryId,
) -> Result<(EntryManifest, Option<CommittedEntryId>)> {
    // Validate the complete inventory before indexing/hashing any suffix bytes.
    manifest(rows)?;
    let mut previous = native_id(selected);
    let mut present = None;
    for row in rows.iter().copied().filter(|row| is_entry(*row)) {
        let index = u64::from_be_bytes(
            row.key[1..]
                .try_into()
                .map_err(|_| ModelCodecError::Observation)?,
        );
        if index <= selected.index {
            continue;
        }
        let entry = super::super::codec::decode_entry(row.value)
            .map_err(|_| ModelCodecError::Observation)?;
        if row.key != key(entry.log_id.index)
            || previous.index.checked_add(1) != Some(entry.log_id.index)
            || entry.log_id <= previous
        {
            return Err(ModelCodecError::Fields);
        }
        previous = entry.log_id;
        present = Some(committed_id(entry.log_id));
    }
    Ok((manifest_from(rows, Some(selected.index))?, present))
}

pub(super) fn validate(
    rows: &[Row<'_>],
    progress: Progress,
    baseline: &NativeCheckpointSummary,
) -> Result<()> {
    let inventory = manifest(rows)?;
    if progress.purged != baseline.last.map(|mark| mark.id)
        || progress.entries != inventory.count
        || progress.bytes != inventory.bytes
    {
        return Err(ModelCodecError::Fields);
    }
    let mut previous = progress.purged.map(native_id);
    let mut present = None;
    for row in rows.iter().copied().filter(|row| is_entry(*row)) {
        let entry = super::super::codec::decode_entry(row.value)
            .map_err(|_| ModelCodecError::Observation)?;
        if row.key != key(entry.log_id.index) {
            return Err(ModelCodecError::Observation);
        }
        let valid = previous.map_or(entry.log_id.index == 0, |previous| {
            previous.index.checked_add(1) == Some(entry.log_id.index) && entry.log_id > previous
        });
        if !valid {
            return Err(ModelCodecError::Observation);
        }
        previous = Some(entry.log_id);
        present = Some(committed_id(entry.log_id));
    }
    if present != progress.present {
        return Err(ModelCodecError::Fields);
    }
    Ok(())
}

pub(super) fn vote_sufficient(
    vote: Option<Vote>,
    selected: CommittedEntryId,
    rows: &[Row<'_>],
) -> Result<bool> {
    let Some(vote) = vote.filter(|vote| vote.committed) else {
        return Ok(false);
    };
    let sufficient = |id: CommittedEntryId| {
        matches!(
            vote.native()
                .partial_cmp(&crate::LogVote::new_committed(id.term, id.node_id)),
            Some(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater)
        )
    };
    if !sufficient(selected) {
        return Ok(false);
    }
    for row in rows.iter().copied().filter(|row| is_entry(*row)) {
        let entry = super::super::codec::decode_entry(row.value)
            .map_err(|_| ModelCodecError::Observation)?;
        if !sufficient(committed_id(entry.log_id)) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn reaches_old(
    rows: &[Row<'_>],
    baseline: &NativeCheckpointSummary,
    old: CheckpointFields<'_>,
) -> Result<bool> {
    let mut last = baseline.last;
    let mut previous = baseline.previous;
    let mut watermark = baseline.highest_timestamp.as_millis();
    let mut membership = baseline.membership.clone();
    let matches = |last, previous, watermark, membership: &Option<CommittedMembership>| {
        old.stream == *baseline.stream.as_bytes()
            && old.last == last
            && old.previous == previous
            && old.timestamp == watermark
            && match (old.membership, membership.as_ref()) {
                (None, None) => true,
                (Some(old), Some(actual)) => {
                    old.source == actual.source
                        && old.schema == actual.schema_version
                        && old.payload.0 == actual.payload.as_slice()
                }
                _ => false,
            }
    };
    if matches(last, previous, watermark, &membership) {
        return Ok(true);
    }
    let Some(target) = old.last else {
        return Ok(false);
    };
    for row in rows.iter().copied().filter(|row| is_entry(*row)) {
        let entry = super::super::codec::decode_entry(row.value)
            .map_err(|_| ModelCodecError::Observation)?;
        if entry.log_id.index > target.id.index {
            break;
        }
        let id = committed_id(entry.log_id);
        let work = match entry.payload {
            EntryPayload::Blank => CommittedQueueWork::Blank,
            EntryPayload::Membership(value) => {
                let payload = super::super::encode_membership(&value)
                    .map_err(|_| ModelCodecError::Observation)?;
                membership = Some(CommittedMembership {
                    source: id,
                    schema_version: super::super::MEMBERSHIP_SCHEMA_VERSION,
                    payload: payload.clone(),
                });
                CommittedQueueWork::Membership {
                    schema_version: super::super::MEMBERSHIP_SCHEMA_VERSION,
                    payload,
                }
            }
            EntryPayload::Normal(command) => {
                watermark =
                    watermark.max(super::super::queue_command_timestamp(&command).as_millis());
                command.into_committed_work()
            }
        };
        previous = last;
        last = Some(
            work.entry_mark(&CommittedCheckpointUpdate {
                stream: baseline.stream,
                expected_previous: previous,
                entry: id,
            })
            .map_err(|_| ModelCodecError::Fields)?,
        );
        if matches(last, previous, watermark, &membership) {
            return Ok(true);
        }
    }
    Ok(false)
}
