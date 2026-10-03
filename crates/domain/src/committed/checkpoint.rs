use super::*;

const HEADER: [u8; 5] = *b"SWYC\x01";

// Borrowing the membership payload rejects malformed lengths without trusting
// them as allocation sizes. Only a checked, bounded payload is copied out.
#[derive(Serialize, Deserialize)]
struct CheckpointV1<'a> {
    stream: CommittedStreamId,
    last: Option<CommittedEntryMark>,
    previous: Option<CommittedEntryMark>,
    highest_timestamp: u64,
    #[serde(borrow)]
    membership: Option<MembershipV1<'a>>,
}

#[derive(Serialize, Deserialize)]
struct MembershipV1<'a> {
    source: CommittedEntryId,
    schema_version: u16,
    #[serde(borrow)]
    payload: &'a [u8],
}

pub(crate) fn encode_checkpoint(
    checkpoint: &CommittedCheckpoint,
) -> Result<Vec<u8>, CommittedApplyError> {
    validate_checkpoint(checkpoint)?;
    let wire = CheckpointV1 {
        stream: checkpoint.stream,
        last: checkpoint.last,
        previous: checkpoint.previous,
        highest_timestamp: checkpoint.highest_timestamp.as_millis(),
        membership: checkpoint
            .membership
            .as_ref()
            .map(|membership| MembershipV1 {
                source: membership.source,
                schema_version: membership.schema_version,
                payload: &membership.payload,
            }),
    };
    let payload = postcard::to_stdvec(&wire).map_err(|_| CommittedApplyError::CorruptCheckpoint)?;
    if payload.len() > MAX_COMMITTED_CHECKPOINT_BYTES - HEADER.len() {
        return Err(CommittedApplyError::CorruptCheckpoint);
    }
    let mut bytes = Vec::with_capacity(HEADER.len() + payload.len());
    bytes.extend_from_slice(&HEADER);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub(crate) fn decode_checkpoint(bytes: &[u8]) -> Result<CommittedCheckpoint, CommittedApplyError> {
    if bytes.len() > MAX_COMMITTED_CHECKPOINT_BYTES {
        return Err(CommittedApplyError::CorruptCheckpoint);
    }
    let payload = bytes
        .strip_prefix(&HEADER)
        .ok_or(CommittedApplyError::CorruptCheckpoint)?;
    let (wire, remaining): (CheckpointV1<'_>, _) =
        postcard::take_from_bytes(payload).map_err(|_| CommittedApplyError::CorruptCheckpoint)?;
    if !remaining.is_empty()
        || wire
            .membership
            .as_ref()
            .is_some_and(|membership| membership.payload.len() > MAX_COMMITTED_MEMBERSHIP_BYTES)
    {
        return Err(CommittedApplyError::CorruptCheckpoint);
    }
    let checkpoint = CommittedCheckpoint {
        stream: wire.stream,
        last: wire.last,
        previous: wire.previous,
        highest_timestamp: Timestamp::from_millis(wire.highest_timestamp),
        membership: wire.membership.map(|membership| CommittedMembership {
            source: membership.source,
            schema_version: membership.schema_version,
            payload: membership.payload.to_vec(),
        }),
    };
    validate_checkpoint(&checkpoint)?;
    if encode_checkpoint(&checkpoint)? != bytes {
        return Err(CommittedApplyError::CorruptCheckpoint);
    }
    Ok(checkpoint)
}

fn validate_checkpoint(checkpoint: &CommittedCheckpoint) -> Result<(), CommittedApplyError> {
    checkpoint
        .stream
        .validate()
        .map_err(|_| CommittedApplyError::CorruptCheckpoint)?;
    let previous_valid = match (checkpoint.last, checkpoint.previous) {
        (None, None) => {
            checkpoint.highest_timestamp == Timestamp::UNIX_EPOCH && checkpoint.membership.is_none()
        }
        (Some(last), None) => last.id.index == 0,
        (Some(last), Some(previous)) => {
            previous.id.index.checked_add(1) == Some(last.id.index)
                && previous.id.term <= last.id.term
        }
        (None, Some(_)) => false,
    };
    let membership_valid = checkpoint.membership.as_ref().is_none_or(|membership| {
        membership.schema_version != 0
            && membership.payload.len() <= MAX_COMMITTED_MEMBERSHIP_BYTES
            && checkpoint.last.is_some_and(|last| {
                membership.source.index <= last.id.index
                    && membership.source.term <= last.id.term
                    && (membership.source.index != last.id.index || membership.source == last.id)
                    && checkpoint.previous.is_none_or(|previous| {
                        (membership.source.index != previous.id.index
                            || membership.source == previous.id)
                            && (membership.source.index > previous.id.index
                                || membership.source.term <= previous.id.term)
                    })
            })
    });
    if !previous_valid || !membership_valid {
        return Err(CommittedApplyError::CorruptCheckpoint);
    }
    Ok(())
}
