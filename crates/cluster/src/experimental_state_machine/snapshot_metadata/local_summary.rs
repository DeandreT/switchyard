use domain::{
    CommittedCheckpoint, CommittedEntryId, CommittedEntryMark, CommittedMembership,
    CommittedStreamId, Timestamp,
};

use super::{NativeSnapshotMetadataError as Error, codec};
use crate::experimental_state_machine::{AppliedState, state::recover_fields};

/// Internal field summary only: no image validity, writer, or purge authority.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct NativeCheckpointSummary {
    pub(crate) stream: CommittedStreamId,
    pub(crate) last: Option<CommittedEntryMark>,
    pub(crate) previous: Option<CommittedEntryMark>,
    pub(crate) highest_timestamp: Timestamp,
    pub(crate) membership: Option<CommittedMembership>,
    pub(crate) artifact_bytes: u64,
    pub(crate) digest: [u8; 32],
}

impl NativeCheckpointSummary {
    #[cfg(test)]
    pub(crate) fn encode_for_test(&self) -> Result<Vec<u8>, Error> {
        let id = |id: CommittedEntryId| codec::IdV1 {
            term: id.term,
            node_id: id.node_id,
            index: id.index,
        };
        let mark = |mark: CommittedEntryMark| codec::MarkV1 {
            id: id(mark.id),
            fingerprint: mark.fingerprint,
        };
        codec::encode(&codec::MetadataV1 {
            stream: *self.stream.as_bytes(),
            artifact_bytes: self.artifact_bytes,
            digest: self.digest,
            last: self.last.map(mark),
            previous: self.previous.map(mark),
            highest_timestamp: self.highest_timestamp.as_millis(),
            membership: self
                .membership
                .as_ref()
                .map(|membership| codec::MembershipV1 {
                    source: id(membership.source),
                    schema_version: membership.schema_version,
                    payload: &membership.payload,
                }),
        })
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let wire = codec::decode(bytes)?;
        let stream =
            CommittedStreamId::new(wire.stream).map_err(|_| Error::IncompatibleCheckpoint)?;
        let membership = match wire.membership {
            None => None,
            Some(value) => {
                let mut payload = Vec::new();
                payload
                    .try_reserve_exact(value.payload.len())
                    .map_err(|_| Error::Allocation)?;
                payload.extend_from_slice(value.payload);
                Some(CommittedMembership {
                    source: id(value.source),
                    schema_version: value.schema_version,
                    payload,
                })
            }
        };
        let summary = Self {
            stream,
            last: wire.last.map(mark),
            previous: wire.previous.map(mark),
            highest_timestamp: Timestamp::from_millis(wire.highest_timestamp),
            membership,
            artifact_bytes: wire.artifact_bytes,
            digest: wire.digest,
        };
        summary.recover()?;
        if summary.last.is_none()
            && (summary.highest_timestamp != Timestamp::UNIX_EPOCH || summary.membership.is_some())
        {
            return Err(Error::IncompatibleCheckpoint);
        }
        Ok(summary)
    }

    pub(crate) fn recover(&self) -> Result<AppliedState, Error> {
        recover_fields(self.last, self.previous, self.membership.as_ref())
            .map_err(|_| Error::IncompatibleCheckpoint)
    }

    pub(crate) fn matches(&self, checkpoint: &CommittedCheckpoint) -> bool {
        self.stream == checkpoint.stream()
            && self.last == checkpoint.last()
            && self.previous == checkpoint.previous()
            && self.highest_timestamp == checkpoint.highest_timestamp()
            && self.membership.as_ref() == checkpoint.membership()
    }

    pub(crate) fn same_checkpoint(&self, other: &Self) -> bool {
        self.stream == other.stream
            && self.last == other.last
            && self.previous == other.previous
            && self.highest_timestamp == other.highest_timestamp
            && self.membership == other.membership
    }
}

impl std::fmt::Debug for NativeCheckpointSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeCheckpointSummary")
            .finish_non_exhaustive()
    }
}

fn id(value: codec::IdV1) -> CommittedEntryId {
    CommittedEntryId {
        term: value.term,
        node_id: value.node_id,
        index: value.index,
    }
}
fn mark(value: codec::MarkV1) -> CommittedEntryMark {
    CommittedEntryMark {
        id: id(value.id),
        fingerprint: value.fingerprint,
    }
}

#[cfg(test)]
mod tests;
