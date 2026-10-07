//! Reference bytes from existing domain/Memory APIs: TEST SETUP ONLY, not
//! paired-role creation, independent committed history or physical evidence.

use std::collections::{BTreeMap, BTreeSet};

use domain::{
    CommittedCheckpointUpdate, CommittedEntryId, CommittedImageRole, CommittedQueueCommand,
    CommittedQueueWork, CommittedSend, CommittedStateMachine, CommittedStreamId,
    EncodedCommittedImage, EntityPath, NamespaceName, QueueConfig, Timestamp,
};
use openraft::Membership;
use storage::{CommittedStore, MemoryReplicaStore, StateStore, StoreSnapshot};

use super::*;

pub(super) type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

pub(super) struct Image {
    pub(super) artifact: Vec<u8>,
    pub(super) metadata: Vec<u8>,
    pub(super) checkpoint: CommittedCheckpoint,
    pub(super) native: SnapshotMeta<u64, BasicNode>,
    snapshot: StoreSnapshot,
}

impl Image {
    pub(super) fn populated() -> TestResult<Self> {
        Self::variant([7; 16], 10, 11, 7)
    }

    pub(super) fn variant(
        stream: [u8; 16],
        create_at: u64,
        send_at: u64,
        member: u64,
    ) -> TestResult<Self> {
        Self::variant_prefix(stream, create_at, send_at, member, 3)
    }

    pub(super) fn before_send() -> TestResult<Self> {
        Self::variant_prefix([7; 16], 10, 11, 7, 2)
    }

    fn variant_prefix(
        stream: [u8; 16],
        create_at: u64,
        send_at: u64,
        member: u64,
        steps: usize,
    ) -> TestResult<Self> {
        let stream = CommittedStreamId::new(stream)?;
        let writer = MemoryReplicaStore::new();
        let reader = writer.reader();
        let mut machine = CommittedStateMachine::create(writer, stream)?;
        let membership = Membership::new(
            vec![BTreeSet::from([member, 8, 9])],
            BTreeMap::from([
                (member, BasicNode::new("PRIVATE-node")),
                (8, BasicNode::new("PRIVATE-node-8")),
                (9, BasicNode::new("PRIVATE-node-9")),
            ]),
        );
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        let work = [
            CommittedQueueWork::Membership {
                schema_version: 1,
                payload: crate::experimental_log::encode_membership(&membership)?,
            },
            CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
                namespace.clone(),
                entity.clone(),
                Timestamp::from_millis(create_at),
                QueueConfig::default(),
            )),
            CommittedQueueWork::Queue(CommittedQueueCommand::send(
                namespace,
                entity,
                Timestamp::from_millis(send_at),
                CommittedSend {
                    message_id: "PRIVATE-id".into(),
                    body: b"PRIVATE-body".to_vec(),
                    time_to_live_millis: None,
                    session_id: None,
                },
            )),
        ];
        for (index, work) in work.iter().take(steps).enumerate() {
            machine.apply_committed(
                &CommittedCheckpointUpdate {
                    stream,
                    expected_previous: machine.checkpoint()?.last(),
                    entry: if index == 0 {
                        CommittedEntryId {
                            term: 0,
                            node_id: 0,
                            index: 0,
                        }
                    } else {
                        CommittedEntryId {
                            term: 1,
                            node_id: 7,
                            index: index as u64,
                        }
                    },
                },
                work,
            )?;
        }
        Self::from_snapshot(stream, reader.snapshot()?)
    }

    pub(super) fn initial(noninitial_blank: bool) -> TestResult<Self> {
        let stream = CommittedStreamId::new([7; 16])?;
        let writer = MemoryReplicaStore::new();
        let reader = writer.reader();
        let mut machine = CommittedStateMachine::create(writer, stream)?;
        if noninitial_blank {
            machine.apply_committed(
                &CommittedCheckpointUpdate {
                    stream,
                    expected_previous: None,
                    entry: CommittedEntryId {
                        term: 0,
                        node_id: 0,
                        index: 0,
                    },
                },
                &CommittedQueueWork::Blank,
            )?;
        }
        Self::from_snapshot(stream, reader.snapshot()?)
    }

    fn from_snapshot(stream: CommittedStreamId, snapshot: StoreSnapshot) -> TestResult<Self> {
        let artifact = EncodedCommittedImage::encode(
            CommittedImageRole::CreateSendLayout17V1,
            stream,
            &snapshot,
        )?;
        let metadata = crate::EncodedNativeSnapshotMetadata::encode(artifact.as_bytes())?;
        let pair = DecodedNativeSnapshotPair::decode(metadata.as_bytes(), artifact.as_bytes())?;
        Ok(Self {
            checkpoint: pair.checkpoint().clone(),
            native: pair.snapshot_meta()?,
            artifact: artifact.as_bytes().to_vec(),
            metadata: metadata.as_bytes().to_vec(),
            snapshot,
        })
    }

    pub(super) fn changed_body(&self) -> TestResult<Self> {
        let key = domain::keys::message(
            &NamespaceName::new("tenant")?,
            &EntityPath::new("orders")?,
            domain::SequenceNumber::new(1),
        );
        let original = &self
            .snapshot
            .entries()
            .iter()
            .find(|(candidate, _)| candidate == &key)
            .ok_or("message missing")?
            .1;
        let mut message = domain::MessageRecord::decode(original)?;
        *message.body.first_mut().ok_or("body missing")? ^= 1;
        let store = storage::MemoryStore::default();
        let mut batch = storage::WriteBatch::default();
        for (key, value) in self.snapshot.entries() {
            batch.push_put(key.clone(), value.clone());
        }
        batch.push_put(key, domain::codec::encode(&message)?);
        store.apply(batch)?;
        Self::from_snapshot(self.checkpoint.stream(), store.snapshot()?)
    }

    pub(super) fn after_blank(&self) -> TestResult<Self> {
        let writer = MemoryReplicaStore::new();
        let reader = writer.reader();
        let mut machine = CommittedStateMachine::bootstrap_create_send_image(
            writer,
            domain::TrustedCreateSendBootstrap::new(
                self.checkpoint.stream(),
                &self.checkpoint,
                Sha256::digest(&self.artifact).into(),
                &self.artifact,
            ),
        )?;
        let previous = self.checkpoint.last().ok_or("last missing")?;
        machine.apply_committed(
            &CommittedCheckpointUpdate {
                stream: self.checkpoint.stream(),
                expected_previous: Some(previous),
                entry: CommittedEntryId {
                    index: previous.id.index.checked_add(1).ok_or("index exhausted")?,
                    ..previous.id
                },
            },
            &CommittedQueueWork::Blank,
        )?;
        Self::from_snapshot(self.checkpoint.stream(), reader.snapshot()?)
    }
}

pub(super) struct Case {
    pub(super) image: Image,
    pub(super) profile: LogProfile,
    pub(super) controls: [Vec<u8>; 3],
    pub(super) vote: LogVote,
}

impl Case {
    pub(super) fn new() -> TestResult<Self> {
        Self::from_image(Image::populated()?)
    }

    pub(super) fn from_image(image: Image) -> TestResult<Self> {
        let profile = LogProfile::new(7, image.checkpoint.stream())?;
        let vote = LogVote::new_committed(2, 7);
        let baseline = if image.checkpoint.membership().is_some() {
            Baseline::make(&profile, 1, &image.metadata)?
        } else {
            Baseline::empty(&profile)?
        };
        let progress = crate::experimental_log::types::LogProgress {
            vote: Some(vote),
            last_purged: baseline.through(),
            ..Default::default()
        };
        let controls = [
            super::super::super::codec::encode_profile(&profile)?,
            crate::experimental_log::codec::encode_progress(&progress)?,
            baseline.bytes,
        ];
        Ok(Self {
            image,
            profile,
            controls,
            vote,
        })
    }

    pub(super) fn rows(&self) -> [(&[u8], &[u8]); 3] {
        [
            (&[1], &self.controls[0]),
            (&[2], &self.controls[1]),
            (&[3], &self.controls[2]),
        ]
    }

    pub(super) fn expectation(&self) -> AlignedSeedExpectation<'_> {
        AlignedSeedExpectation {
            profile: &self.profile,
            checkpoint: &self.image.checkpoint,
            artifact_sha256: Sha256::digest(&self.image.artifact).into(),
            artifact_bytes: self.image.artifact.len(),
            native: &self.image.native,
            vote: self.vote,
            baseline_ordinal: 1,
        }
    }

    pub(super) fn run(&self, expected: &AlignedSeedExpectation<'_>) -> Result<()> {
        let rows = self.rows();
        inspect_aligned_seed(
            BorrowedAlignedSeed {
                metadata: &self.image.metadata,
                artifact: &self.image.artifact,
                log_rows: &rows,
            },
            expected,
        )
        .map(|_| ())
    }

    pub(super) fn progress(&self) -> TestResult<crate::experimental_log::types::LogProgress> {
        Ok(crate::experimental_log::codec::decode_progress(
            &self.controls[1],
        )?)
    }

    pub(super) fn set_progress(
        &mut self,
        progress: &crate::experimental_log::types::LogProgress,
    ) -> TestResult {
        self.controls[1] = crate::experimental_log::codec::encode_progress(progress)?;
        Ok(())
    }
}
