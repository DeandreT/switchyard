//! Existing Memory/domain construction creates reference bytes ONLY. It is not
//! paired-role storage, creation, adoption, retirement or physical crash evidence.

use std::collections::{BTreeMap, BTreeSet};

use domain::{
    CommittedCheckpoint, CommittedCheckpointUpdate, CommittedEntryId, CommittedImageRole,
    CommittedQueueCommand, CommittedQueueWork, CommittedSend, CommittedStateMachine,
    CommittedStreamId, EncodedCommittedImage, EntityPath, NamespaceName, QueueConfig, Timestamp,
};
use openraft::{BasicNode, EntryPayload, Membership};
use storage::{CommittedStore, MemoryReplicaStore, StateStore, StoreSnapshot};

use super::*;

#[derive(Clone)]
pub(super) struct Image {
    pub(super) artifact: Vec<u8>,
    pub(super) metadata: Vec<u8>,
    pub(super) checkpoint: Vec<u8>,
    pub(super) native: Vec<u8>,
    pub(super) domain_checkpoint: CommittedCheckpoint,
    snapshot: StoreSnapshot,
}

impl Image {
    pub(super) fn identity(&self) -> ImageIdentity<'_> {
        ImageIdentity {
            checkpoint: Blob(&self.checkpoint),
            digest: digest(&self.artifact),
            bytes: self.artifact.len() as u64,
        }
    }

    pub(super) fn initial() -> TestResult<Self> {
        let writer = MemoryReplicaStore::new();
        let reader = writer.reader();
        let _machine = CommittedStateMachine::create(writer, CommittedStreamId::new([7; 16])?)?;
        Self::from_snapshot(reader.snapshot()?)
    }

    pub(super) fn populated() -> TestResult<Self> {
        let writer = MemoryReplicaStore::new();
        let reader = writer.reader();
        let stream = CommittedStreamId::new([7; 16])?;
        let mut machine = CommittedStateMachine::create(writer, stream)?;
        let membership = Membership::new(
            vec![BTreeSet::from([7, 8, 9])],
            BTreeMap::from([
                (7, BasicNode::new("PRIVATE-node-7")),
                (8, BasicNode::new("PRIVATE-node-8")),
                (9, BasicNode::new("PRIVATE-node-9")),
            ]),
        );
        let namespace = NamespaceName::new("tenant")?;
        let entity = EntityPath::new("orders")?;
        let work = [
            CommittedQueueWork::Membership {
                schema_version: 1,
                payload: super::super::super::encode_membership(&membership)?,
            },
            CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
                namespace.clone(),
                entity.clone(),
                Timestamp::from_millis(10),
                QueueConfig::default(),
            )),
            CommittedQueueWork::Queue(CommittedQueueCommand::send(
                namespace,
                entity,
                Timestamp::from_millis(11),
                CommittedSend {
                    message_id: "PRIVATE-id".into(),
                    body: b"PRIVATE-body".to_vec(),
                    time_to_live_millis: None,
                    session_id: None,
                },
            )),
        ];
        for (index, work) in work.iter().enumerate() {
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
        Self::from_snapshot(reader.snapshot()?)
    }

    pub(super) fn from_snapshot(snapshot: StoreSnapshot) -> TestResult<Self> {
        let artifact = EncodedCommittedImage::encode(
            CommittedImageRole::CreateSendLayout17V1,
            CommittedStreamId::new([7; 16])?,
            &snapshot,
        )?;
        let metadata = crate::EncodedNativeSnapshotMetadata::encode(artifact.as_bytes())?;
        let pair =
            crate::DecodedNativeSnapshotPair::decode(metadata.as_bytes(), artifact.as_bytes())?;
        let domain_checkpoint = pair.checkpoint().clone();
        let checkpoint = frame::encode_payload(
            &CheckpointFields::from_checkpoint(&domain_checkpoint),
            MAX_CHECKPOINT,
        )?;
        let native = identity::native_factory(&pair.snapshot_meta()?)?;
        Ok(Self {
            artifact: artifact.as_bytes().to_vec(),
            metadata: metadata.as_bytes().to_vec(),
            checkpoint,
            native,
            domain_checkpoint,
            snapshot,
        })
    }

    pub(super) fn changed_body(&self) -> TestResult<Self> {
        let key = domain::keys::message(
            &NamespaceName::new("tenant")?,
            &EntityPath::new("orders")?,
            domain::SequenceNumber::new(1),
        );
        let mut record = domain::MessageRecord::decode(
            &self
                .snapshot
                .entries()
                .iter()
                .find(|(candidate, _)| candidate == &key)
                .ok_or("message missing")?
                .1,
        )?;
        *record.body.first_mut().ok_or("body missing")? ^= 1;
        let store = storage::MemoryStore::default();
        let mut batch = storage::WriteBatch::default();
        for (key, value) in self.snapshot.entries() {
            batch.push_put(key.clone(), value.clone());
        }
        batch.push_put(key, domain::codec::encode(&record)?);
        store.apply(batch)?;
        Self::from_snapshot(store.snapshot()?)
    }

    pub(super) fn after_blank(&self) -> TestResult<Self> {
        let writer = MemoryReplicaStore::new();
        let reader = writer.reader();
        let mut machine = CommittedStateMachine::bootstrap_create_send_image(
            writer,
            domain::TrustedCreateSendBootstrap::new(
                self.domain_checkpoint.stream(),
                &self.domain_checkpoint,
                digest(&self.artifact),
                &self.artifact,
            ),
        )?;
        let previous = self
            .domain_checkpoint
            .last()
            .ok_or("noninitial fixture required")?;
        machine.apply_committed(
            &CommittedCheckpointUpdate {
                stream: self.domain_checkpoint.stream(),
                expected_previous: Some(previous),
                entry: CommittedEntryId {
                    term: previous.id.term,
                    node_id: previous.id.node_id,
                    index: previous.id.index.checked_add(1).ok_or("index exhausted")?,
                },
            },
            &CommittedQueueWork::Blank,
        )?;
        Self::from_snapshot(reader.snapshot()?)
    }

    pub(super) fn with_leader_term(&self, term: u64) -> TestResult<Self> {
        let mut fields = CheckpointFields::from_checkpoint(&self.domain_checkpoint);
        fields.last.as_mut().ok_or("last missing")?.id.term = term;
        fields.previous.as_mut().ok_or("previous missing")?.id.term = term;
        fields.check()?;
        // Test-owned frozen SWYC1 construction only, not a public domain API.
        let mut value = b"SWYC\x01".to_vec();
        value.extend_from_slice(&frame::encode_payload(&fields, MAX_CHECKPOINT)?);
        let store = storage::MemoryStore::default();
        let mut batch = storage::WriteBatch::default();
        for (key, value) in self.snapshot.entries() {
            batch.push_put(key.clone(), value.clone());
        }
        batch.push_put([0x12], value);
        store.apply(batch)?;
        Self::from_snapshot(store.snapshot()?)
    }

    pub(super) fn without_membership(&self) -> TestResult<Self> {
        let mut fields = CheckpointFields::from_checkpoint(&self.domain_checkpoint);
        fields.membership = None;
        fields.check()?;
        // Test-owned frozen checkpoint only; noninitial no-member stays report-only.
        let mut value = b"SWYC\x01".to_vec();
        value.extend_from_slice(&frame::encode_payload(&fields, MAX_CHECKPOINT)?);
        let store = storage::MemoryStore::default();
        let mut batch = storage::WriteBatch::default();
        for (key, value) in self.snapshot.entries() {
            batch.push_put(key.clone(), value.clone());
        }
        batch.push_put([0x12], value);
        store.apply(batch)?;
        Self::from_snapshot(store.snapshot()?)
    }
}

pub(super) struct Case {
    pub(super) binding: Binding,
    pub(super) old: Image,
    pub(super) selected: Image,
    pub(super) catalog: Option<Image>,
    pub(super) seed_rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub(super) final_rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub(super) vote: Option<Vote>,
    pub(super) final_vote: Option<Vote>,
    pub(super) tail: TailChoice,
    pub(super) seed: [u8; 32],
    pub(super) header: Vec<u8>,
    pub(super) old_fence: Vec<u8>,
    pub(super) old_progress: Vec<u8>,
    pub(super) old_baseline: Vec<u8>,
    pub(super) final_progress: Vec<u8>,
    pub(super) final_baseline: Vec<u8>,
    pub(super) state_recipe: Vec<u8>,
    pub(super) log_recipe: Vec<u8>,
    pub(super) intent: Vec<u8>,
}

impl Case {
    pub(super) fn new() -> TestResult<Self> {
        let old = Image::populated()?;
        let selected = old.changed_body()?;
        let vote = Some(Vote {
            term: 2,
            node: 7,
            committed: true,
        });
        let mut case = Self {
            binding: Binding {
                pair: [1; 16],
                state: [2; 16],
                log: [3; 16],
                node: 7,
                stream: [7; 16],
            },
            catalog: Some(old.clone()),
            old,
            selected,
            seed_rows: Vec::new(),
            final_rows: Vec::new(),
            vote,
            final_vote: vote,
            tail: TailChoice::ExactResetEmpty,
            seed: [0; 32],
            header: Vec::new(),
            old_fence: Vec::new(),
            old_progress: Vec::new(),
            old_baseline: Vec::new(),
            final_progress: Vec::new(),
            final_baseline: Vec::new(),
            state_recipe: Vec::new(),
            log_recipe: Vec::new(),
            intent: Vec::new(),
        };
        case.rebuild()?;
        Ok(case)
    }

    pub(super) fn view(&self) -> Result<ModelIntent<'_>> {
        Ok(ModelIntent {
            binding: self.binding,
            serial: 1,
            seed: self.seed,
            old: self.old.identity(),
            old_catalog: self.catalog.as_ref().map(|image| CatalogIdentity {
                image: image.identity(),
                metadata: Blob(&image.metadata),
            }),
            selected: self.selected.identity(),
            selected_metadata: Blob(&self.selected.metadata),
            selected_native: Blob(&self.selected.native),
            old_header: Blob(&self.header),
            old_fence: Some(Blob(&self.old_fence)),
            old_progress: Blob(&self.old_progress),
            old_baseline: Blob(&self.old_baseline),
            old_entries: entries::manifest(&rows(&self.seed_rows))?,
            old_phase: OldJournalPhase::SeedReady,
            tail: self.tail,
            final_progress: Blob(&self.final_progress),
            final_baseline: Blob(&self.final_baseline),
            final_entries: entries::manifest(&rows(&self.final_rows))?,
            selected_recipe: Blob(&self.state_recipe),
            final_recipe: Blob(&self.log_recipe),
        })
    }

    pub(super) fn rebuild(&mut self) -> TestResult {
        let old_base = self.catalog.as_ref().unwrap_or(&self.old);
        self.old_baseline = frame::encode(
            Kind::Baseline,
            Role::Log,
            &Baseline {
                binding: self.binding,
                ordinal: 0,
                metadata: Blob(&old_base.metadata),
            },
            MAX_BASELINE,
        )?;
        self.final_baseline = frame::encode(
            Kind::Baseline,
            Role::Log,
            &Baseline {
                binding: self.binding,
                ordinal: 1,
                metadata: Blob(&self.selected.metadata),
            },
            MAX_BASELINE,
        )?;
        self.old_progress = encoded_progress(
            self.vote,
            old_base.domain_checkpoint.last().map(|mark| mark.id),
            &self.seed_rows,
        )?;
        self.final_progress = encoded_progress(
            self.final_vote,
            self.selected.domain_checkpoint.last().map(|mark| mark.id),
            &self.final_rows,
        )?;
        self.seed = identity::seed_manifest(&self.view()?)?;
        self.header = frame::encode(
            Kind::Header,
            Role::Log,
            &Header {
                binding: self.binding,
                seed: self.seed,
                limits: 1,
            },
            MAX_CONTROL,
        )?;
        self.old_fence = frame::encode(
            Kind::StateFence,
            Role::State,
            &StateFence {
                binding: self.binding,
                phase: Phase::Ready,
                serial: 0,
                seed: self.seed,
                intent: None,
                selection: identity::old_selection(&self.view()?)?,
            },
            MAX_CONTROL,
        )?;
        self.state_recipe = frame::encode_payload(
            &StateRecipe {
                binding: self.binding,
                phase: Phase::Selected,
                serial: 1,
                seed: self.seed,
                selection: identity::selected_selection(&self.view()?)?,
            },
            MAX_CONTROL,
        )?;
        self.log_recipe = frame::encode_payload(
            &LogRecipe {
                binding: self.binding,
                serial: 1,
                controls: identity::final_controls(&self.view()?)?,
                entries: self.view()?.final_entries.digest,
            },
            MAX_CONTROL,
        )?;
        self.intent = encode_intent(&self.view()?)?;
        Ok(())
    }

    pub(super) fn state(&self, selected: bool, stage: bool) -> TestResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut output = vec![(
            vec![0x20, 1],
            frame::encode(
                Kind::Header,
                Role::State,
                &header(Role::Log, &self.header)?,
                MAX_CONTROL,
            )?,
        )];
        let fence = if selected {
            frame::encode(
                Kind::StateFence,
                Role::State,
                &state_recipe(&self.state_recipe)?.bind(digest(&self.intent)),
                MAX_CONTROL,
            )?
        } else {
            self.old_fence.clone()
        };
        output.push((vec![0x20, 2], fence));
        if stage {
            output.push((
                vec![0x20, 3],
                frame::encode(
                    Kind::StageBinding,
                    Role::State,
                    &StageBinding {
                        binding: self.binding,
                        serial: 1,
                        intent: digest(&self.intent),
                        selection: identity::selected_selection(&self.view()?)?,
                    },
                    MAX_CONTROL,
                )?,
            ));
            output.push((vec![0x20, 4], self.selected.metadata.clone()));
            output.push((vec![0x20, 5], self.selected.artifact.clone()));
        }
        let catalog = if selected {
            Some(&self.selected)
        } else {
            self.catalog.as_ref()
        };
        if let Some(image) = catalog {
            output.push((vec![0x20, 6], image.metadata.clone()));
            output.push((vec![0x20, 7], image.artifact.clone()));
        }
        Ok(output)
    }

    pub(super) fn log(&self, finalized: bool) -> TestResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut output = vec![
            (vec![0x20, 1], self.header.clone()),
            (
                vec![0x20, 2],
                if finalized {
                    self.final_progress.clone()
                } else {
                    self.old_progress.clone()
                },
            ),
            (
                vec![0x20, 3],
                if finalized {
                    self.final_baseline.clone()
                } else {
                    self.old_baseline.clone()
                },
            ),
            (vec![0x20, 4], self.intent.clone()),
        ];
        if finalized {
            output.push((
                vec![0x20, 5],
                frame::encode(
                    Kind::LogFence,
                    Role::Log,
                    &log_recipe(&self.log_recipe)?.bind(digest(&self.intent)),
                    MAX_CONTROL,
                )?,
            ));
        }
        output.extend(
            if finalized {
                &self.final_rows
            } else {
                &self.seed_rows
            }
            .iter()
            .cloned(),
        );
        Ok(output)
    }

    pub(super) fn classify(
        &self,
        selected: bool,
        finalized: bool,
        stage: bool,
    ) -> TestResult<ModelClassification> {
        let state = self.state(selected, stage)?;
        let log = self.log(finalized)?;
        self.inspect(
            if selected {
                &self.selected.artifact
            } else {
                &self.old.artifact
            },
            &state,
            &log,
        )
    }

    pub(super) fn inspect(
        &self,
        business: &[u8],
        state: &[(Vec<u8>, Vec<u8>)],
        log: &[(Vec<u8>, Vec<u8>)],
    ) -> TestResult<ModelClassification> {
        Ok(classify::classify(
            &self.intent,
            BorrowedPairObservation {
                business,
                state: &rows(state),
                log: &rows(log),
            },
            TrustedModelInputs {
                seed: self.seed,
                selected: self.selected.identity(),
                native: Blob(&self.selected.native),
                tail: self.tail,
                final_entries: entries::manifest(&rows(&self.final_rows))?,
                seed_rows: &rows(&self.seed_rows),
            },
        )?)
    }
}

pub(super) fn rows(values: &[(Vec<u8>, Vec<u8>)]) -> Vec<Row<'_>> {
    values
        .iter()
        .map(|(key, value)| Row { key, value })
        .collect()
}

pub(super) fn blank(index: u64) -> TestResult<(Vec<u8>, Vec<u8>)> {
    let encoded = super::super::super::codec::encode_entry(&crate::LogEntry {
        log_id: crate::LogId::new(openraft::CommittedLeaderId::new(1, 7), index),
        payload: EntryPayload::Blank,
    })?;
    Ok((entries::key(index).to_vec(), encoded.bytes().to_vec()))
}

fn encoded_progress(
    vote: Option<Vote>,
    purged: Option<CommittedEntryId>,
    values: &[(Vec<u8>, Vec<u8>)],
) -> TestResult<Vec<u8>> {
    let manifest = entries::manifest(&rows(values))?;
    let present = values
        .last()
        .map(|(_, value)| {
            super::super::super::codec::decode_entry(value).map(|entry| committed_id(entry.log_id))
        })
        .transpose()?;
    Ok(frame::encode(
        Kind::Progress,
        Role::Log,
        &Progress {
            vote,
            purged,
            present,
            entries: manifest.count,
            bytes: manifest.bytes,
        },
        MAX_CONTROL,
    )?)
}
