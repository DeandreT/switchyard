use sha2::{Digest, Sha256};

use domain::{CommittedCheckpoint, EntityPath, NamespaceName, TrustedCreateSendReplacement};
use storage::MemoryReplicaStore;

use super::*;

pub(super) struct Source {
    pub image: EncodedCommittedImage,
    pub checkpoint: CommittedCheckpoint,
    pub rows: StoreSnapshot,
}

#[derive(Clone, Copy)]
pub(super) enum Kind {
    Populated,
    Initial,
    RefusalOnly,
}

pub(super) fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(super) fn request<'a>(
    source: &'a Source,
    target_checkpoint: &'a CommittedCheckpoint,
) -> TrustedCreateSendReplacement<'a> {
    TrustedCreateSendReplacement::new(
        source.checkpoint.stream(),
        target_checkpoint,
        &source.checkpoint,
        digest(source.image.as_bytes()),
        source.image.as_bytes(),
    )
}

pub(super) fn source(kind: Kind) -> TestResult<Source> {
    let mut machine = CommittedStateMachine::create(MemoryReplicaStore::new(), stream()?)?;
    match kind {
        Kind::Initial => {}
        Kind::RefusalOnly => {
            applied(apply(
                &mut machine,
                0,
                &send(42, "refused", b"private-refused")?,
            )?)?;
            applied(apply(
                &mut machine,
                1,
                &CommittedQueueWork::Membership {
                    schema_version: 999,
                    payload: b"private-selected-opaque-membership".to_vec(),
                },
            )?)?;
        }
        Kind::Populated => {
            applied(apply(
                &mut machine,
                0,
                &CommittedQueueWork::Membership {
                    schema_version: 999,
                    payload: b"private-selected-opaque-membership".to_vec(),
                },
            )?)?;
            let config = QueueConfig {
                max_message_bytes: MAX_COMMITTED_BODY_BYTES,
                requires_session: true,
                requires_duplicate_detection: true,
                default_time_to_live_millis: Some(1000),
                duplicate_detection_history_time_window_millis: 20_000,
                ..QueueConfig::default()
            };
            applied(apply(&mut machine, 1, &create(20, config)?)?)?;
            let body = (0..MAX_COMMITTED_BODY_BYTES)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>();
            applied(apply(
                &mut machine,
                2,
                &super::super::maximum_send(21, Some(SessionId::new("private-session")?), &body)?,
            )?)?;
            applied(apply(
                &mut machine,
                3,
                &super::super::maximum_send(
                    22,
                    Some(SessionId::new("private-session")?),
                    b"duplicate hole",
                )?,
            )?)?;
            let (_, refused) = applied(apply(
                &mut machine,
                4,
                &super::super::maximum_send(2000, None, b"private-refused")?,
            )?)?;
            assert_eq!(
                refused,
                CommittedApplication::Refused(domain::BrokerError::SessionRequired)
            );
        }
    }
    let rows = machine.reader().snapshot()?;
    let image = machine.export_create_send_image()?;
    let checkpoint = machine.checkpoint()?;
    Ok(Source {
        image,
        checkpoint,
        rows,
    })
}

pub(super) fn populate_target<W: CommittedStore>(
    machine: &mut CommittedStateMachine<W>,
) -> TestResult {
    let old_config = QueueConfig {
        requires_duplicate_detection: true,
        default_time_to_live_millis: Some(1000),
        ..QueueConfig::default()
    };
    applied(apply(
        machine,
        0,
        &CommittedQueueWork::Membership {
            schema_version: 77,
            payload: b"private-old-opaque-membership".to_vec(),
        },
    )?)?;
    applied(apply(machine, 1, &create(10, old_config)?)?)?;
    applied(apply(
        machine,
        2,
        &send(11, "private-old", b"private-old-body")?,
    )?)?;
    let stale = EntityPath::new("stale-orders")?;
    let works = [
        CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
            NamespaceName::new("tenant")?,
            stale.clone(),
            Timestamp::from_millis(12),
            old_config,
        )),
        CommittedQueueWork::Queue(CommittedQueueCommand::send(
            NamespaceName::new("tenant")?,
            stale,
            Timestamp::from_millis(13),
            CommittedSend {
                message_id: "private-stale".into(),
                body: b"private-stale-body".to_vec(),
                time_to_live_millis: None,
                session_id: None,
            },
        )),
    ];
    for (offset, work) in works.iter().enumerate() {
        applied(apply(machine, offset as u64 + 3, work)?)?;
    }
    Ok(())
}

pub(super) fn changed_rows(
    source: &Source,
    mutations: WriteBatch,
) -> TestResult<EncodedCommittedImage> {
    let raw = MemoryStore::default();
    let mut batch = WriteBatch::default();
    for (key, value) in source.rows.entries() {
        batch.push_put(key.clone(), value.clone());
    }
    raw.apply(batch)?;
    raw.apply(mutations)?;
    Ok(EncodedCommittedImage::encode(
        CommittedImageRole::CreateSendLayout17V1,
        source.checkpoint.stream(),
        &raw.snapshot()?,
    )?)
}

pub(super) fn rehash(bytes: &mut [u8]) {
    let end = bytes.len() - 32;
    let checksum = digest(&bytes[..end]);
    bytes[end..].copy_from_slice(&checksum);
}
