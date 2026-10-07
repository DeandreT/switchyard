use domain::{CommittedImageRole, EncodedCommittedImage};
use openraft::Snapshot;
use storage::{SnapshotCatalogReader, StateStore, StoreSnapshot};

use super::*;
use observed::{Control, Counts};

pub(super) struct Source {
    pub(super) carrier: Snapshot<crate::LogTypes>,
    pub(super) checkpoint: CommittedCheckpoint,
    pub(super) rows: StoreSnapshot,
    pub(super) digest: [u8; 32],
    pub(super) pointer: usize,
}

pub(super) fn source(maximum: bool) -> TestResult<Source> {
    from_selected(captured::selected(maximum)?)
}

pub(super) fn from_selected(selected: captured::Selected) -> TestResult<Source> {
    let metadata = EncodedNativeSnapshotMetadata::encode(selected.image.as_bytes())?;
    let meta = DecodedNativeSnapshotPair::decode(metadata.as_bytes(), selected.image.as_bytes())?
        .snapshot_meta()?;
    let pointer = selected.image.as_bytes().as_ptr() as usize;
    let digest = captured::digest(selected.image.as_bytes());
    let data = crate::BoundedSnapshotData::from_image(selected.image)?;
    assert_eq!(data.as_bytes().as_ptr() as usize, pointer);
    Ok(Source {
        carrier: Snapshot {
            meta,
            snapshot: Box::new(data),
        },
        checkpoint: selected.checkpoint,
        rows: selected.snapshot,
        digest,
        pointer,
    })
}

pub(super) fn request(
    target: CommittedCheckpoint,
    source: Source,
) -> TestResult<OwnedTrustedNativeReplacement> {
    Ok(OwnedTrustedNativeReplacement::new(
        captured::stream()?,
        target,
        source.checkpoint,
        source.digest,
        source.carrier,
    )?)
}

pub(super) fn prepared(
    target: CommittedCheckpoint,
    source: Source,
) -> TestResult<(
    OwnedTrustedNativeReplacement,
    StoreSnapshot,
    CommittedCheckpoint,
    usize,
)> {
    let Source {
        carrier,
        checkpoint,
        rows,
        digest,
        pointer,
    } = source;
    let request = OwnedTrustedNativeReplacement::new(
        captured::stream()?,
        target,
        checkpoint.clone(),
        digest,
        carrier,
    )?;
    Ok((request, rows, checkpoint, pointer))
}

pub(super) async fn target<W>(
    writer: W,
) -> TestResult<(ExperimentalStateMachine, Control<W>, CommittedCheckpoint)>
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed::observed(writer);
    let mut machine =
        ExperimentalStateMachine::create_with_snapshot_replacement(writer, captured::stream()?)?;
    let seeded = catalog_fixture::seed(&mut machine, false).await;
    if let Err(error) = seeded {
        let _ = machine.shutdown().await;
        return Err(error);
    }
    let checkpoint = match machine.checkpoint().await {
        Ok(checkpoint) => checkpoint,
        Err(error) => {
            let _ = machine.shutdown().await;
            return Err(error.into());
        }
    };
    control.reset();
    Ok((machine, control, checkpoint))
}

pub(super) fn assert_counts<W: CatalogCommittedStore>(
    control: &Control<W>,
    bounded: usize,
    commits: usize,
) {
    assert_eq!(
        control.counts(),
        Counts {
            bounded,
            catalog_commits: commits,
            ..Counts::default()
        }
    );
}

pub(super) fn exact_target<W: CatalogCommittedStore>(
    control: &Control<W>,
    rows: &StoreSnapshot,
    checkpoint: &CommittedCheckpoint,
) -> TestResult {
    let actual = control.reader().snapshot()?;
    assert_eq!(actual.entries(), rows.entries());
    let retained = control
        .catalog_reader()
        .read_catalog()?
        .ok_or("catalog absent")?;
    let image = EncodedCommittedImage::encode(
        CommittedImageRole::CreateSendLayout17V1,
        checkpoint.stream(),
        rows,
    )?;
    assert_eq!(retained.artifact(), image.as_bytes());
    let pair = DecodedNativeSnapshotPair::decode(retained.metadata(), retained.artifact())?;
    assert_eq!(pair.checkpoint(), checkpoint);
    Ok(())
}

pub(super) async fn finish(machine: ExperimentalStateMachine, result: TestResult) -> TestResult {
    catalog_fixture::finish(machine, result).await
}
