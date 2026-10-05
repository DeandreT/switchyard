use std::io::{ErrorKind, SeekFrom};

use openraft::storage::RaftStateMachine;
use storage::{
    BoundedStateStore, CatalogCommittedStore, SnapshotCatalogReader, StateStore, WriteBatch,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::{
    TestResult, captured,
    fixture::{finish, seeded, source},
    observed::{Counts, observed},
    *,
};

fn owned<T: Send + 'static>(_: &T) {}

pub(super) async fn factory_and_default_disabled_build_are_inert<W>(writer: W) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, captured::stream()?)?;
    control.reset();
    let mut builder = machine.create_send_snapshot_builder();
    owned(&builder);
    let result = async {
        assert_eq!(control.counts(), Counts::default());
        assert_eq!(machine.workload()?.accepted_jobs, 0);
        assert_eq!(machine.workload()?.encoded_bytes, 0);
        assert_eq!(format!("{builder:?}"), "CreateSendSnapshotBuilder { .. }");
        assert_eq!(
            builder
                .build_snapshot()
                .await
                .err()
                .map(|error| error.to_string()),
            Some(build_error().to_string())
        );
        assert_eq!(control.counts(), Counts::default());
        assert_eq!(machine.workload()?.accepted_jobs, 0);
        assert_eq!(machine.workload()?.encoded_bytes, 0);
        Ok(())
    }
    .await;
    finish(machine, result).await?;

    let mut machine =
        ExperimentalStateMachine::open_with_image_export(control.writer(), captured::stream()?)?;
    control.reset();
    let mut builder = machine.create_send_snapshot_builder();
    let result = async {
        assert!(builder.build_snapshot().await.is_err());
        assert_eq!(control.counts(), Counts::default());
        machine.export_create_send_image().await?;
        assert_eq!(control.counts().bounded, 1);
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn trait_build_moves_exact_whole_frame_and_projection_into_sealed_transport<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, true).await?;
    let mut builder = machine.create_send_snapshot_builder();
    let result = async {
        let before = source(&control)?;
        let mut snapshot = builder.build_snapshot().await?;
        assert_eq!(snapshot.snapshot.position(), 0);
        assert_eq!(snapshot.snapshot.as_bytes(), before.image.as_bytes());
        assert_eq!(snapshot.meta.last_log_id.map(|id| id.index), Some(4));
        assert_eq!(
            snapshot.meta.last_membership.membership(),
            &captured::members()
        );
        let pointers = control.committed_pointers();
        assert_eq!(pointers.len(), 1);
        assert_eq!(
            pointers[0].1,
            snapshot.snapshot.as_bytes().as_ptr() as usize
        );
        assert_eq!(control.catalog_batches(), vec![WriteBatch::default()]);
        assert_eq!(
            control.counts(),
            Counts {
                bounded: 1,
                catalog_commits: 1,
                ..Counts::default()
            }
        );
        assert_eq!(control.reader().snapshot()?, before.snapshot);
        let retained = control
            .catalog_reader()
            .read_catalog()?
            .ok_or("missing retained pair")?;
        assert_eq!(retained.artifact(), snapshot.snapshot.as_bytes());
        let pair = DecodedNativeSnapshotPair::decode(retained.metadata(), retained.artifact())?;
        assert_eq!(pair.checkpoint(), &before.checkpoint);
        assert_eq!(pair.snapshot_meta()?, snapshot.meta);

        let original = snapshot.snapshot.as_bytes().to_vec();
        let mut received = Vec::new();
        snapshot.snapshot.read_to_end(&mut received).await?;
        assert_eq!(received, original);
        assert_eq!(
            snapshot.snapshot.seek(SeekFrom::End(0)).await?,
            original.len() as u64
        );
        snapshot.snapshot.seek(SeekFrom::Start(0)).await?;
        assert_eq!(
            snapshot
                .snapshot
                .write(b"PRIVATE-alteration")
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::PermissionDenied
        );
        assert_eq!(
            snapshot.snapshot.write(&[]).await.unwrap_err().kind(),
            ErrorKind::PermissionDenied
        );
        assert_eq!(snapshot.snapshot.as_bytes(), original);
        assert_eq!(snapshot.snapshot.position(), 0);
        snapshot.snapshot.flush().await?;
        snapshot.snapshot.shutdown().await?;
        let mut after_shutdown = [0u8; 5];
        snapshot.snapshot.read_exact(&mut after_shutdown).await?;
        assert_eq!(&after_shutdown, &original[..5]);
        for diagnostic in [format!("{builder:?}"), format!("{:?}", snapshot.snapshot)] {
            for secret in ["PRIVATE", "tenant", "orders", "node-7", "swyi-v1-sha256:"] {
                assert!(!diagnostic.contains(secret));
            }
        }
        assert_eq!(machine.workload()?.accepted_jobs, 0);
        assert_eq!(machine.workload()?.encoded_bytes, 0);
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn standalone_success_does_not_enable_any_engine_snapshot_method<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let result = async {
        let mut standalone = machine.create_send_snapshot_builder();
        let snapshot = standalone.build_snapshot().await?;
        control.reset();
        let mut engine_builder = machine.get_snapshot_builder().await;
        assert!(engine_builder.build_snapshot().await.is_err());
        assert!(machine.begin_receiving_snapshot().await.is_err());
        assert!(
            machine
                .install_snapshot(&snapshot.meta, snapshot.snapshot)
                .await
                .is_err()
        );
        assert_eq!(control.counts(), Counts::default());
        assert!(machine.get_current_snapshot().await?.is_none());
        assert_eq!(control.counts().bounded, 0);
        assert_eq!(control.counts().catalog_reads, 0);
        assert_eq!(control.counts().catalog_commits, 0);
        Ok(())
    }
    .await;
    finish(machine, result).await
}
