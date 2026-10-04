use openraft::storage::RaftStateMachine;
use storage::{
    BoundedStateStore, CatalogCommittedStore, SnapshotCatalogReader, StateStore, WriteBatch,
};

use super::{
    TestResult, captured,
    fixture::{finish, seeded, source},
    observed::{Counts, observed},
    *,
};

pub(super) async fn legacy_owners_refuse_catalog_operations_without_source_io<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, captured::stream()?)?;
    control.reset();
    let result = async {
        assert_eq!(
            machine.build_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Disabled)
        );
        assert_eq!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Disabled)
        );
        assert_eq!(control.counts(), Counts::default());
        assert_eq!(machine.applied_state().await?.0, None);
        Ok(())
    }
    .await;
    finish(machine, result).await?;
    let mut machine =
        ExperimentalStateMachine::open_with_image_export(control.writer(), captured::stream()?)?;
    control.reset();
    let result = async {
        assert_eq!(
            machine.build_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Disabled)
        );
        assert_eq!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Disabled)
        );
        assert_eq!(control.counts(), Counts::default());
        machine.export_create_send_image().await?;
        assert_eq!(control.counts().bounded, 1);
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn build_moves_exact_capture_and_read_uses_only_retained_pair<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, true).await?;
    let result = async {
        let before = source(&control)?;
        assert_eq!(
            machine.export_create_send_image().await.err(),
            Some(crate::StateMachineImageExportError::Disabled)
        );
        assert_eq!(control.counts(), Counts::default());
        let built = machine.build_create_send_catalog().await?;
        assert_eq!(built.image_bytes(), before.image.as_bytes());
        assert_eq!(
            built.snapshot_meta().last_log_id.map(|id| id.index),
            Some(4)
        );
        assert_eq!(
            built.snapshot_meta().last_membership.membership(),
            &captured::members()
        );
        assert!(built.metadata_bytes().len() <= MAX_NATIVE_CATALOG_METADATA_OVERHEAD_BYTES);
        assert_eq!(
            control.committed_pointers(),
            vec![(
                built.metadata_bytes().as_ptr() as usize,
                built.image_bytes().as_ptr() as usize,
            )]
        );
        assert_eq!(
            control.counts(),
            Counts {
                bounded: 1,
                catalog_commits: 1,
                ..Counts::default()
            }
        );
        assert_eq!(control.catalog_batches(), vec![WriteBatch::default()]);
        assert_eq!(control.limits().len(), 1);
        assert_eq!(
            control.limits()[0].max_total_bytes,
            domain::MAX_COMMITTED_IMAGE_BYTES
        );
        assert_eq!(control.reader().snapshot()?, before.snapshot);
        let raw = control
            .catalog_reader()
            .read_catalog()?
            .ok_or("missing retained slot")?;
        assert_eq!(raw.artifact(), built.image_bytes());
        assert_eq!(raw.metadata(), built.metadata_bytes());
        assert_eq!(machine.workload()?.accepted_jobs, 0);
        assert_eq!(machine.workload()?.encoded_bytes, 0);
        control.reset();
        let retained = machine
            .read_create_send_catalog()
            .await?
            .ok_or("missing native retained pair")?;
        assert_eq!(retained.image_bytes(), built.image_bytes());
        assert_eq!(retained.metadata_bytes(), built.metadata_bytes());
        assert_eq!(retained.snapshot_meta(), built.snapshot_meta());
        assert_eq!(retained.checkpoint(), &before.checkpoint);
        assert_eq!(
            control.read_pointers(),
            vec![(
                retained.metadata_bytes().as_ptr() as usize,
                retained.image_bytes().as_ptr() as usize,
            )]
        );
        assert_eq!(
            control.counts(),
            Counts {
                catalog_factories: 1,
                catalog_reads: 1,
                ..Counts::default()
            }
        );
        assert_eq!(control.reader().snapshot()?, before.snapshot);
        for diagnostic in [format!("{built:?}"), format!("{retained:?}")] {
            for secret in ["PRIVATE", "tenant", "orders", "node-7", "swyi-v1-sha256:"] {
                assert!(!diagnostic.contains(secret));
            }
        }
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn older_catalog_stays_valid_after_current_progress_advances<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let result = async {
        let built = machine.build_create_send_catalog().await?;
        assert_eq!(
            machine.apply([captured::next_send()?]).await?,
            vec![crate::LogApplication::Sent { sequence: 3 }]
        );
        let current = source(&control)?;
        assert_eq!(current.checkpoint.last().map(|mark| mark.id.index), Some(5));
        control.reset();
        let retained = machine
            .read_create_send_catalog()
            .await?
            .ok_or("missing old retained pair")?;
        assert_eq!(retained.image_bytes(), built.image_bytes());
        assert_eq!(retained.metadata_bytes(), built.metadata_bytes());
        assert_eq!(
            retained.snapshot_meta().last_log_id.map(|id| id.index),
            Some(4)
        );
        assert_eq!(
            control.counts(),
            Counts {
                catalog_factories: 1,
                catalog_reads: 1,
                ..Counts::default()
            }
        );
        assert_eq!(control.reader().snapshot()?, current.snapshot);
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn opaque_mismatched_and_ahead_pairs_do_not_acquire_frontier_authority<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let result = async {
        control.reset();
        assert!(machine.read_create_send_catalog().await?.is_none());
        assert_eq!(
            control.counts(),
            Counts {
                catalog_factories: 1,
                catalog_reads: 1,
                ..Counts::default()
            }
        );
        let before = source(&control)?;
        let metadata = EncodedNativeSnapshotMetadata::encode(before.image.as_bytes())?;
        control.retain(
            b"PRIVATE-arbitrary-opaque-metadata",
            before.image.as_bytes(),
        )?;
        control.reset();
        assert_eq!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Metadata(
                NativeSnapshotMetadataError::InvalidMetadata
            ))
        );
        assert_eq!(
            control.counts(),
            Counts {
                catalog_factories: 1,
                catalog_reads: 1,
                ..Counts::default()
            }
        );

        let unsupported = captured::with_record(
            &before,
            &[0x7f],
            b"PRIVATE-unsupported-business-row".to_vec(),
        )?;
        control.retain(metadata.as_bytes(), unsupported.image.as_bytes())?;
        control.reset();
        assert_eq!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Domain(
                CommittedCatalogError::UnsupportedProfile
            ))
        );
        assert_eq!(
            control.counts(),
            Counts {
                catalog_factories: 1,
                catalog_reads: 1,
                ..Counts::default()
            }
        );
        control.retain(metadata.as_bytes(), b"PRIVATE-not-an-image")?;
        control.reset();
        assert_eq!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Domain(
                CommittedCatalogError::InvalidImage
            ))
        );
        assert_eq!(
            control.counts(),
            Counts {
                catalog_factories: 1,
                catalog_reads: 1,
                ..Counts::default()
            }
        );

        let ahead = captured::altered_checkpoint(&before, |wire| {
            wire.last.as_mut().unwrap().id.index = 5;
            wire.previous.as_mut().unwrap().id.index = 4;
        })?;
        captured::supported(&ahead)?;
        control.retain(metadata.as_bytes(), ahead.image.as_bytes())?;
        control.reset();
        assert_eq!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Metadata(
                NativeSnapshotMetadataError::ImageMismatch
            ))
        );
        assert_eq!(
            control.counts(),
            Counts {
                catalog_factories: 1,
                catalog_reads: 1,
                ..Counts::default()
            }
        );

        let ahead_metadata = EncodedNativeSnapshotMetadata::encode(ahead.image.as_bytes())?;
        control.retain(ahead_metadata.as_bytes(), ahead.image.as_bytes())?;
        control.reset();
        let retained = machine
            .read_create_send_catalog()
            .await?
            .ok_or("missing ahead pair")?;
        assert_eq!(
            retained.snapshot_meta().last_log_id.map(|id| id.index),
            Some(5)
        );
        assert_eq!(retained.checkpoint(), &ahead.checkpoint);
        assert_eq!(
            control.counts(),
            Counts {
                catalog_factories: 1,
                catalog_reads: 1,
                ..Counts::default()
            }
        );
        assert_eq!(control.reader().snapshot()?, before.snapshot);
        assert_eq!(machine.applied_state().await?.0.map(|id| id.index), Some(4));
        // Pair agreement deliberately remains a DTO, never an adoption operation.
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn native_incompatible_capture_is_refused_before_retention_without_poison<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let result = async {
        let before = source(&control)?;
        let incompatible = captured::altered_checkpoint(&before, |wire| {
            wire.previous.as_mut().unwrap().id.node_id = 9;
        })?;
        captured::supported(&incompatible)?;
        let checkpoint_bytes = incompatible
            .snapshot
            .entries()
            .iter()
            .find(|(key, _)| key == &[0x12])
            .ok_or("missing modified checkpoint")?
            .1
            .clone();
        control.inject(WriteBatch::default().put(vec![0x12], checkpoint_bytes))?;
        control.reset();
        assert_eq!(
            machine.build_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Metadata(
                NativeSnapshotMetadataError::IncompatibleCheckpoint
            ))
        );
        assert_eq!(
            control.counts(),
            Counts {
                bounded: 1,
                ..Counts::default()
            }
        );
        assert!(control.catalog_reader().read_catalog()?.is_none());

        let original = before
            .snapshot
            .entries()
            .iter()
            .find(|(key, _)| key == &[0x12])
            .ok_or("missing original checkpoint")?
            .1
            .clone();
        control.inject(WriteBatch::default().put(vec![0x12], original))?;
        control.reset();
        machine.build_create_send_catalog().await?;
        assert_eq!(
            control.counts(),
            Counts {
                bounded: 1,
                catalog_commits: 1,
                ..Counts::default()
            }
        );
        assert_eq!(control.reader().snapshot()?, before.snapshot);
        Ok(())
    }
    .await;
    finish(machine, result).await
}
