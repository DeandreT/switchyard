use openraft::storage::RaftStateMachine;
use storage::{
    BoundedStateStore, CatalogCommittedStore, SnapshotCatalogReader, StateStore, WriteBatch,
};

use super::{
    TestResult, captured,
    fixture::{finish, seeded, source},
    observed::{Counts, Fault},
    *,
};

fn expected_counts() -> Counts {
    Counts {
        bounded: 1,
        catalog_commits: 1,
        ..Counts::default()
    }
}

pub(super) async fn bounded_refusal_is_nonfatal_and_does_not_fallback<W>(writer: W) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let mut builder = machine.create_send_snapshot_builder();
    let result = async {
        let before = source(&control)?;
        control.fault(Fault::CaptureLimit);
        assert_eq!(
            builder
                .build_snapshot()
                .await
                .err()
                .map(|error| error.to_string()),
            Some(build_error().to_string())
        );
        assert_eq!(
            control.counts(),
            Counts {
                bounded: 1,
                ..Counts::default()
            }
        );
        assert!(control.catalog_reader().read_catalog()?.is_none());
        assert_eq!(control.reader().snapshot()?, before.snapshot);
        assert_eq!(machine.workload()?.accepted_jobs, 0);
        assert_eq!(machine.workload()?.encoded_bytes, 0);
        control.reset();
        let built = builder.build_snapshot().await?;
        assert_eq!(built.snapshot.as_bytes(), before.image.as_bytes());
        assert_eq!(control.counts(), expected_counts());
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn physical_capture_poison_prevents_later_source_io<W>(writer: W) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let mut builder = machine.create_send_snapshot_builder();
    let result = async {
        let before = source(&control)?;
        control.fault(Fault::CapturePhysical);
        assert_eq!(
            builder
                .build_snapshot()
                .await
                .err()
                .map(|error| error.to_string()),
            Some(build_error().to_string())
        );
        let counts = Counts {
            bounded: 1,
            ..Counts::default()
        };
        assert_eq!(control.counts(), counts);
        assert!(builder.build_snapshot().await.is_err());
        assert_eq!(control.counts(), counts);
        assert_eq!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Owner(StateMachineError::Poisoned))
        );
        assert_eq!(control.counts(), counts);
        assert_eq!(control.reader().snapshot()?, before.snapshot);
        Ok(())
    }
    .await;
    finish(machine, result).await?;
    let mut machine = ExperimentalStateMachine::open_with_snapshot_catalog(
        control.writer(),
        captured::stream()?,
    )?;
    control.reset();
    let result = async {
        machine
            .create_send_snapshot_builder()
            .build_snapshot()
            .await?;
        assert_eq!(control.counts(), expected_counts());
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn native_incompatible_capture_is_nonfatal_without_retention<W>(
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
            wire.membership.as_mut().unwrap().schema_version = 2;
        })?;
        captured::supported(&incompatible)?;
        let row = incompatible
            .snapshot
            .entries()
            .iter()
            .find(|(key, _)| key.as_slice() == [0x12])
            .ok_or("missing altered checkpoint")?;
        control.inject(WriteBatch::default().put(row.0.clone(), row.1.clone()))?;
        control.reset();
        let mut builder = machine.create_send_snapshot_builder();
        assert!(builder.build_snapshot().await.is_err());
        assert_eq!(
            control.counts(),
            Counts {
                bounded: 1,
                ..Counts::default()
            }
        );
        assert!(control.catalog_reader().read_catalog()?.is_none());
        assert_eq!(control.reader().snapshot()?, incompatible.snapshot);
        let row = before
            .snapshot
            .entries()
            .iter()
            .find(|(key, _)| key.as_slice() == [0x12])
            .ok_or("missing original checkpoint")?;
        control.inject(WriteBatch::default().put(row.0.clone(), row.1.clone()))?;
        control.reset();
        assert_eq!(
            builder.build_snapshot().await?.snapshot.as_bytes(),
            before.image.as_bytes()
        );
        assert_eq!(control.counts(), expected_counts());
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn retention_errors_stay_unknown_before_and_after_actual_commit<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let setup = async {
        let old = source(&control)?;
        machine
            .create_send_snapshot_builder()
            .build_snapshot()
            .await?;
        assert_eq!(
            machine.apply([captured::next_send()?]).await?,
            vec![crate::LogApplication::Sent { sequence: 3 }]
        );
        Ok::<_, Box<dyn std::error::Error>>(old)
    }
    .await;
    let old = match setup {
        Ok(old) => old,
        Err(error) => {
            let _ = machine.shutdown().await;
            return Err(error);
        }
    };
    control.reset();
    let result = async {
        let mut builder = machine.create_send_snapshot_builder();
        control.fault(Fault::CommitBefore);
        assert!(builder.build_snapshot().await.is_err());
        assert_eq!(control.counts(), expected_counts());
        assert!(builder.build_snapshot().await.is_err());
        assert_eq!(control.counts(), expected_counts());
        let slot = control
            .catalog_reader()
            .read_catalog()?
            .ok_or("missing prior catalog")?;
        assert_eq!(slot.artifact(), old.image.as_bytes());
        Ok(())
    }
    .await;
    finish(machine, result).await?;

    let current = source(&control)?;
    let mut machine = ExperimentalStateMachine::open_with_snapshot_catalog(
        control.writer(),
        captured::stream()?,
    )?;
    control.reset();
    let result = async {
        let mut builder = machine.create_send_snapshot_builder();
        control.fault(Fault::CommitAfter);
        assert_eq!(
            builder
                .build_snapshot()
                .await
                .err()
                .map(|error| error.to_string()),
            Some(build_error().to_string())
        );
        assert_eq!(control.counts(), expected_counts());
        assert!(builder.build_snapshot().await.is_err());
        assert_eq!(control.counts(), expected_counts());
        let slot = control
            .catalog_reader()
            .read_catalog()?
            .ok_or("missing durable unknown result")?;
        assert_eq!(slot.artifact(), current.image.as_bytes());
        DecodedNativeSnapshotPair::decode(slot.metadata(), slot.artifact())?;
        assert_eq!(control.reader().snapshot()?, current.snapshot);
        Ok(())
    }
    .await;
    finish(machine, result).await?;

    // Only reopening recovers actual durability; neither unknown request is retried.
    let mut machine = ExperimentalStateMachine::open_with_snapshot_catalog(
        control.writer(),
        captured::stream()?,
    )?;
    let result = async {
        let retained = machine
            .read_create_send_catalog()
            .await?
            .ok_or("missing recovered pair")?;
        assert_eq!(retained.image_bytes(), current.image.as_bytes());
        assert_eq!(control.reader().snapshot()?, current.snapshot);
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn capture_panic_refunds_and_really_joins_the_owner<W>(writer: W) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let handle = machine.handle.clone();
    let mut builder = machine.create_send_snapshot_builder();
    let result: TestResult = async {
        let before = source(&control)?;
        control.fault(Fault::CapturePanic);
        assert_eq!(
            builder
                .build_snapshot()
                .await
                .err()
                .map(|error| error.to_string()),
            Some(build_error().to_string())
        );
        assert_eq!(handle.workload()?.accepted_jobs, 0);
        assert_eq!(handle.workload()?.encoded_bytes, 0);
        let counts = Counts {
            bounded: 1,
            ..Counts::default()
        };
        assert_eq!(control.counts(), counts);
        // The catch/close race may produce Closed or Panicked internally; the
        // public trait refusal is static and neither path touches the source.
        assert!(builder.build_snapshot().await.is_err());
        assert_eq!(control.counts(), counts);
        assert_eq!(control.reader().snapshot()?, before.snapshot);
        Ok(())
    }
    .await;
    let joined = machine.shutdown().await;
    result?;
    assert_eq!(joined, Err(StateMachineError::Panicked));
    let mut machine = ExperimentalStateMachine::open_with_snapshot_catalog(
        control.writer(),
        captured::stream()?,
    )?;
    let result = async {
        machine
            .create_send_snapshot_builder()
            .build_snapshot()
            .await?;
        Ok(())
    }
    .await;
    finish(machine, result).await
}
