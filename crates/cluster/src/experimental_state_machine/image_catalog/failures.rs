use openraft::storage::RaftStateMachine;
use storage::{BoundedStateStore, CatalogCommittedStore, SnapshotCatalogReader, StateStore};

use super::{
    TestResult, captured,
    fixture::{finish, seeded, source},
    observed::{Counts, Fault},
    *,
};

pub(super) async fn bounded_and_allocation_refusals_are_nonfatal_without_fallback<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let result = async {
        let before = source(&control)?;
        control.fault(Fault::CaptureLimit);
        assert_eq!(
            machine.build_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Domain(
                CommittedCatalogError::LimitExceeded
            ))
        );
        assert_eq!(
            control.counts(),
            Counts {
                bounded: 1,
                ..Counts::default()
            }
        );
        machine.build_create_send_catalog().await?;
        for (fault, error) in [
            (Fault::CatalogLimit, CommittedCatalogError::LimitExceeded),
            (Fault::CatalogAllocation, CommittedCatalogError::Allocation),
        ] {
            control.reset();
            control.fault(fault);
            assert_eq!(
                machine.read_create_send_catalog().await.err(),
                Some(StateMachineCatalogError::Domain(error))
            );
            assert_eq!(
                control.counts(),
                Counts {
                    catalog_factories: 1,
                    catalog_reads: 1,
                    ..Counts::default()
                }
            );
            assert!(machine.read_create_send_catalog().await?.is_some());
            assert_eq!(
                control.counts(),
                Counts {
                    catalog_factories: 2,
                    catalog_reads: 2,
                    ..Counts::default()
                }
            );
            assert_eq!(machine.workload()?.accepted_jobs, 0);
            assert_eq!(machine.workload()?.encoded_bytes, 0);
        }
        assert_eq!(control.reader().snapshot()?, before.snapshot);
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn physical_capture_and_catalog_reads_poison_before_any_later_io<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let before = match source(&control) {
        Ok(source) => source,
        Err(error) => {
            let _ = machine.shutdown().await;
            return Err(error);
        }
    };
    let result = async {
        control.fault(Fault::CapturePhysical);
        assert_eq!(
            machine.build_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Domain(
                CommittedCatalogError::ReadFailed
            ))
        );
        assert_eq!(
            control.counts(),
            Counts {
                bounded: 1,
                ..Counts::default()
            }
        );
        let counts = control.counts();
        assert_eq!(
            machine.build_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Owner(StateMachineError::Poisoned))
        );
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
        control.fault(Fault::CatalogPhysical);
        assert_eq!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Domain(
                CommittedCatalogError::ReadFailed
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
        let counts = control.counts();
        assert_eq!(
            machine.build_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Owner(StateMachineError::Poisoned))
        );
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
    let result = async {
        machine.build_create_send_catalog().await?;
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn retention_error_is_unknown_before_or_after_actual_catalog_commit<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let before = match source(&control) {
        Ok(source) => source,
        Err(error) => {
            let _ = machine.shutdown().await;
            return Err(error);
        }
    };
    let result = async {
        machine.build_create_send_catalog().await?;
        assert_eq!(
            machine.apply([captured::next_send()?]).await?,
            vec![crate::LogApplication::Sent { sequence: 3 }]
        );
        control.reset();
        control.fault(Fault::CommitBefore);
        assert_eq!(
            machine.build_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Domain(
                CommittedCatalogError::CommitUnknown
            ))
        );
        assert_eq!(
            control.counts(),
            Counts {
                bounded: 1,
                catalog_commits: 1,
                ..Counts::default()
            }
        );
        let counts = control.counts();
        assert_eq!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Owner(StateMachineError::Poisoned))
        );
        assert_eq!(control.counts(), counts);
        let slot = control
            .catalog_reader()
            .read_catalog()?
            .ok_or("missing prior slot")?;
        assert_eq!(slot.artifact(), before.image.as_bytes());
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
        control.fault(Fault::CommitAfter);
        assert_eq!(
            machine.build_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Domain(
                CommittedCatalogError::CommitUnknown
            ))
        );
        assert_eq!(
            control.counts(),
            Counts {
                bounded: 1,
                catalog_commits: 1,
                ..Counts::default()
            }
        );
        let counts = control.counts();
        assert_eq!(
            machine.build_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Owner(StateMachineError::Poisoned))
        );
        assert_eq!(control.counts(), counts);
        let slot = control
            .catalog_reader()
            .read_catalog()?
            .ok_or("missing complete after-error slot")?;
        assert_eq!(slot.artifact(), current.image.as_bytes());
        assert_eq!(control.reader().snapshot()?, current.snapshot);
        Ok(())
    }
    .await;
    finish(machine, result).await?;

    // Reopen observes the actual decision; neither unknown result is retried.
    let mut machine = ExperimentalStateMachine::open_with_snapshot_catalog(
        control.writer(),
        captured::stream()?,
    )?;
    let result = async {
        let slot = machine
            .read_create_send_catalog()
            .await?
            .ok_or("missing recovered slot")?;
        assert_eq!(slot.image_bytes(), current.image.as_bytes());
        assert_eq!(control.reader().snapshot()?, current.snapshot);
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn capture_and_catalog_backend_panics_refund_and_really_join<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let setup: TestResult = async {
        machine.build_create_send_catalog().await?;
        Ok(())
    }
    .await;
    if let Err(error) = setup {
        let _ = machine.shutdown().await;
        return Err(error);
    }
    let before = match source(&control) {
        Ok(source) => source,
        Err(error) => {
            let _ = machine.shutdown().await;
            return Err(error);
        }
    };
    control.reset();
    control.fault(Fault::CapturePanic);
    let result: TestResult = async {
        let error = machine.build_create_send_catalog().await.err();
        assert_eq!(
            error,
            Some(StateMachineCatalogError::Owner(StateMachineError::Panicked))
        );
        assert!(!format!("{error:?}").contains("PRIVATE"));
        assert_eq!(machine.workload()?.accepted_jobs, 0);
        assert_eq!(machine.workload()?.encoded_bytes, 0);
        let counts = control.counts();
        assert!(matches!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Owner(
                StateMachineError::Closed | StateMachineError::Panicked
            ))
        ));
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
    control.reset();
    control.fault(Fault::CatalogPanic);
    let result: TestResult = async {
        assert_eq!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Owner(StateMachineError::Panicked))
        );
        assert_eq!(machine.workload()?.accepted_jobs, 0);
        assert_eq!(machine.workload()?.encoded_bytes, 0);
        let counts = control.counts();
        assert!(matches!(
            machine.build_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Owner(
                StateMachineError::Closed | StateMachineError::Panicked
            ))
        ));
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
        let catalog = machine
            .read_create_send_catalog()
            .await?
            .ok_or("missing healthy retained pair")?;
        assert_eq!(catalog.image_bytes(), before.image.as_bytes());
        Ok(())
    }
    .await;
    finish(machine, result).await
}
