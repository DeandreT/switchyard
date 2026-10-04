use openraft::storage::RaftStateMachine;
use storage::{
    CatalogCommittedStore, CommittedStore, FjallCatalogReplicaStore, SnapshotCatalogReader,
    StateStore,
};

use super::{
    TestResult, captured, finish,
    observed::{Fault, bootstrap_counts, observed},
    refused, *,
};

#[tokio::test]
async fn joined_bootstrap_releases_all_physical_handles_before_fjall_pair_reopen() -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let source = captured::selected(true)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    {
        let (writer, control) = observed(FjallCatalogReplicaStore::open(directory.path())?);
        let mut machine = ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
            writer,
            source.request(),
            metadata.as_bytes(),
        )?;
        let result = async {
            assert_eq!(control.counts(), bootstrap_counts());
            assert_eq!(control.reader().snapshot()?, source.snapshot);
            assert_eq!(machine.applied_state().await?.0.map(|id| id.index), Some(4));
            Ok(())
        }
        .await;
        finish(machine, result).await?;
        drop(control);
    }
    {
        let writer = FjallCatalogReplicaStore::open(directory.path())?;
        let reader = writer.reader();
        assert_eq!(reader.snapshot()?, source.snapshot);
        let raw = writer
            .catalog_reader()
            .read_catalog()?
            .ok_or("missing reopened catalog")?;
        assert_eq!(raw.artifact(), source.image.as_bytes());
        assert_eq!(raw.metadata(), metadata.as_bytes());
        let mut machine =
            ExperimentalStateMachine::open_with_snapshot_catalog(writer, captured::stream()?)?;
        let result = async {
            let pair = machine
                .read_create_send_catalog()
                .await?
                .ok_or("missing native reopened pair")?;
            assert_eq!(pair.image_bytes(), source.image.as_bytes());
            assert_eq!(pair.metadata_bytes(), metadata.as_bytes());
            assert_eq!(pair.snapshot_meta().last_log_id.map(|id| id.index), Some(4));
            assert_eq!(
                machine.apply([captured::next_send()?]).await?,
                vec![crate::LogApplication::Sent { sequence: 3 }]
            );
            let old = machine
                .read_create_send_catalog()
                .await?
                .ok_or("ordinary apply removed catalog")?;
            assert_eq!(old.image_bytes(), source.image.as_bytes());
            assert_eq!(old.snapshot_meta().last_log_id.map(|id| id.index), Some(4));
            Ok(())
        }
        .await;
        finish(machine, result).await?;
        drop(reader);
    }
    // The former owner and every old source-bearing capability were released.
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    let raw = writer
        .catalog_reader()
        .read_catalog()?
        .ok_or("missing final durable catalog")?;
    assert_eq!(raw.artifact(), source.image.as_bytes());
    assert_eq!(raw.metadata(), metadata.as_bytes());
    let mut machine = ExperimentalStateMachine::open(writer, captured::stream()?)?;
    let result = async {
        assert_eq!(machine.applied_state().await?.0.map(|id| id.index), Some(5));
        assert_eq!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Disabled)
        );
        Ok(())
    }
    .await;
    finish(machine, result).await
}

async fn unknown_reopen(after: bool) -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    {
        let (writer, control) = observed(FjallCatalogReplicaStore::open(directory.path())?);
        control.fault(if after {
            Fault::CommitAfter
        } else {
            Fault::CommitBefore
        });
        let mut started = false;
        let result = super::super::bootstrap_with_starter(
            writer,
            source.request(),
            metadata.as_bytes(),
            |state| {
                started = true;
                drop(state);
                Err(StateMachineError::ThreadStart)
            },
        );
        assert_eq!(
            refused(result).await?,
            StateMachineCatalogBootstrapError::Domain(
                domain::CommittedImageBootstrapError::CommitUnknown
            )
        );
        assert!(!started);
        assert_eq!(control.counts(), bootstrap_counts());
        drop(control);
    }
    {
        let writer = FjallCatalogReplicaStore::open(directory.path())?;
        let reader = writer.reader();
        let raw = writer.catalog_reader().read_catalog()?;
        assert_eq!(writer.is_initialized()?, after);
        if after {
            assert_eq!(reader.snapshot()?, source.snapshot);
            let raw = raw.ok_or("missing complete after-error catalog")?;
            assert_eq!(raw.artifact(), source.image.as_bytes());
            assert_eq!(raw.metadata(), metadata.as_bytes());
            let mut machine =
                ExperimentalStateMachine::open_with_snapshot_catalog(writer, captured::stream()?)?;
            let result = async {
                let pair = machine
                    .read_create_send_catalog()
                    .await?
                    .ok_or("missing recovered native pair")?;
                assert_eq!(pair.image_bytes(), source.image.as_bytes());
                assert_eq!(pair.metadata_bytes(), metadata.as_bytes());
                assert_eq!(machine.applied_state().await?.0.map(|id| id.index), Some(4));
                Ok(())
            }
            .await;
            finish(machine, result).await?;
        } else {
            assert!(raw.is_none());
            assert!(reader.snapshot()?.entries().is_empty());
            match ExperimentalStateMachine::open(writer, captured::stream()?) {
                Err(error) => assert_eq!(error, StateMachineError::InvalidState),
                Ok(machine) => {
                    machine.shutdown().await?;
                    return Err("unexpected pristine open succeeded (owner joined)".into());
                }
            }
        }
        drop(reader);
    }
    // Recovery inspects the decision; no bootstrap retry follows either error.
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    assert_eq!(writer.is_initialized()?, after);
    if after {
        assert_eq!(writer.reader().snapshot()?, source.snapshot);
        let raw = writer
            .catalog_reader()
            .read_catalog()?
            .ok_or("missing final complete slot")?;
        assert_eq!(raw.artifact(), source.image.as_bytes());
        assert_eq!(raw.metadata(), metadata.as_bytes());
    } else {
        assert!(writer.reader().snapshot()?.entries().is_empty());
        assert!(writer.catalog_reader().read_catalog()?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn unknown_before_actual_combined_commit_reopens_fully_pristine_without_retry() -> TestResult
{
    unknown_reopen(false).await
}

#[tokio::test]
async fn unknown_after_actual_combined_commit_reopens_complete_without_retry() -> TestResult {
    unknown_reopen(true).await
}

#[tokio::test]
async fn simulated_known_commit_startup_refusal_releases_state_before_physical_reopen() -> TestResult
{
    let directory = testkit::DurableProvider::temporary()?;
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    {
        let (writer, control) = observed(FjallCatalogReplicaStore::open(directory.path())?);
        let mut started = false;
        let result = super::super::bootstrap_with_starter(
            writer,
            source.request(),
            metadata.as_bytes(),
            |state| {
                started = true;
                assert_eq!(control.counts(), bootstrap_counts());
                assert!(state.image_catalog.is_none());
                assert!(state.image_export.is_none());
                drop(state);
                Err(StateMachineError::ThreadStart)
            },
        );
        assert_eq!(
            refused(result).await?,
            StateMachineCatalogBootstrapError::OwnerStartAfterCommit
        );
        assert!(started);
        assert_eq!(control.counts(), bootstrap_counts());
        drop(control);
    }
    // The fake starter deliberately refused and spawned no OS thread. All
    // physical handles were nevertheless consumed/released before this open.
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    let reader = writer.reader();
    assert!(writer.is_initialized()?);
    assert_eq!(reader.snapshot()?, source.snapshot);
    let raw = writer
        .catalog_reader()
        .read_catalog()?
        .ok_or("missing known-committed catalog")?;
    assert_eq!(raw.artifact(), source.image.as_bytes());
    assert_eq!(raw.metadata(), metadata.as_bytes());
    let mut machine =
        ExperimentalStateMachine::open_with_snapshot_catalog(writer, captured::stream()?)?;
    let result = async {
        let pair = machine
            .read_create_send_catalog()
            .await?
            .ok_or("missing recovered pair")?;
        assert_eq!(pair.image_bytes(), source.image.as_bytes());
        assert_eq!(pair.metadata_bytes(), metadata.as_bytes());
        Ok(())
    }
    .await;
    finish(machine, result).await?;
    drop(reader);
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    assert_eq!(writer.reader().snapshot()?, source.snapshot);
    Ok(())
}
