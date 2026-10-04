use domain::{CommittedImageBootstrapError as DomainError, TrustedCreateSendBootstrap};
use openraft::storage::RaftStateMachine;
use storage::{
    BoundedStateStore, CatalogCommittedStore, Mutation, SnapshotCatalogReader, StateStore,
    WriteBatch,
};

use super::{
    TestResult, captured, finish,
    observed::{Counts, Fault, WithoutBoundedReader, bootstrap_counts, observed},
    refused, *,
};

fn exact_calls<W: CatalogCommittedStore>(
    control: &super::observed::Control<W>,
    source: &captured::Selected,
    metadata: &[u8],
) -> TestResult {
    assert_eq!(control.counts(), bootstrap_counts());
    let calls = control.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].metadata, metadata);
    assert_eq!(calls[0].metadata_ptr, metadata.as_ptr() as usize);
    assert_eq!(
        calls[0].artifact_ptr,
        source.image.as_bytes().as_ptr() as usize
    );
    assert_eq!(
        calls[0].artifact_digest,
        captured::digest(source.image.as_bytes())
    );
    let puts = calls[0]
        .batch
        .mutations()
        .iter()
        .map(|mutation| match mutation {
            Mutation::Put { key, value } => Ok((key.clone(), value.clone())),
            Mutation::Delete { .. } => Err("bootstrap attempted a delete"),
        })
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(puts, source.snapshot.entries());
    assert!(control.initialized()?);
    assert_eq!(control.reader().snapshot()?, source.snapshot);
    let slot = control
        .catalog_reader()
        .read_catalog()?
        .ok_or("missing combined catalog")?;
    assert_eq!(slot.metadata(), metadata);
    assert_eq!(slot.artifact(), source.image.as_bytes());
    Ok(())
}

pub(super) async fn plain_bootstrap_combines_exact_rows_and_pair_without_postcommit_reads<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
{
    let source = captured::selected(true)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
        writer,
        source.request(),
        metadata.as_bytes(),
    )?;
    let result = async {
        exact_calls(&control, &source, metadata.as_bytes())?;
        control.reset();
        assert_eq!(
            machine.build_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Disabled)
        );
        assert_eq!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Disabled)
        );
        assert_eq!(
            machine.export_create_send_image().await.err(),
            Some(StateMachineImageExportError::Disabled)
        );
        assert_eq!(control.counts(), Counts::default());
        assert_eq!(machine.applied_state().await?.0.map(|id| id.index), Some(4));
        assert_eq!(
            machine.apply([captured::next_send()?]).await?,
            vec![crate::LogApplication::Sent { sequence: 3 }]
        );
        let slot = control
            .catalog_reader()
            .read_catalog()?
            .ok_or("ordinary apply erased catalog")?;
        assert_eq!(slot.artifact(), source.image.as_bytes());
        assert_eq!(slot.metadata(), metadata.as_bytes());
        assert!(machine.get_current_snapshot().await?.is_none());
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn plain_bootstrap_accepts_a_reader_without_bounded_capability<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
{
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let (writer, control) = observed(WithoutBoundedReader(writer));
    let mut machine = ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
        writer,
        source.request(),
        metadata.as_bytes(),
    )?;
    let result = async {
        exact_calls(&control, &source, metadata.as_bytes())?;
        control.reset();
        assert_eq!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Disabled)
        );
        assert_eq!(
            machine.build_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Disabled)
        );
        assert_eq!(control.counts(), Counts::default());
        assert_eq!(machine.applied_state().await?.0.map(|id| id.index), Some(4));
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn initial_checkpoint_catalog_bootstrap_has_no_hidden_capture<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
{
    let source = captured::initial()?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
        writer,
        source.request(),
        metadata.as_bytes(),
    )?;
    let result = async {
        exact_calls(&control, &source, metadata.as_bytes())?;
        control.reset();
        assert_eq!(machine.applied_state().await?.0, None);
        assert_eq!(
            machine.read_create_send_catalog().await.err(),
            Some(StateMachineCatalogError::Disabled)
        );
        assert_eq!(control.counts().bounded, 0);
        assert_eq!(control.counts().catalog_reads, 0);
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn explicit_catalog_operations_read_original_pair_and_capture_only_when_requested<
    W,
>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let (writer, control) = observed(writer);
    let mut machine =
        ExperimentalStateMachine::bootstrap_create_send_image_with_catalog_operations(
            writer,
            source.request(),
            metadata.as_bytes(),
        )?;
    let result = async {
        exact_calls(&control, &source, metadata.as_bytes())?;
        control.reset();
        let retained = machine
            .read_create_send_catalog()
            .await?
            .ok_or("missing native retained catalog")?;
        assert_eq!(retained.image_bytes(), source.image.as_bytes());
        assert_eq!(retained.metadata_bytes(), metadata.as_bytes());
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
        assert_eq!(
            machine.export_create_send_image().await.err(),
            Some(StateMachineImageExportError::Disabled)
        );
        assert_eq!(
            machine.apply([captured::next_send()?]).await?,
            vec![crate::LogApplication::Sent { sequence: 3 }]
        );
        control.reset();
        let old = machine
            .read_create_send_catalog()
            .await?
            .ok_or("missing older pair")?;
        assert_eq!(old.snapshot_meta().last_log_id.map(|id| id.index), Some(4));
        assert_eq!(
            control.counts(),
            Counts {
                catalog_factories: 1,
                catalog_reads: 1,
                ..Counts::default()
            }
        );
        control.reset();
        let new = machine.build_create_send_catalog().await?;
        assert_eq!(new.snapshot_meta().last_log_id.map(|id| id.index), Some(5));
        assert_eq!(
            control.counts(),
            Counts {
                bounded: 1,
                catalog_commits: 1,
                ..Counts::default()
            }
        );
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn valid_pair_does_not_override_exact_trusted_selection<W>(writer: W) -> TestResult
where
    W: CatalogCommittedStore,
{
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let (writer, control) = observed(writer);
    let changed = captured::altered_checkpoint(&source, |wire| {
        wire.previous.as_mut().unwrap().id.node_id = 9;
    })?;
    // Actual pair is healthy, while the selected expectation is native-incompatible.
    // Exact domain selection, not expected-CP-only recovery, must classify this.
    let request = TrustedCreateSendBootstrap::new(
        captured::stream()?,
        &changed.checkpoint,
        captured::digest(source.image.as_bytes()),
        source.image.as_bytes(),
    );
    assert_eq!(
        refused(
            ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
                writer,
                request,
                metadata.as_bytes(),
            )
        )
        .await?,
        StateMachineCatalogBootstrapError::Domain(DomainError::SelectionMismatch)
    );
    assert_eq!(control.counts(), Counts::default());
    assert!(!control.initialized()?);
    assert!(control.reader().snapshot()?.entries().is_empty());
    assert!(control.catalog_reader().read_catalog()?.is_none());
    Ok(())
}

pub(super) async fn initialized_target_is_never_replaced<W>(writer: W) -> TestResult
where
    W: CatalogCommittedStore,
{
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let (writer, control) = observed(writer);
    control.inject(WriteBatch::default().put(vec![0x7f], b"PRIVATE-existing-target".to_vec()))?;
    let before = control.reader().snapshot()?;
    assert_eq!(
        refused(
            ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
                writer,
                source.request(),
                metadata.as_bytes(),
            )
        )
        .await?,
        StateMachineCatalogBootstrapError::Domain(DomainError::TargetNotPristine)
    );
    assert_eq!(
        control.counts(),
        Counts {
            factories: 1,
            initialized: 1,
            ..Counts::default()
        }
    );
    assert_eq!(control.reader().snapshot()?, before);
    assert!(control.catalog_reader().read_catalog()?.is_none());
    Ok(())
}

pub(super) async fn target_read_failures_are_static_before_any_commit<W>(writer: W) -> TestResult
where
    W: CatalogCommittedStore,
{
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let (writer, control) = observed(writer);
    control.fault(Fault::Initialized);
    assert_eq!(
        refused(
            ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
                writer,
                source.request(),
                metadata.as_bytes(),
            )
        )
        .await?,
        StateMachineCatalogBootstrapError::Domain(DomainError::TargetReadFailed)
    );
    assert_eq!(
        control.counts(),
        Counts {
            factories: 1,
            initialized: 1,
            ..Counts::default()
        }
    );
    control.reset();
    control.fault(Fault::Probe);
    assert_eq!(
        refused(
            ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
                control.writer(),
                source.request(),
                metadata.as_bytes(),
            )
        )
        .await?,
        StateMachineCatalogBootstrapError::Domain(DomainError::TargetReadFailed)
    );
    assert_eq!(
        control.counts(),
        Counts {
            factories: 1,
            initialized: 1,
            scans: 1,
            ..Counts::default()
        }
    );
    assert!(!control.initialized()?);
    assert!(control.reader().snapshot()?.entries().is_empty());
    assert!(control.catalog_reader().read_catalog()?.is_none());
    Ok(())
}

pub(super) async fn combined_commit_errors_are_unknown_without_starting_native_owner<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
{
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let (writer, control) = observed(writer);
    control.fault(Fault::CommitAfter);
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
        StateMachineCatalogBootstrapError::Domain(DomainError::CommitUnknown)
    );
    assert!(!started);
    exact_calls(&control, &source, metadata.as_bytes())?;
    Ok(())
}

pub(super) async fn combined_commit_before_error_leaves_pristine_without_starting_owner<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
{
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let (writer, control) = observed(writer);
    control.fault(Fault::CommitBefore);
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
        StateMachineCatalogBootstrapError::Domain(DomainError::CommitUnknown)
    );
    assert!(!started);
    assert_eq!(control.counts(), bootstrap_counts());
    let calls = control.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0].artifact_ptr,
        source.image.as_bytes().as_ptr() as usize
    );
    assert_eq!(calls[0].metadata_ptr, metadata.as_bytes().as_ptr() as usize);
    assert!(!control.initialized()?);
    assert!(control.reader().snapshot()?.entries().is_empty());
    assert!(control.catalog_reader().read_catalog()?.is_none());
    // The API still reports Unknown; this test deliberately does not retry it.
    Ok(())
}

pub(super) async fn simulated_startup_refusal_follows_known_combined_commit<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
{
    let source = captured::selected(false)?;
    let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let (writer, control) = observed(writer);
    let mut started = false;
    let result = super::super::bootstrap_with_starter(
        writer,
        source.request(),
        metadata.as_bytes(),
        |state| {
            started = true;
            assert_eq!(control.counts(), bootstrap_counts());
            assert!(state.image_export.is_none());
            assert!(state.image_catalog.is_none());
            assert!(!state.poisoned);
            drop(state);
            Err(StateMachineError::ThreadStart)
        },
    );
    assert_eq!(
        refused(result).await?,
        StateMachineCatalogBootstrapError::OwnerStartAfterCommit
    );
    assert!(started);
    exact_calls(&control, &source, metadata.as_bytes())?;
    // This consuming fake starter launched no OS thread and exhausted no OS resource.
    Ok(())
}
