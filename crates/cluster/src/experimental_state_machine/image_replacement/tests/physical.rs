use storage::{CommittedStore, SnapshotCatalogReader, StateStore};

use super::*;
use observed::Fault;

#[tokio::test]
async fn actual_complete_commit_decisions_reopen_after_all_handles_drop_with_owned_outputs_alive()
-> TestResult {
    for fault in [Fault::None, Fault::CommitBefore, Fault::CommitAfter] {
        let directory = testkit::DurableProvider::temporary()?;
        let (expected_rows, expected_checkpoint, retained, before, inert, receipt) = {
            let (mut machine, control, target) =
                fixture::target(FjallCatalogReplicaStore::open(directory.path())?).await?;
            let business_reader = control.reader();
            let business_clone = business_reader.clone();
            let catalog_reader = control.catalog_reader();
            let catalog_clone = catalog_reader.clone();
            let result = async {
                let before = catalog_fixture::source(&control)?;
                let old_metadata = EncodedNativeSnapshotMetadata::encode(before.image.as_bytes())?;
                control.retain(old_metadata.as_bytes(), before.image.as_bytes())?;
                control.reset();
                let source = fixture::from_selected(captured::initial()?)?;
                let (request, selected_rows, selected_checkpoint, pointer) =
                    fixture::prepared(target.clone(), source)?;
                control.fault(fault);
                let result = machine
                    .replace_create_send_image_with_catalog(request)
                    .await;
                let receipt = match fault {
                    Fault::None => Some(result?),
                    _ => {
                        assert_eq!(
                            result.err(),
                            Some(StateMachineImageReplacementError::Domain(
                                CommittedImageReplacementError::CommitUnknown
                            ))
                        );
                        assert_eq!(
                            machine.checkpoint().await.err(),
                            Some(StateMachineError::Poisoned)
                        );
                        None
                    }
                };
                fixture::assert_counts(&control, 1, 1);
                assert_eq!(control.committed_pointers()[0].1, pointer);
                let retained = catalog_clone
                    .read_catalog()?
                    .ok_or("missing retained output")?;
                let (expected_rows, expected_checkpoint) = if matches!(fault, Fault::CommitBefore) {
                    (before.snapshot.clone(), before.checkpoint.clone())
                } else {
                    (selected_rows, selected_checkpoint)
                };
                assert_eq!(business_clone.snapshot()?, expected_rows);
                let request =
                    fixture::request(expected_checkpoint.clone(), fixture::source(true)?)?;
                let inert = machine.replace_create_send_image_with_catalog(request);
                Ok::<_, Box<dyn Error>>((
                    expected_rows,
                    expected_checkpoint,
                    retained,
                    before,
                    inert,
                    receipt,
                ))
            }
            .await;
            let joined = machine.shutdown().await;
            // All originating physical handles, including observed unique writer,
            // business/catalog readers and their clones, really leave this scope.
            drop(business_clone);
            drop(business_reader);
            drop(catalog_clone);
            drop(catalog_reader);
            drop(control);
            let outputs = result?;
            joined?;
            outputs
        };

        let writer = FjallCatalogReplicaStore::open(directory.path())?;
        let reader = writer.reader();
        let reader_clone = reader.clone();
        let catalog_reader = writer.catalog_reader();
        let catalog_clone = catalog_reader.clone();
        assert_eq!(reader_clone.snapshot()?, expected_rows);
        let reopened = catalog_clone
            .read_catalog()?
            .ok_or("missing reopened catalog")?;
        assert_eq!(reopened.artifact(), retained.artifact());
        assert_eq!(reopened.metadata(), retained.metadata());
        let pair = DecodedNativeSnapshotPair::decode(reopened.metadata(), reopened.artifact())?;
        assert_eq!(pair.checkpoint(), &expected_checkpoint);
        let machine =
            ExperimentalStateMachine::open_with_snapshot_replacement(writer, captured::stream()?)?;
        let result: TestResult = async {
            assert_eq!(machine.checkpoint().await?, expected_checkpoint);
            assert_eq!(
                inert.await.err(),
                Some(StateMachineImageReplacementError::Owner(
                    StateMachineError::Closed
                ))
            );
            Ok(())
        }
        .await;
        fixture::finish(machine, result).await?;
        drop(reader_clone);
        drop(reader);
        drop(catalog_clone);
        drop(catalog_reader);

        // Original owned old image, retained/reopened bytes, and any known static
        // receipt survive a second independent same-directory physical reopen.
        let writer = FjallCatalogReplicaStore::open(directory.path())?;
        assert_eq!(writer.reader().snapshot()?, expected_rows);
        let final_catalog = writer
            .catalog_reader()
            .read_catalog()?
            .ok_or("missing final catalog")?;
        assert_eq!(final_catalog.artifact(), retained.artifact());
        assert_eq!(final_catalog.metadata(), retained.metadata());
        assert!(!before.image.as_bytes().is_empty());
        assert_eq!(receipt.is_some(), matches!(fault, Fault::None));
        assert_eq!(reopened.artifact(), retained.artifact());
    }
    Ok(())
}

#[tokio::test]
async fn all_existing_create_constructors_leave_replacement_disabled() -> TestResult {
    for case in 0..3 {
        let (writer, control) = observed::observed(MemoryCatalogReplicaStore::new());
        let mut machine = match case {
            0 => ExperimentalStateMachine::create(writer, captured::stream()?)?,
            1 => ExperimentalStateMachine::create_with_image_export(writer, captured::stream()?)?,
            _ => {
                ExperimentalStateMachine::create_with_snapshot_catalog(writer, captured::stream()?)?
            }
        };
        let target = match machine.checkpoint().await {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                let _ = machine.shutdown().await;
                return Err(error.into());
            }
        };
        control.reset();
        let result: TestResult = async {
            let request = fixture::request(target, fixture::source(false)?)?;
            assert_eq!(
                machine
                    .replace_create_send_image_with_catalog(request)
                    .await
                    .err(),
                Some(StateMachineImageReplacementError::Disabled)
            );
            fixture::assert_counts(&control, 0, 0);
            Ok(())
        }
        .await;
        fixture::finish(machine, result).await?;
    }
    Ok(())
}

#[tokio::test]
async fn all_existing_bootstrap_variants_leave_replacement_disabled() -> TestResult {
    for case in 0..4 {
        let source = captured::selected(false)?;
        let metadata = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
        let (writer, control) = observed::observed(MemoryCatalogReplicaStore::new());
        let mut machine = match case {
            0 => ExperimentalStateMachine::bootstrap_create_send_image(writer, source.request())?,
            1 => ExperimentalStateMachine::bootstrap_create_send_image_with_export(
                writer,
                source.request(),
            )?,
            2 => ExperimentalStateMachine::bootstrap_create_send_image_with_catalog(
                writer,
                source.request(),
                metadata.as_bytes(),
            )?,
            _ => ExperimentalStateMachine::bootstrap_create_send_image_with_catalog_operations(
                writer,
                source.request(),
                metadata.as_bytes(),
            )?,
        };
        control.reset();
        let result: TestResult = async {
            let request = fixture::request(source.checkpoint, fixture::source(false)?)?;
            assert_eq!(
                machine
                    .replace_create_send_image_with_catalog(request)
                    .await
                    .err(),
                Some(StateMachineImageReplacementError::Disabled)
            );
            fixture::assert_counts(&control, 0, 0);
            Ok(())
        }
        .await;
        fixture::finish(machine, result).await?;
    }
    Ok(())
}
