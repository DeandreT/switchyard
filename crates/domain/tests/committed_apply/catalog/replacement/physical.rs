use storage::{FjallCatalogReplicaStore, SnapshotCatalogReader};

use super::*;

#[test]
fn real_unknown_before_and_after_commit_reopen_only_after_every_originating_handle_drops()
-> TestResult {
    let selected = source(Kind::Populated)?;
    let selected_pointer = selected.image.as_bytes().as_ptr() as usize;
    for fault in [Fault::CommitBefore, Fault::CommitAfter] {
        let directory = testkit::DurableProvider::temporary()?;
        let (old_image, old_pair, actual_pair, expected_rows, expected_checkpoint) = {
            let writer = FjallCatalogReplicaStore::open(directory.path())?;
            let business = writer.reader();
            let business_clone = business.clone();
            let catalog = writer.catalog_reader();
            let catalog_clone = catalog.clone();
            let (mut machine, control) = target(writer)?;
            let old_checkpoint = machine.checkpoint()?;
            let old_rows = business.snapshot()?;
            let old_image = machine.export_create_send_image()?;
            let old_pair = catalog.read_catalog()?.ok_or("missing old durable pair")?;
            let blocked = update(&machine, 5)?;
            control.reset();
            control.fault(fault);
            assert_eq!(
                machine.replace_create_send_image_with_catalog(
                    request(&selected, &old_checkpoint),
                    b"private-new-durable-pair",
                ),
                Err(Error::CommitUnknown)
            );
            assert_capture(&control, 1);
            let attempts = control.attempts();
            assert_eq!(attempts.len(), 1);
            assert_batch(&attempts[0].business, &old_rows, &selected.rows);
            assert_eq!(attempts[0].artifact_pointer, selected_pointer);
            let counts = control.counts();
            assert_eq!(
                machine.replace_create_send_image_with_catalog(
                    request(&selected, &old_checkpoint),
                    b""
                ),
                Err(Error::Poisoned)
            );
            assert_eq!(
                machine.read_create_send_catalog().err(),
                Some(CommittedCatalogError::Poisoned)
            );
            assert_eq!(
                machine.apply_committed(&blocked, &CommittedQueueWork::Blank),
                Err(CommittedApplyError::Poisoned)
            );
            assert_eq!(control.counts(), counts);
            let (expected_rows, expected_checkpoint) = if matches!(fault, Fault::CommitAfter) {
                (selected.rows.clone(), selected.checkpoint.clone())
            } else {
                (old_rows, old_checkpoint)
            };
            assert_eq!(business.snapshot()?, expected_rows);
            // Diagnostics intentionally remain available after an unknown decision.
            assert_eq!(machine.checkpoint()?, expected_checkpoint);
            let actual_pair = catalog
                .read_catalog()?
                .ok_or("missing pair after unknown write")?;
            if matches!(fault, Fault::CommitAfter) {
                assert_eq!(actual_pair.artifact(), selected.image.as_bytes());
                assert_eq!(actual_pair.metadata(), b"private-new-durable-pair");
            } else {
                assert_eq!(actual_pair.artifact(), old_pair.artifact());
                assert_eq!(actual_pair.metadata(), old_pair.metadata());
            }
            drop(machine);
            drop(control);
            assert!(matches!(
                FjallCatalogReplicaStore::open(directory.path()),
                Err(StorageError::Backend { .. })
            ));
            drop(business);
            drop(catalog);
            assert!(matches!(
                FjallCatalogReplicaStore::open(directory.path()),
                Err(StorageError::Backend { .. })
            ));
            drop(business_clone);
            assert!(matches!(
                FjallCatalogReplicaStore::open(directory.path()),
                Err(StorageError::Backend { .. })
            ));
            drop(catalog_clone);
            // No test control/writer/reader survives; these values own bytes only.
            (
                old_image,
                old_pair,
                actual_pair,
                expected_rows,
                expected_checkpoint,
            )
        };
        let (retained, continued_checkpoint) = {
            let writer = FjallCatalogReplicaStore::open(directory.path())?;
            let mut machine = CommittedStateMachine::open(writer, stream()?)?;
            assert_eq!(machine.reader().snapshot()?, expected_rows);
            assert_eq!(machine.checkpoint()?, expected_checkpoint);
            let retained = machine
                .read_create_send_catalog()?
                .ok_or("missing recovered pair")?;
            assert_eq!(retained.image_bytes(), actual_pair.artifact());
            assert_eq!(retained.metadata(), actual_pair.metadata());
            assert_eq!(retained.checkpoint(), &expected_checkpoint);
            machine.apply_committed(
                &CommittedCheckpointUpdate {
                    stream: stream()?,
                    expected_previous: expected_checkpoint.last(),
                    entry: CommittedEntryId {
                        term: 1,
                        node_id: 9,
                        index: 5,
                    },
                },
                &CommittedQueueWork::Blank,
            )?;
            let continued_checkpoint = machine.checkpoint()?;
            (retained, continued_checkpoint)
        };
        // The selected/old images, snapshots/checkpoints, raw pair and retained
        // validated result remain alive across this second physical acquisition.
        let writer = FjallCatalogReplicaStore::open(directory.path())?;
        let mut reopened = CommittedStateMachine::open(writer, stream()?)?;
        assert_eq!(reopened.checkpoint()?, continued_checkpoint);
        let older = reopened
            .read_create_send_catalog()?
            .ok_or("missing older recovered pair")?;
        assert_eq!(older.image_bytes(), actual_pair.artifact());
        assert_eq!(older.metadata(), actual_pair.metadata());
        assert_eq!(older.checkpoint(), &expected_checkpoint);
        assert_ne!(older.checkpoint(), &continued_checkpoint);
        assert_eq!(old_image.as_bytes(), old_pair.artifact());
        assert_eq!(retained.image_bytes(), older.image_bytes());
        assert_eq!(
            selected.image.as_bytes().as_ptr() as usize,
            selected_pointer
        );
    }
    Ok(())
}
