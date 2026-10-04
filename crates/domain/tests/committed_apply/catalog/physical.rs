use storage::{FjallCatalogReplicaStore, SnapshotCatalogReader};

use super::*;

#[test]
fn durable_unknown_before_and_after_commit_reopen_after_all_handles_are_released() -> TestResult {
    for fault in [Fault::CommitBefore, Fault::CommitAfter] {
        let directory = testkit::DurableProvider::temporary()?;
        let (
            original_image,
            original_catalog,
            expected_rows,
            expected_checkpoint,
            expected_metadata,
            expected_image,
        ) = {
            let durable = FjallCatalogReplicaStore::open(directory.path())?;
            let business_reader = durable.reader();
            let catalog_reader = durable.catalog_reader();
            let catalog_clone = catalog_reader.clone();
            let (writer, control) = observed(durable);
            let mut machine = CommittedStateMachine::create(writer, stream()?)?;
            applied(apply(&mut machine, 0, &create(1, QueueConfig::default())?)?)?;
            applied(apply(
                &mut machine,
                1,
                &send(2, "original", b"private-original-body")?,
            )?)?;
            let original_image = machine
                .prepare_create_send_catalog()?
                .retain(b"original opaque metadata")?;
            let original_catalog = machine
                .read_create_send_catalog()?
                .ok_or("missing original catalog")?;
            let original_checkpoint = original_catalog.checkpoint().clone();
            applied(apply(
                &mut machine,
                2,
                &send(3, "later", b"private-later-body")?,
            )?)?;
            let expected_checkpoint = machine.checkpoint()?;
            assert_ne!(expected_checkpoint, original_checkpoint);
            let expected_rows = business_reader.snapshot()?;
            let blocked_update = update(&machine, 3)?;
            let blocked_work = send(4, "blocked", b"must not be applied")?;
            control.reset();
            let token = machine.prepare_create_send_catalog()?;
            assert_eq!(token.checkpoint(), &expected_checkpoint);
            let attempted_image = token.image_bytes().to_vec();
            let attempted_pointer = token.image_bytes().as_ptr() as usize;
            control.fault(fault);
            assert_eq!(
                token.retain(b"attempted opaque metadata").unwrap_err(),
                CommittedCatalogError::CommitUnknown
            );
            assert_capture(&control, 1);
            let attempts = control.attempts();
            assert_eq!(attempts.len(), 1);
            assert!(attempts[0].business.is_empty());
            assert_eq!(attempts[0].artifact, attempted_image);
            assert_eq!(attempts[0].artifact_pointer, attempted_pointer);
            assert_eq!(business_reader.snapshot()?, expected_rows);
            let counts = control.counts();
            assert_eq!(
                machine.prepare_create_send_catalog().unwrap_err(),
                CommittedCatalogError::Poisoned
            );
            assert_eq!(
                machine.read_create_send_catalog().unwrap_err(),
                CommittedCatalogError::Poisoned
            );
            assert_eq!(
                machine.apply_committed(&blocked_update, &blocked_work),
                Err(CommittedApplyError::Poisoned)
            );
            assert_eq!(control.counts(), counts);
            let (expected_metadata, expected_image) = if matches!(fault, Fault::CommitAfter) {
                (b"attempted opaque metadata".to_vec(), attempted_image)
            } else {
                (
                    original_catalog.metadata().to_vec(),
                    original_catalog.image_bytes().to_vec(),
                )
            };
            let actual = catalog_reader
                .read_catalog()?
                .ok_or("missing low-level catalog after unknown decision")?;
            assert_eq!(actual.metadata(), expected_metadata);
            assert_eq!(actual.artifact(), expected_image);
            // Drop the machine and every test control holding the actual writer.
            // Originating readers still hold the database until individually dropped.
            drop(machine);
            drop(control);
            assert!(matches!(
                FjallCatalogReplicaStore::open(directory.path()),
                Err(StorageError::Backend { .. })
            ));
            drop(business_reader);
            drop(catalog_reader);
            assert!(matches!(
                FjallCatalogReplicaStore::open(directory.path()),
                Err(StorageError::Backend { .. })
            ));
            drop(catalog_clone);
            // `actual`, `original_image` and `original_catalog` contain bytes only.
            assert_eq!(actual.artifact(), expected_image);
            (
                original_image,
                original_catalog,
                expected_rows,
                expected_checkpoint,
                expected_metadata,
                expected_image,
            )
        };
        let writer = FjallCatalogReplicaStore::open(directory.path())?;
        let mut machine = CommittedStateMachine::open(writer, stream()?)?;
        assert_eq!(machine.reader().snapshot()?, expected_rows);
        assert_eq!(machine.checkpoint()?, expected_checkpoint);
        let retained = machine
            .read_create_send_catalog()?
            .ok_or("missing catalog after actual reopen")?;
        assert_eq!(retained.metadata(), expected_metadata);
        assert_eq!(retained.image_bytes(), expected_image);
        if matches!(fault, Fault::CommitBefore) {
            assert_ne!(retained.checkpoint(), &expected_checkpoint);
            assert_eq!(retained.checkpoint(), original_catalog.checkpoint());
        } else {
            assert_eq!(retained.checkpoint(), &expected_checkpoint);
        }
        assert_eq!(original_image.as_bytes(), original_catalog.image_bytes());
        assert_eq!(original_catalog.metadata(), b"original opaque metadata");
        applied(apply(
            &mut machine,
            3,
            &send(4, "continued", b"continued after reopen")?,
        )?)?;
        let continued_checkpoint = machine.checkpoint()?;
        let unchanged = machine
            .read_create_send_catalog()?
            .ok_or("missing older catalog after continued apply")?;
        assert_eq!(unchanged.image_bytes(), expected_image);
        assert_eq!(unchanged.metadata(), expected_metadata);
        drop(machine);
        // All retained results stay alive across another real same-directory open.
        let writer = FjallCatalogReplicaStore::open(directory.path())?;
        let mut reopened = CommittedStateMachine::open(writer, stream()?)?;
        assert_eq!(reopened.checkpoint()?, continued_checkpoint);
        let catalog = reopened
            .read_create_send_catalog()?
            .ok_or("missing catalog on second reopen")?;
        assert_eq!(catalog.image_bytes(), expected_image);
        assert_eq!(catalog.metadata(), expected_metadata);
        assert_eq!(original_image.as_bytes(), original_catalog.image_bytes());
        assert_eq!(retained.image_bytes(), unchanged.image_bytes());
    }
    Ok(())
}
