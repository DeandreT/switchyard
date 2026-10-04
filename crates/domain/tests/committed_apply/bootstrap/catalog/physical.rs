use storage::{FjallCatalogReplicaStore, SnapshotCatalogReader};

use super::*;

#[test]
fn successful_combined_bootstrap_reopens_after_every_writer_and_reader_handle_is_released()
-> TestResult {
    let selected = source(true)?;
    let directory = testkit::DurableProvider::temporary()?;
    let metadata = b"private-opaque-reopen-metadata\x00\xff";
    let (retained, stored) = {
        let durable = FjallCatalogReplicaStore::open(directory.path())?;
        let business_reader = durable.reader();
        let business_clone = business_reader.clone();
        let catalog_reader = durable.catalog_reader();
        let catalog_clone = catalog_reader.clone();
        let (writer, control) = observed(durable);
        let mut machine = bootstrap_without_business_bound(writer, &selected, metadata)?;
        assert_catalog_counts(control.counts(), 1);
        assert_eq!(business_reader.snapshot()?, selected.snapshot);
        let stored = catalog_reader
            .read_catalog()?
            .ok_or("missing low-level complete pair")?;
        assert_eq!(stored.artifact(), selected.image.as_bytes());
        assert_eq!(stored.metadata(), metadata);
        control.reset();
        let retained = machine
            .read_create_send_catalog()?
            .ok_or("missing domain complete pair")?;
        assert_only_catalog_read(control.counts());
        assert_eq!(retained.image_bytes(), selected.image.as_bytes());
        assert_eq!(retained.metadata(), metadata);
        assert_eq!(retained.checkpoint(), &selected.checkpoint);
        drop(machine);
        drop(control);
        assert!(matches!(
            FjallCatalogReplicaStore::open(directory.path()),
            Err(StorageError::Backend { .. })
        ));
        drop(business_reader);
        drop(business_clone);
        drop(catalog_reader);
        assert!(matches!(
            FjallCatalogReplicaStore::open(directory.path()),
            Err(StorageError::Backend { .. })
        ));
        drop(catalog_clone);
        (retained, stored)
    };
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    let mut machine = CommittedStateMachine::open(writer, stream()?)?;
    assert_eq!(machine.reader().snapshot()?, selected.snapshot);
    assert_eq!(machine.checkpoint()?, selected.checkpoint);
    let reopened = machine
        .read_create_send_catalog()?
        .ok_or("missing catalog after actual reopen")?;
    assert_eq!(reopened.image_bytes(), stored.artifact());
    assert_eq!(reopened.metadata(), metadata);
    assert_eq!(retained.image_bytes(), selected.image.as_bytes());
    machine.apply_committed(
        &CommittedCheckpointUpdate {
            stream: stream()?,
            expected_previous: selected.checkpoint.last(),
            entry: CommittedEntryId {
                term: 1,
                node_id: 9,
                index: 5,
            },
        },
        &work_send(
            501,
            b"continued".to_vec(),
            Some(SessionId::new("continued")?),
            "continued",
        )?,
    )?;
    let continued = machine.checkpoint()?;
    drop(machine);
    // All three owned catalog outputs and the original selected image remain
    // alive across another real directory open; none retains a database handle.
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    let mut machine = CommittedStateMachine::open(writer, stream()?)?;
    assert_eq!(machine.checkpoint()?, continued);
    assert_ne!(continued, selected.checkpoint);
    let older = machine
        .read_create_send_catalog()?
        .ok_or("combined catalog not preserved after apply")?;
    assert_eq!(older.checkpoint(), &selected.checkpoint);
    assert_eq!(older.image_bytes(), selected.image.as_bytes());
    assert_eq!(older.metadata(), metadata);
    assert_eq!(retained.image_bytes(), reopened.image_bytes());
    assert_eq!(stored.artifact(), reopened.image_bytes());
    Ok(())
}

fn unknown_reopen(fault: Fault, installed: bool) -> TestResult {
    let selected = source(false)?;
    let metadata = b"unknown-decision opaque metadata\x00\xff";
    let directory = testkit::DurableProvider::temporary()?;
    let stored = {
        let durable = FjallCatalogReplicaStore::open(directory.path())?;
        let business_reader = durable.reader();
        let catalog_reader = durable.catalog_reader();
        let catalog_clone = catalog_reader.clone();
        let (writer, control) = observed(durable);
        control.fault(fault);
        let error = bootstrap_without_business_bound(writer, &selected, metadata)
            .err()
            .ok_or("expected physical unknown decision")?;
        assert_eq!(error, CommittedImageBootstrapError::CommitUnknown);
        assert_static(error);
        assert_catalog_counts(control.counts(), 1);
        assert_eq!(control.batches().len(), 1);
        assert_puts(&control.batches()[0], &selected.snapshot);
        let attempts = control.catalog_attempts();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].artifact, selected.image.as_bytes());
        assert_eq!(
            attempts[0].artifact_pointer,
            selected.image.as_bytes().as_ptr() as usize
        );
        assert_eq!(attempts[0].metadata, metadata);
        assert_eq!(control.initialized()?, installed);
        let stored = catalog_reader.read_catalog()?;
        if installed {
            assert_eq!(business_reader.snapshot()?, selected.snapshot);
            let catalog = stored.as_ref().ok_or("completed commit has no pair")?;
            assert_eq!(catalog.artifact(), selected.image.as_bytes());
            assert_eq!(catalog.metadata(), metadata);
        } else {
            assert!(business_reader.snapshot()?.entries().is_empty());
            assert!(stored.is_none());
        }
        // The failed call consumed its W and temporary target reader. The test
        // control still owns the actual backend until explicitly dropped here.
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
        stored
    };
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    assert_eq!(writer.is_initialized()?, installed);
    assert_eq!(writer.catalog_reader().read_catalog()?.is_some(), installed);
    if !installed {
        assert!(writer.reader().snapshot()?.entries().is_empty());
        assert_eq!(
            CommittedStateMachine::open(writer, stream()?).err(),
            Some(domain::CommittedApplyError::NotInitialized)
        );
    } else {
        let mut machine = CommittedStateMachine::open(writer, stream()?)?;
        assert_eq!(machine.checkpoint()?, selected.checkpoint);
        assert_eq!(machine.reader().snapshot()?, selected.snapshot);
        let pair = machine
            .read_create_send_catalog()?
            .ok_or("complete pair not readable after unknown reopen")?;
        assert_eq!(
            pair.image_bytes(),
            stored
                .as_ref()
                .ok_or("missing owned before-reopen result")?
                .artifact()
        );
        assert_eq!(pair.metadata(), metadata);
        drop(machine);
    }
    // This second fresh open follows inspection of the actual result. Only the
    // proven pristine case makes a newly authorized explicit bootstrap attempt.
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    let mut machine = if installed {
        assert_eq!(
            bootstrap_without_business_bound(writer, &selected, b"must not replace").err(),
            Some(CommittedImageBootstrapError::TargetNotPristine)
        );
        CommittedStateMachine::open(FjallCatalogReplicaStore::open(directory.path())?, stream()?)?
    } else {
        bootstrap_without_business_bound(writer, &selected, metadata)?
    };
    assert_eq!(machine.reader().snapshot()?, selected.snapshot);
    let retained = machine
        .read_create_send_catalog()?
        .ok_or("missing catalog after inspected recovery")?;
    assert_eq!(retained.metadata(), metadata);
    assert_eq!(retained.image_bytes(), selected.image.as_bytes());
    assert_eq!(retained.checkpoint(), &selected.checkpoint);
    if let Some(stored) = stored {
        assert_eq!(stored.artifact(), retained.image_bytes());
    }
    Ok(())
}

#[test]
fn before_combined_commit_error_releases_all_handles_and_reopens_records_init_and_catalog_pristine()
-> TestResult {
    unknown_reopen(Fault::CommitBefore, false)
}

#[test]
fn after_combined_commit_error_releases_all_handles_and_reopens_the_complete_records_init_catalog_pair()
-> TestResult {
    unknown_reopen(Fault::CommitAfter, true)
}
