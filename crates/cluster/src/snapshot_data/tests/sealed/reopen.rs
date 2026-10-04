use domain::{
    CommittedCheckpointUpdate, CommittedEntryId, CommittedQueueWork, CommittedStateMachine,
};
use storage::{
    CatalogCommittedStore, CommittedStore, FjallCatalogReplicaStore, SnapshotCatalogReader,
    StateStore, StorageError,
};

use super::*;

#[tokio::test]
async fn sealed_image_keeps_no_writer_or_reader_handle_across_real_same_directory_reopens()
-> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let (mut data, stored, rows, captured_checkpoint, pointer) = {
        let writer = FjallCatalogReplicaStore::open(directory.path())?;
        let business_reader = writer.reader();
        let business_clone = business_reader.clone();
        let catalog_reader = writer.catalog_reader();
        let catalog_clone = catalog_reader.clone();
        let mut machine = CommittedStateMachine::create(writer, fixture::stream()?)?;
        fixture::populate(&mut machine, PRIVATE_BODY)?;
        let captured_checkpoint = machine.checkpoint()?;
        let rows = business_reader.snapshot()?;
        let image = machine
            .prepare_create_send_catalog()?
            .retain(b"opaque transport fixture metadata")?;
        let pointer = image.as_bytes().as_ptr() as usize;
        let data = BoundedSnapshotData::from_image(image)?;
        assert_sealed(&data, pointer, 0);
        let stored = catalog_reader
            .read_catalog()?
            .ok_or("missing source catalog")?;
        assert_eq!(stored.artifact(), data.as_bytes());
        assert_eq!(stored.metadata(), b"opaque transport fixture metadata");
        // No fixture controls own a hidden writer. Drop the actual machine/W,
        // then each originating reader and clone, before touching this path again.
        drop(machine);
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
        (data, stored, rows, captured_checkpoint, pointer)
    };
    {
        let writer = FjallCatalogReplicaStore::open(directory.path())?;
        let mut machine = CommittedStateMachine::open(writer, fixture::stream()?)?;
        assert_eq!(machine.reader().snapshot()?, rows);
        assert_eq!(machine.checkpoint()?, captured_checkpoint);
        let catalog = machine
            .read_create_send_catalog()?
            .ok_or("missing source pair after real reopen")?;
        assert_eq!(catalog.image_bytes(), data.as_bytes());
        assert_eq!(catalog.image_bytes(), stored.artifact());
        machine.apply_committed(
            &CommittedCheckpointUpdate {
                stream: fixture::stream()?,
                expected_previous: captured_checkpoint.last(),
                entry: CommittedEntryId {
                    term: 1,
                    node_id: 9,
                    index: 3,
                },
            },
            &CommittedQueueWork::Blank,
        )?;
    }
    let mut received = Vec::new();
    data.read_to_end(&mut received).await?;
    assert_eq!(received, stored.artifact());
    assert_sealed(&data, pointer, u64::try_from(data.len())?);
    assert_eq!(
        DecodedCommittedImage::decode(data.as_bytes())?.checkpoint(),
        &captured_checkpoint
    );
    assert_write_refusal(data.write(b"cannot modify source bytes").await.unwrap_err());
    assert_sealed(&data, pointer, u64::try_from(data.len())?);
    // The sealed buffer, captured rows/checkpoint, and owned catalog still live;
    // none prevents another actual same-directory backend acquisition.
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    let mut machine = CommittedStateMachine::open(writer, fixture::stream()?)?;
    assert_ne!(machine.checkpoint()?, captured_checkpoint);
    let catalog = machine
        .read_create_send_catalog()?
        .ok_or("older source pair was lost")?;
    assert_eq!(catalog.image_bytes(), data.as_bytes());
    assert_eq!(catalog.checkpoint(), &captured_checkpoint);
    Ok(())
}
