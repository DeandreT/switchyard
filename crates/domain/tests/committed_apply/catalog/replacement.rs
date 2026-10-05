use domain::{
    CommittedCheckpointUpdate, CommittedEntryId, CommittedImageReplacementError as Error,
    TrustedCreateSendReplacement,
};
use storage::{MAX_CATALOG_ARTIFACT_BYTES, Mutation};

use super::*;

#[path = "replacement/crash.rs"]
mod crash;
#[path = "replacement/fixture.rs"]
mod fixture;
#[path = "replacement/physical.rs"]
mod physical;

use fixture::{Kind, Source, digest, request, source};

macro_rules! for_each_catalog_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory {
            $(#[test] fn $case() -> super::TestResult { super::$case(storage::MemoryCatalogReplicaStore::new()) })+
        }
        mod durable {
            $(#[test] fn $case() -> super::TestResult {
                let directory = testkit::DurableProvider::temporary()?;
                super::$case(storage::FjallCatalogReplicaStore::open(directory.path())?)
            })+
        }
    };
}

fn target<W>(
    writer: W,
) -> TestResult<(
    CommittedStateMachine<observed::Writer<W>>,
    observed::Control<W>,
)>
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    fixture::populate_target(&mut machine)?;
    machine
        .prepare_create_send_catalog()?
        .retain(b"private-old-catalog")?;
    control.reset();
    Ok((machine, control))
}

fn assert_pair<W: CatalogCommittedStore>(
    control: &observed::Control<W>,
    metadata: &[u8],
    artifact: &[u8],
) -> TestResult {
    let stored = control.catalog()?.ok_or("missing exact catalog pair")?;
    assert_eq!(stored.metadata(), metadata);
    assert_eq!(stored.artifact(), artifact);
    Ok(())
}

fn assert_batch(batch: &WriteBatch, old: &StoreSnapshot, selected: &StoreSnapshot) {
    let mut expected = WriteBatch::default();
    for (key, _) in old.entries() {
        if selected
            .entries()
            .binary_search_by(|(candidate, _)| candidate.cmp(key))
            .is_err()
        {
            expected.push_delete(key.clone());
        }
    }
    for (key, value) in selected.entries() {
        expected.push_put(key.clone(), value.clone());
    }
    assert_eq!(batch, &expected);
    let stale = batch
        .mutations()
        .iter()
        .take_while(|row| matches!(row, Mutation::Delete { .. }))
        .count();
    assert!(stale <= old.entries().len());
    assert_eq!(batch.mutations().len(), stale + selected.entries().len());
}

fn exact_replacement_preserves_original_pair_and_all_selected_rows_with_one_commit<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let selected = source(Kind::Populated)?;
    let (mut machine, control) = target(writer)?;
    let old_checkpoint = machine.checkpoint()?;
    let old_rows = control.reader().snapshot()?;
    let existing_reader = machine.reader();
    let mut metadata = b"private-opaque-native-metadata\x00\xff".to_vec();
    metadata.resize(MAX_CATALOG_METADATA_BYTES, 19);
    let pointer = selected.image.as_bytes().as_ptr() as usize;
    control.reset();
    machine
        .replace_create_send_image_with_catalog(request(&selected, &old_checkpoint), &metadata)?;
    assert_capture(&control, 1);
    let attempts = control.attempts();
    assert_eq!(attempts.len(), 1);
    assert_batch(&attempts[0].business, &old_rows, &selected.rows);
    assert!(
        attempts[0]
            .business
            .mutations()
            .iter()
            .any(|row| matches!(row, Mutation::Delete { .. }))
    );
    assert_eq!(attempts[0].metadata, metadata);
    assert_eq!(attempts[0].artifact, selected.image.as_bytes());
    assert_eq!(attempts[0].artifact_pointer, pointer);
    assert_eq!(selected.image.as_bytes().as_ptr() as usize, pointer);
    assert_eq!(control.reader().snapshot()?, selected.rows);
    assert_pair(&control, &metadata, selected.image.as_bytes())?;
    assert_capture(&control, 1);
    // This is the already-held live reader, not a replacement reader factory.
    assert_eq!(
        existing_reader.get(&[0x12])?,
        selected
            .rows
            .entries()
            .iter()
            .find(|(key, _)| key.as_slice() == [0x12])
            .map(|(_, value)| value.clone())
    );
    assert_eq!(machine.checkpoint()?, selected.checkpoint);
    let replay = CommittedCheckpointUpdate {
        stream: stream()?,
        expected_previous: selected.checkpoint.previous(),
        entry: selected
            .checkpoint
            .last()
            .ok_or("missing selected mark")?
            .id,
    };
    assert!(matches!(
        machine.apply_committed(
            &replay,
            &super::maximum_send(2000, None, b"private-refused")?
        )?,
        domain::CommittedApplyResult::AlreadyApplied { .. }
    ));
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
        &CommittedQueueWork::Blank,
    )?;
    control.reset();
    let older = machine
        .read_create_send_catalog()?
        .ok_or("replacement catalog disappeared after apply")?;
    assert_catalog_read(&control);
    assert_eq!(older.image_bytes(), selected.image.as_bytes());
    assert_eq!(older.metadata(), metadata);
    assert_eq!(older.checkpoint(), &selected.checkpoint);
    assert_eq!(
        older
            .checkpoint()
            .membership()
            .ok_or("missing opaque membership")?
            .schema_version,
        999
    );
    Ok(())
}

fn initialized_initial_refusal_and_explicit_earlier_no_clock_images_replace_exactly<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = target(writer)?;
    for kind in [
        Kind::Initial,
        Kind::RefusalOnly,
        Kind::Populated,
        Kind::Initial,
    ] {
        let selected = source(kind)?;
        let old_checkpoint = machine.checkpoint()?;
        control.reset();
        machine.replace_create_send_image_with_catalog(request(&selected, &old_checkpoint), b"")?;
        assert_capture(&control, 1);
        assert_eq!(control.reader().snapshot()?, selected.rows);
        assert_pair(&control, b"", selected.image.as_bytes())?;
        assert_capture(&control, 1);
        assert_eq!(machine.checkpoint()?, selected.checkpoint);
        if matches!(kind, Kind::Initial | Kind::RefusalOnly) {
            assert_eq!(selected.rows.entries().len(), 1);
            assert!(control.reader().get(&keys::clock())?.is_none());
        }
    }
    Ok(())
}

fn every_source_and_metadata_refusal_precedes_all_target_operations<W>(writer: W) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let selected = source(Kind::Populated)?;
    let (mut machine, control) = target(writer)?;
    let old_checkpoint = machine.checkpoint()?;
    let before = control.reader().snapshot()?;
    let old_catalog = control.catalog()?.ok_or("missing baseline pair")?;
    let foreign = source_with_stream()?;
    let metadata = vec![0; MAX_CATALOG_METADATA_BYTES + 1];
    control.reset();
    let malformed = TrustedCreateSendReplacement::new(
        foreign.checkpoint.stream(),
        &old_checkpoint,
        &foreign.checkpoint,
        [0; 32],
        b"malformed",
    );
    assert_eq!(
        machine.replace_create_send_image_with_catalog(malformed, &metadata),
        Err(Error::LimitExceeded)
    );
    assert_eq!(control.counts(), Counts::default());
    let unsupported = fixture::changed_rows(
        &selected,
        WriteBatch::default().put(vec![0x7f], b"private-unsupported"),
    )?;
    let invalid = fixture::changed_rows(
        &selected,
        WriteBatch::default().put(keys::clock(), vec![11, 0xff]),
    )?;
    let mut legacy = selected.image.as_bytes().to_vec();
    let checkpoint_offset = legacy
        .windows(5)
        .position(|value| value == b"SWYC\x01")
        .ok_or("missing CP framing")?;
    legacy[checkpoint_offset + 4] = 2;
    fixture::rehash(&mut legacy);
    let mut unsupported_container = selected.image.as_bytes().to_vec();
    unsupported_container[5] = 2;
    fixture::rehash(&mut unsupported_container);
    let mut bad_checksum = selected.image.as_bytes().to_vec();
    *bad_checksum.last_mut().ok_or("missing checksum")? ^= 1;
    for (stream, old, cp, hash, artifact, error) in [
        (
            foreign.checkpoint.stream(),
            &old_checkpoint,
            &foreign.checkpoint,
            digest(foreign.image.as_bytes()),
            foreign.image.as_bytes(),
            Error::InvalidSelection,
        ),
        (
            stream()?,
            &foreign.checkpoint,
            &selected.checkpoint,
            digest(selected.image.as_bytes()),
            selected.image.as_bytes(),
            Error::InvalidSelection,
        ),
        (
            stream()?,
            &old_checkpoint,
            &foreign.checkpoint,
            digest(selected.image.as_bytes()),
            selected.image.as_bytes(),
            Error::InvalidSelection,
        ),
        (
            stream()?,
            &old_checkpoint,
            &selected.checkpoint,
            [0; 32],
            selected.image.as_bytes(),
            Error::SelectionMismatch,
        ),
        (
            stream()?,
            &old_checkpoint,
            &old_checkpoint,
            digest(selected.image.as_bytes()),
            selected.image.as_bytes(),
            Error::SelectionMismatch,
        ),
        (
            stream()?,
            &old_checkpoint,
            &selected.checkpoint,
            digest(&unsupported_container),
            unsupported_container.as_slice(),
            Error::UnsupportedProfile,
        ),
        (
            stream()?,
            &old_checkpoint,
            &selected.checkpoint,
            digest(&bad_checksum),
            bad_checksum.as_slice(),
            Error::InvalidImage,
        ),
        (
            stream()?,
            &old_checkpoint,
            &selected.checkpoint,
            digest(unsupported.as_bytes()),
            unsupported.as_bytes(),
            Error::UnsupportedProfile,
        ),
        (
            stream()?,
            &old_checkpoint,
            &selected.checkpoint,
            digest(invalid.as_bytes()),
            invalid.as_bytes(),
            Error::InvalidImage,
        ),
        (
            stream()?,
            &old_checkpoint,
            &selected.checkpoint,
            digest(&legacy),
            legacy.as_slice(),
            Error::InvalidImage,
        ),
    ] {
        control.reset();
        assert_eq!(
            machine.replace_create_send_image_with_catalog(
                TrustedCreateSendReplacement::new(stream, old, cp, hash, artifact),
                b"opaque",
            ),
            Err(error)
        );
        assert_eq!(control.counts(), Counts::default());
        assert_eq!(control.reader().snapshot()?, before);
        assert_pair(&control, old_catalog.metadata(), old_catalog.artifact())?;
        assert_eq!(control.counts(), Counts::default());
    }
    Ok(())
}

fn source_with_stream() -> TestResult<Source> {
    let foreign = CommittedStreamId::new([8; 16])?;
    let mut machine = CommittedStateMachine::create(storage::MemoryReplicaStore::new(), foreign)?;
    let rows = machine.reader().snapshot()?;
    let image = machine.export_create_send_image()?;
    let checkpoint = machine.checkpoint()?;
    Ok(Source {
        image,
        checkpoint,
        rows,
    })
}

fn mismatched_target_checkpoint_refuses_without_poisoning_or_changing_the_pair<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let selected = source(Kind::Populated)?;
    let (mut machine, control) = target(writer)?;
    let actual = machine.checkpoint()?;
    let before = control.reader().snapshot()?;
    let pair = control.catalog()?.ok_or("missing original pair")?;
    control.reset();
    assert_eq!(
        machine.replace_create_send_image_with_catalog(
            request(&selected, &selected.checkpoint),
            b"opaque"
        ),
        Err(Error::TargetMismatch)
    );
    assert_capture(&control, 0);
    assert_eq!(control.reader().snapshot()?, before);
    assert_pair(&control, pair.metadata(), pair.artifact())?;
    control.reset();
    machine.replace_create_send_image_with_catalog(request(&selected, &actual), b"opaque")?;
    assert_capture(&control, 1);
    Ok(())
}

fn unsupported_invalid_legacy_and_overlong_targets_are_conservative_preserved_refusals<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let selected = source(Kind::Populated)?;
    let (mut machine, control) = target(writer)?;
    let actual = machine.checkpoint()?;
    let before = control.reader().snapshot()?;
    let pair = control.catalog()?.ok_or("missing original pair")?;
    let (cp_key, mut legacy_cp) = before
        .entries()
        .iter()
        .find(|(key, _)| key.as_slice() == [0x12])
        .cloned()
        .ok_or("missing target CP")?;
    legacy_cp[4] = 2;
    for (mutation, error) in [
        (
            WriteBatch::default().put(vec![0x7f], b"private-unknown"),
            Error::UnsupportedTargetProfile,
        ),
        (
            WriteBatch::default().delete(keys::ready(
                &namespace()?,
                &entity()?,
                SequenceNumber::new(1),
            )),
            Error::InvalidTarget,
        ),
        (
            WriteBatch::default().put(keys::clock(), vec![11, 0xff]),
            Error::InvalidTarget,
        ),
        (
            WriteBatch::default().put(cp_key, legacy_cp),
            Error::InvalidTarget,
        ),
        (
            WriteBatch::default().put(vec![7; MAX_COMMITTED_IMAGE_KEY_BYTES + 1], Vec::new()),
            Error::LimitExceeded,
        ),
        (
            WriteBatch::default().put(vec![0x7f], vec![0; MAX_COMMITTED_IMAGE_VALUE_BYTES + 1]),
            Error::LimitExceeded,
        ),
    ] {
        control.inject_business(mutation)?;
        let refused_state = control.reader().snapshot()?;
        control.reset();
        assert_eq!(
            machine.replace_create_send_image_with_catalog(request(&selected, &actual), b"opaque"),
            Err(error)
        );
        assert_capture(&control, 0);
        assert_eq!(control.reader().snapshot()?, refused_state);
        assert_pair(&control, pair.metadata(), pair.artifact())?;
        let mut restore = WriteBatch::default();
        for (key, _) in refused_state.entries() {
            restore.push_delete(key.clone());
        }
        for (key, value) in before.entries() {
            restore.push_put(key.clone(), value.clone());
        }
        control.inject_business(restore)?;
        assert_eq!(
            machine.export_create_send_image()?.as_bytes(),
            pair.artifact()
        );
    }
    control.reset();
    machine.replace_create_send_image_with_catalog(request(&selected, &actual), b"opaque")?;
    assert_capture(&control, 1);
    Ok(())
}

fn bounded_capture_limit_is_nonfatal_but_physical_failure_poisons_before_any_commit<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let selected = source(Kind::Populated)?;
    let (mut machine, control) = target(writer)?;
    let actual = machine.checkpoint()?;
    let before = control.reader().snapshot()?;
    let pair = control.catalog()?.ok_or("missing original pair")?;
    let blocked = update(&machine, 5)?;
    control.reset();
    control.fault(Fault::CaptureLimit);
    assert_eq!(
        machine.replace_create_send_image_with_catalog(request(&selected, &actual), b"opaque"),
        Err(Error::LimitExceeded)
    );
    assert_capture(&control, 0);
    control.reset();
    control.fault(Fault::CapturePhysical);
    assert_eq!(
        machine.replace_create_send_image_with_catalog(request(&selected, &actual), b"opaque"),
        Err(Error::TargetReadFailed)
    );
    assert_capture(&control, 0);
    assert_eq!(control.reader().snapshot()?, before);
    assert_pair(&control, pair.metadata(), pair.artifact())?;
    let counts = control.counts();
    let overlong_metadata = vec![0; MAX_CATALOG_METADATA_BYTES + 1];
    assert_eq!(
        machine.replace_create_send_image_with_catalog(
            request(&selected, &actual),
            &overlong_metadata
        ),
        Err(Error::Poisoned)
    );
    assert_eq!(
        machine.export_create_send_image().err(),
        Some(CommittedImageExportError::Poisoned)
    );
    assert_eq!(
        machine.prepare_create_send_catalog().err(),
        Some(CommittedCatalogError::Poisoned)
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
    Ok(())
}

fn every_returned_commit_error_is_unknown_poisoned_without_success_or_retry<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let selected = source(Kind::Populated)?;
    let (mut machine, control) = target(writer)?;
    for fault in [
        Fault::CommitBefore,
        Fault::CommitAfter,
        Fault::CommitLimit,
        Fault::CommitCorrupt,
    ] {
        let actual = machine.checkpoint()?;
        let before = control.reader().snapshot()?;
        let pair = control.catalog()?.ok_or("missing pre-attempt pair")?;
        let blocked = update(
            &machine,
            actual.last().ok_or("missing target mark")?.id.index + 1,
        )?;
        control.reset();
        control.fault(fault);
        assert_eq!(
            machine.replace_create_send_image_with_catalog(
                request(&selected, &actual),
                b"private-attempted"
            ),
            Err(Error::CommitUnknown)
        );
        assert_capture(&control, 1);
        assert_eq!(control.attempts().len(), 1);
        let counts = control.counts();
        assert_eq!(
            machine.replace_create_send_image_with_catalog(request(&selected, &actual), b""),
            Err(Error::Poisoned)
        );
        assert_eq!(
            machine.export_create_send_image().err(),
            Some(CommittedImageExportError::Poisoned)
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
        if matches!(fault, Fault::CommitAfter) {
            assert_eq!(control.reader().snapshot()?, selected.rows);
            assert_pair(&control, b"private-attempted", selected.image.as_bytes())?;
        } else {
            assert_eq!(control.reader().snapshot()?, before);
            assert_pair(&control, pair.metadata(), pair.artifact())?;
        }
        // Explicit test-only reacquisition is not evidence of a physical reopen.
        drop(machine);
        machine = CommittedStateMachine::open(control.recover_writer(), stream()?)?;
    }
    Ok(())
}

fn request_and_all_flat_diagnostics_are_source_private<W>(writer: W) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let selected = source(Kind::Populated)?;
    let (machine, _) = target(writer)?;
    let old = machine.checkpoint()?;
    let request = request(&selected, &old);
    assert_eq!(
        format!("{request:?}"),
        "TrustedCreateSendReplacement { .. }"
    );
    assert_eq!(request.expected_target_checkpoint(), &old);
    assert_eq!(request.selected_checkpoint(), &selected.checkpoint);
    for error in [
        Error::Poisoned,
        Error::InvalidSelection,
        Error::SelectionMismatch,
        Error::LimitExceeded,
        Error::Allocation,
        Error::UnsupportedProfile,
        Error::InvalidImage,
        Error::TargetMismatch,
        Error::UnsupportedTargetProfile,
        Error::InvalidTarget,
        Error::TargetReadFailed,
        Error::CommitUnknown,
    ] {
        assert!(std::error::Error::source(&error).is_none());
        assert!(!format!("{error:?}: {error}").contains("private-"));
    }
    Ok(())
}

#[test]
fn actual_overlong_source_artifact_refuses_before_target_capture_or_commit() -> TestResult {
    let selected = source(Kind::Initial)?;
    let (mut machine, control) = target(MemoryCatalogReplicaStore::new())?;
    let old = machine.checkpoint()?;
    let artifact = vec![0; MAX_CATALOG_ARTIFACT_BYTES + 1];
    control.reset();
    assert_eq!(
        machine.replace_create_send_image_with_catalog(
            TrustedCreateSendReplacement::new(
                stream()?,
                &old,
                &selected.checkpoint,
                [0; 32],
                &artifact
            ),
            b"",
        ),
        Err(Error::LimitExceeded)
    );
    assert_eq!(control.counts(), Counts::default());
    Ok(())
}

for_each_catalog_backend!(
    exact_replacement_preserves_original_pair_and_all_selected_rows_with_one_commit,
    initialized_initial_refusal_and_explicit_earlier_no_clock_images_replace_exactly,
    every_source_and_metadata_refusal_precedes_all_target_operations,
    mismatched_target_checkpoint_refuses_without_poisoning_or_changing_the_pair,
    unsupported_invalid_legacy_and_overlong_targets_are_conservative_preserved_refusals,
    bounded_capture_limit_is_nonfatal_but_physical_failure_poisons_before_any_commit,
    every_returned_commit_error_is_unknown_poisoned_without_success_or_retry,
    request_and_all_flat_diagnostics_are_source_private,
);
