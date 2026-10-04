use storage::{
    CatalogCommittedStore, MAX_CATALOG_ARTIFACT_BYTES, MAX_CATALOG_METADATA_BYTES,
    MemoryCatalogReplicaStore, StorageError,
};

use super::*;

#[path = "catalog/crash.rs"]
mod crash;
#[path = "catalog/physical.rs"]
mod physical;

fn assert_catalog_counts(counts: Counts, commits: usize) {
    assert_eq!(
        counts,
        Counts {
            reader_factories: 1,
            initialized: 1,
            scans: vec![(Vec::new(), Vec::new(), 1)],
            catalog_commits: commits,
            ..Counts::default()
        }
    );
}

fn assert_only_catalog_read(counts: Counts) {
    assert_eq!(
        counts,
        Counts {
            catalog_reader_factories: 1,
            catalog_reads: 1,
            ..Counts::default()
        }
    );
}

fn bootstrap_without_business_bound<W: CatalogCommittedStore>(
    writer: W,
    selected: &Source,
    metadata: &[u8],
) -> Result<CommittedStateMachine<W>, CommittedImageBootstrapError> {
    CommittedStateMachine::bootstrap_create_send_image_with_catalog(
        writer,
        request(selected),
        metadata,
    )
}

fn assert_static(error: CommittedImageBootstrapError) {
    assert!(std::error::Error::source(&error).is_none());
    assert!(!format!("{error:?}: {error}").contains("SECRET"));
    assert!(!format!("{error:?}: {error}").contains("private-"));
}

fn exact_combined_bootstrap_publishes_maximum_rows_and_opaque_pair_with_one_commit<
    W: CatalogCommittedStore,
>(
    writer: W,
) -> TestResult {
    let selected = source(true)?;
    let mut metadata = b"private-opaque-native-metadata\x00\xff".to_vec();
    metadata.resize(MAX_CATALOG_METADATA_BYTES, 7);
    let artifact_pointer = selected.image.as_bytes().as_ptr() as usize;
    let (writer, control) = observed(writer);
    // This generic helper has no bounded-reader constraint; observed::Reader
    // itself implements only StateStore, not BoundedStateStore.
    let mut machine = bootstrap_without_business_bound(writer, &selected, &metadata)?;
    assert_catalog_counts(control.counts(), 1);
    assert!(control.initialized()?);
    assert_eq!(control.batches().len(), 1);
    assert_puts(&control.batches()[0], &selected.snapshot);
    let attempts = control.catalog_attempts();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].metadata, metadata);
    assert_eq!(attempts[0].artifact, selected.image.as_bytes());
    assert_eq!(attempts[0].artifact_pointer, artifact_pointer);
    assert_eq!(control.reader().snapshot()?, selected.snapshot);
    assert_eq!(
        control
            .catalog()?
            .ok_or("missing combined opaque slot")?
            .artifact(),
        selected.image.as_bytes()
    );
    assert_catalog_counts(control.counts(), 1);
    control.reset();
    let catalog = machine
        .read_create_send_catalog()?
        .ok_or("missing validated combined catalog")?;
    assert_only_catalog_read(control.counts());
    assert_eq!(catalog.metadata(), metadata);
    assert_eq!(catalog.image_bytes(), selected.image.as_bytes());
    assert_eq!(catalog.checkpoint(), &selected.checkpoint);
    assert_eq!(
        catalog
            .checkpoint()
            .membership()
            .ok_or("missing opaque membership")?
            .schema_version,
        77
    );
    assert!(!format!("{catalog:?}").contains("private-opaque-native-metadata"));
    assert_eq!(
        record(&control.reader(), 1)?
            .ok_or("missing maximum message")?
            .body
            .len(),
        MAX_COMMITTED_BODY_BYTES
    );
    assert!(record(&control.reader(), 2)?.is_none());
    let replay = CommittedCheckpointUpdate {
        stream: stream()?,
        expected_previous: selected.checkpoint.previous(),
        entry: selected
            .checkpoint
            .last()
            .ok_or("missing selected last mark")?
            .id,
    };
    let before = control.reader().snapshot()?;
    assert!(matches!(
        machine.apply_committed(&replay, &selected.last_work)?,
        CommittedApplyResult::AlreadyApplied { .. }
    ));
    assert_eq!(control.reader().snapshot()?, before);
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
            Some(SessionId::new("next")?),
            "next",
        )?,
    )?;
    assert_eq!(
        record(&control.reader(), 3)?
            .ok_or("missing continued message")?
            .body,
        b"continued"
    );
    assert!(record(&control.reader(), 4)?.is_none());
    control.reset();
    let older = machine
        .read_create_send_catalog()?
        .ok_or("combined catalog disappeared after apply")?;
    assert_only_catalog_read(control.counts());
    assert_eq!(older.image_bytes(), selected.image.as_bytes());
    assert_eq!(older.metadata(), metadata);
    assert_eq!(older.checkpoint(), &selected.checkpoint);
    assert_eq!(catalog.image_bytes(), older.image_bytes());
    Ok(())
}

fn catalog_bounds_precede_selection_errors_and_all_target_operations<W: CatalogCommittedStore>(
    writer: W,
) -> TestResult {
    let selected = source(false)?;
    let (writer, control) = observed(writer);
    let oversized_metadata = vec![0; MAX_CATALOG_METADATA_BYTES + 1];
    let wrong_stream = domain::CommittedStreamId::new([8; 16])?;
    let bad_selections = [
        TrustedCreateSendBootstrap::new(
            stream()?,
            &selected.checkpoint,
            [0; 32],
            selected.image.as_bytes(),
        ),
        TrustedCreateSendBootstrap::new(
            wrong_stream,
            &selected.checkpoint,
            [0; 32],
            selected.image.as_bytes(),
        ),
        TrustedCreateSendBootstrap::new(stream()?, &selected.checkpoint, [0; 32], b"malformed"),
    ];
    let mut writer = Some(writer);
    for selection in bad_selections {
        let error = CommittedStateMachine::bootstrap_create_send_image_with_catalog(
            writer.take().ok_or("missing owned writer")?,
            selection,
            &oversized_metadata,
        )
        .err()
        .ok_or("expected metadata limit")?;
        assert_eq!(error, CommittedImageBootstrapError::LimitExceeded);
        assert_static(error);
        assert_eq!(control.counts(), Counts::default());
        assert!(control.batches().is_empty());
        assert!(control.catalog_attempts().is_empty());
        assert!(!control.initialized()?);
        assert!(control.reader().snapshot()?.entries().is_empty());
        assert!(control.catalog()?.is_none());
        writer = Some(control.writer());
    }
    let oversized_artifact = vec![0; MAX_CATALOG_ARTIFACT_BYTES + 1];
    let invalid = TrustedCreateSendBootstrap::new(
        wrong_stream,
        &selected.checkpoint,
        [0; 32],
        &oversized_artifact,
    );
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image_with_catalog(
            writer.take().ok_or("missing writer for artifact limit")?,
            invalid,
            &[],
        )
        .err(),
        Some(CommittedImageBootstrapError::LimitExceeded)
    );
    assert_eq!(control.counts(), Counts::default());
    assert!(control.catalog_attempts().is_empty());
    let mut machine = bootstrap_without_business_bound(control.writer(), &selected, &[])?;
    assert_catalog_counts(control.counts(), 1);
    control.reset();
    assert!(
        machine
            .read_create_send_catalog()?
            .ok_or("missing empty-metadata catalog")?
            .metadata()
            .is_empty()
    );
    assert_only_catalog_read(control.counts());
    Ok(())
}

fn complete_selection_stream_checkpoint_and_digest_are_required_before_target_io<
    W: CatalogCommittedStore,
>(
    writer: W,
) -> TestResult {
    let selected = source(false)?;
    let (writer, control) = observed(writer);
    let mut initial = CommittedStateMachine::create(MemoryReplicaStore::new(), stream()?)?;
    let initial_image = initial.export_create_send_image()?;
    let initial_checkpoint = domain::DecodedCommittedImage::decode(initial_image.as_bytes())?
        .checkpoint()
        .clone();
    let foreign = domain::CommittedStreamId::new([8; 16])?;
    let other = CommittedStateMachine::create(MemoryReplicaStore::new(), foreign)?;
    let foreign_checkpoint = other.checkpoint()?;
    let wrong = [
        (
            TrustedCreateSendBootstrap::new(
                stream()?,
                &selected.checkpoint,
                [0; 32],
                selected.image.as_bytes(),
            ),
            CommittedImageBootstrapError::SelectionMismatch,
        ),
        (
            TrustedCreateSendBootstrap::new(
                foreign,
                &selected.checkpoint,
                digest(selected.image.as_bytes()),
                selected.image.as_bytes(),
            ),
            CommittedImageBootstrapError::InvalidSelection,
        ),
        (
            TrustedCreateSendBootstrap::new(
                foreign,
                &foreign_checkpoint,
                digest(selected.image.as_bytes()),
                selected.image.as_bytes(),
            ),
            CommittedImageBootstrapError::SelectionMismatch,
        ),
        (
            TrustedCreateSendBootstrap::new(
                stream()?,
                &initial_checkpoint,
                digest(selected.image.as_bytes()),
                selected.image.as_bytes(),
            ),
            CommittedImageBootstrapError::SelectionMismatch,
        ),
        (
            TrustedCreateSendBootstrap::new(
                stream()?,
                &selected.checkpoint,
                digest(&selected.image.as_bytes()[..selected.image.len() - 32]),
                selected.image.as_bytes(),
            ),
            CommittedImageBootstrapError::SelectionMismatch,
        ),
    ];
    let mut writer = Some(writer);
    for (selection, expected) in wrong {
        let error = CommittedStateMachine::bootstrap_create_send_image_with_catalog(
            writer.take().ok_or("missing owned selection writer")?,
            selection,
            b"opaque metadata",
        )
        .err()
        .ok_or("expected selection error")?;
        assert_eq!(error, expected);
        assert_static(error);
        assert_eq!(control.counts(), Counts::default());
        assert!(control.catalog_attempts().is_empty());
        assert!(!control.initialized()?);
        assert!(control.reader().snapshot()?.entries().is_empty());
        assert!(control.catalog()?.is_none());
        writer = Some(control.writer());
    }
    let _machine = bootstrap_without_business_bound(
        writer.take().ok_or("missing valid writer")?,
        &selected,
        b"valid",
    )?;
    assert_catalog_counts(control.counts(), 1);
    Ok(())
}

fn a_valid_changed_business_row_cannot_replace_the_selected_complete_digest<
    W: CatalogCommittedStore,
>(
    writer: W,
) -> TestResult {
    let selected = source(false)?;
    let mut raw = MemoryReplicaStore::new();
    let mut rows = WriteBatch::default();
    for (key, value) in selected.snapshot.entries() {
        rows.push_put(key.clone(), value.clone());
    }
    raw.commit(rows)?;
    let key = keys::message(&namespace()?, &entity()?, domain::SequenceNumber::new(1));
    let mut message = record(&raw.reader(), 1)?.ok_or("missing selected business record")?;
    message.body = b"different-valid-body".to_vec();
    raw.commit(WriteBatch::default().put(key, codec::encode(&message)?))?;
    let changed = EncodedCommittedImage::encode(
        CommittedImageRole::CreateSendV1,
        stream()?,
        &raw.reader().snapshot()?,
    )?;
    let checked = domain::ValidatedCreateSendImage::validate(
        domain::DecodedCommittedImage::decode(changed.as_bytes())?,
    )?;
    assert_eq!(checked.checkpoint(), &selected.checkpoint);
    let (writer, control) = observed(writer);
    let selection = TrustedCreateSendBootstrap::new(
        stream()?,
        &selected.checkpoint,
        digest(selected.image.as_bytes()),
        changed.as_bytes(),
    );
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image_with_catalog(
            writer, selection, b"opaque"
        )
        .err(),
        Some(CommittedImageBootstrapError::SelectionMismatch)
    );
    assert_eq!(control.counts(), Counts::default());
    assert!(control.batches().is_empty());
    assert!(control.catalog_attempts().is_empty());
    assert!(!control.initialized()?);
    assert!(control.catalog()?.is_none());
    assert!(control.reader().snapshot()?.entries().is_empty());
    Ok(())
}

fn semantic_legacy_and_container_refusals_never_touch_the_target<W: CatalogCommittedStore>(
    writer: W,
) -> TestResult {
    let selected = source(false)?;
    let mut raw = MemoryReplicaStore::new();
    let mut baseline = WriteBatch::default();
    for (key, value) in selected.snapshot.entries() {
        baseline.push_put(key.clone(), value.clone());
    }
    raw.commit(baseline)?;
    let ready = keys::session_ready(
        &namespace()?,
        &entity()?,
        &SessionId::new("s".repeat(domain::MAX_SESSION_ID_BYTES))?,
        domain::SequenceNumber::new(1),
    );
    raw.commit(WriteBatch::default().delete(ready.clone()))?;
    let missing_ready = EncodedCommittedImage::encode(
        CommittedImageRole::CreateSendV1,
        stream()?,
        &raw.reader().snapshot()?,
    )?;
    raw.commit(
        WriteBatch::default()
            .put(ready, Vec::new())
            .put(vec![0x7f], Vec::new()),
    )?;
    let unsupported = EncodedCommittedImage::encode(
        CommittedImageRole::CreateSendV1,
        stream()?,
        &raw.reader().snapshot()?,
    )?;
    raw.commit(WriteBatch::default().delete(vec![0x7f]))?;
    let message_key = keys::message(&namespace()?, &entity()?, domain::SequenceNumber::new(1));
    let message = raw
        .reader()
        .get(&message_key)?
        .ok_or("missing legacy fixture message")?;
    let mut legacy = message.clone();
    legacy[0] = 10;
    assert_eq!(
        codec::decode::<domain::MessageRecord>(&legacy)?,
        codec::decode::<domain::MessageRecord>(&message)?
    );
    raw.commit(WriteBatch::default().put(message_key, legacy))?;
    let legacy = EncodedCommittedImage::encode(
        CommittedImageRole::CreateSendV1,
        stream()?,
        &raw.reader().snapshot()?,
    )?;
    let mut truncated = selected.image.as_bytes().to_vec();
    truncated.pop();
    let mut checksum = selected.image.as_bytes().to_vec();
    *checksum.last_mut().ok_or("missing checksum")? ^= 1;
    let refused = [
        (
            missing_ready.as_bytes(),
            CommittedImageBootstrapError::InvalidImage,
        ),
        (
            unsupported.as_bytes(),
            CommittedImageBootstrapError::UnsupportedProfile,
        ),
        (
            legacy.as_bytes(),
            CommittedImageBootstrapError::UnsupportedProfile,
        ),
        (
            truncated.as_slice(),
            CommittedImageBootstrapError::InvalidImage,
        ),
        (
            checksum.as_slice(),
            CommittedImageBootstrapError::InvalidImage,
        ),
    ];
    let (writer, control) = observed(writer);
    let mut writer = Some(writer);
    for (artifact, expected) in refused {
        let selection = TrustedCreateSendBootstrap::new(
            stream()?,
            &selected.checkpoint,
            digest(artifact),
            artifact,
        );
        let error = CommittedStateMachine::bootstrap_create_send_image_with_catalog(
            writer.take().ok_or("missing semantic refusal writer")?,
            selection,
            b"private-opaque metadata",
        )
        .err()
        .ok_or("expected source refusal")?;
        assert_eq!(error, expected);
        assert_static(error);
        assert_eq!(control.counts(), Counts::default());
        assert!(control.batches().is_empty());
        assert!(control.catalog_attempts().is_empty());
        assert!(!control.initialized()?);
        assert!(control.reader().snapshot()?.entries().is_empty());
        assert!(control.catalog()?.is_none());
        writer = Some(control.writer());
    }
    Ok(())
}

fn initialized_empty_catalog_only_and_orphan_targets_are_not_adopted<W: CatalogCommittedStore>(
    writer: W,
) -> TestResult {
    let selected = source(false)?;
    let (writer, control) = observed(writer);
    control.inject(WriteBatch::default())?;
    assert_eq!(
        bootstrap_without_business_bound(writer, &selected, b"new").err(),
        Some(CommittedImageBootstrapError::TargetNotPristine)
    );
    assert_eq!(
        control.counts(),
        Counts {
            reader_factories: 1,
            initialized: 1,
            ..Counts::default()
        }
    );
    assert!(control.reader().snapshot()?.entries().is_empty());
    assert!(control.catalog()?.is_none());
    control.inject_catalog(b"existing catalog only", selected.image.as_bytes())?;
    control.reset();
    assert_eq!(
        bootstrap_without_business_bound(control.writer(), &selected, b"new").err(),
        Some(CommittedImageBootstrapError::TargetNotPristine)
    );
    assert_eq!(
        control.counts(),
        Counts {
            reader_factories: 1,
            initialized: 1,
            ..Counts::default()
        }
    );
    assert!(control.reader().snapshot()?.entries().is_empty());
    assert_eq!(
        control
            .catalog()?
            .ok_or("catalog-only target changed")?
            .metadata(),
        b"existing catalog only"
    );
    assert!(control.catalog_attempts().is_empty());
    control.inject(WriteBatch::default().put(b"orphan".to_vec(), b"preserved".to_vec()))?;
    let before = control.reader().snapshot()?;
    // This intentionally lying test adapter exercises the ordinary empty probe;
    // it is not proof of a conforming uninitialized catalog-profile state.
    control.force_uninitialized();
    control.reset();
    assert_eq!(
        bootstrap_without_business_bound(control.writer(), &selected, b"new").err(),
        Some(CommittedImageBootstrapError::TargetNotPristine)
    );
    assert_catalog_counts(control.counts(), 0);
    assert_eq!(control.reader().snapshot()?, before);
    assert_eq!(
        control
            .catalog()?
            .ok_or("orphan target catalog changed")?
            .metadata(),
        b"existing catalog only"
    );
    assert!(control.catalog_attempts().is_empty());
    assert!(control.batches().is_empty());
    Ok(())
}

fn every_target_probe_failure_is_static_and_never_commits<W: CatalogCommittedStore>(
    writer: W,
) -> TestResult {
    let selected = source(false)?;
    let (writer, control) = observed(writer);
    let mut writer = Some(writer);
    for fault in [
        Fault::Initialized,
        Fault::InitializedLimit,
        Fault::Scan,
        Fault::ScanLimit,
    ] {
        control.reset();
        control.fault(fault);
        let error = bootstrap_without_business_bound(
            writer.take().ok_or("missing target refusal writer")?,
            &selected,
            b"opaque",
        )
        .err()
        .ok_or("expected target read failure")?;
        assert_eq!(error, CommittedImageBootstrapError::TargetReadFailed);
        assert_static(error);
        let mut expected = Counts {
            reader_factories: 1,
            initialized: 1,
            ..Counts::default()
        };
        if matches!(fault, Fault::Scan | Fault::ScanLimit) {
            expected.scans.push((Vec::new(), Vec::new(), 1));
        }
        assert_eq!(control.counts(), expected);
        assert!(control.batches().is_empty());
        assert!(control.catalog_attempts().is_empty());
        assert!(!control.initialized()?);
        assert!(control.reader().snapshot()?.entries().is_empty());
        assert!(control.catalog()?.is_none());
        writer = Some(control.writer());
    }
    control.reset();
    let _machine = bootstrap_without_business_bound(
        writer.take().ok_or("missing recovered target writer")?,
        &selected,
        b"valid",
    )?;
    assert_catalog_counts(control.counts(), 1);
    Ok(())
}

fn every_catalog_commit_error_is_unknown_with_no_retry_or_machine_result<
    W: CatalogCommittedStore,
>(
    writer: W,
) -> TestResult {
    let selected = source(false)?;
    let (writer, control) = observed(writer);
    let mut writer = Some(writer);
    for fault in [
        Fault::CommitBefore,
        Fault::CommitLimit,
        Fault::CommitCorrupt,
        Fault::CommitAfter,
    ] {
        control.reset();
        control.fault(fault);
        let error = bootstrap_without_business_bound(
            writer.take().ok_or("missing commit refusal writer")?,
            &selected,
            b"attempted opaque metadata",
        )
        .err()
        .ok_or("expected unknown catalog commit")?;
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
        assert_eq!(attempts[0].metadata, b"attempted opaque metadata");
        if matches!(fault, Fault::CommitAfter) {
            assert!(control.initialized()?);
            assert_eq!(control.reader().snapshot()?, selected.snapshot);
            let catalog = control
                .catalog()?
                .ok_or("complete commit lost its catalog")?;
            assert_eq!(catalog.artifact(), selected.image.as_bytes());
            assert_eq!(catalog.metadata(), b"attempted opaque metadata");
        } else {
            assert!(!control.initialized()?);
            assert!(control.reader().snapshot()?.entries().is_empty());
            assert!(control.catalog()?.is_none());
        }
        // This fixture capability follows inspection of the actual outcome,
        // never retry authorization inferred from an unknown error. The completed
        // target is only refusal-probed below; it is never installed again.
        writer = Some(control.writer());
    }
    control.reset();
    assert_eq!(
        bootstrap_without_business_bound(
            writer.take().ok_or("missing initialized target writer")?,
            &selected,
            b"replace"
        )
        .err(),
        Some(CommittedImageBootstrapError::TargetNotPristine)
    );
    assert_eq!(
        control.counts(),
        Counts {
            reader_factories: 1,
            initialized: 1,
            ..Counts::default()
        }
    );
    assert!(control.catalog_attempts().is_empty());
    let mut machine = CommittedStateMachine::open(control.writer(), stream()?)?;
    control.reset();
    assert_eq!(
        machine
            .read_create_send_catalog()?
            .ok_or("committed catalog not readable after unknown")?
            .image_bytes(),
        selected.image.as_bytes()
    );
    assert_only_catalog_read(control.counts());
    Ok(())
}

fn without_business_clock<W: CatalogCommittedStore>(writer: W, refusal: bool) -> TestResult {
    let mut source = CommittedStateMachine::create(MemoryReplicaStore::new(), stream()?)?;
    if refusal {
        source.apply_committed(
            &CommittedCheckpointUpdate {
                stream: stream()?,
                expected_previous: None,
                entry: CommittedEntryId {
                    term: 1,
                    node_id: 9,
                    index: 0,
                },
            },
            &CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
                namespace()?,
                entity()?,
                Timestamp::from_millis(90),
                QueueConfig {
                    max_delivery_count: 0,
                    ..QueueConfig::default()
                },
            )),
        )?;
    }
    let image = source.export_create_send_image()?;
    let checkpoint = domain::DecodedCommittedImage::decode(image.as_bytes())?
        .checkpoint()
        .clone();
    let rows = source.reader().snapshot()?;
    assert_eq!(rows.entries().len(), 1);
    assert!(source.reader().get(&keys::clock())?.is_none());
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::bootstrap_create_send_image_with_catalog(
        writer,
        TrustedCreateSendBootstrap::new(
            stream()?,
            &checkpoint,
            digest(image.as_bytes()),
            image.as_bytes(),
        ),
        b"\x00\xffopaque",
    )?;
    assert_catalog_counts(control.counts(), 1);
    assert_puts(&control.batches()[0], &rows);
    assert_eq!(control.reader().snapshot()?, rows);
    assert!(control.reader().get(&keys::clock())?.is_none());
    control.reset();
    let catalog = machine
        .read_create_send_catalog()?
        .ok_or("missing no-clock catalog")?;
    assert_only_catalog_read(control.counts());
    assert_eq!(catalog.checkpoint(), &checkpoint);
    assert_eq!(catalog.image_bytes(), image.as_bytes());
    assert_eq!(catalog.metadata(), b"\x00\xffopaque");
    assert_eq!(
        checkpoint.highest_timestamp(),
        Timestamp::from_millis(if refusal { 90 } else { 0 })
    );
    Ok(())
}

fn initial_image_bootstraps_with_exact_empty_progress_and_no_business_clock<
    W: CatalogCommittedStore,
>(
    writer: W,
) -> TestResult {
    without_business_clock(writer, false)
}

fn refusal_only_image_bootstraps_its_watermark_without_inventing_a_business_clock<
    W: CatalogCommittedStore,
>(
    writer: W,
) -> TestResult {
    without_business_clock(writer, true)
}

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

for_each_catalog_backend!(
    exact_combined_bootstrap_publishes_maximum_rows_and_opaque_pair_with_one_commit,
    catalog_bounds_precede_selection_errors_and_all_target_operations,
    complete_selection_stream_checkpoint_and_digest_are_required_before_target_io,
    a_valid_changed_business_row_cannot_replace_the_selected_complete_digest,
    semantic_legacy_and_container_refusals_never_touch_the_target,
    initialized_empty_catalog_only_and_orphan_targets_are_not_adopted,
    every_target_probe_failure_is_static_and_never_commits,
    every_catalog_commit_error_is_unknown_with_no_retry_or_machine_result,
    initial_image_bootstraps_with_exact_empty_progress_and_no_business_clock,
    refusal_only_image_bootstraps_its_watermark_without_inventing_a_business_clock,
);

#[test]
fn combined_bootstrap_compiles_with_a_custom_reader_that_has_no_bounded_capability() -> TestResult {
    let selected = source(false)?;
    let (writer, control) = observed(MemoryCatalogReplicaStore::new());
    let _machine = bootstrap_without_business_bound(writer, &selected, b"opaque")?;
    assert_catalog_counts(control.counts(), 1);
    Ok(())
}
