use sha2::{Digest, Sha256};

use domain::{
    CommittedApplyResult, CommittedCheckpoint, CommittedCheckpointUpdate, CommittedEntryId,
    CommittedImageBootstrapError, CommittedImageRole, CommittedQueueCommand, CommittedQueueWork,
    CommittedSend, CommittedStateMachine, EncodedCommittedImage, MAX_COMMITTED_BODY_BYTES,
    MAX_COMMITTED_IMAGE_BYTES, QueueConfig, SessionId, Timestamp, TrustedCreateSendBootstrap,
    codec, keys,
};
use storage::{
    CommittedStore, FjallReplicaStore, MemoryReplicaStore, Mutation, StateStore, StoreSnapshot,
    WriteBatch,
};

use super::{
    TestResult,
    fixture::{entity, namespace, record, stream},
};

#[path = "bootstrap/catalog.rs"]
mod catalog;
#[path = "bootstrap/crash.rs"]
pub(crate) mod crash;
#[path = "bootstrap/observed.rs"]
mod observed;
use observed::{Counts, Fault, observed};

struct Source {
    image: EncodedCommittedImage,
    checkpoint: CommittedCheckpoint,
    snapshot: StoreSnapshot,
    last_work: CommittedQueueWork,
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn request(source: &Source) -> TrustedCreateSendBootstrap<'_> {
    TrustedCreateSendBootstrap::new(
        source.checkpoint.stream(),
        &source.checkpoint,
        digest(source.image.as_bytes()),
        source.image.as_bytes(),
    )
}

fn work_send(
    time: u64,
    body: Vec<u8>,
    session: Option<SessionId>,
    name: &str,
) -> TestResult<CommittedQueueWork> {
    Ok(CommittedQueueWork::Queue(CommittedQueueCommand::send(
        namespace()?,
        entity()?,
        Timestamp::from_millis(time),
        CommittedSend {
            message_id: name.into(),
            body,
            time_to_live_millis: Some(123),
            session_id: session,
        },
    )))
}

fn source(maximum: bool) -> TestResult<Source> {
    let mut machine = CommittedStateMachine::create(MemoryReplicaStore::new(), stream()?)?;
    let session = SessionId::new("s".repeat(domain::MAX_SESSION_ID_BYTES))?;
    let config = QueueConfig {
        max_message_bytes: MAX_COMMITTED_BODY_BYTES,
        default_time_to_live_millis: Some(1000),
        requires_session: true,
        requires_duplicate_detection: true,
        duplicate_detection_history_time_window_millis: 20_000,
        ..QueueConfig::default()
    };
    let body = if maximum {
        (0..MAX_COMMITTED_BODY_BYTES)
            .map(|n| (n % 251) as u8)
            .collect()
    } else {
        b"selected-body".to_vec()
    };
    let name = "\u{0800}".repeat(domain::MAX_MESSAGE_ID_LENGTH);
    let last_work = work_send(500, b"refused".to_vec(), None, "refused")?;
    let works = [
        CommittedQueueWork::Membership {
            schema_version: 77,
            payload: b"opaque-selected-member".to_vec(),
        },
        CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
            namespace()?,
            entity()?,
            Timestamp::from_millis(10),
            config,
        )),
        work_send(11, body, Some(session.clone()), &name)?,
        work_send(12, b"duplicate-hole".to_vec(), Some(session), &name)?,
        last_work.clone(),
    ];
    for (index, work) in works.iter().enumerate() {
        let update = CommittedCheckpointUpdate {
            stream: stream()?,
            expected_previous: machine.checkpoint()?.last(),
            entry: CommittedEntryId {
                term: 1,
                node_id: 9,
                index: index as u64,
            },
        };
        machine.apply_committed(&update, work)?;
    }
    let snapshot = machine.reader().snapshot()?;
    let image = machine.export_create_send_image()?;
    let checkpoint = domain::DecodedCommittedImage::decode(image.as_bytes())?
        .checkpoint()
        .clone();
    Ok(Source {
        image,
        checkpoint,
        snapshot,
        last_work,
    })
}

fn assert_pristine_counts(counts: Counts, commits: usize) {
    assert_eq!(
        counts,
        Counts {
            reader_factories: 1,
            initialized: 1,
            scans: vec![(Vec::new(), Vec::new(), 1)],
            commits,
            ..Counts::default()
        }
    );
}

fn assert_puts(batch: &WriteBatch, snapshot: &StoreSnapshot) {
    assert_eq!(batch.mutations().len(), snapshot.entries().len());
    for (mutation, (expected_key, expected_value)) in
        batch.mutations().iter().zip(snapshot.entries())
    {
        match mutation {
            Mutation::Put { key, value } => {
                assert_eq!((key, value), (expected_key, expected_value))
            }
            Mutation::Delete { .. } => panic!("a bootstrap emitted a delete"),
        }
    }
}

fn exact_bootstrap_preserves_maximum_rows_checkpoint_holes_and_next_allocation<
    W: CommittedStore,
>(
    writer: W,
) -> TestResult {
    let selected = source(true)?;
    let (writer, control) = observed(writer);
    let mut machine =
        CommittedStateMachine::bootstrap_create_send_image(writer, request(&selected))?;
    assert_pristine_counts(control.counts(), 1);
    assert!(control.initialized()?);
    assert_eq!(control.batches().len(), 1);
    assert_puts(&control.batches()[0], &selected.snapshot);
    assert_eq!(control.reader().snapshot()?, selected.snapshot);
    assert_eq!(machine.checkpoint()?, selected.checkpoint);
    assert_eq!(
        record(&control.reader(), 1)?
            .ok_or("missing restored original")?
            .body
            .len(),
        MAX_COMMITTED_BODY_BYTES
    );
    assert!(record(&control.reader(), 2)?.is_none());
    assert_eq!(
        selected.checkpoint.highest_timestamp(),
        Timestamp::from_millis(500)
    );
    assert_eq!(
        selected
            .checkpoint
            .membership()
            .ok_or("missing opaque membership")?
            .schema_version,
        77
    );
    assert_eq!(
        codec::decode::<Timestamp>(
            &control
                .reader()
                .get(&keys::clock())?
                .ok_or("missing exact business clock")?
        )?,
        Timestamp::from_millis(12)
    );
    let before = control.reader().snapshot()?;
    let replay = CommittedCheckpointUpdate {
        stream: stream()?,
        expected_previous: selected.checkpoint.previous(),
        entry: selected
            .checkpoint
            .last()
            .ok_or("missing restored last mark")?
            .id,
    };
    assert!(matches!(
        machine.apply_committed(&replay, &selected.last_work)?,
        CommittedApplyResult::AlreadyApplied { .. }
    ));
    assert_eq!(control.reader().snapshot()?, before);
    let next = CommittedCheckpointUpdate {
        stream: stream()?,
        expected_previous: selected.checkpoint.last(),
        entry: CommittedEntryId {
            term: 1,
            node_id: 9,
            index: 5,
        },
    };
    machine.apply_committed(
        &next,
        &work_send(
            501,
            b"continued".to_vec(),
            Some(SessionId::new("next")?),
            "next",
        )?,
    )?;
    assert_eq!(
        record(&control.reader(), 3)?
            .ok_or("missing resumed allocation")?
            .body,
        b"continued"
    );
    assert!(record(&control.reader(), 4)?.is_none());
    Ok(())
}

fn selected_identity_and_digest_refuse_before_target_io<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let selected = source(false)?;
    let (writer, control) = observed(writer);
    let before = control.reader().snapshot()?;
    let mut wrong_digest = digest(selected.image.as_bytes());
    wrong_digest[0] ^= 1;
    let wrong = TrustedCreateSendBootstrap::new(
        stream()?,
        &selected.checkpoint,
        wrong_digest,
        selected.image.as_bytes(),
    );
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image(writer, wrong).err(),
        Some(CommittedImageBootstrapError::SelectionMismatch)
    );
    assert_eq!(control.counts(), Counts::default());
    assert_eq!(control.reader().snapshot()?, before);
    assert!(!control.initialized()?);
    let foreign = domain::CommittedStreamId::new([8; 16])?;
    let wrong = TrustedCreateSendBootstrap::new(
        foreign,
        &selected.checkpoint,
        digest(selected.image.as_bytes()),
        selected.image.as_bytes(),
    );
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image(control.writer(), wrong).err(),
        Some(CommittedImageBootstrapError::InvalidSelection)
    );
    assert_eq!(control.counts(), Counts::default());
    let mut foreign_source = CommittedStateMachine::create(MemoryReplicaStore::new(), foreign)?;
    let foreign_image = foreign_source.export_create_send_image()?;
    let foreign_checkpoint = domain::DecodedCommittedImage::decode(foreign_image.as_bytes())?
        .checkpoint()
        .clone();
    let wrong = TrustedCreateSendBootstrap::new(
        foreign,
        &foreign_checkpoint,
        digest(foreign_image.as_bytes()),
        selected.image.as_bytes(),
    );
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image(control.writer(), wrong).err(),
        Some(CommittedImageBootstrapError::SelectionMismatch)
    );
    assert_eq!(control.counts(), Counts::default());
    Ok(())
}

fn valid_changed_row_with_identical_checkpoint_cannot_replace_selected_digest<W: CommittedStore>(
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
    let mut message = record(&raw.reader(), 1)?.ok_or("missing selected record")?;
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
    let wrong = TrustedCreateSendBootstrap::new(
        stream()?,
        &selected.checkpoint,
        digest(selected.image.as_bytes()),
        changed.as_bytes(),
    );
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image(writer, wrong).err(),
        Some(CommittedImageBootstrapError::SelectionMismatch)
    );
    assert_eq!(control.counts(), Counts::default());
    assert!(!control.initialized()?);
    assert!(control.reader().snapshot()?.entries().is_empty());
    Ok(())
}

fn initialized_even_empty_and_uninitialized_orphan_targets_are_never_adopted<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let selected = source(false)?;
    let (writer, control) = observed(writer);
    control.inject(WriteBatch::default())?;
    assert!(control.reader().snapshot()?.entries().is_empty());
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image(writer, request(&selected)).err(),
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
    assert!(control.batches().is_empty());
    control.inject(WriteBatch::default().put(b"orphan".to_vec(), b"preserved".to_vec()))?;
    let before = control.reader().snapshot()?;
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image(control.writer(), request(&selected))
            .err(),
        Some(CommittedImageBootstrapError::TargetNotPristine)
    );
    assert_eq!(
        control.counts(),
        Counts {
            reader_factories: 2,
            initialized: 2,
            ..Counts::default()
        }
    );
    assert_eq!(control.reader().snapshot()?, before);
    control.force_uninitialized();
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image(control.writer(), request(&selected))
            .err(),
        Some(CommittedImageBootstrapError::TargetNotPristine)
    );
    assert_eq!(
        control.counts(),
        Counts {
            reader_factories: 3,
            initialized: 3,
            scans: vec![(Vec::new(), Vec::new(), 1)],
            ..Counts::default()
        }
    );
    assert_eq!(control.reader().snapshot()?, before);
    assert!(control.batches().is_empty());
    Ok(())
}

fn target_read_failures_are_static_and_never_commit<W: CommittedStore>(writer: W) -> TestResult {
    let selected = source(false)?;
    let (writer, control) = observed(writer);
    control.fault(Fault::Initialized);
    let error = CommittedStateMachine::bootstrap_create_send_image(writer, request(&selected))
        .err()
        .ok_or("expected target read refusal")?;
    assert_eq!(error, CommittedImageBootstrapError::TargetReadFailed);
    assert!(!format!("{error:?}: {error}").contains("SECRET"));
    assert_eq!(
        control.counts(),
        Counts {
            reader_factories: 1,
            initialized: 1,
            ..Counts::default()
        }
    );
    control.fault(Fault::Scan);
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image(control.writer(), request(&selected))
            .err(),
        Some(CommittedImageBootstrapError::TargetReadFailed)
    );
    assert_eq!(
        control.counts(),
        Counts {
            reader_factories: 2,
            initialized: 2,
            scans: vec![(Vec::new(), Vec::new(), 1)],
            ..Counts::default()
        }
    );
    assert!(!control.initialized()?);
    assert!(control.reader().snapshot()?.entries().is_empty());
    Ok(())
}

fn both_physical_commit_error_decisions_are_unknown_with_exact_actual_state<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let selected = source(false)?;
    let (writer, control) = observed(writer);
    control.fault(Fault::CommitBefore);
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image(writer, request(&selected)).err(),
        Some(CommittedImageBootstrapError::CommitUnknown)
    );
    assert_pristine_counts(control.counts(), 1);
    assert!(!control.initialized()?);
    assert!(control.reader().snapshot()?.entries().is_empty());
    // This second attempt is explicit only after inspecting the actual pristine
    // result; no retry is inferred from the prior unknown error itself.
    control.fault(Fault::CommitAfter);
    let error =
        CommittedStateMachine::bootstrap_create_send_image(control.writer(), request(&selected))
            .err()
            .ok_or("expected unknown postcommit decision")?;
    assert_eq!(error, CommittedImageBootstrapError::CommitUnknown);
    assert!(!format!("{error:?}: {error}").contains("SECRET"));
    assert!(control.initialized()?);
    assert_eq!(control.reader().snapshot()?, selected.snapshot);
    let reopened = CommittedStateMachine::open(control.writer(), stream()?)?;
    assert_eq!(reopened.checkpoint()?, selected.checkpoint);
    drop(reopened);
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image(control.writer(), request(&selected))
            .err(),
        Some(CommittedImageBootstrapError::TargetNotPristine)
    );
    assert_eq!(control.reader().snapshot()?, selected.snapshot);
    Ok(())
}

fn unsupported_inconsistent_and_oversized_sources_refuse_without_target_io<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let selected = source(false)?;
    let (writer, control) = observed(writer);
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
    let malformed = EncodedCommittedImage::encode(
        CommittedImageRole::CreateSendV1,
        stream()?,
        &raw.reader().snapshot()?,
    )?;
    let request = TrustedCreateSendBootstrap::new(
        stream()?,
        &selected.checkpoint,
        digest(malformed.as_bytes()),
        malformed.as_bytes(),
    );
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image(writer, request).err(),
        Some(CommittedImageBootstrapError::InvalidImage)
    );
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
    let request = TrustedCreateSendBootstrap::new(
        stream()?,
        &selected.checkpoint,
        digest(unsupported.as_bytes()),
        unsupported.as_bytes(),
    );
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image(control.writer(), request).err(),
        Some(CommittedImageBootstrapError::UnsupportedProfile)
    );
    let oversized = vec![0; MAX_COMMITTED_IMAGE_BYTES + 1];
    let request =
        TrustedCreateSendBootstrap::new(stream()?, &selected.checkpoint, [0; 32], &oversized);
    assert_eq!(
        CommittedStateMachine::bootstrap_create_send_image(control.writer(), request).err(),
        Some(CommittedImageBootstrapError::LimitExceeded)
    );
    let mut truncated = selected.image.as_bytes().to_vec();
    truncated.pop();
    let mut bad_checksum = selected.image.as_bytes().to_vec();
    let last = bad_checksum.last_mut().ok_or("missing artifact checksum")?;
    *last ^= 1;
    for malformed in [truncated, bad_checksum] {
        let request = TrustedCreateSendBootstrap::new(
            stream()?,
            &selected.checkpoint,
            digest(&malformed),
            &malformed,
        );
        assert_eq!(
            CommittedStateMachine::bootstrap_create_send_image(control.writer(), request).err(),
            Some(CommittedImageBootstrapError::InvalidImage)
        );
    }
    assert_eq!(control.counts(), Counts::default());
    assert!(!control.initialized()?);
    assert!(control.reader().snapshot()?.entries().is_empty());
    Ok(())
}

fn bootstrap_without_business_clock<W: CommittedStore>(writer: W, refusal: bool) -> TestResult {
    let mut source = CommittedStateMachine::create(MemoryReplicaStore::new(), stream()?)?;
    if refusal {
        let update = CommittedCheckpointUpdate {
            stream: stream()?,
            expected_previous: None,
            entry: CommittedEntryId {
                term: 1,
                node_id: 9,
                index: 0,
            },
        };
        let result = source.apply_committed(
            &update,
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
        assert!(matches!(
            result,
            CommittedApplyResult::Applied {
                application: domain::CommittedApplication::Refused(_),
                ..
            }
        ));
    }
    let image = source.export_create_send_image()?;
    let checkpoint = domain::DecodedCommittedImage::decode(image.as_bytes())?
        .checkpoint()
        .clone();
    let expected = source.reader().snapshot()?;
    assert_eq!(expected.entries().len(), 1);
    assert!(source.reader().get(&keys::clock())?.is_none());
    let (writer, control) = observed(writer);
    let machine = CommittedStateMachine::bootstrap_create_send_image(
        writer,
        TrustedCreateSendBootstrap::new(
            stream()?,
            &checkpoint,
            digest(image.as_bytes()),
            image.as_bytes(),
        ),
    )?;
    assert_pristine_counts(control.counts(), 1);
    assert_eq!(control.reader().snapshot()?, expected);
    assert!(control.reader().get(&keys::clock())?.is_none());
    assert_eq!(machine.checkpoint()?, checkpoint);
    assert_eq!(
        checkpoint.highest_timestamp(),
        Timestamp::from_millis(if refusal { 90 } else { 0 })
    );
    Ok(())
}

fn an_initial_image_keeps_its_exact_empty_checkpoint<W: CommittedStore>(writer: W) -> TestResult {
    bootstrap_without_business_clock(writer, false)
}

fn a_refusal_only_image_keeps_its_watermark_without_inventing_a_business_clock<
    W: CommittedStore,
>(
    writer: W,
) -> TestResult {
    bootstrap_without_business_clock(writer, true)
}

for_each_backend!(
    exact_bootstrap_preserves_maximum_rows_checkpoint_holes_and_next_allocation,
    selected_identity_and_digest_refuse_before_target_io,
    valid_changed_row_with_identical_checkpoint_cannot_replace_selected_digest,
    initialized_even_empty_and_uninitialized_orphan_targets_are_never_adopted,
    target_read_failures_are_static_and_never_commit,
    both_physical_commit_error_decisions_are_unknown_with_exact_actual_state,
    unsupported_inconsistent_and_oversized_sources_refuse_without_target_io,
    an_initial_image_keeps_its_exact_empty_checkpoint,
    a_refusal_only_image_keeps_its_watermark_without_inventing_a_business_clock,
);

#[test]
fn actual_domain_drop_and_fjall_reopen_preserves_every_bootstrapped_row() -> TestResult {
    let selected = source(true)?;
    let directory = testkit::DurableProvider::temporary()?;
    {
        let machine = CommittedStateMachine::bootstrap_create_send_image(
            FjallReplicaStore::open(directory.path())?,
            request(&selected),
        )?;
        assert_eq!(machine.reader().snapshot()?, selected.snapshot);
        assert_eq!(machine.checkpoint()?, selected.checkpoint);
    }
    let reopened =
        CommittedStateMachine::open(FjallReplicaStore::open(directory.path())?, stream()?)?;
    assert_eq!(reopened.reader().snapshot()?, selected.snapshot);
    assert_eq!(reopened.checkpoint()?, selected.checkpoint);
    assert_eq!(
        reopened.reader().apply(WriteBatch::default()),
        Err(storage::StorageError::ReplicaWriteRequired)
    );
    Ok(())
}

fn reopen_physical_error(fault: Fault, installed: bool) -> TestResult {
    let selected = source(false)?;
    let directory = testkit::DurableProvider::temporary()?;
    {
        let (writer, control) = observed(FjallReplicaStore::open(directory.path())?);
        control.fault(fault);
        assert_eq!(
            CommittedStateMachine::bootstrap_create_send_image(writer, request(&selected)).err(),
            Some(CommittedImageBootstrapError::CommitUnknown)
        );
        assert_pristine_counts(control.counts(), 1);
        assert_eq!(control.initialized()?, installed);
        if installed {
            assert_eq!(control.reader().snapshot()?, selected.snapshot);
        } else {
            assert!(control.reader().snapshot()?.entries().is_empty());
        }
    }
    let writer = FjallReplicaStore::open(directory.path())?;
    assert_eq!(writer.is_initialized()?, installed);
    if installed {
        let machine = CommittedStateMachine::open(writer, stream()?)?;
        assert_eq!(machine.checkpoint()?, selected.checkpoint);
        assert_eq!(machine.reader().snapshot()?, selected.snapshot);
    } else {
        assert!(writer.reader().snapshot()?.entries().is_empty());
        assert_eq!(
            CommittedStateMachine::open(writer, stream()?).err(),
            Some(domain::CommittedApplyError::NotInitialized)
        );
    }
    Ok(())
}

#[test]
fn a_before_commit_error_releases_every_handle_and_reopens_exactly_pristine() -> TestResult {
    reopen_physical_error(Fault::CommitBefore, false)
}

#[test]
fn an_after_commit_error_releases_every_handle_and_reopens_the_complete_image() -> TestResult {
    reopen_physical_error(Fault::CommitAfter, true)
}
