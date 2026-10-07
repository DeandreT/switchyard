use domain::{
    BrokerError, CommittedApplication, CommittedApplyError, CommittedImageExportError,
    CommittedImageRole, CommittedQueueCommand, CommittedQueueWork, CommittedSend,
    CommittedStateMachine, DecodedCommittedImage, EncodedCommittedImage, MAX_COMMITTED_BODY_BYTES,
    MAX_COMMITTED_IMAGE_BYTES, MAX_COMMITTED_IMAGE_KEY_BYTES, MAX_COMMITTED_IMAGE_ROWS,
    MAX_COMMITTED_IMAGE_VALUE_BYTES, QueueConfig, SequenceNumber, SessionId, Timestamp,
    ValidatedCreateSendLayout17Image, keys,
};
use storage::{
    BoundedStateStore, CommittedStore, FjallReplicaStore, ReadLimits, StateStore, StorageError,
    WriteBatch,
};

use super::{
    TestResult,
    fixture::{applied, apply, create, entity, namespace, send, stream, update},
};

#[path = "export/observed.rs"]
mod observed;
use observed::{Counts, Fault, observed};

fn limits() -> ReadLimits {
    ReadLimits {
        max_rows: MAX_COMMITTED_IMAGE_ROWS,
        max_key_bytes: MAX_COMMITTED_IMAGE_KEY_BYTES,
        max_value_bytes: MAX_COMMITTED_IMAGE_VALUE_BYTES,
        max_total_bytes: MAX_COMMITTED_IMAGE_BYTES,
    }
}

fn assert_one_capture<W: CommittedStore>(control: &observed::Control<W>) {
    assert_eq!(
        control.counts(),
        Counts {
            bounded: 1,
            ..Counts::default()
        }
    );
    assert_eq!(control.limits(), vec![limits()]);
}

fn session_input(
    time: u64,
    session: Option<SessionId>,
    body: &[u8],
) -> TestResult<CommittedQueueWork> {
    Ok(CommittedQueueWork::Queue(CommittedQueueCommand::send(
        namespace()?,
        entity()?,
        Timestamp::from_millis(time),
        CommittedSend {
            message_id: "\u{0800}".repeat(domain::MAX_MESSAGE_ID_LENGTH),
            body: body.to_vec(),
            time_to_live_millis: Some(123),
            session_id: session,
        },
    )))
}

fn healthy_export_captures_exact_maximum_records_and_watermark_gap_once<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let config = QueueConfig {
        max_message_bytes: MAX_COMMITTED_BODY_BYTES,
        default_time_to_live_millis: Some(1000),
        requires_session: true,
        requires_duplicate_detection: true,
        duplicate_detection_history_time_window_millis: 20_000,
        ..QueueConfig::default()
    };
    applied(apply(&mut machine, 0, &create(100, config)?)?)?;
    let session = SessionId::new("s".repeat(domain::MAX_SESSION_ID_BYTES))?;
    let body = (0..MAX_COMMITTED_BODY_BYTES)
        .map(|offset| (offset % 251) as u8)
        .collect::<Vec<_>>();
    applied(apply(
        &mut machine,
        1,
        &session_input(110, Some(session.clone()), &body)?,
    )?)?;
    applied(apply(
        &mut machine,
        2,
        &session_input(111, Some(session), b"duplicate")?,
    )?)?;
    let (_, refused) = applied(apply(
        &mut machine,
        3,
        &session_input(50_000, None, b"refused")?,
    )?)?;
    assert_eq!(
        refused,
        CommittedApplication::Refused(BrokerError::SessionRequired)
    );
    let captured = control.reader().snapshot()?;
    let expected_checkpoint = machine.checkpoint()?;
    let expected = EncodedCommittedImage::encode(
        CommittedImageRole::CreateSendLayout17V1,
        stream()?,
        &captured,
    )?;
    control.reset();
    let image = machine.export_create_send_image()?;
    assert_one_capture(&control);
    assert_eq!(image.as_bytes(), expected.as_bytes());
    let decoded = DecodedCommittedImage::decode(image.as_bytes())?;
    assert_eq!(decoded.checkpoint(), &expected_checkpoint);
    assert_eq!(
        decoded.checkpoint().highest_timestamp(),
        Timestamp::from_millis(50_000)
    );
    assert_eq!(
        decoded
            .rows()
            .map(|row| (row.key().to_vec(), row.value().to_vec()))
            .collect::<Vec<_>>(),
        captured.entries()
    );
    let checked = ValidatedCreateSendLayout17Image::validate(decoded)?;
    assert_eq!((checked.queue_count(), checked.message_count()), (1, 1));
    assert_eq!(control.reader().snapshot()?, captured);
    assert_eq!(
        control.reader().apply(WriteBatch::default()),
        Err(StorageError::ReplicaWriteRequired)
    );
    assert_one_capture(&control);
    Ok(())
}

fn initial_refusal_only_and_opaque_membership_export_without_extra_progress_reads<W>(
    writer: W,
) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    for step in 0..3 {
        if step == 1 {
            let (_, refused) = applied(apply(&mut machine, 0, &send(777, "missing", b"body")?)?)?;
            assert_eq!(
                refused,
                CommittedApplication::Refused(BrokerError::QueueNotFound)
            );
        } else if step == 2 {
            applied(apply(
                &mut machine,
                1,
                &CommittedQueueWork::Membership {
                    schema_version: 999,
                    payload: b"opaque-not-cluster-membership".to_vec(),
                },
            )?)?;
        }
        let captured = control.reader().snapshot()?;
        let checkpoint = machine.checkpoint()?;
        control.reset();
        let image = machine.export_create_send_image()?;
        assert_one_capture(&control);
        let checked = ValidatedCreateSendLayout17Image::validate(DecodedCommittedImage::decode(
            image.as_bytes(),
        )?)?;
        assert_eq!((checked.queue_count(), checked.message_count()), (0, 0));
        assert_eq!(checked.checkpoint(), &checkpoint);
        assert_eq!(control.reader().snapshot()?, captured);
        assert_one_capture(&control);
    }
    Ok(())
}

fn prior_pre_and_post_commit_write_errors_refuse_export_before_all_io<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    applied(apply(&mut machine, 0, &create(1, QueueConfig::default())?)?)?;
    let work = send(2, "first", b"first-body")?;
    let request = update(&machine, 1)?;
    control.fault(Fault::WriteBefore);
    assert!(matches!(
        machine.apply_committed(&request, &work),
        Err(CommittedApplyError::Storage(_))
    ));
    let before = control.reader().snapshot()?;
    control.reset();
    assert_eq!(
        machine.export_create_send_image().err(),
        Some(CommittedImageExportError::Poisoned)
    );
    assert_eq!(
        machine.apply_committed(&request, &work),
        Err(CommittedApplyError::Poisoned)
    );
    assert_eq!(control.counts(), Counts::default());
    assert_eq!(control.reader().snapshot()?, before);
    drop(machine);

    let mut machine = CommittedStateMachine::open(control.recover_writer(), stream()?)?;
    applied(machine.apply_committed(&request, &work)?)?;
    let work = send(3, "second", b"second-body")?;
    let request = update(&machine, 2)?;
    control.fault(Fault::WriteAfter);
    assert!(matches!(
        machine.apply_committed(&request, &work),
        Err(CommittedApplyError::Storage(_))
    ));
    let after_physical_commit = control.reader().snapshot()?;
    let second_key = keys::message(&namespace()?, &entity()?, SequenceNumber::new(2));
    assert!(
        after_physical_commit
            .entries()
            .iter()
            .any(|(key, _)| key == &second_key)
    );
    control.reset();
    assert_eq!(
        machine.export_create_send_image().err(),
        Some(CommittedImageExportError::Poisoned)
    );
    assert_eq!(
        machine.apply_committed(&request, &work),
        Err(CommittedApplyError::Poisoned)
    );
    assert_eq!(control.counts(), Counts::default());
    assert_eq!(control.reader().snapshot()?, after_physical_commit);
    Ok(())
}

fn physical_read_error_is_redacted_and_poisons_further_work_before_io<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let request = update(&machine, 0)?;
    let work = create(1, QueueConfig::default())?;
    let source = control.reader().snapshot()?;
    control.reset();
    control.fault(Fault::ReadPhysical);
    let error = machine
        .export_create_send_image()
        .err()
        .ok_or("expected read refusal")?;
    assert_eq!(error, CommittedImageExportError::ReadFailed);
    assert!(!format!("{error:?}: {error}").contains("secret-backend-detail"));
    assert_one_capture(&control);
    assert_eq!(control.reader().snapshot()?, source);
    let counts = control.counts();
    assert_eq!(
        machine.export_create_send_image().err(),
        Some(CommittedImageExportError::Poisoned)
    );
    assert_eq!(
        machine.apply_committed(&request, &work),
        Err(CommittedApplyError::Poisoned)
    );
    assert_eq!(control.counts(), counts);
    drop(machine);
    let mut recovered = CommittedStateMachine::open(control.recover_writer(), stream()?)?;
    assert!(recovered.export_create_send_image().is_ok());
    Ok(())
}

fn bounded_quota_refusals_do_not_poison_and_never_use_a_fallback<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    applied(apply(&mut machine, 0, &create(1, QueueConfig::default())?)?)?;
    let baseline = control.reader().snapshot()?;
    control.reset();
    control.fault(Fault::ReadLimit);
    assert_eq!(
        machine.export_create_send_image().err(),
        Some(CommittedImageExportError::LimitExceeded)
    );
    assert_one_capture(&control);
    assert_eq!(control.reader().snapshot()?, baseline);

    // A real backend limit fails before this too-large value is copied into
    // the captured result. It is not normalized, truncated, or retried.
    let oversized = vec![0; MAX_COMMITTED_IMAGE_VALUE_BYTES + 1];
    let extra = b"\x7fextra".to_vec();
    control.inject(WriteBatch::default().put(extra.clone(), oversized))?;
    let over_limit = control.reader().snapshot()?;
    control.reset();
    assert_eq!(
        machine.export_create_send_image().err(),
        Some(CommittedImageExportError::LimitExceeded)
    );
    assert_one_capture(&control);
    assert_eq!(control.reader().snapshot()?, over_limit);
    control.inject(WriteBatch::default().delete(extra))?;
    control.reset();
    assert!(machine.export_create_send_image().is_ok());
    assert_one_capture(&control);
    assert_eq!(control.reader().snapshot()?, baseline);
    let (_, application) = applied(apply(&mut machine, 1, &send(2, "healthy", b"body")?)?)?;
    assert!(
        matches!(application, CommittedApplication::Queue(result) if result.outcome == (domain::CommandOutcome::Sent { sequence: SequenceNumber::new(1) }))
    );
    Ok(())
}

fn broader_malformed_relational_and_checkpoint_refusals_are_nonfatal<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    applied(apply(&mut machine, 0, &create(1, QueueConfig::default())?)?)?;
    applied(apply(&mut machine, 1, &send(2, "one", b"original")?)?)?;
    let baseline = control.reader().snapshot()?;
    let checkpoint = machine.checkpoint()?;
    let message_key = keys::message(&namespace()?, &entity()?, SequenceNumber::new(1));
    let ready_key = keys::ready(&namespace()?, &entity()?, SequenceNumber::new(1));
    let message_value = baseline
        .entries()
        .iter()
        .find(|(key, _)| *key == message_key)
        .ok_or("missing fixture message")?
        .1
        .clone();
    let (checkpoint_key, checkpoint_value) = baseline
        .entries()
        .iter()
        .find(|(key, _)| key.as_slice() == [0x12])
        .ok_or("missing checkpoint")?;
    let mut legacy = message_value.clone();
    legacy[0] = 10;
    assert_eq!(
        domain::codec::decode::<domain::MessageRecord>(&legacy)?,
        domain::codec::decode::<domain::MessageRecord>(&message_value)?,
        "the nonfatal legacy fixture must also be valid under ordinary migration decoding",
    );
    let mut trailing = message_value.clone();
    trailing.push(0);
    // An opened store can legitimately contain a broader/legacy value while
    // its checkpoint is valid; opening is not a CreateSend-only guarantee.
    control.inject(WriteBatch::default().put(message_key.clone(), legacy))?;
    drop(machine);
    let mut machine = CommittedStateMachine::open(control.recover_writer(), stream()?)?;
    let broader = control.reader().snapshot()?;
    control.reset();
    assert_eq!(
        machine.export_create_send_image().err(),
        Some(CommittedImageExportError::UnsupportedProfile)
    );
    assert_one_capture(&control);
    assert_eq!(control.reader().snapshot()?, broader);
    control.inject(WriteBatch::default().put(message_key.clone(), message_value.clone()))?;
    for (mutation, error) in [
        (
            WriteBatch::default().delete(ready_key.clone()),
            CommittedImageExportError::InvalidImage,
        ),
        (
            WriteBatch::default().put(message_key.clone(), trailing),
            CommittedImageExportError::InvalidImage,
        ),
        (
            WriteBatch::default().put(checkpoint_key.clone(), vec![0]),
            CommittedImageExportError::InvalidImage,
        ),
    ] {
        control.inject(mutation)?;
        let mutated = control.reader().snapshot()?;
        control.reset();
        assert_eq!(machine.export_create_send_image().err(), Some(error));
        assert_one_capture(&control);
        assert_eq!(control.reader().snapshot()?, mutated);
        let mut restore = WriteBatch::default();
        restore.push_put(message_key.clone(), message_value.clone());
        restore.push_put(ready_key.clone(), Vec::new());
        restore.push_put(checkpoint_key.clone(), checkpoint_value.clone());
        control.inject(restore)?;
        assert_eq!(machine.checkpoint()?, checkpoint);
        control.reset();
        assert!(machine.export_create_send_image().is_ok());
        assert_one_capture(&control);
        assert_eq!(control.reader().snapshot()?, baseline);
    }
    let (_, application) = applied(apply(&mut machine, 2, &send(3, "two", b"healthy")?)?)?;
    assert!(
        matches!(application, CommittedApplication::Queue(result) if result.outcome == (domain::CommandOutcome::Sent { sequence: SequenceNumber::new(2) }))
    );
    Ok(())
}

for_each_backend!(
    healthy_export_captures_exact_maximum_records_and_watermark_gap_once,
    initial_refusal_only_and_opaque_membership_export_without_extra_progress_reads,
    prior_pre_and_post_commit_write_errors_refuse_export_before_all_io,
    physical_read_error_is_redacted_and_poisons_further_work_before_io,
    bounded_quota_refusals_do_not_poison_and_never_use_a_fallback,
    broader_malformed_relational_and_checkpoint_refusals_are_nonfatal,
);

#[test]
fn durable_export_releases_every_handle_before_reopening_the_same_directory() -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let (bytes, source) = {
        let writer = FjallReplicaStore::open(directory.path())?;
        let mut machine = CommittedStateMachine::create(writer, stream()?)?;
        applied(apply(&mut machine, 0, &create(1, QueueConfig::default())?)?)?;
        applied(apply(
            &mut machine,
            1,
            &send(2, "retained", b"same-directory")?,
        )?)?;
        let reader = machine.reader();
        let before = reader.snapshot_bounded(limits())?;
        let image = machine.export_create_send_image()?;
        assert_eq!(reader.snapshot_bounded(limits())?, before);
        let bytes = image.as_bytes().to_vec();
        drop(reader);
        drop(machine);
        (bytes, before)
    };
    let mut reopened =
        CommittedStateMachine::open(FjallReplicaStore::open(directory.path())?, stream()?)?;
    assert_eq!(reopened.reader().snapshot_bounded(limits())?, source);
    let image = reopened.export_create_send_image()?;
    assert_eq!(image.as_bytes(), bytes);
    assert_eq!(reopened.reader().snapshot_bounded(limits())?, source);
    Ok(())
}
