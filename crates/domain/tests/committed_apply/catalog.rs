use domain::{
    CommittedApplication, CommittedApplyError, CommittedCatalogError, CommittedImageExportError,
    CommittedImageRole, CommittedQueueCommand, CommittedQueueWork, CommittedSend,
    CommittedStateMachine, CommittedStreamId, DecodedCommittedImage, EncodedCommittedImage,
    MAX_COMMITTED_BODY_BYTES, MAX_COMMITTED_IMAGE_BYTES, MAX_COMMITTED_IMAGE_KEY_BYTES,
    MAX_COMMITTED_IMAGE_ROWS, MAX_COMMITTED_IMAGE_VALUE_BYTES, QueueConfig, SequenceNumber,
    SessionId, Timestamp, ValidatedCreateSendImage, keys,
};
use storage::{
    BoundedStateStore, CatalogCommittedStore, CommittedStore, MAX_CATALOG_METADATA_BYTES,
    MemoryCatalogReplicaStore, MemoryStore, ReadLimits, StateStore, StorageError, StoreSnapshot,
    WriteBatch,
};

use super::{
    TestResult,
    fixture::{applied, apply, create, entity, namespace, send, stream, update},
};

#[path = "catalog/observed.rs"]
mod observed;
#[path = "catalog/physical.rs"]
mod physical;
#[path = "catalog/replacement.rs"]
mod replacement;
#[path = "catalog/unbounded.rs"]
mod unbounded;

use observed::{Counts, Fault, observed};

fn limits() -> ReadLimits {
    ReadLimits {
        max_rows: MAX_COMMITTED_IMAGE_ROWS,
        max_key_bytes: MAX_COMMITTED_IMAGE_KEY_BYTES,
        max_value_bytes: MAX_COMMITTED_IMAGE_VALUE_BYTES,
        max_total_bytes: MAX_COMMITTED_IMAGE_BYTES,
    }
}

fn assert_capture<W: CatalogCommittedStore>(control: &observed::Control<W>, commits: usize) {
    assert_eq!(
        control.counts(),
        Counts {
            bounded: 1,
            catalog_commits: commits,
            ..Counts::default()
        }
    );
    assert_eq!(control.limits(), vec![limits()]);
}

fn assert_catalog_read<W: CatalogCommittedStore>(control: &observed::Control<W>) {
    assert_eq!(
        control.counts(),
        Counts {
            catalog_factories: 1,
            catalog_reads: 1,
            ..Counts::default()
        }
    );
}

fn maximum_send(
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

fn capture_and_retention_preserve_the_original_max_body_image_and_exact_empty_batch<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
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
        &maximum_send(110, Some(session.clone()), &body)?,
    )?)?;
    applied(apply(
        &mut machine,
        2,
        &maximum_send(111, Some(session), b"duplicate")?,
    )?)?;
    let (_, refused) = applied(apply(
        &mut machine,
        3,
        &maximum_send(50_000, None, b"refused")?,
    )?)?;
    assert_eq!(
        refused,
        CommittedApplication::Refused(domain::BrokerError::SessionRequired)
    );
    let source = control.reader().snapshot()?;
    let checkpoint = machine.checkpoint()?;
    let expected =
        EncodedCommittedImage::encode(CommittedImageRole::CreateSendV1, stream()?, &source)?;
    let mut metadata = b"private-opaque-native-address\x00\xff".to_vec();
    metadata.resize(MAX_CATALOG_METADATA_BYTES, 7);
    control.reset();
    let token = machine.prepare_create_send_catalog()?;
    assert_capture(&control, 0);
    assert_eq!(token.image_bytes(), expected.as_bytes());
    assert_eq!(token.checkpoint(), &checkpoint);
    assert_eq!(
        token.checkpoint().highest_timestamp(),
        Timestamp::from_millis(50_000)
    );
    let pointer = token.image_bytes().as_ptr() as usize;
    assert!(!format!("{token:?}").contains("private-opaque-native-address"));
    // Metadata is owned independently before moving the token into retain.
    let image = token.retain(&metadata)?;
    assert_capture(&control, 1);
    assert_eq!(image.as_bytes().as_ptr() as usize, pointer);
    assert_eq!(image.as_bytes(), expected.as_bytes());
    let attempts = control.attempts();
    assert_eq!(attempts.len(), 1);
    assert!(attempts[0].business.is_empty());
    assert_eq!(attempts[0].metadata, metadata);
    assert_eq!(attempts[0].artifact, expected.as_bytes());
    assert_eq!(attempts[0].artifact_pointer, pointer);
    assert_eq!(control.reader().snapshot()?, source);
    assert_capture(&control, 1);
    control.reset();
    let retained = machine
        .read_create_send_catalog()?
        .ok_or("missing retained maximum image")?;
    assert_catalog_read(&control);
    assert_eq!(
        control.catalog_pointers(),
        vec![retained.image_bytes().as_ptr() as usize]
    );
    assert_eq!(retained.image_bytes(), image.as_bytes());
    assert_eq!(retained.metadata(), metadata);
    assert_eq!(retained.checkpoint(), &checkpoint);
    let checked =
        ValidatedCreateSendImage::validate(DecodedCommittedImage::decode(retained.image_bytes())?)?;
    assert_eq!((checked.queue_count(), checked.message_count()), (1, 1));
    assert!(!format!("{retained:?}").contains("private-opaque-native-address"));
    assert_eq!(control.reader().snapshot()?, source);
    assert_catalog_read(&control);
    Ok(())
}

fn absent_slot_dropped_token_and_opaque_membership_need_no_extra_source_reads<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let source = control.reader().snapshot()?;
    control.reset();
    assert!(machine.read_create_send_catalog()?.is_none());
    assert_catalog_read(&control);
    control.reset();
    let token = machine.prepare_create_send_catalog()?;
    assert!(token.checkpoint().last().is_none());
    drop(token);
    assert_capture(&control, 0);
    assert!(control.catalog()?.is_none());
    assert_eq!(control.reader().snapshot()?, source);
    let (_, refusal) = applied(apply(&mut machine, 0, &send(777, "missing", b"body")?)?)?;
    assert_eq!(
        refusal,
        CommittedApplication::Refused(domain::BrokerError::QueueNotFound)
    );
    applied(apply(
        &mut machine,
        1,
        &CommittedQueueWork::Membership {
            schema_version: 999,
            payload: b"private-opaque-membership-not-native".to_vec(),
        },
    )?)?;
    let checkpoint = machine.checkpoint()?;
    let source = control.reader().snapshot()?;
    control.reset();
    let token = machine.prepare_create_send_catalog()?;
    assert_eq!(token.checkpoint(), &checkpoint);
    assert!(!format!("{token:?}").contains("private-opaque-membership-not-native"));
    let image = token.retain(&[])?;
    assert_capture(&control, 1);
    assert_eq!(control.reader().snapshot()?, source);
    control.reset();
    let retained = machine
        .read_create_send_catalog()?
        .ok_or("missing opaque retained slot")?;
    assert_catalog_read(&control);
    assert_eq!(retained.image_bytes(), image.as_bytes());
    assert_eq!(retained.checkpoint(), &checkpoint);
    assert!(retained.metadata().is_empty());
    assert!(!format!("{retained:?}").contains("private-opaque-membership-not-native"));
    Ok(())
}

fn consumed_metadata_limit_token_never_commits_and_does_not_poison<W>(writer: W) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let source = control.reader().snapshot()?;
    control.reset();
    let token = machine.prepare_create_send_catalog()?;
    assert_eq!(
        token.retain(&vec![0; MAX_CATALOG_METADATA_BYTES + 1]).err(),
        Some(CommittedCatalogError::LimitExceeded)
    );
    assert_capture(&control, 0);
    assert!(control.attempts().is_empty());
    assert!(control.catalog()?.is_none());
    assert_eq!(control.reader().snapshot()?, source);
    control.reset();
    assert!(machine.read_create_send_catalog()?.is_none());
    assert_catalog_read(&control);
    control.reset();
    let image = machine
        .prepare_create_send_catalog()?
        .retain(b"healthy metadata")?;
    assert_capture(&control, 1);
    assert!(!image.is_empty());
    applied(apply(&mut machine, 0, &create(1, QueueConfig::default())?)?)?;
    Ok(())
}

fn retained_catalog_is_allowed_to_lag_after_later_apply_and_remains_owned<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    applied(apply(&mut machine, 0, &create(1, QueueConfig::default())?)?)?;
    applied(apply(&mut machine, 1, &send(2, "first", b"first body")?)?)?;
    let token = machine.prepare_create_send_catalog()?;
    let captured_checkpoint = token.checkpoint().clone();
    let image = token.retain(b"older metadata")?;
    let old = machine
        .read_create_send_catalog()?
        .ok_or("missing old slot")?;
    applied(apply(&mut machine, 2, &send(3, "second", b"second body")?)?)?;
    let current_checkpoint = machine.checkpoint()?;
    assert_ne!(current_checkpoint, captured_checkpoint);
    let current_rows = control.reader().snapshot()?;
    control.reset();
    let retained = machine
        .read_create_send_catalog()?
        .ok_or("missing preserved old slot")?;
    assert_catalog_read(&control);
    assert_eq!(retained.checkpoint(), &captured_checkpoint);
    assert_eq!(retained.image_bytes(), image.as_bytes());
    assert_eq!(retained.metadata(), b"older metadata");
    assert_eq!(
        control.catalog_pointers(),
        vec![retained.image_bytes().as_ptr() as usize]
    );
    assert_eq!(control.reader().snapshot()?, current_rows);
    assert_catalog_read(&control);
    let newer = machine
        .prepare_create_send_catalog()?
        .retain(b"newer metadata")?;
    assert_ne!(newer.as_bytes(), image.as_bytes());
    assert_eq!(old.image_bytes(), image.as_bytes());
    assert_eq!(old.checkpoint(), &captured_checkpoint);
    Ok(())
}

fn capture_and_catalog_quota_or_allocation_refusals_are_nonfatal_without_fallback<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let image = machine.prepare_create_send_catalog()?.retain(b"valid")?;
    let source = control.reader().snapshot()?;
    control.reset();
    control.fault(Fault::CaptureLimit);
    assert_eq!(
        machine.prepare_create_send_catalog().err(),
        Some(CommittedCatalogError::LimitExceeded)
    );
    assert_capture(&control, 0);
    for (fault, error) in [
        (Fault::CatalogLimit, CommittedCatalogError::LimitExceeded),
        (
            Fault::CatalogNestedLimit,
            CommittedCatalogError::LimitExceeded,
        ),
        (Fault::CatalogAllocation, CommittedCatalogError::Allocation),
    ] {
        control.reset();
        control.fault(fault);
        assert_eq!(machine.read_create_send_catalog().err(), Some(error));
        assert_catalog_read(&control);
        assert_eq!(control.reader().snapshot()?, source);
        control.reset();
        assert_eq!(
            machine
                .read_create_send_catalog()?
                .ok_or("missing healthy slot")?
                .image_bytes(),
            image.as_bytes()
        );
        assert_catalog_read(&control);
    }
    // This is an actual too-large business value, not an injected read error.
    let key = b"\x7foversized-capture".to_vec();
    control.inject_business(
        WriteBatch::default().put(key.clone(), vec![0; MAX_COMMITTED_IMAGE_VALUE_BYTES + 1]),
    )?;
    control.reset();
    assert_eq!(
        machine.prepare_create_send_catalog().err(),
        Some(CommittedCatalogError::LimitExceeded)
    );
    assert_capture(&control, 0);
    control.inject_business(WriteBatch::default().delete(key))?;
    control.reset();
    assert!(
        machine
            .prepare_create_send_catalog()?
            .retain(b"healthy")
            .is_ok()
    );
    assert_capture(&control, 1);
    applied(apply(&mut machine, 0, &create(1, QueueConfig::default())?)?)?;
    Ok(())
}

fn physical_capture_or_catalog_read_errors_poison_all_mutating_catalog_work_before_io<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    machine.prepare_create_send_catalog()?.retain(b"valid")?;
    let source = control.reader().snapshot()?;
    let checkpoint = machine.checkpoint()?;
    let request = update(&machine, 0)?;
    let work = create(1, QueueConfig::default())?;
    for fault in [
        Fault::CapturePhysical,
        Fault::CatalogPhysical,
        Fault::CatalogCorrupt,
    ] {
        control.reset();
        control.fault(fault);
        let error = if matches!(fault, Fault::CapturePhysical) {
            let error = machine
                .prepare_create_send_catalog()
                .err()
                .ok_or("missing capture error")?;
            assert_capture(&control, 0);
            error
        } else {
            let error = machine
                .read_create_send_catalog()
                .err()
                .ok_or("missing catalog error")?;
            assert_catalog_read(&control);
            error
        };
        assert_eq!(error, CommittedCatalogError::ReadFailed);
        assert!(!format!("{error:?}: {error}").contains("private-"));
        assert!(std::error::Error::source(&error).is_none());
        let counts = control.counts();
        assert_eq!(
            machine.prepare_create_send_catalog().err(),
            Some(CommittedCatalogError::Poisoned)
        );
        assert_eq!(
            machine.read_create_send_catalog().err(),
            Some(CommittedCatalogError::Poisoned)
        );
        assert_eq!(
            machine.export_create_send_image().err(),
            Some(CommittedImageExportError::Poisoned)
        );
        assert_eq!(
            machine.apply_committed(&request, &work),
            Err(CommittedApplyError::Poisoned)
        );
        assert_eq!(control.counts(), counts);
        assert_eq!(control.reader().snapshot()?, source);
        control.reset();
        assert_eq!(
            machine.checkpoint()?,
            checkpoint,
            "diagnostic checkpoint behavior remains unchanged"
        );
        assert_eq!(
            control.counts(),
            Counts {
                gets: 1,
                initialized: 1,
                ..Counts::default()
            }
        );
        drop(machine);
        machine = CommittedStateMachine::open(control.recover_writer(), stream()?)?;
        control.reset();
        assert!(machine.read_create_send_catalog()?.is_some());
        assert_catalog_read(&control);
    }
    applied(machine.apply_committed(&request, &work)?)?;
    Ok(())
}

fn mutated_artifact(
    source: &StoreSnapshot,
    mutation: WriteBatch,
) -> TestResult<EncodedCommittedImage> {
    let temporary = MemoryStore::default();
    let mut batch = WriteBatch::default();
    for (key, value) in source.entries() {
        batch.push_put(key.clone(), value.clone());
    }
    temporary.apply(batch)?;
    temporary.apply(mutation)?;
    Ok(EncodedCommittedImage::encode(
        CommittedImageRole::CreateSendV1,
        stream()?,
        &temporary.snapshot()?,
    )?)
}

fn arbitrary_opaque_catalog_artifact_refusals_are_nonfatal_not_certified_corruption<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    applied(apply(&mut machine, 0, &create(1, QueueConfig::default())?)?)?;
    applied(apply(
        &mut machine,
        1,
        &send(2, "first", b"private-message-body")?,
    )?)?;
    let source = control.reader().snapshot()?;
    let good = machine
        .prepare_create_send_catalog()?
        .retain(b"valid metadata")?;
    let message_key = keys::message(&namespace()?, &entity()?, SequenceNumber::new(1));
    let ready_key = keys::ready(&namespace()?, &entity()?, SequenceNumber::new(1));
    let message = source
        .entries()
        .iter()
        .find(|(key, _)| key == &message_key)
        .ok_or("missing fixture message")?
        .1
        .clone();
    let mut legacy = message.clone();
    legacy[0] = 10;
    assert_eq!(
        domain::codec::decode::<domain::MessageRecord>(&legacy)?,
        domain::codec::decode::<domain::MessageRecord>(&message)?
    );
    let legacy = mutated_artifact(
        &source,
        WriteBatch::default().put(message_key.clone(), legacy),
    )?;
    let missing_ready = mutated_artifact(&source, WriteBatch::default().delete(ready_key))?;
    let unknown = mutated_artifact(
        &source,
        WriteBatch::default().put(b"\x7funknown-family", b"private-source-value"),
    )?;
    let mut trailing = message;
    trailing.push(0);
    let malformed = mutated_artifact(&source, WriteBatch::default().put(message_key, trailing))?;
    let mut checksum = good.as_bytes().to_vec();
    *checksum.last_mut().ok_or("missing checksum")? ^= 1;
    let mut other = CommittedStateMachine::create(
        MemoryCatalogReplicaStore::new(),
        CommittedStreamId::new([8; 16])?,
    )?;
    let wrong_stream = other.export_create_send_image()?;
    for (artifact, expected) in [
        (legacy.as_bytes(), CommittedCatalogError::UnsupportedProfile),
        (
            missing_ready.as_bytes(),
            CommittedCatalogError::InvalidImage,
        ),
        (
            unknown.as_bytes(),
            CommittedCatalogError::UnsupportedProfile,
        ),
        (malformed.as_bytes(), CommittedCatalogError::InvalidImage),
        (checksum.as_slice(), CommittedCatalogError::InvalidImage),
        (wrong_stream.as_bytes(), CommittedCatalogError::WrongStream),
    ] {
        control.inject_catalog(b"private-native-opaque-token", artifact)?;
        // Ordinary open validates live progress, not this arbitrary retained role.
        drop(machine);
        machine = CommittedStateMachine::open(control.recover_writer(), stream()?)?;
        control.reset();
        let error = machine
            .read_create_send_catalog()
            .err()
            .ok_or("expected retained source refusal")?;
        assert_eq!(error, expected);
        assert!(!format!("{error:?}: {error}").contains("private-"));
        assert_catalog_read(&control);
        assert_eq!(control.reader().snapshot()?, source);
        assert_eq!(
            control
                .catalog()?
                .ok_or("raw arbitrary source vanished")?
                .artifact(),
            artifact
        );
        control.inject_catalog(b"valid metadata", good.as_bytes())?;
        control.reset();
        assert_eq!(
            machine
                .read_create_send_catalog()?
                .ok_or("healthy slot refused after nonfatal error")?
                .image_bytes(),
            good.as_bytes()
        );
        assert_catalog_read(&control);
    }
    applied(apply(
        &mut machine,
        2,
        &send(3, "next", b"healthy after semantic refusals")?,
    )?)?;
    Ok(())
}

fn preparation_semantic_or_checkpoint_refusals_leave_catalog_and_machine_healthy<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    applied(apply(&mut machine, 0, &create(1, QueueConfig::default())?)?)?;
    applied(apply(&mut machine, 1, &send(2, "first", b"original")?)?)?;
    let source = control.reader().snapshot()?;
    let retained = machine
        .prepare_create_send_catalog()?
        .retain(b"preserved slot")?;
    let message_key = keys::message(&namespace()?, &entity()?, SequenceNumber::new(1));
    let ready_key = keys::ready(&namespace()?, &entity()?, SequenceNumber::new(1));
    let (checkpoint_key, checkpoint_value) = source
        .entries()
        .iter()
        .find(|(key, _)| key.as_slice() == [0x12])
        .cloned()
        .ok_or("missing checkpoint")?;
    let message = source
        .entries()
        .iter()
        .find(|(key, _)| key == &message_key)
        .ok_or("missing message")?
        .1
        .clone();
    let mut legacy = message.clone();
    legacy[0] = 10;
    assert_eq!(
        domain::codec::decode::<domain::MessageRecord>(&legacy)?,
        domain::codec::decode::<domain::MessageRecord>(&message)?
    );
    for (mutation, error) in [
        (
            WriteBatch::default().put(message_key.clone(), legacy),
            CommittedCatalogError::UnsupportedProfile,
        ),
        (
            WriteBatch::default().delete(ready_key.clone()),
            CommittedCatalogError::InvalidImage,
        ),
        (
            WriteBatch::default().put(checkpoint_key.clone(), vec![0]),
            CommittedCatalogError::InvalidImage,
        ),
    ] {
        control.inject_business(mutation)?;
        let mutated = control.reader().snapshot()?;
        control.reset();
        assert_eq!(machine.prepare_create_send_catalog().err(), Some(error));
        assert_capture(&control, 0);
        assert_eq!(control.reader().snapshot()?, mutated);
        assert_eq!(
            control
                .catalog()?
                .ok_or("retained slot was changed by preparation failure")?
                .artifact(),
            retained.as_bytes()
        );
        control.inject_business(
            WriteBatch::default()
                .put(message_key.clone(), message.clone())
                .put(ready_key.clone(), Vec::new())
                .put(checkpoint_key.clone(), checkpoint_value.clone()),
        )?;
        control.reset();
        drop(machine.prepare_create_send_catalog()?);
        assert_capture(&control, 0);
        assert_eq!(control.reader().snapshot()?, source);
    }
    applied(apply(&mut machine, 2, &send(3, "next", b"healthy")?)?)?;
    Ok(())
}

fn every_catalog_commit_error_is_unknown_poisoning_even_limit_or_profile_refusals<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    applied(apply(&mut machine, 0, &create(1, QueueConfig::default())?)?)?;
    let good = machine
        .prepare_create_send_catalog()?
        .retain(b"original slot")?;
    let source = control.reader().snapshot()?;
    let request = update(&machine, 1)?;
    let work = send(2, "next", b"next")?;
    for fault in [
        Fault::CommitBefore,
        Fault::CommitAfter,
        Fault::CommitLimit,
        Fault::CommitCorrupt,
    ] {
        control.reset();
        let token = machine.prepare_create_send_catalog()?;
        control.fault(fault);
        let error = token
            .retain(b"attempted opaque metadata")
            .err()
            .ok_or("expected unknown commit decision")?;
        assert_eq!(error, CommittedCatalogError::CommitUnknown);
        assert!(!format!("{error:?}: {error}").contains("private-"));
        assert!(std::error::Error::source(&error).is_none());
        assert_capture(&control, 1);
        let attempts = control.attempts();
        assert_eq!(attempts.len(), 1);
        assert!(attempts[0].business.is_empty());
        assert_eq!(attempts[0].artifact, good.as_bytes());
        assert_eq!(control.reader().snapshot()?, source);
        let actual = control
            .catalog()?
            .ok_or("initial retained catalog disappeared")?;
        assert_eq!(
            actual.metadata(),
            if matches!(fault, Fault::CommitAfter) {
                b"attempted opaque metadata".as_slice()
            } else {
                b"original slot".as_slice()
            }
        );
        let counts = control.counts();
        assert_eq!(
            machine.prepare_create_send_catalog().err(),
            Some(CommittedCatalogError::Poisoned)
        );
        assert_eq!(
            machine.read_create_send_catalog().err(),
            Some(CommittedCatalogError::Poisoned)
        );
        assert_eq!(
            machine.apply_committed(&request, &work),
            Err(CommittedApplyError::Poisoned)
        );
        assert_eq!(control.counts(), counts);
        drop(machine);
        machine = CommittedStateMachine::open(control.recover_writer(), stream()?)?;
        control.inject_catalog(b"original slot", good.as_bytes())?;
    }
    applied(machine.apply_committed(&request, &work)?)?;
    Ok(())
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
    capture_and_retention_preserve_the_original_max_body_image_and_exact_empty_batch,
    absent_slot_dropped_token_and_opaque_membership_need_no_extra_source_reads,
    consumed_metadata_limit_token_never_commits_and_does_not_poison,
    retained_catalog_is_allowed_to_lag_after_later_apply_and_remains_owned,
    capture_and_catalog_quota_or_allocation_refusals_are_nonfatal_without_fallback,
    physical_capture_or_catalog_read_errors_poison_all_mutating_catalog_work_before_io,
    arbitrary_opaque_catalog_artifact_refusals_are_nonfatal_not_certified_corruption,
    preparation_semantic_or_checkpoint_refusals_leave_catalog_and_machine_healthy,
    every_catalog_commit_error_is_unknown_poisoning_even_limit_or_profile_refusals,
);
