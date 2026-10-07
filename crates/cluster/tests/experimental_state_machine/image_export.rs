use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};

use cluster::{
    ExperimentalStateMachine, LogApplication, LogQueueRefusal, QueueLogCommand, StateMachineError,
    StateMachineImageExportError,
};
use domain::{
    CommittedCheckpointUpdate, CommittedEntryId, CommittedImageExportError, CommittedImageRole,
    CommittedQueueWork, CommittedSend, CommittedStateMachine, DecodedCommittedImage,
    EncodedCommittedImage, MAX_COMMITTED_BODY_BYTES, MAX_COMMITTED_IMAGE_BYTES,
    MAX_COMMITTED_IMAGE_KEY_BYTES, MAX_COMMITTED_IMAGE_ROWS, MAX_COMMITTED_IMAGE_VALUE_BYTES,
    QueueConfig, SequenceNumber, SessionId, Timestamp, ValidatedCreateSendLayout17Image, keys,
};
use openraft::{EntryPayload, storage::RaftStateMachine};
use storage::{
    BoundedStateStore, CommittedStore, FjallReplicaStore, MemoryReplicaStore, ReadLimits,
    StateStore, StoreSnapshot, WriteBatch,
};

use super::{
    DEADLINE, TestResult,
    fixture::{create, entity, id, membership, namespace, send, stream},
};

#[path = "image_export/observed.rs"]
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

fn one_capture<W: CommittedStore>(control: &observed::Control<W>) {
    assert_eq!(
        control.counts(),
        Counts {
            bounded: 1,
            ..Counts::default()
        }
    );
    assert_eq!(control.limits(), vec![limits()]);
}

fn check_image(image: &EncodedCommittedImage, source: &StoreSnapshot) -> TestResult {
    let expected =
        EncodedCommittedImage::encode(CommittedImageRole::CreateSendLayout17V1, stream()?, source)?;
    assert_eq!(image.as_bytes(), expected.as_bytes());
    let checked = ValidatedCreateSendLayout17Image::validate(DecodedCommittedImage::decode(
        image.as_bytes(),
    )?)?;
    assert_eq!(
        checked
            .rows()
            .map(|row| (row.key().to_vec(), row.value().to_vec()))
            .collect::<Vec<_>>(),
        source.entries()
    );
    Ok(())
}

async fn finish(machine: ExperimentalStateMachine, result: TestResult) -> TestResult {
    let joined = machine.shutdown().await;
    result?;
    joined?;
    Ok(())
}

fn owned<F: Future + Send + 'static>(_: &F) {}

async fn first_poll<F: Future + ?Sized>(mut future: Pin<&mut F>) -> Option<F::Output> {
    poll_fn(|cx| {
        Poll::Ready(match future.as_mut().poll(cx) {
            Poll::Pending => None,
            Poll::Ready(result) => Some(result),
        })
    })
    .await
}

async fn release_and_join<F>(mut shutdown: Pin<&mut F>, gate: &observed::GateGuard) -> TestResult
where
    F: Future<Output = Result<(), StateMachineError>>,
{
    let early = first_poll(shutdown.as_mut()).await;
    gate.release();
    match early {
        None => {
            shutdown.await?;
            Ok(())
        }
        Some(result) => {
            result?;
            Err("the native owner joined before its captured read was released".into())
        }
    }
}

async fn old_constructors_leave_export_disabled_without_any_source_io<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create(writer, stream()?)?;
    let result = async {
        control.reset();
        assert_eq!(
            machine.export_create_send_image().await.err(),
            Some(StateMachineImageExportError::Disabled)
        );
        assert_eq!(control.counts(), Counts::default());
        machine.apply([membership(0)]).await?;
        control.reset();
        assert_eq!(
            machine.export_create_send_image().await.err(),
            Some(StateMachineImageExportError::Disabled)
        );
        assert_eq!(control.counts(), Counts::default());
        Ok(())
    }
    .await;
    finish(machine, result).await?;
    let mut machine = ExperimentalStateMachine::open(control.writer(), stream()?)?;
    let result = async {
        control.reset();
        assert_eq!(
            machine.export_create_send_image().await.err(),
            Some(StateMachineImageExportError::Disabled)
        );
        assert_eq!(control.counts(), Counts::default());
        Ok(())
    }
    .await;
    finish(machine, result).await?;
    let mut machine =
        ExperimentalStateMachine::open_with_image_export(control.writer(), stream()?)?;
    let result = async {
        let source = control.reader().snapshot()?;
        control.reset();
        let image = machine.export_create_send_image().await?;
        one_capture(&control);
        check_image(&image, &source)
    }
    .await;
    finish(machine, result).await
}

fn session_send(
    index: u64,
    time: u64,
    session: Option<SessionId>,
    body: Vec<u8>,
) -> TestResult<cluster::LogEntry> {
    Ok(cluster::LogEntry {
        log_id: id(1, index),
        payload: EntryPayload::Normal(QueueLogCommand::send(
            namespace()?,
            entity()?,
            Timestamp::from_millis(time),
            CommittedSend {
                message_id: "\u{0800}".repeat(domain::MAX_MESSAGE_ID_LENGTH),
                body,
                time_to_live_millis: Some(123),
                session_id: session,
            },
        )),
    })
}

async fn native_export_preserves_max_body_membership_and_refusal_watermark<W>(
    writer: W,
) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create_with_image_export(writer, stream()?)?;
    let result = async {
        let config = QueueConfig {
            max_message_bytes: MAX_COMMITTED_BODY_BYTES,
            default_time_to_live_millis: Some(1000),
            requires_session: true,
            requires_duplicate_detection: true,
            duplicate_detection_history_time_window_millis: 20_000,
            ..QueueConfig::default()
        };
        let session = SessionId::new("s".repeat(domain::MAX_SESSION_ID_BYTES))?;
        let body = (0..MAX_COMMITTED_BODY_BYTES)
            .map(|offset| (offset % 251) as u8)
            .collect::<Vec<_>>();
        assert_eq!(
            machine
                .apply([
                    membership(0),
                    create(1, 100, config)?,
                    session_send(2, 110, Some(session.clone()), body.clone())?,
                    session_send(3, 111, Some(session), b"duplicate".to_vec())?,
                    session_send(4, 50_000, None, b"refused".to_vec())?,
                ])
                .await?,
            vec![
                LogApplication::CheckpointOnly,
                LogApplication::QueueCreated,
                LogApplication::Sent { sequence: 1 },
                LogApplication::Sent { sequence: 2 },
                LogApplication::Refused(LogQueueRefusal::SessionRequired),
            ]
        );
        let source = control.reader().snapshot()?;
        control.reset();
        let image = machine.export_create_send_image().await?;
        one_capture(&control);
        assert_eq!(machine.workload()?.accepted_jobs, 0);
        assert_eq!(machine.workload()?.encoded_bytes, 0);
        check_image(&image, &source)?;
        let checked = ValidatedCreateSendLayout17Image::validate(DecodedCommittedImage::decode(
            image.as_bytes(),
        )?)?;
        assert_eq!((checked.queue_count(), checked.message_count()), (1, 1));
        assert_eq!(
            checked.checkpoint().last().map(|mark| mark.id),
            Some(CommittedEntryId {
                term: 1,
                node_id: 7,
                index: 4
            })
        );
        assert_eq!(
            checked.checkpoint().highest_timestamp(),
            Timestamp::from_millis(50_000)
        );
        let member = checked
            .checkpoint()
            .membership()
            .ok_or("missing captured membership")?;
        assert_eq!(member.schema_version, 1);
        assert_eq!(
            member.source,
            CommittedEntryId {
                term: 1,
                node_id: 7,
                index: 0
            }
        );
        let row = domain::StateMachine::new(control.reader())
            .message(&namespace()?, &entity()?, SequenceNumber::new(1))?
            .ok_or("missing actual maximum body")?;
        assert_eq!(row.body, body);
        assert_eq!(control.reader().snapshot()?, source);
        one_capture(&control);
        Ok(())
    }
    .await;
    finish(machine, result).await
}

async fn conservative_export_refusals_leave_native_owner_healthy<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create_with_image_export(writer, stream()?)?;
    let result = async {
        machine
            .apply([
                create(0, 1, QueueConfig::default())?,
                send(1, 2, b"original".to_vec())?,
            ])
            .await?;
        let baseline = control.reader().snapshot()?;
        control.reset();
        control.fault(Fault::ReadLimit);
        assert_eq!(
            machine.export_create_send_image().await.err(),
            Some(StateMachineImageExportError::Domain(
                CommittedImageExportError::LimitExceeded
            ))
        );
        one_capture(&control);
        let extra = b"\x7fextra".to_vec();
        control.inject(
            WriteBatch::default().put(extra.clone(), vec![0; MAX_COMMITTED_IMAGE_VALUE_BYTES + 1]),
        )?;
        control.reset();
        assert_eq!(
            machine.export_create_send_image().await.err(),
            Some(StateMachineImageExportError::Domain(
                CommittedImageExportError::LimitExceeded
            ))
        );
        one_capture(&control);
        control.inject(WriteBatch::default().delete(extra))?;
        let message_key = keys::message(&namespace()?, &entity()?, SequenceNumber::new(1));
        let ready_key = keys::ready(&namespace()?, &entity()?, SequenceNumber::new(1));
        let original = baseline
            .entries()
            .iter()
            .find(|(key, _)| *key == message_key)
            .ok_or("missing source message")?
            .1
            .clone();
        let mut legacy = original.clone();
        legacy[0] = 10;
        let mut trailing = original.clone();
        trailing.push(0);
        for (batch, expected) in [
            (
                WriteBatch::default().put(message_key.clone(), legacy),
                CommittedImageExportError::UnsupportedProfile,
            ),
            (
                WriteBatch::default().put(message_key.clone(), trailing),
                CommittedImageExportError::InvalidImage,
            ),
            (
                WriteBatch::default().delete(ready_key.clone()),
                CommittedImageExportError::InvalidImage,
            ),
        ] {
            control.inject(batch)?;
            let changed = control.reader().snapshot()?;
            control.reset();
            assert_eq!(
                machine.export_create_send_image().await.err(),
                Some(StateMachineImageExportError::Domain(expected))
            );
            one_capture(&control);
            assert_eq!(control.reader().snapshot()?, changed);
            control.inject(
                WriteBatch::default()
                    .put(message_key.clone(), original.clone())
                    .put(ready_key.clone(), Vec::new()),
            )?;
            control.reset();
            let image = machine.export_create_send_image().await?;
            one_capture(&control);
            check_image(&image, &baseline)?;
        }
        assert_eq!(
            machine.apply([send(2, 3, b"healthy".to_vec())?]).await?,
            vec![LogApplication::Sent { sequence: 2 }]
        );
        Ok(())
    }
    .await;
    finish(machine, result).await
}

fn opaque_checkpoint() -> TestResult<Vec<u8>> {
    let mut machine = CommittedStateMachine::create(MemoryReplicaStore::new(), stream()?)?;
    machine.apply_committed(
        &CommittedCheckpointUpdate {
            stream: stream()?,
            expected_previous: None,
            entry: CommittedEntryId {
                term: 1,
                node_id: 7,
                index: 0,
            },
        },
        &CommittedQueueWork::Membership {
            schema_version: 999,
            payload: b"opaque-native-incompatible".to_vec(),
        },
    )?;
    machine
        .reader()
        .get(&[0x12])?
        .ok_or_else(|| "missing opaque checkpoint".into())
}

async fn native_membership_incompatibility_is_nonfatal_and_source_private<W>(
    writer: W,
) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create_with_image_export(writer, stream()?)?;
    let result = async {
        let baseline = control.reader().snapshot()?;
        let original = control
            .reader()
            .get(&[0x12])?
            .ok_or("missing initial checkpoint")?;
        control.inject(WriteBatch::default().put(vec![0x12], opaque_checkpoint()?))?;
        let changed = control.reader().snapshot()?;
        control.reset();
        let error = machine
            .export_create_send_image()
            .await
            .err()
            .ok_or("expected native metadata refusal")?;
        assert_eq!(error, StateMachineImageExportError::IncompatibleMetadata);
        assert!(!format!("{error:?}: {error}").contains("opaque-native-incompatible"));
        one_capture(&control);
        assert_eq!(control.reader().snapshot()?, changed);
        control.inject(WriteBatch::default().put(vec![0x12], original))?;
        control.reset();
        check_image(&machine.export_create_send_image().await?, &baseline)?;
        one_capture(&control);
        assert_eq!(
            machine.apply([membership(0)]).await?,
            vec![LogApplication::CheckpointOnly]
        );
        Ok(())
    }
    .await;
    finish(machine, result).await
}

async fn physical_capture_failure_poisons_before_later_export_and_trait_queries<W>(
    writer: W,
) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create_with_image_export(writer, stream()?)?;
    let result = async {
        let source = control.reader().snapshot()?;
        control.reset();
        control.fault(Fault::ReadPhysical);
        let error = machine
            .export_create_send_image()
            .await
            .err()
            .ok_or("expected bounded read failure")?;
        assert_eq!(
            error,
            StateMachineImageExportError::Domain(CommittedImageExportError::ReadFailed)
        );
        assert!(!format!("{error:?}: {error}").contains("SECRET"));
        one_capture(&control);
        assert_eq!(control.reader().snapshot()?, source);
        control.reset();
        assert_eq!(
            machine.export_create_send_image().await.err(),
            Some(StateMachineImageExportError::Owner(
                StateMachineError::Poisoned
            ))
        );
        assert!(machine.applied_state().await.is_err());
        assert!(machine.get_current_snapshot().await.is_err());
        assert!(
            machine
                .apply([create(0, 1, QueueConfig::default())?])
                .await
                .is_err()
        );
        assert_eq!(control.counts(), Counts::default());
        assert_eq!(control.reader().snapshot()?, source);
        Ok(())
    }
    .await;
    finish(machine, result).await?;
    let mut reopened =
        ExperimentalStateMachine::open_with_image_export(control.writer(), stream()?)?;
    let result = async {
        control.reset();
        reopened.export_create_send_image().await?;
        one_capture(&control);
        Ok(())
    }
    .await;
    finish(reopened, result).await
}

async fn prior_physical_commit_errors_refuse_export_before_capture<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create_with_image_export(writer, stream()?)?;
    let result = async {
        machine
            .apply([create(0, 1, QueueConfig::default())?])
            .await?;
        control.fault(Fault::CommitBefore);
        assert!(
            machine
                .apply([send(1, 2, b"first".to_vec())?])
                .await
                .is_err()
        );
        let source = control.reader().snapshot()?;
        control.reset();
        assert_eq!(
            machine.export_create_send_image().await.err(),
            Some(StateMachineImageExportError::Owner(
                StateMachineError::Poisoned
            ))
        );
        assert_eq!(control.counts(), Counts::default());
        assert_eq!(control.reader().snapshot()?, source);
        Ok(())
    }
    .await;
    finish(machine, result).await?;
    let mut machine =
        ExperimentalStateMachine::open_with_image_export(control.writer(), stream()?)?;
    let result = async {
        machine.apply([send(1, 2, b"first".to_vec())?]).await?;
        control.fault(Fault::CommitAfter);
        assert!(
            machine
                .apply([send(2, 3, b"second".to_vec())?])
                .await
                .is_err()
        );
        let source = control.reader().snapshot()?;
        let actual = domain::StateMachine::new(control.reader())
            .message(&namespace()?, &entity()?, SequenceNumber::new(2))?
            .ok_or("missing real post-error commitment")?;
        assert_eq!(actual.body, b"second");
        control.reset();
        assert_eq!(
            machine.export_create_send_image().await.err(),
            Some(StateMachineImageExportError::Owner(
                StateMachineError::Poisoned
            ))
        );
        assert_eq!(control.counts(), Counts::default());
        assert_eq!(control.reader().snapshot()?, source);
        Ok(())
    }
    .await;
    finish(machine, result).await
}

async fn capture_panic_refunds_then_retires_and_joins_before_healthy_reopen<W>(
    writer: W,
) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create_with_image_export(writer, stream()?)?;
    let setup: TestResult<_> = async {
        machine
            .apply([
                create(0, 1, QueueConfig::default())?,
                send(1, 2, b"panic-retained-original".to_vec())?,
            ])
            .await?;
        Ok(control.reader().snapshot()?)
    }
    .await;
    let source = match setup {
        Ok(source) => source,
        Err(error) => return finish(machine, Err(error)).await,
    };
    control.reset();
    let inert = machine.export_create_send_image();
    let gate = control.gate();
    control.fault(Fault::ReadPanic);
    let mut capture = Box::pin(machine.export_create_send_image());
    let mut early = None;
    let observations: TestResult = async {
        early = first_poll(capture.as_mut()).await;
        if early.is_some() {
            return Err("the panic export completed before its actual capture gate".into());
        }
        gate.entered().await?;
        assert_eq!(machine.workload()?.accepted_jobs, 1);
        assert_eq!(machine.workload()?.encoded_bytes, MAX_COMMITTED_IMAGE_BYTES);
        one_capture(&control);
        assert_eq!(control.reader().snapshot()?, source);
        Ok(())
    }
    .await;
    gate.release();
    let captured = match early {
        Some(result) => result,
        None => capture.await,
    };
    let observed_completion: TestResult = async {
        observations?;
        let error = captured.err().ok_or("expected the owner panic reply")?;
        assert_eq!(
            error,
            StateMachineImageExportError::Owner(StateMachineError::Panicked)
        );
        assert!(!format!("{error:?}: {error}").contains("injected bounded image capture panic"));
        assert_eq!(machine.workload()?.accepted_jobs, 0);
        assert_eq!(machine.workload()?.encoded_bytes, 0);
        one_capture(&control);
        Ok(())
    }
    .await;
    // Packet Drop publishes/refunds before the catch closes admission. The
    // caller's shutdown can therefore establish Closed before that catch; the
    // real native thread join is still unconditionally Panicked.
    let joined = machine.shutdown().await;
    observed_completion?;
    assert_eq!(joined, Err(StateMachineError::Panicked));
    match inert.await {
        Err(StateMachineImageExportError::Owner(
            StateMachineError::Panicked | StateMachineError::Closed,
        )) => {}
        result => {
            return Err(
                format!("unexpected inert export result after panic/join: {result:?}").into(),
            );
        }
    }
    one_capture(&control);
    assert_eq!(control.reader().snapshot()?, source);
    let mut reopened =
        ExperimentalStateMachine::open_with_image_export(control.writer(), stream()?)?;
    let result = async {
        control.reset();
        let image = reopened.export_create_send_image().await?;
        check_image(&image, &source)?;
        one_capture(&control);
        assert_eq!(control.reader().snapshot()?, source);
        Ok(())
    }
    .await;
    finish(reopened, result).await
}

async fn native_metadata_comes_only_from_the_actual_captured_view<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create_with_image_export(writer, stream()?)?;
    let setup: TestResult<_> = async {
        machine.apply([membership(0)]).await?;
        Ok((
            control.reader().snapshot()?,
            control
                .reader()
                .get(&[0x12])?
                .ok_or("missing canonical checkpoint")?,
        ))
    }
    .await;
    let (source, original) = match setup {
        Ok(value) => value,
        Err(error) => return finish(machine, Err(error)).await,
    };
    control.reset();
    let gate = control.gate();
    let mut capture = Box::pin(machine.export_create_send_image());
    let mut early = None;
    let observations: TestResult = async {
        early = first_poll(capture.as_mut()).await;
        if early.is_some() {
            return Err("the export completed before its actual capture gate".into());
        }
        gate.entered().await?;
        control.inject(WriteBatch::default().put(vec![0x12], opaque_checkpoint()?))?;
        one_capture(&control);
        Ok(())
    }
    .await;
    gate.release();
    let captured = match early {
        Some(result) => result,
        None => capture.await,
    };
    let restored = control.inject(WriteBatch::default().put(vec![0x12], original));
    let result = async {
        observations?;
        restored?;
        check_image(&captured?, &source)?;
        one_capture(&control);
        assert_eq!(control.reader().snapshot()?, source);
        Ok(())
    }
    .await;
    finish(machine, result).await
}

async fn lost_export_waiter_retains_full_charge_and_actual_capture_until_join<W>(
    writer: W,
) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create_with_image_export(writer, stream()?)?;
    let source = match control.reader().snapshot() {
        Ok(source) => source,
        Err(error) => return finish(machine, Err(error.into())).await,
    };
    control.reset();
    let gate = control.gate();
    let mut capture = Box::pin(machine.export_create_send_image());
    owned(&capture);
    let observations: TestResult = async {
        if first_poll(capture.as_mut()).await.is_some() {
            return Err("the export completed before its actual capture gate".into());
        }
        gate.entered().await?;
        drop(capture);
        assert_eq!(machine.workload()?.accepted_jobs, 1);
        assert_eq!(machine.workload()?.encoded_bytes, MAX_COMMITTED_IMAGE_BYTES);
        assert_eq!(
            machine.export_create_send_image().await.err(),
            Some(StateMachineImageExportError::Owner(StateMachineError::Busy))
        );
        one_capture(&control);
        assert_eq!(control.reader().snapshot()?, source);
        Ok(())
    }
    .await;
    let mut shutdown = Box::pin(machine.shutdown());
    let joined = release_and_join(shutdown.as_mut(), &gate).await;
    observations?;
    joined?;
    one_capture(&control);
    assert_eq!(control.reader().snapshot()?, source);
    Ok(())
}

async fn an_unpolled_export_factory_neither_reads_nor_retains_the_native_owner<W>(
    writer: W,
) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::create_with_image_export(writer, stream()?)?;
    control.reset();
    let capture = machine.export_create_send_image();
    owned(&capture);
    let observations: TestResult = async {
        assert_eq!(machine.workload()?.accepted_jobs, 0);
        assert_eq!(control.counts(), Counts::default());
        Ok(())
    }
    .await;
    let joined = machine.shutdown().await;
    observations?;
    joined?;
    assert_eq!(control.counts(), Counts::default());
    assert_eq!(
        capture.await.err(),
        Some(StateMachineImageExportError::Owner(
            StateMachineError::Closed
        ))
    );
    assert_eq!(control.counts(), Counts::default());
    Ok(())
}

// No timeout drops a whole owning scenario or a native I/O operation. The
// asynchronous gate wait has its own deadline and always releases before join.
macro_rules! for_each_image_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory {
            $(
                #[tokio::test]
                async fn $case() -> crate::TestResult {
                    super::$case(storage::MemoryReplicaStore::new()).await
                }
            )+
        }
        mod durable {
            $(
                #[tokio::test]
                async fn $case() -> crate::TestResult {
                    let directory = testkit::DurableProvider::temporary()?;
                    super::$case(storage::FjallReplicaStore::open(directory.path())?).await
                }
            )+
        }
    };
}

for_each_image_backend!(
    old_constructors_leave_export_disabled_without_any_source_io,
    native_export_preserves_max_body_membership_and_refusal_watermark,
    conservative_export_refusals_leave_native_owner_healthy,
    native_membership_incompatibility_is_nonfatal_and_source_private,
    physical_capture_failure_poisons_before_later_export_and_trait_queries,
    prior_physical_commit_errors_refuse_export_before_capture,
    capture_panic_refunds_then_retires_and_joins_before_healthy_reopen,
    native_metadata_comes_only_from_the_actual_captured_view,
    lost_export_waiter_retains_full_charge_and_actual_capture_until_join,
    an_unpolled_export_factory_neither_reads_nor_retains_the_native_owner,
);

#[test]
fn image_export_error_metadata_is_flat_static_and_source_private() {
    for error in [
        StateMachineImageExportError::Disabled,
        StateMachineImageExportError::IncompatibleMetadata,
        StateMachineImageExportError::Owner(StateMachineError::Closed),
        StateMachineImageExportError::Owner(StateMachineError::Busy),
        StateMachineImageExportError::Domain(CommittedImageExportError::ReadFailed),
        StateMachineImageExportError::Domain(CommittedImageExportError::Allocation),
    ] {
        assert!(!format!("{error:?}: {error}").contains("SECRET"));
    }
}

#[test]
fn the_fixed_export_charge_fits_and_exhausts_the_current_owner_byte_budget() {
    assert_eq!(
        MAX_COMMITTED_IMAGE_BYTES,
        cluster::MAX_STATE_MACHINE_OWNER_BYTES
    );
}

#[tokio::test]
async fn a_joined_lost_capture_and_live_unpolled_future_allow_real_fjall_reopen() -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let source = {
        let (writer, control) = observed(FjallReplicaStore::open(directory.path())?);
        let mut machine = ExperimentalStateMachine::create_with_image_export(writer, stream()?)?;
        let seeded: TestResult = async {
            machine
                .apply([
                    membership(0),
                    create(1, 1, QueueConfig::default())?,
                    send(2, 2, b"retained".to_vec())?,
                ])
                .await?;
            Ok(())
        }
        .await;
        if let Err(error) = seeded {
            let _ = machine.shutdown().await;
            return Err(error);
        }
        let source = match control.reader().snapshot() {
            Ok(source) => source,
            Err(error) => return finish(machine, Err(error.into())).await,
        };
        let unpolled = machine.export_create_send_image();
        let gate = control.gate();
        let mut lost = Box::pin(machine.export_create_send_image());
        let observations: TestResult = async {
            if first_poll(lost.as_mut()).await.is_some() {
                return Err("the export completed before its actual capture gate".into());
            }
            gate.entered().await?;
            drop(lost);
            Ok(())
        }
        .await;
        let mut shutdown = Box::pin(machine.shutdown());
        let joined = release_and_join(shutdown.as_mut(), &gate).await;
        observations?;
        joined?;
        drop(control);
        // Only the inert future's channel/admission capability remains. Neither
        // it nor the caller-owned snapshot retains a physical store handle.
        let mut reopened = ExperimentalStateMachine::open_with_image_export(
            FjallReplicaStore::open(directory.path())?,
            stream()?,
        )?;
        let result = async {
            check_image(&reopened.export_create_send_image().await?, &source)?;
            assert_eq!(
                unpolled.await.err(),
                Some(StateMachineImageExportError::Owner(
                    StateMachineError::Closed
                ))
            );
            Ok(())
        }
        .await;
        finish(reopened, result).await?;
        source
    };
    let writer = FjallReplicaStore::open(directory.path())?;
    assert_eq!(writer.reader().snapshot_bounded(limits())?, source);
    Ok(())
}
