use domain::{
    CommittedImageRole, CommittedSend, EncodedCommittedImage, QueueConfig, SessionId, Timestamp,
};
use openraft::{CommittedLeaderId, EntryPayload, storage::RaftStateMachine};
use storage::{BoundedStateStore, CatalogCommittedStore, StateStore};

use super::{
    TestResult, captured,
    observed::{Control, observed},
};
use crate::{
    ExperimentalStateMachine, LogApplication, LogEntry, LogId, LogQueueRefusal, QueueLogCommand,
};

pub(in crate::experimental_state_machine) async fn seeded<W>(
    writer: W,
    maximum: bool,
) -> TestResult<(ExperimentalStateMachine, Control<W>)>
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (writer, control) = observed(writer);
    let mut machine =
        ExperimentalStateMachine::create_with_snapshot_catalog(writer, captured::stream()?)?;
    let result = seed(&mut machine, maximum).await;
    if let Err(error) = result {
        let _ = machine.shutdown().await;
        return Err(error);
    }
    control.reset();
    Ok((machine, control))
}

pub(in crate::experimental_state_machine) async fn seed(
    machine: &mut ExperimentalStateMachine,
    maximum: bool,
) -> TestResult {
    let config = QueueConfig {
        max_message_bytes: domain::MAX_COMMITTED_BODY_BYTES + domain::BROKER_HEADER_RESERVE_BYTES,
        default_time_to_live_millis: Some(1000),
        requires_session: true,
        requires_duplicate_detection: true,
        duplicate_detection_history_time_window_millis: 20_000,
        ..QueueConfig::default()
    };
    let session = SessionId::new("s".repeat(domain::MAX_SESSION_ID_BYTES))?;
    let message_id = "\u{0800}".repeat(domain::MAX_MESSAGE_ID_LENGTH);
    let body = if maximum {
        (0..domain::MAX_COMMITTED_BODY_BYTES)
            .map(|i| (i % 251) as u8)
            .collect()
    } else {
        b"PRIVATE-original-body".to_vec()
    };
    let message = |body, session_id| CommittedSend {
        message_id: message_id.clone(),
        body,
        time_to_live_millis: Some(123),
        session_id,
    };
    let entries = [
        EntryPayload::Membership(captured::members()),
        EntryPayload::Normal(QueueLogCommand::create_queue(
            captured::namespace()?,
            captured::entity()?,
            Timestamp::from_millis(10),
            config,
        )),
        EntryPayload::Normal(QueueLogCommand::send(
            captured::namespace()?,
            captured::entity()?,
            Timestamp::from_millis(11),
            message(body, Some(session.clone())),
        )),
        EntryPayload::Normal(QueueLogCommand::send(
            captured::namespace()?,
            captured::entity()?,
            Timestamp::from_millis(12),
            message(b"PRIVATE-duplicate".to_vec(), Some(session)),
        )),
        EntryPayload::Normal(QueueLogCommand::send(
            captured::namespace()?,
            captured::entity()?,
            Timestamp::from_millis(500),
            message(b"PRIVATE-refused".to_vec(), None),
        )),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, payload)| LogEntry {
        log_id: LogId::new(CommittedLeaderId::new(1, 7), index as u64),
        payload,
    })
    .collect::<Vec<_>>();
    assert_eq!(
        machine.apply(entries).await?,
        vec![
            LogApplication::CheckpointOnly,
            LogApplication::QueueCreated,
            LogApplication::Sent { sequence: 1 },
            LogApplication::Sent { sequence: 2 },
            LogApplication::Refused(LogQueueRefusal::SessionRequired),
        ]
    );
    Ok(())
}

pub(in crate::experimental_state_machine) fn source<W: CatalogCommittedStore>(
    control: &Control<W>,
) -> TestResult<captured::Selected> {
    let snapshot = control.reader().snapshot()?;
    let image = EncodedCommittedImage::encode(
        CommittedImageRole::CreateSendLayout17V1,
        captured::stream()?,
        &snapshot,
    )?;
    let checkpoint = domain::DecodedCommittedImage::decode(image.as_bytes())?
        .checkpoint()
        .clone();
    Ok(captured::Selected {
        snapshot,
        image,
        checkpoint,
    })
}

pub(in crate::experimental_state_machine) async fn finish(
    machine: ExperimentalStateMachine,
    result: TestResult,
) -> TestResult {
    let joined = machine.shutdown().await;
    result?;
    joined?;
    Ok(())
}
