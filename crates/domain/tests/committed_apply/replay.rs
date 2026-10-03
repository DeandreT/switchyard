use domain::{
    CommittedApplyError, CommittedApplyResult, CommittedQueueCommand, CommittedQueueWork,
    CommittedSend, CommittedStateMachine, CommittedStreamId, EntityPath, QueueConfig, SessionId,
    Timestamp,
};
use storage::{CommittedStore, StateStore};

use super::{TestResult, fixture::*};

fn exact_replay_has_no_business_reads_or_reconstructed_effects<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    apply(&mut machine, 0, &create(1, QueueConfig::default())?)?;
    let work = send(2, "original", b"body")?;
    let request = update(&machine, 1)?;
    let (position, _) = applied(machine.apply_committed(&request, &work)?)?;
    assert_ne!(position.fingerprint, [0; 32]);
    let checkpoint = machine.checkpoint()?;
    let before = control.reader().snapshot()?;
    let counts = control.counts();
    assert_eq!(
        machine.apply_committed(&request, &work),
        Ok(CommittedApplyResult::AlreadyApplied { position })
    );
    let after = control.counts();
    assert_eq!(after.gets, counts.gets + 1);
    assert_eq!(after.scans, counts.scans);
    assert_eq!(after.commits, counts.commits);
    assert_eq!(control.reader().snapshot()?, before);
    assert_eq!(machine.checkpoint()?, checkpoint);
    assert_eq!(
        counters(&machine.reader())?
            .ok_or("missing counters")?
            .next_sequence,
        2
    );
    drop(machine);
    let mut reopened = CommittedStateMachine::open(control.recover_writer(), stream()?)?;
    assert_eq!(
        reopened.apply_committed(&request, &work),
        Ok(CommittedApplyResult::AlreadyApplied { position })
    );
    assert_eq!(control.reader().snapshot()?, before);
    Ok(())
}

fn fingerprint_covers_send_fields_origin_and_predecessor<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let create_request = update(&machine, 0)?;
    let original_create = create(1, QueueConfig::default())?;
    machine.apply_committed(&create_request, &original_create)?;
    let changed_config = create(
        1,
        QueueConfig {
            max_delivery_count: 11,
            ..QueueConfig::default()
        },
    )?;
    assert_eq!(
        machine.apply_committed(&create_request, &changed_config),
        Err(CommittedApplyError::ReplayConflict)
    );

    let original = send(2, "original", &[0, 255, 17])?;
    let request = update(&machine, 1)?;
    machine.apply_committed(&request, &original)?;
    let before = control.reader().snapshot()?;
    let counts = control.counts();
    let changed = [
        send(2, "replacement", &[0, 255, 17])?,
        send(2, "original", &[0, 255, 18])?,
        send(3, "original", &[0, 255, 17])?,
        CommittedQueueWork::Queue(CommittedQueueCommand::send(
            namespace()?,
            entity()?,
            Timestamp::from_millis(2),
            CommittedSend {
                message_id: "original".into(),
                body: vec![0, 255, 17],
                time_to_live_millis: Some(3),
                session_id: None,
            },
        )),
        CommittedQueueWork::Queue(CommittedQueueCommand::send(
            namespace()?,
            entity()?,
            Timestamp::from_millis(2),
            CommittedSend {
                message_id: "original".into(),
                body: vec![0, 255, 17],
                time_to_live_millis: None,
                session_id: Some(SessionId::new("group")?),
            },
        )),
        CommittedQueueWork::Queue(CommittedQueueCommand::send(
            namespace()?,
            EntityPath::new("Orders")?,
            Timestamp::from_millis(2),
            CommittedSend {
                message_id: "original".into(),
                body: vec![0, 255, 17],
                time_to_live_millis: None,
                session_id: None,
            },
        )),
    ];
    for work in changed {
        assert_eq!(
            machine.apply_committed(&request, &work),
            Err(CommittedApplyError::ReplayConflict)
        );
    }
    for request in [
        domain::CommittedCheckpointUpdate {
            entry: domain::CommittedEntryId {
                term: 2,
                ..request.entry
            },
            ..request.clone()
        },
        domain::CommittedCheckpointUpdate {
            entry: domain::CommittedEntryId {
                node_id: 10,
                ..request.entry
            },
            ..request.clone()
        },
        domain::CommittedCheckpointUpdate {
            expected_previous: None,
            ..request.clone()
        },
    ] {
        assert_eq!(
            machine.apply_committed(&request, &original),
            Err(CommittedApplyError::ReplayConflict)
        );
    }
    assert_eq!(control.counts().commits, counts.commits);
    assert_eq!(control.counts().scans, counts.scans);
    assert_eq!(control.reader().snapshot()?, before);
    Ok(())
}

fn predecessor_stream_and_contiguity_fail_closed<W: CommittedStore>(writer: W) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let first = update(&machine, 1)?;
    assert_eq!(
        machine.apply_committed(&first, &CommittedQueueWork::Blank),
        Err(CommittedApplyError::NonContiguous)
    );
    apply(&mut machine, 0, &CommittedQueueWork::Blank)?;
    let next = update(&machine, 1)?;
    let before = control.reader().snapshot()?;
    let counts = control.counts();
    let mut wrong_stream = next.clone();
    wrong_stream.stream = CommittedStreamId::new([8; 16])?;
    assert_eq!(
        machine.apply_committed(&wrong_stream, &CommittedQueueWork::Blank),
        Err(CommittedApplyError::WrongStream)
    );
    let mut wrong_previous = next.clone();
    let mark = wrong_previous
        .expected_previous
        .as_mut()
        .ok_or("missing predecessor")?;
    mark.fingerprint[0] ^= 1;
    assert_eq!(
        machine.apply_committed(&wrong_previous, &CommittedQueueWork::Blank),
        Err(CommittedApplyError::PreviousMismatch)
    );
    let mut gap = next.clone();
    gap.entry.index = 2;
    assert_eq!(
        machine.apply_committed(&gap, &CommittedQueueWork::Blank),
        Err(CommittedApplyError::NonContiguous)
    );
    let mut regressed_term = next.clone();
    regressed_term.entry.term = 0;
    assert_eq!(
        machine.apply_committed(&regressed_term, &CommittedQueueWork::Blank),
        Err(CommittedApplyError::NonContiguous)
    );
    assert_eq!(control.counts().commits, counts.commits);
    assert_eq!(control.reader().snapshot()?, before);
    machine.apply_committed(&next, &CommittedQueueWork::Blank)?;
    let mut backward = update(&machine, 0)?;
    assert_eq!(
        machine.apply_committed(&backward, &CommittedQueueWork::Blank),
        Err(CommittedApplyError::NonContiguous)
    );
    backward.expected_previous = first.expected_previous;
    assert_eq!(
        machine.apply_committed(&backward, &CommittedQueueWork::Blank),
        Err(CommittedApplyError::PreviousMismatch)
    );
    Ok(())
}

for_each_backend!(
    exact_replay_has_no_business_reads_or_reconstructed_effects,
    fingerprint_covers_send_fields_origin_and_predecessor,
    predecessor_stream_and_contiguity_fail_closed,
);
