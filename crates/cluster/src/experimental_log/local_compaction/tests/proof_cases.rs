use super::super::super::types::{EncodedAppend, entry_key};
use super::*;
use crate::experimental_local_compaction::{
    LocalCompactionError as Error,
    frontier::{Frontier, PairIdentity, ReceiptData},
};

fn initialized(entries: &[LogEntry]) -> TestResult<(state::StoreState<Writer>, Control)> {
    let (writer, control) = Writer::new();
    let mut state = state::StoreState::create(writer, profile()?)?;
    state.append(EncodedAppend::from_entries(entries.to_vec())?)?;
    state.save_vote(LogVote::new_committed(1, 7))?;
    Ok((state, control))
}

#[test]
fn fresh_fold_catches_same_identity_changed_content_before_claim() -> TestResult {
    let entries = entries()?;
    let (checkpoint, metadata, projection) = source(&entries)?;
    let (mut state, control) = initialized(&entries)?;
    let identity = PairIdentity::new();
    let (frontier, publisher) = Frontier::new(identity.clone());
    state.seal(identity.clone(), &checkpoint, None)?;
    let attempt = frontier.begin()?;
    let receipt = Arc::new(ReceiptData {
        identity,
        attempt,
        node_id: 7,
        checkpoint,
        metadata,
        projection,
    });
    publisher.publish(receipt.clone())?;
    let permit = state.permit(frontier, receipt)?;
    let mut changed = entries[2].clone();
    if let openraft::EntryPayload::Normal(command) = &mut changed.payload
        && let crate::experimental_log::types::QueueLogKind::Send { message, .. } =
            command.0.as_mut()
    {
        message.body[0] ^= 1;
    }
    let encoded = super::super::super::codec::encode_entry(&changed)?;
    let old = control.records.get(&entry_key(2))?.unwrap();
    assert_eq!(old.len(), encoded.bytes().len());
    put(&control, &entry_key(2), encoded.bytes())?;
    let commits = control.commits.load(Ordering::SeqCst);
    assert_eq!(state.compact(permit).err(), Some(Error::InvalidHistory));
    assert_eq!(control.commits.load(Ordering::SeqCst), commits);
    assert!(control.records.get(&entry_key(0))?.is_some());
    Ok(())
}

#[test]
fn changed_command_membership_or_watermark_never_match_full_checkpoint() -> TestResult {
    let entries = entries()?;
    let (checkpoint, _, _) = source(&entries)?;
    for case in 0..4 {
        let mut changed = entries.clone();
        match case {
            0 => {
                if let openraft::EntryPayload::Normal(command) = &mut changed[2].payload
                    && let crate::experimental_log::types::QueueLogKind::Send { message, .. } =
                        command.0.as_mut()
                {
                    message.body[0] ^= 1;
                }
            }
            1 => {
                if let openraft::EntryPayload::Normal(command) = &mut changed[2].payload
                    && let crate::experimental_log::types::QueueLogKind::Send { issued_at, .. } =
                        command.0.as_mut()
                {
                    *issued_at = Timestamp::from_millis(99);
                }
            }
            2 => {
                changed[0].payload = openraft::EntryPayload::Membership(openraft::Membership::new(
                    vec![BTreeSet::from([7])],
                    BTreeMap::from([(7, openraft::BasicNode::new("changed"))]),
                ))
            }
            _ => changed[2].log_id = LogId::new(openraft::CommittedLeaderId::new(1, 8), 2),
        }
        let (mut state, _) = initialized(&changed)?;
        assert_eq!(
            state.seal(PairIdentity::new(), &checkpoint, None).err(),
            Some(Error::InvalidHistory)
        );
    }
    Ok(())
}

#[test]
fn catalog_ahead_and_same_checkpoint_different_digest_refuse() -> TestResult {
    let entries = entries()?;
    let (checkpoint, metadata, projection) = source(&entries)?;
    let mut different_digest =
        crate::experimental_state_machine::NativeCheckpointSummary::decode(metadata.as_bytes())?;
    different_digest.digest[0] ^= 1;
    let different_digest = different_digest.encode_for_test()?;
    let (mut state, control) = initialized(&entries)?;
    let identity = PairIdentity::new();
    let (frontier, publisher) = Frontier::new(identity.clone());
    state.seal(identity.clone(), &checkpoint, None)?;
    let attempt = frontier.begin()?;
    let receipt = Arc::new(ReceiptData {
        identity,
        attempt,
        node_id: 7,
        checkpoint: checkpoint.clone(),
        metadata,
        projection,
    });
    publisher.publish(receipt.clone())?;
    let permit = state.permit(frontier, receipt)?;
    state.compact(permit)?;
    drop(state);
    let mut changed = entries.clone();
    changed.push(LogEntry {
        log_id: id(3),
        payload: openraft::EntryPayload::Blank,
    });
    let (_, ahead, _) = source(&changed)?;
    let mut reopened = state::StoreState::open(Writer(control), profile()?)?;
    assert_eq!(
        reopened
            .seal(PairIdentity::new(), &checkpoint, Some(ahead.as_bytes()))
            .err(),
        Some(Error::InvalidHistory)
    );
    assert_eq!(
        reopened
            .seal(PairIdentity::new(), &checkpoint, Some(&different_digest))
            .err(),
        Some(Error::InvalidHistory)
    );
    Ok(())
}

#[test]
fn inclusive_reader_reaches_maximum_index_from_a_canonical_baseline() -> TestResult {
    let entries = entries()?;
    let (_, metadata, _) = source(&entries)?;
    let mut summary =
        crate::experimental_state_machine::NativeCheckpointSummary::decode(metadata.as_bytes())?;
    summary.last.as_mut().unwrap().id.index = u64::MAX - 1;
    summary.previous.as_mut().unwrap().id.index = u64::MAX - 2;
    let profile = profile()?;
    let baseline = codec::Baseline::make(&profile, 1, &summary.encode_for_test()?)?;
    let entry = super::super::super::codec::encode_entry(&LogEntry {
        log_id: id(u64::MAX),
        payload: openraft::EntryPayload::Blank,
    })?;
    let progress = super::super::super::types::LogProgress {
        vote: Some(LogVote::new_committed(1, 7)),
        last_purged: baseline.through(),
        last_present: Some(entry.id()),
        retained_entries: 1,
        retained_bytes: entry.encoded_len() as u64,
    };
    let (mut writer, _) = Writer::new();
    let mut batch = WriteBatch::default();
    batch.push_put([1], codec::encode_profile(&profile)?);
    batch.push_put([2], super::super::super::codec::encode_progress(&progress)?);
    batch.push_put(codec::BASELINE_KEY, baseline.bytes);
    batch.push_put(entry_key(u64::MAX), entry.bytes());
    writer.commit(batch)?;
    let mut reopened = state::StoreState::open(writer, profile)?;
    assert_eq!(
        reopened
            .read(u64::MAX, u64::MAX)?
            .last()
            .map(|entry| entry.log_id.index),
        Some(u64::MAX)
    );
    assert_eq!(reopened.read(u64::MAX - 1, u64::MAX)?.len(), 1);
    Ok(())
}

#[test]
fn inclusive_reader_is_bounded_and_reads_thirty_two_of_full_retained_limit() -> TestResult {
    let mut entries = entries()?;
    entries.extend((3..256).map(|index| LogEntry {
        log_id: id(index),
        payload: openraft::EntryPayload::Blank,
    }));
    let (writer, _) = Writer::new();
    let mut state = state::StoreState::create(writer, profile()?)?;
    for chunk in entries.chunks(32) {
        state.append(EncodedAppend::from_entries(chunk.to_vec())?)?;
    }
    assert_eq!(state.read(0, u64::MAX)?.len(), 32);
    assert_eq!(
        state
            .read(255, u64::MAX)?
            .last()
            .map(|entry| entry.log_id.index),
        Some(255)
    );
    assert_eq!(
        state
            .append(EncodedAppend::from_entries([LogEntry {
                log_id: id(256),
                payload: openraft::EntryPayload::Blank
            }])?)
            .err(),
        Some(Error::LimitExceeded)
    );
    assert_eq!(state.read(255, u64::MAX)?.len(), 1);
    let mut large = entries[..2].to_vec();
    for index in 2..22 {
        large.push(LogEntry {
            log_id: id(index),
            payload: openraft::EntryPayload::Normal(QueueLogCommand::send(
                domain::NamespaceName::new("tenant")?,
                domain::EntityPath::new("orders")?,
                Timestamp::from_millis(index),
                domain::CommittedSend {
                    message_id: format!("PRIVATE-{index}"),
                    body: vec![7; 220 * 1024],
                    time_to_live_millis: None,
                    session_id: None,
                },
            )),
        });
    }
    let (writer, _) = Writer::new();
    let mut bytes_limited = state::StoreState::create(writer, profile()?)?;
    for chunk in large.chunks(10) {
        bytes_limited.append(EncodedAppend::from_entries(chunk.to_vec())?)?;
    }
    let returned = bytes_limited.read(0, u64::MAX)?;
    assert!(returned.len() < 32);
    let bytes = returned
        .iter()
        .map(super::super::super::codec::entry_len)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .sum::<usize>();
    assert!(bytes <= crate::MAX_LIMITED_BYTES);
    assert!(
        bytes + super::super::super::codec::entry_len(&large[returned.len()])?
            > crate::MAX_LIMITED_BYTES
    );
    Ok(())
}

#[test]
fn ordinal_saturation_is_terminal_without_a_physical_commit() -> TestResult {
    let entries = entries()?;
    let (checkpoint, metadata, projection) = source(&entries)?;
    let profile = profile()?;
    let baseline = codec::Baseline::make(&profile, u64::MAX, metadata.as_bytes())?;
    let progress = super::super::super::types::LogProgress {
        vote: Some(LogVote::new_committed(1, 7)),
        last_purged: baseline.through(),
        ..Default::default()
    };
    let (mut writer, control) = Writer::new();
    let mut batch = WriteBatch::default();
    batch.push_put([1], codec::encode_profile(&profile)?);
    batch.push_put([2], super::super::super::codec::encode_progress(&progress)?);
    batch.push_put(codec::BASELINE_KEY, baseline.bytes);
    writer.commit(batch)?;
    let mut state = state::StoreState::open(writer, profile)?;
    let identity = PairIdentity::new();
    let (frontier, publisher) = Frontier::new(identity.clone());
    state.seal(identity.clone(), &checkpoint, Some(metadata.as_bytes()))?;
    let attempt = frontier.begin()?;
    let receipt = Arc::new(ReceiptData {
        identity,
        attempt,
        node_id: 7,
        checkpoint,
        metadata,
        projection,
    });
    publisher.publish(receipt.clone())?;
    let permit = state.permit(frontier.clone(), receipt)?;
    let commits = control.commits.load(Ordering::SeqCst);
    assert_eq!(state.compact(permit).err(), Some(Error::Exhausted));
    assert_eq!(control.commits.load(Ordering::SeqCst), commits);
    assert_eq!(frontier.begin(), Err(Error::Exhausted));
    Ok(())
}
