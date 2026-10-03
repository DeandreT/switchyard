use std::cell::Cell;

use domain::{CommittedSend, EntityPath, NamespaceName, Timestamp};
use openraft::{CommittedLeaderId, EntryPayload};

use crate::{LogId, MAX_LOG_BODY_BYTES, QueueLogCommand};

use super::*;

fn blank(term: u64, node: u64, index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId::new(CommittedLeaderId::new(term, node), index),
        payload: EntryPayload::Blank,
    }
}

#[test]
fn count_bound_consumes_at_most_one_entry_beyond_the_full_apply_limit() {
    let consumed = Cell::new(0usize);
    let input = (0..).map(|index| {
        consumed.set(consumed.get() + 1);
        blank(1, 7, index)
    });
    assert!(matches!(
        PreparedApply::from_entries(input),
        Err(StateMachineError::Capacity)
    ));
    assert_eq!(consumed.get(), MAX_APPLY_ENTRIES + 1);
}

#[test]
fn a_full_retained_range_is_not_mistaken_for_an_append_chunk() {
    let entries = (0..MAX_APPLY_ENTRIES as u64)
        .map(|index| blank(1, 7, index))
        .collect::<Vec<_>>();
    let expected_bytes = entries
        .iter()
        .map(|entry| validated_entry_len(entry).expect("valid blank entry"))
        .sum::<usize>();
    let input = PreparedApply::from_entries(entries.clone()).expect("full valid apply range");
    assert_eq!(input.encoded_bytes(), expected_bytes);
    assert_eq!(input.into_entries(), entries);
}

#[test]
fn full_identity_regressions_gaps_and_index_wrapping_are_refused() {
    for entries in [
        vec![blank(2, 7, 0), blank(2, 6, 1)],
        vec![blank(2, 7, 0), blank(1, 9, 1)],
        vec![blank(2, 7, 0), blank(2, 7, 2)],
        vec![blank(2, 7, u64::MAX), blank(3, 7, 0)],
    ] {
        assert!(matches!(
            PreparedApply::from_entries(entries),
            Err(StateMachineError::InvalidApply)
        ));
    }
    assert!(PreparedApply::from_entries([blank(2, 7, 0), blank(3, 1, 1)]).is_ok());
}

#[test]
fn a_late_oversized_body_prevents_preparation_of_the_entire_packet() {
    let send = LogEntry {
        log_id: LogId::new(CommittedLeaderId::new(1, 7), 1),
        payload: EntryPayload::Normal(QueueLogCommand::send(
            NamespaceName::new("tenant").expect("namespace"),
            EntityPath::new("orders").expect("entity"),
            Timestamp::from_millis(2),
            CommittedSend {
                message_id: "bounded-input".into(),
                body: vec![0; MAX_LOG_BODY_BYTES + 1],
                time_to_live_millis: None,
                session_id: None,
            },
        )),
    };
    assert!(matches!(
        PreparedApply::from_entries([blank(1, 7, 0), send]),
        Err(StateMachineError::Codec)
    ));
}
