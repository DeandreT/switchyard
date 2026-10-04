use std::time::{Duration, UNIX_EPOCH};

use domain::{CommittedEntryId, CommittedSend, EntityPath, NamespaceName, QueueConfig, Timestamp};

use crate::{
    LogApplication, LogId, LogQueueConfigRefusal, LogQueueRefusal, MAX_LOG_BODY_BYTES,
    MAX_LOG_ENTRY_BYTES,
    experimental_log::{
        LogRetention, MAX_RETAINED_BYTES, MAX_RETAINED_ENTRIES, queue_entry_upper_bound,
        stamp_queue_command, validated_entry_len,
    },
};

use super::*;

pub(super) fn create(config: QueueConfig) -> QueueIntent {
    QueueIntent::create_queue(
        NamespaceName::new("private-namespace").unwrap(),
        EntityPath::new("private-path").unwrap(),
        config,
    )
    .unwrap()
}

fn send(body: Vec<u8>, id: String) -> Result<QueueIntent, QueueWriteError> {
    QueueIntent::send(
        NamespaceName::new("private-namespace").unwrap(),
        EntityPath::new("private-path").unwrap(),
        CommittedSend {
            message_id: id,
            body,
            time_to_live_millis: None,
            session_id: None,
        },
    )
}

fn mark() -> CommittedEntryId {
    CommittedEntryId {
        term: 1,
        node_id: 7,
        index: 2,
    }
}

#[test]
fn business_invalid_configuration_and_message_id_remain_hashable_intents() {
    let config = QueueConfig {
        max_message_bytes: 0,
        ..QueueConfig::default()
    };
    assert!(create(config).encoded_bytes() > 0);
    assert!(send(vec![1], "x".repeat(129)).is_ok());
}

#[test]
fn body_and_canonical_entry_caps_are_local_and_debug_is_opaque() {
    assert!(send(vec![0; MAX_LOG_BODY_BYTES], "id".into()).is_ok());
    assert_eq!(
        send(vec![0; MAX_LOG_BODY_BYTES + 1], "id".into()).unwrap_err(),
        QueueWriteError::KnownRejected(QueueWriteRejection::InvalidIntent)
    );
    assert!(send(Vec::new(), "x".repeat(MAX_LOG_ENTRY_BYTES)).is_err());
    let debug = format!("{:?}", create(QueueConfig::default()));
    assert!(!debug.contains("private-namespace") && !debug.contains("private-path"));
    assert!(
        !format!(
            "{:?}",
            send(b"private-body".to_vec(), "private-id".into()).unwrap()
        )
        .contains("private-body")
    );
}

#[test]
fn sizing_covers_maximum_timestamp_and_full_log_id_without_changing_work() {
    let intent = send(vec![17; MAX_LOG_BODY_BYTES], "long-but-bounded".repeat(9)).unwrap();
    let charge = intent.encoded_bytes();
    let mut command = intent.into_command();
    assert_eq!(queue_entry_upper_bound(&command).unwrap(), charge);
    stamp_queue_command(&mut command, Timestamp::from_millis(u64::MAX));
    let entry = crate::LogEntry {
        log_id: LogId::new(
            openraft::CommittedLeaderId::new(u64::MAX, u64::MAX),
            u64::MAX,
        ),
        payload: openraft::EntryPayload::Normal(command),
    };
    assert_eq!(validated_entry_len(&entry).unwrap(), charge);
    assert!(charge <= MAX_LOG_ENTRY_BYTES);
}

#[test]
fn epoch_conversion_is_exact_and_fails_closed_outside_representable_time() {
    assert_eq!(
        write::epoch_millis(UNIX_EPOCH + Duration::from_millis(42)),
        Ok(Timestamp::from_millis(42))
    );
    assert!(write::epoch_millis(UNIX_EPOCH - Duration::from_secs(1)).is_err());
    if let Some(time) = UNIX_EPOCH.checked_add(Duration::from_secs(u64::MAX)) {
        assert!(write::epoch_millis(time).is_err());
    }
}

#[test]
fn committed_time_clamp_is_bounded_at_exact_500_millisecond_boundary() {
    let stamp = |now, watermark| {
        write::bounded_stamp(
            Timestamp::from_millis(now),
            Timestamp::from_millis(watermark),
        )
    };
    assert_eq!(stamp(1_000, 999), Ok(Timestamp::from_millis(1_000)));
    assert_eq!(stamp(1_000, 1_000), Ok(Timestamp::from_millis(1_000)));
    assert_eq!(stamp(1_000, 1_500), Ok(Timestamp::from_millis(1_500)));
    assert_eq!(stamp(1_000, 1_501), Err(()));
    assert_eq!(stamp(0, u64::MAX), Err(()));
    assert_eq!(
        stamp(u64::MAX, u64::MAX),
        Ok(Timestamp::from_millis(u64::MAX))
    );
}

#[test]
fn headroom_reserves_both_write_and_one_noop_at_exact_count_and_byte_boundaries() {
    let mut retention = LogRetention {
        last_present: None,
        last_purged: None,
        retained_entries: MAX_RETAINED_ENTRIES - 2,
        retained_bytes: MAX_RETAINED_BYTES - 164,
    };
    assert!(write::has_headroom(retention, 100));
    retention.retained_entries += 1;
    assert!(!write::has_headroom(retention, 100));
    retention.retained_entries -= 1;
    retention.retained_bytes += 1;
    assert!(!write::has_headroom(retention, 100));
    retention.retained_bytes = 0;
    retention.last_purged = Some(LogId::default());
    assert!(!write::has_headroom(retention, 100));
}

#[test]
fn application_matches_kind_and_never_reconstructs_valid_replay_response() {
    assert_eq!(
        write::application(false, mark(), LogApplication::QueueCreated),
        Ok(QueueWriteOutcome::QueueCreated)
    );
    assert_eq!(
        write::application(true, mark(), LogApplication::Sent { sequence: 3 }),
        Ok(QueueWriteOutcome::Sent { sequence: 3 })
    );
    assert_eq!(
        write::application(false, mark(), LogApplication::Sent { sequence: 3 }),
        Err(QueueWriteUnknown::UnexpectedApplication)
    );
    assert_eq!(
        write::application(true, mark(), LogApplication::QueueCreated),
        Err(QueueWriteUnknown::UnexpectedApplication)
    );
    assert_eq!(
        write::application(true, mark(), LogApplication::Sent { sequence: 0 }),
        Err(QueueWriteUnknown::UnexpectedApplication)
    );
    assert_eq!(
        write::application(
            true,
            mark(),
            LogApplication::Sent {
                sequence: domain::MAX_SEQUENCE_NUMBER + 1
            }
        ),
        Err(QueueWriteUnknown::UnexpectedApplication)
    );
    assert_eq!(
        write::application(
            true,
            mark(),
            LogApplication::AlreadyApplied { entry: mark() }
        ),
        Err(QueueWriteUnknown::OriginalResultUnavailable)
    );
    let other = CommittedEntryId { index: 3, ..mark() };
    assert_eq!(
        write::application(
            true,
            mark(),
            LogApplication::AlreadyApplied { entry: other }
        ),
        Err(QueueWriteUnknown::UnexpectedApplication)
    );
    assert_eq!(
        write::application(false, mark(), LogApplication::CheckpointOnly),
        Err(QueueWriteUnknown::UnexpectedApplication)
    );
}

#[test]
fn refused_results_remain_committed_outcomes_with_finite_kind_allowlists() {
    let common = LogQueueRefusal::ClockRegression {
        last_applied_millis: 8,
        proposed_millis: 7,
    };
    for send in [false, true] {
        assert_eq!(
            write::application(send, mark(), LogApplication::Refused(common)),
            Ok(QueueWriteOutcome::Refused(common))
        );
    }
    let config =
        LogQueueRefusal::InvalidQueueConfiguration(LogQueueConfigRefusal::MaxMessageBytesTooSmall);
    assert!(write::application(false, mark(), LogApplication::Refused(config)).is_ok());
    assert!(write::application(true, mark(), LogApplication::Refused(config)).is_err());
    let message = LogQueueRefusal::MessageIdTooLong {
        length: 129,
        maximum: 128,
    };
    assert!(write::application(true, mark(), LogApplication::Refused(message)).is_ok());
    assert!(write::application(false, mark(), LogApplication::Refused(message)).is_err());
}

#[test]
fn client_errors_never_contain_untrusted_source_or_native_error_text() {
    for error in [
        QueueWriteError::KnownRejected(QueueWriteRejection::Storage),
        QueueWriteError::Unknown(QueueWriteUnknown::Storage),
        QueueWriteError::Unknown(QueueWriteUnknown::OriginalResultUnavailable),
    ] {
        let text = format!("{error:?} {error}");
        for private in [
            "private-body",
            "private-namespace",
            "private-path",
            "private-id",
        ] {
            assert!(!text.contains(private));
        }
    }
}
