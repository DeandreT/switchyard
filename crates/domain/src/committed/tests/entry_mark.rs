use storage::{CommittedStore, StateStore};

use super::{send, stream, update};
use crate::{
    CommittedApplyError, CommittedApplyResult, CommittedCheckpointUpdate, CommittedEntryMark,
    CommittedQueueCommand, CommittedQueueWork, CommittedStateMachine, CommittedStreamId,
    EntityPath, IdentifierError, MAX_COMMITTED_BODY_BYTES, MAX_COMMITTED_ENTRY_BYTES,
    MAX_COMMITTED_MEMBERSHIP_BYTES, NamespaceName, QueueConfig, Timestamp,
};

#[test]
fn canonical_mark_matches_the_existing_frozen_hash_without_initializing_storage() {
    let writer = storage::MemoryReplicaStore::new();
    let reader = writer.reader();
    let before = reader.snapshot().expect("snapshot");
    let work = CommittedQueueWork::Blank;
    let update = update();
    let mark = work.entry_mark(&update).expect("mark");
    assert_eq!(mark.id, update.entry);
    assert_eq!(
        mark.fingerprint,
        crate::committed::entry_fingerprint(&update, &work).expect("frozen hash")
    );
    assert_eq!(reader.snapshot().expect("snapshot"), before);
    assert!(!writer.is_initialized().expect("initialization state"));
}

#[test]
fn a_pure_mark_chain_matches_real_applied_and_checkpoint_marks() {
    let writer = storage::MemoryReplicaStore::new();
    let reader = writer.reader();
    let mut machine = CommittedStateMachine::create(writer, stream()).expect("machine");
    let work = [
        CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
            NamespaceName::new("test").expect("namespace"),
            EntityPath::new("queue").expect("entity"),
            Timestamp::from_millis(1),
            QueueConfig::default(),
        )),
        send(vec![1, 255, 0], "id".into()),
        CommittedQueueWork::Blank,
    ];
    let mut previous = None;
    let mut predecessor = None;
    for (index, work) in work.iter().enumerate() {
        let mut update = update();
        update.entry.index = index as u64;
        update.expected_previous = previous;
        let before = reader.snapshot().expect("snapshot");
        let mark = work.entry_mark(&update).expect("mark");
        assert_eq!(reader.snapshot().expect("snapshot"), before);
        let result = machine.apply_committed(&update, work).expect("apply");
        assert!(
            matches!(result, CommittedApplyResult::Applied { position, .. } if position == mark)
        );
        predecessor = previous;
        previous = Some(mark);
    }
    let checkpoint = machine.checkpoint().expect("checkpoint");
    assert_eq!(checkpoint.last(), previous);
    assert_eq!(checkpoint.previous(), predecessor);
}

#[test]
fn predecessors_content_position_and_stream_affect_the_public_mark() {
    let work = send(vec![1], "id".into());
    let base = work.entry_mark(&update()).expect("mark");
    let mut changed = update();
    changed.expected_previous = Some(CommittedEntryMark {
        id: changed.entry,
        fingerprint: [3; 32],
    });
    let predecessor = work.entry_mark(&changed).expect("mark");
    assert_ne!(base.fingerprint, predecessor.fingerprint);
    changed
        .expected_previous
        .as_mut()
        .expect("predecessor")
        .fingerprint[0] = 4;
    assert_ne!(
        predecessor.fingerprint,
        work.entry_mark(&changed).expect("mark").fingerprint
    );
    changed = update();
    changed.entry.node_id += 1;
    assert_ne!(
        base.fingerprint,
        work.entry_mark(&changed).expect("mark").fingerprint
    );
    changed = update();
    changed.stream = CommittedStreamId::new([2; 16]).expect("stream");
    assert_ne!(
        base.fingerprint,
        work.entry_mark(&changed).expect("mark").fingerprint
    );
    assert_ne!(
        base.fingerprint,
        send(vec![2], "id".into())
            .entry_mark(&update())
            .expect("mark")
            .fingerprint
    );
    assert_ne!(
        base.fingerprint,
        send(vec![1], "other".into())
            .entry_mark(&update())
            .expect("mark")
            .fingerprint
    );
}

#[test]
fn public_mark_retains_independent_body_and_canonical_entry_bounds() {
    assert!(
        send(vec![7; MAX_COMMITTED_BODY_BYTES], "id".into())
            .entry_mark(&update())
            .is_ok()
    );
    assert_eq!(
        send(vec![7; MAX_COMMITTED_BODY_BYTES + 1], "id".into()).entry_mark(&update()),
        Err(CommittedApplyError::TooLarge {
            resource: "body",
            maximum: MAX_COMMITTED_BODY_BYTES
        }),
    );
    assert_eq!(
        send(Vec::new(), "i".repeat(MAX_COMMITTED_ENTRY_BYTES)).entry_mark(&update()),
        Err(CommittedApplyError::TooLarge {
            resource: "entry",
            maximum: MAX_COMMITTED_ENTRY_BYTES
        }),
    );
}

#[test]
fn public_mark_retains_membership_bounds_and_nonzero_schema_policy() {
    let membership = |schema_version, length| CommittedQueueWork::Membership {
        schema_version,
        payload: vec![5; length],
    };
    assert!(
        membership(1, MAX_COMMITTED_MEMBERSHIP_BYTES)
            .entry_mark(&update())
            .is_ok()
    );
    assert_eq!(
        membership(0, 0).entry_mark(&update()),
        Err(CommittedApplyError::InvalidMembershipSchema),
    );
    assert_eq!(
        membership(1, MAX_COMMITTED_MEMBERSHIP_BYTES + 1).entry_mark(&update()),
        Err(CommittedApplyError::TooLarge {
            resource: "membership",
            maximum: MAX_COMMITTED_MEMBERSHIP_BYTES
        }),
    );
    assert_ne!(
        membership(1, 0)
            .entry_mark(&update())
            .expect("mark")
            .fingerprint,
        membership(2, 0)
            .entry_mark(&update())
            .expect("mark")
            .fingerprint,
    );
}

#[test]
fn hashable_business_refusals_are_not_rejected_by_the_pure_helper() {
    let work = CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
        NamespaceName::new("test").expect("namespace"),
        EntityPath::new("queue").expect("entity"),
        Timestamp::UNIX_EPOCH,
        QueueConfig {
            max_message_bytes: 0,
            ..QueueConfig::default()
        },
    ));
    assert!(work.entry_mark(&update()).is_ok());
    assert!(
        send(Vec::new(), "i".repeat(129))
            .entry_mark(&update())
            .is_ok()
    );
}

#[test]
fn forged_identifiers_and_zero_stream_cannot_bypass_mark_validation() {
    let namespace: NamespaceName =
        postcard::from_bytes(&postcard::to_stdvec("bad\0scope").expect("encoding"))
            .expect("raw identifier");
    let work = CommittedQueueWork::Queue(CommittedQueueCommand::create_queue(
        namespace,
        EntityPath::new("queue").expect("entity"),
        Timestamp::UNIX_EPOCH,
        QueueConfig::default(),
    ));
    assert_eq!(
        work.entry_mark(&update()),
        Err(CommittedApplyError::InvalidIdentifier(
            IdentifierError::ControlCharacter { kind: "namespace" }
        )),
    );
    let zero: CommittedStreamId = postcard::from_bytes(&[0; 16]).expect("raw stream");
    let changed = CommittedCheckpointUpdate {
        stream: zero,
        ..update()
    };
    assert_eq!(
        CommittedQueueWork::Blank.entry_mark(&changed),
        Err(CommittedApplyError::InvalidStreamId)
    );
}

#[test]
fn a_computed_mark_does_not_make_an_unadmitted_predecessor_applicable() {
    let writer = storage::MemoryReplicaStore::new();
    let reader = writer.reader();
    let mut machine = CommittedStateMachine::create(writer, stream()).expect("machine");
    let before = reader.snapshot().expect("snapshot");
    let mut update = update();
    update.expected_previous = Some(CommittedEntryMark {
        id: update.entry,
        fingerprint: [7; 32],
    });
    let work = CommittedQueueWork::Blank;
    assert!(work.entry_mark(&update).is_ok());
    assert_eq!(reader.snapshot().expect("snapshot"), before);
    assert_eq!(
        machine.apply_committed(&update, &work),
        Err(CommittedApplyError::PreviousMismatch)
    );
    assert_eq!(reader.snapshot().expect("snapshot"), before);
    assert_eq!(machine.checkpoint().expect("checkpoint").last(), None);
}
