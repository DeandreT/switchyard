use std::collections::BTreeSet;

use domain::{
    EntityPath, MessageBody, MessageIdentifier, MessageRecord, MessageState, MessageValue,
    NamespaceName, SequenceNumber, StateMachine, keys,
};
use storage::StateStore;

use super::*;

const EXPECTED_SEND: &[(u64, &str, &str)] = &[
    (1, "atomic-send-commit-a", "send-commit-a"),
    (2, "atomic-send-commit-b", "send-commit-b"),
    (3, "atomic-cold-send-commit-a", "cold-send-commit-a"),
    (4, "atomic-cold-send-commit-b", "cold-send-commit-b"),
];
const EXPECTED_HELD: &[(u64, &str, &str)] = &[(2, "atomic-held-commit", "held-commit")];

pub(super) fn check<S: StateStore>(store: &S, namespace: &NamespaceName) -> TestResult {
    let machine = StateMachine::new(store.clone());
    for (name, expected) in [
        (SEND_QUEUE, EXPECTED_SEND),
        (HELD_QUEUE, EXPECTED_HELD),
        (CONTROL_QUEUE, &[][..]),
    ] {
        let queue = EntityPath::new(name)?;
        assert_eq!(
            machine.queue_config(namespace, &queue)?,
            Some(domain::QueueConfig {
                lock_duration_millis: domain::MAX_LOCK_DURATION_MILLIS,
                default_time_to_live_millis: None,
                ..domain::QueueConfig::default()
            })
        );
        let rows =
            store.scan_prefix(&keys::message_prefix(namespace, &queue), expected.len() + 1)?;
        let actual_keys = rows
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<BTreeSet<_>>();
        let expected_keys = expected
            .iter()
            .map(|(sequence, _, _)| {
                keys::message(namespace, &queue, SequenceNumber::new(*sequence))
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            rows.len(),
            expected.len(),
            "unexpected retained message in {name}"
        );
        assert_eq!(
            actual_keys, expected_keys,
            "unexpected/aborted record in {name}"
        );
        let mut ready = BTreeSet::new();
        let mut locks = BTreeSet::new();
        let mut expiry = BTreeSet::new();
        for &(sequence, message_id, body) in expected {
            let sequence = SequenceNumber::new(sequence);
            let record = machine
                .message(namespace, &queue, sequence)?
                .expect("exact committed record");
            assert_record(&record, sequence, message_id, body);
            match record.state {
                MessageState::Ready => {
                    assert_eq!(record.delivery_count, 0);
                    ready.insert(keys::ready(namespace, &queue, sequence));
                    if let Some(expires_at) = record.expires_at {
                        assert!(expires_at >= record.enqueued_at);
                        expiry.insert(keys::expiry(namespace, &queue, expires_at, sequence));
                    }
                }
                MessageState::Locked {
                    token,
                    locked_until,
                } if name == HELD_QUEUE => {
                    assert_eq!(record.delivery_count, 1);
                    assert_eq!(
                        token.as_u64(),
                        2,
                        "replacement uses the next canonical lock, not the completed original"
                    );
                    assert!(locked_until > record.enqueued_at);
                    locks.insert(keys::lock(namespace, &queue, locked_until, sequence));
                }
                state => panic!("committed record must be Ready or Locked, not {state:?}"),
            }
        }
        assert_index(store, keys::ready_prefix(namespace, &queue), ready)?;
        assert_index(store, keys::lock_prefix(namespace, &queue), locks)?;
        assert_index(store, keys::expiry_prefix(namespace, &queue), expiry)?;
        for prefix in [
            keys::scheduled_prefix(namespace, &queue),
            keys::session_lock_prefix(namespace, &queue),
            keys::entity_session_prefix(namespace, &queue),
            keys::duplicate_history_prefix(namespace, &queue),
            keys::duplicate_history_expiry_prefix(namespace, &queue),
        ] {
            assert!(
                store.scan_prefix(&prefix, 1)?.is_empty(),
                "unexpected queue runtime row in {name}"
            );
        }
        let shadow = queue.dead_letter_queue()?;
        for prefix in [
            keys::message_prefix(namespace, &shadow),
            keys::ready_prefix(namespace, &shadow),
            keys::lock_prefix(namespace, &shadow),
            keys::expiry_prefix(namespace, &shadow),
            keys::scheduled_prefix(namespace, &shadow),
            keys::session_lock_prefix(namespace, &shadow),
        ] {
            assert!(
                store.scan_prefix(&prefix, 1)?.is_empty(),
                "unexpected dead-letter retention in {name}"
            );
        }
    }
    assert!(
        machine
            .message(
                namespace,
                &EntityPath::new(HELD_QUEUE)?,
                SequenceNumber::new(1)
            )?
            .is_none(),
        "Complete did not remove the original canonical sequence"
    );
    Ok(())
}

fn assert_record(record: &MessageRecord, sequence: SequenceNumber, message_id: &str, body: &str) {
    assert_eq!(record.sequence, sequence);
    assert_eq!(record.message_id, message_id);
    assert_eq!(record.body, body.as_bytes());
    assert!(record.session_id.is_none());
    assert!(record.dead_letter.is_none());
    assert!(record.scheduled_enqueue_time.is_none());
    let envelope = record
        .envelope
        .as_ref()
        .expect("SDK retained typed producer content");
    assert_eq!(
        envelope.body,
        MessageBody::Data(vec![body.as_bytes().to_vec()])
    );
    assert_eq!(
        envelope.properties.message_id,
        Some(MessageIdentifier::String(message_id.into()))
    );
    assert_eq!(
        envelope.properties.correlation_id,
        Some(MessageIdentifier::String(format!(
            "{message_id}-correlation"
        )))
    );
    assert_eq!(envelope.properties.subject.as_deref(), Some("atomic-sdk"));
    assert_eq!(
        envelope.properties.content_type.as_deref(),
        Some("text/plain")
    );
    assert_eq!(envelope.application_properties.len(), 3);
    assert_eq!(
        envelope.application_properties.get("phase"),
        Some(&MessageValue::String("commit".into()))
    );
    assert_eq!(
        envelope.application_properties.get("number"),
        Some(&MessageValue::Long(42))
    );
    assert_eq!(
        envelope.application_properties.get("enabled"),
        Some(&MessageValue::Bool(true))
    );
}

fn assert_index<S: StateStore>(
    store: &S,
    prefix: Vec<u8>,
    expected: BTreeSet<Vec<u8>>,
) -> TestResult {
    let rows = store.scan_prefix(&prefix, expected.len() + 1)?;
    let keys = rows
        .iter()
        .map(|(key, _)| key.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(rows.len(), expected.len(), "unexpected index row count");
    assert_eq!(
        keys, expected,
        "index does not correspond to committed record state"
    );
    assert!(
        rows.iter().all(|(_, value)| value.is_empty()),
        "index marker must be empty"
    );
    Ok(())
}
