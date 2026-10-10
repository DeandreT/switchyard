//! Explicit dead-lettering cannot move a held DLQ delivery into another shadow.

use std::error::Error;

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, EntityPath, NamespaceName, QueueConfig,
    ReceiveMode, Timestamp, keys, snapshot_validation::validate_catalog,
};
use storage::{StateStore, StoreSnapshot};
use testkit::{DurableProvider, MemoryProvider, QueueFixture, StoreProvider};

fn assert_no_nested_records(image: &StoreSnapshot, namespace: &NamespaceName, nested: &EntityPath) {
    assert!(
        image.entries().iter().all(|(key, _)| {
            keys::entity_scope_parts(key) != Some((namespace.as_str(), nested.as_str()))
        }),
        "no keyspace tag has a record for the nested shadow"
    );
}

fn explicit_dlq_dead_letter_is_refused<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            lock_duration_millis: 30_000,
            ..QueueConfig::default()
        },
    )?;
    let CommandOutcome::Sent { sequence } = fixture.at(
        10,
        CommandKind::Send {
            message_id: "poison".to_owned(),
            body: b"original body".to_vec(),
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
    )?
    else {
        panic!("expected the original message to be sent");
    };
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        20,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("expected the parent delivery");
    };
    assert_eq!(delivery.sequence, sequence);
    assert_eq!(
        fixture.at(
            30,
            CommandKind::DeadLetter {
                sequence,
                lock_token: delivery.lock.expect("parent delivery is held").token,
                reason: "SchemaMismatch".to_owned(),
                description: "original explanation".to_owned(),
                replacement_envelope: None,
            },
        )?,
        CommandOutcome::DeadLettered,
    );

    let dlq = fixture.entity.dead_letter_queue()?;
    let nested = dlq.dead_letter_queue()?;
    let command = |millis, kind| {
        Command::new(
            fixture.namespace.clone(),
            dlq.clone(),
            Timestamp::from_millis(millis),
            kind,
        )
    };
    let CommandOutcome::Received(Some(delivery)) = fixture.machine.apply(&command(
        40,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    ))?
    else {
        panic!("expected the original message in its DLQ");
    };
    assert_eq!(delivery.sequence, sequence);
    assert_eq!(delivery.body, b"original body".to_vec());
    let lock = delivery.lock.expect("DLQ delivery is held");
    let lock_key = keys::lock(&fixture.namespace, &dlq, lock.locked_until, sequence);
    let original_record = fixture
        .machine
        .message(&fixture.namespace, &dlq, sequence)?;
    assert!(original_record.is_some());
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(40)
    );
    assert!(fixture.machine.store().get(&keys::clock())?.is_some());
    assert!(
        fixture
            .machine
            .store()
            .get(&keys::queue_counters(&fixture.namespace, &fixture.entity))?
            .is_some()
    );
    assert!(
        fixture
            .machine
            .store()
            .get(&keys::queue_counters(&fixture.namespace, &dlq))?
            .is_some()
    );
    assert_eq!(fixture.machine.store().get(&lock_key)?, Some(Vec::new()));
    assert_no_nested_records(&before, &fixture.namespace, &nested);
    validate_catalog(before.entries()).expect("original DLQ image has valid catalog companions");

    // This must refuse before staging any Clock, message, counter or lock change.
    assert_eq!(
        fixture.machine.apply(&command(
            50,
            CommandKind::DeadLetter {
                sequence,
                lock_token: lock.token,
                reason: "MustNotCascade".to_owned(),
                description: "must not replace the original explanation".to_owned(),
                replacement_envelope: None,
            },
        )),
        Err(BrokerError::DeadLetterQueueIsReserved),
    );
    let after = fixture.machine.store().snapshot()?;
    assert_eq!(
        after, before,
        "the entire logical image, including the held lock, is unchanged"
    );
    assert_no_nested_records(&after, &fixture.namespace, &nested);

    // restart drops the sole machine/store handle before real Fjall reopen.
    // Memory uses a fresh shared handle; neither case claims power-cut recovery.
    let fixture = fixture.restart()?;
    let reopened = fixture.machine.store().snapshot()?;
    assert_eq!(reopened, before);
    assert_no_nested_records(&reopened, &fixture.namespace, &nested);
    validate_catalog(reopened.entries()).expect("reopened image has valid catalog companions");
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &dlq, sequence)?,
        original_record
    );
    assert_eq!(fixture.machine.store().get(&lock_key)?, Some(Vec::new()));
    assert_eq!(
        fixture.machine.apply(&Command::new(
            fixture.namespace.clone(),
            dlq.clone(),
            Timestamp::from_millis(60),
            CommandKind::Complete {
                sequence,
                lock_token: lock.token,
            },
        ))?,
        CommandOutcome::Completed,
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &dlq, sequence)?,
        None
    );
    assert_eq!(fixture.machine.store().get(&lock_key)?, None);
    let completed = fixture.machine.store().snapshot()?;
    assert_no_nested_records(&completed, &fixture.namespace, &nested);
    validate_catalog(completed.entries()).expect("completed image has valid catalog companions");
    Ok(())
}

#[test]
fn memory_explicit_dead_letter_from_dlq_refuses_without_mutation() -> Result<(), Box<dyn Error>> {
    explicit_dlq_dead_letter_is_refused(MemoryProvider::new())
}

#[test]
fn fjall_explicit_dead_letter_from_dlq_refuses_without_mutation() -> Result<(), Box<dyn Error>> {
    explicit_dlq_dead_letter_is_refused(DurableProvider::temporary()?)
}
