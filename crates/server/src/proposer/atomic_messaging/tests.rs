use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use domain::{DeleteEntityTarget, EntityIncarnationKind, ReceiveMode, SequenceNumber};
use storage::MemoryStore;

use super::*;

#[derive(Clone)]
struct CountingClock {
    now: Timestamp,
    calls: Arc<AtomicUsize>,
}

impl Clock for CountingClock {
    fn now(&self) -> Timestamp {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.now
    }
}

fn fixture(
    config: QueueConfig,
) -> (
    LocalProposer<MemoryStore, CountingClock>,
    EntityBinding,
    Arc<AtomicUsize>,
) {
    let machine = StateMachine::new(MemoryStore::default());
    let namespace = NamespaceName::new("tenant").unwrap();
    let entity = EntityPath::new("orders").unwrap();
    machine
        .apply(&Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(1_000),
            CommandKind::CreateQueue { config },
        ))
        .unwrap();
    let binding = machine
        .bind_entity(&namespace, &entity, &entity, EntityIncarnationKind::Queue)
        .unwrap()
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let proposer = LocalProposer::new(
        machine,
        CountingClock {
            now: Timestamp::from_millis(2_000),
            calls: calls.clone(),
        },
    );
    (proposer, binding, calls)
}

fn send(id: &str) -> CommandKind {
    CommandKind::Send {
        message_id: id.to_owned(),
        body: id.as_bytes().to_vec(),
        time_to_live_millis: Some(5_000),
        session_id: None,
    }
}

#[test]
fn one_clock_read_stamps_every_member_and_its_deadline() {
    let (proposer, binding, calls) = fixture(QueueConfig::default());
    let applied = proposer
        .propose_atomic_messaging(&binding, vec![send("one"), send("two")])
        .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        applied.outcomes,
        vec![
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(1)
            },
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(2)
            }
        ]
    );
    assert_eq!(applied.enqueue_targets, vec![binding.target().clone()]);
    let CommandOutcome::Peeked(messages) = proposer
        .machine()
        .apply(&Command::new(
            binding.namespace().clone(),
            binding.target().clone(),
            Timestamp::from_millis(2_000),
            CommandKind::Peek {
                from_sequence: SequenceNumber::new(0),
                max_messages: 2,
                session_id: None,
            },
        ))
        .unwrap()
    else {
        panic!("peek outcome")
    };
    assert_eq!(messages.len(), 2);
    assert!(
        messages
            .iter()
            .all(|message| message.enqueued_at == Timestamp::from_millis(2_000))
    );
    assert!(
        messages
            .iter()
            .all(|message| message.expires_at == Some(Timestamp::from_millis(7_000)))
    );
}

#[test]
fn empty_group_validates_without_reading_clock_or_committing() {
    let (proposer, binding, calls) = fixture(QueueConfig::default());
    let before = proposer.machine().store().snapshot().unwrap();
    let applied = proposer
        .propose_atomic_messaging(&binding, Vec::new())
        .unwrap();
    assert!(applied.outcomes.is_empty());
    assert!(applied.enqueue_targets.is_empty());
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(proposer.machine().store().snapshot().unwrap(), before);
}

#[test]
fn stale_group_is_rejected_before_host_clock() {
    let (proposer, binding, calls) = fixture(QueueConfig::default());
    for kind in [
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    ] {
        proposer
            .machine()
            .apply(&Command::new(
                binding.namespace().clone(),
                binding.target().clone(),
                Timestamp::from_millis(1_000),
                kind,
            ))
            .unwrap();
    }
    let before = proposer.machine().store().snapshot().unwrap();
    for kinds in [vec![send("stale")], Vec::new()] {
        assert_eq!(
            proposer.propose_atomic_messaging(&binding, kinds),
            Err(ProposeError::Broker(BrokerError::EntityBindingStale))
        );
    }
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(proposer.machine().store().snapshot().unwrap(), before);
}

#[test]
fn unsupported_operation_is_rejected_before_host_clock() {
    let (proposer, binding, calls) = fixture(QueueConfig::default());
    let before = proposer.machine().store().snapshot().unwrap();
    assert_eq!(
        proposer.propose_atomic_messaging(
            &binding,
            vec![
                send("early"),
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None
                }
            ]
        ),
        Err(ProposeError::Broker(
            BrokerError::AtomicMessagingOperationNotSupported
        ))
    );
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(proposer.machine().store().snapshot().unwrap(), before);
}

#[test]
fn session_queue_is_rejected_even_for_an_empty_group() {
    let (proposer, binding, calls) = fixture(QueueConfig {
        requires_session: true,
        ..QueueConfig::default()
    });
    let before = proposer.machine().store().snapshot().unwrap();
    assert_eq!(
        proposer.propose_atomic_messaging(&binding, Vec::new()),
        Err(ProposeError::Broker(
            BrokerError::AtomicMessagingOperationNotSupported
        ))
    );
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(proposer.machine().store().snapshot().unwrap(), before);
}

#[test]
fn large_host_clock_regression_rejects_whole_group() {
    let (mut proposer, binding, calls) = fixture(QueueConfig::default());
    proposer.clock.now = Timestamp::UNIX_EPOCH;
    let before = proposer.machine().store().snapshot().unwrap();
    assert!(matches!(
        proposer.propose_atomic_messaging(&binding, vec![send("one"), send("two")]),
        Err(ProposeError::ClockWentBackward { .. })
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(proposer.machine().store().snapshot().unwrap(), before);
}
