//! Timer topic history expiry remains bounded and independent of activation.
use domain::{
    Command, CommandKind, CommandOutcome, EntityPath, MessageInput, NamespaceName, SequenceNumber,
    StateMachine, TIMER_SCAN_LIMIT, Timestamp, TopicConfig, keys,
};
use server::{Broker, LocalProposer, MAX_ROUNDS_PER_INDEX, ManualClock, TimerWorker};
use storage::{StateStore, WriteBatch};
use testkit::StoreProvider;

type TestResult = Result<(), Box<dyn std::error::Error>>;
fn sweep<P: StoreProvider>(provider: P, corrupt_activation: bool) -> TestResult {
    let ns = NamespaceName::new("n")?;
    let topic = EntityPath::new("t")?;
    let machine = StateMachine::new(provider.open()?);
    machine.apply(&Command::new(
        ns.clone(),
        topic.clone(),
        Timestamp::UNIX_EPOCH,
        CommandKind::CreateTopic {
            config: TopicConfig {
                requires_duplicate_detection: true,
                duplicate_detection_history_millis: 20_000,
                ..TopicConfig::default()
            },
        },
    ))?;
    let count = TIMER_SCAN_LIMIT * MAX_ROUNDS_PER_INDEX + 1;
    let messages = (0..count)
        .map(|index| MessageInput {
            message_id: format!("id-{index:05}"),
            scheduled_enqueue_at: (corrupt_activation && index == 0)
                .then_some(Timestamp::from_millis(20_000)),
            ..MessageInput::default()
        })
        .collect();
    machine.apply(&Command::new(
        ns.clone(),
        topic.clone(),
        Timestamp::UNIX_EPOCH,
        CommandKind::SendBatch { messages },
    ))?;
    if corrupt_activation {
        machine
            .store()
            .apply(WriteBatch::default().delete(keys::message(
                &ns,
                &topic,
                SequenceNumber::new(1),
            )))?;
    }
    let clock = ManualClock::at(20_000);
    let broker = Broker::spawn(LocalProposer::new(machine, clock));
    let handle = broker.handle();
    let timer = TimerWorker::new(&handle);
    let first = timer.sweep_once()?;
    assert_eq!(first.topics_swept, 1);
    assert_eq!(first.duplicate_history_removed as usize, count - 1);
    assert_eq!(first.commands_failed, u32::from(corrupt_activation));
    assert!(first.work_remaining);
    let second = timer.sweep_once()?;
    assert_eq!(second.duplicate_history_removed, 1);
    assert_eq!(second.commands_failed, u32::from(corrupt_activation));
    assert!(!second.work_remaining);
    // Explicit topic maintenance remains independently available after both pages.
    assert_eq!(
        handle.submit_blocking(
            ns.clone(),
            topic.clone(),
            CommandKind::ExpireDuplicateHistory
        )?,
        CommandOutcome::DuplicateHistoryExpired { removed: 0 }
    );
    drop(timer);
    drop(handle);
    drop(broker); // joins the owner and drops the final Fjall handle
    let reopened = StateMachine::new(provider.open()?);
    assert!(
        reopened
            .duplicate_history_deadline(&ns, &topic, "id-00000")?
            .is_none()
    );
    assert!(
        reopened
            .store()
            .scan_prefix(&keys::duplicate_expiry_prefix(&ns, &topic), count)?
            .is_empty()
    );
    Ok(())
}
#[test]
fn bounded_memory() -> TestResult {
    sweep(testkit::MemoryProvider::new(), false)
}
#[test]
fn bounded_fjall() -> TestResult {
    sweep(testkit::DurableProvider::temporary()?, false)
}
#[test]
fn activation_error_memory() -> TestResult {
    sweep(testkit::MemoryProvider::new(), true)
}
#[test]
fn activation_error_fjall() -> TestResult {
    sweep(testkit::DurableProvider::temporary()?, true)
}
