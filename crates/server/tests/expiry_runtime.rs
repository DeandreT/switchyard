//! The timer worker against a real state machine, on both backends.
//!
//! The state machine has no clock, so nothing expires until the worker proposes
//! it. These cases drive a hand-set clock through the same code the process
//! runs, which is the only way to tell that expiry actually happens rather than
//! merely being expressible.

use std::error::Error;

use domain::{
    CommandKind, CommandOutcome, Delivery, EntityPath, NamespaceName, QueueConfig, ReceiveMode,
    SessionId, StateMachine, TIMER_SCAN_LIMIT,
};
use server::{
    Broker, BrokerHandle, LocalProposer, ManualClock, SubmitError, SweepReport, TimerWorker,
};
use testkit::StoreProvider;

const LOCK_MILLIS: u64 = 30_000;

struct Runtime<P: StoreProvider> {
    _provider: P,
    /// Held only to keep the owner thread alive; dropping it stops the broker.
    _broker: Broker,
    handle: BrokerHandle,
    clock: ManualClock,
    namespace: NamespaceName,
    entity: EntityPath,
}

impl<P: StoreProvider> Runtime<P> {
    fn new(provider: P, config: QueueConfig) -> Result<Self, Box<dyn Error>> {
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(provider.open()?),
            clock.clone(),
        ));
        let runtime = Self {
            handle: broker.handle(),
            _broker: broker,
            _provider: provider,
            clock,
            namespace: NamespaceName::new("tenant")?,
            entity: EntityPath::new("orders")?,
        };
        runtime.propose(CommandKind::CreateQueue { config })?;
        Ok(runtime)
    }

    fn propose(&self, kind: CommandKind) -> Result<CommandOutcome, SubmitError> {
        self.handle
            .submit_blocking(self.namespace.clone(), self.entity.clone(), kind)
    }

    fn send(&self, message_id: &str, time_to_live_millis: Option<u64>) -> Result<(), SubmitError> {
        self.propose(CommandKind::Send {
            message_id: message_id.to_owned(),
            body: message_id.as_bytes().to_vec(),
            time_to_live_millis,
            session_id: None,
        })?;
        Ok(())
    }

    fn receive(&self) -> Result<Option<Delivery>, SubmitError> {
        match self.propose(CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        })? {
            CommandOutcome::Received(delivery) => Ok(delivery),
            other => panic!("expected a receive outcome, got {other:?}"),
        }
    }

    fn sweep(&self) -> Result<SweepReport, SubmitError> {
        TimerWorker::new(&self.handle).sweep_once()
    }
}

fn queue_config() -> QueueConfig {
    QueueConfig {
        lock_duration_millis: LOCK_MILLIS,
        ..QueueConfig::default()
    }
}

// ---- the suite -------------------------------------------------------------

fn a_sweep_returns_a_message_whose_lock_elapsed<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let runtime = Runtime::new(provider, queue_config())?;
    runtime.send("first", None)?;
    let delivery = runtime.receive()?.expect("the queue holds one message");
    let locked_until = delivery
        .lock
        .expect("a peek-lock delivery is locked")
        .locked_until
        .as_millis();

    // Nothing is due yet, and without the worker nothing would ever be.
    assert!(runtime.sweep()?.is_idle());
    assert_eq!(runtime.receive()?, None);

    runtime.clock.set(locked_until);
    assert_eq!(
        runtime.sweep()?,
        SweepReport {
            queues_swept: 2, // the queue and its dead-letter shadow
            locks_returned_to_ready: 1,
            ..SweepReport::default()
        }
    );

    let redelivered = runtime.receive()?.expect("the message came back");
    assert_eq!(redelivered.sequence, delivery.sequence);
    assert_eq!(redelivered.delivery_count, 2);
    Ok(())
}

fn a_sweep_dead_letters_a_message_past_its_time_to_live<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let runtime = Runtime::new(provider, queue_config())?;
    runtime.send("perishable", Some(100))?;

    runtime.clock.advance(99);
    assert!(runtime.sweep()?.is_idle());

    runtime.clock.advance(1);
    assert_eq!(
        runtime.sweep()?,
        SweepReport {
            queues_swept: 2, // the queue and its dead-letter shadow
            messages_dead_lettered: 1,
            ..SweepReport::default()
        }
    );
    assert_eq!(runtime.receive()?, None);
    Ok(())
}

fn a_sweep_releases_a_session_whose_lock_elapsed<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let session_id = SessionId::new("cart-1")?;
    let runtime = Runtime::new(
        provider,
        QueueConfig {
            requires_session: true,
            ..queue_config()
        },
    )?;
    runtime.propose(CommandKind::Send {
        message_id: String::from("first"),
        body: Vec::new(),
        time_to_live_millis: None,
        session_id: Some(session_id.clone()),
    })?;

    let CommandOutcome::SessionAccepted(Some(accepted)) =
        runtime.propose(CommandKind::AcceptSession {
            session_id: Some(session_id.clone()),
            lock_duration_millis: None,
        })?
    else {
        panic!("expected the session to be accepted");
    };

    assert!(runtime.sweep()?.is_idle());
    runtime.clock.set(accepted.lock.locked_until.as_millis());
    assert_eq!(
        runtime.sweep()?,
        SweepReport {
            queues_swept: 2, // the queue and its dead-letter shadow
            sessions_released: 1,
            ..SweepReport::default()
        }
    );

    // Released means another receiver can have it.
    let CommandOutcome::SessionAccepted(Some(next)) =
        runtime.propose(CommandKind::AcceptSession {
            session_id: Some(session_id),
            lock_duration_millis: None,
        })?
    else {
        panic!("expected the freed session to be accepted");
    };
    assert_ne!(next.lock.token, accepted.lock.token);
    Ok(())
}

fn one_sweep_drains_a_backlog_larger_than_a_single_command<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let runtime = Runtime::new(provider, queue_config())?;
    let backlog = TIMER_SCAN_LIMIT + 1;
    for index in 0..backlog {
        runtime.send(&format!("message-{index}"), None)?;
    }
    for _ in 0..backlog {
        runtime.receive()?.expect("a message is ready to lock");
    }

    // One command sweeps at most TIMER_SCAN_LIMIT entries, so a sweep that
    // stopped there would leave one lock held. The worker proposes again.
    runtime.clock.advance(LOCK_MILLIS);
    assert_eq!(
        runtime.sweep()?,
        SweepReport {
            queues_swept: 2, // the queue and its dead-letter shadow
            locks_returned_to_ready: backlog as u32,
            ..SweepReport::default()
        }
    );
    assert!(runtime.receive()?.is_some());
    Ok(())
}

fn a_sweep_never_moves_the_applied_clock_backward<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let runtime = Runtime::new(provider, queue_config())?;
    runtime.send("first", None)?;
    let applied = runtime.handle.last_applied_blocking()?;

    // A host clock that steps back a little must not stall the timer: the
    // state machine refuses a command that precedes what it applied.
    runtime.clock.set(applied.as_millis() - 1);
    assert!(runtime.sweep().is_ok());
    assert_eq!(runtime.handle.last_applied_blocking()?, applied);
    Ok(())
}

// ---- instantiation ---------------------------------------------------------

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory {
            $(
                #[test]
                fn $case() -> Result<(), Box<dyn std::error::Error>> {
                    super::$case(::testkit::MemoryProvider::new())
                }
            )+
        }

        mod durable {
            $(
                #[test]
                fn $case() -> Result<(), Box<dyn std::error::Error>> {
                    super::$case(::testkit::DurableProvider::temporary()?)
                }
            )+
        }
    };
}

for_each_backend! {
    a_sweep_returns_a_message_whose_lock_elapsed,
    a_sweep_dead_letters_a_message_past_its_time_to_live,
    a_sweep_releases_a_session_whose_lock_elapsed,
    one_sweep_drains_a_backlog_larger_than_a_single_command,
    a_sweep_never_moves_the_applied_clock_backward,
}
