//! The worker that proposes scheduled activation and expiry commands.
//!
//! The state machine has no clock of its own, so nothing expires until something
//! asks it to. This is that something: on every tick it walks the queues,
//! activates scheduled messages, and proposes the four expiry commands for
//! each. Without it, scheduled messages remain hidden, locks are held forever,
//! messages outlive their time to live, and duplicate history grows forever.
//!
//! The sweep itself is deterministic given the clock, so a test drives it
//! directly and only the surrounding loop deals in real time.

use std::{
    sync::{Condvar, Mutex},
    time::Duration,
};

use domain::{CommandKind, CommandOutcome, EntityPath, NamespaceName, TIMER_SCAN_LIMIT};
use tracing::{debug, warn};

use crate::{BrokerHandle, ProposeError, SubmitError};

/// Queues one sweep will visit. A store with more than this is swept in the
/// order its keys sort, and the worker's cursor resumes with the next page on
/// the following tick.
pub const MAX_QUEUES_PER_SWEEP: usize = 1_024;

/// Times one sweep will re-propose against a single index before moving on.
///
/// A sweep command processes at most [`TIMER_SCAN_LIMIT`] entries, so a backlog
/// needs several. Bounding the rounds keeps one queue's backlog from starving
/// every other queue on the tick.
pub const MAX_ROUNDS_PER_INDEX: usize = 8;

pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// What one sweep did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SweepReport {
    pub queues_swept: usize,
    pub scheduled_activated: u32,
    pub duplicate_history_removed: u32,
    pub locks_returned_to_ready: u32,
    pub messages_dead_lettered: u32,
    pub sessions_released: u32,
}

impl SweepReport {
    /// True when the sweep changed nothing, which is the steady state.
    pub fn is_idle(&self) -> bool {
        self.scheduled_activated == 0
            && self.duplicate_history_removed == 0
            && self.locks_returned_to_ready == 0
            && self.messages_dead_lettered == 0
            && self.sessions_released == 0
    }
}

pub struct TimerWorker<'a> {
    broker: &'a BrokerHandle,
    queue_cursor: Mutex<Option<(NamespaceName, EntityPath)>>,
}

impl<'a> TimerWorker<'a> {
    pub fn new(broker: &'a BrokerHandle) -> Self {
        Self {
            broker,
            queue_cursor: Mutex::new(None),
        }
    }

    /// Proposes one round of timer commands for every queue in the store.
    ///
    /// An error abandons the rest of the sweep. Each command was atomic, so what
    /// already applied stands and the next tick resumes from there.
    pub fn sweep_once(&self) -> Result<SweepReport, SubmitError> {
        // Hold the cursor for the complete sweep so concurrent callers cannot
        // fetch the same page and accidentally skip the one that follows it.
        let mut cursor = self
            .queue_cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut queues = self
            .broker
            .queues_after_blocking(cursor.as_ref(), MAX_QUEUES_PER_SWEEP + 1)?;
        let next_cursor = if queues.len() > MAX_QUEUES_PER_SWEEP {
            queues.truncate(MAX_QUEUES_PER_SWEEP);
            queues.last().cloned()
        } else {
            // This page reached the end. The next tick wraps to the first queue,
            // including any queue inserted before the old cursor meanwhile.
            None
        };
        let mut report = SweepReport {
            queues_swept: queues.len(),
            ..SweepReport::default()
        };

        for (namespace, entity) in queues {
            self.activate_scheduled(&namespace, &entity, &mut report)?;
            self.expire_duplicate_history(&namespace, &entity, &mut report)?;
            self.expire_locks(&namespace, &entity, &mut report)?;
            self.expire_messages(&namespace, &entity, &mut report)?;
            self.expire_session_locks(&namespace, &entity, &mut report)?;
        }
        *cursor = next_cursor;
        Ok(report)
    }

    fn activate_scheduled(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        report: &mut SweepReport,
    ) -> Result<(), SubmitError> {
        for _ in 0..MAX_ROUNDS_PER_INDEX {
            let outcome = self.broker.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::ActivateScheduled,
            )?;
            let CommandOutcome::ScheduledActivated { activated } = outcome else {
                return Err(unexpected(outcome));
            };
            report.scheduled_activated += activated;

            if (activated as usize) < TIMER_SCAN_LIMIT {
                break;
            }
        }
        Ok(())
    }

    fn expire_duplicate_history(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        report: &mut SweepReport,
    ) -> Result<(), SubmitError> {
        for _ in 0..MAX_ROUNDS_PER_INDEX {
            let outcome = self.broker.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::ExpireDuplicateHistory,
            )?;
            let CommandOutcome::DuplicateHistoryExpired { removed } = outcome else {
                return Err(unexpected(outcome));
            };
            report.duplicate_history_removed += removed;

            if (removed as usize) < TIMER_SCAN_LIMIT {
                break;
            }
        }
        Ok(())
    }

    fn expire_locks(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        report: &mut SweepReport,
    ) -> Result<(), SubmitError> {
        for _ in 0..MAX_ROUNDS_PER_INDEX {
            let outcome = self.broker.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::ExpireLocks,
            )?;
            let CommandOutcome::LocksExpired {
                returned_to_ready,
                dead_lettered,
            } = outcome
            else {
                return Err(unexpected(outcome));
            };
            report.locks_returned_to_ready += returned_to_ready;
            report.messages_dead_lettered += dead_lettered;

            if ((returned_to_ready + dead_lettered) as usize) < TIMER_SCAN_LIMIT {
                break;
            }
        }
        Ok(())
    }

    fn expire_messages(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        report: &mut SweepReport,
    ) -> Result<(), SubmitError> {
        for _ in 0..MAX_ROUNDS_PER_INDEX {
            let outcome = self.broker.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::ExpireMessages,
            )?;
            let CommandOutcome::MessagesExpired { dead_lettered } = outcome else {
                return Err(unexpected(outcome));
            };
            report.messages_dead_lettered += dead_lettered;

            if (dead_lettered as usize) < TIMER_SCAN_LIMIT {
                break;
            }
        }
        Ok(())
    }

    fn expire_session_locks(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        report: &mut SweepReport,
    ) -> Result<(), SubmitError> {
        for _ in 0..MAX_ROUNDS_PER_INDEX {
            let outcome = self.broker.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::ExpireSessionLocks,
            )?;
            let CommandOutcome::SessionLocksExpired { released } = outcome else {
                return Err(unexpected(outcome));
            };
            report.sessions_released += released;

            if (released as usize) < TIMER_SCAN_LIMIT {
                break;
            }
        }
        Ok(())
    }

    /// Sweeps every `interval` until `shutdown` is signalled.
    ///
    /// A failed sweep is logged rather than fatal: a host clock that stepped
    /// backward recovers on its own once it catches up, and a storage error is
    /// the store's problem to report.
    pub fn run(&self, interval: Duration, shutdown: &Shutdown) {
        while !shutdown.wait_for(interval) {
            match self.sweep_once() {
                Ok(report) if report.is_idle() => {
                    debug!(
                        queues = report.queues_swept,
                        "sweep found nothing to expire"
                    );
                }
                Ok(report) => {
                    debug!(
                        queues = report.queues_swept,
                        scheduled_activated = report.scheduled_activated,
                        duplicate_history_removed = report.duplicate_history_removed,
                        locks_returned_to_ready = report.locks_returned_to_ready,
                        messages_dead_lettered = report.messages_dead_lettered,
                        sessions_released = report.sessions_released,
                        "sweep expired entries"
                    );
                }
                Err(error) => warn!(%error, "sweep failed, retrying on the next tick"),
            }
        }
    }
}

fn unexpected(outcome: CommandOutcome) -> SubmitError {
    SubmitError::Propose(ProposeError::UnexpectedOutcome {
        outcome: format!("{outcome:?}"),
    })
}

/// A latch the timer loop waits on, so shutdown does not wait out a full tick.
#[derive(Debug, Default)]
pub struct Shutdown {
    signalled: Mutex<bool>,
    changed: Condvar,
}

impl Shutdown {
    pub fn signal(&self) {
        if let Ok(mut signalled) = self.signalled.lock() {
            *signalled = true;
            self.changed.notify_all();
        }
    }

    pub fn is_signalled(&self) -> bool {
        self.signalled.lock().map(|state| *state).unwrap_or(true)
    }

    /// Waits up to `timeout`, returning true if shutdown was signalled.
    ///
    /// The condition is checked before waiting and again on every wake, so a
    /// signal that arrives before the wait starts is not lost and a spurious
    /// wake does not cut the interval short.
    ///
    /// A poisoned lock reports shutdown: the thread that held it panicked, and
    /// continuing to sweep past that is worse than stopping.
    fn wait_for(&self, timeout: Duration) -> bool {
        let Ok(signalled) = self.signalled.lock() else {
            return true;
        };
        match self
            .changed
            .wait_timeout_while(signalled, timeout, |signalled| !*signalled)
        {
            Ok((signalled, _)) => *signalled,
            Err(_) => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time::Instant};

    use domain::{QueueConfig, ReceiveMode, StateMachine, Timestamp};
    use storage::MemoryStore;

    use super::*;
    use crate::{Broker, LocalProposer, ManualClock};

    /// Long enough that waiting it out is unmistakable in the elapsed time, so
    /// these cannot pass by timing out and reading a flag that turned true in
    /// the meantime.
    const NEVER: Duration = Duration::from_secs(30);
    const PROMPTLY: Duration = Duration::from_secs(5);

    #[test]
    fn shutdown_wakes_a_waiting_sweep() {
        let shutdown = Arc::new(Shutdown::default());
        assert!(!shutdown.is_signalled());

        let waiter = Arc::clone(&shutdown);
        let started = Instant::now();
        let handle = thread::spawn(move || waiter.wait_for(NEVER));

        shutdown.signal();
        assert!(handle.join().expect("the waiter thread finishes"));
        assert!(shutdown.is_signalled());
        assert!(
            started.elapsed() < PROMPTLY,
            "the wait ran for {:?}, so the signal did not wake it",
            started.elapsed()
        );
    }

    #[test]
    fn a_signal_that_arrives_before_the_wait_is_not_lost() {
        // The waiter may not reach the condition variable until after shutdown
        // was requested. Waiting on the notification alone would miss it and
        // sleep out the whole interval.
        let shutdown = Shutdown::default();
        shutdown.signal();

        let started = Instant::now();
        assert!(shutdown.wait_for(NEVER));
        assert!(
            started.elapsed() < PROMPTLY,
            "the wait ran for {:?} despite shutdown already being signalled",
            started.elapsed()
        );
    }

    #[test]
    fn a_sweep_of_an_empty_store_visits_nothing() -> Result<(), SubmitError> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            ManualClock::at(1_000),
        ));
        let report = TimerWorker::new(&broker.handle()).sweep_once()?;

        assert_eq!(report, SweepReport::default());
        assert!(report.is_idle());
        Ok(())
    }

    #[test]
    fn duplicate_history_cleanup_makes_a_sweep_non_idle() {
        let report = SweepReport {
            duplicate_history_removed: 1,
            ..SweepReport::default()
        };

        assert!(!report.is_idle());
    }

    #[test]
    fn a_sweep_visits_every_queue_in_every_namespace() -> Result<(), SubmitError> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            ManualClock::at(1_000),
        ));
        for (namespace, entity) in [
            ("tenant-a", "orders"),
            ("tenant-a", "invoices"),
            ("tenant-b", "orders"),
        ] {
            broker.handle().submit_blocking(
                NamespaceName::new(namespace).expect("a valid namespace"),
                EntityPath::new(entity).expect("a valid entity path"),
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            )?;
        }

        // Each queue casts a dead-letter shadow, and the sweep visits both.
        assert_eq!(
            TimerWorker::new(&broker.handle())
                .sweep_once()?
                .queues_swept,
            6
        );
        Ok(())
    }

    #[test]
    fn a_sweep_rotates_to_queues_beyond_the_first_page() -> Result<(), SubmitError> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            ManualClock::at(10_000),
        ));
        let namespace = NamespaceName::new("tenant").expect("a valid namespace");

        // CreateQueue also creates its dead-letter shadow. These 513 user queues
        // therefore produce 1,026 independently swept queue records: the final
        // pair sits strictly beyond MAX_QUEUES_PER_SWEEP.
        let mut target = None;
        for index in 0..=(MAX_QUEUES_PER_SWEEP / 2) {
            let entity = EntityPath::new(format!("queue-{index:04}"))
                .expect("a generated entity path is valid");
            broker.handle().submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            )?;
            target = Some(entity);
        }
        let target = target.expect("at least one queue was created");

        assert!(matches!(
            broker.handle().submit_blocking(
                namespace.clone(),
                target.clone(),
                CommandKind::Send {
                    message_id: String::from("scheduled-on-later-page"),
                    body: b"scheduled".to_vec(),
                    time_to_live_millis: None,
                    session_id: None,
                    scheduled_enqueue_at: Some(Timestamp::from_millis(10_000)),
                    envelope: None,
                },
            )?,
            CommandOutcome::Sent { .. }
        ));
        assert!(matches!(
            broker.handle().submit_blocking(
                namespace.clone(),
                target.clone(),
                CommandKind::Send {
                    message_id: String::from("expired-on-later-page"),
                    body: b"expired".to_vec(),
                    time_to_live_millis: Some(0),
                    session_id: None,
                    scheduled_enqueue_at: None,
                    envelope: None,
                },
            )?,
            CommandOutcome::Sent { .. }
        ));

        let timer_handle = broker.handle();
        let worker = TimerWorker::new(&timer_handle);
        let first = worker.sweep_once()?;
        assert_eq!(first.queues_swept, MAX_QUEUES_PER_SWEEP);
        assert!(first.is_idle(), "the first page must not touch the target");

        let second = worker.sweep_once()?;
        assert_eq!(
            second,
            SweepReport {
                queues_swept: 2,
                scheduled_activated: 1,
                messages_dead_lettered: 1,
                ..SweepReport::default()
            },
            "the next tick must resume after the first page"
        );

        let outcome = broker.handle().submit_blocking(
            namespace.clone(),
            target.clone(),
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        )?;
        let CommandOutcome::Received(Some(activated)) = outcome else {
            panic!("expected the scheduled message to activate, got {outcome:?}");
        };
        assert_eq!(activated.message_id, "scheduled-on-later-page");

        let outcome = broker.handle().submit_blocking(
            namespace,
            target
                .dead_letter_queue()
                .expect("the target has a valid dead-letter shadow"),
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        )?;
        let CommandOutcome::Received(Some(expired)) = outcome else {
            panic!("expected expiry on the later page, got {outcome:?}");
        };
        assert_eq!(expired.message_id, "expired-on-later-page");
        Ok(())
    }
}
