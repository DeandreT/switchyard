//! The worker that proposes deadline commands.
//!
//! The state machine has no clock of its own, so nothing expires until something
//! asks it to. This is that something: on every tick it walks queues and topics,
//! proposing queue deadlines, topic activation, and each entity's duplicate-history
//! cleanup. Without it, scheduled messages stay hidden, locks are held forever,
//! and messages outlive their TTL.
//!
//! The sweep itself is deterministic given the clock, so a test drives it
//! directly and only the surrounding loop deals in real time.

use std::{
    sync::{Condvar, Mutex},
    time::Duration,
};

use domain::{
    CommandKind, CommandOutcome, EntityPath, NamespaceName, QueueCursor, TIMER_SCAN_LIMIT,
    TopicCursor,
};
use tracing::{debug, warn};

use crate::{BrokerHandle, ProposeError, SubmitError};

/// Queues one sweep will visit. Successive sweeps page through key order and
/// wrap at the end, so every queue gets a turn.
pub const MAX_QUEUES_PER_SWEEP: usize = domain::MAX_QUEUE_PAGE_SIZE;
/// Topics one sweep visits independently of the queue discovery position.
pub const MAX_TOPICS_PER_SWEEP: usize = domain::MAX_TOPIC_PAGE_SIZE;

/// Times one sweep will re-propose against a single index before moving on.
///
/// A sweep command processes at most [`TIMER_SCAN_LIMIT`] entries, so a backlog
/// needs several. Bounding the rounds keeps one entity's backlog from starving
/// every other entity on the tick. Topic activation can stop at a smaller
/// prefix because its retained fanout has separate byte, value, and copy caps.
pub const MAX_ROUNDS_PER_INDEX: usize = 8;

pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// What one sweep did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SweepReport {
    pub queues_swept: usize,
    pub topics_swept: usize,
    pub locks_returned_to_ready: u32,
    pub messages_dead_lettered: u32,
    pub messages_dropped: u32,
    pub sessions_released: u32,
    pub messages_activated: u32,
    pub duplicate_history_expired: u32,
}

impl SweepReport {
    /// True when the sweep changed nothing, which is the steady state.
    pub fn is_idle(&self) -> bool {
        self.locks_returned_to_ready == 0
            && self.messages_dead_lettered == 0
            && self.messages_dropped == 0
            && self.sessions_released == 0
            && self.messages_activated == 0
            && self.duplicate_history_expired == 0
    }
}

pub struct TimerWorker<'a> {
    broker: &'a BrokerHandle,
    cursors: Mutex<SweepCursors>,
}

#[derive(Default)]
struct SweepCursors {
    queues: Option<QueueCursor>,
    topics: Option<TopicCursor>,
}

impl<'a> TimerWorker<'a> {
    pub fn new(broker: &'a BrokerHandle) -> Self {
        Self {
            broker,
            cursors: Mutex::new(SweepCursors::default()),
        }
    }

    /// Proposes deadlines for one independent bounded page of each entity kind.
    ///
    /// An error abandons the rest of that family's page, but the other family
    /// still runs. Committed commands stand, and each cursor resumes independently.
    pub fn sweep_once(&self) -> Result<SweepReport, SubmitError> {
        let mut cursors = self
            .cursors
            .lock()
            .expect("the timer cursor lock is not poisoned");
        let mut report = SweepReport::default();
        let queues = self.sweep_queues(&mut cursors.queues, &mut report);
        let topics = self.sweep_topics(&mut cursors.topics, &mut report);
        queues?;
        topics?;
        Ok(report)
    }

    fn sweep_queues(
        &self,
        cursor: &mut Option<QueueCursor>,
        report: &mut SweepReport,
    ) -> Result<(), SubmitError> {
        let mut page =
            self.broker
                .queues_page_blocking(None, cursor.clone(), MAX_QUEUES_PER_SWEEP)?;
        if page.queues.is_empty() && cursor.is_some() {
            page = self
                .broker
                .queues_page_blocking(None, None, MAX_QUEUES_PER_SWEEP)?;
        }
        for (namespace, entity) in page.queues {
            // A failed queue must not pin discovery to this page forever.
            *cursor = Some(QueueCursor {
                namespace: namespace.clone(),
                entity: entity.clone(),
            });
            report.queues_swept += 1;
            self.activate_scheduled(&namespace, &entity, report)?;
            self.expire_locks(&namespace, &entity, report)?;
            self.expire_messages(&namespace, &entity, report)?;
            self.expire_session_locks(&namespace, &entity, report)?;
            self.expire_duplicate_history(&namespace, &entity, report)?;
        }
        *cursor = page.continuation;
        Ok(())
    }

    fn sweep_topics(
        &self,
        cursor: &mut Option<TopicCursor>,
        report: &mut SweepReport,
    ) -> Result<(), SubmitError> {
        let mut page =
            self.broker
                .topics_page_blocking(None, cursor.clone(), MAX_TOPICS_PER_SWEEP)?;
        if page.topics.is_empty() && cursor.is_some() {
            page = self
                .broker
                .topics_page_blocking(None, None, MAX_TOPICS_PER_SWEEP)?;
        }
        for (namespace, entity) in page.topics {
            *cursor = Some(TopicCursor {
                namespace: namespace.clone(),
                entity: entity.clone(),
            });
            report.topics_swept += 1;
            self.activate_topic_scheduled(&namespace, &entity, report)?;
            self.expire_duplicate_history(&namespace, &entity, report)?;
        }
        *cursor = page.continuation;
        Ok(())
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
            report.messages_activated += activated;
            if (activated as usize) < TIMER_SCAN_LIMIT {
                break;
            }
        }
        Ok(())
    }

    fn activate_topic_scheduled(
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
            report.messages_activated += activated;
            // A positive prefix can be smaller than TIMER_SCAN_LIMIT when
            // fanout admission fills first. Only zero proves no progress.
            if activated == 0 {
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
                dropped,
            } = outcome
            else {
                return Err(unexpected(outcome));
            };
            report.locks_returned_to_ready += returned_to_ready;
            report.messages_dead_lettered += dead_lettered;
            report.messages_dropped += dropped;

            if ((returned_to_ready + dead_lettered + dropped) as usize) < TIMER_SCAN_LIMIT {
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
            let CommandOutcome::MessagesExpired {
                dead_lettered,
                dropped,
                processed,
            } = outcome
            else {
                return Err(unexpected(outcome));
            };
            report.messages_dead_lettered += dead_lettered;
            report.messages_dropped += dropped;

            if (processed as usize) < TIMER_SCAN_LIMIT {
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
            let CommandOutcome::DuplicateHistoryExpired { expired } = outcome else {
                return Err(unexpected(outcome));
            };
            report.duplicate_history_expired += expired;
            if (expired as usize) < TIMER_SCAN_LIMIT {
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
                        topics = report.topics_swept,
                        "sweep found nothing to expire"
                    );
                }
                Ok(report) => {
                    debug!(
                        queues = report.queues_swept,
                        topics = report.topics_swept,
                        locks_returned_to_ready = report.locks_returned_to_ready,
                        messages_dead_lettered = report.messages_dead_lettered,
                        messages_dropped = report.messages_dropped,
                        sessions_released = report.sessions_released,
                        messages_activated = report.messages_activated,
                        duplicate_history_expired = report.duplicate_history_expired,
                        "sweep applied deadlines"
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

    use domain::{QueueConfig, StateMachine};
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
}
