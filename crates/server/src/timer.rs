//! The worker that proposes scheduled activation and expiry commands.
//!
//! The state machine has no clock of its own, so nothing expires until something
//! asks it to. This is that something: on every tick it walks the queues and
//! topics, activates scheduled messages, and proposes the four queue-only
//! expiry commands. Without it, scheduled messages remain hidden, locks are
//! held forever, messages outlive their time to live, and duplicate history
//! grows forever.
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

/// Topics one sweep will visit. Topics have their own cursor so a large queue
/// catalog cannot delay scheduled publications and vice versa.
pub const MAX_TOPICS_PER_SWEEP: usize = 1_024;

/// Times one sweep will re-propose against a single index before moving on.
///
/// A sweep command processes at most [`TIMER_SCAN_LIMIT`] entries, so a backlog
/// needs several. Bounding the rounds keeps one entity's backlog from starving
/// every other entity on the tick.
pub const MAX_ROUNDS_PER_INDEX: usize = 8;

pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Default)]
struct CatalogSweepState {
    after: Option<(NamespaceName, EntityPath)>,
    /// Once an entity saturates, keep sweeping without delay until one complete
    /// follow-up catalog cycle finishes without another saturation. This also
    /// covers a busy entity on a later page after the cursor wraps to page one.
    retry_until_clean_cycle: bool,
    saturated_this_cycle: bool,
}

impl CatalogSweepState {
    fn finish_page(
        &mut self,
        next: Option<(NamespaceName, EntityPath)>,
        page_saturated: bool,
    ) -> bool {
        if page_saturated {
            self.retry_until_clean_cycle = true;
            self.saturated_this_cycle = true;
        }
        self.after = next;
        if self.after.is_none() {
            if self.saturated_this_cycle {
                self.saturated_this_cycle = false;
            } else {
                self.retry_until_clean_cycle = false;
            }
        }
        self.retry_until_clean_cycle
    }
}

/// What one sweep did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SweepReport {
    pub queues_swept: usize,
    pub topics_swept: usize,
    pub scheduled_activated: u32,
    pub duplicate_history_removed: u32,
    pub locks_returned_to_ready: u32,
    pub messages_dead_lettered: u32,
    pub sessions_released: u32,
    /// At least one bounded index round filled every attempt. The run loop
    /// immediately follows up instead of adding timer-interval latency.
    pub work_remaining: bool,
    /// Maintenance failures do not prevent unrelated entities or catalog
    /// families from being swept; they remain visible here and in warning logs.
    pub commands_failed: u32,
}

impl SweepReport {
    /// True when the sweep changed nothing, which is the steady state.
    pub fn is_idle(&self) -> bool {
        self.scheduled_activated == 0
            && self.duplicate_history_removed == 0
            && self.locks_returned_to_ready == 0
            && self.messages_dead_lettered == 0
            && self.sessions_released == 0
            && !self.work_remaining
            && self.commands_failed == 0
    }
}

pub struct TimerWorker<'a> {
    broker: &'a BrokerHandle,
    queue_catalog: Mutex<CatalogSweepState>,
    topic_catalog: Mutex<CatalogSweepState>,
}

impl<'a> TimerWorker<'a> {
    pub fn new(broker: &'a BrokerHandle) -> Self {
        Self {
            broker,
            queue_catalog: Mutex::new(CatalogSweepState::default()),
            topic_catalog: Mutex::new(CatalogSweepState::default()),
        }
    }

    /// Proposes one bounded page of topic activation followed by queue
    /// maintenance.
    ///
    /// Topic work runs first, so a busy or malformed queue cannot delay due
    /// publications. An entity-scoped failure is recorded and the sweep moves
    /// on; catalog failures are recorded after both families have had a chance
    /// to run, while a stopped broker is returned to the caller. Every command
    /// remains independently atomic.
    pub fn sweep_once(&self) -> Result<SweepReport, SubmitError> {
        let mut report = SweepReport::default();
        let topic_result = self.sweep_topics(&mut report);
        let queue_result = self.sweep_queues(&mut report);
        let mut stopped = None;
        for (family, result) in [("topic", topic_result), ("queue", queue_result)] {
            if let Err(error) = result {
                if error == SubmitError::BrokerStopped {
                    if stopped.is_none() {
                        stopped = Some(error);
                    }
                } else {
                    report.commands_failed = report.commands_failed.saturating_add(1);
                    warn!(family, %error, "entity catalog scan failed; continuing timer");
                }
            }
        }
        match stopped {
            Some(error) => Err(error),
            None => Ok(report),
        }
    }

    fn sweep_topics(&self, report: &mut SweepReport) -> Result<(), SubmitError> {
        // Hold each cursor for its complete family sweep so concurrent callers
        // cannot fetch the same page and skip the page that follows it.
        let mut catalog = self
            .topic_catalog
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut topics = self
            .broker
            .topics_after_blocking(catalog.after.as_ref(), MAX_TOPICS_PER_SWEEP + 1)?;
        let next_topic_cursor = if topics.len() > MAX_TOPICS_PER_SWEEP {
            topics.truncate(MAX_TOPICS_PER_SWEEP);
            topics.last().cloned()
        } else {
            None
        };
        let previous_work_remaining = std::mem::take(&mut report.work_remaining);
        report.topics_swept = topics.len();
        for (namespace, entity) in topics {
            if let Err(error) = self.activate_scheduled_topic(&namespace, &entity, report) {
                record_entity_failure(report, &error)?;
                warn!(%namespace, %entity, %error, "topic activation failed; continuing sweep");
            }
        }
        let page_saturated = report.work_remaining;
        report.work_remaining =
            previous_work_remaining || catalog.finish_page(next_topic_cursor, page_saturated);
        Ok(())
    }

    fn sweep_queues(&self, report: &mut SweepReport) -> Result<(), SubmitError> {
        let mut catalog = self
            .queue_catalog
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut queues = self
            .broker
            .queues_after_blocking(catalog.after.as_ref(), MAX_QUEUES_PER_SWEEP + 1)?;
        let next_cursor = if queues.len() > MAX_QUEUES_PER_SWEEP {
            queues.truncate(MAX_QUEUES_PER_SWEEP);
            queues.last().cloned()
        } else {
            // This page reached the end. The next tick wraps to the first queue,
            // including any queue inserted before the old cursor meanwhile.
            None
        };
        let previous_work_remaining = std::mem::take(&mut report.work_remaining);
        report.queues_swept = queues.len();

        for (namespace, entity) in queues {
            let result = self.sweep_queue(&namespace, &entity, report);
            if let Err(error) = result {
                record_entity_failure(report, &error)?;
                warn!(%namespace, %entity, %error, "queue maintenance failed; continuing sweep");
            }
        }
        let page_saturated = report.work_remaining;
        report.work_remaining =
            previous_work_remaining || catalog.finish_page(next_cursor, page_saturated);
        Ok(())
    }

    fn sweep_queue(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        report: &mut SweepReport,
    ) -> Result<(), SubmitError> {
        self.activate_scheduled(namespace, entity, report)?;
        self.expire_duplicate_history(namespace, entity, report)?;
        self.expire_locks(namespace, entity, report)?;
        self.expire_messages(namespace, entity, report)?;
        self.expire_session_locks(namespace, entity, report)
    }

    fn activate_scheduled(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        report: &mut SweepReport,
    ) -> Result<(), SubmitError> {
        for round in 0..MAX_ROUNDS_PER_INDEX {
            let outcome = self.broker.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::ActivateScheduled,
            )?;
            let CommandOutcome::ScheduledActivated { activated, .. } = outcome else {
                return Err(unexpected(outcome));
            };
            report.scheduled_activated += activated;

            if (activated as usize) < TIMER_SCAN_LIMIT {
                return Ok(());
            }
            if round + 1 == MAX_ROUNDS_PER_INDEX {
                report.work_remaining = true;
            }
        }
        Ok(())
    }

    fn activate_scheduled_topic(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        report: &mut SweepReport,
    ) -> Result<(), SubmitError> {
        // Topic command size is fanout-aware. Retry a bounded number of full
        // commands, then ask the run loop for an immediate follow-up sweep.
        for round in 0..MAX_ROUNDS_PER_INDEX {
            let outcome = self.broker.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::ActivateScheduled,
            )?;
            let CommandOutcome::ScheduledActivated { activated, .. } = outcome else {
                return Err(unexpected(outcome));
            };
            report.scheduled_activated += activated;
            if activated == 0 {
                return Ok(());
            }
            if round + 1 == MAX_ROUNDS_PER_INDEX {
                report.work_remaining = true;
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
        for round in 0..MAX_ROUNDS_PER_INDEX {
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
                return Ok(());
            }
            if round + 1 == MAX_ROUNDS_PER_INDEX {
                report.work_remaining = true;
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
        for round in 0..MAX_ROUNDS_PER_INDEX {
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
                return Ok(());
            }
            if round + 1 == MAX_ROUNDS_PER_INDEX {
                report.work_remaining = true;
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
        for round in 0..MAX_ROUNDS_PER_INDEX {
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
                return Ok(());
            }
            if round + 1 == MAX_ROUNDS_PER_INDEX {
                report.work_remaining = true;
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
        for round in 0..MAX_ROUNDS_PER_INDEX {
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
                return Ok(());
            }
            if round + 1 == MAX_ROUNDS_PER_INDEX {
                report.work_remaining = true;
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
        let mut delay = interval;
        while !shutdown.wait_for(delay) {
            delay = interval;
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
                        scheduled_activated = report.scheduled_activated,
                        duplicate_history_removed = report.duplicate_history_removed,
                        locks_returned_to_ready = report.locks_returned_to_ready,
                        messages_dead_lettered = report.messages_dead_lettered,
                        sessions_released = report.sessions_released,
                        commands_failed = report.commands_failed,
                        work_remaining = report.work_remaining,
                        "sweep applied timer work"
                    );
                    delay = next_sweep_delay(interval, &report);
                }
                Err(error) => warn!(%error, "sweep failed, retrying on the next tick"),
            }
        }
    }
}

fn next_sweep_delay(interval: Duration, report: &SweepReport) -> Duration {
    if report.work_remaining {
        Duration::ZERO
    } else {
        interval
    }
}

fn unexpected(outcome: CommandOutcome) -> SubmitError {
    SubmitError::Propose(ProposeError::UnexpectedOutcome {
        outcome: format!("{outcome:?}"),
    })
}

fn record_entity_failure(report: &mut SweepReport, error: &SubmitError) -> Result<(), SubmitError> {
    if error == &SubmitError::BrokerStopped {
        return Err(error.clone());
    }
    report.commands_failed = report.commands_failed.saturating_add(1);
    Ok(())
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
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::Instant,
    };

    use domain::{
        QueueConfig, ReceiveMode, StateMachine, SubscriptionConfig, SubscriptionName, Timestamp,
        TopicConfig,
    };
    use storage::{Key, MemoryStore, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};

    use super::*;
    use crate::{Broker, LocalProposer, ManualClock};

    /// Long enough that waiting it out is unmistakable in the elapsed time, so
    /// these cannot pass by timing out and reading a flag that turned true in
    /// the meantime.
    const NEVER: Duration = Duration::from_secs(30);
    const PROMPTLY: Duration = Duration::from_secs(5);

    /// Fails the second top-level entity-catalog scan. A topic-first timer sees
    /// its topic catalog and activates due work before this simulates the queue
    /// catalog becoming unavailable.
    #[derive(Clone, Debug, Default)]
    struct FailSecondCatalogStore {
        inner: MemoryStore,
        catalog_scans: Arc<AtomicUsize>,
    }

    impl StateStore for FailSecondCatalogStore {
        fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
            self.inner.get(key)
        }

        fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
            self.inner.apply(batch)
        }

        fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
            self.inner.snapshot()
        }

        fn scan_from(
            &self,
            prefix: &[u8],
            start: &[u8],
            limit: usize,
        ) -> Result<Vec<(Key, Value)>, StorageError> {
            if prefix.len() == 1 && self.catalog_scans.fetch_add(1, Ordering::SeqCst) == 1 {
                return Err(StorageError::Backend {
                    operation: "scan the queue catalog",
                    detail: String::from("injected timer isolation failure"),
                });
            }
            self.inner.scan_from(prefix, start, limit)
        }
    }

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
    fn bounded_backlog_requests_an_immediate_follow_up() {
        let interval = Duration::from_secs(1);
        let report = SweepReport {
            work_remaining: true,
            ..SweepReport::default()
        };

        assert!(!report.is_idle());
        assert_eq!(next_sweep_delay(interval, &report), Duration::ZERO);
        assert_eq!(
            next_sweep_delay(interval, &SweepReport::default()),
            interval
        );
    }

    #[test]
    fn a_broker_stop_is_never_downgraded_to_an_entity_failure() {
        let mut report = SweepReport::default();

        assert_eq!(
            record_entity_failure(&mut report, &SubmitError::BrokerStopped),
            Err(SubmitError::BrokerStopped)
        );
        assert_eq!(report.commands_failed, 0);
    }

    #[test]
    fn a_queue_catalog_failure_does_not_block_due_topic_activation() -> Result<(), SubmitError> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(FailSecondCatalogStore::default()),
            ManualClock::at(1_000),
        ));
        let namespace = NamespaceName::new("tenant").expect("a valid namespace");
        let topic = EntityPath::new("events").expect("a valid topic");
        broker.handle().submit_blocking(
            namespace.clone(),
            topic.clone(),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        let subscription = match broker.handle().submit_blocking(
            namespace.clone(),
            topic.clone(),
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("all").expect("a valid subscription"),
                config: SubscriptionConfig::default(),
            },
        )? {
            CommandOutcome::SubscriptionCreated { entity } => entity,
            other => return Err(unexpected(other)),
        };
        broker.handle().submit_blocking(
            namespace.clone(),
            topic,
            CommandKind::Send {
                message_id: String::from("survives-queue-catalog-failure"),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: Some(Timestamp::from_millis(1_000)),
                envelope: None,
            },
        )?;

        let report = TimerWorker::new(&broker.handle()).sweep_once()?;
        assert_eq!(report.commands_failed, 1);
        assert_eq!(report.scheduled_activated, 1);
        let outcome = broker.handle().submit_blocking(
            namespace,
            subscription,
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        )?;
        let CommandOutcome::Received(Some(delivery)) = outcome else {
            return Err(unexpected(outcome));
        };
        assert_eq!(delivery.message_id, "survives-queue-catalog-failure");
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

    #[test]
    fn a_sweep_discovers_topics_and_activates_due_publications() -> Result<(), SubmitError> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            ManualClock::at(1_000),
        ));
        let namespace = NamespaceName::new("tenant").expect("a valid namespace");
        let topic = EntityPath::new("events").expect("a valid topic");
        assert_eq!(
            broker.handle().submit_blocking(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
            )?,
            CommandOutcome::TopicCreated
        );
        let subscription = match broker.handle().submit_blocking(
            namespace.clone(),
            topic.clone(),
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("all").expect("a valid subscription"),
                config: SubscriptionConfig::default(),
            },
        )? {
            CommandOutcome::SubscriptionCreated { entity } => entity,
            other => return Err(unexpected(other)),
        };
        assert!(matches!(
            broker.handle().submit_blocking(
                namespace.clone(),
                topic,
                CommandKind::Send {
                    message_id: String::from("scheduled-topic-message"),
                    body: b"scheduled".to_vec(),
                    time_to_live_millis: None,
                    session_id: None,
                    scheduled_enqueue_at: Some(Timestamp::from_millis(1_000)),
                    envelope: None,
                },
            )?,
            CommandOutcome::Published {
                subscriptions,
                ..
            } if subscriptions.is_empty()
        ));

        let report = TimerWorker::new(&broker.handle()).sweep_once()?;
        assert_eq!(report.topics_swept, 1);
        assert_eq!(report.scheduled_activated, 1);

        let outcome = broker.handle().submit_blocking(
            namespace,
            subscription,
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        )?;
        let CommandOutcome::Received(Some(delivery)) = outcome else {
            return Err(unexpected(outcome));
        };
        assert_eq!(delivery.message_id, "scheduled-topic-message");
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

    #[test]
    fn a_saturated_topic_retries_across_catalog_pagination() -> Result<(), SubmitError> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            ManualClock::at(10_000),
        ));
        let namespace = NamespaceName::new("tenant").expect("a valid namespace");
        let mut backlogged = None;
        for index in 0..=MAX_TOPICS_PER_SWEEP {
            let topic = EntityPath::new(format!("topic-{index:04}"))
                .expect("a generated topic path is valid");
            broker.handle().submit_blocking(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
            )?;
            if index == MAX_TOPICS_PER_SWEEP {
                backlogged = Some(topic.clone());
            }
        }
        let backlogged = backlogged.expect("the final-page topic was created");

        // Eight full activation commands leave one due publication behind on
        // the final page. The retry signal must survive the wrap and idle first
        // page so the cursor reaches this page again without a timer interval.
        for index in 0..=(TIMER_SCAN_LIMIT * MAX_ROUNDS_PER_INDEX) {
            assert!(matches!(
                broker.handle().submit_blocking(
                    namespace.clone(),
                    backlogged.clone(),
                    CommandKind::Send {
                        message_id: format!("backlog-{index}"),
                        body: Vec::new(),
                        time_to_live_millis: None,
                        session_id: None,
                        scheduled_enqueue_at: Some(Timestamp::from_millis(10_000)),
                        envelope: None,
                    },
                )?,
                CommandOutcome::Published { .. }
            ));
        }

        let timer_handle = broker.handle();
        let worker = TimerWorker::new(&timer_handle);
        let first = worker.sweep_once()?;
        assert_eq!(first.topics_swept, MAX_TOPICS_PER_SWEEP);
        assert_eq!(first.scheduled_activated, 0);
        assert!(!first.work_remaining);

        let second = worker.sweep_once()?;
        assert_eq!(second.topics_swept, 1);
        assert_eq!(
            second.scheduled_activated as usize,
            TIMER_SCAN_LIMIT * MAX_ROUNDS_PER_INDEX
        );
        assert!(second.work_remaining);

        let third = worker.sweep_once()?;
        assert_eq!(third.topics_swept, MAX_TOPICS_PER_SWEEP);
        assert_eq!(third.scheduled_activated, 0);
        assert!(
            third.work_remaining,
            "the saturated final page must request an immediate traversal back to itself"
        );

        let fourth = worker.sweep_once()?;
        assert_eq!(fourth.topics_swept, 1);
        assert_eq!(fourth.scheduled_activated, 1);
        assert!(!fourth.work_remaining, "the backlog is now fully drained");
        Ok(())
    }
}
