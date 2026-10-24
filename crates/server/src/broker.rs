//! The one owner of the state machine.
//!
//! [`domain::StateMachine::apply`] reads the records a command touches and then
//! commits one batch. That is atomic against a crash but not against a second
//! caller: two threads applying at once can both read the same counter and both
//! write from it, so one message's sequence number silently overwrites another's.
//! Every command therefore goes through a single owner thread, and callers hand
//! it work rather than touching the machine themselves.
//!
//! That is also the shape consensus imposes later. A replicated node takes its
//! order from the log instead of from this channel, but there is still exactly
//! one thing applying commands in one order.
//!
//! A request carries the command's *intent*, not a finished command. Stamping
//! happens on the owner thread, because a timestamp chosen before queueing could
//! reach the machine out of order and be refused.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
};

use admin_api::{EntityTarget, ServiceError};
use domain::{
    AdminCatalogCursor, AdminCatalogEntry, AdminCatalogKind, AdminCatalogPage, CommandKind,
    CommandOutcome, EntityBinding, EntityPath, NamespaceName, Timestamp,
};
use storage::StateStore;
use thiserror::Error;
use tokio::sync::Notify;
use tracing::debug;

use crate::{AdminError, Clock, LocalProposer, ProposeError};

/// Commands that may be waiting ahead of a caller's own.
///
/// Bounded, so a flood of clients cannot grow the queue without limit. A full
/// queue makes senders wait, which is the backpressure the protocol edge turns
/// into flow control.
const COMMAND_QUEUE_DEPTH: usize = 1_024;

enum Request {
    AdminGet {
        namespace: NamespaceName,
        target: EntityTarget,
        reply: flume::Sender<Result<Option<AdminCatalogEntry>, AdminError>>,
    },
    AdminList {
        namespace: NamespaceName,
        kind: AdminCatalogKind,
        after: Option<AdminCatalogCursor>,
        limit: usize,
        reply: flume::Sender<Result<AdminCatalogPage, AdminError>>,
    },
    Bind {
        namespace: NamespaceName,
        entity: EntityPath,
        reply: flume::Sender<Result<EntityBinding, ProposeError>>,
    },
    ApplyBound {
        binding: EntityBinding,
        kind: Box<CommandKind>,
        reply: flume::Sender<Result<CommandOutcome, ProposeError>>,
    },
    Apply {
        namespace: NamespaceName,
        entity: EntityPath,
        kind: Box<CommandKind>,
        reply: flume::Sender<Result<CommandOutcome, ProposeError>>,
    },
    /// Reading which queues exist does not race the way applying does, but it
    /// goes through the owner anyway so that a handle needs no type parameter
    /// and the protocol edge never has to name a backend.
    ListQueues {
        after: Option<(NamespaceName, EntityPath)>,
        limit: usize,
        reply: flume::Sender<Result<Vec<(NamespaceName, EntityPath)>, ProposeError>>,
    },
    /// Topics are catalogued separately because only scheduled activation runs
    /// against them; queue expiry and lock maintenance do not apply.
    ListTopics {
        after: Option<(NamespaceName, EntityPath)>,
        limit: usize,
        reply: flume::Sender<Result<Vec<(NamespaceName, EntityPath)>, ProposeError>>,
    },
    /// The highest timestamp the machine has applied. Readiness and
    /// diagnostics need it, and it is what a caller compares its own clock
    /// against.
    LastApplied {
        reply: flume::Sender<Result<Timestamp, ProposeError>>,
    },
    Stop,
}

/// Wakes the links waiting on an entity when a command makes it worth asking
/// again.
///
/// One notify per entity, permit-style: a notification with nobody waiting is
/// kept and satisfies the next waiter immediately, which is what closes the gap
/// between a receive that found nothing and the wait that follows it.
#[derive(Debug, Default)]
struct Watchers {
    entities: Mutex<HashMap<(NamespaceName, EntityPath), Arc<Notify>>>,
}

impl Watchers {
    fn watch(&self, namespace: &NamespaceName, entity: &EntityPath) -> Arc<Notify> {
        let mut entities = self
            .entities
            .lock()
            .expect("the watcher lock is not poisoned");
        Arc::clone(
            entities
                .entry((namespace.clone(), entity.clone()))
                .or_default(),
        )
    }

    fn notify(&self, namespace: &NamespaceName, entity: &EntityPath) {
        let entities = self
            .entities
            .lock()
            .expect("the watcher lock is not poisoned");
        // Nothing is created here: an entity nobody has ever waited on needs no
        // notification.
        if let Some(notify) = entities.get(&(namespace.clone(), entity.clone())) {
            notify.notify_one();
        }
    }

    /// Wakes the concrete receive paths made deliverable by an outcome.
    ///
    /// A queue command affects the entity it was submitted against. A topic
    /// publish is different: the topic itself is never receivable, and fanout
    /// may populate several subscriptions. The domain returns those paths so
    /// every already-waiting subscription link gets its own permit.
    fn notify_outcome(
        &self,
        namespace: &NamespaceName,
        submitted_entity: &EntityPath,
        outcome: &CommandOutcome,
    ) {
        match outcome {
            CommandOutcome::Published { subscriptions, .. } => {
                for entity in subscriptions {
                    self.notify(namespace, entity);
                }
            }
            CommandOutcome::ScheduledActivated {
                deliverable_entities,
                ..
            } => {
                for entity in deliverable_entities {
                    self.notify(namespace, entity);
                }
            }
            outcome if makes_deliverable(outcome) => {
                self.notify(namespace, submitted_entity);
            }
            _ => {}
        }
    }
}

/// Whether applying this outcome may have made something deliverable, which is
/// what a waiting link wants to be woken for.
fn makes_deliverable(outcome: &CommandOutcome) -> bool {
    match outcome {
        CommandOutcome::Sent { .. } => true,
        CommandOutcome::DuplicateSuppressed { .. } => false,
        CommandOutcome::BatchSent { stored, .. } => *stored > 0,
        CommandOutcome::Abandoned { dead_lettered } => !dead_lettered,
        CommandOutcome::LocksExpired {
            returned_to_ready, ..
        } => *returned_to_ready > 0,
        CommandOutcome::SessionReleased => true,
        CommandOutcome::SessionLocksExpired { released } => *released > 0,
        _ => false,
    }
}

/// A cheap, shared way to reach the broker.
///
/// Cloning is how every connection, link, and timer gets one; they all queue
/// onto the same owner.
#[derive(Clone, Debug)]
pub struct BrokerHandle {
    requests: flume::Sender<Request>,
    watchers: Arc<Watchers>,
}

impl BrokerHandle {
    pub(crate) async fn admin_get(
        &self,
        namespace: NamespaceName,
        target: EntityTarget,
    ) -> Result<Option<AdminCatalogEntry>, AdminError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::AdminGet {
                namespace,
                target,
                reply,
            })
            .await
            .map_err(|_| ServiceError::Backend(SubmitError::BrokerStopped))?;
        result
            .recv_async()
            .await
            .map_err(|_| ServiceError::Backend(SubmitError::BrokerStopped))?
    }

    pub(crate) async fn admin_list(
        &self,
        namespace: NamespaceName,
        kind: AdminCatalogKind,
        after: Option<AdminCatalogCursor>,
        limit: usize,
    ) -> Result<AdminCatalogPage, AdminError> {
        let (reply, result) = flume::bounded(1);
        self.requests
            .send_async(Request::AdminList {
                namespace,
                kind,
                after,
                limit,
                reply,
            })
            .await
            .map_err(|_| ServiceError::Backend(SubmitError::BrokerStopped))?;
        result
            .recv_async()
            .await
            .map_err(|_| ServiceError::Backend(SubmitError::BrokerStopped))?
    }

    /// Applies a command, blocking the calling thread until it commits.
    ///
    /// For callers with a thread of their own, such as the timer worker. An
    /// async caller uses [`BrokerHandle::submit`] instead, since this would hold
    /// an executor thread for the length of an fsync.
    pub fn submit_blocking(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, SubmitError> {
        let (reply, outcome) = flume::bounded(1);
        self.requests
            .send(Request::Apply {
                namespace,
                entity,
                kind: Box::new(kind),
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        outcome
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Applies a command without blocking the caller's executor.
    pub async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, SubmitError> {
        let (reply, outcome) = flume::bounded(1);
        self.requests
            .send_async(Request::Apply {
                namespace,
                entity,
                kind: Box::new(kind),
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        outcome
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Resolves a live entity binding on the owner without stamping a command.
    pub fn bind_entity_blocking(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<EntityBinding, SubmitError> {
        let (reply, binding) = flume::bounded(1);
        self.requests
            .send(Request::Bind {
                namespace,
                entity,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        binding
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Resolves a live entity binding without blocking the caller's executor.
    pub async fn bind_entity(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<EntityBinding, SubmitError> {
        let (reply, binding) = flume::bounded(1);
        self.requests
            .send_async(Request::Bind {
                namespace,
                entity,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        binding
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Applies a command through its retained entity binding on the owner.
    pub fn submit_bound_blocking(
        &self,
        binding: EntityBinding,
        kind: CommandKind,
    ) -> Result<CommandOutcome, SubmitError> {
        let (reply, outcome) = flume::bounded(1);
        self.requests
            .send(Request::ApplyBound {
                binding,
                kind: Box::new(kind),
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        outcome
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Applies a bound command without blocking the caller's executor.
    pub async fn submit_bound(
        &self,
        binding: EntityBinding,
        kind: CommandKind,
    ) -> Result<CommandOutcome, SubmitError> {
        let (reply, outcome) = flume::bounded(1);
        self.requests
            .send_async(Request::ApplyBound {
                binding,
                kind: Box::new(kind),
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        outcome
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// The highest timestamp the machine has applied.
    pub fn last_applied_blocking(&self) -> Result<Timestamp, SubmitError> {
        let (reply, applied) = flume::bounded(1);
        self.requests
            .send(Request::LastApplied { reply })
            .map_err(|_| SubmitError::BrokerStopped)?;
        applied
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Every queue in the store, up to `limit`, in key order.
    pub fn queues_blocking(
        &self,
        limit: usize,
    ) -> Result<Vec<(NamespaceName, EntityPath)>, SubmitError> {
        self.queues_after_blocking(None, limit)
    }

    /// Queues strictly after `after`, up to `limit`, in key order.
    pub fn queues_after_blocking(
        &self,
        after: Option<&(NamespaceName, EntityPath)>,
        limit: usize,
    ) -> Result<Vec<(NamespaceName, EntityPath)>, SubmitError> {
        let (reply, queues) = flume::bounded(1);
        self.requests
            .send(Request::ListQueues {
                after: after.cloned(),
                limit,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        queues
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Topics strictly after `after`, up to `limit`, in key order.
    pub fn topics_after_blocking(
        &self,
        after: Option<&(NamespaceName, EntityPath)>,
        limit: usize,
    ) -> Result<Vec<(NamespaceName, EntityPath)>, SubmitError> {
        let (reply, topics) = flume::bounded(1);
        self.requests
            .send(Request::ListTopics {
                after: after.cloned(),
                limit,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        topics
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }
}

/// The owner thread, and the handle onto it.
#[derive(Debug)]
pub struct Broker {
    handle: BrokerHandle,
    owner: Option<JoinHandle<()>>,
}

impl Broker {
    /// Starts the owner thread for `proposer`.
    pub fn spawn<S: StateStore, C: Clock>(proposer: LocalProposer<S, C>) -> Self {
        let (requests, incoming) = flume::bounded::<Request>(COMMAND_QUEUE_DEPTH);
        let watchers = Arc::new(Watchers::default());
        let watching = Arc::clone(&watchers);
        let owner = thread::Builder::new()
            .name(String::from("switchyard-broker"))
            .spawn(move || {
                while let Ok(request) = incoming.recv() {
                    match request {
                        Request::AdminGet {
                            namespace,
                            target,
                            reply,
                        } => {
                            // Both queue/topic probes are inside this one owner
                            // turn; no queued mutation can run between them.
                            let machine = proposer.machine();
                            let entry = (|| match target {
                                EntityTarget::Entity(path) => {
                                    if let Some(queue) = machine.admin_queue(&namespace, &path)? {
                                        Ok(Some(AdminCatalogEntry::Queue(queue)))
                                    } else {
                                        machine
                                            .admin_topic(&namespace, &path)
                                            .map(|topic| topic.map(AdminCatalogEntry::Topic))
                                    }
                                }
                                EntityTarget::Subscription { topic, name, .. } => machine
                                    .admin_subscription(&namespace, &topic, &name)
                                    .map(|subscription| {
                                        subscription.map(AdminCatalogEntry::Subscription)
                                    }),
                            })()
                            .map_err(ServiceError::Catalog);
                            let _ = reply.send(entry);
                        }
                        Request::AdminList {
                            namespace,
                            kind,
                            after,
                            limit,
                            reply,
                        } => {
                            let page = proposer
                                .machine()
                                .admin_catalog_page(&namespace, kind, after.as_ref(), limit)
                                .map_err(ServiceError::Catalog);
                            let _ = reply.send(page);
                        }
                        Request::Bind {
                            namespace,
                            entity,
                            reply,
                        } => {
                            let _ = reply.send(proposer.bind_entity(&namespace, &entity));
                        }
                        Request::ApplyBound {
                            binding,
                            kind,
                            reply,
                        } => {
                            let outcome = proposer.propose_bound(&binding, *kind);
                            if let Ok(outcome) = outcome.as_ref() {
                                watching.notify_outcome(
                                    binding.namespace(),
                                    binding.target(),
                                    outcome,
                                );
                            }
                            let _ = reply.send(outcome);
                        }
                        Request::Apply {
                            namespace,
                            entity,
                            kind,
                            reply,
                        } => {
                            let outcome = proposer.propose(&namespace, &entity, *kind);
                            if let Ok(outcome) = outcome.as_ref() {
                                watching.notify_outcome(&namespace, &entity, outcome);
                            }
                            // A caller that stopped waiting is not an error: the
                            // command still applied, and it gave up, not us.
                            let _ = reply.send(outcome);
                        }
                        Request::ListQueues {
                            after,
                            limit,
                            reply,
                        } => {
                            let _ = reply.send(
                                proposer
                                    .machine()
                                    .queues_after(after.as_ref(), limit)
                                    .map_err(ProposeError::from),
                            );
                        }
                        Request::ListTopics {
                            after,
                            limit,
                            reply,
                        } => {
                            let _ = reply.send(
                                proposer
                                    .machine()
                                    .topics_after(after.as_ref(), limit)
                                    .map_err(ProposeError::from),
                            );
                        }
                        Request::LastApplied { reply } => {
                            let _ = reply.send(
                                proposer
                                    .machine()
                                    .last_applied_time()
                                    .map_err(ProposeError::from),
                            );
                        }
                        Request::Stop => break,
                    }
                }
                debug!("broker owner thread stopped");
            })
            .expect("the broker owner thread can be spawned");

        Self {
            handle: BrokerHandle { requests, watchers },
            owner: Some(owner),
        }
    }

    pub fn handle(&self) -> BrokerHandle {
        self.handle.clone()
    }
}

impl Drop for Broker {
    /// Stops the owner even if handles are still outstanding, then waits for the
    /// command it was applying to finish.
    ///
    /// Queueing the stop rather than closing the channel is what makes this
    /// terminate: dropping this end alone would leave every clone keeping the
    /// thread alive and the join would never return. Once the owner breaks it
    /// drops the receiver, and outstanding handles start reporting
    /// [`SubmitError::BrokerStopped`].
    fn drop(&mut self) {
        let _ = self.handle.requests.send(Request::Stop);
        if let Some(owner) = self.owner.take() {
            let _ = owner.join();
        }
    }
}

/// The broker as the protocol edge sees it.
///
/// Separating a refusal from an unreachable broker is what lets the edge report
/// a condition the client can act on instead of a generic failure.
impl protocol_amqp::Broker for BrokerHandle {
    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl std::future::Future<Output = ()> + Send {
        let notify = self.watchers.watch(namespace, entity);
        async move { notify.notified().await }
    }

    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, protocol_amqp::BrokerRejection> {
        BrokerHandle::submit(self, namespace, entity, kind)
            .await
            .map_err(|error| match error {
                SubmitError::Propose(ProposeError::Broker(refused)) => {
                    protocol_amqp::BrokerRejection::Refused(refused)
                }
                other => protocol_amqp::BrokerRejection::Unavailable(other.to_string()),
            })
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SubmitError {
    #[error("the broker is not running")]
    BrokerStopped,
    #[error(transparent)]
    Propose(#[from] ProposeError),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use domain::{QueueConfig, SequenceNumber, StateMachine};
    use storage::MemoryStore;

    use super::*;
    use crate::ManualClock;

    fn names() -> (NamespaceName, EntityPath) {
        (
            NamespaceName::new("tenant").expect("a valid namespace"),
            EntityPath::new("orders").expect("a valid entity path"),
        )
    }

    fn broker() -> Broker {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            ManualClock::at(1_000),
        ));
        let (namespace, entity) = names();
        broker
            .handle()
            .submit_blocking(
                namespace,
                entity,
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            )
            .expect("the queue is created");
        broker
    }

    fn send(handle: &BrokerHandle, message_id: &str) -> Result<SequenceNumber, SubmitError> {
        let (namespace, entity) = names();
        match handle.submit_blocking(
            namespace,
            entity,
            CommandKind::Send {
                message_id: message_id.to_owned(),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            },
        )? {
            CommandOutcome::Sent { sequence } => Ok(sequence),
            other => panic!("expected a send outcome, got {other:?}"),
        }
    }

    #[test]
    fn a_command_applies_and_reports_its_outcome() -> Result<(), SubmitError> {
        let broker = broker();
        assert_eq!(send(&broker.handle(), "first")?, SequenceNumber::new(1));
        assert_eq!(send(&broker.handle(), "second")?, SequenceNumber::new(2));
        Ok(())
    }

    #[test]
    fn duplicate_suppression_wakes_only_for_messages_that_were_stored() {
        assert!(!makes_deliverable(&CommandOutcome::DuplicateSuppressed {
            sequence: domain::SequenceNumber::new(2),
        }));
        assert!(!makes_deliverable(&CommandOutcome::BatchSent {
            sequences: vec![domain::SequenceNumber::new(3)],
            stored: 0,
        }));
        assert!(makes_deliverable(&CommandOutcome::BatchSent {
            sequences: vec![
                domain::SequenceNumber::new(4),
                domain::SequenceNumber::new(5),
            ],
            stored: 1,
        }));
    }

    #[tokio::test]
    async fn a_publish_wakes_every_populated_subscription_but_not_the_topic() {
        let watchers = Watchers::default();
        let namespace = NamespaceName::new("tenant").expect("valid namespace");
        let topic = EntityPath::new("billing").expect("valid topic");
        let accounting =
            EntityPath::new("billing/subscriptions/accounting").expect("valid subscription");
        let analytics =
            EntityPath::new("billing/subscriptions/analytics").expect("valid subscription");

        let topic_waiter = watchers.watch(&namespace, &topic);
        let accounting_waiter = watchers.watch(&namespace, &accounting);
        let analytics_waiter = watchers.watch(&namespace, &analytics);
        watchers.notify_outcome(
            &namespace,
            &topic,
            &CommandOutcome::Published {
                sequences: vec![SequenceNumber::new(1)],
                subscriptions: vec![accounting, analytics],
            },
        );

        tokio::time::timeout(
            std::time::Duration::from_millis(20),
            accounting_waiter.notified(),
        )
        .await
        .expect("the first populated subscription is notified");
        tokio::time::timeout(
            std::time::Duration::from_millis(20),
            analytics_waiter.notified(),
        )
        .await
        .expect("the second populated subscription is notified");
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                topic_waiter.notified(),
            )
            .await
            .is_err(),
            "a publish must not wake the non-receivable topic"
        );
    }

    #[tokio::test]
    async fn scheduled_topic_activation_wakes_populated_subscriptions_not_the_topic() {
        let watchers = Watchers::default();
        let namespace = NamespaceName::new("tenant").expect("valid namespace");
        let topic = EntityPath::new("billing").expect("valid topic");
        let accounting =
            EntityPath::new("billing/subscriptions/accounting").expect("valid subscription");

        let topic_waiter = watchers.watch(&namespace, &topic);
        let accounting_waiter = watchers.watch(&namespace, &accounting);
        watchers.notify_outcome(
            &namespace,
            &topic,
            &CommandOutcome::ScheduledActivated {
                activated: 1,
                deliverable_entities: vec![accounting],
            },
        );

        tokio::time::timeout(
            std::time::Duration::from_millis(20),
            accounting_waiter.notified(),
        )
        .await
        .expect("the populated subscription is notified");
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                topic_waiter.notified(),
            )
            .await
            .is_err(),
            "scheduled topic activation must not wake the non-receivable topic"
        );
    }

    #[tokio::test]
    async fn scheduled_queue_activation_wakes_the_queue_but_empty_activation_does_not() {
        let watchers = Watchers::default();
        let namespace = NamespaceName::new("tenant").expect("valid namespace");
        let queue = EntityPath::new("orders").expect("valid queue");
        let queue_waiter = watchers.watch(&namespace, &queue);

        watchers.notify_outcome(
            &namespace,
            &queue,
            &CommandOutcome::ScheduledActivated {
                activated: 0,
                deliverable_entities: Vec::new(),
            },
        );
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                queue_waiter.notified(),
            )
            .await
            .is_err(),
            "an empty activation must not wake a waiting receiver"
        );

        watchers.notify_outcome(
            &namespace,
            &queue,
            &CommandOutcome::ScheduledActivated {
                activated: 1,
                deliverable_entities: vec![queue.clone()],
            },
        );
        tokio::time::timeout(
            std::time::Duration::from_millis(20),
            queue_waiter.notified(),
        )
        .await
        .expect("the activated queue is notified");
    }

    #[test]
    fn a_rejection_reaches_the_caller_that_asked_for_it() {
        let broker = broker();
        let (namespace, entity) = names();
        assert_eq!(
            broker.handle().submit_blocking(
                namespace,
                entity,
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            ),
            Err(SubmitError::Propose(ProposeError::Broker(
                domain::BrokerError::QueueAlreadyExists
            )))
        );
    }

    #[test]
    fn listing_queues_reaches_the_same_owner() -> Result<(), SubmitError> {
        let broker = broker();
        let (namespace, entity) = names();
        // A queue and the dead-letter shadow it casts, in key order.
        assert_eq!(
            broker.handle().queues_blocking(16)?,
            vec![
                (namespace.clone(), entity.clone()),
                (
                    namespace,
                    entity.dead_letter_queue().expect("a valid shadow")
                ),
            ]
        );
        Ok(())
    }

    #[test]
    fn concurrent_senders_never_share_a_sequence_number() {
        const SENDERS: u64 = 8;
        const EACH: u64 = 32;

        let broker = broker();
        let sequences = thread::scope(|scope| {
            let workers: Vec<_> = (0..SENDERS)
                .map(|sender| {
                    let handle = broker.handle();
                    scope.spawn(move || {
                        (0..EACH)
                            .map(|index| {
                                send(&handle, &format!("sender-{sender}-{index}"))
                                    .expect("the send applies")
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            workers
                .into_iter()
                .flat_map(|worker| worker.join().expect("a sender thread finishes"))
                .collect::<BTreeSet<_>>()
        });

        // Every sender got a distinct sequence and together they cover the range
        // exactly. Applying from more than one thread would lose some to a
        // read-modify-write race and repeat others.
        assert_eq!(sequences.len() as u64, SENDERS * EACH);
        assert_eq!(
            sequences.iter().next_back(),
            Some(&SequenceNumber::new(SENDERS * EACH))
        );
    }

    #[test]
    fn an_outstanding_handle_does_not_keep_a_stopped_broker_alive() {
        let broker = broker();
        let orphan = broker.handle();

        // Dropping the broker has to stop the owner even though `orphan` still
        // holds a sender, and has to return rather than wait on it.
        drop(broker);
        assert_eq!(send(&orphan, "first"), Err(SubmitError::BrokerStopped));
        assert_eq!(orphan.queues_blocking(16), Err(SubmitError::BrokerStopped));
    }

    #[tokio::test]
    async fn an_async_caller_reaches_the_same_owner() -> Result<(), SubmitError> {
        let broker = broker();
        let (namespace, entity) = names();
        let outcome = broker
            .handle()
            .submit(
                namespace,
                entity,
                CommandKind::Send {
                    message_id: String::from("first"),
                    body: Vec::new(),
                    time_to_live_millis: None,
                    session_id: None,
                    scheduled_enqueue_at: None,
                    envelope: None,
                },
            )
            .await?;

        assert_eq!(
            outcome,
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(1)
            }
        );
        Ok(())
    }

    struct PausedClock {
        entered: std::sync::mpsc::Sender<()>,
        resume: Mutex<std::sync::mpsc::Receiver<()>>,
        first: std::sync::atomic::AtomicBool,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Clock for PausedClock {
        fn now(&self) -> Timestamp {
            use std::sync::atomic::Ordering;
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.first.swap(false, Ordering::SeqCst) {
                self.entered.send(()).expect("the test waits for admission");
                self.resume
                    .lock()
                    .unwrap()
                    .recv_timeout(std::time::Duration::from_secs(30))
                    .expect("the test releases the owner");
            }
            Timestamp::from_millis(1_000)
        }
    }

    fn paused_broker<S: StateStore>(
        store: S,
    ) -> (
        Broker,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::Sender<()>,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let (entered, waiting) = std::sync::mpsc::channel();
        let (resume, paused) = std::sync::mpsc::channel();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store),
            PausedClock {
                entered,
                resume: Mutex::new(paused),
                first: std::sync::atomic::AtomicBool::new(true),
                calls: calls.clone(),
            },
        ));
        (broker, waiting, resume, calls)
    }

    fn enqueue_queue(
        handle: &BrokerHandle,
        entity: &str,
    ) -> flume::Receiver<Result<CommandOutcome, ProposeError>> {
        let (reply, result) = flume::bounded(1);
        handle
            .requests
            .send(Request::Apply {
                namespace: names().0,
                entity: EntityPath::new(entity).unwrap(),
                kind: Box::new(CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                }),
                reply,
            })
            .unwrap();
        result
    }

    struct NoopWake;
    impl std::task::Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    async fn full_channel_cancellation<P: testkit::StoreProvider>(provider: P) {
        use admin_api::{
            CreateEntityRequest, EntityAddress, EntityKind, EntityOptions, GetEntityRequest,
        };
        use std::{
            future::Future,
            sync::atomic::Ordering,
            task::{Context, Poll, Waker},
        };

        let store = provider.open().unwrap();
        let (broker, entered, resume, calls) = paused_broker(store.clone());
        let handle = broker.handle();
        let first = enqueue_queue(&handle, "orders");
        entered
            .recv_timeout(std::time::Duration::from_secs(30))
            .unwrap();
        for _ in 0..COMMAND_QUEUE_DEPTH {
            let (reply, _cancelled) = flume::bounded(1);
            handle
                .requests
                .try_send(Request::Bind {
                    namespace: names().0,
                    entity: names().1,
                    reply,
                })
                .unwrap();
        }
        assert_eq!(
            handle.requests.len(),
            1_024,
            "the production queue bound is unchanged"
        );
        let service = crate::AdminService::new(names().0, handle.clone());
        let mut pending = Box::pin(service.create(CreateEntityRequest {
            namespace: "tenant".into(),
            address: EntityAddress::Entity {
                path: "unadmitted".into(),
            },
            kind: EntityKind::Queue,
            options: EntityOptions::default(),
        }));
        let waker = Waker::from(Arc::new(NoopWake));
        assert!(matches!(
            pending.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        assert_eq!(handle.requests.len(), COMMAND_QUEUE_DEPTH);
        drop(pending);
        resume.send(()).unwrap();
        assert_eq!(first.recv().unwrap().unwrap(), CommandOutcome::QueueCreated);
        assert!(matches!(
            service
                .get(GetEntityRequest {
                    namespace: "tenant".into(),
                    address: EntityAddress::Entity {
                        path: "unadmitted".into()
                    },
                })
                .await,
            Err(ServiceError::NotFound { .. })
        ));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "cancelled unadmitted Create never stamped"
        );
        let before = store.snapshot().unwrap();
        drop(service);
        drop(handle);
        drop(broker);
        drop(store);
        assert_eq!(provider.open().unwrap().snapshot().unwrap(), before);
    }

    fn queued_stop<P: testkit::StoreProvider>(provider: P) {
        let store = provider.open().unwrap();
        let (broker, entered, resume, _) = paused_broker(store.clone());
        let handle = broker.handle();
        let first = enqueue_queue(&handle, "first");
        entered
            .recv_timeout(std::time::Duration::from_secs(30))
            .unwrap();
        let ahead = enqueue_queue(&handle, "ahead");
        handle.requests.send(Request::Stop).unwrap();
        let behind = enqueue_queue(&handle, "behind");
        resume.send(()).unwrap();
        assert_eq!(first.recv().unwrap().unwrap(), CommandOutcome::QueueCreated);
        assert_eq!(ahead.recv().unwrap().unwrap(), CommandOutcome::QueueCreated);
        assert!(behind.recv().is_err(), "a request behind Stop need not run");
        drop(handle);
        drop(broker);
        let machine = StateMachine::new(store.clone());
        assert!(
            machine
                .admin_queue(&names().0, &EntityPath::new("first").unwrap())
                .unwrap()
                .is_some()
        );
        assert!(
            machine
                .admin_queue(&names().0, &EntityPath::new("ahead").unwrap())
                .unwrap()
                .is_some()
        );
        assert!(
            machine
                .admin_queue(&names().0, &EntityPath::new("behind").unwrap())
                .unwrap()
                .is_none()
        );
        let before = store.snapshot().unwrap();
        drop(machine);
        drop(store);
        assert_eq!(provider.open().unwrap().snapshot().unwrap(), before);
    }

    #[tokio::test]
    async fn memory_full_channel_cancellation_does_not_admit_create() {
        full_channel_cancellation(testkit::MemoryProvider::new()).await;
    }

    #[tokio::test]
    async fn fjall_full_channel_cancellation_does_not_admit_create() {
        full_channel_cancellation(testkit::DurableProvider::temporary().unwrap()).await;
    }

    #[test]
    fn memory_queued_stop_preserves_only_work_ahead() {
        queued_stop(testkit::MemoryProvider::new());
    }

    #[test]
    fn fjall_queued_stop_preserves_only_work_ahead() {
        queued_stop(testkit::DurableProvider::temporary().unwrap());
    }
}
