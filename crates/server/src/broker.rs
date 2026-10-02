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

use domain::{
    CommandKind, CommandOutcome, EntityBinding, EntityPath, NamespaceName, QueueConfig,
    QueueCursor, QueuePage, Timestamp, TopicCursor, TopicPage,
};
use protocol_amqp::{Attachment, EntityAdmission, EntityMetadata};
use storage::StateStore;
use thiserror::Error;
use tokio::sync::{Notify, futures::OwnedNotified};
use tracing::debug;

use crate::{AdminTarget, Clock, LocalProposer, ProposeError};

mod admin_reads;
mod atomic_messaging;
mod atomic_work;
mod bindings;
mod guarded_atomic_messaging;
mod native_atomic_messaging;
mod native_atomic_protocol;
mod protocol;
mod request_queue;

pub use guarded_atomic_messaging::GuardedAtomicSubmitError;
pub use native_atomic_messaging::{NativeAtomicMessagingCompletion, NativeAtomicSubmitError};

/// Commands that may be waiting ahead of a caller's own.
///
/// Bounded, so a flood of clients cannot grow the queue without limit. A full
/// queue makes senders wait, which is the backpressure the protocol edge turns
/// into flow control.
const COMMAND_QUEUE_DEPTH: usize = 1_024;

enum Request {
    ApplyNativeAtomicMessagingOwned {
        submission: Box<protocol_amqp::OwnedNativeAtomicMessagingSubmission>,
        reply: flume::Sender<NativeAtomicMessagingCompletion>,
    },
    ApplyEmptyAtomicMessagingOwned {
        submission: protocol_amqp::OwnedEmptyAtomicMessagingSubmission,
        reply: flume::Sender<Result<domain::AtomicMessagingApplication, GuardedAtomicSubmitError>>,
    },
    ApplyAtomicMessagingOwned {
        submission: protocol_amqp::OwnedAtomicMessagingSubmission,
        reply: flume::Sender<Result<domain::AtomicMessagingApplication, GuardedAtomicSubmitError>>,
    },
    ApplyAtomicMessagingGuarded {
        binding: EntityBinding,
        kinds: Vec<CommandKind>,
        ticket: protocol_amqp::AtomicCommitTicket,
        reply: flume::Sender<Result<domain::AtomicMessagingApplication, GuardedAtomicSubmitError>>,
    },
    ApplyAtomicMessaging {
        binding: EntityBinding,
        kinds: Vec<CommandKind>,
        reply: flume::Sender<Result<domain::AtomicMessagingApplication, ProposeError>>,
    },
    Apply {
        namespace: NamespaceName,
        entity: EntityPath,
        binding: Option<EntityBinding>,
        kind: Box<CommandKind>,
        reply: flume::Sender<Result<CommandOutcome, ProposeError>>,
    },
    /// Reading which queues exist does not race the way applying does, but it
    /// goes through the owner anyway so that a handle needs no type parameter
    /// and the protocol edge never has to name a backend.
    ListQueues {
        limit: usize,
        reply: flume::Sender<Result<Vec<(NamespaceName, EntityPath)>, ProposeError>>,
    },
    ListQueuesPage {
        namespace: Option<NamespaceName>,
        after: Option<QueueCursor>,
        limit: usize,
        reply: flume::Sender<Result<QueuePage, ProposeError>>,
    },
    ListTopicsPage {
        namespace: Option<NamespaceName>,
        after: Option<TopicCursor>,
        limit: usize,
        reply: flume::Sender<Result<TopicPage, ProposeError>>,
    },
    GetQueueConfig {
        namespace: NamespaceName,
        entity: EntityPath,
        reply: flume::Sender<Result<Option<QueueConfig>, ProposeError>>,
    },
    GetEntityMetadata {
        namespace: NamespaceName,
        target: Attachment,
        reply: flume::Sender<Result<Option<EntityMetadata>, ProposeError>>,
    },
    BindEntity {
        namespace: NamespaceName,
        target: Attachment,
        reply: flume::Sender<Result<Option<EntityAdmission>, ProposeError>>,
    },
    GetAdminEntityMetadata {
        namespace: NamespaceName,
        target: AdminTarget,
        reply: flume::Sender<Result<Option<EntityMetadata>, ProposeError>>,
    },
    ListSubscriptions {
        namespace: NamespaceName,
        topic: EntityPath,
        reply: flume::Sender<Result<Vec<domain::SubscriptionDefinition>, ProposeError>>,
    },
    ListRules {
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: domain::SubscriptionName,
        binding: Option<EntityBinding>,
        reply: flume::Sender<Result<Vec<domain::RuleDefinition>, ProposeError>>,
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
/// One broadcast channel per registered entity. Each watch captures its
/// notification before the receive, so even an unpolled wait observes the
/// broadcast. The last completed or canceled watch removes the entry.
#[derive(Debug, Default)]
struct Watchers {
    entities: Mutex<HashMap<(NamespaceName, EntityPath), EntityWaiters>>,
}

#[derive(Debug, Default)]
struct EntityWaiters {
    notify: Arc<Notify>,
    waiter_count: usize,
}

struct EntityWatch {
    watchers: Arc<Watchers>,
    key: (NamespaceName, EntityPath),
    notification: Option<OwnedNotified>,
    #[cfg(test)]
    notify: Arc<Notify>,
}

impl EntityWatch {
    async fn wait(mut self) {
        if let Some(notification) = self.notification.take() {
            notification.await;
        }
    }
}

impl Drop for EntityWatch {
    fn drop(&mut self) {
        let mut entities = self
            .watchers
            .entities
            .lock()
            .expect("the watcher lock is not poisoned");
        let entry = entities
            .get_mut(&self.key)
            .expect("a live watch has an entity registration");
        entry.waiter_count -= 1;
        if entry.waiter_count == 0 {
            entities.remove(&self.key);
        }
    }
}

impl Watchers {
    fn watch(self: &Arc<Self>, namespace: &NamespaceName, entity: &EntityPath) -> EntityWatch {
        let key = (namespace.clone(), entity.clone());
        let mut entities = self
            .entities
            .lock()
            .expect("the watcher lock is not poisoned");
        let entry = entities.entry(key.clone()).or_default();
        entry.waiter_count += 1;
        EntityWatch {
            watchers: Arc::clone(self),
            key,
            notification: Some(Arc::clone(&entry.notify).notified_owned()),
            #[cfg(test)]
            notify: Arc::clone(&entry.notify),
        }
    }

    fn notify(&self, namespace: &NamespaceName, entity: &EntityPath) {
        let notify = self
            .entities
            .lock()
            .expect("the watcher lock is not poisoned")
            .get(&(namespace.clone(), entity.clone()))
            .map(|entry| Arc::clone(&entry.notify));
        // Waking a waiter can drop its registration, which takes the map lock.
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }

    #[cfg(test)]
    fn entry_count(&self) -> usize {
        self.entities
            .lock()
            .expect("the watcher lock is not poisoned")
            .len()
    }

    #[cfg(test)]
    fn waiter_count(&self, namespace: &NamespaceName, entity: &EntityPath) -> usize {
        self.entities
            .lock()
            .expect("the watcher lock is not poisoned")
            .get(&(namespace.clone(), entity.clone()))
            .map_or(0, |entry| entry.waiter_count)
    }
}

/// Whether applying this outcome may have made something deliverable, which is
/// what a waiting link wants to be woken for.
fn makes_deliverable(outcome: &CommandOutcome) -> bool {
    match outcome {
        CommandOutcome::Sent { .. } => true,
        CommandOutcome::BatchSent { sequences } => !sequences.is_empty(),
        CommandOutcome::Scheduled { .. } => true,
        CommandOutcome::ScheduledActivated { activated } => *activated > 0,
        CommandOutcome::Abandoned {
            dead_lettered,
            dropped,
        } => !dead_lettered && !dropped,
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
    requests: request_queue::RequestSender,
    watchers: Arc<Watchers>,
}

impl BrokerHandle {
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
                binding: None,
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
                binding: None,
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

    /// Reads a committed queue configuration on the owner thread.
    pub fn queue_config_blocking(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<Option<QueueConfig>, SubmitError> {
        let (reply, config) = flume::bounded(1);
        self.requests
            .send(Request::GetQueueConfig {
                namespace,
                entity,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        config
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Reads a committed queue configuration without blocking the executor.
    pub async fn queue_config(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<Option<QueueConfig>, SubmitError> {
        let (reply, config) = flume::bounded(1);
        self.requests
            .send_async(Request::GetQueueConfig {
                namespace,
                entity,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        config
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Reads validated link topology on the owner thread without a command stamp.
    pub fn entity_metadata_blocking(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityMetadata>, SubmitError> {
        let (reply, metadata) = flume::bounded(1);
        self.requests
            .send(Request::GetEntityMetadata {
                namespace,
                target,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        metadata
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Reads validated link topology without blocking the executor.
    pub async fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityMetadata>, SubmitError> {
        let (reply, metadata) = flume::bounded(1);
        self.requests
            .send_async(Request::GetEntityMetadata {
                namespace,
                target,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        metadata
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
        let (reply, queues) = flume::bounded(1);
        self.requests
            .send(Request::ListQueues { limit, reply })
            .map_err(|_| SubmitError::BrokerStopped)?;
        queues
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// One exclusive page of queues, optionally scoped to a namespace.
    pub fn queues_page_blocking(
        &self,
        namespace: Option<NamespaceName>,
        after: Option<QueueCursor>,
        limit: usize,
    ) -> Result<QueuePage, SubmitError> {
        let (reply, queues) = flume::bounded(1);
        self.requests
            .send(Request::ListQueuesPage {
                namespace,
                after,
                limit,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        queues
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Discovers a queue page without blocking the caller's executor.
    pub async fn queues_page(
        &self,
        namespace: Option<NamespaceName>,
        after: Option<QueueCursor>,
        limit: usize,
    ) -> Result<QueuePage, SubmitError> {
        let (reply, queues) = flume::bounded(1);
        self.requests
            .send_async(Request::ListQueuesPage {
                namespace,
                after,
                limit,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        queues
            .recv_async()
            .await
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// One exclusive page of topics, optionally scoped to a namespace.
    pub fn topics_page_blocking(
        &self,
        namespace: Option<NamespaceName>,
        after: Option<TopicCursor>,
        limit: usize,
    ) -> Result<TopicPage, SubmitError> {
        let (reply, topics) = flume::bounded(1);
        self.requests
            .send(Request::ListTopicsPage {
                namespace,
                after,
                limit,
                reply,
            })
            .map_err(|_| SubmitError::BrokerStopped)?;
        topics
            .recv()
            .map_err(|_| SubmitError::BrokerStopped)?
            .map_err(SubmitError::Propose)
    }

    /// Discovers a topic page without blocking the caller's executor.
    pub async fn topics_page(
        &self,
        namespace: Option<NamespaceName>,
        after: Option<TopicCursor>,
        limit: usize,
    ) -> Result<TopicPage, SubmitError> {
        let (reply, topics) = flume::bounded(1);
        self.requests
            .send_async(Request::ListTopicsPage {
                namespace,
                after,
                limit,
                reply,
            })
            .await
            .map_err(|_| SubmitError::BrokerStopped)?;
        topics
            .recv_async()
            .await
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
        let (requests, incoming) = request_queue::bounded(COMMAND_QUEUE_DEPTH);
        let watchers = Arc::new(Watchers::default());
        let watching = Arc::clone(&watchers);
        let owner = thread::Builder::new()
            .name(String::from("switchyard-broker"))
            .spawn(move || {
                while let Ok(request) = incoming.recv() {
                    match request {
                        Request::ApplyNativeAtomicMessagingOwned { submission, reply } => {
                            native_atomic_messaging::apply_owned(
                                &proposer,
                                &watching,
                                *submission,
                                reply,
                            );
                        }
                        Request::ApplyEmptyAtomicMessagingOwned { submission, reply } => {
                            atomic_work::apply_empty_owned(submission, reply);
                        }
                        Request::ApplyAtomicMessagingOwned { submission, reply } => {
                            atomic_work::apply_owned(&proposer, &watching, submission, reply);
                        }
                        Request::ApplyAtomicMessagingGuarded {
                            binding,
                            kinds,
                            ticket,
                            reply,
                        } => guarded_atomic_messaging::apply_guarded(
                            &proposer, &watching, binding, kinds, ticket, reply,
                        ),
                        Request::ApplyAtomicMessaging {
                            binding,
                            kinds,
                            reply,
                        } => {
                            let application = proposer.propose_atomic_messaging(&binding, kinds);
                            if let Ok(applied) = &application {
                                for target in &applied.enqueue_targets {
                                    watching.notify(binding.namespace(), target);
                                }
                            }
                            let _ = reply.send(application);
                        }
                        Request::Apply {
                            namespace,
                            entity,
                            binding,
                            kind,
                            reply,
                        } => {
                            let application = match binding {
                                Some(binding) => {
                                    proposer.propose_fenced_with_effects(&binding, &entity, *kind)
                                }
                                None => proposer.propose_with_effects(&namespace, &entity, *kind),
                            };
                            if let Ok(applied) = &application {
                                if let Some(targets) = &applied.entity_deletions {
                                    for target in targets {
                                        watching.notify(&namespace, target);
                                    }
                                }
                                if let Some(targets) = &applied.subscription_enqueues {
                                    for target in targets {
                                        watching.notify(&namespace, target);
                                    }
                                } else if makes_deliverable(&applied.outcome) {
                                    watching.notify(&namespace, &entity);
                                }
                            }
                            if !entity.is_dead_letter_queue()
                                && application.as_ref().is_ok_and(|applied| {
                                    applied.subscription_enqueues.is_none()
                                        && applied.dead_letters_enqueued
                                })
                                && let Ok(shadow) = entity.dead_letter_queue()
                            {
                                watching.notify(&namespace, &shadow);
                            }
                            // A caller that stopped waiting is not an error: the
                            // command still applied, and it gave up, not us.
                            let _ = reply.send(application.map(|applied| applied.outcome));
                        }
                        Request::ListQueues { limit, reply } => {
                            let _ = reply
                                .send(proposer.machine().queues(limit).map_err(ProposeError::from));
                        }
                        Request::ListQueuesPage {
                            namespace,
                            after,
                            limit,
                            reply,
                        } => {
                            let _ = reply.send(proposer.queues_page(
                                namespace.as_ref(),
                                after.as_ref(),
                                limit,
                            ));
                        }
                        Request::ListTopicsPage {
                            namespace,
                            after,
                            limit,
                            reply,
                        } => {
                            let _ = reply.send(proposer.topics_page(
                                namespace.as_ref(),
                                after.as_ref(),
                                limit,
                            ));
                        }
                        Request::GetQueueConfig {
                            namespace,
                            entity,
                            reply,
                        } => {
                            let _ = reply.send(proposer.queue_config(&namespace, &entity));
                        }
                        Request::GetEntityMetadata {
                            namespace,
                            target,
                            reply,
                        } => {
                            let _ = reply.send(proposer.entity_metadata(&namespace, &target));
                        }
                        Request::BindEntity {
                            namespace,
                            target,
                            reply,
                        } => {
                            let _ = reply.send(proposer.bind_entity(&namespace, &target));
                        }
                        Request::GetAdminEntityMetadata {
                            namespace,
                            target,
                            reply,
                        } => {
                            let _ = reply.send(proposer.admin_entity_metadata(&namespace, &target));
                        }
                        Request::ListSubscriptions {
                            namespace,
                            topic,
                            reply,
                        } => {
                            let _ = reply.send(proposer.subscriptions(&namespace, &topic));
                        }
                        Request::ListRules {
                            namespace,
                            topic,
                            subscription,
                            binding,
                            reply,
                        } => {
                            let result = match binding {
                                Some(binding) => {
                                    proposer.rules_fenced(&binding, &topic, &subscription)
                                }
                                None => proposer.rules(&namespace, &topic, &subscription),
                            };
                            let _ = reply.send(result);
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

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SubmitError {
    #[error("the broker is not running")]
    BrokerStopped,
    #[error(transparent)]
    Propose(#[from] ProposeError),
}

#[cfg(test)]
mod wakeup_tests;

#[cfg(test)]
mod deletion_tests;

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, future::Future, task::Poll, time::Duration};

    use domain::{QueueConfig, SequenceNumber, StateMachine};
    use protocol_amqp::Broker as _;
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
    fn only_ready_transitions_wake_the_parent() {
        for (outcome, expected) in [
            (
                CommandOutcome::BatchSent {
                    sequences: Vec::new(),
                },
                false,
            ),
            (
                CommandOutcome::BatchSent {
                    sequences: vec![SequenceNumber::new(1)],
                },
                true,
            ),
            (CommandOutcome::DeadLettered, false),
            (
                CommandOutcome::Abandoned {
                    dead_lettered: true,
                    dropped: false,
                },
                false,
            ),
            (
                CommandOutcome::Abandoned {
                    dead_lettered: false,
                    dropped: false,
                },
                true,
            ),
            (
                CommandOutcome::MessagesExpired {
                    dead_lettered: 1,
                    dropped: 0,
                    processed: 1,
                },
                false,
            ),
            (
                CommandOutcome::MessagesExpired {
                    dead_lettered: 0,
                    dropped: 1,
                    processed: 1,
                },
                false,
            ),
            (
                CommandOutcome::LocksExpired {
                    returned_to_ready: 0,
                    dead_lettered: 1,
                    dropped: 0,
                },
                false,
            ),
            (
                CommandOutcome::LocksExpired {
                    returned_to_ready: 1,
                    dead_lettered: 0,
                    dropped: 0,
                },
                true,
            ),
            (CommandOutcome::Received(None), false),
        ] {
            assert_eq!(makes_deliverable(&outcome), expected);
        }
    }

    #[test]
    fn dropping_an_expired_message_does_not_wake_a_receiver() {
        let outcome = CommandOutcome::Abandoned {
            dead_lettered: false,
            dropped: true,
        };
        assert!(!makes_deliverable(&outcome));
    }

    #[tokio::test]
    async fn a_dead_letter_wakes_an_existing_shadow_waiter() -> Result<(), SubmitError> {
        let broker = broker();
        let handle = broker.handle();
        let (namespace, entity) = names();
        let shadow = entity.dead_letter_queue().expect("a valid shadow");
        let wakeup = handle.deliverable(&namespace, &shadow);
        send(&handle, "poison")?;
        let CommandOutcome::Received(Some(delivery)) = handle
            .submit(
                namespace.clone(),
                entity.clone(),
                CommandKind::Receive {
                    mode: domain::ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None,
                },
            )
            .await?
        else {
            panic!("the message is delivered");
        };
        handle
            .submit(
                namespace.clone(),
                entity,
                CommandKind::DeadLetter {
                    sequence: delivery.sequence,
                    lock_token: delivery.lock.expect("a delivery lock").token,
                    reason: "invalid".to_owned(),
                    description: "cannot process".to_owned(),
                },
            )
            .await?;
        tokio::time::timeout(std::time::Duration::from_millis(250), wakeup)
            .await
            .expect("a dead letter immediately notifies its shadow");
        Ok(())
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

    #[test]
    fn dropping_thousands_of_unpolled_entity_waits_removes_every_entry() {
        let broker = broker();
        let handle = broker.handle();
        let namespace = NamespaceName::new("tenant").expect("namespace");
        let entities = (0..4_096)
            .map(|index| EntityPath::new(format!("missing-{index}")).expect("entity"))
            .collect::<Vec<_>>();
        let waiting = entities
            .iter()
            .map(|entity| handle.deliverable(&namespace, entity))
            .collect::<Vec<_>>();
        assert_eq!(handle.watchers.entry_count(), 4_096);
        assert_eq!(handle.watchers.waiter_count(&namespace, &entities[0]), 1);
        drop(waiting);
        assert_eq!(handle.watchers.entry_count(), 0);
    }

    #[tokio::test]
    async fn shared_entity_waits_keep_the_entry_and_its_notification() {
        let watchers = Arc::new(Watchers::default());
        let (namespace, entity) = names();
        let first = watchers.watch(&namespace, &entity);
        let second = watchers.watch(&namespace, &entity);
        assert!(Arc::ptr_eq(&first.notify, &second.notify));
        assert_eq!(watchers.entry_count(), 1);
        assert_eq!(watchers.waiter_count(&namespace, &entity), 2);
        drop(first);
        assert_eq!(watchers.entry_count(), 1);
        assert_eq!(watchers.waiter_count(&namespace, &entity), 1);
        watchers.notify(&namespace, &entity);
        tokio::time::timeout(Duration::from_millis(250), second.wait())
            .await
            .expect("the remaining registration retains the permit");
        assert_eq!(watchers.entry_count(), 0);
    }

    #[tokio::test]
    async fn a_fulfilled_protocol_wait_removes_its_registration() -> Result<(), SubmitError> {
        let broker = broker();
        let handle = broker.handle();
        let (namespace, entity) = names();
        let waiting = handle.deliverable(&namespace, &entity);
        assert_eq!(handle.watchers.entry_count(), 1);
        send(&handle, "ready-before-poll")?;
        tokio::time::timeout(Duration::from_millis(250), waiting)
            .await
            .expect("registration precedes the receive and the first poll");
        assert_eq!(handle.watchers.entry_count(), 0);
        send(&handle, "no-current-waiter")?;
        assert_eq!(handle.watchers.entry_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn canceling_a_polled_wait_removes_only_its_registration() {
        let broker = broker();
        let handle = broker.handle();
        let (namespace, entity) = names();
        let retained = handle.deliverable(&namespace, &entity);
        let mut canceled = Box::pin(handle.deliverable(&namespace, &entity));
        std::future::poll_fn(|context| {
            assert!(canceled.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(handle.watchers.waiter_count(&namespace, &entity), 2);
        drop(canceled);
        assert_eq!(handle.watchers.entry_count(), 1);
        assert_eq!(handle.watchers.waiter_count(&namespace, &entity), 1);
        handle.watchers.notify(&namespace, &entity);
        tokio::time::timeout(Duration::from_millis(250), retained)
            .await
            .expect("canceling one wait does not discard the other wait's notification");
        assert_eq!(handle.watchers.entry_count(), 0);
    }

    #[test]
    fn a_failed_receive_can_drop_its_unpolled_registration() {
        let broker = broker();
        let handle = broker.handle();
        let namespace = NamespaceName::new("tenant").expect("namespace");
        let entity = EntityPath::new("missing").expect("entity");
        let waiting = handle.deliverable(&namespace, &entity);
        assert_eq!(handle.watchers.entry_count(), 1);
        assert_eq!(
            handle.submit_blocking(
                namespace.clone(),
                entity.clone(),
                CommandKind::Receive {
                    mode: domain::ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None,
                },
            ),
            Err(SubmitError::Propose(ProposeError::Broker(
                domain::BrokerError::QueueNotFound
            )))
        );
        drop(waiting);
        assert_eq!(handle.watchers.entry_count(), 0);
    }

    #[test]
    fn notifications_without_registrations_never_create_entries() {
        let watchers = Arc::new(Watchers::default());
        let namespace = NamespaceName::new("tenant").expect("namespace");
        for index in 0..4_096 {
            let entity = EntityPath::new(format!("missing-{index}")).expect("entity");
            watchers.notify(&namespace, &entity);
        }
        assert_eq!(watchers.entry_count(), 0);
    }

    #[test]
    fn a_reentrant_wake_can_drop_another_registration() {
        use std::task::{Context, Wake, Waker};

        struct CancelOnWake(Mutex<Option<EntityWatch>>);

        impl Wake for CancelOnWake {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }

            fn wake_by_ref(self: &Arc<Self>) {
                drop(self.0.lock().expect("registration").take());
            }
        }

        let watchers = Arc::new(Watchers::default());
        let (namespace, entity) = names();
        let mut waiting = Box::pin(watchers.watch(&namespace, &entity).wait());
        let wake = Arc::new(CancelOnWake(Mutex::new(Some(
            watchers.watch(&namespace, &entity),
        ))));
        let waker = Waker::from(wake);
        let mut context = Context::from_waker(&waker);
        assert!(waiting.as_mut().poll(&mut context).is_pending());
        assert_eq!(watchers.waiter_count(&namespace, &entity), 2);
        watchers.notify(&namespace, &entity);
        assert_eq!(watchers.waiter_count(&namespace, &entity), 1);
        assert!(waiting.as_mut().poll(&mut context).is_ready());
        drop(waiting);
        assert_eq!(watchers.entry_count(), 0);
    }

    #[test]
    fn concurrent_registration_and_cancellation_leave_no_entries() {
        let watchers = Arc::new(Watchers::default());
        let (namespace, entity) = names();
        thread::scope(|scope| {
            for _ in 0..8 {
                let watchers = Arc::clone(&watchers);
                let namespace = &namespace;
                let entity = &entity;
                scope.spawn(move || {
                    for _ in 0..256 {
                        let waiting = watchers.watch(namespace, entity).wait();
                        watchers.notify(namespace, entity);
                        drop(waiting);
                    }
                });
            }
        });
        assert_eq!(watchers.entry_count(), 0);
    }
}
