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

use domain::{CommandKind, CommandOutcome, EntityPath, NamespaceName, Timestamp};
use storage::StateStore;
use thiserror::Error;
use tokio::sync::Notify;
use tracing::debug;

use crate::{Clock, LocalProposer, ProposeError};

/// Commands that may be waiting ahead of a caller's own.
///
/// Bounded, so a flood of clients cannot grow the queue without limit. A full
/// queue makes senders wait, which is the backpressure the protocol edge turns
/// into flow control.
const COMMAND_QUEUE_DEPTH: usize = 1_024;

enum Request {
    Apply {
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
        reply: flume::Sender<Result<CommandOutcome, ProposeError>>,
    },
    /// Reading which queues exist does not race the way applying does, but it
    /// goes through the owner anyway so that a handle needs no type parameter
    /// and the protocol edge never has to name a backend.
    ListQueues {
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
}

/// Whether applying this outcome may have made something deliverable, which is
/// what a waiting link wants to be woken for.
fn makes_deliverable(outcome: &CommandOutcome) -> bool {
    match outcome {
        CommandOutcome::Sent { .. } => true,
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
                kind,
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
                kind,
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
        let (reply, queues) = flume::bounded(1);
        self.requests
            .send(Request::ListQueues { limit, reply })
            .map_err(|_| SubmitError::BrokerStopped)?;
        queues
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
                        Request::Apply {
                            namespace,
                            entity,
                            kind,
                            reply,
                        } => {
                            let outcome = proposer.propose(&namespace, &entity, kind);
                            if outcome.as_ref().is_ok_and(makes_deliverable) {
                                watching.notify(&namespace, &entity);
                            }
                            // A caller that stopped waiting is not an error: the
                            // command still applied, and it gave up, not us.
                            let _ = reply.send(outcome);
                        }
                        Request::ListQueues { limit, reply } => {
                            let _ = reply
                                .send(proposer.machine().queues(limit).map_err(ProposeError::from));
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
}
