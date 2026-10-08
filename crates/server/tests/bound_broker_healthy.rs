//! Public bound-actor controls on both stores, not protocol endpoint migration.

use std::{error::Error, time::Duration};

use domain::{
    CommandKind, CommandOutcome, DeadLetterInfo, DeadLetterReason, Delivery, EntityBinding,
    EntityBindingKind, EntityPath, NamespaceName, QueueConfig, ReceiveMode, SequenceNumber,
    StateMachine, SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig,
};
use server::{Broker, BrokerHandle, LocalProposer, ManualClock, SubmitError};
use testkit::StoreProvider;

const WAIT: Duration = Duration::from_secs(5);
const QUEUE: usize = 0;
const TOPIC: usize = 1;
const SUBSCRIPTION: usize = 2;
const QUEUE_DLQ: usize = 3;
const SUBSCRIPTION_DLQ: usize = 4;

#[derive(Clone)]
struct Names {
    namespace: NamespaceName,
    queue: EntityPath,
    topic: EntityPath,
    subscription: EntityPath,
    queue_dlq: EntityPath,
    subscription_dlq: EntityPath,
}

impl Names {
    fn new() -> Result<Self, Box<dyn Error>> {
        let queue = EntityPath::new("orders")?;
        let topic = EntityPath::new("events")?;
        let subscription = topic.subscription(&SubscriptionName::new("alpha")?)?;
        Ok(Self {
            namespace: NamespaceName::new("tenant")?,
            queue_dlq: queue.dead_letter_queue()?,
            subscription_dlq: subscription.dead_letter_queue()?,
            queue,
            topic,
            subscription,
        })
    }

    fn targets(&self) -> Vec<(&EntityPath, &EntityPath, EntityBindingKind)> {
        vec![
            (&self.queue, &self.queue, EntityBindingKind::Queue),
            (&self.topic, &self.topic, EntityBindingKind::Topic),
            (
                &self.subscription,
                &self.subscription,
                EntityBindingKind::Subscription,
            ),
            (&self.queue_dlq, &self.queue, EntityBindingKind::Queue),
            (
                &self.subscription_dlq,
                &self.subscription,
                EntityBindingKind::Subscription,
            ),
        ]
    }
}

struct Actor<P: StoreProvider> {
    broker: Broker,
    handle: BrokerHandle,
    names: Names,
    provider: P,
}

impl<P: StoreProvider> Actor<P> {
    fn new(provider: P) -> Result<Self, Box<dyn Error>> {
        let names = Names::new()?;
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(provider.open()?),
            ManualClock::at(1_000),
        ));
        let actor = Self {
            handle: broker.handle(),
            broker,
            names,
            provider,
        };
        assert_eq!(
            actor.handle.submit_blocking(
                actor.names.namespace.clone(),
                actor.names.queue.clone(),
                CommandKind::CreateQueue {
                    config: QueueConfig::default()
                },
            )?,
            CommandOutcome::QueueCreated
        );
        assert_eq!(
            actor.handle.submit_blocking(
                actor.names.namespace.clone(),
                actor.names.topic.clone(),
                CommandKind::CreateTopic {
                    config: TopicConfig::default()
                },
            )?,
            CommandOutcome::TopicCreated
        );
        assert_eq!(
            actor.handle.submit_blocking(
                actor.names.namespace.clone(),
                actor.names.topic.clone(),
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new("alpha")?,
                    config: SubscriptionConfig::default(),
                },
            )?,
            CommandOutcome::SubscriptionCreated {
                entity: actor.names.subscription.clone()
            }
        );
        Ok(actor)
    }

    fn restart(self) -> Result<Self, Box<dyn Error>> {
        let Self {
            broker,
            handle,
            names,
            provider,
        } = self;
        drop(handle);
        drop(broker);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(provider.open()?),
            ManualClock::at(1_000),
        ));
        Ok(Self {
            handle: broker.handle(),
            broker,
            names,
            provider,
        })
    }

    async fn bindings(&self) -> Result<Vec<EntityBinding>, Box<dyn Error>> {
        let mut bindings = Vec::new();
        for (target, owner, kind) in self.names.targets() {
            let blocking = self
                .handle
                .bind_entity_blocking(self.names.namespace.clone(), target.clone())?;
            let asynchronous = tokio::time::timeout(
                WAIT,
                self.handle
                    .bind_entity(self.names.namespace.clone(), target.clone()),
            )
            .await??;
            assert_eq!(asynchronous, blocking);
            assert_eq!(blocking.namespace(), &self.names.namespace);
            assert_eq!(blocking.target(), target);
            assert_eq!(blocking.owner(), owner);
            assert_eq!(blocking.kind(), kind);
            assert_eq!(blocking.generation(), 1);
            bindings.push(blocking);
        }
        Ok(bindings)
    }
}

fn send(message_id: &str) -> CommandKind {
    CommandKind::Send {
        message_id: message_id.to_owned(),
        body: message_id.as_bytes().to_vec(),
        time_to_live_millis: None,
        session_id: None,
        scheduled_enqueue_at: None,
        envelope: None,
    }
}

fn receive(mode: ReceiveMode) -> CommandKind {
    CommandKind::Receive {
        mode,
        lock_duration_millis: None,
        session: None,
    }
}

fn delivery(outcome: CommandOutcome, message_id: &str) -> Delivery {
    let CommandOutcome::Received(Some(delivery)) = outcome else {
        panic!("expected one bound delivery, got {outcome:?}");
    };
    assert_eq!(delivery.message_id, message_id);
    assert_eq!(delivery.body, message_id.as_bytes().to_vec());
    assert_eq!(delivery.enqueued_at, Timestamp::from_millis(1_000));
    delivery
}

fn dead_letter(delivery: &Delivery) -> CommandKind {
    CommandKind::DeadLetter {
        sequence: delivery.sequence,
        lock_token: delivery.lock.expect("peek-lock carries a token").token,
        reason: String::from("BoundActorControl"),
        description: String::from("application settlement through retained authority"),
        replacement_envelope: None,
    }
}

fn complete(delivery: &Delivery) -> CommandKind {
    CommandKind::Complete {
        sequence: delivery.sequence,
        lock_token: delivery.lock.expect("peek-lock carries a token").token,
    }
}

fn assert_dead_letter(delivery: &Delivery, sequence: SequenceNumber) {
    assert_eq!(delivery.sequence, sequence);
    assert_eq!(
        delivery.dead_letter,
        Some(DeadLetterInfo {
            reason: DeadLetterReason::Application(String::from("BoundActorControl")),
            description: String::from("application settlement through retained authority"),
            dead_lettered_at: Timestamp::from_millis(1_000),
        })
    );
    assert_eq!(delivery.expires_at, None);
    assert_eq!(delivery.session_id, None);
}

fn current_thread() -> Result<tokio::runtime::Runtime, std::io::Error> {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
}

fn healthy_blocking_and_async_authority_routes_primary_and_dlq_targets<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let actor = Actor::new(provider)?;
    current_thread()?.block_on(async {
        let bindings = actor.bindings().await?;
        let queue_wait = protocol_amqp::Broker::deliverable(
            &actor.handle,
            &actor.names.namespace,
            &actor.names.queue,
        );
        let CommandOutcome::Sent { sequence } = actor
            .handle
            .submit_bound_blocking(bindings[QUEUE].clone(), send("queue-first"))?
        else {
            panic!("expected a queue send");
        };
        tokio::time::timeout(WAIT, queue_wait).await?;
        let queued = delivery(
            tokio::time::timeout(
                WAIT,
                actor
                    .handle
                    .submit_bound(bindings[QUEUE].clone(), receive(ReceiveMode::PeekLock)),
            )
            .await??,
            "queue-first",
        );
        assert_eq!(queued.sequence, sequence);
        assert_eq!(
            tokio::time::timeout(
                WAIT,
                actor
                    .handle
                    .submit_bound(bindings[QUEUE].clone(), dead_letter(&queued),)
            )
            .await??,
            CommandOutcome::DeadLettered
        );
        let drained = delivery(
            actor.handle.submit_bound_blocking(
                bindings[QUEUE_DLQ].clone(),
                receive(ReceiveMode::PeekLock),
            )?,
            "queue-first",
        );
        assert_dead_letter(&drained, sequence);
        assert_eq!(
            tokio::time::timeout(
                WAIT,
                actor
                    .handle
                    .submit_bound(bindings[QUEUE_DLQ].clone(), complete(&drained),)
            )
            .await??,
            CommandOutcome::Completed
        );

        let queue_wait = protocol_amqp::Broker::deliverable(
            &actor.handle,
            &actor.names.namespace,
            &actor.names.queue,
        );
        let CommandOutcome::Sent { sequence } = tokio::time::timeout(
            WAIT,
            actor
                .handle
                .submit_bound(bindings[QUEUE].clone(), send("queue-second")),
        )
        .await??
        else {
            panic!("expected an async queue send");
        };
        tokio::time::timeout(WAIT, queue_wait).await?;
        let queued = delivery(
            actor.handle.submit_bound_blocking(
                bindings[QUEUE].clone(),
                receive(ReceiveMode::ReceiveAndDelete),
            )?,
            "queue-second",
        );
        assert_eq!(queued.sequence, sequence);
        assert_eq!(queued.lock, None);
        assert_eq!(
            tokio::time::timeout(
                WAIT,
                actor.handle.submit_bound(
                    bindings[QUEUE_DLQ].clone(),
                    receive(ReceiveMode::ReceiveAndDelete),
                )
            )
            .await??,
            CommandOutcome::Received(None)
        );

        let subscription_wait = protocol_amqp::Broker::deliverable(
            &actor.handle,
            &actor.names.namespace,
            &actor.names.subscription,
        );
        let CommandOutcome::Published {
            sequences,
            subscriptions,
        } = tokio::time::timeout(
            WAIT,
            actor
                .handle
                .submit_bound(bindings[TOPIC].clone(), send("topic-first")),
        )
        .await??
        else {
            panic!("expected a topic publication");
        };
        assert_eq!(sequences.len(), 1);
        assert_eq!(subscriptions, vec![actor.names.subscription.clone()]);
        tokio::time::timeout(WAIT, subscription_wait).await?;
        let copied = delivery(
            actor.handle.submit_bound_blocking(
                bindings[SUBSCRIPTION].clone(),
                receive(ReceiveMode::PeekLock),
            )?,
            "topic-first",
        );
        assert_eq!(copied.sequence, sequences[0]);
        assert_eq!(
            tokio::time::timeout(
                WAIT,
                actor
                    .handle
                    .submit_bound(bindings[SUBSCRIPTION].clone(), dead_letter(&copied),)
            )
            .await??,
            CommandOutcome::DeadLettered
        );
        let drained = delivery(
            tokio::time::timeout(
                WAIT,
                actor.handle.submit_bound(
                    bindings[SUBSCRIPTION_DLQ].clone(),
                    receive(ReceiveMode::PeekLock),
                ),
            )
            .await??,
            "topic-first",
        );
        assert_dead_letter(&drained, sequences[0]);
        assert_eq!(
            actor
                .handle
                .submit_bound_blocking(bindings[SUBSCRIPTION_DLQ].clone(), complete(&drained),)?,
            CommandOutcome::Completed
        );

        let subscription_wait = protocol_amqp::Broker::deliverable(
            &actor.handle,
            &actor.names.namespace,
            &actor.names.subscription,
        );
        let CommandOutcome::Published {
            sequences,
            subscriptions,
        } = actor
            .handle
            .submit_bound_blocking(bindings[TOPIC].clone(), send("topic-second"))?
        else {
            panic!("expected a blocking topic publication");
        };
        assert_eq!(sequences.len(), 1);
        assert_eq!(subscriptions, vec![actor.names.subscription.clone()]);
        tokio::time::timeout(WAIT, subscription_wait).await?;
        let copied = delivery(
            tokio::time::timeout(
                WAIT,
                actor.handle.submit_bound(
                    bindings[SUBSCRIPTION].clone(),
                    receive(ReceiveMode::ReceiveAndDelete),
                ),
            )
            .await??,
            "topic-second",
        );
        assert_eq!(copied.sequence, sequences[0]);
        assert_eq!(copied.lock, None);
        for index in [QUEUE, SUBSCRIPTION, QUEUE_DLQ, SUBSCRIPTION_DLQ] {
            assert_eq!(
                actor.handle.submit_bound_blocking(
                    bindings[index].clone(),
                    receive(ReceiveMode::ReceiveAndDelete),
                )?,
                CommandOutcome::Received(None)
            );
        }
        Ok(())
    })
}

fn original_bindings_and_retained_messages_survive_owner_reopen<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let actor = Actor::new(provider)?;
    current_thread()?.block_on(async {
        let bindings = actor.bindings().await?;
        actor
            .handle
            .submit_bound_blocking(bindings[QUEUE].clone(), send("queue-dead"))?;
        actor
            .handle
            .submit_bound_blocking(bindings[TOPIC].clone(), send("topic-dead"))?;
        let mut dead_letter_sequences = Vec::new();
        for (index, id) in [(QUEUE, "queue-dead"), (SUBSCRIPTION, "topic-dead")] {
            let locked = delivery(
                actor.handle.submit_bound_blocking(
                    bindings[index].clone(),
                    receive(ReceiveMode::PeekLock),
                )?,
                id,
            );
            assert_eq!(
                actor
                    .handle
                    .submit_bound_blocking(bindings[index].clone(), dead_letter(&locked),)?,
                CommandOutcome::DeadLettered
            );
            let shadow_index = if index == QUEUE {
                QUEUE_DLQ
            } else {
                SUBSCRIPTION_DLQ
            };
            dead_letter_sequences.push((shadow_index, locked.sequence));
        }
        actor
            .handle
            .submit_bound_blocking(bindings[QUEUE].clone(), send("queue-retained"))?;
        actor
            .handle
            .submit_bound_blocking(bindings[TOPIC].clone(), send("topic-retained"))?;
        let actor = actor.restart()?;
        assert_eq!(
            actor.bindings().await?,
            bindings,
            "reopen captures the same physical targets and generations"
        );
        let CommandOutcome::Published { subscriptions, .. } = tokio::time::timeout(
            WAIT,
            actor
                .handle
                .submit_bound(bindings[TOPIC].clone(), send("topic-after-reopen")),
        )
        .await??
        else {
            panic!("retained topic authority must publish after reopen");
        };
        assert_eq!(subscriptions, vec![actor.names.subscription.clone()]);
        for (index, id) in [
            (QUEUE, "queue-retained"),
            (QUEUE_DLQ, "queue-dead"),
            (SUBSCRIPTION, "topic-retained"),
            (SUBSCRIPTION_DLQ, "topic-dead"),
        ] {
            let drained = if index == QUEUE || index == SUBSCRIPTION_DLQ {
                delivery(
                    actor.handle.submit_bound_blocking(
                        bindings[index].clone(),
                        receive(ReceiveMode::ReceiveAndDelete),
                    )?,
                    id,
                )
            } else {
                delivery(
                    tokio::time::timeout(
                        WAIT,
                        actor.handle.submit_bound(
                            bindings[index].clone(),
                            receive(ReceiveMode::ReceiveAndDelete),
                        ),
                    )
                    .await??,
                    id,
                )
            };
            assert_eq!(drained.lock, None);
            if index == QUEUE_DLQ || index == SUBSCRIPTION_DLQ {
                let sequence = dead_letter_sequences
                    .iter()
                    .find(|(target, _)| *target == index)
                    .expect("the original dead-letter sequence was captured")
                    .1;
                assert_dead_letter(&drained, sequence);
            } else {
                assert_eq!(drained.dead_letter, None);
            }
        }
        let copied = delivery(
            actor.handle.submit_bound_blocking(
                bindings[SUBSCRIPTION].clone(),
                receive(ReceiveMode::ReceiveAndDelete),
            )?,
            "topic-after-reopen",
        );
        assert_eq!(copied.lock, None);
        for index in [QUEUE, SUBSCRIPTION, QUEUE_DLQ, SUBSCRIPTION_DLQ] {
            assert_eq!(
                tokio::time::timeout(
                    WAIT,
                    actor.handle.submit_bound(
                        bindings[index].clone(),
                        receive(ReceiveMode::ReceiveAndDelete),
                    )
                )
                .await??,
                CommandOutcome::Received(None)
            );
        }
        Ok(())
    })
}

fn stopped_owner_refuses_both_bind_and_bound_submit_forms<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let actor = Actor::new(provider)?;
    current_thread()?.block_on(async {
        let bindings = actor.bindings().await?;
        let Actor {
            broker,
            handle,
            names,
            provider: _provider,
        } = actor;
        drop(broker);
        for binding in bindings {
            assert_eq!(
                handle.bind_entity_blocking(names.namespace.clone(), binding.target().clone(),),
                Err(SubmitError::BrokerStopped)
            );
            assert_eq!(
                tokio::time::timeout(
                    WAIT,
                    handle.bind_entity(names.namespace.clone(), binding.target().clone(),)
                )
                .await?,
                Err(SubmitError::BrokerStopped)
            );
            assert_eq!(
                handle.submit_bound_blocking(binding.clone(), CommandKind::ActivateScheduled,),
                Err(SubmitError::BrokerStopped)
            );
            assert_eq!(
                tokio::time::timeout(
                    WAIT,
                    handle.submit_bound(binding, CommandKind::ActivateScheduled,)
                )
                .await?,
                Err(SubmitError::BrokerStopped)
            );
        }
        Ok(())
    })
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory {
            $(#[test]
            fn $case() -> Result<(), Box<dyn std::error::Error>> {
                super::$case(::testkit::MemoryProvider::new())
            })+
        }
        mod durable {
            $(#[test]
            fn $case() -> Result<(), Box<dyn std::error::Error>> {
                super::$case(::testkit::DurableProvider::temporary()?)
            })+
        }
    };
}

for_each_backend! {
    healthy_blocking_and_async_authority_routes_primary_and_dlq_targets,
    original_bindings_and_retained_messages_survive_owner_reopen,
    stopped_owner_refuses_both_bind_and_bound_submit_forms,
}
