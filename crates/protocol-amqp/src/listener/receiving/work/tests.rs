use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{Context, Poll},
};

use domain::{RuleDefinition, SubscriptionName};
use futures_util::FutureExt;

use super::*;
use crate::{
    EntityAdmission, EntityMetadata,
    listener::receiving::budget::{ContentBudget, MAX_RECEIVING_CONTENT_BYTES},
};

#[derive(Clone)]
struct NoBrokerCalls;

impl Broker for NoBrokerCalls {
    async fn bind(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<EntityAdmission>, BrokerRejection> {
        panic!("unpolled work must not call the broker")
    }

    async fn submit_fenced(
        &self,
        _: domain::EntityBinding,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        panic!("pending native outcomes must not call the broker")
    }

    async fn rules_fenced(
        &self,
        _: domain::EntityBinding,
        _: EntityPath,
        _: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        panic!("receiving work does not read rules")
    }

    async fn rules(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        panic!("receiving work does not read rules")
    }

    async fn entity_metadata(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        panic!("receiving work does not rebind")
    }

    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        panic!("pending native outcomes must not call the broker")
    }

    fn deliverable(&self, _: &NamespaceName, _: &EntityPath) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

struct NativeWaiter {
    management: Arc<ConnectionManagement>,
    budget: ContentBudget,
    dropped: Arc<AtomicBool>,
}

impl Future for NativeWaiter {
    type Output = Result<PendingSettlement, EngineError>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for NativeWaiter {
    fn drop(&mut self) {
        assert!(
            self.management
                .has_delivery_registration("receiver", LockToken::new(7))
        );
        assert!(self.budget.is_full());
        self.dropped.store(true, Ordering::Release);
    }
}

fn packet(
    retired: watch::Receiver<bool>,
) -> (
    Work<NoBrokerCalls>,
    Arc<ConnectionManagement>,
    ContentBudget,
    Arc<AtomicBool>,
) {
    let namespace = NamespaceName::new("tenant").expect("namespace");
    let entity = EntityPath::new("Orders").expect("entity");
    let binding = crate::broker::test_admission(
        namespace.clone(),
        Attachment::Queue(entity.clone()),
        EntityMetadata::Queue(domain::QueueConfig::default()),
    )
    .binding;
    let management = ConnectionManagement::new();
    let registration = management
        .register_delivery_owned(
            "receiver",
            entity.clone(),
            domain::SequenceNumber::new(2),
            LockToken::new(7),
            binding.clone(),
        )
        .expect("exact registration");
    let budget = ContentBudget::default();
    let content = budget
        .try_acquire(MAX_RECEIVING_CONTENT_BYTES)
        .expect("exact cap");
    let dropped = Arc::new(AtomicBool::new(false));
    let operation = Work {
        sending: Box::pin(NativeWaiter {
            management: Arc::clone(&management),
            budget: budget.clone(),
            dropped: Arc::clone(&dropped),
        }),
        _registration: Some(registration),
        namespace,
        entity,
        broker: BoundBroker::new(NoBrokerCalls, binding),
        sequence: domain::SequenceNumber::new(2),
        lock: Some(domain::DeliveryLock {
            token: LockToken::new(7),
            locked_until: domain::Timestamp::from_millis(10_000),
        }),
        session: None,
        authorization: None,
        retired,
        _content: content,
    };
    (operation, management, budget, dropped)
}

fn operation() -> (
    WorkFuture,
    Arc<ConnectionManagement>,
    ContentBudget,
    Arc<AtomicBool>,
) {
    let (operation, management, budget, dropped) = packet(watch::channel(false).1);
    (
        Box::pin(async move { operation.run().await }),
        management,
        budget,
        dropped,
    )
}

#[test]
fn unpolled_work_destroys_native_waiter_before_registration_and_content_refund() {
    let (future, management, budget, dropped) = operation();
    drop(future);
    assert!(dropped.load(Ordering::Acquire));
    assert!(!management.has_delivery_registration("receiver", LockToken::new(7)));
    assert!(!budget.is_full());
    assert!(budget.try_acquire(MAX_RECEIVING_CONTENT_BYTES).is_some());
}

#[test]
fn polled_pending_work_preserves_the_same_packet_drop_order() {
    let (mut future, management, budget, dropped) = operation();
    assert!(future.as_mut().now_or_never().is_none());
    assert!(!dropped.load(Ordering::Acquire));
    assert!(management.has_delivery_registration("receiver", LockToken::new(7)));
    assert!(budget.is_full());
    drop(future);
    assert!(dropped.load(Ordering::Acquire));
    assert!(!management.has_delivery_registration("receiver", LockToken::new(7)));
    assert!(budget.try_acquire(MAX_RECEIVING_CONTENT_BYTES).is_some());
}

struct AckWaiter {
    polls: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
}

impl Future for AckWaiter {
    type Output = Result<(), EngineError>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::AcqRel);
        Poll::Pending
    }
}

impl Drop for AckWaiter {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

fn ack_waiter() -> (AckWaiter, Arc<AtomicUsize>, Arc<AtomicBool>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    (
        AckWaiter {
            polls: Arc::clone(&polls),
            dropped: Arc::clone(&dropped),
        },
        polls,
        dropped,
    )
}

#[tokio::test]
async fn retired_transport_ack_is_dropped_without_a_poll_or_early_refund() {
    let (_retired, receiver) = watch::channel(true);
    let (mut work, management, budget, native_dropped) = packet(receiver);
    let (ack, polls, ack_dropped) = ack_waiter();
    work.transport_ack(ack)
        .await
        .expect("retired transport needs no wire ACK");
    assert_eq!(polls.load(Ordering::Acquire), 0);
    assert!(ack_dropped.load(Ordering::Acquire));
    assert!(!native_dropped.load(Ordering::Acquire));
    assert!(management.has_delivery_registration("receiver", LockToken::new(7)));
    assert!(budget.is_full());
    drop(work);
    assert!(native_dropped.load(Ordering::Acquire));
    assert!(!management.has_delivery_registration("receiver", LockToken::new(7)));
    assert!(budget.try_acquire(MAX_RECEIVING_CONTENT_BYTES).is_some());
}

#[tokio::test]
async fn retirement_cancels_an_already_pending_transport_ack_only() {
    let (retired, receiver) = watch::channel(false);
    let (mut work, management, budget, native_dropped) = packet(receiver);
    let (ack, polls, ack_dropped) = ack_waiter();
    let mut acknowledgement = Box::pin(work.transport_ack(ack));
    assert!(acknowledgement.as_mut().now_or_never().is_none());
    assert_eq!(polls.load(Ordering::Acquire), 1);
    assert!(!ack_dropped.load(Ordering::Acquire));
    retired
        .send(true)
        .expect("the work still watches retirement");
    assert!(matches!(
        acknowledgement.as_mut().now_or_never(),
        Some(Ok(()))
    ));
    drop(acknowledgement);
    assert_eq!(polls.load(Ordering::Acquire), 1);
    assert!(ack_dropped.load(Ordering::Acquire));
    assert!(!native_dropped.load(Ordering::Acquire));
    assert!(management.has_delivery_registration("receiver", LockToken::new(7)));
    assert!(budget.is_full());
    drop(work);
    assert!(!management.has_delivery_registration("receiver", LockToken::new(7)));
}

#[tokio::test]
async fn retirement_signal_owner_loss_skips_transport_ack_fail_closed() {
    let (retired, receiver) = watch::channel(false);
    let (mut work, management, budget, _) = packet(receiver);
    drop(retired);
    let (ack, polls, ack_dropped) = ack_waiter();
    work.transport_ack(ack)
        .await
        .expect("lost worker signal cancels transport wait");
    assert_eq!(polls.load(Ordering::Acquire), 0);
    assert!(ack_dropped.load(Ordering::Acquire));
    assert!(management.has_delivery_registration("receiver", LockToken::new(7)));
    assert!(budget.is_full());
    drop(work);
    assert!(!management.has_delivery_registration("receiver", LockToken::new(7)));
}
