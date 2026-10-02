use std::future::Future;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Wake, Waker};

use super::*;

#[tokio::test]
async fn consumer_drop_synchronously_defeats_prepared_or_ready_authority() {
    for ready_first in [false, true] {
        let mut fixture = Fixture::new().await;
        let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
        let mut session = fixture.session(SEND).await;
        let (mut sender, handle) = fixture
            .sender(&mut session, SEND, HANDLE, "retirement-sender")
            .await;
        let transaction = txn(20);
        let observer = fixture
            .declare(&mut coordinator, CONTROL, &transaction)
            .await;
        let mut sent = fixture.send(&mut sender, SEND, handle).await;
        let receipt = fixture.retirement(&mut sent, &transaction).await;
        let prepared = fixture.provisional(receipt, &transaction, sent.id).await;
        if ready_first {
            let discharged = fixture
                .discharge(&mut coordinator, CONTROL, &transaction, false)
                .await;
            let ready = discharged
                .receipt
                .prepare_work(vec![NativePreparedWork::Retirement(prepared)])
                .expect("ready exact original");
            let (ticket, resources) = ready.into_owner_parts();
            drop(sent.delivery);
            assert_eq!(observer.state(), NativeTransactionState::Faulted);
            assert!(matches!(
                ticket.try_claim(),
                Err(NativeTransactionError::Faulted(NativeFault::Dropped))
            ));
            drop(resources);
        } else {
            drop(sent.delivery);
            assert_eq!(observer.state(), NativeTransactionState::Faulted);
            drop(prepared);
        }
        fixture.peer.barrier(SEND).await;
        assert_eq!(observer.state(), NativeTransactionState::Faulted);
        fixture.shutdown().await;
    }
}

struct DropConsumerWake {
    consumer: Mutex<Option<SentDelivery>>,
    awakened: AtomicBool,
}

impl Wake for DropConsumerWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let consumer = self.consumer.lock().expect("consumer slot").take();
        drop(consumer);
        self.awakened.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publication_can_reentrantly_drop_consumer_without_locked_callback_or_empty_commit() {
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    let transaction = txn(21);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    let sent = fixture.send(&mut sender, SEND, handle).await;
    let wake = Arc::new(DropConsumerWake {
        consumer: Mutex::new(Some(sent.delivery)),
        awakened: AtomicBool::new(false),
    });
    let waker = Waker::from(Arc::clone(&wake));
    {
        let mut slot = wake.consumer.lock().expect("consumer slot");
        let waiting = slot.as_mut().expect("live consumer").next_disposition();
        tokio::pin!(waiting);
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
    }
    fixture.peer.retirement(SEND, sent.id, &transaction).await;
    fixture.peer.barrier(SEND).await;
    assert!(wake.awakened.load(Ordering::SeqCst));
    assert!(wake.consumer.lock().expect("consumer slot").is_none());
    assert_eq!(observer.state(), NativeTransactionState::Faulted);
    let id = fixture
        .command(
            CONTROL,
            TransactionCommand::Discharge(Discharge {
                txn_id: transaction,
                fail: Some(false),
            }),
        )
        .await;
    let refusal = fixture.peer.disposition(CONTROL, Role::Receiver, id).await;
    let Some(DeliveryState::Rejected(rejected)) = refusal.state else {
        panic!("dropped consumer cannot become empty commit");
    };
    assert_eq!(
        rejected
            .error
            .expect("known native rollback")
            .condition
            .as_symbol()
            .as_str(),
        "amqp:transaction:rollback"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn same_transaction_bytes_from_foreign_controller_cannot_complete_local_manifest() {
    let mut local = Fixture::new().await;
    let mut foreign = Fixture::new().await;
    let (_local_control, mut local_controller) = local.coordinator(CONTROL).await;
    let (_foreign_control, mut foreign_controller) = foreign.coordinator(CONTROL).await;
    let mut local_session = local.session(SEND).await;
    let mut foreign_session = foreign.session(SEND).await;
    let (mut local_sender, local_handle) = local
        .sender(&mut local_session, SEND, HANDLE, "retirement-sender")
        .await;
    let (mut foreign_sender, foreign_handle) = foreign
        .sender(&mut foreign_session, SEND, HANDLE, "retirement-sender")
        .await;
    let transaction = txn(22);
    let local_observer = local
        .declare(&mut local_controller, CONTROL, &transaction)
        .await;
    let foreign_observer = foreign
        .declare(&mut foreign_controller, CONTROL, &transaction)
        .await;
    let mut local_sent = local.send(&mut local_sender, SEND, local_handle).await;
    let mut foreign_sent = foreign
        .send(&mut foreign_sender, SEND, foreign_handle)
        .await;
    assert_eq!(local_sent.id, foreign_sent.id);
    let local_receipt = local.retirement(&mut local_sent, &transaction).await;
    let foreign_receipt = foreign.retirement(&mut foreign_sent, &transaction).await;
    assert_eq!(
        local_receipt.transaction_id(),
        foreign_receipt.transaction_id()
    );
    assert!(
        !local_receipt
            .controller_identity()
            .same_controller(foreign_receipt.controller_identity())
    );
    let local_prepared = local
        .provisional(local_receipt, &transaction, local_sent.id)
        .await;
    let foreign_prepared = foreign
        .provisional(foreign_receipt, &transaction, foreign_sent.id)
        .await;
    let discharged = local
        .discharge(&mut local_controller, CONTROL, &transaction, false)
        .await;
    assert!(matches!(
        discharged
            .receipt
            .prepare_work(vec![NativePreparedWork::Retirement(foreign_prepared)]),
        Err(NativeTransactionError::InvalidPreparedSet)
    ));
    assert_ne!(local_observer.state(), NativeTransactionState::OwnerStarted);
    assert_ne!(
        foreign_observer.state(),
        NativeTransactionState::OwnerStarted
    );
    drop(local_prepared);
    local.peer.barrier(SEND).await;
    foreign.peer.barrier(SEND).await;
    local.shutdown().await;
    foreign.shutdown().await;
}

#[tokio::test]
async fn terminal_history_eviction_does_not_lose_original_rearm_or_late_attempt_identity() {
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    let transaction = txn(23);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    let mut sent = fixture.send(&mut sender, SEND, handle).await;
    let original = sent.delivery.delivery_identity().clone();
    let first = fixture.retirement(&mut sent, &transaction).await;
    let old_prepared = fixture.provisional(first, &transaction, sent.id).await;
    fixture.peer.detach(CONTROL, HANDLE, 0).await;
    assert_eq!(observer.state(), NativeTransactionState::Faulted);
    fixture.peer.barrier(SEND).await;
    let (_other_session, mut other_controller) = fixture.coordinator(HEALTHY).await;
    // More than the bounded 32 terminal decisions removes the original ID history.
    for byte in 40..=72 {
        let temporary = txn(byte);
        let temporary_observer = fixture
            .declare(&mut other_controller, HEALTHY, &temporary)
            .await;
        let discharged = fixture
            .discharge(&mut other_controller, HEALTHY, &temporary, false)
            .await;
        let ready = discharged
            .receipt
            .prepare_work(Vec::new())
            .expect("truly empty native transaction");
        let (ticket, resources) = ready.into_owner_parts();
        ticket
            .try_claim()
            .expect("empty native claim")
            .finish(NativeTransactionDecision::Committed);
        super::lifecycle::finish_committed(&mut fixture, resources, (HEALTHY, discharged.id), &[])
            .await;
        assert_eq!(
            temporary_observer.state(),
            NativeTransactionState::Committed
        );
    }
    let replacement_observer = fixture
        .declare(&mut other_controller, HEALTHY, &transaction)
        .await;
    assert!(!observer.same_transaction(&replacement_observer));
    let receipt = fixture.retirement(&mut sent, &transaction).await;
    assert!(receipt.delivery_identity().same_delivery(&original));
    let current_prepared = fixture.provisional(receipt, &transaction, sent.id).await;
    drop(old_prepared);
    fixture.peer.barrier(SEND).await;
    assert_eq!(
        replacement_observer.state(),
        NativeTransactionState::Pending
    );
    let discharged = fixture
        .discharge(&mut other_controller, HEALTHY, &transaction, false)
        .await;
    let ready = discharged
        .receipt
        .prepare_work(vec![NativePreparedWork::Retirement(current_prepared)])
        .expect("new exact attempt after terminal eviction");
    let (ticket, resources) = ready.into_owner_parts();
    ticket
        .try_claim()
        .expect("new attempt native claim")
        .finish(NativeTransactionDecision::Committed);
    let expected = [(fixture.peer.local(SEND), Role::Sender, sent.id)];
    super::lifecycle::finish_committed(
        &mut fixture,
        resources,
        (HEALTHY, discharged.id),
        &expected,
    )
    .await;
    fixture.shutdown().await;
}
