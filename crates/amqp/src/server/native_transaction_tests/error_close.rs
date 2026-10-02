use std::{
    future::{Future, poll_fn},
    task::Poll,
};

use super::*;

const CONDITION: &str = "amqp:internal-error";
const DESCRIPTION: &str = "native posting stage failed";
const SIBLING_HANDLE: u32 = 33;

fn error() -> Error {
    Error::new(crate::AmqpError::InternalError, DESCRIPTION, None)
}

fn id(byte: u8) -> TransactionId {
    TransactionId::new([byte]).expect("bounded transaction ID")
}

async fn error_detach(fixture: &mut Fixture, expected_channel: u16, handle: u32) {
    for _ in 0..8 {
        match fixture.peer.frame().await {
            Frame::Amqp {
                channel,
                performative: Some(Performative::Detach(detach)),
                payload,
            } => {
                assert_eq!(channel, expected_channel);
                assert_eq!(detach.handle, handle);
                assert!(detach.closed);
                assert_eq!(detach.error, Some(error()));
                assert!(payload.is_empty());
                return;
            }
            Frame::Amqp {
                performative: Some(Performative::Flow(_)),
                payload,
                ..
            } if payload.is_empty() => {}
            frame => panic!("only exact error Detach, never a transaction outcome: {frame:?}"),
        }
    }
    panic!("no error Detach within bounded response count");
}

async fn receiver_on(
    fixture: &mut Fixture,
    session: &mut ServerSession,
    handle: u32,
) -> TransactionalReceiver {
    fixture
        .peer
        .attach(POST_CHANNEL, ordinary_attach(handle))
        .await;
    let incoming = bounded(
        "replacement receiver approval",
        session.next_incoming_attach(),
    )
    .await
    .expect("actor-approved receiver");
    let responses = async {
        let attach = fixture.peer.attached(POST_CHANNEL).await;
        fixture.peer.credit(POST_CHANNEL, attach.handle).await;
    };
    let (receiver, ()) = tokio::join!(
        session.accept_transactional_receiver(incoming, 0),
        responses
    );
    receiver.expect("accepted receiver")
}

async fn coordinator_on(
    fixture: &mut Fixture,
    session: &mut ServerSession,
    handle: u32,
) -> CoordinatorEndpoint {
    fixture
        .peer
        .attach(CONTROL_CHANNEL, coordinator_attach(handle))
        .await;
    let incoming = bounded(
        "replacement controller approval",
        session.next_incoming_attach(),
    )
    .await
    .expect("actor-approved coordinator");
    let responses = async {
        let attach = fixture.peer.attached(CONTROL_CHANNEL).await;
        fixture.peer.credit(CONTROL_CHANNEL, attach.handle).await;
    };
    let (coordinator, ()) = tokio::join!(session.accept_coordinator(incoming, 0), responses);
    coordinator.expect("accepted coordinator")
}

async fn declared_on(
    fixture: &mut Fixture,
    coordinator: &mut CoordinatorEndpoint,
    handle: u32,
    transaction: &TransactionId,
) -> NativeTransactionIdentity {
    fixture
        .peer
        .command(
            CONTROL_CHANNEL,
            handle,
            0,
            TransactionCommand::Declare(Declare::default()),
        )
        .await;
    let CoordinatorRequest::Declare(receipt) = bounded("sibling Declare", coordinator.recv())
        .await
        .expect("Declare receipt")
    else {
        panic!("Declare request")
    };
    let (observer, disposition) = tokio::join!(
        receipt.declared(transaction.clone()),
        fixture.peer.disposition(CONTROL_CHANNEL, 0)
    );
    assert!(disposition.settled);
    assert!(matches!(disposition.state,
        Some(DeliveryState::Declared(crate::Declared { txn_id })) if txn_id == *transaction));
    observer.expect("registered sibling transaction")
}

async fn held(
    fixture: &mut Fixture,
    receiver: &mut TransactionalReceiver,
    transaction: &TransactionId,
) -> TransactionPostingReceipt {
    fixture
        .post(
            transaction,
            &Message::data(vec![transaction.as_bytes()[0]; 127]),
        )
        .await;
    let TransactionalIngress::Posting(posting) = bounded("held posting", receiver.recv())
        .await
        .expect("posting receipt")
    else {
        panic!("posting receipt")
    };
    posting
}

async fn ordinary_on(fixture: &mut Fixture, receiver: &mut TransactionalReceiver, handle: u32) {
    let message = Message::data(b"healthy sibling".to_vec());
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            first(handle, 1, None, false),
            encode_message(&message).expect("ordinary encoding"),
        )
        .await;
    let TransactionalIngress::Ordinary(receipt) =
        bounded("sibling ordinary receipt", receiver.recv())
            .await
            .expect("ordinary receipt")
    else {
        panic!("ordinary receipt")
    };
    assert_eq!(receipt.message(), &message);
    let (settled, disposition) = tokio::join!(
        receiver.accept_retained(&receipt),
        fixture.peer.disposition(POST_CHANNEL, 1)
    );
    settled.expect("sibling ordinary settlement");
    assert!(disposition.settled);
    assert!(matches!(
        disposition.state,
        Some(DeliveryState::Accepted(_))
    ));
}

async fn close_endpoint(
    coordinator: &CoordinatorEndpoint,
    receiver: &TransactionalReceiver,
    control: bool,
) -> Result<(), EngineError> {
    if control {
        coordinator.close_with_error(error()).await
    } else {
        receiver.close_with_error(error()).await
    }
}

fn route(control: bool) -> (u16, u32) {
    if control {
        (CONTROL_CHANNEL, CONTROL_HANDLE)
    } else {
        (POST_CHANNEL, POST_HANDLE)
    }
}

#[tokio::test]
async fn receiver_error_close_faults_pending_post_without_accepting_or_closing_siblings() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let (mut data, mut receiver) = fixture.receiver().await;
    let _healthy = fixture.session(HEALTHY_CHANNEL).await;
    let mut sibling = receiver_on(&mut fixture, &mut data, SIBLING_HANDLE).await;
    let transaction = id(190);
    let observer = fixture.declare(&mut coordinator, &transaction).await;
    let posting = held(&mut fixture, &mut receiver, &transaction).await;
    let sibling_group = fixture.declare(&mut coordinator, &id(191)).await;
    let proof = receiver.receiver_identity();
    let (closed, ()) = tokio::join!(
        receiver.close_with_error(error()),
        error_detach(&mut fixture, POST_CHANNEL, POST_HANDLE)
    );
    closed.expect("receiver error Detach flushed");
    bounded("receiver detached", receiver.on_detach()).await;
    assert!(!proof.is_active());
    assert_eq!(observer.state(), NativeTransactionState::Faulted);
    assert_eq!(sibling_group.state(), NativeTransactionState::Pending);
    assert!(coordinator.controller_identity().is_active());
    assert!(sibling.receiver_identity().is_active());
    assert_eq!(posting.message(), &Message::data(vec![190; 127]));
    ordinary_on(&mut fixture, &mut sibling, SIBLING_HANDLE).await;
    fixture.peer.barrier(HEALTHY_CHANNEL).await;
    assert!(fixture.connection.connection_identity().is_active());
    drop(posting);
    bounded("receiver scoped shutdown", fixture.connection.shutdown()).await;
}

#[tokio::test]
async fn coordinator_error_close_faults_its_pending_groups_but_not_another_controller() {
    let mut fixture = Fixture::new(true).await;
    let (mut control, mut coordinator) = fixture.coordinator().await;
    let (_data, mut receiver) = fixture.receiver().await;
    let _healthy = fixture.session(HEALTHY_CHANNEL).await;
    let mut sibling = coordinator_on(&mut fixture, &mut control, SIBLING_HANDLE).await;
    let transaction = id(192);
    let observer = fixture.declare(&mut coordinator, &transaction).await;
    let second = fixture.declare(&mut coordinator, &id(193)).await;
    let unrelated = declared_on(&mut fixture, &mut sibling, SIBLING_HANDLE, &id(194)).await;
    let posting = held(&mut fixture, &mut receiver, &transaction).await;
    let (closed, ()) = tokio::join!(
        coordinator.close_with_error(error()),
        error_detach(&mut fixture, CONTROL_CHANNEL, CONTROL_HANDLE)
    );
    closed.expect("controller error Detach flushed");
    assert!(!coordinator.controller_identity().is_active());
    assert_eq!(observer.state(), NativeTransactionState::Faulted);
    assert_eq!(second.state(), NativeTransactionState::Faulted);
    assert_eq!(unrelated.state(), NativeTransactionState::Pending);
    assert!(sibling.controller_identity().is_active());
    assert!(receiver.receiver_identity().is_active());
    declared_on(&mut fixture, &mut sibling, SIBLING_HANDLE, &id(195)).await;
    assert_eq!(posting.message(), &Message::data(vec![192; 127]));
    fixture.peer.barrier(HEALTHY_CHANNEL).await;
    drop(posting);
    bounded("controller scoped shutdown", fixture.connection.shutdown()).await;
}

#[tokio::test]
async fn stale_endpoint_error_close_cannot_detach_a_reused_exact_route() {
    for control in [false, true] {
        let mut fixture = Fixture::new(true).await;
        let (mut controls, mut coordinator) = fixture.coordinator().await;
        let (mut data, mut receiver) = fixture.receiver().await;
        let _healthy = fixture.session(HEALTHY_CHANNEL).await;
        let transaction = id(196);
        let observer = fixture.declare(&mut coordinator, &transaction).await;
        let posting = held(&mut fixture, &mut receiver, &transaction).await;
        let (channel, handle) = route(control);
        fixture.peer.detach(channel, handle).await;
        assert_eq!(observer.state(), NativeTransactionState::Faulted);
        if control {
            let mut replacement = coordinator_on(&mut fixture, &mut controls, CONTROL_HANDLE).await;
            bounded(
                "stale controller close",
                coordinator.close_with_error(error()),
            )
            .await
            .expect("retired original close is a no-op");
            fixture.peer.barrier(HEALTHY_CHANNEL).await;
            assert!(replacement.controller_identity().is_active());
            declared_on(&mut fixture, &mut replacement, CONTROL_HANDLE, &id(197)).await;
        } else {
            let mut replacement = receiver_on(&mut fixture, &mut data, POST_HANDLE).await;
            bounded("stale receiver close", receiver.close_with_error(error()))
                .await
                .expect("retired original close is a no-op");
            fixture.peer.barrier(HEALTHY_CHANNEL).await;
            assert!(replacement.receiver_identity().is_active());
            ordinary_on(&mut fixture, &mut replacement, POST_HANDLE).await;
        }
        assert_eq!(posting.message(), &Message::data(vec![196; 127]));
        drop(posting);
        bounded("reused route shutdown", fixture.connection.shutdown()).await;
    }
}

#[tokio::test]
async fn error_close_retires_ready_before_flush_but_cannot_revoke_started_owner() {
    for control in [false, true] {
        for started in [false, true] {
            let mut fixture = Fixture::new(true).await;
            let (_controls, mut coordinator) = fixture.coordinator().await;
            let (_data, mut receiver) = fixture.receiver().await;
            let _healthy = fixture.session(HEALTHY_CHANNEL).await;
            let transaction = id(198);
            let observer = fixture.declare(&mut coordinator, &transaction).await;
            let posting = held(&mut fixture, &mut receiver, &transaction).await;
            let prepared = fixture.provisional(posting, &transaction).await;
            let sealed = fixture
                .discharge(&mut coordinator, &transaction, false)
                .await;
            let ready = sealed.prepare(vec![prepared]).expect("exact Ready bundle");
            let (ticket, resources) = ready.into_owner_parts();
            let (ticket, claim) = if started {
                (
                    None,
                    Some(ticket.try_claim().expect("owner claims before error close")),
                )
            } else {
                (Some(ticket), None)
            };
            let (channel, handle) = route(control);
            fixture.gate.block_error_detach(channel, handle, CONDITION);
            let mut closing = Box::pin(close_endpoint(&coordinator, &receiver, control));
            poll_fn(|cx| {
                assert!(closing.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            fixture.gate.wait().await;
            assert_eq!(
                observer.state(),
                if started {
                    NativeTransactionState::OwnerStarted
                } else {
                    NativeTransactionState::Faulted
                }
            );
            if let Some(ticket) = ticket {
                assert!(matches!(
                    ticket.try_claim(),
                    Err(NativeTransactionError::Faulted(NativeFault::Closed))
                ));
            }
            fixture.gate.unblock();
            bounded("error close reply", closing)
                .await
                .expect("error Detach flushed");
            error_detach(&mut fixture, channel, handle).await;
            if let Some(claim) = claim {
                claim.finish(NativeTransactionDecision::Committed);
                assert_eq!(observer.state(), NativeTransactionState::Committed);
                if control {
                    let (result, disposition) = tokio::join!(
                        resources.finish(),
                        fixture.peer.disposition(POST_CHANNEL, 0)
                    );
                    assert!(
                        result.is_err(),
                        "retired control cannot publish a fake control ACK"
                    );
                    assert!(matches!(
                        disposition.state,
                        Some(DeliveryState::Accepted(_))
                    ));
                } else {
                    assert_eq!(fixture.finish_committed(resources).await, 0);
                }
                assert_eq!(observer.state(), NativeTransactionState::Committed);
            } else {
                drop(resources);
            }
            fixture.peer.barrier(HEALTHY_CHANNEL).await;
            bounded("Ready close shutdown", fixture.connection.shutdown()).await;
        }
    }
}

#[tokio::test]
async fn unpolled_error_close_is_inert_but_queued_cancellation_does_not_revoke_detach() {
    for control in [false, true] {
        let mut fixture = Fixture::new(true).await;
        let (_controls, mut coordinator) = fixture.coordinator().await;
        let (_data, mut receiver) = fixture.receiver().await;
        let _healthy = fixture.session(HEALTHY_CHANNEL).await;
        let transaction = id(199);
        let observer = fixture.declare(&mut coordinator, &transaction).await;
        let posting = held(&mut fixture, &mut receiver, &transaction).await;
        drop(Box::pin(close_endpoint(&coordinator, &receiver, control)));
        fixture.peer.barrier(HEALTHY_CHANNEL).await;
        assert_eq!(observer.state(), NativeTransactionState::Pending);
        assert!(coordinator.controller_identity().is_active());
        assert!(receiver.receiver_identity().is_active());
        let (channel, handle) = route(control);
        fixture.gate.block_error_detach(channel, handle, CONDITION);
        let mut closing = Box::pin(close_endpoint(&coordinator, &receiver, control));
        poll_fn(|cx| {
            assert!(closing.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        fixture.gate.wait().await;
        assert_eq!(observer.state(), NativeTransactionState::Faulted);
        drop(closing);
        fixture.gate.unblock();
        error_detach(&mut fixture, channel, handle).await;
        fixture.peer.barrier(HEALTHY_CHANNEL).await;
        assert_eq!(posting.message(), &Message::data(vec![199; 127]));
        if control {
            assert!(!coordinator.controller_identity().is_active());
        } else {
            assert!(!receiver.receiver_identity().is_active());
        }
        assert!(fixture.connection.connection_identity().is_active());
        drop(posting);
        bounded(
            "canceled error close shutdown",
            fixture.connection.shutdown(),
        )
        .await;
    }
}

#[tokio::test]
async fn error_detach_flush_failure_does_not_report_success_or_accept_retained_posting() {
    for control in [false, true] {
        let mut fixture = Fixture::new(true).await;
        let (_controls, mut coordinator) = fixture.coordinator().await;
        let (_data, mut receiver) = fixture.receiver().await;
        let transaction = id(200);
        let observer = fixture.declare(&mut coordinator, &transaction).await;
        let posting = held(&mut fixture, &mut receiver, &transaction).await;
        let (channel, handle) = route(control);
        fixture.gate.fail_error_detach(channel, handle, CONDITION);
        let (result, ()) = tokio::join!(
            bounded(
                "failed error close reply",
                close_endpoint(&coordinator, &receiver, control)
            ),
            error_detach(&mut fixture, channel, handle)
        );
        assert!(
            result.is_err(),
            "failed flush cannot report successful local close"
        );
        bounded(
            "failed close actor termination",
            fixture.connection.shutdown(),
        )
        .await;
        assert!(!fixture.connection.connection_identity().is_active());
        assert_eq!(observer.state(), NativeTransactionState::Faulted);
        assert_eq!(posting.message(), &Message::data(vec![200; 127]));
        drop(posting);
    }
}
