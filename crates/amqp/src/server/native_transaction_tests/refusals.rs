use super::*;
use crate::{NativeDeclarationRefusal, PendingDeclareReceipt};

const OTHER_CHANNEL: u16 = 37;
const OTHER_HANDLE: u32 = 43;
const STAGING_DESCRIPTION: &str = "native transaction staging was refused";

fn id(byte: u8) -> TransactionId {
    TransactionId::new([byte]).expect("bounded transaction ID")
}

fn profile(handle: u32, mode: ReceiverSettleMode, rejected: bool) -> Attach {
    let mut attach = coordinator_attach(handle);
    attach.rcv_settle_mode = mode;
    let mut outcomes = vec![crate::Symbol::from("amqp:declared:list")];
    if rejected {
        outcomes.push(crate::Symbol::from("amqp:rejected:list"));
    }
    attach.source = Some(Source {
        outcomes: Some(outcomes.into()),
        ..Source::default()
    });
    attach
}

async fn controller(
    fixture: &mut Fixture,
    channel: u16,
    handle: u32,
    mode: ReceiverSettleMode,
    rejected: bool,
) -> (ServerSession, CoordinatorEndpoint) {
    let mut session = fixture.session(channel).await;
    fixture
        .peer
        .attach(channel, profile(handle, mode.clone(), rejected))
        .await;
    let incoming = bounded(
        "refusal controller approval",
        session.next_incoming_attach(),
    )
    .await
    .expect("actor-approved coordinator");
    let responses = async {
        let attach = fixture.peer.attached(channel).await;
        assert_eq!(attach.handle, handle);
        assert_eq!(attach.rcv_settle_mode, mode);
        assert!(
            attach
                .target
                .as_ref()
                .and_then(|target| target.as_coordinator())
                .is_some()
        );
        fixture.peer.credit(channel, handle).await;
    };
    let (endpoint, ()) = tokio::join!(session.accept_coordinator(incoming, 0), responses);
    (session, endpoint.expect("accepted coordinator"))
}

async fn declare_receipt(
    fixture: &mut Fixture,
    coordinator: &mut CoordinatorEndpoint,
    channel: u16,
    handle: u32,
    delivery: u32,
) -> PendingDeclareReceipt {
    fixture
        .peer
        .command(
            channel,
            handle,
            delivery,
            TransactionCommand::Declare(Declare::default()),
        )
        .await;
    let CoordinatorRequest::Declare(receipt) =
        bounded("refused Declare receipt", coordinator.recv())
            .await
            .expect("valid Declare receipt")
    else {
        panic!("Declare request")
    };
    receipt
}

async fn sender_ack(fixture: &mut Fixture, channel: u16, delivery: u32) {
    fixture
        .peer
        .send(
            channel,
            Performative::Disposition(Disposition {
                role: Role::Sender,
                first: delivery,
                last: None,
                settled: true,
                state: None,
                batchable: false,
            }),
            Vec::new(),
        )
        .await;
    fixture.peer.barrier(channel).await;
}

async fn registered(
    fixture: &mut Fixture,
    coordinator: &mut CoordinatorEndpoint,
    channel: u16,
    handle: u32,
    delivery: u32,
    transaction: &TransactionId,
    mode: ReceiverSettleMode,
) -> NativeTransactionIdentity {
    let receipt = declare_receipt(fixture, coordinator, channel, handle, delivery).await;
    let (result, disposition) = tokio::join!(
        receipt.declared(transaction.clone()),
        fixture.peer.disposition(channel, delivery)
    );
    let observer = result.expect("registered transaction after flush");
    assert_eq!(disposition.settled, mode == ReceiverSettleMode::First);
    assert!(
        matches!(disposition.state, Some(DeliveryState::Declared(crate::Declared { txn_id })) if txn_id == *transaction)
    );
    if mode == ReceiverSettleMode::Second {
        sender_ack(fixture, channel, delivery).await;
    }
    observer
}

async fn seal(
    fixture: &mut Fixture,
    coordinator: &mut CoordinatorEndpoint,
    channel: u16,
    handle: u32,
    delivery: u32,
    transaction: &TransactionId,
) -> SealedDischargeReceipt {
    fixture
        .peer
        .command(
            channel,
            handle,
            delivery,
            TransactionCommand::Discharge(Discharge {
                txn_id: transaction.clone(),
                fail: Some(false),
            }),
        )
        .await;
    let CoordinatorRequest::Discharge(receipt) =
        bounded("staging refusal seal", coordinator.recv())
            .await
            .expect("valid sealed receipt")
    else {
        panic!("Discharge request")
    };
    assert!(
        !receipt.fail(),
        "local refusal is not a wire fail=true request"
    );
    assert_eq!(receipt.transaction_id(), transaction);
    receipt
}

async fn negative(
    fixture: &mut Fixture,
    channel: u16,
    handle: u32,
    delivery: u32,
    mode: ReceiverSettleMode,
    rejected: bool,
    description: &str,
) {
    for _ in 0..8 {
        match fixture.peer.frame().await {
            Frame::Amqp {
                channel: actual,
                performative: Some(Performative::Disposition(disposition)),
                payload,
            } if rejected => {
                assert_eq!(actual, channel);
                assert_eq!(disposition.role, Role::Receiver);
                assert_eq!(disposition.first, delivery);
                assert!(disposition.last.is_none());
                assert_eq!(disposition.settled, mode == ReceiverSettleMode::First);
                let Some(DeliveryState::Rejected(crate::Rejected { error: Some(error) })) =
                    disposition.state
                else {
                    panic!("only explicit Rejected, never Declared/Accepted")
                };
                assert_eq!(
                    error.condition.as_symbol().as_str(),
                    "amqp:transaction:rollback"
                );
                assert_eq!(error.description.as_deref(), Some(description));
                assert!(payload.is_empty());
                return;
            }
            Frame::Amqp {
                channel: actual,
                performative: Some(Performative::Detach(detach)),
                payload,
            } if !rejected => {
                assert_eq!(actual, channel);
                assert_eq!(detach.handle, handle);
                assert!(detach.closed);
                let error = detach.error.expect("explicit rollback Detach");
                assert_eq!(
                    error.condition.as_symbol().as_str(),
                    "amqp:transaction:rollback"
                );
                assert_eq!(error.description.as_deref(), Some(description));
                assert!(payload.is_empty());
                return;
            }
            Frame::Amqp {
                performative: Some(Performative::Flow(_)),
                ..
            } => {}
            frame => panic!("unexpected native negative response: {frame:?}"),
        }
    }
    panic!("no negative decision within bounded response count");
}

async fn empty_commit(
    fixture: &mut Fixture,
    coordinator: &mut CoordinatorEndpoint,
    channel: u16,
    handle: u32,
    delivery: u32,
    transaction: &TransactionId,
    mode: ReceiverSettleMode,
) {
    let sealed = seal(fixture, coordinator, channel, handle, delivery, transaction).await;
    bounded("healthy empty readiness", sealed.wait_ready())
        .await
        .expect("ready empty group");
    let (ticket, resources) = sealed
        .prepare(Vec::new())
        .expect("exact empty bundle")
        .into_owner_parts();
    ticket
        .try_claim()
        .expect("healthy native claim")
        .finish(NativeTransactionDecision::Committed);
    let (result, disposition) = tokio::join!(
        resources.finish(),
        fixture.peer.disposition(channel, delivery)
    );
    result.expect("healthy control decision flushed");
    assert_eq!(disposition.settled, mode == ReceiverSettleMode::First);
    assert!(matches!(
        disposition.state,
        Some(DeliveryState::Accepted(_))
    ));
    if mode == ReceiverSettleMode::Second {
        sender_ack(fixture, channel, delivery).await;
    }
}

async fn posting(
    fixture: &mut Fixture,
    receiver: &mut TransactionalReceiver,
    transaction: &TransactionId,
    delivery: u32,
    message: &Message,
) -> TransactionPostingReceipt {
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            first(POST_HANDLE, delivery, Some(transaction.clone()), false),
            encode_message(message).expect("valid post"),
        )
        .await;
    let TransactionalIngress::Posting(receipt) = bounded("retained posting", receiver.recv())
        .await
        .expect("posting receipt")
    else {
        panic!("transactional posting")
    };
    assert_eq!(receipt.message(), message);
    receipt
}

async fn ordinary_reuse(
    fixture: &mut Fixture,
    receiver: &mut TransactionalReceiver,
    delivery: u32,
    mode: ReceiverSettleMode,
) {
    let message = Message::data(b"ordinary replacement remains independent".to_vec());
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            first(POST_HANDLE, delivery, None, false),
            encode_message(&message).expect("replacement encoding"),
        )
        .await;
    let TransactionalIngress::Ordinary(receipt) = bounded("ordinary replacement", receiver.recv())
        .await
        .expect("ordinary receipt")
    else {
        panic!("ordinary replacement")
    };
    assert_eq!(receipt.message(), &message);
    let (result, disposition) = tokio::join!(
        receiver.accept_retained(&receipt),
        fixture.peer.disposition(POST_CHANNEL, delivery)
    );
    result.expect("replacement ordinary settlement");
    assert_eq!(disposition.settled, mode == ReceiverSettleMode::First);
    assert!(matches!(
        disposition.state,
        Some(DeliveryState::Accepted(_))
    ));
    if mode == ReceiverSettleMode::Second {
        sender_ack(fixture, POST_CHANNEL, delivery).await;
    }
}

#[tokio::test]
async fn declaration_refusal_reason_profile_and_settlement_matrix() {
    for (reason, description) in [
        (
            NativeDeclarationRefusal::ResourceLimit,
            "native transaction declaration resource limit reached",
        ),
        (
            NativeDeclarationRefusal::Unavailable,
            "native transaction declaration is unavailable",
        ),
    ] {
        for rejected in [false, true] {
            for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
                let mut fixture = Fixture::new(true).await;
                let (_control, mut coordinator) = controller(
                    &mut fixture,
                    CONTROL_CHANNEL,
                    CONTROL_HANDLE,
                    mode.clone(),
                    rejected,
                )
                .await;
                let (_other, mut other) = controller(
                    &mut fixture,
                    OTHER_CHANNEL,
                    OTHER_HANDLE,
                    ReceiverSettleMode::First,
                    true,
                )
                .await;
                let _healthy = fixture.session(HEALTHY_CHANNEL).await;
                let transaction = id(100);
                let observer = registered(
                    &mut fixture,
                    &mut coordinator,
                    CONTROL_CHANNEL,
                    CONTROL_HANDLE,
                    0,
                    &transaction,
                    mode.clone(),
                )
                .await;
                let receipt = declare_receipt(
                    &mut fixture,
                    &mut coordinator,
                    CONTROL_CHANNEL,
                    CONTROL_HANDLE,
                    1,
                )
                .await;
                let (result, ()) = tokio::join!(
                    receipt.refuse(reason),
                    negative(
                        &mut fixture,
                        CONTROL_CHANNEL,
                        CONTROL_HANDLE,
                        1,
                        mode.clone(),
                        rejected,
                        description
                    )
                );
                result.expect("negotiated declaration refusal flushed");
                if rejected {
                    assert!(coordinator.controller_identity().is_active());
                    assert_eq!(observer.state(), NativeTransactionState::Pending);
                    if mode == ReceiverSettleMode::Second {
                        sender_ack(&mut fixture, CONTROL_CHANNEL, 1).await;
                    }
                    empty_commit(
                        &mut fixture,
                        &mut coordinator,
                        CONTROL_CHANNEL,
                        CONTROL_HANDLE,
                        1,
                        &transaction,
                        mode.clone(),
                    )
                    .await;
                } else {
                    assert!(!coordinator.controller_identity().is_active());
                    assert_eq!(observer.state(), NativeTransactionState::Faulted);
                }
                let other_id = id(101);
                registered(
                    &mut fixture,
                    &mut other,
                    OTHER_CHANNEL,
                    OTHER_HANDLE,
                    0,
                    &other_id,
                    ReceiverSettleMode::First,
                )
                .await;
                empty_commit(
                    &mut fixture,
                    &mut other,
                    OTHER_CHANNEL,
                    OTHER_HANDLE,
                    1,
                    &other_id,
                    ReceiverSettleMode::First,
                )
                .await;
                fixture.peer.barrier(HEALTHY_CHANNEL).await;
                fixture.connection.shutdown().await;
            }
        }
    }
}

#[tokio::test]
async fn refused_declarations_do_not_allocate_native_group_slots() {
    for reason in [
        NativeDeclarationRefusal::ResourceLimit,
        NativeDeclarationRefusal::Unavailable,
    ] {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = controller(
            &mut fixture,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            ReceiverSettleMode::First,
            true,
        )
        .await;
        let description = match reason {
            NativeDeclarationRefusal::ResourceLimit => {
                "native transaction declaration resource limit reached"
            }
            NativeDeclarationRefusal::Unavailable => {
                "native transaction declaration is unavailable"
            }
        };
        for delivery in 0..32 {
            let receipt = declare_receipt(
                &mut fixture,
                &mut coordinator,
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                delivery,
            )
            .await;
            let (result, ()) = tokio::join!(
                receipt.refuse(reason),
                negative(
                    &mut fixture,
                    CONTROL_CHANNEL,
                    CONTROL_HANDLE,
                    delivery,
                    ReceiverSettleMode::First,
                    true,
                    description
                )
            );
            result.expect("refused without allocating an ID");
        }
        let mut observers = Vec::new();
        for delivery in 0..32 {
            observers.push(
                registered(
                    &mut fixture,
                    &mut coordinator,
                    CONTROL_CHANNEL,
                    CONTROL_HANDLE,
                    delivery,
                    &id(delivery as u8),
                    ReceiverSettleMode::First,
                )
                .await,
            );
        }
        assert_eq!(observers.len(), 32);
        assert!(
            observers
                .iter()
                .all(|observer| observer.state() == NativeTransactionState::Pending)
        );
        let overflow_id = id(200);
        let receipt = declare_receipt(
            &mut fixture,
            &mut coordinator,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            32,
        )
        .await;
        let (result, ()) = tokio::join!(
            receipt.declared(overflow_id.clone()),
            negative(
                &mut fixture,
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                32,
                ReceiverSettleMode::First,
                true,
                "native transaction resource limit reached"
            )
        );
        assert!(
            result.is_err(),
            "group registration refuses the thirty-third live ID"
        );
        assert!(coordinator.controller_identity().is_active());
        assert!(
            observers
                .iter()
                .all(|observer| observer.state() == NativeTransactionState::Pending)
        );
        empty_commit(
            &mut fixture,
            &mut coordinator,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            32,
            &id(0),
            ReceiverSettleMode::First,
        )
        .await;
        assert_eq!(observers[0].state(), NativeTransactionState::Committed);
        let admitted = registered(
            &mut fixture,
            &mut coordinator,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            33,
            &overflow_id,
            ReceiverSettleMode::First,
        )
        .await;
        assert_eq!(
            admitted.state(),
            NativeTransactionState::Pending,
            "refused ID never consumed a native group or ID registration"
        );
        assert!(
            observers[1..]
                .iter()
                .all(|observer| observer.state() == NativeTransactionState::Pending)
        );
        fixture.peer.barrier(CONTROL_CHANNEL).await;
        fixture.connection.shutdown().await;
    }
}

#[tokio::test]
async fn staging_refusal_cleans_complete_and_prepared_posts_before_control_decision() {
    for rejected in [false, true] {
        for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
            let mut fixture = Fixture::new(true).await;
            let (_control, mut coordinator) = controller(
                &mut fixture,
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                mode.clone(),
                rejected,
            )
            .await;
            let (_other, mut other) = controller(
                &mut fixture,
                OTHER_CHANNEL,
                OTHER_HANDLE,
                ReceiverSettleMode::First,
                true,
            )
            .await;
            let other_id = id(112);
            let other_observer = registered(
                &mut fixture,
                &mut other,
                OTHER_CHANNEL,
                OTHER_HANDLE,
                0,
                &other_id,
                ReceiverSettleMode::First,
            )
            .await;
            let (_data, mut receiver) = fixture.receiver_with_mode(mode.clone()).await;
            let _healthy = fixture.session(HEALTHY_CHANNEL).await;
            let transaction = id(110);
            let observer = registered(
                &mut fixture,
                &mut coordinator,
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                0,
                &transaction,
                mode.clone(),
            )
            .await;
            let complete_message = Message::data(b"complete application-held post".to_vec());
            let complete = posting(
                &mut fixture,
                &mut receiver,
                &transaction,
                0,
                &complete_message,
            )
            .await;
            let prepared_message = Message::data(b"provisional application-held post".to_vec());
            let receipt = posting(
                &mut fixture,
                &mut receiver,
                &transaction,
                1,
                &prepared_message,
            )
            .await;
            let (prepared, provisional) = tokio::join!(
                receipt.provisional_accept(),
                fixture.peer.disposition(POST_CHANNEL, 1)
            );
            let prepared = prepared.expect("provisional flush");
            assert!(!provisional.settled);
            assert!(matches!(
                provisional.state,
                Some(DeliveryState::Transactional(TransactionalState {
                    outcome: Some(Outcome::Accepted(_)),
                    ..
                }))
            ));
            let sealed = seal(
                &mut fixture,
                &mut coordinator,
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                1,
                &transaction,
            )
            .await;
            assert_eq!(sealed.state(), NativeTransactionState::Sealed);
            let responses = async {
                for delivery in [0, 1] {
                    let disposition = fixture.peer.disposition(POST_CHANNEL, delivery).await;
                    assert_eq!(disposition.settled, mode == ReceiverSettleMode::First);
                    assert!(
                        disposition.state.is_none(),
                        "cleanup never claims an applied outcome"
                    );
                }
                negative(
                    &mut fixture,
                    CONTROL_CHANNEL,
                    CONTROL_HANDLE,
                    1,
                    mode.clone(),
                    rejected,
                    STAGING_DESCRIPTION,
                )
                .await;
            };
            let (result, ()) = tokio::join!(sealed.refuse_staging(), responses);
            result.expect("staging refusal completed");
            assert_eq!(observer.state(), NativeTransactionState::Aborted);
            assert_eq!(complete.message(), &complete_message);
            assert_eq!(prepared.message(), &prepared_message);
            if mode == ReceiverSettleMode::Second {
                for delivery in [0, 1] {
                    sender_ack(&mut fixture, POST_CHANNEL, delivery).await;
                }
                if rejected {
                    sender_ack(&mut fixture, CONTROL_CHANNEL, 1).await;
                }
            }
            let (result, ()) = tokio::join!(
                complete.provisional_accept(),
                fixture.peer.barrier(POST_CHANNEL)
            );
            assert!(
                result.is_err(),
                "aborted original cannot acknowledge a reused alias"
            );
            drop(prepared);
            ordinary_reuse(&mut fixture, &mut receiver, 0, mode.clone()).await;
            if rejected {
                let healthy_id = id(111);
                registered(
                    &mut fixture,
                    &mut coordinator,
                    CONTROL_CHANNEL,
                    CONTROL_HANDLE,
                    1,
                    &healthy_id,
                    mode.clone(),
                )
                .await;
                empty_commit(
                    &mut fixture,
                    &mut coordinator,
                    CONTROL_CHANNEL,
                    CONTROL_HANDLE,
                    2,
                    &healthy_id,
                    mode.clone(),
                )
                .await;
            } else {
                assert!(!coordinator.controller_identity().is_active());
            }
            assert_eq!(other_observer.state(), NativeTransactionState::Pending);
            empty_commit(
                &mut fixture,
                &mut other,
                OTHER_CHANNEL,
                OTHER_HANDLE,
                1,
                &other_id,
                ReceiverSettleMode::First,
            )
            .await;
            fixture.peer.barrier(HEALTHY_CHANNEL).await;
            fixture.connection.shutdown().await;
        }
    }
}

#[tokio::test]
async fn staging_fault_refusal_is_a_known_abort_not_an_accepted_empty_commit() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = controller(
        &mut fixture,
        CONTROL_CHANNEL,
        CONTROL_HANDLE,
        ReceiverSettleMode::First,
        true,
    )
    .await;
    let (_data, mut receiver) = fixture.receiver().await;
    let transaction = id(120);
    let observer = registered(
        &mut fixture,
        &mut coordinator,
        CONTROL_CHANNEL,
        CONTROL_HANDLE,
        0,
        &transaction,
        ReceiverSettleMode::First,
    )
    .await;
    let receipt = posting(
        &mut fixture,
        &mut receiver,
        &transaction,
        0,
        &Message::data(vec![12; 64]),
    )
    .await;
    let sealed = seal(
        &mut fixture,
        &mut coordinator,
        CONTROL_CHANNEL,
        CONTROL_HANDLE,
        1,
        &transaction,
    )
    .await;
    receipt.fail();
    assert_eq!(observer.state(), NativeTransactionState::Faulted);
    let responses = async {
        let cleanup = fixture.peer.disposition(POST_CHANNEL, 0).await;
        assert!(cleanup.settled);
        assert!(cleanup.state.is_none());
        negative(
            &mut fixture,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            1,
            ReceiverSettleMode::First,
            true,
            STAGING_DESCRIPTION,
        )
        .await;
    };
    let (result, ()) = tokio::join!(sealed.refuse_staging(), responses);
    result.expect("faulted staging refused");
    assert_eq!(observer.state(), NativeTransactionState::Aborted);
    ordinary_reuse(&mut fixture, &mut receiver, 0, ReceiverSettleMode::First).await;
    fixture.peer.barrier(CONTROL_CHANNEL).await;
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn staging_refusal_does_not_touch_sender_settled_replacement_generation() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = controller(
        &mut fixture,
        CONTROL_CHANNEL,
        CONTROL_HANDLE,
        ReceiverSettleMode::First,
        true,
    )
    .await;
    let (_data, mut receiver) = fixture.receiver().await;
    let transaction = id(121);
    let observer = registered(
        &mut fixture,
        &mut coordinator,
        CONTROL_CHANNEL,
        CONTROL_HANDLE,
        0,
        &transaction,
        ReceiverSettleMode::First,
    )
    .await;
    fixture
        .post(
            &transaction,
            &Message::data(b"old retained payload".to_vec()),
        )
        .await;
    let TransactionalIngress::Posting(post) = bounded("posting", receiver.recv())
        .await
        .expect("valid posting")
    else {
        panic!("posting")
    };
    let prepared = fixture.provisional(post, &transaction).await;
    sender_ack(&mut fixture, POST_CHANNEL, 0).await;
    let replacement_message = Message::data(b"new numeric alias".to_vec());
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            first(POST_HANDLE, 0, None, false),
            encode_message(&replacement_message).expect("replacement encoding"),
        )
        .await;
    let TransactionalIngress::Ordinary(replacement) =
        bounded("replacement receipt", receiver.recv())
            .await
            .expect("valid replacement")
    else {
        panic!("ordinary replacement")
    };
    let sealed = seal(
        &mut fixture,
        &mut coordinator,
        CONTROL_CHANNEL,
        CONTROL_HANDLE,
        1,
        &transaction,
    )
    .await;
    assert_eq!(sealed.state(), NativeTransactionState::Ready);
    let (result, ()) = tokio::join!(
        sealed.refuse_staging(),
        negative(
            &mut fixture,
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            1,
            ReceiverSettleMode::First,
            true,
            STAGING_DESCRIPTION
        )
    );
    result.expect("refusal has no replacement-ID disposition");
    assert_eq!(observer.state(), NativeTransactionState::Aborted);
    assert_eq!(
        prepared.message(),
        &Message::data(b"old retained payload".to_vec())
    );
    assert_eq!(replacement.message(), &replacement_message);
    let (result, response) = tokio::join!(
        receiver.accept_retained(&replacement),
        fixture.peer.disposition(POST_CHANNEL, 0)
    );
    result.expect("replacement remains settleable");
    assert!(response.settled);
    assert!(matches!(response.state, Some(DeliveryState::Accepted(_))));
    drop(prepared);
    fixture.peer.barrier(CONTROL_CHANNEL).await;
    fixture.connection.shutdown().await;
}

#[path = "refusals/lifetime.rs"]
mod lifetime;
