use std::{future::poll_fn, task::Poll};

use super::*;

fn id(byte: u8) -> TransactionId {
    TransactionId::new([byte]).expect("opaque bounded ID")
}

#[tokio::test]
async fn opt_in_coordinator_approval_cannot_change_kind_connection_or_generation() {
    let mut first = Fixture::new(true).await;
    let mut session = first.session(CONTROL_CHANNEL).await;
    first
        .peer
        .attach(CONTROL_CHANNEL, coordinator_attach(CONTROL_HANDLE))
        .await;
    let incoming = bounded("coordinator receipt", session.next_incoming_attach())
        .await
        .expect("coordinator request");
    assert!(session.accept_attach(incoming.clone(), 0).await.is_err());
    assert!(
        session
            .accept_transactional_receiver(incoming.clone(), 0)
            .await
            .is_err()
    );
    let mut changed = incoming.clone();
    changed.target = Some(Target::new("queue").into());
    assert!(session.accept_coordinator(changed, 0).await.is_err());
    let mut changed = incoming.clone();
    changed.source = Some(Source {
        outcomes: Some(vec![crate::Symbol::from("amqp:rejected:list")].into()),
        ..Source::default()
    });
    assert!(
        session.accept_coordinator(changed, 0).await.is_err(),
        "immutable coordinator outcome profile"
    );
    let mut changed = incoming.clone();
    changed.target = Some(
        Coordinator {
            capabilities: Some(vec![crate::Symbol::from("amqp:local-transactions")].into()),
        }
        .into(),
    );
    assert!(
        session.accept_coordinator(changed, 0).await.is_err(),
        "immutable coordinator capability profile"
    );
    let healthy = first.session(HEALTHY_CHANNEL).await;
    assert!(
        healthy
            .accept_coordinator(incoming.clone(), 0)
            .await
            .is_err()
    );
    let mut other = Fixture::new(true).await;
    let other_session = other.session(CONTROL_CHANNEL).await;
    assert!(
        other_session
            .accept_coordinator(incoming.clone(), 0)
            .await
            .is_err()
    );
    other.peer.barrier(CONTROL_CHANNEL).await;
    first.gate.block_attach(CONTROL_CHANNEL);
    let mut approving = Box::pin(session.accept_coordinator(incoming.clone(), 0));
    poll_fn(|cx| {
        assert!(approving.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    first.gate.wait().await;
    let response = first.peer.attached(CONTROL_CHANNEL).await;
    assert!(
        response
            .target
            .as_ref()
            .and_then(|target| target.as_coordinator())
            .is_some()
    );
    poll_fn(|cx| {
        assert!(approving.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    first.gate.unblock();
    let _endpoint = bounded("approval finishes after flush", approving)
        .await
        .expect("accepted coordinator");
    first.peer.credit(CONTROL_CHANNEL, CONTROL_HANDLE).await;
    assert!(
        session.accept_coordinator(incoming, 0).await.is_err(),
        "consumed approval cannot mint another endpoint"
    );
    first.peer.barrier(CONTROL_CHANNEL).await;
    first.connection.shutdown().await;
    other.connection.shutdown().await;
}

#[tokio::test]
async fn declare_registers_provided_id_only_after_its_declared_flush() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let (_data, mut receiver) = fixture.receiver().await;
    fixture
        .peer
        .command(
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            0,
            TransactionCommand::Declare(Declare::default()),
        )
        .await;
    let CoordinatorRequest::Declare(declare) = bounded("pending Declare", coordinator.recv())
        .await
        .expect("Declare receipt")
    else {
        panic!("Declare")
    };
    let transaction = id(1);
    fixture.gate.block_declared(CONTROL_CHANNEL, 0);
    let mut registering = Box::pin(declare.declared(transaction.clone()));
    poll_fn(|cx| {
        assert!(registering.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    fixture.gate.wait().await;
    let disposition = fixture.peer.disposition(CONTROL_CHANNEL, 0).await;
    assert!(
        matches!(disposition.state, Some(DeliveryState::Declared(crate::Declared { txn_id })) if txn_id == transaction)
    );
    poll_fn(|cx| {
        assert!(registering.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    fixture.gate.unblock();
    bounded("Declared flush completes registration", registering)
        .await
        .expect("registered transaction");
    fixture
        .post(&transaction, &Message::data(vec![1, 2, 3]))
        .await;
    let TransactionalIngress::Posting(posting) = bounded("registered-ID post", receiver.recv())
        .await
        .expect("posting")
    else {
        panic!("transactional posting")
    };
    assert_eq!(posting.transaction_id(), &transaction);
    drop(posting);
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn completed_post_queued_on_other_session_prevents_empty_discharge_overtake() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let (_data, mut receiver) = fixture.receiver().await;
    let _healthy = fixture.session(HEALTHY_CHANNEL).await;
    let transaction = id(2);
    fixture.declare(&mut coordinator, &transaction).await;
    let message = Message::data(vec![2; 127]);
    fixture.post(&transaction, &message).await;
    let sealed = fixture
        .discharge(&mut coordinator, &transaction, false)
        .await;
    assert_eq!(sealed.state(), NativeTransactionState::Sealed);
    let mut waiting = Box::pin(sealed.wait_ready());
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    fixture.peer.barrier(HEALTHY_CHANNEL).await;
    let TransactionalIngress::Posting(posting) =
        bounded("earlier queued post is still deliverable", receiver.recv())
            .await
            .expect("posting")
    else {
        panic!("posting")
    };
    assert_eq!(posting.message(), &message);
    assert_eq!(posting.message_format(), 0);
    let prepared = fixture.provisional(posting, &transaction).await;
    assert_eq!(prepared.message(), &message);
    assert_eq!(prepared.transaction_id(), &transaction);
    bounded("sealing waits outside the actor", waiting)
        .await
        .expect("native readiness");
    assert_eq!(sealed.state(), NativeTransactionState::Ready);
    let ready = sealed.prepare(vec![prepared]).expect("exact posting set");
    fixture.committed(ready).await;
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn omitted_continuation_state_inherits_first_fragment_transaction() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let (_data, mut receiver) = fixture.receiver().await;
    let transaction = id(3);
    fixture.declare(&mut coordinator, &transaction).await;
    let message = Message::data(vec![3; 127]);
    let encoded = encode_message(&message).expect("fragmented message");
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            first(POST_HANDLE, 0, Some(transaction.clone()), true),
            encoded[..17].to_vec(),
        )
        .await;
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            continuation(POST_HANDLE, None, false),
            encoded[17..].to_vec(),
        )
        .await;
    let TransactionalIngress::Posting(posting) =
        bounded("completed inherited post", receiver.recv())
            .await
            .expect("posting")
    else {
        panic!("posting")
    };
    assert_eq!(posting.transaction_id(), &transaction);
    assert_eq!(posting.message(), &message);
    let prepared = fixture.provisional(posting, &transaction).await;
    let sealed = fixture
        .discharge(&mut coordinator, &transaction, false)
        .await;
    bounded("completed fragment readiness", sealed.wait_ready())
        .await
        .expect("ready");
    fixture
        .committed(sealed.prepare(vec![prepared]).expect("one exact posting"))
        .await;
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn provisional_flush_keeps_sealed_readiness_pending_and_payload_owned() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let (_data, mut receiver) = fixture.receiver().await;
    let transaction = id(4);
    fixture.declare(&mut coordinator, &transaction).await;
    let message = Message::data(vec![4; 127]);
    fixture.post(&transaction, &message).await;
    let sealed = fixture
        .discharge(&mut coordinator, &transaction, false)
        .await;
    let TransactionalIngress::Posting(posting) = bounded("posting receipt", receiver.recv())
        .await
        .expect("posting")
    else {
        panic!("posting")
    };
    fixture.gate.block_provisional(POST_CHANNEL, 0);
    let mut accepting = Box::pin(posting.provisional_accept());
    poll_fn(|cx| {
        assert!(accepting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    fixture.gate.wait().await;
    let disposition = fixture.peer.disposition(POST_CHANNEL, 0).await;
    assert!(!disposition.settled);
    assert!(
        matches!(disposition.state, Some(DeliveryState::Transactional(TransactionalState { txn_id, outcome: Some(Outcome::Accepted(_)) })) if txn_id == transaction)
    );
    let mut ready = Box::pin(sealed.wait_ready());
    poll_fn(|cx| {
        assert!(ready.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    fixture.gate.unblock();
    let prepared = bounded("ACK flush completes", accepting)
        .await
        .expect("prepared after flush");
    assert_eq!(prepared.message(), &message);
    bounded("readiness follows ACK flush", ready)
        .await
        .expect("ready");
    fixture
        .committed(sealed.prepare(vec![prepared]).expect("prepared set"))
        .await;
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn ordinary_ingress_remains_retained_and_settleable_on_opted_in_receiver() {
    let mut fixture = Fixture::new(true).await;
    let (_session, mut receiver) = fixture.receiver().await;
    let message = Message::data(vec![5; 127]);
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            first(POST_HANDLE, 0, None, false),
            encode_message(&message).expect("message encoding"),
        )
        .await;
    let TransactionalIngress::Ordinary(receipt) =
        bounded("ordinary retained receive", receiver.recv())
            .await
            .expect("ordinary ingress")
    else {
        panic!("ordinary receipt")
    };
    assert_eq!(receipt.message(), &message);
    assert!(receipt.belongs_to_connection(fixture.connection.connection_identity()));
    let (result, disposition) = tokio::join!(
        receiver.accept_retained(&receipt),
        fixture.peer.disposition(POST_CHANNEL, 0)
    );
    result.expect("ordinary settlement");
    assert!(disposition.settled);
    assert!(matches!(
        disposition.state,
        Some(DeliveryState::Accepted(_))
    ));
    drop(receipt);
    fixture.peer.barrier(POST_CHANNEL).await;
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn sender_transport_ack_does_not_erase_exact_prepared_post_obligation() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let (_data, mut receiver) = fixture.receiver().await;
    let transaction = id(6);
    fixture.declare(&mut coordinator, &transaction).await;
    let message = Message::data(vec![6; 127]);
    fixture.post(&transaction, &message).await;
    let TransactionalIngress::Posting(posting) = bounded("posting receipt", receiver.recv())
        .await
        .expect("posting")
    else {
        panic!("posting")
    };
    let prepared = fixture.provisional(posting, &transaction).await;
    fixture
        .peer
        .send(
            POST_CHANNEL,
            Performative::Disposition(Disposition {
                role: Role::Sender,
                first: 0,
                last: None,
                settled: true,
                state: None,
                batchable: false,
            }),
            Vec::new(),
        )
        .await;
    fixture.peer.barrier(POST_CHANNEL).await;
    assert_eq!(prepared.message(), &message);
    let replacement_message = Message::data(b"replacement generation".to_vec());
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            first(POST_HANDLE, 0, None, false),
            encode_message(&replacement_message).expect("replacement encoding"),
        )
        .await;
    let TransactionalIngress::Ordinary(replacement) = bounded(
        "same numeric ID is a fresh ordinary delivery",
        receiver.recv(),
    )
    .await
    .expect("replacement receipt") else {
        panic!("ordinary replacement")
    };
    assert_eq!(replacement.message(), &replacement_message);
    let sealed = fixture
        .discharge(&mut coordinator, &transaction, false)
        .await;
    bounded(
        "prepared post remains a native obligation",
        sealed.wait_ready(),
    )
    .await
    .expect("ready");
    assert_eq!(
        fixture
            .committed(
                sealed
                    .prepare(vec![prepared])
                    .expect("original exact generation remains required")
            )
            .await,
        0,
        "old final ACK cannot touch the reused delivery ID"
    );
    let (result, disposition) = tokio::join!(
        receiver.accept_retained(&replacement),
        fixture.peer.disposition(POST_CHANNEL, 0)
    );
    result.expect("replacement remains independently unsettled");
    assert!(disposition.settled);
    assert!(matches!(
        disposition.state,
        Some(DeliveryState::Accepted(_))
    ));
    drop(replacement);
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn sender_settlement_before_provisional_cannot_ack_a_reused_delivery_id() {
    for fragmented in [false, true] {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = fixture.coordinator().await;
        let (_data, mut receiver) = fixture.receiver().await;
        let _healthy = fixture.session(HEALTHY_CHANNEL).await;
        let transaction = id(10);
        fixture.declare(&mut coordinator, &transaction).await;
        let message = Message::data(b"original transaction".to_vec());
        let bytes = encode_message(&message).expect("original message");
        fixture
            .peer
            .transfer(
                POST_CHANNEL,
                first(POST_HANDLE, 0, Some(transaction), fragmented),
                if fragmented {
                    bytes[..17].to_vec()
                } else {
                    bytes.clone()
                },
            )
            .await;
        let mut posting = None;
        if !fragmented {
            let TransactionalIngress::Posting(receipt) =
                bounded("original complete posting", receiver.recv())
                    .await
                    .expect("posting")
            else {
                panic!("posting")
            };
            posting = Some(receipt);
        }
        fixture
            .peer
            .send(
                POST_CHANNEL,
                Performative::Disposition(Disposition {
                    role: Role::Sender,
                    first: 0,
                    last: None,
                    settled: true,
                    state: None,
                    batchable: false,
                }),
                Vec::new(),
            )
            .await;
        if fragmented {
            fixture
                .peer
                .transfer(
                    POST_CHANNEL,
                    continuation(POST_HANDLE, None, false),
                    bytes[17..].to_vec(),
                )
                .await;
            let TransactionalIngress::Posting(receipt) =
                bounded("sender-settled completion", receiver.recv())
                    .await
                    .expect("posting")
            else {
                panic!("posting")
            };
            posting = Some(receipt);
        }
        fixture.peer.barrier(POST_CHANNEL).await;
        let replacement_id = if fragmented { 0 } else { 1 };
        let replacement_message = Message::data(b"same numeric delivery replacement".to_vec());
        fixture
            .peer
            .transfer(
                POST_CHANNEL,
                first(POST_HANDLE, replacement_id, None, false),
                encode_message(&replacement_message).expect("replacement message"),
            )
            .await;
        let TransactionalIngress::Ordinary(replacement) =
            bounded("replacement generation", receiver.recv())
                .await
                .expect("ordinary receipt")
        else {
            panic!("ordinary replacement")
        };
        assert_eq!(replacement.message(), &replacement_message);
        assert!(
            bounded(
                "old provisional must be refused locally",
                posting.expect("original receipt").provisional_accept()
            )
            .await
            .is_err()
        );
        fixture.peer.barrier(POST_CHANNEL).await;
        fixture.peer.barrier(HEALTHY_CHANNEL).await;
        let (result, disposition) = tokio::join!(
            receiver.accept_retained(&replacement),
            fixture.peer.disposition(POST_CHANNEL, replacement_id)
        );
        result.expect("replacement remains unsettled until its own acceptance");
        assert!(disposition.settled);
        assert!(matches!(
            disposition.state,
            Some(DeliveryState::Accepted(_))
        ));
        drop(replacement);
        fixture.connection.shutdown().await;
    }
}

#[tokio::test]
async fn ready_requires_exact_posting_set_not_only_ready_observation() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let (_data, mut receiver) = fixture.receiver().await;
    let transaction = id(7);
    fixture.declare(&mut coordinator, &transaction).await;
    fixture
        .post(&transaction, &Message::data(vec![7; 127]))
        .await;
    let TransactionalIngress::Posting(posting) = bounded("posting receipt", receiver.recv())
        .await
        .expect("posting")
    else {
        panic!("posting")
    };
    let prepared = fixture.provisional(posting, &transaction).await;
    let sealed = fixture
        .discharge(&mut coordinator, &transaction, false)
        .await;
    bounded("ready observation", sealed.wait_ready())
        .await
        .expect("ready");
    assert!(matches!(
        sealed.prepare(Vec::new()),
        Err(NativeTransactionError::InvalidPreparedSet)
    ));
    assert_eq!(prepared.message(), &Message::data(vec![7; 127]));
    drop(prepared);
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn empty_discharge_has_real_control_bundle_and_cannot_be_reclaimed() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let transaction = id(8);
    fixture.declare(&mut coordinator, &transaction).await;
    let sealed = fixture
        .discharge(&mut coordinator, &transaction, false)
        .await;
    bounded("native empty readiness", sealed.wait_ready())
        .await
        .expect("ready");
    fixture
        .committed(
            sealed
                .prepare(Vec::new())
                .expect("genuine empty control authority"),
        )
        .await;
    fixture.peer.barrier(CONTROL_CHANNEL).await;
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn second_mode_final_outcomes_remain_unsettled_until_sender_ack() {
    let mut fixture = Fixture::new(true).await;
    let mut attach = coordinator_attach(CONTROL_HANDLE);
    attach.rcv_settle_mode = ReceiverSettleMode::Second;
    let (_control, mut coordinator) = fixture.coordinator_with(attach).await;
    let (_data, mut receiver) = fixture.receiver_with_mode(ReceiverSettleMode::Second).await;
    let transaction = id(9);
    fixture
        .peer
        .command(
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            0,
            TransactionCommand::Declare(Declare::default()),
        )
        .await;
    let CoordinatorRequest::Declare(declare) = bounded("Second Declare", coordinator.recv())
        .await
        .expect("Declare")
    else {
        panic!("Declare")
    };
    let (result, declared) = tokio::join!(
        declare.declared(transaction.clone()),
        fixture.peer.disposition(CONTROL_CHANNEL, 0)
    );
    result.expect("Declared flush");
    assert!(!declared.settled);
    assert!(
        matches!(declared.state, Some(DeliveryState::Declared(crate::Declared { txn_id })) if txn_id == transaction)
    );
    fixture
        .peer
        .send(
            CONTROL_CHANNEL,
            Performative::Disposition(Disposition {
                role: Role::Sender,
                first: 0,
                last: None,
                settled: true,
                state: None,
                batchable: false,
            }),
            Vec::new(),
        )
        .await;
    fixture
        .post(&transaction, &Message::data(vec![9; 127]))
        .await;
    let TransactionalIngress::Posting(posting) = bounded("Second posting", receiver.recv())
        .await
        .expect("post")
    else {
        panic!("posting")
    };
    let prepared = fixture.provisional(posting, &transaction).await;
    let sealed = fixture
        .discharge(&mut coordinator, &transaction, false)
        .await;
    bounded("Second readiness", sealed.wait_ready())
        .await
        .expect("ready");
    let (ticket, resources) = sealed
        .prepare(vec![prepared])
        .expect("exact Second bundle")
        .into_owner_parts();
    ticket
        .try_claim()
        .expect("native claim")
        .finish(NativeTransactionDecision::Committed);
    let responses = async {
        let post = fixture.peer.disposition(POST_CHANNEL, 0).await;
        assert!(!post.settled);
        assert!(
            matches!(post.state, Some(DeliveryState::Accepted(_))),
            "applied outcome is ordinary Accepted, not provisional TxState"
        );
        let control = fixture.peer.disposition(CONTROL_CHANNEL, 1).await;
        assert!(!control.settled);
        assert!(matches!(control.state, Some(DeliveryState::Accepted(_))));
    };
    let (result, ()) = tokio::join!(resources.finish(), responses);
    result.expect("Second final outcomes flushed");
    for (channel, delivery) in [(POST_CHANNEL, 0), (CONTROL_CHANNEL, 1)] {
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
    let message = Message::data(b"reused after Second ACK".to_vec());
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            first(POST_HANDLE, 0, None, false),
            encode_message(&message).expect("replacement message"),
        )
        .await;
    let TransactionalIngress::Ordinary(receipt) = bounded("post-ACK alias reuse", receiver.recv())
        .await
        .expect("replacement receipt")
    else {
        panic!("ordinary replacement")
    };
    assert_eq!(receipt.message(), &message);
    let (result, disposition) = tokio::join!(
        receiver.accept_retained(&receipt),
        fixture.peer.disposition(POST_CHANNEL, 0)
    );
    result.expect("independent ordinary acceptance");
    assert!(!disposition.settled);
    assert!(matches!(
        disposition.state,
        Some(DeliveryState::Accepted(_))
    ));
    drop(receipt);
    fixture.connection.shutdown().await;
}
