use std::{future::poll_fn, task::Poll};

use super::*;

fn id(byte: u8) -> TransactionId {
    TransactionId::new([byte]).expect("bounded ID")
}

fn rejected_profile() -> Attach {
    let mut attach = coordinator_attach(CONTROL_HANDLE);
    attach.source = Some(Source {
        outcomes: Some(
            vec![
                crate::Symbol::from("amqp:declared:list"),
                crate::Symbol::from("amqp:rejected:list"),
            ]
            .into(),
        ),
        ..Source::default()
    });
    attach
}

#[tokio::test]
async fn dropped_or_failed_post_cannot_turn_sealed_group_into_ready_empty() {
    for failed in [false, true] {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = fixture.coordinator().await;
        let (_data, mut receiver) = fixture.receiver().await;
        let _healthy = fixture.session(HEALTHY_CHANNEL).await;
        let transaction = id(40);
        fixture.declare(&mut coordinator, &transaction).await;
        fixture
            .post(&transaction, &Message::data(vec![40; 127]))
            .await;
        let sealed = fixture
            .discharge(&mut coordinator, &transaction, false)
            .await;
        let TransactionalIngress::Posting(posting) = bounded("post receipt", receiver.recv())
            .await
            .expect("posting")
        else {
            panic!("posting")
        };
        if failed {
            posting.fail();
        } else {
            drop(posting);
        }
        assert_eq!(sealed.state(), NativeTransactionState::Faulted);
        assert_eq!(
            bounded("failed post cannot hang readiness", sealed.wait_ready()).await,
            Err(NativeTransactionError::Faulted(if failed {
                NativeFault::Stage
            } else {
                NativeFault::Dropped
            }))
        );
        assert!(matches!(
            sealed.prepare(Vec::new()),
            Err(NativeTransactionError::NotReady)
        ));
        fixture.peer.barrier(HEALTHY_CHANNEL).await;
        fixture.connection.shutdown().await;
    }
}

#[tokio::test]
async fn failed_provisional_flush_faults_sealed_group_instead_of_exposing_prepared_post() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let (_data, mut receiver) = fixture.receiver().await;
    let transaction = id(41);
    fixture.declare(&mut coordinator, &transaction).await;
    fixture
        .post(&transaction, &Message::data(vec![41; 127]))
        .await;
    let sealed = fixture
        .discharge(&mut coordinator, &transaction, false)
        .await;
    let TransactionalIngress::Posting(posting) = bounded("post receipt", receiver.recv())
        .await
        .expect("posting")
    else {
        panic!("posting")
    };
    fixture.gate.fail_provisional(POST_CHANNEL, 0);
    assert!(
        bounded("failed provisional write", posting.provisional_accept())
            .await
            .is_err()
    );
    assert_eq!(
        bounded(
            "failed flush cannot become native Ready",
            sealed.wait_ready()
        )
        .await,
        Err(NativeTransactionError::Faulted(NativeFault::Flush))
    );
    assert!(matches!(
        sealed.prepare(Vec::new()),
        Err(NativeTransactionError::NotReady)
    ));
    bounded("tainted driver shutdown", fixture.connection.shutdown()).await;
}

#[tokio::test]
async fn canceled_provisional_caller_cannot_leave_acknowledged_empty_group() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let (_data, mut receiver) = fixture.receiver().await;
    let _healthy = fixture.session(HEALTHY_CHANNEL).await;
    let transaction = id(42);
    fixture.declare(&mut coordinator, &transaction).await;
    fixture
        .post(&transaction, &Message::data(vec![42; 127]))
        .await;
    let sealed = fixture
        .discharge(&mut coordinator, &transaction, false)
        .await;
    let TransactionalIngress::Posting(posting) = bounded("post receipt", receiver.recv())
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
    drop(accepting);
    fixture.gate.unblock();
    let acknowledgement = fixture.peer.disposition(POST_CHANNEL, 0).await;
    assert!(!acknowledgement.settled);
    assert!(matches!(
        acknowledgement.state,
        Some(DeliveryState::Transactional(_))
    ));
    fixture.peer.barrier(HEALTHY_CHANNEL).await;
    assert_eq!(
        bounded(
            "lost prepared reply faults the native group",
            sealed.wait_ready()
        )
        .await,
        Err(NativeTransactionError::Faulted(NativeFault::Dropped))
    );
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn partial_post_at_actor_seal_refuses_control_without_waiting_for_continuation() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator_with(rejected_profile()).await;
    let (_data, _receiver) = fixture.receiver().await;
    let _healthy = fixture.session(HEALTHY_CHANNEL).await;
    let transaction = id(43);
    fixture.declare(&mut coordinator, &transaction).await;
    let bytes = encode_message(&Message::data(vec![43; 127])).expect("message encoding");
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            first(POST_HANDLE, 0, Some(transaction.clone()), true),
            bytes[..17].to_vec(),
        )
        .await;
    fixture
        .peer
        .command(
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            1,
            TransactionCommand::Discharge(Discharge {
                txn_id: transaction,
                fail: Some(false),
            }),
        )
        .await;
    fixture
        .peer
        .refusal(CONTROL_CHANNEL, CONTROL_HANDLE, "amqp:transaction:rollback")
        .await;
    assert!(matches!(
        bounded(
            "partial seal never publishes empty Discharge",
            coordinator.recv()
        )
        .await,
        Err(EngineError::RemoteDetached)
    ));
    fixture.peer.barrier(HEALTHY_CHANNEL).await;
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn changed_continuation_transaction_or_outcome_is_scoped_refusal() {
    for state in [
        DeliveryState::Transactional(TransactionalState {
            txn_id: id(99),
            outcome: None,
        }),
        DeliveryState::Transactional(TransactionalState {
            txn_id: id(44),
            outcome: Some(Outcome::Accepted(Accepted)),
        }),
    ] {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = fixture.coordinator().await;
        let (_data, mut receiver) = fixture.receiver().await;
        let _healthy = fixture.session(HEALTHY_CHANNEL).await;
        let transaction = id(44);
        fixture.declare(&mut coordinator, &transaction).await;
        let bytes = encode_message(&Message::data(vec![44; 127])).expect("message encoding");
        fixture
            .peer
            .transfer(
                POST_CHANNEL,
                first(POST_HANDLE, 0, Some(transaction), true),
                bytes[..17].to_vec(),
            )
            .await;
        fixture
            .peer
            .transfer(
                POST_CHANNEL,
                continuation(POST_HANDLE, Some(state), false),
                bytes[17..].to_vec(),
            )
            .await;
        fixture
            .peer
            .refusal(POST_CHANNEL, POST_HANDLE, "amqp:transaction:rollback")
            .await;
        assert!(matches!(
            bounded("no changed-identity posting", receiver.recv()).await,
            Err(EngineError::RemoteDetached)
        ));
        fixture.peer.barrier(HEALTHY_CHANNEL).await;
        fixture.connection.shutdown().await;
    }
}

#[tokio::test]
async fn late_post_after_discharge_actor_seal_cannot_join_the_prepared_set() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let (_data, mut receiver) = fixture.receiver().await;
    let _healthy = fixture.session(HEALTHY_CHANNEL).await;
    let transaction = id(45);
    fixture.declare(&mut coordinator, &transaction).await;
    fixture
        .post(&transaction, &Message::data(vec![45; 127]))
        .await;
    let TransactionalIngress::Posting(earlier) =
        bounded("earlier completed posting", receiver.recv())
            .await
            .expect("posting")
    else {
        panic!("posting")
    };
    let sealed = fixture
        .discharge(&mut coordinator, &transaction, false)
        .await;
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            first(POST_HANDLE, 1, Some(transaction), false),
            encode_message(&Message::data(vec![46; 127])).expect("late post encoding"),
        )
        .await;
    fixture
        .peer
        .refusal(POST_CHANNEL, POST_HANDLE, "amqp:transaction:rollback")
        .await;
    assert_eq!(
        bounded(
            "late posting faults the sealed group before link retirement",
            sealed.wait_ready()
        )
        .await,
        Err(NativeTransactionError::Faulted(NativeFault::Stage))
    );
    drop(earlier);
    assert!(matches!(
        bounded("retired inbox cannot stage late post", receiver.recv()).await,
        Err(EngineError::RemoteDetached)
    ));
    fixture.peer.barrier(HEALTHY_CHANNEL).await;
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn global_declare_and_sender_settled_control_are_refused_before_application_receipt() {
    for sender_settled in [false, true] {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = fixture.coordinator().await;
        let _healthy = fixture.session(HEALTHY_CHANNEL).await;
        let command = TransactionCommand::Declare(Declare {
            global_id: (!sender_settled).then_some(Value::Uint(7)),
        });
        let payload = encode_message(&Message {
            body: crate::Body::Value(Value::from(command)),
            ..Message::default()
        })
        .expect("Declare encoding");
        let mut transfer = first(CONTROL_HANDLE, 0, None, false);
        transfer.settled = Some(sender_settled);
        fixture
            .peer
            .transfer(CONTROL_CHANNEL, transfer, payload)
            .await;
        fixture
            .peer
            .refusal(
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                if sender_settled {
                    "amqp:illegal-state"
                } else {
                    "amqp:not-implemented"
                },
            )
            .await;
        assert!(matches!(
            bounded("invalid Declare cannot register ID", coordinator.recv()).await,
            Err(EngineError::RemoteDetached)
        ));
        fixture.peer.barrier(HEALTHY_CHANNEL).await;
        fixture.connection.shutdown().await;
    }
}

#[tokio::test]
async fn advertised_rejected_unknown_id_preserves_controller_and_valid_group() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator_with(rejected_profile()).await;
    let transaction = id(47);
    fixture.declare(&mut coordinator, &transaction).await;
    fixture
        .peer
        .command(
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            1,
            TransactionCommand::Discharge(Discharge {
                txn_id: id(99),
                fail: Some(false),
            }),
        )
        .await;
    let disposition = fixture.peer.disposition(CONTROL_CHANNEL, 1).await;
    assert!(disposition.settled);
    assert!(
        matches!(disposition.state, Some(DeliveryState::Rejected(crate::Rejected { error: Some(error) })) if error.condition.as_symbol().as_str() == "amqp:transaction:unknown-id")
    );
    assert!(coordinator.controller_identity().is_active());
    fixture.peer.barrier(CONTROL_CHANNEL).await;
    let sealed = fixture
        .discharge(&mut coordinator, &transaction, false)
        .await;
    bounded(
        "valid group remains usable after negative outcome",
        sealed.wait_ready(),
    )
    .await
    .expect("ready");
    assert_eq!(
        fixture
            .committed(
                sealed
                    .prepare(Vec::new())
                    .expect("genuine empty valid group")
            )
            .await,
        0
    );
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn unknown_id_without_rejected_profile_refuses_only_coordinator_link() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let _healthy = fixture.session(HEALTHY_CHANNEL).await;
    fixture
        .peer
        .command(
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            0,
            TransactionCommand::Discharge(Discharge {
                txn_id: id(99),
                fail: Some(false),
            }),
        )
        .await;
    fixture
        .peer
        .refusal(
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            "amqp:transaction:unknown-id",
        )
        .await;
    assert!(matches!(
        bounded("no unknown-ID control authority", coordinator.recv()).await,
        Err(EngineError::RemoteDetached)
    ));
    assert!(!coordinator.controller_identity().is_active());
    fixture.peer.barrier(HEALTHY_CHANNEL).await;
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn decoder_or_closed_inbox_never_publishes_a_post_or_ready_empty_group() {
    for closed_inbox in [false, true] {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = fixture.coordinator_with(rejected_profile()).await;
        let (_data, receiver) = fixture.receiver().await;
        let _healthy = fixture.session(HEALTHY_CHANNEL).await;
        let mut receiver = Some(receiver);
        let transaction = id(48);
        fixture.declare(&mut coordinator, &transaction).await;
        if closed_inbox {
            drop(receiver.take());
        }
        fixture
            .peer
            .transfer(
                POST_CHANNEL,
                first(POST_HANDLE, 0, Some(transaction.clone()), false),
                if closed_inbox {
                    encode_message(&Message::data(vec![48; 127])).expect("valid message")
                } else {
                    vec![0x00]
                },
            )
            .await;
        fixture
            .peer
            .refusal(
                POST_CHANNEL,
                POST_HANDLE,
                if closed_inbox {
                    "amqp:resource-limit-exceeded"
                } else {
                    "amqp:invalid-field"
                },
            )
            .await;
        if let Some(receiver) = &mut receiver {
            assert!(matches!(
                bounded("decoder failure cannot publish posting", receiver.recv()).await,
                Err(EngineError::RemoteDetached)
            ));
        }
        fixture
            .peer
            .command(
                CONTROL_CHANNEL,
                CONTROL_HANDLE,
                1,
                TransactionCommand::Discharge(Discharge {
                    txn_id: transaction,
                    fail: Some(false),
                }),
            )
            .await;
        let disposition = fixture.peer.disposition(CONTROL_CHANNEL, 1).await;
        assert!(
            matches!(disposition.state, Some(DeliveryState::Rejected(crate::Rejected { error: Some(error) })) if error.condition.as_symbol().as_str() == "amqp:transaction:rollback")
        );
        assert!(coordinator.controller_identity().is_active());
        fixture.peer.barrier(HEALTHY_CHANNEL).await;
        fixture.connection.shutdown().await;
    }
}

#[tokio::test]
async fn aborted_partial_post_does_not_turn_into_a_committable_empty_group() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator_with(rejected_profile()).await;
    let (_data, mut receiver) = fixture.receiver().await;
    let transaction = id(49);
    fixture.declare(&mut coordinator, &transaction).await;
    let encoded = encode_message(&Message::data(vec![49; 127])).expect("message");
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            first(POST_HANDLE, 0, Some(transaction.clone()), true),
            encoded[..17].to_vec(),
        )
        .await;
    let mut aborted = continuation(POST_HANDLE, None, false);
    aborted.aborted = true;
    fixture
        .peer
        .transfer(POST_CHANNEL, aborted, Vec::new())
        .await;
    fixture.peer.barrier(POST_CHANNEL).await;
    fixture
        .peer
        .command(
            CONTROL_CHANNEL,
            CONTROL_HANDLE,
            1,
            TransactionCommand::Discharge(Discharge {
                txn_id: transaction,
                fail: Some(false),
            }),
        )
        .await;
    let disposition = fixture.peer.disposition(CONTROL_CHANNEL, 1).await;
    assert!(
        matches!(disposition.state, Some(DeliveryState::Rejected(crate::Rejected { error: Some(error) })) if error.condition.as_symbol().as_str() == "amqp:transaction:rollback")
    );
    let message = Message::data(b"ordinary after aborted post".to_vec());
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            first(POST_HANDLE, 0, None, false),
            encode_message(&message).expect("ordinary message"),
        )
        .await;
    let TransactionalIngress::Ordinary(receipt) =
        bounded("aborted alias releases ordinary capacity", receiver.recv())
            .await
            .expect("ordinary ingress")
    else {
        panic!("ordinary receipt")
    };
    assert_eq!(receipt.message(), &message);
    let (result, disposition) = tokio::join!(
        receiver.accept_retained(&receipt),
        fixture.peer.disposition(POST_CHANNEL, 0)
    );
    result.expect("ordinary settlement");
    assert!(matches!(
        disposition.state,
        Some(DeliveryState::Accepted(_))
    ));
    drop(receipt);
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn explicit_abort_has_control_receipt_but_cannot_mint_native_ready() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let transaction = id(50);
    fixture.declare(&mut coordinator, &transaction).await;
    let sealed = fixture
        .discharge(&mut coordinator, &transaction, true)
        .await;
    assert_eq!(sealed.state(), NativeTransactionState::Aborted);
    assert!(
        bounded("explicit abort has no ready authority", sealed.wait_ready())
            .await
            .is_err()
    );
    let (result, disposition) = tokio::join!(
        sealed.rollback(),
        fixture.peer.disposition(CONTROL_CHANNEL, 1)
    );
    result.expect("rollback control flush");
    assert!(disposition.settled);
    assert!(matches!(
        disposition.state,
        Some(DeliveryState::Accepted(_))
    ));
    fixture.peer.barrier(CONTROL_CHANNEL).await;
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn dropped_owner_claim_is_indeterminate_without_positive_control_outcome() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let _healthy = fixture.session(HEALTHY_CHANNEL).await;
    let transaction = id(51);
    fixture.declare(&mut coordinator, &transaction).await;
    let sealed = fixture
        .discharge(&mut coordinator, &transaction, false)
        .await;
    bounded("empty native readiness", sealed.wait_ready())
        .await
        .expect("ready");
    let (ticket, resources) = sealed
        .prepare(Vec::new())
        .expect("exact empty control bundle")
        .into_owner_parts();
    drop(ticket.try_claim().expect("native owner claim"));
    let (result, ()) = tokio::join!(
        resources.finish(),
        fixture
            .peer
            .refusal(CONTROL_CHANNEL, CONTROL_HANDLE, "amqp:internal-error")
    );
    assert!(result.is_err());
    assert!(!coordinator.controller_identity().is_active());
    fixture.peer.barrier(HEALTHY_CHANNEL).await;
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn abort_cleans_only_original_post_alias_without_consuming_held_payload() {
    for flushed in [false, true] {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = fixture.coordinator().await;
        let (_data, mut receiver) = fixture.receiver().await;
        let transaction = id(52);
        let observer = fixture.declare(&mut coordinator, &transaction).await;
        let message = Message::data(b"held aborted original".to_vec());
        fixture.post(&transaction, &message).await;
        let TransactionalIngress::Posting(posting) = bounded("original post", receiver.recv())
            .await
            .expect("posting")
        else {
            panic!("posting")
        };
        let (posting, prepared) = if flushed {
            (None, Some(fixture.provisional(posting, &transaction).await))
        } else {
            (Some(posting), None)
        };
        let abort = fixture
            .discharge(&mut coordinator, &transaction, true)
            .await;
        assert_eq!(abort.state(), NativeTransactionState::Aborted);
        let responses = async {
            let cleanup = fixture.peer.disposition(POST_CHANNEL, 0).await;
            assert!(cleanup.settled);
            assert!(
                cleanup.state.is_none(),
                "rollback cleanup cannot claim an applied message outcome"
            );
            let control = fixture.peer.disposition(CONTROL_CHANNEL, 1).await;
            assert!(control.settled);
            assert!(matches!(control.state, Some(DeliveryState::Accepted(_))));
        };
        let (result, ()) = tokio::join!(abort.rollback(), responses);
        result.expect("known abort transport cleanup");
        assert_eq!(observer.state(), NativeTransactionState::Aborted);
        if let Some(posting) = &posting {
            assert_eq!(posting.message(), &message);
        }
        if let Some(prepared) = &prepared {
            assert_eq!(prepared.message(), &message);
        }
        let replacement_message = Message::data(b"ordinary after original abort".to_vec());
        fixture
            .peer
            .transfer(
                POST_CHANNEL,
                first(POST_HANDLE, 0, None, false),
                encode_message(&replacement_message).expect("replacement message"),
            )
            .await;
        let TransactionalIngress::Ordinary(replacement) =
            bounded("aborted alias is reusable", receiver.recv())
                .await
                .expect("ordinary receipt")
        else {
            panic!("ordinary replacement")
        };
        assert_eq!(replacement.message(), &replacement_message);
        drop(posting);
        drop(prepared);
        assert_eq!(observer.state(), NativeTransactionState::Aborted);
        let (result, disposition) = tokio::join!(
            receiver.accept_retained(&replacement),
            fixture.peer.disposition(POST_CHANNEL, 0)
        );
        result.expect("fresh alias is independently accepted");
        assert!(matches!(
            disposition.state,
            Some(DeliveryState::Accepted(_))
        ));
        drop(replacement);
        fixture.connection.shutdown().await;
    }
}

#[tokio::test]
async fn known_owner_rejection_without_advertised_outcome_detaches_coordinator() {
    for aborted in [false, true] {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = fixture.coordinator().await;
        let _healthy = fixture.session(HEALTHY_CHANNEL).await;
        let transaction = id(53);
        let observer = fixture.declare(&mut coordinator, &transaction).await;
        let sealed = fixture
            .discharge(&mut coordinator, &transaction, false)
            .await;
        bounded("known empty group readiness", sealed.wait_ready())
            .await
            .expect("ready");
        let (ticket, resources) = sealed
            .prepare(Vec::new())
            .expect("exact native control bundle")
            .into_owner_parts();
        let claim = ticket.try_claim().expect("known native owner claim");
        if aborted {
            claim.abort();
        } else {
            claim.finish(NativeTransactionDecision::Rejected);
        }
        let expected = if aborted {
            NativeTransactionState::Aborted
        } else {
            NativeTransactionState::Rejected
        };
        assert_eq!(observer.state(), expected);
        let (result, ()) = tokio::join!(
            resources.finish(),
            fixture
                .peer
                .refusal(CONTROL_CHANNEL, CONTROL_HANDLE, "amqp:transaction:rollback")
        );
        result.expect("negotiated negative response is successfully flushed");
        assert_eq!(
            observer.state(),
            expected,
            "transport refusal cannot rewrite a known owner decision"
        );
        assert!(!coordinator.controller_identity().is_active());
        fixture.peer.barrier(HEALTHY_CHANNEL).await;
        assert!(fixture.connection.connection_identity().is_active());
        fixture.connection.shutdown().await;
    }
}
