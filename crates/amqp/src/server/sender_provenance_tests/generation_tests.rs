use super::*;
use crate::Body;

#[path = "generation_fixture.rs"]
mod generation_fixture;

use generation_fixture::*;

fn assert_delivery_origin(
    receipt: &PendingSettlement,
    sender: &Sender,
    connection: &NativeConnectionIdentity,
) {
    let identity = receipt.delivery_identity();
    assert!(identity.same_delivery(&identity.clone()));
    assert!(identity.belongs_to_sender(&sender.sender_identity()));
    assert!(
        identity
            .connection_identity()
            .expect("actor-bound original delivery")
            .same_connection(connection)
    );
    assert_eq!(
        format!("{identity:?}"),
        "NativeOutgoingDeliveryIdentity { .. }"
    );
}

#[tokio::test]
async fn original_delivery_is_returned_for_all_native_settlement_modes() {
    for (mode, remote_settled, source_settled) in [
        (ReceiverSettleMode::First, true, false),
        (ReceiverSettleMode::Second, false, false),
        (ReceiverSettleMode::Second, true, false),
        (ReceiverSettleMode::First, true, true),
    ] {
        let mut fixture = Fixture::new().await;
        let mut session = fixture.session(CHANNEL).await;
        let incoming = fixture
            .incoming_with_mode(
                &mut session,
                CHANNEL,
                HANDLE,
                "same-link",
                mode,
                if source_settled {
                    SenderSettleMode::Settled
                } else {
                    SenderSettleMode::Unsettled
                },
            )
            .await;
        let mut sender = fixture
            .accept(&mut session, incoming, CHANNEL, HANDLE)
            .await;
        let (receipt, id) = if source_settled {
            let handle = sender.handle;
            let (receipt, transfer) = bounded("source-settled original delivery", async {
                tokio::join!(
                    sender.send_with_settlement(message(), TAG.to_vec().into()),
                    fixture.peer.transfer_with_settlement(CHANNEL, handle, true)
                )
            })
            .await;
            (
                receipt.expect("source-settled flushed result"),
                transfer.delivery_id.expect("original transfer id"),
            )
        } else {
            fixture.receipt(&mut sender, CHANNEL, remote_settled).await
        };
        assert_eq!(receipt.outcome(), &Outcome::Accepted(Accepted));
        assert_eq!(id, 0);
        assert_delivery_origin(&receipt, &sender, fixture.connection.connection_identity());
        let original = receipt.delivery_identity().clone();
        if let Some(ack) = &receipt.acknowledgement {
            assert!(ack.delivery_identity().same_delivery(&original));
            fixture.acknowledge(&receipt, CHANNEL, id).await;
            assert!(ack.delivery_identity().same_delivery(&original));
        } else {
            bounded("no-ack original result", receipt.accept())
                .await
                .expect("no-ack completion");
            fixture.peer.barrier(CHANNEL).await;
        }
        assert!(original.same_delivery(receipt.delivery_identity()));
        fixture.shutdown().await;
        assert!(!original.belongs_to_sender(&sender.sender_identity()));
        assert!(original.same_delivery(receipt.delivery_identity()));
    }
}

#[tokio::test]
async fn successive_same_tag_sends_and_matched_foreign_aliases_have_distinct_originals() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let mut sender = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::First,
        )
        .await;
    let (first, first_id) = fixture.receipt(&mut sender, CHANNEL, true).await;
    let (second, second_id) = fixture.receipt(&mut sender, CHANNEL, true).await;
    assert_eq!((first_id, second_id), (0, 1));
    assert!(
        !first
            .delivery_identity()
            .same_delivery(second.delivery_identity())
    );
    assert!(
        first
            .delivery_identity()
            .belongs_to_sender(&sender.sender_identity())
    );
    assert!(
        second
            .delivery_identity()
            .belongs_to_sender(&sender.sender_identity())
    );

    let mut foreign_fixture = Fixture::new().await;
    let mut foreign_session = foreign_fixture.session(CHANNEL).await;
    let mut foreign_sender = foreign_fixture
        .sender(
            &mut foreign_session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::First,
        )
        .await;
    let (foreign, foreign_id) = foreign_fixture
        .receipt(&mut foreign_sender, CHANNEL, true)
        .await;
    assert_eq!(
        (sender.channel, sender.handle, first_id),
        (foreign_sender.channel, foreign_sender.handle, foreign_id)
    );
    assert!(
        !first
            .delivery_identity()
            .same_delivery(foreign.delivery_identity())
    );
    assert!(
        !first
            .delivery_identity()
            .belongs_to_sender(&foreign_sender.sender_identity())
    );
    assert!(
        !foreign
            .delivery_identity()
            .belongs_to_sender(&sender.sender_identity())
    );
    fixture.shutdown().await;
    foreign_fixture.shutdown().await;
}

#[tokio::test]
async fn historical_delivery_identity_survives_link_and_session_alias_replacement() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let mut sender = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::First,
        )
        .await;
    let (receipt, id) = fixture.receipt(&mut sender, CHANNEL, true).await;
    let original = receipt.delivery_identity().clone();
    let clone = original.clone();
    let route = (sender.channel, sender.handle, id);
    drop(receipt);
    fixture.close_link(&sender, CHANNEL, HANDLE).await;
    let mut replacement = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::First,
        )
        .await;
    assert_eq!(
        (sender.channel, sender.handle),
        (replacement.channel, replacement.handle)
    );
    assert!(!original.belongs_to_sender(&sender.sender_identity()));
    assert!(!original.belongs_to_sender(&replacement.sender_identity()));
    let (replacement_receipt, _) = fixture.receipt(&mut replacement, CHANNEL, true).await;
    assert!(!original.same_delivery(replacement_receipt.delivery_identity()));
    assert!(original.same_delivery(&clone));
    fixture.peer.end(CHANNEL).await;
    let mut new_session = fixture.session(CHANNEL).await;
    let mut new_sender = fixture
        .sender(
            &mut new_session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::First,
        )
        .await;
    let (new_receipt, new_id) = fixture.receipt(&mut new_sender, CHANNEL, true).await;
    assert_eq!(route, (new_sender.channel, new_sender.handle, new_id));
    assert!(!original.same_delivery(new_receipt.delivery_identity()));
    assert!(!original.belongs_to_sender(&new_sender.sender_identity()));
    assert!(original.same_delivery(&clone));
    assert_delivery_origin(
        &new_receipt,
        &new_sender,
        fixture.connection.connection_identity(),
    );
    fixture.shutdown().await;
    assert!(original.same_delivery(&clone));
}

#[tokio::test]
async fn acknowledgement_keeps_original_generation_after_pending_wrapper_drop() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let mut sender = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::Second,
        )
        .await;
    let (receipt, id) = fixture.receipt(&mut sender, CHANNEL, false).await;
    let original = receipt.delivery_identity().clone();
    let acknowledgement = receipt
        .acknowledgement
        .as_ref()
        .expect("pending second-mode ACK")
        .clone();
    drop(receipt);
    assert!(acknowledgement.delivery_identity().same_delivery(&original));
    assert!(!acknowledgement.is_settled());
    let (reply, result) = oneshot::channel();
    bounded(
        "enqueue exact original ACK",
        sender.commands.send(Command::SettleOutgoing {
            channel: sender.channel,
            handle: sender.handle,
            owner: sender.identity.clone(),
            identity: Some(acknowledgement.clone()),
            state: DeliveryState::Accepted(Accepted),
            reply,
        }),
    )
    .await
    .expect("live native actor");
    let (result, (channel, frame)) = bounded("exact ACK after wrapper drop", async {
        tokio::join!(result, fixture.peer.control())
    })
    .await;
    result.expect("ACK reply").expect("original ACK flushed");
    assert_eq!(channel, sender.channel);
    let Performative::Disposition(disposition) = frame else {
        panic!("exact original Sender ACK");
    };
    assert_eq!(disposition.role, Role::Sender);
    assert_eq!(disposition.first, id);
    assert!(disposition.last.is_none());
    assert!(disposition.settled);
    assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
    assert!(acknowledgement.is_settled());
    assert!(acknowledgement.delivery_identity().same_delivery(&original));
    fixture.peer.barrier(CHANNEL).await;
    fixture.shutdown().await;
    assert!(acknowledgement.delivery_identity().same_delivery(&original));
}

#[tokio::test]
async fn historical_observer_clones_do_not_retain_sender_commands_or_connection_activity() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let mut sender = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::First,
        )
        .await;
    let (receipt, _) = fixture.receipt(&mut sender, CHANNEL, true).await;
    let original = receipt.delivery_identity().clone();
    let observer = sender.sender_identity();
    let command_owners = fixture.connection.commands.strong_count();
    let clones: Vec<_> = (0..128).map(|_| original.clone()).collect();
    assert_eq!(fixture.connection.commands.strong_count(), command_owners);
    drop(receipt);
    drop(sender);
    drop(session);
    fixture.shutdown().await;
    assert!(
        !original
            .connection_identity()
            .expect("historical connection")
            .is_active()
    );
    assert!(!original.belongs_to_sender(&observer));
    assert!(clones.iter().all(|clone| original.same_delivery(clone)));
    assert_eq!(
        format!("{original:?}"),
        "NativeOutgoingDeliveryIdentity { .. }"
    );
}

#[tokio::test]
async fn production_helpers_mint_distinct_originals_for_same_link_id_and_tag_reuse() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let sender = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::Second,
        )
        .await;
    let mut helper = ActorHarness::from_sender(&sender);
    let result = helper.enqueue(message()).await;
    assert!(helper.retained_bytes() > 0);
    helper.fragment().await;
    assert_eq!(helper.retained_bytes(), 0);
    helper.outcome(false).await;
    let first = result
        .await
        .expect("first helper reply")
        .expect("first original");
    let first_ack = first.acknowledgement.expect("first pending ACK");
    assert!(
        first_ack
            .delivery_identity()
            .same_delivery(&first.delivery_identity)
    );
    helper.settle(first_ack.clone()).await;
    helper.reuse_id();
    helper.output.clear();

    let result = helper.enqueue(message()).await;
    helper.fragment().await;
    helper.outcome(false).await;
    let second = result
        .await
        .expect("second helper reply")
        .expect("second original");
    let second_ack = second.acknowledgement.expect("second pending ACK");
    assert_eq!(
        (first_ack.id(), first_ack.tag()),
        (second_ack.id(), second_ack.tag())
    );
    assert!(
        !first
            .delivery_identity
            .same_delivery(&second.delivery_identity)
    );
    assert!(!first_ack.same_ack(&second_ack));
    assert!(
        first
            .delivery_identity
            .belongs_to_sender(&sender.sender_identity())
    );
    assert!(
        second
            .delivery_identity
            .belongs_to_sender(&sender.sender_identity())
    );
    helper.output.clear();
    helper.settle(first_ack).await;
    assert!(helper.output.frames().await.is_empty());
    assert!(!second_ack.is_settled());
    helper.settle(second_ack).await;
    assert_eq!(helper.retained_bytes(), 0);
    fixture.shutdown().await;
    assert!(
        !first
            .delivery_identity
            .belongs_to_sender(&sender.sender_identity())
    );
}

#[tokio::test]
async fn early_fragmented_outcome_returns_original_only_after_final_transfer_flush() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let sender = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::Second,
        )
        .await;
    let mut helper = ActorHarness::from_sender(&sender);
    let message = Message {
        body: Body::Data(vec![vec![42; 2_000].into()]),
        ..Message::default()
    };
    let encoded_bytes = encode_message(&message)
        .expect("fragmented message encoding")
        .len();
    let mut result = helper.enqueue(message).await;
    assert_eq!(helper.retained_bytes(), encoded_bytes);
    helper.fragment().await;
    let original = helper
        .link()
        .active
        .as_ref()
        .expect("active first fragment")
        .delivery_identity
        .clone();
    assert!(original.same_delivery(&helper.link().unsettled[&REUSED_ID].delivery_identity));
    helper.outcome(false).await;
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    for _ in 0..16 {
        if helper.next_fragment_is_last() {
            break;
        }
        helper.fragment().await;
    }
    assert!(helper.next_fragment_is_last());
    helper.output.clear();
    helper.output.block_final_transfer();
    let output = Arc::clone(&helper.output);
    let budget = helper.writer.content_budget().clone();
    let finishing = send_fragment(
        ACTOR_CHANNEL,
        ACTOR_HANDLE,
        helper
            .sessions
            .get_mut(&ACTOR_CHANNEL)
            .expect("helper session"),
        &mut helper.writer,
    );
    tokio::pin!(finishing);
    assert_pending(finishing.as_mut()).await;
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(budget.retained_bytes(), encoded_bytes);
    output.release();
    bounded("final Transfer flush released", finishing.as_mut())
        .await
        .expect("final Transfer flushed");
    let completed = result
        .await
        .expect("completed send result")
        .expect("early outcome after final flush");
    assert!(original.same_delivery(&completed.delivery_identity));
    assert!(
        completed
            .acknowledgement
            .as_ref()
            .expect("original pending ACK")
            .delivery_identity()
            .same_delivery(&original)
    );
    assert_eq!(budget.retained_bytes(), 0);
    drop(completed);
    assert_eq!(budget.retained_bytes(), 0);
    assert!(original.belongs_to_sender(&sender.sender_identity()));
    fixture.shutdown().await;
}

#[tokio::test]
async fn canceled_send_reply_does_not_cancel_native_transfer_or_hold_encoded_content() {
    for drop_before_transfer in [true, false] {
        let mut fixture = Fixture::new().await;
        let mut session = fixture.session(CHANNEL).await;
        let sender = fixture
            .sender(
                &mut session,
                CHANNEL,
                HANDLE,
                "same-link",
                ReceiverSettleMode::Second,
            )
            .await;
        let mut helper = ActorHarness::from_sender(&sender);
        let mut result = Some(helper.enqueue(message()).await);
        if drop_before_transfer {
            drop(result.take());
        }
        assert!(helper.retained_bytes() > 0);
        helper.fragment().await;
        let original = helper.link().unsettled[&REUSED_ID]
            .delivery_identity
            .clone();
        let clones: Vec<_> = (0..128).map(|_| original.clone()).collect();
        assert_eq!(helper.retained_bytes(), 0);
        helper.outcome(false).await;
        drop(result.take());
        assert!(helper.link().unsettled.is_empty());
        let acknowledgement = helper.link().pending_acknowledgements[&REUSED_ID].clone();
        assert!(acknowledgement.delivery_identity().same_delivery(&original));
        assert!(!acknowledgement.is_settled());
        let frames = helper.output.frames().await;
        assert_eq!(frames.len(), 1);
        assert!(matches!(
            &frames[0],
            Frame::Amqp {
                performative: Some(Performative::Transfer(Transfer {
                    delivery_id: Some(REUSED_ID),
                    ..
                })),
                ..
            }
        ));
        helper.settle(acknowledgement).await;
        assert_eq!(helper.retained_bytes(), 0);
        assert!(clones.iter().all(|clone| original.same_delivery(clone)));
        fixture.shutdown().await;
        assert!(!original.belongs_to_sender(&sender.sender_identity()));
    }
}
