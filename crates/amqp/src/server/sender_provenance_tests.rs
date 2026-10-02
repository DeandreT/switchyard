use super::*;

#[path = "sender_provenance_tests/fixture.rs"]
mod fixture;

use fixture::*;

fn assert_origin(
    receipt: &PendingSettlement,
    sender: &NativeSenderIdentity,
    connection: &NativeConnectionIdentity,
) {
    assert!(sender.is_active());
    assert!(receipt.belongs_to_sender(sender));
    assert!(
        sender
            .connection_identity()
            .expect("actor-bound sender")
            .same_connection(connection)
    );
}

#[tokio::test]
async fn first_second_and_remote_settled_outcomes_preserve_exact_link_origin() {
    for (mode, settled, needs_ack) in [
        (ReceiverSettleMode::First, true, false),
        (ReceiverSettleMode::Second, false, true),
        (ReceiverSettleMode::Second, true, false),
    ] {
        let mut fixture = Fixture::new().await;
        let connection = fixture.connection.connection_identity().clone();
        let mut session = fixture.session(CHANNEL).await;
        let mut sender = fixture
            .sender(&mut session, CHANNEL, HANDLE, "same-link", mode)
            .await;
        let observer = sender.sender_identity();
        let clone = observer.clone();
        assert!(observer.same_sender(&clone));
        let (receipt, id) = fixture.receipt(&mut sender, CHANNEL, settled).await;
        assert_eq!(id, 0);
        assert_origin(&receipt, &observer, &connection);
        assert_eq!(receipt.acknowledgement.is_some(), needs_ack);
        if needs_ack {
            assert!(
                !receipt
                    .acknowledgement
                    .as_ref()
                    .expect("second-mode identity")
                    .is_settled()
            );
            fixture.acknowledge(&receipt, CHANNEL, id).await;
            assert!(
                receipt
                    .acknowledgement
                    .as_ref()
                    .expect("terminal identity retained")
                    .is_settled()
            );
        } else {
            bounded("bounded no-ack completion", receipt.accept())
                .await
                .expect("ordinary no-ack result");
            fixture.peer.barrier(CHANNEL).await;
        }
        assert_origin(&receipt, &observer, &connection);
        assert_eq!(
            format!("{observer:?}"),
            "NativeSenderIdentity { active: true, .. }"
        );
        drop(clone);
        assert!(observer.is_active());
        fixture.shutdown().await;
        assert!(!observer.is_active());
        assert!(!receipt.belongs_to_sender(&observer));
        assert!(observer.same_sender(&observer.clone()));
        assert_eq!(
            format!("{observer:?}"),
            "NativeSenderIdentity { active: false, .. }"
        );
    }
}

#[tokio::test]
async fn other_links_sessions_and_matched_foreign_aliases_are_not_the_same_sender() {
    let mut first = Fixture::new().await;
    let mut session = first.session(CHANNEL).await;
    let mut sender = first
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::First,
        )
        .await;
    let other_link = first
        .sender(
            &mut session,
            CHANNEL,
            OTHER_HANDLE,
            "other-link",
            ReceiverSettleMode::First,
        )
        .await;
    let mut other_session = first.session(OTHER_CHANNEL).await;
    let session_sender = first
        .sender(
            &mut other_session,
            OTHER_CHANNEL,
            HANDLE,
            "other-session-link",
            ReceiverSettleMode::First,
        )
        .await;
    let observer = sender.sender_identity();
    let (receipt, id) = first.receipt(&mut sender, CHANNEL, true).await;
    for foreign in [
        other_link.sender_identity(),
        session_sender.sender_identity(),
    ] {
        assert!(!observer.same_sender(&foreign));
        assert!(!receipt.belongs_to_sender(&foreign));
        assert!(
            observer
                .connection_identity()
                .expect("origin connection")
                .same_connection(foreign.connection_identity().expect("same connection"))
        );
    }

    let mut second = Fixture::new().await;
    let mut matched_session = second.session(CHANNEL).await;
    let mut matched_sender = second
        .sender(
            &mut matched_session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::First,
        )
        .await;
    let matched_observer = matched_sender.sender_identity();
    let (matched_receipt, matched_id) = second.receipt(&mut matched_sender, CHANNEL, true).await;
    assert_eq!(
        (sender.channel, sender.handle, sender.name(), id),
        (
            matched_sender.channel,
            matched_sender.handle,
            matched_sender.name(),
            matched_id
        )
    );
    assert!(!observer.same_sender(&matched_observer));
    assert!(
        !observer
            .connection_identity()
            .expect("origin connection")
            .same_connection(
                matched_observer
                    .connection_identity()
                    .expect("foreign connection")
            )
    );
    assert!(!receipt.belongs_to_sender(&matched_observer));
    assert!(!matched_receipt.belongs_to_sender(&observer));
    assert_origin(&receipt, &observer, first.connection.connection_identity());
    assert_origin(
        &matched_receipt,
        &matched_observer,
        second.connection.connection_identity(),
    );
    bounded("first ordinary completion", receipt.accept())
        .await
        .expect("first receipt");
    bounded("foreign ordinary completion", matched_receipt.accept())
        .await
        .expect("foreign receipt");
    first.peer.barrier(CHANNEL).await;
    second.peer.barrier(CHANNEL).await;
    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn terminal_origin_proof_does_not_acknowledge_a_later_delivery() {
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
    let observer = sender.sender_identity();
    let (old, old_id) = fixture.receipt(&mut sender, CHANNEL, false).await;
    fixture.acknowledge(&old, CHANNEL, old_id).await;
    let (current, current_id) = fixture.receipt(&mut sender, CHANNEL, false).await;
    assert_eq!((old_id, current_id), (0, 1));
    assert!(old.belongs_to_sender(&observer));
    assert!(current.belongs_to_sender(&observer));
    assert!(
        !old.acknowledgement
            .as_ref()
            .expect("old ack")
            .same_ack(current.acknowledgement.as_ref().expect("current ack"))
    );

    bounded("bounded terminal receipt retry", old.accept())
        .await
        .expect("idempotent old acknowledgement");
    fixture.peer.barrier(CHANNEL).await;
    assert!(
        !current
            .acknowledgement
            .as_ref()
            .expect("current exact ack")
            .is_settled()
    );
    assert!(old.belongs_to_sender(&observer));
    fixture.acknowledge(&current, CHANNEL, current_id).await;
    assert!(current.belongs_to_sender(&observer));
    fixture.shutdown().await;
}

#[tokio::test]
async fn reaccepted_handle_and_name_have_a_distinct_sender_origin() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let mut old_sender = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::First,
        )
        .await;
    let old_observer = old_sender.sender_identity();
    let clone = old_observer.clone();
    let (old_receipt, _) = fixture.receipt(&mut old_sender, CHANNEL, true).await;
    fixture.close_link(&old_sender, CHANNEL, HANDLE).await;
    assert!(!old_observer.is_active());
    assert!(old_observer.same_sender(&clone));
    assert!(!old_receipt.belongs_to_sender(&old_observer));

    let mut replacement = fixture
        .sender(
            &mut session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::First,
        )
        .await;
    let observer = replacement.sender_identity();
    assert_eq!(
        (old_sender.channel, old_sender.handle, old_sender.name()),
        (replacement.channel, replacement.handle, replacement.name())
    );
    assert!(!old_observer.same_sender(&observer));
    assert!(!old_receipt.belongs_to_sender(&observer));
    assert!(matches!(
        bounded("stale link completion", old_receipt.accept()).await,
        Err(EngineError::RemoteDetached)
    ));
    fixture.peer.barrier(CHANNEL).await;
    let (receipt, _) = fixture.receipt(&mut replacement, CHANNEL, true).await;
    assert_origin(
        &receipt,
        &observer,
        fixture.connection.connection_identity(),
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn reused_session_channel_and_delivery_id_do_not_reuse_sender_origin() {
    let mut fixture = Fixture::new().await;
    let mut old_session = fixture.session(CHANNEL).await;
    let mut old_sender = fixture
        .sender(
            &mut old_session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::First,
        )
        .await;
    let old_observer = old_sender.sender_identity();
    let (old_receipt, old_id) = fixture.receipt(&mut old_sender, CHANNEL, true).await;
    fixture.peer.end(CHANNEL).await;
    assert!(!old_observer.is_active());
    let mut replacement_session = fixture.session(CHANNEL).await;
    let mut replacement = fixture
        .sender(
            &mut replacement_session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::First,
        )
        .await;
    let observer = replacement.sender_identity();
    let (receipt, id) = fixture.receipt(&mut replacement, CHANNEL, true).await;
    assert_eq!(
        (old_sender.channel, old_sender.handle, old_id),
        (replacement.channel, replacement.handle, id)
    );
    assert!(!old_observer.same_sender(&observer));
    assert!(!old_receipt.belongs_to_sender(&observer));
    assert!(matches!(
        bounded("stale session completion", old_receipt.accept()).await,
        Err(EngineError::RemoteDetached)
    ));
    fixture.peer.barrier(CHANNEL).await;
    assert_origin(
        &receipt,
        &observer,
        fixture.connection.connection_identity(),
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn observer_clones_and_receipts_do_not_keep_a_closed_connection_active() {
    for peer_eof in [false, true] {
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
        let observer = sender.sender_identity();
        let clone = observer.clone();
        let (receipt, _) = fixture.receipt(&mut sender, CHANNEL, true).await;
        drop(observer.clone());
        assert!(observer.is_active());
        if peer_eof {
            bounded("peer EOF write", fixture.peer.io.shutdown())
                .await
                .expect("peer write half closed");
            bounded(
                "actor EOF termination fence",
                fixture.connection.lifecycle.wait_terminated(),
            )
            .await;
        } else {
            fixture.close().await;
        }
        assert!(!observer.is_active());
        assert!(!clone.is_active());
        assert!(observer.same_sender(&clone));
        assert!(
            !observer
                .connection_identity()
                .expect("retained connection identity")
                .is_active()
        );
        assert!(!receipt.belongs_to_sender(&observer));
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn mutated_actor_approved_role_cannot_mint_a_sender_origin() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(CHANNEL).await;
    let incoming = fixture
        .incoming(
            &mut session,
            CHANNEL,
            HANDLE,
            "same-link",
            ReceiverSettleMode::First,
        )
        .await;
    let mut changed = incoming.clone();
    changed.attach_mut().role = Role::Sender;
    changed.attach_mut().initial_delivery_count = Some(0);
    assert!(
        bounded(
            "mutated role refused before approval",
            session.accept_attach(changed, 0)
        )
        .await
        .is_err()
    );
    fixture.peer.barrier(CHANNEL).await;
    let mut sender = fixture
        .accept(&mut session, incoming, CHANNEL, HANDLE)
        .await;
    let observer = sender.sender_identity();
    let (receipt, _) = fixture.receipt(&mut sender, CHANNEL, true).await;
    assert_origin(
        &receipt,
        &observer,
        fixture.connection.connection_identity(),
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn ordinary_receiver_transactional_disposition_still_ends_only_its_session() {
    for native_posting_enabled in [false, true] {
        let mut fixture = Fixture::with_native_transactions(native_posting_enabled).await;
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
        let observer = sender.sender_identity();
        let local_handle = sender.handle;
        let offending = async {
            let transfer = fixture.peer.transfer(CHANNEL, local_handle).await;
            fixture
                .peer
                .outcome(
                    CHANNEL,
                    transfer.delivery_id.expect("delivery id"),
                    false,
                    DeliveryState::Transactional(crate::TransactionalState {
                        txn_id: crate::TransactionId::new(b"unissued").expect("bounded id"),
                        outcome: Some(Outcome::Accepted(Accepted)),
                    }),
                )
                .await;
            let (channel, response) = fixture.peer.control().await;
            assert_eq!(channel, session.channel);
            let Performative::End(end) = response else {
                panic!("transaction disposition session refusal");
            };
            assert_eq!(
                end.error
                    .expect("explicit unsupported transaction")
                    .condition
                    .as_symbol()
                    .as_str(),
                "amqp:not-implemented"
            );
            fixture
                .peer
                .send(CHANNEL, Performative::End(End { error: None }))
                .await;
        };
        let (result, ()) = bounded("bounded transactional disposition refusal", async {
            tokio::join!(
                sender.send_with_settlement(message(), TAG.to_vec().into()),
                offending
            )
        })
        .await;
        assert!(matches!(result, Err(EngineError::RemoteDetached)));
        assert!(!observer.is_active());
        assert!(fixture.connection.connection_identity().is_active());
        fixture.peer.forget_session(CHANNEL);
        let mut healthy_session = fixture.session(OTHER_CHANNEL).await;
        let mut healthy = fixture
            .sender(
                &mut healthy_session,
                OTHER_CHANNEL,
                HANDLE,
                "healthy-link",
                ReceiverSettleMode::First,
            )
            .await;
        let (receipt, _) = fixture.receipt(&mut healthy, OTHER_CHANNEL, true).await;
        assert_origin(
            &receipt,
            &healthy.sender_identity(),
            fixture.connection.connection_identity(),
        );
        fixture.shutdown().await;
    }
}

#[test]
fn unbound_private_sender_and_receipt_fail_closed_without_an_active_connection() {
    let identity = LinkIdentity::new();
    let (commands, _incoming) = mpsc::channel(1);
    let (_detached, detached) = watch::channel(false);
    let sender = Sender {
        name: String::from("same-link"),
        max_message_size: None,
        channel: 0,
        handle: 0,
        commands: commands.clone(),
        detached,
        identity: identity.clone(),
    };
    let receipt = PendingSettlement {
        outcome: Outcome::Accepted(Accepted),
        identity,
        acknowledgement: None,
        channel: 0,
        handle: 0,
        commands,
    };
    let observer = sender.sender_identity();
    assert!(observer.same_sender(&observer.clone()));
    assert!(observer.connection_identity().is_none());
    assert!(!observer.is_active());
    assert!(!receipt.belongs_to_sender(&observer));
}
