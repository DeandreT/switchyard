use super::*;

#[tokio::test]
async fn strict_work_mixed_request_cannot_be_laundered_by_mutation() {
    let mut fixture = Fixture::new(Policy::Work).await;
    let mut session = fixture.session().await;
    let mut request = request(Role::Receiver);
    request.snd_settle_mode = SenderSettleMode::Mixed;
    let incoming = fixture.incoming(&mut session, request).await;
    assert!(
        bounded(session.accept_transactional_sender(incoming.clone(), 0))
            .await
            .is_err()
    );
    let mut changed = incoming.clone();
    changed.snd_settle_mode = SenderSettleMode::Unsettled;
    assert!(
        bounded(session.accept_transactional_sender(changed.clone(), 0))
            .await
            .is_err()
    );
    assert!(
        bounded(session.accept_transactional_sender_negotiating_unsettled(changed, 0))
            .await
            .is_err()
    );
    fixture.barrier().await;
    let (ordinary, response) =
        bounded(async { tokio::join!(session.accept_attach(incoming, 0), fixture.attached()) })
            .await;
    assert!(matches!(ordinary, Ok(LinkEndpoint::Sender(_))));
    assert_eq!(response.snd_settle_mode, SenderSettleMode::Mixed);
    assert_eq!(response.rcv_settle_mode, ReceiverSettleMode::Second);
    fixture.shutdown().await;
}

#[tokio::test]
async fn explicit_negotiation_sends_actual_unsettled_original_and_preserves_ordinary_settlement() {
    for policy in [Policy::Work, Policy::Defaults] {
        for mode in [SenderSettleMode::Mixed, SenderSettleMode::Unsettled] {
            let mut fixture = Fixture::new(policy).await;
            let mut session = fixture.session().await;
            let mut request = request(Role::Receiver);
            request.snd_settle_mode = mode.clone();
            let incoming = fixture.incoming(&mut session, request).await;
            assert_eq!(incoming.snd_settle_mode, mode);
            let (sender, response) = bounded(async {
                tokio::join!(
                    session.accept_transactional_sender_negotiating_unsettled(incoming, 0),
                    fixture.attached()
                )
            })
            .await;
            let mut sender = sender.expect("explicit negotiated sender");
            assert_eq!(response.role, Role::Sender);
            assert_eq!(response.snd_settle_mode, SenderSettleMode::Unsettled);
            assert_eq!(response.rcv_settle_mode, ReceiverSettleMode::Second);
            assert!(response.max_message_size.is_none());
            fixture.flow(Some(HANDLE), false).await;
            fixture.barrier().await;
            let message = Message {
                body: Body::Data(vec![b"negotiated-body".to_vec().into()]),
                ..Message::default()
            };
            let (sent, id) = bounded(async {
                tokio::join!(
                    sender.send_with_dispositions(
                        message.clone(),
                        b"negotiated-original".to_vec().into()
                    ),
                    fixture.outgoing(response.handle, &message)
                )
            })
            .await;
            let mut sent = sent.expect("fully flushed original");
            fixture
                .disposition(id, DeliveryState::Accepted(Accepted), false)
                .await;
            let TransactionalDisposition::Ordinary(pending) = bounded(sent.next_disposition())
                .await
                .expect("actual ordinary disposition")
            else {
                panic!("ordinary outcome remains ordinary");
            };
            assert!(pending.belongs_to_sender(&sender.sender_identity()));
            assert!(
                pending
                    .delivery_identity()
                    .same_delivery(sent.delivery_identity())
            );
            let (accepted, (channel, frame)) =
                bounded(async { tokio::join!(pending.accept(), fixture.control()) }).await;
            accepted.expect("exact native ordinary acknowledgement");
            assert_eq!(channel, fixture.local());
            let Performative::Disposition(ack) = frame else {
                panic!("ordinary sender ACK");
            };
            assert_eq!(ack.role, Role::Sender);
            assert_eq!(ack.first, id);
            assert!(ack.settled);
            assert!(matches!(ack.state, Some(DeliveryState::Accepted(_))));
            fixture.shutdown().await;
        }
    }
}

#[tokio::test]
async fn negotiating_method_keeps_connection_policy_and_settlement_guards() {
    for (policy, sender_mode, receiver_mode) in [
        (
            Policy::Disabled,
            SenderSettleMode::Mixed,
            ReceiverSettleMode::Second,
        ),
        (
            Policy::Posting,
            SenderSettleMode::Mixed,
            ReceiverSettleMode::Second,
        ),
        (
            Policy::Defaults,
            SenderSettleMode::Settled,
            ReceiverSettleMode::Second,
        ),
        (
            Policy::Defaults,
            SenderSettleMode::Mixed,
            ReceiverSettleMode::First,
        ),
    ] {
        let mut fixture = Fixture::new(policy).await;
        let mut session = fixture.session().await;
        let mut request = request(Role::Receiver);
        request.snd_settle_mode = sender_mode.clone();
        request.rcv_settle_mode = receiver_mode.clone();
        let incoming = fixture.incoming(&mut session, request).await;
        assert!(
            bounded(session.accept_transactional_sender_negotiating_unsettled(incoming.clone(), 0))
                .await
                .is_err()
        );
        fixture.barrier().await;
        let (ordinary, response) =
            bounded(async { tokio::join!(session.accept_attach(incoming, 0), fixture.attached()) })
                .await;
        assert!(matches!(ordinary, Ok(LinkEndpoint::Sender(_))));
        assert_eq!(response.snd_settle_mode, sender_mode);
        assert_eq!(response.rcv_settle_mode, receiver_mode);
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn negotiation_rejects_changed_modes_and_transactional_source_defaults_without_wire() {
    for variant in 0..4 {
        let mut fixture = Fixture::new(Policy::Defaults).await;
        let mut session = fixture.session().await;
        let mut request = request(Role::Receiver);
        request.snd_settle_mode = if variant == 0 {
            SenderSettleMode::Settled
        } else {
            SenderSettleMode::Mixed
        };
        request.rcv_settle_mode = if variant == 1 {
            ReceiverSettleMode::First
        } else {
            ReceiverSettleMode::Second
        };
        let incoming = fixture.incoming(&mut session, request).await;
        let mut changed = incoming.clone();
        match variant {
            0 => changed.snd_settle_mode = SenderSettleMode::Mixed,
            1 => changed.rcv_settle_mode = ReceiverSettleMode::Second,
            2 => changed.snd_settle_mode = SenderSettleMode::Unsettled,
            _ => {
                changed.source.as_mut().expect("source").default_outcome =
                    Some(DeliveryState::Declared(crate::Declared {
                        txn_id: TransactionId::new([1]).expect("bounded ID"),
                    }))
            }
        }
        assert!(
            bounded(session.accept_transactional_sender_negotiating_unsettled(changed, 0))
                .await
                .is_err()
        );
        fixture.barrier().await;
        let (ordinary, _) =
            bounded(async { tokio::join!(session.accept_attach(incoming, 0), fixture.attached()) })
                .await;
        assert!(matches!(ordinary, Ok(LinkEndpoint::Sender(_))));
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn retired_negotiating_approval_cannot_alias_a_recreated_same_name_and_handle() {
    let mut fixture = Fixture::new(Policy::Defaults).await;
    let mut session = fixture.session().await;
    let mut request = request(Role::Receiver);
    request.snd_settle_mode = SenderSettleMode::Mixed;
    let incoming = fixture.incoming(&mut session, request.clone()).await;
    let stale = incoming.clone();
    let (sender, response) = bounded(async {
        tokio::join!(
            session.accept_transactional_sender_negotiating_unsettled(incoming, 0),
            fixture.attached()
        )
    })
    .await;
    let mut sender = sender.expect("original sender");
    fixture
        .send(
            Performative::Detach(Detach {
                handle: HANDLE,
                closed: true,
                error: None,
            }),
            Vec::new(),
        )
        .await;
    let (channel, frame) = fixture.control().await;
    assert_eq!(channel, fixture.local());
    assert!(
        matches!(frame, Performative::Detach(Detach { handle, closed: true, error: None }) if handle == response.handle)
    );
    bounded(sender.on_detach()).await;
    fixture.barrier().await;
    let fresh = fixture.incoming(&mut session, request).await;
    assert!(
        !stale
            .approval()
            .link_identity()
            .same_link(fresh.approval().link_identity())
    );
    assert!(matches!(
        bounded(session.accept_transactional_sender_negotiating_unsettled(stale, 0)).await,
        Err(EngineError::RemoteDetached)
    ));
    fixture.barrier().await;
    let (fresh_sender, response) = bounded(async {
        tokio::join!(
            session.accept_transactional_sender_negotiating_unsettled(fresh, 0),
            fixture.attached()
        )
    })
    .await;
    assert!(
        fresh_sender
            .expect("replacement sender")
            .sender_identity()
            .is_active()
    );
    assert_eq!(response.snd_settle_mode, SenderSettleMode::Unsettled);
    fixture.shutdown().await;
}
