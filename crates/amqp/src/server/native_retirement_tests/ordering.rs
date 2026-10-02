use super::*;

#[tokio::test]
async fn early_outgoing_partial_retirement_refuses_without_inbound_partial_at_seal_mutation() {
    let mut fixture = Fixture::with_policy(Policy::Work, 512).await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    let transaction = txn(14);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    fixture.peer.one_frame_window(SEND).await;
    let message = Message {
        body: Body::Data(vec![vec![42; 2_000].into()]),
        ..Message::default()
    };
    let sending = sender.send_with_dispositions(message, TAG.to_vec().into());
    tokio::pin!(sending);
    let frame = bounded("first partial outgoing Transfer", async {
        tokio::select! {
            result = sending.as_mut() => panic!("sent handle before complete outgoing message: {}", result.is_ok()),
            frame = async {
                for _ in 0..16 {
                    let frame = fixture.peer.frame().await;
                    if !fixture.peer.valid_flow(&frame) {
                        return frame;
                    }
                }
                panic!("bounded partial Transfer frame count");
            } => frame,
        }
    }).await;
    let Frame::Amqp {
        channel,
        performative: Some(Performative::Transfer(transfer)),
        ..
    } = frame
    else {
        panic!("first partial Transfer");
    };
    assert_eq!(channel, fixture.peer.local(SEND));
    assert_eq!(transfer.handle, handle);
    assert!(transfer.more);
    fixture
        .peer
        .retirement(
            SEND,
            transfer.delivery_id.expect("original partial ID"),
            &transaction,
        )
        .await;
    let (channel, refusal) = fixture.peer.control().await;
    assert_eq!(channel, fixture.peer.local(SEND));
    let Performative::Detach(detach) = refusal else {
        panic!("unsupported partial retirement source refusal");
    };
    assert_eq!(detach.handle, handle);
    assert!(detach.closed);
    assert_eq!(
        detach
            .error
            .expect("fully flushed originals only")
            .condition
            .as_symbol()
            .as_str(),
        "amqp:not-implemented"
    );
    assert!(
        bounded("incomplete send fails", sending.as_mut())
            .await
            .is_err()
    );
    assert_eq!(observer.state(), NativeTransactionState::Pending);
    fixture
        .peer
        .send(
            SEND,
            Performative::Detach(Detach {
                handle: HANDLE,
                closed: true,
                error: None,
            }),
            Vec::new(),
        )
        .await;
    let discharged = fixture
        .discharge(&mut coordinator, CONTROL, &transaction, false)
        .await;
    let ready = discharged
        .receipt
        .prepare_work(Vec::new())
        .expect("unmutated truly empty group");
    let (ticket, resources) = ready.into_owner_parts();
    ticket
        .try_claim()
        .expect("empty native claim")
        .finish(NativeTransactionDecision::Committed);
    super::lifecycle::finish_committed(&mut fixture, resources, (CONTROL, discharged.id), &[])
        .await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn partial_inbound_post_forces_control_detach_but_restores_fully_flushed_retirement() {
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    let (_posting_session, _receiver) = fixture.receiver().await;
    let transaction = txn(15);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    let mut sent = fixture.send(&mut sender, SEND, handle).await;
    let original = sent.delivery.delivery_identity().clone();
    let receipt = fixture.retirement(&mut sent, &transaction).await;
    let prepared = fixture.provisional(receipt, &transaction, sent.id).await;
    fixture.peer.partial_post(&transaction).await;
    fixture.peer.barrier(POST).await;
    fixture
        .command(
            CONTROL,
            TransactionCommand::Discharge(Discharge {
                txn_id: transaction,
                fail: Some(false),
            }),
        )
        .await;
    let (channel, frame) = fixture.peer.control().await;
    assert_eq!(channel, fixture.peer.local(CONTROL));
    let Performative::Detach(detach) = frame else {
        panic!("inbound PartialAtSeal mandates coordinator Detach");
    };
    assert_eq!(
        detach
            .error
            .expect("partial posting rollback")
            .condition
            .as_symbol()
            .as_str(),
        "amqp:transaction:rollback"
    );
    assert_eq!(observer.state(), NativeTransactionState::Faulted);
    fixture.peer.barrier(SEND).await;
    drop(prepared);
    fixture
        .peer
        .outcome(
            SEND,
            sent.id,
            None,
            DeliveryState::Accepted(Accepted),
            false,
        )
        .await;
    let TransactionalDisposition::Ordinary(receipt) = bounded(
        "same original restored despite inbound partial",
        sent.delivery.next_disposition(),
    )
    .await
    .expect("original rearmed") else {
        panic!("ordinary original outcome");
    };
    assert!(receipt.delivery_identity().same_delivery(&original));
    let (result, acknowledgement) = bounded("restored original ACK", async {
        tokio::join!(
            receipt.accept(),
            fixture.peer.disposition(SEND, Role::Sender, sent.id)
        )
    })
    .await;
    result.expect("restored original flushed ACK");
    assert!(acknowledgement.settled);
    assert_eq!(
        acknowledgement.state,
        Some(DeliveryState::Accepted(Accepted))
    );
    fixture.shutdown().await;
}
