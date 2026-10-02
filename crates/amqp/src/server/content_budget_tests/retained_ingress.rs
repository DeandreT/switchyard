use super::*;
use crate::{Modified, RetainedDelivery};

fn command_queue(receiver: &mut Receiver) -> mpsc::Receiver<Command> {
    let (commands, queued) = mpsc::channel(2);
    receiver.commands = commands;
    queued
}

async fn settle<F>(
    fixture: &mut Fixture,
    commands: &mut mpsc::Receiver<Command>,
    future: F,
) -> Result<(), EngineError>
where
    F: Future<Output = Result<(), EngineError>>,
{
    let mut future = Box::pin(future);
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let command = commands.try_recv().expect("settlement command");
    assert!(matches!(&command, Command::Settle { .. }));
    handle_command(command, &mut fixture.writer, &mut fixture.sessions, 512)
        .await
        .expect("settlement driver");
    future.await
}

async fn disposition(fixture: &Fixture) -> Disposition {
    let bytes = fixture.output.bytes.lock().expect("output").clone();
    let mut input = bytes.as_slice();
    let Frame::Amqp {
        channel,
        performative: Some(Performative::Disposition(disposition)),
        payload,
    } = read_frame(&mut input).await.expect("disposition frame")
    else {
        panic!("one disposition")
    };
    assert_eq!(channel, CHANNEL);
    assert!(payload.is_empty());
    assert!(input.is_empty());
    disposition
}

#[tokio::test]
async fn fragmented_dequeue_retains_exact_charge_and_refills_credit_independently() {
    let message = Message::data(vec![7; 59]);
    let encoded = encode_message(&message).expect("encoded message");
    let mut fixture = Fixture::new(encoded.len());
    let mut receiver = fixture.receiver(CHANNEL, RECEIVING).await;
    fixture
        .transfer(CHANNEL, first(RECEIVING, 0, true), encoded[..20].to_vec())
        .await;
    assert_eq!(fixture.budget.retained_bytes(), 20);
    fixture
        .transfer(
            CHANNEL,
            continuation(RECEIVING, false),
            encoded[20..].to_vec(),
        )
        .await;
    assert_eq!(fixture.budget.retained_bytes(), encoded.len());
    assert_eq!(fixture.receiving(CHANNEL, RECEIVING).credit.occupied(), 1);
    let receipt: RetainedDelivery = receiver.recv_retained().await.expect("retained delivery");
    assert_eq!(receipt.message(), &message);
    assert_eq!(receipt.message_format(), 0);
    assert!(
        receipt.inner().content_lease.is_none(),
        "the private naked delivery cannot duplicate the reservation"
    );
    assert_eq!(fixture.budget.retained_bytes(), encoded.len());
    fixture.clear_output();
    refresh_consumed(&mut fixture.writer, &mut fixture.sessions)
        .await
        .expect("same dequeue credit refresh");
    assert_eq!(fixture.receiving(CHANNEL, RECEIVING).credit.occupied(), 0);
    assert_eq!(
        fixture
            .receiving(CHANNEL, RECEIVING)
            .credit
            .snapshot()
            .link_credit,
        DELIVERY_QUEUE_CAPACITY as u32
    );
    assert_eq!(fixture.budget.retained_bytes(), encoded.len());
    fixture.clear_output();
    drop(receipt);
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert_eq!(
        fixture.output_len(),
        0,
        "receipt drop sends no disposition or credit"
    );
    refresh_consumed(&mut fixture.writer, &mut fixture.sessions)
        .await
        .expect("no second consumption");
    assert_eq!(fixture.output_len(), 0);
}

#[tokio::test]
async fn retained_receipt_blocks_same_and_cross_session_send_without_side_effects() {
    let message = Message::data(vec![2; 59]);
    let size = message_size(&message);
    let mut fixture = Fixture::new(size);
    fixture.session(CHANNEL + 1);
    let mut receiver = fixture.receiver(CHANNEL, RECEIVING).await;
    fixture.complete(CHANNEL, RECEIVING, 0, &message).await;
    let receipt = receiver.recv_retained().await.expect("retained delivery");
    for channel in [CHANNEL, CHANNEL + 1] {
        fixture.sender(channel, SENDING, SenderSettleMode::Unsettled);
        let flow = fixture.sessions[&channel].flow.snapshot();
        let credit = fixture.sending(channel, SENDING).credit.snapshot();
        let next_id = fixture.sessions[&channel].next_delivery_id;
        let output = fixture.output_len();
        let allocations = crate::codec::encoded_message_buffer_allocations();
        let refused = fixture.queue(channel, SENDING, message.clone(), 1).await;
        assert!(matches!(
            refused.await.expect("send reply"),
            Err(EngineError::InvalidState(_))
        ));
        assert_eq!(fixture.budget.retained_bytes(), size);
        assert_eq!(fixture.sessions[&channel].flow.snapshot(), flow);
        assert_eq!(fixture.sending(channel, SENDING).credit.snapshot(), credit);
        assert_eq!(fixture.sessions[&channel].next_delivery_id, next_id);
        assert!(
            fixture
                .sending(channel, SENDING)
                .outstanding_tags
                .is_empty()
        );
        assert!(fixture.sending(channel, SENDING).queued.is_empty());
        assert_eq!(fixture.output_len(), output);
        assert_eq!(
            crate::codec::encoded_message_buffer_allocations(),
            allocations
        );
    }
    drop(receipt);
    assert_eq!(fixture.budget.retained_bytes(), 0);
    let _healthy = fixture.queue(CHANNEL + 1, SENDING, message, 1).await;
    assert_eq!(fixture.budget.retained_bytes(), size);
}

#[tokio::test]
async fn every_retained_settlement_keeps_charge_after_successful_ack_flush() {
    for outcome in 0..4 {
        let message = Message::data(vec![3; 59]);
        let size = message_size(&message);
        let mut fixture = Fixture::new(size);
        let mut receiver = fixture.receiver(CHANNEL, RECEIVING).await;
        let mut commands = command_queue(&mut receiver);
        fixture.complete(CHANNEL, RECEIVING, 0, &message).await;
        let receipt = receiver.recv_retained().await.expect("retained delivery");
        fixture.clear_output();
        let (result, expected) = match outcome {
            0 => (
                settle(
                    &mut fixture,
                    &mut commands,
                    receiver.accept_retained(&receipt),
                )
                .await,
                DeliveryState::Accepted(Accepted),
            ),
            1 => (
                settle(
                    &mut fixture,
                    &mut commands,
                    receiver.reject_retained(&receipt, None),
                )
                .await,
                DeliveryState::Rejected(crate::Rejected { error: None }),
            ),
            2 => (
                settle(
                    &mut fixture,
                    &mut commands,
                    receiver.release_retained(&receipt),
                )
                .await,
                DeliveryState::Released(crate::Released),
            ),
            3 => {
                let modified = Modified {
                    delivery_failed: Some(true),
                    ..Modified::default()
                };
                (
                    settle(
                        &mut fixture,
                        &mut commands,
                        receiver.modify_retained(&receipt, modified.clone()),
                    )
                    .await,
                    DeliveryState::Modified(modified),
                )
            }
            _ => unreachable!(),
        };
        result.expect("ordinary settlement succeeds");
        let frame = disposition(&fixture).await;
        assert_eq!(frame.role, Role::Receiver);
        assert_eq!(frame.first, 0);
        assert!(frame.last.is_none());
        assert!(frame.settled);
        assert_eq!(frame.state, Some(expected));
        assert_eq!(fixture.budget.retained_bytes(), size);
        assert_eq!(receipt.message(), &message);
        assert_eq!(
            fixture.sessions[&CHANNEL]
                .incoming
                .settlement(&receiver.identity, &receipt.inner().identity)
                .expect("terminal identity"),
            SettlementAction::NoDisposition
        );
        drop(receipt);
        assert_eq!(fixture.budget.retained_bytes(), 0);
    }
}

#[tokio::test]
async fn sender_settled_receipt_needs_no_disposition_but_retains_charge_until_drop() {
    let message = Message::data(b"private-retained-content".to_vec());
    let encoded = encode_message(&message).expect("encoded message");
    let mut fixture = Fixture::new(encoded.len());
    let mut receiver = fixture.receiver(CHANNEL, RECEIVING).await;
    let mut commands = command_queue(&mut receiver);
    let mut transfer = first(RECEIVING, 0, false);
    transfer.settled = Some(true);
    transfer.delivery_tag = Some(b"private-retained-tag".to_vec().into());
    fixture.transfer(CHANNEL, transfer, encoded.clone()).await;
    let receipt = receiver
        .recv_retained()
        .await
        .expect("sender-settled receipt");
    assert_eq!(receipt.message(), &message);
    assert_eq!(format!("{receipt:?}"), "RetainedDelivery { .. }");
    assert_eq!(fixture.budget.retained_bytes(), encoded.len());
    assert_eq!(
        fixture.sessions[&CHANNEL]
            .incoming
            .settlement(&receiver.identity, &receipt.inner().identity)
            .expect("sender settlement"),
        SettlementAction::NoDisposition
    );
    fixture.clear_output();
    settle(
        &mut fixture,
        &mut commands,
        receiver.accept_retained(&receipt),
    )
    .await
    .expect("idempotent local settlement");
    assert_eq!(fixture.output_len(), 0);
    assert_eq!(fixture.budget.retained_bytes(), encoded.len());
    drop(receipt);
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert_eq!(fixture.output_len(), 0);
}

#[tokio::test]
async fn blocked_disposition_flush_keeps_receipt_charged_before_and_after_success() {
    let message = Message::data(vec![4; 59]);
    let size = message_size(&message);
    let mut fixture = Fixture::new(size);
    let mut receiver = fixture.receiver(CHANNEL, RECEIVING).await;
    let mut commands = command_queue(&mut receiver);
    fixture.complete(CHANNEL, RECEIVING, 0, &message).await;
    let receipt = receiver.recv_retained().await.expect("retained delivery");
    fixture.clear_output();
    let mut settled = Box::pin(receiver.accept_retained(&receipt));
    poll_fn(|cx| {
        assert!(settled.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    fixture.output.block_flush.store(true, Ordering::Release);
    let command = commands.try_recv().expect("settlement command");
    let mut processing = Box::pin(handle_command(
        command,
        &mut fixture.writer,
        &mut fixture.sessions,
        512,
    ));
    poll_fn(|cx| {
        assert!(processing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(fixture.budget.retained_bytes(), size);
    poll_fn(|cx| {
        assert!(settled.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    fixture.output.block_flush.store(false, Ordering::Release);
    processing.await.expect("successful resumed flush");
    settled.await.expect("settlement reply follows flush");
    assert_eq!(fixture.budget.retained_bytes(), size);
    assert!(matches!(
        disposition(&fixture).await.state,
        Some(DeliveryState::Accepted(_))
    ));
    drop(receipt);
    assert_eq!(fixture.budget.retained_bytes(), 0);
}

#[tokio::test]
async fn failed_or_cancelled_disposition_write_never_claims_ack_or_refunds_receipt() {
    for failed in [true, false] {
        let message = Message::data(vec![5; 59]);
        let size = message_size(&message);
        let mut fixture = Fixture::new(size);
        let mut receiver = fixture.receiver(CHANNEL, RECEIVING).await;
        let mut commands = command_queue(&mut receiver);
        fixture.complete(CHANNEL, RECEIVING, 0, &message).await;
        let receipt = receiver.recv_retained().await.expect("retained delivery");
        let mut settled = Box::pin(receiver.accept_retained(&receipt));
        poll_fn(|cx| {
            assert!(settled.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        fixture.output.fail_flush.store(failed, Ordering::Release);
        fixture.output.block_flush.store(!failed, Ordering::Release);
        let command = commands.try_recv().expect("settlement command");
        let mut processing = Box::pin(handle_command(
            command,
            &mut fixture.writer,
            &mut fixture.sessions,
            512,
        ));
        if failed {
            assert!(processing.await.is_err());
        } else {
            poll_fn(|cx| {
                assert!(processing.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(processing);
        }
        assert!(matches!(settled.await, Err(EngineError::Stopped)));
        assert_eq!(fixture.budget.retained_bytes(), size);
        assert_eq!(
            fixture.sessions[&CHANNEL]
                .incoming
                .settlement(&receiver.identity, &receipt.inner().identity)
                .expect("uncertain flush leaves logical delivery live"),
            SettlementAction::SendDisposition { settled: true }
        );
        stop_session(fixture.sessions.get_mut(&CHANNEL).expect("session"));
        assert_eq!(
            fixture.budget.retained_bytes(),
            size,
            "external receipt remains the content owner after actor teardown"
        );
        drop(receipt);
        assert_eq!(fixture.budget.retained_bytes(), 0);
    }
}

#[tokio::test]
async fn oversized_rejection_preflight_keeps_charge_and_allows_healthy_settlement() {
    let message = Message::data(vec![6; 59]);
    let size = message_size(&message);
    let mut fixture = Fixture::new(size);
    let mut receiver = fixture.receiver(CHANNEL, RECEIVING).await;
    let mut commands = command_queue(&mut receiver);
    fixture.complete(CHANNEL, RECEIVING, 0, &message).await;
    let receipt = receiver.recv_retained().await.expect("retained delivery");
    fixture.clear_output();
    let error = Error::new(crate::AmqpError::InvalidField, "x".repeat(1_000), None);
    assert!(
        settle(
            &mut fixture,
            &mut commands,
            receiver.reject_retained(&receipt, Some(error))
        )
        .await
        .is_err()
    );
    assert_eq!(fixture.output_len(), 0);
    assert_eq!(fixture.budget.retained_bytes(), size);
    assert_eq!(
        fixture.sessions[&CHANNEL]
            .incoming
            .settlement(&receiver.identity, &receipt.inner().identity)
            .expect("preflight keeps delivery live"),
        SettlementAction::SendDisposition { settled: true }
    );
    settle(
        &mut fixture,
        &mut commands,
        receiver.accept_retained(&receipt),
    )
    .await
    .expect("healthy retry");
    assert_eq!(fixture.budget.retained_bytes(), size);
    drop(receipt);
    assert_eq!(fixture.budget.retained_bytes(), 0);
}

#[tokio::test]
async fn second_mode_receipt_lifetime_does_not_promise_peer_acknowledgement() {
    let message = Message::data(vec![7; 59]);
    let size = message_size(&message);
    let mut fixture = Fixture::new(size);
    let mut receiver = fixture.receiver(CHANNEL, RECEIVING).await;
    let mut commands = command_queue(&mut receiver);
    let LinkState::Receiving(link) = fixture
        .sessions
        .get_mut(&CHANNEL)
        .expect("session")
        .links
        .get_mut(&RECEIVING)
        .expect("receiver")
    else {
        panic!("receiving link")
    };
    link.receiver_settle_mode = ReceiverSettleMode::Second;
    fixture.complete(CHANNEL, RECEIVING, 0, &message).await;
    let receipt = receiver.recv_retained().await.expect("retained delivery");
    let identity = receipt.inner().identity.clone();
    fixture.clear_output();
    settle(
        &mut fixture,
        &mut commands,
        receiver.accept_retained(&receipt),
    )
    .await
    .expect("local disposition flushed");
    assert!(!disposition(&fixture).await.settled);
    assert_eq!(fixture.budget.retained_bytes(), size);
    assert!(
        !fixture.sessions[&CHANNEL]
            .incoming
            .sender_is_settled(&identity)
            .expect("still waiting for sender ACK")
    );
    drop(receipt);
    assert_eq!(
        fixture.budget.retained_bytes(),
        0,
        "receipt drop does not await second-mode ACK"
    );
    assert!(
        !fixture.sessions[&CHANNEL]
            .incoming
            .sender_is_settled(&identity)
            .expect("transport settlement remains pending")
    );
    apply_disposition(
        CHANNEL,
        Disposition {
            role: Role::Sender,
            first: 0,
            last: None,
            settled: true,
            state: None,
            batchable: false,
        },
        &mut fixture.writer,
        &mut fixture.sessions,
    )
    .await
    .expect("later sender ACK");
    assert_eq!(
        fixture.sessions[&CHANNEL]
            .incoming
            .settlement(&receiver.identity, &identity)
            .expect("peer acknowledged"),
        SettlementAction::NoDisposition
    );
    assert_eq!(fixture.budget.retained_bytes(), 0);
}

#[tokio::test]
async fn foreign_link_and_reused_handle_cannot_settle_retained_authority() {
    let message = Message::data(vec![8; 59]);
    let size = message_size(&message);
    let mut fixture = Fixture::new(size);
    let mut original = fixture.receiver(CHANNEL, RECEIVING).await;
    fixture.complete(CHANNEL, RECEIVING, 0, &message).await;
    let receipt = original.recv_retained().await.expect("retained delivery");
    let foreign = fixture.receiver(CHANNEL, RECEIVING + 1).await;
    fixture.clear_output();
    assert!(matches!(
        foreign.accept_retained(&receipt).await,
        Err(EngineError::InvalidState(_))
    ));
    assert!(matches!(
        foreign.reject_retained(&receipt, None).await,
        Err(EngineError::InvalidState(_))
    ));
    assert!(matches!(
        foreign.release_retained(&receipt).await,
        Err(EngineError::InvalidState(_))
    ));
    assert!(matches!(
        foreign.modify_retained(&receipt, Modified::default()).await,
        Err(EngineError::InvalidState(_))
    ));
    assert_eq!(fixture.output_len(), 0);
    stop_session(fixture.sessions.get_mut(&CHANNEL).expect("session"));
    fixture.sessions.remove(&CHANNEL);
    fixture.session(CHANNEL);
    let replacement = fixture.receiver(CHANNEL, RECEIVING).await;
    fixture.clear_output();
    assert!(matches!(
        replacement.accept_retained(&receipt).await,
        Err(EngineError::InvalidState(_))
    ));
    assert_eq!(fixture.output_len(), 0);
    assert_eq!(fixture.budget.retained_bytes(), size);
    drop(original);
    drop(foreign);
    assert_eq!(fixture.budget.retained_bytes(), size);
    drop(receipt);
    assert_eq!(fixture.budget.retained_bytes(), 0);
}

#[tokio::test]
async fn retired_original_endpoint_refuses_settlement_without_releasing_external_receipt() {
    let message = Message::data(vec![9; 59]);
    let size = message_size(&message);
    let mut fixture = Fixture::new(size);
    let mut receiver = fixture.receiver(CHANNEL, RECEIVING).await;
    let mut commands = command_queue(&mut receiver);
    fixture.complete(CHANNEL, RECEIVING, 0, &message).await;
    let receipt = receiver.recv_retained().await.expect("retained delivery");
    stop_session(fixture.sessions.get_mut(&CHANNEL).expect("session"));
    fixture.clear_output();
    assert!(
        settle(
            &mut fixture,
            &mut commands,
            receiver.accept_retained(&receipt)
        )
        .await
        .is_err()
    );
    assert_eq!(fixture.output_len(), 0);
    assert_eq!(fixture.budget.retained_bytes(), size);
    drop(receiver);
    assert_eq!(fixture.budget.retained_bytes(), size);
    drop(receipt);
    assert_eq!(fixture.budget.retained_bytes(), 0);
}

#[tokio::test]
async fn canceling_application_await_drops_receipt_once_without_settlement() {
    let message = Message::data(vec![10; 59]);
    let size = message_size(&message);
    let mut fixture = Fixture::new(size);
    let mut receiver = fixture.receiver(CHANNEL, RECEIVING).await;
    fixture.complete(CHANNEL, RECEIVING, 0, &message).await;
    let receipt = receiver.recv_retained().await.expect("retained delivery");
    let identity = receipt.inner().identity.clone();
    fixture.clear_output();
    let mut application = Box::pin(async move {
        let _held = receipt;
        std::future::pending::<()>().await;
    });
    poll_fn(|cx| {
        assert!(application.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(fixture.budget.retained_bytes(), size);
    drop(application);
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert_eq!(fixture.output_len(), 0);
    assert_eq!(
        fixture.sessions[&CHANNEL]
            .incoming
            .settlement(&receiver.identity, &identity)
            .expect("drop is not acceptance"),
        SettlementAction::SendDisposition { settled: true }
    );
    assert_eq!(fixture.receiving(CHANNEL, RECEIVING).credit.occupied(), 1);
    refresh_consumed(&mut fixture.writer, &mut fixture.sessions)
        .await
        .expect("dequeue consumption applies once");
    assert_eq!(fixture.receiving(CHANNEL, RECEIVING).credit.occupied(), 0);
}

fn raw_decoder(bytes: &[u8]) -> io::Result<Message> {
    Ok(Message::data(bytes.to_vec()))
}

fn failed_decoder(_: &[u8]) -> io::Result<Message> {
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "custom decoder refused",
    ))
}

#[tokio::test]
async fn custom_format_retains_received_bytes_and_decoder_or_inbox_failure_refunds() {
    const FORMAT: u32 = 0xf123_4567;
    let mut fixture = Fixture::new(8);
    let mut receiver = fixture.receiver(CHANNEL, RECEIVING).await;
    let LinkState::Receiving(link) = fixture
        .sessions
        .get_mut(&CHANNEL)
        .expect("session")
        .links
        .get_mut(&RECEIVING)
        .expect("receiving link")
    else {
        panic!("receiving link")
    };
    link.decoders = MessageFormatDecoders::default()
        .with_decoder(FORMAT, raw_decoder)
        .expect("custom format");
    let mut transfer = first(RECEIVING, 0, false);
    transfer.message_format = Some(FORMAT);
    fixture.transfer(CHANNEL, transfer, vec![1, 2, 3]).await;
    let receipt = receiver
        .recv_retained()
        .await
        .expect("custom retained receipt");
    assert_eq!(receipt.message(), &Message::data(vec![1, 2, 3]));
    assert_eq!(receipt.message_format(), FORMAT);
    assert_eq!(
        fixture.budget.retained_bytes(),
        3,
        "charge is received content, not decoded re-encoding"
    );
    drop(receipt);
    assert_eq!(fixture.budget.retained_bytes(), 0);
    let _bad = fixture.receiver(CHANNEL, RECEIVING + 1).await;
    let LinkState::Receiving(link) = fixture
        .sessions
        .get_mut(&CHANNEL)
        .expect("session")
        .links
        .get_mut(&(RECEIVING + 1))
        .expect("receiving link")
    else {
        panic!("receiving link")
    };
    link.decoders = MessageFormatDecoders::default()
        .with_decoder(FORMAT, failed_decoder)
        .expect("custom format");
    let mut transfer = first(RECEIVING + 1, 1, false);
    transfer.message_format = Some(FORMAT);
    fixture.transfer(CHANNEL, transfer, vec![1; 8]).await;
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert!(
        !fixture.sessions[&CHANNEL]
            .links
            .contains_key(&(RECEIVING + 1))
    );
    let unavailable = fixture.receiver(CHANNEL, RECEIVING + 2).await;
    drop(unavailable);
    fixture
        .complete(CHANNEL, RECEIVING + 2, 1, &Message::data(vec![1; 3]))
        .await;
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert!(
        !fixture.sessions[&CHANNEL]
            .links
            .contains_key(&(RECEIVING + 2))
    );
    let mut healthy = fixture.receiver(CHANNEL, RECEIVING + 3).await;
    let message = Message::data(vec![2; 3]);
    fixture.complete(CHANNEL, RECEIVING + 3, 1, &message).await;
    let receipt = healthy
        .recv_retained()
        .await
        .expect("healthy sibling retained receive");
    assert_eq!(fixture.budget.retained_bytes(), 8);
    assert_eq!(receipt.message(), &message);
    drop(receipt);
    assert_eq!(fixture.budget.retained_bytes(), 0);
}

#[tokio::test]
async fn ordinary_recv_still_refunds_at_dequeue_and_does_not_wait_for_settlement() {
    let message = Message::data(vec![11; 59]);
    let size = message_size(&message);
    let mut fixture = Fixture::new(size);
    let mut receiver = fixture.receiver(CHANNEL, RECEIVING).await;
    fixture.complete(CHANNEL, RECEIVING, 0, &message).await;
    let delivery = receiver.recv().await.expect("ordinary delivery");
    assert!(delivery.content_lease.is_none());
    assert_eq!(fixture.budget.retained_bytes(), 0);
    let clone = delivery.clone();
    assert!(clone.content_lease.is_none());
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert_eq!(
        fixture.sessions[&CHANNEL]
            .incoming
            .settlement(&receiver.identity, &delivery.identity)
            .expect("ordinary delivery still unsettled"),
        SettlementAction::SendDisposition { settled: true }
    );
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn client_retained_receive_mirrors_all_settlements_and_foreign_link_guard() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (server_io, client_io) = tokio::io::duplex(4_096);
        let server = tokio::spawn(async move {
            let mut connection = ServerConnection::accept(server_io, "retained-server", None)
                .await
                .expect("server connection");
            let incoming = connection
                .next_incoming_session()
                .await
                .expect("incoming session");
            let mut session = connection
                .accept_session(incoming)
                .await
                .expect("server session");
            let attach = session
                .next_incoming_attach()
                .await
                .expect("receiver attach");
            let LinkEndpoint::Sender(mut sender) = session
                .accept_attach(attach, 0)
                .await
                .expect("sender endpoint")
            else {
                panic!("sending endpoint")
            };
            let attach = session
                .next_incoming_attach()
                .await
                .expect("foreign receiver attach");
            let _foreign = session
                .accept_attach(attach, 0)
                .await
                .expect("foreign endpoint");
            let mut outcomes = Vec::new();
            for index in 0..4 {
                outcomes.push(
                    sender
                        .send(Message::data(vec![index; 59]), vec![index].into())
                        .await
                        .expect("client settlement"),
                );
            }
            (connection, outcomes)
        });
        let mut connection = crate::ClientConnection::open(client_io, "retained-client", None)
            .await
            .expect("client connection");
        let mut session = crate::ClientSession::begin(&mut connection)
            .await
            .expect("client session");
        let mut receiver = crate::ClientReceiver::attach(&mut session, "retained", "queue")
            .await
            .expect("receiver");
        let foreign = crate::ClientReceiver::attach(&mut session, "foreign", "queue")
            .await
            .expect("foreign receiver");
        for index in 0..4 {
            let receipt: RetainedDelivery = receiver
                .recv_retained()
                .await
                .expect("client retained delivery");
            assert_eq!(receipt.message(), &Message::data(vec![index; 59]));
            assert_eq!(receipt.message_format(), 0);
            assert!(matches!(
                foreign.accept_retained(&receipt).await,
                Err(EngineError::InvalidState(_))
            ));
            assert!(matches!(
                foreign.reject_retained(&receipt, None).await,
                Err(EngineError::InvalidState(_))
            ));
            assert!(matches!(
                foreign.release_retained(&receipt).await,
                Err(EngineError::InvalidState(_))
            ));
            assert!(matches!(
                foreign.modify_retained(&receipt, Modified::default()).await,
                Err(EngineError::InvalidState(_))
            ));
            match index {
                0 => receiver.accept_retained(&receipt).await,
                1 => receiver.reject_retained(&receipt, None).await,
                2 => receiver.release_retained(&receipt).await,
                3 => {
                    receiver
                        .modify_retained(
                            &receipt,
                            Modified {
                                delivery_failed: Some(true),
                                ..Modified::default()
                            },
                        )
                        .await
                }
                _ => unreachable!(),
            }
            .expect("client retained settlement");
            assert_eq!(receipt.message(), &Message::data(vec![index; 59]));
            drop(receipt);
        }
        let (server_connection, outcomes) = server.await.expect("server driver");
        assert_eq!(
            outcomes,
            vec![
                Outcome::Accepted(Accepted),
                Outcome::Rejected(crate::Rejected { error: None }),
                Outcome::Released(crate::Released),
                Outcome::Modified(Modified {
                    delivery_failed: Some(true),
                    ..Modified::default()
                })
            ]
        );
        connection.close().await.expect("clean client Close");
        server_connection.shutdown().await;
    })
    .await
    .expect("bounded client retained mirror");
}
