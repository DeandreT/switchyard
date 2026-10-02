use super::*;

const BATCH_FORMAT: u32 = 0x8001_3700;

fn id(byte: u8) -> TransactionId {
    TransactionId::new([byte]).expect("bounded transaction ID")
}

fn decoders() -> MessageFormatDecoders {
    MessageFormatDecoders::default()
        .with_decoder(BATCH_FORMAT, crate::decode_message)
        .expect("explicit batch outer decoder")
}

fn wrapper() -> Message {
    Message {
        body: crate::Body::Data(vec![
            encode_message(&Message::data(b"first embedded message".to_vec()))
                .expect("first encoding")
                .into(),
            encode_message(&Message::data(b"second embedded message".to_vec()))
                .expect("second encoding")
                .into(),
        ]),
        ..Message::default()
    }
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

async fn ordinary(
    fixture: &mut Fixture,
    receiver: &mut TransactionalReceiver,
    format: u32,
    delivery: u32,
    message: &Message,
) {
    let mut transfer = first(POST_HANDLE, delivery, None, false);
    transfer.message_format = Some(format);
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            transfer,
            encode_message(message).expect("ordinary encoding"),
        )
        .await;
    let TransactionalIngress::Ordinary(receipt) =
        bounded("ordinary formatted delivery", receiver.recv())
            .await
            .expect("ordinary receipt")
    else {
        panic!("ordinary formatted message")
    };
    assert_eq!(receipt.message(), message);
    assert_eq!(receipt.message_format(), format);
    let (result, disposition) = tokio::join!(
        receiver.accept_retained(&receipt),
        fixture.peer.disposition(POST_CHANNEL, delivery)
    );
    result.expect("ordinary formatted settlement");
    assert!(disposition.settled);
    assert!(matches!(
        disposition.state,
        Some(DeliveryState::Accepted(_))
    ));
}

#[tokio::test]
async fn opted_in_batch_outer_decoder_preserves_one_native_posting_and_ordinary_content() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let (_data, mut receiver) = fixture
        .receiver_with_decoders(ReceiverSettleMode::First, decoders())
        .await;
    let proof = receiver.receiver_identity();
    let transaction = id(150);
    fixture.declare(&mut coordinator, &transaction).await;
    let message = wrapper();
    let mut transfer = first(POST_HANDLE, 0, Some(transaction.clone()), false);
    transfer.message_format = Some(BATCH_FORMAT);
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            transfer,
            encode_message(&message).expect("batch outer encoding"),
        )
        .await;
    let TransactionalIngress::Posting(posting) = bounded("one outer posting", receiver.recv())
        .await
        .expect("batch posting")
    else {
        panic!("transactional outer message")
    };
    assert_eq!(posting.message(), &message);
    assert_eq!(posting.message_format(), BATCH_FORMAT);
    assert!(posting.belongs_to_receiver(&proof));
    let prepared = fixture.provisional(posting, &transaction).await;
    assert_eq!(prepared.message(), &message);
    assert_eq!(prepared.message_format(), BATCH_FORMAT);
    assert!(prepared.belongs_to_receiver(&proof));
    let sealed = fixture
        .discharge(&mut coordinator, &transaction, false)
        .await;
    bounded("outer native readiness", sealed.wait_ready())
        .await
        .expect("ready outer posting");
    let ready = sealed
        .prepare(vec![prepared])
        .expect("exactly one native obligation, not one per embedded message");
    assert_eq!(fixture.committed(ready).await, 1);
    ordinary(&mut fixture, &mut receiver, BATCH_FORMAT, 0, &message).await;
    ordinary(
        &mut fixture,
        &mut receiver,
        0,
        0,
        &Message::data(b"built-in format zero remains unchanged".to_vec()),
    )
    .await;
    fixture.peer.barrier(POST_CHANNEL).await;
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn custom_decoder_registration_does_not_leak_to_adjacent_receivers() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator_with(rejected_profile()).await;
    let (mut data, mut configured) = fixture
        .receiver_with_decoders(ReceiverSettleMode::First, decoders())
        .await;
    let _healthy = fixture.session(HEALTHY_CHANNEL).await;
    let peer_handle = POST_HANDLE + 2;
    fixture
        .peer
        .attach(POST_CHANNEL, ordinary_attach(peer_handle))
        .await;
    let incoming = bounded("neighbor approval", data.next_incoming_attach())
        .await
        .expect("neighbor receiver");
    let responses = async {
        let attach = fixture.peer.attached(POST_CHANNEL).await;
        fixture.peer.credit(POST_CHANNEL, attach.handle).await;
        attach.handle
    };
    let (neighbor, own_handle) =
        tokio::join!(data.accept_transactional_receiver(incoming, 0), responses);
    let mut neighbor = neighbor.expect("default-decoder neighbor");
    let proof = neighbor.receiver_identity();
    let transaction = id(151);
    fixture.declare(&mut coordinator, &transaction).await;
    let mut transfer = first(peer_handle, 0, Some(transaction), false);
    transfer.message_format = Some(BATCH_FORMAT);
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            transfer,
            encode_message(&wrapper()).expect("outer encoding"),
        )
        .await;
    fixture
        .peer
        .refusal(POST_CHANNEL, own_handle, "amqp:not-implemented")
        .await;
    bounded("neighbor detached", neighbor.on_detach()).await;
    assert!(!proof.is_active());
    assert!(configured.receiver_identity().is_active());
    ordinary(
        &mut fixture,
        &mut configured,
        0,
        1,
        &Message::data(b"neighbor failure is link scoped".to_vec()),
    )
    .await;
    ordinary(&mut fixture, &mut configured, BATCH_FORMAT, 1, &wrapper()).await;
    fixture.peer.barrier(HEALTHY_CHANNEL).await;
    fixture.connection.shutdown().await;
}

fn reject_outer(_: &[u8]) -> io::Result<Message> {
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "controlled custom decoder refusal",
    ))
}

#[tokio::test]
async fn unknown_versions_decoder_failure_and_changed_fragment_format_cannot_mint_ready() {
    for failure in 0..3 {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = fixture.coordinator_with(rejected_profile()).await;
        let registry = if failure == 1 {
            MessageFormatDecoders::default()
                .with_decoder(BATCH_FORMAT, reject_outer)
                .expect("bounded custom decoder")
        } else {
            decoders()
        };
        let (_data, mut receiver) = fixture
            .receiver_with_decoders(ReceiverSettleMode::First, registry)
            .await;
        let _healthy = fixture.session(HEALTHY_CHANNEL).await;
        let transaction = id(152);
        let observer = fixture.declare(&mut coordinator, &transaction).await;
        let encoded = encode_message(&wrapper()).expect("valid outer message");
        let mut transfer = first(POST_HANDLE, 0, Some(transaction.clone()), failure == 2);
        transfer.message_format = Some(if failure == 0 {
            BATCH_FORMAT + 1
        } else {
            BATCH_FORMAT
        });
        if failure == 2 {
            let split = encoded.len() / 2;
            fixture
                .peer
                .transfer(POST_CHANNEL, transfer, encoded[..split].to_vec())
                .await;
            let mut tail = continuation(POST_HANDLE, None, false);
            tail.message_format = Some(BATCH_FORMAT + 1);
            fixture
                .peer
                .transfer(POST_CHANNEL, tail, encoded[split..].to_vec())
                .await;
        } else {
            fixture.peer.transfer(POST_CHANNEL, transfer, encoded).await;
        }
        fixture
            .peer
            .refusal(
                POST_CHANNEL,
                POST_HANDLE,
                if failure == 0 {
                    "amqp:not-implemented"
                } else {
                    "amqp:invalid-field"
                },
            )
            .await;
        assert!(matches!(
            bounded("decoder must not publish a receipt", receiver.recv()).await,
            Err(EngineError::RemoteDetached)
        ));
        assert_eq!(observer.state(), NativeTransactionState::Faulted);
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
        let refusal = fixture.peer.disposition(CONTROL_CHANNEL, 1).await;
        assert!(refusal.settled);
        assert!(
            matches!(refusal.state, Some(DeliveryState::Rejected(crate::Rejected { error: Some(error) })) if error.condition.as_symbol().as_str() == "amqp:transaction:rollback")
        );
        assert!(coordinator.controller_identity().is_active());
        fixture.peer.barrier(HEALTHY_CHANNEL).await;
        fixture.connection.shutdown().await;
    }
}

#[tokio::test]
async fn decoder_acceptance_still_requires_exact_actor_kind_connection_and_generation() {
    let mut fixture = Fixture::new(true).await;
    let mut data = fixture.session(POST_CHANNEL).await;
    fixture
        .peer
        .attach(POST_CHANNEL, ordinary_attach(POST_HANDLE))
        .await;
    let original = bounded(
        "original native receiver approval",
        data.next_incoming_attach(),
    )
    .await
    .expect("receiver approval");
    let mut other = Fixture::new(true).await;
    let other_data = other.session(POST_CHANNEL).await;
    assert!(
        other_data
            .accept_transactional_receiver_with_decoders(original.clone(), 0, decoders())
            .await
            .is_err()
    );
    other.peer.barrier(POST_CHANNEL).await;
    let mut changed = original.clone();
    changed.target = Some(Coordinator::default().into());
    assert!(
        data.accept_transactional_receiver_with_decoders(changed, 0, decoders())
            .await
            .is_err()
    );
    let responses = async {
        let attach = fixture.peer.attached(POST_CHANNEL).await;
        fixture.peer.credit(POST_CHANNEL, attach.handle).await;
    };
    let (receiver, ()) = tokio::join!(
        data.accept_transactional_receiver_with_decoders(original.clone(), 0, decoders()),
        responses
    );
    let mut receiver = receiver.expect("original approval remains valid");
    let old = receiver.receiver_identity();
    fixture.peer.detach(POST_CHANNEL, POST_HANDLE).await;
    bounded("old decoder receiver detached", receiver.on_detach()).await;
    fixture
        .peer
        .attach(POST_CHANNEL, ordinary_attach(POST_HANDLE))
        .await;
    let replacement = bounded("new generation approval", data.next_incoming_attach())
        .await
        .expect("new approval");
    assert!(
        data.accept_transactional_receiver_with_decoders(original, 0, decoders())
            .await
            .is_err(),
        "old approval cannot install decoders on a replacement"
    );
    fixture.peer.barrier(POST_CHANNEL).await;
    let responses = async {
        let attach = fixture.peer.attached(POST_CHANNEL).await;
        fixture.peer.credit(POST_CHANNEL, attach.handle).await;
    };
    let (replacement, ()) = tokio::join!(
        data.accept_transactional_receiver_with_decoders(replacement, 0, decoders()),
        responses
    );
    let mut replacement = replacement.expect("replacement decoder approval");
    assert!(!old.same_receiver(&replacement.receiver_identity()));
    ordinary(&mut fixture, &mut replacement, BATCH_FORMAT, 0, &wrapper()).await;
    let mut control = fixture.session(CONTROL_CHANNEL).await;
    fixture
        .peer
        .attach(CONTROL_CHANNEL, coordinator_attach(CONTROL_HANDLE))
        .await;
    let coordinator = bounded("coordinator kind approval", control.next_incoming_attach())
        .await
        .expect("coordinator approval");
    assert!(
        control
            .accept_transactional_receiver_with_decoders(coordinator.clone(), 0, decoders())
            .await
            .is_err(),
        "a decoder registry does not disguise a coordinator"
    );
    fixture.peer.barrier(CONTROL_CHANNEL).await;
    let responses = async {
        fixture.peer.attached(CONTROL_CHANNEL).await;
        fixture.peer.credit(CONTROL_CHANNEL, CONTROL_HANDLE).await;
    };
    let (coordinator, ()) = tokio::join!(control.accept_coordinator(coordinator, 0), responses);
    assert!(
        coordinator
            .expect("original coordinator remains approvable")
            .controller_identity()
            .is_active()
    );
    fixture.connection.shutdown().await;
    other.connection.shutdown().await;
}

#[tokio::test]
async fn receiver_decoder_opt_in_cannot_enable_transactions_on_a_default_connection() {
    let mut fixture = Fixture::new(false).await;
    let mut data = fixture.session(POST_CHANNEL).await;
    let _healthy = fixture.session(HEALTHY_CHANNEL).await;
    fixture
        .peer
        .attach(POST_CHANNEL, ordinary_attach(POST_HANDLE))
        .await;
    let incoming = bounded("default receiver approval", data.next_incoming_attach())
        .await
        .expect("ordinary approval");
    assert!(
        data.accept_transactional_receiver_with_decoders(incoming.clone(), 0, decoders())
            .await
            .is_err()
    );
    fixture.peer.barrier(POST_CHANNEL).await;
    let responses = async {
        let attach = fixture.peer.attached(POST_CHANNEL).await;
        fixture.peer.credit(POST_CHANNEL, attach.handle).await;
    };
    let (endpoint, ()) = tokio::join!(
        data.accept_attach_with_decoders(incoming, 0, None, decoders()),
        responses
    );
    let LinkEndpoint::Receiver(mut receiver) =
        endpoint.expect("ordinary decoder opt-in remains available")
    else {
        panic!("ordinary receiving endpoint")
    };
    let message = wrapper();
    let mut transfer = first(POST_HANDLE, 0, None, false);
    transfer.message_format = Some(BATCH_FORMAT);
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            transfer,
            encode_message(&message).expect("ordinary outer encoding"),
        )
        .await;
    let receipt = bounded("ordinary retained outer message", receiver.recv_retained())
        .await
        .expect("ordinary receipt");
    assert_eq!(receipt.message(), &message);
    assert_eq!(receipt.message_format(), BATCH_FORMAT);
    let (result, disposition) = tokio::join!(
        receiver.accept_retained(&receipt),
        fixture.peer.disposition(POST_CHANNEL, 0)
    );
    result.expect("ordinary accepted");
    assert!(matches!(
        disposition.state,
        Some(DeliveryState::Accepted(_))
    ));
    let mut transfer = first(POST_HANDLE, 1, Some(id(153)), false);
    transfer.message_format = Some(BATCH_FORMAT);
    fixture
        .peer
        .transfer(
            POST_CHANNEL,
            transfer,
            encode_message(&message).expect("valid unsupported transaction frame"),
        )
        .await;
    let end = fixture.peer.end(POST_CHANNEL).await;
    assert_eq!(
        end.error
            .expect("default explicit refusal")
            .condition
            .as_symbol()
            .as_str(),
        "amqp:not-implemented"
    );
    fixture
        .peer
        .send(POST_CHANNEL, Performative::End(End::default()), Vec::new())
        .await;
    fixture.peer.barrier(HEALTHY_CHANNEL).await;
    fixture.connection.shutdown().await;
}
