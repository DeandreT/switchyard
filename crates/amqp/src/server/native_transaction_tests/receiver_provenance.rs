use super::*;

fn id(byte: u8) -> TransactionId {
    TransactionId::new([byte]).expect("bounded transaction ID")
}

async fn accept_on(
    fixture: &mut Fixture,
    session: &mut ServerSession,
    handle: u32,
) -> TransactionalReceiver {
    fixture
        .peer
        .attach(POST_CHANNEL, ordinary_attach(handle))
        .await;
    let incoming = bounded(
        "receiver generation approval",
        session.next_incoming_attach(),
    )
    .await
    .expect("incoming receiver");
    let responses = async {
        let attach = fixture.peer.attached(POST_CHANNEL).await;
        assert_eq!(attach.name, format!("ordinary-{handle}"));
        assert_eq!(attach.role, Role::Receiver);
        fixture.peer.credit(POST_CHANNEL, attach.handle).await;
    };
    let (receiver, ()) = tokio::join!(
        session.accept_transactional_receiver(incoming, 0),
        responses
    );
    receiver.expect("actor-approved receiver")
}

async fn sender_ack(fixture: &mut Fixture, delivery: u32) {
    fixture
        .peer
        .send(
            POST_CHANNEL,
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
    fixture.peer.barrier(POST_CHANNEL).await;
}

async fn posting(
    fixture: &mut Fixture,
    receiver: &mut TransactionalReceiver,
    transaction: &TransactionId,
    message: &Message,
) -> TransactionPostingReceipt {
    fixture.post(transaction, message).await;
    let TransactionalIngress::Posting(posting) = bounded("provenance posting", receiver.recv())
        .await
        .expect("posting receipt")
    else {
        panic!("transactional posting")
    };
    assert_eq!(posting.message(), message);
    posting
}

#[tokio::test]
async fn receiver_proofs_are_exact_inert_and_private_across_matched_connections() {
    let mut first = Fixture::new(true).await;
    let (_control, mut first_control) = first.coordinator().await;
    let (mut first_session, mut first_receiver) = first.receiver().await;
    let second_receiver = accept_on(&mut first, &mut first_session, POST_HANDLE + 2).await;
    let proof = first_receiver.receiver_identity();
    let observer = proof.clone();
    assert!(proof.same_receiver(&first_receiver.receiver_identity()));
    assert!(proof.same_receiver(&observer));
    assert!(!proof.same_receiver(&second_receiver.receiver_identity()));
    assert!(
        proof
            .connection_identity()
            .expect("bound origin")
            .same_connection(first.connection.connection_identity())
    );
    assert!(
        second_receiver
            .receiver_identity()
            .connection_identity()
            .expect("same connection")
            .same_connection(first.connection.connection_identity())
    );
    drop(observer);
    assert!(proof.is_active(), "observer destruction is inert");
    let rendered = format!("{proof:?}");
    assert!(rendered.contains("active: true"));
    assert!(!rendered.contains("ordinary-31"));
    assert!(!rendered.contains("queue"));

    let mut foreign = Fixture::new(true).await;
    let (_control, mut foreign_control) = foreign.coordinator().await;
    let (_data, mut foreign_receiver) = foreign.receiver().await;
    let foreign_proof = foreign_receiver.receiver_identity();
    assert!(
        !proof.same_receiver(&foreign_proof),
        "identical names, handles and channels are not provenance"
    );
    assert!(
        !proof
            .connection_identity()
            .expect("first origin")
            .same_connection(foreign_proof.connection_identity().expect("foreign origin"))
    );
    let transaction = id(140);
    first.declare(&mut first_control, &transaction).await;
    foreign.declare(&mut foreign_control, &transaction).await;
    let message = Message::data(b"matched delivery ID and tag".to_vec());
    let first_post = posting(&mut first, &mut first_receiver, &transaction, &message).await;
    let foreign_post = posting(&mut foreign, &mut foreign_receiver, &transaction, &message).await;
    assert!(first_post.belongs_to_receiver(&proof));
    assert!(!first_post.belongs_to_receiver(&foreign_proof));
    assert!(!first_post.belongs_to_receiver(&second_receiver.receiver_identity()));
    assert!(foreign_post.belongs_to_receiver(&foreign_proof));
    assert!(!foreign_post.belongs_to_receiver(&proof));
    first.connection.shutdown().await;
    foreign.connection.shutdown().await;
    assert!(!proof.is_active());
    assert!(!foreign_proof.is_active());
    assert!(proof.same_receiver(&first_receiver.receiver_identity()));
    assert!(!first_post.belongs_to_receiver(&proof));
    assert!(!foreign_post.belongs_to_receiver(&foreign_proof));
    assert!(format!("{proof:?}").contains("active: false"));
}

#[tokio::test]
async fn prepared_receiver_provenance_survives_sender_ack_and_numeric_alias_reuse() {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = fixture.coordinator().await;
        let (_data, mut receiver) = fixture.receiver_with_mode(mode.clone()).await;
        let proof = receiver.receiver_identity();
        let transaction = id(141);
        fixture.declare(&mut coordinator, &transaction).await;
        let message = Message::data(b"retained native origin".to_vec());
        let receipt = posting(&mut fixture, &mut receiver, &transaction, &message).await;
        assert!(receipt.belongs_to_receiver(&proof));
        let prepared = fixture.provisional(receipt, &transaction).await;
        assert!(prepared.belongs_to_receiver(&proof));
        sender_ack(&mut fixture, 0).await;
        assert!(
            prepared.belongs_to_receiver(&proof),
            "transport alias removal does not erase receiver origin"
        );
        let replacement_message =
            Message::data(b"replacement alias is not original posting".to_vec());
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
                .expect("replacement")
        else {
            panic!("ordinary replacement")
        };
        assert_eq!(replacement.message(), &replacement_message);
        assert!(prepared.belongs_to_receiver(&proof));
        assert_eq!(prepared.message(), &message);
        let sealed = fixture
            .discharge(&mut coordinator, &transaction, false)
            .await;
        bounded("single posting ready", sealed.wait_ready())
            .await
            .expect("ready");
        assert_eq!(
            fixture
                .committed(
                    sealed
                        .prepare(vec![prepared])
                        .expect("exact original posting")
                )
                .await,
            0,
            "already sender-settled original needs no replacement disposition"
        );
        assert!(
            proof.is_active(),
            "transaction completion does not retire its receiver"
        );
        let (result, disposition) = tokio::join!(
            receiver.accept_retained(&replacement),
            fixture.peer.disposition(POST_CHANNEL, 0)
        );
        result.expect("replacement remains independently settleable");
        assert_eq!(disposition.settled, mode == ReceiverSettleMode::First);
        assert!(matches!(
            disposition.state,
            Some(DeliveryState::Accepted(_))
        ));
        if mode == ReceiverSettleMode::Second {
            sender_ack(&mut fixture, 0).await;
        }
        let ((), ()) = tokio::join!(
            bounded("receiver detach notification", receiver.on_detach()),
            fixture.peer.detach(POST_CHANNEL, POST_HANDLE)
        );
        assert!(!proof.is_active());
        assert!(proof.same_receiver(&receiver.receiver_identity()));
        assert!(fixture.connection.connection_identity().is_active());
        fixture.connection.shutdown().await;
    }
}

#[tokio::test]
async fn same_session_handle_and_name_recreation_has_a_new_receiver_generation() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let (mut data, mut original) = fixture.receiver().await;
    let _healthy = fixture.session(HEALTHY_CHANNEL).await;
    let old = original.receiver_identity();
    let transaction = id(142);
    fixture.declare(&mut coordinator, &transaction).await;
    let message = Message::data(b"old receiver generation".to_vec());
    let old_post = posting(&mut fixture, &mut original, &transaction, &message).await;
    let ((), ()) = tokio::join!(
        bounded("old receiver detach", original.on_detach()),
        fixture.peer.detach(POST_CHANNEL, POST_HANDLE)
    );
    assert!(!old.is_active());
    assert!(old.same_receiver(&original.receiver_identity()));
    assert!(!old_post.belongs_to_receiver(&old));
    assert_eq!(old_post.message(), &message);
    let mut replacement = accept_on(&mut fixture, &mut data, POST_HANDLE).await;
    let new = replacement.receiver_identity();
    assert!(new.is_active());
    assert!(!old.same_receiver(&new));
    assert!(
        old.connection_identity()
            .expect("old connection")
            .same_connection(new.connection_identity().expect("new connection"))
    );
    assert!(!old_post.belongs_to_receiver(&new));
    let transaction = id(143);
    fixture.declare(&mut coordinator, &transaction).await;
    let new_post = posting(
        &mut fixture,
        &mut replacement,
        &transaction,
        &Message::data(b"new generation, same delivery labels".to_vec()),
    )
    .await;
    assert!(new_post.belongs_to_receiver(&new));
    assert!(!new_post.belongs_to_receiver(&old));
    let (result, ()) = tokio::join!(
        old_post.provisional_accept(),
        fixture.peer.barrier(HEALTHY_CHANNEL)
    );
    assert!(
        result.is_err(),
        "retired receipt cannot acknowledge replacement generation"
    );
    let prepared = fixture.provisional(new_post, &transaction).await;
    assert!(prepared.belongs_to_receiver(&new));
    let sealed = fixture
        .discharge(&mut coordinator, &transaction, false)
        .await;
    bounded("replacement readiness", sealed.wait_ready())
        .await
        .expect("replacement ready");
    assert_eq!(
        fixture
            .committed(
                sealed
                    .prepare(vec![prepared])
                    .expect("replacement exact bundle")
            )
            .await,
        1
    );
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn session_end_retires_receiver_origin_without_retiring_the_connection() {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let (_data, mut receiver) = fixture.receiver().await;
    let _healthy = fixture.session(HEALTHY_CHANNEL).await;
    let proof = receiver.receiver_identity();
    let transaction = id(144);
    fixture.declare(&mut coordinator, &transaction).await;
    let message = Message::data(b"receipt survives session retirement".to_vec());
    let held = posting(&mut fixture, &mut receiver, &transaction, &message).await;
    fixture
        .peer
        .send(POST_CHANNEL, Performative::End(End::default()), Vec::new())
        .await;
    let end = fixture.peer.end(POST_CHANNEL).await;
    assert!(end.error.is_none());
    bounded("session retirement reaches receiver", receiver.on_detach()).await;
    assert!(!proof.is_active());
    assert!(proof.same_receiver(&receiver.receiver_identity()));
    assert!(!held.belongs_to_receiver(&proof));
    assert_eq!(held.message(), &message);
    assert!(fixture.connection.connection_identity().is_active());
    fixture.peer.barrier(HEALTHY_CHANNEL).await;
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn actor_shutdown_and_eof_retire_receiver_observers_and_retained_receipts() {
    for eof in [false, true] {
        let mut fixture = Fixture::new(true).await;
        let (_control, mut coordinator) = fixture.coordinator().await;
        let (_data, mut receiver) = fixture.receiver().await;
        let proof = receiver.receiver_identity();
        let transaction = id(145);
        fixture.declare(&mut coordinator, &transaction).await;
        let message = Message::data(b"receipt retains data, not active authority".to_vec());
        let held = posting(&mut fixture, &mut receiver, &transaction, &message).await;
        if eof {
            drop(fixture.peer.io);
        }
        bounded(
            "actor termination, not merely on_close",
            fixture.connection.shutdown(),
        )
        .await;
        bounded("terminated receiver notification", receiver.on_detach()).await;
        assert!(!fixture.connection.connection_identity().is_active());
        assert!(!proof.is_active());
        assert!(proof.same_receiver(&receiver.receiver_identity()));
        assert!(!held.belongs_to_receiver(&proof));
        assert_eq!(held.message(), &message);
    }
}
