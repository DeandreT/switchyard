use super::*;

struct ReadyFixture {
    fixture: Fixture,
    _coordinator: CoordinatorEndpoint,
    _receiver: TransactionalReceiver,
    ready: NativeReadySubmission,
    observer: NativeTransactionIdentity,
}

async fn ready_fixture(byte: u8) -> ReadyFixture {
    let mut fixture = Fixture::new(true).await;
    let (_control, mut coordinator) = fixture.coordinator().await;
    let (_data, mut receiver) = fixture.receiver().await;
    let _healthy = fixture.session(HEALTHY_CHANNEL).await;
    let transaction = TransactionId::new([byte]).expect("bounded ID");
    let observer = fixture.declare(&mut coordinator, &transaction).await;
    fixture
        .post(&transaction, &Message::data(vec![byte; 127]))
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
    bounded("ready state", sealed.wait_ready())
        .await
        .expect("ready");
    let ready = sealed.prepare(vec![prepared]).expect("exact native bundle");
    ReadyFixture {
        fixture,
        _coordinator: coordinator,
        _receiver: receiver,
        ready,
        observer,
    }
}

#[tokio::test]
async fn ready_ticket_cannot_claim_after_controller_or_data_link_retirement() {
    for (channel, handle) in [
        (CONTROL_CHANNEL, CONTROL_HANDLE),
        (POST_CHANNEL, POST_HANDLE),
    ] {
        let ReadyFixture {
            mut fixture,
            _coordinator,
            _receiver,
            ready,
            observer,
        } = ready_fixture(20).await;
        let (ticket, resources) = ready.into_owner_parts();
        fixture.peer.detach(channel, handle).await;
        assert!(matches!(
            ticket.try_claim(),
            Err(NativeTransactionError::Faulted(NativeFault::Closed))
        ));
        assert_eq!(observer.state(), NativeTransactionState::Faulted);
        fixture.peer.barrier(HEALTHY_CHANNEL).await;
        assert!(fixture.connection.connection_identity().is_active());
        drop(resources);
        fixture.connection.shutdown().await;
    }
}

#[tokio::test]
async fn started_native_claim_survives_data_link_retirement_without_alias_reuse() {
    let ReadyFixture {
        mut fixture,
        _coordinator,
        _receiver,
        ready,
        observer,
    } = ready_fixture(21).await;
    let (ticket, resources) = ready.into_owner_parts();
    let claim = ticket
        .try_claim()
        .expect("claim precedes physical retirement");
    fixture.peer.detach(POST_CHANNEL, POST_HANDLE).await;
    fixture.peer.barrier(HEALTHY_CHANNEL).await;
    claim.finish(NativeTransactionDecision::Committed);
    assert_eq!(observer.state(), NativeTransactionState::Committed);
    assert_eq!(
        fixture.finish_committed(resources).await,
        0,
        "retired exact posting owner needs no alias acknowledgement"
    );
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn dropping_exact_resource_bundle_poisons_unclaimed_ticket() {
    let ReadyFixture {
        mut fixture,
        _coordinator,
        _receiver,
        ready,
        observer,
    } = ready_fixture(22).await;
    let (ticket, resources) = ready.into_owner_parts();
    drop(resources);
    assert!(matches!(
        ticket.try_claim(),
        Err(NativeTransactionError::Faulted(NativeFault::Dropped))
    ));
    assert_eq!(observer.state(), NativeTransactionState::Faulted);
    fixture.peer.barrier(HEALTHY_CHANNEL).await;
    fixture.connection.shutdown().await;
}

#[tokio::test]
async fn actor_shutdown_faults_ready_authority_without_changing_origin_observer() {
    let ReadyFixture {
        fixture,
        _coordinator,
        _receiver,
        ready,
        observer,
    } = ready_fixture(23).await;
    let origin = fixture.connection.connection_identity().clone();
    let (ticket, resources) = ready.into_owner_parts();
    bounded("actor shutdown", fixture.connection.shutdown()).await;
    assert!(!origin.is_active());
    assert!(origin.same_connection(fixture.connection.connection_identity()));
    assert!(matches!(
        ticket.try_claim(),
        Err(NativeTransactionError::Faulted(NativeFault::Closed))
    ));
    assert_eq!(observer.state(), NativeTransactionState::Faulted);
    drop(resources);
}

#[tokio::test]
async fn prepared_payload_from_same_named_foreign_connection_cannot_mint_ready() {
    let mut first = Fixture::new(true).await;
    let mut second = Fixture::new(true).await;
    let (_first_control, mut first_coordinator) = first.coordinator().await;
    let (_first_data, mut first_receiver) = first.receiver().await;
    let (_second_control, mut second_coordinator) = second.coordinator().await;
    let (_second_data, mut second_receiver) = second.receiver().await;
    let transaction = TransactionId::new([24]).expect("same bytes on distinct connections");
    first.declare(&mut first_coordinator, &transaction).await;
    second.declare(&mut second_coordinator, &transaction).await;
    let message = Message::data(vec![24; 127]);
    first.post(&transaction, &message).await;
    second.post(&transaction, &message).await;
    let TransactionalIngress::Posting(first_post) = bounded("first post", first_receiver.recv())
        .await
        .expect("first posting")
    else {
        panic!("first posting")
    };
    let TransactionalIngress::Posting(second_post) = bounded("second post", second_receiver.recv())
        .await
        .expect("second posting")
    else {
        panic!("second posting")
    };
    assert!(
        !first_post
            .controller_identity()
            .same_controller(second_post.controller_identity())
    );
    assert!(
        first_post
            .controller_identity()
            .connection_identity()
            .expect("first origin")
            .same_connection(first.connection.connection_identity())
    );
    assert!(
        second_post
            .controller_identity()
            .connection_identity()
            .expect("second origin")
            .same_connection(second.connection.connection_identity())
    );
    let first_prepared = first.provisional(first_post, &transaction).await;
    let second_prepared = second.provisional(second_post, &transaction).await;
    let sealed = first
        .discharge(&mut first_coordinator, &transaction, false)
        .await;
    bounded("first ready observation", sealed.wait_ready())
        .await
        .expect("first ready");
    assert!(matches!(
        sealed.prepare(vec![second_prepared]),
        Err(NativeTransactionError::InvalidPreparedSet)
    ));
    assert_eq!(first_prepared.message(), &message);
    drop(first_prepared);
    first.connection.shutdown().await;
    second.connection.shutdown().await;
}

#[tokio::test]
async fn started_native_claim_survives_controller_retirement_and_cannot_publish_stale_control_ack()
{
    let ReadyFixture {
        mut fixture,
        _coordinator,
        _receiver,
        ready,
        observer,
    } = ready_fixture(25).await;
    let (ticket, resources) = ready.into_owner_parts();
    let claim = ticket.try_claim().expect("native claim");
    assert_eq!(observer.state(), NativeTransactionState::OwnerStarted);
    fixture.peer.detach(CONTROL_CHANNEL, CONTROL_HANDLE).await;
    assert_eq!(observer.state(), NativeTransactionState::OwnerStarted);
    claim.finish(NativeTransactionDecision::Committed);
    assert_eq!(observer.state(), NativeTransactionState::Committed);
    let (result, disposition) = tokio::join!(
        resources.finish(),
        fixture.peer.disposition(POST_CHANNEL, 0)
    );
    assert!(
        result.is_err(),
        "retired exact controller cannot acknowledge its old control delivery"
    );
    assert!(matches!(
        disposition.state,
        Some(DeliveryState::Accepted(_))
    ));
    fixture.peer.barrier(HEALTHY_CHANNEL).await;
    assert_eq!(observer.state(), NativeTransactionState::Committed);
    fixture.connection.shutdown().await;
}
