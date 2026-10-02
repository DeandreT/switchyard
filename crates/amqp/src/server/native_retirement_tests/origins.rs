use super::*;

#[tokio::test]
async fn sealed_retirement_origins_include_unpolled_originals_and_remain_inert() {
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut first, first_handle) = fixture
        .sender(&mut session, SEND, HANDLE, "first-origin")
        .await;
    let (mut second, second_handle) = fixture
        .sender(&mut session, SEND, HANDLE + 1, "second-origin")
        .await;
    let transaction = txn(81);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    let sent_first = fixture.send(&mut first, SEND, first_handle).await;
    let sent_second = fixture.send(&mut second, SEND, second_handle).await;
    fixture
        .peer
        .retirement(SEND, sent_first.id, &transaction)
        .await;
    fixture
        .peer
        .retirement(SEND, sent_second.id, &transaction)
        .await;
    let discharged = fixture
        .discharge(&mut coordinator, CONTROL, &transaction, false)
        .await;
    assert_eq!(observer.state(), NativeTransactionState::Sealed);
    let origins = discharged
        .receipt
        .retirement_origins()
        .expect("live sealed snapshot");
    assert_eq!(origins.len(), 2);
    let expected = [
        (
            first.sender_identity(),
            sent_first.delivery.delivery_identity(),
        ),
        (
            second.sender_identity(),
            sent_second.delivery.delivery_identity(),
        ),
    ];
    for (sender, original) in &origins {
        assert_eq!(
            expected
                .iter()
                .filter(|(actual_sender, actual_original)| {
                    sender.same_sender(actual_sender) && original.same_delivery(actual_original)
                })
                .count(),
            1
        );
    }
    drop(origins.clone());
    assert_eq!(observer.state(), NativeTransactionState::Sealed);
    fixture.shutdown().await;
    assert_eq!(observer.state(), NativeTransactionState::Faulted);
    for (sender, original) in &origins {
        assert!(!sender.is_active());
        assert_eq!(
            expected
                .iter()
                .filter(|(actual_sender, actual_original)| {
                    sender.same_sender(actual_sender) && original.same_delivery(actual_original)
                })
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn an_empty_live_snapshot_is_distinct_from_terminal_replay_without_a_manifest() {
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let transaction = txn(82);
    fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    let discharged = fixture
        .discharge(&mut coordinator, CONTROL, &transaction, true)
        .await;
    assert!(
        discharged
            .receipt
            .retirement_origins()
            .expect("empty live group")
            .is_empty()
    );
    assert!(
        discharged
            .receipt
            .posting_receivers()
            .expect("empty live postings")
            .is_empty()
    );
    let (result, ()) = bounded("empty group rollback", async {
        tokio::join!(
            discharged.receipt.rollback(),
            fixture.control_accepted(CONTROL, discharged.id)
        )
    })
    .await;
    result.expect("actual rollback flush");
    let replay = fixture
        .discharge(&mut coordinator, CONTROL, &transaction, true)
        .await;
    assert!(replay.receipt.retirement_origins().is_none());
    assert!(replay.receipt.posting_receivers().is_none());
    let (result, ()) = bounded("terminal rollback replay", async {
        tokio::join!(
            replay.receipt.rollback(),
            fixture.control_accepted(CONTROL, replay.id)
        )
    })
    .await;
    result.expect("terminal control flush");
    fixture.shutdown().await;
}

#[tokio::test]
async fn sealed_posting_receivers_count_duplicate_unpolled_origins_without_retirements() {
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let (_posting_session, receiver) = fixture.receiver().await;
    let transaction = txn(83);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    for id in 0..2 {
        fixture
            .peer
            .transfer(POST, HANDLE, id, Some(&transaction), &message())
            .await;
    }
    let discharged = fixture
        .discharge(&mut coordinator, CONTROL, &transaction, false)
        .await;
    assert_eq!(observer.state(), NativeTransactionState::Sealed);
    let receivers = discharged
        .receipt
        .posting_receivers()
        .expect("live posting snapshot");
    assert_eq!(receivers.len(), 2);
    assert!(
        receivers
            .iter()
            .all(|origin| origin.same_receiver(&receiver.receiver_identity()))
    );
    assert!(
        discharged
            .receipt
            .retirement_origins()
            .expect("live retirement snapshot")
            .is_empty()
    );
    drop(receivers.clone());
    assert_eq!(observer.state(), NativeTransactionState::Sealed);
    fixture.shutdown().await;
    assert_eq!(observer.state(), NativeTransactionState::Faulted);
    assert!(
        receivers.iter().all(
            |origin| !origin.is_active() && origin.same_receiver(&receiver.receiver_identity())
        )
    );
}
