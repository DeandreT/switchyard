use super::*;

#[tokio::test]
async fn dedicated_sender_preserves_ordinary_sender_settled_default_outcome() {
    let mut fixture = Fixture::new().await;
    let mut session = fixture.session(SEND).await;
    let mut request = attach(HANDLE, "retirement-sender", Role::Receiver);
    request
        .source
        .as_mut()
        .expect("original sender source")
        .default_outcome = Some(DeliveryState::Accepted(Accepted));
    let incoming = fixture.incoming(&mut session, SEND, request).await;
    let (sender, response) = bounded("dedicated sender default outcome admission", async {
        tokio::join!(
            session.accept_transactional_sender(incoming, 0),
            fixture.peer.attached(SEND)
        )
    })
    .await;
    let mut sender = sender.expect("dedicated sender");
    fixture.peer.grant(SEND, HANDLE).await;
    let mut sent = fixture.send(&mut sender, SEND, response.handle).await;
    fixture
        .peer
        .send(
            SEND,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: sent.id,
                last: None,
                settled: true,
                state: None,
                batchable: false,
            }),
            Vec::new(),
        )
        .await;
    let TransactionalDisposition::Ordinary(receipt) = bounded(
        "ordinary source default outcome",
        sent.delivery.next_disposition(),
    )
    .await
    .expect("ordinary event") else {
        panic!("remote settlement is not retirement admission");
    };
    assert_eq!(receipt.outcome(), &Outcome::Accepted(Accepted));
    assert!(
        receipt
            .delivery_identity()
            .same_delivery(sent.delivery.delivery_identity())
    );
    bounded(
        "already remotely settled ordinary outcome",
        receipt.accept(),
    )
    .await
    .expect("no transport ACK required");
    fixture.peer.barrier(SEND).await;
    fixture.shutdown().await;
}

pub(super) async fn finish_committed(
    fixture: &mut Fixture,
    resources: NativeTransactionResources,
    control: (u16, u32),
    expected: &[(u16, Role, u32)],
) {
    let responses = async {
        let mut seen = Vec::new();
        for _ in 0..expected.len() {
            let (channel, frame) = fixture.peer.control().await;
            let Performative::Disposition(disposition) = frame else {
                panic!("terminal resource outcome");
            };
            assert!(disposition.settled);
            assert!(disposition.last.is_none());
            assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
            let actual = (channel, disposition.role, disposition.first);
            assert!(
                expected.contains(&actual),
                "unexpected final resource: {actual:?}"
            );
            assert!(!seen.contains(&actual), "duplicate final resource");
            seen.push(actual);
        }
        fixture.control_accepted(control.0, control.1).await;
    };
    let (result, ()) = bounded("native mixed terminal flushes", async {
        tokio::join!(resources.finish(), responses)
    })
    .await;
    result.expect("committed resource finalization");
}

#[tokio::test]
async fn posting_and_retirement_commit_as_one_exact_cross_session_bundle() {
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    let (_posting_session, mut receiver) = fixture.receiver().await;
    let transaction = txn(1);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    let mut sent = fixture.send(&mut sender, SEND, handle).await;
    let original = sent.delivery.delivery_identity().clone();
    let receipt = fixture.retirement(&mut sent, &transaction).await;
    assert!(receipt.belongs_to_sender(&sender.sender_identity()));
    assert!(
        receipt
            .controller_identity()
            .same_controller(coordinator.controller_identity())
    );
    let retirement = fixture.provisional(receipt, &transaction, sent.id).await;
    assert!(retirement.delivery_identity().same_delivery(&original));
    assert!(retirement.belongs_to_sender(&sender.sender_identity()));
    let posting = fixture.post(&mut receiver, &transaction).await;
    let discharged = fixture
        .discharge(&mut coordinator, CONTROL, &transaction, false)
        .await;
    bounded("mixed group ready", discharged.receipt.wait_ready())
        .await
        .expect("both exact provisional flushes");
    let ready = discharged
        .receipt
        .prepare_work(vec![
            NativePreparedWork::Posting(posting),
            NativePreparedWork::Retirement(retirement),
        ])
        .expect("exact mixed manifest");
    let (ticket, resources) = ready.into_owner_parts();
    let claim = ticket.try_claim().expect("one mixed native claim");
    claim.finish(NativeTransactionDecision::Committed);
    assert_eq!(observer.state(), NativeTransactionState::Committed);
    let expected = [
        (fixture.peer.local(POST), Role::Receiver, 0),
        (fixture.peer.local(SEND), Role::Sender, sent.id),
    ];
    finish_committed(&mut fixture, resources, (CONTROL, discharged.id), &expected).await;
    assert!(original.same_delivery(sent.delivery.delivery_identity()));
    fixture.peer.barrier(SEND).await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn explicit_rollback_rearms_same_original_and_late_attempt_drop_cannot_reset_new_transaction()
{
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    let first_id = txn(2);
    let first_observer = fixture.declare(&mut coordinator, CONTROL, &first_id).await;
    let mut sent = fixture.send(&mut sender, SEND, handle).await;
    let original = sent.delivery.delivery_identity().clone();
    let first_receipt = fixture.retirement(&mut sent, &first_id).await;
    let held_first = fixture.provisional(first_receipt, &first_id, sent.id).await;
    let discharged = fixture
        .discharge(&mut coordinator, CONTROL, &first_id, true)
        .await;
    let (result, ()) = bounded("explicit retirement rollback", async {
        tokio::join!(
            discharged.receipt.rollback(),
            fixture.control_accepted(CONTROL, discharged.id)
        )
    })
    .await;
    result.expect("known rollback completed");
    assert_eq!(first_observer.state(), NativeTransactionState::Aborted);
    fixture.peer.barrier(SEND).await;

    let second_id = txn(3);
    let second_observer = fixture.declare(&mut coordinator, CONTROL, &second_id).await;
    let second_receipt = fixture.retirement(&mut sent, &second_id).await;
    assert!(second_receipt.delivery_identity().same_delivery(&original));
    let second = fixture
        .provisional(second_receipt, &second_id, sent.id)
        .await;
    drop(held_first);
    fixture.peer.barrier(SEND).await;
    assert_eq!(second_observer.state(), NativeTransactionState::Pending);
    let discharged = fixture
        .discharge(&mut coordinator, CONTROL, &second_id, false)
        .await;
    let ready = discharged
        .receipt
        .prepare_work(vec![NativePreparedWork::Retirement(second)])
        .expect("new exact attempt only");
    let (ticket, resources) = ready.into_owner_parts();
    ticket
        .try_claim()
        .expect("second attempt claimed")
        .finish(NativeTransactionDecision::Committed);
    let expected = [(fixture.peer.local(SEND), Role::Sender, sent.id)];
    finish_committed(&mut fixture, resources, (CONTROL, discharged.id), &expected).await;
    assert_eq!(second_observer.state(), NativeTransactionState::Committed);
    assert!(original.same_delivery(sent.delivery.delivery_identity()));
    fixture.shutdown().await;
}

#[tokio::test]
async fn known_owner_rejection_or_explicit_claim_abort_restore_ordinary_outcome_capacity() {
    for abort_claim in [false, true] {
        let mut fixture = Fixture::new().await;
        let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
        let mut session = fixture.session(SEND).await;
        let (mut sender, handle) = fixture
            .sender(&mut session, SEND, HANDLE, "retirement-sender")
            .await;
        let transaction = txn(4);
        let observer = fixture
            .declare(&mut coordinator, CONTROL, &transaction)
            .await;
        let mut sent = fixture.send(&mut sender, SEND, handle).await;
        let original = sent.delivery.delivery_identity().clone();
        let receipt = fixture.retirement(&mut sent, &transaction).await;
        let prepared = fixture.provisional(receipt, &transaction, sent.id).await;
        let discharged = fixture
            .discharge(&mut coordinator, CONTROL, &transaction, false)
            .await;
        let ready = discharged
            .receipt
            .prepare_work(vec![NativePreparedWork::Retirement(prepared)])
            .expect("exact retirement ready");
        let (ticket, resources) = ready.into_owner_parts();
        let claim = ticket.try_claim().expect("actual native claim");
        if abort_claim {
            claim.abort();
        } else {
            claim.finish(NativeTransactionDecision::Rejected);
        }
        assert_eq!(
            observer.state(),
            if abort_claim {
                NativeTransactionState::Aborted
            } else {
                NativeTransactionState::Rejected
            }
        );
        let (result, disposition) = bounded("known negative discharge", async {
            tokio::join!(
                resources.finish(),
                fixture
                    .peer
                    .disposition(CONTROL, Role::Receiver, discharged.id)
            )
        })
        .await;
        result.expect("known rollback response flushed");
        assert!(disposition.settled);
        let Some(DeliveryState::Rejected(crate::Rejected { error: Some(error) })) =
            disposition.state
        else {
            panic!("negative discharge outcome");
        };
        assert_eq!(
            error.condition.as_symbol().as_str(),
            "amqp:transaction:rollback"
        );
        fixture.peer.barrier(SEND).await;
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
        let TransactionalDisposition::Ordinary(pending) = bounded(
            "ordinary outcome after rollback",
            sent.delivery.next_disposition(),
        )
        .await
        .expect("rearmed original") else {
            panic!("ordinary disposition");
        };
        assert!(pending.delivery_identity().same_delivery(&original));
        let (result, acknowledgement) = bounded("ordinary second-mode final ACK", async {
            tokio::join!(
                pending.accept(),
                fixture.peer.disposition(SEND, Role::Sender, sent.id)
            )
        })
        .await;
        result.expect("ordinary ACK flushed");
        assert!(acknowledgement.settled);
        assert_eq!(
            acknowledgement.state,
            Some(DeliveryState::Accepted(Accepted))
        );
        assert!(original.same_delivery(sent.delivery.delivery_identity()));
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn ready_observation_does_not_replace_exact_mixed_manifest() {
    for omit_posting in [false, true] {
        let mut fixture = Fixture::new().await;
        let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
        let mut session = fixture.session(SEND).await;
        let (mut sender, handle) = fixture
            .sender(&mut session, SEND, HANDLE, "retirement-sender")
            .await;
        let (_posting_session, mut receiver) = fixture.receiver().await;
        let transaction = txn(5);
        fixture
            .declare(&mut coordinator, CONTROL, &transaction)
            .await;
        let mut sent = fixture.send(&mut sender, SEND, handle).await;
        let receipt = fixture.retirement(&mut sent, &transaction).await;
        let retirement = fixture.provisional(receipt, &transaction, sent.id).await;
        let posting = fixture.post(&mut receiver, &transaction).await;
        let discharged = fixture
            .discharge(&mut coordinator, CONTROL, &transaction, false)
            .await;
        bounded(
            "all mixed resources flushed",
            discharged.receipt.wait_ready(),
        )
        .await
        .expect("Ready observation");
        let result = if omit_posting {
            let result = discharged
                .receipt
                .prepare_work(vec![NativePreparedWork::Retirement(retirement)]);
            drop(posting);
            result
        } else {
            let result = discharged
                .receipt
                .prepare_work(vec![NativePreparedWork::Posting(posting)]);
            drop(retirement);
            result
        };
        assert!(matches!(
            result,
            Err(NativeTransactionError::InvalidPreparedSet)
        ));
        fixture.shutdown().await;
    }
}
