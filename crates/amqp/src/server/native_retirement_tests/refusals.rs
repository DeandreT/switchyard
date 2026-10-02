use super::*;

#[tokio::test]
async fn dedicated_retirement_requires_new_policy_and_exact_unsettled_second_profile() {
    for (policy, mode, settled) in [
        (Policy::Disabled, ReceiverSettleMode::Second, false),
        (Policy::Posting, ReceiverSettleMode::Second, false),
        (Policy::Work, ReceiverSettleMode::First, false),
        (Policy::Work, ReceiverSettleMode::Second, true),
    ] {
        let mut fixture = Fixture::with_policy(policy, 262_144).await;
        let mut session = fixture.session(SEND).await;
        let mut request = attach(HANDLE, "profile-control", Role::Receiver);
        request.rcv_settle_mode = mode;
        if settled {
            request.snd_settle_mode = SenderSettleMode::Settled;
        }
        let incoming = fixture.incoming(&mut session, SEND, request).await;
        assert!(
            bounded(
                "unsupported sender profile local refusal",
                session.accept_transactional_sender(incoming.clone(), 0)
            )
            .await
            .is_err()
        );
        fixture.peer.barrier(SEND).await;
        let (ordinary, response) = bounded("original ordinary approval remains usable", async {
            tokio::join!(
                session.accept_attach(incoming, 0),
                fixture.peer.attached(SEND)
            )
        })
        .await;
        assert_eq!(response.role, Role::Sender);
        assert!(matches!(ordinary, Ok(LinkEndpoint::Sender(_))));
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn received_nonaccepted_or_early_settled_retirement_refuses_without_group_admission() {
    for variant in 0..3 {
        let mut fixture = Fixture::new().await;
        let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
        let mut session = fixture.session(SEND).await;
        let (mut sender, handle) = fixture
            .sender(&mut session, SEND, HANDLE, "retirement-sender")
            .await;
        let transaction = txn(24);
        let observer = fixture
            .declare(&mut coordinator, CONTROL, &transaction)
            .await;
        let sent = fixture.send(&mut sender, SEND, handle).await;
        let state = match variant {
            0 => DeliveryState::Received {
                section_number: 0,
                section_offset: 1,
            },
            1 => DeliveryState::Transactional(TransactionalState {
                txn_id: transaction.clone(),
                outcome: Some(Outcome::Released(crate::Released)),
            }),
            _ => DeliveryState::Transactional(TransactionalState {
                txn_id: transaction.clone(),
                outcome: Some(Outcome::Accepted(Accepted)),
            }),
        };
        fixture
            .peer
            .outcome(SEND, sent.id, None, state, variant == 2)
            .await;
        let (channel, frame) = fixture.peer.control().await;
        assert_eq!(channel, fixture.peer.local(SEND));
        let Performative::Detach(detach) = frame else {
            panic!("unsupported source state refusal");
        };
        assert_eq!(detach.handle, handle);
        assert!(detach.closed);
        assert_eq!(
            detach
                .error
                .expect("unsupported dedicated state")
                .condition
                .as_symbol()
                .as_str(),
            "amqp:not-implemented"
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
            .expect("rejected source state reserved no work");
        let (ticket, resources) = ready.into_owner_parts();
        ticket
            .try_claim()
            .expect("unaffected empty group")
            .finish(NativeTransactionDecision::Committed);
        super::lifecycle::finish_committed(&mut fixture, resources, (CONTROL, discharged.id), &[])
            .await;
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn transactional_range_containing_ordinary_sender_is_refused_before_any_prefix_retirement() {
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    let request = attach(HANDLE + 1, "ordinary-sender", Role::Receiver);
    let incoming = fixture.incoming(&mut session, SEND, request).await;
    let (ordinary, response) = bounded("ordinary sibling sender", async {
        tokio::join!(
            session.accept_attach(incoming, 0),
            fixture.peer.attached(SEND)
        )
    })
    .await;
    let LinkEndpoint::Sender(mut ordinary) = ordinary.expect("ordinary link") else {
        panic!("ordinary sender");
    };
    fixture.peer.grant(SEND, HANDLE + 1).await;
    let transaction = txn(6);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    let sent = fixture
        .send_tag(&mut sender, SEND, handle, b"dedicated-first")
        .await;
    let ordinary_message = message();
    let posting =
        ordinary.send_with_settlement(ordinary_message.clone(), b"ordinary-second".to_vec().into());
    tokio::pin!(posting);
    let ordinary_id = bounded("ordinary sibling Transfer", async {
        tokio::select! {
            result = posting.as_mut() => panic!("ordinary outcome before peer disposition: {}", result.is_ok()),
            id = fixture.peer.outgoing(SEND, response.handle, &ordinary_message, b"ordinary-second") => id,
        }
    }).await;
    assert_eq!(ordinary_id, sent.id + 1);
    fixture
        .peer
        .outcome(
            SEND,
            sent.id,
            Some(ordinary_id),
            DeliveryState::Transactional(TransactionalState {
                txn_id: transaction,
                outcome: Some(Outcome::Accepted(Accepted)),
            }),
            false,
        )
        .await;
    let (channel, frame) = fixture.peer.control().await;
    assert_eq!(channel, fixture.peer.local(SEND));
    let Performative::End(end) = frame else {
        panic!("whole-range transaction refusal");
    };
    assert_eq!(
        end.error
            .expect("unsupported ordinary member")
            .condition
            .as_symbol()
            .as_str(),
        "amqp:not-implemented"
    );
    assert!(
        bounded(
            "ordinary wait failed without accepted prefix",
            posting.as_mut()
        )
        .await
        .is_err()
    );
    assert_eq!(observer.state(), NativeTransactionState::Pending);
    assert_eq!(sent.delivery.delivery_identity().id(), sent.id);
    fixture
        .peer
        .send(SEND, Performative::End(End { error: None }), Vec::new())
        .await;
    let discharged = fixture
        .discharge(&mut coordinator, CONTROL, &txn(6), true)
        .await;
    let (result, ()) = bounded("unmodified group cleanup", async {
        tokio::join!(
            discharged.receipt.rollback(),
            fixture.control_accepted(CONTROL, discharged.id)
        )
    })
    .await;
    result.expect("empty rollback after atomic range refusal");
    fixture.shutdown().await;
}

#[tokio::test]
async fn shared_hundred_resource_limit_counts_postings_and_retirements_together() {
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    let (_posting_session, mut receiver) = fixture.receiver().await;
    let transaction = txn(7);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    let mut work = Vec::new();
    let mut sent_handles = Vec::new();
    for index in 0..50 {
        let mut sent = fixture.send_tag(&mut sender, SEND, handle, &[index]).await;
        let receipt = fixture.retirement(&mut sent, &transaction).await;
        work.push(NativePreparedWork::Retirement(
            fixture.provisional(receipt, &transaction, sent.id).await,
        ));
        work.push(NativePreparedWork::Posting(
            fixture.post(&mut receiver, &transaction).await,
        ));
        sent_handles.push(sent);
    }
    assert_eq!(work.len(), 100);
    let discharged = fixture
        .discharge(&mut coordinator, CONTROL, &transaction, false)
        .await;
    let ready = discharged
        .receipt
        .prepare_work(work)
        .expect("exact shared hundred-resource boundary");
    let (ticket, resources) = ready.into_owner_parts();
    ticket
        .try_claim()
        .expect("hundred resources claimed once")
        .finish(NativeTransactionDecision::Committed);
    let mut expected = Vec::new();
    for (index, sent) in sent_handles.iter().enumerate() {
        expected.push((fixture.peer.local(SEND), Role::Sender, sent.id));
        expected.push((fixture.peer.local(POST), Role::Receiver, index as u32));
    }
    super::lifecycle::finish_committed(
        &mut fixture,
        resources,
        (CONTROL, discharged.id),
        &expected,
    )
    .await;
    assert_eq!(observer.state(), NativeTransactionState::Committed);
    fixture.shutdown().await;
}

#[tokio::test]
async fn ninety_nine_postings_plus_two_original_range_refuses_without_retiring_prefix() {
    let mut fixture = Fixture::new().await;
    let (_control_session, mut coordinator) = fixture.coordinator(CONTROL).await;
    let mut session = fixture.session(SEND).await;
    let (mut sender, handle) = fixture
        .sender(&mut session, SEND, HANDLE, "retirement-sender")
        .await;
    let (_posting_session, mut receiver) = fixture.receiver().await;
    let transaction = txn(16);
    let observer = fixture
        .declare(&mut coordinator, CONTROL, &transaction)
        .await;
    let mut postings = Vec::new();
    for _ in 0..99 {
        postings.push(fixture.post(&mut receiver, &transaction).await);
    }
    let first = fixture
        .send_tag(&mut sender, SEND, handle, b"range-first")
        .await;
    let second = fixture
        .send_tag(&mut sender, SEND, handle, b"range-second")
        .await;
    assert_eq!(second.id, first.id + 1);
    fixture
        .peer
        .outcome(
            SEND,
            first.id,
            Some(second.id),
            DeliveryState::Transactional(TransactionalState {
                txn_id: transaction.clone(),
                outcome: Some(Outcome::Accepted(Accepted)),
            }),
            false,
        )
        .await;
    let (channel, refusal) = fixture.peer.control().await;
    assert_eq!(channel, fixture.peer.local(SEND));
    let Performative::Detach(detach) = refusal else {
        panic!("whole range shared resource source refusal");
    };
    assert_eq!(detach.handle, handle);
    assert!(detach.closed);
    assert_eq!(
        detach
            .error
            .expect("shared hundred resource cap")
            .condition
            .as_symbol()
            .as_str(),
        "amqp:resource-limit-exceeded"
    );
    assert_eq!(observer.state(), NativeTransactionState::Faulted);
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
        .discharge(&mut coordinator, CONTROL, &transaction, true)
        .await;
    let responses = async {
        let mut seen = HashSet::new();
        for _ in 0..99 {
            let (channel, frame) = fixture.peer.control().await;
            assert_eq!(channel, fixture.peer.local(POST));
            let Performative::Disposition(disposition) = frame else {
                panic!("only existing posting rollback cleanup");
            };
            assert_eq!(disposition.role, Role::Receiver);
            assert!(disposition.settled);
            assert!(disposition.state.is_none());
            assert!(disposition.last.is_none());
            assert!(disposition.first < 99);
            assert!(seen.insert(disposition.first));
        }
        fixture.control_accepted(CONTROL, discharged.id).await;
    };
    let (result, ()) = bounded("combined-cap original posting cleanup", async {
        tokio::join!(discharged.receipt.rollback(), responses)
    })
    .await;
    result.expect("known cap rollback cleanup");
    assert_eq!(observer.state(), NativeTransactionState::Aborted);
    drop(postings);
    fixture.shutdown().await;
}
