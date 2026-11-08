use super::*;

fn credit(sessions: &HashMap<u16, SessionState>) -> Flow {
    let session = &sessions[&1];
    let LinkState::Receiving(link) = &session.links[&2] else {
        panic!("receiver")
    };
    link.credit.flow(2, incoming::session_flow(session))
}

#[tokio::test]
async fn fragments_duplicate_continuations_and_aborts_consume_one_wrapping_credit_each() {
    let (mut sessions, mut receiver) = receiving_session(1, 2);
    let LinkState::Receiving(link) = sessions.get_mut(&1).unwrap().links.get_mut(&2).unwrap()
    else {
        panic!("receiver")
    };
    link.credit = incoming::ReceivingCredit::new(u32::MAX);
    let encoded = encode_message(&Message::data(vec![1, 2, 3])).unwrap();
    receive_transfer(
        1,
        transfer(2, None, true),
        encoded[..1].to_vec(),
        &mut sessions,
    )
    .await
    .unwrap();
    // A repeated delivery id on continuations is not another delivery start.
    receive_transfer(1, transfer(2, None, true), vec![], &mut sessions)
        .await
        .unwrap();
    assert_eq!(credit(&sessions).delivery_count, Some(0));
    assert_eq!(credit(&sessions).link_credit, Some(2047));
    apply_flow(
        1,
        Flow {
            handle: Some(2),
            delivery_count: Some(9000),
            link_credit: Some(u32::MAX),
            ..Flow::default()
        },
        &mut Vec::new(),
        &mut sessions,
        512,
    )
    .await
    .unwrap();
    assert_eq!(
        credit(&sessions).delivery_count,
        Some(0),
        "sender progress may be in flight, not received"
    );
    assert_eq!(credit(&sessions).link_credit, Some(2047));
    receive_transfer(
        1,
        transfer(2, None, false),
        encoded[1..].to_vec(),
        &mut sessions,
    )
    .await
    .unwrap();
    let delivery = receiver.recv().await.unwrap();
    let clone = delivery.clone();
    drop((delivery, clone));
    // Complete and partial aborts each consume/refund exactly one credit.
    for partial in [false, true] {
        if partial {
            receive_transfer(1, transfer(2, None, true), vec![0], &mut sessions)
                .await
                .unwrap();
        }
        let mut aborted = transfer(2, None, false);
        aborted.aborted = true;
        receive_transfer(1, aborted, vec![], &mut sessions)
            .await
            .unwrap();
    }
    assert_eq!(credit(&sessions).delivery_count, Some(2));
    assert_eq!(credit(&sessions).link_credit, Some(2045));
    incoming::replenish(1, sessions.get_mut(&1).unwrap(), &mut Vec::new())
        .await
        .unwrap();
    assert_eq!(credit(&sessions).delivery_count, Some(2));
    assert_eq!(
        credit(&sessions).link_credit,
        Some(2045),
        "unadvertised releases do not create credit"
    );
}

#[tokio::test]
async fn over_credit_refuses_but_a_continuation_at_zero_credit_still_finishes() {
    let (mut sessions, _receiver) = receiving_session(1, 2);
    let encoded = encode_message(&Message::data(vec![1])).unwrap();
    // Final start spends the last delivery credit. Abort releases capacity but
    // cannot restore permission until a real Flow advertises the refill.
    for _ in 0..2047 {
        let mut aborted = transfer(2, None, false);
        aborted.aborted = true;
        receive_transfer(1, aborted, vec![], &mut sessions)
            .await
            .unwrap();
    }
    receive_transfer(
        1,
        transfer(2, None, true),
        encoded[..1].to_vec(),
        &mut sessions,
    )
    .await
    .unwrap();
    // Grant session slots without granting link slots, to exercise the distinct
    // boundaries rather than hitting the session window first.
    assert!(sessions.get_mut(&1).unwrap().incoming.replenish());
    assert_eq!(credit(&sessions).link_credit, Some(0));
    receive_transfer(
        1,
        transfer(2, None, false),
        encoded[1..].to_vec(),
        &mut sessions,
    )
    .await
    .unwrap();
    assert!(matches!(
        receive_transfer(1, transfer(2, None, false), vec![], &mut sessions).await,
        Err(EngineError::IncomingCreditLimit)
    ));
}
