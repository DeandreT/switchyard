use super::*;

fn flow(ack: u32, window: u32) -> Flow {
    Flow {
        next_incoming_id: Some(ack),
        incoming_window: window,
        outgoing_window: SESSION_WINDOW,
        ..Flow::default()
    }
}

#[tokio::test]
async fn failed_or_canceled_active_writes_keep_the_original_closed_reply_classification() {
    for canceled in [false, true] {
        let capacity = Arc::new(Semaphore::new(1));
        let (queued, start, outcome) = queued_send(&capacity, 1, 0);
        let mut sessions = queued_sending_session(1, 1, 1, VecDeque::from([queued]));
        let session = sessions.get_mut(&1).unwrap();
        let (mut writer, peer) = tokio::io::duplex(1);
        if canceled {
            let mut write = Box::pin(flush_sends(1, 1, session, &mut writer, 1024));
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(write.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            drop(write);
            drop(peer);
        } else {
            drop(peer);
            assert!(matches!(
                flush_sends(1, 1, session, &mut writer, 1024).await,
                Err(EngineError::Io(_))
            ));
        }
        stop_link(session.links.get_mut(&1).unwrap());
        // Sender::send_pending maps this original channel closure to Stopped,
        // preserving the pre-window active-write result (not RemoteDetached).
        assert!(start.await.is_err());
        assert!(outcome.await.is_err());
        assert_eq!(capacity.available_permits(), 1);
    }
}

#[tokio::test]
async fn pausing_a_partial_send_publishes_spare_link_credit_without_another_command() {
    let capacity = Arc::new(Semaphore::new(1));
    let (mut queued, mut start, mut outcome) = queued_send(&capacity, 1, 0);
    queued.message = Message::data(vec![1; 1024]);
    let mut sessions = queued_sending_session(1, 1, 2, VecDeque::from([queued]));
    let session = sessions.get_mut(&1).unwrap();
    session.outgoing = OutgoingWindow::new(0, 1);
    let LinkState::Sending(link) = session.links.get(&1).unwrap() else {
        unreachable!()
    };
    let credit = link.credit.subscribe();
    assert!(!*credit.borrow());
    let mut bytes = Vec::new();
    flush_sends(1, 1, session, &mut bytes, 512).await.unwrap();
    assert!(
        *credit.borrow(),
        "partial sends do not hide the remaining link slot"
    );
    assert!(start.try_recv().is_err());
    assert!(outcome.try_recv().is_err());
    assert_eq!(capacity.available_permits(), 0);
    assert_eq!(session.next_delivery_id, 1);
    assert_eq!(session.outgoing.next_id(), 1);
    stop_link(session.links.get_mut(&1).unwrap());
    assert!(matches!(
        start.await.unwrap(),
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        outcome.await.unwrap(),
        Err(EngineError::RemoteDetached)
    ));
    assert_eq!(capacity.available_permits(), 1);
}

#[tokio::test]
async fn early_outcome_keeps_the_original_permit_and_start_until_the_last_fragment() {
    let capacity = Arc::new(Semaphore::new(1));
    let (mut queued, mut start, mut outcome) = queued_send(&capacity, 1, 0);
    queued.message = Message::data(vec![1; 1024]);
    let mut sessions = queued_sending_session(1, 1, 1, VecDeque::from([queued]));
    sessions.get_mut(&1).unwrap().outgoing = OutgoingWindow::new(0, 1);
    let mut bytes = Vec::new();
    flush_sends(1, 1, sessions.get_mut(&1).unwrap(), &mut bytes, 512)
        .await
        .unwrap();
    apply_disposition(
        1,
        Disposition {
            role: Role::Receiver,
            first: 0,
            last: None,
            settled: true,
            state: Some(DeliveryState::Accepted(Accepted)),
            batchable: false,
        },
        &mut sessions,
    );
    assert!(start.try_recv().is_err());
    assert!(outcome.try_recv().is_err());
    assert_eq!(
        capacity.available_permits(),
        0,
        "an early outcome cannot free a retained partial send"
    );
    apply_session_flow(1, flow(1, 4096), &mut bytes, &mut sessions, 512)
        .await
        .unwrap();
    assert_eq!(start.await.unwrap().unwrap().delivery_id, 0);
    assert!(matches!(
        outcome.await.unwrap().unwrap().outcome,
        Outcome::Accepted(_)
    ));
    assert_eq!(capacity.available_permits(), 1);
    assert_eq!(sessions[&1].next_delivery_id, 1);
    assert!(sessions[&1].outgoing.next_id() > 1);
}

#[tokio::test]
async fn emitted_transfers_wrap_frame_ids_but_preserve_the_delivery_sequence() {
    let capacity = Arc::new(Semaphore::new(1));
    let (mut queued, start, _outcome) = queued_send(&capacity, 1, 0);
    queued.message = Message::data(vec![1; 700]);
    let mut sessions = queued_sending_session(1, 1, 1, VecDeque::from([queued]));
    let session = sessions.get_mut(&1).unwrap();
    session.outgoing = OutgoingWindow::new(u32::MAX, 1);
    let mut bytes = Vec::new();
    flush_sends(1, 1, session, &mut bytes, 1024).await.unwrap();
    assert_eq!(session.outgoing.next_id(), 0);
    apply_session_flow(1, flow(0, 1), &mut bytes, &mut sessions, 1024)
        .await
        .unwrap();
    assert_eq!(sessions[&1].outgoing.next_id(), 1);
    assert_eq!(start.await.unwrap().unwrap().delivery_id, 0);
    let mut input = bytes.as_slice();
    let Frame::Amqp {
        performative: Some(Performative::Transfer(first)),
        ..
    } = read_frame(&mut input).await.unwrap()
    else {
        panic!("first transfer")
    };
    let Frame::Amqp {
        performative: Some(Performative::Transfer(last)),
        ..
    } = read_frame(&mut input).await.unwrap()
    else {
        panic!("last transfer")
    };
    assert_eq!(first.delivery_id, Some(0));
    assert!(first.more);
    assert_eq!(last.delivery_id, None);
    assert!(!last.more);
    assert!(input.is_empty());
}

#[tokio::test]
async fn cached_link_flow_does_not_replay_its_old_session_acknowledgement() {
    let (mut sessions, _) = receiving_session(1, 9);
    let session = sessions.get_mut(&1).unwrap();
    session.pending_attaches.insert(2, next_link_incarnation());
    session.outgoing = OutgoingWindow::new(0, 2);
    session.outgoing.sent().unwrap();
    let cached = Flow {
        handle: Some(2),
        delivery_count: Some(0),
        link_credit: Some(1),
        ..flow(0, 0)
    };
    let mut bytes = Vec::new();
    apply_session_flow(1, cached, &mut bytes, &mut sessions, 512)
        .await
        .unwrap();
    apply_session_flow(1, flow(1, 1), &mut bytes, &mut sessions, 512)
        .await
        .unwrap();
    let session = sessions.get_mut(&1).unwrap();
    let cached = session.pending_flows.remove(&2).unwrap();
    // AcceptLink replays the cached link hints through apply_flow only.
    apply_flow(1, cached, &mut bytes, &mut sessions, 512)
        .await
        .unwrap();
    assert_eq!(sessions[&1].outgoing.next_id(), 1);
    assert!(sessions[&1].outgoing.has_capacity());
    assert!(
        sessions
            .get_mut(&1)
            .unwrap()
            .outgoing
            .update(&flow(0, u32::MAX))
            .is_err()
    );
}
