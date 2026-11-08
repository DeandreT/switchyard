use super::*;

fn limited_session(
    channel: u16,
    handle: u32,
    queued: QueuedSend,
    max_message_size: Option<u64>,
) -> HashMap<u16, SessionState> {
    let mut sessions = queued_sending_session(channel, handle, 1, VecDeque::from([queued]));
    let Some(LinkState::Sending(link)) = sessions
        .get_mut(&channel)
        .and_then(|session| session.links.get_mut(&handle))
    else {
        panic!("the fixture holds one sending link");
    };
    link.max_message_size = max_message_size;
    sessions
}

fn encoded_size(marker: u8) -> u64 {
    encode_message(&Message::data(vec![marker]))
        .expect("the test message encodes")
        .len() as u64
}

#[test]
fn absent_zero_and_maximum_peer_limits_are_unlimited() {
    let mut attach = Attach {
        name: String::from("link"),
        handle: 0,
        role: Role::Receiver,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: None,
        target: None,
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: None,
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    };
    assert_eq!(super::super::super::peer_message_limit(&attach), None);
    attach.max_message_size = Some(0);
    assert_eq!(super::super::super::peer_message_limit(&attach), None);
    attach.max_message_size = Some(u64::MAX);
    assert_eq!(super::super::super::peer_message_limit(&attach), None);
    attach.max_message_size = Some(1024);
    assert_eq!(super::super::super::peer_message_limit(&attach), Some(1024));
}

#[tokio::test]
async fn a_delivery_exactly_at_the_peer_limit_is_transferred() {
    let (channel, handle) = (3, 1);
    let capacity = Arc::new(Semaphore::new(1));
    let (queued, started, _outcome) = queued_send(&capacity, 7, 0);
    let mut sessions = limited_session(channel, handle, queued, Some(encoded_size(7)));
    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);

    flush_sends(
        channel,
        handle,
        sessions.get_mut(&channel).expect("session exists"),
        &mut wire,
        u32::MAX,
    )
    .await
    .expect("the flush succeeds");

    started
        .await
        .expect("the start acknowledgement remains live")
        .expect("a delivery at the limit starts");
    let frame = read_frame(&mut peer).await.expect("the transfer decodes");
    assert!(matches!(
        frame,
        Frame::Amqp {
            performative: Some(Performative::Transfer(_)),
            ..
        }
    ));
}

#[tokio::test]
async fn a_delivery_one_byte_over_the_peer_limit_detaches_the_link_instead() {
    let (channel, handle) = (3, 1);
    let capacity = Arc::new(Semaphore::new(1));
    let (queued, started, outcome) = queued_send(&capacity, 7, 0);
    let maximum = encoded_size(7) - 1;
    let mut sessions = limited_session(channel, handle, queued, Some(maximum));
    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);

    flush_sends(
        channel,
        handle,
        sessions.get_mut(&channel).expect("session exists"),
        &mut wire,
        u32::MAX,
    )
    .await
    .expect("refusing one delivery is link scoped");

    assert!(matches!(
        started.await.expect("the start acknowledgement remains live"),
        Err(EngineError::MessageSizeExceeded { size, maximum: limit })
            if size == maximum + 1 && limit == maximum
    ));
    assert!(matches!(
        outcome.await.expect("the outcome reply remains live"),
        Err(EngineError::MessageSizeExceeded { .. })
    ));
    assert_eq!(capacity.available_permits(), 1);
    let Frame::Amqp {
        performative: Some(Performative::Detach(detach)),
        ..
    } = read_frame(&mut peer).await.expect("the detach decodes")
    else {
        panic!("an oversized delivery must not reach the wire as a transfer");
    };
    assert_eq!(detach.handle, handle);
    assert!(detach.closed);
    assert_eq!(
        detach.error.expect("the detach explains itself").condition,
        ErrorCondition::Custom(Symbol::from("amqp:link:message-size-exceeded"))
    );
    let session = sessions.get(&channel).expect("the session survives");
    assert!(!session.links.contains_key(&handle));
    assert_eq!(session.next_delivery_id, 0, "no delivery id was consumed");
}
