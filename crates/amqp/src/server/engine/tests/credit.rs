use super::*;

#[tokio::test]
async fn a_reserved_slot_survives_credit_revoke_and_drain_until_sent() {
    let channel = 3;
    let handle = 1;
    let mut sessions = queued_sending_session(channel, handle, 0, VecDeque::new());
    let (drain_tx, drains) = watch::channel(None);
    let (credit_tx, credit_ready) = watch::channel(false);
    let Some(LinkState::Sending(link)) = sessions
        .get_mut(&channel)
        .and_then(|session| session.links.get_mut(&handle))
    else {
        panic!("sending link exists");
    };
    link.drain = LinkDrain::new(drain_tx);
    link.credit = credit_tx;
    let incarnation = link.drain.incarnation;
    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);

    apply_flow(
        channel,
        Flow {
            handle: Some(handle),
            delivery_count: Some(0),
            link_credit: Some(1),
            ..Flow::default()
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("credit is applied");
    assert!(*credit_ready.borrow());

    let (reply, canceled) = oneshot::channel();
    drop(canceled);
    test_handle_command(
        Command::ReserveCredit {
            channel,
            handle,
            incarnation,
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("a canceled reservation is rolled back");
    let Some(LinkState::Sending(link)) = sessions[&channel].links.get(&handle) else {
        panic!("sending link remains attached");
    };
    assert!(link.credit_reservations.is_empty());
    assert!(*credit_ready.borrow());

    let (reply, response) = oneshot::channel();
    test_handle_command(
        Command::ReserveCredit {
            channel,
            handle,
            incarnation,
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("credit is reserved");
    let reservation = response
        .await
        .expect("reservation response remains live")
        .expect("reservation succeeds")
        .expect("one credit slot is available");
    assert!(!*credit_ready.borrow());

    apply_flow(
        channel,
        Flow {
            handle: Some(handle),
            delivery_count: Some(0),
            link_credit: Some(0),
            drain: true,
            ..Flow::default()
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("the drain waits for reserved work");
    let drain = (*drains.borrow()).expect("the drain remains sticky");

    let permit = Arc::new(Semaphore::new(1))
        .try_acquire_owned()
        .expect("the test send fits");
    let (started, start) = oneshot::channel();
    let (reply, _outcome) = oneshot::channel();
    test_handle_command(
        Command::Send {
            channel,
            handle,
            incarnation,
            message: Box::new(Message::data(vec![7])),
            message_format: 0,
            delivery_tag: Binary::from(vec![7]),
            reservation: Some(reservation),
            permit,
            started,
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("reserved work is written before the drain completes");
    assert_eq!(
        start
            .await
            .expect("start response remains live")
            .expect("reserved send starts")
            .delivery_id(),
        0
    );

    let Frame::Amqp {
        performative: Some(Performative::Transfer(transfer)),
        ..
    } = read_frame(&mut peer)
        .await
        .expect("reserved transfer decodes")
    else {
        panic!("expected the reserved transfer");
    };
    assert_eq!(transfer.delivery_id, Some(0));
    let Frame::Amqp {
        performative: Some(Performative::Flow(flow)),
        ..
    } = read_frame(&mut peer)
        .await
        .expect("automatic drain flow decodes")
    else {
        panic!("expected the automatic drain flow");
    };
    assert_eq!(flow.handle, Some(handle));
    assert_eq!(flow.delivery_count, Some(1));
    assert_eq!(flow.link_credit, Some(0));
    assert_eq!(flow.next_outgoing_id, 1);
    assert!(flow.drain);
    assert_eq!(*drains.borrow(), None);

    let (reply, response) = oneshot::channel();
    test_handle_command(
        Command::Drained {
            request: drain,
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("the raced acknowledgement is harmless");
    response
        .await
        .expect("drain response remains live")
        .expect("an auto-completed drain is a no-op");
}

#[tokio::test]
async fn releasing_empty_source_reservation_completes_a_zero_credit_drain() {
    let channel = 3;
    let handle = 1;
    let mut sessions = queued_sending_session(channel, handle, 0, VecDeque::new());
    let (drain_tx, drains) = watch::channel(None);
    let (credit_tx, credit_ready) = watch::channel(false);
    let Some(LinkState::Sending(link)) = sessions
        .get_mut(&channel)
        .and_then(|session| session.links.get_mut(&handle))
    else {
        panic!("sending link exists");
    };
    link.drain = LinkDrain::new(drain_tx);
    link.credit = credit_tx;
    let incarnation = link.drain.incarnation;
    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);

    apply_flow(
        channel,
        Flow {
            handle: Some(handle),
            delivery_count: Some(0),
            link_credit: Some(1),
            ..Flow::default()
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("credit is applied");
    let (reply, response) = oneshot::channel();
    test_handle_command(
        Command::ReserveCredit {
            channel,
            handle,
            incarnation,
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("credit is reserved");
    let reservation = response
        .await
        .expect("reservation response remains live")
        .expect("reservation succeeds")
        .expect("one credit slot is available");

    apply_flow(
        channel,
        Flow {
            handle: Some(handle),
            delivery_count: Some(0),
            link_credit: Some(0),
            drain: true,
            ..Flow::default()
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("the zero-credit drain waits for the reservation");
    assert!(drains.borrow().is_some());

    let (reply, response) = oneshot::channel();
    test_handle_command(
        Command::ReleaseCredit {
            reservation,
            reply: Some(reply),
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("empty-source credit is released");
    response
        .await
        .expect("release response remains live")
        .expect("release succeeds");

    let Frame::Amqp {
        channel: written_channel,
        performative: Some(Performative::Flow(flow)),
        payload,
    } = read_frame(&mut peer)
        .await
        .expect("automatic drain flow decodes")
    else {
        panic!("expected an automatic drain flow");
    };
    assert_eq!(written_channel, channel);
    assert!(payload.is_empty());
    assert_eq!(flow.handle, Some(handle));
    assert_eq!(flow.delivery_count, Some(0));
    assert_eq!(flow.link_credit, Some(0));
    assert_eq!(flow.next_incoming_id, Some(0));
    assert_eq!(flow.incoming_window, SESSION_WINDOW);
    assert_eq!(flow.next_outgoing_id, 0);
    assert_eq!(flow.outgoing_window, SESSION_WINDOW);
    assert!(flow.drain);
    assert!(!flow.echo);
    assert_eq!(*drains.borrow(), None);
    assert!(!*credit_ready.borrow());
}

#[tokio::test]
async fn dropping_a_reservation_bypasses_a_full_command_queue() {
    let channel = 3;
    let handle = 1;
    let mut sessions = queued_sending_session(channel, handle, 0, VecDeque::new());
    let (drain_tx, drains) = watch::channel(None);
    let Some(LinkState::Sending(link)) = sessions
        .get_mut(&channel)
        .and_then(|session| session.links.get_mut(&handle))
    else {
        panic!("sending link exists");
    };
    link.drain = LinkDrain::new(drain_tx);
    let incarnation = link.drain.incarnation;
    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);

    apply_flow(
        channel,
        Flow {
            handle: Some(handle),
            delivery_count: Some(0),
            link_credit: Some(1),
            ..Flow::default()
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("credit is applied");
    let (reply, response) = oneshot::channel();
    test_handle_command(
        Command::ReserveCredit {
            channel,
            handle,
            incarnation,
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("credit is reserved");
    let identity = response
        .await
        .expect("reservation response remains live")
        .expect("reservation succeeds")
        .expect("one credit slot is available");
    apply_flow(
        channel,
        Flow {
            handle: Some(handle),
            delivery_count: Some(0),
            link_credit: Some(0),
            drain: true,
            ..Flow::default()
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("the drain waits for the reservation");
    assert!(drains.borrow().is_some());

    let (commands, mut command_rx) = mpsc::channel(1);
    let (close_reply, _close_response) = oneshot::channel();
    assert!(
        commands
            .try_send(Command::Close {
                error: None,
                reply: close_reply,
            })
            .is_ok()
    );
    assert_eq!(commands.capacity(), 0);
    let (cleanup, mut cleanup_rx) = mpsc::unbounded_channel();
    drop(CreditReservation {
        identity,
        commands,
        cleanup,
        active: true,
    });

    let cleanup = cleanup_rx
        .recv()
        .await
        .expect("drop always queues cleanup while the engine is live");
    super::super::handle_cleanup(cleanup, &mut wire, &mut sessions)
        .await
        .expect("cleanup releases the reserved slot");
    assert!(matches!(
        command_rx.try_recv(),
        Ok(Command::Close { error: None, .. })
    ));

    let Frame::Amqp {
        performative: Some(Performative::Flow(flow)),
        ..
    } = read_frame(&mut peer)
        .await
        .expect("automatic drain flow decodes")
    else {
        panic!("expected an automatic drain flow");
    };
    assert_eq!(flow.handle, Some(handle));
    assert_eq!(flow.delivery_count, Some(0));
    assert_eq!(flow.link_credit, Some(0));
    assert!(flow.drain);
    assert_eq!(*drains.borrow(), None);
    let Some(LinkState::Sending(link)) = sessions[&channel].links.get(&handle) else {
        panic!("sending link remains attached");
    };
    assert!(link.credit_reservations.is_empty());
}
