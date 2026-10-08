use super::super::link_flow::has_unreserved_credit;
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
    let (commands, _command_rx) = mpsc::channel(8);
    let (cleanup, mut cleanup_rx) = mpsc::unbounded_channel();
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
            commands: commands.clone(),
            cleanup: cleanup.clone(),
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
    assert!(matches!(
        cleanup_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    let (reply, response) = oneshot::channel();
    test_handle_command(
        Command::ReserveCredit {
            channel,
            handle,
            incarnation,
            commands: commands.clone(),
            cleanup: cleanup.clone(),
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("credit is reserved");
    let mut guard = response
        .await
        .expect("reservation response remains live")
        .expect("reservation succeeds")
        .expect("one credit slot is available");
    let reservation = guard.identity;
    guard.disarm();
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
    let (commands, _command_rx) = mpsc::channel(8);
    let (cleanup, _cleanup_rx) = mpsc::unbounded_channel();
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
            commands: commands.clone(),
            cleanup: cleanup.clone(),
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("credit is reserved");
    let mut guard = response
        .await
        .expect("reservation response remains live")
        .expect("reservation succeeds")
        .expect("one credit slot is available");
    let reservation = guard.identity;
    guard.disarm();

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
    let (commands, mut command_rx) = mpsc::channel(1);
    let (cleanup, mut cleanup_rx) = mpsc::unbounded_channel();
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
            commands: commands.clone(),
            cleanup: cleanup.clone(),
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
    let identity = reservation.identity;
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
    drop(reservation);

    let cleanup = cleanup_rx
        .recv()
        .await
        .expect("drop always queues cleanup while the engine is live");
    let CleanupCommand::ReleaseCredit { reservation } = &cleanup;
    assert_eq!(*reservation, identity);
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

#[tokio::test]
async fn dropping_an_accepted_cached_credit_reply_owns_the_original_cleanup() {
    let channel = 3;
    let handle = 1;
    let mut sessions = queued_sending_session(channel, handle, 1, VecDeque::new());
    let Some(LinkState::Sending(link)) = sessions[&channel].links.get(&handle) else {
        panic!("sending link exists");
    };
    let incarnation = link.drain.incarnation;
    let (commands, _command_rx) = mpsc::channel(1);
    let (cleanup, mut cleanup_rx) = mpsc::unbounded_channel();
    let (reply, cached_reply) = oneshot::channel();
    let (mut wire, _peer) = tokio::io::duplex(64 * 1024);
    test_handle_command(
        Command::ReserveCredit {
            channel,
            handle,
            incarnation,
            commands,
            cleanup: cleanup.clone(),
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("actor accepted the original reply");
    let Some(LinkState::Sending(link)) = sessions[&channel].links.get(&handle) else {
        panic!("sending link remains attached");
    };
    assert_eq!(link.credit_reservations.len(), 1);
    let original_id = *link.credit_reservations.iter().next().unwrap();
    assert!(!has_unreserved_credit(link));
    assert!(matches!(
        cleanup_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    // No receiver poll observed the accepted reply's actual guard.
    drop(cached_reply);
    let cleanup = cleanup_rx
        .try_recv()
        .expect("cached guard Drop queued cleanup");
    let CleanupCommand::ReleaseCredit { reservation } = &cleanup;
    assert_eq!(reservation.channel, channel);
    assert_eq!(reservation.handle, handle);
    assert_eq!(reservation.incarnation, incarnation);
    assert_eq!(reservation.reservation_id, original_id);
    super::super::handle_cleanup(cleanup, &mut wire, &mut sessions)
        .await
        .unwrap();
    let Some(LinkState::Sending(link)) = sessions[&channel].links.get(&handle) else {
        panic!("sending link remains attached");
    };
    assert!(link.credit_reservations.is_empty());
    assert!(has_unreserved_credit(link));
    assert!(matches!(
        cleanup_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}
