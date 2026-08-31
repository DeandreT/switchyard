use serde_amqp::primitives::Binary;

use super::*;
use crate::{AmqpError, ErrorCondition, Rejected, Source, Target, Value};

mod credit;
mod incarnation;
mod session_incarnation;

async fn test_handle_command<W: tokio::io::AsyncWrite + Unpin>(
    command: Command,
    writer: &mut W,
    sessions: &mut HashMap<u16, SessionState>,
    remote_max_frame_size: u32,
) -> Result<CommandAction, EngineError> {
    handle_command(
        command,
        writer,
        &mut HashMap::new(),
        sessions,
        remote_max_frame_size,
    )
    .await
}

type StartResponse = oneshot::Receiver<Result<DeliveryIdentity, EngineError>>;
type OutcomeResponse = oneshot::Receiver<Result<RemoteOutcome, EngineError>>;

fn test_link_drain() -> LinkDrain {
    let (notifications, _requests) = watch::channel(None);
    LinkDrain::new(notifications)
}

fn test_link_credit() -> watch::Sender<bool> {
    watch::channel(false).0
}

fn sending_session(
    channel: u16,
    handle: u32,
    delivery_id: u32,
    receiver_settle_mode: ReceiverSettleMode,
) -> (HashMap<u16, SessionState>, OutcomeResponse) {
    let (reply, outcome) = oneshot::channel();
    let permit = Arc::new(Semaphore::new(1))
        .try_acquire_owned()
        .expect("one test send fits within capacity");
    let (detached, _detached) = watch::channel(false);
    let link = SendingLink {
        receiver_settle_mode,
        settle_mode: SenderSettleMode::Mixed,
        delivery_count: 0,
        credit_limit: 0,
        queued: VecDeque::new(),
        unsettled: HashMap::from([(
            delivery_id,
            UnsettledSend {
                reply,
                _permit: permit,
            },
        )]),
        detached,
        drain: test_link_drain(),
        credit: test_link_credit(),
        credit_reservations: HashSet::new(),
        next_credit_reservation: 0,
    };
    let session = SessionState {
        incarnation: 0,
        attach_tx: None,
        pending_attaches: HashMap::new(),
        links: HashMap::from([(handle, LinkState::Sending(link))]),
        pending_flows: HashMap::new(),
        next_outgoing_id: 0,
    };
    (HashMap::from([(channel, session)]), outcome)
}

fn receiving_session(
    channel: u16,
    handle: u32,
) -> (HashMap<u16, SessionState>, mpsc::Receiver<Delivery>) {
    let (deliveries, receiver) = mpsc::channel(4);
    let (detached, _detached) = watch::channel(false);
    let link = ReceivingLink {
        incarnation: next_link_incarnation(),
        max_message_size: u64::MAX,
        deliveries,
        partial: None,
        detached,
    };
    let session = SessionState {
        incarnation: 0,
        attach_tx: None,
        pending_attaches: HashMap::new(),
        links: HashMap::from([(handle, LinkState::Receiving(link))]),
        pending_flows: HashMap::new(),
        next_outgoing_id: 0,
    };
    (HashMap::from([(channel, session)]), receiver)
}

fn transfer(handle: u32, message_format: Option<u32>, more: bool) -> Transfer {
    Transfer {
        handle,
        delivery_id: Some(1),
        delivery_tag: Some(Binary::from(vec![1])),
        message_format,
        settled: Some(false),
        more,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

fn queued_send(
    capacity: &Arc<Semaphore>,
    marker: u8,
    message_format: u32,
) -> (QueuedSend, StartResponse, OutcomeResponse) {
    let permit = Arc::clone(capacity)
        .try_acquire_owned()
        .expect("test send fits within capacity");
    let (started, start) = oneshot::channel();
    let (reply, outcome) = oneshot::channel();
    (
        QueuedSend {
            message: Message::data(vec![marker]),
            message_format,
            delivery_tag: Binary::from(vec![marker]),
            reservation: None,
            permit,
            started,
            reply,
        },
        start,
        outcome,
    )
}

fn unsettled_send(capacity: &Arc<Semaphore>) -> (UnsettledSend, OutcomeResponse) {
    let permit = Arc::clone(capacity)
        .try_acquire_owned()
        .expect("test send fits within capacity");
    let (reply, outcome) = oneshot::channel();
    (
        UnsettledSend {
            reply,
            _permit: permit,
        },
        outcome,
    )
}

fn queued_sending_session(
    channel: u16,
    handle: u32,
    credit_limit: u64,
    queued: VecDeque<QueuedSend>,
) -> HashMap<u16, SessionState> {
    let (detached, _detached) = watch::channel(false);
    let link = SendingLink {
        receiver_settle_mode: ReceiverSettleMode::First,
        settle_mode: SenderSettleMode::Unsettled,
        delivery_count: 0,
        credit_limit,
        queued,
        unsettled: HashMap::new(),
        detached,
        drain: test_link_drain(),
        credit: test_link_credit(),
        credit_reservations: HashSet::new(),
        next_credit_reservation: 0,
    };
    let session = SessionState {
        incarnation: 0,
        attach_tx: None,
        pending_attaches: HashMap::new(),
        links: HashMap::from([(handle, LinkState::Sending(link))]),
        pending_flows: HashMap::new(),
        next_outgoing_id: 0,
    };
    HashMap::from([(channel, session)])
}

#[test]
fn a_link_role_selects_the_local_endpoint() {
    assert_eq!(Role::Sender.opposite(), Role::Receiver);
    assert_eq!(Role::Receiver.opposite(), Role::Sender);
}

#[test]
fn protocol_errors_remain_link_scoped_values() {
    let error = Error::new(AmqpError::InvalidField, "bad link", None);
    assert_eq!(error.condition, AmqpError::InvalidField.into());
}

#[test]
fn delivery_tags_are_binary_and_exact() {
    let tag = Binary::from(vec![0, 1, 2, 3]);
    assert_eq!(tag.as_slice(), &[0, 1, 2, 3]);
}

#[tokio::test]
async fn an_incoming_delivery_retains_its_exact_encoded_message() {
    let channel = 3;
    let handle = 1;
    let message = Message::data(b"wire-exact".to_vec());
    let encoded = encode_message(&message).expect("message encodes");
    let split = encoded.len() / 2;
    let (mut sessions, mut deliveries) = receiving_session(channel, handle);

    receive_transfer(
        channel,
        transfer(handle, Some(0), true),
        encoded[..split].to_vec(),
        &mut sessions,
    )
    .await
    .expect("first transfer is accepted");
    let mut continuation = transfer(handle, None, false);
    continuation.delivery_id = None;
    continuation.delivery_tag = None;
    continuation.settled = None;
    receive_transfer(
        channel,
        continuation,
        encoded[split..].to_vec(),
        &mut sessions,
    )
    .await
    .expect("continuation is accepted");

    let delivery = deliveries.recv().await.expect("delivery is emitted");
    assert_eq!(delivery.message_format(), 0);
    assert_eq!(delivery.message(), &message);
    assert_eq!(delivery.encoded_message(), encoded);
}

#[tokio::test]
async fn a_fragmented_nonzero_message_format_is_preserved() {
    let channel = 3;
    let handle = 1;
    let message_format = 0x8001_3700;
    let message = Message::data(b"formatted-batch".to_vec());
    let encoded = encode_message(&message).expect("message encodes");
    let split = encoded.len() / 2;
    let (mut sessions, mut deliveries) = receiving_session(channel, handle);

    receive_transfer(
        channel,
        transfer(handle, Some(message_format), true),
        encoded[..split].to_vec(),
        &mut sessions,
    )
    .await
    .expect("a nonzero format starts a delivery");
    let mut continuation = transfer(handle, None, false);
    continuation.delivery_id = None;
    continuation.delivery_tag = None;
    continuation.settled = None;
    receive_transfer(
        channel,
        continuation,
        encoded[split..].to_vec(),
        &mut sessions,
    )
    .await
    .expect("an omitted continuation format inherits the first transfer");

    let delivery = deliveries.recv().await.expect("delivery is emitted");
    assert_eq!(delivery.message_format(), message_format);
    assert_eq!(delivery.message(), &message);
    assert_eq!(delivery.encoded_message(), encoded);
}

#[tokio::test]
async fn a_continuation_cannot_change_a_nonzero_message_format() {
    let channel = 3;
    let handle = 1;
    let (mut sessions, _deliveries) = receiving_session(channel, handle);
    receive_transfer(
        channel,
        transfer(handle, Some(0x8001_3700), true),
        vec![0],
        &mut sessions,
    )
    .await
    .expect("a nonzero format starts a delivery");
    let mut continuation = transfer(handle, Some(0x8001_3701), false);
    continuation.delivery_id = None;
    continuation.delivery_tag = None;
    let error = receive_transfer(channel, continuation, Vec::new(), &mut sessions)
        .await
        .expect_err("a continuation cannot change message format");
    assert!(matches!(error, EngineError::InvalidState(_)));
}

#[tokio::test]
async fn a_queued_send_starts_only_after_link_credit_is_consumed() {
    let channel = 3;
    let handle = 1;
    let capacity = Arc::new(Semaphore::new(1));
    let (queued, mut started, outcome) = queued_send(&capacity, 7, 0x8001_3700);
    let mut sessions = queued_sending_session(channel, handle, 0, VecDeque::from([queued]));
    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);

    flush_sends(
        channel,
        handle,
        sessions.get_mut(&channel).expect("session exists"),
        &mut wire,
        u32::MAX,
    )
    .await
    .expect("a send without credit remains queued");
    assert!(matches!(
        started.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));

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
    .expect("credit flushes the queued send");

    let identity = started
        .await
        .expect("the start acknowledgement remains live")
        .expect("the transfer starts");
    assert_eq!(identity.delivery_id(), 0);
    assert_eq!(capacity.available_permits(), 0);
    let Frame::Amqp {
        performative: Some(Performative::Transfer(transfer)),
        ..
    } = read_frame(&mut peer).await.expect("the transfer decodes")
    else {
        panic!("expected an AMQP transfer frame");
    };
    assert_eq!(transfer.message_format, Some(0x8001_3700));

    apply_disposition(
        channel,
        Disposition {
            role: Role::Receiver,
            first: identity.delivery_id(),
            last: None,
            settled: true,
            state: Some(DeliveryState::Accepted(Accepted)),
            batchable: false,
        },
        &mut sessions,
    );
    outcome
        .await
        .expect("the outcome reply remains live")
        .expect("the outcome succeeds");
    assert_eq!(capacity.available_permits(), 1);
}

#[tokio::test]
async fn three_unsettled_deliveries_complete_out_of_order() {
    let channel = 3;
    let handle = 1;
    let capacity = Arc::new(Semaphore::new(3));
    let (first, first_started, first_outcome) = queued_send(&capacity, 1, 0);
    let (second, second_started, second_outcome) = queued_send(&capacity, 2, 0);
    let (third, third_started, third_outcome) = queued_send(&capacity, 3, 0);
    let mut sessions =
        queued_sending_session(channel, handle, 3, VecDeque::from([first, second, third]));
    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);

    flush_sends(
        channel,
        handle,
        sessions.get_mut(&channel).expect("session exists"),
        &mut wire,
        u32::MAX,
    )
    .await
    .expect("all three sends consume credit");

    let first_id = first_started
        .await
        .expect("first start reply remains live")
        .expect("first send starts");
    let second_id = second_started
        .await
        .expect("second start reply remains live")
        .expect("second send starts");
    let third_id = third_started
        .await
        .expect("third start reply remains live")
        .expect("third send starts");
    assert_eq!(
        [
            first_id.delivery_id(),
            second_id.delivery_id(),
            third_id.delivery_id(),
        ],
        [0, 1, 2]
    );
    assert_eq!(capacity.available_permits(), 0);

    for expected_id in 0..3 {
        let Frame::Amqp {
            performative: Some(Performative::Transfer(transfer)),
            ..
        } = read_frame(&mut peer).await.expect("the transfer decodes")
        else {
            panic!("expected an AMQP transfer frame");
        };
        assert_eq!(transfer.delivery_id, Some(expected_id));
    }

    apply_disposition(
        channel,
        Disposition {
            role: Role::Receiver,
            first: third_id.delivery_id(),
            last: None,
            settled: true,
            state: Some(DeliveryState::Released(crate::Released)),
            batchable: false,
        },
        &mut sessions,
    );
    let third_remote = third_outcome
        .await
        .expect("third outcome reply remains live")
        .expect("third outcome succeeds");
    assert_eq!(third_remote.outcome, Outcome::Released(crate::Released));

    apply_disposition(
        channel,
        Disposition {
            role: Role::Receiver,
            first: first_id.delivery_id(),
            last: None,
            settled: true,
            state: Some(DeliveryState::Accepted(Accepted)),
            batchable: false,
        },
        &mut sessions,
    );
    let first_remote = first_outcome
        .await
        .expect("first outcome reply remains live")
        .expect("first outcome succeeds");
    assert_eq!(first_remote.outcome, Outcome::Accepted(Accepted));

    apply_disposition(
        channel,
        Disposition {
            role: Role::Receiver,
            first: second_id.delivery_id(),
            last: None,
            settled: true,
            state: Some(DeliveryState::Rejected(Rejected::default())),
            batchable: false,
        },
        &mut sessions,
    );
    let second_remote = second_outcome
        .await
        .expect("second outcome reply remains live")
        .expect("second outcome succeeds");
    assert_eq!(
        second_remote.outcome,
        Outcome::Rejected(Rejected::default())
    );
    assert_eq!(capacity.available_permits(), 3);
}

#[tokio::test]
async fn link_credit_arriving_during_attach_is_applied_when_the_link_is_accepted() {
    let channel = 3;
    let handle = 1;
    let (attach_tx, _attaches) = mpsc::channel(1);
    let mut sessions = HashMap::from([(
        channel,
        SessionState {
            incarnation: 0,
            attach_tx: Some(attach_tx),
            pending_attaches: HashMap::new(),
            links: HashMap::new(),
            pending_flows: HashMap::new(),
            next_outgoing_id: 0,
        },
    )]);
    sessions
        .get_mut(&channel)
        .expect("the session exists")
        .pending_attaches
        .insert(handle, 7);
    let (mut wire, _peer) = tokio::io::duplex(64 * 1024);

    apply_flow(
        channel,
        Flow {
            handle: Some(handle),
            delivery_count: Some(0),
            link_credit: Some(50),
            ..Flow::default()
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("an early flow is buffered");
    assert!(sessions[&channel].pending_flows.contains_key(&handle));

    let (deliveries_tx, _deliveries) = mpsc::channel(1);
    let (detached_tx, _detached) = watch::channel(false);
    let (drain_tx, _drains) = watch::channel(None);
    let (credit_tx, _credits) = watch::channel(false);
    let (reply, response) = oneshot::channel();
    test_handle_command(
        Command::AcceptLink {
            channel,
            session_incarnation: 0,
            incarnation: 7,
            attach: Box::new(Attach {
                name: String::from("response"),
                handle,
                role: Role::Receiver,
                snd_settle_mode: SenderSettleMode::Settled,
                rcv_settle_mode: ReceiverSettleMode::First,
                source: Some(Source::new("node")),
                target: Some(Target::new("reply-to")),
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: None,
                max_message_size: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            }),
            max_message_size: 1024,
            properties: None,
            deliveries_tx,
            detached_tx,
            drain_tx,
            credit_tx,
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("the link is accepted");
    response
        .await
        .expect("the accept reply remains live")
        .expect("the attach is valid");

    assert!(sessions[&channel].pending_flows.is_empty());
    let Some(LinkState::Sending(link)) = sessions[&channel].links.get(&handle) else {
        panic!("the remote receiver created a local sending link");
    };
    assert_eq!(link.credit_limit, 50);
}

#[tokio::test]
async fn drain_acknowledgement_is_generation_bound_and_returns_unused_credit() {
    let channel = 3;
    let handle = 1;
    let (notifications, mut requests) = watch::channel(None);
    let (detached, _detached) = watch::channel(false);
    let link = SendingLink {
        receiver_settle_mode: ReceiverSettleMode::First,
        settle_mode: SenderSettleMode::Unsettled,
        delivery_count: 2,
        credit_limit: 2,
        queued: VecDeque::new(),
        unsettled: HashMap::new(),
        detached,
        drain: LinkDrain::new(notifications),
        credit: test_link_credit(),
        credit_reservations: HashSet::new(),
        next_credit_reservation: 0,
    };
    let session = SessionState {
        incarnation: 0,
        attach_tx: None,
        pending_attaches: HashMap::new(),
        links: HashMap::from([(handle, LinkState::Sending(link))]),
        pending_flows: HashMap::new(),
        next_outgoing_id: 2,
    };
    let mut sessions = HashMap::from([(channel, session)]);
    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);

    for credit in [4, 5] {
        apply_flow(
            channel,
            Flow {
                handle: Some(handle),
                delivery_count: Some(2),
                link_credit: Some(credit),
                drain: true,
                ..Flow::default()
            },
            &mut wire,
            &mut sessions,
            u32::MAX,
        )
        .await
        .expect("the drain request is applied");
    }
    let current = (*requests.borrow_and_update()).expect("drain remains asserted");
    let stale = DrainRequest {
        generation: current.generation.wrapping_sub(1),
        ..current
    };

    let (reply, response) = oneshot::channel();
    test_handle_command(
        Command::Drained {
            request: stale,
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("a stale acknowledgement does not stop the connection");
    response
        .await
        .expect("stale response remains live")
        .expect("a stale generation is a benign no-op");
    assert_eq!(*requests.borrow(), Some(current));

    let (reply, response) = oneshot::channel();
    test_handle_command(
        Command::Drained {
            request: DrainRequest {
                incarnation: current.incarnation.wrapping_add(1),
                ..current
            },
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("a replaced-link acknowledgement does not stop the connection");
    assert!(matches!(
        response.await.expect("replaced-link response remains live"),
        Err(EngineError::InvalidState(_))
    ));
    assert_eq!(*requests.borrow(), Some(current));

    let (reply, response) = oneshot::channel();
    test_handle_command(
        Command::Drained {
            request: current,
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("the current drain is acknowledged");
    response
        .await
        .expect("drain response remains live")
        .expect("the drain succeeds");

    let Frame::Amqp {
        channel: written_channel,
        performative: Some(Performative::Flow(flow)),
        payload,
    } = read_frame(&mut peer).await.expect("drained flow decodes")
    else {
        panic!("expected an AMQP flow frame");
    };
    assert_eq!(written_channel, channel);
    assert!(payload.is_empty());
    assert_eq!(flow.handle, Some(handle));
    assert_eq!(flow.delivery_count, Some(7));
    assert_eq!(flow.link_credit, Some(0));
    assert_eq!(flow.next_incoming_id, Some(0));
    assert_eq!(flow.incoming_window, SESSION_WINDOW);
    assert_eq!(flow.next_outgoing_id, 2);
    assert_eq!(flow.outgoing_window, SESSION_WINDOW);
    assert_eq!(flow.available, None);
    assert!(flow.drain);
    assert!(!flow.echo);
    assert_eq!(flow.properties, None);
    assert_eq!(*requests.borrow(), None);

    let Some(LinkState::Sending(link)) = sessions[&channel].links.get(&handle) else {
        panic!("sending link remains attached");
    };
    assert_eq!(link.delivery_count, 7);
    assert_eq!(link.credit_limit, 7);
    assert!(link.drain.current.is_none());
}

#[tokio::test]
async fn a_drain_can_complete_after_queued_transfers_exhaust_credit() {
    let channel = 3;
    let handle = 1;
    let capacity = Arc::new(Semaphore::new(3));
    let (first, first_started, _first_outcome) = queued_send(&capacity, 1, 0);
    let (second, second_started, _second_outcome) = queued_send(&capacity, 2, 0);
    let (third, mut third_started, _third_outcome) = queued_send(&capacity, 3, 0);
    let mut sessions =
        queued_sending_session(channel, handle, 0, VecDeque::from([first, second, third]));
    let (notifications, requests) = watch::channel(None);
    let (credit, credit_ready) = watch::channel(false);
    let Some(LinkState::Sending(link)) = sessions
        .get_mut(&channel)
        .and_then(|session| session.links.get_mut(&handle))
    else {
        panic!("sending link exists");
    };
    link.drain = LinkDrain::new(notifications);
    link.credit = credit;
    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);

    apply_flow(
        channel,
        Flow {
            handle: Some(handle),
            delivery_count: Some(0),
            link_credit: Some(2),
            drain: true,
            ..Flow::default()
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("credit flushes two queued transfers");
    first_started
        .await
        .expect("first start remains live")
        .expect("first transfer starts");
    second_started
        .await
        .expect("second start remains live")
        .expect("second transfer starts");
    assert!(matches!(
        third_started.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(*requests.borrow(), None);
    assert!(!*credit_ready.borrow());

    for expected_id in 0..2 {
        let Frame::Amqp {
            performative: Some(Performative::Transfer(transfer)),
            ..
        } = read_frame(&mut peer).await.expect("transfer decodes")
        else {
            panic!("expected a transfer before the drained flow");
        };
        assert_eq!(transfer.delivery_id, Some(expected_id));
    }
    let Frame::Amqp {
        performative: Some(Performative::Flow(flow)),
        ..
    } = read_frame(&mut peer).await.expect("drained flow decodes")
    else {
        panic!("expected a drained flow after the transfers");
    };
    assert_eq!(flow.delivery_count, Some(2));
    assert_eq!(flow.link_credit, Some(0));
    assert_eq!(flow.handle, Some(handle));
    assert_eq!(flow.next_outgoing_id, 2);
    assert!(flow.drain);

    let session = &sessions[&channel];
    assert_eq!(session.next_outgoing_id, 2);
    let Some(LinkState::Sending(link)) = session.links.get(&handle) else {
        panic!("sending link remains attached");
    };
    assert_eq!(link.delivery_count, 2);
    assert_eq!(link.queued.len(), 1);
}

#[tokio::test]
async fn sender_drain_notification_is_sticky_and_detach_aware() {
    let channel = 3;
    let handle = 1;
    let request = DrainRequest {
        channel,
        handle,
        incarnation: 1,
        generation: 9,
    };
    let (commands, _command_rx) = mpsc::channel(1);
    let (cleanup, _cleanup_rx) = mpsc::unbounded_channel();
    let (detached_tx, detached) = watch::channel(false);
    let (drain_tx, drains) = watch::channel(Some(request));
    let sender = Sender {
        name: String::from("sender"),
        channel,
        handle,
        incarnation: request.incarnation,
        commands,
        cleanup,
        detached,
        drains,
        credits: watch::channel(false).1,
        send_capacity: Arc::new(Semaphore::new(1)),
        pending_confirmation: None,
    };

    assert_eq!(sender.on_drain().await.expect("drain is observed"), request);
    assert_eq!(
        sender.on_drain().await.expect("drain remains observable"),
        request
    );
    drain_tx
        .send(None)
        .expect("sender still observes drain notifications");
    detached_tx
        .send(true)
        .expect("sender still observes detach notifications");
    assert!(matches!(
        sender.on_drain().await,
        Err(EngineError::RemoteDetached)
    ));
}

#[tokio::test]
async fn second_mode_rejection_waits_for_an_accepted_confirmation() {
    let channel = 3;
    let handle = 1;
    let delivery_id = 7;
    let (mut sessions, outcome) =
        sending_session(channel, handle, delivery_id, ReceiverSettleMode::Second);
    let mut info = Fields::default();
    info.insert(
        Symbol::from("dead-letter-reason"),
        Value::String(String::from("InvalidOrder")),
    );
    let rejected = Rejected {
        error: Some(Error::new(
            ErrorCondition::Custom(Symbol::from("com.microsoft:dead-letter")),
            "the order has no customer",
            Some(info),
        )),
    };

    apply_disposition(
        channel,
        Disposition {
            role: Role::Receiver,
            first: delivery_id,
            last: None,
            settled: false,
            state: Some(DeliveryState::Rejected(rejected.clone())),
            batchable: true,
        },
        &mut sessions,
    );

    let remote = outcome
        .await
        .expect("the outcome reply remains live")
        .expect("the receiver outcome is valid");
    assert_eq!(remote.outcome, Outcome::Rejected(rejected));
    let confirmation = remote
        .confirmation
        .expect("second mode leaves confirmation to the application");
    assert_eq!(confirmation.delivery_id, delivery_id);
    assert_eq!(confirmation.handle, handle);
    assert!(confirmation.batchable);

    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);
    let (reply, response) = oneshot::channel();
    test_handle_command(
        Command::Confirm {
            channel,
            handle: confirmation.handle,
            incarnation: confirmation.incarnation,
            delivery_id: confirmation.delivery_id,
            state: DeliveryState::Accepted(Accepted),
            batchable: confirmation.batchable,
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("the confirmation is written");
    response
        .await
        .expect("the confirmation reply remains live")
        .expect("the confirmation succeeds");

    let Frame::Amqp {
        channel: written_channel,
        performative: Some(Performative::Disposition(disposition)),
        payload,
    } = read_frame(&mut peer)
        .await
        .expect("the disposition decodes")
    else {
        panic!("expected an AMQP disposition frame");
    };
    assert_eq!(written_channel, channel);
    assert!(payload.is_empty());
    assert_eq!(disposition.role, Role::Sender);
    assert_eq!(disposition.first, delivery_id);
    assert_eq!(disposition.last, None);
    assert!(disposition.settled);
    assert_eq!(disposition.state, Some(DeliveryState::Accepted(Accepted)));
    assert!(disposition.batchable);
}

#[tokio::test]
async fn first_mode_rejection_needs_no_confirmation() {
    let channel = 3;
    let delivery_id = 7;
    let (mut sessions, outcome) =
        sending_session(channel, 1, delivery_id, ReceiverSettleMode::First);

    apply_disposition(
        channel,
        Disposition {
            role: Role::Receiver,
            first: delivery_id,
            last: None,
            settled: false,
            state: Some(DeliveryState::Rejected(Rejected::default())),
            batchable: false,
        },
        &mut sessions,
    );

    let remote = outcome
        .await
        .expect("the outcome reply remains live")
        .expect("the receiver outcome is valid");
    assert_eq!(remote.outcome, Outcome::Rejected(Rejected::default()));
    assert!(remote.confirmation.is_none());
}

#[tokio::test]
async fn a_range_disposition_completes_only_matching_deliveries() {
    let channel = 3;
    let handle = 1;
    let capacity = Arc::new(Semaphore::new(4));
    let (before, mut before_outcome) = unsettled_send(&capacity);
    let (first, first_outcome) = unsettled_send(&capacity);
    let (middle, middle_outcome) = unsettled_send(&capacity);
    let (last, last_outcome) = unsettled_send(&capacity);
    let (detached, _detached) = watch::channel(false);
    let link = SendingLink {
        receiver_settle_mode: ReceiverSettleMode::First,
        settle_mode: SenderSettleMode::Unsettled,
        delivery_count: 4,
        credit_limit: 4,
        queued: VecDeque::new(),
        unsettled: HashMap::from([(9, before), (10, first), (11, middle), (12, last)]),
        detached,
        drain: test_link_drain(),
        credit: test_link_credit(),
        credit_reservations: HashSet::new(),
        next_credit_reservation: 0,
    };
    let session = SessionState {
        incarnation: 0,
        attach_tx: None,
        pending_attaches: HashMap::new(),
        links: HashMap::from([(handle, LinkState::Sending(link))]),
        pending_flows: HashMap::new(),
        next_outgoing_id: 13,
    };
    let mut sessions = HashMap::from([(channel, session)]);

    apply_disposition(
        channel,
        Disposition {
            role: Role::Receiver,
            first: 10,
            last: Some(12),
            settled: true,
            state: Some(DeliveryState::Accepted(Accepted)),
            batchable: true,
        },
        &mut sessions,
    );

    for outcome in [first_outcome, middle_outcome, last_outcome] {
        let remote = outcome
            .await
            .expect("the ranged reply remains live")
            .expect("the ranged outcome succeeds");
        assert_eq!(remote.outcome, Outcome::Accepted(Accepted));
    }
    assert!(matches!(
        before_outcome.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(capacity.available_permits(), 3);
}

#[tokio::test]
async fn detach_fails_queued_and_unsettled_sends_and_releases_capacity() {
    let capacity = Arc::new(Semaphore::new(2));
    let (queued, queued_started, queued_outcome) = queued_send(&capacity, 1, 0);
    let (unsettled, unsettled_outcome) = unsettled_send(&capacity);
    let (detached, _detached) = watch::channel(false);
    let mut link = LinkState::Sending(SendingLink {
        receiver_settle_mode: ReceiverSettleMode::First,
        settle_mode: SenderSettleMode::Unsettled,
        delivery_count: 1,
        credit_limit: 1,
        queued: VecDeque::from([queued]),
        unsettled: HashMap::from([(0, unsettled)]),
        detached,
        drain: test_link_drain(),
        credit: test_link_credit(),
        credit_reservations: HashSet::new(),
        next_credit_reservation: 0,
    });

    stop_link(&mut link);

    assert!(matches!(
        queued_started.await.expect("start reply remains live"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        queued_outcome.await.expect("queued reply remains live"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        unsettled_outcome
            .await
            .expect("unsettled reply remains live"),
        Err(EngineError::RemoteDetached)
    ));
    assert_eq!(capacity.available_permits(), 2);
}

#[tokio::test]
async fn a_send_command_crossing_a_detach_gets_a_detach_error() {
    let channel = 3;
    let handle = 1;
    let capacity = Arc::new(Semaphore::new(1));
    let permit = Arc::clone(&capacity)
        .try_acquire_owned()
        .expect("test send fits within capacity");
    let (started, start) = oneshot::channel();
    let (reply, outcome) = oneshot::channel();
    let mut sessions = HashMap::from([(
        channel,
        SessionState {
            incarnation: 0,
            attach_tx: None,
            pending_attaches: HashMap::new(),
            links: HashMap::new(),
            pending_flows: HashMap::new(),
            next_outgoing_id: 0,
        },
    )]);
    let (mut wire, _peer) = tokio::io::duplex(64 * 1024);

    test_handle_command(
        Command::Send {
            channel,
            handle,
            incarnation: 0,
            message: Box::new(Message::data(vec![1])),
            message_format: 0,
            delivery_tag: Binary::from(vec![1]),
            reservation: None,
            permit,
            started,
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await
    .expect("the stale send does not stop the connection");

    assert!(matches!(
        start.await.expect("start reply remains live"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        outcome.await.expect("outcome reply remains live"),
        Err(EngineError::RemoteDetached)
    ));
    assert_eq!(capacity.available_permits(), 1);
}

#[tokio::test]
async fn a_confirmation_is_sendable_and_bound_to_its_delivery_identity() {
    let identity = DeliveryIdentity {
        channel: 3,
        handle: 1,
        delivery_id: 7,
    };
    let confirmation_capacity = Arc::new(Semaphore::new(1));
    let confirmation_permit = Arc::clone(&confirmation_capacity)
        .try_acquire_owned()
        .expect("confirmation fits within capacity");
    let (commands, mut command_rx) = mpsc::channel(1);
    let (reply, response) = oneshot::channel();
    let pending = PendingDelivery {
        identity,
        response,
        commands,
    };
    assert!(
        reply
            .send(Ok(RemoteOutcome {
                outcome: Outcome::Accepted(Accepted),
                confirmation: Some(PendingConfirmation {
                    handle: identity.handle(),
                    incarnation: 11,
                    delivery_id: identity.delivery_id(),
                    batchable: true,
                    permit: confirmation_permit,
                }),
            }))
            .is_ok(),
        "the pending outcome remains live"
    );

    let outcome = pending.await.expect("the remote outcome succeeds");
    assert_eq!(outcome.identity(), identity);
    assert!(outcome.needs_confirmation());
    let (_, _, confirmation) = outcome.into_parts();
    let confirmation = confirmation.expect("second mode returns a confirmation");
    assert_eq!(confirmation.identity(), identity);
    assert_eq!(confirmation_capacity.available_permits(), 0);

    let confirmation_task = tokio::spawn(confirmation.confirm(DeliveryState::Accepted(Accepted)));
    let command = command_rx.recv().await.expect("confirmation is submitted");
    let Command::Confirm {
        channel,
        handle,
        incarnation,
        delivery_id,
        state,
        batchable,
        reply,
    } = command
    else {
        panic!("expected an identity-bound confirmation command");
    };
    assert_eq!(channel, identity.channel());
    assert_eq!(handle, identity.handle());
    assert_eq!(incarnation, 11);
    assert_eq!(delivery_id, identity.delivery_id());
    assert_eq!(state, DeliveryState::Accepted(Accepted));
    assert!(batchable);
    reply.send(Ok(())).expect("confirmation task remains live");
    confirmation_task
        .await
        .expect("confirmation task joins")
        .expect("confirmation succeeds");
    assert_eq!(confirmation_capacity.available_permits(), 1);
}
