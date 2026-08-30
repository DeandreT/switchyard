use serde_amqp::primitives::Binary;

use super::*;
use crate::{AmqpError, ErrorCondition, Rejected, Source, Target, Value};

fn sending_session(
    channel: u16,
    handle: u32,
    delivery_id: u32,
    receiver_settle_mode: ReceiverSettleMode,
) -> (
    HashMap<u16, SessionState>,
    oneshot::Receiver<Result<RemoteOutcome, EngineError>>,
) {
    let (reply, outcome) = oneshot::channel();
    let (detached, _detached) = watch::channel(false);
    let link = SendingLink {
        receiver_settle_mode,
        settle_mode: SenderSettleMode::Mixed,
        delivery_count: 0,
        credit_limit: 0,
        queued: VecDeque::new(),
        unsettled: HashMap::from([(delivery_id, reply)]),
        detached,
    };
    let session = SessionState {
        attach_tx: None,
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
        max_message_size: u64::MAX,
        deliveries,
        partial: None,
        detached,
    };
    let session = SessionState {
        attach_tx: None,
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
    assert_eq!(delivery.message(), &message);
    assert_eq!(delivery.encoded_message(), encoded);
}

#[tokio::test]
async fn unsupported_message_formats_are_rejected_on_first_and_continuation_transfers() {
    let channel = 3;
    let handle = 1;
    let (mut sessions, _deliveries) = receiving_session(channel, handle);
    let error = receive_transfer(
        channel,
        transfer(handle, Some(1), false),
        Vec::new(),
        &mut sessions,
    )
    .await
    .expect_err("a non-zero first message format is unsupported");
    assert!(matches!(error, EngineError::InvalidState(_)));

    let (mut sessions, _deliveries) = receiving_session(channel, handle);
    receive_transfer(
        channel,
        transfer(handle, Some(0), true),
        vec![0],
        &mut sessions,
    )
    .await
    .expect("format zero starts a delivery");
    let mut continuation = transfer(handle, Some(1), false);
    continuation.delivery_id = None;
    continuation.delivery_tag = None;
    let error = receive_transfer(channel, continuation, Vec::new(), &mut sessions)
        .await
        .expect_err("a continuation cannot change to a non-zero message format");
    assert!(matches!(error, EngineError::InvalidState(_)));
}

#[tokio::test]
async fn link_credit_arriving_during_attach_is_applied_when_the_link_is_accepted() {
    let channel = 3;
    let handle = 1;
    let (attach_tx, _attaches) = mpsc::channel(1);
    let mut sessions = HashMap::from([(
        channel,
        SessionState {
            attach_tx: Some(attach_tx),
            links: HashMap::new(),
            pending_flows: HashMap::new(),
            next_outgoing_id: 0,
        },
    )]);
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
    let (reply, response) = oneshot::channel();
    handle_command(
        Command::AcceptLink {
            channel,
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
    handle_command(
        Command::Confirm {
            channel,
            handle: confirmation.handle,
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
