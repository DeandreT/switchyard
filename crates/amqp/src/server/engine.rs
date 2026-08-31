use super::*;

mod link_flow;

pub(super) use link_flow::{LinkDrain, apply_flow};
use link_flow::{
    acknowledge_drain, complete_zero_credit_drain, publish_credit, release_credit, reserve_credit,
};

pub(super) enum Command {
    AcceptSession {
        channel: u16,
        incarnation: u64,
        attach_tx: mpsc::Sender<IncomingAttach>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    AcceptLink {
        channel: u16,
        session_incarnation: u64,
        incarnation: u64,
        attach: Box<Attach>,
        max_message_size: u64,
        properties: Option<Fields>,
        deliveries_tx: mpsc::Sender<Delivery>,
        detached_tx: watch::Sender<bool>,
        drain_tx: watch::Sender<Option<DrainRequest>>,
        credit_tx: watch::Sender<bool>,
        reply: oneshot::Sender<Result<u64, EngineError>>,
    },
    Send {
        channel: u16,
        handle: u32,
        incarnation: u64,
        message: Box<Message>,
        message_format: u32,
        delivery_tag: DeliveryTag,
        reservation: Option<CreditReservationIdentity>,
        permit: OwnedSemaphorePermit,
        started: oneshot::Sender<Result<DeliveryIdentity, EngineError>>,
        reply: oneshot::Sender<Result<RemoteOutcome, EngineError>>,
    },
    ReserveCredit {
        channel: u16,
        handle: u32,
        incarnation: u64,
        reply: oneshot::Sender<Result<Option<CreditReservationIdentity>, EngineError>>,
    },
    ReleaseCredit {
        reservation: CreditReservationIdentity,
        reply: Option<oneshot::Sender<Result<(), EngineError>>>,
    },
    Confirm {
        channel: u16,
        handle: u32,
        incarnation: u64,
        delivery_id: u32,
        state: DeliveryState,
        batchable: bool,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Drained {
        request: DrainRequest,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Settle {
        channel: u16,
        handle: u32,
        incarnation: u64,
        delivery_id: u32,
        state: DeliveryState,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Detach {
        channel: u16,
        handle: u32,
        incarnation: u64,
        error: Option<Error>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Close {
        error: Option<Error>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
}

pub(super) enum CleanupCommand {
    ReleaseCredit {
        reservation: CreditReservationIdentity,
    },
}

pub(super) struct SessionState {
    pub(super) incarnation: u64,
    pub(super) attach_tx: Option<mpsc::Sender<IncomingAttach>>,
    pub(super) pending_attaches: HashMap<u32, u64>,
    pub(super) links: HashMap<u32, LinkState>,
    pub(super) pending_flows: HashMap<u32, Flow>,
    pub(super) next_outgoing_id: u32,
}

pub(super) enum LinkState {
    Sending(SendingLink),
    Receiving(ReceivingLink),
}

impl LinkState {
    pub(super) fn incarnation(&self) -> u64 {
        match self {
            Self::Sending(link) => link.drain.incarnation,
            Self::Receiving(link) => link.incarnation,
        }
    }
}

pub(super) struct SendingLink {
    pub(super) receiver_settle_mode: ReceiverSettleMode,
    pub(super) settle_mode: SenderSettleMode,
    pub(super) delivery_count: u32,
    pub(super) credit_limit: u64,
    pub(super) queued: VecDeque<QueuedSend>,
    pub(super) unsettled: HashMap<u32, UnsettledSend>,
    pub(super) detached: watch::Sender<bool>,
    pub(super) drain: LinkDrain,
    pub(super) credit: watch::Sender<bool>,
    pub(super) credit_reservations: HashSet<u64>,
    pub(super) next_credit_reservation: u64,
}

pub(super) struct QueuedSend {
    pub(super) message: Message,
    pub(super) message_format: u32,
    pub(super) delivery_tag: DeliveryTag,
    pub(super) reservation: Option<CreditReservationIdentity>,
    pub(super) permit: OwnedSemaphorePermit,
    pub(super) started: oneshot::Sender<Result<DeliveryIdentity, EngineError>>,
    pub(super) reply: oneshot::Sender<Result<RemoteOutcome, EngineError>>,
}

pub(super) struct UnsettledSend {
    pub(super) reply: oneshot::Sender<Result<RemoteOutcome, EngineError>>,
    pub(super) _permit: OwnedSemaphorePermit,
}

pub(super) struct RemoteOutcome {
    pub(super) outcome: Outcome,
    pub(super) confirmation: Option<PendingConfirmation>,
}

pub(super) struct PendingConfirmation {
    pub(super) handle: u32,
    pub(super) incarnation: u64,
    pub(super) delivery_id: u32,
    pub(super) batchable: bool,
    pub(super) permit: OwnedSemaphorePermit,
}

pub(super) struct ReceivingLink {
    pub(super) incarnation: u64,
    pub(super) max_message_size: u64,
    pub(super) deliveries: mpsc::Sender<Delivery>,
    pub(super) partial: Option<PartialDelivery>,
    pub(super) detached: watch::Sender<bool>,
}

pub(super) struct PartialDelivery {
    id: u32,
    settled: bool,
    message_format: u32,
    bytes: Vec<u8>,
}

pub(super) async fn run_connection<Io>(
    stream: Io,
    remote_max_frame_size: u32,
    mut commands: mpsc::Receiver<Command>,
    mut cleanup: mpsc::UnboundedReceiver<CleanupCommand>,
    incoming_sessions: mpsc::Sender<IncomingSession>,
) where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (frames_tx, mut frames) = mpsc::channel(256);
    tokio::spawn(async move {
        loop {
            let frame = read_frame(&mut reader).await;
            let done = frame.is_err();
            if frames_tx.send(frame).await.is_err() || done {
                break;
            }
        }
    });

    let mut sessions = HashMap::<u16, SessionState>::new();
    let mut pending_sessions = HashMap::<u16, u64>::new();
    let mut closing_reply: Option<oneshot::Sender<Result<(), EngineError>>> = None;
    let mut cleanup_open = true;
    loop {
        tokio::select! {
            biased;
            cleanup_command = cleanup.recv(), if cleanup_open => {
                match cleanup_command {
                    Some(cleanup_command) => {
                        if handle_cleanup(cleanup_command, &mut writer, &mut sessions).await.is_err() {
                            break;
                        }
                    }
                    None => cleanup_open = false,
                }
            }
            frame = frames.recv() => {
                let Some(frame) = frame else { break };
                let Ok(frame) = frame else { break };
                match handle_frame(
                    frame,
                    &mut writer,
                    &incoming_sessions,
                    &mut pending_sessions,
                    &mut sessions,
                    remote_max_frame_size,
                ).await {
                    Ok(FrameAction::Continue) => {}
                    Ok(FrameAction::Closed) => {
                        if let Some(reply) = closing_reply.take() {
                            let _ = reply.send(Ok(()));
                        }
                        break;
                    }
                    Err(_) => break,
                }
            }
            command = commands.recv() => {
                let Some(command) = command else { break };
                match handle_command(
                    command,
                    &mut writer,
                    &mut pending_sessions,
                    &mut sessions,
                    remote_max_frame_size,
                ).await {
                    Ok(CommandAction::Continue) => {}
                    Ok(CommandAction::Closing(reply)) => closing_reply = Some(reply),
                    Err(_) => break,
                }
            }
        }
    }

    for session in sessions.values_mut() {
        for link in session.links.values_mut() {
            stop_link(link);
        }
    }
    if let Some(reply) = closing_reply {
        let _ = reply.send(Err(EngineError::RemoteClosed));
    }
}

async fn handle_cleanup<W: AsyncWrite + Unpin>(
    command: CleanupCommand,
    writer: &mut W,
    sessions: &mut HashMap<u16, SessionState>,
) -> Result<(), EngineError> {
    match command {
        CleanupCommand::ReleaseCredit { reservation } => {
            release_credit(reservation, None, sessions, writer).await
        }
    }
}

enum FrameAction {
    Continue,
    Closed,
}

async fn handle_frame<W: AsyncWrite + Unpin>(
    frame: Frame,
    writer: &mut W,
    incoming_sessions: &mpsc::Sender<IncomingSession>,
    pending_sessions: &mut HashMap<u16, u64>,
    sessions: &mut HashMap<u16, SessionState>,
    remote_max_frame_size: u32,
) -> Result<FrameAction, EngineError> {
    let Frame::Amqp {
        channel,
        performative,
        payload,
    } = frame
    else {
        return Err(invalid_state("SASL frame after AMQP open"));
    };
    let Some(performative) = performative else {
        return Ok(FrameAction::Continue);
    };

    match performative {
        Performative::Begin(begin) => {
            if sessions.contains_key(&channel) || pending_sessions.contains_key(&channel) {
                return Err(invalid_state("duplicate AMQP session channel"));
            }
            let incarnation = next_session_incarnation();
            pending_sessions.insert(channel, incarnation);
            incoming_sessions
                .send(IncomingSession {
                    channel,
                    incarnation,
                    begin,
                })
                .await
                .map_err(|_| EngineError::Stopped)?;
        }
        Performative::Attach(attach) => {
            let attach = *attach;
            let session = sessions
                .get_mut(&channel)
                .ok_or_else(|| invalid_state("attach on an unknown session"))?;
            let handle = attach.handle;
            if session.links.contains_key(&handle) || session.pending_attaches.contains_key(&handle)
            {
                return Err(invalid_state("link handle is already attached"));
            }
            let incarnation = next_link_incarnation();
            session.pending_attaches.insert(handle, incarnation);
            session
                .attach_tx
                .as_ref()
                .ok_or_else(|| invalid_state("session cannot accept remote links"))?
                .send(IncomingAttach {
                    session_incarnation: session.incarnation,
                    incarnation,
                    attach,
                })
                .await
                .map_err(|_| EngineError::Stopped)?;
        }
        Performative::Flow(flow) => {
            apply_flow(channel, flow, writer, sessions, remote_max_frame_size).await?;
        }
        Performative::Transfer(transfer) => {
            receive_transfer(channel, transfer, payload, sessions).await?;
        }
        Performative::Disposition(disposition) => {
            apply_disposition(channel, disposition, sessions);
        }
        Performative::Detach(detach) => {
            let answered = if let Some(session) = sessions.get_mut(&channel) {
                if let Some(mut link) = session.links.remove(&detach.handle) {
                    stop_link(&mut link);
                    true
                } else {
                    let removed = session.pending_attaches.remove(&detach.handle).is_some();
                    if removed {
                        session.pending_flows.remove(&detach.handle);
                    }
                    removed
                }
            } else {
                false
            };
            if answered {
                write_amqp(
                    writer,
                    channel,
                    Performative::Detach(Detach {
                        handle: detach.handle,
                        closed: true,
                        error: None,
                    }),
                    Vec::new(),
                )
                .await?;
            }
        }
        Performative::End(_) => {
            pending_sessions.remove(&channel);
            if let Some(mut session) = sessions.remove(&channel) {
                for link in session.links.values_mut() {
                    stop_link(link);
                }
            }
            write_amqp(
                writer,
                channel,
                Performative::End(End::default()),
                Vec::new(),
            )
            .await?;
        }
        Performative::Close(_) => {
            write_amqp(writer, 0, Performative::Close(Close::default()), Vec::new()).await?;
            return Ok(FrameAction::Closed);
        }
        Performative::Open(_) => return Err(invalid_state("duplicate AMQP open")),
    }
    Ok(FrameAction::Continue)
}

enum CommandAction {
    Continue,
    Closing(oneshot::Sender<Result<(), EngineError>>),
}

async fn handle_command<W: AsyncWrite + Unpin>(
    command: Command,
    writer: &mut W,
    pending_sessions: &mut HashMap<u16, u64>,
    sessions: &mut HashMap<u16, SessionState>,
    remote_max_frame_size: u32,
) -> Result<CommandAction, EngineError> {
    match command {
        Command::AcceptSession {
            channel,
            incarnation,
            attach_tx,
            reply,
        } => {
            if pending_sessions.get(&channel) != Some(&incarnation) {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            }
            pending_sessions.remove(&channel);
            write_amqp(
                writer,
                channel,
                Performative::Begin(Begin {
                    remote_channel: Some(channel),
                    ..Begin::default()
                }),
                Vec::new(),
            )
            .await?;
            sessions.insert(
                channel,
                SessionState {
                    incarnation,
                    attach_tx: Some(attach_tx),
                    pending_attaches: HashMap::new(),
                    links: HashMap::new(),
                    pending_flows: HashMap::new(),
                    next_outgoing_id: 0,
                },
            );
            let _ = reply.send(Ok(()));
        }
        Command::AcceptLink {
            channel,
            session_incarnation,
            incarnation,
            attach,
            max_message_size,
            properties,
            deliveries_tx,
            detached_tx,
            drain_tx,
            credit_tx,
            reply,
        } => {
            let attach = *attach;
            let Some(session) = sessions.get_mut(&channel) else {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            };
            if session.incarnation != session_incarnation {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            }
            let handle = attach.handle;
            if session.pending_attaches.get(&handle) != Some(&incarnation) {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            }
            session.pending_attaches.remove(&handle);
            if session.links.contains_key(&handle) {
                let _ = reply.send(Err(invalid_state("link handle is already attached")));
                return Ok(CommandAction::Continue);
            }
            let mut response = attach.response(attach.source.clone(), attach.target.clone());
            response.max_message_size =
                (response.role == Role::Receiver).then_some(max_message_size);
            response.properties = properties;
            write_amqp(
                writer,
                channel,
                Performative::Attach(Box::new(response)),
                Vec::new(),
            )
            .await?;
            let incarnation = match attach.role {
                Role::Sender => {
                    session.links.insert(
                        handle,
                        LinkState::Receiving(ReceivingLink {
                            incarnation,
                            max_message_size,
                            deliveries: deliveries_tx,
                            partial: None,
                            detached: detached_tx,
                        }),
                    );
                    write_amqp(
                        writer,
                        channel,
                        Performative::Flow(Flow {
                            next_incoming_id: Some(0),
                            incoming_window: SESSION_WINDOW,
                            next_outgoing_id: session.next_outgoing_id,
                            outgoing_window: SESSION_WINDOW,
                            handle: Some(handle),
                            delivery_count: Some(0),
                            link_credit: Some(LINK_CREDIT),
                            ..Flow::default()
                        }),
                        Vec::new(),
                    )
                    .await?;
                    incarnation
                }
                Role::Receiver => {
                    let drain = LinkDrain::with_incarnation(incarnation, drain_tx);
                    session.links.insert(
                        handle,
                        LinkState::Sending(SendingLink {
                            settle_mode: attach.snd_settle_mode,
                            receiver_settle_mode: attach.rcv_settle_mode,
                            delivery_count: 0,
                            credit_limit: 0,
                            queued: VecDeque::new(),
                            unsettled: HashMap::new(),
                            detached: detached_tx,
                            drain,
                            credit: credit_tx,
                            credit_reservations: HashSet::new(),
                            next_credit_reservation: 0,
                        }),
                    );
                    incarnation
                }
            };
            let pending_flow = session.pending_flows.remove(&handle);
            if let Some(flow) = pending_flow {
                apply_flow(channel, flow, writer, sessions, remote_max_frame_size).await?;
            }
            let _ = reply.send(Ok(incarnation));
        }
        Command::ReserveCredit {
            channel,
            handle,
            incarnation,
            reply,
        } => reserve_credit(channel, handle, incarnation, reply, sessions),
        Command::ReleaseCredit { reservation, reply } => {
            release_credit(reservation, reply, sessions, writer).await?;
        }
        Command::Send {
            channel,
            handle,
            incarnation,
            message,
            message_format,
            delivery_tag,
            reservation,
            permit,
            started,
            reply,
        } => {
            let Some(session) = sessions.get_mut(&channel) else {
                let _ = started.send(Err(EngineError::RemoteDetached));
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            };
            let Some(link) = session.links.get_mut(&handle) else {
                let _ = started.send(Err(EngineError::RemoteDetached));
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            };
            if link.incarnation() != incarnation {
                let _ = started.send(Err(EngineError::RemoteDetached));
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            }
            let LinkState::Sending(link) = link else {
                let _ = started.send(Err(invalid_state("send on a receiving link")));
                let _ = reply.send(Err(invalid_state("send on a receiving link")));
                return Ok(CommandAction::Continue);
            };
            if let Some(reservation) = reservation {
                let valid = reservation.channel == channel
                    && reservation.handle == handle
                    && reservation.incarnation == link.drain.incarnation
                    && link
                        .credit_reservations
                        .contains(&reservation.reservation_id);
                if !valid {
                    let _ = started.send(Err(invalid_state(
                        "send used an invalid credit reservation",
                    )));
                    let _ = reply.send(Err(invalid_state(
                        "send used an invalid credit reservation",
                    )));
                    return Ok(CommandAction::Continue);
                }
            }
            let queued = QueuedSend {
                message: *message,
                message_format,
                delivery_tag,
                reservation,
                permit,
                started,
                reply,
            };
            if reservation.is_some() {
                let index = link
                    .queued
                    .iter()
                    .position(|queued| queued.reservation.is_none())
                    .unwrap_or(link.queued.len());
                link.queued.insert(index, queued);
            } else {
                link.queued.push_back(queued);
            }
            flush_sends(channel, handle, session, writer, remote_max_frame_size).await?;
        }
        Command::Confirm {
            channel,
            handle,
            incarnation,
            delivery_id,
            state,
            batchable,
            reply,
        } => {
            let Some(session) = sessions.get(&channel) else {
                let _ = reply.send(Err(invalid_state(
                    "settlement confirmation on an unknown session",
                )));
                return Ok(CommandAction::Continue);
            };
            if !matches!(
                session.links.get(&handle),
                Some(LinkState::Sending(link)) if link.drain.incarnation == incarnation
            ) {
                let _ = reply.send(Err(invalid_state(
                    "settlement confirmation on an unknown sending link",
                )));
                return Ok(CommandAction::Continue);
            }
            let result = write_amqp(
                writer,
                channel,
                Performative::Disposition(Disposition {
                    role: Role::Sender,
                    first: delivery_id,
                    last: None,
                    settled: true,
                    state: Some(state),
                    batchable,
                }),
                Vec::new(),
            )
            .await;
            let _ = reply.send(
                result
                    .as_ref()
                    .map(|_| ())
                    .map_err(|error| EngineError::InvalidState(error.to_string())),
            );
            result?;
        }
        Command::Drained { request, reply } => {
            acknowledge_drain(request, reply, sessions, writer).await?;
        }
        Command::Settle {
            channel,
            handle,
            incarnation,
            delivery_id,
            state,
            reply,
        } => {
            let Some(session) = sessions.get_mut(&channel) else {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            };
            if !matches!(
                session.links.get(&handle),
                Some(LinkState::Receiving(link)) if link.incarnation == incarnation
            ) {
                let _ = reply.send(Err(invalid_state("settlement on an unknown link")));
                return Ok(CommandAction::Continue);
            }
            write_amqp(
                writer,
                channel,
                Performative::Disposition(Disposition {
                    role: Role::Receiver,
                    first: delivery_id,
                    last: None,
                    settled: true,
                    state: Some(state),
                    batchable: false,
                }),
                Vec::new(),
            )
            .await?;
            let _ = reply.send(Ok(()));
        }
        Command::Detach {
            channel,
            handle,
            incarnation,
            error,
            reply,
        } => {
            let link = sessions.get_mut(&channel).and_then(|session| {
                let is_current = session
                    .links
                    .get(&handle)
                    .is_some_and(|link| link.incarnation() == incarnation);
                is_current.then(|| session.links.remove(&handle)).flatten()
            });
            if let Some(mut link) = link {
                write_amqp(
                    writer,
                    channel,
                    Performative::Detach(Detach {
                        handle,
                        closed: true,
                        error,
                    }),
                    Vec::new(),
                )
                .await?;
                stop_link(&mut link);
            }
            let _ = reply.send(Ok(()));
        }
        Command::Close { error, reply } => {
            write_amqp(writer, 0, Performative::Close(Close { error }), Vec::new()).await?;
            return Ok(CommandAction::Closing(reply));
        }
    }
    Ok(CommandAction::Continue)
}

pub(super) async fn receive_transfer(
    channel: u16,
    transfer: Transfer,
    payload: Vec<u8>,
    sessions: &mut HashMap<u16, SessionState>,
) -> Result<(), EngineError> {
    let session = sessions
        .get_mut(&channel)
        .ok_or_else(|| invalid_state("transfer on an unknown session"))?;
    let Some(link) = session.links.get_mut(&transfer.handle) else {
        // A transfer can cross a link-scoped refusal on the wire. The detach is
        // authoritative; a late transfer must not escalate it to the connection.
        return Ok(());
    };
    let LinkState::Receiving(link) = link else {
        return Err(invalid_state("transfer sent to a sending link"));
    };

    let message_format = match &link.partial {
        Some(partial) => transfer.message_format.unwrap_or(partial.message_format),
        None => transfer.message_format.unwrap_or(0),
    };
    if let Some(partial) = &link.partial
        && message_format != partial.message_format
    {
        return Err(invalid_state(
            "message format changed across transfer continuations",
        ));
    }

    if transfer.aborted {
        link.partial = None;
        return Ok(());
    }
    let partial = match link.partial.take() {
        Some(mut partial) => {
            partial.bytes.extend_from_slice(&payload);
            partial
        }
        None => PartialDelivery {
            id: transfer
                .delivery_id
                .ok_or_else(|| invalid_state("first transfer has no delivery id"))?,
            settled: transfer.settled.unwrap_or(false),
            message_format,
            bytes: payload,
        },
    };
    if partial.bytes.len() as u64 > link.max_message_size {
        return Err(invalid_state("message exceeds the link's maximum size"));
    }
    if transfer.more {
        link.partial = Some(partial);
        return Ok(());
    }

    let message = decode_message(&partial.bytes)?;
    link.deliveries
        .send(Delivery {
            id: partial.id,
            settled: partial.settled,
            message_format: partial.message_format,
            message,
            encoded_message: partial.bytes,
        })
        .await
        .map_err(|_| EngineError::Stopped)
}

pub(super) async fn flush_sends<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    session: &mut SessionState,
    writer: &mut W,
    remote_max_frame_size: u32,
) -> Result<(), EngineError> {
    loop {
        let Some(LinkState::Sending(link)) = session.links.get_mut(&handle) else {
            return Ok(());
        };
        let front_is_reserved = link
            .queued
            .front()
            .is_some_and(|queued| queued.reservation.is_some());
        let committed =
            u64::from(link.delivery_count).saturating_add(link.credit_reservations.len() as u64);
        if !front_is_reserved && committed >= link.credit_limit {
            trace!(
                channel,
                handle,
                delivery_count = link.delivery_count,
                credit_limit = link.credit_limit,
                reservations = link.credit_reservations.len(),
                queued = link.queued.len(),
                "send is waiting for link credit"
            );
            publish_credit(link);
            complete_zero_credit_drain(channel, handle, session, writer).await?;
            return Ok(());
        }
        let Some(queued) = link.queued.pop_front() else {
            publish_credit(link);
            return Ok(());
        };
        if let Some(reservation) = queued.reservation {
            let removed = reservation.incarnation == link.drain.incarnation
                && link.credit_reservations.remove(&reservation.reservation_id);
            debug_assert!(removed, "queued sends retain their credit reservation");
        }
        let delivery_id = session.next_outgoing_id;
        session.next_outgoing_id = session.next_outgoing_id.wrapping_add(1);
        link.delivery_count = link.delivery_count.wrapping_add(1);
        let identity = DeliveryIdentity {
            channel,
            handle,
            delivery_id,
        };
        let settled = link.settle_mode == SenderSettleMode::Settled;
        let payload = encode_message(&queued.message)?;
        write_transfer(
            writer,
            channel,
            handle,
            delivery_id,
            queued.delivery_tag,
            queued.message_format,
            settled,
            payload,
            remote_max_frame_size,
        )
        .await?;
        if settled {
            let _ = queued.started.send(Ok(identity));
            let _ = queued.reply.send(Ok(RemoteOutcome {
                outcome: Outcome::Accepted(Accepted),
                confirmation: None,
            }));
        } else {
            link.unsettled.insert(
                delivery_id,
                UnsettledSend {
                    reply: queued.reply,
                    _permit: queued.permit,
                },
            );
            let _ = queued.started.send(Ok(identity));
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn write_transfer<W: AsyncWrite + Unpin>(
    writer: &mut W,
    channel: u16,
    handle: u32,
    delivery_id: u32,
    delivery_tag: DeliveryTag,
    message_format: u32,
    settled: bool,
    payload: Vec<u8>,
    remote_max_frame_size: u32,
) -> Result<(), EngineError> {
    let maximum = usize::try_from(remote_max_frame_size)
        .unwrap_or(usize::MAX)
        .saturating_sub(FRAME_OVERHEAD_RESERVE)
        .max(1);
    let chunks = if payload.is_empty() {
        vec![&[][..]]
    } else {
        payload.chunks(maximum).collect()
    };
    let chunk_count = chunks.len();
    for (index, chunk) in chunks.into_iter().enumerate() {
        write_amqp(
            writer,
            channel,
            Performative::Transfer(Transfer {
                handle,
                delivery_id: (index == 0).then_some(delivery_id),
                delivery_tag: (index == 0).then_some(delivery_tag.clone()),
                message_format: (index == 0).then_some(message_format),
                settled: (index == 0).then_some(settled),
                more: index + 1 < chunk_count,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            }),
            chunk.to_vec(),
        )
        .await?;
    }
    Ok(())
}

pub(super) fn apply_disposition(
    channel: u16,
    disposition: Disposition,
    sessions: &mut HashMap<u16, SessionState>,
) {
    if disposition.role != Role::Receiver {
        return;
    }
    let Some(state) = disposition.state else {
        return;
    };
    let Ok(outcome) = Outcome::try_from(state) else {
        return;
    };
    let last = disposition.last.unwrap_or(disposition.first);
    if last < disposition.first {
        return;
    }
    let Some(session) = sessions.get_mut(&channel) else {
        return;
    };
    for (handle, link) in &mut session.links {
        let LinkState::Sending(link) = link else {
            continue;
        };
        let mut delivery_ids = link
            .unsettled
            .keys()
            .copied()
            .filter(|id| *id >= disposition.first && *id <= last)
            .collect::<Vec<_>>();
        delivery_ids.sort_unstable();
        for id in delivery_ids {
            if let Some(unsettled) = link.unsettled.remove(&id) {
                let confirmation = (link.receiver_settle_mode == ReceiverSettleMode::Second
                    && !disposition.settled)
                    .then(|| PendingConfirmation {
                        handle: *handle,
                        incarnation: link.drain.incarnation,
                        delivery_id: id,
                        batchable: disposition.batchable,
                        permit: unsettled._permit,
                    });
                let _ = unsettled.reply.send(Ok(RemoteOutcome {
                    outcome: outcome.clone(),
                    confirmation,
                }));
            }
        }
    }
}

pub(super) fn stop_link(link: &mut LinkState) {
    match link {
        LinkState::Sending(link) => {
            let _ = link.detached.send(true);
            for queued in link.queued.drain(..) {
                let _ = queued.started.send(Err(EngineError::RemoteDetached));
                let _ = queued.reply.send(Err(EngineError::RemoteDetached));
            }
            for (_, unsettled) in link.unsettled.drain() {
                let _ = unsettled.reply.send(Err(EngineError::RemoteDetached));
            }
        }
        LinkState::Receiving(link) => {
            let _ = link.detached.send(true);
        }
    }
}

pub(super) async fn write_amqp<W: AsyncWrite + Unpin>(
    writer: &mut W,
    channel: u16,
    performative: Performative,
    payload: Vec<u8>,
) -> Result<(), EngineError> {
    write_frame(
        writer,
        &Frame::Amqp {
            channel,
            performative: Some(performative),
            payload,
        },
    )
    .await?;
    Ok(())
}

pub(super) fn invalid_state(message: impl Into<String>) -> EngineError {
    EngineError::InvalidState(message.into())
}

#[cfg(test)]
mod tests;
