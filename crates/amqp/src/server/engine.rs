use super::*;
use tokio::io::AsyncWriteExt;

mod link_flow;
mod session_flow;

pub(super) use link_flow::{LinkDrain, apply_flow};
use link_flow::{
    acknowledge_drain, complete_zero_credit_drain, publish_credit, release_credit, reserve_credit,
};
pub(super) use session_flow::OutgoingWindow;

pub(super) enum Command {
    AcceptSession {
        channel: u16,
        incarnation: u64,
        attach_tx: mpsc::Sender<IncomingAttach>,
        ended_tx: watch::Sender<bool>,
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
        commands: mpsc::Sender<Command>,
        cleanup: mpsc::UnboundedSender<CleanupCommand>,
        reply: oneshot::Sender<Result<Option<CreditReservation>, EngineError>>,
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
    // The original state owns the only producer; End/Stop/panic drop is terminal.
    pub(super) _ended: watch::Sender<bool>,
    pub(super) pending_attaches: HashMap<u32, u64>,
    pub(super) links: HashMap<u32, LinkState>,
    pub(super) pending_flows: HashMap<u32, Flow>,
    /// Delivery IDs remain independent from the per-frame sequence in Flow.
    pub(super) next_delivery_id: u32,
    pub(super) outgoing: OutgoingWindow,
}

#[derive(Debug)]
pub(super) struct PendingSession {
    incarnation: u64,
    incoming_window: u32,
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
    pub(super) transmitting: Option<Box<TransmittingSend>>,
    pub(super) unsettled: HashMap<u32, UnsettledSend>,
    pub(super) detached: watch::Sender<bool>,
    pub(super) drain: LinkDrain,
    pub(super) credit: watch::Sender<bool>,
    pub(super) credit_reservations: HashSet<u64>,
    pub(super) next_credit_reservation: u64,
    /// The receiving peer's binding `max-message-size`; `None` is unlimited.
    pub(super) max_message_size: Option<u64>,
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

pub(super) struct TransmittingSend {
    payload: Vec<u8>,
    offset: usize,
    first: bool,
    delivery_id: u32,
    delivery_tag: DeliveryTag,
    message_format: u32,
    settled: bool,
    // A canceled/failed original write drops its reply channels (Stopped),
    // unlike a peer retirement observed between writes (RemoteDetached).
    writing: bool,
    started: oneshot::Sender<Result<DeliveryIdentity, EngineError>>,
    // The unsettled map owns the permit until transmission and any required
    // settlement finish. Early peer outcomes stay here until the final frame.
    early_outcome: Option<(Outcome, bool, bool)>,
    settled_reply: Option<oneshot::Sender<Result<RemoteOutcome, EngineError>>>,
    _settled_permit: Option<OwnedSemaphorePermit>,
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

pub(super) async fn run_connection<W>(
    mut writer: W,
    remote_max_frame_size: u32,
    mut commands: mpsc::Receiver<Command>,
    mut cleanup: mpsc::UnboundedReceiver<CleanupCommand>,
    incoming_sessions: mpsc::Sender<IncomingSession>,
    mut frames: mpsc::Receiver<Result<Frame, EngineError>>,
    mut stop: watch::Receiver<bool>,
) where
    W: AsyncWrite + Send + Unpin + 'static,
{
    let mut sessions = HashMap::<u16, SessionState>::new();
    let mut pending_sessions = HashMap::<u16, PendingSession>::new();
    let mut closing_reply: Option<oneshot::Sender<Result<(), EngineError>>> = None;
    let mut cleanup_open = true;
    // Cancellation must reach nested writes and channel sends before link cleanup.
    tokio::select! {
        biased;
        _ = super::wait_for_stop(&mut stop) => {}
        _ = async {
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
        } => {}
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
    pending_sessions: &mut HashMap<u16, PendingSession>,
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
            pending_sessions.insert(
                channel,
                PendingSession {
                    incarnation,
                    incoming_window: begin.incoming_window,
                },
            );
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
            apply_session_flow(channel, flow, writer, sessions, remote_max_frame_size).await?;
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
            // The reader may stop next, so it cannot flush a buffered transport for us.
            writer.flush().await?;
            return Ok(FrameAction::Closed);
        }
        Performative::Open(_) => return Err(invalid_state("duplicate AMQP open")),
    }
    Ok(FrameAction::Continue)
}

pub(in crate::server) async fn apply_session_flow<W: AsyncWrite + Unpin>(
    channel: u16,
    flow: Flow,
    writer: &mut W,
    sessions: &mut HashMap<u16, SessionState>,
    remote_max_frame_size: u32,
) -> Result<(), EngineError> {
    let Some(session) = sessions.get_mut(&channel) else {
        return Ok(());
    };
    session.outgoing.update(&flow)?;
    apply_flow(channel, flow, writer, sessions, remote_max_frame_size).await?;
    // Link hints cached before AcceptLink are replayed by apply_flow alone;
    // replaying their older session acknowledgement would rewind peer state.
    // Session-only Flow also wakes sends on every applicable link.
    let session = sessions
        .get_mut(&channel)
        .expect("the session remains installed");
    let mut handles = session.links.keys().copied().collect::<Vec<_>>();
    handles.sort_unstable();
    for handle in handles {
        flush_sends(channel, handle, session, writer, remote_max_frame_size).await?;
    }
    Ok(())
}

enum CommandAction {
    Continue,
    Closing(oneshot::Sender<Result<(), EngineError>>),
}

async fn handle_command<W: AsyncWrite + Unpin>(
    command: Command,
    writer: &mut W,
    pending_sessions: &mut HashMap<u16, PendingSession>,
    sessions: &mut HashMap<u16, SessionState>,
    remote_max_frame_size: u32,
) -> Result<CommandAction, EngineError> {
    match command {
        Command::AcceptSession {
            channel,
            incarnation,
            attach_tx,
            ended_tx,
            reply,
        } => {
            if pending_sessions
                .get(&channel)
                .map(|pending| pending.incarnation)
                != Some(incarnation)
            {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            }
            let pending = pending_sessions
                .remove(&channel)
                .expect("the original pending session exists");
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
                    _ended: ended_tx,
                    pending_attaches: HashMap::new(),
                    links: HashMap::new(),
                    pending_flows: HashMap::new(),
                    next_delivery_id: 0,
                    outgoing: OutgoingWindow::new(0, pending.incoming_window),
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
                            next_outgoing_id: session.outgoing.next_id(),
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
                    let max_message_size = super::peer_message_limit(&attach);
                    let drain = LinkDrain::with_incarnation(incarnation, drain_tx);
                    session.links.insert(
                        handle,
                        LinkState::Sending(SendingLink {
                            settle_mode: attach.snd_settle_mode,
                            receiver_settle_mode: attach.rcv_settle_mode,
                            delivery_count: 0,
                            credit_limit: 0,
                            queued: VecDeque::new(),
                            transmitting: None,
                            unsettled: HashMap::new(),
                            detached: detached_tx,
                            drain,
                            credit: credit_tx,
                            credit_reservations: HashSet::new(),
                            next_credit_reservation: 0,
                            max_message_size,
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
            commands,
            cleanup,
            reply,
        } => reserve_credit(
            channel,
            handle,
            incarnation,
            commands,
            cleanup,
            reply,
            sessions,
        ),
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
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            };
            let Some(link) = session.links.get(&handle) else {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            };
            // A durable result can reach its confirmation after native Detach or
            // End. Its old owner is retired, not an active protocol fault; never
            // write that disposition into a replacement at the same wire handle.
            if link.incarnation() != incarnation {
                let _ = reply.send(Err(EngineError::RemoteDetached));
                return Ok(CommandAction::Continue);
            }
            if !matches!(link, LinkState::Sending(_)) {
                let _ = reply.send(Err(invalid_state(
                    "settlement confirmation on a receiving link",
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
            let is_current = sessions
                .get(&channel)
                .and_then(|session| session.links.get(&handle))
                .is_some_and(|link| link.incarnation() == incarnation);
            if is_current {
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
                if let Some(mut link) = sessions
                    .get_mut(&channel)
                    .and_then(|session| session.links.remove(&handle))
                {
                    stop_link(&mut link);
                }
            }
            let _ = reply.send(Ok(()));
        }
        Command::Close { error, reply } => {
            write_amqp(writer, 0, Performative::Close(Close { error }), Vec::new()).await?;
            writer.flush().await?;
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

    // A peer's transfer: decode with every received-message check.
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
        if link.transmitting.is_none() {
            let front_is_reserved = link
                .queued
                .front()
                .is_some_and(|queued| queued.reservation.is_some());
            let committed = u64::from(link.delivery_count)
                .saturating_add(link.credit_reservations.len() as u64);
            if !front_is_reserved && committed >= link.credit_limit {
                publish_credit(link);
                complete_zero_credit_drain(channel, handle, session, writer).await?;
                return Ok(());
            }
            if link.queued.is_empty() || !session.outgoing.has_capacity() {
                publish_credit(link);
                return Ok(());
            }
            let queued = link
                .queued
                .pop_front()
                .expect("the queued send remains owned");
            if let Some(reservation) = queued.reservation {
                let removed = reservation.incarnation == link.drain.incarnation
                    && link.credit_reservations.remove(&reservation.reservation_id);
                debug_assert!(removed, "queued sends retain their credit reservation");
            }
            let payload = encode_message(&queued.message)?;
            if let Some(maximum) = link.max_message_size
                && payload.len() as u64 > maximum
            {
                let size = payload.len() as u64;
                let _ = queued
                    .started
                    .send(Err(EngineError::MessageSizeExceeded { size, maximum }));
                let _ = queued
                    .reply
                    .send(Err(EngineError::MessageSizeExceeded { size, maximum }));
                return detach_oversized(channel, handle, session, writer, size, maximum).await;
            }
            let delivery_id = session.next_delivery_id;
            session.next_delivery_id = session.next_delivery_id.wrapping_add(1);
            link.delivery_count = link.delivery_count.wrapping_add(1);
            let settled = link.settle_mode == SenderSettleMode::Settled;
            let (settled_reply, settled_permit) = if settled {
                (Some(queued.reply), Some(queued.permit))
            } else {
                link.unsettled.insert(
                    delivery_id,
                    UnsettledSend {
                        reply: queued.reply,
                        _permit: queued.permit,
                    },
                );
                (None, None)
            };
            link.transmitting = Some(Box::new(TransmittingSend {
                payload,
                offset: 0,
                first: true,
                delivery_id,
                delivery_tag: queued.delivery_tag,
                message_format: queued.message_format,
                settled,
                writing: false,
                started: queued.started,
                early_outcome: None,
                settled_reply,
                _settled_permit: settled_permit,
            }));
        }
        if !session.outgoing.has_capacity() {
            publish_credit(link);
            return Ok(());
        }
        let sending = link
            .transmitting
            .as_mut()
            .expect("the original partial send is owned");
        let maximum = usize::try_from(remote_max_frame_size)
            .unwrap_or(usize::MAX)
            .saturating_sub(FRAME_OVERHEAD_RESERVE)
            .max(1);
        let end = sending
            .offset
            .saturating_add(maximum)
            .min(sending.payload.len());
        let more = end < sending.payload.len();
        sending.writing = true;
        write_amqp(
            writer,
            channel,
            Performative::Transfer(Transfer {
                handle,
                delivery_id: sending.first.then_some(sending.delivery_id),
                delivery_tag: sending.first.then(|| sending.delivery_tag.clone()),
                message_format: sending.first.then_some(sending.message_format),
                settled: sending.first.then_some(sending.settled),
                more,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            }),
            sending.payload[sending.offset..end].to_vec(),
        )
        .await?;
        sending.writing = false;
        session.outgoing.sent()?;
        sending.offset = end;
        sending.first = false;
        if !more {
            let sending = link
                .transmitting
                .take()
                .expect("the completed original send is owned");
            let _ = sending.started.send(Ok(DeliveryIdentity {
                channel,
                handle,
                delivery_id: sending.delivery_id,
            }));
            if let Some(reply) = sending.settled_reply {
                let _ = reply.send(Ok(RemoteOutcome {
                    outcome: Outcome::Accepted(Accepted),
                    confirmation: None,
                }));
            }
            if let Some((outcome, settled, batchable)) = sending.early_outcome {
                settle_send(
                    link,
                    handle,
                    sending.delivery_id,
                    outcome,
                    settled,
                    batchable,
                );
            }
        }
    }
}

/// Ends a sending link whose next delivery exceeds the peer's limit, the same
/// way a local close does: queued and unsettled sends fail as detached.
async fn detach_oversized<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    session: &mut SessionState,
    writer: &mut W,
    size: u64,
    maximum: u64,
) -> Result<(), EngineError> {
    write_amqp(
        writer,
        channel,
        Performative::Detach(Detach {
            handle,
            closed: true,
            error: Some(Error::new(
                crate::ErrorCondition::Custom(Symbol::from(super::MESSAGE_SIZE_EXCEEDED_CONDITION)),
                format!(
                    "the {size}-byte message exceeds the link's {maximum}-byte max-message-size"
                ),
                None,
            )),
        }),
        Vec::new(),
    )
    .await?;
    if let Some(mut link) = session.links.remove(&handle) {
        stop_link(&mut link);
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
            if let Some(sending) = link.transmitting.as_mut()
                && sending.delivery_id == id
            {
                // Do not release the original send permit while its payload
                // is still retained, or expose a complete send prematurely.
                sending.early_outcome.get_or_insert((
                    outcome.clone(),
                    disposition.settled,
                    disposition.batchable,
                ));
                continue;
            }
            settle_send(
                link,
                *handle,
                id,
                outcome.clone(),
                disposition.settled,
                disposition.batchable,
            );
        }
    }
}

fn settle_send(
    link: &mut SendingLink,
    handle: u32,
    id: u32,
    outcome: Outcome,
    settled: bool,
    batchable: bool,
) {
    if let Some(unsettled) = link.unsettled.remove(&id) {
        let confirmation = (link.receiver_settle_mode == ReceiverSettleMode::Second && !settled)
            .then(|| PendingConfirmation {
                handle,
                incarnation: link.drain.incarnation,
                delivery_id: id,
                batchable,
                permit: unsettled._permit,
            });
        let _ = unsettled.reply.send(Ok(RemoteOutcome {
            outcome,
            confirmation,
        }));
    }
}

pub(super) fn stop_link(link: &mut LinkState) {
    match link {
        LinkState::Sending(link) => {
            let _ = link.detached.send(true);
            if let Some(sending) = link.transmitting.take() {
                if sending.writing {
                    // Preserve the original write-future cancellation result:
                    // no peer Detach was received, so close the channels and
                    // let the caller observe Stopped, not fabricated retirement.
                    link.unsettled.remove(&sending.delivery_id);
                } else {
                    let _ = sending.started.send(Err(EngineError::RemoteDetached));
                    if let Some(reply) = sending.settled_reply {
                        let _ = reply.send(Err(EngineError::RemoteDetached));
                    }
                }
            }
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
