use super::*;

pub(super) enum Command {
    AcceptSession {
        channel: u16,
        attach_tx: mpsc::Sender<Attach>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    AcceptLink {
        channel: u16,
        attach: Box<Attach>,
        max_message_size: u64,
        properties: Option<Fields>,
        deliveries_tx: mpsc::Sender<Delivery>,
        detached_tx: watch::Sender<bool>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Send {
        channel: u16,
        handle: u32,
        message: Box<Message>,
        delivery_tag: DeliveryTag,
        reply: oneshot::Sender<Result<RemoteOutcome, EngineError>>,
    },
    Confirm {
        channel: u16,
        handle: u32,
        delivery_id: u32,
        state: DeliveryState,
        batchable: bool,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Settle {
        channel: u16,
        handle: u32,
        delivery_id: u32,
        state: DeliveryState,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Detach {
        channel: u16,
        handle: u32,
        error: Option<Error>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Close {
        error: Option<Error>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
}

pub(super) struct SessionState {
    pub(super) attach_tx: Option<mpsc::Sender<Attach>>,
    pub(super) links: HashMap<u32, LinkState>,
    pub(super) pending_flows: HashMap<u32, Flow>,
    pub(super) next_outgoing_id: u32,
}

pub(super) enum LinkState {
    Sending(SendingLink),
    Receiving(ReceivingLink),
}

pub(super) struct SendingLink {
    pub(super) receiver_settle_mode: ReceiverSettleMode,
    pub(super) settle_mode: SenderSettleMode,
    pub(super) delivery_count: u32,
    pub(super) credit_limit: u64,
    pub(super) queued: VecDeque<QueuedSend>,
    pub(super) unsettled: HashMap<u32, oneshot::Sender<Result<RemoteOutcome, EngineError>>>,
    pub(super) detached: watch::Sender<bool>,
}

pub(super) struct QueuedSend {
    pub(super) message: Message,
    pub(super) delivery_tag: DeliveryTag,
    pub(super) reply: oneshot::Sender<Result<RemoteOutcome, EngineError>>,
}

pub(super) struct RemoteOutcome {
    pub(super) outcome: Outcome,
    pub(super) confirmation: Option<PendingConfirmation>,
}

#[derive(Clone, Copy)]
pub(super) struct PendingConfirmation {
    pub(super) handle: u32,
    pub(super) delivery_id: u32,
    pub(super) batchable: bool,
}

pub(super) struct ReceivingLink {
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
    let mut closing_reply: Option<oneshot::Sender<Result<(), EngineError>>> = None;
    loop {
        tokio::select! {
            frame = frames.recv() => {
                let Some(frame) = frame else { break };
                let Ok(frame) = frame else { break };
                match handle_frame(
                    frame,
                    &mut writer,
                    &incoming_sessions,
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

enum FrameAction {
    Continue,
    Closed,
}

async fn handle_frame<W: AsyncWrite + Unpin>(
    frame: Frame,
    writer: &mut W,
    incoming_sessions: &mpsc::Sender<IncomingSession>,
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
            incoming_sessions
                .send(IncomingSession { channel, begin })
                .await
                .map_err(|_| EngineError::Stopped)?;
        }
        Performative::Attach(attach) => {
            let session = sessions
                .get(&channel)
                .ok_or_else(|| invalid_state("attach on an unknown session"))?;
            session
                .attach_tx
                .as_ref()
                .ok_or_else(|| invalid_state("session cannot accept remote links"))?
                .send(*attach)
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
            if let Some(session) = sessions.get_mut(&channel)
                && let Some(mut link) = session.links.remove(&detach.handle)
            {
                stop_link(&mut link);
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
    sessions: &mut HashMap<u16, SessionState>,
    remote_max_frame_size: u32,
) -> Result<CommandAction, EngineError> {
    match command {
        Command::AcceptSession {
            channel,
            attach_tx,
            reply,
        } => {
            if sessions.contains_key(&channel) {
                let _ = reply.send(Err(invalid_state("session channel is already open")));
                return Ok(CommandAction::Continue);
            }
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
                    attach_tx: Some(attach_tx),
                    links: HashMap::new(),
                    pending_flows: HashMap::new(),
                    next_outgoing_id: 0,
                },
            );
            let _ = reply.send(Ok(()));
        }
        Command::AcceptLink {
            channel,
            attach,
            max_message_size,
            properties,
            deliveries_tx,
            detached_tx,
            reply,
        } => {
            let attach = *attach;
            let session = sessions
                .get_mut(&channel)
                .ok_or_else(|| invalid_state("link accepted on an unknown session"))?;
            let handle = attach.handle;
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
            match attach.role {
                Role::Sender => {
                    session.links.insert(
                        handle,
                        LinkState::Receiving(ReceivingLink {
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
                }
                Role::Receiver => {
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
                        }),
                    );
                }
            }
            let pending_flow = session.pending_flows.remove(&handle);
            if let Some(flow) = pending_flow {
                apply_flow(channel, flow, writer, sessions, remote_max_frame_size).await?;
            }
            let _ = reply.send(Ok(()));
        }
        Command::Send {
            channel,
            handle,
            message,
            delivery_tag,
            reply,
        } => {
            let session = sessions
                .get_mut(&channel)
                .ok_or_else(|| invalid_state("send on an unknown session"))?;
            let LinkState::Sending(link) = session
                .links
                .get_mut(&handle)
                .ok_or_else(|| invalid_state("send on an unknown link"))?
            else {
                let _ = reply.send(Err(invalid_state("send on a receiving link")));
                return Ok(CommandAction::Continue);
            };
            link.queued.push_back(QueuedSend {
                message: *message,
                delivery_tag,
                reply,
            });
            flush_sends(channel, handle, session, writer, remote_max_frame_size).await?;
        }
        Command::Confirm {
            channel,
            handle,
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
            if !matches!(session.links.get(&handle), Some(LinkState::Sending(_))) {
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
        Command::Settle {
            channel,
            handle,
            delivery_id,
            state,
            reply,
        } => {
            let session = sessions
                .get_mut(&channel)
                .ok_or_else(|| invalid_state("settlement on an unknown session"))?;
            if !matches!(session.links.get(&handle), Some(LinkState::Receiving(_))) {
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
            error,
            reply,
        } => {
            if let Some(session) = sessions.get_mut(&channel)
                && let Some(mut link) = session.links.remove(&handle)
            {
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
    if message_format != 0 {
        return Err(invalid_state(format!(
            "unsupported AMQP message format {message_format}"
        )));
    }
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
            message,
            encoded_message: partial.bytes,
        })
        .await
        .map_err(|_| EngineError::Stopped)
}

pub(super) async fn apply_flow<W: AsyncWrite + Unpin>(
    channel: u16,
    flow: Flow,
    writer: &mut W,
    sessions: &mut HashMap<u16, SessionState>,
    remote_max_frame_size: u32,
) -> Result<(), EngineError> {
    let Some(handle) = flow.handle else {
        return Ok(());
    };
    let Some(session) = sessions.get_mut(&channel) else {
        trace!(channel, handle, "ignoring flow for an unknown session");
        return Ok(());
    };
    let link = match session.links.get_mut(&handle) {
        Some(LinkState::Sending(link)) => link,
        Some(LinkState::Receiving(_)) => {
            trace!(channel, handle, "ignoring flow for a receiving link");
            return Ok(());
        }
        None => {
            trace!(channel, handle, "buffering flow for a pending attach");
            session.pending_flows.insert(handle, flow);
            return Ok(());
        }
    };
    if let Some(credit) = flow.link_credit {
        link.credit_limit =
            u64::from(flow.delivery_count.unwrap_or(link.delivery_count)) + u64::from(credit);
        trace!(
            channel,
            handle,
            delivery_count = link.delivery_count,
            credit,
            credit_limit = link.credit_limit,
            queued = link.queued.len(),
            "link credit updated"
        );
    }
    flush_sends(channel, handle, session, writer, remote_max_frame_size).await
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
        if u64::from(link.delivery_count) >= link.credit_limit {
            trace!(
                channel,
                handle,
                delivery_count = link.delivery_count,
                credit_limit = link.credit_limit,
                queued = link.queued.len(),
                "send is waiting for link credit"
            );
            return Ok(());
        }
        let Some(queued) = link.queued.pop_front() else {
            return Ok(());
        };
        let delivery_id = session.next_outgoing_id;
        session.next_outgoing_id = session.next_outgoing_id.wrapping_add(1);
        link.delivery_count = link.delivery_count.wrapping_add(1);
        let settled = link.settle_mode == SenderSettleMode::Settled;
        let payload = encode_message(&queued.message)?;
        write_transfer(
            writer,
            channel,
            handle,
            delivery_id,
            queued.delivery_tag,
            settled,
            payload,
            remote_max_frame_size,
        )
        .await?;
        if settled {
            let _ = queued.reply.send(Ok(RemoteOutcome {
                outcome: Outcome::Accepted(Accepted),
                confirmation: None,
            }));
        } else {
            link.unsettled.insert(delivery_id, queued.reply);
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
                message_format: (index == 0).then_some(0),
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
    let Some(session) = sessions.get_mut(&channel) else {
        return;
    };
    for (handle, link) in &mut session.links {
        let LinkState::Sending(link) = link else {
            continue;
        };
        for id in disposition.first..=last {
            if let Some(reply) = link.unsettled.remove(&id) {
                let confirmation = (link.receiver_settle_mode == ReceiverSettleMode::Second
                    && !disposition.settled)
                    .then_some(PendingConfirmation {
                        handle: *handle,
                        delivery_id: id,
                        batchable: disposition.batchable,
                    });
                let _ = reply.send(Ok(RemoteOutcome {
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
                let _ = queued.reply.send(Err(EngineError::RemoteDetached));
            }
            for (_, reply) in link.unsettled.drain() {
                let _ = reply.send(Err(EngineError::RemoteDetached));
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
