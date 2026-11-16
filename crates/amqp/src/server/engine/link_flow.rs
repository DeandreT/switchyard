use super::sending_credit::DrainTarget;
use super::*;

pub(in crate::server) struct LinkDrain {
    pub(super) notifications: watch::Sender<Option<DrainRequest>>,
    pub(super) current: Option<PendingDrain>,
    pub(in crate::server) incarnation: u64,
    pub(super) next_generation: u64,
}

impl LinkDrain {
    #[cfg(any(feature = "test-client", test))]
    pub(in crate::server) fn new(notifications: watch::Sender<Option<DrainRequest>>) -> Self {
        Self::with_incarnation(next_link_incarnation(), notifications)
    }

    pub(in crate::server) fn with_incarnation(
        incarnation: u64,
        notifications: watch::Sender<Option<DrainRequest>>,
    ) -> Self {
        Self {
            notifications,
            current: None,
            incarnation,
            next_generation: 0,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct PendingDrain {
    pub(super) request: DrainRequest,
    pub(super) target: DrainTarget,
}

pub(super) fn reserve_credit(
    channel: u16,
    handle: u32,
    incarnation: u64,
    commands: mpsc::Sender<Command>,
    cleanup: mpsc::UnboundedSender<CleanupCommand>,
    reply: oneshot::Sender<Result<Option<CreditReservation>, EngineError>>,
    sessions: &mut HashMap<u16, SessionState>,
) {
    let Some(LinkState::Sending(link)) = sessions
        .get_mut(&channel)
        .and_then(|session| session.links.get_mut(&handle))
    else {
        let _ = reply.send(Err(EngineError::RemoteDetached));
        return;
    };
    if link.drain.incarnation != incarnation {
        let _ = reply.send(Err(EngineError::RemoteDetached));
        return;
    }
    if !has_unreserved_credit(link) {
        publish_credit(link);
        let _ = reply.send(Ok(None));
        return;
    }
    let reservation_id = loop {
        let candidate = link.next_credit_reservation;
        link.next_credit_reservation = link.next_credit_reservation.wrapping_add(1);
        if link.credit_reservations.insert(candidate) {
            break candidate;
        }
    };
    let reservation = CreditReservation {
        identity: CreditReservationIdentity {
            channel,
            handle,
            incarnation: link.drain.incarnation,
            reservation_id,
        },
        commands,
        cleanup,
        active: true,
    };
    publish_credit(link);
    if let Err(Ok(Some(mut reservation))) = reply.send(Ok(Some(reservation))) {
        // Failed delivery rolls back immediately; do not also enqueue cleanup.
        reservation.disarm();
        link.credit_reservations.remove(&reservation_id);
        publish_credit(link);
    }
}

pub(super) async fn release_credit<W: AsyncWrite + Unpin>(
    reservation: CreditReservationIdentity,
    reply: Option<oneshot::Sender<Result<(), EngineError>>>,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut W,
) -> Result<(), EngineError> {
    let Some(LinkState::Sending(link)) = sessions
        .get_mut(&reservation.channel)
        .and_then(|session| session.links.get_mut(&reservation.handle))
    else {
        if let Some(reply) = reply {
            let _ = reply.send(Err(EngineError::RemoteDetached));
        }
        return Ok(());
    };
    if reservation.incarnation != link.drain.incarnation {
        if let Some(reply) = reply {
            let _ = reply.send(Err(invalid_state(
                "credit reservation belongs to a replaced link",
            )));
        }
        return Ok(());
    }
    link.credit_reservations.remove(&reservation.reservation_id);
    publish_credit(link);
    let session = sessions
        .get_mut(&reservation.channel)
        .expect("the releasing session still exists");
    let result =
        complete_zero_credit_drain(reservation.channel, reservation.handle, session, writer).await;
    if let Some(reply) = reply {
        let _ = reply.send(
            result
                .as_ref()
                .map(|_| ())
                .map_err(|error| invalid_state(error.to_string())),
        );
    }
    result
}

pub(in crate::server) async fn acknowledge_drain<W: AsyncWrite + Unpin>(
    request: DrainRequest,
    reply: oneshot::Sender<Result<(), EngineError>>,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut W,
) -> Result<(), EngineError> {
    let Some(session) = sessions.get_mut(&request.channel) else {
        let _ = reply.send(Err(EngineError::RemoteDetached));
        return Ok(());
    };
    let Some(LinkState::Sending(link)) = session.links.get_mut(&request.handle) else {
        let _ = reply.send(Err(EngineError::RemoteDetached));
        return Ok(());
    };
    if request.incarnation != link.drain.incarnation {
        let _ = reply.send(Err(invalid_state(
            "drain request belongs to a replaced link",
        )));
        return Ok(());
    }
    let Some(pending) = link.drain.current else {
        let _ = reply.send(Ok(()));
        return Ok(());
    };
    if pending.request != request {
        let _ = reply.send(Ok(()));
        return Ok(());
    }
    if !link.credit_reservations.is_empty() {
        let _ = reply.send(Err(invalid_state("cannot drain live credit reservations")));
        return Ok(());
    }
    if link.transmitting.is_some() {
        let _ = reply.send(Err(invalid_state("cannot drain a transmitting delivery")));
        return Ok(());
    }
    if !link.queued.is_empty() && link.progress.unused(pending.target) != 0 {
        let _ = reply.send(Err(invalid_state(
            "cannot return unused credit while deliveries are queued",
        )));
        return Ok(());
    }

    let result = write_drain(request.channel, request.handle, session, writer).await;
    let _ = reply.send(
        result
            .as_ref()
            .map(|_| ())
            .map_err(|error| EngineError::InvalidState(error.to_string())),
    );
    result
}

pub(in crate::server) async fn apply_flow<W: AsyncWrite + Unpin>(
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
        Some(LinkState::Detaching(_)) => return Ok(()),
        Some(LinkState::Sending(link)) => link,
        Some(LinkState::Receiving(_)) => {
            if flow.drain || flow.echo {
                // Returning unused receiver credit needs a separate drain
                // handshake. Never treat a sender's count as received data or
                // acknowledge quiescence while transfers may still be in flight.
                incoming::retire_receiver(
                    channel,
                    handle,
                    "amqp:not-implemented",
                    sessions,
                    writer,
                )
                .await?;
            }
            return Ok(());
        }
        None if session.pending_attaches.contains_key(&handle) => {
            trace!(channel, handle, "buffering flow for a pending attach");
            session.pending_flows.insert(handle, flow);
            return Ok(());
        }
        None => {
            trace!(channel, handle, "ignoring flow for an unknown link");
            return Ok(());
        }
    };
    let proposed = match link.progress.update(
        flow.delivery_count,
        flow.link_credit,
        link.credit_reservations.len(),
    ) {
        Ok(proposed) => proposed,
        Err(description) => {
            // A bad link observation cannot manufacture permission or retire
            // unrelated links. Retain the closing handle through its answer.
            if begin_detach(channel, handle, sessions)? {
                write_amqp(
                    writer,
                    channel,
                    Performative::Detach(Detach {
                        handle,
                        closed: true,
                        error: Some(Error::new(
                            crate::AmqpError::InvalidField,
                            description,
                            None,
                        )),
                    }),
                    Vec::new(),
                )
                .await?;
            }
            return Ok(());
        }
    };
    // A Flow replaces/cancels its drain generation in this same actor turn.
    // No target in the old peer coordinate survives the rebase below.
    link.progress = proposed;
    trace!(
        channel,
        handle,
        delivery_count = link.progress.count(),
        credit = link.progress.available(),
        queued = link.queued.len(),
        "link credit updated"
    );
    if flow.drain {
        let request = DrainRequest {
            channel,
            handle,
            incarnation: link.drain.incarnation,
            generation: link.drain.next_generation,
        };
        link.drain.next_generation = link.drain.next_generation.wrapping_add(1);
        link.drain.current = Some(PendingDrain {
            request,
            target: link.progress.drain_target(),
        });
        let _ = link.drain.notifications.send(Some(request));
        trace!(
            channel,
            handle,
            generation = request.generation,
            credit = link.progress.available(),
            "remote requested link drain"
        );
    } else if link.drain.current.take().is_some() {
        let _ = link.drain.notifications.send(None);
        trace!(channel, handle, "remote cancelled link drain");
    }
    flush_sends(channel, handle, session, writer, remote_max_frame_size).await
}

pub(super) fn has_unreserved_credit(link: &SendingLink) -> bool {
    link.queued.is_empty() && link.progress.can_reserve(link.credit_reservations.len())
}

pub(super) fn publish_credit(link: &SendingLink) {
    let ready = has_unreserved_credit(link);
    link.credit.send_if_modified(|current| {
        let changed = *current != ready;
        *current = ready;
        changed
    });
}

pub(super) async fn complete_zero_credit_drain<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    session: &mut SessionState,
    writer: &mut W,
) -> Result<(), EngineError> {
    let Some(LinkState::Sending(link)) = session.links.get_mut(&handle) else {
        return Ok(());
    };
    let Some(pending) = link.drain.current else {
        return Ok(());
    };
    if !link.credit_reservations.is_empty()
        || link.transmitting.is_some()
        || link.progress.unused(pending.target) != 0
    {
        return Ok(());
    }
    write_drain(channel, handle, session, writer).await
}

async fn write_drain<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    session: &mut SessionState,
    writer: &mut W,
) -> Result<(), EngineError> {
    let session_flow = incoming::session_flow(session);
    let Some(LinkState::Sending(link)) = session.links.get_mut(&handle) else {
        return Ok(());
    };
    let Some(pending) = link.drain.current else {
        return Ok(());
    };
    loop {
        let step = link
            .progress
            .drain_step(pending.target)
            .map_err(invalid_state)?;
        write_amqp(
            writer,
            channel,
            Performative::Flow(Flow {
                handle: Some(handle),
                delivery_count: Some(step.delivery_count),
                link_credit: Some(step.remaining),
                drain: true,
                ..session_flow.clone()
            }),
            Vec::new(),
        )
        .await?;
        // The original write must succeed before any count/frontier advances.
        // A full uint takes at most three observable RFC-1982 increments.
        link.progress = step.progress;
        if step.remaining == 0 {
            link.drain.current = None;
            let _ = link.drain.notifications.send(None);
            publish_credit(link);
            trace!(
                channel,
                handle,
                delivery_count = link.progress.count(),
                "link credit drained"
            );
            return Ok(());
        }
    }
}
