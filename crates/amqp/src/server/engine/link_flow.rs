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
    pub(super) credit_limit: u64,
}

pub(super) fn reserve_credit(
    channel: u16,
    handle: u32,
    incarnation: u64,
    reply: oneshot::Sender<Result<Option<CreditReservationIdentity>, EngineError>>,
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
    let reservation = CreditReservationIdentity {
        channel,
        handle,
        incarnation: link.drain.incarnation,
        reservation_id,
    };
    publish_credit(link);
    if reply.send(Ok(Some(reservation))).is_err() {
        // A canceled waiter must not consume a slot that no caller can use.
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

pub(super) async fn acknowledge_drain<W: AsyncWrite + Unpin>(
    request: DrainRequest,
    reply: oneshot::Sender<Result<(), EngineError>>,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut W,
) -> Result<(), EngineError> {
    let Some(session) = sessions.get_mut(&request.channel) else {
        let _ = reply.send(Err(EngineError::RemoteDetached));
        return Ok(());
    };
    let next_outgoing_id = session.next_outgoing_id;
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
    if !link.queued.is_empty() && u64::from(link.delivery_count) < pending.credit_limit {
        let _ = reply.send(Err(invalid_state(
            "cannot return unused credit while deliveries are queued",
        )));
        return Ok(());
    }

    let delivery_count = if pending.credit_limit > u64::from(link.delivery_count) {
        pending.credit_limit as u32
    } else {
        link.delivery_count
    };
    let unused_credit = pending
        .credit_limit
        .saturating_sub(u64::from(link.delivery_count));
    let result = write_amqp(
        writer,
        request.channel,
        Performative::Flow(drained_flow(
            request.handle,
            delivery_count,
            next_outgoing_id,
        )),
        Vec::new(),
    )
    .await;
    if result.is_ok() {
        link.delivery_count = delivery_count;
        link.credit_limit = u64::from(delivery_count);
        link.drain.current = None;
        let _ = link.drain.notifications.send(None);
        link.credit.send_if_modified(|ready| {
            let changed = *ready;
            *ready = false;
            changed
        });
        trace!(
            channel = request.channel,
            handle = request.handle,
            delivery_count,
            unused_credit,
            "unused link credit drained"
        );
    }
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
        Some(LinkState::Sending(link)) => link,
        Some(LinkState::Receiving(_)) => {
            trace!(channel, handle, "ignoring flow for a receiving link");
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
            credit_limit: link.credit_limit,
        });
        let _ = link.drain.notifications.send(Some(request));
        trace!(
            channel,
            handle,
            generation = request.generation,
            credit_limit = link.credit_limit,
            "remote requested link drain"
        );
    } else if link.drain.current.take().is_some() {
        let _ = link.drain.notifications.send(None);
        trace!(channel, handle, "remote cancelled link drain");
    }
    flush_sends(channel, handle, session, writer, remote_max_frame_size).await
}

pub(super) fn has_unreserved_credit(link: &SendingLink) -> bool {
    link.queued.is_empty()
        && u64::from(link.delivery_count).saturating_add(link.credit_reservations.len() as u64)
            < link.credit_limit
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
    let next_outgoing_id = session.next_outgoing_id;
    let Some(LinkState::Sending(link)) = session.links.get_mut(&handle) else {
        return Ok(());
    };
    let Some(pending) = link.drain.current else {
        return Ok(());
    };
    if !link.credit_reservations.is_empty() || u64::from(link.delivery_count) < pending.credit_limit
    {
        return Ok(());
    }
    let delivery_count = link.delivery_count;
    write_amqp(
        writer,
        channel,
        Performative::Flow(drained_flow(handle, delivery_count, next_outgoing_id)),
        Vec::new(),
    )
    .await?;
    link.credit_limit = u64::from(delivery_count);
    link.drain.current = None;
    let _ = link.drain.notifications.send(None);
    trace!(channel, handle, delivery_count, "zero link credit drained");
    Ok(())
}

fn drained_flow(handle: u32, delivery_count: u32, next_outgoing_id: u32) -> Flow {
    Flow {
        next_incoming_id: Some(0),
        incoming_window: SESSION_WINDOW,
        next_outgoing_id,
        outgoing_window: SESSION_WINDOW,
        handle: Some(handle),
        delivery_count: Some(delivery_count),
        link_credit: Some(0),
        available: None,
        drain: true,
        echo: false,
        properties: None,
    }
}
