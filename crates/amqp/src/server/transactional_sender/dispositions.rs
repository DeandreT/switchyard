use super::*;
use native_transactions::NativeRetirementCandidate;

struct Selected {
    handle: u32,
    id: u32,
    original: NativeOutgoingDeliveryIdentity,
    consumer: Arc<ConsumerControl>,
    route: NativeRoute,
    permit: mpsc::OwnedPermit<TransactionalDisposition>,
    outcome: Option<Outcome>,
}

fn in_range(id: u32, disposition: &Disposition) -> bool {
    id.wrapping_sub(disposition.first)
        <= disposition
            .last
            .unwrap_or(disposition.first)
            .wrapping_sub(disposition.first)
}

pub(in crate::server) async fn apply_native_outgoing_disposition<W: AsyncWrite + Unpin>(
    channel: u16,
    disposition: &Disposition,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
    book: &mut NativeTransactionBook,
) -> Result<bool, EngineError> {
    if !book.policy().supports_retirement() || disposition.role != Role::Receiver {
        return Ok(false);
    }
    let Some(session) = sessions.get(&channel) else {
        return Ok(false);
    };
    if session.ending
        || session.identity.is_retired()
        || session.error_deliveries.contains_range(
            &disposition.role,
            disposition.first,
            disposition.last,
        )
    {
        return Ok(false);
    }
    let transactional = match &disposition.state {
        Some(DeliveryState::Transactional(state)) => Some(state),
        Some(DeliveryState::Declared(_)) => return Ok(false),
        _ => None,
    };
    // A mixed ordinary/native range is not admitted into a transaction at all.
    if transactional.is_some()
        && session.links.values().any(|link| {
            matches!(link, LinkState::Sending(link) if link.unsettled.iter().any(|(id, row)|
            in_range(*id, disposition) && row.reply.transactional().is_none()))
        })
    {
        return Ok(false);
    }

    let mut selected = Vec::new();
    let mut failure = None;
    for (&handle, link) in &session.links {
        let LinkState::Sending(link) = link else {
            continue;
        };
        if link.identity.is_retired() {
            continue;
        }
        for (&id, row) in &link.unsettled {
            if !in_range(id, disposition) {
                continue;
            }
            let Some(reply) = row.reply.transactional() else {
                continue;
            };
            if !row.delivery_identity.owner().same_link(&link.identity)
                || row.delivery_identity.id() != id
            {
                failure = Some((handle, NativeTransactionError::InvalidPreparedSet));
                break;
            }
            if matches!(disposition.state, Some(DeliveryState::Received { .. })) {
                failure = Some((handle, NativeTransactionError::Unsupported));
                break;
            }
            if let Some(state) = transactional {
                if disposition.settled
                    || !matches!(state.outcome, Some(Outcome::Accepted(_)))
                    || !reply.fully_flushed
                    || link
                        .active
                        .as_ref()
                        .is_some_and(|active| active.delivery_id == id)
                    || row.receiver_settled
                    || row.outcome.is_some()
                {
                    failure = Some((handle, NativeTransactionError::Unsupported));
                    break;
                }
                if let Some(attempt) = &row.retirement {
                    if attempt.transaction_id() == &state.txn_id {
                        continue;
                    }
                    failure = Some((handle, NativeTransactionError::InvalidDecision));
                    break;
                }
                if selected.len() == MAX_NATIVE_TRANSACTION_POSTINGS {
                    failure = Some((handle, NativeTransactionError::Limit));
                    break;
                }
            } else if row.retirement.is_some() {
                failure = Some((handle, NativeTransactionError::InvalidDecision));
                break;
            } else if !reply.fully_flushed {
                // Ordinary terminal outcomes retain the established early-outcome latch.
                continue;
            } else if disposition.state.is_none() && !disposition.settled {
                continue;
            } else if disposition
                .state
                .as_ref()
                .is_some_and(|state| Outcome::try_from(state.clone()).is_err())
            {
                failure = Some((handle, NativeTransactionError::Unsupported));
                break;
            }
            let permit = match reply.events.clone().try_reserve_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    failure = Some((handle, NativeTransactionError::Limit));
                    break;
                }
            };
            selected.push(Selected {
                handle,
                id,
                original: row.delivery_identity.clone(),
                consumer: reply.consumer().clone(),
                route: reply.route.clone(),
                permit,
                outcome: disposition
                    .state
                    .clone()
                    .and_then(|state| Outcome::try_from(state).ok())
                    .or_else(|| {
                        disposition
                            .settled
                            .then(|| link.default_outcome.clone())
                            .flatten()
                    }),
            });
        }
        if failure.is_some() {
            break;
        }
    }
    if let Some((handle, error)) = failure {
        if transactional.is_some() && matches!(error, NativeTransactionError::Limit) {
            book.fault_retirement(disposition.state.as_ref(), NativeFault::Stage);
        }
        drop(selected);
        let session = sessions
            .get_mut(&channel)
            .ok_or(EngineError::RemoteDetached)?;
        detach_link_error(
            channel,
            handle,
            session,
            writer,
            error.condition(),
            error.description(),
        )
        .await?;
        return Ok(true);
    }
    if selected.is_empty() {
        return Ok(transactional.is_some()
            && session.links.values().any(|link| {
                matches!(link, LinkState::Sending(link) if link.unsettled.iter().any(|(id,row)|
                in_range(*id, disposition) && row.reply.transactional().is_some()))
            }));
    }

    if let Some(state) = transactional {
        if let Err((handle, error)) = install_retirements(channel, state, selected, sessions, book)
        {
            detach_link_error(
                channel,
                handle,
                sessions
                    .get_mut(&channel)
                    .ok_or(EngineError::RemoteDetached)?,
                writer,
                error.condition(),
                error.description(),
            )
            .await?;
        }
        Ok(true)
    } else {
        let session = sessions
            .get_mut(&channel)
            .ok_or(EngineError::RemoteDetached)?;
        for entry in selected {
            let Some(LinkState::Sending(link)) = session.links.get_mut(&entry.handle) else {
                return Err(EngineError::RemoteDetached);
            };
            let row = link
                .unsettled
                .remove(&entry.id)
                .ok_or(EngineError::RemoteDetached)?;
            let Some(outcome) = entry.outcome else {
                link.outstanding_tags.remove(row.delivery_tag.as_ref());
                let _ = row
                    .reply
                    .send(Err(EngineError::RemoteSettledWithoutOutcome));
                continue;
            };
            let acknowledgement = (!disposition.settled).then(|| {
                AckIdentity::for_delivery(&row.delivery_identity, row.delivery_tag.as_ref())
            });
            if let Some(identity) = &acknowledgement {
                link.pending_acknowledgements
                    .insert(entry.id, identity.clone());
            } else {
                link.outstanding_tags.remove(row.delivery_tag.as_ref());
            }
            entry
                .permit
                .send(TransactionalDisposition::Ordinary(PendingSettlement {
                    outcome,
                    identity: entry.route.owner,
                    delivery_identity: row.delivery_identity,
                    acknowledgement,
                    channel,
                    handle: entry.handle,
                    commands: entry.route.commands,
                }));
        }
        Ok(false)
    }
}

fn install_retirements(
    channel: u16,
    state: &crate::TransactionalState,
    selected: Vec<Selected>,
    sessions: &mut HashMap<u16, SessionState>,
    book: &mut NativeTransactionBook,
) -> Result<(), (u32, NativeTransactionError)> {
    let Some(first) = selected.first() else {
        return Ok(());
    };
    let handle = first.handle;
    let controls: Vec<_> = selected
        .iter()
        .map(|entry| entry.consumer.clone())
        .collect();
    if controls.iter().any(|control| {
        let guard = control.lock();
        !guard.alive || guard.attempt.is_some()
    }) {
        book.fault_retirement(
            Some(&DeliveryState::Transactional(state.clone())),
            NativeFault::Stage,
        );
        return Err((handle, NativeTransactionError::InvalidDecision));
    }
    let candidates: Vec<_> = selected
        .iter()
        .map(|entry| NativeRetirementCandidate {
            channel,
            handle: entry.handle,
            owner: entry.route.owner.clone(),
            delivery_identity: entry.original.clone(),
        })
        .collect();
    let attempts = book
        .begin_retirements(state, &candidates)
        .map_err(|error| (handle, error))?;
    if attempts.len() != selected.len() {
        for attempt in attempts {
            attempt.fault(NativeFault::Stage);
        }
        return Err((handle, NativeTransactionError::InvalidPreparedSet));
    }
    let mut guards: Vec<_> = controls.iter().map(|control| control.lock()).collect();
    if guards
        .iter()
        .any(|guard| !guard.alive || guard.attempt.is_some())
    {
        drop(guards);
        for attempt in attempts {
            attempt.fault(NativeFault::Stage);
        }
        return Err((handle, NativeTransactionError::InvalidDecision));
    }
    let Some(session) = sessions.get_mut(&channel) else {
        drop(guards);
        for attempt in attempts {
            attempt.fault(NativeFault::Stage);
        }
        return Err((handle, NativeTransactionError::Retired));
    };
    let valid = selected.iter().all(|entry| {
        matches!(session.links.get(&entry.handle), Some(LinkState::Sending(link))
            if link.unsettled.get(&entry.id).is_some_and(|row| row.delivery_identity.same_delivery(&entry.original)))
    });
    if !valid {
        drop(guards);
        for attempt in attempts {
            attempt.fault(NativeFault::Stage);
        }
        return Err((handle, NativeTransactionError::Retired));
    }
    let mut missing = None;
    for ((entry, attempt), guard) in selected.iter().zip(&attempts).zip(guards.iter_mut()) {
        if let Some(LinkState::Sending(link)) = session.links.get_mut(&entry.handle)
            && let Some(row) = link.unsettled.get_mut(&entry.id)
        {
            guard.attempt = Some(attempt.clone());
            row.retirement = Some(attempt.clone());
        } else {
            missing = Some(entry.handle);
            break;
        }
    }
    drop(guards);
    if let Some(handle) = missing {
        for attempt in attempts {
            attempt.fault(NativeFault::Stage);
        }
        return Err((handle, NativeTransactionError::Retired));
    }
    for (entry, attempt) in selected.into_iter().zip(attempts) {
        entry.permit.send(TransactionalDisposition::Retirement(
            attempt.receipt(entry.route.commands),
        ));
    }
    Ok(())
}

fn current_row<'a>(
    attempt: &NativeRetirementAttempt,
    sessions: &'a mut HashMap<u16, SessionState>,
) -> Option<&'a mut SendingLink> {
    let session = sessions.get_mut(&attempt.channel())?;
    if session.ending || session.identity.is_retired() || attempt.owner().is_retired() {
        return None;
    }
    let LinkState::Sending(link) = session.links.get_mut(&attempt.handle())? else {
        return None;
    };
    if !link.identity.same_link(attempt.owner()) {
        return None;
    }
    let row = link.unsettled.get(&attempt.delivery_identity().id())?;
    if !row
        .delivery_identity
        .same_delivery(attempt.delivery_identity())
        || !row
            .retirement
            .as_ref()
            .is_some_and(|current| current.same_attempt(attempt))
    {
        return None;
    }
    Some(link)
}

pub(in crate::server) async fn provisional_native_retirement<W: AsyncWrite + Unpin>(
    attempt: &NativeRetirementAttempt,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    let link = current_row(attempt, sessions).ok_or(EngineError::RemoteDetached)?;
    let id = attempt.delivery_identity().id();
    let row = link.unsettled.get(&id).ok_or(EngineError::RemoteDetached)?;
    if !matches!(
        attempt.state(),
        NativeTransactionState::Pending | NativeTransactionState::Sealed
    ) || attempt.obligation.is_prepared()
        || row.receiver_settled
        || row.outcome.is_some()
        || !row
            .reply
            .transactional()
            .is_some_and(|reply| reply.fully_flushed)
    {
        return Err(invalid_state(
            "native retirement is not eligible for a provisional response",
        ));
    }
    let frame = Frame::Amqp {
        channel: attempt.channel(),
        performative: Some(Performative::Disposition(Disposition {
            role: Role::Sender,
            first: id,
            last: None,
            settled: false,
            state: Some(DeliveryState::Transactional(crate::TransactionalState {
                txn_id: attempt.transaction_id().clone(),
                outcome: Some(Outcome::Accepted(Accepted)),
            })),
            batchable: false,
        })),
        payload: Vec::new(),
    };
    writer.encoded_frame(&frame)?;
    writer.write_frame(&frame).await?;
    Ok(())
}

fn restore(attempt: &NativeRetirementAttempt, sessions: &mut HashMap<u16, SessionState>) {
    let Some(link) = current_row(attempt, sessions) else {
        return;
    };
    if !matches!(
        attempt.state(),
        NativeTransactionState::Faulted
            | NativeTransactionState::Aborted
            | NativeTransactionState::Rejected
    ) {
        return;
    }
    let Some(row) = link.unsettled.get_mut(&attempt.delivery_identity().id()) else {
        return;
    };
    if let Some(reply) = row.reply.transactional() {
        reply.consumer().clear(attempt);
    }
    row.outcome = None;
    row.receiver_settled = false;
    row.retirement = None;
}

pub(in crate::server) async fn finish_native_retirement<W: AsyncWrite + Unpin>(
    attempt: &NativeRetirementAttempt,
    committed: bool,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    if !committed {
        restore(attempt, sessions);
        return Ok(());
    }
    if attempt.state() != NativeTransactionState::Committed {
        return Err(invalid_state("native retirement has no committed decision"));
    }
    let Some(link) = current_row(attempt, sessions) else {
        return Ok(());
    };
    let id = attempt.delivery_identity().id();
    let frame = Frame::Amqp {
        channel: attempt.channel(),
        performative: Some(Performative::Disposition(Disposition {
            role: Role::Sender,
            first: id,
            last: None,
            settled: true,
            state: Some(DeliveryState::Accepted(Accepted)),
            batchable: false,
        })),
        payload: Vec::new(),
    };
    writer.encoded_frame(&frame)?;
    writer.write_frame(&frame).await?;
    if let Some(row) = link.unsettled.remove(&id) {
        if let Some(reply) = row.reply.transactional() {
            reply.consumer().clear(attempt);
        }
        link.outstanding_tags.remove(row.delivery_tag.as_ref());
    }
    Ok(())
}

pub(in crate::server) async fn reconcile_native_retirements<W: AsyncWrite + Unpin>(
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    let mut attempts = Vec::new();
    for session in sessions.values() {
        for link in session.links.values() {
            if let LinkState::Sending(link) = link {
                attempts.extend(
                    link.unsettled
                        .values()
                        .filter_map(|row| row.retirement.clone()),
                );
            }
        }
    }
    for attempt in attempts {
        match attempt.state() {
            NativeTransactionState::Faulted
            | NativeTransactionState::Aborted
            | NativeTransactionState::Rejected => restore(&attempt, sessions),
            NativeTransactionState::Indeterminate if current_row(&attempt, sessions).is_some() => {
                detach_link_error(
                    attempt.channel(),
                    attempt.handle(),
                    sessions
                        .get_mut(&attempt.channel())
                        .ok_or(EngineError::RemoteDetached)?,
                    writer,
                    "amqp:internal-error",
                    "native retirement owner decision is indeterminate",
                )
                .await?;
            }
            _ => {}
        }
    }
    Ok(())
}
