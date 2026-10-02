use super::endpoints::NativeAcceptance;
use super::group::{ControlData, PostData};
use super::*;

pub(in crate::server) async fn handle_native_command<W: AsyncWrite + Unpin>(
    command: NativeCommand,
    book: &mut NativeTransactionBook,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    if book.policy() == NativeIngressPolicy::Disabled {
        command.reject(native_error(NativeTransactionError::Disabled));
        return Ok(());
    }
    match command {
        NativeCommand::AcceptCoordinator { acceptance, reply } => {
            let NativeAcceptance {
                channel,
                session,
                attach,
                maximum,
                sink,
                detached,
                consumption,
                commands,
            } = *acceptance;
            let handle = attach.approval().local_handle();
            let NativeAttachKind::Coordinator(profile) = attach.approval().kind() else {
                let _ = reply.send(Err(native_error(NativeTransactionError::InvalidAttach)));
                return Ok(());
            };
            let maximum =
                effective_receive_maximum(Some(maximum)).min(MAX_NATIVE_TRANSACTION_CONTROL_BYTES);
            let accepted = accept_native_receiving(
                channel,
                session,
                attach,
                maximum,
                sink,
                detached,
                consumption,
                sessions,
                writer,
            )
            .await;
            let result = match accepted {
                Ok(owner) => book
                    .accept_controller(channel, handle, owner, commands, profile)
                    .map_err(native_error),
                Err(EngineError::Io(error)) => {
                    let _ = reply.send(Err(native_error(NativeTransactionError::Faulted(
                        NativeFault::Flush,
                    ))));
                    return Err(error.into());
                }
                Err(error) => Err(error),
            };
            let _ = reply.send(result);
        }
        NativeCommand::AcceptReceiver { acceptance, reply } => {
            let NativeAcceptance {
                channel,
                session,
                attach,
                maximum,
                sink,
                detached,
                consumption,
                commands,
            } = *acceptance;
            let handle = attach.approval().local_handle();
            let accepted = accept_native_receiving(
                channel,
                session,
                attach,
                maximum,
                sink,
                detached,
                consumption,
                sessions,
                writer,
            )
            .await;
            let result = match accepted {
                Ok(owner) => book
                    .accept_receiver(channel, handle, owner.clone(), commands)
                    .map(|()| owner)
                    .map_err(native_error),
                Err(EngineError::Io(error)) => {
                    let _ = reply.send(Err(native_error(NativeTransactionError::Faulted(
                        NativeFault::Flush,
                    ))));
                    return Err(error.into());
                }
                Err(error) => Err(error),
            };
            let _ = reply.send(result);
        }
        NativeCommand::Declare {
            mut data,
            id,
            reply,
        } => {
            if let Err(error) = checked_session(&data.route, sessions).and_then(|session| {
                session
                    .incoming
                    .preflight_transactional_provisional(
                        &data.route.owner,
                        &data.delivery.inner().identity,
                    )
                    .map_err(|_| native_error(NativeTransactionError::Retired))
            }) {
                data.controller.0.close();
                let _ = reply.send(Err(error));
                return Ok(());
            }
            let group = match book.register_declare(&data, id.clone()) {
                Ok(group) => group,
                Err(error) => {
                    let channel = data.route.channel;
                    let refusal = NativeControlRefusal::from_data(error, data, true);
                    if let Some(session) = sessions.get_mut(&channel) {
                        handle_control_refusal(refusal, session, writer).await?;
                    }
                    let _ = reply.send(Err(native_error(error)));
                    return Ok(());
                }
            };
            let state = DeliveryState::Declared(crate::Declared { txn_id: id });
            if let Err(error) = control_outcome(&data, state, sessions, writer).await {
                group.fault(match &error {
                    NativeIoError::Local(_) => NativeFault::Stage,
                    NativeIoError::Write(_) => NativeFault::Flush,
                });
                return reply_failure(reply, error);
            }
            data.disarm();
            if reply
                .send(Ok(NativeTransactionIdentity(group.clone())))
                .is_err()
            {
                group.fault(NativeFault::Dropped);
            }
        }
        NativeCommand::Provisional { data, reply } => {
            if !matches!(
                data.group.state(),
                NativeTransactionState::Pending | NativeTransactionState::Sealed
            ) {
                let _ = reply.send(Err(native_error(data.group.error())));
                return Ok(());
            }
            match provisional(&data, sessions, writer).await {
                Ok(()) => {
                    data.obligation.flushed();
                    data.group.refresh_ready();
                    let _ = reply.send(Ok(PreparedPosting { data: *data }));
                }
                Err(NativeIoError::Local(error)) => {
                    data.group.fault(NativeFault::Stage);
                    let _ = reply.send(Err(error));
                }
                Err(NativeIoError::Write(error)) => {
                    data.group.fault(NativeFault::Flush);
                    let _ = reply.send(Err(native_error(NativeTransactionError::Faulted(
                        NativeFault::Flush,
                    ))));
                    return Err(error);
                }
            }
        }
        NativeCommand::Rollback { mut data, reply } => {
            if !data.fail
                || !(data.terminal_abort
                    || data
                        .group
                        .as_ref()
                        .is_some_and(|group| group.state() == NativeTransactionState::Aborted))
            {
                let _ = reply.send(Err(native_error(NativeTransactionError::InvalidDecision)));
                return Ok(());
            }
            if let Some(group) = &data.group {
                for obligation in group.obligations() {
                    if let Err(error) = abort_post(&obligation, sessions, writer).await {
                        return reply_failure(reply, error);
                    }
                }
            }
            if let Err(error) =
                control_outcome(&data, DeliveryState::Accepted(Accepted), sessions, writer).await
            {
                return reply_failure(reply, error);
            }
            data.disarm();
            let _ = reply.send(Ok(()));
        }
        NativeCommand::Finish {
            mut control,
            postings,
            reply,
        } => {
            let state = control.group.as_ref().map(|group| group.state());
            match state {
                Some(
                    NativeTransactionState::Committed
                    | NativeTransactionState::Rejected
                    | NativeTransactionState::Aborted,
                ) => {}
                Some(NativeTransactionState::Indeterminate) => {
                    control.controller.0.close();
                    if sessions.get(&control.route.channel).and_then(|session| session.links.get(&control.route.handle)).is_some_and(|link| matches!(link, LinkState::Receiving(link) if link.identity.same_link(&control.route.owner))) {
                        detach_link_error(control.route.channel, control.route.handle, sessions.get_mut(&control.route.channel).ok_or(EngineError::RemoteDetached)?, writer, "amqp:internal-error", "native transaction owner decision is indeterminate").await?;
                    }
                    let _ = reply.send(Err(native_error(NativeTransactionError::InvalidDecision)));
                    return Ok(());
                }
                _ => {
                    let _ = reply.send(Err(native_error(NativeTransactionError::InvalidDecision)));
                    return Ok(());
                }
            }
            for posting in &postings {
                let outcome = (state == Some(NativeTransactionState::Committed))
                    .then_some(DeliveryState::Accepted(Accepted));
                if let Err(error) = finish_post(&posting.data, outcome, sessions, writer).await {
                    return reply_failure(reply, error);
                }
            }
            if state != Some(NativeTransactionState::Committed) {
                let channel = control.route.channel;
                let refusal = NativeControlRefusal::from_data(
                    NativeTransactionError::Faulted(NativeFault::Aborted),
                    control,
                    true,
                );
                let Some(session) = sessions.get_mut(&channel) else {
                    let _ = reply.send(Err(EngineError::RemoteDetached));
                    return Ok(());
                };
                if let Err(error) = handle_control_refusal(refusal, session, writer).await {
                    let error = if matches!(&error, EngineError::Io(_)) {
                        NativeIoError::Write(error)
                    } else {
                        NativeIoError::Local(error)
                    };
                    return reply_failure(reply, error);
                }
                let _ = reply.send(Ok(()));
                return Ok(());
            }
            if let Err(error) = control_outcome(
                &control,
                DeliveryState::Accepted(Accepted),
                sessions,
                writer,
            )
            .await
            {
                return reply_failure(reply, error);
            }
            control.disarm();
            let _ = reply.send(Ok(()));
        }
    }
    Ok(())
}

enum NativeIoError {
    Local(EngineError),
    Write(EngineError),
}

fn reply_failure<T>(
    reply: oneshot::Sender<Result<T, EngineError>>,
    error: NativeIoError,
) -> Result<(), EngineError> {
    match error {
        NativeIoError::Local(error) => {
            let _ = reply.send(Err(error));
            Ok(())
        }
        NativeIoError::Write(error) => {
            let _ = reply.send(Err(native_error(NativeTransactionError::Faulted(
                NativeFault::Flush,
            ))));
            Err(error)
        }
    }
}

pub(in crate::server) async fn handle_control_refusal<W: AsyncWrite + Unpin>(
    mut refusal: Box<NativeControlRefusal>,
    session: &mut SessionState,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    let Some(LinkState::Receiving(link)) = session.links.get_mut(&refusal.handle) else {
        return Ok(());
    };
    if !link.identity.same_link(&refusal.owner) || refusal.owner.is_retired() {
        return Ok(());
    }
    if !refusal.published {
        link.credit
            .abort_delivery()
            .map_err(|_| invalid_state("native control refusal has no occupied delivery"))?;
    }
    if !refusal.supports_rejected()
        || refusal.error == NativeTransactionError::Faulted(NativeFault::PartialAtSeal)
    {
        return detach_link_error(
            refusal.channel,
            refusal.handle,
            session,
            writer,
            refusal.condition(),
            refusal.description(),
        )
        .await;
    }
    let action = session
        .incoming
        .settlement(&refusal.owner, refusal.identity())
        .map_err(|_| native_error(NativeTransactionError::Retired))?;
    if let SettlementAction::SendDisposition { settled } = action {
        writer
            .write_amqp(
                refusal.channel,
                Performative::Disposition(Disposition {
                    role: Role::Receiver,
                    first: refusal.identity().id(),
                    last: None,
                    settled,
                    state: Some(DeliveryState::Rejected(crate::Rejected {
                        error: Some(Error::new(
                            crate::ErrorCondition::Custom(Symbol::from(refusal.condition())),
                            refusal.description(),
                            None,
                        )),
                    })),
                    batchable: false,
                }),
                Vec::new(),
            )
            .await?;
    }
    session
        .incoming
        .commit_settlement(&refusal.owner, refusal.identity())
        .map_err(|_| native_error(NativeTransactionError::Retired))?;
    refusal.disarm();
    Ok(())
}

fn checked_session<'a>(
    route: &super::group::NativeRoute,
    sessions: &'a mut HashMap<u16, SessionState>,
) -> Result<&'a mut SessionState, EngineError> {
    let session = sessions
        .get_mut(&route.channel)
        .ok_or(EngineError::RemoteDetached)?;
    let Some(LinkState::Receiving(link)) = session.links.get(&route.handle) else {
        return Err(EngineError::RemoteDetached);
    };
    if session.ending || !link.identity.same_link(&route.owner) || route.owner.is_retired() {
        return Err(EngineError::RemoteDetached);
    }
    Ok(session)
}

async fn control_outcome<W: AsyncWrite + Unpin>(
    data: &ControlData,
    state: DeliveryState,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
) -> Result<(), NativeIoError> {
    let session = checked_session(&data.route, sessions).map_err(NativeIoError::Local)?;
    let identity = &data.delivery.inner().identity;
    let action = session
        .incoming
        .settlement(&data.route.owner, identity)
        .map_err(|_| NativeIoError::Local(native_error(NativeTransactionError::Retired)))?;
    if let SettlementAction::SendDisposition { settled } = action {
        writer
            .write_amqp(
                data.route.channel,
                Performative::Disposition(Disposition {
                    role: Role::Receiver,
                    first: identity.id(),
                    last: None,
                    settled,
                    state: Some(state),
                    batchable: false,
                }),
                Vec::new(),
            )
            .await
            .map_err(|error| NativeIoError::Write(error.into()))?;
    }
    session
        .incoming
        .commit_settlement(&data.route.owner, identity)
        .map_err(|_| NativeIoError::Local(native_error(NativeTransactionError::Retired)))
}

async fn provisional<W: AsyncWrite + Unpin>(
    data: &PostData,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
) -> Result<(), NativeIoError> {
    let session = checked_session(&data.route, sessions).map_err(NativeIoError::Local)?;
    let identity = &data.delivery.inner().identity;
    session
        .incoming
        .preflight_transactional_provisional(&data.route.owner, identity)
        .map_err(|_| NativeIoError::Local(native_error(NativeTransactionError::Retired)))?;
    writer
        .write_amqp(
            data.route.channel,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: identity.id(),
                last: None,
                settled: false,
                state: Some(DeliveryState::Transactional(TransactionalState {
                    txn_id: data.group.id.clone(),
                    outcome: Some(Outcome::Accepted(Accepted)),
                })),
                batchable: false,
            }),
            Vec::new(),
        )
        .await
        .map_err(|error| NativeIoError::Write(error.into()))?;
    // This is not ordinary commit_settlement: the independent obligation remains.
    session
        .incoming
        .mark_transactional_provisional(&data.route.owner, identity)
        .map_err(|_| NativeIoError::Local(native_error(NativeTransactionError::Retired)))
}

async fn finish_post<W: AsyncWrite + Unpin>(
    data: &PostData,
    outcome: Option<DeliveryState>,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
) -> Result<(), NativeIoError> {
    let Some(session) = sessions.get_mut(&data.route.channel) else {
        return Ok(());
    };
    let identity = &data.delivery.inner().identity;
    let action = session
        .incoming
        .finish_transactional_post(&data.route.owner, identity)
        .map_err(|_| {
            NativeIoError::Local(native_error(NativeTransactionError::InvalidPreparedSet))
        })?;
    if let SettlementAction::SendDisposition { settled } = action {
        writer
            .write_amqp(
                data.route.channel,
                Performative::Disposition(Disposition {
                    role: Role::Receiver,
                    first: identity.id(),
                    last: None,
                    settled,
                    state: outcome,
                    batchable: false,
                }),
                Vec::new(),
            )
            .await
            .map_err(|error| NativeIoError::Write(error.into()))?;
        session
            .incoming
            .commit_transactional_post(&data.route.owner, identity)
            .map_err(|_| {
                NativeIoError::Local(native_error(NativeTransactionError::InvalidPreparedSet))
            })?;
    }
    Ok(())
}

async fn abort_post<W: AsyncWrite + Unpin>(
    obligation: &super::group::Obligation,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
) -> Result<(), NativeIoError> {
    let Some(session) = sessions.get_mut(&obligation.channel) else {
        return Ok(());
    };
    if !session.links.get(&obligation.handle).is_some_and(|link| matches!(link, LinkState::Receiving(link) if link.identity.same_link(&obligation.owner))) { return Ok(()); }
    let action = session
        .incoming
        .finish_transactional_abort(&obligation.owner, &obligation.identity)
        .map_err(|_| {
            NativeIoError::Local(native_error(NativeTransactionError::InvalidPreparedSet))
        })?;
    if let SettlementAction::SendDisposition { settled } = action {
        writer
            .write_amqp(
                obligation.channel,
                Performative::Disposition(Disposition {
                    role: Role::Receiver,
                    first: obligation.identity.id(),
                    last: None,
                    settled,
                    state: None,
                    batchable: false,
                }),
                Vec::new(),
            )
            .await
            .map_err(|error| NativeIoError::Write(error.into()))?;
    }
    session
        .incoming
        .commit_transactional_abort(&obligation.owner, &obligation.identity)
        .map_err(|_| NativeIoError::Local(native_error(NativeTransactionError::InvalidPreparedSet)))
}
