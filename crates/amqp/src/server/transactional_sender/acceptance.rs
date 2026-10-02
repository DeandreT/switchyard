use super::*;

pub(in crate::server) struct NativeSenderAcceptance {
    pub(in crate::server) channel: u16,
    pub(in crate::server) session: SessionIdentity,
    pub(in crate::server) attach: IncomingAttach,
    pub(in crate::server) detached: watch::Sender<bool>,
    pub(in crate::server) commands: mpsc::Sender<Command>,
    pub(super) policy: NativeSenderSettlePolicy,
}

#[derive(Clone, Copy)]
pub(super) enum NativeSenderSettlePolicy {
    StrictUnsettled,
    NegotiateUnsettled,
}

pub(super) fn validate_sender_approval(
    attach: &IncomingAttach,
    session: &SessionIdentity,
    policy: NativeSenderSettlePolicy,
) -> Result<(), EngineError> {
    attach
        .validate_request(session)
        .map_err(attach_approval_error)?;
    native_transactions::validate_accept_kind(
        attach,
        native_transactions::NativeAttachKind::Ordinary,
    )?;
    let original = attach.approval().refusal_attach();
    let valid_sender_mode = match policy {
        NativeSenderSettlePolicy::StrictUnsettled => {
            original.snd_settle_mode == SenderSettleMode::Unsettled
        }
        NativeSenderSettlePolicy::NegotiateUnsettled => matches!(
            original.snd_settle_mode,
            SenderSettleMode::Mixed | SenderSettleMode::Unsettled
        ),
    };
    if original.role != Role::Sender
        || attach.role != Role::Receiver
        || !valid_sender_mode
        || original.snd_settle_mode != attach.snd_settle_mode
        || original.rcv_settle_mode != ReceiverSettleMode::Second
        || attach.rcv_settle_mode != ReceiverSettleMode::Second
        || has_recovery_state(attach)
        || attach_uses_transactions(attach)
    {
        return Err(invalid_state(match policy {
            NativeSenderSettlePolicy::StrictUnsettled => {
                "native retirement requires original Unsettled/Second ordinary source"
            }
            NativeSenderSettlePolicy::NegotiateUnsettled => {
                "native retirement requires original Mixed or Unsettled/Second ordinary source"
            }
        }));
    }
    Ok(())
}

pub(in crate::server) async fn accept_native_sender<W: AsyncWrite + Unpin>(
    acceptance: NativeSenderAcceptance,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
) -> Result<LinkIdentity, EngineError> {
    let NativeSenderAcceptance {
        channel,
        session: owner,
        attach,
        detached,
        policy,
        ..
    } = acceptance;
    validate_sender_approval(&attach, &owner, policy)?;
    let session = sessions
        .get_mut(&channel)
        .ok_or(EngineError::RemoteDetached)?;
    if session.ending || session.identity.is_retired() || !session.identity.same_session(&owner) {
        return Err(EngineError::RemoteDetached);
    }
    let handle = attach.approval().local_handle();
    let approval = session
        .pending_attaches
        .get(&handle)
        .and_then(|pending| pending.approval.as_ref())
        .ok_or(EngineError::RemoteDetached)?;
    attach
        .validate(&session.identity, approval)
        .map_err(attach_approval_error)?;
    if !session.handle_aliases.get(&handle).is_some_and(|alias| {
        alias.identity.same_link(approval.link_identity())
            && alias.peer_handle == Some(attach.handle)
    }) || session.pending_attaches[&handle].recovery_refusal
        || session.links.contains_key(&handle)
        || session.closing_handles.contains(&handle)
    {
        return Err(invalid_state(
            "native sender approval has no available exact alias",
        ));
    }
    let (attach, approval) = attach.into_parts();
    let identity = approval.link_identity().clone();
    let mut response = attach.response(attach.source.clone(), attach.target.clone());
    response.handle = handle;
    response.max_message_size = None;
    response.snd_settle_mode = SenderSettleMode::Unsettled;
    let default_outcome = source_default_outcome(response.source.as_ref())?;
    let frame = Frame::Amqp {
        channel,
        performative: Some(Performative::Attach(Box::new(response.clone()))),
        payload: Vec::new(),
    };
    if let Err(error) = writer.encoded_frame(&frame) {
        close_pending_link(
            channel,
            handle,
            &approval,
            session,
            writer,
            Some(Error::new(
                crate::AmqpError::FrameSizeTooSmall,
                "attach response exceeds the peer frame limit",
                None,
            )),
            false,
        )
        .await?;
        return Err(error.into());
    }
    ensure_local_begin(channel, session, writer).await?;
    writer.write_frame(&frame).await?;
    let alias = session
        .handle_aliases
        .get_mut(&handle)
        .ok_or(EngineError::RemoteDetached)?;
    alias.own_attach_sent = true;
    let credit = session
        .pending_attaches
        .get(&handle)
        .map(|pending| pending.credit.clone())
        .unwrap_or_else(|| LinkCredit::new(response.initial_delivery_count.unwrap_or(0)));
    session.links.insert(
        handle,
        LinkState::Sending(Box::new(SendingLink {
            identity: identity.clone(),
            auto_acknowledge: false,
            max_message_size: normalized_message_size(attach.max_message_size),
            settle_mode: SenderSettleMode::Unsettled,
            receiver_settle_mode: attach.rcv_settle_mode,
            default_outcome,
            outstanding_tags: HashSet::new(),
            credit,
            reservations: Default::default(),
            queued: VecDeque::new(),
            active: None,
            unsettled: HashMap::new(),
            pending_acknowledgements: HashMap::new(),
            detached,
        })),
    );
    if let Some(flow) = session
        .pending_attaches
        .remove(&handle)
        .and_then(|pending| pending.latest)
    {
        apply_link_flow(channel, flow, writer, sessions).await?;
    }
    if identity.is_retired() {
        return Err(EngineError::RemoteDetached);
    }
    Ok(identity)
}
