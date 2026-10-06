//! Refuse an original pending ordinary Attach without installing an endpoint.

use super::*;

impl ServerSession {
    /// Refuses a pending ordinary link using its original admission receipt.
    ///
    /// A normal refusal flushes a null-terminus Attach followed by error Detach.
    /// No ordinary endpoint or link credit is installed. Existing session/frame
    /// refusal limits still apply. Cancellation after enqueue does not undo the
    /// actor-owned refusal or its acknowledgment ownership.
    ///
    /// ```no_run
    /// async fn refuse(
    ///     session: &amqp::ServerSession,
    ///     request: amqp::IncomingAttach,
    /// ) -> Result<(), amqp::EngineError> {
    ///     session.reject_attach(request, amqp::Error::new(
    ///         amqp::AmqpError::UnauthorizedAccess, "link is not authorized", None,
    ///     )).await
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// fn raw_request(
    ///     session: &amqp::ServerSession, request: amqp::Attach, error: amqp::Error,
    /// ) {
    ///     let _ = session.reject_attach(request, error);
    /// }
    /// ```
    pub async fn reject_attach(
        &self,
        attach: IncomingAttach,
        error: Error,
    ) -> Result<(), EngineError> {
        validate_request(&attach, &self.identity)?;
        request(&self.commands, |reply| Command::RejectAttach {
            channel: self.channel,
            session: self.identity.clone(),
            attach: Box::new(attach),
            error,
            reply,
        })
        .await
    }
}

fn validate_request(attach: &IncomingAttach, owner: &SessionIdentity) -> Result<(), EngineError> {
    attach
        .validate_request(owner)
        .map_err(attach_approval_error)?;
    native_transactions::validate_accept_kind(
        attach,
        native_transactions::NativeAttachKind::Ordinary,
    )?;
    if has_recovery_state(attach) {
        return Err(invalid_state(RECOVERY_NOT_IMPLEMENTED));
    }
    if attach_uses_transactions(attach) {
        return Err(invalid_state(TRANSACTIONS_NOT_IMPLEMENTED));
    }
    source_default_outcome(attach.source.as_ref())?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_rejection<W: AsyncWrite + Unpin>(
    channel: u16,
    owner: SessionIdentity,
    attach: IncomingAttach,
    error: Error,
    reply: oneshot::Sender<Result<(), EngineError>>,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut FrameWriter<W>,
) -> Result<(), EngineError> {
    let checked = (|| {
        let session = sessions.get(&channel).ok_or(EngineError::RemoteDetached)?;
        if owner.is_retired() || session.ending || session.identity.is_retired() {
            return Err(EngineError::RemoteDetached);
        }
        if !session.identity.same_session(&owner) {
            return Err(invalid_state(
                "attach approval belongs to a different session generation",
            ));
        }
        validate_request(&attach, &owner)?;
        let handle = attach.approval().local_handle();
        let approval = session
            .pending_attaches
            .get(&handle)
            .and_then(|pending| pending.approval.as_ref())
            .ok_or(EngineError::RemoteDetached)?;
        attach
            .validate(&session.identity, approval)
            .map_err(attach_approval_error)?;
        if !current_alias(handle, session).is_some_and(|alias| {
            alias.identity.same_link(approval.link_identity())
                && alias.peer_handle == Some(attach.handle)
                && alias.name.as_ref() == approval.name().as_ref()
                && alias.role == approval.local_role()
                && !alias.own_attach_sent
        }) {
            return Err(invalid_state(
                "attach approval has no matching pending handle alias",
            ));
        }
        if session.links.contains_key(&handle) || session.closing_handles.contains(&handle) {
            return Err(invalid_state(
                "link handle is attached or awaiting detach acknowledgement",
            ));
        }
        if session.pending_attaches[&handle].recovery_refusal {
            return Err(invalid_state(RECOVERY_NOT_IMPLEMENTED));
        }
        Ok((handle, Arc::clone(approval)))
    })();
    let (handle, approval) = match checked {
        Ok(checked) => checked,
        Err(error) => {
            let _ = reply.send(Err(error));
            return Ok(());
        }
    };
    let session = sessions.get_mut(&channel).expect("validated session");
    close_pending_link(
        channel,
        handle,
        &approval,
        session,
        writer,
        Some(error),
        false,
    )
    .await?;
    let result = if session.ending {
        Err(EngineError::RemoteDetached)
    } else {
        Ok(())
    };
    let _ = reply.send(result);
    Ok(())
}

#[cfg(test)]
mod tests;
