//! Keep a null-operative-terminus response pending until its original Detach.

use super::*;

pub(super) fn is_refusal(attach: &Attach) -> bool {
    match attach.role {
        Role::Sender => attach.source.is_none(),
        Role::Receiver => attach.target.is_none(),
    }
}

pub(super) fn park(
    attach: Attach,
    channel: u16,
    pending_attaches: &mut PendingAttaches,
    sessions: &mut HashMap<u16, SessionState>,
) -> Result<(), EngineError> {
    let role = attach.role.opposite();
    let pending = pending_attaches
        .get(&attach.name, &role)
        .expect("validated directional pending attach");
    let session = sessions
        .get_mut(&channel)
        .expect("validated pending session");
    if matches!(&pending.link, LinkState::Receiving(_))
        && session
            .pending_attaches
            .get(&pending.handle)
            .and_then(|flow| flow.initial_sender_count)
            .is_some_and(|count| Some(count) != attach.initial_delivery_count)
    {
        return Err(invalid_state(
            "sender attach disagrees with the pending delivery count",
        ));
    }
    let handle = pending.handle;
    session
        .handle_aliases
        .get_mut(&handle)
        .expect("validated pending handle alias")
        .peer_handle = Some(attach.handle);
    session.error_peer_handles.reassign(attach.handle);
    let name = attach.name.clone();
    pending_attaches
        .get_mut(&name, &role)
        .expect("validated directional pending attach")
        .refused_response = Some(attach);
    Ok(())
}

pub(super) struct RetiredAttachReply {
    handle: u32,
    reply: oneshot::Sender<Result<(u32, Attach), EngineError>>,
    response: Attach,
}

impl RetiredAttachReply {
    pub(super) fn publish(self) {
        let _ = self.reply.send(Ok((self.handle, self.response)));
    }
}

pub(super) fn pending_detach(pending: PendingAttach) -> Option<RetiredAttachReply> {
    let PendingAttach {
        handle,
        reply,
        mut link,
        refused_response,
        ..
    } = pending;
    stop_link(&mut link);
    drop(link);
    match refused_response {
        Some(response) => Some(RetiredAttachReply {
            handle,
            reply,
            response,
        }),
        None => {
            let _ = reply.send(Err(EngineError::RemoteDetached));
            None
        }
    }
}

#[cfg(test)]
#[path = "client_refusal/tests.rs"]
mod tests;
