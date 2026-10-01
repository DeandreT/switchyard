use super::*;

pub(super) const TRANSACTIONS_NOT_IMPLEMENTED: &str = "AMQP transactions are not implemented";

pub(super) fn transaction_state(state: Option<&DeliveryState>) -> bool {
    matches!(
        state,
        Some(DeliveryState::Declared(_) | DeliveryState::Transactional(_))
    )
}

pub(super) fn source_uses_transactions(source: Option<&crate::Source>) -> bool {
    source.is_some_and(|source| {
        transaction_state(source.default_outcome.as_ref())
            || source.outcomes.as_ref().is_some_and(|outcomes| {
                outcomes.iter().any(|outcome| {
                    matches!(
                        outcome.as_str(),
                        "amqp:declared:list" | "amqp:transactional-state:list"
                    )
                })
            })
    })
}

pub(super) fn attach_uses_transactions(attach: &Attach) -> bool {
    attach
        .target
        .as_ref()
        .is_some_and(|target| target.as_coordinator().is_some())
        || source_uses_transactions(attach.source.as_ref())
}

pub(super) fn flow_uses_transactions(flow: &Flow) -> bool {
    flow.properties
        .as_ref()
        .is_some_and(|properties| properties.keys().any(|key| key.as_str() == "txn-id"))
}

pub(super) async fn refuse_transaction_flow<W: AsyncWrite + Unpin>(
    channel: u16,
    flow: &Flow,
    session: &mut SessionState,
    writer: &mut FrameWriter<W>,
) -> Result<bool, EngineError> {
    if !flow_uses_transactions(flow)
        || flow
            .handle
            .is_some_and(|handle| session.closing_handles.contains(&handle))
    {
        return Ok(false);
    }
    if let Some(handle) = flow
        .handle
        .filter(|handle| session.links.contains_key(handle))
    {
        // Unsupported transactional acquisition is a link error, not a credit update.
        detach_link_error(
            channel,
            handle,
            session,
            writer,
            "amqp:not-implemented",
            TRANSACTIONS_NOT_IMPLEMENTED,
        )
        .await?;
    } else {
        refuse_session_state(
            channel,
            "amqp:not-implemented",
            TRANSACTIONS_NOT_IMPLEMENTED,
            session,
            writer,
        )
        .await?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests;
