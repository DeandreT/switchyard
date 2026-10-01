use super::{SessionState, incoming_ledger::LinkIdentity, link_slot_count};

#[derive(Debug)]
pub(super) struct HandleAlias {
    pub identity: LinkIdentity,
    pub peer_handle: Option<u32>,
    pub own_attach_sent: bool,
}

pub(super) fn local_handle_for_peer(peer_handle: u32, session: &SessionState) -> Option<u32> {
    session.handle_aliases.iter().find_map(|(handle, alias)| {
        if alias.peer_handle != Some(peer_handle) {
            return None;
        }
        if let Some(link) = session.links.get(handle)
            && !alias.identity.same_link(link.identity())
        {
            return None;
        }
        if let Some(approval) = session
            .pending_attaches
            .get(handle)
            .and_then(|pending| pending.approval.as_ref())
            && !alias.identity.same_link(approval.link_identity())
        {
            return None;
        }
        (session.links.contains_key(handle)
            || session.pending_attaches.contains_key(handle)
            || session.closing_handles.contains(handle))
        .then_some(*handle)
    })
}

fn occupied(handle: u32, session: &SessionState) -> bool {
    session.handle_aliases.contains_key(&handle)
        || session.links.contains_key(&handle)
        || session.pending_attaches.contains_key(&handle)
        || session.closing_handles.contains(&handle)
}

pub(super) fn vacant_handle(start: u32, maximum: u32, session: &SessionState) -> Option<u32> {
    let count = u64::from(maximum) + 1;
    // There can be no more occupied candidates than lifecycle aliases.
    let probes = count.min(link_slot_count(session).saturating_add(1) as u64);
    (0..probes)
        .map(|offset| ((u64::from(start) + offset) % count) as u32)
        .find(|handle| !occupied(*handle, session))
}

pub(super) fn preferred_vacant_handle(
    preferred: u32,
    maximum: u32,
    session: &SessionState,
) -> Option<u32> {
    if preferred <= maximum && !occupied(preferred, session) {
        Some(preferred)
    } else {
        vacant_handle(0, maximum, session)
    }
}
