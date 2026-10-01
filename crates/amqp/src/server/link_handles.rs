use std::{collections::HashMap, sync::Arc};

use crate::Role;

use super::{SessionState, incoming_ledger::LinkIdentity, link_slot_count};

#[derive(Debug)]
pub(super) struct HandleAlias {
    pub identity: LinkIdentity,
    pub name: Arc<str>,
    pub role: Role,
    pub peer_handle: Option<u32>,
    pub own_attach_sent: bool,
    pub error_detached: bool,
}

pub(super) fn current_alias(handle: u32, session: &SessionState) -> Option<&HandleAlias> {
    session
        .handle_aliases
        .get(&handle)
        .filter(|alias| alias_is_current(handle, alias, session))
}

pub(super) fn connection_link_name_in_use(
    sessions: &HashMap<u16, SessionState>,
    name: &str,
    local_role: &Role,
) -> bool {
    sessions.values().any(|session| {
        !session.ending
            && !session.identity.is_retired()
            && session.handle_aliases.keys().any(|&handle| {
                current_alias(handle, session).is_some_and(|alias| {
                    !alias.error_detached
                        && (!alias.identity.is_retired()
                            || session.closing_handles.contains(&handle))
                        && alias.name.as_ref() == name
                        && &alias.role == local_role
                })
            })
    })
}

fn alias_is_current(handle: u32, alias: &HandleAlias, session: &SessionState) -> bool {
    if let Some(link) = session.links.get(&handle)
        && !alias.identity.same_link(link.identity())
    {
        return false;
    }
    if let Some(approval) = session
        .pending_attaches
        .get(&handle)
        .and_then(|pending| pending.approval.as_ref())
        && !alias.identity.same_link(approval.link_identity())
    {
        return false;
    }
    session.links.contains_key(&handle)
        || session.pending_attaches.contains_key(&handle)
        || session.closing_handles.contains(&handle)
}

pub(super) fn local_handle_for_peer(peer_handle: u32, session: &SessionState) -> Option<u32> {
    session.handle_aliases.iter().find_map(|(&handle, alias)| {
        (alias.peer_handle == Some(peer_handle) && alias_is_current(handle, alias, session))
            .then_some(handle)
    })
}

pub(super) fn is_error_detached(session: &SessionState, handle: u32) -> bool {
    session.closing_handles.contains(&handle)
        && session
            .handle_aliases
            .get(&handle)
            .is_some_and(|alias| alias.error_detached && alias_is_current(handle, alias, session))
}

pub(super) fn mark_error_detached(
    session: &mut SessionState,
    handle: u32,
    owner: &LinkIdentity,
) -> bool {
    if !session.closing_handles.contains(&handle)
        || !session.handle_aliases.get(&handle).is_some_and(|alias| {
            alias.identity.same_link(owner) && alias_is_current(handle, alias, session)
        })
    {
        return false;
    }
    let Some(alias) = session.handle_aliases.get_mut(&handle) else {
        return false;
    };
    alias.error_detached = true;
    true
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
