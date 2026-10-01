use std::collections::HashMap;

use super::SessionState;

pub(super) fn local_channel_for_peer(
    peer_channel: u16,
    sessions: &HashMap<u16, SessionState>,
) -> Option<u16> {
    sessions.iter().find_map(|(channel, session)| {
        (session.peer_channel == Some(peer_channel)).then_some(*channel)
    })
}

pub(super) fn vacant_channel(
    start: u16,
    maximum: u16,
    sessions: &HashMap<u16, SessionState>,
) -> Option<u16> {
    let count = u32::from(maximum) + 1;
    // One more candidate than occupied slots must expose a vacant channel.
    let probes = count.min(sessions.len().saturating_add(1) as u32);
    (0..probes)
        .map(|offset| ((u32::from(start) + offset) % count) as u16)
        .find(|channel| !sessions.contains_key(channel))
}

pub(super) fn preferred_vacant_channel(
    preferred: u16,
    maximum: u16,
    sessions: &HashMap<u16, SessionState>,
) -> Option<u16> {
    if preferred <= maximum && !sessions.contains_key(&preferred) {
        Some(preferred)
    } else {
        vacant_channel(0, maximum, sessions)
    }
}
