use std::collections::{HashMap, HashSet};

use crate::Role;

use super::incoming_ledger::LinkIdentity;

pub(super) const MAX_RETIRED_DELIVERIES_PER_DIRECTION: usize = 4_096;

#[derive(Debug, Default)]
pub(super) struct ErrorDeliveryHistory {
    // Peer Disposition roles: Sender addresses our incoming IDs, Receiver our outgoing IDs.
    incoming: HashMap<u32, LinkIdentity>,
    outgoing: HashMap<u32, LinkIdentity>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(super) enum ErrorDeliveryHistoryError {
    #[error("error-owned delivery history limit of {maximum} reached on this session direction")]
    LimitReached { maximum: usize },
}

impl ErrorDeliveryHistory {
    fn records(&self, role: &Role) -> &HashMap<u32, LinkIdentity> {
        match role {
            Role::Sender => &self.incoming,
            Role::Receiver => &self.outgoing,
        }
    }

    pub(super) fn check_record(
        &self,
        role: &Role,
        ids: &HashSet<u32>,
    ) -> Result<(), ErrorDeliveryHistoryError> {
        let records = self.records(role);
        let additional = ids.iter().filter(|id| !records.contains_key(id)).count();
        if records
            .len()
            .checked_add(additional)
            .is_none_or(|count| count > MAX_RETIRED_DELIVERIES_PER_DIRECTION)
        {
            return Err(ErrorDeliveryHistoryError::LimitReached {
                maximum: MAX_RETIRED_DELIVERIES_PER_DIRECTION,
            });
        }
        Ok(())
    }

    pub(super) fn record(
        &mut self,
        role: &Role,
        owner: &LinkIdentity,
        ids: &HashSet<u32>,
    ) -> Result<(), ErrorDeliveryHistoryError> {
        self.check_record(role, ids)?;
        let records = match role {
            Role::Sender => &mut self.incoming,
            Role::Receiver => &mut self.outgoing,
        };
        for &id in ids {
            records.entry(id).or_insert_with(|| owner.clone());
        }
        Ok(())
    }

    pub(super) fn contains_range(&self, role: &Role, first: u32, last: Option<u32>) -> bool {
        let width = last.unwrap_or(first).wrapping_sub(first);
        self.records(role)
            .keys()
            .any(|id| id.wrapping_sub(first) <= width)
    }

    #[cfg(test)]
    pub(super) fn owner(&self, role: &Role, id: u32) -> Option<&LinkIdentity> {
        self.records(role).get(&id)
    }

    pub(super) fn outgoing_contains(&self, id: u32) -> bool {
        self.outgoing.contains_key(&id)
    }

    pub(super) fn outgoing_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.outgoing.keys().copied()
    }

    pub(super) fn reassign_incoming(&mut self, id: u32) -> bool {
        self.incoming.remove(&id).is_some()
    }

    pub(super) fn clear(&mut self) {
        self.incoming.clear();
        self.outgoing.clear();
    }
}
