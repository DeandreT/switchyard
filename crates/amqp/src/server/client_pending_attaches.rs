use std::collections::HashMap;

use super::{LinkState, PendingAttach, Role};

#[derive(Default)]
pub(super) struct PendingAttaches {
    sending: HashMap<String, PendingAttach>,
    receiving: HashMap<String, PendingAttach>,
}

impl PendingAttaches {
    pub(super) fn get(&self, name: &str, local_role: &Role) -> Option<&PendingAttach> {
        self.map(local_role).get(name)
    }

    pub(super) fn contains_key(&self, name: &str, local_role: &Role) -> bool {
        self.map(local_role).contains_key(name)
    }

    pub(super) fn insert(&mut self, name: String, pending: PendingAttach) -> Option<PendingAttach> {
        let local_role = match &pending.link {
            LinkState::Sending(_) => Role::Sender,
            LinkState::Receiving(_) => Role::Receiver,
        };
        self.map_mut(&local_role).insert(name, pending)
    }

    pub(super) fn remove(&mut self, name: &str, local_role: &Role) -> Option<PendingAttach> {
        self.map_mut(local_role).remove(name)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&str, Role, &PendingAttach)> {
        self.sending
            .iter()
            .map(|(name, pending)| (name.as_str(), Role::Sender, pending))
            .chain(
                self.receiving
                    .iter()
                    .map(|(name, pending)| (name.as_str(), Role::Receiver, pending)),
            )
    }

    pub(super) fn drain(&mut self) -> impl Iterator<Item = (String, PendingAttach)> {
        self.sending.drain().chain(self.receiving.drain())
    }

    fn map(&self, local_role: &Role) -> &HashMap<String, PendingAttach> {
        match local_role {
            Role::Sender => &self.sending,
            Role::Receiver => &self.receiving,
        }
    }

    fn map_mut(&mut self, local_role: &Role) -> &mut HashMap<String, PendingAttach> {
        match local_role {
            Role::Sender => &mut self.sending,
            Role::Receiver => &mut self.receiving,
        }
    }
}
