use std::{collections::HashMap, sync::Arc};

use crate::Role;

use super::incoming_ledger::LinkIdentity;

pub(super) const MAX_ERROR_LINK_NAMES: usize = 256;
pub(super) const MAX_ERROR_LINK_NAME_BYTES: usize = 1024 * 1024;
pub(super) const MAX_ERROR_PEER_HANDLES: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(super) enum ErrorLinkHistoryError {
    #[error("error-owned link name limit of {maximum} reached on this connection")]
    NameCount { maximum: usize },
    #[error("error-owned link name byte limit of {maximum} reached on this connection")]
    NameBytes { maximum: usize },
    #[error("error-owned peer handle limit of {maximum} reached on this session")]
    PeerHandles { maximum: usize },
}

#[derive(Debug, Default)]
pub(super) struct ErrorLinkNames {
    // Canonical local roles, independent of peer Attach and Disposition polarity.
    sending: HashMap<Arc<str>, LinkIdentity>,
    receiving: HashMap<Arc<str>, LinkIdentity>,
    name_bytes: usize,
}

impl ErrorLinkNames {
    fn records(&self, local_role: &Role) -> &HashMap<Arc<str>, LinkIdentity> {
        match local_role {
            Role::Sender => &self.sending,
            Role::Receiver => &self.receiving,
        }
    }

    pub(super) fn contains(&self, name: &str, local_role: &Role) -> bool {
        self.records(local_role).contains_key(name)
    }

    pub(super) fn check_record(
        &self,
        name: &str,
        local_role: &Role,
    ) -> Result<(), ErrorLinkHistoryError> {
        if self.contains(name, local_role) {
            return Ok(());
        }
        if self.sending.len() + self.receiving.len() >= MAX_ERROR_LINK_NAMES {
            return Err(ErrorLinkHistoryError::NameCount {
                maximum: MAX_ERROR_LINK_NAMES,
            });
        }
        if self
            .name_bytes
            .checked_add(name.len())
            .is_none_or(|bytes| bytes > MAX_ERROR_LINK_NAME_BYTES)
        {
            return Err(ErrorLinkHistoryError::NameBytes {
                maximum: MAX_ERROR_LINK_NAME_BYTES,
            });
        }
        Ok(())
    }

    pub(super) fn record(
        &mut self,
        name: Arc<str>,
        local_role: &Role,
        owner: &LinkIdentity,
    ) -> Result<(), ErrorLinkHistoryError> {
        self.check_record(&name, local_role)?;
        if self.contains(&name, local_role) {
            return Ok(());
        }
        self.name_bytes += name.len();
        let records = match local_role {
            Role::Sender => &mut self.sending,
            Role::Receiver => &mut self.receiving,
        };
        records.insert(name, owner.clone());
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn owner(&self, name: &str, local_role: &Role) -> Option<&LinkIdentity> {
        self.records(local_role).get(name)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.sending.len() + self.receiving.len()
    }

    #[cfg(test)]
    pub(super) fn name_bytes(&self) -> usize {
        self.name_bytes
    }
}

#[derive(Debug, Default)]
pub(super) struct ErrorPeerHandles {
    handles: HashMap<u32, LinkIdentity>,
}

impl ErrorPeerHandles {
    pub(super) fn contains(&self, peer_handle: u32) -> bool {
        self.handles.contains_key(&peer_handle)
    }

    pub(super) fn check_record(&self, peer_handle: u32) -> Result<(), ErrorLinkHistoryError> {
        if !self.contains(peer_handle) && self.handles.len() >= MAX_ERROR_PEER_HANDLES {
            return Err(ErrorLinkHistoryError::PeerHandles {
                maximum: MAX_ERROR_PEER_HANDLES,
            });
        }
        Ok(())
    }

    pub(super) fn record(
        &mut self,
        peer_handle: u32,
        owner: &LinkIdentity,
    ) -> Result<(), ErrorLinkHistoryError> {
        self.check_record(peer_handle)?;
        self.handles
            .entry(peer_handle)
            .or_insert_with(|| owner.clone());
        Ok(())
    }

    pub(super) fn reassign(&mut self, peer_handle: u32) -> bool {
        self.handles.remove(&peer_handle).is_some()
    }

    pub(super) fn clear(&mut self) {
        self.handles.clear();
    }

    #[cfg(test)]
    pub(super) fn owner(&self, peer_handle: u32) -> Option<&LinkIdentity> {
        self.handles.get(&peer_handle)
    }
}
