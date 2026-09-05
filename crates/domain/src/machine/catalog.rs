//! Entity catalog reads used by data operations and the timer worker.

use storage::StateStore;

use crate::{BrokerError, EntityPath, NamespaceName, QueueConfig, keys};

use super::StateMachine;

impl<S: StateStore> StateMachine<S> {
    pub fn queue_config(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> Result<Option<QueueConfig>, BrokerError> {
        self.read(&keys::queue_config(namespace, entity))
    }

    /// Every queue in the store, in key order, across every namespace. The timer
    /// worker walks this to learn what there is to sweep.
    pub fn queues(&self, limit: usize) -> Result<Vec<(NamespaceName, EntityPath)>, BrokerError> {
        self.queues_after(None, limit)
    }

    /// Queues strictly after `after`, in key order across every namespace.
    ///
    /// The timer keeps the last queue from a full page and resumes here on its
    /// next tick. The cursor is an entity identity rather than a storage key so
    /// it remains meaningful if that queue is deleted between pages.
    pub fn queues_after(
        &self,
        after: Option<&(NamespaceName, EntityPath)>,
        limit: usize,
    ) -> Result<Vec<(NamespaceName, EntityPath)>, BrokerError> {
        let prefix = keys::queue_config_prefix();
        let after_key = after.map(|(namespace, entity)| keys::queue_config(namespace, entity));
        // `scan_from` is inclusive. Read one extra entry when resuming so the
        // cursor itself can be discarded without shrinking the requested page.
        let scan_limit = limit.saturating_add(usize::from(after_key.is_some()));
        self.store()
            .scan_from(&prefix, after_key.as_deref().unwrap_or(&prefix), scan_limit)?
            .into_iter()
            .filter(|(key, _)| {
                after_key
                    .as_ref()
                    .is_none_or(|after_key| key.as_slice() > after_key.as_slice())
            })
            .take(limit)
            .map(|(key, _)| {
                let (namespace, entity) =
                    keys::entity_scope_parts(&key).ok_or(BrokerError::MalformedIndexKey)?;
                Ok((
                    NamespaceName::new(namespace)?,
                    EntityPath::from_internal(entity)?,
                ))
            })
            .collect()
    }
}
