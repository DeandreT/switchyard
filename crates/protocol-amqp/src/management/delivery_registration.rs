use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, PoisonError},
};

use super::{ConnectionManagement, DeliveryKey, EntityBinding, ManagedDelivery};

#[derive(Debug)]
struct RegistrationIdentity;

#[derive(Debug)]
struct RegisteredDelivery {
    delivery: ManagedDelivery,
    identity: Arc<RegistrationIdentity>,
}

#[derive(Debug, Default)]
pub(super) struct DeliveryRegistry {
    entries: Mutex<HashMap<DeliveryKey, RegisteredDelivery>>,
}

impl DeliveryRegistry {
    pub(super) fn register(&self, key: DeliveryKey, delivery: ManagedDelivery) {
        let entry = RegisteredDelivery {
            delivery,
            identity: Arc::new(RegistrationIdentity),
        };
        if let Ok(mut entries) = self.entries.lock() {
            entries.insert(key, entry);
        }
    }

    pub(super) fn register_owned(
        &self,
        management: Arc<ConnectionManagement>,
        key: DeliveryKey,
        delivery: ManagedDelivery,
    ) -> Result<DeliveryRegistration, DeliveryRegistrationError> {
        let identity = Arc::new(RegistrationIdentity);
        let entry = RegisteredDelivery {
            delivery,
            identity: Arc::clone(&identity),
        };
        // Prepare the guard and its key before publishing the association.
        let registration = DeliveryRegistration {
            management,
            key: key.clone(),
            identity,
        };
        {
            let mut entries = self.entries.lock().map_err(|_| DeliveryRegistrationError)?;
            entries.insert(key, entry);
        }
        Ok(registration)
    }

    pub(super) fn get(&self, key: &DeliveryKey) -> Option<ManagedDelivery> {
        self.entries
            .lock()
            .ok()?
            .get(key)
            .map(|entry| entry.delivery.clone())
    }

    pub(super) fn unregister(&self, key: &DeliveryKey, binding: &EntityBinding) {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        if entries
            .get(key)
            .is_some_and(|entry| &entry.delivery.binding == binding)
        {
            entries.remove(key);
        }
    }

    fn unregister_owned(&self, key: &DeliveryKey, identity: &Arc<RegistrationIdentity>) {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        if entries
            .get(key)
            .is_some_and(|entry| Arc::ptr_eq(&entry.identity, identity))
        {
            entries.remove(key);
        }
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    }
}

/// Cleanup ownership only; this guard conveys no broker settlement authority.
pub(crate) struct DeliveryRegistration {
    management: Arc<ConnectionManagement>,
    key: DeliveryKey,
    identity: Arc<RegistrationIdentity>,
}

impl Drop for DeliveryRegistration {
    fn drop(&mut self) {
        self.management
            .deliveries
            .unregister_owned(&self.key, &self.identity);
    }
}

impl fmt::Debug for DeliveryRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryRegistration")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("the delivery association is unavailable")]
pub(crate) struct DeliveryRegistrationError;

#[cfg(test)]
mod tests;
