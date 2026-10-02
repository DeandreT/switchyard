use super::*;

/// An opaque observer of one actor-accepted transactional receiving link.
///
/// Cloning this observer does not keep its link or connection active. Activity
/// is neither authorization nor a reservation for a later operation. Exact
/// receiver provenance remains comparable after retirement.
///
/// ```compile_fail
/// let _identity = amqp::NativeReceiverIdentity::new();
/// ```
///
/// ```compile_fail
/// let _identity = amqp::NativeReceiverIdentity::default();
/// ```
#[derive(Clone)]
pub struct NativeReceiverIdentity {
    owner: LinkIdentity,
}

impl NativeReceiverIdentity {
    pub(super) fn for_accepted_receiver(owner: &LinkIdentity) -> Self {
        Self {
            owner: owner.clone(),
        }
    }

    pub fn same_receiver(&self, other: &Self) -> bool {
        self.owner.same_link(&other.owner)
    }

    pub fn is_active(&self) -> bool {
        !self.owner.is_retired()
            && self
                .connection_identity()
                .is_some_and(NativeConnectionIdentity::is_active)
    }

    pub fn connection_identity(&self) -> Option<&NativeConnectionIdentity> {
        self.owner.connection_identity()
    }

    pub(super) fn owns_delivery(&self, delivery: &DeliveryIdentity) -> bool {
        self.is_active() && delivery.belongs_to(&self.owner)
    }
}

impl fmt::Debug for NativeReceiverIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeReceiverIdentity")
            .field("active", &self.is_active())
            .finish_non_exhaustive()
    }
}
