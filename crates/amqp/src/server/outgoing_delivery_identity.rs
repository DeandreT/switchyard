use std::{fmt, sync::Arc};

use super::{NativeConnectionIdentity, NativeSenderIdentity, incoming_ledger::LinkIdentity};

/// An opaque observer of one actor-minted original outgoing delivery generation.
///
/// Cloning this metadata does not retain message content or keep transport
/// active. Exact provenance survives numeric alias release and retirement.
/// This may describe a transport-terminal delivery; it is neither a live
/// unsettled-delivery promise nor settlement or transaction authority.
///
/// ```compile_fail
/// let _identity = amqp::NativeOutgoingDeliveryIdentity::new();
/// ```
///
/// ```compile_fail
/// let _identity = amqp::NativeOutgoingDeliveryIdentity::default();
/// ```
#[derive(Clone)]
pub struct NativeOutgoingDeliveryIdentity(Arc<OutgoingDeliveryGeneration>);

struct OutgoingDeliveryGeneration {
    owner: LinkIdentity,
    id: u32,
}

impl NativeOutgoingDeliveryIdentity {
    pub(super) fn for_delivery(owner: &LinkIdentity, id: u32) -> Self {
        Self(Arc::new(OutgoingDeliveryGeneration {
            owner: owner.clone(),
            id,
        }))
    }

    pub fn same_delivery(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Tests active exact sending-link origin, not current alias availability.
    pub fn belongs_to_sender(&self, sender: &NativeSenderIdentity) -> bool {
        sender.matches_active_origin(&self.0.owner)
    }

    pub fn connection_identity(&self) -> Option<&NativeConnectionIdentity> {
        self.0.owner.connection_identity()
    }

    pub(super) fn id(&self) -> u32 {
        self.0.id
    }

    pub(super) fn owner(&self) -> &LinkIdentity {
        &self.0.owner
    }
}

impl fmt::Debug for NativeOutgoingDeliveryIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeOutgoingDeliveryIdentity")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::{connection_identity::ConnectionActorExit, outgoing_identity::AckIdentity};
    use tokio::sync::watch;

    #[test]
    fn equal_owner_and_numeric_id_still_mint_distinct_original_generations() {
        let owner = LinkIdentity::new();
        let original = NativeOutgoingDeliveryIdentity::for_delivery(&owner, 7);
        let clone = original.clone();
        let replacement = NativeOutgoingDeliveryIdentity::for_delivery(&owner, 7);
        assert!(original.same_delivery(&clone));
        assert!(!original.same_delivery(&replacement));
        assert_eq!(original.id(), replacement.id());
        assert!(original.owner().same_link(replacement.owner()));
        owner.retire();
        assert!(original.same_delivery(&clone));
        assert!(!original.same_delivery(&replacement));
    }

    #[test]
    fn active_origin_is_exact_and_missing_connection_provenance_fails_closed() {
        let unbound = LinkIdentity::new();
        let original = NativeOutgoingDeliveryIdentity::for_delivery(&unbound, 0);
        let sender = NativeSenderIdentity::for_accepted_sender(&unbound);
        assert!(original.connection_identity().is_none());
        assert!(!original.belongs_to_sender(&sender));

        let connection = NativeConnectionIdentity::new();
        let owner = LinkIdentity::for_connection(&connection);
        let other = LinkIdentity::for_connection(&connection);
        let original = NativeOutgoingDeliveryIdentity::for_delivery(&owner, 0);
        let clone = original.clone();
        let sender = NativeSenderIdentity::for_accepted_sender(&owner);
        let foreign = NativeSenderIdentity::for_accepted_sender(&other);
        assert!(original.belongs_to_sender(&sender));
        assert!(!original.belongs_to_sender(&foreign));
        assert!(
            original
                .connection_identity()
                .expect("original connection")
                .same_connection(&connection)
        );
        owner.retire();
        assert!(!original.belongs_to_sender(&sender));
        assert!(original.same_delivery(&clone));
    }

    #[test]
    fn observer_drop_is_inert_and_connection_retirement_preserves_original_identity() {
        let connection = NativeConnectionIdentity::new();
        let owner = LinkIdentity::for_connection(&connection);
        let original = NativeOutgoingDeliveryIdentity::for_delivery(&owner, u32::MAX);
        let clone = original.clone();
        let sender = NativeSenderIdentity::for_accepted_sender(&owner);
        drop(original.clone());
        assert!(!owner.is_retired());
        assert!(connection.is_active());
        assert!(original.belongs_to_sender(&sender));
        let (terminated, observed) = watch::channel(false);
        drop(ConnectionActorExit::new(connection.clone(), terminated));
        assert!(*observed.borrow());
        assert!(!original.belongs_to_sender(&sender));
        assert!(original.same_delivery(&clone));
    }

    #[test]
    fn acknowledgement_retains_the_same_original_metadata_after_observer_drop() {
        let owner = LinkIdentity::new();
        let original = NativeOutgoingDeliveryIdentity::for_delivery(&owner, 42);
        let weak = Arc::downgrade(&original.0);
        let acknowledgement = AckIdentity::for_delivery(&original, b"private-tag");
        assert!(acknowledgement.delivery_identity().same_delivery(&original));
        assert_eq!(acknowledgement.id(), 42);
        drop(original);
        assert!(weak.upgrade().is_some());
        acknowledgement.mark_settled();
        assert!(weak.upgrade().is_some());
        drop(acknowledgement);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn debug_contains_no_aliases_or_origin_details() {
        let owner = LinkIdentity::new();
        let original = NativeOutgoingDeliveryIdentity::for_delivery(&owner, 1_234_567);
        assert_eq!(
            format!("{original:?}"),
            "NativeOutgoingDeliveryIdentity { .. }"
        );
        owner.retire();
        assert_eq!(
            format!("{original:?}"),
            "NativeOutgoingDeliveryIdentity { .. }"
        );
    }
}
