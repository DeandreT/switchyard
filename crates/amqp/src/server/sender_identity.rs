use std::fmt;

use super::{NativeConnectionIdentity, incoming_ledger::LinkIdentity};

/// An opaque observer of one actor-accepted native sending link.
///
/// Cloning this observer does not keep its link or connection active. Activity
/// is neither authorization nor a reservation for a later operation. This is
/// link-origin evidence, not original-delivery or transaction authority. Exact
/// sender provenance remains comparable after retirement.
///
/// ```compile_fail
/// let _identity = amqp::NativeSenderIdentity::new();
/// ```
///
/// ```compile_fail
/// let _identity = amqp::NativeSenderIdentity::default();
/// ```
#[derive(Clone)]
pub struct NativeSenderIdentity {
    owner: LinkIdentity,
}

impl NativeSenderIdentity {
    pub(super) fn for_accepted_sender(owner: &LinkIdentity) -> Self {
        Self {
            owner: owner.clone(),
        }
    }

    pub fn same_sender(&self, other: &Self) -> bool {
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

    pub(super) fn matches_active_origin(&self, owner: &LinkIdentity) -> bool {
        self.is_active() && self.owner.same_link(owner)
    }
}

impl fmt::Debug for NativeSenderIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeSenderIdentity")
            .field("active", &self.is_active())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::connection_identity::ConnectionActorExit;
    use tokio::sync::watch;

    #[test]
    fn missing_connection_provenance_fails_closed() {
        let owner = LinkIdentity::new();
        let identity = NativeSenderIdentity::for_accepted_sender(&owner);
        let clone = identity.clone();
        assert!(identity.connection_identity().is_none());
        assert!(!identity.is_active());
        assert!(!identity.matches_active_origin(&owner));
        assert!(identity.same_sender(&clone));
        owner.retire();
        assert!(identity.same_sender(&clone));
        assert!(!clone.is_active());
    }

    #[test]
    fn cloned_observers_are_inert_and_match_only_the_exact_live_link() {
        let connection = NativeConnectionIdentity::new();
        let owner = LinkIdentity::for_connection(&connection);
        let other = LinkIdentity::for_connection(&connection);
        let identity = NativeSenderIdentity::for_accepted_sender(&owner);
        let clone = identity.clone();
        let replacement = NativeSenderIdentity::for_accepted_sender(&other);
        assert!(identity.same_sender(&clone));
        assert!(!identity.same_sender(&replacement));
        assert!(identity.matches_active_origin(&owner));
        assert!(!identity.matches_active_origin(&other));
        assert!(identity.is_active());
        assert!(
            identity
                .connection_identity()
                .expect("bound proof")
                .same_connection(&connection)
        );
        drop(identity);
        drop(clone.clone());
        assert!(!owner.is_retired());
        assert!(connection.is_active());
        assert!(clone.is_active());
        owner.retire();
        assert!(!clone.is_active());
        assert!(!clone.matches_active_origin(&owner));
        assert!(replacement.is_active());
        assert!(!clone.same_sender(&replacement));
    }

    #[test]
    fn connection_retirement_invalidates_observers_without_changing_origin() {
        let connection = NativeConnectionIdentity::new();
        let owner = LinkIdentity::for_connection(&connection);
        let identity = NativeSenderIdentity::for_accepted_sender(&owner);
        let clone = identity.clone();
        let (terminated, observed) = watch::channel(false);
        let exit = ConnectionActorExit::new(connection.clone(), terminated);
        assert!(identity.is_active());
        drop(exit);
        assert!(*observed.borrow());
        assert!(!connection.is_active());
        assert!(!owner.is_retired());
        assert!(!identity.is_active());
        assert!(!identity.matches_active_origin(&owner));
        assert!(identity.same_sender(&clone));
        assert!(
            clone
                .connection_identity()
                .expect("origin remains present")
                .same_connection(&connection)
        );
    }

    #[test]
    fn debug_exposes_only_activity() {
        let connection = NativeConnectionIdentity::new();
        let owner = LinkIdentity::for_connection(&connection);
        let identity = NativeSenderIdentity::for_accepted_sender(&owner);
        assert_eq!(
            format!("{identity:?}"),
            "NativeSenderIdentity { active: true, .. }"
        );
        owner.retire();
        assert_eq!(
            format!("{identity:?}"),
            "NativeSenderIdentity { active: false, .. }"
        );
    }
}
