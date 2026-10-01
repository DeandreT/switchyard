use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use super::incoming_ledger::LinkIdentity;

#[derive(Clone, Debug)]
pub(super) struct AckIdentity(Arc<AckGeneration>);

#[derive(Debug)]
struct AckGeneration {
    owner: LinkIdentity,
    id: u32,
    tag: Vec<u8>,
    settled: AtomicBool,
}

impl AckIdentity {
    pub(super) fn new(owner: &LinkIdentity, id: u32, tag: &[u8]) -> Self {
        Self(Arc::new(AckGeneration {
            owner: owner.clone(),
            id,
            tag: tag.to_vec(),
            settled: AtomicBool::new(false),
        }))
    }

    pub(super) fn id(&self) -> u32 {
        self.0.id
    }

    pub(super) fn tag(&self) -> &[u8] {
        &self.0.tag
    }

    pub(super) fn belongs_to(&self, owner: &LinkIdentity) -> bool {
        self.0.owner.same_link(owner)
    }

    pub(super) fn same_ack(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(super) fn is_settled(&self) -> bool {
        self.0.settled.load(Ordering::Acquire)
    }

    // The actor commits an exact owned token after a flushed Sender ACK or
    // after observing the peer receiver settle that pending delivery.
    pub(super) fn mark_settled(&self) {
        self.0.settled.store(true, Ordering::Release);
    }

    pub(super) fn validate_owner(&self, owner: &LinkIdentity) -> Result<(), AckIdentityError> {
        if !self.belongs_to(owner) {
            return Err(AckIdentityError::WrongOwner);
        }
        if owner.is_retired() {
            return Err(AckIdentityError::RetiredOwner);
        }
        Ok(())
    }
}

impl PartialEq for AckIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.same_ack(other)
    }
}

impl Eq for AckIdentity {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AckIdentityError {
    WrongOwner,
    RetiredOwner,
}

impl fmt::Display for AckIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::WrongOwner => "outgoing acknowledgement belongs to a different link generation",
            Self::RetiredOwner => "outgoing acknowledgement link generation is retired",
        })
    }
}

impl std::error::Error for AckIdentityError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloned_identity_is_exact_but_fresh_token_with_same_owner_and_id_is_not() {
        let owner = LinkIdentity::new();
        let mut tag = vec![1, 2];
        let identity = AckIdentity::new(&owner, 17, &tag);
        let clone = identity.clone();
        tag.fill(9);
        let replacement = AckIdentity::new(&owner, 17, &tag);
        assert_eq!(identity.tag(), &[1, 2]);
        assert_eq!(clone.tag(), &[1, 2]);
        assert_eq!(replacement.tag(), &[9, 9]);
        assert!(identity.same_ack(&clone));
        assert_eq!(identity, clone);
        assert!(!identity.same_ack(&replacement));
        assert_ne!(identity, replacement);
        assert!(clone.belongs_to(&owner));
        assert!(replacement.belongs_to(&owner));
        assert_eq!(identity.id(), replacement.id());
    }

    #[test]
    fn numeric_id_reuse_does_not_reassign_old_owner_or_terminal_token() {
        let owner = LinkIdentity::new();
        let replacement_owner = LinkIdentity::new();
        let identity = AckIdentity::new(&owner, u32::MAX, &[]);
        let replacement = AckIdentity::new(&replacement_owner, u32::MAX, &[]);
        identity.mark_settled();
        assert_eq!(identity.id(), u32::MAX);
        assert_eq!(replacement.id(), u32::MAX);
        assert!(identity.is_settled());
        assert!(!replacement.is_settled());
        assert!(!identity.same_ack(&replacement));
        assert!(!identity.belongs_to(&replacement_owner));
        assert_eq!(
            identity.validate_owner(&replacement_owner),
            Err(AckIdentityError::WrongOwner)
        );
        assert_eq!(identity.validate_owner(&owner), Ok(()));
        assert_eq!(replacement.validate_owner(&replacement_owner), Ok(()));
    }

    #[test]
    fn postwrite_settlement_is_monotonic_and_shared_by_owned_clones() {
        let owner = LinkIdentity::new();
        let identity = AckIdentity::new(&owner, 0, &[]);
        let clone = identity.clone();
        assert!(!identity.is_settled());
        assert!(!clone.is_settled());
        clone.mark_settled();
        assert!(identity.is_settled());
        assert!(clone.is_settled());
        identity.mark_settled();
        assert!(clone.is_settled());
        assert_eq!(identity.id(), 0);
        assert_eq!(clone.validate_owner(&owner), Ok(()));
    }

    #[test]
    fn wrong_owner_validation_does_not_change_either_pending_identity() {
        let owner = LinkIdentity::new();
        let other = LinkIdentity::new();
        let identity = AckIdentity::new(&owner, 8, &[]);
        let other_identity = AckIdentity::new(&other, 8, &[]);
        for _ in 0..2 {
            assert_eq!(
                identity.validate_owner(&other),
                Err(AckIdentityError::WrongOwner)
            );
            assert_eq!(identity.validate_owner(&owner), Ok(()));
            assert!(!identity.is_settled());
            assert!(!other_identity.is_settled());
        }
    }

    #[test]
    fn retired_owner_is_rejected_before_pending_or_terminal_noop() {
        let owner = LinkIdentity::new();
        let clone = owner.clone();
        let pending = AckIdentity::new(&owner, 5, &[]);
        let terminal = AckIdentity::new(&owner, 6, &[]);
        terminal.mark_settled();
        owner.retire();
        clone.retire();
        assert_eq!(
            pending.validate_owner(&clone),
            Err(AckIdentityError::RetiredOwner)
        );
        assert_eq!(
            terminal.validate_owner(&clone),
            Err(AckIdentityError::RetiredOwner)
        );
        assert!(!pending.is_settled());
        assert!(terminal.is_settled());
        assert_eq!(pending.id(), 5);
        assert_eq!(terminal.id(), 6);
    }

    #[test]
    fn terminal_old_token_cannot_settle_a_new_token_reusing_the_same_numeric_id() {
        let owner = LinkIdentity::new();
        let old = AckIdentity::new(&owner, 77, &[]);
        old.mark_settled();
        let replacement = AckIdentity::new(&owner, 77, &[]);
        assert_eq!(old.validate_owner(&owner), Ok(()));
        assert!(old.is_settled());
        assert!(!old.same_ack(&replacement));
        assert!(!replacement.is_settled());
        old.mark_settled();
        assert!(!replacement.is_settled());

        let stranger = LinkIdentity::new();
        stranger.retire();
        assert_eq!(
            old.validate_owner(&stranger),
            Err(AckIdentityError::WrongOwner)
        );
        assert_eq!(replacement.validate_owner(&owner), Ok(()));
        assert!(!replacement.is_settled());
    }
}
