use std::{
    fmt,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use crate::{Attach, ReceiverSettleMode, Role, SenderSettleMode};

use super::incoming_ledger::LinkIdentity;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct SessionIdentity(LinkIdentity);

impl SessionIdentity {
    pub(super) fn new() -> Self {
        Self(LinkIdentity::new())
    }

    pub(super) fn same_session(&self, other: &Self) -> bool {
        self.0.same_link(&other.0)
    }

    pub(super) fn is_retired(&self) -> bool {
        self.0.is_retired()
    }

    pub(super) fn retire(&self) {
        self.0.retire();
    }
}

#[derive(Debug)]
pub(super) struct AttachApproval {
    session: SessionIdentity,
    link: LinkIdentity,
    handle: u32,
    local_handle: u32,
    name: Arc<str>,
    role: Role,
    sender_settle_mode: SenderSettleMode,
    receiver_settle_mode: ReceiverSettleMode,
}

impl AttachApproval {
    pub(super) fn link_identity(&self) -> &LinkIdentity {
        &self.link
    }

    pub(super) fn local_handle(&self) -> u32 {
        self.local_handle
    }

    pub(super) fn name(&self) -> &Arc<str> {
        &self.name
    }

    pub(super) fn local_role(&self) -> Role {
        self.role.opposite()
    }

    pub(super) fn refusal_attach(&self) -> Attach {
        Attach {
            name: self.name.to_string(),
            handle: self.local_handle,
            role: self.role.opposite(),
            snd_settle_mode: self.sender_settle_mode.clone(),
            rcv_settle_mode: self.receiver_settle_mode.clone(),
            source: None,
            target: None,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: (self.role == Role::Receiver).then_some(0),
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }

    pub(super) fn retire(&self) {
        self.link.retire();
    }
}

/// An incoming attach request tied to its original session and link generation.
#[derive(Clone, Debug)]
pub struct IncomingAttach {
    attach: Attach,
    approval: Arc<AttachApproval>,
}

impl IncomingAttach {
    pub(super) fn new(attach: Attach, session: SessionIdentity, local_handle: u32) -> Self {
        let approval = Arc::new(AttachApproval {
            session,
            link: LinkIdentity::new(),
            handle: attach.handle,
            local_handle,
            name: Arc::from(attach.name.as_str()),
            role: attach.role.clone(),
            sender_settle_mode: attach.snd_settle_mode.clone(),
            receiver_settle_mode: attach.rcv_settle_mode.clone(),
        });
        Self { attach, approval }
    }

    pub fn attach(&self) -> &Attach {
        &self.attach
    }

    pub fn attach_mut(&mut self) -> &mut Attach {
        &mut self.attach
    }

    /// Returns the request without its approval provenance.
    pub fn into_attach(self) -> Attach {
        self.attach
    }

    pub(super) fn approval(&self) -> &Arc<AttachApproval> {
        &self.approval
    }

    // Pending membership remains actor-owned; callers can validate the receipt
    // itself before allocating channels or enqueuing an acceptance command.
    pub(super) fn validate_request(
        &self,
        current_session: &SessionIdentity,
    ) -> Result<(), AttachApprovalError> {
        if current_session.is_retired() || self.approval.session.is_retired() {
            return Err(AttachApprovalError::RetiredSession);
        }
        if !self.approval.session.same_session(current_session) {
            return Err(AttachApprovalError::WrongSession);
        }
        if self.approval.link.is_retired() {
            return Err(AttachApprovalError::RetiredApproval);
        }
        if self.attach.handle != self.approval.handle {
            return Err(AttachApprovalError::ChangedHandle);
        }
        if self.attach.name != self.approval.name.as_ref() {
            return Err(AttachApprovalError::ChangedName);
        }
        if self.attach.role != self.approval.role {
            return Err(AttachApprovalError::ChangedRole);
        }
        Ok(())
    }

    // The actor separately checks that the session is not ending and that the
    // supplied pending approval is still installed before any IO or mutation.
    pub(super) fn validate(
        &self,
        current_session: &SessionIdentity,
        pending_approval: &Arc<AttachApproval>,
    ) -> Result<(), AttachApprovalError> {
        self.validate_request(current_session)?;
        if pending_approval.session.is_retired() {
            return Err(AttachApprovalError::RetiredSession);
        }
        if !pending_approval.session.same_session(current_session) {
            return Err(AttachApprovalError::WrongSession);
        }
        if pending_approval.link.is_retired() {
            return Err(AttachApprovalError::RetiredApproval);
        }
        if !Arc::ptr_eq(&self.approval, pending_approval) {
            return Err(AttachApprovalError::StaleApproval);
        }
        Ok(())
    }

    pub(super) fn into_parts(self) -> (Attach, Arc<AttachApproval>) {
        (self.attach, self.approval)
    }
}

impl Deref for IncomingAttach {
    type Target = Attach;

    fn deref(&self) -> &Self::Target {
        self.attach()
    }
}

impl DerefMut for IncomingAttach {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.attach_mut()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AttachApprovalError {
    RetiredSession,
    RetiredApproval,
    WrongSession,
    StaleApproval,
    ChangedHandle,
    ChangedName,
    ChangedRole,
}

impl fmt::Display for AttachApprovalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RetiredSession => "attach approval session generation is retired",
            Self::RetiredApproval => "attach approval link generation is retired",
            Self::WrongSession => "attach approval belongs to a different session generation",
            Self::StaleApproval => "attach approval is not the pending link generation",
            Self::ChangedHandle => "incoming attach handle cannot be changed",
            Self::ChangedName => "incoming attach name cannot be changed",
            Self::ChangedRole => "incoming attach role cannot be changed",
        })
    }
}

impl std::error::Error for AttachApprovalError {}

#[cfg(test)]
mod tests {
    use crate::{
        ReceiverSettleMode, SenderSettleMode, Source, Target,
        server::incoming_ledger::{IncomingLedger, IncomingLedgerError},
    };

    use super::*;

    fn attach() -> Attach {
        Attach {
            name: String::from("messages"),
            handle: 7,
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(Source::new("queue")),
            target: Some(Target::new("queue").into()),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }

    #[test]
    fn session_clones_share_generation_but_fresh_sessions_do_not() {
        let session = SessionIdentity::new();
        let clone = session.clone();
        let replacement = SessionIdentity::new();
        assert!(session.same_session(&clone));
        assert_eq!(session, clone);
        assert!(!session.same_session(&replacement));
        assert_ne!(session, replacement);
        assert!(!session.is_retired());
        assert!(!clone.is_retired());
        assert!(!replacement.is_retired());
        session.retire();
        clone.retire();
        assert!(session.is_retired());
        assert!(clone.is_retired());
        assert!(!replacement.is_retired());
        assert!(session.same_session(&clone));
        assert!(!session.same_session(&replacement));
    }

    #[test]
    fn cloned_receipt_preserves_exact_approval_and_endpoint_generation() {
        let session = SessionIdentity::new();
        let receipt = IncomingAttach::new(attach(), session.clone(), 7);
        let clone = receipt.clone();
        assert!(Arc::ptr_eq(receipt.approval(), clone.approval()));
        assert!(
            receipt
                .approval()
                .link_identity()
                .same_link(clone.approval().link_identity())
        );
        assert_eq!(clone.validate(&session, receipt.approval()), Ok(()));
    }

    #[test]
    fn identical_request_content_cannot_approve_replacement_link() {
        let session = SessionIdentity::new();
        let old = IncomingAttach::new(attach(), session.clone(), 7);
        let replacement = IncomingAttach::new(old.attach().clone(), session.clone(), 7);
        assert_eq!(old.attach(), replacement.attach());
        assert_eq!(
            old.validate(&session, replacement.approval()),
            Err(AttachApprovalError::StaleApproval)
        );
        assert_eq!(
            replacement.validate(&session, replacement.approval()),
            Ok(())
        );
        assert!(
            !old.approval()
                .link_identity()
                .same_link(replacement.approval().link_identity())
        );
    }

    #[test]
    fn identical_request_content_cannot_cross_session_reuse() {
        let old_session = SessionIdentity::new();
        let new_session = SessionIdentity::new();
        let old = IncomingAttach::new(attach(), old_session.clone(), 7);
        let replacement = IncomingAttach::new(old.attach().clone(), new_session.clone(), 7);
        assert_eq!(
            old.validate(&new_session, replacement.approval()),
            Err(AttachApprovalError::WrongSession)
        );
        assert_eq!(
            replacement.validate(&old_session, replacement.approval()),
            Err(AttachApprovalError::WrongSession)
        );
        assert_eq!(
            old.validate(&old_session, replacement.approval()),
            Err(AttachApprovalError::WrongSession)
        );
        assert_eq!(
            replacement.validate(&new_session, replacement.approval()),
            Ok(())
        );
    }

    #[test]
    fn handle_name_and_role_are_immutable_approval_fields() {
        let session = SessionIdentity::new();
        let receipt = IncomingAttach::new(attach(), session.clone(), 7);
        let mut changed = receipt.clone();
        changed.handle += 1;
        assert_eq!(
            changed.validate_request(&session),
            Err(AttachApprovalError::ChangedHandle)
        );
        assert_eq!(
            changed.validate(&session, receipt.approval()),
            Err(AttachApprovalError::ChangedHandle)
        );
        let mut changed = receipt.clone();
        changed.name.push_str("-replacement");
        assert_eq!(
            changed.validate_request(&session),
            Err(AttachApprovalError::ChangedName)
        );
        assert_eq!(
            changed.validate(&session, receipt.approval()),
            Err(AttachApprovalError::ChangedName)
        );
        let mut changed = receipt.clone();
        changed.role = Role::Receiver;
        assert_eq!(
            changed.validate_request(&session),
            Err(AttachApprovalError::ChangedRole)
        );
        assert_eq!(
            changed.validate(&session, receipt.approval()),
            Err(AttachApprovalError::ChangedRole)
        );
        assert_eq!(receipt.validate(&session, receipt.approval()), Ok(()));
        assert_eq!(receipt.validate_request(&session), Ok(()));
    }

    #[test]
    fn listener_filter_and_missing_sender_count_adjustments_remain_valid() {
        let session = SessionIdentity::new();
        let mut receipt = IncomingAttach::new(attach(), session.clone(), 7);
        let mut source = receipt.source.clone().expect("fixture source");
        source.filter = Some(Default::default());
        source.address = Some(String::from("granted-session"));
        receipt.source = Some(source);
        receipt.initial_delivery_count = Some(0);
        receipt.max_message_size = Some(262_144);
        receipt.rcv_settle_mode = ReceiverSettleMode::Second;
        receipt.target = Some(Target::new("approved-queue").into());
        assert_eq!(receipt.validate_request(&session), Ok(()));
        assert_eq!(receipt.validate(&session, receipt.approval()), Ok(()));
        assert_eq!(receipt.attach().initial_delivery_count, Some(0));
        assert_eq!(receipt.attach().rcv_settle_mode, ReceiverSettleMode::Second);
    }

    #[test]
    fn attach_accessors_and_owned_parts_keep_provenance_separate() {
        let session = SessionIdentity::new();
        let mut receipt = IncomingAttach::new(attach(), session.clone(), 7);
        receipt.attach_mut().max_message_size = Some(512);
        assert_eq!(receipt.max_message_size, Some(512));
        let expected = receipt.attach().clone();
        let approval = receipt.approval().clone();
        let (request, captured) = receipt.into_parts();
        assert_eq!(request, expected);
        assert!(Arc::ptr_eq(&approval, &captured));

        let receipt = IncomingAttach::new(request.clone(), session, 7);
        let retained = receipt.approval().clone();
        assert_eq!(receipt.into_attach(), request);
        assert!(!Arc::ptr_eq(&retained, &approval));
    }

    #[test]
    fn retiring_pending_approval_retires_its_endpoint_generation_only() {
        let session = SessionIdentity::new();
        let old = IncomingAttach::new(attach(), session.clone(), 7);
        let replacement = IncomingAttach::new(old.attach().clone(), session, 7);
        let mut ledger = IncomingLedger::new();
        old.approval().retire();
        old.approval().retire();
        assert!(matches!(
            ledger.reserve(old.approval().link_identity(), 0, b"old"),
            Err(IncomingLedgerError::RetiredLink)
        ));
        assert!(
            ledger
                .reserve(replacement.approval().link_identity(), 0, b"new")
                .is_ok()
        );
    }

    #[test]
    fn prequeue_validation_does_not_claim_an_identical_pending_generation() {
        let session = SessionIdentity::new();
        let receipt = IncomingAttach::new(attach(), session.clone(), 7);
        let replacement = IncomingAttach::new(receipt.attach().clone(), session.clone(), 7);
        assert_eq!(receipt.validate_request(&session), Ok(()));
        assert_eq!(replacement.validate_request(&session), Ok(()));
        assert_eq!(
            receipt.validate(&session, replacement.approval()),
            Err(AttachApprovalError::StaleApproval)
        );
        assert_eq!(
            receipt.validate_request(&SessionIdentity::new()),
            Err(AttachApprovalError::WrongSession)
        );
        assert_eq!(receipt.validate(&session, receipt.approval()), Ok(()));
        assert_eq!(
            replacement.validate(&session, replacement.approval()),
            Ok(())
        );
    }

    #[test]
    fn retired_current_or_origin_sessions_fail_prequeue_and_final_validation() {
        let origin = SessionIdentity::new();
        let receipt = IncomingAttach::new(attach(), origin.clone(), 7);
        let retired_current = SessionIdentity::new();
        retired_current.retire();
        assert_eq!(
            receipt.validate_request(&retired_current),
            Err(AttachApprovalError::RetiredSession)
        );
        assert_eq!(
            receipt.validate(&retired_current, receipt.approval()),
            Err(AttachApprovalError::RetiredSession)
        );
        assert_eq!(receipt.validate_request(&origin), Ok(()));
        let retired_pending = IncomingAttach::new(attach(), retired_current, 7);
        assert_eq!(
            receipt.validate(&origin, retired_pending.approval()),
            Err(AttachApprovalError::RetiredSession)
        );
        assert_eq!(receipt.validate(&origin, receipt.approval()), Ok(()));
        origin.retire();
        for current in [origin.clone(), SessionIdentity::new()] {
            assert_eq!(
                receipt.validate_request(&current),
                Err(AttachApprovalError::RetiredSession)
            );
            assert_eq!(
                receipt.validate(&current, receipt.approval()),
                Err(AttachApprovalError::RetiredSession)
            );
        }
    }

    #[test]
    fn retired_receipt_or_pending_approval_cannot_validate_fresh_metadata() {
        let session = SessionIdentity::new();
        let mut receipt = IncomingAttach::new(attach(), session.clone(), 7);
        let replacement = IncomingAttach::new(receipt.attach().clone(), session.clone(), 7);
        receipt.approval().retire();
        receipt.initial_delivery_count = Some(0);
        receipt.source = Some(Source::new("fresh-metadata"));
        assert_eq!(
            receipt.validate_request(&session),
            Err(AttachApprovalError::RetiredApproval)
        );
        assert_eq!(
            receipt.validate(&session, receipt.approval()),
            Err(AttachApprovalError::RetiredApproval)
        );
        assert_eq!(replacement.validate_request(&session), Ok(()));
        assert_eq!(
            replacement.validate(&session, receipt.approval()),
            Err(AttachApprovalError::RetiredApproval)
        );
        assert_eq!(
            replacement.validate(&session, replacement.approval()),
            Ok(())
        );
        assert!(!session.is_retired());
        assert!(!replacement.approval().link_identity().is_retired());
    }

    #[test]
    fn retirement_is_rejected_before_mutated_identity_fields() {
        let session = SessionIdentity::new();
        let mut receipt = IncomingAttach::new(attach(), session.clone(), 7);
        receipt.role = Role::Receiver;
        receipt.approval().retire();
        assert_eq!(
            receipt.validate_request(&session),
            Err(AttachApprovalError::RetiredApproval)
        );
        session.retire();
        assert_eq!(
            receipt.validate_request(&session),
            Err(AttachApprovalError::RetiredSession)
        );
    }
}
