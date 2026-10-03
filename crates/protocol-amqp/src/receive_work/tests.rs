use domain::{EntityIncarnationKind, NamespaceName};

use super::*;
use crate::{ReceiveClaimState, ReceiveOwnerUnavailableCause};

pub(crate) fn submission() -> OwnedReceiveSubmission {
    let entity = EntityPath::new("Orders").expect("entity");
    let binding = EntityBinding::new(
        NamespaceName::new("tenant").expect("namespace"),
        entity.clone(),
        entity.clone(),
        EntityIncarnationKind::Queue,
        1,
    )
    .expect("binding");
    let (_, ticket) = ReceiveClaimPermit::new(u64::MAX);
    OwnedReceiveSubmission::new(binding, entity, ReceiveMode::PeekLock, None, ticket)
}

#[test]
fn packet_drop_cancels_pending_without_a_result() {
    let work = submission();
    let permit = work.permit().clone();
    assert_eq!(permit.state(), ReceiveClaimState::Pending);
    drop(work);
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
}

#[test]
fn decomposition_preserves_exact_ticket_route_and_receive_mode() {
    let work = submission();
    let permit = work.permit().clone();
    let identity = work.binding().clone();
    let target = work.entity().clone();
    let (ticket, binding, entity, mode, session) = work.into_owner_parts();
    assert_eq!(binding, identity);
    assert_eq!(entity, target);
    assert_eq!(mode, ReceiveMode::PeekLock);
    assert!(session.is_none());
    assert_eq!(ticket.claim_expiry_epoch_seconds(), u64::MAX);
    assert_eq!(ticket.try_claim(), Ok(()));
    assert_eq!(permit.state(), ReceiveClaimState::Started);
}

#[test]
fn debug_and_unavailable_diagnostics_are_static() {
    let debug = format!("{:?}", submission());
    assert!(!debug.contains("Orders"));
    assert!(!debug.contains("tenant"));
    for cause in [
        ReceiveOwnerUnavailableCause::Stopped,
        ReceiveOwnerUnavailableCause::ResponseUnavailable,
        ReceiveOwnerUnavailableCause::Storage,
        ReceiveOwnerUnavailableCause::Clock,
        ReceiveOwnerUnavailableCause::UnexpectedOutcome,
    ] {
        let error = ReceiveSubmitError::OwnerUnavailable(cause);
        assert!(
            error
                .to_string()
                .starts_with("the receive owner is unavailable")
        );
    }
}
