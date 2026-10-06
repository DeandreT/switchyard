use super::*;

fn fixture() -> (ServerSession, IncomingAttach) {
    let identity = SessionIdentity::new();
    let (commands, _commands) = mpsc::channel(1);
    let (_attaches, incoming_attaches) = mpsc::channel(1);
    let session = ServerSession {
        channel: 0,
        identity: identity.clone(),
        commands,
        incoming_attaches,
        consumed: Arc::new(Notify::new()),
    };
    let attach = IncomingAttach::new(
        Attach {
            name: "paging-origin".into(),
            handle: 7,
            role: Role::Receiver,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(crate::Source::new("orders")),
            target: None,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        },
        identity,
        0,
    );
    (session, attach)
}

#[test]
fn incoming_origin_preserves_metadata_but_refuses_changed_identity() {
    let (session, original) = fixture();
    assert!(session.validate_incoming_attach_origin(&original).is_ok());
    let mut metadata = original.clone();
    metadata.source.as_mut().expect("source").address = Some("granted".into());
    metadata.initial_delivery_count = Some(0);
    assert!(session.validate_incoming_attach_origin(&metadata).is_ok());
    for field in 0..3 {
        let mut changed = original.clone();
        match field {
            0 => changed.handle += 1,
            1 => changed.name.push_str("-changed"),
            _ => changed.role = Role::Sender,
        }
        assert!(matches!(
            session.validate_incoming_attach_origin(&changed),
            Err(EngineError::InvalidState(_))
        ));
    }
    assert!(session.validate_incoming_attach_origin(&original).is_ok());
}

#[test]
fn incoming_origin_observes_original_link_and_session_retirement() {
    let (session, receipt) = fixture();
    receipt.approval().retire();
    assert!(matches!(
        session.validate_incoming_attach_origin(&receipt),
        Err(EngineError::RemoteDetached)
    ));
    let (session, receipt) = fixture();
    session.identity.retire();
    assert!(matches!(
        session.validate_incoming_attach_origin(&receipt),
        Err(EngineError::RemoteDetached)
    ));
}

#[test]
fn incoming_origin_cannot_cross_identical_session_reuse_or_certify_pending_membership() {
    let (session, original) = fixture();
    let (replacement, _) = fixture();
    assert!(matches!(
        replacement.validate_incoming_attach_origin(&original),
        Err(EngineError::InvalidState(_))
    ));
    let identical = IncomingAttach::new(original.attach().clone(), session.identity.clone(), 0);
    // Both original receipts pass pure origin validation. Only the actor can choose its pending approval.
    assert!(session.validate_incoming_attach_origin(&original).is_ok());
    assert!(session.validate_incoming_attach_origin(&identical).is_ok());
    assert_eq!(
        original.validate(&session.identity, identical.approval()),
        Err(AttachApprovalError::StaleApproval)
    );
}
