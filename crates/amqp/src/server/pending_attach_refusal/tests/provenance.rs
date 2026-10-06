use super::fixture::*;
use super::*;

#[tokio::test]
async fn reject_attach_rejects_a_receipt_from_another_session() {
    let mut state = State::new(Role::Sender, 512);
    let foreign = IncomingAttach::new(attach(Role::Sender), SessionIdentity::new(), HANDLE);
    assert!(matches!(
        state.reject(foreign).await,
        Err(EngineError::InvalidState(_))
    ));
    assert!(state.original_owned());
    assert!(state.frames().await.is_empty());
}

#[tokio::test]
async fn reject_attach_rejects_changed_receipt_identity_fields() {
    for field in 0..3 {
        let mut state = State::new(Role::Sender, 512);
        let mut changed = state.receipt.clone();
        match field {
            0 => changed.handle += 1,
            1 => changed.name.push_str("-changed"),
            _ => changed.role = changed.role.opposite(),
        }
        assert!(matches!(
            state.reject(changed).await,
            Err(EngineError::InvalidState(_))
        ));
        assert!(state.original_owned());
        assert!(state.frames().await.is_empty());
    }
}

#[tokio::test]
async fn reject_attach_rejects_stale_accepted_or_retired_receipts() {
    for case in 0..5 {
        let mut state = State::new(Role::Sender, 512);
        let mut original = state.receipt.clone();
        match case {
            0 => {
                let replacement = IncomingAttach::new(
                    attach(Role::Sender),
                    state.sessions[&LOCAL].identity.clone(),
                    HANDLE,
                );
                state
                    .sessions
                    .get_mut(&LOCAL)
                    .expect("session")
                    .pending_attaches
                    .insert(HANDLE, PendingLinkFlow::incoming(&replacement));
            }
            1 => {
                let (deliveries_tx, _inbox) = mpsc::channel(1);
                let (detached_tx, _detached) = watch::channel(false);
                let (reply, response) = oneshot::channel();
                let command = Command::AcceptLink {
                    channel: LOCAL,
                    session: state.sessions[&LOCAL].identity.clone(),
                    attach: Box::new(original.clone()),
                    max_message_size: 4096,
                    properties: None,
                    decoders: MessageFormatDecoders::default(),
                    deliveries_tx,
                    detached_tx,
                    consumption: Arc::new(Consumption::new(Arc::new(Notify::new()))),
                    reply,
                };
                bounded(handle_command(
                    command,
                    &mut state.writer,
                    &mut state.sessions,
                    512,
                ))
                .await
                .expect("ordinary acceptance");
                bounded(response)
                    .await
                    .expect("ordinary reply")
                    .expect("accepted");
                assert!(state.sessions[&LOCAL].links.contains_key(&HANDLE));
                let _positive = state.frames().await;
            }
            2 => original.approval().retire(),
            3 => state.sessions[&LOCAL].identity.retire(),
            _ => {
                let mut request = attach(Role::Sender);
                request.source = None;
                request.target = Some(crate::Coordinator::default().into());
                let kind = native_transactions::classify_attach(
                    &request,
                    native_transactions::NativeIngressPolicy::Posting,
                )
                .expect("native kind");
                original = IncomingAttach::new_with_kind(
                    request,
                    state.sessions[&LOCAL].identity.clone(),
                    HANDLE,
                    kind,
                );
                let session = state.sessions.get_mut(&LOCAL).expect("session");
                session
                    .pending_attaches
                    .insert(HANDLE, PendingLinkFlow::incoming(&original));
                session
                    .handle_aliases
                    .get_mut(&HANDLE)
                    .expect("alias")
                    .identity = original.approval().link_identity().clone();
            }
        }
        assert!(state.reject(original).await.is_err());
        assert!(state.frames().await.is_empty());
    }
}
