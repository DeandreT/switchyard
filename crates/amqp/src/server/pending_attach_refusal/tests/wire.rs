use super::super::super::error_links::MAX_ERROR_LINK_NAMES;
use super::fixture::*;
use super::*;

#[tokio::test]
async fn pending_refusal_writes_minimal_attach_then_error_detach_for_both_roles() {
    for role in [Role::Sender, Role::Receiver] {
        let mut state = State::new(role.clone(), 512);
        if role == Role::Receiver {
            state
                .sessions
                .get_mut(&LOCAL)
                .expect("session")
                .pending_attaches
                .get_mut(&HANDLE)
                .expect("pending")
                .update(Flow {
                    handle: Some(HANDLE),
                    delivery_count: Some(0),
                    link_credit: Some(7),
                    ..Flow::default()
                })
                .expect("early peer credit");
        }
        let frozen = state.receipt.approval().refusal_attach();
        state
            .reject(state.receipt.clone())
            .await
            .expect("pending refusal");
        let frames = state.frames().await;
        assert_eq!(frames.len(), 2);
        match &frames[0] {
            Frame::Amqp {
                channel,
                performative: Some(Performative::Attach(actual)),
                payload,
            } => {
                assert_eq!(*channel, LOCAL);
                assert!(payload.is_empty());
                assert_eq!(
                    crate::encode_frame(&frames[0]).expect("actual encoding"),
                    crate::encode_frame(&Frame::Amqp {
                        channel: LOCAL,
                        performative: Some(Performative::Attach(Box::new(frozen))),
                        payload: Vec::new()
                    })
                    .expect("frozen encoding")
                );
                assert!(actual.source.is_none() && actual.target.is_none());
                assert_eq!(
                    actual.initial_delivery_count,
                    (role == Role::Receiver).then_some(0)
                );
            }
            other => panic!("expected minimal attach, got {other:?}"),
        }
        assert!(matches!(
            &frames[1],
            Frame::Amqp {
                channel: LOCAL,
                performative: Some(Performative::Detach(Detach {
                    handle: HANDLE,
                    closed: true,
                    error: Some(_),
                    ..
                })),
                ..
            }
        ));
        let session = &state.sessions[&LOCAL];
        assert!(session.links.is_empty());
        assert!(session.pending_attaches.is_empty());
        assert!(state.receipt.approval().link_identity().is_retired());
        assert!(session.closing_handles.contains(&HANDLE));
        assert!(session.handle_aliases[&HANDLE].error_detached);
    }
}

#[tokio::test]
async fn pending_refusal_retains_strict_error_histories_and_errant_frames() {
    for case in 0..5 {
        let mut state = State::new(Role::Sender, 512);
        let owner = state.receipt.approval().link_identity().clone();
        state
            .sessions
            .get_mut(&LOCAL)
            .expect("session")
            .error_deliveries
            .record(&Role::Sender, &owner, &HashSet::from([23]))
            .expect("existing delivery history");
        state.reject(state.receipt.clone()).await.expect("refusal");
        let _refusal = state.frames().await;
        assert!(
            state
                .writer
                .error_link_names()
                .owner("pending-refusal", &Role::Receiver)
                .expect("name owner")
                .same_link(&owner)
        );
        assert!(
            state.sessions[&LOCAL]
                .error_peer_handles
                .owner(PEER_HANDLE)
                .expect("peer owner")
                .same_link(&owner)
        );
        assert!(
            state.sessions[&LOCAL]
                .error_deliveries
                .owner(&Role::Sender, 23)
                .expect("delivery owner")
                .same_link(&owner)
        );
        if case == 0 {
            state
                .input(Performative::Detach(Detach {
                    handle: PEER_HANDLE,
                    closed: true,
                    error: None,
                }))
                .await
                .expect("original peer acknowledgement");
            assert!(!state.sessions[&LOCAL].handle_aliases.contains_key(&HANDLE));
            assert!(!state.sessions[&LOCAL].closing_handles.contains(&HANDLE));
            assert!(
                state.sessions[&LOCAL]
                    .error_peer_handles
                    .owner(PEER_HANDLE)
                    .expect("history persists")
                    .same_link(&owner)
            );
            assert!(state.frames().await.is_empty());
            continue;
        }
        let errant = match case {
            1 => Performative::Flow(Flow {
                handle: Some(PEER_HANDLE),
                delivery_count: Some(0),
                link_credit: Some(1),
                ..Flow::default()
            }),
            2 => Performative::Transfer(Transfer {
                handle: PEER_HANDLE,
                delivery_id: None,
                delivery_tag: None,
                message_format: None,
                settled: None,
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            }),
            3 => Performative::Attach(Box::new(attach(Role::Sender))),
            _ => Performative::Disposition(Disposition {
                role: Role::Sender,
                first: 23,
                last: None,
                settled: true,
                state: None,
                batchable: false,
            }),
        };
        state
            .input(errant)
            .await
            .expect("strict existing frame path");
        assert!(state.sessions[&LOCAL].ending);
        assert!(state.frames().await.iter().any(|frame| matches!(
            frame,
            Frame::Amqp {
                performative: Some(Performative::End(End { error: Some(_), .. })),
                ..
            }
        )));
        assert!(state.sessions[&LOCAL].links.is_empty());
    }
}

#[tokio::test]
async fn refusal_preflight_uses_existing_bounded_failure_paths() {
    for case in 0..3 {
        let mut request = attach(Role::Sender);
        if case == 0 {
            request.name = "x".repeat(1024);
        }
        let mut state = State::with_attach(request, 512);
        if case == 2 {
            for index in 0..MAX_ERROR_LINK_NAMES {
                state
                    .writer
                    .error_link_names_mut()
                    .record(
                        format!("old-{index}").into(),
                        &Role::Sender,
                        &LinkIdentity::new(),
                    )
                    .expect("bounded history seed");
            }
        }
        let (reply, response) = oneshot::channel();
        let error = if case == 1 {
            Error::new(crate::AmqpError::UnauthorizedAccess, "x".repeat(1024), None)
        } else {
            denied()
        };
        let result = handle_rejection(
            LOCAL,
            state.sessions[&LOCAL].identity.clone(),
            state.receipt.clone(),
            error,
            reply,
            &mut state.sessions,
            &mut state.writer,
        )
        .await;
        if case == 1 {
            assert!(result.is_err());
            assert!(state.frames().await.is_empty());
            assert!(state.original_owned());
            assert!(bounded(response).await.is_err());
        } else {
            result.expect("existing session fallback");
            assert!(matches!(
                bounded(response).await.expect("reply"),
                Err(EngineError::RemoteDetached)
            ));
            assert!(state.sessions[&LOCAL].ending);
            let frames = state.frames().await;
            assert!(frames.iter().any(|frame| matches!(
                frame,
                Frame::Amqp {
                    performative: Some(Performative::End(_)),
                    ..
                }
            )));
            assert!(!frames.iter().any(|frame| matches!(
                frame,
                Frame::Amqp {
                    performative: Some(
                        Performative::Attach(_) | Performative::Flow(_) | Performative::Transfer(_)
                    ),
                    ..
                }
            )));
        }
        assert!(state.sessions[&LOCAL].links.is_empty());
    }
}
