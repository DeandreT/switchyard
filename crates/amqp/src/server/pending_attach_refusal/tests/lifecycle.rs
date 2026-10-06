use super::fixture::*;
use super::*;

#[tokio::test]
async fn unpolled_and_cancelled_rejection_preserve_actor_ownership() {
    let mut state = State::new(Role::Sender, 512);
    let (commands, mut queued) = mpsc::channel(1);
    let (_incoming, incoming_attaches) = mpsc::channel(1);
    let session = Arc::new(ServerSession {
        channel: LOCAL,
        identity: state.sessions[&LOCAL].identity.clone(),
        commands,
        incoming_attaches,
        consumed: Arc::new(Notify::new()),
    });
    drop(session.reject_attach(state.receipt.clone(), denied()));
    assert!(queued.try_recv().is_err());
    assert!(state.original_owned());
    assert!(state.frames().await.is_empty());
    let caller_session = session.clone();
    let receipt = state.receipt.clone();
    let mut caller =
        tokio::spawn(async move { caller_session.reject_attach(receipt, denied()).await });
    let command = bounded(queued.recv()).await.expect("actual queued command");
    caller.abort();
    let caller_result = bounded(&mut caller).await;
    assert!(caller_result.expect_err("cancelled caller").is_cancelled());
    bounded(handle_command(
        command,
        &mut state.writer,
        &mut state.sessions,
        512,
    ))
    .await
    .expect("actor command");
    assert!(state.sessions[&LOCAL].pending_attaches.is_empty());
    assert!(state.sessions[&LOCAL].closing_handles.contains(&HANDLE));
    assert!(state.sessions[&LOCAL].handle_aliases.contains_key(&HANDLE));
    state
        .input(Performative::Detach(Detach {
            handle: PEER_HANDLE,
            closed: true,
            error: None,
        }))
        .await
        .expect("original acknowledgement");
    assert!(!state.sessions[&LOCAL].handle_aliases.contains_key(&HANDLE));
}

#[tokio::test]
async fn refusal_flush_cancellation_and_disconnect_join_original_tasks() {
    for (disconnect, skip_flushes) in [(false, 0), (true, 0), (false, 1), (true, 1)] {
        let mut socket = Socket::new().await;
        let mut session = socket.session().await;
        let receipt = socket.receipt(&mut session, Role::Sender).await;
        socket.gate.arm_after(skip_flushes);
        socket.callers.push(tokio::spawn(async move {
            session.reject_attach(receipt, denied()).await
        }));
        let observed = caught(async {
            socket.gate.entered().await;
            assert!(!socket.callers[0].is_finished());
            if skip_flushes == 1 {
                assert!(matches!(
                    bounded(read_frame(&mut socket.peer))
                        .await
                        .expect("flushed Attach"),
                    Frame::Amqp {
                        performative: Some(Performative::Attach(_)),
                        ..
                    }
                ));
            }
            socket.callers[0].abort();
            socket.gate.release();
            if disconnect {
                bounded(tokio::io::AsyncWriteExt::shutdown(&mut socket.peer))
                    .await
                    .expect("peer disconnect");
            }
        })
        .await;
        let report = socket.finish().await;
        if let Err(payload) = observed {
            std::panic::resume_unwind(payload);
        }
        assert!(matches!(&socket.caller_results[0], Err(error) if error.is_cancelled()));
        assert!(matches!(&socket.acceptance, Ok(Ok(()))));
        assert!(report.actor().is_some_and(Result::is_ok));
        assert!(report.reader().is_some());
        assert!(Arc::ptr_eq(report.anchor(), &socket.anchor));
    }
}
