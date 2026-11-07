use super::*;

async fn confirm(
    sessions: &mut HashMap<u16, SessionState>,
    channel: u16,
    handle: u32,
    incarnation: u64,
) -> (Result<(), EngineError>, Vec<u8>) {
    let (reply, response) = oneshot::channel();
    let mut wire = Vec::new();
    test_handle_command(
        Command::Confirm {
            channel,
            handle,
            incarnation,
            delivery_id: 7,
            state: DeliveryState::Accepted(Accepted),
            batchable: false,
            reply,
        },
        &mut wire,
        sessions,
        u32::MAX,
    )
    .await
    .expect("confirmation must not stop the engine");
    (response.await.expect("actual engine reply"), wire)
}

#[tokio::test]
async fn retired_confirmation_returns_detached_without_writing_to_an_absent_session() {
    let (result, bytes) = confirm(&mut HashMap::new(), 3, 1, 7).await;
    assert!(matches!(result, Err(EngineError::RemoteDetached)));
    assert!(bytes.is_empty());
}

#[tokio::test]
async fn active_confirmation_on_a_receiving_link_is_still_an_invalid_state() {
    let (mut sessions, _deliveries) = receiving_session(3, 1);
    let current = sessions[&3].links[&1].incarnation();
    let (result, bytes) = confirm(&mut sessions, 3, 1, current).await;
    assert!(
        matches!(result, Err(EngineError::InvalidState(_))),
        "an active wrong-role command must not be mistaken for retired custody"
    );
    assert!(bytes.is_empty());
    assert_eq!(sessions[&3].links[&1].incarnation(), current);
}

#[tokio::test]
async fn stale_confirmation_cannot_write_to_a_reused_receiving_link() {
    let (mut sessions, _deliveries) = receiving_session(3, 1);
    let current = sessions[&3].links[&1].incarnation();
    let (result, bytes) = confirm(&mut sessions, 3, 1, current.wrapping_sub(1)).await;
    assert!(matches!(result, Err(EngineError::RemoteDetached)));
    assert!(bytes.is_empty());
    assert_eq!(sessions[&3].links[&1].incarnation(), current);
}

#[tokio::test]
async fn active_confirmation_write_failure_remains_an_engine_fault() {
    let (mut sessions, _outcome) = sending_session(3, 1, 7, ReceiverSettleMode::Second);
    let incarnation = sessions[&3].links[&1].incarnation();
    let (mut wire, peer) = tokio::io::duplex(1024);
    drop(peer);
    let (reply, response) = oneshot::channel();
    let result = test_handle_command(
        Command::Confirm {
            channel: 3,
            handle: 1,
            incarnation,
            delivery_id: 7,
            state: DeliveryState::Accepted(Accepted),
            batchable: false,
            reply,
        },
        &mut wire,
        &mut sessions,
        u32::MAX,
    )
    .await;
    assert!(
        matches!(result, Err(EngineError::Io(_))),
        "active IO failures must still stop the engine"
    );
    assert!(matches!(
        response.await.expect("active confirmation failure reply"),
        Err(EngineError::InvalidState(_))
    ));
}
