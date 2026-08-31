use super::*;

#[tokio::test]
async fn a_stale_sender_detach_cannot_remove_a_reused_handle() {
    let channel = 3;
    let handle = 1;
    let mut sessions = queued_sending_session(channel, handle, 0, VecDeque::new());
    let current = sessions[&channel].links[&handle].incarnation();
    let stale = current.wrapping_add(1);
    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);

    detach(channel, handle, stale, &mut wire, &mut sessions).await;
    assert_eq!(sessions[&channel].links[&handle].incarnation(), current);
    assert_no_frame(&mut peer).await;

    detach(channel, handle, current, &mut wire, &mut sessions).await;
    assert!(!sessions[&channel].links.contains_key(&handle));
    assert_detach_frame(channel, handle, &mut peer).await;
}

#[tokio::test]
async fn a_stale_receiver_detach_cannot_remove_a_reused_handle() {
    let channel = 3;
    let handle = 1;
    let (mut sessions, _deliveries) = receiving_session(channel, handle);
    let current = sessions[&channel].links[&handle].incarnation();
    let stale = current.wrapping_add(1);
    let (mut wire, mut peer) = tokio::io::duplex(64 * 1024);

    detach(channel, handle, stale, &mut wire, &mut sessions).await;
    assert_eq!(sessions[&channel].links[&handle].incarnation(), current);
    assert_no_frame(&mut peer).await;

    detach(channel, handle, current, &mut wire, &mut sessions).await;
    assert!(!sessions[&channel].links.contains_key(&handle));
    assert_detach_frame(channel, handle, &mut peer).await;
}

async fn detach(
    channel: u16,
    handle: u32,
    incarnation: u64,
    wire: &mut tokio::io::DuplexStream,
    sessions: &mut HashMap<u16, SessionState>,
) {
    let (reply, response) = oneshot::channel();
    test_handle_command(
        Command::Detach {
            channel,
            handle,
            incarnation,
            error: None,
            reply,
        },
        wire,
        sessions,
        u32::MAX,
    )
    .await
    .expect("detach handling keeps the connection live");
    response
        .await
        .expect("the detach response remains live")
        .expect("a current or stale endpoint can close idempotently");
}

async fn assert_detach_frame(channel: u16, handle: u32, peer: &mut tokio::io::DuplexStream) {
    let Frame::Amqp {
        channel: actual_channel,
        performative: Some(Performative::Detach(detach)),
        ..
    } = read_frame(peer)
        .await
        .expect("the current detach frame decodes")
    else {
        panic!("expected a detach frame");
    };
    assert_eq!(actual_channel, channel);
    assert_eq!(detach.handle, handle);
    assert!(detach.closed);
}

async fn assert_no_frame(peer: &mut tokio::io::DuplexStream) {
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), read_frame(peer))
            .await
            .is_err(),
        "a stale detach must write no frame"
    );
}
