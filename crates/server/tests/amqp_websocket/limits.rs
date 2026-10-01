use tokio_tungstenite::tungstenite::protocol::frame::{FrameHeader, coding::Control};

use super::*;

pub(super) async fn text_and_oversized_messages_are_refused<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, false, false).await?;
    let before = node.snapshot()?;
    for fragmented in [false, true] {
        let mut peer = RawPeer::new(node.websocket().await?);
        peer.header(ProtocolHeader::AMQP).await?;
        if fragmented {
            ws_send(
                &mut peer.socket,
                WsMessage::Frame(WsFrame::message(
                    vec![0_u8; MESSAGE_LIMIT],
                    OpCode::Data(Data::Binary),
                    false,
                )),
            )
            .await?;
            ws_send(
                &mut peer.socket,
                WsMessage::Frame(WsFrame::message(
                    vec![0_u8; 1],
                    OpCode::Data(Data::Continue),
                    true,
                )),
            )
            .await?;
        } else {
            let header = FrameHeader {
                opcode: OpCode::Data(Data::Binary),
                mask: Some([0; 4]),
                ..FrameHeader::default()
            };
            let mut bytes = Vec::new();
            header.format((MESSAGE_LIMIT + 1) as u64, &mut bytes)?;
            timeout(DEADLINE, peer.socket.get_mut().write_all(&bytes)).await??;
        }
        assert_close(&mut peer.socket, CloseCode::Size).await?;
        drop(peer);
        assert_eq!(node.snapshot()?, before);
        node.reusable(None).await?;
    }
    let mut peer = RawPeer::new(node.websocket().await?);
    peer.open().await?;
    ws_send(
        &mut peer.socket,
        WsMessage::Text("not AMQP binary data".into()),
    )
    .await?;
    assert_close(&mut peer.socket, CloseCode::Unsupported).await?;
    drop(peer);
    node.reusable(None).await?;
    assert_eq!(node.snapshot()?, before);
    Ok(())
}

pub(super) async fn malformed_websocket_frames_are_refused<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, false, false).await?;
    let before = node.snapshot()?;
    let frames = [
        WsFrame::message(amqp::AMQP_HEADER.to_vec(), OpCode::Data(Data::Binary), true),
        WsFrame::from_payload(
            FrameHeader {
                rsv1: true,
                opcode: OpCode::Data(Data::Binary),
                mask: Some([0; 4]),
                ..FrameHeader::default()
            },
            amqp::AMQP_HEADER.to_vec().into(),
        ),
        WsFrame::from_payload(
            FrameHeader {
                is_final: false,
                opcode: OpCode::Control(Control::Ping),
                mask: Some([0; 4]),
                ..FrameHeader::default()
            },
            b"fragmented-control".to_vec().into(),
        ),
        WsFrame::from_payload(
            FrameHeader {
                opcode: OpCode::Control(Control::Ping),
                mask: Some([0; 4]),
                ..FrameHeader::default()
            },
            vec![0_u8; 126].into(),
        ),
    ];
    for frame in frames {
        let mut socket = node.websocket().await?;
        let mut bytes = Vec::new();
        // Structured frame formatting bypasses the client's automatic masking
        // and validation without introducing a second production codec.
        frame.format(&mut bytes)?;
        timeout(DEADLINE, socket.get_mut().write_all(&bytes)).await??;
        assert_close(&mut socket, CloseCode::Protocol).await?;
        drop(socket);
        assert_eq!(node.snapshot()?, before);
        node.reusable(None).await?;
    }
    Ok(())
}

pub(super) async fn oversized_http_and_frame_lengths_are_bounded<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, false, false).await?;
    let before = node.snapshot()?;
    let request = handshake::upgrade_request(PATH, "amqp").replace(
        "\r\n\r\n",
        &format!("\r\nX-Padding: {}\r\n\r\n", "x".repeat(16 * 1024)),
    );
    let mut stream = node.transport(true).await?;
    timeout(DEADLINE, stream.write_all(request.as_bytes())).await??;
    assert!(
        !handshake::http_header(&mut stream)
            .await?
            .starts_with(b"HTTP/1.1 101")
    );
    drop(stream);
    node.reusable(None).await?;
    let mut peer = RawPeer::new(node.websocket().await?);
    peer.header(ProtocolHeader::AMQP).await?;
    let header = FrameHeader {
        opcode: OpCode::Data(Data::Binary),
        mask: Some([0; 4]),
        ..FrameHeader::default()
    };
    let mut bytes = Vec::new();
    header.format((MESSAGE_LIMIT + 1) as u64, &mut bytes)?;
    // No declared payload is sent: the length alone must trigger the cap.
    timeout(DEADLINE, peer.socket.get_mut().write_all(&bytes)).await??;
    assert_close(&mut peer.socket, CloseCode::Size).await?;
    drop(peer);
    node.reusable(None).await?;
    let mut peer = RawPeer::new(node.websocket().await?);
    peer.open().await?;
    // The native AMQP receive ceiling remains independent of WS's larger cap.
    let frame_header = [0, 4, 0, 1, 2, 0, 0, 0];
    ws_send(
        &mut peer.socket,
        WsMessage::Binary(frame_header.to_vec().into()),
    )
    .await?;
    match peer.read().await? {
        Frame::Amqp {
            performative: Some(Performative::Close(close)),
            ..
        } => {
            assert_eq!(
                close
                    .error
                    .expect("native frame-size refusal")
                    .condition
                    .as_symbol(),
                Symbol::from("amqp:connection:framing-error")
            );
        }
        other => panic!("expected native frame-size Close, got {other:?}"),
    }
    assert!(matches!(
        ws_next(&mut peer.socket).await?,
        WsMessage::Close(_)
    ));
    timeout(DEADLINE, peer.socket.flush()).await??;
    drop(peer);
    node.reusable(None).await?;
    assert_eq!(node.snapshot()?, before);
    Ok(())
}
