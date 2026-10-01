use tokio_tungstenite::tungstenite::{Error as WsError, protocol::Role as WsRole};

use super::*;

pub(super) fn upgrade_request(path: &str, protocol: &str) -> String {
    format!(
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: {protocol}\r\n\r\n"
    )
}

pub(super) async fn http_header(stream: &mut Box<dyn Io>) -> TestResult<Vec<u8>> {
    timeout(DEADLINE, async {
        let mut bytes = Vec::new();
        loop {
            let mut byte = [0_u8; 1];
            match stream.read(&mut byte).await {
                Ok(0) => break,
                Ok(_) => bytes.push(byte[0]),
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::ConnectionReset | io::ErrorKind::UnexpectedEof
                    ) =>
                {
                    break;
                }
                Err(error) => return Err(error.into()),
            }
            if bytes.ends_with(b"\r\n\r\n") {
                break;
            }
            assert!(
                bytes.len() <= 16 * 1024,
                "HTTP response must remain bounded"
            );
        }
        Ok::<_, Box<dyn Error>>(bytes)
    })
    .await?
}

pub(super) async fn strict_http_path_and_subprotocol<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider, false, false).await?;
    let before = node.snapshot()?;
    for (path, protocol, status) in [
        ("/", Some("amqp"), 404),
        ("/$servicebus/WebSocket/", Some("amqp"), 404),
        ("/$servicebus/websocket/?x=1", Some("amqp"), 404),
        (PATH, None, 400),
        (PATH, Some("AMQP"), 400),
        (PATH, Some("unknown"), 400),
    ] {
        let result = timeout(
            DEADLINE,
            client_async(node.request(path, protocol)?, node.transport(true).await?),
        )
        .await?;
        match result {
            Err(WsError::Http(response)) => assert_eq!(response.status().as_u16(), status),
            Err(error) => panic!("explicit callback refusal must return HTTP {status}: {error}"),
            Ok(_) => panic!("unexpected successful upgrade {path} / {protocol:?}"),
        }
        assert_eq!(node.snapshot()?, before);
        node.reusable(None).await?;
    }
    // Selection is exact among offered tokens, not equality against the entire header.
    let (mut socket, response) = timeout(
        DEADLINE,
        client_async(
            node.request(PATH, Some("other, amqp"))?,
            node.transport(true).await?,
        ),
    )
    .await??;
    assert_eq!(response.headers()["Sec-WebSocket-Protocol"], "amqp");
    timeout(DEADLINE, socket.close(None)).await??;
    drop(socket);
    node.reusable(None).await?;
    for request in [
        upgrade_request(PATH, "amqp").replace("Host: localhost\r\n", ""),
        upgrade_request(PATH, "amqp").replace("GET ", "POST "),
        upgrade_request(PATH, "amqp")
            .replace("Sec-WebSocket-Version: 13", "Sec-WebSocket-Version: 12"),
        upgrade_request(PATH, "amqp").replace("dGhlIHNhbXBsZSBub25jZQ==", "bad-key"),
        upgrade_request(PATH, "amqp").replace("\r\n\r\n", "\r\nContent-Length: 1\r\n\r\nx"),
    ] {
        let mut stream = node.transport(true).await?;
        timeout(DEADLINE, stream.write_all(request.as_bytes())).await??;
        let response = http_header(&mut stream).await?;
        assert!(
            !response.starts_with(b"HTTP/1.1 101"),
            "malformed request upgraded: {response:?}"
        );
        drop(stream);
        assert_eq!(node.snapshot()?, before);
        node.reusable(None).await?;
    }
    Ok(())
}

pub(super) async fn incomplete_http_and_open_release_admission<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::with_timeout(provider, false, false, Duration::from_millis(400)).await?;
    let before = node.snapshot()?;
    let mut silent = node.transport(true).await?;
    assert!(http_header(&mut silent).await?.is_empty());
    drop(silent);
    node.reusable(None).await?;
    let request = upgrade_request(PATH, "amqp");
    let mut stream = node.transport(true).await?;
    timeout(DEADLINE, stream.write_all(&request.as_bytes()[..5])).await??;
    tokio::time::sleep(Duration::from_millis(250)).await;
    timeout(DEADLINE, stream.write_all(&request.as_bytes()[5..])).await??;
    assert!(http_header(&mut stream).await?.starts_with(b"HTTP/1.1 101"));
    let socket = WebSocketStream::from_raw_socket(stream, WsRole::Client, None).await;
    let mut peer = RawPeer::new(socket);
    peer.header(ProtocolHeader::AMQP).await?;
    timeout(Duration::from_millis(300), async {
        assert!(matches!(
            ws_next(&mut peer.socket).await?,
            WsMessage::Close(_)
        ));
        peer.socket.flush().await?;
        Ok::<(), Box<dyn Error>>(())
    })
    .await??;
    drop(peer);
    node.reusable(None).await?;
    // An upgraded socket with only a partial frame also shares the Open deadline.
    let mut stalled = node.websocket().await?;
    ws_send(
        &mut stalled,
        WsMessage::Binary(amqp::AMQP_HEADER.to_vec().into()),
    )
    .await?;
    assert_eq!(
        ws_next(&mut stalled).await?,
        WsMessage::Binary(amqp::AMQP_HEADER.to_vec().into())
    );
    let open = encode_frame(&amqp_frame(0, Performative::Open(Open::new("partial"))))?;
    ws_send(&mut stalled, WsMessage::Binary(open[..4].to_vec().into())).await?;
    assert!(matches!(ws_next(&mut stalled).await?, WsMessage::Close(_)));
    timeout(DEADLINE, stalled.flush()).await??;
    drop(stalled);
    node.reusable(None).await?;
    assert_eq!(node.snapshot()?, before);
    Ok(())
}

pub(super) async fn strict_first_and_post_sasl_headers<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, false, true).await?;
    let before = node.snapshot()?;
    for bytes in [
        amqp::SASL_HEADER[..4].to_vec(),
        [amqp::SASL_HEADER.as_slice(), b"extra"].concat(),
    ] {
        let mut socket = node.websocket().await?;
        ws_send(&mut socket, WsMessage::Binary(bytes.into())).await?;
        assert_close(&mut socket, CloseCode::Protocol).await?;
        drop(socket);
        node.reusable(Some(plain(KEY))).await?;
    }
    for bytes in [
        amqp::AMQP_HEADER[..4].to_vec(),
        [amqp::AMQP_HEADER.as_slice(), b"extra"].concat(),
    ] {
        let mut peer = RawPeer::new(node.websocket().await?);
        peer.header(ProtocolHeader::SASL).await?;
        assert!(matches!(
            peer.read().await?,
            Frame::Sasl(amqp::SaslPerformative::Mechanisms(_))
        ));
        let init = Frame::Sasl(amqp::SaslPerformative::Init(plain(KEY)));
        ws_send(
            &mut peer.socket,
            WsMessage::Binary(encode_frame(&init)?.into()),
        )
        .await?;
        assert!(
            matches!(peer.read().await?, Frame::Sasl(amqp::SaslPerformative::Outcome(outcome)) if outcome.code == amqp::SaslCode::Ok)
        );
        ws_send(&mut peer.socket, WsMessage::Binary(bytes.into())).await?;
        assert_close(&mut peer.socket, CloseCode::Protocol).await?;
        drop(peer);
        node.reusable(Some(plain(KEY))).await?;
    }
    assert_eq!(node.snapshot()?, before);
    Ok(())
}
