use serde_amqp::{Value, primitives::Binary};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    time::timeout,
};

use super::*;

const TEST_DEADLINE: Duration = Duration::from_secs(4);

async fn next_frame(peer: &mut DuplexStream) -> Frame {
    timeout(TEST_DEADLINE, read_frame(peer))
        .await
        .expect("the engine responds without another body read")
        .expect("valid AMQP frame")
}

async fn eof(peer: &mut DuplexStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    timeout(TEST_DEADLINE, peer.read_to_end(&mut bytes))
        .await
        .expect("the connection releases the socket")
        .expect("read until EOF");
    bytes
}

fn sized_open(bytes: u32, advertised_maximum: u32) -> Frame {
    let make = |padding| Frame::Amqp {
        channel: 0,
        performative: Some(Performative::Open(Open {
            max_frame_size: advertised_maximum,
            ..Open::new("x".repeat(padding))
        })),
        payload: Vec::new(),
    };
    let overhead = crate::encode_frame(&make(1024)).expect("encode Open").len() - 1024;
    let frame = make(bytes as usize - overhead);
    assert_eq!(
        crate::encode_frame(&frame).expect("encode Open").len(),
        bytes as usize
    );
    frame
}

fn sized_begin(bytes: u32, remote_channel: Option<u16>) -> Frame {
    let make = |padding| {
        let mut properties = Fields::default();
        properties.insert(
            Symbol::from("padding"),
            Value::Binary(Binary::from(vec![0; padding])),
        );
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Begin(Begin {
                remote_channel,
                properties: Some(properties),
                ..Begin::default()
            })),
            payload: Vec::new(),
        }
    };
    let overhead = crate::encode_frame(&make(1024))
        .expect("encode Begin")
        .len()
        - 1024;
    let frame = make(bytes as usize - overhead);
    assert_eq!(
        crate::encode_frame(&frame).expect("encode Begin").len(),
        bytes as usize
    );
    frame
}

async fn server_pair(peer_maximum: u32, wire_capacity: usize) -> (ServerConnection, DuplexStream) {
    let (wire, mut peer) = tokio::io::duplex(wire_capacity);
    let opening = async {
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("server header");
        write_amqp(
            &mut peer,
            0,
            Performative::Open(Open {
                max_frame_size: peer_maximum,
                ..Open::new("peer")
            }),
            Vec::new(),
        )
        .await
        .expect("peer Open");
        let Frame::Amqp {
            performative: Some(Performative::Open(open)),
            ..
        } = next_frame(&mut peer).await
        else {
            panic!("server Open");
        };
        assert_eq!(open.max_frame_size, DEFAULT_MAX_FRAME_SIZE);
        peer
    };
    let (connection, peer) = tokio::join!(ServerConnection::accept(wire, "server", None), opening);
    (connection.expect("server opens"), peer)
}

async fn assert_framing_close(peer: &mut DuplexStream) {
    let Frame::Amqp {
        performative: Some(Performative::Close(close)),
        ..
    } = next_frame(peer).await
    else {
        panic!("oversized running frame sends Close");
    };
    assert_eq!(
        close.error.expect("framing error").condition,
        crate::ErrorCondition::Custom(Symbol::from("amqp:connection:framing-error")),
    );
    assert!(eof(peer).await.is_empty());
}

fn assert_size_error(result: Result<ServerConnection, EngineError>, size: u32, maximum: u32) {
    let Err(EngineError::Io(error)) = result else {
        panic!("prefix is rejected with a frame-size error");
    };
    let error = error
        .get_ref()
        .and_then(|error| error.downcast_ref::<crate::codec::FrameSizeError>())
        .expect("typed size error");
    assert_eq!(error.size, size);
    assert_eq!(error.maximum, maximum);
}

#[tokio::test]
async fn server_advertises_its_own_limit_independent_of_the_peer_limit() {
    for peer_maximum in [MIN_MAX_FRAME_SIZE, DEFAULT_MAX_FRAME_SIZE, u32::MAX] {
        let (connection, mut peer) = server_pair(peer_maximum, 4096).await;
        connection.shutdown().await;
        assert!(eof(&mut peer).await.is_empty());
    }
}

#[tokio::test]
async fn server_accepts_an_open_at_the_initial_512_byte_boundary() {
    let (wire, mut peer) = tokio::io::duplex(4096);
    let opening = async {
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("server header");
        write_frame(
            &mut peer,
            &sized_open(MIN_MAX_FRAME_SIZE, DEFAULT_MAX_FRAME_SIZE),
        )
        .await
        .expect("boundary Open");
        assert!(matches!(
            next_frame(&mut peer).await,
            Frame::Amqp {
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        peer
    };
    let (connection, mut peer) =
        tokio::join!(ServerConnection::accept(wire, "server", None), opening);
    connection.expect("boundary Open accepted").shutdown().await;
    assert!(eof(&mut peer).await.is_empty());
}

#[tokio::test]
async fn server_rejects_a_pre_open_513_byte_prefix_without_reading_the_body() {
    let (wire, mut peer) = tokio::io::duplex(4096);
    let opening = async {
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("server header");
        peer.write_all(&(MIN_MAX_FRAME_SIZE + 1).to_be_bytes())
            .await
            .expect("oversized prefix");
        assert!(eof(&mut peer).await.is_empty());
    };
    let (result, ()) = tokio::join!(ServerConnection::accept(wire, "server", None), opening);
    assert_size_error(result, MIN_MAX_FRAME_SIZE + 1, MIN_MAX_FRAME_SIZE);
}

#[tokio::test]
async fn server_refuses_a_peer_advertised_limit_below_the_protocol_minimum() {
    let (wire, mut peer) = tokio::io::duplex(4096);
    let opening = async {
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("server header");
        write_amqp(
            &mut peer,
            0,
            Performative::Open(Open {
                max_frame_size: 511,
                ..Open::new("peer")
            }),
            Vec::new(),
        )
        .await
        .expect("peer Open");
        assert!(eof(&mut peer).await.is_empty());
    };
    let (result, ()) = tokio::join!(ServerConnection::accept(wire, "server", None), opening);
    assert!(matches!(result, Err(EngineError::InvalidState(_))));
}

#[tokio::test]
async fn server_accepts_a_running_frame_at_its_advertised_boundary() {
    let (mut connection, mut peer) = server_pair(u32::MAX, 4096).await;
    let frame = sized_begin(DEFAULT_MAX_FRAME_SIZE, None);
    let sending = write_frame(&mut peer, &frame);
    let receiving = connection.next_incoming_session();
    let (sent, incoming) = tokio::join!(sending, receiving);
    sent.expect("boundary Begin sent");
    assert!(
        incoming
            .expect("boundary frame decoded")
            .begin
            .properties
            .is_some()
    );
    connection.shutdown().await;
    assert!(eof(&mut peer).await.is_empty());
}

#[tokio::test]
async fn peer_advertised_large_frames_do_not_enlarge_the_server_read_limit() {
    let (connection, mut peer) = server_pair(u32::MAX, 4096).await;
    peer.write_all(&(DEFAULT_MAX_FRAME_SIZE + 1).to_be_bytes())
        .await
        .expect("oversized prefix only");
    assert_framing_close(&mut peer).await;
    connection.shutdown().await;
}

#[tokio::test]
async fn framing_error_close_has_a_deadline_when_the_peer_does_not_read() {
    let (connection, mut peer) = server_pair(u32::MAX, 1).await;
    peer.write_all(&(DEFAULT_MAX_FRAME_SIZE + 1).to_be_bytes())
        .await
        .expect("oversized prefix only");
    timeout(TEST_DEADLINE, connection.lifecycle.wait_terminated())
        .await
        .expect("bounded error Close write");
    let partial = eof(&mut peer).await;
    assert_eq!(
        partial.len(),
        1,
        "the deliberately blocked Close write never resumes"
    );
}

#[tokio::test]
async fn server_rejects_a_long_local_container_before_handshake_io() {
    let (wire, mut peer) = tokio::io::duplex(4096);
    let (result, bytes) = tokio::join!(
        ServerConnection::accept(wire, "x".repeat(512), None),
        eof(&mut peer)
    );
    assert!(matches!(result, Err(EngineError::InvalidState(_))));
    assert!(bytes.is_empty());
}

struct Anonymous;

impl SaslAuthenticator for Anonymous {
    fn mechanisms(&self) -> Vec<Symbol> {
        vec![Symbol::from("ANONYMOUS")]
    }
    fn authenticate(&self, _: &SaslInit) -> SaslCode {
        SaslCode::Ok
    }
}

#[tokio::test]
async fn server_sasl_reads_use_the_local_resource_limit() {
    let (wire, mut peer) = tokio::io::duplex(4096);
    let opening = async {
        write_protocol_header(&mut peer, ProtocolHeader::SASL)
            .await
            .expect("peer SASL header");
        expect_header(&mut peer, ProtocolHeader::SASL)
            .await
            .expect("server SASL header");
        assert!(matches!(
            next_frame(&mut peer).await,
            Frame::Sasl(SaslPerformative::Mechanisms(_))
        ));
        peer.write_all(&(DEFAULT_MAX_FRAME_SIZE + 1).to_be_bytes())
            .await
            .expect("oversized SASL prefix");
        assert!(eof(&mut peer).await.is_empty());
    };
    let (result, ()) = tokio::join!(
        ServerConnection::accept(wire, "server", Some(Arc::new(Anonymous))),
        opening
    );
    assert_size_error(result, DEFAULT_MAX_FRAME_SIZE + 1, DEFAULT_MAX_FRAME_SIZE);
}

#[cfg(feature = "test-client")]
mod client_limits {
    use super::*;

    async fn client_pair(local_maximum: u32) -> (ClientConnection, DuplexStream) {
        let (wire, mut peer) = tokio::io::duplex(4096);
        let opening = async {
            expect_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("client header");
            write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("peer header");
            let Frame::Amqp {
                performative: Some(Performative::Open(open)),
                ..
            } = next_frame(&mut peer).await
            else {
                panic!("client Open");
            };
            assert_eq!(
                open.max_frame_size,
                local_maximum.min(crate::codec::MAX_FRAME_SIZE as u32)
            );
            write_amqp(
                &mut peer,
                0,
                Performative::Open(Open::new("peer")),
                Vec::new(),
            )
            .await
            .expect("peer Open");
            peer
        };
        let (connection, peer) = tokio::join!(
            ClientConnection::builder()
                .max_frame_size(local_maximum)
                .open_with_stream(wire),
            opening
        );
        (connection.expect("client opens"), peer)
    }

    fn assert_client_size_error(
        result: Result<ClientConnection, EngineError>,
        size: u32,
        maximum: u32,
    ) {
        let Err(EngineError::Io(error)) = result else {
            panic!("prefix rejected");
        };
        let error = error
            .get_ref()
            .and_then(|error| error.downcast_ref::<crate::codec::FrameSizeError>())
            .expect("typed size error");
        assert_eq!((error.size, error.maximum), (size, maximum));
    }

    #[tokio::test]
    async fn builder_clamps_its_advertised_limit_to_the_codec_ceiling() {
        let (connection, mut peer) = client_pair(u32::MAX).await;
        connection.shutdown().await;
        assert!(eof(&mut peer).await.is_empty());
    }

    #[tokio::test]
    async fn invalid_local_limits_and_long_container_ids_are_rejected_before_io() {
        for builder in [
            ClientConnection::builder().max_frame_size(511),
            ClientConnection::builder().container_id("x".repeat(512)),
        ] {
            let (wire, mut peer) = tokio::io::duplex(4096);
            let (result, bytes) = tokio::join!(builder.open_with_stream(wire), eof(&mut peer));
            assert!(matches!(result, Err(EngineError::InvalidState(_))));
            assert!(bytes.is_empty());
        }
        let result = ClientConnection::builder()
            .max_frame_size(511)
            .open("invalid URL")
            .await;
        let Err(EngineError::InvalidState(reason)) = result else {
            panic!("local config refused");
        };
        assert!(
            reason.contains("512"),
            "validation precedes URL parsing or connecting"
        );
    }

    #[tokio::test]
    async fn client_pre_open_reads_use_512_even_when_advertising_a_larger_limit() {
        let (wire, mut peer) = tokio::io::duplex(4096);
        let opening = async {
            expect_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("client header");
            write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("peer header");
            let _ = next_frame(&mut peer).await;
            peer.write_all(&513_u32.to_be_bytes())
                .await
                .expect("oversized pre-Open prefix");
            assert!(eof(&mut peer).await.is_empty());
        };
        let (result, ()) = tokio::join!(ClientConnection::open(wire, "client", None), opening);
        assert_client_size_error(result, 513, 512);
    }

    #[tokio::test]
    async fn client_accepts_a_remote_open_at_the_initial_boundary() {
        let (wire, mut peer) = tokio::io::duplex(4096);
        let opening = async {
            expect_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("client header");
            write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("peer header");
            let _ = next_frame(&mut peer).await;
            write_frame(&mut peer, &sized_open(512, DEFAULT_MAX_FRAME_SIZE))
                .await
                .expect("boundary Open");
            peer
        };
        let (connection, mut peer) =
            tokio::join!(ClientConnection::open(wire, "client", None), opening);
        connection.expect("boundary accepted").shutdown().await;
        assert!(eof(&mut peer).await.is_empty());
    }

    #[tokio::test]
    async fn client_refuses_remote_advertised_limits_below_512() {
        let (wire, mut peer) = tokio::io::duplex(4096);
        let opening = async {
            expect_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("client header");
            write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                .await
                .expect("peer header");
            let _ = next_frame(&mut peer).await;
            write_amqp(
                &mut peer,
                0,
                Performative::Open(Open {
                    max_frame_size: 511,
                    ..Open::new("peer")
                }),
                Vec::new(),
            )
            .await
            .expect("peer Open");
            assert!(eof(&mut peer).await.is_empty());
        };
        let (result, ()) = tokio::join!(ClientConnection::open(wire, "client", None), opening);
        assert!(matches!(result, Err(EngineError::InvalidState(_))));
    }

    #[tokio::test]
    async fn client_running_reader_uses_its_own_advertised_limit() {
        let (mut connection, mut peer) = client_pair(512).await;
        let answering = async {
            assert!(matches!(
                next_frame(&mut peer).await,
                Frame::Amqp {
                    performative: Some(Performative::Begin(_)),
                    ..
                }
            ));
            write_frame(&mut peer, &sized_begin(512, Some(0)))
                .await
                .expect("boundary Begin");
        };
        let (session, ()) = tokio::join!(connection.begin(), answering);
        let _session = session.expect("boundary frame accepted");
        peer.write_all(&513_u32.to_be_bytes())
            .await
            .expect("oversized prefix only");
        assert_framing_close(&mut peer).await;
        connection.shutdown().await;
    }

    #[tokio::test]
    async fn client_sasl_mechanisms_and_outcome_use_its_local_limit() {
        for reject_outcome in [false, true] {
            let (wire, mut peer) = tokio::io::duplex(4096);
            let opening = async {
                expect_header(&mut peer, ProtocolHeader::SASL)
                    .await
                    .expect("client SASL header");
                write_protocol_header(&mut peer, ProtocolHeader::SASL)
                    .await
                    .expect("peer SASL header");
                if reject_outcome {
                    write_frame(
                        &mut peer,
                        &Frame::Sasl(SaslPerformative::Mechanisms(SaslMechanisms {
                            mechanisms: vec![Symbol::from("ANONYMOUS")],
                        })),
                    )
                    .await
                    .expect("mechanisms");
                    assert!(matches!(
                        next_frame(&mut peer).await,
                        Frame::Sasl(SaslPerformative::Init(_))
                    ));
                }
                peer.write_all(&513_u32.to_be_bytes())
                    .await
                    .expect("oversized SASL prefix");
                assert!(eof(&mut peer).await.is_empty());
            };
            let opening_client = ClientConnection::builder()
                .max_frame_size(512)
                .sasl(SaslInit {
                    mechanism: Symbol::from("ANONYMOUS"),
                    initial_response: None,
                    hostname: None,
                })
                .open_with_stream(wire);
            let (result, ()) = tokio::join!(opening_client, opening);
            assert_client_size_error(result, 513, 512);
        }
    }
}
