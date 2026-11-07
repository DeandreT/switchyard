//! The native accept deadline bounds SASL and Open, and only them.

use std::{io, sync::Arc, time::Duration};

use amqp::{
    Begin, EngineError, Frame, Open, Performative, ProtocolHeader, SaslAuthenticator, SaslCode,
    SaslInit, SaslPerformative, ServerConnection, Symbol, read_frame, read_protocol_header,
    write_frame, write_protocol_header,
};
use tokio::{
    io::{AsyncReadExt, DuplexStream, duplex},
    time::{Instant, timeout},
};

#[path = "../src/test_deadline.rs"]
mod test_deadline;

use test_deadline::WAIT;

const BUDGET: Duration = Duration::from_millis(200);

struct Anonymous;

impl SaslAuthenticator for Anonymous {
    fn mechanisms(&self) -> Vec<Symbol> {
        vec![Symbol::from("ANONYMOUS")]
    }

    fn authenticate(&self, _: &SaslInit) -> SaslCode {
        SaslCode::Ok
    }
}

fn frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

/// The stage the peer completes before it stalls.
#[derive(Clone, Copy, Debug)]
enum Stall {
    Silent,
    SaslInit,
    Open,
}

async fn reach(peer: &mut DuplexStream, stall: Stall) {
    match stall {
        Stall::Silent => {}
        Stall::SaslInit => {
            write_protocol_header(peer, ProtocolHeader::SASL)
                .await
                .unwrap();
            assert_eq!(
                read_protocol_header(peer).await.unwrap(),
                ProtocolHeader::SASL
            );
            assert!(matches!(
                read_frame(peer).await.unwrap(),
                Frame::Sasl(SaslPerformative::Mechanisms(_))
            ));
        }
        Stall::Open => {
            write_protocol_header(peer, ProtocolHeader::AMQP)
                .await
                .unwrap();
            assert_eq!(
                read_protocol_header(peer).await.unwrap(),
                ProtocolHeader::AMQP
            );
        }
    }
}

#[tokio::test]
async fn a_peer_stalled_before_sasl_or_open_times_out_and_its_stream_is_closed() {
    for stall in [Stall::Silent, Stall::SaslInit, Stall::Open] {
        let (server, mut peer) = duplex(64 * 1024);
        let sasl = matches!(stall, Stall::SaslInit)
            .then(|| Arc::new(Anonymous) as Arc<dyn SaslAuthenticator>);
        let started = Instant::now();
        let deadline = started + BUDGET;
        let (accepted, ()) = timeout(WAIT, async {
            tokio::join!(
                ServerConnection::accept_before(server, "deadline-server", sasl, deadline),
                reach(&mut peer, stall)
            )
        })
        .await
        .expect("the expired handshake returns");
        let Err(EngineError::Io(error)) = accepted else {
            panic!("{stall:?}: the handshake fails at the deadline");
        };
        assert_eq!(error.kind(), io::ErrorKind::TimedOut, "{stall:?}");
        assert!(
            Instant::now() >= deadline,
            "{stall:?}: not before the budget"
        );
        let mut rest = Vec::new();
        assert_eq!(
            timeout(WAIT, peer.read_to_end(&mut rest))
                .await
                .expect("the dropped handshake closes its stream")
                .unwrap(),
            0,
            "{stall:?}: nothing follows the stalled stage"
        );
    }
}

#[tokio::test]
async fn an_open_connection_outlives_its_admission_deadline() {
    let (server, mut peer) = duplex(64 * 1024);
    let deadline = Instant::now() + BUDGET;
    let (accepted, ()) = timeout(WAIT, async {
        tokio::join!(
            ServerConnection::accept_before(server, "deadline-server", None, deadline),
            async {
                reach(&mut peer, Stall::Open).await;
                write_frame(
                    &mut peer,
                    &frame(0, Performative::Open(Open::new("deadline-peer"))),
                )
                .await
                .unwrap();
                assert!(matches!(
                    read_frame(&mut peer).await.unwrap(),
                    Frame::Amqp {
                        performative: Some(Performative::Open(_)),
                        ..
                    }
                ));
            }
        )
    })
    .await
    .expect("handshake completes");
    let mut connection = accepted.expect("Open before the deadline is admitted");
    // Waiting out the budget is the condition under test, not a witness.
    tokio::time::sleep_until(deadline + BUDGET).await;
    write_frame(&mut peer, &frame(1, Performative::Begin(Begin::default())))
        .await
        .unwrap();
    assert!(
        timeout(WAIT, connection.next_incoming_session())
            .await
            .expect("a Begin after the deadline reaches the open connection")
            .is_some()
    );
    timeout(WAIT, connection.shutdown())
        .await
        .expect("shutdown joins")
        .expect("tasks join cleanly");
}
