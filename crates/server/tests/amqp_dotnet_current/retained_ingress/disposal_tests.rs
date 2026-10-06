use std::{future::Future, panic::AssertUnwindSafe};

use amqp::{Close, Frame, Open, Performative, ProtocolHeader};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use super::*;

const OBSERVATION_DEADLINE: Duration = Duration::from_secs(5);

async fn drive<T>(
    controller: &mut fixture::Controller,
    original: impl Future<Output = TestResult<T>>,
) -> TestResult<T> {
    tokio::pin!(original);
    loop {
        tokio::select! {
            result = &mut original => return result,
            () = controller.step() => {},
        }
    }
}

async fn hello(peer: &mut TcpStream) -> TestResult {
    amqp::write_protocol_header(peer, ProtocolHeader::AMQP).await?;
    assert_eq!(
        amqp::read_protocol_header(peer).await?,
        ProtocolHeader::AMQP
    );
    amqp::write_frame(
        peer,
        &Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(Open::new("sdk-disposal-peer"))),
            payload: Vec::new(),
        },
    )
    .await?;
    assert!(matches!(
        amqp::read_frame(peer).await?,
        Frame::Amqp { channel: 0, performative: Some(Performative::Open(_)), payload }
            if payload.is_empty()
    ));
    Ok(())
}

async fn close_and_read_reply(
    peer: &mut TcpStream,
    channel: u16,
    close: Close,
    payload: Vec<u8>,
) -> TestResult {
    amqp::write_frame(
        peer,
        &Frame::Amqp {
            channel,
            performative: Some(Performative::Close(close)),
            payload,
        },
    )
    .await?;
    assert!(matches!(
        amqp::read_frame(peer).await?,
        Frame::Amqp { channel: 0, performative: Some(Performative::Close(close)), payload }
            if close.error.is_none() && payload.is_empty()
    ));
    Ok(())
}

fn sole_report(fixture: &fixture::Fixture) -> &fixture::Report {
    assert!(!fixture.controller.has_setup_failure());
    assert_eq!(fixture.controller.lifetime, 1);
    assert_eq!(fixture.controller.report_count(), 1);
    let report = fixture.controller.reports[0]
        .as_ref()
        .expect("original connection report");
    assert_eq!(report.anchor().ordinal, 0);
    assert_eq!(report.session_attempts(), 0);
    assert_eq!(report.session_joins(), 0);
    assert_eq!(report.worker_launches(), 0);
    assert_eq!(report.worker_joins(), 0);
    assert!(report.admissions().is_empty());
    assert!(report.sessions().is_empty());
    assert_eq!(report.workers().count(), 0);
    report
}

fn propagate_observation(
    observation: Result<
        Result<TestResult, tokio::time::error::Elapsed>,
        Box<dyn std::any::Any + Send>,
    >,
) -> TestResult {
    match observation {
        Ok(result) => result?,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

#[tokio::test]
async fn peer_close_disposal_keeps_expected_native_errors_raw() -> TestResult {
    let mut fixture = fixture::Fixture::start(false).await?;
    let mut peer = None;
    let observation = AssertUnwindSafe(tokio::time::timeout(OBSERVATION_DEADLINE, async {
        peer = Some(fixture.connect_peer().await?);
        let original = peer.as_mut().expect("original peer socket");
        drive(&mut fixture.controller, async {
            hello(original).await?;
            close_and_read_reply(original, 0, Close::default(), Vec::new()).await
        })
        .await
    }))
    .catch_unwind()
    .await;
    // Keep the original peer open until native finish observes its Reader token.
    fixture.finish().await;
    propagate_observation(observation)?;
    let report = sole_report(&fixture);
    let socket = report.socket();
    assert!(completed_peer_close(socket));
    let reader_error = socket
        .reader()
        .expect("original Reader result")
        .as_ref()
        .expect_err("open peer leaves the original Reader for actual shutdown");
    assert!(reader_error.is_cancelled());
    let reader = socket
        .native_observations()
        .reader()
        .expect("original Reader observation");
    assert_eq!(reader_error.id(), reader.id());
    assert!(reader.requested_by(amqp::ServerConnectionAbortSource::ActorReaderShutdown));
    assert!(
        !unexpected_report_failure(report),
        "{:?}",
        RedactedReport(report)
    );
    assert!(
        report.has_failures(),
        "raw cancellation remains a library failure"
    );
    if let Some(original) = report.native_close() {
        assert!(matches!(original, Err(amqp::EngineError::Stopped)));
        assert!(!unexpected_native_close(Some(original), true, true));
    }

    // These are controlled predicate inputs, not recreated connection receipts.
    assert!(!unexpected_native_close(None, false, false));
    let ok = Ok(());
    assert!(!unexpected_native_close(Some(&ok), false, false));
    let stopped = Err(amqp::EngineError::Stopped);
    for (peer_completed, shutdown) in [(false, false), (false, true), (true, false), (true, true)] {
        assert_eq!(
            unexpected_native_close(Some(&stopped), peer_completed, shutdown),
            !(peer_completed && shutdown),
        );
    }
    for original in [
        Err(amqp::EngineError::RemoteClosed),
        Err(amqp::EngineError::RemoteDetached),
        Err(amqp::EngineError::Io(std::io::Error::other(
            "controlled-disposal-io",
        ))),
        Err(amqp::EngineError::Timeout("controlled-disposal")),
        Err(amqp::EngineError::InvalidState(
            "controlled-disposal".to_owned(),
        )),
    ] {
        assert!(unexpected_native_close(Some(&original), true, true));
    }
    drop(peer);
    Ok(())
}

#[derive(Clone, Copy)]
enum RefusedPeerClose {
    Error,
    NonzeroChannel,
    NonemptyPayload,
}

#[tokio::test]
async fn peer_close_error_and_framing_never_pass_sdk_disposal() -> TestResult {
    for case in [
        RefusedPeerClose::Error,
        RefusedPeerClose::NonzeroChannel,
        RefusedPeerClose::NonemptyPayload,
    ] {
        let mut fixture = fixture::Fixture::start(false).await?;
        let mut peer = None;
        let channel = if matches!(case, RefusedPeerClose::NonzeroChannel) {
            1
        } else {
            0
        };
        let payload = if matches!(case, RefusedPeerClose::NonemptyPayload) {
            vec![0x71]
        } else {
            Vec::new()
        };
        let close = Close {
            error: matches!(case, RefusedPeerClose::Error).then(|| {
                amqp::Error::new(
                    amqp::AmqpError::InternalError,
                    "controlled-peer-close",
                    None,
                )
            }),
        };
        let observation = AssertUnwindSafe(tokio::time::timeout(OBSERVATION_DEADLINE, async {
            peer = Some(fixture.connect_peer().await?);
            let original = peer.as_mut().expect("original peer socket");
            drive(&mut fixture.controller, async {
                hello(original).await?;
                close_and_read_reply(original, channel, close, payload).await
            })
            .await
        }))
        .catch_unwind()
        .await;
        fixture.finish().await;
        propagate_observation(observation)?;
        let report = sole_report(&fixture);
        let original = report
            .socket()
            .native_observations()
            .peer_close()
            .expect("actual decoded peer Close");
        assert_eq!(original.channel(), channel);
        assert_eq!(
            original.payload().is_empty(),
            !matches!(case, RefusedPeerClose::NonemptyPayload)
        );
        assert_eq!(
            original.close().error.is_some(),
            matches!(case, RefusedPeerClose::Error)
        );
        assert!(!original.locally_closing());
        assert_eq!(
            original.reply_state(),
            amqp::ServerPeerCloseReplyState::Ready
        );
        assert!(matches!(original.reply_result(), Some(Ok(()))));
        assert!(!completed_peer_close(report.socket()));
        assert!(unexpected_report_failure(report));
        drop(peer);
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum NonPeerDisposal {
    Eof,
    ServerRequestedStop,
}

#[tokio::test]
async fn eof_and_server_requested_stop_never_pass_sdk_disposal() -> TestResult {
    for case in [NonPeerDisposal::Eof, NonPeerDisposal::ServerRequestedStop] {
        let mut fixture = fixture::Fixture::start(false).await?;
        let mut peer = None;
        let observation = AssertUnwindSafe(tokio::time::timeout(OBSERVATION_DEADLINE, async {
            peer = Some(fixture.connect_peer().await?);
            let original = peer.as_mut().expect("original peer socket");
            drive(&mut fixture.controller, async { hello(original).await }).await?;
            match case {
                NonPeerDisposal::Eof => {
                    drive(&mut fixture.controller, async {
                        original.shutdown().await?;
                        let mut byte = [0];
                        assert_eq!(original.read(&mut byte).await?, 0);
                        Ok(())
                    })
                    .await?;
                }
                NonPeerDisposal::ServerRequestedStop => fixture.controller.close_and_stop(),
            }
            Ok::<_, Box<dyn Error>>(())
        }))
        .catch_unwind()
        .await;
        fixture.finish().await;
        propagate_observation(observation)?;
        let report = sole_report(&fixture);
        assert!(report.socket().native_observations().peer_close().is_none());
        assert!(!completed_peer_close(report.socket()));
        assert!(unexpected_report_failure(report));
        drop(peer);
    }
    Ok(())
}
