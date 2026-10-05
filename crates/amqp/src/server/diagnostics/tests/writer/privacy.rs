use super::*;
use crate::{AmqpError, Close, Error, Open, SaslMechanisms, SaslPerformative};
use serde_amqp::primitives::Symbol;

#[tokio::test(start_paused = true)]
async fn actual_writer_rows_are_private_owned_and_immutable_after_source_drop() -> TestResult {
    let recorder = ServerDiagnosticRecorder::try_new()?;
    let recorder_weak = Arc::downgrade(&recorder.0);
    let (mut output, control, _) = writer(Mode::Healthy, Some(&recorder), None)?;
    let control_weak = Arc::downgrade(&control);
    for frame in [
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(Open::new("PRIVATE_CONTAINER_NAME"))),
            payload: Vec::new(),
        },
        transfer(b"PRIVATE_BODY_PRIVATE_TOKEN_PRIVATE_TXID".to_vec()),
        Frame::Sasl(SaslPerformative::Mechanisms(SaslMechanisms {
            mechanisms: vec![Symbol::from("PRIVATE_CREDENTIAL")],
        })),
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Close(Close {
                error: Some(Error::new(
                    AmqpError::InvalidField,
                    "PRIVATE_DESCRIPTION",
                    None,
                )),
            })),
            payload: Vec::new(),
        },
    ] {
        output.write_frame(&frame).await?;
    }
    let (mut failing, failure_control, _) = writer(
        Mode::ErrorFlush(io::ErrorKind::TimedOut),
        Some(&recorder),
        None,
    )?;
    let error = failing
        .write_frame(&heartbeat())
        .await
        .expect_err("typed private error");
    assert_native_error(&error, io::ErrorKind::TimedOut, &failure_control);
    let capture = recorder.capture()?;
    let rows = capture.records().to_vec();
    let formatted = capture.format_bounded()?;
    for private in [
        "PRIVATE_CONTAINER_NAME",
        "PRIVATE_BODY",
        "PRIVATE_TOKEN",
        "PRIVATE_TXID",
        "PRIVATE_TAG",
        "PRIVATE_CREDENTIAL",
        "PRIVATE_DESCRIPTION",
        "PRIVATE_IO_DISPLAY",
        "PRIVATE_IO_DEBUG",
    ] {
        assert!(!formatted.contains(private));
    }
    assert!(formatted.is_ascii());
    assert!(formatted.len() <= MAX_SERVER_DIAGNOSTIC_FORMAT_BYTES);
    assert!(
        capture
            .records()
            .iter()
            .all(|record| matches!(record.event(), DiagnosticEvent::Writer(_)))
    );
    assert_eq!(capture.records().len(), 24);
    assert_eq!(failure_control.rendering.load(Ordering::Relaxed), 0);
    drop(error);
    drop(failing);
    drop(failure_control);
    drop(output);
    drop(control);
    drop(recorder);
    assert!(control_weak.upgrade().is_none());
    assert!(recorder_weak.upgrade().is_none());
    assert_eq!(capture.records(), rows);
    assert_eq!(capture.format_bounded()?, formatted);
    Ok(())
}
