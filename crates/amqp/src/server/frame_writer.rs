//! An encoded frame is checked against the peer's limit before any socket write.

use std::io;

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{Frame, Performative, codec};

use super::{ConnectionOptions, content_budget::ContentBudget, idle::Activity};

const MIN_FRAME_SIZE: u32 = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("encoded AMQP frame of {actual} bytes exceeds the peer maximum of {maximum}")]
pub(super) struct FrameWriteError {
    pub actual: u32,
    pub maximum: u32,
}

pub(super) struct FrameWriter<W> {
    inner: W,
    maximum: u32,
    options: ConnectionOptions,
    peer_idle_millis: u32,
    activity: Activity,
    content_budget: ContentBudget,
}

impl<W> FrameWriter<W> {
    pub fn new(inner: W, maximum: u32) -> io::Result<Self> {
        if maximum < MIN_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "maximum frame size must be at least 512 bytes",
            ));
        }
        Ok(Self {
            inner,
            maximum: maximum.min(codec::MAX_FRAME_SIZE as u32),
            options: ConnectionOptions::default(),
            peer_idle_millis: 0,
            activity: Activity::new(),
            content_budget: ContentBudget::default(),
        })
    }

    pub fn content_budget(&self) -> &ContentBudget {
        &self.content_budget
    }

    #[cfg(test)]
    pub fn new_with_content_budget(
        inner: W,
        maximum: u32,
        budget: ContentBudget,
    ) -> io::Result<Self> {
        let mut writer = Self::new(inner, maximum)?;
        writer.content_budget = budget;
        Ok(writer)
    }

    pub fn configure_activity(
        &mut self,
        options: ConnectionOptions,
        peer_idle_millis: u32,
        activity: Activity,
    ) {
        self.options = options;
        self.peer_idle_millis = peer_idle_millis;
        self.activity = activity;
    }

    pub fn maximum_frame_size(&self) -> u32 {
        self.maximum
    }

    /// Peer-limit failures are typed; codec errors (including its own ceiling)
    /// retain their original cause. Neither can emit a partial frame.
    pub fn encoded_frame(&self, frame: &Frame) -> io::Result<Vec<u8>> {
        let encoded = codec::encode_frame(frame)?;
        let actual = encoded.len() as u32;
        if actual > self.maximum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                FrameWriteError {
                    actual,
                    maximum: self.maximum,
                },
            ));
        }
        Ok(encoded)
    }
}

impl<W: AsyncWrite + Unpin> FrameWriter<W> {
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        if self.activity.is_tainted() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "an incomplete AMQP frame has tainted the transport",
            ));
        }
        let encoded = self.encoded_frame(frame)?;
        let close = matches!(
            frame,
            Frame::Amqp {
                performative: Some(Performative::Close(_)),
                ..
            }
        );
        let deadline = self
            .activity
            .write_deadline(self.options, self.peer_idle_millis, close)?;
        self.activity.begin_write(close);
        let mut result = tokio::time::timeout_at(deadline, async {
            self.inner.write_all(&encoded).await?;
            self.inner.flush().await
        })
        .await
        .unwrap_or_else(|_| {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "AMQP frame write or flush timed out",
            ))
        });
        if result.is_ok() && tokio::time::Instant::now() >= deadline {
            result = Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "AMQP frame write or flush completed after its deadline",
            ));
        }
        if result.is_ok() {
            self.activity.completed_write();
        } else {
            self.activity.failed_write();
        }
        result
    }

    pub async fn write_amqp(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Vec<u8>,
    ) -> io::Result<()> {
        self.write_frame(&Frame::Amqp {
            channel,
            performative: Some(performative),
            payload,
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
    };

    use super::*;
    use crate::{
        AmqpError, Begin, Close, Error, Open, SaslChallenge, SaslMechanisms, SaslPerformative,
        Transfer,
    };
    use serde_amqp::primitives::{Binary, Symbol};

    #[derive(Clone, Default)]
    struct SentinelWriter {
        calls: Arc<AtomicUsize>,
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl AsyncWrite for SentinelWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.bytes.lock().expect("bytes").extend_from_slice(buffer);
            Poll::Ready(Ok(buffer.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn transfer(payload: Vec<u8>) -> Frame {
        Frame::Amqp {
            channel: 3,
            performative: Some(Performative::Transfer(Transfer {
                handle: 19,
                delivery_id: Some(27),
                delivery_tag: Some(vec![1, 2, 3].into()),
                message_format: Some(0),
                settled: Some(false),
                more: true,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            })),
            payload,
        }
    }

    fn transfer_with_size(size: usize) -> Frame {
        let overhead = codec::encode_frame(&transfer(Vec::new()))
            .expect("empty transfer")
            .len();
        let frame = transfer(vec![7; size.checked_sub(overhead).expect("frame overhead")]);
        if size <= codec::MAX_FRAME_SIZE {
            assert_eq!(
                codec::encode_frame(&frame).expect("transfer frame").len(),
                size
            );
        }
        frame
    }

    fn close_with_description(description: String) -> Frame {
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Close(Close {
                error: Some(Error::new(AmqpError::InvalidField, description, None)),
            })),
            payload: Vec::new(),
        }
    }

    fn control_with_size(size: usize) -> Frame {
        // Keep the string and containing lists in their 32-bit encodings so
        // changing the description length changes the frame length exactly.
        let base = codec::encode_frame(&close_with_description("x".repeat(256)))
            .expect("control frame")
            .len();
        let frame = close_with_description("x".repeat(256 + size - base));
        assert_eq!(
            codec::encode_frame(&frame).expect("control frame").len(),
            size
        );
        frame
    }

    fn typed_overflow(error: &io::Error, actual: u32, maximum: u32) {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error
                .get_ref()
                .and_then(|cause| cause.downcast_ref::<FrameWriteError>()),
            Some(&FrameWriteError { actual, maximum })
        );
    }

    #[test]
    fn malformed_configuration_is_rejected_without_polling_the_writer() {
        for maximum in [0, 1, 7, 511] {
            let sentinel = SentinelWriter::default();
            let error = match FrameWriter::new(sentinel.clone(), maximum) {
                Ok(_) => panic!("invalid maximum accepted"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(sentinel.calls.load(Ordering::Relaxed), 0);
            assert!(sentinel.bytes.lock().expect("bytes").is_empty());
        }
    }

    #[test]
    fn maximum_is_clamped_to_the_codec_ceiling() {
        for maximum in [codec::MAX_FRAME_SIZE as u32, u32::MAX] {
            let writer = FrameWriter::new((), maximum).expect("maximum");
            assert_eq!(writer.maximum_frame_size(), codec::MAX_FRAME_SIZE as u32);
        }
        let writer = FrameWriter::new((), 512).expect("minimum maximum");
        assert_eq!(writer.maximum_frame_size(), 512);
    }

    #[test]
    fn control_and_transfer_frames_accept_exact_cap_and_reject_one_extra_byte() {
        for build in [control_with_size as fn(usize) -> Frame, transfer_with_size] {
            for maximum in [512, 1_024] {
                let writer = FrameWriter::new((), maximum).expect("maximum");
                let accepted = build(maximum as usize);
                assert_eq!(
                    writer.encoded_frame(&accepted).expect("exact cap").len(),
                    maximum as usize
                );
                let refused = build(maximum as usize + 1);
                let error = writer.encoded_frame(&refused).expect_err("cap plus one");
                typed_overflow(&error, maximum + 1, maximum);
            }
        }
    }

    #[tokio::test]
    async fn rejected_control_and_transfer_frames_emit_no_first_byte() {
        for frame in [control_with_size(513), transfer_with_size(513)] {
            let sentinel = SentinelWriter::default();
            let mut writer = FrameWriter::new(sentinel.clone(), 512).expect("maximum");
            let error = writer
                .write_frame(&frame)
                .await
                .expect_err("oversized frame");
            typed_overflow(&error, 513, 512);
            assert_eq!(sentinel.calls.load(Ordering::Relaxed), 0);
            assert!(sentinel.bytes.lock().expect("bytes").is_empty());
        }
    }

    #[tokio::test]
    async fn preflight_rejection_does_not_poison_later_valid_writes() {
        let sentinel = SentinelWriter::default();
        let mut writer = FrameWriter::new(sentinel.clone(), 512).expect("maximum");
        let error = writer
            .write_frame(&control_with_size(513))
            .await
            .expect_err("oversized control");
        typed_overflow(&error, 513, 512);
        let valid = transfer_with_size(512);
        let expected = codec::encode_frame(&valid).expect("valid frame");
        writer.write_frame(&valid).await.expect("valid retry");
        assert_eq!(*sentinel.bytes.lock().expect("bytes"), expected);
        assert_eq!(sentinel.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn write_amqp_checks_the_same_cap_and_preserves_channel_and_payload() {
        let sentinel = SentinelWriter::default();
        let mut writer = FrameWriter::new(sentinel.clone(), 512).expect("maximum");
        let Frame::Amqp {
            channel,
            performative,
            payload,
        } = transfer_with_size(513)
        else {
            panic!("AMQP frame")
        };
        let error = writer
            .write_amqp(channel, performative.expect("transfer"), payload)
            .await
            .expect_err("oversized AMQP frame");
        typed_overflow(&error, 513, 512);
        assert_eq!(sentinel.calls.load(Ordering::Relaxed), 0);
        let valid = transfer_with_size(512);
        let expected = codec::encode_frame(&valid).expect("valid frame");
        let Frame::Amqp {
            channel,
            performative,
            payload,
        } = valid
        else {
            panic!("AMQP frame")
        };
        writer
            .write_amqp(channel, performative.expect("transfer"), payload)
            .await
            .expect("valid AMQP frame");
        assert_eq!(*sentinel.bytes.lock().expect("bytes"), expected);
    }

    #[tokio::test]
    async fn codec_ceiling_failure_also_emits_no_bytes() {
        let sentinel = SentinelWriter::default();
        let mut writer = FrameWriter::new(sentinel.clone(), u32::MAX).expect("clamped maximum");
        let exact = transfer_with_size(codec::MAX_FRAME_SIZE);
        assert_eq!(
            writer.encoded_frame(&exact).expect("exact ceiling").len(),
            codec::MAX_FRAME_SIZE
        );
        let over = transfer_with_size(codec::MAX_FRAME_SIZE + 1);
        let error = writer.write_frame(&over).await.expect_err("codec ceiling");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .get_ref()
                .and_then(|cause| cause.downcast_ref::<FrameWriteError>())
                .is_none()
        );
        assert_eq!(sentinel.calls.load(Ordering::Relaxed), 0);
        assert!(sentinel.bytes.lock().expect("bytes").is_empty());
    }

    #[test]
    fn arbitrary_control_encodings_use_actual_encoded_length_not_a_fixed_reserve() {
        let writer = FrameWriter::new((), 512).expect("maximum");
        for length in [0, 1, 254, 255, 256, 500, 700] {
            for frame in [
                close_with_description("x".repeat(length)),
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Open(Open::new("x".repeat(length)))),
                    payload: Vec::new(),
                },
            ] {
                let encoded = codec::encode_frame(&frame).expect("control frame");
                if encoded.len() <= 512 {
                    assert_eq!(writer.encoded_frame(&frame).expect("fits"), encoded);
                } else {
                    let error = writer
                        .encoded_frame(&frame)
                        .expect_err("actual encoded cap");
                    typed_overflow(&error, encoded.len() as u32, 512);
                }
            }
        }
    }

    #[tokio::test]
    async fn heartbeat_and_sasl_frames_share_the_preflight_boundary() {
        let sentinel = SentinelWriter::default();
        let mut writer = FrameWriter::new(sentinel.clone(), 512).expect("maximum");
        let heartbeat = Frame::Amqp {
            channel: 0,
            performative: None,
            payload: Vec::new(),
        };
        writer.write_frame(&heartbeat).await.expect("heartbeat");
        assert_eq!(
            *sentinel.bytes.lock().expect("bytes"),
            codec::encode_frame(&heartbeat).expect("heartbeat bytes")
        );
        let calls = sentinel.calls.load(Ordering::Relaxed);
        let before = sentinel.bytes.lock().expect("bytes").clone();
        let challenge = Frame::Sasl(SaslPerformative::Challenge(SaslChallenge {
            challenge: Binary::from(vec![7; 512]),
        }));
        let actual = codec::encode_frame(&challenge).expect("SASL frame").len() as u32;
        let error = writer
            .write_frame(&challenge)
            .await
            .expect_err("oversized SASL frame");
        typed_overflow(&error, actual, 512);
        assert_eq!(sentinel.calls.load(Ordering::Relaxed), calls);
        assert_eq!(*sentinel.bytes.lock().expect("bytes"), before);
    }

    #[tokio::test]
    async fn codec_validation_failure_is_rejected_before_any_write() {
        let sentinel = SentinelWriter::default();
        let mut writer = FrameWriter::new(sentinel.clone(), 512).expect("maximum");
        let invalid = Frame::Sasl(SaslPerformative::Mechanisms(SaslMechanisms {
            mechanisms: vec![Symbol::from("non-ascii-\u{e9}")],
        }));
        let error = writer
            .write_frame(&invalid)
            .await
            .expect_err("invalid symbol");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(sentinel.calls.load(Ordering::Relaxed), 0);
        assert!(sentinel.bytes.lock().expect("bytes").is_empty());
    }

    #[tokio::test]
    async fn consecutive_nonfragmentable_control_frames_are_not_coalesced() {
        let sentinel = SentinelWriter::default();
        let mut writer = FrameWriter::new(sentinel.clone(), 512).expect("maximum");
        let frames = [
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Begin(Begin::default())),
                payload: Vec::new(),
            },
            control_with_size(512),
        ];
        let mut expected = Vec::new();
        for frame in frames {
            expected.extend(codec::encode_frame(&frame).expect("encoded control"));
            writer.write_frame(&frame).await.expect("control");
        }
        assert_eq!(*sentinel.bytes.lock().expect("bytes"), expected);
        assert_eq!(sentinel.calls.load(Ordering::Relaxed), 2);
    }
}
