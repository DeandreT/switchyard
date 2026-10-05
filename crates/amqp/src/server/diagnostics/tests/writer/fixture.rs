use super::*;
use crate::{Begin, Transfer};
use std::{
    fmt,
    sync::atomic::{AtomicBool, AtomicUsize},
    task::{Context, Waker},
};
use tokio::io::AsyncWrite;

#[derive(Clone, Copy)]
pub(super) enum Mode {
    Healthy,
    PendingWrite,
    PendingFlush,
    ErrorWrite(io::ErrorKind),
    ErrorFlush(io::ErrorKind),
}

#[derive(Default)]
pub(super) struct Control {
    bytes: Mutex<Vec<u8>>,
    writes: AtomicUsize,
    flushes: AtomicUsize,
    partial: AtomicBool,
    pub(super) marker: Arc<()>,
    pub(super) rendering: Arc<AtomicUsize>,
}

#[derive(Debug, Default, Eq, PartialEq)]
pub(super) struct WireSnapshot {
    pub(super) bytes: Vec<u8>,
    pub(super) writes: usize,
    pub(super) flushes: usize,
}

impl Control {
    pub(super) fn snapshot(&self) -> WireSnapshot {
        WireSnapshot {
            bytes: self.bytes.lock().unwrap().clone(),
            writes: self.writes.load(Ordering::Relaxed),
            flushes: self.flushes.load(Ordering::Relaxed),
        }
    }
    fn error(&self, kind: io::ErrorKind) -> io::Error {
        io::Error::new(
            kind,
            PrivateIoError {
                marker: self.marker.clone(),
                rendering: self.rendering.clone(),
            },
        )
    }
}

pub(super) struct PrivateIoError {
    pub(super) marker: Arc<()>,
    rendering: Arc<AtomicUsize>,
}
impl fmt::Display for PrivateIoError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.rendering.fetch_add(1, Ordering::Relaxed);
        panic!("PRIVATE_IO_DISPLAY")
    }
}
impl fmt::Debug for PrivateIoError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.rendering.fetch_add(1, Ordering::Relaxed);
        panic!("PRIVATE_IO_DEBUG")
    }
}
impl std::error::Error for PrivateIoError {}

pub(super) fn assert_native_error(error: &io::Error, kind: io::ErrorKind, control: &Control) {
    assert_eq!(error.kind(), kind);
    let cause = error
        .get_ref()
        .and_then(|cause| cause.downcast_ref::<PrivateIoError>())
        .expect("same typed cause");
    assert!(Arc::ptr_eq(&cause.marker, &control.marker));
    assert_eq!(control.rendering.load(Ordering::Relaxed), 0);
}

pub(super) struct ControlledWriter {
    control: Arc<Control>,
    mode: Mode,
}
pub(super) type WriterFixture = (FrameWriter<ControlledWriter>, Arc<Control>, Activity);
impl AsyncWrite for ControlledWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.control.writes.fetch_add(1, Ordering::Relaxed);
        if let Mode::ErrorWrite(kind) = self.mode {
            return Poll::Ready(Err(self.control.error(kind)));
        }
        if matches!(self.mode, Mode::PendingWrite) {
            if self.control.partial.swap(true, Ordering::Relaxed) {
                return Poll::Pending;
            }
            self.control.bytes.lock().unwrap().push(bytes[0]);
            return Poll::Ready(Ok(1));
        }
        self.control.bytes.lock().unwrap().extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.control.flushes.fetch_add(1, Ordering::Relaxed);
        match self.mode {
            Mode::PendingFlush => Poll::Pending,
            Mode::ErrorFlush(kind) => Poll::Ready(Err(self.control.error(kind))),
            _ => Poll::Ready(Ok(())),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

pub(super) fn writer(
    mode: Mode,
    recorder: Option<&ServerDiagnosticRecorder>,
    parent: Option<&DiagnosticScope>,
) -> io::Result<WriterFixture> {
    writer_with_maximum(mode, 512, recorder, parent)
}

pub(super) fn writer_with_maximum(
    mode: Mode,
    maximum: u32,
    recorder: Option<&ServerDiagnosticRecorder>,
    parent: Option<&DiagnosticScope>,
) -> io::Result<WriterFixture> {
    let control = Arc::new(Control::default());
    let io = ControlledWriter {
        control: control.clone(),
        mode,
    };
    let mut writer = match recorder {
        Some(recorder) => {
            FrameWriter::new_with_diagnostics(io, maximum, recorder.clone(), parent.cloned())?
        }
        None => FrameWriter::new(io, maximum)?,
    };
    let activity = Activity::new();
    writer.configure_activity(ConnectionOptions::default(), 0, activity.clone());
    Ok((writer, control, activity))
}

pub(super) struct LateWriter {
    pub(super) control: Arc<Control>,
    pub(super) flush: Pin<Box<tokio::time::Sleep>>,
}
impl AsyncWrite for LateWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.control.writes.fetch_add(1, Ordering::Relaxed);
        self.control.bytes.lock().unwrap().extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.control.flushes.fetch_add(1, Ordering::Relaxed);
        self.flush.as_mut().poll(cx).map(|_| Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

pub(super) fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

pub(super) fn heartbeat() -> Frame {
    Frame::Amqp {
        channel: 0,
        performative: None,
        payload: Vec::new(),
    }
}
pub(super) fn control_frame() -> Frame {
    Frame::Amqp {
        channel: 0,
        performative: Some(Performative::Begin(Begin::default())),
        payload: Vec::new(),
    }
}
pub(super) fn transfer(payload: Vec<u8>) -> Frame {
    Frame::Amqp {
        channel: 3,
        performative: Some(Performative::Transfer(Transfer {
            handle: 19,
            delivery_id: Some(27),
            delivery_tag: Some(b"PRIVATE_TAG".to_vec().into()),
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
pub(super) fn sized_transfer(size: usize) -> Frame {
    let overhead = codec::encode_frame(&transfer(Vec::new())).unwrap().len();
    transfer(vec![7; size.checked_sub(overhead).unwrap()])
}
