use super::*;
use crate::server::{
    ConnectionOptions, EngineError, NativeIngressPolicy, ServerConnection, connection_launch,
};
use crate::{
    Frame, Open, Performative, ProtocolHeader, read_frame, read_protocol_header, write_frame,
    write_protocol_header,
};
use std::{fmt, sync::atomic::AtomicUsize};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

pub(super) type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[derive(Default)]
pub(super) struct IoWitness {
    pub(super) wire: Mutex<Vec<u8>>,
    pub(super) dropped: AtomicUsize,
}

pub(super) struct MarkedIo {
    inner: DuplexStream,
    pub(super) witness: Arc<IoWitness>,
}

impl AsyncRead for MarkedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}
impl AsyncWrite for MarkedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, bytes);
        if let Poll::Ready(Ok(count)) = result {
            locked(&self.witness.wire).extend_from_slice(&bytes[..count]);
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
impl Drop for MarkedIo {
    fn drop(&mut self) {
        self.witness.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) fn transport() -> (MarkedIo, DuplexStream, Arc<IoWitness>) {
    let (inner, peer) = tokio::io::duplex(64 * 1024);
    let witness = Arc::new(IoWitness::default());
    (
        MarkedIo {
            inner,
            witness: witness.clone(),
        },
        peer,
        witness,
    )
}

pub(super) async fn hello(peer: &mut DuplexStream) -> io::Result<()> {
    write_protocol_header(peer, ProtocolHeader::AMQP).await?;
    let header = read_protocol_header(peer).await?;
    if header != ProtocolHeader::AMQP {
        return Err(io::Error::other("unexpected header"));
    }
    write_frame(
        peer,
        &Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(Open::new("peer"))),
            payload: Vec::new(),
        },
    )
    .await?;
    match read_frame(peer).await? {
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(_)),
            ..
        } => Ok(()),
        _ => Err(io::Error::other("unexpected negotiated Open")),
    }
}

pub(super) async fn negotiated() -> Result<
    (NegotiatedConnection<MarkedIo>, DuplexStream, Arc<IoWitness>),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let (io, mut peer, witness) = transport();
    // Both inline handshake futures finish; neither is a detached task.
    let (accepted, greeted) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            connection_launch::negotiate(
                io,
                "server",
                None,
                ConnectionOptions::default(),
                NativeIngressPolicy::Disabled
            ),
            hello(&mut peer),
        )
    })
    .await?;
    greeted?;
    Ok((accepted?, peer, witness))
}

pub(super) fn launch<A>(
    owner: &PairOwner<A>,
    negotiated: NegotiatedConnection<MarkedIo>,
) -> ServerConnection {
    owner
        .launch(negotiated)
        .unwrap_or_else(|_| panic!("fresh owner refused launch"))
}

pub(super) async fn observe_gate(gate: &Gate) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(Duration::from_secs(2), gate.entered()).await
}

pub(super) async fn observe_reader<A>(
    owner: &PairOwner<A>,
) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let created = matches!(
                *locked(&owner.reader.state),
                State::Pending(_) | State::Leased | State::Joined(_)
            );
            if created {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
}

pub(super) async fn observe_actor_ready<A>(
    owner: &PairOwner<A>,
) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let ready = match &*locked(&owner.actor.state) {
                State::Pending(handle) => handle.is_finished(),
                State::Joined(_) => true,
                _ => false,
            };
            if ready {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
}

pub(super) fn actor_abort<A>(owner: &PairOwner<A>) -> tokio::task::AbortHandle {
    let state = locked(&owner.actor.state);
    match &*state {
        State::Pending(handle) => handle.abort_handle(),
        _ => panic!("launcher must root the actor before returning"),
    }
}

pub(super) fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

pub(super) struct PrivateCause(pub(super) Arc<()>);
impl fmt::Debug for PrivateCause {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("cause must not be rendered");
    }
}
impl fmt::Display for PrivateCause {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("cause must not be rendered");
    }
}
impl std::error::Error for PrivateCause {}

pub(super) struct FailedIo {
    pub(super) marker: Arc<()>,
}
impl AsyncRead for FailedIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::ConnectionReset,
            PrivateCause(self.marker.clone()),
        )))
    }
}
impl AsyncWrite for FailedIo {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Pending
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

pub(super) fn same_cause(error: EngineError, marker: &Arc<()>) -> bool {
    match error {
        EngineError::Io(error) if error.kind() == io::ErrorKind::ConnectionReset => error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<PrivateCause>())
            .is_some_and(|cause| Arc::ptr_eq(&cause.0, marker)),
        _ => false,
    }
}
