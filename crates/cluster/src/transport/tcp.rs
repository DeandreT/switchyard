use std::{collections::HashMap, future::Future, sync::Arc};

use raft::eraftpb::Message;
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, watch},
    task::{Id, JoinSet},
    time::{Instant, timeout_at},
};

use crate::consensus::ConsensusIdentity;

pub use super::budget::TransportReport;
use super::{
    FRAME_HEADER_BYTES, FrameCodec, TransportError, TransportLimits,
    budget::{LeaseRef, Shared, TransportStatus},
    frame::allocation_charge,
};

/// Trusted dispatch future. It owns one validated core message. It must retain
/// and await its original work, not spawn detached persistence or retain
/// unaccounted clones after returning. Transport never cancels begun dispatch on
/// peer disconnect, IO deadline, retirement or canceled serve/finish borrowers.
pub trait PeerHandler: Send + Sync + 'static {
    fn dispatch(&self, message: Message)
    -> impl Future<Output = Result<(), TransportError>> + Send;
}

impl<F, R> PeerHandler for F
where
    F: Fn(Message) -> R + Send + Sync + 'static,
    R: Future<Output = Result<(), TransportError>> + Send,
{
    fn dispatch(
        &self,
        message: Message,
    ) -> impl Future<Output = Result<(), TransportError>> + Send {
        self(message)
    }
}

struct Outbound {
    frame: Vec<u8>,
    peer: u64,
    deadline: Instant,
    lease: LeaseRef,
    reply: oneshot::Sender<Result<(), TransportError>>,
}
struct Original {
    lease: LeaseRef,
    reply: Option<oneshot::Sender<Result<(), TransportError>>>,
}

/// Local write-and-join completion ONLY, not a peer response or durable ack.
pub struct SendReceipt {
    reply: oneshot::Receiver<Result<(), TransportError>>,
    consumed: bool,
}
impl SendReceipt {
    /// Canceling this borrower preserves the accepted original and its credits.
    pub async fn wait(&mut self) -> Result<(), TransportError> {
        if self.consumed {
            return Err(TransportError::Malformed("receipt consumed"));
        }
        let result = (&mut self.reply).await;
        self.consumed = true;
        result.map_err(|_| TransportError::OwnerLost)?
    }
}

#[derive(Clone)]
pub struct PeerSender {
    codec: FrameCodec,
    shared: Arc<Shared>,
    queue: mpsc::Sender<Outbound>,
}
impl PeerSender {
    /// Nonwaiting admission: caller keeps retry policy. No hidden retry,
    /// connection cache, background reader or unbounded pending sender queue.
    pub fn try_send(&self, message: Message) -> Result<SendReceipt, TransportError> {
        let deadline = Instant::now() + self.shared.limits.io_timeout;
        let body_bytes = self.codec.outbound_size(&message)?;
        let charge = allocation_charge(
            FRAME_HEADER_BYTES + body_bytes,
            self.shared.limits.max_entries,
        )?;
        let lease = self.shared.reserve(Some(message.to), charge)?;
        let frame = self.codec.encode(&message)?;
        let (reply, receiver) = oneshot::channel();
        self.shared
            .if_live(|| {
                self.queue.try_send(Outbound {
                    frame,
                    peer: message.to,
                    deadline,
                    lease,
                    reply,
                })
            })?
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => TransportError::Busy("outbound queue"),
                mpsc::error::TrySendError::Closed(_) => TransportError::Retired,
            })?;
        Ok(SendReceipt {
            reply: receiver,
            consumed: false,
        })
    }
    pub fn retire(&self) {
        self.shared.retire();
    }
    pub fn status(&self) -> TransportStatus {
        self.shared.status()
    }
    pub fn subscribe(&self) -> watch::Receiver<TransportStatus> {
        self.shared.subscribe()
    }
}

/// Borrowed serve/finish preserve one owner across cancellation. Tasks and their
/// leases remain registered until actual join, including panics. Keep this owner
/// alive until finish returns: dropping/aborting the owner is NOT graceful drain.
/// There is deliberately no detached supervisor or Drop-time success claim.
///
/// ```compile_fail
/// use cluster::transport::{PeerHandler, PeerTransport};
/// fn duplicate<H: PeerHandler>(owner: PeerTransport<H>) {
///     let alias = owner.clone();
/// }
/// ```
#[must_use = "retain the transport and finish its original tasks"]
pub struct PeerTransport<H> {
    codec: FrameCodec,
    shared: Arc<Shared>,
    listener: Option<TcpListener>,
    queue: mpsc::Receiver<Outbound>,
    queue_closed: bool,
    handler: Arc<H>,
    tasks: JoinSet<Result<(), TransportError>>,
    originals: HashMap<Id, Original>,
}

impl<H: PeerHandler> PeerTransport<H> {
    pub async fn bind(
        identity: ConsensusIdentity,
        local: u64,
        limits: TransportLimits,
        handler: H,
    ) -> Result<(Self, PeerSender), TransportError> {
        let codec = FrameCodec::new(identity.clone(), local, limits.clone())?;
        let address = identity
            .voters()
            .iter()
            .find(|v| v.id == local)
            .ok_or(TransportError::Identity)?
            .endpoint;
        let listener = TcpListener::bind(address).await?;
        let shared = Shared::new(identity, limits.clone());
        let (sender, queue) = mpsc::channel(limits.max_operations);
        let handle = PeerSender {
            codec: codec.clone(),
            shared: shared.clone(),
            queue: sender,
        };
        Ok((
            Self {
                codec,
                shared,
                listener: Some(listener),
                queue,
                queue_closed: false,
                handler: Arc::new(handler),
                tasks: JoinSet::new(),
                originals: HashMap::new(),
            },
            handle,
        ))
    }

    pub fn status(&self) -> TransportStatus {
        self.shared.status()
    }
    pub fn retire(&self) {
        self.shared.retire();
    }
    pub async fn finish(&mut self) -> TransportReport {
        self.retire();
        self.serve().await
    }

    pub async fn serve(&mut self) -> TransportReport {
        let mut stop = self.shared.stop_receiver();
        let mut updates = self.shared.subscribe();
        loop {
            if self.shared.status().retired {
                self.listener.take();
                self.queue.close();
                while let Ok(job) = self.queue.try_recv() {
                    let _ = job.reply.send(Err(TransportError::Retired));
                    self.shared.retired_queued();
                }
                if self.tasks.is_empty() {
                    return self.shared.status().report;
                }
                self.reap().await;
                continue;
            }
            updates.borrow_and_update();
            let can_accept = self.shared.can_accept();
            // Default fair selection prevents a hot outbound producer/reaper
            // from permanently outranking inbound accept. Every branch handles
            // at most one original before reconsidering retirement and quotas.
            tokio::select! {
                result = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => {
                    if let Some(result) = result { self.joined(result); }
                }
                _ = stopped(&mut stop) => {},
                job = self.queue.recv(), if !self.queue_closed => {
                    if let Some(job) = job { self.send(job); } else { self.queue_closed = true; }
                }
                accepted = self.listener.as_ref().expect("live listener").accept(), if can_accept => {
                    match accepted {
                        Ok((stream, _)) => self.receive(stream),
                        Err(_) => { self.shared.listener_error(); self.retire(); }
                    }
                }
                _ = updates.changed() => {},
            }
        }
    }

    fn receive(&mut self, stream: TcpStream) {
        let deadline = Instant::now() + self.shared.limits.io_timeout;
        let Ok(lease) = self.shared.reserve(None, FRAME_HEADER_BYTES) else {
            return;
        };
        let retained = lease.clone();
        let codec = self.codec.clone();
        let handler = self.handler.clone();
        let stop = self.shared.stop_receiver();
        let id = self
            .tasks
            .spawn(async move { receive(stream, codec, handler, lease, stop, deadline).await })
            .id();
        self.originals.insert(
            id,
            Original {
                lease: retained,
                reply: None,
            },
        );
    }

    fn send(&mut self, job: Outbound) {
        let address = self
            .codec
            .identity()
            .voters()
            .iter()
            .find(|v| v.id == job.peer)
            .expect("validated peer")
            .endpoint;
        let retained = job.lease.clone();
        let shared = self.shared.clone();
        let stop = self.shared.stop_receiver();
        let id = self
            .tasks
            .spawn(async move {
                let _lease = job.lease;
                send(address, job.frame, shared, stop, job.deadline).await
            })
            .id();
        self.originals.insert(
            id,
            Original {
                lease: retained,
                reply: Some(job.reply),
            },
        );
    }

    async fn reap(&mut self) {
        if let Some(result) = self.tasks.join_next_with_id().await {
            self.joined(result);
        }
    }
    fn joined(&mut self, result: Result<(Id, Result<(), TransportError>), tokio::task::JoinError>) {
        let (id, result) = match result {
            Ok((id, result)) => (id, result),
            Err(error) => (
                error.id(),
                Err(if error.is_panic() {
                    TransportError::TaskPanic
                } else {
                    TransportError::TaskCancelled
                }),
            ),
        };
        let Original { lease, reply } = self.originals.remove(&id).expect("retained original task");
        let dispatched = lease.lock().expect("lease lock").dispatching;
        self.shared.joined(dispatched, reply.is_some(), &result);
        // The final lease is dropped only here, AFTER original join, even if
        // its task panicked or its caller stopped observing the result.
        drop(lease);
        // Observing completion must also permit immediate quota reuse.
        if let Some(reply) = reply {
            let _ = reply.send(result);
        }
    }
}

impl<H> Drop for PeerTransport<H> {
    fn drop(&mut self) {
        self.shared.retire();
    }
}

async fn stopped(stop: &mut watch::Receiver<bool>) {
    let _ = stop.wait_for(|v| *v).await;
}

async fn io_until<T>(
    stop: &mut watch::Receiver<bool>,
    deadline: Instant,
    io: impl Future<Output = std::io::Result<T>>,
) -> Result<T, TransportError> {
    if *stop.borrow() {
        return Err(TransportError::Retired);
    }
    if Instant::now() >= deadline {
        return Err(TransportError::Deadline);
    }
    tokio::select! {
        biased;
        _ = stopped(stop) => Err(TransportError::Retired),
        result = timeout_at(deadline, io) => result.map_err(|_| TransportError::Deadline)?.map_err(TransportError::Io),
    }
}

async fn receive<H: PeerHandler>(
    mut stream: TcpStream,
    codec: FrameCodec,
    handler: Arc<H>,
    lease: LeaseRef,
    mut stop: watch::Receiver<bool>,
    deadline: Instant,
) -> Result<(), TransportError> {
    let mut header = [0; FRAME_HEADER_BYTES];
    io_until(&mut stop, deadline, stream.read_exact(&mut header)).await?;
    let header = codec.header(&header)?;
    let charge = allocation_charge(
        FRAME_HEADER_BYTES + header.body_bytes,
        codec.limits().max_entries,
    )?;
    lease
        .lock()
        .expect("lease lock")
        .identify(header.source, charge)?;
    let mut body = vec![0; header.body_bytes];
    io_until(&mut stop, deadline, stream.read_exact(&mut body)).await?;
    let message = codec.body(header, &body)?;
    drop(body);
    // One frame per full socket. Never split/detach a reader: dropping this
    // entire stream closes both directions even when the remote stays idle.
    drop(stream);
    if Instant::now() >= deadline {
        return Err(TransportError::Deadline);
    }
    lease.lock().expect("lease lock").begin_dispatch()?;
    handler.dispatch(message).await
}

async fn send(
    address: std::net::SocketAddr,
    frame: Vec<u8>,
    shared: Arc<Shared>,
    mut stop: watch::Receiver<bool>,
    deadline: Instant,
) -> Result<(), TransportError> {
    let stream = io_until(&mut stop, deadline, TcpStream::connect(address)).await?;
    io_until(&mut stop, deadline, stream.writable()).await?;
    let mut written = 0;
    let mut observed_block = false;
    while written < frame.len() {
        if *stop.borrow() {
            return Err(TransportError::Retired);
        }
        if Instant::now() >= deadline {
            return Err(TransportError::Deadline);
        }
        match stream.try_write(&frame[written..]) {
            Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into()),
            Ok(bytes) => written += bytes,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if written > 0 && !observed_block {
                    observed_block = true;
                    shared.blocked_write();
                }
                io_until(&mut stop, deadline, stream.writable()).await?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    // Local write completion is not peer dispatch/persistence confirmation.
    if Instant::now() >= deadline {
        return Err(TransportError::Deadline);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn shared_io_deadline_and_retirement_do_not_restart() {
        let (sender, mut receiver) = watch::channel(false);
        assert!(matches!(
            io_until(
                &mut receiver,
                Instant::now(),
                std::future::pending::<std::io::Result<()>>()
            )
            .await,
            Err(TransportError::Deadline)
        ));
        sender.send_replace(true);
        assert!(matches!(
            io_until(
                &mut receiver,
                Instant::now() + std::time::Duration::from_secs(10),
                std::future::pending::<std::io::Result<()>>()
            )
            .await,
            Err(TransportError::Retired)
        ));
    }
}
