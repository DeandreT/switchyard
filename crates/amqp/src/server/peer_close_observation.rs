use std::{
    fmt,
    future::{Future, poll_fn},
    io,
    pin::pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU8, Ordering},
    },
    task::Poll,
};

use tokio::task::Id;

use super::NativeConnectionIdentity;
use crate::Close;

/// The existing native site that actually requested an original token's abort.
/// This is an observation, not a claim that the request caused its result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerConnectionAbortSource {
    ActorReaderShutdown,
    OwnerFinish,
}

impl ServerConnectionAbortSource {
    pub(super) fn bit(self) -> u8 {
        match self {
            Self::ActorReaderShutdown => 1,
            Self::OwnerFinish => 2,
        }
    }
}

/// Data from one original installed task token; contains no task authority.
pub struct ServerConnectionTaskObservation {
    id: Id,
    abort_sources: u8,
}

impl ServerConnectionTaskObservation {
    pub(super) fn new(id: Id, abort_sources: u8) -> Self {
        Self { id, abort_sources }
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn abort_requested(&self) -> bool {
        self.abort_sources != 0
    }

    pub fn requested_by(&self, source: ServerConnectionAbortSource) -> bool {
        self.abort_sources & source.bit() != 0
    }
}

/// Whether the original peer-Close response had an observed original Ready result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerPeerCloseReplyState {
    NotRequired,
    Pending,
    Ready,
    AbandonedBeforeReady,
}

/// Original decoded peer frame and response result, never a close certificate.
pub struct ServerPeerCloseObservation {
    connection: NativeConnectionIdentity,
    close: Close,
    channel: u16,
    payload: Vec<u8>,
    locally_closing: bool,
    state: AtomicU8,
    result: OnceLock<io::Result<()>>,
}

impl ServerPeerCloseObservation {
    pub fn connection_identity(&self) -> &NativeConnectionIdentity {
        &self.connection
    }

    pub fn close(&self) -> &Close {
        &self.close
    }

    pub fn channel(&self) -> u16 {
        self.channel
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn locally_closing(&self) -> bool {
        self.locally_closing
    }

    pub fn reply_state(&self) -> ServerPeerCloseReplyState {
        match self.state.load(Ordering::Acquire) {
            0 => ServerPeerCloseReplyState::NotRequired,
            1 => ServerPeerCloseReplyState::Pending,
            2 => ServerPeerCloseReplyState::Ready,
            3 => ServerPeerCloseReplyState::AbandonedBeforeReady,
            _ => unreachable!("private reply state"),
        }
    }

    pub fn reply_result(&self) -> Option<&io::Result<()>> {
        self.result.get()
    }
}

pub(super) struct PeerCloseCell {
    received: OnceLock<ServerPeerCloseObservation>,
}

impl PeerCloseCell {
    pub(super) fn new() -> Self {
        Self {
            received: OnceLock::new(),
        }
    }

    pub(super) fn receive(
        &self,
        connection: &NativeConnectionIdentity,
        close: Close,
        channel: u16,
        payload: Vec<u8>,
        locally_closing: bool,
    ) -> &ServerPeerCloseObservation {
        let observation = ServerPeerCloseObservation {
            connection: connection.clone(),
            close,
            channel,
            payload,
            locally_closing,
            state: AtomicU8::new(if locally_closing { 0 } else { 1 }),
            result: OnceLock::new(),
        };
        assert!(
            self.received.set(observation).is_ok(),
            "one original peer Close"
        );
        self.received.get().expect("installed original peer Close")
    }

    pub(super) fn received(&self) -> Option<&ServerPeerCloseObservation> {
        self.received.get()
    }
}

/// Bounded same-connection observations retained beside the original raw joins.
/// These fields explain only the observed frame/write and actual abort requests.
pub struct ServerConnectionObservations {
    peer_close: Arc<PeerCloseCell>,
    actor: Option<ServerConnectionTaskObservation>,
    reader: Option<ServerConnectionTaskObservation>,
}

impl ServerConnectionObservations {
    pub(super) fn new(
        peer_close: Arc<PeerCloseCell>,
        actor: Option<ServerConnectionTaskObservation>,
        reader: Option<ServerConnectionTaskObservation>,
    ) -> Self {
        Self {
            peer_close,
            actor,
            reader,
        }
    }

    pub fn peer_close(&self) -> Option<&ServerPeerCloseObservation> {
        self.peer_close.received()
    }

    pub fn actor(&self) -> Option<&ServerConnectionTaskObservation> {
        self.actor.as_ref()
    }

    pub fn reader(&self) -> Option<&ServerConnectionTaskObservation> {
        self.reader.as_ref()
    }
}

pub(super) struct ReplyLoan<'a> {
    observation: &'a ServerPeerCloseObservation,
    ready: bool,
}

impl<'a> ReplyLoan<'a> {
    pub(super) fn new(observation: &'a ServerPeerCloseObservation) -> Self {
        Self {
            observation,
            ready: observation.locally_closing,
        }
    }
}

impl Drop for ReplyLoan<'_> {
    fn drop(&mut self) {
        if !self.ready {
            self.observation.state.store(3, Ordering::Release);
        }
    }
}

pub(super) async fn observe_reply<F>(mut loan: ReplyLoan<'_>, future: F) -> bool
where
    F: Future<Output = io::Result<()>>,
{
    let observation = loan.observation;
    let mut future = pin!(future);
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Pending => Poll::Pending,
        Poll::Ready(result) => {
            let failed = result.is_err();
            assert!(
                observation.result.set(result).is_ok(),
                "one original reply Ready"
            );
            observation.state.store(2, Ordering::Release);
            loan.ready = true;
            Poll::Ready(failed)
        }
    })
    .await
}

macro_rules! opaque_debug {
    ($name:ident) => {
        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .finish_non_exhaustive()
            }
        }
    };
}

opaque_debug!(ServerConnectionTaskObservation);
opaque_debug!(ServerPeerCloseObservation);
opaque_debug!(ServerConnectionObservations);
