//! Concrete TCP listener custody. Borrowers never own accepted tasks.

use std::{
    any::Any,
    error::Error,
    fmt,
    future::Future,
    io,
    net::SocketAddr,
    panic::{AssertUnwindSafe, resume_unwind},
    sync::Arc,
};

use amqp::ServerConnection;
use futures_util::FutureExt;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{Id, JoinError},
};
use tracing::debug;

use super::{
    AmqpListener, ConnectionAuthorization, SharedAccessSaslAcceptor,
    connection_custody::{ConnectionCustody, ConnectionTaskExit, wait_for_retirement},
    serve_open_connection,
};
use crate::Broker;

mod connection_family;
#[cfg(test)]
mod tcp_tests;
#[cfg(test)]
mod test_support;

use connection_family::{AdmittedConnectionTask, ConnectionFamily, FamilyFailures, PreparedTask};

type PanicPayload = Box<dyn Any + Send>;
type Primary = std::thread::Result<io::Result<()>>;

enum PumpExit {
    Retired,
    Accept(io::Error),
}

/// Requests retirement only; this capability does not certify completed joins.
#[derive(Clone)]
pub struct AmqpListenerRetirement {
    requested: watch::Sender<bool>,
}

impl AmqpListenerRetirement {
    fn new() -> Self {
        let (requested, _) = watch::channel(false);
        Self { requested }
    }

    /// Requests that the owner stop admission and retire accepted connections.
    pub fn request(&self) {
        self.requested.send_replace(true);
    }

    fn is_requested(&self) -> bool {
        *self.requested.borrow()
    }

    fn observer(&self) -> impl Future<Output = ()> + Send + 'static + use<> {
        wait_for_retirement(self.requested.subscribe())
    }
}

/// The selected original listener failure, with its origin preserved.
pub enum AmqpListenerFailure {
    Accept(io::Error),
    Join(JoinError),
    Returned(Box<dyn Error + Send + Sync>),
    MissingExit(Id),
}

impl fmt::Debug for AmqpListenerFailure {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accept(error) => output.debug_tuple("Accept").field(error).finish(),
            Self::Join(error) => output.debug_tuple("Join").field(error).finish(),
            Self::Returned(error) => output.debug_tuple("Returned").field(error).finish(),
            Self::MissingExit(id) => output.debug_tuple("MissingExit").field(id).finish(),
        }
    }
}

impl fmt::Display for AmqpListenerFailure {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accept(error) => write!(output, "listener accept failed: {error}"),
            Self::Join(error) => write!(output, "original connection task failed: {error}"),
            Self::Returned(error) => write!(output, "connection ended: {error}"),
            Self::MissingExit(id) => {
                write!(
                    output,
                    "original connection task {id} completed without its typed result"
                )
            }
        }
    }
}

impl Error for AmqpListenerFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Accept(error) => Some(error),
            Self::Join(error) => Some(error),
            Self::Returned(error) => Some(&**error),
            Self::MissingExit(_) => None,
        }
    }
}

/// Extracted once, only after the retained listener and its task family drain.
pub enum AmqpListenerExit {
    Complete(Result<(), AmqpListenerFailure>),
    ReportOnly(PanicPayload),
}

struct PreparedConnection<B> {
    bridge: PreparedTask,
    config: AmqpListener<B>,
}

/// Retains one TCP listener and the original accepted connection task family.
pub struct AmqpListenerService<B> {
    config: AmqpListener<B>,
    listener: Option<TcpListener>,
    retirement: AmqpListenerRetirement,
    accepted: Option<(TcpStream, SocketAddr)>,
    prepared: Option<PreparedConnection<B>>,
    receipt: Option<AdmittedConnectionTask>,
    family: ConnectionFamily,
    primary: Option<Primary>,
    finished: bool,
    extracted: bool,
}

impl<B: Broker> AmqpListenerService<B> {
    pub(super) fn new(config: AmqpListener<B>, listener: TcpListener) -> Self {
        Self {
            config,
            listener: Some(listener),
            retirement: AmqpListenerRetirement::new(),
            accepted: None,
            prepared: None,
            receipt: None,
            family: ConnectionFamily::new(),
            primary: None,
            finished: false,
            extracted: false,
        }
    }

    /// Returns a request-only handle for this original listener owner.
    pub fn retirement_handle(&self) -> AmqpListenerRetirement {
        self.retirement.clone()
    }

    /// Records terminal admission without extracting errors or draining tasks.
    /// Call `finish` before `take_exit`; cancellation preserves this owner.
    pub async fn serve(&mut self) {
        if self.primary.is_some() || self.retirement.is_requested() {
            return;
        }
        let exit = AssertUnwindSafe(self.pump()).catch_unwind().await;
        assert!(self.primary.is_none(), "one original listener primary");
        self.primary = match exit {
            Ok(PumpExit::Retired) => None,
            Ok(PumpExit::Accept(error)) => Some(Ok(Err(error))),
            Err(payload) => Some(Err(payload)),
        };
    }

    async fn pump(&mut self) -> PumpExit {
        let retired = self.retirement.observer();
        tokio::pin!(retired);
        loop {
            if self.retirement.is_requested() {
                return PumpExit::Retired;
            }
            if self.receipt.is_some() {
                self.adopt_receipt();
                #[cfg(test)]
                self.checkpoint(test_support::Point::Adopted).await;
                continue;
            }
            if self.accepted.is_some() {
                if self.prepared.is_none() {
                    let config = AmqpListener {
                        broker: self.config.broker.clone(),
                        namespace: self.config.namespace.clone(),
                        container_id: self.config.container_id.clone(),
                        tls_acceptor: self.config.tls_acceptor.clone(),
                        shared_access_authentication: self
                            .config
                            .shared_access_authentication
                            .clone(),
                    };
                    self.prepared = Some(PreparedConnection {
                        bridge: PreparedTask::new(),
                        config,
                    });
                }
                #[cfg(test)]
                self.checkpoint(test_support::Point::Prepared).await;
                if self.retirement.is_requested() {
                    return PumpExit::Retired;
                }
                let (stream, peer) = match self.accepted.take() {
                    Some(packet) => packet,
                    None => unreachable!("prepared accepted socket"),
                };
                let PreparedConnection { bridge, config } = match self.prepared.take() {
                    Some(prepared) => prepared,
                    None => unreachable!("prepared original connection context"),
                };
                let task_retirement = bridge.retirement();
                self.receipt =
                    Some(bridge.spawn(peer, serve_tcp_task(stream, config, task_retirement)));
                self.finished = false;
                #[cfg(test)]
                self.checkpoint(test_support::Point::ReceiptCached).await;
                continue;
            }
            tokio::select! {
                biased;
                () = &mut retired => return PumpExit::Retired,
                true = self.family.next(), if !self.family.is_empty() => {
                    #[cfg(test)]
                    self.checkpoint(test_support::Point::Reaped).await;
                },
                packet = async {
                    match self.listener.as_ref() {
                        Some(listener) => listener.accept().await,
                        None => unreachable!("active listener is retained"),
                    }
                } => {
                    self.accepted = Some(match packet {
                        Ok(packet) => packet,
                        Err(error) => return PumpExit::Accept(error),
                    });
                    #[cfg(test)]
                    self.checkpoint(test_support::Point::Accepted).await;
                    let peer = match self.accepted.as_ref() {
                        Some((_, peer)) => peer,
                        None => unreachable!("accepted socket cached before diagnostics"),
                    };
                    debug!(%peer, "connection accepted");
                },
            }
        }
    }

    fn adopt_receipt(&mut self) {
        if let Some(receipt) = self.receipt.take() {
            self.family.adopt(receipt);
            self.finished = false;
        }
    }

    /// Retires admission and joins originals; a cancelled borrower may retry.
    pub async fn finish(&mut self) {
        self.retirement.request();
        self.listener = None;
        self.accepted = None;
        self.prepared = None;
        if let Some(receipt) = self.receipt.as_ref() {
            receipt.retirement.request();
        }
        self.adopt_receipt();
        self.family.retire();
        self.family.finish().await;
        self.finished = true;
    }

    /// Moves one terminal outcome after drain. Genuine primary panics resume.
    /// A second extraction returns `None`, including after a resumed panic.
    pub fn take_exit(&mut self) -> Option<AmqpListenerExit> {
        assert!(
            self.finished && self.listener.is_none(),
            "listener drains before extraction"
        );
        assert!(
            self.accepted.is_none() && self.prepared.is_none() && self.receipt.is_none(),
            "accepted socket and original task receipt drain before extraction"
        );
        self.family.assert_drained();
        if self.extracted {
            return None;
        }
        self.extracted = true;
        Some(resolve_terminal(
            self.primary.take(),
            self.family.take_failures(),
        ))
    }

    #[cfg(test)]
    async fn checkpoint(&self, point: test_support::Point) {
        let facts = test_support::Facts {
            point,
            accepted: self.accepted.is_some(),
            receipt: self.receipt.as_ref().map(|receipt| receipt.id),
            pending: self.family.pending_ids(),
            reaped: self.family.reaped_id(),
            finished: self.family.finished_ids(),
        };
        test_support::checkpoint(facts).await;
    }
}

fn resolve_terminal(primary: Option<Primary>, failures: FamilyFailures) -> AmqpListenerExit {
    match primary {
        Some(Err(payload)) => resume_unwind(payload),
        Some(Ok(Err(error))) => {
            return AmqpListenerExit::Complete(Err(AmqpListenerFailure::Accept(error)));
        }
        Some(Ok(Ok(()))) | None => {}
    }
    if let Some(error) = failures.join {
        return AmqpListenerExit::Complete(Err(AmqpListenerFailure::Join(error)));
    }
    if let Some(error) = failures.returned {
        return AmqpListenerExit::Complete(Err(AmqpListenerFailure::Returned(error)));
    }
    if let Some(id) = failures.missing {
        return AmqpListenerExit::Complete(Err(AmqpListenerFailure::MissingExit(id)));
    }
    if let Some(payload) = failures.report.or(failures.diagnostic) {
        return AmqpListenerExit::ReportOnly(payload);
    }
    AmqpListenerExit::Complete(Ok(()))
}

pub(super) fn legacy_result(exit: AmqpListenerExit) -> io::Result<()> {
    match exit {
        AmqpListenerExit::Complete(Err(AmqpListenerFailure::Accept(error))) => Err(error),
        AmqpListenerExit::Complete(_) | AmqpListenerExit::ReportOnly(_) => Ok(()),
    }
}

async fn serve_tcp_task<B: Broker>(
    stream: TcpStream,
    config: AmqpListener<B>,
    retirement: AmqpListenerRetirement,
) -> ConnectionTaskExit {
    match config.tls_acceptor.clone() {
        Some(acceptor) => {
            match until_retired(&retirement, acceptor.accept(stream), Stage::Tls).await {
                Some(Ok(stream)) => serve_retained_connection(stream, config, retirement).await,
                Some(Err(error)) => ConnectionTaskExit::Complete(Err(error.into())),
                None => ConnectionTaskExit::Complete(Ok(())),
            }
        }
        None => serve_retained_connection(stream, config, retirement).await,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    Tls,
    Protocol,
}

async fn until_retired<F: Future>(
    retirement: &AmqpListenerRetirement,
    future: F,
    stage: Stage,
) -> Option<F::Output> {
    #[cfg(test)]
    let future = test_support::ObservedPending::new(future, stage);
    #[cfg(not(test))]
    let _ = stage;
    tokio::select! {
        biased;
        () = retirement.observer() => None,
        result = future => Some(result),
    }
}

async fn serve_retained_connection<Io, B>(
    stream: Io,
    config: AmqpListener<B>,
    retirement: AmqpListenerRetirement,
) -> ConnectionTaskExit
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    B: Broker,
{
    let sasl = config
        .shared_access_authentication
        .as_ref()
        .map(SharedAccessSaslAcceptor::new);
    let authenticator = sasl
        .as_ref()
        .map(|sasl| Arc::new(sasl.clone()) as Arc<dyn amqp::SaslAuthenticator>);
    let accepted = until_retired(
        &retirement,
        ServerConnection::accept(stream, config.container_id, authenticator),
        Stage::Protocol,
    )
    .await;
    let connection = match accepted {
        Some(Ok(connection)) => connection,
        Some(Err(error)) => return ConnectionTaskExit::Complete(Err(error.into())),
        None => return ConnectionTaskExit::Complete(Ok(())),
    };
    let mut custody = ConnectionCustody::new(connection);
    let primary = AssertUnwindSafe(async {
        #[cfg(test)]
        test_support::native_accepted();
        let authorization = config.shared_access_authentication.map(|config| {
            let grant = sasl.as_ref().and_then(SharedAccessSaslAcceptor::grant);
            ConnectionAuthorization::new(config, grant)
        });
        let request = custody.request_handle();
        tokio::select! {
            biased;
            () = retirement.observer() => {
                request.request();
                Ok(())
            },
            primary = serve_open_connection(&mut custody, config.namespace, config.broker, authorization) => primary,
        }
    }).catch_unwind().await;
    custody.record_primary(primary);
    custody.finish().await;
    custody.finish_exit()
}
