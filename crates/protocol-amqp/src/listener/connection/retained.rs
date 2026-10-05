//! Explicit one-socket wrapper. Legacy listener/semaphore paths do not use it.

use amqp::{ScopedConnectionAcceptance, ServerConnectionAcceptor};
use tokio::net::TcpStream;

use super::*;
#[cfg(test)]
use crate::listener::retained_connection::Controls;
use crate::listener::retained_connection::{
    PrimaryPublisher, Publisher, RetainedConnectionOutcome as Outcome,
    RetainedConnectionStartCause as StartCause, RetainedConnectionStartError,
    RetainedConnectionStarter,
    ready::{self, Attempt, Primary},
};

#[cfg(test)]
mod socket_collector;

trait RetainedDriver<B: Broker>: Send + 'static {
    const ADMISSION: AdmissionMode;

    fn serve_retained_open<'a>(
        self,
        connection: &'a mut ServerConnection,
        namespace: domain::NamespaceName,
        broker: B,
        authorization: Option<Arc<ConnectionAuthorization>>,
    ) -> impl Future<Output = crate::listener::retained_connection::RetainedConnectionResult> + Send + 'a;
}

impl<B: Broker, D: ConnectionDriver<B>> RetainedDriver<B> for D {
    const ADMISSION: AdmissionMode = <D as ConnectionDriver<B>>::ADMISSION;

    fn serve_retained_open<'a>(
        self,
        connection: &'a mut ServerConnection,
        namespace: domain::NamespaceName,
        broker: B,
        authorization: Option<Arc<ConnectionAuthorization>>,
    ) -> impl Future<Output = crate::listener::retained_connection::RetainedConnectionResult> + Send + 'a
    {
        self.serve_open(connection, namespace, broker, authorization)
    }
}

impl<B: Broker> AmqpListener<B> {
    /// Starts ONE already accepted socket on the starter's captured live runtime.
    ///
    /// Caller owns socket acceptance and multi-connection admission; this does
    /// not apply max_connections across independent roots. Retain the separate
    /// owner and runtime, then drive borrowed finish on that runtime. Legacy
    /// serve/semaphore/default behavior is unchanged. No descendant/fixture
    /// cleanup, safe-reopen, SDK or physical termination deadline is implied.
    ///
    /// Setup failures return their original error plus listener/socket. Setup
    /// validates options, sets TCP_NODELAY and computes the same one absolute
    /// handshake deadline BEFORE a sealed-launch refusal. A successful call
    /// synchronously installs its original wrapper token before returning.
    pub fn start_retained_connection(
        self,
        stream: TcpStream,
        starter: RetainedConnectionStarter,
    ) -> Result<(), RetainedConnectionStartError<B>> {
        self.start_retained_with_driver(stream, starter, OrdinaryDriver)
    }

    fn start_retained_with_driver<D: RetainedDriver<B>>(
        self,
        stream: TcpStream,
        starter: RetainedConnectionStarter,
        driver: D,
    ) -> Result<(), RetainedConnectionStartError<B>> {
        let setup = self
            .connection_options
            .validate()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
            .and_then(|()| stream.set_nodelay(true))
            .and_then(|()| {
                tokio::time::Instant::now()
                    .checked_add(self.handshake_timeout)
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "the configured handshake timeout cannot be represented",
                        )
                    })
            });
        let deadline = match setup {
            Ok(deadline) => deadline,
            Err(error) => {
                return Err(RetainedConnectionStartError::new(
                    StartCause::Setup(error),
                    self,
                    stream,
                ));
            }
        };
        let Some((claim, acceptor, publisher)) = starter.claim() else {
            return Err(RetainedConnectionStartError::new(
                StartCause::Stopped,
                self,
                stream,
            ));
        };
        claim.spawn(run_wrapper(
            self, stream, deadline, driver, acceptor, publisher,
        ));
        Ok(())
    }
}

impl<B: NativeAtomicBroker> AmqpListener<B> {
    /// Explicit one-socket equivalent of the existing posting ingress policy.
    /// Root/runtime/admission obligations match start_retained_connection.
    pub fn start_retained_atomic_posting_ingress(
        self,
        stream: TcpStream,
        starter: RetainedConnectionStarter,
    ) -> Result<(), RetainedConnectionStartError<B>> {
        self.start_retained_with_driver(stream, starter, AtomicPostingDriver)
    }

    /// Explicit one-socket equivalent of the existing messaging ingress policy.
    /// This does not enable SDK scopes or change ordinary/posting defaults.
    pub fn start_retained_atomic_messaging_ingress(
        self,
        stream: TcpStream,
        starter: RetainedConnectionStarter,
    ) -> Result<(), RetainedConnectionStartError<B>> {
        self.start_retained_with_driver(stream, starter, AtomicMessagingDriver)
    }
}

async fn run_wrapper<B: Broker, D: RetainedDriver<B>>(
    listener: AmqpListener<B>,
    stream: TcpStream,
    deadline: tokio::time::Instant,
    driver: D,
    acceptor: ServerConnectionAcceptor,
    publisher: Publisher,
) {
    #[cfg(test)]
    publisher.controls.first_poll().await;
    if deadline <= tokio::time::Instant::now() {
        publisher.primary.publish(Outcome::SkippedExpiredDeadline);
        return;
    }
    let settings = ConnectionSettings {
        container_id: listener.container_id,
        namespace: listener.namespace,
        broker: listener.broker,
        shared_access_authentication: listener.shared_access_authentication,
        connection_options: listener.connection_options,
        deadline,
    };
    match listener.tls_acceptor {
        Some(tls) => {
            let established = ready::observe(
                tokio::time::timeout_at(deadline, tls.accept(stream)),
                move |result| match result {
                    Ok(Ok(stream)) => Some((stream, publisher)),
                    Ok(Err(error)) => {
                        publisher
                            .primary
                            .publish(Outcome::Finished(Err(error.into())));
                        None
                    }
                    Err(_) => {
                        publisher
                            .primary
                            .publish(Outcome::Finished(Err(handshake_timeout_error())));
                        None
                    }
                },
                #[cfg(test)]
                || {},
            )
            .await;
            if let Some((stream, publisher)) = established {
                serve_owned_transport(
                    stream,
                    listener.websocket,
                    settings,
                    driver,
                    acceptor,
                    publisher,
                )
                .await;
            }
        }
        None => {
            serve_owned_transport(
                stream,
                listener.websocket,
                settings,
                driver,
                acceptor,
                publisher,
            )
            .await;
        }
    }
}

async fn serve_owned_transport<Io, B, D>(
    stream: Io,
    websocket: bool,
    settings: ConnectionSettings<B>,
    driver: D,
    acceptor: ServerConnectionAcceptor,
    publisher: Publisher,
) where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    B: Broker,
    D: RetainedDriver<B>,
{
    let Publisher {
        primary,
        close,
        #[cfg(test)]
        controls,
    } = publisher;
    if !websocket {
        serve_owned_connection(
            stream,
            settings,
            driver,
            acceptor,
            primary,
            #[cfg(test)]
            controls,
        )
        .await;
        return;
    }
    let upgraded = ready::observe(
        tokio::time::timeout_at(
            settings.deadline,
            websocket::upgrade(stream, settings.shared_access_authentication.is_some()),
        ),
        move |result| match result {
            Ok(Ok(upgraded)) => Some((upgraded, primary)),
            Ok(Err(error)) => {
                primary.publish(Outcome::Finished(Err(error)));
                None
            }
            Err(_) => {
                primary.publish(Outcome::Finished(Err(handshake_timeout_error())));
                None
            }
        },
        #[cfg(test)]
        || {},
    )
    .await;
    let Some(((stream, closing), primary)) = upgraded else {
        return;
    };
    serve_owned_connection(
        stream,
        settings,
        driver,
        acceptor,
        primary,
        #[cfg(test)]
        controls.clone(),
    )
    .await;
    ready::observe(
        closing.finish(),
        |closed| {
            close.publish(closed);
            #[cfg(test)]
            controls.close_ready();
        },
        #[cfg(test)]
        || controls.close_pending(),
    )
    .await;
}

/// Payload-free closed marker; no outer observer republishes an inner result.
enum Opened {
    Accepted(ServerConnection),
    AlreadyPublished,
}

async fn serve_owned_connection<Io, B, D>(
    stream: Io,
    settings: ConnectionSettings<B>,
    driver: D,
    acceptor: ServerConnectionAcceptor,
    primary: PrimaryPublisher,
    #[cfg(test)] controls: Arc<Controls>,
) where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    B: Broker,
    D: RetainedDriver<B>,
{
    let ConnectionSettings {
        container_id,
        namespace,
        broker,
        shared_access_authentication,
        connection_options,
        deadline,
    } = settings;
    if deadline <= tokio::time::Instant::now() {
        primary.publish(Outcome::Finished(Err(handshake_timeout_error())));
        return;
    }
    let primary = Primary::new(primary);
    let opened = async {
        match shared_access_authentication {
            Some(config) => {
                let sasl = SharedAccessSaslAcceptor::new(&config);
                let acceptance = accept_owned::<Io, B, D>(
                    acceptor,
                    stream,
                    container_id,
                    Some(Arc::new(sasl.clone())),
                    connection_options,
                    &primary,
                    #[cfg(test)]
                    &controls,
                )
                .await;
                match acceptance {
                    Opened::Accepted(connection) => {
                        let authorization = ConnectionAuthorization::new(config, sasl.grant());
                        (Opened::Accepted(connection), Some(authorization))
                    }
                    Opened::AlreadyPublished => (Opened::AlreadyPublished, None),
                }
            }
            None => (
                accept_owned::<Io, B, D>(
                    acceptor,
                    stream,
                    container_id,
                    None,
                    connection_options,
                    &primary,
                    #[cfg(test)]
                    &controls,
                )
                .await,
                None,
            ),
        }
    };
    // This PARENT observer must not loan while it polls the nested accept leaf.
    let (opened, authorization) = ready::observe(
        tokio::time::timeout_at(deadline, opened),
        |result| match result {
            Ok(opened) => opened,
            Err(_) => {
                match primary.loan() {
                    Attempt::Available(loan) => {
                        // Construct only when this exact timeout boundary is Ready.
                        loan.publish(Outcome::Finished(Err(handshake_timeout_error())));
                    }
                    Attempt::Published => {}
                }
                (Opened::AlreadyPublished, None)
            }
        },
        #[cfg(test)]
        || {},
    )
    .await;
    let mut connection = match opened {
        Opened::Accepted(connection) => connection,
        Opened::AlreadyPublished => return,
    };
    #[cfg(test)]
    controls.opened(deadline).await;
    if deadline <= tokio::time::Instant::now() {
        // Preserve original construction order: no Ready timeout error exists
        // until this legacy signal-only shutdown actually finishes.
        ready::observe(
            connection.shutdown(),
            |()| {},
            #[cfg(test)]
            || controls.shutdown_pending(),
        )
        .await;
        if let Attempt::Available(loan) = primary.loan() {
            loan.publish(Outcome::Finished(Err(handshake_timeout_error())));
        }
        return;
    }
    ready::with_primary(
        driver.serve_retained_open(&mut connection, namespace, broker, authorization),
        &primary,
        || (),
        |result, loan| {
            loan.publish(Outcome::Finished(result));
            #[cfg(test)]
            controls.primary_ready();
        },
        #[cfg(test)]
        || {},
    )
    .await;
    ready::observe(
        connection.shutdown(),
        |()| {},
        #[cfg(test)]
        || controls.shutdown_pending(),
    )
    .await;
}

async fn accept_owned<Io, B, D>(
    acceptor: ServerConnectionAcceptor,
    stream: Io,
    container_id: String,
    sasl: Option<Arc<dyn amqp::SaslAuthenticator>>,
    options: amqp::ConnectionOptions,
    primary: &Primary,
    #[cfg(test)] controls: &Controls,
) -> Opened
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    B: Broker,
    D: RetainedDriver<B>,
{
    let mapping = |result: Result<ScopedConnectionAcceptance<Io>, amqp::EngineError>,
                   loan: ready::PrimaryLoan<'_>| {
        match result {
            Ok(ScopedConnectionAcceptance::Accepted(connection)) => Opened::Accepted(connection),
            Ok(ScopedConnectionAcceptance::Refused(refused)) => {
                // Disposition precedes advanced Io Drop AND completed accept FutureDrop.
                loan.publish(Outcome::LaunchRefused);
                #[cfg(test)]
                controls.launch_refused();
                drop(refused);
                Opened::AlreadyPublished
            }
            Err(error) => {
                loan.publish(Outcome::Finished(Err(error.into())));
                Opened::AlreadyPublished
            }
        }
    };
    match D::ADMISSION {
        AdmissionMode::Ordinary => {
            ready::with_primary(
                acceptor.accept_with_options(stream, container_id, sasl, options),
                primary,
                || Opened::AlreadyPublished,
                mapping,
                #[cfg(test)]
                || {},
            )
            .await
        }
        AdmissionMode::AtomicPosting => {
            ready::with_primary(
                acceptor.accept_with_transactional_ingress(stream, container_id, sasl, options),
                primary,
                || Opened::AlreadyPublished,
                mapping,
                #[cfg(test)]
                || {},
            )
            .await
        }
        AdmissionMode::AtomicMessaging => {
            ready::with_primary(
                acceptor.accept_with_transactional_work_defaults(
                    stream,
                    container_id,
                    sasl,
                    options,
                ),
                primary,
                || Opened::AlreadyPublished,
                mapping,
                #[cfg(test)]
                || {},
            )
            .await
        }
    }
}

#[cfg(test)]
fn start_test<Io, B, D>(
    stream: Io,
    websocket: bool,
    settings: ConnectionSettings<B>,
    driver: D,
    starter: RetainedConnectionStarter,
) -> bool
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    B: Broker,
    D: RetainedDriver<B>,
{
    let Some((claim, acceptor, publisher)) = starter.claim() else {
        return false;
    };
    claim.spawn(async move {
        publisher.controls.first_poll().await;
        if settings.deadline <= tokio::time::Instant::now() {
            publisher.primary.publish(Outcome::SkippedExpiredDeadline);
            return;
        }
        serve_owned_transport(stream, websocket, settings, driver, acceptor, publisher).await;
    });
    true
}

#[cfg(test)]
mod tests;
