use std::{
    error::Error,
    fmt,
    future::{Future, pending, poll_fn},
    pin::pin,
    sync::{Arc, Mutex, OnceLock},
    task::Poll,
};

use amqp::{EngineError, IncomingSession, NativeConnectionIdentity, ServerConnection};
use domain::NamespaceName;

use super::{
    admission::Cell,
    outcomes::{Data, Reason, locked},
    worker_history::{Budget, Closed},
};
use crate::listener::{
    connection::{AdmissionMode, retained::RetainedDriver},
    retained_connection::RetainedConnectionResult,
};
use crate::{
    NativeAtomicBroker,
    authorization::{ConnectionAuthorization, InitialControlState},
};

pub(super) struct Open<B> {
    pub(super) identity: NativeConnectionIdentity,
    pub(super) namespace: NamespaceName,
    pub(super) broker: B,
    pub(super) authorization: Option<Arc<ConnectionAuthorization>>,
    pub(super) deadline: Option<tokio::time::Instant>,
}
pub(super) struct Exchange<B> {
    pub(super) open: Mutex<Option<Open<B>>>,
    pub(super) cells: Vec<Arc<Cell>>,
    pub(super) overflow: Mutex<Option<IncomingSession>>,
    pub(super) close: Arc<OnceLock<Result<(), EngineError>>>,
    pub(super) data: Arc<Data>,
    pub(super) admissions: Arc<Budget>,
    #[cfg(test)]
    pub(super) hooks: Arc<super::outcomes::Hooks>,
}
pub(super) struct Bridge<B> {
    pub(super) exchange: Arc<Exchange<B>>,
}

impl<B> Exchange<B> {
    pub(super) fn take_open(&self) -> Option<Open<B>> {
        locked(&self.open).take()
    }
    pub(super) async fn wait(
        &self,
        predicate: impl Fn(super::outcomes::RetainedAtomicMessagingProgress) -> bool,
    ) {
        loop {
            let changed = self.data.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if predicate(self.data.progress()) {
                return;
            }
            changed.await;
        }
    }
}
impl<B> Drop for Bridge<B> {
    fn drop(&mut self) {
        self.exchange.data.update(|state| state.bridge_done = true);
    }
}

struct CloseFailure {
    original: Arc<OnceLock<Result<(), EngineError>>>,
}
impl fmt::Debug for CloseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedNativeCloseFailure")
            .finish_non_exhaustive()
    }
}
impl fmt::Display for CloseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the original retained native Close failed")
    }
}
impl Error for CloseFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.original
            .get()
            .and_then(|result| result.as_ref().err())
            .map(|error| error as &(dyn Error + 'static))
    }
}

async fn initial_expiry(
    authorization: Option<Arc<ConnectionAuthorization>>,
    deadline: Option<tokio::time::Instant>,
) {
    if let (Some(authorization), Some(deadline)) = (authorization, deadline) {
        tokio::time::sleep_until(deadline).await;
        if matches!(
            authorization.initial_control_state().await,
            InitialControlState::InitialExpired
        ) {
            return;
        }
    }
    pending().await
}

impl<B: NativeAtomicBroker> Bridge<B> {
    async fn native_close(
        &self,
        connection: &ServerConnection,
        reason: Reason,
    ) -> RetainedConnectionResult {
        self.exchange.data.natural(reason);
        self.exchange.wait(|state| state.authority_closed).await;
        {
            let mut original = pin!(async {
                match reason {
                    Reason::Expired => {
                        connection
                            .close_with_error(crate::listener::unauthorized_error(
                                "no CBS token was supplied before the authorization deadline",
                            ))
                            .await
                    }
                    Reason::History => {
                        connection
                            .close_with_error(crate::listener::error_for(
                                amqp::AmqpError::ResourceLimitExceeded,
                                "retained atomic messaging history limit reached".to_owned(),
                            ))
                            .await
                    }
                    Reason::PeerEnd | Reason::Worker => connection.close().await,
                }
            });
            poll_fn(|cx| match original.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    // The raw result is rooted before the completed native future drops.
                    assert!(
                        self.exchange.close.set(result).is_ok(),
                        "one original native Close result"
                    );
                    Poll::Ready(())
                }
            })
            .await;
        }
        let result = self
            .exchange
            .close
            .get()
            .expect("observed original native Close");
        let mapped: RetainedConnectionResult = if result.is_ok()
            || (reason == Reason::PeerEnd
                && matches!(
                    result,
                    Err(EngineError::RemoteClosed | EngineError::Stopped)
                )) {
            // Legacy peer-end mapping is not proof that the separately retained Close succeeded.
            Ok(())
        } else {
            Err(Box::new(CloseFailure {
                original: Arc::clone(&self.exchange.close),
            }))
        };
        #[cfg(test)]
        self.exchange.hooks.wrapper_return.hold().await;
        mapped
    }
}

impl<B: NativeAtomicBroker> RetainedDriver<B> for Bridge<B> {
    const ADMISSION: AdmissionMode = AdmissionMode::AtomicMessaging;

    async fn serve_retained_open(
        self,
        connection: &mut ServerConnection,
        namespace: NamespaceName,
        broker: B,
        authorization: Option<Arc<ConnectionAuthorization>>,
    ) -> RetainedConnectionResult {
        // Save this one absolute post-Open deadline before publication or binding delay.
        let deadline = authorization.as_ref().and_then(|authorization| {
            tokio::time::Instant::now().checked_add(authorization.authorization_timeout())
        });
        if authorization.is_some() && deadline.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "the configured authorization timeout cannot be represented",
            )
            .into());
        }
        if let (Some(authorization), Some(deadline)) = (&authorization, deadline) {
            authorization.enable_initial_control_grace(deadline).await;
        }
        let mut expiry = pin!(initial_expiry(authorization.clone(), deadline));
        *locked(&self.exchange.open) = Some(Open {
            identity: connection.connection_identity().clone(),
            namespace,
            broker,
            authorization,
            deadline,
        });
        self.exchange.data.changed.notify_waiters();
        tokio::select! {
            () = self.exchange.wait(|state| state.bound || state.authority_closed) => {},
            () = &mut expiry => return self.native_close(connection, Reason::Expired).await,
        }
        let state = self.exchange.data.progress();
        if state.authority_closed {
            if let Some(reason) = state.reason {
                return self.native_close(connection, reason).await;
            }
            #[cfg(test)]
            self.exchange.hooks.wrapper_return.hold().await;
            return Ok(());
        }
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        for ordinal in 0..=self.exchange.cells.len() {
            enum Found {
                Incoming,
                PeerEnd,
                External,
                Natural(Reason),
            }
            let found = {
                let cell = self.exchange.cells.get(ordinal).cloned();
                let identity = connection.connection_identity().clone();
                let mut original = pin!(connection.next_incoming_session());
                loop {
                    let observed = poll_fn(|cx| match original.as_mut().poll(cx) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(Some(incoming)) => {
                            if let Some(cell) = &cell {
                                let mut loan =
                                    cell.loan().expect("unique new discovery receipt slot");
                                loan.observed(incoming);
                                drop(loan);
                                self.exchange
                                    .data
                                    .update(|state| state.session_attempts += 1);
                            } else {
                                let previous = locked(&self.exchange.overflow).replace(incoming);
                                assert!(
                                    previous.is_none(),
                                    "one bounded original overflow receipt"
                                );
                            }
                            Poll::Ready(Found::Incoming)
                        }
                        Poll::Ready(None) => {
                            self.exchange.data.natural(Reason::PeerEnd);
                            Poll::Ready(Found::PeerEnd)
                        }
                    });
                    tokio::pin!(observed);
                    let state = tokio::select! {
                        result = &mut observed => Some(result),
                        () = self.exchange.wait(|state| state.authority_closed || state.reason.is_some()) => {
                            let state = self.exchange.data.progress();
                            Some(if let Some(reason) = state.reason { Found::Natural(reason) } else { Found::External })
                        },
                        () = &mut expiry => Some(Found::Natural(Reason::Expired)),
                        _ = tick.tick() => {
                            if identity.is_active() { None }
                            else { Some(Found::Natural(Reason::PeerEnd)) }
                        },
                    };
                    if let Some(state) = state {
                        break state;
                    }
                }
            };
            match found {
                Found::External => break,
                Found::PeerEnd => return self.native_close(connection, Reason::PeerEnd).await,
                Found::Natural(reason) => return self.native_close(connection, reason).await,
                Found::Incoming => {}
            }
            if ordinal == self.exchange.cells.len() {
                return self.native_close(connection, Reason::History).await;
            }
            #[cfg(test)]
            self.exchange.hooks.discovery_ready.hold().await;
            if self.exchange.data.progress().sealed {
                break;
            }
            let cell = &self.exchange.cells[ordinal];
            let mut loan = cell.loan().expect("same original incoming conversion slot");
            match loan.convert(connection, ordinal, &self.exchange.admissions) {
                Ok(()) => {}
                Err(Closed::Sealed) => break,
                Err(Closed::History) => {
                    drop(loan);
                    return self.native_close(connection, Reason::History).await;
                }
            }
            #[cfg(test)]
            if let super::admission::Payload::Admission(record) = &loan.packet().payload {
                self.exchange.hooks.native_address.store(
                    record.original.as_ref().get_ref() as *const _ as *const () as usize,
                    std::sync::atomic::Ordering::SeqCst,
                );
            }
            #[cfg(test)]
            self.exchange.hooks.conversion.hold().await;
            if self.exchange.data.progress().sealed
                && let super::admission::Payload::Admission(record) = &mut loan.packet_mut().payload
            {
                record.ticket.take();
            }
            drop(loan);
            self.exchange.data.changed.notify_waiters();
        }
        self.exchange.wait(|state| state.authority_closed).await;
        #[cfg(test)]
        self.exchange.hooks.wrapper_return.hold().await;
        Ok(())
    }
}
