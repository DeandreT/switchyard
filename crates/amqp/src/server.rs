use std::{
    collections::{HashMap, VecDeque},
    io,
    sync::Arc,
};

use serde_amqp::primitives::Symbol;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot, watch},
};
use tracing::trace;

use crate::{
    Accepted, Attach, Begin, Close, DeliveryState, DeliveryTag, Detach, Disposition, End, Error,
    Fields, Flow, Frame, Message, Open, Outcome, Performative, ProtocolHeader, ReceiverSettleMode,
    Role, SaslCode, SaslInit, SaslMechanisms, SaslOutcome, SaslPerformative, SenderSettleMode,
    Transfer, decode_message, encode_message, read_frame, read_protocol_header, write_frame,
    write_protocol_header,
};

const LINK_CREDIT: u32 = 2_048;
const SESSION_WINDOW: u32 = 2_048;
const FRAME_OVERHEAD_RESERVE: usize = 512;

pub trait SaslAuthenticator: Send + Sync + 'static {
    fn mechanisms(&self) -> Vec<Symbol>;
    fn authenticate(&self, init: &SaslInit) -> SaslCode;
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("the remote peer closed the connection")]
    RemoteClosed,
    #[error("the remote peer detached the link")]
    RemoteDetached,
    #[error("the AMQP engine stopped")]
    Stopped,
    #[error("invalid AMQP state: {0}")]
    InvalidState(String),
    #[error("SASL authentication failed with {0:?}")]
    SaslAuthentication(SaslCode),
}

pub struct ServerConnection {
    commands: mpsc::Sender<Command>,
    incoming_sessions: mpsc::Receiver<IncomingSession>,
}

pub struct IncomingSession {
    channel: u16,
    pub begin: Begin,
}

pub struct ServerSession {
    channel: u16,
    commands: mpsc::Sender<Command>,
    incoming_attaches: mpsc::Receiver<Attach>,
}

pub enum LinkEndpoint {
    Sender(Sender),
    Receiver(Receiver),
}

pub struct Sender {
    name: String,
    channel: u16,
    handle: u32,
    commands: mpsc::Sender<Command>,
    detached: watch::Receiver<bool>,
    pending_confirmation: Option<PendingConfirmation>,
}

pub struct Receiver {
    channel: u16,
    handle: u32,
    commands: mpsc::Sender<Command>,
    deliveries: mpsc::Receiver<Delivery>,
    detached: watch::Receiver<bool>,
}

#[derive(Clone, Debug)]
pub struct Delivery {
    id: u32,
    settled: bool,
    message: Message,
    encoded_message: Vec<u8>,
}

impl Delivery {
    pub fn message(&self) -> &Message {
        &self.message
    }

    pub fn encoded_message(&self) -> &[u8] {
        &self.encoded_message
    }
}

impl ServerConnection {
    pub async fn accept<Io>(
        mut stream: Io,
        container_id: impl Into<String>,
        sasl: Option<Arc<dyn SaslAuthenticator>>,
    ) -> Result<Self, EngineError>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        if let Some(authenticator) = sasl {
            expect_header(&mut stream, ProtocolHeader::SASL).await?;
            write_protocol_header(&mut stream, ProtocolHeader::SASL).await?;
            write_frame(
                &mut stream,
                &Frame::Sasl(SaslPerformative::Mechanisms(SaslMechanisms {
                    mechanisms: authenticator.mechanisms(),
                })),
            )
            .await?;
            let init = match read_frame(&mut stream).await? {
                Frame::Sasl(SaslPerformative::Init(init)) => init,
                _ => return Err(invalid_state("expected SASL init")),
            };
            let code = authenticator.authenticate(&init);
            write_frame(
                &mut stream,
                &Frame::Sasl(SaslPerformative::Outcome(SaslOutcome {
                    code: code.clone(),
                    additional_data: None,
                })),
            )
            .await?;
            if code != SaslCode::Ok {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "SASL authentication failed",
                )
                .into());
            }
        }

        expect_header(&mut stream, ProtocolHeader::AMQP).await?;
        write_protocol_header(&mut stream, ProtocolHeader::AMQP).await?;
        let remote_open = match read_frame(&mut stream).await? {
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(open)),
                ..
            } => open,
            _ => return Err(invalid_state("expected AMQP open")),
        };
        let local_open = Open {
            max_frame_size: remote_open.max_frame_size.max(512),
            channel_max: remote_open.channel_max,
            ..Open::new(container_id)
        };
        write_frame(
            &mut stream,
            &Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(local_open)),
                payload: Vec::new(),
            },
        )
        .await?;

        let (commands, command_rx) = mpsc::channel(256);
        let (incoming_session_tx, incoming_sessions) = mpsc::channel(32);
        tokio::spawn(run_connection(
            stream,
            remote_open.max_frame_size,
            command_rx,
            incoming_session_tx,
        ));
        Ok(Self {
            commands,
            incoming_sessions,
        })
    }

    pub async fn next_incoming_session(&mut self) -> Option<IncomingSession> {
        self.incoming_sessions.recv().await
    }

    pub async fn accept_session(
        &self,
        incoming: IncomingSession,
    ) -> Result<ServerSession, EngineError> {
        let (attach_tx, incoming_attaches) = mpsc::channel(32);
        request(&self.commands, |reply| Command::AcceptSession {
            channel: incoming.channel,
            attach_tx,
            reply,
        })
        .await?;
        Ok(ServerSession {
            channel: incoming.channel,
            commands: self.commands.clone(),
            incoming_attaches,
        })
    }

    pub async fn close(&self) -> Result<(), EngineError> {
        self.close_inner(None).await
    }

    pub async fn close_with_error(&self, error: Error) -> Result<(), EngineError> {
        self.close_inner(Some(error)).await
    }

    async fn close_inner(&self, error: Option<Error>) -> Result<(), EngineError> {
        request(&self.commands, |reply| Command::Close { error, reply }).await
    }
}

impl ServerSession {
    pub async fn next_incoming_attach(&mut self) -> Option<Attach> {
        self.incoming_attaches.recv().await
    }

    pub async fn accept_attach(
        &self,
        attach: Attach,
        max_message_size: u64,
    ) -> Result<LinkEndpoint, EngineError> {
        self.accept_attach_with_properties(attach, max_message_size, None)
            .await
    }

    pub async fn accept_attach_with_properties(
        &self,
        attach: Attach,
        max_message_size: u64,
        properties: Option<Fields>,
    ) -> Result<LinkEndpoint, EngineError> {
        let (deliveries_tx, deliveries) = mpsc::channel(32);
        let (detached_tx, detached) = watch::channel(false);
        let role = attach.role.clone();
        let name = attach.name.clone();
        let handle = attach.handle;
        request(&self.commands, |reply| Command::AcceptLink {
            channel: self.channel,
            attach: Box::new(attach),
            max_message_size,
            properties,
            deliveries_tx,
            detached_tx,
            reply,
        })
        .await?;

        Ok(match role {
            Role::Sender => LinkEndpoint::Receiver(Receiver {
                channel: self.channel,
                handle,
                commands: self.commands.clone(),
                deliveries,
                detached,
            }),
            Role::Receiver => LinkEndpoint::Sender(Sender {
                name,
                channel: self.channel,
                handle,
                commands: self.commands.clone(),
                detached,
                pending_confirmation: None,
            }),
        })
    }
}

impl Sender {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn send(
        &mut self,
        message: Message,
        delivery_tag: DeliveryTag,
    ) -> Result<Outcome, EngineError> {
        let outcome = self.send_unconfirmed(message, delivery_tag).await?;
        self.confirm(outcome_state(&outcome)).await?;
        Ok(outcome)
    }

    /// Sends a delivery and returns the receiver's outcome before confirming
    /// settlement back to it.
    ///
    /// A caller that needs to commit application state before acknowledging a
    /// receiver-settle-mode `second` disposition must call [`Self::confirm`]
    /// after that commit. Only one such delivery may be pending on a sender.
    pub async fn send_unconfirmed(
        &mut self,
        message: Message,
        delivery_tag: DeliveryTag,
    ) -> Result<Outcome, EngineError> {
        if self.pending_confirmation.is_some() {
            return Err(invalid_state(
                "send attempted before the previous outcome was confirmed",
            ));
        }
        let (reply, outcome) = oneshot::channel();
        self.commands
            .send(Command::Send {
                channel: self.channel,
                handle: self.handle,
                message: Box::new(message),
                delivery_tag,
                reply,
            })
            .await
            .map_err(|_| EngineError::Stopped)?;
        let remote = outcome.await.map_err(|_| EngineError::Stopped)??;
        self.pending_confirmation = remote.confirmation;
        Ok(remote.outcome)
    }

    /// Confirms the sender's final delivery state after the receiving
    /// application has durably applied the requested outcome.
    pub async fn confirm(&mut self, state: DeliveryState) -> Result<(), EngineError> {
        let Some(confirmation) = self.pending_confirmation else {
            return Ok(());
        };
        let result = request(&self.commands, |reply| Command::Confirm {
            channel: self.channel,
            handle: confirmation.handle,
            delivery_id: confirmation.delivery_id,
            state,
            batchable: confirmation.batchable,
            reply,
        })
        .await;
        if result.is_ok() {
            self.pending_confirmation = None;
        }
        result
    }

    pub async fn on_detach(&mut self) {
        wait_for_detach(&mut self.detached).await;
    }

    pub async fn close(&self) -> Result<(), EngineError> {
        self.close_inner(None).await
    }

    pub async fn close_with_error(&self, error: Error) -> Result<(), EngineError> {
        self.close_inner(Some(error)).await
    }

    async fn close_inner(&self, error: Option<Error>) -> Result<(), EngineError> {
        request(&self.commands, |reply| Command::Detach {
            channel: self.channel,
            handle: self.handle,
            error,
            reply,
        })
        .await
    }
}

fn outcome_state(outcome: &Outcome) -> DeliveryState {
    match outcome {
        Outcome::Accepted(value) => DeliveryState::Accepted(value.clone()),
        Outcome::Rejected(value) => DeliveryState::Rejected(value.clone()),
        Outcome::Released(value) => DeliveryState::Released(value.clone()),
        Outcome::Modified(value) => DeliveryState::Modified(value.clone()),
    }
}

impl Receiver {
    pub async fn recv(&mut self) -> Result<Delivery, EngineError> {
        self.deliveries.recv().await.ok_or_else(|| {
            if *self.detached.borrow() {
                EngineError::RemoteDetached
            } else {
                EngineError::Stopped
            }
        })
    }

    pub async fn accept(&self, delivery: &Delivery) -> Result<(), EngineError> {
        self.settle(delivery, DeliveryState::Accepted(Accepted))
            .await
    }

    pub async fn reject(
        &self,
        delivery: &Delivery,
        error: Option<Error>,
    ) -> Result<(), EngineError> {
        self.settle(delivery, DeliveryState::Rejected(crate::Rejected { error }))
            .await
    }

    pub async fn release(&self, delivery: &Delivery) -> Result<(), EngineError> {
        self.settle(delivery, DeliveryState::Released(crate::Released))
            .await
    }

    pub async fn modify(
        &self,
        delivery: &Delivery,
        modified: crate::Modified,
    ) -> Result<(), EngineError> {
        self.settle(delivery, DeliveryState::Modified(modified))
            .await
    }

    async fn settle(&self, delivery: &Delivery, state: DeliveryState) -> Result<(), EngineError> {
        if delivery.settled {
            return Ok(());
        }
        request(&self.commands, |reply| Command::Settle {
            channel: self.channel,
            handle: self.handle,
            delivery_id: delivery.id,
            state,
            reply,
        })
        .await
    }

    pub async fn on_detach(&mut self) {
        wait_for_detach(&mut self.detached).await;
    }

    pub async fn close(&self) -> Result<(), EngineError> {
        self.close_inner(None).await
    }

    pub async fn close_with_error(&self, error: Error) -> Result<(), EngineError> {
        self.close_inner(Some(error)).await
    }

    async fn close_inner(&self, error: Option<Error>) -> Result<(), EngineError> {
        request(&self.commands, |reply| Command::Detach {
            channel: self.channel,
            handle: self.handle,
            error,
            reply,
        })
        .await
    }
}

async fn expect_header<Io>(stream: &mut Io, expected: ProtocolHeader) -> Result<(), EngineError>
where
    Io: AsyncRead + Unpin,
{
    let actual = read_protocol_header(stream).await?;
    if actual != expected {
        return Err(invalid_state(format!(
            "expected protocol id {}, got {}",
            expected.protocol_id, actual.protocol_id
        )));
    }
    Ok(())
}

async fn wait_for_detach(detached: &mut watch::Receiver<bool>) {
    while !*detached.borrow_and_update() {
        if detached.changed().await.is_err() {
            break;
        }
    }
}

async fn request<T>(
    commands: &mpsc::Sender<Command>,
    make: impl FnOnce(oneshot::Sender<Result<T, EngineError>>) -> Command,
) -> Result<T, EngineError> {
    let (reply, response) = oneshot::channel();
    commands
        .send(make(reply))
        .await
        .map_err(|_| EngineError::Stopped)?;
    response.await.map_err(|_| EngineError::Stopped)?
}

mod engine;
use engine::*;

#[cfg(feature = "test-client")]
mod client;

#[cfg(feature = "test-client")]
pub use client::{ClientConnection, ClientDelivery, ClientReceiver, ClientSender, ClientSession};
