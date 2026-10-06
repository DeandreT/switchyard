use super::*;

use super::owned_connection_tasks::{ActorClaim, ReaderBirth as ScopedReaderBirth};

pub(super) struct NegotiatedConnection<Io> {
    stream: Io,
    settings: ConnectionSettings,
    native_policy: NativeIngressPolicy,
}

pub(super) async fn negotiate<Io>(
    mut stream: Io,
    container_id: impl Into<String>,
    sasl: Option<Arc<dyn SaslAuthenticator>>,
    options: ConnectionOptions,
    native_policy: NativeIngressPolicy,
) -> Result<NegotiatedConnection<Io>, EngineError>
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    options.validate()?;
    let local_max_frame_size = normalized_frame_size(DEFAULT_MAX_FRAME_SIZE)?;
    let local_open = Open {
        max_frame_size: local_max_frame_size,
        idle_time_out: Some(options.advertised_idle_timeout()),
        ..Open::new(container_id)
    };
    checked_open_frame(local_open.clone())?;
    if let Some(authenticator) = sasl {
        expect_header(&mut stream, ProtocolHeader::SASL).await?;
        negotiation_header(&mut stream, ProtocolHeader::SASL, options).await?;
        negotiation_frame(
            &mut stream,
            &Frame::Sasl(SaslPerformative::Mechanisms(SaslMechanisms {
                mechanisms: authenticator.mechanisms(),
            })),
            options,
        )
        .await?;
        let init = match read_frame_with_max_size(&mut stream, local_max_frame_size).await? {
            Frame::Sasl(SaslPerformative::Init(init)) => init,
            _ => return Err(invalid_state("expected SASL init")),
        };
        let code = authenticator.authenticate(&init);
        negotiation_frame(
            &mut stream,
            &Frame::Sasl(SaslPerformative::Outcome(SaslOutcome {
                code: code.clone(),
                additional_data: None,
            })),
            options,
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
    negotiation_header(&mut stream, ProtocolHeader::AMQP, options).await?;
    let remote_open = match read_frame_with_max_size(&mut stream, MIN_MAX_FRAME_SIZE).await? {
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(open)),
            ..
        } => open,
        _ => return Err(invalid_state("expected AMQP open")),
    };
    let remote_max_frame_size = normalized_frame_size(remote_open.max_frame_size)?;
    let channel_max = local_open.channel_max;
    negotiation_frame(&mut stream, &checked_open_frame(local_open)?, options).await?;
    let peer_idle_millis = peer_idle_timeout(
        &mut stream,
        remote_open.idle_time_out,
        remote_max_frame_size,
        options,
    )
    .await?;

    Ok(NegotiatedConnection {
        stream,
        settings: ConnectionSettings {
            remote_max_frame_size,
            local_max_frame_size,
            channel_max,
            remote_channel_max: remote_open.channel_max,
            options,
            peer_idle_millis,
        },
        native_policy,
    })
}

pub(super) enum ActorBirth {
    Legacy,
    Scoped(ActorClaim),
}

pub(super) enum ReaderBirth {
    Legacy,
    Scoped(ScopedReaderBirth),
}

pub(super) struct ReaderTask {
    legacy: Option<ConnectionReader>,
    scoped: Option<super::owned_connection_tasks::ReaderView>,
    absent: bool,
}

impl ReaderTask {
    pub(super) async fn shutdown(&mut self) {
        if let Some(reader) = &mut self.legacy {
            reader.shutdown().await;
        }
        if let Some(reader) = &self.scoped {
            reader.shutdown().await;
        }
    }

    pub(super) fn is_absent(&self) -> bool {
        self.absent
    }
}

impl ReaderBirth {
    pub(super) fn spawn<F>(self, future: F) -> ReaderTask
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match self {
            Self::Legacy => ReaderTask {
                legacy: Some(ConnectionReader(Some(tokio::spawn(future)))),
                scoped: None,
                absent: false,
            },
            Self::Scoped(birth) => {
                let scoped = birth.spawn(future);
                ReaderTask {
                    legacy: None,
                    absent: scoped.is_none(),
                    scoped,
                }
            }
        }
    }
}

impl ActorBirth {
    fn peer_close(&self) -> Option<&Arc<super::peer_close_observation::PeerCloseCell>> {
        match self {
            Self::Legacy => None,
            Self::Scoped(claim) => Some(claim.peer_close()),
        }
    }

    fn reader(&self, lifecycle: &ConnectionLifecycle) -> ReaderBirth {
        match self {
            Self::Legacy => ReaderBirth::Legacy,
            Self::Scoped(claim) => {
                claim.bind(lifecycle);
                ReaderBirth::Scoped(claim.reader_birth())
            }
        }
    }

    fn spawn<F>(self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match self {
            Self::Legacy => drop(tokio::spawn(future)),
            Self::Scoped(claim) => claim.spawn(future),
        }
    }

    #[cfg(test)]
    fn final_guard(&self) -> super::owned_connection_tasks::FinalGuard {
        match self {
            Self::Legacy => super::owned_connection_tasks::FinalGuard::default(),
            Self::Scoped(claim) => claim.final_guard(),
        }
    }
}

impl<Io> NegotiatedConnection<Io> {
    pub(super) fn into_transport(self) -> Io {
        self.stream
    }
}

impl<Io: AsyncRead + AsyncWrite + Send + Unpin + 'static> NegotiatedConnection<Io> {
    pub(super) fn launch(self, birth: ActorBirth) -> ServerConnection {
        let (commands, command_rx) = mpsc::channel(256);
        let (incoming_session_tx, incoming_sessions) = mpsc::channel(32);
        let consumed = Arc::new(Notify::new());
        let driver_consumed = consumed.clone();
        let (lifecycle, cancellation, actor_exit) = ConnectionLifecycle::new();
        let reader_birth = birth.reader(&lifecycle);
        let peer_close = birth.peer_close().cloned();
        #[cfg(test)]
        let final_guard = birth.final_guard();
        birth.spawn(async move {
            let exit_guard = actor_exit;
            #[cfg(test)]
            let final_guard = final_guard;
            run_connection(
                self.stream,
                self.settings,
                exit_guard.identity(),
                self.native_policy,
                command_rx,
                incoming_session_tx,
                driver_consumed,
                cancellation,
                reader_birth,
                peer_close,
            )
            .await;
            drop(exit_guard);
            #[cfg(test)]
            drop(final_guard);
        });
        ServerConnection {
            commands,
            incoming_sessions,
            lifecycle,
            close_timeout: DEFAULT_CLOSE_TIMEOUT,
            consumed,
        }
    }
}
