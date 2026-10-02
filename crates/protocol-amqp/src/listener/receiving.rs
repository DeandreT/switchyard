use super::*;

mod budget;
mod pipeline;
mod work;

type LinkError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
enum ReceiveExit {
    Detached,
    Unauthorized,
    Broker(BrokerRejection),
    Refused(AmqpProtocolError),
    Failed(LinkError),
}

impl From<EngineError> for ReceiveExit {
    fn from(error: EngineError) -> Self {
        match error {
            EngineError::RemoteClosed
            | EngineError::RemoteDetached
            | EngineError::Stopped
            | EngineError::SendReservationRevoked => Self::Detached,
            error => Self::Failed(error.into()),
        }
    }
}

pub(super) async fn serve_receiving_client<B: Broker>(
    mut sender: Sender,
    namespace: NamespaceName,
    entity: EntityPath,
    broker: BoundBroker<B>,
    mode: ReceiveMode,
    session: Option<SessionHold>,
    protocol: ReceivingLinkProtocol,
) -> Result<(), LinkError> {
    let exit = match pipeline::receive_until_stopped(
        &mut sender,
        &namespace,
        &entity,
        &broker,
        mode,
        session.as_ref(),
        &protocol,
    )
    .await
    {
        Ok(()) => ReceiveExit::Detached,
        Err(exit) => exit,
    };
    release_session(&broker, &namespace, &entity, session.as_ref()).await;
    let result: Result<(), LinkError> = match exit {
        ReceiveExit::Detached => {
            let _ = sender.close().await;
            Ok(())
        }
        ReceiveExit::Unauthorized => sender
            .close_with_error(unauthorized_error("the link's authorization has expired"))
            .await
            .map_err(Into::into),
        ReceiveExit::Broker(rejection) => sender
            .close_with_error(rejection_error(&rejection))
            .await
            .map_err(Into::into),
        ReceiveExit::Refused(error) => sender.close_with_error(error).await.map_err(Into::into),
        ReceiveExit::Failed(error) => {
            let _ = sender
                .close_with_error(error_for(
                    AmqpError::InternalError,
                    "the receiving operation could not be completed".to_owned(),
                ))
                .await;
            Err(error)
        }
    };
    result
}
