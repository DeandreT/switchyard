use std::sync::Arc;

use amqp::{
    AmqpError, EngineError, IncomingAttach, LinkEndpoint, MessageFormatDecoders,
    ReceiverSettleMode, Role, SenderSettleMode, ServerSession, TargetTerminus,
};
use auth::Permission;
use domain::{EntityIncarnationKind, NamespaceName};
use tokio::{
    sync::{Semaphore, mpsc, oneshot},
    task::JoinSet,
};

use super::{Event, IngressError, IngressMode, QueueAdmission, workers};
use crate::{
    Attachment, BrokerRejection, EntityMetadata, NativeAtomicBroker,
    authorization::ConnectionAuthorization,
    cbs::{serve_cbs_replies, serve_cbs_requests},
    listener::{LinkAuthorization, detach_with, error_for, rejection_error, unauthorized_error},
    parse_attachment,
};

pub(super) async fn serve_session<B: NativeAtomicBroker>(
    mut session: ServerSession,
    namespace: NamespaceName,
    broker: B,
    authorization: Option<Arc<ConnectionAuthorization>>,
    events: mpsc::Sender<Event>,
    links: Arc<Semaphore>,
    mode: IngressMode,
) -> Result<(), IngressError> {
    let mut workers = JoinSet::new();
    let result: Result<(), IngressError> = async {
        loop {
            let attach = tokio::select! {
                result = workers.join_next(), if !workers.is_empty() => {
                    match result {
                        Some(Ok(Ok(()))) => continue,
                        Some(Ok(Err(error))) => return Err(error),
                        Some(Err(error)) => return Err(error.into()),
                        None => continue,
                    }
                }
                attach = session.next_incoming_attach() => attach,
            };
            let Some(mut attach) = attach else { break };
            let permit = match Arc::clone(&links).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    refuse(
                        &session,
                        attach,
                        error_for(
                            AmqpError::ResourceLimitExceeded,
                            "posting link limit reached".into(),
                        ),
                    )
                    .await?;
                    continue;
                }
            };

            if attach
                .target
                .as_ref()
                .and_then(TargetTerminus::as_coordinator)
                .is_some()
            {
                if let Some(authorization) = authorization.as_ref()
                    && !authorization.has_valid_grant().await
                {
                    refuse(
                        &session,
                        attach,
                        unauthorized_error("a coordinator requires a valid grant"),
                    )
                    .await?;
                    continue;
                }
                let endpoint = match session
                    .accept_coordinator(
                        attach,
                        crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64,
                    )
                    .await
                {
                    Ok(endpoint) => endpoint,
                    Err(EngineError::RemoteDetached) => continue,
                    Err(error) => return Err(error.into()),
                };
                workers.spawn(workers::controller(
                    endpoint,
                    authorization.clone(),
                    events.clone(),
                    permit,
                ));
                continue;
            }

            let source = attach
                .source
                .as_ref()
                .and_then(|source| source.address.clone())
                .unwrap_or_default();
            let target = attach
                .target
                .as_ref()
                .and_then(TargetTerminus::as_target)
                .and_then(|target| target.address.clone())
                .unwrap_or_default();
            if let Some(authorization) = authorization.as_ref()
                && (source == crate::CBS_NODE || target == crate::CBS_NODE)
            {
                if attach.role == Role::Sender && attach.initial_delivery_count.is_none() {
                    attach.initial_delivery_count = Some(0);
                }
                let endpoint = match session
                    .accept_attach(attach, crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64)
                    .await
                {
                    Ok(endpoint) => endpoint,
                    Err(EngineError::RemoteDetached) => continue,
                    Err(error) => return Err(error.into()),
                };
                let authorization = Arc::clone(authorization);
                match (target.as_str(), source.as_str(), endpoint) {
                    (crate::CBS_NODE, _, LinkEndpoint::Receiver(receiver)) => {
                        workers.spawn(async move {
                            let _permit = permit;
                            serve_cbs_requests(receiver, authorization).await
                        });
                    }
                    (_, crate::CBS_NODE, LinkEndpoint::Sender(sender)) if !target.is_empty() => {
                        let (route, responses) =
                            authorization.register_reply_route(target.clone()).await;
                        workers.spawn(async move {
                            let _permit = permit;
                            serve_cbs_replies(sender, target, route, responses, authorization).await
                        });
                    }
                    (_, _, endpoint) => {
                        detach_with(
                            endpoint,
                            error_for(AmqpError::InvalidField, "invalid CBS link".into()),
                        )
                        .await;
                    }
                }
                continue;
            }

            // Unsupported roles and endpoints are refused without reading topology.
            if (attach.role != Role::Sender && mode != IngressMode::Messaging)
                || crate::address::strip_control_suffix(&target, "/$management").is_some()
                || (attach.role == Role::Receiver
                    && crate::address::strip_control_suffix(&source, "/$management").is_some())
            {
                refuse(
                    &session,
                    attach,
                    error_for(
                        AmqpError::NotImplemented,
                        "this endpoint does not support the requested link".into(),
                    ),
                )
                .await?;
                continue;
            }
            if attach.role == Role::Receiver {
                if !matches!(
                    attach.snd_settle_mode,
                    SenderSettleMode::Mixed | SenderSettleMode::Unsettled
                )
                    || attach.rcv_settle_mode != ReceiverSettleMode::Second
                    || attach.source.as_ref().is_some_and(|source| {
                        source.dynamic
                            || source.durable != 0
                            || source.distribution_mode.as_ref().is_some_and(|mode| mode.as_str() != "move")
                            || source.filter.as_ref().is_some_and(|filter| !filter.is_empty())
                    })
                {
                    refuse(
                        &session,
                        attach,
                        error_for(
                            AmqpError::NotAllowed,
                            "atomic receiving requires a fixed, non-durable, unfiltered Mixed-or-Unsettled/Second move source".into(),
                        ),
                    )
                    .await?;
                    continue;
                }
                let (admission, link_authorization) = match admit_queue(
                    &broker,
                    &namespace,
                    &source,
                    authorization.as_ref(),
                    Permission::Listen,
                )
                .await
                {
                    Ok(admission) => admission,
                    Err(error) => {
                        refuse(&session, attach, error).await?;
                        continue;
                    }
                };
                let maximum = admission.config.max_message_bytes as u64;
                let sender = match session
                    .accept_transactional_sender_negotiating_unsettled(attach, maximum)
                    .await
                {
                    Ok(sender) => sender,
                    Err(EngineError::RemoteDetached) => continue,
                    Err(error) => return Err(error.into()),
                };
                workers.spawn(workers::consumer(
                    sender,
                    admission,
                    broker.clone(),
                    link_authorization,
                    events.clone(),
                    permit,
                ));
                continue;
            }
            if attach.snd_settle_mode == SenderSettleMode::Settled {
                refuse(
                    &session,
                    attach,
                    error_for(
                        AmqpError::NotAllowed,
                        "posting links require unsettled transfers".into(),
                    ),
                )
                .await?;
                continue;
            }

            let (admission, link_authorization) =
                match admit_queue(&broker, &namespace, &target, authorization.as_ref(), Permission::Send).await {
                    Ok(admission) => admission,
                    Err(error) => {
                        refuse(&session, attach, error).await?;
                        continue;
                    }
                };
            let decoders = MessageFormatDecoders::default().with_decoder(
                crate::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
                amqp::decode_message,
            )?;
            let maximum = admission.config.max_message_bytes as u64;
            let receiver = match session
                .accept_transactional_receiver_with_decoders(attach, maximum, decoders)
                .await
            {
                Ok(receiver) => receiver,
                Err(EngineError::RemoteDetached) => continue,
                Err(error) => return Err(error.into()),
            };
            workers.spawn(workers::producer(
                receiver,
                admission,
                broker.clone(),
                link_authorization,
                events.clone(),
                permit,
            ));
        }
        // Peer End retires each endpoint, allowing its scoped cleanup to complete.
        while let Some(result) = workers.join_next().await {
            result??;
        }
        Ok(())
    }
    .await;
    if result.is_err() {
        let (reply, stopped) = oneshot::channel();
        if events.send(Event::StopConnection { reply }).await.is_ok() {
            let _ = stopped.await;
        }
    }
    result
}

async fn admit_queue<B: NativeAtomicBroker>(
    broker: &B,
    namespace: &NamespaceName,
    address: &str,
    authorization: Option<&Arc<ConnectionAuthorization>>,
    permission: Permission,
) -> Result<(QueueAdmission, Option<LinkAuthorization>), amqp::Error> {
    let target = parse_attachment(address)
        .map_err(|error| error_for(AmqpError::InvalidField, error.to_string()))?;
    let Attachment::Queue(entity) = &target else {
        return Err(error_for(
            AmqpError::NotAllowed,
            "only primary queues accept atomic messaging".into(),
        ));
    };
    let link_authorization = match authorization {
        Some(authorization) => {
            let resource = authorization
                .authorize_entity(entity.as_str(), permission)
                .await
                .map_err(|_| {
                    unauthorized_error("the required operation is not authorized for this queue")
                })?;
            Some(LinkAuthorization {
                connection: Arc::clone(authorization),
                resource,
                permission,
            })
        }
        None => None,
    };
    let admission = broker
        .bind(namespace.clone(), target.clone())
        .await
        .map_err(|error| rejection_error(&error))?
        .ok_or_else(|| {
            rejection_error(&BrokerRejection::Refused(
                domain::BrokerError::QueueNotFound,
            ))
        })?;
    if admission.binding.namespace() != namespace
        || admission.binding.target() != entity
        || admission.binding.owner() != entity
    {
        return Err(error_for(
            AmqpError::InternalError,
            "queue admission identity mismatch".into(),
        ));
    }
    let config = match admission.metadata {
        EntityMetadata::Queue(config)
            if admission.binding.kind() == EntityIncarnationKind::Queue =>
        {
            config
        }
        EntityMetadata::Topic(_) => {
            return Err(error_for(
                AmqpError::NotAllowed,
                "atomic topic posting is not supported".into(),
            ));
        }
        _ => {
            return Err(error_for(
                AmqpError::InternalError,
                "queue admission metadata mismatch".into(),
            ));
        }
    };
    if config.requires_session {
        return Err(error_for(
            AmqpError::NotAllowed,
            "atomic session queues are not supported".into(),
        ));
    }
    Ok((
        QueueAdmission {
            binding: admission.binding,
            config,
        },
        link_authorization,
    ))
}

async fn refuse(
    session: &ServerSession,
    attach: IncomingAttach,
    error: amqp::Error,
) -> Result<(), IngressError> {
    if attach
        .target
        .as_ref()
        .and_then(TargetTerminus::as_coordinator)
        .is_some()
    {
        match session
            .accept_coordinator(attach, crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64)
            .await
        {
            Ok(endpoint) => endpoint.close_with_error(error).await?,
            Err(EngineError::RemoteDetached) => {}
            Err(error) => return Err(error.into()),
        }
    } else {
        match session
            .accept_attach(attach, crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES as u64)
            .await
        {
            Ok(endpoint) => detach_with(endpoint, error).await,
            Err(EngineError::RemoteDetached) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
