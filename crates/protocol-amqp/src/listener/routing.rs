use super::*;

/// Authorize before reading topology, so missing and corrupt targets are not
/// an existence probe for an unauthorized connection.
pub(super) async fn plan_link<B: Broker>(
    broker: &B,
    namespace: &NamespaceName,
    address: &str,
    attach: &Attach,
    authorization: Option<&Arc<ConnectionAuthorization>>,
) -> Result<
    (
        EntityPath,
        Option<AcceptedSession>,
        Option<LinkAuthorization>,
        BoundBroker<B>,
    ),
    AmqpProtocolError,
> {
    let target = parse_attachment(address)
        .map_err(|error| error_for(AmqpError::InvalidField, error.to_string()))?;
    let entity = target
        .canonical_entity()
        .map_err(|error| error_for(AmqpError::InvalidField, error.to_string()))?;
    let permission = match attach.role {
        Role::Sender => Permission::Send,
        Role::Receiver => Permission::Listen,
    };
    let link_authorization = match authorization {
        Some(authorization) => {
            let resource = authorization
                .authorize_entity(entity.as_str(), permission)
                .await
                .map_err(|_| {
                    unauthorized_error(format!("{permission:?} is not authorized for {entity}"))
                })?;
            Some(LinkAuthorization {
                connection: Arc::clone(authorization),
                resource,
                permission,
            })
        }
        None => None,
    };
    let admission = admission(broker, namespace, &target).await?;
    let metadata = admission.metadata;
    let broker = BoundBroker::new(broker.clone(), admission.binding);
    if !data_role_allowed(&target, metadata, &attach.role) {
        return Err(error_for(
            AmqpError::NotAllowed,
            format!("{entity} does not support this data link role"),
        ));
    }
    if attach.role != Role::Receiver {
        return Ok((entity, None, link_authorization, broker));
    }
    let session_request = read_session_filter(attach.source.as_ref())
        .map_err(|error| error_for(AmqpError::InvalidField, error.to_string()))?;
    if matches!(metadata, crate::EntityMetadata::Subscription(config) if config.requires_session)
        && matches!(session_request, SessionRequest::None)
    {
        return Err(rejection_error(&BrokerRejection::Refused(
            domain::BrokerError::SessionRequired,
        )));
    }
    let session_id = match session_request {
        SessionRequest::None => return Ok((entity, None, link_authorization, broker)),
        SessionRequest::NextAvailable => None,
        SessionRequest::Named(session_id) => Some(session_id),
    };
    match broker
        .submit(
            namespace.clone(),
            entity.clone(),
            CommandKind::AcceptSession {
                session_id,
                lock_duration_millis: None,
            },
        )
        .await
    {
        Ok(CommandOutcome::SessionAccepted(Some(accepted))) => {
            Ok((entity, Some(accepted), link_authorization, broker))
        }
        Ok(CommandOutcome::SessionAccepted(None)) => Err(AmqpProtocolError::new(
            ErrorCondition::Custom(Symbol::from(crate::TIMEOUT)),
            String::from("no session is available to accept"),
            None,
        )),
        Ok(other) => Err(error_for(
            AmqpError::InternalError,
            format!("accepting a session produced an unexpected outcome: {other:?}"),
        )),
        Err(rejection) => Err(rejection_error(&rejection)),
    }
}

pub(super) async fn plan_management<B: Broker>(
    broker: &B,
    namespace: &NamespaceName,
    target: Attachment,
    authorization: Option<&Arc<ConnectionAuthorization>>,
) -> Result<(EntityPath, Option<ManagementAuthorization>, BoundBroker<B>), AmqpProtocolError> {
    let entity = target
        .canonical_entity()
        .map_err(|error| error_for(AmqpError::InvalidField, error.to_string()))?;
    let endpoint = format!("{entity}/$management");
    let link_authorization = match authorization {
        Some(authorization) => {
            let resource = authorization
                .authorize_entity_any(&endpoint, &[Permission::Send, Permission::Listen])
                .await
                .map_err(|_| {
                    unauthorized_error(format!("Send or Listen is not authorized for {endpoint}",))
                })?;
            Some(ManagementAuthorization::new(
                Arc::clone(authorization),
                resource,
            ))
        }
        None => None,
    };
    let admission = admission(broker, namespace, &target).await?;
    Ok((
        entity,
        link_authorization,
        BoundBroker::new(broker.clone(), admission.binding),
    ))
}

async fn admission<B: Broker>(
    broker: &B,
    namespace: &NamespaceName,
    target: &Attachment,
) -> Result<crate::EntityAdmission, AmqpProtocolError> {
    let admission = broker
        .bind(namespace.clone(), target.clone())
        .await
        .map_err(|rejection| rejection_error(&rejection))?
        .ok_or_else(|| {
            rejection_error(&BrokerRejection::Refused(
                domain::BrokerError::QueueNotFound,
            ))
        })?;
    let matches_target = matches!(
        (target, admission.metadata),
        (
            Attachment::Queue(_),
            crate::EntityMetadata::Queue(_) | crate::EntityMetadata::Topic(_)
        ) | (
            Attachment::Subscription { .. },
            crate::EntityMetadata::Subscription(_)
        ) | (
            Attachment::DeadLetter(_) | Attachment::SubscriptionDeadLetter { .. },
            crate::EntityMetadata::DeadLetter(_)
        )
    );
    if !matches_target {
        return Err(error_for(
            AmqpError::InternalError,
            String::from("entity metadata does not match the requested target"),
        ));
    }
    if admission.binding.namespace() != namespace
        || admission.binding.target()
            != &target
                .canonical_entity()
                .map_err(|error| error_for(AmqpError::InvalidField, error.to_string()))?
    {
        return Err(error_for(
            AmqpError::InternalError,
            "admission identity does not match the requested target".to_owned(),
        ));
    }
    Ok(admission)
}

fn data_role_allowed(target: &Attachment, metadata: crate::EntityMetadata, role: &Role) -> bool {
    match role {
        Role::Sender => matches!(target, Attachment::Queue(_)),
        Role::Receiver => !matches!(metadata, crate::EntityMetadata::Topic(_)),
    }
}

pub(super) fn management_target(address: &str) -> Option<Result<Attachment, ProtocolError>> {
    let entity = crate::address::strip_control_suffix(address, "/$management")?;
    Some(parse_attachment(entity))
}

#[cfg(test)]
pub(super) fn management_entity(address: &str) -> Option<Result<EntityPath, ProtocolError>> {
    management_target(address).map(|target| target.and_then(|target| target.canonical_entity()))
}
