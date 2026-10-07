use std::time::{Duration, SystemTime, UNIX_EPOCH};

use auth::{AccessGrant, Permission, ResourceScope, SharedAccessPolicy};
use domain::{
    EntityBinding, EntityIncarnationKind, NamespaceName, QueueCapacityStatus, QueueCapacityView,
};
use http_body_util::{BodyExt, Full};
use hyper::{
    Request, Response, StatusCode,
    body::{Body, Bytes},
    header::AUTHORIZATION,
};
use tokio::time::timeout;

use super::xml;
use crate::BrokerHandle;
use response::{RequestFailure, response};
use route::{Operation, Target};

mod response;
mod route;

pub(super) const MAX_HEADER_COUNT: usize = 32;
pub(super) const MAX_HEADER_BYTES: usize = 16_384;
const MAX_TARGET_BYTES: usize = 4_096;
const MAX_TOKEN_BYTES: usize = 8_192;
const MAX_BODY_BYTES: usize = xml::MAX_BODY_BYTES;
const MAX_BODY_FRAMES: usize = 1_024;
const BODY_TIMEOUT: Duration = Duration::from_secs(10);
const OWNER_TIMEOUT: Duration = Duration::from_secs(20);

pub(super) struct RequestContext {
    pub(super) broker: BrokerHandle,
    pub(super) namespace: NamespaceName,
    pub(super) policy: SharedAccessPolicy,
    pub(super) audience: ResourceScope,
}

pub(super) async fn handle<B>(
    request: Request<B>,
    context: &RequestContext,
) -> Response<Full<Bytes>>
where
    B: Body<Data = Bytes> + Unpin,
{
    handle_with_epoch(request, context, epoch_seconds).await
}

async fn handle_with_epoch<B>(
    request: Request<B>,
    context: &RequestContext,
    epoch: impl Fn() -> Result<u64, RequestFailure>,
) -> Response<Full<Bytes>>
where
    B: Body<Data = Bytes> + Unpin,
{
    match process(request, context, epoch).await {
        Ok(response) => response,
        Err(failure) => failure.into_response(),
    }
}

fn epoch_seconds() -> Result<u64, RequestFailure> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .map_err(|_| RequestFailure::Authentication)
}

struct Authorization {
    grant: AccessGrant,
    requested: ResourceScope,
}

impl Authorization {
    fn recheck(
        &self,
        epoch: &impl Fn() -> Result<u64, RequestFailure>,
    ) -> Result<(), RequestFailure> {
        if !self
            .grant
            .allows(&self.requested, Permission::Manage, epoch()?)
        {
            return Err(RequestFailure::Authentication);
        }
        Ok(())
    }
}

async fn process<B>(
    request: Request<B>,
    context: &RequestContext,
    epoch: impl Fn() -> Result<u64, RequestFailure>,
) -> Result<Response<Full<Bytes>>, RequestFailure>
where
    B: Body<Data = Bytes> + Unpin,
{
    let (parts, body) = request.into_parts();
    let target = route::target(&parts.uri, &parts.headers)?;
    let requested = target.scope(&context.audience)?;
    let token = route::singleton(&parts.headers, AUTHORIZATION)
        .map_err(|_| RequestFailure::Authentication)?
        .filter(|token| token.len() <= MAX_TOKEN_BYTES)
        .ok_or(RequestFailure::Authentication)?;
    let now = epoch()?;
    let grant = context
        .policy
        .authenticate_atom_sas(token, now)
        .map_err(|_| RequestFailure::Authentication)?;
    let authorization = Authorization { grant, requested };
    if !authorization
        .grant
        .allows(&authorization.requested, Permission::Manage, now)
    {
        return Err(RequestFailure::Authentication);
    }

    // None of these substantive validations or body polls precede Manage.
    let operation = route::operation(
        &parts.method,
        parts.version,
        &parts.uri,
        &parts.headers,
        &target,
    )?;
    if matches!(target, Target::Subscription { .. }) {
        return process_subscription(target, operation, body, context, authorization, &epoch).await;
    }
    let entity = match target {
        Target::Entity(_) => Some(target.entity()?),
        Target::Collection => None,
        Target::Subscription { .. } => return Err(RequestFailure::Internal),
    };
    let body = timeout(BODY_TIMEOUT, collect_body(body))
        .await
        .map_err(|_| RequestFailure::Unavailable)??;
    let definition = if matches!(operation, Operation::Create | Operation::Update) {
        let definition = xml::decode_definition(&body)?;
        let entity = entity.as_ref().ok_or(RequestFailure::BadRequest)?;
        // This is representability preflight only, not an ownership proof.
        let binding = EntityBinding::new(
            context.namespace.clone(),
            entity.clone(),
            entity.clone(),
            EntityIncarnationKind::Queue,
            1,
        )
        .map_err(|_| RequestFailure::BadRequest)?;
        xml::validate_view(&QueueCapacityView {
            binding,
            config: definition.config,
            capacity: QueueCapacityStatus::FiniteV1 {
                limit: definition.limit,
                reserved_bytes: 0,
                message_count: 0,
            },
        })?;
        Some(definition)
    } else {
        if !body.is_empty() {
            return Err(RequestFailure::BadRequest);
        }
        None
    };

    // Retain the original literal fixed-host scope and verified grant across
    // body awaits. Expiry here does not cancel work queued after admission.
    authorization.recheck(&epoch)?;
    match operation {
        Operation::List { skip, top } => {
            let views = timeout(
                OWNER_TIMEOUT,
                context
                    .broker
                    .atom_finite_queues_page(context.namespace.clone(), skip, top),
            )
            .await
            .map_err(|_| RequestFailure::Unavailable)??;
            Ok(response(
                StatusCode::OK,
                xml::encode_feed(&views)?,
                "application/atom+xml",
            ))
        }
        Operation::Get => {
            let view = timeout(
                OWNER_TIMEOUT,
                context.broker.get_atom_finite_queue(
                    context.namespace.clone(),
                    entity.ok_or(RequestFailure::BadRequest)?,
                ),
            )
            .await
            .map_err(|_| RequestFailure::Unavailable)??
            .ok_or(RequestFailure::NotFound)?;
            Ok(response(
                StatusCode::OK,
                xml::encode_entry(&view)?,
                "application/atom+xml",
            ))
        }
        Operation::Create | Operation::Update => {
            let definition = definition.ok_or(RequestFailure::Internal)?;
            let entity = entity.ok_or(RequestFailure::BadRequest)?;
            let view = if operation == Operation::Create {
                timeout(
                    OWNER_TIMEOUT,
                    context.broker.create_finite_queue(
                        context.namespace.clone(),
                        entity,
                        definition.config,
                        definition.limit,
                    ),
                )
                .await
                .map_err(|_| RequestFailure::Unavailable)??
            } else {
                timeout(
                    OWNER_TIMEOUT,
                    context.broker.update_atom_finite_queue(
                        context.namespace.clone(),
                        entity,
                        definition.config,
                        definition.limit,
                    ),
                )
                .await
                .map_err(|_| RequestFailure::Unavailable)??
            };
            let status = if operation == Operation::Create {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            };
            Ok(response(
                status,
                xml::encode_entry(&view)?,
                "application/atom+xml",
            ))
        }
        Operation::Delete => {
            timeout(
                OWNER_TIMEOUT,
                context.broker.delete_atom_finite_queue(
                    context.namespace.clone(),
                    entity.ok_or(RequestFailure::BadRequest)?,
                ),
            )
            .await
            .map_err(|_| RequestFailure::Unavailable)??;
            Ok(response(StatusCode::OK, Vec::new(), "application/atom+xml"))
        }
    }
}

async fn process_subscription<B>(
    target: Target,
    operation: Operation,
    body: B,
    context: &RequestContext,
    authorization: Authorization,
    epoch: &impl Fn() -> Result<u64, RequestFailure>,
) -> Result<Response<Full<Bytes>>, RequestFailure>
where
    B: Body<Data = Bytes> + Unpin,
{
    use super::xml::subscriptions;
    let (topic, name) = target.subscription()?;
    let body = timeout(BODY_TIMEOUT, collect_body(body))
        .await
        .map_err(|_| RequestFailure::Unavailable)??;
    let definition = if operation == Operation::Create {
        Some(subscriptions::decode_definition(&body)?)
    } else {
        if !body.is_empty() {
            return Err(RequestFailure::BadRequest);
        }
        None
    };
    authorization.recheck(epoch)?;
    match operation {
        Operation::Create => {
            let config = timeout(
                OWNER_TIMEOUT,
                context.broker.create_atom_subscription(
                    context.namespace.clone(),
                    topic,
                    name.clone(),
                    definition.ok_or(RequestFailure::Internal)?,
                ),
            )
            .await
            .map_err(|_| RequestFailure::Unavailable)??;
            Ok(response(
                StatusCode::CREATED,
                subscriptions::encode_entry(&name, &config)?,
                "application/atom+xml",
            ))
        }
        Operation::Get => {
            let config = timeout(
                OWNER_TIMEOUT,
                context.broker.get_atom_subscription(
                    context.namespace.clone(),
                    topic,
                    name.clone(),
                ),
            )
            .await
            .map_err(|_| RequestFailure::Unavailable)??
            .ok_or(RequestFailure::SubscriptionNotFound)?;
            Ok(response(
                StatusCode::OK,
                subscriptions::encode_entry(&name, &config)?,
                "application/atom+xml",
            ))
        }
        Operation::Delete => {
            timeout(
                OWNER_TIMEOUT,
                context
                    .broker
                    .delete_atom_subscription(context.namespace.clone(), topic, name),
            )
            .await
            .map_err(|_| RequestFailure::Unavailable)??;
            Ok(response(StatusCode::OK, Vec::new(), "application/atom+xml"))
        }
        Operation::List { .. } | Operation::Update => Err(RequestFailure::MethodNotAllowed),
    }
}

async fn collect_body<B>(mut body: B) -> Result<Vec<u8>, RequestFailure>
where
    B: Body<Data = Bytes> + Unpin,
{
    let mut bytes = Vec::new();
    let mut frames = 0usize;
    while let Some(frame) = body.frame().await {
        frames = frames.checked_add(1).ok_or(RequestFailure::BadRequest)?;
        if frames > MAX_BODY_FRAMES {
            return Err(RequestFailure::BadRequest);
        }
        let data = frame
            .map_err(|_| RequestFailure::BadRequest)?
            .into_data()
            .map_err(|_| RequestFailure::BadRequest)?;
        let total = bytes
            .len()
            .checked_add(data.len())
            .ok_or(RequestFailure::BadRequest)?;
        if total > MAX_BODY_BYTES {
            return Err(RequestFailure::BadRequest);
        }
        bytes.extend_from_slice(&data);
        if frames.is_multiple_of(32) {
            tokio::task::yield_now().await;
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests;
