//! Closed, read-only ordinary subscription definition singleton. No request XML
//! parser, collection/runtime materialization or mutation adapter is installed.
mod xml;

use super::https::{
    Address, AuthorizedHead, AuthorizedRequest, BodyMode, ClosedRoutes, Handler, HandlerFuture,
    Profile, Refusal, Reply,
};
use crate::{
    EntityAddress, EntityBackend, EntityDefinition, EntityProfile, EntityService, GetEntityRequest,
    ServiceError,
    rpc::{BackendErrorClassifier, BackendErrorKind, InternalBackendErrors},
};
use domain::{AdminCatalogError, EntityPath, NamespaceName, SubscriptionName};
use hyper::{Method, StatusCode};

/// Holds the existing namespace-bound typed service privately. Only its Get
/// method is used, inside the transport-owned retained callback original.
pub struct SubscriptionGet<B, C = InternalBackendErrors> {
    service: EntityService<B>,
    classifier: C,
}

impl<B: EntityBackend, C: BackendErrorClassifier<B::Error>> SubscriptionGet<B, C> {
    pub fn new(service: EntityService<B>, classifier: C) -> Self {
        Self {
            service,
            classifier,
        }
    }

    fn selected(&self, head: &AuthorizedHead) -> Result<bool, Refusal> {
        if head.namespace() != self.service.namespace() {
            return Err(Refusal::Forbidden);
        }
        Ok(head.method() == Method::GET
            && head.profile() == Profile::DefinitionSingleton
            && matches!(head.address(), Address::Subscription { .. }))
    }

    async fn get(&self, head: &AuthorizedHead) -> Result<Vec<u8>, Failure> {
        let Address::Subscription { topic, name } = head.address() else {
            return Err(Failure::Internal);
        };
        let definition = self
            .service
            .get(GetEntityRequest {
                namespace: head.namespace().to_string(),
                address: EntityAddress::Subscription {
                    topic: topic.to_string(),
                    name: name.to_string(),
                },
            })
            .await
            .map_err(|error| self.failure(error))?;
        project(definition, head.namespace(), topic, name)
    }

    fn failure(&self, error: ServiceError<B::Error>) -> Failure {
        match error {
            ServiceError::Identifier(_)
            | ServiceError::InvalidEntityPath
            | ServiceError::AddressKindMismatch => Failure::Invalid,
            ServiceError::ForeignNamespace { .. } => Failure::Forbidden,
            ServiceError::NotFound { .. } => Failure::NotFound,
            ServiceError::Unsupported(_) => Failure::Unsupported,
            ServiceError::Catalog(
                AdminCatalogError::InvalidPageSize { .. }
                | AdminCatalogError::InvalidEntityPath
                | AdminCatalogError::InvalidCursor
                | AdminCatalogError::CursorScopeMismatch,
            ) => Failure::Invalid,
            ServiceError::Catalog(_) | ServiceError::UnexpectedOutcome { .. } => Failure::Internal,
            ServiceError::Backend(error) => {
                if self.classifier.classify(&error) == BackendErrorKind::Unavailable {
                    Failure::Unavailable
                } else {
                    Failure::Internal
                }
            }
        }
    }
}

impl<B: EntityBackend + 'static, C: BackendErrorClassifier<B::Error> + 'static> Handler
    for SubscriptionGet<B, C>
{
    fn select(&self, head: &AuthorizedHead) -> Result<BodyMode, Refusal> {
        if self.selected(head)? {
            Ok(BodyMode::Empty)
        } else {
            ClosedRoutes.select(head)
        }
    }
    fn handle(&self, request: AuthorizedRequest) -> HandlerFuture<'_> {
        Box::pin(async move {
            if !self.selected(request.head())? {
                return Err(Refusal::Unsupported);
            }
            // Empty mode is enforced before this callback; do not add a parser.
            if !request.body().is_empty() {
                return Err(Refusal::Encoding);
            }
            match self.get(request.head()).await {
                Ok(bytes) => Ok(Reply::atom_entry(bytes)),
                Err(failure) => Ok(Reply::xml_error(
                    failure.status(),
                    xml::error(failure).map_err(|_| Refusal::Internal)?,
                )),
            }
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Failure {
    Invalid,
    Forbidden,
    NotFound,
    Unsupported,
    Unavailable,
    Internal,
}
impl Failure {
    fn status(self) -> StatusCode {
        match self {
            Self::Invalid => StatusCode::BAD_REQUEST,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Unsupported => StatusCode::NOT_IMPLEMENTED,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
    fn message(self) -> &'static str {
        match self {
            Self::Invalid => "invalid subscription request",
            Self::Forbidden => "subscription management permission required",
            Self::NotFound => "subscription not found",
            Self::Unsupported => "subscription definition profile unsupported",
            Self::Unavailable => "subscription backend unavailable",
            Self::Internal => "subscription read failed",
        }
    }
}

fn project(
    definition: EntityDefinition,
    namespace: &NamespaceName,
    topic: &EntityPath,
    name: &SubscriptionName,
) -> Result<Vec<u8>, Failure> {
    let EntityDefinition {
        namespace: actual_namespace,
        path,
        profile,
        placement_group_id,
        max_size_bytes,
        used_logical_bytes,
    } = definition;
    if &actual_namespace != namespace
        || topic.subscription(name).map_err(|_| Failure::Internal)? != path
    {
        return Err(Failure::Internal);
    }
    let EntityProfile::Subscription(config) = profile else {
        return Err(Failure::Internal);
    };
    if placement_group_id.is_some() || max_size_bytes.is_some() || used_logical_bytes.is_some() {
        return Err(Failure::Unsupported);
    }
    config.validate().map_err(|_| Failure::Internal)?;
    xml::entry(name.as_str(), config)
}

#[cfg(test)]
mod tests;
