//! Library-only authenticated native entity handlers. No listener is started here.

mod conversion;

use std::time::{SystemTime, UNIX_EPOCH};

use auth::{JwtError, JwtPolicy, Permission, ResourceScope};
use domain::AdminCatalogError;
use tonic::{Request, Response, Status, metadata::MetadataMap};

use crate::{EntityBackend, EntityService, ServiceError, wire};

/// A deliberately small, public-safe classification, never a backend's message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendErrorKind {
    InvalidArgument,
    NotFound,
    AlreadyExists,
    FailedPrecondition,
    Unavailable,
    Internal,
}

/// Implement in the backend's crate (or inject a closure) to avoid a dependency
/// from this library back to the server. Unclassified errors should be Internal.
pub trait BackendErrorClassifier<E>: Send + Sync {
    fn classify(&self, error: &E) -> BackendErrorKind;
}

impl<E, F> BackendErrorClassifier<E> for F
where
    F: Fn(&E) -> BackendErrorKind + Send + Sync,
{
    fn classify(&self, error: &E) -> BackendErrorKind {
        self(error)
    }
}

/// Secure default when a backend-specific classification has not been supplied.
#[derive(Clone, Copy, Debug, Default)]
pub struct InternalBackendErrors;

impl<E> BackendErrorClassifier<E> for InternalBackendErrors {
    fn classify(&self, _: &E) -> BackendErrorKind {
        BackendErrorKind::Internal
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NamespaceRootRequired;

impl std::fmt::Display for NamespaceRootRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("native administration requires a namespace-root resource scope")
    }
}

impl std::error::Error for NamespaceRootRequired {}

/// Create/Get/List require a freshly verified namespace-root Manage JWT on every
/// call. The configured amqps resource host is independent of the HTTPS endpoint
/// and the deployment audience inside JwtPolicy. This is not a connection cache.
#[derive(Clone)]
pub struct EntityRpc<B, C = InternalBackendErrors> {
    service: EntityService<B>,
    policy: JwtPolicy,
    namespace_root: ResourceScope,
    classifier: C,
    #[cfg(test)]
    clock: Option<std::sync::Arc<dyn Fn() -> u64 + Send + Sync>>,
}

impl<B: EntityBackend, C: BackendErrorClassifier<B::Error>> EntityRpc<B, C> {
    pub fn new(
        service: EntityService<B>,
        policy: JwtPolicy,
        namespace_root: ResourceScope,
        classifier: C,
    ) -> Result<Self, NamespaceRootRequired> {
        if namespace_root.path().next().is_some() {
            return Err(NamespaceRootRequired);
        }
        Ok(Self {
            service,
            policy,
            namespace_root,
            classifier,
            #[cfg(test)]
            clock: None,
        })
    }

    /// Returns only the entity service. Other generated services have no handler
    /// implementations or registration in this library.
    pub fn into_service(self) -> wire::entity_service_server::EntityServiceServer<Self>
    where
        B: 'static,
        C: 'static,
    {
        wire::entity_service_server::EntityServiceServer::new(self)
    }

    fn authorize(&self, metadata: &MetadataMap) -> Result<(), Status> {
        let mut values = metadata.get_all("authorization").iter();
        let value = values.next().ok_or_else(unauthenticated)?;
        if values.next().is_some() {
            return Err(unauthenticated());
        }
        let value = value.to_str().map_err(|_| unauthenticated())?;
        let (scheme, token) = value.split_once(' ').ok_or_else(unauthenticated)?;
        if !scheme.eq_ignore_ascii_case("bearer")
            || token.is_empty()
            || token.bytes().any(|byte| byte.is_ascii_whitespace())
        {
            return Err(unauthenticated());
        }
        let now = self.now()?;
        let grant = self
            .policy
            .validate(token, &self.namespace_root, now)
            .map_err(|error| match error {
                JwtError::ScopeMismatch => denied(),
                _ => unauthenticated(),
            })?;
        if !grant.allows(&self.namespace_root, Permission::Manage, now) {
            return Err(denied());
        }
        Ok(())
    }

    fn now(&self) -> Result<u64, Status> {
        #[cfg(test)]
        if let Some(clock) = &self.clock {
            return Ok(clock());
        }
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .map_err(|_| Status::unavailable("authorization clock unavailable"))
    }

    fn status(&self, error: ServiceError<B::Error>) -> Status {
        match error {
            ServiceError::Identifier(_)
            | ServiceError::InvalidEntityPath
            | ServiceError::AddressKindMismatch => invalid(),
            ServiceError::ForeignNamespace { .. } => denied(),
            ServiceError::Unsupported(_) => Status::unimplemented("entity setting unsupported"),
            ServiceError::NotFound { .. } => Status::not_found("entity not found"),
            ServiceError::Catalog(error) => match error {
                AdminCatalogError::InvalidPageSize { .. }
                | AdminCatalogError::InvalidEntityPath
                | AdminCatalogError::InvalidCursor
                | AdminCatalogError::CursorScopeMismatch => invalid(),
                AdminCatalogError::CorruptCatalog | AdminCatalogError::Storage(_) => internal(),
            },
            ServiceError::UnexpectedOutcome { .. } => internal(),
            ServiceError::Backend(error) => match self.classifier.classify(&error) {
                BackendErrorKind::InvalidArgument => invalid(),
                BackendErrorKind::NotFound => Status::not_found("entity not found"),
                BackendErrorKind::AlreadyExists => Status::already_exists("entity already exists"),
                BackendErrorKind::FailedPrecondition => {
                    Status::failed_precondition("entity operation refused")
                }
                BackendErrorKind::Unavailable => Status::unavailable("entity backend unavailable"),
                BackendErrorKind::Internal => internal(),
            },
        }
    }
}

fn unauthenticated() -> Status {
    Status::unauthenticated("valid bearer credential required")
}

fn denied() -> Status {
    Status::permission_denied("namespace management permission required")
}

fn invalid() -> Status {
    Status::invalid_argument("invalid entity request")
}

fn internal() -> Status {
    Status::internal("entity operation failed")
}

#[tonic::async_trait]
impl<B, C> wire::entity_service_server::EntityService for EntityRpc<B, C>
where
    B: EntityBackend + 'static,
    C: BackendErrorClassifier<B::Error> + 'static,
{
    async fn create_entity(
        &self,
        request: Request<wire::CreateEntityRequest>,
    ) -> Result<Response<wire::Entity>, Status> {
        self.authorize(request.metadata())?;
        let request = request.into_inner().try_into()?;
        let entity = self
            .service
            .create(request)
            .await
            .map_err(|e| self.status(e))?;
        Ok(Response::new(entity.try_into()?))
    }

    async fn get_entity(
        &self,
        request: Request<wire::GetEntityRequest>,
    ) -> Result<Response<wire::Entity>, Status> {
        self.authorize(request.metadata())?;
        let request = request.into_inner().try_into()?;
        let entity = self
            .service
            .get(request)
            .await
            .map_err(|e| self.status(e))?;
        Ok(Response::new(entity.try_into()?))
    }

    async fn list_entities(
        &self,
        request: Request<wire::ListEntitiesRequest>,
    ) -> Result<Response<wire::ListEntitiesResponse>, Status> {
        self.authorize(request.metadata())?;
        let request = request.into_inner().try_into()?;
        let page = self
            .service
            .list(request)
            .await
            .map_err(|e| self.status(e))?;
        Ok(Response::new(page.try_into()?))
    }

    async fn delete_entity(
        &self,
        request: Request<wire::DeleteEntityRequest>,
    ) -> Result<Response<wire::Operation>, Status> {
        self.authorize(request.metadata())?;
        Err(Status::unimplemented("entity deletion unsupported"))
    }
}

#[cfg(test)]
mod tests;
