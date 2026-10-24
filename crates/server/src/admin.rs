//! Real owner adapter for the transport-free administration service.

use admin_api::{EntityBackend, EntityService, EntityTarget, ServiceError};
use domain::{
    AdminCatalogCursor, AdminCatalogEntry, AdminCatalogKind, AdminCatalogPage, CommandKind,
    CommandOutcome, EntityPath, NamespaceName,
};

use crate::{BrokerHandle, SubmitError};

pub type AdminService = EntityService<BrokerHandle>;
pub type AdminError = ServiceError<SubmitError>;

impl EntityBackend for BrokerHandle {
    type Error = SubmitError;

    async fn create(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        command: CommandKind,
    ) -> Result<CommandOutcome, AdminError> {
        self.submit(namespace, entity, command)
            .await
            .map_err(ServiceError::Backend)
    }

    async fn get(
        &self,
        namespace: NamespaceName,
        target: EntityTarget,
    ) -> Result<Option<AdminCatalogEntry>, AdminError> {
        self.admin_get(namespace, target).await
    }

    async fn list(
        &self,
        namespace: NamespaceName,
        kind: AdminCatalogKind,
        after: Option<AdminCatalogCursor>,
        limit: usize,
    ) -> Result<AdminCatalogPage, AdminError> {
        self.admin_list(namespace, kind, after, limit).await
    }
}
