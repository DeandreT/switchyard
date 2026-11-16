//! Private drivers for the one Broker owner loop. No public trait injection.

use super::*;

pub(super) trait OwnerAdapter: Send + 'static {
    fn admin_get(
        &self,
        namespace: &NamespaceName,
        target: EntityTarget,
    ) -> Result<Option<AdminCatalogEntry>, AdminError>;
    fn admin_list(
        &self,
        namespace: &NamespaceName,
        kind: AdminCatalogKind,
        after: Option<&AdminCatalogCursor>,
        limit: usize,
    ) -> Result<AdminCatalogPage, AdminError>;
    fn bind_entity(
        &self,
        namespace: &NamespaceName,
        target: &EntityPath,
    ) -> Result<EntityBinding, ProposeError>;
    fn propose(
        &mut self,
        namespace: &NamespaceName,
        target: &EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, ProposeError>;
    fn propose_bound(
        &mut self,
        binding: &EntityBinding,
        kind: CommandKind,
    ) -> Result<CommandOutcome, ProposeError>;
    fn queues_after(
        &self,
        after: Option<&(NamespaceName, EntityPath)>,
        limit: usize,
    ) -> Result<Vec<(NamespaceName, EntityPath)>, ProposeError>;
    fn topics_after(
        &self,
        after: Option<&(NamespaceName, EntityPath)>,
        limit: usize,
    ) -> Result<Vec<(NamespaceName, EntityPath)>, ProposeError>;
    fn last_applied_time(&self) -> Result<Timestamp, ProposeError>;
}

pub(super) struct LocalAdapter<S, C>(pub(super) LocalProposer<S, C>);

impl<S: StateStore, C: Clock> OwnerAdapter for LocalAdapter<S, C> {
    fn admin_get(
        &self,
        namespace: &NamespaceName,
        target: EntityTarget,
    ) -> Result<Option<AdminCatalogEntry>, AdminError> {
        let machine = self.0.machine();
        (|| match target {
            EntityTarget::Entity(path) => {
                if let Some(queue) = machine.admin_queue(namespace, &path)? {
                    Ok(Some(AdminCatalogEntry::Queue(queue)))
                } else {
                    machine
                        .admin_topic(namespace, &path)
                        .map(|topic| topic.map(AdminCatalogEntry::Topic))
                }
            }
            EntityTarget::Subscription { topic, name, .. } => machine
                .admin_subscription(namespace, &topic, &name)
                .map(|subscription| subscription.map(AdminCatalogEntry::Subscription)),
        })()
        .map_err(ServiceError::Catalog)
    }
    fn admin_list(
        &self,
        namespace: &NamespaceName,
        kind: AdminCatalogKind,
        after: Option<&AdminCatalogCursor>,
        limit: usize,
    ) -> Result<AdminCatalogPage, AdminError> {
        self.0
            .machine()
            .admin_catalog_page(namespace, kind, after, limit)
            .map_err(ServiceError::Catalog)
    }
    fn bind_entity(
        &self,
        namespace: &NamespaceName,
        target: &EntityPath,
    ) -> Result<EntityBinding, ProposeError> {
        self.0.bind_entity(namespace, target)
    }
    fn propose(
        &mut self,
        namespace: &NamespaceName,
        target: &EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, ProposeError> {
        self.0.propose(namespace, target, kind)
    }
    fn propose_bound(
        &mut self,
        binding: &EntityBinding,
        kind: CommandKind,
    ) -> Result<CommandOutcome, ProposeError> {
        self.0.propose_bound(binding, kind)
    }
    fn queues_after(
        &self,
        after: Option<&(NamespaceName, EntityPath)>,
        limit: usize,
    ) -> Result<Vec<(NamespaceName, EntityPath)>, ProposeError> {
        self.0
            .machine()
            .queues_after(after, limit)
            .map_err(Into::into)
    }
    fn topics_after(
        &self,
        after: Option<&(NamespaceName, EntityPath)>,
        limit: usize,
    ) -> Result<Vec<(NamespaceName, EntityPath)>, ProposeError> {
        self.0
            .machine()
            .topics_after(after, limit)
            .map_err(Into::into)
    }
    fn last_applied_time(&self) -> Result<Timestamp, ProposeError> {
        self.0.machine().last_applied_time().map_err(Into::into)
    }
}
