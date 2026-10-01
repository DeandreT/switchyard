use super::*;

/// Every operation from an admitted endpoint retains the same identity.
#[derive(Clone)]
pub(crate) struct BoundBroker<B> {
    inner: B,
    binding: EntityBinding,
}

impl<B: Broker> BoundBroker<B> {
    pub(crate) fn new(inner: B, binding: EntityBinding) -> Self {
        Self { inner, binding }
    }

    pub(crate) fn binding(&self) -> &EntityBinding {
        &self.binding
    }

    fn namespace_matches(&self, namespace: &NamespaceName) -> Result<(), BrokerRejection> {
        if self.binding.namespace() == namespace {
            Ok(())
        } else {
            Err(BrokerRejection::Refused(BrokerError::InvalidEntityBinding))
        }
    }

    fn binding_matches(&self, binding: &EntityBinding) -> Result<(), BrokerRejection> {
        if &self.binding == binding {
            Ok(())
        } else {
            Err(BrokerRejection::Refused(BrokerError::InvalidEntityBinding))
        }
    }
}

impl<B: Broker> Broker for BoundBroker<B> {
    async fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityAdmission>, BrokerRejection> {
        self.namespace_matches(&namespace)?;
        let target_path = target
            .canonical_entity()
            .map_err(|_| BrokerRejection::Refused(BrokerError::InvalidEntityBinding))?;
        if &target_path != self.binding.target() {
            return Err(BrokerRejection::Refused(BrokerError::InvalidEntityBinding));
        }
        let admission = self.inner.bind(namespace, target).await?;
        match admission {
            Some(admission) if admission.binding == self.binding => Ok(Some(admission)),
            _ => Err(BrokerRejection::Refused(BrokerError::EntityBindingStale)),
        }
    }

    async fn submit_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.binding_matches(&binding)?;
        self.inner.submit_fenced(binding, entity, kind).await
    }

    async fn rules_fenced(
        &self,
        binding: EntityBinding,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        self.binding_matches(&binding)?;
        self.inner.rules_fenced(binding, topic, subscription).await
    }

    async fn rules(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        self.namespace_matches(&namespace)?;
        self.inner
            .rules_fenced(self.binding.clone(), topic, subscription)
            .await
    }

    async fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        Ok(self
            .bind(namespace, target)
            .await?
            .map(|admission| admission.metadata))
    }

    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.namespace_matches(&namespace)?;
        self.inner
            .submit_fenced(self.binding.clone(), entity, kind)
            .await
    }

    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        self.inner.deliverable(namespace, entity)
    }
}

#[cfg(test)]
mod tests;
