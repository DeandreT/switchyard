//! Concrete test-only proposer. Uses the same Broker owner loop and the actual
//! shared domain preparation. Retained requests never recapture their binding.

use super::*;
use domain::{
    BoundCommand, Command, DurableProposal, capacity_conservation::CapacityHarness,
    capacity_fence::CapacityIntent,
};

pub struct CapacityTestProposer<S, C> {
    harness: CapacityHarness<S>,
    clock: C,
}

impl<S: StateStore, C: Clock> CapacityTestProposer<S, C> {
    pub fn new(harness: CapacityHarness<S>, clock: C) -> Self {
        Self { harness, clock }
    }

    fn stamp(&self) -> Result<Timestamp, ProposeError> {
        let now = self.clock.now();
        let last_applied = self.harness.last_applied_time()?;
        if now >= last_applied {
            return Ok(now);
        }
        if last_applied.as_millis() - now.as_millis() > crate::DEFAULT_MAX_CLOCK_REGRESSION_MILLIS {
            return Err(ProposeError::ClockWentBackward {
                last_applied,
                now,
                allowed_millis: crate::DEFAULT_MAX_CLOCK_REGRESSION_MILLIS,
            });
        }
        Ok(last_applied)
    }
}

impl<S: StateStore, C: Clock> OwnerAdapter for CapacityTestProposer<S, C> {
    fn admin_get(
        &self,
        namespace: &NamespaceName,
        target: EntityTarget,
    ) -> Result<Option<AdminCatalogEntry>, AdminError> {
        (|| match target {
            EntityTarget::Entity(path) => {
                if let Some(queue) = self.harness.admin_queue(namespace, &path)? {
                    Ok(Some(AdminCatalogEntry::Queue(queue)))
                } else {
                    self.harness
                        .admin_topic(namespace, &path)
                        .map(|topic| topic.map(AdminCatalogEntry::Topic))
                }
            }
            EntityTarget::Subscription { topic, name, .. } => self
                .harness
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
        self.harness
            .admin_catalog_page(namespace, kind, after, limit)
            .map_err(ServiceError::Catalog)
    }
    fn bind_entity(
        &self,
        namespace: &NamespaceName,
        target: &EntityPath,
    ) -> Result<EntityBinding, ProposeError> {
        Ok(if self.harness.is_owned(namespace, target) {
            self.harness.bind(target)?
        } else {
            self.harness.bind_ordinary(namespace, target)?
        })
    }
    fn propose(
        &mut self,
        namespace: &NamespaceName,
        target: &EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, ProposeError> {
        if self.harness.is_owned(namespace, target) {
            let binding = self.harness.bind(target)?;
            self.harness.preflight(&binding, &kind)?;
            let command = Command::new(namespace.clone(), target.clone(), self.stamp()?, kind);
            let intent = CapacityIntent::capture(
                self.harness.owner().clone(),
                DurableProposal::unbound(command),
            )
            .map_err(|_| domain::BrokerError::InvalidEntityBinding)?;
            Ok(self.harness.apply(&intent)?)
        } else {
            self.harness.preflight_ordinary(namespace, target, None)?;
            let command = Command::new(namespace.clone(), target.clone(), self.stamp()?, kind);
            Ok(self
                .harness
                .apply_ordinary(&DurableProposal::unbound(command))?)
        }
    }
    fn propose_bound(
        &mut self,
        binding: &EntityBinding,
        kind: CommandKind,
    ) -> Result<CommandOutcome, ProposeError> {
        let owned = self.harness.is_owned(binding.namespace(), binding.target());
        if owned {
            self.harness.preflight(binding, &kind)?;
        } else {
            self.harness.preflight_ordinary(
                binding.namespace(),
                binding.target(),
                Some(binding),
            )?;
        }
        let command = Command::new(
            binding.namespace().clone(),
            binding.target().clone(),
            self.stamp()?,
            kind,
        );
        let proposal = DurableProposal::bound(BoundCommand::new(binding.clone(), command))
            .map_err(|_| domain::BrokerError::InvalidEntityBinding)?;
        if owned {
            let intent = CapacityIntent::capture(self.harness.owner().clone(), proposal)
                .map_err(|_| domain::BrokerError::InvalidEntityBinding)?;
            Ok(self.harness.apply(&intent)?)
        } else {
            Ok(self.harness.apply_ordinary(&proposal)?)
        }
    }
    fn queues_after(
        &self,
        after: Option<&(NamespaceName, EntityPath)>,
        limit: usize,
    ) -> Result<Vec<(NamespaceName, EntityPath)>, ProposeError> {
        self.harness.queues_after(after, limit).map_err(Into::into)
    }
    fn topics_after(
        &self,
        after: Option<&(NamespaceName, EntityPath)>,
        limit: usize,
    ) -> Result<Vec<(NamespaceName, EntityPath)>, ProposeError> {
        self.harness.topics_after(after, limit).map_err(Into::into)
    }
    fn last_applied_time(&self) -> Result<Timestamp, ProposeError> {
        self.harness.last_applied_time().map_err(Into::into)
    }
}

#[cfg(test)]
#[path = "capacity_test_adapter_tests.rs"]
mod tests;
