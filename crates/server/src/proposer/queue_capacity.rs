use domain::{EntityBinding, FiniteQueueCapacity, QueueCapacityCommandV1, QueueCapacityView};

use super::*;

impl<S: StateStore, C: Clock> LocalProposer<S, C> {
    pub fn create_finite_queue(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        config: QueueConfig,
        limit: FiniteQueueCapacity,
    ) -> Result<QueueCapacityView, ProposeError> {
        let issued_at = self.stamp()?;
        Ok(self
            .machine
            .apply_queue_capacity(&QueueCapacityCommandV1::CreateFinite {
                namespace: namespace.clone(),
                entity: entity.clone(),
                issued_at,
                config,
                limit,
            })?)
    }

    /// Rejects stale identity before consulting the host clock, then rechecks
    /// identity deterministically inside the separate capacity instruction.
    pub fn set_queue_capacity_limit_fenced(
        &self,
        binding: &EntityBinding,
        limit: FiniteQueueCapacity,
    ) -> Result<QueueCapacityView, ProposeError> {
        self.machine.validate_queue_capacity_limit_intent(binding)?;
        let issued_at = self.stamp()?;
        Ok(self
            .machine
            .apply_queue_capacity(&QueueCapacityCommandV1::SetLimitFenced {
                binding: binding.clone(),
                issued_at,
                limit,
            })?)
    }

    pub fn describe_queue_capacity(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> Result<Option<QueueCapacityView>, ProposeError> {
        Ok(self.machine.describe_queue_capacity(namespace, entity)?)
    }
}
