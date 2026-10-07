use crate::queue_capacity::QueueCapacityMode;
use crate::{
    EntityBinding, EntityIncarnationKind, FiniteQueueCapacity, QueueCapacityCommandV1,
    QueueCapacityStatus, QueueCapacityView, QueueConfigUpdate, QueueTimeToLiveUpdate,
};

use super::*;

fn view(
    profile: &queue_capacity::QueueCapacityOwnerProfile,
) -> Result<QueueCapacityView, BrokerError> {
    let capacity = match (profile.mode().limit_bytes(), profile.usage()) {
        (None, None) => QueueCapacityStatus::NonFinite,
        (Some(limit), Some(usage)) => QueueCapacityStatus::FiniteV1 {
            limit: FiniteQueueCapacity::new(limit.get())?,
            reserved_bytes: usage.reserved_bytes(),
            message_count: usage.message_count(),
        },
        _ => return Err(BrokerError::QueueCapacityCorrupt),
    };
    Ok(QueueCapacityView {
        binding: EntityBinding::new(
            profile.namespace().clone(),
            profile.owner().clone(),
            profile.owner().clone(),
            EntityIncarnationKind::Queue,
            profile.generation(),
        )?,
        config: profile.config(),
        capacity,
    })
}

impl<S: StateStore> StateMachine<S> {
    /// Describes one primary queue without consulting or advancing a clock.
    /// This validates the owner profile, not every message/ledger relationship.
    pub fn describe_queue_capacity(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> Result<Option<QueueCapacityView>, BrokerError> {
        NamespaceName::new(namespace.as_str())?;
        EntityPath::new(entity.as_str())?;
        Self::require_primary_entity_path(entity)?;
        let queue = self.store.get(&keys::queue_config(namespace, entity))?;
        if queue.is_none() {
            if self
                .store
                .get(&keys::topic_config(namespace, entity))?
                .is_some()
            {
                return Err(BrokerError::EntityKindMismatch);
            }
            if self
                .store
                .get(&keys::queue_capacity_mode(namespace, entity))?
                .is_some()
                || self
                    .store
                    .get(&keys::queue_capacity_usage(namespace, entity))?
                    .is_some()
                || self
                    .entity_incarnation(namespace, entity)?
                    .is_some_and(|record| !record.is_retired())
            {
                return Err(BrokerError::QueueCapacityCorrupt);
            }
            return Ok(None);
        }
        view(&queue_capacity::validate_owner_profile(
            self, namespace, entity,
        )?)
        .map(Some)
    }

    /// The proposer runs this fence before reading its host clock.
    pub fn validate_queue_capacity_limit_intent(
        &self,
        binding: &EntityBinding,
    ) -> Result<(), BrokerError> {
        binding.validate()?;
        if binding.kind() != EntityIncarnationKind::Queue || binding.target() != binding.owner() {
            return Err(BrokerError::QueueCapacityNotSupported);
        }
        Self::require_primary_entity_path(binding.owner())?;
        self.validate_binding_target(binding, binding.namespace(), binding.target())
    }

    /// Applies a separate versioned capacity instruction in one atomic batch.
    /// The returned view is prepared before commit; there is no post-commit reread.
    pub fn apply_queue_capacity(
        &self,
        instruction: &QueueCapacityCommandV1,
    ) -> Result<QueueCapacityView, BrokerError> {
        let issued_at = match instruction {
            QueueCapacityCommandV1::CreateFinite { issued_at, .. } => *issued_at,
            QueueCapacityCommandV1::SetLimitFenced {
                binding, issued_at, ..
            }
            | QueueCapacityCommandV1::SetDefinitionFenced {
                binding, issued_at, ..
            } => {
                self.validate_queue_capacity_limit_intent(binding)?;
                *issued_at
            }
        };
        let last_applied = self.last_applied_time()?;
        if issued_at < last_applied {
            return Err(BrokerError::ClockRegression {
                last_applied,
                proposed: issued_at,
            });
        }
        let mut batch = WriteBatch::default();
        let result = match instruction {
            QueueCapacityCommandV1::CreateFinite {
                namespace,
                entity,
                config,
                limit,
                ..
            } => {
                NamespaceName::new(namespace.as_str())?;
                EntityPath::new(entity.as_str())?;
                let command = Command::new(
                    namespace.clone(),
                    entity.clone(),
                    issued_at,
                    CommandKind::CreateQueue { config: *config },
                );
                let mut plan = CapacityPlan::existing(namespace, entity);
                let (_, incarnation) = self.create_queue_with_capacity(
                    &command,
                    *config,
                    Some(*limit),
                    &mut batch,
                    &mut plan,
                )?;
                plan.finish(self, &mut batch)?;
                QueueCapacityView {
                    binding: EntityBinding::new(
                        namespace.clone(),
                        entity.clone(),
                        entity.clone(),
                        EntityIncarnationKind::Queue,
                        incarnation.generation(),
                    )?,
                    config: *config,
                    capacity: QueueCapacityStatus::FiniteV1 {
                        limit: *limit,
                        reserved_bytes: 0,
                        message_count: 0,
                    },
                }
            }
            QueueCapacityCommandV1::SetLimitFenced { binding, limit, .. } => {
                // Deterministic identity and profile recheck after clock validation.
                self.validate_queue_capacity_limit_intent(binding)?;
                let profile = queue_capacity::validate_owner_profile(
                    self,
                    binding.namespace(),
                    binding.owner(),
                )?;
                let current = profile
                    .mode()
                    .limit_bytes()
                    .ok_or(BrokerError::QueueCapacityNotSupported)?;
                let usage = profile.usage().ok_or(BrokerError::QueueCapacityCorrupt)?;
                if usage.reserved_bytes() > limit.bytes() {
                    return Err(BrokerError::QueueCapacityFull);
                }
                let proposed = QueueCapacityMode::finite_v1(profile.generation(), limit.nonzero())
                    .map_err(|_| BrokerError::InvalidQueueCapacity)?;
                if current.get() != limit.bytes() {
                    batch.push_put(
                        keys::queue_capacity_mode(binding.namespace(), binding.owner()),
                        proposed
                            .encode()
                            .map_err(|_| BrokerError::QueueCapacityCorrupt)?,
                    );
                }
                let mut result = view(&profile)?;
                result.capacity = QueueCapacityStatus::FiniteV1 {
                    limit: *limit,
                    reserved_bytes: usage.reserved_bytes(),
                    message_count: usage.message_count(),
                };
                result
            }
            QueueCapacityCommandV1::SetDefinitionFenced {
                binding,
                config,
                limit,
                ..
            } => {
                self.validate_queue_capacity_limit_intent(binding)?;
                let profile = queue_capacity::validate_owner_profile(
                    self,
                    binding.namespace(),
                    binding.owner(),
                )?;
                let current = profile
                    .mode()
                    .limit_bytes()
                    .ok_or(BrokerError::QueueCapacityNotSupported)?;
                let usage = profile.usage().ok_or(BrokerError::QueueCapacityCorrupt)?;
                let proposed_config = QueueConfigUpdate {
                    lock_duration_millis: Some(config.lock_duration_millis),
                    max_delivery_count: Some(config.max_delivery_count),
                    default_time_to_live_millis: Some(match config.default_time_to_live_millis {
                        None => QueueTimeToLiveUpdate::Unlimited,
                        Some(millis) => QueueTimeToLiveUpdate::Finite { millis },
                    }),
                    max_message_bytes: Some(config.max_message_bytes),
                    requires_session: Some(config.requires_session),
                    requires_duplicate_detection: Some(config.requires_duplicate_detection),
                    duplicate_detection_history_time_window_millis: Some(
                        config.duplicate_detection_history_time_window_millis,
                    ),
                    dead_lettering_on_message_expiration: Some(
                        config.dead_lettering_on_message_expiration,
                    ),
                }
                .apply_to(profile.config())?;
                if usage.reserved_bytes() > limit.bytes() {
                    return Err(BrokerError::QueueCapacityFull);
                }
                if proposed_config != profile.config() {
                    batch.push_put(
                        keys::queue_config(binding.namespace(), binding.owner()),
                        codec::encode(&proposed_config)?,
                    );
                    batch.push_put(
                        keys::queue_config(
                            binding.namespace(),
                            &binding.owner().dead_letter_queue()?,
                        ),
                        codec::encode(&proposed_config.dead_letter_shadow())?,
                    );
                }
                if current.get() != limit.bytes() {
                    let proposed =
                        QueueCapacityMode::finite_v1(profile.generation(), limit.nonzero())
                            .map_err(|_| BrokerError::InvalidQueueCapacity)?;
                    batch.push_put(
                        keys::queue_capacity_mode(binding.namespace(), binding.owner()),
                        proposed
                            .encode()
                            .map_err(|_| BrokerError::QueueCapacityCorrupt)?,
                    );
                }
                let mut result = view(&profile)?;
                result.config = proposed_config;
                result.capacity = QueueCapacityStatus::FiniteV1 {
                    limit: *limit,
                    reserved_bytes: usage.reserved_bytes(),
                    message_count: usage.message_count(),
                };
                result
            }
        };
        if !batch.is_empty() {
            batch.push_put(keys::clock(), codec::encode(&issued_at)?);
            self.store.apply(batch)?;
        }
        Ok(result)
    }
}
