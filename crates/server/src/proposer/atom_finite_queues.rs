use crate::AtomQueueOwnerError;
use domain::{
    DeleteEntityTarget, EntityIncarnationKind, QueueCapacityStatus, QueueCapacityView,
    QueueConfigUpdate, QueueTimeToLiveUpdate,
};
use protocol_amqp::EntityMetadata;

use super::*;

mod page;

impl<S: StateStore, C: Clock> LocalProposer<S, C> {
    /// Reads an Atom-representable finite primary in one owner turn.
    /// The prepared owner profile is not a whole-ledger health certificate.
    pub fn get_atom_finite_queue(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> Result<Option<QueueCapacityView>, AtomQueueOwnerError> {
        NamespaceName::new(namespace.as_str()).map_err(BrokerError::from)?;
        AdminTarget::Primary(entity.clone()).canonical_entity()?;
        let topology = self.primary_entity_metadata_topology(namespace, entity)?;
        match topology.metadata {
            Some(EntityMetadata::Queue(_)) => {
                let view = self
                    .machine
                    .describe_queue_capacity(namespace, entity)?
                    .ok_or(BrokerError::QueueCapacityCorrupt)?;
                self.validate_metadata_capacity(
                    namespace,
                    &topology.capacity_owners,
                    Some((entity, EntityIncarnationKind::Queue)),
                )?;
                crate::atom_admin::xml::validate_view(&view)
                    .map_err(|_| AtomQueueOwnerError::UnsupportedDefinition)?;
                Ok(Some(view))
            }
            None => {
                if self
                    .machine
                    .entity_incarnation(namespace, entity)?
                    .is_some_and(|incarnation| !incarnation.is_retired())
                {
                    return Err(BrokerError::DanglingEntityMetadata.into());
                }
                self.validate_metadata_capacity(namespace, &topology.capacity_owners, None)?;
                Ok(None)
            }
            Some(_) => {
                self.validate_metadata_capacity(namespace, &topology.capacity_owners, None)?;
                Err(AtomQueueOwnerError::UnsupportedDefinition)
            }
        }
    }

    /// Resolves current identity and applies a complete definition without an
    /// intervening owner request or a postcommit description read.
    /// Current profile, immutable settings, desired core values and desired Atom
    /// projection are validated before host stamping in this by-name API.
    pub fn update_atom_finite_queue(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        config: QueueConfig,
        limit: domain::FiniteQueueCapacity,
    ) -> Result<QueueCapacityView, AtomQueueOwnerError> {
        let current = self
            .get_atom_finite_queue(namespace, entity)?
            .ok_or(BrokerError::QueueNotFound)?;
        let config = QueueConfigUpdate {
            lock_duration_millis: Some(config.lock_duration_millis),
            max_delivery_count: Some(config.max_delivery_count),
            default_time_to_live_millis: Some(
                config
                    .default_time_to_live_millis
                    .map_or(QueueTimeToLiveUpdate::Unlimited, |millis| {
                        QueueTimeToLiveUpdate::Finite { millis }
                    }),
            ),
            max_message_bytes: Some(config.max_message_bytes),
            requires_session: Some(config.requires_session),
            requires_duplicate_detection: Some(config.requires_duplicate_detection),
            duplicate_detection_history_time_window_millis: Some(
                config.duplicate_detection_history_time_window_millis,
            ),
            dead_lettering_on_message_expiration: Some(config.dead_lettering_on_message_expiration),
        }
        .apply_to(current.config)?;
        let QueueCapacityStatus::FiniteV1 {
            reserved_bytes,
            message_count,
            ..
        } = current.capacity
        else {
            return Err(AtomQueueOwnerError::UnsupportedDefinition);
        };
        let desired = QueueCapacityView {
            binding: current.binding.clone(),
            config,
            capacity: QueueCapacityStatus::FiniteV1 {
                limit,
                reserved_bytes,
                message_count,
            },
        };
        crate::atom_admin::xml::validate_view(&desired)
            .map_err(|_| AtomQueueOwnerError::UnsupportedDefinition)?;
        Ok(self.set_finite_queue_definition_fenced(&current.binding, config, limit)?)
    }

    /// Keeps Usage and Charge opaque while proving finite identity, then uses
    /// the original fenced purge in the same owner turn.
    pub fn delete_atom_finite_queue_with_effects(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> Result<CommandApplication, AtomQueueOwnerError> {
        let binding = self
            .machine
            .bind_finite_queue_for_deletion(namespace, entity)?;
        Ok(self.propose_fenced_with_effects(
            &binding,
            entity,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Queue,
            },
        )?)
    }

    /// Returns a filled visible page or a proven exhausted suffix. A refused
    /// page never returns its partially prepared descriptions.
    pub fn atom_finite_queues_page(
        &self,
        namespace: &NamespaceName,
        skip: usize,
        top: usize,
    ) -> Result<Vec<QueueCapacityView>, AtomQueueOwnerError> {
        page::read(self, namespace, skip, top)
    }
}
