use crate::EntityIncarnationKind;

use super::{metadata::PrimaryMetadata, *};

impl<S: StateStore> StateMachine<S> {
    pub(super) fn prepare_committed_create(
        &self,
        command: &Command,
        config: QueueConfig,
        batch: &mut WriteBatch,
        capacity: &mut CapacityPlan,
    ) -> Result<CommandOutcome, CommittedPreparationError> {
        Self::require_primary_entity_path(&command.entity)
            .map_err(CommittedPreparationError::refused)?;
        let config = config
            .validate()
            .map_err(CommittedPreparationError::refused)?;
        let shadow = command
            .entity
            .dead_letter_queue()
            .map_err(CommittedPreparationError::refused)?;
        let previous = match self.committed_primary_metadata(command, &shadow)? {
            PrimaryMetadata::Queue(_) => {
                return Err(CommittedPreparationError::Refused(
                    BrokerError::QueueAlreadyExists,
                ));
            }
            PrimaryMetadata::Topic => {
                return Err(CommittedPreparationError::Refused(
                    BrokerError::EntityPathAlreadyExists,
                ));
            }
            PrimaryMetadata::Absent(previous) => previous,
        };
        if previous.is_some_and(|record| record.generation().checked_add(1).is_none()) {
            return Err(CommittedPreparationError::Refused(
                BrokerError::EntityIncarnationExhausted,
            ));
        }

        let incarnation = self
            .stage_create_incarnation(
                &command.namespace,
                &command.entity,
                EntityIncarnationKind::Queue,
                batch,
            )
            .map_err(CommittedPreparationError::business_state)?;
        let mode = crate::queue_capacity::QueueCapacityMode::non_finite(incarnation.generation())
            .map_err(|_| {
            CommittedPreparationError::business_state(BrokerError::QueueCapacityCorrupt)
        })?;
        batch.push_put(
            keys::queue_capacity_mode(&command.namespace, &command.entity),
            mode.encode().map_err(|_| {
                CommittedPreparationError::business_state(BrokerError::QueueCapacityCorrupt)
            })?,
        );
        capacity
            .prepare_owner(config, incarnation, mode)
            .map_err(CommittedPreparationError::business_state)?;
        self.stage_queue_configuration(command, config, &shadow, batch)
            .map_err(CommittedPreparationError::business_state)
    }
}
