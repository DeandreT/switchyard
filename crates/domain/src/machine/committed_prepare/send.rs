use super::{metadata::PrimaryMetadata, *};

impl<S: StateStore> StateMachine<S> {
    pub(super) fn prepare_committed_send(
        &self,
        command: &Command,
        message: MessageInput<'_>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, CommittedPreparationError> {
        Self::require_primary_entity_path(&command.entity)
            .map_err(CommittedPreparationError::refused)?;
        let shadow = command
            .entity
            .dead_letter_queue()
            .map_err(CommittedPreparationError::refused)?;
        let config = match self.committed_primary_metadata(command, &shadow)? {
            PrimaryMetadata::Queue(config) => config,
            PrimaryMetadata::Absent(_) => {
                return Err(CommittedPreparationError::Refused(
                    BrokerError::QueueNotFound,
                ));
            }
            PrimaryMetadata::Topic => {
                return Err(CommittedPreparationError::Refused(
                    BrokerError::EntityKindMismatch,
                ));
            }
        };
        // The frozen CreateSendV1 image role still requires session agreement.
        require_session_agreement(&config, message.session_id.is_some())
            .map_err(CommittedPreparationError::refused)?;
        validate_message_input(&config, message).map_err(CommittedPreparationError::refused)?;
        let mut counters = self.committed_queue_counters(command)?;
        let sequence = counters
            .allocate_sequence()
            .map_err(CommittedPreparationError::refused)?;
        self.stage_queue_message(command, &config, message, counters, sequence, batch)
            .map_err(CommittedPreparationError::business_state)
    }
}
