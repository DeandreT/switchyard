use crate::{
    AtomicMessagingApplication, AtomicMessagingCommand, EntityIncarnationKind,
    atomic_messaging::validate_kinds,
};

use super::*;

mod overlay;
use overlay::AtomicOverlay;

fn validate_atomic_prelude(envelope: &AtomicMessagingCommand) -> Result<(), BrokerError> {
    validate_kinds(
        envelope.commands.iter().map(|command| &command.kind),
        envelope.commands.len(),
    )?;
    let binding = &envelope.binding;
    if envelope.commands.iter().any(|command| {
        &command.namespace != binding.namespace()
            || &command.entity != binding.target()
            || command.issued_at != envelope.issued_at
    }) {
        return Err(BrokerError::InvalidAtomicMessagingCommand);
    }
    binding.validate()?;
    if binding.kind() != EntityIncarnationKind::Queue || binding.target() != binding.owner() {
        return Err(BrokerError::AtomicMessagingOperationNotSupported);
    }
    Ok(())
}

impl<S: StateStore> StateMachine<S> {
    /// Validates the envelope and current queue identity without consulting a clock.
    pub fn validate_atomic_messaging(
        &self,
        envelope: &AtomicMessagingCommand,
    ) -> Result<(), BrokerError> {
        validate_atomic_prelude(envelope)?;
        let overlay = AtomicOverlay::for_queue(
            self.store.clone(),
            envelope.binding.namespace(),
            envelope.binding.owner(),
        );
        let machine = StateMachine::new(overlay.clone());
        machine
            .validate_atomic_inner(envelope)
            .map_err(|error| overlay.map_error(error))
    }

    fn validate_atomic_inner(&self, envelope: &AtomicMessagingCommand) -> Result<(), BrokerError> {
        let binding = &envelope.binding;
        self.validate_binding_identity(binding, binding.namespace(), binding.target())?;
        let config = self
            .queue_config(binding.namespace(), binding.owner())?
            .ok_or(BrokerError::DanglingEntityMetadata)?
            .validate()?;
        if self
            .store
            .get(&keys::topic_config(binding.namespace(), binding.owner()))?
            .is_some()
        {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        if config.requires_session {
            return Err(BrokerError::AtomicMessagingOperationNotSupported);
        }
        let shadow = binding.owner().dead_letter_queue()?;
        if self.queue_config(binding.namespace(), &shadow)? != Some(config.dead_letter_shadow())
            || self
                .store
                .get(&keys::topic_config(binding.namespace(), &shadow))?
                .is_some()
        {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        self.validate_capacity_binding_profile(
            binding.namespace(),
            binding.target(),
            binding.owner(),
            binding.kind(),
        )
    }

    /// Applies all allowed operations through one bounded read-your-writes view,
    /// then commits exactly one normalized batch to the backing store.
    pub fn apply_atomic_messaging(
        &self,
        envelope: &AtomicMessagingCommand,
    ) -> Result<AtomicMessagingApplication, BrokerError> {
        validate_atomic_prelude(envelope)?;
        let overlay = AtomicOverlay::for_queue(
            self.store.clone(),
            envelope.binding.namespace(),
            envelope.binding.owner(),
        );
        let machine = StateMachine::new(overlay.clone());
        machine
            .validate_atomic_inner(envelope)
            .map_err(|error| overlay.map_error(error))?;
        let mut outcomes = Vec::with_capacity(envelope.commands.len());
        for command in &envelope.commands {
            let prepared = machine
                .prepare_command(command)
                .map_err(|error| overlay.map_error(error))?;
            overlay.stage(prepared.batch)?;
            outcomes.push(prepared.application.outcome);
        }
        let batch = overlay.finish()?;
        let enqueue_targets = atomic_enqueue_targets(envelope, &batch)?;
        if !batch.is_empty() {
            self.store.apply(batch)?;
        }
        Ok(AtomicMessagingApplication {
            outcomes,
            enqueue_targets,
        })
    }
}

fn atomic_enqueue_targets(
    envelope: &AtomicMessagingCommand,
    batch: &WriteBatch,
) -> Result<Vec<EntityPath>, BrokerError> {
    let source = envelope.binding.target();
    let shadow = source.dead_letter_queue()?;
    let mut targets = Vec::new();
    for target in [source, &shadow] {
        let prefix = keys::ready_prefix(envelope.binding.namespace(), target);
        if batch.mutations().iter().any(|mutation| {
            matches!(mutation, Mutation::Put { key, .. } if key.len() == prefix.len() + 8 && key.starts_with(&prefix))
        }) {
            targets.push(target.clone());
        }
    }
    targets.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    Ok(targets)
}

#[cfg(test)]
mod tests;
