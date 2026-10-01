use domain::{
    AtomicMessagingApplication, AtomicMessagingCommand, EntityBinding,
    validate_atomic_messaging_kinds,
};

use super::*;

impl<S: StateStore, C: Clock> LocalProposer<S, C> {
    /// Applies a bounded group on one primary non-session queue. This trusted
    /// entry point provides neither authorization nor successful-retry deduplication.
    pub fn propose_atomic_messaging(
        &self,
        binding: &EntityBinding,
        kinds: Vec<CommandKind>,
    ) -> Result<AtomicMessagingApplication, ProposeError> {
        validate_atomic_messaging_kinds(&kinds)?;
        let mut command = AtomicMessagingCommand {
            binding: binding.clone(),
            issued_at: Timestamp::UNIX_EPOCH,
            commands: kinds
                .into_iter()
                .map(|kind| {
                    Command::new(
                        binding.namespace().clone(),
                        binding.target().clone(),
                        Timestamp::UNIX_EPOCH,
                        kind,
                    )
                })
                .collect(),
        };
        self.machine.validate_atomic_messaging(&command)?;
        if !command.commands.is_empty() {
            let issued_at = self.stamp()?;
            command.issued_at = issued_at;
            for member in &mut command.commands {
                member.issued_at = issued_at;
            }
        }
        Ok(self.machine.apply_atomic_messaging(&command)?)
    }
}

#[cfg(test)]
mod tests;
