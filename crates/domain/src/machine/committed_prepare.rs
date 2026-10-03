use crate::CommittedApplyError;

use super::*;

mod create;
mod metadata;
mod send;

pub(super) enum CommittedPreparationError {
    Refused(BrokerError),
    Fatal(CommittedApplyError),
}

impl CommittedPreparationError {
    fn refused(error: impl Into<BrokerError>) -> Self {
        Self::Refused(error.into())
    }

    fn business_state(error: impl Into<BrokerError>) -> Self {
        Self::Fatal(CommittedApplyError::BusinessState(error.into()))
    }
}

impl<S: StateStore> StateMachine<S> {
    /// Prepares only the typed committed queue subset, retaining error origins.
    /// The caller appends its checkpoint and commits this private batch once.
    pub(super) fn prepare_committed_queue(
        &self,
        command: &Command,
        watermark: Timestamp,
    ) -> Result<PreparedCommand, CommittedPreparationError> {
        let applied = self
            .last_applied_time()
            .map_err(CommittedPreparationError::business_state)?;
        if applied > watermark {
            return Err(CommittedPreparationError::Fatal(
                CommittedApplyError::BusinessClockAhead { applied, watermark },
            ));
        }
        let last_applied = applied.max(watermark);
        if command.issued_at < last_applied {
            return Err(CommittedPreparationError::Refused(
                BrokerError::ClockRegression {
                    last_applied,
                    proposed: command.issued_at,
                },
            ));
        }

        let mut batch = WriteBatch::default();
        let outcome = match &command.kind {
            CommandKind::CreateQueue { config } => {
                self.prepare_committed_create(command, *config, &mut batch)?
            }
            CommandKind::Send {
                message_id,
                body,
                time_to_live_millis,
                session_id,
            } => self.prepare_committed_send(
                command,
                MessageInput {
                    message_id,
                    body,
                    time_to_live_millis: *time_to_live_millis,
                    session_id: session_id.as_ref(),
                    envelope: None,
                },
                &mut batch,
            )?,
            _ => {
                return Err(CommittedPreparationError::business_state(
                    BrokerError::EntityKindMismatch,
                ));
            }
        };
        let dead_letters_enqueued = committed_dead_letter_put(command, &batch);
        if !batch.is_empty() {
            batch.push_put(
                keys::clock(),
                codec::encode(&command.issued_at)
                    .map_err(CommittedPreparationError::business_state)?,
            );
        }
        Ok(PreparedCommand {
            batch,
            application: CommandApplication {
                outcome,
                dead_letters_enqueued,
                subscription_enqueues: None,
                entity_deletions: None,
            },
        })
    }
}
