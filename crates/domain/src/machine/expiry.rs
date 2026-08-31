//! Message lock and time-to-live expiry transitions.

use storage::{StateStore, WriteBatch};

use crate::{BrokerError, Command, CommandOutcome, DeadLetterReason, MessageState, keys};

use super::{StateMachine, TIMER_SCAN_LIMIT, exceeded_delivery_limit};

impl<S: StateStore> StateMachine<S> {
    pub(super) fn expire_locks(
        &self,
        command: &Command,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let config = self.load_config(command)?;
        let namespace = &command.namespace;
        let entity = &command.entity;
        let locks = self
            .store()
            .scan_prefix(&keys::lock_prefix(namespace, entity), TIMER_SCAN_LIMIT)?;

        let mut returned_to_ready = 0;
        let mut dead_lettered = 0;
        for (key, _) in locks {
            let (locked_until, sequence) =
                keys::trailing_deadline(&key).ok_or(BrokerError::MalformedIndexKey)?;
            if locked_until > command.issued_at {
                break;
            }

            let record = self
                .message(namespace, entity, sequence)?
                .ok_or(BrokerError::DanglingIndexEntry { sequence })?;
            let origin = match record.state {
                MessageState::Locked { origin, .. } => origin,
                _ => return Err(BrokerError::MessageNotLocked { sequence }),
            };

            if record.is_expired_at(command.issued_at) {
                self.move_to_dead_letter(
                    command,
                    record,
                    DeadLetterReason::TimeToLiveExpired,
                    String::from("the message exceeded its time to live"),
                    batch,
                )?;
                dead_lettered += 1;
            } else if exceeded_delivery_limit(command, &config, &record) {
                self.move_to_dead_letter(
                    command,
                    record,
                    DeadLetterReason::MaxDeliveryCountExceeded,
                    String::from("the message reached its maximum delivery count"),
                    batch,
                )?;
                dead_lettered += 1;
            } else {
                self.return_unsettled(command, record, locked_until, origin, batch)?;
                returned_to_ready += 1;
            }
        }

        Ok(CommandOutcome::LocksExpired {
            returned_to_ready,
            dead_lettered,
        })
    }

    pub(super) fn expire_messages(
        &self,
        command: &Command,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let namespace = &command.namespace;
        let entity = &command.entity;
        let expiring = self
            .store()
            .scan_prefix(&keys::expiry_prefix(namespace, entity), TIMER_SCAN_LIMIT)?;

        let mut dead_lettered = 0;
        for (key, _) in expiring {
            let (expires_at, sequence) =
                keys::trailing_deadline(&key).ok_or(BrokerError::MalformedIndexKey)?;
            if expires_at > command.issued_at {
                break;
            }

            let record = self
                .message(namespace, entity, sequence)?
                .ok_or(BrokerError::DanglingIndexEntry { sequence })?;
            match record.state {
                MessageState::Ready => {
                    self.move_to_dead_letter(
                        command,
                        record,
                        DeadLetterReason::TimeToLiveExpired,
                        String::from("the message exceeded its time to live"),
                        batch,
                    )?;
                    dead_lettered += 1;
                }
                // A lock protects a message from TTL until settlement, abandon,
                // or lock expiry. Deferred messages are checked only when a
                // client explicitly retrieves their sequence. Scheduled
                // messages never enter the expiry index before activation.
                MessageState::Locked { .. } | MessageState::Deferred | MessageState::Scheduled => {
                    batch.push_delete(key);
                }
            }
        }

        Ok(CommandOutcome::MessagesExpired { dead_lettered })
    }
}
