//! Turning an intent into an applied command.
//!
//! This is the seam consensus slots into. On a single node the proposer stamps a
//! command and applies it here. Replicated, the same stamping happens on the
//! leader and the command is applied once the entry commits — the state
//! machine's contract does not change, because the timestamp is decided before
//! replication rather than after it.

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, EntityPath, NamespaceName, StateMachine,
    Timestamp,
};
use storage::StateStore;
use thiserror::Error;

use crate::Clock;

/// How far the host clock may step backward before the proposer refuses.
///
/// Ordinary clock discipline moves time by small amounts. A jump past this is
/// not discipline, and stamping through it would either regress the log or
/// invent deadlines from a time the node no longer believes in.
pub const DEFAULT_MAX_CLOCK_REGRESSION_MILLIS: u64 = 500;

pub struct LocalProposer<S, C> {
    machine: StateMachine<S>,
    clock: C,
    max_clock_regression_millis: u64,
}

impl<S: StateStore, C: Clock> LocalProposer<S, C> {
    pub fn new(machine: StateMachine<S>, clock: C) -> Self {
        Self {
            machine,
            clock,
            max_clock_regression_millis: DEFAULT_MAX_CLOCK_REGRESSION_MILLIS,
        }
    }

    pub fn with_max_clock_regression_millis(mut self, millis: u64) -> Self {
        self.max_clock_regression_millis = millis;
        self
    }

    pub fn machine(&self) -> &StateMachine<S> {
        &self.machine
    }

    /// Stamps `kind` with the current time and applies it.
    pub fn propose(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, ProposeError> {
        let issued_at = self.stamp()?;
        let command = Command::new(namespace.clone(), entity.clone(), issued_at, kind);
        Ok(self.machine.apply(&command)?)
    }

    /// The timestamp to put on the next command.
    ///
    /// The state machine refuses a command that precedes the one it last
    /// applied, so a proposer that passed a backward reading straight through
    /// would stall the node. A small step backward reuses the applied timestamp
    /// instead: time stands still for a moment rather than moving either way.
    fn stamp(&self) -> Result<Timestamp, ProposeError> {
        let now = self.clock.now();
        let last_applied = self.machine.last_applied_time()?;
        if now >= last_applied {
            return Ok(now);
        }

        let regression_millis = last_applied.as_millis() - now.as_millis();
        if regression_millis > self.max_clock_regression_millis {
            return Err(ProposeError::ClockWentBackward {
                last_applied,
                now,
                allowed_millis: self.max_clock_regression_millis,
            });
        }
        Ok(last_applied)
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ProposeError {
    #[error(
        "host clock reads {now} but {last_applied} has been applied, a regression beyond the {allowed_millis}ms tolerance"
    )]
    ClockWentBackward {
        last_applied: Timestamp,
        now: Timestamp,
        allowed_millis: u64,
    },
    /// A command produced an outcome of the wrong shape, which means the
    /// proposer and the state machine disagree about what it does.
    #[error("command produced an unexpected outcome: {outcome}")]
    UnexpectedOutcome { outcome: String },
    #[error(transparent)]
    Broker(#[from] BrokerError),
}

#[cfg(test)]
mod tests {
    use domain::{QueueConfig, SequenceNumber};
    use storage::MemoryStore;

    use super::*;
    use crate::ManualClock;

    fn proposer(clock: ManualClock) -> LocalProposer<MemoryStore, ManualClock> {
        LocalProposer::new(StateMachine::new(MemoryStore::default()), clock)
    }

    fn names() -> (NamespaceName, EntityPath) {
        (
            NamespaceName::new("tenant").expect("a valid namespace"),
            EntityPath::new("orders").expect("a valid entity path"),
        )
    }

    fn create_queue(
        proposer: &LocalProposer<MemoryStore, ManualClock>,
    ) -> Result<(), ProposeError> {
        let (namespace, entity) = names();
        proposer.propose(
            &namespace,
            &entity,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        )?;
        Ok(())
    }

    fn send(
        proposer: &LocalProposer<MemoryStore, ManualClock>,
    ) -> Result<CommandOutcome, ProposeError> {
        let (namespace, entity) = names();
        proposer.propose(
            &namespace,
            &entity,
            CommandKind::Send {
                message_id: String::from("first"),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None,
                envelope: None,
            },
        )
    }

    #[test]
    fn stamps_commands_with_the_clock_it_was_given() -> Result<(), ProposeError> {
        let clock = ManualClock::at(1_000);
        let proposer = proposer(clock.clone());
        create_queue(&proposer)?;

        clock.set(2_000);
        send(&proposer)?;
        assert_eq!(
            proposer.machine().last_applied_time()?,
            Timestamp::from_millis(2_000)
        );
        Ok(())
    }

    #[test]
    fn a_small_step_backward_holds_time_still() -> Result<(), ProposeError> {
        let clock = ManualClock::at(1_000);
        let proposer = proposer(clock.clone());
        create_queue(&proposer)?;

        // Ordinary clock discipline. The command still applies, stamped at the
        // timestamp already applied rather than at a regressed one.
        clock.set(1_000 - DEFAULT_MAX_CLOCK_REGRESSION_MILLIS);
        assert_eq!(
            send(&proposer)?,
            CommandOutcome::Sent {
                sequence: SequenceNumber::new(1)
            }
        );
        assert_eq!(
            proposer.machine().last_applied_time()?,
            Timestamp::from_millis(1_000)
        );
        Ok(())
    }

    #[test]
    fn a_large_step_backward_is_refused_rather_than_stamped_through()
    -> Result<(), Box<dyn std::error::Error>> {
        let clock = ManualClock::at(10_000);
        let proposer = proposer(clock.clone());
        create_queue(&proposer)?;

        clock.set(10_000 - DEFAULT_MAX_CLOCK_REGRESSION_MILLIS - 1);
        assert_eq!(
            send(&proposer),
            Err(ProposeError::ClockWentBackward {
                last_applied: Timestamp::from_millis(10_000),
                now: Timestamp::from_millis(10_000 - DEFAULT_MAX_CLOCK_REGRESSION_MILLIS - 1),
                allowed_millis: DEFAULT_MAX_CLOCK_REGRESSION_MILLIS,
            })
        );
        // The refusal wrote nothing, so the node recovers by itself once the
        // host clock catches up.
        assert_eq!(
            proposer
                .machine()
                .ready_sequences(&names().0, &names().1, 16)?,
            Vec::new()
        );

        clock.set(10_001);
        assert!(send(&proposer).is_ok());
        Ok(())
    }
}
