//! Owned work reservations for trusted atomic messaging submissions.

use std::{
    fmt,
    sync::{Arc, Mutex},
};

use domain::{AtomicMessagingInputUsage, BrokerError, CommandKind, EntityBinding};

use crate::{AtomicCommitPermit, AtomicCommitTicket};

pub const MAX_ATOMIC_WORK_GROUPS: usize = 32;
pub const MAX_ATOMIC_WORK_CONTENT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_ATOMIC_WORK_VALUE_ITEMS: usize = 131_072;

/// Payload-free counters shared by one work budget's observers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AtomicMessagingWorkUsage {
    groups: usize,
    content_bytes: usize,
    value_items: usize,
}

impl AtomicMessagingWorkUsage {
    pub const fn groups(&self) -> usize {
        self.groups
    }

    pub const fn content_bytes(&self) -> usize {
        self.content_bytes
    }

    pub const fn value_items(&self) -> usize {
        self.value_items
    }
}

/// Refusal to retain more work. Input failures preserve the domain's typed cause.
#[derive(Debug, thiserror::Error)]
pub enum AtomicMessagingWorkError {
    #[error("atomic messaging work exceeds {maximum} groups")]
    Group { maximum: usize },
    #[error("atomic messaging work exceeds {maximum_bytes} content bytes")]
    Content { maximum_bytes: usize },
    #[error("atomic messaging work exceeds {maximum} message value items")]
    ValueItem { maximum: usize },
    #[error("atomic messaging input was refused: {0}")]
    Input(#[source] BrokerError),
    #[error("atomic messaging work reservation is unavailable")]
    Unavailable,
}

/// Clonable observation and admission for a fixed, shared work budget.
///
/// Observers contain no commands, and their destruction does not release work.
/// The content tally is the domain input tally, not serialized size or an RSS
/// bound. Admission does not establish identity, authorization, or queue policy.
#[derive(Clone, Default)]
pub struct AtomicMessagingWorkBudget {
    usage: Arc<Mutex<AtomicMessagingWorkUsage>>,
}

impl AtomicMessagingWorkBudget {
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes a diagnostic snapshot, including after a poisoned admission lock.
    /// New reservations fail closed on poison; existing leases still refund.
    pub fn usage(&self) -> AtomicMessagingWorkUsage {
        *self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Reserves one slot immediately, including for an empty command group.
    pub fn stage(
        &self,
        binding: EntityBinding,
    ) -> Result<StagedAtomicMessaging, AtomicMessagingWorkError> {
        {
            let mut usage = self
                .usage
                .lock()
                .map_err(|_| AtomicMessagingWorkError::Unavailable)?;
            if usage.groups >= MAX_ATOMIC_WORK_GROUPS {
                return Err(AtomicMessagingWorkError::Group {
                    maximum: MAX_ATOMIC_WORK_GROUPS,
                });
            }
            usage.groups += 1;
        }
        Ok(StagedAtomicMessaging {
            binding,
            commands: Vec::new(),
            usage: AtomicMessagingInputUsage::default(),
            lease: WorkLease {
                budget: self.clone(),
                content_bytes: 0,
                value_items: 0,
            },
        })
    }
}

impl fmt::Debug for AtomicMessagingWorkBudget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AtomicMessagingWorkBudget")
            .field("usage", &self.usage())
            .finish()
    }
}

/// One non-clonable staging group whose slot stays reserved even when empty.
pub struct StagedAtomicMessaging {
    binding: EntityBinding,
    // Declaration order drops payloads before their reservation is refunded.
    commands: Vec<CommandKind>,
    usage: AtomicMessagingInputUsage,
    lease: WorkLease,
}

impl StagedAtomicMessaging {
    pub fn binding(&self) -> &EntityBinding {
        &self.binding
    }

    pub fn usage(&self) -> AtomicMessagingInputUsage {
        self.usage
    }

    /// Consumes a candidate, retaining it only after all input and shared caps
    /// pass. A refused candidate is dropped outside the admission lock.
    pub fn try_push(&mut self, kind: CommandKind) -> Result<(), AtomicMessagingWorkError> {
        let mut next = self.usage;
        next.try_extend(&kind)
            .map_err(AtomicMessagingWorkError::Input)?;
        self.commands
            .try_reserve(1)
            .map_err(|_| AtomicMessagingWorkError::Unavailable)?;
        self.lease.try_charge(next)?;
        self.usage = next;
        self.commands.push(kind);
        Ok(())
    }

    /// Moves commands and their reservation together into unique queue work.
    pub fn into_submission(self, ticket: AtomicCommitTicket) -> OwnedAtomicMessagingSubmission {
        OwnedAtomicMessagingSubmission {
            binding: self.binding,
            commands: self.commands,
            ticket,
            usage: self.usage,
            lease: self.lease,
        }
    }
}

impl fmt::Debug for StagedAtomicMessaging {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StagedAtomicMessaging")
            .field("usage", &self.usage)
            .finish_non_exhaustive()
    }
}

/// Unique queued work. Its permit observers never retain its commands or lease.
///
/// ```compile_fail
/// fn duplicate(work: protocol_amqp::OwnedAtomicMessagingSubmission) {
///     let _second = work.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn submit_twice(work: protocol_amqp::OwnedAtomicMessagingSubmission) {
///     let _first = work.into_owner_parts();
///     let _second = work.into_owner_parts();
/// }
/// ```
pub struct OwnedAtomicMessagingSubmission {
    binding: EntityBinding,
    commands: Vec<CommandKind>,
    ticket: AtomicCommitTicket,
    usage: AtomicMessagingInputUsage,
    lease: WorkLease,
}

impl OwnedAtomicMessagingSubmission {
    pub fn permit(&self) -> &AtomicCommitPermit {
        self.ticket.permit()
    }

    /// The owner retains the returned work through its entire commit scope.
    pub fn into_owner_parts(self) -> (EntityBinding, AtomicCommitTicket, AtomicMessagingOwnerWork) {
        (
            self.binding,
            self.ticket,
            AtomicMessagingOwnerWork {
                commands: self.commands,
                usage: self.usage,
                taken: false,
                _lease: self.lease,
            },
        )
    }
}

impl fmt::Debug for OwnedAtomicMessagingSubmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnedAtomicMessagingSubmission")
            .field("usage", &self.usage)
            .field("permit", self.permit())
            .finish_non_exhaustive()
    }
}

/// Non-clonable owner scope that keeps the charge after commands are taken.
///
/// This is a trusted Rust ownership contract, not proof of native receipt
/// authority. A callback must not return or retain commands beyond this work's
/// lifetime. No budget lock is held while running it or dropping its payload.
pub struct AtomicMessagingOwnerWork {
    commands: Vec<CommandKind>,
    usage: AtomicMessagingInputUsage,
    taken: bool,
    _lease: WorkLease,
}

impl AtomicMessagingOwnerWork {
    /// Takes commands once while retaining the lease through the callback and
    /// subsequent owner scope. A second call is refused even if the first
    /// callback panicked; the lease stays charged until work is destroyed.
    pub fn with_commands<R>(
        &mut self,
        callback: impl FnOnce(Vec<CommandKind>) -> R,
    ) -> Result<R, AtomicMessagingWorkError> {
        if self.taken {
            return Err(AtomicMessagingWorkError::Unavailable);
        }
        self.taken = true;
        Ok(callback(std::mem::take(&mut self.commands)))
    }
}

impl fmt::Debug for AtomicMessagingOwnerWork {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AtomicMessagingOwnerWork")
            .field("usage", &self.usage)
            .field("commands_taken", &self.taken)
            .finish_non_exhaustive()
    }
}

struct WorkLease {
    budget: AtomicMessagingWorkBudget,
    content_bytes: usize,
    value_items: usize,
}

impl WorkLease {
    fn try_charge(
        &mut self,
        next: AtomicMessagingInputUsage,
    ) -> Result<(), AtomicMessagingWorkError> {
        let content_delta = next.content_bytes() - self.content_bytes;
        let value_delta = next.value_items() - self.value_items;
        let mut usage = self
            .budget
            .usage
            .lock()
            .map_err(|_| AtomicMessagingWorkError::Unavailable)?;
        let content_bytes = usage
            .content_bytes
            .checked_add(content_delta)
            .filter(|total| *total <= MAX_ATOMIC_WORK_CONTENT_BYTES)
            .ok_or(AtomicMessagingWorkError::Content {
                maximum_bytes: MAX_ATOMIC_WORK_CONTENT_BYTES,
            })?;
        let value_items = usage
            .value_items
            .checked_add(value_delta)
            .filter(|total| *total <= MAX_ATOMIC_WORK_VALUE_ITEMS)
            .ok_or(AtomicMessagingWorkError::ValueItem {
                maximum: MAX_ATOMIC_WORK_VALUE_ITEMS,
            })?;
        usage.content_bytes = content_bytes;
        usage.value_items = value_items;
        self.content_bytes = next.content_bytes();
        self.value_items = next.value_items();
        Ok(())
    }
}

impl Drop for WorkLease {
    fn drop(&mut self) {
        let mut usage = self
            .budget
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        usage.groups -= 1;
        usage.content_bytes -= self.content_bytes;
        usage.value_items -= self.value_items;
    }
}

#[cfg(test)]
mod tests;
