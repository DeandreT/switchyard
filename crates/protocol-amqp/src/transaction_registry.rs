//! Serialized, connection-local staging for trusted future transaction owners.
//! This does not accept wire declarations or establish native delivery authority.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use amqp::TransactionId;
use domain::{CommandKind, EntityBinding, EntityIncarnationKind};

use crate::{
    AtomicCommitPermit, AtomicCommitState, AtomicCommitTicket, AtomicMessagingWorkBudget,
    AtomicMessagingWorkError, AtomicMessagingWorkUsage, MAX_ATOMIC_WORK_GROUPS,
    OwnedAtomicMessagingSubmission, OwnedEmptyAtomicMessagingSubmission,
    atomic_work::UnboundAtomicMessaging,
};

pub const MAX_ATOMIC_TRANSACTION_TERMINALS: usize = 32;
/// Local runtime policy, measured monotonically from declaration, not first work.
pub const ATOMIC_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(120);

static PROCESS_IDS: OnceLock<Arc<AtomicU64>> = OnceLock::new();

#[derive(Debug, thiserror::Error)]
pub enum AtomicTransactionRegistryError {
    #[error("the atomic transaction registry is closed")]
    Closed,
    #[error("the atomic transaction controller is invalid or closed")]
    InvalidController,
    #[error("the atomic transaction identifier is unknown")]
    UnknownId,
    #[error("atomic transaction work requires one primary queue target")]
    UnsupportedTarget,
    #[error("atomic transaction work does not match its established binding")]
    BindingMismatch,
    #[error("atomic transaction discharge contradicts its first fail flag")]
    DischargeConflict,
    #[error("the atomic transaction identifier space is exhausted")]
    IdExhausted,
    #[error("atomic transaction staging is unavailable")]
    Unavailable,
    #[error(transparent)]
    Work(#[from] AtomicMessagingWorkError),
}

struct Connection {
    active: AtomicBool,
}

struct Controller {
    connection: Arc<Connection>,
    generation: u64,
    active: AtomicBool,
}

/// Opaque controller provenance. Cloning or dropping a token never closes it.
/// Explicit close, or destruction of its registry owner, invalidates it.
#[derive(Clone)]
pub struct AtomicTransactionController {
    inner: Arc<Controller>,
}

impl AtomicTransactionController {
    pub fn is_active(&self) -> bool {
        self.inner.connection.active.load(Ordering::Acquire)
            && self.inner.active.load(Ordering::Acquire)
    }
}

impl fmt::Debug for AtomicTransactionController {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AtomicTransactionController")
            .field("generation", &self.inner.generation)
            .field("active", &self.is_active())
            .finish()
    }
}

/// Inert, payload-free observation, not another registry owner or admission API.
/// Counts may stay positive after close while queued or started work owns leases.
/// Clone counts and the content tally are not process-memory guarantees.
#[derive(Clone)]
pub struct AtomicTransactionRegistryHandle {
    connection: Arc<Connection>,
    budget: AtomicMessagingWorkBudget,
}

impl AtomicTransactionRegistryHandle {
    pub fn is_active(&self) -> bool {
        self.connection.active.load(Ordering::Acquire)
    }

    pub fn work_usage(&self) -> AtomicMessagingWorkUsage {
        self.budget.usage()
    }
}

impl fmt::Debug for AtomicTransactionRegistryHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AtomicTransactionRegistryHandle")
            .field("active", &self.is_active())
            .field("work_usage", &self.work_usage())
            .finish()
    }
}

/// One uniquely owned queue handoff. Empty means zero staged actions, not zero
/// content or zero messages. An accepted empty batch is a bound action.
///
/// ```compile_fail
/// fn duplicate(work: protocol_amqp::AtomicTransactionSubmission) {
///     let _second = work.clone();
/// }
/// ```
pub enum AtomicTransactionSubmission {
    Bound(OwnedAtomicMessagingSubmission),
    Empty(OwnedEmptyAtomicMessagingSubmission),
}

impl AtomicTransactionSubmission {
    pub fn permit(&self) -> &AtomicCommitPermit {
        match self {
            Self::Bound(submission) => submission.permit(),
            Self::Empty(submission) => submission.permit(),
        }
    }

    /// Tightens the unique handoff ticket without retaining an expiry on its
    /// payload-free permit observers.
    pub fn restrict_claim_expiry_epoch_seconds(&mut self, expiry: u64) {
        match self {
            Self::Bound(submission) => submission.restrict_claim_expiry_epoch_seconds(expiry),
            Self::Empty(submission) => submission.restrict_claim_expiry_epoch_seconds(expiry),
        }
    }
}

impl fmt::Debug for AtomicTransactionSubmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bound(submission) => formatter.debug_tuple("Bound").field(submission).finish(),
            Self::Empty(submission) => formatter.debug_tuple("Empty").field(submission).finish(),
        }
    }
}

/// The first commit request moves unique work out. Repeats only report state;
/// they never return another submission or restore forgotten terminal entries.
#[derive(Debug)]
pub enum AtomicTransactionDischarge {
    Submit(AtomicTransactionSubmission),
    State(AtomicCommitState),
}

enum Phase {
    Staging {
        binding: Option<EntityBinding>,
        ticket: AtomicCommitTicket,
        work: UnboundAtomicMessaging,
    },
    Queued,
}

struct Entry {
    controller: u64,
    deadline: Instant,
    permit: AtomicCommitPermit,
    first_fail: Option<bool>,
    abort_cause: Option<AbortCause>,
    phase: Phase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AbortCause {
    Requested,
    StagingRefused,
    TimedOut,
    ControllerClosed,
    ConnectionClosed,
    Canceled,
    Unavailable,
}

#[derive(Clone, Copy)]
struct Terminal {
    id: u64,
    controller: u64,
    state: AtomicCommitState,
    first_fail: Option<bool>,
    abort_cause: Option<AbortCause>,
}

/// One serialized owner for a connection. All mutation requires `&mut self`;
/// future concurrent wire tasks still need their own bounded ordering boundary.
///
/// The private budget bounds 32 outstanding leases, including aborted queued
/// work. Terminal history retains at most 32 compact records and no payloads.
/// Process-local checked IDs are not reused in this process; no persistence or
/// cross-restart uniqueness is claimed. No controller list is retained.
///
/// ```compile_fail
/// fn duplicate(owner: protocol_amqp::AtomicTransactionRegistry) {
///     let _second = owner.clone();
/// }
/// ```
pub struct AtomicTransactionRegistry {
    connection: Arc<Connection>,
    budget: AtomicMessagingWorkBudget,
    ids: Arc<AtomicU64>,
    last_controller: u64,
    entries: BTreeMap<u64, Entry>,
    terminals: VecDeque<Terminal>,
}

impl AtomicTransactionRegistry {
    pub fn new() -> (Self, AtomicTransactionRegistryHandle) {
        Self::with_ids(Arc::clone(
            PROCESS_IDS.get_or_init(|| Arc::new(AtomicU64::new(0))),
        ))
    }

    fn with_ids(ids: Arc<AtomicU64>) -> (Self, AtomicTransactionRegistryHandle) {
        let connection = Arc::new(Connection {
            active: AtomicBool::new(true),
        });
        let budget = AtomicMessagingWorkBudget::new();
        let handle = AtomicTransactionRegistryHandle {
            connection: Arc::clone(&connection),
            budget: budget.clone(),
        };
        (
            Self {
                connection,
                budget,
                ids,
                last_controller: 0,
                entries: BTreeMap::new(),
                terminals: VecDeque::new(),
            },
            handle,
        )
    }

    pub fn controller(
        &mut self,
    ) -> Result<AtomicTransactionController, AtomicTransactionRegistryError> {
        self.require_active()?;
        let generation = self
            .last_controller
            .checked_add(1)
            .ok_or(AtomicTransactionRegistryError::IdExhausted)?;
        self.last_controller = generation;
        Ok(AtomicTransactionController {
            inner: Arc::new(Controller {
                connection: Arc::clone(&self.connection),
                generation,
                active: AtomicBool::new(true),
            }),
        })
    }

    /// Reserves an unbound slot immediately. This trusted runtime method does
    /// not accept a wire Declare or authenticate a coordinator link.
    pub fn declare(
        &mut self,
        controller: &AtomicTransactionController,
    ) -> Result<TransactionId, AtomicTransactionRegistryError> {
        self.declare_at(controller, Instant::now())
    }

    fn declare_at(
        &mut self,
        controller: &AtomicTransactionController,
        now: Instant,
    ) -> Result<TransactionId, AtomicTransactionRegistryError> {
        self.require_controller(controller)?;
        self.reap_at(now);
        if self.entries.len() >= MAX_ATOMIC_WORK_GROUPS {
            return Err(AtomicMessagingWorkError::Group {
                maximum: MAX_ATOMIC_WORK_GROUPS,
            }
            .into());
        }
        let deadline = now
            .checked_add(ATOMIC_TRANSACTION_TIMEOUT)
            .ok_or(AtomicTransactionRegistryError::Unavailable)?;
        let work = self.budget.stage_unbound()?;
        let previous = self
            .ids
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(1)
            })
            .map_err(|_| AtomicTransactionRegistryError::IdExhausted)?;
        let id = previous
            .checked_add(1)
            .ok_or(AtomicTransactionRegistryError::IdExhausted)?;
        let wire_id = TransactionId::new(id.to_be_bytes())
            .map_err(|_| AtomicTransactionRegistryError::Unavailable)?;
        let (permit, ticket) = AtomicCommitPermit::new(deadline);
        self.entries.insert(
            id,
            Entry {
                controller: controller.inner.generation,
                deadline,
                permit,
                first_fail: None,
                abort_cause: None,
                phase: Phase::Staging {
                    binding: None,
                    ticket,
                    work,
                },
            },
        );
        Ok(wire_id)
    }

    /// Pins the exact binding only after the first successful action. Pure
    /// staging does not prove authorization, live configuration, or lock state.
    pub fn try_stage(
        &mut self,
        controller: &AtomicTransactionController,
        id: &TransactionId,
        binding: EntityBinding,
        kind: CommandKind,
    ) -> Result<(), AtomicTransactionRegistryError> {
        self.try_stage_at(controller, id, binding, kind, Instant::now())
    }

    fn try_stage_at(
        &mut self,
        controller: &AtomicTransactionController,
        id: &TransactionId,
        binding: EntityBinding,
        kind: CommandKind,
        now: Instant,
    ) -> Result<(), AtomicTransactionRegistryError> {
        self.require_controller(controller)?;
        self.reap_at(now);
        let id = Self::id_key(id)?;
        let entry = self
            .entries
            .get_mut(&id)
            .filter(|entry| entry.controller == controller.inner.generation)
            .ok_or(AtomicTransactionRegistryError::UnknownId)?;
        if entry.first_fail.is_some() || entry.permit.state() != AtomicCommitState::Pending {
            return Err(AtomicTransactionRegistryError::Unavailable);
        }
        let Phase::Staging {
            binding: established,
            work,
            ..
        } = &mut entry.phase
        else {
            return Err(AtomicTransactionRegistryError::Unavailable);
        };
        if binding.kind() != EntityIncarnationKind::Queue
            || binding.target() != binding.owner()
            || binding.owner().is_dead_letter_queue()
            || binding.owner().is_subscription_path()
        {
            return Err(AtomicTransactionRegistryError::UnsupportedTarget);
        }
        if established
            .as_ref()
            .is_some_and(|current| current != &binding)
        {
            return Err(AtomicTransactionRegistryError::BindingMismatch);
        }
        work.try_push(kind)?;
        if established.is_none() {
            *established = Some(binding);
        }
        Ok(())
    }

    pub fn discharge(
        &mut self,
        controller: &AtomicTransactionController,
        id: &TransactionId,
        fail: bool,
    ) -> Result<AtomicTransactionDischarge, AtomicTransactionRegistryError> {
        self.discharge_at(controller, id, fail, Instant::now())
    }

    fn discharge_at(
        &mut self,
        controller: &AtomicTransactionController,
        id: &TransactionId,
        fail: bool,
        now: Instant,
    ) -> Result<AtomicTransactionDischarge, AtomicTransactionRegistryError> {
        self.require_controller(controller)?;
        self.reap_at(now);
        let id = Self::id_key(id)?;
        if let Some(terminal) = self.terminals.iter_mut().find(|terminal| {
            terminal.id == id && terminal.controller == controller.inner.generation
        }) {
            Self::record_fail(&mut terminal.first_fail, fail)?;
            return Ok(AtomicTransactionDischarge::State(terminal.state));
        }
        let entry = self
            .entries
            .get_mut(&id)
            .filter(|entry| entry.controller == controller.inner.generation)
            .ok_or(AtomicTransactionRegistryError::UnknownId)?;
        Self::record_fail(&mut entry.first_fail, fail)?;
        if matches!(entry.phase, Phase::Queued) {
            return Ok(AtomicTransactionDischarge::State(entry.permit.state()));
        }
        if fail {
            if entry.permit.abort() {
                entry.abort_cause = Some(AbortCause::Requested);
            }
            let state = entry.permit.state();
            self.reap_at(now);
            return Ok(AtomicTransactionDischarge::State(state));
        }
        let entry = self
            .entries
            .remove(&id)
            .ok_or(AtomicTransactionRegistryError::UnknownId)?;
        let Entry {
            controller,
            deadline,
            permit,
            first_fail,
            abort_cause,
            phase,
        } = entry;
        let Phase::Staging {
            binding,
            ticket,
            work,
        } = phase
        else {
            return Err(AtomicTransactionRegistryError::Unavailable);
        };
        let submission = match binding {
            Some(binding) => {
                AtomicTransactionSubmission::Bound(work.into_bound_submission(binding, ticket))
            }
            None => match work.into_empty_submission(ticket) {
                Ok(submission) => AtomicTransactionSubmission::Empty(submission),
                Err(error) => {
                    self.remember_terminal(Terminal {
                        id,
                        controller,
                        state: permit.state(),
                        first_fail,
                        abort_cause: Some(AbortCause::Unavailable),
                    });
                    return Err(error.into());
                }
            },
        };
        self.entries.insert(
            id,
            Entry {
                controller,
                deadline,
                permit,
                first_fail,
                abort_cause,
                phase: Phase::Queued,
            },
        );
        Ok(AtomicTransactionDischarge::Submit(submission))
    }

    /// A serialized diagnostic snapshot. Closed controllers can still observe
    /// their own retained state; foreign controller provenance is always refused.
    pub fn state(
        &mut self,
        controller: &AtomicTransactionController,
        id: &TransactionId,
    ) -> Result<AtomicCommitState, AtomicTransactionRegistryError> {
        self.state_at(controller, id, Instant::now())
    }

    fn state_at(
        &mut self,
        controller: &AtomicTransactionController,
        id: &TransactionId,
        now: Instant,
    ) -> Result<AtomicCommitState, AtomicTransactionRegistryError> {
        self.require_provenance(controller)?;
        self.reap_at(now);
        let id = Self::id_key(id)?;
        if let Some(entry) = self
            .entries
            .get(&id)
            .filter(|entry| entry.controller == controller.inner.generation)
        {
            return Ok(entry.permit.state());
        }
        self.terminals
            .iter()
            .find(|terminal| {
                terminal.id == id && terminal.controller == controller.inner.generation
            })
            .map(|terminal| terminal.state)
            .ok_or(AtomicTransactionRegistryError::UnknownId)
    }

    /// Aborts only this controller's still-pending group and reports its state.
    ///
    /// This trusted cleanup does not manufacture a discharge request or change
    /// its first fail flag. Started work and final decisions are not reversed.
    /// Exact provenance remains required after controller or connection close.
    /// Queued work retains its outside owner's lease until that work is dropped.
    /// Normal deadline reaping still applies to other groups.
    pub fn abort_pending(
        &mut self,
        controller: &AtomicTransactionController,
        id: &TransactionId,
    ) -> Result<AtomicCommitState, AtomicTransactionRegistryError> {
        self.abort_pending_at(controller, id, Instant::now())
    }

    fn abort_pending_at(
        &mut self,
        controller: &AtomicTransactionController,
        id: &TransactionId,
        now: Instant,
    ) -> Result<AtomicCommitState, AtomicTransactionRegistryError> {
        self.require_provenance(controller)?;
        self.reap_at(now);
        let id = Self::id_key(id)?;
        if let Some(terminal) = self.terminals.iter().find(|terminal| {
            terminal.id == id && terminal.controller == controller.inner.generation
        }) {
            return Ok(terminal.state);
        }
        let entry = self
            .entries
            .get_mut(&id)
            .filter(|entry| entry.controller == controller.inner.generation)
            .ok_or(AtomicTransactionRegistryError::UnknownId)?;
        if entry.permit.abort() {
            entry.abort_cause = Some(AbortCause::StagingRefused);
        }
        let state = entry.permit.state();
        self.reap_at(now);
        Ok(state)
    }

    /// Reaps completed or expired transactions, returning the number retired.
    /// There is no background timer in this foundation; owners must call this
    /// or another registry operation to reap idle local staging.
    pub fn expire(&mut self) -> usize {
        self.reap_at(Instant::now())
    }

    pub fn close_controller(
        &mut self,
        controller: &AtomicTransactionController,
    ) -> Result<(), AtomicTransactionRegistryError> {
        self.close_controller_at(controller, Instant::now())
    }

    fn close_controller_at(
        &mut self,
        controller: &AtomicTransactionController,
        now: Instant,
    ) -> Result<(), AtomicTransactionRegistryError> {
        self.require_provenance(controller)?;
        controller.inner.active.store(false, Ordering::Release);
        for entry in self.entries.values_mut() {
            if entry.controller == controller.inner.generation && entry.permit.abort() {
                entry.abort_cause = Some(AbortCause::ControllerClosed);
            }
        }
        self.reap_at(now);
        Ok(())
    }

    /// Invalidates every controller without retaining a controller list.
    /// Pending work is aborted; a started owner remains free to finalize.
    pub fn close(&mut self) {
        self.close_at(Instant::now());
    }

    fn close_at(&mut self, now: Instant) {
        self.connection.active.store(false, Ordering::Release);
        for entry in self.entries.values_mut() {
            if entry.permit.abort() {
                entry.abort_cause = Some(AbortCause::ConnectionClosed);
            }
        }
        self.reap_at(now);
    }

    fn require_active(&self) -> Result<(), AtomicTransactionRegistryError> {
        if self.connection.active.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(AtomicTransactionRegistryError::Closed)
        }
    }

    fn require_provenance(
        &self,
        controller: &AtomicTransactionController,
    ) -> Result<(), AtomicTransactionRegistryError> {
        if Arc::ptr_eq(&self.connection, &controller.inner.connection) {
            Ok(())
        } else {
            Err(AtomicTransactionRegistryError::InvalidController)
        }
    }

    fn require_controller(
        &self,
        controller: &AtomicTransactionController,
    ) -> Result<(), AtomicTransactionRegistryError> {
        self.require_active()?;
        self.require_provenance(controller)?;
        if controller.inner.active.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(AtomicTransactionRegistryError::InvalidController)
        }
    }

    fn id_key(id: &TransactionId) -> Result<u64, AtomicTransactionRegistryError> {
        let bytes = id
            .as_bytes()
            .try_into()
            .map_err(|_| AtomicTransactionRegistryError::UnknownId)?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn record_fail(
        first: &mut Option<bool>,
        fail: bool,
    ) -> Result<(), AtomicTransactionRegistryError> {
        match *first {
            Some(previous) if previous != fail => {
                Err(AtomicTransactionRegistryError::DischargeConflict)
            }
            Some(_) => Ok(()),
            None => {
                *first = Some(fail);
                Ok(())
            }
        }
    }

    fn reap_at(&mut self, now: Instant) -> usize {
        let mut finished = Vec::new();
        for (&id, entry) in &mut self.entries {
            if now >= entry.deadline && entry.permit.abort() {
                entry.abort_cause = Some(AbortCause::TimedOut);
            }
            if matches!(
                entry.permit.state(),
                AtomicCommitState::Aborted
                    | AtomicCommitState::Committed
                    | AtomicCommitState::Rejected
                    | AtomicCommitState::Indeterminate
            ) {
                finished.push(id);
            }
        }
        let count = finished.len();
        for id in finished {
            if let Some(entry) = self.entries.remove(&id) {
                let state = entry.permit.state();
                self.remember_terminal(Terminal {
                    id,
                    controller: entry.controller,
                    state,
                    first_fail: entry.first_fail,
                    abort_cause: if state == AtomicCommitState::Aborted {
                        entry.abort_cause.or(Some(AbortCause::Canceled))
                    } else {
                        None
                    },
                });
                // Staged payloads and leases are destroyed without a registry
                // or budget lock; queued entries own only permit observations.
                drop(entry);
            }
        }
        count
    }

    fn remember_terminal(&mut self, terminal: Terminal) {
        if self.terminals.len() == MAX_ATOMIC_TRANSACTION_TERMINALS {
            self.terminals.pop_front();
        }
        self.terminals.push_back(terminal);
    }
}

impl fmt::Debug for AtomicTransactionRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AtomicTransactionRegistry")
            .field("active", &self.connection.active.load(Ordering::Acquire))
            .field("live_entries", &self.entries.len())
            .field("terminal_entries", &self.terminals.len())
            .field(
                "terminal_aborts",
                &self
                    .terminals
                    .iter()
                    .filter(|terminal| terminal.abort_cause.is_some())
                    .count(),
            )
            .field("work_usage", &self.budget.usage())
            .finish()
    }
}

impl Drop for AtomicTransactionRegistry {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests;
