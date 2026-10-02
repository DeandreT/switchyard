use std::sync::{
    Mutex, MutexGuard, Weak,
    atomic::{AtomicBool, AtomicU8, Ordering},
};

use super::*;

mod receipts;
pub(in crate::server) use receipts::{
    ControlData, DischargeStatus, NativePartialPosting, NativeRoute, PostData,
};
pub use receipts::{
    PendingDeclareReceipt, PreparedPosting, SealedDischargeReceipt, TransactionPostingReceipt,
};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[derive(Default)]
struct RetirementMembers {
    closed: bool,
    controller: Option<Weak<Controller>>,
    groups: Vec<Weak<Group>>,
}

pub(in crate::server) struct NativeRetirementHook(Mutex<RetirementMembers>);

impl NativeRetirementHook {
    pub(in crate::server) fn new() -> Arc<Self> {
        Arc::new(Self(Mutex::new(RetirementMembers::default())))
    }
    pub(in crate::server) fn is_closed(&self) -> bool {
        lock(&self.0).closed
    }

    pub(in crate::server) fn controller(
        &self,
        controller: &Arc<Controller>,
    ) -> Result<(), NativeTransactionError> {
        let mut members = lock(&self.0);
        if members.closed {
            return Err(NativeTransactionError::Retired);
        }
        members.controller = Some(Arc::downgrade(controller));
        Ok(())
    }

    pub(in crate::server) fn track(
        &self,
        group: &Arc<Group>,
    ) -> Result<(), NativeTransactionError> {
        let mut members = lock(&self.0);
        if members.closed {
            return Err(NativeTransactionError::Retired);
        }
        members
            .groups
            .retain(|group| group.upgrade().is_some_and(|group| group.needs_fault()));
        if members
            .groups
            .iter()
            .any(|candidate| candidate.ptr_eq(&Arc::downgrade(group)))
        {
            return Ok(());
        }
        if members.groups.len() >= MAX_NATIVE_TRANSACTIONS {
            return Err(NativeTransactionError::Limit);
        }
        members.groups.push(Arc::downgrade(group));
        Ok(())
    }

    pub(in crate::server) fn close(&self) {
        let (controller, groups) = {
            let mut members = lock(&self.0);
            members.closed = true;
            (
                members.controller.take(),
                std::mem::take(&mut members.groups),
            )
        };
        if let Some(controller) = controller.and_then(|controller| controller.upgrade()) {
            controller.close();
        }
        for group in groups.into_iter().filter_map(|group| group.upgrade()) {
            group.fault(NativeFault::Closed);
        }
    }
}

impl fmt::Debug for NativeRetirementHook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeRetirementHook")
            .finish_non_exhaustive()
    }
}

struct ControllerLinks {
    closed: bool,
    groups: Vec<Weak<Group>>,
}

pub(in crate::server) struct Controller {
    pub(in crate::server) owner: LinkIdentity,
    pub(in crate::server) profile: NativeCoordinatorProfile,
    active: AtomicBool,
    links: Mutex<ControllerLinks>,
}

/// An opaque origin observer minted only after a coordinator Attach is flushed.
#[derive(Clone)]
pub struct NativeControllerIdentity(pub(in crate::server) Arc<Controller>);

impl NativeControllerIdentity {
    pub fn is_active(&self) -> bool {
        self.0.active.load(Ordering::Acquire)
    }

    pub fn same_controller(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn connection_identity(&self) -> Option<&NativeConnectionIdentity> {
        self.0.owner.connection_identity()
    }
}

impl fmt::Debug for NativeControllerIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeControllerIdentity")
            .field("active", &self.is_active())
            .finish_non_exhaustive()
    }
}

impl Controller {
    pub(in crate::server) fn new(
        owner: LinkIdentity,
        profile: NativeCoordinatorProfile,
    ) -> Arc<Self> {
        Arc::new(Self {
            owner,
            profile,
            active: AtomicBool::new(true),
            links: Mutex::new(ControllerLinks {
                closed: false,
                groups: Vec::new(),
            }),
        })
    }

    pub(in crate::server) fn register(
        &self,
        group: &Arc<Group>,
    ) -> Result<(), NativeTransactionError> {
        let mut links = lock(&self.links);
        links
            .groups
            .retain(|group| group.upgrade().is_some_and(|group| group.needs_fault()));
        if links.closed || self.owner.is_retired() {
            return Err(NativeTransactionError::Retired);
        }
        if links.groups.len() >= MAX_NATIVE_TRANSACTIONS {
            return Err(NativeTransactionError::Limit);
        }
        links.groups.push(Arc::downgrade(group));
        Ok(())
    }

    pub(in crate::server) fn close(&self) {
        let groups = {
            let mut links = lock(&self.links);
            links.closed = true;
            std::mem::take(&mut links.groups)
        };
        for group in groups.into_iter().filter_map(|group| group.upgrade()) {
            group.fault(NativeFault::Closed);
        }
        self.active.store(false, Ordering::Release);
    }
}

pub(in crate::server) struct Obligation {
    pub(in crate::server) channel: u16,
    pub(in crate::server) handle: u32,
    pub(in crate::server) identity: DeliveryIdentity,
    pub(in crate::server) owner: LinkIdentity,
    phase: AtomicU8,
}

impl Obligation {
    pub(in crate::server) fn queued(&self) {
        self.phase.store(1, Ordering::Release);
    }
    pub(in crate::server) fn held(&self) {
        self.phase.store(2, Ordering::Release);
    }
    pub(in crate::server) fn flushed(&self) {
        self.phase.store(3, Ordering::Release);
    }
    pub(in crate::server) fn is_partial(&self) -> bool {
        self.phase.load(Ordering::Acquire) == 0
    }
    fn is_flushed(&self) -> bool {
        self.phase.load(Ordering::Acquire) == 3
    }
}

pub(in crate::server) struct Group {
    pub(in crate::server) id: TransactionId,
    pub(in crate::server) controller: NativeControllerIdentity,
    phase: AtomicU8,
    obligations: Mutex<Vec<Arc<Obligation>>>,
    retirements: Mutex<Vec<Arc<super::retirement::RetirementObligation>>>,
    changed: Notify,
    cleanup: Option<Arc<Notify>>,
}

impl Group {
    #[cfg(test)]
    pub(in crate::server) fn new(
        id: TransactionId,
        controller: NativeControllerIdentity,
    ) -> Arc<Self> {
        Self::new_inner(id, controller, None)
    }

    pub(in crate::server) fn new_with_cleanup(
        id: TransactionId,
        controller: NativeControllerIdentity,
        cleanup: Arc<Notify>,
    ) -> Arc<Self> {
        Self::new_inner(id, controller, Some(cleanup))
    }

    fn new_inner(
        id: TransactionId,
        controller: NativeControllerIdentity,
        cleanup: Option<Arc<Notify>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            controller,
            phase: AtomicU8::new(0),
            obligations: Mutex::new(Vec::new()),
            retirements: Mutex::new(Vec::new()),
            changed: Notify::new(),
            cleanup,
        })
    }

    fn notify_changed(&self) {
        self.changed.notify_waiters();
        if let Some(cleanup) = &self.cleanup {
            cleanup.notify_one();
        }
    }

    pub(in crate::server) fn state(&self) -> NativeTransactionState {
        match self.phase.load(Ordering::Acquire) {
            0 => NativeTransactionState::Pending,
            1 => NativeTransactionState::Sealed,
            2 => NativeTransactionState::Ready,
            3 => NativeTransactionState::OwnerStarted,
            4 => NativeTransactionState::Aborted,
            5 => NativeTransactionState::Committed,
            6 => NativeTransactionState::Rejected,
            7 => NativeTransactionState::Indeterminate,
            _ => NativeTransactionState::Faulted,
        }
    }

    fn needs_fault(&self) -> bool {
        self.phase.load(Ordering::Acquire) <= 2
    }

    pub(in crate::server) fn fault(&self, fault: NativeFault) {
        let mut previous = self.phase.load(Ordering::Acquire);
        while previous <= 2 {
            match self.phase.compare_exchange_weak(
                previous,
                16 + fault as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.notify_changed();
                    return;
                }
                Err(next) => previous = next,
            }
        }
    }

    pub(in crate::server) fn error(&self) -> NativeTransactionError {
        let fault = match self.phase.load(Ordering::Acquire).saturating_sub(16) {
            1 => NativeFault::PartialAtSeal,
            2 => NativeFault::Decode,
            3 => NativeFault::Aborted,
            4 => NativeFault::Inbox,
            5 => NativeFault::Continuation,
            6 => NativeFault::Flush,
            7 => NativeFault::Closed,
            8 => NativeFault::Stage,
            _ => NativeFault::Dropped,
        };
        NativeTransactionError::Faulted(fault)
    }

    pub(in crate::server) fn reserve(
        &self,
        channel: u16,
        handle: u32,
        owner: LinkIdentity,
        identity: DeliveryIdentity,
    ) -> Result<Arc<Obligation>, NativeTransactionError> {
        let mut obligations = lock(&self.obligations);
        let retirements = lock(&self.retirements);
        if self.state() != NativeTransactionState::Pending {
            return Err(self.error());
        }
        if obligations.len() + retirements.len() >= MAX_NATIVE_TRANSACTION_POSTINGS {
            return Err(NativeTransactionError::Limit);
        }
        let obligation = Arc::new(Obligation {
            channel,
            handle,
            owner,
            identity,
            phase: AtomicU8::new(0),
        });
        obligations.push(obligation.clone());
        Ok(obligation)
    }

    pub(in crate::server) fn reserve_retirements(
        self: &Arc<Self>,
        candidates: &[NativeRetirementCandidate],
    ) -> Result<Vec<NativeRetirementAttempt>, NativeTransactionError> {
        let obligations = lock(&self.obligations);
        let mut retirements = lock(&self.retirements);
        if self.state() != NativeTransactionState::Pending {
            return Err(self.error());
        }
        if obligations.len() + retirements.len() + candidates.len()
            > MAX_NATIVE_TRANSACTION_POSTINGS
        {
            return Err(NativeTransactionError::Limit);
        }
        for (index, candidate) in candidates.iter().enumerate() {
            if retirements.iter().any(|obligation| {
                obligation
                    .delivery_identity
                    .same_delivery(&candidate.delivery_identity)
            }) || candidates[..index].iter().any(|prior| {
                prior
                    .delivery_identity
                    .same_delivery(&candidate.delivery_identity)
            }) {
                return Err(NativeTransactionError::InvalidPreparedSet);
            }
        }
        let attempts: Vec<_> = candidates
            .iter()
            .map(|candidate| NativeRetirementAttempt {
                group: self.clone(),
                obligation: super::retirement::RetirementObligation::new(candidate),
            })
            .collect();
        retirements.extend(attempts.iter().map(|attempt| attempt.obligation.clone()));
        Ok(attempts)
    }

    pub(in crate::server) fn seal(&self, fail: bool) -> Result<(), NativeTransactionError> {
        if fail && self.state() == NativeTransactionState::Faulted {
            return self.known_abort();
        }
        if self
            .phase
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(self.error());
        }
        let partial = lock(&self.obligations)
            .iter()
            .any(|obligation| obligation.is_partial());
        if partial {
            self.fault(NativeFault::PartialAtSeal);
            return Err(self.error());
        }
        if fail {
            self.abort();
        } else {
            self.refresh_ready();
        }
        Ok(())
    }

    pub(in crate::server) fn known_abort(&self) -> Result<(), NativeTransactionError> {
        let previous = self.phase.load(Ordering::Acquire);
        if previous == 4 || previous == 6 {
            return Ok(());
        }
        if previous < 16 {
            return Err(NativeTransactionError::InvalidDecision);
        }
        if previous == 16 + NativeFault::PartialAtSeal as u8 {
            return Err(self.error());
        }
        if self
            .phase
            .compare_exchange(previous, 4, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(NativeTransactionError::InvalidDecision);
        }
        self.notify_changed();
        Ok(())
    }

    pub(in crate::server) fn refuse_staging(&self) -> Result<(), NativeTransactionError> {
        let mut previous = self.phase.load(Ordering::Acquire);
        loop {
            match previous {
                4 => return Ok(()),
                value if value == 16 + NativeFault::PartialAtSeal as u8 => {
                    return Err(NativeTransactionError::Faulted(NativeFault::PartialAtSeal));
                }
                1 | 2 => {}
                value if value >= 16 => {}
                _ => return Err(NativeTransactionError::InvalidDecision),
            }
            match self
                .phase
                .compare_exchange(previous, 4, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    self.notify_changed();
                    return Ok(());
                }
                Err(next) => previous = next,
            }
        }
    }

    pub(in crate::server) fn refresh_ready(&self) {
        let obligations = lock(&self.obligations);
        let retirements = lock(&self.retirements);
        let ready = obligations.iter().all(|obligation| obligation.is_flushed())
            && retirements
                .iter()
                .all(|obligation| obligation.is_prepared());
        drop(retirements);
        drop(obligations);
        if ready
            && self
                .phase
                .compare_exchange(1, 2, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.notify_changed();
        }
    }

    pub(in crate::server) fn abort(&self) {
        let mut previous = self.phase.load(Ordering::Acquire);
        while previous <= 2 {
            match self
                .phase
                .compare_exchange_weak(previous, 4, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    self.notify_changed();
                    return;
                }
                Err(next) => previous = next,
            }
        }
    }

    pub(in crate::server) async fn wait_ready(&self) -> Result<(), NativeTransactionError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.state() {
                NativeTransactionState::Ready => return Ok(()),
                NativeTransactionState::Pending | NativeTransactionState::Sealed => changed.await,
                _ => return Err(self.error()),
            }
        }
    }

    pub(in crate::server) fn exact_prepared(&self, work: &[NativePreparedWork]) -> bool {
        let obligations = lock(&self.obligations);
        let retirements = lock(&self.retirements);
        obligations.len() + retirements.len() == work.len()
            && obligations.iter().all(|obligation| {
                work
                    .iter()
                    .filter(|item| matches!(item, NativePreparedWork::Posting(posting) if posting.matches(self, obligation)))
                    .count()
                    == 1
            })
            && retirements.iter().all(|obligation| {
                work
                    .iter()
                    .filter(|item| matches!(item, NativePreparedWork::Retirement(retirement) if retirement.matches(self, obligation)))
                    .count()
                    == 1
            })
    }

    pub(in crate::server) fn obligations(&self) -> Vec<Arc<Obligation>> {
        lock(&self.obligations).clone()
    }

    pub(in crate::server) fn retirement_attempts(self: &Arc<Self>) -> Vec<NativeRetirementAttempt> {
        lock(&self.retirements)
            .iter()
            .map(|obligation| NativeRetirementAttempt {
                group: self.clone(),
                obligation: obligation.clone(),
            })
            .collect()
    }

    pub(in crate::server) fn is_terminal(&self) -> bool {
        !matches!(
            self.state(),
            NativeTransactionState::Pending
                | NativeTransactionState::Sealed
                | NativeTransactionState::Ready
                | NativeTransactionState::OwnerStarted
        )
    }
}

/// A payload-free observer returned only after an approved Declare is flushed.
#[derive(Clone)]
pub struct NativeTransactionIdentity(pub(in crate::server) Arc<Group>);

impl NativeTransactionIdentity {
    pub fn state(&self) -> NativeTransactionState {
        self.0.state()
    }
    pub fn transaction_id(&self) -> &TransactionId {
        &self.0.id
    }
    pub fn controller_identity(&self) -> &NativeControllerIdentity {
        &self.0.controller
    }
    pub fn same_transaction(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl fmt::Debug for NativeTransactionIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeTransactionIdentity")
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

/// Unique commit authority bundled with the exact native resources.
pub struct NativeReadySubmission {
    ticket: NativeReadyTicket,
    resources: NativeTransactionResources,
}

impl NativeReadySubmission {
    pub(in crate::server) fn new(control: ControlData, work: Vec<NativePreparedWork>) -> Self {
        let group = control.group.as_ref().cloned();
        Self {
            ticket: NativeReadyTicket {
                group: group.clone(),
            },
            resources: NativeTransactionResources {
                control: Some(control),
                postings: work,
                group,
            },
        }
    }

    pub fn into_owner_parts(self) -> (NativeReadyTicket, NativeTransactionResources) {
        (self.ticket, self.resources)
    }
}

/// Unique native authority; cloning or constructing it is not supported.
/// ```compile_fail
/// fn duplicate(ticket: amqp::NativeReadyTicket) { let _ = ticket.clone(); }
/// ```
/// ```compile_fail
/// let _ = amqp::NativeReadyTicket { group: None };
/// ```
pub struct NativeReadyTicket {
    group: Option<Arc<Group>>,
}

impl NativeReadyTicket {
    pub fn try_claim(mut self) -> Result<NativeClaim, NativeTransactionError> {
        let group = self
            .group
            .take()
            .ok_or(NativeTransactionError::InvalidPreparedSet)?;
        if group
            .phase
            .compare_exchange(2, 3, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(group.error());
        }
        Ok(NativeClaim { group: Some(group) })
    }
}

impl Drop for NativeReadyTicket {
    fn drop(&mut self) {
        if let Some(group) = &self.group {
            group.fault(NativeFault::Dropped);
        }
    }
}

pub struct NativeClaim {
    group: Option<Arc<Group>>,
}

impl NativeClaim {
    pub fn finish(mut self, decision: NativeTransactionDecision) {
        if let Some(group) = self.group.take() {
            let state = match decision {
                NativeTransactionDecision::Committed => 5,
                NativeTransactionDecision::Rejected => 6,
                NativeTransactionDecision::Indeterminate => 7,
            };
            let _ = group
                .phase
                .compare_exchange(3, state, Ordering::AcqRel, Ordering::Acquire);
            group.notify_changed();
        }
    }

    /// Known no-I/O cancellation after a native claim but before a logical claim.
    pub fn abort(mut self) {
        if let Some(group) = self.group.take() {
            let _ = group
                .phase
                .compare_exchange(3, 4, Ordering::AcqRel, Ordering::Acquire);
            group.notify_changed();
        }
    }
}

impl Drop for NativeClaim {
    fn drop(&mut self) {
        if let Some(group) = &self.group {
            let _ = group
                .phase
                .compare_exchange(3, 7, Ordering::AcqRel, Ordering::Acquire);
            group.notify_changed();
        }
    }
}

pub struct NativeTransactionResources {
    pub(in crate::server) control: Option<ControlData>,
    pub(in crate::server) postings: Vec<NativePreparedWork>,
    group: Option<Arc<Group>>,
}

impl NativeTransactionResources {
    pub async fn finish(mut self) -> Result<(), EngineError> {
        let control = self
            .control
            .take()
            .ok_or_else(|| native_error(NativeTransactionError::InvalidPreparedSet))?;
        let commands = control.route.commands.clone();
        let postings = std::mem::take(&mut self.postings);
        request(&commands, |reply| {
            Command::NativeTransactions(NativeCommand::Finish {
                control: Box::new(control),
                postings,
                reply,
            })
        })
        .await
    }
}

impl Drop for NativeTransactionResources {
    fn drop(&mut self) {
        if let Some(group) = &self.group {
            group.fault(NativeFault::Dropped);
        }
    }
}

macro_rules! opaque_debug {
    ($($name:ty),+ $(,)?) => { $(impl fmt::Debug for $name {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct(stringify!($name)).finish_non_exhaustive()
        }
    })+ };
}
opaque_debug!(
    NativeReadySubmission,
    NativeReadyTicket,
    NativeClaim,
    NativeTransactionResources
);
