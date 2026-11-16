//! Explicit committed application mode. No primary routing, server activation,
//! consistent reads, transparent retry or historical-outcome recovery.
mod composition;
mod original;
mod projection;
#[cfg(test)]
mod tests;
mod validation;

use super::{
    AdmissionError, ConsensusDriver, DriverError, DriverHandle, DriverOptions, DriverStatus,
};
use crate::consensus::{
    ConsensusIdentity, MAX_CONSENSUS_CONTEXT_BYTES, MAX_CONSENSUS_ENTRY_BYTES,
    MAX_CONSENSUS_PROPOSAL_BYTES,
};
use composition::{F2View, Hub};
pub use original::ApplicationReceipt;
pub(super) use original::Original;
pub(super) use projection::Policy;
use std::sync::Arc;
use storage::ConsensusBackend;

#[derive(Debug, thiserror::Error)]
pub enum ApplicationError {
    #[error("application input/budget busy before admission")]
    Busy,
    #[error("invalid local durable proposal")]
    InvalidProposal,
    #[error("not admitted in the requested leader term")]
    NotLeader,
    #[error("application unavailable before queue admission")]
    Unavailable,
    #[error("application unavailable; original may have committed/applied, no automatic retry")]
    Unknown,
    #[error("receipt already observed or owner lost")]
    Receipt,
    #[error("invalid application limits")]
    Limits,
    #[error("mixed image invariant: {0}")]
    Corrupt(&'static str),
    #[error("mixed image validation: {0}")]
    Validation(String),
    #[error(transparent)]
    Storage(#[from] storage::StorageError),
    #[error(transparent)]
    Consensus(#[from] crate::consensus::ConsensusError),
    #[error(transparent)]
    Journal(#[from] crate::JournalError),
    #[error(transparent)]
    Replay(#[from] crate::ReplayError),
    #[error(transparent)]
    Indexed(#[from] domain::IndexedApplyError),
    #[error(transparent)]
    Proposal(#[from] domain::DurableProposalError),
}
#[derive(Debug)]
pub struct ApplicationResult {
    pub term: u64,
    pub raft_index: u64,
    pub application_ordinal: u64,
    pub outcome: domain::IndexedApplyOutcome,
}
/// Latest diagnostics only: neither a readiness certificate nor read authority.
#[derive(Clone, Debug, Default)]
pub struct ApplicationStatus {
    pub applied_ordinal: u64,
    pub applied_raft: u64,
    pub committed_raft: u64,
    pub projected: u64,
    pub projected_commit: u64,
    pub originals: usize,
    pub original_bytes: usize,
    pub retired: bool,
    pub uncertain: bool,
}

/// Finite logical custody limits, not allocator/RSS or physical disk guarantees.
#[derive(Clone, Copy, Debug)]
pub struct ApplicationLimits {
    pub max_image_bytes: usize,
    pub max_rows: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_originals: usize,
    pub original_bytes: usize,
}
impl Default for ApplicationLimits {
    fn default() -> Self {
        let mut limits = Self {
            max_image_bytes: 1024 * 1024 * 1024,
            max_rows: 65536,
            max_key_bytes: 4096,
            max_value_bytes: 16 * 1024 * 1024,
            max_originals: 1,
            original_bytes: 0,
        };
        limits.original_bytes = limits.original_charge().expect("bounded defaults");
        limits
    }
}
impl ApplicationLimits {
    /// Conservative LOGICAL reply reservation, not Rust heap/RSS. Delivery,
    /// session and rule replies borrow/copy distinct image rows (2*image);
    /// postcard scalar vectors cost at most 16*proposal; scheduled fanout paths
    /// are bounded by timer scan * subscriptions * max path. Row wrappers reserve
    /// 256 bytes each. This covers replies much larger than the proposal.
    pub fn reply_reservation(self) -> Option<usize> {
        self.max_image_bytes
            .checked_mul(2)?
            .checked_add(MAX_CONSENSUS_PROPOSAL_BYTES.checked_mul(16)?)?
            .checked_add(
                domain::TIMER_SCAN_LIMIT
                    .checked_mul(domain::MAX_TOPIC_SUBSCRIPTIONS)?
                    .checked_mul(domain::MAX_ENTITY_PATH_BYTES + 32)?,
            )?
            .checked_add(self.max_rows.checked_mul(256)?)
    }
    pub fn original_charge(self) -> Option<usize> {
        self.reply_reservation()?
            .checked_add(MAX_CONSENSUS_PROPOSAL_BYTES + MAX_CONSENSUS_CONTEXT_BYTES + 512)
    }
    fn validate(self, driver: &DriverOptions) -> Result<(), ApplicationError> {
        // Complete F2 value + 40-byte record frame, full F0 proposal + 48-byte
        // envelope, ordinary owner/checkpoint and immutable-root/headroom.
        let minimum = 2usize
            .checked_mul(driver.retained.max_bytes as usize)
            .and_then(|n| {
                n.checked_add(
                    MAX_CONSENSUS_ENTRY_BYTES + 40 + crate::MAX_JOURNAL_PAYLOAD_BYTES + 48 + 16384,
                )
            })
            .ok_or(ApplicationError::Limits)?;
        let key_min = domain::MAX_NAMESPACE_NAME_BYTES
            + domain::MAX_ENTITY_PATH_BYTES
            + domain::MAX_SESSION_ID_BYTES
            + 4 * domain::MAX_RULE_NAME_CHARACTERS
            + 64;
        let charge = self.original_charge().ok_or(ApplicationError::Limits)?;
        // At most max_originals queued application events. Keep four complete
        // peer/control slots AND frame charges outside that business allowance.
        let event_min = self
            .max_originals
            .checked_mul(MAX_CONSENSUS_PROPOSAL_BYTES + MAX_CONSENSUS_CONTEXT_BYTES + 256)
            .and_then(|n| {
                driver
                    .transport
                    .max_frame_bytes
                    .checked_mul(4)
                    .and_then(|reserve| n.checked_add(reserve))
            })
            .ok_or(ApplicationError::Limits)?;
        if self.max_image_bytes < minimum
            || self.max_image_bytes > 2 * 1024 * 1024 * 1024
            || !(16..=1_048_576).contains(&self.max_rows)
            || self.max_key_bytes < key_min
            || self.max_key_bytes > 65536
            || self.max_value_bytes < MAX_CONSENSUS_ENTRY_BYTES + 40
            || self.max_value_bytes > self.max_image_bytes
            || !(1..=4).contains(&self.max_originals)
            || self.original_bytes < charge
            || self.original_bytes > 16usize * 1024 * 1024 * 1024
            || driver.max_events < self.max_originals + 4
            || driver.max_event_bytes < event_min
        {
            return Err(ApplicationError::Limits);
        }
        Ok(())
    }
}

/// Cloneable bounded admission, not a writer, raw driver or routing facade.
#[derive(Clone)]
pub struct ApplicationHandle {
    driver: DriverHandle,
    shared: Arc<original::Shared>,
    limits: ApplicationLimits,
}
impl ApplicationHandle {
    /// Queue one exact original. Success here means queue admission ONLY; wait
    /// on its receipt for the actual leader decision. A receipt's `Unknown`
    /// must not trigger an automatic mutation retry. Malformed input, Busy and
    /// Unavailable returned directly here mean definite non-admission.
    pub fn submit(
        &self,
        data: Vec<u8>,
        context: Vec<u8>,
        expected_term: u64,
    ) -> Result<ApplicationReceipt, ApplicationError> {
        if data.is_empty()
            || data.len() > MAX_CONSENSUS_PROPOSAL_BYTES
            || context.len() > MAX_CONSENSUS_CONTEXT_BYTES
        {
            return Err(ApplicationError::InvalidProposal);
        }
        let p = domain::DurableProposal::decode(&data)
            .map_err(|_| ApplicationError::InvalidProposal)?;
        if &p.command().namespace != self.driver.shared.identity.namespace() {
            return Err(ApplicationError::InvalidProposal);
        }
        let credit = self.shared.pool.reserve(
            self.limits
                .original_charge()
                .ok_or(ApplicationError::Limits)?,
        )?;
        let (original, receipt) = Original::new(data, context, expected_term, credit);
        self.driver.application(original).map_err(|e| match e {
            AdmissionError::Busy => ApplicationError::Busy,
            AdmissionError::NotLeader => ApplicationError::NotLeader,
            _ => ApplicationError::Unavailable,
        })?;
        Ok(receipt)
    }
    pub fn status(&self) -> ApplicationStatus {
        let mut s = self
            .shared
            .status
            .lock()
            .expect("application status")
            .clone();
        (s.originals, s.original_bytes) = self.shared.pool.usage();
        s
    }
    pub fn driver_status(&self) -> DriverStatus {
        self.driver.status()
    }
    pub fn retire(&self) {
        self.shared.retire();
        self.driver.retire();
    }
    /// Bounded core controls, not primary/client routing or read authority.
    pub fn campaign(&self) -> Result<super::Receipt<()>, AdmissionError> {
        self.driver.campaign()
    }
    pub fn tick(&self) -> Result<super::Receipt<()>, AdmissionError> {
        self.driver.tick()
    }
}

/// One non-Clone retained live core/network/physical-application owner. No raw
/// StateStore, backend, reader or arbitrary callback capability escapes.
///
/// ```compile_fail
/// use cluster::consensus::driver::application::ApplicationOwner;
/// fn duplicate(owner: ApplicationOwner) { let _ = owner.clone(); }
/// ```
/// ```compile_fail
/// use cluster::consensus::driver::application::ApplicationOwner;
/// fn raw_writer(owner: ApplicationOwner) {
///     fn requires_store<S: storage::StateStore>(_: S) {}
///     requires_store(owner);
/// }
/// ```
pub struct ApplicationOwner {
    driver: ConsensusDriver<F2View>,
    hub: Hub,
    handle: ApplicationHandle,
    identity: ConsensusIdentity,
    local: u64,
    options: DriverOptions,
}
/// Opaque original backend custody after actual finish. Consuming reopen fully
/// validates it again and preserves credits held by old unobserved receipts.
pub struct ClosedApplicationOwner {
    hub: Hub,
    pool: Arc<original::Pool>,
    identity: ConsensusIdentity,
    local: u64,
    options: DriverOptions,
    limits: ApplicationLimits,
}
/// Fixed latest-contextual-entry diagnostics for controlled fixtures, not a generic reader
/// or application/quorum authority. Original context is never projected to F0.
#[cfg(feature = "consensus-driver-testkit")]
#[derive(Debug)]
pub struct CommittedObservation {
    pub index: u64,
    pub term: u64,
    pub data_bytes: usize,
    pub context_bytes: usize,
    pub data_digest: [u8; 32],
    pub context_digest: [u8; 32],
}
impl ApplicationOwner {
    #[cfg(feature = "consensus-driver-testkit")]
    pub fn committed_observation(&self) -> Result<Option<CommittedObservation>, DriverError> {
        use sha2::{Digest, Sha256};
        let store = self.driver.observed_store();
        let index = store.hard_state()?.commit;
        if index == 0 {
            return Ok(None);
        }
        let mut contextual = None;
        for r in 1..=index {
            let entry = store
                .read_entries(r, 1)?
                .pop()
                .ok_or(ApplicationError::Corrupt("missing committed observation"))?;
            if !entry.context.is_empty() || r == index && contextual.is_none() {
                contextual = Some(entry);
            }
        }
        let entry = contextual.expect("nonempty committed prefix");
        Ok(Some(CommittedObservation {
            index: entry.index,
            term: entry.term,
            data_bytes: entry.data.len(),
            context_bytes: entry.context.len(),
            data_digest: Sha256::digest(&entry.data).into(),
            context_digest: Sha256::digest(&entry.context).into(),
        }))
    }
    /// Validate configuration/bind before the factory, validate the complete
    /// mixed image, then recover missing phases before exposing admission. The
    /// trusted backend must exclude all external/raw writers for this lifetime.
    pub async fn bind<B: ConsensusBackend>(
        identity: ConsensusIdentity,
        local: u64,
        options: DriverOptions,
        limits: ApplicationLimits,
        fresh: bool,
        factory: impl FnOnce() -> Result<B, storage::StorageError>,
    ) -> Result<(Self, ApplicationHandle), DriverError> {
        Self::bind_boxed(
            identity,
            local,
            options,
            limits,
            fresh,
            || Ok(Box::new(factory()?) as Box<dyn ConsensusBackend>),
            None,
        )
        .await
    }
    async fn bind_boxed(
        identity: ConsensusIdentity,
        local: u64,
        options: DriverOptions,
        limits: ApplicationLimits,
        fresh: bool,
        factory: impl FnOnce() -> Result<Box<dyn ConsensusBackend>, storage::StorageError>,
        pool: Option<Arc<original::Pool>>,
    ) -> Result<(Self, ApplicationHandle), DriverError> {
        limits.validate(&options)?;
        let shared =
            original::Shared::new(pool.unwrap_or_else(|| {
                original::Pool::new(limits.max_originals, limits.original_bytes)
            }));
        let mut retained = None;
        let expected = identity.clone();
        let shared_factory = shared.clone();
        let retention = options.retained;
        let (driver, handle) =
            ConsensusDriver::bind_with_store(identity.clone(), local, options.clone(), || {
                let hub = Hub::new(factory()?, limits);
                let validated = validation::open(&hub, expected, local, retention, fresh)?;
                let (store, mut projection) =
                    projection::Projection::new(validated, shared_factory)?;
                projection.recover(&store)?;
                retained = Some(hub);
                Ok((store, Policy::Committed(Box::new(projection))))
            })
            .await?;
        let handle = ApplicationHandle {
            driver: handle,
            shared,
            limits,
        };
        Ok((
            Self {
                driver,
                hub: retained.expect("validated composition"),
                handle: handle.clone(),
                identity,
                local,
                options,
            },
            handle,
        ))
    }
    pub async fn run(&mut self) -> crate::transport::TransportReport {
        self.driver.run().await
    }
    pub async fn finish(&mut self) -> crate::transport::TransportReport {
        self.driver.finish().await
    }
    pub fn status(&self) -> ApplicationStatus {
        self.handle.status()
    }
    pub fn driver_status(&self) -> DriverStatus {
        self.driver.status()
    }
    pub fn transport_status(&self) -> crate::transport::TransportStatus {
        self.driver.transport_status()
    }
    pub fn error(&self) -> Option<&DriverError> {
        self.driver.error()
    }
    pub fn panic_payload(&self) -> Option<&(dyn std::any::Any + Send)> {
        self.driver.panic_payload()
    }
    pub fn into_closed(self) -> Result<ClosedApplicationOwner, Box<Self>> {
        if !self.driver.status().finished {
            return Err(Box::new(self));
        }
        let Self {
            driver,
            hub,
            handle,
            identity,
            local,
            options,
        } = self;
        drop(driver);
        Ok(ClosedApplicationOwner {
            hub,
            pool: handle.shared.pool.clone(),
            identity,
            local,
            options,
            limits: handle.limits,
        })
    }
    #[cfg(feature = "consensus-driver-testkit")]
    pub fn network_control(&self) -> super::testing::NetworkControl {
        self.driver.network_control()
    }
    #[cfg(feature = "consensus-driver-testkit")]
    pub fn dispatch_gate(&self) -> super::testing::DispatchGate {
        self.driver.dispatch_gate()
    }
}
impl ClosedApplicationOwner {
    pub async fn reopen(self) -> Result<(ApplicationOwner, ApplicationHandle), DriverError> {
        let Self {
            hub,
            pool,
            identity,
            local,
            options,
            limits,
        } = self;
        let backend = hub.into_backend()?;
        ApplicationOwner::bind_boxed(
            identity,
            local,
            options,
            limits,
            false,
            || Ok(backend),
            Some(pool),
        )
        .await
    }
}
