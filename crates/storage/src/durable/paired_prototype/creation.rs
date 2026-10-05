use super::control::{CreationPhase, PhysicalPairBinding, Role};
use super::fjall::{
    AcquisitionCause, AcquisitionFailure, ControlledLocations, NativeCapsule, PairedFjallLog,
    PairedFjallState,
};
use super::input::{CompositeFixtureParts, SeedLogParts, SeedStateParts};
use super::inventory::{Budget, LOGICAL, PairInventory, RoleData, RoleStatus};
use super::memory::{MemoryCapsule, PairedMemoryLog, PairedMemoryState};
use super::{CommitFault, Counts, Error, Result, copy};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CreationStep {
    LogPrepared,
    StateReady,
    LogReady,
}

pub(super) enum PrepareFailure {
    Input(Error),
    Acquisition(AcquisitionFailure),
}

impl std::fmt::Debug for PrepareFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Input(error) => std::fmt::Debug::fmt(error, f),
            Self::Acquisition(error) => std::fmt::Debug::fmt(error, f),
        }
    }
}
impl std::fmt::Display for PrepareFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Input(error) => std::fmt::Display::fmt(error, f),
            Self::Acquisition(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for PrepareFailure {}

pub(super) enum BackendPair {
    Memory {
        state: PairedMemoryState,
        log: PairedMemoryLog,
    },
    Fjall {
        state: PairedFjallState,
        log: PairedFjallLog,
    },
}

pub(super) struct PreparedFixturePair {
    pub(super) backend: BackendPair,
    pub(super) counts: Counts,
    pending: [Option<RoleData>; 3],
    attempted: bool,
    ready: bool,
    composite_attempted: bool,
    poisoned: bool,
}

fn preflight(
    binding: PhysicalPairBinding,
    state: &SeedStateParts<'_>,
    log: &SeedLogParts<'_>,
) -> Result<[Option<RoleData>; 3]> {
    state.validate()?;
    log.validate()?;
    // All borrowed shape checks finish before the first owned body is constructed.
    Ok([
        Some(RoleData::seed_log(binding, log, CreationPhase::Prepared)?),
        Some(RoleData::seed_state(binding, state)?),
        Some(RoleData::seed_log(binding, log, CreationPhase::Ready)?),
    ])
}

impl PreparedFixturePair {
    pub(super) fn memory(
        binding: PhysicalPairBinding,
        state: &SeedStateParts<'_>,
        log: &SeedLogParts<'_>,
    ) -> Result<Self> {
        let pending = preflight(binding, state, log)?;
        Ok(Self::new(
            BackendPair::Memory {
                state: PairedMemoryState {
                    capsule: MemoryCapsule::new(Role::State, binding),
                },
                log: PairedMemoryLog {
                    capsule: MemoryCapsule::new(Role::Log, binding),
                },
            },
            pending,
        ))
    }

    pub(super) fn fjall(
        locations: &ControlledLocations,
        binding: PhysicalPairBinding,
        state: &SeedStateParts<'_>,
        log: &SeedLogParts<'_>,
    ) -> std::result::Result<Self, PrepareFailure> {
        let pending = preflight(binding, state, log).map_err(PrepareFailure::Input)?;
        let acquire = || -> fjall::Result<BackendPair> {
            let state = NativeCapsule::create(&locations.paths[0], Role::State, binding)?;
            let log = NativeCapsule::create(&locations.paths[1], Role::Log, binding)?;
            Ok(BackendPair::Fjall {
                state: PairedFjallState { capsule: state },
                log: PairedFjallLog { capsule: log },
            })
        };
        let backend = acquire().map_err(|error| {
            PrepareFailure::Acquisition(AcquisitionFailure {
                cause: AcquisitionCause::Native(error),
                paths: locations.paths.clone(),
            })
        })?;
        // Only this returned success supplies the controlled EmptyNative boundary.
        Ok(Self::new(backend, pending))
    }

    fn new(backend: BackendPair, pending: [Option<RoleData>; 3]) -> Self {
        Self {
            backend,
            counts: Counts::default(),
            pending,
            attempted: false,
            ready: false,
            composite_attempted: false,
            poisoned: false,
        }
    }

    fn healthy(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn poison(&mut self) {
        self.poisoned = true;
        match &mut self.backend {
            BackendPair::Memory { state, log } => {
                state.capsule.poison();
                log.capsule.poison();
            }
            BackendPair::Fjall { state, log } => {
                state.capsule.poison();
                log.capsule.poison();
            }
        }
    }

    pub(super) fn capture(&mut self) -> Result<PairInventory> {
        self.healthy()?;
        self.counts.captures += 1;
        let value = match &self.backend {
            BackendPair::Memory { state, log } => (|| {
                Ok(PairInventory {
                    state: state.capsule.capture()?,
                    log: log.capsule.capture()?,
                })
            })(),
            BackendPair::Fjall { state, log } => (|| {
                Ok(PairInventory {
                    state: state.capsule.capture()?,
                    log: log.capsule.capture()?,
                })
            })(),
        };
        if value.is_err() {
            self.poison();
        }
        value
    }

    pub(super) fn create(&mut self, fault: Option<(CreationStep, CommitFault)>) -> Result<()> {
        self.healthy()?;
        if self.attempted {
            return Err(Error::Used);
        }
        self.attempted = true;
        for (index, step) in [
            CreationStep::LogPrepared,
            CreationStep::StateReady,
            CreationStep::LogReady,
        ]
        .into_iter()
        .enumerate()
        {
            let candidate = self.pending[index].take().ok_or(Error::Used)?;
            let fault = fault
                .filter(|(target, _)| *target == step)
                .map_or(CommitFault::None, |(_, fault)| fault);
            self.enter(index, candidate, &[], fault)?;
        }
        self.ready = true;
        Ok(())
    }

    fn enter(
        &mut self,
        index: usize,
        candidate: RoleData,
        old_keys: &[Vec<u8>],
        fault: CommitFault,
    ) -> Result<()> {
        self.counts.entered[index] += 1;
        let result = if fault == CommitFault::BeforeBackendErr {
            Err(Error::Backend)
        } else {
            self.counts.backend[index] += 1;
            let committed = match &mut self.backend {
                BackendPair::Memory { state, log } => {
                    if index == 0 || index == 2 {
                        log.capsule.replace(candidate)
                    } else {
                        state.capsule.replace(candidate)
                    }
                }
                BackendPair::Fjall { state, log } => match index {
                    0 => log.capsule.seed(&candidate),
                    1 => state.capsule.seed(&candidate),
                    2 => log.capsule.ready(&candidate),
                    _ => state.capsule.composite(&candidate, old_keys),
                },
            };
            if committed.is_ok() && fault == CommitFault::AfterSyncErr {
                Err(Error::Backend)
            } else {
                committed
            }
        };
        if result.is_err() {
            self.poison();
            return Err(Error::CommitUnknown);
        }
        Ok(())
    }

    pub(super) fn composite(
        &mut self,
        parts: &CompositeFixtureParts<'_>,
        fault: CommitFault,
    ) -> Result<()> {
        self.healthy()?;
        if !self.ready || self.composite_attempted {
            return Err(Error::Used);
        }
        self.composite_attempted = true;
        parts.validate()?;
        let old = self.capture()?;
        if old.state.status != RoleStatus::Paired(CreationPhase::Ready)
            || old.log.status != RoleStatus::Paired(CreationPhase::Ready)
        {
            self.poison();
            return Err(Error::InvalidLogical);
        }
        let old_fence = old
            .state
            .metadata
            .iter()
            .find(|(key, _)| key == LOGICAL[1])
            .ok_or(Error::InvalidLogical)?;
        if old_fence.1.as_slice() == parts.fence {
            return Err(Error::InvalidLogical);
        }
        let mut budget = Budget::new(131_076, 201_335_808);
        for (key, _) in &old.state.records {
            budget.consume(key.len(), 0)?;
        }
        for &(key, value) in parts.business {
            budget.consume(key.len(), value.len())?;
        }
        for (key, value) in [
            (super::control::INIT_KEY, &[1][..]),
            (LOGICAL[1], parts.fence),
            (LOGICAL[5], parts.live.0),
            (LOGICAL[6], parts.live.1),
        ] {
            budget.consume(key.len(), value.len())?;
        }
        let mut candidate = RoleData::from_inventory(&old.state)?;
        candidate.records.clear();
        for &(key, value) in parts.business {
            candidate.records.insert(copy(key)?, copy(value)?);
        }
        for (key, value) in [
            (super::control::INIT_KEY, &[1][..]),
            (LOGICAL[1], parts.fence),
            (LOGICAL[5], parts.live.0),
            (LOGICAL[6], parts.live.1),
        ] {
            candidate.metadata.insert(copy(key)?, copy(value)?);
        }
        let mut old_keys = Vec::new();
        old_keys
            .try_reserve_exact(old.state.records.len())
            .map_err(|_| Error::Allocation)?;
        for (key, _) in &old.state.records {
            old_keys.push(copy(key)?);
        }
        self.enter(3, candidate, &old_keys, fault)
    }
}
