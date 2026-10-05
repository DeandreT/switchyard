mod atomicity;
mod bounds;
mod crash;
mod fixture;
mod ordering;

use super::control::{CreationPhase, PhysicalPairBinding, Role};
use super::creation::{BackendPair, CreationStep, PrepareFailure, PreparedFixturePair};
use super::fjall::{AcquisitionCause, ControlledLocations, reopen_controlled_fixture};
use super::input::{CompositeFixtureParts, SeedLogParts, SeedStateParts};
use super::inventory::{LOGICAL, PairInventory, RoleStatus};
use super::*;
use fixture::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
