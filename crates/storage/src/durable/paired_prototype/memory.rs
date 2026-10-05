use std::sync::RwLock;

use super::control::{PhysicalPairBinding, Role};
use super::inventory::{RoleData, RoleInventory};
use super::{Error, Result};

pub(super) struct MemoryCapsule {
    pub(super) data: RwLock<RoleData>,
    role: Role,
    binding: PhysicalPairBinding,
    poisoned: bool,
}

pub(super) struct PairedMemoryState {
    pub(super) capsule: MemoryCapsule,
}
pub(super) struct PairedMemoryLog {
    pub(super) capsule: MemoryCapsule,
}

impl MemoryCapsule {
    pub(super) fn new(role: Role, binding: PhysicalPairBinding) -> Self {
        Self {
            data: RwLock::new(RoleData::default()),
            role,
            binding,
            poisoned: false,
        }
    }
    pub(super) fn poison(&mut self) {
        self.poisoned = true;
    }
    pub(super) fn capture(&self) -> Result<RoleInventory> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        self.data
            .read()
            .map_err(|_| Error::Backend)?
            .capture(self.role, self.binding)
    }
    pub(super) fn replace(&mut self, candidate: RoleData) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let mut data = self.data.write().map_err(|_| Error::Backend)?;
        *data = candidate;
        Ok(())
    }
}
