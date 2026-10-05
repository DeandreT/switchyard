use std::collections::BTreeMap;

use super::super::FORMAT_VERSION_KEY;
use super::control::{
    BINDING_KEY, CreationPhase, INIT_KEY, MANIFEST_KEY, PROFILE_KEY, PhysicalPairBinding, Role,
    STAMP_KEY,
};
use super::input::{SeedLogParts, SeedStateParts};
use super::{BorrowedRow, Error, Result, Rows, copy};

pub(super) const MAX_IMAGE: usize = 64 * 1024 * 1024;
pub(super) const STATE_META_BYTES: usize = 134_235_239;
pub(super) const LOG_META_BYTES: usize = 131_419;
pub(super) const LOG_RECORD_BYTES: usize = 67_259_402;
pub(super) const LOGICAL: [&[u8]; 7] = [
    &[0x20, 1],
    &[0x20, 2],
    &[0x20, 3],
    &[0x20, 4],
    &[0x20, 5],
    &[0x20, 6],
    &[0x20, 7],
];

#[derive(Default)]
pub(super) struct RoleData {
    pub(super) metadata: BTreeMap<Vec<u8>, Vec<u8>>,
    pub(super) records: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RoleStatus {
    Empty,
    Paired(CreationPhase),
}

#[derive(Eq, PartialEq)]
pub(super) struct RoleInventory {
    pub(super) status: RoleStatus,
    pub(super) metadata: Rows,
    pub(super) records: Rows,
}

#[derive(Eq, PartialEq)]
pub(super) struct PairInventory {
    pub(super) state: RoleInventory,
    pub(super) log: RoleInventory,
}

impl std::fmt::Debug for RoleInventory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoleInventory")
            .field("status", &self.status)
            .field("metadata_rows", &self.metadata.len())
            .field("record_rows", &self.records.len())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for PairInventory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PairInventory")
            .field("state", &self.state)
            .field("log", &self.log)
            .finish()
    }
}

pub(super) struct Budget {
    rows: usize,
    bytes: usize,
    max_rows: usize,
    max_bytes: usize,
}

impl Budget {
    pub(super) fn new(max_rows: usize, max_bytes: usize) -> Self {
        Self {
            rows: 0,
            bytes: 0,
            max_rows,
            max_bytes,
        }
    }
    pub(super) fn consume(&mut self, key: usize, value: usize) -> Result<()> {
        let rows = self
            .rows
            .checked_add(1)
            .filter(|n| *n <= self.max_rows)
            .ok_or(Error::Limit)?;
        let bytes = key
            .checked_add(value)
            .and_then(|n| self.bytes.checked_add(n))
            .filter(|n| *n <= self.max_bytes)
            .ok_or(Error::Limit)?;
        self.rows = rows;
        self.bytes = bytes;
        Ok(())
    }
}

pub(super) fn business_shape<'a>(rows: impl IntoIterator<Item = BorrowedRow<'a>>) -> Result<()> {
    let mut budget = crate::ReadBudget::new(crate::ReadLimits {
        max_rows: 65_536,
        max_key_bytes: 1024,
        max_value_bytes: 266_240,
        max_total_bytes: MAX_IMAGE,
    });
    let mut previous = None;
    for (key, value) in rows {
        if key.is_empty() || previous.is_some_and(|p| p >= key) {
            return Err(Error::InvalidLogical);
        }
        budget
            .consume(key.len(), value.len())
            .map_err(|_| Error::Limit)?;
        previous = Some(key);
    }
    Ok(())
}

pub(super) fn log_shape<'a>(rows: impl IntoIterator<Item = BorrowedRow<'a>>) -> Result<()> {
    let mut budget = Budget::new(256, MAX_IMAGE);
    let mut previous = None;
    for (key, value) in rows {
        if key.len() != 9 || key[0] != 0x21 || previous.is_some_and(|p| p >= key) {
            return Err(Error::InvalidLogical);
        }
        budget.consume(0, value.len())?;
        previous = Some(key);
    }
    Ok(())
}

pub(super) struct Shape {
    role: Role,
    metadata: Budget,
    records: Budget,
    entries: Budget,
    metadata_bits: u16,
    control_bits: u8,
}

impl Shape {
    pub(super) fn new(role: Role) -> Self {
        Self {
            role,
            metadata: Budget::new(
                if role == Role::State { 12 } else { 6 },
                if role == Role::State {
                    STATE_META_BYTES
                } else {
                    LOG_META_BYTES
                },
            ),
            records: Budget::new(
                if role == Role::State { 65_536 } else { 261 },
                if role == Role::State {
                    MAX_IMAGE
                } else {
                    LOG_RECORD_BYTES
                },
            ),
            entries: Budget::new(256, MAX_IMAGE),
            metadata_bits: 0,
            control_bits: 0,
        }
    }

    pub(super) fn metadata(&mut self, key: &[u8], size: usize) -> Result<()> {
        let (bit, cap) = if key == FORMAT_VERSION_KEY {
            (0, 4)
        } else if key == PROFILE_KEY {
            (1, 64)
        } else if key == INIT_KEY {
            (2, 1)
        } else if key == BINDING_KEY {
            (3, 112)
        } else if key == STAMP_KEY {
            (4, 112)
        } else if self.role == Role::Log && key == MANIFEST_KEY {
            (5, 131072)
        } else if self.role == Role::State {
            let index = LOGICAL
                .iter()
                .position(|candidate| *candidate == key)
                .ok_or(Error::InvalidLogical)?;
            (
                5 + index,
                match index {
                    0..=2 => 256,
                    3 | 5 => 8192,
                    _ => MAX_IMAGE,
                },
            )
        } else {
            return Err(Error::InvalidLogical);
        };
        if size > cap {
            return Err(Error::Limit);
        }
        if self.metadata_bits & (1 << bit) != 0 {
            return Err(Error::InvalidLogical);
        }
        self.metadata.consume(key.len(), size)?;
        self.metadata_bits |= 1 << bit;
        Ok(())
    }

    pub(super) fn record(&mut self, key: &[u8], size: usize) -> Result<()> {
        if self.role == Role::State {
            if key.is_empty() {
                return Err(Error::InvalidLogical);
            }
            if key.len() > 1024 || size > 266240 {
                return Err(Error::Limit);
            }
        } else if key.len() == 9 && key[0] == 0x21 {
            self.entries.consume(0, size)?;
        } else {
            let index = LOGICAL[..5]
                .iter()
                .position(|candidate| *candidate == key)
                .ok_or(Error::InvalidLogical)?;
            let cap = match index {
                0 | 1 | 4 => 256,
                2 => 16384,
                _ => 131072,
            };
            if size > cap {
                return Err(Error::Limit);
            }
            if self.control_bits & (1 << index) != 0 {
                return Err(Error::InvalidLogical);
            }
            self.control_bits |= 1 << index;
        }
        self.records.consume(key.len(), size)
    }

    pub(super) fn finish(&self) -> Result<bool> {
        if self.metadata.rows == 0 && self.records.rows == 0 {
            return Ok(true);
        }
        let required = if self.role == Role::State { 0x7f } else { 0x3f };
        if self.metadata_bits & required != required {
            return Err(Error::InvalidLogical);
        }
        if self.role == Role::State {
            let live = self.metadata_bits & ((1 << 10) | (1 << 11));
            if live != 0 && live != ((1 << 10) | (1 << 11)) {
                return Err(Error::InvalidLogical);
            }
        } else if self.control_bits & 7 != 7 {
            return Err(Error::InvalidLogical);
        }
        Ok(false)
    }

    pub(super) fn counts(&self) -> (usize, usize) {
        (self.metadata.rows, self.records.rows)
    }
}

pub(super) fn fixed_records(
    role: Role,
    expected: PhysicalPairBinding,
    values: [&[u8]; 5],
) -> Result<RoleStatus> {
    if values[0] != role.format().to_be_bytes() || values[1] != role.profile() || values[2] != [1] {
        return Err(Error::InvalidLogical);
    }
    let (binding, _) = PhysicalPairBinding::decode(values[3], role, false)?;
    let (stamp, phase) = PhysicalPairBinding::decode(values[4], role, true)?;
    let phase = phase.ok_or(Error::InvalidLogical)?;
    if binding != expected
        || stamp != expected
        || (role == Role::State && phase != CreationPhase::Ready)
    {
        return Err(Error::InvalidLogical);
    }
    Ok(RoleStatus::Paired(phase))
}

fn map_insert(map: &mut BTreeMap<Vec<u8>, Vec<u8>>, key: &[u8], value: &[u8]) -> Result<()> {
    map.insert(copy(key)?, copy(value)?);
    Ok(())
}

impl RoleData {
    fn common(role: Role, binding: PhysicalPairBinding, phase: CreationPhase) -> Result<Self> {
        let mut data = Self::default();
        let format = role.format().to_be_bytes();
        let encoded_binding = binding.encode(role, None);
        let stamp = binding.encode(role, Some(phase));
        for (key, value) in [
            (FORMAT_VERSION_KEY, format.as_slice()),
            (PROFILE_KEY, role.profile()),
            (INIT_KEY, &[1]),
            (BINDING_KEY, encoded_binding.as_slice()),
            (STAMP_KEY, stamp.as_slice()),
        ] {
            map_insert(&mut data.metadata, key, value)?;
        }
        Ok(data)
    }

    pub(super) fn seed_state(
        binding: PhysicalPairBinding,
        parts: &SeedStateParts<'_>,
    ) -> Result<Self> {
        let mut data = Self::common(Role::State, binding, CreationPhase::Ready)?;
        map_insert(&mut data.metadata, LOGICAL[0], parts.header)?;
        map_insert(&mut data.metadata, LOGICAL[1], parts.fence)?;
        for &(key, value) in parts.business {
            map_insert(&mut data.records, key, value)?;
        }
        if let Some((metadata, artifact)) = parts.live {
            map_insert(&mut data.metadata, LOGICAL[5], metadata)?;
            map_insert(&mut data.metadata, LOGICAL[6], artifact)?;
        }
        data.seed_budget(Role::State)?;
        Ok(data)
    }

    pub(super) fn seed_log(
        binding: PhysicalPairBinding,
        parts: &SeedLogParts<'_>,
        phase: CreationPhase,
    ) -> Result<Self> {
        let mut data = Self::common(Role::Log, binding, phase)?;
        map_insert(&mut data.metadata, MANIFEST_KEY, parts.manifest)?;
        for (key, value) in [
            (LOGICAL[0], parts.header),
            (LOGICAL[1], parts.progress),
            (LOGICAL[2], parts.baseline),
        ] {
            map_insert(&mut data.records, key, value)?;
        }
        for &(key, value) in parts.entries {
            map_insert(&mut data.records, key, value)?;
        }
        data.seed_budget(Role::Log)?;
        Ok(data)
    }

    fn seed_budget(&self, role: Role) -> Result<()> {
        let mut budget = if role == Role::State {
            Budget::new(65_545, 134_226_944)
        } else {
            Budget::new(265, 67_272_704)
        };
        for (key, value) in self.metadata.iter().chain(&self.records) {
            budget.consume(key.len(), value.len())?;
        }
        Ok(())
    }

    pub(super) fn capture(
        &self,
        role: Role,
        binding: PhysicalPairBinding,
    ) -> Result<RoleInventory> {
        let mut shape = Shape::new(role);
        for (key, value) in &self.metadata {
            shape.metadata(key, value.len())?;
        }
        for (key, value) in &self.records {
            shape.record(key, value.len())?;
        }
        let status = if shape.finish()? {
            RoleStatus::Empty
        } else {
            let get = |key: &[u8]| {
                self.metadata
                    .get(key)
                    .map(Vec::as_slice)
                    .ok_or(Error::InvalidLogical)
            };
            fixed_records(
                role,
                binding,
                [
                    get(FORMAT_VERSION_KEY)?,
                    get(PROFILE_KEY)?,
                    get(INIT_KEY)?,
                    get(BINDING_KEY)?,
                    get(STAMP_KEY)?,
                ],
            )?
        };
        Ok(RoleInventory {
            status,
            metadata: copy_rows(&self.metadata)?,
            records: copy_rows(&self.records)?,
        })
    }

    pub(super) fn from_inventory(value: &RoleInventory) -> Result<Self> {
        let mut data = Self::default();
        for (key, value) in &value.metadata {
            map_insert(&mut data.metadata, key, value)?;
        }
        for (key, value) in &value.records {
            map_insert(&mut data.records, key, value)?;
        }
        Ok(data)
    }
}

pub(super) fn copy_rows(map: &BTreeMap<Vec<u8>, Vec<u8>>) -> Result<Rows> {
    let mut rows = Vec::new();
    rows.try_reserve_exact(map.len())
        .map_err(|_| Error::Allocation)?;
    for (key, value) in map {
        rows.push((copy(key)?, copy(value)?));
    }
    Ok(rows)
}
