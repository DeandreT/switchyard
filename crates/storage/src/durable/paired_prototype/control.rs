use super::{Error, Result};

pub(super) const BINDING_KEY: &[u8] = &[0x22, 1];
pub(super) const STAMP_KEY: &[u8] = &[0x22, 2];
pub(super) const MANIFEST_KEY: &[u8] = &[0x22, 3];
pub(super) const PROFILE_KEY: &[u8] = b"replica_profile";
pub(super) const INIT_KEY: &[u8] = b"replica_initialized";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Role {
    State,
    Log,
}

impl Role {
    pub(super) fn tag(self) -> u8 {
        match self {
            Self::State => 1,
            Self::Log => 2,
        }
    }
    pub(super) fn profile(self) -> &'static [u8] {
        match self {
            Self::State => b"committed-state-paired-adoption-v1",
            Self::Log => b"committed-log-paired-adoption-v1",
        }
    }
    pub(super) fn format(self) -> u32 {
        let active = super::super::ACTIVE_STORE_FORMAT;
        assert!(active <= 0x0fff_ffff);
        match self {
            Self::State => 0xa000_0000 | active,
            Self::Log => 0xb000_0000 | active,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CreationPhase {
    Prepared,
    Ready,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct PhysicalPairBinding {
    ids: [[u8; 16]; 3],
    node: u64,
    stream: [u8; 16],
    seed: [u8; 32],
}

impl PhysicalPairBinding {
    pub(super) fn new(
        ids: [[u8; 16]; 3],
        node: u64,
        stream: [u8; 16],
        seed: [u8; 32],
    ) -> Result<Self> {
        if ids.contains(&[0; 16])
            || stream == [0; 16]
            || ids[0] == ids[1]
            || ids[0] == ids[2]
            || ids[1] == ids[2]
        {
            return Err(Error::InvalidLogical);
        }
        Ok(Self {
            ids,
            node,
            stream,
            seed,
        })
    }

    pub(super) fn encode(self, role: Role, phase: Option<CreationPhase>) -> [u8; 112] {
        let mut bytes = [0; 112];
        bytes[..4].copy_from_slice(if phase.is_some() { b"SWCR" } else { b"SWAP" });
        bytes[4..6].copy_from_slice(&1u16.to_be_bytes());
        bytes[6] = role.tag();
        bytes[7] = u8::from(phase == Some(CreationPhase::Ready));
        for (index, id) in self.ids.iter().enumerate() {
            bytes[8 + index * 16..24 + index * 16].copy_from_slice(id);
        }
        bytes[56..64].copy_from_slice(&self.node.to_be_bytes());
        bytes[64..80].copy_from_slice(&self.stream);
        bytes[80..].copy_from_slice(&self.seed);
        bytes
    }

    pub(super) fn decode(
        bytes: &[u8],
        role: Role,
        stamp: bool,
    ) -> Result<(Self, Option<CreationPhase>)> {
        if bytes.len() != 112
            || &bytes[..4] != if stamp { b"SWCR" } else { b"SWAP" }
            || bytes[4..6] != [0, 1]
            || bytes[6] != role.tag()
            || bytes[7] > u8::from(stamp)
        {
            return Err(Error::InvalidLogical);
        }
        let mut ids = [[0; 16]; 3];
        for (index, id) in ids.iter_mut().enumerate() {
            id.copy_from_slice(&bytes[8 + index * 16..24 + index * 16]);
        }
        let node = u64::from_be_bytes(
            bytes[56..64]
                .try_into()
                .map_err(|_| Error::InvalidLogical)?,
        );
        let stream = bytes[64..80]
            .try_into()
            .map_err(|_| Error::InvalidLogical)?;
        let seed = bytes[80..].try_into().map_err(|_| Error::InvalidLogical)?;
        let binding = Self::new(ids, node, stream, seed)?;
        let phase = stamp.then_some(if bytes[7] == 0 {
            CreationPhase::Prepared
        } else {
            CreationPhase::Ready
        });
        if binding.encode(role, phase).as_slice() != bytes {
            return Err(Error::InvalidLogical);
        }
        Ok((binding, phase))
    }
}

impl std::fmt::Debug for PhysicalPairBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PhysicalPairBinding { .. }")
    }
}
