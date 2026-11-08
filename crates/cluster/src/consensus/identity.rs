use std::{collections::BTreeSet, net::SocketAddr};

use domain::NamespaceName;
use sha2::{Digest, Sha256};

use super::ConsensusError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Voter {
    pub id: u64,
    pub endpoint: SocketAddr,
}

/// Immutable, canonical fixed-three configuration, shared by all replicas.
/// Endpoint declarations and hashes are not peer authentication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsensusIdentity {
    cluster: String,
    group: String,
    namespace: NamespaceName,
    voters: [Voter; 3],
}

impl ConsensusIdentity {
    pub fn new(
        cluster: String,
        group: String,
        namespace: NamespaceName,
        mut voters: [Voter; 3],
    ) -> Result<Self, ConsensusError> {
        for value in [&cluster, &group] {
            if value.is_empty()
                || value.len() > 256
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            {
                return Err(ConsensusError::Identity(
                    "cluster/group must be 1..=256 ASCII identifier bytes",
                ));
            }
        }
        voters.sort_by_key(|v| v.id);
        let mut endpoints = BTreeSet::new();
        for (position, voter) in voters.iter().enumerate() {
            if voter.id == 0 || (position > 0 && voters[position - 1].id == voter.id) {
                return Err(ConsensusError::Identity(
                    "voters must have three unique nonzero IDs",
                ));
            }
            if voter.endpoint.port() == 0
                || voter.endpoint.ip().is_unspecified()
                || voter.endpoint.ip().is_multicast()
                || matches!(voter.endpoint, SocketAddr::V6(address) if address.flowinfo() != 0)
                || !endpoints.insert(voter.endpoint)
            {
                return Err(ConsensusError::Identity(
                    "voters require three distinct concrete endpoints",
                ));
            }
        }
        Ok(Self {
            cluster,
            group,
            namespace,
            voters,
        })
    }

    pub fn cluster(&self) -> &str {
        &self.cluster
    }
    pub fn group(&self) -> &str {
        &self.group
    }
    pub fn namespace(&self) -> &NamespaceName {
        &self.namespace
    }
    pub fn voters(&self) -> &[Voter; 3] {
        &self.voters
    }
    pub fn contains(&self, id: u64) -> bool {
        self.voters.iter().any(|v| v.id == id)
    }

    pub fn fingerprint(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"switchyard fixed consensus identity v1\0");
        hash.update(self.encode());
        hash.finalize().into()
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in [
            &self.cluster,
            &self.group,
            &self.namespace.as_str().to_owned(),
        ] {
            bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
        for voter in &self.voters {
            bytes.extend_from_slice(&voter.id.to_be_bytes());
            let endpoint = voter.endpoint.to_string();
            bytes.extend_from_slice(&(endpoint.len() as u32).to_be_bytes());
            bytes.extend_from_slice(endpoint.as_bytes());
        }
        bytes
    }

    pub(super) fn replica_bytes(&self, local_id: u64) -> Result<Vec<u8>, ConsensusError> {
        if !self.contains(local_id) {
            return Err(ConsensusError::Identity(
                "local ID is not a configured voter",
            ));
        }
        let mut bytes = self.encode();
        bytes.extend_from_slice(&local_id.to_be_bytes());
        bytes.extend_from_slice(&self.fingerprint());
        Ok(bytes)
    }
}
