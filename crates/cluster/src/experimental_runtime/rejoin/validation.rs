use std::{collections::BTreeMap, sync::Arc};

use domain::CommittedStreamId;
use openraft::{BasicNode, Membership};

use crate::ExperimentalReplicaStores;

use super::super::{
    Error,
    continuity::RetirementEvidence,
    startup::{self, Mode},
};

pub(super) async fn validate(
    stores: &mut ExperimentalReplicaStores,
    node_id: u64,
    stream: CommittedStreamId,
    members: &BTreeMap<u64, BasicNode>,
    floor: &Arc<RetirementEvidence>,
) -> Result<(), Error> {
    if members.len() != 3
        || !members.contains_key(&node_id)
        || stores.progress().node_id() != node_id
        || stores.progress().stream() != stream
        || floor.log.profile().node_id() != node_id
        || floor.log.profile().stream() != stream
        || floor.checkpoint.stream() != stream
    {
        return Err(Error::ProfileMismatch);
    }

    let expected = Membership::new(vec![members.keys().copied().collect()], members.clone());
    // Reuse refreshed full-history, fixed-membership, and startup headroom
    // checks. A stopped pristine voter may legitimately have an empty floor;
    // the cluster, not this individual candidate, owns initialized membership.
    let _initialized = startup::validate(stores, Mode::Open, &expected).await?;
    let (candidate_log, candidate_checkpoint) = stores
        .continuity_snapshot()
        .await
        .map_err(|_| Error::Storage)?;
    if candidate_log != floor.log || candidate_checkpoint != floor.checkpoint {
        return Err(Error::InvalidHistory);
    }
    stores.pause_runtime_ticks();
    Ok(())
}

#[cfg(test)]
#[path = "validation_tests.rs"]
mod tests;
