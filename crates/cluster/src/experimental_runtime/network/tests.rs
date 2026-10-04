use std::sync::Arc;

use domain::CommittedStreamId;
use openraft::BasicNode;

use super::registry::Routes;
use crate::{LogEntry, LogId};

pub(super) fn routes() -> Arc<Routes> {
    Routes::new(
        CommittedStreamId::new([41; 16]).expect("nonzero stream"),
        [7, 8, 9],
    )
    .expect("fixed routes")
}

pub(super) fn membership() -> openraft::Membership<u64, BasicNode> {
    openraft::Membership::new(
        vec![[7, 8, 9].into_iter().collect()],
        [7, 8, 9]
            .into_iter()
            .map(|id| (id, BasicNode::new(super::registry::stable_label(id))))
            .collect::<std::collections::BTreeMap<_, _>>(),
    )
}

pub(super) fn initial() -> LogEntry {
    LogEntry {
        log_id: LogId::default(),
        payload: openraft::EntryPayload::Membership(membership()),
    }
}

pub(super) fn blank(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(1, 7), index),
        payload: openraft::EntryPayload::Blank,
    }
}
