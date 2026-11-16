use super::{
    ApplicationError,
    composition::{F2View, Hub},
};
use crate::{
    CommittedReplay, Journal,
    consensus::{ConsensusIdentity, ConsensusStore, EntryKind, RetainedLogLimits, entry_kind},
};
use domain::{
    DurableProposal, IndexedWriter, legacy_content::validate_legacy_content,
    snapshot_validation::validate_complete_state,
};
use storage::StateStore;

/// Entire physical image is inspected before its F2-filtered view is entrusted
/// to the unchanged ordinary ConsensusStore opener. This token never escapes.
pub(super) struct Validated {
    store: ConsensusStore<F2View>,
    hub: Hub,
    actual: u64,
}
impl Validated {
    pub fn consume(self) -> (ConsensusStore<F2View>, Hub, u64) {
        (self.store, self.hub, self.actual)
    }
}
pub(super) fn open(
    hub: &Hub,
    identity: ConsensusIdentity,
    local: u64,
    retained: RetainedLogLimits,
    fresh: bool,
) -> Result<Validated, ApplicationError> {
    let rows = hub.image()?;
    if fresh {
        if !rows.is_empty() {
            return Err(ApplicationError::Corrupt(
                "fresh mode requires physically empty backend",
            ));
        }
        return Ok(Validated {
            store: ConsensusStore::bootstrap(hub.f2(), identity, local, retained)?,
            hub: hub.clone(),
            actual: 0,
        });
    }
    for (k, _) in &rows {
        match k[0] {
            0..=0x11 | 0xf1 | 0xf2 => {}
            0xf0 => crate::journal::validate_snapshot_namespace(k)?,
            _ => {
                return Err(ApplicationError::Corrupt(
                    "unowned physical row or closed capacity fence",
                ));
            }
        }
    }
    let domain_rows: Vec<_> = rows.iter().filter(|(k, _)| k[0] <= 0x11).cloned().collect();
    let domain = validate_complete_state(&domain_rows)
        .map_err(|e| ApplicationError::Validation(e.to_string()))?;
    validate_legacy_content(&domain_rows)
        .map_err(|e| ApplicationError::Validation(e.to_string()))?;
    for (k, _) in &domain_rows {
        if k[0] != 0 {
            let (ns, _) = domain::keys::entity_scope_parts(k)
                .ok_or(ApplicationError::Corrupt("invalid domain scope"))?;
            if ns != identity.namespace().as_str() {
                return Err(ApplicationError::Corrupt("foreign domain namespace"));
            }
        }
    }
    let readonly = hub.readonly();
    domain::capacity_fence::check_recovery(&readonly)
        .map_err(|e| ApplicationError::Validation(e.to_string()))?;
    // Read-only open routines validate every exact F0/F1 record; no replay is
    // performed and no writable domain capability is supplied to these owners.
    let journal = Journal::open(readonly.clone())?;
    if journal.snapshot_anchor()?.is_some()
        || IndexedWriter::persisted_owner_version(&readonly)?.is_some_and(|v| v != 1)
    {
        return Err(ApplicationError::Corrupt(
            "anchored application ownership is unsupported",
        ));
    }
    let replay = CommittedReplay::open(readonly.clone())?;
    let (a, j, l) = (
        replay.applied_index()?,
        replay.committed_index()?,
        replay.last_appended_index()?,
    );
    if a == 0 && !domain_rows.is_empty() {
        return Err(ApplicationError::Corrupt(
            "unindexed populated domain state",
        ));
    }
    // F1 with no Clock/domain rows IS valid: a first refusal or no-effect apply
    // atomically publishes its checkpoint, without inventing a Clock write.
    let store = ConsensusStore::open(hub.f2(), identity, local, retained)?;
    let mut ordinal = 0;
    let mut actual = 0;
    let mut max_applied_time = domain::Timestamp::UNIX_EPOCH;
    let mut blocked = false;
    for r in 1..=store.hard_state()?.commit {
        let entry = store
            .read_entries(r, 1)?
            .pop()
            .ok_or(ApplicationError::Corrupt("missing committed F2"))?;
        if entry_kind(&entry) == EntryKind::Proposal {
            ordinal += 1;
            let proposal = DurableProposal::decode(&entry.data)?;
            if ordinal <= l {
                let key = crate::journal::entry_key(ordinal);
                let bytes = readonly
                    .get(&key)?
                    .ok_or(ApplicationError::Corrupt("missing F0 original"))?;
                if crate::journal::decode_entry(&key, &bytes)?.payload() != entry.data.as_ref() {
                    return Err(ApplicationError::Corrupt(
                        "F0 conflicts with exact committed F2 original",
                    ));
                }
            }
            if ordinal > a {
                blocked = true;
            } else {
                max_applied_time = max_applied_time.max(proposal.command().issued_at);
            }
        }
        if !blocked {
            actual = r;
        }
    }
    if a > j || j > l || l > ordinal {
        return Err(ApplicationError::Corrupt(
            "application frontier exceeds committed authority",
        ));
    }
    if domain
        .catalog()
        .clock()
        .is_some_and(|clock| clock > max_applied_time)
    {
        return Err(ApplicationError::Corrupt(
            "Clock ahead of all applied original times",
        ));
    }
    Ok(Validated {
        store,
        hub: hub.clone(),
        actual,
    })
}
