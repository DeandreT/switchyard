use super::{MAX_REPLICA_PAYLOAD_ENTRIES, ReplicaPreparationError};

pub(super) fn no_snapshot_config() -> Result<openraft::Config, ReplicaPreparationError> {
    openraft::Config {
        snapshot_policy: openraft::SnapshotPolicy::Never,
        max_payload_entries: MAX_REPLICA_PAYLOAD_ENTRIES,
        replication_lag_threshold: crate::MAX_RETAINED_ENTRIES + 1,
        max_in_snapshot_log_to_keep: crate::MAX_RETAINED_ENTRIES,
        ..openraft::Config::default()
    }
    .validate()
    .map_err(|_| ReplicaPreparationError::Configuration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_only_replication_limit_fits_maximum_encoded_entries() {
        assert_eq!(MAX_REPLICA_PAYLOAD_ENTRIES, 15);
        assert!(
            MAX_REPLICA_PAYLOAD_ENTRIES as usize * crate::MAX_LOG_ENTRY_BYTES
                <= crate::MAX_APPEND_BYTES
        );
        assert!(
            (MAX_REPLICA_PAYLOAD_ENTRIES as usize + 1) * crate::MAX_LOG_ENTRY_BYTES
                > crate::MAX_APPEND_BYTES
        );
        let config = no_snapshot_config().expect("bounded no-snapshot configuration");
        assert_eq!(config.snapshot_policy, openraft::SnapshotPolicy::Never);
        assert_eq!(config.max_payload_entries, MAX_REPLICA_PAYLOAD_ENTRIES);
        assert!(config.replication_lag_threshold > crate::MAX_RETAINED_ENTRIES);
    }
}
