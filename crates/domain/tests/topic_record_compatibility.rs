use domain::{
    AdminCatalogKind, CodecError, Command, CommandKind, EntityPath, NamespaceName, StateMachine,
    Timestamp, TopicConfig, TopicConfigError, TopicConfigUpdate, codec, keys,
    snapshot_validation::{validate_catalog, validate_complete_state},
};
use storage::{StateStore, WriteBatch};
use testkit::StoreProvider;

#[test]
fn exact_legacy_bytes_and_topic_local_fence() {
    for ttl in [None, Some(17)] {
        let config = TopicConfig {
            default_time_to_live_millis: ttl,
            max_message_bytes: 42,
            ..TopicConfig::default()
        };
        let legacy = codec::encode(&(ttl, 42_usize)).unwrap();
        assert_eq!(codec::topic::encode(&config).unwrap(), legacy);
        assert_eq!(codec::topic::decode(&legacy).unwrap(), config);
        for cut in 0..legacy.len() {
            assert!(codec::topic::decode(&legacy[..cut]).is_err());
        }
        for config in [
            TopicConfig {
                requires_duplicate_detection: true,
                ..config
            },
            TopicConfig {
                duplicate_detection_history_millis: 20_000,
                ..config
            },
        ] {
            let encoded = codec::topic::encode(&config).unwrap();
            assert_eq!(encoded[0], 2);
            assert_eq!(codec::topic::decode(&encoded).unwrap(), config);
            // The old generic reader refuses the value before its old DTO.
            assert_eq!(
                codec::decode::<(Option<u64>, usize)>(&encoded),
                Err(CodecError::UnsupportedVersion { version: 2 })
            );
            for cut in 0..encoded.len() {
                assert!(codec::topic::decode(&encoded[..cut]).is_err());
            }
            let mut trailing = encoded;
            trailing.push(0);
            assert!(codec::topic::decode(&trailing).is_err());
        }
        let mut trailing = legacy;
        trailing.push(0);
        assert!(codec::topic::decode(&trailing).is_err());
    }
    let mut redundant = vec![2];
    redundant.extend(postcard::to_stdvec(&TopicConfig::default()).unwrap());
    assert_eq!(codec::topic::decode(&redundant), Err(CodecError::Decode));
    assert_eq!(
        codec::topic::decode(&[99, 0]),
        Err(CodecError::UnsupportedVersion { version: 99 })
    );
    // Nonminimal postcard encoding of max_message_bytes=1.
    assert_eq!(
        codec::topic::decode(&[1, 0, 0x81, 0]),
        Err(CodecError::Decode)
    );
}

#[test]
fn window_bounds_apply_even_when_disabled() {
    let default = TopicConfig::default();
    assert!(!default.requires_duplicate_detection);
    assert_eq!(default.duplicate_detection_history_millis, 600_000);
    for enabled in [false, true] {
        for window in [20_000, 604_800_000] {
            assert!(
                TopicConfig {
                    requires_duplicate_detection: enabled,
                    duplicate_detection_history_millis: window,
                    ..default
                }
                .validate()
                .is_ok()
            );
        }
        for window in [0, 19_999, 604_800_001, u64::MAX] {
            let error = TopicConfig {
                requires_duplicate_detection: enabled,
                duplicate_detection_history_millis: window,
                ..default
            }
            .validate()
            .unwrap_err();
            assert!(matches!(
                error,
                TopicConfigError::DuplicateDetectionHistoryTooShort { .. }
                    | TopicConfigError::DuplicateDetectionHistoryTooLong { .. }
            ));
        }
    }
}

fn readers<P: StoreProvider>(provider: P) -> Result<(), Box<dyn std::error::Error>> {
    let ns = NamespaceName::new("n")?;
    let topic = EntityPath::new("t")?;
    let machine = StateMachine::new(provider.open()?);
    let config = TopicConfig {
        requires_duplicate_detection: true,
        ..TopicConfig::default()
    };
    machine.apply(&Command::new(
        ns.clone(),
        topic.clone(),
        Timestamp::from_millis(1),
        CommandKind::CreateTopic { config },
    ))?;
    machine.apply(&Command::new(
        ns.clone(),
        topic.clone(),
        Timestamp::from_millis(2),
        CommandKind::UpdateTopic {
            update: TopicConfigUpdate {
                max_message_bytes: Some(12),
                ..TopicConfigUpdate::default()
            },
        },
    ))?;
    let expected = TopicConfig {
        max_message_bytes: 12,
        ..config
    };
    assert_eq!(machine.topic_config(&ns, &topic)?, Some(expected));
    assert_eq!(
        machine
            .admin_catalog_page(&ns, AdminCatalogKind::Topics, None, 1)?
            .entries
            .len(),
        1
    );
    let snapshot = machine.store().snapshot()?;
    validate_catalog(snapshot.entries())?;
    validate_complete_state(snapshot.entries())?;
    let key = keys::topic_config(&ns, &topic);
    let mut corrupt = machine.store().get(&key)?.unwrap();
    corrupt.push(0);
    machine
        .store()
        .apply(WriteBatch::default().put(key, corrupt))?;
    let before = machine.store().snapshot()?;
    assert!(machine.topic_config(&ns, &topic).is_err());
    assert!(
        machine
            .admin_catalog_page(&ns, AdminCatalogKind::Topics, None, 1)
            .is_err()
    );
    assert!(validate_catalog(before.entries()).is_err());
    assert!(validate_complete_state(before.entries()).is_err());
    assert_eq!(machine.store().snapshot()?, before);
    Ok(())
}

#[test]
fn readers_memory() -> Result<(), Box<dyn std::error::Error>> {
    readers(testkit::MemoryProvider::new())
}
#[test]
fn readers_fjall() -> Result<(), Box<dyn std::error::Error>> {
    readers(testkit::DurableProvider::temporary()?)
}
