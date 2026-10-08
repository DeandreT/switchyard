use super::*;

#[test]
fn canonical_non_finite_topic_modes_roundtrip_every_generation_boundary() {
    for generation in [
        1,
        127,
        128,
        16_383,
        16_384,
        u16::MAX as u64,
        u32::MAX as u64,
        u64::MAX,
    ] {
        let mode = NonFiniteTopicMode::non_finite(generation).unwrap();
        let bytes = mode.encode().unwrap();
        assert_eq!(bytes[0], codec::VALUE_FORMAT_V11);
        assert!(bytes.len() <= MAX_TOPIC_MODE_RECORD_BYTES);
        assert_eq!(NonFiniteTopicMode::decode(&bytes, generation), Ok(mode));
        assert_eq!(mode.validate_generation(generation), Ok(()));
    }
    assert_eq!(
        NonFiniteTopicMode::non_finite(1).unwrap().encode().unwrap(),
        [11, b'T', b'M', b'O', b'D', 1, 1, 0]
    );
}

#[test]
fn topic_modes_refuse_wrong_record_schema_mode_and_owner_generation() {
    let corrupt = Err(BrokerError::TopicCapacityCorrupt);
    assert_eq!(NonFiniteTopicMode::non_finite(0), corrupt);
    for (record, schema, generation, mode) in [
        (*b"QMOD", 1_u8, 1_u64, 0_u32),
        (TOPIC_MODE_RECORD, 0, 1, 0),
        (TOPIC_MODE_RECORD, 2, 1, 0),
        (TOPIC_MODE_RECORD, 1, 0, 0),
        (TOPIC_MODE_RECORD, 1, 1, 1),
        (TOPIC_MODE_RECORD, 1, 1, u32::MAX),
    ] {
        let bytes = codec::encode(&(record, schema, generation, mode)).unwrap();
        assert_eq!(NonFiniteTopicMode::decode(&bytes, 1), corrupt);
    }
    let bytes = NonFiniteTopicMode::non_finite(1).unwrap().encode().unwrap();
    assert_eq!(NonFiniteTopicMode::decode(&bytes, 0), corrupt);
    assert_eq!(NonFiniteTopicMode::decode(&bytes, 2), corrupt);
    for queue_mode in [
        crate::queue_capacity::QueueCapacityMode::non_finite(1).unwrap(),
        crate::queue_capacity::QueueCapacityMode::finite_v1(
            1,
            std::num::NonZeroU64::new(u64::MAX).unwrap(),
        )
        .unwrap(),
    ] {
        assert_eq!(
            NonFiniteTopicMode::decode(&queue_mode.encode().unwrap(), 1),
            corrupt
        );
    }
}

#[test]
fn topic_modes_refuse_older_unknown_trailing_noncanonical_and_oversized_envelopes() {
    let canonical = NonFiniteTopicMode::non_finite(1).unwrap().encode().unwrap();
    for version in (0..=12).chain(std::iter::once(255)) {
        if version == codec::VALUE_FORMAT_V11 {
            continue;
        }
        let mut bytes = canonical.clone();
        bytes[0] = version;
        assert_eq!(
            NonFiniteTopicMode::decode(&bytes, 1),
            Err(BrokerError::TopicCapacityCorrupt)
        );
    }
    for bytes in [
        Vec::new(),
        canonical[..canonical.len() - 1].to_vec(),
        [canonical.as_slice(), &[0]].concat(),
        vec![0; MAX_TOPIC_MODE_RECORD_BYTES + 1],
        [
            canonical[..6].as_ref(),
            &[0x81, 0x00],
            canonical[7..].as_ref(),
        ]
        .concat(),
    ] {
        assert_eq!(
            NonFiniteTopicMode::decode(&bytes, 1),
            Err(BrokerError::TopicCapacityCorrupt)
        );
    }
}
