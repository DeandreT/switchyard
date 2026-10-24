use super::*;

fn input(id: &str, scheduled: bool) -> domain::MessageInput {
    domain::MessageInput {
        message_id: id.into(),
        body: id.as_bytes().to_vec(),
        scheduled_enqueue_at: scheduled.then_some(Timestamp::from_millis(30_000)),
        ..domain::MessageInput::default()
    }
}
fn replay(durable: bool) -> Result<Vec<(Key, Value)>, Box<dyn Error>> {
    let mut fixture = Fixture::new(durable)?;
    let legacy = EntityPath::new("legacy")?;
    let invalid = EntityPath::new("invalid")?;
    let originals = [
        proposal(
            &legacy,
            0,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
        proposal(
            &topic(),
            1,
            CommandKind::CreateTopic {
                config: TopicConfig {
                    requires_duplicate_detection: true,
                    duplicate_detection_history_millis: 20_000,
                    ..TopicConfig::default()
                },
            },
        ),
        proposal(
            &topic(),
            2,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("worker")?,
                config: SubscriptionConfig::default(),
            },
        ),
        proposal(
            &topic(),
            3,
            CommandKind::UpdateTopic {
                update: TopicConfigUpdate {
                    max_message_bytes: Some(100),
                    ..TopicConfigUpdate::default()
                },
            },
        ),
        proposal(
            &topic(),
            4,
            CommandKind::SendBatch {
                messages: vec![
                    input("same", false),
                    input("same", true),
                    input("later", true),
                ],
            },
        ),
        proposal(
            &invalid,
            5,
            CommandKind::CreateTopic {
                config: TopicConfig {
                    duplicate_detection_history_millis: 0,
                    ..TopicConfig::default()
                },
            },
        ),
        proposal(
            &topic(),
            6,
            CommandKind::UpdateSubscription {
                name: SubscriptionName::new("worker")?,
                update: SubscriptionConfigUpdate {
                    max_delivery_count: Some(3),
                    ..SubscriptionConfigUpdate::default()
                },
            },
        ),
        proposal(&topic(), 30_000, CommandKind::ActivateScheduled),
        proposal(
            &topic(),
            30_001,
            CommandKind::SendBatch {
                messages: vec![input("later", false), input("later", false)],
            },
        ),
    ];
    for (p, version) in originals.iter().zip([1, 3, 1, 2, 1, 3, 2, 1, 1]) {
        let bytes = p.encode()?;
        assert_eq!(bytes[7], version);
        assert_eq!(DurableProposal::decode(&bytes)?, *p);
    }
    fixture.proposals(&originals, originals.len() as u64);
    let frozen = fixture.snapshot();
    let mut owner = fixture.open()?;
    assert_eq!(owner.replay_batch(5)?, progress(5, 9, 5));
    drop(owner);
    fixture.reopen()?;
    let mut owner = fixture.open()?;
    assert_eq!(owner.replay_batch(5)?, progress(9, 9, 4));
    frozen_f0(&fixture, &frozen);
    let machine = StateMachine::new(fixture.store().clone());
    assert!(machine.topic_config(&ns(), &invalid)?.is_none());
    assert_eq!(
        machine.duplicate_history_deadline(&ns(), &topic(), "later")?,
        Some(Timestamp::from_millis(50_001))
    );
    assert_eq!(
        machine.ready_sequences(&ns(), &subscription(), 20)?,
        [1, 4, 5].map(domain::SequenceNumber::new).to_vec()
    );
    domain::snapshot_validation::validate_complete_state(&fixture.snapshot())?;
    let expected = Fixture::new(durable)?;
    expected.seed(&originals);
    assert_eq!(
        reserved(&fixture.snapshot(), F1),
        reserved(&expected.snapshot(), F1)
    );
    assert_eq!(
        fixture.store().get(&key(F1, 1))?,
        Some(checkpoint(9, &originals[8]))
    );
    let domain_rows = |rows: Vec<(Key, Value)>| {
        rows.into_iter()
            .filter(|(key, _)| key[0] < 0xF0)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        domain_rows(fixture.snapshot()),
        domain_rows(expected.snapshot())
    );
    let complete = fixture.snapshot();
    drop(machine);
    drop(owner);
    fixture.reopen()?;
    assert_eq!(fixture.snapshot(), complete);
    let mut owner = fixture.open()?;
    caught_up(&mut owner, &fixture, 9, 9);
    Ok(complete)
}

#[test]
fn mixed_v1_v2_v3_replay_and_checkpoint_hashes_match_across_backends() -> TestResult {
    let memory = replay(false)?;
    let fjall = replay(true)?;
    assert_eq!(memory, fjall);
    let hash = |rows: &[(Key, Value)]| {
        let mut hash = Sha256::new();
        for (key, value) in rows {
            hash.update((key.len() as u64).to_be_bytes());
            hash.update(key);
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value);
        }
        hash.finalize()
    };
    assert_eq!(hash(&memory), hash(&fjall));
    Ok(())
}
