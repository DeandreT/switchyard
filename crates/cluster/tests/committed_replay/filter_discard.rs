use domain::{DEFAULT_RULE_NAME, SequenceNumber, SqlRuleFilter};

use super::{sql_rules::publish, *};

fn keeper() -> EntityPath {
    topic()
        .subscription(&SubscriptionName::new("keeper").unwrap())
        .unwrap()
}

fn west_only(entity: &EntityPath, at: u64) -> [DurableProposal; 2] {
    [
        proposal(
            entity,
            at,
            CommandKind::DeleteRule {
                name: RuleName::new(DEFAULT_RULE_NAME).unwrap(),
            },
        ),
        proposal(
            entity,
            at,
            CommandKind::CreateRule {
                name: RuleName::new("west").unwrap(),
                filter: RuleFilter::Sql(SqlRuleFilter::new("region = 'west'")),
            },
        ),
    ]
}

fn update(at: u64, dead_letter: bool) -> DurableProposal {
    proposal(
        &topic(),
        at,
        CommandKind::UpdateSubscription {
            name: SubscriptionName::new("worker").unwrap(),
            update: SubscriptionConfigUpdate {
                dead_lettering_on_filter_evaluation_exceptions: Some(dead_letter),
                ..SubscriptionConfigUpdate::default()
            },
        },
    )
}

// A string comparison against an int is a filter error.
const MISMATCH: [u8; 5] = [0x71, 0, 0, 0, 1];

fn replay(durable: bool) -> Result<Vec<(Key, Value)>, Box<dyn Error>> {
    let mut fixture = Fixture::new(durable)?;
    let mut originals = vec![
        proposal(
            &topic(),
            0,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
        proposal(
            &topic(),
            1,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("worker")?,
                config: SubscriptionConfig {
                    dead_lettering_on_filter_evaluation_exceptions: false,
                    ..SubscriptionConfig::default()
                },
            },
        ),
        proposal(
            &topic(),
            1,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("keeper")?,
                config: SubscriptionConfig::default(),
            },
        ),
    ];
    originals.extend(west_only(&subscription(), 2));
    originals.extend(west_only(&keeper(), 2));
    originals.extend([
        // Sequence 1: the worker discards; the keeper dead-letters.
        publish(3, "mismatch", &MISMATCH),
        update(4, true),
        // Sequence 2: both dead-letter.
        publish(5, "again", &MISMATCH),
        update(6, false),
        // Sequence 3: matching still delivers to both.
        publish(7, "match", &[0xa1, 4, b'w', b'e', b's', b't']),
    ]);
    for (p, version) in originals.iter().zip([1, 8, 1, 1, 6, 1, 6, 4, 8, 4, 8, 4]) {
        let bytes = p.encode()?;
        assert_eq!(bytes[7], version);
        assert_eq!(DurableProposal::decode(&bytes)?, *p);
    }
    let total = originals.len() as u64;
    fixture.proposals(&originals, total);
    let frozen = fixture.snapshot();
    let mut owner = fixture.open()?;
    assert_eq!(owner.replay_batch(5)?, progress(5, total, 5));
    drop(owner);
    fixture.reopen()?;
    let mut owner = fixture.open()?;
    assert_eq!(
        owner.replay_batch(64)?,
        progress(total, total, total as usize - 5)
    );
    frozen_f0(&fixture, &frozen);

    let machine = StateMachine::new(fixture.store().clone());
    let membership = |entity: &EntityPath, name: &str| {
        fixture
            .store()
            .get(&keys::topic_subscription(
                &ns(),
                &topic(),
                &SubscriptionName::new(name).unwrap(),
            ))
            .unwrap()
            .map(|value| (value, codec::encode(entity).unwrap()))
            .expect("membership")
    };
    let (worker_row, legacy) = membership(&subscription(), "worker");
    assert_eq!(worker_row[0], codec::membership::VALUE_FORMAT_V2);
    assert_ne!(worker_row, legacy);
    let (keeper_row, legacy) = membership(&keeper(), "keeper");
    assert_eq!(keeper_row, legacy);
    let sequences = |entity: &EntityPath| machine.ready_sequences(&ns(), entity, 20).unwrap();
    assert_eq!(sequences(&subscription()), [SequenceNumber::new(3)]);
    assert_eq!(sequences(&keeper()), [SequenceNumber::new(3)]);
    assert_eq!(
        sequences(&subscription().dead_letter_queue()?),
        [SequenceNumber::new(2)]
    );
    assert_eq!(
        sequences(&keeper().dead_letter_queue()?),
        [SequenceNumber::new(1), SequenceNumber::new(2)]
    );
    let rows = fixture.snapshot();
    domain::snapshot_validation::validate_catalog(&rows)?;
    domain::snapshot_validation::validate_complete_state(&rows)?;
    domain::legacy_content::validate_legacy_content(&rows)?;

    let expected = Fixture::new(durable)?;
    expected.seed(&originals);
    assert_eq!(reserved(&rows, F1), reserved(&expected.snapshot(), F1));
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
    caught_up(&mut owner, &fixture, total, total);
    Ok(complete)
}

#[test]
fn v8_filter_error_discard_replays_and_routes_identically_across_backends() -> TestResult {
    assert_eq!(replay(false)?, replay(true)?);
    Ok(())
}
