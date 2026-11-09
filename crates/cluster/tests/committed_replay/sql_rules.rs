use domain::{DEFAULT_RULE_NAME, DeadLetterReason, SequenceNumber, SqlRuleFilter};

use super::*;

/// A properties section naming `id`, one `region` application property with
/// the given encoded value, and a one-byte data section.
fn amqp(id: &str, region: &[u8]) -> Vec<u8> {
    let mut bytes = vec![
        0x00,
        0x53,
        0x73,
        0xc0,
        id.len() as u8 + 3,
        1,
        0xa1,
        id.len() as u8,
    ];
    bytes.extend(id.as_bytes());
    bytes.extend([0x00, 0x53, 0x74, 0xc1, 9 + region.len() as u8, 2, 0xa1, 6]);
    bytes.extend(b"region");
    bytes.extend(region);
    bytes.extend([0x00, 0x53, 0x75, 0xa0, 1, 0x2a]);
    bytes
}

fn rule(at: u64, name: &str, expression: &str) -> DurableProposal {
    proposal(
        &subscription(),
        at,
        CommandKind::CreateRule {
            name: RuleName::new(name).unwrap(),
            filter: RuleFilter::Sql(SqlRuleFilter::new(expression)),
        },
    )
}

pub(super) fn publish(at: u64, id: &str, region: &[u8]) -> DurableProposal {
    proposal(
        &topic(),
        at,
        CommandKind::SendContent {
            content: MessageContent::Amqp(amqp(id, region)),
        },
    )
}

fn replay(durable: bool) -> Result<Vec<(Key, Value)>, Box<dyn Error>> {
    let mut fixture = Fixture::new(durable)?;
    let originals = [
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
                config: SubscriptionConfig::default(),
            },
        ),
        proposal(
            &subscription(),
            1,
            CommandKind::DeleteRule {
                name: RuleName::new(DEFAULT_RULE_NAME)?,
            },
        ),
        rule(2, "West", "region = 'west'"),
        // Refused at apply, yet checkpointed and replayed identically.
        rule(2, "broken", "region ="),
        publish(3, "match", &[0xa1, 4, b'w', b'e', b's', b't']),
        // A string comparison against an int is a filter error.
        publish(3, "mismatch", &[0x71, 0, 0, 0, 1]),
        publish(3, "other", &[0xa1, 4, b'e', b'a', b's', b't']),
    ];
    for (p, version) in originals.iter().zip([1, 1, 1, 6, 6, 4, 4, 4]) {
        let bytes = p.encode()?;
        assert_eq!(bytes[7], version);
        assert_eq!(DurableProposal::decode(&bytes)?, *p);
    }
    let total = originals.len() as u64;
    fixture.proposals(&originals, total);
    let frozen = fixture.snapshot();
    let mut owner = fixture.open()?;
    assert_eq!(owner.replay_batch(4)?, progress(4, total, 4));
    drop(owner);
    fixture.reopen()?;
    let mut owner = fixture.open()?;
    assert_eq!(owner.replay_batch(64)?, progress(total, total, 4));
    frozen_f0(&fixture, &frozen);

    let machine = StateMachine::new(fixture.store().clone());
    let rules = machine.rules(&ns(), &subscription(), 0, 10)?;
    assert_eq!(
        rules
            .iter()
            .map(|rule| (rule.name.display_name(), &rule.filter))
            .collect::<Vec<_>>(),
        [(
            "West",
            &RuleFilter::Sql(SqlRuleFilter::new("region = 'west'"))
        )]
    );
    let rule_row = fixture
        .store()
        .get(&keys::subscription_rule(
            &ns(),
            &subscription(),
            &RuleName::new("west")?,
        ))?
        .expect("stored SQL rule");
    assert_eq!(rule_row[0], codec::rule::VALUE_FORMAT_V2);
    assert_eq!(
        machine.ready_sequences(&ns(), &subscription(), 20)?,
        [SequenceNumber::new(1)]
    );
    let dead_letter_queue = subscription().dead_letter_queue()?;
    assert_eq!(
        machine.ready_sequences(&ns(), &dead_letter_queue, 20)?,
        [SequenceNumber::new(2)]
    );
    let copy = machine
        .message(&ns(), &dead_letter_queue, SequenceNumber::new(2))?
        .expect("filter-error copy");
    let info = copy.dead_letter.expect("dead-letter information");
    assert_eq!(
        info.reason,
        DeadLetterReason::Application("FilterException".into())
    );
    assert_eq!(
        info.description,
        "rule West: SQL operands have incompatible types"
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
fn v6_sql_rules_replay_and_route_identically_across_backends() -> TestResult {
    assert_eq!(replay(false)?, replay(true)?);
    Ok(())
}

fn replay_actions(durable: bool) -> Result<Vec<(Key, Value)>, Box<dyn Error>> {
    let mut fixture = Fixture::new(durable)?;
    let originals = vec![
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
                config: SubscriptionConfig::default(),
            },
        ),
        proposal(
            &subscription(),
            2,
            CommandKind::CreateRuleWithAction {
                name: RuleName::new("remove")?,
                filter: RuleFilter::True,
                action: domain::SqlRuleAction::new("REMOVE user.region"),
            },
        ),
        proposal(
            &subscription(),
            2,
            CommandKind::CreateRuleWithAction {
                name: RuleName::new("invalid")?,
                filter: RuleFilter::True,
                action: domain::SqlRuleAction::new("SET sys.Label = 'unsupported'"),
            },
        ),
        publish(3, "original", &[0xa1, 4, b'w', b'e', b's', b't']),
        proposal(
            &subscription(),
            4,
            CommandKind::CreateRuleWithAction {
                name: RuleName::new("error")?,
                filter: RuleFilter::True,
                action: domain::SqlRuleAction::new("SET user.region = 1"),
            },
        ),
        publish(5, "error", &[0xa1, 4, b'w', b'e', b's', b't']),
    ];
    let total = originals.len() as u64;
    fixture.proposals(&originals, total);
    let mut owner = fixture.open()?;
    assert_eq!(owner.replay_batch(4)?, progress(4, total, 4));
    drop(owner);
    fixture.reopen()?;
    let mut owner = fixture.open()?;
    assert_eq!(owner.replay_batch(64)?, progress(total, total, 3));
    let machine = StateMachine::new(fixture.store().clone());
    assert_eq!(
        machine.rules(&ns(), &subscription(), 0, 10)?.len(),
        3,
        "invalid action checkpointed without admission"
    );
    let copies = machine.ready_sequences(&ns(), &subscription(), 20)?;
    assert_eq!(
        copies.iter().map(|s| s.as_u64()).collect::<Vec<_>>(),
        [1, 2, 3, 5]
    );
    let removed = machine
        .message(&ns(), &subscription(), SequenceNumber::new(2))?
        .unwrap();
    assert_eq!(
        removed.format,
        domain::MessageRecordFormat::V4 { text_ids: false }
    );
    assert_eq!(removed.envelope.as_ref().unwrap().removals(), ["region"]);
    let dead = machine
        .message(
            &ns(),
            &subscription().dead_letter_queue()?,
            SequenceNumber::new(4),
        )?
        .unwrap();
    assert_eq!(
        dead.dead_letter.as_ref().unwrap().reason.as_str(),
        "ActionException"
    );
    let rows = fixture.snapshot();
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
    assert_eq!(domain_rows(rows.clone()), domain_rows(expected.snapshot()));
    drop(machine);
    drop(owner);
    fixture.reopen()?;
    assert_eq!(fixture.snapshot(), rows);
    Ok(rows)
}

#[test]
fn v10_action_rules_and_v4_copies_replay_exactly_across_backends() -> TestResult {
    assert_eq!(replay_actions(false)?, replay_actions(true)?);
    Ok(())
}

fn quota_replay_controls(durable: bool) -> TestResult {
    for corrupt in [false, true] {
        let mut fixture = Fixture::new(durable)?;
        let other = topic().subscription(&SubscriptionName::new("other")?)?;
        let originals = vec![
            proposal(
                &topic(),
                1,
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
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
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new("other")?,
                    config: SubscriptionConfig::default(),
                },
            ),
            proposal(
                &subscription(),
                10,
                CommandKind::CreateRuleWithAction {
                    name: RuleName::new("over-quota")?,
                    filter: RuleFilter::True,
                    action: domain::SqlRuleAction::new("REMOVE user.old"),
                },
            ),
            proposal(
                &subscription(),
                11,
                CommandKind::CreateRule {
                    name: RuleName::new("later-valid")?,
                    filter: RuleFilter::False,
                },
            ),
        ];
        fixture.proposals(&originals, 5);
        fixture.seed(&originals[..3]);
        // Capacity rows are controlled fixture setup, not unjournaled runtime
        // writers or a claim that the entire historical journal was reconstructed.
        let mut batch = WriteBatch::default();
        for (entity, actions) in [(subscription(), false), (other, true)] {
            for index in 0..domain::MAX_TOPIC_SQL_RULES / 2 {
                let definition = domain::RuleDefinition {
                    name: RuleName::new(format!("q{index:04}"))?,
                    filter: if actions {
                        RuleFilter::True
                    } else {
                        RuleFilter::Sql(SqlRuleFilter::new("1 = 1"))
                    },
                    action: actions.then(|| {
                        domain::SqlRuleAction::new(if corrupt && index == 0 {
                            "SET sys.Label = 'unsupported'"
                        } else {
                            "REMOVE user.old"
                        })
                    }),
                    created_at: Timestamp::from_millis(3),
                };
                batch.push_put(
                    keys::subscription_rule(&ns(), &entity, &definition.name),
                    codec::rule::encode(&definition)?,
                );
            }
        }
        fixture.store().apply(batch)?;
        let before = fixture.snapshot();
        let mut owner = fixture.open()?;
        if corrupt {
            assert!(matches!(
                owner.replay_batch(1),
                Err(ReplayError::Indexed(IndexedApplyError::Domain(_)))
            ));
            assert_eq!(
                fixture.snapshot(),
                before,
                "corrupt sibling must not advance the quota checkpoint"
            );
            drop(owner);
            fixture.reopen()?;
            let mut owner = fixture.open()?;
            assert_eq!(owner.applied_index()?, 3);
            assert!(matches!(
                owner.replay_batch(1),
                Err(ReplayError::Indexed(IndexedApplyError::Domain(_)))
            ));
            assert_eq!(fixture.snapshot(), before);
        } else {
            assert_eq!(owner.replay_batch(1)?, progress(4, 5, 1));
            let domain_rows = |rows: Vec<(Key, Value)>| {
                rows.into_iter()
                    .filter(|(key, _)| key[0] < 0xF0)
                    .collect::<Vec<_>>()
            };
            assert_eq!(domain_rows(fixture.snapshot()), domain_rows(before));
            let checkpointed = fixture.snapshot();
            drop(owner);
            fixture.reopen()?;
            assert_eq!(fixture.snapshot(), checkpointed);
            let mut owner = fixture.open()?;
            assert_eq!(owner.applied_index()?, 4);
            assert_eq!(owner.replay_batch(1)?, progress(5, 5, 1));
            let machine = StateMachine::new(fixture.store().clone());
            assert!(
                machine
                    .rules(&ns(), &subscription(), 0, 100)?
                    .iter()
                    .any(|r| r.name.as_str() == "later-valid")
            );
            assert_eq!(machine.last_applied_time()?, Timestamp::from_millis(11));
            domain::snapshot_validation::validate_complete_state(&fixture.snapshot())?;
            drop(machine);
            drop(owner);
            fixture.reopen()?;
            let mut owner = fixture.open()?;
            caught_up(&mut owner, &fixture, 5, 5);
        }
    }
    Ok(())
}

#[test]
fn healthy_and_corrupt_sql_quota_replay_memory() -> TestResult {
    quota_replay_controls(false)
}
#[test]
fn healthy_and_corrupt_sql_quota_replay_fjall() -> TestResult {
    quota_replay_controls(true)
}
