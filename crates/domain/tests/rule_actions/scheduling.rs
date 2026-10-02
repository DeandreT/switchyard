use super::*;

fn scheduled_activation_uses_current_actions_without_rewriting_existing_copies<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            default_time_to_live_millis: Some(80),
            ..TopicConfig::default()
        },
    )?;
    let child = subscribe(
        &fixture,
        "child",
        SubscriptionConfig {
            default_time_to_live_millis: Some(30),
            ..SubscriptionConfig::default()
        },
    )?;
    add(
        &fixture,
        "child",
        "old",
        RuleFilter::True,
        Some("REMOVE color"),
        0,
    )?;
    publish(&fixture, 1, vec![member("active")])?;
    let old = records(&fixture, &child)?;
    assert_eq!(old.len(), 2);
    let admission = apply(
        &fixture,
        2,
        CommandKind::ScheduleEnvelopes {
            messages: vec![
                scheduled(member("future-a"), 100),
                scheduled(member("future-b"), 100),
            ],
        },
    )?;
    assert_eq!(
        admission.outcome,
        CommandOutcome::Scheduled {
            sequences: vec![SequenceNumber::new(3), SequenceNumber::new(4)]
        }
    );
    effects(&admission, &[]);
    remove(&fixture, "child", "old", 3)?;
    add(
        &fixture,
        "child",
        "current",
        RuleFilter::True,
        Some("REMOVE number"),
        4,
    )?;
    assert_eq!(records(&fixture, &child)?, old);
    for sequence in [3, 4] {
        assert!(matches!(
            record(&fixture, &fixture.entity, sequence)?
                .expect("source schedule")
                .state,
            MessageState::Scheduled { .. }
        ));
    }
    let activation = apply(&fixture, 100, CommandKind::ActivateScheduled)?;
    assert_eq!(
        activation.outcome,
        CommandOutcome::ScheduledActivated { activated: 2 }
    );
    effects(&activation, std::slice::from_ref(&child));
    for sequence in [3, 4] {
        assert!(record(&fixture, &fixture.entity, sequence)?.is_none());
    }
    for (base, action, id) in [(5, 7, "future-a"), (6, 8, "future-b")] {
        let original = member(id);
        let mut expected = original.envelope.clone();
        expected.application_properties.remove("number");
        expected
            .application_properties
            .insert("RuleName".into(), MessageValue::String("current".into()));
        for (sequence, envelope) in [(base, &original.envelope), (action, &expected)] {
            let copy = record(&fixture, &child, sequence)?.expect("current activation copy");
            assert_eq!(copy.envelope.as_deref(), Some(envelope));
            assert_eq!(copy.enqueued_at, Timestamp::from_millis(100));
            assert_eq!(copy.expires_at, Some(Timestamp::from_millis(130)));
            assert_eq!(
                copy.scheduled_enqueue_time,
                Some(Timestamp::from_millis(100))
            );
        }
    }
    assert_eq!(record(&fixture, &child, 1)?, Some(old[0].clone()));
    assert_eq!(record(&fixture, &child, 2)?, Some(old[1].clone()));
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("handles then activation bases then actions")
            .next_sequence,
        9
    );
    Ok(())
}

for_each_backend! {
    scheduled_activation_uses_current_actions_without_rewriting_existing_copies,
}
