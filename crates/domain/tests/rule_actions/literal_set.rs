use super::*;

#[path = "literal_set/scheduling.rs"]
mod scheduling;
use scheduling::{
    literal_set_current_actions_are_evaluated_at_scheduled_activation,
    literal_set_unfit_activation_head_remains_pending_and_cancelable,
};

fn properties(record: &MessageRecord) -> &BTreeMap<String, MessageValue> {
    &record
        .envelope
        .as_ref()
        .expect("typed retained copy")
        .application_properties
}

fn subscribe_at<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    name: &str,
    config: SubscriptionConfig,
    millis: u64,
) -> TestResult<EntityPath> {
    let subscription = SubscriptionName::new(name)?;
    fixture.at(
        millis,
        CommandKind::CreateSubscription {
            name: subscription.clone(),
            config,
        },
    )?;
    Ok(fixture.entity.subscription(&subscription)?)
}

fn refuses<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    kind: CommandKind,
) -> TestResult<BrokerError> {
    let before = fixture.machine.store().snapshot()?;
    let clock = fixture.machine.last_applied_time()?;
    let error = apply(fixture, millis, kind).expect_err("whole command must refuse");
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, clock);
    Ok(error)
}

fn literal_set_copies_use_original_filters_and_isolated_overrides<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let alpha = subscribe(&fixture, "alpha", SubscriptionConfig::default())?;
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default())?;
    add(&fixture, "alpha", "plain", RuleFilter::True, None, 0)?;
    add(
        &fixture,
        "alpha",
        "a-change",
        RuleFilter::True,
        Some("SET color='Blue';SET number=11;SET added=TRUE"),
        0,
    )?;
    add(
        &fixture,
        "alpha",
        "b-original",
        RuleFilter::Sql(SqlFilter::new("color = 'Red' AND number = 7")?),
        Some("SET number=-7"),
        0,
    )?;
    let original = member("one");
    let application = publish(&fixture, 1, vec![original.clone()])?;
    effects(&application, &[alpha.clone(), beta.clone()]);
    let copies = records(&fixture, &alpha)?;
    assert_eq!(copies.len(), 3);
    assert_eq!(
        copies
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        [
            SequenceNumber::new(1),
            SequenceNumber::new(2),
            SequenceNumber::new(3)
        ]
    );
    assert_eq!(copies[0].envelope.as_deref(), Some(&original.envelope));
    assert_eq!(
        properties(&copies[1])["color"],
        MessageValue::String("Blue".into())
    );
    assert_eq!(properties(&copies[1])["number"], MessageValue::Int(11));
    assert_eq!(properties(&copies[1])["added"], MessageValue::Bool(true));
    assert_eq!(
        properties(&copies[2])["color"],
        MessageValue::String("Red".into())
    );
    assert_eq!(properties(&copies[2])["number"], MessageValue::Int(-7));
    assert!(!properties(&copies[2]).contains_key("added"));
    for (copy, name) in copies[1..].iter().zip(["a-change", "b-original"]) {
        assert_eq!(
            properties(copy)["RuleName"],
            MessageValue::String(name.into())
        );
        assert_eq!(copy.body, original.body);
        assert_eq!(
            copy.envelope.as_ref().expect("envelope").properties,
            original.envelope.properties
        );
    }
    assert_eq!(
        records(&fixture, &beta)?[0].envelope.as_deref(),
        Some(&original.envelope)
    );
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("parent sequences")
            .next_sequence,
        4
    );
    Ok(())
}

fn literal_set_legacy_bodies_and_final_rule_name_are_authoritative<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default())?;
    remove(&fixture, "child", "$Default", 0)?;
    add(
        &fixture,
        "child",
        "a-final",
        RuleFilter::True,
        Some("SET RuleName='ignored';SET rulename='case kept';SET added=7"),
        0,
    )?;
    add(
        &fixture,
        "child",
        "b-final",
        RuleFilter::True,
        Some("SET RuleName='ignored';REMOVE RuleName;SET flag=FALSE"),
        0,
    )?;
    apply(
        &fixture,
        1,
        CommandKind::Send {
            message_id: "legacy".into(),
            body: vec![0, 255, 7],
            time_to_live_millis: None,
            session_id: None,
        },
    )?;
    let copies = records(&fixture, &child)?;
    assert_eq!(copies.len(), 2);
    for (copy, name) in copies.iter().zip(["a-final", "b-final"]) {
        assert_eq!(
            copy.envelope.as_ref().expect("legacy materialized").body,
            MessageBody::Data(vec![vec![0, 255, 7]])
        );
        assert_eq!(
            properties(copy)["RuleName"],
            MessageValue::String(name.into())
        );
    }
    assert_eq!(properties(&copies[0])["added"], MessageValue::Long(7));
    assert_eq!(
        properties(&copies[0])["rulename"],
        MessageValue::String("case kept".into())
    );
    assert!(!properties(&copies[1]).contains_key("rulename"));
    Ok(())
}

fn literal_set_failure_dead_letters_one_original_action_copy<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(
        &fixture,
        "child",
        SubscriptionConfig {
            dead_lettering_on_filter_evaluation_exceptions: false,
            ..SubscriptionConfig::default()
        },
    )?;
    let sibling = subscribe(&fixture, "sibling", SubscriptionConfig::default())?;
    let shadow = child.dead_letter_queue()?;
    add(
        &fixture,
        "child",
        "a-good",
        RuleFilter::True,
        Some("SET number=9"),
        0,
    )?;
    add(
        &fixture,
        "child",
        "b-fail",
        RuleFilter::True,
        Some("REMOVE color;SET number='private-value'"),
        0,
    )?;
    let mut original = member("failure");
    original.time_to_live_millis = Some(1000);
    original.session_id = Some(SessionId::new("original-session")?);
    let application = publish(&fixture, 1, vec![original.clone()])?;
    effects(
        &application,
        &[child.clone(), shadow.clone(), sibling.clone()],
    );
    assert_eq!(records(&fixture, &child)?.len(), 2);
    let failures = records(&fixture, &shadow)?;
    assert_eq!(failures.len(), 1);
    let failure = &failures[0];
    assert_eq!(failure.sequence, SequenceNumber::new(3));
    let mut expected = original.envelope.clone();
    expected
        .application_properties
        .insert("RuleName".into(), MessageValue::String("b-fail".into()));
    assert_eq!(failure.envelope.as_deref(), Some(&expected));
    assert_eq!(failure.body, original.body);
    assert_eq!(failure.expires_at, None);
    assert_eq!(failure.session_id, None);
    let detail = failure
        .dead_letter
        .as_ref()
        .expect("local conversion details");
    assert_eq!(
        detail.reason,
        DeadLetterReason::Application("SwitchyardSqlActionError".into())
    );
    assert_eq!(detail.description, "TypeMismatch");
    assert!(!format!("{detail:?}").contains("private-value"));
    assert_eq!(
        properties(&records(&fixture, &child)?[1])["number"],
        MessageValue::Int(9)
    );
    assert_eq!(
        records(&fixture, &sibling)?[0].envelope.as_deref(),
        Some(&original.envelope)
    );
    Ok(())
}

fn literal_set_filter_errors_and_missing_sessions_keep_defined_priority<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let filtered = subscribe(
        &fixture,
        "filtered",
        SubscriptionConfig {
            requires_session: true,
            ..SubscriptionConfig::default()
        },
    )?;
    let session = subscribe(
        &fixture,
        "session",
        SubscriptionConfig {
            requires_session: true,
            ..SubscriptionConfig::default()
        },
    )?;
    add(
        &fixture,
        "filtered",
        "a-filter-error",
        RuleFilter::Sql(SqlFilter::new("number / 0 = 1")?),
        Some("SET number='bad'"),
        0,
    )?;
    add(
        &fixture,
        "filtered",
        "b-action-error",
        RuleFilter::True,
        Some("SET number='bad'"),
        0,
    )?;
    add(
        &fixture,
        "session",
        "a-good",
        RuleFilter::True,
        Some("SET number=11"),
        0,
    )?;
    add(
        &fixture,
        "session",
        "b-action-error",
        RuleFilter::True,
        Some("SET number='bad'"),
        0,
    )?;
    publish(&fixture, 1, vec![member("priority")])?;
    assert!(records(&fixture, &filtered)?.is_empty());
    assert!(records(&fixture, &session)?.is_empty());
    let filter_copies = records(&fixture, &filtered.dead_letter_queue()?)?;
    assert_eq!(filter_copies.len(), 1);
    assert_eq!(
        filter_copies[0]
            .dead_letter
            .as_ref()
            .expect("filter precedence")
            .reason,
        DeadLetterReason::Application("SwitchyardSqlFilterError".into())
    );
    assert!(!properties(&filter_copies[0]).contains_key("RuleName"));
    let session_copies = records(&fixture, &session.dead_letter_queue()?)?;
    assert_eq!(session_copies.len(), 3);
    assert_eq!(
        session_copies[0]
            .dead_letter
            .as_ref()
            .expect("base session error")
            .reason,
        DeadLetterReason::MissingSessionId
    );
    assert_eq!(
        session_copies[1]
            .dead_letter
            .as_ref()
            .expect("good session error")
            .reason,
        DeadLetterReason::MissingSessionId
    );
    assert_eq!(
        properties(&session_copies[1])["number"],
        MessageValue::Int(11)
    );
    assert_eq!(
        session_copies[2]
            .dead_letter
            .as_ref()
            .expect("action precedence")
            .reason,
        DeadLetterReason::Application("SwitchyardSqlActionError".into())
    );
    assert_eq!(
        properties(&session_copies[2])["number"],
        MessageValue::Int(7)
    );
    Ok(())
}

fn retained_cost(
    message: &IngressEnvelope,
    annotation: Option<&str>,
    added: Option<&str>,
) -> usize {
    let mut envelope = message.envelope.clone();
    if let Some(value) = added {
        envelope
            .application_properties
            .insert("added".into(), MessageValue::String(value.into()));
    }
    if let Some(name) = annotation {
        envelope
            .application_properties
            .insert("RuleName".into(), MessageValue::String(name.into()));
    }
    envelope.content_size() + message.body.len() + message.message_id.len()
}

fn full_header(id: &str) -> IngressEnvelope {
    let mut message = member(id);
    for index in 0..8 {
        message.envelope.application_properties.insert(
            format!("pad{index}"),
            MessageValue::String("x".repeat(8000)),
        );
    }
    let target = domain::MAX_MESSAGE_HEADER_BYTES - domain::BROKER_HEADER_RESERVE_BYTES;
    let extra = target - message.envelope.header_content_size();
    message.envelope.application_properties.insert(
        "pad0".into(),
        MessageValue::String("x".repeat(8000 + extra)),
    );
    assert_eq!(
        message.envelope.header_content_size() + domain::BROKER_HEADER_RESERVE_BYTES,
        domain::MAX_MESSAGE_HEADER_BYTES
    );
    message
}

fn literal_set_caps_refuse_whole_publications_before_state_changes<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fail_next = Arc::new(AtomicBool::new(false));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let fixture = topic(
        ObservedProvider {
            inner: provider,
            fail_next,
            observations: observations.clone(),
        },
        TopicConfig {
            requires_duplicate_detection: true,
            max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
            ..TopicConfig::default()
        },
    )?;
    let child = subscribe(
        &fixture,
        "child",
        SubscriptionConfig {
            max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
            ..SubscriptionConfig::default()
        },
    )?;
    add(
        &fixture,
        "child",
        "a-good",
        RuleFilter::True,
        Some("SET number=9"),
        0,
    )?;
    add(
        &fixture,
        "child",
        "b-fail",
        RuleFilter::True,
        Some("SET number='bad'"),
        0,
    )?;
    let committed = observations.lock().expect("observations").commits;
    let error = refuses(
        &fixture,
        10,
        CommandKind::SendBatch {
            messages: (0..MAX_TOPIC_FANOUT_COPIES / 3 + 1)
                .map(|index| member(&format!("cap-{index}")))
                .collect(),
        },
    )?;
    assert!(matches!(
        error,
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::Messages,
            maximum: MAX_TOPIC_FANOUT_COPIES
        }
    ));
    assert_eq!(
        observations.lock().expect("observations").commits,
        committed
    );
    publish(
        &fixture,
        1,
        (0..MAX_TOPIC_FANOUT_COPIES / 3)
            .map(|index| member(&format!("cap-{index}")))
            .collect(),
    )?;
    assert_eq!(
        records(&fixture, &child)?.len(),
        2 * (MAX_TOPIC_FANOUT_COPIES / 3)
    );
    assert_eq!(
        records(&fixture, &child.dead_letter_queue()?)?.len(),
        MAX_TOPIC_FANOUT_COPIES / 3
    );

    remove(&fixture, "child", "a-good", 2)?;
    remove(&fixture, "child", "b-fail", 2)?;
    let mut bytes = member("bytes");
    bytes.body.clear();
    bytes.envelope.body = MessageBody::Data(vec![vec![]]);
    let padding = (0..6)
        .find(|count| {
            let value = "x".repeat(*count);
            let cost = retained_cost(&bytes, None, None)
                + retained_cost(&bytes, Some("a-good"), Some(&value))
                + retained_cost(&bytes, Some("b-fail"), None)
                + "SwitchyardSqlActionError".len()
                + "TypeMismatch".len();
            (MAX_TOPIC_FANOUT_CONTENT_BYTES - cost).is_multiple_of(6)
        })
        .expect("one-byte literal adjusts the exact six-body-byte slope");
    let value = "x".repeat(padding);
    let fixed = retained_cost(&bytes, None, None)
        + retained_cost(&bytes, Some("a-good"), Some(&value))
        + retained_cost(&bytes, Some("b-fail"), None)
        + "SwitchyardSqlActionError".len()
        + "TypeMismatch".len();
    let body_bytes = (MAX_TOPIC_FANOUT_CONTENT_BYTES - fixed) / 6;
    add(
        &fixture,
        "child",
        "a-good",
        RuleFilter::True,
        Some(&format!("SET added='{value}'")),
        2,
    )?;
    add(
        &fixture,
        "child",
        "b-fail",
        RuleFilter::True,
        Some("SET number='bad'"),
        2,
    )?;
    bytes.body = vec![7; body_bytes + 1];
    bytes.envelope.body = MessageBody::Data(vec![bytes.body.clone()]);
    let committed = observations.lock().expect("observations").commits;
    assert!(matches!(
        refuses(&fixture, 10, rich(bytes.clone()))?,
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::ContentBytes,
            maximum: MAX_TOPIC_FANOUT_CONTENT_BYTES
        }
    ));
    assert_eq!(
        observations.lock().expect("observations").commits,
        committed
    );
    bytes.body.pop();
    bytes.envelope.body = MessageBody::Data(vec![bytes.body.clone()]);
    assert_eq!(fixed + 6 * bytes.body.len(), MAX_TOPIC_FANOUT_CONTENT_BYTES);
    publish(&fixture, 3, vec![bytes])?;

    remove(&fixture, "child", "a-good", 4)?;
    let mut nodes = member("nodes");
    nodes.envelope.application_properties = BTreeMap::from([
        ("number".into(), MessageValue::Int(7)),
        ("RuleName".into(), MessageValue::String("publisher".into())),
    ]);
    let exact_nodes = (MAX_TOPIC_FANOUT_VALUE_ITEMS - 6) / 2;
    nodes.envelope.body = MessageBody::Sequence(vec![vec![MessageValue::Null; exact_nodes + 1]]);
    assert!(matches!(
        refuses(&fixture, 10, rich(nodes.clone()))?,
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::ValueItems,
            maximum: MAX_TOPIC_FANOUT_VALUE_ITEMS
        }
    ));
    nodes.envelope.body = MessageBody::Sequence(vec![vec![MessageValue::Null; exact_nodes]]);
    assert_eq!(2 * (exact_nodes + 2) + 2, MAX_TOPIC_FANOUT_VALUE_ITEMS);
    publish(&fixture, 5, vec![nodes])?;

    remove(&fixture, "child", "b-fail", 6)?;
    add(
        &fixture,
        "child",
        "growth",
        RuleFilter::True,
        Some("SET added='growth'"),
        6,
    )?;
    assert!(matches!(
        refuses(&fixture, 10, rich(full_header("header")))?,
        BrokerError::MessageHeaderTooLarge { .. }
    ));
    let mut property = member("property");
    property.envelope.application_properties.insert(
        "too-large".into(),
        MessageValue::String("x".repeat(domain::MAX_MESSAGE_PROPERTY_BYTES)),
    );
    assert!(matches!(
        refuses(&fixture, 10, rich(property))?,
        BrokerError::MessagePropertyTooLarge { .. }
    ));
    let mut shape = member("shape");
    shape
        .envelope
        .application_properties
        .insert("number".into(), MessageValue::List(vec![]));
    // The conversion failure must not launder an invalid original application value.
    add(
        &fixture,
        "child",
        "failure",
        RuleFilter::True,
        Some("SET number=1"),
        6,
    )?;
    assert!(matches!(
        refuses(&fixture, 10, rich(shape))?,
        BrokerError::InvalidMessageContent { .. }
    ));

    for name in ["growth", "failure", "$Default"] {
        remove(&fixture, "child", name, 6)?;
    }
    add(
        &fixture,
        "child",
        "intermediate",
        RuleFilter::True,
        Some("SET added=TRUE;REMOVE added"),
        6,
    )?;
    let mut full = member("local-nodes");
    full.envelope.application_properties = BTreeMap::from([
        ("number".into(), MessageValue::Int(7)),
        ("RuleName".into(), MessageValue::String("publisher".into())),
    ]);
    full.envelope.body = MessageBody::Sequence(vec![vec![
        MessageValue::Null;
        domain::MAX_MESSAGE_VALUE_ITEMS - 2
    ]]);
    assert!(matches!(
        refuses(&fixture, 10, rich(full.clone()))?,
        BrokerError::InvalidMessageContent { .. }
    ));
    remove(&fixture, "child", "intermediate", 6)?;
    add(
        &fixture,
        "child",
        "exact-nodes",
        RuleFilter::True,
        Some("SET number=9;SET RuleName='ignored'"),
        6,
    )?;
    publish(&fixture, 7, vec![full])?;
    remove(&fixture, "child", "exact-nodes", 8)?;
    let small = subscribe_at(
        &fixture,
        "small",
        SubscriptionConfig {
            max_message_bytes: 1024,
            ..SubscriptionConfig::default()
        },
        8,
    )?;
    remove(&fixture, "small", "$Default", 8)?;
    add(
        &fixture,
        "small",
        "large-set",
        RuleFilter::True,
        Some(&format!("SET added='{}'", "x".repeat(900))),
        8,
    )?;
    assert!(matches!(
        refuses(&fixture, 10, rich(member("message")))?,
        BrokerError::MessageTooLarge {
            maximum_bytes: 1024,
            ..
        }
    ));
    assert!(records(&fixture, &small)?.is_empty());
    Ok(())
}

fn literal_set_work_and_configuration_limits_are_shared<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut fixture = topic(provider, TopicConfig::default())?;
    subscribe(&fixture, "child", SubscriptionConfig::default())?;
    remove(&fixture, "child", "$Default", 0)?;
    let source = format!("{}SET[a]=1", "REMOVE[a];".repeat(31));
    for index in 0..32 {
        add(
            &fixture,
            "child",
            &format!("rule-{index:02}"),
            RuleFilter::False,
            Some(&source),
            0,
        )?;
    }
    let mut work = member("work");
    work.envelope.application_properties = (0..2000)
        .map(|index| (format!("k{index:04}"), MessageValue::Null))
        .collect();
    assert!(matches!(
        refuses(&fixture, 1, rich(work))?,
        BrokerError::TopicRuleMatchTooLarge {
            limit: domain::RuleMatchLimit::WorkUnits,
            ..
        }
    ));
    let mut bytes = member("comparison");
    bytes.envelope.application_properties = (0..32)
        .map(|index| {
            (
                format!("k{index:02}"),
                MessageValue::String("x".repeat(1900)),
            )
        })
        .collect();
    assert!(matches!(
        refuses(&fixture, 1, rich(bytes))?,
        BrokerError::TopicRuleMatchTooLarge {
            limit: domain::RuleMatchLimit::ComparisonBytes,
            ..
        }
    ));
    assert!(matches!(
        refuses(
            &fixture,
            1,
            CommandKind::CreateRuleWithAction {
                subscription: SubscriptionName::new("child")?,
                name: RuleName::new("overflow")?,
                filter: RuleFilter::False,
                action: SqlAction::new("SET a=1")?
            }
        )?,
        BrokerError::RuleLimitExceeded { maximum: 32 }
    ));

    let original_topic = fixture.entity.clone();
    fixture.entity = EntityPath::new("visits")?;
    fixture.at(
        0,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default())?;
    remove(&fixture, "child", "$Default", 0)?;
    let mut full = member("bounded-visits");
    full.envelope.body = MessageBody::Sequence(vec![vec![
        MessageValue::Null;
        domain::MAX_MESSAGE_VALUE_ITEMS - 2
    ]]);
    // False filters isolate possible-action planning charges from copy limits.
    // N=65536, A=P=2, H=D=0, S=m=1. Shared profiling costs N+A+P+S;
    // Each definition plus program costs 2N+6A+2S+3P+16+4*m*m+m+3P+3m+1.
    let profiling_work = domain::MAX_MESSAGE_VALUE_ITEMS + 5;
    let per_program_work = 2 * domain::MAX_MESSAGE_VALUE_ITEMS + 51;
    assert!(profiling_work + 7 * per_program_work <= domain::MAX_TOPIC_RULE_MATCH_WORK);
    assert!(profiling_work + 8 * per_program_work > domain::MAX_TOPIC_RULE_MATCH_WORK);
    for index in 0..8 {
        add(
            &fixture,
            "child",
            &format!("bounded-{index}"),
            RuleFilter::False,
            Some("SET number=9"),
            0,
        )?;
        if [0, 1, 6].contains(&index) {
            let application = publish(&fixture, 0, vec![full.clone()])?;
            effects(&application, &[]);
            assert!(records(&fixture, &child)?.is_empty());
        }
    }
    assert!(matches!(
        refuses(&fixture, 1, rich(full))?,
        BrokerError::TopicRuleMatchTooLarge {
            limit: domain::RuleMatchLimit::WorkUnits,
            maximum: domain::MAX_TOPIC_RULE_MATCH_WORK
        }
    ));
    fixture.entity = original_topic;
    for child in 0..12 {
        let name = format!("more-{child:02}");
        subscribe(&fixture, &name, SubscriptionConfig::default())?;
        remove(&fixture, &name, "$Default", 0)?;
        for index in 0..32 {
            add(
                &fixture,
                &name,
                &format!("rule-{index:02}"),
                RuleFilter::False,
                Some(&source),
                0,
            )?;
        }
    }
    assert!(matches!(
        refuses(&fixture, 1, rich(member("compile")))?,
        BrokerError::SqlActionCompilation(SqlCompileError::Limit {
            kind: domain::SqlCompileLimit::AggregateTokens,
            maximum: domain::MAX_SQL_COMPILE_TOKENS
        })
    ));
    Ok(())
}

fn literal_set_batch_duplicates_and_action_sequences_remain_deterministic<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default())?;
    add(
        &fixture,
        "child",
        "a-good",
        RuleFilter::True,
        Some("SET number=8"),
        0,
    )?;
    add(
        &fixture,
        "child",
        "b-fail",
        RuleFilter::True,
        Some("SET number='bad'"),
        0,
    )?;
    let publication = publish(
        &fixture,
        1,
        vec![member("one"), member("one"), member("two")],
    )?;
    assert_eq!(
        publication.outcome,
        CommandOutcome::BatchSent {
            sequences: (1..=3).map(SequenceNumber::new).collect()
        }
    );
    let active = records(&fixture, &child)?;
    let dead = records(&fixture, &child.dead_letter_queue()?)?;
    assert_eq!(
        active
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        [1, 3, 4, 6].map(SequenceNumber::new)
    );
    assert_eq!(
        dead.iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        [5, 7].map(SequenceNumber::new)
    );
    assert_eq!(active[0].message_id, "one");
    assert_eq!(active[1].message_id, "two");
    assert_eq!(active[2].message_id, "one");
    assert_eq!(active[3].message_id, "two");
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("all bases first, including duplicate input")
            .next_sequence,
        8
    );
    let before = fixture.machine.store().snapshot()?;
    let application = publish(&fixture, 2, vec![member("one"), member("two")])?;
    effects(&application, &[]);
    assert_eq!(
        application.outcome,
        CommandOutcome::BatchSent {
            sequences: [8, 9].map(SequenceNumber::new).to_vec()
        }
    );
    assert_eq!(records(&fixture, &child)?, active);
    assert_eq!(records(&fixture, &child.dead_letter_queue()?)?, dead);
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("duplicate inputs allocate bases, never action copies")
            .next_sequence,
        10
    );
    assert_ne!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn literal_set_one_commit_failure_and_reopen_preserve_complete_records<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fail_next = Arc::new(AtomicBool::new(false));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut fixture = topic(
        ObservedProvider {
            inner: provider,
            fail_next: fail_next.clone(),
            observations: observations.clone(),
        },
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default())?;
    apply(
        &fixture,
        0,
        CommandKind::CreateRuleWithAction {
            subscription: SubscriptionName::new("child")?,
            name: RuleName::new("a-v1")?,
            filter: RuleFilter::True,
            action: SqlAction::with_semantic_version(" /* v1 */ REMOVE color; ", 1)?,
        },
    )?;
    add(
        &fixture,
        "child",
        "b-v2",
        RuleFilter::True,
        Some(" /* v2 */ SET number=11; "),
        0,
    )?;
    add(
        &fixture,
        "child",
        "c-error",
        RuleFilter::True,
        Some("REMOVE color;SET number='bad'"),
        0,
    )?;
    let expected = rules(&fixture, "child")?;
    assert_eq!(
        expected[1].action.as_ref().expect("v1").semantic_version(),
        1
    );
    assert_eq!(
        expected[2].action.as_ref().expect("v2").semantic_version(),
        2
    );
    let before = fixture.machine.store().snapshot()?;
    observations.lock().expect("observations").commits = 0;
    fail_next.store(true, Ordering::SeqCst);
    assert!(matches!(
        refuses(
            &fixture,
            1,
            CommandKind::SendBatch {
                messages: vec![member("one"), member("two")]
            }
        )?,
        BrokerError::Storage(StorageError::Backend { .. })
    ));
    assert_eq!(observations.lock().expect("observations").commits, 1);
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(rules(&fixture, "child")?, expected);
    observations.lock().expect("observations").commits = 0;
    publish(&fixture, 1, vec![member("one"), member("two")])?;
    assert_eq!(observations.lock().expect("observations").commits, 1);
    let active = records(&fixture, &child)?;
    let dead = records(&fixture, &child.dead_letter_queue()?)?;
    assert_eq!(active.len(), 6);
    assert_eq!(dead.len(), 2);
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("all copies")
            .next_sequence,
        9
    );
    let committed = fixture.machine.store().snapshot()?;
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, committed);
    assert_eq!(records(&fixture, &child)?, active);
    assert_eq!(records(&fixture, &child.dead_letter_queue()?)?, dead);
    assert_eq!(rules(&fixture, "child")?, expected);
    Ok(())
}

for_each_backend! {
    literal_set_copies_use_original_filters_and_isolated_overrides,
    literal_set_legacy_bodies_and_final_rule_name_are_authoritative,
    literal_set_failure_dead_letters_one_original_action_copy,
    literal_set_filter_errors_and_missing_sessions_keep_defined_priority,
    literal_set_caps_refuse_whole_publications_before_state_changes,
    literal_set_work_and_configuration_limits_are_shared,
    literal_set_batch_duplicates_and_action_sequences_remain_deterministic,
    literal_set_current_actions_are_evaluated_at_scheduled_activation,
    literal_set_unfit_activation_head_remains_pending_and_cancelable,
    literal_set_one_commit_failure_and_reopen_preserve_complete_records,
}
