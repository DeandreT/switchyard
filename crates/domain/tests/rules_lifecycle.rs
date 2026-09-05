//! Durable subscription-rule management and filtered topic fanout on every
//! storage backend.

use std::{collections::BTreeMap, error::Error};

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, CorrelationFilter, CorrelationValue,
    DEFAULT_RULE_NAME, Delivery, EntityPath, FilterProperties, MAX_RULE_PAGE, MessageEnvelope,
    MessageInput, NamespaceName, ReceiveMode, RuleDefinition, RuleFilter, RuleName, SequenceNumber,
    StateMachine, SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig,
};
use testkit::StoreProvider;

struct RuleFixture<P: StoreProvider> {
    namespace: NamespaceName,
    topic: EntityPath,
    machine: StateMachine<P::Store>,
    provider: P,
}

impl<P: StoreProvider> RuleFixture<P> {
    fn new(provider: P) -> Result<Self, Box<dyn Error>> {
        let fixture = Self {
            namespace: NamespaceName::new("tenant")?,
            topic: EntityPath::new("events")?,
            machine: StateMachine::new(provider.open()?),
            provider,
        };
        assert_eq!(
            fixture.at_topic(
                0,
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
            )?,
            CommandOutcome::TopicCreated
        );
        Ok(fixture)
    }

    fn at_topic(&self, millis: u64, kind: CommandKind) -> Result<CommandOutcome, BrokerError> {
        self.at_entity(&self.topic, millis, kind)
    }

    fn at_entity(
        &self,
        entity: &EntityPath,
        millis: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&Command::new(
            self.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(millis),
            kind,
        ))
    }

    fn subscribe(&self, millis: u64, name: &str) -> Result<EntityPath, Box<dyn Error>> {
        let name = SubscriptionName::new(name)?;
        match self.at_topic(
            millis,
            CommandKind::CreateSubscription {
                name,
                config: SubscriptionConfig::default(),
            },
        )? {
            CommandOutcome::SubscriptionCreated { entity } => Ok(entity),
            other => panic!("expected subscription creation, got {other:?}"),
        }
    }

    fn create_rule(
        &self,
        subscription: &EntityPath,
        millis: u64,
        name: &str,
        filter: RuleFilter,
    ) -> Result<CommandOutcome, Box<dyn Error>> {
        Ok(self.at_entity(
            subscription,
            millis,
            CommandKind::CreateRule {
                name: RuleName::new(name)?,
                filter,
            },
        )?)
    }

    fn delete_rule(
        &self,
        subscription: &EntityPath,
        millis: u64,
        name: &str,
    ) -> Result<CommandOutcome, Box<dyn Error>> {
        Ok(self.at_entity(
            subscription,
            millis,
            CommandKind::DeleteRule {
                name: RuleName::new(name)?,
            },
        )?)
    }

    fn list_rules(
        &self,
        subscription: &EntityPath,
        millis: u64,
        skip: u32,
        max_rules: u32,
    ) -> Result<Vec<RuleDefinition>, BrokerError> {
        match self.at_entity(
            subscription,
            millis,
            CommandKind::ListRules { skip, max_rules },
        )? {
            CommandOutcome::RulesListed { rules } => Ok(rules),
            other => panic!("expected a rule listing, got {other:?}"),
        }
    }

    fn publish(
        &self,
        millis: u64,
        input: MessageInput,
    ) -> Result<(Vec<SequenceNumber>, Vec<EntityPath>), BrokerError> {
        match self.at_topic(
            millis,
            CommandKind::Send {
                message_id: input.message_id,
                body: input.body,
                time_to_live_millis: input.time_to_live_millis,
                session_id: input.session_id,
                scheduled_enqueue_at: input.scheduled_enqueue_at,
                envelope: input.envelope,
            },
        )? {
            CommandOutcome::Published {
                sequences,
                subscriptions,
            } => Ok((sequences, subscriptions)),
            other => panic!("expected a topic publication, got {other:?}"),
        }
    }

    fn receive(
        &self,
        subscription: &EntityPath,
        millis: u64,
    ) -> Result<Option<Delivery>, BrokerError> {
        match self.at_entity(
            subscription,
            millis,
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        )? {
            CommandOutcome::Received(delivery) => Ok(delivery),
            other => panic!("expected a receive outcome, got {other:?}"),
        }
    }

    fn restart(self) -> Result<Self, Box<dyn Error>> {
        let Self {
            namespace,
            topic,
            machine,
            provider,
        } = self;
        drop(machine);
        Ok(Self {
            namespace,
            topic,
            machine: StateMachine::new(provider.open()?),
            provider,
        })
    }
}

fn scalar(tag: u8, value: &str) -> Result<CorrelationValue, Box<dyn Error>> {
    let mut canonical = vec![tag];
    canonical.extend_from_slice(value.as_bytes());
    Ok(CorrelationValue::new(canonical)?)
}

fn default_rules_are_atomic_durable_and_case_insensitive<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = RuleFixture::new(provider)?;
    let subscription = fixture.subscribe(10, "Accounting")?;

    assert_eq!(
        fixture.list_rules(&subscription, 11, 0, 10)?,
        vec![RuleDefinition {
            name: RuleName::new(DEFAULT_RULE_NAME)?,
            filter: RuleFilter::True,
            created_at: Timestamp::from_millis(10),
        }]
    );
    assert_eq!(
        fixture.create_rule(&subscription, 12, "West", RuleFilter::False)?,
        CommandOutcome::RuleCreated
    );
    assert_eq!(
        fixture.create_rule(&subscription, 13, "ALPHA", RuleFilter::True)?,
        CommandOutcome::RuleCreated
    );

    let page = fixture.list_rules(&subscription, 14, 1, 1)?;
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].name, RuleName::new("alpha")?);
    assert_eq!(page[0].name.display_name(), "ALPHA");
    assert_eq!(page[0].created_at, Timestamp::from_millis(13));
    assert_eq!(
        fixture.at_entity(
            &subscription,
            15,
            CommandKind::CreateRule {
                name: RuleName::new("wEST")?,
                filter: RuleFilter::True,
            },
        ),
        Err(BrokerError::RuleAlreadyExists {
            name: RuleName::new("west")?,
        })
    );

    let fixture = fixture.restart()?;
    let recovered = fixture.list_rules(&subscription, 16, 0, 10)?;
    assert_eq!(
        recovered
            .iter()
            .map(|rule| rule.name.as_str())
            .collect::<Vec<_>>(),
        vec!["$default", "alpha", "west"]
    );
    assert_eq!(
        recovered
            .iter()
            .map(|rule| rule.name.display_name())
            .collect::<Vec<_>>(),
        vec!["$default", "ALPHA", "West"]
    );
    assert_eq!(
        fixture.delete_rule(&subscription, 17, "WEST")?,
        CommandOutcome::RuleDeleted
    );
    assert_eq!(
        fixture.at_entity(
            &subscription,
            18,
            CommandKind::DeleteRule {
                name: RuleName::new("west")?,
            },
        ),
        Err(BrokerError::RuleNotFound {
            name: RuleName::new("west")?,
        })
    );
    assert_eq!(
        fixture.at_entity(
            &subscription,
            19,
            CommandKind::ListRules {
                skip: 0,
                max_rules: 0,
            },
        ),
        Err(BrokerError::EmptyRulePage)
    );
    assert_eq!(
        fixture.at_entity(
            &subscription,
            20,
            CommandKind::ListRules {
                skip: 0,
                max_rules: MAX_RULE_PAGE + 1,
            },
        ),
        Err(BrokerError::RulePageTooLarge {
            requested: MAX_RULE_PAGE + 1,
            maximum: MAX_RULE_PAGE,
        })
    );
    Ok(())
}

fn matching_rules_filter_fanout_once_per_subscription_after_restart<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = RuleFixture::new(provider)?;
    let west = fixture.subscribe(1, "west")?;
    let audit = fixture.subscribe(2, "audit")?;
    assert_eq!(
        fixture.delete_rule(&west, 3, DEFAULT_RULE_NAME)?,
        CommandOutcome::RuleDeleted
    );
    assert_eq!(
        fixture.delete_rule(&audit, 4, DEFAULT_RULE_NAME)?,
        CommandOutcome::RuleDeleted
    );

    let application_properties = BTreeMap::from([
        (String::from("region"), scalar(1, "West")?),
        (String::from("attempt"), scalar(2, "7")?),
    ]);
    let correlation = CorrelationFilter {
        correlation_id: Some(String::from("order-7")),
        message_id: Some(String::from("message-7")),
        to: Some(String::from("orders")),
        reply_to: Some(String::from("replies")),
        subject: Some(String::from("created")),
        session_id: None,
        reply_to_session_id: Some(String::from("reply-session-7")),
        content_type: Some(String::from("application/json")),
        application_properties: application_properties.clone(),
    };
    for (millis, name) in [(5, "west-primary"), (6, "west-secondary")] {
        assert_eq!(
            fixture.create_rule(
                &west,
                millis,
                name,
                RuleFilter::Correlation(correlation.clone()),
            )?,
            CommandOutcome::RuleCreated
        );
    }
    assert_eq!(
        fixture.create_rule(&audit, 7, "never", RuleFilter::False)?,
        CommandOutcome::RuleCreated
    );

    let fixture = fixture.restart()?;
    let matching_projection = FilterProperties {
        correlation_id: Some(String::from("order-7")),
        message_id: Some(String::from("envelope-id-is-overlaid")),
        to: Some(String::from("orders")),
        reply_to: Some(String::from("replies")),
        subject: Some(String::from("created")),
        session_id: None,
        reply_to_session_id: Some(String::from("reply-session-7")),
        content_type: Some(String::from("application/json")),
        application_properties: application_properties.clone(),
    };
    let matching_input = MessageInput {
        message_id: String::from("message-7"),
        body: b"matching".to_vec(),
        envelope: Some(
            MessageEnvelope::new(b"opaque-amqp".to_vec())
                .with_filter_properties(matching_projection.clone()),
        ),
        ..MessageInput::default()
    };
    assert_eq!(
        fixture.publish(8, matching_input.clone())?,
        (vec![SequenceNumber::new(1)], vec![west.clone()])
    );
    assert_eq!(
        fixture
            .receive(&west, 9)?
            .expect("two matching rules still create one subscription copy")
            .body,
        b"matching".to_vec()
    );
    assert_eq!(fixture.receive(&west, 10)?, None);
    assert_eq!(fixture.receive(&audit, 11)?, None);

    let mut wrong_type = matching_projection;
    wrong_type
        .application_properties
        .insert(String::from("attempt"), scalar(3, "7")?);
    let nonmatching_input = MessageInput {
        envelope: Some(
            MessageEnvelope::new(b"opaque-amqp".to_vec()).with_filter_properties(wrong_type),
        ),
        ..matching_input.clone()
    };
    assert_eq!(
        fixture.publish(12, nonmatching_input)?,
        (vec![SequenceNumber::new(2)], Vec::new())
    );

    assert_eq!(
        fixture.delete_rule(&west, 13, "WEST-PRIMARY")?,
        CommandOutcome::RuleDeleted
    );
    assert_eq!(
        fixture.delete_rule(&west, 14, "west-secondary")?,
        CommandOutcome::RuleDeleted
    );
    assert_eq!(
        fixture.publish(15, matching_input)?,
        (vec![SequenceNumber::new(3)], Vec::new()),
        "a subscription with no rules receives no copy"
    );
    assert_eq!(fixture.receive(&west, 16)?, None);
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory {
            $(
                #[test]
                fn $case() -> Result<(), Box<dyn std::error::Error>> {
                    super::$case(::testkit::MemoryProvider::new())
                }
            )+
        }

        mod durable {
            $(
                #[test]
                fn $case() -> Result<(), Box<dyn std::error::Error>> {
                    super::$case(::testkit::DurableProvider::temporary()?)
                }
            )+
        }
    };
}

for_each_backend! {
    default_rules_are_atomic_durable_and_case_insensitive,
    matching_rules_filter_fanout_once_per_subscription_after_restart,
}
