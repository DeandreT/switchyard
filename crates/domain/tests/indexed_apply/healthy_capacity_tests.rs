//! Selected-reader provenance, not global catalog health or recovery.
//! Raw catalogs/faults are serialized setup; Fjall reopen drops all handles.
//! Returned apply errors below are logical atomic boundaries, not power cuts.

use super::*;
use domain::{DurableProposalAuthority, MessageInput};

#[derive(Clone, Copy, Debug)]
enum Catalog {
    Rules,
    Subscriptions,
}

impl Catalog {
    fn error(self) -> BrokerError {
        match self {
            Self::Rules => BrokerError::RuleLimitExceeded {
                maximum: MAX_SUBSCRIPTION_RULES,
            },
            Self::Subscriptions => BrokerError::SubscriptionLimitExceeded {
                maximum: MAX_TOPIC_SUBSCRIPTIONS,
            },
        }
    }

    fn create(self, at: u64) -> DurableProposal {
        match self {
            Self::Rules => proposal(
                &subscription(),
                at,
                CommandKind::CreateRule {
                    name: RuleName::new("new-rule").unwrap(),
                    filter: RuleFilter::True,
                },
            ),
            Self::Subscriptions => proposal(
                &topic(),
                at,
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new("new-child").unwrap(),
                    config: SubscriptionConfig::default(),
                },
            ),
        }
    }

    fn prefix(self) -> Key {
        match self {
            Self::Rules => keys::subscription_rule_prefix(&namespace(), &subscription()),
            Self::Subscriptions => keys::topic_subscription_prefix(&namespace(), &topic()),
        }
    }
}

fn seed_full<P: StoreProvider>(
    fixture: &Fixture<P>,
    writer: &mut IndexedWriter<Observed<P::Store>>,
    catalog: Catalog,
) -> TestResult {
    writer.apply(
        1,
        &proposal(
            &topic(),
            10,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
    )?;
    writer.apply(
        2,
        &proposal(
            &topic(),
            20,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("worker")?,
                config: SubscriptionConfig::default(),
            },
        ),
    )?;
    let mut batch = WriteBatch::default();
    match catalog {
        Catalog::Rules => {
            for offset in 1..MAX_SUBSCRIPTION_RULES {
                let name = RuleName::new(format!("rule-{offset:04}"))?;
                batch.push_put(
                    keys::subscription_rule(&namespace(), &subscription(), &name),
                    codec::encode(&RuleDefinition {
                        name,
                        filter: RuleFilter::True,
                        created_at: Timestamp::from_millis(20),
                    })?,
                );
            }
        }
        Catalog::Subscriptions => {
            let store = &fixture.store().inner;
            let backing = store
                .get(&keys::queue_config(&namespace(), &subscription()))?
                .unwrap();
            let shadow = store
                .get(&keys::queue_config(
                    &namespace(),
                    &subscription().dead_letter_queue()?,
                ))?
                .unwrap();
            let head = store
                .get(&keys::entity_metadata(&namespace(), &subscription()))?
                .unwrap();
            let default = RuleName::new(domain::DEFAULT_RULE_NAME)?;
            let rule = store
                .get(&keys::subscription_rule(
                    &namespace(),
                    &subscription(),
                    &default,
                ))?
                .unwrap();
            // Mirror actual-created profiles/owner/default rule without thousands
            // of separate fsyncs. The bounded production reader proves these rows.
            for offset in 1..MAX_TOPIC_SUBSCRIPTIONS {
                let name = SubscriptionName::new(format!("child-{offset:04}"))?;
                let entity = topic().subscription(&name)?;
                batch.push_put(
                    keys::topic_subscription(&namespace(), &topic(), &name),
                    codec::encode(&entity)?,
                );
                batch.push_put(keys::queue_config(&namespace(), &entity), backing.clone());
                batch.push_put(
                    keys::queue_config(&namespace(), &entity.dead_letter_queue()?),
                    shadow.clone(),
                );
                batch.push_put(keys::entity_metadata(&namespace(), &entity), head.clone());
                batch.push_put(
                    keys::subscription_rule(&namespace(), &entity, &default),
                    rule.clone(),
                );
            }
        }
    }
    fixture.raw(batch);
    Ok(())
}

fn with_authority<P: StoreProvider>(
    fixture: &Fixture<P>,
    original: DurableProposal,
    bound: bool,
) -> Result<DurableProposal, Box<dyn Error>> {
    if bound {
        let binding = fixture
            .machine()
            .bind_entity(&namespace(), &original.command().entity)?;
        Ok(DurableProposal::bound(BoundCommand::new(
            binding,
            original.command().clone(),
        ))?)
    } else {
        Ok(original)
    }
}

fn ordinary<P: StoreProvider>(
    fixture: &Fixture<P>,
    original: &DurableProposal,
) -> Result<CommandOutcome, BrokerError> {
    match original.authority() {
        DurableProposalAuthority::Unbound => fixture.machine().apply(original.command()),
        DurableProposalAuthority::Bound(binding) => fixture.machine().apply_bound(
            &BoundCommand::new(binding.clone(), original.command().clone()),
        ),
    }
}

fn checkpoint_only<P: StoreProvider>(
    fixture: &Fixture<P>,
    writer: &mut IndexedWriter<Observed<P::Store>>,
    index: u64,
    original: &DurableProposal,
    error: BrokerError,
) {
    let before = fixture.snapshot();
    fixture.reset();
    assert_eq!(ordinary(fixture, original), Err(error.clone()));
    let prepared = fixture.trace();
    assert!(prepared.batches.is_empty());
    assert_eq!(fixture.snapshot(), before);
    fixture.reset();
    assert_eq!(
        writer.apply(index, original),
        Ok(IndexedApplyOutcome::Refused(error))
    );
    let indexed = fixture.trace();
    assert_eq!(indexed.gets, prepared.gets, "provenance must not reread");
    assert_eq!(indexed.scans, prepared.scans, "provenance must not rescan");
    assert_eq!(indexed.snapshots, prepared.snapshots);
    let expected = WriteBatch::default().put(key(1), checkpoint(index, proposal_hash(original)));
    assert_eq!(indexed.batches, vec![expected.clone()]);
    assert!(effects(&indexed.batches[0]).is_empty());
    assert_eq!(fixture.snapshot(), apply_rows(&before, &expected));
    assert_eq!(domain_rows(&fixture.snapshot()), domain_rows(&before));
    assert_eq!(writer.applied_index().unwrap(), index);
}

fn denied<P: StoreProvider>(
    fixture: &Fixture<P>,
    writer: &mut IndexedWriter<Observed<P::Store>>,
    index: u64,
    original: &DurableProposal,
    error: BrokerError,
) {
    let before = fixture.snapshot();
    fixture.reset();
    assert_eq!(ordinary(fixture, original), Err(error.clone()));
    let prepared = fixture.trace();
    assert!(prepared.batches.is_empty());
    assert_eq!(fixture.snapshot(), before);
    fixture.reset();
    assert_eq!(
        writer.apply(index, original),
        Err(IndexedApplyError::Domain(error))
    );
    let indexed = fixture.trace();
    assert_eq!(indexed.gets, prepared.gets);
    assert_eq!(indexed.scans, prepared.scans);
    assert_eq!(indexed.snapshots, prepared.snapshots);
    assert!(indexed.batches.is_empty());
    assert_eq!(writer.applied_index().unwrap(), index - 1);
    assert_eq!(fixture.snapshot(), before);
}

fn no_io(trace: &Trace) {
    assert!(trace.gets.is_empty() && trace.scans.is_empty() && trace.batches.is_empty());
    assert_eq!(trace.snapshots, 0);
}

fn replace_row<P: StoreProvider>(
    fixture: &Fixture<P>,
    fault: &mut WriteBatch,
    undo: &mut WriteBatch,
    row: Key,
    value: Value,
) -> Result<(), Box<dyn Error>> {
    match fixture.store().inner.get(&row)? {
        Some(original) => undo.push_put(row.clone(), original),
        None => undo.push_delete(row.clone()),
    }
    fault.push_put(row, value);
    Ok(())
}

fn corrupt_catalog<P: StoreProvider>(
    fixture: &Fixture<P>,
    catalog: Catalog,
    case: usize,
) -> Result<(BrokerError, WriteBatch), Box<dyn Error>> {
    let mut fault = WriteBatch::default();
    let mut undo = WriteBatch::default();
    let codec_error = BrokerError::Codec(domain::CodecError::UnsupportedVersion { version: 255 });
    let expected = match catalog {
        Catalog::Rules => {
            let name = RuleName::new("rule-0001")?;
            let row = keys::subscription_rule(&namespace(), &subscription(), &name);
            if case < 3 {
                let mut definition: RuleDefinition =
                    codec::decode(&fixture.store().inner.get(&row)?.unwrap())?;
                let value = match case {
                    0 => {
                        definition.name = RuleName::new("other-name")?;
                        codec::encode(&definition)?
                    }
                    1 => {
                        definition.filter = RuleFilter::Correlation(CorrelationFilter::default());
                        codec::encode(&definition)?
                    }
                    _ => vec![255],
                };
                replace_row(fixture, &mut fault, &mut undo, row, value)?;
                if case == 2 {
                    codec_error.clone()
                } else {
                    BrokerError::EntityMetadataCorrupt
                }
            } else {
                let name = RuleName::new("zz-overfull")?;
                replace_row(
                    fixture,
                    &mut fault,
                    &mut undo,
                    keys::subscription_rule(&namespace(), &subscription(), &name),
                    codec::encode(&RuleDefinition {
                        name,
                        filter: RuleFilter::Correlation(CorrelationFilter::default()),
                        created_at: Timestamp::from_millis(20),
                    })?,
                )?;
                if case == 4 {
                    // The full ordered decode still precedes the overfull guard.
                    replace_row(fixture, &mut fault, &mut undo, row, vec![255])?;
                    codec_error.clone()
                } else {
                    catalog.error()
                }
            }
        }
        Catalog::Subscriptions => {
            if case >= 3 {
                let name = SubscriptionName::new("zz-overfull")?;
                replace_row(
                    fixture,
                    &mut fault,
                    &mut undo,
                    keys::topic_subscription(&namespace(), &topic(), &name),
                    vec![255],
                )?;
            }
            match case {
                0 => {
                    replace_row(
                        fixture,
                        &mut fault,
                        &mut undo,
                        keys::topic_subscription(
                            &namespace(),
                            &topic(),
                            &SubscriptionName::new("worker")?,
                        ),
                        vec![255],
                    )?;
                    BrokerError::TopicTopologyCorrupt
                }
                1 => {
                    replace_row(
                        fixture,
                        &mut fault,
                        &mut undo,
                        keys::queue_config(&namespace(), &subscription()),
                        codec::encode(&QueueConfig {
                            lock_duration_millis: 0,
                            ..QueueConfig::default()
                        })?,
                    )?;
                    BrokerError::TopicTopologyCorrupt
                }
                2 | 4 => {
                    replace_row(
                        fixture,
                        &mut fault,
                        &mut undo,
                        keys::entity_metadata(&namespace(), &subscription()),
                        vec![255],
                    )?;
                    if case == 4 {
                        catalog.error()
                    } else {
                        BrokerError::EntityMetadataCorrupt
                    }
                }
                _ => catalog.error(),
            }
        }
    };
    fixture.raw(fault);
    Ok((expected, undo))
}

fn healthy_exact_capacity<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    for catalog in [Catalog::Rules, Catalog::Subscriptions] {
        fixture.clear();
        let mut writer = fixture.writer();
        seed_full(&fixture, &mut writer, catalog)?;
        for (offset, bound) in [false, true].into_iter().enumerate() {
            let original = with_authority(&fixture, catalog.create(30 + offset as u64), bound)?;
            checkpoint_only(
                &fixture,
                &mut writer,
                3 + offset as u64,
                &original,
                catalog.error(),
            );
            assert_eq!(
                fixture.machine().last_applied_time()?,
                Timestamp::from_millis(20)
            );
        }
        let before = fixture.snapshot();
        drop(writer);
        fixture.reopen();
        assert_eq!(fixture.writer().applied_index()?, 4);
        assert_eq!(fixture.snapshot(), before);
    }
    Ok(())
}

fn malformed_and_overfull_origins<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    for catalog in [Catalog::Rules, Catalog::Subscriptions] {
        for case in 0..5 {
            fixture.clear();
            let mut writer = fixture.writer();
            seed_full(&fixture, &mut writer, catalog)?;
            let unbound = catalog.create(30);
            let bound = with_authority(&fixture, unbound.clone(), true)?;
            let (error, undo) = corrupt_catalog(&fixture, catalog, case)?;
            for original in [&unbound, &bound] {
                denied(&fixture, &mut writer, 3, original, error.clone());
            }
            assert!(fixture.trace().scans.contains(&catalog.prefix()));
            let corrupt = fixture.snapshot();
            drop(writer);
            fixture.reopen();
            assert_eq!(fixture.snapshot(), corrupt);
            let mut writer = fixture.writer();
            assert_eq!(writer.applied_index()?, 2);
            denied(&fixture, &mut writer, 3, &unbound, error);
            // Controlled fixture restoration is not an integrity-repair API.
            fixture.raw(undo);
            checkpoint_only(&fixture, &mut writer, 3, &unbound, catalog.error());
            drop(writer);
        }
    }
    Ok(())
}

// Private owner record shape is mirrored only for a controlled generation fault.
#[derive(Serialize)]
struct SelectedHead {
    generation: u64,
    kind: u8,
    retired: bool,
}

fn earlier_priorities<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    for catalog in [Catalog::Rules, Catalog::Subscriptions] {
        fixture.clear();
        let mut writer = fixture.writer();
        seed_full(&fixture, &mut writer, catalog)?;
        let original = catalog.create(30);
        let bound = with_authority(&fixture, original.clone(), true)?;
        let mut index = 3;
        let collision = match catalog {
            Catalog::Rules => proposal(
                &subscription(),
                30,
                CommandKind::CreateRule {
                    name: RuleName::new(domain::DEFAULT_RULE_NAME)?,
                    filter: RuleFilter::True,
                },
            ),
            Catalog::Subscriptions => proposal(
                &topic(),
                30,
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new("worker")?,
                    config: SubscriptionConfig::default(),
                },
            ),
        };
        let (corruption, undo) = corrupt_catalog(&fixture, catalog, 0)?;
        let collision_error = match catalog {
            Catalog::Rules => BrokerError::RuleAlreadyExists {
                name: RuleName::new(domain::DEFAULT_RULE_NAME)?,
            },
            Catalog::Subscriptions => BrokerError::SubscriptionAlreadyExists,
        };
        checkpoint_only(&fixture, &mut writer, index, &collision, collision_error);
        assert!(!fixture.trace().scans.contains(&catalog.prefix()));
        index += 1;
        denied(&fixture, &mut writer, index, &original, corruption);
        fixture.raw(undo);
        match catalog {
            Catalog::Rules => {
                let invalid = proposal(
                    &subscription(),
                    30,
                    CommandKind::CreateRule {
                        name: RuleName::new("new-rule")?,
                        filter: RuleFilter::Correlation(CorrelationFilter::default()),
                    },
                );
                checkpoint_only(
                    &fixture,
                    &mut writer,
                    index,
                    &invalid,
                    BrokerError::RuleConfig(RuleConfigError::EmptyCorrelationFilter),
                );
                assert!(!fixture.trace().scans.contains(&catalog.prefix()));
            }
            Catalog::Subscriptions => {
                let invalid = proposal(
                    &topic(),
                    30,
                    CommandKind::CreateSubscription {
                        name: SubscriptionName::new("new-child")?,
                        config: SubscriptionConfig {
                            lock_duration_millis: 0,
                            ..SubscriptionConfig::default()
                        },
                    },
                );
                // Healthy capacity precedes the requested child's validation.
                checkpoint_only(&fixture, &mut writer, index, &invalid, catalog.error());
            }
        }
        index += 1;
        let regression = with_authority(&fixture, catalog.create(5), false)?;
        checkpoint_only(
            &fixture,
            &mut writer,
            index,
            &regression,
            BrokerError::ClockRegression {
                last_applied: Timestamp::from_millis(20),
                proposed: Timestamp::from_millis(5),
            },
        );
        assert!(!fixture.trace().scans.contains(&catalog.prefix()));
        index += 1;
        let clock_key = keys::clock();
        let clock = fixture.store().inner.get(&clock_key)?.unwrap();
        let entity = &original.command().entity;
        let head_key = keys::entity_metadata(&namespace(), entity);
        let head = fixture.store().inner.get(&head_key)?.unwrap();
        fixture.raw(
            WriteBatch::default()
                .put(clock_key.clone(), vec![255])
                .put(head_key.clone(), vec![255]),
        );
        denied(
            &fixture,
            &mut writer,
            index,
            &bound,
            BrokerError::EntityMetadataCorrupt,
        );
        assert!(!fixture.trace().gets.contains(&clock_key));
        fixture.raw(WriteBatch::default().put(
            head_key.clone(),
            codec::encode(&SelectedHead {
                generation: 2,
                kind: match catalog {
                    Catalog::Rules => 2,
                    Catalog::Subscriptions => 1,
                },
                retired: false,
            })?,
        ));
        checkpoint_only(
            &fixture,
            &mut writer,
            index,
            &bound,
            BrokerError::StaleEntityBinding,
        );
        assert!(!fixture.trace().gets.contains(&clock_key));
        index += 1;
        fixture.raw(WriteBatch::default().put(head_key, head));
        denied(
            &fixture,
            &mut writer,
            index,
            &original,
            BrokerError::Codec(domain::CodecError::UnsupportedVersion { version: 255 }),
        );
        let wrong = BoundCommand::new(
            match bound.authority() {
                DurableProposalAuthority::Bound(binding) => binding.clone(),
                _ => unreachable!(),
            },
            command(&queue(), 30, receive()),
        );
        fixture.reset();
        assert_eq!(
            fixture.machine().apply_bound(&wrong),
            Err(BrokerError::InvalidEntityBinding)
        );
        assert_eq!(
            DurableProposal::bound(wrong),
            Err(domain::DurableProposalError::InvalidAuthority)
        );
        no_io(&fixture.trace());
        fixture.raw(WriteBatch::default().put(clock_key, clock));
        checkpoint_only(&fixture, &mut writer, index, &bound, catalog.error());
        let before = fixture.snapshot();
        drop(writer);
        fixture.reopen();
        assert_eq!(fixture.writer().applied_index()?, index);
        assert_eq!(fixture.snapshot(), before);
    }
    Ok(())
}

fn noncreation_and_unselected<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    for catalog in [Catalog::Rules, Catalog::Subscriptions] {
        fixture.clear();
        let mut writer = fixture.writer();
        seed_full(&fixture, &mut writer, catalog)?;
        writer.apply(3, &proposal(&topic(), 40, scheduled("due", 100)))?;
        let (_, undo) = corrupt_catalog(&fixture, catalog, 3)?;
        let before = domain_rows(&fixture.snapshot());
        fixture.reset();
        assert_eq!(
            writer.apply(4, &proposal(&topic(), 50, CommandKind::ActivateScheduled))?,
            applied(CommandOutcome::ScheduledActivated {
                activated: 0,
                deliverable_entities: Vec::new()
            })
        );
        assert!(!fixture.trace().scans.contains(&catalog.prefix()));
        assert_eq!(domain_rows(&fixture.snapshot()), before);
        fixture.reset();
        writer.apply(5, &proposal(&topic(), 60, scheduled("later", 300)))?;
        assert!(!fixture.trace().scans.contains(&catalog.prefix()));
        let batch = CommandKind::SendBatch {
            messages: vec![MessageInput {
                message_id: "batch".to_owned(),
                body: b"payload".to_vec(),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            }],
        };
        for kind in [send("immediate"), batch, CommandKind::ActivateScheduled] {
            denied(
                &fixture,
                &mut writer,
                6,
                &proposal(&topic(), 200, kind),
                catalog.error(),
            );
        }
        let next = if matches!(catalog, Catalog::Rules) {
            denied(
                &fixture,
                &mut writer,
                6,
                &proposal(
                    &subscription(),
                    200,
                    CommandKind::ListRules {
                        skip: 0,
                        max_rules: 1,
                    },
                ),
                catalog.error(),
            );
            fixture.reset();
            assert_eq!(
                writer.apply(
                    6,
                    &proposal(
                        &subscription(),
                        70,
                        CommandKind::DeleteRule {
                            name: RuleName::new("zz-overfull")?
                        }
                    )
                )?,
                applied(CommandOutcome::RuleDeleted)
            );
            assert!(!fixture.trace().scans.contains(&catalog.prefix()));
            7
        } else {
            fixture.raw(undo);
            6
        };
        assert!(matches!(
            writer.apply(
                next,
                &proposal(&topic(), 200, CommandKind::ActivateScheduled)
            )?,
            IndexedApplyOutcome::Applied(CommandOutcome::ScheduledActivated { activated: 1, .. })
        ));
        assert_eq!(
            fixture.machine().last_applied_time()?,
            Timestamp::from_millis(200)
        );
        let before = fixture.snapshot();
        drop(writer);
        fixture.reopen();
        assert_eq!(fixture.writer().applied_index()?, next);
        assert_eq!(fixture.snapshot(), before);
    }
    Ok(())
}

fn latest_refusal_is_read_free<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    for catalog in [Catalog::Rules, Catalog::Subscriptions] {
        for bound in [false, true] {
            fixture.clear();
            let mut writer = fixture.writer();
            seed_full(&fixture, &mut writer, catalog)?;
            let original = with_authority(&fixture, catalog.create(30), bound)?;
            checkpoint_only(&fixture, &mut writer, 3, &original, catalog.error());
            let (_, undo) = corrupt_catalog(&fixture, catalog, 0)?;
            let clock = keys::clock();
            let old_clock = fixture.store().inner.get(&clock)?.unwrap();
            fixture.raw(WriteBatch::default().put(clock.clone(), vec![255]));
            *fixture.store().read_fault.lock().unwrap() = Some(clock.clone());
            let corrupt = fixture.snapshot();
            fixture.reset();
            assert_eq!(
                writer.apply(3, &original)?,
                IndexedApplyOutcome::AlreadyApplied
            );
            let different = catalog.create(31);
            assert_eq!(
                writer.apply(3, &different),
                Err(IndexedApplyError::ConflictingProposal { index: 3 })
            );
            no_io(&fixture.trace());
            assert_eq!(fixture.snapshot(), corrupt);
            drop(writer);
            fixture.reopen();
            let mut writer = fixture.writer();
            *fixture.store().read_fault.lock().unwrap() = Some(clock.clone());
            fixture.reset();
            assert_eq!(
                writer.apply(3, &original)?,
                IndexedApplyOutcome::AlreadyApplied
            );
            no_io(&fixture.trace());
            assert_eq!(fixture.snapshot(), corrupt);
            *fixture.store().read_fault.lock().unwrap() = None;
            denied(
                &fixture,
                &mut writer,
                4,
                &different,
                BrokerError::Codec(domain::CodecError::UnsupportedVersion { version: 255 }),
            );
            fixture.raw(undo);
            fixture.raw(WriteBatch::default().put(clock, old_clock));
            checkpoint_only(&fixture, &mut writer, 4, &different, catalog.error());
            drop(writer);
        }
    }
    Ok(())
}

fn ambiguous_capacity_checkpoint<P: StoreProvider>(provider: P) -> TestResult {
    let mut fixture = Fixture::new(provider);
    for catalog in [Catalog::Rules, Catalog::Subscriptions] {
        for bound in [false, true] {
            for after_apply in [false, true] {
                fixture.clear();
                let mut writer = fixture.writer();
                seed_full(&fixture, &mut writer, catalog)?;
                let original = with_authority(&fixture, catalog.create(30), bound)?;
                let before = fixture.snapshot();
                fixture.reset();
                fixture
                    .store()
                    .apply_fault
                    .store(if after_apply { 2 } else { 1 }, Ordering::SeqCst);
                assert_eq!(
                    writer.apply(3, &original),
                    Err(IndexedApplyError::Storage(simulated_error()))
                );
                let trace = fixture.trace();
                let expected =
                    WriteBatch::default().put(key(1), checkpoint(3, proposal_hash(&original)));
                assert_eq!(trace.batches, vec![expected.clone()]);
                let complete = apply_rows(&before, &expected);
                assert_eq!(
                    fixture.snapshot(),
                    if after_apply {
                        complete.clone()
                    } else {
                        before.clone()
                    }
                );
                fixture.reset();
                assert_eq!(writer.applied_index(), Err(IndexedApplyError::Unusable));
                for index in [0, 3, 4] {
                    assert_eq!(
                        writer.apply(index, &original),
                        Err(IndexedApplyError::Unusable)
                    );
                }
                no_io(&fixture.trace());
                drop(writer);
                fixture.reopen();
                assert_eq!(
                    fixture.snapshot(),
                    if after_apply {
                        complete.clone()
                    } else {
                        before
                    }
                );
                let mut writer = fixture.writer();
                assert_eq!(writer.applied_index()?, if after_apply { 3 } else { 2 });
                fixture.reset();
                assert_eq!(
                    writer.apply(3, &original)?,
                    if after_apply {
                        IndexedApplyOutcome::AlreadyApplied
                    } else {
                        IndexedApplyOutcome::Refused(catalog.error())
                    }
                );
                if after_apply {
                    no_io(&fixture.trace());
                } else {
                    assert_eq!(fixture.trace().batches, vec![expected]);
                }
                assert_eq!(fixture.snapshot(), complete);
                assert_eq!(
                    fixture.machine().last_applied_time()?,
                    Timestamp::from_millis(20)
                );
                drop(writer);
            }
        }
    }
    Ok(())
}

macro_rules! paired {
    ($memory:ident, $fjall:ident, $body:ident) => {
        #[test]
        fn $memory() -> TestResult {
            $body(MemoryProvider::new())
        }
        #[test]
        fn $fjall() -> TestResult {
            $body(DurableProvider::temporary()?)
        }
    };
}

paired!(
    memory_healthy_capacity_checkpoints_exact_refusal_without_rereads,
    fjall_healthy_capacity_checkpoints_exact_refusal_without_rereads,
    healthy_exact_capacity
);
paired!(
    memory_malformed_and_overfull_capacity_origins_never_checkpoint,
    fjall_malformed_and_overfull_capacity_origins_never_checkpoint,
    malformed_and_overfull_origins
);
paired!(
    memory_capacity_provenance_preserves_earlier_error_priorities,
    fjall_capacity_provenance_preserves_earlier_error_priorities,
    earlier_priorities
);
paired!(
    memory_noncreation_caps_and_unselected_paths_keep_their_boundaries,
    fjall_noncreation_caps_and_unselected_paths_keep_their_boundaries,
    noncreation_and_unselected
);
paired!(
    memory_latest_capacity_refusal_is_outcome_free_and_read_free,
    fjall_latest_capacity_refusal_is_outcome_free_and_read_free,
    latest_refusal_is_read_free
);
paired!(
    memory_ambiguous_capacity_checkpoint_retires_until_reopen,
    fjall_ambiguous_capacity_checkpoint_retires_until_reopen,
    ambiguous_capacity_checkpoint
);
