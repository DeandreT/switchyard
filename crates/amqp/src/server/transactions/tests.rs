use tokio::{io::DuplexStream, time::timeout};

use super::*;
use crate::{Coordinator, Declared, Source, Target, TransactionId, TransactionalState};

const DEADLINE: Duration = Duration::from_secs(2);

fn states() -> [DeliveryState; 2] {
    let txn_id = TransactionId::new([1, 2]).expect("bounded transaction id");
    [
        DeliveryState::Declared(Declared {
            txn_id: txn_id.clone(),
        }),
        DeliveryState::Transactional(TransactionalState {
            txn_id,
            outcome: Some(Outcome::Accepted(Accepted)),
        }),
    ]
}

fn request() -> Attach {
    Attach {
        name: "transaction-link".into(),
        handle: 55,
        role: Role::Sender,
        initial_delivery_count: Some(0),
        source: Some(Source::new("queue")),
        target: Some(Target::new("queue").into()),
        snd_settle_mode: SenderSettleMode::Mixed,
        rcv_settle_mode: ReceiverSettleMode::First,
        unsettled: None,
        incomplete_unsettled: false,
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

fn transfer(handle: u32) -> Transfer {
    Transfer {
        handle,
        delivery_id: None,
        delivery_tag: None,
        message_format: None,
        settled: None,
        more: false,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

struct Fixture {
    sessions: HashMap<u16, SessionState>,
    writer: FrameWriter<DuplexStream>,
    peer: DuplexStream,
    incoming: mpsc::Sender<IncomingSession>,
    budget: super::super::content_budget::ContentBudget,
}

impl Fixture {
    fn new() -> Self {
        let (wire, peer) = tokio::io::duplex(64 * 1024);
        let (incoming, _) = mpsc::channel(2);
        let budget = super::super::content_budget::ContentBudget::new(4096);
        let sessions = [0, 1]
            .into_iter()
            .map(|channel| {
                let mut session = SessionState::new(&Begin::default());
                session.peer_channel = Some(17 + channel);
                session.local_begin_sent = true;
                (channel, session)
            })
            .collect();
        Self {
            sessions,
            writer: FrameWriter::new_with_content_budget(wire, 512, budget.clone())
                .expect("writer"),
            peer,
            incoming,
            budget,
        }
    }

    fn link(
        &mut self,
        channel: u16,
        handle: u32,
        peer: u32,
        role: Role,
    ) -> (LinkIdentity, mpsc::Receiver<Delivery>) {
        let owner = LinkIdentity::new();
        let (deliveries, inbox) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        let (detached, _) = watch::channel(false);
        let link = if role == Role::Receiver {
            let mut credit = ReceiveCredit::new(
                0,
                LINK_CREDIT,
                Arc::new(Consumption::new(Arc::new(Notify::new()))),
            );
            credit.take_refill();
            LinkState::Receiving(Box::new(ReceivingLink {
                identity: owner.clone(),
                max_message_size: 4096,
                deliveries: deliveries.into(),
                partial: None,
                detached,
                credit,
                decoders: MessageFormatDecoders::default(),
                sender_settle_mode: SenderSettleMode::Mixed,
                receiver_settle_mode: ReceiverSettleMode::First,
            }))
        } else {
            LinkState::Sending(Box::new(SendingLink {
                identity: owner.clone(),
                auto_acknowledge: false,
                max_message_size: None,
                receiver_settle_mode: ReceiverSettleMode::Second,
                default_outcome: None,
                outstanding_tags: HashSet::new(),
                settle_mode: SenderSettleMode::Mixed,
                credit: LinkCredit::new(0),
                queued: VecDeque::new(),
                active: None,
                unsettled: HashMap::new(),
                pending_acknowledgements: HashMap::new(),
                detached,
            }))
        };
        let session = self.sessions.get_mut(&channel).expect("session");
        session.links.insert(handle, link);
        session.handle_aliases.insert(
            handle,
            HandleAlias {
                identity: owner.clone(),
                name: format!("link-{channel}-{handle}").into(),
                role,
                peer_handle: Some(peer),
                own_attach_sent: true,
                error_detached: false,
            },
        );
        (owner, inbox)
    }

    async fn input(&mut self, performative: Performative, payload: Vec<u8>) -> FrameAction {
        timeout(
            DEADLINE,
            handle_frame(
                Frame::Amqp {
                    channel: 17,
                    performative: Some(performative),
                    payload,
                },
                &mut self.writer,
                &self.incoming,
                &mut self.sessions,
                512,
                u16::MAX,
                false,
            ),
        )
        .await
        .expect("prompt frame")
        .expect("frame handling")
    }

    async fn frame(&mut self) -> (u16, Performative) {
        let Frame::Amqp {
            channel,
            performative: Some(performative),
            payload,
        } = timeout(DEADLINE, read_frame(&mut self.peer))
            .await
            .expect("prompt refusal")
            .expect("frame")
        else {
            panic!("AMQP performative")
        };
        assert!(payload.is_empty());
        (channel, performative)
    }

    fn fail_end_preflight(&mut self) {
        let session = self.sessions.get_mut(&0).expect("session");
        session.local_begin_sent = false;
        session.peer_channel = None;
    }
}

fn assert_end(frame: (u16, Performative), condition: &str) {
    assert!(
        matches!(frame, (0, Performative::End(end)) if end.error.as_ref().expect("error").condition.as_symbol().as_str() == condition)
    );
}

#[tokio::test]
async fn unsupported_attaches_never_publish_aliases_or_approval_events() {
    let mut variants = vec![request()];
    variants[0].target = Some(Coordinator::default().into());
    for state in states() {
        let mut attach = request();
        attach.source.as_mut().expect("source").default_outcome = Some(state);
        variants.push(attach);
    }
    for symbol in ["amqp:declared:list", "amqp:transactional-state:list"] {
        let mut attach = request();
        attach.source.as_mut().expect("source").outcomes = Some(vec![Symbol::from(symbol)].into());
        variants.push(attach);
    }
    for attach in variants {
        let mut fixture = Fixture::new();
        let (approval, mut events) = mpsc::channel(1);
        fixture.sessions.get_mut(&0).expect("session").attach_tx = Some(approval);
        let (healthy, _) = fixture.link(1, 0, 77, Role::Receiver);
        assert!(matches!(
            fixture
                .input(Performative::Attach(Box::new(attach)), Vec::new())
                .await,
            FrameAction::Continue
        ));
        assert_end(fixture.frame().await, "amqp:not-implemented");
        assert!(events.try_recv().is_err());
        assert!(fixture.sessions[&0].handle_aliases.is_empty());
        assert!(fixture.sessions[&0].pending_attaches.is_empty());
        assert!(fixture.sessions[&0].ending);
        assert!(!healthy.is_retired() && !fixture.sessions[&1].ending);
    }
}

#[tokio::test]
async fn duplicate_handle_and_historical_recovery_keep_their_refusal_priority() {
    let mut fixture = Fixture::new();
    fixture.link(0, 0, 55, Role::Receiver);
    let mut attach = request();
    attach.target = Some(Coordinator::default().into());
    assert!(matches!(
        fixture
            .input(Performative::Attach(Box::new(attach)), Vec::new())
            .await,
        FrameAction::CloseSent
    ));
    assert!(
        matches!(fixture.frame().await, (0, Performative::Close(close)) if close.error.as_ref().expect("error").condition.as_symbol().as_str() == "amqp:session:handle-in-use")
    );

    for state in states() {
        let mut fixture = Fixture::new();
        let owner = LinkIdentity::new();
        fixture
            .writer
            .error_link_names_mut()
            .record("transaction-link".into(), &Role::Receiver, &owner)
            .expect("history");
        let (approval, mut events) = mpsc::channel(1);
        fixture.sessions.get_mut(&0).expect("session").attach_tx = Some(approval);
        let mut attach = request();
        attach.target = Some(Coordinator::default().into());
        attach.source.as_mut().expect("source").default_outcome = Some(state);
        attach.unsettled = Some(Default::default());
        fixture
            .input(Performative::Attach(Box::new(attach)), Vec::new())
            .await;
        assert!(
            matches!(fixture.frame().await, (0, Performative::Attach(attach)) if attach.handle == 55 && attach.target.is_none())
        );
        assert!(
            matches!(fixture.frame().await, (0, Performative::Detach(detach)) if detach.handle == 55 && detach.error.as_ref().expect("error").condition.as_symbol().as_str() == "amqp:not-implemented")
        );
        assert!(events.try_recv().is_err());
        assert!(!fixture.sessions[&0].ending);
        assert!(is_error_detached(&fixture.sessions[&0], 55));
    }
}

#[tokio::test]
async fn transactional_first_and_continuation_transfers_precede_window_credit_and_content_mutation()
{
    for continuation in [false, true] {
        for state in states() {
            let mut fixture = Fixture::new();
            let (owner, mut inbox) = fixture.link(0, 0, 55, Role::Receiver);
            if continuation {
                receive_transfer(
                    0,
                    Transfer {
                        delivery_id: Some(7),
                        delivery_tag: Some(vec![7].into()),
                        message_format: Some(0),
                        more: true,
                        ..transfer(0)
                    },
                    vec![1, 2],
                    &mut fixture.sessions,
                    &mut fixture.writer,
                )
                .await
                .expect("partial");
            }
            let session = &fixture.sessions[&0];
            let flow = session.flow.clone();
            let LinkState::Receiving(link) = &session.links[&0] else {
                panic!("receiver")
            };
            let credit = link.credit.snapshot();
            let retained = fixture.budget.retained_bytes();
            fixture.fail_end_preflight();
            assert!(
                receive_transfer(
                    0,
                    Transfer {
                        delivery_id: Some(7),
                        delivery_tag: Some(vec![7].into()),
                        state: Some(state),
                        ..transfer(0)
                    },
                    vec![3, 4],
                    &mut fixture.sessions,
                    &mut fixture.writer
                )
                .await
                .is_err()
            );
            let session = &fixture.sessions[&0];
            let LinkState::Receiving(link) = &session.links[&0] else {
                panic!("receiver")
            };
            assert_eq!(session.flow, flow);
            assert_eq!(link.credit.snapshot(), credit);
            assert_eq!(link.partial.is_some(), continuation);
            if let Some(partial) = &link.partial {
                assert_eq!(partial.bytes, [1, 2]);
            }
            assert_eq!(fixture.budget.retained_bytes(), retained);
            assert!(inbox.try_recv().is_err() && !owner.is_retired());
        }
    }
}

#[tokio::test]
async fn transactional_disposition_prechecks_entire_wrapping_range_before_settlement() {
    for role in [Role::Sender, Role::Receiver] {
        for state in states() {
            let mut fixture = Fixture::new();
            let local_role = role.opposite();
            let (owner, _) = fixture.link(0, 0, 55, local_role);
            let session = fixture.sessions.get_mut(&0).expect("session");
            let mut incoming = Vec::new();
            let mut outgoing = Vec::new();
            for id in [u32::MAX, 0, 1] {
                if role == Role::Sender {
                    let identity = session
                        .incoming
                        .reserve(&owner, id, &id.to_be_bytes())
                        .expect("reserve");
                    session
                        .incoming
                        .complete(&identity, false, ReceiverSettleMode::First)
                        .expect("complete");
                    incoming.push(identity);
                } else {
                    let LinkState::Sending(link) = session.links.get_mut(&0).expect("link") else {
                        panic!("sender")
                    };
                    let identity = AckIdentity::new(&owner, id, &id.to_be_bytes());
                    link.outstanding_tags.insert(identity.tag().to_vec());
                    link.pending_acknowledgements.insert(id, identity.clone());
                    outgoing.push(identity);
                }
            }
            fixture.fail_end_preflight();
            assert!(
                apply_disposition(
                    0,
                    Disposition {
                        role: role.clone(),
                        first: u32::MAX,
                        last: Some(1),
                        settled: true,
                        state: Some(state),
                        batchable: false
                    },
                    &mut fixture.writer,
                    &mut fixture.sessions
                )
                .await
                .is_err()
            );
            let session = &fixture.sessions[&0];
            for identity in incoming {
                assert!(
                    !session
                        .incoming
                        .sender_is_settled(&identity)
                        .expect("live receipt")
                );
            }
            if let Some(LinkState::Sending(link)) = session.links.get(&0) {
                for identity in outgoing {
                    assert!(!identity.is_settled());
                    assert!(link.pending_acknowledgements[&identity.id()].same_ack(&identity));
                    assert!(link.outstanding_tags.contains(identity.tag()));
                }
            }
            assert!(!owner.is_retired());
        }
    }
}

fn transactional_flow(handle: Option<u32>) -> Flow {
    Flow {
        handle,
        next_incoming_id: Some(0),
        next_outgoing_id: 0,
        incoming_window: 0,
        outgoing_window: 0,
        delivery_count: Some(0),
        link_credit: Some(50),
        properties: Some(
            [(Symbol::from("txn-id"), crate::Value::Null)]
                .into_iter()
                .collect(),
        ),
        ..Flow::default()
    }
}

#[tokio::test]
async fn transactional_acquisition_detaches_exact_link_before_shared_flow_mutation() {
    for direct in [false, true] {
        let mut fixture = Fixture::new();
        let (owner, _) = fixture.link(0, 0, 55, Role::Sender);
        let (healthy, _) = fixture.link(0, 1, 0, Role::Sender);
        let flow_before = fixture.sessions[&0].flow.clone();
        let result = if direct {
            apply_link_flow(
                0,
                transactional_flow(Some(0)),
                &mut fixture.writer,
                &mut fixture.sessions,
            )
            .await
        } else {
            apply_flow(
                0,
                transactional_flow(Some(0)),
                &mut fixture.writer,
                &mut fixture.sessions,
                512,
            )
            .await
        };
        result.expect("unsupported acquisition refusal");
        assert!(
            matches!(fixture.frame().await, (0, Performative::Detach(detach)) if detach.handle == 0 && detach.error.as_ref().expect("error").condition.as_symbol().as_str() == "amqp:not-implemented")
        );
        assert_eq!(fixture.sessions[&0].flow, flow_before);
        assert!(owner.is_retired() && !healthy.is_retired());
        assert!(!fixture.sessions[&0].ending);
        apply_link_flow(
            0,
            Flow {
                handle: Some(1),
                delivery_count: Some(0),
                link_credit: Some(32),
                ..Flow::default()
            },
            &mut fixture.writer,
            &mut fixture.sessions,
        )
        .await
        .expect("healthy sibling credit");
        let LinkState::Sending(link) = &fixture.sessions[&0].links[&1] else {
            panic!("healthy sender")
        };
        assert_eq!(link.credit.allowance(), 32);
    }
}

#[tokio::test]
async fn transactional_flow_preflight_failure_preserves_live_window_and_credit() {
    for direct in [false, true] {
        let mut fixture = Fixture::new();
        let (owner, _) = fixture.link(0, 0, 55, Role::Sender);
        let flow_before = fixture.sessions[&0].flow.clone();
        fixture.fail_end_preflight();
        let result = if direct {
            apply_link_flow(
                0,
                transactional_flow(Some(0)),
                &mut fixture.writer,
                &mut fixture.sessions,
            )
            .await
        } else {
            apply_flow(
                0,
                transactional_flow(Some(0)),
                &mut fixture.writer,
                &mut fixture.sessions,
                512,
            )
            .await
        };
        assert!(result.is_err());
        let session = &fixture.sessions[&0];
        let LinkState::Sending(link) = &session.links[&0] else {
            panic!("sender")
        };
        assert_eq!(session.flow, flow_before);
        assert_eq!(link.credit.allowance(), 0);
        assert!(!owner.is_retired() && !is_error_detached(session, 0));
        assert!(fixture.writer.error_link_names().len() == 0);
    }
}

#[tokio::test]
async fn error_delivery_wrapping_history_and_ending_discard_precede_transaction_refusal() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new();
        let owner = LinkIdentity::new();
        fixture
            .sessions
            .get_mut(&0)
            .expect("session")
            .error_deliveries
            .record(&role, &owner, &HashSet::from([0]))
            .expect("history");
        fixture
            .input(
                Performative::Disposition(Disposition {
                    role,
                    first: u32::MAX,
                    last: Some(1),
                    settled: true,
                    state: Some(states()[1].clone()),
                    batchable: false,
                }),
                Vec::new(),
            )
            .await;
        assert_end(fixture.frame().await, "amqp:session:errant-link");
    }
    let mut fixture = Fixture::new();
    fixture.sessions.get_mut(&0).expect("session").ending = true;
    let flow = fixture.sessions[&0].flow.clone();
    for performative in [
        Performative::Attach(Box::new(Attach {
            target: Some(Coordinator::default().into()),
            ..request()
        })),
        Performative::Transfer(Transfer {
            state: Some(states()[0].clone()),
            ..transfer(55)
        }),
        Performative::Flow(transactional_flow(Some(55))),
        Performative::Disposition(Disposition {
            role: Role::Sender,
            first: 0,
            last: None,
            settled: true,
            state: Some(states()[1].clone()),
            batchable: false,
        }),
    ] {
        fixture.input(performative, Vec::new()).await;
    }
    assert_eq!(fixture.sessions[&0].flow, flow);
    assert!(
        timeout(Duration::from_millis(20), read_frame(&mut fixture.peer))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn transaction_markers_do_not_override_error_history_or_closing_discard() {
    for closing in [false, true] {
        let mut fixture = Fixture::new();
        let (owner, _) = fixture.link(0, 0, 55, Role::Receiver);
        if closing {
            fixture
                .sessions
                .get_mut(&0)
                .expect("session")
                .closing_handles
                .insert(0);
        } else {
            let session = fixture.sessions.get_mut(&0).expect("session");
            session.closing_handles.insert(0);
            assert!(mark_error_detached(session, 0, &owner));
        }
        fixture
            .input(
                Performative::Transfer(Transfer {
                    state: Some(states()[0].clone()),
                    ..transfer(55)
                }),
                vec![1],
            )
            .await;
        if closing {
            assert!(!fixture.sessions[&0].ending);
        } else {
            assert_end(fixture.frame().await, "amqp:session:errant-link");
        }
    }
}

#[test]
fn ordinary_source_defaults_and_acknowledgements_cannot_smuggle_transaction_states() {
    for state in states() {
        let source = Source {
            default_outcome: Some(state.clone()),
            ..Source::default()
        };
        assert!(matches!(
            source_default_outcome(Some(&source)),
            Err(EngineError::InvalidState(_))
        ));
        let mut fixture = Fixture::new();
        let (owner, _) = fixture.link(0, 0, 55, Role::Sender);
        let LinkState::Sending(link) = &fixture.sessions[&0].links[&0] else {
            panic!("sender")
        };
        assert!(matches!(
            outgoing_acknowledgement(0, link, &owner, None, Some(state)),
            Err(EngineError::InvalidState(_))
        ));
    }
    assert_eq!(
        source_default_outcome(Some(&Source {
            default_outcome: Some(DeliveryState::Accepted(Accepted)),
            ..Source::default()
        }))
        .expect("ordinary outcome"),
        Some(Outcome::Accepted(Accepted))
    );
}

#[tokio::test]
async fn mutable_public_approval_cannot_install_a_coordinator_or_transaction_default() {
    for coordinator in [false, true] {
        let identity = SessionIdentity::new();
        let (commands, mut pending_commands) = mpsc::channel(1);
        let (_, incoming_attaches) = mpsc::channel(1);
        let session = ServerSession {
            channel: 0,
            identity: identity.clone(),
            commands,
            incoming_attaches,
            consumed: Arc::new(Notify::new()),
        };
        let mut receipt = IncomingAttach::new(request(), identity, 0);
        if coordinator {
            receipt.target = Some(Coordinator::default().into());
        } else {
            receipt.source.as_mut().expect("source").default_outcome = Some(states()[0].clone());
        }
        assert!(matches!(
            session.accept_attach(receipt, 4096).await,
            Err(EngineError::InvalidState(_))
        ));
        assert!(pending_commands.try_recv().is_err());
    }
}
