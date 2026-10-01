use tokio::{io::DuplexStream, time::timeout};

use super::*;
use crate::{Source, Target};

const DEADLINE: Duration = Duration::from_secs(2);

struct Fixture {
    sessions: HashMap<u16, SessionState>,
    writer: FrameWriter<DuplexStream>,
    peer: DuplexStream,
    incoming_sessions: mpsc::Sender<IncomingSession>,
    incoming_attaches: mpsc::Receiver<IncomingAttach>,
    affected: mpsc::Receiver<Delivery>,
    healthy: mpsc::Receiver<Delivery>,
    detached: watch::Receiver<bool>,
    affected_owner: LinkIdentity,
}

impl Fixture {
    fn new() -> Self {
        let (wire, peer) = tokio::io::duplex(64 * 1024);
        let (incoming_sessions, _) = mpsc::channel(32);
        let (attaches, incoming_attaches) = mpsc::channel(32);
        let mut session = SessionState::new(&Begin::default());
        session.peer_channel = Some(0);
        session.local_begin_sent = true;
        session.attach_tx = Some(attaches);
        let mut receivers = Vec::new();
        let mut affected_detached = None;
        let affected_owner = LinkIdentity::new();
        for handle in 0..2 {
            let (deliveries, receiver) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
            let (detached, observed_detached) = watch::channel(false);
            let mut credit = ReceiveCredit::new(
                0,
                LINK_CREDIT,
                Arc::new(Consumption::new(Arc::new(Notify::new()))),
            );
            credit.take_refill();
            session.links.insert(
                handle,
                LinkState::Receiving(ReceivingLink {
                    max_message_size: 4 * 1024 * 1024,
                    deliveries,
                    partial: None,
                    detached,
                    credit,
                    decoders: MessageFormatDecoders::default(),
                    identity: if handle == 0 {
                        affected_owner.clone()
                    } else {
                        LinkIdentity::new()
                    },
                    sender_settle_mode: SenderSettleMode::Mixed,
                    receiver_settle_mode: ReceiverSettleMode::First,
                }),
            );
            receivers.push(receiver);
            if handle == 0 {
                affected_detached = Some(observed_detached);
            }
        }
        let healthy = receivers.pop().expect("healthy queue");
        let affected = receivers.pop().expect("affected queue");
        Self {
            sessions: HashMap::from([(0, session)]),
            writer: FrameWriter::new(wire, 512).expect("frame writer"),
            peer,
            incoming_sessions,
            incoming_attaches,
            affected,
            healthy,
            detached: affected_detached.expect("affected detach signal"),
            affected_owner,
        }
    }

    async fn transfer(&mut self, transfer: Transfer, payload: Vec<u8>) {
        timeout(
            DEADLINE,
            receive_transfer(0, transfer, payload, &mut self.sessions, &mut self.writer),
        )
        .await
        .expect("prompt transfer handling")
        .expect("unsupported recovery does not close the connection");
    }

    async fn attach(&mut self, attach: Attach) {
        let action = timeout(
            DEADLINE,
            handle_frame(
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Attach(Box::new(attach))),
                    payload: Vec::new(),
                },
                &mut self.writer,
                &self.incoming_sessions,
                &mut self.sessions,
                512,
                u16::MAX,
                false,
            ),
        )
        .await
        .expect("prompt attach handling")
        .expect("unsupported recovery does not close the connection");
        assert!(matches!(action, FrameAction::Continue));
    }

    async fn frame(&mut self) -> Frame {
        timeout(DEADLINE, read_frame(&mut self.peer))
            .await
            .expect("prompt control frame")
            .expect("encoded control frame")
    }

    async fn refusal(&mut self, handle: u32) {
        let Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Detach(detach)),
            ..
        } = self.frame().await
        else {
            panic!("recovery is refused link-locally");
        };
        assert_eq!(detach.handle, handle);
        assert!(detach.closed);
        assert_eq!(
            detach
                .error
                .expect("recovery refusal condition")
                .condition
                .as_symbol(),
            Symbol::from("amqp:not-implemented")
        );
        assert!(!self.sessions[&0].ending);
        assert!(!self.sessions[&0].links.contains_key(&handle));
        assert!(self.sessions[&0].links.contains_key(&1));
    }

    async fn assert_healthy(&mut self) {
        self.transfer(first(1, 100), encoded()).await;
        assert_eq!(
            timeout(DEADLINE, self.healthy.recv())
                .await
                .expect("healthy receiver remains responsive")
                .expect("healthy delivery")
                .message(),
            &message()
        );
    }

    fn assert_alias_available(&mut self, id: u32, tag: &[u8]) {
        let owner = LinkIdentity::new();
        let incoming = &mut self.sessions.get_mut(&0).expect("live session").incoming;
        let token = incoming
            .reserve(&owner, id, tag)
            .expect("no retained recovery alias");
        incoming.abort(&token).expect("release probe reservation");
    }
}

fn message() -> Message {
    Message::data(b"recovery is not fresh content".to_vec())
}

fn encoded() -> Vec<u8> {
    encode_message(&message()).expect("encoded message")
}

fn continuation(handle: u32) -> Transfer {
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

fn first(handle: u32, id: u32) -> Transfer {
    Transfer {
        delivery_id: Some(id),
        delivery_tag: Some(id.to_be_bytes().to_vec().into()),
        message_format: Some(0),
        ..continuation(handle)
    }
}

fn attach(handle: u32, role: Role) -> Attach {
    Attach {
        name: format!("recovery-{handle}"),
        handle,
        role: role.clone(),
        snd_settle_mode: SenderSettleMode::Mixed,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::new("source")),
        target: Some(Target::new("target")),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: (role == Role::Sender).then_some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

fn add_recovery_state(attach: &mut Attach, case: usize) {
    match case {
        0 => {
            attach.unsettled = Some([(vec![1, 0, 255].into(), None)].into_iter().collect());
        }
        1 => attach.incomplete_unsettled = true,
        2 => {
            attach.unsettled = Some(Default::default());
            attach.incomplete_unsettled = true;
        }
        _ => unreachable!("three recovery state cases"),
    }
}

#[tokio::test]
async fn retained_or_incomplete_attach_state_is_refused_before_application_approval() {
    for role in [Role::Sender, Role::Receiver] {
        for case in 0..3 {
            let mut fixture = Fixture::new();
            let mut request = attach(2, role.clone());
            add_recovery_state(&mut request, case);
            fixture.attach(request).await;
            let Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Attach(response)),
                ..
            } = fixture.frame().await
            else {
                panic!("refusal still responds to the attach exchange");
            };
            assert_eq!(response.handle, 2);
            assert_eq!(response.role, role.opposite());
            if role == Role::Sender {
                assert!(response.target.is_none());
            } else {
                assert!(response.source.is_none());
            }
            assert!(response.unsettled.is_none());
            assert!(!response.incomplete_unsettled);
            fixture.refusal(2).await;
            assert!(matches!(
                fixture.incoming_attaches.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            assert!(fixture.sessions[&0].pending_attaches.is_empty());
            assert_eq!(fixture.sessions[&0].flow.snapshot().next_incoming_id, 0);
            fixture.assert_healthy().await;
        }
    }
}

#[tokio::test]
async fn complete_empty_unsettled_map_is_safe_for_fresh_attach_approval() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new();
        let mut request = attach(2, role);
        request.unsettled = Some(Default::default());
        fixture.attach(request.clone()).await;
        assert_eq!(
            fixture
                .incoming_attaches
                .try_recv()
                .expect("fresh attach approval")
                .attach(),
            &request
        );
        assert!(fixture.sessions[&0].pending_attaches.contains_key(&2));
        assert!(!fixture.sessions[&0].ending);
        assert_eq!(fixture.sessions[&0].flow.snapshot().next_incoming_id, 0);
        fixture.assert_healthy().await;
    }
}

#[tokio::test]
async fn caller_mutated_recovery_state_is_rejected_before_queueing_acceptance() {
    for role in [Role::Sender, Role::Receiver] {
        for case in 0..3 {
            let mut fixture = Fixture::new();
            fixture.attach(attach(2, role.clone())).await;
            let original = fixture
                .incoming_attaches
                .try_recv()
                .expect("original fresh approval");
            let (commands, mut queued) = mpsc::channel(4);
            let (_, incoming_attaches) = mpsc::channel(1);
            let session = ServerSession {
                channel: 0,
                identity: fixture.sessions[&0].identity.clone(),
                commands,
                incoming_attaches,
                consumed: Arc::new(Notify::new()),
            };
            let mut mutated = original.clone();
            add_recovery_state(&mut mutated, case);
            let result = timeout(DEADLINE, session.accept_attach(mutated, 262_144))
                .await
                .expect("local validation is prompt");
            assert!(matches!(
                result,
                Err(EngineError::InvalidState(reason)) if reason == RECOVERY_NOT_IMPLEMENTED
            ));
            assert!(matches!(
                queued.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            assert!(fixture.sessions[&0].pending_attaches.contains_key(&2));
            assert!(fixture.sessions[&0].closing_handles.is_empty());
            assert_eq!(fixture.sessions[&0].links.len(), 2);
            assert_eq!(fixture.sessions[&0].flow.snapshot().next_incoming_id, 0);
            assert!(
                timeout(Duration::from_millis(20), read_frame(&mut fixture.peer))
                    .await
                    .is_err()
            );

            let (endpoint, action) = timeout(DEADLINE, async {
                tokio::join!(session.accept_attach(original, 262_144), async {
                    let command = queued.recv().await.expect("valid retry is queued");
                    handle_command(command, &mut fixture.writer, &mut fixture.sessions, 512).await
                })
            })
            .await
            .expect("valid approval remains usable");
            let endpoint = endpoint.expect("valid approval accepted");
            assert!(matches!(action, Ok(CommandAction::Continue)));
            assert!(matches!(
                (&role, endpoint),
                (Role::Sender, LinkEndpoint::Receiver(_))
                    | (Role::Receiver, LinkEndpoint::Sender(_))
            ));
            assert!(!fixture.sessions[&0].pending_attaches.contains_key(&2));
            assert!(fixture.sessions[&0].links.contains_key(&2));
            assert!(matches!(
                fixture.frame().await,
                Frame::Amqp {
                    performative: Some(Performative::Attach(_)),
                    ..
                }
            ));
            if role == Role::Sender {
                assert!(matches!(
                    fixture.frame().await,
                    Frame::Amqp {
                        performative: Some(Performative::Flow(_)),
                        ..
                    }
                ));
            }
            fixture.assert_healthy().await;
        }
    }
}

#[tokio::test]
async fn forged_actor_acceptance_with_recovery_state_preserves_pending_approval() {
    for role in [Role::Sender, Role::Receiver] {
        for case in 0..3 {
            let mut fixture = Fixture::new();
            fixture.attach(attach(2, role.clone())).await;
            let original = fixture
                .incoming_attaches
                .try_recv()
                .expect("original fresh approval");
            let (deliveries_tx, _deliveries) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
            let (detached_tx, _detached) = watch::channel(false);
            let consumption = Arc::new(Consumption::new(Arc::new(Notify::new())));
            for mutated in [true, false] {
                let mut request = original.clone();
                if mutated {
                    add_recovery_state(&mut request, case);
                }
                let (reply, result) = oneshot::channel();
                let command = Command::AcceptLink {
                    channel: 0,
                    session: fixture.sessions[&0].identity.clone(),
                    attach: Box::new(request),
                    max_message_size: 262_144,
                    properties: None,
                    decoders: MessageFormatDecoders::default(),
                    deliveries_tx: deliveries_tx.clone(),
                    detached_tx: detached_tx.clone(),
                    consumption: consumption.clone(),
                    reply,
                };
                let action = timeout(
                    DEADLINE,
                    handle_command(command, &mut fixture.writer, &mut fixture.sessions, 512),
                )
                .await
                .expect("actor validation is prompt")
                .expect("invalid local command does not close the connection");
                assert!(matches!(action, CommandAction::Continue));
                let result = result.await.expect("command has a result");
                if mutated {
                    assert!(matches!(
                        result,
                        Err(EngineError::InvalidState(reason)) if reason == RECOVERY_NOT_IMPLEMENTED
                    ));
                    assert!(fixture.sessions[&0].pending_attaches.contains_key(&2));
                    assert!(fixture.sessions[&0].closing_handles.is_empty());
                    assert_eq!(fixture.sessions[&0].links.len(), 2);
                    assert_eq!(fixture.sessions[&0].flow.snapshot().next_incoming_id, 0);
                    assert!(
                        timeout(Duration::from_millis(20), read_frame(&mut fixture.peer))
                            .await
                            .is_err()
                    );
                } else {
                    result.expect("original pending approval survives misuse");
                }
            }
            assert!(!fixture.sessions[&0].pending_attaches.contains_key(&2));
            assert!(fixture.sessions[&0].links.contains_key(&2));
            assert!(matches!(
                fixture.frame().await,
                Frame::Amqp {
                    performative: Some(Performative::Attach(_)),
                    ..
                }
            ));
            if role == Role::Sender {
                assert!(matches!(
                    fixture.frame().await,
                    Frame::Amqp {
                        performative: Some(Performative::Flow(_)),
                        ..
                    }
                ));
            }
            fixture.assert_healthy().await;
        }
    }
}

#[tokio::test]
async fn resumed_first_transfer_never_decodes_or_publishes_payload_even_when_aborted_or_fragmented()
{
    fn forbidden_decoder(_: &[u8]) -> io::Result<Message> {
        panic!("unsupported resume payload must not reach a registered decoder");
    }

    for (more, aborted, payload) in [
        (false, false, encoded()),
        (true, false, vec![255; 4 * 1024]),
        (true, true, encoded()),
        (false, true, Vec::new()),
    ] {
        let mut fixture = Fixture::new();
        let Some(LinkState::Receiving(link)) = fixture
            .sessions
            .get_mut(&0)
            .and_then(|session| session.links.get_mut(&0))
        else {
            panic!("affected receiving link");
        };
        link.decoders = MessageFormatDecoders::default()
            .with_decoder(42, forbidden_decoder)
            .expect("registered outer format");
        let mut transfer = first(0, 17);
        transfer.resume = true;
        transfer.more = more;
        transfer.aborted = aborted;
        transfer.message_format = Some(42);
        fixture.transfer(transfer, payload).await;
        fixture.refusal(0).await;
        assert!(*fixture.detached.borrow());
        assert!(fixture.affected.try_recv().is_err());
        assert_eq!(fixture.sessions[&0].flow.snapshot().next_incoming_id, 1);
        fixture.assert_alias_available(17, &17_u32.to_be_bytes());
        fixture.assert_healthy().await;
    }
}

#[tokio::test]
async fn resume_refusal_precedes_existing_delivery_collision_and_exhausted_credit() {
    for exhausted_credit in [false, true] {
        let mut fixture = Fixture::new();
        let session = fixture.sessions.get_mut(&0).expect("live session");
        let old = session
            .incoming
            .reserve(&fixture.affected_owner, 17, &17_u32.to_be_bytes())
            .expect("existing application-held delivery");
        session
            .incoming
            .complete(&old, false, ReceiverSettleMode::First)
            .expect("existing complete receipt");
        if exhausted_credit {
            let Some(LinkState::Receiving(link)) = session.links.get_mut(&0) else {
                panic!("affected receiving link");
            };
            for _ in 0..LINK_CREDIT {
                link.credit.try_begin_delivery().expect("consume grant");
            }
            assert_eq!(link.credit.snapshot().link_credit, 0);
        }
        let mut transfer = first(0, 17);
        transfer.resume = true;
        fixture.transfer(transfer, encoded()).await;
        fixture.refusal(0).await;
        assert_eq!(fixture.sessions[&0].flow.snapshot().next_incoming_id, 1);
        assert!(fixture.affected.try_recv().is_err());
        assert_eq!(
            fixture.sessions[&0]
                .incoming
                .settlement(&fixture.affected_owner, &old),
            Err(incoming_ledger::IncomingLedgerError::RetiredLink)
        );
        fixture.assert_alias_available(17, &17_u32.to_be_bytes());
        fixture.assert_healthy().await;
    }
}

#[tokio::test]
async fn resumed_continuation_discards_only_its_original_partial_and_owner() {
    let mut fixture = Fixture::new();
    let payload = encoded();
    let split = payload.len() / 2;
    let mut initial = first(0, 17);
    initial.more = true;
    fixture.transfer(initial, payload[..split].to_vec()).await;
    let token = {
        let Some(LinkState::Receiving(link)) = fixture.sessions[&0].links.get(&0) else {
            panic!("live partial receiving link");
        };
        let partial = link.partial.as_ref().expect("partial delivery buffered");
        assert_eq!(partial.bytes, payload[..split]);
        partial.identity.clone()
    };
    let mut resumed = continuation(0);
    resumed.resume = true;
    fixture.transfer(resumed, payload[split..].to_vec()).await;
    fixture.refusal(0).await;
    assert!(fixture.affected.try_recv().is_err());
    assert_eq!(fixture.sessions[&0].flow.snapshot().next_incoming_id, 2);
    assert_eq!(
        fixture.sessions[&0]
            .incoming
            .settlement(&fixture.affected_owner, &token),
        Err(incoming_ledger::IncomingLedgerError::RetiredLink)
    );
    fixture.assert_alias_available(17, &17_u32.to_be_bytes());
    fixture.assert_healthy().await;
}

#[cfg(feature = "test-client")]
async fn client_pair() -> (ClientConnection, DuplexStream) {
    let (wire, mut peer) = tokio::io::duplex(64 * 1024);
    let responding = async {
        expect_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("client header");
        write_protocol_header(&mut peer, ProtocolHeader::AMQP)
            .await
            .expect("peer header");
        assert!(matches!(
            timeout(DEADLINE, read_frame(&mut peer))
                .await
                .expect("client Open deadline")
                .expect("client Open"),
            Frame::Amqp {
                performative: Some(Performative::Open(_)),
                ..
            }
        ));
        write_amqp(
            &mut peer,
            0,
            Performative::Open(Open::new("peer")),
            Vec::new(),
        )
        .await
        .expect("peer Open");
        peer
    };
    let (connection, peer) = timeout(DEADLINE, async {
        tokio::join!(ClientConnection::open(wire, "client", None), responding)
    })
    .await
    .expect("client connection deadline");
    (connection.expect("client connection"), peer)
}

#[cfg(feature = "test-client")]
async fn client_session(
    connection: &mut ClientConnection,
    peer: &mut DuplexStream,
) -> ClientSession {
    let responding = async {
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Begin(_)),
            ..
        } = timeout(DEADLINE, read_frame(peer))
            .await
            .expect("client Begin deadline")
            .expect("client Begin")
        else {
            panic!("client Begin");
        };
        write_amqp(
            peer,
            channel,
            Performative::Begin(Begin {
                remote_channel: Some(channel),
                ..Begin::default()
            }),
            Vec::new(),
        )
        .await
        .expect("peer Begin");
    };
    let (session, ()) = timeout(DEADLINE, async {
        tokio::join!(connection.begin(), responding)
    })
    .await
    .expect("session deadline");
    session.expect("client session")
}

#[cfg(feature = "test-client")]
#[tokio::test]
async fn client_rejects_retained_or_incomplete_attach_response_before_exposing_endpoint() {
    for case in 0..3 {
        let (mut connection, mut peer) = client_pair().await;
        let mut session = client_session(&mut connection, &mut peer).await;
        let responding = async {
            let Frame::Amqp {
                channel,
                performative: Some(Performative::Attach(request)),
                ..
            } = timeout(DEADLINE, read_frame(&mut peer))
                .await
                .expect("client Attach deadline")
                .expect("client Attach")
            else {
                panic!("client Attach");
            };
            let mut response = request.response(request.source.clone(), request.target.clone());
            add_recovery_state(&mut response, case);
            write_amqp(
                &mut peer,
                channel,
                Performative::Attach(Box::new(response)),
                Vec::new(),
            )
            .await
            .expect("unsupported attach response");
            let Frame::Amqp {
                channel: actual_channel,
                performative: Some(Performative::Detach(detach)),
                ..
            } = timeout(DEADLINE, read_frame(&mut peer))
                .await
                .expect("client recovery refusal deadline")
                .expect("client recovery refusal")
            else {
                panic!("client must refuse before sending link credit or transfers");
            };
            assert_eq!(actual_channel, channel);
            assert_eq!(detach.handle, request.handle);
            assert!(detach.closed);
            assert_eq!(
                detach
                    .error
                    .expect("recovery condition")
                    .condition
                    .as_symbol(),
                Symbol::from("amqp:not-implemented")
            );
            write_amqp(
                &mut peer,
                channel,
                Performative::Detach(Detach {
                    handle: request.handle,
                    closed: true,
                    error: None,
                }),
                Vec::new(),
            )
            .await
            .expect("peer detach acknowledgement");
        };
        let (endpoint, ()) = timeout(DEADLINE, async {
            tokio::join!(
                session.attach_receiver("unsupported-recovery", "orders"),
                responding
            )
        })
        .await
        .expect("refused endpoint deadline");
        assert!(
            endpoint.is_err(),
            "recovery state cannot expose a fresh endpoint"
        );

        let responding = async {
            let Frame::Amqp {
                channel,
                performative: Some(Performative::Attach(request)),
                ..
            } = timeout(DEADLINE, read_frame(&mut peer))
                .await
                .expect("healthy Attach deadline")
                .expect("healthy Attach")
            else {
                panic!("healthy sibling Attach");
            };
            let mut response = request.response(request.source.clone(), request.target.clone());
            response.unsettled = Some(Default::default());
            write_amqp(
                &mut peer,
                channel,
                Performative::Attach(Box::new(response)),
                Vec::new(),
            )
            .await
            .expect("complete empty state response");
            assert!(matches!(
                timeout(DEADLINE, read_frame(&mut peer))
                    .await
                    .expect("healthy Flow deadline")
                    .expect("healthy Flow"),
                Frame::Amqp {
                    performative: Some(Performative::Flow(Flow {
                        link_credit: Some(LINK_CREDIT),
                        ..
                    })),
                    ..
                }
            ));
        };
        let (endpoint, ()) = timeout(DEADLINE, async {
            tokio::join!(
                session.attach_receiver("healthy-recovery-sibling", "orders"),
                responding
            )
        })
        .await
        .expect("healthy endpoint deadline");
        assert!(
            endpoint.is_ok(),
            "complete empty state remains a fresh endpoint"
        );
        connection.shutdown().await;
    }
}
