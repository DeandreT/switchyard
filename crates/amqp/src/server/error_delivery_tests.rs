use std::{
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use super::error_deliveries::{
    ErrorDeliveryHistory, ErrorDeliveryHistoryError, MAX_RETIRED_DELIVERIES_PER_DIRECTION,
};
use super::link_handles::HandleAlias;
use super::*;
use tokio::time::timeout;

#[derive(Default)]
struct Output {
    bytes: Mutex<Vec<u8>>,
    fail_flush: AtomicBool,
    block_flush: AtomicBool,
}

#[derive(Clone, Default)]
struct Writer(Arc<Output>);

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0
            .bytes
            .lock()
            .expect("output")
            .extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.0.fail_flush.load(Ordering::Acquire) {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "error history flush failure",
            )))
        } else if self.0.block_flush.load(Ordering::Acquire) {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct Fixture {
    sessions: HashMap<u16, SessionState>,
    output: Writer,
    writer: FrameWriter<Writer>,
}

impl Fixture {
    fn new() -> Self {
        let output = Writer::default();
        let mut sessions = HashMap::new();
        for channel in [0, 1] {
            let mut session = SessionState::new(&Begin::default());
            session.local_begin_sent = true;
            session.peer_channel = Some(17 + channel);
            sessions.insert(channel, session);
        }
        Self {
            sessions,
            writer: FrameWriter::new(output.clone(), 512).expect("frame writer"),
            output,
        }
    }

    fn link(&mut self, channel: u16, handle: u32, peer: u32, role: Role) -> LinkIdentity {
        let owner = LinkIdentity::new();
        let (detached, _) = watch::channel(false);
        let link = if role == Role::Sender {
            let mut credit = LinkCredit::new(0);
            credit
                .update_peer(Some(0), LINK_CREDIT, false)
                .expect("sender credit");
            LinkState::Sending(Box::new(SendingLink {
                identity: owner.clone(),
                auto_acknowledge: false,
                max_message_size: None,
                receiver_settle_mode: ReceiverSettleMode::Second,
                default_outcome: None,
                outstanding_tags: HashSet::new(),
                settle_mode: SenderSettleMode::Mixed,
                credit,
                queued: VecDeque::new(),
                active: None,
                unsettled: HashMap::new(),
                pending_acknowledgements: HashMap::new(),
                detached,
            }))
        } else {
            let (deliveries, _) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
            let mut credit = ReceiveCredit::new(
                0,
                LINK_CREDIT,
                Arc::new(Consumption::new(Arc::new(Notify::new()))),
            );
            credit.take_refill();
            LinkState::Receiving(ReceivingLink {
                identity: owner.clone(),
                max_message_size: u64::MAX,
                deliveries,
                partial: None,
                detached,
                credit,
                decoders: MessageFormatDecoders::default(),
                sender_settle_mode: SenderSettleMode::Mixed,
                receiver_settle_mode: ReceiverSettleMode::First,
            })
        };
        let session = self.sessions.get_mut(&channel).expect("session");
        assert!(session.links.insert(handle, link).is_none());
        assert!(
            session
                .handle_aliases
                .insert(
                    handle,
                    HandleAlias {
                        identity: owner.clone(),
                        name: format!("delivery-{channel}-{handle}").into(),
                        role,
                        peer_handle: Some(peer),
                        own_attach_sent: true,
                        error_detached: false,
                    }
                )
                .is_none()
        );
        owner
    }

    fn sender(&mut self, channel: u16, handle: u32) -> &mut SendingLink {
        let LinkState::Sending(link) = self
            .sessions
            .get_mut(&channel)
            .expect("session")
            .links
            .get_mut(&handle)
            .expect("link")
        else {
            panic!("sending link");
        };
        link
    }

    fn receiver(&mut self, handle: u32) -> &mut ReceivingLink {
        let LinkState::Receiving(link) = self
            .sessions
            .get_mut(&0)
            .expect("session")
            .links
            .get_mut(&handle)
            .expect("link")
        else {
            panic!("receiving link");
        };
        link
    }

    fn incoming(&mut self, owner: &LinkIdentity, id: u32, phase: u8) -> DeliveryIdentity {
        let ledger = &mut self.sessions.get_mut(&0).expect("session").incoming;
        let identity = ledger
            .reserve(owner, id, &id.to_be_bytes())
            .expect("incoming ID");
        if phase != 0 {
            let mode = if phase == 1 {
                ReceiverSettleMode::First
            } else {
                ReceiverSettleMode::Second
            };
            ledger
                .complete(&identity, false, mode)
                .expect("complete incoming ID");
        }
        if phase == 2 {
            ledger
                .commit_settlement(owner, &identity)
                .expect("awaiting sender ACK");
        }
        identity
    }

    fn outgoing(
        &mut self,
        channel: u16,
        handle: u32,
        id: u32,
    ) -> oneshot::Receiver<Result<SendOutcome, EngineError>> {
        let (reply, result) = oneshot::channel();
        self.sender(channel, handle).unsettled.insert(
            id,
            OutgoingDelivery {
                reply,
                delivery_tag: id.to_be_bytes().to_vec().into(),
                outcome: None,
                receiver_settled: false,
            },
        );
        result
    }

    fn historical(&mut self, role: &Role, ids: HashSet<u32>) -> LinkIdentity {
        let owner = LinkIdentity::new();
        owner.retire();
        self.sessions
            .get_mut(&0)
            .expect("session")
            .error_deliveries
            .record(role, &owner, &ids)
            .expect("historical IDs");
        owner
    }

    async fn detach_error(&mut self) {
        detach_link_error(
            0,
            0,
            self.sessions.get_mut(&0).expect("session"),
            &mut self.writer,
            "amqp:invalid-field",
            "failed link",
        )
        .await
        .expect("error detach");
        let frames = self.frames().await;
        assert!(matches!(
            frames.as_slice(),
            [Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Detach(Detach {
                    handle: 0,
                    error: Some(_),
                    ..
                })),
                ..
            }]
        ));
    }

    async fn input(&mut self, channel: u16, performative: Performative, payload: Vec<u8>) {
        let (incoming, _) = mpsc::channel(1);
        assert!(matches!(
            handle_frame(
                Frame::Amqp {
                    channel: 17 + channel,
                    performative: Some(performative),
                    payload
                },
                &mut self.writer,
                &incoming,
                &mut self.sessions,
                512,
                u16::MAX,
                false
            )
            .await
            .expect("mapped input"),
            FrameAction::Continue
        ));
    }

    async fn frames(&self) -> Vec<Frame> {
        let bytes = std::mem::take(&mut *self.output.0.bytes.lock().expect("output"));
        let mut input = bytes.as_slice();
        let mut frames = Vec::new();
        while !input.is_empty() {
            let frame = read_frame(&mut input).await.expect("captured frame");
            assert!(crate::encode_frame(&frame).expect("frame encoding").len() <= 512);
            frames.push(frame);
        }
        frames
    }

    async fn assert_end(&self, condition: &str) {
        let frames = self.frames().await;
        let [
            Frame::Amqp {
                channel: 0,
                performative: Some(Performative::End(end)),
                payload,
            },
        ] = frames.as_slice()
        else {
            panic!("one scoped End: {frames:?}");
        };
        assert!(payload.is_empty());
        assert_eq!(
            end.error.as_ref().expect("End error").condition.as_symbol(),
            Symbol::from(condition)
        );
    }

    fn owns(&self, role: &Role, id: u32, owner: &LinkIdentity) -> bool {
        self.sessions[&0]
            .error_deliveries
            .owner(role, id)
            .is_some_and(|known| known.same_link(owner))
    }
}

fn disposition(role: Role, first: u32, last: Option<u32>, settled: bool) -> Disposition {
    Disposition {
        role,
        first,
        last,
        settled,
        state: None,
        batchable: false,
    }
}

fn first(id: u32) -> Transfer {
    Transfer {
        handle: 42,
        delivery_id: Some(id),
        delivery_tag: Some(id.to_be_bytes().to_vec().into()),
        message_format: Some(0),
        settled: Some(false),
        more: true,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

#[test]
fn history_is_directional_bounded_and_preserves_the_original_owner_until_reassignment() {
    let mut history = ErrorDeliveryHistory::default();
    let original = LinkIdentity::new();
    let replacement = LinkIdentity::new();
    let ids: HashSet<_> = (0..MAX_RETIRED_DELIVERIES_PER_DIRECTION as u32).collect();
    for role in [Role::Sender, Role::Receiver] {
        history
            .check_record(&role, &ids)
            .expect("exact directional capacity");
        history
            .record(&role, &original, &ids)
            .expect("bounded history");
        history
            .record(&role, &replacement, &HashSet::from([0]))
            .expect("same numeric ID spends no slot");
        assert!(
            history
                .owner(&role, 0)
                .expect("first owner")
                .same_link(&original)
        );
        let extra = HashSet::from([MAX_RETIRED_DELIVERIES_PER_DIRECTION as u32]);
        assert!(matches!(
            history.check_record(&role, &extra),
            Err(ErrorDeliveryHistoryError::LimitReached {
                maximum: MAX_RETIRED_DELIVERIES_PER_DIRECTION
            })
        ));
        assert!(history.record(&role, &replacement, &extra).is_err());
        assert!(
            history
                .owner(&role, MAX_RETIRED_DELIVERIES_PER_DIRECTION as u32)
                .is_none()
        );
        assert!(history.contains_range(&role, u32::MAX, Some(0)));
        assert!(history.contains_range(&role, 1, Some(0)));
        assert!(!history.contains_range(&role, 5_000, Some(6_000)));
    }
    assert_eq!(
        history.outgoing_ids().count(),
        MAX_RETIRED_DELIVERIES_PER_DIRECTION
    );
    assert!(history.outgoing_contains(0));
    assert!(history.reassign_incoming(0));
    assert!(!history.reassign_incoming(0));
    assert!(
        history
            .owner(&Role::Receiver, 0)
            .expect("opposite direction retained")
            .same_link(&original)
    );
    history
        .record(&Role::Sender, &replacement, &HashSet::from([0]))
        .expect("reassigned incoming ID");
    assert!(
        history
            .owner(&Role::Sender, 0)
            .expect("replacement owner")
            .same_link(&replacement)
    );
    history.clear();
    assert_eq!(history.outgoing_ids().count(), 0);
    assert!(!history.contains_range(&Role::Sender, 1, Some(0)));
    assert!(!history.contains_range(&Role::Receiver, 1, Some(0)));
}

#[test]
fn installed_snapshots_collect_exact_incoming_phases_and_the_outgoing_live_union() {
    let mut fixture = Fixture::new();
    let incoming = fixture.link(0, 0, 42, Role::Receiver);
    for (id, phase) in [(u32::MAX, 0), (0, 1), (1, 2)] {
        fixture.incoming(&incoming, id, phase);
    }
    let foreign = fixture.link(0, 1, 43, Role::Receiver);
    fixture.incoming(&foreign, 2, 0);
    assert_eq!(
        snapshot_error_deliveries(&fixture.sessions[&0], 0, &incoming).expect("incoming snapshot"),
        Some((Role::Sender, HashSet::from([u32::MAX, 0, 1])))
    );
    assert!(
        snapshot_error_deliveries(&fixture.sessions[&0], 0, &LinkIdentity::new())
            .expect("foreign snapshot")
            .is_none()
    );
    let outgoing = fixture.link(0, 2, 44, Role::Sender);
    drop(fixture.outgoing(0, 2, 7));
    drop(fixture.outgoing(0, 2, 8));
    let lease = fixture
        .writer
        .content_budget()
        .try_reserve(1)
        .expect("active content");
    let queued_lease = fixture
        .writer
        .content_budget()
        .try_reserve(1)
        .expect("queued content");
    let (reply, result) = oneshot::channel();
    drop(result);
    let link = fixture.sender(0, 2);
    link.queued.push_back(QueuedSend {
        payload: vec![1],
        content_lease: queued_lease,
        delivery_tag: vec![77].into(),
        message_format: 0,
        reply,
    });
    link.active = Some(ActiveSend {
        payload: vec![1],
        content_lease: lease,
        offset: 0,
        first_frame_sent: true,
        delivery_id: 8,
        delivery_tag: vec![8].into(),
        message_format: 0,
        settled: false,
        settled_reply: None,
    });
    link.pending_acknowledgements
        .insert(9, AckIdentity::new(&outgoing, 9, &[9]));
    link.pending_acknowledgements
        .insert(10, AckIdentity::new(&outgoing, 100, &[10]));
    link.pending_acknowledgements
        .insert(11, AckIdentity::new(&foreign, 11, &[11]));
    let terminal = AckIdentity::new(&outgoing, 12, &[12]);
    terminal.mark_settled();
    link.pending_acknowledgements.insert(12, terminal);
    assert_eq!(
        snapshot_error_deliveries(&fixture.sessions[&0], 2, &outgoing).expect("outgoing snapshot"),
        Some((Role::Receiver, HashSet::from([7, 8, 9])))
    );
    assert!(
        snapshot_error_deliveries(&fixture.sessions[&0], 2, &incoming)
            .expect("wrong owner")
            .is_none()
    );
    assert!(
        fixture.sessions[&0]
            .error_deliveries
            .outgoing_ids()
            .next()
            .is_none()
    );
    assert!(
        !fixture.sessions[&0]
            .error_deliveries
            .contains_range(&Role::Sender, 1, Some(0))
    );
}

#[tokio::test]
async fn error_owned_ids_survive_mapped_detach_ack_and_handle_generation_reuse() {
    for role in [Role::Sender, Role::Receiver] {
        for settled in [false, true] {
            let mut fixture = Fixture::new();
            let owner = fixture.link(
                0,
                0,
                42,
                if role == Role::Sender {
                    Role::Receiver
                } else {
                    Role::Sender
                },
            );
            if role == Role::Sender {
                fixture.incoming(&owner, 7, 0);
            } else {
                drop(fixture.outgoing(0, 0, 7));
            }
            let sibling = fixture.link(1, 0, 77, Role::Sender);
            fixture.detach_error().await;
            assert!(fixture.owns(&role, 7, &owner));
            fixture
                .input(
                    0,
                    Performative::Detach(Detach {
                        handle: 42,
                        closed: true,
                        error: None,
                    }),
                    Vec::new(),
                )
                .await;
            assert!(fixture.frames().await.is_empty());
            assert!(fixture.owns(&role, 7, &owner));
            let replacement = fixture.link(
                0,
                0,
                42,
                if role == Role::Sender {
                    Role::Receiver
                } else {
                    Role::Sender
                },
            );
            assert!(!replacement.same_link(&owner));
            let opposite = if role == Role::Sender {
                Role::Receiver
            } else {
                Role::Sender
            };
            fixture
                .input(
                    0,
                    Performative::Disposition(disposition(opposite, 7, None, true)),
                    Vec::new(),
                )
                .await;
            assert!(fixture.frames().await.is_empty());
            assert!(fixture.owns(&role, 7, &owner));
            fixture
                .input(
                    0,
                    Performative::Disposition(disposition(role.clone(), 7, None, settled)),
                    Vec::new(),
                )
                .await;
            fixture.assert_end("amqp:session:errant-link").await;
            assert!(fixture.sessions[&0].ending && replacement.is_retired());
            assert!(!fixture.sessions[&1].ending && !sibling.is_retired());
            assert!(
                !fixture.sessions[&0]
                    .error_deliveries
                    .contains_range(&role, 1, Some(0))
            );
        }
    }
}

#[tokio::test]
async fn wrapping_mixed_disposition_ranges_refuse_before_live_settlement_and_remain_session_scoped()
{
    for settled in [false, true] {
        let mut fixture = Fixture::new();
        fixture.historical(&Role::Receiver, HashSet::from([u32::MAX]));
        let healthy = fixture.link(0, 0, 42, Role::Sender);
        let mut result = fixture.outgoing(0, 0, 0);
        let acknowledgement = AckIdentity::new(&healthy, 1, &[1]);
        fixture
            .sender(0, 0)
            .pending_acknowledgements
            .insert(1, acknowledgement.clone());
        let sibling = fixture.link(1, 0, 77, Role::Sender);
        let sibling_ack = AckIdentity::new(&sibling, 0, &[0]);
        fixture
            .sender(1, 0)
            .pending_acknowledgements
            .insert(0, sibling_ack.clone());
        let mut update = disposition(Role::Receiver, u32::MAX, Some(1), settled);
        update.state = Some(DeliveryState::Accepted(Accepted));
        fixture
            .input(0, Performative::Disposition(update), Vec::new())
            .await;
        fixture.assert_end("amqp:session:errant-link").await;
        assert!(
            !acknowledgement.is_settled(),
            "range precheck precedes healthy ACK mutation"
        );
        assert!(matches!(
            result.try_recv().expect("retired send reply"),
            Err(EngineError::RemoteDetached)
        ));
        assert!(!sibling_ack.is_settled() && !sibling.is_retired());
        assert!(!fixture.sessions[&1].ending);
    }
}

#[tokio::test]
async fn incoming_range_hit_fails_refusal_preflight_before_mutating_a_healthy_live_alias() {
    for settled in [false, true] {
        let mut fixture = Fixture::new();
        fixture.historical(&Role::Sender, HashSet::from([7]));
        let owner = fixture.link(0, 0, 42, Role::Receiver);
        let healthy = fixture.incoming(&owner, 6, 2);
        let session = fixture.sessions.get_mut(&0).expect("session");
        session.local_begin_sent = false;
        session.peer_channel = None;
        let result = apply_disposition(
            0,
            disposition(Role::Sender, 6, Some(7), settled),
            &mut fixture.writer,
            &mut fixture.sessions,
        )
        .await;
        assert!(matches!(result, Err(EngineError::InvalidState(_))));
        assert!(!owner.is_retired() && !fixture.sessions[&0].ending);
        assert!(
            !fixture.sessions[&0]
                .incoming
                .sender_is_settled(&healthy)
                .expect("healthy sender settlement unchanged")
        );
        assert_eq!(
            fixture.sessions[&0]
                .incoming
                .owned_live_ids(&owner)
                .collect::<HashSet<_>>(),
            HashSet::from([6])
        );
        assert!(fixture.frames().await.is_empty());
    }
}

#[tokio::test]
async fn validated_first_delivery_reassigns_only_incoming_history_and_body_failure_records_the_new_owner()
 {
    for body_failure in [false, true] {
        let mut fixture = Fixture::new();
        let old = fixture.historical(&Role::Sender, HashSet::from([7]));
        let outgoing = fixture.historical(&Role::Receiver, HashSet::from([7]));
        let fresh = fixture.link(0, 0, 42, Role::Receiver);
        if body_failure {
            fixture.receiver(0).max_message_size = 1;
        }
        fixture
            .input(0, Performative::Transfer(first(7)), vec![1, 2])
            .await;
        assert!(fixture.owns(&Role::Receiver, 7, &outgoing));
        assert!(!fixture.owns(&Role::Sender, 7, &old));
        if body_failure {
            assert!(fixture.owns(&Role::Sender, 7, &fresh));
            assert!(fresh.is_retired());
            let frames = fixture.frames().await;
            assert!(
                matches!(frames.as_slice(), [Frame::Amqp { performative: Some(Performative::Detach(detach)), .. }] if detach.error.as_ref().expect("size error").condition.as_symbol() == Symbol::from("amqp:link:message-size-exceeded"))
            );
        } else {
            assert!(
                fixture.sessions[&0]
                    .error_deliveries
                    .owner(&Role::Sender, 7)
                    .is_none()
            );
            assert!(!fresh.is_retired());
            assert!(fixture.frames().await.is_empty());
            let token = fixture
                .receiver(0)
                .partial
                .as_ref()
                .expect("fresh partial")
                .identity
                .clone();
            assert!(token.belongs_to(&fresh));
            apply_disposition(
                0,
                disposition(Role::Sender, 7, None, true),
                &mut fixture.writer,
                &mut fixture.sessions,
            )
            .await
            .expect("new incoming ID accepts its sender settlement");
            assert!(
                fixture.sessions[&0]
                    .incoming
                    .sender_is_settled(&token)
                    .expect("new live ID")
            );
            assert!(!fixture.sessions[&0].ending);
        }
    }
}

#[tokio::test]
async fn rejected_first_delivery_never_clears_the_prior_historical_owner() {
    for invalid in 0..7 {
        let mut fixture = Fixture::new();
        let old = fixture.historical(&Role::Sender, HashSet::from([7]));
        let attempted = fixture.link(0, 0, 42, Role::Receiver);
        let mut transfer = first(7);
        match invalid {
            0 => transfer.delivery_id = None,
            1 => transfer.delivery_tag = None,
            2 => transfer.message_format = None,
            3 => transfer.message_format = Some(999),
            4 => transfer.delivery_tag = Some(vec![0; 33].into()),
            5 => {
                fixture.receiver(0).credit =
                    ReceiveCredit::new(0, 0, Arc::new(Consumption::new(Arc::new(Notify::new()))));
            }
            _ => attempted.retire(),
        }
        fixture
            .input(0, Performative::Transfer(transfer), vec![1])
            .await;
        assert!(
            fixture.owns(&Role::Sender, 7, &old),
            "rejection {invalid} retains the old owner"
        );
        assert!(!fixture.owns(&Role::Sender, 7, &attempted));
        assert!(!fixture.sessions[&0].ending);
        let frames = fixture.frames().await;
        assert!(
            matches!(frames.as_slice(), [Frame::Amqp { performative: Some(Performative::Detach(detach)), .. }] if detach.error.is_some())
        );
    }
}

#[tokio::test]
async fn tag_collision_and_changed_continuation_id_do_not_commit_historical_reassignment() {
    for continuation in [false, true] {
        let mut fixture = Fixture::new();
        let old = fixture.historical(&Role::Sender, HashSet::from([7]));
        let owner = fixture.link(0, 0, 42, Role::Receiver);
        if continuation {
            fixture
                .input(0, Performative::Transfer(first(8)), vec![1])
                .await;
            assert!(fixture.frames().await.is_empty());
            let mut changed = first(7);
            changed.delivery_tag = None;
            changed.message_format = None;
            fixture
                .input(0, Performative::Transfer(changed), vec![1])
                .await;
        } else {
            fixture
                .sessions
                .get_mut(&0)
                .expect("session")
                .incoming
                .reserve(&owner, 8, &7_u32.to_be_bytes())
                .expect("existing tag");
            fixture
                .input(0, Performative::Transfer(first(7)), vec![1])
                .await;
        }
        assert!(fixture.owns(&Role::Sender, 7, &old));
        assert!(fixture.owns(&Role::Sender, 8, &owner));
        let frames = fixture.frames().await;
        assert!(
            matches!(frames.as_slice(), [Frame::Amqp { performative: Some(Performative::Detach(detach)), .. }] if detach.error.as_ref().expect("identity error").condition.as_symbol() == Symbol::from("amqp:invalid-field"))
        );
    }
}

#[tokio::test]
async fn directional_history_overflow_sends_scoped_resource_end_instead_of_an_untracked_error_detach()
 {
    for role in [Role::Sender, Role::Receiver] {
        for api_close in [false, true] {
            let mut fixture = Fixture::new();
            let ids: HashSet<_> = (0..MAX_RETIRED_DELIVERIES_PER_DIRECTION as u32).collect();
            fixture.historical(&role, ids);
            let owner = fixture.link(
                0,
                0,
                42,
                if role == Role::Sender {
                    Role::Receiver
                } else {
                    Role::Sender
                },
            );
            if role == Role::Sender {
                fixture.incoming(&owner, MAX_RETIRED_DELIVERIES_PER_DIRECTION as u32, 0);
            } else {
                drop(fixture.outgoing(0, 0, MAX_RETIRED_DELIVERIES_PER_DIRECTION as u32));
            }
            let sibling = fixture.link(1, 0, 77, Role::Sender);
            if api_close {
                let (reply, result) = oneshot::channel();
                handle_command(
                    Command::Detach {
                        channel: 0,
                        handle: 0,
                        identity: owner.clone(),
                        error: Some(Error::new(
                            crate::AmqpError::NotImplemented,
                            "overflowing endpoint close",
                            None,
                        )),
                        reply,
                    },
                    &mut fixture.writer,
                    &mut fixture.sessions,
                    512,
                )
                .await
                .expect("bounded API close overflow");
                result
                    .await
                    .expect("close reply")
                    .expect("scoped End is an idempotent successful close");
            } else {
                detach_link_error(
                    0,
                    0,
                    fixture.sessions.get_mut(&0).expect("session"),
                    &mut fixture.writer,
                    "amqp:invalid-field",
                    "overflowing error link",
                )
                .await
                .expect("bounded history overflow");
            }
            fixture.assert_end("amqp:resource-limit-exceeded").await;
            assert!(fixture.sessions[&0].ending && owner.is_retired());
            assert!(!fixture.sessions[&1].ending && !sibling.is_retired());
            assert!(!fixture.sessions[&0].error_deliveries.contains_range(
                &Role::Sender,
                1,
                Some(0)
            ));
            assert!(!fixture.sessions[&0].error_deliveries.contains_range(
                &Role::Receiver,
                1,
                Some(0)
            ));
        }
    }
}

#[test]
fn outgoing_allocator_skips_the_full_live_and_error_retired_union_across_wrap_without_mutation() {
    let mut fixture = Fixture::new();
    let start = u32::MAX - 4_095;
    let retired: HashSet<_> = (0..MAX_RETIRED_DELIVERIES_PER_DIRECTION as u32)
        .map(|offset| start.wrapping_add(offset))
        .collect();
    fixture.historical(&Role::Receiver, retired);
    for handle in 0..4 {
        fixture.link(0, handle, 42 + handle, Role::Sender);
        for offset in 0..MAX_OUTGOING_DELIVERIES_PER_LINK as u32 {
            let id = start.wrapping_add(
                MAX_RETIRED_DELIVERIES_PER_DIRECTION as u32
                    + handle * MAX_OUTGOING_DELIVERIES_PER_LINK as u32
                    + offset,
            );
            drop(fixture.outgoing(0, handle, id));
        }
    }
    let next = start.wrapping_add(
        (MAX_RETIRED_DELIVERIES_PER_DIRECTION + MAX_OUTGOING_DELIVERIES_PER_SESSION) as u32,
    );
    fixture.historical(&Role::Sender, HashSet::from([next]));
    let session = fixture.sessions.get_mut(&0).expect("session");
    session.next_delivery_id = start;
    assert_eq!(vacant_delivery_id(session), Some(next));
    assert_eq!(session.next_delivery_id, start);
    assert!(delivery_id_in_use(session, start));
    assert!(!delivery_id_in_use(session, next));
    assert_eq!(
        session.error_deliveries.outgoing_ids().count(),
        MAX_RETIRED_DELIVERIES_PER_DIRECTION
    );
    let live: usize = session
        .links
        .values()
        .map(|link| match link {
            LinkState::Sending(link) => link.unsettled.len(),
            LinkState::Receiving(_) => 0,
        })
        .sum();
    assert_eq!(live, MAX_OUTGOING_DELIVERIES_PER_SESSION);
}

#[tokio::test]
async fn ordinary_detach_does_not_record_error_history_and_session_end_clears_both_directions() {
    let mut fixture = Fixture::new();
    let owner = fixture.link(0, 0, 42, Role::Sender);
    drop(fixture.outgoing(0, 0, 7));
    let (reply, result) = oneshot::channel();
    handle_command(
        Command::Detach {
            channel: 0,
            handle: 0,
            identity: owner,
            error: None,
            reply,
        },
        &mut fixture.writer,
        &mut fixture.sessions,
        512,
    )
    .await
    .expect("ordinary local detach");
    result
        .await
        .expect("detach result")
        .expect("ordinary detach");
    assert!(
        !fixture.sessions[&0]
            .error_deliveries
            .contains_range(&Role::Receiver, 7, None)
    );
    drop(fixture.frames().await);
    fixture.historical(&Role::Sender, HashSet::from([7]));
    fixture.historical(&Role::Receiver, HashSet::from([8]));
    refuse_session(
        0,
        "amqp:session:errant-link",
        "session cleanup",
        &mut fixture.writer,
        &mut fixture.sessions,
    )
    .await
    .expect("session End");
    fixture.assert_end("amqp:session:errant-link").await;
    assert!(
        !fixture.sessions[&0]
            .error_deliveries
            .contains_range(&Role::Sender, 1, Some(0))
    );
    assert!(
        !fixture.sessions[&0]
            .error_deliveries
            .contains_range(&Role::Receiver, 1, Some(0))
    );
}

#[tokio::test]
async fn oversized_error_detach_preflight_preserves_live_ids_without_recording_new_history() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new();
        let original = fixture.historical(&role, HashSet::from([8]));
        let owner = fixture.link(
            0,
            0,
            42,
            if role == Role::Sender {
                Role::Receiver
            } else {
                Role::Sender
            },
        );
        if role == Role::Sender {
            fixture.incoming(&owner, 7, 0);
        } else {
            drop(fixture.outgoing(0, 0, 7));
        }
        let result = detach_link_error(
            0,
            0,
            fixture.sessions.get_mut(&0).expect("session"),
            &mut fixture.writer,
            "amqp:invalid-field",
            "x".repeat(2048),
        )
        .await;
        assert!(matches!(result, Err(EngineError::Io(_))));
        assert!(!owner.is_retired() && !fixture.sessions[&0].ending);
        assert!(fixture.sessions[&0].links[&0].identity().same_link(&owner));
        assert!(!fixture.sessions[&0].closing_handles.contains(&0));
        assert!(!fixture.sessions[&0].handle_aliases[&0].error_detached);
        assert!(
            fixture.sessions[&0]
                .error_deliveries
                .owner(&role, 7)
                .is_none()
        );
        assert!(fixture.owns(&role, 8, &original));
        assert_eq!(
            snapshot_error_deliveries(&fixture.sessions[&0], 0, &owner)
                .expect("still-live snapshot"),
            Some((role, HashSet::from([7])))
        );
        assert!(fixture.frames().await.is_empty());
    }
}

#[tokio::test]
async fn failed_or_cancelled_error_detach_flush_keeps_known_ids_owned_in_both_directions() {
    for role in [Role::Sender, Role::Receiver] {
        for failed in [false, true] {
            let mut fixture = Fixture::new();
            let owner = fixture.link(
                0,
                0,
                42,
                if role == Role::Sender {
                    Role::Receiver
                } else {
                    Role::Sender
                },
            );
            if role == Role::Sender {
                fixture.incoming(&owner, 7, 0);
                fixture.incoming(&owner, 8, 1);
                fixture.incoming(&owner, 9, 2);
            } else {
                drop(fixture.outgoing(0, 0, 7));
                let acknowledgement = AckIdentity::new(&owner, 8, &[8]);
                fixture
                    .sender(0, 0)
                    .pending_acknowledgements
                    .insert(8, acknowledgement);
                let lease = fixture
                    .writer
                    .content_budget()
                    .try_reserve(1)
                    .expect("active content");
                fixture.sender(0, 0).active = Some(ActiveSend {
                    payload: vec![1],
                    content_lease: lease,
                    offset: 0,
                    first_frame_sent: true,
                    delivery_id: 9,
                    delivery_tag: vec![9].into(),
                    message_format: 0,
                    settled: true,
                    settled_reply: None,
                });
            }
            fixture.output.0.fail_flush.store(failed, Ordering::Release);
            fixture
                .output
                .0
                .block_flush
                .store(!failed, Ordering::Release);
            let detaching = detach_link_error(
                0,
                0,
                fixture.sessions.get_mut(&0).expect("session"),
                &mut fixture.writer,
                "amqp:invalid-field",
                "uncertain error detach",
            );
            if failed {
                assert!(matches!(detaching.await, Err(EngineError::Io(_))));
            } else {
                assert!(timeout(Duration::from_millis(10), detaching).await.is_err());
            }
            assert!(owner.is_retired());
            assert!(!fixture.sessions[&0].links.contains_key(&0));
            assert!(fixture.sessions[&0].closing_handles.contains(&0));
            assert!(fixture.sessions[&0].handle_aliases[&0].error_detached);
            for id in [7, 8, 9] {
                assert!(fixture.owns(&role, id, &owner));
            }
            let opposite = if role == Role::Sender {
                Role::Receiver
            } else {
                Role::Sender
            };
            assert!(
                !fixture.sessions[&0]
                    .error_deliveries
                    .contains_range(&opposite, 1, Some(0))
            );
            let frames = fixture.frames().await;
            assert!(matches!(
                frames.as_slice(),
                [Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Detach(Detach {
                        handle: 0,
                        error: Some(_),
                        ..
                    })),
                    ..
                }]
            ));
        }
    }
}
