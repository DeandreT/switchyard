use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use super::*;
use crate::Value;
use crate::server::content_budget::ContentBudget;
use group::{ControlData, DischargeStatus, Group, NativeRoute};

const CHANNEL: u16 = 0;
const CONTROL: u32 = 1;
const POST: u32 = 2;

#[derive(Clone, Default)]
struct RecordedWriter(Arc<Mutex<Vec<u8>>>);

impl AsyncWrite for RecordedWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0
            .lock()
            .expect("captured frames")
            .extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct Fixture {
    budget: ContentBudget,
    sessions: HashMap<u16, SessionState>,
    book: NativeTransactionBook,
    writer: FrameWriter<RecordedWriter>,
    output: RecordedWriter,
    mode: ReceiverSettleMode,
    commands: mpsc::Sender<Command>,
    queued: mpsc::Receiver<Command>,
    controller: NativeControllerIdentity,
    controller_owner: LinkIdentity,
    post_owner: LinkIdentity,
    group: Arc<Group>,
    post: Message,
    control: Message,
    post_bytes: usize,
    control_bytes: usize,
}

impl Fixture {
    fn new() -> Self {
        Self::with_mode(ReceiverSettleMode::First)
    }

    fn with_mode(mode: ReceiverSettleMode) -> Self {
        let connection = NativeConnectionIdentity::new();
        let controller_owner = LinkIdentity::for_connection(&connection);
        let post_owner = LinkIdentity::for_connection(&connection);
        let (commands, queued) = mpsc::channel(4);
        let mut book = NativeTransactionBook::new(&connection, NativeIngressPolicy::Posting);
        let controller = book
            .accept_controller(
                CHANNEL,
                CONTROL,
                controller_owner.clone(),
                commands.clone(),
                NativeCoordinatorProfile {
                    capabilities: 0,
                    outcomes: 0,
                },
            )
            .expect("bound controller fixture");
        book.accept_receiver(CHANNEL, POST, post_owner.clone(), commands.clone())
            .expect("bound data fixture");
        let id = TransactionId::new([42]).expect("bounded ID");
        let group = Group::new(id.clone(), controller.clone());
        let post = Message::data(vec![7; 127]);
        let control = Message {
            body: Body::Value(Value::from(TransactionCommand::Discharge(
                crate::Discharge {
                    txn_id: id,
                    fail: Some(false),
                },
            ))),
            ..Message::default()
        };
        let post_bytes = encode_message(&post).expect("post encoding").len();
        let control_bytes = encode_message(&control).expect("control encoding").len();
        let budget = ContentBudget::new(post_bytes + control_bytes);
        let mut session = SessionState::for_connection(&Begin::default(), &connection);
        session.local_begin_sent = true;
        for (handle, owner) in [(CONTROL, &controller_owner), (POST, &post_owner)] {
            let (deliveries, _inbox) = mpsc::channel(2);
            let (detached, _) = watch::channel(false);
            let consumption = Arc::new(Consumption::new(Arc::new(Notify::new())));
            session.links.insert(
                handle,
                LinkState::Receiving(Box::new(ReceivingLink {
                    max_message_size: 0,
                    deliveries: ReceivingSink::Ordinary(deliveries),
                    partial: None,
                    detached,
                    credit: ReceiveCredit::new(0, LINK_CREDIT, consumption),
                    decoders: MessageFormatDecoders::default(),
                    identity: owner.clone(),
                    sender_settle_mode: SenderSettleMode::Unsettled,
                    receiver_settle_mode: mode.clone(),
                })),
            );
        }
        let output = RecordedWriter::default();
        Self {
            writer: FrameWriter::new_with_content_budget(output.clone(), 512, budget.clone())
                .expect("bounded frame writer"),
            output,
            mode,
            budget,
            sessions: HashMap::from([(CHANNEL, session)]),
            book,
            commands,
            queued,
            controller,
            controller_owner,
            post_owner,
            group,
            post,
            control,
            post_bytes,
            control_bytes,
        }
    }

    fn delivery(
        &mut self,
        owner: &LinkIdentity,
        id: u32,
        message: Message,
        bytes: usize,
    ) -> Delivery {
        let session = self.sessions.get_mut(&CHANNEL).expect("session");
        let identity = session
            .incoming
            .reserve(owner, id, &[id as u8])
            .expect("exact delivery identity");
        session
            .incoming
            .complete(&identity, false, self.mode.clone())
            .expect("complete delivery");
        Delivery {
            id,
            settled: false,
            message_format: 0,
            message,
            identity,
            content_lease: Some(Arc::new(
                self.budget
                    .try_reserve(bytes)
                    .expect("encoded content reservation"),
            )),
        }
    }

    fn posting(&mut self) -> TransactionPostingReceipt {
        let owner = self.post_owner.clone();
        let delivery = self.delivery(&owner, 0, self.post.clone(), self.post_bytes);
        let obligation = self
            .group
            .reserve(CHANNEL, POST, owner.clone(), delivery.identity.clone())
            .expect("original posting obligation");
        let partial = NativePartialPosting::new(self.group.clone(), obligation);
        let receipt = partial.into_post(
            NativeRoute {
                channel: CHANNEL,
                handle: POST,
                owner,
                commands: self.commands.clone(),
            },
            delivery,
        );
        receipt.dequeued();
        receipt
    }

    fn sealed(&mut self) -> SealedDischargeReceipt {
        self.sealed_with_fail(false)
    }

    fn sealed_with_fail(&mut self, fail: bool) -> SealedDischargeReceipt {
        self.group.seal(fail).expect("nonpartial seal");
        let owner = self.controller_owner.clone();
        let control = if fail {
            Message {
                body: Body::Value(Value::from(TransactionCommand::Discharge(
                    crate::Discharge {
                        txn_id: self.group.id.clone(),
                        fail: Some(true),
                    },
                ))),
                ..Message::default()
            }
        } else {
            self.control.clone()
        };
        assert_eq!(
            encode_message(&control).expect("control encoding").len(),
            self.control_bytes
        );
        let delivery = self.delivery(&owner, 1, control, self.control_bytes);
        let data = ControlData::new(
            NativeRoute {
                channel: CHANNEL,
                handle: CONTROL,
                owner,
                commands: self.commands.clone(),
            },
            self.controller.clone(),
            delivery,
            Some(self.group.clone()),
            fail,
        );
        SealedDischargeReceipt {
            data,
            status: DischargeStatus::Live(self.group.clone()),
        }
    }

    async fn dispositions(&self) -> Vec<Disposition> {
        let bytes = std::mem::take(&mut *self.output.0.lock().expect("captured frames"));
        let mut input = bytes.as_slice();
        let mut result = Vec::new();
        while !input.is_empty() {
            assert!(result.len() < 4, "bounded fixture dispositions");
            let Frame::Amqp {
                channel: CHANNEL,
                performative: Some(Performative::Disposition(disposition)),
                payload,
            } = read_frame(&mut input).await.expect("captured disposition")
            else {
                panic!("only exact disposition frames")
            };
            assert!(payload.is_empty());
            assert_eq!(disposition.role, Role::Receiver);
            result.push(disposition);
        }
        result
    }

    async fn drive<T>(&mut self, future: impl Future<Output = Result<T, EngineError>>) -> T {
        let mut future = Box::pin(future);
        poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let Command::NativeTransactions(command) = self.queued.try_recv().expect("native command")
        else {
            panic!("native command")
        };
        handle_native_command(
            command,
            &mut self.book,
            &mut self.sessions,
            &mut self.writer,
        )
        .await
        .expect("native handler flush");
        future.await.expect("native reply")
    }
}

#[tokio::test]
async fn posting_prepared_and_resources_keep_original_content_charge_through_finish() {
    let mut fixture = Fixture::new();
    let posting = fixture.posting();
    assert_eq!(fixture.budget.retained_bytes(), fixture.post_bytes);
    assert_eq!(posting.message(), &fixture.post);
    let sealed = fixture.sealed();
    let total = fixture.post_bytes + fixture.control_bytes;
    assert_eq!(fixture.budget.retained_bytes(), total);
    let prepared = fixture.drive(posting.provisional_accept()).await;
    assert_eq!(
        fixture.budget.retained_bytes(),
        total,
        "provisional flush does not refund either receipt"
    );
    assert_eq!(prepared.message(), &fixture.post);
    fixture
        .sessions
        .get_mut(&CHANNEL)
        .expect("session")
        .incoming
        .sender_settled_range(0, None);
    assert_eq!(
        fixture.budget.retained_bytes(),
        total,
        "transport sender ACK cannot refund retained native content"
    );
    assert!(fixture.budget.try_reserve(1).is_err());
    sealed.wait_ready().await.expect("ready");
    let ready = sealed.prepare(vec![prepared]).expect("exact resources");
    assert_eq!(fixture.budget.retained_bytes(), total);
    let (ticket, resources) = ready.into_owner_parts();
    let claim = ticket.try_claim().expect("native claim");
    claim.finish(NativeTransactionDecision::Committed);
    assert_eq!(
        fixture.budget.retained_bytes(),
        total,
        "decision alone does not release the native payload"
    );
    let mut finishing = Box::pin(resources.finish());
    poll_fn(|cx| {
        assert!(finishing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(
        fixture.budget.retained_bytes(),
        total,
        "queued final acknowledgement still owns all payloads"
    );
    let Command::NativeTransactions(command) = fixture.queued.try_recv().expect("Finish command")
    else {
        panic!("native Finish")
    };
    handle_native_command(
        command,
        &mut fixture.book,
        &mut fixture.sessions,
        &mut fixture.writer,
    )
    .await
    .expect("final native flushes");
    finishing.await.expect("completion reply");
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert_eq!(fixture.group.state(), NativeTransactionState::Committed);
}

#[tokio::test]
async fn unclaimed_resource_drop_faults_before_reservation_refund() {
    let mut fixture = Fixture::new();
    let posting = fixture.posting();
    let sealed = fixture.sealed();
    let prepared = fixture.drive(posting.provisional_accept()).await;
    let (ticket, resources) = sealed
        .prepare(vec![prepared])
        .expect("exact prepared resources")
        .into_owner_parts();
    assert_eq!(
        fixture.budget.retained_bytes(),
        fixture.post_bytes + fixture.control_bytes
    );
    drop(resources);
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert_eq!(fixture.group.state(), NativeTransactionState::Faulted);
    assert_eq!(
        fixture.group.error(),
        NativeTransactionError::Faulted(NativeFault::Dropped)
    );
    assert!(matches!(
        ticket.try_claim(),
        Err(NativeTransactionError::Faulted(NativeFault::Dropped))
    ));
}

#[tokio::test]
async fn dropped_claim_records_indeterminate_while_resources_remain_charged() {
    let mut fixture = Fixture::new();
    let posting = fixture.posting();
    let sealed = fixture.sealed();
    let prepared = fixture.drive(posting.provisional_accept()).await;
    let (ticket, resources) = sealed
        .prepare(vec![prepared])
        .expect("exact prepared resources")
        .into_owner_parts();
    let claim = ticket.try_claim().expect("native claim");
    drop(claim);
    assert_eq!(fixture.group.state(), NativeTransactionState::Indeterminate);
    assert_eq!(
        fixture.budget.retained_bytes(),
        fixture.post_bytes + fixture.control_bytes
    );
    drop(resources);
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert_eq!(fixture.group.state(), NativeTransactionState::Indeterminate);
}

#[tokio::test]
async fn dropped_or_explicitly_failed_posting_refunds_once_without_readiness() {
    for stage_failed in [false, true] {
        let mut fixture = Fixture::new();
        let posting = fixture.posting();
        assert_eq!(fixture.budget.retained_bytes(), fixture.post_bytes);
        if stage_failed {
            posting.fail();
        } else {
            drop(posting);
        }
        assert_eq!(fixture.budget.retained_bytes(), 0);
        assert_eq!(fixture.group.state(), NativeTransactionState::Faulted);
        assert_eq!(
            fixture.group.error(),
            NativeTransactionError::Faulted(if stage_failed {
                NativeFault::Stage
            } else {
                NativeFault::Dropped
            })
        );
    }
}

#[tokio::test]
async fn native_receipt_finalization_obeys_both_modes_without_waiting_for_sender_ack() {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        let settled = mode == ReceiverSettleMode::First;
        let mut fixture = Fixture::with_mode(mode);
        let posting = fixture.posting();
        let sealed = fixture.sealed();
        let total = fixture.post_bytes + fixture.control_bytes;
        let prepared = fixture.drive(posting.provisional_accept()).await;
        let provisional = fixture.dispositions().await;
        assert_eq!(provisional.len(), 1);
        assert_eq!(provisional[0].first, 0);
        assert!(!provisional[0].settled);
        assert!(matches!(
            provisional[0].state,
            Some(DeliveryState::Transactional(TransactionalState {
                outcome: Some(Outcome::Accepted(_)),
                ..
            }))
        ));
        assert_eq!(fixture.budget.retained_bytes(), total);
        let (ticket, resources) = sealed
            .prepare(vec![prepared])
            .expect("exact prepared set")
            .into_owner_parts();
        ticket
            .try_claim()
            .expect("native claim")
            .finish(NativeTransactionDecision::Committed);
        fixture.drive(resources.finish()).await;
        assert_eq!(
            fixture.budget.retained_bytes(),
            0,
            "finish promises flushed transport outcomes, not sender settlement"
        );
        let final_outcomes = fixture.dispositions().await;
        assert_eq!(final_outcomes.len(), 2);
        for (outcome, expected_id) in final_outcomes.iter().zip([0, 1]) {
            assert_eq!(outcome.first, expected_id);
            assert_eq!(outcome.settled, settled);
            assert!(
                matches!(outcome.state, Some(DeliveryState::Accepted(_))),
                "only provisional outcome is TransactionalState"
            );
        }
        let incoming = &mut fixture
            .sessions
            .get_mut(&CHANNEL)
            .expect("session")
            .incoming;
        assert_eq!(
            incoming.owned_live_ids(&fixture.post_owner).count(),
            usize::from(!settled)
        );
        assert_eq!(
            incoming.owned_live_ids(&fixture.controller_owner).count(),
            usize::from(!settled)
        );
        incoming.sender_settled_range(0, Some(1));
        assert_eq!(incoming.owned_live_ids(&fixture.post_owner).count(), 0);
        assert_eq!(
            incoming.owned_live_ids(&fixture.controller_owner).count(),
            0
        );
        assert_eq!(fixture.budget.retained_bytes(), 0);
    }
}

#[tokio::test]
async fn abort_metadata_cleanup_keeps_complete_and_prepared_content_owned_until_drop() {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        for flushed in [false, true] {
            let settled = mode == ReceiverSettleMode::First;
            let mut fixture = Fixture::with_mode(mode.clone());
            let posting = fixture.posting();
            let (posting, prepared) = if flushed {
                let prepared = fixture.drive(posting.provisional_accept()).await;
                assert_eq!(fixture.dispositions().await.len(), 1);
                (None, Some(prepared))
            } else {
                (Some(posting), None)
            };
            let abort = fixture.sealed_with_fail(true);
            assert_eq!(
                fixture.budget.retained_bytes(),
                fixture.post_bytes + fixture.control_bytes
            );
            fixture.drive(abort.rollback()).await;
            assert_eq!(fixture.group.state(), NativeTransactionState::Aborted);
            assert_eq!(
                fixture.budget.retained_bytes(),
                fixture.post_bytes,
                "metadata-only cleanup must not refund an external posting receipt"
            );
            let outcomes = fixture.dispositions().await;
            assert_eq!(outcomes.len(), 2);
            assert_eq!(outcomes[0].first, 0);
            assert_eq!(outcomes[0].settled, settled);
            assert!(outcomes[0].state.is_none());
            assert_eq!(outcomes[1].first, 1);
            assert_eq!(outcomes[1].settled, settled);
            assert!(matches!(
                outcomes[1].state,
                Some(DeliveryState::Accepted(_))
            ));
            fixture
                .sessions
                .get_mut(&CHANNEL)
                .expect("session")
                .incoming
                .sender_settled_range(0, Some(1));
            assert_eq!(
                fixture.budget.retained_bytes(),
                fixture.post_bytes,
                "sender ACK is not receipt ownership"
            );
            if let Some(posting) = &posting {
                assert_eq!(posting.message(), &fixture.post);
            }
            if let Some(prepared) = &prepared {
                assert_eq!(prepared.message(), &fixture.post);
            }
            drop(posting);
            drop(prepared);
            assert_eq!(fixture.budget.retained_bytes(), 0);
            assert_eq!(fixture.group.state(), NativeTransactionState::Aborted);
        }
    }
}

#[tokio::test]
async fn ordinary_error_detach_faults_native_group_without_refunding_held_content() {
    for flushed in [false, true] {
        let mut fixture = Fixture::new();
        fixture
            .controller
            .0
            .register(&fixture.group)
            .expect("controller retirement tracks the original group");
        let posting = fixture.posting();
        let (posting, prepared) = if flushed {
            let prepared = fixture.drive(posting.provisional_accept()).await;
            assert_eq!(fixture.dispositions().await.len(), 1);
            (None, Some(prepared))
        } else {
            (Some(posting), None)
        };
        assert_eq!(fixture.budget.retained_bytes(), fixture.post_bytes);
        let error = Error::new(
            crate::AmqpError::InternalError,
            "native posting stage failed",
            None,
        );
        let (reply, response) = oneshot::channel();
        let result = crate::server::handle_command(
            Command::Detach {
                channel: CHANNEL,
                handle: CONTROL,
                identity: fixture.controller_owner.clone(),
                error: Some(error.clone()),
                reply,
            },
            &mut fixture.writer,
            &mut fixture.sessions,
            512,
        )
        .await
        .expect("ordinary exact Detach handler");
        assert!(matches!(result, CommandAction::Continue));
        response
            .await
            .expect("close reply")
            .expect("error Detach flushed");
        assert_eq!(fixture.group.state(), NativeTransactionState::Faulted);
        assert_eq!(
            fixture.group.error(),
            NativeTransactionError::Faulted(NativeFault::Closed)
        );
        assert!(!fixture.controller.is_active());
        assert_eq!(
            fixture.budget.retained_bytes(),
            fixture.post_bytes,
            "error Detach retires metadata, not an external native receipt lease"
        );
        let bytes = std::mem::take(&mut *fixture.output.0.lock().expect("captured frames"));
        let mut input = bytes.as_slice();
        let Frame::Amqp {
            channel: CHANNEL,
            performative: Some(Performative::Detach(detach)),
            payload,
        } = read_frame(&mut input).await.expect("exact error Detach")
        else {
            panic!("no posting or control outcome")
        };
        assert_eq!(detach.handle, CONTROL);
        assert!(detach.closed);
        assert_eq!(detach.error, Some(error));
        assert!(payload.is_empty());
        assert!(input.is_empty());
        if let Some(posting) = &posting {
            assert_eq!(posting.message(), &fixture.post);
        }
        if let Some(prepared) = &prepared {
            assert_eq!(prepared.message(), &fixture.post);
        }
        drop(posting);
        drop(prepared);
        assert_eq!(fixture.budget.retained_bytes(), 0);
        assert_eq!(fixture.group.state(), NativeTransactionState::Faulted);
    }
}
